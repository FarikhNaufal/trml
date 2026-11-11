package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trml/internal/config"
	"trml/internal/exchange"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/storage"
)

func TestPollOpenInterest_ConcurrentAndPartialFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	var mu sync.Mutex
	histCalls := make(map[string]int)
	liveCalls := make(map[string]int)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/futures/data/openInterestHist":
			mu.Lock()
			histCalls[sym]++
			mu.Unlock()

			if sym == "FAILUSDT" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
				return
			}
			resp := []exchange.OpenInterestHistItem{
				{Symbol: sym, SumOpenInterest: "1000.0", Timestamp: now.Add(-13 * time.Minute).UnixMilli()},
				{Symbol: sym, SumOpenInterest: "1010.0", Timestamp: now.Add(-8 * time.Minute).UnixMilli()},
				{Symbol: sym, SumOpenInterest: "1020.0", Timestamp: now.Add(-3 * time.Minute).UnixMilli()},
			}
			_ = json.NewEncoder(w).Encode(resp)

		case "/fapi/v1/openInterest":
			mu.Lock()
			liveCalls[sym]++
			mu.Unlock()

			if sym == "FAILUSDT" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"symbol":       sym,
				"openInterest": "1050.0",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := exchange.NewClient(ts.URL, "", "")
	engine := &BotEngine{
		cfg: &config.Config{
			CandleDuration: 5 * time.Minute,
			MaxOIInterval:  10 * time.Minute,
		},
		client:          client,
		activeStates:    make(map[string]*model.SymbolState),
		watchedCoins:    []string{"ZECUSDT", "FAILUSDT", "ETHUSDT"},
		oiBootAttemptAt: make(map[string]time.Time),
	}

	for _, sym := range engine.watchedCoins {
		st := model.NewSymbolState(sym, model.ExchangeFilter{}, 10, 10)
		st.TargetOIInterval = 5 * time.Minute
		engine.activeStates[sym] = st
	}

	// Putaran 1: FAILUSDT gagal, ZECUSDT & ETHUSDT harus tetap sukses bootstrap + live poll
	engine.pollOpenInterest(context.Background())

	for _, sym := range []string{"ZECUSDT", "ETHUSDT"} {
		st := engine.activeStates[sym]
		st.Mu.Lock()
		prevOI := st.PrevOpenInterest
		currOI := st.CurrOpenInterest
		needsBoot := st.NeedsOIBootstrapLocked(time.Now().UTC(), engine.cfg.MaxOIInterval)
		st.Mu.Unlock()

		if prevOI <= 0 || currOI != 1050.0 {
			t.Fatalf("%s expected valid OI pair (prev > 0, curr == 1050), got prev=%f curr=%f", sym, prevOI, currOI)
		}
		if needsBoot {
			t.Fatalf("%s should no longer need OI bootstrap", sym)
		}
	}

	// Putaran 2 segera sesudahnya: FAILUSDT dalam cooldown 2m -> tidak memborbardir openInterestHist,
	// tetapi tetap mencoba live poll; ZECUSDT/ETHUSDT tidak bootstrap ulang.
	engine.pollOpenInterest(context.Background())

	mu.Lock()
	zecHist := histCalls["ZECUSDT"]
	failHist := histCalls["FAILUSDT"]
	failLive := liveCalls["FAILUSDT"]
	mu.Unlock()

	if zecHist != 1 {
		t.Fatalf("expected ZECUSDT openInterestHist called once, got %d", zecHist)
	}
	if failHist != 1 {
		t.Fatalf("expected FAILUSDT openInterestHist throttled to 1 call within cooldown, got %d", failHist)
	}
	if failLive < 2 {
		t.Fatalf("expected FAILUSDT live poll attempted on both ticks, got %d", failLive)
	}

	// Simulasi cooldown habis untuk FAILUSDT -> NeedsOIBootstrapLocked tetap true dan bootstrap bisa dicoba lagi
	engine.oiBootMu.Lock()
	engine.oiBootAttemptAt["FAILUSDT"] = time.Now().UTC().Add(-3 * time.Minute)
	engine.oiBootMu.Unlock()

	engine.pollOpenInterest(context.Background())

	mu.Lock()
	failHistAfter := histCalls["FAILUSDT"]
	mu.Unlock()
	if failHistAfter != 2 {
		t.Fatalf("expected FAILUSDT openInterestHist retried after cooldown, got %d", failHistAfter)
	}

	// Guard anti-overlap: bila oiPollRunning sudah true, panggilan berikutnya langsung skip tanpa memanggil API
	liveBefore := liveCalls["ZECUSDT"]
	engine.oiPollRunning.Store(true)
	engine.pollOpenInterest(context.Background())
	engine.oiPollRunning.Store(false)

	mu.Lock()
	liveAfter := liveCalls["ZECUSDT"]
	mu.Unlock()
	if liveAfter != liveBefore {
		t.Fatalf("expected overlapping pollOpenInterest to skip, calls changed from %d to %d", liveBefore, liveAfter)
	}
}

func TestResolveScannerConfigUsesEnvValues(t *testing.T) {
	cfg := &config.Config{
		MinVolume24hUSDT:      25_000_000,
		MaxSpreadPct:          0.04,
		MinRVOL:               1.35,
		MinFallbackRVOL:       0.75,
		MinNATRPct:            0.60,
		MaxNATRPct:            2.00,
		MaxScannerVWAPDistATR: 2.50,
		MaxWatchedCoins:       30,
		Timeframe:             "15m",
		CandleDuration:        15 * time.Minute,
	}
	sc := scanner.FromAppConfig(cfg, 1000)
	if sc.MinRVOL != 1.35 {
		t.Fatalf("expected MinRVOL=1.35 from config, got %v", sc.MinRVOL)
	}
	if sc.CandleDuration != 15*time.Minute || sc.Timeframe != "15m" {
		t.Fatalf("expected 15m timeframe propagated, got %s (%v)", sc.Timeframe, sc.CandleDuration)
	}
	if sc.RankingMode != "pullback" {
		t.Fatalf("expected default RankingMode='pullback', got %q", sc.RankingMode)
	}

	cfg.MinRVOL = 0
	cfg.ScannerRankingMode = "rvol"
	if fallback := scanner.FromAppConfig(cfg, 1000); fallback.MinRVOL != 1.80 || fallback.RankingMode != "rvol" {
		t.Fatalf("expected fallback MinRVOL=1.80 and RankingMode='rvol', got MinRVOL=%v RankingMode=%q", fallback.MinRVOL, fallback.RankingMode)
	}
}

func TestExchangeNowRespectsTimeOffset(t *testing.T) {
	client := &exchange.Client{TimeOffset: 15_000}
	eng := &BotEngine{client: client}

	before := time.Now().UTC()
	got := eng.exchangeNow()
	after := time.Now().UTC()

	if got.Before(before.Add(14*time.Second)) || got.After(after.Add(16*time.Second)) {
		t.Fatalf("expected exchangeNow offset ~+15s from %v..%v, got %v", before, after, got)
	}
}

func TestNextSafeScanDelayAvoidsCandleBoundaries(t *testing.T) {
	base := time.Date(2026, 11, 11, 22, 0, 0, 0, time.UTC)

	if d := nextSafeScanDelay(base, 15*time.Minute, 5*time.Minute); d != 25*time.Second {
		t.Fatalf("at bar close 22:00:00 expected 25s delay, got %v", d)
	}
	afterScan := base.Add(30 * time.Second)
	if d := nextSafeScanDelay(afterScan, 15*time.Minute, 5*time.Minute); d != 14*time.Minute+55*time.Second {
		t.Fatalf("at 22:00:30 expected 14m55s to 22:15:25, got %v", d)
	}
	if d := nextSafeScanDelay(base, 1*time.Minute, 1*time.Minute); d != 10*time.Second {
		t.Fatalf("for 1m timeframe expected 10s safe offset, got %v", d)
	}
}

func TestBuildMarketStreamSubscriptions_AggTradeToggle(t *testing.T) {
	pubLive, mktLive := buildMarketStreamSubscriptions([]string{"ETHUSDT"}, "5m", false)
	if len(pubLive) != 2 || !slices.Contains(pubLive, "ethusdt@bookTicker") || !slices.Contains(pubLive, "btcusdt@bookTicker") {
		t.Fatalf("unexpected public streams in live mode: %v", pubLive)
	}
	if slices.Contains(mktLive, "ethusdt@aggTrade") || slices.Contains(mktLive, "btcusdt@aggTrade") {
		t.Fatalf("expected @aggTrade omitted in live mode, got %v", mktLive)
	}
	if len(mktLive) != 4 || !slices.Contains(mktLive, "ethusdt@markPrice@1s") || !slices.Contains(mktLive, "ethusdt@kline_5m") {
		t.Fatalf("unexpected market streams in live mode: %v", mktLive)
	}

	_, mktDryRun := buildMarketStreamSubscriptions([]string{"ETHUSDT"}, "5m", true)
	if !slices.Contains(mktDryRun, "ethusdt@aggTrade") || !slices.Contains(mktDryRun, "btcusdt@aggTrade") {
		t.Fatalf("expected @aggTrade included in dry-run mode, got %v", mktDryRun)
	}
}

func TestRunMarketScanRetainsPriorWatchlistOnFetchTimeout(t *testing.T) {
	var stallKlines atomic.Bool

	now := time.Now().UTC().Truncate(5 * time.Minute)
	makeKlinesPayload := func() [][]any {
		rows := make([][]any, 0, 260)
		for i := 0; i < 260; i++ {
			tOpen := now.Add(time.Duration(i-260) * 5 * time.Minute)
			tClose := tOpen.Add(5 * time.Minute)
			rows = append(rows, []any{
				float64(tOpen.UnixMilli()),
				"100.0", "101.0", "99.0", "100.2",
				"500.0",
				float64(tClose.UnixMilli()),
				"50000.0",
				float64(10),
				"250.0",
				"25000.0",
			})
		}
		return rows
	}
	klinesRows := makeKlinesPayload()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"symbols": []map[string]any{
					{
						"symbol": "ETHUSDT", "contractType": "PERPETUAL", "status": "TRADING",
						"filters": []map[string]string{
							{"filterType": "PRICE_FILTER", "tickSize": "0.01"},
							{"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
							{"filterType": "MIN_NOTIONAL", "notional": "5.0"},
						},
					},
					{
						"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING",
						"filters": []map[string]string{
							{"filterType": "PRICE_FILTER", "tickSize": "0.1"},
							{"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
							{"filterType": "MIN_NOTIONAL", "notional": "5.0"},
						},
					},
				},
			})
		case "/fapi/v1/ticker/24hr":
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"symbol": "ETHUSDT", "quoteVolume": "100000000"},
			})
		case "/fapi/v1/ticker/bookTicker":
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"symbol": "ETHUSDT", "bidPrice": "100.00", "askPrice": "100.01"},
			})
		case "/fapi/v1/klines":
			if stallKlines.Load() {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
			_ = json.NewEncoder(w).Encode(klinesRows)
		case "/futures/data/openInterestHist":
			sym := r.URL.Query().Get("symbol")
			_ = json.NewEncoder(w).Encode([]exchange.OpenInterestHistItem{
				{Symbol: sym, SumOpenInterest: "1000", Timestamp: now.Add(-6 * time.Minute).UnixMilli()},
				{Symbol: sym, SumOpenInterest: "1010", Timestamp: now.Add(-1 * time.Minute).UnixMilli()},
			})
		case "/fapi/v1/openInterest":
			_ = json.NewEncoder(w).Encode(map[string]string{"symbol": r.URL.Query().Get("symbol"), "openInterest": "1020"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := exchange.NewClient(ts.URL, "", "")
	cfg := &config.Config{
		DryRun:                true,
		Timeframe:             "5m",
		CandleDuration:        5 * time.Minute,
		MaxOIInterval:         10 * time.Minute,
		MinVolume24hUSDT:      15_000_000,
		MaxSpreadPct:          0.05,
		MinRVOL:               0.50,
		MinFallbackRVOL:       0.50,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		MaxScannerVWAPDistATR: 2.50,
		MaxWatchedCoins:       10,
	}
	eng := &BotEngine{
		cfg:             cfg,
		client:          client,
		activeStates:    make(map[string]*model.SymbolState),
		cooldownExits:   make(map[string]storage.RecentExitRecord),
		verifiedSymbols: make(map[string]bool),
		oiBootAttemptAt: make(map[string]time.Time),
		scannerCfg:      scanner.FromAppConfig(cfg, 1000),
	}
	defer func() {
		eng.streamMu.Lock()
		if eng.streamCancel != nil {
			eng.streamCancel()
		}
		eng.streamMu.Unlock()
	}()

	// Scan awal sukses memilih ETHUSDT
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	eng.runMarketScan(baseCtx)
	if !slices.Equal(eng.watchedCoins, []string{"ETHUSDT"}) {
		t.Fatalf("expected initial scan to select [ETHUSDT], got %v", eng.watchedCoins)
	}

	// Scan kedua mengalami timeout di tengah fetch klines -> prior watchlist harus tetap dipertahankan
	stallKlines.Store(true)
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer timeoutCancel()
	eng.runMarketScan(timeoutCtx)

	if !slices.Equal(eng.watchedCoins, []string{"ETHUSDT"}) {
		t.Fatalf("expected prior watchlist [ETHUSDT] retained on scan timeout, got %v", eng.watchedCoins)
	}
}
