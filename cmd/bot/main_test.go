package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trml/internal/broker"
	"trml/internal/config"
	"trml/internal/exchange"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/storage"
	"trml/internal/strategy"
	"trml/internal/ws"
	"github.com/shopspring/decimal"
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

func TestBotEngine_RepegConfigAndGetBestQuoteAndAttachAudit(t *testing.T) {
	if resolveBrokerRepegAttempts(0) != -1 || resolveBrokerRepegAttempts(-3) != -1 {
		t.Fatalf("expected <=0 config attempts to map to -1 (explicit disable)")
	}
	if resolveBrokerRepegAttempts(3) != 3 {
		t.Fatalf("expected positive attempts preserved")
	}

	eng := &BotEngine{
		cfg: &config.Config{
			MaxBookStaleness: 5 * time.Second,
		},
		activeStates: make(map[string]*model.SymbolState),
	}

	if _, _, ok := eng.getBestQuote("MISSINGUSDT"); ok {
		t.Fatalf("expected ok=false for missing symbol")
	}

	now := time.Now().UTC()
	st := &model.SymbolState{
		Symbol:        "ETHUSDT",
		BestBid:       2500.1,
		BestAsk:       2500.2,
		BookUpdatedAt: now.Add(-10 * time.Second), // Stale > 5s
	}
	eng.activeStates["ETHUSDT"] = st
	if _, _, ok := eng.getBestQuote("ETHUSDT"); ok {
		t.Fatalf("expected ok=false for stale BBO quote")
	}

	st.Mu.Lock()
	st.BookUpdatedAt = now.Add(-500 * time.Millisecond)
	st.Mu.Unlock()
	bid, ask, ok := eng.getBestQuote("ETHUSDT")
	if !ok || bid != 2500.1 || ask != 2500.2 {
		t.Fatalf("expected fresh quote (2500.1, 2500.2, true), got (%v, %v, %v)", bid, ask, ok)
	}

	// Uji attachSignalAuditID ke DryRunBroker secara paralel/asinkron
	dry := broker.NewDryRunBroker(decimal.NewFromFloat(1000.0), broker.DryRunConfig{
		MaxSlots:        3,
		AccountLeverage: 8.0,
	})
	defer dry.Close()
	eng.broker = dry

	intent := model.OrderIntent{
		ClientOrderID: "BOT_ASYNC_1",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1),
		LimitPrice:    decimal.NewFromFloat(2500.2),
		SLPrice:       decimal.NewFromFloat(2550.0),
		TPPrice:       decimal.NewFromFloat(2400.0),
		ATRAtEntry:    decimal.NewFromFloat(25.0),
		CreatedAt:     now,
	}
	if ok, err := dry.SubmitLimitEntry(intent); !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}
	eng.attachSignalAuditID("ETHUSDT", "BOT_ASYNC_1", 9123)

	dry.Mu.Lock()
	gotAuditID := dry.PendingOrders["ETHUSDT"].Intent.SignalAuditID
	dry.Mu.Unlock()
	if gotAuditID != 9123 {
		t.Fatalf("expected SignalAuditID=9123 attached on pending order, got %d", gotAuditID)
	}
}

func TestBotEngine_FreshCachedMarkPriceLocked(t *testing.T) {
	eng := &BotEngine{
		cfg: &config.Config{
			MaxMarkStaleness: 15 * time.Second,
		},
	}
	now := time.Now().UTC()
	st := &model.SymbolState{
		Symbol:             "ETHUSDT",
		MarkPrice:          2500.0,
		MarkPriceUpdatedAt: now.Add(-20 * time.Second), // Stale > 15s
	}
	if mp := eng.freshCachedMarkPriceLocked(st, now); mp != 0 {
		t.Fatalf("expected 0 for stale cached mark price, got %v", mp)
	}

	st.MarkPriceUpdatedAt = now.Add(-2 * time.Second)
	if mp := eng.freshCachedMarkPriceLocked(st, now); mp != 2500.0 {
		t.Fatalf("expected 2500.0 for fresh cached mark price, got %v", mp)
	}
}

func TestBotEngine_VerifyLiveWatchlist_PreservesMonitoredSymbolOnFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	live := broker.NewBinanceLiveBroker(broker.LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
	})
	defer live.Close()

	// ADAUSDT sudah punya posisi terbuka di live broker; XRPUSDT hanya kandidat scan
	live.Mu.Lock()
	live.ActivePositions["ADAUSDT"] = decimal.NewFromFloat(10.0)
	live.Mu.Unlock()

	eng := &BotEngine{
		cfg: &config.Config{
			DryRun:   false,
			Leverage: 5.0,
		},
		client:          exchange.NewClient(ts.URL, "KEY", "SECRET"),
		broker:          live,
		liveBroker:      live,
		activeStates:    make(map[string]*model.SymbolState),
		verifiedSymbols: make(map[string]bool),
		cooldownExits:   make(map[string]storage.RecentExitRecord),
	}
	eng.activeStates["ADAUSDT"] = model.NewSymbolState("ADAUSDT", model.ExchangeFilter{}, 10, 10)
	eng.activeStates["XRPUSDT"] = model.NewSymbolState("XRPUSDT", model.ExchangeFilter{}, 10, 10)

	got := eng.verifyLiveWatchlist(context.Background(), []string{"ADAUSDT", "XRPUSDT"})
	if !slices.Contains(got, "ADAUSDT") {
		t.Fatalf("expected actively monitored ADAUSDT preserved in watchlist despite setup failure, got %v", got)
	}
	if slices.Contains(got, "XRPUSDT") {
		t.Fatalf("expected unmonitored failed XRPUSDT excluded from watchlist, got %v", got)
	}
	if eng.activeStates["ADAUSDT"] == nil || eng.activeStates["ADAUSDT"].IsLeverageIsolatedVerified {
		t.Fatalf("expected ADAUSDT activeState retained with IsLeverageIsolatedVerified=false")
	}
	if _, exists := eng.activeStates["XRPUSDT"]; exists {
		t.Fatalf("expected unmonitored failed XRPUSDT deleted from activeStates")
	}
}

func TestBotEngine_RecordSymbolExit_SyncsCooldownAndState(t *testing.T) {
	now := time.Now().UTC()
	st := model.NewSymbolState("SOLUSDT", model.ExchangeFilter{}, 10, 10)
	eng := &BotEngine{
		activeStates:  map[string]*model.SymbolState{"SOLUSDT": st},
		cooldownExits: make(map[string]storage.RecentExitRecord),
	}

	eng.recordSymbolExit("SOLUSDT", now, "STOP_LOSS")
	if st.LastExitTime.IsZero() || st.LastExitReason != "STOP_LOSS" {
		t.Fatalf("expected SymbolState exit recorded, got time=%v reason=%q", st.LastExitTime, st.LastExitReason)
	}
	if rec, ok := eng.cooldownExits["SOLUSDT"]; !ok || rec.ExitReason != "STOP_LOSS" {
		t.Fatalf("expected cooldownExits recorded for SOLUSDT, got %+v ok=%v", rec, ok)
	}

	// PARTIAL_TAKE_PROFIT tidak boleh memicu cooldown penuh
	eng.recordSymbolExit("SOLUSDT", now.Add(time.Minute), "PARTIAL_TAKE_PROFIT")
	if eng.cooldownExits["SOLUSDT"].ExitReason != "STOP_LOSS" {
		t.Fatalf("expected PARTIAL_TAKE_PROFIT ignored for cooldown, got %+v", eng.cooldownExits["SOLUSDT"])
	}
}

func TestBotEngine_EvaluateAndLogClosedCandle_PartitionScopeAndStrategyAudit(t *testing.T) {
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := storage.NewRepository(db)

	now := time.Now().UTC()
	count := 80
	candles := make([]model.Candle, count)
	deltas := make([]float64, count)
	for i := 0; i < count; i++ {
		tClose := now.Add(-time.Duration(count-1-i) * 5 * time.Minute)
		tOpen := tClose.Add(-5 * time.Minute)
		closeP := 120.0 - float64(i)*0.20
		openP := closeP + 0.08
		candles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose,
			Open:            openP,
			High:            openP + 0.15,
			Low:             closeP - 0.15,
			Close:           closeP,
			VolumeBase:      1000.0,
			QuoteVolumeUSDT: 1000.0 * closeP,
		}
		if i%2 == 0 {
			deltas[i] = 100.0
		} else {
			deltas[i] = -100.0
		}
	}

	priorHigh := 0.0
	for i := count - 1 - 12; i < count-1; i++ {
		if candles[i].High > priorHigh {
			priorHigh = candles[i].High
		}
	}
	latest := &candles[count-1]
	latest.Open = priorHigh - 0.30
	latest.Close = priorHigh - 1.20
	latest.High = priorHigh + 0.60
	latest.Low = latest.Close - 0.10
	deltas[count-1] = -80.0

	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}

	buildSweepState := func() *model.SymbolState {
		st := model.NewSymbolState("SOLUSDT", filter, count+10, count+10)
		st.IsLeverageIsolatedVerified = true
		st.BestBid = latest.Close - 0.01
		st.BestAsk = latest.Close + 0.01
		st.BookUpdatedAt = time.Now().UTC()
		st.MarkPrice = latest.Close
		st.MarkPriceUpdatedAt = time.Now().UTC()
		st.FundingRate = 0.0001
		st.FundingUpdatedAt = time.Now().UTC()
		st.CurrentSessionVWAP = priorHigh + 5.0
		st.ClosedCandles = append([]model.Candle(nil), candles...)
		st.ClosedBarDeltasUSDT = append([]float64(nil), deltas...)
		return st
	}

	dry := broker.NewDryRunBroker(decimal.NewFromFloat(500.0), broker.DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		AccountLeverage:     8.0,
	})
	defer dry.Close()

	evalCfg := strategy.EvaluatorConfig{
		CandleDuration:        5 * time.Minute,
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.10,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBounceZScore:       0.75,
		MinBounceZScoreLong:   0.50,
		MinBounceZScoreShort:  0.70,
		MinEMA200DistanceATR:  0.20,
		MaxEMA200DistanceATR:  3.80,
		LookbackImpulseBars:   18,
		RetestLookbackBars:    5,
		MinImpulseATR:         0.62,
		MaxEntryDistanceATR:   1.20,
		PullbackToleranceATR:  0.38,
		EntryOrderType:        "LIMIT",
		DisableOIFilter:       true,
		MaxAdverseFundingRate: 0.0005,
		AllowedSide:           "AUTO",
		MaxFrenzyRVOL:         2.50,
		MaxCounterImpulseATR:  1.50,
		Sweep: strategy.SweepConfig{
			Enabled:           false,
			LookbackBars:      12,
			MinUpperWickRatio: 0.25,
			MaxLowerWickRatio: 0.45,
			MaxDeltaZScore:    -0.25,
		},
		Sizing: strategy.SizingConfig{
			RiskPerTradePct:                decimal.NewFromFloat(0.015),
			MaxSlotMarginFraction:          decimal.NewFromFloat(0.20),
			AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
			AccountLeverage:                8.0,
			SLMultiplier:                   1.65,
			TPMultiplier:                   3.60,
			TP1Multiplier:                  1.25,
			MinSLDistancePct:               decimal.NewFromFloat(0.01),
			MakerFeeRate:                   decimal.NewFromFloat(0.0002),
			TakerFeeRate:                   decimal.NewFromFloat(0.0005),
			SlippageRate:                   decimal.NewFromFloat(0.0002),
			EntryOrderType:                 "LIMIT",
		},
	}

	eng := &BotEngine{
		cfg: &config.Config{
			DryRun:           true,
			MaxDailyDrawdown: -0.05,
		},
		repo:                 repo,
		broker:               dry,
		dryRunBroker:         dry,
		activeStates:         make(map[string]*model.SymbolState),
		primaryWatchedSet:    map[string]bool{"ETHUSDT": true},
		sweepExtraWatchedSet: map[string]bool{"SOLUSDT": true},
		cooldownExits:        make(map[string]storage.RecentExitRecord),
		verifiedSymbols:      make(map[string]bool),
		evaluatorCfg:         evalCfg,
	}

	// 1. Sweep disabled -> SOLUSDT (in sweepExtra only) must NOT trigger sweep
	eng.evaluateAndLogClosedCandle(*latest, buildSweepState())
	var dec1, strat1 string
	if err := db.RawConn().QueryRow("SELECT decision, strategy_id FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&dec1, &strat1); err != nil {
		t.Fatalf("failed to query signal_audit_log (1): %v", err)
	}
	if dec1 != "REJECT" {
		t.Fatalf("expected REJECT when Sweep.Enabled=false, got decision=%q strategy_id=%q", dec1, strat1)
	}

	// 2. Sweep enabled -> SOLUSDT (sweepExtra only) approves via LIQUIDITY_SWEEP_SHORT and persists strategy_id
	eng.evaluatorCfg.Sweep.Enabled = true
	eng.evaluateAndLogClosedCandle(*latest, buildSweepState())

	// Wait briefly for async SubmitLimitEntry goroutine to settle before dry.Close()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && !dry.HasActiveOrderOrPosition("SOLUSDT") {
		time.Sleep(5 * time.Millisecond)
	}

	var dec2, reason2, strat2 string
	if err := db.RawConn().QueryRow("SELECT decision, reject_reason_code, strategy_id FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&dec2, &reason2, &strat2); err != nil {
		t.Fatalf("failed to query signal_audit_log (2): %v", err)
	}
	if dec2 != "APPROVE" || reason2 != "SIGNAL_APPROVED" || strat2 != strategy.StrategyIDLiquiditySweep {
		t.Fatalf("expected APPROVE SIGNAL_APPROVED %s, got decision=%q reason=%q strategy_id=%q", strategy.StrategyIDLiquiditySweep, dec2, reason2, strat2)
	}

	dry.Mu.Lock()
	po := dry.PendingOrders["SOLUSDT"]
	dry.Mu.Unlock()
	if po == nil || po.Intent.StrategyID != strategy.StrategyIDLiquiditySweep {
		t.Fatalf("expected pending order StrategyID=%s, got %+v", strategy.StrategyIDLiquiditySweep, po)
	}
}

func TestBotEngine_ProductionParity_Breadth_Drawdown_ClusterSL_BTCMacro_BarCap(t *testing.T) {
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := storage.NewRepository(db)

	now := time.Now().UTC().Truncate(15 * time.Minute)
	count := 100
	candles := make([]model.Candle, count)
	deltas := make([]float64, count)
	for i := 0; i < count; i++ {
		tClose := now.Add(-time.Duration(count-1-i) * 15 * time.Minute)
		tOpen := tClose.Add(-15 * time.Minute)
		closeP := 120.0 - float64(i)*0.25
		openP := closeP + 0.10
		candles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose.Add(-time.Millisecond),
			Open:            openP,
			High:            openP + 0.20,
			Low:             closeP - 0.15,
			Close:           closeP,
			VolumeBase:      1000.0,
			QuoteVolumeUSDT: 1000.0 * closeP,
		}
		if i%2 == 0 {
			deltas[i] = 100.0
		} else {
			deltas[i] = -100.0
		}
	}
	priorHigh := 0.0
	for i := count - 1 - 12; i < count-1; i++ {
		if candles[i].High > priorHigh {
			priorHigh = candles[i].High
		}
	}
	latest := &candles[count-1]
	latest.Open = priorHigh - 0.30
	latest.Close = priorHigh - 1.50
	latest.High = priorHigh + 0.60
	latest.Low = latest.Close - 0.10
	deltas[count-1] = -90.0

	// BTC 4h (+16 bars on 15m) pumped +4.0% so RS4h strong (0.50x) fires EVEN when BTCGateEnabled=false
	btcCandles := make([]model.Candle, 40)
	for i := 0; i < len(btcCandles); i++ {
		tClose := now.Add(-time.Duration(len(btcCandles)-1-i) * 15 * time.Minute)
		tOpen := tClose.Add(-15 * time.Minute)
		p := 50000.0
		if i >= len(btcCandles)-16 {
			p = 52000.0 // +4% over last 4h
		}
		btcCandles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose.Add(-time.Millisecond),
			Open:            p,
			High:            p + 10,
			Low:             p - 10,
			Close:           p,
			VolumeBase:      10,
			QuoteVolumeUSDT: 10 * p,
		}
	}
	btcSt := model.NewSymbolState(BenchmarkSymbol, model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.1)}, 100, 100)
	btcSt.BestBid = 51999.0
	btcSt.BestAsk = 52001.0
	btcSt.BookUpdatedAt = time.Now().UTC()
	btcSt.MarkPrice = 52000.0
	btcSt.MarkPriceUpdatedAt = time.Now().UTC()
	btcSt.FundingUpdatedAt = time.Now().UTC()
	btcSt.CurrentSessionVWAP = 51000.0
	btcSt.ClosedCandles = btcCandles

	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	makeSymState := func(sym string) *model.SymbolState {
		st := model.NewSymbolState(sym, filter, 150, 100)
		st.IsLeverageIsolatedVerified = true
		st.BestBid = latest.Close - 0.01
		st.BestAsk = latest.Close + 0.01
		st.BookUpdatedAt = time.Now().UTC()
		st.MarkPrice = latest.Close
		st.MarkPriceUpdatedAt = time.Now().UTC()
		st.FundingRate = 0.0001
		st.FundingUpdatedAt = time.Now().UTC()
		st.CurrentSessionVWAP = priorHigh + 5.0
		st.ClosedCandles = append([]model.Candle(nil), candles...)
		st.ClosedBarDeltasUSDT = append([]float64(nil), deltas...)
		return st
	}

	buildEngine := func(peakEq float64, maxPerBar int) (*BotEngine, *broker.DryRunBroker) {
		dry := broker.NewDryRunBroker(decimal.NewFromFloat(500.0), broker.DryRunConfig{
			MaxSlots:            5,
			MaxDailyDrawdownPct: -0.50,
			AccountLeverage:     10.0,
		})
		if peakEq > 500.0 {
			dry.SetPeakEffectiveEquityUSDT(decimal.NewFromFloat(peakEq))
		}
		evalCfg := strategy.EvaluatorConfig{
			CandleDuration:            15 * time.Minute,
			MaxSlots:                  5,
			MaxDailyDrawdownPct:       -0.50,
			MaxBookStaleness:          5 * time.Second,
			MaxMarkStaleness:          15 * time.Second,
			MaxDataStaleness:          930 * time.Second,
			MaxSpreadPct:              0.10,
			MinNATRPct:                0.10,
			MaxNATRPct:                5.00,
			EMAPeriod:                 50,
			ATRPeriod:                 14,
			DeltaZScoreWindow:         50,
			DisableOIFilter:           true,
			MaxAdverseFundingRate:     0.0005,
			AllowedSide:               "AUTO",
			MaxFrenzyRVOL:             2.50,
			MaxCounterImpulseATR:      1.50,
			EntryOrderType:            "LIMIT",
			DisableVWAP:               true,
			BTCGateEnabled:            false,
			MarketBreadthLowThreshold: 0.20,
			MarketBreadthLowRiskMult:  0.40,
			DDThrottleThresholdPct:    0.15, // 20% DD from 625->500 triggers 0.50x
			DDThrottleRiskMult:        0.50,
			RS4hStrongThresholdPct:    2.0,  // BTC +4% while alt drops triggers RS4h 0.50x even with BTCGateEnabled=false
			RS4hStrongRiskMult:        0.50,
			Sweep: strategy.SweepConfig{
				Enabled:           true,
				LookbackBars:      12,
				MinUpperWickRatio: 0.25,
				MaxLowerWickRatio: 0.45,
				MaxDeltaZScore:    -0.25,
			},
			Sizing: strategy.SizingConfig{
				RiskPerTradePct:                decimal.NewFromFloat(0.025),
				MaxSlotMarginFraction:          decimal.NewFromFloat(0.50),
				AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
				AccountLeverage:                10.0,
				SLMultiplier:                   2.00,
				TPMultiplier:                   5.00,
				MinSLDistancePct:               decimal.NewFromFloat(0.01),
				MakerFeeRate:                   decimal.NewFromFloat(0.0002),
				TakerFeeRate:                   decimal.NewFromFloat(0.0005),
				SlippageRate:                   decimal.NewFromFloat(0.0002),
				EntryOrderType:                 "LIMIT",
			},
		}
		eng := &BotEngine{
			cfg: &config.Config{
				DryRun:                true,
				EquityModeCompounding: true,
				MaxDailyDrawdown:      -0.50,
			},
			repo:                   repo,
			broker:                 dry,
			dryRunBroker:           dry,
			activeStates:           map[string]*model.SymbolState{BenchmarkSymbol: btcSt},
			primaryWatchedSet:      map[string]bool{"ETHUSDT": true, "ADAUSDT": true},
			breakoutWatchedSet:     make(map[string]bool),
			sweepExtraWatchedSet:   make(map[string]bool),
			cooldownExits:          make(map[string]storage.RecentExitRecord),
			verifiedSymbols:        make(map[string]bool),
			marketBreadthPct:       0.10, // <= 0.20 triggers 0.40x breadth throttle
			marketBreadthValid:     true,
			marketBreadthUpdatedAt: time.Now().UTC(),
			clusterSL:              strategy.NewClusterSLTracker(3, 90*time.Minute),
			barEntries:             strategy.NewBarEntryTracker(maxPerBar),
			evaluatorCfg:           evalCfg,
		}
		return eng, dry
	}

	// 1. Baseline (no DD throttle, no RS4h) vs Parity (DD=20% + RS4h strong + Breadth=0.10)
	engBase, dryBase := buildEngine(500.0, 1)
	engBase.marketBreadthPct = 0.50
	engBase.evaluatorCfg.RS4hStrongThresholdPct = 0
	engBase.evaluateAndLogClosedCandle(*latest, makeSymState("ETHUSDT"))

	engThrot, dryThrot := buildEngine(625.0, 1)
	engThrot.evaluateAndLogClosedCandle(*latest, makeSymState("ETHUSDT"))

	dryBase.Mu.Lock()
	baseOrd := dryBase.PendingOrders["ETHUSDT"]
	dryBase.Mu.Unlock()
	dryThrot.Mu.Lock()
	throtOrd := dryThrot.PendingOrders["ETHUSDT"]
	dryThrot.Mu.Unlock()
	if baseOrd == nil || throtOrd == nil {
		t.Fatalf("expected both baseline and throttled ETHUSDT orders submitted, got base=%v throt=%v", baseOrd, throtOrd)
	}
	if !throtOrd.Intent.Qty.LessThan(baseOrd.Intent.Qty.Mul(decimal.NewFromFloat(0.20))) {
		t.Fatalf("expected combined Breadth(0.4x)*DD(0.5x)*RS4h(0.5x)=0.1x qty << base qty, got base=%s throt=%s", baseOrd.Intent.Qty, throtOrd.Intent.Qty)
	}

	// 2. Same bar with MaxNewEntriesPerBar=1 must reject ADAUSDT with BAR_ENTRY_CAP_REACHED
	engThrot.evaluateAndLogClosedCandle(*latest, makeSymState("ADAUSDT"))
	var capReason string
	if err := db.RawConn().QueryRow("SELECT reject_reason_code FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&capReason); err != nil {
		t.Fatalf("query signal_audit_log failed: %v", err)
	}
	if capReason != "BAR_ENTRY_CAP_REACHED" {
		t.Fatalf("expected BAR_ENTRY_CAP_REACHED for 2nd signal on same bar, got %q", capReason)
	}

	// 3. 3 STOP_LOSS exits via recordSymbolExit must trigger PORTFOLIO_CLUSTER_SL_COOLDOWN on next bar
	exitBase := time.Now().UTC()
	engThrot.recordSymbolExit("X1USDT", exitBase.Add(-20*time.Minute), "STOP_LOSS")
	engThrot.recordSymbolExit("X2USDT", exitBase.Add(-10*time.Minute), "STOP_LOSS")
	engThrot.recordSymbolExit("X3USDT", exitBase.Add(-2*time.Minute), "STOP_LOSS")
	nextBar := *latest
	nextBar.OpenTime = latest.OpenTime.Add(15 * time.Minute)
	engThrot.evaluateAndLogClosedCandle(nextBar, makeSymState("ADAUSDT"))
	var slReason string
	if err := db.RawConn().QueryRow("SELECT reject_reason_code FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&slReason); err != nil {
		t.Fatalf("query signal_audit_log failed: %v", err)
	}
	if slReason != "PORTFOLIO_CLUSTER_SL_COOLDOWN" {
		t.Fatalf("expected PORTFOLIO_CLUSTER_SL_COOLDOWN after 3 STOP_LOSS exits, got %q", slReason)
	}

	dryBase.Close()
	dryThrot.Close()
}

func TestMarketStreamShardingUnderBinance200Limit(t *testing.T) {
	if maxMarketStreamsPerConn <= 0 || maxMarketStreamsPerConn >= 200 {
		t.Fatalf("maxMarketStreamsPerConn must stay strictly under Binance 200-stream limit, got %d", maxMarketStreamsPerConn)
	}
	symbols := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		symbols = append(symbols, fmt.Sprintf("ALT%03dUSDT", i))
	}

	// Dry-Run includes @aggTrade -> (100+1)*3 = 303 streams
	_, dryMarketStreams := buildMarketStreamSubscriptions(symbols, "15m", true)
	if len(dryMarketStreams) != 303 {
		t.Fatalf("expected 303 dry-run market streams (with BTCUSDT), got %d", len(dryMarketStreams))
	}
	drySharded := ws.NewShardedDynamicStream(ws.NewStreamManager(ws.EventHandlers{}), "wss://example.com", dryMarketStreams, maxMarketStreamsPerConn)
	expectedDryShards := (len(dryMarketStreams) + maxMarketStreamsPerConn - 1) / maxMarketStreamsPerConn
	if drySharded.ShardCount() != expectedDryShards || drySharded.ShardCount() < 2 {
		t.Fatalf("expected %d shards (<200 per shard) for %d dry-run streams, got %d", expectedDryShards, len(dryMarketStreams), drySharded.ShardCount())
	}

	// Live excludes @aggTrade -> (100+1)*2 = 202 streams
	_, liveMarketStreams := buildMarketStreamSubscriptions(symbols, "15m", false)
	if len(liveMarketStreams) != 202 {
		t.Fatalf("expected 202 live market streams (with BTCUSDT), got %d", len(liveMarketStreams))
	}
	liveSharded := ws.NewShardedDynamicStream(ws.NewStreamManager(ws.EventHandlers{}), "wss://example.com", liveMarketStreams, maxMarketStreamsPerConn)
	expectedLiveShards := (len(liveMarketStreams) + maxMarketStreamsPerConn - 1) / maxMarketStreamsPerConn
	if liveSharded.ShardCount() != expectedLiveShards || liveSharded.ShardCount() < 2 {
		t.Fatalf("expected %d shards (<200 per shard) for %d live streams, got %d", expectedLiveShards, len(liveMarketStreams), liveSharded.ShardCount())
	}
}

func TestBotEngine_WaitForBTCCandleSynchronizesDelayedBTC(t *testing.T) {
	eng := &BotEngine{}
	barOpen := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	// 1. BTC update arrives slightly later (35ms) -> waitForBTCCandle waits cleanly and wakes up early
	go func() {
		time.Sleep(35 * time.Millisecond)
		eng.notifyBTCCandleClosed(barOpen)
	}()
	start := time.Now()
	eng.waitForBTCCandle(barOpen, 800*time.Millisecond)
	elapsed := time.Since(start)
	if elapsed < 25*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("expected waitForBTCCandle to unblock right after delayed BTC bar (~35ms), took %v", elapsed)
	}

	// 2. Subsequent BTC never arrives for next bar -> bounded timeout prevents stall
	nextOpen := barOpen.Add(15 * time.Minute)
	start2 := time.Now()
	eng.waitForBTCCandle(nextOpen, 40*time.Millisecond)
	if elapsed2 := time.Since(start2); elapsed2 < 30*time.Millisecond || elapsed2 > 300*time.Millisecond {
		t.Fatalf("expected bounded wait timeout (~40ms), took %v", elapsed2)
	}
}

func TestBotEngine_BTCFullBootstrapRetryAfterInitialFailure(t *testing.T) {
	var failBTC atomic.Bool
	failBTC.Store(true)

	var btcMu sync.Mutex
	var btcLimits []int

	now := time.Now().UTC().Truncate(15 * time.Minute)
	makeRows := func(n int) [][]any {
		rows := make([][]any, 0, n)
		for i := 0; i < n; i++ {
			tOpen := now.Add(time.Duration(i-n) * 15 * time.Minute)
			tClose := tOpen.Add(15*time.Minute - time.Millisecond)
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

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"symbols": []map[string]any{
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
			_ = json.NewEncoder(w).Encode([]map[string]string{})
		case "/fapi/v1/ticker/bookTicker":
			_ = json.NewEncoder(w).Encode([]map[string]string{})
		case "/fapi/v1/klines":
			sym := r.URL.Query().Get("symbol")
			if sym == "BTCUSDT" {
				var lim int
				_, _ = fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &lim)
				btcMu.Lock()
				btcLimits = append(btcLimits, lim)
				btcMu.Unlock()
				if failBTC.Load() {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_ = json.NewEncoder(w).Encode(makeRows(lim))
				return
			}
			_ = json.NewEncoder(w).Encode([][]any{})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := exchange.NewClient(ts.URL, "", "")
	eng := &BotEngine{
		cfg: &config.Config{
			DryRun:          true,
			Timeframe:       "15m",
			CandleDuration:  15 * time.Minute,
			DisableOIFilter: true,
			MaxSpreadPct:    0.10,
		},
		client:               client,
		activeStates:         make(map[string]*model.SymbolState),
		primaryWatchedSet:    make(map[string]bool),
		breakoutWatchedSet:   make(map[string]bool),
		sweepExtraWatchedSet: make(map[string]bool),
		exchangeFilters:      make(map[string]model.ExchangeFilter),
		cooldownExits:        make(map[string]storage.RecentExitRecord),
		verifiedSymbols:      make(map[string]bool),
		oiBootAttemptAt:      make(map[string]time.Time),
		scannerCfg: scanner.ScannerConfig{
			Timeframe:      "15m",
			CandleDuration: 15 * time.Minute,
			BootstrapCount: 1280,
			DeltaWindow:    50,
			EMAPeriod:      200,
		},
	}

	// Scan #1: BTCUSDT fails -> BTC state exists with 0 candles
	eng.runMarketScan(context.Background())

	// Scan #2: BTCUSDT recovers -> must STILL request full BootstrapCount (1280), NOT 100!
	failBTC.Store(false)
	eng.runMarketScan(context.Background())

	// Scan #3: BTCUSDT now has 1280 candles -> drops down to incremental 100!
	eng.runMarketScan(context.Background())

	btcMu.Lock()
	defer btcMu.Unlock()
	if len(btcLimits) < 3 {
		t.Fatalf("expected at least 3 BTCUSDT kline fetch attempts, got %v", btcLimits)
	}
	if btcLimits[0] != 1280 || btcLimits[len(btcLimits)-2] != 1280 || btcLimits[len(btcLimits)-1] != 100 {
		t.Fatalf("expected full retry 1280 after failure then incremental 100 once warmed up, got sequence %v", btcLimits)
	}
}

func TestRestartMarketStream_SkipsWhenParentContextCanceled(t *testing.T) {
	eng := &BotEngine{
		cfg: &config.Config{
			DryRun:      true,
			Timeframe:   "15m",
			WSPublicURL: "wss://invalid.example.test",
			WSMarketURL: "wss://invalid.example.test",
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	eng.restartMarketStream(ctx, []string{"ETHUSDT"})

	eng.streamMu.Lock()
	hasPub := eng.publicDynStream != nil
	hasMkt := eng.marketDynStream != nil
	hasCancel := eng.streamCancel != nil
	eng.streamMu.Unlock()
	if hasPub || hasMkt || hasCancel {
		t.Fatalf("expected canceled parentCtx not to spawn streams or WaitGroup work")
	}
	eng.streamWg.Wait()
}

func TestRunMarketScan_CleanEmptyWatchlistClearsOldAndKeepsBTCAlive(t *testing.T) {
	now := time.Now().UTC().Truncate(5 * time.Minute)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"symbols": []map[string]any{
					{"symbol": "ETHUSDT", "contractType": "PERPETUAL", "status": "TRADING", "filters": []map[string]string{{"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"}}},
					{"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "filters": []map[string]string{{"filterType": "PRICE_FILTER", "tickSize": "0.1"}, {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}}},
				},
			})
		case "/fapi/v1/ticker/24hr":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"symbol": "ETHUSDT", "quoteVolume": "1000"}}) // Below MinVolume -> clean 0-coin watchlist
		case "/fapi/v1/ticker/bookTicker":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"symbol": "ETHUSDT", "bidPrice": "100", "askPrice": "100.01"}})
		case "/fapi/v1/klines":
			_ = json.NewEncoder(w).Encode([][]any{})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := exchange.NewClient(ts.URL, "", "")
	eng := &BotEngine{
		cfg: &config.Config{
			DryRun:          true,
			Timeframe:       "5m",
			CandleDuration:  5 * time.Minute,
			DisableOIFilter: true,
			MaxSpreadPct:    0.10,
			WSPublicURL:     "wss://invalid.example.test",
			WSMarketURL:     "wss://invalid.example.test",
		},
		client: client,
		activeStates: map[string]*model.SymbolState{
			"OLDUSDT": model.NewSymbolState("OLDUSDT", model.ExchangeFilter{}, 10, 10),
			"BTCUSDT": model.NewSymbolState("BTCUSDT", model.ExchangeFilter{}, 10, 10),
		},
		watchedCoins:         []string{"OLDUSDT"},
		primaryWatchedSet:    map[string]bool{"OLDUSDT": true},
		breakoutWatchedSet:   map[string]bool{"OLDUSDT": true},
		sweepExtraWatchedSet: map[string]bool{"OLDUSDT": true},
		exchangeFilters:      make(map[string]model.ExchangeFilter),
		cooldownExits:        make(map[string]storage.RecentExitRecord),
		verifiedSymbols:      make(map[string]bool),
		oiBootAttemptAt:      map[string]time.Time{"OLDUSDT": now},
		scannerCfg: scanner.ScannerConfig{
			MinVolume24hUSDT: 50_000_000,
			MaxSpreadPct:     0.10,
			EMAPeriod:        50,
			Timeframe:        "5m",
			CandleDuration:   5 * time.Minute,
			BootstrapCount:   100,
			DeltaWindow:      50,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	eng.runMarketScan(ctx)

	eng.statesMu.RLock()
	watchedLen := len(eng.watchedCoins)
	_, hasOld := eng.activeStates["OLDUSDT"]
	_, hasBTC := eng.activeStates["BTCUSDT"]
	primLen := len(eng.primaryWatchedSet)
	boLen := len(eng.breakoutWatchedSet)
	swLen := len(eng.sweepExtraWatchedSet)
	eng.statesMu.RUnlock()

	eng.streamMu.Lock()
	hasStreamCancel := eng.streamCancel != nil
	eng.streamMu.Unlock()

	cancel()
	if eng.streamCancel != nil {
		eng.streamCancel()
	}
	eng.streamWg.Wait()

	if watchedLen != 0 || primLen != 0 || boLen != 0 || swLen != 0 || hasOld {
		t.Fatalf("expected clean empty watchlist to clear old watchedCoins/sets/OLDUSDT, got watched=%d prim=%d bo=%d sw=%d hasOld=%v", watchedLen, primLen, boLen, swLen, hasOld)
	}
	if !hasBTC || !hasStreamCancel {
		t.Fatalf("expected BTCUSDT state and WS stream to stay alive on empty alt watchlist, hasBTC=%v hasStreamCancel=%v", hasBTC, hasStreamCancel)
	}
}

func TestVerifyLiveWatchlist_NeverDeletesBenchmarkSymbol(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	eng := &BotEngine{
		cfg:                  &config.Config{DryRun: false, Leverage: 5},
		client:               exchange.NewClient(ts.URL, "K", "S"),
		activeStates:         map[string]*model.SymbolState{BenchmarkSymbol: model.NewSymbolState(BenchmarkSymbol, model.ExchangeFilter{}, 10, 10), "ETHUSDT": model.NewSymbolState("ETHUSDT", model.ExchangeFilter{}, 10, 10)},
		primaryWatchedSet:    map[string]bool{"ETHUSDT": true},
		breakoutWatchedSet:   make(map[string]bool),
		sweepExtraWatchedSet: make(map[string]bool),
		verifiedSymbols:      make(map[string]bool),
	}
	eng.verifyLiveWatchlist(context.Background(), []string{BenchmarkSymbol, "ETHUSDT"})
	eng.statesMu.RLock()
	_, hasBTC := eng.activeStates[BenchmarkSymbol]
	_, hasETH := eng.activeStates["ETHUSDT"]
	eng.statesMu.RUnlock()
	if !hasBTC || hasETH {
		t.Fatalf("expected BenchmarkSymbol preserved and failing ETHUSDT pruned, hasBTC=%v hasETH=%v", hasBTC, hasETH)
	}
}

func TestHandleGappedClosedCandle_BackfillsMissingBarsAndFailsClosedOnError(t *testing.T) {
	base := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	step := 15 * time.Minute
	var returnErr atomic.Bool

	makeRow := func(idx int) []any {
		ot := base.Add(time.Duration(idx) * step)
		ct := ot.Add(step - time.Millisecond)
		return []any{float64(ot.UnixMilli()), "100.0", "101.0", "99.0", "100.5", "200.0", float64(ct.UnixMilli()), "20000.0", float64(10), "100.0", "10000.0"}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if returnErr.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var rows [][]any
		for i := 0; i <= 4; i++ {
			rows = append(rows, makeRow(i))
		}
		_ = json.NewEncoder(w).Encode(rows)
	}))
	defer ts.Close()

	eng := &BotEngine{
		cfg:    &config.Config{DryRun: true, Timeframe: "15m", CandleDuration: step},
		client: exchange.NewClient(ts.URL, "", ""),
		scannerCfg: scanner.ScannerConfig{
			Timeframe:      "15m",
			CandleDuration: step,
			BootstrapCount: 100,
			DeltaWindow:    50,
		},
	}

	// 1. Fail-closed on REST error: gap trigger candle must NOT be appended
	returnErr.Store(true)
	stFail := model.NewSymbolState(BenchmarkSymbol, model.ExchangeFilter{}, 100, 51)
	c0 := model.Candle{OpenTime: base, CloseTime: base.Add(step - time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100.5, VolumeBase: 200}
	stFail.ApplyClosedCandleLocked(c0, 100, 51)
	trigger := model.Candle{OpenTime: base.Add(4 * step), CloseTime: base.Add(5*step - time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100.5, VolumeBase: 200}
	eng.handleGappedClosedCandle(context.Background(), BenchmarkSymbol, stFail, trigger)
	if len(stFail.ClosedCandles) != 1 || !stFail.LastProcessedCandleOpenTime.Equal(base) {
		t.Fatalf("expected fail-closed on backfill error without appending gapped candle, got len=%d last=%v", len(stFail.ClosedCandles), stFail.LastProcessedCandleOpenTime)
	}

	// 2. Success path: backfills missing bars 1..3 cleanly and wakes BTC notification
	returnErr.Store(false)
	eng.handleGappedClosedCandle(context.Background(), BenchmarkSymbol, stFail, trigger)
	if len(stFail.ClosedCandles) != 5 || !stFail.LastProcessedCandleOpenTime.Equal(trigger.OpenTime) || stFail.HasRecentGapLocked(step) {
		t.Fatalf("expected contiguous 5 bars after backfill, got len=%d last=%v hasGap=%v", len(stFail.ClosedCandles), stFail.LastProcessedCandleOpenTime, stFail.HasRecentGapLocked(step))
	}
}

func TestPollSymbolOpenInterest_Uses15mPeriodAndExchangeClock(t *testing.T) {
	var seenPeriod string
	exchangeNow := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
	offsetMs := time.Until(exchangeNow).Milliseconds()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/futures/data/openInterestHist":
			seenPeriod = r.URL.Query().Get("period")
			_ = json.NewEncoder(w).Encode([]exchange.OpenInterestHistItem{
				{Symbol: "ETHUSDT", SumOpenInterest: "1000.0", Timestamp: exchangeNow.Add(-15 * time.Minute).UnixMilli()},
				{Symbol: "ETHUSDT", SumOpenInterest: "1020.0", Timestamp: exchangeNow.UnixMilli()},
			})
		case "/fapi/v1/openInterest":
			_ = json.NewEncoder(w).Encode(map[string]string{"symbol": "ETHUSDT", "openInterest": "1030.0"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := exchange.NewClient(ts.URL, "", "")
	client.TimeOffset = offsetMs
	st := model.NewSymbolState("ETHUSDT", model.ExchangeFilter{}, 10, 10)
	st.TargetOIInterval = 15 * time.Minute
	eng := &BotEngine{
		cfg:             &config.Config{Timeframe: "15m", CandleDuration: 15 * time.Minute, MaxOIInterval: 30 * time.Minute},
		client:          client,
		activeStates:    map[string]*model.SymbolState{"ETHUSDT": st},
		oiBootAttemptAt: make(map[string]time.Time),
	}

	eng.pollSymbolOpenInterest(context.Background(), "ETHUSDT")
	if seenPeriod != "15m" {
		t.Fatalf("expected period=15m sent to openInterestHist, got %q", seenPeriod)
	}
	if st.PrevOpenInterest != 1000.0 || st.CurrOpenInterest != 1030.0 {
		t.Fatalf("expected exchange-clock-aligned OI pair 1000->1030, got prev=%v curr=%v", st.PrevOpenInterest, st.CurrOpenInterest)
	}
}

func TestShouldRetainWatchlistOnFetchOutage(t *testing.T) {
	// Retain prior watchlist when all pre-filtered kline fetches failed (even if open positions kept finalWatched non-empty).
	outage := scanner.ScanStats{PreFilterPassed: 50, KlineFetchFailed: 50, KlineFetchOrShort: 0}
	if !shouldRetainWatchlistOnFetchOutage(true, true, outage) {
		t.Fatalf("expected retention when KlineFetchFailed == PreFilterPassed")
	}
	mixed := scanner.ScanStats{PreFilterPassed: 50, KlineFetchFailed: 45, KlineFetchOrShort: 5}
	if !shouldRetainWatchlistOnFetchOutage(true, true, mixed) {
		t.Fatalf("expected retention when all pre-filtered symbols failed fetch or short")
	}

	// Do NOT retain on a healthy scan where all symbols merely returned short candles (KlineFetchFailed == 0).
	healthyShortOnly := scanner.ScanStats{PreFilterPassed: 50, KlineFetchFailed: 0, KlineFetchOrShort: 50}
	if shouldRetainWatchlistOnFetchOutage(true, true, healthyShortOnly) {
		t.Fatalf("expected NO retention when KlineFetchFailed == 0")
	}
	// Do NOT retain when at least one scanned candidate succeeded.
	if shouldRetainWatchlistOnFetchOutage(true, false, outage) {
		t.Fatalf("expected NO retention when scanned candidates exist")
	}
}

func TestBotEngine_EmptyScanBlocksDefaultSymbolScope(t *testing.T) {
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := storage.NewRepository(db)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"symbols": []map[string]any{
					{"symbol": "SOLUSDT", "contractType": "PERPETUAL", "status": "TRADING", "filters": []map[string]string{{"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1"}}},
					{"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "filters": []map[string]string{{"filterType": "PRICE_FILTER", "tickSize": "0.1"}, {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}}},
				},
			})
		case "/fapi/v1/ticker/24hr":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"symbol": "SOLUSDT", "quoteVolume": "1000"}})
		case "/fapi/v1/ticker/bookTicker":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"symbol": "SOLUSDT", "bidPrice": "100", "askPrice": "100.01"}})
		case "/fapi/v1/klines":
			_ = json.NewEncoder(w).Encode([][]any{})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	now := time.Now().UTC().Truncate(5 * time.Minute)
	count := 80
	candles := make([]model.Candle, count)
	deltas := make([]float64, count)
	for i := 0; i < count; i++ {
		tClose := now.Add(-time.Duration(count-1-i) * 5 * time.Minute)
		tOpen := tClose.Add(-5 * time.Minute)
		closeP := 120.0 - float64(i)*0.20
		openP := closeP + 0.08
		candles[i] = model.Candle{
			OpenTime: tOpen, CloseTime: tClose.Add(-time.Millisecond),
			Open: openP, High: openP + 0.15, Low: closeP - 0.15, Close: closeP,
			VolumeBase: 1000, QuoteVolumeUSDT: 1000 * closeP,
		}
		if i%2 == 0 {
			deltas[i] = 100
		} else {
			deltas[i] = -100
		}
	}
	priorHigh := 0.0
	for i := count - 1 - 12; i < count-1; i++ {
		if candles[i].High > priorHigh {
			priorHigh = candles[i].High
		}
	}
	latest := &candles[count-1]
	latest.Open = priorHigh - 0.30
	latest.Close = priorHigh - 1.20
	latest.High = priorHigh + 0.60
	latest.Low = latest.Close - 0.10
	deltas[count-1] = -80

	filter := model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1),
		MinQty: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5.0),
	}
	st := model.NewSymbolState("SOLUSDT", filter, count+10, count+10)
	st.IsLeverageIsolatedVerified = true
	st.BestBid = latest.Close - 0.01
	st.BestAsk = latest.Close + 0.01
	st.BookUpdatedAt = time.Now().UTC()
	st.MarkPrice = latest.Close
	st.MarkPriceUpdatedAt = time.Now().UTC()
	st.FundingRate = 0.0001
	st.FundingUpdatedAt = time.Now().UTC()
	st.CurrentSessionVWAP = priorHigh + 5.0
	st.ClosedCandles = candles
	st.ClosedBarDeltasUSDT = deltas

	dry := broker.NewDryRunBroker(decimal.NewFromFloat(500), broker.DryRunConfig{
		MaxSlots: 3, MaxDailyDrawdownPct: -0.05, AccountLeverage: 8.0,
	})
	defer dry.Close()

	client := exchange.NewClient(ts.URL, "", "")
	eng := &BotEngine{
		cfg: &config.Config{
			DryRun: true, Timeframe: "5m", CandleDuration: 5 * time.Minute,
			DisableOIFilter: true, MaxDailyDrawdown: -0.05,
			WSPublicURL: "wss://invalid.example.test", WSMarketURL: "wss://invalid.example.test",
		},
		repo: repo, client: client, broker: dry, dryRunBroker: dry,
		activeStates: map[string]*model.SymbolState{
			"SOLUSDT":       st,
			BenchmarkSymbol: model.NewSymbolState(BenchmarkSymbol, filter, 10, 10),
		},
		exchangeFilters: make(map[string]model.ExchangeFilter),
		cooldownExits:   make(map[string]storage.RecentExitRecord),
		verifiedSymbols: make(map[string]bool),
		oiBootAttemptAt: make(map[string]time.Time),
		scannerCfg: scanner.ScannerConfig{
			MinVolume24hUSDT: 50_000_000, MaxSpreadPct: 0.10, EMAPeriod: 50,
			Timeframe: "5m", CandleDuration: 5 * time.Minute, BootstrapCount: 100, DeltaWindow: 50,
		},
		evaluatorCfg: strategy.EvaluatorConfig{
			CandleDuration: 5 * time.Minute, MaxSlots: 3, MaxBookStaleness: 5 * time.Second,
			MaxMarkStaleness: 15 * time.Second, MaxDataStaleness: 330 * time.Second, MaxSpreadPct: 0.05,
			MinNATRPct: 0.10, MaxNATRPct: 5.0, EMAPeriod: 50, ATRPeriod: 14, DeltaZScoreWindow: 50,
			DisableOIFilter: true, MaxAdverseFundingRate: 0.0005, AllowedSide: "AUTO",
			MaxFrenzyRVOL: 2.5, MaxCounterImpulseATR: 1.5, EntryOrderType: "LIMIT",
			Sweep: strategy.SweepConfig{Enabled: true, LookbackBars: 12, MinUpperWickRatio: 0.25, MaxLowerWickRatio: 0.45, MaxDeltaZScore: -0.25},
			Sizing: strategy.SizingConfig{
				RiskPerTradePct: decimal.NewFromFloat(0.015), MaxSlotMarginFraction: decimal.NewFromFloat(0.20),
				AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90), AccountLeverage: 8.0,
				SLMultiplier: 1.65, TPMultiplier: 3.60, MinSLDistancePct: decimal.NewFromFloat(0.01),
				MakerFeeRate: decimal.NewFromFloat(0.0002), TakerFeeRate: decimal.NewFromFloat(0.0005), SlippageRate: decimal.NewFromFloat(0.0002),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	eng.runMarketScan(ctx)
	cancel()
	if eng.streamCancel != nil {
		eng.streamCancel()
	}
	eng.streamWg.Wait()

	eng.statesMu.RLock()
	completed := eng.hasCompletedScan
	eng.statesMu.RUnlock()
	if !completed {
		t.Fatalf("expected hasCompletedScan == true after clean zero-candidate scan")
	}

	// Simulate SOLUSDT still held in memory (e.g., pending order canceled right after scan).
	// Because a scan completed with zero candidates, DefaultSymbolScope must NOT re-enable entries.
	eng.evaluateAndLogClosedCandle(*latest, st)
	var decision, reason string
	if err := db.RawConn().QueryRow("SELECT decision, reject_reason_code FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&decision, &reason); err != nil {
		t.Fatalf("failed to query signal_audit_log: %v", err)
	}
	if decision != "REJECT" || dry.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected REJECT without opening order on clean empty scan, got decision=%q reason=%q hasActive=%v", decision, reason, dry.HasActiveOrderOrPosition("SOLUSDT"))
	}
}

func TestBotEngine_LateWSCandleEvaluatedAfterRESTBootstrap(t *testing.T) {
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := storage.NewRepository(db)

	dry := broker.NewDryRunBroker(decimal.NewFromFloat(500), broker.DryRunConfig{MaxSlots: 3})
	defer dry.Close()

	step := 5 * time.Minute
	now := time.Now().UTC().Truncate(step)
	prevBar := model.Candle{OpenTime: now.Add(-2 * step), CloseTime: now.Add(-step - time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100.5, VolumeBase: 10, QuoteVolumeUSDT: 1000}
	lateBar := model.Candle{OpenTime: now.Add(-step), CloseTime: now.Add(-time.Millisecond), Open: 100.5, High: 102, Low: 100, Close: 101, VolumeBase: 12, QuoteVolumeUSDT: 1200}

	st := model.NewSymbolState("SOLUSDT", model.ExchangeFilter{}, 50, 50)
	st.IsLeverageIsolatedVerified = true
	st.ApplyClosedCandleLocked(prevBar, 50, 50)
	st.MarkCandleEvaluatedLocked(prevBar.OpenTime)
	// REST bootstrap ingests lateBar before late WS event arrives.
	if !st.ApplyClosedCandleLocked(lateBar, 50, 50) {
		t.Fatalf("expected REST bootstrap to ingest lateBar")
	}

	eng := &BotEngine{
		cfg:          &config.Config{DryRun: true, Timeframe: "5m", CandleDuration: step, MaxDailyDrawdown: -0.05},
		repo:         repo,
		broker:       dry,
		dryRunBroker: dry,
		activeStates: map[string]*model.SymbolState{"SOLUSDT": st},
		primaryWatchedSet: map[string]bool{"SOLUSDT": true},
		cooldownExits:     make(map[string]storage.RecentExitRecord),
		verifiedSymbols:   make(map[string]bool),
		scannerCfg:        scanner.ScannerConfig{BootstrapCount: 50, DeltaWindow: 49},
		evaluatorCfg:      strategy.EvaluatorConfig{CandleDuration: step, MaxSlots: 3},
	}

	// Late WS arrives for lateBar -> MUST evaluate once even though LastProcessedCandleOpenTime == lateBar.OpenTime.
	eng.processClosedKlineWS(context.Background(), "SOLUSDT", lateBar, "5m")
	eng.evalWg.Wait()

	var cnt1 int
	if err := db.RawConn().QueryRow("SELECT COUNT(*) FROM signal_audit_log WHERE symbol = 'SOLUSDT'").Scan(&cnt1); err != nil {
		t.Fatalf("count signal_audit_log failed: %v", err)
	}
	if cnt1 != 1 {
		t.Fatalf("expected late WS bar evaluated once after REST ingestion, got audit rows=%d", cnt1)
	}

	// Duplicate WS close for same lateBar -> MUST NOT re-evaluate.
	eng.processClosedKlineWS(context.Background(), "SOLUSDT", lateBar, "5m")
	eng.evalWg.Wait()

	var cnt2 int
	if err := db.RawConn().QueryRow("SELECT COUNT(*) FROM signal_audit_log WHERE symbol = 'SOLUSDT'").Scan(&cnt2); err != nil {
		t.Fatalf("count signal_audit_log failed: %v", err)
	}
	if cnt2 != 1 {
		t.Fatalf("expected duplicate WS bar skipped, got audit rows=%d", cnt2)
	}
}

func TestBotEngine_StaleOrMissingBTCCandleFailClosed(t *testing.T) {
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := storage.NewRepository(db)

	dry := broker.NewDryRunBroker(decimal.NewFromFloat(500), broker.DryRunConfig{MaxSlots: 3})
	defer dry.Close()

	// 1. waitForBTCCandle returns true on arrival and false on timeout.
	engWait := &BotEngine{}
	openT := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	if engWait.waitForBTCCandle(openT, 20*time.Millisecond) {
		t.Fatalf("expected waitForBTCCandle to return false on timeout")
	}
	engWait.notifyBTCCandleClosed(openT)
	if !engWait.waitForBTCCandle(openT, 20*time.Millisecond) {
		t.Fatalf("expected waitForBTCCandle to return true after BTC candle arrival")
	}

	// 2. Empty BTC snapshot fails closed with STALE_BTC_MACRO_CANDLE.
	step := 5 * time.Minute
	now := time.Now().UTC().Truncate(step)
	altCandle := model.Candle{OpenTime: now.Add(-step), CloseTime: now.Add(-time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100.5, VolumeBase: 10, QuoteVolumeUSDT: 1000}
	altSt := model.NewSymbolState("SOLUSDT", model.ExchangeFilter{}, 20, 20)
	altSt.IsLeverageIsolatedVerified = true
	altSt.ApplyClosedCandleLocked(altCandle, 20, 20)

	eng := &BotEngine{
		cfg:             &config.Config{DryRun: true, MaxDailyDrawdown: -0.05},
		repo:            repo,
		broker:          dry,
		dryRunBroker:    dry,
		activeStates:    map[string]*model.SymbolState{"SOLUSDT": altSt},
		cooldownExits:   make(map[string]storage.RecentExitRecord),
		verifiedSymbols: make(map[string]bool),
		evaluatorCfg:    strategy.EvaluatorConfig{CandleDuration: step, MaxSlots: 3, BTCGateEnabled: true},
	}

	eng.evaluateAndLogClosedCandle(altCandle, altSt)
	var r1 string
	if err := db.RawConn().QueryRow("SELECT reject_reason_code FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&r1); err != nil {
		t.Fatalf("query audit log failed: %v", err)
	}
	if r1 != "STALE_BTC_MACRO_CANDLE" {
		t.Fatalf("expected STALE_BTC_MACRO_CANDLE on empty BTC snapshot, got %q", r1)
	}

	// 3. Stale BTC snapshot (1 bar behind altCandle.OpenTime) also fails closed with STALE_BTC_MACRO_CANDLE.
	btcSt := model.NewSymbolState(BenchmarkSymbol, model.ExchangeFilter{}, 20, 20)
	btcCandle := altCandle
	btcCandle.OpenTime = altCandle.OpenTime.Add(-step)
	btcCandle.CloseTime = altCandle.CloseTime.Add(-step)
	btcSt.ApplyClosedCandleLocked(btcCandle, 20, 20)
	eng.statesMu.Lock()
	eng.activeStates[BenchmarkSymbol] = btcSt
	eng.statesMu.Unlock()

	eng.evaluateAndLogClosedCandle(altCandle, altSt)
	var r2 string
	if err := db.RawConn().QueryRow("SELECT reject_reason_code FROM signal_audit_log ORDER BY id DESC LIMIT 1").Scan(&r2); err != nil {
		t.Fatalf("query audit log failed: %v", err)
	}
	if r2 != "STALE_BTC_MACRO_CANDLE" {
		t.Fatalf("expected STALE_BTC_MACRO_CANDLE on stale BTC snapshot, got %q", r2)
	}
}

func TestShouldEmergencyCloseOrphanPosition_DisabledOnDBError(t *testing.T) {
	dbErr := fmt.Errorf("sqlite disk I/O error")
	if shouldEmergencyCloseOrphanPosition(dbErr, false, nil, nil, "CLOSE") {
		t.Fatalf("expected orphan emergency close DISABLED whenever DB read fails")
	}
	if shouldEmergencyCloseOrphanPosition(nil, true, nil, nil, "CLOSE") {
		t.Fatalf("expected NO orphan emergency close when active SL exists")
	}
	if shouldEmergencyCloseOrphanPosition(nil, false, fmt.Errorf("algo err"), nil, "CLOSE") {
		t.Fatalf("expected NO orphan emergency close when algo verification errored")
	}
	if shouldEmergencyCloseOrphanPosition(nil, false, nil, nil, "ADOPT") {
		t.Fatalf("expected NO orphan emergency close when policy is ADOPT")
	}
	if !shouldEmergencyCloseOrphanPosition(nil, false, nil, nil, "CLOSE") {
		t.Fatalf("expected orphan emergency close ONLY when DB ok, no SL, no verify err, and policy==CLOSE")
	}
}

func makeBot15mHourlySeries(
	sym string,
	start time.Time,
	hours int,
	closeAtHour func(h int) float64,
	deltaAtHour func(h int) float64,
) []model.Candle {
	out := make([]model.Candle, 0, hours*4)
	for h := 0; h < hours; h++ {
		c := closeAtHour(h)
		qv := 20_000_000.0
		delta := 0.0
		if deltaAtHour != nil {
			delta = deltaAtHour(h)
		}
		tq := 0.5 * (delta + qv)
		for q := 0; q < 4; q++ {
			oT := start.Add(time.Duration(h)*time.Hour + time.Duration(q)*15*time.Minute)
			out = append(out, model.Candle{
				OpenTime:          oT,
				CloseTime:         oT.Add(15*time.Minute - time.Millisecond),
				Open:              c,
				High:              c * 1.003,
				Low:               c * 0.997,
				Close:             c,
				VolumeBase:        (qv / 4.0) / c,
				QuoteVolumeUSDT:   qv / 4.0,
				TakerBuyQuoteUSDT: tq / 4.0,
			})
		}
	}
	return out
}

func TestBotEngine_MajorPodHourCloseBatchRankingAndPod1Isolation(t *testing.T) {
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := storage.NewRepository(db)

	dry := broker.NewDryRunBroker(decimal.NewFromFloat(1000.0), broker.DryRunConfig{
		MaxSlots:       7,
		EntryOrderType: "MARKET",
	})
	defer dry.Close()

	const hours = 260
	endNow := time.Now().UTC()
	t0 := endNow.Truncate(time.Hour).Add(-hours * time.Hour)
	btcSeries := makeBot15mHourlySeries(BenchmarkSymbol, t0, hours, func(h int) float64 {
		if h < 230 {
			return 65000.0
		}
		return 64000.0 - float64(h-230)*10.0
	}, nil)
	negDeltaFn := func(h int) float64 {
		if h == hours-1 {
			return -5_000_000.0
		}
		if h%2 == 0 {
			return 100_000.0
		}
		return -100_000.0
	}
	ethSeries := makeBot15mHourlySeries("ETHUSDT", t0, hours, func(h int) float64 {
		if h == hours-1 {
			return 1840.0 // -8% vs flat-ish BTC -> higher |RS24h|
		}
		return 2000.0
	}, negDeltaFn)
	solSeries := makeBot15mHourlySeries("SOLUSDT", t0, hours, func(h int) float64 {
		if h == hours-1 {
			return 95.0 // -5%
		}
		return 100.0
	}, negDeltaFn)

	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.01),
		MinQty:      decimal.NewFromFloat(0.01),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	makeSymState := func(sym string, series []model.Candle, bid, ask float64) *model.SymbolState {
		st := model.NewSymbolState(sym, filter, 1500, 1500)
		st.IsLeverageIsolatedVerified = true
		st.BestBid = bid
		st.BestAsk = ask
		st.BookUpdatedAt = endNow
		st.MarkPrice = (bid + ask) / 2.0
		st.MarkPriceUpdatedAt = endNow
		st.FundingRate = 0.0001
		st.FundingUpdatedAt = endNow
		for _, c := range series {
			st.ApplyClosedCandleLocked(c, 1500, 1500)
		}
		return st
	}

	btcSt := makeSymState(BenchmarkSymbol, btcSeries, 63700, 63701)
	ethSt := makeSymState("ETHUSDT", ethSeries, 1839.9, 1840.1)
	solSt := makeSymState("SOLUSDT", solSeries, 94.99, 95.01)

	eng := &BotEngine{
		cfg: &config.Config{
			DryRun:                  true,
			InitialEquity:           decimal.NewFromFloat(1000.0),
			EquityModeCompounding:   true,
			MaxSlots:                6,
			MaxDailyDrawdown:        -0.075,
			Timeframe:               "15m",
			CandleDuration:          15 * time.Minute,
			PortfolioClusterSLCount: 2,
		},
		repo:              repo,
		broker:            dry,
		dryRunBroker:      dry,
		majorPodSymbols:   []string{"ETHUSDT", "SOLUSDT"},
		majorPodSet:       map[string]bool{"ETHUSDT": true, "SOLUSDT": true},
		primaryWatchedSet: map[string]bool{},
		hasCompletedScan:  true,
		clusterSL:         strategy.NewClusterSLTracker(2, time.Hour),
		activeStates: map[string]*model.SymbolState{
			BenchmarkSymbol: btcSt,
			"ETHUSDT":       ethSt,
			"SOLUSDT":       solSt,
		},
		cooldownExits:    make(map[string]storage.RecentExitRecord),
		verifiedSymbols:  make(map[string]bool),
		btcWaitTimeout:   200 * time.Millisecond,
		majorBatchWindow: 50 * time.Millisecond,
		evaluatorCfg: strategy.EvaluatorConfig{
			CandleDuration:   15 * time.Minute,
			MaxSlots:         6,
			EMAPeriod:        200,
			ATRPeriod:        14,
			MaxSpreadPct:     0.20,
			MaxBookStaleness: 30 * time.Second,
			MaxMarkStaleness: 30 * time.Second,
			MaxDataStaleness: 30 * time.Minute,
			MajorPod: strategy.MajorPodConfig{
				ShortEnabled:        true,
				LongEnabled:         false,
				Symbols:             []string{"ETHUSDT", "SOLUSDT"},
				MaxSlots:            1,
				ShortRiskPct:        decimal.NewFromFloat(0.006),
				LongRiskPct:         decimal.NewFromFloat(0.0055),
				RSWindowHours:       24,
				BreakoutLookbackHrs: 12,
				ShortRSMinPct:       3.0,
				LongRSMinPct:        4.5,
				LongRSMaxPct:        20.0,
				MinDeltaZ:           0.40,
				LongMaxFundingRate:  0.0001,
				ShortMinFundingRate: -1.0,
				SLATRMultiplier:     2.0,
				TPATRMultiplier:     6.0,
				MinSLDistancePct:    decimal.NewFromFloat(0.01),
				EntryOrderType:      "MARKET",
			},
			Sizing: strategy.SizingConfig{
				MaxSlotMarginFraction:          decimal.NewFromFloat(0.25),
				AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
				AccountLeverage:                10.0,
				MakerFeeRate:                   decimal.NewFromFloat(0.0002),
				TakerFeeRate:                   decimal.NewFromFloat(0.0005),
				SlippageRate:                   decimal.NewFromFloat(0.0002),
			},
		},
	}

	eng.notifyBTCCandleClosed(btcSeries[len(btcSeries)-1].OpenTime)
	// Submit weaker RS first (SOL), then stronger RS (ETH) into the same hour close batch
	eng.dispatchEvaluationsForClosedCandle("SOLUSDT", solSeries[len(solSeries)-1], solSt)
	eng.dispatchEvaluationsForClosedCandle("ETHUSDT", ethSeries[len(ethSeries)-1], ethSt)
	eng.evalWg.Wait()

	positions := dry.GetActivePositions()
	if len(positions) != 1 || positions[0].Symbol != "ETHUSDT" || positions[0].StrategyID != strategy.StrategyIDMajorRSMomShort {
		t.Fatalf("expected stronger |RS24h| ETHUSDT SHORT opened under MaxSlots=1, got %+v", positions)
	}
	if nonMaj := eng.getActiveNonMajorSlots(); nonMaj != 0 {
		t.Fatalf("expected Pod #1 non-major slot count still 0 (isolated from MajorPod), got %d", nonMaj)
	}
	if maj := eng.getActiveMajorSlots(); maj != 1 {
		t.Fatalf("expected MajorPod slot count 1, got %d", maj)
	}

	// Verify MajorPod exits do NOT trigger Pod #1 ClusterSLTracker and stay isolated from Pod #1 cooldowns
	eng.recordSymbolExitForStrategy("ETHUSDT", strategy.StrategyIDMajorRSMomShort, endNow, "STOP_LOSS")
	eng.recordSymbolExitForStrategy("SOLUSDT", strategy.StrategyIDMajorRSMomLong, endNow.Add(time.Minute), "STOP_LOSS")
	if paused, _ := eng.clusterSL.IsPaused(endNow.Add(2 * time.Minute)); paused {
		t.Fatalf("expected MajorPod exits not to pause Pod #1 ClusterSLTracker")
	}
	eng.cooldownMu.RLock()
	if _, leaked := eng.cooldownExits["ETHUSDT"]; leaked {
		t.Fatalf("expected MajorPod exit not to pollute Pod #1 cooldownExits")
	}
	if _, okMaj := eng.majorCooldownExits["ETHUSDT"]; !okMaj {
		t.Fatalf("expected ETHUSDT MajorPod exit recorded in majorCooldownExits")
	}
	eng.cooldownMu.RUnlock()

	// Verify web dashboard feeds expose MajorPod symbols and 3-Pod telemetry without duplicating scanned coins
	eng.statesMu.Lock()
	eng.watchedCoins = []string{"SOLUSDT"}
	eng.statesMu.Unlock()
	coins := eng.getScannerCoins()
	if len(coins) != 2 {
		t.Fatalf("expected 2 deduplicated scanner coins (1 SCAN + 1 MAJOR), got %d: %+v", len(coins), coins)
	}
	if coins[0].Symbol != "SOLUSDT" || coins[0].Pod != "SCAN" || coins[1].Symbol != "ETHUSDT" || coins[1].Pod != "MAJOR" || coins[1].MarkPrice <= 0 {
		t.Fatalf("unexpected pod attribution in getScannerCoins: %+v", coins)
	}
	telem := eng.getTelemetry()
	if telem == nil || !telem.MajorShortEnabled || telem.MajorLongEnabled || telem.MajorPodCount != 2 || telem.MajorMaxSlots != 1 || telem.CoreMaxSlots != 6 {
		t.Fatalf("unexpected 3-pod telemetry fields: %+v", telem)
	}
}
