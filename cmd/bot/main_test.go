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

	"trml/internal/broker"
	"trml/internal/config"
	"trml/internal/exchange"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/storage"
	"trml/internal/strategy"
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
