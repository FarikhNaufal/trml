package replay

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
	"trml/internal/broker/protection"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/strategy"
)

func TestReplayEngine_EndToEndSyntheticSimulation(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	step := 5 * time.Minute

	// Buat 250 candle sintetis untuk BTCUSDT
	// Bar 0..200: Base level ~40,000 (Warmup)
	// Bar 201..210: Strong Impulse Up (40,000 -> 42,000)
	// Bar 211..220: Gentle Pullback to VWAP (42,000 -> 41,000) with High Delta Z-score
	// Bar 221..240: Bounce & Trend Continuation (41,000 -> 43,000)
	candles := make([]data.HistoricalCandle, 250)
	currentPrice := 40000.0

	for i := 0; i < 250; i++ {
		curTime := t0.Add(time.Duration(i) * step)

		if i > 200 && i <= 210 {
			currentPrice += 200.0 // Impulse up
		} else if i > 210 && i <= 218 {
			currentPrice -= 100.0 // Pullback to session VWAP
		} else if i > 218 {
			currentPrice += 150.0 // Bounce continuation
		}

		vol := 100.0
		quoteVol := currentPrice * vol
		takerBuyQuote := quoteVol * 0.50 // neutral delta

		if i == 218 {
			// Sinyal Bar: Orderflow Delta Lonjak tinggi (Buyer Maker / Taker Buy Kuat)
			takerBuyQuote = quoteVol * 0.90
		}

		barDelta := 2.0*takerBuyQuote - quoteVol

		candles[i] = data.HistoricalCandle{
			Symbol:              "BTCUSDT",
			OpenTime:            curTime,
			CloseTime:           curTime.Add(step - time.Millisecond),
			Open:                currentPrice - 20.0,
			High:                currentPrice + 50.0,
			Low:                 currentPrice - 50.0,
			Close:               currentPrice,
			Volume:              vol,
			QuoteVolume:         quoteVol,
			TradeCount:          1000,
			TakerBuyBaseVolume:  vol * 0.5,
			TakerBuyQuoteVolume: takerBuyQuote,
			BarDeltaUSDT:        barDelta,
		}
	}

	candleMap := map[string][]data.HistoricalCandle{
		"BTCUSDT": candles,
	}

	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.10),
		StepSize:    decimal.NewFromFloat(0.001),
		MinNotional: decimal.NewFromFloat(5.0),
		MinQty:      decimal.NewFromFloat(0.001),
	}
	filters := map[string]model.ExchangeFilter{
		"BTCUSDT": filter,
	}

	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
		MaxSlots:                3,
		MaxDailyDrawdownPct:     -0.05,
		AccountLeverage:         8.0,
		SignalDelayBars:         1,
		LimitTimeoutBars:        2,
		PenetrationTicks:        1,
		IntrabarConflictSLFirst: true,
		PartialTPEnabled:        true,
		PartialTP1Ratio:         0.50,
		PartialTP1ATRMult:       1.00,
		TrailingEnabled:         true,
		TrailingActivateATR:     1.10,
		TrailingDistanceATR:     0.55,
	})

	evalCfg := strategy.EvaluatorConfig{
		MaxSlots:                 3,
		MaxSpreadPct:             0.0010,
		MinNATRPct:               0.0010, // longgar untuk data sintetis
		MaxNATRPct:               0.0500,
		EMAPeriod:                200,
		ATRPeriod:                14,
		DeltaZScoreWindow:        30,
		MinBarDeltaZScore:        0.50,
		MinBounceZScore:          0.50,
		PullbackToleranceATR:     0.80,
		MinEMA200DistanceATR:     0.10,
		MaxEMA200DistanceATR:     5.00,
		LookbackImpulseBars:      18,
		RetestLookbackBars:       6,
		MinImpulseATR:            0.50,
		MaxEntryDistanceATR:      1.50,
		EntryOrderType:           "LIMIT",
		BTCGateEnabled:           false, // nonaktifkan untuk single-pair synthetic
		AllowedSide:              "BOTH",
		MaxFrenzyRVOL:            5.0,
		MaxCounterImpulseATR:     1.50,
		MinSignalRVOL:            0.20,
		CooldownAfterExitMinutes: 10,
		VWAPSessionWarmupMinutes: 30,
		Sizing: strategy.SizingConfig{
			RiskPerTradePct:                decimal.NewFromFloat(0.02),
			MaxSlotMarginFraction:          decimal.NewFromFloat(0.25),
			AvailableBalanceBufferFraction: decimal.NewFromFloat(0.10),
			AccountLeverage:                8.0,
			SLMultiplier:                   1.75,
			TPMultiplier:                   3.00,
			TP1Multiplier:                  1.00,
			MinSLDistancePct:               decimal.NewFromFloat(0.010),
			MakerFeeRate:                   decimal.NewFromFloat(0.0002),
			TakerFeeRate:                   decimal.NewFromFloat(0.0005),
			SlippageRate:                   decimal.NewFromFloat(0.0005),
			EntryOrderType:                 "LIMIT",
		},
	}

	replayCfg := ReplayConfig{
		WarmupBars:   200,
		SpreadPct:    0.0002,
		FundingRate:  0.0001,
		BenchmarkSym: "BTCUSDT",
	}

	engine := NewReplayEngine(replayCfg, evalCfg, sim, candleMap, filters)

	err := engine.Run(context.Background())
	if err != nil {
		t.Fatalf("replay engine run failed: %v", err)
	}

	if engine.TotalSignalsEvaluated == 0 {
		t.Errorf("expected signals to be evaluated")
	}

	t.Logf("Replay completed: %d signals evaluated, %d approved, %d rejected, %d closed trades",
		engine.TotalSignalsEvaluated, engine.SignalsApproved, engine.SignalsRejected, len(sim.ClosedTrades))
}

func TestSimBrokerFixedEquityAvailableBalanceDeductsUsedMargin(t *testing.T) {
	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT:     decimal.NewFromFloat(100.0),
		MaxSlots:              5,
		AccountLeverage:       10.0,
		EquityModeCompounding: false, // Fixed-equity mode
	})

	// Simulasikan 1 pending limit entry menggunakan margin 20 USDT (notional 200 USDT / lev 10)
	ok, err := sim.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "FIXED_EQ_1",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(0.1),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		SLPrice:       decimal.NewFromFloat(2100.0),
		TPPrice:       decimal.NewFromFloat(1800.0),
		CreatedAt:     time.Now().UTC(),
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}

	avail := sim.GetAvailableBalanceUSDT()
	if !avail.Equal(decimal.NewFromFloat(80.0)) {
		t.Fatalf("expected available balance 80.0 USDT (100 - 20 usedMargin) in fixed-equity mode, got %s", avail)
	}
}

func TestCalcRollingBufferCap_Minimum1000AndScaled1m(t *testing.T) {
	if cap5m := CalcRollingBufferCap(5*time.Minute, 200); cap5m != 1000 {
		t.Fatalf("expected 5m buffer cap 1000, got %d", cap5m)
	}
	if cap1m := CalcRollingBufferCap(1*time.Minute, 200); cap1m != 1640 {
		t.Fatalf("expected 1m buffer cap 1640 (1440 + 200), got %d", cap1m)
	}
}

func TestReplayEngine_OIModeStrictVsAutoAndScannerStats(t *testing.T) {
	t0 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	step := 5 * time.Minute
	candles := make([]data.HistoricalCandle, 120)
	price := 100.0
	for i := 0; i < len(candles); i++ {
		if i > 100 {
			price += 0.2
		}
		candles[i] = data.HistoricalCandle{
			OpenTime:    t0.Add(time.Duration(i) * step),
			CloseTime:   t0.Add(time.Duration(i+1) * step),
			Symbol:      "SOLUSDT",
			Open:        price,
			High:        price + 0.3,
			Low:         price - 0.2,
			Close:       price + 0.1,
			Volume:      1000,
			QuoteVolume: 100000,
		}
	}

	filters := map[string]model.ExchangeFilter{
		"SOLUSDT": {
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
	}
	evalCfg := strategy.EvaluatorConfig{
		EMAPeriod:           20,
		ATRPeriod:           14,
		MaxSlots:            3,
		MaxSpreadPct:        0.10,
		BTCGateEnabled:      false,
		MinOIInterval:       4 * time.Minute,
		MaxOIInterval:       7 * time.Minute,
		MaxOIStaleness:      10 * time.Minute,
		DisableOIFilter:     false,
		MaxDailyDrawdownPct: -0.05,
	}

	// 1. Strict mode without OI samples -> must record MISSING_OI_DATA and OIMissingCount > 0
	simStrict := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000),
		MaxSlots:          3,
	})
	engStrict := NewReplayEngine(ReplayConfig{
		WarmupBars:            30,
		DynamicScannerEnabled: true,
		ScannerInterval:       15 * time.Minute,
		ScannerCfg: scanner.ScannerConfig{
			CandleDuration:   5 * time.Minute,
			MaxWatchedCoins:  5,
			BootstrapCount:   25,
			MinVolume24hUSDT: 1000,
			MaxSpreadPct:     0.15,
			ATRPeriod:        14,
			EMAPeriod:        20,
			MinRVOL:          0.1,
			MinFallbackRVOL:  0.1,
			RVOLLookback:     10,
			MaxNATRPct:       10.0,
		},
		OIMode: OIModeStrict,
	}, evalCfg, simStrict, map[string][]data.HistoricalCandle{"SOLUSDT": candles}, filters)
	if err := engStrict.Run(context.Background()); err != nil {
		t.Fatalf("strict run failed: %v", err)
	}
	if engStrict.Stats.OIMissingCount == 0 || engStrict.RejectReasons["MISSING_OI_DATA"] == 0 {
		t.Fatalf("expected MISSING_OI_DATA rejections in strict mode without OI, stats=%+v rejects=%+v", engStrict.Stats, engStrict.RejectReasons)
	}
	if engStrict.Stats.ScannerStats.CyclesRun == 0 {
		t.Fatalf("expected scanner cycles > 0, got %+v", engStrict.Stats.ScannerStats)
	}
	if len(simStrict.EquityCurve) == 0 {
		t.Fatalf("expected timeline EquityCurve points recorded")
	}

	// 2. Auto/Strict mode with valid 5m OI series -> covered > 0 (~100% coverage)
	oiMgr := data.NewMetricsOIDownloader(t.TempDir(), nil, nil)
	pts := make([]data.OpenInterestPoint, 0, len(candles))
	for _, c := range candles {
		pts = append(pts, data.OpenInterestPoint{
			Timestamp:            c.CloseTime,
			SumOpenInterest:      50000.0,
			SumOpenInterestValue: 5000000.0,
		})
	}
	oiMgr.SetSeriesForTesting("SOLUSDT", pts)
	simCovered := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000),
		MaxSlots:          3,
	})
	engCovered := NewReplayEngine(ReplayConfig{
		WarmupBars: 30,
		OIMode:     OIModeAuto,
		OIMgr:      oiMgr,
	}, evalCfg, simCovered, map[string][]data.HistoricalCandle{"SOLUSDT": candles}, filters)
	if err := engCovered.Run(context.Background()); err != nil {
		t.Fatalf("covered run failed: %v", err)
	}
	if engCovered.Stats.OICoveredCount == 0 || engCovered.Stats.OICoveragePct < 99.0 {
		t.Fatalf("expected ~100%% OI coverage from metrics csv, got %+v", engCovered.Stats)
	}
}

func TestReplayEngine_SubBar1mExecutionAndDelistSettlement(t *testing.T) {
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	step5m := 5 * time.Minute

	// Two symbols:
	// BTCUSDT stays alive for 50 bars
	// DEADUSDT stops producing bars after bar 31 while holding a pending/open position -> ForceClosePosition settles it!
	btcCandles := make([]data.HistoricalCandle, 60)
	for i := range btcCandles {
		btcCandles[i] = data.HistoricalCandle{
			OpenTime:    t0.Add(time.Duration(i) * step5m),
			CloseTime:   t0.Add(time.Duration(i+1) * step5m),
			Symbol:      "BTCUSDT",
			Open:        50000,
			High:        50100,
			Low:         49900,
			Close:       50050,
			Volume:      100,
			QuoteVolume: 5000000,
		}
	}

	deadCandles := make([]data.HistoricalCandle, 12)
	for i := range deadCandles {
		deadCandles[i] = data.HistoricalCandle{
			OpenTime:    t0.Add(time.Duration(i) * step5m),
			CloseTime:   t0.Add(time.Duration(i+1) * step5m),
			Symbol:      "DEADUSDT",
			Open:        10.0,
			High:        10.2,
			Low:         9.8,
			Close:       10.0,
			Volume:      1000,
			QuoteVolume: 10000,
		}
	}

	// Build 1m sub-bars covering deadCandles for minutes inside the 5m intervals
	dead1m := make([]data.HistoricalCandle, 0, len(deadCandles)*5)
	for _, c5 := range deadCandles {
		for m := 0; m < 5; m++ {
			mo := c5.OpenTime.Add(time.Duration(m) * time.Minute)
			dead1m = append(dead1m, data.HistoricalCandle{
				OpenTime:  mo,
				CloseTime: mo.Add(time.Minute),
				Symbol:    "DEADUSDT",
				Open:      10.0,
				High:      10.1,
				Low:       9.9,
				Close:     10.0,
				Volume:    200,
			})
		}
	}

	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000),
		MaxSlots:          3,
		EntryOrderType:    "MARKET",
		MakerFeeRate:      decimal.NewFromFloat(0.0002),
		TakerFeeRate:      decimal.NewFromFloat(0.0005),
	})
	sim.SetExchangeFilter("DEADUSDT", model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(1.0),
		MinNotional: decimal.NewFromFloat(5.0),
	})
	// Pre-place an entry right before bar 10 closes so 1m sub-bars fill DEADUSDT before bar 12 stops.
	_, _ = sim.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DELIST_POS",
		Symbol:        "DEADUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(10.0),
		Qty:           decimal.NewFromFloat(5.0),
		SLPrice:       decimal.NewFromFloat(8.0),
		TPPrice:       decimal.NewFromFloat(15.0),
		CreatedAt:     deadCandles[9].CloseTime,
	})

	eng := NewReplayEngine(ReplayConfig{
		WarmupBars:    5,
		BenchmarkSym:  "BTCUSDT",
		OIMode:        OIModeOff,
		ExecCandleMap: map[string][]data.HistoricalCandle{"DEADUSDT": dead1m},
	}, strategy.EvaluatorConfig{
		EMAPeriod:           200, // disable auto signals; test isolates sub-bar fill + delist settlement
		MaxDailyDrawdownPct: -0.05,
	}, sim, map[string][]data.HistoricalCandle{
		"BTCUSDT":  btcCandles,
		"DEADUSDT": deadCandles,
	}, map[string]model.ExchangeFilter{
		"DEADUSDT": {TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(1.0), MinNotional: decimal.NewFromFloat(5.0)},
	})

	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if eng.Stats.SubBarExecCount == 0 {
		t.Fatalf("expected 1m sub-bar execution count > 0, got %+v", eng.Stats)
	}
	if eng.Stats.DelistForceClosed != 1 || sim.Stats.DelistSettled != 1 {
		t.Fatalf("expected DelistForceClosed=1 and DelistSettled=1, replay=%+v sim=%+v", eng.Stats, sim.Stats)
	}
	if len(sim.ClosedTrades) != 1 || sim.ClosedTrades[0].ExitReason != "DELIST_OR_DATA_END_SETTLE" {
		t.Fatalf("expected DELIST_OR_DATA_END_SETTLE trade, got %+v", sim.ClosedTrades)
	}
}

func TestReplayEngine_SkipStaleSymbolAndEffectiveEvalTimeOI999(t *testing.T) {
	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	step := 5 * time.Minute

	// 1. LIVEUSDT has 40 bars with CloseTime ending in ...59.999 (real Binance kline format)
	// 2. STALEUSDT stops at bar 32 (simulating halted/delisted coin)
	liveCandles := make([]data.HistoricalCandle, 40)
	for i := range liveCandles {
		tOpen := t0.Add(time.Duration(i) * step)
		liveCandles[i] = data.HistoricalCandle{
			OpenTime:    tOpen,
			CloseTime:   tOpen.Add(step - time.Millisecond), // ...59.999
			Symbol:      "LIVEUSDT",
			Open:        100.0,
			High:        100.5,
			Low:         99.5,
			Close:       100.0,
			Volume:      1000,
			QuoteVolume: 100000,
		}
	}
	staleCandles := make([]data.HistoricalCandle, 33)
	copy(staleCandles, liveCandles[:33])
	for i := range staleCandles {
		staleCandles[i].Symbol = "STALEUSDT"
	}

	// OI points are timestamped on the exact 5m boundary (:00.000), as in Binance Vision metrics CSV
	oiMgr := data.NewMetricsOIDownloader(t.TempDir(), nil, nil)
	oiPts := make([]data.OpenInterestPoint, 0, len(liveCandles))
	for _, c := range liveCandles {
		oiPts = append(oiPts, data.OpenInterestPoint{
			Timestamp:            c.OpenTime.Add(step), // exact :00.000 (1ms after CloseTime ...59.999)
			SumOpenInterest:      10000.0,
			SumOpenInterestValue: 1000000.0,
		})
	}
	oiMgr.SetSeriesForTesting("LIVEUSDT", oiPts)
	oiMgr.SetSeriesForTesting("STALEUSDT", oiPts)

	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000),
		MaxSlots:          3,
	})
	f := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	eng := NewReplayEngine(ReplayConfig{
		WarmupBars: 30,
		OIMode:     OIModeStrict,
		OIMgr:      oiMgr,
	}, strategy.EvaluatorConfig{
		EMAPeriod:           20,
		ATRPeriod:           14,
		MaxSlots:            3,
		MaxSpreadPct:        0.10,
		MinOIInterval:       4 * time.Minute,
		MaxOIInterval:       7 * time.Minute,
		MaxOIStaleness:      5 * time.Minute, // strict 5m staleness: would fail if 1ms CloseTime off-by-one lagged OI by 1 bar!
		MaxDailyDrawdownPct: -0.05,
	}, sim, map[string][]data.HistoricalCandle{
		"LIVEUSDT":  liveCandles,
		"STALEUSDT": staleCandles,
	}, map[string]model.ExchangeFilter{
		"LIVEUSDT":  f,
		"STALEUSDT": f,
	})

	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("replay run failed: %v", err)
	}
	// Bars evaluated from barIdx=30..39:
	// - LIVEUSDT closes on all 10 bars (30..39) -> 10 evaluations
	// - STALEUSDT closes only on bars 30, 31, 32 -> 3 evaluations (must NOT be re-evaluated on bars 33..39!)
	if eng.TotalSignalsEvaluated != 13 {
		t.Fatalf("expected exactly 13 signal evaluations (10 LIVEUSDT + 3 STALEUSDT, no phantom re-evaluation of stale symbol), got %d", eng.TotalSignalsEvaluated)
	}
	// Because effectiveEvalTime normalizes ...59.999 -> :00.000, current-bar OI at :00.000 is matched with 0 missing OI!
	if eng.Stats.OIMissingCount != 0 || eng.Stats.OICoveredCount != 13 {
		t.Fatalf("expected 100%% OI coverage (13 covered, 0 missing) with ...59.999 CloseTime and :00.000 OI timestamps, got %+v", eng.Stats)
	}
}

func TestReplayEngine_BuildBTCMacro_EnabledForMacroAndRel(t *testing.T) {
	btcCandles := make([]model.Candle, 40)
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := range btcCandles {
		btcCandles[i] = model.Candle{
			OpenTime:  base.Add(time.Duration(i) * 15 * time.Minute),
			CloseTime: base.Add(time.Duration(i+1)*15*time.Minute - time.Millisecond),
			Open:      60000,
			High:      60100,
			Low:       59900,
			Close:     60050,
		}
	}
	rollingCandles := map[string][]model.Candle{"BTCUSDT": btcCandles}
	rollingDeltas := map[string][]float64{"BTCUSDT": make([]float64, 40)}

	engMacro := NewReplayEngine(ReplayConfig{BenchmarkSym: "BTCUSDT"}, strategy.EvaluatorConfig{
		BTCGateEnabled: false,
		MacroRouter:    strategy.MacroRegimeRouterConfig{Enabled: true},
	}, simbroker.NewSimBroker(simbroker.SimBrokerConfig{}), nil, nil)
	if snap := engMacro.buildBTCMacro(base.Add(10*time.Hour), rollingCandles, rollingDeltas); snap == nil {
		t.Fatalf("expected non-nil btcSnap when MacroRouter.Enabled=true and BTCGateEnabled=false")
	}

	engRel := NewReplayEngine(ReplayConfig{BenchmarkSym: "BTCUSDT"}, strategy.EvaluatorConfig{
		BTCGateEnabled: false,
		RelResidual:    strategy.RelResidualConfig{Enabled: true},
	}, simbroker.NewSimBroker(simbroker.SimBrokerConfig{}), nil, nil)
	if snap := engRel.buildBTCMacro(base.Add(10*time.Hour), rollingCandles, rollingDeltas); snap == nil {
		t.Fatalf("expected non-nil btcSnap when RelResidual.Enabled=true and BTCGateEnabled=false")
	}
}

func TestReplayEngine_ZeroCandidateScanClearsWatchlist(t *testing.T) {
	t0 := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	step := 15 * time.Minute
	totalBars := 75
	candles := make([]data.HistoricalCandle, totalBars)
	for i := 0; i < totalBars; i++ {
		oT := t0.Add(time.Duration(i) * step)
		halfRange := 1.0 // 2.0% NATR on price 100 during bars 0..44
		if i >= 45 {
			halfRange = 0.05 // 0.1% NATR on price 100 during bars 45..74 (healthy scan rejects NATR < 1.0%)
		}
		candles[i] = data.HistoricalCandle{
			OpenTime:          oT,
			CloseTime:         oT.Add(step - time.Millisecond),
			Symbol:            "ALTUSDT",
			Open:              100.0,
			High:              100.0 + halfRange,
			Low:               100.0 - halfRange,
			Close:             100.0,
			Volume:              2000.0,
			QuoteVolume:         200000.0,
			TakerBuyQuoteVolume: 100000.0,
		}
	}

	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1)}
	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		CandleDuration:    step,
		MaxSlots:          2,
	})
	eng := NewReplayEngine(ReplayConfig{
		WarmupBars:            40,
		DynamicScannerEnabled: true,
		ScannerInterval:       step,
		SpreadPct:             0.05,
		ScannerCfg: scanner.ScannerConfig{
			MinVolume24hUSDT:      1_000_000,
			MaxSpreadPct:          0.10,
			MinRVOL:               0.50,
			MinFallbackRVOL:       0.10,
			RVOLLookback:          20,
			MinNATRPct:            1.00,
			MaxNATRPct:            3.00,
			MaxScannerVWAPDistATR: 10.0,
			ATRPeriod:             14,
			EMAPeriod:             30,
			MaxWatchedCoins:       10,
			BootstrapCount:        40,
			DeltaWindow:           20,
			CandleDuration:        step,
			RankingMode:           "pullback",
		},
	}, strategy.EvaluatorConfig{
		CandleDuration: step,
		EMAPeriod:      30,
		ATRPeriod:      14,
		MaxSpreadPct:   0.10,
		DisableOIFilter: true,
	}, sim, map[string][]data.HistoricalCandle{"ALTUSDT": candles}, map[string]model.ExchangeFilter{"ALTUSDT": f})

	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("replay run failed: %v", err)
	}
	// Bars evaluated window is 40..74 (35 bars). Once NATR drops below MinNATRPct around bar 56,
	// a healthy 0-candidate scan must clear watchedSymbols instead of retaining stale ALTUSDT for all 35 bars.
	if eng.TotalSignalsEvaluated == 0 || eng.TotalSignalsEvaluated >= 25 {
		t.Fatalf("expected ALTUSDT evaluated only while valid (<25 of 35 bars), got TotalSignalsEvaluated=%d", eng.TotalSignalsEvaluated)
	}
}

func make15mHourlyHistSeries(
	sym string,
	start time.Time,
	hours int,
	closeAtHour func(h int) float64,
	deltaAtHour func(h int) float64,
) []data.HistoricalCandle {
	out := make([]data.HistoricalCandle, 0, hours*4)
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
			out = append(out, data.HistoricalCandle{
				OpenTime:            oT,
				CloseTime:           oT.Add(15*time.Minute - time.Millisecond),
				Symbol:              sym,
				Open:                c,
				High:                c * 1.003,
				Low:                 c * 0.997,
				Close:               c,
				Volume:              (qv / 4.0) / c,
				QuoteVolume:         qv / 4.0,
				TakerBuyQuoteVolume: tq / 4.0,
				BarDeltaUSDT:        delta / 4.0,
			})
		}
	}
	return out
}

func TestReplayEngine_MajorPodHourCloseRankingAndSlotBudget(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	const hours = 260
	// BTC trending below EMA200 across trailing hours
	btcBars := make15mHourlyHistSeries("BTCUSDT", t0, hours, func(h int) float64 {
		if h < 230 {
			return 65000.0
		}
		return 64000.0 - float64(h-230)*10.0
	}, nil)

	// Two majors both trigger SHORT at hour 255: SOL drops -4% vs BTC, ETH drops -8% vs BTC -> ETH ranked #1!
	deltaNegFn := func(h int) float64 {
		if h == 255 {
			return -5_000_000.0
		}
		if h%2 == 0 {
			return 100_000.0
		}
		return -100_000.0
	}
	ethBars := make15mHourlyHistSeries("ETHUSDT", t0, hours, func(h int) float64 {
		if h == 255 {
			return 1840.0 // -8% from 2000 -> stronger |RS|
		}
		if h > 255 {
			return 1835.0
		}
		return 2000.0
	}, deltaNegFn)
	solBars := make15mHourlyHistSeries("SOLUSDT", t0, hours, func(h int) float64 {
		if h == 255 {
			return 96.0 // -4% from 100
		}
		if h > 255 {
			return 95.8
		}
		return 100.0
	}, deltaNegFn)

	f := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.01),
		MinQty:      decimal.NewFromFloat(0.01),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	evalCfg := strategy.EvaluatorConfig{
		CandleDuration:   15 * time.Minute,
		MaxSlots:         6,
		EMAPeriod:        200,
		ATRPeriod:        14,
		MaxSpreadPct:     0.20,
		DisableVWAP:      true,
		DisableOIFilter:  true,
		MaxBookStaleness: 5 * time.Second,
		MaxMarkStaleness: 15 * time.Second,
		MaxDataStaleness: 20 * time.Minute,
		MajorPod: strategy.MajorPodConfig{
			ShortEnabled:        true,
			LongEnabled:         false,
			Symbols:             []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
			MaxSlots:            1,
			ShortRiskPct:        decimal.NewFromFloat(0.010),
			LongRiskPct:         decimal.NewFromFloat(0.010),
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
			MinSLDistancePct:    decimal.NewFromFloat(0.010),
			TrailingEnabled:     true,
			TrailingActivateATR: 2.5,
			TrailingDistanceATR: 1.2,
			MaxHoldingDuration:  48 * time.Hour,
			StaleLossDuration:   0,
			CooldownMinutes:     240,
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
	}
	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT:     decimal.NewFromFloat(1000.0),
		CandleDuration:        15 * time.Minute,
		MaxSlots:              7,
		AccountLeverage:       10.0,
		EntryOrderType:        "MARKET",
		StaleLossExitDuration: 6 * time.Hour,
		MaxHoldingDuration:    18 * time.Hour,
		StrategyExitProfiles: map[string]protection.StrategyExitProfile{
			strategy.StrategyIDMajorRSMomShort: {
				HasOverride:           true,
				TrailingEnabled:       true,
				TrailingActivateATR:   2.5,
				TrailingDistanceATR:   1.2,
				StaleLossExitDuration: 0,
				MaxHoldingDuration:    48 * time.Hour,
			},
		},
	})
	eng := NewReplayEngine(ReplayConfig{
		WarmupBars:               250 * 4,
		DynamicScannerEnabled:    false,
		SettleOpenPositionsAtEnd: true,
	}, evalCfg, sim, map[string][]data.HistoricalCandle{
		"BTCUSDT": btcBars,
		"ETHUSDT": ethBars,
		"SOLUSDT": solBars,
	}, map[string]model.ExchangeFilter{
		"BTCUSDT": f,
		"ETHUSDT": f,
		"SOLUSDT": f,
	})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("replay run failed: %v", err)
	}
	if len(sim.ClosedTrades) != 1 {
		t.Fatalf("expected exactly 1 filled MajorPod trade under MaxSlots=1, got %d (%+v)", len(sim.ClosedTrades), sim.ClosedTrades)
	}
	if sim.ClosedTrades[0].Symbol != "ETHUSDT" || sim.ClosedTrades[0].StrategyID != strategy.StrategyIDMajorRSMomShort {
		t.Fatalf("expected stronger |RS24h| ETHUSDT SHORT chosen first, got %+v", sim.ClosedTrades[0])
	}
	if eng.RejectReasons["MAJOR_POD_SLOTS_FULL"] == 0 {
		t.Fatalf("expected SOLUSDT rejected with MAJOR_POD_SLOTS_FULL, got %+v", eng.RejectReasons)
	}
}

func TestReplayEngine_MajorPodRetainsOver320HourlyBars(t *testing.T) {
	// Build 500 hours (2,000 15m bars) so rollingCandles (1,280 15m cap = 320h) truncates early history
	// while majorHourlyCandles preserves all 500 1h bars and matches full-history EMA200 instead of 320-bar truncated EMA200.
	totalH := 500
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	btcBars := make15mHourlyHistSeries("BTCUSDT", t0, totalH, func(h int) float64 {
		if h < 180 {
			return 120000.0
		}
		if h < totalH-1 {
			return 62000.0
		}
		// Final bar close = 63500: below 500h EMA200 (~64.4k, pulled up by 120k early bars) -> BEAR,
		// yet ABOVE truncated 320h EMA200 (~62.0k) -> FALSE BULL if truncated!
		return 63500.0
	}, nil)
	ethBars := make15mHourlyHistSeries("ETHUSDT", t0, totalH, func(h int) float64 {
		if h < totalH-1 {
			return 100.0
		}
		return 115.0 // +15% pump vs BTC
	}, func(h int) float64 {
		if h == totalH-1 {
			return 3_000_000.0
		}
		if h%2 == 0 {
			return 100_000.0
		}
		return -100_000.0
	})

	f := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.01),
		MinNotional: decimal.NewFromFloat(5.0),
		MinQty:      decimal.NewFromFloat(0.01),
	}
	evalCfg := strategy.EvaluatorConfig{
		CandleDuration:      15 * time.Minute,
		MaxSlots:            4,
		MaxDailyDrawdownPct: -0.10,
		EMAPeriod:           200,
		ATRPeriod:           14,
		MaxSpreadPct:        0.20,
		DisableVWAP:         true,
		DisableOIFilter:     true,
		MaxBookStaleness:    5 * time.Second,
		MaxMarkStaleness:    15 * time.Second,
		MaxDataStaleness:    20 * time.Minute,
		MajorPod: strategy.MajorPodConfig{
			ShortEnabled:        true,
			LongEnabled:         true,
			Symbols:             []string{"BTCUSDT", "ETHUSDT"},
			MaxSlots:            3,
			ShortRiskPct:        decimal.NewFromFloat(0.010),
			LongRiskPct:         decimal.NewFromFloat(0.010),
			ShortRSMinPct:       3.0,
			LongRSMinPct:        4.5,
			LongRSMaxPct:        20.0,
			RSWindowHours:       24,
			BreakoutLookbackHrs: 12,
			MinDeltaZ:           0.40,
			LongMaxFundingRate:  0.001,
			ShortMinFundingRate: -1.0,
			SLATRMultiplier:     2.0,
			TPATRMultiplier:     6.0,
			MinSLDistancePct:    decimal.NewFromFloat(0.010),
			TrailingEnabled:     true,
			TrailingActivateATR: 2.5,
			TrailingDistanceATR: 1.2,
			MaxHoldingDuration:  48 * time.Hour,
			CooldownMinutes:     240,
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
	}
	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		CandleDuration:    15 * time.Minute,
		MaxSlots:          7,
		AccountLeverage:   10.0,
		EntryOrderType:    "MARKET",
	})
	eng := NewReplayEngine(ReplayConfig{
		WarmupBars:               (totalH - 2) * 4,
		DynamicScannerEnabled:    false,
		SettleOpenPositionsAtEnd: true,
	}, evalCfg, sim, map[string][]data.HistoricalCandle{
		"BTCUSDT": btcBars,
		"ETHUSDT": ethBars,
	}, map[string]model.ExchangeFilter{
		"BTCUSDT": f,
		"ETHUSDT": f,
	})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("replay run failed: %v", err)
	}
	if len(sim.ClosedTrades) != 0 {
		t.Fatalf("expected 0 LONG trades because full 500h BTC EMA200 > 63500 (whereas 320h truncated EMA200 was < 63500), got %+v", sim.ClosedTrades)
	}
	if eng.RejectReasons["MAJOR_SHORT_RS_NOT_WEAK"] == 0 {
		t.Fatalf("expected BTC < full 500h EMA200 to route to SHORT check (MAJOR_SHORT_RS_NOT_WEAK), got %+v", eng.RejectReasons)
	}
}


