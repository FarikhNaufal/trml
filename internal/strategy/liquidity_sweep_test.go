package strategy

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/config"
	"trml/internal/model"
)

func buildDowntrendBaseCandles(now time.Time, count int, startClose, stepDown float64) ([]model.Candle, []float64) {
	candles := make([]model.Candle, count)
	deltas := make([]float64, count)
	for i := 0; i < count; i++ {
		tOpen := now.Add(-time.Duration(count-i) * 5 * time.Minute)
		closeP := startClose - float64(i)*stepDown
		openP := closeP + 0.08
		candles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tOpen.Add(5 * time.Minute),
			Open:            openP,
			High:            openP + 0.15,
			Low:             closeP - 0.15,
			Close:           closeP,
			VolumeBase:      1000.0,
			QuoteVolumeUSDT: 1000.0 * closeP,
		}
		// Alternating delta baseline so std > 0 and z-score stays well defined.
		if i%2 == 0 {
			deltas[i] = 100.0
		} else {
			deltas[i] = -100.0
		}
	}
	return candles, deltas
}

func makeSweepTestConfig(now time.Time) EvaluatorConfig {
	return EvaluatorConfig{
		CandleDuration:           5 * time.Minute,
		MaxSlots:                 5,
		MaxBookStaleness:         5 * time.Second,
		MaxMarkStaleness:         15 * time.Second,
		MaxDataStaleness:         330 * time.Second,
		MaxSpreadPct:             0.05,
		MinNATRPct:               0.10,
		MaxNATRPct:               5.00,
		EMAPeriod:                50,
		ATRPeriod:                14,
		DeltaZScoreWindow:        50,
		MinBounceZScore:          0.75,
		MinBounceZScoreLong:      0.50,
		MinBounceZScoreShort:     0.70,
		MinEMA200DistanceATR:     0.20,
		MaxEMA200DistanceATR:     3.80,
		LookbackImpulseBars:      18,
		RetestLookbackBars:       5,
		MinImpulseATR:            0.62,
		MaxEntryDistanceATR:      1.20,
		PullbackToleranceATR:     0.38,
		EntryOrderType:           "LIMIT",
		DisableOIFilter:          true,
		MaxAdverseFundingRate:    0.0005,
		AllowedSide:              "AUTO",
		MaxFrenzyRVOL:            2.50,
		MaxCounterImpulseATR:     1.50,
		VWAPSessionWarmupMinutes: 20,
		Sweep: SweepConfig{
			Enabled:           true,
			LookbackBars:      12,
			MinUpperWickRatio: 0.25,
			MaxLowerWickRatio: 0.45,
			MaxDeltaZScore:    -0.25,
		},
		Sizing: SizingConfig{
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
}

func makeValidSweepSnapshot(now time.Time) (model.MarketSnapshot, EvaluatorConfig) {
	candles, deltas := buildDowntrendBaseCandles(now, 80, 120.0, 0.20)
	// Latest closed bar is index 79 (~104.2). Prior 12 bars [67..78] have closes falling ~106.6 -> 104.4, highs <= ~106.83.
	// Find prior 12-bar max high so we construct a clean 1-bar sweep above it.
	priorHigh := 0.0
	for i := len(candles) - 1 - 12; i < len(candles)-1; i++ {
		if candles[i].High > priorHigh {
			priorHigh = candles[i].High
		}
	}
	latest := &candles[len(candles)-1]
	latest.Open = priorHigh - 0.30
	latest.Close = priorHigh - 1.20 // Red candle closing below priorHigh
	latest.High = priorHigh + 0.60  // Swept priorHigh (upper wick = 0.90)
	latest.Low = latest.Close - 0.10 // Tiny lower wick = 0.10 (range = 1.90 -> uWick 47.4%, lWick 5.3%)
	deltas[len(deltas)-1] = -80.0    // Z = -0.80 <= -0.25 (and fails L1 if VWAP above / no retest)

	// Set VWAP well above latest.Close so it triggers Sweep cleanly (even outside L1 VWAP distance).
	snap := model.MarketSnapshot{
		Symbol: "SOLUSDT",
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now:                 now,
		BestBid:             latest.Close - 0.01,
		BestAsk:             latest.Close + 0.01,
		BookUpdatedAt:       now,
		MarkPrice:           latest.Close,
		MarkPriceUpdatedAt:  now,
		FundingRate:         0.0001,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  priorHigh + 5.0,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
	}
	return snap, makeSweepTestConfig(now)
}

func TestLiquiditySweepAlpha_ValidAndRejections(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 5, 0, 0, time.UTC)
	baseSnap, baseCfg := makeValidSweepSnapshot(now)

	// 1. Valid 1-bar SHORT sweep
	dec := EvaluatePortfolio(baseSnap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), baseCfg)
	if !dec.Approved || dec.StrategyID != StrategyIDLiquiditySweep || dec.Intent.StrategyID != StrategyIDLiquiditySweep || dec.Intent.Side != model.SideShort {
		t.Fatalf("expected approved %s SHORT, got %+v", StrategyIDLiquiditySweep, dec)
	}

	// 2. When Sweep disabled, EvaluatePortfolio rejects via Primary and never triggers Sweep
	offCfg := baseCfg
	offCfg.Sweep.Enabled = false
	offDec := EvaluatePortfolio(baseSnap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), offCfg)
	if offDec.Approved {
		t.Fatalf("expected reject when Sweep.Enabled=false, got approved %+v", offDec)
	}

	// 3. MaxDeltaZScore = 0.0 MUST work (must not override 0.0)
	zeroZSnap := baseSnap
	zeroZSnap.ClosedBarDeltasUSDT = append([]float64(nil), baseSnap.ClosedBarDeltasUSDT...)
	zeroZSnap.ClosedBarDeltasUSDT[len(zeroZSnap.ClosedBarDeltasUSDT)-1] = -5.0 // z = -0.05 (between -0.25 and 0.0)
	zeroZCfg := baseCfg
	decStrictZ := EvaluatePortfolio(zeroZSnap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), zeroZCfg)
	if decStrictZ.Approved {
		t.Fatalf("expected Z=-0.05 rejected under MaxDeltaZScore=-0.25")
	}
	zeroZCfg.Sweep.MaxDeltaZScore = 0.0
	decZeroZ := EvaluatePortfolio(zeroZSnap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), zeroZCfg)
	if !decZeroZ.Approved || decZeroZ.StrategyID != StrategyIDLiquiditySweep {
		t.Fatalf("expected Z=-0.05 approved under MaxDeltaZScore=0.0, got %+v", decZeroZ)
	}

	// 4. Reject cases when evaluating strictly as SweepExtra scope (AllowPrimary=false, AllowSweep=true)
	sweepOnly := SymbolScope{AllowPrimary: false, AllowSweep: true}
	rejectCases := []struct {
		name   string
		mutate func(s *model.MarketSnapshot, c *EvaluatorConfig)
		reason string
	}{
		{
			name: "no_sweep_high",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				n := len(s.ClosedCandles)
				s.ClosedCandles[n-1].High = s.ClosedCandles[n-2].High - 0.10
				s.ClosedCandles[n-1].Open = s.ClosedCandles[n-1].High - 0.20
				s.ClosedCandles[n-1].Close = s.ClosedCandles[n-1].Open - 0.20
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "breakout_close_above_prior_high",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				n := len(s.ClosedCandles)
				s.ClosedCandles[n-1].High += 2.0
				s.ClosedCandles[n-1].Open += 1.5
				s.ClosedCandles[n-1].Close = s.ClosedCandles[n-1].High - 1.0 // still above priorHigh
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "bullish_candle",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				n := len(s.ClosedCandles)
				s.ClosedCandles[n-1].Open, s.ClosedCandles[n-1].Close = s.ClosedCandles[n-1].Close, s.ClosedCandles[n-1].Open
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "weak_upper_wick",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				n := len(s.ClosedCandles)
				c := &s.ClosedCandles[n-1]
				c.Open = c.High - 0.05 // tiny upper wick, big red body
				c.Low = c.Close - 0.05
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "excessive_lower_wick",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				n := len(s.ClosedCandles)
				c := &s.ClosedCandles[n-1]
				c.Low = c.Close - 5.0 // huge hammer lower wick >= 45%
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "close_above_vwap",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				s.CurrentSessionVWAP = s.ClosedCandles[len(s.ClosedCandles)-1].Close - 1.0
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "close_above_ema200",
			mutate: func(s *model.MarketSnapshot, _ *EvaluatorConfig) {
				n := len(s.ClosedCandles)
				s.ClosedCandles[n-1].Close = 130.0
				s.ClosedCandles[n-1].Open = 131.0
				s.ClosedCandles[n-1].High = 135.0
				s.ClosedCandles[n-1].Low = 129.0
				s.CurrentSessionVWAP = 135.0
			},
			reason: "SWEEP_CONDITIONS_NOT_MET",
		},
		{
			name: "allowed_side_long_only",
			mutate: func(_ *model.MarketSnapshot, c *EvaluatorConfig) {
				c.AllowedSide = "LONG_ONLY"
			},
			reason: "SIDE_DISABLED_LONG_ONLY",
		},
	}

	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			snap := baseSnap
			snap.ClosedCandles = append([]model.Candle(nil), baseSnap.ClosedCandles...)
			snap.ClosedBarDeltasUSDT = append([]float64(nil), baseSnap.ClosedBarDeltasUSDT...)
			cfg := baseCfg
			tc.mutate(&snap, &cfg)

			r := EvaluatePortfolio(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), sweepOnly, cfg)
			if r.Approved || r.Reason != tc.reason {
				t.Fatalf("%s: expected reject %s, got approved=%v reason=%s sub=%s", tc.name, tc.reason, r.Approved, r.Reason, r.SubReason)
			}
		})
	}
}

func TestEvaluatePortfolio_BTCPostFilterBlocksSweep(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 5, 0, 0, time.UTC)
	snap, cfg := makeValidSweepSnapshot(now)
	cfg.BTCGateEnabled = true
	cfg.BTCMax5mPumpPct = 0.65
	cfg.BTCCounterMomentumPct = 0.15

	btcCandles, btcDeltas := buildDowntrendBaseCandles(now, 80, 60000.0, 10.0)
	// Last BTC bar pumps +0.80% (triggers BTC_FLASH_PUMP)
	btcCandles[len(btcCandles)-1].Open = 59000.0
	btcCandles[len(btcCandles)-1].Close = 59472.0
	btcCandles[len(btcCandles)-1].High = 59500.0
	btcCandles[len(btcCandles)-1].Low = 58950.0
	btcSnap := &model.MarketSnapshot{
		Symbol:              "BTCUSDT",
		Now:                 now,
		CurrentSessionVWAP:  60000.0,
		ClosedCandles:       btcCandles,
		ClosedBarDeltasUSDT: btcDeltas,
	}

	dec := EvaluatePortfolio(snap, btcSnap, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), SymbolScope{AllowPrimary: false, AllowSweep: true}, cfg)
	if dec.Approved || dec.Reason != "BTC_FLASH_PUMP" {
		t.Fatalf("expected BTC_FLASH_PUMP block, got approved=%v reason=%s", dec.Approved, dec.Reason)
	}
}

func TestEvaluatePortfolio_PrimaryPriorityAndScopeIsolation(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 5, 0, 0, time.UTC)
	snap, cfg := makeValidSweepSnapshot(now)
	// Adjust latest bar + VWAP + prior impulse so BOTH L1 Primary SHORT AND Sweep SHORT are valid simultaneously!
	n := len(snap.ClosedCandles)
	priorHigh := 0.0
	for i := n - 1 - 12; i < n-1; i++ {
		if snap.ClosedCandles[i].High > priorHigh {
			priorHigh = snap.ClosedCandles[i].High
		}
	}
	vwap := priorHigh - 0.05
	snap.CurrentSessionVWAP = vwap
	// Ensure prior bar n-3 dipped >= 0.62*ATR below VWAP (and n-2 did not invalidate or close too far above)
	snap.ClosedCandles[n-3].Low = vwap - 1.5
	snap.ClosedCandles[n-3].Close = vwap - 0.40
	snap.ClosedCandles[n-2].Open = vwap - 0.35
	snap.ClosedCandles[n-2].Close = vwap - 0.25
	snap.ClosedCandles[n-2].High = vwap - 0.10
	// Latest bar came from below, tested VWAP within [vwap-0.38*ATR .. vwap+0.75*ATR], AND swept priorHigh!
	latest := &snap.ClosedCandles[n-1]
	latest.Open = priorHigh - 0.10
	latest.High = priorHigh + 0.15 // vwap + 0.20 (well within maxAdverseRetestDist = 0.75*ATR)
	latest.Close = vwap - 0.15     // below VWAP & below priorHigh (uWick=0.25, body=0.20, lWick=0.02 -> uWick 53% >= 25%)
	latest.Low = latest.Close - 0.02
	snap.BestBid = latest.Close - 0.01
	snap.BestAsk = latest.Close + 0.01
	snap.MarkPrice = latest.Close
	snap.ClosedBarDeltasUSDT[n-1] = -90.0 // Z = -0.90 (passes both -0.70 Primary and -0.25 Sweep)

	// Adjust EMA distance limit in this synthetic test so Primary & Sweep are both valid simultaneously
	cfg.MaxEMA200DistanceATR = 10.0

	// 1. Default scope: Primary MUST win over Sweep
	bothDec := EvaluatePortfolio(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), cfg)
	if !bothDec.Approved || bothDec.StrategyID != StrategyIDVWAPPullback {
		_, okP, rP, srP := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), cfg)
		t.Fatalf("expected Primary %s priority win, got %+v (primary debug: ok=%v reason=%s sub=%s)", StrategyIDVWAPPullback, bothDec, okP, rP, srP)
	}

	// 2. Sweep-only scope: Primary MUST NOT execute even when textbook valid; Sweep handles it cleanly
	sweepScopeDec := EvaluatePortfolio(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), SymbolScope{AllowPrimary: false, AllowSweep: true}, cfg)
	if !sweepScopeDec.Approved || sweepScopeDec.StrategyID != StrategyIDLiquiditySweep {
		t.Fatalf("expected Sweep %s under AllowPrimary=false, got %+v", StrategyIDLiquiditySweep, sweepScopeDec)
	}

	// 3. EvaluateSignal remains 100% identical with Sweep enabled vs disabled on pure sweep candle
	snapPureSweep, _ := makeValidSweepSnapshot(now)
	_, okOff, rOff, _ := EvaluateSignal(snapPureSweep, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), cfg)
	cfgOff := cfg
	cfgOff.Sweep.Enabled = false
	_, okOff2, rOff2, _ := EvaluateSignal(snapPureSweep, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), cfgOff)
	if okOff != okOff2 || rOff != rOff2 || okOff {
		t.Fatalf("EvaluateSignal must ignore Sweep config: on=(%v,%s) off=(%v,%s)", okOff, rOff, okOff2, rOff2)
	}
}

func TestFromAppConfig_PropagatesSweepAndCore(t *testing.T) {
	appCfg := &config.Config{
		EMAPeriod:     200,
		AllowedSide:   "AUTO",
		EntryOrderType: "LIMIT",
		Sweep: config.SweepStrategyConfig{
			Enabled:           true,
			LookbackBars:      14,
			MinUpperWickRatio: 0.30,
			MaxLowerWickRatio: 0.40,
			MaxDeltaZScore:    -0.15,
		},
	}
	evalCfg := FromAppConfig(appCfg)
	if !evalCfg.Sweep.Enabled || evalCfg.Sweep.LookbackBars != 14 || evalCfg.Sweep.MinUpperWickRatio != 0.30 ||
		evalCfg.Sweep.MaxLowerWickRatio != 0.40 || evalCfg.Sweep.MaxDeltaZScore != -0.15 {
		t.Fatalf("FromAppConfig failed to propagate Sweep config: %+v", evalCfg.Sweep)
	}
}

func TestEvaluatePortfolio_SameScanSweepRejectionAttribution(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 5, 0, 0, time.UTC)
	snap, cfg := makeValidSweepSnapshot(now)

	// Mutate delta so Sweep rejects on WEAK_DELTA_ZSCORE while Primary rejects on ENTRY_TOO_FAR_FROM_VWAP
	// under DefaultSymbolScope (SameScan 1B: AllowPrimary=true, AllowSweep=true).
	snap.ClosedBarDeltasUSDT[len(snap.ClosedBarDeltasUSDT)-1] = 80.0
	dec := EvaluatePortfolio(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), cfg)
	if dec.Approved || dec.StrategyID != StrategyIDLiquiditySweep || dec.Reason != "SWEEP_CONDITIONS_NOT_MET" {
		t.Fatalf("expected SameScan bearish sweep-zone rejection attributed to %s, got %+v", StrategyIDLiquiditySweep, dec)
	}
}
