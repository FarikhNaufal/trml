package strategy

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/indicator"
	"trml/internal/model"
)

func buildHourlyBTCCandles(now time.Time, count int, fn func(i int) (openP, highP, lowP, closeP float64)) []model.Candle {
	out := make([]model.Candle, 0, count*12)
	startHour := now.UTC().Truncate(time.Hour).Add(-time.Duration(count) * time.Hour)
	for h := 0; h < count; h++ {
		o, hi, lo, cl := fn(h)
		for sub := 0; sub < 12; sub++ {
			tOpen := startHour.Add(time.Duration(h)*time.Hour + time.Duration(sub)*5*time.Minute)
			subOpen := o
			subClose := o + (cl-o)*float64(sub+1)/12.0
			if sub > 0 {
				subOpen = o + (cl-o)*float64(sub)/12.0
			}
			subHigh := subOpen
			if subClose > subHigh {
				subHigh = subClose
			}
			subLow := subOpen
			if subClose < subLow {
				subLow = subClose
			}
			if sub == 5 && hi > subHigh {
				subHigh = hi
			}
			if sub == 8 && lo < subLow {
				subLow = lo
			}
			out = append(out, model.Candle{
				OpenTime:  tOpen,
				CloseTime: tOpen.Add(5*time.Minute - time.Millisecond),
				Open:      subOpen,
				High:      subHigh,
				Low:       subLow,
				Close:     subClose,
			})
		}
	}
	return out
}

func makeMultiRegimeBaseCfg(now time.Time) EvaluatorConfig {
	cfg := makeSweepTestConfig(now)
	cfg.Sweep.Enabled = false
	cfg.MaxSpreadPct = 0.20
	cfg.VWAPSessionWarmupMinutes = 5
	cfg.AllowedSide = "AUTO"
	return cfg
}

func makeSnap(sym string, now time.Time, candles []model.Candle, deltas []float64, vwap float64) model.MarketSnapshot {
	last := candles[len(candles)-1]
	return model.MarketSnapshot{
		Symbol:              sym,
		Now:                 now,
		BestBid:             last.Close * 0.9999,
		BestAsk:             last.Close * 1.0001,
		BookUpdatedAt:       now,
		MarkPrice:           last.Close,
		MarkPriceUpdatedAt:  now,
		FundingRate:         0.0001,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  vwap,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
	}
}

func TestClassifyMacroRegime_States(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	upCandles := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 60000.0 + float64(i)*120.0
		return base, base + 80.0, base - 20.0, base + 120.0
	})
	if m := ClassifyMacroRegime(upCandles, 0.30, 1.50); m.Regime != MacroRegimeTrendUp {
		t.Fatalf("expected TREND_UP, got %s (ER=%.2f Vol=%.2f)", m.Regime, m.EfficiencyRatio, m.VolRatio1h)
	}

	downCandles := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 70000.0 - float64(i)*120.0
		return base, base + 20.0, base - 80.0, base - 120.0
	})
	if m := ClassifyMacroRegime(downCandles, 0.30, 1.50); m.Regime != MacroRegimeTrendDown {
		t.Fatalf("expected TREND_DOWN, got %s (ER=%.2f Vol=%.2f)", m.Regime, m.EfficiencyRatio, m.VolRatio1h)
	}

	rangeCandles := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		dir := 60.0
		if i%2 == 1 {
			dir = -60.0
		}
		base := 65000.0
		if dir > 0 {
			return base, base + 90.0, base - 30.0, base + 60.0
		}
		return base + 60.0, base + 90.0, base - 30.0, base
	})
	if m := ClassifyMacroRegime(rangeCandles, 0.30, 1.50); m.Regime != MacroRegimeRange {
		t.Fatalf("expected RANGE, got %s (ER=%.2f Vol=%.2f)", m.Regime, m.EfficiencyRatio, m.VolRatio1h)
	}

	shockCandles := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 65000.0
		if i >= 46 {
			return base, base + 900.0, base - 900.0, base + 50.0
		}
		return base, base + 80.0, base - 80.0, base + 10.0
	})
	if m := ClassifyMacroRegime(shockCandles, 0.30, 1.50); m.Regime != MacroRegimeVolShock {
		t.Fatalf("expected VOL_SHOCK, got %s (ER=%.2f Vol=%.2f)", m.Regime, m.EfficiencyRatio, m.VolRatio1h)
	}

	// Verify incomplete trailing 15m bar in a new UTC hour is excluded so regime doesn't flicker.
	partialBar := model.Candle{
		OpenTime:  now.UTC().Truncate(time.Hour),
		CloseTime: now.UTC().Truncate(time.Hour).Add(15*time.Minute - time.Millisecond),
		Open:      65000, High: 72000, Low: 60000, Close: 71000,
	}
	withPartial := append(append([]model.Candle(nil), rangeCandles...), partialBar)
	mPartial := ClassifyMacroRegime(withPartial, 0.30, 1.50)
	if mPartial.HourlyBars != 60 || mPartial.Regime != MacroRegimeRange {
		t.Fatalf("expected partial hour ignored (60 bars, RANGE), got %d bars %s", mPartial.HourlyBars, mPartial.Regime)
	}
}

func TestRelativeResidualAlpha_MeanRevPumpFadeAboveEMA200(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	n := 70
	altCandles := make([]model.Candle, n)
	btcCandles := make([]model.Candle, n)
	deltas := make([]float64, n)

	for i := 0; i < n; i++ {
		tOpen := now.Add(-time.Duration(n-i) * 5 * time.Minute)
		btcClose := 60000.0
		btcCandles[i] = model.Candle{
			OpenTime: tOpen, CloseTime: tOpen.Add(5 * time.Minute),
			Open: btcClose - 10, High: btcClose + 30, Low: btcClose - 30, Close: btcClose,
			VolumeBase: 500, QuoteVolumeUSDT: 500 * btcClose,
		}
		base := 100.0 + float64(i)*0.05
		altCandles[i] = model.Candle{
			OpenTime: tOpen, CloseTime: tOpen.Add(5 * time.Minute),
			Open: base - 0.1, High: base + 0.3, Low: base - 0.3, Close: base,
			VolumeBase: 1000, QuoteVolumeUSDT: 1000 * base,
		}
		if i%2 == 0 {
			deltas[i] = 100.0
		} else {
			deltas[i] = -100.0
		}
	}

	// Spike altcoin over last 12 bars up +8%, then final bar forms bearish upper-wick rejection with negative delta.
	for i := n - 12; i < n-1; i++ {
		p := altCandles[i-1].Close * 1.0075
		altCandles[i].Open = p - 0.1
		altCandles[i].Close = p
		altCandles[i].High = p + 0.2
		altCandles[i].Low = p - 0.2
	}
	prev := altCandles[n-2].Close
	altCandles[n-1].Open = prev
	altCandles[n-1].High = prev + 1.2
	altCandles[n-1].Low = prev - 0.6
	altCandles[n-1].Close = prev - 0.4
	deltas[n-1] = -400.0

	cfg := makeMultiRegimeBaseCfg(now)
	cfg.DisableVWAP = true
	cfg.AllowedSide = "SHORT_ONLY" // Must still allow SHORT fade even though Close > EMA200!
	cfg.RelResidual = RelResidualConfig{
		Enabled:           true,
		Mode:              "MEANREV",
		LookbackBars:      12,
		MinRelZ:           1.50,
		MinVWAPStretchATR: 1.00,
		MinDeltaZ:         0.20,
		BypassBTCTier2:    true,
	}

	altSnap := makeSnap("SOLUSDT", now, altCandles, deltas, 102.0)
	btcSnap := makeSnap("BTCUSDT", now, btcCandles, deltas, 60000.0)
	dec := EvaluatePortfolio(altSnap, &btcSnap, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), cfg)
	if !dec.Approved || dec.StrategyID != StrategyIDRelResidual || dec.Intent.Side != model.SideShort {
		t.Fatalf("expected approved SHORT REL_RESIDUAL signal, got approved=%v id=%s reason=%s sub=%s",
			dec.Approved, dec.StrategyID, dec.Reason, dec.SubReason)
	}
}

func TestVolBreakoutAlpha_CompressedBreakoutVsChop(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	n := 80
	candles := make([]model.Candle, n)
	deltas := make([]float64, n)

	for i := 0; i < n; i++ {
		tOpen := now.Add(-time.Duration(n-i) * 5 * time.Minute)
		// Early high volatility (bars 0..45) -> later tight squeeze (bars 46..78)
		amp := 2.0
		if i >= 45 {
			amp = 0.25
		}
		base := 100.0
		if i%2 == 0 {
			candles[i] = model.Candle{
				OpenTime: tOpen, CloseTime: tOpen.Add(5 * time.Minute),
				Open: base, High: base + amp, Low: base - amp*0.5, Close: base + amp*0.4,
				VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
			}
			deltas[i] = 50
		} else {
			candles[i] = model.Candle{
				OpenTime: tOpen, CloseTime: tOpen.Add(5 * time.Minute),
				Open: base + amp*0.3, High: base + amp*0.6, Low: base - amp, Close: base - amp*0.1,
				VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
			}
			deltas[i] = -50
		}
	}
	// Strong bullish Donchian breakout bar with high RVOL, full body, above EMA200 & VWAP.
	candles[n-1] = model.Candle{
		OpenTime: now.Add(-5 * time.Minute), CloseTime: now,
		Open: 100.2, High: 102.2, Low: 100.1, Close: 102.0,
		VolumeBase: 500, QuoteVolumeUSDT: 500 * 102.0,
	}
	deltas[n-1] = 350.0

	cfg := makeMultiRegimeBaseCfg(now)
	cfg.DisableVWAP = true
	cfg.VolBreakout = VolBreakoutConfig{
		Enabled:             true,
		LookbackBars:        20,
		MaxCompressionRatio: 0.90,
		MinRVOL:             1.50,
		MinBodyRatio:        0.50,
		MinDeltaZ:           0.75,
	}

	snap := makeSnap("ETHUSDT", now, candles, deltas, 100.0)
	dec := EvaluatePortfolio(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), cfg)
	if !dec.Approved || dec.StrategyID != StrategyIDVolBreakout || dec.Intent.Side != model.SideLong {
		t.Fatalf("expected approved LONG VOL_BREAKOUT signal, got approved=%v id=%s reason=%s sub=%s",
			dec.Approved, dec.StrategyID, dec.Reason, dec.SubReason)
	}

	// Verify uncompressed market rejects breakout
	cfg.VolBreakout.MaxCompressionRatio = 0.05
	decFail := EvaluatePortfolio(snap, nil, 0, false, false, decimal.NewFromFloat(500), decimal.NewFromFloat(500), DefaultSymbolScope(), cfg)
	if decFail.Approved || decFail.Reason != "BREAKOUT_NO_COMPRESSION" {
		t.Fatalf("expected BREAKOUT_NO_COMPRESSION rejection, got approved=%v reason=%s", decFail.Approved, decFail.Reason)
	}
}

func TestCalculateRollingVWAP_ContinuousWindow(t *testing.T) {
	day1 := time.Date(2026, 6, 15, 23, 30, 0, 0, time.UTC)
	candles := []model.Candle{
		{OpenTime: day1, CloseTime: day1.Add(15 * time.Minute), High: 100, Low: 100, Close: 100, VolumeBase: 10},
		{OpenTime: day1.Add(15 * time.Minute), CloseTime: day1.Add(30 * time.Minute), High: 100, Low: 100, Close: 100, VolumeBase: 10},
		{OpenTime: day1.Add(30 * time.Minute), CloseTime: day1.Add(45 * time.Minute), High: 110, Low: 110, Close: 110, VolumeBase: 10}, // next UTC day
	}
	sess := indicator.CalculateSessionVWAP(candles)
	roll := indicator.CalculateRollingVWAP(candles, 3)
	if math.Abs(sess-110.0) > 1e-6 {
		t.Fatalf("expected midnight-reset SessionVWAP=110.0, got %.4f", sess)
	}
	if math.Abs(roll-103.333333) > 1e-3 {
		t.Fatalf("expected RollingVWAP=103.3333, got %.4f", roll)
	}
}

func TestFundingEnvelope_CrowdingFilter(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	candles, deltas := buildDowntrendBaseCandles(now, 80, 120.0, 0.20)
	// Reuse MakeValidSweep-style setup directly on VolBreakout/Sweep
	snap := makeSnap("BTC2USDT", now, candles, deltas, 100.0)
	snap.FundingRate = -0.0003
	cfg := makeMultiRegimeBaseCfg(now)
	cfg.FundingEnvelope = FundingEnvelopeConfig{
		MinShortEnabled: true,
		MinShortRate:    -0.0001,
	}
	ok, reason, _ := evaluateSideAndPostFiltersWithOpts(sharedMarketContext{NATRPct: 1.0, ATR14: 0.5}, snap, nil, model.SideShort, cfg, false)
	if ok || reason != "FUNDING_CROWDING_FILTER" {
		t.Fatalf("expected FUNDING_CROWDING_FILTER when fr=-0.0003 < -0.0001, got ok=%v reason=%s", ok, reason)
	}
}

func TestHTFEMAAlignment_And_MarketBreadthGate(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	// 1. HTF EMA warmup check when hourly bars < HTFEMAPeriod
	shortCandles := buildHourlyBTCCandles(now, 4, func(i int) (float64, float64, float64, float64) {
		return 100, 101, 99, 100
	})
	cfg := makeMultiRegimeBaseCfg(now)
	cfg.HTFEMAPeriod = 10
	ctx := sharedMarketContext{Latest: shortCandles[len(shortCandles)-1], NATRPct: 1.0, ATR14: 0.5}
	snapWarm := makeSnap("TESTUSDT", now, shortCandles, make([]float64, len(shortCandles)), 100.0)
	if ok, r, _ := evaluateSideAndPostFiltersWithOpts(ctx, snapWarm, nil, model.SideShort, cfg, false); ok || r != "HTF_EMA_WARMING_UP" {
		t.Fatalf("expected HTF_EMA_WARMING_UP, got ok=%v reason=%s", ok, r)
	}

	// 2. Downtrend hourly candles: Close < 1h EMA10 passes SHORT; Close > 1h EMA10 fails SHORT
	downCandles := buildHourlyBTCCandles(now, 15, func(i int) (float64, float64, float64, float64) {
		p := 120.0 - float64(i)*1.0
		return p, p + 0.5, p - 0.5, p - 0.8
	})
	snapDown := makeSnap("TESTUSDT", now, downCandles, make([]float64, len(downCandles)), 105.0)
	ctxDown := sharedMarketContext{Latest: downCandles[len(downCandles)-1], NATRPct: 1.0, ATR14: 0.5}
	if ok, r, _ := evaluateSideAndPostFiltersWithOpts(ctxDown, snapDown, nil, model.SideShort, cfg, false); !ok {
		t.Fatalf("expected SHORT to pass below 1h EMA10, got rejected: %s", r)
	}

	upCandles := buildHourlyBTCCandles(now, 15, func(i int) (float64, float64, float64, float64) {
		p := 100.0 + float64(i)*1.0
		return p, p + 0.5, p - 0.5, p + 0.8
	})
	snapUp := makeSnap("TESTUSDT", now, upCandles, make([]float64, len(upCandles)), 114.0)
	ctxUp := sharedMarketContext{Latest: upCandles[len(upCandles)-1], NATRPct: 1.0, ATR14: 0.5}
	if ok, r, _ := evaluateSideAndPostFiltersWithOpts(ctxUp, snapUp, nil, model.SideShort, cfg, false); ok || r != "HTF_EMA_TREND_MISMATCH" {
		t.Fatalf("expected HTF_EMA_TREND_MISMATCH for SHORT above 1h EMA10, got ok=%v reason=%s", ok, r)
	}

	// 3. Cross-sectional Market Breadth hard veto (mult=0) & tiered risk scaling (mult=0.4)
	cfgBreadth := cfg
	cfgBreadth.HTFEMAPeriod = 0
	cfgBreadth.MarketBreadthLowThreshold = 0.20
	cfgBreadth.MarketBreadthLowRiskMult = 0.0
	snapDown.MarketBreadthValid = true
	snapDown.MarketBreadthPct = 0.12
	if ok, r, _ := evaluateSideAndPostFiltersWithOpts(ctxDown, snapDown, nil, model.SideShort, cfgBreadth, false); ok || r != "MARKET_BREADTH_CAPITULATION_VETO" {
		t.Fatalf("expected MARKET_BREADTH_CAPITULATION_VETO, got ok=%v reason=%s", ok, r)
	}

	// Tiered scaling (mult=0.40) reduces position size proportionally vs healthy breadth (>= 0.20)
	cfgScaled := makeMultiRegimeBaseCfg(now)
	cfgScaled.MarketBreadthLowThreshold = 0.20
	cfgScaled.MarketBreadthLowRiskMult = 0.40
	cfgScaled.Sizing.RiskPerTradePct = decimal.NewFromFloat(0.02)
	cfgScaled.Sizing.SLMultiplier = 2.0
	cfgScaled.Sizing.TPMultiplier = 5.0
	cfgScaled.Sizing.MaxSlotMarginFraction = decimal.NewFromFloat(0.50)
	cfgScaled.Sizing.AvailableBalanceBufferFraction = decimal.NewFromFloat(0.90)
	cfgScaled.Sizing.AccountLeverage = 8.0

	snapNormal := snapDown
	snapNormal.MarketBreadthValid = true
	snapNormal.MarketBreadthPct = 0.35
	eq := decimal.NewFromFloat(1000.0)
	intentNorm, okNorm, rNorm, _ := resolveEntryAndBuildIntent(ctxDown, snapNormal, nil, model.SideShort, StrategyIDVWAPPullback, eq, eq, cfgScaled)
	intentLow, okLow, rLow, _ := resolveEntryAndBuildIntent(ctxDown, snapDown, nil, model.SideShort, StrategyIDVWAPPullback, eq, eq, cfgScaled)
	if !okNorm || !okLow {
		t.Fatalf("expected both intents valid, got okNorm=%v(%s) okLow=%v(%s)", okNorm, rNorm, okLow, rLow)
	}
	ratio := intentLow.Qty.InexactFloat64() / intentNorm.Qty.InexactFloat64()
	if ratio < 0.35 || ratio > 0.45 {
		t.Fatalf("expected scaled qty ~0.40x normal qty, got low=%s normal=%s (ratio=%.3f)", intentLow.Qty, intentNorm.Qty, ratio)
	}
}

func TestRound3_HTFSlope_DDThrottle_MacroRangeMult_BreakoutPriority(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	// 1. HTFEMAMaxRisingSlopePct rejects SHORT when 24h slope of 1h EMA10 exceeds cap
	risingThenDip := buildHourlyBTCCandles(now, 40, func(i int) (float64, float64, float64, float64) {
		if i == 39 {
			return 136.0, 136.5, 129.5, 130.0 // Close < EMA10 (~144), while EMA10 itself still rose sharply vs 24h prior
		}
		p := 100.0 + float64(i)*1.3
		return p, p + 0.5, p - 0.5, p + 1.1
	})
	cfgSlope := makeMultiRegimeBaseCfg(now)
	cfgSlope.HTFEMAPeriod = 10
	cfgSlope.HTFEMAMaxRisingSlopePct = 0.20
	snapSlope := makeSnap("ALTUSDT", now, risingThenDip, make([]float64, len(risingThenDip)), 131.0)
	ctxSlope := sharedMarketContext{Latest: risingThenDip[len(risingThenDip)-1], NATRPct: 1.0, ATR14: 0.5}
	if ok, r, _ := evaluateSideAndPostFiltersWithOpts(ctxSlope, snapSlope, nil, model.SideShort, cfgSlope, false); ok || r != "HTF_EMA_SLOPE_RISING" {
		t.Fatalf("expected HTF_EMA_SLOPE_RISING, got ok=%v reason=%s", ok, r)
	}

	// 2. DDThrottleThresholdPct scales position qty proportionally once PortfolioDrawdownPct >= threshold
	downCandles, downDeltas := buildDowntrendBaseCandles(now, 80, 120.0, 0.20)
	ctxDD := sharedMarketContext{Latest: downCandles[len(downCandles)-1], NATRPct: 1.0, ATR14: 0.5}
	cfgDD := makeMultiRegimeBaseCfg(now)
	cfgDD.DDThrottleThresholdPct = 0.10
	cfgDD.DDThrottleRiskMult = 0.50
	cfgDD.Sizing.RiskPerTradePct = decimal.NewFromFloat(0.02)
	cfgDD.Sizing.SLMultiplier = 2.0
	cfgDD.Sizing.TPMultiplier = 5.0
	cfgDD.Sizing.MaxSlotMarginFraction = decimal.NewFromFloat(0.50)
	cfgDD.Sizing.AvailableBalanceBufferFraction = decimal.NewFromFloat(0.90)
	cfgDD.Sizing.AccountLeverage = 8.0

	eq := decimal.NewFromFloat(1000.0)
	snapNoDD := makeSnap("ALTUSDT", now, downCandles, downDeltas, 100.0)
	snapNoDD.PortfolioDrawdownPct = 0.05
	snapWithDD := snapNoDD
	snapWithDD.PortfolioDrawdownPct = 0.12
	intNoDD, ok1, r1, _ := resolveEntryAndBuildIntent(ctxDD, snapNoDD, nil, model.SideShort, StrategyIDVWAPPullback, eq, eq, cfgDD)
	intWithDD, ok2, r2, _ := resolveEntryAndBuildIntent(ctxDD, snapWithDD, nil, model.SideShort, StrategyIDVWAPPullback, eq, eq, cfgDD)
	if !ok1 || !ok2 {
		t.Fatalf("expected valid intents, got ok1=%v(%s) ok2=%v(%s)", ok1, r1, ok2, r2)
	}
	ddRatio := intWithDD.Qty.InexactFloat64() / intNoDD.Qty.InexactFloat64()
	if ddRatio < 0.45 || ddRatio > 0.55 {
		t.Fatalf("expected DD throttle qty ~0.50x, got %s vs %s (ratio=%.3f)", intWithDD.Qty, intNoDD.Qty, ddRatio)
	}

	// 3. MacroRouter.RangeRiskMult throttles in RANGE vs TREND_UP, and BreakoutPriorityFirst flips priority
	n := 80
	candles := make([]model.Candle, n)
	deltas := make([]float64, n)
	for i := 0; i < n; i++ {
		tOpen := now.Add(-time.Duration(n-i) * 5 * time.Minute)
		amp := 2.0
		if i >= 45 {
			amp = 0.30
		}
		base := 100.0
		if i%2 == 0 {
			candles[i] = model.Candle{
				OpenTime: tOpen, CloseTime: tOpen.Add(5 * time.Minute),
				Open: base, High: base + amp, Low: base - amp*0.4, Close: base + amp*0.3,
				VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
			}
			deltas[i] = 50
		} else {
			candles[i] = model.Candle{
				OpenTime: tOpen, CloseTime: tOpen.Add(5 * time.Minute),
				Open: base + amp*0.2, High: base + amp*0.5, Low: base - amp, Close: base - amp*0.1,
				VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
			}
			deltas[i] = -50
		}
	}
	// Prior impulse + VWAP retest before final Donchian breakout bar so BOTH VWAP and Breakout qualify!
	candles[n-4].High = 100.90
	candles[n-4].Close = 100.60
	candles[n-3].Open = 100.50
	candles[n-3].Low = 99.95 // touches VWAP 100.0 coming from above
	candles[n-3].Close = 100.20
	candles[n-2].Open = 100.20
	candles[n-2].High = 100.50
	candles[n-2].Low = 100.05
	candles[n-2].Close = 100.25
	candles[n-1] = model.Candle{
		OpenTime: now.Add(-5 * time.Minute), CloseTime: now,
		Open: 100.25, High: 101.60, Low: 100.15, Close: 101.40,
		VolumeBase: 500, QuoteVolumeUSDT: 500 * 101.40,
	}
	deltas[n-1] = 400.0

	btcRange := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 65000.0
		if i%2 == 0 {
			return base, base + 90, base - 30, base + 60
		}
		return base + 60, base + 90, base - 30, base
	})
	btcTrendUp := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 60000.0 + float64(i)*120.0
		return base, base + 80, base - 20, base + 120
	})

	cfgR3 := makeMultiRegimeBaseCfg(now)
	cfgR3.AllowedSide = "BOTH"
	cfgR3.MinImpulseATR = 0.50
	cfgR3.MaxEntryDistanceATR = 3.50
	cfgR3.VolBreakout = VolBreakoutConfig{
		Enabled: true, LookbackBars: 12, MaxCompressionRatio: 0.90,
		MinRVOL: 1.40, MinBodyRatio: 0.45, MinDeltaZ: 0.50,
	}
	cfgR3.MacroRouter = MacroRegimeRouterConfig{
		Enabled: true, TrendERThreshold: 0.30, VolShockRatio: 1.50, RangeRiskMult: 0.50,
	}
	cfgR3.Sizing.RiskPerTradePct = decimal.NewFromFloat(0.02)
	cfgR3.Sizing.SLMultiplier = 2.0
	cfgR3.Sizing.TPMultiplier = 5.0
	cfgR3.Sizing.MaxSlotMarginFraction = decimal.NewFromFloat(0.50)
	cfgR3.Sizing.AvailableBalanceBufferFraction = decimal.NewFromFloat(0.90)
	cfgR3.Sizing.AccountLeverage = 8.0

	altSnap := makeSnap("ETHUSDT", now, candles, deltas, 100.0)
	btcRangeSnap := makeSnap("BTCUSDT", now, btcRange, deltas, 65000.0)
	btcUpSnap := makeSnap("BTCUSDT", now, btcTrendUp, deltas, 67000.0)

	decRange := EvaluatePortfolio(altSnap, &btcRangeSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgR3)
	decTrend := EvaluatePortfolio(altSnap, &btcUpSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgR3)
	if !decRange.Approved || !decTrend.Approved {
		t.Fatalf("expected both approved, got range=%v(%s) trend=%v(%s)", decRange.Approved, decRange.Reason, decTrend.Approved, decTrend.Reason)
	}
	if decRange.StrategyID != StrategyIDVWAPPullback {
		t.Fatalf("expected VWAP_PULLBACK first by default, got %s", decRange.StrategyID)
	}
	rngRatio := decRange.Intent.Qty.InexactFloat64() / decTrend.Intent.Qty.InexactFloat64()
	if rngRatio < 0.45 || rngRatio > 0.55 {
		t.Fatalf("expected RANGE risk throttle ~0.50x TREND_UP qty, got %s vs %s (ratio=%.3f)", decRange.Intent.Qty, decTrend.Intent.Qty, rngRatio)
	}

	// Zero-value RangeRiskMult=0 must NOT throttle
	cfgNoThrottle := cfgR3
	cfgNoThrottle.MacroRouter.RangeRiskMult = 0.0
	decNoThrottle := EvaluatePortfolio(altSnap, &btcRangeSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgNoThrottle)
	if !decNoThrottle.Approved || !decNoThrottle.Intent.Qty.Equal(decTrend.Intent.Qty) {
		t.Fatalf("expected zero-value RangeRiskMult not to throttle: %s vs %s", decNoThrottle.Intent.Qty, decTrend.Intent.Qty)
	}

	// BreakoutPriorityFirst=true must select VOL_BREAKOUT before VWAP_PULLBACK when both qualify
	cfgR3.BreakoutPriorityFirst = true
	decBOPrio := EvaluatePortfolio(altSnap, &btcUpSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgR3)
	if !decBOPrio.Approved || decBOPrio.StrategyID != StrategyIDVolBreakout {
		t.Fatalf("expected VOL_BREAKOUT first when BreakoutPriorityFirst=true, got approved=%v id=%s (%s)", decBOPrio.Approved, decBOPrio.StrategyID, decBOPrio.Reason)
	}
}

func TestFinalOptInGates_VolBreakoutAndShortVWAP(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	eq := decimal.NewFromInt(1000)

	// Build 220 bearish 5m bars with squeeze + final breakout bar down, plus VWAP pullback test
	n := 220
	candles := make([]model.Candle, n)
	deltas := make([]float64, n)
	for i := 0; i < n; i++ {
		base := 105.0 - float64(i)*0.02
		if i < n-30 {
			candles[i] = model.Candle{
				OpenTime: now.Add(time.Duration(i-n) * 5 * time.Minute),
				CloseTime: now.Add(time.Duration(i-n+1) * 5 * time.Minute),
				Open: base + 0.4, High: base + 0.9, Low: base - 0.9, Close: base - 0.3,
				VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
			}
		} else {
			candles[i] = model.Candle{
				OpenTime: now.Add(time.Duration(i-n) * 5 * time.Minute),
				CloseTime: now.Add(time.Duration(i-n+1) * 5 * time.Minute),
				Open: 100.1, High: 100.25, Low: 99.85, Close: 99.95,
				VolumeBase: 100, QuoteVolumeUSDT: 9995,
			}
		}
		if i%2 == 0 {
			deltas[i] = -50
		} else {
			deltas[i] = -20
		}
	}
	candles[n-4].High = 99.60
	candles[n-4].Low = 99.10
	candles[n-4].Close = 99.40
	candles[n-3].Open = 99.50
	candles[n-3].High = 100.25 // pierces above VWAP 100.0 by 0.25 (~0.37 ATR)
	candles[n-3].Low = 99.45
	candles[n-3].Close = 99.80
	candles[n-2].Open = 99.74
	candles[n-2].High = 99.74
	candles[n-2].Low = 99.71
	candles[n-2].Close = 99.73
	candles[n-1] = model.Candle{
		OpenTime: now.Add(-5 * time.Minute), CloseTime: now,
		Open: 99.48, High: 99.50, Low: 98.40, Close: 98.55,
		VolumeBase: 500, QuoteVolumeUSDT: 500 * 98.55,
	}
	deltas[n-1] = -400.0

	btcTrendUp := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 60000.0 + float64(i)*120.0
		return base, base + 80, base - 20, base + 120
	})
	btcTrendDown := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
		base := 70000.0 - float64(i)*120.0
		return base, base + 20, base - 80, base - 120
	})
	btcUpSnap := makeSnap("BTCUSDT", now, btcTrendUp, deltas, 67000.0)
	btcDownSnap := makeSnap("BTCUSDT", now, btcTrendDown, deltas, 63000.0)
	altSnap := makeSnap("SOLUSDT", now, candles, deltas, 100.0)

	baseCfg := makeMultiRegimeBaseCfg(now)
	baseCfg.AllowedSide = "SHORT_ONLY"
	baseCfg.BTCGateEnabled = false
	baseCfg.MinImpulseATR = 0.50
	baseCfg.MaxEntryDistanceATR = 4.00
	baseCfg.BreakoutPriorityFirst = true
	baseCfg.VolBreakout = VolBreakoutConfig{
		Enabled: true, LookbackBars: 12, MaxCompressionRatio: 0.90,
		MinRVOL: 1.40, MinBodyRatio: 0.45, MinDeltaZ: 0.50,
	}
	baseCfg.MacroRouter = MacroRegimeRouterConfig{
		Enabled: true, TrendERThreshold: 0.30, VolShockRatio: 1.50, BreakoutOnlyInTrend: true,
	}
	baseCfg.Sizing.RiskPerTradePct = decimal.NewFromFloat(0.02)
	baseCfg.Sizing.SLMultiplier = 2.0
	baseCfg.Sizing.TPMultiplier = 5.0
	baseCfg.Sizing.MaxSlotMarginFraction = decimal.NewFromFloat(0.50)
	baseCfg.Sizing.AvailableBalanceBufferFraction = decimal.NewFromFloat(0.90)
	baseCfg.Sizing.AccountLeverage = 8.0

	// 1. Default AlignMacroTrend=false allows SHORT breakout in TREND_UP; AlignMacroTrend=true blocks it
	decUpOff := EvaluatePortfolio(altSnap, &btcUpSnap, 0, false, false, eq, eq, DefaultSymbolScope(), baseCfg)
	if !decUpOff.Approved || decUpOff.StrategyID != StrategyIDVolBreakout {
		t.Fatalf("expected SHORT breakout in TREND_UP when AlignMacroTrend=false, got %+v", decUpOff)
	}
	cfgAlign := baseCfg
	cfgAlign.VolBreakout.AlignMacroTrend = true
	cfgAlign.DisableVWAP = true
	decAlignUp := EvaluatePortfolio(altSnap, &btcUpSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgAlign)
	if decAlignUp.Approved || decAlignUp.Reason != "BREAKOUT_MACRO_DIRECTION_MISMATCH" {
		t.Fatalf("expected BREAKOUT_MACRO_DIRECTION_MISMATCH in TREND_UP, got %+v", decAlignUp)
	}
	decAlignDown := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgAlign)
	if !decAlignDown.Approved || decAlignDown.StrategyID != StrategyIDVolBreakout {
		t.Fatalf("expected SHORT breakout in TREND_DOWN when AlignMacroTrend=true, got %+v", decAlignDown)
	}

	// 2. MaxNATRPct & MaxEMA200DistATR & MaxVWAPDistATR gates on VolBreakout
	cfgNATR := cfgAlign
	cfgNATR.VolBreakout.MaxNATRPct = 0.05
	if d := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgNATR); d.Approved || d.Reason != "BREAKOUT_NATR_TOO_HIGH" {
		t.Fatalf("expected BREAKOUT_NATR_TOO_HIGH, got %+v", d)
	}
	cfgDist := cfgAlign
	cfgDist.VolBreakout.MaxVWAPDistATR = 0.10
	if d := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgDist); d.Approved || d.Reason != "BREAKOUT_TOO_FAR_FROM_VWAP" {
		t.Fatalf("expected BREAKOUT_TOO_FAR_FROM_VWAP, got %+v", d)
	}

	// 3. RiskMult and EntryMarket on VolBreakout
	cfgHalf := cfgAlign
	cfgHalf.VolBreakout.RiskMult = 0.50
	cfgHalf.VolBreakout.EntryMarket = true
	decHalf := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgHalf)
	if !decHalf.Approved || decHalf.Intent.EntryOrderType != "MARKET" {
		t.Fatalf("expected EntryOrderType=MARKET, got %+v", decHalf)
	}
	ratio := decHalf.Intent.Qty.InexactFloat64() / decAlignDown.Intent.Qty.InexactFloat64()
	if ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("expected RiskMult=0.50 to halve qty, got ratio=%.3f", ratio)
	}

	// 4. Symmetric VWAP SHORT filters: MaxAdverseRetestATRShort & MinCandleBodyRatioShort
	cfgVWAP := baseCfg
	cfgVWAP.VolBreakout.Enabled = false
	cfgVWAP.MacroRouter.Enabled = false
	decVWAPOK := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgVWAP)
	if !decVWAPOK.Approved || decVWAPOK.StrategyID != StrategyIDVWAPPullback {
		t.Fatalf("expected VWAP SHORT approved baseline, got %+v", decVWAPOK)
	}
	cfgTightRetest := cfgVWAP
	cfgTightRetest.MaxAdverseRetestATRShort = 0.10 // n-3 High 100.20 vs VWAP 100.0 exceeds 0.10 ATR
	if d := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgTightRetest); d.Approved {
		t.Fatalf("expected MaxAdverseRetestATRShort=0.10 to block over-pierced retest, got %+v", d)
	}
	cfgTightBody := cfgVWAP
	cfgTightBody.MinCandleBodyRatioShort = 0.99
	if d := EvaluatePortfolio(altSnap, &btcDownSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgTightBody); d.Approved {
		t.Fatalf("expected MinCandleBodyRatioShort=0.99 to block weak body, got %+v", d)
	}

	// 5. Split-NATR SymbolScope isolation: low-NATR coin (0.68% < MinNATRPct 0.95%) allowed in VolBreakout when Breakout.MinNATRPct=0.60, blocked in VWAP
	cfgSplit := baseCfg
	cfgSplit.MinNATRPct = 0.95
	cfgSplit.MaxNATRPct = 1.75
	cfgSplit.VolBreakout.MinNATRPct = 0.60
	// Build calmer candles (~0.68% NATR, below 0.95% VWAP floor, above 0.60% BO floor)
	sqCandles := make([]model.Candle, n)
	copy(sqCandles, candles)
	for i := n - 30; i < n-1; i++ {
		sqCandles[i].High = 100.32
		sqCandles[i].Low = 99.68
	}
	sqCandles[n-4].Low = 99.20
	sqCandles[n-3].High = 100.10
	sqCandles[n-1] = model.Candle{
		OpenTime: now.Add(-5 * time.Minute), CloseTime: now,
		Open: 99.85, High: 99.88, Low: 98.80, Close: 98.85,
		VolumeBase: 500, QuoteVolumeUSDT: 500 * 98.85,
	}
	sqSnap := makeSnap("SQUEEZEUSDT", now, sqCandles, deltas, 100.0)
	// Breakout scope explicit (AllowPrimary=true, AllowBreakout=true) -> breakout approved; if AllowBreakout=false -> rejected; VWAP alone -> blocked by VWAP NATR floor
	decSqBO := EvaluatePortfolio(sqSnap, &btcDownSnap, 0, false, false, eq, eq, SymbolScope{AllowPrimary: true, AllowBreakout: true, BreakoutScopeExplicit: true}, cfgSplit)
	if !decSqBO.Approved || decSqBO.StrategyID != StrategyIDVolBreakout {
		t.Fatalf("expected SQUEEZEUSDT to fire VOL_BREAKOUT under Split-NATR, got %+v", decSqBO)
	}
	cfgSplitVWAPOnly := cfgSplit
	cfgSplitVWAPOnly.VolBreakout.Enabled = false
	decSqVWAP := EvaluatePortfolio(sqSnap, &btcDownSnap, 0, false, false, eq, eq, SymbolScope{AllowPrimary: true, AllowBreakout: false, BreakoutScopeExplicit: true}, cfgSplitVWAPOnly)
	if decSqVWAP.Approved || decSqVWAP.Reason != "NATR_OUT_OF_BOUNDS" {
		t.Fatalf("expected SQUEEZEUSDT below VWAP MinNATRPct=0.95%% to be rejected for VWAP, got %+v", decSqVWAP)
	}
}

func TestOrthogonalAlpha3_TriggersRejectsAndRiskMult(t *testing.T) {
	now := time.Date(2026, 10, 10, 16, 0, 0, 0, time.UTC)
	n := 250
	deltas := make([]float64, n)
	for i := 0; i < n-1; i++ {
		if i%2 == 0 {
			deltas[i] = 100
		} else {
			deltas[i] = -100
		}
	}
	deltas[n-1] = -900 // sharp sell aggression

	makeBase := func(trendSlope float64) []model.Candle {
		cs := make([]model.Candle, n)
		for i := 0; i < n; i++ {
			c := 110.0 + trendSlope*float64(i)
			cs[i] = model.Candle{
				OpenTime:          now.Add(-time.Duration(n-i) * 15 * time.Minute),
				CloseTime:         now.Add(-time.Duration(n-1-i) * 15 * time.Minute),
				Open:              c + 0.10,
				High:              c + 0.35,
				Low:               c - 0.35,
				Close:             c,
				VolumeBase:        100,
				QuoteVolumeUSDT:   100 * c,
				TakerBuyQuoteUSDT: 45 * c,
			}
		}
		return cs
	}

	btcDown := makeSnap("BTCUSDT", now, makeBase(-0.04), deltas, 103.0)
	eq := decimal.NewFromFloat(125.0)

	baseCfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxDailyDrawdownPct:   -0.05,
		MaxBookStaleness:      time.Minute,
		MaxMarkStaleness:      time.Minute,
		MaxDataStaleness:      time.Minute,
		EMAPeriod:             200,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MaxSpreadPct:          0.20,
		MinNATRPct:            0.40,
		MaxNATRPct:            3.00,
		DisableOIFilter:       true,
		MaxAdverseFundingRate: 0.0005,
		AllowedSide:           "SHORT_ONLY",
		DisableVWAP:           true,
		Sizing: SizingConfig{
			RiskPerTradePct:                decimal.NewFromFloat(0.02),
			MaxSlotMarginFraction:          decimal.NewFromFloat(0.30),
			AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
			AccountLeverage:                8.0,
			SLMultiplier:                   2.0,
			TPMultiplier:                   5.0,
			MinSLDistancePct:               decimal.NewFromFloat(0.005),
			MakerFeeRate:                   decimal.NewFromFloat(0.0002),
			TakerFeeRate:                   decimal.NewFromFloat(0.0005),
			SlippageRate:                   decimal.NewFromFloat(0.0002),
			EntryOrderType:                 "LIMIT",
		},
	}

	// 1. EMAPullbackAlpha: stacked EMA20 < EMA50 < EMA200, retest EMA20, rejection bar + RiskMult=0.50
	emaCandles := makeBase(-0.05)
	ema20 := indicator.CalculateEMA(emaCandles[:n-1], 20)
	emaCandles[n-2].High = ema20 + 0.15
	emaCandles[n-2].Close = ema20 - 0.05
	emaCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: ema20 - 0.02, High: ema20 + 0.05, Low: ema20 - 0.85, Close: ema20 - 0.78,
		VolumeBase: 250, QuoteVolumeUSDT: 250 * (ema20 - 0.78),
	}
	emaSnap := makeSnap("EMAPULLUSDT", now, emaCandles, deltas, 105.0)
	cfgEMA := baseCfg
	cfgEMA.EMAPullback = EMAPullbackConfig{Enabled: true, FastPeriod: 20, SlowPeriod: 50, RetestLookback: 3, TouchTolATR: 0.30, MinBodyRatio: 0.35, MinDeltaZ: 0.65, MaxEMA200DistATR: 10.0, RiskMult: 1.0}
	decEMA := EvaluatePortfolio(emaSnap, &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgEMA)
	if !decEMA.Approved || decEMA.StrategyID != StrategyIDEMAPullback {
		t.Fatalf("expected EMA_PULLBACK approved, got %+v", decEMA)
	}
	cfgEMAHalf := cfgEMA
	cfgEMAHalf.EMAPullback.RiskMult = 0.50
	decEMAHalf := EvaluatePortfolio(emaSnap, &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgEMAHalf)
	if !decEMAHalf.Approved || decEMAHalf.Intent.Qty.GreaterThanOrEqual(decEMA.Intent.Qty) {
		t.Fatalf("expected RiskMult=0.50 to scale EMA_PULLBACK qty down, got full=%s half=%s", decEMA.Intent.Qty, decEMAHalf.Intent.Qty)
	}
	deltasPos := append([]float64(nil), deltas...)
	deltasPos[n-1] = 100
	emaBadSnap := makeSnap("EMAPULLUSDT", now, emaCandles, deltasPos, 105.0)
	if d := EvaluatePortfolio(emaBadSnap, &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgEMA); d.Approved {
		t.Fatalf("expected positive delta bar to reject EMA_PULLBACK, got %+v", d)
	}

	// 2. VWAPBreakdownAlpha: multi-bar hold above VWAP=99.0 then crack below VWAP
	brkCandles := makeBase(-0.04)
	vwapLvl := 99.0
	for i := n - 6; i < n-1; i++ {
		brkCandles[i].Open = vwapLvl + 0.20
		brkCandles[i].High = vwapLvl + 0.55
		brkCandles[i].Low = vwapLvl - 0.05
		brkCandles[i].Close = vwapLvl + 0.30
	}
	brkCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: vwapLvl + 0.05, High: vwapLvl + 0.10, Low: vwapLvl - 0.75, Close: vwapLvl - 0.65,
		VolumeBase: 300, QuoteVolumeUSDT: 300 * (vwapLvl - 0.65),
	}
	brkSnap := makeSnap("VWAPBRKUSDT", now, brkCandles, deltas, vwapLvl)
	cfgBrk := baseCfg
	cfgBrk.VWAPBreakdown = VWAPBreakdownConfig{Enabled: true, LookbackBars: 6, MinAboveBars: 2, MinRVOL: 1.15, MinBodyRatio: 0.45, MinDeltaZ: 0.65, MaxBelowVWAPATR: 1.50, RiskMult: 1.0}
	decBrk := EvaluatePortfolio(brkSnap, &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgBrk)
	if !decBrk.Approved || decBrk.StrategyID != StrategyIDVWAPBreakdown {
		t.Fatalf("expected VWAP_BREAKDOWN approved, got %+v", decBrk)
	}
	if d := EvaluatePortfolio(makeSnap("VWAPBRKUSDT", now, brkCandles, deltasPos, vwapLvl), &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgBrk); d.Approved {
		t.Fatalf("expected positive delta to reject VWAP_BREAKDOWN, got %+v", d)
	}

	// 3. StructureBreakAlpha: confirmed 5-bar pivot low at n-8, relief bounce, then crack below pivot low
	strCandles := makeBase(-0.04)
	pIdx := n - 8
	pivotLow := 97.50
	for i := pIdx - 2; i <= pIdx+2; i++ {
		strCandles[i].High = 98.40
		strCandles[i].Low = 97.90
		strCandles[i].Close = 98.10
	}
	strCandles[pIdx].Low = pivotLow
	strCandles[pIdx].Close = 97.75
	for i := pIdx + 1; i < n-1; i++ {
		strCandles[i].High = 99.10 // >= 1.60 above pivotLow (bounce >= 0.80 ATR)
		strCandles[i].Low = 97.70
		strCandles[i].Close = 98.00
	}
	strCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: 97.60, High: 97.65, Low: 96.70, Close: 96.80,
		VolumeBase: 320, QuoteVolumeUSDT: 320 * 96.80,
	}
	strSnap := makeSnap("STRUCTUSDT", now, strCandles, deltas, 99.5)
	cfgStr := baseCfg
	cfgStr.StructureBreak = StructureBreakConfig{Enabled: true, LookbackBars: 20, MinBounceATR: 0.80, MinRVOL: 1.15, MinBodyRatio: 0.45, MinDeltaZ: 0.60, MaxEMA200DistATR: 10.0, RiskMult: 1.0}
	decStr := EvaluatePortfolio(strSnap, &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgStr)
	if !decStr.Approved || decStr.StrategyID != StrategyIDStructureBreak {
		t.Fatalf("expected STRUCTURE_BREAK approved, got %+v", decStr)
	}
	if d := EvaluatePortfolio(makeSnap("STRUCTUSDT", now, strCandles, deltasPos, 99.5), &btcDown, 0, false, false, eq, eq, DefaultSymbolScope(), cfgStr); d.Approved {
		t.Fatalf("expected positive delta to reject STRUCTURE_BREAK, got %+v", d)
	}
}

func TestOrthogonalAlpha3_EMAPullback_VWAPBreakdown_StructureBreak(t *testing.T) {
	eq := decimal.NewFromFloat(1000.0)
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	n := 250
	deltas := make([]float64, n)
	for i := range deltas {
		deltas[i] = 10.0
		if i%2 == 0 {
			deltas[i] = -10.0
		}
	}
	deltas[n-1] = -250.0

	btcCandles := make([]model.Candle, n)
	for i := 0; i < n; i++ {
		btcCandles[i] = model.Candle{
			OpenTime:  now.Add(-time.Duration(n-i) * 15 * time.Minute),
			CloseTime: now.Add(-time.Duration(n-1-i) * 15 * time.Minute),
			Open:      100.0, High: 100.5, Low: 99.5, Close: 100.0 - float64(i)*0.02,
			VolumeBase: 1000, QuoteVolumeUSDT: 100000,
		}
	}
	btcSnap := model.MarketSnapshot{Symbol: "BTCUSDT", Now: now, BestBid: 95.0, BestAsk: 95.05, CurrentSessionVWAP: 99.0, ClosedCandles: btcCandles}

	makeDowntrendCandles := func() []model.Candle {
		cs := make([]model.Candle, n)
		for i := 0; i < n; i++ {
			base := 110.0 - float64(i)*0.08
			cs[i] = model.Candle{
				OpenTime:  now.Add(-time.Duration(n-i) * 15 * time.Minute),
				CloseTime: now.Add(-time.Duration(n-1-i) * 15 * time.Minute),
				Open:      base + 0.3, High: base + 0.7, Low: base - 0.7, Close: base,
				VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
			}
		}
		return cs
	}

	baseCfg := func() EvaluatorConfig {
		return EvaluatorConfig{
			EMAPeriod:             200,
			ATRPeriod:             14,
			DeltaZScoreWindow:     50,
			MaxSlots:              5,
			MaxBookStaleness:      time.Minute,
			MaxMarkStaleness:      time.Minute,
			MaxDataStaleness:      time.Minute,
			MaxSpreadPct:          0.20,
			MinNATRPct:            0.10,
			MaxNATRPct:            10.0,
			DisableOIFilter:       true,
			AllowedSide:           "SHORT_ONLY",
			MaxAdverseFundingRate: 0.001,
			DisableVWAP:           true,
			Sizing: SizingConfig{
				RiskPerTradePct:       decimal.NewFromFloat(0.02),
				MaxSlotMarginFraction: decimal.NewFromFloat(0.25),
				AccountLeverage:       8.0,
				SLMultiplier:          2.0,
				TPMultiplier:          5.0,
				MinSLDistancePct:      decimal.NewFromFloat(0.005),
				MakerFeeRate:          decimal.NewFromFloat(0.0002),
				TakerFeeRate:          decimal.NewFromFloat(0.0005),
				SlippageRate:          decimal.NewFromFloat(0.0002),
			},
		}
	}
	makeSnap := func(sym string, cs []model.Candle, vwap float64) model.MarketSnapshot {
		last := cs[len(cs)-1].Close
		return model.MarketSnapshot{
			Symbol: sym, Now: now, BookUpdatedAt: now, MarkPriceUpdatedAt: now, FundingUpdatedAt: now,
			BestBid: last - 0.01, BestAsk: last + 0.01, MarkPrice: last, FundingRate: 0.0001,
			CurrentSessionVWAP: vwap, ClosedCandles: cs, ClosedBarDeltasUSDT: deltas,
			Filter: model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.001), StepSize: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5)},
		}
	}

	// 1. EMAPullbackAlpha: positive trigger + RiskMult + negative rejection when close > EMA20
	epCandles := makeDowntrendCandles()
	// n-2 bounced up toward EMA20 (~91.2), n-1 rejects cleanly below EMA20 (~91.0)
	epCandles[n-2].High = 91.9
	epCandles[n-2].Close = 91.2
	epCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: 91.3, High: 91.4, Low: 90.0, Close: 90.1,
		VolumeBase: 200, QuoteVolumeUSDT: 200 * 90.1,
	}
	epSnap := makeSnap("EMAPULLUSDT", epCandles, 93.0)
	cfgEP := baseCfg()
	cfgEP.EMAPullback = EMAPullbackConfig{Enabled: true, FastPeriod: 20, SlowPeriod: 50, RetestLookback: 3, TouchTolATR: 0.40, MinBodyRatio: 0.35, MinDeltaZ: 0.50, MaxEMA200DistATR: 15.0, RiskMult: 1.0}
	decEP := EvaluatePortfolio(epSnap, &btcSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgEP)
	if !decEP.Approved || decEP.StrategyID != StrategyIDEMAPullback || decEP.Intent.Side != model.SideShort {
		t.Fatalf("expected EMA_PULLBACK SHORT approved, got %+v", decEP)
	}
	cfgEPHalf := cfgEP
	cfgEPHalf.EMAPullback.RiskMult = 0.50
	decEPHalf := EvaluatePortfolio(epSnap, &btcSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgEPHalf)
	if ratio := decEPHalf.Intent.Qty.InexactFloat64() / decEP.Intent.Qty.InexactFloat64(); ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("expected EMA_PULLBACK RiskMult=0.50 to halve qty, got %.3f", ratio)
	}
	epRejectCandles := makeDowntrendCandles()
	epRejectCandles[n-1].Open = 90.0
	epRejectCandles[n-1].Close = 90.5 // green candle rejects
	if d := EvaluatePortfolio(makeSnap("EMAPULLUSDT", epRejectCandles, 93.0), &btcSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgEP); d.Approved {
		t.Fatalf("expected green candle to reject EMA_PULLBACK, got %+v", d)
	}

	// 2. VWAPBreakdownAlpha: relief bars above VWAP (91.0) then bearish crack below VWAP
	vbCandles := makeDowntrendCandles()
	for i := n - 5; i < n-1; i++ {
		vbCandles[i].Open = 91.2
		vbCandles[i].High = 91.6
		vbCandles[i].Low = 91.0
		vbCandles[i].Close = 91.4
	}
	vbCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: 91.3, High: 91.4, Low: 90.1, Close: 90.2,
		VolumeBase: 300, QuoteVolumeUSDT: 300 * 90.2,
	}
	vbSnap := makeSnap("VWAPBRKUSDT", vbCandles, 91.0)
	cfgVB := baseCfg()
	cfgVB.VWAPBreakdown = VWAPBreakdownConfig{Enabled: true, LookbackBars: 6, MinAboveBars: 2, MinRVOL: 1.15, MinBodyRatio: 0.45, MinDeltaZ: 0.50, MaxBelowVWAPATR: 2.0, RiskMult: 1.0}
	decVB := EvaluatePortfolio(vbSnap, &btcSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgVB)
	if !decVB.Approved || decVB.StrategyID != StrategyIDVWAPBreakdown || decVB.Intent.Side != model.SideShort {
		t.Fatalf("expected VWAP_BREAKDOWN SHORT approved, got %+v", decVB)
	}
	vbFailCandles := makeDowntrendCandles() // never above VWAP=95.0
	if d := EvaluatePortfolio(makeSnap("VWAPBRKUSDT", vbFailCandles, 95.0), &btcSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgVB); d.Approved {
		t.Fatalf("expected VWAP_BREAKDOWN rejection without prior above-VWAP bars, got %+v", d)
	}

	// 3. StructureBreakAlpha: 5-bar pivot low at n-7 with relief bounce >= 0.8 ATR, then fresh crack below pivot low
	sbCandles := makeDowntrendCandles()
	for i := n - 12; i < n-1; i++ {
		sbCandles[i].Open = 92.0
		sbCandles[i].High = 92.2
		sbCandles[i].Low = 91.8 - float64(i-(n-12))*0.05
		sbCandles[i].Close = 91.9
	}
	// Unique 5-bar pivot low at n-7 = 90.0
	sbCandles[n-9].Low = 91.2
	sbCandles[n-8].Low = 90.8
	sbCandles[n-7].Low = 90.0
	sbCandles[n-7].Close = 90.4
	sbCandles[n-6].Low = 90.9
	sbCandles[n-5].Low = 91.5
	// Bounce high at n-4 = 93.0 (below EMA200, bounce = 3.0 >= 0.8*ATR)
	sbCandles[n-4].High = 93.0
	sbCandles[n-4].Low = 91.8
	sbCandles[n-3].Low = 91.0
	sbCandles[n-2].Low = 90.2
	sbCandles[n-2].Close = 90.3 // still >= 90.0 before trigger
	sbCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: 90.3, High: 90.4, Low: 89.0, Close: 89.1,
		VolumeBase: 300, QuoteVolumeUSDT: 300 * 89.1,
	}
	sbSnap := makeSnap("STRUCTUSDT", sbCandles, 91.0)
	cfgSB := baseCfg()
	cfgSB.StructureBreak = StructureBreakConfig{Enabled: true, LookbackBars: 20, MinBounceATR: 0.80, MinRVOL: 1.15, MinBodyRatio: 0.45, MinDeltaZ: 0.50, MaxEMA200DistATR: 15.0, RiskMult: 1.0}
	decSB := EvaluatePortfolio(sbSnap, &btcSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgSB)
	if !decSB.Approved || decSB.StrategyID != StrategyIDStructureBreak || decSB.Intent.Side != model.SideShort {
		t.Fatalf("expected STRUCTURE_BREAK SHORT approved, got %+v", decSB)
	}
}

func TestQMaxPortfolioFactors_VolExp_RS4h_DDTier2_RiskCap(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	eq := decimal.NewFromFloat(1000.0)
	n := 250
	deltas := make([]float64, n)
	for i := range deltas {
		if i%2 == 0 {
			deltas[i] = -10.0
		} else {
			deltas[i] = 10.0
		}
	}
	deltas[n-1] = -250.0

	// Build BTC 1h candles (60h): calm for 46h (range +-100), then high-vol expansion for last 14h (range +-320) -> VolRatio >= 1.20
	// Keep 4h-ago BTC close vs latest close configurable to drive exact RS4h test cases while keeping altSnap constant.
	makeBTCSnap := func(btcCloseAt59, btcCloseAt43 float64) model.MarketSnapshot {
		cs := buildHourlyBTCCandles(now, 60, func(i int) (float64, float64, float64, float64) {
			base := 65000.0
			rng := 100.0
			if i >= 46 {
				rng = 320.0
			}
			return base, base + rng, base - rng, base
		})
		cs[len(cs)-1-16].Close = btcCloseAt43
		cs[len(cs)-1].Close = btcCloseAt59
		return makeSnap("BTCUSDT", now, cs, deltas, btcCloseAt59)
	}
	// Constant altcoin downtrend with VWAPBreakdown setup (exact proven pattern from TestOrthogonalAlpha3)
	altCandles := make([]model.Candle, n)
	for i := 0; i < n; i++ {
		base := 110.0 - float64(i)*0.08
		altCandles[i] = model.Candle{
			OpenTime:  now.Add(-time.Duration(n-i) * 15 * time.Minute),
			CloseTime: now.Add(-time.Duration(n-1-i) * 15 * time.Minute),
			Open:      base + 0.3, High: base + 0.7, Low: base - 0.7, Close: base,
			VolumeBase: 100, QuoteVolumeUSDT: 100 * base,
		}
	}
	for i := n - 5; i < n-1; i++ {
		altCandles[i].Open = 91.2
		altCandles[i].High = 91.6
		altCandles[i].Low = 91.0
		altCandles[i].Close = 91.4
	}
	altCandles[n-1] = model.Candle{
		OpenTime: now.Add(-15 * time.Minute), CloseTime: now,
		Open: 91.3, High: 91.4, Low: 90.1, Close: 90.2,
		VolumeBase: 300, QuoteVolumeUSDT: 300 * 90.2,
	}
	altSnap := makeSnap("ALTUSDT", now, altCandles, deltas, 91.0)
	altSnap.Filter = model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.001),
		StepSize:    decimal.NewFromFloat(0.01),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	// Alt 4h return: altCandles[n-17].Close = 110 - 233*0.08 = 91.36 -> altCandles[n-1].Close = 90.2 (-1.27%)
	// Flat BTC (65000 -> 65000): RS4h = -1.27% (within [-2%, +2%] -> neutral)
	// BTC +3% (65000 -> 66950): RS4h = -1.27% - 3.0% = -4.27% (<= -2% -> Weak Laggard)
	// BTC -4% (65000 -> 62400): RS4h = -1.27% - (-4.0%) = +2.73% (>= +2% -> Strong Outperformer)
	btcFlatSnap := makeBTCSnap(65000.0, 65000.0)
	btcUp4hSnap := makeBTCSnap(66950.0, 65000.0)
	btcDn4hSnap := makeBTCSnap(62400.0, 65000.0)

	cfg := makeMultiRegimeBaseCfg(now)
	cfg.CandleDuration = 15 * time.Minute
	cfg.AllowedSide = "SHORT_ONLY"
	cfg.BTCGateEnabled = false
	cfg.DisableVWAP = true
	cfg.VWAPBreakdown = VWAPBreakdownConfig{
		Enabled: true, LookbackBars: 6, MinAboveBars: 2,
		MinRVOL: 1.15, MinBodyRatio: 0.45, MinDeltaZ: 0.50, MaxBelowVWAPATR: 2.0, RiskMult: 1.0,
	}
	cfg.Sizing.RiskPerTradePct = decimal.NewFromFloat(0.02)
	cfg.Sizing.SLMultiplier = 2.0
	cfg.Sizing.TPMultiplier = 5.0
	cfg.Sizing.MaxSlotMarginFraction = decimal.NewFromFloat(0.80)
	cfg.Sizing.AvailableBalanceBufferFraction = decimal.NewFromFloat(0.95)
	cfg.Sizing.AccountLeverage = 15.0

	decBase := EvaluatePortfolio(altSnap, &btcFlatSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfg)
	if !decBase.Approved {
		t.Fatalf("expected baseline altSnap approved, got %+v", decBase)
	}
	baseQty := decBase.Intent.Qty.InexactFloat64()

	// 1. MacroVolExpansionRiskMult = 1.35 triggers because BTC VolRatio >= 1.20
	cfgVol := cfg
	cfgVol.MacroRouter = MacroRegimeRouterConfig{
		Enabled: true, TrendERThreshold: 0.30, VolShockRatio: 2.50,
		VolExpansionRatio: 1.20, VolExpansionRiskMult: 1.35,
	}
	decVol := EvaluatePortfolio(altSnap, &btcFlatSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgVol)
	if !decVol.Approved {
		t.Fatalf("expected VolExpansion approved, got %+v", decVol)
	}
	if r := decVol.Intent.Qty.InexactFloat64() / baseQty; r < 1.28 || r > 1.42 {
		t.Fatalf("expected VolExpansionRiskMult 1.35 to scale qty ~1.35x, got %.3f", r)
	}

	// 2. RS4h Weak boost (1.25x) vs Strong throttle (0.50x) vs Strong veto (0.0)
	cfgRS := cfg
	cfgRS.RS4hWeakThresholdPct = -2.0
	cfgRS.RS4hWeakRiskMult = 1.25
	cfgRS.RS4hStrongThresholdPct = 2.0
	cfgRS.RS4hStrongRiskMult = 0.50
	decRSWeak := EvaluatePortfolio(altSnap, &btcUp4hSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgRS)
	if !decRSWeak.Approved {
		t.Fatalf("expected RS4h weak approved, got %+v", decRSWeak)
	}
	if r := decRSWeak.Intent.Qty.InexactFloat64() / baseQty; r < 1.18 || r > 1.32 {
		t.Fatalf("expected RS4h weak mult 1.25x, got %.3f", r)
	}

	decRSStrong := EvaluatePortfolio(altSnap, &btcDn4hSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgRS)
	if !decRSStrong.Approved {
		t.Fatalf("expected RS4h strong throttled approved, got %+v", decRSStrong)
	}
	if r := decRSStrong.Intent.Qty.InexactFloat64() / baseQty; r < 0.45 || r > 0.55 {
		t.Fatalf("expected RS4h strong mult 0.50x, got %.3f", r)
	}

	cfgRSVeto := cfgRS
	cfgRSVeto.RS4hStrongRiskMult = 0.0
	if d := EvaluatePortfolio(altSnap, &btcDn4hSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgRSVeto); d.Approved || d.Reason != "RS4H_STRONG_OUTPERFORMER_VETO" {
		t.Fatalf("expected RS4H_STRONG_OUTPERFORMER_VETO, got %+v", d)
	}

	// 3. DDThrottleTier2 overrides Tier1 cleanly without double multiplication
	cfgDD := cfg
	cfgDD.DDThrottleThresholdPct = 0.10
	cfgDD.DDThrottleRiskMult = 0.70
	cfgDD.DDThrottleTier2ThresholdPct = 0.16
	cfgDD.DDThrottleTier2RiskMult = 0.45
	altDD1 := altSnap
	altDD1.PortfolioDrawdownPct = 0.12
	decDD1 := EvaluatePortfolio(altDD1, &btcFlatSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgDD)
	if r := decDD1.Intent.Qty.InexactFloat64() / baseQty; r < 0.65 || r > 0.75 {
		t.Fatalf("expected DD Tier1 0.70x at 12%% DD, got %.3f", r)
	}
	altDD2 := altSnap
	altDD2.PortfolioDrawdownPct = 0.18
	decDD2 := EvaluatePortfolio(altDD2, &btcFlatSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgDD)
	if r := decDD2.Intent.Qty.InexactFloat64() / baseQty; r < 0.41 || r > 0.49 {
		t.Fatalf("expected DD Tier2 0.45x (NOT 0.70*0.45) at 18%% DD, got %.3f", r)
	}

	// 4. MaxEffectiveRiskPerTradePct caps stacked multipliers
	cfgCap := cfgVol
	cfgCap.RS4hWeakThresholdPct = -2.0
	cfgCap.RS4hWeakRiskMult = 1.25 // unstacked 0.02 * 1.35 * 1.25 = 0.03375
	cfgCap.MaxEffectiveRiskPerTradePct = 0.025
	decCap := EvaluatePortfolio(altSnap, &btcUp4hSnap, 0, false, false, eq, eq, DefaultSymbolScope(), cfgCap)
	if r := decCap.Intent.Qty.InexactFloat64() / baseQty; r < 1.18 || r > 1.32 {
		t.Fatalf("expected MaxEffectiveRiskPerTradePct=0.025 to cap at 1.25x base (0.02), got %.3f", r)
	}
}

func TestStrategyHardening_Batch2(t *testing.T) {
	now := time.Date(2026, 6, 15, 0, 10, 0, 0, time.UTC) // Inside midnight-UTC warmup unless set to 0
	eq := decimal.NewFromFloat(1000)

	// 1. ResampleToHourly preserves 1h candles with CloseTime.IsZero() and ClassifyMacroRegime rejects NaN -> UNKNOWN
	hourlySeries := make([]model.Candle, 40)
	for i := 0; i < 40; i++ {
		p := 100.0 + float64(i)*0.5
		hourlySeries[i] = model.Candle{
			OpenTime: now.Add(time.Duration(i-40) * time.Hour),
			Open:     p, High: p + 0.5, Low: p - 0.5, Close: p, VolumeBase: 100,
		}
	}
	completed := CompletedHourlyCandles(hourlySeries)
	if len(completed) != 40 {
		t.Fatalf("expected all 40 1h candles preserved when CloseTime is zero, got %d", len(completed))
	}
	nanSeries := append([]model.Candle(nil), hourlySeries...)
	nanSeries[len(nanSeries)-1].High = math.NaN()
	nanSeries[len(nanSeries)-1].Low = math.Inf(-1)
	if resampled := ResampleToHourly(nanSeries); len(resampled) != 39 {
		t.Fatalf("expected non-finite 1h candle skipped cleanly leaving 39 bars, got %d", len(resampled))
	}
	overflowSeries := append([]model.Candle(nil), hourlySeries...)
	overflowSeries[15].Close = 0.01
	overflowSeries[15].Low = 0.01
	overflowSeries[len(overflowSeries)-1].Close = 1e308
	overflowSeries[len(overflowSeries)-1].High = 1e308
	if mInf := ClassifyMacroRegime(overflowSeries, 0.30, 1.50); mInf.Regime != MacroRegimeUnknown {
		t.Fatalf("expected +Inf overflow metric to fail closed to MacroRegimeUnknown, got %+v", mInf)
	}

	// 2. BuildSafeDecimalOrderIntent NaN/Inf safety + DisablePartialTP + MarketMaxQty/MarketStepSize
	downCandles, downDeltas := buildDowntrendBaseCandles(now, 240, 120.0, 0.20)
	snap := makeSnap("TESTUSDT", now, downCandles, downDeltas, 100.0)
	snap.Filter = model.ExchangeFilter{
		TickSize:       decimal.NewFromFloat(0.01),
		StepSize:       decimal.NewFromFloat(0.1),
		MinQty:         decimal.NewFromFloat(0.1),
		MinNotional:    decimal.NewFromFloat(5.0),
		MarketStepSize: decimal.NewFromFloat(1.0),
		MarketMaxQty:   decimal.NewFromFloat(5.0),
	}
	sizing := SizingConfig{
		RiskPerTradePct:       decimal.NewFromFloat(0.02),
		MaxSlotMarginFraction: decimal.NewFromFloat(0.50),
		AccountLeverage:       10.0,
		SLMultiplier:          1.5,
		TPMultiplier:          1.0, // <= TP1Multiplier (1.2), would fail unless DisablePartialTP=true!
		TP1Multiplier:         1.2,
		MakerFeeRate:          decimal.NewFromFloat(0.0002),
		TakerFeeRate:          decimal.NewFromFloat(0.0005),
		SlippageRate:          decimal.NewFromFloat(0.0002),
		DisablePartialTP:      true,
	}
	if _, ok := BuildSafeDecimalOrderIntent(snap, eq, eq, model.SideShort, math.NaN(), 0.5, sizing); ok {
		t.Fatalf("expected NaN rawEntry rejected without panic")
	}
	if _, ok := BuildSafeDecimalOrderIntent(snap, eq, eq, model.SideShort, 100.0, math.Inf(1), sizing); ok {
		t.Fatalf("expected +Inf ATR rejected without panic")
	}
	intentCap, okCap := BuildSafeDecimalOrderIntent(snap, eq, eq, model.SideShort, 100.0, 0.5, sizing)
	if !okCap || !intentCap.TP1Price.IsZero() || !intentCap.Qty.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected DisablePartialTP ok and qty clamped to MarketMaxQty=5 (step 1.0), got ok=%v qty=%s tp1=%s", okCap, intentCap.Qty, intentCap.TP1Price)
	}

	// 3. RS4h with CandleDuration == 0 + Zero Config Overrides (Warmup=0, ProfitCooldown=0, MinOIDeltaLong=0)
	ctx := sharedMarketContext{
		Latest:         downCandles[len(downCandles)-1],
		EMA200:         115.0,
		ATR14:          0.5,
		NATRPct:        1.5,
		VWAP:           100.0,
		BarDeltaZScore: -1.5,
		DeltaZOK:       true,
		OIDeltaOK:      true,
		OIValidLong:    true,
		OIValidShort:   true,
	}
	cfgRS := makeMultiRegimeBaseCfg(now)
	cfgRS.CandleDuration = 0 // Must NOT divide by zero!
	cfgRS.RS4hWeakThresholdPct = -1.0
	cfgRS.RS4hWeakRiskMult = 1.25
	btcSnap := makeSnap("BTCUSDT", now, downCandles, downDeltas, 100.0)
	if _, okRS, _, _ := resolveEntryAndBuildIntent(ctx, snap, &btcSnap, model.SideShort, StrategyIDVWAPPullback, eq, eq, cfgRS); !okRS {
		t.Fatalf("expected RS4h with CandleDuration=0 not to panic and build intent")
	}

	// 4. HTFEMAMaxRisingSlopePct fails closed with HTF_EMA_WARMING_UP when HTFEMAPeriod <= len(hourly) < HTFEMAPeriod+24
	hourly20 := buildHourlyBTCCandles(now, 20, func(i int) (float64, float64, float64, float64) {
		p := 120.0 - float64(i)*0.5
		return p, p + 0.5, p - 0.5, p - 0.3
	})
	snapSlope := makeSnap("ALTUSDT", now, hourly20, make([]float64, len(hourly20)), 110.0)
	cfgSlope := makeMultiRegimeBaseCfg(now)
	cfgSlope.HTFEMAPeriod = 10
	cfgSlope.HTFEMAMaxRisingSlopePct = 0.25
	if okS, rS, _ := evaluateSideAndPostFiltersWithOpts(sharedMarketContext{Latest: hourly20[len(hourly20)-1], NATRPct: 1.0, ATR14: 0.5}, snapSlope, nil, model.SideShort, cfgSlope, false); okS || rS != "HTF_EMA_WARMING_UP" {
		t.Fatalf("expected HTF_EMA_WARMING_UP during 24h slope warmup (20 < 10+24), got ok=%v reason=%s", okS, rS)
	}

	// 5. BTC Tier-2 LONG persistence respects btcVWAP even when BTCTier2LongSymmetric=false
	btcPersist := make([]model.Candle, 60)
	for i := range btcPersist {
		btcPersist[i] = model.Candle{OpenTime: now.Add(time.Duration(i-60) * 5 * time.Minute), CloseTime: now.Add(time.Duration(i-59) * 5 * time.Minute), Open: 60000, High: 60050, Low: 59950, Close: 60000}
	}
	btcPersist[57].Close = 60200 // >= EMA (~60000) but < VWAP (61000)!
	btcPersist[58].Close = 61200
	btcPersist[59].Close = 61200
	btcPersistSnap := model.MarketSnapshot{Symbol: "BTCUSDT", Now: now, CurrentSessionVWAP: 61000, ClosedCandles: btcPersist}
	cfgBTC := makeMultiRegimeBaseCfg(now)
	cfgBTC.AllowedSide = "AUTO"
	cfgBTC.EMAPeriod = 20
	cfgBTC.BTCGateEnabled = true
	cfgBTC.BTCGateShortOnly = false
	cfgBTC.BTCTier2LongSymmetric = false
	cfgBTC.BTCGatePersistenceBars = 3
	cfgBTC.BTCMax5mDumpPct = 5.0
	cfgBTC.BTCCounterMomentumPct = 5.0
	if okL, rL, _ := evaluateSideAndPostFiltersWithOpts(ctx, snap, &btcPersistSnap, model.SideLong, cfgBTC, false); okL || rL != "BTC_TREND_LONG_VETO_PERSISTENCE" {
		t.Fatalf("expected BTC_TREND_LONG_VETO_PERSISTENCE when bar 2 ago closed below btcVWAP, got ok=%v reason=%s", okL, rL)
	}

	// 6. VolBreakout.MaxNATRPct bypasses global MaxNATRPct ONLY for VolBreakout, still blocks VWAP/Sweep/Rel
	cfgBO := makeMultiRegimeBaseCfg(now)
	cfgBO.MinNATRPct = 0.50
	cfgBO.MaxNATRPct = 1.20
	cfgBO.AllowedSide = "BOTH"
	cfgBO.VWAPSessionWarmupMinutes = 0
	cfgBO.VWAPSessionWarmupSet = true
	cfgBO.CooldownProfitMinutes = 0
	cfgBO.CooldownProfitSet = true
	cfgBO.VolBreakout.Enabled = true
	cfgBO.VolBreakout.MaxNATRPct = 3.00
	cfgBO.Sweep.Enabled = true
	cfgBO.RelResidual.Enabled = true
	if rej, ok := checkPrimaryNATRBounds(sharedMarketContext{NATRPct: 2.0}, cfgBO, "SWEEP"); ok || rej.Reason != "NATR_OUT_OF_BOUNDS" {
		t.Fatalf("expected SWEEP/primary NATR cap 1.20%% to reject 2.0%% even when VolBreakout.MaxNATRPct=3.0%%, got ok=%v rej=%+v", ok, rej)
	}

	// 7. RelResidual respects scope.AllowPrimary=false
	cfgRelScope := makeMultiRegimeBaseCfg(now)
	cfgRelScope.DisableVWAP = true
	cfgRelScope.RelResidual = RelResidualConfig{Enabled: true, Mode: "MEANREV", LookbackBars: 12, MinRelZ: 0.5, MinDeltaZ: 0.1}
	highVolSnap := snap
	highVolSnap.LastExitTime = now.Add(-1 * time.Minute)
	highVolSnap.LastExitReason = "TAKE_PROFIT" // Should pass with CooldownProfitSet=true & 0 mins!
	cfgRelScope.VWAPSessionWarmupMinutes = 0
	cfgRelScope.VWAPSessionWarmupSet = true
	cfgRelScope.CooldownProfitMinutes = 0
	cfgRelScope.CooldownProfitSet = true
	cfgRelScope.CooldownAfterExitMinutes = 20
	decScope := EvaluatePortfolio(highVolSnap, &btcSnap, 0, false, false, eq, eq, SymbolScope{AllowPrimary: false, AllowSweep: true}, cfgRelScope)
	if decScope.StrategyID == StrategyIDRelResidual || decScope.Reason == "SYMBOL_IN_COOLDOWN" || decScope.Reason == "VWAP_SESSION_WARMUP" {
		t.Fatalf("expected AllowPrimary=false to block RelResidual and 0-overrides to clear cooldown/warmup, got %+v", decScope)
	}

	// 8. EvaluateSignal isolates VWAP even if VolBreakout.Enabled=true
	decIso, okIso, rIso, _ := EvaluateSignal(highVolSnap, &btcSnap, 0, false, false, eq, eq, cfgBO)
	_ = decIso
	_ = okIso
	if strings.HasPrefix(rIso, "BREAKOUT_") {
		t.Fatalf("expected EvaluateSignal to isolate VWAP and never leak VolBreakout reasons, got %s", rIso)
	}
}
