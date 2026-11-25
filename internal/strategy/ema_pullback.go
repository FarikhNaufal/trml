package strategy

import (
	"fmt"
	"math"

	"trml/internal/indicator"
	"trml/internal/model"
)

// EMAPullbackAlpha detects trend-continuation pullbacks to a fast EMA inside
// a stacked EMA20/EMA50/EMA200 trend regime, firing on orderflow rejection bars.
type EMAPullbackAlpha struct{}

// ID returns the canonical identifier for the EMA Pullback continuation alpha.
func (EMAPullbackAlpha) ID() string {
	return StrategyIDEMAPullback
}

// Evaluate inspects stacked EMA alignment, recent EMA20 pullback retest, and rejection flow.
func (EMAPullbackAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	fastPeriod := cfg.EMAPullback.FastPeriod
	if fastPeriod <= 0 {
		fastPeriod = 20
	}
	slowPeriod := cfg.EMAPullback.SlowPeriod
	if slowPeriod <= fastPeriod {
		slowPeriod = 50
	}
	retestBars := cfg.EMAPullback.RetestLookback
	if retestBars <= 0 {
		retestBars = 3
	}
	touchTol := cfg.EMAPullback.TouchTolATR
	if !cfg.EMAPullback.TouchTolSet && touchTol <= 0 {
		touchTol = 0.30
	}
	minBody := cfg.EMAPullback.MinBodyRatio
	if !cfg.EMAPullback.MinBodySet && minBody <= 0 {
		minBody = 0.35
	}
	minDeltaZ := cfg.EMAPullback.MinDeltaZ
	if minDeltaZ < 0 {
		minDeltaZ = 0
	}
	maxEMA200ATR := cfg.EMAPullback.MaxEMA200DistATR
	if maxEMA200ATR <= 0 {
		maxEMA200ATR = 5.0
	}

	if rej, ok := checkPrimaryNATRBounds(ctx, cfg, "EMA_PULL"); !ok {
		return rej
	}

	n := len(snap.ClosedCandles)
	minNeeded := slowPeriod + 10
	if minNeeded < 60 {
		minNeeded = 60
	}
	if n < minNeeded {
		return AlphaCandidate{
			Reason:    "EMA_PULL_INSUFFICIENT_HISTORY",
			SubReason: fmt.Sprintf("CANDLES_%d_NEED_%d", n, minNeeded),
		}
	}
	if !ctx.DeltaZOK {
		return AlphaCandidate{Reason: "INVALID_ZSCORE_BASELINE"}
	}
	if !ctx.OIDeltaOK {
		return AlphaCandidate{Reason: "INVALID_OI_DELTA"}
	}

	emaFast := indicator.CalculateEMA(snap.ClosedCandles, fastPeriod)
	emaSlow := indicator.CalculateEMA(snap.ClosedCandles, slowPeriod)
	emaFastPrev := indicator.CalculateEMA(snap.ClosedCandles[:n-5], fastPeriod)
	if math.IsNaN(emaFast) || math.IsInf(emaFast, 0) || emaFast <= 0 ||
		math.IsNaN(emaSlow) || math.IsInf(emaSlow, 0) || emaSlow <= 0 ||
		math.IsNaN(emaFastPrev) || math.IsInf(emaFastPrev, 0) || emaFastPrev <= 0 {
		return AlphaCandidate{Reason: "INVALID_EMA"}
	}

	latest := ctx.Latest
	barRange := latest.High - latest.Low
	if barRange <= 0 {
		return AlphaCandidate{Reason: "EMA_PULL_CONDITIONS_NOT_MET", SubReason: "ZERO_RANGE"}
	}
	upperWick := latest.High - math.Max(latest.Open, latest.Close)
	lowerWick := math.Min(latest.Open, latest.Close) - latest.Low

	winStart := n - retestBars
	if winStart < 0 {
		winStart = 0
	}

	// SHORT: stacked bear trend EMA20 < EMA50 < EMA200 with falling EMA20.
	if emaFast < emaSlow && emaSlow < ctx.EMA200 && emaFast < emaFastPrev &&
		latest.Close < ctx.VWAP && latest.Close < ctx.EMA200 &&
		(ctx.EMA200-latest.Close) <= maxEMA200ATR*ctx.ATR14 {
		touchedFast := false
		brokeAboveSlow := false
		for i := winStart; i < n; i++ {
			c := snap.ClosedCandles[i]
			if c.Close > emaSlow {
				brokeAboveSlow = true
				break
			}
			if c.High >= emaFast-(touchTol*ctx.ATR14) && c.High <= emaSlow+(0.35*ctx.ATR14) {
				touchedFast = true
			}
		}
		bodyRatio := (latest.Open - latest.Close) / barRange
		shortOK := touchedFast && !brokeAboveSlow &&
			latest.Close < latest.Open &&
			latest.Close <= emaFast &&
			bodyRatio >= minBody &&
			lowerWick < 0.45*barRange &&
			ctx.BarDeltaZScore <= -minDeltaZ &&
			ctx.OIValidShort &&
			snap.FundingRate >= -cfg.MaxAdverseFundingRate
		if shortOK {
			return AlphaCandidate{Side: model.SideShort, Approved: true}
		}
	}

	// LONG: stacked bull trend EMA20 > EMA50 > EMA200 with rising EMA20.
	if emaFast > emaSlow && emaSlow > ctx.EMA200 && emaFast > emaFastPrev &&
		latest.Close > ctx.VWAP && latest.Close > ctx.EMA200 &&
		(latest.Close-ctx.EMA200) <= maxEMA200ATR*ctx.ATR14 {
		touchedFast := false
		brokeBelowSlow := false
		for i := winStart; i < n; i++ {
			c := snap.ClosedCandles[i]
			if c.Close < emaSlow {
				brokeBelowSlow = true
				break
			}
			if c.Low <= emaFast+(touchTol*ctx.ATR14) && c.Low >= emaSlow-(0.35*ctx.ATR14) {
				touchedFast = true
			}
		}
		bodyRatio := (latest.Close - latest.Open) / barRange
		longOK := touchedFast && !brokeBelowSlow &&
			latest.Close > latest.Open &&
			latest.Close >= emaFast &&
			bodyRatio >= minBody &&
			upperWick < 0.45*barRange &&
			ctx.BarDeltaZScore >= minDeltaZ &&
			ctx.OIValidLong &&
			snap.FundingRate <= cfg.MaxAdverseFundingRate
		if longOK {
			return AlphaCandidate{Side: model.SideLong, Approved: true}
		}
	}

	return AlphaCandidate{
		Reason:    "EMA_PULL_CONDITIONS_NOT_MET",
		SubReason: fmt.Sprintf("EMA20_%.4f_EMA50_%.4f_DZ_%.2f", emaFast, emaSlow, ctx.BarDeltaZScore),
	}
}
