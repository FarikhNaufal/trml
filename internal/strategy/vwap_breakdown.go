package strategy

import (
	"fmt"
	"math"

	"trml/internal/indicator"
	"trml/internal/model"
)

// VWAPBreakdownAlpha detects failed session relief rallies cracking back through
// Session VWAP inside a macro trend regime, disjoint from VWAP_PULLBACK retests.
type VWAPBreakdownAlpha struct{}

// ID returns the canonical identifier for the VWAP Breakdown alpha.
func (VWAPBreakdownAlpha) ID() string {
	return StrategyIDVWAPBreakdown
}

// Evaluate checks prior multi-bar acceptance on the opposite side of Session VWAP
// followed by a high-conviction directional crack across VWAP with RVOL and orderflow delta.
func (VWAPBreakdownAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	lookback := cfg.VWAPBreakdown.LookbackBars
	if lookback <= 0 {
		lookback = 6
	}
	minOppositeBars := cfg.VWAPBreakdown.MinAboveBars
	if minOppositeBars <= 0 {
		minOppositeBars = 2
	}
	minRVOL := cfg.VWAPBreakdown.MinRVOL
	if minRVOL <= 0 {
		minRVOL = 1.15
	}
	minBody := cfg.VWAPBreakdown.MinBodyRatio
	if !cfg.VWAPBreakdown.MinBodySet && minBody <= 0 {
		minBody = 0.45
	}
	minDeltaZ := cfg.VWAPBreakdown.MinDeltaZ
	if minDeltaZ < 0 {
		minDeltaZ = 0
	}
	maxCrossATR := cfg.VWAPBreakdown.MaxBelowVWAPATR
	if maxCrossATR <= 0 {
		maxCrossATR = 1.50
	}

	if rej, ok := checkPrimaryNATRBounds(ctx, cfg, "VWAP_BRK"); !ok {
		return rej
	}

	n := len(snap.ClosedCandles)
	minNeeded := lookback + 2
	if minNeeded < 22 {
		minNeeded = 22
	}
	if n < minNeeded {
		return AlphaCandidate{
			Reason:    "VWAP_BRK_INSUFFICIENT_HISTORY",
			SubReason: fmt.Sprintf("CANDLES_%d_NEED_%d", n, minNeeded),
		}
	}
	if !ctx.DeltaZOK {
		return AlphaCandidate{Reason: "INVALID_ZSCORE_BASELINE"}
	}
	if !ctx.OIDeltaOK {
		return AlphaCandidate{Reason: "INVALID_OI_DELTA"}
	}

	rvol := indicator.CalculateRVOL(snap.ClosedCandles, 20)
	if math.IsNaN(rvol) || math.IsInf(rvol, 0) || rvol < minRVOL {
		return AlphaCandidate{
			Reason:    "VWAP_BRK_LOW_RVOL",
			SubReason: fmt.Sprintf("RVOL_%.2f_MIN_%.2f", rvol, minRVOL),
		}
	}

	latest := ctx.Latest
	barRange := latest.High - latest.Low
	if barRange <= 0 {
		return AlphaCandidate{Reason: "VWAP_BRK_CONDITIONS_NOT_MET", SubReason: "ZERO_RANGE"}
	}
	upperWick := latest.High - math.Max(latest.Open, latest.Close)
	lowerWick := math.Min(latest.Open, latest.Close) - latest.Low

	startIdx := n - 1 - lookback
	if startIdx < 0 {
		startIdx = 0
	}
	aboveCount := 0
	belowCount := 0
	for i := startIdx; i < n-1; i++ {
		if snap.ClosedCandles[i].Close > ctx.VWAP {
			aboveCount++
		} else if snap.ClosedCandles[i].Close < ctx.VWAP {
			belowCount++
		}
	}
	prev := snap.ClosedCandles[n-2]

	// SHORT: failed relief rally above VWAP cracks back below VWAP while below EMA200.
	shortBody := (latest.Open - latest.Close) / barRange
	shortOK := aboveCount >= minOppositeBars &&
		prev.Close >= ctx.VWAP-(0.10*ctx.ATR14) &&
		prev.High > ctx.VWAP &&
		latest.Close < ctx.VWAP &&
		latest.Close < latest.Open &&
		latest.Close < ctx.EMA200 &&
		(ctx.VWAP-latest.Close) <= maxCrossATR*ctx.ATR14 &&
		shortBody >= minBody &&
		lowerWick < 0.35*barRange &&
		ctx.BarDeltaZScore <= -minDeltaZ &&
		ctx.OIValidShort &&
		snap.FundingRate >= -cfg.MaxAdverseFundingRate
	if shortOK {
		return AlphaCandidate{Side: model.SideShort, Approved: true}
	}

	// LONG: symmetric reclaim above VWAP after holding below VWAP while above EMA200.
	longBody := (latest.Close - latest.Open) / barRange
	longOK := belowCount >= minOppositeBars &&
		prev.Close <= ctx.VWAP+(0.10*ctx.ATR14) &&
		prev.Low < ctx.VWAP &&
		latest.Close > ctx.VWAP &&
		latest.Close > latest.Open &&
		latest.Close > ctx.EMA200 &&
		(latest.Close-ctx.VWAP) <= maxCrossATR*ctx.ATR14 &&
		longBody >= minBody &&
		upperWick < 0.35*barRange &&
		ctx.BarDeltaZScore >= minDeltaZ &&
		ctx.OIValidLong &&
		snap.FundingRate <= cfg.MaxAdverseFundingRate
	if longOK {
		return AlphaCandidate{Side: model.SideLong, Approved: true}
	}

	return AlphaCandidate{
		Reason:    "VWAP_BRK_CONDITIONS_NOT_MET",
		SubReason: fmt.Sprintf("ABV_%d_BEL_%d_RVOL_%.2f_DZ_%.2f", aboveCount, belowCount, rvol, ctx.BarDeltaZScore),
	}
}
