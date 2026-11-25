package strategy

import (
	"fmt"
	"math"

	"trml/internal/indicator"
	"trml/internal/model"
)

// StructureBreakAlpha detects confirmed 5-bar swing pivot market-structure breaks
// after a relief bounce/pullback without requiring volatility compression.
type StructureBreakAlpha struct{}

// ID returns the canonical identifier for the Structure Break alpha.
func (StructureBreakAlpha) ID() string {
	return StrategyIDStructureBreak
}

// Evaluate locates the most recent confirmed 5-bar swing pivot using only prior closed bars
// and triggers when the current closed bar cleanly breaks that structure with RVOL & delta.
func (StructureBreakAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	lookback := cfg.StructureBreak.LookbackBars
	if lookback <= 0 {
		lookback = 20
	}
	minBounceATR := cfg.StructureBreak.MinBounceATR
	if minBounceATR <= 0 {
		minBounceATR = 0.80
	}
	minRVOL := cfg.StructureBreak.MinRVOL
	if minRVOL <= 0 {
		minRVOL = 1.15
	}
	minBody := cfg.StructureBreak.MinBodyRatio
	if !cfg.StructureBreak.MinBodySet && minBody <= 0 {
		minBody = 0.45
	}
	minDeltaZ := cfg.StructureBreak.MinDeltaZ
	if minDeltaZ < 0 {
		minDeltaZ = 0
	}
	maxEMA200ATR := cfg.StructureBreak.MaxEMA200DistATR
	if maxEMA200ATR <= 0 {
		maxEMA200ATR = 4.50
	}

	if rej, ok := checkPrimaryNATRBounds(ctx, cfg, "STRUCT"); !ok {
		return rej
	}

	n := len(snap.ClosedCandles)
	minNeeded := lookback + 5
	if minNeeded < 25 {
		minNeeded = 25
	}
	if n < minNeeded {
		return AlphaCandidate{
			Reason:    "STRUCT_INSUFFICIENT_HISTORY",
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
			Reason:    "STRUCT_LOW_RVOL",
			SubReason: fmt.Sprintf("RVOL_%.2f_MIN_%.2f", rvol, minRVOL),
		}
	}

	latest := ctx.Latest
	barRange := latest.High - latest.Low
	if barRange <= 0 {
		return AlphaCandidate{Reason: "STRUCT_CONDITIONS_NOT_MET", SubReason: "ZERO_RANGE"}
	}
	upperWick := latest.High - math.Max(latest.Open, latest.Close)
	lowerWick := math.Min(latest.Open, latest.Close) - latest.Low

	minPivotIdx := n - 1 - lookback
	if minPivotIdx < 2 {
		minPivotIdx = 2
	}
	prev := snap.ClosedCandles[n-2]

	// SHORT: break below confirmed 5-bar pivot low after lower-high relief bounce.
	if latest.Close < ctx.EMA200 && latest.Close < ctx.VWAP &&
		(ctx.EMA200-latest.Close) <= maxEMA200ATR*ctx.ATR14 {
		pivotLowIdx := -1
		for p := n - 4; p >= minPivotIdx; p-- {
			c := snap.ClosedCandles[p]
			if c.Low <= snap.ClosedCandles[p-2].Low &&
				c.Low <= snap.ClosedCandles[p-1].Low &&
				c.Low <= snap.ClosedCandles[p+1].Low &&
				c.Low <= snap.ClosedCandles[p+2].Low {
				pivotLowIdx = p
				break
			}
		}
		if pivotLowIdx >= 0 {
			pivotLow := snap.ClosedCandles[pivotLowIdx].Low
			bounceHigh := snap.ClosedCandles[pivotLowIdx+1].High
			for i := pivotLowIdx + 2; i <= n-2; i++ {
				if snap.ClosedCandles[i].High > bounceHigh {
					bounceHigh = snap.ClosedCandles[i].High
				}
			}
			shortBody := (latest.Open - latest.Close) / barRange
			shortOK := bounceHigh >= pivotLow+(minBounceATR*ctx.ATR14) &&
				bounceHigh < ctx.EMA200 &&
				prev.Close >= pivotLow &&
				latest.Close < pivotLow &&
				latest.Close < latest.Open &&
				shortBody >= minBody &&
				lowerWick < 0.40*barRange &&
				ctx.BarDeltaZScore <= -minDeltaZ &&
				ctx.OIValidShort &&
				snap.FundingRate >= -cfg.MaxAdverseFundingRate
			if shortOK {
				return AlphaCandidate{Side: model.SideShort, Approved: true}
			}
		}
	}

	// LONG: symmetric break above confirmed 5-bar pivot high after higher-low pullback.
	if latest.Close > ctx.EMA200 && latest.Close > ctx.VWAP &&
		(latest.Close-ctx.EMA200) <= maxEMA200ATR*ctx.ATR14 {
		pivotHighIdx := -1
		for p := n - 4; p >= minPivotIdx; p-- {
			c := snap.ClosedCandles[p]
			if c.High >= snap.ClosedCandles[p-2].High &&
				c.High >= snap.ClosedCandles[p-1].High &&
				c.High >= snap.ClosedCandles[p+1].High &&
				c.High >= snap.ClosedCandles[p+2].High {
				pivotHighIdx = p
				break
			}
		}
		if pivotHighIdx >= 0 {
			pivotHigh := snap.ClosedCandles[pivotHighIdx].High
			pullLow := snap.ClosedCandles[pivotHighIdx+1].Low
			for i := pivotHighIdx + 2; i <= n-2; i++ {
				if snap.ClosedCandles[i].Low < pullLow {
					pullLow = snap.ClosedCandles[i].Low
				}
			}
			longBody := (latest.Close - latest.Open) / barRange
			longOK := pullLow <= pivotHigh-(minBounceATR*ctx.ATR14) &&
				pullLow > ctx.EMA200 &&
				prev.Close <= pivotHigh &&
				latest.Close > pivotHigh &&
				latest.Close > latest.Open &&
				longBody >= minBody &&
				upperWick < 0.40*barRange &&
				ctx.BarDeltaZScore >= minDeltaZ &&
				ctx.OIValidLong &&
				snap.FundingRate <= cfg.MaxAdverseFundingRate
			if longOK {
				return AlphaCandidate{Side: model.SideLong, Approved: true}
			}
		}
	}

	return AlphaCandidate{
		Reason:    "STRUCT_CONDITIONS_NOT_MET",
		SubReason: fmt.Sprintf("RVOL_%.2f_DZ_%.2f", rvol, ctx.BarDeltaZScore),
	}
}
