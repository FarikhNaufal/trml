package strategy

import (
	"fmt"
	"strings"

	"trml/internal/model"
)

// LiquiditySweepAlpha implements the secondary 1-bar SHORT-ONLY liquidity sweep rejection strategy.
// It detects upward stop-hunts above recent N-bar highs inside a macro downtrend (Close < EMA200 & <= VWAP)
// that reverse back inside the range as bearish rejection candles with negative orderflow delta.
type LiquiditySweepAlpha struct{}

// ID returns the canonical strategy identifier for telemetry and database attribution.
func (LiquiditySweepAlpha) ID() string {
	return StrategyIDLiquiditySweep
}

// Evaluate checks 1-bar SHORT-ONLY liquidity sweep entry conditions using precomputed shared context.
func (LiquiditySweepAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	if strings.ToUpper(strings.TrimSpace(cfg.AllowedSide)) == "LONG_ONLY" {
		return AlphaCandidate{
			Reason:    "SIDE_DISABLED_LONG_ONLY",
			SubReason: "SHORT_DISABLED_BY_CONFIG",
		}
	}

	lookback := cfg.Sweep.LookbackBars
	if lookback <= 0 {
		lookback = 12
	}
	minUpperWick := cfg.Sweep.MinUpperWickRatio
	if minUpperWick <= 0 {
		minUpperWick = 0.25
	}
	maxLowerWick := cfg.Sweep.MaxLowerWickRatio
	if maxLowerWick <= 0 {
		maxLowerWick = 0.45
	}
	// Note: 0.0 is a valid MaxDeltaZScore threshold (sellers >= buyers); do NOT override 0.0.
	maxDeltaZ := cfg.Sweep.MaxDeltaZScore

	n := len(snap.ClosedCandles)
	if n < lookback+1 {
		return AlphaCandidate{
			Reason:    "SWEEP_INSUFFICIENT_HISTORY",
			SubReason: fmt.Sprintf("CANDLES_%d_NEED_%d", n, lookback+1),
		}
	}
	if !ctx.DeltaZOK {
		return AlphaCandidate{Reason: "INVALID_ZSCORE_BASELINE"}
	}
	if !ctx.OIDeltaOK {
		return AlphaCandidate{Reason: "INVALID_OI_DELTA"}
	}

	latest := ctx.Latest
	minEMADist := cfg.MinEMA200DistanceATR * ctx.ATR14
	trendBearish := latest.Close < ctx.EMA200 && (minEMADist <= 0 || (ctx.EMA200-latest.Close) >= minEMADist)
	belowVWAP := latest.Close <= ctx.VWAP

	priorHigh := snap.ClosedCandles[n-1-lookback].High
	for i := n - lookback; i < n-1; i++ {
		if snap.ClosedCandles[i].High > priorHigh {
			priorHigh = snap.ClosedCandles[i].High
		}
	}
	sweptHigh := latest.High > priorHigh && latest.Close < priorHigh

	barRange := latest.High - latest.Low
	isBearishBar := barRange > 0 && latest.Close < latest.Open
	var hasRejectionWick bool
	if isBearishBar {
		upperWick := latest.High - latest.Open // since Close < Open, max(Open, Close) == Open
		lowerWick := latest.Close - latest.Low
		hasRejectionWick = upperWick >= (minUpperWick*barRange) && lowerWick < (maxLowerWick*barRange)
	}

	deltaOK := ctx.BarDeltaZScore <= maxDeltaZ
	oiOK := ctx.OIValidShort
	fundingOK := snap.FundingRate >= -cfg.MaxAdverseFundingRate

	if trendBearish && belowVWAP && sweptHigh && isBearishBar && hasRejectionWick && deltaOK && oiOK && fundingOK {
		return AlphaCandidate{
			Side:     model.SideShort,
			Approved: true,
		}
	}

	var fails []string
	if latest.Close >= ctx.EMA200 {
		fails = append(fails, "TREND_ABOVE_EMA200")
	} else if !trendBearish {
		fails = append(fails, "EMA200_CHOP_ZONE")
	}
	if !belowVWAP {
		fails = append(fails, "CLOSE_ABOVE_VWAP")
	}
	if !sweptHigh {
		fails = append(fails, "NO_HIGH_SWEEP_RECLAIM")
	}
	if !isBearishBar || !hasRejectionWick {
		fails = append(fails, "WEAK_REJECTION_CANDLE")
	}
	if !deltaOK {
		fails = append(fails, "WEAK_DELTA_ZSCORE")
	}
	if !oiOK {
		fails = append(fails, "LOW_OI_DELTA")
	}
	if !fundingOK {
		fails = append(fails, "ADVERSE_FUNDING")
	}

	return AlphaCandidate{
		Reason:    "SWEEP_CONDITIONS_NOT_MET",
		SubReason: strings.Join(fails, "|"),
	}
}
