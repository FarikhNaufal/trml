package strategy

import (
	"fmt"
	"math"

	"trml/internal/indicator"
	"trml/internal/model"
)

// VolBreakoutAlpha detects volatility compression followed by a high-RVOL,
// high-body-conviction Donchian range breakout aligned with EMA200, VWAP, and orderflow delta.
type VolBreakoutAlpha struct{}

// ID returns the canonical identifier for the Volatility Compression Breakout alpha.
func (VolBreakoutAlpha) ID() string {
	return StrategyIDVolBreakout
}

// Evaluate checks volatility squeeze and directional Donchian breakout confirmations.
func (VolBreakoutAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	lookback := cfg.VolBreakout.LookbackBars
	if lookback <= 0 {
		lookback = 20
	}
	maxComp := cfg.VolBreakout.MaxCompressionRatio
	if maxComp <= 0 {
		maxComp = 0.90
	}
	minRVOL := cfg.VolBreakout.MinRVOL
	if minRVOL <= 0 {
		minRVOL = 1.50
	}
	minBody := cfg.VolBreakout.MinBodyRatio
	if minBody <= 0 {
		minBody = 0.50
	}
	minDeltaZ := cfg.VolBreakout.MinDeltaZ
	if minDeltaZ < 0 {
		minDeltaZ = 0
	}

	n := len(snap.ClosedCandles)
	minNeeded := 65
	if lookback+2 > minNeeded {
		minNeeded = lookback + 2
	}
	if n < minNeeded {
		return AlphaCandidate{
			Reason:    "BREAKOUT_INSUFFICIENT_HISTORY",
			SubReason: fmt.Sprintf("CANDLES_%d_NEED_%d", n, minNeeded),
		}
	}
	if !ctx.DeltaZOK {
		return AlphaCandidate{Reason: "INVALID_ZSCORE_BASELINE"}
	}
	if !ctx.OIDeltaOK {
		return AlphaCandidate{Reason: "INVALID_OI_DELTA"}
	}

	prior := snap.ClosedCandles[:n-1]
	atrShort := indicator.CalculateATR(prior, 14)
	atrLong := indicator.CalculateATR(prior, 60)
	if atrShort <= 0 || atrLong <= 0 {
		return AlphaCandidate{Reason: "INVALID_ATR"}
	}
	compRatio := atrShort / atrLong
	if compRatio > maxComp {
		return AlphaCandidate{
			Reason:    "BREAKOUT_NO_COMPRESSION",
			SubReason: fmt.Sprintf("RATIO_%.2f_MAX_%.2f", compRatio, maxComp),
		}
	}

	rvol := indicator.CalculateRVOL(snap.ClosedCandles, 20)
	if math.IsNaN(rvol) || math.IsInf(rvol, 0) || rvol < minRVOL {
		return AlphaCandidate{
			Reason:    "BREAKOUT_LOW_RVOL",
			SubReason: fmt.Sprintf("RVOL_%.2f_MIN_%.2f", rvol, minRVOL),
		}
	}

	latest := ctx.Latest
	barRange := latest.High - latest.Low
	if barRange <= 0 {
		return AlphaCandidate{Reason: "BREAKOUT_CONDITIONS_NOT_MET", SubReason: "ZERO_RANGE"}
	}
	bodyRatio := math.Abs(latest.Close-latest.Open) / barRange
	if bodyRatio < minBody {
		return AlphaCandidate{
			Reason:    "BREAKOUT_WEAK_BODY",
			SubReason: fmt.Sprintf("BODY_%.2f_MIN_%.2f", bodyRatio, minBody),
		}
	}

	boMinNATR := cfg.VolBreakout.MinNATRPct
	if boMinNATR <= 0 {
		boMinNATR = cfg.MinNATRPct
	}
	if boMinNATR > 0 && ctx.NATRPct < boMinNATR {
		return AlphaCandidate{
			Reason:    "BREAKOUT_NATR_TOO_LOW",
			SubReason: fmt.Sprintf("NATR_%.2f%%_MIN_%.2f%%", ctx.NATRPct, boMinNATR),
		}
	}
	boMaxNATR := cfg.VolBreakout.MaxNATRPct
	if boMaxNATR <= 0 {
		boMaxNATR = cfg.MaxNATRPct
	}
	if boMaxNATR > 0 && ctx.NATRPct > boMaxNATR {
		return AlphaCandidate{
			Reason:    "BREAKOUT_NATR_TOO_HIGH",
			SubReason: fmt.Sprintf("NATR_%.2f%%_MAX_%.2f%%", ctx.NATRPct, boMaxNATR),
		}
	}
	if cfg.VolBreakout.MaxEMA200DistATR > 0 && ctx.ATR14 > 0 {
		distATR := math.Abs(latest.Close-ctx.EMA200) / ctx.ATR14
		if distATR > cfg.VolBreakout.MaxEMA200DistATR {
			return AlphaCandidate{
				Reason:    "BREAKOUT_TOO_FAR_FROM_EMA200",
				SubReason: fmt.Sprintf("DIST_%.2f_ATR_MAX_%.2f_ATR", distATR, cfg.VolBreakout.MaxEMA200DistATR),
			}
		}
	}
	if cfg.VolBreakout.MaxVWAPDistATR > 0 && ctx.ATR14 > 0 {
		distATR := math.Abs(latest.Close-ctx.VWAP) / ctx.ATR14
		if distATR > cfg.VolBreakout.MaxVWAPDistATR {
			return AlphaCandidate{
				Reason:    "BREAKOUT_TOO_FAR_FROM_VWAP",
				SubReason: fmt.Sprintf("DIST_%.2f_ATR_MAX_%.2f_ATR", distATR, cfg.VolBreakout.MaxVWAPDistATR),
			}
		}
	}

	startIdx := n - 1 - lookback
	hh := snap.ClosedCandles[startIdx].High
	ll := snap.ClosedCandles[startIdx].Low
	for i := startIdx + 1; i < n-1; i++ {
		if snap.ClosedCandles[i].High > hh {
			hh = snap.ClosedCandles[i].High
		}
		if snap.ClosedCandles[i].Low < ll {
			ll = snap.ClosedCandles[i].Low
		}
	}

	longOK := latest.Close > hh &&
		latest.Close > ctx.EMA200 &&
		latest.Close > ctx.VWAP &&
		latest.Close > latest.Open &&
		ctx.BarDeltaZScore >= minDeltaZ &&
		ctx.OIValidLong &&
		snap.FundingRate <= cfg.MaxAdverseFundingRate
	if longOK {
		return AlphaCandidate{Side: model.SideLong, Approved: true}
	}

	shortOK := latest.Close < ll &&
		latest.Close < ctx.EMA200 &&
		latest.Close < ctx.VWAP &&
		latest.Close < latest.Open &&
		ctx.BarDeltaZScore <= -minDeltaZ &&
		ctx.OIValidShort &&
		snap.FundingRate >= -cfg.MaxAdverseFundingRate
	if shortOK {
		return AlphaCandidate{Side: model.SideShort, Approved: true}
	}

	return AlphaCandidate{
		Reason:    "BREAKOUT_CONDITIONS_NOT_MET",
		SubReason: fmt.Sprintf("COMP_%.2f_RVOL_%.2f_DZ_%.2f", compRatio, rvol, ctx.BarDeltaZScore),
	}
}
