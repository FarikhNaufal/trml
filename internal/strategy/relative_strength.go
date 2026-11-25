package strategy

import (
	"fmt"
	"math"
	"strings"

	"trml/internal/model"
)

// RelativeResidualAlpha implements a market-relative cross-sectional alpha measuring
// volatility-normalized altcoin excess return vs BTC over LookbackBars.
// MODE=MEANREV fades extreme overextension away from BTC & VWAP on rejection flow.
// MODE=MOMENTUM trades persistent relative leaders/laggards aligned with EMA200 & VWAP.
type RelativeResidualAlpha struct{}

// ID returns the canonical identifier for cross-sectional relative residual alpha.
func (RelativeResidualAlpha) ID() string {
	return StrategyIDRelResidual
}

// Evaluate satisfies AlphaEvaluator with nil BTC context (fails closed since BTC benchmark is required).
func (a RelativeResidualAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	return a.EvaluateWithBTC(ctx, snap, nil, cfg)
}

// EvaluateWithBTC scores altcoin return residual against BTC and returns an entry candidate when confirmed.
func (RelativeResidualAlpha) EvaluateWithBTC(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	cfg EvaluatorConfig,
) AlphaCandidate {
	if rej, ok := checkPrimaryNATRBounds(ctx, cfg, "REL"); !ok {
		return rej
	}
	if strings.EqualFold(snap.Symbol, "BTCUSDT") {
		return AlphaCandidate{Reason: "REL_BENCHMARK_SYMBOL"}
	}
	if btcSnap == nil {
		return AlphaCandidate{Reason: "REL_MISSING_BTC_CONTEXT"}
	}

	lookback := cfg.RelResidual.LookbackBars
	if lookback <= 0 {
		lookback = 12
	}
	minRelZ := cfg.RelResidual.MinRelZ
	if minRelZ <= 0 {
		minRelZ = 1.80
	}
	minStretchATR := cfg.RelResidual.MinVWAPStretchATR
	minDeltaZ := cfg.RelResidual.MinDeltaZ

	altN := len(snap.ClosedCandles)
	btcN := len(btcSnap.ClosedCandles)
	if altN < lookback+1 || btcN < lookback+1 {
		return AlphaCandidate{
			Reason:    "REL_INSUFFICIENT_HISTORY",
			SubReason: fmt.Sprintf("ALT_%d_BTC_%d_NEED_%d", altN, btcN, lookback+1),
		}
	}
	if !ctx.DeltaZOK {
		return AlphaCandidate{Reason: "INVALID_ZSCORE_BASELINE"}
	}
	if !ctx.OIDeltaOK {
		return AlphaCandidate{Reason: "INVALID_OI_DELTA"}
	}
	if ctx.NATRPct <= 0 || ctx.ATR14 <= 0 {
		return AlphaCandidate{Reason: "INVALID_ATR"}
	}

	altPrev := snap.ClosedCandles[altN-1-lookback].Close
	btcPrev := btcSnap.ClosedCandles[btcN-1-lookback].Close
	btcLatest := btcSnap.ClosedCandles[btcN-1].Close
	latest := ctx.Latest
	if altPrev <= 0 || btcPrev <= 0 || btcLatest <= 0 {
		return AlphaCandidate{Reason: "REL_INVALID_PRICES"}
	}

	altRet := (latest.Close - altPrev) / altPrev
	btcRet := (btcLatest - btcPrev) / btcPrev
	denom := (ctx.NATRPct / 100.0) * math.Sqrt(float64(lookback))
	if denom <= 0 {
		return AlphaCandidate{Reason: "REL_INVALID_DENOM"}
	}
	relZ := (altRet - btcRet) / denom

	barRange := latest.High - latest.Low
	if barRange <= 0 {
		return AlphaCandidate{Reason: "REL_CONDITIONS_NOT_MET", SubReason: "ZERO_BAR_RANGE"}
	}
	upperWick := latest.High - math.Max(latest.Open, latest.Close)
	lowerWick := math.Min(latest.Open, latest.Close) - latest.Low
	mode := strings.ToUpper(strings.TrimSpace(cfg.RelResidual.Mode))
	if mode == "" {
		mode = "MEANREV"
	}

	if mode == "MOMENTUM" {
		shortOK := relZ <= -minRelZ &&
			latest.Close < ctx.EMA200 &&
			latest.Close <= ctx.VWAP &&
			latest.Close < latest.Open &&
			lowerWick < 0.45*barRange &&
			ctx.BarDeltaZScore <= -minDeltaZ &&
			ctx.OIValidShort &&
			snap.FundingRate >= -cfg.MaxAdverseFundingRate
		if shortOK {
			return AlphaCandidate{Side: model.SideShort, Approved: true, BypassBTCTier2: cfg.RelResidual.BypassBTCTier2}
		}

		longOK := relZ >= minRelZ &&
			latest.Close > ctx.EMA200 &&
			latest.Close >= ctx.VWAP &&
			latest.Close > latest.Open &&
			upperWick < 0.45*barRange &&
			ctx.BarDeltaZScore >= minDeltaZ &&
			ctx.OIValidLong &&
			snap.FundingRate <= cfg.MaxAdverseFundingRate
		if longOK {
			return AlphaCandidate{Side: model.SideLong, Approved: true, BypassBTCTier2: cfg.RelResidual.BypassBTCTier2}
		}

		return AlphaCandidate{
			Reason:    "REL_CONDITIONS_NOT_MET",
			SubReason: fmt.Sprintf("MODE_MOM_RELZ_%.2f_MIN_%.2f_DZ_%.2f", relZ, minRelZ, ctx.BarDeltaZScore),
		}
	}

	// Default MEANREV mode: fade extreme altcoin excess moves away from BTC & Session VWAP.
	shortFadeOK := relZ >= minRelZ &&
		latest.Close >= ctx.VWAP+(minStretchATR*ctx.ATR14) &&
		latest.Close < latest.Open &&
		(upperWick >= 0.20*barRange || (latest.Close-latest.Low) <= 0.40*barRange) &&
		ctx.BarDeltaZScore <= -minDeltaZ &&
		ctx.OIValidShort &&
		snap.FundingRate >= -cfg.MaxAdverseFundingRate
	if shortFadeOK {
		return AlphaCandidate{Side: model.SideShort, Approved: true, BypassBTCTier2: cfg.RelResidual.BypassBTCTier2}
	}

	longFadeOK := relZ <= -minRelZ &&
		latest.Close <= ctx.VWAP-(minStretchATR*ctx.ATR14) &&
		latest.Close > latest.Open &&
		(lowerWick >= 0.20*barRange || (latest.High-latest.Close) <= 0.40*barRange) &&
		ctx.BarDeltaZScore >= minDeltaZ &&
		ctx.OIValidLong &&
		snap.FundingRate <= cfg.MaxAdverseFundingRate
	if longFadeOK {
		return AlphaCandidate{Side: model.SideLong, Approved: true, BypassBTCTier2: cfg.RelResidual.BypassBTCTier2}
	}

	return AlphaCandidate{
		Reason:    "REL_CONDITIONS_NOT_MET",
		SubReason: fmt.Sprintf("MODE_MR_RELZ_%.2f_MIN_%.2f_DZ_%.2f", relZ, minRelZ, ctx.BarDeltaZScore),
	}
}
