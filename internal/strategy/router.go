package strategy

import (
	"fmt"
	"hash/fnv"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

var (
	primaryAlphaVWAP       = VWAPPullbackAlpha{}
	secondaryAlphaSweep    = LiquiditySweepAlpha{}
)

// EvaluatePortfolio evaluates enabled alphas in strict priority order:
// Priority 1: Primary VWAP Pullback / Orderflow Bounce (when scope.AllowPrimary).
// Priority 2: Secondary 1-Bar Liquidity Sweep SHORT (when cfg.Sweep.Enabled && scope.AllowSweep).
// All shared pre-checks, indicator computations, post-filters, and sizing run from a single SSOT path.
func EvaluatePortfolio(
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	activeSlots int,
	isDailyBreached bool,
	hasActiveSymbol bool,
	effectiveEquity, availBal decimal.Decimal,
	scope SymbolScope,
	cfg EvaluatorConfig,
) PortfolioDecision {
	ctx, ok, reason, subReason := evaluateSharedPreChecks(snap, activeSlots, isDailyBreached, hasActiveSymbol, cfg)
	if !ok {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     reason,
			SubReason:  subReason,
		}
	}

	// Case C: Random Entry Benchmark Mode
	if cfg.RandomEntrySeed > 0 {
		return evaluateRandomBenchmark(ctx, snap, effectiveEquity, availBal, cfg)
	}

	if okSide, rSide, srSide := evaluateEarlyAllowedSide(ctx.Latest.Close, ctx.EMA200, cfg); !okSide {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     rSide,
			SubReason:  srSide,
		}
	}

	if okTime, rTime, srTime := evaluateSessionTimeGuards(snap, cfg); !okTime {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     rTime,
			SubReason:  srTime,
		}
	}

	// Priority 1: Primary Strategy (VWAP Pullback)
	var primReason, primSubReason string
	if scope.AllowPrimary {
		cand := primaryAlphaVWAP.Evaluate(ctx, snap, cfg)
		if cand.Approved {
			if okPost, rPost, srPost := evaluateSideAndPostFilters(ctx, snap, btcSnap, cand.Side, cfg); okPost {
				intent, okBuild, rBuild, srBuild := resolveEntryAndBuildIntent(
					ctx, snap, btcSnap, cand.Side, StrategyIDVWAPPullback, effectiveEquity, availBal, cfg,
				)
				if okBuild {
					return PortfolioDecision{
						Intent:     intent,
						Approved:   true,
						StrategyID: StrategyIDVWAPPullback,
						Reason:     rBuild,
						SubReason:  srBuild,
					}
				}
				primReason, primSubReason = rBuild, srBuild
			} else {
				primReason, primSubReason = rPost, srPost
			}
		} else {
			primReason, primSubReason = cand.Reason, cand.SubReason
		}
	}

	// Priority 2: Secondary Strategy (1-Bar SHORT-ONLY Liquidity Sweep)
	var sweepReason, sweepSubReason string
	if cfg.Sweep.Enabled && scope.AllowSweep {
		cand := secondaryAlphaSweep.Evaluate(ctx, snap, cfg)
		if cand.Approved {
			if okPost, rPost, srPost := evaluateSideAndPostFilters(ctx, snap, btcSnap, cand.Side, cfg); okPost {
				intent, okBuild, rBuild, srBuild := resolveEntryAndBuildIntent(
					ctx, snap, btcSnap, cand.Side, StrategyIDLiquiditySweep, effectiveEquity, availBal, cfg,
				)
				if okBuild {
					return PortfolioDecision{
						Intent:     intent,
						Approved:   true,
						StrategyID: StrategyIDLiquiditySweep,
						Reason:     rBuild,
						SubReason:  StrategyIDLiquiditySweep,
					}
				}
				sweepReason, sweepSubReason = rBuild, srBuild
			} else {
				sweepReason, sweepSubReason = rPost, srPost
			}
		} else {
			sweepReason, sweepSubReason = cand.Reason, cand.SubReason
		}
	}

	if scope.AllowPrimary && primReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     primReason,
			SubReason:  primSubReason,
		}
	}
	if sweepReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDLiquiditySweep,
			Reason:     sweepReason,
			SubReason:  sweepSubReason,
		}
	}
	return PortfolioDecision{
		Approved:   false,
		StrategyID: StrategyIDVWAPPullback,
		Reason:     "CONDITIONS_NOT_MET",
		SubReason:  "",
	}
}

func evaluateRandomBenchmark(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	effectiveEquity, availBal decimal.Decimal,
	cfg EvaluatorConfig,
) PortfolioDecision {
	prob := cfg.RandomEntryProb
	if prob <= 0 {
		prob = 0.0004
	}
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d:%s:%d", cfg.RandomEntrySeed, snap.Symbol, ctx.Latest.CloseTime.Unix())
	hashVal := h.Sum64()
	normalized := float64(hashVal%1000000) / 1000000.0
	if normalized >= prob {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     "RANDOM_ENTRY_NOT_TRIGGERED",
		}
	}

	targetSide := model.SideShort
	if cfg.AllowedSide == "LONG_ONLY" {
		targetSide = model.SideLong
	} else if cfg.AllowedSide == "BOTH" {
		if (hashVal>>32)%2 == 0 {
			targetSide = model.SideLong
		}
	}

	sizingCfg := cfg.Sizing
	if sizingCfg.EntryOrderType == "" {
		sizingCfg.EntryOrderType = cfg.EntryOrderType
	}
	intent, ok := BuildSafeDecimalOrderIntent(snap, effectiveEquity, availBal, targetSide, ctx.Latest.Close, ctx.ATR14, sizingCfg)
	if !ok {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     "SIZING_FAILED",
		}
	}
	intent.StrategyID = StrategyIDVWAPPullback
	return PortfolioDecision{
		Intent:     intent,
		Approved:   true,
		StrategyID: StrategyIDVWAPPullback,
		Reason:     "SIGNAL_APPROVED",
		SubReason:  "RANDOM_ENTRY_BENCHMARK",
	}
}
