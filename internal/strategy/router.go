package strategy

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

var (
	primaryAlphaVWAP     = VWAPPullbackAlpha{}
	secondaryAlphaSweep  = LiquiditySweepAlpha{}
	alphaVolBreakout     = VolBreakoutAlpha{}
	alphaRelResidual     = RelativeResidualAlpha{}
	alphaEMAPullback     = EMAPullbackAlpha{}
	alphaVWAPBreakdown   = VWAPBreakdownAlpha{}
	alphaStructureBreak  = StructureBreakAlpha{}
)

func tryAlphaCandidate(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	cand AlphaCandidate,
	strategyID string,
	effectiveEquity, availBal decimal.Decimal,
	cfg EvaluatorConfig,
) (PortfolioDecision, string, string) {
	if !cand.Approved {
		return PortfolioDecision{}, cand.Reason, cand.SubReason
	}
	okPost, rPost, srPost := evaluateSideAndPostFiltersWithOpts(ctx, snap, btcSnap, cand.Side, cfg, cand.BypassBTCTier2)
	if !okPost {
		return PortfolioDecision{}, rPost, srPost
	}
	intent, okBuild, rBuild, srBuild := resolveEntryAndBuildIntent(
		ctx, snap, btcSnap, cand.Side, strategyID, effectiveEquity, availBal, cfg,
	)
	if !okBuild {
		return PortfolioDecision{}, rBuild, srBuild
	}
	sub := srBuild
	if strategyID == StrategyIDLiquiditySweep {
		sub = StrategyIDLiquiditySweep
	}
	return PortfolioDecision{
		Intent:     intent,
		Approved:   true,
		StrategyID: strategyID,
		Reason:     rBuild,
		SubReason:  sub,
	}, "", ""
}

func breakoutMatchesMacroTrend(cand AlphaCandidate, macroRegime MacroRegime, cfg EvaluatorConfig) (bool, string, string) {
	if !cand.Approved || !cfg.MacroRouter.Enabled || !cfg.VolBreakout.AlignMacroTrend {
		return true, "", ""
	}
	if (cand.Side == model.SideLong && macroRegime == MacroRegimeTrendUp) ||
		(cand.Side == model.SideShort && macroRegime == MacroRegimeTrendDown) {
		return true, "", ""
	}
	return false, "BREAKOUT_MACRO_DIRECTION_MISMATCH", fmt.Sprintf("SIDE_%s_REGIME_%s", cand.Side, macroRegime)
}

func evalBreakoutAlpha(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	macroRegime MacroRegime,
	effectiveEquity, availBal decimal.Decimal,
	evalCfg EvaluatorConfig,
) (PortfolioDecision, string, string) {
	cand := alphaVolBreakout.Evaluate(ctx, snap, evalCfg)
	if okDir, rDir, srDir := breakoutMatchesMacroTrend(cand, macroRegime, evalCfg); !okDir {
		return PortfolioDecision{}, rDir, srDir
	}
	return tryAlphaCandidate(ctx, snap, btcSnap, cand, StrategyIDVolBreakout, effectiveEquity, availBal, evalCfg)
}

// EvaluatePortfolio evaluates enabled alphas in deterministic priority order, optionally gated by the
// 1h BTC Macro Regime Router. Shared pre-checks, post-filters, and sizing run from a single SSOT path.
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

	relMeanRevActive := cfg.RelResidual.Enabled && strings.ToUpper(strings.TrimSpace(cfg.RelResidual.Mode)) != "MOMENTUM"
	if !relMeanRevActive {
		if okSide, rSide, srSide := evaluateEarlyAllowedSide(ctx.Latest.Close, ctx.EMA200, cfg); !okSide {
			return PortfolioDecision{
				Approved:   false,
				StrategyID: StrategyIDVWAPPullback,
				Reason:     rSide,
				SubReason:  srSide,
			}
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

	macroRegime := MacroRegimeUnknown
	macroVolRatio := 0.0
	if cfg.MacroRouter.Enabled && btcSnap != nil {
		macroMetrics := ClassifyMacroRegime(
			btcSnap.ClosedCandles,
			cfg.MacroRouter.TrendERThreshold,
			cfg.MacroRouter.VolShockRatio,
		)
		macroRegime = macroMetrics.Regime
		macroVolRatio = macroMetrics.VolRatio1h
	}

	scopeAllowBO := scope.AllowPrimary
	if scope.BreakoutScopeExplicit {
		scopeAllowBO = scope.AllowBreakout
	}
	allowVWAP := scope.AllowPrimary && !cfg.DisableVWAP &&
		(!cfg.MacroRouter.Enabled || !cfg.MacroRouter.VWAPOnlyInRange || macroRegime == MacroRegimeRange || macroRegime == MacroRegimeUnknown)
	allowSweep := cfg.Sweep.Enabled && scope.AllowSweep
	allowBreakout := cfg.VolBreakout.Enabled && scopeAllowBO &&
		(!cfg.MacroRouter.Enabled || !cfg.MacroRouter.BreakoutOnlyInTrend || macroRegime == MacroRegimeTrendUp || macroRegime == MacroRegimeTrendDown)
	allowEMAPull := cfg.EMAPullback.Enabled && scope.AllowPrimary
	allowVWAPBreak := cfg.VWAPBreakdown.Enabled && scope.AllowPrimary
	allowStructBreak := cfg.StructureBreak.Enabled && scope.AllowPrimary
	allowRel := cfg.RelResidual.Enabled && scope.AllowPrimary &&
		(!cfg.MacroRouter.Enabled || !cfg.MacroRouter.RelOnlyInRange || macroRegime == MacroRegimeRange || macroRegime == MacroRegimeUnknown)

	evalCfg := cfg
	if cfg.MacroRouter.Enabled && macroRegime == MacroRegimeRange &&
		isFinitePositive(cfg.MacroRouter.RangeRiskMult) && cfg.MacroRouter.RangeRiskMult < 1.0 {
		evalCfg.Sizing.RiskPerTradePct = evalCfg.Sizing.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.MacroRouter.RangeRiskMult))
	}
	if cfg.MacroRouter.Enabled && cfg.MacroRouter.VolExpansionRatio > 0 && macroVolRatio >= cfg.MacroRouter.VolExpansionRatio &&
		isFinitePositive(cfg.MacroRouter.VolExpansionRiskMult) && cfg.MacroRouter.VolExpansionRiskMult != 1.0 {
		evalCfg.Sizing.RiskPerTradePct = evalCfg.Sizing.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.MacroRouter.VolExpansionRiskMult))
	}

	var primReason, primSubReason string
	var boReason, boSubReason string

	if evalCfg.BreakoutPriorityFirst {
		if allowBreakout {
			dec, r, sr := evalBreakoutAlpha(ctx, snap, btcSnap, macroRegime, effectiveEquity, availBal, evalCfg)
			if dec.Approved {
				return dec
			}
			boReason, boSubReason = r, sr
		}
		if allowVWAP {
			dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, primaryAlphaVWAP.Evaluate(ctx, snap, evalCfg), StrategyIDVWAPPullback, effectiveEquity, availBal, evalCfg)
			if dec.Approved {
				return dec
			}
			primReason, primSubReason = r, sr
		}
	} else {
		// Priority 1: Primary Strategy (VWAP Pullback)
		if allowVWAP {
			dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, primaryAlphaVWAP.Evaluate(ctx, snap, evalCfg), StrategyIDVWAPPullback, effectiveEquity, availBal, evalCfg)
			if dec.Approved {
				return dec
			}
			primReason, primSubReason = r, sr
		}

		// Priority 2: Volatility Compression Donchian Breakout
		if allowBreakout {
			dec, r, sr := evalBreakoutAlpha(ctx, snap, btcSnap, macroRegime, effectiveEquity, availBal, evalCfg)
			if dec.Approved {
				return dec
			}
			boReason, boSubReason = r, sr
		}
	}

	// Priority 2.5: Orthogonal Strategy #3 Candidates (EMA Pullback, VWAP Breakdown, Structure Break)
	var emaPullReason, emaPullSubReason string
	if allowEMAPull {
		dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, alphaEMAPullback.Evaluate(ctx, snap, evalCfg), StrategyIDEMAPullback, effectiveEquity, availBal, evalCfg)
		if dec.Approved {
			return dec
		}
		emaPullReason, emaPullSubReason = r, sr
	}

	var vwapBreakReason, vwapBreakSubReason string
	if allowVWAPBreak {
		dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, alphaVWAPBreakdown.Evaluate(ctx, snap, evalCfg), StrategyIDVWAPBreakdown, effectiveEquity, availBal, evalCfg)
		if dec.Approved {
			return dec
		}
		vwapBreakReason, vwapBreakSubReason = r, sr
	}

	var structBreakReason, structBreakSubReason string
	if allowStructBreak {
		dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, alphaStructureBreak.Evaluate(ctx, snap, evalCfg), StrategyIDStructureBreak, effectiveEquity, availBal, evalCfg)
		if dec.Approved {
			return dec
		}
		structBreakReason, structBreakSubReason = r, sr
	}

	// Priority 3: Secondary Strategy (1-Bar SHORT-ONLY Liquidity Sweep)
	var sweepReason, sweepSubReason string
	if allowSweep {
		dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, secondaryAlphaSweep.Evaluate(ctx, snap, evalCfg), StrategyIDLiquiditySweep, effectiveEquity, availBal, evalCfg)
		if dec.Approved {
			return dec
		}
		sweepReason, sweepSubReason = r, sr
	}

	// Priority 4: Cross-Sectional Relative Residual vs BTC
	var relReason, relSubReason string
	if allowRel {
		dec, r, sr := tryAlphaCandidate(ctx, snap, btcSnap, alphaRelResidual.EvaluateWithBTC(ctx, snap, btcSnap, evalCfg), StrategyIDRelResidual, effectiveEquity, availBal, evalCfg)
		if dec.Approved {
			return dec
		}
		relReason, relSubReason = r, sr
	}

	if sweepReason != "" && boReason == "" && relReason == "" &&
		shouldAttributeRejectionToSweep(SymbolScope{AllowPrimary: allowVWAP, AllowSweep: allowSweep}, ctx, primReason, sweepReason, sweepSubReason) {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDLiquiditySweep,
			Reason:     sweepReason,
			SubReason:  sweepSubReason,
		}
	}
	if allowVWAP && primReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     primReason,
			SubReason:  primSubReason,
		}
	}
	if boReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVolBreakout,
			Reason:     boReason,
			SubReason:  boSubReason,
		}
	}
	if emaPullReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDEMAPullback,
			Reason:     emaPullReason,
			SubReason:  emaPullSubReason,
		}
	}
	if vwapBreakReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPBreakdown,
			Reason:     vwapBreakReason,
			SubReason:  vwapBreakSubReason,
		}
	}
	if structBreakReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDStructureBreak,
			Reason:     structBreakReason,
			SubReason:  structBreakSubReason,
		}
	}
	if relReason != "" {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDRelResidual,
			Reason:     relReason,
			SubReason:  relSubReason,
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
	if cfg.MacroRouter.Enabled && (macroRegime != MacroRegimeUnknown) {
		return PortfolioDecision{
			Approved:   false,
			StrategyID: StrategyIDVWAPPullback,
			Reason:     "REGIME_ROUTER_BLOCKED",
			SubReason:  string(macroRegime),
		}
	}
	return PortfolioDecision{
		Approved:   false,
		StrategyID: StrategyIDVWAPPullback,
		Reason:     "CONDITIONS_NOT_MET",
		SubReason:  "",
	}
}

// shouldAttributeRejectionToSweep determines when a dual-rejected candle on a shared watchlist symbol
// should be logged under LIQUIDITY_SWEEP_SHORT in signal_audit_log for dashboard observability.
func shouldAttributeRejectionToSweep(
	scope SymbolScope,
	ctx sharedMarketContext,
	primReason, sweepReason, sweepSubReason string,
) bool {
	if !scope.AllowPrimary {
		return true
	}
	// Only attribute to SHORT-ONLY sweep when price is in the bearish macro zone (below EMA200 & VWAP).
	if ctx.Latest.Close >= ctx.EMA200 || ctx.Latest.Close > ctx.VWAP {
		return false
	}
	// 1. Primary VWAP Pullback disqualified by distance from VWAP/EMA200, so Sweep is the active alpha on this bar.
	if primReason == "ENTRY_TOO_FAR_FROM_VWAP" || primReason == "ENTRY_TOO_FAR_FROM_EMA200" {
		return true
	}
	// 2. Candle actively swept and reclaimed the N-bar high (or passed Sweep and hit a post-filter),
	// so reporting the specific Sweep rejection reason is more informative than generic pullback failure.
	if sweepReason != "SWEEP_CONDITIONS_NOT_MET" || !strings.Contains(sweepSubReason, "NO_HIGH_SWEEP_RECLAIM") {
		return true
	}
	return false
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
