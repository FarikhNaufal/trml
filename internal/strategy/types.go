package strategy

import (
	"strings"

	"trml/internal/model"
)

const (
	// StrategyIDVWAPPullback identifies the primary 5m VWAP pullback & orderflow bounce strategy (L1).
	StrategyIDVWAPPullback = "VWAP_PULLBACK"
	// StrategyIDLiquiditySweep identifies the secondary 1-bar SHORT-ONLY liquidity sweep strategy.
	StrategyIDLiquiditySweep = "LIQUIDITY_SWEEP_SHORT"
	// StrategyIDRelResidual identifies the cross-sectional relative residual alpha vs BTC.
	StrategyIDRelResidual = "REL_RESIDUAL"
	// StrategyIDVolBreakout identifies the volatility compression Donchian breakout alpha.
	StrategyIDVolBreakout = "VOL_BREAKOUT"
	// StrategyIDEMAPullback identifies the stacked EMA20/EMA50/EMA200 continuation pullback alpha.
	StrategyIDEMAPullback = "EMA_PULLBACK"
	// StrategyIDVWAPBreakdown identifies the failed-relief VWAP breakdown alpha.
	StrategyIDVWAPBreakdown = "VWAP_BREAKDOWN"
	// StrategyIDStructureBreak identifies the 5-bar pivot market-structure breakdown alpha.
	StrategyIDStructureBreak = "STRUCTURE_BREAK"
)

// NormalizeStrategyID canonicalizes empty strategy identifiers to the primary strategy ID.
func NormalizeStrategyID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return StrategyIDVWAPPullback
	}
	return trimmed
}

// SweepConfig configures the secondary 1-bar SHORT-ONLY liquidity sweep alpha.
type SweepConfig struct {
	Enabled           bool
	LookbackBars      int
	MinUpperWickRatio float64
	MaxLowerWickRatio float64
	MaxDeltaZScore    float64
}

// RelResidualConfig configures the cross-sectional relative residual alpha vs BTC.
type RelResidualConfig struct {
	Enabled           bool
	Mode              string // "MEANREV" or "MOMENTUM"
	LookbackBars      int
	MinRelZ           float64
	MinVWAPStretchATR float64
	MinDeltaZ         float64
	BypassBTCTier2    bool
}

// VolBreakoutConfig configures the volatility compression Donchian breakout alpha.
type VolBreakoutConfig struct {
	Enabled             bool
	LookbackBars        int
	MaxCompressionRatio float64
	MinRVOL             float64
	MinBodyRatio        float64
	MinDeltaZ           float64
	AlignMacroTrend     bool
	MaxEMA200DistATR    float64
	MaxVWAPDistATR      float64
	MaxNATRPct          float64
	MinNATRPct          float64
	RiskMult            float64
	EntryMarket         bool
}

// EMAPullbackConfig configures stacked EMA trend continuation pullback alpha.
type EMAPullbackConfig struct {
	Enabled          bool
	FastPeriod       int
	SlowPeriod       int
	RetestLookback   int
	TouchTolATR      float64
	TouchTolSet      bool
	MinBodyRatio     float64
	MinBodySet       bool
	MinDeltaZ        float64
	MaxEMA200DistATR float64
	RiskMult         float64
}

// VWAPBreakdownConfig configures failed relief-rally VWAP breakdown alpha.
type VWAPBreakdownConfig struct {
	Enabled         bool
	LookbackBars    int
	MinAboveBars    int
	MinRVOL         float64
	MinBodyRatio    float64
	MinBodySet      bool
	MinDeltaZ       float64
	MaxBelowVWAPATR float64
	RiskMult        float64
}

// StructureBreakConfig configures 5-bar swing pivot market-structure break alpha.
type StructureBreakConfig struct {
	Enabled          bool
	LookbackBars     int
	MinBounceATR     float64
	MinRVOL          float64
	MinBodyRatio     float64
	MinBodySet       bool
	MinDeltaZ        float64
	MaxEMA200DistATR float64
	RiskMult         float64
}

// MacroRegimeRouterConfig controls 1h BTC macro regime classification and per-alpha routing.
type MacroRegimeRouterConfig struct {
	Enabled              bool
	TrendERThreshold     float64
	VolShockRatio        float64
	VWAPOnlyInRange      bool
	BreakoutOnlyInTrend  bool
	RelOnlyInRange       bool
	RangeRiskMult        float64
	VolExpansionRatio    float64
	VolExpansionRiskMult float64
}

// FundingEnvelopeConfig applies optional asymmetric funding-rate crowd filters per side.
type FundingEnvelopeConfig struct {
	MinShortEnabled bool
	MinShortRate    float64
	MaxLongEnabled  bool
	MaxLongRate     float64
}

// SymbolScope controls which alpha modules are allowed to evaluate a symbol based on its scanner partition.
type SymbolScope struct {
	AllowPrimary          bool
	AllowBreakout         bool
	BreakoutScopeExplicit bool
	AllowSweep            bool
}

// DefaultSymbolScope allows both primary and secondary alpha evaluation (subject to config flags and priority).
func DefaultSymbolScope() SymbolScope {
	return SymbolScope{
		AllowPrimary: true,
		AllowSweep:   true,
	}
}

// AlphaCandidate represents the pure signal decision returned by an individual alpha module
// before shared post-filters (anti-frenzy, BTC macro gate) and sizing are applied.
type AlphaCandidate struct {
	Side           model.Side
	Approved       bool
	Reason         string
	SubReason      string
	BypassBTCTier2 bool
}

// PortfolioDecision captures the final prioritized portfolio decision across all enabled alphas.
type PortfolioDecision struct {
	Intent     model.OrderIntent
	Approved   bool
	StrategyID string
	Reason     string
	SubReason  string
}

// AlphaEvaluator defines the isolated contract implemented by each modular alpha strategy.
type AlphaEvaluator interface {
	ID() string
	Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate
}
