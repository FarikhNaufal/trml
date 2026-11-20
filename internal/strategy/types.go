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

// SymbolScope controls which alpha modules are allowed to evaluate a symbol based on its scanner partition.
type SymbolScope struct {
	AllowPrimary bool
	AllowSweep   bool
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
	Side      model.Side
	Approved  bool
	Reason    string
	SubReason string
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
