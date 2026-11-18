package validation

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/metrics"
	"trml/internal/backtest/replay"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
	"trml/internal/strategy"
)

// StressConfig specifies input configurations and friction multipliers for stress testing.
type StressConfig struct {
	BaseReplayCfg replay.ReplayConfig
	EvalCfg       strategy.EvaluatorConfig
	BaseBrokerCfg simbroker.SimBrokerConfig
	CandleMap     map[string][]data.HistoricalCandle
	Filters       map[string]model.ExchangeFilter
	BenchmarkSym  string
	Multipliers   []float64
	TestPen2      bool // Also append a harsh 2x slippage + 2 penetration tick scenario
}

// StressScenario holds the summary metrics for one friction multiplier test case.
type StressScenario struct {
	Label            string                  `json:"label"`
	SlippageMult     float64                 `json:"slippage_mult"`
	SlippageBps      float64                 `json:"slippage_bps"`
	PenetrationTicks int                     `json:"penetration_ticks"`
	Summary          metrics.BacktestSummary `json:"summary"`
}

// RunFrictionStressMatrix evaluates strategy resilience across escalating slippage/fill friction assumptions.
func RunFrictionStressMatrix(ctx context.Context, cfg StressConfig) ([]StressScenario, error) {
	mults := cfg.Multipliers
	if len(mults) == 0 {
		mults = []float64{1.0, 1.5, 2.0, 3.0}
	}
	bench := cfg.BenchmarkSym
	if bench == "" {
		bench = "BTCUSDT"
	}

	type scenarioSpec struct {
		label string
		mult  float64
		pen   int
	}
	specs := make([]scenarioSpec, 0, len(mults)+1)
	for _, m := range mults {
		specs = append(specs, scenarioSpec{
			label: fmt.Sprintf("%.1fx Slippage (Pen=%d)", m, cfg.BaseBrokerCfg.PenetrationTicks),
			mult:  m,
			pen:   cfg.BaseBrokerCfg.PenetrationTicks,
		})
	}
	if cfg.TestPen2 && cfg.BaseBrokerCfg.PenetrationTicks < 2 {
		specs = append(specs, scenarioSpec{
			label: "2.0x Slippage + Pen=2 Ticks",
			mult:  2.0,
			pen:   2,
		})
	}

	results := make([]StressScenario, 0, len(specs))
	initEq, _ := cfg.BaseBrokerCfg.InitialEquityUSDT.Float64()

	for _, sc := range specs {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		bCfg := cfg.BaseBrokerCfg
		bCfg.SlippageRate = bCfg.SlippageRate.Mul(decimal.NewFromFloat(sc.mult))
		bCfg.PenetrationTicks = sc.pen

		evalCfg := cfg.EvalCfg
		evalCfg.Sizing.SlippageRate = bCfg.SlippageRate

		sim := simbroker.NewSimBroker(bCfg)
		eng := replay.NewReplayEngine(cfg.BaseReplayCfg, evalCfg, sim, cfg.CandleMap, cfg.Filters)
		if err := eng.Run(ctx); err != nil {
			return nil, fmt.Errorf("stress scenario %s failed: %w", sc.label, err)
		}

		execAudit := &metrics.ExecutionAudit{
			SimExecutionStats: sim.Stats,
			SubBarExecCount:   eng.Stats.SubBarExecCount,
			DelistForceClosed: eng.Stats.DelistForceClosed,
			OICoveredCount:    eng.Stats.OICoveredCount,
			OIFallbackCount:   eng.Stats.OIFallbackCount,
			OIMissingCount:    eng.Stats.OIMissingCount,
			OICoveragePct:     eng.Stats.OICoveragePct,
		}
		summary := metrics.CalculateMetricsV2(
			sim.ClosedTrades, sim.EquityCurve, initEq,
			cfg.CandleMap[bench], nil, execAudit,
		)
		slipBps, _ := bCfg.SlippageRate.Mul(decimal.NewFromFloat(10000)).Float64()

		results = append(results, StressScenario{
			Label:            sc.label,
			SlippageMult:     sc.mult,
			SlippageBps:      slipBps,
			PenetrationTicks: sc.pen,
			Summary:          summary,
		})
	}
	return results, nil
}
