package validation

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/metrics"
	"trml/internal/backtest/replay"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
	"trml/internal/strategy"
)

// WalkForwardConfig configures rolling In-Sample / Out-of-Sample window validation.
type WalkForwardConfig struct {
	TrainDays     int
	TestDays      int
	StepDays      int
	From          time.Time
	To            time.Time
	InitialEquity float64
	BenchmarkSym  string
	ReplayCfg     replay.ReplayConfig
	EvalCfg       strategy.EvaluatorConfig
	BrokerCfg     simbroker.SimBrokerConfig
	CandleMap     map[string][]data.HistoricalCandle
	Filters       map[string]model.ExchangeFilter
}

// WalkForwardFold stores one rolling IS + OOS fold comparison and verdict.
type WalkForwardFold struct {
	FoldIndex      int                     `json:"fold_index"`
	ISFrom         time.Time               `json:"is_from"`
	ISTo           time.Time               `json:"is_to"`
	OOSFrom        time.Time               `json:"oos_from"`
	OOSTo          time.Time               `json:"oos_to"`
	ISSummary      metrics.BacktestSummary `json:"is_summary"`
	OOSSummary     metrics.BacktestSummary `json:"oos_summary"`
	DegradationPct float64                 `json:"degradation_pct"`
	Verdict        string                  `json:"verdict"`
}

// WalkForwardReport aggregates stitched Out-of-Sample performance across all rolling folds.
type WalkForwardReport struct {
	Folds              []WalkForwardFold       `json:"folds"`
	StitchedOOSSummary metrics.BacktestSummary `json:"stitched_oos_summary"`
	StitchedOOSCurve   []simbroker.EquityPoint `json:"stitched_oos_curve,omitempty"`
	MeanDegradationPct float64                 `json:"mean_degradation_pct"`
	OverallVerdict     string                  `json:"overall_verdict"`
}

// RunWalkForward executes rolling IS/OOS validation folds over windowed chunks to prevent look-ahead
// and release per-fold buffers deterministically after each window.
func RunWalkForward(ctx context.Context, cfg WalkForwardConfig) (WalkForwardReport, error) {
	if cfg.TrainDays <= 0 {
		cfg.TrainDays = 45
	}
	if cfg.TestDays <= 0 {
		cfg.TestDays = 15
	}
	if cfg.StepDays <= 0 {
		cfg.StepDays = cfg.TestDays
	}
	if cfg.BenchmarkSym == "" {
		cfg.BenchmarkSym = "BTCUSDT"
	}
	initEq := cfg.InitialEquity
	if initEq <= 0 {
		initEq, _ = cfg.BrokerCfg.InitialEquityUSDT.Float64()
		if initEq <= 0 {
			initEq = 125.0
		}
	}

	barDur := cfg.EvalCfg.CandleDuration
	if barDur <= 0 {
		barDur = 5 * time.Minute
	}
	warmupBars := cfg.ReplayCfg.WarmupBars
	if warmupBars <= 0 {
		warmupBars = 200
	}
	htfEMA := cfg.EvalCfg.HTFEMAPeriod
	if cfg.EvalCfg.MajorPod.IsEnabled() && htfEMA < 200 {
		htfEMA = 200
	}
	leadBars := strategy.CalcRequiredHistoryBars(
		barDur, warmupBars, htfEMA, cfg.EvalCfg.MarketBreadthLowThreshold > 0,
	)
	warmupLead := time.Duration(leadBars) * barDur
	if cfg.EvalCfg.MajorPod.IsEnabled() && warmupLead < 720*time.Hour {
		warmupLead = 720 * time.Hour
	}

	var (
		folds             []WalkForwardFold
		stitchedTrades    []simbroker.SimTradeRecord
		stitchedCurve     []simbroker.EquityPoint
		seenCurveMs       = make(map[int64]bool)
		seenTradeKeys     = make(map[string]bool)
		runningOOSCapital = initEq
		cumulativeOOSPnL  float64
		aggAudit          metrics.ExecutionAudit
		hasAggAudit       bool
		sumDegrad         float64
		foldIdx           int
	)
	compounding := cfg.BrokerCfg.EquityModeCompounding

	cursor := cfg.From
	trainDur := time.Duration(cfg.TrainDays) * 24 * time.Hour
	testDur := time.Duration(cfg.TestDays) * 24 * time.Hour
	stepDur := time.Duration(cfg.StepDays) * 24 * time.Hour

	for {
		select {
		case <-ctx.Done():
			return WalkForwardReport{}, ctx.Err()
		default:
		}

		isFrom := cursor
		isTo := isFrom.Add(trainDur).Add(-time.Second)
		oosFrom := isFrom.Add(trainDur)
		oosTo := oosFrom.Add(testDur).Add(-time.Second)
		if oosTo.After(cfg.To) {
			break
		}

		isSum, _, _, err := runWindowedSlice(ctx, cfg, isFrom, isTo, warmupLead, warmupBars, initEq)
		if err != nil {
			return WalkForwardReport{}, fmt.Errorf("fold %d IS failed: %w", foldIdx, err)
		}
		var oosSum metrics.BacktestSummary
		var oosTrades []simbroker.SimTradeRecord
		var oosCurve []simbroker.EquityPoint
		if compounding && runningOOSCapital <= 0 {
			oosSum = metrics.BacktestSummary{InitialEquity: 0, FinalEquity: 0, SampleWarning: true}
		} else {
			foldStartEq := initEq
			if compounding {
				foldStartEq = runningOOSCapital
			}
			oosSum, oosTrades, oosCurve, err = runWindowedSlice(ctx, cfg, oosFrom, oosTo, warmupLead, warmupBars, foldStartEq)
			if err != nil {
				return WalkForwardReport{}, fmt.Errorf("fold %d OOS failed: %w", foldIdx, err)
			}
		}

		degrad, verdict := scoreFoldDegradation(isSum.ExpectancyUSDT, oosSum.ExpectancyUSDT, oosSum.TotalTrades)
		sumDegrad += degrad
		folds = append(folds, WalkForwardFold{
			FoldIndex:      foldIdx,
			ISFrom:         isFrom,
			ISTo:           isTo,
			OOSFrom:        oosFrom,
			OOSTo:          oosTo,
			ISSummary:      isSum,
			OOSSummary:     oosSum,
			DegradationPct: degrad,
			Verdict:        verdict,
		})

		for _, tr := range oosTrades {
			tKey := fmt.Sprintf("%s|%d|%d|%s|%s", tr.Symbol, tr.EntryTime.UnixNano(), tr.ExitTime.UnixNano(), tr.ExitReason, tr.ID)
			if seenTradeKeys[tKey] {
				continue
			}
			seenTradeKeys[tKey] = true
			stitchedTrades = append(stitchedTrades, tr)
		}
		for _, pt := range oosCurve {
			if !pt.Time.Before(oosFrom) && !pt.Time.After(oosTo) {
				tsMs := pt.Time.UnixMilli()
				if seenCurveMs[tsMs] {
					continue
				}
				seenCurveMs[tsMs] = true
				shifted := pt
				if !compounding {
					shifted.RealizedEquity += cumulativeOOSPnL
					shifted.EffectiveEquity += cumulativeOOSPnL
				}
				stitchedCurve = append(stitchedCurve, shifted)
			}
		}
		cumulativeOOSPnL += oosSum.TotalNetPnL
		if compounding {
			if oosSum.FinalEquity > 0 {
				runningOOSCapital = oosSum.FinalEquity
			} else {
				runningOOSCapital = 0
			}
		}
		if ea := oosSum.ExecutionAudit; ea != nil {
			hasAggAudit = true
			aggAudit.Submitted += ea.Submitted
			aggAudit.Filled += ea.Filled
			aggAudit.MarketFilled += ea.MarketFilled
			aggAudit.TimedOut += ea.TimedOut
			aggAudit.Repegged += ea.Repegged
			aggAudit.RepegAborted += ea.RepegAborted
			aggAudit.UnsplittableTP1 += ea.UnsplittableTP1
			aggAudit.SLFirstConflicts += ea.SLFirstConflicts
			aggAudit.DelistSettled += ea.DelistSettled
			aggAudit.FundingSettlements += ea.FundingSettlements
			aggAudit.LiquidatedCount += ea.LiquidatedCount
			aggAudit.SubBarExecCount += ea.SubBarExecCount
			aggAudit.DelistForceClosed += ea.DelistForceClosed
			aggAudit.OICoveredCount += ea.OICoveredCount
			aggAudit.OIFallbackCount += ea.OIFallbackCount
			aggAudit.OIMissingCount += ea.OIMissingCount
		}

		runtime.GC()
		foldIdx++
		cursor = cursor.Add(stepDur)
	}

	if len(folds) == 0 {
		return WalkForwardReport{}, fmt.Errorf(
			"date range %s..%s too short for TrainDays=%d + TestDays=%d",
			cfg.From.Format("2006-01-02"), cfg.To.Format("2006-01-02"), cfg.TrainDays, cfg.TestDays,
		)
	}

	var finalAudit *metrics.ExecutionAudit
	if hasAggAudit {
		totalOI := aggAudit.OICoveredCount + aggAudit.OIFallbackCount + aggAudit.OIMissingCount
		if totalOI > 0 {
			aggAudit.OICoveragePct = (float64(aggAudit.OICoveredCount) / float64(totalOI)) * 100.0
		}
		finalAudit = &aggAudit
	}

	sort.Slice(stitchedCurve, func(i, j int) bool {
		return stitchedCurve[i].Time.Before(stitchedCurve[j].Time)
	})
	stitchedSummary := metrics.CalculateMetricsV2(
		stitchedTrades, stitchedCurve, initEq, cfg.CandleMap[cfg.BenchmarkSym], nil, finalAudit,
	)
	meanDegrad := sumDegrad / float64(len(folds))
	overallVerdict := classifyOverallWFA(folds, stitchedSummary, meanDegrad)

	return WalkForwardReport{
		Folds:              folds,
		StitchedOOSSummary: stitchedSummary,
		StitchedOOSCurve:   stitchedCurve,
		MeanDegradationPct: meanDegrad,
		OverallVerdict:     overallVerdict,
	}, nil
}

func runWindowedSlice(
	ctx context.Context,
	cfg WalkForwardConfig,
	winFrom, winTo time.Time,
	warmupLead time.Duration,
	warmupBars int,
	equity float64,
) (metrics.BacktestSummary, []simbroker.SimTradeRecord, []simbroker.EquityPoint, error) {
	sliceStart := winFrom.Add(-warmupLead)
	winCandles := sliceCandlesWindow(cfg.CandleMap, sliceStart, winTo)
	if len(winCandles) == 0 {
		return metrics.BacktestSummary{InitialEquity: equity, FinalEquity: equity, SampleWarning: true}, nil, nil, nil
	}

	winExec := sliceCandlesWindow(cfg.ReplayCfg.ExecCandleMap, sliceStart, winTo)

	rCfg := cfg.ReplayCfg
	rCfg.WarmupBars = countWarmupBars(winCandles, winFrom, warmupBars)
	rCfg.ExecCandleMap = winExec
	rCfg.SettleOpenPositionsAtEnd = true

	bCfg := cfg.BrokerCfg
	bCfg.InitialEquityUSDT = decimal.NewFromFloat(equity)

	sim := simbroker.NewSimBroker(bCfg)
	eng := replay.NewReplayEngine(rCfg, cfg.EvalCfg, sim, winCandles, cfg.Filters)
	if err := eng.Run(ctx); err != nil {
		return metrics.BacktestSummary{}, nil, nil, err
	}

	// Only attribute trades and equity curve samples inside [winFrom..winTo] (exclude warm-up leakage).
	var filteredTrades []simbroker.SimTradeRecord
	for _, tr := range sim.ClosedTrades {
		if !tr.EntryTime.Before(winFrom) && !tr.EntryTime.After(winTo) {
			filteredTrades = append(filteredTrades, tr)
		}
	}
	var filteredCurve []simbroker.EquityPoint
	for _, pt := range sim.EquityCurve {
		if !pt.Time.Before(winFrom) && !pt.Time.After(winTo) {
			filteredCurve = append(filteredCurve, pt)
		}
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
		filteredTrades, filteredCurve, equity, winCandles[cfg.BenchmarkSym], nil, execAudit,
	)
	return summary, filteredTrades, filteredCurve, nil
}

func sliceCandlesWindow(src map[string][]data.HistoricalCandle, from, to time.Time) map[string][]data.HistoricalCandle {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string][]data.HistoricalCandle, len(src))
	for sym, bars := range src {
		var sliced []data.HistoricalCandle
		for _, c := range bars {
			if c.CloseTime.Before(from) || c.OpenTime.After(to) {
				continue
			}
			sliced = append(sliced, c)
		}
		if len(sliced) > 0 {
			out[sym] = sliced
		}
	}
	return out
}

func countWarmupBars(winCandles map[string][]data.HistoricalCandle, winFrom time.Time, fallbackWarmup int) int {
	timelineWarm := make(map[int64]struct{})
	for _, bars := range winCandles {
		for _, c := range bars {
			if c.CloseTime.Before(winFrom) {
				timelineWarm[c.CloseTime.UnixMilli()] = struct{}{}
			}
		}
	}
	if len(timelineWarm) > 0 {
		return len(timelineWarm)
	}
	return fallbackWarmup
}

func scoreFoldDegradation(isExp, oosExp float64, oosTrades int) (degradPct float64, verdict string) {
	if oosTrades == 0 {
		return 0, "NO_OOS_TRADES"
	}
	if isExp <= 0 {
		if oosExp > 0 {
			return 0, "WARNING"
		}
		return 100.0, "OVERFIT"
	}
	degradPct = (1.0 - (oosExp / isExp)) * 100.0
	if math.IsNaN(degradPct) || math.IsInf(degradPct, 0) {
		degradPct = 100.0
	}
	switch {
	case oosExp <= 0 || degradPct > 40.0:
		return degradPct, "OVERFIT"
	case degradPct > 25.0 || oosTrades < 5:
		return degradPct, "WARNING"
	default:
		return degradPct, "PASS"
	}
}

func classifyOverallWFA(folds []WalkForwardFold, stitched metrics.BacktestSummary, meanDegrad float64) string {
	overfitCount := 0
	for _, f := range folds {
		if f.Verdict == "OVERFIT" {
			overfitCount++
		}
	}
	if stitched.ExpectancyUSDT <= 0 || overfitCount > len(folds)/2 || meanDegrad > 40.0 {
		return "OVERFIT"
	}
	if overfitCount > 0 || meanDegrad > 25.0 || stitched.TotalTrades < 20 {
		return "WARNING"
	}
	return "PASS"
}
