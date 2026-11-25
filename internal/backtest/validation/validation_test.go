package validation

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/metrics"
	"trml/internal/backtest/replay"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
	"trml/internal/strategy"
)

func TestRunMonteCarlo_DeterminismAndOrdering(t *testing.T) {
	pnls := []float64{25, -15, 30, -10, 40, -20, 15, -12, 35, -18, 22, -10}
	r1 := RunMonteCarlo(pnls, 100.0, 500, 42)
	r2 := RunMonteCarlo(pnls, 100.0, 500, 42)

	if r1.P95MaxDDPct != r2.P95MaxDDPct || r1.P99MaxDDPct != r2.P99MaxDDPct {
		t.Fatalf("expected deterministic Monte Carlo results: %+v vs %+v", r1, r2)
	}
	if r1.P50MaxDDPct > r1.P95MaxDDPct || r1.P95MaxDDPct > r1.P99MaxDDPct {
		t.Fatalf("expected p50 <= p95 <= p99, got %.2f <= %.2f <= %.2f", r1.P50MaxDDPct, r1.P95MaxDDPct, r1.P99MaxDDPct)
	}
	if r1.ExpectancyCI95Lo > r1.ExpectancyMean || r1.ExpectancyCI95Hi < r1.ExpectancyMean {
		t.Fatalf("expected CI bounds around mean, got lo=%.2f mean=%.2f hi=%.2f", r1.ExpectancyCI95Lo, r1.ExpectancyMean, r1.ExpectancyCI95Hi)
	}
}

func TestScoreFoldDegradation_Verdicts(t *testing.T) {
	if deg, v := scoreFoldDegradation(10.0, 8.5, 25); v != "PASS" || deg < 14.9 || deg > 15.1 {
		t.Fatalf("expected PASS (~15%% degradation), got deg=%.2f v=%s", deg, v)
	}
	if deg, v := scoreFoldDegradation(10.0, 6.5, 25); v != "WARNING" || deg < 34.9 || deg > 35.1 {
		t.Fatalf("expected WARNING (~35%% degradation), got deg=%.2f v=%s", deg, v)
	}
	if deg, v := scoreFoldDegradation(10.0, 4.0, 25); v != "OVERFIT" || deg < 59.9 || deg > 60.1 {
		t.Fatalf("expected OVERFIT (~60%% degradation), got deg=%.2f v=%s", deg, v)
	}
	if _, v := scoreFoldDegradation(10.0, -1.0, 25); v != "OVERFIT" {
		t.Fatalf("expected OVERFIT when OOS expectancy <= 0, got %s", v)
	}
}

func TestRunWalkForwardAndFrictionStressMatrix(t *testing.T) {
	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	// Build 18 days of 1h candles for fast synthetic WFA & stress tests (432 bars)
	candles := make([]data.HistoricalCandle, 18*24)
	price := 100.0
	for i := range candles {
		price += 0.05
		candles[i] = data.HistoricalCandle{
			Symbol:    "SOLUSDT",
			OpenTime:  t0.Add(time.Duration(i) * time.Hour),
			CloseTime: t0.Add(time.Duration(i+1) * time.Hour),
			Open:      price - 0.1,
			High:      price + 0.3,
			Low:       price - 0.3,
			Close:     price,
			Volume:    1000,
		}
	}
	cMap := map[string][]data.HistoricalCandle{"SOLUSDT": candles}
	filters := map[string]model.ExchangeFilter{
		"SOLUSDT": {
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
	}

	evalCfg := strategy.EvaluatorConfig{
		CandleDuration:      time.Hour,
		EMAPeriod:           20,
		ATRPeriod:           14,
		MaxSlots:            3,
		MaxSpreadPct:        0.10,
		DisableOIFilter:     true,
		BTCGateEnabled:      false,
		MaxDailyDrawdownPct: -0.05,
	}
	brokerCfg := simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000),
		MaxSlots:          3,
		AccountLeverage:   5.0,
		MakerFeeRate:      decimal.NewFromFloat(0.0002),
		TakerFeeRate:      decimal.NewFromFloat(0.0005),
		SlippageRate:      decimal.NewFromFloat(0.0002),
		PenetrationTicks:  1,
	}

	wfa, err := RunWalkForward(context.Background(), WalkForwardConfig{
		TrainDays:     6,
		TestDays:      4,
		StepDays:      4,
		From:          t0,
		To:            t0.Add(18 * 24 * time.Hour),
		InitialEquity: 1000.0,
		BenchmarkSym:  "SOLUSDT",
		ReplayCfg:     replay.ReplayConfig{WarmupBars: 10, OIMode: replay.OIModeOff, BenchmarkSym: "SOLUSDT"},
		EvalCfg:       evalCfg,
		BrokerCfg:     brokerCfg,
		CandleMap:     cMap,
		Filters:       filters,
	})
	if err != nil {
		t.Fatalf("RunWalkForward failed: %v", err)
	}
	if len(wfa.Folds) < 2 {
		t.Fatalf("expected >= 2 WFA folds over 18d with 6/4/4 schedule, got %d", len(wfa.Folds))
	}
	for _, f := range wfa.Folds {
		if f.OOSSummary.InitialEquity != 1000.0 {
			t.Fatalf("expected non-compounding OOS fold InitialEquity=1000.0, got %.2f in fold %d", f.OOSSummary.InitialEquity, f.FoldIndex)
		}
	}
	if wfa.StitchedOOSSummary.ExecutionAudit == nil {
		t.Fatalf("expected aggregated ExecutionAudit on stitched WFA summary")
	}

	stress, err := RunFrictionStressMatrix(context.Background(), StressConfig{
		BaseReplayCfg: replay.ReplayConfig{WarmupBars: 25, OIMode: replay.OIModeOff, BenchmarkSym: "SOLUSDT"},
		EvalCfg:       evalCfg,
		BaseBrokerCfg: brokerCfg,
		CandleMap:     cMap,
		Filters:       filters,
		BenchmarkSym:  "SOLUSDT",
		Multipliers:   []float64{1.0, 2.0, 3.0},
		TestPen2:      true,
	})
	if err != nil {
		t.Fatalf("RunFrictionStressMatrix failed: %v", err)
	}
	if len(stress) != 4 || stress[0].SlippageBps >= stress[2].SlippageBps {
		t.Fatalf("unexpected stress matrix scenarios: %+v", stress)
	}
}

func TestRunCompoundingMonteCarlo_ScalesMultiplicatively(t *testing.T) {
	// Compounding account grows from 100 to ~1000 over 40 alternating +12% / -4% trades.
	// Late losses are -40 USDT (40% of initial 100), which causes fake >50% DD under additive
	// fixed-dollar shuffling, whereas multiplicative return shuffling keeps true ~4% step drawdowns.
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	eq := 100.0
	trades := make([]metrics.AggregatedTrade, 0, 60)
	pnls := make([]float64, 0, 60)
	for i := 0; i < 60; i++ {
		ret := 0.12
		if i%3 == 1 {
			ret = -0.04
		}
		pnl := eq * ret
		eq += pnl
		pnls = append(pnls, pnl)
		trades = append(trades, metrics.AggregatedTrade{
			SetupID:  "T",
			Symbol:   "BTCUSDT",
			ExitTime: t0.Add(time.Duration(i+1) * time.Hour),
			NetPnL:   pnl,
		})
	}

	compMC := RunCompoundingMonteCarlo(trades, 100.0, 500, 42)
	addMC := RunMonteCarlo(pnls, 100.0, 500, 42)
	if compMC.RiskOfRuin50Pct != 0.0 {
		t.Fatalf("expected 0%% RoR>=50%% under multiplicative MC for +12%%/-4%% returns, got %.2f%%", compMC.RiskOfRuin50Pct)
	}
	if compMC.P95MaxDDPct >= addMC.P95MaxDDPct {
		t.Fatalf("expected compounding MC p95 DD (%.2f%%) < additive MC p95 DD (%.2f%%)", compMC.P95MaxDDPct, addMC.P95MaxDDPct)
	}
}

func TestMonteCarlo_AdditiveDrawdownClampedAt100PctAndOverlappingWFADedupe(t *testing.T) {
	// Additive MC with cumulative losses exceeding initial equity must clamp DD at <= 100%.
	mc := RunMonteCarlo([]float64{-60, -60, -60}, 100.0, 200, 7)
	if mc.P95MaxDDPct > 100.0 || mc.P99MaxDDPct > 100.0 {
		t.Fatalf("expected additive MC DD clamped at <= 100%%, got p95=%.2f p99=%.2f", mc.P95MaxDDPct, mc.P99MaxDDPct)
	}

	// Overlapping WFA folds (StepDays=2, TestDays=4) must produce unique Curve timestamps.
	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	candles := make([]data.HistoricalCandle, 16*24)
	for i := range candles {
		candles[i] = data.HistoricalCandle{
			Symbol:      "SOLUSDT",
			OpenTime:    t0.Add(time.Duration(i) * time.Hour),
			CloseTime:   t0.Add(time.Duration(i+1) * time.Hour),
			Open:        100,
			High:        101,
			Low:         99,
			Close:       100.2,
			Volume:      1000,
			QuoteVolume: 100000,
		}
	}
	wfa, err := RunWalkForward(context.Background(), WalkForwardConfig{
		TrainDays:     4,
		TestDays:      4,
		StepDays:      2,
		From:          t0,
		To:            t0.Add(16 * 24 * time.Hour),
		InitialEquity: 1000.0,
		BenchmarkSym:  "SOLUSDT",
		ReplayCfg:     replay.ReplayConfig{WarmupBars: 10, OIMode: replay.OIModeOff, BenchmarkSym: "SOLUSDT"},
		EvalCfg:       strategy.EvaluatorConfig{CandleDuration: time.Hour, EMAPeriod: 20, ATRPeriod: 14, DisableOIFilter: true, MaxDailyDrawdownPct: -0.05},
		BrokerCfg:     simbroker.SimBrokerConfig{InitialEquityUSDT: decimal.NewFromFloat(1000), MaxSlots: 3},
		CandleMap:     map[string][]data.HistoricalCandle{"SOLUSDT": candles},
		Filters:       map[string]model.ExchangeFilter{"SOLUSDT": {TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5)}},
	})
	if err != nil {
		t.Fatalf("overlapping RunWalkForward failed: %v", err)
	}
	seen := make(map[int64]bool)
	for _, pt := range wfa.StitchedOOSCurve {
		ms := pt.Time.UnixMilli()
		if seen[ms] {
			t.Fatalf("duplicate timestamp %v in StitchedOOSCurve", pt.Time)
		}
		seen[ms] = true
	}
}
