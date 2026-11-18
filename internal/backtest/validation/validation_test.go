package validation

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
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
