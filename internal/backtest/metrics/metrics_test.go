package metrics

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
)

func TestCalculateMetrics_AggregationAndStats(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Setup 1: Win (Leg TP1 + Leg Runner Trailing)
	t1Leg1 := simbroker.SimTradeRecord{
		ID:         "SIM_BTC_1_TP1",
		Symbol:     "BTCUSDT",
		Side:       model.SideLong,
		EntryTime:  now,
		ExitTime:   now.Add(15 * time.Minute),
		GrossPnL:   decimal.NewFromFloat(5.0),
		NetPnL:     decimal.NewFromFloat(4.5),
		FeeTotal:   decimal.NewFromFloat(0.5),
		ExitReason: "PARTIAL_TAKE_PROFIT",
	}
	t1Leg2 := simbroker.SimTradeRecord{
		ID:         "SIM_BTC_1_RUNNER",
		Symbol:     "BTCUSDT",
		Side:       model.SideLong,
		EntryTime:  now,
		ExitTime:   now.Add(30 * time.Minute),
		GrossPnL:   decimal.NewFromFloat(10.0),
		NetPnL:     decimal.NewFromFloat(9.2),
		FeeTotal:   decimal.NewFromFloat(0.8),
		ExitReason: "TRAILING_STOP",
	}

	// Setup 2: Loss (Full SL)
	t2 := simbroker.SimTradeRecord{
		ID:         "SIM_ETH_2",
		Symbol:     "ETHUSDT",
		Side:       model.SideLong,
		EntryTime:  now.Add(1 * time.Hour),
		ExitTime:   now.Add(1*time.Hour + 10*time.Minute),
		GrossPnL:   decimal.NewFromFloat(-8.0),
		NetPnL:     decimal.NewFromFloat(-8.8),
		FeeTotal:   decimal.NewFromFloat(0.8),
		ExitReason: "STOP_LOSS",
	}

	raw := []simbroker.SimTradeRecord{t1Leg1, t1Leg2, t2}
	summary := CalculateMetrics(raw, 1000.0)

	// 3 raw legs should aggregate into 2 setups
	if summary.TotalTrades != 2 {
		t.Fatalf("expected 2 aggregated setups, got %d", summary.TotalTrades)
	}

	if summary.WinsCount != 1 || summary.LossesCount != 1 {
		t.Errorf("expected 1 win and 1 loss, got %d wins / %d losses", summary.WinsCount, summary.LossesCount)
	}

	if summary.FullWinsCount != 1 {
		t.Errorf("expected 1 full win, got %d", summary.FullWinsCount)
	}

	// Win Rate: 1 / 2 = 50.0%
	if summary.WinRatePct != 50.0 {
		t.Errorf("expected 50%% WR, got %f", summary.WinRatePct)
	}

	// Total Net PnL: (4.5 + 9.2) - 8.8 = 13.7 - 8.8 = +4.9 USDT
	if summary.TotalNetPnL < 4.89 || summary.TotalNetPnL > 4.91 {
		t.Errorf("expected net PnL ~4.9, got %f", summary.TotalNetPnL)
	}

	// Expectancy: 4.9 / 2 = +2.45 USDT
	if summary.ExpectancyUSDT < 2.44 || summary.ExpectancyUSDT > 2.46 {
		t.Errorf("expected expectancy ~2.45, got %f", summary.ExpectancyUSDT)
	}
}

func TestBootstrapConfidenceInterval(t *testing.T) {
	samples := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	lower, upper := BootstrapConfidenceInterval(samples, 1000, 0.95)

	if lower >= upper {
		t.Errorf("expected lower < upper, got lower=%f upper=%f", lower, upper)
	}
	if lower < 1.0 || upper > 10.0 {
		t.Errorf("confidence interval out of reasonable sample bounds: [%f, %f]", lower, upper)
	}
}

func TestCalculateMetrics_NetProfitFactor(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	raw := []simbroker.SimTradeRecord{
		{
			ID:        "W1",
			Symbol:    "SOLUSDT",
			Side:      model.SideShort,
			EntryTime: now,
			ExitTime:  now.Add(5 * time.Minute),
			GrossPnL:  decimal.NewFromFloat(10.0),
			NetPnL:    decimal.NewFromFloat(8.0),
			FeeTotal:  decimal.NewFromFloat(2.0),
		},
		{
			ID:        "L1",
			Symbol:    "ETHUSDT",
			Side:      model.SideShort,
			EntryTime: now.Add(10 * time.Minute),
			ExitTime:  now.Add(15 * time.Minute),
			GrossPnL:  decimal.NewFromFloat(-4.0),
			NetPnL:    decimal.NewFromFloat(-5.0),
			FeeTotal:  decimal.NewFromFloat(1.0),
		},
	}

	summary := CalculateMetrics(raw, 100.0)
	if summary.ProfitFactor != 2.5 {
		t.Errorf("expected gross ProfitFactor 2.5, got %f", summary.ProfitFactor)
	}
	if summary.NetProfitFactor != 1.6 {
		t.Errorf("expected NetProfitFactor 1.6 (8.0/5.0), got %f", summary.NetProfitFactor)
	}
}

