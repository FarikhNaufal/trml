package metrics

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
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

func TestCalculateMetricsV2_MTMDrawdownRegimeAndAttribution(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	// Build 60 BTC bull trend candles (rising smoothly so Close > EMA & VWAP).
	btc := make([]data.HistoricalCandle, 60)
	price := 40000.0
	for i := range btc {
		price += 20.0
		btc[i] = data.HistoricalCandle{
			Symbol:    "BTCUSDT",
			OpenTime:  t0.Add(time.Duration(i) * 5 * time.Minute),
			CloseTime: t0.Add(time.Duration(i+1) * 5 * time.Minute),
			Open:      price - 10,
			High:      price + 20,
			Low:       price - 20,
			Close:     price,
			Volume:    100,
		}
	}

	raw := []simbroker.SimTradeRecord{
		{
			ID: "T1", Symbol: "SOLUSDT", Side: model.SideLong,
			EntryTime: btc[50].CloseTime, ExitTime: btc[50].CloseTime.Add(20 * time.Minute),
			EntryPrice: decimal.NewFromFloat(100), SLPrice: decimal.NewFromFloat(98),
			InitialQty: decimal.NewFromFloat(5), Qty: decimal.NewFromFloat(5),
			GrossPnL: decimal.NewFromFloat(25), NetPnL: decimal.NewFromFloat(24),
			ExitReason: "TAKE_PROFIT", EntryMode: "CLEAN_LIMIT",
		},
		{
			ID: "T2", Symbol: "DOGEUSDT", Side: model.SideLong,
			EntryTime: btc[52].CloseTime, ExitTime: btc[52].CloseTime.Add(10 * time.Minute),
			EntryPrice: decimal.NewFromFloat(1), SLPrice: decimal.NewFromFloat(0.95),
			InitialQty: decimal.NewFromFloat(100), Qty: decimal.NewFromFloat(100),
			GrossPnL: decimal.NewFromFloat(-4), NetPnL: decimal.NewFromFloat(-5),
			ExitReason: "STOP_LOSS", RepegCount: 1, EntryMode: "GTX_REPEG",
		},
	}

	// Equity curve includes an unrealized floating dip down to 920 before recovering -> MTM DD must exceed closed-trade DD!
	curve := []simbroker.EquityPoint{
		{Time: t0, RealizedEquity: 1000, EffectiveEquity: 1000, OpenPositions: 1},
		{Time: t0.Add(24 * time.Hour), RealizedEquity: 1000, EffectiveEquity: 920, OpenPositions: 1},
		{Time: t0.Add(48 * time.Hour), RealizedEquity: 1019, EffectiveEquity: 1019, OpenPositions: 0},
	}

	summary := CalculateMetricsV2(raw, curve, 1000.0, btc, &ScannerAudit{CyclesRun: 10}, &ExecutionAudit{OICoveragePct: 95.5})
	if summary.MTMMaxDrawdownUSDT < 79.9 || summary.MTMMaxDrawdownPct <= summary.MaxDrawdownPct {
		t.Fatalf("expected MTM DD (%.2f USDT / %.2f%%) > Closed DD (%.2f%%)",
			summary.MTMMaxDrawdownUSDT, summary.MTMMaxDrawdownPct, summary.MaxDrawdownPct)
	}
	if summary.UlcerIndex <= 0 || summary.ExposurePct <= 0 || summary.AvgRMultiple == 0 {
		t.Fatalf("expected non-zero UlcerIndex/ExposurePct/AvgRMultiple: %+v", summary)
	}
	if len(summary.TopSymbols) != 2 || summary.TopSymbols[0].Symbol != "SOLUSDT" || summary.BottomSymbols[0].Symbol != "DOGEUSDT" {
		t.Fatalf("unexpected symbol attribution: top=%+v bottom=%+v", summary.TopSymbols, summary.BottomSymbols)
	}
	if len(summary.Segmentation.HoldingSegments) != 4 {
		t.Fatalf("expected 4 holding buckets, got %d", len(summary.Segmentation.HoldingSegments))
	}

	bullCount := 0
	for _, r := range summary.RegimeSegments {
		if r.Name == RegimeBTCBull {
			bullCount = r.TotalTrades
		}
	}
	if bullCount != 2 {
		t.Fatalf("expected both trades classified into BTC_BULL, got regimes=%+v", summary.RegimeSegments)
	}
	if len(summary.Segmentation.EntryTypeSegments) != 3 ||
		summary.Segmentation.EntryTypeSegments[0].TotalTrades != 1 ||
		summary.Segmentation.EntryTypeSegments[1].TotalTrades != 1 {
		t.Fatalf("expected 1 Clean Limit + 1 GTX Repeg segment, got %+v", summary.Segmentation.EntryTypeSegments)
	}

	// Verify NetProfitFactor (24/5 = 4.80) vs Gross ProfitFactor (25/4 = 6.25) on LONG side segment
	longSeg := summary.Segmentation.SideSegments[0]
	if longSeg.ProfitFactor < 6.24 || longSeg.ProfitFactor > 6.26 {
		t.Fatalf("expected gross ProfitFactor 6.25, got %.4f", longSeg.ProfitFactor)
	}
	if longSeg.NetProfitFactor < 4.79 || longSeg.NetProfitFactor > 4.81 {
		t.Fatalf("expected NetProfitFactor 4.80, got %.4f", longSeg.NetProfitFactor)
	}
}

func TestCalculateMetricsV2_FullWinWithoutTP1ChronologicalDDAndRuinCAGR(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	// Single-leg full TAKE_PROFIT without PARTIAL_TAKE_PROFIT must count as FullWinsCount=1.
	// Also setup B exits earlier with loss -20 while setup A exits later with +30 -> MaxDD must reflect exit order (-20 = 20%)!
	raw := []simbroker.SimTradeRecord{
		{
			ID: "A_LATE_WIN", Symbol: "BTCUSDT", Side: model.SideLong,
			EntryTime: t0, ExitTime: t0.Add(48 * time.Hour),
			GrossPnL: decimal.NewFromFloat(31), NetPnL: decimal.NewFromFloat(30),
			FeeTotal: decimal.NewFromFloat(1), ExitReason: "TAKE_PROFIT",
		},
		{
			ID: "B_EARLY_LOSS", Symbol: "ETHUSDT", Side: model.SideLong,
			EntryTime: t0.Add(time.Hour), ExitTime: t0.Add(2 * time.Hour),
			GrossPnL: decimal.NewFromFloat(-19), NetPnL: decimal.NewFromFloat(-20),
			FeeTotal: decimal.NewFromFloat(1), ExitReason: "STOP_LOSS",
		},
	}
	s := CalculateMetricsV2(raw, nil, 100.0, nil, nil, nil)
	if s.FullWinsCount != 1 || s.PartialWinsCount != 0 {
		t.Fatalf("expected FullWinsCount=1 PartialWinsCount=0 without TP1, got full=%d part=%d", s.FullWinsCount, s.PartialWinsCount)
	}
	if s.MaxDrawdownUSDT < 19.9 || s.MaxDrawdownPct < 19.9 {
		t.Fatalf("expected chronological exit order to record 20 USDT (20%%) drawdown first, got usdt=%.2f pct=%.2f", s.MaxDrawdownUSDT, s.MaxDrawdownPct)
	}

	// Total ruin (FinalEquity <= 0) must report CAGR = -100.0
	ruin := []simbroker.SimTradeRecord{
		{
			ID: "RUIN", Symbol: "BTCUSDT", Side: model.SideLong,
			EntryTime: t0, ExitTime: t0.Add(10 * 24 * time.Hour),
			GrossPnL: decimal.NewFromFloat(-100), NetPnL: decimal.NewFromFloat(-105),
			ExitReason: "LIQUIDATION",
		},
	}
	sr := CalculateMetricsV2(ruin, nil, 100.0, nil, nil, nil)
	if sr.CAGR != -100.0 {
		t.Fatalf("expected CAGR -100.0 on ruin, got %.2f", sr.CAGR)
	}
}


