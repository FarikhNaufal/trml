package replay

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
	"trml/internal/strategy"
)

func TestReplayEngine_WithFundingAndDynamicSpread(t *testing.T) {
	sym := "SOLUSDT"
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	// Buat 100 candle 5m
	candles := make([]data.HistoricalCandle, 100)
	for i := 0; i < 100; i++ {
		tOpen := start.Add(time.Duration(i*5) * time.Minute)
		tClose := tOpen.Add(5*time.Minute - time.Millisecond)
		price := 100.0 + float64(i)*0.1
		candles[i] = data.HistoricalCandle{
			Symbol:           sym,
			OpenTime:         tOpen,
			CloseTime:        tClose,
			Open:             price - 0.05,
			High:             price + 0.20,
			Low:              price - 0.20,
			Close:            price,
			Volume:           1000.0,
			QuoteVolume:         100000.0,
			BarDeltaUSDT:        5000.0,
			TakerBuyQuoteVolume: 52500.0,
		}
	}

	candleMap := map[string][]data.HistoricalCandle{
		sym: candles,
	}

	filters := map[string]model.ExchangeFilter{
		sym: {
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
	}

	// Funding manager dengan rate 0.0001 pada 08:00
	fm := data.NewFundingManager("")
	fm.SetRatesForTesting(sym, []data.HistoricalFundingRate{
		{
			FundingTime:   start.Add(8 * time.Hour),
			FundingRate:   0.0001,
			IntervalHours: 8,
		},
	})

	brokerCfg := simbroker.SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		MaxSlots:          3,
		MakerFeeRate:      decimal.NewFromFloat(0.0002),
		TakerFeeRate:      decimal.NewFromFloat(0.0005),
	}
	sim := simbroker.NewSimBroker(brokerCfg)

	evalCfg := strategy.EvaluatorConfig{
		MaxDailyDrawdownPct: -0.05,
		MaxSlots:            3,
		EMAPeriod:           50,
		ATRPeriod:           14,
		BTCGateEnabled:      false,
		EntryOrderType:      "LIMIT",
		Sizing: strategy.SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.02),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.25),
			AccountLeverage:       8.0,
			SLMultiplier:          1.75,
			TPMultiplier:          3.00,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
		},
	}

	replayCfg := ReplayConfig{
		WarmupBars:   50,
		FundingMgr:   fm,
		SpreadPct:    0.0002,
		BenchmarkSym: "BTCUSDT",
	}

	engine := NewReplayEngine(replayCfg, evalCfg, sim, candleMap, filters)
	err := engine.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run failed: %v", err)
	}

	if engine.TotalSignalsEvaluated == 0 {
		t.Errorf("expected signals to be evaluated")
	}
}
