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

func TestReplayEngine_EndToEndSyntheticSimulation(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	step := 5 * time.Minute

	// Buat 250 candle sintetis untuk BTCUSDT
	// Bar 0..200: Base level ~40,000 (Warmup)
	// Bar 201..210: Strong Impulse Up (40,000 -> 42,000)
	// Bar 211..220: Gentle Pullback to VWAP (42,000 -> 41,000) with High Delta Z-score
	// Bar 221..240: Bounce & Trend Continuation (41,000 -> 43,000)
	candles := make([]data.HistoricalCandle, 250)
	currentPrice := 40000.0

	for i := 0; i < 250; i++ {
		curTime := t0.Add(time.Duration(i) * step)

		if i > 200 && i <= 210 {
			currentPrice += 200.0 // Impulse up
		} else if i > 210 && i <= 218 {
			currentPrice -= 100.0 // Pullback to session VWAP
		} else if i > 218 {
			currentPrice += 150.0 // Bounce continuation
		}

		vol := 100.0
		quoteVol := currentPrice * vol
		takerBuyQuote := quoteVol * 0.50 // neutral delta

		if i == 218 {
			// Sinyal Bar: Orderflow Delta Lonjak tinggi (Buyer Maker / Taker Buy Kuat)
			takerBuyQuote = quoteVol * 0.90
		}

		barDelta := 2.0*takerBuyQuote - quoteVol

		candles[i] = data.HistoricalCandle{
			Symbol:              "BTCUSDT",
			OpenTime:            curTime,
			CloseTime:           curTime.Add(step - time.Millisecond),
			Open:                currentPrice - 20.0,
			High:                currentPrice + 50.0,
			Low:                 currentPrice - 50.0,
			Close:               currentPrice,
			Volume:              vol,
			QuoteVolume:         quoteVol,
			TradeCount:          1000,
			TakerBuyBaseVolume:  vol * 0.5,
			TakerBuyQuoteVolume: takerBuyQuote,
			BarDeltaUSDT:        barDelta,
		}
	}

	candleMap := map[string][]data.HistoricalCandle{
		"BTCUSDT": candles,
	}

	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.10),
		StepSize:    decimal.NewFromFloat(0.001),
		MinNotional: decimal.NewFromFloat(5.0),
		MinQty:      decimal.NewFromFloat(0.001),
	}
	filters := map[string]model.ExchangeFilter{
		"BTCUSDT": filter,
	}

	sim := simbroker.NewSimBroker(simbroker.SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
		MaxSlots:                3,
		MaxDailyDrawdownPct:     -0.05,
		AccountLeverage:         8.0,
		SignalDelayBars:         1,
		LimitTimeoutBars:        2,
		PenetrationTicks:        1,
		IntrabarConflictSLFirst: true,
		PartialTPEnabled:        true,
		PartialTP1Ratio:         0.50,
		PartialTP1ATRMult:       1.00,
		TrailingEnabled:         true,
		TrailingActivateATR:     1.10,
		TrailingDistanceATR:     0.55,
	})

	evalCfg := strategy.EvaluatorConfig{
		MaxSlots:                 3,
		MaxSpreadPct:             0.0010,
		MinNATRPct:               0.0010, // longgar untuk data sintetis
		MaxNATRPct:               0.0500,
		EMAPeriod:                200,
		ATRPeriod:                14,
		DeltaZScoreWindow:        30,
		MinBarDeltaZScore:        0.50,
		MinBounceZScore:          0.50,
		PullbackToleranceATR:     0.80,
		MinEMA200DistanceATR:     0.10,
		MaxEMA200DistanceATR:     5.00,
		LookbackImpulseBars:      18,
		RetestLookbackBars:       6,
		MinImpulseATR:            0.50,
		MaxEntryDistanceATR:      1.50,
		EntryOrderType:           "LIMIT",
		BTCGateEnabled:           false, // nonaktifkan untuk single-pair synthetic
		AllowedSide:              "BOTH",
		MaxFrenzyRVOL:            5.0,
		MaxCounterImpulseATR:     1.50,
		MinSignalRVOL:            0.20,
		CooldownAfterExitMinutes: 10,
		VWAPSessionWarmupMinutes: 30,
		Sizing: strategy.SizingConfig{
			RiskPerTradePct:                decimal.NewFromFloat(0.02),
			MaxSlotMarginFraction:          decimal.NewFromFloat(0.25),
			AvailableBalanceBufferFraction: decimal.NewFromFloat(0.10),
			AccountLeverage:                8.0,
			SLMultiplier:                   1.75,
			TPMultiplier:                   3.00,
			TP1Multiplier:                  1.00,
			MinSLDistancePct:               decimal.NewFromFloat(0.010),
			MakerFeeRate:                   decimal.NewFromFloat(0.0002),
			TakerFeeRate:                   decimal.NewFromFloat(0.0005),
			SlippageRate:                   decimal.NewFromFloat(0.0005),
			EntryOrderType:                 "LIMIT",
		},
	}

	replayCfg := ReplayConfig{
		WarmupBars:   200,
		SpreadPct:    0.0002,
		FundingRate:  0.0001,
		BenchmarkSym: "BTCUSDT",
	}

	engine := NewReplayEngine(replayCfg, evalCfg, sim, candleMap, filters)

	err := engine.Run(context.Background())
	if err != nil {
		t.Fatalf("replay engine run failed: %v", err)
	}

	if engine.TotalSignalsEvaluated == 0 {
		t.Errorf("expected signals to be evaluated")
	}

	t.Logf("Replay completed: %d signals evaluated, %d approved, %d rejected, %d closed trades",
		engine.TotalSignalsEvaluated, engine.SignalsApproved, engine.SignalsRejected, len(sim.ClosedTrades))
}

