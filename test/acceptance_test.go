package test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker"
	"trml/internal/indicator"
	"trml/internal/model"
	"trml/internal/strategy"
)

// Acceptance Test Suite: Memverifikasi integrasi antar-modul bebas regresi
func TestFullPipelineIntegration(t *testing.T) {
	now := time.Now().UTC()

	// 1. Verifikasi Model & Kuantisasi
	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}

	qPrice, err := model.QuantizePrice(decimal.NewFromFloat(150.126), filter.TickSize, false)
	if err != nil || !qPrice.Equal(decimal.NewFromFloat(150.12)) {
		t.Fatalf("quantize price failed: %v, got %s", err, qPrice)
	}

	// 2. Verifikasi Indikator
	deltas := make([]float64, 51)
	for i := 0; i < 50; i++ {
		deltas[i] = 10.0 + float64(i%3)
	}
	deltas[50] = 30.0 // lonjakan volume delta
	zScore, zOk := indicator.CalculateBarDeltaZScore(deltas, 50)
	if !zOk || zScore <= 1.50 {
		t.Fatalf("expected Z-Score > 1.50, got %f", zScore)
	}

	// 3. Verifikasi Strategy Evaluator & Sizing (VWAP Pullback / Retest)
	closedCandles := make([]model.Candle, 201)
	for i := 0; i < 201; i++ {
		tOpen := now.Add(time.Duration(i-201) * 5 * time.Minute)
		closedCandles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tOpen.Add(5 * time.Minute),
			Open:            140.0 + float64(i)*0.1,
			High:            142.0 + float64(i)*0.1,
			Low:             139.0 + float64(i)*0.1,
			Close:           141.0 + float64(i)*0.1,
			VolumeBase:      100.0,
			QuoteVolumeUSDT: 15000.0,
		}
	}

	vwap := 160.0
	// Prior impulse at i=195 (High >= VWAP + 1.0*ATR)
	closedCandles[195].High = 165.0
	// Latest candle (i=200): Pullback to VWAP (Low <= VWAP + 0.35*ATR) and green bounce
	closedCandles[200].Open = 160.2
	closedCandles[200].Low = 159.9
	closedCandles[200].High = 161.2
	closedCandles[200].Close = 160.8

	snap := model.MarketSnapshot{
		Symbol:              "SOLUSDT",
		Filter:              filter,
		Now:                 now,
		BestBid:             160.80,
		BestAsk:             160.82,
		BookUpdatedAt:       now,
		MarkPrice:           160.81,
		MarkPriceUpdatedAt:  now,
		FundingRate:         0.0001,
		FundingUpdatedAt:    now,
		PrevOpenInterest:    1000.0,
		CurrOpenInterest:    1010.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
		CurrentSessionVWAP:  vwap,
		ClosedCandles:       closedCandles,
		ClosedBarDeltasUSDT: deltas,
	}

	stratCfg := strategy.EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        30 * time.Second,
		MaxSpreadPct:          0.03,
		MinNATRPct:            0.10,
		MaxNATRPct:            5.0,
		EMAPeriod:             200,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		Sizing: strategy.SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	intent, ok, reason, _ := strategy.EvaluateSignal(
		snap,
		nil,
		0,
		false,
		false,
		decimal.NewFromFloat(10000.0),
		decimal.NewFromFloat(10000.0),
		stratCfg,
	)
	if !ok || reason != "SIGNAL_APPROVED" {
		t.Fatalf("expected signal approved, got ok=%v reason=%s", ok, reason)
	}

	// 4. Verifikasi Live Broker Two-Phase Execution
	liveBroker := broker.NewBinanceLiveBroker(broker.LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		MaxAccountStaleness: 60 * time.Second,
		LimitOrderTimeout:   45 * time.Second,
	})

	liveBroker.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	var entryDispatched int32
	liveBroker.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		atomic.AddInt32(&entryDispatched, 1)
		return 888777, nil
	}

	var slDispatched int32
	liveBroker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			atomic.AddInt32(&slDispatched, 1)
			return 666111, nil
		}
		return 666222, nil
	}

	reserved, err := liveBroker.SubmitLimitEntry(intent)
	if !reserved || err != nil {
		t.Fatalf("expected order submitted cleanly: %v", err)
	}
	if atomic.LoadInt32(&entryDispatched) != 1 {
		t.Errorf("expected 1 entry dispatched")
	}

	// Simulasi fill
	liveBroker.HandleUserDataOrderUpdate(intent.ClientOrderID, 888777, "SOLUSDT", model.StatusFilled, intent.Qty, decimal.NewFromFloat(150.0), decimal.Zero)
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&slDispatched) != 1 {
		t.Errorf("expected 1 SL algo order placed after fill")
	}
}
