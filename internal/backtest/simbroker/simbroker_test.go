package simbroker

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/model"
)

func TestSimBroker_LimitPenetrationAndFill(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		PenetrationTicks:  1,
		SignalDelayBars:   1,
		LimitTimeoutBars:  2,
	}
	broker := NewSimBroker(cfg)
	broker.SetExchangeFilter("BTCUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.10),
		StepSize: decimal.NewFromFloat(0.001),
	})

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	intent := model.OrderIntent{
		ClientOrderID: "TEST_BUY_1",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(0.1),
		LimitPrice:    decimal.NewFromFloat(40000.0),
		SLPrice:       decimal.NewFromFloat(39500.0),
		TPPrice:       decimal.NewFromFloat(41000.0),
		TP1Price:      decimal.NewFromFloat(40500.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed submit limit: %v", err)
	}

	// Candle 1 (t=10:05): Low hits 40000.0 exactly (Touch only, NO penetration)
	// Must NOT fill because PenetrationTicks = 1 requires Low <= 40000.0 - 0.10 = 39999.90
	c1 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10*time.Minute - time.Millisecond),
		Symbol:    "BTCUSDT",
		Open:      40100.0,
		High:      40150.0,
		Low:       40000.0, // Touch only
		Close:     40050.0,
	}
	broker.ProcessCandle(c1, 1)

	if broker.HasActiveOrderOrPosition("BTCUSDT") == false {
		t.Fatalf("expected order or position active")
	}
	broker.Mu.Lock()
	_, hasPos := broker.OpenPositions["BTCUSDT"]
	broker.Mu.Unlock()
	if hasPos {
		t.Fatalf("order should not have filled on mere price touch")
	}

	// Candle 2 (t=10:10): Low penetrates to 39999.0 (below 39999.90)
	// MUST fill!
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(10 * time.Minute),
		CloseTime: now.Add(15*time.Minute - time.Millisecond),
		Symbol:    "BTCUSDT",
		Open:      40050.0,
		High:      40080.0,
		Low:       39999.0, // Penetration!
		Close:     40020.0,
	}
	broker.ProcessCandle(c2, 2)

	broker.Mu.Lock()
	pos, hasPos := broker.OpenPositions["BTCUSDT"]
	broker.Mu.Unlock()
	if !hasPos || pos == nil {
		t.Fatalf("order should have filled upon 1-tick penetration")
	}
	if !pos.EntryPrice.Equal(decimal.NewFromFloat(40000.0)) {
		t.Errorf("expected entry price 40000.0, got %s", pos.EntryPrice)
	}
}

func TestSimBroker_PessimisticIntrabarConflictSLFirst(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
		IntrabarConflictSLFirst: true,
	}
	broker := NewSimBroker(cfg)
	broker.SetExchangeFilter("ETHUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01),
		StepSize: decimal.NewFromFloat(0.01),
	})

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Manually open position: Long at 2000, SL at 1900, TP at 2200
	broker.Mu.Lock()
	broker.OpenPositions["ETHUSDT"] = &SimPosition{
		Symbol:         "ETHUSDT",
		Side:           model.SideLong,
		Qty:            decimal.NewFromFloat(1.0),
		InitialQty:     decimal.NewFromFloat(1.0),
		EntryPrice:     decimal.NewFromFloat(2000.0),
		CurrentSLPrice: decimal.NewFromFloat(1900.0),
		InitialSLPrice: decimal.NewFromFloat(1900.0),
		TPPrice:        decimal.NewFromFloat(2200.0),
		TP1Price:       decimal.NewFromFloat(2100.0),
		PeakPrice:      decimal.NewFromFloat(2000.0),
		OpenTime:       now,
	}
	broker.Mu.Unlock()

	// Wild candle: Low is 1850 (touches SL), High is 2250 (touches TP)
	// Pesimistis: SL harus kena duluan!
	cWild := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10*time.Minute - time.Millisecond),
		Symbol:    "ETHUSDT",
		Open:      2000.0,
		High:      2250.0, // touches TP
		Low:       1850.0, // touches SL
		Close:     2150.0,
	}
	broker.ProcessCandle(cWild, 1)

	broker.Mu.Lock()
	trades := broker.ClosedTrades
	broker.Mu.Unlock()

	if len(trades) != 1 {
		t.Fatalf("expected 1 closed trade, got %d", len(trades))
	}
	if trades[0].ExitReason != "STOP_LOSS" {
		t.Errorf("expected exit reason 'STOP_LOSS' due to pessimistic rule, got '%s'", trades[0].ExitReason)
	}
}

func TestSimBroker_PartialTP1AndTrailing(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:   decimal.NewFromFloat(1000.0),
		PartialTPEnabled:    true,
		PartialTP1Ratio:     0.50,
		TrailingEnabled:     true,
		TrailingActivateATR: 1.0,
		TrailingDistanceATR: 0.5,
	}
	broker := NewSimBroker(cfg)
	broker.SetExchangeFilter("SOLUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01),
		StepSize: decimal.NewFromFloat(0.1),
	})

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Long SOL at 100, SL at 95, TP1 at 105, TP at 115, ATR at Entry = 5.0
	broker.Mu.Lock()
	broker.OpenPositions["SOLUSDT"] = &SimPosition{
		Symbol:         "SOLUSDT",
		Side:           model.SideLong,
		Qty:            decimal.NewFromFloat(10.0),
		InitialQty:     decimal.NewFromFloat(10.0),
		EntryPrice:     decimal.NewFromFloat(100.0),
		CurrentSLPrice: decimal.NewFromFloat(95.0),
		InitialSLPrice: decimal.NewFromFloat(95.0),
		TPPrice:        decimal.NewFromFloat(115.0),
		TP1Price:       decimal.NewFromFloat(105.0),
		ATRAtEntry:     decimal.NewFromFloat(5.0),
		PeakPrice:      decimal.NewFromFloat(100.0),
		OpenTime:       now,
	}
	broker.Mu.Unlock()

	// Bar 1: Price touches 106.0 -> Hits TP1!
	c1 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      101.0,
		High:      106.0,
		Low:       100.5,
		Close:     105.5,
	}
	broker.ProcessCandle(c1, 1)

	broker.Mu.Lock()
	pos := broker.OpenPositions["SOLUSDT"]
	trades := broker.ClosedTrades
	broker.Mu.Unlock()

	if len(trades) != 1 || trades[0].ExitReason != "PARTIAL_TAKE_PROFIT" {
		t.Fatalf("expected 1 PARTIAL_TAKE_PROFIT trade, got %+v", trades)
	}
	if !pos.Qty.Equal(decimal.NewFromFloat(5.0)) {
		t.Errorf("expected 5.0 remaining lot, got %s", pos.Qty)
	}
	// SL should be moved to BE (> 100.0)
	if !pos.CurrentSLPrice.GreaterThanOrEqual(decimal.NewFromFloat(100.0)) {
		t.Errorf("expected SL at least BE (>= 100.0), got %s", pos.CurrentSLPrice)
	}

	// Bar 2: Price pushes to 110.0 -> Trailing Stop ratchets!
	// Peak = 110.0, Peak - Entry = 10.0 >= 1.0 ATR (5.0).
	// New SL = Peak (110.0) - 0.5 ATR (2.5) = 107.50!
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(10 * time.Minute),
		CloseTime: now.Add(15*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      105.5,
		High:      110.0,
		Low:       105.0,
		Close:     109.0,
	}
	broker.ProcessCandle(c2, 2)

	broker.Mu.Lock()
	pos = broker.OpenPositions["SOLUSDT"]
	broker.Mu.Unlock()

	if !pos.CurrentSLPrice.Equal(decimal.NewFromFloat(107.50)) {
		t.Errorf("expected ratcheted trailing SL at 107.50, got %s", pos.CurrentSLPrice)
	}
}
