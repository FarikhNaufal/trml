package simbroker

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/broker/protection"
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

	// Candle 1 (t=10:00..10:05): Low hits 40000.0 exactly (Touch only, NO penetration)
	// Must NOT fill because PenetrationTicks = 1 requires Low <= 40000.0 - 0.10 = 39999.90
	c1 := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5*time.Minute - time.Millisecond),
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

	// Candle 2 (t=10:05..10:10): Low penetrates to 39999.0 (below 39999.90)
	// MUST fill!
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10*time.Minute - time.Millisecond),
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

func TestSimBroker_Tier2ProfitLock(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:        decimal.NewFromFloat(1000.0),
		MakerFeeRate:             decimal.NewFromFloat(0.0002),
		TakerFeeRate:             decimal.NewFromFloat(0.0005),
		SlippageRate:             decimal.NewFromFloat(0.0002),
		SignalDelayBars:          1,
		LimitTimeoutBars:         1,
		PenetrationTicks:         1,
		PartialTPEnabled:         false, // runner lock tanpa TP1
		Tier2ProfitLockEnabled:   true,
		Tier2ProfitLockATR:       1.80,
		Tier2ProfitLockOffsetATR: 0.90,
		TrailingEnabled:          false,
	}

	broker := NewSimBroker(cfg)
	broker.ExchangeFilters["SOLUSDT"] = model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
		MinQty:      decimal.NewFromFloat(0.1),
	}

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "SOL_TEST_T2",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		ATRAtEntry:    decimal.NewFromFloat(5.0), // 1 ATR = 5.0 -> 1.8 ATR = 9.0 (target 109.0)
		CreatedAt:     now,
	}
	broker.SubmitLimitEntry(intent)

	// Bar 0: Fill order
	c0 := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      100.2,
		High:      100.3,
		Low:       99.98,
		Close:     100.0,
	}
	broker.ProcessCandle(c0, 0)

	// Bar 1: Price climbs to 109.5 (current >= 100 + 1.8 * 5 = 109.0). Tier 2 activates!
	// Target SL = 100 + 0.9 * 5 = 104.50
	c1 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      100.0,
		High:      109.5,
		Low:       100.0,
		Close:     109.2,
	}
	broker.ProcessCandle(c1, 1)

	broker.Mu.Lock()
	pos := broker.OpenPositions["SOLUSDT"]
	broker.Mu.Unlock()

	if pos == nil {
		t.Fatalf("expected open position, got nil")
	}
	if pos.ProtectionTier != 2 {
		t.Errorf("expected ProtectionTier = 2, got %d", pos.ProtectionTier)
	}
	if !pos.CurrentSLPrice.Equal(decimal.NewFromFloat(104.50)) {
		t.Errorf("expected SL at 104.50, got %s", pos.CurrentSLPrice)
	}

	// Bar 2: Price drops to 104.0, hitting the Tier 2 locked SL
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(10 * time.Minute),
		CloseTime: now.Add(15*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      109.0,
		High:      109.0,
		Low:       104.0,
		Close:     104.2,
	}
	broker.ProcessCandle(c2, 2)

	broker.Mu.Lock()
	closed := broker.ClosedTrades
	broker.Mu.Unlock()

	if len(closed) != 1 {
		t.Fatalf("expected 1 closed trade, got %d", len(closed))
	}
	if closed[0].ExitReason != "PROFIT_LOCK" {
		t.Errorf("expected exit reason PROFIT_LOCK, got %s", closed[0].ExitReason)
	}
	if closed[0].NetPnL.LessThanOrEqual(decimal.Zero) {
		t.Errorf("expected positive NetPnL for profit lock, got %s", closed[0].NetPnL)
	}
}

func TestSimBroker_FundingSettlement(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		MakerFeeRate:      decimal.NewFromFloat(0.0002),
		TakerFeeRate:      decimal.NewFromFloat(0.0005),
	}
	broker := NewSimBroker(cfg)
	broker.SetExchangeFilter("SOLUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01),
		StepSize: decimal.NewFromFloat(0.1),
	})

	now := time.Date(2026, 1, 1, 7, 55, 0, 0, time.UTC)

	// Submit Long: 10 SOL at 100.0 (Notional $1000)
	intent := model.OrderIntent{
		Symbol:     "SOLUSDT",
		Side:       model.SideLong,
		Qty:        decimal.NewFromFloat(10.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(95.0),
		TPPrice:    decimal.NewFromFloat(110.0),
		CreatedAt:  now,
	}
	_, _ = broker.SubmitLimitEntry(intent)

	// Fill order at 100.0 (Low penetrates to 99.5)
	c1 := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      100.5,
		High:      100.5,
		Low:       99.5,
		Close:     100.0,
	}
	broker.ProcessCandle(c1, 1)

	// Initial Equity: 1000 - EntryFee (1000 * 0.0002 = 0.20) = 999.80
	if !broker.RealizedEquityUSDT.Equal(decimal.NewFromFloat(999.80)) {
		t.Fatalf("expected realized equity 999.80, got %s", broker.RealizedEquityUSDT)
	}

	// At 08:00 UTC, funding rate = 0.0001 (0.01% positive)
	// Long pays 1000 * 0.0001 = $0.10
	broker.ApplyFundingSettlement("SOLUSDT", decimal.NewFromFloat(100.0), decimal.NewFromFloat(0.0001))

	// Equity should now be 999.80 - 0.10 = 999.70
	if !broker.RealizedEquityUSDT.Equal(decimal.NewFromFloat(999.70)) {
		t.Fatalf("expected realized equity 999.70 after funding, got %s", broker.RealizedEquityUSDT)
	}

	// Exit at TP 110.0
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(10 * time.Minute),
		CloseTime: now.Add(15*time.Minute - time.Millisecond),
		Symbol:    "SOLUSDT",
		Open:      105.0,
		High:      111.0,
		Low:       104.0,
		Close:     110.0,
	}
	broker.ProcessCandle(c2, 2)

	broker.Mu.Lock()
	trades := broker.ClosedTrades
	broker.Mu.Unlock()

	if len(trades) != 1 {
		t.Fatalf("expected 1 closed trade, got %d", len(trades))
	}
	tr := trades[0]

	// Gross PnL: (110 - 100) * 10 = +$100.00
	// Exit fee: 1100 * 0.0005 = $0.55
	// Entry fee: $0.20
	// Total fee: $0.75
	// Funding paid: $0.10
	// Net PnL: 100 - 0.75 - 0.10 = $99.15
	if !tr.FundingTotal.Equal(decimal.NewFromFloat(0.10)) {
		t.Errorf("expected trade funding total $0.10, got %s", tr.FundingTotal)
	}
	if !tr.NetPnL.Equal(decimal.NewFromFloat(99.15)) {
		t.Errorf("expected trade net PnL $99.15, got %s", tr.NetPnL)
	}

	// Final Realized Equity: 1000 + 99.15 = 1099.15
	if !broker.RealizedEquityUSDT.Equal(decimal.NewFromFloat(1099.15)) {
		t.Errorf("expected final realized equity 1099.15, got %s", broker.RealizedEquityUSDT)
	}
	if broker.Stats.FundingSettlements != 1 {
		t.Errorf("expected FundingSettlements=1, got %d", broker.Stats.FundingSettlements)
	}
}

func TestSimBroker_CircuitBreakerDrawdownParity(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:     decimal.NewFromFloat(1000.0),
		EquityModeCompounding: false,
		MaxDailyDrawdownPct:   -0.05, // -5% breaker
		AccountLeverage:       5.0,
		MaxSlots:              1,
		PenetrationTicks:      1,
		SignalDelayBars:       0,
	}
	broker := NewSimBroker(cfg)
	broker.SetExchangeFilter("BTCUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(1.0),
		StepSize: decimal.NewFromFloat(0.01),
	})

	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)

	// Inisialisasi awal: Breaker tidak aktif
	if broker.IsCircuitBreakerTripped() {
		t.Fatalf("expected circuit breaker not tripped initially")
	}

	// Submit posisi LONG BTCUSDT: Qty = 0.1, Entry = 10000
	intent := model.OrderIntent{
		ClientOrderID: "ORD1",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(0.1),
		LimitPrice:    decimal.NewFromFloat(10000.0),
		SLPrice:       decimal.NewFromFloat(9000.0),
		TPPrice:       decimal.NewFromFloat(12000.0),
		TP1Price:      decimal.NewFromFloat(11000.0),
		CreatedAt:     now,
	}
	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed to submit limit entry: %v", err)
	}

	// Candle 1: Terisi di 10000 (Low menembus ke 9990)
	c1 := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      10000.0,
		High:      10050.0,
		Low:       9990.0,
		Close:     10000.0,
	}
	broker.ProcessCandle(c1, 1)

	if !broker.HasActiveOrderOrPosition("BTCUSDT") {
		t.Fatalf("expected active position for BTCUSDT")
	}

	// Floating loss belum mencapai 5%: Close = 9700 -> Unrealized = (9700 - 10000)*0.1 = -30 USDT (-3%)
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      10000.0,
		High:      10000.0,
		Low:       9650.0,
		Close:     9700.0,
	}
	broker.ProcessCandle(c2, 2)

	if broker.IsCircuitBreakerTripped() {
		t.Fatalf("expected breaker NOT tripped at -3%% floating drawdown")
	}

	// Floating loss menembus 5%: Close = 9400 -> Unrealized = (9400 - 10000)*0.1 = -60 USDT (-6%)
	c3 := data.HistoricalCandle{
		OpenTime:  now.Add(10 * time.Minute),
		CloseTime: now.Add(15 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      9700.0,
		High:      9700.0,
		Low:       9350.0,
		Close:     9400.0,
	}
	broker.ProcessCandle(c3, 3)

	if !broker.IsCircuitBreakerTripped() {
		t.Fatalf("expected breaker TRIPPED at -6%% floating drawdown")
	}

	// Pastikan order baru ditolak saat circuit breaker aktif
	intent2 := model.OrderIntent{
		ClientOrderID: "ORD2",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		CreatedAt:     now.Add(15 * time.Minute),
	}
	ok2, err2 := broker.SubmitLimitEntry(intent2)
	if ok2 || err2 == nil {
		t.Fatalf("expected order submission to be rejected when circuit breaker is active")
	}

	// Rollover hari berikutnya (00:00 UTC): Breaker harus reset
	nextDay := now.Add(24 * time.Hour)
	cNextDay := data.HistoricalCandle{
		OpenTime:  nextDay,
		CloseTime: nextDay.Add(5 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      9400.0,
		High:      9500.0,
		Low:       9390.0,
		Close:     9450.0,
	}
	broker.ProcessCandle(cNextDay, 100)

	if broker.IsCircuitBreakerTripped() {
		t.Fatalf("expected breaker to reset on new UTC day rollover")
	}
}

func TestSimBroker_GapOpenStopLoss(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		MakerFeeRate:      decimal.Zero,
		TakerFeeRate:      decimal.Zero,
		SlippageRate:      decimal.Zero,
		SignalDelayBars:   1,
		LimitTimeoutBars:  1,
		PenetrationTicks:  1,
		SLGapFillEnabled:  true,
	}
	broker := NewSimBroker(cfg)
	broker.ExchangeFilters["BTCUSDT"] = model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.1),
		StepSize: decimal.NewFromFloat(0.001),
	}

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "GAP_TEST",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		CreatedAt:     now,
	}
	broker.SubmitLimitEntry(intent)

	// Bar 0: Fill order at 100.0
	c0 := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      100.0,
		High:      100.5,
		Low:       99.0, // Tembus 1 tick
		Close:     99.5,
	}
	broker.ProcessCandle(c0, 0)

	// Bar 1: Flash crash gap down. Candle opens at 90.0 (below SL 95.0)!
	c1 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      90.0, // Gap down melompati SL 95.0
		High:      91.0,
		Low:       88.0,
		Close:     89.0,
	}
	broker.ProcessCandle(c1, 1)

	if len(broker.ClosedTrades) != 1 {
		t.Fatalf("expected 1 closed trade, got %d", len(broker.ClosedTrades))
	}
	tr := broker.ClosedTrades[0]
	// Exit price harus terisi pada Open bar yang gap (90.0) dengan penalti slippage, bukan 95.0
	expectedExit := decimal.NewFromFloat(90.0).Mul(decimal.NewFromFloat(1.0).Sub(broker.Cfg.SlippageRate))
	if !tr.ExitPrice.Equal(expectedExit) {
		t.Fatalf("expected gap-down fill with slippage %s, got %s", expectedExit, tr.ExitPrice)
	}
}

func TestSimBroker_MultiLegFundingAccounting(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		MakerFeeRate:      decimal.Zero,
		TakerFeeRate:      decimal.Zero,
		SlippageRate:      decimal.Zero,
		PartialTPEnabled:  true,
		PartialTP1Ratio:   0.50,
		PartialTP1ATRMult: 1.0,
	}
	broker := NewSimBroker(cfg)
	broker.ExchangeFilters["BTCUSDT"] = model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.1),
		StepSize: decimal.NewFromFloat(0.001),
	}

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "FUNDING_TEST",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0), // 2.0 BTC
		SLPrice:       decimal.NewFromFloat(90.0),
		TPPrice:       decimal.NewFromFloat(130.0),
		TP1Price:      decimal.NewFromFloat(110.0),
		ATRAtEntry:    decimal.NewFromFloat(10.0),
		CreatedAt:     now,
	}
	broker.SubmitLimitEntry(intent)

	// Fill order at 100.0
	c0 := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      100.0,
		High:      100.5,
		Low:       99.0,
		Close:     100.0,
	}
	broker.ProcessCandle(c0, 0)

	// Simulasikan funding payment pra-TP1 ($10.0 funding fee dibayar)
	pos := broker.OpenPositions["BTCUSDT"]
	pos.FundingPaid = decimal.NewFromFloat(10.0)

	// Bar 1: Sentuh TP1 (110.0), scale out 50% = 1.0 BTC
	c1 := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      105.0,
		High:      111.0,
		Low:       104.0,
		Close:     110.0,
	}
	broker.ProcessCandle(c1, 1)

	if len(broker.ClosedTrades) != 1 {
		t.Fatalf("expected 1 trade for TP1 leg, got %d", len(broker.ClosedTrades))
	}
	tp1Trade := broker.ClosedTrades[0]
	// Separuh funding ($5.0) harus diatribusikan ke TP1
	if !tp1Trade.FundingTotal.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected TP1 funding fee 5.0, got %s", tp1Trade.FundingTotal)
	}

	// Sisa funding di posisi harus tinggal $5.0 dan InitialQty tetap utuh (2.0), sementara pos.Qty sisa 1.0
	if !pos.FundingPaid.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected remaining FundingPaid 5.0, got %s", pos.FundingPaid)
	}
	if !pos.Qty.Equal(decimal.NewFromFloat(1.0)) {
		t.Fatalf("expected remaining Qty 1.0, got %s", pos.Qty)
	}
	if !pos.InitialQty.Equal(decimal.NewFromFloat(2.0)) {
		t.Fatalf("expected immutable InitialQty 2.0, got %s", pos.InitialQty)
	}
	if !tp1Trade.InitialQty.Equal(decimal.NewFromFloat(2.0)) {
		t.Fatalf("expected TP1 trade record InitialQty 2.0, got %s", tp1Trade.InitialQty)
	}

	// Bar 2: Runner kena full TP at 130.0
	c2 := data.HistoricalCandle{
		OpenTime:  now.Add(10 * time.Minute),
		CloseTime: now.Add(15 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      115.0,
		High:      131.0,
		Low:       114.0,
		Close:     130.0,
	}
	broker.ProcessCandle(c2, 2)

	if len(broker.ClosedTrades) != 2 {
		t.Fatalf("expected 2 closed trades total, got %d", len(broker.ClosedTrades))
	}
	runnerTrade := broker.ClosedTrades[1]
	// Sisa funding ($5.0) harus diatribusikan 100% ke runner trade
	if !runnerTrade.FundingTotal.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected runner funding fee 5.0, got %s", runnerTrade.FundingTotal)
	}
}

func TestSimBroker_PartialTP1_UnsplittableMovesToBERunner(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(100.0),
		MaxSlots:                3,
		MaxDailyDrawdownPct:     -0.05,
		AccountLeverage:         8.0,
		MakerFeeRate:            decimal.NewFromFloat(0.0002),
		TakerFeeRate:            decimal.NewFromFloat(0.0005),
		SlippageRate:            decimal.NewFromFloat(0.0002),
		SignalDelayBars:         1,
		LimitTimeoutBars:        1,
		PenetrationTicks:        1,
		IntrabarConflictSLFirst: true,
		PartialTPEnabled:        true,
		PartialTP1Ratio:         0.35,
		TrailingEnabled:         true,
		TrailingActivateATR:     1.50,
		TrailingDistanceATR:     0.35,
	}
	b := NewSimBroker(cfg)
	b.SetExchangeFilter("ADAUSDT", model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.001),
		StepSize:    decimal.NewFromFloat(1.0),
		MinQty:      decimal.NewFromFloat(1.0),
		MinNotional: decimal.NewFromFloat(5.0),
	})

	now := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	// Total notional ~8 USDT -> 35% = 2.8 USDT < MinNotional 5 USDT (unsplittable)
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "SMALL_TP1",
		Symbol:        "ADAUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(8.0),
		LimitPrice:    decimal.NewFromFloat(1.000),
		SLPrice:       decimal.NewFromFloat(1.020),
		TPPrice:       decimal.NewFromFloat(0.950),
		TP1Price:      decimal.NewFromFloat(0.985),
		ATRAtEntry:    decimal.NewFromFloat(0.012),
		CreatedAt:     now,
	})
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now, CloseTime: now.Add(5 * time.Minute),
		Symbol: "ADAUSDT", Open: 1.000, High: 1.005, Low: 0.995, Close: 0.998,
	}, 0)
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now.Add(5 * time.Minute), CloseTime: now.Add(10 * time.Minute),
		Symbol: "ADAUSDT", Open: 0.998, High: 1.000, Low: 0.984, Close: 0.984,
	}, 1)

	pos := b.OpenPositions["ADAUSDT"]
	if pos == nil || !pos.IsTP1Hit || !pos.Qty.Equal(decimal.NewFromFloat(8.0)) {
		t.Fatalf("expected unsplittable position to keep full qty 8.0 and mark IsTP1Hit=true, got %+v", pos)
	}
	if !pos.CurrentSLPrice.LessThan(pos.EntryPrice) || pos.ProtectionTier != 1 {
		t.Fatalf("expected SL moved to BE (< 1.000) and ProtectionTier=1, got SL=%s tier=%d", pos.CurrentSLPrice, pos.ProtectionTier)
	}
	if len(b.ClosedTrades) != 0 {
		t.Fatalf("expected 0 partial closed trades when below MinNotional, got %d", len(b.ClosedTrades))
	}
}

func TestSimBroker_MarketEntryUsesTakerFeeAndOpenSlippage(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		EntryOrderType:    "MARKET",
		MakerFeeRate:      decimal.NewFromFloat(0.0002),
		TakerFeeRate:      decimal.NewFromFloat(0.0005),
		SlippageRate:      decimal.NewFromFloat(0.0010), // 10 bps
	}
	b := NewSimBroker(cfg)
	b.SetExchangeFilter("ETHUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.1),
		StepSize: decimal.NewFromFloat(0.01),
	})

	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	ok, reason := b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "MKT_1",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(90.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		CreatedAt:     now,
	})
	if !ok {
		t.Fatalf("expected submit ok, got %s", reason)
	}

	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now, CloseTime: now.Add(5 * time.Minute),
		Symbol: "ETHUSDT", Open: 100.0, High: 101.0, Low: 99.5, Close: 100.5,
	}, 0)

	pos := b.OpenPositions["ETHUSDT"]
	if pos == nil {
		t.Fatalf("expected open position")
	}
	expectedEntry := decimal.NewFromFloat(100.1) // 100.0 * (1 + 0.001)
	if !pos.EntryPrice.Equal(expectedEntry) {
		t.Fatalf("expected market fill at %s, got %s", expectedEntry, pos.EntryPrice)
	}
	if pos.EntryWasMaker {
		t.Fatalf("expected EntryWasMaker=false for MARKET order")
	}
	expectedFee := expectedEntry.Mul(cfg.TakerFeeRate)
	if !pos.EntryFee.Equal(expectedFee) {
		t.Fatalf("expected taker entry fee %s, got %s", expectedFee, pos.EntryFee)
	}
	if b.Stats.MarketFilled != 1 || b.Stats.Filled != 1 {
		t.Fatalf("expected MarketFilled=1 Filled=1, got %+v", b.Stats)
	}
}

func TestSimBroker_GTXRepegAndDriftAbort(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:     decimal.NewFromFloat(1000.0),
		MakerFeeRate:          decimal.NewFromFloat(0.0002),
		TakerFeeRate:          decimal.NewFromFloat(0.0005),
		SlippageRate:          decimal.Zero,
		PenetrationTicks:      1,
		MaxEntryRepegAttempts: 2,
		MaxEntryRepegDriftPct: 0.01, // 1%
		LimitOrderTimeout:     15 * time.Minute,
	}
	b := NewSimBroker(cfg)
	b.SetExchangeFilter("SOLUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01),
		StepSize: decimal.NewFromFloat(0.1),
	})

	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	// Case 1: LONG limit 100.00, bar gaps DOWN to 99.50 (-0.5% <= 1% drift -> crosses spread!)
	// Repeg shifts bid down to 99.49; Low drops to 99.40 -> fills at 99.49!
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "REPEG_LONG_CROSS",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     now,
	})
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now, CloseTime: now.Add(5 * time.Minute),
		Symbol: "SOLUSDT", Open: 99.50, High: 99.90, Low: 99.40, Close: 99.70,
	}, 0)

	if b.Stats.Repegged != 1 {
		t.Fatalf("expected Repegged=1, got %+v", b.Stats)
	}
	pos := b.OpenPositions["SOLUSDT"]
	if pos == nil {
		t.Fatalf("expected repegged LONG order to fill on bar 0")
	}
	if !pos.EntryPrice.Equal(decimal.NewFromFloat(99.49)) {
		t.Fatalf("expected repegged limit 99.49, got %s", pos.EntryPrice)
	}

	// Case 2: LONG limit 100.00, bar gaps DOWN deeply to 97.00 (-3% > 1% drift) -> RepegAborted
	b.ForceClosePosition("SOLUSDT", decimal.NewFromFloat(99.50), "CLEANUP", now.Add(5*time.Minute), 1)
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "REPEG_ABORT",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     now.Add(5 * time.Minute),
	})
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now.Add(5 * time.Minute), CloseTime: now.Add(10 * time.Minute),
		Symbol: "SOLUSDT", Open: 97.00, High: 97.50, Low: 96.50, Close: 97.00,
	}, 1)

	if b.Stats.RepegAborted != 1 {
		t.Fatalf("expected RepegAborted=1, got %+v", b.Stats)
	}
	if _, stillPending := b.PendingOrders["SOLUSDT"]; stillPending {
		t.Fatalf("expected pending order cancelled on drift abort")
	}

	// Case 3: LONG limit 100.00, bar opens HIGHER at 103.00 -> passive order (no cross), remains resting until pullback!
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "PASSIVE_RESTING",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     now.Add(10 * time.Minute),
	})
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now.Add(10 * time.Minute), CloseTime: now.Add(15 * time.Minute),
		Symbol: "SOLUSDT", Open: 103.00, High: 103.50, Low: 100.50, Close: 102.00,
	}, 2)
	if b.Stats.RepegAborted != 1 {
		t.Fatalf("expected passive LONG order NOT aborted when open moves higher, got RepegAborted=%d", b.Stats.RepegAborted)
	}
	if _, stillPending := b.PendingOrders["SOLUSDT"]; !stillPending {
		t.Fatalf("expected passive LONG order to keep resting until timeout or pullback")
	}

	// Case 4: SHORT limit 100.00, bar opens HIGHER at 100.50 (+0.5% <= 1% -> crosses bid!) -> repegs UP to 100.51 and fills
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "REPEG_SHORT_CROSS",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0),
		SLPrice:       decimal.NewFromFloat(105.0),
		TPPrice:       decimal.NewFromFloat(90.0),
		CreatedAt:     now.Add(15 * time.Minute),
	})
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: now.Add(15 * time.Minute), CloseTime: now.Add(20 * time.Minute),
		Symbol: "ETHUSDT", Open: 100.50, High: 100.60, Low: 100.10, Close: 100.20,
	}, 3)
	if b.Stats.Repegged != 2 {
		t.Fatalf("expected Repegged=2 after SHORT cross, got %+v", b.Stats)
	}
	if ethPos := b.OpenPositions["ETHUSDT"]; ethPos == nil || !ethPos.EntryPrice.Equal(decimal.NewFromFloat(100.51)) {
		t.Fatalf("expected SHORT repegged fill at 100.51, got %+v", ethPos)
	}
}

func TestSimBroker_LimitTimeoutSecondsAndTimelineEquityAndDelistSettle(t *testing.T) {
	cfg := SimBrokerConfig{
		InitialEquityUSDT:      decimal.NewFromFloat(1000.0),
		AccountLeverage:        5.0,
		MakerFeeRate:           decimal.NewFromFloat(0.0002),
		TakerFeeRate:           decimal.NewFromFloat(0.0005),
		SlippageRate:           decimal.NewFromFloat(0.0005),
		LimitOrderTimeout:      150 * time.Second,
		PenetrationTicks:       1,
		Tier1BETriggerLongATR:  0.60,
		Tier1BETriggerShortATR: 0.90,
		Tier1BEEnabled:         true,
		MinBreakEvenOffsetPct:  0.001,
	}
	b := NewSimBroker(cfg)
	b.SetExchangeFilter("BTCUSDT", model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.1),
		StepSize: decimal.NewFromFloat(0.01),
	})

	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "TIMEOUT_1",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(90.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(85.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     t0,
	})

	// 1m bars: after 3rd minute (t0+3m = 180s > 150s), pending order should time out before filling.
	for i := 0; i < 4; i++ {
		cOpen := t0.Add(time.Duration(i) * time.Minute)
		b.ProcessCandle(data.HistoricalCandle{
			OpenTime: cOpen, CloseTime: cOpen.Add(time.Minute),
			Symbol: "BTCUSDT", Open: 95.0, High: 95.5, Low: 94.5, Close: 95.0,
		}, int64(i))
	}
	if b.Stats.TimedOut != 1 {
		t.Fatalf("expected 150s LimitOrderTimeout to expire pending order, stats=%+v", b.Stats)
	}

	// Now fill an entry, test custom Tier1BETriggerLongATR (0.60 * ATR 10 = +6.0 -> triggers BE), RecordTimelineEquity, and ForceClosePosition
	t1 := t0.Add(10 * time.Minute)
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "BE_DELIST",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0),
		SLPrice:       decimal.NewFromFloat(90.0),
		TPPrice:       decimal.NewFromFloat(130.0),
		ATRAtEntry:    decimal.NewFromFloat(10.0),
		CreatedAt:     t1,
	})
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: t1, CloseTime: t1.Add(time.Minute),
		Symbol: "BTCUSDT", Open: 100.0, High: 100.5, Low: 99.8, Close: 100.0,
	}, 10)
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime: t1.Add(time.Minute), CloseTime: t1.Add(2 * time.Minute),
		Symbol: "BTCUSDT", Open: 100.0, High: 106.5, Low: 100.0, Close: 106.2,
	}, 11)

	pos := b.OpenPositions["BTCUSDT"]
	if pos == nil || pos.ProtectionTier < 1 || !pos.CurrentSLPrice.GreaterThan(pos.EntryPrice) {
		t.Fatalf("expected Tier1BETriggerLongATR=0.60 to promote SL above entry, got %+v", pos)
	}

	b.RecordTimelineEquity(t1.Add(2 * time.Minute))
	if len(b.EquityCurve) != 1 || b.EquityCurve[0].OpenPositions != 1 || b.EquityCurve[0].UsedMargin <= 0 {
		t.Fatalf("unexpected EquityCurve sample: %+v", b.EquityCurve)
	}

	closed := b.ForceClosePosition("BTCUSDT", decimal.NewFromFloat(105.0), "DELIST_OR_DATA_END_SETTLE", t1.Add(2*time.Hour), 35)
	if !closed || len(b.OpenPositions) != 0 || b.Stats.DelistSettled != 1 {
		t.Fatalf("expected delist settle close, closed=%v stats=%+v", closed, b.Stats)
	}
}

func TestSimBroker_NoSameBarTP1LookAheadOnEntryFill(t *testing.T) {
	b := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
		MaxSlots:                3,
		AccountLeverage:         8.0,
		MakerFeeRate:            decimal.NewFromFloat(0.0002),
		TakerFeeRate:            decimal.NewFromFloat(0.0005),
		PenetrationTicks:        1,
		PartialTPEnabled:        true,
		PartialTP1Ratio:         0.50,
		IntrabarConflictSLFirst: true,
	})
	b.SetExchangeFilter("SOLUSDT", model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	})

	now := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "NO_LOOKAHEAD_1",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(2.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TP1Price:      decimal.NewFromFloat(102.0),
		TPPrice:       decimal.NewFromFloat(106.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
		CreatedAt:     now,
	})

	// Entry bar: High reaches 102.50 (above TP1 102.0) BEFORE/DURING the bar while Low dips to 99.90 (filling limit 100.0) and closes at 100.20.
	// Must NOT trigger TP1 or ratify PeakPrice=102.50 on the entry bar itself!
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Symbol:    "SOLUSDT",
		Open:      101.0,
		High:      102.5,
		Low:       99.9,
		Close:     100.2,
	}, 1)

	pos := b.OpenPositions["SOLUSDT"]
	if pos == nil {
		t.Fatalf("expected position to fill on entry bar")
	}
	if pos.IsTP1Hit || len(b.ClosedTrades) != 0 {
		t.Fatalf("expected TP1 NOT to trigger on the same bar as entry fill (no pre-fill wick look-ahead), got IsTP1Hit=%v trades=%d", pos.IsTP1Hit, len(b.ClosedTrades))
	}
	if !pos.PeakPrice.Equal(decimal.NewFromFloat(100.2)) {
		t.Fatalf("expected entry-bar PeakPrice capped at post-fill Close (100.2), got %s", pos.PeakPrice)
	}
}

func TestSimBroker_StaleLossExitDuration(t *testing.T) {
	b := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
		MaxSlots:                3,
		AccountLeverage:         5.0,
		EntryOrderType:          "MARKET",
		MakerFeeRate:            decimal.NewFromFloat(0.0002),
		TakerFeeRate:            decimal.NewFromFloat(0.0005),
		SlippageRate:            decimal.NewFromFloat(0.0),
		StaleLossExitDuration:   6 * time.Hour,
		MaxHoldingDuration:      18 * time.Hour,
		IntrabarConflictSLFirst: true,
	})
	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5)}
	b.SetExchangeFilter("LOSSUSDT", f)
	b.SetExchangeFilter("WINUSDT", f)

	t0 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "STALE_LOSS",
		Symbol:        "LOSSUSDT",
		Side:          model.SideShort,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(110.0),
		TPPrice:       decimal.NewFromFloat(85.0),
		CreatedAt:     t0,
	})
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "STALE_WIN",
		Symbol:        "WINUSDT",
		Side:          model.SideShort,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(110.0),
		TPPrice:       decimal.NewFromFloat(85.0),
		CreatedAt:     t0,
	})

	// Entry bar at t0..t0+15m
	b.ProcessCandle(data.HistoricalCandle{OpenTime: t0, CloseTime: t0.Add(15 * time.Minute), Symbol: "LOSSUSDT", Open: 100.0, High: 100.5, Low: 99.5, Close: 100.0}, 1)
	b.ProcessCandle(data.HistoricalCandle{OpenTime: t0, CloseTime: t0.Add(15 * time.Minute), Symbol: "WINUSDT", Open: 100.0, High: 100.5, Low: 99.5, Close: 100.0}, 1)

	// At t0+6h15m: LOSSUSDT closes underwater (101.0 > 100.0) -> must exit via TIME_STOP_STALE_LOSS;
	// WINUSDT closes in profit (98.0 < 100.0) -> must stay open!
	laterOpen := t0.Add(6 * time.Hour)
	b.ProcessCandle(data.HistoricalCandle{OpenTime: laterOpen, CloseTime: laterOpen.Add(15 * time.Minute), Symbol: "LOSSUSDT", Open: 100.8, High: 101.2, Low: 100.5, Close: 101.0}, 25)
	b.ProcessCandle(data.HistoricalCandle{OpenTime: laterOpen, CloseTime: laterOpen.Add(15 * time.Minute), Symbol: "WINUSDT", Open: 98.5, High: 98.8, Low: 97.8, Close: 98.0}, 25)

	if b.OpenPositions["LOSSUSDT"] != nil {
		t.Fatalf("expected underwater LOSSUSDT closed at StaleLossExitDuration")
	}
	if b.OpenPositions["WINUSDT"] == nil {
		t.Fatalf("expected profitable WINUSDT to remain open past StaleLossExitDuration")
	}
	if len(b.ClosedTrades) != 1 || b.ClosedTrades[0].ExitReason != "TIME_STOP_STALE_LOSS" {
		t.Fatalf("expected 1 TIME_STOP_STALE_LOSS trade, got %+v", b.ClosedTrades)
	}
}

func TestSimBroker_GetDrawdownFromPeakPct(t *testing.T) {
	b := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		MaxSlots:          3,
		AccountLeverage:   5.0,
	})
	if dd := b.GetDrawdownFromPeakPct(); dd != 0.0 {
		t.Fatalf("expected initial drawdown 0, got %.4f", dd)
	}
	// Simulate realized drop to 900 -> 10% DD from 1000 peak
	b.Mu.Lock()
	b.RealizedEquityUSDT = decimal.NewFromFloat(900.0)
	b.Mu.Unlock()
	if dd := b.GetDrawdownFromPeakPct(); dd < 0.099 || dd > 0.101 {
		t.Fatalf("expected 10%% DD from 1000 peak, got %.4f", dd)
	}
	// New peak at 1100 -> DD resets to 0, then drop to 1045 -> 5% DD
	b.Mu.Lock()
	b.RealizedEquityUSDT = decimal.NewFromFloat(1100.0)
	b.Mu.Unlock()
	if dd := b.GetDrawdownFromPeakPct(); dd != 0.0 {
		t.Fatalf("expected 0%% DD at new peak 1100, got %.4f", dd)
	}
	b.Mu.Lock()
	b.RealizedEquityUSDT = decimal.NewFromFloat(1045.0)
	b.Mu.Unlock()
	if dd := b.GetDrawdownFromPeakPct(); dd < 0.049 || dd > 0.051 {
		t.Fatalf("expected 5%% DD from new peak 1100, got %.4f", dd)
	}
}

func TestSimBroker_CrossMarginLiquidationAndDepletedEquity(t *testing.T) {
	b := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(100.0),
		MaxSlots:          3,
		AccountLeverage:   10.0,
		MakerFeeRate:      decimal.Zero,
		TakerFeeRate:      decimal.Zero,
		SlippageRate:      decimal.NewFromFloat(0.0002),
		PenetrationTicks:  1,
		LimitOrderTimeout: 15 * time.Minute,
	})
	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	b.SetExchangeFilter("BTCUSDT", filter)
	b.SetExchangeFilter("ETHUSDT", filter)

	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ok, err := b.SubmitLimitEntry(model.OrderIntent{
		Symbol:     "BTCUSDT",
		Side:       model.SideShort,
		Qty:        decimal.NewFromFloat(9.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(150.0),
		TPPrice:    decimal.NewFromFloat(50.0),
		ATRAtEntry: decimal.NewFromFloat(5.0),
		CreatedAt:  t0,
	})
	if !ok || err != nil {
		t.Fatalf("expected entry submit ok, got ok=%v err=%v", ok, err)
	}

	// Bar 1: filled at 100.0 (Notional = 900 USDT, Equity = 100 USDT)
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0,
		CloseTime: t0.Add(15 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      100.0,
		High:      100.5,
		Low:       99.5,
		Close:     100.0,
	}, 1)

	if b.CheckCrossMarginLiquidation(0.005, t0.Add(15*time.Minute), 1) {
		t.Fatalf("did not expect liquidation at entry equity=100, notional=900")
	}

	// Submit a pending order on ETHUSDT to ensure liquidation clears all pending orders too
	okPend, errPend := b.SubmitLimitEntry(model.OrderIntent{
		Symbol:     "ETHUSDT",
		Side:       model.SideShort,
		Qty:        decimal.NewFromFloat(1.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(120.0),
		TPPrice:    decimal.NewFromFloat(80.0),
		ATRAtEntry: decimal.NewFromFloat(2.0),
		CreatedAt:  t0.Add(15 * time.Minute),
	})
	if !okPend || errPend != nil {
		t.Fatalf("expected ETHUSDT pending submit ok, got %v %v", okPend, errPend)
	}

	// Bar 2: price spikes to 112.0 without hitting SL=150 -> Unrealized PnL = -108, effEq = -8 <= maintMargin (~5.04)
	t1 := t0.Add(15 * time.Minute)
	b.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t1,
		CloseTime: t1.Add(15 * time.Minute),
		Symbol:    "BTCUSDT",
		Open:      105.0,
		High:      112.5,
		Low:       104.5,
		Close:     112.0,
	}, 2)

	if !b.CheckCrossMarginLiquidation(0.005, t1.Add(15*time.Minute), 2) {
		t.Fatalf("expected cross-margin liquidation when equity <= maintenance margin")
	}
	if len(b.OpenPositions) != 0 || len(b.PendingOrders) != 0 {
		t.Fatalf("expected 0 open positions and 0 pending orders after liquidation, got open=%d pend=%d", len(b.OpenPositions), len(b.PendingOrders))
	}
	if b.Stats.LiquidatedCount != 1 {
		t.Fatalf("expected LiquidatedCount=1, got %d", b.Stats.LiquidatedCount)
	}
	if len(b.ClosedTrades) != 1 || b.ClosedTrades[0].ExitReason != "LIQUIDATION" {
		t.Fatalf("expected 1 LIQUIDATION closed trade, got %+v", b.ClosedTrades)
	}
	if !b.RealizedEquityUSDT.Equal(decimal.Zero) {
		t.Fatalf("expected realized equity clamped to 0 after wipeout, got %s", b.RealizedEquityUSDT.String())
	}
	if !b.GetAvailableBalanceUSDT().Equal(decimal.Zero) {
		t.Fatalf("expected 0 available balance after liquidation, got %s", b.GetAvailableBalanceUSDT().String())
	}
	if okNew, errNew := b.SubmitLimitEntry(model.OrderIntent{
		Symbol:     "ETHUSDT",
		Side:       model.SideShort,
		Qty:        decimal.NewFromFloat(1.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(120.0),
		TPPrice:    decimal.NewFromFloat(80.0),
		ATRAtEntry: decimal.NewFromFloat(2.0),
		CreatedAt:  t1.Add(15 * time.Minute),
	}); okNew || errNew == nil {
		t.Fatalf("expected new order rejected after liquidation/wipeout, got ok=%v err=%v", okNew, errNew)
	}
}

func TestSimBroker_Batch3TimeNormalizationZeroOverridesAndMarketBarTP(t *testing.T) {
	// 1. Explicit zero fee/slippage/penetration overrides must be preserved when *Set=true.
	bZero := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT:   decimal.NewFromFloat(1000.0),
		MakerFeeRate:        decimal.Zero,
		TakerFeeRate:        decimal.Zero,
		SlippageRate:        decimal.Zero,
		PenetrationTicks:    0,
		MakerFeeSet:         true,
		TakerFeeSet:         true,
		SlippageSet:         true,
		PenetrationTicksSet: true,
		LimitOrderTimeout:   5 * time.Minute,
	})
	if !bZero.Cfg.MakerFeeRate.IsZero() || !bZero.Cfg.TakerFeeRate.IsZero() || !bZero.Cfg.SlippageRate.IsZero() || bZero.Cfg.PenetrationTicks != 0 {
		t.Fatalf("expected zero overrides preserved, got %+v", bZero.Cfg)
	}

	// 2. ...999ms Binance CloseTime triggers exact 5m LimitOrderTimeout on the first bar (no 1-bar lag).
	bZero.SetExchangeFilter("SOLUSDT", model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1)})
	t0 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	_, _ = bZero.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "EXACT_TIMEOUT",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		LimitPrice:    decimal.NewFromFloat(90.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(85.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     t0,
	})
	bZero.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0,
		CloseTime: t0.Add(5*time.Minute - time.Millisecond), // 10:04:59.999 -> normalizes to 10:05:00.000
		Symbol:    "SOLUSDT",
		Open:      100.0,
		High:      101.0,
		Low:       99.0,
		Close:     100.5,
	}, 1)
	if bZero.Stats.TimedOut != 1 {
		t.Fatalf("expected 5m LimitOrderTimeout to expire cleanly at 10:04:59.999 (+1ms), got TimedOut=%d", bZero.Stats.TimedOut)
	}

	// 3. MARKET entry fills at Open and allows intrabar TP1/MFE on entry bar when SL is untouched.
	bMkt := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		EntryOrderType:    "MARKET",
		SlippageRate:      decimal.Zero,
		SlippageSet:       true,
		PartialTPEnabled:  true,
		PartialTP1Ratio:   0.5,
	})
	bMkt.SetExchangeFilter("ETHUSDT", model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5.0)})
	_, _ = bMkt.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID:  "MKT_SAME_BAR_TP1",
		Symbol:         "ETHUSDT",
		Side:           model.SideLong,
		EntryOrderType: "MARKET",
		LimitPrice:     decimal.NewFromFloat(100.0),
		Qty:            decimal.NewFromFloat(2.0),
		SLPrice:        decimal.NewFromFloat(95.0),
		TP1Price:       decimal.NewFromFloat(103.0),
		TPPrice:        decimal.NewFromFloat(110.0),
		ATRAtEntry:     decimal.NewFromFloat(2.0),
		CreatedAt:      t0,
	})
	bMkt.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0,
		CloseTime: t0.Add(5*time.Minute - time.Millisecond),
		Symbol:    "ETHUSDT",
		Open:      100.0,
		High:      104.0,
		Low:       99.0,
		Close:     103.5,
	}, 1)
	if len(bMkt.ClosedTrades) != 1 || bMkt.ClosedTrades[0].ExitReason != "PARTIAL_TAKE_PROFIT" {
		t.Fatalf("expected MARKET entry-bar TP1 trigger when SL untouched, got trades=%+v", bMkt.ClosedTrades)
	}

	// 4. Intrabar adverse wick triggers cross-margin liquidation even if Close recovers.
	bLiq := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(100.0),
		AccountLeverage:         10.0,
		EntryOrderType:          "MARKET",
		SlippageRate:            decimal.Zero,
		SlippageSet:             true,
		MakerFeeSet:             true,
		TakerFeeSet:             true,
		IntrabarConflictSLFirst: true,
	})
	bLiq.SetExchangeFilter("BTCUSDT", model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.1), StepSize: decimal.NewFromFloat(0.1)})
	_, _ = bLiq.SubmitLimitEntry(model.OrderIntent{
		Symbol:     "BTCUSDT",
		Side:       model.SideShort,
		Qty:        decimal.NewFromFloat(9.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(150.0),
		TPPrice:    decimal.NewFromFloat(50.0),
		CreatedAt:  t0,
	})
	bLiq.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0,
		CloseTime: t0.Add(5*time.Minute - time.Millisecond),
		Symbol:    "BTCUSDT",
		Open:      100.0,
		High:      113.0, // unrealized -117 <= maintMargin even though Close recovers to 101!
		Low:       99.5,
		Close:     101.0,
	}, 1)
	if !bLiq.CheckCrossMarginLiquidation(0.005, t0.Add(5*time.Minute), 1) {
		t.Fatalf("expected intrabar wick High=113 to trigger cross-margin liquidation")
	}
}

func TestSimBroker_ExpiredLimitGapDoesNotFillAndIsolatedLiquidation(t *testing.T) {
	t0 := time.Date(2026, 6, 15, 10, 0, 0, 0, time.UTC)
	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1)}

	// 1. Expired LIMIT order must expire as TimedOut if a candle gap skips past TimeoutTime (c.OpenTime >= TimeoutTime).
	bGap := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT: decimal.NewFromFloat(1000.0),
		EntryOrderType:    "LIMIT",
		CandleDuration:    15 * time.Minute,
		SignalDelayBars:   1,
		LimitOrderTimeout: 30 * time.Minute,
		PenetrationTicks:  1,
		PenetrationTicksSet: true,
	})
	bGap.SetExchangeFilter("ETHUSDT", f)
	if ok, err := bGap.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "GAP_LIMIT",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		LimitPrice:    decimal.NewFromFloat(100.0),
		Qty:           decimal.NewFromFloat(1.0),
		SLPrice:       decimal.NewFromFloat(105.0),
		TPPrice:       decimal.NewFromFloat(90.0),
		CreatedAt:     t0, // TimeoutTime = 10:30 UTC (after 1 bar SignalDelayBars=1 at 10:15? CreatedAt+30m = 10:30)
	}); !ok || err != nil {
		t.Fatalf("submit failed: ok=%v err=%v", ok, err)
	}
	// Missing 10:15..10:45 bars; next candle opens at 10:45 UTC (>= 10:30 TimeoutTime) and shoots past LimitPrice 100.
	bGap.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0.Add(45 * time.Minute),
		CloseTime: t0.Add(60*time.Minute - time.Millisecond),
		Symbol:    "ETHUSDT",
		Open:      99.0,
		High:      101.0,
		Low:       98.0,
		Close:     99.5,
	}, 3)
	if bGap.Stats.Filled != 0 || bGap.Stats.TimedOut != 1 || bGap.HasActiveOrderOrPosition("ETHUSDT") {
		t.Fatalf("expected expired LIMIT order across candle gap to time out with 0 fills, got stats=%+v active=%v", bGap.Stats, bGap.HasActiveOrderOrPosition("ETHUSDT"))
	}

	// 2. CheckIsolatedMarginLiquidation liquidates ONLY the breached isolated position even with large total equity.
	bIso := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(10000.0),
		AccountLeverage:         10.0,
		EntryOrderType:          "MARKET",
		SlippageRate:            decimal.Zero,
		SlippageSet:             true,
		MakerFeeSet:             true,
		TakerFeeSet:             true,
		IntrabarConflictSLFirst: true,
		MaxHoldingDuration:      24 * time.Hour,
	})
	bIso.SetExchangeFilter("BADUSDT", f)
	bIso.SetExchangeFilter("GOODUSDT", f)
	// BADUSDT SHORT at 100, Qty=1 -> initial margin 10 USDT; SL intentionally far (130) to simulate gap past SL.
	_, _ = bIso.SubmitLimitEntry(model.OrderIntent{
		Symbol:     "BADUSDT",
		Side:       model.SideShort,
		Qty:        decimal.NewFromFloat(1.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(130.0),
		TPPrice:    decimal.NewFromFloat(70.0),
		CreatedAt:  t0,
	})
	_, _ = bIso.SubmitLimitEntry(model.OrderIntent{
		Symbol:     "GOODUSDT",
		Side:       model.SideShort,
		Qty:        decimal.NewFromFloat(1.0),
		LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice:    decimal.NewFromFloat(110.0),
		TPPrice:    decimal.NewFromFloat(80.0),
		CreatedAt:  t0,
	})
	// BADUSDT wicks to High=111 (-11 USDT unrealized < +10 initMargin - 0.55 maintMargin), Close settles back to 101.
	bIso.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0,
		CloseTime: t0.Add(15*time.Minute - time.Millisecond),
		Symbol:    "BADUSDT",
		Open:      100.0,
		High:      111.0,
		Low:       99.5,
		Close:     101.0,
	}, 1)
	bIso.ProcessCandle(data.HistoricalCandle{
		OpenTime:  t0,
		CloseTime: t0.Add(15*time.Minute - time.Millisecond),
		Symbol:    "GOODUSDT",
		Open:      100.0,
		High:      101.0,
		Low:       99.0,
		Close:     99.5,
	}, 1)

	liqCnt := bIso.CheckIsolatedMarginLiquidation(0.005, t0.Add(15*time.Minute), 1)
	if liqCnt != 1 || bIso.Stats.LiquidatedCount != 1 {
		t.Fatalf("expected exactly 1 isolated liquidation, got liqCnt=%d stats=%+v", liqCnt, bIso.Stats)
	}
	if bIso.HasActiveOrderOrPosition("BADUSDT") {
		t.Fatalf("expected BADUSDT liquidated and removed from OpenPositions")
	}
	if !bIso.HasActiveOrderOrPosition("GOODUSDT") {
		t.Fatalf("expected healthy GOODUSDT position to remain open under ISOLATED margin")
	}
	if len(bIso.ClosedTrades) != 1 || bIso.ClosedTrades[0].ExitReason != "LIQUIDATION" || bIso.ClosedTrades[0].Symbol != "BADUSDT" {
		t.Fatalf("unexpected closed trade after isolated liquidation: %+v", bIso.ClosedTrades)
	}
}

func TestSimBroker_StrategyExitProfilesAndFilteredSlots(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	f := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	b := NewSimBroker(SimBrokerConfig{
		InitialEquityUSDT:     decimal.NewFromFloat(1000.0),
		MaxSlots:              9,
		AccountLeverage:       10.0,
		EntryOrderType:        "MARKET",
		SlippageRate:          decimal.Zero,
		SlippageSet:           true,
		MakerFeeSet:           true,
		TakerFeeSet:           true,
		TrailingEnabled:       true,
		TrailingActivateATR:   2.5,
		TrailingDistanceATR:   1.0,
		StaleLossExitDuration: 6 * time.Hour,
		MaxHoldingDuration:    18 * time.Hour,
		StrategyExitProfiles: map[string]protection.StrategyExitProfile{
			"MAJOR_RSMOM_SHORT": {
				HasOverride:           true,
				TrailingEnabled:       true,
				TrailingActivateATR:   2.5,
				TrailingDistanceATR:   1.2,
				StaleLossExitDuration: 0,
				MaxHoldingDuration:    48 * time.Hour,
			},
		},
	})
	b.SetExchangeFilter("ALTUSDT", f)
	b.SetExchangeFilter("ETHUSDT", f)

	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		Symbol: "ALTUSDT", Side: model.SideShort, StrategyID: "VWAP_PULLBACK",
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(120.0), TPPrice: decimal.NewFromFloat(70.0),
		ATRAtEntry: decimal.NewFromFloat(2.0), CreatedAt: t0,
	})
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		Symbol: "ETHUSDT", Side: model.SideShort, StrategyID: "MAJOR_RSMOM_SHORT",
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(120.0), TPPrice: decimal.NewFromFloat(70.0),
		ATRAtEntry: decimal.NewFromFloat(2.0), CreatedAt: t0,
	})

	for _, sym := range []string{"ALTUSDT", "ETHUSDT"} {
		b.ProcessCandle(data.HistoricalCandle{
			Symbol: sym, OpenTime: t0, CloseTime: t0.Add(15*time.Minute - time.Millisecond),
			Open: 100.0, High: 100.5, Low: 99.5, Close: 100.0,
		}, 1)
	}
	if nonMaj := b.GetActiveSlotCountByFilter(func(s string) bool { return s != "MAJOR_RSMOM_SHORT" }); nonMaj != 1 {
		t.Fatalf("expected 1 non-major slot, got %d", nonMaj)
	}
	if maj := b.GetActiveSlotCountByFilter(func(s string) bool { return s == "MAJOR_RSMOM_SHORT" }); maj != 1 {
		t.Fatalf("expected 1 major slot, got %d", maj)
	}

	t7 := t0.Add(7 * time.Hour)
	for _, sym := range []string{"ALTUSDT", "ETHUSDT"} {
		b.ProcessCandle(data.HistoricalCandle{
			Symbol: sym, OpenTime: t7, CloseTime: t7.Add(15*time.Minute - time.Millisecond),
			Open: 100.5, High: 101.5, Low: 100.0, Close: 101.0,
		}, 29)
	}
	if b.HasActiveOrderOrPosition("ALTUSDT") {
		t.Fatalf("expected VWAP_PULLBACK ALTUSDT closed after 6h underwater stale-loss")
	}
	if !b.HasActiveOrderOrPosition("ETHUSDT") {
		t.Fatalf("expected MAJOR_RSMOM_SHORT ETHUSDT to ignore 6h stale-loss via per-strategy profile")
	}

	t49 := t0.Add(49 * time.Hour)
	b.ProcessCandle(data.HistoricalCandle{
		Symbol: "ETHUSDT", OpenTime: t49, CloseTime: t49.Add(15*time.Minute - time.Millisecond),
		Open: 99.0, High: 99.5, Low: 98.5, Close: 99.0,
	}, 197)
	if b.HasActiveOrderOrPosition("ETHUSDT") {
		t.Fatalf("expected ETHUSDT closed after 48h max holding")
	}
}




