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

	// Sisa funding di posisi harus tinggal $5.0 dan InitialQty disesuaikan menjadi 1.0
	if !pos.FundingPaid.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected remaining FundingPaid 5.0, got %s", pos.FundingPaid)
	}
	if !pos.InitialQty.Equal(decimal.NewFromFloat(1.0)) {
		t.Fatalf("expected remaining InitialQty 1.0, got %s", pos.InitialQty)
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



