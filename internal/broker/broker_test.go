package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestDryRunBrokerAggressorMatching(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		AccountLeverage:     5.0,
		LimitOrderTimeout:   45 * time.Second,
		MaxHoldingDuration:  8 * time.Hour,
		MakerFeeRate:        0.0002,
		TakerFeeRate:        0.0005,
		SlippageRate:        0.0002,
	}
	broker := NewDryRunBroker(decimal.NewFromFloat(10000.0), cfg)
	now := time.Now().UTC()

	intent := model.OrderIntent{
		ClientOrderID: "TEST_BUY_LIMIT",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed to submit limit entry: %v", err)
	}

	// 1. isBuyerMaker == false: maker adalah seller (agresor adalah buyer). Order limit BUY di bid TIDAK boleh terisi!
	broker.OnRealtimeTick("BTCUSDT", 100.0, 100.1, 100.0, 100.0, 0.5, false, now)

	broker.Mu.Lock()
	ord := broker.PendingOrders["BTCUSDT"]
	if !ord.FilledQty.IsZero() {
		t.Errorf("expected 0 filled when isBuyerMaker == false, got: %s", ord.FilledQty)
	}
	broker.Mu.Unlock()

	// 2. isBuyerMaker == true: maker adalah buyer (agresor adalah seller menyapu bid). Order limit BUY di bid HARUS terisi!
	broker.OnRealtimeTick("BTCUSDT", 100.0, 100.1, 100.0, 100.0, 0.5, true, now)

	broker.Mu.Lock()
	ord = broker.PendingOrders["BTCUSDT"]
	if !ord.FilledQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Errorf("expected 0.5 filled, got: %s", ord.FilledQty)
	}
	// Pastikan posisi virtual terbuka seketika untuk partial fill
	pos, hasPos := broker.OpenPositions["BTCUSDT"]
	if !hasPos || !pos.Qty.Equal(decimal.NewFromFloat(0.5)) {
		t.Errorf("expected active open position with 0.5 qty, got pos: %+v", pos)
	}
	broker.Mu.Unlock()
}

func TestLiveBrokerTwoPhaseAndSafeSL(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		MaxAccountStaleness: 60 * time.Second,
		LimitOrderTimeout:   45 * time.Second,
	}
	broker := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()

	broker.HandleAccountUpdate(
		decimal.NewFromFloat(10000.0),
		decimal.Zero,
		decimal.NewFromFloat(10000.0),
		nil,
		now,
	)

	var entryPlaced int32
	broker.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		atomic.AddInt32(&entryPlaced, 1)
		return 999001, nil
	}

	var slPlaced int32
	var oldSLCancelled int32
	broker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			atomic.AddInt32(&slPlaced, 1)
			return 888001, nil
		}
		return 888002, nil
	}

	broker.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		if algoID == 888001 {
			atomic.AddInt32(&oldSLCancelled, 1)
		}
		return nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "LIVE_ORDER_1",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(5.0),
		LimitPrice:    decimal.NewFromFloat(150.0),
		SLPrice:       decimal.NewFromFloat(145.0),
		TPPrice:       decimal.NewFromFloat(160.0),
		ATRAtEntry:    decimal.NewFromFloat(3.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected successful live limit entry submission: %v", err)
	}

	if atomic.LoadInt32(&entryPlaced) != 1 {
		t.Errorf("expected PlaceEntryFn called exactly once")
	}

	// Simulasi fill event
	broker.HandleUserDataOrderUpdate("LIVE_ORDER_1", 999001, "SOLUSDT", model.StatusFilled, decimal.NewFromFloat(5.0), decimal.NewFromFloat(150.0), decimal.Zero)

	time.Sleep(100 * time.Millisecond)

	broker.Mu.Lock()
	prot, hasProt := broker.Protections["SOLUSDT"]
	if !hasProt {
		t.Fatalf("expected protection plan recorded for SOLUSDT")
	}
	if prot.SLStatus != model.ProtectionAcked || prot.SLOrderID != 888001 {
		t.Errorf("expected SL ACKED with ID 888001, got %+v", prot)
	}
	broker.Mu.Unlock()
}

func TestDryRunPartialFillStopLossTrigger(t *testing.T) {
	broker := NewDryRunBroker(decimal.NewFromFloat(10000.0), DryRunConfig{})
	now := time.Now().UTC()

	intent := model.OrderIntent{
		ClientOrderID: "TEST_PARTIAL_SL",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		SLPrice:       decimal.NewFromFloat(1950.0),
		TPPrice:       decimal.NewFromFloat(2100.0),
		ATRAtEntry:    decimal.NewFromFloat(20.0),
		CreatedAt:     now,
	}

	_, _ = broker.SubmitLimitEntry(intent)

	// Partial fill 3 unit
	broker.OnRealtimeTick("ETHUSDT", 2000.0, 2000.5, 2000.0, 2000.0, 3.0, true, now)

	broker.Mu.Lock()
	if _, exists := broker.OpenPositions["ETHUSDT"]; !exists {
		t.Fatalf("expected OpenPosition immediately created for partial fill")
	}
	broker.Mu.Unlock()

	// Harga jatuh menyentuh SL (1940 <= 1950) sebelum timeout 45 detik
	broker.OnRealtimeTick("ETHUSDT", 1940.0, 1940.5, 1940.0, 0, 0, false, now.Add(10*time.Second))

	broker.Mu.Lock()
	defer broker.Mu.Unlock()

	if _, exists := broker.OpenPositions["ETHUSDT"]; exists {
		t.Errorf("expected position closed by SL trigger")
	}
	if _, exists := broker.PendingOrders["ETHUSDT"]; exists {
		t.Errorf("expected remaining pending order cancelled after position SL exit")
	}
}

func TestLiveBrokerExitSiblingCancellation(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{})
	now := time.Now().UTC()

	broker.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	var cancelledAlgoID int64
	broker.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		cancelledAlgoID = algoID
		return nil
	}

	broker.Mu.Lock()
	broker.ActivePositions["BNBUSDT"] = decimal.NewFromFloat(2.0)
	broker.Protections["BNBUSDT"] = &model.LivePositionProtection{
		Symbol:         "BNBUSDT",
		Side:           model.SideLong,
		ProtectedQty:   decimal.NewFromFloat(2.0),
		SLOrderID:      777001,
		SLClientAlgoID: "ALGO_SL_BNB",
		SLStatus:       model.ProtectionAcked,
		TPOrderID:      777002,
		TPClientAlgoID: "ALGO_TP_BNB",
		TPStatus:       model.ProtectionAcked,
	}
	broker.OrdersByClient["ALGO_SL_BNB"] = &model.LiveOrderRecord{
		ClientOrderID:   "ALGO_SL_BNB",
		ExchangeOrderID: 777001,
		Symbol:          "BNBUSDT",
		IsEntry:         false,
		Status:          model.StatusNew,
	}
	broker.OrdersByExchangeID[777001] = broker.OrdersByClient["ALGO_SL_BNB"]

	broker.OrdersByClient["ALGO_TP_BNB"] = &model.LiveOrderRecord{
		ClientOrderID:   "ALGO_TP_BNB",
		ExchangeOrderID: 777002,
		Symbol:          "BNBUSDT",
		IsEntry:         false,
		Status:          model.StatusNew,
	}
	broker.OrdersByExchangeID[777002] = broker.OrdersByClient["ALGO_TP_BNB"]
	broker.Mu.Unlock()

	// Simulasikan event WebSocket: SL terisi (FILLED)
	broker.HandleUserDataOrderUpdate("ALGO_SL_BNB", 777001, "BNBUSDT", model.StatusFilled, decimal.NewFromFloat(2.0), decimal.NewFromFloat(290.0), decimal.NewFromFloat(-20.0))

	if cancelledAlgoID != 777002 {
		t.Errorf("expected sibling TP algo order (777002) cancelled upon SL fill, got: %d", cancelledAlgoID)
	}

	broker.Mu.Lock()
	defer broker.Mu.Unlock()
	if _, exists := broker.ActivePositions["BNBUSDT"]; exists {
		t.Errorf("expected active position cleared after exit fill")
	}
	if _, exists := broker.Protections["BNBUSDT"]; exists {
		t.Errorf("expected protection record cleared after exit fill")
	}
}

func TestLiveBrokerEntryTimeoutCancellation(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{LimitOrderTimeout: 45 * time.Second})
	now := time.Now().UTC()

	var cancelledExchangeID int64
	broker.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
		cancelledExchangeID = exchangeOrderID
		return nil
	}

	broker.Mu.Lock()
	broker.OrdersByClient["EXPIRED_ENTRY"] = &model.LiveOrderRecord{
		ClientOrderID:       "EXPIRED_ENTRY",
		ExchangeOrderID:     555123,
		Symbol:              "AVAXUSDT",
		Side:                model.SideLong,
		IsEntry:             true,
		Status:              model.StatusNew,
		TargetQty:           decimal.NewFromFloat(10.0),
		CumulativeFilledQty: decimal.Zero,
		CreatedAt:           now.Add(-50 * time.Second),
	}
	broker.OrdersByExchangeID[555123] = broker.OrdersByClient["EXPIRED_ENTRY"]
	broker.Mu.Unlock()

	broker.CheckPendingEntryTimeouts(now)

	if cancelledExchangeID != 555123 {
		t.Errorf("expected order 555123 cancelled due to 45s timeout, got: %d", cancelledExchangeID)
	}
}

func TestLiveBrokerTimeStopHolding(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxHoldingDuration: 4 * time.Hour,
	})
	now := time.Now().UTC()

	var emergencyCloseSymbol string
	var emergencyCloseReason string
	broker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
		emergencyCloseSymbol = symbol
		emergencyCloseReason = reason
		return nil
	}

	// Simulasikan posisi terbuka 4 jam 5 menit yang lalu
	broker.HandleAccountUpdate(
		decimal.NewFromFloat(1000),
		decimal.Zero,
		decimal.NewFromFloat(1000),
		map[string]model.LivePosition{
			"SOLUSDT": {
				Symbol:        "SOLUSDT",
				Side:          model.SideLong,
				Qty:           decimal.NewFromFloat(5.0),
				EntryPrice:    decimal.NewFromFloat(150.0),
				UnrealizedPnL: decimal.Zero,
			},
		},
		now.Add(-4*time.Hour-5*time.Minute),
	)

	broker.CheckMaxHoldingTimeouts(now)

	if emergencyCloseSymbol != "SOLUSDT" {
		t.Errorf("expected emergency close for SOLUSDT, got '%s'", emergencyCloseSymbol)
	}
	if emergencyCloseReason != "TIME_STOP_MAX_HOLDING" {
		t.Errorf("expected reason TIME_STOP_MAX_HOLDING, got '%s'", emergencyCloseReason)
	}
}

func TestSlotCountNoDoubleCount(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3})
	now := time.Now().UTC()

	broker.Mu.Lock()
	// Simulasikan partial fill: ADAUSDT ada di OrdersByClient dan di ActivePositions
	broker.OrdersByClient["ENTRY_ADA"] = &model.LiveOrderRecord{
		ClientOrderID:       "ENTRY_ADA",
		Symbol:              "ADAUSDT",
		IsEntry:             true,
		Status:              model.StatusPartiallyFilled,
		TargetQty:           decimal.NewFromFloat(1000),
		CumulativeFilledQty: decimal.NewFromFloat(500),
		CreatedAt:           now,
	}
	broker.ActivePositions["ADAUSDT"] = decimal.NewFromFloat(500)
	broker.Mu.Unlock()

	slots := broker.GetActiveSlotCount()
	if slots != 1 {
		t.Errorf("expected 1 slot for partially filled position, got %d", slots)
	}
}

func TestCheckTimeoutsWatchdog(t *testing.T) {
	now := time.Now().UTC()

	// 1. Test DryRunBroker CheckTimeouts
	dryBroker := NewDryRunBroker(decimal.NewFromFloat(1000), DryRunConfig{
		LimitOrderTimeout:  45 * time.Second,
		MaxHoldingDuration: 4 * time.Hour,
	})

	dryBroker.Mu.Lock()
	dryBroker.PendingOrders["BTCUSDT"] = &VirtualPendingOrder{
		Intent: model.OrderIntent{
			Symbol:    "BTCUSDT",
			CreatedAt: now.Add(-50 * time.Second), // Expired
		},
	}
	dryBroker.OpenPositions["ETHUSDT"] = &VirtualOpenPosition{
		Symbol:        "ETHUSDT",
		OpenTime:      now.Add(-5 * time.Hour), // Expired
		LastMarkPrice: decimal.NewFromFloat(2000),
	}
	dryBroker.Mu.Unlock()

	dryBroker.CheckTimeouts(now)

	dryBroker.Mu.Lock()
	if _, pendingExists := dryBroker.PendingOrders["BTCUSDT"]; pendingExists {
		t.Errorf("expected pending BTCUSDT order to be purged by timeout watchdog")
	}
	if _, posExists := dryBroker.OpenPositions["ETHUSDT"]; posExists {
		t.Errorf("expected expired ETHUSDT position to be closed by timeout watchdog")
	}
	dryBroker.Mu.Unlock()

	// 2. Test BinanceLiveBroker CheckTimeouts
	var emergencyCloseCalled bool
	liveBroker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxHoldingDuration: 4 * time.Hour,
	})
	liveBroker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
		if symbol == "SOLUSDT" && reason == "TIME_STOP_MAX_HOLDING" {
			emergencyCloseCalled = true
		}
		return nil
	}

	liveBroker.Mu.Lock()
	liveBroker.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(10)
	liveBroker.PositionSides["SOLUSDT"] = model.SideLong
	liveBroker.PositionOpenTimes["SOLUSDT"] = now.Add(-5 * time.Hour)
	liveBroker.Mu.Unlock()

	liveBroker.CheckTimeouts(now)

	if !emergencyCloseCalled {
		t.Errorf("expected emergency close to be triggered by live broker CheckTimeouts watchdog")
	}
}

func TestCheckMaxHoldingTimeoutsAntiPanicAndRetry(t *testing.T) {
	now := time.Now().UTC()
	broker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxHoldingDuration: 4 * time.Hour,
	})

	var attempts int
	broker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
		attempts++
		if attempts < 2 {
			return fmt.Errorf("transient connection timeout")
		}
		return nil
	}

	// Skenario 1: Posisi kadaluarsa tapi PositionSides kosong -> TIDAK BOLEH PANIC (C2 FIX)
	broker.Mu.Lock()
	broker.ActivePositions["ORPHAN"] = decimal.NewFromFloat(5)
	broker.PositionOpenTimes["ORPHAN"] = now.Add(-5 * time.Hour)
	// Sengaja tidak mengisi broker.PositionSides["ORPHAN"]
	broker.Mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("CheckMaxHoldingTimeouts panicked on missing PositionSide: %v", r)
		}
	}()

	broker.CheckMaxHoldingTimeouts(now)

	// Skenario 2: Posisi dengan side valid gagal pada attempt 1, sukses pada attempt 2 (H1 FIX)
	broker.Mu.Lock()
	broker.ActivePositions["RETRY_COIN"] = decimal.NewFromFloat(10)
	broker.PositionSides["RETRY_COIN"] = model.SideLong
	broker.PositionOpenTimes["RETRY_COIN"] = now.Add(-5 * time.Hour)
	broker.Mu.Unlock()

	broker.CheckMaxHoldingTimeouts(now)

	if attempts < 2 {
		t.Errorf("expected at least 2 attempts due to retry, got %d", attempts)
	}
}

func TestLiveBrokerDailyStateRecoveryAndBreaker(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxDailyDrawdownPct: -0.03, // -3% limit
	})

	now := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	epochDay := now.Unix() / 86400

	// Simulasikan state yang di-recover dari SQLite saat bot nyala ulang
	broker.Mu.Lock()
	broker.SessionStartEpochDay = epochDay
	broker.SessionStartEquityUSDT = decimal.NewFromFloat(10000.0)
	broker.CircuitBreakerTripped = false
	broker.Mu.Unlock()

	// Datang ACCOUNT_UPDATE pertama pada hari yang sama dengan saldo turun ke 9600 (-4%)
	broker.HandleAccountUpdate(
		decimal.NewFromFloat(9600.0),
		decimal.Zero,
		decimal.NewFromFloat(9600.0),
		nil,
		now.Add(10*time.Minute),
	)

	broker.Mu.Lock()
	defer broker.Mu.Unlock()

	// Pastikan baseline SessionStartEquityUSDT TIDAK terhapus/ditimpa menjadi 9600
	if !broker.SessionStartEquityUSDT.Equal(decimal.NewFromFloat(10000.0)) {
		t.Fatalf("expected SessionStartEquityUSDT to remain 10000.0, got: %s", broker.SessionStartEquityUSDT)
	}
	// Pastikan circuit breaker langsung aktif karena -4% <= -3%
	if !broker.CircuitBreakerTripped {
		t.Fatalf("expected CircuitBreakerTripped to be true for -4%% drawdown against recovered baseline")
	}
}

func TestCheckMaxHoldingTimeoutsInFlightGuard(t *testing.T) {
	now := time.Now().UTC()
	broker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxHoldingDuration: 4 * time.Hour,
	})

	startedClose := make(chan struct{})
	continueClose := make(chan struct{})
	var closeCalls int32

	broker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
		atomic.AddInt32(&closeCalls, 1)
		close(startedClose)
		<-continueClose
		return nil
	}

	broker.Mu.Lock()
	broker.ActivePositions["ETHUSDT"] = decimal.NewFromFloat(1.0)
	broker.PositionSides["ETHUSDT"] = model.SideLong
	broker.PositionOpenTimes["ETHUSDT"] = now.Add(-5 * time.Hour)
	broker.Mu.Unlock()

	// Panggilan pertama berjalan di goroutine terpisah dan sedang in-flight di EmergencyMarketCloseFn
	go broker.CheckMaxHoldingTimeouts(now)

	// Tunggu sampai panggilan pertama benar-benar in-flight
	<-startedClose

	// Panggilan kedua saat panggilan pertama masih in-flight TIDAK boleh memicu close kedua
	broker.CheckMaxHoldingTimeouts(now)
	if calls := atomic.LoadInt32(&closeCalls); calls != 1 {
		t.Fatalf("expected InFlight guard to prevent concurrent close, got %d calls", calls)
	}

	// Lepaskan panggilan pertama agar selesai bersih
	close(continueClose)
}

func TestRepairFailedProtectionsSuccess(t *testing.T) {
	now := time.Now().UTC()
	broker := NewBinanceLiveBroker(LiveBrokerConfig{})

	var placedAlgoType string
	var placedAlgoPrice decimal.Decimal
	var placedCount int32

	broker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		atomic.AddInt32(&placedCount, 1)
		placedAlgoType = orderType
		placedAlgoPrice = triggerPrice
		return 88888, nil
	}

	broker.Mu.Lock()
	broker.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(1.0)
	broker.PositionSides["BTCUSDT"] = model.SideLong
	broker.Protections["BTCUSDT"] = &model.LivePositionProtection{
		Symbol:         "BTCUSDT",
		Side:           model.SideLong,
		PlanGeneration: 1,
		SLOrderID:      11111,
		SLStatus:       model.ProtectionAcked,
		TPStatus:       model.ProtectionFailed,
		TPPrice:        decimal.NewFromFloat(65000.0),
		LastError:      "initial network timeout",
	}
	broker.Mu.Unlock()

	// Panggil CheckTimeouts -> harus memicu RepairFailedProtections
	broker.CheckTimeouts(now)

	if count := atomic.LoadInt32(&placedCount); count != 1 {
		t.Fatalf("expected 1 TP repair attempt, got %d", count)
	}
	if placedAlgoType != "TAKE_PROFIT_MARKET" {
		t.Errorf("expected TAKE_PROFIT_MARKET, got %s", placedAlgoType)
	}
	if !placedAlgoPrice.Equal(decimal.NewFromFloat(65000.0)) {
		t.Errorf("expected trigger price 65000.0, got %s", placedAlgoPrice)
	}

	broker.Mu.Lock()
	prot := broker.Protections["BTCUSDT"]
	if prot.TPStatus != model.ProtectionAcked || prot.TPOrderID != 88888 {
		t.Errorf("expected TPStatus=ProtectionAcked and TPOrderID=88888, got status=%v id=%d", prot.TPStatus, prot.TPOrderID)
	}
	ord, hasOrd := broker.OrdersByExchangeID[88888]
	if !hasOrd || ord.Status != model.StatusNew {
		t.Errorf("expected order record indexed by exchange id 88888")
	}
	broker.Mu.Unlock()

	// Panggilan kedua sesaat kemudian TIDAK boleh memicu repair lagi karena TPStatus sudah ProtectionAcked
	broker.CheckTimeouts(now.Add(1 * time.Second))
	if count := atomic.LoadInt32(&placedCount); count != 1 {
		t.Fatalf("expected still 1 call, got %d", count)
	}
}

func TestRepairFailedProtectionsGhostOrder(t *testing.T) {
	now := time.Now().UTC()
	broker := NewBinanceLiveBroker(LiveBrokerConfig{})

	canceledChan := make(chan int64, 1)
	broker.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		canceledChan <- algoID
		return nil
	}

	startedRepair := make(chan struct{})
	continueRepair := make(chan struct{})

	broker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		close(startedRepair)
		<-continueRepair
		return 99999, nil
	}

	broker.Mu.Lock()
	broker.ActivePositions["ETHUSDT"] = decimal.NewFromFloat(2.0)
	broker.PositionSides["ETHUSDT"] = model.SideLong
	broker.Protections["ETHUSDT"] = &model.LivePositionProtection{
		Symbol:         "ETHUSDT",
		Side:           model.SideLong,
		PlanGeneration: 1,
		TPStatus:       model.ProtectionFailed,
		TPPrice:        decimal.NewFromFloat(3500.0),
	}
	broker.Mu.Unlock()

	go broker.CheckTimeouts(now)

	<-startedRepair
	// Posisi ditutup (misal kena SL) saat order TP repair sedang terbang
	broker.Mu.Lock()
	delete(bActivePositions(broker), "ETHUSDT")
	broker.Mu.Unlock()

	close(continueRepair)

	select {
	case id := <-canceledChan:
		if id != 99999 {
			t.Errorf("expected ghost TP order 99999 to be canceled immediately, got canceled id=%d", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for ghost TP order cancellation")
	}
}

func bActivePositions(b *BinanceLiveBroker) map[string]decimal.Decimal {
	return b.ActivePositions
}

func TestDryRunMarketEntryInstantFill(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		AccountLeverage:     5.0,
		TakerFeeRate:        0.0005,
		EntryOrderType:      "MARKET",
	}
	initialEquity := decimal.NewFromFloat(1000.0)
	broker := NewDryRunBroker(initialEquity, cfg)
	now := time.Now().UTC()

	intent := model.OrderIntent{
		ClientOrderID: "TEST_MKT_BUY",
		Symbol:        "AAVEUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(2.0),
		LimitPrice:    decimal.NewFromFloat(153.0),
		SLPrice:       decimal.NewFromFloat(148.0),
		TPPrice:       decimal.NewFromFloat(163.0),
		ATRAtEntry:    decimal.NewFromFloat(3.0),
		CreatedAt:     now,
		SignalAuditID: 44,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected market order submitted successfully, err: %v", err)
	}

	broker.Mu.Lock()
	defer broker.Mu.Unlock()

	// Pastikan TIDAK masuk ke PendingOrders
	if _, pending := broker.PendingOrders["AAVEUSDT"]; pending {
		t.Errorf("expected no pending order in MARKET mode")
	}

	// Pastikan LANGSUNG masuk ke OpenPositions
	pos, exists := broker.OpenPositions["AAVEUSDT"]
	if !exists {
		t.Fatalf("expected OpenPosition to be created immediately in MARKET mode")
	}
	if !pos.Qty.Equal(decimal.NewFromFloat(2.0)) {
		t.Errorf("expected pos Qty 2.0, got %s", pos.Qty)
	}
	if !pos.EntryPrice.Equal(decimal.NewFromFloat(153.0)) {
		t.Errorf("expected pos EntryPrice 153.0, got %s", pos.EntryPrice)
	}
	if pos.SignalAuditID != 44 {
		t.Errorf("expected SignalAuditID 44, got %d", pos.SignalAuditID)
	}

	// Taker fee: 153.0 * 2.0 * 0.0005 = 0.153 USDT
	expectedFee := decimal.NewFromFloat(153.0 * 2.0 * 0.0005)
	expectedEquity := initialEquity.Sub(expectedFee)
	if !broker.RealizedEquityUSDT.Equal(expectedEquity) {
		t.Errorf("expected RealizedEquityUSDT %s, got %s", expectedEquity, broker.RealizedEquityUSDT)
	}
}

func TestDryRunMAEMFETrackingAndClientOrderID(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		AccountLeverage:     5.0,
		TakerFeeRate:        0.0005,
		EntryOrderType:      "MARKET",
	}
	broker := NewDryRunBroker(decimal.NewFromFloat(10000.0), cfg)
	now := time.Now().UTC()

	var closedRec TradeClosedRecord
	closedChan := make(chan struct{}, 1)
	broker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedRec = rec
		closedChan <- struct{}{}
	}

	origCID := "ORIG_INTENT_CID_999888"
	intent := model.OrderIntent{
		ClientOrderID: origCID,
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0), // Entry = 100
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
		CreatedAt:     now,
		SignalAuditID: 77,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed to submit limit entry: %v", err)
	}

	// 1. Tick at 98.0: Adverse move = (100 - 98) / 100 = 2.0%
	broker.OnRealtimeTick("BTCUSDT", 97.9, 98.1, 98.0, 98.0, 0.1, false, now.Add(1*time.Second))

	// 2. Tick at 105.0: Favorable move = (105 - 100) / 100 = 5.0%
	broker.OnRealtimeTick("BTCUSDT", 104.9, 105.1, 105.0, 105.0, 0.1, false, now.Add(2*time.Second))

	// 3. Tick at 110.0: Hits Take Profit -> Favorable move = 10.0%
	broker.OnRealtimeTick("BTCUSDT", 109.9, 110.1, 110.0, 110.0, 0.1, false, now.Add(3*time.Second))

	select {
	case <-closedChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for trade closed notification")
	}

	// Verify original ClientOrderID was preserved
	if closedRec.ClientOrderID != origCID {
		t.Errorf("expected preserved ClientOrderID %s, got %s", origCID, closedRec.ClientOrderID)
	}

	// Verify MAE and MFE
	if closedRec.MAE < 1.99 || closedRec.MAE > 2.01 {
		t.Errorf("expected MAE around 2.0%%, got %.4f%%", closedRec.MAE)
	}
	if closedRec.MFE < 9.99 || closedRec.MFE > 10.01 {
		t.Errorf("expected MFE around 10.0%%, got %.4f%%", closedRec.MFE)
	}
	if closedRec.ExitReason != "TAKE_PROFIT" {
		t.Errorf("expected ExitReason TAKE_PROFIT, got %s", closedRec.ExitReason)
	}
	if closedRec.SignalAuditID != 77 {
		t.Errorf("expected SignalAuditID 77, got %d", closedRec.SignalAuditID)
	}
}

func TestLiveBrokerEntryAndExitPriceAndPnLCapture(t *testing.T) {
	liveBroker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		MaxAccountStaleness: 60 * time.Second,
		LimitOrderTimeout:   45 * time.Second,
	})
	now := time.Now().UTC()

	var closedRec TradeClosedRecord
	closedChan := make(chan struct{}, 1)
	liveBroker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedRec = rec
		closedChan <- struct{}{}
	}

	liveBroker.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		return 111222, nil
	}
	liveBroker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			return 333444, nil
		}
		return 555666, nil
	}
	liveBroker.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		return nil
	}

	// 1. Account update awal
	liveBroker.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	// 2. Submit Entry order
	intent := model.OrderIntent{
		ClientOrderID: "LIVE_ENTRY_BTC",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(60000.0),
		SLPrice:       decimal.NewFromFloat(58000.0),
		TPPrice:       decimal.NewFromFloat(64000.0),
		ATRAtEntry:    decimal.NewFromFloat(1000.0),
		CreatedAt:     now,
		SignalAuditID: 88,
	}
	ok, err := liveBroker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed to submit entry: %v", err)
	}

	// 3. Entry filled at avgPrice = 60050.0
	liveBroker.HandleUserDataOrderUpdate("LIVE_ENTRY_BTC", 111222, "BTCUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(60050.0), decimal.Zero)
	time.Sleep(50 * time.Millisecond)

	// Verify PositionEntryPrices was stored
	liveBroker.Mu.Lock()
	entryP, hasEP := liveBroker.PositionEntryPrices["BTCUSDT"]
	liveBroker.Mu.Unlock()
	if !hasEP || !entryP.Equal(decimal.NewFromFloat(60050.0)) {
		t.Fatalf("expected PositionEntryPrices to be 60050.0, got %s", entryP)
	}

	// 4. Exit via TP filled at avgPrice = 64100.0 with realizedProfit = 4050.0
	liveBroker.HandleUserDataOrderUpdate("TP_ALGO_BTC", 555666, "BTCUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(64100.0), decimal.NewFromFloat(4050.0))

	select {
	case <-closedChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for OnTradeClosedFn")
	}

	// Verify EntryPrice, ExitPrice, and NetPnL were correctly recorded
	if !closedRec.EntryPrice.Equal(decimal.NewFromFloat(60050.0)) {
		t.Errorf("expected EntryPrice 60050.0, got %s", closedRec.EntryPrice)
	}
	if !closedRec.ExitPrice.Equal(decimal.NewFromFloat(64100.0)) {
		t.Errorf("expected ExitPrice 64100.0, got %s", closedRec.ExitPrice)
	}
	if !closedRec.NetPnL.Equal(decimal.NewFromFloat(4050.0)) {
		t.Errorf("expected NetPnL 4050.0, got %s", closedRec.NetPnL)
	}
	if closedRec.ExitReason != "TAKE_PROFIT" {
		t.Errorf("expected ExitReason TAKE_PROFIT, got %s", closedRec.ExitReason)
	}
	if closedRec.SignalAuditID != 88 {
		t.Errorf("expected SignalAuditID 88, got %d", closedRec.SignalAuditID)
	}
}

func TestDryRunBrokerPersistence_FIFOOrdering(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:            3,
		AccountLeverage:     5.0,
		EntryOrderType:      "MARKET",
		Tier1BEEnabled:      true,
		BreakEvenTriggerATR: 1.0,
		TakerFeeRate:        0.0005,
	}
	b := NewDryRunBroker(decimal.NewFromFloat(1000.0), cfg)
	defer b.Close()

	events := make([]PositionEvent, 0)
	doneChan := make(chan struct{}, 10)

	b.OnPositionSavedFn = func(pos VirtualOpenPosition) {
		events = append(events, PositionEvent{Type: PositionEventSave, Pos: pos})
		doneChan <- struct{}{}
	}
	b.OnPositionDeletedFn = func(idOrSym string) {
		events = append(events, PositionEvent{Type: PositionEventDelete, ClientOrderIDOrSymbol: idOrSym})
		doneChan <- struct{}{}
	}

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "ORDER_FIFO_1",
		Symbol:        "NEARUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromInt(10),
		LimitPrice:    decimal.NewFromFloat(5.0),
		SLPrice:       decimal.NewFromFloat(4.5),
		TPPrice:       decimal.NewFromFloat(6.0),
		ATRAtEntry:    decimal.NewFromFloat(0.5),
		CreatedAt:     now,
		SignalAuditID: 4369,
	}

	// 1. Submit Market Entry -> triggers PositionEventSave
	ok, err := b.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("submit market entry failed: %v", err)
	}

	// Wait for event 1
	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for save event")
	}

	// 2. Trigger Break-Even -> triggers PositionEventSave (update)
	// Entry 5.0, ATR 0.5, BreakEvenTrigger 1.0 -> trigger dist 0.5 -> markPrice 5.6 triggers BE
	b.OnRealtimeTick("NEARUSDT", 5.6, 5.61, 5.6, 5.6, 1.0, true, now.Add(time.Second))

	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for break-even save event")
	}

	// 3. Trigger Take Profit -> triggers PositionEventDelete
	b.OnRealtimeTick("NEARUSDT", 6.0, 6.01, 6.0, 6.0, 1.0, true, now.Add(2*time.Second))

	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for delete event")
	}

	// Verify exact FIFO event sequence: [Save (open), Save (be moved), Delete (tp hit)]
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Type != PositionEventSave || events[0].Pos.ClientOrderID != "ORDER_FIFO_1" || events[0].Pos.IsBreakEvenMoved {
		t.Errorf("event 0 mismatch: %+v", events[0])
	}
	if events[1].Type != PositionEventSave || !events[1].Pos.IsBreakEvenMoved {
		t.Errorf("event 1 mismatch: expected break-even true, got %+v", events[1])
	}
	if events[2].Type != PositionEventDelete || events[2].ClientOrderIDOrSymbol != "ORDER_FIFO_1" {
		t.Errorf("event 2 mismatch: expected delete ORDER_FIFO_1, got %+v", events[2])
	}
}

func TestDryRunBrokerPersistence_RestoreAccounting(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:        3,
		AccountLeverage: 5.0,
		TakerFeeRate:    0.0005,
	}
	b := NewDryRunBroker(decimal.NewFromFloat(100.0), cfg)
	defer b.Close()

	now := time.Now().UTC()
	pos := &VirtualOpenPosition{
		ClientOrderID:    "ORDER_RESTORE_ACCT",
		Symbol:           "NEARUSDT",
		Side:             model.SideLong,
		Qty:              decimal.NewFromInt(10),
		EntryPrice:       decimal.NewFromFloat(10.0), // notional = 100
		EntryFee:         decimal.NewFromFloat(0.05), // entryFee = 0.05
		CurrentSLPrice:   decimal.NewFromFloat(9.0),
		TPPrice:          decimal.NewFromFloat(12.0),
		ATRAtEntry:       decimal.NewFromFloat(0.5),
		LastMarkPrice:    decimal.NewFromFloat(10.0),
		MarkUpdatedAt:    now,
		OpenTime:         now,
		IsBreakEvenMoved: false,
		SignalAuditID:    100,
	}

	// Restore position: RealizedEquityUSDT must be debited by pos.EntryFee ($0.05)
	b.RestoreOpenPosition(pos)
	if !b.RealizedEquityUSDT.Equal(decimal.NewFromFloat(99.95)) {
		t.Errorf("expected RealizedEquity 99.95, got %s", b.RealizedEquityUSDT)
	}

	// Idempotent test: calling RestoreOpenPosition again for same symbol must NOT double-deduct
	b.RestoreOpenPosition(pos)
	if !b.RealizedEquityUSDT.Equal(decimal.NewFromFloat(99.95)) {
		t.Errorf("expected RealizedEquity to remain 99.95 on duplicate restore, got %s", b.RealizedEquityUSDT)
	}

	// Close position with TP at 12.0 (Gross PnL = (12 - 10) * 10 = $20, Exit Fee = 12 * 10 * 0.0005 = $0.06)
	// Addition to RealizedEquity = grossPnL - exitFee = $20 - $0.06 = $19.94
	// Final RealizedEquity = $99.95 + $19.94 = $119.89
	// Net PnL = $20 - $0.05 - $0.06 = $19.89
	// Start Equity $100 + Net PnL $19.89 = $119.89!
	b.closePositionLocked(pos, decimal.NewFromFloat(12.0), 0.0005, "TAKE_PROFIT")

	if !b.RealizedEquityUSDT.Equal(decimal.NewFromFloat(119.89)) {
		t.Errorf("accounting discrepancy: expected 119.89, got %s", b.RealizedEquityUSDT)
	}
}

func TestDryRunBrokerPersistence_PeriodicCheckpoint(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:       3,
		EntryOrderType: "MARKET",
	}
	b := NewDryRunBroker(decimal.NewFromFloat(500.0), cfg)
	defer b.Close()

	savedPositions := make([]VirtualOpenPosition, 0)
	saveChan := make(chan struct{}, 10)

	b.OnPositionSavedFn = func(pos VirtualOpenPosition) {
		savedPositions = append(savedPositions, pos)
		saveChan <- struct{}{}
	}

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "ORDER_CHECKPOINT",
		Symbol:        "NEARUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromInt(10),
		LimitPrice:    decimal.NewFromFloat(5.0),
		SLPrice:       decimal.NewFromFloat(5.5),
		TPPrice:       decimal.NewFromFloat(4.0),
		ATRAtEntry:    decimal.NewFromFloat(0.2),
		CreatedAt:     now,
	}

	_, _ = b.SubmitLimitEntry(intent)

	// Consume initial save event
	select {
	case <-saveChan:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for initial save")
	}

	// Price moves in our favor to 4.80 (MFE = (5 - 4.8)/5 * 100 = 4%)
	b.OnRealtimeTick("NEARUSDT", 4.79, 4.80, 4.80, 4.80, 1.0, true, now.Add(time.Second))

	// Calling CheckTimeouts within 1s should NOT trigger periodic checkpoint yet (interval 5s)
	b.CheckTimeouts(now.Add(time.Second))
	select {
	case <-saveChan:
		t.Fatalf("unexpected save event before 5s checkpoint interval")
	case <-time.After(100 * time.Millisecond):
	}

	// Calling CheckTimeouts after 6s SHOULD trigger periodic checkpoint
	b.CheckTimeouts(now.Add(6 * time.Second))
	select {
	case <-saveChan:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for periodic checkpoint save event")
	}

	lastSaved := savedPositions[len(savedPositions)-1]
	if !lastSaved.LastMarkPrice.Equal(decimal.NewFromFloat(4.80)) {
		t.Errorf("expected LastMarkPrice 4.80, got %s", lastSaved.LastMarkPrice)
	}
	if lastSaved.MaxMFE <= 0 {
		t.Errorf("expected positive MaxMFE, got %f", lastSaved.MaxMFE)
	}
}

func TestLiveBrokerPersistence_RestoreAndCallbacks(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 10 * time.Second,
	}
	broker := NewBinanceLiveBroker(cfg)

	// 1. Test RestoreOpenPosition
	now := time.Now().UTC()
	posToRestore := &LiveOpenPosition{
		Symbol:         "ETHUSDT",
		Side:           model.SideShort,
		Qty:            decimal.NewFromFloat(1.5),
		EntryPrice:     decimal.NewFromFloat(3000.0),
		SLOrderID:      111222,
		SLClientAlgoID: "ALGO_SL_ETHUSDT_1",
		SLPrice:        decimal.NewFromFloat(3050.0),
		TPOrderID:      333444,
		TPClientAlgoID: "ALGO_TP_ETHUSDT_1",
		TPPrice:        decimal.NewFromFloat(2900.0),
		SignalAuditID:  555,
		OpenTime:       now,
		MaxMAE:         0.15,
		MaxMFE:         0.45,
		UpdatedAt:      now,
	}

	broker.RestoreOpenPosition(posToRestore)

	dtos := broker.GetActivePositions()
	if len(dtos) != 1 {
		t.Fatalf("expected 1 restored position in DTO, got %d", len(dtos))
	}
	if dtos[0].Symbol != "ETHUSDT" || dtos[0].Side != "SHORT" || dtos[0].EntryPrice != 3000.0 {
		t.Fatalf("unexpected DTO: %+v", dtos[0])
	}

	// 2. Test OnPositionDeletedFn callback on trade close
	deletedChan := make(chan string, 1)
	broker.OnPositionDeletedFn = func(sym string) {
		deletedChan <- sym
	}

	// Simulate SL execution report
	broker.HandleUserDataOrderUpdate(
		"ALGO_SL_ETHUSDT_1",
		111222,
		"ETHUSDT",
		model.StatusFilled,
		decimal.NewFromFloat(1.5),
		decimal.NewFromFloat(3050.0),
		decimal.NewFromFloat(-75.0),
	)

	select {
	case deletedSym := <-deletedChan:
		if deletedSym != "ETHUSDT" {
			t.Fatalf("expected deleted symbol ETHUSDT, got %s", deletedSym)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for OnPositionDeletedFn callback")
	}

	// Verify position is removed from in-memory maps
	if len(broker.GetActivePositions()) != 0 {
		t.Fatalf("expected 0 positions after close, got %d", len(broker.GetActivePositions()))
	}
}

func TestDryRunBroker_TieredDynamicBreakEvenAndProfitLock(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:                 3,
		AccountLeverage:          5.0,
		EntryOrderType:           "MARKET",
		Tier1BEEnabled:           true,
		Tier1BETriggerLongATR:    0.70,
		Tier1BETriggerShortATR:   0.90,
		Tier2ProfitLockATR:       1.40,
		Tier2ProfitLockOffsetATR: 0.70,
		MinBreakEvenOffsetPct:    0.0010,
		MakerFeeRate:             0.0002,
		TakerFeeRate:             0.0005,
		SlippageRate:             0.0,
	}

	now := time.Now().UTC()

	// 1. UJI TIER 1 BREAK-EVEN (LONG)
	b1 := NewDryRunBroker(decimal.NewFromFloat(1000.0), cfg)
	defer b1.Close()

	closedChan1 := make(chan string, 1)
	b1.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedChan1 <- rec.ExitReason
	}

	intentLong := model.OrderIntent{
		ClientOrderID: "ORDER_T1_LONG",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(97.0),
		TPPrice:       decimal.NewFromFloat(106.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
	}
	_, _ = b1.SubmitLimitEntry(intentLong)

	// Price naik ke 101.40 (+1.40 = +0.70x ATR) -> Memicu Tier 1 Break-Even
	b1.OnRealtimeTick("SOLUSDT", 101.40, 101.41, 101.40, 101.40, 1.0, true, now.Add(time.Second))

	b1.Mu.Lock()
	pos1 := b1.OpenPositions["SOLUSDT"]
	if pos1.ProtectionTier != 1 || !pos1.IsBreakEvenMoved {
		t.Fatalf("expected ProtectionTier=1 and IsBreakEvenMoved=true, got tier=%d moved=%v", pos1.ProtectionTier, pos1.IsBreakEvenMoved)
	}
	// SL harus berada di atas entry: 100 * (1 + 0.0010) = 100.10
	if pos1.CurrentSLPrice.LessThan(decimal.NewFromFloat(100.0)) {
		t.Fatalf("expected CurrentSLPrice > 100.0, got %s", pos1.CurrentSLPrice)
	}
	b1.Mu.Unlock()

	// Price berbalik turun menyentuh SL 100.10
	b1.OnRealtimeTick("SOLUSDT", 100.05, 100.06, 100.05, 100.05, 1.0, true, now.Add(2*time.Second))
	select {
	case exitReason1 := <-closedChan1:
		if exitReason1 != "BREAK_EVEN" {
			t.Fatalf("expected exitReason BREAK_EVEN, got %s", exitReason1)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for trade closed callback")
	}

	// 2. UJI TIER 2 PROFIT LOCK (LONG)
	b2 := NewDryRunBroker(decimal.NewFromFloat(1000.0), cfg)
	defer b2.Close()

	closedChan2 := make(chan string, 1)
	b2.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedChan2 <- rec.ExitReason
	}

	intentLong2 := model.OrderIntent{
		ClientOrderID: "ORDER_T2_LONG",
		Symbol:        "AVAXUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(97.0),
		TPPrice:       decimal.NewFromFloat(106.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
	}
	_, _ = b2.SubmitLimitEntry(intentLong2)

	// Price naik ke 102.85 (>= 100 + 1.40*2.0 = 102.80) -> Memicu Tier 2 Profit Lock
	b2.OnRealtimeTick("AVAXUSDT", 102.85, 102.86, 102.85, 102.85, 1.0, true, now.Add(time.Second))

	b2.Mu.Lock()
	pos2 := b2.OpenPositions["AVAXUSDT"]
	if pos2.ProtectionTier != 2 {
		t.Fatalf("expected ProtectionTier=2, got %d", pos2.ProtectionTier)
	}
	// SL harus terkunci di Entry + 0.70x ATR = 100.0 + 1.40 = 101.40
	expectedSL := decimal.NewFromFloat(101.40)
	if !pos2.CurrentSLPrice.Equal(expectedSL) {
		t.Fatalf("expected CurrentSLPrice=%s, got %s", expectedSL, pos2.CurrentSLPrice)
	}
	b2.Mu.Unlock()

	// Price berbalik turun menyentuh Profit Lock 101.40
	b2.OnRealtimeTick("AVAXUSDT", 101.35, 101.36, 101.35, 101.35, 1.0, true, now.Add(2*time.Second))
	select {
	case exitReason2 := <-closedChan2:
		if exitReason2 != "PROFIT_LOCK" {
			t.Fatalf("expected exitReason PROFIT_LOCK, got %s", exitReason2)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for trade closed callback")
	}

	// 3. UJI TIER 2 PROFIT LOCK (SHORT)
	b3 := NewDryRunBroker(decimal.NewFromFloat(1000.0), cfg)
	defer b3.Close()

	closedChan3 := make(chan string, 1)
	b3.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedChan3 <- rec.ExitReason
	}

	intentShort := model.OrderIntent{
		ClientOrderID: "ORDER_T2_SHORT",
		Symbol:        "LINKUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(103.0),
		TPPrice:       decimal.NewFromFloat(94.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
	}
	_, _ = b3.SubmitLimitEntry(intentShort)

	// Price turun ke 97.10 (<= 100 - 1.40*2.0 = 97.20) -> Memicu Tier 2 Profit Lock
	b3.OnRealtimeTick("LINKUSDT", 97.10, 97.11, 97.10, 97.10, 1.0, true, now.Add(time.Second))

	b3.Mu.Lock()
	pos3 := b3.OpenPositions["LINKUSDT"]
	if pos3.ProtectionTier != 2 {
		t.Fatalf("expected ProtectionTier=2, got %d", pos3.ProtectionTier)
	}
	// SL harus terkunci di Entry - 0.70x ATR = 100.0 - 1.40 = 98.60
	expectedSLShort := decimal.NewFromFloat(98.60)
	if !pos3.CurrentSLPrice.Equal(expectedSLShort) {
		t.Fatalf("expected CurrentSLPrice=%s, got %s", expectedSLShort, pos3.CurrentSLPrice)
	}
	b3.Mu.Unlock()

	// Price memantul naik menyentuh Profit Lock 98.60
	b3.OnRealtimeTick("LINKUSDT", 98.65, 98.66, 98.65, 98.65, 1.0, true, now.Add(2*time.Second))
	select {
	case exitReason3 := <-closedChan3:
		if exitReason3 != "PROFIT_LOCK" {
			t.Fatalf("expected exitReason PROFIT_LOCK, got %s", exitReason3)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for trade closed callback")
	}
}

func TestDryRunBroker_PartialTakeProfit_ScaleOut(t *testing.T) {
	now := time.Now().UTC()
	broker := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:              3,
		AccountLeverage:       5.0,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		PartialTP1ATRMult:     1.20,
		MinBreakEvenOffsetPct: 0.0010,
		EntryOrderType:        "MARKET",
		TakerFeeRate:          0.0005,
		MakerFeeRate:          0.0002,
		SlippageRate:          0.0001,
	})
	defer broker.Close()

	closedTrades := make(chan TradeClosedRecord, 5)
	broker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTrades <- rec
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST-SCALE-OUT-001",
		Symbol:        "ADAUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(100.0),
		LimitPrice:    decimal.NewFromFloat(1.00),
		SLPrice:       decimal.NewFromFloat(0.985),  // SL 1.5%
		TP1Price:      decimal.NewFromFloat(1.012),  // TP1 +1.2%
		TPPrice:       decimal.NewFromFloat(1.0225), // TP2 +2.25%
		ATRAtEntry:    decimal.NewFromFloat(0.010),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected entry to succeed, got ok=%v, err=%v", ok, err)
	}

	// 1. Harga naik menyentuh TP1 (1.0125 >= 1.012)
	broker.OnRealtimeTick("ADAUSDT", 1.0124, 1.0126, 1.0125, 1.0125, 50.0, false, now.Add(1*time.Minute))

	// Verifikasi trade partial take profit
	select {
	case tp1Rec := <-closedTrades:
		if tp1Rec.ExitReason != "PARTIAL_TAKE_PROFIT" {
			t.Errorf("expected exit reason PARTIAL_TAKE_PROFIT, got %s", tp1Rec.ExitReason)
		}
		if !tp1Rec.Qty.Equal(decimal.NewFromFloat(50.0)) {
			t.Errorf("expected closed qty 50.0, got %s", tp1Rec.Qty)
		}
		if tp1Rec.NetPnL.LessThanOrEqual(decimal.Zero) {
			t.Errorf("expected positive netPnL on TP1, got %s", tp1Rec.NetPnL)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for TP1 partial trade closed record")
	}

	// Verifikasi state posisi in-memory: kuantitas tersisa separuh dan SL sudah di Break-Even
	broker.Mu.Lock()
	pos, exists := broker.OpenPositions["ADAUSDT"]
	if !exists {
		broker.Mu.Unlock()
		t.Fatalf("expected position to still exist with remaining runner")
	}
	if !pos.Qty.Equal(decimal.NewFromFloat(50.0)) {
		t.Errorf("expected remaining qty 50.0, got %s", pos.Qty)
	}
	if !pos.IsTP1Hit {
		t.Errorf("expected pos.IsTP1Hit to be true")
	}
	// SL harus sudah dipindahkan ke Break-Even: 1.00 * (1 + 0.0011 fee buffer) = 1.0011
	expectedBESL := decimal.NewFromFloat(1.0011)
	if !pos.CurrentSLPrice.Equal(expectedBESL) {
		t.Errorf("expected BE SL %s, got %s", expectedBESL, pos.CurrentSLPrice)
	}
	broker.Mu.Unlock()

	// 2. Harga retrace tajam turun ke 1.0005 (menyentuh SL Break-Even 1.0010)
	broker.OnRealtimeTick("ADAUSDT", 1.0004, 1.0006, 1.0005, 1.0005, 50.0, false, now.Add(2*time.Minute))

	// Verifikasi sisa posisi ditutup dengan BREAK_EVEN
	select {
	case beRec := <-closedTrades:
		if beRec.ExitReason != "BREAK_EVEN" {
			t.Errorf("expected exit reason BREAK_EVEN for runner retrace, got %s", beRec.ExitReason)
		}
		if !beRec.Qty.Equal(decimal.NewFromFloat(50.0)) {
			t.Errorf("expected closed runner qty 50.0, got %s", beRec.Qty)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for runner trade closed record")
	}

	// Posisi sekarang harus sudah sepenuhnya tertutup
	broker.Mu.Lock()
	if len(broker.OpenPositions) != 0 {
		t.Errorf("expected 0 open positions, got %d", len(broker.OpenPositions))
	}
	// Total realized equity harus lebih besar dari initial equity ($1000) karena TP1 cuan riil!
	if broker.RealizedEquityUSDT.LessThanOrEqual(decimal.NewFromFloat(1000.0)) {
		t.Errorf("expected total realized equity > 1000.0, got %s", broker.RealizedEquityUSDT)
	}
	broker.Mu.Unlock()
}

func TestDryRunBroker_PartialTP1_MinNotionalFallback(t *testing.T) {
	now := time.Now().UTC()
	broker := NewDryRunBroker(decimal.NewFromFloat(100.0), DryRunConfig{
		MaxSlots:              3,
		AccountLeverage:       5.0,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		PartialTP1ATRMult:     1.20,
		MinBreakEvenOffsetPct: 0.0010,
		EntryOrderType:        "MARKET",
		TakerFeeRate:          0.0005,
		MakerFeeRate:          0.0002,
		SlippageRate:          0.0001,
	})
	defer broker.Close()

	closedTrades := make(chan TradeClosedRecord, 5)
	broker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTrades <- rec
	}

	// Posisi kecil: Qty = 4.0 @ $1.00 = 4.0 USDT notional (< 5 USDT MinNotional)
	intent := model.OrderIntent{
		ClientOrderID: "TEST-SMALL-NOTIONAL-001",
		Symbol:        "ADAUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(4.0),
		LimitPrice:    decimal.NewFromFloat(1.00),
		SLPrice:       decimal.NewFromFloat(0.985),
		TP1Price:      decimal.NewFromFloat(1.012),
		TPPrice:       decimal.NewFromFloat(1.0225),
		ATRAtEntry:    decimal.NewFromFloat(0.010),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected entry to succeed, got ok=%v, err=%v", ok, err)
	}

	// Harga naik menyentuh TP1 (1.0125 >= 1.012)
	broker.OnRealtimeTick("ADAUSDT", 1.0124, 1.0126, 1.0125, 1.0125, 50.0, false, now.Add(1*time.Minute))

	// Tidak boleh ada penutupan parsial di closedTrades karena nilai < 5 USDT
	select {
	case rec := <-closedTrades:
		t.Fatalf("unexpected trade closed record: %+v", rec)
	default:
	}

	// Verifikasi: pos.IsTP1Hit harus TRUE, SL digeser ke BE, dan Qty tetap 100% (4.0)
	broker.Mu.Lock()
	pos, exists := broker.OpenPositions["ADAUSDT"]
	if !exists {
		broker.Mu.Unlock()
		t.Fatalf("expected position to exist")
	}
	if !pos.IsTP1Hit {
		t.Errorf("expected pos.IsTP1Hit to be true")
	}
	if !pos.IsBreakEvenMoved {
		t.Errorf("expected pos.IsBreakEvenMoved to be true")
	}
	if pos.ProtectionTier != 1 {
		t.Errorf("expected pos.ProtectionTier to be 1, got %d", pos.ProtectionTier)
	}
	if !pos.Qty.Equal(decimal.NewFromFloat(4.0)) {
		t.Errorf("expected 100%% qty preserved (4.0), got %s", pos.Qty)
	}
	if pos.CurrentSLPrice.LessThanOrEqual(decimal.NewFromFloat(1.00)) {
		t.Errorf("expected CurrentSLPrice to be moved above entry price (1.00), got %s", pos.CurrentSLPrice)
	}
	broker.Mu.Unlock()

	// Tick kedua: tidak boleh looping atau merusak state
	broker.OnRealtimeTick("ADAUSDT", 1.0130, 1.0132, 1.0131, 1.0131, 50.0, false, now.Add(2*time.Minute))
	select {
	case rec := <-closedTrades:
		t.Fatalf("unexpected trade closed on second tick: %+v", rec)
	default:
	}
}

func TestDryRunBrokerRunnerTrailingStop(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:                 3,
		MaxDailyDrawdownPct:      -0.03,
		AccountLeverage:          5.0,
		PartialTPEnabled:         true,
		PartialTP1Ratio:          0.50,
		PartialTP1ATRMult:        1.20,
		TrailingEnabled:          true,
		TrailingActivateATR:      2.00,
		TrailingDistanceATR:      0.80,
		Tier2ProfitLockATR:       1.80,
		Tier2ProfitLockOffsetATR: 0.90,
		Tier1BEEnabled:           false,
		MinBreakEvenOffsetPct:    0.0010,
		EntryOrderType:           "MARKET",
		MakerFeeRate:             0.0002,
		TakerFeeRate:             0.0005,
		SlippageRate:             0.0001,
	}
	broker := NewDryRunBroker(decimal.NewFromFloat(10000.0), cfg)
	defer broker.Close()

	closedTrades := make(chan TradeClosedRecord, 10)
	broker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTrades <- rec
	}

	now := time.Now().UTC()

	// 1. Long Entry di 100.0, ATR = 10.0
	// TP1 = 100 + 1.20*10 = 112.0
	// Fixed TP = 100 + 2.25*10 = 122.5
	// Trailing Activate at Peak >= 100 + 2.0*10 = 120.0
	// Trailing Distance = 0.80*10 = 8.0
	intent := model.OrderIntent{
		ClientOrderID: "TEST_TRAIL_LONG",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(90.0),
		TPPrice:       decimal.NewFromFloat(122.5),
		TP1Price:      decimal.NewFromFloat(112.0),
		ATRAtEntry:    decimal.NewFromFloat(10.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed to submit market entry: %v", err)
	}

	// 2. Harga naik menyentuh TP1 (112.0)
	broker.OnRealtimeTick("SOLUSDT", 112.0, 112.1, 112.0, 112.0, 5.0, false, now.Add(1*time.Minute))

	select {
	case tp1Rec := <-closedTrades:
		if tp1Rec.ExitReason != "PARTIAL_TAKE_PROFIT" {
			t.Errorf("expected exit reason PARTIAL_TAKE_PROFIT, got %s", tp1Rec.ExitReason)
		}
		if !tp1Rec.Qty.Equal(decimal.NewFromFloat(5.0)) {
			t.Errorf("expected closed qty 5.0, got %s", tp1Rec.Qty)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for TP1 record")
	}

	// 3. Harga melesat melampaui fixed TP2 (122.5) menuju 130.0!
	// Dengan TrailingEnabled=true, runner TIDAK tertutup di 122.5, melainkan terus berjalan!
	broker.OnRealtimeTick("SOLUSDT", 130.0, 130.1, 130.0, 130.0, 5.0, false, now.Add(2*time.Minute))

	broker.Mu.Lock()
	pos, exists := broker.OpenPositions["SOLUSDT"]
	if !exists {
		broker.Mu.Unlock()
		t.Fatalf("expected runner to stay open beyond fixed TP2")
	}
	// SL harus sudah trailing: Peak = 130.0 -> SL = 130.0 - 8.0 = 122.0
	expectedSL := decimal.NewFromFloat(122.0)
	if !pos.CurrentSLPrice.Equal(expectedSL) {
		t.Errorf("expected trailing SL %s, got %s", expectedSL, pos.CurrentSLPrice)
	}
	if pos.ProtectionTier != 3 {
		t.Errorf("expected ProtectionTier 3 (TRAILING), got %d", pos.ProtectionTier)
	}
	broker.Mu.Unlock()

	// 4. Harga melesat lagi ke 140.0 (+4.0 ATR extension)
	broker.OnRealtimeTick("SOLUSDT", 140.0, 140.1, 140.0, 140.0, 5.0, false, now.Add(3*time.Minute))

	broker.Mu.Lock()
	pos = broker.OpenPositions["SOLUSDT"]
	// New SL = 140.0 - 8.0 = 132.0
	expectedSL2 := decimal.NewFromFloat(132.0)
	if !pos.CurrentSLPrice.Equal(expectedSL2) {
		t.Errorf("expected trailing SL %s, got %s", expectedSL2, pos.CurrentSLPrice)
	}
	broker.Mu.Unlock()

	// 5. Harga retrace dari peak 140.0 turun ke 131.9 (menembus SL 132.0)
	broker.OnRealtimeTick("SOLUSDT", 131.8, 132.0, 131.9, 131.9, 5.0, false, now.Add(4*time.Minute))

	select {
	case trailRec := <-closedTrades:
		if trailRec.ExitReason != "TRAILING_STOP" {
			t.Errorf("expected exit reason TRAILING_STOP, got %s", trailRec.ExitReason)
		}
		if !trailRec.Qty.Equal(decimal.NewFromFloat(5.0)) {
			t.Errorf("expected closed runner qty 5.0, got %s", trailRec.Qty)
		}
		// Exit price harus di sekitar 132.0 (minus slippage)
		if trailRec.ExitPrice.LessThan(decimal.NewFromFloat(131.0)) {
			t.Errorf("expected exit price near 132.0, got %s", trailRec.ExitPrice)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for TRAILING_STOP record")
	}

	broker.Mu.Lock()
	if len(broker.OpenPositions) != 0 {
		t.Errorf("expected 0 open positions, got %d", len(broker.OpenPositions))
	}
	broker.Mu.Unlock()
}

func TestDryRunBrokerRunnerTrailingStop_Short(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:                 3,
		MaxDailyDrawdownPct:      -0.03,
		AccountLeverage:          5.0,
		PartialTPEnabled:         true,
		PartialTP1Ratio:          0.50,
		PartialTP1ATRMult:        1.20,
		TrailingEnabled:          true,
		TrailingActivateATR:      2.00,
		TrailingDistanceATR:      0.80,
		Tier2ProfitLockATR:       1.80,
		Tier2ProfitLockOffsetATR: 0.90,
		Tier1BEEnabled:           false,
		MinBreakEvenOffsetPct:    0.0010,
		EntryOrderType:           "MARKET",
		MakerFeeRate:             0.0002,
		TakerFeeRate:             0.0005,
		SlippageRate:             0.0001,
	}
	broker := NewDryRunBroker(decimal.NewFromFloat(10000.0), cfg)
	defer broker.Close()

	closedTrades := make(chan TradeClosedRecord, 10)
	broker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTrades <- rec
	}

	now := time.Now().UTC()

	// Short Entry di 100.0, ATR = 10.0
	// TP1 = 100 - 1.20*10 = 88.0
	// Fixed TP = 100 - 2.25*10 = 77.5
	// Trailing Activate at Peak <= 100 - 2.0*10 = 80.0
	// Trailing Distance = 0.80*10 = 8.0
	intent := model.OrderIntent{
		ClientOrderID: "TEST_TRAIL_SHORT",
		Symbol:        "BNBUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(110.0),
		TPPrice:       decimal.NewFromFloat(77.5),
		TP1Price:      decimal.NewFromFloat(88.0),
		ATRAtEntry:    decimal.NewFromFloat(10.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("failed to submit short market entry: %v", err)
	}

	// 1. Harga turun menyentuh TP1 (88.0)
	broker.OnRealtimeTick("BNBUSDT", 87.9, 88.0, 88.0, 88.0, 5.0, false, now.Add(1*time.Minute))

	select {
	case tp1Rec := <-closedTrades:
		if tp1Rec.ExitReason != "PARTIAL_TAKE_PROFIT" {
			t.Errorf("expected exit reason PARTIAL_TAKE_PROFIT, got %s", tp1Rec.ExitReason)
		}
		if !tp1Rec.Qty.Equal(decimal.NewFromFloat(5.0)) {
			t.Errorf("expected closed qty 5.0, got %s", tp1Rec.Qty)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for TP1 record")
	}

	// 2. Harga dump melampaui fixed TP2 (77.5) menuju 70.0!
	broker.OnRealtimeTick("BNBUSDT", 69.9, 70.1, 70.0, 70.0, 5.0, false, now.Add(2*time.Minute))

	broker.Mu.Lock()
	pos, exists := broker.OpenPositions["BNBUSDT"]
	if !exists {
		broker.Mu.Unlock()
		t.Fatalf("expected short runner to stay open beyond fixed TP2")
	}
	// SL harus sudah trailing: Peak = 70.0 -> SL = 70.0 + 8.0 = 78.0
	expectedSL := decimal.NewFromFloat(78.0)
	if !pos.CurrentSLPrice.Equal(expectedSL) {
		t.Errorf("expected trailing SL %s, got %s", expectedSL, pos.CurrentSLPrice)
	}
	if pos.ProtectionTier != 3 {
		t.Errorf("expected ProtectionTier 3 (TRAILING), got %d", pos.ProtectionTier)
	}
	broker.Mu.Unlock()

	// 3. Harga dump lagi ke 60.0 (-4.0 ATR extension) -> New SL = 60.0 + 8.0 = 68.0
	broker.OnRealtimeTick("BNBUSDT", 59.9, 60.1, 60.0, 60.0, 5.0, false, now.Add(3*time.Minute))

	broker.Mu.Lock()
	pos = broker.OpenPositions["BNBUSDT"]
	expectedSL2 := decimal.NewFromFloat(68.0)
	if !pos.CurrentSLPrice.Equal(expectedSL2) {
		t.Errorf("expected trailing SL %s, got %s", expectedSL2, pos.CurrentSLPrice)
	}
	broker.Mu.Unlock()

	// 4. Harga pullback bounce dari 60.0 naik ke 68.1 (menembus SL 68.0)
	broker.OnRealtimeTick("BNBUSDT", 68.0, 68.2, 68.1, 68.1, 5.0, false, now.Add(4*time.Minute))

	select {
	case trailRec := <-closedTrades:
		if trailRec.ExitReason != "TRAILING_STOP" {
			t.Errorf("expected exit reason TRAILING_STOP, got %s", trailRec.ExitReason)
		}
		if !trailRec.Qty.Equal(decimal.NewFromFloat(5.0)) {
			t.Errorf("expected closed runner qty 5.0, got %s", trailRec.Qty)
		}
		if trailRec.ExitPrice.GreaterThan(decimal.NewFromFloat(69.0)) {
			t.Errorf("expected exit price near 68.0, got %s", trailRec.ExitPrice)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for TRAILING_STOP record")
	}

	broker.Mu.Lock()
	if len(broker.OpenPositions) != 0 {
		t.Errorf("expected 0 open positions, got %d", len(broker.OpenPositions))
	}
	broker.Mu.Unlock()
}

func TestLiveBrokerPartialTP1AndBE(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:              3,
		MaxDailyDrawdownPct:   -0.05,
		MaxAccountStaleness:   60 * time.Second,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		Tier1BEEnabled:        true,
		MinBreakEvenOffsetPct: 0.0010,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()

	b.HandleAccountUpdate(
		decimal.NewFromFloat(10000.0),
		decimal.Zero,
		decimal.NewFromFloat(10000.0),
		nil,
		now,
	)

	filter := model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01),
		StepSize: decimal.NewFromFloat(0.1),
	}
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return filter, true
	}

	var marketCloseQty decimal.Decimal
	var marketCloseSide model.Side
	var marketCloseCount int32
	b.MarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, error) {
		atomic.AddInt32(&marketCloseCount, 1)
		marketCloseSide = side
		marketCloseQty = qty
		return 777001, nil
	}

	var slMu sync.Mutex
	var slTriggerPrice decimal.Decimal
	var slOrderQty decimal.Decimal
	var slAlgoCount int32
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			slMu.Lock()
			atomic.AddInt32(&slAlgoCount, 1)
			slTriggerPrice = triggerPrice
			slOrderQty = qty
			slMu.Unlock()
			return 888001, nil
		}
		return 0, nil
	}

	closedTrades := make(chan TradeClosedRecord, 5)
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTrades <- rec
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST_LIVE_TP1_LONG",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(2.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		TP1Price:      decimal.NewFromFloat(103.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
		CreatedAt:     now,
	}

	ok, err := b.ReserveEntryIntent(intent)
	if !ok || err != nil {
		t.Fatalf("failed to reserve intent: %v", err)
	}

	// Simulasi fill entry
	b.HandleUserDataOrderUpdate("TEST_LIVE_TP1_LONG", 111001, "BTCUSDT", model.StatusFilled, decimal.NewFromFloat(2.0), decimal.NewFromFloat(100.0), decimal.Zero)

	// Wait for initial entry SL placement to complete and be acked
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&slAlgoCount) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&slAlgoCount) != 1 {
		t.Fatalf("expected initial SL placement count 1, got %d", atomic.LoadInt32(&slAlgoCount))
	}

	// Price ticks below TP1 -> TP1 should NOT trigger
	b.OnRealtimeTick("BTCUSDT", 101.0, 101.1, 101.0, 101.0, 1.0, true, now)
	if atomic.LoadInt32(&marketCloseCount) != 0 {
		t.Fatalf("expected no market close at price 101")
	}

	// Price ticks at or above TP1 (103.0) -> TP1 should trigger!
	b.OnRealtimeTick("BTCUSDT", 103.0, 103.1, 103.0, 103.0, 1.0, true, now)

	// Wait for async market close goroutine
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&marketCloseCount) > 0 && atomic.LoadInt32(&slAlgoCount) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&marketCloseCount) != 1 {
		t.Fatalf("expected 1 market close call, got %d", atomic.LoadInt32(&marketCloseCount))
	}
	if !marketCloseQty.Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("expected 1.0 close qty (50%% of 2.0), got %s", marketCloseQty)
	}
	if marketCloseSide != model.SideShort {
		t.Errorf("expected close side SHORT for long position, got %s", marketCloseSide)
	}

	select {
	case rec := <-closedTrades:
		if rec.ExitReason != "PARTIAL_TAKE_PROFIT" {
			t.Errorf("expected exit reason PARTIAL_TAKE_PROFIT, got %s", rec.ExitReason)
		}
		if !rec.Qty.Equal(decimal.NewFromFloat(1.0)) {
			t.Errorf("expected 1.0 qty, got %s", rec.Qty)
		}
		if !rec.SLPrice.Equal(decimal.NewFromFloat(95.0)) {
			t.Errorf("expected initial SL price 95.0, got %s", rec.SLPrice)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for partial TP1 trade record")
	}

	// Wait for the asynchronous BE SL order placement to complete
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&slAlgoCount) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&slAlgoCount) < 2 {
		t.Fatalf("timeout waiting for BE SL algo order placement, count=%d", atomic.LoadInt32(&slAlgoCount))
	}

	// Verify that remaining qty is 1.0, tier is 1 (BE), and BE SL was placed
	b.Mu.Lock()
	if !b.ActivePositions["BTCUSDT"].Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("expected remaining active position 1.0, got %s", b.ActivePositions["BTCUSDT"])
	}
	if b.ProtectionTiers["BTCUSDT"] != 1 {
		t.Errorf("expected protection tier 1, got %d", b.ProtectionTiers["BTCUSDT"])
	}
	b.Mu.Unlock()

	slMu.Lock()
	finalSLQty := slOrderQty
	finalSLPrice := slTriggerPrice
	slMu.Unlock()

	if !finalSLQty.Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("expected BE SL order for remaining 1.0 qty, got %s", finalSLQty)
	}
	if finalSLPrice.LessThan(decimal.NewFromFloat(100.0)) {
		t.Errorf("expected BE SL >= entry price (100.0), got %s", finalSLPrice)
	}
}

func TestLiveBrokerTickSizeQuantization(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		TrailingEnabled:     true,
		TrailingActivateATR: 1.0,
		TrailingDistanceATR: 0.5,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()

	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	// Tick size 0.005 -> any price must be multiple of 0.005!
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize: decimal.NewFromFloat(0.005),
			StepSize: decimal.NewFromFloat(1.0),
		}, true
	}

	var slMu sync.Mutex
	var lastSLPrice decimal.Decimal
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		slMu.Lock()
		lastSLPrice = triggerPrice
		slMu.Unlock()
		return 999123, nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST_TICK_QUANT",
		Symbol:        "DOGEUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(100.0),
		LimitPrice:    decimal.NewFromFloat(0.123456),
		SLPrice:       decimal.NewFromFloat(0.111111),
		TPPrice:       decimal.NewFromFloat(0.150000),
		ATRAtEntry:    decimal.NewFromFloat(0.012345),
		CreatedAt:     now,
	}

	b.ReserveEntryIntent(intent)
	b.HandleUserDataOrderUpdate("TEST_TICK_QUANT", 112233, "DOGEUSDT", model.StatusFilled, decimal.NewFromFloat(100.0), decimal.NewFromFloat(0.123456), decimal.Zero)

	time.Sleep(50 * time.Millisecond)

	// Initial SL should be quantized to 0.005 step
	slMu.Lock()
	initialSL := lastSLPrice
	slMu.Unlock()

	rem := initialSL.Mod(decimal.NewFromFloat(0.005))
	if !rem.IsZero() {
		t.Errorf("initial SL %s is not multiple of tickSize 0.005 (rem: %s)", initialSL, rem)
	}

	// Trigger trailing stop with an arbitrary mark price
	b.OnRealtimeTick("DOGEUSDT", 0.150000, 0.150005, 0.150000, 0.150000, 10.0, true, now)
	time.Sleep(50 * time.Millisecond)

	b.Mu.Lock()
	prot := b.Protections["DOGEUSDT"]
	trailSL := prot.SLPrice
	b.Mu.Unlock()

	remTrail := trailSL.Mod(decimal.NewFromFloat(0.005))
	if !remTrail.IsZero() {
		t.Errorf("trailing SL %s is not multiple of tickSize 0.005 (rem: %s)", trailSL, remTrail)
	}
}

func TestLiveBrokerTrailingThrottling(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		TrailingEnabled:     true,
		TrailingActivateATR: 1.0,
		TrailingDistanceATR: 0.5,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()

	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize: decimal.NewFromFloat(0.1),
			StepSize: decimal.NewFromFloat(1.0),
		}, true
	}

	var algoCallCount int32
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		atomic.AddInt32(&algoCallCount, 1)
		return int64(1000 + atomic.LoadInt32(&algoCallCount)), nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST_THROTTLE",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		ATRAtEntry:    decimal.NewFromFloat(5.0),
		CreatedAt:     now,
	}
	b.ReserveEntryIntent(intent)
	b.HandleUserDataOrderUpdate("TEST_THROTTLE", 445566, "SOLUSDT", model.StatusFilled, decimal.NewFromFloat(10.0), decimal.NewFromFloat(100.0), decimal.Zero)
	time.Sleep(50 * time.Millisecond)

	initialCalls := atomic.LoadInt32(&algoCallCount) // Initial SL call

	// Send rapid ticks that activate trailing stop
	b.OnRealtimeTick("SOLUSDT", 110.0, 110.1, 110.0, 110.0, 1.0, true, now)
	time.Sleep(50 * time.Millisecond)
	afterFirstTrail := atomic.LoadInt32(&algoCallCount)
	if afterFirstTrail <= initialCalls {
		t.Fatalf("expected trailing SL to be placed, calls: %d", afterFirstTrail)
	}

	// Send another tick immediately (within 3 seconds) with tiny movement
	b.OnRealtimeTick("SOLUSDT", 110.2, 110.3, 110.2, 110.2, 1.0, true, now.Add(100*time.Millisecond))
	time.Sleep(50 * time.Millisecond)

	// Call count should NOT have increased because of 3-second throttle!
	if atomic.LoadInt32(&algoCallCount) != afterFirstTrail {
		t.Errorf("expected throttled update within 3s, but got new call! count=%d", atomic.LoadInt32(&algoCallCount))
	}
}

func TestLiveBrokerInitialSLPricePreservation(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		TrailingEnabled:     true,
		TrailingActivateATR: 1.0,
		TrailingDistanceATR: 0.5,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()

	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	var slAlgoID int64 = 555001
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		return slAlgoID, nil
	}

	closedTrades := make(chan TradeClosedRecord, 5)
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTrades <- rec
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST_INITIAL_SL_PRESERVE",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		SLPrice:       decimal.NewFromFloat(1950.0),
		TPPrice:       decimal.NewFromFloat(2200.0),
		ATRAtEntry:    decimal.NewFromFloat(40.0),
		CreatedAt:     now,
	}
	b.ReserveEntryIntent(intent)
	b.HandleUserDataOrderUpdate("TEST_INITIAL_SL_PRESERVE", 778899, "ETHUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(2000.0), decimal.Zero)
	time.Sleep(50 * time.Millisecond)

	// Move trailing SL up to ~2060
	b.OnRealtimeTick("ETHUSDT", 2080.0, 2080.5, 2080.0, 2080.0, 1.0, true, now)
	time.Sleep(50 * time.Millisecond)

	// Now Stop Loss fills at the trailing price
	b.HandleUserDataOrderUpdate("live_sl_ETHUSDT", slAlgoID, "ETHUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(2060.0), decimal.NewFromFloat(60.0))

	select {
	case rec := <-closedTrades:
		// SLPrice MUST be the initial SL (1950.0), not the trailing price (2060.0)!
		if !rec.SLPrice.Equal(decimal.NewFromFloat(1950.0)) {
			t.Errorf("expected preserved initial SL 1950.0, got %s", rec.SLPrice)
		}
		if rec.ExitReason != "TRAILING_STOP" {
			t.Errorf("expected exit reason TRAILING_STOP, got %s", rec.ExitReason)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for closed trade record")
	}
}

func TestLiveBrokerHandleUserDataOrderUpdate_ALGO_TP1(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	// Set active position
	b.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["BTCUSDT"] = model.SideLong
	b.PositionEntryPrices["BTCUSDT"] = decimal.NewFromFloat(50000.0)
	b.Protections["BTCUSDT"] = &model.LivePositionProtection{
		Symbol:       "BTCUSDT",
		Side:         model.SideLong,
		ProtectedQty: decimal.NewFromFloat(2.0),
		SLPrice:      decimal.NewFromFloat(49000.0),
		TPPrice:      decimal.NewFromFloat(52000.0),
	}

	// Simulasi event WebSocket fill untuk partial TP1 ("ALGO_TP1_...")
	b.HandleUserDataOrderUpdate("ALGO_TP1_BTCUSDT_179319999", 887766, "BTCUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(51000.0), decimal.NewFromFloat(10.0))

	// MISS #1 FIX: Posisi aktif dan proteksi TIDAK BOLEH dihapus!
	b.Mu.Lock()
	defer b.Mu.Unlock()
	qty, exists := b.ActivePositions["BTCUSDT"]
	if !exists {
		t.Fatalf("expected BTCUSDT active position to be preserved, but it was deleted!")
	}
	if !qty.Equal(decimal.NewFromFloat(2.0)) {
		t.Errorf("expected active position qty 2.0, got %s", qty)
	}
	if _, hasProt := b.Protections["BTCUSDT"]; !hasProt {
		t.Fatalf("expected protections to be preserved for runner")
	}
}

func TestLiveBrokerDynamicSL_RetryOnFailure(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		TrailingEnabled:     true,
		TrailingActivateATR: 1.0,
		TrailingDistanceATR: 0.5,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	var shouldFail atomic.Bool
	var callCount atomic.Int32

	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		callCount.Add(1)
		if shouldFail.Load() {
			return 0, fmt.Errorf("transient network error")
		}
		return 999123, nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST_RETRY_FAIL",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(115.0),
		ATRAtEntry:    decimal.NewFromFloat(5.0),
		CreatedAt:     now,
	}
	b.ReserveEntryIntent(intent)
	b.HandleUserDataOrderUpdate("TEST_RETRY_FAIL", 123456, "SOLUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(100.0), decimal.Zero)
	time.Sleep(50 * time.Millisecond)

	// Sekarang aktifkan kegagalan API khusus saat update dynamic SL pertama
	shouldFail.Store(true)

	// Tick 1: trigger trailing, tapi API Binance gagal
	b.OnRealtimeTick("SOLUSDT", 108.0, 108.5, 108.0, 108.0, 1.0, true, now)
	time.Sleep(50 * time.Millisecond)

	b.Mu.Lock()
	prot := b.Protections["SOLUSDT"]
	// MISS #2.2 FIX: Karena API gagal, SLPrice tidak boleh bermutasi optimistik ke trailing price!
	if !prot.SLPrice.Equal(decimal.NewFromFloat(95.0)) {
		t.Errorf("expected SLPrice to remain 95.0 after failure, got %s", prot.SLPrice)
	}
	if b.ProtectionTiers["SOLUSDT"] != 0 {
		t.Errorf("expected ProtectionTier to remain 0 after failure, got %d", b.ProtectionTiers["SOLUSDT"])
	}
	b.Mu.Unlock()

	// Sekarang perbaiki API Binance (sukses)
	shouldFail.Store(false)

	// Tick 2 (setelah jeda throttle): harus bisa me-retry karena SLPrice lama belum terupdate
	later := now.Add(4 * time.Second)
	b.OnRealtimeTick("SOLUSDT", 109.0, 109.5, 109.0, 109.0, 1.0, true, later)
	time.Sleep(50 * time.Millisecond)

	b.Mu.Lock()
	if b.ProtectionTiers["SOLUSDT"] != 3 {
		t.Errorf("expected ProtectionTier to become 3 after retry success, got %d", b.ProtectionTiers["SOLUSDT"])
	}
	if !b.Protections["SOLUSDT"].SLPrice.GreaterThan(decimal.NewFromFloat(100.0)) {
		t.Errorf("expected SLPrice to be updated > 100.0 after successful retry, got %s", b.Protections["SOLUSDT"].SLPrice)
	}
	b.Mu.Unlock()
}

func TestLiveBrokerRestorePositionWithATR(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
	}
	b := NewBinanceLiveBroker(cfg)
	now := time.Now().UTC()

	pos := &LiveOpenPosition{
		Symbol:         "AVAXUSDT",
		Side:           model.SideShort,
		Qty:            decimal.NewFromFloat(5.0),
		InitialQty:     decimal.NewFromFloat(10.0),
		EntryPrice:     decimal.NewFromFloat(30.0),
		SLOrderID:      112233,
		SLPrice:        decimal.NewFromFloat(31.0),
		InitialSLPrice: decimal.NewFromFloat(32.0),
		TP1Price:       decimal.NewFromFloat(28.5),
		IsTP1Hit:       true,
		ATRAtEntry:     decimal.NewFromFloat(1.5),
		ProtectionTier: 1,
		OpenTime:       now,
	}
	b.RestoreOpenPosition(pos)

	b.Mu.Lock()
	defer b.Mu.Unlock()

	// MISS #3 FIX: Pastikan semua field pulih ke memory
	if !b.ATRAtEntry["AVAXUSDT"].Equal(decimal.NewFromFloat(1.5)) {
		t.Errorf("expected ATRAtEntry 1.5, got %s", b.ATRAtEntry["AVAXUSDT"])
	}
	if !b.InitialQtys["AVAXUSDT"].Equal(decimal.NewFromFloat(10.0)) {
		t.Errorf("expected InitialQtys 10.0, got %s", b.InitialQtys["AVAXUSDT"])
	}
	if !b.InitialSLPrices["AVAXUSDT"].Equal(decimal.NewFromFloat(32.0)) {
		t.Errorf("expected InitialSLPrices 32.0, got %s", b.InitialSLPrices["AVAXUSDT"])
	}
	if !b.TP1Prices["AVAXUSDT"].Equal(decimal.NewFromFloat(28.5)) {
		t.Errorf("expected TP1Prices 28.5, got %s", b.TP1Prices["AVAXUSDT"])
	}
	if !b.IsTP1Hits["AVAXUSDT"] {
		t.Errorf("expected IsTP1Hit true")
	}
}










