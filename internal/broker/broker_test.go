package broker

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
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
		Tier2ProfitLockEnabled:   true,
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
	// SL harus sudah dipindahkan ke Break-Even: EntryPrice * (1 + 0.0011 fee buffer)
	expectedBESL := pos.EntryPrice.Mul(decimal.NewFromFloat(1.0011))
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
	b.MarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, decimal.Decimal, decimal.Decimal, error) {
		atomic.AddInt32(&marketCloseCount, 1)
		marketCloseSide = side
		marketCloseQty = qty
		return 777001, decimal.Zero, qty, nil
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

	_, _ = b.ReserveEntryIntent(intent)
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
	_, _ = b.ReserveEntryIntent(intent)
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
	_, _ = b.ReserveEntryIntent(intent)
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
	if !qty.Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("expected remaining runner qty 1.0 after TP1 fill, got %s", qty)
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
	_, _ = b.ReserveEntryIntent(intent)
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

func TestDryRunBroker_PartialTP1_CushionedBE(t *testing.T) {
	now := time.Now().UTC()
	broker := NewDryRunBroker(decimal.NewFromFloat(100.0), DryRunConfig{
		MaxSlots:              3,
		AccountLeverage:       5.0,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		PartialTP1ATRMult:     1.00,
		RunnerBECushionATR:    0.35,
		MinBreakEvenOffsetPct: 0.0010,
		EntryOrderType:        "MARKET",
		TakerFeeRate:          0.0005,
		MakerFeeRate:          0.0002,
		SlippageRate:          0.0001,
	})
	defer broker.Close()

	intent := model.OrderIntent{
		ClientOrderID: "TEST-CUSHION-LONG",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(96.5),  // Initial SL 3.5 ATR
		TP1Price:      decimal.NewFromFloat(102.0), // TP1 +2.0 (1.0x ATR)
		TPPrice:       decimal.NewFromFloat(105.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected entry to succeed, got ok=%v, err=%v", ok, err)
	}

	// Trigger TP1 at 102.0
	broker.OnRealtimeTick("SOLUSDT", 102.0, 102.1, 102.05, 102.05, 5.0, false, now.Add(1*time.Minute))

	broker.Mu.Lock()
	defer broker.Mu.Unlock()
	pos, exists := broker.OpenPositions["SOLUSDT"]
	if !exists {
		t.Fatalf("expected position to still exist")
	}
	if !pos.IsTP1Hit {
		t.Errorf("expected IsTP1Hit true")
	}
	// Expected Cushioned SL = EntryPrice - Cushion (2.0 * 0.35 = 0.70)
	expectedSL := pos.EntryPrice.Sub(decimal.NewFromFloat(0.70))
	if !pos.CurrentSLPrice.Equal(expectedSL) {
		t.Errorf("expected Cushioned SL %s, got %s", expectedSL, pos.CurrentSLPrice)
	}
}

func TestLiveBrokerPartialTP1AndCushionedBE(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:              3,
		MaxDailyDrawdownPct:   -0.05,
		MaxAccountStaleness:   60 * time.Second,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		RunnerBECushionATR:    0.35,
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

	b.MarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, decimal.Decimal, decimal.Decimal, error) {
		return 777002, decimal.Zero, qty, nil
	}

	var slMu sync.Mutex
	var placedAlgoSL decimal.Decimal
	var slOrderCount int32
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			slMu.Lock()
			placedAlgoSL = triggerPrice
			atomic.AddInt32(&slOrderCount, 1)
			slMu.Unlock()
			return 888002, nil
		}
		return 0, nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "TEST_LIVE_CUSHION_SHORT",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(2.0),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		SLPrice:       decimal.NewFromFloat(2070.0),
		TPPrice:       decimal.NewFromFloat(1900.0),
		TP1Price:      decimal.NewFromFloat(1960.0),
		ATRAtEntry:    decimal.NewFromFloat(40.0), // 40 * 0.35 = 14.0 cushion
		CreatedAt:     now,
	}

	ok, err := b.ReserveEntryIntent(intent)
	if !ok || err != nil {
		t.Fatalf("failed to reserve intent: %v", err)
	}

	b.HandleUserDataOrderUpdate("TEST_LIVE_CUSHION_SHORT", 111002, "ETHUSDT", model.StatusFilled, decimal.NewFromFloat(2.0), decimal.NewFromFloat(2000.0), decimal.Zero)

	// Wait for initial SL
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&slOrderCount) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Trigger TP1 at 1960.0 (Short hits markPrice <= 1960.0)
	b.OnRealtimeTick("ETHUSDT", 1959.0, 1960.0, 1959.5, 1959.5, 1.0, true, now)

	// Wait for async runner SL placement
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&slOrderCount) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	slMu.Lock()
	finalSL := placedAlgoSL
	slMu.Unlock()

	// Short Cushioned SL = Entry (2000.0) + Cushion (40.0 * 0.35 = 14.0) = 2014.0
	expectedLiveSL := decimal.NewFromFloat(2014.0)
	if !finalLiveSLMatches(finalSL, expectedLiveSL) {
		t.Errorf("expected Live Cushioned SL %s, got %s", expectedLiveSL, finalSL)
	}
}

func finalLiveSLMatches(actual, expected decimal.Decimal) bool {
	return actual.Equal(expected)
}

func TestLiveBrokerAccountAntiStalenessAndRefresh(t *testing.T) {
	cfg := LiveBrokerConfig{
		MaxSlots:            3,
		MaxAccountStaleness: 60 * time.Second,
		MaxDailyDrawdownPct: -0.03,
	}
	b := NewBinanceLiveBroker(cfg)

	intent := model.OrderIntent{
		ClientOrderID: "INTENT_STALE_TEST_1",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(0.1),
		LimitPrice:    decimal.NewFromFloat(65000),
		SLPrice:       decimal.NewFromFloat(64000),
		TPPrice:       decimal.NewFromFloat(67000),
		ATRAtEntry:    decimal.NewFromFloat(100),
		SignalAuditID: 999111,
	}

	// 1. Awal boot: LastAccountUpdateTime masih zero -> ReserveEntryIntent harus gagal (stale)
	ok, err := b.ReserveEntryIntent(intent)
	if ok || err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("expected stale error on boot, got ok=%v, err=%v", ok, err)
	}

	// 2. Setup mock FetchAccountFn
	fetchCalled := 0
	b.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
		fetchCalled++
		return decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), make(map[string]model.LivePosition), nil
	}

	// 3. Panggil RefreshAccount secara eksplisit
	if err := b.RefreshAccount(context.Background()); err != nil {
		t.Fatalf("RefreshAccount failed: %v", err)
	}
	if b.WalletBalanceUSDT.String() != "1000" {
		t.Fatalf("expected wallet balance 1000, got %s", b.WalletBalanceUSDT)
	}
	if b.LastAccountUpdateTime.IsZero() {
		t.Fatalf("expected LastAccountUpdateTime updated")
	}

	// 4. Sekarang data segar, ReserveEntryIntent harus sukses
	ok, err = b.ReserveEntryIntent(intent)
	if !ok || err != nil {
		t.Fatalf("expected reserve success after refresh, got ok=%v, err=%v", ok, err)
	}

	// Bersihkan untuk tes SubmitLimitEntry on-demand refresh
	b.Mu.Lock()
	delete(b.OrdersByClient, intent.ClientOrderID)
	// Simulasi staleness: buat LastAccountUpdateTime 70 detik yang lalu
	b.LastAccountUpdateTime = time.Now().UTC().Add(-70 * time.Second)
	b.Mu.Unlock()

	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		return 888777, nil
	}

	initialFetchCount := fetchCalled
	intent2 := model.OrderIntent{
		ClientOrderID: "INTENT_STALE_TEST_2",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(2000),
		SLPrice:       decimal.NewFromFloat(1900),
		TPPrice:       decimal.NewFromFloat(2200),
		ATRAtEntry:    decimal.NewFromFloat(20),
		SignalAuditID: 999222,
	}

	// SubmitLimitEntry harus otomatis refresh on-demand sebelum reserve dan berhasil pasang order
	placed, err := b.SubmitLimitEntry(intent2)
	if !placed || err != nil {
		t.Fatalf("expected SubmitLimitEntry success via auto-refresh, got placed=%v, err=%v", placed, err)
	}
	if fetchCalled <= initialFetchCount {
		t.Fatalf("expected FetchAccountFn to be called on-demand when stale, got fetchCalled=%d", fetchCalled)
	}
}

func TestDryRunPartialLimitFill_TP1_ThenMoreFills(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:              3,
		MaxDailyDrawdownPct:   -0.05,
		AccountLeverage:       5.0,
		BreakEvenTriggerATR:   1.0,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		PartialTP1ATRMult:     0.60,
		RunnerBECushionATR:    0.35,
		MinBreakEvenOffsetPct: 0.0010,
		MakerFeeRate:          0.0002,
		TakerFeeRate:          0.0005,
		SlippageRate:          0.0001,
		EntryOrderType:        "LIMIT",
	}
	broker := NewDryRunBroker(decimal.NewFromFloat(1000.0), cfg)
	defer broker.Close()

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "TEST-CHUNKED-LIMIT",
		Symbol:        "SOLUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		TP1Price:      decimal.NewFromFloat(105.0),
		ATRAtEntry:    decimal.NewFromFloat(5.0),
		CreatedAt:     now,
	}

	ok, err := broker.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected submit limit to succeed, got %v, %v", ok, err)
	}

	// Chunk 1: terisi 5 units (50%)
	broker.OnRealtimeTick("SOLUSDT", 99.9, 100.1, 100.0, 100.0, 5.0, true, now)

	broker.Mu.Lock()
	pos, exists := broker.OpenPositions["SOLUSDT"]
	if !exists {
		broker.Mu.Unlock()
		t.Fatalf("expected position to exist after chunk 1")
	}
	if !pos.Qty.Equal(decimal.NewFromFloat(5.0)) || !pos.InitialQty.Equal(decimal.NewFromFloat(5.0)) {
		broker.Mu.Unlock()
		t.Fatalf("expected Qty 5 and InitialQty 5 after chunk 1, got Qty=%s InitialQty=%s", pos.Qty, pos.InitialQty)
	}
	broker.Mu.Unlock()

	// Harga naik menyentuh TP1 (105.0) -> close 50% dari 5 unit = 2.5 unit, dan sisa limit order dibatalkan
	broker.OnRealtimeTick("SOLUSDT", 105.0, 105.2, 105.0, 105.0, 1.0, false, now.Add(time.Second))

	broker.Mu.Lock()
	pos = broker.OpenPositions["SOLUSDT"]
	if !pos.IsTP1Hit || !pos.Qty.Equal(decimal.NewFromFloat(2.5)) {
		broker.Mu.Unlock()
		t.Fatalf("expected TP1 hit and Qty 2.5, got IsTP1Hit=%v Qty=%s", pos.IsTP1Hit, pos.Qty)
	}
	if _, pendingExists := broker.PendingOrders["SOLUSDT"]; pendingExists {
		broker.Mu.Unlock()
		t.Fatalf("expected remaining pending order to be cancelled upon TP1 hit")
	}
	broker.Mu.Unlock()

	// Chunk 2: Percobaan fill limit order setelah TP1 harus DITOLAK
	broker.OnRealtimeTick("SOLUSDT", 99.9, 100.1, 100.0, 100.0, 2.0, true, now.Add(2*time.Second))

	broker.Mu.Lock()
	pos = broker.OpenPositions["SOLUSDT"]
	// Qty harus tetap 2.5 (runner murni), sisa order tidak boleh menambah ke runner
	expectedQty := decimal.NewFromFloat(2.5)
	if !pos.Qty.Equal(expectedQty) {
		broker.Mu.Unlock()
		t.Fatalf("BUG: Qty changed after TP1! expected %s, got %s", expectedQty, pos.Qty)
	}
	broker.Mu.Unlock()
}

func TestBrokerGracefulShutdownDrainsEvents(t *testing.T) {
	// 1. DryRunBroker: Enqueue many events, close broker, verify all events are dispatched and processed
	db := NewDryRunBroker(decimal.NewFromInt(1000), DryRunConfig{})
	var drySavedCount int32
	db.OnPositionSavedFn = func(pos VirtualOpenPosition) {
		atomic.AddInt32(&drySavedCount, 1)
	}

	for i := 0; i < 50; i++ {
		db.enqueuePositionEvent(PositionEvent{
			Type: PositionEventSave,
			Pos:  VirtualOpenPosition{Symbol: fmt.Sprintf("COIN%dUSDT", i)},
		})
	}
	db.Close()

	if atomic.LoadInt32(&drySavedCount) != 50 {
		t.Fatalf("expected 50 saved events after dryrun Close(), got %d", atomic.LoadInt32(&drySavedCount))
	}

	// 2. BinanceLiveBroker: Enqueue many events, close broker, verify all events are dispatched and processed
	lb := NewBinanceLiveBroker(LiveBrokerConfig{})
	var liveSavedCount int32
	lb.OnPositionSavedFn = func(pos LiveOpenPosition) {
		atomic.AddInt32(&liveSavedCount, 1)
	}

	for i := 0; i < 50; i++ {
		lb.enqueuePositionEvent(LivePositionEvent{
			Type: LivePositionEventSave,
			Pos:  LiveOpenPosition{Symbol: fmt.Sprintf("COIN%dUSDT", i)},
		})
	}
	lb.Close()

	if atomic.LoadInt32(&liveSavedCount) != 50 {
		t.Fatalf("expected 50 saved events after live Close(), got %d", atomic.LoadInt32(&liveSavedCount))
	}
}

func TestLiveBrokerRestorePositionSLVerificationAndRepair(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{})
	defer broker.Close()

	// Simulasi VerifyAlgoOrderFn: kembalikan slAlive = false (SL sudah cancel/expired di exchange)
	broker.VerifyAlgoOrderFn = func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (bool, bool, error) {
		return false, false, nil // Keduanya mati di bursa
	}

	var slRepairedID int64
	broker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			slRepairedID = 999111
			return 999111, nil
		}
		return 888222, nil
	}

	now := time.Now().UTC()
	broker.RestoreOpenPosition(&LiveOpenPosition{
		Symbol:         "BTCUSDT",
		Side:           model.SideLong,
		Qty:            decimal.NewFromFloat(1.0),
		EntryPrice:     decimal.NewFromFloat(60000.0),
		SLOrderID:      12345, // ID di DB, tapi sudah mati di bursa
		SLPrice:        decimal.NewFromFloat(59000.0),
		InitialSLPrice: decimal.NewFromFloat(59000.0),
		TPPrice:        decimal.NewFromFloat(62000.0),
		OpenTime:       now.Add(-10 * time.Minute),
		UpdatedAt:      now,
	})

	broker.Mu.Lock()
	prot, exists := broker.Protections["BTCUSDT"]
	if !exists {
		broker.Mu.Unlock()
		t.Fatalf("expected protection record to exist")
	}
	// Karena VerifyAlgoOrderFn mengembalikan false, SLStatus harus ProtectionFailed (bukan ProtectionAcked!)
	if prot.SLStatus != model.ProtectionFailed || prot.SLOrderID != 0 {
		broker.Mu.Unlock()
		t.Fatalf("expected SLStatus to be ProtectionFailed with SLOrderID=0, got status=%v id=%d", prot.SLStatus, prot.SLOrderID)
	}
	broker.Mu.Unlock()

	// Jalankan RepairFailedProtections: harus memasang Stop Loss baru di Binance
	broker.RepairFailedProtections(now)

	broker.Mu.Lock()
	prot = broker.Protections["BTCUSDT"]
	if prot.SLStatus != model.ProtectionAcked || prot.SLOrderID != 999111 || slRepairedID != 999111 {
		broker.Mu.Unlock()
		t.Fatalf("expected SL to be repaired to ProtectionAcked with new ID 999111, got status=%v id=%d", prot.SLStatus, prot.SLOrderID)
	}
	broker.Mu.Unlock()
}

func TestLiveBrokerAdoptPendingEntry(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{})
	defer broker.Close()

	rec := &model.LiveOrderRecord{
		ClientOrderID:   "ENTRY_RECONCILED_1",
		ExchangeOrderID: 777666,
		Symbol:          "ETHUSDT",
		Side:            model.SideLong,
		TargetQty:       decimal.NewFromFloat(2.5),
		LimitPrice:      decimal.NewFromFloat(3000.0),
		Status:          model.StatusNew,
	}

	broker.AdoptPendingEntryRecord(rec)

	broker.Mu.Lock()
	adoptedByClient, hasClient := broker.OrdersByClient["ENTRY_RECONCILED_1"]
	adoptedByExchange, hasExchange := broker.OrdersByExchangeID[777666]
	broker.Mu.Unlock()

	if !hasClient || !hasExchange || adoptedByClient != rec || adoptedByExchange != rec {
		t.Fatalf("failed to adopt pending entry order into broker records")
	}
	if !adoptedByClient.IsEntry {
		t.Fatalf("expected adopted record to have IsEntry=true")
	}
}

func TestLiveBrokerTimeStopEmergencyClose_GhostOrderAndExitReason(t *testing.T) {
	broker := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxHoldingDuration:  4 * time.Hour,
		MaxAccountStaleness: 60 * time.Second,
	})
	defer broker.Close()
	now := time.Now().UTC()
	broker.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	var cancelledAlgoIDs []int64
	var cancelMu sync.Mutex
	broker.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoOrderID int64) error {
		cancelMu.Lock()
		cancelledAlgoIDs = append(cancelledAlgoIDs, algoOrderID)
		cancelMu.Unlock()
		return nil
	}

	var closedRec *TradeClosedRecord
	var closeMu sync.Mutex
	broker.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closeMu.Lock()
		closedRec = &rec
		closeMu.Unlock()
	}

	var emgClientOrderID string
	broker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
		emgClientOrderID = fmt.Sprintf("live_emg_%s_%d", symbol, time.Now().UnixNano())
		return nil
	}

	broker.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			return 2001, nil
		}
		return 2002, nil
	}

	// 1. Setup entry and initial position
	origClientOrderID := "BOT_ADAUSDT_LONG_1234567890_001"
	intent := model.OrderIntent{
		ClientOrderID: origClientOrderID,
		Symbol:        "ADAUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(100.0),
		LimitPrice:    decimal.NewFromFloat(0.50),
		SLPrice:       decimal.NewFromFloat(0.48),
		TPPrice:       decimal.NewFromFloat(0.55),
		CreatedAt:     now.Add(-5 * time.Hour),
		SignalAuditID: 777,
	}
	resOk, resErr := broker.ReserveEntryIntent(intent)
	if !resOk || resErr != nil {
		t.Fatalf("ReserveEntryIntent failed: %v", resErr)
	}

	// Simulate entry fill
	broker.HandleUserDataOrderUpdate(origClientOrderID, 1001, "ADAUSDT", model.StatusFilled, decimal.NewFromFloat(100.0), decimal.NewFromFloat(0.50), decimal.Zero)

	// Wait until async protection orders (SL/TP) are placed
	deadlinePlace := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadlinePlace) {
		broker.Mu.Lock()
		prot, hasProt := broker.Protections["ADAUSDT"]
		placed := hasProt && prot != nil && prot.SLOrderID > 0 && prot.TPOrderID > 0
		broker.Mu.Unlock()
		if placed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Backdate PositionOpenTimes beyond MaxHoldingDuration
	broker.Mu.Lock()
	broker.PositionOpenTimes["ADAUSDT"] = now.Add(-4*time.Hour - 10*time.Minute)
	broker.Mu.Unlock()

	// 2. Trigger CheckMaxHoldingTimeouts
	broker.CheckMaxHoldingTimeouts(now)

	if emgClientOrderID == "" {
		t.Fatalf("expected EmergencyMarketCloseFn to be invoked")
	}

	// 3. Simulate emergency exit fill on Binance
	broker.HandleUserDataOrderUpdate(emgClientOrderID, 9999, "ADAUSDT", model.StatusFilled, decimal.NewFromFloat(100.0), decimal.NewFromFloat(0.49), decimal.NewFromFloat(-1.0))

	// Wait up to 500ms for async cancellations to complete
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		cancelMu.Lock()
		count := len(cancelledAlgoIDs)
		cancelMu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 4. Assert: BOTH SL and TP algo orders were cancelled (Bug 1 Fix: No Ghost Orders)
	cancelMu.Lock()
	defer cancelMu.Unlock()
	if len(cancelledAlgoIDs) != 2 {
		t.Fatalf("expected 2 algo orders cancelled (SL & TP), got %d: %v", len(cancelledAlgoIDs), cancelledAlgoIDs)
	}
	hasSL := false
	hasTP := false
	for _, id := range cancelledAlgoIDs {
		if id == 2001 {
			hasSL = true
		}
		if id == 2002 {
			hasTP = true
		}
	}
	if !hasSL || !hasTP {
		t.Errorf("expected SL(2001) and TP(2002) to be cancelled, got %v", cancelledAlgoIDs)
	}

	// 5. Assert: Trade record exit reason is TIME_STOP_MAX_HOLDING (NOT TAKE_PROFIT)
	deadlineClose := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadlineClose) {
		closeMu.Lock()
		rec := closedRec
		closeMu.Unlock()
		if rec != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	closeMu.Lock()
	defer closeMu.Unlock()
	if closedRec == nil {
		t.Fatalf("expected OnTradeClosedFn to be called")
	}
	if closedRec.ExitReason != "TIME_STOP_MAX_HOLDING" {
		t.Errorf("expected ExitReason 'TIME_STOP_MAX_HOLDING', got '%s'", closedRec.ExitReason)
	}
	if closedRec.ClientOrderID != origClientOrderID {
		t.Errorf("expected preserved ClientOrderID '%s', got '%s'", origClientOrderID, closedRec.ClientOrderID)
	}
	if closedRec.SignalAuditID != 777 {
		t.Errorf("expected SignalAuditID 777, got %d", closedRec.SignalAuditID)
	}
}

func TestBrokerIsCircuitBreakerTrippedParity(t *testing.T) {
	now := time.Now().UTC()

	// 1. DryRunBroker parity
	dry := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxDailyDrawdownPct: -0.05,
	})
	if dry.IsCircuitBreakerTripped() {
		t.Errorf("expected initial circuit breaker not tripped in dry-run")
	}

	// Cause -6% drawdown in dryrun
	dry.Mu.Lock()
	dry.SessionStartEquity = decimal.NewFromFloat(1000.0)
	dry.RealizedEquityUSDT = decimal.NewFromFloat(940.0) // -6%
	dry.Mu.Unlock()

	if !dry.IsCircuitBreakerTripped() {
		t.Errorf("expected circuit breaker tripped after -6%% drawdown")
	}

	// Bounce equity to -4% drawdown; breaker should remain tripped (latched)
	dry.Mu.Lock()
	dry.RealizedEquityUSDT = decimal.NewFromFloat(960.0) // -4%
	dry.Mu.Unlock()

	if !dry.IsCircuitBreakerTripped() {
		t.Errorf("expected circuit breaker to remain tripped after rebound")
	}

	// 2. BinanceLiveBroker parity
	live := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxDailyDrawdownPct: -0.05,
	})
	defer live.Close()

	live.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)
	if live.IsCircuitBreakerTripped() {
		t.Errorf("expected initial circuit breaker not tripped in live")
	}

	// Cause -6% drawdown in live
	live.HandleAccountUpdate(decimal.NewFromFloat(940.0), decimal.Zero, decimal.NewFromFloat(940.0), nil, now)
	if !live.IsCircuitBreakerTripped() {
		t.Errorf("expected live circuit breaker tripped after -6%% drawdown")
	}

	// Bounce equity to -4% drawdown; breaker should remain tripped
	live.HandleAccountUpdate(decimal.NewFromFloat(960.0), decimal.Zero, decimal.NewFromFloat(960.0), nil, now)
	if !live.IsCircuitBreakerTripped() {
		t.Errorf("expected live circuit breaker to remain tripped after rebound")
	}
}

func TestLiveBrokerRepairProtectionsTickSizeQuantization(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
	})
	defer b.Close()
	now := time.Now().UTC()

	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	// TickSize = 0.005
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize: decimal.NewFromFloat(0.005),
			StepSize: decimal.NewFromFloat(1.0),
		}, true
	}

	var placedSLPrice decimal.Decimal
	var placedTPPrice decimal.Decimal
	var mu sync.Mutex
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		mu.Lock()
		switch orderType {
		case "STOP_MARKET":
			placedSLPrice = triggerPrice
		case "TAKE_PROFIT_MARKET":
			placedTPPrice = triggerPrice
		}
		mu.Unlock()
		return 888111, nil
	}

	b.Mu.Lock()
	b.ActivePositions["XRPUSDT"] = decimal.NewFromFloat(100)
	b.PositionSides["XRPUSDT"] = model.SideLong
	b.Protections["XRPUSDT"] = &model.LivePositionProtection{
		Symbol:         "XRPUSDT",
		Side:           model.SideLong,
		ProtectedQty:   decimal.NewFromFloat(100),
		InitialQty:     decimal.NewFromFloat(100),
		SLPrice:        decimal.NewFromFloat(0.481234), // Not a multiple of 0.005
		TPPrice:        decimal.NewFromFloat(0.612345), // Not a multiple of 0.005
		SLStatus:       model.ProtectionFailed,
		TPStatus:       model.ProtectionPending,
		SLRetryCount:   0,
		TPRetryCount:   0,
		PlanGeneration: 1,
	}
	b.Mu.Unlock()

	b.RepairFailedProtections(now)

	mu.Lock()
	sl := placedSLPrice
	tp := placedTPPrice
	mu.Unlock()

	remSL := sl.Mod(decimal.NewFromFloat(0.005))
	if !remSL.IsZero() {
		t.Errorf("repaired SL price %s is not multiple of 0.005 (remainder: %s)", sl, remSL)
	}

	remTP := tp.Mod(decimal.NewFromFloat(0.005))
	if !remTP.IsZero() {
		t.Errorf("repaired TP price %s is not multiple of 0.005 (remainder: %s)", tp, remTP)
	}
}

func TestLiveBrokerTP1FormattingAndReaderStats(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		TakerFeeRate:        0.0005,
		PartialTPEnabled:    true,
		PartialTP1Ratio:     0.50,
	})
	defer b.Close()
	now := time.Now().UTC()

	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		return 112233, nil
	}

	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		}, true
	}

	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	origCID := "BOT_ETHUSDT_LONG_1700000000_001"
	intent := model.OrderIntent{
		ClientOrderID: origCID,
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(2.0),
		LimitPrice:    decimal.NewFromFloat(3000.0),
		SLPrice:       decimal.NewFromFloat(2900.0),
		TPPrice:       decimal.NewFromFloat(3300.0),
		TP1Price:      decimal.NewFromFloat(3100.0),
		CreatedAt:     now,
	}
	_, _ = b.ReserveEntryIntent(intent)
	b.HandleUserDataOrderUpdate(origCID, 1001, "ETHUSDT", model.StatusFilled, decimal.NewFromFloat(2.0), decimal.NewFromFloat(3000.0), decimal.Zero)

	var closedClientOrderID string
	var closeMu sync.Mutex
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closeMu.Lock()
		closedClientOrderID = rec.ClientOrderID
		closeMu.Unlock()
	}

	b.MarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, decimal.Decimal, decimal.Decimal, error) {
		return 555666, decimal.NewFromFloat(3100.0), decimal.NewFromFloat(1.0), nil
	}

	// Trigger tick to hit TP1 (price >= 3100)
	b.OnRealtimeTick("ETHUSDT", 3105.0, 3105.0, 3105.0, 3105.0, 10.0, true, now)

	time.Sleep(50 * time.Millisecond)

	closeMu.Lock()
	tp1CID := closedClientOrderID
	closeMu.Unlock()

	expectedTP1CID := origCID + "_TP1"
	if tp1CID != expectedTP1CID {
		t.Errorf("expected TP1 client order ID '%s', got '%s'", expectedTP1CID, tp1CID)
	}
}

func TestLiveBrokerLimitMakerTP1AndGhostCancellation(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		PartialTPEnabled:    true,
		PartialTP1Ratio:     0.50,
		MakerFeeRate:        0.0002,
		TakerFeeRate:        0.0005,
	})
	defer b.Close()
	now := time.Now().UTC()

	var placedLimitCloseSymbol string
	var placedLimitCloseSide model.Side
	var placedLimitCloseQty decimal.Decimal
	var placedLimitClosePrice decimal.Decimal
	var placedLimitCloseCID string
	var limitCloseMu sync.Mutex

	b.PlaceLimitCloseFn = func(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (int64, string, error) {
		limitCloseMu.Lock()
		placedLimitCloseSymbol = symbol
		placedLimitCloseSide = side
		placedLimitCloseQty = qty
		placedLimitClosePrice = price
		placedLimitCloseCID = clientOrderID
		limitCloseMu.Unlock()
		return 888777, clientOrderID, nil
	}

	var placedSLAlgoSide model.Side
	var placedSLAlgoPrice decimal.Decimal
	var placedSLAlgoQty decimal.Decimal
	var slMu sync.Mutex
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		slMu.Lock()
		placedSLAlgoSide = side
		placedSLAlgoPrice = triggerPrice
		placedSLAlgoQty = qty
		slMu.Unlock()
		return 112233, nil
	}

	var canceledExchangeOrderID int64
	var canceledClientOrderID string
	var cancelMu sync.Mutex
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
		cancelMu.Lock()
		canceledExchangeOrderID = exchangeOrderID
		canceledClientOrderID = clientOrderID
		cancelMu.Unlock()
		return nil
	}

	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.1),
			StepSize:    decimal.NewFromFloat(0.001),
			MinNotional: decimal.NewFromFloat(5.0),
		}, true
	}

	b.HandleAccountUpdate(decimal.NewFromFloat(10000.0), decimal.Zero, decimal.NewFromFloat(10000.0), nil, now)

	// Step 1: Entry SHORT 1.0 BTC @ 50,000, SL @ 51,000, TP1 @ 49,000
	origCID := "BOT_SHORT_BTC_001"
	intent := model.OrderIntent{
		ClientOrderID: origCID,
		Symbol:        "BTCUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(50000.0),
		SLPrice:       decimal.NewFromFloat(51000.0),
		TPPrice:       decimal.NewFromFloat(48000.0),
		TP1Price:      decimal.NewFromFloat(49000.0),
		ATRAtEntry:    decimal.NewFromFloat(500.0),
		CreatedAt:     now,
	}
	reserved, err := b.ReserveEntryIntent(intent)
	if !reserved || err != nil {
		t.Fatalf("failed to reserve intent: %v", err)
	}

	// Trigger Entry Fill
	b.HandleUserDataOrderUpdate(origCID, 1001, "BTCUSDT", model.StatusFilled, decimal.NewFromFloat(1.0), decimal.NewFromFloat(50000.0), decimal.Zero)

	time.Sleep(100 * time.Millisecond)

	// Verifikasi resting Limit Maker TP1 terpasang via PlaceLimitCloseFn
	limitCloseMu.Lock()
	if placedLimitCloseSymbol != "BTCUSDT" {
		t.Errorf("expected placedLimitCloseSymbol BTCUSDT, got %s", placedLimitCloseSymbol)
	}
	if placedLimitCloseSide != model.SideLong {
		t.Errorf("expected exit side LONG to close SHORT, got %s", placedLimitCloseSide)
	}
	if !placedLimitCloseQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Errorf("expected TP1 qty 0.5, got %s", placedLimitCloseQty)
	}
	if !placedLimitClosePrice.Equal(decimal.NewFromFloat(49000.0)) {
		t.Errorf("expected TP1 price 49000.0, got %s", placedLimitClosePrice)
	}
	tp1CID := placedLimitCloseCID
	limitCloseMu.Unlock()

	b.Mu.Lock()
	prot, hasProt := b.Protections["BTCUSDT"]
	if !hasProt || prot.TP1OrderID != 888777 {
		t.Fatalf("expected TP1OrderID 888777 stored in protection, got %v", prot)
	}
	b.Mu.Unlock()

	// Step 2: Simulasikan fill resting Limit Maker TP1 di bursa via WebSocket ORDER_TRADE_UPDATE
	var tradeRec TradeClosedRecord
	var tradeClosedMu sync.Mutex
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		tradeClosedMu.Lock()
		tradeRec = rec
		tradeClosedMu.Unlock()
	}

	b.HandleUserDataOrderUpdate(tp1CID, 888777, "BTCUSDT", model.StatusFilled, decimal.NewFromFloat(0.5), decimal.NewFromFloat(49000.0), decimal.Zero)

	time.Sleep(150 * time.Millisecond)

	// Verifikasi record trade parsial
	tradeClosedMu.Lock()
	if tradeRec.ExitReason != "PARTIAL_TAKE_PROFIT" {
		t.Errorf("expected exit reason PARTIAL_TAKE_PROFIT, got %s", tradeRec.ExitReason)
	}
	if !tradeRec.Qty.Equal(decimal.NewFromFloat(0.5)) {
		t.Errorf("expected closed qty 0.5, got %s", tradeRec.Qty)
	}
	// Periksa PnL: Gross = (50000 - 49000) * 0.5 = 500. Entry fee = 50000*0.5*0.0005 = 12.5. Exit fee (Maker) = 49000*0.5*0.0002 = 4.9. Net PnL = 500 - 12.5 - 4.9 = 482.6
	expectedNetPnL := decimal.NewFromFloat(482.6)
	if !tradeRec.NetPnL.Equal(expectedNetPnL) {
		t.Errorf("expected NetPnL %s with Maker fee, got %s", expectedNetPnL, tradeRec.NetPnL)
	}
	tradeClosedMu.Unlock()

	// Verifikasi posisi berkurang menjadi 0.5 dan SL dipindahkan ke BE
	b.Mu.Lock()
	remQty := b.ActivePositions["BTCUSDT"]
	if !remQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Errorf("expected active position remaining 0.5, got %s", remQty)
	}
	if !b.IsTP1Hits["BTCUSDT"] {
		t.Errorf("expected IsTP1Hits true")
	}
	b.Mu.Unlock()

	slMu.Lock()
	if placedSLAlgoSide != model.SideLong {
		t.Errorf("expected new SL exit side LONG, got %s", placedSLAlgoSide)
	}
	if !placedSLAlgoQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Errorf("expected new SL qty 0.5, got %s", placedSLAlgoQty)
	}
	if !placedSLAlgoPrice.LessThan(decimal.NewFromFloat(50000.0)) {
		t.Errorf("expected new BE SL price < 50000.0, got %s", placedSLAlgoPrice)
	}
	slMu.Unlock()

	// Step 3: Test skenario ghost order cancellation bila SL tersentuh sebelum TP1 terisi pada posisi lain
	origCID2 := "BOT_SHORT_ETH_002"
	intent2 := model.OrderIntent{
		ClientOrderID: origCID2,
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(3000.0),
		SLPrice:       decimal.NewFromFloat(3100.0),
		TPPrice:       decimal.NewFromFloat(2800.0),
		TP1Price:      decimal.NewFromFloat(2900.0),
		ATRAtEntry:    decimal.NewFromFloat(50.0),
		CreatedAt:     now,
	}
	_, _ = b.ReserveEntryIntent(intent2)
	b.HandleUserDataOrderUpdate(origCID2, 2002, "ETHUSDT", model.StatusFilled, decimal.NewFromFloat(10.0), decimal.NewFromFloat(3000.0), decimal.Zero)
	time.Sleep(100 * time.Millisecond)

	b.Mu.Lock()
	ethProt := b.Protections["ETHUSDT"]
	if ethProt == nil || ethProt.TP1OrderID == 0 {
		t.Fatalf("expected ETH TP1 order to be active")
	}
	ethTP1ID := ethProt.TP1OrderID
	ethTP1CID := ethProt.TP1ClientOrderID
	ethSLID := ethProt.SLOrderID
	b.Mu.Unlock()

	// Simulasikan SL terisi penuh sebelum TP1 tercapai
	cancelMu.Lock()
	canceledExchangeOrderID = 0
	cancelMu.Unlock()

	b.HandleUserDataOrderUpdate("ALGOSL_ETH", ethSLID, "ETHUSDT", model.StatusFilled, decimal.NewFromFloat(10.0), decimal.NewFromFloat(3100.0), decimal.NewFromFloat(-1000.0))
	time.Sleep(100 * time.Millisecond)

	// Verifikasi resting Limit Maker order di ETHUSDT dibatalkan via CancelOrderFn agar tidak jadi ghost order
	cancelMu.Lock()
	if canceledExchangeOrderID != ethTP1ID || canceledClientOrderID != ethTP1CID {
		t.Errorf("expected resting TP1 order %d (%s) canceled on SL hit, got %d (%s)", ethTP1ID, ethTP1CID, canceledExchangeOrderID, canceledClientOrderID)
	}
	cancelMu.Unlock()
}

func TestLiveBrokerHandleUserDataOrderUpdate_EntryAfterTP1NoDeadlock(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3})
	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)

	b.Mu.Lock()
	b.OrdersByClient["ENTRY_LATE_1"] = &model.LiveOrderRecord{
		ClientOrderID: "ENTRY_LATE_1",
		Symbol:        "SOLUSDT",
		Side:          model.SideShort,
		IsEntry:       true,
		Status:        model.StatusNew,
	}
	b.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(5.0)
	b.PositionSides["SOLUSDT"] = model.SideShort
	b.PositionEntryPrices["SOLUSDT"] = decimal.NewFromFloat(100.0)
	b.IsTP1Hits["SOLUSDT"] = true
	b.Mu.Unlock()

	// Panggil update entry terlambat pasca-TP1: sebelumnya memicu return tanpa Unlock (deadlock)
	b.HandleUserDataOrderUpdate("ENTRY_LATE_1", 999111, "SOLUSDT", model.StatusFilled, decimal.NewFromFloat(5.0), decimal.NewFromFloat(100.0), decimal.Zero)

	done := make(chan struct{})
	go func() {
		b.OnRealtimeTick("SOLUSDT", 99.0, 99.1, 99.0, 99.0, 1.0, false, now)
		_ = b.GetActiveSlotCount()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("deadlock detected after post-TP1 entry update")
	}
}

func TestLiveBrokerHandleUserDataOrderUpdate_TP1_PartialFillsThenFilled(t *testing.T) {
	var slMu sync.Mutex
	var beSLQty decimal.Decimal
	var closedTradesMu sync.Mutex
	var closed []TradeClosedRecord

	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		PartialTPEnabled: true,
		PartialTP1Ratio:  0.50,
		MakerFeeRate:     0.0002,
		TakerFeeRate:     0.0005,
	})
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if len(clientAlgoID) > model.MaxBinanceClientOrderIDLen {
			return 0, fmt.Errorf("clientAlgoId too long: %d", len(clientAlgoID))
		}
		slMu.Lock()
		beSLQty = qty
		slMu.Unlock()
		return 556677, nil
	}
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedTradesMu.Lock()
		closed = append(closed, rec)
		closedTradesMu.Unlock()
	}

	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)

	b.Mu.Lock()
	b.ActivePositions["1000PEPEUSDT"] = decimal.NewFromFloat(10.0)
	b.InitialQtys["1000PEPEUSDT"] = decimal.NewFromFloat(10.0)
	b.PositionSides["1000PEPEUSDT"] = model.SideShort
	b.PositionEntryPrices["1000PEPEUSDT"] = decimal.NewFromFloat(1.0)
	b.InitialSLPrices["1000PEPEUSDT"] = decimal.NewFromFloat(1.05)
	b.TP1Prices["1000PEPEUSDT"] = decimal.NewFromFloat(0.95)
	b.ATRAtEntry["1000PEPEUSDT"] = decimal.NewFromFloat(0.02)
	b.Protections["1000PEPEUSDT"] = &model.LivePositionProtection{
		Symbol:           "1000PEPEUSDT",
		Side:             model.SideShort,
		PlanGeneration:   1,
		ProtectedQty:     decimal.NewFromFloat(10.0),
		SLOrderID:        111,
		SLPrice:          decimal.NewFromFloat(1.05),
		SLStatus:         model.ProtectionAcked,
		TP1OrderID:       222,
		TP1ClientOrderID: "ALGO_TP1_1000PEPE_1",
	}
	b.Mu.Unlock()

	// Chunk 1: PARTIALLY_FILLED 2.0 dari 5.0
	b.HandleUserDataOrderUpdate("ALGO_TP1_1000PEPE_1", 222, "1000PEPEUSDT", model.StatusPartiallyFilled, decimal.NewFromFloat(2.0), decimal.NewFromFloat(0.95), decimal.Zero)

	b.Mu.Lock()
	if !b.ActivePositions["1000PEPEUSDT"].Equal(decimal.NewFromFloat(8.0)) {
		t.Fatalf("expected active qty 8.0 after TP1 chunk 1, got %s", b.ActivePositions["1000PEPEUSDT"])
	}
	if b.IsTP1Hits["1000PEPEUSDT"] {
		t.Fatalf("expected IsTP1Hits false before final StatusFilled")
	}
	b.Mu.Unlock()

	// Chunk 2 (Final): FILLED kumulatif 5.0 -> sisa runner harus 5.0 (bukan 8.0 - 5.0 = 3.0!)
	b.HandleUserDataOrderUpdate("ALGO_TP1_1000PEPE_1", 222, "1000PEPEUSDT", model.StatusFilled, decimal.NewFromFloat(5.0), decimal.NewFromFloat(0.95), decimal.Zero)

	b.Mu.Lock()
	if !b.ActivePositions["1000PEPEUSDT"].Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected final runner qty 5.0 after full TP1 fill, got %s", b.ActivePositions["1000PEPEUSDT"])
	}
	if !b.IsTP1Hits["1000PEPEUSDT"] {
		t.Fatalf("expected IsTP1Hits true after StatusFilled")
	}
	b.Mu.Unlock()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		slMu.Lock()
		gotSLQty := beSLQty
		slMu.Unlock()
		closedTradesMu.Lock()
		nClosed := len(closed)
		closedTradesMu.Unlock()
		if nClosed == 1 && !gotSLQty.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	closedTradesMu.Lock()
	if len(closed) != 1 || !closed[0].Qty.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected 1 TP1 TradeClosedRecord with Qty 5.0, got %+v", closed)
	}
	closedTradesMu.Unlock()

	slMu.Lock()
	if !beSLQty.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected BE SL placed for remaining 5.0 qty, got %s", beSLQty)
	}
	slMu.Unlock()
}

func TestLiveBrokerRestoreOpenPosition_PartialTPEnabledSkipsTPRepair(t *testing.T) {
	var tpRepairCalls atomic.Int32
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		PartialTPEnabled: true,
		TrailingEnabled:  true,
	})
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "TAKE_PROFIT_MARKET" {
			tpRepairCalls.Add(1)
		}
		return 99123, nil
	}

	pos := &LiveOpenPosition{
		Symbol:         "1000SHIBUSDT",
		Side:           model.SideShort,
		Qty:            decimal.NewFromFloat(100.0),
		InitialQty:     decimal.NewFromFloat(100.0),
		EntryPrice:     decimal.NewFromFloat(0.020),
		SLOrderID:      12345,
		SLClientAlgoID: "ALGO_SL_1000SHIB_1",
		SLPrice:        decimal.NewFromFloat(0.021),
		TPOrderID:      0, // Sengaja 0 saat PartialTPEnabled=true
		TPPrice:        decimal.NewFromFloat(0.018),
		TP1Price:       decimal.NewFromFloat(0.019),
	}
	b.RestoreOpenPosition(pos)

	b.Mu.Lock()
	prot := b.Protections["1000SHIBUSDT"]
	if prot == nil || prot.TPStatus != model.ProtectionAcked {
		t.Fatalf("expected TPStatus ProtectionAcked under PartialTPEnabled=true, got %+v", prot)
	}
	b.Mu.Unlock()

	b.RepairFailedProtections(time.Now().UTC())
	if tpRepairCalls.Load() != 0 {
		t.Fatalf("expected 0 TAKE_PROFIT_MARKET repair calls under PartialTPEnabled=true, got %d", tpRepairCalls.Load())
	}
}

func TestDryRunBroker_RestingOrderNotCancelledOnLaterBookTick_AndFills(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:          3,
		AccountLeverage:   8.0,
		LimitOrderTimeout: 0, // Harus default ke 90s
		MakerFeeRate:      0.0002,
		TakerFeeRate:      0.0005,
		SlippageRate:      0.0002,
	}
	b := NewDryRunBroker(decimal.NewFromFloat(1000.0), cfg)
	defer b.Close()

	if b.Cfg.LimitOrderTimeout != 150*time.Second {
		t.Fatalf("expected default LimitOrderTimeout 150s, got %s", b.Cfg.LimitOrderTimeout)
	}

	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize: decimal.NewFromFloat(0.001),
			StepSize: decimal.NewFromFloat(1),
		}, true
	}

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "SIM_ZRO_SHORT",
		Symbol:        "ZROUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(20),
		LimitPrice:    decimal.NewFromFloat(1.000),
		SLPrice:       decimal.NewFromFloat(1.030),
		TPPrice:       decimal.NewFromFloat(0.940),
		TP1Price:      decimal.NewFromFloat(0.970),
		ATRAtEntry:    decimal.NewFromFloat(0.020),
		CreatedAt:     now,
	}

	ok, err := b.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: ok=%v err=%v", ok, err)
	}

	// Tick 1: BBO awal valid untuk maker SELL di 1.000 (bid < 1.000) -> GTXValidated menjadi true
	b.OnRealtimeTick("ZROUSDT", 0.998, 1.000, 0.999, 0, 0, false, now.Add(50*time.Millisecond))

	// Tick 2: bestBid naik melewati LimitPrice 1.000 (skenario ZROUSDT/ONEUSDT dulu).
	// Karena order sudah resting GTXValidated=true, order TIDAK boleh dibatalkan oleh ticker BBO berikutnya.
	b.OnRealtimeTick("ZROUSDT", 1.001, 1.002, 1.001, 0, 0, false, now.Add(2*time.Second))

	b.Mu.Lock()
	_, stillPending := b.PendingOrders["ZROUSDT"]
	gtxValidated := stillPending && b.PendingOrders["ZROUSDT"].GTXValidated
	b.Mu.Unlock()
	if !stillPending || !gtxValidated {
		t.Fatalf("expected resting pending order to stay alive with GTXValidated=true, pending=%v gtx=%v", stillPending, gtxValidated)
	}

	// Timeout check pada +120s (skenario WLDUSDT menit ke-2) tidak boleh membatalkan karena timeout sekarang 150s
	b.CheckTimeouts(now.Add(120 * time.Second))
	if !b.HasActiveOrderOrPosition("ZROUSDT") {
		t.Fatalf("expected ZROUSDT order to remain active at +120s under 150s timeout")
	}

	// AggTrade buy (isBuyerMaker=false) di >= LimitPrice mengisi order SHORT
	b.OnRealtimeTick("ZROUSDT", 1.000, 1.001, 1.000, 1.000, 25, false, now.Add(121*time.Second))
	b.Mu.Lock()
	pos := b.OpenPositions["ZROUSDT"]
	b.Mu.Unlock()
	if pos == nil || !pos.EntryPrice.Equal(decimal.NewFromFloat(1.000)) {
		t.Fatalf("expected SHORT position opened at 1.000, got %+v", pos)
	}
}

func TestDryRunBroker_PreSubmitAndFirstTickRepegGTX(t *testing.T) {
	now := time.Now().UTC()

	t.Run("pre-submit repeg via GetBestQuoteFn within drift budget", func(t *testing.T) {
		b := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
			MaxSlots:              3,
			AccountLeverage:       8.0,
			MaxEntryRepegAttempts: 3,
			MaxEntryRepegDriftPct: 0.0005,
		})
		defer b.Close()

		b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
			return model.ExchangeFilter{
				TickSize: decimal.NewFromFloat(0.0001),
				StepSize: decimal.NewFromFloat(1),
			}, true
		}
		b.GetBestQuoteFn = func(symbol string) (float64, float64, bool) {
			return 1.0002, 1.0003, true // +0.03% dari 1.0000 (masih <= 0.05%)
		}

		intent := model.OrderIntent{
			ClientOrderID: "REPEG_PRE_1",
			Symbol:        "SYM1USDT",
			Side:          model.SideShort,
			Qty:           decimal.NewFromFloat(10),
			LimitPrice:    decimal.NewFromFloat(1.0000),
			SLPrice:       decimal.NewFromFloat(1.0200),
			TPPrice:       decimal.NewFromFloat(0.9600),
			TP1Price:      decimal.NewFromFloat(0.9800),
			ATRAtEntry:    decimal.NewFromFloat(0.015),
			CreatedAt:     now,
		}
		ok, err := b.SubmitLimitEntry(intent)
		if !ok || err != nil {
			t.Fatalf("expected pre-repeg submit success, got ok=%v err=%v", ok, err)
		}

		b.Mu.Lock()
		ord := b.PendingOrders["SYM1USDT"]
		b.Mu.Unlock()
		if ord == nil || !ord.GTXValidated || ord.RepegAttempts != 1 {
			t.Fatalf("expected GTXValidated=true RepegAttempts=1, got %+v", ord)
		}
		if !ord.Intent.LimitPrice.Equal(decimal.NewFromFloat(1.0003)) {
			t.Fatalf("expected repegged limit 1.0003, got %s", ord.Intent.LimitPrice)
		}
		if !ord.Intent.SLPrice.Equal(decimal.NewFromFloat(1.0203)) || !ord.Intent.TPPrice.Equal(decimal.NewFromFloat(0.9603)) {
			t.Fatalf("expected shifted brackets SL=1.0203 TP=0.9603, got SL=%s TP=%s", ord.Intent.SLPrice, ord.Intent.TPPrice)
		}
	})

	t.Run("first-tick repeg when GetBestQuoteFn unavailable", func(t *testing.T) {
		b := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
			MaxSlots:        3,
			AccountLeverage: 8.0,
		})
		defer b.Close()
		b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
			return model.ExchangeFilter{
				TickSize: decimal.NewFromFloat(0.0001),
				StepSize: decimal.NewFromFloat(1),
			}, true
		}

		intent := model.OrderIntent{
			ClientOrderID: "REPEG_TICK_1",
			Symbol:        "SYM2USDT",
			Side:          model.SideShort,
			Qty:           decimal.NewFromFloat(10),
			LimitPrice:    decimal.NewFromFloat(1.0000),
			SLPrice:       decimal.NewFromFloat(1.0200),
			TPPrice:       decimal.NewFromFloat(0.9600),
			ATRAtEntry:    decimal.NewFromFloat(0.015),
			CreatedAt:     now,
		}
		ok, err := b.SubmitLimitEntry(intent)
		if !ok || err != nil {
			t.Fatalf("SubmitLimitEntry failed: %v", err)
		}

		b.OnRealtimeTick("SYM2USDT", 1.0001, 1.0002, 1.0001, 0, 0, false, now.Add(20*time.Millisecond))

		b.Mu.Lock()
		ord := b.PendingOrders["SYM2USDT"]
		b.Mu.Unlock()
		if ord == nil || !ord.GTXValidated || !ord.Intent.LimitPrice.Equal(decimal.NewFromFloat(1.0002)) {
			t.Fatalf("expected first-tick repeg to 1.0002 with GTXValidated=true, got %+v", ord)
		}
	})

	t.Run("excessive drift rejected cleanly without holding slot", func(t *testing.T) {
		b := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
			MaxSlots:              3,
			AccountLeverage:       8.0,
			MaxEntryRepegAttempts: 3,
			MaxEntryRepegDriftPct: 0.0005,
		})
		defer b.Close()
		b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
			return model.ExchangeFilter{
				TickSize: decimal.NewFromFloat(0.0001),
				StepSize: decimal.NewFromFloat(1),
			}, true
		}
		b.GetBestQuoteFn = func(symbol string) (float64, float64, bool) {
			return 0.9960, 0.9962, true // LONG target bid=0.9960 -> -0.40% jauh di atas 0.05%
		}

		intent := model.OrderIntent{
			ClientOrderID: "REPEG_DRIFT_REJECT",
			Symbol:        "SYM3USDT",
			Side:          model.SideLong,
			Qty:           decimal.NewFromFloat(10),
			LimitPrice:    decimal.NewFromFloat(1.0000),
			SLPrice:       decimal.NewFromFloat(0.9800),
			TPPrice:       decimal.NewFromFloat(1.0400),
			ATRAtEntry:    decimal.NewFromFloat(0.015),
			CreatedAt:     now,
		}
		ok, err := b.SubmitLimitEntry(intent)
		if ok || err == nil {
			t.Fatalf("expected drift rejection, got ok=%v err=%v", ok, err)
		}
		if b.GetActiveSlotCount() != 0 || b.HasActiveOrderOrPosition("SYM3USDT") {
			t.Fatalf("expected 0 active slots after rejected entry")
		}
	})
}

func TestDryRunBroker_AttachSignalAuditID(t *testing.T) {
	b := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:        3,
		AccountLeverage: 8.0,
	})
	defer b.Close()

	now := time.Now().UTC()
	intent := model.OrderIntent{
		ClientOrderID: "CID_AUDIT_DRY",
		Symbol:        "ADAUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10),
		LimitPrice:    decimal.NewFromFloat(1.000),
		SLPrice:       decimal.NewFromFloat(0.980),
		TPPrice:       decimal.NewFromFloat(1.040),
		ATRAtEntry:    decimal.NewFromFloat(0.015),
		CreatedAt:     now,
	}
	if ok, err := b.SubmitLimitEntry(intent); !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}

	b.OnRealtimeTick("ADAUSDT", 1.000, 1.001, 1.000, 0, 0, false, now.Add(10*time.Millisecond))
	b.AttachSignalAuditID("ADAUSDT", "CID_AUDIT_DRY", 777)

	// Fill posisi -> SignalAuditID harus terbawa ke OpenPosition
	b.OnRealtimeTick("ADAUSDT", 1.000, 1.001, 1.000, 1.000, 15, true, now.Add(1*time.Second))
	b.Mu.Lock()
	pos := b.OpenPositions["ADAUSDT"]
	b.Mu.Unlock()
	if pos == nil || pos.SignalAuditID != 777 {
		t.Fatalf("expected opened position to inherit SignalAuditID=777, got %+v", pos)
	}
}

func TestLiveBroker_PreRepegAndGTX5022RetryWithAttachAuditID(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
	})
	defer b.Close()

	if b.Cfg.LimitOrderTimeout != 150*time.Second {
		t.Fatalf("expected LiveBroker default LimitOrderTimeout 150s, got %s", b.Cfg.LimitOrderTimeout)
	}

	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize: decimal.NewFromFloat(0.0001),
			StepSize: decimal.NewFromFloat(1),
		}, true
	}

	var quoteCalls atomic.Int32
	b.GetBestQuoteFn = func(symbol string) (float64, float64, bool) {
		if quoteCalls.Add(1) == 1 {
			return 0.9999, 1.0001, true // Pre-submit aman (limit=1.0000 > bid=0.9999)
		}
		return 1.0002, 1.0003, true // Saat retry -5022, ask naik ke 1.0003 (+0.03% <= 0.05%)
	}

	var attempts atomic.Int32
	var seenClientIDs []string
	var seenLimits []decimal.Decimal
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		n := attempts.Add(1)
		seenClientIDs = append(seenClientIDs, intent.ClientOrderID)
		seenLimits = append(seenLimits, intent.LimitPrice)
		if n == 1 {
			return 0, fmt.Errorf("binance API error: code=-5022 msg=Due to the order could not be executed as maker, the Post Only order will be rejected.")
		}
		return 555111, nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "LIVE_GTX_1",
		Symbol:        "SOLUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(10),
		LimitPrice:    decimal.NewFromFloat(1.0000),
		SLPrice:       decimal.NewFromFloat(1.0200),
		TPPrice:       decimal.NewFromFloat(0.9600),
		TP1Price:      decimal.NewFromFloat(0.9800),
		ATRAtEntry:    decimal.NewFromFloat(0.015),
		CreatedAt:     now,
	}

	ok, err := b.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("expected live GTX retry to succeed, got ok=%v err=%v", ok, err)
	}
	if len(seenClientIDs) != 2 || seenClientIDs[0] != "LIVE_GTX_1" || !strings.HasPrefix(seenClientIDs[1], "live_repeg_SOLUSDT_") {
		t.Fatalf("unexpected retry ClientOrderIDs: %v", seenClientIDs)
	}
	if !seenLimits[1].Equal(decimal.NewFromFloat(1.0003)) {
		t.Fatalf("expected retry LimitPrice 1.0003, got %s", seenLimits[1])
	}

	b.AttachSignalAuditID("SOLUSDT", "LIVE_GTX_1", 888)

	b.Mu.Lock()
	defer b.Mu.Unlock()
	if _, leaked := b.OrdersByClient["LIVE_GTX_1"]; leaked {
		t.Fatalf("old clientOrderID LIVE_GTX_1 should be removed after re-peg swap")
	}
	newCID := seenClientIDs[1]
	rec := b.OrdersByClient[newCID]
	if rec == nil || rec.ExchangeOrderID != 555111 || !rec.SLPrice.Equal(decimal.NewFromFloat(1.0203)) || rec.SignalAuditID != 888 {
		t.Fatalf("unexpected live order record after repeg+audit: %+v", rec)
	}
	if b.PositionClientOrderIDs["SOLUSDT"] != newCID {
		t.Fatalf("expected PositionClientOrderIDs updated to %s, got %q", newCID, b.PositionClientOrderIDs["SOLUSDT"])
	}
}

func TestLiveBroker_PreRepegExceedsMaxDriftAbortsCleanly(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:              3,
		MaxDailyDrawdownPct:   -0.05,
		MaxAccountStaleness:   60 * time.Second,
		MaxEntryRepegAttempts: 3,
		MaxEntryRepegDriftPct: 0.0005,
	})
	defer b.Close()

	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize: decimal.NewFromFloat(0.0001),
			StepSize: decimal.NewFromFloat(1),
		}, true
	}
	b.GetBestQuoteFn = func(symbol string) (float64, float64, bool) {
		return 1.0050, 1.0060, true // +0.60% jauh melampaui batas 0.05%
	}
	var placeCalls atomic.Int32
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		placeCalls.Add(1)
		return 1, nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "LIVE_DRIFT_ABORT",
		Symbol:        "DOGEUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(10),
		LimitPrice:    decimal.NewFromFloat(1.0000),
		SLPrice:       decimal.NewFromFloat(1.0200),
		TPPrice:       decimal.NewFromFloat(0.9600),
		ATRAtEntry:    decimal.NewFromFloat(0.015),
		CreatedAt:     now,
	}

	ok, err := b.SubmitLimitEntry(intent)
	if ok || err == nil || !strings.Contains(err.Error(), "drift_") {
		t.Fatalf("expected pre-repeg drift rejection, got ok=%v err=%v", ok, err)
	}
	if placeCalls.Load() != 0 {
		t.Fatalf("expected 0 PlaceEntryFn calls when pre-repeg aborts, got %d", placeCalls.Load())
	}
	if b.GetActiveSlotCount() != 0 || b.HasActiveOrderOrPosition("DOGEUSDT") {
		t.Fatalf("expected 0 reserved slots after abort")
	}
}

func TestLiveBroker_OfflineReconciliation_MissedFill_And_OfflineClose(t *testing.T) {
	t.Run("timeout -2011 checks QueryOrderFn and arms SL/TP if filled during offline outage", func(t *testing.T) {
		b := NewBinanceLiveBroker(LiveBrokerConfig{
			MaxSlots:            3,
			MaxDailyDrawdownPct: -0.05,
			MaxAccountStaleness: 60 * time.Second,
			LimitOrderTimeout:   150 * time.Second,
		})
		defer b.Close()

		t0 := time.Now().UTC().Add(-200 * time.Second)
		b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, time.Now().UTC())

		b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
			return 444111, nil
		}
		b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
			return fmt.Errorf("binance error code=-2011 Unknown order sent")
		}
		b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
			return &model.LiveOrderRecord{
				ClientOrderID:       clientOrderID,
				ExchangeOrderID:     444111,
				Symbol:              symbol,
				Side:                model.SideShort,
				IsEntry:             true,
				Status:              model.StatusFilled,
				TargetQty:           decimal.NewFromFloat(5),
				CumulativeFilledQty: decimal.NewFromFloat(5),
				AvgPrice:            decimal.NewFromFloat(100.0),
			}, nil
		}
		b.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
			return decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(950.0), map[string]model.LivePosition{
				"SOLUSDT": {Symbol: "SOLUSDT", Side: model.SideShort, Qty: decimal.NewFromFloat(5), EntryPrice: decimal.NewFromFloat(100.0)},
			}, nil
		}

		var slPlaced, tpPlaced atomic.Int32
		b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
			if orderType == "STOP_MARKET" {
				slPlaced.Add(1)
				return 801, nil
			}
			tpPlaced.Add(1)
			return 802, nil
		}

		ok, err := b.SubmitLimitEntry(model.OrderIntent{
			ClientOrderID: "OFFLINE_FILL_SOL",
			Symbol:        "SOLUSDT",
			Side:          model.SideShort,
			Qty:           decimal.NewFromFloat(5),
			LimitPrice:    decimal.NewFromFloat(100.0),
			SLPrice:       decimal.NewFromFloat(103.0),
			TPPrice:       decimal.NewFromFloat(94.0),
			ATRAtEntry:    decimal.NewFromFloat(2.0),
			CreatedAt:     t0,
		})
		if !ok || err != nil {
			t.Fatalf("SubmitLimitEntry failed: ok=%v err=%v", ok, err)
		}
		b.Mu.Lock()
		b.OrdersByClient["OFFLINE_FILL_SOL"].CreatedAt = t0
		b.Mu.Unlock()

		// Simulasikan internet pulih setelah 200 detik dan Worker 4 mengecek timeout
		b.CheckPendingEntryTimeouts(time.Now().UTC())

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if slPlaced.Load() > 0 && tpPlaced.Load() > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if slPlaced.Load() != 1 || tpPlaced.Load() != 1 {
			t.Fatalf("expected SL & TP algo orders armed after -2011 QueryOrder recovery, got SL=%d TP=%d", slPlaced.Load(), tpPlaced.Load())
		}
		b.Mu.Lock()
		prot := b.Protections["SOLUSDT"]
		b.Mu.Unlock()
		if prot == nil || prot.SLStatus != model.ProtectionAcked {
			t.Fatalf("expected ProtectionAcked on SOLUSDT, got %+v", prot)
		}
	})

	t.Run("RefreshAccount cleans offline-closed position and cancels orphan algo orders", func(t *testing.T) {
		b := NewBinanceLiveBroker(LiveBrokerConfig{
			MaxSlots:            3,
			MaxDailyDrawdownPct: -0.05,
			MaxAccountStaleness: 60 * time.Second,
		})
		defer b.Close()

		deletedCh := make(chan string, 2)
		b.OnPositionDeletedFn = func(sym string) { deletedCh <- sym }

		var canceledAlgos []int64
		var mu sync.Mutex
		b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
			mu.Lock()
			canceledAlgos = append(canceledAlgos, algoID)
			mu.Unlock()
			return nil
		}
		var canceledTP1 int64
		b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
			canceledTP1 = orderID
			return nil
		}

		// Posisi BTCUSDT aktif & terproteksi sejak 5 menit lalu (> grace 15s)
		b.RestoreOpenPosition(&LiveOpenPosition{
			Symbol:         "BTCUSDT",
			Side:           model.SideShort,
			Qty:            decimal.NewFromFloat(0.5),
			InitialQty:     decimal.NewFromFloat(0.5),
			EntryPrice:     decimal.NewFromFloat(65000.0),
			SLOrderID:      701,
			SLClientAlgoID: "ALGO_SL_701",
			SLPrice:        decimal.NewFromFloat(66000.0),
			TPOrderID:      702,
			TPClientAlgoID: "ALGO_TP_702",
			TPPrice:        decimal.NewFromFloat(63000.0),
			TP1OrderID:     703,
			TP1Price:       decimal.NewFromFloat(64000.0),
			ATRAtEntry:     decimal.NewFromFloat(500.0),
			OpenTime:       time.Now().UTC().Add(-5 * time.Minute),
		})

		// Snapshot REST Binance mengembalikan positions kosong (sudah tutup SL/TP di bursa saat offline)
		b.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
			return decimal.NewFromFloat(990.0), decimal.Zero, decimal.NewFromFloat(990.0), map[string]model.LivePosition{}, nil
		}

		if err := b.RefreshAccount(context.Background()); err != nil {
			t.Fatalf("RefreshAccount failed: %v", err)
		}

		b.Mu.Lock()
		_, stillActive := b.ActivePositions["BTCUSDT"]
		_, stillProt := b.Protections["BTCUSDT"]
		b.Mu.Unlock()
		if stillActive || stillProt {
			t.Fatalf("expected stale BTCUSDT position and protection cleared, active=%v prot=%v", stillActive, stillProt)
		}
		select {
		case deletedSym := <-deletedCh:
			if deletedSym != "BTCUSDT" {
				t.Fatalf("expected OnPositionDeletedFn('BTCUSDT'), got %q", deletedSym)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for OnPositionDeletedFn('BTCUSDT')")
		}
		mu.Lock()
		gotAlgos := append([]int64(nil), canceledAlgos...)
		mu.Unlock()
		if len(gotAlgos) != 2 || canceledTP1 != 703 {
			t.Fatalf("expected orphan SL/TP algos (701,702) and TP1 (703) canceled, got algos=%v tp1=%d", gotAlgos, canceledTP1)
		}
	})

	t.Run("timeout -2011 checks QueryOrderFn alone even when FetchAccountFn nil", func(t *testing.T) {
		b := NewBinanceLiveBroker(LiveBrokerConfig{
			MaxSlots:            3,
			MaxDailyDrawdownPct: -0.05,
			MaxAccountStaleness: 60 * time.Second,
			LimitOrderTimeout:   150 * time.Second,
		})
		defer b.Close()

		t0 := time.Now().UTC().Add(-200 * time.Second)
		b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, time.Now().UTC())
		b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
			return 444222, nil
		}
		b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
			return fmt.Errorf("binance API error code=-2011 msg=Unknown order sent")
		}
		b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
			return &model.LiveOrderRecord{
				ClientOrderID:       clientOrderID,
				ExchangeOrderID:     444222,
				Symbol:              symbol,
				Side:                model.SideShort,
				IsEntry:             true,
				Status:              model.StatusFilled,
				TargetQty:           decimal.NewFromFloat(2),
				CumulativeFilledQty: decimal.NewFromFloat(2),
				AvgPrice:            decimal.NewFromFloat(2000.0),
			}, nil
		}
		var slPlaced atomic.Int32
		b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
			if orderType == "STOP_MARKET" {
				slPlaced.Add(1)
			}
			return 901, nil
		}

		if ok, err := b.SubmitLimitEntry(model.OrderIntent{
			ClientOrderID: "OFFLINE_ETH_2011",
			Symbol:        "ETHUSDT",
			Side:          model.SideShort,
			Qty:           decimal.NewFromFloat(2),
			LimitPrice:    decimal.NewFromFloat(2000.0),
			SLPrice:       decimal.NewFromFloat(2060.0),
			TPPrice:       decimal.NewFromFloat(1880.0),
			ATRAtEntry:    decimal.NewFromFloat(40.0),
			CreatedAt:     t0,
		}); !ok || err != nil {
			t.Fatalf("SubmitLimitEntry failed: %v", err)
		}
		b.Mu.Lock()
		b.OrdersByClient["OFFLINE_ETH_2011"].CreatedAt = t0
		b.Mu.Unlock()

		b.CheckPendingEntryTimeouts(time.Now().UTC())
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if slPlaced.Load() > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if slPlaced.Load() != 1 {
			t.Fatalf("expected SL algo order placed via QueryOrderFn fallback, got %d", slPlaced.Load())
		}
	})
}

func TestDryRunBroker_CircuitBreakerCancelsPendingOrders(t *testing.T) {
	b := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.03,
		AccountLeverage:     5.0,
		LimitOrderTimeout:   150 * time.Second,
	})
	defer b.Close()

	now := time.Now().UTC()
	b.Mu.Lock()
	b.SessionDayEpochUTC = now.Unix() / 86400
	b.SessionStartEquity = decimal.NewFromFloat(1000.0)
	b.Mu.Unlock()

	ok, err := b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "CB_PENDING_1",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		SLPrice:       decimal.NewFromFloat(2050.0),
		TPPrice:       decimal.NewFromFloat(1900.0),
		ATRAtEntry:    decimal.NewFromFloat(25.0),
		CreatedAt:     now,
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: ok=%v err=%v", ok, err)
	}

	// Turunkan realized equity menembus -3% drawdown (-40 USDT -> -4%)
	b.Mu.Lock()
	b.RealizedEquityUSDT = decimal.NewFromFloat(960.0)
	b.Mu.Unlock()

	// Tick aggTrade yang seharusnya mengisi order tidak boleh mengisi karena CB harus membatalkan PendingOrders
	b.OnRealtimeTick("ETHUSDT", 1999.0, 2000.0, 1999.5, 2000.0, 5.0, false, now.Add(1*time.Second))

	b.Mu.Lock()
	pendingCount := len(b.PendingOrders)
	openCount := len(b.OpenPositions)
	tripped := b.CircuitBreakerTripped
	b.Mu.Unlock()
	if !tripped || pendingCount != 0 || openCount != 0 {
		t.Fatalf("expected CB tripped and all pending orders canceled without filling, tripped=%v pending=%d open=%d", tripped, pendingCount, openCount)
	}
}

func TestLiveBroker_ReserveEntryIntent_NoPartialFillDoubleCount(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            2,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
	})
	defer b.Close()

	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)

	// Simulasikan BTCUSDT sudah partial fill (ada di ActivePositions DAN OrdersByClient StatusPartiallyFilled) -> harus tetap 1 slot
	b.Mu.Lock()
	b.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(0.2)
	b.OrdersByClient["BTC_PARTIAL"] = &model.LiveOrderRecord{
		ClientOrderID:       "BTC_PARTIAL",
		ExchangeOrderID:     101,
		Symbol:              "BTCUSDT",
		Side:                model.SideShort,
		IsEntry:             true,
		Status:              model.StatusPartiallyFilled,
		TargetQty:           decimal.NewFromFloat(1.0),
		CumulativeFilledQty: decimal.NewFromFloat(0.2),
		CreatedAt:           now,
	}
	b.Mu.Unlock()

	// Slot ke-2 (ETHUSDT) wajib berhasil direservasi (tidak kena double-count 2 >= 2)
	ok, err := b.ReserveEntryIntent(model.OrderIntent{
		ClientOrderID: "ETH_SLOT_2",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(2000.0),
		SLPrice:       decimal.NewFromFloat(2050.0),
		TPPrice:       decimal.NewFromFloat(1900.0),
		ATRAtEntry:    decimal.NewFromFloat(25.0),
		CreatedAt:     now,
	})
	if !ok || err != nil {
		t.Fatalf("expected 2nd distinct slot ETHUSDT reserved when BTCUSDT is partially filled, got ok=%v err=%v", ok, err)
	}
}

func TestLiveBroker_CheckPendingEntryTimeouts_PartialFillAndOfflineCloseCleanup(t *testing.T) {
	t.Run("cancels StatusPartiallyFilled remainder and updates InitialQtys", func(t *testing.T) {
		b := NewBinanceLiveBroker(LiveBrokerConfig{
			MaxSlots:            3,
			MaxDailyDrawdownPct: -0.05,
			MaxAccountStaleness: 60 * time.Second,
			LimitOrderTimeout:   150 * time.Second,
		})
		defer b.Close()

		t0 := time.Now().UTC().Add(-200 * time.Second)
		var cancelCalls atomic.Int32
		b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
			cancelCalls.Add(1)
			return nil
		}

		b.Mu.Lock()
		b.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(4.0)
		b.InitialQtys["SOLUSDT"] = decimal.NewFromFloat(10.0)
		b.OrdersByClient["SOL_PARTIAL"] = &model.LiveOrderRecord{
			ClientOrderID:       "SOL_PARTIAL",
			ExchangeOrderID:     505,
			Symbol:              "SOLUSDT",
			Side:                model.SideShort,
			IsEntry:             true,
			Status:              model.StatusPartiallyFilled,
			TargetQty:           decimal.NewFromFloat(10.0),
			CumulativeFilledQty: decimal.NewFromFloat(4.0),
			CreatedAt:           t0,
		}
		b.OrdersByExchangeID[505] = b.OrdersByClient["SOL_PARTIAL"]
		b.Mu.Unlock()

		b.CheckPendingEntryTimeouts(time.Now().UTC())
		if cancelCalls.Load() != 1 {
			t.Fatalf("expected 1 CancelOrderFn call for timed-out StatusPartiallyFilled order, got %d", cancelCalls.Load())
		}
		b.Mu.Lock()
		_, stillInMap := b.OrdersByClient["SOL_PARTIAL"]
		initQty := b.InitialQtys["SOLUSDT"]
		b.Mu.Unlock()
		if stillInMap || !initQty.Equal(decimal.NewFromFloat(4.0)) {
			t.Fatalf("expected partial order deleted and InitialQtys updated to 4.0, stillInMap=%v initQty=%s", stillInMap, initQty)
		}
	})

	t.Run("-2011 filled and already closed offline cleans up order without infinite loop", func(t *testing.T) {
		b := NewBinanceLiveBroker(LiveBrokerConfig{
			MaxSlots:            3,
			MaxDailyDrawdownPct: -0.05,
			MaxAccountStaleness: 60 * time.Second,
			LimitOrderTimeout:   150 * time.Second,
		})
		defer b.Close()

		t0 := time.Now().UTC().Add(-200 * time.Second)
		var cancelCalls atomic.Int32
		b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
			cancelCalls.Add(1)
			return fmt.Errorf("binance error code=-2011 Unknown order sent")
		}
		b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
			return &model.LiveOrderRecord{
				ClientOrderID:       clientOrderID,
				ExchangeOrderID:     606,
				Symbol:              symbol,
				Side:                model.SideShort,
				IsEntry:             true,
				Status:              model.StatusFilled,
				TargetQty:           decimal.NewFromFloat(5),
				CumulativeFilledQty: decimal.NewFromFloat(5),
				AvgPrice:            decimal.NewFromFloat(100.0),
			}, nil
		}
		b.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
			return decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), map[string]model.LivePosition{}, nil
		}

		b.Mu.Lock()
		b.OrdersByClient["OFFLINE_CLOSED_XRP"] = &model.LiveOrderRecord{
			ClientOrderID:   "OFFLINE_CLOSED_XRP",
			ExchangeOrderID: 606,
			Symbol:          "XRPUSDT",
			Side:            model.SideShort,
			IsEntry:         true,
			Status:          model.StatusNew,
			TargetQty:       decimal.NewFromFloat(5),
			CreatedAt:       t0,
		}
		b.OrdersByExchangeID[606] = b.OrdersByClient["OFFLINE_CLOSED_XRP"]
		b.Mu.Unlock()

		b.CheckPendingEntryTimeouts(time.Now().UTC())
		// Panggilan ke-2 tidak boleh lagi memanggil CancelOrderFn (tidak infinite loop)
		b.CheckPendingEntryTimeouts(time.Now().UTC())

		if cancelCalls.Load() != 1 {
			t.Fatalf("expected exactly 1 CancelOrderFn call (cleaned up after offline close), got %d", cancelCalls.Load())
		}
		if b.GetActiveSlotCount() != 0 {
			t.Fatalf("expected 0 active slots after offline-closed cleanup, got %d", b.GetActiveSlotCount())
		}
	})
}

func TestLiveBroker_ExecuteProtectionPlan_GhostOrderGuardOnStaleGeneration(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
	})
	defer b.Close()

	var mu sync.Mutex
	var canceledAlgos []int64
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		// Simulasikan PlanGeneration sudah naik ke 2 saat order baru sedang dikirim
		b.Mu.Lock()
		if p := b.Protections[symbol]; p != nil {
			p.PlanGeneration = 2
		}
		b.Mu.Unlock()
		return 9001, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		mu.Lock()
		canceledAlgos = append(canceledAlgos, algoID)
		mu.Unlock()
		return nil
	}

	b.Mu.Lock()
	b.Protections["ADAUSDT"] = &model.LivePositionProtection{
		Symbol:         "ADAUSDT",
		Side:           model.SideShort,
		PlanGeneration: 1,
		SLOrderID:      1001,
		SLStatus:       model.ProtectionAcked,
	}
	b.Mu.Unlock()

	b.executeProtectionPlan(model.ProtectionPlanAction{
		Symbol:         "ADAUSDT",
		Generation:     1,
		CancelSLID:     1001,
		NeedNewSL:      true,
		ExitSide:       model.SideLong,
		TargetQty:      decimal.NewFromFloat(100),
		SLPrice:        decimal.NewFromFloat(0.55),
		ClientSLAlgoID: "ALGO_SL_ADA_STALE",
	})

	mu.Lock()
	got := append([]int64(nil), canceledAlgos...)
	mu.Unlock()
	// Wajib membatalkan orphan baru (9001) dan TIDAK boleh membatalkan SL lama (1001)
	if len(got) != 1 || got[0] != 9001 {
		t.Fatalf("expected only orphan new SL 9001 canceled (and old SL 1001 preserved), got %v", got)
	}
}

func TestLiveBroker_ToProtectionConfigUsesConfiguredFeesAndSlippage(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:     3,
		TakerFeeRate: 0.0003,
		SlippageRate: 0.0001,
	})
	defer b.Close()

	pc := b.toProtectionConfig()
	if !pc.TakerFeeRate.Equal(decimal.NewFromFloat(0.0003)) || !pc.SlippageRate.Equal(decimal.NewFromFloat(0.0001)) {
		t.Fatalf("expected TakerFeeRate=0.0003 SlippageRate=0.0001, got taker=%s slip=%s", pc.TakerFeeRate, pc.SlippageRate)
	}
}

func TestLiveBroker_ProcessTP1Executed_FullCloseCleansUpAndCancelsSiblings(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		PartialTPEnabled:    true,
		PartialTP1Ratio:     0.50,
	})
	defer b.Close()

	var mu sync.Mutex
	var canceledAlgos []int64
	var canceledOrders []string
	var closedRec TradeClosedRecord
	var cooldownSym, cooldownReason string
	closedCh := make(chan struct{}, 1)

	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		mu.Lock()
		canceledAlgos = append(canceledAlgos, algoID)
		mu.Unlock()
		return nil
	}
	b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
		mu.Lock()
		canceledOrders = append(canceledOrders, clientOrderID)
		mu.Unlock()
		return nil
	}
	b.OnCooldownRecordedFn = func(symbol string, closedAt time.Time, exitReason string) {
		mu.Lock()
		cooldownSym = symbol
		cooldownReason = exitReason
		mu.Unlock()
	}
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		mu.Lock()
		closedRec = rec
		mu.Unlock()
		closedCh <- struct{}{}
	}

	b.Mu.Lock()
	b.ActivePositions["DOGEUSDT"] = decimal.NewFromFloat(100)
	b.PositionSides["DOGEUSDT"] = model.SideShort
	b.PositionEntryPrices["DOGEUSDT"] = decimal.NewFromFloat(0.20)
	b.PositionOpenTimes["DOGEUSDT"] = time.Now().UTC().Add(-10 * time.Minute)
	b.Protections["DOGEUSDT"] = &model.LivePositionProtection{
		Symbol:         "DOGEUSDT",
		Side:           model.SideShort,
		SLOrderID:      5501,
		TPOrderID:      5502,
		SLPrice:        decimal.NewFromFloat(0.21),
		TPPrice:        decimal.NewFromFloat(0.18),
	}
	b.OrdersByClient["DOGE_PENDING_ENTRY"] = &model.LiveOrderRecord{
		ClientOrderID:   "DOGE_PENDING_ENTRY",
		ExchangeOrderID: 5509,
		Symbol:          "DOGEUSDT",
		IsEntry:         true,
		Status:          model.StatusNew,
	}
	b.Mu.Unlock()

	// TP1 menutup 100% sisa posisi (100 qty) -> wajib bersihkan semua map & batalkan sibling SL/TP & pending entry
	b.processTP1Executed("DOGEUSDT", decimal.NewFromFloat(100), decimal.NewFromFloat(0.19), "ALGO_TP1_DOGE", true)

	select {
	case <-closedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for OnTradeClosedFn")
	}

	mu.Lock()
	defer mu.Unlock()
	if cooldownSym != "DOGEUSDT" || cooldownReason != "TAKE_PROFIT" {
		t.Fatalf("expected TAKE_PROFIT cooldown recorded synchronously, got sym=%s reason=%s", cooldownSym, cooldownReason)
	}
	if closedRec.ExitReason != "TAKE_PROFIT" || !closedRec.Qty.Equal(decimal.NewFromFloat(100)) {
		t.Fatalf("unexpected trade closed record: %+v", closedRec)
	}
	if len(canceledAlgos) != 2 {
		t.Fatalf("expected both sibling SL & TP algo orders canceled, got %v", canceledAlgos)
	}
	if len(canceledOrders) != 1 || canceledOrders[0] != "DOGE_PENDING_ENTRY" {
		t.Fatalf("expected pending entry order canceled, got %v", canceledOrders)
	}
	if b.HasActiveOrderOrPosition("DOGEUSDT") {
		t.Fatalf("expected DOGEUSDT completely cleared from active positions/orders")
	}
}

func TestLiveBroker_ReconcileAfterAccountSnapshot_PartialFillStaysPartiallyFilled(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		LimitOrderTimeout:   150 * time.Second,
	})
	defer b.Close()

	var cancelCalls atomic.Int32
	b.CancelOrderFn = func(ctx context.Context, symbol string, orderID int64, clientOrderID string) error {
		cancelCalls.Add(1)
		return nil
	}

	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000.0), decimal.Zero, decimal.NewFromFloat(1000.0), nil, now)
	b.Mu.Lock()
	b.OrdersByClient["ENTRY_PARTIAL_MISS"] = &model.LiveOrderRecord{
		ClientOrderID:   "ENTRY_PARTIAL_MISS",
		ExchangeOrderID: 8001,
		Symbol:          "SOLUSDT",
		Side:            model.SideShort,
		IsEntry:         true,
		Status:          model.StatusNew,
		TargetQty:       decimal.NewFromFloat(10),
		LimitPrice:      decimal.NewFromFloat(150),
		SLPrice:         decimal.NewFromFloat(155),
		TPPrice:         decimal.NewFromFloat(140),
		ATRAtEntry:      decimal.NewFromFloat(3.0),
		CreatedAt:       now.Add(-200 * time.Second),
	}
	b.Mu.Unlock()

	// Snapshot menunjukkan baru terisi 4 dari 10 -> harus tetap StatusPartiallyFilled sehingga timeout membatalkan sisa 6
	b.reconcileAfterAccountSnapshot(map[string]model.LivePosition{
		"SOLUSDT": {
			Symbol:     "SOLUSDT",
			Side:       model.SideShort,
			Qty:        decimal.NewFromFloat(4),
			EntryPrice: decimal.NewFromFloat(150),
		},
	}, now)

	b.Mu.Lock()
	st := b.OrdersByClient["ENTRY_PARTIAL_MISS"].Status
	b.Mu.Unlock()
	if st != model.StatusPartiallyFilled {
		t.Fatalf("expected StatusPartiallyFilled after partial snapshot reconcile, got %s", st)
	}

	b.CheckPendingEntryTimeouts(now)
	if cancelCalls.Load() != 1 {
		t.Fatalf("expected CheckPendingEntryTimeouts to cancel remaining partial order, got %d", cancelCalls.Load())
	}
}

func TestLiveBroker_SubmitLimitEntry_QueryRecoveryIgnoresTerminalZeroFill(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:            2,
		MaxDailyDrawdownPct: -0.05,
		MaxAccountStaleness: 60 * time.Second,
		EntryOrderType:      "LIMIT",
	})
	defer b.Close()

	now := time.Now().UTC()
	b.HandleAccountUpdate(decimal.NewFromFloat(1000), decimal.Zero, decimal.NewFromFloat(1000), nil, now)
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		return 0, fmt.Errorf("request timeout")
	}
	b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
		return &model.LiveOrderRecord{
			ClientOrderID:       clientOrderID,
			ExchangeOrderID:     9011,
			Symbol:              symbol,
			Status:              model.StatusCanceled,
			CumulativeFilledQty: decimal.Zero,
		}, nil
	}

	ok, err := b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "ENTRY_CANCELED_ZERO",
		Symbol:        "ETHUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1),
		LimitPrice:    decimal.NewFromFloat(2000),
		SLPrice:       decimal.NewFromFloat(2050),
		TPPrice:       decimal.NewFromFloat(1900),
		ATRAtEntry:    decimal.NewFromFloat(25),
		CreatedAt:     now,
	})
	if ok || err == nil {
		t.Fatalf("expected terminal zero-fill order to rollback cleanly, got ok=%v err=%v", ok, err)
	}
	if b.GetActiveSlotCount() != 0 {
		t.Fatalf("expected 0 active slots after terminal zero-fill rollback, got %d", b.GetActiveSlotCount())
	}
}

func TestRepegLimitEntryIntent_RejectsBelowMinNotional(t *testing.T) {
	intent := model.OrderIntent{
		ClientOrderID: "REPEG_MIN_NOTIONAL",
		Symbol:        "ADAUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(5.01),
		LimitPrice:    decimal.NewFromFloat(1.000),
		SLPrice:       decimal.NewFromFloat(0.950),
		TPPrice:       decimal.NewFromFloat(1.100),
		TP1Price:      decimal.NewFromFloat(1.040),
		ATRAtEntry:    decimal.NewFromFloat(0.050),
	}
	ef := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.001),
		StepSize:    decimal.NewFromFloat(0.01),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	// bestBid=0.997, bestAsk=1.001 -> repeg LONG memindahkan LimitPrice turun ke 0.997 -> notional = 5.01*0.997 = 4.99497 < 5.0
	_, ok, reason := repegLimitEntryIntent(intent, intent.LimitPrice, 0.997, 1.001, ef, 0.005)
	if ok || !strings.Contains(reason, "below_min") {
		t.Fatalf("expected repeg rejected below MinNotional, got ok=%v reason=%s", ok, reason)
	}
}

func TestLiveBroker_WSAccountUpdatePartialDoesNotClobberUPnLOrAvailBal(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:        3,
		AccountLeverage: 8.0,
	})

	var restCalls atomic.Int32
	gate := make(chan struct{})
	b.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
		restCalls.Add(1)
		<-gate
		return decimal.NewFromFloat(1000.0), decimal.NewFromFloat(21.0), decimal.NewFromFloat(800.0), map[string]model.LivePosition{
			"BTCUSDT": {Symbol: "BTCUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(1.0), EntryPrice: decimal.NewFromFloat(100.0), UnrealizedPnL: decimal.NewFromFloat(25.0)},
			"ETHUSDT": {Symbol: "ETHUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(1.0), EntryPrice: decimal.NewFromFloat(50.0), UnrealizedPnL: decimal.NewFromFloat(-4.0)},
		}, nil
	}

	t0 := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance:    decimal.NewFromFloat(1000.0),
		AvailableBalance: decimal.NewFromFloat(850.0),
		UnrealizedPnL:    decimal.NewFromFloat(6.0),
		EventTime:        t0,
		IsFullSnapshot:   true,
		Positions: map[string]model.LivePosition{
			"BTCUSDT": {Symbol: "BTCUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(1.0), EntryPrice: decimal.NewFromFloat(100.0), UnrealizedPnL: decimal.NewFromFloat(10.0)},
			"ETHUSDT": {Symbol: "ETHUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(1.0), EntryPrice: decimal.NewFromFloat(50.0), UnrealizedPnL: decimal.NewFromFloat(-4.0)},
		},
	})

	b.Mu.Lock()
	b.ReservedEntryMargins["PEND_SOL"] = decimal.NewFromFloat(50.0)
	b.Mu.Unlock()

	// Partial WS ACCOUNT_UPDATE: only BTCUSDT changed, AvailableBalance=0 (not authoritative on WS delta)
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance:    decimal.NewFromFloat(1000.0),
		AvailableBalance: decimal.Zero,
		UnrealizedPnL:    decimal.Zero,
		EventTime:        t0.Add(2 * time.Second),
		IsFullSnapshot:   false,
		Positions: map[string]model.LivePosition{
			"BTCUSDT": {Symbol: "BTCUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(1.0), EntryPrice: decimal.NewFromFloat(100.0), UnrealizedPnL: decimal.NewFromFloat(25.0)},
		},
	})

	b.Mu.Lock()
	totalUPnL := b.UnrealizedPnLUSDT
	ethUPnL := b.PositionUnrealizedPnLs["ETHUSDT"]
	rawAvail := b.AvailableBalanceUSDT
	b.Mu.Unlock()

	if !totalUPnL.Equal(decimal.NewFromFloat(21.0)) {
		t.Fatalf("expected combined uPnL 21.0 (25 + (-4)), got %s", totalUPnL)
	}
	if !ethUPnL.Equal(decimal.NewFromFloat(-4.0)) {
		t.Fatalf("expected ETHUSDT uPnL -4.0 preserved, got %s", ethUPnL)
	}
	if rawAvail.LessThanOrEqual(decimal.Zero) {
		t.Fatalf("expected AvailableBalanceUSDT NOT overwritten to 0 by partial WS delta, got %s", rawAvail)
	}
	effAvail := b.GetAvailableBalanceUSDT()
	if effAvail.GreaterThan(rawAvail.Sub(decimal.NewFromFloat(50.0))) {
		t.Fatalf("expected GetAvailableBalanceUSDT (%s) to deduct ReservedEntryMargins 50.0 from %s", effAvail, rawAvail)
	}

	close(gate)
	b.Close()
	if restCalls.Load() != 1 {
		t.Fatalf("expected 1 debounced background RefreshAccount call, got %d", restCalls.Load())
	}
}

func TestLiveBroker_ExactNetPnLWithCommissionsAndMakerTP1(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		AccountLeverage:  8.0,
		EntryOrderType:   "LIMIT",
		PartialTPEnabled: true,
		PartialTP1Ratio:  0.5,
		MakerFeeRate:     0.0002,
		TakerFeeRate:     0.0005,
	})
	b.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
		return model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.01),
			MinNotional: decimal.NewFromFloat(5.0),
		}, true
	}

	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance:    decimal.NewFromFloat(1000.0),
		AvailableBalance: decimal.NewFromFloat(1000.0),
		EventTime:        time.Now().UTC(),
		IsFullSnapshot:   true,
	})

	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		return 1001, nil
	}
	tp1ClientCh := make(chan string, 1)
	var tpAlgoClientID string
	var protMu sync.Mutex
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		protMu.Lock()
		defer protMu.Unlock()
		if orderType == "STOP_MARKET" {
			return 2001, nil
		}
		tpAlgoClientID = clientAlgoID
		return 2002, nil
	}
	b.PlaceLimitCloseFn = func(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (int64, string, error) {
		tp1ClientCh <- clientOrderID
		return 3001, clientOrderID, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error { return nil }
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error { return nil }

	var recMu sync.Mutex
	var closed []TradeClosedRecord
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		recMu.Lock()
		closed = append(closed, rec)
		recMu.Unlock()
	}

	intent := model.OrderIntent{
		ClientOrderID: "COMM_ENTRY_1",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(2.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(90.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		TP1Price:      decimal.NewFromFloat(110.0),
		ATRAtEntry:    decimal.NewFromFloat(5.0),
		CreatedAt:     time.Now().UTC(),
	}
	ok, err := b.SubmitLimitEntry(intent)
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: ok=%v err=%v", ok, err)
	}

	// Maker LIMIT entry filled: actual commission = 0.40 USDT
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID:   "COMM_ENTRY_1",
		ExchangeOrderID: 1001,
		Symbol:          "ETHUSDT",
		Status:          model.StatusFilled,
		CumFilledQty:    decimal.NewFromFloat(2.0),
		LastFilledQty:   decimal.NewFromFloat(2.0),
		AvgPrice:        decimal.NewFromFloat(100.0),
		Commission:      decimal.NewFromFloat(0.40),
		CommissionAsset: "USDT",
		IsMaker:         true,
		HasFillMeta:     true,
	})

	var tp1ClientID string
	select {
	case tp1ClientID = <-tp1ClientCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for TP1 limit order placement")
	}

	// Wait briefly until TP algo order ID is also registered
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		protMu.Lock()
		gotTP := tpAlgoClientID
		protMu.Unlock()
		b.Mu.Lock()
		p := b.Protections["ETHUSDT"]
		hasBoth := p != nil && p.TPOrderID == 2002 && p.TP1OrderID == 3001
		b.Mu.Unlock()
		if gotTP != "" && hasBoth {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Maker TP1 fill (1.0 of 2.0): gross = (110-100)*1 = 10.0, pro-rated entryFee = 0.20, exitFee = 0.25 -> Net = 9.55
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID:   tp1ClientID,
		ExchangeOrderID: 3001,
		Symbol:          "ETHUSDT",
		Status:          model.StatusFilled,
		CumFilledQty:    decimal.NewFromFloat(1.0),
		LastFilledQty:   decimal.NewFromFloat(1.0),
		AvgPrice:        decimal.NewFromFloat(110.0),
		Commission:      decimal.NewFromFloat(0.25),
		CommissionAsset: "USDT",
		IsMaker:         true,
		HasFillMeta:     true,
	})

	// Runner TP full close (1.0 of 2.0): RealizedProfit = 20.0, pro-rated entryFee = 0.20, exitFee = 0.50 -> Net = 19.30
	protMu.Lock()
	tpClient := tpAlgoClientID
	protMu.Unlock()
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID:   tpClient,
		ExchangeOrderID: 2002,
		Symbol:          "ETHUSDT",
		Status:          model.StatusFilled,
		CumFilledQty:    decimal.NewFromFloat(1.0),
		LastFilledQty:   decimal.NewFromFloat(1.0),
		AvgPrice:        decimal.NewFromFloat(120.0),
		RealizedProfit:  decimal.NewFromFloat(20.0),
		Commission:      decimal.NewFromFloat(0.50),
		CommissionAsset: "USDT",
		IsMaker:         false,
		HasFillMeta:     true,
	})

	b.Close()
	recMu.Lock()
	defer recMu.Unlock()
	if len(closed) != 2 {
		t.Fatalf("expected 2 closed trade records (TP1 + runner), got %d: %+v", len(closed), closed)
	}
	var gotTP1, gotRunner *TradeClosedRecord
	for i := range closed {
		if closed[i].ExitReason == "PARTIAL_TAKE_PROFIT" {
			gotTP1 = &closed[i]
		} else if closed[i].ExitReason == "TAKE_PROFIT" {
			gotRunner = &closed[i]
		}
	}
	if gotTP1 == nil || !gotTP1.NetPnL.Equal(decimal.NewFromFloat(9.55)) {
		t.Fatalf("expected TP1 NetPnL 9.55, got %+v", gotTP1)
	}
	if gotRunner == nil || !gotRunner.NetPnL.Equal(decimal.NewFromFloat(19.30)) {
		t.Fatalf("expected Runner NetPnL 19.30, got %+v", gotRunner)
	}
}

func TestLiveBroker_ClosePositionZeroMapLeakReflection(t *testing.T) {
	assertAllBrokerMapsEmpty := func(t *testing.T, label string, b *BinanceLiveBroker) {
		t.Helper()
		v := reflect.ValueOf(b).Elem()
		vt := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Kind() == reflect.Map && f.Len() != 0 {
				t.Fatalf("%s: map field %s leaked %d entries: %v", label, vt.Field(i).Name, f.Len(), f.Interface())
			}
		}
	}

	// Subcase A: normal SL full close
	b1 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, AccountLeverage: 8.0, EntryOrderType: "LIMIT"})
	b1.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000.0), AvailableBalance: decimal.NewFromFloat(1000.0),
		EventTime: time.Now().UTC(), IsFullSnapshot: true,
	})
	slCh := make(chan string, 1)
	b1.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) { return 4001, nil }
	b1.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			slCh <- clientAlgoID
			return 5001, nil
		}
		return 5002, nil
	}
	b1.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error { return nil }
	b1.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error { return nil }

	_, err := b1.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "LEAK_A", Symbol: "SOLUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(95.0), TPPrice: decimal.NewFromFloat(115.0),
		ATRAtEntry: decimal.NewFromFloat(2.0), CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("SubmitLimitEntry A failed: %v", err)
	}
	b1.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "LEAK_A", ExchangeOrderID: 4001, Symbol: "SOLUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(1.0), LastFilledQty: decimal.NewFromFloat(1.0),
		AvgPrice: decimal.NewFromFloat(100.0), Commission: decimal.NewFromFloat(0.02), CommissionAsset: "USDT", IsMaker: true, HasFillMeta: true,
	})
	slID := <-slCh
	// Ensure TP algo order is also stored before closing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b1.Mu.Lock()
		p := b1.Protections["SOLUSDT"]
		ready := p != nil && p.TPOrderID == 5002
		b1.Mu.Unlock()
		if ready {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	b1.OnRealtimeTick("SOLUSDT", 94.9, 95.1, 95.0, 0, 0, false, time.Now().UTC())
	b1.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: slID, ExchangeOrderID: 5001, Symbol: "SOLUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(1.0), LastFilledQty: decimal.NewFromFloat(1.0),
		AvgPrice: decimal.NewFromFloat(95.0), RealizedProfit: decimal.NewFromFloat(-5.0), Commission: decimal.NewFromFloat(0.05), CommissionAsset: "USDT", IsMaker: false, HasFillMeta: true,
	})
	b1.Close()
	assertAllBrokerMapsEmpty(t, "normal SL close", b1)

	// Subcase B: offline close reconciled via full snapshot
	b2 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, AccountLeverage: 8.0, EntryOrderType: "LIMIT"})
	b2.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000.0), AvailableBalance: decimal.NewFromFloat(1000.0),
		EventTime: time.Now().UTC(), IsFullSnapshot: true,
	})
	b2.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) { return 6001, nil }
	b2.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			return 7001, nil
		}
		return 7002, nil
	}
	b2.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error { return nil }
	b2.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error { return nil }
	_, err = b2.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "LEAK_B", Symbol: "ADAUSDT", Side: model.SideShort,
		Qty: decimal.NewFromFloat(10.0), LimitPrice: decimal.NewFromFloat(1.0),
		SLPrice: decimal.NewFromFloat(1.1), TPPrice: decimal.NewFromFloat(0.8),
		ATRAtEntry: decimal.NewFromFloat(0.05), CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("SubmitLimitEntry B failed: %v", err)
	}
	b2.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "LEAK_B", ExchangeOrderID: 6001, Symbol: "ADAUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(10.0), LastFilledQty: decimal.NewFromFloat(10.0),
		AvgPrice: decimal.NewFromFloat(1.0), Commission: decimal.NewFromFloat(0.01), CommissionAsset: "USDT", IsMaker: true, HasFillMeta: true,
	})
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b2.Mu.Lock()
		p := b2.Protections["ADAUSDT"]
		ready := p != nil && p.SLOrderID == 7001 && p.TPOrderID == 7002
		b2.Mu.Unlock()
		if ready {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	b2.OnRealtimeTick("ADAUSDT", 0.99, 1.01, 1.0, 0, 0, false, time.Now().UTC())
	// Backdate openTime past offlineReconcileGrace (15s) and simulate REST full-snapshot reconciliation with 0 positions
	b2.Mu.Lock()
	b2.PositionOpenTimes["ADAUSDT"] = time.Now().UTC().Add(-1 * time.Minute)
	b2.Mu.Unlock()
	b2.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
		return decimal.NewFromFloat(1002.0), decimal.Zero, decimal.NewFromFloat(1002.0), map[string]model.LivePosition{}, nil
	}
	if err := b2.RefreshAccount(context.Background()); err != nil {
		t.Fatalf("RefreshAccount B failed: %v", err)
	}
	b2.Close()
	assertAllBrokerMapsEmpty(t, "offline snapshot close", b2)
}

func TestLiveBroker_GetActivePositionsIncludesMarkAndUPnL(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:        3,
		AccountLeverage: 8.0,
	})
	now := time.Now().UTC()
	b.Mu.Lock()
	b.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["BTCUSDT"] = model.SideLong
	b.PositionEntryPrices["BTCUSDT"] = decimal.NewFromFloat(100.0)
	b.PositionOpenTimes["BTCUSDT"] = now.Add(-10 * time.Minute)
	b.Protections["BTCUSDT"] = &model.LivePositionProtection{
		Symbol:  "BTCUSDT",
		SLPrice: decimal.NewFromFloat(95.0),
		TPPrice: decimal.NewFromFloat(120.0),
	}
	b.Mu.Unlock()

	// Without WS PositionUnrealizedPnLs, computes from LastMarkPrices updated by OnRealtimeTick
	b.OnRealtimeTick("BTCUSDT", 104.9, 105.1, 105.0, 0, 0, false, now)
	list := b.GetActivePositions()
	if len(list) != 1 {
		t.Fatalf("expected 1 active position, got %d", len(list))
	}
	if list[0].MarkPrice != 105.0 || list[0].UnrealizedPnL != 10.0 || list[0].UnrealizedROEPct != 40.0 {
		t.Fatalf("unexpected mark/uPnL/ROE from tick: %+v", list[0])
	}

	// When WS/REST PositionUnrealizedPnLs is populated, GetActivePositions prefers authoritative uPnL
	b.Mu.Lock()
	b.PositionUnrealizedPnLs["BTCUSDT"] = decimal.NewFromFloat(12.5)
	b.Mu.Unlock()
	list = b.GetActivePositions()
	if len(list) != 1 || list[0].MarkPrice != 105.0 || list[0].UnrealizedPnL != 12.5 {
		t.Fatalf("expected authoritative PositionUnrealizedPnL 12.5 with mark 105.0, got %+v", list[0])
	}
}

func TestDryRunBroker_FundingSettlement8hAndRestartPersistence(t *testing.T) {
	t0755 := time.Date(2026, 11, 20, 7, 55, 0, 0, time.UTC)
	d := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:         3,
		AccountLeverage:  8.0,
		MakerFeeRate:     0.0002,
		TakerFeeRate:     0.0005,
		SlippageRate:     0.0,
		PartialTPEnabled: false,
	})
	var savedMu sync.Mutex
	var lastSaved *VirtualOpenPosition
	d.OnPositionSavedFn = func(pos VirtualOpenPosition) {
		savedMu.Lock()
		cp := pos
		lastSaved = &cp
		savedMu.Unlock()
	}

	_, err := d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "FUND_DRY_1", Symbol: "BTCUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(1000.0),
		SLPrice: decimal.NewFromFloat(900.0), TPPrice: decimal.NewFromFloat(1200.0),
		CreatedAt: t0755,
	})
	if err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}
	// Passive Maker fill (LimitPrice 1000.0 < bestAsk 1000.1, tradePrice 999.9 <= LimitPrice)
	d.OnRealtimeTick("BTCUSDT", 999.9, 1000.1, 1000.0, 999.9, 2.0, true, t0755)
	// Seed funding epoch before boundary then cross 08:00 UTC boundary
	t0800 := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	d.ApplyFundingTick("BTCUSDT", 1000.0, 0.0001, t0800)
	// Repeat inside same 8h epoch -> idempotent
	d.ApplyFundingTick("BTCUSDT", 1000.0, 0.0001, t0800.Add(5*time.Minute))

	d.Mu.Lock()
	pos := d.OpenPositions["BTCUSDT"]
	if pos == nil {
		d.Mu.Unlock()
		t.Fatalf("expected open position")
	}
	gotFunding := pos.FundingPaid
	gotEpoch := pos.LastFundingEpoch
	gotRealEq := d.RealizedEquityUSDT
	d.Mu.Unlock()

	expectedEpoch := t0800.Unix() / 28800
	if !gotFunding.Equal(decimal.NewFromFloat(0.10)) || gotEpoch != expectedEpoch {
		t.Fatalf("expected FundingPaid=0.10 LastFundingEpoch=%d, got FundingPaid=%s epoch=%d", expectedEpoch, gotFunding, gotEpoch)
	}
	if !gotRealEq.Equal(decimal.NewFromFloat(999.70)) { // 1000 - 0.20 entryFee - 0.10 funding
		t.Fatalf("expected RealizedEquityUSDT=999.70, got %s", gotRealEq)
	}
	d.Close()

	savedMu.Lock()
	restoredInput := lastSaved
	savedMu.Unlock()
	if restoredInput == nil || !restoredInput.FundingPaid.Equal(decimal.NewFromFloat(0.10)) {
		t.Fatalf("expected OnPositionSavedFn to persist FundingPaid=0.10, got %+v", restoredInput)
	}

	// Simulate restart: restore pure closed-trade equity (1000.0) and open position
	d2 := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:         3,
		AccountLeverage:  8.0,
		MakerFeeRate:     0.0002,
		TakerFeeRate:     0.0005,
		SlippageRate:     0.0,
		PartialTPEnabled: false,
	})
	var closedRecs []TradeClosedRecord
	d2.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedRecs = append(closedRecs, rec)
	}
	restoredCopy := *restoredInput
	d2.RestoreOpenPosition(&restoredCopy)
	if pure := d2.GetPureRealizedEquityClosedTradesUSDT(); !pure.Equal(decimal.NewFromFloat(1000.0)) {
		t.Fatalf("expected pure realized equity 1000.0 after restore, got %s", pure)
	}

	// Trigger full TP close at 1200.0: exitFee = 1200 * 1 * 0.0005 = 0.60, entryFee = 0.20, funding = 0.10 -> NetPnL = 199.10
	d2.OnRealtimeTick("BTCUSDT", 1200.0, 1200.1, 1200.0, 1200.0, 1.0, false, t0800.Add(10*time.Minute))
	d2.Close()

	if len(closedRecs) != 1 || !closedRecs[0].NetPnL.Equal(decimal.NewFromFloat(199.10)) {
		t.Fatalf("expected closed NetPnL 199.10 after deducting FundingPaid once, got %+v", closedRecs)
	}
	if pure := d2.GetPureRealizedEquityClosedTradesUSDT(); !pure.Equal(decimal.NewFromFloat(1199.10)) {
		t.Fatalf("expected final pure realized equity 1199.10, got %s", pure)
	}
}

func TestDryRunBroker_SLGapFillAndTimeStopSlippageAndSLFirstOrder(t *testing.T) {
	// Case A & B: SLGapFillEnabled + SL-first precedence over TP/TP1
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	d := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:         3,
		AccountLeverage:  8.0,
		MakerFeeRate:     0.0,
		TakerFeeRate:     0.0,
		SlippageRate:     0.001,
		SLGapFillEnabled: true,
		PartialTPEnabled: true,
		PartialTP1Ratio:  0.5,
	})
	var recs []TradeClosedRecord
	var mu sync.Mutex
	d.OnTradeClosedFn = func(rec TradeClosedRecord) {
		mu.Lock()
		recs = append(recs, rec)
		mu.Unlock()
	}
	_, err := d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "GAP_SL_1", Symbol: "ETHUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(95.0), TPPrice: decimal.NewFromFloat(110.0), TP1Price: decimal.NewFromFloat(105.0),
		ATRAtEntry: decimal.NewFromFloat(5.0), CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}
	d.OnRealtimeTick("ETHUSDT", 99.9, 100.1, 100.0, 99.9, 5.0, true, now)
	// Deliver conflicting tick: mark gaps down to 90.0 (below SL 95.0) while tradePrice=115.0 (>= TP1/TP)
	d.OnRealtimeTick("ETHUSDT", 89.9, 90.1, 90.0, 115.0, 1.0, false, now.Add(5*time.Second))
	d.Close()

	mu.Lock()
	if len(recs) != 1 || recs[0].ExitReason != "STOP_LOSS" || !recs[0].ExitPrice.Equal(decimal.NewFromFloat(89.91)) {
		mu.Unlock()
		t.Fatalf("expected single STOP_LOSS at gap-filled+slippage 89.91, got %+v", recs)
	}
	mu.Unlock()

	// Case C: CheckTimeouts (TIME_STOP_MAX_HOLDING) applies adverse SlippageRate
	d2 := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:           3,
		AccountLeverage:    8.0,
		MakerFeeRate:       0.0,
		TakerFeeRate:       0.0,
		SlippageRate:       0.002,
		MaxHoldingDuration: 1 * time.Hour,
	})
	var tsRecs []TradeClosedRecord
	d2.OnTradeClosedFn = func(rec TradeClosedRecord) {
		tsRecs = append(tsRecs, rec)
	}
	_, _ = d2.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "TS_1", Symbol: "SOLUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(90.0), TPPrice: decimal.NewFromFloat(120.0),
		CreatedAt: now,
	})
	d2.OnRealtimeTick("SOLUSDT", 99.9, 100.1, 100.0, 99.9, 5.0, true, now)
	d2.OnRealtimeTick("SOLUSDT", 99.9, 100.1, 100.0, 0, 0, false, now.Add(61*time.Minute))
	d2.CheckTimeouts(now.Add(61 * time.Minute))
	d2.Close()

	if len(tsRecs) != 1 || tsRecs[0].ExitReason != "TIME_STOP_MAX_HOLDING" || !tsRecs[0].ExitPrice.Equal(decimal.NewFromFloat(99.8)) {
		t.Fatalf("expected TIME_STOP_MAX_HOLDING at 99.8 with 20bps slippage, got %+v", tsRecs)
	}
}

func TestBroker_GracefulCloseDrainsAsyncGoroutines(t *testing.T) {
	now := time.Now().UTC()

	// 1. DryRunBroker Close() drains async OnTradeClosedFn
	d := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots: 3, AccountLeverage: 8.0,
	})
	var dryDone atomic.Bool
	d.OnTradeClosedFn = func(rec TradeClosedRecord) {
		time.Sleep(40 * time.Millisecond)
		dryDone.Store(true)
	}
	_, _ = d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DRAIN_DRY", Symbol: "BTCUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(95.0), TPPrice: decimal.NewFromFloat(110.0),
		CreatedAt: now,
	})
	d.OnRealtimeTick("BTCUSDT", 99.9, 100.1, 100.0, 99.9, 5.0, true, now)
	d.OnRealtimeTick("BTCUSDT", 94.9, 95.1, 95.0, 0, 0, false, now.Add(time.Second))
	d.Close()
	if !dryDone.Load() {
		t.Fatalf("expected DryRunBroker.Close() to wait for async OnTradeClosedFn completion")
	}

	// 2. BinanceLiveBroker Close() drains async OnTradeClosedFn
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots: 3, AccountLeverage: 8.0, EntryOrderType: "LIMIT",
	})
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000.0), AvailableBalance: decimal.NewFromFloat(1000.0),
		EventTime: now, IsFullSnapshot: true,
	})
	slCh := make(chan string, 1)
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) { return 8001, nil }
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			slCh <- clientAlgoID
			return 9001, nil
		}
		return 9002, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error { return nil }
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error { return nil }
	var liveDone atomic.Bool
	b.OnTradeClosedFn = func(rec TradeClosedRecord) {
		time.Sleep(40 * time.Millisecond)
		liveDone.Store(true)
	}
	_, _ = b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DRAIN_LIVE", Symbol: "ETHUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(95.0), TPPrice: decimal.NewFromFloat(110.0),
		ATRAtEntry: decimal.NewFromFloat(2.0), CreatedAt: now,
	})
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "DRAIN_LIVE", ExchangeOrderID: 8001, Symbol: "ETHUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(1.0), LastFilledQty: decimal.NewFromFloat(1.0),
		AvgPrice: decimal.NewFromFloat(100.0), HasFillMeta: true, IsMaker: true,
	})
	slID := <-slCh
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: slID, ExchangeOrderID: 9001, Symbol: "ETHUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(1.0), LastFilledQty: decimal.NewFromFloat(1.0),
		AvgPrice: decimal.NewFromFloat(95.0), RealizedProfit: decimal.NewFromFloat(-5.0), HasFillMeta: true,
	})
	b.Close()
	if !liveDone.Load() {
		t.Fatalf("expected BinanceLiveBroker.Close() to wait for async OnTradeClosedFn completion")
	}
}

func TestLiveBroker_TP1AccountUpdateBeforeOrderUpdateDoesNotCancelSL(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		AccountLeverage:  8.0,
		EntryOrderType:   "LIMIT",
		PartialTPEnabled: true,
		PartialTP1Ratio:  0.50,
	})
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000.0), AvailableBalance: decimal.NewFromFloat(1000.0),
		EventTime: time.Now().UTC(), IsFullSnapshot: true,
	})

	var mu sync.Mutex
	var canceledAlgos []int64
	var placedSLQtys []decimal.Decimal
	tp1ClientCh := make(chan string, 1)
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) { return 1001, nil }
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			mu.Lock()
			placedSLQtys = append(placedSLQtys, qty)
			idx := len(placedSLQtys)
			mu.Unlock()
			return int64(2000 + idx), nil
		}
		return 2099, nil
	}
	b.PlaceLimitCloseFn = func(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (int64, string, error) {
		tp1ClientCh <- clientOrderID
		return 3001, clientOrderID, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		mu.Lock()
		canceledAlgos = append(canceledAlgos, algoID)
		mu.Unlock()
		return nil
	}
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error { return nil }

	_, err := b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "TP1_RACE_ENTRY", Symbol: "ETHUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(95.0), TPPrice: decimal.NewFromFloat(120.0),
		TP1Price: decimal.NewFromFloat(110.0), ATRAtEntry: decimal.NewFromFloat(5.0),
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "TP1_RACE_ENTRY", ExchangeOrderID: 1001, Symbol: "ETHUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(1.0), LastFilledQty: decimal.NewFromFloat(1.0),
		AvgPrice: decimal.NewFromFloat(100.0), HasFillMeta: true, IsMaker: true,
	})
	tp1ClientID := <-tp1ClientCh
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.Mu.Lock()
		p := b.Protections["ETHUSDT"]
		ready := p != nil && p.SLOrderID != 0 && p.TP1OrderID == 3001
		b.Mu.Unlock()
		if ready {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Binance delivers ACCOUNT_UPDATE (qty reduced to 0.50) BEFORE ORDER_TRADE_UPDATE (FILLED 0.50)
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1005.0), EventTime: time.Now().UTC(), IsFullSnapshot: false,
		Positions: map[string]model.LivePosition{
			"ETHUSDT": {Symbol: "ETHUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(0.5), EntryPrice: decimal.NewFromFloat(100.0), UnrealizedPnL: decimal.NewFromFloat(5.0)},
		},
	})
	// Deliver as 2-chunk taker TP1 fill (0.25 + 0.50) to verify both C3 & H6
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: tp1ClientID, ExchangeOrderID: 3001, Symbol: "ETHUSDT",
		Status: model.StatusPartiallyFilled, CumFilledQty: decimal.NewFromFloat(0.25), LastFilledQty: decimal.NewFromFloat(0.25),
		AvgPrice: decimal.NewFromFloat(110.0), HasFillMeta: true, IsMaker: false,
	})
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: tp1ClientID, ExchangeOrderID: 3001, Symbol: "ETHUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(0.50), LastFilledQty: decimal.NewFromFloat(0.25),
		AvgPrice: decimal.NewFromFloat(110.0), HasFillMeta: true, IsMaker: false,
	})
	b.Close()

	b.Mu.Lock()
	remQty := b.ActivePositions["ETHUSDT"]
	prot := b.Protections["ETHUSDT"]
	b.Mu.Unlock()

	if !remQty.Equal(decimal.NewFromFloat(0.50)) || prot == nil || !prot.ProtectedQty.Equal(decimal.NewFromFloat(0.50)) {
		t.Fatalf("expected runner qty 0.50 preserved with active SL protection, got remQty=%s prot=%+v", remQty, prot)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(placedSLQtys) < 2 || !placedSLQtys[len(placedSLQtys)-1].Equal(decimal.NewFromFloat(0.50)) {
		t.Fatalf("expected BE SL placed for remaining 0.50 qty, got placedSLQtys=%v", placedSLQtys)
	}
	for _, cid := range canceledAlgos {
		if cid == placedSLQtys[len(placedSLQtys)-1].IntPart() {
			t.Fatalf("unexpected cancellation of active runner SL")
		}
	}
}

func TestLiveBroker_TP1MarketFallbackAbortsWhenCancelSaysAlreadyFilled(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		AccountLeverage:  8.0,
		PartialTPEnabled: true,
		PartialTP1Ratio:  0.50,
	})
	now := time.Now().UTC()
	b.Mu.Lock()
	b.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(2.0)
	b.InitialQtys["SOLUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["SOLUSDT"] = model.SideLong
	b.PositionEntryPrices["SOLUSDT"] = decimal.NewFromFloat(100.0)
	b.PositionOpenTimes["SOLUSDT"] = now.Add(-5 * time.Minute)
	b.ATRAtEntry["SOLUSDT"] = decimal.NewFromFloat(2.0)
	b.TP1Prices["SOLUSDT"] = decimal.NewFromFloat(105.0)
	b.Protections["SOLUSDT"] = &model.LivePositionProtection{
		Symbol:           "SOLUSDT",
		Side:             model.SideLong,
		PlanGeneration:   1,
		ProtectedQty:     decimal.NewFromFloat(2.0),
		SLOrderID:        2001,
		SLStatus:         model.ProtectionAcked,
		SLPrice:          decimal.NewFromFloat(95.0),
		TP1OrderID:       3001,
		TP1ClientOrderID: "ALGO_TP1_SOL",
		TP1Price:         decimal.NewFromFloat(105.0),
	}
	b.Mu.Unlock()

	var marketCloseCalls atomic.Int32
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
		return fmt.Errorf("exchange: API returned HTTP 400 (code=-2011, msg=Unknown order sent.)")
	}
	b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
		return &model.LiveOrderRecord{
			ClientOrderID:       clientOrderID,
			ExchangeOrderID:     3001,
			Symbol:              symbol,
			Status:              model.StatusFilled,
			CumulativeFilledQty: decimal.NewFromFloat(1.0),
			AvgPrice:            decimal.NewFromFloat(105.0),
		}, nil
	}
	b.MarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, decimal.Decimal, decimal.Decimal, error) {
		marketCloseCalls.Add(1)
		return 4001, decimal.NewFromFloat(106.5), qty, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error { return nil }

	// Tick crosses tp1Price + 0.5*ATR (105 + 1.0 = 106.0)
	b.OnRealtimeTick("SOLUSDT", 106.4, 106.6, 106.5, 0, 0, false, now)
	b.Close()

	if marketCloseCalls.Load() != 0 {
		t.Fatalf("expected MarketCloseFn aborted when resting TP1 cancel failed as already filled, got calls=%d", marketCloseCalls.Load())
	}
	b.Mu.Lock()
	rem := b.ActivePositions["SOLUSDT"]
	hit := b.IsTP1Hits["SOLUSDT"]
	b.Mu.Unlock()
	if !rem.Equal(decimal.NewFromFloat(1.0)) || !hit {
		t.Fatalf("expected remaining qty 1.0 and IsTP1Hits=true reconciled from QueryOrder, got rem=%s hit=%v", rem, hit)
	}
}

func TestLiveBroker_AlgoUpdateCanceledMarksSLFailedForRepair(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, AccountLeverage: 8.0})
	now := time.Now().UTC()
	b.Mu.Lock()
	b.ActivePositions["ADAUSDT"] = decimal.NewFromFloat(10.0)
	b.PositionSides["ADAUSDT"] = model.SideLong
	b.PositionEntryPrices["ADAUSDT"] = decimal.NewFromFloat(1.0)
	b.PositionOpenTimes["ADAUSDT"] = now.Add(-1 * time.Minute)
	b.Protections["ADAUSDT"] = &model.LivePositionProtection{
		Symbol:         "ADAUSDT",
		Side:           model.SideLong,
		PlanGeneration: 1,
		ProtectedQty:   decimal.NewFromFloat(10.0),
		SLOrderID:      5001,
		SLClientAlgoID: "ALGO_SL_ADA",
		SLStatus:       model.ProtectionAcked,
		SLPrice:        decimal.NewFromFloat(0.95),
	}
	b.Mu.Unlock()

	var repaired atomic.Bool
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		repaired.Store(true)
		return 5002, nil
	}

	b.HandleAlgoOrderUpdate(model.AlgoOrderUpdateEvent{
		Symbol: "ADAUSDT", AlgoID: 5001, ClientAlgoID: "ALGO_SL_ADA",
		OrderType: "STOP_MARKET", AlgoStatus: "CANCELED", TriggerPrice: decimal.NewFromFloat(0.95),
	})

	b.Mu.Lock()
	prot := b.Protections["ADAUSDT"]
	statusAfterEvt := prot.SLStatus
	idAfterEvt := prot.SLOrderID
	b.Mu.Unlock()
	if statusAfterEvt != model.ProtectionFailed || idAfterEvt != 0 {
		t.Fatalf("expected ProtectionFailed and SLOrderID=0 after ALGO_UPDATE CANCELED, got status=%s id=%d", statusAfterEvt, idAfterEvt)
	}

	b.RepairFailedProtections(now)
	b.Mu.Lock()
	statusAfterRepair := b.Protections["ADAUSDT"].SLStatus
	idAfterRepair := b.Protections["ADAUSDT"].SLOrderID
	b.Mu.Unlock()
	if !repaired.Load() || statusAfterRepair != model.ProtectionAcked || idAfterRepair != 5002 {
		t.Fatalf("expected RepairFailedProtections to restore SL 5002, got repaired=%v status=%s id=%d", repaired.Load(), statusAfterRepair, idAfterRepair)
	}
}

func TestLiveBroker_ReserveEntryIntentAndSubmitReservedEntry(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:        3,
		AccountLeverage: 8.0,
		EntryOrderType:  "MARKET",
	})
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000.0), AvailableBalance: decimal.NewFromFloat(1000.0),
		EventTime: time.Now().UTC(), IsFullSnapshot: true,
	})
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		return 7777, nil
	}
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		// Simulate fast MARKET fill WS update arriving BEFORE PlaceEntryFn returns
		b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
			ClientOrderID: intent.ClientOrderID, ExchangeOrderID: 6001, Symbol: intent.Symbol,
			Status: model.StatusFilled, CumFilledQty: intent.Qty, LastFilledQty: intent.Qty,
			AvgPrice: intent.LimitPrice, HasFillMeta: true, IsMaker: false,
		})
		return 6001, nil
	}

	intent := model.OrderIntent{
		ClientOrderID: "RSV_MKT_1", Symbol: "BTCUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(1.0), LimitPrice: decimal.NewFromFloat(800.0),
		SLPrice: decimal.NewFromFloat(760.0), TPPrice: decimal.NewFromFloat(880.0),
		ATRAtEntry: decimal.NewFromFloat(10.0), CreatedAt: time.Now().UTC(),
	}
	prep, err := b.PreparePreSubmitIntent(intent)
	if err != nil {
		t.Fatalf("PreparePreSubmitIntent failed: %v", err)
	}
	ok, err := b.ReserveEntryIntent(prep)
	if !ok || err != nil {
		t.Fatalf("ReserveEntryIntent failed: ok=%v err=%v", ok, err)
	}

	b.Mu.Lock()
	rec := b.OrdersByClient["RSV_MKT_1"]
	rsvMargin := b.ReservedEntryMargins["RSV_MKT_1"]
	b.Mu.Unlock()
	if rec == nil || !rec.LimitPrice.Equal(decimal.NewFromFloat(800.0)) || !rsvMargin.Equal(decimal.NewFromFloat(100.0)) {
		t.Fatalf("expected LimitPrice=800 and reserved margin=100 immediately on reserve, got rec=%+v rsv=%s", rec, rsvMargin)
	}

	okSub, errSub := b.SubmitReservedEntry(prep)
	if !okSub || errSub != nil {
		t.Fatalf("SubmitReservedEntry failed: ok=%v err=%v", okSub, errSub)
	}
	b.Close()

	b.Mu.Lock()
	leakedMargin := len(b.ReservedEntryMargins)
	b.Mu.Unlock()
	if leakedMargin != 0 {
		t.Fatalf("expected ReservedEntryMargins empty when WS FILLED raced PlaceEntryFn, got %d entries", leakedMargin)
	}
}

func TestLiveBroker_DynamicSLFailThrottleAndImmediateTrigger2021(t *testing.T) {
	// Subcase A: H5 throttle after dynamic SL failure prevents @bookTicker API storm
	b1 := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:              3,
		AccountLeverage:       8.0,
		Tier1BEEnabled:        true,
		Tier1BETriggerLongATR: 0.70,
		MinBreakEvenOffsetPct: 0.0010,
	})
	now := time.Now().UTC()
	b1.Mu.Lock()
	b1.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(1.0)
	b1.PositionSides["BTCUSDT"] = model.SideLong
	b1.PositionEntryPrices["BTCUSDT"] = decimal.NewFromFloat(100.0)
	b1.PositionOpenTimes["BTCUSDT"] = now.Add(-1 * time.Minute)
	b1.ATRAtEntry["BTCUSDT"] = decimal.NewFromFloat(2.0)
	b1.Protections["BTCUSDT"] = &model.LivePositionProtection{
		Symbol: "BTCUSDT", Side: model.SideLong, PlanGeneration: 1,
		ProtectedQty: decimal.NewFromFloat(1.0), SLOrderID: 8001, SLClientAlgoID: "SL_INIT",
		SLStatus: model.ProtectionAcked, SLPrice: decimal.NewFromFloat(95.0),
	}
	b1.Mu.Unlock()

	var slCalls1 atomic.Int32
	b1.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		slCalls1.Add(1)
		return 0, fmt.Errorf("exchange: API returned HTTP 400 (code=-2021, msg=Order would immediately trigger.)")
	}
	for i := 0; i < 5; i++ {
		b1.OnRealtimeTick("BTCUSDT", 101.9, 102.1, 102.0, 0, 0, false, now.Add(time.Duration(i)*time.Millisecond))
		time.Sleep(10 * time.Millisecond)
	}
	b1.Close()
	if slCalls1.Load() != 1 {
		t.Fatalf("expected 5s cooldown after failed dynamic SL upgrade to limit calls to 1, got %d", slCalls1.Load())
	}

	// Subcase B: H13 -2021 on BE SL breaks immediately and preserves old SLPrice
	b2 := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:              3,
		AccountLeverage:       8.0,
		PartialTPEnabled:      true,
		PartialTP1Ratio:       0.50,
		MinBreakEvenOffsetPct: 0.0010,
	})
	b2.Mu.Lock()
	b2.ActivePositions["ETHUSDT"] = decimal.NewFromFloat(2.0)
	b2.InitialQtys["ETHUSDT"] = decimal.NewFromFloat(2.0)
	b2.PositionSides["ETHUSDT"] = model.SideLong
	b2.PositionEntryPrices["ETHUSDT"] = decimal.NewFromFloat(100.0)
	b2.PositionOpenTimes["ETHUSDT"] = now.Add(-1 * time.Minute)
	b2.ATRAtEntry["ETHUSDT"] = decimal.NewFromFloat(2.0)
	b2.TP1Prices["ETHUSDT"] = decimal.NewFromFloat(105.0)
	b2.Protections["ETHUSDT"] = &model.LivePositionProtection{
		Symbol: "ETHUSDT", Side: model.SideLong, PlanGeneration: 1,
		ProtectedQty: decimal.NewFromFloat(2.0), SLOrderID: 9001, SLClientAlgoID: "SL_INIT_ETH",
		SLStatus: model.ProtectionAcked, SLPrice: decimal.NewFromFloat(95.0),
	}
	b2.Mu.Unlock()

	var slCalls2 atomic.Int32
	b2.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		slCalls2.Add(1)
		return 0, fmt.Errorf("exchange: API returned HTTP 400 (code=-2021, msg=Order would immediately trigger.)")
	}
	b2.processTP1Executed("ETHUSDT", decimal.NewFromFloat(1.0), decimal.NewFromFloat(105.0), "ALGO_TP1_ETH", true)
	b2.Close()

	if slCalls2.Load() != 1 {
		t.Fatalf("expected -2021 immediate trigger to break out of retries after 1 attempt, got %d", slCalls2.Load())
	}
	b2.Mu.Lock()
	retainedProt := b2.Protections["ETHUSDT"]
	b2.Mu.Unlock()
	if retainedProt == nil || retainedProt.SLOrderID != 9001 || !retainedProt.SLPrice.Equal(decimal.NewFromFloat(95.0)) || retainedProt.SLStatus != model.ProtectionAcked {
		t.Fatalf("expected old SL 9001 @ 95.0 retained on -2021 BE SL failure, got %+v", retainedProt)
	}
}

func TestDryRunBroker_PartialFillMargin_PureEquityAfterTP1_And_LiveDashboardDTO(t *testing.T) {
	// 1. DryRun partial fill margin & pure realized equity after TP1
	d := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:          3,
		AccountLeverage:   10.0,
		MakerFeeRate:      0.0002,
		TakerFeeRate:      0.0005,
		PartialTPEnabled:  true,
		PartialTP1Ratio:   0.50,
		MaxDailyDrawdownPct: -0.05,
	})
	defer d.Close()

	now := time.Now().UTC()
	d.Mu.Lock()
	// Simulate partially filled pending order: Qty=2.0, FilledQty=1.0 @ 100.0 (notional=100, margin=10)
	// OpenPosition holds the filled 1.0 @ 100.0 (margin=10); PendingOrder holds remaining 1.0 @ 100.0 (margin=10)
	d.OpenPositions["BTCUSDT"] = &VirtualOpenPosition{
		ClientOrderID:  "DR_PART_1",
		Symbol:         "BTCUSDT",
		Side:           model.SideLong,
		Qty:            decimal.NewFromFloat(1.0),
		InitialQty:     decimal.NewFromFloat(1.0),
		EntryPrice:     decimal.NewFromFloat(100.0),
		EntryFee:       decimal.NewFromFloat(0.02),
		CurrentSLPrice: decimal.NewFromFloat(95.0),
		InitialSLPrice: decimal.NewFromFloat(95.0),
		TPPrice:        decimal.NewFromFloat(110.0),
		TP1Price:       decimal.NewFromFloat(105.0),
		LastMarkPrice:  decimal.NewFromFloat(100.0),
		OpenTime:       now,
	}
	d.PendingOrders["BTCUSDT"] = &VirtualPendingOrder{
		Intent: model.OrderIntent{
			ClientOrderID: "DR_PART_1",
			Symbol:        "BTCUSDT",
			Side:          model.SideLong,
			Qty:           decimal.NewFromFloat(2.0),
			LimitPrice:    decimal.NewFromFloat(100.0),
		},
		FilledQty: decimal.NewFromFloat(1.0),
	}
	d.Mu.Unlock()

	// Total used margin should be 10 (open 1.0) + 10 (remaining pending 1.0) = 20, NOT 10 + 20 = 30
	avail := d.GetAvailableBalanceUSDT()
	if !avail.Equal(decimal.NewFromFloat(980.0)) {
		t.Fatalf("expected available balance 980.0 (no double-counted partial fill margin), got %s", avail)
	}

	// Simulate position of Qty=2.0 where TP1 closed 1.0 (half), leaving Qty=1.0, InitialQty=2.0, EntryFee=0.04
	d.Mu.Lock()
	delete(d.PendingOrders, "BTCUSDT")
	// Suppose initial equity was 1000.0, EntryFee=0.04 was deducted -> RealizedEquity=999.96.
	// Then TP1 closed half for +5.00 gross - 0.01 exit fee = +4.99 -> RealizedEquity=1004.95.
	// Closed trade net PnL for TP1 = 5.00 - 0.02 (half entry fee) - 0.01 = +4.97 -> pure closed equity = 1004.97.
	d.RealizedEquityUSDT = decimal.NewFromFloat(1004.95)
	d.OpenPositions["BTCUSDT"].InitialQty = decimal.NewFromFloat(2.0)
	d.OpenPositions["BTCUSDT"].Qty = decimal.NewFromFloat(1.0)
	d.OpenPositions["BTCUSDT"].EntryFee = decimal.NewFromFloat(0.04)
	d.Mu.Unlock()

	pureEq := d.GetPureRealizedEquityClosedTradesUSDT()
	if !pureEq.Equal(decimal.NewFromFloat(1004.97)) {
		t.Fatalf("expected pure realized equity 1004.97 (adding back only remaining runner half of entry fee 0.02), got %s", pureEq)
	}

	// 2. Live GetActivePositions mark-based uPnL/ROE consistency and ProtectionTier>=1 IsBreakEven badge
	lb := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:        3,
		AccountLeverage: 8.0,
	})
	defer lb.Close()
	lb.Mu.Lock()
	lb.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(10.0)
	lb.PositionSides["SOLUSDT"] = model.SideLong
	lb.PositionEntryPrices["SOLUSDT"] = decimal.NewFromFloat(100.0)
	lb.PositionOpenTimes["SOLUSDT"] = now.Add(-2 * time.Minute)
	// Stale WS uPnL says -5.0, while live mark price is 101.0 (+10.0 uPnL, +8.0% ROE)
	lb.PositionUnrealizedPnLs["SOLUSDT"] = decimal.NewFromFloat(-5.0)
	lb.LastMarkPrices["SOLUSDT"] = decimal.NewFromFloat(101.0)
	// Post-TP1 cushion BE: ProtectionTier=1, SL slightly below entry (99.80) due to RUNNER_BE_CUSHION_ATR
	lb.ProtectionTiers["SOLUSDT"] = 1
	lb.Protections["SOLUSDT"] = &model.LivePositionProtection{
		Symbol:  "SOLUSDT",
		Side:    model.SideLong,
		SLPrice: decimal.NewFromFloat(99.80),
		TPPrice: decimal.NewFromFloat(108.00),
	}
	lb.Mu.Unlock()

	dtos := lb.GetActivePositions()
	if len(dtos) != 1 {
		t.Fatalf("expected 1 active position DTO, got %d", len(dtos))
	}
	if dtos[0].UnrealizedPnL != 10.0 || dtos[0].UnrealizedROEPct <= 0 {
		t.Fatalf("expected consistent positive UnrealizedPnL=10.0 and ROE>0 from live MarkPrice=101, got pnl=%f roe=%f", dtos[0].UnrealizedPnL, dtos[0].UnrealizedROEPct)
	}
	if !dtos[0].IsBreakEven {
		t.Fatalf("expected IsBreakEven=true when ProtectionTier >= 1 (even with cushion BE SL < Entry)")
	}
}

func TestDryRunBroker_StaleLossExit_And_PeakDrawdown(t *testing.T) {
	d := NewDryRunBroker(decimal.NewFromFloat(100.0), DryRunConfig{
		MaxSlots:              5,
		MaxDailyDrawdownPct:   -0.50,
		AccountLeverage:       10.0,
		EntryOrderType:        "LIMIT",
		StaleLossExitDuration: 6 * time.Hour,
		MaxHoldingDuration:    18 * time.Hour,
		MakerFeeRate:          0.0002,
		TakerFeeRate:          0.0005,
		SlippageRate:          0.0002,
	})

	var closedMu sync.Mutex
	closedReasons := make(map[string]string)
	d.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedMu.Lock()
		closedReasons[rec.Symbol] = rec.ExitReason
		closedMu.Unlock()
	}

	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, sym := range []string{"LOSSUSDT", "WINUSDT"} {
		ok, err := d.SubmitLimitEntry(model.OrderIntent{
			ClientOrderID:  "MKT_" + sym,
			Symbol:         sym,
			Side:           model.SideShort,
			StrategyID:     "VOL_BREAKOUT",
			EntryOrderType: "MARKET", // overrides default LIMIT
			Qty:            decimal.NewFromFloat(1.0),
			LimitPrice:     decimal.NewFromFloat(100.0),
			SLPrice:        decimal.NewFromFloat(105.0),
			TPPrice:        decimal.NewFromFloat(90.0),
			CreatedAt:      t0,
		})
		if !ok || err != nil {
			t.Fatalf("SubmitLimitEntry MARKET override failed for %s: ok=%v err=%v", sym, ok, err)
		}
	}

	// Mark-to-market at t0 + 6h: LOSSUSDT underwater (102 > 100), WINUSDT profitable (98 < 100)
	d.Mu.Lock()
	d.OpenPositions["LOSSUSDT"].LastMarkPrice = decimal.NewFromFloat(102.0)
	d.OpenPositions["LOSSUSDT"].MarkUpdatedAt = t0.Add(6 * time.Hour)
	d.OpenPositions["WINUSDT"].LastMarkPrice = decimal.NewFromFloat(98.0)
	d.OpenPositions["WINUSDT"].MarkUpdatedAt = t0.Add(6 * time.Hour)
	d.Mu.Unlock()

	d.CheckTimeouts(t0.Add(6 * time.Hour))

	d.Mu.Lock()
	_, lossStillOpen := d.OpenPositions["LOSSUSDT"]
	_, winStillOpen := d.OpenPositions["WINUSDT"]
	d.Mu.Unlock()
	if lossStillOpen || !winStillOpen {
		t.Fatalf("expected LOSSUSDT closed by stale-loss and WINUSDT open, got lossOpen=%v winOpen=%v", lossStillOpen, winStillOpen)
	}

	d.SetPeakEffectiveEquityUSDT(decimal.NewFromFloat(200.0))
	dd := d.GetDrawdownFromPeakPct()
	if dd < 0.45 || dd > 0.55 {
		t.Fatalf("expected drawdown near 0.50 from hydrated peak 200.0, got %f", dd)
	}
	d.Close()

	closedMu.Lock()
	reason := closedReasons["LOSSUSDT"]
	closedMu.Unlock()
	if reason != "TIME_STOP_STALE_LOSS" {
		t.Fatalf("expected LOSSUSDT closed reason TIME_STOP_STALE_LOSS, got %q", reason)
	}
}

func TestBinanceLiveBroker_StaleLossExit_And_PeakDrawdown(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:              5,
		AccountLeverage:       10.0,
		EntryOrderType:        "LIMIT",
		StaleLossExitDuration: 6 * time.Hour,
		MaxHoldingDuration:    18 * time.Hour,
	})
	defer b.Close()

	var marketCalls, limitCalls atomic.Int32
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		limitCalls.Add(1)
		return 100, nil
	}
	b.PlaceMarketEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		marketCalls.Add(1)
		return 101, nil
	}

	t0 := time.Now().UTC().Add(-8 * time.Hour)
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance:    decimal.NewFromFloat(160.0),
		AvailableBalance: decimal.NewFromFloat(160.0),
		EventTime:        time.Now().UTC(),
		IsFullSnapshot:   true,
	})
	if ok, err := b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID:  "BO_MKT_1",
		Symbol:         "MKTUSDT",
		Side:           model.SideShort,
		EntryOrderType: "MARKET",
		Qty:            decimal.NewFromFloat(1.0),
		LimitPrice:     decimal.NewFromFloat(10.0),
		SLPrice:        decimal.NewFromFloat(11.0),
		TPPrice:        decimal.NewFromFloat(8.0),
	}); !ok || err != nil {
		t.Fatalf("SubmitLimitEntry MARKET override failed: ok=%v err=%v", ok, err)
	}
	if marketCalls.Load() != 1 || limitCalls.Load() != 0 {
		t.Fatalf("expected PlaceMarketEntryFn called once for per-intent MARKET override, got market=%d limit=%d", marketCalls.Load(), limitCalls.Load())
	}

	b.SetPeakEffectiveEquityUSDT(decimal.NewFromFloat(200.0))
	if dd := b.GetDrawdownFromPeakPct(); dd < 0.1999 || dd > 0.2001 {
		t.Fatalf("expected drawdown 0.20 (160 from peak 200), got %f", dd)
	}

	b.Mu.Lock()
	b.ActivePositions["LOSSUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["LOSSUSDT"] = model.SideShort
	b.PositionEntryPrices["LOSSUSDT"] = decimal.NewFromFloat(100.0)
	b.LastMarkPrices["LOSSUSDT"] = decimal.NewFromFloat(103.0)
	b.LastMarkUpdatedAt["LOSSUSDT"] = t0.Add(6 * time.Hour)
	b.PositionOpenTimes["LOSSUSDT"] = t0
	b.ActivePositions["STALEUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["STALEUSDT"] = model.SideShort
	b.PositionEntryPrices["STALEUSDT"] = decimal.NewFromFloat(100.0)
	b.LastMarkPrices["STALEUSDT"] = decimal.NewFromFloat(103.0)
	b.LastMarkUpdatedAt["STALEUSDT"] = t0.Add(5 * time.Hour) // > 15s stale at t0+6h!
	b.PositionOpenTimes["STALEUSDT"] = t0
	b.ActivePositions["WINUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["WINUSDT"] = model.SideShort
	b.PositionEntryPrices["WINUSDT"] = decimal.NewFromFloat(100.0)
	b.LastMarkPrices["WINUSDT"] = decimal.NewFromFloat(97.0)
	b.LastMarkUpdatedAt["WINUSDT"] = t0.Add(6 * time.Hour)
	b.PositionOpenTimes["WINUSDT"] = t0
	b.Mu.Unlock()

	// estimateAvailableBalanceLocked must stay side-effect-free on PeakEffectiveEquityUSDT
	b.Mu.Lock()
	b.WalletBalanceUSDT = decimal.NewFromFloat(250.0)
	_ = b.estimateAvailableBalanceLocked()
	peakAfterEst := b.PeakEffectiveEquityUSDT
	b.Mu.Unlock()
	if !peakAfterEst.Equal(decimal.NewFromFloat(200.0)) {
		t.Fatalf("expected estimateAvailableBalanceLocked not to mutate PeakEffectiveEquityUSDT=200.0, got %s", peakAfterEst)
	}

	// Per-intent MARKET override without HasFillMeta must record EntryWasMaker=false (taker)
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID:   "BO_MKT_1",
		ExchangeOrderID: 101,
		Symbol:          "MKTUSDT",
		Status:          model.StatusFilled,
		CumFilledQty:    decimal.NewFromFloat(1.0),
		AvgPrice:        decimal.NewFromFloat(10.0),
		HasFillMeta:     false,
	})
	b.Mu.Lock()
	mktWasMaker, mktTracked := b.EntryWasMaker["MKTUSDT"]
	b.Mu.Unlock()
	if mktTracked && mktWasMaker {
		t.Fatalf("expected per-intent MARKET EntryWasMaker=false when HasFillMeta=false")
	}

	var failOnce atomic.Bool
	failOnce.Store(true)
	var closedMu sync.Mutex
	closedMap := make(map[string]string)
	b.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
		if failOnce.Load() {
			return fmt.Errorf("transient error")
		}
		closedMu.Lock()
		closedMap[symbol] = reason
		closedMu.Unlock()
		return nil
	}

	// Failure must clear ClosingReason
	b.CheckMaxHoldingTimeouts(t0.Add(6 * time.Hour))
	b.Mu.Lock()
	staleReason := b.ClosingReason["LOSSUSDT"]
	b.Mu.Unlock()
	if staleReason != "" {
		t.Fatalf("expected ClosingReason cleared after EmergencyMarketClose failure, got %q", staleReason)
	}

	// Success triggers TIME_STOP_STALE_LOSS ONLY for fresh underwater LOSSUSDT (ignores stale STALEUSDT and profitable WINUSDT)
	failOnce.Store(false)
	b.CheckMaxHoldingTimeouts(t0.Add(6 * time.Hour))
	closedMu.Lock()
	defer closedMu.Unlock()
	if closedMap["LOSSUSDT"] != "TIME_STOP_STALE_LOSS" || closedMap["STALEUSDT"] != "" || closedMap["WINUSDT"] != "" {
		t.Fatalf("expected only fresh LOSSUSDT closed with TIME_STOP_STALE_LOSS, got %+v", closedMap)
	}
}

func TestLiveBroker_RestingTP1AndPartialEntrySafety(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:          3,
		AccountLeverage:   8.0,
		EntryOrderType:    "LIMIT",
		PartialTPEnabled:  true,
		PartialTP1Ratio:   0.50,
		TrailingEnabled:   true,
	})
	defer b.Close()

	var canceledEntries []string
	var mu sync.Mutex
	tp1PlacedCh := make(chan struct{}, 2)
	b.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) { return 1001, nil }
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		return time.Now().UnixNano(), nil
	}
	b.PlaceLimitCloseFn = func(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (int64, string, error) {
		select {
		case tp1PlacedCh <- struct{}{}:
		default:
		}
		return 3001, clientOrderID, nil
	}
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
		mu.Lock()
		canceledEntries = append(canceledEntries, clientOrderID)
		mu.Unlock()
		return nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error { return nil }
	b.HandleAccountUpdate(decimal.NewFromInt(1000), decimal.Zero, decimal.NewFromInt(1000), nil, time.Now().UTC())

	ok, err := b.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "ENTRY_PART_1", Symbol: "SOLUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(10.0), LimitPrice: decimal.NewFromFloat(100.0),
		SLPrice: decimal.NewFromFloat(98.0), TPPrice: decimal.NewFromFloat(106.0), TP1Price: decimal.NewFromFloat(102.0),
		ATRAtEntry: decimal.NewFromFloat(1.0),
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: ok=%v err=%v", ok, err)
	}

	// Verify ReserveEntryIntent did NOT pre-populate InitialQtys with 10.0
	b.Mu.Lock()
	initPreFill := b.InitialQtys["SOLUSDT"]
	b.Mu.Unlock()
	if !initPreFill.IsZero() {
		t.Fatalf("expected InitialQtys zero before first fill, got %s", initPreFill)
	}

	// Partial entry fill: 6 of 10 filled
	b.HandleUserDataOrderUpdate("ENTRY_PART_1", 1001, "SOLUSDT", model.StatusPartiallyFilled, decimal.NewFromFloat(6.0), decimal.NewFromFloat(100.0), decimal.Zero)
	<-tp1PlacedCh // Wait for gen=1 protection & resting TP1 placement to finish cleanly
	b.Mu.Lock()
	initAfterPart := b.InitialQtys["SOLUSDT"]
	b.Mu.Unlock()
	if !initAfterPart.Equal(decimal.NewFromFloat(6.0)) {
		t.Fatalf("expected InitialQtys=6.0 after partial entry fill, got %s", initAfterPart)
	}

	// Wait briefly for replaceStopLossAsync inside processTP1ExecutedWithFee too if needed, and verify ENTRY_PART_1 canceled
	b.processTP1ExecutedWithFee("SOLUSDT", decimal.NewFromFloat(3.0), decimal.NewFromFloat(102.0), "ALGO_TP1_SOL", true, decimal.Zero, "USDT")

	b.Mu.Lock()
	remQty := b.ActivePositions["SOLUSDT"]
	_, stillPending := b.OrdersByClient["ENTRY_PART_1"]
	b.Mu.Unlock()
	if !remQty.Equal(decimal.NewFromFloat(3.0)) {
		t.Fatalf("expected ActivePositions=3.0 after TP1 on partial fill, got %s", remQty)
	}
	if stillPending {
		t.Fatalf("expected partially filled entry removed on resting TP1 fill")
	}

	// Wait briefly for async cancel call
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(canceledEntries)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	if len(canceledEntries) != 1 || canceledEntries[0] != "ENTRY_PART_1" {
		t.Fatalf("expected ENTRY_PART_1 canceled upon resting TP1 fill, got %v", canceledEntries)
	}
	mu.Unlock()
}

func TestLiveBroker_TP1CanceledAfterAccountUpdate_NoDoubleDeductionOrFalseClose(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, PartialTPEnabled: true, PartialTP1Ratio: 0.50})
	defer b.Close()

	var slCanceled atomic.Int32
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		slCanceled.Add(1)
		return nil
	}

	t0 := time.Now().UTC()
	b.Mu.Lock()
	b.ActivePositions["ETHUSDT"] = decimal.NewFromFloat(5.0) // Already reduced from 10->5 by ACCOUNT_UPDATE!
	b.InitialQtys["ETHUSDT"] = decimal.NewFromFloat(10.0)
	b.PositionSides["ETHUSDT"] = model.SideLong
	b.PositionEntryPrices["ETHUSDT"] = decimal.NewFromFloat(2000.0)
	b.PositionOpenTimes["ETHUSDT"] = t0
	b.Protections["ETHUSDT"] = &model.LivePositionProtection{
		Symbol: "ETHUSDT", Side: model.SideLong, SLOrderID: 9991, SLClientAlgoID: "ALGO_SL_ETH",
		SLPrice: decimal.NewFromFloat(1950), SLStatus: model.ProtectionAcked, ProtectedQty: decimal.NewFromFloat(10.0),
		TP1OrderID: 3331, TP1ClientOrderID: "ALGO_TP1_ETH",
	}
	b.Mu.Unlock()

	// Resting TP1 CANCELED with CumFilledQty=5.0 after ACCOUNT_UPDATE already updated ActivePositions=5.0
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "ALGO_TP1_ETH", ExchangeOrderID: 3331, Symbol: "ETHUSDT",
		Status: model.StatusCanceled, CumFilledQty: decimal.NewFromFloat(5.0), AvgPrice: decimal.NewFromFloat(2050.0),
	})

	b.Mu.Lock()
	rem := b.ActivePositions["ETHUSDT"]
	_, hasProt := b.Protections["ETHUSDT"]
	committed := b.TP1CommittedQtys["ETHUSDT"]
	b.Mu.Unlock()
	if !rem.Equal(decimal.NewFromFloat(5.0)) || !hasProt || !committed.Equal(decimal.NewFromFloat(5.0)) {
		t.Fatalf("expected 5.0 runner and SL intact without double deduction, rem=%s hasProt=%v committed=%s", rem, hasProt, committed)
	}
	if slCanceled.Load() != 0 {
		t.Fatalf("expected live SL NOT canceled when runner remains open")
	}
}

func TestLiveBroker_ExecuteProtectionPlan_CommitsNewIDsBeforeCancel_AndRepairFixes(t *testing.T) {
	b := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, PartialTPEnabled: false, TrailingEnabled: false})
	defer b.Close()

	var slCancelCh = make(chan struct{})
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			return 20002, nil
		}
		return 30002, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		if algoID == 10001 {
			// Simulate Binance WS ALGO_UPDATE(CANCELED) for old SL arriving during cancel!
			b.HandleAlgoOrderUpdate(model.AlgoOrderUpdateEvent{
				Symbol: "BTCUSDT", AlgoID: 10001, ClientAlgoID: "OLD_SL_CID", OrderType: "STOP_MARKET", AlgoStatus: "CANCELED",
			})
			close(slCancelCh)
		}
		return nil
	}

	b.Mu.Lock()
	b.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(1.0)
	b.PositionSides["BTCUSDT"] = model.SideLong
	b.PositionEntryPrices["BTCUSDT"] = decimal.NewFromFloat(60000.0)
	b.Protections["BTCUSDT"] = &model.LivePositionProtection{
		Symbol: "BTCUSDT", Side: model.SideLong, PlanGeneration: 1, SLOrderID: 10001, SLClientAlgoID: "OLD_SL_CID", SLStatus: model.ProtectionAcked,
	}
	b.Mu.Unlock()

	rec := &model.LiveOrderRecord{
		ClientOrderID: "ENT_BTC", Symbol: "BTCUSDT", Side: model.SideLong, IsEntry: true, Status: model.StatusFilled,
		CumulativeFilledQty: decimal.NewFromFloat(1.0), SLPrice: decimal.NewFromFloat(59000), TPPrice: decimal.NewFromFloat(62000),
	}
	b.Mu.Lock()
	plan, need := b.planProtectionSyncLocked(rec)
	b.Mu.Unlock()
	if !need {
		t.Fatalf("expected protection plan needed")
	}
	b.executeProtectionPlan(plan)
	<-slCancelCh

	b.Mu.Lock()
	prot := b.Protections["BTCUSDT"]
	slCID := prot.SLClientAlgoID
	slStatus := prot.SLStatus
	protQty := prot.ProtectedQty
	b.Mu.Unlock()
	if slCID != plan.ClientSLAlgoID || slStatus != model.ProtectionAcked || !protQty.Equal(decimal.NewFromFloat(1.0)) {
		t.Fatalf("expected new SLClientAlgoID=%q & ACKED preserved across old ALGO_UPDATE race, got cid=%q status=%v qty=%s",
			plan.ClientSLAlgoID, slCID, slStatus, protQty)
	}

	// Verify RepairFailedProtections cancels old SL, sets ProtectedQty, resets retries, and updates TPClientAlgoID
	var canceledOlds []int64
	var mu sync.Mutex
	b2 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, PartialTPEnabled: false, TrailingEnabled: false})
	defer b2.Close()
	b2.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			return 55501, nil
		}
		return 55502, nil
	}
	b2.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		mu.Lock()
		canceledOlds = append(canceledOlds, algoID)
		mu.Unlock()
		return nil
	}
	b2.Mu.Lock()
	b2.ActivePositions["ETHUSDT"] = decimal.NewFromFloat(2.0)
	b2.PositionSides["ETHUSDT"] = model.SideLong
	b2.PositionEntryPrices["ETHUSDT"] = decimal.NewFromFloat(2000)
	b2.Protections["ETHUSDT"] = &model.LivePositionProtection{
		Symbol: "ETHUSDT", Side: model.SideLong, PlanGeneration: 2,
		SLOrderID: 44401, SLClientAlgoID: "OLD_R_SL", SLStatus: model.ProtectionFailed, SLPrice: decimal.NewFromFloat(1950), SLRetryCount: 2,
		TPOrderID: 44402, TPClientAlgoID: "OLD_R_TP", TPStatus: model.ProtectionFailed, TPPrice: decimal.NewFromFloat(2100), TPRetryCount: 3,
		ProtectedQty: decimal.Zero,
	}
	b2.Mu.Unlock()

	b2.RepairFailedProtections(time.Now().UTC().Add(time.Minute))
	b2.Mu.Lock()
	p2 := b2.Protections["ETHUSDT"]
	b2.Mu.Unlock()
	if !p2.ProtectedQty.Equal(decimal.NewFromFloat(2.0)) || p2.SLRetryCount != 0 || p2.TPRetryCount != 0 || p2.TPClientAlgoID == "" || p2.TPClientAlgoID == "OLD_R_TP" {
		t.Fatalf("unexpected repair state: %+v", p2)
	}
	mu.Lock()
	if len(canceledOlds) != 2 {
		t.Fatalf("expected old SL & old TP canceled on repair, got %v", canceledOlds)
	}
	mu.Unlock()
}

func TestLiveAndDryRunBroker_CloseDrainsAsyncAndFIFOWithoutResurrection(t *testing.T) {
	// DryRunBroker FIFO Close order + RestoreOpenPosition pro-rated fee
	d := NewDryRunBroker(decimal.NewFromInt(1000), DryRunConfig{MaxSlots: 3, PartialTPEnabled: true, PartialTP1Ratio: 0.50})
	var events []string
	var evMu sync.Mutex
	d.OnPositionSavedFn = func(pos VirtualOpenPosition) {
		evMu.Lock()
		events = append(events, "save:"+pos.Symbol)
		evMu.Unlock()
	}
	d.OnPositionDeletedFn = func(id string) {
		evMu.Lock()
		events = append(events, "delete:"+id)
		evMu.Unlock()
	}

	d.Mu.Lock()
	pos := &VirtualOpenPosition{
		ClientOrderID: "DRY_FIFO", Symbol: "SOLUSDT", Side: model.SideLong,
		Qty: decimal.NewFromInt(5), InitialQty: decimal.NewFromInt(10),
		EntryPrice: decimal.NewFromInt(100), EntryFee: decimal.NewFromInt(10),
		CurrentSLPrice: decimal.NewFromInt(95), TPPrice: decimal.NewFromInt(110),
	}
	d.enqueuePositionEvent(PositionEvent{Type: PositionEventSave, Pos: *pos})
	d.enqueuePositionEvent(PositionEvent{Type: PositionEventDelete, ClientOrderIDOrSymbol: "DRY_FIFO"})
	d.Mu.Unlock()
	d.Close()

	evMu.Lock()
	if len(events) != 2 || events[0] != "save:SOLUSDT" || events[1] != "delete:DRY_FIFO" {
		t.Fatalf("expected strict FIFO [save, delete], got %v", events)
	}
	evMu.Unlock()

	// RestoreOpenPosition post-TP1 fee parity with GetPureRealizedEquityClosedTradesUSDT
	d2 := NewDryRunBroker(decimal.NewFromInt(1043), DryRunConfig{MaxSlots: 3})
	defer d2.Close()
	d2.RestoreOpenPosition(&VirtualOpenPosition{
		ClientOrderID: "DRY_RST", Symbol: "SOLUSDT", Side: model.SideLong,
		Qty: decimal.NewFromInt(5), InitialQty: decimal.NewFromInt(10),
		EntryPrice: decimal.NewFromInt(100), EntryFee: decimal.NewFromInt(10),
		CurrentSLPrice: decimal.NewFromInt(100),
	})
	if !d2.GetRealizedEquityUSDT().Equal(decimal.NewFromInt(1038)) {
		t.Fatalf("expected prorated entryFeeRem=5 subtracted (1043-5=1038), got %s", d2.GetRealizedEquityUSDT())
	}
}

func TestLiveBroker_TP1MarketFallback_AmbiguousError_IsTrackedGuards_AndTierNeverDowngrades(t *testing.T) {
	// 1. TP1 Market Fallback: when cancel fails and resting TP1 is still PARTIALLY_FILLED, do NOT double subtract or send MarketClose
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		PartialTPEnabled: true,
		PartialTP1Ratio:  0.50,
		TrailingEnabled:  true,
	})
	defer b.Close()

	var marketCloseCalls atomic.Int32
	doneQuery := make(chan struct{})
	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
		return fmt.Errorf("transient cancel error")
	}
	b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
		defer close(doneQuery)
		return &model.LiveOrderRecord{
			ClientOrderID: clientOrderID, ExchangeOrderID: 8801, Symbol: symbol,
			Status: model.StatusPartiallyFilled, CumulativeFilledQty: decimal.NewFromFloat(3.0),
			AvgPrice: decimal.NewFromFloat(102.0),
		}, nil
	}
	b.MarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, decimal.Decimal, decimal.Decimal, error) {
		marketCloseCalls.Add(1)
		return 9901, decimal.NewFromFloat(103), qty, nil
	}

	t0 := time.Now().UTC()
	b.Mu.Lock()
	b.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(8.0) // 10 - 2 already filled
	b.InitialQtys["SOLUSDT"] = decimal.NewFromFloat(10.0)
	b.TP1FilledQtys["SOLUSDT"] = decimal.NewFromFloat(2.0)
	b.PositionSides["SOLUSDT"] = model.SideLong
	b.PositionEntryPrices["SOLUSDT"] = decimal.NewFromFloat(100.0)
	b.PositionOpenTimes["SOLUSDT"] = t0
	b.ATRAtEntry["SOLUSDT"] = decimal.NewFromFloat(1.0)
	b.TP1Prices["SOLUSDT"] = decimal.NewFromFloat(102.0)
	b.Protections["SOLUSDT"] = &model.LivePositionProtection{
		Symbol: "SOLUSDT", Side: model.SideLong, PlanGeneration: 1,
		TP1OrderID: 8801, TP1ClientOrderID: "ALGO_TP1_SOL",
	}
	b.Mu.Unlock()

	b.OnRealtimeTick("SOLUSDT", 103.0, 103.1, 103.0, 103.0, 1.0, false, t0.Add(time.Second))
	<-doneQuery
	b.Close() // Wait async goroutine finish

	if marketCloseCalls.Load() != 0 {
		t.Fatalf("expected 0 MarketClose calls while resting TP1 is still PARTIALLY_FILLED, got %d", marketCloseCalls.Load())
	}
	b.Mu.Lock()
	if b.IsTP1Hits["SOLUSDT"] || b.Protections["SOLUSDT"].TP1OrderID != 8801 || !b.ActivePositions["SOLUSDT"].Equal(decimal.NewFromFloat(7.0)) {
		t.Fatalf("expected resting TP1 ID restored and 1.0 delta applied (8->7), hit=%v prot=%+v qty=%s",
			b.IsTP1Hits["SOLUSDT"], b.Protections["SOLUSDT"], b.ActivePositions["SOLUSDT"])
	}
	b.Mu.Unlock()

	// 2. Ambiguous PlaceEntry + QueryOrder failure preserves OrdersByClient & SL metadata
	b2 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, AccountLeverage: 8.0, EntryOrderType: "LIMIT"})
	defer b2.Close()
	b2.PlaceEntryFn = func(ctx context.Context, intent model.OrderIntent) (int64, error) {
		return 0, fmt.Errorf("context deadline exceeded")
	}
	b2.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
		return nil, fmt.Errorf("i/o timeout")
	}
	b2.HandleAccountUpdate(decimal.NewFromInt(1000), decimal.Zero, decimal.NewFromInt(1000), nil, t0)
	_, _ = b2.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "AMB_1", Symbol: "ADAUSDT", Side: model.SideLong,
		Qty: decimal.NewFromFloat(100), LimitPrice: decimal.NewFromFloat(1.0),
		SLPrice: decimal.NewFromFloat(0.95), TPPrice: decimal.NewFromFloat(1.10), ATRAtEntry: decimal.NewFromFloat(0.02),
	})
	b2.Mu.Lock()
	ambRec := b2.OrdersByClient["AMB_1"]
	b2.Mu.Unlock()
	if ambRec == nil || !ambRec.SLPrice.Equal(decimal.NewFromFloat(0.95)) {
		t.Fatalf("expected ambiguous failure to preserve OrdersByClient & SLPrice, got %+v", ambRec)
	}

	// 3. Empty/zero isTrackedSL/isTrackedTP must NOT match MANUAL_CLOSE as STOP_LOSS or TAKE_PROFIT
	b3 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3})
	defer b3.Close()
	var closedReason string
	doneTrade := make(chan struct{})
	b3.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedReason = rec.ExitReason
		close(doneTrade)
	}
	b3.Mu.Lock()
	b3.ActivePositions["DOGEUSDT"] = decimal.NewFromFloat(100)
	b3.PositionSides["DOGEUSDT"] = model.SideLong
	b3.PositionEntryPrices["DOGEUSDT"] = decimal.NewFromFloat(0.10)
	b3.PositionOpenTimes["DOGEUSDT"] = t0
	b3.Protections["DOGEUSDT"] = &model.LivePositionProtection{
		Symbol: "DOGEUSDT", Side: model.SideLong,
		SLOrderID: 12345, SLClientAlgoID: "REAL_SL",
		TPOrderID: 0, TPClientAlgoID: "", // Empty TP tracking (e.g. Partial+Trailing)
	}
	b3.Mu.Unlock()
	b3.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "", ExchangeOrderID: 0, Symbol: "DOGEUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(100), AvgPrice: decimal.NewFromFloat(0.11),
	})
	<-doneTrade
	if closedReason != "MANUAL_CLOSE" {
		t.Fatalf("expected MANUAL_CLOSE when clientOrderID=='' and exchangeOrderID==0, got %q", closedReason)
	}

	// 4. replaceStopLossAsync must NOT downgrade ProtectionTier from 2 to 1
	b4 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3})
	b4.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		return 77701, nil
	}
	b4.Mu.Lock()
	b4.Protections["AVAXUSDT"] = &model.LivePositionProtection{Symbol: "AVAXUSDT", PlanGeneration: 1}
	b4.ProtectionTiers["AVAXUSDT"] = 2
	b4.Mu.Unlock()
	doneReplace := make(chan struct{})
	b4.replaceStopLossAsync(slReplaceRequest{
		symbol: "AVAXUSDT", exitSide: model.SideShort, targetQty: decimal.NewFromFloat(1),
		targetSL: decimal.NewFromFloat(20), clientSLAlgoID: "SL_TIER1", generation: 1,
		targetTier: 1, maxAttempts: 1, onDone: func() { close(doneReplace) },
	})
	<-doneReplace
	b4.Close()
	b4.Mu.Lock()
	tier := b4.ProtectionTiers["AVAXUSDT"]
	b4.Mu.Unlock()
	if tier != 2 {
		t.Fatalf("expected ProtectionTier NOT downgraded from 2 to 1, got %d", tier)
	}
}

func TestDryRunBroker_FullTPRequiresContractPriceNotMarkPrice(t *testing.T) {
	now := time.Now().UTC()
	d := NewDryRunBroker(decimal.NewFromFloat(10000), DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		AccountLeverage:     5.0,
		EntryOrderType:      "MARKET",
		TakerFeeRate:        0.0005,
	})
	defer d.Close()

	closedCh := make(chan TradeClosedRecord, 4)
	d.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedCh <- rec
	}

	// Long position: entry 100, TP 110. Mark hits 110.5 while BestBid stays at 109.5 -> must NOT close.
	ok, err := d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DRY_TP_LONG",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     now,
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry long failed: %v", err)
	}
	d.OnRealtimeTick("ETHUSDT", 109.5, 109.6, 110.5, 0, 0, false, now.Add(time.Second))
	d.Mu.Lock()
	_, stillOpenLong := d.OpenPositions["ETHUSDT"]
	d.Mu.Unlock()
	if !stillOpenLong {
		t.Fatalf("expected LONG position NOT to trigger full TP on markPrice=110.5 while bestBid=109.5")
	}

	// Trade/contract price reaches 110.0 -> triggers TAKE_PROFIT.
	d.OnRealtimeTick("ETHUSDT", 110.0, 110.1, 110.0, 110.0, 0.5, false, now.Add(2*time.Second))
	select {
	case rec := <-closedCh:
		if rec.ExitReason != "TAKE_PROFIT" {
			t.Fatalf("expected TAKE_PROFIT for long, got %s", rec.ExitReason)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for long TAKE_PROFIT")
	}

	// Short position: entry 100, TP 90. Mark hits 89.5 while BestAsk stays at 90.5 -> must NOT close.
	ok, err = d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DRY_TP_SHORT",
		Symbol:        "SOLUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(105.0),
		TPPrice:       decimal.NewFromFloat(90.0),
		CreatedAt:     now.Add(3 * time.Second),
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry short failed: %v", err)
	}
	d.OnRealtimeTick("SOLUSDT", 90.4, 90.5, 89.5, 0, 0, false, now.Add(4*time.Second))
	d.Mu.Lock()
	_, stillOpenShort := d.OpenPositions["SOLUSDT"]
	d.Mu.Unlock()
	if !stillOpenShort {
		t.Fatalf("expected SHORT position NOT to trigger full TP on markPrice=89.5 while bestAsk=90.5")
	}
	d.OnRealtimeTick("SOLUSDT", 89.9, 90.0, 90.0, 90.0, 0.5, true, now.Add(5*time.Second))
	select {
	case rec := <-closedCh:
		if rec.ExitReason != "TAKE_PROFIT" {
			t.Fatalf("expected TAKE_PROFIT for short, got %s", rec.ExitReason)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for short TAKE_PROFIT")
	}
}

func TestDryRunBroker_PartialFillTimeoutEvaluatesSLOnSameTick(t *testing.T) {
	now := time.Now().UTC()
	d := NewDryRunBroker(decimal.NewFromFloat(10000), DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		AccountLeverage:     5.0,
		EntryOrderType:      "LIMIT",
		LimitOrderTimeout:   10 * time.Second,
		MakerFeeRate:        0.0002,
		TakerFeeRate:        0.0005,
	})
	defer d.Close()

	closedCh := make(chan TradeClosedRecord, 2)
	d.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedCh <- rec
	}

	ok, err := d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DRY_PARTIAL_TO",
		Symbol:        "BTCUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(10.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(95.0),
		TPPrice:       decimal.NewFromFloat(110.0),
		CreatedAt:     now,
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}

	// Partial fill of 4 out of 10 at t+1s.
	d.OnRealtimeTick("BTCUSDT", 100.0, 100.1, 100.0, 100.0, 4.0, true, now.Add(1*time.Second))

	// At t+15s (past 10s LimitOrderTimeout), a sell trade at 100 arrives simultaneously with mark breaching SL (94.5).
	// Expired order MUST NOT fill the remaining 6, AND SL MUST evaluate and close the 4-unit position on this exact tick.
	d.OnRealtimeTick("BTCUSDT", 94.4, 94.6, 94.5, 100.0, 6.0, true, now.Add(15*time.Second))

	d.Mu.Lock()
	_, hasPending := d.PendingOrders["BTCUSDT"]
	_, hasPos := d.OpenPositions["BTCUSDT"]
	d.Mu.Unlock()
	if hasPending || hasPos {
		t.Fatalf("expected pending order purged and open position closed by SL on same timeout tick (hasPending=%v hasPos=%v)", hasPending, hasPos)
	}

	select {
	case rec := <-closedCh:
		if rec.ExitReason != "STOP_LOSS" || !rec.Qty.Equal(decimal.NewFromFloat(4.0)) {
			t.Fatalf("expected STOP_LOSS with qty=4, got reason=%s qty=%s", rec.ExitReason, rec.Qty)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for STOP_LOSS trade closed record")
	}
}

func TestDryRunBroker_MaxHoldingRespectsMarkFreshness(t *testing.T) {
	now := time.Now().UTC()
	d := NewDryRunBroker(decimal.NewFromFloat(10000), DryRunConfig{
		MaxSlots:            3,
		MaxDailyDrawdownPct: -0.05,
		AccountLeverage:     5.0,
		EntryOrderType:      "MARKET",
		MaxHoldingDuration:  1 * time.Hour,
		MaxMarkStaleness:    30 * time.Second,
		TakerFeeRate:        0.0005,
	})
	defer d.Close()

	closedCh := make(chan TradeClosedRecord, 2)
	d.OnTradeClosedFn = func(rec TradeClosedRecord) {
		closedCh <- rec
	}

	ok, err := d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "DRY_STALE_MAXHOLD",
		Symbol:        "ETHUSDT",
		Side:          model.SideLong,
		Qty:           decimal.NewFromFloat(1.0),
		LimitPrice:    decimal.NewFromFloat(100.0),
		SLPrice:       decimal.NewFromFloat(90.0),
		TPPrice:       decimal.NewFromFloat(120.0),
		CreatedAt:     now,
	})
	if !ok || err != nil {
		t.Fatalf("SubmitLimitEntry failed: %v", err)
	}

	// Mark price updated at t+1m, then feed goes silent for ~2h.
	d.OnRealtimeTick("ETHUSDT", 99.9, 100.1, 100.0, 0, 0, false, now.Add(1*time.Minute))
	d.CheckTimeouts(now.Add(2 * time.Hour))

	d.Mu.Lock()
	_, stillOpen := d.OpenPositions["ETHUSDT"]
	d.Mu.Unlock()
	if !stillOpen {
		t.Fatalf("expected position NOT closed by TIME_STOP_MAX_HOLDING when mark price is stale > MaxMarkStaleness")
	}

	// Fresh mark arrives -> next CheckTimeouts closes with TIME_STOP_MAX_HOLDING.
	tFresh := now.Add(2*time.Hour + 1*time.Second)
	d.OnRealtimeTick("ETHUSDT", 99.9, 100.1, 100.0, 0, 0, false, tFresh)
	d.CheckTimeouts(tFresh.Add(1 * time.Second))

	select {
	case rec := <-closedCh:
		if rec.ExitReason != "TIME_STOP_MAX_HOLDING" {
			t.Fatalf("expected TIME_STOP_MAX_HOLDING once mark is fresh, got %s", rec.ExitReason)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for TIME_STOP_MAX_HOLDING close")
	}
}

func TestLiveBroker_ReconcileOfflineCloseAbortedWhenPositionRiskNonZeroOrErrored(t *testing.T) {
	now := time.Now().UTC()
	setup := func() (*BinanceLiveBroker, *atomic.Int32) {
		b := NewBinanceLiveBroker(LiveBrokerConfig{
			MaxSlots:         3,
			PartialTPEnabled: true,
			TrailingEnabled:  true,
		})
		var canceledAlgos atomic.Int32
		b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
			canceledAlgos.Add(1)
			return nil
		}
		b.Mu.Lock()
		b.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(5)
		b.PositionSides["SOLUSDT"] = model.SideShort
		b.PositionOpenTimes["SOLUSDT"] = now.Add(-2 * time.Minute) // past 15s grace
		b.Protections["SOLUSDT"] = &model.LivePositionProtection{
			Symbol:         "SOLUSDT",
			Side:           model.SideShort,
			SLOrderID:      9001,
			SLClientAlgoID: "ALGO_SL_SOL",
			SLStatus:       model.ProtectionAcked,
			ProtectedQty:   decimal.NewFromFloat(5),
		}
		b.Mu.Unlock()
		return b, &canceledAlgos
	}

	// 1. ConfirmPositionZeroFn returns (false, nil) -> position STILL open -> must NOT delete state or cancel SL.
	b1, canceled1 := setup()
	b1.ConfirmPositionZeroFn = func(ctx context.Context, symbol string) (bool, error) {
		return false, nil
	}
	b1.reconcileAfterAccountSnapshot(map[string]model.LivePosition{}, now)
	b1.Close()
	if canceled1.Load() != 0 || !b1.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected state & SL preserved when ConfirmPositionZeroFn reports position non-zero (canceled=%d)", canceled1.Load())
	}

	// 2. ConfirmPositionZeroFn returns error -> fail-closed -> must NOT delete state or cancel SL.
	b2, canceled2 := setup()
	b2.ConfirmPositionZeroFn = func(ctx context.Context, symbol string) (bool, error) {
		return false, fmt.Errorf("api timeout")
	}
	b2.reconcileAfterAccountSnapshot(map[string]model.LivePosition{}, now)
	b2.Close()
	if canceled2.Load() != 0 || !b2.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected state & SL preserved when ConfirmPositionZeroFn errors (canceled=%d)", canceled2.Load())
	}

	// 3. ConfirmPositionZeroFn confirms zero (true, nil) -> cleans up and cancels leftover SL.
	b3, canceled3 := setup()
	b3.ConfirmPositionZeroFn = func(ctx context.Context, symbol string) (bool, error) {
		return true, nil
	}
	b3.reconcileAfterAccountSnapshot(map[string]model.LivePosition{}, now)
	b3.Close()
	if canceled3.Load() != 1 || b3.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected state cleaned up & SL canceled when ConfirmPositionZeroFn confirms zero (canceled=%d)", canceled3.Load())
	}
}

func TestLiveBroker_RestoreOpenPosition_UndersizedSLTriggersUpgradeWithoutNakedWindow(t *testing.T) {
	now := time.Now().UTC()
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		PartialTPEnabled: true,
		TrailingEnabled:  true,
	})
	defer b.Close()

	// Undersized SL exists on exchange (qty 4 while live position grew to qty 10).
	b.VerifyAlgoOrderDetailsFn = func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (AlgoOrderVerifyResult, error) {
		return AlgoOrderVerifyResult{
			SLAlive:         true,
			SLQty:           decimal.NewFromFloat(4),
			SLClosePosition: false,
		}, nil
	}

	var mu sync.Mutex
	var events []string
	var newSLQty decimal.Decimal
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		mu.Lock()
		events = append(events, fmt.Sprintf("place:%s", qty.String()))
		newSLQty = qty
		mu.Unlock()
		return 7777, nil
	}
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		mu.Lock()
		events = append(events, fmt.Sprintf("cancel:%d", algoID))
		mu.Unlock()
		return nil
	}

	b.RestoreOpenPosition(&LiveOpenPosition{
		Symbol:         "SOLUSDT",
		Side:           model.SideShort,
		Qty:            decimal.NewFromFloat(10),
		InitialQty:     decimal.NewFromFloat(4),
		ProtectedQty:   decimal.NewFromFloat(4),
		EntryPrice:     decimal.NewFromFloat(100),
		SLOrderID:      5555,
		SLClientAlgoID: "ALGO_SL_OLD",
		SLPrice:        decimal.NewFromFloat(105),
		InitialSLPrice: decimal.NewFromFloat(105),
		OpenTime:       now.Add(-2 * time.Minute),
		UpdatedAt:      now,
	})

	b.Mu.Lock()
	prot := b.Protections["SOLUSDT"]
	b.Mu.Unlock()
	if prot == nil || prot.SLStatus != model.ProtectionFailed || prot.SLOrderID != 5555 || !prot.ProtectedQty.Equal(decimal.NewFromFloat(4)) {
		t.Fatalf("expected ProtectionFailed with retained SLOrderID=5555 and ProtectedQty=4, got %+v", prot)
	}

	b.RepairFailedProtections(now)

	mu.Lock()
	gotEvents := append([]string(nil), events...)
	mu.Unlock()
	if len(gotEvents) != 2 || gotEvents[0] != "place:10" || gotEvents[1] != "cancel:5555" {
		t.Fatalf("expected anti-naked order [place:10 cancel:5555], got %v", gotEvents)
	}
	b.Mu.Lock()
	protAfter := b.Protections["SOLUSDT"]
	b.Mu.Unlock()
	if !newSLQty.Equal(decimal.NewFromFloat(10)) || protAfter.SLStatus != model.ProtectionAcked || !protAfter.ProtectedQty.Equal(decimal.NewFromFloat(10)) {
		t.Fatalf("expected upgraded SL ProtectedQty=10 and ProtectionAcked, got %+v", protAfter)
	}
}

func TestLiveBroker_CheckPendingEntryTimeouts_CancelWithMissedPartialFillArmsSL(t *testing.T) {
	now := time.Now().UTC()
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:          3,
		LimitOrderTimeout: 30 * time.Second,
		PartialTPEnabled:  true,
		TrailingEnabled:   true,
	})
	defer b.Close()

	// Local record still has CumulativeFilledQty == 0 due to WS disconnect,
	// while CancelOrderWithResultFn reveals executedQty == 4.
	b.CancelOrderWithResultFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) (*model.LiveOrderRecord, error) {
		return &model.LiveOrderRecord{
			ClientOrderID:       clientOrderID,
			ExchangeOrderID:     exchangeOrderID,
			Symbol:              symbol,
			Status:              model.StatusCanceled,
			CumulativeFilledQty: decimal.NewFromFloat(4),
			AvgPrice:            decimal.NewFromFloat(100),
		}, nil
	}

	slPlacedCh := make(chan decimal.Decimal, 1)
	var placedSLPrice decimal.Decimal
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			placedSLPrice = triggerPrice
			slPlacedCh <- qty
		}
		return 8888, nil
	}

	b.Mu.Lock()
	b.InitialSLPrices["SOLUSDT"] = decimal.NewFromFloat(95)
	b.OrdersByClient["MISS_PARTIAL"] = &model.LiveOrderRecord{
		ClientOrderID:       "MISS_PARTIAL",
		ExchangeOrderID:     701,
		Symbol:              "SOLUSDT",
		Side:                model.SideLong,
		IsEntry:             true,
		Status:              model.StatusNew,
		TargetQty:           decimal.NewFromFloat(10),
		CumulativeFilledQty: decimal.Zero,
		LimitPrice:          decimal.NewFromFloat(100),
		SLPrice:             decimal.NewFromFloat(95),
		InitialSLPrice:      decimal.NewFromFloat(95),
		CreatedAt:           now.Add(-1 * time.Minute),
	}
	b.OrdersByExchangeID[701] = b.OrdersByClient["MISS_PARTIAL"]
	b.Mu.Unlock()

	b.CheckPendingEntryTimeouts(now)

	select {
	case slQty := <-slPlacedCh:
		if !slQty.Equal(decimal.NewFromFloat(4)) || !placedSLPrice.Equal(decimal.NewFromFloat(95)) {
			t.Fatalf("expected STOP_MARKET SL qty=4 price=95, got qty=%s price=%s", slQty, placedSLPrice)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for SL placement on missed partial fill")
	}

	b.Mu.Lock()
	_, orderStillPending := b.OrdersByClient["MISS_PARTIAL"]
	posQty := b.ActivePositions["SOLUSDT"]
	initQty := b.InitialQtys["SOLUSDT"]
	b.Mu.Unlock()
	if orderStillPending || !posQty.Equal(decimal.NewFromFloat(4)) || !initQty.Equal(decimal.NewFromFloat(4)) {
		t.Fatalf("expected order cleared and position qty=4 preserved, pending=%v pos=%s init=%s", orderStillPending, posQty, initQty)
	}
}

func TestLiveBroker_CheckPendingEntryTimeouts_2011RefreshFailureRetainsOrderForRetry(t *testing.T) {
	now := time.Now().UTC()
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:          3,
		LimitOrderTimeout: 30 * time.Second,
		PartialTPEnabled:  true,
		TrailingEnabled:   true,
	})
	defer b.Close()

	b.CancelOrderFn = func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
		return fmt.Errorf("binance error code=-2011 Unknown order sent")
	}
	b.QueryOrderFn = func(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
		return &model.LiveOrderRecord{
			ClientOrderID:       clientOrderID,
			ExchangeOrderID:     802,
			Symbol:              symbol,
			Side:                model.SideLong,
			IsEntry:             true,
			Status:              model.StatusFilled,
			TargetQty:           decimal.NewFromFloat(5),
			CumulativeFilledQty: decimal.NewFromFloat(5),
			AvgPrice:            decimal.NewFromFloat(100),
		}, nil
	}

	var failRefresh atomic.Bool
	failRefresh.Store(true)
	b.FetchAccountFn = func(ctx context.Context) (decimal.Decimal, decimal.Decimal, decimal.Decimal, map[string]model.LivePosition, error) {
		if failRefresh.Load() {
			return decimal.Zero, decimal.Zero, decimal.Zero, nil, fmt.Errorf("temporary network error")
		}
		return decimal.NewFromFloat(1000), decimal.Zero, decimal.NewFromFloat(900), map[string]model.LivePosition{
			"SOLUSDT": {Symbol: "SOLUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(5), EntryPrice: decimal.NewFromFloat(100)},
		}, nil
	}
	slPlacedCh := make(chan decimal.Decimal, 1)
	b.PlaceAlgoOrderFn = func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (int64, error) {
		if orderType == "STOP_MARKET" {
			slPlacedCh <- qty
		}
		return 9090, nil
	}

	b.Mu.Lock()
	b.OrdersByClient["RETRY_2011"] = &model.LiveOrderRecord{
		ClientOrderID:   "RETRY_2011",
		ExchangeOrderID: 802,
		Symbol:          "SOLUSDT",
		Side:            model.SideLong,
		IsEntry:         true,
		Status:          model.StatusNew,
		TargetQty:       decimal.NewFromFloat(5),
		LimitPrice:      decimal.NewFromFloat(100),
		SLPrice:         decimal.NewFromFloat(95),
		InitialSLPrice:  decimal.NewFromFloat(95),
		CreatedAt:       now.Add(-1 * time.Minute),
	}
	b.OrdersByExchangeID[802] = b.OrdersByClient["RETRY_2011"]
	b.Mu.Unlock()

	// 1. RefreshAccount fails -> order record MUST be retained for retry!
	b.CheckPendingEntryTimeouts(now)
	b.Mu.Lock()
	_, retained := b.OrdersByClient["RETRY_2011"]
	b.Mu.Unlock()
	if !retained {
		t.Fatalf("expected RETRY_2011 retained in OrdersByClient when RefreshAccount fails")
	}

	// 2. RefreshAccount recovers on next tick -> position & SL armed with qty=5!
	failRefresh.Store(false)
	b.CheckPendingEntryTimeouts(now.Add(2 * time.Second))
	select {
	case slQty := <-slPlacedCh:
		if !slQty.Equal(decimal.NewFromFloat(5)) {
			t.Fatalf("expected SL qty=5 after RefreshAccount recovery, got %s", slQty)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for SL placement after RefreshAccount recovery")
	}
}

func TestLiveBroker_ManualPartialCloseOver50PctAccountUpdateFirstPreservesSL(t *testing.T) {
	now := time.Now().UTC()

	// Case 1: Manual partial close >50% (close 6 of 10), ACCOUNT_UPDATE arrives BEFORE ORDER_TRADE_UPDATE.
	b := NewBinanceLiveBroker(LiveBrokerConfig{
		MaxSlots:         3,
		PartialTPEnabled: true,
		TrailingEnabled:  true,
	})
	var slCanceled atomic.Int32
	b.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		slCanceled.Add(1)
		return nil
	}
	b.Mu.Lock()
	b.ActivePositions["SOLUSDT"] = decimal.NewFromFloat(10)
	b.PositionSides["SOLUSDT"] = model.SideLong
	b.PositionEntryPrices["SOLUSDT"] = decimal.NewFromFloat(100)
	b.PositionOpenTimes["SOLUSDT"] = now.Add(-1 * time.Minute)
	b.Protections["SOLUSDT"] = &model.LivePositionProtection{
		Symbol: "SOLUSDT", Side: model.SideLong,
		SLOrderID: 1111, SLClientAlgoID: "ALGO_SL_SOL", SLStatus: model.ProtectionAcked,
		ProtectedQty: decimal.NewFromFloat(10),
	}
	b.Mu.Unlock()

	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000),
		Positions: map[string]model.LivePosition{
			"SOLUSDT": {Symbol: "SOLUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(4), EntryPrice: decimal.NewFromFloat(100)},
		},
		EventTime: now, IsFullSnapshot: false,
	})
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "web_manual_6", ExchangeOrderID: 9901, Symbol: "SOLUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(6), AvgPrice: decimal.NewFromFloat(102),
	})
	b.Close()

	b.Mu.Lock()
	remQty := b.ActivePositions["SOLUSDT"]
	prot1 := b.Protections["SOLUSDT"]
	b.Mu.Unlock()
	if slCanceled.Load() != 0 || prot1 == nil || !remQty.Equal(decimal.NewFromFloat(4)) {
		t.Fatalf("expected remaining qty=4 and SL preserved (canceled=%d prot=%+v qty=%s)", slCanceled.Load(), prot1, remQty)
	}

	// Case 2: Manual partial close <=50% (close 4 of 10), ACCOUNT_UPDATE(6) arrives BEFORE ORDER_TRADE_UPDATE(4) -> NO double deduction!
	b2 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, PartialTPEnabled: true, TrailingEnabled: true})
	var slCanceled2 atomic.Int32
	b2.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		slCanceled2.Add(1)
		return nil
	}
	b2.Mu.Lock()
	b2.ActivePositions["ETHUSDT"] = decimal.NewFromFloat(10)
	b2.PositionSides["ETHUSDT"] = model.SideLong
	b2.PositionEntryPrices["ETHUSDT"] = decimal.NewFromFloat(2000)
	b2.PositionOpenTimes["ETHUSDT"] = now.Add(-1 * time.Minute)
	b2.Protections["ETHUSDT"] = &model.LivePositionProtection{
		Symbol: "ETHUSDT", Side: model.SideLong,
		SLOrderID: 2222, SLClientAlgoID: "ALGO_SL_ETH", SLStatus: model.ProtectionAcked,
		ProtectedQty: decimal.NewFromFloat(10),
	}
	b2.Mu.Unlock()

	b2.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000),
		Positions: map[string]model.LivePosition{
			"ETHUSDT": {Symbol: "ETHUSDT", Side: model.SideLong, Qty: decimal.NewFromFloat(6), EntryPrice: decimal.NewFromFloat(2000)},
		},
		EventTime: now, IsFullSnapshot: false,
	})
	b2.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "web_manual_4", ExchangeOrderID: 9902, Symbol: "ETHUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(4), AvgPrice: decimal.NewFromFloat(2010),
	})
	b2.Close()

	b2.Mu.Lock()
	remQty2 := b2.ActivePositions["ETHUSDT"]
	b2.Mu.Unlock()
	if slCanceled2.Load() != 0 || !remQty2.Equal(decimal.NewFromFloat(6)) {
		t.Fatalf("expected no double-deduction (qty=6) and SL preserved, got qty=%s canceled=%d", remQty2, slCanceled2.Load())
	}

	// Case 3: Full manual close (ACCOUNT_UPDATE(0) arrives BEFORE ORDER_TRADE_UPDATE(10)) still cancels SL cleanly!
	b3 := NewBinanceLiveBroker(LiveBrokerConfig{MaxSlots: 3, PartialTPEnabled: true, TrailingEnabled: true})
	var slCanceled3 atomic.Int32
	b3.CancelAlgoOrderFn = func(ctx context.Context, symbol string, algoID int64) error {
		slCanceled3.Add(1)
		return nil
	}
	b3.Mu.Lock()
	b3.ActivePositions["BTCUSDT"] = decimal.NewFromFloat(10)
	b3.PositionSides["BTCUSDT"] = model.SideLong
	b3.PositionEntryPrices["BTCUSDT"] = decimal.NewFromFloat(50000)
	b3.PositionOpenTimes["BTCUSDT"] = now.Add(-1 * time.Minute)
	b3.Protections["BTCUSDT"] = &model.LivePositionProtection{
		Symbol: "BTCUSDT", Side: model.SideLong,
		SLOrderID: 3333, SLClientAlgoID: "ALGO_SL_BTC", SLStatus: model.ProtectionAcked,
		ProtectedQty: decimal.NewFromFloat(10),
	}
	b3.Mu.Unlock()

	b3.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance: decimal.NewFromFloat(1000),
		Positions: map[string]model.LivePosition{
			"BTCUSDT": {Symbol: "BTCUSDT", Side: model.SideLong, Qty: decimal.Zero},
		},
		EventTime: now, IsFullSnapshot: false,
	})
	b3.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID: "web_manual_full", ExchangeOrderID: 9903, Symbol: "BTCUSDT",
		Status: model.StatusFilled, CumFilledQty: decimal.NewFromFloat(10), AvgPrice: decimal.NewFromFloat(51000),
	})
	b3.Close()

	if slCanceled3.Load() != 1 || b3.HasActiveOrderOrPosition("BTCUSDT") {
		t.Fatalf("expected full manual close to cancel SL (1) and clear BTCUSDT state, got canceled=%d active=%v", slCanceled3.Load(), b3.HasActiveOrderOrPosition("BTCUSDT"))
	}
}

func TestDryRunBroker_StrategyExitProfilesAndFilteredSlots(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	d := NewDryRunBroker(decimal.NewFromFloat(1000.0), DryRunConfig{
		MaxSlots:              9,
		AccountLeverage:       10.0,
		EntryOrderType:        "MARKET",
		StaleLossExitDuration: 6 * time.Hour,
		MaxHoldingDuration:    18 * time.Hour,
		MaxMarkStaleness:      15 * time.Second,
		StrategyExitProfiles: map[string]protection.StrategyExitProfile{
			"MAJOR_RSMOM_LONG": {
				HasOverride:           true,
				TrailingEnabled:       true,
				TrailingActivateATR:   2.5,
				TrailingDistanceATR:   1.2,
				StaleLossExitDuration: 0,
				MaxHoldingDuration:    48 * time.Hour,
			},
		},
	})
	defer d.Close()

	_, err := d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "ALT_VWAP_1", Symbol: "ALTUSDT", Side: model.SideShort, StrategyID: "VWAP_PULLBACK",
		Qty: decimal.NewFromFloat(1), LimitPrice: decimal.NewFromFloat(100), SLPrice: decimal.NewFromFloat(110),
		TPPrice: decimal.NewFromFloat(80), ATRAtEntry: decimal.NewFromFloat(2), CreatedAt: t0,
	})
	if err != nil {
		t.Fatalf("ALT submit err: %v", err)
	}
	_, err = d.SubmitLimitEntry(model.OrderIntent{
		ClientOrderID: "SOL_MAJ_1", Symbol: "SOLUSDT", Side: model.SideLong, StrategyID: "MAJOR_RSMOM_LONG",
		Qty: decimal.NewFromFloat(1), LimitPrice: decimal.NewFromFloat(100), SLPrice: decimal.NewFromFloat(90),
		TPPrice: decimal.NewFromFloat(120), ATRAtEntry: decimal.NewFromFloat(2), CreatedAt: t0,
	})
	if err != nil {
		t.Fatalf("SOL submit err: %v", err)
	}

	if majSlots := d.GetActiveSlotCountByFilter(func(s string) bool { return s == "MAJOR_RSMOM_LONG" }); majSlots != 1 {
		t.Fatalf("expected 1 MAJOR_RSMOM_LONG slot, got %d", majSlots)
	}
	if nonMaj := d.GetActiveSlotCountByFilter(func(s string) bool { return s != "MAJOR_RSMOM_LONG" }); nonMaj != 1 {
		t.Fatalf("expected 1 non-major slot, got %d", nonMaj)
	}

	// At t0+7h underwater: ALTUSDT closes on 6h stale-loss, SOLUSDT ignores 6h stale-loss
	t7 := t0.Add(7 * time.Hour)
	d.OnRealtimeTick("ALTUSDT", 100.9, 101.1, 101.0, 101.0, 1, false, t7)
	d.OnRealtimeTick("SOLUSDT", 98.9, 99.1, 99.0, 99.0, 1, false, t7)
	if d.HasActiveOrderOrPosition("ALTUSDT") {
		t.Fatalf("expected ALTUSDT closed after 6h stale-loss")
	}
	if !d.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected SOLUSDT to remain open past 6h stale-loss")
	}

	// At t0+20h profitable (past 18h QMAX max hold): SOLUSDT STILL remains open (< 48h)
	t20 := t0.Add(20 * time.Hour)
	d.OnRealtimeTick("SOLUSDT", 101.9, 102.1, 102.0, 102.0, 1, false, t20)
	if !d.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected SOLUSDT to remain open at 20h (< 48h)")
	}

	// At t0+49h: SOLUSDT closes on 48h max holding
	t49 := t0.Add(49 * time.Hour)
	d.OnRealtimeTick("SOLUSDT", 101.9, 102.1, 102.0, 102.0, 1, false, t49)
	if d.HasActiveOrderOrPosition("SOLUSDT") {
		t.Fatalf("expected SOLUSDT closed at 49h (> 48h)")
	}
}
