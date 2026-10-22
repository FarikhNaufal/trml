package broker

import (
	"context"
	"fmt"
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






