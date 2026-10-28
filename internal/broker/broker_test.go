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

func TestDryRunBrokerPersistence_FIFOOrdering(t *testing.T) {
	cfg := DryRunConfig{
		MaxSlots:            3,
		AccountLeverage:     5.0,
		EntryOrderType:      "MARKET",
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








