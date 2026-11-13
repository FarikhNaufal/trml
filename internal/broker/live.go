package broker

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
	"trml/internal/model"
)

type LiveBrokerConfig struct {
	MaxSlots                 int
	MaxDailyDrawdownPct      float64
	MaxAccountStaleness      time.Duration
	LimitOrderTimeout        time.Duration
	MaxHoldingDuration       time.Duration
	BreakEvenTriggerATR      float64
	Tier1BEEnabled           bool
	Tier1BETriggerLongATR    float64
	Tier1BETriggerShortATR   float64
	Tier2ProfitLockEnabled   bool
	Tier2ProfitLockATR       float64
	Tier2ProfitLockOffsetATR float64
	PartialTPEnabled         bool
	PartialTP1Ratio          float64
	PartialTP1ATRMult        float64
	RunnerBECushionATR       float64
	TrailingEnabled          bool
	TrailingActivateATR      float64
	TrailingDistanceATR      float64
	MinBreakEvenOffsetPct    float64
	SlippageRate             float64
	TakerFeeRate             float64
	MakerFeeRate             float64
}

type LiveOpenPosition struct {
	Symbol         string
	Side           model.Side
	Qty            decimal.Decimal
	InitialQty     decimal.Decimal
	EntryPrice     decimal.Decimal
	SLOrderID      int64
	SLClientAlgoID string
	SLPrice        decimal.Decimal
	InitialSLPrice decimal.Decimal
	TPOrderID      int64
	TPClientAlgoID string
	TPPrice          decimal.Decimal
	TP1Price         decimal.Decimal
	TP1OrderID       int64
	TP1ClientOrderID string
	IsTP1Hit         bool
	ATRAtEntry     decimal.Decimal
	ProtectionTier int
	SignalAuditID  int64
	OpenTime       time.Time
	MaxMAE         float64
	MaxMFE         float64
	PeakPrice      decimal.Decimal
	UpdatedAt      time.Time
}

type BinanceLiveBroker struct {
	Mu                     sync.Mutex
	Cfg                    LiveBrokerConfig
	SessionStartEpochDay   int64
	SessionStartEquityUSDT decimal.Decimal
	WalletBalanceUSDT      decimal.Decimal
	UnrealizedPnLUSDT      decimal.Decimal
	AvailableBalanceUSDT   decimal.Decimal
	LastAccountUpdateTime  time.Time
	CircuitBreakerTripped  bool

	OrdersByClient      map[string]*model.LiveOrderRecord
	OrdersByExchangeID  map[int64]*model.LiveOrderRecord
	Protections         map[string]*model.LivePositionProtection
	ActivePositions     map[string]decimal.Decimal // symbol -> current position qty
	InitialQtys         map[string]decimal.Decimal // symbol -> initial position qty
	InitialSLPrices     map[string]decimal.Decimal // symbol -> initial SL price
	TP1Prices           map[string]decimal.Decimal // symbol -> partial TP1 price
	IsTP1Hits           map[string]bool            // symbol -> true if TP1 already hit
	PositionOpenTimes   map[string]time.Time       // symbol -> time position was opened
	PositionSides       map[string]model.Side      // symbol -> position side
	PositionEntryPrices map[string]decimal.Decimal // symbol -> entry price
	ClosingInFlight     map[string]bool            // symbol -> true if emergency market close in flight
	TPRepairInFlight    map[string]bool            // symbol -> true if TP repair order in flight
	AlgoUpdateInFlight  map[string]bool            // symbol -> true if algo order update goroutine in flight
	TP1InFlight         map[string]bool            // symbol -> true if partial TP1 close in flight
	TP1FilledQtys       map[string]decimal.Decimal // symbol -> cumulative filled qty on resting TP1 order before final StatusFilled
	TP1LastFillPrices   map[string]decimal.Decimal // symbol -> last reported avg price for partially filled TP1 order
	TP1CommittedQtys    map[string]decimal.Decimal // symbol -> partial TP1 qty already logged after a canceled/expired resting TP1 order
	LastAlgoUpdateTimes map[string]time.Time       // symbol -> time of last algo order update
	LastSentSLPrices    map[string]decimal.Decimal // symbol -> last stop loss price sent to exchange
	ActiveMAE           map[string]float64         // symbol -> max adverse excursion pct
	ActiveMFE           map[string]float64         // symbol -> max favorable excursion pct
	ProtectionTiers     map[string]int             // symbol -> current protection tier (0, 1, 2, 3)
	ATRAtEntry          map[string]decimal.Decimal // symbol -> atr at entry
	PeakPrices          map[string]decimal.Decimal // symbol -> highest (long) or lowest (short) mark price

	PositionClientOrderIDs map[string]string // symbol -> clientOrderID of original entry
	ClosingReason          map[string]string // symbol -> reason for closing (e.g. TIME_STOP_MAX_HOLDING)

	PlaceEntryFn           func(ctx context.Context, intent model.OrderIntent) (exchangeOrderID int64, err error)
	PlaceAlgoOrderFn       func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (algoID int64, err error)
	MarketCloseFn          func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (orderID int64, avgPrice, executedQty decimal.Decimal, err error)
	PlaceLimitCloseFn      func(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (orderID int64, err error)
	CancelOrderFn          func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error
	CancelAlgoOrderFn      func(ctx context.Context, symbol string, algoID int64) error
	VerifyAlgoOrderFn      func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (slAlive, tpAlive bool, err error)
	EmergencyMarketCloseFn func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error
	QueryOrderFn           func(ctx context.Context, symbol string, clientOrderID string) (*model.LiveOrderRecord, error)
	GetExchangeFilterFn    func(symbol string) (model.ExchangeFilter, bool)
	OnTradeClosedFn        func(rec TradeClosedRecord)
	OnPositionSavedFn      func(pos LiveOpenPosition)
	OnPositionDeletedFn    func(symbol string)
	FetchAccountFn         func(ctx context.Context) (walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, err error)

	eventChan chan LivePositionEvent
	eventWg   sync.WaitGroup
	eventMu   sync.Mutex
	isClosed  bool
}

type LivePositionEventType int

const (
	LivePositionEventSave LivePositionEventType = iota
	LivePositionEventDelete
)

type LivePositionEvent struct {
	Type   LivePositionEventType
	Pos    LiveOpenPosition
	Symbol string
}

func NewBinanceLiveBroker(cfg LiveBrokerConfig) *BinanceLiveBroker {
	b := &BinanceLiveBroker{
		Cfg:                    cfg,
		OrdersByClient:         make(map[string]*model.LiveOrderRecord),
		OrdersByExchangeID:     make(map[int64]*model.LiveOrderRecord),
		Protections:            make(map[string]*model.LivePositionProtection),
		ActivePositions:        make(map[string]decimal.Decimal),
		InitialQtys:            make(map[string]decimal.Decimal),
		InitialSLPrices:        make(map[string]decimal.Decimal),
		TP1Prices:              make(map[string]decimal.Decimal),
		IsTP1Hits:              make(map[string]bool),
		PositionOpenTimes:      make(map[string]time.Time),
		PositionSides:          make(map[string]model.Side),
		PositionEntryPrices:    make(map[string]decimal.Decimal),
		ClosingInFlight:        make(map[string]bool),
		TPRepairInFlight:       make(map[string]bool),
		AlgoUpdateInFlight:     make(map[string]bool),
		TP1InFlight:            make(map[string]bool),
		TP1FilledQtys:          make(map[string]decimal.Decimal),
		TP1LastFillPrices:      make(map[string]decimal.Decimal),
		TP1CommittedQtys:       make(map[string]decimal.Decimal),
		LastAlgoUpdateTimes:    make(map[string]time.Time),
		LastSentSLPrices:       make(map[string]decimal.Decimal),
		ActiveMAE:              make(map[string]float64),
		ActiveMFE:              make(map[string]float64),
		ProtectionTiers:        make(map[string]int),
		ATRAtEntry:             make(map[string]decimal.Decimal),
		PeakPrices:             make(map[string]decimal.Decimal),
		PositionClientOrderIDs: make(map[string]string),
		ClosingReason:          make(map[string]string),
		eventChan:              make(chan LivePositionEvent, 500),
	}
	b.startEventWorker()
	return b
}

func (b *BinanceLiveBroker) startEventWorker() {
	b.eventWg.Add(1)
	go func() {
		defer b.eventWg.Done()
		for evt := range b.eventChan {
			b.dispatchPositionEvent(evt)
		}
	}()
}

func (b *BinanceLiveBroker) dispatchPositionEvent(evt LivePositionEvent) {
	if evt.Type == LivePositionEventSave && b.OnPositionSavedFn != nil {
		b.OnPositionSavedFn(evt.Pos)
	} else if evt.Type == LivePositionEventDelete && b.OnPositionDeletedFn != nil {
		b.OnPositionDeletedFn(evt.Symbol)
	}
}

func (b *BinanceLiveBroker) enqueuePositionEvent(evt LivePositionEvent) {
	b.eventMu.Lock()
	if b.isClosed || b.eventChan == nil {
		b.eventMu.Unlock()
		b.dispatchPositionEvent(evt)
		return
	}
	b.eventChan <- evt
	b.eventMu.Unlock()
}

func (b *BinanceLiveBroker) Close() {
	b.eventMu.Lock()
	if b.isClosed {
		b.eventMu.Unlock()
		return
	}
	b.isClosed = true
	if b.eventChan != nil {
		close(b.eventChan)
	}
	b.eventMu.Unlock()

	b.eventWg.Wait()
}

// RestoreOpenPosition memulihkan posisi live dari database/exchange ke memory saat startup (TD-01 & TD-02)
func (b *BinanceLiveBroker) RestoreOpenPosition(pos *LiveOpenPosition) {
	if pos == nil || pos.Symbol == "" {
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.ActivePositions[pos.Symbol] = pos.Qty
	b.PositionSides[pos.Symbol] = pos.Side
	b.PositionEntryPrices[pos.Symbol] = pos.EntryPrice
	b.PositionOpenTimes[pos.Symbol] = pos.OpenTime
	b.ActiveMAE[pos.Symbol] = pos.MaxMAE
	b.ActiveMFE[pos.Symbol] = pos.MaxMFE
	if pos.ProtectionTier > 0 {
		b.ProtectionTiers[pos.Symbol] = pos.ProtectionTier
	}
	if pos.PeakPrice.GreaterThan(decimal.Zero) {
		b.PeakPrices[pos.Symbol] = pos.PeakPrice
	} else if pos.EntryPrice.GreaterThan(decimal.Zero) {
		b.PeakPrices[pos.Symbol] = pos.EntryPrice
	}
	if pos.InitialQty.GreaterThan(decimal.Zero) {
		b.InitialQtys[pos.Symbol] = pos.InitialQty
	} else {
		b.InitialQtys[pos.Symbol] = pos.Qty
	}
	if pos.InitialSLPrice.GreaterThan(decimal.Zero) {
		b.InitialSLPrices[pos.Symbol] = pos.InitialSLPrice
	} else if pos.SLPrice.GreaterThan(decimal.Zero) {
		b.InitialSLPrices[pos.Symbol] = pos.SLPrice
	}
	if pos.TP1Price.GreaterThan(decimal.Zero) {
		b.TP1Prices[pos.Symbol] = pos.TP1Price
	}
	b.IsTP1Hits[pos.Symbol] = pos.IsTP1Hit
	if pos.ATRAtEntry.GreaterThan(decimal.Zero) {
		b.ATRAtEntry[pos.Symbol] = pos.ATRAtEntry
	}

	slStatus := model.ProtectionAcked
	if pos.SLOrderID == 0 || pos.SLPrice.IsZero() {
		slStatus = model.ProtectionFailed
	}
	tpStatus := model.ProtectionAcked
	if !b.Cfg.PartialTPEnabled && (pos.TPOrderID == 0 || pos.TPPrice.IsZero()) {
		tpStatus = model.ProtectionFailed
	}

	// Verifikasi aktual ke Binance jika adapter tersedia (jangan berasumsi ProtectionAcked membabi buta)
	if b.VerifyAlgoOrderFn != nil && (slStatus == model.ProtectionAcked || tpStatus == model.ProtectionAcked) {
		ctxV, cancelV := context.WithTimeout(context.Background(), 5*time.Second)
		slAlive, tpAlive, err := b.VerifyAlgoOrderFn(ctxV, pos.Symbol, pos.SLOrderID, pos.TPOrderID, pos.SLClientAlgoID, pos.TPClientAlgoID)
		cancelV()
		if err == nil {
			if pos.SLOrderID != 0 && !slAlive {
				slog.Warn("live: stored SL order not alive on Binance; marking ProtectionFailed for repair", "symbol", pos.Symbol, "slOrderID", pos.SLOrderID)
				slStatus = model.ProtectionFailed
				pos.SLOrderID = 0
				pos.SLClientAlgoID = ""
			}
			if pos.TPOrderID != 0 && !tpAlive {
				slog.Warn("live: stored TP order not alive on Binance; marking ProtectionFailed for repair", "symbol", pos.Symbol, "tpOrderID", pos.TPOrderID)
				tpStatus = model.ProtectionFailed
				pos.TPOrderID = 0
				pos.TPClientAlgoID = ""
			}
		}
	}

	b.Protections[pos.Symbol] = &model.LivePositionProtection{
		Symbol:          pos.Symbol,
		Side:            pos.Side,
		PlanGeneration:  1,
		ProtectedQty:    pos.Qty,
		InitialQty:      pos.InitialQty,
		SLOrderID:       pos.SLOrderID,
		SLClientAlgoID:  pos.SLClientAlgoID,
		SLStatus:        slStatus,
		SLPrice:         pos.SLPrice,
		InitialSLPrice:  pos.InitialSLPrice,
		TPOrderID:       pos.TPOrderID,
		TPClientAlgoID:  pos.TPClientAlgoID,
		TPStatus:        tpStatus,
		TPPrice:         pos.TPPrice,
		TP1Price:        pos.TP1Price,
		TP1OrderID:       pos.TP1OrderID,
		TP1ClientOrderID: pos.TP1ClientOrderID,
		IsTP1Hit:        pos.IsTP1Hit,
		LastAttemptTime: pos.UpdatedAt,
		SignalAuditID:   pos.SignalAuditID,
	}

	if pos.SLClientAlgoID != "" {
		b.OrdersByClient[pos.SLClientAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   pos.SLClientAlgoID,
			ExchangeOrderID: pos.SLOrderID,
			Symbol:          pos.Symbol,
			Side:            model.OppositeSide(pos.Side),
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       pos.Qty,
			SLPrice:         pos.SLPrice,
			CreatedAt:       pos.OpenTime,
		}
		if pos.SLOrderID != 0 {
			b.OrdersByExchangeID[pos.SLOrderID] = b.OrdersByClient[pos.SLClientAlgoID]
		}
	}

	if pos.TPClientAlgoID != "" {
		b.OrdersByClient[pos.TPClientAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   pos.TPClientAlgoID,
			ExchangeOrderID: pos.TPOrderID,
			Symbol:          pos.Symbol,
			Side:            model.OppositeSide(pos.Side),
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       pos.Qty,
			TPPrice:         pos.TPPrice,
			CreatedAt:       pos.OpenTime,
		}
		if pos.TPOrderID != 0 {
			b.OrdersByExchangeID[pos.TPOrderID] = b.OrdersByClient[pos.TPClientAlgoID]
		}
	}
}

func (b *BinanceLiveBroker) notifyPositionSavedLocked(symbol string) {
	if b.OnPositionSavedFn == nil {
		return
	}
	qty, ok := b.ActivePositions[symbol]
	if !ok || qty.LessThanOrEqual(decimal.Zero) {
		return
	}
	prot, hasProt := b.Protections[symbol]
	if !hasProt {
		return
	}
	initialQty := b.InitialQtys[symbol]
	if initialQty.IsZero() {
		initialQty = qty
	}
	initialSL := b.InitialSLPrices[symbol]
	if initialSL.IsZero() {
		initialSL = prot.SLPrice
	}
	savedPos := LiveOpenPosition{
		Symbol:         symbol,
		Side:           prot.Side,
		Qty:            qty,
		InitialQty:     initialQty,
		EntryPrice:     b.PositionEntryPrices[symbol],
		SLOrderID:      prot.SLOrderID,
		SLClientAlgoID: prot.SLClientAlgoID,
		SLPrice:        prot.SLPrice,
		InitialSLPrice: initialSL,
		TPOrderID:      prot.TPOrderID,
		TPClientAlgoID: prot.TPClientAlgoID,
		TPPrice:        prot.TPPrice,
		TP1Price:         b.TP1Prices[symbol],
		TP1OrderID:       prot.TP1OrderID,
		TP1ClientOrderID: prot.TP1ClientOrderID,
		IsTP1Hit:         b.IsTP1Hits[symbol],
		ATRAtEntry:     b.ATRAtEntry[symbol],
		ProtectionTier: b.ProtectionTiers[symbol],
		SignalAuditID:  prot.SignalAuditID,
		OpenTime:       b.PositionOpenTimes[symbol],
		MaxMAE:         b.ActiveMAE[symbol],
		MaxMFE:         b.ActiveMFE[symbol],
		PeakPrice:      b.PeakPrices[symbol],
		UpdatedAt:      time.Now().UTC(),
	}
	b.enqueuePositionEvent(LivePositionEvent{
		Type: LivePositionEventSave,
		Pos:  savedPos,
	})
}

func (b *BinanceLiveBroker) notifyPositionDeleted(symbol string) {
	b.enqueuePositionEvent(LivePositionEvent{
		Type:   LivePositionEventDelete,
		Symbol: symbol,
	})
}

// AdoptPendingEntryRecord mengadopsi order entry pending dari bursa saat rekonsiliasi startup
func (b *BinanceLiveBroker) AdoptPendingEntryRecord(rec *model.LiveOrderRecord) {
	if rec == nil || rec.Symbol == "" {
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()

	rec.IsEntry = true
	b.OrdersByClient[rec.ClientOrderID] = rec
	b.PositionClientOrderIDs[rec.Symbol] = rec.ClientOrderID
	if rec.ExchangeOrderID != 0 {
		b.OrdersByExchangeID[rec.ExchangeOrderID] = rec
	}
	slog.Info("live: adopted pending entry order from exchange",
		"symbol", rec.Symbol, "clientOrderID", rec.ClientOrderID, "exchangeOrderID", rec.ExchangeOrderID, "side", rec.Side, "qty", rec.TargetQty)
}

// ReserveEntryIntent melakukan reservasi slot atomik in-memory tanpa I/O jaringan (Fase 1 Solusi P0-1)
func (b *BinanceLiveBroker) ReserveEntryIntent(intent model.OrderIntent) (bool, error) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.CircuitBreakerTripped || b.isDailyDrawdownBreachedLocked() {
		return false, fmt.Errorf("daily circuit breaker tripped")
	}

	if b.LastAccountUpdateTime.IsZero() || time.Since(b.LastAccountUpdateTime) > b.Cfg.MaxAccountStaleness {
		return false, fmt.Errorf("account data is stale (> %v)", b.Cfg.MaxAccountStaleness)
	}

	if _, exists := b.OrdersByClient[intent.ClientOrderID]; exists {
		return false, fmt.Errorf("order %s already exists", intent.ClientOrderID)
	}
	if posQty, hasPos := b.ActivePositions[intent.Symbol]; hasPos && posQty.GreaterThan(decimal.Zero) {
		return false, fmt.Errorf("position already exists for symbol %s", intent.Symbol)
	}

	activeCount := len(b.ActivePositions)
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			if ord.Symbol == intent.Symbol {
				return false, fmt.Errorf("pending entry order already exists for %s", intent.Symbol)
			}
			activeCount++
		}
	}
	if activeCount >= b.Cfg.MaxSlots {
		return false, fmt.Errorf("max slots (%d) reached", b.Cfg.MaxSlots)
	}

	b.OrdersByClient[intent.ClientOrderID] = &model.LiveOrderRecord{
		ClientOrderID:       intent.ClientOrderID,
		Symbol:              intent.Symbol,
		Side:                intent.Side,
		IsEntry:             true,
		IsAlgo:              false,
		Status:              model.StatusPendingNew,
		TargetQty:           intent.Qty,
		CumulativeFilledQty: decimal.Zero,
		SLPrice:             intent.SLPrice,
		InitialSLPrice:      intent.SLPrice,
		TPPrice:             intent.TPPrice,
		TP1Price:            intent.TP1Price,
		ATRAtEntry:          intent.ATRAtEntry,
		SignalAuditID:       intent.SignalAuditID,
		CreatedAt:           time.Now().UTC(),
	}
	b.ATRAtEntry[intent.Symbol] = intent.ATRAtEntry
	b.InitialSLPrices[intent.Symbol] = intent.SLPrice
	b.InitialQtys[intent.Symbol] = intent.Qty
	b.TP1Prices[intent.Symbol] = intent.TP1Price
	b.IsTP1Hits[intent.Symbol] = false
	b.PositionClientOrderIDs[intent.Symbol] = intent.ClientOrderID
	return true, nil
}

// SubmitLimitEntry mengimplementasikan Two-Phase Commit: Reservasi Atomik -> I/O Bursa -> Update/Reconcile (Solusi P0-1)
func (b *BinanceLiveBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) {
	// Fallback On-Demand Refresh: jika data akun belum terisi atau mendekati batas staleness,
	// refresh langsung via REST API sebelum reservasi (Solusi P0-2).
	if b.FetchAccountFn != nil {
		b.Mu.Lock()
		isStale := b.LastAccountUpdateTime.IsZero() || time.Since(b.LastAccountUpdateTime) > (b.Cfg.MaxAccountStaleness/2)
		b.Mu.Unlock()
		if isStale {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.RefreshAccount(ctx)
			cancel()
		}
	}

	reserved, err := b.ReserveEntryIntent(intent)
	if !reserved {
		return false, err
	}

	if b.PlaceEntryFn == nil {
		b.Mu.Lock()
		delete(b.OrdersByClient, intent.ClientOrderID)
		delete(b.ATRAtEntry, intent.Symbol)
		b.Mu.Unlock()
		return false, fmt.Errorf("PlaceEntryFn adapter is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exchangeOrderID, err := b.PlaceEntryFn(ctx, intent)
	if err != nil {
		// Pencegahan Duplikasi: Jika penempatan error/timeout, periksa apakah bursa sebenarnya sudah menerima order
		if b.QueryOrderFn != nil {
			ctxQ, cancelQ := context.WithTimeout(context.Background(), 5*time.Second)
			ordRec, qErr := b.QueryOrderFn(ctxQ, intent.Symbol, intent.ClientOrderID)
			cancelQ()
			if qErr == nil && ordRec != nil && ordRec.ExchangeOrderID != 0 {
				slog.Warn("entry placement returned error but order confirmed alive on Binance; retaining reservation",
					"symbol", intent.Symbol, "clientOrderID", intent.ClientOrderID, "exchangeOrderID", ordRec.ExchangeOrderID, "status", ordRec.Status)
				b.Mu.Lock()
				if rec, exists := b.OrdersByClient[intent.ClientOrderID]; exists {
					rec.Status = ordRec.Status
					rec.ExchangeOrderID = ordRec.ExchangeOrderID
					b.OrdersByExchangeID[ordRec.ExchangeOrderID] = rec
				}
				b.Mu.Unlock()
				return true, nil
			}
		}

		b.Mu.Lock()
		delete(b.OrdersByClient, intent.ClientOrderID)
		delete(b.ATRAtEntry, intent.Symbol)
		b.Mu.Unlock()
		return false, fmt.Errorf("entry placement rejected/failed: %w", err)
	}

	b.Mu.Lock()
	if rec, exists := b.OrdersByClient[intent.ClientOrderID]; exists {
		rec.Status = model.StatusNew
		rec.ExchangeOrderID = exchangeOrderID
		b.OrdersByExchangeID[exchangeOrderID] = rec
	}
	b.Mu.Unlock()

	return true, nil
}

// RefreshAccount mengambil data saldo dompet dan posisi aktif terkini via REST API Binance (FetchAccountFn)
// dan langsung memperbarui state in-memory live broker (Solusi P0-2 untuk mencegah account staleness).
func (b *BinanceLiveBroker) RefreshAccount(ctx context.Context) error {
	if b.FetchAccountFn == nil {
		return nil
	}
	wb, up, ab, pos, err := b.FetchAccountFn(ctx)
	if err != nil {
		return fmt.Errorf("refresh account balance: %w", err)
	}
	b.HandleAccountUpdate(wb, up, ab, pos, time.Now().UTC())
	return nil
}

// HandleAccountUpdate memproses pembaruan akun dari REST atau event ACCOUNT_UPDATE (Solusi P0-2)
func (b *BinanceLiveBroker) HandleAccountUpdate(
	walletBal decimal.Decimal,
	unrealizedPnL decimal.Decimal,
	availBal decimal.Decimal,
	positions map[string]model.LivePosition,
	eventTime time.Time,
) {
	var ordersToCancel []*model.LiveOrderRecord

	b.Mu.Lock()
	b.WalletBalanceUSDT = walletBal
	b.UnrealizedPnLUSDT = unrealizedPnL
	b.AvailableBalanceUSDT = availBal
	b.LastAccountUpdateTime = eventTime

	for sym, pos := range positions {
		if pos.Qty.GreaterThan(decimal.Zero) {
			if _, exists := b.PositionOpenTimes[sym]; !exists {
				b.PositionOpenTimes[sym] = eventTime
			}
			b.PositionSides[sym] = pos.Side
			b.PositionEntryPrices[sym] = pos.EntryPrice
			b.ActivePositions[sym] = pos.Qty
		} else {
			delete(b.ActivePositions, sym)
			delete(b.PositionOpenTimes, sym)
			delete(b.PositionSides, sym)
			delete(b.PositionEntryPrices, sym)
			delete(b.ClosingInFlight, sym)
			delete(b.ActiveMAE, sym)
			delete(b.ActiveMFE, sym)
			delete(b.PositionClientOrderIDs, sym)
			delete(b.ClosingReason, sym)
		}
	}

	effectiveEquity := walletBal.Add(unrealizedPnL)

	epochDay := eventTime.UTC().Unix() / 86400
	if b.SessionStartEpochDay != epochDay || b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		b.SessionStartEpochDay = epochDay
		b.SessionStartEquityUSDT = effectiveEquity
		b.CircuitBreakerTripped = false
	}

	if b.SessionStartEquityUSDT.GreaterThan(decimal.Zero) {
		ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
		ddFloat, _ := ddDec.Float64()
		if ddFloat <= b.Cfg.MaxDailyDrawdownPct {
			b.CircuitBreakerTripped = true
			for _, ord := range b.OrdersByClient {
				if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew) {
					ordersToCancel = append(ordersToCancel, ord)
				}
			}
		}
	}
	b.Mu.Unlock()

	if len(ordersToCancel) > 0 && b.CancelOrderFn != nil {
		for _, ord := range ordersToCancel {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
			cancel()
		}
	}
}

// HandleUserDataOrderUpdate merekonsiliasi event ORDER_TRADE_UPDATE (Solusi P0-3 & P0-6)
func (b *BinanceLiveBroker) HandleUserDataOrderUpdate(
	clientOrderID string,
	exchangeOrderID int64,
	symbol string,
	status model.OrderLifecycleStatus,
	cumFilledQty, avgPrice, realizedProfit decimal.Decimal,
) {
	// Guard Kritis MISS #1: Order Partial TP1 ("ALGO_TP1_..." atau Limit Maker TP1)
	// Order penutupan TP1 reduceOnly tidak boleh masuk ke Skenario 2 (penutupan posisi penuh),
	// karena akan menghapus sisa lot runner dan proteksinya dari memori.
	// Akuntansi parsial, eksekusi Maker/Taker, dan pergeseran SL ke BE dikelola terpusat oleh processTP1Executed.
	isTP1Order := strings.HasPrefix(clientOrderID, "ALGO_TP1_") || strings.HasSuffix(clientOrderID, "_TP1")
	if !isTP1Order && exchangeOrderID != 0 {
		b.Mu.Lock()
		if prot, ok := b.Protections[symbol]; ok && prot.TP1OrderID == exchangeOrderID {
			isTP1Order = true
		}
		b.Mu.Unlock()
	}
	if isTP1Order {
		if status == model.StatusPartiallyFilled && cumFilledQty.GreaterThan(decimal.Zero) {
			b.recordTP1PartialChunk(symbol, clientOrderID, cumFilledQty, avgPrice)
		} else if status == model.StatusFilled {
			b.processTP1Executed(symbol, cumFilledQty, avgPrice, clientOrderID, true)
		} else if status == model.StatusCanceled || status == model.StatusExpired || status == model.StatusRejected {
			var canceledPartialRec *TradeClosedRecord
			b.Mu.Lock()
			if prot, ok := b.Protections[symbol]; ok && (prot.TP1OrderID == exchangeOrderID || prot.TP1ClientOrderID == clientOrderID) {
				prot.TP1OrderID = 0
				prot.TP1ClientOrderID = ""
			}
			if !b.IsTP1Hits[symbol] {
				prevFilled := b.TP1FilledQtys[symbol]
				if cumFilledQty.GreaterThan(prevFilled) {
					delta := cumFilledQty.Sub(prevFilled)
					if curQty, hasPos := b.ActivePositions[symbol]; hasPos {
						rem := curQty.Sub(delta)
						if rem.IsNegative() {
							rem = decimal.Zero
						}
						b.ActivePositions[symbol] = rem
					}
					prevFilled = cumFilledQty
				}
				if prevFilled.GreaterThan(decimal.Zero) {
					exitP := avgPrice
					if exitP.IsZero() {
						exitP = b.TP1LastFillPrices[symbol]
					}
					if exitP.IsZero() {
						exitP = b.TP1Prices[symbol]
					}
					recVal := b.buildTP1TradeRecordLocked(symbol, prevFilled, exitP, clientOrderID, true)
					canceledPartialRec = &recVal
					b.TP1CommittedQtys[symbol] = b.TP1CommittedQtys[symbol].Add(prevFilled)
					delete(b.TP1FilledQtys, symbol)
					delete(b.TP1LastFillPrices, symbol)
					b.notifyPositionSavedLocked(symbol)
				}
			}
			b.Mu.Unlock()
			if canceledPartialRec != nil && b.OnTradeClosedFn != nil {
				go b.OnTradeClosedFn(*canceledPartialRec)
			}
		}
		return
	}

	var plan model.ProtectionPlanAction
	var needPlanExecution bool

	b.Mu.Lock()
	rec, exists := b.OrdersByClient[clientOrderID]
	if !exists && exchangeOrderID != 0 {
		rec, exists = b.OrdersByExchangeID[exchangeOrderID]
	}

	if !exists {
		b.Mu.Unlock()
		if b.QueryOrderFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			queriedRec, err := b.QueryOrderFn(ctx, symbol, clientOrderID)
			cancel()
			if err == nil && queriedRec != nil {
				b.Mu.Lock()
				b.OrdersByClient[queriedRec.ClientOrderID] = queriedRec
				if queriedRec.ExchangeOrderID != 0 {
					b.OrdersByExchangeID[queriedRec.ExchangeOrderID] = queriedRec
				}
				rec = queriedRec
				exists = true
				b.Mu.Unlock()
			}
		}

		b.Mu.Lock()
		if !exists {
			if _, hasPos := b.ActivePositions[symbol]; hasPos && status == model.StatusFilled {
				posSide := b.PositionSides[symbol]
				rec = &model.LiveOrderRecord{
					ClientOrderID:       clientOrderID,
					ExchangeOrderID:     exchangeOrderID,
					Symbol:              symbol,
					Side:                model.OppositeSide(posSide),
					IsEntry:             false,
					Status:              status,
					TargetQty:           cumFilledQty,
					CumulativeFilledQty: cumFilledQty,
					AvgPrice:            avgPrice,
					CreatedAt:           time.Now().UTC(),
				}
				b.OrdersByClient[clientOrderID] = rec
				if exchangeOrderID != 0 {
					b.OrdersByExchangeID[exchangeOrderID] = rec
				}
				exists = true
			}
		}
		if !exists {
			b.Mu.Unlock()
			return
		}
	}

	rec.ExchangeOrderID = exchangeOrderID
	rec.Status = status
	rec.CumulativeFilledQty = cumFilledQty
	b.OrdersByExchangeID[exchangeOrderID] = rec

	// SKENARIO 1: Update Order Entry
	if rec.IsEntry {
		// Jika posisi sudah mencapai TP1 milestone, tolak penambahan fill ke runner
		if b.IsTP1Hits[symbol] {
			slog.Warn("live: received entry fill after TP1 milestone reached; ignoring to protect runner state", "symbol", symbol, "orderID", exchangeOrderID)
			b.Mu.Unlock()
			return
		}

		switch status {
		case model.StatusPartiallyFilled, model.StatusFilled:
			if cumFilledQty.GreaterThan(decimal.Zero) {
				b.ActivePositions[symbol] = cumFilledQty
				b.PositionSides[symbol] = rec.Side
				if avgPrice.GreaterThan(decimal.Zero) {
					b.PositionEntryPrices[symbol] = avgPrice
				}
				if _, hasOpenTime := b.PositionOpenTimes[symbol]; !hasOpenTime {
					b.PositionOpenTimes[symbol] = time.Now().UTC()
				}
				if b.InitialQtys[symbol].IsZero() {
					b.InitialQtys[symbol] = cumFilledQty
				}
				if b.InitialSLPrices[symbol].IsZero() {
					if rec.InitialSLPrice.GreaterThan(decimal.Zero) {
						b.InitialSLPrices[symbol] = rec.InitialSLPrice
					} else {
						b.InitialSLPrices[symbol] = rec.SLPrice
					}
				}
				if b.TP1Prices[symbol].IsZero() {
					b.TP1Prices[symbol] = rec.TP1Price
				}
				if b.ATRAtEntry[symbol].IsZero() {
					b.ATRAtEntry[symbol] = rec.ATRAtEntry
				}
				plan, needPlanExecution = b.planProtectionSyncLocked(rec)
			}
		case model.StatusCanceled, model.StatusExpired:
			if cumFilledQty.GreaterThan(decimal.Zero) {
				b.ActivePositions[symbol] = cumFilledQty
				b.PositionSides[symbol] = rec.Side
				if avgPrice.GreaterThan(decimal.Zero) {
					b.PositionEntryPrices[symbol] = avgPrice
				}
				if _, hasOpenTime := b.PositionOpenTimes[symbol]; !hasOpenTime {
					b.PositionOpenTimes[symbol] = time.Now().UTC()
				}
				if b.InitialQtys[symbol].IsZero() {
					b.InitialQtys[symbol] = cumFilledQty
				}
				if b.InitialSLPrices[symbol].IsZero() {
					if rec.InitialSLPrice.GreaterThan(decimal.Zero) {
						b.InitialSLPrices[symbol] = rec.InitialSLPrice
					} else {
						b.InitialSLPrices[symbol] = rec.SLPrice
					}
				}
				if b.TP1Prices[symbol].IsZero() {
					b.TP1Prices[symbol] = rec.TP1Price
				}
				if b.ATRAtEntry[symbol].IsZero() {
					b.ATRAtEntry[symbol] = rec.ATRAtEntry
				}
				plan, needPlanExecution = b.planProtectionSyncLocked(rec)
			} else {
				delete(b.OrdersByClient, rec.ClientOrderID)
				delete(b.OrdersByExchangeID, exchangeOrderID)
			}
		case model.StatusRejected:
			delete(b.OrdersByClient, rec.ClientOrderID)
			delete(b.OrdersByExchangeID, exchangeOrderID)
		}
		b.Mu.Unlock()

		if needPlanExecution {
			go b.executeProtectionPlan(plan)
		}
		return
	}

	// SKENARIO 2: Update Order Exit (SL atau TP Terisi -> Batalkan Sibling Algo Order - Solusi P0-3)
	var tradeRec *TradeClosedRecord
	var algoOrdersToCancel []int64
	var cancelSymbol string
	var tp1OrderToCancel int64
	var tp1ClientOrderToCancel string
	if !rec.IsEntry && status == model.StatusFilled {
		delete(b.ActivePositions, symbol)
		delete(b.PositionSides, symbol)
		openTime := b.PositionOpenTimes[symbol]
		delete(b.PositionOpenTimes, symbol)
		cancelSymbol = symbol
		if prot, ok := b.Protections[symbol]; ok {
			if prot.TP1OrderID != 0 {
				tp1OrderToCancel = prot.TP1OrderID
				tp1ClientOrderToCancel = prot.TP1ClientOrderID
			}
			var exitReason string
			currentTier := b.ProtectionTiers[symbol]
			if (exchangeOrderID == prot.SLOrderID || clientOrderID == prot.SLClientAlgoID) && prot.TPOrderID != 0 {
				algoOrdersToCancel = append(algoOrdersToCancel, prot.TPOrderID)
				switch currentTier {
				case 3:
					exitReason = "TRAILING_STOP"
				case 2:
					exitReason = "PROFIT_LOCK"
				case 1:
					exitReason = "BREAK_EVEN"
				default:
					exitReason = "STOP_LOSS"
				}
			} else if (exchangeOrderID == prot.TPOrderID || clientOrderID == prot.TPClientAlgoID) && prot.SLOrderID != 0 {
				algoOrdersToCancel = append(algoOrdersToCancel, prot.SLOrderID)
				exitReason = "TAKE_PROFIT"
			} else if exchangeOrderID == prot.SLOrderID || clientOrderID == prot.SLClientAlgoID {
				switch currentTier {
				case 3:
					exitReason = "TRAILING_STOP"
				case 2:
					exitReason = "PROFIT_LOCK"
				case 1:
					exitReason = "BREAK_EVEN"
				default:
					exitReason = "STOP_LOSS"
				}
			} else if exchangeOrderID == prot.TPOrderID || clientOrderID == prot.TPClientAlgoID {
				exitReason = "TAKE_PROFIT"
			} else {
				// Exit bukan dari SL algo dan bukan dari TP algo (misal Time-Stop, Emergency Close, atau Manual Close di Binance)
				if reason, exists := b.ClosingReason[symbol]; exists && reason != "" {
					exitReason = reason
				} else if strings.HasPrefix(clientOrderID, "live_emg_") {
					exitReason = "TIME_STOP_MAX_HOLDING"
				} else {
					exitReason = "MANUAL_CLOSE"
				}
				// BUG 1 FIX: Batalkan seluruh pending algo order yang masih aktif di bursa agar tidak menjadi Ghost Order
				if prot.SLOrderID != 0 {
					algoOrdersToCancel = append(algoOrdersToCancel, prot.SLOrderID)
				}
				if prot.TPOrderID != 0 {
					algoOrdersToCancel = append(algoOrdersToCancel, prot.TPOrderID)
				}
			}
			delete(b.ClosingReason, symbol)

			entryPrice := b.PositionEntryPrices[symbol]
			delete(b.PositionEntryPrices, symbol)
			delete(b.ProtectionTiers, symbol)
			delete(b.ATRAtEntry, symbol)
			delete(b.PeakPrices, symbol)

			exitPrice := avgPrice
			if exitPrice.LessThanOrEqual(decimal.Zero) {
				switch exitReason {
				case "STOP_LOSS", "BREAK_EVEN", "PROFIT_LOCK", "TRAILING_STOP":
					exitPrice = prot.SLPrice
				case "TAKE_PROFIT":
					exitPrice = prot.TPPrice
				default:
					exitPrice = entryPrice
				}
			}

			netPnL := realizedProfit
			if netPnL.IsZero() && entryPrice.GreaterThan(decimal.Zero) && exitPrice.GreaterThan(decimal.Zero) {
				if prot.Side == model.SideLong {
					netPnL = exitPrice.Sub(entryPrice).Mul(rec.TargetQty)
				} else {
					netPnL = entryPrice.Sub(exitPrice).Mul(rec.TargetQty)
				}
			}

			slToLog := b.InitialSLPrices[symbol]
			if slToLog.IsZero() {
				slToLog = prot.SLPrice
			}

			clientOrderIDForRec := rec.ClientOrderID
			if clientOrderIDForRec == "" || strings.HasPrefix(clientOrderIDForRec, "live_emg_") {
				if baseID, exists := b.PositionClientOrderIDs[symbol]; exists && baseID != "" {
					clientOrderIDForRec = baseID
				}
			}
			if clientOrderIDForRec == "" {
				clientOrderIDForRec = rec.ClientOrderID
			}

			tradeRec = &TradeClosedRecord{
				ClientOrderID: clientOrderIDForRec,
				Symbol:        symbol,
				Side:          string(prot.Side),
				Qty:           rec.TargetQty,
				EntryPrice:    entryPrice,
				ExitPrice:     exitPrice,
				SLPrice:       slToLog,
				TPPrice:       prot.TPPrice,
				NetPnL:        netPnL,
				MAE:           b.ActiveMAE[symbol],
				MFE:           b.ActiveMFE[symbol],
				ExitReason:    exitReason,
				OpenedAt:      openTime,
				ClosedAt:      time.Now().UTC(),
				SignalAuditID: prot.SignalAuditID,
			}
			delete(b.PositionClientOrderIDs, symbol)
			delete(b.ActiveMAE, symbol)
			delete(b.ActiveMFE, symbol)
			delete(b.Protections, symbol)
			delete(b.InitialQtys, symbol)
			delete(b.InitialSLPrices, symbol)
			delete(b.TP1Prices, symbol)
			delete(b.IsTP1Hits, symbol)
			delete(b.AlgoUpdateInFlight, symbol)
			delete(b.TP1InFlight, symbol)
			delete(b.TP1FilledQtys, symbol)
			delete(b.TP1LastFillPrices, symbol)
			delete(b.TP1CommittedQtys, symbol)
			delete(b.LastAlgoUpdateTimes, symbol)
			delete(b.LastSentSLPrices, symbol)
		}
		for cID, ord := range b.OrdersByClient {
			if ord.Symbol == symbol {
				if ord.ExchangeOrderID != 0 {
					delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
				}
				delete(b.OrdersByClient, cID)
			}
		}
		delete(b.OrdersByClient, rec.ClientOrderID)
		delete(b.OrdersByExchangeID, exchangeOrderID)
	}
	b.Mu.Unlock()

	if tradeRec != nil {
		b.notifyPositionDeleted(tradeRec.Symbol)
		if b.OnTradeClosedFn != nil {
			go b.OnTradeClosedFn(*tradeRec)
		}
	}

	for _, cancelID := range algoOrdersToCancel {
		if cancelID != 0 && b.CancelAlgoOrderFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelAlgoOrderFn(ctx, cancelSymbol, cancelID)
			cancel()
		}
	}

	if tp1OrderToCancel != 0 && b.CancelOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelOrderFn(ctx, cancelSymbol, tp1OrderToCancel, tp1ClientOrderToCancel)
		cancel()
	}
}

func (b *BinanceLiveBroker) planProtectionSyncLocked(entryRec *model.LiveOrderRecord) (model.ProtectionPlanAction, bool) {
	prot, exists := b.Protections[entryRec.Symbol]
	if exists && prot.SLStatus == model.ProtectionAcked && prot.ProtectedQty.Equal(entryRec.CumulativeFilledQty) {
		return model.ProtectionPlanAction{}, false
	}

	var gen uint64 = 1
	var oldSLID, oldTPID, oldTP1ID int64
	var oldTP1ClientID string
	if exists {
		gen = prot.PlanGeneration + 1
		oldSLID = prot.SLOrderID
		oldTPID = prot.TPOrderID
		oldTP1ID = prot.TP1OrderID
		oldTP1ClientID = prot.TP1ClientOrderID
	}

	now := time.Now().UTC()
	slAlgoClientID := model.FormatClientID("ALGO_SL", entryRec.Symbol, now)
	tpAlgoClientID := model.FormatClientID("ALGO_TP", entryRec.Symbol, now)

	needTP := !b.Cfg.PartialTPEnabled
	action := model.ProtectionPlanAction{
		Symbol:            entryRec.Symbol,
		Generation:        gen,
		CancelSLID:        oldSLID,
		CancelTPID:        oldTPID,
		CancelTP1ID:       oldTP1ID,
		CancelTP1ClientID: oldTP1ClientID,
		NeedNewSL:         true,
		NeedNewTP:         needTP,
		ExitSide:          model.OppositeSide(entryRec.Side),
		TargetQty:         entryRec.CumulativeFilledQty,
		SLPrice:           entryRec.SLPrice,
		TPPrice:           entryRec.TPPrice,
		ClientSLAlgoID:    slAlgoClientID,
		ClientTPAlgoID:    tpAlgoClientID,
	}

	// Persiapan resting Limit Maker TP1 jika PartialTPEnabled dan TP1Price valid
	if b.Cfg.PartialTPEnabled && !b.IsTP1Hits[entryRec.Symbol] && entryRec.TP1Price.GreaterThan(decimal.Zero) {
		ratio := b.Cfg.PartialTP1Ratio
		if ratio <= 0.05 || ratio >= 0.95 {
			ratio = 0.45
		}
		closeQty := entryRec.CumulativeFilledQty.Mul(decimal.NewFromFloat(ratio))
		if b.GetExchangeFilterFn != nil {
			if f, ok := b.GetExchangeFilterFn(entryRec.Symbol); ok && f.StepSize.GreaterThan(decimal.Zero) {
				if qQty, err := model.QuantizeQtyFloor(closeQty, f.StepSize); err == nil {
					closeQty = qQty
				}
			}
		}
		minNotional := decimal.NewFromFloat(5.0)
		if b.GetExchangeFilterFn != nil {
			if f, ok := b.GetExchangeFilterFn(entryRec.Symbol); ok && f.MinNotional.GreaterThan(decimal.Zero) {
				minNotional = f.MinNotional
			}
		}
		splitPossible := closeQty.GreaterThan(decimal.Zero) && closeQty.LessThan(entryRec.CumulativeFilledQty)
		remQty := entryRec.CumulativeFilledQty.Sub(closeQty)
		if splitPossible && closeQty.Mul(entryRec.TP1Price).GreaterThanOrEqual(minNotional) && remQty.Mul(entryRec.TP1Price).GreaterThanOrEqual(minNotional) {
			tp1ClientID := model.FormatClientID("ALGO_TP1", entryRec.Symbol, now)
			action.NeedNewTP1Limit = true
			action.TP1Qty = closeQty
			action.TP1Price = entryRec.TP1Price
			action.ClientTP1ID = tp1ClientID
		}
	}

	slPrice := entryRec.SLPrice
	if slPrice.IsZero() {
		// Fallback safe SL based on 1.5% distance jika order diadopsi tanpa parameter SL eksplisit
		minDist := decimal.NewFromFloat(0.015)
		refPrice := entryRec.LimitPrice
		if refPrice.IsZero() {
			if posPrice, has := b.PositionEntryPrices[entryRec.Symbol]; has {
				refPrice = posPrice
			}
		}
		if refPrice.GreaterThan(decimal.Zero) {
			if entryRec.Side == model.SideLong {
				slPrice = refPrice.Mul(decimal.NewFromInt(1).Sub(minDist))
			} else {
				slPrice = refPrice.Mul(decimal.NewFromInt(1).Add(minDist))
			}
		}
	}
	action.SLPrice = slPrice

	tpStatus := model.ProtectionPending
	if !needTP {
		tpStatus = model.ProtectionAcked
	}

	b.Protections[entryRec.Symbol] = &model.LivePositionProtection{
		Symbol:          entryRec.Symbol,
		Side:            entryRec.Side,
		PlanGeneration:  gen,
		ProtectedQty:    decimal.Zero,
		InitialQty:      entryRec.CumulativeFilledQty,
		SLStatus:        model.ProtectionPending,
		SLClientAlgoID:  slAlgoClientID,
		SLPrice:         slPrice,
		InitialSLPrice:  slPrice,
		TPStatus:        tpStatus,
		TPClientAlgoID:  tpAlgoClientID,
		TPPrice:         entryRec.TPPrice,
		TP1Price:        entryRec.TP1Price,
		IsTP1Hit:        false,
		LastAttemptTime: now,
		SignalAuditID:   entryRec.SignalAuditID,
	}

	return action, true
}

func (b *BinanceLiveBroker) executeProtectionPlan(action model.ProtectionPlanAction) {
	// Kuantisasi harga SL dan TP ke TickSize simbol sebelum ditembakkan ke bursa
	if b.GetExchangeFilterFn != nil {
		if f, ok := b.GetExchangeFilterFn(action.Symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
			roundUpSL := (action.ExitSide == model.SideLong)
			if qSL, err := model.QuantizePrice(action.SLPrice, f.TickSize, roundUpSL); err == nil {
				action.SLPrice = qSL
			}
			if action.TPPrice.GreaterThan(decimal.Zero) {
				roundUpTP := (action.ExitSide == model.SideShort)
				if qTP, err := model.QuantizePrice(action.TPPrice, f.TickSize, roundUpTP); err == nil {
					action.TPPrice = qTP
				}
			}
		}
	}

	// LANGKAH 1: Pasang SL baru TERLEBIH DAHULU (Solusi P0-4 Anti-Naked Position)
	var newSLOrderID int64
	var slErr error

	for attempt := 1; attempt <= 3; attempt++ {
		if b.PlaceAlgoOrderFn != nil && action.NeedNewSL {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newSLOrderID, slErr = b.PlaceAlgoOrderFn(ctx, action.Symbol, action.ExitSide, "STOP_MARKET", action.TargetQty, action.SLPrice, action.ClientSLAlgoID)
			cancel()
			if slErr == nil && newSLOrderID != 0 {
				break
			}
			time.Sleep(time.Duration(attempt*300) * time.Millisecond)
		}
	}

	if slErr != nil || newSLOrderID == 0 {
		b.Mu.Lock()
		if prot, ok := b.Protections[action.Symbol]; ok && prot.PlanGeneration == action.Generation {
			prot.SLStatus = model.ProtectionFailed
			prot.LastError = fmt.Sprintf("SL placement failed after retries: %v", slErr)
		}
		hasOldSL := action.CancelSLID != 0
		b.Mu.Unlock()

		if !hasOldSL && b.EmergencyMarketCloseFn != nil {
			b.Mu.Lock()
			b.ClosingReason[action.Symbol] = "EMERGENCY_CLOSE_SL_PLACEMENT_FAIL"
			b.Mu.Unlock()
			var closeErr error
			positionSide := model.OppositeSide(action.ExitSide)
			for closeAttempt := 1; closeAttempt <= 3; closeAttempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				closeErr = b.EmergencyMarketCloseFn(ctx, action.Symbol, positionSide, action.TargetQty, "CRITICAL: SL placement failed, emergency exit executed")
				cancel()
				if closeErr == nil {
					break
				}
				time.Sleep(time.Duration(closeAttempt*250) * time.Millisecond)
			}
			if closeErr != nil {
				slog.Error("CRITICAL ALERT: EMERGENCY CLOSE FAILED", "symbol", action.Symbol, "positionSide", positionSide, "qty", action.TargetQty, "err", closeErr)
			}
		}
		return
	}

	// LANGKAH 2: Pasang TP baru
	var newTPOrderID int64
	var tpErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if b.PlaceAlgoOrderFn != nil && action.NeedNewTP {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newTPOrderID, tpErr = b.PlaceAlgoOrderFn(ctx, action.Symbol, action.ExitSide, "TAKE_PROFIT_MARKET", action.TargetQty, action.TPPrice, action.ClientTPAlgoID)
			cancel()
			if tpErr == nil && newTPOrderID != 0 {
				break
			}
			time.Sleep(time.Duration(attempt*300) * time.Millisecond)
		}
	}

	// LANGKAH 2b: Pasang TP1 Limit Maker Order (Reduce-Only) jika didukung
	var newTP1OrderID int64
	var tp1Err error
	if action.NeedNewTP1Limit && b.PlaceLimitCloseFn != nil {
		tp1Price := action.TP1Price
		if b.GetExchangeFilterFn != nil {
			if f, ok := b.GetExchangeFilterFn(action.Symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
				roundUpTP := (action.ExitSide == model.SideShort)
				if qTP, err := model.QuantizePrice(tp1Price, f.TickSize, roundUpTP); err == nil {
					tp1Price = qTP
				}
			}
		}
		for attempt := 1; attempt <= 3; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newTP1OrderID, tp1Err = b.PlaceLimitCloseFn(ctx, action.Symbol, action.ExitSide, action.TP1Qty, tp1Price, action.ClientTP1ID)
			cancel()
			if tp1Err == nil && newTP1OrderID != 0 {
				break
			}
			time.Sleep(time.Duration(attempt*250) * time.Millisecond)
		}
		if tp1Err != nil {
			slog.Warn("live: resting TP1 limit placement failed; will rely on realtime tick market exit",
				"symbol", action.Symbol, "err", tp1Err)
		}
	}

	// LANGKAH 3: HANYA SETELAH SL BARU TERKONFIRMASI -> Batalkan SL, TP, dan TP1 lama
	if action.CancelSLID != 0 && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, action.Symbol, action.CancelSLID)
		cancel()
	}
	if action.CancelTPID != 0 && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, action.Symbol, action.CancelTPID)
		cancel()
	}
	if action.CancelTP1ID != 0 && b.CancelOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelOrderFn(ctx, action.Symbol, action.CancelTP1ID, action.CancelTP1ClientID)
		cancel()
	}

	// LANGKAH 4: Update status proteksi di bawah lock dengan validasi generation check
	b.Mu.Lock()
	defer b.Mu.Unlock()

	prot, ok := b.Protections[action.Symbol]
	if !ok || prot.PlanGeneration != action.Generation {
		return
	}

	prot.SLOrderID = newSLOrderID
	prot.SLStatus = model.ProtectionAcked
	prot.ProtectedQty = action.TargetQty

	if action.NeedNewTP {
		if tpErr != nil || newTPOrderID == 0 {
			prot.TPStatus = model.ProtectionFailed
			prot.LastError = fmt.Sprintf("TP placement failed: %v", tpErr)
			prot.LastTPAttempt = time.Now().UTC()
		} else {
			prot.TPOrderID = newTPOrderID
			prot.TPStatus = model.ProtectionAcked
		}
	} else {
		prot.TPStatus = model.ProtectionAcked
	}

	b.OrdersByClient[action.ClientSLAlgoID] = &model.LiveOrderRecord{
		ClientOrderID:   action.ClientSLAlgoID,
		ExchangeOrderID: newSLOrderID,
		Symbol:          action.Symbol,
		Side:            action.ExitSide,
		IsEntry:         false,
		IsAlgo:          true,
		Status:          model.StatusNew,
		TargetQty:       action.TargetQty,
		SLPrice:         action.SLPrice,
		CreatedAt:       time.Now().UTC(),
	}
	b.OrdersByExchangeID[newSLOrderID] = b.OrdersByClient[action.ClientSLAlgoID]

	if newTPOrderID != 0 {
		b.OrdersByClient[action.ClientTPAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   action.ClientTPAlgoID,
			ExchangeOrderID: newTPOrderID,
			Symbol:          action.Symbol,
			Side:            action.ExitSide,
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       action.TargetQty,
			TPPrice:         action.TPPrice,
			CreatedAt:       time.Now().UTC(),
		}
		b.OrdersByExchangeID[newTPOrderID] = b.OrdersByClient[action.ClientTPAlgoID]
	}

	if newTP1OrderID != 0 {
		prot.TP1OrderID = newTP1OrderID
		prot.TP1ClientOrderID = action.ClientTP1ID
		b.OrdersByClient[action.ClientTP1ID] = &model.LiveOrderRecord{
			ClientOrderID:   action.ClientTP1ID,
			ExchangeOrderID: newTP1OrderID,
			Symbol:          action.Symbol,
			Side:            action.ExitSide,
			IsEntry:         false,
			IsAlgo:          false,
			Status:          model.StatusNew,
			TargetQty:       action.TP1Qty,
			LimitPrice:      action.TP1Price,
			CreatedAt:       time.Now().UTC(),
		}
		b.OrdersByExchangeID[newTP1OrderID] = b.OrdersByClient[action.ClientTP1ID]
	} else if action.CancelTP1ID != 0 && prot.TP1OrderID == action.CancelTP1ID {
		prot.TP1OrderID = 0
		prot.TP1ClientOrderID = ""
	}

	b.notifyPositionSavedLocked(action.Symbol)
}

func (b *BinanceLiveBroker) CheckPendingEntryTimeouts(now time.Time) {
	var expiredOrders []*model.LiveOrderRecord

	b.Mu.Lock()
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew) {
			if now.Sub(ord.CreatedAt) > b.Cfg.LimitOrderTimeout {
				expiredOrders = append(expiredOrders, ord)
			}
		}
	}
	b.Mu.Unlock()

	if len(expiredOrders) == 0 || b.CancelOrderFn == nil {
		return
	}

	for _, ord := range expiredOrders {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
		cancel()

		b.Mu.Lock()
		if err == nil || (err != nil && (strings.Contains(err.Error(), "Unknown order") || strings.Contains(err.Error(), "-2011"))) {
			ord.Status = model.StatusExpired
			if ord.CumulativeFilledQty.IsZero() {
				delete(b.OrdersByClient, ord.ClientOrderID)
				delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
			}
		}
		b.Mu.Unlock()
	}
}

// CheckMaxHoldingTimeouts memeriksa dan menutup posisi live yang melebihi batas durasi holding maksimal
func (b *BinanceLiveBroker) CheckMaxHoldingTimeouts(now time.Time) {
	if b.Cfg.MaxHoldingDuration <= 0 || b.EmergencyMarketCloseFn == nil {
		return
	}

	type closeTarget struct {
		symbol       string
		positionSide model.Side
		qty          decimal.Decimal
	}
	var targets []closeTarget

	b.Mu.Lock()
	for sym, openTime := range b.PositionOpenTimes {
		if now.Sub(openTime) > b.Cfg.MaxHoldingDuration {
			if b.ClosingInFlight[sym] {
				continue
			}
			qty, exists := b.ActivePositions[sym]
			if exists && qty.GreaterThan(decimal.Zero) {
				side, hasSide := b.PositionSides[sym]
				if !hasSide || side == "" {
					slog.Warn("cannot determine position side for time-stop, skipping", "symbol", sym, "side", side)
					continue
				}
				b.ClosingInFlight[sym] = true
				b.ClosingReason[sym] = "TIME_STOP_MAX_HOLDING"
				targets = append(targets, closeTarget{
					symbol:       sym,
					positionSide: side,
					qty:          qty,
				})
			}
		}
	}
	b.Mu.Unlock()

	for _, tgt := range targets {
		var closeErr error
		for attempt := 1; attempt <= 3; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			closeErr = b.EmergencyMarketCloseFn(ctx, tgt.symbol, tgt.positionSide, tgt.qty, "TIME_STOP_MAX_HOLDING")
			cancel()
			if closeErr == nil {
				slog.Info("time-stop emergency close executed successfully", "symbol", tgt.symbol, "attempt", attempt)
				break
			}
			slog.Warn("time-stop emergency close attempt failed", "symbol", tgt.symbol, "attempt", attempt, "err", closeErr)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
		}

		// Reset flag in-flight agar bila terjadi partial fill,
		// watchdog berikutnya (Worker 4 tiap 2s) dapat menutup sisa kuantitas secara aman.
		b.Mu.Lock()
		delete(b.ClosingInFlight, tgt.symbol)
		b.Mu.Unlock()

		if closeErr != nil {
			slog.Error("time-stop emergency close failed after 3 attempts", "symbol", tgt.symbol, "err", closeErr)
		}
	}
}

// RepairFailedProtections mencoba memasang ulang Stop Loss dan Take Profit order yang gagal
// Dilengkapi guard cooldown, max retries, non-blocking I/O, auto-close bila SL gagal, dan ghost order guard.
func (b *BinanceLiveBroker) RepairFailedProtections(now time.Time) {
	if b.PlaceAlgoOrderFn == nil {
		return
	}

	// 1. Repair Stop Loss
	type slRepairTarget struct {
		symbol         string
		exitSide       model.Side
		qty            decimal.Decimal
		slPrice        decimal.Decimal
		clientSLAlgoID string
		generation     uint64
		retryCount     int
	}
	var slTargets []slRepairTarget

	b.Mu.Lock()
	for sym, prot := range b.Protections {
		if prot.SLStatus != model.ProtectionFailed {
			continue
		}
		qty, hasPos := b.ActivePositions[sym]
		if !hasPos || !qty.GreaterThan(decimal.Zero) {
			continue
		}
		if b.ClosingInFlight[sym] {
			continue
		}
		// Jika retry sudah mencapai 3x, posisikan ke emergency close agar modal uang riil aman
		if prot.SLRetryCount >= 3 {
			b.ClosingInFlight[sym] = true
			b.ClosingReason[sym] = "EMERGENCY_CLOSE_SL_REPAIR_FAIL"
			posSide := b.PositionSides[sym]
			go func(s string, q decimal.Decimal, pSide model.Side) {
				defer func() {
					b.Mu.Lock()
					delete(b.ClosingInFlight, s)
					b.Mu.Unlock()
				}()
				if b.EmergencyMarketCloseFn != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = b.EmergencyMarketCloseFn(ctx, s, pSide, q, "SL repair exceeded max retries; emergency close")
					cancel()
				}
			}(sym, qty, posSide)
			continue
		}
		if prot.SLRetryCount > 0 && !prot.LastAttemptTime.IsZero() && now.Sub(prot.LastAttemptTime) < 5*time.Second {
			continue
		}
		side, hasSide := b.PositionSides[sym]
		if !hasSide {
			continue
		}
		exitSide := model.OppositeSide(side)
		if exitSide == "" {
			continue
		}
		if prot.SLPrice.LessThanOrEqual(decimal.Zero) {
			continue
		}

		prot.SLRetryCount++
		prot.LastAttemptTime = now

		clientAlgoID := model.FormatClientID("ALGOSL_R", sym, now)
		slTargets = append(slTargets, slRepairTarget{
			symbol:         sym,
			exitSide:       exitSide,
			qty:            qty,
			slPrice:        prot.SLPrice,
			clientSLAlgoID: clientAlgoID,
			generation:     prot.PlanGeneration,
			retryCount:     prot.SLRetryCount,
		})
	}
	b.Mu.Unlock()

	for _, tgt := range slTargets {
		slPrice := tgt.slPrice
		if b.GetExchangeFilterFn != nil {
			if f, ok := b.GetExchangeFilterFn(tgt.symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
				roundUp := (tgt.exitSide == model.SideLong)
				if qSL, err := model.QuantizePrice(slPrice, f.TickSize, roundUp); err == nil {
					slPrice = qSL
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		newSLID, err := b.PlaceAlgoOrderFn(
			ctx,
			tgt.symbol,
			tgt.exitSide,
			"STOP_MARKET",
			tgt.qty,
			slPrice,
			tgt.clientSLAlgoID,
		)
		cancel()

		b.Mu.Lock()
		posQty, stillActive := b.ActivePositions[tgt.symbol]
		prot, hasProt := b.Protections[tgt.symbol]

		if err != nil || newSLID == 0 {
			if hasProt && prot.PlanGeneration == tgt.generation {
				prot.LastError = fmt.Sprintf("SL repair attempt %d failed: %v", tgt.retryCount, err)
			}
			slog.Warn("SL repair placement failed", "symbol", tgt.symbol, "attempt", tgt.retryCount, "err", err)
			b.Mu.Unlock()
			continue
		}

		if !stillActive || !posQty.GreaterThan(decimal.Zero) || !hasProt || prot.PlanGeneration != tgt.generation {
			slog.Warn("position closed while SL repair order in flight; canceling orphan SL", "symbol", tgt.symbol, "slOrderID", newSLID)
			b.Mu.Unlock()
			if b.CancelAlgoOrderFn != nil {
				cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = b.CancelAlgoOrderFn(cCtx, tgt.symbol, newSLID)
				cCancel()
			}
			continue
		}

		prot.SLOrderID = newSLID
		prot.SLClientAlgoID = tgt.clientSLAlgoID
		prot.SLStatus = model.ProtectionAcked
		prot.LastError = ""

		b.OrdersByClient[tgt.clientSLAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   tgt.clientSLAlgoID,
			ExchangeOrderID: newSLID,
			Symbol:          tgt.symbol,
			Side:            tgt.exitSide,
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       tgt.qty,
			SLPrice:         tgt.slPrice,
			CreatedAt:       now,
		}
		b.OrdersByExchangeID[newSLID] = b.OrdersByClient[tgt.clientSLAlgoID]
		b.notifyPositionSavedLocked(tgt.symbol)
		b.Mu.Unlock()

		slog.Info("SL repair successfully placed stop loss order", "symbol", tgt.symbol, "slOrderID", newSLID, "price", tgt.slPrice)
	}

	// 2. Repair Take Profit
	type tpRepairTarget struct {
		symbol         string
		exitSide       model.Side
		qty            decimal.Decimal
		tpPrice        decimal.Decimal
		clientTPAlgoID string
		generation     uint64
	}
	var targets []tpRepairTarget

	b.Mu.Lock()
	for sym, prot := range b.Protections {
		if b.Cfg.PartialTPEnabled {
			continue
		}
		if prot.TPStatus != model.ProtectionFailed {
			continue
		}
		qty, hasPos := b.ActivePositions[sym]
		if !hasPos || !qty.GreaterThan(decimal.Zero) {
			continue
		}
		if b.TPRepairInFlight[sym] {
			continue
		}
		if prot.TPRetryCount >= 5 {
			continue
		}
		if !prot.LastTPAttempt.IsZero() && now.Sub(prot.LastTPAttempt) < 10*time.Second {
			continue
		}
		side, hasSide := b.PositionSides[sym]
		if !hasSide {
			continue
		}
		exitSide := model.OppositeSide(side)
		if exitSide == "" {
			continue
		}
		if prot.TPPrice.LessThanOrEqual(decimal.Zero) {
			continue
		}

		b.TPRepairInFlight[sym] = true
		prot.TPRetryCount++
		prot.LastTPAttempt = now

		clientAlgoID := model.FormatClientID("ALGOTP_R", sym, now)
		targets = append(targets, tpRepairTarget{
			symbol:         sym,
			exitSide:       exitSide,
			qty:            qty,
			tpPrice:        prot.TPPrice,
			clientTPAlgoID: clientAlgoID,
			generation:     prot.PlanGeneration,
		})
	}
	b.Mu.Unlock()

	for _, tgt := range targets {
		tpPrice := tgt.tpPrice
		if b.GetExchangeFilterFn != nil {
			if f, ok := b.GetExchangeFilterFn(tgt.symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
				roundUp := (tgt.exitSide == model.SideShort)
				if qTP, err := model.QuantizePrice(tpPrice, f.TickSize, roundUp); err == nil {
					tpPrice = qTP
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		newTPID, err := b.PlaceAlgoOrderFn(
			ctx,
			tgt.symbol,
			tgt.exitSide,
			"TAKE_PROFIT_MARKET",
			tgt.qty,
			tpPrice,
			tgt.clientTPAlgoID,
		)
		cancel()

		b.Mu.Lock()
		delete(b.TPRepairInFlight, tgt.symbol)

		posQty, stillActive := b.ActivePositions[tgt.symbol]
		prot, hasProt := b.Protections[tgt.symbol]

		if err != nil || newTPID == 0 {
			if hasProt && prot.PlanGeneration == tgt.generation {
				prot.LastError = fmt.Sprintf("TP repair attempt %d failed: %v", prot.TPRetryCount, err)
			}
			slog.Warn("TP repair placement failed", "symbol", tgt.symbol, "err", err)
			b.Mu.Unlock()
			continue
		}

		// Ghost Order Guard: jika posisi sudah tertutup saat order terbang, batalkan TP baru seketika
		if !stillActive || !posQty.GreaterThan(decimal.Zero) || !hasProt || prot.PlanGeneration != tgt.generation {
			slog.Warn("position closed while TP repair order in flight; canceling orphan TP", "symbol", tgt.symbol, "tpOrderID", newTPID)
			b.Mu.Unlock()
			if b.CancelAlgoOrderFn != nil {
				cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = b.CancelAlgoOrderFn(cCtx, tgt.symbol, newTPID)
				cCancel()
			}
			continue
		}

		// Sukses: update TPStatus menjadi ProtectionAcked
		prot.TPOrderID = newTPID
		prot.TPStatus = model.ProtectionAcked
		prot.LastError = ""

		b.OrdersByClient[tgt.clientTPAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   tgt.clientTPAlgoID,
			ExchangeOrderID: newTPID,
			Symbol:          tgt.symbol,
			Side:            tgt.exitSide,
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       tgt.qty,
			TPPrice:         tgt.tpPrice,
			CreatedAt:       now,
		}
		b.OrdersByExchangeID[newTPID] = b.OrdersByClient[tgt.clientTPAlgoID]
		b.notifyPositionSavedLocked(tgt.symbol)
		b.Mu.Unlock()

		slog.Info("TP repair successfully restored take-profit order", "symbol", tgt.symbol, "tpOrderID", newTPID, "price", tgt.tpPrice)
	}
}

func (b *BinanceLiveBroker) isDailyDrawdownBreachedLocked() bool {
	if b.CircuitBreakerTripped {
		return true
	}
	if b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		return false
	}
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
	ddFloat, _ := ddDec.Float64()
	if ddFloat <= b.Cfg.MaxDailyDrawdownPct {
		b.CircuitBreakerTripped = true
		return true
	}
	return false
}

// recordTP1PartialChunk mencatat fill parsial berulang dari resting Limit Maker TP1
// agar ActivePositions dan delta qty tetap sinkron sebelum StatusFilled final tiba.
func (b *BinanceLiveBroker) recordTP1PartialChunk(symbol, clientOrderID string, cumFilledQty, avgPrice decimal.Decimal) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.IsTP1Hits[symbol] {
		return
	}
	prevFilled := b.TP1FilledQtys[symbol]
	if cumFilledQty.LessThanOrEqual(prevFilled) {
		return
	}

	delta := cumFilledQty.Sub(prevFilled)
	curQty, hasPos := b.ActivePositions[symbol]
	if !hasPos || curQty.LessThanOrEqual(decimal.Zero) {
		return
	}

	if delta.GreaterThan(curQty) {
		delta = curQty
	}
	remQty := curQty.Sub(delta)
	if remQty.IsNegative() {
		remQty = decimal.Zero
	}

	b.TP1FilledQtys[symbol] = prevFilled.Add(delta)
	if avgPrice.GreaterThan(decimal.Zero) {
		b.TP1LastFillPrices[symbol] = avgPrice
	}
	b.ActivePositions[symbol] = remQty

	if prot, ok := b.Protections[symbol]; ok && prot != nil {
		prot.ProtectedQty = remQty
	}
	if clientOrderID != "" {
		if ord, exists := b.OrdersByClient[clientOrderID]; exists {
			ord.Status = model.StatusPartiallyFilled
			ord.CumulativeFilledQty = b.TP1FilledQtys[symbol]
			if avgPrice.GreaterThan(decimal.Zero) {
				ord.AvgPrice = avgPrice
			}
		}
	}
	b.notifyPositionSavedLocked(symbol)
}

// buildTP1TradeRecordLocked menyusun TradeClosedRecord untuk leg TP1 dengan memperhitungkan fee Maker/Taker.
// Pemanggil wajib memegang b.Mu.
func (b *BinanceLiveBroker) buildTP1TradeRecordLocked(
	symbol string,
	closedQty, exitPrice decimal.Decimal,
	clientOrderID string,
	isMaker bool,
) TradeClosedRecord {
	side := b.PositionSides[symbol]
	ePrice := b.PositionEntryPrices[symbol]
	prot, ok := b.Protections[symbol]

	var grossPnL decimal.Decimal
	if side == model.SideLong {
		grossPnL = exitPrice.Sub(ePrice).Mul(closedQty)
	} else {
		grossPnL = ePrice.Sub(exitPrice).Mul(closedQty)
	}

	feeRate := b.Cfg.TakerFeeRate
	if isMaker {
		feeRate = b.Cfg.MakerFeeRate
	}
	if feeRate <= 0 {
		if isMaker {
			feeRate = 0.0002
		} else {
			feeRate = 0.0005
		}
	}
	takerRate := b.Cfg.TakerFeeRate
	if takerRate <= 0 {
		takerRate = 0.0005
	}
	exitFee := exitPrice.Mul(closedQty).Mul(decimal.NewFromFloat(feeRate))
	entryFee := ePrice.Mul(closedQty).Mul(decimal.NewFromFloat(takerRate))
	netPnL := grossPnL.Sub(entryFee).Sub(exitFee)

	initialSL := b.InitialSLPrices[symbol]
	if initialSL.IsZero() && ok {
		initialSL = prot.SLPrice
	}
	sigID := int64(0)
	if ok {
		sigID = prot.SignalAuditID
	}

	return TradeClosedRecord{
		ClientOrderID: clientOrderID,
		Symbol:        symbol,
		Side:          string(side),
		Qty:           closedQty,
		EntryPrice:    ePrice,
		ExitPrice:     exitPrice,
		SLPrice:       initialSL,
		TPPrice:       b.TP1Prices[symbol],
		NetPnL:        netPnL,
		MAE:           b.ActiveMAE[symbol],
		MFE:           b.ActiveMFE[symbol],
		ExitReason:    "PARTIAL_TAKE_PROFIT",
		OpenedAt:      b.PositionOpenTimes[symbol],
		ClosedAt:      time.Now().UTC(),
		SignalAuditID: sigID,
	}
}

// processTP1Executed memproses eksekusi TP1 (baik dari Limit Maker WebSocket maupun Fallback Market Taker),
// memperbarui lot runner secara deterministik di bawah lock, memindahkan SL ke Break-Even secara asinkron, dan mencatat TradeClosedRecord.
func (b *BinanceLiveBroker) processTP1Executed(
	symbol string,
	actualCloseQty, actualExitPrice decimal.Decimal,
	clientOrderID string,
	isMaker bool,
) {
	b.Mu.Lock()
	if b.IsTP1Hits[symbol] {
		b.Mu.Unlock()
		return
	}
	b.IsTP1Hits[symbol] = true
	delete(b.TP1InFlight, symbol)

	prevFilled := b.TP1FilledQtys[symbol]
	prevPrice := b.TP1LastFillPrices[symbol]
	delete(b.TP1FilledQtys, symbol)
	delete(b.TP1LastFillPrices, symbol)
	delete(b.TP1CommittedQtys, symbol)

	curQty, hasPos := b.ActivePositions[symbol]
	if !hasPos || curQty.LessThanOrEqual(decimal.Zero) {
		b.Mu.Unlock()
		return
	}

	ratio := b.Cfg.PartialTP1Ratio
	if ratio <= 0.05 || ratio >= 0.95 {
		ratio = 0.50
	}
	if actualCloseQty.IsZero() {
		actualCloseQty = curQty.Mul(decimal.NewFromFloat(ratio))
	}

	// Untuk Limit Maker WS update, actualCloseQty adalah kumulatif order; kurangi chunk yang sudah dipotong sebelumnya.
	deductQty := actualCloseQty
	if isMaker && prevFilled.GreaterThan(decimal.Zero) {
		deductQty = actualCloseQty.Sub(prevFilled)
		if deductQty.IsNegative() {
			deductQty = decimal.Zero
		}
	}
	if deductQty.GreaterThan(curQty) {
		deductQty = curQty
		if isMaker {
			actualCloseQty = prevFilled.Add(deductQty)
		} else {
			actualCloseQty = deductQty
		}
	}

	if actualExitPrice.IsZero() {
		if !prevPrice.IsZero() {
			actualExitPrice = prevPrice
		} else {
			actualExitPrice = b.TP1Prices[symbol]
		}
	}

	remQty := curQty.Sub(deductQty)
	if remQty.LessThanOrEqual(decimal.Zero) {
		delete(b.ActivePositions, symbol)
		delete(b.PositionSides, symbol)
		delete(b.PositionOpenTimes, symbol)
		delete(b.PositionEntryPrices, symbol)
		delete(b.Protections, symbol)
		b.notifyPositionDeleted(symbol)
		b.Mu.Unlock()
		return
	}
	b.ActivePositions[symbol] = remQty

	side := b.PositionSides[symbol]
	ePrice := b.PositionEntryPrices[symbol]
	prot, ok := b.Protections[symbol]

	// Pindahkan SL ke Break-Even via Unified Protection Engine
	var tickSize decimal.Decimal
	if b.GetExchangeFilterFn != nil {
		if f, ok := b.GetExchangeFilterFn(symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
			tickSize = f.TickSize
		}
	}
	var currentSL decimal.Decimal
	if ok {
		currentSL = prot.SLPrice
	}
	snap := protection.PositionSnapshot{
		Symbol:         symbol,
		Side:           side,
		EntryPrice:     ePrice,
		CurrentSLPrice: currentSL,
		InitialSLPrice: b.InitialSLPrices[symbol],
		ATRAtEntry:     b.ATRAtEntry[symbol],
		TickSize:       tickSize,
	}
	beSL := protection.CalculateBreakEvenSL(snap, b.toProtectionConfig())

	var oldSLID int64
	var oldClientSLID string
	var gen uint64 = 1
	clientSLID := model.FormatClientID("live_sl_be", symbol, time.Now().UTC())
	if ok {
		oldSLID = prot.SLOrderID
		oldClientSLID = prot.SLClientAlgoID
		prot.ProtectedQty = remQty
		prot.PlanGeneration++
		gen = prot.PlanGeneration
		prot.TP1OrderID = 0
		prot.TP1ClientOrderID = ""
	}

	// Update record order jika ada
	if clientOrderID != "" {
		if ord, exists := b.OrdersByClient[clientOrderID]; exists {
			ord.Status = model.StatusFilled
			ord.CumulativeFilledQty = actualCloseQty
			ord.AvgPrice = actualExitPrice
		}
	}

	tradeRec := b.buildTP1TradeRecordLocked(symbol, actualCloseQty, actualExitPrice, clientOrderID, isMaker)
	b.notifyPositionSavedLocked(symbol)
	b.Mu.Unlock()

	if b.OnTradeClosedFn != nil {
		go b.OnTradeClosedFn(tradeRec)
	}

	// Pasang SL baru untuk remaining lot di Break-Even (3-attempt retry loop asinkron agar tidak memblokir read-loop WS)
	if b.PlaceAlgoOrderFn != nil {
		exitSide := model.OppositeSide(side)
		go func(sym string, eSide model.Side, remainQty, targetSL decimal.Decimal, cSLID string, oSLID int64, oClientSLID string, generation uint64, closedQ decimal.Decimal, maker bool) {
			var newSLID int64
			var slErr error
			for attempt := 1; attempt <= 3; attempt++ {
				ctxSL, cancelSL := context.WithTimeout(context.Background(), 5*time.Second)
				newSLID, slErr = b.PlaceAlgoOrderFn(ctxSL, sym, eSide, "STOP_MARKET", remainQty, targetSL, cSLID)
				cancelSL()
				if slErr == nil && newSLID != 0 {
					break
				}
				time.Sleep(time.Duration(attempt*250) * time.Millisecond)
			}
			if slErr == nil && newSLID != 0 {
				b.Mu.Lock()
				if p, ok := b.Protections[sym]; ok && p.PlanGeneration == generation {
					p.SLOrderID = newSLID
					p.SLPrice = targetSL
					p.SLClientAlgoID = cSLID
					b.LastSentSLPrices[sym] = targetSL
					b.ProtectionTiers[sym] = 1
					b.OrdersByClient[cSLID] = &model.LiveOrderRecord{
						ClientOrderID:   cSLID,
						ExchangeOrderID: newSLID,
						Symbol:          sym,
						Side:            eSide,
						TargetQty:       remainQty,
						SLPrice:         targetSL,
						Status:          model.StatusNew,
						IsEntry:         false,
						IsAlgo:          true,
						CreatedAt:       time.Now().UTC(),
					}
					b.OrdersByExchangeID[newSLID] = b.OrdersByClient[cSLID]
					if oSLID != 0 && oSLID != newSLID {
						delete(b.OrdersByExchangeID, oSLID)
					}
					if oClientSLID != "" && oClientSLID != cSLID {
						delete(b.OrdersByClient, oClientSLID)
					}
					b.notifyPositionSavedLocked(sym)
				}
				b.Mu.Unlock()
				if oSLID != 0 && b.CancelAlgoOrderFn != nil {
					ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
					_ = b.CancelAlgoOrderFn(ctxC, sym, oSLID)
					cancelC()
				}
				slog.Info("live: Partial TP1 executed and SL moved to BE",
					"symbol", sym, "closedQty", closedQ, "remainingQty", remainQty, "beSL", targetSL, "isMaker", maker)
			} else {
				slog.Error("live: failed to place BE SL after TP1", "symbol", sym, "err", slErr)
			}
		}(symbol, exitSide, remQty, beSL, clientSLID, oldSLID, oldClientSLID, gen, actualCloseQty, isMaker)
	}
}

func (b *BinanceLiveBroker) OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {
	if markPrice <= 0 {
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()

	qty, hasPos := b.ActivePositions[symbol]
	if !hasPos || qty.LessThanOrEqual(decimal.Zero) {
		return
	}
	entryPrice, hasEntry := b.PositionEntryPrices[symbol]
	if !hasEntry || entryPrice.LessThanOrEqual(decimal.Zero) {
		return
	}
	side := b.PositionSides[symbol]
	entryF, _ := entryPrice.Float64()
	if entryF <= 0 {
		return
	}

	if side == model.SideLong {
		if markPrice > entryF {
			favPct := ((markPrice - entryF) / entryF) * 100.0
			if favPct > b.ActiveMFE[symbol] {
				b.ActiveMFE[symbol] = favPct
			}
		}
		if markPrice < entryF {
			advPct := ((entryF - markPrice) / entryF) * 100.0
			if advPct > b.ActiveMAE[symbol] {
				b.ActiveMAE[symbol] = advPct
			}
		}
	} else {
		if markPrice < entryF {
			favPct := ((entryF - markPrice) / entryF) * 100.0
			if favPct > b.ActiveMFE[symbol] {
				b.ActiveMFE[symbol] = favPct
			}
		}
		if markPrice > entryF {
			advPct := ((markPrice - entryF) / entryF) * 100.0
			if advPct > b.ActiveMAE[symbol] {
				b.ActiveMAE[symbol] = advPct
			}
		}
	}

	// Partial Take Profit (Scale-Out Phase 2 untuk Live Broker)
	tp1Price := b.TP1Prices[symbol]
	if b.Cfg.PartialTPEnabled && !b.IsTP1Hits[symbol] && !tp1Price.IsZero() && !b.TP1InFlight[symbol] {
		markPriceDec := decimal.NewFromFloat(markPrice)
		isTP1Hit := false
		if side == model.SideLong {
			isTP1Hit = markPriceDec.GreaterThanOrEqual(tp1Price)
		} else {
			isTP1Hit = markPriceDec.LessThanOrEqual(tp1Price)
		}
		if isTP1Hit {
			prot, hasProt := b.Protections[symbol]
			hasRestingTP1 := hasProt && prot != nil && prot.TP1OrderID != 0

			// Jika resting Limit Maker TP1 sudah aktif di bursa, biarkan bursa mencocokkan via Maker.
			// Event fill akan masuk via HandleUserDataOrderUpdate.
			// Hanya fallback ke Market Taker jika tidak ada resting limit order,
			// ATAU harga telah melampaui TP1 sejauh >= 0.5 ATR (maker order tertahan/terlewat).
			atrVal := b.ATRAtEntry[symbol]
			needMarketFallback := !hasRestingTP1
			if hasRestingTP1 && atrVal.GreaterThan(decimal.Zero) {
				threshold := atrVal.Mul(decimal.NewFromFloat(0.5))
				if side == model.SideLong && markPriceDec.Sub(tp1Price).GreaterThan(threshold) {
					needMarketFallback = true
				} else if side == model.SideShort && tp1Price.Sub(markPriceDec).GreaterThan(threshold) {
					needMarketFallback = true
				}
			}

			if !needMarketFallback {
				goto skipTP1Handling
			}

			ratio := b.Cfg.PartialTP1Ratio
			if ratio <= 0.05 || ratio >= 0.95 {
				ratio = 0.50
			}
			initialQty := b.InitialQtys[symbol]
			if initialQty.IsZero() {
				initialQty = qty
			}
			closeQty := initialQty.Mul(decimal.NewFromFloat(ratio))
			if alreadyTP1 := b.TP1FilledQtys[symbol].Add(b.TP1CommittedQtys[symbol]); alreadyTP1.GreaterThan(decimal.Zero) {
				closeQty = closeQty.Sub(alreadyTP1)
			}
			if b.GetExchangeFilterFn != nil {
				if f, ok := b.GetExchangeFilterFn(symbol); ok && f.StepSize.GreaterThan(decimal.Zero) {
					if qQty, err := model.QuantizeQtyFloor(closeQty, f.StepSize); err == nil {
						closeQty = qQty
					}
				}
			}
			splitPossible := closeQty.GreaterThan(decimal.Zero) && closeQty.LessThan(qty)
			if splitPossible {
				// [H-04] Validasi MinNotional dan MinQty agar order split tidak di-reject bursa
				minNotional := decimal.NewFromFloat(5.0)
				if b.GetExchangeFilterFn != nil {
					if f, ok := b.GetExchangeFilterFn(symbol); ok {
						if f.MinNotional.GreaterThan(decimal.Zero) {
							minNotional = f.MinNotional
						}
						if f.MinQty.GreaterThan(decimal.Zero) && (closeQty.LessThan(f.MinQty) || qty.Sub(closeQty).LessThan(f.MinQty)) {
							splitPossible = false
						}
					}
				}
				remQtyCheck := qty.Sub(closeQty)
				if closeQty.Mul(tp1Price).LessThan(minNotional) || remQtyCheck.Mul(tp1Price).LessThan(minNotional) {
					splitPossible = false
				}
			}

			if !splitPossible {
				b.IsTP1Hits[symbol] = true
				if hasRestingTP1 && b.CancelOrderFn != nil {
					ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
					_ = b.CancelOrderFn(ctxC, symbol, prot.TP1OrderID, prot.TP1ClientOrderID)
					cancelC()
					prot.TP1OrderID = 0
					prot.TP1ClientOrderID = ""
				}
				slog.Info("live: position notional too small for partial TP1 split (< 5 USDT); moving SL to BE and letting 100% position run as runner",
					"symbol", symbol, "qty", qty, "tp1Price", tp1Price)

				// Pindahkan SL ke Break-Even (Tier 1) untuk 100% posisi via Unified Protection Engine
				var tickSize decimal.Decimal
				if b.GetExchangeFilterFn != nil {
					if f, ok := b.GetExchangeFilterFn(symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
						tickSize = f.TickSize
					}
				}
				var currentSL decimal.Decimal
				if hasProt {
					currentSL = prot.SLPrice
				}
				snap := protection.PositionSnapshot{
					Symbol:         symbol,
					Side:           side,
					EntryPrice:     entryPrice,
					CurrentSLPrice: currentSL,
					InitialSLPrice: b.InitialSLPrices[symbol],
					ATRAtEntry:     b.ATRAtEntry[symbol],
					TickSize:       tickSize,
				}
				beSL := protection.CalculateBreakEvenSL(snap, b.toProtectionConfig())

				var oldSLID int64
				var oldClientSLID string
				var gen uint64 = 1
				clientSLID := model.FormatClientID("live_sl_be", symbol, time.Now().UTC())
				if hasProt {
					oldSLID = prot.SLOrderID
					oldClientSLID = prot.SLClientAlgoID
					prot.PlanGeneration++
					gen = prot.PlanGeneration
				}

				b.notifyPositionSavedLocked(symbol)

				if b.PlaceAlgoOrderFn != nil {
					exitSide := model.OppositeSide(side)
					go func(sym string, eSide model.Side, remQty, targetSL decimal.Decimal, cSLID string, oSLID int64, oClientSLID string, generation uint64) {
						var newSLID int64
						var slErr error
						for attempt := 1; attempt <= 3; attempt++ {
							ctxSL, cancelSL := context.WithTimeout(context.Background(), 5*time.Second)
							newSLID, slErr = b.PlaceAlgoOrderFn(ctxSL, sym, eSide, "STOP_MARKET", remQty, targetSL, cSLID)
							cancelSL()
							if slErr == nil && newSLID != 0 {
								break
							}
							time.Sleep(time.Duration(attempt*250) * time.Millisecond)
						}
						if slErr == nil && newSLID != 0 {
							b.Mu.Lock()
							if p, ok := b.Protections[sym]; ok && p.PlanGeneration == generation {
								p.SLOrderID = newSLID
								p.SLPrice = targetSL
								p.SLClientAlgoID = cSLID
								b.LastSentSLPrices[sym] = targetSL
								b.ProtectionTiers[sym] = 1
								b.OrdersByClient[cSLID] = &model.LiveOrderRecord{
									ClientOrderID:   cSLID,
									ExchangeOrderID: newSLID,
									Symbol:          sym,
									Side:            eSide,
									TargetQty:       remQty,
									SLPrice:         targetSL,
									Status:          model.StatusNew,
									IsEntry:         false,
									IsAlgo:          true,
									CreatedAt:       time.Now().UTC(),
								}
								b.OrdersByExchangeID[newSLID] = b.OrdersByClient[cSLID]
								if oSLID != 0 && oSLID != newSLID {
									delete(b.OrdersByExchangeID, oSLID)
								}
								if oClientSLID != "" && oClientSLID != cSLID {
									delete(b.OrdersByClient, oClientSLID)
								}
								b.notifyPositionSavedLocked(sym)
							}
							b.Mu.Unlock()
							if oSLID != 0 && b.CancelAlgoOrderFn != nil {
								ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
								_ = b.CancelAlgoOrderFn(ctxC, sym, oSLID)
								cancelC()
							}
						}
					}(symbol, exitSide, qty, beSL, clientSLID, oldSLID, oldClientSLID, gen)
				}
				return
			}

			// Batalkan antrean pending entry yang masih tersisa agar tidak terisi setelah TP1 milestone
			for cID, ord := range b.OrdersByClient {
				if ord.Symbol == symbol && ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
					slog.Info("live: canceling remaining pending entry order upon TP1 hit", "symbol", symbol, "orderID", ord.ExchangeOrderID)
					if b.CancelOrderFn != nil {
						ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
						_ = b.CancelOrderFn(ctxC, symbol, ord.ExchangeOrderID, ord.ClientOrderID)
						cancelC()
					}
					delete(b.OrdersByClient, cID)
					if ord.ExchangeOrderID != 0 {
						delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
					}
				}
			}

			// Batalkan resting TP1 limit jika ada sebelum eksekusi market fallback
			var cancelTP1ID int64
			var cancelTP1ClientID string
			if hasRestingTP1 {
				cancelTP1ID = prot.TP1OrderID
				cancelTP1ClientID = prot.TP1ClientOrderID
				prot.TP1OrderID = 0
				prot.TP1ClientOrderID = ""
			}

			b.TP1InFlight[symbol] = true
			exitSide := model.OppositeSide(side)

			var tp1ClientOrderID string
			if baseID, ok := b.PositionClientOrderIDs[symbol]; ok && baseID != "" {
				if len(baseID) > 32 {
					tp1ClientOrderID = fmt.Sprintf("%s_TP1", baseID[:32])
				} else {
					tp1ClientOrderID = fmt.Sprintf("%s_TP1", baseID)
				}
			} else {
				tp1ClientOrderID = model.FormatClientID("ALGO_TP1", symbol, time.Now().UTC())
			}

			go func(sym string, eSide model.Side, cQty decimal.Decimal, cID string, cTP1ID int64, cTP1CID string) {
				defer func() {
					b.Mu.Lock()
					b.TP1InFlight[sym] = false
					b.Mu.Unlock()
				}()

				// 1. Batalkan resting limit order terdahulu jika ada
				if cTP1ID != 0 && b.CancelOrderFn != nil {
					ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
					_ = b.CancelOrderFn(ctxC, sym, cTP1ID, cTP1CID)
					cancelC()
				}

				if b.MarketCloseFn == nil {
					return
				}

				var orderID int64
				var fillPrice decimal.Decimal
				var execQty decimal.Decimal
				var err error
				for attempt := 1; attempt <= 3; attempt++ {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					orderID, fillPrice, execQty, err = b.MarketCloseFn(ctx, sym, eSide, cQty, cID)
					cancel()
					if err == nil && orderID != 0 {
						break
					}
					time.Sleep(time.Duration(attempt*250) * time.Millisecond)
				}

				if err == nil && orderID != 0 {
					actualCloseQty := execQty
					actualExitPrice := fillPrice

					// Verifikasi status eksekusi aktual dari bursa jika fill data belum terisi lengkap
					if (actualCloseQty.IsZero() || actualExitPrice.IsZero()) && b.QueryOrderFn != nil {
						for qAttempt := 1; qAttempt <= 3; qAttempt++ {
							ctxQ, cancelQ := context.WithTimeout(context.Background(), 5*time.Second)
							qRec, qErr := b.QueryOrderFn(ctxQ, sym, cID)
							cancelQ()
							if qErr == nil && qRec != nil {
								if qRec.Status == model.StatusCanceled || qRec.Status == model.StatusExpired || qRec.Status == model.StatusRejected {
									slog.Error("live: TP1 market close was canceled/expired/rejected without fill", "symbol", sym, "status", qRec.Status)
									return
								}
								if qRec.CumulativeFilledQty.GreaterThan(decimal.Zero) {
									actualCloseQty = qRec.CumulativeFilledQty
								}
								if qRec.AvgPrice.GreaterThan(decimal.Zero) {
									actualExitPrice = qRec.AvgPrice
								} else if qRec.LimitPrice.GreaterThan(decimal.Zero) {
									actualExitPrice = qRec.LimitPrice
								}
								if qRec.Status == model.StatusFilled {
									break
								}
							}
							time.Sleep(time.Duration(qAttempt*200) * time.Millisecond)
						}
					}

					// Eksekusi akuntansi dan pergeseran SL terpusat
					b.processTP1Executed(sym, actualCloseQty, actualExitPrice, cID, false /* isMaker */)
				} else {
					slog.Error("live: failed to execute partial TP1 fallback market close", "symbol", sym, "err", err)
				}
			}(symbol, exitSide, closeQty, tp1ClientOrderID, cancelTP1ID, cancelTP1ClientID)
			return
		}
	}
skipTP1Handling:

	if b.TP1InFlight[symbol] {
		return
	}

	// Proteksi Dinamis Tier 1, Tier 2, & Tier 3 Trailing Stop untuk Live Trading
	atr := b.ATRAtEntry[symbol]
	if atr.GreaterThan(decimal.Zero) {
		markPriceDec := decimal.NewFromFloat(markPrice)
		prot, hasProt := b.Protections[symbol]
		currentTier := b.ProtectionTiers[symbol]

		peakPrice, hasPeak := b.PeakPrices[symbol]
		if !hasPeak || peakPrice.IsZero() {
			peakPrice = entryPrice
		}
		if side == model.SideLong {
			if markPriceDec.GreaterThan(peakPrice) {
				peakPrice = markPriceDec
				b.PeakPrices[symbol] = peakPrice
			}
		} else {
			if markPriceDec.LessThan(peakPrice) {
				peakPrice = markPriceDec
				b.PeakPrices[symbol] = peakPrice
			}
		}

		if hasProt && prot.SLStatus == model.ProtectionAcked && b.PlaceAlgoOrderFn != nil {
			var tickSize decimal.Decimal
			if b.GetExchangeFilterFn != nil {
				if f, ok := b.GetExchangeFilterFn(symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
					tickSize = f.TickSize
				}
			}

			snap := protection.PositionSnapshot{
				Symbol:         symbol,
				Side:           side,
				EntryPrice:     entryPrice,
				CurrentSLPrice: prot.SLPrice,
				PeakPrice:      peakPrice,
				ATRAtEntry:     atr,
				ProtectionTier: protection.ProtectionTier(currentTier),
				IsTP1Hit:       b.IsTP1Hits[symbol],
				TickSize:       tickSize,
			}
			protDec := protection.EvaluatePositionProtection(snap, markPriceDec, b.toProtectionConfig())

			var newTargetTier int
			var newSL decimal.Decimal
			if protDec.ShouldUpdate {
				newTargetTier = int(protDec.NewTier)
				newSL = protDec.NewSLPrice
			}

			if newTargetTier > 0 && !newSL.IsZero() {

				// Throttling & Debounce Guard untuk update order SL
				if b.AlgoUpdateInFlight[symbol] {
					return
				}

				nowUTC := time.Now().UTC()
				lastUpdate := b.LastAlgoUpdateTimes[symbol]
				lastSentSL := b.LastSentSLPrices[symbol]

				// Jika masih tier yang sama (misal tier 3 trailing update berulang), batasi jeda minimal 3 detik DAN pergeseran harga >= 0.05 ATR
				if newTargetTier == currentTier {
					if nowUTC.Sub(lastUpdate) < 3*time.Second {
						return
					}
					minMove := atr.Mul(decimal.NewFromFloat(0.05))
					if tickSize.GreaterThan(decimal.Zero) {
						twoTicks := tickSize.Mul(decimal.NewFromInt(2))
						if twoTicks.GreaterThan(minMove) {
							minMove = twoTicks
						}
					}
					if !lastSentSL.IsZero() && newSL.Sub(lastSentSL).Abs().LessThan(minMove) {
						return
					}
				}

				oldSLID := prot.SLOrderID
				oldClientSLID := prot.SLClientAlgoID
				prot.PlanGeneration++
				gen := prot.PlanGeneration
				exitSide := model.OppositeSide(side)
				clientSLID := model.FormatClientID("live_sl", symbol, nowUTC)

				b.AlgoUpdateInFlight[symbol] = true
				b.LastAlgoUpdateTimes[symbol] = nowUTC

				targetQty := prot.ProtectedQty
				if targetQty.IsZero() {
					targetQty = qty
				}

				go func(sym string, eSide model.Side, targetQty, targetSL decimal.Decimal, cAlgoID string, oldID int64, oldClientSLID string, generation uint64, tier int) {
					defer func() {
						b.Mu.Lock()
						b.AlgoUpdateInFlight[sym] = false
						b.Mu.Unlock()
					}()

					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					newID, err := b.PlaceAlgoOrderFn(ctx, sym, eSide, "STOP_MARKET", targetQty, targetSL, cAlgoID)
					cancel()
					if err == nil && newID != 0 {
						b.Mu.Lock()
						if p, ok := b.Protections[sym]; ok && p.PlanGeneration == generation {
							p.SLOrderID = newID
							p.SLPrice = targetSL
							p.SLClientAlgoID = cAlgoID
							b.LastSentSLPrices[sym] = targetSL
							b.ProtectionTiers[sym] = tier
							b.OrdersByClient[cAlgoID] = &model.LiveOrderRecord{
								ClientOrderID:   cAlgoID,
								ExchangeOrderID: newID,
								Symbol:          sym,
								Side:            eSide,
								TargetQty:       targetQty,
								SLPrice:         targetSL,
								Status:          model.StatusNew,
								IsEntry:         false,
								IsAlgo:          true,
								CreatedAt:       time.Now().UTC(),
							}
							b.OrdersByExchangeID[newID] = b.OrdersByClient[cAlgoID]
							if oldID != 0 && oldID != newID {
								delete(b.OrdersByExchangeID, oldID)
							}
							if oldClientSLID != "" && oldClientSLID != cAlgoID {
								delete(b.OrdersByClient, oldClientSLID)
							}
							b.notifyPositionSavedLocked(sym)
						}
						b.Mu.Unlock()

						if oldID != 0 && b.CancelAlgoOrderFn != nil {
							cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
							_ = b.CancelAlgoOrderFn(cCtx, sym, oldID)
							cCancel()
						}
						slog.Info("live: dynamic protection tier activated",
							"symbol", sym, "tier", tier, "newSL", targetSL, "algoID", newID)
					} else {
						slog.Error("live: failed to place dynamic protection SL",
							"symbol", sym, "tier", tier, "targetSL", targetSL, "err", err)
					}
				}(symbol, exitSide, targetQty, newSL, clientSLID, oldSLID, oldClientSLID, gen, newTargetTier)
			}
		}
	}
}

func (b *BinanceLiveBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
}

func (b *BinanceLiveBroker) GetAvailableBalanceUSDT() decimal.Decimal {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return b.AvailableBalanceUSDT
}

func (b *BinanceLiveBroker) GetDailyTotalDrawdownPct() float64 {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		return 0.0
	}
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
	f, _ := ddDec.Float64()
	return f
}

func (b *BinanceLiveBroker) IsCircuitBreakerTripped() bool {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return b.isDailyDrawdownBreachedLocked()
}

func (b *BinanceLiveBroker) GetActiveSlotCount() int {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	activeSymbols := make(map[string]struct{})
	for sym, posQty := range b.ActivePositions {
		if posQty.GreaterThan(decimal.Zero) {
			activeSymbols[sym] = struct{}{}
		}
	}
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			activeSymbols[ord.Symbol] = struct{}{}
		}
	}
	return len(activeSymbols)
}

func (b *BinanceLiveBroker) HasActiveOrderOrPosition(symbol string) bool {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if posQty, has := b.ActivePositions[symbol]; has && posQty.GreaterThan(decimal.Zero) {
		return true
	}
	for _, ord := range b.OrdersByClient {
		if ord.Symbol == symbol && ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			return true
		}
	}
	return false
}

func (b *BinanceLiveBroker) GetMonitoredSymbols() []string {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	syms := make([]string, 0, len(b.ActivePositions)+len(b.OrdersByClient))
	for sym, posQty := range b.ActivePositions {
		if posQty.GreaterThan(decimal.Zero) && !slices.Contains(syms, sym) {
			syms = append(syms, sym)
		}
	}
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			if !slices.Contains(syms, ord.Symbol) {
				syms = append(syms, ord.Symbol)
			}
		}
	}
	return syms
}

func (b *BinanceLiveBroker) CheckTimeouts(now time.Time) {
	b.CheckPendingEntryTimeouts(now)
	b.CheckMaxHoldingTimeouts(now)
	b.RepairFailedProtections(now)
}

func (b *BinanceLiveBroker) GetActivePositions() []ActivePositionDTO {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	now := time.Now().UTC()
	result := make([]ActivePositionDTO, 0, len(b.ActivePositions))
	for sym, qty := range b.ActivePositions {
		if qty.LessThanOrEqual(decimal.Zero) {
			continue
		}
		side := b.PositionSides[sym]
		entryPrice := b.PositionEntryPrices[sym]
		openTime := b.PositionOpenTimes[sym]
		entryF, _ := entryPrice.Float64()

		var slF, tpF float64
		var isBE bool
		if prot, ok := b.Protections[sym]; ok {
			slF, _ = prot.SLPrice.Float64()
			tpF, _ = prot.TPPrice.Float64()
			if side == model.SideLong && slF >= entryF && slF > 0 {
				isBE = true
			} else if side == model.SideShort && slF <= entryF && slF > 0 {
				isBE = true
			}
		}

		result = append(result, ActivePositionDTO{
			Symbol:         sym,
			Side:           string(side),
			Qty:            qty.String(),
			EntryPrice:     entryF,
			SLPrice:        slF,
			TPPrice:        tpF,
			OpenTimeUTC:    openTime.Format(time.RFC3339),
			DurationSec:    int64(now.Sub(openTime).Seconds()),
			IsBreakEven:    isBE,
			ProtectionTier: b.ProtectionTiers[sym],
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].OpenTimeUTC != result[j].OpenTimeUTC {
			return result[i].OpenTimeUTC > result[j].OpenTimeUTC
		}
		return result[i].Symbol < result[j].Symbol
	})
	return result
}

func (b *BinanceLiveBroker) toProtectionConfig() protection.ProtectionConfig {
	t1Long := b.Cfg.Tier1BETriggerLongATR
	if t1Long <= 0 {
		t1Long = b.Cfg.BreakEvenTriggerATR
	}
	t1Short := b.Cfg.Tier1BETriggerShortATR
	if t1Short <= 0 {
		t1Short = b.Cfg.BreakEvenTriggerATR
	}
	return protection.ProtectionConfig{
		Tier1BEEnabled:           b.Cfg.Tier1BEEnabled,
		Tier1BETriggerLongATR:    t1Long,
		Tier1BETriggerShortATR:   t1Short,
		Tier2ProfitLockEnabled:   b.Cfg.Tier2ProfitLockEnabled,
		Tier2ProfitLockATR:       b.Cfg.Tier2ProfitLockATR,
		Tier2ProfitLockOffsetATR: b.Cfg.Tier2ProfitLockOffsetATR,
		TrailingEnabled:          b.Cfg.TrailingEnabled,
		TrailingActivateATR:      b.Cfg.TrailingActivateATR,
		TrailingDistanceATR:      b.Cfg.TrailingDistanceATR,
		PartialTPEnabled:         b.Cfg.PartialTPEnabled,
		MinBreakEvenOffsetPct:    b.Cfg.MinBreakEvenOffsetPct,
		RunnerBECushionATR:       b.Cfg.RunnerBECushionATR,
		TakerFeeRate:             decimal.NewFromFloat(0.0005),
		SlippageRate:             decimal.NewFromFloat(0.0002),
	}
}
