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
	AccountLeverage          float64
	MaxAccountStaleness      time.Duration
	LimitOrderTimeout        time.Duration
	MaxHoldingDuration       time.Duration
	StaleLossExitDuration    time.Duration
	MaxMarkStaleness         time.Duration
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
	EntryOrderType           string
	MaxEntryRepegAttempts    int
	MaxEntryRepegDriftPct    float64
}

type LiveOpenPosition struct {
	Symbol         string
	Side           model.Side
	StrategyID     string
	Qty            decimal.Decimal
	InitialQty     decimal.Decimal
	ProtectedQty   decimal.Decimal
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

type AlgoOrderVerifyResult struct {
	SLAlive         bool
	SLQty           decimal.Decimal
	SLClosePosition bool
	TPAlive         bool
	TPQty           decimal.Decimal
	TPClosePosition bool
}

type BinanceLiveBroker struct {
	Mu                     sync.Mutex
	Cfg                    LiveBrokerConfig
	SessionStartEpochDay   int64
	SessionStartEquityUSDT decimal.Decimal
	WalletBalanceUSDT       decimal.Decimal
	UnrealizedPnLUSDT       decimal.Decimal
	AvailableBalanceUSDT    decimal.Decimal
	PeakEffectiveEquityUSDT decimal.Decimal
	LastAccountUpdateTime   time.Time
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

	PositionClientOrderIDs map[string]string          // symbol -> clientOrderID of original entry
	PositionStrategyIDs    map[string]string          // symbol -> strategyID of original entry
	ClosingReason          map[string]string          // symbol -> reason for closing (e.g. TIME_STOP_MAX_HOLDING)
	PositionUnrealizedPnLs map[string]decimal.Decimal // symbol -> latest unrealized PnL per symbol
	LastMarkPrices         map[string]decimal.Decimal // symbol -> latest mark price from OnRealtimeTick
	LastMarkUpdatedAt      map[string]time.Time       // symbol -> timestamp of latest mark price tick
	EntryFeesUSDT          map[string]decimal.Decimal // symbol -> accumulated entry commissions in USD
	EntryWasMaker          map[string]bool            // symbol -> true if entry filled as maker
	ExitRealizedProfits    map[string]decimal.Decimal // symbol -> accumulated partial exit realizedProfit
	ExitFeesUSDT           map[string]decimal.Decimal // symbol -> accumulated partial exit commissions in USD
	TP1FeesUSDT            map[string]decimal.Decimal // symbol -> accumulated partial TP1 commissions in USD
	ReservedEntryMargins   map[string]decimal.Decimal // clientOrderID -> locally reserved initial margin pending REST sync
	LastAlgoFailTimes      map[string]time.Time       // symbol -> last dynamic/BE SL placement failure timestamp
	LastClosingAttempt     map[string]time.Time       // symbol -> last successful EmergencyMarketClose dispatch timestamp
	TP1FallbackAttempts    map[string]int             // symbol -> count of TP1 market fallback attempts
	LastTP1FallbackTime    map[string]time.Time       // symbol -> last TP1 market fallback attempt timestamp
	LastAccountPosPrevQty  map[string]decimal.Decimal // symbol -> pre-reduction position qty observed in ACCOUNT_UPDATE
	lastAsyncRefresh       time.Time                  // debounce timestamp for background RefreshAccount

	PlaceEntryFn             func(ctx context.Context, intent model.OrderIntent) (exchangeOrderID int64, err error)
	PlaceMarketEntryFn       func(ctx context.Context, intent model.OrderIntent) (exchangeOrderID int64, err error)
	PlaceAlgoOrderFn         func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (algoID int64, err error)
	MarketCloseFn            func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (orderID int64, avgPrice, executedQty decimal.Decimal, err error)
	PlaceLimitCloseFn        func(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (orderID int64, actualClientOrderID string, err error)
	CancelOrderFn            func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error
	CancelOrderWithResultFn  func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) (*model.LiveOrderRecord, error)
	CancelAlgoOrderFn        func(ctx context.Context, symbol string, algoID int64) error
	VerifyAlgoOrderFn        func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (slAlive, tpAlive bool, err error)
	VerifyAlgoOrderDetailsFn func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (AlgoOrderVerifyResult, error)
	ConfirmPositionZeroFn    func(ctx context.Context, symbol string) (isZero bool, err error)
	EmergencyMarketCloseFn   func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error
	QueryOrderFn           func(ctx context.Context, symbol string, clientOrderID string) (*model.LiveOrderRecord, error)
	GetExchangeFilterFn    func(symbol string) (model.ExchangeFilter, bool)
	GetBestQuoteFn         func(symbol string) (bestBid, bestAsk float64, ok bool)
	OnTradeClosedFn        func(rec TradeClosedRecord)
	OnCooldownRecordedFn   func(symbol string, closedAt time.Time, exitReason string)
	OnPositionSavedFn      func(pos LiveOpenPosition)
	OnPositionDeletedFn    func(symbol string)
	FetchAccountFn         func(ctx context.Context) (walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, err error)

	eventQueue   []LivePositionEvent
	eventCond    *sync.Cond
	eventWg      sync.WaitGroup
	asyncWg      sync.WaitGroup
	eventMu      sync.Mutex
	acceptClosed bool
	eventClosed  bool
}

func (b *BinanceLiveBroker) goAsync(fn func()) {
	b.eventMu.Lock()
	if b.acceptClosed {
		b.eventMu.Unlock()
		return
	}
	b.asyncWg.Add(1)
	b.eventMu.Unlock()
	go func() {
		defer b.asyncWg.Done()
		fn()
	}()
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
	if cfg.MaxSlots <= 0 {
		cfg.MaxSlots = 3
	}
	if cfg.MaxDailyDrawdownPct == 0 {
		cfg.MaxDailyDrawdownPct = -0.05
	}
	if cfg.AccountLeverage <= 0 {
		cfg.AccountLeverage = 8.0
	}
	if cfg.MaxAccountStaleness <= 0 {
		cfg.MaxAccountStaleness = 60 * time.Second
	}
	if cfg.LimitOrderTimeout <= 0 {
		cfg.LimitOrderTimeout = defaultLimitOrderTimeout
	}
	if cfg.MaxHoldingDuration <= 0 {
		cfg.MaxHoldingDuration = 4 * time.Hour
	}
	if cfg.MaxMarkStaleness <= 0 {
		cfg.MaxMarkStaleness = 15 * time.Second
	}
	if cfg.MinBreakEvenOffsetPct <= 0 {
		cfg.MinBreakEvenOffsetPct = 0.0010
	}
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
		PositionStrategyIDs:    make(map[string]string),
		ClosingReason:          make(map[string]string),
		PositionUnrealizedPnLs: make(map[string]decimal.Decimal),
		LastMarkPrices:         make(map[string]decimal.Decimal),
		LastMarkUpdatedAt:      make(map[string]time.Time),
		EntryFeesUSDT:          make(map[string]decimal.Decimal),
		EntryWasMaker:          make(map[string]bool),
		ExitRealizedProfits:    make(map[string]decimal.Decimal),
		ExitFeesUSDT:           make(map[string]decimal.Decimal),
		TP1FeesUSDT:            make(map[string]decimal.Decimal),
		ReservedEntryMargins:   make(map[string]decimal.Decimal),
		LastAlgoFailTimes:      make(map[string]time.Time),
		LastClosingAttempt:     make(map[string]time.Time),
		TP1FallbackAttempts:    make(map[string]int),
		LastTP1FallbackTime:    make(map[string]time.Time),
		LastAccountPosPrevQty:  make(map[string]decimal.Decimal),
	}
	b.eventCond = sync.NewCond(&b.eventMu)
	b.startEventWorker()
	return b
}

func (b *BinanceLiveBroker) startEventWorker() {
	b.eventWg.Add(1)
	go func() {
		defer b.eventWg.Done()
		for {
			b.eventMu.Lock()
			for len(b.eventQueue) == 0 && !b.eventClosed {
				b.eventCond.Wait()
			}
			if len(b.eventQueue) == 0 && b.eventClosed {
				b.eventMu.Unlock()
				return
			}
			evt := b.eventQueue[0]
			b.eventQueue[0] = LivePositionEvent{}
			b.eventQueue = b.eventQueue[1:]
			if len(b.eventQueue) == 0 {
				b.eventQueue = nil
			}
			b.eventMu.Unlock()
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
	if b.eventClosed {
		b.eventMu.Unlock()
		return
	}
	if b.eventCond == nil {
		b.eventMu.Unlock()
		b.dispatchPositionEvent(evt)
		return
	}
	b.eventQueue = append(b.eventQueue, evt)
	b.eventCond.Signal()
	b.eventMu.Unlock()
}

func (b *BinanceLiveBroker) Close() {
	b.eventMu.Lock()
	if b.acceptClosed {
		b.eventMu.Unlock()
		return
	}
	b.acceptClosed = true
	b.eventMu.Unlock()

	// Tunggu seluruh goroutine asinkron selesai men-enqueue Save/Delete terakhir secara FIFO sebelum menutup worker.
	b.asyncWg.Wait()

	b.eventMu.Lock()
	b.eventClosed = true
	if b.eventCond != nil {
		b.eventCond.Broadcast()
	}
	b.eventMu.Unlock()

	b.eventWg.Wait()
}

// ClearSymbolState menghapus state in-memory sebuah simbol secara aman di bawah lock.
func (b *BinanceLiveBroker) ClearSymbolState(symbol string) {
	if symbol == "" {
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.clearSymbolStateLocked(symbol)
}

// RestoreOpenPosition memulihkan posisi live dari database/exchange ke memory saat startup (TD-01 & TD-02)
func (b *BinanceLiveBroker) RestoreOpenPosition(pos *LiveOpenPosition) {
	if pos == nil || pos.Symbol == "" {
		return
	}

	b.Mu.Lock()
	partialTPEnabled := b.Cfg.PartialTPEnabled
	trailingEnabled := b.Cfg.TrailingEnabled
	verifyFn := b.VerifyAlgoOrderFn
	verifyDetailsFn := b.VerifyAlgoOrderDetailsFn
	b.Mu.Unlock()

	slStatus := model.ProtectionAcked
	if pos.SLOrderID == 0 || pos.SLPrice.IsZero() {
		slStatus = model.ProtectionFailed
	}
	tpStatus := model.ProtectionAcked
	if (!partialTPEnabled || !trailingEnabled) && (pos.TPOrderID == 0 || pos.TPPrice.IsZero()) {
		tpStatus = model.ProtectionFailed
	}

	protectedQty := pos.Qty
	if pos.ProtectedQty.GreaterThan(decimal.Zero) && pos.ProtectedQty.LessThan(pos.Qty) {
		protectedQty = pos.ProtectedQty
	}

	slOrderID := pos.SLOrderID
	slClientAlgoID := pos.SLClientAlgoID
	tpOrderID := pos.TPOrderID
	tpClientAlgoID := pos.TPClientAlgoID

	// Verifikasi aktual ke Binance DI LUAR b.Mu agar lock tidak pernah tertahan blocking network I/O 5 detik.
	if verifyDetailsFn != nil && (slStatus == model.ProtectionAcked || tpStatus == model.ProtectionAcked) {
		ctxV, cancelV := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := verifyDetailsFn(ctxV, pos.Symbol, slOrderID, tpOrderID, slClientAlgoID, tpClientAlgoID)
		cancelV()
		if err == nil {
			if slOrderID != 0 {
				if !res.SLAlive {
					slog.Warn("live: stored SL order not alive on Binance; marking ProtectionFailed for repair", "symbol", pos.Symbol, "slOrderID", slOrderID)
					slStatus = model.ProtectionFailed
					slOrderID = 0
					slClientAlgoID = ""
					protectedQty = decimal.Zero
				} else if !res.SLClosePosition && (res.SLQty.LessThanOrEqual(decimal.Zero) || res.SLQty.LessThan(pos.Qty)) {
					slog.Warn("live: stored SL order quantity is smaller than live position quantity; marking ProtectionFailed to upgrade SL",
						"symbol", pos.Symbol, "slOrderID", slOrderID, "slQty", res.SLQty, "posQty", pos.Qty)
					slStatus = model.ProtectionFailed
					protectedQty = res.SLQty
				} else {
					protectedQty = pos.Qty
				}
			}
			if tpOrderID != 0 {
				if !res.TPAlive {
					slog.Warn("live: stored TP order not alive on Binance; marking ProtectionFailed for repair", "symbol", pos.Symbol, "tpOrderID", tpOrderID)
					tpStatus = model.ProtectionFailed
					tpOrderID = 0
					tpClientAlgoID = ""
				} else if !res.TPClosePosition && (res.TPQty.LessThanOrEqual(decimal.Zero) || res.TPQty.LessThan(pos.Qty)) {
					slog.Warn("live: stored TP order quantity is smaller than live position quantity; marking ProtectionFailed to upgrade TP",
						"symbol", pos.Symbol, "tpOrderID", tpOrderID, "tpQty", res.TPQty, "posQty", pos.Qty)
					tpStatus = model.ProtectionFailed
				}
			}
		} else if pos.ProtectedQty.GreaterThan(decimal.Zero) && pos.ProtectedQty.LessThan(pos.Qty) {
			slog.Warn("live: failed to verify stored SL order details and position qty grew since snapshot; marking ProtectionFailed to upgrade SL",
				"symbol", pos.Symbol, "slOrderID", slOrderID, "storedQty", pos.ProtectedQty, "posQty", pos.Qty, "err", err)
			slStatus = model.ProtectionFailed
			protectedQty = pos.ProtectedQty
		}
	} else if verifyFn != nil && (slStatus == model.ProtectionAcked || tpStatus == model.ProtectionAcked) {
		ctxV, cancelV := context.WithTimeout(context.Background(), 5*time.Second)
		slAlive, tpAlive, err := verifyFn(ctxV, pos.Symbol, slOrderID, tpOrderID, slClientAlgoID, tpClientAlgoID)
		cancelV()
		if err == nil {
			if slOrderID != 0 && !slAlive {
				slog.Warn("live: stored SL order not alive on Binance; marking ProtectionFailed for repair", "symbol", pos.Symbol, "slOrderID", slOrderID)
				slStatus = model.ProtectionFailed
				slOrderID = 0
				slClientAlgoID = ""
				protectedQty = decimal.Zero
			}
			if tpOrderID != 0 && !tpAlive {
				slog.Warn("live: stored TP order not alive on Binance; marking ProtectionFailed for repair", "symbol", pos.Symbol, "tpOrderID", tpOrderID)
				tpStatus = model.ProtectionFailed
				tpOrderID = 0
				tpClientAlgoID = ""
			}
		} else if pos.ProtectedQty.GreaterThan(decimal.Zero) && pos.ProtectedQty.LessThan(pos.Qty) {
			slStatus = model.ProtectionFailed
			protectedQty = pos.ProtectedQty
		}
	} else if pos.ProtectedQty.GreaterThan(decimal.Zero) && pos.ProtectedQty.LessThan(pos.Qty) {
		slStatus = model.ProtectionFailed
		protectedQty = pos.ProtectedQty
	}

	if slStatus == model.ProtectionFailed && slOrderID == 0 {
		protectedQty = decimal.Zero
	}

	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.ActivePositions[pos.Symbol] = pos.Qty
	b.PositionSides[pos.Symbol] = pos.Side
	b.PositionStrategyIDs[pos.Symbol] = normalizeBrokerStrategyID(pos.StrategyID)
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

	b.Protections[pos.Symbol] = &model.LivePositionProtection{
		Symbol:           pos.Symbol,
		Side:             pos.Side,
		PlanGeneration:   1,
		ProtectedQty:     protectedQty,
		InitialQty:       pos.InitialQty,
		SLOrderID:        slOrderID,
		SLClientAlgoID:   slClientAlgoID,
		SLStatus:         slStatus,
		SLPrice:          pos.SLPrice,
		InitialSLPrice:   pos.InitialSLPrice,
		TPOrderID:        tpOrderID,
		TPClientAlgoID:   tpClientAlgoID,
		TPStatus:         tpStatus,
		TPPrice:          pos.TPPrice,
		TP1Price:         pos.TP1Price,
		TP1OrderID:       pos.TP1OrderID,
		TP1ClientOrderID: pos.TP1ClientOrderID,
		IsTP1Hit:         pos.IsTP1Hit,
		LastAttemptTime:  pos.UpdatedAt,
		SignalAuditID:    pos.SignalAuditID,
	}

	if slClientAlgoID != "" {
		b.OrdersByClient[slClientAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   slClientAlgoID,
			ExchangeOrderID: slOrderID,
			Symbol:          pos.Symbol,
			Side:            model.OppositeSide(pos.Side),
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       pos.Qty,
			SLPrice:         pos.SLPrice,
			CreatedAt:       pos.OpenTime,
		}
		if slOrderID != 0 {
			b.OrdersByExchangeID[slOrderID] = b.OrdersByClient[slClientAlgoID]
		}
	}

	if tpClientAlgoID != "" {
		b.OrdersByClient[tpClientAlgoID] = &model.LiveOrderRecord{
			ClientOrderID:   tpClientAlgoID,
			ExchangeOrderID: tpOrderID,
			Symbol:          pos.Symbol,
			Side:            model.OppositeSide(pos.Side),
			IsEntry:         false,
			IsAlgo:          true,
			Status:          model.StatusNew,
			TargetQty:       pos.Qty,
			TPPrice:         pos.TPPrice,
			CreatedAt:       pos.OpenTime,
		}
		if tpOrderID != 0 {
			b.OrdersByExchangeID[tpOrderID] = b.OrdersByClient[tpClientAlgoID]
		}
	}

	if pos.TP1ClientOrderID != "" {
		b.OrdersByClient[pos.TP1ClientOrderID] = &model.LiveOrderRecord{
			ClientOrderID:   pos.TP1ClientOrderID,
			ExchangeOrderID: pos.TP1OrderID,
			Symbol:          pos.Symbol,
			Side:            model.OppositeSide(pos.Side),
			IsEntry:         false,
			IsAlgo:          false,
			Status:          model.StatusNew,
			LimitPrice:      pos.TP1Price,
			CreatedAt:       pos.OpenTime,
		}
		if pos.TP1OrderID != 0 {
			b.OrdersByExchangeID[pos.TP1OrderID] = b.OrdersByClient[pos.TP1ClientOrderID]
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
		StrategyID:     normalizeBrokerStrategyID(b.PositionStrategyIDs[symbol]),
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

	activeSymbols := make(map[string]struct{})
	for sym, qty := range b.ActivePositions {
		if qty.GreaterThan(decimal.Zero) {
			activeSymbols[sym] = struct{}{}
		}
	}
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			if ord.Symbol == intent.Symbol {
				return false, fmt.Errorf("pending entry order already exists for %s", intent.Symbol)
			}
			activeSymbols[ord.Symbol] = struct{}{}
		}
	}
	if len(activeSymbols) >= b.Cfg.MaxSlots {
		return false, fmt.Errorf("max slots (%d) reached", b.Cfg.MaxSlots)
	}

	b.clearSymbolTransientMetadataLocked(intent.Symbol)

	b.OrdersByClient[intent.ClientOrderID] = &model.LiveOrderRecord{
		ClientOrderID:       intent.ClientOrderID,
		Symbol:              intent.Symbol,
		Side:                intent.Side,
		StrategyID:          normalizeBrokerStrategyID(intent.StrategyID),
		EntryOrderType:      effectiveEntryOrderType(intent.EntryOrderType, b.Cfg.EntryOrderType),
		IsEntry:             true,
		IsAlgo:              false,
		Status:              model.StatusPendingNew,
		TargetQty:           intent.Qty,
		CumulativeFilledQty: decimal.Zero,
		LimitPrice:          intent.LimitPrice,
		SLPrice:             intent.SLPrice,
		InitialSLPrice:      intent.SLPrice,
		TPPrice:             intent.TPPrice,
		TP1Price:            intent.TP1Price,
		ATRAtEntry:          intent.ATRAtEntry,
		SignalAuditID:       intent.SignalAuditID,
		CreatedAt:           time.Now().UTC(),
	}
	lev := b.Cfg.AccountLeverage
	if lev <= 0 {
		lev = 8.0
	}
	if intent.LimitPrice.GreaterThan(decimal.Zero) && intent.Qty.GreaterThan(decimal.Zero) {
		if b.ReservedEntryMargins == nil {
			b.ReservedEntryMargins = make(map[string]decimal.Decimal)
		}
		b.ReservedEntryMargins[intent.ClientOrderID] = intent.LimitPrice.Mul(intent.Qty).Div(decimal.NewFromFloat(lev))
	}
	b.ATRAtEntry[intent.Symbol] = intent.ATRAtEntry
	b.InitialSLPrices[intent.Symbol] = intent.SLPrice
	b.TP1Prices[intent.Symbol] = intent.TP1Price
	b.IsTP1Hits[intent.Symbol] = false
	b.PositionClientOrderIDs[intent.Symbol] = intent.ClientOrderID
	b.PositionStrategyIDs[intent.Symbol] = normalizeBrokerStrategyID(intent.StrategyID)
	return true, nil
}

// PreparePreSubmitIntent memvalidasi dan melakukan pre-repeg GTX cepat di memory menggunakan BBO lokal sebelum reservasi slot.
func (b *BinanceLiveBroker) PreparePreSubmitIntent(intent model.OrderIntent) (model.OrderIntent, error) {
	effOrderType := effectiveEntryOrderType(intent.EntryOrderType, b.Cfg.EntryOrderType)
	isLimit := !strings.EqualFold(effOrderType, "MARKET")
	if !isLimit || b.GetBestQuoteFn == nil {
		return intent, nil
	}
	maxRepegs, maxDrift := resolveEntryRepegSettings(b.Cfg.MaxEntryRepegAttempts, b.Cfg.MaxEntryRepegDriftPct)
	if bid, ask, ok := b.GetBestQuoteFn(intent.Symbol); ok && bid > 0 && ask > 0 {
		if wouldCrossMakerBook(intent.Side, intent.LimitPrice, bid, ask) {
			if maxRepegs <= 0 {
				return intent, fmt.Errorf("live: limit entry crossed spread before submit (bid=%v ask=%v limit=%s)", bid, ask, intent.LimitPrice)
			}
			var ef model.ExchangeFilter
			if b.GetExchangeFilterFn != nil {
				ef, _ = b.GetExchangeFilterFn(intent.Symbol)
			}
			updated, okRepeg, reason := repegLimitEntryIntent(intent, intent.LimitPrice, bid, ask, ef, maxDrift)
			if !okRepeg {
				return intent, fmt.Errorf("live: pre-submit GTX repeg rejected (%s)", reason)
			}
			slog.Info("live: entry order PRE-REPEGGED at submit (prevented GTX cross)",
				"symbol", intent.Symbol, "clientOrderID", intent.ClientOrderID,
				"oldLimit", intent.LimitPrice, "newLimit", updated.LimitPrice, "bid", bid, "ask", ask)
			return updated, nil
		}
	}
	return intent, nil
}

// SubmitReservedEntry mengeksekusi penempatan order ke bursa untuk intent yang telah direservasi secara atomik.
func (b *BinanceLiveBroker) SubmitReservedEntry(intent model.OrderIntent) (bool, error) {
	return b.submitReservedEntryWithParams(intent, intent.LimitPrice, 0)
}

// SubmitReservedEntryWithParams mengeksekusi penempatan order ke bursa untuk intent yang telah direservasi
// dengan meneruskan origLimit dan startAttempt dari hasil PreparePreSubmitIntent agar batas repeg dan drift akurat.
func (b *BinanceLiveBroker) SubmitReservedEntryWithParams(intent model.OrderIntent, origLimit decimal.Decimal, startAttempt int) (bool, error) {
	return b.submitReservedEntryWithParams(intent, origLimit, startAttempt)
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

	origLimit := intent.LimitPrice
	startAttempt := 0
	prepared, prepErr := b.PreparePreSubmitIntent(intent)
	if prepErr != nil {
		return false, prepErr
	}
	if !prepared.LimitPrice.Equal(intent.LimitPrice) {
		startAttempt = 1
	}
	intent = prepared

	reserved, err := b.ReserveEntryIntent(intent)
	if !reserved {
		return false, err
	}
	return b.submitReservedEntryWithParams(intent, origLimit, startAttempt)
}

func (b *BinanceLiveBroker) submitReservedEntryWithParams(intent model.OrderIntent, origLimit decimal.Decimal, startAttempt int) (bool, error) {
	effOrderType := effectiveEntryOrderType(intent.EntryOrderType, b.Cfg.EntryOrderType)
	isLimit := !strings.EqualFold(effOrderType, "MARKET")
	maxRepegs, maxDrift := resolveEntryRepegSettings(b.Cfg.MaxEntryRepegAttempts, b.Cfg.MaxEntryRepegDriftPct)
	if origLimit.LessThanOrEqual(decimal.Zero) {
		origLimit = intent.LimitPrice
	}

	placeFn := b.PlaceEntryFn
	if !isLimit && b.PlaceMarketEntryFn != nil {
		placeFn = b.PlaceMarketEntryFn
	}
	if placeFn == nil {
		b.Mu.Lock()
		b.rollbackReservedEntryLocked(intent.Symbol, intent.ClientOrderID)
		b.Mu.Unlock()
		return false, fmt.Errorf("PlaceEntryFn adapter is not configured")
	}

	currIntent := intent
	var exchangeOrderID int64
	var err error
	for attempt := startAttempt; attempt <= maxRepegs; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		exchangeOrderID, err = placeFn(ctx, currIntent)
		cancel()
		if err == nil {
			break
		}
		if !isLimit || !isGTXPostOnlyError(err) || attempt >= maxRepegs || b.GetBestQuoteFn == nil {
			break
		}

		bid, ask, ok := b.GetBestQuoteFn(currIntent.Symbol)
		if !ok || bid <= 0 || ask <= 0 {
			break
		}
		var ef model.ExchangeFilter
		if b.GetExchangeFilterFn != nil {
			ef, _ = b.GetExchangeFilterFn(currIntent.Symbol)
		}
		updated, okRepeg, reason := repegLimitEntryIntent(currIntent, origLimit, bid, ask, ef, maxDrift)
		if !okRepeg {
			slog.Warn("live: entry order GTX repeg aborted",
				"symbol", currIntent.Symbol, "clientOrderID", currIntent.ClientOrderID,
				"attempt", attempt+1, "reason", reason)
			break
		}
		// Jika target price kebetulan sama persis dengan yang baru saja ditolak -5022, dorong 1 tick keluar
		if updated.LimitPrice.Equal(currIntent.LimitPrice) && ef.TickSize.GreaterThan(decimal.Zero) {
			nudgedBid, nudgedAsk := bid, ask
			if currIntent.Side == model.SideShort {
				nudgedBid = updated.LimitPrice.InexactFloat64()
				nudgedAsk, _ = updated.LimitPrice.Add(ef.TickSize).Float64()
			} else {
				nudgedAsk = updated.LimitPrice.InexactFloat64()
				nudgedBid, _ = updated.LimitPrice.Sub(ef.TickSize).Float64()
			}
			if altUpdated, okAlt, _ := repegLimitEntryIntent(currIntent, origLimit, nudgedBid, nudgedAsk, ef, maxDrift); okAlt {
				updated = altUpdated
			} else {
				break
			}
		}

		oldClientID := currIntent.ClientOrderID
		updated.ClientOrderID = model.FormatClientID("live_repeg", currIntent.Symbol, time.Now().UTC())

		lev := b.Cfg.AccountLeverage
		if lev <= 0 {
			lev = 8.0
		}

		b.Mu.Lock()
		if rec, exists := b.OrdersByClient[oldClientID]; exists {
			delete(b.OrdersByClient, oldClientID)
			rec.ClientOrderID = updated.ClientOrderID
			rec.LimitPrice = updated.LimitPrice
			rec.SLPrice = updated.SLPrice
			rec.InitialSLPrice = updated.SLPrice
			rec.TPPrice = updated.TPPrice
			rec.TP1Price = updated.TP1Price
			b.OrdersByClient[updated.ClientOrderID] = rec
			b.InitialSLPrices[updated.Symbol] = updated.SLPrice
			b.TP1Prices[updated.Symbol] = updated.TP1Price
			b.PositionClientOrderIDs[updated.Symbol] = updated.ClientOrderID
		}
		if b.ReservedEntryMargins != nil {
			delete(b.ReservedEntryMargins, oldClientID)
			if updated.LimitPrice.GreaterThan(decimal.Zero) && updated.Qty.GreaterThan(decimal.Zero) {
				b.ReservedEntryMargins[updated.ClientOrderID] = updated.LimitPrice.Mul(updated.Qty).Div(decimal.NewFromFloat(lev))
			}
		}
		b.Mu.Unlock()

		slog.Info("live: entry order RE-PEGGED after GTX reject",
			"symbol", updated.Symbol, "oldClientOrderID", oldClientID, "newClientOrderID", updated.ClientOrderID,
			"oldLimit", currIntent.LimitPrice, "newLimit", updated.LimitPrice, "attempt", attempt+1)
		currIntent = updated
	}

	if err != nil {
		// Pencegahan Duplikasi: Jika penempatan error/timeout, periksa apakah bursa sebenarnya sudah menerima order
		var queryErr error
		if b.QueryOrderFn != nil {
			ctxQ, cancelQ := context.WithTimeout(context.Background(), 5*time.Second)
			ordRec, qErr := b.QueryOrderFn(ctxQ, currIntent.Symbol, currIntent.ClientOrderID)
			cancelQ()
			queryErr = qErr
			if qErr == nil && ordRec != nil && ordRec.ExchangeOrderID != 0 {
				isTerminalZero := ordRec.CumulativeFilledQty.LessThanOrEqual(decimal.Zero) &&
					(ordRec.Status == model.StatusCanceled || ordRec.Status == model.StatusExpired || ordRec.Status == model.StatusRejected)
				if !isTerminalZero {
					slog.Warn("entry placement returned error but order confirmed alive on Binance; retaining reservation",
						"symbol", currIntent.Symbol, "clientOrderID", currIntent.ClientOrderID, "exchangeOrderID", ordRec.ExchangeOrderID, "status", ordRec.Status)
					b.Mu.Lock()
					if rec, exists := b.OrdersByClient[currIntent.ClientOrderID]; exists {
						rec.Status = ordRec.Status
						rec.ExchangeOrderID = ordRec.ExchangeOrderID
						b.OrdersByExchangeID[ordRec.ExchangeOrderID] = rec
						if ordRec.Status == model.StatusNew || ordRec.Status == model.StatusPendingNew || ordRec.Status == model.StatusPartiallyFilled {
							lev := b.Cfg.AccountLeverage
							if lev <= 0 {
								lev = 8.0
							}
							if currIntent.LimitPrice.GreaterThan(decimal.Zero) && currIntent.Qty.GreaterThan(decimal.Zero) {
								if b.ReservedEntryMargins == nil {
									b.ReservedEntryMargins = make(map[string]decimal.Decimal)
								}
								b.ReservedEntryMargins[currIntent.ClientOrderID] = currIntent.LimitPrice.Mul(currIntent.Qty).Div(decimal.NewFromFloat(lev))
							}
						} else {
							delete(b.ReservedEntryMargins, currIntent.ClientOrderID)
						}
					}
					b.Mu.Unlock()
					if ordRec.CumulativeFilledQty.GreaterThan(decimal.Zero) {
						b.HandleUserDataOrderUpdate(
							currIntent.ClientOrderID,
							ordRec.ExchangeOrderID,
							currIntent.Symbol,
							ordRec.Status,
							ordRec.CumulativeFilledQty,
							ordRec.AvgPrice,
							decimal.Zero,
						)
					}
					return true, nil
				}
			}
		}

		// Jika placement maupun verifikasi query mengalami error ambigu (bukan penolakan bisnis 4xx),
		// pertahankan OrdersByClient & metadata SL/TP agar WS fill atau CheckPendingEntryTimeouts tidak kehilangan proteksi.
		if b.QueryOrderFn != nil && queryErr != nil && !isDefinitiveExchangeReject(err) && !isDefinitiveExchangeReject(queryErr) {
			slog.Warn("live: entry placement and verification both failed ambiguously; preserving local order metadata for reconciliation",
				"symbol", currIntent.Symbol, "clientOrderID", currIntent.ClientOrderID, "placeErr", err, "queryErr", queryErr)
			return false, fmt.Errorf("entry placement ambiguous (verify error: %v): %w", queryErr, err)
		}

		b.Mu.Lock()
		b.rollbackReservedEntryLocked(currIntent.Symbol, currIntent.ClientOrderID)
		b.Mu.Unlock()
		return false, fmt.Errorf("entry placement rejected/failed: %w", err)
	}

	b.Mu.Lock()
	if rec, exists := b.OrdersByClient[currIntent.ClientOrderID]; exists {
		if rec.Status == model.StatusPendingNew {
			rec.Status = model.StatusNew
		}
		rec.ExchangeOrderID = exchangeOrderID
		b.OrdersByExchangeID[exchangeOrderID] = rec
		if rec.Status == model.StatusPendingNew || rec.Status == model.StatusNew || rec.Status == model.StatusPartiallyFilled {
			lev := b.Cfg.AccountLeverage
			if lev <= 0 {
				lev = 8.0
			}
			if currIntent.LimitPrice.GreaterThan(decimal.Zero) && currIntent.Qty.GreaterThan(decimal.Zero) {
				if b.ReservedEntryMargins == nil {
					b.ReservedEntryMargins = make(map[string]decimal.Decimal)
				}
				b.ReservedEntryMargins[currIntent.ClientOrderID] = currIntent.LimitPrice.Mul(currIntent.Qty).Div(decimal.NewFromFloat(lev))
			}
		} else {
			delete(b.ReservedEntryMargins, currIntent.ClientOrderID)
		}
	} else {
		delete(b.ReservedEntryMargins, currIntent.ClientOrderID)
	}
	b.Mu.Unlock()

	return true, nil
}

func isDefinitiveExchangeReject(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, tok := range []string{
		"-2010", "-2011", "-2013", "-2019", "-5022",
		"-1013", "-1111", "-4003", "-4005", "-4014", "-4131",
		"Post Only", "Unknown order", "Order does not exist",
		"API returned HTTP 400",
	} {
		if strings.Contains(msg, tok) {
			return true
		}
	}
	return false
}

func (b *BinanceLiveBroker) collectAndDetachPendingEntriesLocked(symbol string) []model.LiveOrderRecord {
	var entries []model.LiveOrderRecord
	for cID, ord := range b.OrdersByClient {
		if ord != nil && ord.Symbol == symbol && ord.IsEntry &&
			(ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			entries = append(entries, *ord)
			delete(b.OrdersByClient, cID)
			delete(b.ReservedEntryMargins, cID)
			if ord.ExchangeOrderID != 0 {
				delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
			}
		}
	}
	if len(entries) > 0 {
		if curPos := b.ActivePositions[symbol]; curPos.GreaterThan(decimal.Zero) {
			actualTotalEntry := curPos.Add(b.TP1FilledQtys[symbol]).Add(b.TP1CommittedQtys[symbol])
			if actualTotalEntry.GreaterThan(decimal.Zero) {
				b.InitialQtys[symbol] = actualTotalEntry
			}
		}
	}
	return entries
}

func (b *BinanceLiveBroker) cancelEntryOrdersAsync(entries []model.LiveOrderRecord) {
	if len(entries) == 0 || b.CancelOrderFn == nil {
		return
	}
	b.goAsync(func() {
		for _, ord := range entries {
			ctxE, cancelE := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctxE, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
			cancelE()
		}
	})
}

func (b *BinanceLiveBroker) IsAccountStale(now time.Time) bool {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	if b.LastAccountUpdateTime.IsZero() {
		return true
	}
	if b.Cfg.MaxAccountStaleness <= 0 {
		return false
	}
	return now.Sub(b.LastAccountUpdateTime) > b.Cfg.MaxAccountStaleness
}

func (b *BinanceLiveBroker) cleanupClosedPositionLocked(symbol string) []model.LiveOrderRecord {
	var entryOrdersToCancel []model.LiveOrderRecord
	for _, ord := range b.OrdersByClient {
		if ord != nil && ord.Symbol == symbol && ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			entryOrdersToCancel = append(entryOrdersToCancel, *ord)
		}
	}
	b.clearSymbolStateLocked(symbol)
	return entryOrdersToCancel
}

func (b *BinanceLiveBroker) rollbackReservedEntryLocked(symbol, clientOrderID string) {
	delete(b.OrdersByClient, clientOrderID)
	delete(b.ReservedEntryMargins, clientOrderID)
	if qty, hasPos := b.ActivePositions[symbol]; !hasPos || qty.LessThanOrEqual(decimal.Zero) {
		delete(b.ATRAtEntry, symbol)
		delete(b.InitialSLPrices, symbol)
		delete(b.InitialQtys, symbol)
		delete(b.TP1Prices, symbol)
		delete(b.IsTP1Hits, symbol)
		if b.PositionClientOrderIDs[symbol] == clientOrderID {
			delete(b.PositionClientOrderIDs, symbol)
			delete(b.PositionStrategyIDs, symbol)
		}
		b.clearSymbolTransientMetadataLocked(symbol)
	}
}

func (b *BinanceLiveBroker) clearSymbolTransientMetadataLocked(symbol string) {
	delete(b.ClosingInFlight, symbol)
	delete(b.ClosingReason, symbol)
	delete(b.ProtectionTiers, symbol)
	delete(b.PeakPrices, symbol)
	delete(b.ActiveMAE, symbol)
	delete(b.ActiveMFE, symbol)
	delete(b.AlgoUpdateInFlight, symbol)
	delete(b.TPRepairInFlight, symbol)
	delete(b.TP1InFlight, symbol)
	delete(b.TP1FilledQtys, symbol)
	delete(b.TP1LastFillPrices, symbol)
	delete(b.TP1CommittedQtys, symbol)
	delete(b.TP1FeesUSDT, symbol)
	delete(b.LastAlgoUpdateTimes, symbol)
	delete(b.LastSentSLPrices, symbol)
	delete(b.LastMarkPrices, symbol)
	delete(b.LastMarkUpdatedAt, symbol)
	delete(b.PositionUnrealizedPnLs, symbol)
	delete(b.EntryFeesUSDT, symbol)
	delete(b.EntryWasMaker, symbol)
	delete(b.ExitRealizedProfits, symbol)
	delete(b.ExitFeesUSDT, symbol)
	delete(b.LastAlgoFailTimes, symbol)
	delete(b.LastClosingAttempt, symbol)
	delete(b.TP1FallbackAttempts, symbol)
	delete(b.LastTP1FallbackTime, symbol)
	delete(b.LastAccountPosPrevQty, symbol)
}

// RefreshAccount mengambil data saldo dompet dan posisi aktif terkini via REST API Binance (FetchAccountFn)
// dan langsung memperbarui state in-memory live broker (Solusi P0-2 untuk mencegah account staleness),
// sekaligus merekonsiliasi posisi yang terisi atau tertutup selama User Data WS terputus.
func (b *BinanceLiveBroker) RefreshAccount(ctx context.Context) error {
	if b.FetchAccountFn == nil {
		return nil
	}
	wb, up, ab, pos, err := b.FetchAccountFn(ctx)
	if err != nil {
		return fmt.Errorf("refresh account balance: %w", err)
	}
	now := time.Now().UTC()
	b.HandleAccountUpdate(wb, up, ab, pos, now)
	b.reconcileAfterAccountSnapshot(pos, now)
	return nil
}

const offlineReconcileGrace = 15 * time.Second

func (b *BinanceLiveBroker) reconcileAfterAccountSnapshot(positions map[string]model.LivePosition, now time.Time) {
	var plans []model.ProtectionPlanAction
	type offlineCloseCleanup struct {
		symbol      string
		slAlgoID    int64
		tpAlgoID    int64
		tp1OrderID  int64
		entryOrders []model.LiveOrderRecord
	}
	var closedCleanups []offlineCloseCleanup

	b.Mu.Lock()
	// 1. Posisi terbuka di Binance yang belum punya SL/TP lokal (miss ORDER_TRADE_UPDATE saat WS/internet mati)
	for sym, pos := range positions {
		if !pos.Qty.GreaterThan(decimal.Zero) {
			continue
		}
		if b.IsTP1Hits[sym] {
			continue
		}
		if prot, hasProt := b.Protections[sym]; hasProt && prot != nil {
			if prot.SLStatus == model.ProtectionPending || prot.SLStatus == model.ProtectionFailed {
				continue
			}
			if prot.SLStatus == model.ProtectionAcked && prot.ProtectedQty.GreaterThanOrEqual(pos.Qty) {
				continue
			}
		}
		var entryRec *model.LiveOrderRecord
		if cid, ok := b.PositionClientOrderIDs[sym]; ok && cid != "" {
			if r := b.OrdersByClient[cid]; r != nil && r.IsEntry && r.Symbol == sym {
				entryRec = r
			}
		}
		if entryRec == nil {
			for _, r := range b.OrdersByClient {
				if r != nil && r.IsEntry && r.Symbol == sym {
					entryRec = r
					break
				}
			}
		}
		if entryRec == nil {
			continue
		}
		// Hindari race dengan SubmitLimitEntry yang baru beberapa milidetik berjalan sebelum ExchangeOrderID terisi
		if entryRec.Status == model.StatusPendingNew && entryRec.ExchangeOrderID == 0 && now.Sub(entryRec.CreatedAt) < 5*time.Second {
			continue
		}
		if entryRec.TargetQty.GreaterThan(decimal.Zero) && pos.Qty.LessThan(entryRec.TargetQty) {
			entryRec.Status = model.StatusPartiallyFilled
		} else {
			entryRec.Status = model.StatusFilled
		}
		entryRec.CumulativeFilledQty = pos.Qty
		entryRec.AvgPrice = pos.EntryPrice
		if plan, needExec := b.applyEntryFillLocked(entryRec, pos.Qty, pos.EntryPrice, now); needExec {
			slog.Warn("live: reconciled missed entry fill from REST account snapshot; arming SL/TP protections",
				"symbol", sym, "clientOrderID", entryRec.ClientOrderID, "qty", pos.Qty, "entryPrice", pos.EntryPrice)
			plans = append(plans, plan)
		}
	}

	// 2. Posisi lokal yang sudah tidak ada di snapshot REST Binance penuh (tertutup SL/TP saat offline)
	trackedSymbols := make(map[string]bool)
	for sym, q := range b.ActivePositions {
		if q.GreaterThan(decimal.Zero) {
			trackedSymbols[sym] = true
		}
	}
	for sym := range b.Protections {
		trackedSymbols[sym] = true
	}

	confirmFn := b.ConfirmPositionZeroFn
	var missingCandidates []string
	for sym := range trackedSymbols {
		if pos, exists := positions[sym]; exists && pos.Qty.GreaterThan(decimal.Zero) {
			continue
		}
		if b.ClosingInFlight[sym] {
			continue
		}
		if openTime, hasOpen := b.PositionOpenTimes[sym]; hasOpen && now.Sub(openTime) < offlineReconcileGrace {
			continue
		}
		if confirmFn != nil {
			missingCandidates = append(missingCandidates, sym)
			continue
		}
		var slAlgoID, tpAlgoID, tp1OrderID int64
		if prot := b.Protections[sym]; prot != nil {
			slAlgoID = prot.SLOrderID
			tpAlgoID = prot.TPOrderID
			tp1OrderID = prot.TP1OrderID
		}
		entryOrdersToCancel := b.cleanupClosedPositionLocked(sym)
		closedCleanups = append(closedCleanups, offlineCloseCleanup{
			symbol:      sym,
			slAlgoID:    slAlgoID,
			tpAlgoID:    tpAlgoID,
			tp1OrderID:  tp1OrderID,
			entryOrders: entryOrdersToCancel,
		})
	}
	b.Mu.Unlock()

	if confirmFn != nil && len(missingCandidates) > 0 {
		var confirmedZero []string
		for _, sym := range missingCandidates {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			isZero, cErr := confirmFn(cCtx, sym)
			cCancel()
			if cErr != nil || !isZero {
				slog.Warn("live: offline-close confirmation aborted (position still open or confirm failed); retaining local state and SL/TP",
					"symbol", sym, "isZero", isZero, "err", cErr)
				continue
			}
			confirmedZero = append(confirmedZero, sym)
		}
		if len(confirmedZero) > 0 {
			b.Mu.Lock()
			for _, sym := range confirmedZero {
				if b.ClosingInFlight[sym] {
					continue
				}
				if openTime, hasOpen := b.PositionOpenTimes[sym]; hasOpen && now.Sub(openTime) < offlineReconcileGrace {
					continue
				}
				_, hasActive := b.ActivePositions[sym]
				_, hasProt := b.Protections[sym]
				if !hasActive && !hasProt {
					continue
				}
				var slAlgoID, tpAlgoID, tp1OrderID int64
				if prot := b.Protections[sym]; prot != nil {
					slAlgoID = prot.SLOrderID
					tpAlgoID = prot.TPOrderID
					tp1OrderID = prot.TP1OrderID
				}
				entryOrdersToCancel := b.cleanupClosedPositionLocked(sym)
				closedCleanups = append(closedCleanups, offlineCloseCleanup{
					symbol:      sym,
					slAlgoID:    slAlgoID,
					tpAlgoID:    tpAlgoID,
					tp1OrderID:  tp1OrderID,
					entryOrders: entryOrdersToCancel,
				})
			}
			b.Mu.Unlock()
		}
	}

	for _, plan := range plans {
		p := plan
		b.goAsync(func() { b.executeProtectionPlan(p) })
	}

	for _, cl := range closedCleanups {
		slog.Warn("live: reconciled offline-closed position from full account snapshot; cleaning stale local state",
			"symbol", cl.symbol, "slAlgoID", cl.slAlgoID, "tpAlgoID", cl.tpAlgoID)
		b.notifyPositionDeleted(cl.symbol)
		if b.OnCooldownRecordedFn != nil {
			b.OnCooldownRecordedFn(cl.symbol, now, "OFFLINE_CLOSED")
		}
		for _, algoID := range []int64{cl.slAlgoID, cl.tpAlgoID} {
			if algoID != 0 && b.CancelAlgoOrderFn != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = b.CancelAlgoOrderFn(ctx, cl.symbol, algoID)
				cancel()
			}
		}
		if cl.tp1OrderID != 0 && b.CancelOrderFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctx, cl.symbol, cl.tp1OrderID, "")
			cancel()
		}
		if b.CancelOrderFn != nil {
			for _, eOrd := range cl.entryOrders {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = b.CancelOrderFn(ctx, eOrd.Symbol, eOrd.ExchangeOrderID, eOrd.ClientOrderID)
				cancel()
			}
		}
	}
}

func (b *BinanceLiveBroker) applyEntryFillLocked(
	rec *model.LiveOrderRecord,
	filledQty, avgPrice decimal.Decimal,
	now time.Time,
) (model.ProtectionPlanAction, bool) {
	symbol := rec.Symbol
	alreadyTP1 := b.TP1FilledQtys[symbol].Add(b.TP1CommittedQtys[symbol])
	netQty := filledQty.Sub(alreadyTP1)
	if netQty.IsNegative() {
		netQty = decimal.Zero
	}
	b.ActivePositions[symbol] = netQty
	b.PositionSides[symbol] = rec.Side
	if b.PositionStrategyIDs == nil {
		b.PositionStrategyIDs = make(map[string]string)
	}
	if b.PositionStrategyIDs[symbol] == "" {
		b.PositionStrategyIDs[symbol] = normalizeBrokerStrategyID(rec.StrategyID)
	}
	if avgPrice.GreaterThan(decimal.Zero) {
		b.PositionEntryPrices[symbol] = avgPrice
	}
	if _, hasOpenTime := b.PositionOpenTimes[symbol]; !hasOpenTime {
		b.PositionOpenTimes[symbol] = now
	}
	if !b.IsTP1Hits[symbol] && filledQty.GreaterThan(b.InitialQtys[symbol]) {
		b.InitialQtys[symbol] = filledQty
	} else if b.InitialQtys[symbol].IsZero() {
		b.InitialQtys[symbol] = filledQty
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
	return b.planProtectionSyncLocked(rec)
}

func (b *BinanceLiveBroker) clearSymbolStateLocked(symbol string) {
	delete(b.ActivePositions, symbol)
	delete(b.PositionOpenTimes, symbol)
	delete(b.PositionSides, symbol)
	delete(b.PositionEntryPrices, symbol)
	delete(b.PositionClientOrderIDs, symbol)
	delete(b.PositionStrategyIDs, symbol)
	delete(b.Protections, symbol)
	delete(b.InitialQtys, symbol)
	delete(b.InitialSLPrices, symbol)
	delete(b.TP1Prices, symbol)
	delete(b.IsTP1Hits, symbol)
	delete(b.ATRAtEntry, symbol)
	b.clearSymbolTransientMetadataLocked(symbol)
	for cID, ord := range b.OrdersByClient {
		if ord != nil && ord.Symbol == symbol {
			delete(b.ReservedEntryMargins, cID)
			if ord.ExchangeOrderID != 0 {
				delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
			}
			delete(b.OrdersByClient, cID)
		}
	}
}

func (b *BinanceLiveBroker) estimateAvailableBalanceLocked() decimal.Decimal {
	return b.estimateAvailableBalanceWithOptsLocked(true)
}

func (b *BinanceLiveBroker) estimateAvailableBalanceWithOptsLocked(includeReservedOrders bool) decimal.Decimal {
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	lev := b.Cfg.AccountLeverage
	if lev <= 0 {
		lev = 8.0
	}
	levDec := decimal.NewFromFloat(lev)
	usedMargin := decimal.Zero
	for sym, qty := range b.ActivePositions {
		if qty.GreaterThan(decimal.Zero) {
			ep := b.PositionEntryPrices[sym]
			if ep.GreaterThan(decimal.Zero) {
				usedMargin = usedMargin.Add(ep.Mul(qty).Div(levDec))
			}
		}
	}
	for cID, ord := range b.OrdersByClient {
		if ord != nil && ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			if !includeReservedOrders {
				if _, alreadyReserved := b.ReservedEntryMargins[cID]; alreadyReserved {
					continue
				}
			}
			remQty := ord.TargetQty.Sub(ord.CumulativeFilledQty)
			if remQty.GreaterThan(decimal.Zero) && ord.LimitPrice.GreaterThan(decimal.Zero) {
				usedMargin = usedMargin.Add(ord.LimitPrice.Mul(remQty).Div(levDec))
			}
		}
	}
	avail := effectiveEquity.Sub(usedMargin)
	if avail.LessThan(decimal.Zero) {
		return decimal.Zero
	}
	return avail
}

// HandleAccountUpdate memproses snapshot akun lengkap dari REST atau test helper (Solusi P0-2).
func (b *BinanceLiveBroker) HandleAccountUpdate(
	walletBal decimal.Decimal,
	unrealizedPnL decimal.Decimal,
	availBal decimal.Decimal,
	positions map[string]model.LivePosition,
	eventTime time.Time,
) {
	b.HandleAccountUpdateEvent(model.AccountUpdateEvent{
		WalletBalance:    walletBal,
		AvailableBalance: availBal,
		UnrealizedPnL:    unrealizedPnL,
		Positions:        positions,
		EventTime:        eventTime,
		IsFullSnapshot:   true,
	})
}

// HandleAccountUpdateEvent memproses baik snapshot REST penuh maupun delta WebSocket ACCOUNT_UPDATE parsial
// tanpa menimpa total UnrealizedPnLUSDT atau AvailableBalanceUSDT oleh data parsial/crossWallet.
func (b *BinanceLiveBroker) HandleAccountUpdateEvent(evt model.AccountUpdateEvent) {
	var ordersToCancel []model.LiveOrderRecord
	triggerAsyncRefresh := false

	b.Mu.Lock()
	if evt.IsFullSnapshot {
		b.WalletBalanceUSDT = evt.WalletBalance
		b.UnrealizedPnLUSDT = evt.UnrealizedPnL
		b.AvailableBalanceUSDT = evt.AvailableBalance
		preservedMargins := make(map[string]decimal.Decimal)
		for cid, m := range b.ReservedEntryMargins {
			if rec, ok := b.OrdersByClient[cid]; ok && rec != nil && rec.IsEntry && rec.Status == model.StatusPendingNew {
				preservedMargins[cid] = m
			}
		}
		b.ReservedEntryMargins = preservedMargins
		if b.PositionUnrealizedPnLs == nil {
			b.PositionUnrealizedPnLs = make(map[string]decimal.Decimal)
		} else {
			clear(b.PositionUnrealizedPnLs)
		}
	} else {
		if evt.WalletBalance.GreaterThan(decimal.Zero) {
			b.WalletBalanceUSDT = evt.WalletBalance
		}
		if b.PositionUnrealizedPnLs == nil {
			b.PositionUnrealizedPnLs = make(map[string]decimal.Decimal)
		}
	}
	b.LastAccountUpdateTime = evt.EventTime

	hasDecreasedOrClosed := false
	if b.LastAccountPosPrevQty == nil {
		b.LastAccountPosPrevQty = make(map[string]decimal.Decimal)
	}
	for sym, pos := range evt.Positions {
		if pos.Qty.GreaterThan(decimal.Zero) {
			if prevQty, existed := b.ActivePositions[sym]; existed && pos.Qty.LessThan(prevQty) {
				hasDecreasedOrClosed = true
				if existingPrev := b.LastAccountPosPrevQty[sym]; existingPrev.LessThan(prevQty) {
					b.LastAccountPosPrevQty[sym] = prevQty
				}
			} else if existed && pos.Qty.GreaterThan(prevQty) {
				delete(b.LastAccountPosPrevQty, sym)
			}
			if _, exists := b.PositionOpenTimes[sym]; !exists {
				b.PositionOpenTimes[sym] = evt.EventTime
			}
			b.PositionSides[sym] = pos.Side
			b.PositionEntryPrices[sym] = pos.EntryPrice
			b.ActivePositions[sym] = pos.Qty
			b.PositionUnrealizedPnLs[sym] = pos.UnrealizedPnL
		} else {
			hasDecreasedOrClosed = true
			delete(b.LastAccountPosPrevQty, sym)
			delete(b.ActivePositions, sym)
			delete(b.PositionUnrealizedPnLs, sym)
			if _, hasProt := b.Protections[sym]; !hasProt {
				b.clearSymbolStateLocked(sym)
			}
		}
	}

	if !evt.IsFullSnapshot {
		// Hitung ulang total UnrealizedPnLUSDT dari seluruh posisi terbuka agar update parsial 1 koin tidak menghapus uPnL koin lain.
		totalUPnL := decimal.Zero
		for sym, qty := range b.ActivePositions {
			if qty.GreaterThan(decimal.Zero) {
				totalUPnL = totalUPnL.Add(b.PositionUnrealizedPnLs[sym])
			}
		}
		b.UnrealizedPnLUSDT = totalUPnL

		// Jangan timpa AvailableBalanceUSDT dengan CrossWallet (cw); jaga estimasi konservatif sambil menjadwalkan REST sync.
		// Hitung tanpa order yang sudah tercatat di ReservedEntryMargins agar GetAvailableBalanceUSDT tidak memotong ganda.
		estAvail := b.estimateAvailableBalanceWithOptsLocked(false)
		if b.AvailableBalanceUSDT.IsZero() || estAvail.LessThan(b.AvailableBalanceUSDT) {
			b.AvailableBalanceUSDT = estAvail
		}
		if b.FetchAccountFn != nil && (len(evt.Positions) > 0 || hasDecreasedOrClosed) {
			if b.lastAsyncRefresh.IsZero() || evt.EventTime.Sub(b.lastAsyncRefresh) >= 3*time.Second {
				b.lastAsyncRefresh = evt.EventTime
				triggerAsyncRefresh = true
			}
		}
	}

	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	b.PeakEffectiveEquityUSDT, _ = protection.UpdatePeakAndDrawdownPct(b.PeakEffectiveEquityUSDT, effectiveEquity)

	epochDay := evt.EventTime.UTC().Unix() / 86400
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
				if ord != nil && ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
					ordersToCancel = append(ordersToCancel, *ord)
				}
			}
		}
	}
	b.Mu.Unlock()

	if triggerAsyncRefresh && b.FetchAccountFn != nil {
		b.goAsync(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = b.RefreshAccount(ctx)
		})
	}

	if len(ordersToCancel) > 0 && b.CancelOrderFn != nil {
		for _, ord := range ordersToCancel {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
			cancel()
		}
	}
}

func isUSDCommissionAsset(asset string) bool {
	switch strings.ToUpper(strings.TrimSpace(asset)) {
	case "", "USDT", "USDC", "FDUSD", "BFUSD", "BUSD":
		return true
	default:
		return false
	}
}

// HandleUserDataOrderUpdate merekonsiliasi event ORDER_TRADE_UPDATE (Solusi P0-3 & P0-6).
// Mempertahankan signature 7-argumen untuk kompatibilitas tes; stream live memanggil HandleUserDataOrderUpdateDetailed.
func (b *BinanceLiveBroker) HandleUserDataOrderUpdate(
	clientOrderID string,
	exchangeOrderID int64,
	symbol string,
	status model.OrderLifecycleStatus,
	cumFilledQty, avgPrice, realizedProfit decimal.Decimal,
) {
	b.HandleUserDataOrderUpdateDetailed(model.OrderTradeUpdateEvent{
		ClientOrderID:   clientOrderID,
		ExchangeOrderID: exchangeOrderID,
		Symbol:          symbol,
		Status:          status,
		CumFilledQty:    cumFilledQty,
		AvgPrice:        avgPrice,
		RealizedProfit:  realizedProfit,
		HasFillMeta:     false,
	})
}

// HandleUserDataOrderUpdateDetailed merekonsiliasi event ORDER_TRADE_UPDATE beserta komisi & flag maker/taker aktual.
func (b *BinanceLiveBroker) HandleUserDataOrderUpdateDetailed(evt model.OrderTradeUpdateEvent) {
	clientOrderID := evt.ClientOrderID
	exchangeOrderID := evt.ExchangeOrderID
	symbol := evt.Symbol
	status := evt.Status
	cumFilledQty := evt.CumFilledQty
	avgPrice := evt.AvgPrice
	realizedProfit := evt.RealizedProfit

	// Guard Kritis MISS #1: Order Partial TP1 ("ALGO_TP1_..." atau Limit Maker TP1)
	// Order penutupan TP1 reduceOnly tidak boleh masuk ke Skenario 2 (penutupan posisi penuh),
	// karena akan menghapus sisa lot runner dan proteksinya dari memori.
	isTP1Order := strings.HasPrefix(clientOrderID, "ALGO_TP1_") || strings.HasSuffix(clientOrderID, "_TP1")
	if !isTP1Order && exchangeOrderID != 0 {
		b.Mu.Lock()
		if prot, ok := b.Protections[symbol]; ok && prot.TP1OrderID == exchangeOrderID {
			isTP1Order = true
		}
		b.Mu.Unlock()
	}
	if isTP1Order {
		tp1IsMaker := true
		if evt.HasFillMeta {
			tp1IsMaker = evt.IsMaker
		}
		if status == model.StatusPartiallyFilled && cumFilledQty.GreaterThan(decimal.Zero) {
			b.recordTP1PartialChunkWithFee(symbol, clientOrderID, cumFilledQty, avgPrice, evt.Commission, evt.CommissionAsset)
		} else if status == model.StatusFilled {
			b.processTP1ExecutedWithFee(symbol, cumFilledQty, avgPrice, clientOrderID, tp1IsMaker, evt.Commission, evt.CommissionAsset)
		} else if status == model.StatusCanceled || status == model.StatusExpired || status == model.StatusRejected {
			var canceledPartialRec *TradeClosedRecord
			var fullClose bool
			var siblingAlgoIDs []int64
			var entryOrdersToCancel []model.LiveOrderRecord
			b.Mu.Lock()
			if prot, ok := b.Protections[symbol]; ok && (prot.TP1OrderID == exchangeOrderID || (clientOrderID != "" && prot.TP1ClientOrderID == clientOrderID)) {
				prot.TP1OrderID = 0
				prot.TP1ClientOrderID = ""
			}
			if !b.IsTP1Hits[symbol] {
				prevFilled := b.TP1FilledQtys[symbol]
				prevCommitted := b.TP1CommittedQtys[symbol]
				if cumFilledQty.GreaterThan(prevFilled) {
					rem, recordedCum, hasPos, hasProt := b.deductTP1QtyLocked(symbol, prevCommitted, prevFilled, cumFilledQty)
					if hasPos || hasProt {
						if hasPos && b.ActivePositions[symbol].GreaterThan(decimal.Zero) {
							b.ActivePositions[symbol] = rem
						}
						if evt.Commission.GreaterThan(decimal.Zero) && isUSDCommissionAsset(evt.CommissionAsset) {
							b.TP1FeesUSDT[symbol] = b.TP1FeesUSDT[symbol].Add(evt.Commission)
						}
						prevFilled = recordedCum
					}
				}
				if prevFilled.GreaterThan(decimal.Zero) {
					exitP := avgPrice
					if exitP.IsZero() {
						exitP = b.TP1LastFillPrices[symbol]
					}
					if exitP.IsZero() {
						exitP = b.TP1Prices[symbol]
					}
					remAfter, hasPos := b.ActivePositions[symbol]
					_, hasProt := b.Protections[symbol]
					initQty := b.InitialQtys[symbol]
					// Anggap full close hanya jika sisa posisi benar-benar nol DAN memang tidak ada runner tersisa
					// (cegah double-deduction/false full close saat ACCOUNT_UPDATE datang lebih dulu untuk partial TP1).
					isZeroAfterTP1 := !hasPos || remAfter.LessThanOrEqual(decimal.Zero)
					if hasProt && isZeroAfterTP1 && (hasPos || initQty.LessThanOrEqual(decimal.Zero) || prevCommitted.Add(prevFilled).GreaterThanOrEqual(initQty)) {
						var fullRec TradeClosedRecord
						fullRec, siblingAlgoIDs, entryOrdersToCancel = b.finalizeTP1FullCloseLocked(symbol, prevFilled, exitP, clientOrderID, tp1IsMaker)
						canceledPartialRec = &fullRec
						fullClose = true
					} else {
						recVal := b.buildTP1TradeRecordLocked(symbol, prevFilled, exitP, clientOrderID, tp1IsMaker)
						canceledPartialRec = &recVal
						b.TP1CommittedQtys[symbol] = b.TP1CommittedQtys[symbol].Add(prevFilled)
						delete(b.TP1FilledQtys, symbol)
						delete(b.TP1LastFillPrices, symbol)
						b.notifyPositionSavedLocked(symbol)
					}
				}
			}
			b.Mu.Unlock()
			if fullClose && canceledPartialRec != nil {
				b.dispatchTP1FullClose(*canceledPartialRec, siblingAlgoIDs, entryOrdersToCancel)
			} else if canceledPartialRec != nil && b.OnTradeClosedFn != nil {
				recCopy := *canceledPartialRec
				cb := b.OnTradeClosedFn
				b.goAsync(func() { cb(recCopy) })
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
		var queriedRec *model.LiveOrderRecord
		if b.QueryOrderFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			qRec, err := b.QueryOrderFn(ctx, symbol, clientOrderID)
			cancel()
			if err == nil && qRec != nil {
				queriedRec = qRec
			}
		}

		b.Mu.Lock()
		rec, exists = b.OrdersByClient[clientOrderID]
		if !exists && exchangeOrderID != 0 {
			rec, exists = b.OrdersByExchangeID[exchangeOrderID]
		}
		if !exists && queriedRec != nil {
			// Nolkan CumulativeFilledQty sebelum assignment agar deltaFilled == cumFilledQty event ini.
			queriedRec.CumulativeFilledQty = decimal.Zero
			if queriedRec.IsEntry {
				if queriedRec.SLPrice.IsZero() {
					if !b.InitialSLPrices[symbol].IsZero() {
						queriedRec.SLPrice = b.InitialSLPrices[symbol]
						queriedRec.InitialSLPrice = b.InitialSLPrices[symbol]
					} else if p, ok := b.Protections[symbol]; ok && p != nil && p.SLPrice.GreaterThan(decimal.Zero) {
						queriedRec.SLPrice = p.SLPrice
						queriedRec.InitialSLPrice = p.InitialSLPrice
					}
				}
				if queriedRec.TPPrice.IsZero() {
					if p, ok := b.Protections[symbol]; ok && p != nil && p.TPPrice.GreaterThan(decimal.Zero) {
						queriedRec.TPPrice = p.TPPrice
					}
				}
				if queriedRec.TP1Price.IsZero() && b.TP1Prices[symbol].GreaterThan(decimal.Zero) {
					queriedRec.TP1Price = b.TP1Prices[symbol]
				}
				if queriedRec.ATRAtEntry.IsZero() && b.ATRAtEntry[symbol].GreaterThan(decimal.Zero) {
					queriedRec.ATRAtEntry = b.ATRAtEntry[symbol]
				}
				if queriedRec.SignalAuditID == 0 {
					if p, ok := b.Protections[symbol]; ok && p != nil && p.SignalAuditID > 0 {
						queriedRec.SignalAuditID = p.SignalAuditID
					}
				}
				if queriedRec.StrategyID == "" && b.PositionStrategyIDs[symbol] != "" {
					queriedRec.StrategyID = b.PositionStrategyIDs[symbol]
				}
			}
			b.OrdersByClient[queriedRec.ClientOrderID] = queriedRec
			if queriedRec.ExchangeOrderID != 0 {
				b.OrdersByExchangeID[queriedRec.ExchangeOrderID] = queriedRec
			}
			rec = queriedRec
			exists = true
		}
		if !exists {
			_, hasPos := b.ActivePositions[symbol]
			_, hasProt := b.Protections[symbol]
			if (hasPos || hasProt) && (status == model.StatusFilled || status == model.StatusPartiallyFilled) {
				posSide := b.PositionSides[symbol]
				rec = &model.LiveOrderRecord{
					ClientOrderID:       clientOrderID,
					ExchangeOrderID:     exchangeOrderID,
					Symbol:              symbol,
					Side:                model.OppositeSide(posSide),
					IsEntry:             false,
					Status:              status,
					TargetQty:           cumFilledQty,
					CumulativeFilledQty: decimal.Zero,
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

	prevCumFilled := rec.CumulativeFilledQty
	deltaFilled := cumFilledQty.Sub(prevCumFilled)

	rec.ExchangeOrderID = exchangeOrderID
	rec.Status = status
	rec.CumulativeFilledQty = cumFilledQty
	if avgPrice.GreaterThan(decimal.Zero) {
		rec.AvgPrice = avgPrice
	}
	b.OrdersByExchangeID[exchangeOrderID] = rec

	// SKENARIO 1: Update Order Entry
	if rec.IsEntry {
		// Jika posisi sudah mencapai TP1 milestone, tolak penambahan fill ke runner
		if b.IsTP1Hits[symbol] {
			slog.Warn("live: received entry fill after TP1 milestone reached; ignoring to protect runner state", "symbol", symbol, "orderID", exchangeOrderID)
			b.Mu.Unlock()
			return
		}

		recordEntryFeeLocked := func() {
			if b.EntryFeesUSDT == nil {
				b.EntryFeesUSDT = make(map[string]decimal.Decimal)
			}
			if b.EntryWasMaker == nil {
				b.EntryWasMaker = make(map[string]bool)
			}
			if evt.HasFillMeta {
				b.EntryWasMaker[symbol] = evt.IsMaker
				if deltaFilled.GreaterThan(decimal.Zero) {
					if evt.Commission.GreaterThan(decimal.Zero) && isUSDCommissionAsset(evt.CommissionAsset) {
						b.EntryFeesUSDT[symbol] = b.EntryFeesUSDT[symbol].Add(evt.Commission)
					} else if avgPrice.GreaterThan(decimal.Zero) {
						chunkQty := evt.LastFilledQty
						if chunkQty.LessThanOrEqual(decimal.Zero) {
							chunkQty = deltaFilled
						}
						rate := b.effectiveFeeRate(evt.IsMaker)
						b.EntryFeesUSDT[symbol] = b.EntryFeesUSDT[symbol].Add(chunkQty.Mul(avgPrice).Mul(decimal.NewFromFloat(rate)))
					}
				}
			} else if _, set := b.EntryWasMaker[symbol]; !set {
				effOrderType := effectiveEntryOrderType(rec.EntryOrderType, b.Cfg.EntryOrderType)
				b.EntryWasMaker[symbol] = strings.EqualFold(effOrderType, "LIMIT")
			}
		}

		switch status {
		case model.StatusPartiallyFilled, model.StatusFilled:
			if cumFilledQty.GreaterThan(decimal.Zero) {
				recordEntryFeeLocked()
				if status == model.StatusFilled {
					delete(b.ReservedEntryMargins, rec.ClientOrderID)
				}
				plan, needPlanExecution = b.applyEntryFillLocked(rec, cumFilledQty, avgPrice, time.Now().UTC())
			}
		case model.StatusCanceled, model.StatusExpired:
			delete(b.ReservedEntryMargins, rec.ClientOrderID)
			if cumFilledQty.GreaterThan(decimal.Zero) {
				recordEntryFeeLocked()
				b.InitialQtys[symbol] = cumFilledQty
				plan, needPlanExecution = b.applyEntryFillLocked(rec, cumFilledQty, avgPrice, time.Now().UTC())
			} else {
				delete(b.OrdersByExchangeID, exchangeOrderID)
				b.rollbackReservedEntryLocked(symbol, rec.ClientOrderID)
			}
		case model.StatusRejected:
			delete(b.ReservedEntryMargins, rec.ClientOrderID)
			delete(b.OrdersByExchangeID, exchangeOrderID)
			b.rollbackReservedEntryLocked(symbol, rec.ClientOrderID)
		}
		b.Mu.Unlock()

		if needPlanExecution {
			p := plan
			b.goAsync(func() { b.executeProtectionPlan(p) })
		}
		return
	}

	// SKENARIO 2: Update Order Exit
	if status == model.StatusPartiallyFilled && deltaFilled.GreaterThan(decimal.Zero) {
		if b.ExitRealizedProfits == nil {
			b.ExitRealizedProfits = make(map[string]decimal.Decimal)
		}
		if b.ExitFeesUSDT == nil {
			b.ExitFeesUSDT = make(map[string]decimal.Decimal)
		}
		b.ExitRealizedProfits[symbol] = b.ExitRealizedProfits[symbol].Add(realizedProfit)
		if evt.Commission.GreaterThan(decimal.Zero) && isUSDCommissionAsset(evt.CommissionAsset) {
			b.ExitFeesUSDT[symbol] = b.ExitFeesUSDT[symbol].Add(evt.Commission)
		} else if evt.HasFillMeta && avgPrice.GreaterThan(decimal.Zero) {
			chunkQty := evt.LastFilledQty
			if chunkQty.LessThanOrEqual(decimal.Zero) {
				chunkQty = deltaFilled
			}
			rate := b.effectiveFeeRate(evt.IsMaker)
			b.ExitFeesUSDT[symbol] = b.ExitFeesUSDT[symbol].Add(chunkQty.Mul(avgPrice).Mul(decimal.NewFromFloat(rate)))
		}
		b.Mu.Unlock()
		return
	}

	var tradeRec *TradeClosedRecord
	var algoOrdersToCancel []int64
	var cancelSymbol string
	var tp1OrderToCancel int64
	var tp1ClientOrderToCancel string
	var entryOrdersToCancel []model.LiveOrderRecord
	if status == model.StatusFilled {
		prot, hasProt := b.Protections[symbol]
		isTrackedSL := hasProt && prot != nil && ((exchangeOrderID != 0 && exchangeOrderID == prot.SLOrderID) || (clientOrderID != "" && clientOrderID == prot.SLClientAlgoID))
		isTrackedTP := hasProt && prot != nil && ((exchangeOrderID != 0 && exchangeOrderID == prot.TPOrderID) || (clientOrderID != "" && clientOrderID == prot.TPClientAlgoID))
		if !isTrackedSL && rec != nil && !rec.IsEntry && rec.SLPrice.GreaterThan(decimal.Zero) &&
			(strings.HasPrefix(rec.ClientOrderID, "ALGO_SL_") || strings.HasPrefix(rec.ClientOrderID, "ALGOSL_") || strings.HasPrefix(rec.ClientOrderID, "live_sl")) {
			isTrackedSL = true
		}
		if !isTrackedTP && rec != nil && !rec.IsEntry && rec.TPPrice.GreaterThan(decimal.Zero) &&
			(strings.HasPrefix(rec.ClientOrderID, "ALGO_TP_") || strings.HasPrefix(rec.ClientOrderID, "ALGOTP_")) {
			isTrackedTP = true
		}
		isSystemClose := isTrackedSL || isTrackedTP || b.ClosingInFlight[symbol] || strings.HasPrefix(clientOrderID, "live_emg_")
		prevPosQty, hadPos := b.ActivePositions[symbol]

		// Jika close eksternal (misal manual partial close di UI Binance), tangani ketiga kasus pengurutan event:
		// 1) ACCOUNT_UPDATE tiba lebih dulu dengan pengurangan >50% atau <=50% sementara posisi tersisa > 0,
		// 2) ORDER_TRADE_UPDATE tiba lebih dulu sehingga prevPosQty > deltaSub,
		// 3) Ambigu (prevPosQty < deltaSub tanpa jejak LastAccountPosPrevQty) -> konfirmasi REST PositionRisk dulu sebelum membatalkan SL/TP.
		if !isSystemClose && hadPos && cumFilledQty.GreaterThan(decimal.Zero) {
			deltaSub := deltaFilled
			if deltaSub.LessThanOrEqual(decimal.Zero) {
				deltaSub = cumFilledQty
			}
			lastPrev := b.LastAccountPosPrevQty[symbol]
			if prevPosQty.GreaterThan(decimal.Zero) && lastPrev.GreaterThan(prevPosQty) && lastPrev.Sub(prevPosQty).GreaterThanOrEqual(deltaSub) {
				newPrev := lastPrev.Sub(deltaSub)
				if newPrev.GreaterThan(prevPosQty) {
					b.LastAccountPosPrevQty[symbol] = newPrev
				} else {
					delete(b.LastAccountPosPrevQty, symbol)
				}
				if hasProt && prot != nil {
					prot.ProtectedQty = prevPosQty
				}
				delete(b.OrdersByClient, rec.ClientOrderID)
				if exchangeOrderID != 0 {
					delete(b.OrdersByExchangeID, exchangeOrderID)
				}
				b.notifyPositionSavedLocked(symbol)
				b.Mu.Unlock()
				return
			}
			if prevPosQty.GreaterThan(deltaSub) {
				remAfterManual := prevPosQty.Sub(deltaSub)
				if remAfterManual.GreaterThan(decimal.Zero) {
					b.ActivePositions[symbol] = remAfterManual
					if hasProt && prot != nil {
						prot.ProtectedQty = remAfterManual
					}
					delete(b.OrdersByClient, rec.ClientOrderID)
					if exchangeOrderID != 0 {
						delete(b.OrdersByExchangeID, exchangeOrderID)
					}
					b.notifyPositionSavedLocked(symbol)
					b.Mu.Unlock()
					return
				}
			}
			if prevPosQty.GreaterThan(decimal.Zero) && prevPosQty.LessThan(deltaSub) && b.ConfirmPositionZeroFn != nil {
				confirmFn := b.ConfirmPositionZeroFn
				fetchFn := b.FetchAccountFn
				recClientID := rec.ClientOrderID
				b.Mu.Unlock()

				cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
				isZero, cErr := confirmFn(cCtx, symbol)
				cCancel()
				if cErr != nil || !isZero {
					slog.Warn("live: manual close size exceeded local qty; REST confirmation shows open/unknown position so retaining SL/TP",
						"symbol", symbol, "prevPosQty", prevPosQty, "cumFilledQty", cumFilledQty, "isZero", isZero, "err", cErr)
					b.Mu.Lock()
					delete(b.OrdersByClient, recClientID)
					if exchangeOrderID != 0 {
						delete(b.OrdersByExchangeID, exchangeOrderID)
					}
					b.Mu.Unlock()
					if fetchFn != nil {
						b.goAsync(func() {
							rCtx, rCancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer rCancel()
							_ = b.RefreshAccount(rCtx)
						})
					}
					return
				}

				b.Mu.Lock()
				prot, hasProt = b.Protections[symbol]
				prevPosQty, hadPos = b.ActivePositions[symbol]
				if !hadPos && !hasProt {
					b.Mu.Unlock()
					return
				}
			}
		}

		delete(b.ActivePositions, symbol)
		delete(b.PositionSides, symbol)
		openTime := b.PositionOpenTimes[symbol]
		delete(b.PositionOpenTimes, symbol)
		cancelSymbol = symbol
		if hasProt && prot != nil {
			if prot.TP1OrderID != 0 {
				tp1OrderToCancel = prot.TP1OrderID
				tp1ClientOrderToCancel = prot.TP1ClientOrderID
			}
			var exitReason string
			currentTier := b.ProtectionTiers[symbol]
			if isTrackedSL && prot.TPOrderID != 0 {
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
			} else if isTrackedTP && prot.SLOrderID != 0 {
				algoOrdersToCancel = append(algoOrdersToCancel, prot.SLOrderID)
				exitReason = "TAKE_PROFIT"
			} else if isTrackedSL {
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
			} else if isTrackedTP {
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
				if prot.SLOrderID != 0 {
					algoOrdersToCancel = append(algoOrdersToCancel, prot.SLOrderID)
				}
				if prot.TPOrderID != 0 {
					algoOrdersToCancel = append(algoOrdersToCancel, prot.TPOrderID)
				}
			}

			entryPrice := b.PositionEntryPrices[symbol]
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

			closedQty := cumFilledQty
			if closedQty.LessThanOrEqual(decimal.Zero) {
				closedQty = rec.TargetQty
			}

			grossRP := b.ExitRealizedProfits[symbol].Add(realizedProfit)
			if grossRP.IsZero() && entryPrice.GreaterThan(decimal.Zero) && exitPrice.GreaterThan(decimal.Zero) {
				if prot.Side == model.SideLong {
					grossRP = exitPrice.Sub(entryPrice).Mul(closedQty)
				} else {
					grossRP = entryPrice.Sub(exitPrice).Mul(closedQty)
				}
			}

			initQty := b.InitialQtys[symbol]
			if initQty.LessThanOrEqual(decimal.Zero) {
				initQty = closedQty
			}
			var entryFeePortion, exitFee decimal.Decimal
			if totalEntryFee := b.EntryFeesUSDT[symbol]; totalEntryFee.GreaterThan(decimal.Zero) && initQty.GreaterThan(decimal.Zero) {
				ratio := closedQty.Div(initQty)
				if ratio.GreaterThan(decimal.NewFromInt(1)) {
					ratio = decimal.NewFromInt(1)
				}
				entryFeePortion = totalEntryFee.Mul(ratio)
			} else if evt.HasFillMeta && entryPrice.GreaterThan(decimal.Zero) {
				wasMaker := b.EntryWasMaker[symbol]
				entryFeePortion = entryPrice.Mul(closedQty).Mul(decimal.NewFromFloat(b.effectiveFeeRate(wasMaker)))
			}

			exitComm := b.ExitFeesUSDT[symbol]
			if deltaFilled.GreaterThan(decimal.Zero) {
				if evt.Commission.GreaterThan(decimal.Zero) && isUSDCommissionAsset(evt.CommissionAsset) {
					exitComm = exitComm.Add(evt.Commission)
				} else if exitComm.GreaterThan(decimal.Zero) && exitPrice.GreaterThan(decimal.Zero) {
					// Chunk sebelumnya terestimasi tetapi chunk terakhir memakai aset non-USD (misal BNB): lengkapi estimasi delta terakhir.
					exitComm = exitComm.Add(exitPrice.Mul(deltaFilled).Mul(decimal.NewFromFloat(b.effectiveFeeRate(evt.IsMaker))))
				}
			}
			if exitComm.GreaterThan(decimal.Zero) {
				exitFee = exitComm
			} else if evt.HasFillMeta && exitPrice.GreaterThan(decimal.Zero) {
				exitFee = exitPrice.Mul(closedQty).Mul(decimal.NewFromFloat(b.effectiveFeeRate(evt.IsMaker)))
			}

			netPnL := grossRP.Sub(entryFeePortion).Sub(exitFee)

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
				StrategyID:    normalizeBrokerStrategyID(b.PositionStrategyIDs[symbol]),
				Qty:           closedQty,
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
		}
		entryOrdersToCancel = b.cleanupClosedPositionLocked(symbol)
		delete(b.OrdersByClient, rec.ClientOrderID)
		delete(b.OrdersByExchangeID, exchangeOrderID)
	}
	b.Mu.Unlock()

	if tradeRec != nil {
		b.notifyPositionDeleted(tradeRec.Symbol)
		if b.OnCooldownRecordedFn != nil {
			b.OnCooldownRecordedFn(tradeRec.Symbol, tradeRec.ClosedAt, tradeRec.ExitReason)
		}
		if b.OnTradeClosedFn != nil {
			recCopy := *tradeRec
			cb := b.OnTradeClosedFn
			b.goAsync(func() { cb(recCopy) })
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

	if len(entryOrdersToCancel) > 0 && b.CancelOrderFn != nil {
		for _, ord := range entryOrdersToCancel {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
			cancel()
		}
	}
}

func (b *BinanceLiveBroker) effectiveFeeRate(isMaker bool) float64 {
	if isMaker {
		if b.Cfg.MakerFeeRate > 0 {
			return b.Cfg.MakerFeeRate
		}
		return 0.0002
	}
	if b.Cfg.TakerFeeRate > 0 {
		return b.Cfg.TakerFeeRate
	}
	return 0.0005
}

func (b *BinanceLiveBroker) planProtectionSyncLocked(entryRec *model.LiveOrderRecord) (model.ProtectionPlanAction, bool) {
	alreadyTP1 := b.TP1FilledQtys[entryRec.Symbol].Add(b.TP1CommittedQtys[entryRec.Symbol])
	targetQty := entryRec.CumulativeFilledQty.Sub(alreadyTP1)
	if targetQty.LessThanOrEqual(decimal.Zero) {
		if curPos := b.ActivePositions[entryRec.Symbol]; curPos.GreaterThan(decimal.Zero) {
			targetQty = curPos
		} else {
			return model.ProtectionPlanAction{}, false
		}
	}

	prot, exists := b.Protections[entryRec.Symbol]
	if exists && prot.SLStatus == model.ProtectionAcked && prot.ProtectedQty.Equal(targetQty) {
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

	needTP := !b.Cfg.PartialTPEnabled || !b.Cfg.TrailingEnabled
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
		TargetQty:         targetQty,
		SLPrice:           entryRec.SLPrice,
		TPPrice:           entryRec.TPPrice,
		ClientSLAlgoID:    slAlgoClientID,
		ClientTPAlgoID:    tpAlgoClientID,
	}

	// Persiapan resting Limit Maker TP1 jika PartialTPEnabled dan TP1Price valid (SSOT protection.PlanPartialTP1Split)
	if b.Cfg.PartialTPEnabled && !b.IsTP1Hits[entryRec.Symbol] && entryRec.TP1Price.GreaterThan(decimal.Zero) {
		var ef model.ExchangeFilter
		if b.GetExchangeFilterFn != nil {
			ef, _ = b.GetExchangeFilterFn(entryRec.Symbol)
		}
		splitPlan := protection.PlanPartialTP1Split(
			entryRec.CumulativeFilledQty, targetQty, alreadyTP1,
			entryRec.TP1Price, b.Cfg.PartialTP1Ratio, ef,
		)
		if splitPlan.SplitPossible {
			tp1ClientID := model.FormatClientID("ALGO_TP1", entryRec.Symbol, now)
			action.NeedNewTP1Limit = true
			action.TP1Qty = splitPlan.CloseQty
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
		Symbol:           entryRec.Symbol,
		Side:             entryRec.Side,
		PlanGeneration:   gen,
		ProtectedQty:     decimal.Zero,
		InitialQty:       entryRec.CumulativeFilledQty,
		SLOrderID:        oldSLID,
		SLStatus:         model.ProtectionPending,
		SLClientAlgoID:   slAlgoClientID,
		SLPrice:          slPrice,
		InitialSLPrice:   slPrice,
		TPOrderID:        oldTPID,
		TPStatus:         tpStatus,
		TPClientAlgoID:   tpAlgoClientID,
		TPPrice:          entryRec.TPPrice,
		TP1Price:         entryRec.TP1Price,
		TP1OrderID:       oldTP1ID,
		TP1ClientOrderID: oldTP1ClientID,
		IsTP1Hit:         false,
		LastAttemptTime:  now,
		SignalAuditID:    entryRec.SignalAuditID,
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
			if isImmediateTriggerError(slErr) {
				break
			}
			time.Sleep(time.Duration(attempt*300) * time.Millisecond)
		}
	}

	if slErr != nil || newSLOrderID == 0 {
		b.Mu.Lock()
		prot, protExists := b.Protections[action.Symbol]
		protGenMatches := protExists && prot != nil && prot.PlanGeneration == action.Generation
		if protGenMatches {
			prot.SLStatus = model.ProtectionFailed
			prot.LastError = fmt.Sprintf("SL placement failed after retries: %v", slErr)
		}
		hasOldSL := action.CancelSLID != 0
		posQty := b.ActivePositions[action.Symbol]
		recentClose := !b.LastClosingAttempt[action.Symbol].IsZero() && time.Since(b.LastClosingAttempt[action.Symbol]) < 10*time.Second
		shouldEmergencyClose := !hasOldSL && protGenMatches && posQty.GreaterThan(decimal.Zero) && !b.ClosingInFlight[action.Symbol] && !recentClose && b.EmergencyMarketCloseFn != nil
		if shouldEmergencyClose {
			b.ClosingInFlight[action.Symbol] = true
			b.ClosingReason[action.Symbol] = "EMERGENCY_CLOSE_SL_PLACEMENT_FAIL"
		}
		b.Mu.Unlock()

		if shouldEmergencyClose {
			defer func() {
				b.Mu.Lock()
				delete(b.ClosingInFlight, action.Symbol)
				b.Mu.Unlock()
			}()
			var closeErr error
			positionSide := model.OppositeSide(action.ExitSide)
			for closeAttempt := 1; closeAttempt <= 3; closeAttempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				closeErr = b.EmergencyMarketCloseFn(ctx, action.Symbol, positionSide, action.TargetQty, "CRITICAL: SL placement failed, emergency exit executed")
				cancel()
				if closeErr == nil {
					b.Mu.Lock()
					if b.LastClosingAttempt == nil {
						b.LastClosingAttempt = make(map[string]time.Time)
					}
					b.LastClosingAttempt[action.Symbol] = time.Now().UTC()
					b.Mu.Unlock()
					break
				}
				if closeAttempt < 3 {
					time.Sleep(time.Duration(closeAttempt*250) * time.Millisecond)
				}
			}
			if closeErr != nil {
				b.Mu.Lock()
				delete(b.ClosingReason, action.Symbol)
				b.Mu.Unlock()
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
	actualTP1ClientID := action.ClientTP1ID
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
			var usedCID string
			newTP1OrderID, usedCID, tp1Err = b.PlaceLimitCloseFn(ctx, action.Symbol, action.ExitSide, action.TP1Qty, tp1Price, action.ClientTP1ID)
			cancel()
			if tp1Err == nil && newTP1OrderID != 0 {
				if usedCID != "" {
					actualTP1ClientID = usedCID
				}
				break
			}
			time.Sleep(time.Duration(attempt*250) * time.Millisecond)
		}
		if tp1Err != nil {
			slog.Warn("live: resting TP1 limit placement failed; will rely on realtime tick market exit",
				"symbol", action.Symbol, "err", tp1Err)
		}
	}

	cancelNewlyPlacedOrphans := func() {
		slog.Warn("live: protection plan superseded or position closed; canceling newly placed orphan orders",
			"symbol", action.Symbol, "generation", action.Generation, "newSLID", newSLOrderID, "newTPID", newTPOrderID, "newTP1ID", newTP1OrderID)
		if newSLOrderID != 0 && b.CancelAlgoOrderFn != nil {
			ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelAlgoOrderFn(ctxC, action.Symbol, newSLOrderID)
			cancelC()
		}
		if newTPOrderID != 0 && b.CancelAlgoOrderFn != nil {
			ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelAlgoOrderFn(ctxC, action.Symbol, newTPOrderID)
			cancelC()
		}
		if newTP1OrderID != 0 && b.CancelOrderFn != nil {
			ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctxC, action.Symbol, newTP1OrderID, actualTP1ClientID)
			cancelC()
		}
	}

	// LANGKAH 3: Commit ID baru & status ke prot DI BAWAH LOCK TERLEBIH DAHULU sebelum membatalkan SL/TP lama.
	// Mencegah race ALGO_UPDATE(CANCELED) atas oldSLID/oldTPID menghapus ClientAlgoID baru dari prot!
	var oldSLClientID, oldTPClientID string
	b.Mu.Lock()
	prot, ok := b.Protections[action.Symbol]
	if !ok || prot == nil || prot.PlanGeneration != action.Generation {
		b.Mu.Unlock()
		cancelNewlyPlacedOrphans()
		return
	}

	if action.CancelSLID != 0 {
		if oldRec, exists := b.OrdersByExchangeID[action.CancelSLID]; exists && oldRec != nil {
			oldSLClientID = oldRec.ClientOrderID
		}
	}
	if action.CancelTPID != 0 {
		if oldRec, exists := b.OrdersByExchangeID[action.CancelTPID]; exists && oldRec != nil {
			oldTPClientID = oldRec.ClientOrderID
		}
	}

	prot.SLOrderID = newSLOrderID
	prot.SLClientAlgoID = action.ClientSLAlgoID
	prot.SLPrice = action.SLPrice
	prot.SLStatus = model.ProtectionAcked
	prot.SLRetryCount = 0
	prot.ProtectedQty = action.TargetQty

	if action.NeedNewTP {
		if tpErr != nil || newTPOrderID == 0 {
			prot.TPStatus = model.ProtectionFailed
			prot.LastError = fmt.Sprintf("TP placement failed: %v", tpErr)
			prot.LastTPAttempt = time.Now().UTC()
		} else {
			prot.TPOrderID = newTPOrderID
			prot.TPClientAlgoID = action.ClientTPAlgoID
			prot.TPPrice = action.TPPrice
			prot.TPStatus = model.ProtectionAcked
			prot.TPRetryCount = 0
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
		prot.TP1ClientOrderID = actualTP1ClientID
		b.OrdersByClient[actualTP1ClientID] = &model.LiveOrderRecord{
			ClientOrderID:   actualTP1ClientID,
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
		b.OrdersByExchangeID[newTP1OrderID] = b.OrdersByClient[actualTP1ClientID]
	} else if action.CancelTP1ID != 0 && prot.TP1OrderID == action.CancelTP1ID {
		prot.TP1OrderID = 0
		prot.TP1ClientOrderID = ""
	}

	b.notifyPositionSavedLocked(action.Symbol)
	b.Mu.Unlock()

	// LANGKAH 4: Batalkan SL, TP, dan TP1 lama setelah ID baru aman ter-commit di prot
	if action.CancelSLID != 0 && action.CancelSLID != newSLOrderID && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, action.Symbol, action.CancelSLID)
		cancel()
	}
	if action.CancelTPID != 0 && action.CancelTPID != newTPOrderID && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, action.Symbol, action.CancelTPID)
		cancel()
	}
	if action.CancelTP1ID != 0 && action.CancelTP1ID != newTP1OrderID && b.CancelOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelOrderFn(ctx, action.Symbol, action.CancelTP1ID, action.CancelTP1ClientID)
		cancel()
	}

	// Bersihkan mapping lama setelah pembatalan selesai
	if (action.CancelSLID != 0 && action.CancelSLID != newSLOrderID) ||
		(action.CancelTPID != 0 && action.CancelTPID != newTPOrderID) ||
		(action.CancelTP1ID != 0 && action.CancelTP1ID != newTP1OrderID) {
		b.Mu.Lock()
		if action.CancelSLID != 0 && action.CancelSLID != newSLOrderID {
			delete(b.OrdersByExchangeID, action.CancelSLID)
		}
		if oldSLClientID != "" && oldSLClientID != action.ClientSLAlgoID {
			delete(b.OrdersByClient, oldSLClientID)
		}
		if action.CancelTPID != 0 && action.CancelTPID != newTPOrderID {
			delete(b.OrdersByExchangeID, action.CancelTPID)
		}
		if oldTPClientID != "" && oldTPClientID != action.ClientTPAlgoID {
			delete(b.OrdersByClient, oldTPClientID)
		}
		if action.CancelTP1ID != 0 && action.CancelTP1ID != newTP1OrderID {
			delete(b.OrdersByExchangeID, action.CancelTP1ID)
		}
		if action.CancelTP1ClientID != "" && action.CancelTP1ClientID != actualTP1ClientID {
			delete(b.OrdersByClient, action.CancelTP1ClientID)
		}
		b.Mu.Unlock()
	}
}

func (b *BinanceLiveBroker) CheckPendingEntryTimeouts(now time.Time) {
	var expiredOrders []model.LiveOrderRecord

	b.Mu.Lock()
	for _, ord := range b.OrdersByClient {
		if ord != nil && ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew || ord.Status == model.StatusPartiallyFilled) {
			if now.Sub(ord.CreatedAt) > b.Cfg.LimitOrderTimeout {
				expiredOrders = append(expiredOrders, *ord)
			}
		}
	}
	b.Mu.Unlock()

	if len(expiredOrders) == 0 || (b.CancelOrderFn == nil && b.CancelOrderWithResultFn == nil) {
		return
	}

	for _, ord := range expiredOrders {
		var canceledRec *model.LiveOrderRecord
		var err error
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if b.CancelOrderWithResultFn != nil {
			canceledRec, err = b.CancelOrderWithResultFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
		} else {
			err = b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
		}
		cancel()

		if err == nil {
			var exchangeFilledQty, exchangeAvgPrice decimal.Decimal
			if canceledRec != nil {
				exchangeFilledQty = canceledRec.CumulativeFilledQty
				exchangeAvgPrice = canceledRec.AvgPrice
			} else if b.QueryOrderFn != nil && ord.CumulativeFilledQty.LessThanOrEqual(decimal.Zero) {
				qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
				qRec, qErr := b.QueryOrderFn(qCtx, ord.Symbol, ord.ClientOrderID)
				qCancel()
				if qErr == nil && qRec != nil && qRec.CumulativeFilledQty.GreaterThan(decimal.Zero) {
					exchangeFilledQty = qRec.CumulativeFilledQty
					exchangeAvgPrice = qRec.AvgPrice
				}
			}

			var plan model.ProtectionPlanAction
			var needPlanExec bool
			b.Mu.Lock()
			if rec, exists := b.OrdersByClient[ord.ClientOrderID]; exists {
				effectiveFilled := rec.CumulativeFilledQty
				if exchangeFilledQty.GreaterThan(effectiveFilled) {
					effectiveFilled = exchangeFilledQty
				}
				if effectiveFilled.LessThanOrEqual(decimal.Zero) && b.ActivePositions[ord.Symbol].GreaterThan(decimal.Zero) {
					effectiveFilled = b.ActivePositions[ord.Symbol]
				}

				effectiveAvg := exchangeAvgPrice
				if effectiveAvg.LessThanOrEqual(decimal.Zero) {
					effectiveAvg = rec.AvgPrice
				}
				if effectiveAvg.LessThanOrEqual(decimal.Zero) {
					effectiveAvg = b.PositionEntryPrices[ord.Symbol]
				}
				if effectiveAvg.LessThanOrEqual(decimal.Zero) {
					effectiveAvg = rec.LimitPrice
				}

				if effectiveFilled.LessThanOrEqual(decimal.Zero) {
					delete(b.OrdersByExchangeID, rec.ExchangeOrderID)
					b.rollbackReservedEntryLocked(ord.Symbol, ord.ClientOrderID)
				} else {
					rec.Status = model.StatusExpired
					rec.CumulativeFilledQty = effectiveFilled
					if effectiveAvg.GreaterThan(decimal.Zero) {
						rec.AvgPrice = effectiveAvg
					}
					b.InitialQtys[ord.Symbol] = effectiveFilled
					prot := b.Protections[ord.Symbol]
					needArm := prot == nil || prot.SLStatus == model.ProtectionFailed || prot.ProtectedQty.LessThan(effectiveFilled)
					if needArm {
						plan, needPlanExec = b.applyEntryFillLocked(rec, effectiveFilled, effectiveAvg, now)
					}
					delete(b.OrdersByClient, ord.ClientOrderID)
					delete(b.ReservedEntryMargins, ord.ClientOrderID)
					if rec.ExchangeOrderID != 0 {
						delete(b.OrdersByExchangeID, rec.ExchangeOrderID)
					}
				}
			}
			if ord.ExchangeOrderID != 0 {
				delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
			}
			b.Mu.Unlock()
			if needPlanExec {
				slog.Warn("live: timeout cancel discovered missed partial fill; arming SL/TP protections",
					"symbol", ord.Symbol, "clientOrderID", ord.ClientOrderID, "filledQty", plan.TargetQty)
				p := plan
				b.goAsync(func() { b.executeProtectionPlan(p) })
			}
			continue
		}

		if strings.Contains(err.Error(), "Unknown order") || strings.Contains(err.Error(), "-2011") {
			// Jika order sudah tidak ditemukan (-2011), periksa REST QueryOrder agar fill yang terjadi saat WS mati
			// tidak terbuang tanpa pemasangan proteksi SL/TP.
			if b.QueryOrderFn != nil {
				qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
				qRec, qErr := b.QueryOrderFn(qCtx, ord.Symbol, ord.ClientOrderID)
				qCancel()
				if qErr != nil && !isDefinitiveExchangeReject(qErr) {
					slog.Warn("live: QueryOrder failed ambiguously after -2011 timeout cancel; retaining entry record for retry",
						"symbol", ord.Symbol, "clientOrderID", ord.ClientOrderID, "err", qErr)
					continue
				}
				if qErr == nil && qRec != nil && qRec.CumulativeFilledQty.GreaterThan(decimal.Zero) {
					var refreshErr error
					if b.FetchAccountFn != nil {
						rCtx, rCancel := context.WithTimeout(context.Background(), 5*time.Second)
						refreshErr = b.RefreshAccount(rCtx)
						rCancel()
					}
					b.Mu.Lock()
					openQty := b.ActivePositions[ord.Symbol]
					_, hasProt := b.Protections[ord.Symbol]
					b.Mu.Unlock()
					if (b.FetchAccountFn == nil || openQty.GreaterThan(decimal.Zero)) && !hasProt {
						b.HandleUserDataOrderUpdate(
							qRec.ClientOrderID,
							qRec.ExchangeOrderID,
							qRec.Symbol,
							qRec.Status,
							qRec.CumulativeFilledQty,
							qRec.AvgPrice,
							decimal.Zero,
						)
					} else if b.FetchAccountFn != nil && refreshErr != nil {
						slog.Warn("live: RefreshAccount failed after -2011 filled entry query; retaining entry record for retry",
							"symbol", ord.Symbol, "clientOrderID", ord.ClientOrderID, "err", refreshErr)
						continue
					} else if b.FetchAccountFn != nil && refreshErr == nil && openQty.LessThanOrEqual(decimal.Zero) {
						// Terisi dan sudah tertutup selama offline: bersihkan order lokal agar tidak leak slot atau loop tiap 2 detik
						b.Mu.Lock()
						delete(b.OrdersByClient, ord.ClientOrderID)
						delete(b.ReservedEntryMargins, ord.ClientOrderID)
						if ord.ExchangeOrderID != 0 {
							delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
						}
						if qRec.ExchangeOrderID != 0 {
							delete(b.OrdersByExchangeID, qRec.ExchangeOrderID)
						}
						b.Mu.Unlock()
					} else if hasProt {
						var upgradePlan model.ProtectionPlanAction
						var needUpgrade bool
						b.Mu.Lock()
						if rec, exists := b.OrdersByClient[ord.ClientOrderID]; exists {
							prevFilled := rec.CumulativeFilledQty
							rec.Status = qRec.Status
							rec.CumulativeFilledQty = qRec.CumulativeFilledQty
							if qRec.AvgPrice.GreaterThan(decimal.Zero) {
								rec.AvgPrice = qRec.AvgPrice
							}
							if qRec.CumulativeFilledQty.GreaterThan(prevFilled) {
								upgradePlan, needUpgrade = b.applyEntryFillLocked(rec, qRec.CumulativeFilledQty, qRec.AvgPrice, now)
							}
							if qRec.Status == model.StatusFilled || qRec.Status == model.StatusCanceled || qRec.Status == model.StatusExpired {
								delete(b.ReservedEntryMargins, ord.ClientOrderID)
							}
							if qRec.Status == model.StatusCanceled || qRec.Status == model.StatusExpired {
								b.InitialQtys[ord.Symbol] = qRec.CumulativeFilledQty
								delete(b.OrdersByClient, ord.ClientOrderID)
								if rec.ExchangeOrderID != 0 {
									delete(b.OrdersByExchangeID, rec.ExchangeOrderID)
								}
							}
						}
						b.Mu.Unlock()
						if needUpgrade {
							p := upgradePlan
							b.goAsync(func() { b.executeProtectionPlan(p) })
						}
					}
					continue
				}
			}

			b.Mu.Lock()
			if rec, exists := b.OrdersByClient[ord.ClientOrderID]; exists {
				rec.Status = model.StatusExpired
				if rec.CumulativeFilledQty.GreaterThan(decimal.Zero) {
					b.InitialQtys[ord.Symbol] = rec.CumulativeFilledQty
				}
				delete(b.OrdersByClient, ord.ClientOrderID)
				delete(b.ReservedEntryMargins, ord.ClientOrderID)
				if rec.ExchangeOrderID != 0 {
					delete(b.OrdersByExchangeID, rec.ExchangeOrderID)
				}
			}
			if ord.ExchangeOrderID != 0 {
				delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
			}
			b.Mu.Unlock()
		}
	}
}

// CheckMaxHoldingTimeouts memeriksa dan menutup posisi live yang melebihi batas durasi holding maksimal atau stale-loss duration
func (b *BinanceLiveBroker) CheckMaxHoldingTimeouts(now time.Time) {
	if (b.Cfg.MaxHoldingDuration <= 0 && b.Cfg.StaleLossExitDuration <= 0) || b.EmergencyMarketCloseFn == nil {
		return
	}

	type closeTarget struct {
		symbol       string
		positionSide model.Side
		qty          decimal.Decimal
		reason       string
	}
	var targets []closeTarget

	b.Mu.Lock()
	for sym, openTime := range b.PositionOpenTimes {
		reason := ""
		if b.Cfg.MaxHoldingDuration > 0 && now.Sub(openTime) > b.Cfg.MaxHoldingDuration {
			reason = "TIME_STOP_MAX_HOLDING"
		} else if b.Cfg.StaleLossExitDuration > 0 {
			markDec := b.LastMarkPrices[sym]
			markAt := b.LastMarkUpdatedAt[sym]
			markFresh := b.Cfg.MaxMarkStaleness <= 0 || (!markAt.IsZero() && now.Sub(markAt) <= b.Cfg.MaxMarkStaleness)
			if markFresh && protection.ShouldTriggerStaleLossExit(b.PositionSides[sym], b.PositionEntryPrices[sym], markDec, openTime, now, b.Cfg.StaleLossExitDuration) {
				reason = "TIME_STOP_STALE_LOSS"
			}
		}
		if reason == "" {
			continue
		}
		if b.ClosingInFlight[sym] || (!b.LastClosingAttempt[sym].IsZero() && now.Sub(b.LastClosingAttempt[sym]) < 10*time.Second) {
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
			b.ClosingReason[sym] = reason
			targets = append(targets, closeTarget{
				symbol:       sym,
				positionSide: side,
				qty:          qty,
				reason:       reason,
			})
		}
	}
	b.Mu.Unlock()

	for _, tgt := range targets {
		var closeErr error
		for attempt := 1; attempt <= 3; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			closeErr = b.EmergencyMarketCloseFn(ctx, tgt.symbol, tgt.positionSide, tgt.qty, tgt.reason)
			cancel()
			if closeErr == nil {
				slog.Info("time-stop emergency close executed successfully", "symbol", tgt.symbol, "reason", tgt.reason, "attempt", attempt)
				break
			}
			slog.Warn("time-stop emergency close attempt failed", "symbol", tgt.symbol, "reason", tgt.reason, "attempt", attempt, "err", closeErr)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
		}

		// Reset flag in-flight; tahan retry berulang selama 10s jika sukses agar WS/REST sempat menyusul.
		b.Mu.Lock()
		delete(b.ClosingInFlight, tgt.symbol)
		if closeErr == nil {
			if b.LastClosingAttempt == nil {
				b.LastClosingAttempt = make(map[string]time.Time)
			}
			b.LastClosingAttempt[tgt.symbol] = time.Now().UTC()
		} else {
			delete(b.ClosingReason, tgt.symbol)
		}
		b.Mu.Unlock()

		if closeErr != nil {
			slog.Error("time-stop emergency close failed after 3 attempts", "symbol", tgt.symbol, "reason", tgt.reason, "err", closeErr)
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
		oldSLID        int64
		oldClientSLID  string
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
		if b.ClosingInFlight[sym] || (!b.LastClosingAttempt[sym].IsZero() && now.Sub(b.LastClosingAttempt[sym]) < 10*time.Second) {
			continue
		}
		// Jika retry sudah mencapai 3x, posisikan ke emergency close agar modal uang riil aman
		if prot.SLRetryCount >= 3 {
			posSide := b.PositionSides[sym]
			if posSide == "" {
				slog.Warn("live: cannot determine position side for SL repair emergency close, skipping", "symbol", sym)
				continue
			}
			b.ClosingInFlight[sym] = true
			b.ClosingReason[sym] = "EMERGENCY_CLOSE_SL_REPAIR_FAIL"
			b.goAsync(func() {
				s, q, pSide := sym, qty, posSide
				var closeErr error
				defer func() {
					b.Mu.Lock()
					delete(b.ClosingInFlight, s)
					if closeErr == nil {
						if b.LastClosingAttempt == nil {
							b.LastClosingAttempt = make(map[string]time.Time)
						}
						b.LastClosingAttempt[s] = time.Now().UTC()
					} else {
						delete(b.ClosingReason, s)
					}
					b.Mu.Unlock()
				}()
				if b.EmergencyMarketCloseFn != nil {
					for closeAttempt := 1; closeAttempt <= 3; closeAttempt++ {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						closeErr = b.EmergencyMarketCloseFn(ctx, s, pSide, q, "SL repair exceeded max retries; emergency close")
						cancel()
						if closeErr == nil {
							break
						}
						if closeAttempt < 3 {
							time.Sleep(time.Duration(closeAttempt*250) * time.Millisecond)
						}
					}
					if closeErr != nil {
						slog.Error("CRITICAL ALERT: EMERGENCY CLOSE FAILED", "symbol", s, "positionSide", pSide, "qty", q, "err", closeErr)
					}
				}
			})
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
		slTargetPrice := prot.SLPrice
		if slTargetPrice.LessThanOrEqual(decimal.Zero) {
			refPrice := b.PositionEntryPrices[sym]
			if refPrice.LessThanOrEqual(decimal.Zero) {
				refPrice = b.LastMarkPrices[sym]
			}
			if refPrice.LessThanOrEqual(decimal.Zero) {
				continue
			}
			minDist := decimal.NewFromFloat(0.015)
			if side == model.SideLong {
				slTargetPrice = refPrice.Mul(decimal.NewFromInt(1).Sub(minDist))
			} else {
				slTargetPrice = refPrice.Mul(decimal.NewFromInt(1).Add(minDist))
			}
			prot.SLPrice = slTargetPrice
			if prot.InitialSLPrice.LessThanOrEqual(decimal.Zero) {
				prot.InitialSLPrice = slTargetPrice
			}
			b.InitialSLPrices[sym] = slTargetPrice
		}

		prot.SLRetryCount++
		prot.LastAttemptTime = now

		clientAlgoID := model.FormatClientID("ALGOSL_R", sym, now)
		slTargets = append(slTargets, slRepairTarget{
			symbol:         sym,
			exitSide:       exitSide,
			qty:            qty,
			slPrice:        slTargetPrice,
			clientSLAlgoID: clientAlgoID,
			oldSLID:        prot.SLOrderID,
			oldClientSLID:  prot.SLClientAlgoID,
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
		prot.SLPrice = slPrice
		prot.ProtectedQty = tgt.qty
		prot.SLStatus = model.ProtectionAcked
		prot.SLRetryCount = 0
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
			SLPrice:         slPrice,
			CreatedAt:       now,
		}
		b.OrdersByExchangeID[newSLID] = b.OrdersByClient[tgt.clientSLAlgoID]
		b.notifyPositionSavedLocked(tgt.symbol)
		b.Mu.Unlock()

		// Batalkan SL lama bila masih tersisa setelah SL baru terpasang, lalu bersihkan map lama.
		if tgt.oldSLID != 0 && tgt.oldSLID != newSLID && b.CancelAlgoOrderFn != nil {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelAlgoOrderFn(cCtx, tgt.symbol, tgt.oldSLID)
			cCancel()
		}
		if (tgt.oldSLID != 0 && tgt.oldSLID != newSLID) || (tgt.oldClientSLID != "" && tgt.oldClientSLID != tgt.clientSLAlgoID) {
			b.Mu.Lock()
			if tgt.oldSLID != 0 && tgt.oldSLID != newSLID {
				delete(b.OrdersByExchangeID, tgt.oldSLID)
			}
			if tgt.oldClientSLID != "" && tgt.oldClientSLID != tgt.clientSLAlgoID {
				delete(b.OrdersByClient, tgt.oldClientSLID)
			}
			b.Mu.Unlock()
		}

		slog.Info("SL repair successfully placed stop loss order", "symbol", tgt.symbol, "slOrderID", newSLID, "price", slPrice)
	}

	// 2. Repair Take Profit
	type tpRepairTarget struct {
		symbol         string
		exitSide       model.Side
		qty            decimal.Decimal
		tpPrice        decimal.Decimal
		clientTPAlgoID string
		oldTPID        int64
		oldClientTPID  string
		generation     uint64
	}
	var targets []tpRepairTarget

	b.Mu.Lock()
	for sym, prot := range b.Protections {
		if b.Cfg.PartialTPEnabled && b.Cfg.TrailingEnabled {
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
			oldTPID:        prot.TPOrderID,
			oldClientTPID:  prot.TPClientAlgoID,
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

		// Sukses: update TPStatus menjadi ProtectionAcked beserta TPClientAlgoID & harga terkuantisasi
		prot.TPOrderID = newTPID
		prot.TPClientAlgoID = tgt.clientTPAlgoID
		prot.TPPrice = tpPrice
		prot.TPStatus = model.ProtectionAcked
		prot.TPRetryCount = 0
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
			TPPrice:         tpPrice,
			CreatedAt:       now,
		}
		b.OrdersByExchangeID[newTPID] = b.OrdersByClient[tgt.clientTPAlgoID]
		b.notifyPositionSavedLocked(tgt.symbol)
		b.Mu.Unlock()

		if tgt.oldTPID != 0 && tgt.oldTPID != newTPID && b.CancelAlgoOrderFn != nil {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelAlgoOrderFn(cCtx, tgt.symbol, tgt.oldTPID)
			cCancel()
		}
		if (tgt.oldTPID != 0 && tgt.oldTPID != newTPID) || (tgt.oldClientTPID != "" && tgt.oldClientTPID != tgt.clientTPAlgoID) {
			b.Mu.Lock()
			if tgt.oldTPID != 0 && tgt.oldTPID != newTPID {
				delete(b.OrdersByExchangeID, tgt.oldTPID)
			}
			if tgt.oldClientTPID != "" && tgt.oldClientTPID != tgt.clientTPAlgoID {
				delete(b.OrdersByClient, tgt.oldClientTPID)
			}
			b.Mu.Unlock()
		}

		slog.Info("TP repair successfully restored take-profit order", "symbol", tgt.symbol, "tpOrderID", newTPID, "price", tpPrice)
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
	b.PeakEffectiveEquityUSDT, _ = protection.UpdatePeakAndDrawdownPct(b.PeakEffectiveEquityUSDT, effectiveEquity)
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
	b.recordTP1PartialChunkWithFee(symbol, clientOrderID, cumFilledQty, avgPrice, decimal.Zero, "")
}

// deductTP1QtyLocked menghitung sisa qty posisi secara SSOT setelah potongan TP1 tanpa double deduction
// bila ACCOUNT_UPDATE / RefreshAccount tiba lebih awal daripada event ORDER_TRADE_UPDATE.
func (b *BinanceLiveBroker) deductTP1QtyLocked(
	symbol string,
	prevCommitted, prevFilled, newCumFilled decimal.Decimal,
) (remQty, recordedCum decimal.Decimal, hasPos bool, hasProt bool) {
	curQty, okPos := b.ActivePositions[symbol]
	_, okProt := b.Protections[symbol]
	if (!okPos || curQty.LessThanOrEqual(decimal.Zero)) && !okProt {
		return decimal.Zero, newCumFilled, false, false
	}

	initQty := b.InitialQtys[symbol]
	deductQty := newCumFilled
	if prevFilled.GreaterThan(decimal.Zero) {
		deductQty = newCumFilled.Sub(prevFilled)
		if deductQty.IsNegative() {
			deductQty = decimal.Zero
		}
	}
	recordedCum = newCumFilled
	if okPos && curQty.GreaterThan(decimal.Zero) && deductQty.GreaterThan(curQty) {
		deductQty = curQty
		if prevFilled.GreaterThan(decimal.Zero) {
			recordedCum = prevFilled.Add(deductQty)
		} else {
			recordedCum = deductQty
		}
	}

	if !okPos || curQty.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, recordedCum, okPos, okProt
	}

	if initQty.GreaterThan(decimal.Zero) {
		expectedRem := initQty.Sub(prevCommitted.Add(recordedCum))
		if expectedRem.IsNegative() {
			expectedRem = decimal.Zero
		}
		if curQty.GreaterThan(expectedRem) {
			remQty = curQty.Sub(deductQty)
			if expectedRem.GreaterThan(decimal.Zero) && remQty.LessThan(expectedRem) {
				remQty = expectedRem
			}
		} else {
			remQty = curQty
		}
	} else {
		remQty = curQty.Sub(deductQty)
	}
	if remQty.IsNegative() {
		remQty = decimal.Zero
	}
	return remQty, recordedCum, okPos, okProt
}

func (b *BinanceLiveBroker) recordTP1PartialChunkWithFee(symbol, clientOrderID string, cumFilledQty, avgPrice, commission decimal.Decimal, commissionAsset string) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.IsTP1Hits[symbol] {
		return
	}
	prevFilled := b.TP1FilledQtys[symbol]
	if cumFilledQty.LessThanOrEqual(prevFilled) {
		return
	}

	remQty, recordedCum, hasPos, hasProt := b.deductTP1QtyLocked(symbol, b.TP1CommittedQtys[symbol], prevFilled, cumFilledQty)
	if !hasPos && !hasProt {
		return
	}
	if hasPos && b.ActivePositions[symbol].GreaterThan(decimal.Zero) {
		b.ActivePositions[symbol] = remQty
	}

	b.TP1FilledQtys[symbol] = recordedCum
	if commission.GreaterThan(decimal.Zero) && isUSDCommissionAsset(commissionAsset) {
		if b.TP1FeesUSDT == nil {
			b.TP1FeesUSDT = make(map[string]decimal.Decimal)
		}
		b.TP1FeesUSDT[symbol] = b.TP1FeesUSDT[symbol].Add(commission)
	}
	if avgPrice.GreaterThan(decimal.Zero) {
		b.TP1LastFillPrices[symbol] = avgPrice
	}

	if prot, ok := b.Protections[symbol]; ok && prot != nil && hasPos {
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

func (b *BinanceLiveBroker) finalizeTP1FullCloseLocked(
	symbol string,
	closedQty, exitPrice decimal.Decimal,
	clientOrderID string,
	isMaker bool,
) (TradeClosedRecord, []int64, []model.LiveOrderRecord) {
	var siblingAlgoIDs []int64
	if prot, ok := b.Protections[symbol]; ok && prot != nil {
		if prot.SLOrderID != 0 {
			siblingAlgoIDs = append(siblingAlgoIDs, prot.SLOrderID)
		}
		if prot.TPOrderID != 0 {
			siblingAlgoIDs = append(siblingAlgoIDs, prot.TPOrderID)
		}
	}
	tradeRec := b.buildTP1TradeRecordLocked(symbol, closedQty, exitPrice, clientOrderID, isMaker)
	tradeRec.ExitReason = "TAKE_PROFIT"
	entryOrdersToCancel := b.cleanupClosedPositionLocked(symbol)
	b.notifyPositionDeleted(symbol)
	return tradeRec, siblingAlgoIDs, entryOrdersToCancel
}

func (b *BinanceLiveBroker) dispatchTP1FullClose(
	tradeRec TradeClosedRecord,
	siblingAlgoIDs []int64,
	entryOrdersToCancel []model.LiveOrderRecord,
) {
	if b.OnCooldownRecordedFn != nil {
		b.OnCooldownRecordedFn(tradeRec.Symbol, tradeRec.ClosedAt, tradeRec.ExitReason)
	}
	if b.OnTradeClosedFn != nil {
		cb := b.OnTradeClosedFn
		b.goAsync(func() { cb(tradeRec) })
	}
	if b.CancelAlgoOrderFn != nil {
		for _, algoID := range siblingAlgoIDs {
			ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelAlgoOrderFn(ctxC, tradeRec.Symbol, algoID)
			cancelC()
		}
	}
	if b.CancelOrderFn != nil {
		for _, ord := range entryOrdersToCancel {
			ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctxC, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
			cancelC()
		}
	}
}

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

	feeRate := b.effectiveFeeRate(isMaker)
	exitFee := b.TP1FeesUSDT[symbol]
	delete(b.TP1FeesUSDT, symbol)
	if exitFee.LessThanOrEqual(decimal.Zero) {
		exitFee = exitPrice.Mul(closedQty).Mul(decimal.NewFromFloat(feeRate))
	}

	initQty := b.InitialQtys[symbol]
	var entryFee decimal.Decimal
	if totalEntryFee := b.EntryFeesUSDT[symbol]; totalEntryFee.GreaterThan(decimal.Zero) && initQty.GreaterThan(decimal.Zero) {
		ratio := closedQty.Div(initQty)
		if ratio.GreaterThan(decimal.NewFromInt(1)) {
			ratio = decimal.NewFromInt(1)
		}
		entryFee = totalEntryFee.Mul(ratio)
	} else {
		wasMaker, tracked := b.EntryWasMaker[symbol]
		entryIsMaker := (tracked && wasMaker) || (!tracked && strings.EqualFold(b.Cfg.EntryOrderType, "LIMIT"))
		entryFee = ePrice.Mul(closedQty).Mul(decimal.NewFromFloat(b.effectiveFeeRate(entryIsMaker)))
	}
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
		StrategyID:    normalizeBrokerStrategyID(b.PositionStrategyIDs[symbol]),
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

func isImmediateTriggerError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "-2021") || strings.Contains(msg, "immediately trigger")
}

// HandleAlgoOrderUpdate memproses event ALGO_UPDATE dari Binance User Data Stream.
// Jika SL/TP algo order dibatalkan/kadaluarsa/ditolak saat posisi aktif masih ada, tandai FAILED agar segera diperbaiki.
func (b *BinanceLiveBroker) HandleAlgoOrderUpdate(evt model.AlgoOrderUpdateEvent) {
	if evt.Symbol == "" {
		return
	}
	status := strings.ToUpper(strings.TrimSpace(evt.AlgoStatus))
	if status != "CANCELED" && status != "EXPIRED" && status != "REJECTED" {
		return
	}

	b.Mu.Lock()
	defer b.Mu.Unlock()

	qty, hasPos := b.ActivePositions[evt.Symbol]
	if !hasPos || qty.LessThanOrEqual(decimal.Zero) || b.ClosingInFlight[evt.Symbol] {
		return
	}
	prot, hasProt := b.Protections[evt.Symbol]
	if !hasProt || prot == nil {
		return
	}

	changed := false
	if (evt.AlgoID != 0 && evt.AlgoID == prot.SLOrderID) || (evt.ClientAlgoID != "" && evt.ClientAlgoID == prot.SLClientAlgoID) {
		slog.Warn("live: ALGO_UPDATE reported SL order inactive on exchange; marking ProtectionFailed for repair",
			"symbol", evt.Symbol, "algoID", evt.AlgoID, "clientAlgoID", evt.ClientAlgoID, "status", status)
		prot.SLOrderID = 0
		prot.SLClientAlgoID = ""
		prot.SLStatus = model.ProtectionFailed
		prot.LastAttemptTime = time.Time{}
		prot.LastError = fmt.Sprintf("algo SL %s via ALGO_UPDATE", status)
		changed = true
	} else if (evt.AlgoID != 0 && evt.AlgoID == prot.TPOrderID) || (evt.ClientAlgoID != "" && evt.ClientAlgoID == prot.TPClientAlgoID) {
		slog.Warn("live: ALGO_UPDATE reported TP order inactive on exchange; marking TP ProtectionFailed for repair",
			"symbol", evt.Symbol, "algoID", evt.AlgoID, "clientAlgoID", evt.ClientAlgoID, "status", status)
		prot.TPOrderID = 0
		prot.TPClientAlgoID = ""
		prot.TPStatus = model.ProtectionFailed
		prot.LastTPAttempt = time.Time{}
		prot.LastError = fmt.Sprintf("algo TP %s via ALGO_UPDATE", status)
		changed = true
	}

	if changed {
		if evt.ClientAlgoID != "" {
			delete(b.OrdersByClient, evt.ClientAlgoID)
		}
		if evt.AlgoID != 0 {
			delete(b.OrdersByExchangeID, evt.AlgoID)
		}
		b.notifyPositionSavedLocked(evt.Symbol)
	}
}

type slReplaceRequest struct {
	symbol             string
	exitSide           model.Side
	targetQty          decimal.Decimal
	targetSL           decimal.Decimal
	clientSLAlgoID     string
	oldSLID            int64
	oldClientSLID      string
	generation         uint64
	targetTier         int
	maxAttempts        int
	markFailedOnErr    bool
	contextLabel       string
	attemptTime        time.Time
	onDone             func()
}

func (b *BinanceLiveBroker) replaceStopLossAsync(req slReplaceRequest) {
	if b.PlaceAlgoOrderFn == nil {
		if req.onDone != nil {
			req.onDone()
		}
		return
	}
	attempts := req.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	b.goAsync(func() {
		if req.onDone != nil {
			defer req.onDone()
		}
		var newID int64
		var slErr error
		for attempt := 1; attempt <= attempts; attempt++ {
			ctxSL, cancelSL := context.WithTimeout(context.Background(), 5*time.Second)
			newID, slErr = b.PlaceAlgoOrderFn(ctxSL, req.symbol, req.exitSide, "STOP_MARKET", req.targetQty, req.targetSL, req.clientSLAlgoID)
			cancelSL()
			if slErr == nil && newID != 0 {
				break
			}
			if isImmediateTriggerError(slErr) {
				break
			}
			if attempt < attempts {
				time.Sleep(time.Duration(attempt*250) * time.Millisecond)
			}
		}

		if slErr == nil && newID != 0 {
			committed := false
			b.Mu.Lock()
			if p, ok := b.Protections[req.symbol]; ok && p.PlanGeneration == req.generation {
				p.SLOrderID = newID
				p.SLPrice = req.targetSL
				p.SLClientAlgoID = req.clientSLAlgoID
				p.SLStatus = model.ProtectionAcked
				p.SLRetryCount = 0
				b.LastSentSLPrices[req.symbol] = req.targetSL
				if req.targetTier > b.ProtectionTiers[req.symbol] {
					b.ProtectionTiers[req.symbol] = req.targetTier
				}
				b.OrdersByClient[req.clientSLAlgoID] = &model.LiveOrderRecord{
					ClientOrderID:   req.clientSLAlgoID,
					ExchangeOrderID: newID,
					Symbol:          req.symbol,
					Side:            req.exitSide,
					TargetQty:       req.targetQty,
					SLPrice:         req.targetSL,
					Status:          model.StatusNew,
					IsEntry:         false,
					IsAlgo:          true,
					CreatedAt:       time.Now().UTC(),
				}
				b.OrdersByExchangeID[newID] = b.OrdersByClient[req.clientSLAlgoID]
				b.notifyPositionSavedLocked(req.symbol)
				committed = true
			}
			b.Mu.Unlock()

			if committed {
				if req.oldSLID != 0 && req.oldSLID != newID && b.CancelAlgoOrderFn != nil {
					ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
					_ = b.CancelAlgoOrderFn(ctxC, req.symbol, req.oldSLID)
					cancelC()
				}
				// Hapus SL lama dari tracking map HANYA SETELAH pembatalan selesai agar fill balapan tetap dikenali sebagai SL.
				if (req.oldSLID != 0 && req.oldSLID != newID) || (req.oldClientSLID != "" && req.oldClientSLID != req.clientSLAlgoID) {
					b.Mu.Lock()
					if req.oldSLID != 0 && req.oldSLID != newID {
						delete(b.OrdersByExchangeID, req.oldSLID)
					}
					if req.oldClientSLID != "" && req.oldClientSLID != req.clientSLAlgoID {
						delete(b.OrdersByClient, req.oldClientSLID)
					}
					b.Mu.Unlock()
				}
				slog.Info("live: protection SL updated",
					"symbol", req.symbol, "context", req.contextLabel, "tier", req.targetTier, "newSL", req.targetSL, "algoID", newID)
			} else if b.CancelAlgoOrderFn != nil {
				slog.Warn("live: replacement SL superseded or position closed; canceling orphan SL",
					"symbol", req.symbol, "context", req.contextLabel, "orphanSLID", newID, "generation", req.generation)
				ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
				_ = b.CancelAlgoOrderFn(ctxC, req.symbol, newID)
				cancelC()
			}
			return
		}

		b.Mu.Lock()
		if b.LastAlgoFailTimes == nil {
			b.LastAlgoFailTimes = make(map[string]time.Time)
		}
		failAt := req.attemptTime
		if failAt.IsZero() {
			failAt = time.Now().UTC()
		}
		b.LastAlgoFailTimes[req.symbol] = failAt
		if req.markFailedOnErr {
			if p, ok := b.Protections[req.symbol]; ok && p.PlanGeneration == req.generation {
				if isImmediateTriggerError(slErr) && req.oldSLID != 0 {
					// Harga sudah menembus target SL (misal BE SL -2021): jangan timpa p.SLPrice atau buang SL lama.
					slog.Warn("live: replacement SL rejected -2021 (price crossed trigger); retaining previous SL",
						"symbol", req.symbol, "context", req.contextLabel, "targetSL", req.targetSL, "retainedSL", p.SLPrice)
				} else {
					p.SLStatus = model.ProtectionFailed
					p.SLPrice = req.targetSL
					p.LastError = fmt.Sprintf("%s SL placement failed: %v", req.contextLabel, slErr)
					p.LastAttemptTime = time.Now().UTC()
				}
			}
		}
		b.Mu.Unlock()
		slog.Error("live: failed to place replacement SL",
			"symbol", req.symbol, "context", req.contextLabel, "targetSL", req.targetSL, "err", slErr)
	})
}

func (b *BinanceLiveBroker) processTP1Executed(
	symbol string,
	actualCloseQty, actualExitPrice decimal.Decimal,
	clientOrderID string,
	isMaker bool,
) {
	b.processTP1ExecutedWithFee(symbol, actualCloseQty, actualExitPrice, clientOrderID, isMaker, decimal.Zero, "")
}

// processTP1ExecutedWithFee memproses eksekusi TP1 beserta komisi WS aktual, memperbarui runner lot,
// dan menggeser SL ke Break-Even secara asinkron tanpa memblokir read-loop WS.
func (b *BinanceLiveBroker) processTP1ExecutedWithFee(
	symbol string,
	actualCloseQty, actualExitPrice decimal.Decimal,
	clientOrderID string,
	isMaker bool,
	commission decimal.Decimal,
	commissionAsset string,
) {
	b.Mu.Lock()
	if b.IsTP1Hits[symbol] {
		b.Mu.Unlock()
		return
	}
	b.IsTP1Hits[symbol] = true
	delete(b.TP1InFlight, symbol)

	// Lepaskan dan batalkan sisa order entry limit yang belum terisi agar runner tidak bertambah tanpa SL.
	pendingEntriesToCancel := b.collectAndDetachPendingEntriesLocked(symbol)

	prevFilled := b.TP1FilledQtys[symbol]
	prevPrice := b.TP1LastFillPrices[symbol]
	prevCommitted := b.TP1CommittedQtys[symbol]
	delete(b.TP1FilledQtys, symbol)
	delete(b.TP1LastFillPrices, symbol)
	delete(b.TP1CommittedQtys, symbol)

	curQty, hasPos := b.ActivePositions[symbol]
	_, hasProt := b.Protections[symbol]
	if (!hasPos || curQty.LessThanOrEqual(decimal.Zero)) && !hasProt {
		b.Mu.Unlock()
		b.cancelEntryOrdersAsync(pendingEntriesToCancel)
		return
	}

	if commission.GreaterThan(decimal.Zero) && isUSDCommissionAsset(commissionAsset) {
		if b.TP1FeesUSDT == nil {
			b.TP1FeesUSDT = make(map[string]decimal.Decimal)
		}
		b.TP1FeesUSDT[symbol] = b.TP1FeesUSDT[symbol].Add(commission)
	}

	ratio := b.Cfg.PartialTP1Ratio
	if ratio <= 0.05 || ratio >= 0.95 {
		ratio = 0.50
	}
	initQty := b.InitialQtys[symbol]
	if actualCloseQty.IsZero() {
		baseQty := curQty
		if baseQty.LessThanOrEqual(decimal.Zero) {
			baseQty = initQty
		}
		actualCloseQty = baseQty.Mul(decimal.NewFromFloat(ratio))
	}
	if actualCloseQty.LessThanOrEqual(decimal.Zero) {
		b.Mu.Unlock()
		b.cancelEntryOrdersAsync(pendingEntriesToCancel)
		return
	}

	if actualExitPrice.IsZero() {
		if !prevPrice.IsZero() {
			actualExitPrice = prevPrice
		} else {
			actualExitPrice = b.TP1Prices[symbol]
		}
	}

	// Cegah double deduction secara SSOT jika ACCOUNT_UPDATE / RefreshAccount lebih dulu memotong ActivePositions.
	remQty, recordedCloseQty, _, _ := b.deductTP1QtyLocked(symbol, prevCommitted, prevFilled, actualCloseQty)
	actualCloseQty = recordedCloseQty

	if remQty.LessThanOrEqual(decimal.Zero) {
		tradeRec, siblingAlgoIDs, entryOrdersToCancel := b.finalizeTP1FullCloseLocked(symbol, actualCloseQty, actualExitPrice, clientOrderID, isMaker)
		entryOrdersToCancel = append(entryOrdersToCancel, pendingEntriesToCancel...)
		b.Mu.Unlock()
		b.dispatchTP1FullClose(tradeRec, siblingAlgoIDs, entryOrdersToCancel)
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

	b.cancelEntryOrdersAsync(pendingEntriesToCancel)

	if b.OnTradeClosedFn != nil {
		cb := b.OnTradeClosedFn
		b.goAsync(func() { cb(tradeRec) })
	}

	if b.PlaceAlgoOrderFn != nil {
		b.replaceStopLossAsync(slReplaceRequest{
			symbol:          symbol,
			exitSide:        model.OppositeSide(side),
			targetQty:       remQty,
			targetSL:        beSL,
			clientSLAlgoID:  clientSLID,
			oldSLID:         oldSLID,
			oldClientSLID:   oldClientSLID,
			generation:      gen,
			targetTier:      1,
			maxAttempts:     3,
			markFailedOnErr: true,
			contextLabel:    "tp1_be",
		})
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

	if b.LastMarkPrices == nil {
		b.LastMarkPrices = make(map[string]decimal.Decimal)
	}
	if b.LastMarkUpdatedAt == nil {
		b.LastMarkUpdatedAt = make(map[string]time.Time)
	}
	b.LastMarkPrices[symbol] = decimal.NewFromFloat(markPrice)
	b.LastMarkUpdatedAt[symbol] = now

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

			initialQty := b.InitialQtys[symbol]
			if initialQty.IsZero() {
				initialQty = qty
			}
			alreadyTP1 := b.TP1FilledQtys[symbol].Add(b.TP1CommittedQtys[symbol])
			var ef model.ExchangeFilter
			if b.GetExchangeFilterFn != nil {
				ef, _ = b.GetExchangeFilterFn(symbol)
			}
			splitPlan := protection.PlanPartialTP1Split(initialQty, qty, alreadyTP1, tp1Price, b.Cfg.PartialTP1Ratio, ef)
			closeQty := splitPlan.CloseQty
			splitPossible := splitPlan.SplitPossible

			if !splitPossible {
				b.IsTP1Hits[symbol] = true
				pendingEntriesToCancel := b.collectAndDetachPendingEntriesLocked(symbol)
				var cancelTP1ID int64
				var cancelTP1CID string
				if hasRestingTP1 && b.CancelOrderFn != nil {
					cancelTP1ID = prot.TP1OrderID
					cancelTP1CID = prot.TP1ClientOrderID
					prot.TP1OrderID = 0
					prot.TP1ClientOrderID = ""
				}
				slog.Info("live: position notional too small for partial TP1 split (< 5 USDT); moving SL to BE and letting 100% position run as runner",
					"symbol", symbol, "qty", qty, "tp1Price", tp1Price)

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
					TickSize:       ef.TickSize,
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
				b.Mu.Unlock()

				b.cancelEntryOrdersAsync(pendingEntriesToCancel)

				if cancelTP1ID != 0 && b.CancelOrderFn != nil {
					symCopy, ordID, cid := symbol, cancelTP1ID, cancelTP1CID
					b.goAsync(func() {
						ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
						_ = b.CancelOrderFn(ctxC, symCopy, ordID, cid)
						cancelC()
					})
				}

				if b.PlaceAlgoOrderFn != nil {
					b.replaceStopLossAsync(slReplaceRequest{
						symbol:          symbol,
						exitSide:        model.OppositeSide(side),
						targetQty:       qty,
						targetSL:        beSL,
						clientSLAlgoID:  clientSLID,
						oldSLID:         oldSLID,
						oldClientSLID:   oldClientSLID,
						generation:      gen,
						targetTier:      1,
						maxAttempts:     3,
						markFailedOnErr: true,
						contextLabel:    "non_split_tp1_be",
					})
				}
				return
			}

			// Batasi frekuensi & jumlah percobaan TP1 market fallback agar stream @bookTicker tidak memicu badai API.
			if b.TP1FallbackAttempts[symbol] >= 3 {
				goto skipTP1Handling
			}
			if lastAttempt := b.LastTP1FallbackTime[symbol]; !lastAttempt.IsZero() && now.Sub(lastAttempt) < 5*time.Second {
				goto skipTP1Handling
			}
			if b.TP1FallbackAttempts == nil {
				b.TP1FallbackAttempts = make(map[string]int)
			}
			if b.LastTP1FallbackTime == nil {
				b.LastTP1FallbackTime = make(map[string]time.Time)
			}
			b.TP1FallbackAttempts[symbol]++
			fallbackAttempt := b.TP1FallbackAttempts[symbol]
			b.LastTP1FallbackTime[symbol] = now

			// Batalkan antrean pending entry yang masih tersisa agar tidak terisi setelah TP1 milestone
			pendingEntriesToCancel := b.collectAndDetachPendingEntriesLocked(symbol)

			// Batalkan resting TP1 limit jika ada sebelum eksekusi market fallback
			var cancelTP1ID int64
			var cancelTP1ClientID string
			var planGen uint64
			if hasRestingTP1 {
				cancelTP1ID = prot.TP1OrderID
				cancelTP1ClientID = prot.TP1ClientOrderID
				planGen = prot.PlanGeneration
				prot.TP1OrderID = 0
				prot.TP1ClientOrderID = ""
			}
			restingPriorFilled := b.TP1FilledQtys[symbol]

			b.TP1InFlight[symbol] = true
			exitSide := model.OppositeSide(side)

			var tp1ClientOrderID string
			if fallbackAttempt == 1 {
				if baseID, ok := b.PositionClientOrderIDs[symbol]; ok && baseID != "" {
					if len(baseID) > 32 {
						tp1ClientOrderID = fmt.Sprintf("%s_TP1", baseID[:32])
					} else {
						tp1ClientOrderID = fmt.Sprintf("%s_TP1", baseID)
					}
				}
			}
			if tp1ClientOrderID == "" {
				tp1ClientOrderID = model.FormatClientID("ALGO_TP1", symbol, now)
			}

			symCopy, cQty, cID, cTP1ID, cTP1CID, priorFilled, pGen, entriesToCancel := symbol, closeQty, tp1ClientOrderID, cancelTP1ID, cancelTP1ClientID, restingPriorFilled, planGen, pendingEntriesToCancel
			b.goAsync(func() {
				defer func() {
					b.Mu.Lock()
					b.TP1InFlight[symCopy] = false
					b.Mu.Unlock()
				}()

				if b.CancelOrderFn != nil {
					for _, eOrd := range entriesToCancel {
						ctxE, cancelE := context.WithTimeout(context.Background(), 5*time.Second)
						_ = b.CancelOrderFn(ctxE, symCopy, eOrd.ExchangeOrderID, eOrd.ClientOrderID)
						cancelE()
					}
				}

				if cTP1ID != 0 && b.CancelOrderFn != nil {
					ctxC, cancelC := context.WithTimeout(context.Background(), 5*time.Second)
					cancelErr := b.CancelOrderFn(ctxC, symCopy, cTP1ID, cTP1CID)
					cancelC()
					if cancelErr != nil {
						if b.QueryOrderFn != nil && cTP1CID != "" {
							qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
							qRec, qErr := b.QueryOrderFn(qCtx, symCopy, cTP1CID)
							qCancel()
							if qErr != nil {
								slog.Warn("live: aborting TP1 market fallback because resting TP1 cancel & verify failed",
									"symbol", symCopy, "tp1ClientID", cTP1CID, "cancelErr", cancelErr, "queryErr", qErr)
								return
							}
							if qRec != nil {
								unaccountedDelta := qRec.CumulativeFilledQty.Sub(priorFilled)
								if unaccountedDelta.IsNegative() {
									unaccountedDelta = decimal.Zero
								}
								if qRec.Status == model.StatusFilled {
									exitP := qRec.AvgPrice
									if exitP.IsZero() {
										exitP = qRec.LimitPrice
									}
									b.processTP1Executed(symCopy, qRec.CumulativeFilledQty, exitP, cTP1CID, true)
									return
								}
								// Jika resting TP1 masih aktif (NEW / PARTIALLY_FILLED) karena cancel gagal, JANGAN eksekusi market close ganda
								// atau tandai IsTP1Hits=true; pulihkan ID order TP1 dan catat chunk fill baru bila ada.
								if qRec.Status == model.StatusNew || qRec.Status == model.StatusPartiallyFilled {
									b.Mu.Lock()
									if p, ok := b.Protections[symCopy]; ok && p != nil && p.PlanGeneration == pGen {
										p.TP1OrderID = cTP1ID
										p.TP1ClientOrderID = cTP1CID
									}
									b.Mu.Unlock()
									if unaccountedDelta.GreaterThan(decimal.Zero) {
										exitP := qRec.AvgPrice
										if exitP.IsZero() {
											exitP = qRec.LimitPrice
										}
										b.recordTP1PartialChunk(symCopy, cTP1CID, qRec.CumulativeFilledQty, exitP)
									}
									slog.Warn("live: aborting TP1 market fallback because resting TP1 order is still active on exchange",
										"symbol", symCopy, "tp1OrderID", cTP1ID, "status", qRec.Status, "cumFilled", qRec.CumulativeFilledQty)
									return
								}
								// Status terminal CANCELED/EXPIRED/REJECTED: hanya kurangi delta fill baru yang belum terhitung dalam cQty.
								if unaccountedDelta.GreaterThan(decimal.Zero) {
									exitP := qRec.AvgPrice
									if exitP.IsZero() {
										exitP = qRec.LimitPrice
									}
									b.recordTP1PartialChunk(symCopy, cTP1CID, qRec.CumulativeFilledQty, exitP)
									cQty = cQty.Sub(unaccountedDelta)
									if cQty.LessThanOrEqual(decimal.Zero) {
										b.processTP1Executed(symCopy, qRec.CumulativeFilledQty, exitP, cTP1CID, true)
										return
									}
								}
							}
						} else if strings.Contains(cancelErr.Error(), "-2011") || strings.Contains(cancelErr.Error(), "Unknown order") {
							slog.Warn("live: aborting TP1 market fallback because resting TP1 returned -2011 (likely already filled)",
								"symbol", symCopy, "tp1OrderID", cTP1ID, "err", cancelErr)
							return
						}
					}
				}

				b.Mu.Lock()
				alreadyHit := b.IsTP1Hits[symCopy]
				remActive := b.ActivePositions[symCopy]
				b.Mu.Unlock()
				if alreadyHit || remActive.LessThanOrEqual(decimal.Zero) {
					return
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
					orderID, fillPrice, execQty, err = b.MarketCloseFn(ctx, symCopy, exitSide, cQty, cID)
					cancel()
					if err == nil && orderID != 0 {
						break
					}
					if attempt < 3 {
						time.Sleep(time.Duration(attempt*250) * time.Millisecond)
					}
				}

				if err == nil && orderID != 0 {
					actualCloseQty := execQty
					actualExitPrice := fillPrice

					if (actualCloseQty.IsZero() || actualExitPrice.IsZero()) && b.QueryOrderFn != nil {
						for qAttempt := 1; qAttempt <= 3; qAttempt++ {
							ctxQ, cancelQ := context.WithTimeout(context.Background(), 5*time.Second)
							qRec, qErr := b.QueryOrderFn(ctxQ, symCopy, cID)
							cancelQ()
							if qErr == nil && qRec != nil {
								if qRec.Status == model.StatusCanceled || qRec.Status == model.StatusExpired || qRec.Status == model.StatusRejected {
									slog.Error("live: TP1 market close was canceled/expired/rejected without fill", "symbol", symCopy, "status", qRec.Status)
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
							if qAttempt < 3 {
								time.Sleep(time.Duration(qAttempt*200) * time.Millisecond)
							}
						}
					}

					b.processTP1Executed(symCopy, actualCloseQty, actualExitPrice, cID, false /* isMaker */)
				} else {
					slog.Error("live: failed to execute partial TP1 fallback market close", "symbol", symCopy, "err", err)
				}
			})
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

				nowUTC := now
				if nowUTC.IsZero() {
					nowUTC = time.Now().UTC()
				}
				if lastFail := b.LastAlgoFailTimes[symbol]; !lastFail.IsZero() && nowUTC.Sub(lastFail) < 3*time.Second {
					return
				}
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

				symCopy := symbol
				b.replaceStopLossAsync(slReplaceRequest{
					symbol:          symCopy,
					exitSide:        exitSide,
					targetQty:       targetQty,
					targetSL:        newSL,
					clientSLAlgoID:  clientSLID,
					oldSLID:         oldSLID,
					oldClientSLID:   oldClientSLID,
					generation:      gen,
					targetTier:      newTargetTier,
					maxAttempts:     1,
					markFailedOnErr: false,
					contextLabel:    "dynamic_protection",
					attemptTime:     nowUTC,
					onDone: func() {
						b.Mu.Lock()
						b.AlgoUpdateInFlight[symCopy] = false
						b.Mu.Unlock()
					},
				})
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
	avail := b.AvailableBalanceUSDT
	for _, margin := range b.ReservedEntryMargins {
		avail = avail.Sub(margin)
	}
	if avail.LessThan(decimal.Zero) {
		return decimal.Zero
	}
	return avail
}

func (b *BinanceLiveBroker) GetDailyTotalDrawdownPct() float64 {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		return 0.0
	}
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	b.PeakEffectiveEquityUSDT, _ = protection.UpdatePeakAndDrawdownPct(b.PeakEffectiveEquityUSDT, effectiveEquity)
	ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
	f, _ := ddDec.Float64()
	return f
}

// SetPeakEffectiveEquityUSDT raises the live high-water mark equity if peak is higher than current peak.
func (b *BinanceLiveBroker) SetPeakEffectiveEquityUSDT(peak decimal.Decimal) {
	if peak.LessThanOrEqual(decimal.Zero) {
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()
	if peak.GreaterThan(b.PeakEffectiveEquityUSDT) {
		b.PeakEffectiveEquityUSDT = peak
	}
}

// GetDrawdownFromPeakPct returns non-negative drawdown fraction from running HWM equity.
func (b *BinanceLiveBroker) GetDrawdownFromPeakPct() float64 {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	var dd float64
	b.PeakEffectiveEquityUSDT, dd = protection.UpdatePeakAndDrawdownPct(b.PeakEffectiveEquityUSDT, effectiveEquity)
	return dd
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
		markDec := b.LastMarkPrices[sym]
		if markDec.LessThanOrEqual(decimal.Zero) {
			markDec = entryPrice
		}
		markF, _ := markDec.Float64()

		lev := b.Cfg.AccountLeverage
		if lev <= 0 {
			lev = 8.0
		}

		var pnlF, roeF float64
		var mPnlF float64
		if markDec.GreaterThan(decimal.Zero) && entryPrice.GreaterThan(decimal.Zero) {
			if side == model.SideLong {
				mPnlF, _ = markDec.Sub(entryPrice).Mul(qty).Float64()
			} else {
				mPnlF, _ = entryPrice.Sub(markDec).Mul(qty).Float64()
			}
		}
		if uPnL, hasUPnL := b.PositionUnrealizedPnLs[sym]; hasUPnL && !uPnL.IsZero() {
			uF, _ := uPnL.Float64()
			if mPnlF != 0 && (uF*mPnlF) < 0 {
				pnlF = mPnlF
			} else {
				pnlF = uF
			}
		} else {
			pnlF = mPnlF
		}
		qtyF, _ := qty.Float64()
		if entryF > 0 && qtyF > 0 && pnlF != 0 {
			initialMargin := (entryF * qtyF) / lev
			if initialMargin > 0 {
				roeF = (pnlF / initialMargin) * 100.0
			}
		} else if entryF > 0 && markF > 0 {
			if side == model.SideLong {
				roeF = ((markF - entryF) / entryF) * lev * 100.0
			} else {
				roeF = ((entryF - markF) / entryF) * lev * 100.0
			}
		}

		var slF, tpF float64
		isBE := b.ProtectionTiers[sym] >= 1
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
			Symbol:           sym,
			Side:             string(side),
			StrategyID:       normalizeBrokerStrategyID(b.PositionStrategyIDs[sym]),
			Qty:              qty.String(),
			EntryPrice:       entryF,
			MarkPrice:        markF,
			SLPrice:          slF,
			TPPrice:          tpF,
			UnrealizedPnL:    pnlF,
			UnrealizedROEPct: roeF,
			OpenTimeUTC:      openTime.Format(time.RFC3339),
			DurationSec:      int64(now.Sub(openTime).Seconds()),
			IsBreakEven:      isBE,
			ProtectionTier:   b.ProtectionTiers[sym],
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
	takerFee := b.Cfg.TakerFeeRate
	if takerFee <= 0 {
		takerFee = 0.0005
	}
	slippage := b.Cfg.SlippageRate
	if slippage <= 0 {
		slippage = 0.0002
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
		TakerFeeRate:             decimal.NewFromFloat(takerFee),
		SlippageRate:             decimal.NewFromFloat(slippage),
	}
}

// AttachSignalAuditID melampirkan signal_audit ID ke order entry atau proteksi live aktif setelah penulisan SQLite selesai paralel.
func (b *BinanceLiveBroker) AttachSignalAuditID(symbol, clientOrderID string, signalAuditID int64) {
	if signalAuditID <= 0 {
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()

	activeCID := b.PositionClientOrderIDs[symbol]
	matchesForSymbol := func(cID string) bool {
		if matchesEntryClientID(cID, clientOrderID) {
			return true
		}
		return activeCID != "" && cID == activeCID && strings.HasPrefix(cID, "live_repeg_")
	}

	for cID, rec := range b.OrdersByClient {
		if rec != nil && rec.IsEntry && rec.Symbol == symbol && rec.SignalAuditID == 0 && matchesForSymbol(cID) {
			rec.SignalAuditID = signalAuditID
		}
	}
	if prot, exists := b.Protections[symbol]; exists && prot != nil && prot.SignalAuditID == 0 {
		if activeCID == "" || matchesForSymbol(activeCID) {
			prot.SignalAuditID = signalAuditID
			b.notifyPositionSavedLocked(symbol)
		}
	}
}
