package broker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type LiveBrokerConfig struct {
	MaxSlots            int
	MaxDailyDrawdownPct float64
	MaxAccountStaleness time.Duration
	LimitOrderTimeout   time.Duration
	MaxHoldingDuration       time.Duration
	BreakEvenTriggerATR      float64
	Tier1BEEnabled           bool
	Tier1BETriggerLongATR    float64
	Tier1BETriggerShortATR   float64
	Tier2ProfitLockATR       float64
	Tier2ProfitLockOffsetATR float64
	PartialTPEnabled         bool
	PartialTP1Ratio          float64
	PartialTP1ATRMult        float64
	TrailingEnabled          bool
	TrailingActivateATR      float64
	TrailingDistanceATR      float64
	MinBreakEvenOffsetPct    float64
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
	TPOrderID      int64
	TPClientAlgoID string
	TPPrice        decimal.Decimal
	TP1Price       decimal.Decimal
	IsTP1Hit       bool
	ProtectionTier int
	SignalAuditID  int64
	OpenTime       time.Time
	MaxMAE         float64
	MaxMFE         float64
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

	OrdersByClient         map[string]*model.LiveOrderRecord
	OrdersByExchangeID     map[int64]*model.LiveOrderRecord
	Protections            map[string]*model.LivePositionProtection
	ActivePositions        map[string]decimal.Decimal // symbol -> current position qty
	PositionOpenTimes      map[string]time.Time       // symbol -> time position was opened
	PositionSides          map[string]model.Side      // symbol -> position side
	PositionEntryPrices    map[string]decimal.Decimal // symbol -> entry price
	ClosingInFlight        map[string]bool            // symbol -> true if emergency market close in flight
	TPRepairInFlight       map[string]bool            // symbol -> true if TP repair order in flight
	ActiveMAE              map[string]float64         // symbol -> max adverse excursion pct
	ActiveMFE              map[string]float64         // symbol -> max favorable excursion pct
	ProtectionTiers        map[string]int             // symbol -> current protection tier (0, 1, 2, 3)
	ATRAtEntry             map[string]decimal.Decimal // symbol -> atr at entry
	PeakPrices             map[string]decimal.Decimal // symbol -> highest (long) or lowest (short) mark price

	PlaceEntryFn           func(ctx context.Context, intent model.OrderIntent) (exchangeOrderID int64, err error)
	PlaceAlgoOrderFn       func(ctx context.Context, symbol string, side model.Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (algoID int64, err error)
	CancelOrderFn          func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error
	CancelAlgoOrderFn      func(ctx context.Context, symbol string, algoID int64) error
	EmergencyMarketCloseFn func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error
	QueryOrderFn           func(ctx context.Context, symbol string, clientOrderID string) (*model.LiveOrderRecord, error)
	OnTradeClosedFn        func(rec TradeClosedRecord)
	OnPositionSavedFn      func(pos LiveOpenPosition)
	OnPositionDeletedFn    func(symbol string)
}

func NewBinanceLiveBroker(cfg LiveBrokerConfig) *BinanceLiveBroker {
	return &BinanceLiveBroker{
		Cfg:                 cfg,
		OrdersByClient:      make(map[string]*model.LiveOrderRecord),
		OrdersByExchangeID:  make(map[int64]*model.LiveOrderRecord),
		Protections:         make(map[string]*model.LivePositionProtection),
		ActivePositions:     make(map[string]decimal.Decimal),
		PositionOpenTimes:   make(map[string]time.Time),
		PositionSides:       make(map[string]model.Side),
		PositionEntryPrices: make(map[string]decimal.Decimal),
		ClosingInFlight:     make(map[string]bool),
		TPRepairInFlight:    make(map[string]bool),
		ActiveMAE:           make(map[string]float64),
		ActiveMFE:           make(map[string]float64),
		ProtectionTiers:     make(map[string]int),
		ATRAtEntry:          make(map[string]decimal.Decimal),
		PeakPrices:          make(map[string]decimal.Decimal),
	}
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
	if pos.EntryPrice.GreaterThan(decimal.Zero) {
		b.PeakPrices[pos.Symbol] = pos.EntryPrice
	}

	b.Protections[pos.Symbol] = &model.LivePositionProtection{
		Symbol:          pos.Symbol,
		Side:            pos.Side,
		PlanGeneration:  1,
		ProtectedQty:    pos.Qty,
		SLOrderID:       pos.SLOrderID,
		SLClientAlgoID:  pos.SLClientAlgoID,
		SLStatus:        model.ProtectionAcked,
		SLPrice:         pos.SLPrice,
		TPOrderID:       pos.TPOrderID,
		TPClientAlgoID:  pos.TPClientAlgoID,
		TPStatus:        model.ProtectionAcked,
		TPPrice:         pos.TPPrice,
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
	savedPos := LiveOpenPosition{
		Symbol:         symbol,
		Side:           prot.Side,
		Qty:            qty,
		EntryPrice:     b.PositionEntryPrices[symbol],
		SLOrderID:      prot.SLOrderID,
		SLClientAlgoID: prot.SLClientAlgoID,
		SLPrice:        prot.SLPrice,
		TPOrderID:      prot.TPOrderID,
		TPClientAlgoID: prot.TPClientAlgoID,
		TPPrice:        prot.TPPrice,
		InitialQty:     qty,
		TP1Price:       decimal.Zero,
		IsTP1Hit:       false,
		ProtectionTier: b.ProtectionTiers[symbol],
		SignalAuditID:  prot.SignalAuditID,
		OpenTime:       b.PositionOpenTimes[symbol],
		MaxMAE:         b.ActiveMAE[symbol],
		MaxMFE:         b.ActiveMFE[symbol],
		UpdatedAt:      time.Now().UTC(),
	}
	fn := b.OnPositionSavedFn
	go fn(savedPos)
}

func (b *BinanceLiveBroker) notifyPositionDeleted(symbol string) {
	if b.OnPositionDeletedFn != nil {
		fn := b.OnPositionDeletedFn
		go fn(symbol)
	}
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
		TPPrice:             intent.TPPrice,
		SignalAuditID:       intent.SignalAuditID,
		CreatedAt:           time.Now().UTC(),
	}
	b.ATRAtEntry[intent.Symbol] = intent.ATRAtEntry
	return true, nil
}

// SubmitLimitEntry mengimplementasikan Two-Phase Commit: Reservasi Atomik -> I/O Bursa -> Update/Reconcile (Solusi P0-1)
func (b *BinanceLiveBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) {
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
	var plan model.ProtectionPlanAction
	var needPlanExecution bool
	var siblingCancelAlgoID int64
	var siblingCancelSymbol string

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
			}
		}
		if !exists {
			return
		}
	}

	rec.ExchangeOrderID = exchangeOrderID
	rec.Status = status
	rec.CumulativeFilledQty = cumFilledQty
	b.OrdersByExchangeID[exchangeOrderID] = rec

	// SKENARIO 1: Update Order Entry
	if rec.IsEntry {
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
	if !rec.IsEntry && status == model.StatusFilled {
		delete(b.ActivePositions, symbol)
		delete(b.PositionSides, symbol)
		openTime := b.PositionOpenTimes[symbol]
		delete(b.PositionOpenTimes, symbol)
		if prot, ok := b.Protections[symbol]; ok {
			var exitReason string
			currentTier := b.ProtectionTiers[symbol]
			if (exchangeOrderID == prot.SLOrderID || clientOrderID == prot.SLClientAlgoID) && prot.TPOrderID != 0 {
				siblingCancelAlgoID = prot.TPOrderID
				siblingCancelSymbol = symbol
				exitReason = "STOP_LOSS"
				if currentTier == 3 {
					exitReason = "TRAILING_STOP"
				} else if currentTier == 2 {
					exitReason = "PROFIT_LOCK"
				} else if currentTier == 1 {
					exitReason = "BREAK_EVEN"
				}
			} else if (exchangeOrderID == prot.TPOrderID || clientOrderID == prot.TPClientAlgoID) && prot.SLOrderID != 0 {
				siblingCancelAlgoID = prot.SLOrderID
				siblingCancelSymbol = symbol
				exitReason = "TAKE_PROFIT"
			} else if exchangeOrderID == prot.SLOrderID || clientOrderID == prot.SLClientAlgoID {
				exitReason = "STOP_LOSS"
				if currentTier == 3 {
					exitReason = "TRAILING_STOP"
				} else if currentTier == 2 {
					exitReason = "PROFIT_LOCK"
				} else if currentTier == 1 {
					exitReason = "BREAK_EVEN"
				}
			} else {
				exitReason = "TAKE_PROFIT"
			}

			entryPrice := b.PositionEntryPrices[symbol]
			delete(b.PositionEntryPrices, symbol)
			delete(b.ProtectionTiers, symbol)
			delete(b.ATRAtEntry, symbol)
			delete(b.PeakPrices, symbol)

			exitPrice := avgPrice
			if exitPrice.LessThanOrEqual(decimal.Zero) {
				if exitReason == "STOP_LOSS" || exitReason == "BREAK_EVEN" || exitReason == "PROFIT_LOCK" || exitReason == "TRAILING_STOP" {
					exitPrice = prot.SLPrice
				} else {
					exitPrice = prot.TPPrice
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

			tradeRec = &TradeClosedRecord{
				ClientOrderID: rec.ClientOrderID,
				Symbol:        symbol,
				Side:          string(prot.Side),
				Qty:           rec.TargetQty,
				EntryPrice:    entryPrice,
				ExitPrice:     exitPrice,
				SLPrice:       prot.SLPrice,
				TPPrice:       prot.TPPrice,
				NetPnL:        netPnL,
				MAE:           b.ActiveMAE[symbol],
				MFE:           b.ActiveMFE[symbol],
				ExitReason:    exitReason,
				OpenedAt:      openTime,
				ClosedAt:      time.Now().UTC(),
				SignalAuditID: prot.SignalAuditID,
			}
			delete(b.ActiveMAE, symbol)
			delete(b.ActiveMFE, symbol)
			delete(b.Protections, symbol)
		}
		delete(b.OrdersByClient, rec.ClientOrderID)
		delete(b.OrdersByExchangeID, exchangeOrderID)
	}
	b.Mu.Unlock()

	if tradeRec != nil {
		if b.OnPositionDeletedFn != nil {
			go b.OnPositionDeletedFn(tradeRec.Symbol)
		}
		if b.OnTradeClosedFn != nil {
			go b.OnTradeClosedFn(*tradeRec)
		}
	}

	if siblingCancelAlgoID != 0 && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, siblingCancelSymbol, siblingCancelAlgoID)
		cancel()
	}
}

func (b *BinanceLiveBroker) planProtectionSyncLocked(entryRec *model.LiveOrderRecord) (model.ProtectionPlanAction, bool) {
	prot, exists := b.Protections[entryRec.Symbol]
	if exists && prot.SLStatus == model.ProtectionAcked && prot.ProtectedQty.Equal(entryRec.CumulativeFilledQty) {
		return model.ProtectionPlanAction{}, false
	}

	var gen uint64 = 1
	var oldSLID, oldTPID int64
	if exists {
		gen = prot.PlanGeneration + 1
		oldSLID = prot.SLOrderID
		oldTPID = prot.TPOrderID
	}

	now := time.Now().UTC()
	slAlgoClientID := fmt.Sprintf("ALGO_SL_%s_%d", entryRec.Symbol, now.UnixNano())
	tpAlgoClientID := fmt.Sprintf("ALGO_TP_%s_%d", entryRec.Symbol, now.UnixNano())

	action := model.ProtectionPlanAction{
		Symbol:         entryRec.Symbol,
		Generation:     gen,
		CancelSLID:     oldSLID,
		CancelTPID:     oldTPID,
		NeedNewSL:      true,
		NeedNewTP:      true,
		ExitSide:       model.OppositeSide(entryRec.Side),
		TargetQty:      entryRec.CumulativeFilledQty,
		SLPrice:        entryRec.SLPrice,
		TPPrice:        entryRec.TPPrice,
		ClientSLAlgoID: slAlgoClientID,
		ClientTPAlgoID: tpAlgoClientID,
	}

	b.Protections[entryRec.Symbol] = &model.LivePositionProtection{
		Symbol:          entryRec.Symbol,
		Side:            entryRec.Side,
		PlanGeneration:  gen,
		ProtectedQty:    decimal.Zero,
		SLStatus:        model.ProtectionPending,
		SLClientAlgoID:  slAlgoClientID,
		SLPrice:         entryRec.SLPrice,
		TPStatus:        model.ProtectionPending,
		TPClientAlgoID:  tpAlgoClientID,
		TPPrice:         entryRec.TPPrice,
		LastAttemptTime: now,
		SignalAuditID:   entryRec.SignalAuditID,
	}

	return action, true
}

func (b *BinanceLiveBroker) executeProtectionPlan(action model.ProtectionPlanAction) {
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
			var closeErr error
			for closeAttempt := 1; closeAttempt <= 3; closeAttempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				closeErr = b.EmergencyMarketCloseFn(ctx, action.Symbol, action.ExitSide, action.TargetQty, "CRITICAL: SL placement failed, emergency exit executed")
				cancel()
				if closeErr == nil {
					break
				}
				time.Sleep(time.Duration(closeAttempt*250) * time.Millisecond)
			}
			if closeErr != nil {
				slog.Error("CRITICAL ALERT: EMERGENCY CLOSE FAILED", "symbol", action.Symbol, "side", action.ExitSide, "qty", action.TargetQty, "err", closeErr)
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

	// LANGKAH 3: HANYA SETELAH SL BARU TERKONFIRMASI -> Batalkan SL dan TP lama
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

	if tpErr != nil || newTPOrderID == 0 {
		prot.TPStatus = model.ProtectionFailed
		prot.LastError = fmt.Sprintf("TP placement failed: %v", tpErr)
		prot.LastTPAttempt = time.Now().UTC()
	} else {
		prot.TPOrderID = newTPOrderID
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
		symbol   string
		exitSide model.Side
		qty      decimal.Decimal
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
				exitSide := model.OppositeSide(side)
				if !hasSide || exitSide == "" {
					slog.Warn("cannot determine exit side for position time-stop, skipping", "symbol", sym, "side", side)
					continue
				}
				b.ClosingInFlight[sym] = true
				targets = append(targets, closeTarget{
					symbol:   sym,
					exitSide: exitSide,
					qty:      qty,
				})
			}
		}
	}
	b.Mu.Unlock()

	for _, tgt := range targets {
		var closeErr error
		for attempt := 1; attempt <= 3; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			closeErr = b.EmergencyMarketCloseFn(ctx, tgt.symbol, tgt.exitSide, tgt.qty, "TIME_STOP_MAX_HOLDING")
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

// RepairFailedProtections mencoba memasang ulang Take Profit order yang gagal (M4 TP Repair Loop)
// Dilengkapi guard cooldown 10 detik, max 5x percobaan, non-blocking I/O, dan ghost order guard.
func (b *BinanceLiveBroker) RepairFailedProtections(now time.Time) {
	if b.PlaceAlgoOrderFn == nil {
		return
	}

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

		clientAlgoID := fmt.Sprintf("ALGOTP_REP_%s_%d", sym, now.Unix())
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		newTPID, err := b.PlaceAlgoOrderFn(
			ctx,
			tgt.symbol,
			tgt.exitSide,
			"TAKE_PROFIT_MARKET",
			tgt.qty,
			tgt.tpPrice,
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
	if b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		return false
	}
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
	ddFloat, _ := ddDec.Float64()
	return ddFloat <= b.Cfg.MaxDailyDrawdownPct
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

	// Proteksi Dinamis Tier 1 & Tier 2 Break-Even / Profit Lock untuk Live Trading
	atr := b.ATRAtEntry[symbol]
	if atr.GreaterThan(decimal.Zero) {
		t1LongTriggerATR := b.Cfg.Tier1BETriggerLongATR
		if t1LongTriggerATR <= 0 {
			t1LongTriggerATR = b.Cfg.BreakEvenTriggerATR
		}
		if t1LongTriggerATR <= 0 {
			t1LongTriggerATR = 0.70
		}

		t1ShortTriggerATR := b.Cfg.Tier1BETriggerShortATR
		if t1ShortTriggerATR <= 0 {
			t1ShortTriggerATR = b.Cfg.BreakEvenTriggerATR
		}
		if t1ShortTriggerATR <= 0 {
			t1ShortTriggerATR = 0.90
		}

		t2TriggerATR := b.Cfg.Tier2ProfitLockATR
		if t2TriggerATR <= 0 {
			t2TriggerATR = 1.80
		}

		t2OffsetATR := b.Cfg.Tier2ProfitLockOffsetATR
		if t2OffsetATR <= 0 {
			t2OffsetATR = 0.90
		}

		beOffsetRate := decimal.NewFromFloat(math.Max(b.Cfg.MinBreakEvenOffsetPct, 0.0010))
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

		trailActivateATR := b.Cfg.TrailingActivateATR
		if trailActivateATR <= 0 {
			trailActivateATR = 2.00
		}
		trailDistATR := b.Cfg.TrailingDistanceATR
		if trailDistATR <= 0 {
			trailDistATR = 0.80
		}
		trailActivateDist := atr.Mul(decimal.NewFromFloat(trailActivateATR))
		trailDist := atr.Mul(decimal.NewFromFloat(trailDistATR))

		if hasProt && prot.SLStatus == model.ProtectionAcked && b.PlaceAlgoOrderFn != nil {
			var newTargetTier int
			var newSL decimal.Decimal

			if side == model.SideLong {
				t1TriggerDist := atr.Mul(decimal.NewFromFloat(t1LongTriggerATR))
				t2TriggerDist := atr.Mul(decimal.NewFromFloat(t2TriggerATR))
				t2OffsetDist := atr.Mul(decimal.NewFromFloat(t2OffsetATR))

				if b.Cfg.TrailingEnabled && peakPrice.Sub(entryPrice).GreaterThanOrEqual(trailActivateDist) {
					calculatedSL := peakPrice.Sub(trailDist)
					if calculatedSL.GreaterThan(prot.SLPrice) {
						newTargetTier = 3
						newSL = calculatedSL
					}
				} else if currentTier < 2 && markPriceDec.Sub(entryPrice).GreaterThanOrEqual(t2TriggerDist) {
					calculatedSL := entryPrice.Add(t2OffsetDist)
					if calculatedSL.GreaterThan(prot.SLPrice) {
						newTargetTier = 2
						newSL = calculatedSL
					}
				} else if b.Cfg.Tier1BEEnabled && currentTier < 1 && markPriceDec.Sub(entryPrice).GreaterThanOrEqual(t1TriggerDist) {
					calculatedSL := entryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffsetRate))
					if calculatedSL.GreaterThan(prot.SLPrice) {
						newTargetTier = 1
						newSL = calculatedSL
					}
				}
			} else {
				t1TriggerDist := atr.Mul(decimal.NewFromFloat(t1ShortTriggerATR))
				t2TriggerDist := atr.Mul(decimal.NewFromFloat(t2TriggerATR))
				t2OffsetDist := atr.Mul(decimal.NewFromFloat(t2OffsetATR))

				if b.Cfg.TrailingEnabled && entryPrice.Sub(peakPrice).GreaterThanOrEqual(trailActivateDist) {
					calculatedSL := peakPrice.Add(trailDist)
					if calculatedSL.LessThan(prot.SLPrice) {
						newTargetTier = 3
						newSL = calculatedSL
					}
				} else if currentTier < 2 && entryPrice.Sub(markPriceDec).GreaterThanOrEqual(t2TriggerDist) {
					calculatedSL := entryPrice.Sub(t2OffsetDist)
					if calculatedSL.LessThan(prot.SLPrice) {
						newTargetTier = 2
						newSL = calculatedSL
					}
				} else if b.Cfg.Tier1BEEnabled && currentTier < 1 && entryPrice.Sub(markPriceDec).GreaterThanOrEqual(t1TriggerDist) {
					calculatedSL := entryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffsetRate))
					if calculatedSL.LessThan(prot.SLPrice) {
						newTargetTier = 1
						newSL = calculatedSL
					}
				}
			}

			if newTargetTier > 0 && !newSL.IsZero() {
				b.ProtectionTiers[symbol] = newTargetTier
				oldSLID := prot.SLOrderID
				prot.SLPrice = newSL
				prot.PlanGeneration++
				gen := prot.PlanGeneration
				exitSide := model.OppositeSide(side)
				clientSLID := fmt.Sprintf("live_sl_%s_%d", symbol, time.Now().UnixNano())
				prot.SLClientAlgoID = clientSLID

				go func(sym string, eSide model.Side, targetQty, targetSL decimal.Decimal, cAlgoID string, oldID int64, generation uint64, tier int) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					newID, err := b.PlaceAlgoOrderFn(ctx, sym, eSide, "STOP_MARKET", targetQty, targetSL, cAlgoID)
					cancel()
					if err == nil && newID != 0 {
						b.Mu.Lock()
						if p, ok := b.Protections[sym]; ok && p.PlanGeneration == generation {
							p.SLOrderID = newID
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
				}(symbol, exitSide, qty, newSL, clientSLID, oldSLID, gen, newTargetTier)
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
			Symbol:           sym,
			Side:             string(side),
			Qty:              qty.String(),
			EntryPrice:       entryF,
			SLPrice:          slF,
			TPPrice:          tpF,
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


