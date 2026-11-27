package broker

import (
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
	"trml/internal/model"
)

type VirtualPendingOrder struct {
	Intent         model.OrderIntent
	OrigLimitPrice decimal.Decimal
	FilledQty      decimal.Decimal
	RepegAttempts  int
	GTXValidated   bool
}

type VirtualOpenPosition struct {
	ClientOrderID    string
	Symbol           string
	Side             model.Side
	StrategyID       string
	Qty              decimal.Decimal
	InitialQty       decimal.Decimal
	EntryPrice       decimal.Decimal
	EntryFee         decimal.Decimal
	CurrentSLPrice   decimal.Decimal
	InitialSLPrice   decimal.Decimal
	TPPrice          decimal.Decimal
	TP1Price         decimal.Decimal
	IsTP1Hit         bool
	ATRAtEntry       decimal.Decimal
	LastMarkPrice    decimal.Decimal
	MarkUpdatedAt    time.Time
	OpenTime         time.Time
	IsBreakEvenMoved bool
	ProtectionTier   int
	SignalAuditID    int64
	MaxMAE           float64
	MaxMFE           float64
	PeakPrice        decimal.Decimal
	FundingPaid      decimal.Decimal
	LastFundingEpoch int64
}

type DryRunConfig struct {
	MaxSlots                 int
	MaxDailyDrawdownPct      float64
	AccountLeverage          float64
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
	LimitOrderTimeout        time.Duration
	MaxHoldingDuration       time.Duration
	StaleLossExitDuration    time.Duration
	MaxMarkStaleness         time.Duration
	MakerFeeRate             float64
	TakerFeeRate             float64
	SlippageRate             float64
	SLGapFillEnabled         bool
	EntryOrderType           string
	MaxEntryRepegAttempts    int
	MaxEntryRepegDriftPct    float64
	StrategyExitProfiles     map[string]protection.StrategyExitProfile
}

type TradeClosedRecord struct {
	ClientOrderID string
	Symbol        string
	Side          string
	StrategyID    string
	Qty           decimal.Decimal
	EntryPrice    decimal.Decimal
	ExitPrice     decimal.Decimal
	SLPrice       decimal.Decimal
	TPPrice       decimal.Decimal
	NetPnL        decimal.Decimal
	MAE           float64
	MFE           float64
	ExitReason    string
	OpenedAt      time.Time
	ClosedAt      time.Time
	SignalAuditID int64
}

func normalizeBrokerStrategyID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return "VWAP_PULLBACK"
	}
	return trimmed
}

type PositionEventType int

const (
	PositionEventSave PositionEventType = iota
	PositionEventDelete
)

type PositionEvent struct {
	Type                  PositionEventType
	Pos                   VirtualOpenPosition
	ClientOrderIDOrSymbol string
}

type DryRunBroker struct {
	Mu                    sync.Mutex
	Cfg                   DryRunConfig
	SessionDayEpochUTC    int64
	SessionStartEquity      decimal.Decimal
	RealizedEquityUSDT      decimal.Decimal
	PeakEffectiveEquityUSDT decimal.Decimal
	CircuitBreakerTripped   bool
	PendingOrders         map[string]*VirtualPendingOrder
	OpenPositions         map[string]*VirtualOpenPosition
	OnTradeClosedFn               func(rec TradeClosedRecord)
	OnCooldownRecordedFn          func(symbol string, closedAt time.Time, exitReason string)
	OnStrategyCooldownRecordedFn  func(symbol, strategyID string, closedAt time.Time, exitReason string)
	OnPositionSavedFn     func(pos VirtualOpenPosition)
	OnPositionDeletedFn   func(clientOrderIDOrSymbol string)
	GetExchangeFilterFn   func(symbol string) (model.ExchangeFilter, bool)
	GetBestQuoteFn        func(symbol string) (bestBid, bestAsk float64, ok bool)

	eventQueue       []PositionEvent
	eventCond        *sync.Cond
	eventWg          sync.WaitGroup
	asyncWg          sync.WaitGroup
	eventMu          sync.Mutex
	acceptClosed     bool
	eventClosed      bool
	lastPeriodicSync time.Time
}

func (d *DryRunBroker) goAsync(fn func()) {
	d.eventMu.Lock()
	if d.acceptClosed {
		d.eventMu.Unlock()
		return
	}
	d.asyncWg.Add(1)
	d.eventMu.Unlock()
	go func() {
		defer d.asyncWg.Done()
		fn()
	}()
}

// RestoreOpenPosition memulihkan posisi virtual dari database ke memory saat startup
// dan menyesuaikan saldo realized equity dengan entryFee serta funding posisi agar akuntansi presisi
func (d *DryRunBroker) RestoreOpenPosition(pos *VirtualOpenPosition) {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	if pos.InitialQty.IsZero() {
		pos.InitialQty = pos.Qty
	}
	if pos.PeakPrice.IsZero() {
		pos.PeakPrice = pos.EntryPrice
	}
	if pos.LastFundingEpoch == 0 && !pos.OpenTime.IsZero() {
		pos.LastFundingEpoch = pos.OpenTime.UTC().Unix() / 28800
	}
	if _, exists := d.OpenPositions[pos.Symbol]; !exists {
		entryFeeRem := pos.EntryFee
		if !pos.InitialQty.IsZero() && pos.InitialQty.GreaterThan(pos.Qty) {
			entryFeeRem = pos.EntryFee.Mul(pos.Qty).Div(pos.InitialQty)
		}
		d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(entryFeeRem).Sub(pos.FundingPaid)
	}
	d.OpenPositions[pos.Symbol] = pos
}

func (d *DryRunBroker) enqueuePositionEvent(evt PositionEvent) {
	d.eventMu.Lock()
	if d.eventClosed {
		d.eventMu.Unlock()
		return
	}
	if d.eventCond == nil {
		d.eventMu.Unlock()
		d.dispatchPositionEvent(evt)
		return
	}
	d.eventQueue = append(d.eventQueue, evt)
	d.eventCond.Signal()
	d.eventMu.Unlock()
}

func (d *DryRunBroker) startEventWorker() {
	d.eventWg.Add(1)
	go func() {
		defer d.eventWg.Done()
		for {
			d.eventMu.Lock()
			for len(d.eventQueue) == 0 && !d.eventClosed {
				d.eventCond.Wait()
			}
			if len(d.eventQueue) == 0 && d.eventClosed {
				d.eventMu.Unlock()
				return
			}
			evt := d.eventQueue[0]
			d.eventQueue[0] = PositionEvent{}
			d.eventQueue = d.eventQueue[1:]
			if len(d.eventQueue) == 0 {
				d.eventQueue = nil
			}
			d.eventMu.Unlock()
			d.dispatchPositionEvent(evt)
		}
	}()
}

func (d *DryRunBroker) dispatchPositionEvent(evt PositionEvent) {
	if evt.Type == PositionEventSave && d.OnPositionSavedFn != nil {
		d.OnPositionSavedFn(evt.Pos)
	} else if evt.Type == PositionEventDelete && d.OnPositionDeletedFn != nil {
		d.OnPositionDeletedFn(evt.ClientOrderIDOrSymbol)
	}
}

func (d *DryRunBroker) Close() {
	d.eventMu.Lock()
	if d.acceptClosed {
		d.eventMu.Unlock()
		return
	}
	d.acceptClosed = true
	d.eventMu.Unlock()

	d.asyncWg.Wait()

	d.eventMu.Lock()
	d.eventClosed = true
	if d.eventCond != nil {
		d.eventCond.Broadcast()
	}
	d.eventMu.Unlock()

	d.eventWg.Wait()
}

func NewDryRunBroker(initialEquity decimal.Decimal, cfg DryRunConfig) *DryRunBroker {
	if cfg.MaxSlots <= 0 {
		cfg.MaxSlots = 3
	}
	if cfg.MaxDailyDrawdownPct == 0 {
		cfg.MaxDailyDrawdownPct = -0.05
	}
	if cfg.AccountLeverage <= 0 {
		cfg.AccountLeverage = 8.0
	}
	if cfg.BreakEvenTriggerATR <= 0 {
		cfg.BreakEvenTriggerATR = 1.15
	}
	if cfg.MinBreakEvenOffsetPct <= 0 {
		cfg.MinBreakEvenOffsetPct = 0.0010
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
	broker := &DryRunBroker{
		Cfg:                     cfg,
		SessionStartEquity:      initialEquity,
		RealizedEquityUSDT:      initialEquity,
		PeakEffectiveEquityUSDT: initialEquity,
		PendingOrders:           make(map[string]*VirtualPendingOrder),
		OpenPositions:           make(map[string]*VirtualOpenPosition),
		lastPeriodicSync:        time.Now().UTC(),
	}
	broker.eventCond = sync.NewCond(&broker.eventMu)
	broker.startEventWorker()
	return broker
}

func (d *DryRunBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) {
	origLimit := intent.LimitPrice
	gtxValidated := false
	repegAttempts := 0
	effOrderType := effectiveEntryOrderType(intent.EntryOrderType, d.Cfg.EntryOrderType)

	// Lakukan refresh BBO dan pre-repeg di luar lock agar tidak terjadi cross-lock contention
	if !strings.EqualFold(effOrderType, "MARKET") && d.GetBestQuoteFn != nil {
		if bid, ask, ok := d.GetBestQuoteFn(intent.Symbol); ok && bid > 0 && ask > 0 {
			if wouldCrossMakerBook(intent.Side, intent.LimitPrice, bid, ask) {
				maxRepegs, maxDrift := resolveEntryRepegSettings(d.Cfg.MaxEntryRepegAttempts, d.Cfg.MaxEntryRepegDriftPct)
				if maxRepegs <= 0 {
					return false, fmt.Errorf("dryrun: limit entry crossed spread at submit (bid=%v ask=%v limit=%s)", bid, ask, intent.LimitPrice)
				}
				var ef model.ExchangeFilter
				if d.GetExchangeFilterFn != nil {
					ef, _ = d.GetExchangeFilterFn(intent.Symbol)
				}
				updated, okRepeg, reason := repegLimitEntryIntent(intent, origLimit, bid, ask, ef, maxDrift)
				if !okRepeg {
					slog.Warn("dryrun: limit entry REJECTED at submit (cross spread & repeg rejected)",
						"symbol", intent.Symbol, "clientOrderID", intent.ClientOrderID,
						"origLimit", origLimit, "bid", bid, "ask", ask, "reason", reason)
					return false, fmt.Errorf("dryrun: limit entry repeg rejected (%s)", reason)
				}
				slog.Info("dryrun: limit entry PRE-REPEGGED at submit (prevented GTX cross)",
					"symbol", intent.Symbol, "clientOrderID", intent.ClientOrderID,
					"oldLimit", intent.LimitPrice, "newLimit", updated.LimitPrice, "bid", bid, "ask", ask)
				intent = updated
				repegAttempts = 1
			}
			gtxValidated = true
		}
	}

	d.Mu.Lock()
	defer d.Mu.Unlock()

	if d.isDailyDrawdownBreachedLocked() {
		return false, fmt.Errorf("daily circuit breaker active")
	}
	if _, exists := d.OpenPositions[intent.Symbol]; exists {
		return false, fmt.Errorf("position already open for symbol %s", intent.Symbol)
	}
	if _, exists := d.PendingOrders[intent.Symbol]; exists {
		return false, fmt.Errorf("pending order already exists for symbol %s", intent.Symbol)
	}
	activeCount := len(d.OpenPositions)
	for sym := range d.PendingOrders {
		if _, exists := d.OpenPositions[sym]; !exists {
			activeCount++
		}
	}
	if activeCount >= d.Cfg.MaxSlots {
		return false, fmt.Errorf("all %d slots are occupied", d.Cfg.MaxSlots)
	}

	if strings.EqualFold(effOrderType, "MARKET") {
		fillPrice := intent.LimitPrice
		if d.Cfg.SlippageRate > 0 {
			if intent.Side == model.SideLong {
				fillPrice = fillPrice.Mul(decimal.NewFromFloat(1.0 + d.Cfg.SlippageRate))
			} else {
				fillPrice = fillPrice.Mul(decimal.NewFromFloat(1.0 - d.Cfg.SlippageRate))
			}
			if d.GetExchangeFilterFn != nil {
				if f, ok := d.GetExchangeFilterFn(intent.Symbol); ok && f.TickSize.GreaterThan(decimal.Zero) {
					if qPrice, err := model.QuantizePrice(fillPrice, f.TickSize, intent.Side == model.SideLong); err == nil {
						fillPrice = qPrice
					}
				}
			}
		}
		entryFee := fillPrice.Mul(intent.Qty).Mul(decimal.NewFromFloat(d.Cfg.TakerFeeRate))
		d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(entryFee)

		now := intent.CreatedAt
		if now.IsZero() {
			now = time.Now().UTC()
		}

		pos := &VirtualOpenPosition{
			ClientOrderID:    intent.ClientOrderID,
			Symbol:           intent.Symbol,
			Side:             intent.Side,
			StrategyID:       normalizeBrokerStrategyID(intent.StrategyID),
			Qty:              intent.Qty,
			InitialQty:       intent.Qty,
			EntryPrice:       fillPrice,
			EntryFee:         entryFee,
			CurrentSLPrice:   intent.SLPrice,
			InitialSLPrice:   intent.SLPrice,
			TPPrice:          intent.TPPrice,
			TP1Price:         intent.TP1Price,
			ATRAtEntry:       intent.ATRAtEntry,
			LastMarkPrice:    fillPrice,
			MarkUpdatedAt:    now,
			OpenTime:         now,
			IsBreakEvenMoved: false,
			SignalAuditID:    intent.SignalAuditID,
			PeakPrice:        fillPrice,
			LastFundingEpoch: now.UTC().Unix() / 28800,
		}
		d.OpenPositions[intent.Symbol] = pos

		d.enqueuePositionEvent(PositionEvent{
			Type: PositionEventSave,
			Pos:  *pos,
		})

		slog.Info("dryrun: market entry FILLED",
			"symbol", intent.Symbol,
			"clientOrderID", intent.ClientOrderID,
			"side", intent.Side,
			"filledQty", intent.Qty,
			"price", fillPrice,
			"fee", entryFee,
			"signalAuditID", intent.SignalAuditID,
		)
		return true, nil
	}

	d.PendingOrders[intent.Symbol] = &VirtualPendingOrder{
		Intent:         intent,
		OrigLimitPrice: origLimit,
		FilledQty:      decimal.Zero,
		RepegAttempts:  repegAttempts,
		GTXValidated:   gtxValidated,
	}
	return true, nil
}

func (d *DryRunBroker) isDailyDrawdownBreachedLocked() bool {
	if d.CircuitBreakerTripped {
		return true
	}
	if d.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		return false
	}
	effEquity := d.calculateEffectiveEquityLocked()
	ddDec := effEquity.Sub(d.SessionStartEquity).Div(d.SessionStartEquity)
	ddFloat, _ := ddDec.Float64()
	if ddFloat <= d.Cfg.MaxDailyDrawdownPct {
		d.CircuitBreakerTripped = true
		return true
	}
	return false
}

func (d *DryRunBroker) cancelAllPendingOrdersLocked(reason string) {
	for sym, ord := range d.PendingOrders {
		slog.Warn("dryrun: canceling pending limit order",
			"symbol", sym, "clientOrderID", ord.Intent.ClientOrderID,
			"limitPrice", ord.Intent.LimitPrice, "reason", reason)
		if ord.FilledQty.GreaterThan(decimal.Zero) {
			if pos, exists := d.OpenPositions[sym]; exists {
				pos.InitialQty = ord.FilledQty
				d.enqueuePositionEvent(PositionEvent{
					Type: PositionEventSave,
					Pos:  *pos,
				})
			}
		}
		delete(d.PendingOrders, sym)
	}
}

func (d *DryRunBroker) OnRealtimeTick(
	symbol string,
	bestBid, bestAsk, markPrice, tradePrice, tradeQty float64,
	isBuyerMaker bool,
	now time.Time,
) {
	var closedRecs []TradeClosedRecord
	d.Mu.Lock()
	defer func() {
		d.Mu.Unlock()
		if len(closedRecs) > 0 {
			for _, rec := range closedRecs {
				if d.OnStrategyCooldownRecordedFn != nil {
					d.OnStrategyCooldownRecordedFn(rec.Symbol, rec.StrategyID, rec.ClosedAt, rec.ExitReason)
				}
				if d.OnCooldownRecordedFn != nil {
					d.OnCooldownRecordedFn(rec.Symbol, rec.ClosedAt, rec.ExitReason)
				}
			}
		}
	}()

	// Solusi P1-7: Mark-to-market terlebih dahulu SEBELUM rollover baseline harian
	if markPrice > 0 {
		if pos, ok := d.OpenPositions[symbol]; ok {
			pos.LastMarkPrice = decimal.NewFromFloat(markPrice)
			pos.MarkUpdatedAt = now
			d.PeakEffectiveEquityUSDT, _ = protection.UpdatePeakAndDrawdownPct(d.PeakEffectiveEquityUSDT, d.calculateEffectiveEquityLocked())

			entryF, _ := pos.EntryPrice.Float64()
			if entryF > 0 {
				refFavPrice := markPrice
				if tradePrice > 0 {
					if pos.Side == model.SideLong && tradePrice > refFavPrice {
						refFavPrice = tradePrice
					} else if pos.Side == model.SideShort && tradePrice < refFavPrice {
						refFavPrice = tradePrice
					}
				}
				if pos.Side == model.SideLong {
					if refFavPrice > entryF {
						favPct := ((refFavPrice - entryF) / entryF) * 100.0
						if favPct > pos.MaxMFE {
							pos.MaxMFE = favPct
						}
					}
					if markPrice < entryF {
						advPct := ((entryF - markPrice) / entryF) * 100.0
						if advPct > pos.MaxMAE {
							pos.MaxMAE = advPct
						}
					}
				} else {
					if refFavPrice < entryF {
						favPct := ((entryF - refFavPrice) / entryF) * 100.0
						if favPct > pos.MaxMFE {
							pos.MaxMFE = favPct
						}
					}
					if markPrice > entryF {
						advPct := ((markPrice - entryF) / entryF) * 100.0
						if advPct > pos.MaxMAE {
							pos.MaxMAE = advPct
						}
					}
				}
			}
		}
	}

	epochDay := now.UTC().Unix() / 86400
	if epochDay != d.SessionDayEpochUTC {
		d.SessionDayEpochUTC = epochDay
		d.SessionStartEquity = d.calculateEffectiveEquityLocked()
		d.CircuitBreakerTripped = false
	}

	if d.isDailyDrawdownBreachedLocked() && len(d.PendingOrders) > 0 {
		d.cancelAllPendingOrdersLocked("CIRCUIT_BREAKER_BREACHED")
	}

	// 1. Evaluasi Antrean Limit Order Masuk (Solusi P1-1 & P1-2)
	if ord, exists := d.PendingOrders[symbol]; exists {
		// Validasi Post-Only GTX hanya dilakukan sekali saat penempatan / konfirmasi book awal.
		// Setelah lolos diterima di order book (GTXValidated=true), pergeseran BBO menyentuh/melewati harga limit bukan lagi GTX reject, melainkan potensi fill antrean.
		if !ord.GTXValidated && ord.FilledQty.IsZero() && bestBid > 0 && bestAsk > 0 {
			if wouldCrossMakerBook(ord.Intent.Side, ord.Intent.LimitPrice, bestBid, bestAsk) {
				maxRepegs, maxDrift := resolveEntryRepegSettings(d.Cfg.MaxEntryRepegAttempts, d.Cfg.MaxEntryRepegDriftPct)
				if ord.RepegAttempts < maxRepegs {
					var ef model.ExchangeFilter
					if d.GetExchangeFilterFn != nil {
						ef, _ = d.GetExchangeFilterFn(symbol)
					}
					if updated, okRepeg, reason := repegLimitEntryIntent(ord.Intent, ord.OrigLimitPrice, bestBid, bestAsk, ef, maxDrift); okRepeg {
						slog.Info("dryrun: limit entry RE-PEGGED (prevented GTX cross spread)",
							"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
							"oldLimit", ord.Intent.LimitPrice, "newLimit", updated.LimitPrice,
							"bestBid", bestBid, "bestAsk", bestAsk, "attempt", ord.RepegAttempts+1)
						ord.Intent = updated
						ord.RepegAttempts++
						ord.GTXValidated = true
					} else {
						slog.Warn("dryrun: limit entry CANCELED (GTX post-only cross spread)",
							"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
							"limitPrice", ord.Intent.LimitPrice, "bestBid", bestBid, "bestAsk", bestAsk, "repegFailReason", reason)
						delete(d.PendingOrders, symbol)
						return
					}
				} else {
					slog.Warn("dryrun: limit entry CANCELED (GTX post-only cross spread)",
						"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
						"limitPrice", ord.Intent.LimitPrice, "bestBid", bestBid, "bestAsk", bestAsk, "repegFailReason", "max_attempts_reached")
					delete(d.PendingOrders, symbol)
					return
				}
			} else {
				ord.GTXValidated = true
			}
		}

		// Timeout antrean limit order
		if now.Sub(ord.Intent.CreatedAt) > d.Cfg.LimitOrderTimeout {
			slog.Warn("dryrun: limit entry EXPIRED (unfilled after timeout)",
				"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
				"limitPrice", ord.Intent.LimitPrice, "timeout", d.Cfg.LimitOrderTimeout,
				"filledQty", ord.FilledQty, "targetQty", ord.Intent.Qty)
			if ord.FilledQty.GreaterThan(decimal.Zero) {
				if pos, exists := d.OpenPositions[symbol]; exists {
					pos.InitialQty = ord.FilledQty
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
				}
			}
			delete(d.PendingOrders, symbol)
		} else if tradePrice > 0 && tradeQty > 0 {
			// Aggressor-Side Matching:
			// Buy Limit di Bid hanya terisi jika agresor adalah penjual (isBuyerMaker == true)
			// Sell Limit di Ask hanya terisi jika agresor adalah pembeli (isBuyerMaker == false)
			tpDec := decimal.NewFromFloat(tradePrice)
			tqDec := decimal.NewFromFloat(tradeQty)

			var matched bool
			if ord.Intent.Side == model.SideLong {
				matched = (tpDec.LessThanOrEqual(ord.Intent.LimitPrice)) && isBuyerMaker
			} else {
				matched = (tpDec.GreaterThanOrEqual(ord.Intent.LimitPrice)) && !isBuyerMaker
			}

			if matched {
				remainingQty := ord.Intent.Qty.Sub(ord.FilledQty)
				fillChunk := decimal.Min(remainingQty, tqDec)
				if fillChunk.GreaterThan(decimal.Zero) {
					ord.FilledQty = ord.FilledQty.Add(fillChunk)
					entryFee := ord.Intent.LimitPrice.Mul(fillChunk).Mul(decimal.NewFromFloat(d.Cfg.MakerFeeRate))
					d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(entryFee)

					// Solusi P1-2: Segera sinkronisasikan partial fill ke OpenPositions
					d.syncPartialFillToOpenPositionLocked(ord, fillChunk, entryFee, now)

					slog.Info("dryrun: limit entry FILLED",
						"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
						"side", ord.Intent.Side, "filledChunk", fillChunk,
						"cumFilled", ord.FilledQty, "targetQty", ord.Intent.Qty,
						"price", ord.Intent.LimitPrice)

					if ord.FilledQty.GreaterThanOrEqual(ord.Intent.Qty) {
						delete(d.PendingOrders, symbol)
					}
				}
			}
		}
	}

	// 2. Evaluasi Posisi Terbuka (Break-Even, SL, TP, dan Time Stop)
	if pos, exists := d.OpenPositions[symbol]; exists {
		if d.Cfg.MaxMarkStaleness > 0 && !pos.MarkUpdatedAt.IsZero() && now.Sub(pos.MarkUpdatedAt) > d.Cfg.MaxMarkStaleness {
			return // Tolak mengevaluasi exit menggunakan mark price stale (Solusi P1-8)
		}

		triggerRefPrice := pos.LastMarkPrice
		if triggerRefPrice.LessThanOrEqual(decimal.Zero) {
			return
		}

		if pos.Side == model.SideLong {
			if pos.PeakPrice.IsZero() || triggerRefPrice.GreaterThan(pos.PeakPrice) {
				pos.PeakPrice = triggerRefPrice
			}
		} else {
			if pos.PeakPrice.IsZero() || triggerRefPrice.LessThan(pos.PeakPrice) {
				pos.PeakPrice = triggerRefPrice
			}
		}

		chartPriceDec := decimal.Zero
		if tradePrice > 0 {
			chartPriceDec = decimal.NewFromFloat(tradePrice)
		} else if pos.Side == model.SideLong && bestBid > 0 {
			chartPriceDec = decimal.NewFromFloat(bestBid)
		} else if pos.Side == model.SideShort && bestAsk > 0 {
			chartPriceDec = decimal.NewFromFloat(bestAsk)
		}

		slTouched := false
		tp1Touched := false
		tpFullTouched := false
		if pos.Side == model.SideLong {
			slTouched = !pos.CurrentSLPrice.IsZero() && triggerRefPrice.LessThanOrEqual(pos.CurrentSLPrice)
			tp1Touched = !pos.TP1Price.IsZero() && (triggerRefPrice.GreaterThanOrEqual(pos.TP1Price) || (!chartPriceDec.IsZero() && chartPriceDec.GreaterThanOrEqual(pos.TP1Price)))
			tpFullTouched = !pos.TPPrice.IsZero() && !chartPriceDec.IsZero() && chartPriceDec.GreaterThanOrEqual(pos.TPPrice)
		} else {
			slTouched = !pos.CurrentSLPrice.IsZero() && triggerRefPrice.GreaterThanOrEqual(pos.CurrentSLPrice)
			tp1Touched = !pos.TP1Price.IsZero() && (triggerRefPrice.LessThanOrEqual(pos.TP1Price) || (!chartPriceDec.IsZero() && chartPriceDec.LessThanOrEqual(pos.TP1Price)))
			tpFullTouched = !pos.TPPrice.IsZero() && !chartPriceDec.IsZero() && chartPriceDec.LessThanOrEqual(pos.TPPrice)
		}

		// 1. Stop Loss FIRST (pessimistic intrabar conflict parity with SimBroker, plus SL Gap-Fill support)
		if slTouched {
			baseSL := pos.CurrentSLPrice
			if d.Cfg.SLGapFillEnabled {
				if pos.Side == model.SideLong && triggerRefPrice.LessThan(baseSL) {
					baseSL = triggerRefPrice
				} else if pos.Side == model.SideShort && triggerRefPrice.GreaterThan(baseSL) {
					baseSL = triggerRefPrice
				}
			}
			var exitPrice decimal.Decimal
			if pos.Side == model.SideLong {
				exitPrice = baseSL.Mul(decimal.NewFromFloat(1.0 - d.Cfg.SlippageRate))
			} else {
				exitPrice = baseSL.Mul(decimal.NewFromFloat(1.0 + d.Cfg.SlippageRate))
			}
			exitReason := "STOP_LOSS"
			if pos.ProtectionTier == 3 {
				exitReason = "TRAILING_STOP"
			} else if pos.ProtectionTier == 2 {
				exitReason = "PROFIT_LOCK"
			} else if pos.IsBreakEvenMoved {
				exitReason = "BREAK_EVEN"
			}
			closedRecs = append(closedRecs, d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, exitReason))
			delete(d.PendingOrders, symbol)
			return
		}

		// 2. Partial Take Profit (Scale-Out) - Maker Limit execution
		protCfg := d.toProtectionConfigFor(pos.StrategyID)
		if tp1Touched && protCfg.PartialTPEnabled && !pos.IsTP1Hit && !pos.TP1Price.IsZero() {
			d.executePartialTP1Locked(pos, pos.TP1Price, d.Cfg.MakerFeeRate)
		}

		// 3. Full Take Profit
		if tpFullTouched && !pos.TPPrice.IsZero() && (!protCfg.TrailingEnabled || !pos.IsTP1Hit) {
			closedRecs = append(closedRecs, d.closePositionLocked(pos, pos.TPPrice, d.Cfg.TakerFeeRate, "TAKE_PROFIT"))
			delete(d.PendingOrders, symbol)
			return
		}

		// 4. Unified Position Protection Engine (Tier 1 Early BE, Tier 2 Profit Lock, Tier 3 Trailing Ratchet)
		var tickSize decimal.Decimal
		if d.GetExchangeFilterFn != nil {
			if f, ok := d.GetExchangeFilterFn(symbol); ok {
				tickSize = f.TickSize
			}
		}
		protDec := protection.EvaluatePositionProtection(pos.toSnapshot(tickSize), triggerRefPrice, protCfg)
		if protDec.ShouldUpdate {
			pos.CurrentSLPrice = protDec.NewSLPrice
			pos.IsBreakEvenMoved = true
			pos.ProtectionTier = int(protDec.NewTier)
			d.enqueuePositionEvent(PositionEvent{
				Type: PositionEventSave,
				Pos:  *pos,
			})
			slog.Info("dryrun: protection updated",
				"symbol", symbol, "tier", protDec.NewTier, "reason", protDec.TriggerReason,
				"newSL", protDec.NewSLPrice, "peakPrice", pos.PeakPrice, "triggerPrice", triggerRefPrice)
		}

		// 5. Underwater Stale-Loss Time-Cut
		staleDur := protection.ResolveStaleLossExitDuration(d.Cfg.StaleLossExitDuration, d.Cfg.StrategyExitProfiles, pos.StrategyID)
		if protection.ShouldTriggerStaleLossExit(pos.Side, pos.EntryPrice, triggerRefPrice, pos.OpenTime, now, staleDur) {
			exitPrice := d.applyMarketExitSlippage(pos.Side, triggerRefPrice)
			closedRecs = append(closedRecs, d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, "TIME_STOP_STALE_LOSS"))
			delete(d.PendingOrders, symbol)
			return
		}

		// 6. Time-Stop Max Holding (termasuk SlippageRate agar paritas dengan market close & SimBroker)
		maxHoldDur := protection.ResolveMaxHoldingDuration(d.Cfg.MaxHoldingDuration, d.Cfg.StrategyExitProfiles, pos.StrategyID)
		if maxHoldDur > 0 && now.Sub(pos.OpenTime) > maxHoldDur {
			exitPrice := d.applyMarketExitSlippage(pos.Side, triggerRefPrice)
			closedRecs = append(closedRecs, d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, "TIME_STOP_MAX_HOLDING"))
			delete(d.PendingOrders, symbol)
			return
		}
	}
}

func (d *DryRunBroker) applyMarketExitSlippage(side model.Side, refPrice decimal.Decimal) decimal.Decimal {
	if d.Cfg.SlippageRate <= 0 {
		return refPrice
	}
	if side == model.SideLong {
		return refPrice.Mul(decimal.NewFromFloat(1.0 - d.Cfg.SlippageRate))
	}
	return refPrice.Mul(decimal.NewFromFloat(1.0 + d.Cfg.SlippageRate))
}

// ApplyFundingTick memproses settlement biaya pendanaan tiap batas 8 jam UTC (00:00, 08:00, 16:00).
func (d *DryRunBroker) ApplyFundingTick(symbol string, markPrice, fundingRate float64, now time.Time) {
	if markPrice <= 0 || math.IsNaN(markPrice) || math.IsNaN(fundingRate) || math.IsInf(fundingRate, 0) {
		return
	}
	currEpoch := now.UTC().Unix() / 28800

	d.Mu.Lock()
	defer d.Mu.Unlock()

	pos, exists := d.OpenPositions[symbol]
	if !exists || pos == nil || pos.Qty.LessThanOrEqual(decimal.Zero) {
		return
	}
	if pos.LastFundingEpoch == 0 {
		pos.LastFundingEpoch = currEpoch
		return
	}
	if currEpoch <= pos.LastFundingEpoch {
		return
	}

	fundingFee := pos.Qty.Mul(decimal.NewFromFloat(markPrice)).Mul(decimal.NewFromFloat(fundingRate))
	if pos.Side == model.SideLong {
		pos.FundingPaid = pos.FundingPaid.Add(fundingFee)
		d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(fundingFee)
	} else {
		pos.FundingPaid = pos.FundingPaid.Sub(fundingFee)
		d.RealizedEquityUSDT = d.RealizedEquityUSDT.Add(fundingFee)
	}
	pos.LastFundingEpoch = currEpoch
	d.enqueuePositionEvent(PositionEvent{
		Type: PositionEventSave,
		Pos:  *pos,
	})
}

func (d *DryRunBroker) syncPartialFillToOpenPositionLocked(ord *VirtualPendingOrder, fillChunk, chunkFee decimal.Decimal, now time.Time) {
	if fillChunk.LessThanOrEqual(decimal.Zero) {
		return
	}
	pos, exists := d.OpenPositions[ord.Intent.Symbol]
	if exists && pos.IsTP1Hit {
		slog.Warn("dryrun: rejecting entry fill because position already reached TP1 milestone", "symbol", ord.Intent.Symbol)
		delete(d.PendingOrders, ord.Intent.Symbol)
		return
	}
	if !exists {
		pos = &VirtualOpenPosition{
			ClientOrderID:    ord.Intent.ClientOrderID,
			Symbol:           ord.Intent.Symbol,
			Side:             ord.Intent.Side,
			StrategyID:       normalizeBrokerStrategyID(ord.Intent.StrategyID),
			Qty:              fillChunk,
			InitialQty:       fillChunk,
			EntryPrice:       ord.Intent.LimitPrice,
			EntryFee:         chunkFee,
			CurrentSLPrice:   ord.Intent.SLPrice,
			InitialSLPrice:   ord.Intent.SLPrice,
			TPPrice:          ord.Intent.TPPrice,
			TP1Price:         ord.Intent.TP1Price,
			ATRAtEntry:       ord.Intent.ATRAtEntry,
			LastMarkPrice:    ord.Intent.LimitPrice,
			MarkUpdatedAt:    now,
			OpenTime:         now,
			IsBreakEvenMoved: false,
			SignalAuditID:    ord.Intent.SignalAuditID,
			PeakPrice:        ord.Intent.LimitPrice,
			LastFundingEpoch: now.UTC().Unix() / 28800,
		}
		d.OpenPositions[ord.Intent.Symbol] = pos
	} else {
		// Posisi sudah ada (bisa jadi sebagian sudah di-close oleh TP1).
		// Tambahkan kuantitas chunk baru ke Qty & InitialQty secara inkremental tanpa menimpa status runner TP1.
		pos.Qty = pos.Qty.Add(fillChunk)
		pos.InitialQty = pos.InitialQty.Add(fillChunk)
		pos.EntryFee = pos.EntryFee.Add(chunkFee)
	}
	d.enqueuePositionEvent(PositionEvent{
		Type: PositionEventSave,
		Pos:  *pos,
	})
}

func (d *DryRunBroker) executePartialTP1Locked(pos *VirtualOpenPosition, exitPrice decimal.Decimal, feeRate float64) {
	// Batalkan antrean pending entry yang masih tersisa agar tidak terisi setelah TP1 milestone tercapai
	if pending, exists := d.PendingOrders[pos.Symbol]; exists {
		slog.Info("dryrun: canceling remaining pending entry order upon TP1 hit", "symbol", pos.Symbol, "clientOrderID", pending.Intent.ClientOrderID)
		delete(d.PendingOrders, pos.Symbol)
	}

	var ef model.ExchangeFilter
	if d.GetExchangeFilterFn != nil {
		ef, _ = d.GetExchangeFilterFn(pos.Symbol)
	}
	splitPlan := protection.PlanPartialTP1Split(pos.InitialQty, pos.Qty, decimal.Zero, exitPrice, d.Cfg.PartialTP1Ratio, ef)
	if !splitPlan.SplitPossible {
		// Ukuran posisi terlalu kecil untuk dipecah (atau MinNotional < 5 USDT).
		// Anggap TP1 milestone tercapai: tandai IsTP1Hit, geser SL ke BE, biarkan 100% posisi berjalan sebagai runner.
		pos.IsTP1Hit = true
		newSL := protection.CalculateBreakEvenSL(pos.toSnapshot(ef.TickSize), d.toProtectionConfigFor(pos.StrategyID))
		if pos.Side == model.SideLong {
			if newSL.GreaterThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		} else {
			if newSL.LessThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		}
		pos.IsBreakEvenMoved = true
		if pos.ProtectionTier < 1 {
			pos.ProtectionTier = 1
		}

		slog.Info("dryrun: position notional too small for partial TP1 split (< 5 USDT); moving SL to BE and letting 100% position run as runner",
			"symbol", pos.Symbol, "qty", pos.Qty, "exitPrice", exitPrice, "newSL", pos.CurrentSLPrice)

		d.enqueuePositionEvent(PositionEvent{
			Type: PositionEventSave,
			Pos:  *pos,
		})
		return
	}
	closeQty := splitPlan.CloseQty

	var grossPnL decimal.Decimal
	if pos.Side == model.SideLong {
		grossPnL = exitPrice.Sub(pos.EntryPrice).Mul(closeQty)
	} else {
		grossPnL = pos.EntryPrice.Sub(exitPrice).Mul(closeQty)
	}
	exitFee := exitPrice.Mul(closeQty).Mul(decimal.NewFromFloat(feeRate))

	initQty := pos.InitialQty
	if initQty.IsZero() {
		initQty = pos.Qty
	}
	entryFeePortion := pos.EntryFee.Mul(closeQty).Div(initQty)

	var fundingLeg decimal.Decimal
	if pos.Qty.GreaterThan(decimal.Zero) && !pos.FundingPaid.IsZero() {
		fundingLeg = pos.FundingPaid.Mul(closeQty).Div(pos.Qty)
	}
	pos.FundingPaid = pos.FundingPaid.Sub(fundingLeg)

	netPnL := grossPnL.Sub(entryFeePortion).Sub(exitFee).Sub(fundingLeg)

	// Saldo akun saat entry sudah didebet pos.EntryFee (dan funding saat epoch), maka penambahan saat exit adalah: grossPnL - exitFee
	d.RealizedEquityUSDT = d.RealizedEquityUSDT.Add(grossPnL.Sub(exitFee))
	pos.Qty = pos.Qty.Sub(closeQty)
	pos.IsTP1Hit = true

	// Pindahkan SL ke Break-Even via Unified Protection Engine
	newSL := protection.CalculateBreakEvenSL(pos.toSnapshot(ef.TickSize), d.toProtectionConfigFor(pos.StrategyID))
	if pos.Side == model.SideLong {
		if newSL.GreaterThan(pos.CurrentSLPrice) {
			pos.CurrentSLPrice = newSL
		}
	} else {
		if newSL.LessThan(pos.CurrentSLPrice) {
			pos.CurrentSLPrice = newSL
		}
	}
	pos.IsBreakEvenMoved = true
	if pos.ProtectionTier < 1 {
		pos.ProtectionTier = 1
	}

	d.enqueuePositionEvent(PositionEvent{
		Type: PositionEventSave,
		Pos:  *pos,
	})

	now := time.Now().UTC()
	if d.OnTradeClosedFn != nil {
		slPriceTP1 := pos.InitialSLPrice
		if slPriceTP1.IsZero() {
			slPriceTP1 = pos.CurrentSLPrice
		}
		tpPriceTP1 := pos.TP1Price
		if tpPriceTP1.IsZero() {
			tpPriceTP1 = pos.TPPrice
		}
		tradeRec := TradeClosedRecord{
			ClientOrderID: fmt.Sprintf("%s_TP1", pos.ClientOrderID),
			Symbol:        pos.Symbol,
			Side:          string(pos.Side),
			StrategyID:    normalizeBrokerStrategyID(pos.StrategyID),
			Qty:           closeQty,
			EntryPrice:    pos.EntryPrice,
			ExitPrice:     exitPrice,
			SLPrice:       slPriceTP1,
			TPPrice:       tpPriceTP1,
			NetPnL:        netPnL,
			MAE:           pos.MaxMAE,
			MFE:           pos.MaxMFE,
			ExitReason:    "PARTIAL_TAKE_PROFIT",
			OpenedAt:      pos.OpenTime,
			ClosedAt:      now,
			SignalAuditID: pos.SignalAuditID,
		}
		cb := d.OnTradeClosedFn
		d.goAsync(func() { cb(tradeRec) })
	}

	slog.Info("dryrun: TP1 Partial Take Profit filled",
		"symbol", pos.Symbol,
		"clientOrderID", pos.ClientOrderID,
		"closedQty", closeQty,
		"remainingQty", pos.Qty,
		"exitPrice", exitPrice,
		"netPnL", netPnL,
		"newSL", pos.CurrentSLPrice)
}

func (d *DryRunBroker) closePositionLocked(pos *VirtualOpenPosition, exitPrice decimal.Decimal, feeRate float64, reason string) TradeClosedRecord {
	var grossPnL decimal.Decimal
	if pos.Side == model.SideLong {
		grossPnL = exitPrice.Sub(pos.EntryPrice).Mul(pos.Qty)
	} else {
		grossPnL = pos.EntryPrice.Sub(exitPrice).Mul(pos.Qty)
	}
	exitFee := exitPrice.Mul(pos.Qty).Mul(decimal.NewFromFloat(feeRate))

	entryFeePortion := pos.EntryFee
	if !pos.InitialQty.IsZero() && pos.InitialQty.GreaterThan(pos.Qty) {
		entryFeePortion = pos.EntryFee.Mul(pos.Qty).Div(pos.InitialQty)
	}
	fundingLeg := pos.FundingPaid
	pos.FundingPaid = decimal.Zero

	// Round-Trip True Net PnL = Gross PnL - Entry Fee - Exit Fee - Funding Paid
	netPnL := grossPnL.Sub(entryFeePortion).Sub(exitFee).Sub(fundingLeg)
	// Saldo akun saat entry sudah didebet pos.EntryFee (dan funding saat epoch), maka penambahan saat exit adalah: grossPnL - exitFee
	d.RealizedEquityUSDT = d.RealizedEquityUSDT.Add(grossPnL.Sub(exitFee))

	now := time.Now().UTC()
	clientOrderID := pos.ClientOrderID
	if clientOrderID == "" {
		clientOrderID = fmt.Sprintf("SIM-%s-%d", pos.Symbol, pos.OpenTime.UnixNano())
	}

	slToLog := pos.InitialSLPrice
	if slToLog.IsZero() {
		slToLog = pos.CurrentSLPrice
	}

	rec := TradeClosedRecord{
		ClientOrderID: clientOrderID,
		Symbol:        pos.Symbol,
		Side:          string(pos.Side),
		StrategyID:    normalizeBrokerStrategyID(pos.StrategyID),
		Qty:           pos.Qty,
		EntryPrice:    pos.EntryPrice,
		ExitPrice:     exitPrice,
		SLPrice:       slToLog,
		TPPrice:       pos.TPPrice,
		NetPnL:        netPnL,
		MAE:           pos.MaxMAE,
		MFE:           pos.MaxMFE,
		ExitReason:    reason,
		OpenedAt:      pos.OpenTime,
		ClosedAt:      now,
		SignalAuditID: pos.SignalAuditID,
	}

	delete(d.OpenPositions, pos.Symbol)
	d.PeakEffectiveEquityUSDT, _ = protection.UpdatePeakAndDrawdownPct(d.PeakEffectiveEquityUSDT, d.calculateEffectiveEquityLocked())
	_ = d.isDailyDrawdownBreachedLocked()

	d.enqueuePositionEvent(PositionEvent{
		Type:                  PositionEventDelete,
		ClientOrderIDOrSymbol: clientOrderID,
	})

	if d.OnTradeClosedFn != nil {
		cb := d.OnTradeClosedFn
		d.goAsync(func() { cb(rec) })
	}
	return rec
}

func (d *DryRunBroker) calculateEffectiveEquityLocked() decimal.Decimal {
	unrealized := decimal.Zero
	for _, pos := range d.OpenPositions {
		if pos.LastMarkPrice.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				unrealized = unrealized.Add(pos.LastMarkPrice.Sub(pos.EntryPrice).Mul(pos.Qty))
			} else {
				unrealized = unrealized.Add(pos.EntryPrice.Sub(pos.LastMarkPrice).Mul(pos.Qty))
			}
		}
	}
	return d.RealizedEquityUSDT.Add(unrealized)
}

func (d *DryRunBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return d.calculateEffectiveEquityLocked()
}

// GetRealizedEquityUSDT mengembalikan realized equity saat ini di memory
func (d *DryRunBroker) GetRealizedEquityUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return d.RealizedEquityUSDT
}

// GetPureRealizedEquityClosedTradesUSDT mengembalikan realized equity murni dari seluruh trade yang telah ditutup
// (RealizedEquityUSDT ditambah kembali entry fee dan funding posisi terbuka) agar tidak terjadi
// double-deduction saat state dipulihkan setelah restart.
func (d *DryRunBroker) GetPureRealizedEquityClosedTradesUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	pure := d.RealizedEquityUSDT
	for _, pos := range d.OpenPositions {
		entryFeeRem := pos.EntryFee
		if !pos.InitialQty.IsZero() && pos.InitialQty.GreaterThan(pos.Qty) {
			entryFeeRem = pos.EntryFee.Mul(pos.Qty).Div(pos.InitialQty)
		}
		pure = pure.Add(entryFeeRem).Add(pos.FundingPaid)
	}
	return pure
}

func (d *DryRunBroker) GetAvailableBalanceUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	usedMargin := decimal.Zero
	lev := decimal.NewFromFloat(d.Cfg.AccountLeverage)

	for _, pos := range d.OpenPositions {
		notional := pos.EntryPrice.Mul(pos.Qty)
		usedMargin = usedMargin.Add(notional.Div(lev))
	}
	for _, ord := range d.PendingOrders {
		remQty := ord.Intent.Qty.Sub(ord.FilledQty)
		if remQty.GreaterThan(decimal.Zero) {
			notional := ord.Intent.LimitPrice.Mul(remQty)
			usedMargin = usedMargin.Add(notional.Div(lev))
		}
	}

	effEquity := d.calculateEffectiveEquityLocked()
	avail := effEquity.Sub(usedMargin)
	if avail.LessThan(decimal.Zero) {
		return decimal.Zero
	}
	return avail
}

func (d *DryRunBroker) GetDailyTotalDrawdownPct() float64 {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	if d.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		return 0.0
	}
	effEquity := d.calculateEffectiveEquityLocked()
	ddDec := effEquity.Sub(d.SessionStartEquity).Div(d.SessionStartEquity)
	ddFloat, _ := ddDec.Float64()
	return ddFloat
}

// SetPeakEffectiveEquityUSDT raises the dry-run high-water mark equity if peak is higher than current peak.
func (d *DryRunBroker) SetPeakEffectiveEquityUSDT(peak decimal.Decimal) {
	if peak.LessThanOrEqual(decimal.Zero) {
		return
	}
	d.Mu.Lock()
	defer d.Mu.Unlock()
	if peak.GreaterThan(d.PeakEffectiveEquityUSDT) {
		d.PeakEffectiveEquityUSDT = peak
	}
}

// GetDrawdownFromPeakPct returns non-negative drawdown fraction from running HWM equity.
func (d *DryRunBroker) GetDrawdownFromPeakPct() float64 {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	var dd float64
	d.PeakEffectiveEquityUSDT, dd = protection.UpdatePeakAndDrawdownPct(d.PeakEffectiveEquityUSDT, d.calculateEffectiveEquityLocked())
	return dd
}

func (d *DryRunBroker) IsCircuitBreakerTripped() bool {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return d.isDailyDrawdownBreachedLocked()
}

func (d *DryRunBroker) GetActiveSlotCount() int {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	activeSymbols := make(map[string]struct{})
	for sym := range d.OpenPositions {
		activeSymbols[sym] = struct{}{}
	}
	for sym := range d.PendingOrders {
		activeSymbols[sym] = struct{}{}
	}
	return len(activeSymbols)
}

func (d *DryRunBroker) HasActiveOrderOrPosition(symbol string) bool {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	_, hasOrd := d.PendingOrders[symbol]
	_, hasPos := d.OpenPositions[symbol]
	return hasOrd || hasPos
}

func (d *DryRunBroker) GetMonitoredSymbols() []string {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	syms := make([]string, 0, len(d.OpenPositions)+len(d.PendingOrders))
	for sym := range d.OpenPositions {
		syms = append(syms, sym)
	}
	for sym := range d.PendingOrders {
		if !slices.Contains(syms, sym) {
			syms = append(syms, sym)
		}
	}
	return syms
}

func (d *DryRunBroker) CheckTimeouts(now time.Time) {
	var closedRecs []TradeClosedRecord
	d.Mu.Lock()
	defer func() {
		d.Mu.Unlock()
		if len(closedRecs) > 0 {
			for _, rec := range closedRecs {
				if d.OnStrategyCooldownRecordedFn != nil {
					d.OnStrategyCooldownRecordedFn(rec.Symbol, rec.StrategyID, rec.ClosedAt, rec.ExitReason)
				}
				if d.OnCooldownRecordedFn != nil {
					d.OnCooldownRecordedFn(rec.Symbol, rec.ClosedAt, rec.ExitReason)
				}
			}
		}
	}()

	if d.isDailyDrawdownBreachedLocked() && len(d.PendingOrders) > 0 {
		d.cancelAllPendingOrdersLocked("CIRCUIT_BREAKER_BREACHED")
	}

	if d.Cfg.LimitOrderTimeout > 0 {
		for sym, ord := range d.PendingOrders {
			if now.Sub(ord.Intent.CreatedAt) > d.Cfg.LimitOrderTimeout {
				slog.Warn("dryrun: pending limit order timed out via CheckTimeouts",
					"symbol", sym, "clientOrderID", ord.Intent.ClientOrderID,
					"limitPrice", ord.Intent.LimitPrice, "timeout", d.Cfg.LimitOrderTimeout)
				if ord.FilledQty.GreaterThan(decimal.Zero) {
					if pos, exists := d.OpenPositions[sym]; exists {
						pos.InitialQty = ord.FilledQty
						d.enqueuePositionEvent(PositionEvent{
							Type: PositionEventSave,
							Pos:  *pos,
						})
					}
				}
				delete(d.PendingOrders, sym)
			}
		}
	}

	if d.Cfg.MaxHoldingDuration > 0 || d.Cfg.StaleLossExitDuration > 0 || len(d.Cfg.StrategyExitProfiles) > 0 {
		for sym, pos := range d.OpenPositions {
			markFresh := d.Cfg.MaxMarkStaleness <= 0 || pos.MarkUpdatedAt.IsZero() || now.Sub(pos.MarkUpdatedAt) <= d.Cfg.MaxMarkStaleness
			if !markFresh {
				continue
			}
			reason := ""
			maxHoldDur := protection.ResolveMaxHoldingDuration(d.Cfg.MaxHoldingDuration, d.Cfg.StrategyExitProfiles, pos.StrategyID)
			staleDur := protection.ResolveStaleLossExitDuration(d.Cfg.StaleLossExitDuration, d.Cfg.StrategyExitProfiles, pos.StrategyID)
			if maxHoldDur > 0 && now.Sub(pos.OpenTime) > maxHoldDur {
				reason = "TIME_STOP_MAX_HOLDING"
			} else if staleDur > 0 {
				if protection.ShouldTriggerStaleLossExit(pos.Side, pos.EntryPrice, pos.LastMarkPrice, pos.OpenTime, now, staleDur) {
					reason = "TIME_STOP_STALE_LOSS"
				}
			}
			if reason != "" && pos.LastMarkPrice.GreaterThan(decimal.Zero) {
				exitPrice := d.applyMarketExitSlippage(pos.Side, pos.LastMarkPrice)
				closedRecs = append(closedRecs, d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, reason))
				delete(d.PendingOrders, sym)
			}
		}
	}

	// Sinkronisasi periodik MAE, MFE, dan Mark Price terkini ke SQLite setiap 5 detik
	if len(d.OpenPositions) > 0 {
		if d.lastPeriodicSync.IsZero() {
			d.lastPeriodicSync = now
		} else if now.Sub(d.lastPeriodicSync) >= 5*time.Second {
			d.lastPeriodicSync = now
			for _, pos := range d.OpenPositions {
				d.enqueuePositionEvent(PositionEvent{
					Type: PositionEventSave,
					Pos:  *pos,
				})
			}
		}
	}
}

func (d *DryRunBroker) GetActivePositions() []ActivePositionDTO {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	now := time.Now().UTC()
	result := make([]ActivePositionDTO, 0, len(d.OpenPositions))
	for _, pos := range d.OpenPositions {
		entryF, _ := pos.EntryPrice.Float64()
		markF, _ := pos.LastMarkPrice.Float64()
		slF, _ := pos.CurrentSLPrice.Float64()
		tpF, _ := pos.TPPrice.Float64()

		var pnlF float64
		var roeF float64
		if pos.LastMarkPrice.GreaterThan(decimal.Zero) && pos.EntryPrice.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				pnlDec := pos.LastMarkPrice.Sub(pos.EntryPrice).Mul(pos.Qty)
				pnlF, _ = pnlDec.Float64()
				roeF = ((markF - entryF) / entryF) * d.Cfg.AccountLeverage * 100.0
			} else {
				pnlDec := pos.EntryPrice.Sub(pos.LastMarkPrice).Mul(pos.Qty)
				pnlF, _ = pnlDec.Float64()
				roeF = ((entryF - markF) / entryF) * d.Cfg.AccountLeverage * 100.0
			}
		}

		result = append(result, ActivePositionDTO{
			Symbol:           pos.Symbol,
			Side:             string(pos.Side),
			StrategyID:       normalizeBrokerStrategyID(pos.StrategyID),
			Qty:              pos.Qty.String(),
			EntryPrice:       entryF,
			MarkPrice:        markF,
			SLPrice:          slF,
			TPPrice:          tpF,
			UnrealizedPnL:    pnlF,
			UnrealizedROEPct: roeF,
			OpenTimeUTC:      pos.OpenTime.Format(time.RFC3339),
			DurationSec:      int64(now.Sub(pos.OpenTime).Seconds()),
			IsBreakEven:      pos.IsBreakEvenMoved,
			ProtectionTier:   pos.ProtectionTier,
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

// GetActiveSlotCountByFilter counts unique active symbols whose StrategyID satisfies match.
func (d *DryRunBroker) GetActiveSlotCountByFilter(match func(strategyID string) bool) int {
	if match == nil {
		return d.GetActiveSlotCount()
	}
	d.Mu.Lock()
	defer d.Mu.Unlock()
	activeSymbols := make(map[string]struct{})
	for sym, pos := range d.OpenPositions {
		if pos != nil && match(normalizeBrokerStrategyID(pos.StrategyID)) {
			activeSymbols[sym] = struct{}{}
		}
	}
	for sym, ord := range d.PendingOrders {
		if _, hasOpen := d.OpenPositions[sym]; hasOpen {
			continue
		}
		if ord != nil && match(normalizeBrokerStrategyID(ord.Intent.StrategyID)) {
			activeSymbols[sym] = struct{}{}
		}
	}
	return len(activeSymbols)
}

func (d *DryRunBroker) toProtectionConfigFor(strategyID string) protection.ProtectionConfig {
	return protection.ResolveProtectionConfig(d.toProtectionConfig(), d.Cfg.StrategyExitProfiles, normalizeBrokerStrategyID(strategyID))
}

func (d *DryRunBroker) toProtectionConfig() protection.ProtectionConfig {
	t1Long := d.Cfg.Tier1BETriggerLongATR
	if t1Long <= 0 {
		t1Long = d.Cfg.BreakEvenTriggerATR
	}
	t1Short := d.Cfg.Tier1BETriggerShortATR
	if t1Short <= 0 {
		t1Short = d.Cfg.BreakEvenTriggerATR
	}
	return protection.ProtectionConfig{
		Tier1BEEnabled:           d.Cfg.Tier1BEEnabled,
		Tier1BETriggerLongATR:    t1Long,
		Tier1BETriggerShortATR:   t1Short,
		Tier2ProfitLockEnabled:   d.Cfg.Tier2ProfitLockEnabled,
		Tier2ProfitLockATR:       d.Cfg.Tier2ProfitLockATR,
		Tier2ProfitLockOffsetATR: d.Cfg.Tier2ProfitLockOffsetATR,
		TrailingEnabled:          d.Cfg.TrailingEnabled,
		TrailingActivateATR:      d.Cfg.TrailingActivateATR,
		TrailingDistanceATR:      d.Cfg.TrailingDistanceATR,
		PartialTPEnabled:         d.Cfg.PartialTPEnabled,
		MinBreakEvenOffsetPct:    d.Cfg.MinBreakEvenOffsetPct,
		RunnerBECushionATR:       d.Cfg.RunnerBECushionATR,
		TakerFeeRate:             decimal.NewFromFloat(d.Cfg.TakerFeeRate),
		SlippageRate:             decimal.NewFromFloat(d.Cfg.SlippageRate),
	}
}

func (pos *VirtualOpenPosition) toSnapshot(tickSize decimal.Decimal) protection.PositionSnapshot {
	return protection.PositionSnapshot{
		Symbol:         pos.Symbol,
		Side:           pos.Side,
		EntryPrice:     pos.EntryPrice,
		CurrentSLPrice: pos.CurrentSLPrice,
		InitialSLPrice: pos.InitialSLPrice,
		PeakPrice:      pos.PeakPrice,
		ATRAtEntry:     pos.ATRAtEntry,
		ProtectionTier: protection.ProtectionTier(pos.ProtectionTier),
		IsTP1Hit:       pos.IsTP1Hit,
		TickSize:       tickSize,
	}
}

// AttachSignalAuditID melampirkan signal_audit ID ke pending order atau posisi aktif setelah penulisan SQLite selesai paralel.
func (d *DryRunBroker) AttachSignalAuditID(symbol, clientOrderID string, signalAuditID int64) {
	if signalAuditID <= 0 {
		return
	}
	d.Mu.Lock()
	defer d.Mu.Unlock()

	if po, exists := d.PendingOrders[symbol]; exists && po != nil && po.Intent.SignalAuditID == 0 && matchesEntryClientID(po.Intent.ClientOrderID, clientOrderID) {
		po.Intent.SignalAuditID = signalAuditID
	}
	if pos, exists := d.OpenPositions[symbol]; exists && pos != nil && pos.SignalAuditID == 0 && matchesEntryClientID(pos.ClientOrderID, clientOrderID) {
		pos.SignalAuditID = signalAuditID
		d.enqueuePositionEvent(PositionEvent{
			Type: PositionEventSave,
			Pos:  *pos,
		})
	}
}
