package broker

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type VirtualPendingOrder struct {
	Intent    model.OrderIntent
	FilledQty decimal.Decimal
}

type VirtualOpenPosition struct {
	ClientOrderID    string
	Symbol           string
	Side             model.Side
	Qty              decimal.Decimal
	EntryPrice       decimal.Decimal
	EntryFee         decimal.Decimal
	CurrentSLPrice   decimal.Decimal
	TPPrice          decimal.Decimal
	ATRAtEntry       decimal.Decimal
	LastMarkPrice    decimal.Decimal
	MarkUpdatedAt    time.Time
	OpenTime         time.Time
	IsBreakEvenMoved bool
	SignalAuditID    int64
	MaxMAE           float64
	MaxMFE           float64
}

type DryRunConfig struct {
	MaxSlots              int
	MaxDailyDrawdownPct   float64
	AccountLeverage       float64
	BreakEvenTriggerATR   float64
	MinBreakEvenOffsetPct float64
	LimitOrderTimeout     time.Duration
	MaxHoldingDuration    time.Duration
	MaxMarkStaleness      time.Duration
	MakerFeeRate          float64
	TakerFeeRate          float64
	SlippageRate          float64
	EntryOrderType        string
}

type TradeClosedRecord struct {
	ClientOrderID string
	Symbol        string
	Side          string
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

type DryRunBroker struct {
	Mu                    sync.Mutex
	Cfg                   DryRunConfig
	SessionDayEpochUTC    int64
	SessionStartEquity    decimal.Decimal
	RealizedEquityUSDT    decimal.Decimal
	CircuitBreakerTripped bool
	PendingOrders         map[string]*VirtualPendingOrder
	OpenPositions         map[string]*VirtualOpenPosition
	OnTradeClosedFn       func(rec TradeClosedRecord)
}

func NewDryRunBroker(initialEquity decimal.Decimal, cfg DryRunConfig) *DryRunBroker {
	if cfg.MaxSlots <= 0 {
		cfg.MaxSlots = 3
	}
	if cfg.MaxDailyDrawdownPct == 0 {
		cfg.MaxDailyDrawdownPct = -0.03
	}
	if cfg.AccountLeverage <= 0 {
		cfg.AccountLeverage = 5.0
	}
	if cfg.BreakEvenTriggerATR <= 0 {
		cfg.BreakEvenTriggerATR = 1.15
	}
	if cfg.MinBreakEvenOffsetPct <= 0 {
		cfg.MinBreakEvenOffsetPct = 0.0010
	}
	if cfg.LimitOrderTimeout <= 0 {
		cfg.LimitOrderTimeout = 45 * time.Second
	}
	if cfg.MaxHoldingDuration <= 0 {
		cfg.MaxHoldingDuration = 8 * time.Hour
	}
	if cfg.MaxMarkStaleness <= 0 {
		cfg.MaxMarkStaleness = 15 * time.Second
	}
	return &DryRunBroker{
		Cfg:                cfg,
		SessionStartEquity: initialEquity,
		RealizedEquityUSDT: initialEquity,
		PendingOrders:      make(map[string]*VirtualPendingOrder),
		OpenPositions:      make(map[string]*VirtualOpenPosition),
	}
}

func (d *DryRunBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) {
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

	if strings.ToUpper(d.Cfg.EntryOrderType) == "MARKET" {
		fillPrice := intent.LimitPrice
		entryFee := fillPrice.Mul(intent.Qty).Mul(decimal.NewFromFloat(d.Cfg.TakerFeeRate))
		d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(entryFee)

		now := intent.CreatedAt
		if now.IsZero() {
			now = time.Now().UTC()
		}

		d.OpenPositions[intent.Symbol] = &VirtualOpenPosition{
			ClientOrderID:    intent.ClientOrderID,
			Symbol:           intent.Symbol,
			Side:             intent.Side,
			Qty:              intent.Qty,
			EntryPrice:       fillPrice,
			EntryFee:         entryFee,
			CurrentSLPrice:   intent.SLPrice,
			TPPrice:          intent.TPPrice,
			ATRAtEntry:       intent.ATRAtEntry,
			LastMarkPrice:    fillPrice,
			MarkUpdatedAt:    now,
			OpenTime:         now,
			IsBreakEvenMoved: false,
			SignalAuditID:    intent.SignalAuditID,
		}

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
		Intent:    intent,
		FilledQty: decimal.Zero,
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

func (d *DryRunBroker) OnRealtimeTick(
	symbol string,
	bestBid, bestAsk, markPrice, tradePrice, tradeQty float64,
	isBuyerMaker bool,
	now time.Time,
) {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	// Solusi P1-7: Mark-to-market terlebih dahulu SEBELUM rollover baseline harian
	if markPrice > 0 {
		if pos, ok := d.OpenPositions[symbol]; ok {
			pos.LastMarkPrice = decimal.NewFromFloat(markPrice)
			pos.MarkUpdatedAt = now

			entryF, _ := pos.EntryPrice.Float64()
			if entryF > 0 {
				if pos.Side == model.SideLong {
					if markPrice > entryF {
						favPct := ((markPrice - entryF) / entryF) * 100.0
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
					if markPrice < entryF {
						favPct := ((entryF - markPrice) / entryF) * 100.0
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

	// 1. Evaluasi Antrean Limit Order Masuk (Solusi P1-1 & P1-2)
	if ord, exists := d.PendingOrders[symbol]; exists {
		// Validasi Post-Only (GTX reject)
		if ord.FilledQty.IsZero() && bestBid > 0 && bestAsk > 0 {
			if (ord.Intent.Side == model.SideLong && ord.Intent.LimitPrice.GreaterThanOrEqual(decimal.NewFromFloat(bestAsk))) ||
				(ord.Intent.Side == model.SideShort && ord.Intent.LimitPrice.LessThanOrEqual(decimal.NewFromFloat(bestBid))) {
				slog.Warn("dryrun: limit entry CANCELED (GTX post-only cross spread)",
					"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
					"limitPrice", ord.Intent.LimitPrice, "bestBid", bestBid, "bestAsk", bestAsk)
				delete(d.PendingOrders, symbol)
				return
			}
		}

		// Timeout 45 Detik
		if now.Sub(ord.Intent.CreatedAt) > d.Cfg.LimitOrderTimeout {
			slog.Warn("dryrun: limit entry EXPIRED (unfilled after timeout)",
				"symbol", symbol, "clientOrderID", ord.Intent.ClientOrderID,
				"limitPrice", ord.Intent.LimitPrice, "timeout", d.Cfg.LimitOrderTimeout,
				"filledQty", ord.FilledQty, "targetQty", ord.Intent.Qty)
			delete(d.PendingOrders, symbol)
			return
		}

		// Aggressor-Side Matching:
		// Buy Limit di Bid hanya terisi jika agresor adalah penjual (isBuyerMaker == true)
		// Sell Limit di Ask hanya terisi jika agresor adalah pembeli (isBuyerMaker == false)
		if tradePrice > 0 && tradeQty > 0 {
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
					d.syncPartialFillToOpenPositionLocked(ord, now)

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
		if !pos.MarkUpdatedAt.IsZero() && now.Sub(pos.MarkUpdatedAt) > d.Cfg.MaxMarkStaleness {
			return // Tolak mengevaluasi exit menggunakan mark price stale (Solusi P1-8)
		}

		triggerRefPrice := pos.LastMarkPrice
		if triggerRefPrice.LessThanOrEqual(decimal.Zero) {
			return
		}

		beTriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(d.Cfg.BreakEvenTriggerATR))
		beOffsetRate := decimal.NewFromFloat(math.Max(d.Cfg.MinBreakEvenOffsetPct, 2.0*d.Cfg.TakerFeeRate+d.Cfg.SlippageRate))

		if now.Sub(pos.OpenTime) > d.Cfg.MaxHoldingDuration {
			d.closePositionLocked(pos, triggerRefPrice, d.Cfg.TakerFeeRate, "TIME_STOP_MAX_HOLDING")
			delete(d.PendingOrders, symbol)
			return
		}

		if pos.Side == model.SideLong {
			if !pos.IsBreakEvenMoved && triggerRefPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(beTriggerDist) {
				pos.CurrentSLPrice = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffsetRate))
				pos.IsBreakEvenMoved = true
			}
			if triggerRefPrice.LessThanOrEqual(pos.CurrentSLPrice) {
				exitPrice := pos.CurrentSLPrice.Mul(decimal.NewFromFloat(1.0 - d.Cfg.SlippageRate))
				exitReason := "STOP_LOSS"
				if pos.IsBreakEvenMoved {
					exitReason = "BREAK_EVEN"
				}
				d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, exitReason)
				delete(d.PendingOrders, symbol)
				return
			}
			if triggerRefPrice.GreaterThanOrEqual(pos.TPPrice) {
				d.closePositionLocked(pos, pos.TPPrice, d.Cfg.TakerFeeRate, "TAKE_PROFIT")
				delete(d.PendingOrders, symbol)
				return
			}
		} else {
			if !pos.IsBreakEvenMoved && pos.EntryPrice.Sub(triggerRefPrice).GreaterThanOrEqual(beTriggerDist) {
				pos.CurrentSLPrice = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffsetRate))
				pos.IsBreakEvenMoved = true
			}
			if triggerRefPrice.GreaterThanOrEqual(pos.CurrentSLPrice) {
				exitPrice := pos.CurrentSLPrice.Mul(decimal.NewFromFloat(1.0 + d.Cfg.SlippageRate))
				exitReason := "STOP_LOSS"
				if pos.IsBreakEvenMoved {
					exitReason = "BREAK_EVEN"
				}
				d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, exitReason)
				delete(d.PendingOrders, symbol)
				return
			}
			if triggerRefPrice.LessThanOrEqual(pos.TPPrice) {
				d.closePositionLocked(pos, pos.TPPrice, d.Cfg.TakerFeeRate, "TAKE_PROFIT")
				delete(d.PendingOrders, symbol)
				return
			}
		}
	}
}

func (d *DryRunBroker) syncPartialFillToOpenPositionLocked(ord *VirtualPendingOrder, now time.Time) {
	if ord.FilledQty.LessThanOrEqual(decimal.Zero) {
		return
	}
	pos, exists := d.OpenPositions[ord.Intent.Symbol]
	entryFee := ord.Intent.LimitPrice.Mul(ord.FilledQty).Mul(decimal.NewFromFloat(d.Cfg.MakerFeeRate))
	if !exists {
		d.OpenPositions[ord.Intent.Symbol] = &VirtualOpenPosition{
			ClientOrderID:    ord.Intent.ClientOrderID,
			Symbol:           ord.Intent.Symbol,
			Side:             ord.Intent.Side,
			Qty:              ord.FilledQty,
			EntryPrice:       ord.Intent.LimitPrice,
			EntryFee:         entryFee,
			CurrentSLPrice:   ord.Intent.SLPrice,
			TPPrice:          ord.Intent.TPPrice,
			ATRAtEntry:       ord.Intent.ATRAtEntry,
			LastMarkPrice:    ord.Intent.LimitPrice,
			MarkUpdatedAt:    now,
			OpenTime:         now,
			IsBreakEvenMoved: false,
			SignalAuditID:    ord.Intent.SignalAuditID,
		}
	} else {
		pos.Qty = ord.FilledQty
		pos.EntryFee = entryFee
	}
}

func (d *DryRunBroker) closePositionLocked(pos *VirtualOpenPosition, exitPrice decimal.Decimal, feeRate float64, reason string) {
	var grossPnL decimal.Decimal
	if pos.Side == model.SideLong {
		grossPnL = exitPrice.Sub(pos.EntryPrice).Mul(pos.Qty)
	} else {
		grossPnL = pos.EntryPrice.Sub(exitPrice).Mul(pos.Qty)
	}
	exitFee := exitPrice.Mul(pos.Qty).Mul(decimal.NewFromFloat(feeRate))
	// Round-Trip True Net PnL = Gross PnL - Entry Fee - Exit Fee
	netPnL := grossPnL.Sub(pos.EntryFee).Sub(exitFee)
	// Saldo akun saat entry sudah didebet pos.EntryFee, maka penambahan saat exit adalah: grossPnL - exitFee
	d.RealizedEquityUSDT = d.RealizedEquityUSDT.Add(grossPnL.Sub(exitFee))


	now := time.Now().UTC()
	clientOrderID := pos.ClientOrderID
	if clientOrderID == "" {
		clientOrderID = fmt.Sprintf("SIM-%s-%d", pos.Symbol, pos.OpenTime.UnixNano())
	}

	rec := TradeClosedRecord{
		ClientOrderID: clientOrderID,
		Symbol:        pos.Symbol,
		Side:          string(pos.Side),
		Qty:           pos.Qty,
		EntryPrice:    pos.EntryPrice,
		ExitPrice:     exitPrice,
		SLPrice:       pos.CurrentSLPrice,
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

	if d.OnTradeClosedFn != nil {
		go d.OnTradeClosedFn(rec)
	}
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
		notional := ord.Intent.LimitPrice.Mul(ord.Intent.Qty)
		usedMargin = usedMargin.Add(notional.Div(lev))
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

func (d *DryRunBroker) CheckTimeouts(now time.Time) {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	if d.Cfg.LimitOrderTimeout > 0 {
		for sym, ord := range d.PendingOrders {
			if now.Sub(ord.Intent.CreatedAt) > d.Cfg.LimitOrderTimeout {
				slog.Warn("dryrun: pending limit order timed out via CheckTimeouts",
					"symbol", sym, "clientOrderID", ord.Intent.ClientOrderID,
					"limitPrice", ord.Intent.LimitPrice, "timeout", d.Cfg.LimitOrderTimeout)
				delete(d.PendingOrders, sym)
			}
		}
	}

	if d.Cfg.MaxHoldingDuration > 0 {
		for sym, pos := range d.OpenPositions {
			if now.Sub(pos.OpenTime) > d.Cfg.MaxHoldingDuration {
				triggerRefPrice := pos.LastMarkPrice
				if triggerRefPrice.GreaterThan(decimal.Zero) {
					d.closePositionLocked(pos, triggerRefPrice, d.Cfg.TakerFeeRate, "TIME_STOP_MAX_HOLDING")
					delete(d.PendingOrders, sym)
				}
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
		})
	}
	return result
}


