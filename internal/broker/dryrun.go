package broker

import (
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

type VirtualPendingOrder struct {
	Intent    model.OrderIntent
	FilledQty decimal.Decimal
}

type VirtualOpenPosition struct {
	ClientOrderID    string
	Symbol           string
	Side             model.Side
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
}

type DryRunConfig struct {
	MaxSlots                 int
	MaxDailyDrawdownPct      float64
	AccountLeverage          float64
	BreakEvenTriggerATR      float64
	Tier1BEEnabled           bool
	Tier1BETriggerLongATR    float64
	Tier1BETriggerShortATR   float64
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
	MaxMarkStaleness         time.Duration
	MakerFeeRate             float64
	TakerFeeRate             float64
	SlippageRate             float64
	EntryOrderType           string
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
	SessionStartEquity    decimal.Decimal
	RealizedEquityUSDT    decimal.Decimal
	CircuitBreakerTripped bool
	PendingOrders         map[string]*VirtualPendingOrder
	OpenPositions         map[string]*VirtualOpenPosition
	OnTradeClosedFn       func(rec TradeClosedRecord)
	OnPositionSavedFn     func(pos VirtualOpenPosition)
	OnPositionDeletedFn   func(clientOrderIDOrSymbol string)

	eventChan        chan PositionEvent
	eventStop        chan struct{}
	lastPeriodicSync time.Time
}

// RestoreOpenPosition memulihkan posisi virtual dari database ke memory saat startup
// dan menyesuaikan saldo realized equity dengan entryFee posisi agar akuntansi presisi
func (d *DryRunBroker) RestoreOpenPosition(pos *VirtualOpenPosition) {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	if pos.InitialQty.IsZero() {
		pos.InitialQty = pos.Qty
	}
	if pos.PeakPrice.IsZero() {
		pos.PeakPrice = pos.EntryPrice
	}
	if _, exists := d.OpenPositions[pos.Symbol]; !exists {
		d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(pos.EntryFee)
	}
	d.OpenPositions[pos.Symbol] = pos
}

func (d *DryRunBroker) enqueuePositionEvent(evt PositionEvent) {
	if d.eventChan == nil {
		return
	}
	select {
	case d.eventChan <- evt:
	default:
		slog.Warn("dryrun: persistence event channel full, dropping event", "type", evt.Type)
	}
}

func (d *DryRunBroker) startEventWorker() {
	go func() {
		for {
			select {
			case <-d.eventStop:
				for len(d.eventChan) > 0 {
					evt := <-d.eventChan
					d.dispatchPositionEvent(evt)
				}
				return
			case evt := <-d.eventChan:
				d.dispatchPositionEvent(evt)
			}
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
	if d.eventStop != nil {
		close(d.eventStop)
	}
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
	broker := &DryRunBroker{
		Cfg:                cfg,
		SessionStartEquity: initialEquity,
		RealizedEquityUSDT: initialEquity,
		PendingOrders:      make(map[string]*VirtualPendingOrder),
		OpenPositions:      make(map[string]*VirtualOpenPosition),
		eventChan:          make(chan PositionEvent, 256),
		eventStop:          make(chan struct{}),
		lastPeriodicSync:   time.Now().UTC(),
	}
	broker.startEventWorker()
	return broker
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

		pos := &VirtualOpenPosition{
			ClientOrderID:    intent.ClientOrderID,
			Symbol:           intent.Symbol,
			Side:             intent.Side,
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

		trailActivateATR := d.Cfg.TrailingActivateATR
		if trailActivateATR <= 0 {
			trailActivateATR = 2.00
		}
		trailDistATR := d.Cfg.TrailingDistanceATR
		if trailDistATR <= 0 {
			trailDistATR = 0.80
		}

		t1LongTriggerATR := d.Cfg.Tier1BETriggerLongATR
		if t1LongTriggerATR <= 0 {
			t1LongTriggerATR = d.Cfg.BreakEvenTriggerATR
		}
		if t1LongTriggerATR <= 0 {
			t1LongTriggerATR = 0.70
		}

		t1ShortTriggerATR := d.Cfg.Tier1BETriggerShortATR
		if t1ShortTriggerATR <= 0 {
			t1ShortTriggerATR = d.Cfg.BreakEvenTriggerATR
		}
		if t1ShortTriggerATR <= 0 {
			t1ShortTriggerATR = 0.90
		}

		t2TriggerATR := d.Cfg.Tier2ProfitLockATR
		if t2TriggerATR <= 0 {
			t2TriggerATR = 1.80
		}

		t2OffsetATR := d.Cfg.Tier2ProfitLockOffsetATR
		if t2OffsetATR <= 0 {
			t2OffsetATR = 0.90
		}

		beOffsetRate := decimal.NewFromFloat(math.Max(d.Cfg.MinBreakEvenOffsetPct, 2.0*d.Cfg.TakerFeeRate+d.Cfg.SlippageRate))

		if d.Cfg.MaxHoldingDuration > 0 && now.Sub(pos.OpenTime) > d.Cfg.MaxHoldingDuration {
			d.closePositionLocked(pos, triggerRefPrice, d.Cfg.TakerFeeRate, "TIME_STOP_MAX_HOLDING")
			delete(d.PendingOrders, symbol)
			return
		}

		chartPriceDec := decimal.Zero
		if tradePrice > 0 {
			chartPriceDec = decimal.NewFromFloat(tradePrice)
		} else if pos.Side == model.SideLong && bestBid > 0 {
			chartPriceDec = decimal.NewFromFloat(bestBid)
		} else if pos.Side == model.SideShort && bestAsk > 0 {
			chartPriceDec = decimal.NewFromFloat(bestAsk)
		}

		if pos.Side == model.SideLong {
			isTPHit := triggerRefPrice.GreaterThanOrEqual(pos.TPPrice) ||
				(!chartPriceDec.IsZero() && chartPriceDec.GreaterThanOrEqual(pos.TPPrice))
			if isTPHit && (!d.Cfg.TrailingEnabled || !pos.IsTP1Hit) {
				d.closePositionLocked(pos, pos.TPPrice, d.Cfg.TakerFeeRate, "TAKE_PROFIT")
				delete(d.PendingOrders, symbol)
				return
			}

			if triggerRefPrice.LessThanOrEqual(pos.CurrentSLPrice) {
				exitPrice := pos.CurrentSLPrice.Mul(decimal.NewFromFloat(1.0 - d.Cfg.SlippageRate))
				exitReason := "STOP_LOSS"
				if pos.ProtectionTier == 3 {
					exitReason = "TRAILING_STOP"
				} else if pos.ProtectionTier == 2 {
					exitReason = "PROFIT_LOCK"
				} else if pos.IsBreakEvenMoved {
					exitReason = "BREAK_EVEN"
				}
				d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, exitReason)
				delete(d.PendingOrders, symbol)
				return
			}

			// Partial Take Profit (Scale-Out Phase 2)
			if d.Cfg.PartialTPEnabled && !pos.IsTP1Hit && !pos.TP1Price.IsZero() {
				isTP1Hit := triggerRefPrice.GreaterThanOrEqual(pos.TP1Price) ||
					(!chartPriceDec.IsZero() && chartPriceDec.GreaterThanOrEqual(pos.TP1Price))
				if isTP1Hit {
					d.executePartialTP1Locked(pos, pos.TP1Price, d.Cfg.TakerFeeRate, beOffsetRate)
				}
			}

			trailActivateDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailActivateATR))
			trailDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailDistATR))
			t1TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t1LongTriggerATR))
			t2TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2TriggerATR))
			t2OffsetDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2OffsetATR))

			// Tier 3: Trailing Stop untuk Runner (aktif setelah TP1 tercapai dan Peak >= Entry + trailActivateDist)
			if d.Cfg.TrailingEnabled && pos.IsTP1Hit && pos.PeakPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(trailActivateDist) {
				newSL := pos.PeakPrice.Sub(trailDist)
				if newSL.GreaterThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.IsBreakEvenMoved = true
					pos.ProtectionTier = 3
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
					slog.Info("dryrun: Trailing Stop updated for runner",
						"symbol", symbol, "peakPrice", pos.PeakPrice, "newSL", newSL, "triggerPrice", triggerRefPrice)
				}
			} else if pos.ProtectionTier < 2 && triggerRefPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(t2TriggerDist) {
				// Tier 2: Runner Profit Lock (+1.80x ATR MFE -> Kunci SL ke Entry + 0.90x ATR)
				newSL := pos.EntryPrice.Add(t2OffsetDist)
				if newSL.GreaterThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.IsBreakEvenMoved = true
					pos.ProtectionTier = 2
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
					slog.Info("dryrun: Tier 2 Profit Lock activated",
						"symbol", symbol, "newSL", newSL, "triggerPrice", triggerRefPrice, "entryPrice", pos.EntryPrice)
				}
			} else if d.Cfg.Tier1BEEnabled && pos.ProtectionTier < 1 && triggerRefPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(t1TriggerDist) {
				// Tier 1: Rem Dini / Early BE Shield (+0.70x ATR MFE -> Pindahkan SL ke Entry + Fee Buffer)
				newSL := pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffsetRate))
				if newSL.GreaterThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.IsBreakEvenMoved = true
					pos.ProtectionTier = 1
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
					slog.Info("dryrun: Tier 1 Break-Even activated",
						"symbol", symbol, "newSL", newSL, "triggerPrice", triggerRefPrice, "entryPrice", pos.EntryPrice)
				}
			}
		} else {
			isTPHit := triggerRefPrice.LessThanOrEqual(pos.TPPrice) ||
				(!chartPriceDec.IsZero() && chartPriceDec.LessThanOrEqual(pos.TPPrice))
			if isTPHit && (!d.Cfg.TrailingEnabled || !pos.IsTP1Hit) {
				d.closePositionLocked(pos, pos.TPPrice, d.Cfg.TakerFeeRate, "TAKE_PROFIT")
				delete(d.PendingOrders, symbol)
				return
			}

			if triggerRefPrice.GreaterThanOrEqual(pos.CurrentSLPrice) {
				exitPrice := pos.CurrentSLPrice.Mul(decimal.NewFromFloat(1.0 + d.Cfg.SlippageRate))
				exitReason := "STOP_LOSS"
				if pos.ProtectionTier == 3 {
					exitReason = "TRAILING_STOP"
				} else if pos.ProtectionTier == 2 {
					exitReason = "PROFIT_LOCK"
				} else if pos.IsBreakEvenMoved {
					exitReason = "BREAK_EVEN"
				}
				d.closePositionLocked(pos, exitPrice, d.Cfg.TakerFeeRate, exitReason)
				delete(d.PendingOrders, symbol)
				return
			}

			// Partial Take Profit (Scale-Out Phase 2)
			if d.Cfg.PartialTPEnabled && !pos.IsTP1Hit && !pos.TP1Price.IsZero() {
				isTP1Hit := triggerRefPrice.LessThanOrEqual(pos.TP1Price) ||
					(!chartPriceDec.IsZero() && chartPriceDec.LessThanOrEqual(pos.TP1Price))
				if isTP1Hit {
					d.executePartialTP1Locked(pos, pos.TP1Price, d.Cfg.TakerFeeRate, beOffsetRate)
				}
			}

			trailActivateDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailActivateATR))
			trailDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailDistATR))
			t1TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t1ShortTriggerATR))
			t2TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2TriggerATR))
			t2OffsetDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2OffsetATR))

			// Tier 3: Trailing Stop untuk Runner (aktif setelah TP1 tercapai dan Peak <= Entry - trailActivateDist)
			if d.Cfg.TrailingEnabled && pos.IsTP1Hit && pos.EntryPrice.Sub(pos.PeakPrice).GreaterThanOrEqual(trailActivateDist) {
				newSL := pos.PeakPrice.Add(trailDist)
				if newSL.LessThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.IsBreakEvenMoved = true
					pos.ProtectionTier = 3
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
					slog.Info("dryrun: Trailing Stop updated for runner",
						"symbol", symbol, "peakPrice", pos.PeakPrice, "newSL", newSL, "triggerPrice", triggerRefPrice)
				}
			} else if pos.ProtectionTier < 2 && pos.EntryPrice.Sub(triggerRefPrice).GreaterThanOrEqual(t2TriggerDist) {
				// Tier 2: Runner Profit Lock (+1.80x ATR MFE -> Kunci SL ke Entry - 0.90x ATR)
				newSL := pos.EntryPrice.Sub(t2OffsetDist)
				if newSL.LessThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.IsBreakEvenMoved = true
					pos.ProtectionTier = 2
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
					slog.Info("dryrun: Tier 2 Profit Lock activated",
						"symbol", symbol, "newSL", newSL, "triggerPrice", triggerRefPrice, "entryPrice", pos.EntryPrice)
				}
			} else if d.Cfg.Tier1BEEnabled && pos.ProtectionTier < 1 && pos.EntryPrice.Sub(triggerRefPrice).GreaterThanOrEqual(t1TriggerDist) {
				// Tier 1: Rem Dini / Early BE Shield (+0.90x ATR MFE -> Pindahkan SL ke Entry - Fee Buffer)
				newSL := pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffsetRate))
				if newSL.LessThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.IsBreakEvenMoved = true
					pos.ProtectionTier = 1
					d.enqueuePositionEvent(PositionEvent{
						Type: PositionEventSave,
						Pos:  *pos,
					})
					slog.Info("dryrun: Tier 1 Break-Even activated",
						"symbol", symbol, "newSL", newSL, "triggerPrice", triggerRefPrice, "entryPrice", pos.EntryPrice)
				}
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
		pos = &VirtualOpenPosition{
			ClientOrderID:    ord.Intent.ClientOrderID,
			Symbol:           ord.Intent.Symbol,
			Side:             ord.Intent.Side,
			Qty:              ord.FilledQty,
			InitialQty:       ord.Intent.Qty,
			EntryPrice:       ord.Intent.LimitPrice,
			EntryFee:         entryFee,
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
		}
		d.OpenPositions[ord.Intent.Symbol] = pos
	} else {
		pos.Qty = ord.FilledQty
		pos.EntryFee = entryFee
	}
	d.enqueuePositionEvent(PositionEvent{
		Type: PositionEventSave,
		Pos:  *pos,
	})
}

func (d *DryRunBroker) executePartialTP1Locked(pos *VirtualOpenPosition, exitPrice decimal.Decimal, feeRate float64, beOffsetRate decimal.Decimal) {
	ratio := d.Cfg.PartialTP1Ratio
	if ratio <= 0.05 || ratio >= 0.95 {
		ratio = 0.50
	}
	closeQty := pos.Qty.Mul(decimal.NewFromFloat(ratio))
	minNotional := decimal.NewFromFloat(5.0)
	remQty := pos.Qty.Sub(closeQty)

	// [H-04] Validasi MinNotional (5.0 USDT) agar paritas dengan batas minimum eksekusi Binance
	splitPossible := closeQty.GreaterThan(decimal.Zero) &&
		closeQty.LessThan(pos.Qty) &&
		closeQty.Mul(exitPrice).GreaterThanOrEqual(minNotional) &&
		remQty.Mul(exitPrice).GreaterThanOrEqual(minNotional)

	if !splitPossible {
		// Ukuran posisi terlalu kecil untuk dipecah (atau MinNotional < 5 USDT).
		// Anggap TP1 milestone tercapai: tandai IsTP1Hit, geser SL ke BE, biarkan 100% posisi berjalan sebagai runner.
		pos.IsTP1Hit = true

		var newSL decimal.Decimal
		if pos.Side == model.SideLong {
			newSL = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffsetRate))
			if newSL.GreaterThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		} else {
			newSL = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffsetRate))
			if newSL.LessThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		}
		pos.IsBreakEvenMoved = true
		pos.ProtectionTier = 1

		slog.Info("dryrun: position notional too small for partial TP1 split (< 5 USDT); moving SL to BE and letting 100% position run as runner",
			"symbol", pos.Symbol, "qty", pos.Qty, "exitPrice", exitPrice, "newSL", pos.CurrentSLPrice)

		d.enqueuePositionEvent(PositionEvent{
			Type: PositionEventSave,
			Pos:  *pos,
		})
		return
	}

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
	netPnL := grossPnL.Sub(entryFeePortion).Sub(exitFee)

	// Saldo akun saat entry sudah didebet pos.EntryFee, maka penambahan saat exit adalah: grossPnL - exitFee
	d.RealizedEquityUSDT = d.RealizedEquityUSDT.Add(grossPnL.Sub(exitFee))
	pos.Qty = pos.Qty.Sub(closeQty)
	pos.IsTP1Hit = true

	// Pindahkan SL ke Break-Even (dengan Runner Cushion jika dikonfigurasi)
	var newSL decimal.Decimal
	if d.Cfg.RunnerBECushionATR > 0 && pos.ATRAtEntry.GreaterThan(decimal.Zero) {
		cushionDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(d.Cfg.RunnerBECushionATR))
		if pos.Side == model.SideLong {
			newSL = pos.EntryPrice.Sub(cushionDist)
			if pos.InitialSLPrice.GreaterThan(decimal.Zero) && newSL.LessThan(pos.InitialSLPrice) {
				newSL = pos.InitialSLPrice
			}
			if newSL.GreaterThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		} else {
			newSL = pos.EntryPrice.Add(cushionDist)
			if pos.InitialSLPrice.GreaterThan(decimal.Zero) && newSL.GreaterThan(pos.InitialSLPrice) {
				newSL = pos.InitialSLPrice
			}
			if newSL.LessThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		}
	} else {
		if pos.Side == model.SideLong {
			newSL = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffsetRate))
			if newSL.GreaterThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		} else {
			newSL = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffsetRate))
			if newSL.LessThan(pos.CurrentSLPrice) {
				pos.CurrentSLPrice = newSL
			}
		}
	}
	pos.IsBreakEvenMoved = true
	pos.ProtectionTier = 1

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
		go d.OnTradeClosedFn(tradeRec)
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

func (d *DryRunBroker) closePositionLocked(pos *VirtualOpenPosition, exitPrice decimal.Decimal, feeRate float64, reason string) {
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
	// Round-Trip True Net PnL = Gross PnL - Entry Fee - Exit Fee
	netPnL := grossPnL.Sub(entryFeePortion).Sub(exitFee)
	// Saldo akun saat entry sudah didebet pos.EntryFee, maka penambahan saat exit adalah: grossPnL - exitFee
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

	d.enqueuePositionEvent(PositionEvent{
		Type:                  PositionEventDelete,
		ClientOrderIDOrSymbol: clientOrderID,
	})

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

// GetRealizedEquityUSDT mengembalikan realized equity saat ini di memory
func (d *DryRunBroker) GetRealizedEquityUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return d.RealizedEquityUSDT
}

// GetPureRealizedEquityClosedTradesUSDT mengembalikan realized equity murni dari seluruh trade yang telah ditutup
// (RealizedEquityUSDT ditambah kembali seluruh entry fee dari posisi yang masih terbuka) agar tidak terjadi
// double-deduction fee atau distorsi floating PnL saat state dipulihkan setelah restart.
func (d *DryRunBroker) GetPureRealizedEquityClosedTradesUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	pure := d.RealizedEquityUSDT
	for _, pos := range d.OpenPositions {
		pure = pure.Add(pos.EntryFee)
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


