package simbroker

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/model"
)

type SimBroker struct {
	Mu                    sync.Mutex
	Cfg                   SimBrokerConfig
	SessionDayEpochUTC    int64
	SessionStartEquity    decimal.Decimal
	RealizedEquityUSDT    decimal.Decimal
	CircuitBreakerTripped bool
	CurrentSimTime        time.Time

	PendingOrders   map[string]*SimPendingOrder // symbol -> pending
	OpenPositions   map[string]*SimPosition     // symbol -> open
	ClosedTrades    []SimTradeRecord
	ExchangeFilters map[string]model.ExchangeFilter
}

func NewSimBroker(cfg SimBrokerConfig) *SimBroker {
	if cfg.InitialEquityUSDT.LessThanOrEqual(decimal.Zero) {
		cfg.InitialEquityUSDT = decimal.NewFromFloat(1000.0)
	}
	if cfg.MaxSlots <= 0 {
		cfg.MaxSlots = 3
	}
	if cfg.MaxDailyDrawdownPct >= 0 {
		cfg.MaxDailyDrawdownPct = -0.05 // default -5%
	}
	if cfg.MakerFeeRate.IsZero() {
		cfg.MakerFeeRate = decimal.NewFromFloat(0.0002) // 2.0 bps
	}
	if cfg.TakerFeeRate.IsZero() {
		cfg.TakerFeeRate = decimal.NewFromFloat(0.0005) // 5.0 bps
	}
	if cfg.SlippageRate.IsZero() {
		cfg.SlippageRate = decimal.NewFromFloat(0.0005) // 5.0 bps
	}
	if cfg.SignalDelayBars <= 0 {
		cfg.SignalDelayBars = 1
	}
	if cfg.LimitTimeoutBars <= 0 {
		cfg.LimitTimeoutBars = 1
	}
	if cfg.PenetrationTicks <= 0 {
		cfg.PenetrationTicks = 1
	}
	if cfg.MaxHoldingDuration <= 0 {
		cfg.MaxHoldingDuration = 4 * time.Hour
	}
	if cfg.PartialTP1Ratio <= 0 {
		cfg.PartialTP1Ratio = 0.50
	}

	return &SimBroker{
		Cfg:                cfg,
		SessionStartEquity: cfg.InitialEquityUSDT,
		RealizedEquityUSDT: cfg.InitialEquityUSDT,
		PendingOrders:      make(map[string]*SimPendingOrder),
		OpenPositions:      make(map[string]*SimPosition),
		ClosedTrades:       make([]SimTradeRecord, 0),
		ExchangeFilters:    make(map[string]model.ExchangeFilter),
	}
}

// SetExchangeFilter menyetel spesifikasi filter simbol bursa (tick size, min notional, dsb).
func (s *SimBroker) SetExchangeFilter(symbol string, filter model.ExchangeFilter) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.ExchangeFilters[symbol] = filter
}

func (s *SimBroker) GetExchangeFilter(symbol string) (model.ExchangeFilter, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	f, ok := s.ExchangeFilters[symbol]
	if !ok {
		// Default fail-safe filter jika belum terdaftar
		return model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		}, true
	}
	return f, true
}

// ApplyFundingSettlement memproses pemotongan/penerimaan biaya pendanaan (funding rate)
// pada seluruh posisi terbuka untuk simbol tertentu sesuai formula resmi Binance Futures.
func (s *SimBroker) ApplyFundingSettlement(symbol string, markPrice decimal.Decimal, fundingRate decimal.Decimal) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	pos, hasPos := s.OpenPositions[symbol]
	if !hasPos || pos.Qty.LessThanOrEqual(decimal.Zero) {
		return
	}

	// Nilai nosional posisi = Qty * MarkPrice
	notional := pos.Qty.Mul(markPrice)
	fundingFee := notional.Mul(fundingRate)

	if pos.Side == model.SideLong {
		// Posisi LONG: bayar fee jika funding positif, terima fee jika funding negatif
		pos.FundingPaid = pos.FundingPaid.Add(fundingFee)
		s.RealizedEquityUSDT = s.RealizedEquityUSDT.Sub(fundingFee)
	} else {
		// Posisi SHORT: terima fee jika funding positif, bayar fee jika funding negatif
		pos.FundingPaid = pos.FundingPaid.Sub(fundingFee)
		s.RealizedEquityUSDT = s.RealizedEquityUSDT.Add(fundingFee)
	}
}

// SubmitLimitEntry menerima order intent dari Evaluator (Fase 1 Commit).
func (s *SimBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.CircuitBreakerTripped || s.isDailyDrawdownBreachedLocked() {
		return false, fmt.Errorf("daily circuit breaker active")
	}
	if _, exists := s.OpenPositions[intent.Symbol]; exists {
		return false, fmt.Errorf("position already exists for symbol %s", intent.Symbol)
	}
	if _, exists := s.PendingOrders[intent.Symbol]; exists {
		return false, fmt.Errorf("pending order already exists for symbol %s", intent.Symbol)
	}

	activeCount := len(s.OpenPositions)
	for sym := range s.PendingOrders {
		if _, exists := s.OpenPositions[sym]; !exists {
			activeCount++
		}
	}
	if activeCount >= s.Cfg.MaxSlots {
		return false, fmt.Errorf("max slots (%d) occupied", s.Cfg.MaxSlots)
	}

	timeout := intent.CreatedAt.Add(time.Duration(s.Cfg.LimitTimeoutBars*5) * time.Minute)
	s.PendingOrders[intent.Symbol] = &SimPendingOrder{
		Intent:      intent,
		PlacedTime:  intent.CreatedAt,
		TimeoutTime: timeout,
	}

	return true, nil
}

// ProcessCandle memproses 1 candlestick historis 5m untuk evaluasi fill limit order dan exit ladder.
func (s *SimBroker) ProcessCandle(c data.HistoricalCandle, barIndex int64) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.CurrentSimTime = c.CloseTime

	// 1. Rollover harian sirkuit breaker (00:00 UTC)
	epochDay := c.OpenTime.UTC().Unix() / 86400
	if s.SessionDayEpochUTC != epochDay || s.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		s.SessionDayEpochUTC = epochDay
		s.SessionStartEquity = s.calculateRealEffectiveEquityLocked()
		s.CircuitBreakerTripped = false
	}

	// Periksa apakah sirkuit breaker harian breached
	if s.isDailyDrawdownBreachedLocked() {
		// Batalkan seluruh pending entry yang belum terisi
		s.PendingOrders = make(map[string]*SimPendingOrder)
	}

	filter, hasFilter := s.ExchangeFilters[c.Symbol]
	if !hasFilter {
		filter = model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		}
	}

	// 2. Evaluasi Pending Limit Order
	if pending, exists := s.PendingOrders[c.Symbol]; exists {
		// Syarat signal delay bar
		if c.OpenTime.After(pending.PlacedTime) || c.OpenTime.Equal(pending.PlacedTime) {
			penTick := filter.TickSize.Mul(decimal.NewFromInt(int64(s.Cfg.PenetrationTicks)))
			if penTick.IsZero() {
				penTick = decimal.NewFromFloat(0.01)
			}

			filled := false
			limitP := pending.Intent.LimitPrice
			lowDec := decimal.NewFromFloat(c.Low)
			highDec := decimal.NewFromFloat(c.High)

			if pending.Intent.Side == model.SideLong {
				// LIMIT Buy terisi hanya jika Low menembus sedikitnya 1 tick di bawah limit price
				if lowDec.LessThanOrEqual(limitP.Sub(penTick)) {
					filled = true
				}
			} else {
				// LIMIT Sell terisi hanya jika High menembus sedikitnya 1 tick di atas limit price
				if highDec.GreaterThanOrEqual(limitP.Add(penTick)) {
					filled = true
				}
			}

			if filled {
				fillPrice := limitP
				makerFee := fillPrice.Mul(pending.Intent.Qty).Mul(s.Cfg.MakerFeeRate)
				s.RealizedEquityUSDT = s.RealizedEquityUSDT.Sub(makerFee)

				pos := &SimPosition{
					Symbol:         c.Symbol,
					Side:           pending.Intent.Side,
					Qty:            pending.Intent.Qty,
					InitialQty:     pending.Intent.Qty,
					EntryPrice:     fillPrice,
					EntryFee:       makerFee,
					CurrentSLPrice: pending.Intent.SLPrice,
					InitialSLPrice: pending.Intent.SLPrice,
					TPPrice:        pending.Intent.TPPrice,
					TP1Price:       pending.Intent.TP1Price,
					IsTP1Hit:       false,
					ATRAtEntry:     pending.Intent.ATRAtEntry,
					PeakPrice:      fillPrice,
					OpenTime:       c.OpenTime,
					OpenBarIndex:   barIndex,
					SignalAuditID:  pending.Intent.SignalAuditID,
					LastClosePrice: decimal.NewFromFloat(c.Close),
				}
				s.OpenPositions[c.Symbol] = pos
				delete(s.PendingOrders, c.Symbol)
			} else if !c.CloseTime.Before(pending.TimeoutTime) {
				// Expired timeout
				delete(s.PendingOrders, c.Symbol)
			}
		}
	}

	// 3. Evaluasi Exit Ladder pada Posisi Terbuka
	pos, hasPos := s.OpenPositions[c.Symbol]
	if !hasPos || pos.Qty.LessThanOrEqual(decimal.Zero) {
		return
	}
	pos.LastClosePrice = decimal.NewFromFloat(c.Close)

	// Update Peak Price dan MAE/MFE
	ePriceF, _ := pos.EntryPrice.Float64()
	if pos.Side == model.SideLong {
		if c.High > ePriceF {
			favPct := ((c.High - ePriceF) / ePriceF) * 100.0
			if favPct > pos.MaxMFE {
				pos.MaxMFE = favPct
			}
		}
		if c.Low < ePriceF {
			advPct := ((ePriceF - c.Low) / ePriceF) * 100.0
			if advPct > pos.MaxMAE {
				pos.MaxMAE = advPct
			}
		}
		if decimal.NewFromFloat(c.High).GreaterThan(pos.PeakPrice) {
			pos.PeakPrice = decimal.NewFromFloat(c.High)
		}
	} else {
		if c.Low < ePriceF {
			favPct := ((ePriceF - c.Low) / ePriceF) * 100.0
			if favPct > pos.MaxMFE {
				pos.MaxMFE = favPct
			}
		}
		if c.High > ePriceF {
			advPct := ((c.High - ePriceF) / ePriceF) * 100.0
			if advPct > pos.MaxMAE {
				pos.MaxMAE = advPct
			}
		}
		if pos.PeakPrice.IsZero() || decimal.NewFromFloat(c.Low).LessThan(pos.PeakPrice) {
			pos.PeakPrice = decimal.NewFromFloat(c.Low)
		}
	}

	lowDec := decimal.NewFromFloat(c.Low)
	highDec := decimal.NewFromFloat(c.High)
	slTouched := false
	tp1Touched := false
	tpFullTouched := false

	if pos.Side == model.SideLong {
		if lowDec.LessThanOrEqual(pos.CurrentSLPrice) {
			slTouched = true
		}
		if !pos.IsTP1Hit && highDec.GreaterThanOrEqual(pos.TP1Price) {
			tp1Touched = true
		}
		if highDec.GreaterThanOrEqual(pos.TPPrice) {
			tpFullTouched = true
		}
	} else {
		if highDec.GreaterThanOrEqual(pos.CurrentSLPrice) {
			slTouched = true
		}
		if !pos.IsTP1Hit && lowDec.LessThanOrEqual(pos.TP1Price) {
			tp1Touched = true
		}
		if lowDec.LessThanOrEqual(pos.TPPrice) {
			tpFullTouched = true
		}
	}

	// [PESSIMISTIC INTRABAR CONFLICT]
	// Jika SL dan TP sama-sama tersentuh di candle yang sama: SL dieksekusi lebih dulu!
	if slTouched && (tp1Touched || tpFullTouched) && s.Cfg.IntrabarConflictSLFirst {
		tp1Touched = false
		tpFullTouched = false
	}

	// A. Eksekusi Stop Loss
	if slTouched {
		exitPrice := pos.CurrentSLPrice
		// Terapkan slippage pada SL market exit
		if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
			} else {
				exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
			}
		}

		exitReason := "STOP_LOSS"
		switch pos.ProtectionTier {
		case 3:
			exitReason = "TRAILING_STOP"
		case 1:
			exitReason = "BREAK_EVEN"
		}

		s.recordClosedTradeLocked(pos, pos.Qty, exitPrice, exitReason, c.CloseTime, int(barIndex-pos.OpenBarIndex))
		delete(s.OpenPositions, c.Symbol)
		return
	}

	// B. Eksekusi Partial TP1 (Scale-Out 50% + Pindahkan SL ke BE)
	if tp1Touched && s.Cfg.PartialTPEnabled && !pos.IsTP1Hit {
		closeRatio := decimal.NewFromFloat(s.Cfg.PartialTP1Ratio)
		closeQty := pos.InitialQty.Mul(closeRatio)
		if filter.StepSize.GreaterThan(decimal.Zero) {
			if qQty, err := model.QuantizeQtyFloor(closeQty, filter.StepSize); err == nil {
				closeQty = qQty
			}
		}

		if closeQty.GreaterThan(decimal.Zero) && closeQty.LessThan(pos.Qty) {
			s.recordClosedTradeLocked(pos, closeQty, pos.TP1Price, "PARTIAL_TAKE_PROFIT", c.CloseTime, int(barIndex-pos.OpenBarIndex))
			pos.Qty = pos.Qty.Sub(closeQty)
			pos.IsTP1Hit = true

			// Geser SL ke Break-Even (+ Runner Cushion jika ada)
			beOffset := decimal.NewFromFloat(math.Max(s.Cfg.MinBreakEvenOffsetPct, 0.0010))
			var beSL decimal.Decimal
			if s.Cfg.RunnerBECushionATR > 0 && pos.ATRAtEntry.GreaterThan(decimal.Zero) {
				cushion := pos.ATRAtEntry.Mul(decimal.NewFromFloat(s.Cfg.RunnerBECushionATR))
				if pos.Side == model.SideLong {
					beSL = pos.EntryPrice.Sub(cushion)
					if pos.InitialSLPrice.GreaterThan(decimal.Zero) && beSL.LessThan(pos.InitialSLPrice) {
						beSL = pos.InitialSLPrice
					}
				} else {
					beSL = pos.EntryPrice.Add(cushion)
					if pos.InitialSLPrice.GreaterThan(decimal.Zero) && beSL.GreaterThan(pos.InitialSLPrice) {
						beSL = pos.InitialSLPrice
					}
				}
			} else {
				if pos.Side == model.SideLong {
					beSL = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffset))
				} else {
					beSL = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffset))
				}
			}

			// Kuantisasi ke tick size
			if filter.TickSize.GreaterThan(decimal.Zero) {
				if qSL, err := model.QuantizePrice(beSL, filter.TickSize, pos.Side == model.SideShort); err == nil {
					beSL = qSL
				}
			}

			pos.CurrentSLPrice = beSL
			pos.ProtectionTier = 1
		}
	}

	// C. Eksekusi Full Take Profit
	if tpFullTouched {
		s.recordClosedTradeLocked(pos, pos.Qty, pos.TPPrice, "TAKE_PROFIT", c.CloseTime, int(barIndex-pos.OpenBarIndex))
		delete(s.OpenPositions, c.Symbol)
		return
	}

	// D. Trailing Stop Ratchet (Hanya aktif setelah TP1 tercapai)
	if pos.IsTP1Hit && s.Cfg.TrailingEnabled && pos.ATRAtEntry.GreaterThan(decimal.Zero) {
		actDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(s.Cfg.TrailingActivateATR))
		trailDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(s.Cfg.TrailingDistanceATR))

		if pos.Side == model.SideLong {
			if pos.PeakPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(actDist) {
				newSL := pos.PeakPrice.Sub(trailDist)
				if filter.TickSize.GreaterThan(decimal.Zero) {
					if qSL, err := model.QuantizePrice(newSL, filter.TickSize, false); err == nil {
						newSL = qSL
					}
				}
				// Ratchet Guard: SL tidak boleh mundur
				if newSL.GreaterThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.ProtectionTier = 3
				}
			}
		} else {
			if pos.EntryPrice.Sub(pos.PeakPrice).GreaterThanOrEqual(actDist) {
				newSL := pos.PeakPrice.Add(trailDist)
				if filter.TickSize.GreaterThan(decimal.Zero) {
					if qSL, err := model.QuantizePrice(newSL, filter.TickSize, true); err == nil {
						newSL = qSL
					}
				}
				// Ratchet Guard: SL tidak boleh mundur
				if newSL.LessThan(pos.CurrentSLPrice) {
					pos.CurrentSLPrice = newSL
					pos.ProtectionTier = 3
				}
			}
		}
	}

	// E. Time-Stop Holding Timeout (4 Jam = 48 bar 5m)
	if s.Cfg.MaxHoldingDuration > 0 && c.CloseTime.Sub(pos.OpenTime) > s.Cfg.MaxHoldingDuration {
		exitPrice := decimal.NewFromFloat(c.Close)
		if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
			} else {
				exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
			}
		}
		s.recordClosedTradeLocked(pos, pos.Qty, exitPrice, "TIME_STOP_MAX_HOLDING", c.CloseTime, int(barIndex-pos.OpenBarIndex))
		delete(s.OpenPositions, c.Symbol)
	}
}

func (s *SimBroker) recordClosedTradeLocked(
	pos *SimPosition,
	qty, exitPrice decimal.Decimal,
	reason string,
	closedAt time.Time,
	holdingBars int,
) {
	var grossPnL decimal.Decimal
	if pos.Side == model.SideLong {
		grossPnL = exitPrice.Sub(pos.EntryPrice).Mul(qty)
	} else {
		grossPnL = pos.EntryPrice.Sub(exitPrice).Mul(qty)
	}

	exitFee := exitPrice.Mul(qty).Mul(s.Cfg.TakerFeeRate)
	entryFeeLeg := pos.EntryPrice.Mul(qty).Mul(s.Cfg.MakerFeeRate)
	totalFee := entryFeeLeg.Add(exitFee)

	// Proporsi funding yang diatribusikan ke trade leg ini
	var fundingLeg decimal.Decimal
	if pos.InitialQty.GreaterThan(decimal.Zero) {
		ratio := qty.Div(pos.InitialQty)
		fundingLeg = pos.FundingPaid.Mul(ratio)
	}

	// Net PnL = Gross PnL - Trading Fees - Funding Paid (+ Funding Received jika negatif)
	netPnL := grossPnL.Sub(totalFee).Sub(fundingLeg)

	// RealizedEquityUSDT sudah dipotong entryFee dan funding secara real-time saat kejadian.
	// Saat exit, kita membukukan grossPnL dikurangi exitFee ke RealizedEquityUSDT.
	s.RealizedEquityUSDT = s.RealizedEquityUSDT.Add(grossPnL.Sub(exitFee))

	tradeID := fmt.Sprintf("SIM_%s_%d", pos.Symbol, closedAt.UnixNano())
	rec := SimTradeRecord{
		ID:            tradeID,
		Symbol:        pos.Symbol,
		Side:          pos.Side,
		EntryTime:     pos.OpenTime,
		ExitTime:      closedAt,
		EntryPrice:    pos.EntryPrice,
		ExitPrice:     exitPrice,
		Qty:           qty,
		InitialQty:    pos.InitialQty,
		SLPrice:       pos.InitialSLPrice,
		TPPrice:       pos.TPPrice,
		TP1Price:      pos.TP1Price,
		GrossPnL:      grossPnL,
		NetPnL:        netPnL,
		FeeTotal:      totalFee,
		FundingTotal:  fundingLeg,
		ExitReason:    reason,
		MAE:           pos.MaxMAE,
		MFE:           pos.MaxMFE,
		HoldingBars:   holdingBars,
		SignalAuditID: pos.SignalAuditID,
		ATRAtEntry:    pos.ATRAtEntry,
	}

	s.ClosedTrades = append(s.ClosedTrades, rec)
}

func (s *SimBroker) calculateRealEffectiveEquityLocked() decimal.Decimal {
	unrealized := decimal.Zero
	for _, pos := range s.OpenPositions {
		mark := pos.LastClosePrice
		if mark.IsZero() {
			mark = pos.EntryPrice
		}
		if mark.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				unrealized = unrealized.Add(mark.Sub(pos.EntryPrice).Mul(pos.Qty))
			} else {
				unrealized = unrealized.Add(pos.EntryPrice.Sub(mark).Mul(pos.Qty))
			}
		}
	}
	return s.RealizedEquityUSDT.Add(unrealized)
}

func (s *SimBroker) calculateEffectiveEquityLocked() decimal.Decimal {
	// Mode fixed: equity sizing tidak compounding
	if !s.Cfg.EquityModeCompounding {
		return s.Cfg.InitialEquityUSDT
	}
	return s.calculateRealEffectiveEquityLocked()
}

func (s *SimBroker) isDailyDrawdownBreachedLocked() bool {
	if s.CircuitBreakerTripped {
		return true
	}
	if s.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		return false
	}
	effEquity := s.calculateRealEffectiveEquityLocked()
	ddDec := effEquity.Sub(s.SessionStartEquity).Div(s.SessionStartEquity)
	ddFloat, _ := ddDec.Float64()
	if ddFloat <= s.Cfg.MaxDailyDrawdownPct {
		s.CircuitBreakerTripped = true
		return true
	}
	return false
}

// ExecutionBroker Interface Implementation:
func (s *SimBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.calculateEffectiveEquityLocked()
}

func (s *SimBroker) GetAvailableBalanceUSDT() decimal.Decimal {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	usedMargin := decimal.Zero
	lev := decimal.NewFromFloat(s.Cfg.AccountLeverage)
	if lev.LessThanOrEqual(decimal.Zero) {
		lev = decimal.NewFromFloat(8.0)
	}

	for _, pos := range s.OpenPositions {
		notional := pos.EntryPrice.Mul(pos.Qty)
		usedMargin = usedMargin.Add(notional.Div(lev))
	}
	for _, ord := range s.PendingOrders {
		notional := ord.Intent.LimitPrice.Mul(ord.Intent.Qty)
		usedMargin = usedMargin.Add(notional.Div(lev))
	}

	eff := s.calculateRealEffectiveEquityLocked()
	avail := eff.Sub(usedMargin)
	if avail.LessThan(decimal.Zero) {
		return decimal.Zero
	}
	return avail
}

func (s *SimBroker) GetDailyTotalDrawdownPct() float64 {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		return 0.0
	}
	eff := s.calculateRealEffectiveEquityLocked()
	ddDec := eff.Sub(s.SessionStartEquity).Div(s.SessionStartEquity)
	ddFloat, _ := ddDec.Float64()
	return ddFloat
}

func (s *SimBroker) IsCircuitBreakerTripped() bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.isDailyDrawdownBreachedLocked()
}

func (s *SimBroker) GetActiveSlotCount() int {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	symbols := make(map[string]struct{})
	for sym := range s.OpenPositions {
		symbols[sym] = struct{}{}
	}
	for sym := range s.PendingOrders {
		symbols[sym] = struct{}{}
	}
	return len(symbols)
}

func (s *SimBroker) HasActiveOrderOrPosition(symbol string) bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if _, exists := s.OpenPositions[symbol]; exists {
		return true
	}
	if _, exists := s.PendingOrders[symbol]; exists {
		return true
	}
	return false
}

func (s *SimBroker) CheckTimeouts(now time.Time) {}

func (s *SimBroker) OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {
}

func (s *SimBroker) Close() {}
