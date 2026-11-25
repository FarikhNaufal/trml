package simbroker

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/broker/protection"
	"trml/internal/model"
)

func isFinitePos(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0
}

func normalizeCloseTime(t time.Time) time.Time {
	if !t.IsZero() && t.UnixMilli()%1000 == 999 {
		return t.Add(time.Millisecond)
	}
	return t
}

func isMoreProtectiveSL(side model.Side, currentSL, candidateSL decimal.Decimal) bool {
	if candidateSL.LessThanOrEqual(decimal.Zero) {
		return false
	}
	if currentSL.LessThanOrEqual(decimal.Zero) {
		return true
	}
	if side == model.SideLong {
		return candidateSL.GreaterThan(currentSL)
	}
	return candidateSL.LessThan(currentSL)
}

type SimBroker struct {
	Mu                    sync.Mutex
	Cfg                   SimBrokerConfig
	SessionDayEpochUTC      int64
	SessionStartEquity      decimal.Decimal
	RealizedEquityUSDT      decimal.Decimal
	PeakEffectiveEquityUSDT decimal.Decimal
	CircuitBreakerTripped   bool
	CurrentSimTime        time.Time

	PendingOrders   map[string]*SimPendingOrder // symbol -> pending
	OpenPositions   map[string]*SimPosition     // symbol -> open
	ClosedTrades    []SimTradeRecord
	EquityCurve     []EquityPoint
	Stats           SimExecutionStats
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
	if !cfg.MakerFeeSet && cfg.MakerFeeRate.IsZero() {
		cfg.MakerFeeRate = decimal.NewFromFloat(0.0002) // 2.0 bps
	}
	if !cfg.TakerFeeSet && cfg.TakerFeeRate.IsZero() {
		cfg.TakerFeeRate = decimal.NewFromFloat(0.0005) // 5.0 bps
	}
	if !cfg.SlippageSet && cfg.SlippageRate.IsZero() {
		cfg.SlippageRate = decimal.NewFromFloat(0.0005) // 5.0 bps
	}
	if cfg.SignalDelayBars <= 0 {
		cfg.SignalDelayBars = 1
	}
	if cfg.LimitTimeoutBars <= 0 {
		cfg.LimitTimeoutBars = 1
	}
	if !cfg.PenetrationTicksSet && cfg.PenetrationTicks <= 0 {
		cfg.PenetrationTicks = 1
	}
	if cfg.PenetrationTicks < 0 {
		cfg.PenetrationTicks = 0
	}
	if cfg.CandleDuration <= 0 {
		cfg.CandleDuration = 5 * time.Minute
	}
	if cfg.MaxHoldingDuration <= 0 {
		cfg.MaxHoldingDuration = 4 * time.Hour
	}
	if cfg.PartialTP1Ratio <= 0 {
		cfg.PartialTP1Ratio = 0.50
	}

	return &SimBroker{
		Cfg:                     cfg,
		SessionStartEquity:      cfg.InitialEquityUSDT,
		RealizedEquityUSDT:      cfg.InitialEquityUSDT,
		PeakEffectiveEquityUSDT: cfg.InitialEquityUSDT,
		PendingOrders:           make(map[string]*SimPendingOrder),
		OpenPositions:      make(map[string]*SimPosition),
		ClosedTrades:       make([]SimTradeRecord, 0),
		EquityCurve:        make([]EquityPoint, 0, 256),
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
	s.Stats.FundingSettlements++
}

// SubmitLimitEntry menerima order intent dari Evaluator (Fase 1 Commit).
func (s *SimBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.CircuitBreakerTripped || s.isDailyDrawdownBreachedLocked() {
		return false, fmt.Errorf("daily circuit breaker active")
	}
	if s.calculateRealEffectiveEquityLocked().LessThanOrEqual(decimal.Zero) {
		return false, fmt.Errorf("account equity depleted")
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

	timeoutDur := s.Cfg.LimitOrderTimeout
	if timeoutDur <= 0 {
		barDur := s.Cfg.CandleDuration
		if barDur <= 0 {
			barDur = 5 * time.Minute
		}
		timeoutDur = time.Duration(s.Cfg.LimitTimeoutBars) * barDur
	}
	timeout := intent.CreatedAt.Add(timeoutDur)
	s.PendingOrders[intent.Symbol] = &SimPendingOrder{
		Intent:         intent,
		OrigLimitPrice: intent.LimitPrice,
		PlacedTime:     intent.CreatedAt,
		TimeoutTime:    timeout,
	}
	s.Stats.Submitted++

	return true, nil
}

// ProcessCandle memproses 1 candlestick historis 5m untuk evaluasi fill limit order dan exit ladder.
func (s *SimBroker) ProcessCandle(c data.HistoricalCandle, barIndex int64) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if !isFinitePos(c.Open) || !isFinitePos(c.High) || !isFinitePos(c.Low) || !isFinitePos(c.Close) {
		return
	}

	evalCloseTime := normalizeCloseTime(c.CloseTime)
	s.CurrentSimTime = evalCloseTime

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

	// 2. Evaluasi Pending Entry Order (LIMIT + GTX Repeg opsional / MARKET)
	limitFilledOnThisBar := false
	if pending, exists := s.PendingOrders[c.Symbol]; exists {
		if c.OpenTime.After(pending.PlacedTime) || c.OpenTime.Equal(pending.PlacedTime) {
			openDec := decimal.NewFromFloat(c.Open)
			lowDec := decimal.NewFromFloat(c.Low)
			highDec := decimal.NewFromFloat(c.High)

			orderType := pending.Intent.EntryOrderType
			if orderType == "" {
				orderType = s.Cfg.EntryOrderType
			}
			if strings.EqualFold(orderType, "MARKET") {
				fillPrice := openDec
				if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
					if pending.Intent.Side == model.SideLong {
						fillPrice = openDec.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
					} else {
						fillPrice = openDec.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
					}
				}
				if filter.TickSize.GreaterThan(decimal.Zero) {
					if q, qErr := model.QuantizePrice(fillPrice, filter.TickSize, pending.Intent.Side == model.SideLong); qErr == nil && q.GreaterThan(decimal.Zero) {
						fillPrice = q
					}
				}
				takerFee := fillPrice.Mul(pending.Intent.Qty).Mul(s.Cfg.TakerFeeRate)
				s.RealizedEquityUSDT = s.RealizedEquityUSDT.Sub(takerFee)

				pos := s.newSimPositionLocked(c, pending, fillPrice, takerFee, false, barIndex)
				s.OpenPositions[c.Symbol] = pos
				delete(s.PendingOrders, c.Symbol)
				s.Stats.Filled++
				s.Stats.MarketFilled++
			} else {
				// Validasi Post-Only GTX saat pembukaan bar pertama menggunakan SSOT protection.WouldCrossMakerBook / RepegLimitEntryIntent
				// (hanya repeg ketika order menyeberang spread; jika harga menjauh/tetap pasif, biarkan resting hingga terisi atau timeout).
				shouldEvalFill := true
				if !pending.GTXValidated && openDec.GreaterThan(decimal.Zero) {
					gappedThrough := (pending.Intent.Side == model.SideLong && openDec.LessThan(pending.Intent.LimitPrice)) ||
						(pending.Intent.Side == model.SideShort && openDec.GreaterThan(pending.Intent.LimitPrice))
					if gappedThrough {
						openBidF, openAskF := syntheticBarOpenBBO(pending.Intent.Side, openDec, filter.TickSize)
						if protection.WouldCrossMakerBook(pending.Intent.Side, pending.Intent.LimitPrice, openBidF, openAskF) {
							maxRepegs, maxDrift := resolveSimRepegSettings(s.Cfg.MaxEntryRepegAttempts, s.Cfg.MaxEntryRepegDriftPct)
							if pending.RepegAttempts < maxRepegs {
								if repegged, okRepeg, _ := protection.RepegLimitEntryIntent(
									pending.Intent, pending.OrigLimitPrice, openBidF, openAskF, filter, maxDrift,
								); okRepeg {
									pending.Intent = repegged
									pending.RepegAttempts++
									pending.GTXValidated = true
									s.Stats.Repegged++
								} else {
									delete(s.PendingOrders, c.Symbol)
									s.Stats.RepegAborted++
									shouldEvalFill = false
								}
							} else {
								delete(s.PendingOrders, c.Symbol)
								s.Stats.RepegAborted++
								shouldEvalFill = false
							}
						} else {
							pending.GTXValidated = true
						}
					} else {
						pending.GTXValidated = true
					}
				}

				if shouldEvalFill {
					penTick := filter.TickSize.Mul(decimal.NewFromInt(int64(s.Cfg.PenetrationTicks)))
					if penTick.IsZero() && s.Cfg.PenetrationTicks > 0 {
						penTick = decimal.NewFromFloat(0.01)
					}

					filled := false
					limitP := pending.Intent.LimitPrice
					if pending.Intent.Side == model.SideLong {
						if lowDec.LessThanOrEqual(limitP.Sub(penTick)) {
							filled = true
						}
					} else {
						if highDec.GreaterThanOrEqual(limitP.Add(penTick)) {
							filled = true
						}
					}

					if filled {
						fillPrice := limitP
						makerFee := fillPrice.Mul(pending.Intent.Qty).Mul(s.Cfg.MakerFeeRate)
						s.RealizedEquityUSDT = s.RealizedEquityUSDT.Sub(makerFee)

						pos := s.newSimPositionLocked(c, pending, fillPrice, makerFee, true, barIndex)
						s.OpenPositions[c.Symbol] = pos
						delete(s.PendingOrders, c.Symbol)
						s.Stats.Filled++
						limitFilledOnThisBar = true
					} else if !evalCloseTime.Before(pending.TimeoutTime) {
						delete(s.PendingOrders, c.Symbol)
						s.Stats.TimedOut++
					}
				}
			}
		}
	}

	// 3. Evaluasi Exit Ladder pada Posisi Terbuka
	pos, hasPos := s.OpenPositions[c.Symbol]
	if !hasPos || pos.Qty.LessThanOrEqual(decimal.Zero) {
		return
	}
	pos.LastClosePrice = decimal.NewFromFloat(c.Close)
	lowDec := decimal.NewFromFloat(c.Low)
	highDec := decimal.NewFromFloat(c.High)
	worstPrice := lowDec
	if pos.Side == model.SideShort {
		worstPrice = highDec
	}
	if pos.LastWorstBarIdx != barIndex || pos.LastWorstPrice.LessThanOrEqual(decimal.Zero) {
		pos.LastWorstBarIdx = barIndex
		pos.LastWorstPrice = worstPrice
	} else if (pos.Side == model.SideLong && worstPrice.LessThan(pos.LastWorstPrice)) ||
		(pos.Side == model.SideShort && worstPrice.GreaterThan(pos.LastWorstPrice)) {
		pos.LastWorstPrice = worstPrice
	}

	// Update Peak Price dan MAE/MFE
	// Pada bar saat limit entry baru saja terisi (limitFilledOnThisBar), jangan gunakan High/Low bar penuh
	// sebagai PeakPrice/MFE karena kita tidak tahu apakah wick menguntungkan terjadi sebelum atau sesudah
	// limit fill (mencegah look-ahead bias); untuk MARKET entry di Open bar seluruh bar terjadi pasca-fill.
	ePriceF, _ := pos.EntryPrice.Float64()
	if pos.Side == model.SideLong {
		favRef := c.High
		if limitFilledOnThisBar {
			favRef = c.Close
		}
		if favRef > ePriceF {
			favPct := ((favRef - ePriceF) / ePriceF) * 100.0
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
		favDec := decimal.NewFromFloat(favRef)
		if favDec.GreaterThan(pos.PeakPrice) {
			pos.PeakPrice = favDec
		}
	} else {
		favRef := c.Low
		if limitFilledOnThisBar {
			favRef = c.Close
		}
		if favRef < ePriceF {
			favPct := ((ePriceF - favRef) / ePriceF) * 100.0
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
		favDec := decimal.NewFromFloat(favRef)
		if pos.PeakPrice.IsZero() || favDec.LessThan(pos.PeakPrice) {
			pos.PeakPrice = favDec
		}
	}

	slTouched := false
	tp1Touched := false
	tpFullTouched := false

	if pos.Side == model.SideLong {
		if !pos.CurrentSLPrice.IsZero() && lowDec.LessThanOrEqual(pos.CurrentSLPrice) {
			slTouched = true
		}
		if !limitFilledOnThisBar && !pos.IsTP1Hit && !pos.TP1Price.IsZero() && highDec.GreaterThanOrEqual(pos.TP1Price) {
			tp1Touched = true
		}
		if !limitFilledOnThisBar && !pos.TPPrice.IsZero() && highDec.GreaterThanOrEqual(pos.TPPrice) {
			tpFullTouched = true
		}
	} else {
		if !pos.CurrentSLPrice.IsZero() && highDec.GreaterThanOrEqual(pos.CurrentSLPrice) {
			slTouched = true
		}
		if !limitFilledOnThisBar && !pos.IsTP1Hit && !pos.TP1Price.IsZero() && lowDec.LessThanOrEqual(pos.TP1Price) {
			tp1Touched = true
		}
		if !limitFilledOnThisBar && !pos.TPPrice.IsZero() && lowDec.LessThanOrEqual(pos.TPPrice) {
			tpFullTouched = true
		}
	}

	// [PESSIMISTIC INTRABAR CONFLICT]
	// Jika SL dan TP sama-sama tersentuh di candle yang sama: SL dieksekusi lebih dulu!
	if slTouched && (tp1Touched || tpFullTouched) && s.Cfg.IntrabarConflictSLFirst {
		s.Stats.SLFirstConflicts++
		tp1Touched = false
		tpFullTouched = false
	}

	// A. Eksekusi Stop Loss
	if slTouched {
		exitPrice := pos.CurrentSLPrice
		if s.Cfg.SLGapFillEnabled {
			openDec := decimal.NewFromFloat(c.Open)
			if pos.Side == model.SideLong {
				if openDec.LessThan(exitPrice) {
					exitPrice = openDec // Gap down: terisi di harga pembukaan bar
				}
				if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
					exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
				}
			} else {
				if openDec.GreaterThan(exitPrice) {
					exitPrice = openDec // Gap up: terisi di harga pembukaan bar
				}
				if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
					exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
				}
			}
		} else {
			// Perilaku commit 9815362: terisi tepat di nominal SL price (+ slippage)
			if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
				if pos.Side == model.SideLong {
					exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
				} else {
					exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
				}
			}
		}

		exitReason := "STOP_LOSS"
		switch pos.ProtectionTier {
		case 3:
			exitReason = "TRAILING_STOP"
		case 2:
			exitReason = "PROFIT_LOCK"
		case 1:
			exitReason = "BREAK_EVEN"
		}

		s.recordClosedTradeLocked(pos, pos.Qty, exitPrice, exitReason, c.CloseTime, int(barIndex-pos.OpenBarIndex))
		delete(s.OpenPositions, c.Symbol)
		return
	}

	// B. Eksekusi Partial TP1 (Scale-Out + Pindahkan SL ke BE)
	if tp1Touched && s.Cfg.PartialTPEnabled && !pos.IsTP1Hit {
		splitPlan := protection.PlanPartialTP1Split(pos.InitialQty, pos.Qty, decimal.Zero, pos.TP1Price, s.Cfg.PartialTP1Ratio, filter)
		if splitPlan.SplitPossible {
			s.recordClosedTradeLocked(pos, splitPlan.CloseQty, pos.TP1Price, "PARTIAL_TAKE_PROFIT", c.CloseTime, int(barIndex-pos.OpenBarIndex))
			pos.Qty = splitPlan.RemainingQty
			pos.IsTP1Hit = true

			// Geser SL ke Break-Even (+ Runner Cushion jika ada) via Unified Protection Engine tanpa downgrade tier/SL
			beSL := protection.CalculateBreakEvenSL(pos.toSnapshot(filter.TickSize), s.toProtectionConfig())
			if isMoreProtectiveSL(pos.Side, pos.CurrentSLPrice, beSL) {
				pos.CurrentSLPrice = beSL
			}
			if pos.ProtectionTier < 1 {
				pos.ProtectionTier = 1
			}
		} else {
			// Posisi terlalu kecil untuk dipecah (MinNotional / MinQty): transisikan 100% menjadi runner BE
			s.Stats.UnsplittableTP1++
			pos.IsTP1Hit = true
			beSL := protection.CalculateBreakEvenSL(pos.toSnapshot(filter.TickSize), s.toProtectionConfig())
			if isMoreProtectiveSL(pos.Side, pos.CurrentSLPrice, beSL) {
				pos.CurrentSLPrice = beSL
			}
			if pos.ProtectionTier < 1 {
				pos.ProtectionTier = 1
			}
		}
	}

	// C. Eksekusi Full Take Profit
	// Jika Trailing Stop aktif dan TP1 sudah tercapai, posisi runner dibiarkan trailing tanpa hard-cap kaku (selaras dengan Live & DryRun)
	if tpFullTouched && (!s.Cfg.TrailingEnabled || !pos.IsTP1Hit) {
		s.recordClosedTradeLocked(pos, pos.Qty, pos.TPPrice, "TAKE_PROFIT", c.CloseTime, int(barIndex-pos.OpenBarIndex))
		delete(s.OpenPositions, c.Symbol)
		return
	}

	// D. Unified Position Protection Engine (Tier 1 Early BE, Tier 2 Profit Lock, Tier 3 Trailing Ratchet)
	protDec := protection.EvaluatePositionProtection(pos.toSnapshot(filter.TickSize), decimal.NewFromFloat(c.Close), s.toProtectionConfig())
	if protDec.ShouldUpdate {
		pos.CurrentSLPrice = protDec.NewSLPrice
		if int(protDec.NewTier) > pos.ProtectionTier {
			pos.ProtectionTier = int(protDec.NewTier)
		}
	}

	// D2. Underwater Stale-Loss Time-Cut
	closeDec := decimal.NewFromFloat(c.Close)
	if protection.ShouldTriggerStaleLossExit(pos.Side, pos.EntryPrice, closeDec, pos.OpenTime, evalCloseTime, s.Cfg.StaleLossExitDuration) {
		exitPrice := closeDec
		if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
			} else {
				exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
			}
		}
		s.recordClosedTradeLocked(pos, pos.Qty, exitPrice, "TIME_STOP_STALE_LOSS", c.CloseTime, int(barIndex-pos.OpenBarIndex))
		delete(s.OpenPositions, c.Symbol)
		return
	}

	// E. Time-Stop Holding Timeout (4 Jam = 48 bar 5m)
	if s.Cfg.MaxHoldingDuration > 0 && evalCloseTime.Sub(pos.OpenTime) > s.Cfg.MaxHoldingDuration {
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

	var exitFee decimal.Decimal
	if reason == "PARTIAL_TAKE_PROFIT" {
		exitFee = exitPrice.Mul(qty).Mul(s.Cfg.MakerFeeRate)
	} else {
		exitFee = exitPrice.Mul(qty).Mul(s.Cfg.TakerFeeRate)
	}
	entryFeeRate := s.Cfg.MakerFeeRate
	if !pos.EntryWasMaker {
		entryFeeRate = s.Cfg.TakerFeeRate
	}
	entryFeeLeg := pos.EntryPrice.Mul(qty).Mul(entryFeeRate)
	totalFee := entryFeeLeg.Add(exitFee)

	// Proporsi funding yang diatribusikan ke trade leg ini menggunakan sisa lot sebelum penutupan (pos.Qty)
	// agar pos.InitialQty tetap utuh sebagai referensi ukuran entry awal.
	var fundingLeg decimal.Decimal
	if pos.Qty.GreaterThan(decimal.Zero) {
		ratio := qty.Div(pos.Qty)
		fundingLeg = pos.FundingPaid.Mul(ratio)
	}
	pos.FundingPaid = pos.FundingPaid.Sub(fundingLeg)

	// Net PnL = Gross PnL - Trading Fees - Funding Paid (+ Funding Received jika negatif)
	netPnL := grossPnL.Sub(totalFee).Sub(fundingLeg)

	// RealizedEquityUSDT sudah dipotong entryFee dan funding secara real-time saat kejadian.
	// Saat exit, kita membukukan grossPnL dikurangi exitFee ke RealizedEquityUSDT.
	s.RealizedEquityUSDT = s.RealizedEquityUSDT.Add(grossPnL.Sub(exitFee))

	entryMode := "CLEAN_LIMIT"
	if !pos.EntryWasMaker {
		entryMode = "MARKET"
	} else if pos.RepegCount > 0 {
		entryMode = "GTX_REPEG"
	}

	stratID := strings.TrimSpace(pos.StrategyID)
	if stratID == "" {
		stratID = "VWAP_PULLBACK"
	}

	tradeID := fmt.Sprintf("SIM_%s_%d", pos.Symbol, closedAt.UnixNano())
	rec := SimTradeRecord{
		ID:            tradeID,
		Symbol:        pos.Symbol,
		Side:          pos.Side,
		StrategyID:    stratID,
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
		RepegCount:    pos.RepegCount,
		EntryWasMaker: pos.EntryWasMaker,
		EntryMode:     entryMode,
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

	realEff := s.calculateRealEffectiveEquityLocked()
	if realEff.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	eff := decimal.Min(s.calculateEffectiveEquityLocked(), realEff)
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

func (s *SimBroker) toProtectionConfig() protection.ProtectionConfig {
	t1Long := s.Cfg.Tier1BETriggerLongATR
	if t1Long <= 0 {
		t1Long = 0.70
	}
	t1Short := s.Cfg.Tier1BETriggerShortATR
	if t1Short <= 0 {
		t1Short = 0.90
	}
	return protection.ProtectionConfig{
		Tier1BEEnabled:           s.Cfg.Tier1BEEnabled,
		Tier1BETriggerLongATR:    t1Long,
		Tier1BETriggerShortATR:   t1Short,
		Tier2ProfitLockEnabled:   s.Cfg.Tier2ProfitLockEnabled,
		Tier2ProfitLockATR:       s.Cfg.Tier2ProfitLockATR,
		Tier2ProfitLockOffsetATR: s.Cfg.Tier2ProfitLockOffsetATR,
		TrailingEnabled:          s.Cfg.TrailingEnabled,
		TrailingActivateATR:      s.Cfg.TrailingActivateATR,
		TrailingDistanceATR:      s.Cfg.TrailingDistanceATR,
		PartialTPEnabled:         s.Cfg.PartialTPEnabled,
		MinBreakEvenOffsetPct:    s.Cfg.MinBreakEvenOffsetPct,
		RunnerBECushionATR:       s.Cfg.RunnerBECushionATR,
		TakerFeeRate:             s.Cfg.TakerFeeRate,
		SlippageRate:             s.Cfg.SlippageRate,
	}
}

func (s *SimBroker) newSimPositionLocked(
	c data.HistoricalCandle,
	pending *SimPendingOrder,
	fillPrice, entryFee decimal.Decimal,
	wasMaker bool,
	barIndex int64,
) *SimPosition {
	stratID := strings.TrimSpace(pending.Intent.StrategyID)
	if stratID == "" {
		stratID = "VWAP_PULLBACK"
	}
	return &SimPosition{
		Symbol:         c.Symbol,
		Side:           pending.Intent.Side,
		StrategyID:     stratID,
		Qty:            pending.Intent.Qty,
		InitialQty:     pending.Intent.Qty,
		EntryPrice:     fillPrice,
		EntryFee:       entryFee,
		EntryWasMaker:  wasMaker,
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
		RepegCount:     pending.RepegAttempts,
	}
}

func resolveSimRepegSettings(maxAttempts int, maxDriftPct float64) (int, float64) {
	if maxAttempts <= 0 {
		if maxDriftPct <= 0 {
			maxDriftPct = protection.DefaultMaxEntryRepegDriftPct
		}
		return 0, maxDriftPct
	}
	return protection.ResolveEntryRepegSettings(maxAttempts, maxDriftPct)
}

func syntheticBarOpenBBO(side model.Side, openDec, tick decimal.Decimal) (float64, float64) {
	spreadStep := tick
	if spreadStep.LessThanOrEqual(decimal.Zero) {
		spreadStep = openDec.Mul(decimal.NewFromFloat(0.0001))
	}
	var bidDec, askDec decimal.Decimal
	if side == model.SideLong {
		askDec = openDec
		bidDec = openDec.Sub(spreadStep)
		if bidDec.LessThanOrEqual(decimal.Zero) {
			bidDec = openDec
		}
	} else {
		bidDec = openDec
		askDec = openDec.Add(spreadStep)
	}
	bidF, _ := bidDec.Float64()
	askF, _ := askDec.Float64()
	return bidF, askF
}

func (pos *SimPosition) toSnapshot(tickSize decimal.Decimal) protection.PositionSnapshot {
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
