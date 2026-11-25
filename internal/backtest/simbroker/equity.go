package simbroker

import (
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
	"trml/internal/model"
)

// EquityPoint captures mark-to-market account equity and margin exposure at a simulation timestamp.
type EquityPoint struct {
	Time            time.Time `json:"time"`
	RealizedEquity  float64   `json:"realized_equity"`
	EffectiveEquity float64   `json:"effective_equity"`
	UsedMargin      float64   `json:"used_margin"`
	OpenPositions   int       `json:"open_positions"`
}

func (s *SimBroker) usedMarginLocked() decimal.Decimal {
	usedMargin := decimal.Zero
	lev := decimal.NewFromFloat(s.Cfg.AccountLeverage)
	if lev.LessThanOrEqual(decimal.Zero) {
		lev = decimal.NewFromFloat(8.0)
	}
	for _, pos := range s.OpenPositions {
		usedMargin = usedMargin.Add(pos.EntryPrice.Mul(pos.Qty).Div(lev))
	}
	for _, ord := range s.PendingOrders {
		usedMargin = usedMargin.Add(ord.Intent.LimitPrice.Mul(ord.Intent.Qty).Div(lev))
	}
	return usedMargin
}

// RecordTimelineEquity samples mark-to-market equity and margin utilization after processing all bars at simTime.
func (s *SimBroker) RecordTimelineEquity(simTime time.Time) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	effDec := s.calculateRealEffectiveEquityLocked()
	s.PeakEffectiveEquityUSDT, _ = protection.UpdatePeakAndDrawdownPct(s.PeakEffectiveEquityUSDT, effDec)
	realF, _ := s.RealizedEquityUSDT.Float64()
	effF, _ := effDec.Float64()
	usedF, _ := s.usedMarginLocked().Float64()

	s.EquityCurve = append(s.EquityCurve, EquityPoint{
		Time:            simTime,
		RealizedEquity:  realF,
		EffectiveEquity: effF,
		UsedMargin:      usedF,
		OpenPositions:   len(s.OpenPositions),
	})
}

// GetDrawdownFromPeakPct returns the current non-negative drawdown fraction from running HWM equity.
func (s *SimBroker) GetDrawdownFromPeakPct() float64 {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	eff := s.calculateRealEffectiveEquityLocked()
	var ddF float64
	s.PeakEffectiveEquityUSDT, ddF = protection.UpdatePeakAndDrawdownPct(s.PeakEffectiveEquityUSDT, eff)
	return ddF
}

// ForceClosePosition settles an open position forcefully (e.g. contract delisted / data stream ended mid-trade).
func (s *SimBroker) ForceClosePosition(symbol string, fallbackPrice decimal.Decimal, reason string, closedAt time.Time, barIndex int64) bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	pos, hasPos := s.OpenPositions[symbol]
	if !hasPos || pos.Qty.LessThanOrEqual(decimal.Zero) {
		delete(s.PendingOrders, symbol)
		return false
	}

	exitPrice := pos.LastClosePrice
	if exitPrice.LessThanOrEqual(decimal.Zero) {
		exitPrice = fallbackPrice
	}
	if exitPrice.LessThanOrEqual(decimal.Zero) {
		exitPrice = pos.EntryPrice
	}
	if s.Cfg.SlippageRate.GreaterThan(decimal.Zero) {
		if pos.Side == model.SideLong {
			exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Sub(s.Cfg.SlippageRate))
		} else {
			exitPrice = exitPrice.Mul(decimal.NewFromFloat(1.0).Add(s.Cfg.SlippageRate))
		}
	}

	holdingBars := int(barIndex - pos.OpenBarIndex)
	if holdingBars < 0 {
		holdingBars = 0
	}
	s.recordClosedTradeLocked(pos, pos.Qty, exitPrice, reason, closedAt, holdingBars)
	delete(s.OpenPositions, symbol)
	delete(s.PendingOrders, symbol)
	s.Stats.DelistSettled++
	return true
}

func positionLiquidationMark(pos *SimPosition, barIdx int64, useWorstWick bool) decimal.Decimal {
	if useWorstWick && pos.LastWorstBarIdx == barIdx && pos.LastWorstPrice.GreaterThan(decimal.Zero) {
		return pos.LastWorstPrice
	}
	if pos.LastClosePrice.GreaterThan(decimal.Zero) {
		return pos.LastClosePrice
	}
	return pos.EntryPrice
}

// CheckCrossMarginLiquidation checks whether account-wide mark-to-market equity has fallen
// to or below maintenance margin across all open positions and liquidates all open exposure.
func (s *SimBroker) CheckCrossMarginLiquidation(mmr float64, simTime time.Time, barIdx int64) bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if len(s.OpenPositions) == 0 {
		return false
	}
	if mmr <= 0 {
		mmr = 0.005
	}

	useWorstWick := s.Cfg.IntrabarConflictSLFirst
	unrealized := decimal.Zero
	totalNotional := decimal.Zero
	symbols := make([]string, 0, len(s.OpenPositions))
	for sym, pos := range s.OpenPositions {
		symbols = append(symbols, sym)
		mark := positionLiquidationMark(pos, barIdx, useWorstWick)
		if mark.GreaterThan(decimal.Zero) {
			if pos.Side == model.SideLong {
				unrealized = unrealized.Add(mark.Sub(pos.EntryPrice).Mul(pos.Qty))
			} else {
				unrealized = unrealized.Add(pos.EntryPrice.Sub(mark).Mul(pos.Qty))
			}
			totalNotional = totalNotional.Add(mark.Mul(pos.Qty))
		}
	}
	effEq := s.RealizedEquityUSDT.Add(unrealized)
	maintMargin := totalNotional.Mul(decimal.NewFromFloat(mmr))

	if effEq.GreaterThan(maintMargin) && effEq.GreaterThan(decimal.Zero) {
		return false
	}

	sort.Strings(symbols)
	mmrDec := decimal.NewFromFloat(mmr)
	slipDec := s.Cfg.SlippageRate
	if slipDec.LessThan(decimal.Zero) {
		slipDec = decimal.Zero
	}
	penalty := slipDec.Add(mmrDec)

	for _, sym := range symbols {
		pos := s.OpenPositions[sym]
		mark := positionLiquidationMark(pos, barIdx, useWorstWick)
		var exitPrice decimal.Decimal
		if pos.Side == model.SideLong {
			exitPrice = mark.Mul(decimal.NewFromFloat(1.0).Sub(penalty))
		} else {
			exitPrice = mark.Mul(decimal.NewFromFloat(1.0).Add(penalty))
		}
		if exitPrice.LessThan(decimal.Zero) {
			exitPrice = decimal.Zero
		}
		holdingBars := int(barIdx - pos.OpenBarIndex)
		if holdingBars < 0 {
			holdingBars = 0
		}
		s.recordClosedTradeLocked(pos, pos.Qty, exitPrice, "LIQUIDATION", simTime, holdingBars)
		delete(s.OpenPositions, sym)
		s.Stats.LiquidatedCount++
	}

	s.PendingOrders = make(map[string]*SimPendingOrder)
	s.CircuitBreakerTripped = true
	if s.RealizedEquityUSDT.LessThan(decimal.Zero) {
		s.RealizedEquityUSDT = decimal.Zero
	}
	return true
}
