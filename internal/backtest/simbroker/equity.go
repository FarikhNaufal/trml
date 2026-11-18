package simbroker

import (
	"time"

	"github.com/shopspring/decimal"
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

	realF, _ := s.RealizedEquityUSDT.Float64()
	effF, _ := s.calculateRealEffectiveEquityLocked().Float64()
	usedF, _ := s.usedMarginLocked().Float64()

	s.EquityCurve = append(s.EquityCurve, EquityPoint{
		Time:            simTime,
		RealizedEquity:  realF,
		EffectiveEquity: effF,
		UsedMargin:      usedF,
		OpenPositions:   len(s.OpenPositions),
	})
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
