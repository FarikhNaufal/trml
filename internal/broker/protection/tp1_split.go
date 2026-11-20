package protection

import (
	"github.com/shopspring/decimal"
	"trml/internal/model"
)

// PartialTP1SplitPlan captures the deterministic scale-out quantities and leg viability for TP1.
type PartialTP1SplitPlan struct {
	SplitPossible bool
	CloseQty      decimal.Decimal
	RemainingQty  decimal.Decimal
}

// PlanPartialTP1Split is the Single Source of Truth (SSOT) for calculating Partial TP1 close qty
// and validating Binance exchange rules (StepSize, MinQty, MinNotional) across SimBroker, DryRun, and Live.
func PlanPartialTP1Split(
	initialQty, currentQty, alreadyClosedTP1, tp1Price decimal.Decimal,
	ratio float64,
	filter model.ExchangeFilter,
) PartialTP1SplitPlan {
	if currentQty.LessThanOrEqual(decimal.Zero) || tp1Price.LessThanOrEqual(decimal.Zero) {
		return PartialTP1SplitPlan{}
	}
	if ratio <= 0.05 || ratio >= 0.95 {
		ratio = 0.50
	}
	if initialQty.LessThanOrEqual(decimal.Zero) {
		initialQty = currentQty
	}

	closeQty := initialQty.Mul(decimal.NewFromFloat(ratio))
	if alreadyClosedTP1.GreaterThan(decimal.Zero) {
		closeQty = closeQty.Sub(alreadyClosedTP1)
	}
	if filter.StepSize.GreaterThan(decimal.Zero) && closeQty.GreaterThan(decimal.Zero) {
		if qQty, err := model.QuantizeQtyFloor(closeQty, filter.StepSize); err == nil {
			closeQty = qQty
		}
	}

	if closeQty.LessThanOrEqual(decimal.Zero) || !closeQty.LessThan(currentQty) {
		return PartialTP1SplitPlan{}
	}

	remQty := currentQty.Sub(closeQty)
	if filter.MinQty.GreaterThan(decimal.Zero) && (closeQty.LessThan(filter.MinQty) || remQty.LessThan(filter.MinQty)) {
		return PartialTP1SplitPlan{}
	}

	minNotional := filter.MinNotional
	if minNotional.LessThanOrEqual(decimal.Zero) {
		minNotional = decimal.NewFromFloat(5.0)
	}
	if closeQty.Mul(tp1Price).LessThan(minNotional) || remQty.Mul(tp1Price).LessThan(minNotional) {
		return PartialTP1SplitPlan{}
	}

	return PartialTP1SplitPlan{
		SplitPossible: true,
		CloseQty:      closeQty,
		RemainingQty:  remQty,
	}
}
