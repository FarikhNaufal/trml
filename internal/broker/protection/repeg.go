package protection

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

const (
	DefaultLimitOrderTimeout     = 150 * time.Second
	DefaultMaxEntryRepegAttempts = 3
	DefaultMaxEntryRepegDriftPct = 0.0005
	MaxEntryRepegATRFraction     = 0.25
)

// WouldCrossMakerBook reports whether placing a post-only LIMIT order at limitPrice would cross
// the current inside quote (buying at/above bestAsk, or selling at/below bestBid).
func WouldCrossMakerBook(side model.Side, limitPrice decimal.Decimal, bestBid, bestAsk float64) bool {
	if limitPrice.LessThanOrEqual(decimal.Zero) {
		return false
	}
	if side == model.SideLong {
		return bestAsk > 0 && limitPrice.GreaterThanOrEqual(decimal.NewFromFloat(bestAsk))
	}
	return bestBid > 0 && limitPrice.LessThanOrEqual(decimal.NewFromFloat(bestBid))
}

// ResolveEntryRepegSettings normalizes attempt count and maximum drift percentage for GTX entry repegging.
func ResolveEntryRepegSettings(maxAttempts int, maxDriftPct float64) (int, float64) {
	if maxAttempts < 0 {
		maxAttempts = 0
	} else if maxAttempts == 0 {
		maxAttempts = DefaultMaxEntryRepegAttempts
	}
	if maxDriftPct <= 0 {
		maxDriftPct = DefaultMaxEntryRepegDriftPct
	}
	return maxAttempts, maxDriftPct
}

// RepegLimitEntryIntent adjusts LimitPrice so it remains a safe maker quote against the current BBO
// while enforcing origin drift limits (maxDriftPct and 0.25 ATR), bracket geometry, and MinNotional.
func RepegLimitEntryIntent(
	intent model.OrderIntent,
	origLimit decimal.Decimal,
	bestBid, bestAsk float64,
	filter model.ExchangeFilter,
	maxDriftPct float64,
) (model.OrderIntent, bool, string) {
	if bestBid <= 0 || bestAsk <= 0 {
		return intent, false, "invalid_bbo"
	}
	if origLimit.LessThanOrEqual(decimal.Zero) {
		origLimit = intent.LimitPrice
	}
	if origLimit.LessThanOrEqual(decimal.Zero) {
		return intent, false, "invalid_origin_limit"
	}
	if maxDriftPct <= 0 {
		maxDriftPct = DefaultMaxEntryRepegDriftPct
	}

	tick := filter.TickSize
	bidDec := decimal.NewFromFloat(bestBid)
	askDec := decimal.NewFromFloat(bestAsk)
	isShort := intent.Side == model.SideShort

	var target decimal.Decimal
	if isShort {
		target = askDec
		if tick.GreaterThan(decimal.Zero) {
			if q, err := model.QuantizePrice(askDec, tick, true); err == nil {
				target = q
			}
			if target.LessThanOrEqual(bidDec) {
				if q, err := model.QuantizePrice(bidDec.Add(tick), tick, true); err == nil {
					target = q
				} else {
					target = bidDec.Add(tick)
				}
			}
		}
		if target.LessThanOrEqual(bidDec) {
			return intent, false, "unable_to_find_maker_ask"
		}
	} else {
		target = bidDec
		if tick.GreaterThan(decimal.Zero) {
			if q, err := model.QuantizePrice(bidDec, tick, false); err == nil {
				target = q
			}
			if target.GreaterThanOrEqual(askDec) {
				if q, err := model.QuantizePrice(askDec.Sub(tick), tick, false); err == nil {
					target = q
				} else {
					target = askDec.Sub(tick)
				}
			}
		}
		if target.LessThanOrEqual(decimal.Zero) || target.GreaterThanOrEqual(askDec) {
			return intent, false, "unable_to_find_maker_bid"
		}
	}

	diff := target.Sub(origLimit).Abs()
	maxDriftDist := origLimit.Mul(decimal.NewFromFloat(maxDriftPct))
	if tick.GreaterThan(decimal.Zero) {
		minOneTickRoom := tick.Mul(decimal.NewFromFloat(1.5))
		if minOneTickRoom.GreaterThan(maxDriftDist) {
			maxDriftDist = minOneTickRoom
		}
	}
	if diff.GreaterThan(maxDriftDist) {
		return intent, false, fmt.Sprintf("drift_%s_exceeds_max_%s", diff.String(), maxDriftDist.String())
	}
	if intent.ATRAtEntry.GreaterThan(decimal.Zero) {
		maxATRDrift := intent.ATRAtEntry.Mul(decimal.NewFromFloat(MaxEntryRepegATRFraction))
		if diff.GreaterThan(maxATRDrift) {
			return intent, false, fmt.Sprintf("drift_%s_exceeds_atr_bound_%s", diff.String(), maxATRDrift.String())
		}
	}

	delta := target.Sub(intent.LimitPrice)
	updated := intent
	updated.LimitPrice = target

	if intent.SLPrice.GreaterThan(decimal.Zero) {
		updated.SLPrice = quantizePriceSafely(intent.SLPrice.Add(delta), tick, isShort)
	}
	if intent.TPPrice.GreaterThan(decimal.Zero) {
		updated.TPPrice = quantizePriceSafely(intent.TPPrice.Add(delta), tick, !isShort)
	}
	if intent.TP1Price.GreaterThan(decimal.Zero) {
		updated.TP1Price = quantizePriceSafely(intent.TP1Price.Add(delta), tick, !isShort)
	}

	if isShort {
		if updated.SLPrice.GreaterThan(decimal.Zero) && updated.SLPrice.LessThanOrEqual(updated.LimitPrice) {
			return intent, false, "repeg_invalid_short_sl"
		}
		if updated.TPPrice.GreaterThan(decimal.Zero) && updated.TPPrice.GreaterThanOrEqual(updated.LimitPrice) {
			return intent, false, "repeg_invalid_short_tp"
		}
		if updated.TP1Price.GreaterThan(decimal.Zero) && updated.TP1Price.GreaterThanOrEqual(updated.LimitPrice) {
			return intent, false, "repeg_invalid_short_tp1"
		}
	} else {
		if updated.SLPrice.GreaterThan(decimal.Zero) && updated.SLPrice.GreaterThanOrEqual(updated.LimitPrice) {
			return intent, false, "repeg_invalid_long_sl"
		}
		if updated.TPPrice.GreaterThan(decimal.Zero) && updated.TPPrice.LessThanOrEqual(updated.LimitPrice) {
			return intent, false, "repeg_invalid_long_tp"
		}
		if updated.TP1Price.GreaterThan(decimal.Zero) && updated.TP1Price.LessThanOrEqual(updated.LimitPrice) {
			return intent, false, "repeg_invalid_long_tp1"
		}
	}

	if filter.MinNotional.GreaterThan(decimal.Zero) && updated.Qty.GreaterThan(decimal.Zero) {
		if updated.Qty.Mul(updated.LimitPrice).LessThan(filter.MinNotional) {
			return intent, false, fmt.Sprintf("repeg_notional_%s_below_min_%s", updated.Qty.Mul(updated.LimitPrice).String(), filter.MinNotional.String())
		}
	}

	return updated, true, ""
}
