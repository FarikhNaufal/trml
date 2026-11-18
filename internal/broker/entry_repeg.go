package broker

import (
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

const (
	defaultLimitOrderTimeout     = 150 * time.Second
	defaultMaxEntryRepegAttempts = 3
	defaultMaxEntryRepegDriftPct = 0.0005
	maxEntryRepegATRFraction     = 0.25
)

func wouldCrossMakerBook(side model.Side, limitPrice decimal.Decimal, bestBid, bestAsk float64) bool {
	if limitPrice.LessThanOrEqual(decimal.Zero) {
		return false
	}
	if side == model.SideLong {
		return bestAsk > 0 && limitPrice.GreaterThanOrEqual(decimal.NewFromFloat(bestAsk))
	}
	return bestBid > 0 && limitPrice.LessThanOrEqual(decimal.NewFromFloat(bestBid))
}

func isGTXPostOnlyError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "-5022") ||
		strings.Contains(msg, "post only") ||
		strings.Contains(msg, "post-only") ||
		strings.Contains(msg, "would immediately match") ||
		strings.Contains(msg, "cross spread")
}

func resolveEntryRepegSettings(maxAttempts int, maxDriftPct float64) (int, float64) {
	if maxAttempts < 0 {
		maxAttempts = 0
	} else if maxAttempts == 0 {
		maxAttempts = defaultMaxEntryRepegAttempts
	}
	if maxDriftPct <= 0 {
		maxDriftPct = defaultMaxEntryRepegDriftPct
	}
	return maxAttempts, maxDriftPct
}

func quantizeBracketPrice(p, tickSize decimal.Decimal, roundUp bool) decimal.Decimal {
	if p.LessThanOrEqual(decimal.Zero) || tickSize.LessThanOrEqual(decimal.Zero) {
		return p
	}
	q, err := model.QuantizePrice(p, tickSize, roundUp)
	if err != nil {
		return p
	}
	return q
}

// repegLimitEntryIntent menyesuaikan LimitPrice agar tetap menjadi maker order yang aman terhadap BBO terkini,
// dengan menjaga batas deviasi harga dari sinyal awal (drift <= maxDriftPct dan <= 0.25 ATR) serta menggeser bracket SL/TP secara proporsional.
func repegLimitEntryIntent(
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
		maxDriftPct = defaultMaxEntryRepegDriftPct
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
		maxATRDrift := intent.ATRAtEntry.Mul(decimal.NewFromFloat(maxEntryRepegATRFraction))
		if diff.GreaterThan(maxATRDrift) {
			return intent, false, fmt.Sprintf("drift_%s_exceeds_atr_bound_%s", diff.String(), maxATRDrift.String())
		}
	}

	delta := target.Sub(intent.LimitPrice)
	updated := intent
	updated.LimitPrice = target

	if intent.SLPrice.GreaterThan(decimal.Zero) {
		updated.SLPrice = quantizeBracketPrice(intent.SLPrice.Add(delta), tick, isShort)
	}
	if intent.TPPrice.GreaterThan(decimal.Zero) {
		updated.TPPrice = quantizeBracketPrice(intent.TPPrice.Add(delta), tick, !isShort)
	}
	if intent.TP1Price.GreaterThan(decimal.Zero) {
		updated.TP1Price = quantizeBracketPrice(intent.TP1Price.Add(delta), tick, !isShort)
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

	return updated, true, ""
}

func matchesEntryClientID(actualID, baseID string) bool {
	return baseID == "" || actualID == baseID || strings.HasPrefix(actualID, baseID+"-r")
}
