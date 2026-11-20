package broker

import (
	"strings"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
	"trml/internal/model"
)

const defaultLimitOrderTimeout = protection.DefaultLimitOrderTimeout

func wouldCrossMakerBook(side model.Side, limitPrice decimal.Decimal, bestBid, bestAsk float64) bool {
	return protection.WouldCrossMakerBook(side, limitPrice, bestBid, bestAsk)
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
	return protection.ResolveEntryRepegSettings(maxAttempts, maxDriftPct)
}

// repegLimitEntryIntent delegates to protection.RepegLimitEntryIntent (SSOT shared across Live, DryRun, and SimBroker).
func repegLimitEntryIntent(
	intent model.OrderIntent,
	origLimit decimal.Decimal,
	bestBid, bestAsk float64,
	filter model.ExchangeFilter,
	maxDriftPct float64,
) (model.OrderIntent, bool, string) {
	return protection.RepegLimitEntryIntent(intent, origLimit, bestBid, bestAsk, filter, maxDriftPct)
}

func matchesEntryClientID(actualID, baseID string) bool {
	return baseID == "" || actualID == baseID || strings.HasPrefix(actualID, baseID+"-r")
}
