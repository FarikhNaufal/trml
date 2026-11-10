package strategy

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestEvaluateSignal_DynamicTimeframe_OINormalization(t *testing.T) {
	now := time.Now().UTC()

	// 15m snapshot
	snap, baseCfg := helperCreateValidLongSnapshot(now)

	// Set prev and curr OI separated by 15 minutes (900s)
	snap.PrevOIUpdatedAt = now.Add(-15 * time.Minute)
	snap.CurrOIUpdatedAt = now
	snap.PrevOpenInterest = 10000.0
	// 0.25% raw OI increase: 10000 -> 10025
	snap.CurrOpenInterest = 10025.0

	// Set MinOIDeltaLongPct to 0.20%
	baseCfg.MinOIDeltaLongPct = 0.20
	baseCfg.MinOIDeltaPct = 0.20
	baseCfg.MinOIDeltaShortPct = 0.20

	// Case 1: Config with CandleDuration = 15m
	cfg15m := baseCfg
	cfg15m.CandleDuration = 15 * time.Minute
	cfg15m.MinOIInterval = 1 * time.Minute
	cfg15m.MaxOIInterval = 17 * time.Minute
	cfg15m.MaxOIStaleness = 15*time.Minute + 30*time.Second
	// With 15m basis: normalized OI = 0.25% * (900 / 900) = 0.25% >= 0.20% -> Pass OI filter
	_, ok15m, reason15m, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg15m)
	if !ok15m {
		t.Errorf("expected 15m signal to pass OI check, but rejected with reason: %s", reason15m)
	}

	// Case 2: Config with 15m vs 5m basis on 450s interval
	// Raw drop is -0.035% over 450s (10000 -> 9996.5)
	snapHalf := snap
	snapHalf.PrevOIUpdatedAt = now.Add(-450 * time.Second)
	snapHalf.CurrOpenInterest = 9996.5

	// With 15m basis: normalized OI = -0.035% * (900 / 450) = -0.070%
	// Capitulation floor is -0.05%, so -0.070% is below the capitulation floor -> Reject LOW_OI_DELTA
	_, okHalf15m, reasonHalf15m, subReasonHalf15m := EvaluateSignal(snapHalf, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg15m)
	if okHalf15m || !strings.Contains(subReasonHalf15m, "LOW_OI_DELTA") {
		t.Errorf("expected 15m basis (-0.070%%) to be rejected with LOW_OI_DELTA, got ok=%v reason=%s subReason=%s", okHalf15m, reasonHalf15m, subReasonHalf15m)
	}

	// With 5m basis: normalized OI = -0.035% * (300 / 450) = -0.0233%
	// Capitulation floor is -0.05%, so -0.0233% is above floor and rescued by capitulation -> Pass SIGNAL_APPROVED
	cfg5m := baseCfg
	cfg5m.CandleDuration = 5 * time.Minute
	cfg5m.MinOIInterval = 1 * time.Minute
	cfg5m.MaxOIInterval = 10 * time.Minute
	cfg5m.MaxOIStaleness = 5*time.Minute + 30*time.Second
	_, okHalf5m, reasonHalf5m, _ := EvaluateSignal(snapHalf, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg5m)
	if !okHalf5m {
		t.Errorf("expected 5m basis (-0.023%%) to pass via capitulation floor, got reason=%s", reasonHalf5m)
	}
}
