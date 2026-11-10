package strategy

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestOptionCLongSizing(t *testing.T) {
	snap := model.MarketSnapshot{
		Symbol: "SOLUSDT",
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: time.Now().UTC(),
	}

	cfg := SizingConfig{
		RiskPerTradePct:       decimal.NewFromFloat(0.01),
		MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
		AccountLeverage:       5.0,
		SLMultiplier:          1.5,
		SLMultiplierLong:      2.0,
		TPMultiplier:          3.0,
		TP1Multiplier:         1.2,
		TP1MultiplierLong:     0.8,
		MakerFeeRate:          decimal.NewFromFloat(0.0002),
		TakerFeeRate:          decimal.NewFromFloat(0.0005),
		SlippageRate:          decimal.NewFromFloat(0.0002),
	}

	equity := decimal.NewFromFloat(1000.0)
	avail := decimal.NewFromFloat(1000.0)
	rawEntry := 100.0
	atr14 := 2.0

	// Test LONG uses SLMultiplierLong (2.0 -> SL dist 4.0) and TP1MultiplierLong (0.8 -> TP1 dist 1.6)
	intentLong, ok := BuildSafeDecimalOrderIntent(snap, equity, avail, model.SideLong, rawEntry, atr14, cfg)
	if !ok {
		t.Fatalf("expected successful long intent creation")
	}
	expectedSLLong := decimal.NewFromFloat(96.00) // 100 - (2.0 * 2.0)
	if !intentLong.SLPrice.Equal(expectedSLLong) {
		t.Errorf("expected SL %s, got %s", expectedSLLong, intentLong.SLPrice)
	}
	expectedTP1Long := decimal.NewFromFloat(101.60) // 100 + (0.8 * 2.0)
	if !intentLong.TP1Price.Equal(expectedTP1Long) {
		t.Errorf("expected TP1 %s, got %s", expectedTP1Long, intentLong.TP1Price)
	}

	// Test SHORT uses default SLMultiplier (1.5 -> SL dist 3.0) and TP1Multiplier (1.2 -> TP1 dist 2.4)
	intentShort, ok := BuildSafeDecimalOrderIntent(snap, equity, avail, model.SideShort, rawEntry, atr14, cfg)
	if !ok {
		t.Fatalf("expected successful short intent creation")
	}
	expectedSLShort := decimal.NewFromFloat(103.00) // 100 + (1.5 * 2.0)
	if !intentShort.SLPrice.Equal(expectedSLShort) {
		t.Errorf("expected SL %s, got %s", expectedSLShort, intentShort.SLPrice)
	}
	expectedTP1Short := decimal.NewFromFloat(97.60) // 100 - (1.2 * 2.0)
	if !intentShort.TP1Price.Equal(expectedTP1Short) {
		t.Errorf("expected TP1 %s, got %s", expectedTP1Short, intentShort.TP1Price)
	}
}

func TestOptionCLongBodyFilter(t *testing.T) {
	now := time.Now().UTC()
	snap, cfg := helperCreateValidLongSnapshot(now)

	// Baseline snapshot passes
	_, ok, _, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !ok {
		t.Fatalf("expected baseline snapshot to pass")
	}

	// Turn on MinCandleBodyRatioLong=0.80 (very strict body required)
	cfg.MinCandleBodyRatioLong = 0.80
	_, ok, reason, subReason := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok {
		t.Errorf("expected rejection when body ratio does not meet 0.80")
	}
	if !strings.Contains(subReason, "CANDLE_NOT_BULLISH") {
		t.Errorf("expected subReason to contain CANDLE_NOT_BULLISH, got reason=%s subReason=%s", reason, subReason)
	}
}
