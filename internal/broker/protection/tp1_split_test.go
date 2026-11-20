package protection

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestPlanPartialTP1Split(t *testing.T) {
	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.2),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	tp1Price := decimal.NewFromFloat(100.0)

	t.Run("standard 50 pct split quantized", func(t *testing.T) {
		plan := PlanPartialTP1Split(
			decimal.NewFromFloat(1.0),
			decimal.NewFromFloat(1.0),
			decimal.Zero,
			tp1Price,
			0.50,
			filter,
		)
		if !plan.SplitPossible || !plan.CloseQty.Equal(decimal.NewFromFloat(0.5)) || !plan.RemainingQty.Equal(decimal.NewFromFloat(0.5)) {
			t.Fatalf("unexpected plan: %+v", plan)
		}
	})

	t.Run("invalid ratio falls back to 0.50", func(t *testing.T) {
		plan := PlanPartialTP1Split(
			decimal.NewFromFloat(1.0),
			decimal.NewFromFloat(1.0),
			decimal.Zero,
			tp1Price,
			0.99,
			filter,
		)
		if !plan.SplitPossible || !plan.CloseQty.Equal(decimal.NewFromFloat(0.5)) {
			t.Fatalf("expected 0.50 fallback split, got %+v", plan)
		}
	})

	t.Run("subtracts alreadyClosedTP1", func(t *testing.T) {
		plan := PlanPartialTP1Split(
			decimal.NewFromFloat(1.0),
			decimal.NewFromFloat(0.8),
			decimal.NewFromFloat(0.2),
			tp1Price,
			0.50,
			filter,
		)
		if !plan.SplitPossible || !plan.CloseQty.Equal(decimal.NewFromFloat(0.3)) || !plan.RemainingQty.Equal(decimal.NewFromFloat(0.5)) {
			t.Fatalf("expected close=0.3 rem=0.5, got %+v", plan)
		}
	})

	t.Run("rejects below min notional or min qty", func(t *testing.T) {
		// 0.08 * 100 = 8 USDT total -> half = 4 USDT < 5 USDT MinNotional
		plan := PlanPartialTP1Split(
			decimal.NewFromFloat(0.8),
			decimal.NewFromFloat(0.8),
			decimal.Zero,
			decimal.NewFromFloat(10.0),
			0.50,
			filter,
		)
		if plan.SplitPossible {
			t.Fatalf("expected SplitPossible=false when notional < 5 USDT, got %+v", plan)
		}

		// Below MinQty
		planSmallQty := PlanPartialTP1Split(
			decimal.NewFromFloat(0.3),
			decimal.NewFromFloat(0.3),
			decimal.Zero,
			decimal.NewFromFloat(1000.0),
			0.50,
			filter,
		)
		if planSmallQty.SplitPossible {
			t.Fatalf("expected SplitPossible=false when legs < MinQty (0.2), got %+v", planSmallQty)
		}
	})
}

func TestWouldCrossAndRepegLimitEntryIntent(t *testing.T) {
	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}

	if !WouldCrossMakerBook(model.SideLong, decimal.NewFromFloat(100.0), 99.98, 100.00) {
		t.Fatalf("expected LONG limit >= bestAsk to cross")
	}
	if WouldCrossMakerBook(model.SideLong, decimal.NewFromFloat(99.99), 99.98, 100.00) {
		t.Fatalf("expected LONG limit < bestAsk to be passive")
	}
	if !WouldCrossMakerBook(model.SideShort, decimal.NewFromFloat(100.0), 100.00, 100.02) {
		t.Fatalf("expected SHORT limit <= bestBid to cross")
	}
	if WouldCrossMakerBook(model.SideShort, decimal.NewFromFloat(100.01), 100.00, 100.02) {
		t.Fatalf("expected SHORT limit > bestBid to be passive")
	}

	intent := model.OrderIntent{
		Symbol:     "SOLUSDT",
		Side:       model.SideLong,
		Qty:        decimal.NewFromFloat(1.0),
		LimitPrice: decimal.NewFromFloat(100.00),
		SLPrice:    decimal.NewFromFloat(98.00),
		TPPrice:    decimal.NewFromFloat(104.00),
		TP1Price:   decimal.NewFromFloat(102.00),
		ATRAtEntry: decimal.NewFromFloat(2.0),
	}

	updated, ok, reason := RepegLimitEntryIntent(intent, intent.LimitPrice, 99.96, 99.98, filter, 0.0005)
	if !ok {
		t.Fatalf("expected successful repeg within 0.05%%, failed: %s", reason)
	}
	if !updated.LimitPrice.Equal(decimal.NewFromFloat(99.96)) {
		t.Fatalf("expected new limit 99.96, got %s", updated.LimitPrice)
	}
	if !updated.SLPrice.Equal(decimal.NewFromFloat(97.96)) || !updated.TPPrice.Equal(decimal.NewFromFloat(103.96)) {
		t.Fatalf("expected shifted brackets SL=97.96 TP=103.96, got SL=%s TP=%s", updated.SLPrice, updated.TPPrice)
	}

	_, okFail, failReason := RepegLimitEntryIntent(intent, intent.LimitPrice, 99.00, 99.02, filter, 0.0005)
	if okFail || !strings.Contains(failReason, "drift") {
		t.Fatalf("expected drift rejection, got ok=%v reason=%s", okFail, failReason)
	}
}
