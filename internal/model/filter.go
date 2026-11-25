package model

import (
	"fmt"

	"github.com/shopspring/decimal"
)

type ExchangeFilter struct {
	TickSize       decimal.Decimal
	StepSize       decimal.Decimal
	MinQty         decimal.Decimal
	MaxQty         decimal.Decimal
	MinNotional    decimal.Decimal
	MarketStepSize decimal.Decimal
	MarketMinQty   decimal.Decimal
	MarketMaxQty   decimal.Decimal
}

// EffectiveStepSize returns the lot step size that satisfies both order entry and market/stop-market exits.
func (f ExchangeFilter) EffectiveStepSize(isMarket bool) decimal.Decimal {
	step := f.StepSize
	if f.MarketStepSize.GreaterThan(decimal.Zero) && (isMarket || f.MarketStepSize.GreaterThan(step)) {
		return f.MarketStepSize
	}
	return step
}

// EffectiveMinQty returns the minimum order quantity enforced for entry/exit.
func (f ExchangeFilter) EffectiveMinQty(isMarket bool) decimal.Decimal {
	minQ := f.MinQty
	if isMarket && f.MarketMinQty.GreaterThan(minQ) {
		return f.MarketMinQty
	}
	return minQ
}

// EffectiveMaxQty returns the tightest positive maximum quantity across LOT_SIZE and MARKET_LOT_SIZE.
func (f ExchangeFilter) EffectiveMaxQty(isMarket bool) decimal.Decimal {
	_ = isMarket // Even LIMIT entries must stay within MARKET_LOT_SIZE.maxQty so STOP_MARKET / emergency closes succeed.
	if f.MaxQty.GreaterThan(decimal.Zero) && f.MarketMaxQty.GreaterThan(decimal.Zero) {
		return decimal.Min(f.MaxQty, f.MarketMaxQty)
	}
	if f.MarketMaxQty.GreaterThan(decimal.Zero) {
		return f.MarketMaxQty
	}
	if f.MaxQty.GreaterThan(decimal.Zero) {
		return f.MaxQty
	}
	return decimal.Zero
}

// QuantizePrice melakukan pembulatan harga bursa secara fail-closed dan kebal off-by-one epsilon (Solusi P1-9 & M6)
func QuantizePrice(price, tickSize decimal.Decimal, roundUp bool) (decimal.Decimal, error) {
	if price.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize price: price must be positive, got %s", price.String())
	}
	if tickSize.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize price: invalid tickSize <= 0 (%s)", tickSize.String())
	}
	rem := price.Mod(tickSize)
	if rem.IsZero() {
		return price, nil
	}
	if roundUp {
		return price.Sub(rem).Add(tickSize), nil
	}
	return price.Sub(rem), nil
}

// QuantizeQtyFloor melakukan pembulatan lot kuantitas bursa ke bawah secara fail-closed (Solusi P1-9)
func QuantizeQtyFloor(qty, stepSize decimal.Decimal) (decimal.Decimal, error) {
	if qty.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize qty: qty must be positive, got %s", qty.String())
	}
	if stepSize.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize qty: invalid stepSize <= 0 (%s)", stepSize.String())
	}
	rem := qty.Mod(stepSize)
	if rem.IsZero() {
		return qty, nil
	}
	return qty.Sub(rem), nil
}
