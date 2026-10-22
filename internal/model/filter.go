package model

import (
	"fmt"

	"github.com/shopspring/decimal"
)

type ExchangeFilter struct {
	TickSize    decimal.Decimal
	StepSize    decimal.Decimal
	MinQty      decimal.Decimal
	MinNotional decimal.Decimal
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
