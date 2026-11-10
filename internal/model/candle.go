package model

import (
	"fmt"
	"math"
	"time"
)

type Candle struct {
	OpenTime          time.Time
	CloseTime         time.Time
	Open              float64
	High              float64
	Low               float64
	Close             float64
	VolumeBase        float64 // Kline index [5] -> Session VWAP & RVOL
	QuoteVolumeUSDT   float64 // Kline index [7] -> Likuiditas & Bar Delta
	TakerBuyQuoteUSDT float64 // Kline index [10] -> Bar Delta
}

func isInvalidFloat(v float64) bool {
	return math.IsNaN(v) || math.IsInf(v, 0)
}

// ValidateAndFilterCandles memeriksa urutan strictly-monotonic, kline closed, dan harga valid (Solusi P1-5)
func ValidateAndFilterCandles(raw []Candle, serverTime time.Time, expectedStep ...time.Duration) ([]Candle, error) {
	targetStep := 5 * time.Minute
	if len(expectedStep) > 0 && expectedStep[0] > 0 {
		targetStep = expectedStep[0]
	}
	var valid []Candle
	for i, c := range raw {
		if !c.CloseTime.Before(serverTime) && !c.CloseTime.Equal(serverTime) {
			continue // Lewati candle aktif yang belum tertutup
		}
		if isInvalidFloat(c.Open) || isInvalidFloat(c.High) || isInvalidFloat(c.Low) || isInvalidFloat(c.Close) {
			return nil, fmt.Errorf("candle index %d has NaN/Inf price values", i)
		}
		if isInvalidFloat(c.VolumeBase) || isInvalidFloat(c.QuoteVolumeUSDT) || isInvalidFloat(c.TakerBuyQuoteUSDT) {
			return nil, fmt.Errorf("candle index %d has NaN/Inf volume values", i)
		}
		if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 || c.Low > c.High {
			return nil, fmt.Errorf("candle index %d has invalid price values", i)
		}
		if c.High < c.Open || c.High < c.Close || c.Low > c.Open || c.Low > c.Close {
			return nil, fmt.Errorf("candle index %d violates OHLC bounds (High must >= Open,Close; Low must <= Open,Close)", i)
		}
		if c.VolumeBase < 0 || c.QuoteVolumeUSDT < 0 || c.TakerBuyQuoteUSDT < 0 {
			return nil, fmt.Errorf("candle index %d has negative volume", i)
		}
		if len(valid) > 0 {
			prev := valid[len(valid)-1]
			if !c.OpenTime.After(prev.OpenTime) {
				return nil, fmt.Errorf("candle non-monotonic timestamp at index %d", i)
			}
			step := c.OpenTime.Sub(prev.OpenTime)
			if step != targetStep {
				return nil, fmt.Errorf("candle gap detected: expected %v, got %v", targetStep, step)
			}
		}
		valid = append(valid, c)
	}
	return valid, nil
}

// Ring buffer slice helpers terpusat (Anti-Duplikasi Helper)
func AppendCandleWithCap(slice []Candle, elem Candle, capLimit int) []Candle {
	slice = append(slice, elem)
	if len(slice) > capLimit {
		slice = slice[len(slice)-capLimit:]
	}
	return slice
}

func AppendFloatWithCap(slice []float64, elem float64, capLimit int) []float64 {
	slice = append(slice, elem)
	if len(slice) > capLimit {
		slice = slice[len(slice)-capLimit:]
	}
	return slice
}
