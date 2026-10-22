package model

import (
	"fmt"
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

// ValidateAndFilterCandles memeriksa urutan strictly-monotonic, kline closed, dan harga valid (Solusi P1-5)
func ValidateAndFilterCandles(raw []Candle, serverTime time.Time) ([]Candle, error) {
	var valid []Candle
	for i, c := range raw {
		if !c.CloseTime.Before(serverTime) && !c.CloseTime.Equal(serverTime) {
			continue // Lewati candle aktif yang belum tertutup
		}
		if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 || c.Low > c.High {
			return nil, fmt.Errorf("candle index %d has invalid price values", i)
		}
		if c.High < c.Open || c.High < c.Close || c.Low > c.Open || c.Low > c.Close {
			return nil, fmt.Errorf("candle index %d violates OHLC bounds (High must >= Open,Close; Low must <= Open,Close)", i)
		}
		if c.VolumeBase < 0 || c.QuoteVolumeUSDT < 0 {
			return nil, fmt.Errorf("candle index %d has negative volume", i)
		}
		if len(valid) > 0 {
			prev := valid[len(valid)-1]
			if !c.OpenTime.After(prev.OpenTime) {
				return nil, fmt.Errorf("candle non-monotonic timestamp at index %d", i)
			}
			step := c.OpenTime.Sub(prev.OpenTime)
			if step != 5*time.Minute {
				return nil, fmt.Errorf("candle gap detected: expected 5m, got %v", step)
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
