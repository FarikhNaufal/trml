package indicator

import (
	"math"
	"testing"
	"time"

	"trml/internal/model"
)

func TestCalculateEMA(t *testing.T) {
	// Buat 10 candle dengan close konstan 100
	candles := make([]model.Candle, 10)
	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		candles[i] = model.Candle{
			OpenTime: now.Add(time.Duration(i*5) * time.Minute),
			Close:    100.0,
		}
	}

	ema := CalculateEMA(candles, 5)
	if math.Abs(ema-100.0) > 1e-6 {
		t.Errorf("expected EMA 100, got %f", ema)
	}
}

func TestCalculateBarDeltaZScore(t *testing.T) {
	// Buat 51 deltas: 50 bernilai 10.0, delta ke-51 bernilai 20.0
	deltas := make([]float64, 51)
	for i := 0; i < 50; i++ {
		// variasikan sedikit agar stddev > 0
		if i%2 == 0 {
			deltas[i] = 10.0
		} else {
			deltas[i] = 12.0
		}
	}
	deltas[50] = 25.0 // Lonjakan agresi signifikan

	z, ok := CalculateBarDeltaZScore(deltas, 50)
	if !ok {
		t.Fatalf("expected valid Z-Score calculation")
	}
	if z <= 2.0 {
		t.Errorf("expected high positive Z-Score (> 2.0), got %f", z)
	}
}

func TestIndicators_NaNAndInfRejection(t *testing.T) {
	now := time.Now().UTC()

	// 1. BarDeltaZScore dengan NaN atau Inf
	deltas := make([]float64, 51)
	for i := 0; i < 50; i++ {
		deltas[i] = 10.0
	}
	deltas[25] = math.NaN()
	deltas[50] = 25.0
	if _, ok := CalculateBarDeltaZScore(deltas, 50); ok {
		t.Errorf("expected CalculateBarDeltaZScore to reject NaN delta")
	}

	deltas[25] = math.Inf(1)
	if _, ok := CalculateBarDeltaZScore(deltas, 50); ok {
		t.Errorf("expected CalculateBarDeltaZScore to reject +Inf delta")
	}

	// Zero stddev
	for i := 0; i < 50; i++ {
		deltas[i] = 10.0
	}
	deltas[50] = 10.0
	if _, ok := CalculateBarDeltaZScore(deltas, 50); ok {
		t.Errorf("expected CalculateBarDeltaZScore to reject zero variance baseline")
	}

	// 2. EMA Safe dengan NaN / Inf
	candles := make([]model.Candle, 10)
	for i := 0; i < 10; i++ {
		candles[i] = model.Candle{
			OpenTime: now.Add(time.Duration(i*5) * time.Minute),
			Close:    100.0,
		}
	}
	candles[4].Close = math.NaN()
	if _, ok := CalculateEMASafe(candles, 5); ok {
		t.Errorf("expected CalculateEMASafe to reject NaN close")
	}

	// 3. ATR Safe dengan NaN / Inf
	candlesATR := make([]model.Candle, 20)
	for i := 0; i < 20; i++ {
		candlesATR[i] = model.Candle{
			OpenTime: now.Add(time.Duration(i*5) * time.Minute),
			High:     110.0,
			Low:      90.0,
			Close:    100.0,
		}
	}
	candlesATR[5].High = math.NaN()
	if _, ok := CalculateATRSafe(candlesATR, 14); ok {
		t.Errorf("expected CalculateATRSafe to reject NaN high")
	}

	// 4. RVOL dengan NaN
	candlesRVOL := make([]model.Candle, 25)
	for i := 0; i < 25; i++ {
		candlesRVOL[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i*5) * time.Minute),
			VolumeBase: 1000.0,
		}
	}
	candlesRVOL[10].VolumeBase = math.NaN()
	if rvol := CalculateRVOL(candlesRVOL, 20); rvol != 0.0 {
		t.Errorf("expected CalculateRVOL to return 0 on NaN volume, got %f", rvol)
	}

	// 5. Session VWAP dengan NaN
	candlesVWAP := make([]model.Candle, 10)
	for i := 0; i < 10; i++ {
		candlesVWAP[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i*5) * time.Minute),
			High:       105.0,
			Low:        95.0,
			Close:      100.0,
			VolumeBase: 500.0,
		}
	}
	candlesVWAP[3].Close = math.NaN()
	if vwap := CalculateSessionVWAP(candlesVWAP); vwap != 0.0 {
		t.Errorf("expected CalculateSessionVWAP to return 0 on NaN close, got %f", vwap)
	}
}
