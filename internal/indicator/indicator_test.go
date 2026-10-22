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
