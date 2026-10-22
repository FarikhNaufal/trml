package indicator

import (
	"math"

	"trml/internal/model"
)

// CalculateEMA menghitung Exponential Moving Average dari slice candle.
// Menggunakan SMA dari 'period' bar pertama sebagai seed, lalu menerapkan rekursi multiplier = 2 / (period + 1).
func CalculateEMA(candles []model.Candle, period int) float64 {
	val, _ := CalculateEMASafe(candles, period)
	return val
}

// CalculateEMASafe menghitung EMA dan mengembalikan false jika panjang candle tidak mencukupi
func CalculateEMASafe(candles []model.Candle, period int) (float64, bool) {
	if len(candles) < period || period <= 0 {
		return 0.0, false
	}
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += candles[i].Close
	}
	ema := sum / float64(period)
	multiplier := 2.0 / float64(period+1)

	for i := period; i < len(candles); i++ {
		ema = (candles[i].Close-ema)*multiplier + ema
	}
	return ema, true
}

// CalculateATR menghitung Average True Range menggunakan Wilder's Smoothing (RMA).
// Membutuhkan minimal (period + 1) candle: period candle untuk seed SMA awal dari True Range,
// dan candle-candle selanjutnya untuk rekursi smoothing RMA: ATR_t = ((ATR_{t-1} * (N-1)) + TR_t) / N.
func CalculateATR(candles []model.Candle, period int) float64 {
	val, _ := CalculateATRSafe(candles, period)
	return val
}

// CalculateATRSafe menghitung ATR dan mengembalikan false jika panjang candle tidak mencukupi
func CalculateATRSafe(candles []model.Candle, period int) (float64, bool) {
	if len(candles) < period+1 || period <= 0 {
		return 0.0, false
	}
	trs := make([]float64, len(candles)-1)
	for i := 1; i < len(candles); i++ {
		highLow := candles[i].High - candles[i].Low
		highPrevClose := math.Abs(candles[i].High - candles[i-1].Close)
		lowPrevClose := math.Abs(candles[i].Low - candles[i-1].Close)
		trs[i-1] = math.Max(highLow, math.Max(highPrevClose, lowPrevClose))
	}

	sumTR := 0.0
	for i := 0; i < period; i++ {
		sumTR += trs[i]
	}
	atr := sumTR / float64(period)

	for i := period; i < len(trs); i++ {
		atr = ((atr * float64(period-1)) + trs[i]) / float64(period)
	}
	return atr, true
}
