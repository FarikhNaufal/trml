package indicator

import (
	"math"
)

// CalculateMeanAndStdDev menghitung rata-rata dan deviasi standar populasi (/N).
func CalculateMeanAndStdDev(values []float64) (float64, float64) {
	n := len(values)
	if n == 0 {
		return 0.0, 0.0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(n)

	varSq := 0.0
	for _, v := range values {
		diff := v - mean
		varSq += diff * diff
	}
	return mean, math.Sqrt(varSq / float64(n))
}

// CalculateBarDeltaZScore menghitung Z-Score dari delta candle t terhadap baseline N candle sebelumnya.
// Baseline murni diambil dari deltas[len-1-window : len-1] tanpa mengikutsertakan candle t.
func CalculateBarDeltaZScore(deltas []float64, window int) (float64, bool) {
	n := len(deltas)
	if n < window+1 || window <= 0 {
		return 0.0, false
	}
	baseline := deltas[n-1-window : n-1]
	mean, std := CalculateMeanAndStdDev(baseline)
	if std <= 0 {
		return 0.0, false
	}
	latestDelta := deltas[n-1]
	zScore := (latestDelta - mean) / std
	return zScore, true
}
