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
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0.0, 0.0
		}
		sum += v
	}
	mean := sum / float64(n)
	if math.IsNaN(mean) || math.IsInf(mean, 0) {
		return 0.0, 0.0
	}

	varSq := 0.0
	for _, v := range values {
		diff := v - mean
		varSq += diff * diff
	}
	variance := varSq / float64(n)
	if math.IsNaN(variance) || math.IsInf(variance, 0) || variance <= 0 {
		return mean, 0.0
	}
	std := math.Sqrt(variance)
	if math.IsNaN(std) || math.IsInf(std, 0) {
		return mean, 0.0
	}
	return mean, std
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
	if math.IsNaN(std) || math.IsInf(std, 0) || std <= 0 || math.IsNaN(mean) || math.IsInf(mean, 0) {
		return 0.0, false
	}
	latestDelta := deltas[n-1]
	if math.IsNaN(latestDelta) || math.IsInf(latestDelta, 0) {
		return 0.0, false
	}
	zScore := (latestDelta - mean) / std
	if math.IsNaN(zScore) || math.IsInf(zScore, 0) {
		return 0.0, false
	}
	return zScore, true
}
