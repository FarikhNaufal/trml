package indicator

import (
	"trml/internal/model"
)

// CalculateRVOL menghitung rasio VolumeBase candle tertutup terakhir terhadap
// rata-rata VolumeBase dari 'lookback' candle tertutup sebelumnya (eksklusif candle terakhir).
func CalculateRVOL(candles []model.Candle, lookback int) float64 {
	if len(candles) < lookback+1 || lookback <= 0 {
		return 0.0
	}
	latestVol := candles[len(candles)-1].VolumeBase
	sumPrevVol := 0.0
	startIdx := len(candles) - 1 - lookback
	for i := startIdx; i < len(candles)-1; i++ {
		sumPrevVol += candles[i].VolumeBase
	}
	avgPrevVol := sumPrevVol / float64(lookback)
	if avgPrevVol <= 0 {
		return 0.0
	}
	return latestVol / avgPrevVol
}
