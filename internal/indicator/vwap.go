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

// CalculateSessionVWAP menghitung Session VWAP (Midnight 00:00 UTC) dari candle tertutup.
// Hanya candle dari hari UTC yang sama dengan candle terakhir yang diakumulasikan.
func CalculateSessionVWAP(candles []model.Candle) float64 {
	if len(candles) == 0 {
		return 0.0
	}
	latest := candles[len(candles)-1]
	currentEpochDay := latest.OpenTime.UTC().Unix() / 86400

	var cumPV, cumVol float64
	for _, c := range candles {
		if c.OpenTime.UTC().Unix()/86400 == currentEpochDay {
			typical := (c.High + c.Low + c.Close) / 3.0
			cumPV += typical * c.VolumeBase
			cumVol += c.VolumeBase
		}
	}
	if cumVol <= 0 {
		return 0.0
	}
	return cumPV / cumVol
}
