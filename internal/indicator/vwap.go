package indicator

import (
	"math"

	"trml/internal/model"
)

// CalculateRVOL menghitung rasio VolumeBase candle tertutup terakhir terhadap
// rata-rata VolumeBase dari 'lookback' candle tertutup sebelumnya (eksklusif candle terakhir).
func CalculateRVOL(candles []model.Candle, lookback int) float64 {
	if len(candles) < lookback+1 || lookback <= 0 {
		return 0.0
	}
	latestVol := candles[len(candles)-1].VolumeBase
	if math.IsNaN(latestVol) || math.IsInf(latestVol, 0) || latestVol < 0 {
		return 0.0
	}
	sumPrevVol := 0.0
	startIdx := len(candles) - 1 - lookback
	for i := startIdx; i < len(candles)-1; i++ {
		v := candles[i].VolumeBase
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return 0.0
		}
		sumPrevVol += v
	}
	avgPrevVol := sumPrevVol / float64(lookback)
	if avgPrevVol <= 0 || math.IsNaN(avgPrevVol) || math.IsInf(avgPrevVol, 0) {
		return 0.0
	}
	rvol := latestVol / avgPrevVol
	if math.IsNaN(rvol) || math.IsInf(rvol, 0) || rvol < 0 {
		return 0.0
	}
	return rvol
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
			if math.IsNaN(c.High) || math.IsNaN(c.Low) || math.IsNaN(c.Close) || math.IsNaN(c.VolumeBase) ||
				math.IsInf(c.High, 0) || math.IsInf(c.Low, 0) || math.IsInf(c.Close, 0) || math.IsInf(c.VolumeBase, 0) {
				return 0.0
			}
			typical := (c.High + c.Low + c.Close) / 3.0
			cumPV += typical * c.VolumeBase
			cumVol += c.VolumeBase
		}
	}
	if cumVol <= 0 || math.IsNaN(cumVol) || math.IsInf(cumVol, 0) {
		return 0.0
	}
	vwap := cumPV / cumVol
	if math.IsNaN(vwap) || math.IsInf(vwap, 0) || vwap <= 0 {
		return 0.0
	}
	return vwap
}
