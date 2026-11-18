package data

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// ValidateCandles memeriksa kualitas data candlestick secara menyeluruh dan mengembalikan DataValidationResult.
func ValidateCandles(symbol string, candles []HistoricalCandle, expectedStep time.Duration) DataValidationResult {
	res := DataValidationResult{
		Symbol:    symbol,
		TotalBars: len(candles),
		Warnings:  make([]string, 0),
	}

	if len(candles) == 0 {
		res.Warnings = append(res.Warnings, "kumpulan candle kosong (0 bar)")
		return res
	}

	// Pastikan terurut berdasarkan OpenTime
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].OpenTime.Before(candles[j].OpenTime)
	})

	res.StartTime = candles[0].OpenTime
	res.EndTime = candles[len(candles)-1].CloseTime

	seenTimes := make(map[int64]bool)
	nowUTC := time.Now().UTC()

	for i := 0; i < len(candles); i++ {
		c := candles[i]
		openMs := c.OpenTime.UnixMilli()

		// Deteksi duplikasi
		if seenTimes[openMs] {
			res.Duplicates++
		}
		seenTimes[openMs] = true

		if c.CloseTime.After(nowUTC) {
			res.FutureBars++
		}

		// Deteksi harga tidak valid / NaN / Inf
		if math.IsNaN(c.Open) || math.IsNaN(c.High) || math.IsNaN(c.Low) || math.IsNaN(c.Close) ||
			math.IsInf(c.Open, 0) || math.IsInf(c.High, 0) || math.IsInf(c.Low, 0) || math.IsInf(c.Close, 0) ||
			c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 {
			res.OHLCAnomalies++
			res.Warnings = append(res.Warnings, fmt.Sprintf("bar %s memiliki harga <= 0 atau NaN/Inf", c.OpenTime.Format(time.RFC3339)))
		} else if c.Low > c.High || c.Low > math.Min(c.Open, c.Close) || c.High < math.Max(c.Open, c.Close) {
			res.OHLCAnomalies++
			res.Warnings = append(res.Warnings, fmt.Sprintf("bar %s anomali High/Low vs Open/Close", c.OpenTime.Format(time.RFC3339)))
		}

		// Deteksi volume nol
		if c.Volume <= 0 {
			res.ZeroVolCount++
		}

		// Deteksi celah waktu (gap) terhadap bar sebelumnya
		if i > 0 {
			prev := candles[i-1]
			diff := c.OpenTime.Sub(prev.OpenTime)
			if diff > expectedStep {
				res.GapsCount++
				if res.GapsCount <= 5 { // batasi log warning agar tidak flood
					res.Warnings = append(res.Warnings, fmt.Sprintf("gap waktu terdeteksi antara %s dan %s (durasi: %v)", prev.OpenTime.Format(time.RFC3339), c.OpenTime.Format(time.RFC3339), diff))
				}
			}
		}
	}

	if res.GapsCount > 5 {
		res.Warnings = append(res.Warnings, fmt.Sprintf("total %d gap waktu terdeteksi pada simbol %s", res.GapsCount, symbol))
	}

	return res
}

// ValidateCandlesWithOI audits OHLCV integrity and calculates historical point-in-time Open Interest coverage percentage.
func ValidateCandlesWithOI(
	symbol string,
	candles []HistoricalCandle,
	expectedStep time.Duration,
	oiMgr *MetricsOIDownloader,
	minOIInterval, maxOIInterval, maxOIStaleness time.Duration,
) DataValidationResult {
	res := ValidateCandles(symbol, candles, expectedStep)
	if oiMgr == nil || len(candles) == 0 {
		return res
	}
	covered := 0
	for _, c := range candles {
		if _, _, _, _, ok := oiMgr.LookupOpenInterestStrict(symbol, c.CloseTime, minOIInterval, maxOIInterval, maxOIStaleness); ok {
			covered++
		}
	}
	res.OICoveragePct = (float64(covered) / float64(len(candles))) * 100.0
	if res.OICoveragePct < 90.0 {
		res.Warnings = append(res.Warnings, fmt.Sprintf("cakupan OI historis 5m hanya %.1f%% (< 90%%)", res.OICoveragePct))
	}
	return res
}


