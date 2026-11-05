package data

import (
	"fmt"
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

	for i := 0; i < len(candles); i++ {
		c := candles[i]
		openMs := c.OpenTime.UnixMilli()

		// Deteksi duplikasi
		if seenTimes[openMs] {
			res.Duplicates++
		}
		seenTimes[openMs] = true

		// Deteksi harga tidak valid
		if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 {
			res.Warnings = append(res.Warnings, fmt.Sprintf("bar %s memiliki harga <= 0", c.OpenTime.Format(time.RFC3339)))
		}
		if c.Low > c.Open || c.Low > c.Close || c.High < c.Open || c.High < c.Close {
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

