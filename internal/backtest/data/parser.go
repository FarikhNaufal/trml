package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// ParseBinanceKlineCSV membaca stream CSV Binance Vision dan mengonversinya menjadi slice HistoricalCandle.
func ParseBinanceKlineCSV(r io.Reader, symbol string, expectedDuration ...time.Duration) ([]HistoricalCandle, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // izinkan fleksibilitas jika ada trailing column

	defaultStepMs := int64(5*60*1000 - 1)
	if len(expectedDuration) > 0 && expectedDuration[0] > 0 {
		defaultStepMs = expectedDuration[0].Milliseconds() - 1
	}

	var candles []HistoricalCandle
	isFirstLine := true

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv read error: %w", err)
		}

		if len(record) < 11 {
			continue
		}

		// Skip header jika ada (contoh: "open_time,open,...")
		if isFirstLine {
			isFirstLine = false
			if strings.EqualFold(strings.TrimSpace(record[0]), "open_time") {
				continue
			}
		}

		openTimeMs, err := strconv.ParseInt(strings.TrimSpace(record[0]), 10, 64)
		if err != nil {
			continue // baris korup atau header sekunder
		}

		openP, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64)
		if err != nil {
			continue
		}
		highP, err := strconv.ParseFloat(strings.TrimSpace(record[2]), 64)
		if err != nil {
			continue
		}
		lowP, err := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)
		if err != nil {
			continue
		}
		closeP, err := strconv.ParseFloat(strings.TrimSpace(record[4]), 64)
		if err != nil {
			continue
		}
		volume, err := strconv.ParseFloat(strings.TrimSpace(record[5]), 64)
		if err != nil {
			continue
		}

		closeTimeMs, err := strconv.ParseInt(strings.TrimSpace(record[6]), 10, 64)
		if err != nil {
			closeTimeMs = openTimeMs + defaultStepMs
		}

		quoteVol, err := strconv.ParseFloat(strings.TrimSpace(record[7]), 64)
		if err != nil {
			quoteVol = volume * closeP
		}

		tradeCount, _ := strconv.ParseInt(strings.TrimSpace(record[8]), 10, 64)
		takerBuyBase, _ := strconv.ParseFloat(strings.TrimSpace(record[9]), 64)
		takerBuyQuote, _ := strconv.ParseFloat(strings.TrimSpace(record[10]), 64)

		// Perhitungan Net Bar Delta USDT eksak:
		// Total Quote Volume = V_t
		// Taker Buy Quote Volume = V_b
		// Taker Sell Quote Volume = V_s = V_t - V_b
		// Net Delta = V_b - V_s = 2*V_b - V_t
		barDelta := 2.0*takerBuyQuote - quoteVol

		candle := HistoricalCandle{
			Symbol:              symbol,
			OpenTime:            time.UnixMilli(openTimeMs).UTC(),
			CloseTime:           time.UnixMilli(closeTimeMs).UTC(),
			Open:                openP,
			High:                highP,
			Low:                 lowP,
			Close:               closeP,
			Volume:              volume,
			QuoteVolume:         quoteVol,
			TradeCount:          tradeCount,
			TakerBuyBaseVolume:  takerBuyBase,
			TakerBuyQuoteVolume: takerBuyQuote,
			BarDeltaUSDT:        barDelta,
		}

		candles = append(candles, candle)
	}

	return candles, nil
}
