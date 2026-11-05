package data

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Downloader struct {
	client   *http.Client
	cacheDir string
	baseURL  string
}

func NewDownloader(cacheDir string) *Downloader {
	if cacheDir == "" {
		cacheDir = filepath.Join("data", "backtest", "cache")
	}
	return &Downloader{
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		cacheDir: cacheDir,
		baseURL:  "https://data.binance.vision/data/futures/um",
	}
}

// FetchCandleRange mengunduh dan memuat candlestick Binance Vision untuk rentang tanggal yang diminta.
// Mengutamakan arsip bulanan (monthly), dan menyimpan cache lokal untuk eksekusi deterministik cepat.
func (d *Downloader) FetchCandleRange(
	ctx context.Context,
	symbol string,
	interval string,
	from time.Time,
	to time.Time,
) ([]HistoricalCandle, error) {
	if from.After(to) {
		return nil, fmt.Errorf("invalid range: from %s is after to %s", from, to)
	}

	symDir := filepath.Join(d.cacheDir, symbol, interval)
	if err := os.MkdirAll(symDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache dir: %w", err)
	}

	// Kumpulkan seluruh bulan yang bersinggungan dengan rentang [from..to]
	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)

	var allCandles []HistoricalCandle

	for !cur.After(endMonth) {
		year := cur.Year()
		month := int(cur.Month())

		candles, err := d.fetchMonthly(ctx, symbol, interval, year, month, symDir)
		if err != nil {
			slog.Info("monthly archive not available, falling back to daily klines", "symbol", symbol, "year", year, "month", month)
			// Fallback ke unduhan harian jika arsip bulanan belum ada (misal bulan berjalan)
			monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
			for day := monthStart; day.Month() == time.Month(month) && !day.After(to); day = day.AddDate(0, 0, 1) {
				if day.Before(time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)) {
					continue
				}
				dCandles, dErr := d.fetchDaily(ctx, symbol, interval, day, symDir)
				if dErr == nil {
					allCandles = append(allCandles, dCandles...)
				}
			}
		} else {
			allCandles = append(allCandles, candles...)
		}

		// Geser ke bulan berikutnya
		cur = cur.AddDate(0, 1, 0)
	}

	if len(allCandles) == 0 {
		return nil, fmt.Errorf("no kline data downloaded for %s between %s and %s", symbol, from.Format("2006-01-02"), to.Format("2006-01-02"))
	}

	// Deduplikasi dan sorting
	sort.Slice(allCandles, func(i, j int) bool {
		return allCandles[i].OpenTime.Before(allCandles[j].OpenTime)
	})

	deduped := make([]HistoricalCandle, 0, len(allCandles))
	seen := make(map[int64]bool)
	for _, c := range allCandles {
		ms := c.OpenTime.UnixMilli()
		if seen[ms] {
			continue
		}
		// Filter strictly within requested range [from..to]
		if c.OpenTime.Before(from) || c.OpenTime.After(to) {
			continue
		}
		seen[ms] = true
		deduped = append(deduped, c)
	}

	return deduped, nil
}

func (d *Downloader) fetchMonthly(
	ctx context.Context,
	symbol, interval string,
	year, month int,
	symDir string,
) ([]HistoricalCandle, error) {
	fileName := fmt.Sprintf("%s-%s-%04d-%02d.csv", symbol, interval, year, month)
	cacheFile := filepath.Join(symDir, fileName)

	// 1. Cek apakah CSV sudah ada di cache lokal
	if fileData, err := os.ReadFile(cacheFile); err == nil && len(fileData) > 0 {
		return ParseBinanceKlineCSV(bytes.NewReader(fileData), symbol)
	}

	// 2. Download ZIP dari Binance Vision
	zipName := fmt.Sprintf("%s-%s-%04d-%02d.zip", symbol, interval, year, month)
	url := fmt.Sprintf("%s/monthly/klines/%s/%s/%s", d.baseURL, symbol, interval, zipName)

	slog.Info("downloading Binance Vision monthly klines", "url", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("monthly archive not found on Binance Vision (HTTP 404): %s", url)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	zipBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	// 3. Ekstrak ZIP di memori
	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed unzipping archive from %s: %w", url, err)
	}

	for _, f := range zipReader.File {
		if strings.HasSuffix(f.Name, ".csv") {
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("failed opening %s inside zip: %w", f.Name, err)
			}
			csvBytes, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return nil, fmt.Errorf("failed reading csv %s: %w", f.Name, err)
			}

			// Simpan ke cache lokal
			_ = os.WriteFile(cacheFile, csvBytes, 0644)

			// Parse ke data struct
			return ParseBinanceKlineCSV(bytes.NewReader(csvBytes), symbol)
		}
	}

	return nil, fmt.Errorf("no .csv file found inside %s", zipName)
}

func (d *Downloader) fetchDaily(
	ctx context.Context,
	symbol, interval string,
	date time.Time,
	symDir string,
) ([]HistoricalCandle, error) {
	dateStr := date.Format("2006-01-02")
	fileName := fmt.Sprintf("%s-%s-%s.csv", symbol, interval, dateStr)
	cacheFile := filepath.Join(symDir, fileName)

	// 1. Cek apakah CSV sudah ada di cache lokal (jika bukan hari ini)
	nowUTC := time.Now().UTC()
	isToday := date.Year() == nowUTC.Year() && date.Month() == nowUTC.Month() && date.Day() == nowUTC.Day()
	if !isToday {
		if fileData, err := os.ReadFile(cacheFile); err == nil && len(fileData) > 0 {
			return ParseBinanceKlineCSV(bytes.NewReader(fileData), symbol)
		}
	}

	// 2. Download ZIP harian dari Binance Vision
	zipName := fmt.Sprintf("%s-%s-%s.zip", symbol, interval, dateStr)
	url := fmt.Sprintf("%s/daily/klines/%s/%s/%s", d.baseURL, symbol, interval, zipName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		// Fallback ke live Binance Futures API jika arsip Vision belum terbit (hari ini / ongoing)
		slog.Info("Binance Vision daily archive 404, attempting live Binance Futures API fallback", "symbol", symbol, "date", dateStr)
		return d.fetchFromLiveAPI(ctx, symbol, interval, date, cacheFile)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	zipBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed unzipping archive from %s: %w", url, err)
	}

	for _, f := range zipReader.File {
		if strings.HasSuffix(f.Name, ".csv") {
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("failed opening %s inside zip: %w", f.Name, err)
			}
			csvBytes, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return nil, fmt.Errorf("failed reading csv %s: %w", f.Name, err)
			}

			_ = os.WriteFile(cacheFile, csvBytes, 0644)
			return ParseBinanceKlineCSV(bytes.NewReader(csvBytes), symbol)
		}
	}

	return nil, fmt.Errorf("no .csv file found inside %s", zipName)
}

func (d *Downloader) fetchFromLiveAPI(
	ctx context.Context,
	symbol, interval string,
	date time.Time,
	cacheFile string,
) ([]HistoricalCandle, error) {
	startMs := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
	endMs := startMs + (24*time.Hour - time.Millisecond).Milliseconds()

	apiURL := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=1500",
		symbol, interval, startMs, endMs)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get live api %s: %w", apiURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("live api status %d for %s", resp.StatusCode, apiURL)
	}

	var rawKlines [][]interface{}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&rawKlines); err != nil {
		return nil, fmt.Errorf("failed decoding live klines: %w", err)
	}

	if len(rawKlines) == 0 {
		return nil, fmt.Errorf("no live klines returned for %s on %s", symbol, date.Format("2006-01-02"))
	}

	var sb strings.Builder
	sb.WriteString("open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n")
	for _, k := range rawKlines {
		if len(k) < 11 {
			continue
		}
		fmt.Fprintf(&sb, "%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
			k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7], k[8], k[9], k[10], 0)
	}

	csvBytes := []byte(sb.String())
	nowUTC := time.Now().UTC()
	if date.Year() != nowUTC.Year() || date.Month() != nowUTC.Month() || date.Day() != nowUTC.Day() {
		_ = os.WriteFile(cacheFile, csvBytes, 0644)
	}

	return ParseBinanceKlineCSV(bytes.NewReader(csvBytes), symbol)
}
