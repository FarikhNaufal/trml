package data

import (
	"archive/zip"
	"bytes"
	"context"
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
			slog.Warn("failed to fetch monthly kline; attempting fallback or partial range", "symbol", symbol, "year", year, "month", month, "err", err)
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
