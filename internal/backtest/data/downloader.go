package data

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"trml/internal/config"
)

type Downloader struct {
	dirs       BacktestDirs
	cacheDir   string
	baseURL    string
	offline    bool
	httpClient *BacktestHTTPClient
	store      *BacktestStore
}

func NewDownloader(cacheDir string) *Downloader {
	dirs := ResolveBacktestDirs(cacheDir)
	return &Downloader{
		dirs:       dirs,
		cacheDir:   dirs.LegacyCSV,
		baseURL:    "https://data.binance.vision/data/futures/um",
		httpClient: NewBacktestHTTPClient(BacktestHTTPConfig{AllowRESTFallback: false}),
	}
}

func (d *Downloader) SetOffline(offline bool) {
	d.offline = offline
}

func (d *Downloader) SetAllowRESTFallback(allow bool) {
	if d.httpClient == nil {
		d.httpClient = NewBacktestHTTPClient(BacktestHTTPConfig{AllowRESTFallback: allow})
		return
	}
	d.httpClient.SetAllowRESTFallback(allow)
}

func (d *Downloader) SetHTTPClient(client *BacktestHTTPClient) {
	if client != nil {
		d.httpClient = client
	}
}

func (d *Downloader) SetStore(store *BacktestStore) {
	d.store = store
}

// FetchCandleRange downloads and loads Binance Vision candlesticks across [from..to].
// Prefers precompiled .bin files and closed monthly CSV archives; never caches unclosed future bars.
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

	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)
	nowUTC := time.Now().UTC()

	var allCandles []HistoricalCandle

	for !cur.After(endMonth) {
		year := cur.Year()
		month := int(cur.Month())

		isCurrentMonth := year == nowUTC.Year() && time.Month(month) == nowUTC.Month()
		var candles []HistoricalCandle
		var err error
		if !isCurrentMonth {
			candles, err = d.fetchMonthly(ctx, symbol, interval, year, month, symDir, nowUTC)
		} else {
			err = fmt.Errorf("current month has no monthly archive")
		}

		if err != nil {
			if !isCurrentMonth {
				slog.Debug("monthly archive not available, falling back to daily klines", "symbol", symbol, "year", year, "month", month)
			}
			monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
			for day := monthStart; day.Month() == time.Month(month) && !day.After(to); day = day.AddDate(0, 0, 1) {
				if day.Before(time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)) {
					continue
				}
				dCandles, dErr := d.fetchDaily(ctx, symbol, interval, day, symDir, nowUTC)
				if dErr == nil {
					allCandles = append(allCandles, dCandles...)
				}
			}
		} else {
			allCandles = append(allCandles, candles...)
		}

		cur = cur.AddDate(0, 1, 0)
	}

	if len(allCandles) == 0 {
		return nil, fmt.Errorf("no kline data downloaded for %s between %s and %s", symbol, from.Format("2006-01-02"), to.Format("2006-01-02"))
	}

	// Only accept closed, finite OHLCV candles inside [from..to].
	allCandles = FilterClosedCandles(allCandles, nowUTC)
	sort.Slice(allCandles, func(i, j int) bool {
		return allCandles[i].OpenTime.Before(allCandles[j].OpenTime)
	})

	deduped := make([]HistoricalCandle, 0, len(allCandles))
	seen := make(map[int64]bool, len(allCandles))
	for _, c := range allCandles {
		ms := c.OpenTime.UnixMilli()
		if seen[ms] || c.OpenTime.Before(from) || c.OpenTime.After(to) {
			continue
		}
		seen[ms] = true
		deduped = append(deduped, c)
	}

	return deduped, nil
}

func parseDurationWithFallback(interval string) time.Duration {
	dur, err := config.ParseTimeframeToDuration(interval)
	if err != nil || dur <= 0 {
		slog.Warn("Unknown interval for duration calculation, defaulting to 5m", "interval", interval, "err", err)
		return 5 * time.Minute
	}
	return dur
}

func (d *Downloader) fetchMonthly(
	ctx context.Context,
	symbol, interval string,
	year, month int,
	symDir string,
	nowUTC time.Time,
) ([]HistoricalCandle, error) {
	ym := fmt.Sprintf("%04d-%02d", year, month)
	binPath := filepath.Join(d.dirs.StoreDir, symbol, interval, ym+".bin")
	if bars, err := ReadCompactBarsBin(binPath, symbol); err == nil && len(bars) > 0 {
		return bars, nil
	}

	fileName := fmt.Sprintf("%s-%s-%s.csv", symbol, interval, ym)
	cacheFile := filepath.Join(symDir, fileName)
	dur := parseDurationWithFallback(interval)

	// 1. Check CSV cache and lazily compile into compact .bin
	if fileData, err := os.ReadFile(cacheFile); err == nil && len(fileData) > 0 {
		parsed, pErr := ParseBinanceKlineCSV(bytes.NewReader(fileData), symbol, dur)
		if pErr == nil && len(parsed) > 0 {
			closed := FilterClosedCandles(parsed, nowUTC)
			if len(closed) > 0 {
				_ = WriteCompactBarsBin(binPath, closed)
				if d.store != nil {
					_ = d.store.UpsertCacheManifest(ctx, fmt.Sprintf("kline:%s:%s:%s", symbol, interval, ym), len(closed), closed[0].OpenTime.UnixMilli(), closed[len(closed)-1].CloseTime.UnixMilli(), false)
				}
			}
			return closed, nil
		}
	}

	if d.offline {
		return nil, fmt.Errorf("offline mode: candle bulanan %s-%s-%s tidak ada di cache", symbol, interval, ym)
	}

	// 2. Download ZIP via shared rate-safe HTTP client (Binance Vision S3)
	zipName := fmt.Sprintf("%s-%s-%s.zip", symbol, interval, ym)
	rawURL := fmt.Sprintf("%s/monthly/klines/%s/%s/%s", d.baseURL, symbol, interval, zipName)
	zipBytes, status, err := d.httpClient.GetVision(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		return nil, fmt.Errorf("monthly archive not found on Binance Vision (HTTP 404): %s", rawURL)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed unzipping archive from %s: %w", rawURL, err)
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

			parsed, pErr := ParseBinanceKlineCSV(bytes.NewReader(csvBytes), symbol, dur)
			if pErr != nil {
				return nil, pErr
			}
			closed := FilterClosedCandles(parsed, nowUTC)
			_ = WriteFileAtomic(cacheFile, csvBytes, 0644)
			if len(closed) > 0 {
				_ = WriteCompactBarsBin(binPath, closed)
				if d.store != nil {
					_ = d.store.UpsertCacheManifest(ctx, fmt.Sprintf("kline:%s:%s:%s", symbol, interval, ym), len(closed), closed[0].OpenTime.UnixMilli(), closed[len(closed)-1].CloseTime.UnixMilli(), false)
				}
			}
			return closed, nil
		}
	}

	return nil, fmt.Errorf("no .csv file found inside %s", zipName)
}

func (d *Downloader) fetchDaily(
	ctx context.Context,
	symbol, interval string,
	date time.Time,
	symDir string,
	nowUTC time.Time,
) ([]HistoricalCandle, error) {
	dateStr := date.Format("2006-01-02")
	fileName := fmt.Sprintf("%s-%s-%s.csv", symbol, interval, dateStr)
	cacheFile := filepath.Join(symDir, fileName)
	dur := parseDurationWithFallback(interval)

	isToday := date.Year() == nowUTC.Year() && date.Month() == nowUTC.Month() && date.Day() == nowUTC.Day()
	if fileData, err := os.ReadFile(cacheFile); err == nil && len(fileData) > 0 {
		info, sErr := os.Stat(cacheFile)
		if !isToday || (sErr == nil && time.Since(info.ModTime()) < 5*time.Minute) || d.offline {
			parsed, pErr := ParseBinanceKlineCSV(bytes.NewReader(fileData), symbol, dur)
			if pErr == nil {
				return FilterClosedCandles(parsed, nowUTC), nil
			}
		}
	}

	if d.offline {
		return nil, fmt.Errorf("offline mode: candle harian %s-%s-%s tidak ada di cache", symbol, interval, dateStr)
	}

	zipName := fmt.Sprintf("%s-%s-%s.zip", symbol, interval, dateStr)
	rawURL := fmt.Sprintf("%s/daily/klines/%s/%s/%s", d.baseURL, symbol, interval, zipName)
	zipBytes, status, err := d.httpClient.GetVision(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		// Protected REST fallback (only runs when explicitly enabled via --allow-rest-fallback / --today, and rate-limited <= 900w/m).
		if !d.httpClient.AllowRESTFallback() {
			return nil, fmt.Errorf("Binance Vision daily archive 404 for %s %s (REST fallback disabled to protect IP)", symbol, dateStr)
		}
		return d.fetchFromLiveAPI(ctx, symbol, interval, date, cacheFile, isToday, nowUTC)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed unzipping archive from %s: %w", rawURL, err)
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

			parsed, pErr := ParseBinanceKlineCSV(bytes.NewReader(csvBytes), symbol, dur)
			if pErr != nil {
				return nil, pErr
			}
			closed := FilterClosedCandles(parsed, nowUTC)
			if !isToday {
				_ = WriteFileAtomic(cacheFile, csvBytes, 0644)
			}
			return closed, nil
		}
	}

	return nil, fmt.Errorf("no .csv file found inside %s", zipName)
}

func (d *Downloader) fetchFromLiveAPI(
	ctx context.Context,
	symbol, interval string,
	date time.Time,
	cacheFile string,
	isToday bool,
	nowUTC time.Time,
) ([]HistoricalCandle, error) {
	startMs := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
	endMs := startMs + (24*time.Hour - time.Millisecond).Milliseconds()

	apiURL := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=1500",
		symbol, interval, startMs, endMs)

	body, _, err := d.httpClient.GetREST(ctx, apiURL, 10, false)
	if err != nil {
		return nil, err
	}

	var rawKlines [][]interface{}
	dec := json.NewDecoder(bytes.NewReader(body))
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
	dur := parseDurationWithFallback(interval)
	parsed, pErr := ParseBinanceKlineCSV(bytes.NewReader(csvBytes), symbol, dur)
	if pErr != nil {
		return nil, pErr
	}
	closed := FilterClosedCandles(parsed, nowUTC)
	if len(closed) > 0 && !isToday {
		_ = WriteFileAtomic(cacheFile, csvBytes, 0644)
	} else if isToday && len(closed) > 0 {
		// Rewrite filtered closed-only bars for today's short-TTL cache
		_ = WriteFileAtomic(cacheFile, formatCandlesCSV(closed), 0644)
	}
	return closed, nil
}

func formatCandlesCSV(candles []HistoricalCandle) []byte {
	var sb strings.Builder
	sb.WriteString("open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n")
	for _, c := range candles {
		fmt.Fprintf(&sb, "%d,%g,%g,%g,%g,%g,%d,%g,%d,%g,%g,0\n",
			c.OpenTime.UnixMilli(), c.Open, c.High, c.Low, c.Close, c.Volume,
			c.CloseTime.UnixMilli(), c.QuoteVolume, c.TradeCount, c.TakerBuyBaseVolume, c.TakerBuyQuoteVolume)
	}
	return []byte(sb.String())
}
