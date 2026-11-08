package data

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HistoricalFundingRate menyimpan data tingkat pendanaan resmi dari Binance.
type HistoricalFundingRate struct {
	FundingTime   time.Time
	FundingRate   float64
	IntervalHours int
}

// FundingManager bertugas mengunduh, menyimpan cache, dan melayani data historical funding rate.
type FundingManager struct {
	cacheDir string
	client   *http.Client
	rates    map[string][]HistoricalFundingRate // symbol -> sorted rates
}

func NewFundingManager(cacheDir string) *FundingManager {
	if cacheDir == "" {
		cacheDir = filepath.Join("data", "backtest", "cache")
	}
	return &FundingManager{
		cacheDir: cacheDir,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		rates: make(map[string][]HistoricalFundingRate),
	}
}

// LoadFundingRates memuat funding rates untuk sekumpulan simbol dalam rentang tanggal [from..to] secara paralel.
func (fm *FundingManager) LoadFundingRates(ctx context.Context, symbols []string, from, to time.Time) error {
	total := len(symbols)
	if total == 0 {
		return nil
	}

	taskChan := make(chan string, total)
	for _, sym := range symbols {
		taskChan <- sym
	}
	close(taskChan)

	numWorkers := 16
	if total < numWorkers {
		numWorkers = total
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sym := range taskChan {
				rates, err := fm.fetchSymbolFunding(ctx, sym, from, to)
				if err != nil {
					slog.Debug("Gagal memuat funding rate historis simbol, fallback default", "symbol", sym, "err", err)
					continue
				}
				mu.Lock()
				fm.rates[sym] = rates
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return nil
}

// GetFundingRate mengembalikan funding rate yang aktif pada waktu simTime untuk simbol tertentu.
func (fm *FundingManager) GetFundingRate(symbol string, simTime time.Time) float64 {
	list, ok := fm.rates[symbol]
	if !ok || len(list) == 0 {
		return 0.0001 // Fallback jika data historis tidak tersedia (0.01% baseline)
	}

	// Binary search mencari funding rate terakhir sebelum atau tepat pada simTime
	idx := sort.Search(len(list), func(i int) bool {
		return list[i].FundingTime.After(simTime)
	})

	if idx == 0 {
		return list[0].FundingRate
	}
	return list[idx-1].FundingRate
}

// CheckSettlement memeriksa apakah pada bar candlestick [openTime, closeTime] terjadi settlement pendanaan bursa.
// Mengembalikan nilai funding rate dan true jika bar ini adalah bar settlement.
func (fm *FundingManager) CheckSettlement(symbol string, openTime, closeTime time.Time) (float64, bool) {
	list, ok := fm.rates[symbol]
	if !ok || len(list) == 0 {
		// Fallback settlement jika tidak ada data: periksa jam 00, 08, 16 UTC tepat pada openTime
		if openTime.Minute() == 0 && (openTime.Hour()%8 == 0) {
			return 0.0001, true
		}
		return 0, false
	}

	openSec := openTime.Unix()
	closeSec := closeTime.Unix()

	// Cari apakah ada funding event di dalam rentang detik bar ini
	for _, f := range list {
		fSec := f.FundingTime.Unix()
		if fSec >= openSec && fSec <= closeSec {
			return f.FundingRate, true
		}
		if fSec > closeSec+300 {
			break
		}
	}

	return 0, false
}

// SetRatesForTesting memungkinkan penyuntikan data untuk keperluan unit testing.
func (fm *FundingManager) SetRatesForTesting(symbol string, rates []HistoricalFundingRate) {
	fm.rates[symbol] = rates
}

func (fm *FundingManager) fetchSymbolFunding(ctx context.Context, symbol string, from, to time.Time) ([]HistoricalFundingRate, error) {
	fundingDir := filepath.Join(fm.cacheDir, symbol, "funding")
	if err := os.MkdirAll(fundingDir, 0755); err != nil {
		return nil, fmt.Errorf("create funding dir: %w", err)
	}

	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)

	var allRates []HistoricalFundingRate

	for !cur.After(endMonth) {
		year := cur.Year()
		month := int(cur.Month())

		rates, err := fm.fetchMonthlyFunding(ctx, symbol, year, month, fundingDir)
		if err != nil {
			// Fallback ke REST API jika arsip bulanan Binance Vision belum terbit (misal bulan berjalan / hari ini)
			rates, err = fm.fetchLiveAPIFunding(ctx, symbol, cur, fundingDir)
			if err != nil {
				slog.Debug("fallback live funding failed", "symbol", symbol, "year", year, "month", month, "err", err)
			}
		}

		if len(rates) > 0 {
			allRates = append(allRates, rates...)
		}

		cur = cur.AddDate(0, 1, 0)
	}

	if len(allRates) == 0 {
		// Percobaan terakhir: fetch langsung via live API untuk rentang tanggal
		rates, err := fm.fetchLiveAPIFundingRange(ctx, symbol, from, to)
		if err == nil && len(rates) > 0 {
			allRates = rates
		}
	}

	// Urutkan berdasarkan waktu
	sort.Slice(allRates, func(i, j int) bool {
		return allRates[i].FundingTime.Before(allRates[j].FundingTime)
	})

	// Deduplikasi
	deduped := make([]HistoricalFundingRate, 0, len(allRates))
	seen := make(map[int64]bool)
	for _, r := range allRates {
		sec := r.FundingTime.Unix()
		if !seen[sec] {
			seen[sec] = true
			deduped = append(deduped, r)
		}
	}

	return deduped, nil
}

func (fm *FundingManager) fetchMonthlyFunding(
	ctx context.Context,
	symbol string,
	year, month int,
	fundingDir string,
) ([]HistoricalFundingRate, error) {
	cacheFile := filepath.Join(fundingDir, fmt.Sprintf("%s-fundingRate-%04d-%02d.csv", symbol, year, month))

	// 1. Cek disk cache
	if fileInfo, err := os.Stat(cacheFile); err == nil && fileInfo.Size() > 0 {
		isCurrentMonth := year == time.Now().UTC().Year() && time.Month(month) == time.Now().UTC().Month()
		// Jika bulan lampau, cache permanen berlaku. Jika bulan berjalan, gunakan cache jika masih fresh (< 4 jam).
		if !isCurrentMonth || time.Since(fileInfo.ModTime()) < 4*time.Hour {
			if fileBytes, err := os.ReadFile(cacheFile); err == nil && len(fileBytes) > 0 {
				return parseFundingCSV(bytes.NewReader(fileBytes))
			}
		}
	}

	// 2. Unduh zip dari Binance Vision
	zipName := fmt.Sprintf("%s-fundingRate-%04d-%02d.zip", symbol, year, month)
	url := fmt.Sprintf("https://data.binance.vision/data/futures/um/monthly/fundingRate/%s/%s", symbol, zipName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := fm.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance vision funding zip not found: HTTP %d", resp.StatusCode)
	}

	zipBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, err
	}

	for _, f := range zipReader.File {
		if strings.HasSuffix(f.Name, ".csv") {
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			csvBytes, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return nil, err
			}

			_ = os.WriteFile(cacheFile, csvBytes, 0644)
			return parseFundingCSV(bytes.NewReader(csvBytes))
		}
	}

	return nil, fmt.Errorf("no csv inside funding zip")
}

func (fm *FundingManager) fetchLiveAPIFunding(
	ctx context.Context,
	symbol string,
	monthTime time.Time,
	fundingDir string,
) ([]HistoricalFundingRate, error) {
	startMs := time.Date(monthTime.Year(), monthTime.Month(), 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	endMs := time.Date(monthTime.Year(), monthTime.Month()+1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1

	nowMs := time.Now().UTC().UnixMilli()
	if endMs > nowMs {
		endMs = nowMs
	}

	cacheFile := filepath.Join(fundingDir, fmt.Sprintf("%s-fundingRate-%04d-%02d.csv", symbol, monthTime.Year(), int(monthTime.Month())))
	rates, err := fm.queryBinanceFundingAPI(ctx, symbol, startMs, endMs)
	if err != nil {
		return nil, err
	}

	if len(rates) > 0 {
		// Simpan cache ke disk agar tidak menembak REST API berulang-ulang
		fm.saveRatesToCSV(rates, cacheFile)
	}

	return rates, nil
}

func (fm *FundingManager) fetchLiveAPIFundingRange(
	ctx context.Context,
	symbol string,
	from, to time.Time,
) ([]HistoricalFundingRate, error) {
	startMs := from.UnixMilli()
	endMs := to.UnixMilli()
	return fm.queryBinanceFundingAPI(ctx, symbol, startMs, endMs)
}

func (fm *FundingManager) queryBinanceFundingAPI(
	ctx context.Context,
	symbol string,
	startMs, endMs int64,
) ([]HistoricalFundingRate, error) {
	apiURL := fmt.Sprintf("https://fapi.binance.com/fapi/v1/fundingRate?symbol=%s&startTime=%d&endTime=%d&limit=1000",
		symbol, startMs, endMs)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := fm.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance api funding status %d", resp.StatusCode)
	}

	var rawList []struct {
		Symbol      string `json:"symbol"`
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawList); err != nil {
		return nil, err
	}

	res := make([]HistoricalFundingRate, 0, len(rawList))
	for _, item := range rawList {
		rate, _ := strconv.ParseFloat(item.FundingRate, 64)
		fTime := time.UnixMilli(item.FundingTime).UTC()
		res = append(res, HistoricalFundingRate{
			FundingTime:   fTime,
			FundingRate:   rate,
			IntervalHours: 8,
		})
	}

	return res, nil
}

func (fm *FundingManager) saveRatesToCSV(rates []HistoricalFundingRate, path string) {
	var sb strings.Builder
	sb.WriteString("calc_time,funding_interval_hours,last_funding_rate\n")
	for _, r := range rates {
		fmt.Fprintf(&sb, "%d,%d,%.8f\n", r.FundingTime.UnixMilli(), r.IntervalHours, r.FundingRate)
	}
	_ = os.WriteFile(path, []byte(sb.String()), 0644)
}

func parseFundingCSV(r io.Reader) ([]HistoricalFundingRate, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read funding csv: %w", err)
	}

	var res []HistoricalFundingRate
	for i, row := range rows {
		if len(row) < 3 {
			continue
		}
		// Skip header
		if i == 0 && (strings.Contains(row[0], "calc_time") || strings.Contains(row[0], "time")) {
			continue
		}

		ms, err := strconv.ParseInt(strings.TrimSpace(row[0]), 10, 64)
		if err != nil {
			continue
		}
		interval, _ := strconv.Atoi(strings.TrimSpace(row[1]))
		rate, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil {
			continue
		}

		fTime := time.UnixMilli(ms).UTC()
		res = append(res, HistoricalFundingRate{
			FundingTime:   fTime,
			FundingRate:   rate,
			IntervalHours: interval,
		})
	}

	return res, nil
}

