package data

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OpenInterestPoint holds a 5-minute Binance Futures USD-M metrics snapshot.
type OpenInterestPoint struct {
	Symbol               string
	Timestamp            time.Time
	SumOpenInterest      float64
	SumOpenInterestValue float64
}

// MetricsOIDownloader fetches, caches (.bin + CSV), and performs strict point-in-time OI lookup.
type MetricsOIDownloader struct {
	dirs       BacktestDirs
	baseURL    string
	httpClient *BacktestHTTPClient
	offline    bool
	store      *BacktestStore

	mu     sync.RWMutex
	series map[string][]OpenInterestPoint
}

func NewMetricsOIDownloader(cacheDir string, httpClient *BacktestHTTPClient, store *BacktestStore) *MetricsOIDownloader {
	if httpClient == nil {
		httpClient = NewBacktestHTTPClient(BacktestHTTPConfig{})
	}
	return &MetricsOIDownloader{
		dirs:       ResolveBacktestDirs(cacheDir),
		baseURL:    "https://data.binance.vision/data/futures/um",
		httpClient: httpClient,
		store:      store,
		series:     make(map[string][]OpenInterestPoint),
	}
}

func (m *MetricsOIDownloader) SetOffline(offline bool) {
	m.offline = offline
}

func (m *MetricsOIDownloader) SetSeriesForTesting(symbol string, pts []OpenInterestPoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]OpenInterestPoint, len(pts))
	copy(copied, pts)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].Timestamp.Before(copied[j].Timestamp)
	})
	m.series[symbol] = copied
}

// LoadOpenInterestRange loads 5m metrics OI across [from..to] for each symbol concurrently.
func (m *MetricsOIDownloader) LoadOpenInterestRange(ctx context.Context, symbols []string, from, to time.Time) error {
	if len(symbols) == 0 {
		return nil
	}
	taskCh := make(chan string, len(symbols))
	for _, s := range symbols {
		taskCh <- s
	}
	close(taskCh)

	workers := 8
	if len(symbols) < workers {
		workers = len(symbols)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sym := range taskCh {
				if ctx.Err() != nil {
					return
				}
				pts, err := m.FetchOpenInterestRange(ctx, sym, from, to)
				if err == nil && len(pts) > 0 {
					m.mu.Lock()
					m.series[sym] = pts
					m.mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// FetchOpenInterestRange returns sorted, deduplicated 5m OI snapshots strictly bounded within [from..to].
func (m *MetricsOIDownloader) FetchOpenInterestRange(ctx context.Context, symbol string, from, to time.Time) ([]OpenInterestPoint, error) {
	if from.After(to) {
		return nil, fmt.Errorf("invalid OI range: from %s after to %s", from, to)
	}
	nowUTC := time.Now().UTC()
	startDay := time.Date(from.UTC().Year(), from.UTC().Month(), from.UTC().Day(), 0, 0, 0, 0, time.UTC)
	endDay := time.Date(to.UTC().Year(), to.UTC().Month(), to.UTC().Day(), 0, 0, 0, 0, time.UTC)

	var allPts []OpenInterestPoint
	monthlyCompiled := make(map[string]bool)

	for day := startDay; !day.After(endDay); day = day.AddDate(0, 0, 1) {
		ym := day.Format("2006-01")
		monthBin := filepath.Join(m.dirs.StoreDir, symbol, "oi", ym+".bin")
		if !isCurrentOrFutureMonth(ym, nowUTC) && !monthlyCompiled[ym] {
			if pts, err := ReadCompactOIBin(monthBin, symbol); err == nil && len(pts) > 0 {
				allPts = append(allPts, pts...)
				monthlyCompiled[ym] = true
				continue
			}
		}
		if monthlyCompiled[ym] {
			continue
		}

		pts, err := m.fetchDailyMetrics(ctx, symbol, day, nowUTC)
		if err == nil && len(pts) > 0 {
			allPts = append(allPts, pts...)
		}
	}

	sort.Slice(allPts, func(i, j int) bool {
		return allPts[i].Timestamp.Before(allPts[j].Timestamp)
	})

	out := make([]OpenInterestPoint, 0, len(allPts))
	seen := make(map[int64]bool, len(allPts))
	for _, pt := range allPts {
		if pt.Timestamp.Before(from) || pt.Timestamp.After(to) {
			continue
		}
		ms := pt.Timestamp.UnixMilli()
		if seen[ms] {
			continue
		}
		seen[ms] = true
		out = append(out, pt)
	}

	// Only compact finished full calendar months so partial range queries never poison YYYY-MM.bin.
	m.compactFinishedMonths(ctx, symbol, out, from, to, nowUTC)
	return out, nil
}

func (m *MetricsOIDownloader) compactFinishedMonths(ctx context.Context, symbol string, pts []OpenInterestPoint, reqFrom, reqTo, nowUTC time.Time) {
	if len(pts) == 0 {
		return
	}
	byMonth := make(map[string][]OpenInterestPoint)
	for _, pt := range pts {
		ym := pt.Timestamp.UTC().Format("2006-01")
		if isCurrentOrFutureMonth(ym, nowUTC) {
			continue
		}
		byMonth[ym] = append(byMonth[ym], pt)
	}
	for ym, list := range byMonth {
		mStart, err := time.Parse("2006-01", ym)
		if err != nil {
			continue
		}
		mEnd := mStart.AddDate(0, 1, 0).Add(-time.Minute)
		// Require full month request coverage [00:00 day 1 .. 23:55 last day] before sealing into YYYY-MM.bin
		if reqFrom.After(mStart) || reqTo.Before(mEnd) {
			continue
		}
		binPath := filepath.Join(m.dirs.StoreDir, symbol, "oi", ym+".bin")
		if _, err := os.Stat(binPath); err == nil {
			continue
		}
		if err := WriteCompactOIBin(binPath, list); err == nil && m.store != nil {
			_ = m.store.UpsertCacheManifest(ctx, fmt.Sprintf("oi:%s:%s", symbol, ym), len(list), list[0].Timestamp.UnixMilli(), list[len(list)-1].Timestamp.UnixMilli(), false)
		}
	}
}

func (m *MetricsOIDownloader) fetchDailyMetrics(ctx context.Context, symbol string, day time.Time, nowUTC time.Time) ([]OpenInterestPoint, error) {
	dateStr := day.Format("2006-01-02")
	dayBin := filepath.Join(m.dirs.StoreDir, symbol, "oi", "daily", dateStr+".bin")
	legacyCSV := filepath.Join(m.dirs.LegacyCSV, symbol, "metrics", fmt.Sprintf("%s-metrics-%s.csv", symbol, dateStr))

	isToday := day.Year() == nowUTC.Year() && day.Month() == nowUTC.Month() && day.Day() == nowUTC.Day()
	if !isToday || m.offline {
		if pts, err := ReadCompactOIBin(dayBin, symbol); err == nil && len(pts) > 0 {
			return pts, nil
		}
		if raw, err := os.ReadFile(legacyCSV); err == nil && len(raw) > 0 {
			if pts, pErr := ParseBinanceMetricsCSV(bytes.NewReader(raw), symbol); pErr == nil && len(pts) > 0 {
				_ = WriteCompactOIBin(dayBin, pts)
				return pts, nil
			}
		}
	}
	if m.offline {
		return nil, fmt.Errorf("offline: metrics OI %s %s not cached", symbol, dateStr)
	}

	zipName := fmt.Sprintf("%s-metrics-%s.zip", symbol, dateStr)
	rawURL := fmt.Sprintf("%s/daily/metrics/%s/%s", m.baseURL, symbol, zipName)
	zipBytes, status, err := m.httpClient.GetVision(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		return nil, fmt.Errorf("metrics OI 404 for %s %s", symbol, dateStr)
	}

	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("unzip metrics %s: %w", rawURL, err)
	}
	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".csv") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		csvBytes, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		pts, err := ParseBinanceMetricsCSV(bytes.NewReader(csvBytes), symbol)
		if err != nil {
			return nil, err
		}
		if !isToday && len(pts) > 0 {
			_ = WriteCompactOIBin(dayBin, pts)
		}
		return pts, nil
	}
	return nil, fmt.Errorf("no csv inside metrics zip %s", zipName)
}

// ParseBinanceMetricsCSV parses Binance Vision daily/metrics CSV (`create_time,symbol,sum_open_interest,sum_open_interest_value,...`).
func ParseBinanceMetricsCSV(r io.Reader, symbol string) ([]OpenInterestPoint, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1

	var out []OpenInterestPoint
	first := true
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse metrics csv: %w", err)
		}
		if len(rec) < 4 {
			continue
		}
		if first {
			first = false
			if strings.Contains(strings.ToLower(rec[0]), "create_time") {
				continue
			}
		}
		ts, ok := parseMetricsTimestamp(rec[0])
		if !ok {
			continue
		}
		oi, errOI := strconv.ParseFloat(strings.TrimSpace(rec[2]), 64)
		oiVal, errVal := strconv.ParseFloat(strings.TrimSpace(rec[3]), 64)
		if errOI != nil || errVal != nil || math.IsNaN(oi) || math.IsInf(oi, 0) || oi <= 0 {
			continue
		}
		if math.IsNaN(oiVal) || math.IsInf(oiVal, 0) || oiVal < 0 {
			oiVal = 0
		}
		out = append(out, OpenInterestPoint{
			Symbol:               symbol,
			Timestamp:            ts,
			SumOpenInterest:      oi,
			SumOpenInterestValue: oiVal,
		})
	}
	return out, nil
}

func parseMetricsTimestamp(raw string) (time.Time, bool) {
	s := strings.TrimSpace(raw)
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		if ms > 1_000_000_000_000 {
			return time.UnixMilli(ms).UTC(), true
		}
		return time.Unix(ms, 0).UTC(), true
	}
	for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// LookupOpenInterestStrict resolves point-in-time (PrevOI, CurrOI) strictly with Timestamp <= simTime.
// Guarantees zero look-ahead and enforces staleness/interval constraints matching EvaluatorConfig.
func (m *MetricsOIDownloader) LookupOpenInterestStrict(
	symbol string,
	simTime time.Time,
	minInterval, maxInterval, maxStaleness time.Duration,
) (prevOI, currOI float64, prevTime, currTime time.Time, ok bool) {
	m.mu.RLock()
	pts := m.series[symbol]
	m.mu.RUnlock()

	if len(pts) < 2 {
		return 0, 0, time.Time{}, time.Time{}, false
	}

	// First index strictly AFTER simTime -> idx-1 is the latest point <= simTime.
	idx := sort.Search(len(pts), func(i int) bool {
		return pts[i].Timestamp.After(simTime)
	})
	currIdx := idx - 1
	if currIdx <= 0 {
		return 0, 0, time.Time{}, time.Time{}, false
	}
	curr := pts[currIdx]
	if maxStaleness > 0 && simTime.Sub(curr.Timestamp) > maxStaleness {
		return 0, 0, time.Time{}, time.Time{}, false
	}

	// Search backwards for an earlier sample matching [minInterval, maxInterval] before curr.Timestamp.
	for j := currIdx - 1; j >= 0; j-- {
		diff := curr.Timestamp.Sub(pts[j].Timestamp)
		if minInterval > 0 && diff < minInterval {
			continue
		}
		if maxInterval > 0 && diff > maxInterval {
			break
		}
		return pts[j].SumOpenInterest, curr.SumOpenInterest, pts[j].Timestamp, curr.Timestamp, true
	}
	return 0, 0, time.Time{}, time.Time{}, false
}
