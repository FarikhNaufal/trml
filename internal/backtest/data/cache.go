package data

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	barBinMagic     = "TRMLBAR1"
	oiBinMagic      = "TRMLOI01"
	fundingBinMagic = "TRMLFND1"
	binHeaderSize   = 16 // 8B magic + 4B uint32 count + 4B uint32 crc32
	barRecordSize   = 88 // 3*int64 + 8*float64
	oiRecordSize    = 24 // int64 + 2*float64
	fundRecordSize  = 24 // int64 + float64 + int64
)

// BacktestDirs organizes isolated storage paths for backtest cache, binary store, and SQLite DB.
type BacktestDirs struct {
	BaseDir   string
	LegacyCSV string
	StoreDir  string
	DBPath    string
}

// ResolveBacktestDirs derives clean directories from a cacheDir flag without mixing into live bot data.
func ResolveBacktestDirs(cacheDir string) BacktestDirs {
	if strings.TrimSpace(cacheDir) == "" {
		cacheDir = filepath.Join("data", "backtest", "cache")
	}
	clean := filepath.Clean(cacheDir)
	base := clean
	if filepath.Base(clean) == "cache" {
		base = filepath.Dir(clean)
	}
	return BacktestDirs{
		BaseDir:   base,
		LegacyCSV: clean,
		StoreDir:  filepath.Join(base, "store"),
		DBPath:    filepath.Join(base, "backtest.db"),
	}
}

// WriteFileAtomic safely writes bytes via temp file + sync + rename to prevent corrupt disk caches on Ctrl+C/crash.
func WriteFileAtomic(targetPath string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmpFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp in %s: %w", dir, err)
	}
	tmpName := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("write temp %s: %w", tmpName, err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("sync temp %s: %w", tmpName, err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp %s: %w", tmpName, err)
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return fmt.Errorf("chmod temp %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpName, targetPath, err)
	}
	cleanup = false
	return nil
}

// FilterClosedCandles rejects malformed, NaN/Inf, or unclosed future candles relative to referenceNow.
func FilterClosedCandles(candles []HistoricalCandle, referenceNow time.Time) []HistoricalCandle {
	out := make([]HistoricalCandle, 0, len(candles))
	for _, c := range candles {
		if c.OpenTime.IsZero() || c.CloseTime.IsZero() {
			continue
		}
		if !referenceNow.IsZero() && c.CloseTime.After(referenceNow) {
			continue
		}
		if math.IsNaN(c.Open) || math.IsNaN(c.High) || math.IsNaN(c.Low) || math.IsNaN(c.Close) ||
			math.IsInf(c.Open, 0) || math.IsInf(c.High, 0) || math.IsInf(c.Low, 0) || math.IsInf(c.Close, 0) {
			continue
		}
		if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 || c.Low > c.High {
			continue
		}
		if c.High < math.Max(c.Open, c.Close) || c.Low > math.Min(c.Open, c.Close) {
			continue
		}
		if math.IsNaN(c.Volume) || math.IsInf(c.Volume, 0) || c.Volume < 0 ||
			math.IsNaN(c.QuoteVolume) || math.IsInf(c.QuoteVolume, 0) || c.QuoteVolume < 0 ||
			math.IsNaN(c.TakerBuyQuoteVolume) || math.IsInf(c.TakerBuyQuoteVolume, 0) || c.TakerBuyQuoteVolume < 0 {
			continue
		}
		out = append(out, c)
	}
	return out
}

func WriteCompactBarsBin(path string, bars []HistoricalCandle) error {
	payload := make([]byte, len(bars)*barRecordSize)
	for i, c := range bars {
		off := i * barRecordSize
		binary.LittleEndian.PutUint64(payload[off+0:], uint64(c.OpenTime.UTC().UnixMilli()))
		binary.LittleEndian.PutUint64(payload[off+8:], uint64(c.CloseTime.UTC().UnixMilli()))
		binary.LittleEndian.PutUint64(payload[off+16:], uint64(c.TradeCount))
		binary.LittleEndian.PutUint64(payload[off+24:], math.Float64bits(c.Open))
		binary.LittleEndian.PutUint64(payload[off+32:], math.Float64bits(c.High))
		binary.LittleEndian.PutUint64(payload[off+40:], math.Float64bits(c.Low))
		binary.LittleEndian.PutUint64(payload[off+48:], math.Float64bits(c.Close))
		binary.LittleEndian.PutUint64(payload[off+56:], math.Float64bits(c.Volume))
		binary.LittleEndian.PutUint64(payload[off+64:], math.Float64bits(c.QuoteVolume))
		binary.LittleEndian.PutUint64(payload[off+72:], math.Float64bits(c.TakerBuyBaseVolume))
		binary.LittleEndian.PutUint64(payload[off+80:], math.Float64bits(c.TakerBuyQuoteVolume))
	}
	return writeBinaryWithHeader(path, barBinMagic, len(bars), payload)
}

func ReadCompactBarsBin(path, symbol string) ([]HistoricalCandle, error) {
	count, payload, err := readBinaryWithHeader(path, barBinMagic, barRecordSize)
	if err != nil {
		return nil, err
	}
	bars := make([]HistoricalCandle, count)
	for i := 0; i < count; i++ {
		off := i * barRecordSize
		openMs := int64(binary.LittleEndian.Uint64(payload[off+0:]))
		closeMs := int64(binary.LittleEndian.Uint64(payload[off+8:]))
		tradeCount := int64(binary.LittleEndian.Uint64(payload[off+16:]))
		openP := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+24:]))
		highP := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+32:]))
		lowP := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+40:]))
		closeP := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+48:]))
		vol := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+56:]))
		qVol := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+64:]))
		tbBase := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+72:]))
		tbQuote := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+80:]))

		bars[i] = HistoricalCandle{
			Symbol:              symbol,
			OpenTime:            time.UnixMilli(openMs).UTC(),
			CloseTime:           time.UnixMilli(closeMs).UTC(),
			Open:                openP,
			High:                highP,
			Low:                 lowP,
			Close:               closeP,
			Volume:              vol,
			QuoteVolume:         qVol,
			TradeCount:          tradeCount,
			TakerBuyBaseVolume:  tbBase,
			TakerBuyQuoteVolume: tbQuote,
			BarDeltaUSDT:        2.0*tbQuote - qVol,
		}
	}
	return bars, nil
}

func WriteCompactOIBin(path string, points []OpenInterestPoint) error {
	payload := make([]byte, len(points)*oiRecordSize)
	for i, pt := range points {
		off := i * oiRecordSize
		binary.LittleEndian.PutUint64(payload[off+0:], uint64(pt.Timestamp.UTC().UnixMilli()))
		binary.LittleEndian.PutUint64(payload[off+8:], math.Float64bits(pt.SumOpenInterest))
		binary.LittleEndian.PutUint64(payload[off+16:], math.Float64bits(pt.SumOpenInterestValue))
	}
	return writeBinaryWithHeader(path, oiBinMagic, len(points), payload)
}

func ReadCompactOIBin(path, symbol string) ([]OpenInterestPoint, error) {
	count, payload, err := readBinaryWithHeader(path, oiBinMagic, oiRecordSize)
	if err != nil {
		return nil, err
	}
	points := make([]OpenInterestPoint, count)
	for i := 0; i < count; i++ {
		off := i * oiRecordSize
		ms := int64(binary.LittleEndian.Uint64(payload[off+0:]))
		oi := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+8:]))
		val := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+16:]))
		points[i] = OpenInterestPoint{
			Symbol:               symbol,
			Timestamp:            time.UnixMilli(ms).UTC(),
			SumOpenInterest:      oi,
			SumOpenInterestValue: val,
		}
	}
	return points, nil
}

func WriteCompactFundingBin(path string, rates []HistoricalFundingRate) error {
	payload := make([]byte, len(rates)*fundRecordSize)
	for i, r := range rates {
		off := i * fundRecordSize
		binary.LittleEndian.PutUint64(payload[off+0:], uint64(r.FundingTime.UTC().UnixMilli()))
		binary.LittleEndian.PutUint64(payload[off+8:], math.Float64bits(r.FundingRate))
		binary.LittleEndian.PutUint64(payload[off+16:], uint64(r.IntervalHours))
	}
	return writeBinaryWithHeader(path, fundingBinMagic, len(rates), payload)
}

func ReadCompactFundingBin(path string) ([]HistoricalFundingRate, error) {
	count, payload, err := readBinaryWithHeader(path, fundingBinMagic, fundRecordSize)
	if err != nil {
		return nil, err
	}
	rates := make([]HistoricalFundingRate, count)
	for i := 0; i < count; i++ {
		off := i * fundRecordSize
		ms := int64(binary.LittleEndian.Uint64(payload[off+0:]))
		rate := math.Float64frombits(binary.LittleEndian.Uint64(payload[off+8:]))
		interval := int(binary.LittleEndian.Uint64(payload[off+16:]))
		rates[i] = HistoricalFundingRate{
			FundingTime:   time.UnixMilli(ms).UTC(),
			FundingRate:   rate,
			IntervalHours: interval,
		}
	}
	return rates, nil
}

func writeBinaryWithHeader(path, magic string, count int, payload []byte) error {
	buf := make([]byte, binHeaderSize+len(payload))
	copy(buf[:8], magic)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(count))
	binary.LittleEndian.PutUint32(buf[12:16], crc32.ChecksumIEEE(payload))
	copy(buf[16:], payload)
	return WriteFileAtomic(path, buf, 0644)
}

func readBinaryWithHeader(path, expectedMagic string, recordSize int) (int, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}
	if len(raw) < binHeaderSize {
		return 0, nil, fmt.Errorf("corrupt binary cache %s: header too short (%d bytes)", path, len(raw))
	}
	if string(raw[:8]) != expectedMagic {
		return 0, nil, fmt.Errorf("corrupt binary cache %s: invalid magic %q", path, string(raw[:8]))
	}
	count := int(binary.LittleEndian.Uint32(raw[8:12]))
	expectedCRC := binary.LittleEndian.Uint32(raw[12:16])
	payload := raw[binHeaderSize:]
	if len(payload) != count*recordSize {
		return 0, nil, fmt.Errorf("corrupt binary cache %s: expected %d bytes payload, got %d", path, count*recordSize, len(payload))
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return 0, nil, fmt.Errorf("corrupt binary cache %s: CRC32 mismatch", path)
	}
	return count, payload, nil
}

// ImportStats summarizes migration of legacy CSV files in data/backtest/cache into binary store & backtest.db.
type ImportStats struct {
	SymbolsScanned int
	KlineFiles     int
	KlineBars      int
	FundingFiles   int
	OIFiles        int
	SkippedPartial int
}

// ImportLegacyCSVCache imports existing monthly/daily CSVs without making network requests.
// Closed historical monthly CSVs are compiled into fast .bin files and indexed in backtest.db if provided.
func ImportLegacyCSVCache(
	ctx context.Context,
	dirs BacktestDirs,
	sqliteStore *BacktestStore,
	referenceNow time.Time,
) (ImportStats, error) {
	var stats ImportStats
	if referenceNow.IsZero() {
		referenceNow = time.Now().UTC()
	}

	entries, err := os.ReadDir(dirs.LegacyCSV)
	if err != nil {
		if os.IsNotExist(err) {
			return stats, nil
		}
		return stats, fmt.Errorf("read legacy cache dir %s: %w", dirs.LegacyCSV, err)
	}

	for _, ent := range entries {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if !ent.IsDir() {
			continue
		}
		sym := strings.ToUpper(ent.Name())
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		stats.SymbolsScanned++
		symLegacyDir := filepath.Join(dirs.LegacyCSV, ent.Name())
		subEntries, err := os.ReadDir(symLegacyDir)
		if err != nil {
			continue
		}

		for _, sub := range subEntries {
			if !sub.IsDir() {
				continue
			}
			kind := sub.Name()
			kindDir := filepath.Join(symLegacyDir, kind)
			files, err := os.ReadDir(kindDir)
			if err != nil {
				continue
			}

			switch kind {
			case "funding":
				for _, f := range files {
					if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
						continue
					}
					ym, ok := extractYearMonthFromFilename(f.Name())
					if !ok || isCurrentOrFutureMonth(ym, referenceNow) {
						stats.SkippedPartial++
						continue
					}
					raw, err := os.ReadFile(filepath.Join(kindDir, f.Name()))
					if err != nil || len(raw) == 0 {
						continue
					}
					rates, err := parseFundingCSV(bytes.NewReader(raw))
					if err != nil || len(rates) == 0 {
						continue
					}
					binPath := filepath.Join(dirs.StoreDir, sym, "funding", ym+".bin")
					if err := WriteCompactFundingBin(binPath, rates); err == nil {
						stats.FundingFiles++
						if sqliteStore != nil {
							startMs := rates[0].FundingTime.UnixMilli()
							endMs := rates[len(rates)-1].FundingTime.UnixMilli()
							_ = sqliteStore.UpsertCacheManifest(ctx, fmt.Sprintf("funding:%s:%s", sym, ym), len(rates), startMs, endMs, false)
						}
					}
				}
			default:
				dur, tfErr := parseIntervalDurationStrict(kind)
				if tfErr != nil {
					continue
				}
				monthlyBuckets := make(map[string][]HistoricalCandle)
				for _, f := range files {
					if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
						continue
					}
					ym, ok := extractYearMonthFromFilename(f.Name())
					if !ok || isCurrentOrFutureMonth(ym, referenceNow) {
						stats.SkippedPartial++
						continue
					}
					raw, err := os.ReadFile(filepath.Join(kindDir, f.Name()))
					if err != nil || len(raw) == 0 {
						continue
					}
					parsed, err := ParseBinanceKlineCSV(bytes.NewReader(raw), sym, dur)
					if err != nil || len(parsed) == 0 {
						continue
					}
					closed := FilterClosedCandles(parsed, referenceNow)
					if len(closed) > 0 {
						monthlyBuckets[ym] = append(monthlyBuckets[ym], closed...)
					}
				}
				for ym, bars := range monthlyBuckets {
					deduped := sortAndDedupeBars(bars)
					if len(deduped) == 0 {
						continue
					}
					binPath := filepath.Join(dirs.StoreDir, sym, kind, ym+".bin")
					if err := WriteCompactBarsBin(binPath, deduped); err == nil {
						stats.KlineFiles++
						stats.KlineBars += len(deduped)
						if sqliteStore != nil {
							startMs := deduped[0].OpenTime.UnixMilli()
							endMs := deduped[len(deduped)-1].CloseTime.UnixMilli()
							_ = sqliteStore.UpsertCacheManifest(ctx, fmt.Sprintf("kline:%s:%s:%s", sym, kind, ym), len(deduped), startMs, endMs, false)
						}
					}
				}
			}
		}
	}

	return stats, nil
}

func sortAndDedupeBars(bars []HistoricalCandle) []HistoricalCandle {
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].OpenTime.Before(bars[j].OpenTime)
	})
	out := make([]HistoricalCandle, 0, len(bars))
	var lastMs int64 = -1
	for _, b := range bars {
		ms := b.OpenTime.UnixMilli()
		if ms == lastMs {
			continue
		}
		lastMs = ms
		out = append(out, b)
	}
	return out
}

func extractYearMonthFromFilename(name string) (string, bool) {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	parts := strings.Split(base, "-")
	for i := 0; i < len(parts)-1; i++ {
		if len(parts[i]) == 4 && len(parts[i+1]) == 2 {
			if _, err := time.Parse("2006-01", parts[i]+"-"+parts[i+1]); err == nil {
				return parts[i] + "-" + parts[i+1], true
			}
		}
	}
	return "", false
}

func isCurrentOrFutureMonth(ym string, ref time.Time) bool {
	t, err := time.Parse("2006-01", ym)
	if err != nil {
		return false
	}
	curMonth := time.Date(ref.UTC().Year(), ref.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	return !t.Before(curMonth)
}

func parseIntervalDurationStrict(interval string) (time.Duration, error) {
	switch strings.ToLower(strings.TrimSpace(interval)) {
	case "1m":
		return 1 * time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return 1 * time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval %s", interval)
	}
}
