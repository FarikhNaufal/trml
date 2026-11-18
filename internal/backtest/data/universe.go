package data

import (
	"context"
	"encoding/xml"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

const (
	defaultS3ListBaseURL = "https://data.binance.vision"
	s3MonthlyKlinePrefix = "data/futures/um/monthly/klines/"
)

// UniverseResolution describes the point-in-time candidate universe and blocklisted non-crypto perpetuals.
type UniverseResolution struct {
	Symbols           []string
	ActiveCryptoCount int
	DelistedCount     int
	BlocklistedTradFi int
}

type s3ListBucketResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	CommonPrefixes        []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
}

// DiscoverS3HistoricalSymbols lists all USDT futures symbols archived on data.binance.vision (including delisted contracts)
// by walking paginated ListObjectsV2 XML responses cleanly.
func DiscoverS3HistoricalSymbols(ctx context.Context, client *BacktestHTTPClient, s3BaseURL string) ([]string, error) {
	if client == nil {
		client = NewBacktestHTTPClient(BacktestHTTPConfig{})
	}
	if strings.TrimSpace(s3BaseURL) == "" {
		s3BaseURL = defaultS3ListBaseURL
	}
	s3BaseURL = strings.TrimRight(s3BaseURL, "/")

	seen := make(map[string]struct{})
	var syms []string
	token := ""

	for page := 0; page < 50; page++ {
		params := url.Values{}
		params.Set("list-type", "2")
		params.Set("delimiter", "/")
		params.Set("prefix", s3MonthlyKlinePrefix)
		if token != "" {
			params.Set("continuation-token", token)
		}
		reqURL := fmt.Sprintf("%s/?%s", s3BaseURL, params.Encode())
		body, status, err := client.GetVision(ctx, reqURL)
		if err != nil {
			return nil, fmt.Errorf("S3 symbol listing failed (page %d): %w", page, err)
		}
		if status != 200 {
			return nil, fmt.Errorf("S3 symbol listing status %d", status)
		}

		var parsed s3ListBucketResult
		if err := xml.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("parse S3 symbol listing XML: %w", err)
		}

		for _, cp := range parsed.CommonPrefixes {
			trimmed := strings.TrimSuffix(strings.TrimPrefix(cp.Prefix, s3MonthlyKlinePrefix), "/")
			sym := strings.ToUpper(strings.TrimSpace(trimmed))
			if strings.HasSuffix(sym, "USDT") {
				if _, ok := seen[sym]; !ok {
					seen[sym] = struct{}{}
					syms = append(syms, sym)
				}
			}
		}

		if !parsed.IsTruncated || parsed.NextContinuationToken == "" {
			break
		}
		token = parsed.NextContinuationToken
	}

	sort.Strings(syms)
	return syms, nil
}

// ReconcileHistoricalUniverse combines S3 historical archives (or local cache) with exchangeInfo metadata:
//   - Excludes non-PERPETUAL contracts (e.g., TRADIFI_PERPETUAL, QUARTERLY) via explicit blocklist.
//   - Keeps delisted crypto perpetuals present on Binance Vision / local cache rather than skipping them.
//   - Always ensures BTCUSDT is included as macro benchmark.
func ReconcileHistoricalUniverse(
	ctx context.Context,
	s3Symbols []string,
	exchangeInfo CatalogedExchangeInfo,
	localCached []string,
	store *BacktestStore,
) UniverseResolution {
	combined := make(map[string]struct{}, len(s3Symbols)+len(localCached)+len(exchangeInfo.ActiveFilters))
	for _, s := range s3Symbols {
		combined[strings.ToUpper(s)] = struct{}{}
	}
	for _, s := range localCached {
		combined[strings.ToUpper(s)] = struct{}{}
	}
	for sym := range exchangeInfo.ActiveFilters {
		combined[sym] = struct{}{}
	}
	combined["BTCUSDT"] = struct{}{}

	var res UniverseResolution
	var metas []UniverseSymbolMeta

	for sym := range combined {
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		if exchangeInfo.Blocklisted[sym] {
			res.BlocklistedTradFi++
			metas = append(metas, UniverseSymbolMeta{
				Symbol:        sym,
				Status:        "BLOCKLISTED",
				ContractType:  exchangeInfo.ContractTypes[sym],
				IsBlocklisted: true,
			})
			continue
		}

		if _, isActive := exchangeInfo.ActiveFilters[sym]; isActive {
			res.ActiveCryptoCount++
			metas = append(metas, UniverseSymbolMeta{
				Symbol:        sym,
				Status:        "TRADING",
				ContractType:  "PERPETUAL",
				IsBlocklisted: false,
			})
		} else {
			res.DelistedCount++
			metas = append(metas, UniverseSymbolMeta{
				Symbol:        sym,
				Status:        "DELISTED_OR_ARCHIVED",
				ContractType:  "PERPETUAL_HISTORICAL",
				IsBlocklisted: false,
			})
		}
		res.Symbols = append(res.Symbols, sym)
	}

	sort.Strings(res.Symbols)
	if store != nil && len(metas) > 0 {
		_ = store.UpsertUniverseSymbols(ctx, metas)
	}
	return res
}

// InferFilterFromCandles derives conservative tickSize and stepSize from observed decimal precision
// of historical OHLCV bars for delisted contracts missing from today's exchangeInfo.
func InferFilterFromCandles(candles []HistoricalCandle) model.ExchangeFilter {
	maxPriceDec := 2
	maxVolDec := 1

	sampleLimit := len(candles)
	if sampleLimit > 200 {
		sampleLimit = 200
	}
	for i := 0; i < sampleLimit; i++ {
		c := candles[i]
		for _, p := range []float64{c.Open, c.High, c.Low, c.Close} {
			if d := decimalPlaces(p); d > maxPriceDec && d <= 8 {
				maxPriceDec = d
			}
		}
		if d := decimalPlaces(c.Volume); d > maxVolDec && d <= 6 {
			maxVolDec = d
		}
	}

	tick := decimal.New(1, -int32(maxPriceDec))
	step := decimal.New(1, -int32(maxVolDec))
	return model.ExchangeFilter{
		TickSize:    tick,
		StepSize:    step,
		MinQty:      step,
		MinNotional: decimal.NewFromFloat(5.0),
	}
}

func decimalPlaces(v float64) int {
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0
	}
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if idx := strings.IndexByte(s, '.'); idx != -1 {
		return len(s) - idx - 1
	}
	return 0
}

// listLegacyCacheSymbols returns cached USDT folder names without touching network.
func listLegacyCacheSymbols(legacyDir string) []string {
	entries, err := os.ReadDir(legacyDir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		name := strings.ToUpper(e.Name())
		if e.IsDir() && strings.HasSuffix(name, "USDT") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
