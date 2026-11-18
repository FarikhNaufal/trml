package data

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

const exchangeInfoTTL = 24 * time.Hour

// CatalogedExchangeInfo classifies Binance exchangeInfo symbols into active crypto perpetuals vs blocklisted non-perpetuals.
type CatalogedExchangeInfo struct {
	ActiveFilters map[string]model.ExchangeFilter
	Blocklisted   map[string]bool
	ContractTypes map[string]string
}

// LoadExchangeFilters memuat metadata presisi dan filter bursa resmi Binance Futures.
func LoadExchangeFilters(ctx context.Context, cacheDir string) (map[string]model.ExchangeFilter, error) {
	cat, err := LoadExchangeFiltersCatalog(ctx, cacheDir, nil, false)
	if err != nil {
		return nil, err
	}
	return cat.ActiveFilters, nil
}

// LoadExchangeFiltersCatalog loads exchange filters with atomic disk cache, 24h TTL, and explicit non-crypto blocklist.
func LoadExchangeFiltersCatalog(
	ctx context.Context,
	cacheDir string,
	httpClient *BacktestHTTPClient,
	forceRefresh bool,
) (CatalogedExchangeInfo, error) {
	dirs := ResolveBacktestDirs(cacheDir)
	cacheFile := filepath.Join(dirs.LegacyCSV, "exchange_info.json")
	if _, err := os.Stat(cacheFile); err != nil {
		alt := filepath.Join(dirs.StoreDir, "exchange_info.json")
		if _, altErr := os.Stat(alt); altErr == nil {
			cacheFile = alt
		}
	}

	var rawBytes []byte
	if !forceRefresh {
		if info, statErr := os.Stat(cacheFile); statErr == nil && info.Size() > 0 && time.Since(info.ModTime()) < exchangeInfoTTL {
			if fileData, readErr := os.ReadFile(cacheFile); readErr == nil && len(fileData) > 0 {
				if cat, parseErr := ParseExchangeInfoCatalog(fileData); parseErr == nil {
					return cat, nil
				}
			}
		}
	}

	if httpClient == nil {
		httpClient = NewBacktestHTTPClient(BacktestHTTPConfig{AllowRESTFallback: true})
	}
	apiURL := "https://fapi.binance.com/fapi/v1/exchangeInfo"
	body, _, reqErr := httpClient.GetREST(ctx, apiURL, 1, true)
	if reqErr == nil && len(body) > 0 {
		rawBytes = body
		_ = WriteFileAtomic(cacheFile, rawBytes, 0644)
		return ParseExchangeInfoCatalog(rawBytes)
	}

	// Fallback to stale local cache if network failed
	if fileData, readErr := os.ReadFile(cacheFile); readErr == nil && len(fileData) > 0 {
		return ParseExchangeInfoCatalog(fileData)
	}
	return CatalogedExchangeInfo{}, reqErr
}

// LoadExchangeFiltersOffline memuat filter bursa resmi MURNI dari cache lokal tanpa menyentuh network.
func LoadExchangeFiltersOffline(cacheDir string) (map[string]model.ExchangeFilter, error) {
	cat, err := LoadExchangeFiltersCatalogOffline(cacheDir)
	if err != nil {
		return nil, err
	}
	return cat.ActiveFilters, nil
}

// LoadExchangeFiltersCatalogOffline reads exchange_info.json purely from local cache without network access.
func LoadExchangeFiltersCatalogOffline(cacheDir string) (CatalogedExchangeInfo, error) {
	dirs := ResolveBacktestDirs(cacheDir)
	for _, candidate := range []string{
		filepath.Join(dirs.LegacyCSV, "exchange_info.json"),
		filepath.Join(dirs.StoreDir, "exchange_info.json"),
	} {
		if fileData, err := os.ReadFile(candidate); err == nil && len(fileData) > 0 {
			return ParseExchangeInfoCatalog(fileData)
		}
	}
	return CatalogedExchangeInfo{}, fmt.Errorf("exchange_info.json tidak ditemukan di cache lokal %s", dirs.LegacyCSV)
}

// DiscoverMarketSymbolsOffline mengembalikan daftar koin murni dari direktori cache/store lokal.
func DiscoverMarketSymbolsOffline(cacheDir string) ([]string, error) {
	dirs := ResolveBacktestDirs(cacheDir)
	seen := make(map[string]struct{})
	for _, root := range []string{dirs.StoreDir, dirs.LegacyCSV} {
		for _, sym := range listLegacyCacheSymbols(root) {
			seen[sym] = struct{}{}
		}
	}

	var cat CatalogedExchangeInfo
	if loadedCat, err := LoadExchangeFiltersCatalogOffline(cacheDir); err == nil {
		cat = loadedCat
	}

	cachedSyms := make([]string, 0, len(seen))
	for sym := range seen {
		if cat.Blocklisted != nil && cat.Blocklisted[sym] {
			continue
		}
		cachedSyms = append(cachedSyms, sym)
	}
	if len(cachedSyms) == 0 {
		return nil, fmt.Errorf("tidak ada koin USDT di cache lokal %s", dirs.LegacyCSV)
	}

	if _, ok := seen["BTCUSDT"]; !ok {
		cachedSyms = append(cachedSyms, "BTCUSDT")
	}
	sort.Strings(cachedSyms)
	return cachedSyms, nil
}

func parseExchangeInfoJSON(raw []byte) (map[string]model.ExchangeFilter, error) {
	cat, err := ParseExchangeInfoCatalog(raw)
	if err != nil {
		return nil, err
	}
	return cat.ActiveFilters, nil
}

// ParseExchangeInfoCatalog parses Binance exchangeInfo into active crypto perpetual filters and non-crypto blocklist.
func ParseExchangeInfoCatalog(raw []byte) (CatalogedExchangeInfo, error) {
	var res struct {
		Symbols []struct {
			Symbol       string `json:"symbol"`
			ContractType string `json:"contractType"`
			Status       string `json:"status"`
			Filters      []struct {
				FilterType  string `json:"filterType"`
				TickSize    string `json:"tickSize"`
				StepSize    string `json:"stepSize"`
				MinQty      string `json:"minQty"`
				MinNotional string `json:"minNotional"`
				Notional    string `json:"notional"`
			} `json:"filters"`
		} `json:"symbols"`
	}

	if err := json.Unmarshal(raw, &res); err != nil {
		return CatalogedExchangeInfo{}, fmt.Errorf("parse exchangeInfo JSON: %w", err)
	}

	out := CatalogedExchangeInfo{
		ActiveFilters: make(map[string]model.ExchangeFilter, len(res.Symbols)),
		Blocklisted:   make(map[string]bool),
		ContractTypes: make(map[string]string, len(res.Symbols)),
	}

	for _, s := range res.Symbols {
		sym := strings.ToUpper(strings.TrimSpace(s.Symbol))
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		out.ContractTypes[sym] = s.ContractType
		if s.ContractType != "PERPETUAL" {
			out.Blocklisted[sym] = true
			continue
		}

		var ef model.ExchangeFilter
		for _, f := range s.Filters {
			switch f.FilterType {
			case "PRICE_FILTER":
				ef.TickSize, _ = decimal.NewFromString(f.TickSize)
			case "LOT_SIZE":
				ef.StepSize, _ = decimal.NewFromString(f.StepSize)
				ef.MinQty, _ = decimal.NewFromString(f.MinQty)
			case "MIN_NOTIONAL":
				if f.MinNotional != "" {
					ef.MinNotional, _ = decimal.NewFromString(f.MinNotional)
				} else if f.Notional != "" {
					ef.MinNotional, _ = decimal.NewFromString(f.Notional)
				}
			}
		}

		if ef.TickSize.GreaterThan(decimal.Zero) && ef.StepSize.GreaterThan(decimal.Zero) {
			out.ActiveFilters[sym] = ef
		}
	}

	return out, nil
}

// DiscoverPointInTimeUniverse discovers all historical USDT crypto perpetuals (active + delisted) from S3 Vision
// without filtering by today's 24h volume (preventing survivorship & look-ahead bias).
func DiscoverPointInTimeUniverse(
	ctx context.Context,
	cacheDir string,
	httpClient *BacktestHTTPClient,
	store *BacktestStore,
	offline bool,
) (UniverseResolution, error) {
	dirs := ResolveBacktestDirs(cacheDir)
	localSyms := append(listLegacyCacheSymbols(dirs.StoreDir), listLegacyCacheSymbols(dirs.LegacyCSV)...)

	var cat CatalogedExchangeInfo
	if offline {
		cat, _ = LoadExchangeFiltersCatalogOffline(cacheDir)
	} else {
		cat, _ = LoadExchangeFiltersCatalog(ctx, cacheDir, httpClient, false)
	}

	var s3Syms []string
	if !offline {
		var err error
		s3Syms, err = DiscoverS3HistoricalSymbols(ctx, httpClient, defaultS3ListBaseURL)
		if err != nil && store != nil {
			if metas, dbErr := store.GetEligibleUniverseSymbols(ctx); dbErr == nil && len(metas) > 0 {
				for _, m := range metas {
					s3Syms = append(s3Syms, m.Symbol)
				}
			}
		}
		res := ReconcileHistoricalUniverse(ctx, s3Syms, cat, localSyms, store)
		if len(res.Symbols) == 0 {
			return res, fmt.Errorf("point-in-time universe is empty (check network or local cache %s)", cacheDir)
		}
		return res, nil
	}

	// Offline mode: restrict universe candidates strictly to locally cached symbols (excluding TradFi blocklist)
	// so offline runs never waste iterations scanning hundreds of uncached remote symbols.
	offCat := CatalogedExchangeInfo{
		ActiveFilters: make(map[string]model.ExchangeFilter, len(localSyms)),
		Blocklisted:   cat.Blocklisted,
		ContractTypes: cat.ContractTypes,
	}
	for _, sym := range localSyms {
		if f, ok := cat.ActiveFilters[sym]; ok {
			offCat.ActiveFilters[sym] = f
		}
	}
	res := ReconcileHistoricalUniverse(ctx, nil, offCat, localSyms, store)
	if len(res.Symbols) == 0 {
		return res, fmt.Errorf("point-in-time offline universe is empty (cache %s)", cacheDir)
	}
	return res, nil
}

// DiscoverMarketSymbols keeps legacy signature while enforcing rate-limited REST access when queried online.
func DiscoverMarketSymbols(ctx context.Context, cacheDir string, minVolumeUSDT float64) ([]string, error) {
	dirs := ResolveBacktestDirs(cacheDir)
	client := NewBacktestHTTPClient(BacktestHTTPConfig{AllowRESTFallback: true})
	cat, _ := LoadExchangeFiltersCatalog(ctx, cacheDir, client, false)

	apiURL := "https://fapi.binance.com/fapi/v1/ticker/24hr"
	body, status, err := client.GetREST(ctx, apiURL, 40, true)
	if err == nil && status == 200 && len(body) > 0 {
		var tickers []struct {
			Symbol      string `json:"symbol"`
			QuoteVolume string `json:"quoteVolume"`
		}
		if json.Unmarshal(body, &tickers) == nil {
			if minVolumeUSDT <= 0 {
				minVolumeUSDT = 25000000.0
			}
			var syms []string
			hasBTC := false
			for _, t := range tickers {
				sym := strings.ToUpper(t.Symbol)
				if !strings.HasSuffix(sym, "USDT") || cat.Blocklisted[sym] {
					continue
				}
				if sym == "BTCUSDT" {
					hasBTC = true
				}
				vol, _ := decimal.NewFromString(t.QuoteVolume)
				if vol.GreaterThanOrEqual(decimal.NewFromFloat(minVolumeUSDT)) {
					syms = append(syms, sym)
				}
			}
			if !hasBTC {
				syms = append([]string{"BTCUSDT"}, syms...)
			}
			if len(syms) > 0 {
				sort.Strings(syms)
				return syms, nil
			}
		}
	}

	if offlineSyms, offErr := DiscoverMarketSymbolsOffline(dirs.LegacyCSV); offErr == nil {
		return offlineSyms, nil
	}
	return nil, fmt.Errorf("gagal mendapatkan simbol dari Binance dan cache lokal kosong")
}
