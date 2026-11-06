package data

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

// LoadExchangeFilters memuat metadata presisi dan filter bursa resmi Binance Futures.
// Jika file cache lokal tersedia, membacanya langsung; jika belum, mengambil dari Binance REST API.
func LoadExchangeFilters(ctx context.Context, cacheDir string) (map[string]model.ExchangeFilter, error) {
	if cacheDir == "" {
		cacheDir = filepath.Join("data", "backtest", "cache")
	}
	cacheFile := filepath.Join(cacheDir, "exchange_info.json")

	var rawBytes []byte
	var err error

	// 1. Cek apakah ada di cache lokal dan tidak kosong
	if fileData, statErr := os.ReadFile(cacheFile); statErr == nil && len(fileData) > 0 {
		rawBytes = fileData
	} else {
		// 2. Fetch langsung dari endpoint publik Binance Futures exchangeInfo
		apiURL := "https://fapi.binance.com/fapi/v1/exchangeInfo"
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if reqErr != nil {
			return nil, reqErr
		}

		client := &http.Client{Timeout: 30 * time.Second}
		resp, respErr := client.Do(req)
		if respErr != nil {
			return nil, fmt.Errorf("fetch exchangeInfo: %w", respErr)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("exchangeInfo status code: %d", resp.StatusCode)
		}

		rawBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read exchangeInfo body: %w", err)
		}

		// Simpan ke disk cache untuk eksekusi offline cepat berikutnya
		_ = os.MkdirAll(cacheDir, 0755)
		_ = os.WriteFile(cacheFile, rawBytes, 0644)
	}

	return parseExchangeInfoJSON(rawBytes)
}

func parseExchangeInfoJSON(data []byte) (map[string]model.ExchangeFilter, error) {
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

	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("parse exchangeInfo JSON: %w", err)
	}

	filters := make(map[string]model.ExchangeFilter, len(res.Symbols))
	for _, s := range res.Symbols {
		if s.ContractType != "PERPETUAL" || !strings.HasSuffix(s.Symbol, "USDT") {
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
			filters[s.Symbol] = ef
		}
	}

	return filters, nil
}

// DiscoverMarketSymbols mengembalikan daftar koin real dari Binance Futures USDT.
// Urutan prioritas:
// 1. Jika folder cache lokal memiliki subfolder koin USDT, gunakan daftar tersebut (cepat & offline).
// 2. Jika cache kosong, fetch langsung dari endpoint publik Binance 24h ticker:
//    - Filter koin perpetual USDT yang memiliki volume 24 jam >= minVolumeUSDT.
func DiscoverMarketSymbols(ctx context.Context, cacheDir string, minVolumeUSDT float64) ([]string, error) {
	if cacheDir == "" {
		cacheDir = filepath.Join("data", "backtest", "cache")
	}

	// 1. Cek apakah di cache lokal sudah ada subfolder koin USDT
	if entries, err := os.ReadDir(cacheDir); err == nil && len(entries) > 0 {
		var cachedSyms []string
		for _, e := range entries {
			name := strings.ToUpper(e.Name())
			if e.IsDir() && strings.HasSuffix(name, "USDT") {
				cachedSyms = append(cachedSyms, name)
			}
		}
		if len(cachedSyms) > 0 {
			sort.Strings(cachedSyms)
			return cachedSyms, nil
		}
	}

	// 2. Jika cache belum ada, ambil daftar koin real dari Binance publik REST API
	apiURL := "https://fapi.binance.com/fapi/v1/ticker/24hr"
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if reqErr != nil {
		return nil, reqErr
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, fmt.Errorf("fetch 24hr tickers: %w", respErr)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("24hr tickers status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read 24hr tickers body: %w", err)
	}

	var tickers []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}
	if err := json.Unmarshal(body, &tickers); err != nil {
		return nil, fmt.Errorf("parse 24hr tickers JSON: %w", err)
	}

	if minVolumeUSDT <= 0 {
		minVolumeUSDT = 25000000.0 // default $25M
	}

	var syms []string
	hasBTC := false
	for _, t := range tickers {
		sym := strings.ToUpper(t.Symbol)
		if !strings.HasSuffix(sym, "USDT") {
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

	sort.Strings(syms)
	return syms, nil
}

