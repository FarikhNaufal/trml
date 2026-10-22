package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/shopspring/decimal"
	"trml/internal/model"
)

// FetchExchangeInfo mengambil spesifikasi filter presisi seluruh simbol perpetual
func (c *Client) FetchExchangeInfo(ctx context.Context) (map[string]model.ExchangeFilter, error) {
	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/exchangeInfo", nil, false)
	if err != nil {
		return nil, fmt.Errorf("exchangeInfo: %w", err)
	}

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

	if err := sonic.Unmarshal(respBytes, &res); err != nil {
		return nil, fmt.Errorf("exchangeInfo: failed to decode JSON: %w", err)
	}

	filters := make(map[string]model.ExchangeFilter)
	for _, s := range res.Symbols {
		if s.ContractType != "PERPETUAL" || s.Status != "TRADING" || !strings.HasSuffix(s.Symbol, "USDT") {
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

// Fetch24hTickers mengambil volume USDT 24 jam untuk seluruh simbol
func (c *Client) Fetch24hTickers(ctx context.Context) (map[string]float64, error) {
	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/ticker/24hr", nil, false)
	if err != nil {
		return nil, fmt.Errorf("ticker24hr: %w", err)
	}

	var list []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}

	if err := sonic.Unmarshal(respBytes, &list); err != nil {
		return nil, fmt.Errorf("ticker24hr: failed to decode JSON: %w", err)
	}

	volMap := make(map[string]float64, len(list))
	for _, item := range list {
		v, _ := strconv.ParseFloat(item.QuoteVolume, 64)
		volMap[item.Symbol] = v
	}

	return volMap, nil
}

// FetchBookTickers mengambil snapshot harga bid/ask terbaik terkini
func (c *Client) FetchBookTickers(ctx context.Context) (map[string]model.BookQuote, error) {
	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/ticker/bookTicker", nil, false)
	if err != nil {
		return nil, fmt.Errorf("bookTicker: %w", err)
	}

	var list []struct {
		Symbol   string `json:"symbol"`
		BidPrice string `json:"bidPrice"`
		AskPrice string `json:"askPrice"`
	}

	if err := sonic.Unmarshal(respBytes, &list); err != nil {
		return nil, fmt.Errorf("bookTicker: failed to decode JSON: %w", err)
	}

	bookMap := make(map[string]model.BookQuote, len(list))
	for _, item := range list {
		bid, _ := strconv.ParseFloat(item.BidPrice, 64)
		ask, _ := strconv.ParseFloat(item.AskPrice, 64)
		if bid > 0 && ask > 0 {
			bookMap[item.Symbol] = model.BookQuote{
				BestBid: bid,
				BestAsk: ask,
			}
		}
	}

	return bookMap, nil
}

// FetchKlines mengambil data candle historis (GET /fapi/v1/klines)
func (c *Client) FetchKlines(ctx context.Context, symbol, interval string, limit int) ([]model.Candle, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("interval", interval)
	params.Set("limit", strconv.Itoa(limit))

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/klines", params, false)
	if err != nil {
		return nil, fmt.Errorf("klines %s: %w", symbol, err)
	}

	var rawList [][]interface{}
	if err := json.Unmarshal(respBytes, &rawList); err != nil {
		return nil, fmt.Errorf("klines: failed to decode JSON: %w", err)
	}

	candles := make([]model.Candle, 0, len(rawList))
	for _, row := range rawList {
		if len(row) < 11 {
			continue
		}
		openTimeMs, _ := row[0].(float64)
		closeTimeMs, _ := row[6].(float64)

		o, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[1]), 64)
		h, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[2]), 64)
		l, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[3]), 64)
		cl, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[4]), 64)
		vb, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[5]), 64)
		qv, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[7]), 64)
		tbq, _ := strconv.ParseFloat(fmt.Sprintf("%v", row[10]), 64)

		candles = append(candles, model.Candle{
			OpenTime:          time.UnixMilli(int64(openTimeMs)).UTC(),
			CloseTime:         time.UnixMilli(int64(closeTimeMs)).UTC(),
			Open:              o,
			High:              h,
			Low:               l,
			Close:             cl,
			VolumeBase:        vb,
			QuoteVolumeUSDT:   qv,
			TakerBuyQuoteUSDT: tbq,
		})
	}

	return candles, nil
}

// FetchOpenInterest mengambil metrik Open Interest terkini (GET /fapi/v1/openInterest)
func (c *Client) FetchOpenInterest(ctx context.Context, symbol string) (float64, error) {
	params := url.Values{}
	params.Set("symbol", symbol)

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/openInterest", params, false)
	if err != nil {
		return 0, fmt.Errorf("openInterest %s: %w", symbol, err)
	}

	var res struct {
		Symbol       string `json:"symbol"`
		OpenInterest string `json:"openInterest"`
	}

	if err := sonic.Unmarshal(respBytes, &res); err != nil {
		return 0, fmt.Errorf("openInterest: failed to decode JSON: %w", err)
	}

	oi, err := strconv.ParseFloat(res.OpenInterest, 64)
	if err != nil {
		return 0, fmt.Errorf("openInterest: invalid float: %w", err)
	}

	return oi, nil
}

// OpenInterestHistItem merepresentasikan entri data historis Open Interest dari Binance
type OpenInterestHistItem struct {
	Symbol          string `json:"symbol"`
	SumOpenInterest string `json:"sumOpenInterest"`
	Timestamp       int64  `json:"timestamp"`
}

// FetchOpenInterestHist mengambil data historis Open Interest 5-menit (GET /futures/data/openInterestHist)
// Mengembalikan slice bar historis dengan unit kontrak sumOpenInterest (Pengawal 1)
func (c *Client) FetchOpenInterestHist(ctx context.Context, symbol string, limit int) ([]OpenInterestHistItem, error) {
	if limit <= 0 {
		limit = 3
	}
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("period", "5m")
	params.Set("limit", strconv.Itoa(limit))

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/futures/data/openInterestHist", params, false)
	if err != nil {
		return nil, fmt.Errorf("openInterestHist %s: %w", symbol, err)
	}

	var res []OpenInterestHistItem
	if err := sonic.Unmarshal(respBytes, &res); err != nil {
		return nil, fmt.Errorf("openInterestHist %s: failed to decode JSON: %w", symbol, err)
	}

	return res, nil
}

// CreateListenKey membuat user data stream listenKey (POST /fapi/v1/listenKey)
func (c *Client) CreateListenKey(ctx context.Context) (string, error) {
	respBytes, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/listenKey", nil, false)
	if err != nil {
		return "", fmt.Errorf("create listenKey: %w", err)
	}

	var res struct {
		ListenKey string `json:"listenKey"`
	}
	if err := sonic.Unmarshal(respBytes, &res); err != nil {
		return "", fmt.Errorf("create listenKey: failed to decode JSON: %w", err)
	}

	return res.ListenKey, nil
}

// KeepAliveListenKey memperpanjang masa aktif listenKey (PUT /fapi/v1/listenKey)
func (c *Client) KeepAliveListenKey(ctx context.Context) error {
	_, err := c.DoRequest(ctx, http.MethodPut, "/fapi/v1/listenKey", nil, false)
	if err != nil {
		return fmt.Errorf("keepalive listenKey: %w", err)
	}
	return nil
}

// DeleteListenKey menutup dan menghapus user data stream listenKey (DELETE /fapi/v1/listenKey)
func (c *Client) DeleteListenKey(ctx context.Context, listenKey string) error {
	params := url.Values{}
	if listenKey != "" {
		params.Set("listenKey", listenKey)
	}
	_, err := c.DoRequest(ctx, http.MethodDelete, "/fapi/v1/listenKey", params, false)
	if err != nil {
		return fmt.Errorf("delete listenKey: %w", err)
	}
	return nil
}

