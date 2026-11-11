package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// APIError merepresentasikan respon error terstruktur dari Binance API
type APIError struct {
	HTTPStatusCode int    `json:"-"`
	Code           int    `json:"code"`
	Msg            string `json:"msg"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("exchange: API returned HTTP %d (code=%d, msg=%s)", e.HTTPStatusCode, e.Code, e.Msg)
}

type Client struct {
	BaseURL     string
	APIKey      string
	SecretKey   string
	HTTPClient  *http.Client
	TimeOffset  int64
	RecvWindow  int64
	bannedUntil int64 // unix milli timestamp (circuit breaker untuk HTTP 418 / code -1003)
}

func (c *Client) IsIPBanned() (bool, time.Time) {
	b := atomic.LoadInt64(&c.bannedUntil)
	if b == 0 {
		return false, time.Time{}
	}
	now := time.Now().UnixMilli()
	if b > now {
		return true, time.UnixMilli(b)
	}
	return false, time.Time{}
}

func (c *Client) SetBanUntil(untilMs int64) {
	atomic.StoreInt64(&c.bannedUntil, untilMs)
}

func parseBanExpiry(header http.Header, msg string) int64 {
	// 1. Cek header Retry-After
	if retryHeader := header.Get("Retry-After"); retryHeader != "" {
		if s, err := strconv.ParseInt(retryHeader, 10, 64); err == nil && s > 0 {
			return time.Now().Add(time.Duration(s) * time.Second).UnixMilli()
		}
	}

	// 2. Cek pesan body: "banned until 1794327217106"
	idx := strings.Index(msg, "banned until ")
	if idx != -1 {
		sub := msg[idx+len("banned until "):]
		endIdx := strings.IndexAny(sub, ". ,;\t\n")
		var numStr string
		if endIdx != -1 {
			numStr = sub[:endIdx]
		} else {
			numStr = sub
		}
		if ts, err := strconv.ParseInt(numStr, 10, 64); err == nil && ts > 0 {
			return ts
		}
	}

	// 3. Fallback: proteksi freeze 5 menit
	return time.Now().Add(5 * time.Minute).UnixMilli()
}

func NewClient(baseURL, apiKey, secretKey string) *Client {
	return &Client{
		BaseURL:    baseURL,
		APIKey:     apiKey,
		SecretKey:  secretKey,
		RecvWindow: 5000,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SyncServerTime menyinkronkan jam lokal dengan jam bursa untuk mencegah error -1021
func (c *Client) SyncServerTime(ctx context.Context) error {
	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/time", nil, false)
	if err != nil {
		return fmt.Errorf("sync server time: %w", err)
	}

	var res struct {
		ServerTime int64 `json:"serverTime"`
	}
	if err := json.Unmarshal(respBytes, &res); err != nil {
		return fmt.Errorf("sync server time unmarshal: %w", err)
	}

	if res.ServerTime > 0 {
		c.TimeOffset = res.ServerTime - time.Now().UnixMilli()
	}
	return nil
}

func (c *Client) signQuery(params url.Values) string {
	mac := hmac.New(sha256.New, []byte(c.SecretKey))
	mac.Write([]byte(params.Encode()))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Client) DoRequest(ctx context.Context, method, endpoint string, params url.Values, signed bool) ([]byte, error) {
	if banned, until := c.IsIPBanned(); banned {
		return nil, fmt.Errorf("exchange: outgoing requests paused due to active Binance IP ban until %s", until.Format(time.RFC3339))
	}

	if params == nil {
		params = url.Values{}
	}

	maxRetries := 3
	var lastErr error
	var lastBody []byte

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Jika signed, timestamp harus dibuat baru pada setiap attempt
		attemptParams := make(url.Values, len(params)+3)
		for k, v := range params {
			attemptParams[k] = append([]string(nil), v...)
		}

		if signed {
			ts := time.Now().UnixMilli() + c.TimeOffset
			attemptParams.Set("timestamp", strconv.FormatInt(ts, 10))
			recvWin := c.RecvWindow
			if recvWin <= 0 {
				recvWin = 5000
			}
			attemptParams.Set("recvWindow", strconv.FormatInt(recvWin, 10))
			sig := c.signQuery(attemptParams)
			attemptParams.Set("signature", sig)
		}

		reqURL := fmt.Sprintf("%s%s", c.BaseURL, endpoint)
		var req *http.Request
		var err error

		if method == http.MethodGet || method == http.MethodDelete {
			if len(attemptParams) > 0 {
				reqURL = fmt.Sprintf("%s?%s", reqURL, attemptParams.Encode())
			}
			req, err = http.NewRequestWithContext(ctx, method, reqURL, nil)
		} else {
			req, err = http.NewRequestWithContext(ctx, method, reqURL, strings.NewReader(attemptParams.Encode()))
			if req != nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
		}
		if err != nil {
			return nil, fmt.Errorf("exchange: failed to create request: %w", err)
		}

		if c.APIKey != "" {
			req.Header.Set("X-MBX-APIKEY", c.APIKey)
		}

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("exchange: network error on %s %s: %w", method, endpoint, err)

			// Pencegahan Order Ganda: Jika POST order mengalami network error, cek apakah order sebenarnya sudah diterima Binance
			if method == http.MethodPost && (strings.Contains(endpoint, "/order") || strings.Contains(endpoint, "/algoOrder")) {
				clientOID := params.Get("newClientOrderId")
				if clientOID == "" {
					clientOID = params.Get("clientAlgoId")
				}
				symbol := params.Get("symbol")
				if clientOID != "" && symbol != "" {
					qParams := url.Values{}
					qParams.Set("symbol", symbol)
					qParams.Set("origClientOrderId", clientOID)
					checkBody, checkErr := c.doRequestOnce(ctx, http.MethodGet, "/fapi/v1/order", qParams, true)
					if checkErr == nil && len(checkBody) > 0 {
						return checkBody, nil
					}
				}
			}

			// Network error sementara: tunggu sebelum retry dengan menghormati pembatalan context
			if attempt < maxRetries-1 {
				retryDelay := time.Duration(250*(1<<attempt)) * time.Millisecond
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(retryDelay):
					continue
				}
			}
			return nil, lastErr
		}

		// L3: Bounded reader 10MB untuk proteksi konsumsi memori
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("exchange: failed to read response body: %w", readErr)
		}
		lastBody = body

		// Circuit Breaker: Tangani HTTP 418 (IP Ban) tanpa retry agar tidak memperpanjang ban
		if resp.StatusCode == 418 {
			var apiErr APIError
			_ = json.Unmarshal(body, &apiErr)
			apiErr.HTTPStatusCode = 418
			expiry := parseBanExpiry(resp.Header, apiErr.Msg)
			c.SetBanUntil(expiry)
			return body, &apiErr
		}

		// Tangani Rate Limit (429) dan Server Error (5xx) dengan retry bounded
		if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode <= 504) {
			if method == http.MethodPost && (strings.Contains(endpoint, "/order") || strings.Contains(endpoint, "/algoOrder")) {
				clientOID := params.Get("newClientOrderId")
				if clientOID == "" {
					clientOID = params.Get("clientAlgoId")
				}
				symbol := params.Get("symbol")
				if clientOID != "" && symbol != "" {
					qParams := url.Values{}
					qParams.Set("symbol", symbol)
					qParams.Set("origClientOrderId", clientOID)
					checkBody, checkErr := c.doRequestOnce(ctx, http.MethodGet, "/fapi/v1/order", qParams, true)
					if checkErr == nil && len(checkBody) > 0 {
						return checkBody, nil
					}
				}
			}

			retryDelay := time.Duration(500*(1<<attempt)) * time.Millisecond
			if retryHeader := resp.Header.Get("Retry-After"); retryHeader != "" {
				if s, err := strconv.Atoi(retryHeader); err == nil && s > 0 {
					retryDelay = time.Duration(s) * time.Second
				}
			}
			if attempt < maxRetries-1 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(retryDelay):
					continue
				}
			}
		}

		if resp.StatusCode >= 400 {
			var apiErr APIError
			if jsonErr := json.Unmarshal(body, &apiErr); jsonErr == nil && apiErr.Code != 0 {
				apiErr.HTTPStatusCode = resp.StatusCode
				if apiErr.Code == -1003 {
					expiry := parseBanExpiry(resp.Header, apiErr.Msg)
					c.SetBanUntil(expiry)
				}
				return body, &apiErr
			}
			return body, fmt.Errorf("exchange: API returned HTTP %d: %s", resp.StatusCode, string(body))
		}

		return body, nil
	}

	if lastErr != nil {
		return lastBody, lastErr
	}
	return lastBody, fmt.Errorf("exchange: request failed after %d retries", maxRetries)
}

func (c *Client) doRequestOnce(ctx context.Context, method, endpoint string, params url.Values, signed bool) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}
	attemptParams := make(url.Values, len(params)+3)
	for k, v := range params {
		attemptParams[k] = append([]string(nil), v...)
	}
	if signed {
		ts := time.Now().UnixMilli() + c.TimeOffset
		attemptParams.Set("timestamp", strconv.FormatInt(ts, 10))
		recvWin := c.RecvWindow
		if recvWin <= 0 {
			recvWin = 5000
		}
		attemptParams.Set("recvWindow", strconv.FormatInt(recvWin, 10))
		sig := c.signQuery(attemptParams)
		attemptParams.Set("signature", sig)
	}

	reqURL := fmt.Sprintf("%s%s", c.BaseURL, endpoint)
	var req *http.Request
	var err error

	if method == http.MethodGet || method == http.MethodDelete {
		if len(attemptParams) > 0 {
			reqURL = fmt.Sprintf("%s?%s", reqURL, attemptParams.Encode())
		}
		req, err = http.NewRequestWithContext(ctx, method, reqURL, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, reqURL, strings.NewReader(attemptParams.Encode()))
		if req != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("exchange: failed to create request: %w", err)
	}
	if c.APIKey != "" {
		req.Header.Set("X-MBX-APIKEY", c.APIKey)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode >= 400 {
		var apiErr APIError
		if jsonErr := json.Unmarshal(body, &apiErr); jsonErr == nil && apiErr.Code != 0 {
			apiErr.HTTPStatusCode = resp.StatusCode
			return body, &apiErr
		}
		return body, fmt.Errorf("exchange: API returned HTTP %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}
