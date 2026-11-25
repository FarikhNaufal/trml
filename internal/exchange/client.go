package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
		atomic.StoreInt64(&c.TimeOffset, res.ServerTime-time.Now().UnixMilli())
	}
	return nil
}

// GetTimeOffset mengembalikan offset jam bursa secara thread-safe (ms).
func (c *Client) GetTimeOffset() int64 {
	return atomic.LoadInt64(&c.TimeOffset)
}

func (c *Client) signString(payload string) string {
	mac := hmac.New(sha256.New, []byte(c.SecretKey))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Client) buildEncodedParams(params url.Values, signed bool) string {
	attemptParams := make(url.Values, len(params)+2)
	for k, v := range params {
		attemptParams[k] = append([]string(nil), v...)
	}
	if signed {
		ts := time.Now().UnixMilli() + atomic.LoadInt64(&c.TimeOffset)
		attemptParams.Set("timestamp", strconv.FormatInt(ts, 10))
		recvWin := c.RecvWindow
		if recvWin <= 0 {
			recvWin = 5000
		}
		attemptParams.Set("recvWindow", strconv.FormatInt(recvWin, 10))
	}
	encoded := attemptParams.Encode()
	if !signed {
		return encoded
	}
	sig := c.signString(encoded)
	if encoded == "" {
		return "signature=" + sig
	}
	return encoded + "&signature=" + sig
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

		encodedBody := c.buildEncodedParams(params, signed)
		reqURL := fmt.Sprintf("%s%s", c.BaseURL, endpoint)
		var req *http.Request
		var err error

		if method == http.MethodGet || method == http.MethodDelete {
			if encodedBody != "" {
				reqURL = fmt.Sprintf("%s?%s", reqURL, encodedBody)
			}
			req, err = http.NewRequestWithContext(ctx, method, reqURL, nil)
		} else {
			req, err = http.NewRequestWithContext(ctx, method, reqURL, strings.NewReader(encodedBody))
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

			// Pencegahan Order Ganda (Fail-Closed): Jika POST /order atau /algoOrder mengalami network error,
			// cek apakah order sudah diterima atau secara eksplisit belum ada (-2013 / terminal zero-fill).
			// Jangan pernah retry buta saat hasil probe ambigu karena order MARKET bisa terisi ganda/rangkap tiga.
			if method == http.MethodPost && isOrderCreationEndpoint(endpoint) {
				existingBody, st := c.verifyPostOrderIdempotencyStatus(ctx, endpoint, params)
				if st == postIdempConfirmedExists {
					return existingBody, nil
				}
				if st != postIdempConfirmedNotFound {
					return nil, fmt.Errorf("exchange: ambiguous POST %s network error (retry aborted to prevent duplicate order): %w", endpoint, lastErr)
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

		// Tangani Rate Limit (429) dengan memeriksa kode -1003 terlebih dahulu tanpa spam GET idempotency
		if resp.StatusCode == http.StatusTooManyRequests {
			var apiErr APIError
			if jsonErr := json.Unmarshal(body, &apiErr); jsonErr == nil && apiErr.Code == -1003 {
				apiErr.HTTPStatusCode = resp.StatusCode
				expiry := parseBanExpiry(resp.Header, apiErr.Msg)
				c.SetBanUntil(expiry)
				return body, &apiErr
			}
		}

		// Tangani Rate Limit (429) dan Server Error (5xx) dengan retry bounded
		if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode <= 504) {
			if method == http.MethodPost && resp.StatusCode >= 500 && resp.StatusCode <= 504 && isOrderCreationEndpoint(endpoint) {
				existingBody, st := c.verifyPostOrderIdempotencyStatus(ctx, endpoint, params)
				if st == postIdempConfirmedExists {
					return existingBody, nil
				}
				if st != postIdempConfirmedNotFound {
					return body, fmt.Errorf("exchange: ambiguous POST %s HTTP %d (retry aborted to prevent duplicate order): %s", endpoint, resp.StatusCode, string(body))
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
					return body, &apiErr
				}
				if signed && apiErr.Code == -1021 && attempt < maxRetries-1 {
					_ = c.SyncServerTime(ctx)
					continue
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
	if banned, until := c.IsIPBanned(); banned {
		return nil, fmt.Errorf("exchange: outgoing requests paused due to active Binance IP ban until %s", until.Format(time.RFC3339))
	}
	if params == nil {
		params = url.Values{}
	}
	encodedBody := c.buildEncodedParams(params, signed)

	reqURL := fmt.Sprintf("%s%s", c.BaseURL, endpoint)
	var req *http.Request
	var err error

	if method == http.MethodGet || method == http.MethodDelete {
		if encodedBody != "" {
			reqURL = fmt.Sprintf("%s?%s", reqURL, encodedBody)
		}
		req, err = http.NewRequestWithContext(ctx, method, reqURL, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, reqURL, strings.NewReader(encodedBody))
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
	if resp.StatusCode == 418 {
		var apiErr APIError
		_ = json.Unmarshal(body, &apiErr)
		apiErr.HTTPStatusCode = 418
		expiry := parseBanExpiry(resp.Header, apiErr.Msg)
		c.SetBanUntil(expiry)
		return body, &apiErr
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

type postOrderIdempStatus uint8

const (
	postIdempUnknown postOrderIdempStatus = iota
	postIdempConfirmedExists
	postIdempConfirmedNotFound
)

func isOrderCreationEndpoint(endpoint string) bool {
	return strings.Contains(endpoint, "/algoOrder") || strings.Contains(endpoint, "/order")
}

// verifyPostOrderIdempotency memeriksa apakah POST /order atau POST /algoOrder sebenarnya sudah diterima bursa
// sebelum melakukan retry setelah network error / rate-limit / 5xx.
func (c *Client) verifyPostOrderIdempotency(ctx context.Context, endpoint string, params url.Values) ([]byte, bool) {
	body, st := c.verifyPostOrderIdempotencyStatus(ctx, endpoint, params)
	return body, st == postIdempConfirmedExists
}

func (c *Client) verifyPostOrderIdempotencyStatus(ctx context.Context, endpoint string, params url.Values) ([]byte, postOrderIdempStatus) {
	if params == nil {
		return nil, postIdempUnknown
	}
	symbol := params.Get("symbol")
	if symbol == "" {
		return nil, postIdempUnknown
	}

	if strings.Contains(endpoint, "/algoOrder") {
		clientAlgoID := params.Get("clientAlgoId")
		if clientAlgoID == "" {
			return nil, postIdempUnknown
		}
		qParams := url.Values{}
		qParams.Set("symbol", symbol)
		checkBody, checkErr := c.doRequestOnce(ctx, http.MethodGet, "/fapi/v1/openAlgoOrders", qParams, true)
		if checkErr != nil || len(checkBody) == 0 {
			return nil, postIdempUnknown
		}
		var openAlgos []QueryAlgoOrderResponse
		if jsonErr := json.Unmarshal(checkBody, &openAlgos); jsonErr != nil {
			return nil, postIdempUnknown
		}
		for _, item := range openAlgos {
			if item.ClientAlgoID == clientAlgoID && item.AlgoID != 0 {
				synth, mErr := json.Marshal(AlgoOrderResponse{
					AlgoID:       item.AlgoID,
					ClientAlgoID: item.ClientAlgoID,
				})
				if mErr == nil {
					return synth, postIdempConfirmedExists
				}
			}
		}
		// Jangan fail-open retry POST /algoOrder meskipun belum terlihat di openAlgoOrders,
		// karena conditional algo order dapat langsung terpicu atau sedang dalam propagasi.
		return nil, postIdempUnknown
	}

	if strings.Contains(endpoint, "/order") {
		clientOID := params.Get("newClientOrderId")
		if clientOID == "" {
			return nil, postIdempUnknown
		}
		qParams := url.Values{}
		qParams.Set("symbol", symbol)
		qParams.Set("origClientOrderId", clientOID)
		checkBody, checkErr := c.doRequestOnce(ctx, http.MethodGet, "/fapi/v1/order", qParams, true)
		if checkErr == nil && len(checkBody) > 0 {
			var qResp QueryOrderResponse
			if jErr := json.Unmarshal(checkBody, &qResp); jErr == nil {
				execQty, _ := strconv.ParseFloat(qResp.ExecutedQty, 64)
				isTerminalZero := (qResp.Status == "EXPIRED" || qResp.Status == "EXPIRED_IN_MATCH" || qResp.Status == "REJECTED" || qResp.Status == "CANCELED") && execQty <= 0
				if isTerminalZero {
					return nil, postIdempConfirmedNotFound
				}
			}
			return checkBody, postIdempConfirmedExists
		}
		var apiErr *APIError
		if errors.As(checkErr, &apiErr) && apiErr.Code == -2013 {
			return nil, postIdempConfirmedNotFound
		}
	}

	return nil, postIdempUnknown
}
