package data

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultVisionTimeout    = 45 * time.Second
	defaultRESTTimeout      = 20 * time.Second
	defaultRESTWeightPerMin = 900 // <= 37.5% of Binance's 2400 weight/min IP limit to protect live bot
	defaultMaxRetries       = 3
)

// BacktestHTTPConfig configures the shared HTTP client for Binance Vision S3 and optional REST API fallback.
type BacktestHTTPConfig struct {
	AllowRESTFallback   bool
	MaxRESTWeightPerMin int
	VisionTimeout       time.Duration
	RESTTimeout         time.Duration
	MaxRetries          int
}

// BacktestHTTPClient centralizes all historical backtest HTTP fetches with REST weight limiting,
// HTTP 429 Retry-After backoff, and fail-closed HTTP 418 IP ban circuit breaking.
type BacktestHTTPClient struct {
	visionClient      *http.Client
	restClient        *http.Client
	allowRESTFallback atomic.Bool
	maxRetries        int

	mu             sync.Mutex
	maxWeightPerMn int
	windowStart    time.Time
	usedWeight     int

	ipBanned    atomic.Bool
	bannedUntil atomic.Int64 // UnixNano
}

func NewBacktestHTTPClient(cfg BacktestHTTPConfig) *BacktestHTTPClient {
	vTimeout := cfg.VisionTimeout
	if vTimeout <= 0 {
		vTimeout = defaultVisionTimeout
	}
	rTimeout := cfg.RESTTimeout
	if rTimeout <= 0 {
		rTimeout = defaultRESTTimeout
	}
	maxWeight := cfg.MaxRESTWeightPerMin
	if maxWeight <= 0 {
		maxWeight = defaultRESTWeightPerMin
	}
	retries := cfg.MaxRetries
	if retries <= 0 {
		retries = defaultMaxRetries
	}

	c := &BacktestHTTPClient{
		visionClient:   &http.Client{Timeout: vTimeout},
		restClient:     &http.Client{Timeout: rTimeout},
		maxRetries:     retries,
		maxWeightPerMn: maxWeight,
		windowStart:    time.Now().UTC().Truncate(time.Minute),
	}
	c.allowRESTFallback.Store(cfg.AllowRESTFallback)
	return c
}

func (c *BacktestHTTPClient) SetAllowRESTFallback(allow bool) {
	c.allowRESTFallback.Store(allow)
}

func (c *BacktestHTTPClient) AllowRESTFallback() bool {
	return c.allowRESTFallback.Load()
}

func (c *BacktestHTTPClient) IsIPBanned() (bool, time.Time) {
	if !c.ipBanned.Load() {
		return false, time.Time{}
	}
	untilNano := c.bannedUntil.Load()
	if untilNano > 0 && time.Now().UTC().UnixNano() > untilNano {
		c.ipBanned.Store(false)
		return false, time.Time{}
	}
	if untilNano > 0 {
		return true, time.Unix(0, untilNano).UTC()
	}
	return true, time.Time{}
}

// GetVision downloads an archive or S3 listing from Binance Vision (no API weight cost).
// Returns (body, statusCode, error). HTTP 404 returns (nil, 404, nil) without retry.
func (c *BacktestHTTPClient) GetVision(ctx context.Context, rawURL string) ([]byte, int, error) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, 0, err
		}
		resp, err := c.visionClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries {
				if err := sleepWithCtx(ctx, backoffDuration(attempt)); err != nil {
					return nil, 0, err
				}
			}
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return nil, http.StatusNotFound, nil
		}
		if resp.StatusCode == http.StatusOK {
			if readErr != nil {
				lastErr = readErr
				if attempt < c.maxRetries {
					if err := sleepWithCtx(ctx, backoffDuration(attempt)); err != nil {
						return nil, 0, err
					}
				}
				continue
			}
			return body, http.StatusOK, nil
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("vision HTTP %d for %s", resp.StatusCode, rawURL)
			waitDur := parseRetryAfter(resp.Header.Get("Retry-After"), backoffDuration(attempt))
			if attempt < c.maxRetries {
				if err := sleepWithCtx(ctx, waitDur); err != nil {
					return nil, resp.StatusCode, err
				}
			}
			continue
		}

		return nil, resp.StatusCode, fmt.Errorf("vision unexpected status %d for %s", resp.StatusCode, rawURL)
	}
	return nil, 0, fmt.Errorf("vision request failed after retries: %w", lastErr)
}

// GetREST performs a rate-limited request to fapi.binance.com when permitted.
func (c *BacktestHTTPClient) GetREST(ctx context.Context, rawURL string, weight int, bypassFallbackGate bool) ([]byte, int, error) {
	if !bypassFallbackGate && !c.allowRESTFallback.Load() {
		return nil, 0, fmt.Errorf("REST fallback disabled (enable --allow-rest-fallback to query %s)", rawURL)
	}
	if weight <= 0 {
		weight = 1
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if banned, until := c.IsIPBanned(); banned {
			return nil, 418, fmt.Errorf("binance REST IP ban active until %s", until.Format(time.RFC3339))
		}
		if err := c.acquireRESTWeight(ctx, weight); err != nil {
			return nil, 0, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, 0, err
		}
		resp, err := c.restClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries {
				if err := sleepWithCtx(ctx, backoffDuration(attempt)); err != nil {
					return nil, 0, err
				}
			}
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if hdrWeight := resp.Header.Get("X-MBX-USED-WEIGHT-1M"); hdrWeight != "" {
			if serverWeight, convErr := strconv.Atoi(strings.TrimSpace(hdrWeight)); convErr == nil {
				c.syncServerWeight(serverWeight)
			}
		}

		if resp.StatusCode == 418 {
			c.tripIPBan(body)
			return nil, 418, fmt.Errorf("binance HTTP 418 IP ban triggered: %s", truncateString(string(body), 160))
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			waitDur := parseRetryAfter(resp.Header.Get("Retry-After"), backoffDuration(attempt)+2*time.Second)
			lastErr = fmt.Errorf("binance REST HTTP 429 rate limited")
			if attempt < c.maxRetries {
				if err := sleepWithCtx(ctx, waitDur); err != nil {
					return nil, 429, err
				}
			}
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("binance REST HTTP %d", resp.StatusCode)
			if attempt < c.maxRetries {
				if err := sleepWithCtx(ctx, backoffDuration(attempt)); err != nil {
					return nil, resp.StatusCode, err
				}
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, resp.StatusCode, fmt.Errorf("binance REST status %d: %s", resp.StatusCode, truncateString(string(body), 160))
		}
		if readErr != nil {
			lastErr = readErr
			if attempt < c.maxRetries {
				if err := sleepWithCtx(ctx, backoffDuration(attempt)); err != nil {
					return nil, 0, err
				}
			}
			continue
		}
		return body, http.StatusOK, nil
	}
	return nil, 0, fmt.Errorf("binance REST failed after retries: %w", lastErr)
}

func (c *BacktestHTTPClient) acquireRESTWeight(ctx context.Context, weight int) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.mu.Lock()
		now := time.Now().UTC()
		curMin := now.Truncate(time.Minute)
		if curMin.After(c.windowStart) {
			c.windowStart = curMin
			c.usedWeight = 0
		}
		if c.usedWeight+weight <= c.maxWeightPerMn {
			c.usedWeight += weight
			c.mu.Unlock()
			return nil
		}
		waitUntil := c.windowStart.Add(time.Minute + 150*time.Millisecond)
		c.mu.Unlock()

		waitDur := time.Until(waitUntil)
		if waitDur <= 0 {
			waitDur = 100 * time.Millisecond
		}
		if err := sleepWithCtx(ctx, waitDur); err != nil {
			return err
		}
	}
}

func (c *BacktestHTTPClient) syncServerWeight(serverWeight int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	curMin := time.Now().UTC().Truncate(time.Minute)
	if curMin.After(c.windowStart) {
		c.windowStart = curMin
		c.usedWeight = serverWeight
		return
	}
	if serverWeight > c.usedWeight {
		c.usedWeight = serverWeight
	}
}

func (c *BacktestHTTPClient) tripIPBan(body []byte) {
	// Default 5m safety ban if exact expiry timestamp isn't present in payload
	until := time.Now().UTC().Add(5 * time.Minute)
	msg := string(body)
	if idx := strings.Index(msg, "until "); idx != -1 {
		rest := msg[idx+len("until "):]
		fields := strings.FieldsFunc(rest, func(r rune) bool {
			return r < '0' || r > '9'
		})
		if len(fields) > 0 {
			if ms, err := strconv.ParseInt(fields[0], 10, 64); err == nil && ms > 1_000_000_000_000 {
				until = time.UnixMilli(ms).UTC()
			}
		}
	}
	// Publish deadline before flipping ipBanned so concurrent IsIPBanned() never observes stale expiry.
	c.bannedUntil.Store(until.UnixNano())
	c.ipBanned.Store(true)
}

func parseRetryAfter(val string, fallback time.Duration) time.Duration {
	if val == "" {
		return fallback
	}
	if sec, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && sec > 0 {
		return time.Duration(sec) * time.Second
	}
	return fallback
}

func backoffDuration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := 250 * time.Millisecond * time.Duration(1<<uint(attempt))
	if d > 8*time.Second {
		d = 8 * time.Second
	}
	return d
}

func sleepWithCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
