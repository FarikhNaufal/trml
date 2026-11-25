package ai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSanitizePromptInput(t *testing.T) {
	input := "BTC`USDT\x00{hack}\n"
	sanitized := SanitizePromptInput(input)
	if strings.Contains(sanitized, "`") && !strings.Contains(sanitized, "\\`") {
		t.Errorf("expected backtick to be escaped, got %s", sanitized)
	}
	if strings.Contains(sanitized, "\x00") {
		t.Errorf("expected control characters to be stripped, got %s", sanitized)
	}

	// Multi-byte UTF-8 truncation must preserve valid rune boundaries.
	longUTF8 := strings.Repeat("牛来USDT_", 40)
	trunc := SanitizePromptInput(longUTF8)
	if !utf8.ValidString(trunc) || len([]rune(trunc)) != 250 {
		t.Fatalf("expected valid 250-rune UTF-8 string, valid=%v lenRunes=%d", utf8.ValidString(trunc), len([]rune(trunc)))
	}
}

func TestGeminiClientNotConfigured(t *testing.T) {
	client := NewGeminiClient("", "", 5)
	if client.IsConfigured() {
		t.Errorf("expected client not configured with empty api key")
	}

	resp := client.GenerateContent(context.Background(), "test", "", "hello")
	if resp.Error != "GEMINI_API_KEY_NOT_CONFIGURED" {
		t.Errorf("expected GEMINI_API_KEY_NOT_CONFIGURED, got %s", resp.Error)
	}
}

func TestGeminiClientMockSuccess(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "key=") {
			t.Errorf("expected API key NOT passed in URL query string, got %s", r.URL.RawQuery)
		}
		if r.Header.Get("x-goog-api-key") != "dummy_key" {
			t.Errorf("expected x-goog-api-key header 'dummy_key', got %q", r.Header.Get("x-goog-api-key"))
		}
		w.Header().Set("Content-Type", "application/json")
		resp := geminiResponse{
			Candidates: []struct {
				Content struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"content"`
				FinishReason string `json:"finishReason"`
			}{
				{
					Content: struct {
						Parts []struct {
							Text string `json:"text"`
						} `json:"parts"`
					}{
						Parts: []struct {
							Text string `json:"text"`
						}{
							{Text: "Analisa performa bot baik,"},
							{Text: "eksekusi disiplin."},
						},
					},
					FinishReason: "STOP",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockServer.Close()

	client := NewGeminiClient("dummy_key", "gemini-2.5-flash", 2)
	// Override httpClient to route to mock server
	client.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mockReq, _ := http.NewRequestWithContext(req.Context(), req.Method, mockServer.URL, req.Body)
			mockReq.Header = req.Header
			return http.DefaultClient.Do(mockReq)
		}),
		Timeout: 2 * time.Second,
	}

	resp := client.GenerateExecutiveDebrief(context.Background(), map[string]any{"trades": 5})
	if resp.Error != "" {
		t.Fatalf("expected no error, got %s", resp.Error)
	}
	if !strings.Contains(resp.Text, "Analisa performa bot baik,\neksekusi disiplin.") {
		t.Errorf("expected multi-part text joined, got %q", resp.Text)
	}
	if resp.Source != "live" || resp.Model != "gemini-2.5-flash" {
		t.Errorf("expected source 'live' and model 'gemini-2.5-flash', got source=%s model=%s", resp.Source, resp.Model)
	}
}

func TestGeminiClientErrorClassification(t *testing.T) {
	// 1. HTML 503 should return HTTP_503, not INVALID_JSON_RESPONSE
	htmlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("<html><body>Service Unavailable</body></html>"))
	}))
	defer htmlSrv.Close()

	c := NewGeminiClient("secret_key", "gemini-2.5-flash", 2)
	c.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mockReq, _ := http.NewRequestWithContext(req.Context(), req.Method, htmlSrv.URL, req.Body)
			mockReq.Header = req.Header
			return http.DefaultClient.Do(mockReq)
		}),
	}
	if r := c.GenerateContent(context.Background(), "test", "", "hi"); r.Error != "HTTP_503" {
		t.Fatalf("expected HTTP_503 on HTML 503, got %+v", r)
	}

	// 2. HTTP 429 should return RATE_LIMITED
	rlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer rlSrv.Close()
	c.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mockReq, _ := http.NewRequestWithContext(req.Context(), req.Method, rlSrv.URL, req.Body)
		return http.DefaultClient.Do(mockReq)
	})
	if r := c.GenerateContent(context.Background(), "test", "", "hi"); r.Error != "RATE_LIMITED" {
		t.Fatalf("expected RATE_LIMITED on HTTP 429, got %+v", r)
	}

	// 3. Timeout error (even via net.Error / Client.Timeout) should return GEMINI_TIMEOUT
	c.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, &netTimeoutErr{err: errors.New("net/http: request canceled (Client.Timeout exceeded)")}
	})
	if r := c.GenerateContent(context.Background(), "test", "", "hi"); r.Error != "GEMINI_TIMEOUT" {
		t.Fatalf("expected GEMINI_TIMEOUT, got %+v", r)
	}
}

type netTimeoutErr struct{ err error }

func (e *netTimeoutErr) Error() string   { return e.err.Error() }
func (e *netTimeoutErr) Timeout() bool   { return true }
func (e *netTimeoutErr) Temporary() bool { return true }

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
