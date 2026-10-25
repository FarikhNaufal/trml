package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
							{Text: "Analisa performa bot baik, eksekusi disiplin."},
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
	if !strings.Contains(resp.Text, "Analisa performa bot baik") {
		t.Errorf("unexpected output text: %s", resp.Text)
	}
	if resp.Source != "live" {
		t.Errorf("expected source 'live', got %s", resp.Source)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
