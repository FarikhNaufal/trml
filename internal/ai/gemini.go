package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type AIResponse struct {
	Text   string `json:"text,omitempty"`
	Error  string `json:"error,omitempty"` // "GEMINI_UNAVAILABLE", "RATE_LIMITED", "INVALID_PIN", etc.
	Source string `json:"source"`          // "live", "cache", "fallback"
}

type GeminiClient struct {
	apiKey     string
	model      string
	timeout    time.Duration
	httpClient *http.Client
}

func NewGeminiClient(apiKey, model string, timeoutSec int) *GeminiClient {
	if model == "" {
		model = "gemini-2.5-flash"
	}
	if timeoutSec <= 0 {
		timeoutSec = 25
	}
	return &GeminiClient{
		apiKey:  strings.TrimSpace(apiKey),
		model:   strings.TrimSpace(model),
		timeout: time.Duration(timeoutSec) * time.Second,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutSec) * time.Second,
		},
	}
}

func (c *GeminiClient) IsConfigured() bool {
	return c.apiKey != ""
}

func SanitizePromptInput(s string) string {
	cleaned := strings.Map(func(r rune) rune {
		if r < 32 && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, s)
	cleaned = strings.ReplaceAll(cleaned, "`", "\\`")
	cleaned = strings.ReplaceAll(cleaned, "{", "\\{")
	cleaned = strings.ReplaceAll(cleaned, "}", "\\}")
	if len(cleaned) > 250 {
		cleaned = cleaned[:250]
	}
	return cleaned
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

type geminiGenConfig struct {
	Temperature float64 `json:"temperature"`
}

type geminiRequest struct {
	Contents          []geminiContent  `json:"contents"`
	SystemInstruction *geminiContent   `json:"system_instruction,omitempty"`
	GenerationConfig  *geminiGenConfig `json:"generationConfig,omitempty"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	} `json:"candidates"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error,omitempty"`
	UsageMetadata *struct {
		PromptTokenCount     int `json:"promptTokenCount"`
		CandidatesTokenCount int `json:"candidatesTokenCount"`
		TotalTokenCount      int `json:"totalTokenCount"`
	} `json:"usageMetadata,omitempty"`
}

// GenerateContent memanggil Google Gemini API dengan timeout, retry 1x, dan logging terstruktur
func (c *GeminiClient) GenerateContent(ctx context.Context, endpointName, systemInstruction, userPrompt string) AIResponse {
	if !c.IsConfigured() {
		return AIResponse{
			Error:  "GEMINI_API_KEY_NOT_CONFIGURED",
			Source: "fallback",
		}
	}

	start := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{
				Role: "user",
				Parts: []geminiPart{
					{Text: userPrompt},
				},
			},
		},
		GenerationConfig: &geminiGenConfig{
			Temperature: 0.2,
		},
	}

	if systemInstruction != "" {
		reqBody.SystemInstruction = &geminiContent{
			Role: "system",
			Parts: []geminiPart{
				{Text: systemInstruction},
			},
		}
	}

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		slog.Error("ai_gemini: failed to marshal request", "err", err)
		return AIResponse{
			Error:  "INTERNAL_REQUEST_ERROR",
			Source: "fallback",
		}
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", c.model, c.apiKey)

	// Eksekusi HTTP request dengan 1x retry jika error jaringan atau 5xx
	var resp *http.Response
	var lastErr error

retryLoop:
	for attempt := 1; attempt <= 2; attempt++ {
		httpReq, reqErr := http.NewRequestWithContext(callCtx, http.MethodPost, url, bytes.NewReader(jsonBytes))
		if reqErr != nil {
			lastErr = reqErr
			break retryLoop
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, lastErr = c.httpClient.Do(httpReq)
		if lastErr == nil && resp.StatusCode < 500 {
			break retryLoop
		}

		if attempt == 1 {
			if lastErr == nil && resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			slog.Warn("ai_gemini: transient error, retrying in 250ms", "attempt", attempt, "err", lastErr)
			select {
			case <-callCtx.Done():
				lastErr = callCtx.Err()
				break retryLoop
			case <-time.After(250 * time.Millisecond):
			}
		}
	}

	elapsed := time.Since(start)

	if lastErr != nil {
		slog.Error("ai_gemini: request failed",
			"endpoint", endpointName,
			"model", c.model,
			"latency_ms", elapsed.Milliseconds(),
			"err", lastErr,
		)
		if strings.Contains(lastErr.Error(), "context deadline exceeded") {
			return AIResponse{Error: "GEMINI_TIMEOUT", Source: "fallback"}
		}
		return AIResponse{Error: "GEMINI_UNAVAILABLE", Source: "fallback"}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests {
		slog.Warn("ai_gemini: upstream rate limited", "status", resp.StatusCode)
		return AIResponse{Error: "RATE_LIMITED", Source: "fallback"}
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("ai_gemini: failed to read response body", "err", err)
		return AIResponse{Error: "INVALID_RESPONSE_BODY", Source: "fallback"}
	}

	var gResp geminiResponse
	if err := json.Unmarshal(bodyBytes, &gResp); err != nil {
		slog.Error("ai_gemini: failed to unmarshal response", "err", err, "body", string(bodyBytes))
		return AIResponse{Error: "INVALID_JSON_RESPONSE", Source: "fallback"}
	}

	if gResp.Error != nil {
		slog.Error("ai_gemini: API returned error",
			"code", gResp.Error.Code,
			"status", gResp.Error.Status,
			"message", gResp.Error.Message,
		)
		if gResp.Error.Code == 429 {
			return AIResponse{Error: "RATE_LIMITED", Source: "fallback"}
		}
		return AIResponse{Error: gResp.Error.Message, Source: "fallback"}
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("ai_gemini: unexpected HTTP status", "status", resp.StatusCode, "body", string(bodyBytes))
		return AIResponse{Error: fmt.Sprintf("HTTP_%d", resp.StatusCode), Source: "fallback"}
	}

	if len(gResp.Candidates) == 0 || len(gResp.Candidates[0].Content.Parts) == 0 {
		slog.Warn("ai_gemini: empty candidate in response", "body", string(bodyBytes))
		return AIResponse{Error: "EMPTY_CANDIDATE_RESPONSE", Source: "fallback"}
	}

	outputText := strings.TrimSpace(gResp.Candidates[0].Content.Parts[0].Text)

	var tokensIn, tokensOut int
	if gResp.UsageMetadata != nil {
		tokensIn = gResp.UsageMetadata.PromptTokenCount
		tokensOut = gResp.UsageMetadata.CandidatesTokenCount
	}

	slog.Info("ai_call",
		"endpoint", endpointName,
		"model", c.model,
		"latency_ms", elapsed.Milliseconds(),
		"tokens_in", tokensIn,
		"tokens_out", tokensOut,
		"cached", false,
		"status", "success",
	)

	return AIResponse{
		Text:   outputText,
		Source: "live",
	}
}
