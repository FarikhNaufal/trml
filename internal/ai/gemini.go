package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

const maxGeminiResponseBytes = 2 << 20 // 2 MB cap against oversized responses

type AIResponse struct {
	Text   string `json:"text,omitempty"`
	Error  string `json:"error,omitempty"` // "GEMINI_UNAVAILABLE", "RATE_LIMITED", "INVALID_PIN", etc.
	Source string `json:"source"`          // "live", "cache", "fallback"
	Model  string `json:"model,omitempty"`
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
	timeout := time.Duration(timeoutSec) * time.Second
	return &GeminiClient{
		apiKey:  strings.TrimSpace(apiKey),
		model:   strings.TrimSpace(model),
		timeout: timeout,
		httpClient: &http.Client{
			// Let callCtx enforce the exact timeout cleanly before client-level ceiling fires.
			Timeout: timeout + 5*time.Second,
		},
	}
}

// Model returns the configured Gemini model name.
func (c *GeminiClient) Model() string {
	if c == nil || c.model == "" {
		return "gemini-2.5-flash"
	}
	return c.model
}

func (c *GeminiClient) IsConfigured() bool {
	return c != nil && c.apiKey != ""
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
	runes := []rune(cleaned)
	if len(runes) > 250 {
		cleaned = string(runes[:250])
	}
	return cleaned
}

func redactSecret(s, secret string) string {
	if secret != "" {
		s = strings.ReplaceAll(s, secret, "[REDACTED]")
	}
	return s
}

func isTimeoutError(err error, callCtx context.Context) bool {
	if errors.Is(err, context.DeadlineExceeded) || (callCtx != nil && errors.Is(callCtx.Err(), context.DeadlineExceeded)) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
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
			Model:  c.Model(),
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
			Model:  c.Model(),
		}
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", c.model)

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
		httpReq.Header.Set("x-goog-api-key", c.apiKey)

		resp, lastErr = c.httpClient.Do(httpReq)
		if lastErr == nil && resp.StatusCode < 500 {
			break retryLoop
		}

		if attempt == 1 {
			if lastErr == nil && resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			errSummary := ""
			if lastErr != nil {
				errSummary = redactSecret(lastErr.Error(), c.apiKey)
			}
			slog.Warn("ai_gemini: transient error, retrying in 250ms", "attempt", attempt, "err", errSummary)
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
			"err", redactSecret(lastErr.Error(), c.apiKey),
		)
		if isTimeoutError(lastErr, callCtx) {
			return AIResponse{Error: "GEMINI_TIMEOUT", Source: "fallback", Model: c.Model()}
		}
		return AIResponse{Error: "GEMINI_UNAVAILABLE", Source: "fallback", Model: c.Model()}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests {
		slog.Warn("ai_gemini: upstream rate limited", "status", resp.StatusCode)
		return AIResponse{Error: "RATE_LIMITED", Source: "fallback", Model: c.Model()}
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxGeminiResponseBytes))
	if err != nil {
		slog.Error("ai_gemini: failed to read response body", "err", redactSecret(err.Error(), c.apiKey))
		return AIResponse{Error: "INVALID_RESPONSE_BODY", Source: "fallback", Model: c.Model()}
	}

	var gResp geminiResponse
	unmarshalErr := json.Unmarshal(bodyBytes, &gResp)

	if gResp.Error != nil && gResp.Error.Code == http.StatusTooManyRequests {
		slog.Warn("ai_gemini: upstream rate limited in payload", "status", resp.StatusCode)
		return AIResponse{Error: "RATE_LIMITED", Source: "fallback", Model: c.Model()}
	}

	if resp.StatusCode != http.StatusOK {
		bodySnippet := redactSecret(string(bodyBytes), c.apiKey)
		if len(bodySnippet) > 512 {
			bodySnippet = bodySnippet[:512] + "..."
		}
		slog.Error("ai_gemini: unexpected HTTP status", "status", resp.StatusCode, "body", bodySnippet)
		return AIResponse{Error: fmt.Sprintf("HTTP_%d", resp.StatusCode), Source: "fallback", Model: c.Model()}
	}

	if unmarshalErr != nil {
		bodySnippet := redactSecret(string(bodyBytes), c.apiKey)
		if len(bodySnippet) > 512 {
			bodySnippet = bodySnippet[:512] + "..."
		}
		slog.Error("ai_gemini: failed to unmarshal response", "err", unmarshalErr, "body", bodySnippet)
		return AIResponse{Error: "INVALID_JSON_RESPONSE", Source: "fallback", Model: c.Model()}
	}

	if gResp.Error != nil {
		slog.Error("ai_gemini: API returned error",
			"code", gResp.Error.Code,
			"status", gResp.Error.Status,
			"message", redactSecret(gResp.Error.Message, c.apiKey),
		)
		return AIResponse{Error: "GEMINI_API_ERROR", Source: "fallback", Model: c.Model()}
	}

	if len(gResp.Candidates) == 0 || len(gResp.Candidates[0].Content.Parts) == 0 {
		slog.Warn("ai_gemini: empty candidate in response")
		return AIResponse{Error: "EMPTY_CANDIDATE_RESPONSE", Source: "fallback", Model: c.Model()}
	}

	var parts []string
	for _, p := range gResp.Candidates[0].Content.Parts {
		if trimmed := strings.TrimSpace(p.Text); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	outputText := strings.Join(parts, "\n")
	if outputText == "" {
		slog.Warn("ai_gemini: candidate contained only empty text parts")
		return AIResponse{Error: "EMPTY_CANDIDATE_RESPONSE", Source: "fallback", Model: c.Model()}
	}

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
		Model:  c.Model(),
	}
}
