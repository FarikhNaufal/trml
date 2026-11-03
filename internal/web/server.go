package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trml/internal/ai"
	"trml/internal/broker"
	"trml/internal/storage"
)

type ScannerCoinDTO struct {
	Symbol        string  `json:"symbol"`
	MarkPrice     float64 `json:"mark_price"`
	SessionVWAP   float64 `json:"session_vwap"`
	DistVWAPPct   float64 `json:"dist_vwap_pct"`
	SpreadPct     float64 `json:"spread_pct"`
	FundingRate   float64 `json:"funding_rate"`
	NATRPct       float64 `json:"natr_pct"`
	RVOL          float64 `json:"rvol"`
	LastUpdateUTC string  `json:"last_update_utc"`
}

type ServerConfig struct {
	Port              string
	Env               string
	MaxSlots          int
	MaxDailyDrawdown  float64
	Reader            *WebReader
	Broker            broker.ExecutionBroker
	GetScannerCoinsFn func() []ScannerCoinDTO
	IsCircuitBreaker  func() bool
	Repo              storage.Repository
	DashboardPIN      string
	GeminiAPIKey      string
	GeminiModel       string
	GeminiTimeoutSec  int
	AIRateLimitRPM    int
}

type Server struct {
	cfg         ServerConfig
	httpServer  *http.Server
	rateLimiter *IPRateLimiter
	gemini      *ai.GeminiClient
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Port == "" {
		cfg.Port = "2020"
	}
	if !strings.HasPrefix(cfg.Port, ":") {
		cfg.Port = ":" + cfg.Port
	}

	s := &Server{
		cfg:         cfg,
		rateLimiter: NewIPRateLimiter(cfg.AIRateLimitRPM, 3),
		gemini:      ai.NewGeminiClient(cfg.GeminiAPIKey, cfg.GeminiModel, cfg.GeminiTimeoutSec),
	}

	mux := http.NewServeMux()

	// 1. Auth & Status Endpoints
	mux.HandleFunc("/api/auth/status", s.handleAuthStatus)
	mux.HandleFunc("/api/auth/verify", s.handleVerifyPIN)

	// 2. Core Dashboard Endpoints (Protected by PIN if configured)
	mux.HandleFunc("/api/summary", s.requireAuth(s.handleSummary))
	mux.HandleFunc("/api/scanner", s.requireAuth(s.handleScanner))
	mux.HandleFunc("/api/trades", s.requireAuth(s.handleTrades))
	mux.HandleFunc("/api/signals", s.requireAuth(s.handleSignals))
	mux.HandleFunc("/health", s.handleHealth)

	// 3. AI Copilot Endpoints (Protected by PIN + Token Bucket Rate Limit)
	mux.HandleFunc("/api/ai/executive-summary", s.requireAuth(s.aiRateLimit(s.handleAIExecutiveSummary)))
	mux.HandleFunc("/api/ai/position-health", s.requireAuth(s.aiRateLimit(s.handleAIPositionHealth)))
	mux.HandleFunc("/api/ai/trade-autopsy", s.requireAuth(s.aiRateLimit(s.handleAITradeAutopsy)))
	mux.HandleFunc("/api/ai/pattern-miner", s.requireAuth(s.aiRateLimit(s.handleAIPatternMiner)))

	// 2. Static File Server (Embedded PWA Assets)
	subFS, err := fs.Sub(StaticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("failed to create sub filesystem for static: %w", err)
	}
	fileServer := http.FileServer(http.FS(subFS))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, ".js") || strings.HasSuffix(path, ".css") || path == "/" || path == "/index.html" {
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
		}
		fileServer.ServeHTTP(w, r)
	})

	s.httpServer = &http.Server{
		Addr:         cfg.Port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s, nil
}

func (s *Server) Start() error {
	slog.Info("web dashboard listening", "addr", s.httpServer.Addr, "env", s.cfg.Env)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down web dashboard server...")
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

	var equityF, balanceF float64
	var ddPct float64
	var activeSlots int
	var positions []broker.ActivePositionDTO

	if s.cfg.Broker != nil {
		eqDec := s.cfg.Broker.GetEffectiveTotalEquityUSDT()
		equityF, _ = eqDec.Float64()

		balDec := s.cfg.Broker.GetAvailableBalanceUSDT()
		balanceF, _ = balDec.Float64()

		ddPct = s.cfg.Broker.GetDailyTotalDrawdownPct()
		activeSlots = s.cfg.Broker.GetActiveSlotCount()
		positions = s.cfg.Broker.GetActivePositions()
	}

	stats, _ := s.cfg.Reader.GetOverallStats(ctx)

	circuitBreaker := false
	if s.cfg.IsCircuitBreaker != nil {
		circuitBreaker = s.cfg.IsCircuitBreaker()
	}

	resp := map[string]any{
		"equity":                 equityF,
		"balance":                balanceF,
		"daily_drawdown_pct":     ddPct,
		"max_daily_drawdown_pct": s.cfg.MaxDailyDrawdown,
		"active_slots":           activeSlots,
		"max_slots":              s.cfg.MaxSlots,
		"env":                    s.cfg.Env,
		"circuit_breaker":        circuitBreaker,
		"positions":              positions,
		"stats":                  stats,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScanner(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var coins []ScannerCoinDTO
	if s.cfg.GetScannerCoinsFn != nil {
		coins = s.cfg.GetScannerCoinsFn()
	}

	resp := map[string]any{
		"coins":          coins,
		"scanned_at_utc": time.Now().UTC().Format(time.RFC3339),
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleTrades(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	status := q.Get("status")

	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

	trades, total, err := s.cfg.Reader.GetTradesPaginated(ctx, page, limit, status)
	if err != nil {
		slog.Error("web server: failed to get trades", "err", err)
		http.Error(w, `{"error":"failed to query trades"}`, http.StatusInternalServerError)
		return
	}

	if limit < 1 || limit > 100 {
		limit = 10
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + limit - 1) / limit
	}

	resp := map[string]any{
		"trades":      trades,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSignals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	decision := q.Get("decision")
	symbol := q.Get("symbol")

	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

	signals, total, err := s.cfg.Reader.GetSignalsPaginated(ctx, page, limit, decision, symbol)
	if err != nil {
		slog.Error("web server: failed to get signals", "err", err)
		http.Error(w, `{"error":"failed to query signals"}`, http.StatusInternalServerError)
		return
	}

	if limit < 1 || limit > 100 {
		limit = 15
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + limit - 1) / limit
	}

	resp := map[string]any{
		"signals":     signals,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"pin_required": s.isPINConfigured(),
	})
}

func (s *Server) handleVerifyPIN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ip := s.rateLimiter.getClientIP(r)

	if blocked, wait := s.rateLimiter.IsBlocked(ip); blocked {
		seconds := int(wait.Seconds())
		if seconds < 1 {
			seconds = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error":   "TOO_MANY_FAILED_ATTEMPTS",
			"message": "Terlalu banyak percobaan PIN yang salah. Akses ditangguhkan selama 5 menit.",
		})
		return
	}

	var req struct {
		PIN string `json:"pin"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "BAD_REQUEST",
			"message": "Format request tidak valid.",
		})
		return
	}

	if !s.validatePIN(req.PIN) {
		isBlocked := s.rateLimiter.RecordFailedPIN(ip)
		if isBlocked {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{
				"error":   "TOO_MANY_FAILED_ATTEMPTS",
				"message": "Terlalu banyak percobaan PIN yang salah. Akses ditangguhkan selama 5 menit.",
			})
			return
		}
		writeJSON(w, http.StatusUnauthorized, map[string]string{
			"error":   "INVALID_PIN",
			"message": "PIN yang dimasukkan salah.",
		})
		return
	}

	s.rateLimiter.ResetFailedPIN(ip)
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"message": "PIN berhasil diverifikasi.",
	})
}

func (s *Server) handleAIExecutiveSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var stats StatsItem
	if s.cfg.Reader != nil {
		stats, _ = s.cfg.Reader.GetOverallStats(ctx)
	}

	var equityF float64
	dailyDrawdownPct := 0.0
	if s.cfg.Broker != nil {
		eqDec := s.cfg.Broker.GetEffectiveTotalEquityUSDT()
		equityF, _ = eqDec.Float64()
		dailyDrawdownPct = s.cfg.Broker.GetDailyTotalDrawdownPct()
	}

	maxDailyDrawdown := s.cfg.MaxDailyDrawdown
	if maxDailyDrawdown == 0 {
		maxDailyDrawdown = -0.03
	}

	// Ambil riwayat trade terakhir (kosongkan AIAutopsy agar hemat token prompt)
	var recentTrades []TradeItem
	if s.cfg.Reader != nil {
		recentTrades, _, _ = s.cfg.Reader.GetTradesPaginated(ctx, 1, 6, "ALL")
		for i := range recentTrades {
			recentTrades[i].AIAutopsy = ""
		}
	}

	// Agregasi sinyal komprehensif dari signal audit log
	var auditSummary SignalAuditSummary
	if s.cfg.Reader != nil {
		auditSummary, _ = s.cfg.Reader.GetSignalAuditSummary(ctx)
	}

	activeSlots := 0
	if s.cfg.Broker != nil {
		activeSlots = len(s.cfg.Broker.GetActivePositions())
	}
	maxSlots := s.cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}

	payload := map[string]any{
		"timestamp_utc": time.Now().UTC().Format(time.RFC3339),
		"metrics": map[string]any{
			"equity_usdt":            equityF,
			"total_closed_trades":    stats.TotalTrades,
			"net_pnl_usdt":           stats.TotalNetPnLUSDT,
			"win_rate_pct":           stats.WinRatePct,
			"adjusted_win_rate_pct":  stats.AdjustedWinRatePct,
			"realized_rr":            stats.RealizedRR,
			"blended_rr":             stats.BlendedRR,
			"profit_factor":          stats.ProfitFactor,
			"wins_tp":                stats.WinsCount,
			"full_wins":              stats.FullWinsCount,
			"partial_wins":           stats.PartialWinsCount,
			"break_even":             stats.BreakEvenCount,
			"losses_sl":              stats.LossesCount,
			"avg_win_usdt":           stats.AvgWinUSDT,
			"avg_full_win_usdt":      stats.AvgFullWinUSDT,
			"avg_loss_usdt":          stats.AvgLossUSDT,
			"active_slots":           activeSlots,
			"max_slots":              maxSlots,
			"daily_drawdown_pct":     dailyDrawdownPct,
			"max_daily_drawdown_pct": maxDailyDrawdown,
		},
		"signal_audit_summary": auditSummary,
		"recent_trades":        recentTrades,
	}

	aiResp := s.gemini.GenerateExecutiveDebrief(ctx, payload)
	writeJSON(w, http.StatusOK, aiResp)
}

func (s *Server) handleAIPositionHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	if symbol == "" {
		writeJSON(w, http.StatusBadRequest, ai.AIResponse{
			Error:  "SYMBOL_REQUIRED",
			Source: "fallback",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var targetPos *broker.ActivePositionDTO
	if s.cfg.Broker != nil {
		positions := s.cfg.Broker.GetActivePositions()
		for i := range positions {
			if positions[i].Symbol == symbol {
				targetPos = &positions[i]
				break
			}
		}
	}

	if targetPos == nil {
		writeJSON(w, http.StatusNotFound, ai.AIResponse{
			Error:  "POSITION_NOT_FOUND",
			Source: "fallback",
		})
		return
	}

	var sessionVWAP, distVWAPPct float64
	if s.cfg.GetScannerCoinsFn != nil {
		coins := s.cfg.GetScannerCoinsFn()
		for _, c := range coins {
			if c.Symbol == symbol {
				sessionVWAP = c.SessionVWAP
				distVWAPPct = c.DistVWAPPct
				break
			}
		}
	}

	// Kalkulasi matematis jarak persentase ke target TP dan batas SL
	var distToTPPct, distToSLPct, tpProgressPct float64
	if targetPos.EntryPrice > 0 && targetPos.MarkPrice > 0 {
		if strings.ToUpper(targetPos.Side) == "LONG" {
			if targetPos.TPPrice > 0 {
				distToTPPct = ((targetPos.TPPrice - targetPos.MarkPrice) / targetPos.MarkPrice) * 100.0
				totalTPDist := targetPos.TPPrice - targetPos.EntryPrice
				if totalTPDist > 0 {
					tpProgressPct = ((targetPos.MarkPrice - targetPos.EntryPrice) / totalTPDist) * 100.0
				}
			}
			if targetPos.SLPrice > 0 {
				distToSLPct = ((targetPos.MarkPrice - targetPos.SLPrice) / targetPos.MarkPrice) * 100.0
			}
		} else { // SHORT
			if targetPos.TPPrice > 0 {
				distToTPPct = ((targetPos.MarkPrice - targetPos.TPPrice) / targetPos.MarkPrice) * 100.0
				totalTPDist := targetPos.EntryPrice - targetPos.TPPrice
				if totalTPDist > 0 {
					tpProgressPct = ((targetPos.EntryPrice - targetPos.MarkPrice) / totalTPDist) * 100.0
				}
			}
			if targetPos.SLPrice > 0 {
				distToSLPct = ((targetPos.SLPrice - targetPos.MarkPrice) / targetPos.MarkPrice) * 100.0
			}
		}
	}

	payload := map[string]any{
		"symbol":              ai.SanitizePromptInput(targetPos.Symbol),
		"side":                targetPos.Side,
		"entry_price":         targetPos.EntryPrice,
		"mark_price":          targetPos.MarkPrice,
		"unrealized_pnl_usdt": targetPos.UnrealizedPnL,
		"unrealized_roe_pct":  targetPos.UnrealizedROEPct,
		"sl_price":            targetPos.SLPrice,
		"tp_price":            targetPos.TPPrice,
		"dist_to_tp_pct":      distToTPPct,
		"dist_to_sl_pct":      distToSLPct,
		"tp_progress_pct":     tpProgressPct,
		"is_break_even_moved": targetPos.IsBreakEven,
		"duration_sec":        targetPos.DurationSec,
		"session_vwap":        sessionVWAP,
		"dist_to_vwap_pct":    distVWAPPct,
	}

	aiResp := s.gemini.GeneratePositionHealth(ctx, payload)
	writeJSON(w, http.StatusOK, aiResp)
}

func (s *Server) handleAITradeAutopsy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	clientOrderID := strings.TrimSpace(r.URL.Query().Get("client_order_id"))
	if clientOrderID == "" {
		writeJSON(w, http.StatusBadRequest, ai.AIResponse{
			Error:  "CLIENT_ORDER_ID_REQUIRED",
			Source: "fallback",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	forceRefresh := r.URL.Query().Get("refresh") == "true"

	// 1. Cek cache di SQLite terlebih dahulu jika bukan force refresh
	if !forceRefresh && s.cfg.Repo != nil {
		if cachedAutopsy, err := s.cfg.Repo.GetTradeAutopsy(ctx, clientOrderID); err == nil && cachedAutopsy != "" {
			writeJSON(w, http.StatusOK, ai.AIResponse{
				Text:   cachedAutopsy,
				Source: "cache",
			})
			return
		}
	}

	// 2. Ambil detail trade dari reader
	if s.cfg.Reader == nil {
		writeJSON(w, http.StatusInternalServerError, ai.AIResponse{
			Error:  "READER_UNAVAILABLE",
			Source: "fallback",
		})
		return
	}

	trade, err := s.cfg.Reader.GetTradeByClientOrderID(ctx, clientOrderID)
	if err != nil || trade == nil {
		writeJSON(w, http.StatusNotFound, ai.AIResponse{
			Error:  "TRADE_NOT_FOUND",
			Source: "fallback",
		})
		return
	}

	// Jika di TradeItem sudah ada ai_autopsy dan bukan force refresh
	if !forceRefresh && trade.AIAutopsy != "" {
		writeJSON(w, http.StatusOK, ai.AIResponse{
			Text:   trade.AIAutopsy,
			Source: "cache",
		})
		return
	}

	payload := map[string]any{
		"client_order_id":  ai.SanitizePromptInput(trade.ClientOrderID),
		"symbol":           ai.SanitizePromptInput(trade.Symbol),
		"side":             trade.Side,
		"qty":              trade.Qty,
		"entry_price":      trade.EntryPrice,
		"exit_price":       trade.ExitPrice,
		"sl_price_initial": trade.SLPriceInitial,
		"tp_price_initial": trade.TPPriceInitial,
		"planned_sl_pct":   trade.PlannedSLPct,
		"planned_tp_pct":   trade.PlannedTPPct,
		"net_pnl_usdt":     trade.NetPnLUSDT,
		"roe_pct":          trade.ROEPct,
		"exit_reason":      ai.SanitizePromptInput(trade.ExitReason),
		"mae_pct":          trade.MAEPct,
		"mfe_pct":          trade.MFEPct,
		"duration_sec":     trade.DurationSec,
		"opened_at_utc":    trade.OpenedAtUTC,
		"closed_at_utc":    trade.ClosedAtUTC,
	}

	aiResp := s.gemini.GenerateTradeAutopsy(ctx, payload)

	// Simpan ke SQLite jika pemanggilan AI berhasil
	if aiResp.Text != "" && s.cfg.Repo != nil {
		if err := s.cfg.Repo.UpdateTradeAutopsy(ctx, clientOrderID, aiResp.Text); err != nil {
			slog.Warn("web server: failed to cache ai autopsy into sqlite", "clientOrderID", clientOrderID, "err", err)
		}
	}

	writeJSON(w, http.StatusOK, aiResp)
}

func (s *Server) handleAIPatternMiner(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()

	var stats StatsItem
	var auditSummary SignalAuditSummary
	var trades []TradeItem

	if s.cfg.Reader != nil {
		stats, _ = s.cfg.Reader.GetOverallStats(ctx)
		auditSummary, _ = s.cfg.Reader.GetSignalAuditSummary(ctx)
		trades, _ = s.cfg.Reader.GetTradesForPatternMiner(ctx, 50)
	}

	payload := map[string]any{
		"timestamp_utc": time.Now().UTC().Format(time.RFC3339),
		"overall_performance": map[string]any{
			"total_trades":          stats.TotalTrades,
			"net_pnl_usdt":          stats.TotalNetPnLUSDT,
			"win_rate_pct":          stats.WinRatePct,
			"adjusted_win_rate_pct": stats.AdjustedWinRatePct,
			"realized_rr":           stats.RealizedRR,
			"blended_rr":            stats.BlendedRR,
			"profit_factor":         stats.ProfitFactor,
			"wins_tp":               stats.WinsCount,
			"full_wins":             stats.FullWinsCount,
			"partial_wins":          stats.PartialWinsCount,
			"break_even":            stats.BreakEvenCount,
			"losses_sl":             stats.LossesCount,
			"avg_win_usdt":          stats.AvgWinUSDT,
			"avg_full_win_usdt":     stats.AvgFullWinUSDT,
			"avg_loss_usdt":         stats.AvgLossUSDT,
		},
		"signal_audit_summary": auditSummary,
		"historical_trades":    trades,
	}

	aiResp := s.gemini.GeneratePatternMiner(ctx, payload)
	writeJSON(w, http.StatusOK, aiResp)
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
