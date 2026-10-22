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

	"trml/internal/broker"
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
	Reader            *WebReader
	Broker            broker.ExecutionBroker
	GetScannerCoinsFn func() []ScannerCoinDTO
	IsCircuitBreaker  func() bool
}

type Server struct {
	cfg        ServerConfig
	httpServer *http.Server
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if !strings.HasPrefix(cfg.Port, ":") {
		cfg.Port = ":" + cfg.Port
	}

	s := &Server{
		cfg: cfg,
	}

	mux := http.NewServeMux()

	// 1. API Endpoints
	mux.HandleFunc("/api/summary", s.handleSummary)
	mux.HandleFunc("/api/scanner", s.handleScanner)
	mux.HandleFunc("/api/trades", s.handleTrades)
	mux.HandleFunc("/api/signals", s.handleSignals)
	mux.HandleFunc("/health", s.handleHealth)

	// 2. Static File Server (Embedded PWA Assets)
	subFS, err := fs.Sub(StaticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("failed to create sub filesystem for static: %w", err)
	}
	fileServer := http.FileServer(http.FS(subFS))
	mux.Handle("/", fileServer)

	s.httpServer = &http.Server{
		Addr:         cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
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
		"equity":             equityF,
		"balance":            balanceF,
		"daily_drawdown_pct": ddPct,
		"active_slots":       activeSlots,
		"max_slots":          s.cfg.MaxSlots,
		"env":                s.cfg.Env,
		"circuit_breaker":    circuitBreaker,
		"positions":          positions,
		"stats":              stats,
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

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
