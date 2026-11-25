package web

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker"
	"trml/internal/model"
)

type mockBroker struct{}

func (m *mockBroker) SubmitLimitEntry(intent model.OrderIntent) (bool, error) { return true, nil }
func (m *mockBroker) OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {
}
func (m *mockBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal {
	return decimal.NewFromFloat(1234.56)
}
func (m *mockBroker) GetAvailableBalanceUSDT() decimal.Decimal    { return decimal.NewFromFloat(987.65) }
func (m *mockBroker) GetDailyTotalDrawdownPct() float64           { return -0.015 }
func (m *mockBroker) GetDrawdownFromPeakPct() float64             { return 0.0 }
func (m *mockBroker) IsCircuitBreakerTripped() bool               { return false }
func (m *mockBroker) GetActiveSlotCount() int                     { return 1 }
func (m *mockBroker) HasActiveOrderOrPosition(symbol string) bool { return symbol == "BTCUSDT" }
func (m *mockBroker) GetMonitoredSymbols() []string                 { return []string{"BTCUSDT"} }
func (m *mockBroker) CheckTimeouts(now time.Time)                 {}
func (m *mockBroker) GetActivePositions() []broker.ActivePositionDTO {
	return []broker.ActivePositionDTO{
		{
			Symbol:           "BTCUSDT",
			Side:             "LONG",
			StrategyID:       "VWAP_PULLBACK",
			Qty:              "0.05",
			EntryPrice:       65000,
			MarkPrice:        65500,
			SLPrice:          64000,
			TPPrice:          67000,
			UnrealizedPnL:    25.0,
			UnrealizedROEPct: 3.84,
			OpenTimeUTC:      time.Now().UTC().Format(time.RFC3339),
			DurationSec:      300,
			IsBreakEven:      true,
		},
	}
}

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}

	createSchema := `
	CREATE TABLE trade_ledger (
		client_order_id TEXT PRIMARY KEY,
		symbol TEXT NOT NULL,
		side TEXT NOT NULL,
		strategy_id TEXT NOT NULL DEFAULT 'VWAP_PULLBACK',
		qty TEXT NOT NULL,
		entry_price TEXT NOT NULL,
		exit_price TEXT NOT NULL,
		sl_price_initial TEXT NOT NULL,
		tp_price_initial TEXT NOT NULL,
		net_pnl_usdt TEXT NOT NULL,
		exit_reason TEXT NOT NULL,
		opened_at_utc TEXT NOT NULL,
		closed_at_utc TEXT NOT NULL,
		mae_pct REAL NOT NULL DEFAULT 0.0,
		mfe_pct REAL NOT NULL DEFAULT 0.0,
		signal_audit_id INTEGER NOT NULL DEFAULT 0,
		ai_autopsy TEXT NOT NULL DEFAULT '',
		created_at_utc TEXT NOT NULL
	);

	CREATE TABLE signal_audit_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		candle_open_time_utc TEXT NOT NULL,
		symbol TEXT NOT NULL,
		side TEXT NOT NULL,
		strategy_id TEXT NOT NULL DEFAULT 'VWAP_PULLBACK',
		close_price REAL NOT NULL,
		ema200 REAL NOT NULL,
		session_vwap REAL NOT NULL,
		atr14 REAL NOT NULL,
		bar_delta_zscore REAL NOT NULL,
		oi_delta_pct REAL NOT NULL,
		funding_rate REAL NOT NULL,
		spread_pct REAL NOT NULL,
		rvol REAL NOT NULL,
		impulse_atr_mult REAL NOT NULL,
		decision TEXT NOT NULL,
		reject_reason_code TEXT NOT NULL,
		reject_sub_reason TEXT NOT NULL,
		created_at_utc TEXT NOT NULL
	);
	`

	if _, err := db.Exec(createSchema); err != nil {
		t.Fatalf("failed to exec schema: %v", err)
	}

	now := time.Now().UTC()
	tOpen := now.Add(-10 * time.Minute).Format(time.RFC3339Nano)
	tClose := now.Format(time.RFC3339Nano)

	// Seed 2 trades
	_, err = db.Exec(`
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, mae_pct, mfe_pct, signal_audit_id, created_at_utc)
		VALUES 
		('ORD_1', 'ETHUSDT', 'LONG', '1.5', '3000', '3050', '2950', '3050', '75.0', 'TAKE_PROFIT', ?, ?, 0.2, 1.8, 101, ?),
		('ORD_2', 'SOLUSDT', 'SHORT', '10', '150', '153', '153', '140', '-30.0', 'STOP_LOSS', ?, ?, 2.1, 0.4, 102, ?);
	`, tOpen, tClose, tClose, tOpen, tClose, tClose)
	if err != nil {
		t.Fatalf("failed to seed trades: %v", err)
	}

	// Seed 2 signals
	_, err = db.Exec(`
		INSERT INTO signal_audit_log (candle_open_time_utc, symbol, side, close_price, ema200, session_vwap, atr14, bar_delta_zscore, oi_delta_pct, funding_rate, spread_pct, rvol, impulse_atr_mult, decision, reject_reason_code, reject_sub_reason, created_at_utc)
		VALUES
		(?, 'ETHUSDT', 'LONG', 3000, 2900, 2980, 25, 2.3, 0.02, 0.0001, 0.0002, 2.5, 1.2, 'APPROVED', '', '', ?),
		(?, 'BTCUSDT', 'SHORT', 65000, 66000, 65200, 300, -0.8, -0.01, 0.0001, 0.0001, 1.2, 0.7, 'REJECTED', 'GATE_DELTA_ZSCORE', 'Delta Z-Score -0.8 < 1.5', ?);
	`, tOpen, tClose, tOpen, tClose)
	if err != nil {
		t.Fatalf("failed to seed signals: %v", err)
	}

	return db, func() {
		_ = db.Close()
	}
}

func TestServerEndpoints(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	reader := &WebReader{db: db}
	b := &mockBroker{}

	server, err := NewServer(ServerConfig{
		Port:     "8080",
		Env:      "DRY-RUN",
		MaxSlots: 3,
		Reader:   reader,
		Broker:   b,
		GetScannerCoinsFn: func() []ScannerCoinDTO {
			return []ScannerCoinDTO{
				{
					Symbol:      "BTCUSDT",
					MarkPrice:   65000,
					SessionVWAP: 65100,
					DistVWAPPct: -0.15,
					RVOL:        2.1,
				},
			}
		},
		IsCircuitBreaker: func() bool { return false },
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// 1. Test Static Index.html
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /, got %d", rec.Code)
	}
	if !contains(rec.Body.String(), "TR<span>M</span>L FUTURES") {
		t.Errorf("index.html missing brand header")
	}

	// 2. Test Static CSS
	req = httptest.NewRequest("GET", "/style.css", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /style.css, got %d", rec.Code)
	}

	// 3. Test Static JS
	req = httptest.NewRequest("GET", "/app.js", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /app.js, got %d", rec.Code)
	}

	// 4. Test Health Endpoint
	req = httptest.NewRequest("GET", "/health", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /health, got %d", rec.Code)
	}

	// 5. Test Summary Endpoint
	req = httptest.NewRequest("GET", "/api/summary", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/summary, got %d", rec.Code)
	}
	var sumResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &sumResp); err != nil {
		t.Fatalf("failed to parse summary json: %v", err)
	}
	if sumResp["env"] != "DRY-RUN" {
		t.Errorf("expected env DRY-RUN, got %v", sumResp["env"])
	}
	if sumResp["equity"] != 1234.56 {
		t.Errorf("expected equity 1234.56, got %v", sumResp["equity"])
	}

	// 6. Test Trades Endpoint
	req = httptest.NewRequest("GET", "/api/trades?page=1&limit=10", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/trades, got %d", rec.Code)
	}
	var tradesResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &tradesResp); err != nil {
		t.Fatalf("failed to parse trades json: %v", err)
	}
	if tradesResp["total"].(float64) != 2 {
		t.Errorf("expected 2 trades, got %v", tradesResp["total"])
	}
	tradesList, ok := tradesResp["trades"].([]any)
	if !ok || len(tradesList) != 2 {
		t.Fatalf("expected 2 trade items, got %v", tradesList)
	}
	firstTrade := tradesList[0].(map[string]any)
	if firstTrade["roe_pct"] == nil || firstTrade["price_change_pct"] == nil {
		t.Errorf("expected roe_pct and price_change_pct to be present, got %v", firstTrade)
	}
	if firstTrade["strategy_id"] != "VWAP_PULLBACK" {
		t.Errorf("expected strategy_id VWAP_PULLBACK, got %v", firstTrade["strategy_id"])
	}

	// 7. Test Signals Endpoint
	req = httptest.NewRequest("GET", "/api/signals?page=1&limit=10", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/signals, got %d", rec.Code)
	}
	var signalsResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &signalsResp); err != nil {
		t.Fatalf("failed to parse signals json: %v", err)
	}
	if signalsResp["total"].(float64) != 2 {
		t.Errorf("expected 2 signals, got %v", signalsResp["total"])
	}
	signalsList, ok := signalsResp["signals"].([]any)
	if !ok || len(signalsList) != 2 {
		t.Fatalf("expected 2 signal items, got %v", signalsList)
	}
	firstSig := signalsList[0].(map[string]any)
	if firstSig["strategy_id"] != "VWAP_PULLBACK" {
		t.Errorf("expected signal strategy_id VWAP_PULLBACK, got %v", firstSig["strategy_id"])
	}

	// 7b. Test Signals Filter by APPROVED
	req = httptest.NewRequest("GET", "/api/signals?page=1&limit=10&decision=APPROVED", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/signals?decision=APPROVED, got %d", rec.Code)
	}
	var appResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &appResp)
	if appResp["total"].(float64) != 1 {
		t.Errorf("expected 1 approved signal, got %v", appResp["total"])
	}

	// 7c. Test Signals Filter by REJECTED
	req = httptest.NewRequest("GET", "/api/signals?page=1&limit=10&decision=REJECTED", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/signals?decision=REJECTED, got %d", rec.Code)
	}
	var rejResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &rejResp)
	if rejResp["total"].(float64) != 1 {
		t.Errorf("expected 1 rejected signal, got %v", rejResp["total"])
	}

	// 8. Test Scanner Endpoint
	req = httptest.NewRequest("GET", "/api/scanner", nil)

	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/scanner, got %d", rec.Code)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || (len(s) > 0 && stringContains(s, substr)))
}

func stringContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestAuthAndAIEndpoints(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Insert test trade
	_, err := db.Exec(`
		INSERT INTO trade_ledger (
			client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial,
			net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, mae_pct, mfe_pct, signal_audit_id, ai_autopsy, created_at_utc
		) VALUES (
			'TRML_TEST_123', 'BTCUSDT', 'LONG', '0.05', '65000', '67000', '64000', '67000',
			'100.0', 'TAKE_PROFIT', '2026-10-25T10:00:00Z', '2026-10-25T10:30:00Z', 0.5, 3.2, 1, 'Cached autopsy text from DB', '2026-10-25T10:30:00Z'
		);
	`)
	if err != nil {
		t.Fatalf("failed to insert test trade: %v", err)
	}

	reader := &WebReader{db: db, leverage: 8.0}
	server, err := NewServer(ServerConfig{
		Port:         ":2020",
		Env:          "DRY-RUN",
		MaxSlots:     3,
		Reader:       reader,
		Broker:       &mockBroker{},
		DashboardPIN: "123456",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// 1. Check Auth Status (unprotected)
	req := httptest.NewRequest("GET", "/api/auth/status", nil)
	rec := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/auth/status, got %d", rec.Code)
	}
	var authStatus map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &authStatus)
	if authStatus["pin_required"] != true {
		t.Errorf("expected pin_required = true, got %v", authStatus["pin_required"])
	}

	// 2. Access summary without PIN -> should get 401
	req = httptest.NewRequest("GET", "/api/summary", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for /api/summary without PIN, got %d", rec.Code)
	}

	// 3. Verify PIN with wrong PIN -> should get 401
	verifyBody := `{"pin":"999999"}`
	req = httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(verifyBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong PIN, got %d", rec.Code)
	}

	// 4. Verify PIN with correct PIN -> should get 200
	verifyBody = `{"pin":"123456"}`
	req = httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(verifyBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for correct PIN, got %d", rec.Code)
	}

	// 5. Access summary with valid PIN header -> should get 200
	req = httptest.NewRequest("GET", "/api/summary", nil)
	req.Header.Set("X-TRML-PIN", "123456")
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 with X-TRML-PIN header, got %d", rec.Code)
	}

	// 6. Test Trade Autopsy reading from cache
	req = httptest.NewRequest("GET", "/api/ai/trade-autopsy?client_order_id=TRML_TEST_123", nil)
	req.Header.Set("X-TRML-PIN", "123456")
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for trade autopsy, got %d", rec.Code)
	}
	var autopsyResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &autopsyResp)
	if autopsyResp["source"] != "cache" {
		t.Errorf("expected source = cache, got %v", autopsyResp["source"])
	}
	if autopsyResp["text"] != "Cached autopsy text from DB" {
		t.Errorf("expected cached autopsy text, got %v", autopsyResp["text"])
	}

	// 6b. Test Pattern Miner endpoint (with PIN auth, fallback client)
	req = httptest.NewRequest("GET", "/api/ai/pattern-miner", nil)
	req.Header.Set("X-TRML-PIN", "123456")
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for pattern miner, got %d", rec.Code)
	}

	// 7. Test PIN lockout after 5 consecutive failed attempts
	testIP := "192.168.1.200:54321"
	for i := 0; i < 4; i++ {
		req = httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"pin":"000000"}`))
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = testIP
		rec = httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for attempt %d, got %d", i+1, rec.Code)
		}
	}
	// 5th attempt -> triggers block (HTTP 429)
	req = httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"pin":"000000"}`))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = testIP
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on 5th failed attempt, got %d", rec.Code)
	}

	// 6th attempt while blocked (even with correct PIN) -> must reject immediately with HTTP 429
	req = httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"pin":"123456"}`))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = testIP
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 while blocked even with correct PIN, got %d", rec.Code)
	}
}

func TestGetOverallStats_Categorization(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Clear table
	_, _ = db.Exec("DELETE FROM trade_ledger;")

	// Setup 1: Full Win (TP1 + Trailing Stop)
	_, err := db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES 
		('ORDER_1_TP1', 'BTCUSDT', 'LONG', '1', '100', '102', '98', '104', '0.50', 'PARTIAL_TAKE_PROFIT', '2026-11-03 10:00:00', '2026-11-03 10:05:00', 101, '2026-11-03 10:05:00'),
		('ORDER_1', 'BTCUSDT', 'LONG', '1', '100', '103', '98', '104', '1.00', 'TRAILING_STOP', '2026-11-03 10:00:00', '2026-11-03 10:10:00', 101, '2026-11-03 10:10:00');
	`)
	if err != nil {
		t.Fatalf("failed insert setup 1: %v", err)
	}

	// Setup 2: Partial Win (TP1 hit + Runner BE scratch)
	_, err = db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES 
		('ORDER_2_TP1', 'ETHUSDT', 'LONG', '1', '100', '102', '98', '104', '0.40', 'PARTIAL_TAKE_PROFIT', '2026-11-03 11:00:00', '2026-11-03 11:05:00', 102, '2026-11-03 11:05:00'),
		('ORDER_2', 'ETHUSDT', 'LONG', '1', '100', '100', '98', '104', '-0.00005', 'BREAK_EVEN', '2026-11-03 11:00:00', '2026-11-03 11:15:00', 102, '2026-11-03 11:15:00');
	`)
	if err != nil {
		t.Fatalf("failed insert setup 2: %v", err)
	}

	// Setup 3: Pure Break-Even (Rem Dini Tier 1 without reaching TP1)
	_, err = db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES 
		('ORDER_3', 'SOLUSDT', 'LONG', '1', '100', '100', '98', '104', '-0.00004', 'BREAK_EVEN', '2026-11-03 12:00:00', '2026-11-03 12:10:00', 103, '2026-11-03 12:10:00');
	`)
	if err != nil {
		t.Fatalf("failed insert setup 3: %v", err)
	}

	// Setup 4: Full Loss (Stop Loss)
	_, err = db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES 
		('ORDER_4', 'XRPUSDT', 'LONG', '1', '100', '98', '98', '104', '-1.80', 'STOP_LOSS', '2026-11-03 13:00:00', '2026-11-03 13:05:00', 104, '2026-11-03 13:05:00');
	`)
	if err != nil {
		t.Fatalf("failed insert setup 4: %v", err)
	}

	reader := &WebReader{db: db, leverage: 8.0}
	stats, err := reader.GetOverallStats(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats.TotalTrades != 4 {
		t.Errorf("expected 4 total trades, got %d", stats.TotalTrades)
	}
	if stats.WinsCount != 2 {
		t.Errorf("expected 2 total wins (1 full + 1 partial), got %d", stats.WinsCount)
	}
	if stats.FullWinsCount != 1 {
		t.Errorf("expected 1 full win, got %d", stats.FullWinsCount)
	}
	if stats.PartialWinsCount != 1 {
		t.Errorf("expected 1 partial win, got %d", stats.PartialWinsCount)
	}
	if stats.BreakEvenCount != 1 {
		t.Errorf("expected 1 pure break even, got %d", stats.BreakEvenCount)
	}
	if stats.LossesCount != 1 {
		t.Errorf("expected 1 loss, got %d", stats.LossesCount)
	}

	// Win Rate checks:
	// All-inclusive WR: 2 / 4 = 50.0%
	if math.Abs(stats.WinRatePct-50.0) > 0.01 {
		t.Errorf("expected all-inclusive WR 50.0%%, got %f", stats.WinRatePct)
	}
	// Pure trading Adjusted WR: 2 / (2 + 1) = 66.67%
	if math.Abs(stats.AdjustedWinRatePct-66.666) > 0.1 {
		t.Errorf("expected adjusted WR ~66.7%%, got %f", stats.AdjustedWinRatePct)
	}

	// Realized RR: Full Win (+$1.50) / Loss ($1.80) = 0.833
	if math.Abs(stats.RealizedRR-0.833) > 0.05 {
		t.Errorf("expected RealizedRR ~0.83, got %f", stats.RealizedRR)
	}

	// Blended RR: (1.50 + 0.40)/2 / 1.80 = 0.95 / 1.80 = 0.527
	if math.Abs(stats.BlendedRR-0.527) > 0.05 {
		t.Errorf("expected BlendedRR ~0.53, got %f", stats.BlendedRR)
	}
}

func TestGetOverallStats_AlgoTP1AndFallbackGrouping(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Clear table
	_, _ = db.Exec("DELETE FROM trade_ledger;")

	// Setup 1: Legacy live order with ALGO_TP1_ and ALGO_LIVE_ without signal_audit_id
	_, err := db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES
		('ALGO_TP1_BTC_123', 'BTCUSDT', 'LONG', '1', '100', '102', '98', '104', '0.50', 'PARTIAL_TAKE_PROFIT', '2026-11-03 10:00:00', '2026-11-03 10:05:00', 0, '2026-11-03 10:05:00'),
		('ALGO_LIVE_BTC_123', 'BTCUSDT', 'LONG', '1', '100', '103', '98', '104', '1.00', 'TRAILING_STOP', '2026-11-03 10:00:00', '2026-11-03 10:10:00', 0, '2026-11-03 10:10:00');
	`)
	if err != nil {
		t.Fatalf("failed insert setup 1: %v", err)
	}

	// Setup 2: Standard deterministic order with %_TP1 without signal_audit_id
	_, err = db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES
		('BOT_ETH_LONG_999_TP1', 'ETHUSDT', 'LONG', '1', '100', '102', '98', '104', '0.40', 'PARTIAL_TAKE_PROFIT', '2026-11-03 11:00:00', '2026-11-03 11:05:00', 0, '2026-11-03 11:05:00'),
		('BOT_ETH_LONG_999', 'ETHUSDT', 'LONG', '1', '100', '100', '98', '104', '-0.00005', 'BREAK_EVEN', '2026-11-03 11:00:00', '2026-11-03 11:15:00', 0, '2026-11-03 11:15:00');
	`)
	if err != nil {
		t.Fatalf("failed insert setup 2: %v", err)
	}

	reader := &WebReader{db: db, leverage: 8.0}
	stats, err := reader.GetOverallStats(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 4 ledger rows should map to exactly 2 trades
	if stats.TotalTrades != 2 {
		t.Errorf("expected 2 grouped trades, got %d", stats.TotalTrades)
	}
	if stats.WinsCount != 2 {
		t.Errorf("expected 2 wins, got %d", stats.WinsCount)
	}
	if stats.FullWinsCount != 1 {
		t.Errorf("expected 1 full win, got %d", stats.FullWinsCount)
	}
	if stats.PartialWinsCount != 1 {
		t.Errorf("expected 1 partial win, got %d", stats.PartialWinsCount)
	}
}

func TestStrategyStatsAndFiltering(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	_, _ = db.Exec("DELETE FROM trade_ledger;")
	_, _ = db.Exec("DELETE FROM signal_audit_log;")

	_, err := db.ExecContext(ctx, `
		INSERT INTO trade_ledger (client_order_id, symbol, side, strategy_id, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id, created_at_utc)
		VALUES
		('ORD_VWAP_1', 'BTCUSDT', 'LONG', 'VWAP_PULLBACK', '1', '100', '103', '98', '104', '3.0', 'TAKE_PROFIT', '2026-11-04 10:00:00', '2026-11-04 10:10:00', 201, '2026-11-04 10:10:00'),
		('ORD_SWEEP_1', 'SOLUSDT', 'SHORT', 'LIQUIDITY_SWEEP_SHORT', '2', '200', '195', '202', '192', '10.0', 'TRAILING_STOP', '2026-11-04 11:00:00', '2026-11-04 11:15:00', 202, '2026-11-04 11:15:00'),
		('ORD_SWEEP_2', 'ETHUSDT', 'SHORT', 'LIQUIDITY_SWEEP_SHORT', '1', '3000', '3005', '3005', '2980', '-5.0', 'STOP_LOSS', '2026-11-04 12:00:00', '2026-11-04 12:05:00', 203, '2026-11-04 12:05:00');
	`)
	if err != nil {
		t.Fatalf("failed seed trades: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO signal_audit_log (candle_open_time_utc, symbol, side, strategy_id, close_price, ema200, session_vwap, atr14, bar_delta_zscore, oi_delta_pct, funding_rate, spread_pct, rvol, impulse_atr_mult, decision, reject_reason_code, reject_sub_reason, created_at_utc)
		VALUES
		('2026-11-04T10:00:00Z', 'BTCUSDT', 'LONG', 'VWAP_PULLBACK', 100, 99, 100, 1, 2.0, 0.1, 0.0001, 0.01, 1.5, 1.0, 'APPROVE', 'SIGNAL_APPROVED', '', '2026-11-04T10:00:00Z'),
		('2026-11-04T11:00:00Z', 'SOLUSDT', 'SHORT', 'LIQUIDITY_SWEEP_SHORT', 200, 205, 201, 2, -0.8, 0.2, 0.0001, 0.01, 1.8, 0.0, 'APPROVE', 'SIGNAL_APPROVED', 'LIQUIDITY_SWEEP_SHORT', '2026-11-04T11:00:00Z');
	`)
	if err != nil {
		t.Fatalf("failed seed signals: %v", err)
	}

	reader := &WebReader{db: db, leverage: 8.0}
	server, err := NewServer(ServerConfig{
		Port:   "8080",
		Env:    "DRY-RUN",
		Reader: reader,
		Broker: &mockBroker{},
	})
	if err != nil {
		t.Fatalf("failed create server: %v", err)
	}

	// 1. Verify summary contains strategy_stats for both strategies
	req := httptest.NewRequest("GET", "/api/summary", nil)
	rec := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /api/summary, got %d", rec.Code)
	}
	var sumResp struct {
		StrategyStats []StrategyStatsItem `json:"strategy_stats"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &sumResp); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if len(sumResp.StrategyStats) != 2 {
		t.Fatalf("expected 2 strategy_stats entries, got %+v", sumResp.StrategyStats)
	}
	sweepStat := sumResp.StrategyStats[0] // LIQUIDITY_SWEEP_SHORT sorts first
	if sweepStat.StrategyID != "LIQUIDITY_SWEEP_SHORT" || sweepStat.TotalTrades != 2 || math.Abs(sweepStat.TotalNetPnLUSDT-5.0) > 0.01 || math.Abs(sweepStat.ProfitFactor-2.0) > 0.01 {
		t.Fatalf("unexpected sweep strategy stats: %+v", sweepStat)
	}

	// 2. Verify /api/trades?strategy=LIQUIDITY_SWEEP_SHORT filters cleanly
	req = httptest.NewRequest("GET", "/api/trades?page=1&limit=10&strategy=LIQUIDITY_SWEEP_SHORT", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /api/trades filter, got %d", rec.Code)
	}
	var trResp struct {
		Total  int         `json:"total"`
		Trades []TradeItem `json:"trades"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &trResp); err != nil {
		t.Fatalf("unmarshal trades: %v", err)
	}
	if trResp.Total != 2 || len(trResp.Trades) != 2 || trResp.Trades[0].StrategyID != "LIQUIDITY_SWEEP_SHORT" {
		t.Fatalf("unexpected filtered trades: %+v", trResp)
	}

	// 2b. Verify status=TAKE_PROFIT includes TRAILING_STOP (ORD_SWEEP_1) as well as TAKE_PROFIT (ORD_VWAP_1)
	req = httptest.NewRequest("GET", "/api/trades?page=1&limit=10&status=TAKE_PROFIT", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /api/trades?status=TAKE_PROFIT, got %d", rec.Code)
	}
	var tpFilterResp struct {
		Total int `json:"total"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &tpFilterResp)
	if tpFilterResp.Total != 2 {
		t.Fatalf("expected status=TAKE_PROFIT to include TRAILING_STOP and TAKE_PROFIT (2 rows), got %d", tpFilterResp.Total)
	}

	// 3. Verify /api/signals?strategy=LIQUIDITY_SWEEP_SHORT filters cleanly
	req = httptest.NewRequest("GET", "/api/signals?page=1&limit=10&strategy=LIQUIDITY_SWEEP_SHORT", nil)
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /api/signals filter, got %d", rec.Code)
	}
	var sgResp struct {
		Total   int          `json:"total"`
		Signals []SignalItem `json:"signals"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &sgResp); err != nil {
		t.Fatalf("unmarshal signals: %v", err)
	}
	if sgResp.Total != 1 || len(sgResp.Signals) != 1 || sgResp.Signals[0].StrategyID != "LIQUIDITY_SWEEP_SHORT" {
		t.Fatalf("unexpected filtered signals: %+v", sgResp)
	}
}

func TestSecurity_SpoofedProxyHeadersDoNotBypassPINLockout(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	reader := &WebReader{db: db, leverage: 8.0}
	server, err := NewServer(ServerConfig{
		Port:         ":2020",
		Env:          "DRY-RUN",
		Reader:       reader,
		Broker:       &mockBroker{},
		DashboardPIN: "123456",
	})
	if err != nil {
		t.Fatalf("failed create server: %v", err)
	}

	attackerAddr := "203.0.113.50:41234"
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"pin":"000000"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("CF-Connecting-IP", "1.1.1."+string(rune('1'+i)))
		req.Header.Set("X-Forwarded-For", "9.9.9."+string(rune('1'+i)))
		req.RemoteAddr = attackerAddr
		rec := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d expected 401, got %d", i+1, rec.Code)
		}
	}

	// 5th attempt with a fresh spoofed header must STILL trip lockout (429) for attackerAddr
	req := httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"pin":"000000"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("CF-Connecting-IP", "8.8.8.8")
	req.RemoteAddr = attackerAddr
	rec := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 lockout even with rotated spoofed proxy header, got %d", rec.Code)
	}

	// ?pin= query parameter must NOT be accepted for authentication
	req = httptest.NewRequest("GET", "/api/summary?pin=123456", nil)
	req.RemoteAddr = "203.0.113.99:1111"
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when passing PIN via query param, got %d", rec.Code)
	}

	// Oversized verify body must return 400
	huge := `{"pin":"` + strings.Repeat("1", 8192) + `"}`
	req = httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(huge))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "203.0.113.98:1111"
	rec = httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized verify payload, got %d", rec.Code)
	}

	// Valid PIN via header resets failed count on clean IP
	cleanAddr := "203.0.113.77:2222"
	for i := 0; i < 3; i++ {
		rFail := httptest.NewRequest("GET", "/api/summary", nil)
		rFail.Header.Set("X-TRML-PIN", "000000")
		rFail.RemoteAddr = cleanAddr
		recFail := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(recFail, rFail)
	}
	rOK := httptest.NewRequest("GET", "/api/summary", nil)
	rOK.Header.Set("X-TRML-PIN", "123456")
	rOK.RemoteAddr = cleanAddr
	recOK := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(recOK, rOK)
	if recOK.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid header PIN, got %d", recOK.Code)
	}
	for i := 0; i < 4; i++ {
		rFail := httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"pin":"000000"}`))
		rFail.Header.Set("Content-Type", "application/json")
		rFail.RemoteAddr = cleanAddr
		recFail := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(recFail, rFail)
		if recFail.Code != http.StatusUnauthorized {
			t.Fatalf("expected failed count reset after valid auth (attempt %d got %d)", i+1, recFail.Code)
		}
	}
}

func TestWebReader_ModeIsolation(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`
		CREATE TABLE trade_ledger (
			client_order_id TEXT PRIMARY KEY,
			mode TEXT NOT NULL DEFAULT '',
			symbol TEXT NOT NULL,
			side TEXT NOT NULL,
			strategy_id TEXT NOT NULL DEFAULT 'VWAP_PULLBACK',
			qty TEXT NOT NULL,
			entry_price TEXT NOT NULL,
			exit_price TEXT NOT NULL,
			sl_price_initial TEXT NOT NULL,
			tp_price_initial TEXT NOT NULL,
			net_pnl_usdt TEXT NOT NULL,
			exit_reason TEXT NOT NULL,
			opened_at_utc TEXT NOT NULL,
			closed_at_utc TEXT NOT NULL,
			mae_pct REAL NOT NULL DEFAULT 0.0,
			mfe_pct REAL NOT NULL DEFAULT 0.0,
			signal_audit_id INTEGER NOT NULL DEFAULT 0,
			ai_autopsy TEXT NOT NULL DEFAULT ''
		);
		INSERT INTO trade_ledger (client_order_id, mode, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id)
		VALUES
		('DRY_1', 'DRY_RUN', 'BTCUSDT', 'LONG', '1', '100', '105', '95', '105', '50.0', 'TAKE_PROFIT', '2026-11-01T10:00:00.000000000Z', '2026-11-01T10:15:00.000000000Z', 101),
		('LIVE_1', 'LIVE', 'ETHUSDT', 'SHORT', '2', '200', '190', '210', '190', '20.0', 'TAKE_PROFIT', '2026-11-01T11:00:00.000000000Z', '2026-11-01T11:20:00.000000000Z', 102);
	`)
	if err != nil {
		t.Fatalf("seed db: %v", err)
	}

	ctx := context.Background()
	liveReader := &WebReader{db: db, leverage: 8.0}
	liveReader.SetMode("LIVE")

	stats, err := liveReader.GetOverallStats(ctx)
	if err != nil {
		t.Fatalf("GetOverallStats(LIVE): %v", err)
	}
	if stats.TotalTrades != 1 || math.Abs(stats.TotalNetPnLUSDT-20.0) > 1e-6 {
		t.Fatalf("expected only LIVE trade ($20), got %+v", stats)
	}

	trades, total, err := liveReader.GetTradesPaginated(ctx, 1, 10, "ALL")
	if err != nil || total != 1 || len(trades) != 1 || trades[0].ClientOrderID != "LIVE_1" {
		t.Fatalf("expected only LIVE_1 in paginated trades, got total=%d trades=%+v err=%v", total, trades, err)
	}
	if tr, err := liveReader.GetTradeByClientOrderID(ctx, "DRY_1"); err != nil || tr != nil {
		t.Fatalf("expected DRY_1 hidden from LIVE reader, got tr=%+v err=%v", tr, err)
	}

	dryReader := &WebReader{db: db, leverage: 8.0}
	dryReader.SetMode("DRY_RUN")
	dryStats, err := dryReader.GetOverallStats(ctx)
	if err != nil || dryStats.TotalTrades != 1 || math.Abs(dryStats.TotalNetPnLUSDT-50.0) > 1e-6 {
		t.Fatalf("expected only DRY_RUN trade ($50), got %+v err=%v", dryStats, err)
	}
}

func TestServer_NilReaderClampedPageAndCachedAutopsyNoRateLimit(t *testing.T) {
	// 1. Nil Reader does not panic on /api/summary, /api/trades, /api/signals
	srvNil, err := NewServer(ServerConfig{
		Port:        "8080",
		Env:         "DRY-RUN",
		Broker:      &mockBroker{},
		GeminiModel: "gemini-2.5-pro",
	})
	if err != nil {
		t.Fatalf("NewServer nil reader: %v", err)
	}
	rec := httptest.NewRecorder()
	srvNil.httpServer.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/summary", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /api/summary with nil Reader, got %d", rec.Code)
	}
	var sumBody map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &sumBody)
	if sumBody["gemini_model"] != "gemini-2.5-pro" {
		t.Fatalf("expected gemini_model gemini-2.5-pro in /api/summary, got %v", sumBody["gemini_model"])
	}

	rec = httptest.NewRecorder()
	srvNil.httpServer.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/trades", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 /api/trades with nil Reader, got %d", rec.Code)
	}

	// 2. Clamped page=0 -> page=1 & cached trade autopsy bypasses AI RPM limit
	db, cleanup := setupTestDB(t)
	defer cleanup()
	_, _ = db.Exec(`
		INSERT INTO trade_ledger (
			client_order_id, symbol, side, qty, entry_price, exit_price, sl_price_initial, tp_price_initial,
			net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, mae_pct, mfe_pct, signal_audit_id, ai_autopsy, created_at_utc
		) VALUES (
			'CACHED_AUTOPSY_1', 'BTCUSDT', 'LONG', '0.05', '65000', '67000', '64000', '67000',
			'100.0', 'TAKE_PROFIT', '2026-10-25T10:00:00Z', '2026-10-25T10:30:00Z', 0.5, 3.2, 1, 'Precomputed cache', '2026-10-25T10:30:00Z'
		);
	`)
	srv, err := NewServer(ServerConfig{
		Port:           "8080",
		Env:            "DRY-RUN",
		Reader:         &WebReader{db: db, leverage: 8.0},
		Broker:         &mockBroker{},
		AIRateLimitRPM: 1, // burst=3
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	recPage := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(recPage, httptest.NewRequest("GET", "/api/trades?page=-2&limit=5", nil))
	var pageResp map[string]any
	_ = json.Unmarshal(recPage.Body.Bytes(), &pageResp)
	if pageResp["page"] != float64(1) {
		t.Fatalf("expected clamped page=1 for ?page=-2, got %v", pageResp["page"])
	}

	// Exhaust AI rate limit tokens (burst=3) on non-cached endpoint
	for i := 0; i < 4; i++ {
		r := httptest.NewRequest("GET", "/api/ai/executive-summary", nil)
		r.RemoteAddr = "198.51.100.10:1234"
		srv.httpServer.Handler.ServeHTTP(httptest.NewRecorder(), r)
	}

	// Cached trade-autopsy must still succeed with 200 (cache hits do NOT consume AI quota)
	rCache := httptest.NewRequest("GET", "/api/ai/trade-autopsy?client_order_id=CACHED_AUTOPSY_1", nil)
	rCache.RemoteAddr = "198.51.100.10:1234"
	recCache := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(recCache, rCache)
	if recCache.Code != http.StatusOK {
		t.Fatalf("expected cached trade autopsy to bypass AI rate limit, got HTTP %d: %s", recCache.Code, recCache.Body.String())
	}
}
