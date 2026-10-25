package web

import (
	"database/sql"
	"encoding/json"
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
func (m *mockBroker) OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {}
func (m *mockBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal { return decimal.NewFromFloat(1234.56) }
func (m *mockBroker) GetAvailableBalanceUSDT() decimal.Decimal { return decimal.NewFromFloat(987.65) }
func (m *mockBroker) GetDailyTotalDrawdownPct() float64 { return -0.015 }
func (m *mockBroker) GetActiveSlotCount() int { return 1 }
func (m *mockBroker) HasActiveOrderOrPosition(symbol string) bool { return symbol == "BTCUSDT" }
func (m *mockBroker) CheckTimeouts(now time.Time) {}
func (m *mockBroker) GetActivePositions() []broker.ActivePositionDTO {
	return []broker.ActivePositionDTO{
		{
			Symbol:           "BTCUSDT",
			Side:             "LONG",
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
		qty TEXT NOT NULL,
		entry_price TEXT NOT NULL,
		exit_price TEXT NOT NULL,
		sl_price TEXT NOT NULL,
		tp_price TEXT NOT NULL,
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
		INSERT INTO trade_ledger (client_order_id, symbol, side, qty, entry_price, exit_price, sl_price, tp_price, net_pnl_usdt, exit_reason, opened_at_utc, closed_at_utc, mae_pct, mfe_pct, signal_audit_id, created_at_utc)
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
			client_order_id, symbol, side, qty, entry_price, exit_price, sl_price, tp_price,
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
}

