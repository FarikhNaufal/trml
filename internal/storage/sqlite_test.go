package storage

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestSQLiteRepository(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite db: %v", err)
	}
	defer func() { _ = db.Close() }()

	repo := NewRepository(db)
	ctx := context.Background()

	// 1. Test Save & Get Daily State
	epochDay := int64(20743)
	startEq := decimal.NewFromFloat(10000.0)
	realEq := decimal.NewFromFloat(9850.0)

	err = repo.SaveDailyState(ctx, epochDay, startEq, realEq, false)
	if err != nil {
		t.Fatalf("failed to save daily state: %v", err)
	}

	gotStart, gotReal, tripped, found, err := repo.GetDailyState(ctx, epochDay)
	if err != nil || !found {
		t.Fatalf("failed to get daily state: found=%v, err=%v", found, err)
	}
	if !gotStart.Equal(startEq) || !gotReal.Equal(realEq) || tripped {
		t.Errorf("mismatch in daily state: got start=%s real=%s tripped=%v", gotStart, gotReal, tripped)
	}

	// 1b. Test Upsert Conflict: session_start_equity must NOT be overwritten, but realized and breaker must update
	updatedRealEq := decimal.NewFromFloat(9500.0)
	differentStartAttempt := decimal.NewFromFloat(9500.0) // should be ignored by ON CONFLICT
	err = repo.SaveDailyState(ctx, epochDay, differentStartAttempt, updatedRealEq, true)
	if err != nil {
		t.Fatalf("failed to upsert daily state: %v", err)
	}
	gotStart2, gotReal2, tripped2, found2, err := repo.GetDailyState(ctx, epochDay)
	if err != nil || !found2 {
		t.Fatalf("failed to get daily state after upsert: %v", err)
	}
	if !gotStart2.Equal(startEq) {
		t.Fatalf("session_start_equity was wrongly overwritten on conflict: expected %s, got %s", startEq, gotStart2)
	}
	if !gotReal2.Equal(updatedRealEq) || !tripped2 {
		t.Fatalf("expected updated realEq=%s tripped=true, got realEq=%s tripped=%v", updatedRealEq, gotReal2, tripped2)
	}

	// 2. Test Log Trade
	now := time.Now().UTC()
	err = repo.LogTrade(
		ctx,
		"ORDER_BTC_1",
		"BTCUSDT",
		"LONG",
		decimal.NewFromFloat(1.5),
		decimal.NewFromFloat(65000.0),
		decimal.NewFromFloat(66500.0),
		decimal.NewFromFloat(64000.0),
		decimal.NewFromFloat(67000.0),
		decimal.NewFromFloat(2250.0),
		"TAKE_PROFIT",
		now.Add(-30*time.Minute),
		now,
		-0.005,
		0.023,
		101,
	)
	if err != nil {
		t.Fatalf("failed to log trade: %v", err)
	}

	var sigAuditID int64
	err = db.conn.QueryRowContext(ctx, "SELECT signal_audit_id FROM trade_ledger WHERE client_order_id = 'ORDER_BTC_1'").Scan(&sigAuditID)
	if err != nil || sigAuditID != 101 {
		t.Fatalf("expected signal_audit_id=101, got %d, err=%v", sigAuditID, err)
	}

	// 3. Test Log Signal Audit
	sigID, err := repo.LogSignalAudit(
		ctx,
		now,
		"ETHUSDT",
		"LONG",
		3500.0,
		3450.0,
		3480.0,
		45.0,
		1.85,
		0.45,
		0.0001,
		0.025,
		1.45,
		1.20,
		3499.5,
		3432.5,
		3633.5,
		"EXECUTED_LONG",
		"NONE",
		"NONE",
	)
	if err != nil {
		t.Fatalf("failed to log signal audit: %v", err)
	}
	if sigID <= 0 {
		t.Fatalf("expected positive sigID, got %d", sigID)
	}

	var spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice float64
	err = db.conn.QueryRowContext(ctx, "SELECT spread_pct, rvol, impulse_atr_mult, limit_price, sl_price, tp_price FROM signal_audit_log WHERE id = ?", sigID).Scan(&spreadPct, &rvol, &impulseATRMult, &limitPrice, &slPrice, &tpPrice)
	if err != nil {
		t.Fatalf("failed to query new columns: %v", err)
	}
	if spreadPct != 0.025 || rvol != 1.45 || impulseATRMult != 1.20 || limitPrice != 3499.5 || slPrice != 3432.5 || tpPrice != 3633.5 {
		t.Fatalf("unexpected values: spread=%.4f rvol=%.2f impulse=%.2f limit=%.2f sl=%.2f tp=%.2f", spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice)
	}
}
