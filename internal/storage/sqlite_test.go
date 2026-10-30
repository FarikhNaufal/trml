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

	// 4. Test PruneOldSignalAuditLogs
	// Insert 1 old REJECT (10 days ago), 1 old APPROVE (10 days ago), 1 recent REJECT (1 day ago)
	tenDaysAgo := time.Now().UTC().AddDate(0, 0, -10).Format(time.RFC3339Nano)
	oneDayAgo := time.Now().UTC().AddDate(0, 0, -1).Format(time.RFC3339Nano)

	_, _ = db.conn.ExecContext(ctx, `INSERT INTO signal_audit_log (candle_open_time_utc, symbol, close_price, ema200, session_vwap, atr14, bar_delta_zscore, oi_delta_pct, funding_rate, decision, reject_reason_code, created_at_utc) VALUES (?, 'BTCUSDT', 60000, 60000, 60000, 100, 0.5, 0.01, 0.0001, 'REJECT', 'SPREAD_EXCEEDED', ?)`, tenDaysAgo, tenDaysAgo)
	_, _ = db.conn.ExecContext(ctx, `INSERT INTO signal_audit_log (candle_open_time_utc, symbol, close_price, ema200, session_vwap, atr14, bar_delta_zscore, oi_delta_pct, funding_rate, decision, reject_reason_code, created_at_utc) VALUES (?, 'BTCUSDT', 60000, 60000, 60000, 100, 0.5, 0.01, 0.0001, 'APPROVE', 'SIGNAL_APPROVED', ?)`, tenDaysAgo, tenDaysAgo)
	_, _ = db.conn.ExecContext(ctx, `INSERT INTO signal_audit_log (candle_open_time_utc, symbol, close_price, ema200, session_vwap, atr14, bar_delta_zscore, oi_delta_pct, funding_rate, decision, reject_reason_code, created_at_utc) VALUES (?, 'BTCUSDT', 60000, 60000, 60000, 100, 0.5, 0.01, 0.0001, 'REJECT', 'SPREAD_EXCEEDED', ?)`, oneDayAgo, oneDayAgo)

	pruned, err := repo.PruneOldSignalAuditLogs(ctx, 7)
	if err != nil {
		t.Fatalf("failed to prune old signals: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 old reject pruned, got %d", pruned)
	}

	var countOldApprove int
	_ = db.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM signal_audit_log WHERE decision = 'APPROVE'").Scan(&countOldApprove)
	if countOldApprove != 1 {
		t.Fatalf("old APPROVE row should not have been pruned, count=%d", countOldApprove)
	}

	// 5. Test Dry-Run Position Persistence (Save, GetAll, Upsert, Delete)
	pos1 := StoredDryRunPosition{
		ClientOrderID:    "SIM-NEARUSDT-12345",
		Symbol:           "NEARUSDT",
		Side:             "SHORT",
		Qty:              decimal.NewFromInt(39),
		EntryPrice:       decimal.NewFromFloat(4.4600),
		EntryFee:         decimal.NewFromFloat(0.08697),
		CurrentSLPrice:   decimal.NewFromFloat(4.5050),
		TPPrice:          decimal.NewFromFloat(4.3930),
		ATRAtEntry:       decimal.NewFromFloat(0.02967),
		LastMarkPrice:    decimal.NewFromFloat(4.4800),
		PeakPrice:        decimal.NewFromFloat(4.4200),
		MarkUpdatedAt:    now,
		OpenedAt:         now,
		IsBreakEvenMoved: false,
		SignalAuditID:    4369,
		MaxMAE:           0.45,
		MaxMFE:           0.10,
	}

	if err := repo.SaveDryRunPosition(ctx, pos1); err != nil {
		t.Fatalf("failed to save dry-run position: %v", err)
	}

	positions, err := repo.GetAllDryRunPositions(ctx)
	if err != nil {
		t.Fatalf("failed to get dry-run positions: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
	if positions[0].Symbol != "NEARUSDT" || positions[0].ClientOrderID != "SIM-NEARUSDT-12345" || positions[0].SignalAuditID != 4369 {
		t.Fatalf("unexpected position data: %+v", positions[0])
	}
	if !positions[0].PeakPrice.Equal(decimal.NewFromFloat(4.4200)) {
		t.Fatalf("expected dryrun PeakPrice 4.4200, got %s", positions[0].PeakPrice)
	}

	// Update break-even
	pos1.CurrentSLPrice = decimal.NewFromFloat(4.4550)
	pos1.IsBreakEvenMoved = true
	if err := repo.SaveDryRunPosition(ctx, pos1); err != nil {
		t.Fatalf("failed to upsert dry-run position: %v", err)
	}

	positionsAfterUpdate, _ := repo.GetAllDryRunPositions(ctx)
	if len(positionsAfterUpdate) != 1 || !positionsAfterUpdate[0].IsBreakEvenMoved || !positionsAfterUpdate[0].CurrentSLPrice.Equal(decimal.NewFromFloat(4.4550)) {
		t.Fatalf("upsert failed: %+v", positionsAfterUpdate[0])
	}

	// Delete
	if err := repo.DeleteDryRunPosition(ctx, "SIM-NEARUSDT-12345"); err != nil {
		t.Fatalf("failed to delete dry-run position: %v", err)
	}
	positionsAfterDelete, _ := repo.GetAllDryRunPositions(ctx)
	if len(positionsAfterDelete) != 0 {
		t.Fatalf("expected 0 positions after delete, got %d", len(positionsAfterDelete))
	}

	// 6. Test Live Position Persistence (TD-01)
	livePos := StoredLivePosition{
		Symbol:         "BTCUSDT",
		Side:           "SHORT",
		Qty:            decimal.NewFromFloat(0.05),
		EntryPrice:     decimal.NewFromFloat(81000.0),
		SLOrderID:      1234567,
		SLClientAlgoID: "ALGO_SL_BTCUSDT_1",
		SLPrice:        decimal.NewFromFloat(81500.0),
		TPOrderID:      7654321,
		TPClientAlgoID: "ALGO_TP_BTCUSDT_1",
		TPPrice:        decimal.NewFromFloat(80000.0),
		SignalAuditID:  9988,
		OpenedAt:       now,
		MaxMAE:         0.25,
		MaxMFE:         0.80,
		UpdatedAt:      now,
		PeakPrice:      decimal.NewFromFloat(80500.0),
	}

	if err := repo.SaveLivePosition(ctx, livePos); err != nil {
		t.Fatalf("failed to save live position: %v", err)
	}

	livePositions, err := repo.GetAllLivePositions(ctx)
	if err != nil {
		t.Fatalf("failed to get live positions: %v", err)
	}
	if len(livePositions) != 1 {
		t.Fatalf("expected 1 live position, got %d", len(livePositions))
	}
	if livePositions[0].Symbol != "BTCUSDT" || livePositions[0].SLOrderID != 1234567 || !livePositions[0].SLPrice.Equal(decimal.NewFromFloat(81500.0)) {
		t.Fatalf("unexpected live position data: %+v", livePositions[0])
	}
	if !livePositions[0].PeakPrice.Equal(decimal.NewFromFloat(80500.0)) {
		t.Fatalf("expected live PeakPrice 80500.0, got %s", livePositions[0].PeakPrice)
	}

	// 7. Test GetLatestDailyState (C-05)
	latestStart, latestReal, latestBreaker, latestEpoch, latestFound, err := repo.GetLatestDailyState(ctx)
	if err != nil || !latestFound {
		t.Fatalf("failed to get latest daily state: found=%v, err=%v", latestFound, err)
	}
	if latestEpoch != epochDay || !latestReal.Equal(updatedRealEq) || !latestBreaker {
		t.Fatalf("unexpected latest daily state: epoch=%d, realEq=%s, breaker=%v", latestEpoch, latestReal, latestBreaker)
	}
	_ = latestStart

	// Update live position (e.g. trailing SL move)
	livePos.SLPrice = decimal.NewFromFloat(80900.0)
	livePos.SLOrderID = 9999999
	if err := repo.SaveLivePosition(ctx, livePos); err != nil {
		t.Fatalf("failed to update live position: %v", err)
	}

	updatedLive, err := repo.GetAllLivePositions(ctx)
	if err != nil || len(updatedLive) != 1 {
		t.Fatalf("expected 1 updated live position, got %d, err=%v", len(updatedLive), err)
	}
	if !updatedLive[0].SLPrice.Equal(decimal.NewFromFloat(80900.0)) || updatedLive[0].SLOrderID != 9999999 {
		t.Fatalf("live position update failed: %+v", updatedLive[0])
	}

	// Delete live position
	if err := repo.DeleteLivePosition(ctx, "BTCUSDT"); err != nil {
		t.Fatalf("failed to delete live position: %v", err)
	}
	remainingLive, _ := repo.GetAllLivePositions(ctx)
	if len(remainingLive) != 0 {
		t.Fatalf("expected 0 live positions after delete, got %d", len(remainingLive))
	}

	// 8. Test GetRecentExits (anti-amnesia cooldown persistence across reboot)
	nowUTC := time.Now().UTC()
	// Trade 1: SOLUSDT full exit (STOP_LOSS) 5 mins ago -> should be returned
	_ = repo.LogTrade(ctx, "cid-sol-1", "SOLUSDT", "LONG", decimal.NewFromFloat(1), decimal.NewFromFloat(100), decimal.NewFromFloat(98), decimal.NewFromFloat(98), decimal.NewFromFloat(105), decimal.NewFromFloat(-2), "STOP_LOSS", nowUTC.Add(-10*time.Minute), nowUTC.Add(-5*time.Minute), 0.02, 0.0, 1)

	// Trade 2: ADAUSDT partial take profit 3 mins ago -> should NOT trigger full exit cooldown
	_ = repo.LogTrade(ctx, "cid-ada-tp1", "ADAUSDT", "LONG", decimal.NewFromFloat(1), decimal.NewFromFloat(1), decimal.NewFromFloat(1.05), decimal.NewFromFloat(0.95), decimal.NewFromFloat(1.10), decimal.NewFromFloat(0.05), "PARTIAL_TAKE_PROFIT", nowUTC.Add(-15*time.Minute), nowUTC.Add(-3*time.Minute), 0.0, 0.05, 2)

	// Trade 3: XRPUSDT exit 45 mins ago -> should be excluded by 30 min cutoff
	_ = repo.LogTrade(ctx, "cid-xrp-old", "XRPUSDT", "SHORT", decimal.NewFromFloat(1), decimal.NewFromFloat(0.5), decimal.NewFromFloat(0.52), decimal.NewFromFloat(0.52), decimal.NewFromFloat(0.46), decimal.NewFromFloat(-0.02), "STOP_LOSS", nowUTC.Add(-50*time.Minute), nowUTC.Add(-45*time.Minute), 0.04, 0.0, 3)

	cutoff := nowUTC.Add(-30 * time.Minute)
	recentExits, err := repo.GetRecentExits(ctx, cutoff)
	if err != nil {
		t.Fatalf("failed to query recent exits: %v", err)
	}
	if _, ok := recentExits["ADAUSDT"]; ok {
		t.Fatalf("expected ADAUSDT (PARTIAL_TAKE_PROFIT) to NOT be in recent exits, got: %+v", recentExits)
	}
	if _, ok := recentExits["XRPUSDT"]; ok {
		t.Fatalf("expected XRPUSDT (older than cutoff) to NOT be in recent exits, got: %+v", recentExits)
	}
	if solExit, ok := recentExits["SOLUSDT"]; !ok {
		t.Fatalf("expected SOLUSDT in recent exits, got: %+v", recentExits)
	} else if solExit.ClosedAt.Before(nowUTC.Add(-6*time.Minute)) || solExit.ClosedAt.After(nowUTC.Add(-4*time.Minute)) {
		t.Fatalf("unexpected SOLUSDT exit timestamp: %s", solExit.ClosedAt)
	} else if solExit.ExitReason != "STOP_LOSS" {
		t.Fatalf("expected SOLUSDT exit reason STOP_LOSS, got: %s", solExit.ExitReason)
	}
}
