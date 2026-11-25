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
	var tradeStratID string
	err = db.conn.QueryRowContext(ctx, "SELECT signal_audit_id, strategy_id FROM trade_ledger WHERE client_order_id = 'ORDER_BTC_1'").Scan(&sigAuditID, &tradeStratID)
	if err != nil || sigAuditID != 101 || tradeStratID != "VWAP_PULLBACK" {
		t.Fatalf("expected signal_audit_id=101 strategy_id=VWAP_PULLBACK, got %d %q, err=%v", sigAuditID, tradeStratID, err)
	}

	// Verify explicit strategy_id persistence on LogTrade
	err = repo.LogTrade(
		ctx,
		"ORDER_SWEEP_1",
		"AVAXUSDT",
		"SHORT",
		decimal.NewFromFloat(5.0),
		decimal.NewFromFloat(180.0),
		decimal.NewFromFloat(175.0),
		decimal.NewFromFloat(183.0),
		decimal.NewFromFloat(172.0),
		decimal.NewFromFloat(25.0),
		"TAKE_PROFIT",
		now.Add(-15*time.Minute),
		now,
		-0.002,
		0.028,
		102,
		"LIQUIDITY_SWEEP_SHORT",
	)
	if err != nil {
		t.Fatalf("failed to log sweep trade: %v", err)
	}
	var sweepTradeStratID string
	err = db.conn.QueryRowContext(ctx, "SELECT strategy_id FROM trade_ledger WHERE client_order_id = 'ORDER_SWEEP_1'").Scan(&sweepTradeStratID)
	if err != nil || sweepTradeStratID != "LIQUIDITY_SWEEP_SHORT" {
		t.Fatalf("expected sweep trade strategy_id=LIQUIDITY_SWEEP_SHORT, got %q, err=%v", sweepTradeStratID, err)
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
	var sigStratID string
	err = db.conn.QueryRowContext(ctx, "SELECT spread_pct, rvol, impulse_atr_mult, limit_price, sl_price, tp_price, strategy_id FROM signal_audit_log WHERE id = ?", sigID).Scan(&spreadPct, &rvol, &impulseATRMult, &limitPrice, &slPrice, &tpPrice, &sigStratID)
	if err != nil {
		t.Fatalf("failed to query new columns: %v", err)
	}
	if spreadPct != 0.025 || rvol != 1.45 || impulseATRMult != 1.20 || limitPrice != 3499.5 || slPrice != 3432.5 || tpPrice != 3633.5 || sigStratID != "VWAP_PULLBACK" {
		t.Fatalf("unexpected values: spread=%.4f rvol=%.2f impulse=%.2f limit=%.2f sl=%.2f tp=%.2f strat=%q", spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice, sigStratID)
	}

	sweepSigID, err := repo.LogSignalAudit(
		ctx,
		now,
		"SOLUSDT",
		"SHORT",
		180.0,
		185.0,
		182.0,
		1.5,
		-1.10,
		0.20,
		0.0001,
		0.02,
		1.25,
		0.0,
		179.9,
		182.0,
		175.0,
		"EXECUTED_SHORT",
		"SIGNAL_APPROVED",
		"LIQUIDITY_SWEEP_SHORT",
		"LIQUIDITY_SWEEP_SHORT",
	)
	if err != nil {
		t.Fatalf("failed to log sweep signal audit: %v", err)
	}
	var sweepSigStratID string
	err = db.conn.QueryRowContext(ctx, "SELECT strategy_id FROM signal_audit_log WHERE id = ?", sweepSigID).Scan(&sweepSigStratID)
	if err != nil || sweepSigStratID != "LIQUIDITY_SWEEP_SHORT" {
		t.Fatalf("expected sweep signal strategy_id=LIQUIDITY_SWEEP_SHORT, got %q, err=%v", sweepSigStratID, err)
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
		StrategyID:       "LIQUIDITY_SWEEP_SHORT",
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
	if positions[0].Symbol != "NEARUSDT" || positions[0].ClientOrderID != "SIM-NEARUSDT-12345" || positions[0].SignalAuditID != 4369 || positions[0].StrategyID != "LIQUIDITY_SWEEP_SHORT" {
		t.Fatalf("unexpected position data: %+v", positions[0])
	}
	if !positions[0].PeakPrice.Equal(decimal.NewFromFloat(4.4200)) {
		t.Fatalf("expected dryrun PeakPrice 4.4200, got %s", positions[0].PeakPrice)
	}

	// Update break-even, entry fee and price (e.g. after further fills)
	pos1.CurrentSLPrice = decimal.NewFromFloat(4.4550)
	pos1.IsBreakEvenMoved = true
	pos1.EntryFee = decimal.NewFromFloat(0.0450)
	pos1.EntryPrice = decimal.NewFromFloat(4.5100)
	if err := repo.SaveDryRunPosition(ctx, pos1); err != nil {
		t.Fatalf("failed to upsert dry-run position: %v", err)
	}

	positionsAfterUpdate, _ := repo.GetAllDryRunPositions(ctx)
	if len(positionsAfterUpdate) != 1 || !positionsAfterUpdate[0].IsBreakEvenMoved || !positionsAfterUpdate[0].CurrentSLPrice.Equal(decimal.NewFromFloat(4.4550)) {
		t.Fatalf("upsert failed: %+v", positionsAfterUpdate[0])
	}
	if !positionsAfterUpdate[0].EntryFee.Equal(decimal.NewFromFloat(0.0450)) || !positionsAfterUpdate[0].EntryPrice.Equal(decimal.NewFromFloat(4.5100)) {
		t.Fatalf("upsert entry_fee or entry_price failed: %+v", positionsAfterUpdate[0])
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
		StrategyID:     "LIQUIDITY_SWEEP_SHORT",
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
	if livePositions[0].Symbol != "BTCUSDT" || livePositions[0].SLOrderID != 1234567 || !livePositions[0].SLPrice.Equal(decimal.NewFromFloat(81500.0)) || livePositions[0].StrategyID != "LIQUIDITY_SWEEP_SHORT" {
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

func TestCorruptDecimalFailClosed(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite db: %v", err)
	}
	defer func() { _ = db.Close() }()

	repo := NewRepository(db)
	ctx := context.Background()

	// Insert row with corrupt tp_price
	_, err = db.conn.ExecContext(ctx, `
		INSERT INTO dryrun_open_positions (
			client_order_id, symbol, side, qty, initial_qty, entry_price, entry_fee,
			current_sl_price, initial_sl_price, tp_price, atr_at_entry, last_mark_price,
			is_tp1_hit, is_breakeven_moved, protection_tier, signal_audit_id,
			max_mae, max_mfe, peak_price, mark_updated_at_utc, opened_at_utc
		) VALUES (
			'ORDER_CORRUPT', 'ETHUSDT', 'LONG', '1.0', '1.0', '2000.0', '1.0',
			'1950.0', '1950.0', 'NOT_A_DECIMAL', '10.0', '2000.0',
			0, 0, 0, 0,
			0, 0, '2000.0', datetime('now'), datetime('now')
		)
	`)
	if err != nil {
		t.Fatalf("failed to insert test corrupt row: %v", err)
	}

	_, err = repo.GetAllDryRunPositions(ctx)
	if err == nil {
		t.Fatalf("expected error loading positions with corrupt optional decimal, got nil")
	}
}

func TestModeIsolatedDailyStateAndFundingPersistence(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	repo := NewRepository(db)
	ctx := context.Background()
	epochDay := int64(20800)

	// Legacy row written for DRY_RUN
	if err := repo.SaveDailyState(ctx, epochDay-1, decimal.NewFromFloat(140.0), decimal.NewFromFloat(145.0), false); err != nil {
		t.Fatalf("legacy SaveDailyState failed: %v", err)
	}

	// LIVE must NOT inherit legacy/DRY_RUN state
	if _, _, _, found, err := repo.GetDailyStateForMode(ctx, "LIVE", epochDay-1); err != nil || found {
		t.Fatalf("expected LIVE mode to stay isolated from legacy DRY_RUN row, got found=%v err=%v", found, err)
	}
	if _, _, _, _, found, err := repo.GetLatestDailyStateForMode(ctx, "LIVE"); err != nil || found {
		t.Fatalf("expected LIVE latest state to stay isolated from legacy DRY_RUN row, got found=%v err=%v", found, err)
	}

	// DRY_RUN should seamlessly fall back to legacy row
	startDR, realDR, _, foundDR, err := repo.GetDailyStateForMode(ctx, "DRY_RUN", epochDay-1)
	if err != nil || !foundDR || !startDR.Equal(decimal.NewFromFloat(140.0)) || !realDR.Equal(decimal.NewFromFloat(145.0)) {
		t.Fatalf("expected DRY_RUN legacy fallback 140/145, got start=%s real=%s found=%v err=%v", startDR, realDR, foundDR, err)
	}

	// Writing both modes for the same epochDay stays completely independent
	if err := repo.SaveDailyStateForMode(ctx, "DRY_RUN", epochDay, decimal.NewFromFloat(150.0), decimal.NewFromFloat(152.0), false); err != nil {
		t.Fatalf("SaveDailyStateForMode DRY_RUN failed: %v", err)
	}
	if err := repo.SaveDailyStateForMode(ctx, "LIVE", epochDay, decimal.NewFromFloat(500.0), decimal.NewFromFloat(490.0), true); err != nil {
		t.Fatalf("SaveDailyStateForMode LIVE failed: %v", err)
	}

	sDR, rDR, bDR, okDR, _ := repo.GetDailyStateForMode(ctx, "DRY_RUN", epochDay)
	sLV, rLV, bLV, okLV, _ := repo.GetDailyStateForMode(ctx, "LIVE", epochDay)
	if !okDR || !okLV || !sDR.Equal(decimal.NewFromFloat(150.0)) || !rDR.Equal(decimal.NewFromFloat(152.0)) || bDR {
		t.Fatalf("unexpected DRY_RUN state: start=%s real=%s breaker=%v ok=%v", sDR, rDR, bDR, okDR)
	}
	if !sLV.Equal(decimal.NewFromFloat(500.0)) || !rLV.Equal(decimal.NewFromFloat(490.0)) || !bLV {
		t.Fatalf("unexpected LIVE state: start=%s real=%s breaker=%v", sLV, rLV, bLV)
	}

	// Out-of-order SaveDailyStateForMode with breakerTripped=false on the SAME UTC day must NOT clear sticky breaker
	if err := repo.SaveDailyStateForMode(ctx, "LIVE", epochDay, decimal.NewFromFloat(500.0), decimal.NewFromFloat(492.0), false); err != nil {
		t.Fatalf("SaveDailyStateForMode LIVE sticky check failed: %v", err)
	}
	_, _, bLVAfter, _, _ := repo.GetDailyStateForMode(ctx, "LIVE", epochDay)
	if !bLVAfter {
		t.Fatalf("expected circuit_breaker_hit to remain sticky (true) on same UTC day after out-of-order false update")
	}

	// DryRun position funding round-trip
	now := time.Now().UTC()
	if err := repo.SaveDryRunPosition(ctx, StoredDryRunPosition{
		ClientOrderID:    "SIM-FUND-1",
		Symbol:           "SOLUSDT",
		Side:             "LONG",
		StrategyID:       "VWAP_PULLBACK",
		Qty:              decimal.NewFromFloat(2.0),
		InitialQty:       decimal.NewFromFloat(2.0),
		EntryPrice:       decimal.NewFromFloat(100.0),
		EntryFee:         decimal.NewFromFloat(0.04),
		CurrentSLPrice:   decimal.NewFromFloat(97.0),
		InitialSLPrice:   decimal.NewFromFloat(97.0),
		TPPrice:          decimal.NewFromFloat(106.0),
		ATRAtEntry:       decimal.NewFromFloat(1.5),
		LastMarkPrice:    decimal.NewFromFloat(101.0),
		PeakPrice:        decimal.NewFromFloat(101.5),
		MarkUpdatedAt:    now,
		OpenedAt:         now,
		FundingPaid:      decimal.NewFromFloat(0.1234),
		LastFundingEpoch: 60000,
	}); err != nil {
		t.Fatalf("SaveDryRunPosition failed: %v", err)
	}
	loaded, err := repo.GetAllDryRunPositions(ctx)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("GetAllDryRunPositions failed: len=%d err=%v", len(loaded), err)
	}
	if !loaded[0].FundingPaid.Equal(decimal.NewFromFloat(0.1234)) || loaded[0].LastFundingEpoch != 60000 {
		t.Fatalf("funding persistence mismatch: got FundingPaid=%s LastFundingEpoch=%d", loaded[0].FundingPaid, loaded[0].LastFundingEpoch)
	}

	// Late SignalAuditID attachment on SaveDryRunPosition conflict update
	if err := repo.SaveDryRunPosition(ctx, StoredDryRunPosition{
		ClientOrderID:    "SIM-FUND-1",
		Symbol:           "SOLUSDT",
		Side:             "LONG",
		StrategyID:       "VWAP_PULLBACK",
		Qty:              decimal.NewFromFloat(2.0),
		InitialQty:       decimal.NewFromFloat(2.0),
		EntryPrice:       decimal.NewFromFloat(100.0),
		EntryFee:         decimal.NewFromFloat(0.04),
		CurrentSLPrice:   decimal.NewFromFloat(97.0),
		InitialSLPrice:   decimal.NewFromFloat(97.0),
		TPPrice:          decimal.NewFromFloat(106.0),
		ATRAtEntry:       decimal.NewFromFloat(1.5),
		LastMarkPrice:    decimal.NewFromFloat(101.0),
		PeakPrice:        decimal.NewFromFloat(101.5),
		MarkUpdatedAt:    now,
		OpenedAt:         now,
		SignalAuditID:    777,
		FundingPaid:      decimal.NewFromFloat(0.1234),
		LastFundingEpoch: 60000,
	}); err != nil {
		t.Fatalf("SaveDryRunPosition update SignalAuditID failed: %v", err)
	}
	loadedAfterAudit, _ := repo.GetAllDryRunPositions(ctx)
	if len(loadedAfterAudit) != 1 || loadedAfterAudit[0].SignalAuditID != 777 {
		t.Fatalf("expected updated SignalAuditID=777 persisted on conflict update, got %+v", loadedAfterAudit)
	}

	// Live position TP1OrderID / TP1ClientOrderID round-trip
	if err := repo.SaveLivePosition(ctx, StoredLivePosition{
		Symbol:           "ETHUSDT",
		Side:             "LONG",
		StrategyID:       "VWAP_PULLBACK",
		Qty:              decimal.NewFromFloat(1.5),
		InitialQty:       decimal.NewFromFloat(1.5),
		EntryPrice:       decimal.NewFromFloat(3000.0),
		SLOrderID:        1001,
		SLClientAlgoID:   "ALGO_SL_ETH",
		SLPrice:          decimal.NewFromFloat(2950.0),
		InitialSLPrice:   decimal.NewFromFloat(2950.0),
		TPPrice:          decimal.NewFromFloat(3100.0),
		TP1Price:         decimal.NewFromFloat(3050.0),
		TP1OrderID:       445566,
		TP1ClientOrderID: "ALGO_TP1_ETH_1",
		OpenedAt:         now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("SaveLivePosition failed: %v", err)
	}
	liveLoaded, err := repo.GetAllLivePositions(ctx)
	if err != nil || len(liveLoaded) != 1 {
		t.Fatalf("GetAllLivePositions failed: len=%d err=%v", len(liveLoaded), err)
	}
	if liveLoaded[0].TP1OrderID != 445566 || liveLoaded[0].TP1ClientOrderID != "ALGO_TP1_ETH_1" {
		t.Fatalf("expected TP1OrderID=445566 TP1ClientOrderID=ALGO_TP1_ETH_1, got %+v", liveLoaded[0])
	}
}

func TestGetPeakEquityForMode_And_GetRecentStopLossTimes(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := NewRepository(db)
	ctx := context.Background()

	_ = repo.SaveDailyStateForMode(ctx, "DRY_RUN", 20740, decimal.NewFromFloat(100.0), decimal.NewFromFloat(140.0), false)
	_ = repo.SaveDailyStateForMode(ctx, "DRY_RUN", 20741, decimal.NewFromFloat(140.0), decimal.NewFromFloat(120.0), false)
	_ = repo.SaveDailyStateForMode(ctx, "LIVE", 20740, decimal.NewFromFloat(500.0), decimal.NewFromFloat(480.0), false)

	dryPeak, foundDry, err := repo.GetPeakEquityForMode(ctx, "DRY_RUN")
	if err != nil || !foundDry || !dryPeak.Equal(decimal.NewFromFloat(140.0)) {
		t.Fatalf("expected DRY_RUN peak=140.0 found=true, got peak=%s found=%v err=%v", dryPeak, foundDry, err)
	}
	livePeak, foundLive, err := repo.GetPeakEquityForMode(ctx, "LIVE")
	if err != nil || !foundLive || !livePeak.Equal(decimal.NewFromFloat(500.0)) {
		t.Fatalf("expected LIVE peak=500.0 found=true, got peak=%s found=%v err=%v", livePeak, foundLive, err)
	}

	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	qtyOne := decimal.NewFromInt(1)
	_ = repo.LogTradeForMode(ctx, "LIVE", "T1", "BTCUSDT", "SHORT", qtyOne, decimal.NewFromFloat(100), decimal.NewFromFloat(95), decimal.NewFromFloat(105), decimal.NewFromFloat(90), decimal.NewFromFloat(5), "TAKE_PROFIT", now.Add(-50*time.Minute), now.Add(-20*time.Minute), 0, 0, 1)
	_ = repo.LogTradeForMode(ctx, "LIVE", "T2", "ETHUSDT", "SHORT", qtyOne, decimal.NewFromFloat(100), decimal.NewFromFloat(102), decimal.NewFromFloat(102), decimal.NewFromFloat(95), decimal.NewFromFloat(-2), "STOP_LOSS", now.Add(-55*time.Minute), now.Add(-40*time.Minute), 0, 0, 2)
	_ = repo.LogTradeForMode(ctx, "LIVE", "T3", "SOLUSDT", "SHORT", qtyOne, decimal.NewFromFloat(100), decimal.NewFromFloat(102), decimal.NewFromFloat(102), decimal.NewFromFloat(95), decimal.NewFromFloat(-2), "STOP_LOSS", now.Add(-30*time.Minute), now.Add(-10*time.Minute), 0, 0, 3)
	_ = repo.LogTradeForMode(ctx, "DRY_RUN", "T_DRY", "DOGEUSDT", "SHORT", qtyOne, decimal.NewFromFloat(100), decimal.NewFromFloat(102), decimal.NewFromFloat(102), decimal.NewFromFloat(95), decimal.NewFromFloat(-2), "STOP_LOSS", now.Add(-25*time.Minute), now.Add(-8*time.Minute), 0, 0, 5)
	_ = repo.LogTradeForMode(ctx, "LIVE", "T4", "XRPUSDT", "SHORT", qtyOne, decimal.NewFromFloat(100), decimal.NewFromFloat(99), decimal.NewFromFloat(102), decimal.NewFromFloat(95), decimal.NewFromFloat(1), "PARTIAL_TAKE_PROFIT", now.Add(-20*time.Minute), now.Add(-5*time.Minute), 0, 0, 4)

	slLive, err := repo.GetRecentStopLossTimesForMode(ctx, "LIVE", now.Add(-60*time.Minute))
	if err != nil {
		t.Fatalf("GetRecentStopLossTimesForMode(LIVE) failed: %v", err)
	}
	if len(slLive) != 2 || !slLive[0].Equal(now.Add(-40*time.Minute)) || !slLive[1].Equal(now.Add(-10*time.Minute)) {
		t.Fatalf("expected 2 LIVE STOP_LOSS timestamps ascending (-40m, -10m), got %v", slLive)
	}
	slDry, err := repo.GetRecentStopLossTimesForMode(ctx, "DRY_RUN", now.Add(-60*time.Minute))
	if err != nil || len(slDry) != 1 || !slDry[0].Equal(now.Add(-8*time.Minute)) {
		t.Fatalf("expected 1 DRY_RUN STOP_LOSS (-8m), got %v err=%v", slDry, err)
	}
}

func TestSQLiteTimestampFixedWidthOrdering_ModeExits_AndLiveOpenedAtUpdate(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer func() { _ = db.Close() }()
	repo := NewRepository(db)
	ctx := context.Background()

	// 1. Boundary second lexicographical ordering test: .000000000Z < .100000000Z < .199999999Z
	baseSec := time.Date(2026, 11, 25, 15, 0, 0, 0, time.UTC)
	t0 := baseSec                                         // 15:00:00.000000000Z
	t1 := baseSec.Add(100 * time.Millisecond)             // 15:00:00.100000000Z (RFC3339Nano ".1Z" used to sort AFTER .199Z!)
	t2 := baseSec.Add(199999999 * time.Nanosecond)        // 15:00:00.199999999Z
	qtyOne := decimal.NewFromInt(1)

	_ = repo.LogTradeForMode(ctx, "LIVE", "E_EARLY", "ETHUSDT", "SHORT", qtyOne, decimal.NewFromInt(100), decimal.NewFromInt(102), decimal.NewFromInt(102), decimal.NewFromInt(95), decimal.NewFromInt(-2), "STOP_LOSS", t0.Add(-5*time.Minute), t1, 0, 0, 1)
	_ = repo.LogTradeForMode(ctx, "LIVE", "E_LATER", "ETHUSDT", "SHORT", qtyOne, decimal.NewFromInt(100), decimal.NewFromInt(95), decimal.NewFromInt(102), decimal.NewFromInt(95), decimal.NewFromInt(5), "TAKE_PROFIT", t0.Add(-4*time.Minute), t2, 0, 0, 2)
	// Same timestamp PARTIAL_TAKE_PROFIT + STOP_LOSS -> outer join must ignore PARTIAL_TAKE_PROFIT
	_ = repo.LogTradeForMode(ctx, "LIVE", "SOL_SL", "SOLUSDT", "SHORT", qtyOne, decimal.NewFromInt(100), decimal.NewFromInt(102), decimal.NewFromInt(102), decimal.NewFromInt(95), decimal.NewFromInt(-2), "STOP_LOSS", t0.Add(-5*time.Minute), t2, 0, 0, 3)
	_ = repo.LogTradeForMode(ctx, "LIVE", "SOL_SL_TP1", "SOLUSDT", "SHORT", qtyOne, decimal.NewFromInt(100), decimal.NewFromInt(98), decimal.NewFromInt(102), decimal.NewFromInt(95), decimal.NewFromInt(2), "PARTIAL_TAKE_PROFIT", t0.Add(-5*time.Minute), t2, 0, 0, 3)
	// DRY_RUN exit must NOT leak into GetRecentExitsForMode(LIVE)
	_ = repo.LogTradeForMode(ctx, "DRY_RUN", "DRY_DOGE", "DOGEUSDT", "SHORT", qtyOne, decimal.NewFromInt(100), decimal.NewFromInt(102), decimal.NewFromInt(102), decimal.NewFromInt(95), decimal.NewFromInt(-2), "STOP_LOSS", t0.Add(-5*time.Minute), t2, 0, 0, 4)

	liveExits, err := repo.GetRecentExitsForMode(ctx, "LIVE", baseSec.Add(150*time.Millisecond))
	if err != nil {
		t.Fatalf("GetRecentExitsForMode(LIVE) failed: %v", err)
	}
	if _, leaked := liveExits["DOGEUSDT"]; leaked {
		t.Fatalf("expected DRY_RUN exit NOT to leak into LIVE exits, got %+v", liveExits)
	}
	if rec, ok := liveExits["ETHUSDT"]; !ok || rec.ExitReason != "TAKE_PROFIT" || !rec.ClosedAt.Equal(t2) {
		t.Fatalf("expected ETHUSDT latest exit at t2 TAKE_PROFIT, got %+v ok=%v", rec, ok)
	}
	if rec, ok := liveExits["SOLUSDT"]; !ok || rec.ExitReason != "STOP_LOSS" {
		t.Fatalf("expected SOLUSDT exit STOP_LOSS (ignoring same-ts PARTIAL_TAKE_PROFIT), got %+v ok=%v", rec, ok)
	}

	slTimes, err := repo.GetRecentStopLossTimesForMode(ctx, "LIVE", baseSec.Add(50*time.Millisecond))
	if err != nil || len(slTimes) != 2 || !slTimes[0].Equal(t1) || !slTimes[1].Equal(t2) {
		t.Fatalf("expected 2 SL timestamps ordered [t1, t2], got %v err=%v", slTimes, err)
	}

	// 2. SaveLivePosition ON CONFLICT DO UPDATE must update opened_at_utc
	oldOpen := baseSec.Add(-48 * time.Hour)
	newOpen := baseSec
	_ = repo.SaveLivePosition(ctx, StoredLivePosition{
		Symbol: "AVAXUSDT", Side: "LONG", Qty: qtyOne, EntryPrice: decimal.NewFromInt(20),
		SLPrice: decimal.NewFromInt(19), OpenedAt: oldOpen, UpdatedAt: oldOpen,
	})
	_ = repo.SaveLivePosition(ctx, StoredLivePosition{
		Symbol: "AVAXUSDT", Side: "LONG", Qty: qtyOne, EntryPrice: decimal.NewFromInt(21),
		SLPrice: decimal.NewFromInt(20), OpenedAt: newOpen, UpdatedAt: newOpen,
	})
	loaded, err := repo.GetAllLivePositions(ctx)
	if err != nil || len(loaded) != 1 || !loaded[0].OpenedAt.Equal(newOpen) {
		t.Fatalf("expected SaveLivePosition conflict update to update OpenedAt to %v, got %+v err=%v", newOpen, loaded, err)
	}
}
