package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type StoredDryRunPosition struct {
	Symbol           string
	ClientOrderID    string
	Side             string
	StrategyID       string
	Qty              decimal.Decimal
	InitialQty       decimal.Decimal
	EntryPrice       decimal.Decimal
	EntryFee         decimal.Decimal
	CurrentSLPrice   decimal.Decimal
	InitialSLPrice   decimal.Decimal
	TPPrice          decimal.Decimal
	TP1Price         decimal.Decimal
	IsTP1Hit         bool
	ATRAtEntry       decimal.Decimal
	LastMarkPrice    decimal.Decimal
	MarkUpdatedAt    time.Time
	OpenedAt         time.Time
	IsBreakEvenMoved bool
	ProtectionTier   int
	SignalAuditID    int64
	MaxMAE           float64
	MaxMFE           float64
	PeakPrice        decimal.Decimal
	FundingPaid      decimal.Decimal
	LastFundingEpoch int64
}

type StoredLivePosition struct {
	Symbol         string
	Side           string
	StrategyID     string
	Qty            decimal.Decimal
	InitialQty     decimal.Decimal
	EntryPrice     decimal.Decimal
	SLOrderID      int64
	SLClientAlgoID string
	SLPrice        decimal.Decimal
	InitialSLPrice decimal.Decimal
	TPOrderID        int64
	TPClientAlgoID   string
	TPPrice          decimal.Decimal
	TP1Price         decimal.Decimal
	TP1OrderID       int64
	TP1ClientOrderID string
	IsTP1Hit         bool
	ATRAtEntry     decimal.Decimal
	ProtectionTier int
	SignalAuditID  int64
	OpenedAt       time.Time
	MaxMAE         float64
	MaxMFE         float64
	UpdatedAt      time.Time
	PeakPrice      decimal.Decimal
}

type RecentExitRecord struct {
	Symbol     string
	ClosedAt   time.Time
	ExitReason string
}

type Repository interface {
	SaveDailyState(ctx context.Context, epochDay int64, sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool) error
	GetDailyState(ctx context.Context, epochDay int64) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, found bool, err error)
	GetLatestDailyState(ctx context.Context) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, epochDay int64, found bool, err error)
	SaveDailyStateForMode(ctx context.Context, mode string, epochDay int64, sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool) error
	GetDailyStateForMode(ctx context.Context, mode string, epochDay int64) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, found bool, err error)
	GetLatestDailyStateForMode(ctx context.Context, mode string) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, epochDay int64, found bool, err error)
	LogTrade(ctx context.Context, clientOrderID, symbol, side string, qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal, exitReason string, openedAt, closedAt time.Time, mae, mfe float64, signalAuditID int64, strategyID ...string) error
	LogTradeForMode(ctx context.Context, mode, clientOrderID, symbol, side string, qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal, exitReason string, openedAt, closedAt time.Time, mae, mfe float64, signalAuditID int64, strategyID ...string) error
	LogSignalAudit(ctx context.Context, candleOpenTime time.Time, symbol, side string, closePrice, ema200, sessionVwap, atr14, barDeltaZScore, oiDeltaPct, fundingRate, spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice float64, decision, rejectReason, rejectSubReason string, strategyID ...string) (int64, error)
	PruneOldSignalAuditLogs(ctx context.Context, olderThanDays int) (int64, error)
	SaveDryRunPosition(ctx context.Context, pos StoredDryRunPosition) error
	DeleteDryRunPosition(ctx context.Context, symbol string) error
	GetAllDryRunPositions(ctx context.Context) ([]StoredDryRunPosition, error)
	SaveLivePosition(ctx context.Context, pos StoredLivePosition) error
	DeleteLivePosition(ctx context.Context, symbol string) error
	GetAllLivePositions(ctx context.Context) ([]StoredLivePosition, error)
	UpdateTradeAutopsy(ctx context.Context, clientOrderID, autopsy string) error
	GetTradeAutopsy(ctx context.Context, clientOrderID string) (string, error)
	GetRecentExits(ctx context.Context, since time.Time) (map[string]RecentExitRecord, error)
	GetRecentExitsForMode(ctx context.Context, mode string, since time.Time) (map[string]RecentExitRecord, error)
	GetPeakEquityForMode(ctx context.Context, mode string) (decimal.Decimal, bool, error)
	GetRecentStopLossTimes(ctx context.Context, since time.Time) ([]time.Time, error)
	GetRecentStopLossTimesForMode(ctx context.Context, mode string, since time.Time) ([]time.Time, error)
}

const sqliteTimeFormat = "2006-01-02T15:04:05.000000000Z"

func formatSQLiteTime(t time.Time) string {
	return t.UTC().Format(sqliteTimeFormat)
}

type SQLiteRepository struct {
	db *DB
}

func NewRepository(db *DB) *SQLiteRepository {
	return &SQLiteRepository{db: db}
}

func (r *SQLiteRepository) SaveDailyState(
	ctx context.Context,
	epochDay int64,
	sessionStartEquity, realizedEquity decimal.Decimal,
	breakerTripped bool,
) error {
	query := `
	INSERT INTO account_daily_state (
		epoch_day_utc, session_start_equity, realized_equity, circuit_breaker_hit, updated_at_utc
	) VALUES (?, ?, ?, ?, ?)
	ON CONFLICT(epoch_day_utc) DO UPDATE SET
		realized_equity = excluded.realized_equity,
		circuit_breaker_hit = MAX(account_daily_state.circuit_breaker_hit, excluded.circuit_breaker_hit),
		updated_at_utc = excluded.updated_at_utc;
	`
	breakerInt := 0
	if breakerTripped {
		breakerInt = 1
	}

	_, err := r.db.conn.ExecContext(
		ctx,
		query,
		epochDay,
		sessionStartEquity.String(),
		realizedEquity.String(),
		breakerInt,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("storage: failed to save daily state: %w", err)
	}
	return nil
}

func (r *SQLiteRepository) GetDailyState(
	ctx context.Context,
	epochDay int64,
) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, found bool, err error) {
	query := `
	SELECT session_start_equity, realized_equity, circuit_breaker_hit
	FROM account_daily_state
	WHERE epoch_day_utc = ?;
	`
	row := r.db.conn.QueryRowContext(ctx, query, epochDay)

	var startStr, realStr string
	var breakerInt int

	if err := row.Scan(&startStr, &realStr, &breakerInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return decimal.Zero, decimal.Zero, false, false, nil
		}
		return decimal.Zero, decimal.Zero, false, false, fmt.Errorf("storage: failed to query daily state: %w", err)
	}

	startDec, err := decimal.NewFromString(startStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, false, fmt.Errorf("storage: corrupt start equity: %w", err)
	}
	realDec, err := decimal.NewFromString(realStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, false, fmt.Errorf("storage: corrupt realized equity: %w", err)
	}

	return startDec, realDec, breakerInt == 1, true, nil
}

// GetLatestDailyState mengambil record account_daily_state terbaru untuk memulihkan ekuitas saat rollover (C-05)
func (r *SQLiteRepository) GetLatestDailyState(
	ctx context.Context,
) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, epochDay int64, found bool, err error) {
	query := `
	SELECT epoch_day_utc, session_start_equity, realized_equity, circuit_breaker_hit
	FROM account_daily_state
	ORDER BY epoch_day_utc DESC
	LIMIT 1;
	`
	row := r.db.conn.QueryRowContext(ctx, query)

	var startStr, realStr string
	var breakerInt int
	var epDay int64

	if err := row.Scan(&epDay, &startStr, &realStr, &breakerInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return decimal.Zero, decimal.Zero, false, 0, false, nil
		}
		return decimal.Zero, decimal.Zero, false, 0, false, fmt.Errorf("storage: failed to query latest daily state: %w", err)
	}

	startDec, err := decimal.NewFromString(startStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, 0, false, fmt.Errorf("storage: corrupt start equity: %w", err)
	}
	realDec, err := decimal.NewFromString(realStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, 0, false, fmt.Errorf("storage: corrupt realized equity: %w", err)
	}

	return startDec, realDec, breakerInt == 1, epDay, true, nil
}

func normalizeDailyStateMode(mode string) string {
	m := strings.ToUpper(strings.TrimSpace(mode))
	if m == "LIVE" {
		return "LIVE"
	}
	return "DRY_RUN"
}

func (r *SQLiteRepository) SaveDailyStateForMode(
	ctx context.Context,
	mode string,
	epochDay int64,
	sessionStartEquity, realizedEquity decimal.Decimal,
	breakerTripped bool,
) error {
	normMode := normalizeDailyStateMode(mode)
	query := `
	INSERT INTO account_daily_state_by_mode (
		mode, epoch_day_utc, session_start_equity, realized_equity, circuit_breaker_hit, updated_at_utc
	) VALUES (?, ?, ?, ?, ?, ?)
	ON CONFLICT(mode, epoch_day_utc) DO UPDATE SET
		realized_equity = excluded.realized_equity,
		circuit_breaker_hit = MAX(account_daily_state_by_mode.circuit_breaker_hit, excluded.circuit_breaker_hit),
		updated_at_utc = excluded.updated_at_utc;
	`
	breakerInt := 0
	if breakerTripped {
		breakerInt = 1
	}

	_, err := r.db.conn.ExecContext(
		ctx,
		query,
		normMode,
		epochDay,
		sessionStartEquity.String(),
		realizedEquity.String(),
		breakerInt,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("storage: failed to save daily state for mode %s: %w", normMode, err)
	}
	if normMode == "DRY_RUN" {
		_ = r.SaveDailyState(ctx, epochDay, sessionStartEquity, realizedEquity, breakerTripped)
	}
	return nil
}

func (r *SQLiteRepository) GetDailyStateForMode(
	ctx context.Context,
	mode string,
	epochDay int64,
) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, found bool, err error) {
	normMode := normalizeDailyStateMode(mode)
	query := `
	SELECT session_start_equity, realized_equity, circuit_breaker_hit
	FROM account_daily_state_by_mode
	WHERE mode = ? AND epoch_day_utc = ?;
	`
	row := r.db.conn.QueryRowContext(ctx, query, normMode, epochDay)

	var startStr, realStr string
	var breakerInt int
	if err := row.Scan(&startStr, &realStr, &breakerInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if normMode == "DRY_RUN" {
				return r.GetDailyState(ctx, epochDay)
			}
			return decimal.Zero, decimal.Zero, false, false, nil
		}
		return decimal.Zero, decimal.Zero, false, false, fmt.Errorf("storage: failed to query daily state for mode %s: %w", normMode, err)
	}

	startDec, err := decimal.NewFromString(startStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, false, fmt.Errorf("storage: corrupt start equity: %w", err)
	}
	realDec, err := decimal.NewFromString(realStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, false, fmt.Errorf("storage: corrupt realized equity: %w", err)
	}
	return startDec, realDec, breakerInt == 1, true, nil
}

func (r *SQLiteRepository) GetLatestDailyStateForMode(
	ctx context.Context,
	mode string,
) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, epochDay int64, found bool, err error) {
	normMode := normalizeDailyStateMode(mode)
	query := `
	SELECT epoch_day_utc, session_start_equity, realized_equity, circuit_breaker_hit
	FROM account_daily_state_by_mode
	WHERE mode = ?
	ORDER BY epoch_day_utc DESC
	LIMIT 1;
	`
	row := r.db.conn.QueryRowContext(ctx, query, normMode)

	var startStr, realStr string
	var breakerInt int
	var epDay int64
	if err := row.Scan(&epDay, &startStr, &realStr, &breakerInt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if normMode == "DRY_RUN" {
				return r.GetLatestDailyState(ctx)
			}
			return decimal.Zero, decimal.Zero, false, 0, false, nil
		}
		return decimal.Zero, decimal.Zero, false, 0, false, fmt.Errorf("storage: failed to query latest daily state for mode %s: %w", normMode, err)
	}

	startDec, err := decimal.NewFromString(startStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, 0, false, fmt.Errorf("storage: corrupt start equity: %w", err)
	}
	realDec, err := decimal.NewFromString(realStr)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, 0, false, fmt.Errorf("storage: corrupt realized equity: %w", err)
	}
	return startDec, realDec, breakerInt == 1, epDay, true, nil
}

func resolveRepoStrategyID(strategyID ...string) string {
	if len(strategyID) > 0 {
		if trimmed := strings.TrimSpace(strategyID[0]); trimmed != "" {
			return trimmed
		}
	}
	return "VWAP_PULLBACK"
}

func (r *SQLiteRepository) LogTrade(
	ctx context.Context,
	clientOrderID, symbol, side string,
	qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal,
	exitReason string,
	openedAt, closedAt time.Time,
	mae, mfe float64,
	signalAuditID int64,
	strategyID ...string,
) error {
	return r.LogTradeForMode(
		ctx, "", clientOrderID, symbol, side,
		qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL,
		exitReason, openedAt, closedAt, mae, mfe, signalAuditID, strategyID...,
	)
}

// LogTradeForMode records a closed trade tagged with its execution mode (DRY_RUN or LIVE) for mode-isolated lookups.
func (r *SQLiteRepository) LogTradeForMode(
	ctx context.Context,
	mode, clientOrderID, symbol, side string,
	qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal,
	exitReason string,
	openedAt, closedAt time.Time,
	mae, mfe float64,
	signalAuditID int64,
	strategyID ...string,
) error {
	normMode := ""
	if strings.TrimSpace(mode) != "" {
		normMode = normalizeDailyStateMode(mode)
	}
	stratID := resolveRepoStrategyID(strategyID...)
	query := `
	INSERT INTO trade_ledger (
		client_order_id, mode, symbol, side, strategy_id, qty, entry_price, exit_price,
		sl_price_initial, tp_price_initial, net_pnl_usdt, mae_pct, mfe_pct,
		exit_reason, opened_at_utc, closed_at_utc, signal_audit_id
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(client_order_id) DO UPDATE SET
		mode = CASE WHEN excluded.mode != '' THEN excluded.mode ELSE trade_ledger.mode END,
		strategy_id = CASE WHEN excluded.strategy_id != '' THEN excluded.strategy_id ELSE trade_ledger.strategy_id END,
		exit_price = excluded.exit_price,
		net_pnl_usdt = excluded.net_pnl_usdt,
		mae_pct = excluded.mae_pct,
		mfe_pct = excluded.mfe_pct,
		exit_reason = excluded.exit_reason,
		closed_at_utc = excluded.closed_at_utc,
		signal_audit_id = CASE WHEN trade_ledger.signal_audit_id = 0 THEN excluded.signal_audit_id ELSE trade_ledger.signal_audit_id END;
	`
	_, err := r.db.conn.ExecContext(
		ctx,
		query,
		clientOrderID,
		normMode,
		symbol,
		side,
		stratID,
		qty.String(),
		entryPrice.String(),
		exitPrice.String(),
		slPrice.String(),
		tpPrice.String(),
		netPnL.String(),
		mae,
		mfe,
		exitReason,
		formatSQLiteTime(openedAt),
		formatSQLiteTime(closedAt),
		signalAuditID,
	)
	if err != nil {
		return fmt.Errorf("storage: failed to log trade: %w", err)
	}
	return nil
}

func (r *SQLiteRepository) LogSignalAudit(
	ctx context.Context,
	candleOpenTime time.Time,
	symbol, side string,
	closePrice, ema200, sessionVwap, atr14, barDeltaZScore, oiDeltaPct, fundingRate, spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice float64,
	decision, rejectReason, rejectSubReason string,
	strategyID ...string,
) (int64, error) {
	stratID := resolveRepoStrategyID(strategyID...)
	query := `
	INSERT INTO signal_audit_log (
		candle_open_time_utc, symbol, side, strategy_id, close_price, ema200, session_vwap,
		atr14, bar_delta_zscore, oi_delta_pct, funding_rate, spread_pct, rvol, impulse_atr_mult,
		limit_price, sl_price, tp_price, decision, reject_reason_code, reject_sub_reason, created_at_utc
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	res, err := r.db.conn.ExecContext(
		ctx,
		query,
		formatSQLiteTime(candleOpenTime),
		symbol,
		side,
		stratID,
		closePrice,
		ema200,
		sessionVwap,
		atr14,
		barDeltaZScore,
		oiDeltaPct,
		fundingRate,
		spreadPct,
		rvol,
		impulseATRMult,
		limitPrice,
		slPrice,
		tpPrice,
		decision,
		rejectReason,
		rejectSubReason,
		formatSQLiteTime(time.Now()),
	)
	if err != nil {
		return 0, fmt.Errorf("storage: failed to log signal audit: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, nil
	}
	return id, nil
}

// PruneOldSignalAuditLogs menghapus log sinyal REJECT yang lebih tua dari olderThanDays (default 7 hari).
// Sinyal APPROVE/APPROVED dan trade_ledger tidak pernah dihapus untuk menjaga integritas audit.
func (r *SQLiteRepository) PruneOldSignalAuditLogs(ctx context.Context, olderThanDays int) (int64, error) {
	if olderThanDays <= 0 {
		olderThanDays = 7
	}
	cutoff := formatSQLiteTime(time.Now().UTC().AddDate(0, 0, -olderThanDays))
	query := `
	DELETE FROM signal_audit_log
	WHERE decision IN ('REJECT', 'REJECTED')
	  AND created_at_utc < ?;
	`
	res, err := r.db.conn.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("storage: failed to prune old signal audit logs: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	return rowsAffected, nil
}

// SaveDryRunPosition menyimpan atau memperbarui posisi virtual dry-run ke SQLite agar tahan restart
func (r *SQLiteRepository) SaveDryRunPosition(ctx context.Context, pos StoredDryRunPosition) error {
	stratID := resolveRepoStrategyID(pos.StrategyID)
	query := `
	INSERT INTO dryrun_open_positions (
		client_order_id, symbol, side, strategy_id, qty, initial_qty, entry_price, entry_fee,
		current_sl_price, initial_sl_price, tp_price, tp1_price, is_tp1_hit, atr_at_entry, last_mark_price,
		mark_updated_at_utc, opened_at_utc, is_breakeven_moved, protection_tier,
		signal_audit_id, max_mae, max_mfe, peak_price, funding_paid, last_funding_epoch
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(client_order_id) DO UPDATE SET
		strategy_id = excluded.strategy_id,
		qty = excluded.qty,
		initial_qty = excluded.initial_qty,
		entry_price = excluded.entry_price,
		entry_fee = excluded.entry_fee,
		tp1_price = excluded.tp1_price,
		is_tp1_hit = excluded.is_tp1_hit,
		current_sl_price = excluded.current_sl_price,
		initial_sl_price = CASE WHEN dryrun_open_positions.initial_sl_price = '' THEN excluded.initial_sl_price ELSE dryrun_open_positions.initial_sl_price END,
		last_mark_price = excluded.last_mark_price,
		mark_updated_at_utc = excluded.mark_updated_at_utc,
		is_breakeven_moved = excluded.is_breakeven_moved,
		protection_tier = excluded.protection_tier,
		signal_audit_id = CASE WHEN excluded.signal_audit_id > 0 THEN excluded.signal_audit_id ELSE dryrun_open_positions.signal_audit_id END,
		max_mae = excluded.max_mae,
		max_mfe = excluded.max_mfe,
		peak_price = excluded.peak_price,
		funding_paid = excluded.funding_paid,
		last_funding_epoch = excluded.last_funding_epoch;
	`
	beInt := 0
	if pos.IsBreakEvenMoved {
		beInt = 1
	}
	tpHitInt := 0
	if pos.IsTP1Hit {
		tpHitInt = 1
	}

	markTime := formatSQLiteTime(pos.MarkUpdatedAt)
	if pos.MarkUpdatedAt.IsZero() {
		markTime = formatSQLiteTime(time.Now())
	}
	openTime := formatSQLiteTime(pos.OpenedAt)
	if pos.OpenedAt.IsZero() {
		openTime = formatSQLiteTime(time.Now())
	}

	initQtyStr := pos.InitialQty.String()
	if pos.InitialQty.IsZero() {
		initQtyStr = pos.Qty.String()
	}

	initSLStr := pos.InitialSLPrice.String()
	if pos.InitialSLPrice.IsZero() {
		initSLStr = pos.CurrentSLPrice.String()
	}

	peakStr := pos.PeakPrice.String()
	if pos.PeakPrice.IsZero() {
		peakStr = pos.EntryPrice.String()
	}

	_, err := r.db.conn.ExecContext(
		ctx,
		query,
		pos.ClientOrderID,
		pos.Symbol,
		pos.Side,
		stratID,
		pos.Qty.String(),
		initQtyStr,
		pos.EntryPrice.String(),
		pos.EntryFee.String(),
		pos.CurrentSLPrice.String(),
		initSLStr,
		pos.TPPrice.String(),
		pos.TP1Price.String(),
		tpHitInt,
		pos.ATRAtEntry.String(),
		pos.LastMarkPrice.String(),
		markTime,
		openTime,
		beInt,
		pos.ProtectionTier,
		pos.SignalAuditID,
		pos.MaxMAE,
		pos.MaxMFE,
		peakStr,
		pos.FundingPaid.String(),
		pos.LastFundingEpoch,
	)
	if err != nil {
		return fmt.Errorf("storage: failed to save dry-run position %s (%s): %w", pos.Symbol, pos.ClientOrderID, err)
	}
	return nil
}

// DeleteDryRunPosition menghapus posisi virtual dry-run dari database setelah posisi ditutup
func (r *SQLiteRepository) DeleteDryRunPosition(ctx context.Context, clientOrderIDOrSymbol string) error {
	query := `DELETE FROM dryrun_open_positions WHERE client_order_id = ? OR symbol = ?;`
	_, err := r.db.conn.ExecContext(ctx, query, clientOrderIDOrSymbol, clientOrderIDOrSymbol)
	if err != nil {
		return fmt.Errorf("storage: failed to delete dry-run position %s: %w", clientOrderIDOrSymbol, err)
	}
	return nil
}

// GetAllDryRunPositions mengambil seluruh posisi terbuka yang tersimpan saat startup bot
func (r *SQLiteRepository) GetAllDryRunPositions(ctx context.Context) ([]StoredDryRunPosition, error) {
	query := `
	SELECT symbol, client_order_id, side, COALESCE(strategy_id, 'VWAP_PULLBACK'), qty, initial_qty, entry_price, entry_fee,
	       current_sl_price, initial_sl_price, tp_price, tp1_price, is_tp1_hit, atr_at_entry, last_mark_price,
	       mark_updated_at_utc, opened_at_utc, is_breakeven_moved, protection_tier,
	       signal_audit_id, max_mae, max_mfe, peak_price, funding_paid, last_funding_epoch
	FROM dryrun_open_positions;
	`
	rows, err := r.db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to query dry-run positions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []StoredDryRunPosition
	for rows.Next() {
		var p StoredDryRunPosition
		var qtyStr, initQtyStr, entryStr, feeStr, slStr, initSLStr, tpStr, tp1Str, atrStr, markStr, peakStr, fundingStr string
		var markTimeStr, openTimeStr string
		var beInt, tpHitInt int

		err := rows.Scan(
			&p.Symbol,
			&p.ClientOrderID,
			&p.Side,
			&p.StrategyID,
			&qtyStr,
			&initQtyStr,
			&entryStr,
			&feeStr,
			&slStr,
			&initSLStr,
			&tpStr,
			&tp1Str,
			&tpHitInt,
			&atrStr,
			&markStr,
			&markTimeStr,
			&openTimeStr,
			&beInt,
			&p.ProtectionTier,
			&p.SignalAuditID,
			&p.MaxMAE,
			&p.MaxMFE,
			&peakStr,
			&fundingStr,
			&p.LastFundingEpoch,
		)
		if err != nil {
			return nil, fmt.Errorf("storage: failed to scan dry-run position: %w", err)
		}

		var parseErr error
		if p.Qty, parseErr = parseDecimalRequired(qtyStr, "Qty", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialQty, parseErr = parseDecimalOptional(initQtyStr, p.Qty, "InitialQty", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialQty.IsZero() {
			p.InitialQty = p.Qty
		}
		if p.EntryPrice, parseErr = parseDecimalRequired(entryStr, "EntryPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.EntryFee, parseErr = parseDecimalOptional(feeStr, decimal.Zero, "EntryFee", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.CurrentSLPrice, parseErr = parseDecimalRequired(slStr, "CurrentSLPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialSLPrice, parseErr = parseDecimalOptional(initSLStr, p.CurrentSLPrice, "InitialSLPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialSLPrice.IsZero() {
			p.InitialSLPrice = p.CurrentSLPrice
		}
		if p.TPPrice, parseErr = parseDecimalOptional(tpStr, decimal.Zero, "TPPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.TP1Price, parseErr = parseDecimalOptional(tp1Str, decimal.Zero, "TP1Price", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		p.IsTP1Hit = (tpHitInt == 1)
		if p.ATRAtEntry, parseErr = parseDecimalOptional(atrStr, decimal.Zero, "ATRAtEntry", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.LastMarkPrice, parseErr = parseDecimalOptional(markStr, p.EntryPrice, "LastMarkPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		p.IsBreakEvenMoved = (beInt == 1)
		if p.PeakPrice, parseErr = parseDecimalOptional(peakStr, p.EntryPrice, "PeakPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.PeakPrice.IsZero() {
			p.PeakPrice = p.EntryPrice
		}
		if p.FundingPaid, parseErr = parseDecimalOptional(fundingStr, decimal.Zero, "FundingPaid", p.Symbol); parseErr != nil {
			return nil, parseErr
		}

		if t, err := time.Parse(time.RFC3339Nano, markTimeStr); err == nil {
			p.MarkUpdatedAt = t
		} else if t, err := time.Parse(time.RFC3339, markTimeStr); err == nil {
			p.MarkUpdatedAt = t
		}

		if t, err := time.Parse(time.RFC3339Nano, openTimeStr); err == nil {
			p.OpenedAt = t
		} else if t, err := time.Parse(time.RFC3339, openTimeStr); err == nil {
			p.OpenedAt = t
		}

		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: error iterating dry-run positions: %w", err)
	}
	return results, nil
}

func parseDecimalRequired(valStr, fieldName, sym string) (decimal.Decimal, error) {
	if strings.TrimSpace(valStr) == "" {
		return decimal.Zero, fmt.Errorf("storage: empty required decimal %s for %s", fieldName, sym)
	}
	d, err := decimal.NewFromString(valStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("storage: corrupt decimal %s '%s' for %s: %w", fieldName, valStr, sym, err)
	}
	return d, nil
}

func parseDecimalOptional(valStr string, fallback decimal.Decimal, fieldName, sym string) (decimal.Decimal, error) {
	if strings.TrimSpace(valStr) == "" {
		return fallback, nil
	}
	d, err := decimal.NewFromString(valStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("storage: corrupt optional decimal %s '%s' for %s: %w", fieldName, valStr, sym, err)
	}
	return d, nil
}

// UpdateTradeAutopsy menyimpan hasil analisis AI post-trade autopsy ke tabel trade_ledger
func (r *SQLiteRepository) UpdateTradeAutopsy(ctx context.Context, clientOrderID, autopsy string) error {
	query := `UPDATE trade_ledger SET ai_autopsy = ? WHERE client_order_id = ?;`
	res, err := r.db.conn.ExecContext(ctx, query, autopsy, clientOrderID)
	if err != nil {
		return fmt.Errorf("storage: failed to update trade autopsy: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("storage: trade not found with client_order_id %s", clientOrderID)
	}
	return nil
}

// GetTradeAutopsy mengambil hasil analisis AI yang sudah tersimpan di database jika ada
func (r *SQLiteRepository) GetTradeAutopsy(ctx context.Context, clientOrderID string) (string, error) {
	query := `SELECT ai_autopsy FROM trade_ledger WHERE client_order_id = ?;`
	var autopsy string
	err := r.db.conn.QueryRowContext(ctx, query, clientOrderID).Scan(&autopsy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("storage: failed to get trade autopsy: %w", err)
	}
	return autopsy, nil
}

// GetRecentExits mengambil record penutupan posisi terakhir untuk tiap simbol sejak waktu 'since'.
// Mengabaikan PARTIAL_TAKE_PROFIT agar hanya penutupan posisi penuh (SL, full TP, time stop, dsb) yang memicu cooldown.
func (r *SQLiteRepository) GetRecentExits(ctx context.Context, since time.Time) (map[string]RecentExitRecord, error) {
	return r.GetRecentExitsForMode(ctx, "", since)
}

// GetRecentExitsForMode mengambil record penutupan posisi terakhir untuk tiap simbol sejak waktu 'since',
// terisolasi ke mode ('DRY_RUN' / 'LIVE') bila diisi dan selalu mengabaikan baris PARTIAL_TAKE_PROFIT pada outer join.
func (r *SQLiteRepository) GetRecentExitsForMode(ctx context.Context, mode string, since time.Time) (map[string]RecentExitRecord, error) {
	sinceStr := formatSQLiteTime(since)
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(mode) != "" {
		normMode := normalizeDailyStateMode(mode)
		query := `
		SELECT t.symbol, t.exit_reason, t.closed_at_utc
		FROM trade_ledger t
		INNER JOIN (
			SELECT symbol, MAX(closed_at_utc) AS max_closed
			FROM trade_ledger
			WHERE exit_reason != 'PARTIAL_TAKE_PROFIT' AND mode = ? AND closed_at_utc >= ?
			GROUP BY symbol
		) latest ON t.symbol = latest.symbol AND t.closed_at_utc = latest.max_closed
		WHERE t.exit_reason != 'PARTIAL_TAKE_PROFIT' AND t.mode = ?;
		`
		rows, err = r.db.conn.QueryContext(ctx, query, normMode, sinceStr, normMode)
	} else {
		query := `
		SELECT t.symbol, t.exit_reason, t.closed_at_utc
		FROM trade_ledger t
		INNER JOIN (
			SELECT symbol, MAX(closed_at_utc) AS max_closed
			FROM trade_ledger
			WHERE exit_reason != 'PARTIAL_TAKE_PROFIT' AND closed_at_utc >= ?
			GROUP BY symbol
		) latest ON t.symbol = latest.symbol AND t.closed_at_utc = latest.max_closed
		WHERE t.exit_reason != 'PARTIAL_TAKE_PROFIT';
		`
		rows, err = r.db.conn.QueryContext(ctx, query, sinceStr)
	}
	if err != nil {
		return nil, fmt.Errorf("storage: failed to query recent exits: %w", err)
	}
	defer func() { _ = rows.Close() }()

	exits := make(map[string]RecentExitRecord)
	for rows.Next() {
		var sym, reason, closedAtStr string
		if err := rows.Scan(&sym, &reason, &closedAtStr); err != nil {
			return nil, fmt.Errorf("storage: failed to scan recent exit row: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, closedAtStr)
		if err != nil {
			t, err = time.Parse(time.RFC3339, closedAtStr)
		}
		if err == nil {
			exits[sym] = RecentExitRecord{
				Symbol:     sym,
				ClosedAt:   t.UTC(),
				ExitReason: reason,
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: error iterating recent exits: %w", err)
	}
	return exits, nil
}

// GetPeakEquityForMode scans stored daily equity snapshots for mode and returns the highest recorded equity.
func (r *SQLiteRepository) GetPeakEquityForMode(ctx context.Context, mode string) (decimal.Decimal, bool, error) {
	normMode := normalizeDailyStateMode(mode)
	scanPeakRows := func(rows *sql.Rows) (decimal.Decimal, bool, error) {
		defer func() { _ = rows.Close() }()
		peak := decimal.Zero
		found := false
		for rows.Next() {
			var startStr, realStr string
			if err := rows.Scan(&startStr, &realStr); err != nil {
				return decimal.Zero, false, fmt.Errorf("storage: failed to scan peak equity row: %w", err)
			}
			for _, raw := range []string{startStr, realStr} {
				if v, err := decimal.NewFromString(strings.TrimSpace(raw)); err == nil && v.GreaterThan(peak) {
					peak = v
					found = true
				}
			}
		}
		if err := rows.Err(); err != nil {
			return decimal.Zero, false, fmt.Errorf("storage: error iterating peak equity rows: %w", err)
		}
		return peak, found, nil
	}

	rows, err := r.db.conn.QueryContext(
		ctx,
		`SELECT session_start_equity, realized_equity FROM account_daily_state_by_mode WHERE mode = ?;`,
		normMode,
	)
	if err != nil {
		return decimal.Zero, false, fmt.Errorf("storage: failed to query peak equity for mode %s: %w", normMode, err)
	}
	peak, found, err := scanPeakRows(rows)
	if err != nil || found {
		return peak, found, err
	}
	if normMode == "DRY_RUN" {
		legacyRows, legErr := r.db.conn.QueryContext(
			ctx,
			`SELECT session_start_equity, realized_equity FROM account_daily_state;`,
		)
		if legErr != nil {
			return decimal.Zero, false, fmt.Errorf("storage: failed to query legacy peak equity: %w", legErr)
		}
		return scanPeakRows(legacyRows)
	}
	return decimal.Zero, false, nil
}

// GetRecentStopLossTimes returns chronological UTC timestamps for STOP_LOSS exits since 'since'.
func (r *SQLiteRepository) GetRecentStopLossTimes(ctx context.Context, since time.Time) ([]time.Time, error) {
	return r.GetRecentStopLossTimesForMode(ctx, "", since)
}

// GetRecentStopLossTimesForMode returns chronological UTC timestamps for STOP_LOSS exits since 'since',
// scoped to mode (DRY_RUN / LIVE) when non-empty.
func (r *SQLiteRepository) GetRecentStopLossTimesForMode(ctx context.Context, mode string, since time.Time) ([]time.Time, error) {
	sinceStr := formatSQLiteTime(since)
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(mode) != "" {
		normMode := normalizeDailyStateMode(mode)
		rows, err = r.db.conn.QueryContext(
			ctx,
			`SELECT closed_at_utc FROM trade_ledger WHERE exit_reason = 'STOP_LOSS' AND mode = ? AND closed_at_utc >= ? ORDER BY closed_at_utc ASC;`,
			normMode, sinceStr,
		)
	} else {
		rows, err = r.db.conn.QueryContext(
			ctx,
			`SELECT closed_at_utc FROM trade_ledger WHERE exit_reason = 'STOP_LOSS' AND closed_at_utc >= ? ORDER BY closed_at_utc ASC;`,
			sinceStr,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("storage: failed to query recent stop losses: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []time.Time
	for rows.Next() {
		var closedAtStr string
		if err := rows.Scan(&closedAtStr); err != nil {
			return nil, fmt.Errorf("storage: failed to scan recent stop loss timestamp: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, closedAtStr)
		if err != nil {
			t, err = time.Parse(time.RFC3339, closedAtStr)
		}
		if err == nil {
			out = append(out, t.UTC())
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: error iterating recent stop losses: %w", err)
	}
	return out, nil
}

// SaveLivePosition menyimpan atau memperbarui posisi live ke SQLite agar tahan restart (TD-01)
func (r *SQLiteRepository) SaveLivePosition(ctx context.Context, pos StoredLivePosition) error {
	stratID := resolveRepoStrategyID(pos.StrategyID)
	query := `
	INSERT INTO live_open_positions (
		symbol, side, strategy_id, qty, initial_qty, entry_price, sl_order_id, sl_client_algo_id,
		sl_price, initial_sl_price, tp_order_id, tp_client_algo_id, tp_price, tp1_price,
		tp1_order_id, tp1_client_order_id, is_tp1_hit,
		atr_at_entry, protection_tier, signal_audit_id, opened_at_utc, max_mae, max_mfe, updated_at_utc, peak_price
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(symbol) DO UPDATE SET
		side = excluded.side,
		strategy_id = excluded.strategy_id,
		qty = excluded.qty,
		initial_qty = excluded.initial_qty,
		entry_price = excluded.entry_price,
		sl_order_id = excluded.sl_order_id,
		sl_client_algo_id = excluded.sl_client_algo_id,
		sl_price = excluded.sl_price,
		initial_sl_price = excluded.initial_sl_price,
		tp_order_id = excluded.tp_order_id,
		tp_client_algo_id = excluded.tp_client_algo_id,
		tp_price = excluded.tp_price,
		tp1_price = excluded.tp1_price,
		tp1_order_id = excluded.tp1_order_id,
		tp1_client_order_id = excluded.tp1_client_order_id,
		is_tp1_hit = excluded.is_tp1_hit,
		atr_at_entry = excluded.atr_at_entry,
		protection_tier = excluded.protection_tier,
		signal_audit_id = excluded.signal_audit_id,
		opened_at_utc = excluded.opened_at_utc,
		max_mae = excluded.max_mae,
		max_mfe = excluded.max_mfe,
		updated_at_utc = excluded.updated_at_utc,
		peak_price = excluded.peak_price;
	`
	openTime := formatSQLiteTime(pos.OpenedAt)
	if pos.OpenedAt.IsZero() {
		openTime = formatSQLiteTime(time.Now())
	}
	updTime := formatSQLiteTime(pos.UpdatedAt)
	if pos.UpdatedAt.IsZero() {
		updTime = formatSQLiteTime(time.Now())
	}

	initQtyStr := pos.InitialQty.String()
	if pos.InitialQty.IsZero() {
		initQtyStr = pos.Qty.String()
	}
	initSLStr := pos.InitialSLPrice.String()
	if pos.InitialSLPrice.IsZero() {
		initSLStr = pos.SLPrice.String()
	}
	tpHitInt := 0
	if pos.IsTP1Hit {
		tpHitInt = 1
	}

	peakStr := pos.PeakPrice.String()
	if pos.PeakPrice.IsZero() {
		peakStr = pos.EntryPrice.String()
	}

	_, err := r.db.conn.ExecContext(
		ctx,
		query,
		pos.Symbol,
		pos.Side,
		stratID,
		pos.Qty.String(),
		initQtyStr,
		pos.EntryPrice.String(),
		pos.SLOrderID,
		pos.SLClientAlgoID,
		pos.SLPrice.String(),
		initSLStr,
		pos.TPOrderID,
		pos.TPClientAlgoID,
		pos.TPPrice.String(),
		pos.TP1Price.String(),
		pos.TP1OrderID,
		pos.TP1ClientOrderID,
		tpHitInt,
		pos.ATRAtEntry.String(),
		pos.ProtectionTier,
		pos.SignalAuditID,
		openTime,
		pos.MaxMAE,
		pos.MaxMFE,
		updTime,
		peakStr,
	)
	if err != nil {
		return fmt.Errorf("storage: failed to save live position %s: %w", pos.Symbol, err)
	}
	return nil
}

// DeleteLivePosition menghapus posisi live dari database setelah posisi ditutup
func (r *SQLiteRepository) DeleteLivePosition(ctx context.Context, symbol string) error {
	query := `DELETE FROM live_open_positions WHERE symbol = ?;`
	_, err := r.db.conn.ExecContext(ctx, query, symbol)
	if err != nil {
		return fmt.Errorf("storage: failed to delete live position %s: %w", symbol, err)
	}
	return nil
}

// GetAllLivePositions mengambil seluruh posisi live terbuka yang tersimpan saat startup bot
func (r *SQLiteRepository) GetAllLivePositions(ctx context.Context) ([]StoredLivePosition, error) {
	query := `
	SELECT symbol, side, COALESCE(strategy_id, 'VWAP_PULLBACK'), qty, initial_qty, entry_price, sl_order_id, sl_client_algo_id,
	       sl_price, initial_sl_price, tp_order_id, tp_client_algo_id, tp_price, tp1_price,
	       COALESCE(tp1_order_id, 0), COALESCE(tp1_client_order_id, ''), is_tp1_hit,
	       atr_at_entry, protection_tier, signal_audit_id, opened_at_utc, max_mae, max_mfe, updated_at_utc, peak_price
	FROM live_open_positions;
	`
	rows, err := r.db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to query live positions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []StoredLivePosition
	for rows.Next() {
		var p StoredLivePosition
		var qtyStr, initQtyStr, entryStr, slStr, initSLStr, tpStr, tp1Str, atrStr string
		var openTimeStr, updTimeStr, peakStr string
		var tpHitInt int
		err := rows.Scan(
			&p.Symbol,
			&p.Side,
			&p.StrategyID,
			&qtyStr,
			&initQtyStr,
			&entryStr,
			&p.SLOrderID,
			&p.SLClientAlgoID,
			&slStr,
			&initSLStr,
			&p.TPOrderID,
			&p.TPClientAlgoID,
			&tpStr,
			&tp1Str,
			&p.TP1OrderID,
			&p.TP1ClientOrderID,
			&tpHitInt,
			&atrStr,
			&p.ProtectionTier,
			&p.SignalAuditID,
			&openTimeStr,
			&p.MaxMAE,
			&p.MaxMFE,
			&updTimeStr,
			&peakStr,
		)
		if err != nil {
			return nil, fmt.Errorf("storage: failed to scan live position: %w", err)
		}
		var parseErr error
		if p.Qty, parseErr = parseDecimalRequired(qtyStr, "Qty", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialQty, parseErr = parseDecimalOptional(initQtyStr, p.Qty, "InitialQty", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialQty.IsZero() {
			p.InitialQty = p.Qty
		}
		if p.EntryPrice, parseErr = parseDecimalRequired(entryStr, "EntryPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.SLPrice, parseErr = parseDecimalOptional(slStr, decimal.Zero, "SLPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialSLPrice, parseErr = parseDecimalOptional(initSLStr, p.SLPrice, "InitialSLPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.InitialSLPrice.IsZero() {
			p.InitialSLPrice = p.SLPrice
		}
		if p.TPPrice, parseErr = parseDecimalOptional(tpStr, decimal.Zero, "TPPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.TP1Price, parseErr = parseDecimalOptional(tp1Str, decimal.Zero, "TP1Price", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		p.IsTP1Hit = (tpHitInt == 1)
		if p.ATRAtEntry, parseErr = parseDecimalOptional(atrStr, decimal.Zero, "ATRAtEntry", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.PeakPrice, parseErr = parseDecimalOptional(peakStr, p.EntryPrice, "PeakPrice", p.Symbol); parseErr != nil {
			return nil, parseErr
		}
		if p.PeakPrice.IsZero() {
			p.PeakPrice = p.EntryPrice
		}
		if t, err := time.Parse(time.RFC3339Nano, openTimeStr); err == nil {
			p.OpenedAt = t
		} else if t, err := time.Parse(time.RFC3339, openTimeStr); err == nil {
			p.OpenedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, updTimeStr); err == nil {
			p.UpdatedAt = t
		} else if t, err := time.Parse(time.RFC3339, updTimeStr); err == nil {
			p.UpdatedAt = t
		}
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: error iterating live positions: %w", err)
	}
	return results, nil
}
