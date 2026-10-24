package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Repository interface {
	SaveDailyState(ctx context.Context, epochDay int64, sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool) error
	GetDailyState(ctx context.Context, epochDay int64) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, found bool, err error)
	LogTrade(ctx context.Context, clientOrderID, symbol, side string, qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal, exitReason string, openedAt, closedAt time.Time, mae, mfe float64, signalAuditID int64) error
	LogSignalAudit(ctx context.Context, candleOpenTime time.Time, symbol, side string, closePrice, ema200, sessionVwap, atr14, barDeltaZScore, oiDeltaPct, fundingRate, spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice float64, decision, rejectReason, rejectSubReason string) (int64, error)
	PruneOldSignalAuditLogs(ctx context.Context, olderThanDays int) (int64, error)
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
		circuit_breaker_hit = excluded.circuit_breaker_hit,
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

func (r *SQLiteRepository) LogTrade(
	ctx context.Context,
	clientOrderID, symbol, side string,
	qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal,
	exitReason string,
	openedAt, closedAt time.Time,
	mae, mfe float64,
	signalAuditID int64,
) error {
	query := `
	INSERT INTO trade_ledger (
		client_order_id, symbol, side, qty, entry_price, exit_price,
		sl_price_initial, tp_price_initial, net_pnl_usdt, mae_pct, mfe_pct,
		exit_reason, opened_at_utc, closed_at_utc, signal_audit_id
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(client_order_id) DO UPDATE SET
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
		symbol,
		side,
		qty.String(),
		entryPrice.String(),
		exitPrice.String(),
		slPrice.String(),
		tpPrice.String(),
		netPnL.String(),
		mae,
		mfe,
		exitReason,
		openedAt.UTC().Format(time.RFC3339Nano),
		closedAt.UTC().Format(time.RFC3339Nano),
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
) (int64, error) {
	query := `
	INSERT INTO signal_audit_log (
		candle_open_time_utc, symbol, side, close_price, ema200, session_vwap,
		atr14, bar_delta_zscore, oi_delta_pct, funding_rate, spread_pct, rvol, impulse_atr_mult,
		limit_price, sl_price, tp_price, decision, reject_reason_code, reject_sub_reason, created_at_utc
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	res, err := r.db.conn.ExecContext(
		ctx,
		query,
		candleOpenTime.UTC().Format(time.RFC3339Nano),
		symbol,
		side,
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
		time.Now().UTC().Format(time.RFC3339Nano),
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
	cutoff := time.Now().UTC().AddDate(0, 0, -olderThanDays).Format(time.RFC3339Nano)
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
