package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type StoredDryRunPosition struct {
	Symbol           string
	ClientOrderID    string
	Side             string
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
}

type StoredLivePosition struct {
	Symbol         string
	Side           string
	Qty            decimal.Decimal
	InitialQty     decimal.Decimal
	EntryPrice     decimal.Decimal
	SLOrderID      int64
	SLClientAlgoID string
	SLPrice        decimal.Decimal
	InitialSLPrice decimal.Decimal
	TPOrderID      int64
	TPClientAlgoID string
	TPPrice        decimal.Decimal
	TP1Price       decimal.Decimal
	IsTP1Hit       bool
	ATRAtEntry     decimal.Decimal
	ProtectionTier int
	SignalAuditID  int64
	OpenedAt       time.Time
	MaxMAE         float64
	MaxMFE         float64
	UpdatedAt      time.Time
	PeakPrice      decimal.Decimal
}

type Repository interface {
	SaveDailyState(ctx context.Context, epochDay int64, sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool) error
	GetDailyState(ctx context.Context, epochDay int64) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, found bool, err error)
	GetLatestDailyState(ctx context.Context) (sessionStartEquity, realizedEquity decimal.Decimal, breakerTripped bool, epochDay int64, found bool, err error)
	LogTrade(ctx context.Context, clientOrderID, symbol, side string, qty, entryPrice, exitPrice, slPrice, tpPrice, netPnL decimal.Decimal, exitReason string, openedAt, closedAt time.Time, mae, mfe float64, signalAuditID int64) error
	LogSignalAudit(ctx context.Context, candleOpenTime time.Time, symbol, side string, closePrice, ema200, sessionVwap, atr14, barDeltaZScore, oiDeltaPct, fundingRate, spreadPct, rvol, impulseATRMult, limitPrice, slPrice, tpPrice float64, decision, rejectReason, rejectSubReason string) (int64, error)
	PruneOldSignalAuditLogs(ctx context.Context, olderThanDays int) (int64, error)
	SaveDryRunPosition(ctx context.Context, pos StoredDryRunPosition) error
	DeleteDryRunPosition(ctx context.Context, symbol string) error
	GetAllDryRunPositions(ctx context.Context) ([]StoredDryRunPosition, error)
	SaveLivePosition(ctx context.Context, pos StoredLivePosition) error
	DeleteLivePosition(ctx context.Context, symbol string) error
	GetAllLivePositions(ctx context.Context) ([]StoredLivePosition, error)
	UpdateTradeAutopsy(ctx context.Context, clientOrderID, autopsy string) error
	GetTradeAutopsy(ctx context.Context, clientOrderID string) (string, error)
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

// SaveDryRunPosition menyimpan atau memperbarui posisi virtual dry-run ke SQLite agar tahan restart
func (r *SQLiteRepository) SaveDryRunPosition(ctx context.Context, pos StoredDryRunPosition) error {
	query := `
	INSERT INTO dryrun_open_positions (
		client_order_id, symbol, side, qty, initial_qty, entry_price, entry_fee,
		current_sl_price, initial_sl_price, tp_price, tp1_price, is_tp1_hit, atr_at_entry, last_mark_price,
		mark_updated_at_utc, opened_at_utc, is_breakeven_moved, protection_tier,
		signal_audit_id, max_mae, max_mfe, peak_price
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(client_order_id) DO UPDATE SET
		qty = excluded.qty,
		initial_qty = excluded.initial_qty,
		tp1_price = excluded.tp1_price,
		is_tp1_hit = excluded.is_tp1_hit,
		current_sl_price = excluded.current_sl_price,
		initial_sl_price = CASE WHEN dryrun_open_positions.initial_sl_price = '' THEN excluded.initial_sl_price ELSE dryrun_open_positions.initial_sl_price END,
		last_mark_price = excluded.last_mark_price,
		mark_updated_at_utc = excluded.mark_updated_at_utc,
		is_breakeven_moved = excluded.is_breakeven_moved,
		protection_tier = excluded.protection_tier,
		max_mae = excluded.max_mae,
		max_mfe = excluded.max_mfe,
		peak_price = excluded.peak_price;
	`
	beInt := 0
	if pos.IsBreakEvenMoved {
		beInt = 1
	}
	tpHitInt := 0
	if pos.IsTP1Hit {
		tpHitInt = 1
	}

	markTime := pos.MarkUpdatedAt.UTC().Format(time.RFC3339Nano)
	if pos.MarkUpdatedAt.IsZero() {
		markTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	openTime := pos.OpenedAt.UTC().Format(time.RFC3339Nano)
	if pos.OpenedAt.IsZero() {
		openTime = time.Now().UTC().Format(time.RFC3339Nano)
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
	SELECT symbol, client_order_id, side, qty, initial_qty, entry_price, entry_fee,
	       current_sl_price, initial_sl_price, tp_price, tp1_price, is_tp1_hit, atr_at_entry, last_mark_price,
	       mark_updated_at_utc, opened_at_utc, is_breakeven_moved, protection_tier,
	       signal_audit_id, max_mae, max_mfe, peak_price
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
		var qtyStr, initQtyStr, entryStr, feeStr, slStr, initSLStr, tpStr, tp1Str, atrStr, markStr, peakStr string
		var markTimeStr, openTimeStr string
		var beInt, tpHitInt int

		err := rows.Scan(
			&p.Symbol,
			&p.ClientOrderID,
			&p.Side,
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
		)
		if err != nil {
			return nil, fmt.Errorf("storage: failed to scan dry-run position: %w", err)
		}

		p.Qty, _ = decimal.NewFromString(qtyStr)
		p.InitialQty, _ = decimal.NewFromString(initQtyStr)
		if p.InitialQty.IsZero() {
			p.InitialQty = p.Qty
		}
		p.EntryPrice, _ = decimal.NewFromString(entryStr)
		p.EntryFee, _ = decimal.NewFromString(feeStr)
		p.CurrentSLPrice, _ = decimal.NewFromString(slStr)
		p.InitialSLPrice, _ = decimal.NewFromString(initSLStr)
		if p.InitialSLPrice.IsZero() {
			p.InitialSLPrice = p.CurrentSLPrice
		}
		p.TPPrice, _ = decimal.NewFromString(tpStr)
		p.TP1Price, _ = decimal.NewFromString(tp1Str)
		p.IsTP1Hit = (tpHitInt == 1)
		p.ATRAtEntry, _ = decimal.NewFromString(atrStr)
		p.LastMarkPrice, _ = decimal.NewFromString(markStr)
		p.IsBreakEvenMoved = (beInt == 1)
		p.PeakPrice, _ = decimal.NewFromString(peakStr)
		if p.PeakPrice.IsZero() {
			p.PeakPrice = p.EntryPrice
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

// SaveLivePosition menyimpan atau memperbarui posisi live ke SQLite agar tahan restart (TD-01)
func (r *SQLiteRepository) SaveLivePosition(ctx context.Context, pos StoredLivePosition) error {
	query := `
	INSERT INTO live_open_positions (
		symbol, side, qty, initial_qty, entry_price, sl_order_id, sl_client_algo_id,
		sl_price, initial_sl_price, tp_order_id, tp_client_algo_id, tp_price, tp1_price, is_tp1_hit,
		atr_at_entry, protection_tier, signal_audit_id, opened_at_utc, max_mae, max_mfe, updated_at_utc, peak_price
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(symbol) DO UPDATE SET
		side = excluded.side,
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
		is_tp1_hit = excluded.is_tp1_hit,
		atr_at_entry = excluded.atr_at_entry,
		protection_tier = excluded.protection_tier,
		signal_audit_id = excluded.signal_audit_id,
		max_mae = excluded.max_mae,
		max_mfe = excluded.max_mfe,
		updated_at_utc = excluded.updated_at_utc,
		peak_price = excluded.peak_price;
	`
	openTime := pos.OpenedAt.UTC().Format(time.RFC3339Nano)
	if pos.OpenedAt.IsZero() {
		openTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	updTime := pos.UpdatedAt.UTC().Format(time.RFC3339Nano)
	if pos.UpdatedAt.IsZero() {
		updTime = time.Now().UTC().Format(time.RFC3339Nano)
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
	SELECT symbol, side, qty, initial_qty, entry_price, sl_order_id, sl_client_algo_id,
	       sl_price, initial_sl_price, tp_order_id, tp_client_algo_id, tp_price, tp1_price, is_tp1_hit,
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
		p.Qty, _ = decimal.NewFromString(qtyStr)
		p.InitialQty, _ = decimal.NewFromString(initQtyStr)
		if p.InitialQty.IsZero() {
			p.InitialQty = p.Qty
		}
		p.EntryPrice, _ = decimal.NewFromString(entryStr)
		p.SLPrice, _ = decimal.NewFromString(slStr)
		p.InitialSLPrice, _ = decimal.NewFromString(initSLStr)
		if p.InitialSLPrice.IsZero() {
			p.InitialSLPrice = p.SLPrice
		}
		p.TPPrice, _ = decimal.NewFromString(tpStr)
		p.TP1Price, _ = decimal.NewFromString(tp1Str)
		p.IsTP1Hit = (tpHitInt == 1)
		p.ATRAtEntry, _ = decimal.NewFromString(atrStr)
		p.PeakPrice, _ = decimal.NewFromString(peakStr)
		if p.PeakPrice.IsZero() {
			p.PeakPrice = p.EntryPrice
		}
		if err != nil {
			return nil, fmt.Errorf("storage: failed to scan live position: %w", err)
		}
		p.Qty, _ = decimal.NewFromString(qtyStr)
		p.InitialQty, _ = decimal.NewFromString(initQtyStr)
		if p.InitialQty.IsZero() {
			p.InitialQty = p.Qty
		}
		p.EntryPrice, _ = decimal.NewFromString(entryStr)
		p.SLPrice, _ = decimal.NewFromString(slStr)
		p.InitialSLPrice, _ = decimal.NewFromString(initSLStr)
		if p.InitialSLPrice.IsZero() {
			p.InitialSLPrice = p.SLPrice
		}
		p.TPPrice, _ = decimal.NewFromString(tpStr)
		p.TP1Price, _ = decimal.NewFromString(tp1Str)
		p.IsTP1Hit = (tpHitInt == 1)
		p.ATRAtEntry, _ = decimal.NewFromString(atrStr)
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



