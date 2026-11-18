package data

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
	_ "modernc.org/sqlite"
)

const backtestSchemaDDL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS universe_symbols (
    symbol         TEXT PRIMARY KEY,
    status         TEXT NOT NULL,
    contract_type  TEXT NOT NULL,
    is_blocklisted INTEGER NOT NULL DEFAULT 0,
    first_seen_ms  INTEGER NOT NULL DEFAULT 0,
    last_seen_ms   INTEGER NOT NULL DEFAULT 0,
    updated_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS exchange_filters_history (
    symbol        TEXT PRIMARY KEY,
    tick_size     TEXT NOT NULL,
    step_size     TEXT NOT NULL,
    min_qty       TEXT NOT NULL,
    min_notional  TEXT NOT NULL,
    inferred      INTEGER NOT NULL DEFAULT 0,
    source        TEXT NOT NULL,
    updated_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_manifest (
    cache_key      TEXT PRIMARY KEY,
    row_count      INTEGER NOT NULL,
    start_ms       INTEGER NOT NULL,
    end_ms         INTEGER NOT NULL,
    partial_day    INTEGER NOT NULL DEFAULT 0,
    updated_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id              TEXT PRIMARY KEY,
    commit_hash         TEXT NOT NULL,
    mode                TEXT NOT NULL,
    from_utc            TEXT NOT NULL,
    to_utc              TEXT NOT NULL,
    exec_tf             TEXT NOT NULL,
    oi_mode             TEXT NOT NULL,
    universe_mode       TEXT NOT NULL,
    total_trades        INTEGER NOT NULL,
    win_rate_pct        REAL NOT NULL,
    net_pnl_usdt        REAL NOT NULL,
    profit_factor       REAL NOT NULL,
    mtm_max_dd_pct      REAL NOT NULL,
    expectancy_usdt     REAL NOT NULL,
    oi_coverage_pct     REAL NOT NULL DEFAULT 0.0,
    config_json         TEXT NOT NULL DEFAULT '',
    created_at_utc      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_trades (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    side           TEXT NOT NULL,
    entry_time_ms  INTEGER NOT NULL,
    exit_time_ms   INTEGER NOT NULL,
    entry_price    REAL NOT NULL,
    exit_price     REAL NOT NULL,
    qty            REAL NOT NULL,
    net_pnl_usdt   REAL NOT NULL,
    fee_usdt       REAL NOT NULL,
    funding_usdt   REAL NOT NULL,
    exit_reason    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_equity_curve (
    run_id           TEXT NOT NULL,
    time_ms          INTEGER NOT NULL,
    realized_equity  REAL NOT NULL,
    effective_equity REAL NOT NULL,
    used_margin      REAL NOT NULL,
    open_positions   INTEGER NOT NULL,
    PRIMARY KEY (run_id, time_ms)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS walkforward_folds (
    run_id           TEXT NOT NULL,
    fold_index       INTEGER NOT NULL,
    is_from_utc      TEXT NOT NULL,
    is_to_utc        TEXT NOT NULL,
    oos_from_utc     TEXT NOT NULL,
    oos_to_utc       TEXT NOT NULL,
    is_expectancy    REAL NOT NULL,
    oos_expectancy   REAL NOT NULL,
    degradation_pct  REAL NOT NULL,
    verdict          TEXT NOT NULL,
    PRIMARY KEY (run_id, fold_index)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_run_trades_run_id ON run_trades(run_id);
`

// BacktestStore manages metadata, filter history, cache manifest, and experiment history in SQLite (backtest.db),
// cleanly separated from the live operational database (trml.db).
type BacktestStore struct {
	conn *sql.DB
}

func OpenBacktestStore(dbPath string) (*BacktestStore, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create backtest db dir %s: %w", dir, err)
	}
	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open backtest sqlite %s: %w", dbPath, err)
	}
	conn.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, backtestSchemaDDL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("init backtest sqlite schema: %w", err)
	}
	return &BacktestStore{conn: conn}, nil
}

func (s *BacktestStore) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

type UniverseSymbolMeta struct {
	Symbol        string
	Status        string
	ContractType  string
	IsBlocklisted bool
	FirstSeenMs   int64
	LastSeenMs    int64
}

func (s *BacktestStore) UpsertUniverseSymbols(ctx context.Context, metas []UniverseSymbolMeta) error {
	if len(metas) == 0 {
		return nil
	}
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO universe_symbols (symbol, status, contract_type, is_blocklisted, first_seen_ms, last_seen_ms, updated_at_utc)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(symbol) DO UPDATE SET
			status = CASE WHEN excluded.status != '' THEN excluded.status ELSE universe_symbols.status END,
			contract_type = CASE WHEN excluded.contract_type != '' THEN excluded.contract_type ELSE universe_symbols.contract_type END,
			is_blocklisted = CASE WHEN excluded.is_blocklisted = 1 THEN 1 ELSE universe_symbols.is_blocklisted END,
			first_seen_ms = CASE
				WHEN universe_symbols.first_seen_ms = 0 THEN excluded.first_seen_ms
				WHEN excluded.first_seen_ms > 0 AND excluded.first_seen_ms < universe_symbols.first_seen_ms THEN excluded.first_seen_ms
				ELSE universe_symbols.first_seen_ms
			END,
			last_seen_ms = MAX(universe_symbols.last_seen_ms, excluded.last_seen_ms),
			updated_at_utc = excluded.updated_at_utc
	`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	nowUnix := time.Now().UTC().Unix()
	for _, m := range metas {
		blockInt := 0
		if m.IsBlocklisted {
			blockInt = 1
		}
		if _, err := stmt.ExecContext(ctx, m.Symbol, m.Status, m.ContractType, blockInt, m.FirstSeenMs, m.LastSeenMs, nowUnix); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *BacktestStore) GetEligibleUniverseSymbols(ctx context.Context) ([]UniverseSymbolMeta, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT symbol, status, contract_type, is_blocklisted, first_seen_ms, last_seen_ms
		FROM universe_symbols
		WHERE is_blocklisted = 0
		ORDER BY symbol ASC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []UniverseSymbolMeta
	for rows.Next() {
		var m UniverseSymbolMeta
		var blockInt int
		if err := rows.Scan(&m.Symbol, &m.Status, &m.ContractType, &blockInt, &m.FirstSeenMs, &m.LastSeenMs); err != nil {
			return nil, err
		}
		m.IsBlocklisted = blockInt == 1
		out = append(out, m)
	}
	return out, rows.Err()
}

type StoredFilterMeta struct {
	Filter   model.ExchangeFilter
	Inferred bool
	Source   string
}

func (s *BacktestStore) UpsertExchangeFilters(ctx context.Context, filters map[string]StoredFilterMeta) error {
	if len(filters) == 0 {
		return nil
	}
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO exchange_filters_history (symbol, tick_size, step_size, min_qty, min_notional, inferred, source, updated_at_utc)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(symbol) DO UPDATE SET
			tick_size = CASE WHEN excluded.inferred = 0 OR exchange_filters_history.inferred = 1 THEN excluded.tick_size ELSE exchange_filters_history.tick_size END,
			step_size = CASE WHEN excluded.inferred = 0 OR exchange_filters_history.inferred = 1 THEN excluded.step_size ELSE exchange_filters_history.step_size END,
			min_qty = CASE WHEN excluded.inferred = 0 OR exchange_filters_history.inferred = 1 THEN excluded.min_qty ELSE exchange_filters_history.min_qty END,
			min_notional = CASE WHEN excluded.inferred = 0 OR exchange_filters_history.inferred = 1 THEN excluded.min_notional ELSE exchange_filters_history.min_notional END,
			inferred = CASE WHEN excluded.inferred = 0 THEN 0 ELSE exchange_filters_history.inferred END,
			source = CASE WHEN excluded.inferred = 0 OR exchange_filters_history.inferred = 1 THEN excluded.source ELSE exchange_filters_history.source END,
			updated_at_utc = excluded.updated_at_utc
	`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	nowUnix := time.Now().UTC().Unix()
	for sym, meta := range filters {
		infInt := 0
		if meta.Inferred {
			infInt = 1
		}
		if _, err := stmt.ExecContext(ctx,
			sym,
			meta.Filter.TickSize.String(),
			meta.Filter.StepSize.String(),
			meta.Filter.MinQty.String(),
			meta.Filter.MinNotional.String(),
			infInt,
			meta.Source,
			nowUnix,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *BacktestStore) GetExchangeFilters(ctx context.Context) (map[string]StoredFilterMeta, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT symbol, tick_size, step_size, min_qty, min_notional, inferred, source
		FROM exchange_filters_history
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]StoredFilterMeta)
	for rows.Next() {
		var sym, ts, ss, mq, mn, src string
		var infInt int
		if err := rows.Scan(&sym, &ts, &ss, &mq, &mn, &infInt, &src); err != nil {
			return nil, err
		}
		tDec, _ := decimal.NewFromString(ts)
		sDec, _ := decimal.NewFromString(ss)
		mqDec, _ := decimal.NewFromString(mq)
		mnDec, _ := decimal.NewFromString(mn)
		if tDec.GreaterThan(decimal.Zero) && sDec.GreaterThan(decimal.Zero) {
			out[sym] = StoredFilterMeta{
				Filter: model.ExchangeFilter{
					TickSize:    tDec,
					StepSize:    sDec,
					MinQty:      mqDec,
					MinNotional: mnDec,
				},
				Inferred: infInt == 1,
				Source:   src,
			}
		}
	}
	return out, rows.Err()
}

func (s *BacktestStore) UpsertCacheManifest(ctx context.Context, key string, rowCount int, startMs, endMs int64, partial bool) error {
	partInt := 0
	if partial {
		partInt = 1
	}
	_, err := s.conn.ExecContext(ctx, `
		INSERT INTO cache_manifest (cache_key, row_count, start_ms, end_ms, partial_day, updated_at_utc)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(cache_key) DO UPDATE SET
			row_count = excluded.row_count,
			start_ms = excluded.start_ms,
			end_ms = excluded.end_ms,
			partial_day = excluded.partial_day,
			updated_at_utc = excluded.updated_at_utc
	`, key, rowCount, startMs, endMs, partInt, time.Now().UTC().Unix())
	return err
}

// BacktestRunRecord represents a persisted backtest execution header in backtest.db.
type BacktestRunRecord struct {
	RunID         string
	CommitHash    string
	Mode          string
	FromUTC       string
	ToUTC         string
	ExecTF        string
	OIMode        string
	UniverseMode  string
	TotalTrades   int
	WinRatePct    float64
	NetPnLUSDT    float64
	ProfitFactor  float64
	MTMMaxDDPct   float64
	ExpectancyUSD float64
	OICoveragePct float64
	ConfigJSON    string
	CreatedAtUTC  string
}

// StoredRunTrade represents a serialized simulated trade leg row in backtest.db.
type StoredRunTrade struct {
	Symbol      string
	Side        string
	EntryTimeMs int64
	ExitTimeMs  int64
	EntryPrice  float64
	ExitPrice   float64
	Qty         float64
	NetPnLUSDT  float64
	FeeUSDT     float64
	FundingUSDT float64
	ExitReason  string
}

// StoredEquityPoint represents a timeline mark-to-market equity point stored in backtest.db.
type StoredEquityPoint struct {
	TimeMs          int64
	RealizedEquity  float64
	EffectiveEquity float64
	UsedMargin      float64
	OpenPositions   int
}

// WalkForwardFoldRecord represents a persisted IS/OOS fold result in walkforward_folds.
type WalkForwardFoldRecord struct {
	FoldIndex      int
	IsFromUTC      string
	IsToUTC        string
	OOSFromUTC     string
	OOSToUTC       string
	ISExpectancy   float64
	OOSExpectancy  float64
	DegradationPct float64
	Verdict        string
}

// SaveBacktestRun atomically persists a run header along with its trades, equity curve, and WFA folds.
func (s *BacktestStore) SaveBacktestRun(
	ctx context.Context,
	run BacktestRunRecord,
	trades []StoredRunTrade,
	curve []StoredEquityPoint,
	folds []WalkForwardFoldRecord,
) error {
	if s == nil || s.conn == nil || run.RunID == "" {
		return nil
	}
	if run.CreatedAtUTC == "" {
		run.CreatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO backtest_runs (
			run_id, commit_hash, mode, from_utc, to_utc, exec_tf, oi_mode, universe_mode,
			total_trades, win_rate_pct, net_pnl_usdt, profit_factor, mtm_max_dd_pct,
			expectancy_usdt, oi_coverage_pct, config_json, created_at_utc
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			total_trades = excluded.total_trades,
			win_rate_pct = excluded.win_rate_pct,
			net_pnl_usdt = excluded.net_pnl_usdt,
			profit_factor = excluded.profit_factor,
			mtm_max_dd_pct = excluded.mtm_max_dd_pct,
			expectancy_usdt = excluded.expectancy_usdt,
			oi_coverage_pct = excluded.oi_coverage_pct,
			config_json = excluded.config_json
	`, run.RunID, run.CommitHash, run.Mode, run.FromUTC, run.ToUTC, run.ExecTF, run.OIMode, run.UniverseMode,
		run.TotalTrades, run.WinRatePct, run.NetPnLUSDT, run.ProfitFactor, run.MTMMaxDDPct,
		run.ExpectancyUSD, run.OICoveragePct, run.ConfigJSON, run.CreatedAtUTC)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM run_trades WHERE run_id = ?`, run.RunID); err != nil {
		return err
	}
	if len(trades) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO run_trades (
				run_id, symbol, side, entry_time_ms, exit_time_ms,
				entry_price, exit_price, qty, net_pnl_usdt, fee_usdt, funding_usdt, exit_reason
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		for _, t := range trades {
			if _, err := stmt.ExecContext(ctx, run.RunID, t.Symbol, t.Side, t.EntryTimeMs, t.ExitTimeMs,
				t.EntryPrice, t.ExitPrice, t.Qty, t.NetPnLUSDT, t.FeeUSDT, t.FundingUSDT, t.ExitReason); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM run_equity_curve WHERE run_id = ?`, run.RunID); err != nil {
		return err
	}
	if len(curve) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO run_equity_curve (
				run_id, time_ms, realized_equity, effective_equity, used_margin, open_positions
			) VALUES (?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		for _, pt := range curve {
			if _, err := stmt.ExecContext(ctx, run.RunID, pt.TimeMs, pt.RealizedEquity, pt.EffectiveEquity, pt.UsedMargin, pt.OpenPositions); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM walkforward_folds WHERE run_id = ?`, run.RunID); err != nil {
		return err
	}
	if len(folds) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO walkforward_folds (
				run_id, fold_index, is_from_utc, is_to_utc, oos_from_utc, oos_to_utc,
				is_expectancy, oos_expectancy, degradation_pct, verdict
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		for _, f := range folds {
			if _, err := stmt.ExecContext(ctx, run.RunID, f.FoldIndex, f.IsFromUTC, f.IsToUTC, f.OOSFromUTC, f.OOSToUTC,
				f.ISExpectancy, f.OOSExpectancy, f.DegradationPct, f.Verdict); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	return tx.Commit()
}

// ListRecentRuns returns the newest persisted backtest runs ordered by creation timestamp descending.
func (s *BacktestStore) ListRecentRuns(ctx context.Context, limit int) ([]BacktestRunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT run_id, commit_hash, mode, from_utc, to_utc, exec_tf, oi_mode, universe_mode,
			total_trades, win_rate_pct, net_pnl_usdt, profit_factor, mtm_max_dd_pct,
			expectancy_usdt, oi_coverage_pct, config_json, created_at_utc
		FROM backtest_runs
		ORDER BY created_at_utc DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []BacktestRunRecord
	for rows.Next() {
		var r BacktestRunRecord
		if err := rows.Scan(
			&r.RunID, &r.CommitHash, &r.Mode, &r.FromUTC, &r.ToUTC, &r.ExecTF, &r.OIMode, &r.UniverseMode,
			&r.TotalTrades, &r.WinRatePct, &r.NetPnLUSDT, &r.ProfitFactor, &r.MTMMaxDDPct,
			&r.ExpectancyUSD, &r.OICoveragePct, &r.ConfigJSON, &r.CreatedAtUTC,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

