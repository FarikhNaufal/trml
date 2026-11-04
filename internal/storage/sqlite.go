package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct {
	conn *sql.DB
}

const SchemaDDL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS account_daily_state (
    epoch_day_utc        INTEGER PRIMARY KEY,
    session_start_equity TEXT NOT NULL,
    realized_equity      TEXT NOT NULL,
    circuit_breaker_hit  INTEGER NOT NULL DEFAULT 0,
    updated_at_utc       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_ledger (
    client_order_id      TEXT PRIMARY KEY,
    symbol               TEXT NOT NULL,
    side                 TEXT NOT NULL,
    qty                  TEXT NOT NULL,
    entry_price          TEXT NOT NULL,
    exit_price           TEXT NOT NULL,
    sl_price_initial     TEXT NOT NULL,
    tp_price_initial     TEXT NOT NULL,
    net_pnl_usdt         TEXT NOT NULL,
    mae_pct              REAL NOT NULL DEFAULT 0.0,
    mfe_pct              REAL NOT NULL DEFAULT 0.0,
    exit_reason          TEXT NOT NULL,
    opened_at_utc        TEXT NOT NULL,
    closed_at_utc        TEXT NOT NULL,
    signal_audit_id      INTEGER NOT NULL DEFAULT 0,
    ai_autopsy           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS signal_audit_log (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    candle_open_time_utc TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    side                 TEXT NOT NULL DEFAULT 'NONE',
    close_price          REAL NOT NULL,
    ema200               REAL NOT NULL,
    session_vwap         REAL NOT NULL,
    atr14                REAL NOT NULL,
    bar_delta_zscore     REAL NOT NULL,
    oi_delta_pct         REAL NOT NULL,
    funding_rate         REAL NOT NULL,
    spread_pct           REAL NOT NULL DEFAULT 0.0,
    rvol                 REAL NOT NULL DEFAULT 0.0,
    impulse_atr_mult     REAL NOT NULL DEFAULT 0.0,
    limit_price          REAL NOT NULL DEFAULT 0.0,
    sl_price             REAL NOT NULL DEFAULT 0.0,
    tp_price             REAL NOT NULL DEFAULT 0.0,
    decision             TEXT NOT NULL,
    reject_reason_code   TEXT NOT NULL,
    reject_sub_reason    TEXT NOT NULL DEFAULT '',
    created_at_utc       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dryrun_open_positions (
    client_order_id      TEXT PRIMARY KEY,
    symbol               TEXT NOT NULL,
    side                 TEXT NOT NULL,
    qty                  TEXT NOT NULL,
    entry_price          TEXT NOT NULL,
    entry_fee            TEXT NOT NULL,
    current_sl_price     TEXT NOT NULL,
    initial_sl_price     TEXT NOT NULL DEFAULT '',
    tp_price             TEXT NOT NULL,
    atr_at_entry         TEXT NOT NULL,
    last_mark_price      TEXT NOT NULL,
    mark_updated_at_utc  TEXT NOT NULL,
    opened_at_utc        TEXT NOT NULL,
    is_breakeven_moved   INTEGER NOT NULL DEFAULT 0,
    protection_tier      INTEGER NOT NULL DEFAULT 0,
    initial_qty          TEXT NOT NULL DEFAULT '',
    tp1_price            TEXT NOT NULL DEFAULT '',
    is_tp1_hit           INTEGER NOT NULL DEFAULT 0,
    signal_audit_id      INTEGER NOT NULL DEFAULT 0,
    max_mae              REAL NOT NULL DEFAULT 0.0,
    max_mfe              REAL NOT NULL DEFAULT 0.0,
    peak_price           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS live_open_positions (
    symbol               TEXT PRIMARY KEY,
    side                 TEXT NOT NULL,
    qty                  TEXT NOT NULL,
    initial_qty          TEXT NOT NULL DEFAULT '',
    entry_price          TEXT NOT NULL,
    sl_order_id          INTEGER NOT NULL DEFAULT 0,
    sl_client_algo_id    TEXT NOT NULL DEFAULT '',
    sl_price             TEXT NOT NULL,
    initial_sl_price     TEXT NOT NULL DEFAULT '',
    tp_order_id          INTEGER NOT NULL DEFAULT 0,
    tp_client_algo_id    TEXT NOT NULL DEFAULT '',
    tp_price             TEXT NOT NULL,
    tp1_price            TEXT NOT NULL DEFAULT '',
    is_tp1_hit           INTEGER NOT NULL DEFAULT 0,
    atr_at_entry         TEXT NOT NULL DEFAULT '',
    protection_tier      INTEGER NOT NULL DEFAULT 0,
    signal_audit_id      INTEGER NOT NULL DEFAULT 0,
    opened_at_utc        TEXT NOT NULL,
    max_mae              REAL NOT NULL DEFAULT 0.0,
    max_mfe              REAL NOT NULL DEFAULT 0.0,
    updated_at_utc       TEXT NOT NULL,
    peak_price           TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_dryrun_open_positions_symbol ON dryrun_open_positions (symbol);
CREATE INDEX IF NOT EXISTS idx_live_open_positions_symbol ON live_open_positions (symbol);
CREATE INDEX IF NOT EXISTS idx_signal_audit_log_symbol_time ON signal_audit_log (symbol, candle_open_time_utc);
CREATE INDEX IF NOT EXISTS idx_signal_audit_log_created_at ON signal_audit_log (created_at_utc);
CREATE INDEX IF NOT EXISTS idx_signal_audit_log_decision_id ON signal_audit_log (decision, id DESC);
CREATE INDEX IF NOT EXISTS idx_trade_ledger_symbol ON trade_ledger (symbol);
CREATE INDEX IF NOT EXISTS idx_trade_ledger_closed_at ON trade_ledger (closed_at_utc);
`

func OpenDB(dbPath string) (*DB, error) {
	dsn := dbPath
	if dbPath != ":memory:" {
		dir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}
		sep := "?"
		if strings.Contains(dbPath, "?") {
			sep = "&"
		}
		// Pastikan PRAGMA WAL, synchronous, dan busy_timeout terkonfigurasi di DSN agar tidak hilang saat recycle
		dsn = fmt.Sprintf("%s%s_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath, sep)
	}

	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	conn.SetMaxOpenConns(1) // SQLite write serialization
	conn.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := conn.ExecContext(ctx, SchemaDDL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to initialize sqlite schema: %w", err)
	}

	ensureColumn := func(table, column, colDef string) error {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name = '%s';", table, column)
		if err := conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("check column %s.%s: %w", table, column, err)
		}
		if count == 0 {
			alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, colDef)
			if _, err := conn.ExecContext(ctx, alterQuery); err != nil {
				return fmt.Errorf("add column %s.%s: %w", table, column, err)
			}
		}
		return nil
	}

	// Migrasi otomatis kolom signal_audit_log
	auditCols := []struct{ col, def string }{
		{"reject_sub_reason", "TEXT NOT NULL DEFAULT ''"},
		{"spread_pct", "REAL NOT NULL DEFAULT 0.0"},
		{"rvol", "REAL NOT NULL DEFAULT 0.0"},
		{"impulse_atr_mult", "REAL NOT NULL DEFAULT 0.0"},
		{"limit_price", "REAL NOT NULL DEFAULT 0.0"},
		{"sl_price", "REAL NOT NULL DEFAULT 0.0"},
		{"tp_price", "REAL NOT NULL DEFAULT 0.0"},
	}
	for _, ac := range auditCols {
		if err := ensureColumn("signal_audit_log", ac.col, ac.def); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to migrate signal_audit_log schema: %w", err)
		}
	}

	// Migrasi otomatis kolom trade_ledger
	ledgerCols := []struct{ col, def string }{
		{"signal_audit_id", "INTEGER NOT NULL DEFAULT 0"},
		{"ai_autopsy", "TEXT NOT NULL DEFAULT ''"},
	}
	for _, lc := range ledgerCols {
		if err := ensureColumn("trade_ledger", lc.col, lc.def); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to migrate trade_ledger schema: %w", err)
		}
	}

	// Migrasi otomatis kolom posisi (dryrun dan live)
	posTables := []string{"dryrun_open_positions", "live_open_positions"}
	posCols := []struct{ col, def string }{
		{"protection_tier", "INTEGER NOT NULL DEFAULT 0"},
		{"initial_qty", "TEXT NOT NULL DEFAULT ''"},
		{"tp1_price", "TEXT NOT NULL DEFAULT ''"},
		{"is_tp1_hit", "INTEGER NOT NULL DEFAULT 0"},
		{"initial_sl_price", "TEXT NOT NULL DEFAULT ''"},
		{"peak_price", "TEXT NOT NULL DEFAULT ''"},
	}
	for _, tbl := range posTables {
		for _, pc := range posCols {
			if err := ensureColumn(tbl, pc.col, pc.def); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("failed to migrate %s schema: %w", tbl, err)
			}
		}
	}

	// Migrasi kolom khusus live_open_positions
	if err := ensureColumn("live_open_positions", "atr_at_entry", "TEXT NOT NULL DEFAULT ''"); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to migrate live_open_positions schema: %w", err)
	}

	if _, err := conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_trade_ledger_signal_id ON trade_ledger (signal_audit_id);`); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create index idx_trade_ledger_signal_id: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_signal_audit_log_decision_id ON signal_audit_log (decision, id DESC);`); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create index idx_signal_audit_log_decision_id: %w", err)
	}

	// Schema version check / user_version migration guard
	var userVersion int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version;").Scan(&userVersion); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to query pragma user_version: %w", err)
	}
	if userVersion == 0 {
		if _, err := conn.ExecContext(ctx, "PRAGMA user_version = 1;"); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to update pragma user_version: %w", err)
		}
	}

	return &DB{conn: conn}, nil
}

func (d *DB) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d *DB) RawConn() *sql.DB {
	return d.conn
}
