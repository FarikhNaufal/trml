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
    signal_audit_id      INTEGER NOT NULL DEFAULT 0
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

	// Migrasi otomatis kolom signal_audit_log jika database lama belum memilikinya
	var colCount int
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'reject_sub_reason';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN reject_sub_reason TEXT NOT NULL DEFAULT '';`)
	}
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'spread_pct';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN spread_pct REAL NOT NULL DEFAULT 0.0;`)
	}
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'rvol';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN rvol REAL NOT NULL DEFAULT 0.0;`)
	}
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'impulse_atr_mult';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN impulse_atr_mult REAL NOT NULL DEFAULT 0.0;`)
	}
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'limit_price';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN limit_price REAL NOT NULL DEFAULT 0.0;`)
	}
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'sl_price';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN sl_price REAL NOT NULL DEFAULT 0.0;`)
	}
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('signal_audit_log') WHERE name = 'tp_price';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE signal_audit_log ADD COLUMN tp_price REAL NOT NULL DEFAULT 0.0;`)
	}

	// Migrasi otomatis kolom signal_audit_id di trade_ledger jika belum ada
	_ = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('trade_ledger') WHERE name = 'signal_audit_id';`).Scan(&colCount)
	if colCount == 0 {
		_, _ = conn.ExecContext(ctx, `ALTER TABLE trade_ledger ADD COLUMN signal_audit_id INTEGER NOT NULL DEFAULT 0;`)
	}
	_, _ = conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_trade_ledger_signal_id ON trade_ledger (signal_audit_id);`)
	_, _ = conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_signal_audit_log_decision_id ON signal_audit_log (decision, id DESC);`)

	// Schema version check / user_version migration guard
	var userVersion int
	_ = conn.QueryRowContext(ctx, "PRAGMA user_version;").Scan(&userVersion)
	if userVersion == 0 {
		_, _ = conn.ExecContext(ctx, "PRAGMA user_version = 1;")
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
