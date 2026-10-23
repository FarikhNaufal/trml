package web

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type TradeItem struct {
	ClientOrderID  string  `json:"client_order_id"`
	Symbol         string  `json:"symbol"`
	Side           string  `json:"side"`
	Qty            string  `json:"qty"`
	EntryPrice     string  `json:"entry_price"`
	ExitPrice      string  `json:"exit_price"`
	NetPnLUSDT     float64 `json:"net_pnl_usdt"`
	PriceChangePct float64 `json:"price_change_pct"`
	ROEPct         float64 `json:"roe_pct"`
	MAEPct         float64 `json:"mae_pct"`
	MFEPct         float64 `json:"mfe_pct"`
	ExitReason     string  `json:"exit_reason"`
	OpenedAtUTC    string  `json:"opened_at_utc"`
	ClosedAtUTC    string  `json:"closed_at_utc"`
	DurationSec    int64   `json:"duration_sec"`
	SignalAuditID  int64   `json:"signal_audit_id"`
}

type SignalItem struct {
	ID                 int64   `json:"id"`
	CandleOpenTimeUTC  string  `json:"candle_open_time_utc"`
	Symbol             string  `json:"symbol"`
	Side               string  `json:"side"`
	ClosePrice         float64 `json:"close_price"`
	EMA200             float64 `json:"ema200"`
	SessionVWAP        float64 `json:"session_vwap"`
	ATR14              float64 `json:"atr14"`
	BarDeltaZScore     float64 `json:"bar_delta_zscore"`
	OIDeltaPct         float64 `json:"oi_delta_pct"`
	FundingRate        float64 `json:"funding_rate"`
	SpreadPct          float64 `json:"spread_pct"`
	RVOL               float64 `json:"rvol"`
	ImpulseATRMult     float64 `json:"impulse_atr_mult"`
	Decision           string  `json:"decision"`
	RejectReasonCode   string  `json:"reject_reason_code"`
	RejectSubReason    string  `json:"reject_sub_reason"`
	CreatedAtUTC       string  `json:"created_at_utc"`
}

type StatsItem struct {
	TotalTrades     int     `json:"total_trades"`
	WinsCount       int     `json:"wins_count"`
	LossesCount     int     `json:"losses_count"`
	BreakEvenCount  int     `json:"break_even_count"`
	TotalNetPnLUSDT float64 `json:"total_net_pnl_usdt"`
	WinRatePct      float64 `json:"win_rate_pct"`
	AvgWinUSDT      float64 `json:"avg_win_usdt"`
	AvgLossUSDT     float64 `json:"avg_loss_usdt"`
	RealizedRR      float64 `json:"realized_rr"`
}

type WebReader struct {
	db       *sql.DB
	leverage float64
}

func OpenReadOnlyReader(dbPath string, leverage ...float64) (*WebReader, error) {
	sep := "?"
	if strings.Contains(dbPath, "?") {
		sep = "&"
	}
	dsn := fmt.Sprintf("%s%smode=ro&_pragma=busy_timeout(2000)&_pragma=journal_mode(WAL)&_pragma=query_only(true)", dbPath, sep)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("web reader: failed to open read-only sqlite db: %w", err)
	}

	db.SetMaxOpenConns(4)
	db.SetConnMaxLifetime(time.Hour)

	lev := 8.0
	if len(leverage) > 0 && leverage[0] > 0 {
		lev = leverage[0]
	}

	return &WebReader{db: db, leverage: lev}, nil
}

func (r *WebReader) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *WebReader) GetTradesPaginated(ctx context.Context, page, limit int, statusFilter string) ([]TradeItem, int, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 10
	}
	offset := (page - 1) * limit

	var whereClauses []string
	var args []any

	if statusFilter != "" && statusFilter != "ALL" {
		sf := strings.ToUpper(strings.TrimSpace(statusFilter))
		switch sf {
		case "TAKE_PROFIT", "TP":
			whereClauses = append(whereClauses, "exit_reason IN ('TAKE_PROFIT', 'TP')")
		case "BREAK_EVEN", "BE":
			whereClauses = append(whereClauses, "exit_reason IN ('BREAK_EVEN', 'BE')")
		case "STOP_LOSS", "SL":
			whereClauses = append(whereClauses, "exit_reason IN ('STOP_LOSS', 'SL')")
		default:
			whereClauses = append(whereClauses, "exit_reason = ?")
			args = append(args, statusFilter)
		}
	}


	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	countQuery := "SELECT COUNT(*) FROM trade_ledger" + whereSQL
	var total int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count trades: %w", err)
	}

	query := `
		SELECT client_order_id, symbol, side, qty, entry_price, exit_price, 
		       net_pnl_usdt, mae_pct, mfe_pct, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id
		FROM trade_ledger` + whereSQL + `
		ORDER BY rowid DESC
		LIMIT ? OFFSET ?;
	`
	queryArgs := append(args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query trades: %w", err)
	}
	defer rows.Close()

	items := make([]TradeItem, 0, limit)
	for rows.Next() {
		var it TradeItem
		var pnlStr string
		err := rows.Scan(
			&it.ClientOrderID, &it.Symbol, &it.Side, &it.Qty, &it.EntryPrice, &it.ExitPrice,
			&pnlStr, &it.MAEPct, &it.MFEPct, &it.ExitReason, &it.OpenedAtUTC, &it.ClosedAtUTC, &it.SignalAuditID,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan trade row: %w", err)
		}
		it.NetPnLUSDT, _ = strconv.ParseFloat(pnlStr, 64)

		entryF, _ := strconv.ParseFloat(it.EntryPrice, 64)
		exitF, _ := strconv.ParseFloat(it.ExitPrice, 64)
		qtyF, _ := strconv.ParseFloat(it.Qty, 64)

		if entryF > 0 {
			if strings.ToUpper(it.Side) == "LONG" {
				it.PriceChangePct = ((exitF - entryF) / entryF) * 100.0
			} else {
				it.PriceChangePct = ((entryF - exitF) / entryF) * 100.0
			}

			lev := r.leverage
			if lev <= 0 {
				lev = 8.0
			}
			notional := entryF * qtyF
			if notional > 0 {
				initialMargin := notional / lev
				it.ROEPct = (it.NetPnLUSDT / initialMargin) * 100.0
			} else {
				it.ROEPct = it.PriceChangePct * lev
			}
		}

		tOpen, err1 := time.Parse(time.RFC3339Nano, it.OpenedAtUTC)
		tClose, err2 := time.Parse(time.RFC3339Nano, it.ClosedAtUTC)
		if err1 == nil && err2 == nil {
			it.DurationSec = int64(tClose.Sub(tOpen).Seconds())
		}

		items = append(items, it)
	}

	return items, total, nil
}

func (r *WebReader) GetSignalsPaginated(ctx context.Context, page, limit int, decisionFilter, symbolFilter string) ([]SignalItem, int, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 15
	}
	offset := (page - 1) * limit

	var whereClauses []string
	var args []any

	if decisionFilter != "" && decisionFilter != "ALL" {
		df := strings.ToUpper(strings.TrimSpace(decisionFilter))
		switch df {
		case "APPROVED", "APPROVE":
			whereClauses = append(whereClauses, "decision IN ('APPROVE', 'APPROVED')")
		case "REJECTED", "REJECT":
			whereClauses = append(whereClauses, "decision IN ('REJECT', 'REJECTED')")
		default:
			whereClauses = append(whereClauses, "decision = ?")
			args = append(args, decisionFilter)
		}
	}

	if symbolFilter != "" && symbolFilter != "ALL" {
		whereClauses = append(whereClauses, "symbol = ?")
		args = append(args, symbolFilter)
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	countQuery := "SELECT COUNT(*) FROM signal_audit_log" + whereSQL
	var total int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count signals: %w", err)
	}

	query := `
		SELECT id, candle_open_time_utc, symbol, side, close_price, ema200, session_vwap, 
		       atr14, bar_delta_zscore, oi_delta_pct, funding_rate, spread_pct, rvol, 
		       impulse_atr_mult, decision, reject_reason_code, reject_sub_reason, created_at_utc
		FROM signal_audit_log` + whereSQL + `
		ORDER BY id DESC
		LIMIT ? OFFSET ?;
	`
	queryArgs := append(args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query signals: %w", err)
	}
	defer rows.Close()

	items := make([]SignalItem, 0, limit)
	for rows.Next() {
		var it SignalItem
		err := rows.Scan(
			&it.ID, &it.CandleOpenTimeUTC, &it.Symbol, &it.Side, &it.ClosePrice, &it.EMA200, &it.SessionVWAP,
			&it.ATR14, &it.BarDeltaZScore, &it.OIDeltaPct, &it.FundingRate, &it.SpreadPct, &it.RVOL,
			&it.ImpulseATRMult, &it.Decision, &it.RejectReasonCode, &it.RejectSubReason, &it.CreatedAtUTC,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan signal row: %w", err)
		}
		items = append(items, it)
	}

	return items, total, nil
}

func (r *WebReader) GetOverallStats(ctx context.Context) (StatsItem, error) {
	query := `
		SELECT exit_reason, net_pnl_usdt FROM trade_ledger;
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return StatsItem{}, fmt.Errorf("query stats: %w", err)
	}
	defer rows.Close()

	var stats StatsItem
	var totalWinVal, totalLossVal float64

	for rows.Next() {
		var reason, pnlStr string
		if err := rows.Scan(&reason, &pnlStr); err != nil {
			continue
		}
		pnl, _ := strconv.ParseFloat(pnlStr, 64)
		stats.TotalTrades++
		stats.TotalNetPnLUSDT += pnl

		if reason == "BREAK_EVEN" || (reason != "TAKE_PROFIT" && reason != "STOP_LOSS" && math.Abs(pnl) <= 0.50) {
			stats.BreakEvenCount++
		} else if reason == "TAKE_PROFIT" || pnl > 0.01 {
			stats.WinsCount++
			totalWinVal += pnl
		} else {
			stats.LossesCount++
			totalLossVal += math.Abs(pnl)
		}
	}


	if stats.TotalTrades > 0 {
		stats.WinRatePct = (float64(stats.WinsCount) / float64(stats.TotalTrades)) * 100.0
	}
	if stats.WinsCount > 0 {
		stats.AvgWinUSDT = totalWinVal / float64(stats.WinsCount)
	}
	if stats.LossesCount > 0 {
		stats.AvgLossUSDT = totalLossVal / float64(stats.LossesCount)
	}
	if stats.AvgLossUSDT > 0 {
		stats.RealizedRR = stats.AvgWinUSDT / stats.AvgLossUSDT
	}

	return stats, nil
}
