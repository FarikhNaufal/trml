package web

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type TradeItem struct {
	ClientOrderID  string  `json:"client_order_id"`
	Symbol         string  `json:"symbol"`
	Side           string  `json:"side"`
	StrategyID     string  `json:"strategy_id"`
	Qty            string  `json:"qty"`
	EntryPrice     string  `json:"entry_price"`
	ExitPrice      string  `json:"exit_price"`
	SLPriceInitial string  `json:"sl_price_initial"`
	TPPriceInitial string  `json:"tp_price_initial"`
	PlannedSLPct   float64 `json:"planned_sl_pct"`
	PlannedTPPct   float64 `json:"planned_tp_pct"`
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
	AIAutopsy      string  `json:"ai_autopsy"`
}

type SignalItem struct {
	ID                int64   `json:"id"`
	CandleOpenTimeUTC string  `json:"candle_open_time_utc"`
	Symbol            string  `json:"symbol"`
	Side              string  `json:"side"`
	StrategyID        string  `json:"strategy_id"`
	ClosePrice        float64 `json:"close_price"`
	EMA200            float64 `json:"ema200"`
	SessionVWAP       float64 `json:"session_vwap"`
	ATR14             float64 `json:"atr14"`
	BarDeltaZScore    float64 `json:"bar_delta_zscore"`
	OIDeltaPct        float64 `json:"oi_delta_pct"`
	FundingRate       float64 `json:"funding_rate"`
	SpreadPct         float64 `json:"spread_pct"`
	RVOL              float64 `json:"rvol"`
	ImpulseATRMult    float64 `json:"impulse_atr_mult"`
	Decision          string  `json:"decision"`
	RejectReasonCode  string  `json:"reject_reason_code"`
	RejectSubReason   string  `json:"reject_sub_reason"`
	CreatedAtUTC      string  `json:"created_at_utc"`
}

type StatsItem struct {
	TotalTrades        int     `json:"total_trades"`
	WinsCount          int     `json:"wins_count"`
	FullWinsCount      int     `json:"full_wins_count"`
	PartialWinsCount   int     `json:"partial_wins_count"`
	BreakEvenCount     int     `json:"break_even_count"`
	LossesCount        int     `json:"losses_count"`
	TotalNetPnLUSDT    float64 `json:"total_net_pnl_usdt"`
	WinRatePct         float64 `json:"win_rate_pct"`
	AdjustedWinRatePct float64 `json:"adjusted_win_rate_pct"`
	AvgWinUSDT         float64 `json:"avg_win_usdt"`
	AvgFullWinUSDT     float64 `json:"avg_full_win_usdt"`
	AvgLossUSDT        float64 `json:"avg_loss_usdt"`
	RealizedRR         float64 `json:"realized_rr"`
	BlendedRR          float64 `json:"blended_rr"`
	ProfitFactor       float64 `json:"profit_factor"`
}

type StrategyStatsItem struct {
	StrategyID         string  `json:"strategy_id"`
	TotalTrades        int     `json:"total_trades"`
	WinsCount          int     `json:"wins_count"`
	LossesCount        int     `json:"losses_count"`
	BreakEvenCount     int     `json:"break_even_count"`
	TotalNetPnLUSDT    float64 `json:"total_net_pnl_usdt"`
	WinRatePct         float64 `json:"win_rate_pct"`
	AdjustedWinRatePct float64 `json:"adjusted_win_rate_pct"`
	ProfitFactor       float64 `json:"profit_factor"`
}

type SignalAuditSummary struct {
	TotalEvaluated   int             `json:"total_evaluated"`
	TotalApproved    int             `json:"total_approved"`
	TotalRejected    int             `json:"total_rejected"`
	RejectionRatePct float64         `json:"rejection_rate_pct"`
	TopRejections    []RejectionStat `json:"top_rejections"`
}

type RejectionStat struct {
	ReasonCode string  `json:"reason_code"`
	Count      int     `json:"count"`
	Pct        float64 `json:"pct"`
}

type EquityCurvePoint struct {
	TimestampUTC  string  `json:"timestamp_utc"`
	CumulativePnL float64 `json:"cumulative_pnl"`
	Equity        float64 `json:"equity"`
	TradeCount    int     `json:"trade_count"`
}

type WebReader struct {
	db       *sql.DB
	leverage float64
	mode     string
}

// SetMode restricts all trade_ledger queries to the normalized trading mode ("DRY_RUN" or "LIVE").
// An empty mode disables mode filtering (preserving compatibility with legacy/test schemas).
func (r *WebReader) SetMode(mode string) {
	if r == nil {
		return
	}
	r.mode = strings.ToUpper(strings.TrimSpace(mode))
}

func OpenReadOnlyReader(dbPath string, leverage float64, mode ...string) (*WebReader, error) {
	sep := "?"
	if strings.Contains(dbPath, "?") {
		sep = "&"
	}
	dsn := fmt.Sprintf("%s%smode=ro&_pragma=busy_timeout(2000)&_pragma=query_only(true)", dbPath, sep)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("web reader: failed to open read-only sqlite db: %w", err)
	}

	db.SetMaxOpenConns(4)
	db.SetConnMaxLifetime(time.Hour)

	lev := leverage
	if lev <= 0 {
		lev = 8.0
	}

	normMode := ""
	if len(mode) > 0 {
		normMode = strings.ToUpper(strings.TrimSpace(mode[0]))
	}

	return &WebReader{db: db, leverage: lev, mode: normMode}, nil
}

func (r *WebReader) Close() error {
	if r != nil && r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *WebReader) modeFilterClause(colPrefix string) (string, []any) {
	if r == nil || r.mode == "" {
		return "", nil
	}
	return colPrefix + "mode = ?", []any{r.mode}
}

func enrichTradeItem(it *TradeItem, leverage float64) {
	entryF, _ := strconv.ParseFloat(it.EntryPrice, 64)
	exitF, _ := strconv.ParseFloat(it.ExitPrice, 64)
	qtyF, _ := strconv.ParseFloat(it.Qty, 64)
	slF, _ := strconv.ParseFloat(it.SLPriceInitial, 64)
	tpF, _ := strconv.ParseFloat(it.TPPriceInitial, 64)

	if entryF > 0 {
		if strings.ToUpper(it.Side) == "LONG" {
			it.PriceChangePct = ((exitF - entryF) / entryF) * 100.0
			if slF > 0 {
				it.PlannedSLPct = math.Abs((entryF-slF)/entryF) * 100.0
			}
			if tpF > 0 {
				it.PlannedTPPct = math.Abs((tpF-entryF)/entryF) * 100.0
			}
		} else {
			it.PriceChangePct = ((entryF - exitF) / entryF) * 100.0
			if slF > 0 {
				it.PlannedSLPct = math.Abs((slF-entryF)/entryF) * 100.0
			}
			if tpF > 0 {
				it.PlannedTPPct = math.Abs((entryF-tpF)/entryF) * 100.0
			}
		}

		lev := leverage
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
	if err1 != nil {
		tOpen, err1 = time.Parse(time.RFC3339, it.OpenedAtUTC)
	}
	tClose, err2 := time.Parse(time.RFC3339Nano, it.ClosedAtUTC)
	if err2 != nil {
		tClose, err2 = time.Parse(time.RFC3339, it.ClosedAtUTC)
	}
	if err1 == nil && err2 == nil {
		it.DurationSec = int64(tClose.Sub(tOpen).Seconds())
	}
}

func classifySetupOutcome(pnl float64, hasTP1, hasRunnerWin, hasBE, hasSL int) (isFullWin, isPartialWin, isBE, isLoss bool) {
	if (hasRunnerWin == 1 && pnl > 0) || (hasSL == 0 && hasBE == 0 && pnl > 0.05) {
		return true, false, false, false
	}
	if hasTP1 == 1 && pnl > 0.005 {
		return false, true, false, false
	}
	if (hasBE == 1 && pnl >= -0.05) || (hasSL == 0 && math.Abs(pnl) <= 0.05) {
		return false, false, true, false
	}
	return false, false, false, true
}

func (r *WebReader) buildSetupStatsQuery() (string, []any) {
	whereSQL := ""
	clause, args := r.modeFilterClause("")
	if clause != "" {
		whereSQL = " WHERE " + clause
	}
	query := `
		SELECT
			CASE
				WHEN signal_audit_id > 0 THEN 'SIG_' || CAST(signal_audit_id AS TEXT)
				WHEN client_order_id LIKE '%_TP1' THEN SUBSTR(client_order_id, 1, LENGTH(client_order_id) - 4)
				WHEN client_order_id LIKE 'ALGO_TP1_%' THEN 'ALGO_LIVE_' || SUBSTR(client_order_id, 10)
				ELSE client_order_id
			END AS setup_id,
			COALESCE(MAX(NULLIF(strategy_id, '')), 'VWAP_PULLBACK') AS strategy_id,
			SUM(CAST(net_pnl_usdt AS REAL)) AS setup_net_pnl,
			MAX(CASE WHEN exit_reason = 'PARTIAL_TAKE_PROFIT' THEN 1 ELSE 0 END) AS has_tp1,
			MAX(CASE WHEN exit_reason IN ('TAKE_PROFIT', 'TRAILING_STOP', 'PROFIT_LOCK') THEN 1 ELSE 0 END) AS has_runner_win,
			MAX(CASE WHEN exit_reason = 'BREAK_EVEN' THEN 1 ELSE 0 END) AS has_be,
			MAX(CASE WHEN exit_reason = 'STOP_LOSS' THEN 1 ELSE 0 END) AS has_sl
		FROM trade_ledger` + whereSQL + `
		GROUP BY setup_id;
	`
	return query, args
}

func (r *WebReader) GetTradesPaginated(ctx context.Context, page, limit int, statusFilter string, strategyFilter ...string) ([]TradeItem, int, error) {
	if r == nil || r.db == nil {
		return nil, 0, errors.New("reader: database not available")
	}
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 10
	}
	offset := (page - 1) * limit

	var whereClauses []string
	var args []any

	if modeClause, modeArgs := r.modeFilterClause(""); modeClause != "" {
		whereClauses = append(whereClauses, modeClause)
		args = append(args, modeArgs...)
	}

	if statusFilter != "" && statusFilter != "ALL" {
		sf := strings.ToUpper(strings.TrimSpace(statusFilter))
		switch sf {
		case "TAKE_PROFIT", "TP", "WIN":
			whereClauses = append(whereClauses, "exit_reason IN ('TAKE_PROFIT', 'TP', 'TRAILING_STOP', 'PROFIT_LOCK', 'PARTIAL_TAKE_PROFIT')")
		case "BREAK_EVEN", "BE":
			whereClauses = append(whereClauses, "exit_reason IN ('BREAK_EVEN', 'BE')")
		case "STOP_LOSS", "SL":
			whereClauses = append(whereClauses, "exit_reason IN ('STOP_LOSS', 'SL')")
		default:
			whereClauses = append(whereClauses, "exit_reason = ?")
			args = append(args, statusFilter)
		}
	}

	if len(strategyFilter) > 0 {
		strat := strings.ToUpper(strings.TrimSpace(strategyFilter[0]))
		if strat != "" && strat != "ALL" {
			whereClauses = append(whereClauses, "COALESCE(NULLIF(strategy_id, ''), 'VWAP_PULLBACK') = ?")
			args = append(args, strat)
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
		SELECT client_order_id, symbol, side, COALESCE(strategy_id, 'VWAP_PULLBACK'), qty, entry_price, exit_price,
		       COALESCE(sl_price_initial, ''), COALESCE(tp_price_initial, ''),
		       net_pnl_usdt, mae_pct, mfe_pct, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id,
		       COALESCE(ai_autopsy, '')
		FROM trade_ledger` + whereSQL + `
		ORDER BY closed_at_utc DESC, rowid DESC
		LIMIT ? OFFSET ?;
	`
	queryArgs := append(args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query trades: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]TradeItem, 0, limit)
	for rows.Next() {
		var it TradeItem
		var pnlStr string
		err := rows.Scan(
			&it.ClientOrderID, &it.Symbol, &it.Side, &it.StrategyID, &it.Qty, &it.EntryPrice, &it.ExitPrice,
			&it.SLPriceInitial, &it.TPPriceInitial,
			&pnlStr, &it.MAEPct, &it.MFEPct, &it.ExitReason, &it.OpenedAtUTC, &it.ClosedAtUTC, &it.SignalAuditID,
			&it.AIAutopsy,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan trade row: %w", err)
		}
		it.NetPnLUSDT, _ = strconv.ParseFloat(pnlStr, 64)
		enrichTradeItem(&it, r.leverage)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate trade rows: %w", err)
	}

	return items, total, nil
}

// GetTradeByClientOrderID mencari 1 data trade berdasarkan client_order_id
func (r *WebReader) GetTradeByClientOrderID(ctx context.Context, clientOrderID string) (*TradeItem, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("reader: database not available")
	}
	whereClauses := []string{"client_order_id = ?"}
	args := []any{clientOrderID}
	if modeClause, modeArgs := r.modeFilterClause(""); modeClause != "" {
		whereClauses = append(whereClauses, modeClause)
		args = append(args, modeArgs...)
	}
	query := `
		SELECT client_order_id, symbol, side, COALESCE(strategy_id, 'VWAP_PULLBACK'), qty, entry_price, exit_price,
		       COALESCE(sl_price_initial, ''), COALESCE(tp_price_initial, ''),
		       net_pnl_usdt, mae_pct, mfe_pct, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id,
		       COALESCE(ai_autopsy, '')
		FROM trade_ledger
		WHERE ` + strings.Join(whereClauses, " AND ") + `
		LIMIT 1;
	`
	var it TradeItem
	var pnlStr string
	err := r.db.QueryRowContext(ctx, query, args...).Scan(
		&it.ClientOrderID, &it.Symbol, &it.Side, &it.StrategyID, &it.Qty, &it.EntryPrice, &it.ExitPrice,
		&it.SLPriceInitial, &it.TPPriceInitial,
		&pnlStr, &it.MAEPct, &it.MFEPct, &it.ExitReason, &it.OpenedAtUTC, &it.ClosedAtUTC, &it.SignalAuditID,
		&it.AIAutopsy,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan trade by id: %w", err)
	}
	it.NetPnLUSDT, _ = strconv.ParseFloat(pnlStr, 64)
	enrichTradeItem(&it, r.leverage)

	return &it, nil
}

func (r *WebReader) GetSignalsPaginated(ctx context.Context, page, limit int, decisionFilter, symbolFilter string, strategyFilter ...string) ([]SignalItem, int, error) {
	if r == nil || r.db == nil {
		return nil, 0, errors.New("reader: database not available")
	}
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
		case "APPROVED", "APPROVE", "EXECUTED", "EXEC":
			whereClauses = append(whereClauses, "decision IN ('APPROVE', 'APPROVED', 'EXECUTED')")
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

	if len(strategyFilter) > 0 {
		strat := strings.ToUpper(strings.TrimSpace(strategyFilter[0]))
		if strat != "" && strat != "ALL" {
			whereClauses = append(whereClauses, "COALESCE(NULLIF(strategy_id, ''), 'VWAP_PULLBACK') = ?")
			args = append(args, strat)
		}
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
		SELECT id, candle_open_time_utc, symbol, side, COALESCE(strategy_id, 'VWAP_PULLBACK'), close_price, ema200, session_vwap, 
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
	defer func() { _ = rows.Close() }()

	items := make([]SignalItem, 0, limit)
	for rows.Next() {
		var it SignalItem
		err := rows.Scan(
			&it.ID, &it.CandleOpenTimeUTC, &it.Symbol, &it.Side, &it.StrategyID, &it.ClosePrice, &it.EMA200, &it.SessionVWAP,
			&it.ATR14, &it.BarDeltaZScore, &it.OIDeltaPct, &it.FundingRate, &it.SpreadPct, &it.RVOL,
			&it.ImpulseATRMult, &it.Decision, &it.RejectReasonCode, &it.RejectSubReason, &it.CreatedAtUTC,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan signal row: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate signal rows: %w", err)
	}

	return items, total, nil
}

func (r *WebReader) GetOverallStats(ctx context.Context) (StatsItem, error) {
	if r == nil || r.db == nil {
		return StatsItem{}, errors.New("reader: database not available")
	}
	// Agregasikan metrik per setup posisi (menggabungkan tiket partial _TP1 dan Runner menjadi 1 setup utuh)
	// agar Win Rate, Avg Win, dan Realized R:R tidak terdistorsi ke bawah oleh pembagian lot.
	query, args := r.buildSetupStatsQuery()
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return StatsItem{}, fmt.Errorf("query stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stats StatsItem
	var totalWinVal, totalFullWinVal, totalLossVal float64

	for rows.Next() {
		var setupID, stratID string
		var pnl float64
		var hasTP1, hasRunnerWin, hasBE, hasSL int
		if err := rows.Scan(&setupID, &stratID, &pnl, &hasTP1, &hasRunnerWin, &hasBE, &hasSL); err != nil {
			continue
		}
		stats.TotalTrades++
		stats.TotalNetPnLUSDT += pnl

		isFull, isPart, isBE, _ := classifySetupOutcome(pnl, hasTP1, hasRunnerWin, hasBE, hasSL)
		switch {
		case isFull:
			stats.WinsCount++
			stats.FullWinsCount++
			totalWinVal += pnl
			totalFullWinVal += pnl
		case isPart:
			stats.WinsCount++
			stats.PartialWinsCount++
			totalWinVal += pnl
		case isBE:
			stats.BreakEvenCount++
		default:
			stats.LossesCount++
			totalLossVal += math.Abs(pnl)
		}
	}
	if err := rows.Err(); err != nil {
		return StatsItem{}, fmt.Errorf("iterate stats rows: %w", err)
	}

	if stats.TotalTrades > 0 {
		stats.WinRatePct = (float64(stats.WinsCount) / float64(stats.TotalTrades)) * 100.0
	}
	decisiveTrades := stats.WinsCount + stats.LossesCount
	if decisiveTrades > 0 {
		stats.AdjustedWinRatePct = (float64(stats.WinsCount) / float64(decisiveTrades)) * 100.0
	}
	if stats.WinsCount > 0 {
		stats.AvgWinUSDT = totalWinVal / float64(stats.WinsCount)
	}
	if stats.FullWinsCount > 0 {
		stats.AvgFullWinUSDT = totalFullWinVal / float64(stats.FullWinsCount)
	}
	if stats.LossesCount > 0 {
		stats.AvgLossUSDT = totalLossVal / float64(stats.LossesCount)
	}
	if stats.AvgLossUSDT > 0 {
		if stats.FullWinsCount > 0 {
			stats.RealizedRR = stats.AvgFullWinUSDT / stats.AvgLossUSDT
		} else if stats.WinsCount > 0 {
			stats.RealizedRR = stats.AvgWinUSDT / stats.AvgLossUSDT
		}
		if stats.WinsCount > 0 {
			stats.BlendedRR = stats.AvgWinUSDT / stats.AvgLossUSDT
		}
	}
	if totalLossVal > 0 {
		stats.ProfitFactor = totalWinVal / totalLossVal
	} else if totalWinVal > 0 {
		stats.ProfitFactor = 99.0
	}

	return stats, nil
}

// GetStrategyStats mengagregasi statistik setup (WR & Net PF) per strategy_id dari trade_ledger.
func (r *WebReader) GetStrategyStats(ctx context.Context) ([]StrategyStatsItem, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("reader: database not available")
	}

	query, args := r.buildSetupStatsQuery()
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query strategy stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type accum struct {
		item    StrategyStatsItem
		winSum  float64
		lossSum float64
	}
	byStrat := make(map[string]*accum)

	for rows.Next() {
		var setupID, stratID string
		var pnl float64
		var hasTP1, hasRunnerWin, hasBE, hasSL int
		if err := rows.Scan(&setupID, &stratID, &pnl, &hasTP1, &hasRunnerWin, &hasBE, &hasSL); err != nil {
			continue
		}
		a, exists := byStrat[stratID]
		if !exists {
			a = &accum{item: StrategyStatsItem{StrategyID: stratID}}
			byStrat[stratID] = a
		}
		a.item.TotalTrades++
		a.item.TotalNetPnLUSDT += pnl

		isFull, isPart, isBE, _ := classifySetupOutcome(pnl, hasTP1, hasRunnerWin, hasBE, hasSL)
		switch {
		case isFull, isPart:
			a.item.WinsCount++
			a.winSum += pnl
		case isBE:
			a.item.BreakEvenCount++
		default:
			a.item.LossesCount++
			a.lossSum += math.Abs(pnl)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate strategy stats rows: %w", err)
	}

	keys := make([]string, 0, len(byStrat))
	for k := range byStrat {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]StrategyStatsItem, 0, len(keys))
	for _, k := range keys {
		a := byStrat[k]
		if a.item.TotalTrades > 0 {
			a.item.WinRatePct = (float64(a.item.WinsCount) / float64(a.item.TotalTrades)) * 100.0
		}
		decisive := a.item.WinsCount + a.item.LossesCount
		if decisive > 0 {
			a.item.AdjustedWinRatePct = (float64(a.item.WinsCount) / float64(decisive)) * 100.0
		}
		if a.lossSum > 0 {
			a.item.ProfitFactor = a.winSum / a.lossSum
		} else if a.winSum > 0 {
			a.item.ProfitFactor = 99.0
		}
		out = append(out, a.item)
	}
	return out, nil
}

// GetSignalAuditSummary mengagregasi seluruh log evaluasi sinyal secara akurat
func (r *WebReader) GetSignalAuditSummary(ctx context.Context) (SignalAuditSummary, error) {
	var summary SignalAuditSummary
	if r == nil || r.db == nil {
		return summary, errors.New("reader: database not available")
	}

	// 1. Agregasi total evaluasi, approved, rejected
	rows, err := r.db.QueryContext(ctx, `SELECT decision, COUNT(*) FROM signal_audit_log GROUP BY decision`)
	if err != nil {
		return summary, fmt.Errorf("query decision counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var dec string
		var cnt int
		if err := rows.Scan(&dec, &cnt); err == nil {
			summary.TotalEvaluated += cnt
			u := strings.ToUpper(strings.TrimSpace(dec))
			if u == "APPROVE" || u == "APPROVED" {
				summary.TotalApproved += cnt
			} else {
				summary.TotalRejected += cnt
			}
		}
	}
	if err := rows.Err(); err != nil {
		return summary, fmt.Errorf("iterate decision counts: %w", err)
	}

	if summary.TotalEvaluated > 0 {
		summary.RejectionRatePct = (float64(summary.TotalRejected) / float64(summary.TotalEvaluated)) * 100.0
	}

	// 2. Ambil top 5 reject reasons
	rejRows, err := r.db.QueryContext(ctx, `
		SELECT reject_reason_code, COUNT(*) as cnt 
		FROM signal_audit_log 
		WHERE decision IN ('REJECT', 'REJECTED')
		GROUP BY reject_reason_code 
		ORDER BY cnt DESC 
		LIMIT 5
	`)
	if err == nil {
		defer func() { _ = rejRows.Close() }()
		for rejRows.Next() {
			var code string
			var count int
			if err := rejRows.Scan(&code, &count); err == nil {
				if code == "" {
					code = "UNKNOWN"
				}
				pct := 0.0
				if summary.TotalRejected > 0 {
					pct = (float64(count) / float64(summary.TotalRejected)) * 100.0
				}
				summary.TopRejections = append(summary.TopRejections, RejectionStat{
					ReasonCode: code,
					Count:      count,
					Pct:        pct,
				})
			}
		}
		if err := rejRows.Err(); err != nil {
			return summary, fmt.Errorf("iterate top rejections: %w", err)
		}
	}

	return summary, nil
}

// GetTradesForPatternMiner mengambil riwayat trade untuk penambangan pola AI tanpa memuat teks autopsy lama
func (r *WebReader) GetTradesForPatternMiner(ctx context.Context, limit int) ([]TradeItem, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("reader: database not available")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	items, _, err := r.GetTradesPaginated(ctx, 1, limit, "ALL")
	if err != nil {
		return nil, err
	}
	// Kosongkan teks autopsy lama agar hemat token prompt
	for i := range items {
		items[i].AIAutopsy = ""
	}
	return items, nil
}

// GetEquityCurve membangun kurva ekuitas kronologis dari trade_ledger (terisolasi per mode)
// dengan titik awal baseline = currentEquity - totalCumulativePnL.
func (r *WebReader) GetEquityCurve(ctx context.Context, currentEquity float64, maxPoints int) ([]EquityCurvePoint, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("reader: database not available")
	}

	whereSQL := ""
	clause, args := r.modeFilterClause("")
	if clause != "" {
		whereSQL = " WHERE " + clause
	}

	query := `
		SELECT closed_at_utc, net_pnl_usdt
		FROM trade_ledger` + whereSQL + `
		ORDER BY closed_at_utc ASC, rowid ASC;
	`
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query equity curve: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type rawTrade struct {
		ts  string
		pnl float64
	}
	var raw []rawTrade
	totalCumPnL := 0.0
	for rows.Next() {
		var ts, pnlStr string
		if err := rows.Scan(&ts, &pnlStr); err != nil {
			return nil, fmt.Errorf("scan equity curve row: %w", err)
		}
		pnl, _ := strconv.ParseFloat(pnlStr, 64)
		totalCumPnL += pnl
		raw = append(raw, rawTrade{ts: ts, pnl: pnl})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate equity curve rows: %w", err)
	}

	if len(raw) == 0 {
		return []EquityCurvePoint{}, nil
	}

	baselineEquity := currentEquity - totalCumPnL
	points := make([]EquityCurvePoint, 0, len(raw)+1)
	points = append(points, EquityCurvePoint{
		TimestampUTC:  raw[0].ts,
		CumulativePnL: 0,
		Equity:        baselineEquity,
		TradeCount:    0,
	})

	cumPnL := 0.0
	for i, item := range raw {
		cumPnL += item.pnl
		points = append(points, EquityCurvePoint{
			TimestampUTC:  item.ts,
			CumulativePnL: cumPnL,
			Equity:        baselineEquity + cumPnL,
			TradeCount:    i + 1,
		})
	}

	if maxPoints > 2 && len(points) > maxPoints {
		points = downsampleEquityCurve(points, maxPoints)
	}
	return points, nil
}

func downsampleEquityCurve(points []EquityCurvePoint, maxPoints int) []EquityCurvePoint {
	n := len(points)
	if maxPoints <= 2 || n <= maxPoints {
		return points
	}

	peakIdx, troughIdx := 0, 0
	for i, pt := range points {
		if pt.Equity > points[peakIdx].Equity {
			peakIdx = i
		}
		if pt.Equity < points[troughIdx].Equity {
			troughIdx = i
		}
	}

	keep := make(map[int]bool, maxPoints+2)
	keep[0] = true
	keep[n-1] = true
	keep[peakIdx] = true
	keep[troughIdx] = true

	step := float64(n-1) / float64(maxPoints-1)
	for i := 1; i < maxPoints-1; i++ {
		idx := int(math.Round(float64(i) * step))
		if idx > 0 && idx < n-1 {
			keep[idx] = true
		}
	}

	indices := make([]int, 0, len(keep))
	for idx := range keep {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	out := make([]EquityCurvePoint, 0, len(indices))
	for _, idx := range indices {
		out = append(out, points[idx])
	}
	return out
}
