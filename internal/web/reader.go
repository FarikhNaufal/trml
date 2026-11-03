package web

import (
	"context"
	"database/sql"
	"errors"
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
		case "TAKE_PROFIT", "TP", "WIN":
			whereClauses = append(whereClauses, "exit_reason IN ('TAKE_PROFIT', 'TP', 'PROFIT_LOCK', 'PARTIAL_TAKE_PROFIT')")
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
		       COALESCE(sl_price_initial, ''), COALESCE(tp_price_initial, ''),
		       net_pnl_usdt, mae_pct, mfe_pct, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id,
		       COALESCE(ai_autopsy, '')
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
			&it.SLPriceInitial, &it.TPPriceInitial,
			&pnlStr, &it.MAEPct, &it.MFEPct, &it.ExitReason, &it.OpenedAtUTC, &it.ClosedAtUTC, &it.SignalAuditID,
			&it.AIAutopsy,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan trade row: %w", err)
		}
		it.NetPnLUSDT, _ = strconv.ParseFloat(pnlStr, 64)

		entryF, _ := strconv.ParseFloat(it.EntryPrice, 64)
		exitF, _ := strconv.ParseFloat(it.ExitPrice, 64)
		qtyF, _ := strconv.ParseFloat(it.Qty, 64)
		slF, _ := strconv.ParseFloat(it.SLPriceInitial, 64)
		tpF, _ := strconv.ParseFloat(it.TPPriceInitial, 64)

		if entryF > 0 {
			if strings.ToUpper(it.Side) == "LONG" {
				it.PriceChangePct = ((exitF - entryF) / entryF) * 100.0
				if slF > 0 {
					it.PlannedSLPct = math.Abs((entryF - slF) / entryF) * 100.0
				}
				if tpF > 0 {
					it.PlannedTPPct = math.Abs((tpF - entryF) / entryF) * 100.0
				}
			} else {
				it.PriceChangePct = ((entryF - exitF) / entryF) * 100.0
				if slF > 0 {
					it.PlannedSLPct = math.Abs((slF - entryF) / entryF) * 100.0
				}
				if tpF > 0 {
					it.PlannedTPPct = math.Abs((entryF - tpF) / entryF) * 100.0
				}
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

// GetTradeByClientOrderID mencari 1 data trade berdasarkan client_order_id
func (r *WebReader) GetTradeByClientOrderID(ctx context.Context, clientOrderID string) (*TradeItem, error) {
	query := `
		SELECT client_order_id, symbol, side, qty, entry_price, exit_price,
		       COALESCE(sl_price_initial, ''), COALESCE(tp_price_initial, ''),
		       net_pnl_usdt, mae_pct, mfe_pct, exit_reason, opened_at_utc, closed_at_utc, signal_audit_id,
		       COALESCE(ai_autopsy, '')
		FROM trade_ledger
		WHERE client_order_id = ?
		LIMIT 1;
	`
	var it TradeItem
	var pnlStr string
	err := r.db.QueryRowContext(ctx, query, clientOrderID).Scan(
		&it.ClientOrderID, &it.Symbol, &it.Side, &it.Qty, &it.EntryPrice, &it.ExitPrice,
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

	entryF, _ := strconv.ParseFloat(it.EntryPrice, 64)
	exitF, _ := strconv.ParseFloat(it.ExitPrice, 64)
	qtyF, _ := strconv.ParseFloat(it.Qty, 64)
	slF, _ := strconv.ParseFloat(it.SLPriceInitial, 64)
	tpF, _ := strconv.ParseFloat(it.TPPriceInitial, 64)

	if entryF > 0 {
		if strings.ToUpper(it.Side) == "LONG" {
			it.PriceChangePct = ((exitF - entryF) / entryF) * 100.0
			if slF > 0 {
				it.PlannedSLPct = math.Abs((entryF - slF) / entryF) * 100.0
			}
			if tpF > 0 {
				it.PlannedTPPct = math.Abs((tpF - entryF) / entryF) * 100.0
			}
		} else {
			it.PriceChangePct = ((entryF - exitF) / entryF) * 100.0
			if slF > 0 {
				it.PlannedSLPct = math.Abs((slF - entryF) / entryF) * 100.0
			}
			if tpF > 0 {
				it.PlannedTPPct = math.Abs((entryF - tpF) / entryF) * 100.0
			}
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

	return &it, nil
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
	// Agregasikan metrik per setup posisi (menggabungkan tiket partial _TP1 dan Runner menjadi 1 setup utuh)
	// agar Win Rate, Avg Win, dan Realized R:R tidak terdistorsi ke bawah oleh pembagian lot.
	query := `
		SELECT 
			CASE 
				WHEN signal_audit_id > 0 THEN 'SIG_' || CAST(signal_audit_id AS TEXT)
				WHEN client_order_id LIKE '%_TP1' THEN SUBSTR(client_order_id, 1, LENGTH(client_order_id) - 4)
				ELSE client_order_id 
			END AS setup_id,
			SUM(CAST(net_pnl_usdt AS REAL)) AS setup_net_pnl,
			MAX(CASE WHEN exit_reason = 'PARTIAL_TAKE_PROFIT' THEN 1 ELSE 0 END) AS has_tp1,
			MAX(CASE WHEN exit_reason IN ('TAKE_PROFIT', 'TRAILING_STOP', 'PROFIT_LOCK') THEN 1 ELSE 0 END) AS has_runner_win,
			MAX(CASE WHEN exit_reason = 'BREAK_EVEN' THEN 1 ELSE 0 END) AS has_be,
			MAX(CASE WHEN exit_reason = 'STOP_LOSS' THEN 1 ELSE 0 END) AS has_sl
		FROM trade_ledger
		GROUP BY setup_id;
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return StatsItem{}, fmt.Errorf("query stats: %w", err)
	}
	defer rows.Close()

	var stats StatsItem
	var totalWinVal, totalFullWinVal, totalLossVal float64

	for rows.Next() {
		var setupID string
		var pnl float64
		var hasTP1, hasRunnerWin, hasBE, hasSL int
		if err := rows.Scan(&setupID, &pnl, &hasTP1, &hasRunnerWin, &hasBE, &hasSL); err != nil {
			continue
		}
		stats.TotalTrades++
		stats.TotalNetPnLUSDT += pnl

		if hasRunnerWin == 1 || (hasSL == 0 && hasBE == 0 && pnl > 0.05) {
			// Full Win: runner mencapai target penuh (TP2, Trailing Stop, atau Profit Lock)
			stats.WinsCount++
			stats.FullWinsCount++
			totalWinVal += pnl
			totalFullWinVal += pnl
		} else if hasTP1 == 1 && (hasBE == 1 || pnl > 0.005) {
			// Partial Win: TP1 mengamankan kas cuan, runner keluar di BE
			stats.WinsCount++
			stats.PartialWinsCount++
			totalWinVal += pnl
		} else if (hasBE == 1 || (hasSL == 0 && math.Abs(pnl) <= 0.05)) && hasTP1 == 0 {
			// Pure Break-Even: Tier 1 rem dini modal kembali utuh (~$0.00)
			stats.BreakEvenCount++
		} else {
			// Full Loss: Stop Loss tersenggol
			stats.LossesCount++
			totalLossVal += math.Abs(pnl)
		}
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

// GetSignalAuditSummary mengagregasi seluruh log evaluasi sinyal secara akurat
func (r *WebReader) GetSignalAuditSummary(ctx context.Context) (SignalAuditSummary, error) {
	var summary SignalAuditSummary
	if r.db == nil {
		return summary, errors.New("reader: database not available")
	}

	// 1. Agregasi total evaluasi, approved, rejected
	rows, err := r.db.QueryContext(ctx, `SELECT decision, COUNT(*) FROM signal_audit_log GROUP BY decision`)
	if err != nil {
		return summary, fmt.Errorf("query decision counts: %w", err)
	}
	defer rows.Close()

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
		defer rejRows.Close()
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
	}

	return summary, nil
}

// GetTradesForPatternMiner mengambil riwayat trade untuk penambangan pola AI tanpa memuat teks autopsy lama
func (r *WebReader) GetTradesForPatternMiner(ctx context.Context, limit int) ([]TradeItem, error) {
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

