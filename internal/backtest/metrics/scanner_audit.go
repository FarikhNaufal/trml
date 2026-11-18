package metrics

import "trml/internal/backtest/simbroker"

// ScannerAudit captures aggregated dynamic watchlist behavior across a backtest run.
type ScannerAudit struct {
	CyclesRun          int     `json:"cycles_run"`
	FallbackCycles     int     `json:"fallback_cycles"`
	AvgCandidatesValid float64 `json:"avg_candidates_valid"`
	AvgSelectedCoins   float64 `json:"avg_selected_coins"`
	WatchlistTurnover  float64 `json:"watchlist_turnover_pct"`
	TotalVolRejected   int64   `json:"total_vol_rejected"`
	TotalSpreadReject  int64   `json:"total_spread_rejected"`
	TotalNATRReject    int64   `json:"total_natr_rejected"`
	TotalVWAPReject    int64   `json:"total_vwap_rejected"`
}

// ExecutionAudit summarizes order-level execution fidelity and historical OI/1m coverage.
type ExecutionAudit struct {
	simbroker.SimExecutionStats
	SubBarExecCount   int64   `json:"sub_bar_exec_count"`
	DelistForceClosed int64   `json:"delist_force_closed"`
	OICoveredCount    int64   `json:"oi_covered_count"`
	OIFallbackCount   int64   `json:"oi_fallback_count"`
	OIMissingCount    int64   `json:"oi_missing_count"`
	OICoveragePct     float64 `json:"oi_coverage_pct"`
}
