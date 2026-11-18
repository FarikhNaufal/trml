package report

import (
	"encoding/json"
	"os"
	"path/filepath"

	"trml/internal/backtest/data"
	"trml/internal/backtest/metrics"
	"trml/internal/backtest/simbroker"
	"trml/internal/backtest/validation"
)

// JSONReportPayload structures complete Backtest v2 outputs for programmatic analysis or dashboard consumption.
type JSONReportPayload struct {
	RunID           string                        `json:"run_id,omitempty"`
	CommitHash      string                        `json:"commit_hash,omitempty"`
	DataRangeFrom   string                        `json:"data_range_from,omitempty"`
	DataRangeTo     string                        `json:"data_range_to,omitempty"`
	Summary         metrics.BacktestSummary       `json:"summary"`
	RejectReasons   map[string]int64              `json:"reject_reasons,omitempty"`
	MonteCarlo      *validation.MonteCarloResult  `json:"monte_carlo,omitempty"`
	WalkForward     *validation.WalkForwardReport `json:"walk_forward,omitempty"`
	StressScenarios []validation.StressScenario   `json:"stress_scenarios,omitempty"`
	Trades          []simbroker.SimTradeRecord    `json:"trades"`
	EquityCurve     []simbroker.EquityPoint       `json:"equity_curve,omitempty"`
}

// WriteJSONReport atomically serializes the structured report payload to JSON.
func WriteJSONReport(path string, payload JSONReportPayload) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return data.WriteFileAtomic(path, encoded, 0644)
}
