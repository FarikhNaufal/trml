package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/metrics"
	"trml/internal/backtest/simbroker"
	"trml/internal/backtest/validation"
	"trml/internal/model"
)

func TestGenerateMarkdownReport_ConclusionBranches(t *testing.T) {
	baseMeta := ReportMeta{
		CommitHash:    "test",
		DataRangeFrom: "2026-01-01",
		DataRangeTo:   "2026-10-31",
		Symbols:       []string{"BTCUSDT"},
	}

	cases := []struct {
		name    string
		summary metrics.BacktestSummary
		want    string
	}{
		{
			name: "strong positive edge",
			summary: metrics.BacktestSummary{
				TotalTrades:     250,
				ProfitFactor:    1.35,
				NetProfitFactor: 1.20,
				ExpectancyUSDT:  0.12,
				CI95LowerUSDT:   0.04,
				CI95UpperUSDT:   0.20,
			},
			want: "[!TIP] **KESIMPULAN: STRATEGI VALID DENGAN EXPECTANCY POSITIF.**",
		},
		{
			name: "moderate PF with positive 95pct CI",
			summary: metrics.BacktestSummary{
				TotalTrades:     1000,
				ProfitFactor:    1.29,
				NetProfitFactor: 1.16,
				ExpectancyUSDT:  0.11,
				CI95LowerUSDT:   0.03,
				CI95UpperUSDT:   0.20,
			},
			want: "[!NOTE] **KESIMPULAN: EXPECTANCY POSITIF TERKONFIRMASI (PF MODERAT < 1.30).**",
		},
		{
			name: "borderline CI crosses zero",
			summary: metrics.BacktestSummary{
				TotalTrades:     300,
				ProfitFactor:    1.27,
				NetProfitFactor: 1.14,
				ExpectancyUSDT:  0.10,
				CI95LowerUSDT:   -0.02,
				CI95UpperUSDT:   0.22,
			},
			want: "[!IMPORTANT] **KESIMPULAN: EXPECTANCY POSITIF TIPIS (BORDERLINE).**",
		},
		{
			name: "negative expectancy or PF below 1",
			summary: metrics.BacktestSummary{
				TotalTrades:     300,
				ProfitFactor:    0.95,
				NetProfitFactor: 0.85,
				ExpectancyUSDT:  -0.05,
				CI95LowerUSDT:   -0.15,
				CI95UpperUSDT:   0.05,
			},
			want: "[!CAUTION] **KESIMPULAN: EXPECTANCY NEGATIF ATAU PROFIT FACTOR < 1.0.**",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := GenerateMarkdownReport(tc.summary, baseMeta, nil)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("expected report conclusion %q, got:\n%s", tc.want, out)
			}
			if !strings.Contains(out, "**Net Profit Factor**") {
				t.Fatalf("expected Net Profit Factor row in report")
			}
		})
	}
}

func TestReportV2_MarkdownSectionsAndCSVJSONExporters(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	summary := metrics.BacktestSummary{
		TotalTrades:        250,
		WinsCount:          140,
		LossesCount:        110,
		WinRatePct:         56.0,
		ProfitFactor:       1.40,
		NetProfitFactor:    1.25,
		ExpectancyUSDT:     0.20,
		CI95LowerUSDT:      0.05,
		CI95UpperUSDT:      0.35,
		MTMMaxDrawdownPct:  4.50,
		MTMMaxDrawdownUSDT: 45.0,
		ExecutionAudit:     &metrics.ExecutionAudit{OICoveragePct: 92.3},
		ScannerAudit:       &metrics.ScannerAudit{CyclesRun: 12, AvgSelectedCoins: 6.5},
	}
	meta := ReportMeta{
		RunID:      "run-v2-test",
		CommitHash: "deadbeef",
		Symbols:    []string{"BTCUSDT", "ETHUSDT"},
		MonteCarlo: &validation.MonteCarloResult{Iterations: 500, P95MaxDDPct: 6.2, P99MaxDDPct: 8.1},
		WalkForward: &validation.WalkForwardReport{
			OverallVerdict: "PASS",
			Folds: []validation.WalkForwardFold{
				{FoldIndex: 0, ISFrom: now, ISTo: now.Add(24 * time.Hour), OOSFrom: now.Add(24 * time.Hour), OOSTo: now.Add(48 * time.Hour), Verdict: "PASS"},
			},
		},
		StressScenarios: []validation.StressScenario{
			{Label: "1.0x Slippage", SlippageBps: 2.0, Summary: summary},
		},
	}
	md := GenerateMarkdownReport(summary, meta, map[string]int64{"LOW_OI_DELTA": 10})
	for _, want := range []string{"MTM Max Drawdown", "Monte Carlo p95 / p99 DD", "Walk-Forward Validation", "Friction & Slippage Stress Matrix", "Audit Mikrostruktur Eksekusi"} {
		if !strings.Contains(md, want) {
			t.Fatalf("expected %q in v2 markdown report", want)
		}
	}

	dir := t.TempDir()
	trades := []simbroker.SimTradeRecord{
		{ID: "T1", Symbol: "BTCUSDT", Side: model.SideLong, EntryTime: now, ExitTime: now.Add(time.Hour), EntryPrice: decimal.NewFromFloat(50000), ExitPrice: decimal.NewFromFloat(50500), NetPnL: decimal.NewFromFloat(10)},
	}
	curve := []simbroker.EquityPoint{
		{Time: now, RealizedEquity: 1000, EffectiveEquity: 1005, UsedMargin: 20, OpenPositions: 1},
	}

	tradesCSV := filepath.Join(dir, "trades.csv")
	eqCSV := filepath.Join(dir, "equity.csv")
	jsonFile := filepath.Join(dir, "report.json")
	if err := WriteTradesCSV(tradesCSV, trades); err != nil {
		t.Fatalf("WriteTradesCSV failed: %v", err)
	}
	if err := WriteEquityCurveCSV(eqCSV, curve); err != nil {
		t.Fatalf("WriteEquityCurveCSV failed: %v", err)
	}
	if err := WriteJSONReport(jsonFile, JSONReportPayload{RunID: "run-v2-test", Summary: summary, Trades: trades, EquityCurve: curve}); err != nil {
		t.Fatalf("WriteJSONReport failed: %v", err)
	}
	for _, p := range []string{tradesCSV, eqCSV, jsonFile} {
		st, err := os.Stat(p)
		if err != nil || st.Size() == 0 {
			t.Fatalf("expected non-empty output file %s, err=%v", p, err)
		}
	}
}


