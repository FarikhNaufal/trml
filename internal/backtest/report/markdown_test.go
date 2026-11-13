package report

import (
	"strings"
	"testing"

	"trml/internal/backtest/metrics"
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

