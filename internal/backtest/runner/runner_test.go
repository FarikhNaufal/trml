package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/replay"
	"trml/internal/model"
)

func TestRunner_ExecuteRunWalkForwardStressAndPersist(t *testing.T) {
	tmp := t.TempDir()
	envPath := filepath.Join(tmp, "test.env")
	if err := os.WriteFile(envPath, []byte("ENV=development\nTRADING_MODE=PAPER\nTIMEFRAME=1h\nMAX_CONCURRENT_SLOTS=3\n"), 0644); err != nil {
		t.Fatal(err)
	}

	dirs := data.ResolveBacktestDirs(filepath.Join(tmp, "cache"))
	store, err := data.OpenBacktestStore(dirs.DBPath)
	if err != nil {
		t.Fatalf("OpenBacktestStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	bars := make([]data.HistoricalCandle, 18*24)
	price := 100.0
	for i := range bars {
		price += 0.05
		bars[i] = data.HistoricalCandle{
			Symbol:      "SOLUSDT",
			OpenTime:    t0.Add(time.Duration(i) * time.Hour),
			CloseTime:   t0.Add(time.Duration(i+1) * time.Hour),
			Open:        price - 0.1,
			High:        price + 0.3,
			Low:         price - 0.3,
			Close:       price,
			Volume:      1000,
			QuoteVolume: 100000,
		}
	}

	ds := &LoadedDataset{
		Interval:      "1h",
		CandleDur:     time.Hour,
		ExecTF:        "1h",
		OIMode:        replay.OIModeOff,
		InitialEquity: 1000.0,
		Symbols:       []string{"SOLUSDT"},
		CandleMap:     map[string][]data.HistoricalCandle{"SOLUSDT": bars},
		Filters: map[string]model.ExchangeFilter{
			"SOLUSDT": {TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinQty: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5.0)},
		},
		Store: store,
	}

	outDir := filepath.Join(tmp, "reports")
	common := CommonOptions{
		EnvPath:      envPath,
		OIMode:       replay.OIModeOff,
		UniverseMode: "symbols",
		From:         t0,
		To:           t0.Add(18 * 24 * time.Hour),
		BenchmarkSym: "SOLUSDT",
		CommitHash:   "testsha",
		SaveToDB:     true,
		WarmupBars:   25,
		OutMarkdown:  filepath.Join(outDir, "run.md"),
		OutTradesCSV: filepath.Join(outDir, "trades.csv"),
		OutEquityCSV: filepath.Join(outDir, "equity.csv"),
		OutJSON:      filepath.Join(outDir, "report.json"),
	}

	runOut, err := ExecuteRun(context.Background(), ds, RunOptions{CommonOptions: common, BTCGateShortOnly: true})
	if err != nil {
		t.Fatalf("ExecuteRun failed: %v", err)
	}
	if !strings.Contains(runOut.Markdown, "Run ID") {
		t.Fatalf("expected Run ID in markdown report")
	}
	for _, p := range []string{common.OutMarkdown, common.OutTradesCSV, common.OutEquityCSV, common.OutJSON} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected output artifact %s: %v", p, err)
		}
	}

	// Execute Walk-Forward and persist
	wfaOpts := WalkForwardOptions{
		CommonOptions: common,
		TrainDays:     6,
		TestDays:      4,
		StepDays:      4,
	}
	wfaRes, wfaMD, err := ExecuteWalkForward(context.Background(), ds, wfaOpts)
	if err != nil {
		t.Fatalf("ExecuteWalkForward failed: %v", err)
	}
	if len(wfaRes.Folds) < 2 || !strings.Contains(wfaMD, "Walk-Forward Validation") {
		t.Fatalf("unexpected WFA result: folds=%d", len(wfaRes.Folds))
	}

	// Verify both runs were persisted in backtest.db
	runs, err := store.ListRecentRuns(context.Background(), 10)
	if err != nil || len(runs) != 2 {
		t.Fatalf("expected 2 persisted runs in backtest.db, got %d (err=%v)", len(runs), err)
	}

	// Execute Friction Stress Matrix
	stressRes, stressMD, err := ExecuteStress(context.Background(), ds, StressOptions{
		CommonOptions: common,
		Multipliers:   []float64{1.0, 2.0, 3.0},
		TestPen2:      true,
	})
	if err != nil {
		t.Fatalf("ExecuteStress failed: %v", err)
	}
	if len(stressRes) != 4 || !strings.Contains(stressMD, "Friction & Slippage Stress Matrix") {
		t.Fatalf("unexpected stress output: count=%d", len(stressRes))
	}
}
