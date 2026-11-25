package strategy

import (
	"fmt"
	"math"
	"testing"
	"time"

	"trml/internal/model"
)

func TestNeedsBTCMacroSnapshot(t *testing.T) {
	if NeedsBTCMacroSnapshot(EvaluatorConfig{}) {
		t.Fatalf("expected false for empty EvaluatorConfig")
	}
	cases := []struct {
		name string
		cfg  EvaluatorConfig
	}{
		{"BTCGateEnabled", EvaluatorConfig{BTCGateEnabled: true}},
		{"BTCChopBlockLong", EvaluatorConfig{BTCChopBlockLong: true}},
		{"BTCChopBlockShort", EvaluatorConfig{BTCChopBlockShort: true}},
		{"MacroRouter", EvaluatorConfig{MacroRouter: MacroRegimeRouterConfig{Enabled: true}}},
		{"RelResidual", EvaluatorConfig{RelResidual: RelResidualConfig{Enabled: true}}},
		{"BTCChopRiskMult", EvaluatorConfig{BTCChopRiskMult: 0.75}},
		{"RS4hWeak", EvaluatorConfig{RS4hWeakThresholdPct: -2.0}},
		{"RS4hStrong", EvaluatorConfig{RS4hStrongThresholdPct: 2.0}},
	}
	for _, tc := range cases {
		if !NeedsBTCMacroSnapshot(tc.cfg) {
			t.Fatalf("expected NeedsBTCMacroSnapshot=true for %s", tc.name)
		}
	}
}

func TestClusterSLTracker_WindowCooldownAndHydrate(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	tr := NewClusterSLTracker(3, 90*time.Minute)

	tr.RecordExit(t0, "TAKE_PROFIT")
	tr.RecordExit(t0.Add(5*time.Minute), "STOP_LOSS")
	tr.RecordExit(t0.Add(20*time.Minute), "STOP_LOSS")
	if paused, _ := tr.IsPaused(t0.Add(21 * time.Minute)); paused {
		t.Fatalf("should not pause after only 2 STOP_LOSS exits")
	}

	// 3rd STOP_LOSS within 60m activates 90m cooldown
	tr.RecordExit(t0.Add(40*time.Minute), "STOP_LOSS")
	paused, until := tr.IsPaused(t0.Add(60 * time.Minute))
	if !paused || !until.Equal(t0.Add(130*time.Minute)) {
		t.Fatalf("expected pause until +130m, got paused=%v until=%v", paused, until)
	}
	if stillPaused, _ := tr.IsPaused(t0.Add(130 * time.Minute)); stillPaused {
		t.Fatalf("expected cooldown expired cleanly at exact boundary")
	}

	// Hydrate after reboot with unsorted timestamps where 2nd/3rd hit past cutoff restore active cooldown
	restored := NewClusterSLTracker(3, 90*time.Minute)
	restored.Hydrate([]time.Time{
		t0.Add(40 * time.Minute),
		t0.Add(5 * time.Minute),
		t0.Add(20 * time.Minute),
	})
	if okPaused, restUntil := restored.IsPaused(t0.Add(120 * time.Minute)); !okPaused || !restUntil.Equal(t0.Add(130*time.Minute)) {
		t.Fatalf("expected hydrated cooldown active at +120m until +130m, got paused=%v until=%v", okPaused, restUntil)
	}
}

func TestBarEntryTracker_CapAndRelease(t *testing.T) {
	bar1 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	bar2 := bar1.Add(15 * time.Minute)

	unlim := NewBarEntryTracker(0)
	unlim.RecordEntry(bar1)
	if !unlim.CanEnter(bar1) {
		t.Fatalf("expected unlimited tracker when maxPerBar <= 0")
	}

	capped := NewBarEntryTracker(2)
	if !capped.CanEnter(bar1) {
		t.Fatalf("expected bar1 initial entry permitted")
	}
	capped.RecordEntry(bar1)
	capped.RecordEntry(bar1)
	if capped.CanEnter(bar1) {
		t.Fatalf("expected bar1 blocked at cap=2")
	}
	if !capped.CanEnter(bar2) {
		t.Fatalf("expected independent bar2 still permitted")
	}
	capped.ReleaseEntry(bar1)
	if !capped.CanEnter(bar1) {
		t.Fatalf("expected ReleaseEntry to free 1 slot on bar1")
	}
}

func makeTest15mSeriesForBreadth(start time.Time, hours int, startClose, step float64) []model.Candle {
	bars := hours * 4
	out := make([]model.Candle, 0, bars)
	for i := 0; i < bars; i++ {
		ot := start.Add(time.Duration(i) * 15 * time.Minute)
		ct := ot.Add(15*time.Minute - time.Millisecond)
		c := startClose + float64(i)*step
		out = append(out, model.Candle{
			OpenTime:        ot,
			CloseTime:       ct,
			Open:            c,
			High:            c * 1.002,
			Low:             c * 0.998,
			Close:           c,
			VolumeBase:      100,
			QuoteVolumeUSDT: 100 * c,
		})
	}
	return out
}

func TestMarketBreadth_IsAboveHourlyEMA200_And_Finalize(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	bull := makeTest15mSeriesForBreadth(start, 205, 100.0, 0.2)
	bear := makeTest15mSeriesForBreadth(start, 205, 500.0, -0.3)

	if above, ok := IsAboveHourlyEMA200(bull); !ok || !above {
		t.Fatalf("expected rising series above 1h EMA200, got above=%v ok=%v", above, ok)
	}
	if above, ok := IsAboveHourlyEMA200(bear); !ok || above {
		t.Fatalf("expected falling series below 1h EMA200, got above=%v ok=%v", above, ok)
	}
	if _, ok := IsAboveHourlyEMA200(bull[:200]); ok {
		t.Fatalf("expected insufficient hourly bars (<200h) to return ok=false")
	}

	if _, valid := FinalizeMarketBreadth(2, 9, DefaultMinBreadthSymbols); valid {
		t.Fatalf("expected total < 10 invalid")
	}
	series := map[string][]model.Candle{
		"BTCUSDT": bull, // must be excluded
	}
	for i := 0; i < 3; i++ {
		series[fmt.Sprintf("BULL%dUSDT", i)] = bull
	}
	for i := 0; i < 9; i++ {
		series[fmt.Sprintf("BEAR%dUSDT", i)] = bear
	}
	pct, valid, total := ComputeMarketBreadthFromMap(series, "BTCUSDT", DefaultMinBreadthSymbols)
	if !valid || total != 12 || math.Abs(pct-0.25) > 1e-9 {
		t.Fatalf("expected breadth=0.25 total=12 valid=true, got pct=%f total=%d valid=%v", pct, total, valid)
	}
}

func TestCalcRequiredHistoryBars(t *testing.T) {
	if got := CalcRequiredHistoryBars(5*time.Minute, 200, 0, false); got != 1000 {
		t.Fatalf("expected 5m baseline 1000, got %d", got)
	}
	if got := CalcRequiredHistoryBars(15*time.Minute, 200, 200, false); got != 1280 {
		t.Fatalf("expected 15m HTF200 = 1280, got %d", got)
	}
	if got := CalcRequiredHistoryBars(15*time.Minute, 200, 0, true); got != 1280 {
		t.Fatalf("expected 15m breadth=true promotes HTF to 200 (1280 bars), got %d", got)
	}
}
