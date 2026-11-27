package strategy

import (
	"math"
	"slices"
	"sync"
	"time"

	"trml/internal/indicator"
	"trml/internal/model"
)

const (
	// DefaultMinBreadthSymbols is the minimum number of liquid non-benchmark symbols required
	// before market breadth is treated as statistically valid.
	DefaultMinBreadthSymbols = 10
	// DefaultClusterSLWindow is the rolling lookback window for counting cluster stop-losses.
	DefaultClusterSLWindow = time.Hour
)

// NeedsBTCMacroSnapshot reports whether any active strategy gate, router, or RS4h factor
// requires the benchmark BTCUSDT snapshot (SSOT across backtest ReplayEngine and live BotEngine).
func NeedsBTCMacroSnapshot(cfg EvaluatorConfig) bool {
	return cfg.BTCGateEnabled ||
		cfg.BTCChopBlockLong ||
		cfg.BTCChopBlockShort ||
		cfg.MacroRouter.Enabled ||
		cfg.RelResidual.Enabled ||
		cfg.MajorPod.IsEnabled() ||
		(cfg.BTCChopRiskMult > 0 && cfg.BTCChopRiskMult < 1.0) ||
		cfg.RS4hWeakThresholdPct < 0 ||
		cfg.RS4hStrongThresholdPct > 0
}

// CalcRequiredHistoryBars computes a bar-history buffer capacity large enough for a 24h window
// plus warmup bars (never below 1000), expanded when hourly HTF EMAs or breadth metrics need history.
func CalcRequiredHistoryBars(candleDur time.Duration, warmupBars int, htfEMAPeriod int, needMarketBreadth bool) int {
	if candleDur <= 0 {
		candleDur = 5 * time.Minute
	}
	if needMarketBreadth && htfEMAPeriod < 200 {
		htfEMAPeriod = 200
	}
	bars24h := int((24 * time.Hour) / candleDur)
	if bars24h <= 0 {
		bars24h = 288
	}
	capNeeded := bars24h + warmupBars
	if capNeeded < 1000 {
		capNeeded = 1000
	}
	if htfEMAPeriod > 0 {
		barsPerHour := int(time.Hour / candleDur)
		if barsPerHour <= 0 {
			barsPerHour = 1
		}
		htfBars := (htfEMAPeriod + 120) * barsPerHour
		if htfBars > capNeeded {
			capNeeded = htfBars
		}
	}
	return capNeeded
}

// IsAboveHourlyEMA200 evaluates whether a symbol's completed 1h close is above its 1h EMA200.
func IsAboveHourlyEMA200(candles []model.Candle) (above bool, valid bool) {
	if len(candles) < 200 {
		return false, false
	}
	hourly := CompletedHourlyCandles(candles)
	if len(hourly) < 200 {
		return false, false
	}
	ema200 := indicator.CalculateEMA(hourly, 200)
	if math.IsNaN(ema200) || math.IsInf(ema200, 0) || ema200 <= 0 {
		return false, false
	}
	return hourly[len(hourly)-1].Close > ema200, true
}

// FinalizeMarketBreadth converts counted symbols into a validated breadth fraction in [0, 1].
func FinalizeMarketBreadth(above, total, minSymbols int) (pct float64, valid bool) {
	if minSymbols <= 0 {
		minSymbols = DefaultMinBreadthSymbols
	}
	if total < minSymbols {
		return 0, false
	}
	return float64(above) / float64(total), true
}

// ComputeMarketBreadthFromMap computes 1h EMA200 market breadth across a symbol->candles map,
// excluding benchmarkSym.
func ComputeMarketBreadthFromMap(candlesBySymbol map[string][]model.Candle, benchmarkSym string, minSymbols int) (pct float64, valid bool, total int) {
	above := 0
	for sym, candles := range candlesBySymbol {
		if sym == benchmarkSym {
			continue
		}
		isAbove, ok := IsAboveHourlyEMA200(candles)
		if !ok {
			continue
		}
		total++
		if isAbove {
			above++
		}
	}
	pct, valid = FinalizeMarketBreadth(above, total, minSymbols)
	return pct, valid, total
}

// ClusterSLTracker is a leaf-locked tracker that pauses portfolio new entries after
// slCountThreshold STOP_LOSS exits within DefaultClusterSLWindow.
type ClusterSLTracker struct {
	mu            sync.Mutex
	slCountThresh int
	window        time.Duration
	cooldown      time.Duration
	recentSLTimes []time.Time
	cooldownUntil time.Time
}

// NewClusterSLTracker constructs a portfolio cluster stop-loss cooldown tracker.
func NewClusterSLTracker(slCountThreshold int, cooldown time.Duration) *ClusterSLTracker {
	return &ClusterSLTracker{
		slCountThresh: slCountThreshold,
		window:        DefaultClusterSLWindow,
		cooldown:      cooldown,
	}
}

// RecordExit records a closed trade exit and activates cooldown if the rolling STOP_LOSS threshold is hit.
func (t *ClusterSLTracker) RecordExit(exitTime time.Time, exitReason string) {
	if t == nil || t.slCountThresh <= 0 || exitReason != "STOP_LOSS" || exitTime.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.recentSLTimes = append(t.recentSLTimes, exitTime)
	latest := exitTime
	for _, ts := range t.recentSLTimes {
		if ts.After(latest) {
			latest = ts
		}
	}
	cutoff := latest.Add(-t.window)
	filtered := t.recentSLTimes[:0]
	for _, ts := range t.recentSLTimes {
		if !ts.Before(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	t.recentSLTimes = filtered
	if len(t.recentSLTimes) >= t.slCountThresh && t.cooldown > 0 {
		until := latest.Add(t.cooldown)
		if until.After(t.cooldownUntil) {
			t.cooldownUntil = until
		}
	}
}

// Hydrate replays historical STOP_LOSS timestamps chronologically to restore cooldown state after restart.
func (t *ClusterSLTracker) Hydrate(slTimes []time.Time) {
	if t == nil || t.slCountThresh <= 0 || len(slTimes) == 0 {
		return
	}
	sorted := make([]time.Time, 0, len(slTimes))
	for _, ts := range slTimes {
		if !ts.IsZero() {
			sorted = append(sorted, ts)
		}
	}
	slices.SortFunc(sorted, func(a, b time.Time) int {
		return a.Compare(b)
	})
	for _, ts := range sorted {
		t.RecordExit(ts, "STOP_LOSS")
	}
}

// IsPaused reports whether new portfolio entries are currently paused due to a stop-loss cluster.
func (t *ClusterSLTracker) IsPaused(now time.Time) (bool, time.Time) {
	if t == nil || t.slCountThresh <= 0 {
		return false, time.Time{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cooldownUntil.IsZero() || !now.Before(t.cooldownUntil) {
		return false, t.cooldownUntil
	}
	return true, t.cooldownUntil
}

// BarEntryTracker is a leaf-locked tracker enforcing MaxNewEntriesPerBar per closed candle timestamp.
type BarEntryTracker struct {
	mu          sync.Mutex
	maxPerBar   int
	countsByBar map[int64]int
}

// NewBarEntryTracker constructs a per-bar entry cap tracker. Zero or negative maxPerBar disables capping.
func NewBarEntryTracker(maxPerBar int) *BarEntryTracker {
	return &BarEntryTracker{
		maxPerBar:   maxPerBar,
		countsByBar: make(map[int64]int),
	}
}

// CanEnter checks whether another entry is permitted for the given candle bar open time.
func (b *BarEntryTracker) CanEnter(barOpenTime time.Time) bool {
	if b == nil || b.maxPerBar <= 0 || barOpenTime.IsZero() {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.countsByBar[barOpenTime.UTC().Unix()] < b.maxPerBar
}

// RecordEntry increments the entry count for barOpenTime and prunes stale bar keys.
func (b *BarEntryTracker) RecordEntry(barOpenTime time.Time) {
	if b == nil || b.maxPerBar <= 0 || barOpenTime.IsZero() {
		return
	}
	key := barOpenTime.UTC().Unix()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.countsByBar == nil {
		b.countsByBar = make(map[int64]int)
	}
	b.countsByBar[key]++
	if len(b.countsByBar) > 8 {
		keys := make([]int64, 0, len(b.countsByBar))
		for k := range b.countsByBar {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, oldK := range keys[:len(keys)-4] {
			delete(b.countsByBar, oldK)
		}
	}
}

// ReleaseEntry decrements the entry count for barOpenTime if a downstream async submission fails.
func (b *BarEntryTracker) ReleaseEntry(barOpenTime time.Time) {
	if b == nil || b.maxPerBar <= 0 || barOpenTime.IsZero() {
		return
	}
	key := barOpenTime.UTC().Unix()
	b.mu.Lock()
	defer b.mu.Unlock()
	if cnt := b.countsByBar[key]; cnt > 1 {
		b.countsByBar[key] = cnt - 1
	} else {
		delete(b.countsByBar, key)
	}
}
