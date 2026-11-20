package replay

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/strategy"
)

// ReplayConfig configures synchronized multi-symbol bar-by-bar simulation.
type ReplayConfig struct {
	WarmupBars            int                              // jumlah bar awal untuk warmup indikator (default 200)
	SpreadPct             float64                          // baseline estimasi spread jika dyn calc gagal (default 0.0002)
	FundingRate           float64                          // default fallback funding jika manager kosong (0.01% = 0.0001)
	FundingMgr            *data.FundingManager             // manager data historical funding rate Binance Vision/API
	OIMode                string                           // "auto" | "strict" | "off" (default "auto")
	OIMgr                 *data.MetricsOIDownloader        // loader 5m historical OI metrics Binance Vision
	ExecCandleMap         map[string][]data.HistoricalCandle // opsional: bar eksekusi 1m untuk sub-bar step broker
	BenchmarkSym          string                           // default "BTCUSDT"
	DynamicScannerEnabled bool                             // apakah dynamic rolling scanner aktif
	ScannerInterval       time.Duration                    // interval scanning ulang (default 15 menit)
	ScannerCfg            scanner.ScannerConfig            // konfigurasi scanner produksi
	UnconstrainedMode     bool                             // mode evaluasi sinyal murni (tanpa batas slot portofolio & cooldown)
	DeductPendingMargin   bool                             // kurangi available margin intra-bar saat order disubmit
	EquityModeCompounding bool                             // compounding vs modal tetap
}

// ReplayStats captures execution, scanner, and data-coverage telemetry produced by a ReplayEngine run.
type ReplayStats struct {
	OICoveredCount    int64             `json:"oi_covered_count"`
	OIFallbackCount   int64             `json:"oi_fallback_count"`
	OIMissingCount    int64             `json:"oi_missing_count"`
	OICoveragePct     float64           `json:"oi_coverage_pct"`
	SubBarExecCount   int64             `json:"sub_bar_exec_count"`
	DelistForceClosed int64             `json:"delist_force_closed"`
	ScannerStats      ScannerAuditStats `json:"scanner_stats"`
}

type lastExitInfo struct {
	time   time.Time
	reason string
}

// ReplayEngine replays historical market candles, scanner cycles, signal generation, and simulated execution.
type ReplayEngine struct {
	cfg          ReplayConfig
	evaluatorCfg strategy.EvaluatorConfig
	broker       *simbroker.SimBroker
	candleMap    map[string][]data.HistoricalCandle
	filters      map[string]model.ExchangeFilter
	fundingMgr   *data.FundingManager
	oiMgr        *data.MetricsOIDownloader
	lastExits    map[string]lastExitInfo

	// Signal observability
	TotalSignalsEvaluated int64
	SignalsApproved       int64
	SignalsRejected       int64
	RejectReasons         map[string]int64
	Stats                 ReplayStats
}

func NewReplayEngine(
	cfg ReplayConfig,
	evaluatorCfg strategy.EvaluatorConfig,
	broker *simbroker.SimBroker,
	candleMap map[string][]data.HistoricalCandle,
	filters map[string]model.ExchangeFilter,
) *ReplayEngine {
	if cfg.WarmupBars <= 0 {
		cfg.WarmupBars = 200
	}
	if cfg.SpreadPct <= 0 {
		cfg.SpreadPct = 0.0002
	}
	if cfg.BenchmarkSym == "" {
		cfg.BenchmarkSym = "BTCUSDT"
	}
	cfg.OIMode = NormalizeOIMode(cfg.OIMode)
	if evaluatorCfg.MaxDailyDrawdownPct >= 0 {
		evaluatorCfg.MaxDailyDrawdownPct = -0.05
	}
	if cfg.DynamicScannerEnabled && cfg.ScannerInterval <= 0 {
		cfg.ScannerInterval = 15 * time.Minute
	}

	return &ReplayEngine{
		cfg:           cfg,
		evaluatorCfg:  evaluatorCfg,
		broker:        broker,
		candleMap:     candleMap,
		filters:       filters,
		fundingMgr:    cfg.FundingMgr,
		oiMgr:         cfg.OIMgr,
		lastExits:     make(map[string]lastExitInfo),
		RejectReasons: make(map[string]int64),
	}
}

// SetFundingManager menyetel atau memperbarui data funding rate historis Binance.
func (r *ReplayEngine) SetFundingManager(fm *data.FundingManager) {
	r.fundingMgr = fm
}

// SetOIDownloader menyetel atau memperbarui loader Open Interest 5m historis.
func (r *ReplayEngine) SetOIDownloader(oi *data.MetricsOIDownloader) {
	r.oiMgr = oi
}

// Run mengeksekusi replay tersinkronisasi bar-demi-bar lintas seluruh simbol.
func (r *ReplayEngine) Run(ctx context.Context) error {
	timeline := r.buildTimeline()
	if len(timeline) == 0 {
		return fmt.Errorf("no candle data available for replay")
	}

	sortedSymbols := make([]string, 0, len(r.candleMap))
	for sym := range r.candleMap {
		sortedSymbols = append(sortedSymbols, sym)
	}
	sort.Strings(sortedSymbols)

	for sym, f := range r.filters {
		r.broker.SetExchangeFilter(sym, f)
	}

	candleIndices := make(map[string]int, len(sortedSymbols))
	execIndices := make(map[string]int, len(r.cfg.ExecCandleMap))
	rollingCandles := make(map[string][]model.Candle, len(sortedSymbols))
	rollingDeltas := make(map[string][]float64, len(sortedSymbols))
	lastSeenBarTime := make(map[string]time.Time, len(sortedSymbols))
	lastSeenClose := make(map[string]float64, len(sortedSymbols))

	bufferCap := CalcRollingBufferCap(r.cfg.ScannerCfg.CandleDuration, r.cfg.WarmupBars)
	scanSim := NewScannerSimulator(r.cfg.ScannerCfg, r.filters, func(cList []model.Candle, lastClose float64) float64 {
		return calculateDynamicSpread(cList, lastClose, r.cfg.SpreadPct)
	})

	var lastScanTime time.Time
	watchedSymbols := make(map[string]bool)
	sweepExtraSymbols := make(map[string]bool)
	lastClosedTradeIdx := 0
	var prevSimTime time.Time

	for barIdx, simTime := range timeline {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// A. Step execution & update rolling buffers per symbol at simTime
		closedAtSimTime := make(map[string]bool, len(sortedSymbols))
		for _, sym := range sortedSymbols {
			if r.stepSymbolAtTime(
				sym, simTime, prevSimTime, int64(barIdx), bufferCap,
				candleIndices, execIndices, rollingCandles, rollingDeltas,
				lastSeenBarTime, lastSeenClose,
			) {
				closedAtSimTime[sym] = true
			}
		}

		lastClosedTradeIdx = r.syncLastExits(lastClosedTradeIdx)

		if barIdx >= r.cfg.WarmupBars {
			r.broker.RecordTimelineEquity(simTime)

			// B. Prepare BTC macro snapshot and evaluate signals on active/watched symbols
			btcSnap := r.buildBTCMacro(simTime, rollingCandles, rollingDeltas)
			r.evaluateSignalsAtBar(simTime, sortedSymbols, closedAtSimTime, watchedSymbols, sweepExtraSymbols, rollingCandles, rollingDeltas, btcSnap)
		}

		// C. Post-bar dynamic scan cadence: new watchlist activates on subsequent closed bars
		// (begins at the end of the final warmup bar so barIdx == WarmupBars has an active watchlist).
		if r.cfg.DynamicScannerEnabled && barIdx >= r.cfg.WarmupBars-1 &&
			(lastScanTime.IsZero() || simTime.Sub(lastScanTime) >= r.cfg.ScannerInterval) {
			lastScanTime = simTime
			watchedSymbols, sweepExtraSymbols = scanSim.RunCyclePartitioned(sortedSymbols, rollingCandles, simTime)
		}

		prevSimTime = simTime
	}

	// Settle any dangling positions whose symbol data ended prematurely before the simulation horizon.
	lastTime := timeline[len(timeline)-1]
	lastBarIdx := int64(len(timeline) - 1)
	for _, sym := range sortedSymbols {
		if !r.broker.HasActiveOrderOrPosition(sym) {
			continue
		}
		lastSymTime := lastSeenBarTime[sym]
		if !lastSymTime.IsZero() && lastTime.Sub(lastSymTime) >= 15*time.Minute {
			if r.broker.ForceClosePosition(sym, decimal.NewFromFloat(lastSeenClose[sym]), "DELIST_OR_DATA_END_SETTLE", lastTime, lastBarIdx) {
				r.Stats.DelistForceClosed++
			}
		}
	}

	r.finalizeStats(scanSim.Stats)
	return nil
}

func (r *ReplayEngine) buildTimeline() []time.Time {
	timeMap := make(map[int64]time.Time)
	for _, candles := range r.candleMap {
		for _, c := range candles {
			timeMap[c.CloseTime.UnixMilli()] = c.CloseTime
		}
	}
	timeline := make([]time.Time, 0, len(timeMap))
	for _, t := range timeMap {
		timeline = append(timeline, t)
	}
	sort.Slice(timeline, func(i, j int) bool {
		return timeline[i].Before(timeline[j])
	})
	return timeline
}

func (r *ReplayEngine) stepSymbolAtTime(
	sym string,
	simTime, prevSimTime time.Time,
	barIndex int64,
	bufferCap int,
	candleIndices, execIndices map[string]int,
	rollingCandles map[string][]model.Candle,
	rollingDeltas map[string][]float64,
	lastSeenBarTime map[string]time.Time,
	lastSeenClose map[string]float64,
) bool {
	candles := r.candleMap[sym]
	idx := candleIndices[sym]
	hasBar := idx < len(candles) && candles[idx].CloseTime.Equal(simTime)

	if hasBar {
		c := candles[idx]
		lastSeenBarTime[sym] = c.CloseTime
		lastSeenClose[sym] = c.Close

		if r.fundingMgr != nil {
			if rate, isSettlement := r.fundingMgr.CheckSettlement(sym, c.OpenTime, c.CloseTime); isSettlement {
				r.broker.ApplyFundingSettlement(sym, decimal.NewFromFloat(c.Close), decimal.NewFromFloat(rate))
			}
		}

		if !r.processExecSubBars(sym, simTime, prevSimTime, barIndex, execIndices) {
			r.broker.ProcessCandle(c, barIndex)
		}

		rollingCandles[sym] = append(rollingCandles[sym], c.ToModelCandle())
		rollingDeltas[sym] = append(rollingDeltas[sym], c.BarDeltaUSDT)
		if len(rollingCandles[sym]) > bufferCap {
			rollingCandles[sym] = rollingCandles[sym][len(rollingCandles[sym])-bufferCap:]
		}
		if len(rollingDeltas[sym]) > bufferCap {
			rollingDeltas[sym] = rollingDeltas[sym][len(rollingDeltas[sym])-bufferCap:]
		}
		candleIndices[sym]++
		return true
	}

	// No base bar at simTime: advance any 1m cursor cleanly and settle delisted/stale open positions.
	_ = r.processExecSubBars(sym, simTime, prevSimTime, barIndex, execIndices)
	if r.broker.HasActiveOrderOrPosition(sym) {
		lastSym := lastSeenBarTime[sym]
		if !lastSym.IsZero() && simTime.Sub(lastSym) >= 2*time.Hour {
			if r.broker.ForceClosePosition(sym, decimal.NewFromFloat(lastSeenClose[sym]), "DELIST_OR_DATA_END_SETTLE", simTime, barIndex) {
				r.Stats.DelistForceClosed++
			}
		}
	}
	return false
}

func (r *ReplayEngine) processExecSubBars(
	sym string,
	simTime, prevSimTime time.Time,
	barIndex int64,
	execIndices map[string]int,
) bool {
	if len(r.cfg.ExecCandleMap) == 0 {
		return false
	}
	subCandles := r.cfg.ExecCandleMap[sym]
	if len(subCandles) == 0 {
		return false
	}

	idx := execIndices[sym]
	if !prevSimTime.IsZero() {
		for idx < len(subCandles) && !subCandles[idx].CloseTime.After(prevSimTime) {
			idx++
		}
	}

	processed := false
	for idx < len(subCandles) && !subCandles[idx].CloseTime.After(simTime) {
		if r.broker.HasActiveOrderOrPosition(sym) {
			r.broker.ProcessCandle(subCandles[idx], barIndex)
			r.Stats.SubBarExecCount++
			processed = true
		}
		idx++
	}
	execIndices[sym] = idx
	return processed
}

func (r *ReplayEngine) syncLastExits(fromIdx int) int {
	r.broker.Mu.Lock()
	defer r.broker.Mu.Unlock()

	for i := fromIdx; i < len(r.broker.ClosedTrades); i++ {
		tr := r.broker.ClosedTrades[i]
		if tr.ExitReason == "PARTIAL_TAKE_PROFIT" {
			continue
		}
		r.lastExits[tr.Symbol] = lastExitInfo{
			time:   tr.ExitTime,
			reason: tr.ExitReason,
		}
	}
	return len(r.broker.ClosedTrades)
}

func (r *ReplayEngine) buildBTCMacro(
	simTime time.Time,
	rollingCandles map[string][]model.Candle,
	rollingDeltas map[string][]float64,
) *model.MarketSnapshot {
	if !r.evaluatorCfg.BTCGateEnabled && !r.evaluatorCfg.BTCChopBlockLong && !r.evaluatorCfg.BTCChopBlockShort &&
		(r.evaluatorCfg.BTCChopRiskMult <= 0 || r.evaluatorCfg.BTCChopRiskMult >= 1.0) {
		return nil
	}
	return BuildBTCMacroSnapshot(
		r.cfg.BenchmarkSym,
		simTime,
		rollingCandles[r.cfg.BenchmarkSym],
		rollingDeltas[r.cfg.BenchmarkSym],
		r.cfg.SpreadPct,
	)
}

func (r *ReplayEngine) evaluateSignalsAtBar(
	simTime time.Time,
	sortedSymbols []string,
	closedAtSimTime map[string]bool,
	watchedSymbols map[string]bool,
	sweepExtraSymbols map[string]bool,
	rollingCandles map[string][]model.Candle,
	rollingDeltas map[string][]float64,
	btcSnap *model.MarketSnapshot,
) {
	activeSlots := r.broker.GetActiveSlotCount()
	availBal := r.broker.GetAvailableBalanceUSDT()
	totEq := r.broker.GetEffectiveTotalEquityUSDT()
	isDailyBreached := r.broker.IsCircuitBreakerTripped() || (r.broker.GetDailyTotalDrawdownPct() <= r.evaluatorCfg.MaxDailyDrawdownPct)

	if r.cfg.UnconstrainedMode {
		activeSlots = 0
		isDailyBreached = false
		availBal = decimal.NewFromFloat(1000000.0)
	}

	for _, sym := range sortedSymbols {
		if !closedAtSimTime[sym] {
			continue
		}
		cList, ok := rollingCandles[sym]
		if !ok {
			continue
		}
		if sym == r.cfg.BenchmarkSym && (len(sortedSymbols) > 1 || !r.isSymbolTarget(sym)) {
			continue
		}
		hasActive := r.broker.HasActiveOrderOrPosition(sym)
		inPrimary := !r.cfg.DynamicScannerEnabled || watchedSymbols[sym]
		inSweepExtra := r.cfg.DynamicScannerEnabled && sweepExtraSymbols[sym]
		if r.cfg.DynamicScannerEnabled && !inPrimary && !inSweepExtra && !hasActive {
			continue
		}
		if len(cList) < r.evaluatorCfg.EMAPeriod {
			continue
		}

		exitInfo := r.lastExits[sym]
		if r.cfg.UnconstrainedMode {
			exitInfo = lastExitInfo{}
		}

		fundingRate := r.cfg.FundingRate
		if r.fundingMgr != nil {
			fundingRate = r.fundingMgr.GetFundingRate(sym, simTime)
		}

		snap, evalCfg, oiStatus, okSnap := BuildSymbolSnapshot(
			sym, simTime, cList, rollingDeltas[sym], r.filters[sym], exitInfo,
			r.cfg.SpreadPct, fundingRate, r.cfg.OIMode, r.oiMgr, r.evaluatorCfg,
		)
		if !okSnap {
			continue
		}
		switch oiStatus {
		case oiStatusCovered:
			r.Stats.OICoveredCount++
		case oiStatusFallback:
			r.Stats.OIFallbackCount++
		case oiStatusMissing:
			r.Stats.OIMissingCount++
		}

		r.TotalSignalsEvaluated++
		scope := strategy.SymbolScope{
			AllowPrimary: inPrimary,
			AllowSweep:   inPrimary || inSweepExtra,
		}
		dec := strategy.EvaluatePortfolio(
			snap,
			btcSnap,
			activeSlots,
			isDailyBreached,
			hasActive,
			totEq,
			availBal,
			scope,
			evalCfg,
		)

		if dec.Approved {
			r.SignalsApproved++
			_, _ = r.broker.SubmitLimitEntry(dec.Intent)
			if !r.cfg.UnconstrainedMode {
				activeSlots = r.broker.GetActiveSlotCount()
				if r.cfg.DeductPendingMargin {
					availBal = r.broker.GetAvailableBalanceUSDT()
				}
			}
		} else {
			r.SignalsRejected++
			r.RejectReasons[dec.Reason]++
		}
	}
}

func (r *ReplayEngine) finalizeStats(scanStats ScannerAuditStats) {
	r.Stats.ScannerStats = scanStats
	totalOI := r.Stats.OICoveredCount + r.Stats.OIFallbackCount + r.Stats.OIMissingCount
	if totalOI > 0 {
		r.Stats.OICoveragePct = (float64(r.Stats.OICoveredCount) / float64(totalOI)) * 100.0
	}
}

func (r *ReplayEngine) isSymbolTarget(symbol string) bool {
	_, ok := r.filters[symbol]
	return ok
}

