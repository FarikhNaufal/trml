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
	DeductPendingMargin      bool          // kurangi available margin intra-bar saat order disubmit
	EquityModeCompounding    bool          // compounding vs modal tetap
	MaxNewEntriesPerBar      int           // batas maksimal entry baru dalam satu bar (0 = unlimited)
	PortfolioClusterSLCount  int           // jumlah SL dalam 60 menit untuk memicu jeda portofolio (0 = off)
	PortfolioClusterCooldown time.Duration // durasi jeda saat cluster SL tercapai
	SettleOpenPositionsAtEnd bool          // tutup posisi terbuka di akhir timeline (untuk WFA window boundary)
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
	candleMap      map[string][]data.HistoricalCandle
	filters        map[string]model.ExchangeFilter
	fundingMgr     *data.FundingManager
	oiMgr          *data.MetricsOIDownloader
	lastExits      map[string]lastExitInfo
	lastMajorExits map[string]lastExitInfo

	clusterSL          *strategy.ClusterSLTracker
	lastBreadthHourKey int64
	cachedBreadthPct   float64
	cachedBreadthValid bool

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
		fundingMgr:     cfg.FundingMgr,
		oiMgr:          cfg.OIMgr,
		lastExits:      make(map[string]lastExitInfo),
		lastMajorExits: make(map[string]lastExitInfo),
		clusterSL:      strategy.NewClusterSLTracker(cfg.PortfolioClusterSLCount, cfg.PortfolioClusterCooldown),
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

	candleDur := r.cfg.ScannerCfg.CandleDuration
	if candleDur <= 0 {
		candleDur = r.evaluatorCfg.CandleDuration
	}
	htfEMACap := r.evaluatorCfg.HTFEMAPeriod
	if r.evaluatorCfg.MajorPod.IsEnabled() && htfEMACap < 200 {
		htfEMACap = 200
	}
	bufferCap := strategy.CalcRequiredHistoryBars(
		candleDur,
		r.cfg.WarmupBars,
		htfEMACap,
		r.evaluatorCfg.MarketBreadthLowThreshold > 0,
	)
	scanSim := NewScannerSimulator(r.cfg.ScannerCfg, r.filters, func(cList []model.Candle, lastClose float64) float64 {
		return calculateDynamicSpread(cList, lastClose, r.cfg.SpreadPct)
	})

	var lastScanTime time.Time
	watchedSymbols := make(map[string]bool)
	breakoutSymbols := make(map[string]bool)
	sweepExtraSymbols := make(map[string]bool)
	var primaryOrder []string
	var breakoutOrder []string
	lastClosedTradeIdx := 0
	var prevSimTime time.Time

	majorPodEnabled := r.evaluatorCfg.MajorPod.IsEnabled()
	var majorTrackSet map[string]bool
	var majorHourlyCandles map[string][]model.Candle
	hourlySubTail := 16
	if candleDur > 0 && candleDur < time.Hour {
		if req := int(time.Hour/candleDur) + 4; req > hourlySubTail {
			hourlySubTail = req
		}
	}
	if majorPodEnabled {
		majorSyms := r.evaluatorCfg.MajorPod.Symbols
		if len(majorSyms) == 0 {
			majorSyms = strategy.DefaultMajorPodSymbols()
		}
		majorTrackSet = make(map[string]bool, len(majorSyms)+1)
		majorTrackSet[r.cfg.BenchmarkSym] = true
		for _, ms := range majorSyms {
			if ms != "" {
				majorTrackSet[ms] = true
			}
		}
		majorHourlyCandles = make(map[string][]model.Candle, len(majorTrackSet))
	}

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

		if majorPodEnabled && strategy.IsHourCloseTimestamp(simTime) {
			for sym := range majorTrackSet {
				if !closedAtSimTime[sym] {
					continue
				}
				cList := rollingCandles[sym]
				if len(cList) == 0 {
					continue
				}
				startIdx := 0
				if len(cList) > hourlySubTail {
					startIdx = len(cList) - hourlySubTail
				}
				majorHourlyCandles[sym] = strategy.MergeCompletedHourlyCandle(
					majorHourlyCandles[sym], cList[startIdx:], candleDur, strategy.MajorPodHourlyHistoryCap,
				)
			}
		}

		if !r.cfg.UnconstrainedMode {
			r.broker.CheckIsolatedMarginLiquidation(0.005, simTime, int64(barIdx))
			r.broker.CheckCrossMarginLiquidation(0.005, simTime, int64(barIdx))
		}

		lastClosedTradeIdx = r.syncLastExits(lastClosedTradeIdx)

		if barIdx >= r.cfg.WarmupBars {
			r.broker.RecordTimelineEquity(simTime)

			// B. Prepare BTC macro snapshot and evaluate signals on active/watched symbols
			btcSnap := r.buildBTCMacro(simTime, rollingCandles, rollingDeltas)
			r.evaluateSignalsAtBar(simTime, sortedSymbols, primaryOrder, breakoutOrder, closedAtSimTime, watchedSymbols, breakoutSymbols, sweepExtraSymbols, rollingCandles, rollingDeltas, majorHourlyCandles, btcSnap)
		}

		// C. Post-bar dynamic scan cadence: new watchlist activates on subsequent closed bars
		// (begins at the end of the final warmup bar so barIdx == WarmupBars has an active watchlist).
		if r.cfg.DynamicScannerEnabled && barIdx >= r.cfg.WarmupBars-1 &&
			(lastScanTime.IsZero() || simTime.Sub(lastScanTime) >= r.cfg.ScannerInterval) {
			nextWatch, nextSweepExtra := scanSim.RunCyclePartitioned(sortedSymbols, rollingCandles, simTime)
			nextPrimaryOrder := append([]string(nil), scanSim.LastPrimaryOrder...)
			nextBreakoutOrder := append([]string(nil), scanSim.LastBreakoutOrder...)
			nextBreakout := make(map[string]bool, len(scanSim.LastBreakoutSet))
			for sym, ok := range scanSim.LastBreakoutSet {
				if ok {
					nextBreakout[sym] = true
				}
			}

			// Synchronize entry watchlist cleanly with BotEngine once warmup has completed
			// (a valid scan returning 0 candidates clears entry watchlists, while open positions continue stepping via stepSymbolAtTime).
			emaReq := r.cfg.ScannerCfg.EMAPeriod
			if emaReq <= 0 {
				emaReq = 200
			}
			scanWarmedUp := !lastScanTime.IsZero() || len(nextWatch) > 0 || len(nextBreakout) > 0 || len(nextSweepExtra) > 0 || barIdx >= emaReq
			if scanWarmedUp {
				watchedSymbols = nextWatch
				sweepExtraSymbols = nextSweepExtra
				primaryOrder = nextPrimaryOrder
				breakoutOrder = nextBreakoutOrder
				breakoutSymbols = nextBreakout
				lastScanTime = simTime
			}
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
	if r.cfg.SettleOpenPositionsAtEnd {
		settledAny := false
		for _, sym := range sortedSymbols {
			if !r.broker.HasActiveOrderOrPosition(sym) {
				continue
			}
			if r.broker.ForceClosePosition(sym, decimal.NewFromFloat(lastSeenClose[sym]), "WINDOW_END_SETTLE", lastTime, lastBarIdx) {
				settledAny = true
			}
		}
		if settledAny {
			r.broker.RecordTimelineEquity(lastTime)
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
				fundMark := c.Open
				if fundMark <= 0 {
					fundMark = c.Close
				}
				r.broker.ApplyFundingSettlement(sym, decimal.NewFromFloat(fundMark), decimal.NewFromFloat(rate))
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
		info := lastExitInfo{
			time:   tr.ExitTime,
			reason: tr.ExitReason,
		}
		if strategy.IsMajorPodStrategy(tr.StrategyID) {
			r.lastMajorExits[tr.Symbol] = info
			continue
		}
		r.lastExits[tr.Symbol] = info
		if r.clusterSL != nil {
			r.clusterSL.RecordExit(tr.ExitTime, tr.ExitReason)
		}
	}
	return len(r.broker.ClosedTrades)
}

func (r *ReplayEngine) buildBTCMacro(
	simTime time.Time,
	rollingCandles map[string][]model.Candle,
	rollingDeltas map[string][]float64,
) *model.MarketSnapshot {
	if !strategy.NeedsBTCMacroSnapshot(r.evaluatorCfg) {
		return nil
	}
	return BuildBTCMacroSnapshot(
		r.cfg.BenchmarkSym,
		simTime,
		rollingCandles[r.cfg.BenchmarkSym],
		rollingDeltas[r.cfg.BenchmarkSym],
		r.cfg.SpreadPct,
		r.evaluatorCfg.RollingVWAPBars,
	)
}

func (r *ReplayEngine) evaluateSignalsAtBar(
	simTime time.Time,
	sortedSymbols []string,
	primaryOrder []string,
	breakoutOrder []string,
	closedAtSimTime map[string]bool,
	watchedSymbols map[string]bool,
	breakoutSymbols map[string]bool,
	sweepExtraSymbols map[string]bool,
	rollingCandles map[string][]model.Candle,
	rollingDeltas map[string][]float64,
	majorHourlyCandles map[string][]model.Candle,
	btcSnap *model.MarketSnapshot,
) {
	majorPodEnabled := r.evaluatorCfg.MajorPod.IsEnabled()
	activeSlots := r.broker.GetActiveSlotCount()
	if majorPodEnabled {
		activeSlots = r.broker.GetActiveSlotCountByFilter(func(sid string) bool {
			return !strategy.IsMajorPodStrategy(sid)
		})
	}
	availBal := r.broker.GetAvailableBalanceUSDT()
	totEq := r.broker.GetEffectiveTotalEquityUSDT()
	isDailyBreached := r.broker.IsCircuitBreakerTripped() || (r.broker.GetDailyTotalDrawdownPct() <= r.evaluatorCfg.MaxDailyDrawdownPct)

	if r.cfg.UnconstrainedMode {
		activeSlots = 0
		isDailyBreached = false
		availBal = decimal.NewFromFloat(1000000.0)
	}
	entriesThisBar := 0
	clusterPaused := false
	if !r.cfg.UnconstrainedMode && r.clusterSL != nil {
		clusterPaused, _ = r.clusterSL.IsPaused(simTime)
	}
	breadthPct, breadthValid := r.getMarketBreadth(simTime, sortedSymbols, rollingCandles)
	portDD := r.broker.GetDrawdownFromPeakPct()

	evalOrder := sortedSymbols
	if r.cfg.DynamicScannerEnabled && (len(primaryOrder) > 0 || len(breakoutOrder) > 0) {
		ordered := make([]string, 0, len(sortedSymbols))
		seen := make(map[string]bool, len(sortedSymbols))
		for _, sym := range primaryOrder {
			if !seen[sym] {
				seen[sym] = true
				ordered = append(ordered, sym)
			}
		}
		for _, sym := range breakoutOrder {
			if !seen[sym] {
				seen[sym] = true
				ordered = append(ordered, sym)
			}
		}
		for _, sym := range sortedSymbols {
			if !seen[sym] {
				ordered = append(ordered, sym)
			}
		}
		evalOrder = ordered
	}

	for _, sym := range evalOrder {
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
		inBreakout := !r.cfg.DynamicScannerEnabled || breakoutSymbols[sym]
		inSweepExtra := r.cfg.DynamicScannerEnabled && sweepExtraSymbols[sym]
		if r.cfg.DynamicScannerEnabled && !inPrimary && !inBreakout && !inSweepExtra && !hasActive {
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
		snap.MarketBreadthPct = breadthPct
		snap.MarketBreadthValid = breadthValid
		snap.PortfolioDrawdownPct = portDD
		switch oiStatus {
		case oiStatusCovered:
			r.Stats.OICoveredCount++
		case oiStatusFallback:
			r.Stats.OIFallbackCount++
		case oiStatusMissing:
			r.Stats.OIMissingCount++
		}

		r.TotalSignalsEvaluated++
		if clusterPaused {
			r.SignalsRejected++
			r.RejectReasons["PORTFOLIO_CLUSTER_SL_COOLDOWN"]++
			continue
		}
		if !r.cfg.UnconstrainedMode && r.cfg.MaxNewEntriesPerBar > 0 && entriesThisBar >= r.cfg.MaxNewEntriesPerBar {
			r.SignalsRejected++
			r.RejectReasons["BAR_ENTRY_CAP_REACHED"]++
			continue
		}
		scope := strategy.SymbolScope{
			AllowPrimary:          inPrimary,
			AllowBreakout:         inBreakout,
			BreakoutScopeExplicit: r.cfg.ScannerCfg.BreakoutScannerEnabled,
			AllowSweep:            inPrimary || inSweepExtra,
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
			if okSub, _ := r.broker.SubmitLimitEntry(dec.Intent); okSub {
				r.SignalsApproved++
				entriesThisBar++
			} else {
				r.SignalsRejected++
				r.RejectReasons["BROKER_SUBMIT_REJECTED"]++
			}
			if !r.cfg.UnconstrainedMode {
				if majorPodEnabled {
					activeSlots = r.broker.GetActiveSlotCountByFilter(func(sid string) bool {
						return !strategy.IsMajorPodStrategy(sid)
					})
				} else {
					activeSlots = r.broker.GetActiveSlotCount()
				}
				if r.cfg.DeductPendingMargin {
					availBal = r.broker.GetAvailableBalanceUSDT()
				}
			}
		} else {
			r.SignalsRejected++
			r.RejectReasons[dec.Reason]++
		}
	}

	if majorPodEnabled && strategy.IsHourCloseTimestamp(simTime) {
		r.evaluateMajorPodAtHourClose(simTime, closedAtSimTime, rollingCandles, rollingDeltas, majorHourlyCandles, btcSnap, isDailyBreached, portDD)
	}
}

type majorPodApprovedItem struct {
	cand strategy.MajorRSMomCandidate
	snap model.MarketSnapshot
}

func (r *ReplayEngine) evaluateMajorPodAtHourClose(
	simTime time.Time,
	closedAtSimTime map[string]bool,
	rollingCandles map[string][]model.Candle,
	rollingDeltas map[string][]float64,
	majorHourlyCandles map[string][]model.Candle,
	btcSnap *model.MarketSnapshot,
	isDailyBreached bool,
	portDD float64,
) {
	symbols := r.evaluatorCfg.MajorPod.Symbols
	if len(symbols) == 0 {
		symbols = strategy.DefaultMajorPodSymbols()
	}

	btcForMajor := btcSnap
	if btcSnap != nil && len(majorHourlyCandles[r.cfg.BenchmarkSym]) > 0 {
		cp := *btcSnap
		cp.ClosedHourlyCandles = majorHourlyCandles[r.cfg.BenchmarkSym]
		btcForMajor = &cp
	}
	simClose := simTime
	if simClose.UnixMilli()%1000 == 999 {
		simClose = simClose.Add(time.Millisecond)
	}

	var approved []majorPodApprovedItem
	for _, sym := range symbols {
		if sym == r.cfg.BenchmarkSym || !closedAtSimTime[sym] {
			continue
		}
		cList, ok := rollingCandles[sym]
		if !ok || len(cList) == 0 {
			continue
		}

		exitInfo := r.lastMajorExits[sym]
		if r.cfg.UnconstrainedMode {
			exitInfo = lastExitInfo{}
		}
		fundingRate := r.cfg.FundingRate
		if r.fundingMgr != nil {
			fundingRate = r.fundingMgr.GetFundingRate(sym, simTime)
		}

		snap, evalCfg, _, okSnap := BuildSymbolSnapshot(
			sym, simTime, cList, rollingDeltas[sym], r.filters[sym], exitInfo,
			r.cfg.SpreadPct, fundingRate, OIModeOff, nil, r.evaluatorCfg,
		)
		if !okSnap {
			continue
		}
		snap.PortfolioDrawdownPct = portDD
		if len(majorHourlyCandles[sym]) > 0 {
			snap.ClosedHourlyCandles = majorHourlyCandles[sym]
		}
		if len(snap.ClosedHourlyCandles) > 0 {
			lastH := snap.ClosedHourlyCandles[len(snap.ClosedHourlyCandles)-1]
			hClose := lastH.CloseTime
			if hClose.UnixMilli()%1000 == 999 {
				hClose = hClose.Add(time.Millisecond)
			}
			if hClose.IsZero() {
				hClose = lastH.OpenTime.Add(time.Hour)
			}
			if hClose.Before(simClose) {
				r.TotalSignalsEvaluated++
				r.SignalsRejected++
				r.RejectReasons["STALE_SYM_1H_BAR"]++
				continue
			}
		}
		hasActive := r.broker.HasActiveOrderOrPosition(sym)

		r.TotalSignalsEvaluated++
		cand := strategy.EvaluateMajorRSMomCandidate(snap, btcForMajor, hasActive, isDailyBreached, evalCfg)
		if !cand.Approved {
			r.SignalsRejected++
			r.RejectReasons[cand.Reason]++
			continue
		}
		approved = append(approved, majorPodApprovedItem{cand: cand, snap: snap})
	}

	if len(approved) == 0 {
		return
	}
	sort.Slice(approved, func(i, j int) bool {
		if approved[i].cand.AbsRS24hPct != approved[j].cand.AbsRS24hPct {
			return approved[i].cand.AbsRS24hPct > approved[j].cand.AbsRS24hPct
		}
		return approved[i].cand.Symbol < approved[j].cand.Symbol
	})

	maxMajorSlots := r.evaluatorCfg.MajorPod.MaxSlots
	if maxMajorSlots <= 0 {
		maxMajorSlots = 3
	}
	majorSlots := r.broker.GetActiveSlotCountByFilter(strategy.IsMajorPodStrategy)
	totEq := r.broker.GetEffectiveTotalEquityUSDT()
	availBal := r.broker.GetAvailableBalanceUSDT()
	if r.cfg.UnconstrainedMode {
		majorSlots = 0
		availBal = decimal.NewFromFloat(1000000.0)
	}

	for _, item := range approved {
		if !r.cfg.UnconstrainedMode && majorSlots >= maxMajorSlots {
			r.SignalsRejected++
			r.RejectReasons["MAJOR_POD_SLOTS_FULL"]++
			continue
		}
		if r.broker.HasActiveOrderOrPosition(item.cand.Symbol) {
			r.SignalsRejected++
			r.RejectReasons["SYMBOL_ALREADY_ACTIVE"]++
			continue
		}
		intent, okBuild, buildReason := strategy.BuildMajorRSMomOrderIntent(item.cand, item.snap, totEq, availBal, r.evaluatorCfg)
		if !okBuild {
			r.SignalsRejected++
			r.RejectReasons[buildReason]++
			continue
		}
		if okSub, _ := r.broker.SubmitLimitEntry(intent); okSub {
			r.SignalsApproved++
			if !r.cfg.UnconstrainedMode {
				majorSlots = r.broker.GetActiveSlotCountByFilter(strategy.IsMajorPodStrategy)
				if r.cfg.DeductPendingMargin {
					availBal = r.broker.GetAvailableBalanceUSDT()
				}
			}
		} else {
			r.SignalsRejected++
			r.RejectReasons["BROKER_SUBMIT_REJECTED"]++
		}
	}
}

func (r *ReplayEngine) getMarketBreadth(
	simTime time.Time,
	sortedSymbols []string,
	rollingCandles map[string][]model.Candle,
) (float64, bool) {
	if r.evaluatorCfg.MarketBreadthLowThreshold <= 0 {
		return 0, false
	}
	hourKey := effectiveEvalTime(simTime).UTC().Unix() / 3600
	if hourKey == r.lastBreadthHourKey && r.lastBreadthHourKey != 0 {
		return r.cachedBreadthPct, r.cachedBreadthValid
	}

	candleDur := r.cfg.ScannerCfg.CandleDuration
	if candleDur <= 0 {
		candleDur = r.evaluatorCfg.CandleDuration
	}
	if candleDur <= 0 {
		candleDur = 15 * time.Minute
	}
	bars24h := int((24 * time.Hour) / candleDur)
	if bars24h <= 0 {
		bars24h = 96
	}
	minVol := r.cfg.ScannerCfg.MinVolume24hUSDT
	if minVol <= 0 {
		minVol = 15_000_000.0
	}

	above := 0
	total := 0
	for _, sym := range sortedSymbols {
		if sym == r.cfg.BenchmarkSym {
			continue
		}
		cList := rollingCandles[sym]
		if len(cList) < 200 {
			continue
		}
		startVol := len(cList) - bars24h
		if startVol < 0 {
			startVol = 0
		}
		vol24h := 0.0
		for _, c := range cList[startVol:] {
			vol24h += c.QuoteVolumeUSDT
		}
		if vol24h < minVol {
			continue
		}
		if isAbove, ok := strategy.IsAboveHourlyEMA200(cList); ok {
			total++
			if isAbove {
				above++
			}
		}
	}

	pct, valid := strategy.FinalizeMarketBreadth(above, total, strategy.DefaultMinBreadthSymbols)
	r.lastBreadthHourKey = hourKey
	r.cachedBreadthPct = pct
	r.cachedBreadthValid = valid
	return pct, valid
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

