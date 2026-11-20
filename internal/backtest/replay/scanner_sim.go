package replay

import (
	"math"
	"time"

	"trml/internal/model"
	"trml/internal/scanner"
)

// ScannerAuditStats summarizes dynamic universe selection health across a backtest run.
type ScannerAuditStats struct {
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

// ScannerSimulator encapsulates periodic dynamic watchlist selection so its output
// becomes active only on subsequent closed bars (matching Live +25s scan cadence without look-ahead).
type ScannerSimulator struct {
	cfg      scanner.ScannerConfig
	filters  map[string]model.ExchangeFilter
	spreadFn func([]model.Candle, float64) float64

	lastWatchlist      map[string]bool
	sumCandidatesValid int64
	sumSelected        int64
	sumTurnoverPct     float64
	turnoverSamples    int
	Stats              ScannerAuditStats
}

// NewScannerSimulator constructs a deterministic simulator wrapper around scanner.RunBulkMarketScannerWithStats.
func NewScannerSimulator(
	cfg scanner.ScannerConfig,
	filters map[string]model.ExchangeFilter,
	spreadFn func([]model.Candle, float64) float64,
) *ScannerSimulator {
	return &ScannerSimulator{
		cfg:           cfg,
		filters:       filters,
		spreadFn:      spreadFn,
		lastWatchlist: make(map[string]bool),
	}
}

// RunCycle evaluates the rolling universe at simTime and returns the primary watchlist.
func (s *ScannerSimulator) RunCycle(
	sortedSymbols []string,
	rollingCandles map[string][]model.Candle,
	simTime time.Time,
) map[string]bool {
	primaryWatch, _ := s.RunCyclePartitioned(sortedSymbols, rollingCandles, simTime)
	return primaryWatch
}

// RunCyclePartitioned evaluates the rolling universe at simTime using a single scanner pass,
// returning disjoint primary (VWAP Pullback) and sweep-extra (extended-VWAP SHORT sweep) watchlists.
func (s *ScannerSimulator) RunCyclePartitioned(
	sortedSymbols []string,
	rollingCandles map[string][]model.Candle,
	simTime time.Time,
) (map[string]bool, map[string]bool) {
	vol24hMap := make(map[string]float64, len(rollingCandles))
	bookMap := make(map[string]model.BookQuote, len(rollingCandles))
	activeStates := make(map[string]*model.SymbolState)

	bars24h := 288
	if s.cfg.CandleDuration > 0 {
		bars24h = int((24 * time.Hour) / s.cfg.CandleDuration)
	}
	if bars24h <= 0 {
		bars24h = 288
	}

	for _, sym := range sortedSymbols {
		cList, ok := rollingCandles[sym]
		if !ok || len(cList) == 0 {
			continue
		}
		lastC := cList[len(cList)-1]
		maxGap := 3 * s.cfg.CandleDuration
		if maxGap < 15*time.Minute {
			maxGap = 15 * time.Minute
		}
		if !lastC.CloseTime.IsZero() && simTime.Sub(lastC.CloseTime) > maxGap {
			continue
		}
		startIdx := len(cList) - bars24h
		if startIdx < 0 {
			startIdx = 0
		}
		var sumVol float64
		for _, c := range cList[startIdx:] {
			sumVol += c.QuoteVolumeUSDT
		}
		vol24hMap[sym] = sumVol

		lastClose := cList[len(cList)-1].Close
		symSpread := s.spreadFn(cList, lastClose)
		bookMap[sym] = model.BookQuote{
			BestBid: lastClose * (1.0 - symSpread/2.0),
			BestAsk: lastClose * (1.0 + symSpread/2.0),
		}
	}

	fetchKlinesFn := func(symbol string, limit int) []model.Candle {
		cList := rollingCandles[symbol]
		if len(cList) == 0 {
			return nil
		}
		start := len(cList) - limit
		if start < 0 {
			start = 0
		}
		return cList[start:]
	}

	scanRes := scanner.RunBulkMarketScannerPartitioned(
		s.cfg,
		activeStates,
		s.filters,
		vol24hMap,
		bookMap,
		fetchKlinesFn,
		simTime,
	)

	nextWatch := make(map[string]bool, len(scanRes.Primary))
	for _, sym := range scanRes.Primary {
		nextWatch[sym] = true
	}
	nextSweepExtra := make(map[string]bool, len(scanRes.SweepExtra))
	for _, sym := range scanRes.SweepExtra {
		nextSweepExtra[sym] = true
	}

	s.recordCycleStats(scanRes.Stats, nextWatch)
	s.lastWatchlist = nextWatch
	return nextWatch, nextSweepExtra
}

func (s *ScannerSimulator) recordCycleStats(st scanner.ScanStats, nextWatch map[string]bool) {
	s.Stats.CyclesRun++
	if st.FallbackUsed > 0 {
		s.Stats.FallbackCycles++
	}
	s.sumCandidatesValid += int64(st.CandidatesValid)
	s.sumSelected += int64(st.SelectedCount)
	s.Stats.TotalVolRejected += int64(st.VolRejected)
	s.Stats.TotalSpreadReject += int64(st.SpreadRejected)
	s.Stats.TotalNATRReject += int64(st.NATRRejected)
	s.Stats.TotalVWAPReject += int64(st.VWAPDistRejected)

	if s.Stats.CyclesRun > 0 {
		s.Stats.AvgCandidatesValid = float64(s.sumCandidatesValid) / float64(s.Stats.CyclesRun)
		s.Stats.AvgSelectedCoins = float64(s.sumSelected) / float64(s.Stats.CyclesRun)
	}

	if len(s.lastWatchlist) > 0 || len(nextWatch) > 0 {
		if s.Stats.CyclesRun > 1 {
			changed := 0
			union := make(map[string]struct{}, len(s.lastWatchlist)+len(nextWatch))
			for k := range s.lastWatchlist {
				union[k] = struct{}{}
				if !nextWatch[k] {
					changed++
				}
			}
			for k := range nextWatch {
				union[k] = struct{}{}
				if !s.lastWatchlist[k] {
					changed++
				}
			}
			if len(union) > 0 {
				s.sumTurnoverPct += (float64(changed) / float64(len(union))) * 100.0
				s.turnoverSamples++
				s.Stats.WatchlistTurnover = math.Round((s.sumTurnoverPct/float64(s.turnoverSamples))*100) / 100
			}
		}
	}
}
