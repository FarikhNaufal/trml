package scanner

import (
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"trml/internal/config"
	"trml/internal/indicator"
	"trml/internal/model"
)

const (
	defaultScannerMinVolume24h    = 25_000_000.0
	defaultScannerMaxSpreadPct    = 0.05
	defaultScannerMinRVOL         = 1.80
	defaultScannerFallbackRVOL    = 0.50
	defaultScannerMinNATRPct      = 0.50
	defaultScannerMaxNATRPct      = 1.80
	defaultScannerVWAPDistATR     = 2.50
	defaultScannerATRPeriod       = 14
	defaultScannerEMAPeriod       = 200
	defaultScannerRVOLLookback    = 20
	defaultScannerMaxWatchedCoins = 20
	defaultScannerBootstrap       = 1000
	defaultScannerDeltaWindow     = 50
	defaultScannerRankingMode     = "pullback"

	scannerSpreadSafetyRatio = 0.80 // Require 20% spread headroom during scanning vs execution cutoff.
	maxScannerKlineLimit     = 600  // Cap REST kline weight per symbol during bulk scans.
	scannerWorkerConcurrency = 5    // Bound concurrent kline REST fetches to respect exchange rate limits.

	exhaustionRVOLRatio        = 2.50 / defaultScannerMinRVOL // Penalize late parabolic spikes (>= 2.50 at default 1.80 MinRVOL).
	exhaustionRVOLScorePenalty = 0.50
	natrRangePenaltySlope      = 0.50 // Prefer calmer volatility near MinNATRPct over upper bound.
	liquidityLogScaleFactor    = 2.00 // Log10 scale caps full bonus at 100x MinVolume24hUSDT.

	pullbackWeightVWAP      = 0.35
	pullbackWeightRVOL      = 0.30
	pullbackWeightNATR      = 0.20
	pullbackWeightLiquidity = 0.15
)

type ScannerConfig struct {
	MinVolume24hUSDT      float64
	MaxSpreadPct          float64
	MinRVOL               float64
	MinFallbackRVOL       float64
	RVOLLookback          int
	MinNATRPct            float64
	MaxNATRPct            float64
	MaxScannerVWAPDistATR float64
	ATRPeriod             int
	EMAPeriod             int
	MaxWatchedCoins       int
	BootstrapCount        int
	DeltaWindow           int
	Timeframe             string
	CandleDuration        time.Duration
	RankingMode           string
	SweepExtendedVWAP     bool
	SweepMaxExtraCoins    int
}

// FromAppConfig builds a ScannerConfig from application config while applying safe defaults.
func FromAppConfig(cfg *config.Config, bootstrapCount int) ScannerConfig {
	if bootstrapCount <= 0 {
		bootstrapCount = defaultScannerBootstrap
	}
	if cfg == nil {
		return ScannerConfig{
			Timeframe:             "5m",
			CandleDuration:        5 * time.Minute,
			MinVolume24hUSDT:      defaultScannerMinVolume24h,
			MaxSpreadPct:          defaultScannerMaxSpreadPct,
			MinRVOL:               defaultScannerMinRVOL,
			MinFallbackRVOL:       defaultScannerFallbackRVOL,
			RVOLLookback:          defaultScannerRVOLLookback,
			MinNATRPct:            defaultScannerMinNATRPct,
			MaxNATRPct:            defaultScannerMaxNATRPct,
			MaxScannerVWAPDistATR: defaultScannerVWAPDistATR,
			ATRPeriod:             defaultScannerATRPeriod,
			EMAPeriod:             defaultScannerEMAPeriod,
			MaxWatchedCoins:       defaultScannerMaxWatchedCoins,
			BootstrapCount:        bootstrapCount,
			DeltaWindow:           defaultScannerDeltaWindow,
			RankingMode:           defaultScannerRankingMode,
		}
	}

	minRVOL := cfg.MinRVOL
	if minRVOL <= 0 {
		minRVOL = defaultScannerMinRVOL
	}
	emaPeriod := cfg.EMAPeriod
	if emaPeriod <= 0 {
		emaPeriod = defaultScannerEMAPeriod
	}
	timeframe := strings.TrimSpace(cfg.Timeframe)
	if timeframe == "" {
		timeframe = "5m"
	}
	candleDuration := cfg.CandleDuration
	if candleDuration <= 0 {
		candleDuration = 5 * time.Minute
	}
	rankingMode := strings.ToLower(strings.TrimSpace(cfg.ScannerRankingMode))
	if rankingMode == "" {
		rankingMode = defaultScannerRankingMode
	}

	return ScannerConfig{
		MinVolume24hUSDT:      cfg.MinVolume24hUSDT,
		MaxSpreadPct:          cfg.MaxSpreadPct,
		MinRVOL:               minRVOL,
		MinFallbackRVOL:       cfg.MinFallbackRVOL,
		RVOLLookback:          defaultScannerRVOLLookback,
		MinNATRPct:            cfg.MinNATRPct,
		MaxNATRPct:            cfg.MaxNATRPct,
		MaxScannerVWAPDistATR: cfg.MaxScannerVWAPDistATR,
		ATRPeriod:             defaultScannerATRPeriod,
		EMAPeriod:             emaPeriod,
		MaxWatchedCoins:       cfg.MaxWatchedCoins,
		BootstrapCount:        bootstrapCount,
		DeltaWindow:           defaultScannerDeltaWindow,
		Timeframe:             timeframe,
		CandleDuration:        candleDuration,
		RankingMode:           rankingMode,
		SweepExtendedVWAP:     cfg.Sweep.Enabled && cfg.Sweep.ScannerAllowExtendedVWAP,
		SweepMaxExtraCoins:    cfg.Sweep.ScannerMaxExtraCoins,
	}
}

// ScanStats summarizes per-stage filtering outcomes for a bulk market scan cycle.
type ScanStats struct {
	TotalPerpUSDT     int
	VolRejected       int
	SpreadRejected    int
	PreFilterPassed   int
	KlineFetchOrShort int
	KlineGapOrInvalid int
	ATRInvalid        int
	NATRRejected      int
	VWAPDistRejected  int
	CandidatesValid   int
	PrimaryPassed     int
	FallbackUsed      int
	SelectedCount     int
	SweepExtraCount   int
}

// PartitionedScanResult separates primary VWAP pullback symbols from extended-VWAP liquidity-sweep extras.
type PartitionedScanResult struct {
	Primary    []string
	SweepExtra []string
	Stats      ScanStats
}

type scanRejectKind uint8

const (
	scanValid scanRejectKind = iota
	scanRejectKlineShort
	scanRejectKlineInvalid
	scanRejectATR
	scanRejectNATR
	scanRejectVWAPDist
)

func RunBulkMarketScanner(
	cfg ScannerConfig,
	activeStates map[string]*model.SymbolState,
	perpetualFilters map[string]model.ExchangeFilter,
	vol24hMap map[string]float64,
	bookMap map[string]model.BookQuote,
	fetchKlinesFn func(symbol string, limit int) []model.Candle,
	serverTime time.Time,
) []string {
	return RunBulkMarketScannerPartitioned(
		cfg,
		activeStates,
		perpetualFilters,
		vol24hMap,
		bookMap,
		fetchKlinesFn,
		serverTime,
	).Primary
}

func computeLiquidityLogBonus(vol24h, minVol float64) float64 {
	if vol24h > 0 && minVol > 0 && vol24h >= minVol {
		return math.Min(math.Log10(vol24h/minVol)/liquidityLogScaleFactor, 1.0)
	}
	return 0.0
}

func computePullbackScore(cfg ScannerConfig, rvol, distVWAPATR, natrPct, vol24h float64) float64 {
	maxDist := cfg.MaxScannerVWAPDistATR
	if maxDist <= 0 {
		maxDist = defaultScannerVWAPDistATR
	}
	vwapNorm := 1.0 - math.Min(distVWAPATR/maxDist, 1.0)
	if vwapNorm < 0 {
		vwapNorm = 0
	}

	rvolRef := cfg.MinRVOL
	if rvolRef <= 0 {
		rvolRef = defaultScannerMinRVOL
	}
	rvolScore := math.Min(rvol, rvolRef) / rvolRef
	if rvol >= rvolRef*exhaustionRVOLRatio {
		rvolScore *= exhaustionRVOLScorePenalty
	}

	natrQuality := 1.0
	natrSpan := cfg.MaxNATRPct - cfg.MinNATRPct
	if natrSpan > 0 {
		ratio := (natrPct - cfg.MinNATRPct) / natrSpan
		if ratio < 0 {
			ratio = 0
		} else if ratio > 1 {
			ratio = 1
		}
		natrQuality = 1.0 - natrRangePenaltySlope*ratio
	}

	liqBonus := computeLiquidityLogBonus(vol24h, cfg.MinVolume24hUSDT)

	return pullbackWeightVWAP*vwapNorm +
		pullbackWeightRVOL*rvolScore +
		pullbackWeightNATR*natrQuality +
		pullbackWeightLiquidity*liqBonus
}

// RunBulkMarketScannerWithStats runs the market scanner and returns primary selected symbols and per-stage diagnostics.
func RunBulkMarketScannerWithStats(
	cfg ScannerConfig,
	activeStates map[string]*model.SymbolState,
	perpetualFilters map[string]model.ExchangeFilter,
	vol24hMap map[string]float64,
	bookMap map[string]model.BookQuote,
	fetchKlinesFn func(symbol string, limit int) []model.Candle,
	serverTime time.Time,
) ([]string, ScanStats) {
	res := RunBulkMarketScannerPartitioned(
		cfg,
		activeStates,
		perpetualFilters,
		vol24hMap,
		bookMap,
		fetchKlinesFn,
		serverTime,
	)
	return res.Primary, res.Stats
}

// RunBulkMarketScannerPartitioned runs a single-pass bulk scan and returns both the primary watchlist
// (unchanged L1 VWAP Pullback criteria) and optional extended-VWAP downtrend candidates for liquidity sweep.
func RunBulkMarketScannerPartitioned(
	cfg ScannerConfig,
	activeStates map[string]*model.SymbolState,
	perpetualFilters map[string]model.ExchangeFilter,
	vol24hMap map[string]float64,
	bookMap map[string]model.BookQuote,
	fetchKlinesFn func(symbol string, limit int) []model.Candle,
	serverTime time.Time,
) PartitionedScanResult {
	type Candidate struct {
		Symbol    string
		RVOL      float64
		Score     float64
		Candles   []model.Candle
		IsPrimary bool
	}
	var stats ScanStats
	var passed []Candidate
	var candidates []Candidate
	var sweepCandidates []Candidate
	rankMode := strings.ToLower(strings.TrimSpace(cfg.RankingMode))

	var symbolsToScan []string
	for sym := range perpetualFilters {
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		stats.TotalPerpUSDT++
		vol := vol24hMap[sym]
		if math.IsNaN(vol) || math.IsInf(vol, 0) || vol < cfg.MinVolume24hUSDT {
			stats.VolRejected++
			continue
		}
		bk, ok := bookMap[sym]
		if !ok || math.IsNaN(bk.BestBid) || math.IsNaN(bk.BestAsk) || math.IsInf(bk.BestBid, 0) || math.IsInf(bk.BestAsk, 0) || bk.BestBid <= 0 || bk.BestAsk <= bk.BestBid {
			stats.SpreadRejected++
			continue
		}
		spreadPct := ((bk.BestAsk - bk.BestBid) / bk.BestBid) * 100.0
		if math.IsNaN(spreadPct) || math.IsInf(spreadPct, 0) {
			stats.SpreadRejected++
			continue
		}
		maxScanSpread := cfg.MaxSpreadPct * scannerSpreadSafetyRatio
		if maxScanSpread <= 0 {
			maxScanSpread = cfg.MaxSpreadPct
		}
		if spreadPct > maxScanSpread {
			stats.SpreadRejected++
			continue
		}
		symbolsToScan = append(symbolsToScan, sym)
	}
	sort.Strings(symbolsToScan)
	stats.PreFilterPassed = len(symbolsToScan)

	klineLimit := cfg.BootstrapCount + 1
	if klineLimit > maxScannerKlineLimit {
		klineLimit = maxScannerKlineLimit
	}
	if klineLimit < cfg.EMAPeriod+50 {
		klineLimit = cfg.EMAPeriod + 50
	}

	type scanResult struct {
		cand      Candidate
		sweepCand *Candidate
		reject    scanRejectKind
	}

	resultsChan := make(chan scanResult, len(symbolsToScan))
	workerCount := scannerWorkerConcurrency
	if len(symbolsToScan) < workerCount {
		workerCount = len(symbolsToScan)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	sem := make(chan struct{}, workerCount)
	var wg sync.WaitGroup

	for _, sym := range symbolsToScan {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			var rawKlines []model.Candle
			if fetchKlinesFn != nil {
				rawKlines = fetchKlinesFn(symbol, klineLimit)
			}
			if len(rawKlines) < cfg.EMAPeriod+1 {
				resultsChan <- scanResult{reject: scanRejectKlineShort}
				return
			}
			validKlines, err := model.ValidateAndFilterCandles(rawKlines, serverTime, cfg.CandleDuration)
			if err != nil {
				resultsChan <- scanResult{reject: scanRejectKlineInvalid}
				return
			}
			if len(validKlines) < cfg.EMAPeriod+1 {
				resultsChan <- scanResult{reject: scanRejectKlineShort}
				return
			}

			atr14 := indicator.CalculateATR(validKlines, cfg.ATRPeriod)
			if math.IsNaN(atr14) || math.IsInf(atr14, 0) || atr14 <= 0 {
				resultsChan <- scanResult{reject: scanRejectATR}
				return
			}
			lastClose := validKlines[len(validKlines)-1].Close
			if math.IsNaN(lastClose) || math.IsInf(lastClose, 0) || lastClose <= 0 {
				resultsChan <- scanResult{reject: scanRejectKlineInvalid}
				return
			}
			natrPct := (atr14 / lastClose) * 100.0
			if math.IsNaN(natrPct) || math.IsInf(natrPct, 0) || natrPct < cfg.MinNATRPct || natrPct > cfg.MaxNATRPct {
				resultsChan <- scanResult{reject: scanRejectNATR}
				return
			}

			var distVWAPATR float64
			needVWAP := (cfg.MaxScannerVWAPDistATR > 0 || rankMode == "pullback") && atr14 > 0
			if needVWAP {
				sessionVWAP := indicator.CalculateSessionVWAP(validKlines)
				if math.IsNaN(sessionVWAP) || math.IsInf(sessionVWAP, 0) || sessionVWAP <= 0 {
					resultsChan <- scanResult{reject: scanRejectVWAPDist}
					return
				}
				distVWAPATR = math.Abs(lastClose-sessionVWAP) / atr14
				if cfg.MaxScannerVWAPDistATR > 0 && distVWAPATR > cfg.MaxScannerVWAPDistATR {
					var extra *Candidate
					if cfg.SweepExtendedVWAP && lastClose <= sessionVWAP {
						ema200 := indicator.CalculateEMA(validKlines, cfg.EMAPeriod)
						if !math.IsNaN(ema200) && !math.IsInf(ema200, 0) && ema200 > 0 && lastClose < ema200 {
							rvolExt := indicator.CalculateRVOL(validKlines, cfg.RVOLLookback)
							if !math.IsNaN(rvolExt) && !math.IsInf(rvolExt, 0) {
								liqB := computeLiquidityLogBonus(vol24hMap[symbol], cfg.MinVolume24hUSDT)
								extra = &Candidate{
									Symbol:  symbol,
									RVOL:    rvolExt,
									Score:   0.50*liqB + 0.25*math.Min(rvolExt, 2.0),
									Candles: validKlines,
								}
							}
						}
					}
					resultsChan <- scanResult{reject: scanRejectVWAPDist, sweepCand: extra}
					return
				}
			}

			rvol := indicator.CalculateRVOL(validKlines, cfg.RVOLLookback)
			if math.IsNaN(rvol) || math.IsInf(rvol, 0) {
				resultsChan <- scanResult{reject: scanRejectATR}
				return
			}
			score := rvol
			if rankMode == "pullback" {
				score = computePullbackScore(cfg, rvol, distVWAPATR, natrPct, vol24hMap[symbol])
			}
			cand := Candidate{
				Symbol:    symbol,
				RVOL:      rvol,
				Score:     score,
				Candles:   validKlines,
				IsPrimary: rvol >= cfg.MinRVOL,
			}
			resultsChan <- scanResult{cand: cand, reject: scanValid}
		}(sym)
	}

	wg.Wait()
	close(resultsChan)

	fallbackFloor := cfg.MinFallbackRVOL
	if fallbackFloor <= 0 {
		fallbackFloor = defaultScannerFallbackRVOL
	}

	for res := range resultsChan {
		switch res.reject {
		case scanRejectKlineShort:
			stats.KlineFetchOrShort++
		case scanRejectKlineInvalid:
			stats.KlineGapOrInvalid++
		case scanRejectATR:
			stats.ATRInvalid++
		case scanRejectNATR:
			stats.NATRRejected++
		case scanRejectVWAPDist:
			stats.VWAPDistRejected++
			if res.sweepCand != nil {
				sweepCandidates = append(sweepCandidates, *res.sweepCand)
			}
		case scanValid:
			stats.CandidatesValid++
			candidates = append(candidates, res.cand)
			if res.cand.IsPrimary {
				stats.PrimaryPassed++
				passed = append(passed, res.cand)
			}
		}
	}

	sortByMode := func(list []Candidate, mode string) {
		sort.SliceStable(list, func(i, j int) bool {
			if mode == "pullback" {
				if list[i].Score != list[j].Score {
					return list[i].Score > list[j].Score
				}
				return list[i].Symbol < list[j].Symbol
			}
			if list[i].RVOL == list[j].RVOL {
				return list[i].Symbol < list[j].Symbol
			}
			return list[i].RVOL > list[j].RVOL
		})
	}

	if rankMode == "pullback" {
		passed = passed[:0]
		for _, cand := range candidates {
			if cand.RVOL >= fallbackFloor {
				passed = append(passed, cand)
			}
		}
		sortByMode(passed, rankMode)
	} else {
		// Jika jumlah koin lolos (RVOL >= MinRVOL) belum mencapai MaxWatchedCoins,
		// lengkapi sisa slot dari kandidat lain yang memenuhi MinFallbackRVOL
		if len(passed) < cfg.MaxWatchedCoins && len(candidates) > 0 {
			alreadyPassed := make(map[string]bool, len(passed))
			for _, p := range passed {
				alreadyPassed[p.Symbol] = true
			}
			sortByMode(candidates, rankMode)
			for _, cand := range candidates {
				if len(passed) >= cfg.MaxWatchedCoins {
					break
				}
				if !alreadyPassed[cand.Symbol] && cand.RVOL >= fallbackFloor {
					passed = append(passed, cand)
					alreadyPassed[cand.Symbol] = true
				}
			}
		}
		sortByMode(passed, rankMode)
	}

	bootstrapCand := func(cand Candidate) bool {
		st, exists := activeStates[cand.Symbol]
		if !exists {
			st = model.NewSymbolState(cand.Symbol, perpetualFilters[cand.Symbol], cfg.BootstrapCount, cfg.DeltaWindow+1)
			st.TargetOIInterval = cfg.CandleDuration
		}
		if err := BootstrapSymbolState(st, cand.Candles, serverTime, cfg.BootstrapCount, cfg.DeltaWindow+1, cfg.CandleDuration); err != nil {
			slog.Warn("scanner: failed to bootstrap symbol state, skipping symbol", "symbol", cand.Symbol, "err", err)
			return false
		}
		if !exists {
			activeStates[cand.Symbol] = st
		}
		return true
	}

	var selected []string
	selectedSet := make(map[string]bool, cfg.MaxWatchedCoins)
	for i := 0; i < len(passed) && len(selected) < cfg.MaxWatchedCoins; i++ {
		cand := passed[i]
		if !bootstrapCand(cand) {
			continue
		}
		if !cand.IsPrimary {
			stats.FallbackUsed++
		}
		selected = append(selected, cand.Symbol)
		selectedSet[cand.Symbol] = true
	}
	stats.SelectedCount = len(selected)

	var sweepSelected []string
	if cfg.SweepExtendedVWAP && cfg.SweepMaxExtraCoins > 0 && len(sweepCandidates) > 0 {
		var eligible []Candidate
		for _, cand := range sweepCandidates {
			if !selectedSet[cand.Symbol] && cand.RVOL >= fallbackFloor {
				eligible = append(eligible, cand)
			}
		}
		sortByMode(eligible, "pullback")
		for i := 0; i < len(eligible); i++ {
			if cfg.SweepMaxExtraCoins > 0 && len(sweepSelected) >= cfg.SweepMaxExtraCoins {
				break
			}
			if bootstrapCand(eligible[i]) {
				sweepSelected = append(sweepSelected, eligible[i].Symbol)
			}
		}
	}
	stats.SweepExtraCount = len(sweepSelected)

	return PartitionedScanResult{
		Primary:    selected,
		SweepExtra: sweepSelected,
		Stats:      stats,
	}
}
