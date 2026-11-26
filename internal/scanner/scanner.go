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
	"trml/internal/strategy"
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

	BenchmarkSymbol = "BTCUSDT"

	scannerSpreadSafetyRatio = 0.80 // Require 20% spread headroom during scanning vs execution cutoff.
	scannerSideEMAHeadroomATR = 1.0 // Allow symbols near EMA200 crossover within 1.0 ATR across 30m scan gaps.
	maxScannerKlineLimit     = 4500 // Support 15m/5m HTF EMA200 + 24h slope bootstrap via REST pagination.
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
	ExcludeTopVolRank            int
	SweepExtendedVWAP            bool
	SweepMaxExtraCoins           int
	BreakoutScannerEnabled       bool
	BreakoutMinNATRPct           float64
	BreakoutMaxNATRPct           float64
	BreakoutMinVolume24hUSDT     float64
	BreakoutMaxWatchedCoins      int
	BreakoutExcludeTopVolRank     int
	BreakoutMaxScannerVWAPDistATR float64
	RollingVWAPBars               int
	ComputeMarketBreadth          bool
	AllowedSide                   string
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
	allowedSide := strings.ToUpper(strings.TrimSpace(cfg.AllowedSide))
	if cfg.RelResidual.Enabled && strings.ToUpper(strings.TrimSpace(cfg.RelResidual.Mode)) != "MOMENTUM" {
		allowedSide = ""
	}
	minVol := cfg.MinVolume24hUSDT
	if minVol <= 0 {
		minVol = defaultScannerMinVolume24h
	}
	maxSpread := cfg.MaxSpreadPct
	if maxSpread <= 0 {
		maxSpread = defaultScannerMaxSpreadPct
	}
	minFallbackRVOL := cfg.MinFallbackRVOL
	if minFallbackRVOL <= 0 {
		minFallbackRVOL = defaultScannerFallbackRVOL
	}
	minNATR := cfg.MinNATRPct
	if minNATR <= 0 {
		minNATR = defaultScannerMinNATRPct
	}
	maxNATR := cfg.MaxNATRPct
	if maxNATR <= 0 {
		maxNATR = defaultScannerMaxNATRPct
	}
	if maxNATR < minNATR {
		maxNATR = minNATR
	}
	maxWatched := cfg.MaxWatchedCoins
	if maxWatched <= 0 {
		maxWatched = defaultScannerMaxWatchedCoins
	}
	maxVWAPDist := cfg.MaxScannerVWAPDistATR
	if maxVWAPDist < 0 {
		maxVWAPDist = defaultScannerVWAPDistATR
	}

	return ScannerConfig{
		MinVolume24hUSDT:      minVol,
		MaxSpreadPct:          maxSpread,
		MinRVOL:               minRVOL,
		MinFallbackRVOL:       minFallbackRVOL,
		RVOLLookback:          defaultScannerRVOLLookback,
		MinNATRPct:            minNATR,
		MaxNATRPct:            maxNATR,
		MaxScannerVWAPDistATR: maxVWAPDist,
		ATRPeriod:             defaultScannerATRPeriod,
		EMAPeriod:             emaPeriod,
		MaxWatchedCoins:       maxWatched,
		BootstrapCount:        bootstrapCount,
		DeltaWindow:           defaultScannerDeltaWindow,
		Timeframe:             timeframe,
		CandleDuration:        candleDuration,
		RankingMode:           rankingMode,
		ExcludeTopVolRank:            cfg.ScannerExcludeTopVolRank,
		SweepExtendedVWAP:            cfg.Sweep.Enabled && cfg.Sweep.ScannerAllowExtendedVWAP,
		SweepMaxExtraCoins:           cfg.Sweep.ScannerMaxExtraCoins,
		BreakoutScannerEnabled:       cfg.VolBreakout.ScannerEnabled,
		BreakoutMinNATRPct:           cfg.VolBreakout.MinNATRPct,
		BreakoutMaxNATRPct:           cfg.VolBreakout.MaxNATRPct,
		BreakoutMinVolume24hUSDT:     cfg.VolBreakout.MinVolume24hUSDT,
		BreakoutMaxWatchedCoins:      cfg.VolBreakout.MaxWatchedCoins,
		BreakoutExcludeTopVolRank:     cfg.VolBreakout.ExcludeTopVolRank,
		BreakoutMaxScannerVWAPDistATR: cfg.VolBreakout.MaxScannerVWAPDistATR,
		RollingVWAPBars:               cfg.RollingVWAPBars,
		ComputeMarketBreadth:          cfg.MarketBreadthLowThreshold > 0,
		AllowedSide:                   allowedSide,
	}
}

// ScanStats summarizes per-stage filtering outcomes for a bulk market scan cycle.
type ScanStats struct {
	TotalPerpUSDT     int
	VolRejected       int
	BookMissing       int
	SpreadRejected    int
	PreFilterPassed   int
	KlineFetchFailed  int
	KlineFetchOrShort int
	KlineGapOrInvalid int
	ATRInvalid        int
	NATRRejected      int
	SideRejected      int
	VWAPDistRejected  int
	CandidatesValid   int
	PrimaryPassed     int
	FallbackUsed      int
	SelectedCount     int
	BreakoutCount     int
	SweepExtraCount   int
}

// PartitionedScanResult separates primary VWAP pullback symbols from breakout and extended-VWAP liquidity-sweep extras.
type PartitionedScanResult struct {
	Primary            []string
	Breakout           []string
	SweepExtra         []string
	Stats              ScanStats
	MarketBreadthPct   float64
	MarketBreadthValid bool
	MarketBreadthTotal int
}

type scanRejectKind uint8

const (
	scanValid scanRejectKind = iota
	scanRejectKlineFetch
	scanRejectKlineShort
	scanRejectKlineInvalid
	scanRejectATR
	scanRejectNATR
	scanRejectSide
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

func buildTopVolExclusion(perpetualFilters map[string]model.ExchangeFilter, vol24hMap map[string]float64, limit int) map[string]bool {
	if limit <= 0 {
		return nil
	}
	type volRankEntry struct {
		sym string
		vol float64
	}
	ranked := make([]volRankEntry, 0, len(perpetualFilters))
	for sym := range perpetualFilters {
		if !strings.HasSuffix(sym, "USDT") || sym == BenchmarkSymbol {
			continue
		}
		v := vol24hMap[sym]
		if v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) {
			ranked = append(ranked, volRankEntry{sym: sym, vol: v})
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].vol != ranked[j].vol {
			return ranked[i].vol > ranked[j].vol
		}
		return ranked[i].sym < ranked[j].sym
	})
	if limit > len(ranked) {
		limit = len(ranked)
	}
	excluded := make(map[string]bool, limit)
	for i := 0; i < limit; i++ {
		excluded[ranked[i].sym] = true
	}
	return excluded
}

// RunBulkMarketScannerPartitioned runs a single-pass bulk scan and returns both the primary watchlist
// (unchanged L1 VWAP Pullback criteria) and optional breakout / extended-VWAP downtrend candidates.
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
	var breakoutCandidates []Candidate
	var sweepCandidates []Candidate
	rankMode := strings.ToLower(strings.TrimSpace(cfg.RankingMode))
	sideFilter := strings.ToUpper(strings.TrimSpace(cfg.AllowedSide))

	excludedTopVol := buildTopVolExclusion(perpetualFilters, vol24hMap, cfg.ExcludeTopVolRank)
	boEnabled := cfg.BreakoutScannerEnabled
	boMinVol := cfg.BreakoutMinVolume24hUSDT
	if boMinVol <= 0 {
		boMinVol = cfg.MinVolume24hUSDT
	}
	boMinNATR := cfg.BreakoutMinNATRPct
	if boMinNATR <= 0 {
		boMinNATR = cfg.MinNATRPct
	}
	boMaxNATR := cfg.BreakoutMaxNATRPct
	if boMaxNATR <= 0 {
		boMaxNATR = cfg.MaxNATRPct
	}
	boMaxCoins := cfg.BreakoutMaxWatchedCoins
	if boMaxCoins <= 0 {
		boMaxCoins = cfg.MaxWatchedCoins
	}
	boExclRank := cfg.BreakoutExcludeTopVolRank
	if boExclRank < 0 {
		boExclRank = cfg.ExcludeTopVolRank
	}
	var excludedBreakoutTopVol map[string]bool
	if boEnabled {
		excludedBreakoutTopVol = buildTopVolExclusion(perpetualFilters, vol24hMap, boExclRank)
	}

	type preScreenFlags struct {
		allowPrimary   bool
		allowBreakout  bool
		includeBreadth bool
	}
	preFlags := make(map[string]preScreenFlags)
	var symbolsToScan []string
	for sym := range perpetualFilters {
		if !strings.HasSuffix(sym, "USDT") || sym == BenchmarkSymbol {
			continue
		}
		stats.TotalPerpUSDT++
		vol := vol24hMap[sym]
		volFinite := !math.IsNaN(vol) && !math.IsInf(vol, 0)
		includeBreadth := cfg.ComputeMarketBreadth && sym != "BTCUSDT" && volFinite && vol >= cfg.MinVolume24hUSDT
		pOK := !excludedTopVol[sym] && volFinite && vol >= cfg.MinVolume24hUSDT
		bOK := boEnabled && !excludedBreakoutTopVol[sym] && volFinite && vol >= boMinVol
		if !pOK && !bOK {
			stats.VolRejected++
			if includeBreadth {
				preFlags[sym] = preScreenFlags{includeBreadth: true}
				symbolsToScan = append(symbolsToScan, sym)
			}
			continue
		}
		bk, ok := bookMap[sym]
		if !ok {
			stats.BookMissing++
			if includeBreadth {
				preFlags[sym] = preScreenFlags{includeBreadth: true}
				symbolsToScan = append(symbolsToScan, sym)
			}
			continue
		}
		spreadOK := !math.IsNaN(bk.BestBid) && !math.IsNaN(bk.BestAsk) && !math.IsInf(bk.BestBid, 0) && !math.IsInf(bk.BestAsk, 0) && bk.BestBid > 0 && bk.BestAsk > bk.BestBid
		if spreadOK {
			spreadPct := ((bk.BestAsk - bk.BestBid) / bk.BestBid) * 100.0
			maxScanSpread := cfg.MaxSpreadPct * scannerSpreadSafetyRatio
			if maxScanSpread <= 0 {
				maxScanSpread = cfg.MaxSpreadPct
			}
			if math.IsNaN(spreadPct) || math.IsInf(spreadPct, 0) || spreadPct > maxScanSpread {
				spreadOK = false
			}
		}
		if !spreadOK {
			stats.SpreadRejected++
			if includeBreadth {
				preFlags[sym] = preScreenFlags{includeBreadth: true}
				symbolsToScan = append(symbolsToScan, sym)
			}
			continue
		}
		stats.PreFilterPassed++
		preFlags[sym] = preScreenFlags{allowPrimary: pOK, allowBreakout: bOK, includeBreadth: includeBreadth}
		symbolsToScan = append(symbolsToScan, sym)
	}
	sort.Strings(symbolsToScan)

	klineLimit := cfg.BootstrapCount + 1
	if klineLimit > maxScannerKlineLimit {
		klineLimit = maxScannerKlineLimit
	}
	if klineLimit < cfg.EMAPeriod+50 {
		klineLimit = cfg.EMAPeriod + 50
	}

	type scanResult struct {
		cand           *Candidate
		boCand         *Candidate
		sweepCand      *Candidate
		reject         scanRejectKind
		breadthOnly    bool
		breadthCounted bool
		breadthAbove   bool
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
		pf := preFlags[sym]
		go func(symbol string, flags preScreenFlags) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			var rawKlines []model.Candle
			if fetchKlinesFn != nil {
				rawKlines = fetchKlinesFn(symbol, klineLimit)
			}
			if rawKlines == nil {
				if !flags.allowPrimary && !flags.allowBreakout {
					resultsChan <- scanResult{breadthOnly: true}
					return
				}
				resultsChan <- scanResult{reject: scanRejectKlineFetch}
				return
			}
			if len(rawKlines) < cfg.EMAPeriod+1 {
				if !flags.allowPrimary && !flags.allowBreakout {
					resultsChan <- scanResult{breadthOnly: true}
					return
				}
				resultsChan <- scanResult{reject: scanRejectKlineShort}
				return
			}
			validKlines, err := model.ValidateAndFilterCandles(rawKlines, serverTime, cfg.CandleDuration)
			if err != nil {
				if !flags.allowPrimary && !flags.allowBreakout {
					resultsChan <- scanResult{breadthOnly: true}
					return
				}
				resultsChan <- scanResult{reject: scanRejectKlineInvalid}
				return
			}
			var breadthCounted, breadthAbove bool
			if flags.includeBreadth {
				breadthAbove, breadthCounted = strategy.IsAboveHourlyEMA200(validKlines)
			}
			if !flags.allowPrimary && !flags.allowBreakout {
				resultsChan <- scanResult{
					breadthOnly:    true,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}
			if len(validKlines) < cfg.EMAPeriod+1 {
				resultsChan <- scanResult{
					reject:         scanRejectKlineShort,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}

			atr14 := indicator.CalculateATR(validKlines, cfg.ATRPeriod)
			if math.IsNaN(atr14) || math.IsInf(atr14, 0) || atr14 <= 0 {
				resultsChan <- scanResult{
					reject:         scanRejectATR,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}
			lastClose := validKlines[len(validKlines)-1].Close
			if math.IsNaN(lastClose) || math.IsInf(lastClose, 0) || lastClose <= 0 {
				resultsChan <- scanResult{
					reject:         scanRejectKlineInvalid,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}
			natrPct := (atr14 / lastClose) * 100.0
			if math.IsNaN(natrPct) || math.IsInf(natrPct, 0) {
				resultsChan <- scanResult{
					reject:         scanRejectNATR,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}

			pNATROK := flags.allowPrimary && natrPct >= cfg.MinNATRPct && natrPct <= cfg.MaxNATRPct
			bNATROK := flags.allowBreakout && natrPct >= boMinNATR && natrPct <= boMaxNATR
			if !pNATROK && !bNATROK {
				resultsChan <- scanResult{
					reject:         scanRejectNATR,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}

			var ema200 float64
			var hasEMA200 bool
			if sideFilter == "SHORT_ONLY" || sideFilter == "LONG_ONLY" {
				ema200 = indicator.CalculateEMA(validKlines, cfg.EMAPeriod)
				hasEMA200 = !math.IsNaN(ema200) && !math.IsInf(ema200, 0) && ema200 > 0
				sideHeadroom := scannerSideEMAHeadroomATR * atr14
				if hasEMA200 && ((sideFilter == "SHORT_ONLY" && lastClose > ema200+sideHeadroom) || (sideFilter == "LONG_ONLY" && lastClose < ema200-sideHeadroom)) {
					resultsChan <- scanResult{
						reject:         scanRejectSide,
						breadthCounted: breadthCounted,
						breadthAbove:   breadthAbove,
					}
					return
				}
			}

			var distVWAPATR float64
			var sessionVWAP float64
			needVWAP := ((cfg.MaxScannerVWAPDistATR > 0 || rankMode == "pullback") && pNATROK) || (cfg.BreakoutMaxScannerVWAPDistATR > 0 && bNATROK)
			if needVWAP {
				sessionVWAP = indicator.CalculateEffectiveVWAP(validKlines, cfg.RollingVWAPBars)
				if !math.IsNaN(sessionVWAP) && !math.IsInf(sessionVWAP, 0) && sessionVWAP > 0 {
					distVWAPATR = math.Abs(lastClose-sessionVWAP) / atr14
				} else {
					sessionVWAP = 0
				}
			}

			var sweepExtra *Candidate
			pVWAPOK := pNATROK
			if pVWAPOK && (cfg.MaxScannerVWAPDistATR > 0 || rankMode == "pullback") {
				if sessionVWAP <= 0 {
					pVWAPOK = false
				} else if cfg.MaxScannerVWAPDistATR > 0 && distVWAPATR > cfg.MaxScannerVWAPDistATR {
					pVWAPOK = false
					if cfg.SweepExtendedVWAP && lastClose <= sessionVWAP {
						if !hasEMA200 {
							ema200 = indicator.CalculateEMA(validKlines, cfg.EMAPeriod)
							hasEMA200 = !math.IsNaN(ema200) && !math.IsInf(ema200, 0) && ema200 > 0
						}
						if hasEMA200 && lastClose < ema200 {
							rvolExt := indicator.CalculateRVOL(validKlines, cfg.RVOLLookback)
							if !math.IsNaN(rvolExt) && !math.IsInf(rvolExt, 0) {
								liqB := computeLiquidityLogBonus(vol24hMap[symbol], cfg.MinVolume24hUSDT)
								sweepExtra = &Candidate{
									Symbol:  symbol,
									RVOL:    rvolExt,
									Score:   0.50*liqB + 0.25*math.Min(rvolExt, 2.0),
									Candles: validKlines,
								}
							}
						}
					}
				}
			}

			bVWAPOK := bNATROK
			if bVWAPOK && cfg.BreakoutMaxScannerVWAPDistATR > 0 {
				if sessionVWAP <= 0 || distVWAPATR > cfg.BreakoutMaxScannerVWAPDistATR {
					bVWAPOK = false
				}
			}

			if !pVWAPOK && !bVWAPOK {
				resultsChan <- scanResult{
					reject:         scanRejectVWAPDist,
					sweepCand:      sweepExtra,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}

			rvol := indicator.CalculateRVOL(validKlines, cfg.RVOLLookback)
			if math.IsNaN(rvol) || math.IsInf(rvol, 0) {
				resultsChan <- scanResult{
					reject:         scanRejectATR,
					breadthCounted: breadthCounted,
					breadthAbove:   breadthAbove,
				}
				return
			}

			var pCand *Candidate
			if pVWAPOK {
				score := rvol
				if rankMode == "pullback" {
					score = computePullbackScore(cfg, rvol, distVWAPATR, natrPct, vol24hMap[symbol])
				}
				pCand = &Candidate{
					Symbol:    symbol,
					RVOL:      rvol,
					Score:     score,
					Candles:   validKlines,
					IsPrimary: rvol >= cfg.MinRVOL,
				}
			}

			var bCand *Candidate
			if bVWAPOK {
				atr60 := indicator.CalculateATR(validKlines, 60)
				compRatio := 1.0
				if !math.IsNaN(atr60) && !math.IsInf(atr60, 0) && atr60 > 0 {
					compRatio = atr14 / atr60
				}
				squeezeScore := math.Max(0.0, 1.20-compRatio)
				liqBonus := computeLiquidityLogBonus(vol24hMap[symbol], boMinVol)
				bCand = &Candidate{
					Symbol:  symbol,
					RVOL:    rvol,
					Score:   0.65*squeezeScore + 0.35*liqBonus,
					Candles: validKlines,
				}
			}

			rej := scanValid
			if pCand == nil {
				// Record primary-bucket rejection telemetry when only breakout matched
				if flags.allowPrimary && !pNATROK {
					rej = scanRejectNATR
				} else if flags.allowPrimary && !pVWAPOK {
					rej = scanRejectVWAPDist
				}
			}
			resultsChan <- scanResult{
				cand:           pCand,
				boCand:         bCand,
				sweepCand:      sweepExtra,
				reject:         rej,
				breadthCounted: breadthCounted,
				breadthAbove:   breadthAbove,
			}
		}(sym, pf)
	}

	wg.Wait()
	close(resultsChan)

	fallbackFloor := cfg.MinFallbackRVOL
	if fallbackFloor <= 0 {
		fallbackFloor = defaultScannerFallbackRVOL
	}

	breadthTotal := 0
	breadthAboveCount := 0
	for res := range resultsChan {
		if res.breadthCounted {
			breadthTotal++
			if res.breadthAbove {
				breadthAboveCount++
			}
		}
		if res.breadthOnly {
			continue
		}
		if res.boCand != nil {
			breakoutCandidates = append(breakoutCandidates, *res.boCand)
		}
		if res.cand != nil {
			stats.CandidatesValid++
			candidates = append(candidates, *res.cand)
			if res.cand.IsPrimary {
				stats.PrimaryPassed++
				passed = append(passed, *res.cand)
			}
			continue
		}
		switch res.reject {
		case scanRejectKlineFetch:
			stats.KlineFetchFailed++
		case scanRejectKlineShort:
			stats.KlineFetchOrShort++
		case scanRejectKlineInvalid:
			stats.KlineGapOrInvalid++
		case scanRejectATR:
			stats.ATRInvalid++
		case scanRejectNATR:
			stats.NATRRejected++
		case scanRejectSide:
			stats.SideRejected++
		case scanRejectVWAPDist:
			stats.VWAPDistRejected++
			if res.sweepCand != nil {
				sweepCandidates = append(sweepCandidates, *res.sweepCand)
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
		} else {
			st.Mu.Lock()
			if updatedFilter, hasFilter := perpetualFilters[cand.Symbol]; hasFilter {
				st.Filter = updatedFilter
			}
			if cfg.CandleDuration > 0 {
				st.TargetOIInterval = cfg.CandleDuration
			}
			st.Mu.Unlock()
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

	var boSelected []string
	if boEnabled && boMaxCoins > 0 && len(breakoutCandidates) > 0 {
		sortByMode(breakoutCandidates, "pullback")
		for i := 0; i < len(breakoutCandidates) && len(boSelected) < boMaxCoins; i++ {
			cand := breakoutCandidates[i]
			if cand.RVOL < fallbackFloor {
				continue
			}
			if !selectedSet[cand.Symbol] && !bootstrapCand(cand) {
				continue
			}
			boSelected = append(boSelected, cand.Symbol)
		}
	}
	stats.BreakoutCount = len(boSelected)

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

	var breadthPct float64
	var breadthValid bool
	if cfg.ComputeMarketBreadth {
		breadthPct, breadthValid = strategy.FinalizeMarketBreadth(breadthAboveCount, breadthTotal, strategy.DefaultMinBreadthSymbols)
	}

	return PartitionedScanResult{
		Primary:            selected,
		Breakout:           boSelected,
		SweepExtra:         sweepSelected,
		Stats:              stats,
		MarketBreadthPct:   breadthPct,
		MarketBreadthValid: breadthValid,
		MarketBreadthTotal: breadthTotal,
	}
}
