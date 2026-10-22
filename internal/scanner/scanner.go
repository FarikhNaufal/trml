package scanner

import (
	"log/slog"
	"sort"
	"strings"
	"time"

	"trml/internal/indicator"
	"trml/internal/model"
)

type ScannerConfig struct {
	MinVolume24hUSDT float64
	MaxSpreadPct     float64
	MinRVOL          float64
	MinFallbackRVOL  float64
	RVOLLookback     int
	MinNATRPct       float64
	MaxNATRPct       float64
	ATRPeriod        int
	EMAPeriod        int
	MaxWatchedCoins  int
	BootstrapCount   int
	DeltaWindow      int
}

func RunBulkMarketScanner(
	cfg ScannerConfig,
	activeStates map[string]*model.SymbolState,
	perpetualFilters map[string]model.ExchangeFilter,
	vol24hMap map[string]float64,
	bookMap map[string]model.BookQuote,
	fetchKlinesFn func(symbol string, limit int) []model.Candle,
	serverTime time.Time,
) []string {
	type Candidate struct {
		Symbol  string
		RVOL    float64
		Candles []model.Candle
	}
	var passed []Candidate
	var candidates []Candidate

	for sym := range perpetualFilters {
		if !strings.HasSuffix(sym, "USDT") {
			continue
		}
		if vol24hMap[sym] < cfg.MinVolume24hUSDT {
			continue
		}
		bk, ok := bookMap[sym]
		if !ok || bk.BestBid <= 0 || bk.BestAsk <= bk.BestBid {
			continue
		}
		spreadPct := ((bk.BestAsk - bk.BestBid) / bk.BestBid) * 100.0
		maxScanSpread := cfg.MaxSpreadPct * 0.8
		if maxScanSpread <= 0 {
			maxScanSpread = cfg.MaxSpreadPct
		}
		if spreadPct > maxScanSpread {
			continue
		}

		rawKlines := fetchKlinesFn(sym, cfg.BootstrapCount+1)
		validKlines, err := model.ValidateAndFilterCandles(rawKlines, serverTime)
		if err != nil || len(validKlines) < cfg.EMAPeriod+1 {
			continue
		}

		atr14 := indicator.CalculateATR(validKlines, cfg.ATRPeriod)
		lastClose := validKlines[len(validKlines)-1].Close
		if lastClose <= 0 {
			continue
		}
		natrPct := (atr14 / lastClose) * 100.0
		if natrPct < cfg.MinNATRPct || natrPct > cfg.MaxNATRPct {
			continue
		}

		rvol := indicator.CalculateRVOL(validKlines, cfg.RVOLLookback)
		cand := Candidate{Symbol: sym, RVOL: rvol, Candles: validKlines}
		candidates = append(candidates, cand)
		if rvol >= cfg.MinRVOL {
			passed = append(passed, cand)
		}
	}

	// Jika jumlah koin lolos (RVOL >= MinRVOL) belum mencapai MaxWatchedCoins,
	// lengkapi sisa slot dari kandidat lain yang memenuhi MinFallbackRVOL (default 1.20)
	if len(passed) < cfg.MaxWatchedCoins && len(candidates) > 0 {
		fallbackFloor := cfg.MinFallbackRVOL
		if fallbackFloor <= 0 {
			fallbackFloor = 0.50
		}
		alreadyPassed := make(map[string]bool, len(passed))
		for _, p := range passed {
			alreadyPassed[p.Symbol] = true
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].RVOL == candidates[j].RVOL {
				return candidates[i].Symbol < candidates[j].Symbol
			}
			return candidates[i].RVOL > candidates[j].RVOL
		})
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

	sort.SliceStable(passed, func(i, j int) bool {
		if passed[i].RVOL == passed[j].RVOL {
			return passed[i].Symbol < passed[j].Symbol
		}
		return passed[i].RVOL > passed[j].RVOL
	})

	var selected []string
	for i := 0; i < len(passed) && i < cfg.MaxWatchedCoins; i++ {
		cand := passed[i]
		selected = append(selected, cand.Symbol)
		st, exists := activeStates[cand.Symbol]
		if !exists {
			st = model.NewSymbolState(cand.Symbol, perpetualFilters[cand.Symbol], cfg.BootstrapCount, cfg.DeltaWindow+1)
			activeStates[cand.Symbol] = st
		}
		if err := BootstrapSymbolState(st, cand.Candles, serverTime, cfg.BootstrapCount, cfg.DeltaWindow+1); err != nil {
			slog.Warn("scanner: failed to bootstrap symbol state", "symbol", cand.Symbol, "err", err)
		}
	}
	return selected
}
