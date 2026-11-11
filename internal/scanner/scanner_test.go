package scanner

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/config"
	"trml/internal/model"
)

func TestScannerAndBootstrap(t *testing.T) {
	now := time.Now().UTC()
	cfg := ScannerConfig{
		MinVolume24hUSDT: 50_000_000,
		MaxSpreadPct:     0.03,
		MinRVOL:          1.50,
		RVOLLookback:     20,
		MinNATRPct:       0.20,
		MaxNATRPct:       5.0,
		ATRPeriod:        14,
		EMAPeriod:        50,
		MaxWatchedCoins:  10,
		BootstrapCount:   100,
		DeltaWindow:      50,
	}

	activeStates := make(map[string]*model.SymbolState)
	filters := map[string]model.ExchangeFilter{
		"BTCUSDT": {
			TickSize: decimal.NewFromFloat(0.1),
			StepSize: decimal.NewFromFloat(0.001),
		},
	}
	volMap := map[string]float64{"BTCUSDT": 100_000_000}
	bookMap := map[string]model.BookQuote{
		"BTCUSDT": {BestBid: 60000.0, BestAsk: 60005.0}, // Spread 0.008% <= 0.03%
	}

	// Buat 101 candle
	candles := make([]model.Candle, 101)
	for i := 0; i < 101; i++ {
		tOpen := now.Add(time.Duration(i-101) * 5 * time.Minute)
		tClose := tOpen.Add(5 * time.Minute)
		candles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose,
			Open:            60000 + float64(i),
			High:            60300 + float64(i),
			Low:             59700 + float64(i),
			Close:           60005 + float64(i),
			VolumeBase:      100 + float64(i),
			QuoteVolumeUSDT: 6_000_000,
		}
	}
	candles[100].VolumeBase = 500.0 // RVOL > 2.0x

	fetchFn := func(sym string, limit int) []model.Candle {
		return candles
	}

	selected := RunBulkMarketScanner(cfg, activeStates, filters, volMap, bookMap, fetchFn, now)
	if len(selected) != 1 || selected[0] != "BTCUSDT" {
		t.Fatalf("expected BTCUSDT selected, got %v", selected)
	}

	st, exists := activeStates["BTCUSDT"]
	if !exists {
		t.Fatalf("expected active state created for BTCUSDT")
	}
	if len(st.ClosedCandles) < 50 {
		t.Errorf("expected closed candles populated, got %d", len(st.ClosedCandles))
	}
}

func TestScannerFallbackFilter(t *testing.T) {
	now := time.Now().UTC()
	cfg := ScannerConfig{
		MinVolume24hUSDT: 50_000_000,
		MaxSpreadPct:     0.03,
		MinRVOL:          1.80,
		MinFallbackRVOL:  1.20,
		RVOLLookback:     20,
		MinNATRPct:       0.20,
		MaxNATRPct:       5.0,
		ATRPeriod:        14,
		EMAPeriod:        50,
		MaxWatchedCoins:  10,
		BootstrapCount:   100,
		DeltaWindow:      50,
	}

	filters := map[string]model.ExchangeFilter{
		"BTCUSDT": {
			TickSize: decimal.NewFromFloat(0.1),
			StepSize: decimal.NewFromFloat(0.001),
		},
	}
	volMap := map[string]float64{"BTCUSDT": 100_000_000}
	bookMap := map[string]model.BookQuote{
		"BTCUSDT": {BestBid: 60000.0, BestAsk: 60005.0},
	}

	// 1. Kasus koin mati (Volume sangat rendah, RVOL ~ 0.33)
	candlesDead := make([]model.Candle, 101)
	for i := 0; i < 101; i++ {
		tOpen := now.Add(time.Duration(i-101) * 5 * time.Minute)
		tClose := tOpen.Add(5 * time.Minute)
		candlesDead[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose,
			Open:            60000 + float64(i),
			High:            60300 + float64(i),
			Low:             59700 + float64(i),
			Close:           60005 + float64(i),
			VolumeBase:      150,
			QuoteVolumeUSDT: 6_000_000,
		}
	}
	candlesDead[100].VolumeBase = 50.0 // RVOL = 50/150 = 0.33 < MinFallbackRVOL (1.20)

	activeStates := make(map[string]*model.SymbolState)
	selected := RunBulkMarketScanner(cfg, activeStates, filters, volMap, bookMap, func(sym string, limit int) []model.Candle {
		return candlesDead
	}, now)
	if len(selected) != 0 {
		t.Fatalf("expected 0 selected for dead coin below MinFallbackRVOL, got %d", len(selected))
	}

	// 2. Kasus fallback valid (RVOL 1.33 >= MinFallbackRVOL 1.20 tapi < MinRVOL 1.80)
	candlesModerate := make([]model.Candle, 101)
	copy(candlesModerate, candlesDead)
	candlesModerate[100].VolumeBase = 200.0 // RVOL = 200/150 = 1.33

	selectedMod := RunBulkMarketScanner(cfg, activeStates, filters, volMap, bookMap, func(sym string, limit int) []model.Candle {
		return candlesModerate
	}, now)
	if len(selectedMod) != 1 || selectedMod[0] != "BTCUSDT" {
		t.Fatalf("expected BTCUSDT selected via fallback, got %v", selectedMod)
	}
}

func TestScannerVWAPProximityFilter(t *testing.T) {
	now := time.Now().UTC()
	cfg := ScannerConfig{
		MinVolume24hUSDT:      50_000_000,
		MaxSpreadPct:          0.05,
		MinRVOL:               1.0,
		RVOLLookback:          20,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.0,
		MaxScannerVWAPDistATR: 2.50,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       10,
		BootstrapCount:        100,
		DeltaWindow:           50,
	}

	filters := map[string]model.ExchangeFilter{
		"PUMPUSDT": {
			TickSize: decimal.NewFromFloat(0.01),
			StepSize: decimal.NewFromFloat(1.0),
		},
	}
	volMap := map[string]float64{"PUMPUSDT": 100_000_000}
	bookMap := map[string]model.BookQuote{
		"PUMPUSDT": {BestBid: 150.0, BestAsk: 150.01},
	}

	// 101 candle dengan harga naik drastis di candle terakhir menjauhi Session VWAP
	// VWAP berada di ~100, tapi harga melonjak ke 150 (ATR = 10 -> Dist = 5 ATR > 2.50)
	candlesFar := make([]model.Candle, 101)
	for i := 0; i < 101; i++ {
		tOpen := now.Add(time.Duration(i-101) * 5 * time.Minute)
		tClose := tOpen.Add(5 * time.Minute)
		price := 100.0
		if i == 100 {
			price = 150.0 // Melonjak jauh
		}
		candlesFar[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose,
			Open:            price,
			High:            price + 5.0,
			Low:             price - 5.0,
			Close:           price,
			VolumeBase:      1000.0,
			QuoteVolumeUSDT: 100_000,
		}
	}
	candlesFar[100].VolumeBase = 3000.0 // High RVOL

	activeStates := make(map[string]*model.SymbolState)
	selected := RunBulkMarketScanner(cfg, activeStates, filters, volMap, bookMap, func(sym string, limit int) []model.Candle {
		return candlesFar
	}, now)

	if len(selected) != 0 {
		t.Fatalf("expected PUMPUSDT to be filtered out due to VWAP distance > 2.50 ATR, got %v", selected)
	}
}

func TestScannerNaNAndInfRejection(t *testing.T) {
	now := time.Now().UTC()
	cfg := ScannerConfig{
		MinVolume24hUSDT: 50_000_000,
		MaxSpreadPct:     0.05,
		MinRVOL:          1.0,
		RVOLLookback:     20,
		MinNATRPct:       0.20,
		MaxNATRPct:       5.0,
		ATRPeriod:        14,
		EMAPeriod:        50,
		MaxWatchedCoins:  10,
		BootstrapCount:   100,
		DeltaWindow:      50,
	}

	filters := map[string]model.ExchangeFilter{
		"NANUSDT": {
			TickSize: decimal.NewFromFloat(0.01),
			StepSize: decimal.NewFromFloat(1.0),
		},
	}

	// 1. NaN Volume
	volMapNaN := map[string]float64{"NANUSDT": math.NaN()}
	bookMapValid := map[string]model.BookQuote{"NANUSDT": {BestBid: 100.0, BestAsk: 100.02}}
	selected := RunBulkMarketScanner(cfg, make(map[string]*model.SymbolState), filters, volMapNaN, bookMapValid, nil, now)
	if len(selected) != 0 {
		t.Fatalf("expected NaN volume to be rejected, got %v", selected)
	}

	// 2. NaN BestBid
	volMapValid := map[string]float64{"NANUSDT": 100_000_000}
	bookMapNaNBid := map[string]model.BookQuote{"NANUSDT": {BestBid: math.NaN(), BestAsk: 100.02}}
	selected = RunBulkMarketScanner(cfg, make(map[string]*model.SymbolState), filters, volMapValid, bookMapNaNBid, nil, now)
	if len(selected) != 0 {
		t.Fatalf("expected NaN BestBid to be rejected, got %v", selected)
	}

	// 3. Inf BestAsk
	bookMapInfAsk := map[string]model.BookQuote{"NANUSDT": {BestBid: 100.0, BestAsk: math.Inf(1)}}
	selected = RunBulkMarketScanner(cfg, make(map[string]*model.SymbolState), filters, volMapValid, bookMapInfAsk, nil, now)
	if len(selected) != 0 {
		t.Fatalf("expected Inf BestAsk to be rejected, got %v", selected)
	}
}

func makeTestCandles(baseClose float64, count int, now time.Time, lastVol float64) []model.Candle {
	out := make([]model.Candle, count)
	for i := 0; i < count; i++ {
		tOpen := now.Add(time.Duration(i-count) * 5 * time.Minute)
		tClose := tOpen.Add(5 * time.Minute)
		vol := 100.0
		if i == count-1 {
			vol = lastVol
		}
		out[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tClose,
			Open:            baseClose,
			High:            baseClose * 1.005,
			Low:             baseClose * 0.995,
			Close:           baseClose,
			VolumeBase:      vol,
			QuoteVolumeUSDT: vol * baseClose,
		}
	}
	return out
}

func TestRunBulkMarketScannerWithStats(t *testing.T) {
	now := time.Now().UTC().Truncate(5 * time.Minute).Add(2 * time.Minute)
	cfg := ScannerConfig{
		MinVolume24hUSDT:      50_000_000,
		MaxSpreadPct:          0.05,
		MinRVOL:               1.80,
		MinFallbackRVOL:       0.50,
		RVOLLookback:          20,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.0,
		MaxScannerVWAPDistATR: 2.50,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       3,
		BootstrapCount:        100,
		DeltaWindow:           50,
		CandleDuration:        5 * time.Minute,
	}

	f := model.ExchangeFilter{
		TickSize: decimal.NewFromFloat(0.01),
		StepSize: decimal.NewFromFloat(0.1),
	}
	filters := map[string]model.ExchangeFilter{
		"BTCBUSD":  f, // Non-USDT ignored
		"LOWVOL":   f, // Will reject unless suffix USDT -> rename below
		"VOLUSDT":  f, // Low 24h vol
		"SPRUSDT":  f, // Wide spread
		"GAPUSDT":  f, // Gap in klines
		"NATRUSDT": f, // NATR below MinNATRPct
		"PRIMUSDT": f, // Passes primary RVOL >= 1.80
		"FALLUSDT": f, // Passes fallback 1.0 >= 0.50
	}
	delete(filters, "LOWVOL")

	volMap := map[string]float64{
		"VOLUSDT":  1_000_000,
		"SPRUSDT":  100_000_000,
		"GAPUSDT":  100_000_000,
		"NATRUSDT": 100_000_000,
		"PRIMUSDT": 100_000_000,
		"FALLUSDT": 100_000_000,
	}
	bookMap := map[string]model.BookQuote{
		"SPRUSDT":  {BestBid: 100.0, BestAsk: 100.20}, // 0.20% > 0.04%
		"GAPUSDT":  {BestBid: 100.0, BestAsk: 100.01},
		"NATRUSDT": {BestBid: 100.0, BestAsk: 100.01},
		"PRIMUSDT": {BestBid: 100.0, BestAsk: 100.01},
		"FALLUSDT": {BestBid: 100.0, BestAsk: 100.01},
	}

	gapCandles := makeTestCandles(100.0, 80, now, 100.0)
	// Introduce a 10m gap between candle 40 and 41
	gapCandles = append(gapCandles[:40], gapCandles[41:]...)

	flatCandles := make([]model.Candle, 80)
	for i := 0; i < 80; i++ {
		tOpen := now.Add(time.Duration(i-80) * 5 * time.Minute)
		flatCandles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tOpen.Add(5 * time.Minute),
			Open:            100.0,
			High:            100.01,
			Low:             99.99,
			Close:           100.0,
			VolumeBase:      100.0,
			QuoteVolumeUSDT: 10_000,
		}
	}

	primCandles := makeTestCandles(100.0, 80, now, 250.0) // RVOL = 2.50
	fallCandles := makeTestCandles(100.0, 80, now, 100.0) // RVOL = 1.00

	fetchFn := func(sym string, limit int) []model.Candle {
		switch sym {
		case "GAPUSDT":
			return gapCandles
		case "NATRUSDT":
			return flatCandles
		case "PRIMUSDT":
			return primCandles
		case "FALLUSDT":
			return fallCandles
		default:
			return nil
		}
	}

	activeStates := make(map[string]*model.SymbolState)
	selected, stats := RunBulkMarketScannerWithStats(cfg, activeStates, filters, volMap, bookMap, fetchFn, now)

	if len(selected) != 2 || selected[0] != "PRIMUSDT" || selected[1] != "FALLUSDT" {
		t.Fatalf("expected [PRIMUSDT FALLUSDT], got %v", selected)
	}
	if stats.TotalPerpUSDT != 6 || stats.VolRejected != 1 || stats.SpreadRejected != 1 || stats.PreFilterPassed != 4 {
		t.Fatalf("unexpected pre-filter stats: %+v", stats)
	}
	if stats.KlineGapOrInvalid != 1 || stats.NATRRejected != 1 {
		t.Fatalf("unexpected kline/natr reject stats: %+v", stats)
	}
	if stats.PrimaryPassed != 1 || stats.FallbackUsed != 1 || stats.SelectedCount != 2 {
		t.Fatalf("unexpected selection stats: %+v", stats)
	}
}

func TestFromAppConfig(t *testing.T) {
	nilCfg := FromAppConfig(nil, 250)
	if nilCfg.BootstrapCount != 250 || nilCfg.RankingMode != defaultScannerRankingMode || nilCfg.MinNATRPct != defaultScannerMinNATRPct {
		t.Fatalf("unexpected defaults for nil app config: %+v", nilCfg)
	}

	appCfg := &config.Config{
		MinVolume24hUSDT:      30_000_000,
		MaxSpreadPct:          0.04,
		MinRVOL:               1.35,
		MinFallbackRVOL:       0.65,
		MinNATRPct:            0.60,
		MaxNATRPct:            2.00,
		MaxScannerVWAPDistATR: 2.50,
		EMAPeriod:             150,
		MaxWatchedCoins:       30,
		Timeframe:             "15m",
		CandleDuration:        15 * time.Minute,
	}
	sc := FromAppConfig(appCfg, 1000)
	if sc.MinRVOL != 1.35 || sc.EMAPeriod != 150 || sc.CandleDuration != 15*time.Minute || sc.Timeframe != "15m" || sc.RankingMode != "pullback" {
		t.Fatalf("unexpected resolved ScannerConfig: %+v", sc)
	}

	appCfg.MinRVOL = 0
	appCfg.EMAPeriod = 0
	appCfg.ScannerRankingMode = "rvol"
	fb := FromAppConfig(appCfg, 0)
	if fb.MinRVOL != defaultScannerMinRVOL || fb.EMAPeriod != defaultScannerEMAPeriod || fb.BootstrapCount != defaultScannerBootstrap || fb.RankingMode != "rvol" {
		t.Fatalf("unexpected fallback fields: %+v", fb)
	}
}

func TestRunBulkMarketScanner_PullbackRanking(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC)
	baseCfg := ScannerConfig{
		MinVolume24hUSDT:      15_000_000,
		MaxSpreadPct:          0.05,
		MinRVOL:               1.20,
		MinFallbackRVOL:       0.50,
		RVOLLookback:          20,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		MaxScannerVWAPDistATR: 2.50,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       1,
		BootstrapCount:        100,
		DeltaWindow:           50,
		CandleDuration:        5 * time.Minute,
	}
	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1)}
	filters := map[string]model.ExchangeFilter{"PULLUSDT": f, "PUMPUSDT": f}
	volMap := map[string]float64{"PULLUSDT": 300_000_000, "PUMPUSDT": 50_000_000}
	bookMap := map[string]model.BookQuote{
		"PULLUSDT": {BestBid: 100.0, BestAsk: 100.01},
		"PUMPUSDT": {BestBid: 100.0, BestAsk: 100.01},
	}

	pullCandles := makeTestCandles(100.0, 80, now, 160.0) // RVOL 1.60, right on VWAP
	pumpCandles := makeTestCandles(100.0, 80, now, 350.0) // RVOL 3.50 (>= 2.50 exhaustion penalty)
	last := &pumpCandles[len(pumpCandles)-1]
	last.Close = 101.2 // ~1.2 ATR away from VWAP
	last.High = 101.3
	last.Low = 99.9

	fetchFn := func(sym string, limit int) []model.Candle {
		if sym == "PULLUSDT" {
			return pullCandles
		}
		return pumpCandles
	}

	rvolSelected := RunBulkMarketScanner(baseCfg, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if len(rvolSelected) != 1 || rvolSelected[0] != "PUMPUSDT" {
		t.Fatalf("default RVOL ranking expected [PUMPUSDT], got %v", rvolSelected)
	}

	pullCfg := baseCfg
	pullCfg.RankingMode = "pullback"
	pullSelected := RunBulkMarketScanner(pullCfg, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if len(pullSelected) != 1 || pullSelected[0] != "PULLUSDT" {
		t.Fatalf("pullback ranking expected [PULLUSDT], got %v", pullSelected)
	}
}
