package scanner

import (
	"fmt"
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
		"ETHUSDT": {
			TickSize: decimal.NewFromFloat(0.1),
			StepSize: decimal.NewFromFloat(0.001),
		},
	}
	volMap := map[string]float64{"ETHUSDT": 100_000_000}
	bookMap := map[string]model.BookQuote{
		"ETHUSDT": {BestBid: 60000.0, BestAsk: 60005.0}, // Spread 0.008% <= 0.03%
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
	if len(selected) != 1 || selected[0] != "ETHUSDT" {
		t.Fatalf("expected ETHUSDT selected, got %v", selected)
	}

	st, exists := activeStates["ETHUSDT"]
	if !exists {
		t.Fatalf("expected active state created for ETHUSDT")
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
		"ETHUSDT": {
			TickSize: decimal.NewFromFloat(0.1),
			StepSize: decimal.NewFromFloat(0.001),
		},
	}
	volMap := map[string]float64{"ETHUSDT": 100_000_000}
	bookMap := map[string]model.BookQuote{
		"ETHUSDT": {BestBid: 60000.0, BestAsk: 60005.0},
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
	if len(selectedMod) != 1 || selectedMod[0] != "ETHUSDT" {
		t.Fatalf("expected ETHUSDT selected via fallback, got %v", selectedMod)
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

func TestRunBulkMarketScannerPartitioned_PrimaryInvarianceAndSweepExtra(t *testing.T) {
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
		MaxWatchedCoins:       10,
		BootstrapCount:        100,
		DeltaWindow:           50,
		CandleDuration:        5 * time.Minute,
		RankingMode:           "pullback",
		SweepMaxExtraCoins:    1,
	}

	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1)}
	filters := map[string]model.ExchangeFilter{
		"PRIMUSDT":  f,
		"DOWN1USDT": f,
		"DOWN2USDT": f,
		"UPEXTUSDT": f,
	}
	volMap := map[string]float64{
		"PRIMUSDT":  100_000_000,
		"DOWN1USDT": 300_000_000, // higher liquidity -> should win 1-slot cap over DOWN2USDT
		"DOWN2USDT": 30_000_000,
		"UPEXTUSDT": 500_000_000,
	}
	bookMap := map[string]model.BookQuote{
		"PRIMUSDT":  {BestBid: 100.0, BestAsk: 100.01},
		"DOWN1USDT": {BestBid: 95.0, BestAsk: 95.01},
		"DOWN2USDT": {BestBid: 95.0, BestAsk: 95.01},
		"UPEXTUSDT": {BestBid: 105.0, BestAsk: 105.01},
	}

	primCandles := makeTestCandles(100.0, 80, now, 120.0)
	// Extended downtrend below VWAP (> 2.5 ATR below session VWAP, and < EMA50) with valid OHLC
	makeExtendedCandles := func(lastP float64) []model.Candle {
		cs := makeTestCandles(100.0, 80, now, 120.0)
		// Gently shift the last bar cleanly with valid OHLC (Open inside [Low, High])
		cs[len(cs)-1].Open = lastP
		cs[len(cs)-1].Close = lastP
		cs[len(cs)-1].High = lastP + 0.2
		cs[len(cs)-1].Low = lastP - 0.2
		return cs
	}
	down1Candles := makeExtendedCandles(95.0)
	down2Candles := makeExtendedCandles(95.0)
	// Extended uptrend above VWAP (> 2.5 ATR above VWAP -> MUST NOT appear in SweepExtra!)
	upCandles := makeExtendedCandles(105.0)

	fetchFn := func(sym string, limit int) []model.Candle {
		switch sym {
		case "PRIMUSDT":
			return primCandles
		case "DOWN1USDT":
			return down1Candles
		case "DOWN2USDT":
			return down2Candles
		case "UPEXTUSDT":
			return upCandles
		default:
			return nil
		}
	}

	offRes := RunBulkMarketScannerPartitioned(baseCfg, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if len(offRes.Primary) != 1 || offRes.Primary[0] != "PRIMUSDT" || len(offRes.SweepExtra) != 0 {
		t.Fatalf("expected Primary=[PRIMUSDT] and SweepExtra=[] when disabled, got %+v", offRes)
	}

	onCfg := baseCfg
	onCfg.SweepExtendedVWAP = true
	onStates := make(map[string]*model.SymbolState)
	onRes := RunBulkMarketScannerPartitioned(onCfg, onStates, filters, volMap, bookMap, fetchFn, now)
	if len(onRes.Primary) != 1 || onRes.Primary[0] != "PRIMUSDT" {
		t.Fatalf("Primary watchlist must remain invariant, got %v", onRes.Primary)
	}
	if len(onRes.SweepExtra) != 1 || onRes.SweepExtra[0] != "DOWN1USDT" {
		t.Fatalf("expected SweepExtra=[DOWN1USDT] (capped at 1, uptrend excluded), got %v", onRes.SweepExtra)
	}
	if _, exists := onStates["DOWN1USDT"]; !exists {
		t.Fatalf("expected DOWN1USDT state bootstrapped into activeStates")
	}
	if _, exists := onStates["UPEXTUSDT"]; exists {
		t.Fatalf("did not expect uptrend extended symbol UPEXTUSDT bootstrapped into activeStates")
	}

	// When SweepExtendedVWAP=true but SweepMaxExtraCoins=0, 0 extra coins must be selected
	zeroExtraCfg := onCfg
	zeroExtraCfg.SweepMaxExtraCoins = 0
	zeroRes := RunBulkMarketScannerPartitioned(zeroExtraCfg, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if len(zeroRes.SweepExtra) != 0 {
		t.Fatalf("expected SweepExtra=[] when SweepMaxExtraCoins=0, got %v", zeroRes.SweepExtra)
	}
}

func TestRunBulkMarketScannerPartitioned_ExcludeTopVolRank(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC)
	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5)}
	filters := map[string]model.ExchangeFilter{
		"BTCUSDT": f,
		"ETHUSDT": f,
		"SOLUSDT": f,
		"MID1USDT": f,
		"MID2USDT": f,
	}
	volMap := map[string]float64{
		"BTCUSDT":  5_000_000_000,
		"ETHUSDT":  3_000_000_000,
		"SOLUSDT":  1_500_000_000,
		"MID1USDT": 200_000_000,
		"MID2USDT": 100_000_000,
	}
	bookMap := map[string]model.BookQuote{
		"BTCUSDT":  {BestBid: 100, BestAsk: 100.01},
		"ETHUSDT":  {BestBid: 100, BestAsk: 100.01},
		"SOLUSDT":  {BestBid: 100, BestAsk: 100.01},
		"MID1USDT": {BestBid: 100, BestAsk: 100.01},
		"MID2USDT": {BestBid: 100, BestAsk: 100.01},
	}
	candles := makeTestCandles(100.0, 80, now, 120.0)
	fetchFn := func(sym string, limit int) []model.Candle { return candles }

	cfg := ScannerConfig{
		MinVolume24hUSDT:      25_000_000,
		MaxSpreadPct:          0.06,
		MinRVOL:               1.2,
		MinFallbackRVOL:       0.5,
		RVOLLookback:          20,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.0,
		MaxScannerVWAPDistATR: 2.5,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       10,
		BootstrapCount:        100,
		DeltaWindow:           50,
		Timeframe:             "5m",
		CandleDuration:        5 * time.Minute,
		RankingMode:           "pullback",
		ExcludeTopVolRank:     2, // Exclude ETHUSDT & SOLUSDT (BTCUSDT is ignored for alt volume ranking)
	}

	res := RunBulkMarketScannerPartitioned(cfg, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	got := make(map[string]bool)
	for _, s := range res.Primary {
		got[s] = true
	}
	if got["ETHUSDT"] || got["SOLUSDT"] {
		t.Fatalf("expected Top-2 alts (ETHUSDT, SOLUSDT) excluded, got %v", res.Primary)
	}
	if !got["MID1USDT"] || !got["MID2USDT"] {
		t.Fatalf("expected Rank 3+ alts (MID1USDT, MID2USDT) retained, got %v", res.Primary)
	}
}

func TestRunBulkMarketScannerPartitioned_DualBucketBreakoutLabeling(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC)
	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5)}
	filters := map[string]model.ExchangeFilter{
		"HIGHNATRUSDT": f,
		"SQUEEZEUSDT":  f,
	}
	volMap := map[string]float64{
		"HIGHNATRUSDT": 20_000_000,
		"SQUEEZEUSDT":  8_000_000,
	}
	bookMap := map[string]model.BookQuote{
		"HIGHNATRUSDT": {BestBid: 100, BestAsk: 100.01},
		"SQUEEZEUSDT":  {BestBid: 100, BestAsk: 100.01},
	}

	makeNATRCandles := func(halfRange float64) []model.Candle {
		out := make([]model.Candle, 80)
		for i := 0; i < 80; i++ {
			tOpen := now.Add(time.Duration(i-80) * 5 * time.Minute)
			out[i] = model.Candle{
				OpenTime:        tOpen,
				CloseTime:       tOpen.Add(5 * time.Minute),
				Open:            100.0,
				High:            100.0 + halfRange,
				Low:             100.0 - halfRange,
				Close:           100.0,
				VolumeBase:      100,
				QuoteVolumeUSDT: 10000,
			}
		}
		return out
	}
	// HIGHNATRUSDT: ~1.20% NATR (halfRange 0.6 -> true range 1.2)
	// SQUEEZEUSDT: ~0.60% NATR (halfRange 0.3 -> true range 0.6), compressed vol
	highC := makeNATRCandles(0.60)
	sqC := makeNATRCandles(0.30)
	fetchFn := func(sym string, limit int) []model.Candle {
		if sym == "HIGHNATRUSDT" {
			return highC
		}
		return sqC
	}

	cfgOff := ScannerConfig{
		MinVolume24hUSDT:      15_000_000,
		MaxSpreadPct:          0.08,
		MinRVOL:               1.0,
		MinFallbackRVOL:       0.5,
		RVOLLookback:          20,
		MinNATRPct:            0.95,
		MaxNATRPct:            1.75,
		MaxScannerVWAPDistATR: 2.5,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       10,
		BootstrapCount:        100,
		DeltaWindow:           50,
		Timeframe:             "5m",
		CandleDuration:        5 * time.Minute,
		RankingMode:           "pullback",
	}
	resOff := RunBulkMarketScannerPartitioned(cfgOff, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if len(resOff.Breakout) != 0 {
		t.Fatalf("expected nil/empty Breakout when BreakoutScannerEnabled=false, got %v", resOff.Breakout)
	}
	if len(resOff.Primary) != 1 || resOff.Primary[0] != "HIGHNATRUSDT" {
		t.Fatalf("expected only HIGHNATRUSDT in Primary, got %v", resOff.Primary)
	}

	cfgOn := cfgOff
	cfgOn.BreakoutScannerEnabled = true
	cfgOn.BreakoutMinNATRPct = 0.55
	cfgOn.BreakoutMinVolume24hUSDT = 6_000_000
	cfgOn.BreakoutMaxWatchedCoins = 10

	states := make(map[string]*model.SymbolState)
	resOn := RunBulkMarketScannerPartitioned(cfgOn, states, filters, volMap, bookMap, fetchFn, now)
	if len(resOn.Primary) != 1 || resOn.Primary[0] != "HIGHNATRUSDT" {
		t.Fatalf("expected Primary=[HIGHNATRUSDT] untouched, got %v", resOn.Primary)
	}
	boGot := make(map[string]bool)
	for _, s := range resOn.Breakout {
		boGot[s] = true
	}
	if !boGot["HIGHNATRUSDT"] || !boGot["SQUEEZEUSDT"] {
		t.Fatalf("expected both HIGHNATRUSDT and SQUEEZEUSDT labeled in Breakout, got %v", resOn.Breakout)
	}
	if _, ok := states["SQUEEZEUSDT"]; !ok {
		t.Fatalf("expected breakout-only SQUEEZEUSDT bootstrapped into activeStates")
	}
}

func TestRunBulkMarketScannerPartitioned_ComputesMarketBreadthIncludingExcludedAndRejected(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	make15mSeries := func(startPrice, step float64) []model.Candle {
		const totalBars = 205 * 4
		out := make([]model.Candle, 0, totalBars)
		for i := 0; i < totalBars; i++ {
			ot := now.Add(-time.Duration(totalBars-i) * 15 * time.Minute)
			ct := ot.Add(15*time.Minute - time.Millisecond)
			c := startPrice + float64(i)*step
			out = append(out, model.Candle{
				OpenTime:        ot,
				CloseTime:       ct,
				Open:            c,
				High:            c * 1.005,
				Low:             c * 0.995,
				Close:           c,
				VolumeBase:      1000,
				QuoteVolumeUSDT: 1000 * c,
			})
		}
		return out
	}

	bull := make15mSeries(100.0, 0.2)
	bear := make15mSeries(500.0, -0.3)

	filters := make(map[string]model.ExchangeFilter)
	volMap := make(map[string]float64)
	bookMap := make(map[string]model.BookQuote)
	klineMap := make(map[string][]model.Candle)

	// 12 liquid coins: 3 bullish (including top-vol excluded ALT0USDT), 9 bearish (including wide-spread ALT11USDT)
	for i := 0; i < 12; i++ {
		sym := fmt.Sprintf("ALT%dUSDT", i)
		filters[sym] = model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinQty: decimal.NewFromFloat(0.1), MinNotional: decimal.NewFromFloat(5)}
		volMap[sym] = 50_000_000.0 - float64(i)*1_000_000.0
		if i < 3 {
			klineMap[sym] = bull
			px := bull[len(bull)-1].Close
			bookMap[sym] = model.BookQuote{BestBid: px, BestAsk: px * 1.0002}
		} else {
			klineMap[sym] = bear
			px := bear[len(bear)-1].Close
			spread := 1.0002
			if i == 11 {
				spread = 1.01 // fails spread pre-screen yet must still count toward liquidity breadth
			}
			bookMap[sym] = model.BookQuote{BestBid: px, BestAsk: px * spread}
		}
	}
	filters["BTCUSDT"] = model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.1)}
	volMap["BTCUSDT"] = 999_000_000.0
	klineMap["BTCUSDT"] = bull
	bookMap["BTCUSDT"] = model.BookQuote{BestBid: 100, BestAsk: 100.01}

	fetchFn := func(sym string, limit int) []model.Candle {
		c := klineMap[sym]
		if limit < len(c) {
			return c[len(c)-limit:]
		}
		return c
	}

	cfgOff := ScannerConfig{
		MinVolume24hUSDT:     15_000_000,
		MaxSpreadPct:         0.10,
		MinRVOL:              0.50,
		MinFallbackRVOL:      0.50,
		RVOLLookback:         20,
		MinNATRPct:           0.10,
		MaxNATRPct:           5.00,
		ATRPeriod:            14,
		EMAPeriod:            200,
		MaxWatchedCoins:      10,
		BootstrapCount:       900,
		DeltaWindow:          50,
		Timeframe:            "15m",
		CandleDuration:       15 * time.Minute,
		RankingMode:          "pullback",
		ExcludeTopVolRank:    1,
		ComputeMarketBreadth: false,
	}
	resOff := RunBulkMarketScannerPartitioned(cfgOff, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if resOff.MarketBreadthValid {
		t.Fatalf("expected MarketBreadthValid=false when ComputeMarketBreadth=false")
	}

	cfgOn := cfgOff
	cfgOn.ComputeMarketBreadth = true
	resOn := RunBulkMarketScannerPartitioned(cfgOn, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if !resOn.MarketBreadthValid || resOn.MarketBreadthTotal != 12 || math.Abs(resOn.MarketBreadthPct-0.25) > 1e-9 {
		t.Fatalf("expected breadth=0.25 total=12 valid=true, got pct=%f total=%d valid=%v", resOn.MarketBreadthPct, resOn.MarketBreadthTotal, resOn.MarketBreadthValid)
	}
	for _, s := range resOn.Primary {
		if s == "ALT0USDT" || s == "ALT11USDT" {
			t.Fatalf("expected excluded/wide-spread symbol %s omitted from Primary watchlist", s)
		}
	}
}

func TestRunBulkMarketScannerPartitioned_ExcludesBTCUSDTFromWatchlistAndRefreshesFilter(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	oldFilter := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1), MinQty: decimal.NewFromFloat(0.1)}
	newFilter := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.001), StepSize: decimal.NewFromFloat(1.0), MinQty: decimal.NewFromFloat(1.0), MaxQty: decimal.NewFromFloat(500)}
	filters := map[string]model.ExchangeFilter{
		"BTCUSDT": oldFilter,
		"ETHUSDT": newFilter,
	}
	volMap := map[string]float64{"BTCUSDT": 9_000_000_000, "ETHUSDT": 200_000_000}
	bookMap := map[string]model.BookQuote{
		"BTCUSDT": {BestBid: 80000, BestAsk: 80001},
		"ETHUSDT": {BestBid: 100, BestAsk: 100.01},
	}

	candles := makeTestCandles(100.0, 90, now, 250.0)
	var btcFetched int
	fetchFn := func(sym string, limit int) []model.Candle {
		if sym == "BTCUSDT" {
			btcFetched++
		}
		return candles
	}

	existingETH := model.NewSymbolState("ETHUSDT", oldFilter, 200, 51)
	// Inject a gapped pair inside ETHUSDT's cached state so BootstrapSymbolState repairs it cleanly.
	existingETH.ApplyClosedCandleLocked(candles[0], 200, 51)
	existingETH.ApplyClosedCandleLocked(candles[5], 200, 51)

	activeStates := map[string]*model.SymbolState{"ETHUSDT": existingETH}
	cfg := ScannerConfig{
		MinVolume24hUSDT:      25_000_000,
		MaxSpreadPct:          0.08,
		MinRVOL:               1.20,
		MinFallbackRVOL:       0.50,
		RVOLLookback:          20,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		MaxScannerVWAPDistATR: 2.50,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       10,
		BootstrapCount:        100,
		DeltaWindow:           50,
		Timeframe:             "5m",
		CandleDuration:        5 * time.Minute,
		RankingMode:           "pullback",
		BreakoutScannerEnabled: true,
		BreakoutMaxWatchedCoins: 5,
		SweepExtendedVWAP:     true,
		SweepMaxExtraCoins:    5,
	}

	res := RunBulkMarketScannerPartitioned(cfg, activeStates, filters, volMap, bookMap, fetchFn, now)
	if btcFetched != 0 {
		t.Fatalf("expected scanner never to fetch BTCUSDT klines, got %d calls", btcFetched)
	}
	if len(res.Primary) != 1 || res.Primary[0] != "ETHUSDT" || len(res.Breakout) > 1 && res.Breakout[0] == "BTCUSDT" {
		t.Fatalf("expected only ETHUSDT selected and BTCUSDT excluded, got %+v", res)
	}
	if !existingETH.Filter.TickSize.Equal(newFilter.TickSize) || !existingETH.Filter.StepSize.Equal(newFilter.StepSize) || !existingETH.Filter.MaxQty.Equal(newFilter.MaxQty) {
		t.Fatalf("expected existing ETHUSDT SymbolState filter refreshed, got %+v", existingETH.Filter)
	}
	existingETH.Mu.Lock()
	hasGap := existingETH.HasRecentGapLocked(5 * time.Minute)
	existingETH.Mu.Unlock()
	if hasGap {
		t.Fatalf("expected BootstrapSymbolState to rebuild & repair prior cached candle gap")
	}
}

func TestRunBulkMarketScannerPartitioned_RollingVWAPAndHighKlineLimit(t *testing.T) {
	now := time.Date(2026, 11, 20, 0, 10, 0, 0, time.UTC) // Just past midnight UTC
	f := model.ExchangeFilter{TickSize: decimal.NewFromFloat(0.01), StepSize: decimal.NewFromFloat(0.1)}
	filters := map[string]model.ExchangeFilter{"SOLUSDT": f}
	volMap := map[string]float64{"SOLUSDT": 100_000_000}
	bookMap := map[string]model.BookQuote{"SOLUSDT": {BestBid: 100, BestAsk: 100.01}}

	// Build 80 15m candles: 78 on previous UTC day at 100, last 2 on current UTC day at 108 -> session VWAP ~108 (dist ~0), rolling VWAP(50) ~100.3 (dist ~8 ATR)
	candles := make([]model.Candle, 80)
	step := 15 * time.Minute
	for i := 0; i < 80; i++ {
		tOpen := now.Add(time.Duration(i-80) * step)
		vol := 100.0
		if i == 79 {
			vol = 200.0
		}
		price := 100.0
		if i >= 78 {
			price = 108.0
		}
		candles[i] = model.Candle{
			OpenTime:        tOpen,
			CloseTime:       tOpen.Add(step - time.Millisecond),
			Open:            price,
			High:            price + 0.5,
			Low:             price - 0.5,
			Close:           price,
			VolumeBase:      vol,
			QuoteVolumeUSDT: vol * price,
		}
	}

	var requestedLimit int
	fetchFn := func(sym string, limit int) []model.Candle {
		requestedLimit = limit
		return candles
	}

	cfg := ScannerConfig{
		MinVolume24hUSDT:      25_000_000,
		MaxSpreadPct:          0.08,
		MinRVOL:               1.20,
		MinFallbackRVOL:       0.50,
		RVOLLookback:          20,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		MaxScannerVWAPDistATR: 2.00,
		ATRPeriod:             14,
		EMAPeriod:             50,
		MaxWatchedCoins:       10,
		BootstrapCount:        1280,
		DeltaWindow:           50,
		Timeframe:             "15m",
		CandleDuration:        15 * time.Minute,
		RankingMode:           "pullback",
		RollingVWAPBars:       50,
	}

	res := RunBulkMarketScannerPartitioned(cfg, make(map[string]*model.SymbolState), filters, volMap, bookMap, fetchFn, now)
	if requestedLimit != 1281 {
		t.Fatalf("expected requested scanner kline limit 1281 (above legacy 1000 cap), got %d", requestedLimit)
	}
	if len(res.Primary) != 0 || res.Stats.VWAPDistRejected != 1 {
		t.Fatalf("expected RollingVWAPBars=50 to reject 108 vs ~100.3 rolling VWAP, got %+v", res)
	}
}
