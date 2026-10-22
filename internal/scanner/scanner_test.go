package scanner

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
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
