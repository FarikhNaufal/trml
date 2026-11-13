package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/model"
)

func TestBuildSimBrokerConfig_FromEnvFile(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, "test_backtest.env")
	content := `ENV=development
TRADING_MODE=PAPER
ALLOWED_SIDE=SHORT_ONLY
TIMEFRAME=15m
MAX_CONCURRENT_SLOTS=10
MAX_DAILY_DRAWDOWN_PCT=-0.05
ACCOUNT_LEVERAGE=8
MAKER_FEE_RATE=0.0002
TAKER_FEE_RATE=0.0005
SLIPPAGE_RATE=0.0002
PARTIAL_TP_ENABLED=true
PARTIAL_TP1_RATIO=0.45
PARTIAL_TP1_ATR_MULT=1.20
TRAILING_ENABLED=true
TRAILING_ACTIVATE_ATR=1.50
TRAILING_DISTANCE_ATR=0.60
MAX_HOLDING_DURATION=6h
EQUITY_MODE_COMPOUNDING=true
`
	if err := os.WriteFile(envFile, []byte(content), 0644); err != nil {
		t.Fatalf("write temp env failed: %v", err)
	}

	simCfg := buildSimBrokerConfig(envFile, 250.0)
	if !simCfg.InitialEquityUSDT.Equal(decimal.NewFromFloat(250.0)) {
		t.Errorf("expected equity override 250.0, got %s", simCfg.InitialEquityUSDT)
	}
	if simCfg.MaxSlots != 10 {
		t.Errorf("expected MaxSlots 10, got %d", simCfg.MaxSlots)
	}
	if !simCfg.EquityModeCompounding {
		t.Errorf("expected EquityModeCompounding true from env file")
	}
	if simCfg.MaxHoldingDuration != 6*time.Hour {
		t.Errorf("expected 6h max holding, got %v", simCfg.MaxHoldingDuration)
	}
	if simCfg.PartialTP1Ratio != 0.45 {
		t.Errorf("expected PartialTP1Ratio 0.45, got %f", simCfg.PartialTP1Ratio)
	}

	evalCfg := buildDefaultEvaluatorConfig(envFile)
	if evalCfg.AllowedSide != "SHORT_ONLY" {
		t.Errorf("expected AllowedSide SHORT_ONLY, got %s", evalCfg.AllowedSide)
	}
	if evalCfg.CandleDuration != 15*time.Minute {
		t.Errorf("expected CandleDuration 15m, got %v", evalCfg.CandleDuration)
	}

	cfg, err := loadConfigForBacktest(envFile)
	if err != nil {
		t.Fatalf("loadConfigForBacktest failed: %v", err)
	}
	interval, dur, scanCfg := resolveBacktestScannerConfig(cfg, "")
	if interval != "15m" || dur != 15*time.Minute || scanCfg.Timeframe != "15m" {
		t.Errorf("unexpected scanner config resolution: interval=%s dur=%v tf=%s", interval, dur, scanCfg.Timeframe)
	}

	if !isCompoundingConfigured(envFile) {
		t.Errorf("expected isCompoundingConfigured true for EQUITY_MODE_COMPOUNDING=true")
	}
}

func TestIsCompoundingConfigured_FalseByDefault(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, "fixed.env")
	if err := os.WriteFile(envFile, []byte("EQUITY_MODE_COMPOUNDING=false\n"), 0644); err != nil {
		t.Fatalf("write temp env failed: %v", err)
	}
	t.Setenv("EQUITY_MODE_COMPOUNDING", "")
	if isCompoundingConfigured(envFile) {
		t.Errorf("expected false when EQUITY_MODE_COMPOUNDING=false")
	}
}

func TestLoadCandlesParallel_SkipsAbsentSymbolsWhenExchangeFiltersPresent(t *testing.T) {
	cacheDir := t.TempDir()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	writeKline := func(sym string) {
		dir := filepath.Join(cacheDir, sym, "5m")
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		content := "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n" +
			"1767225600000,100.0,101.0,99.0,100.5,10.0,1767225899999,1005.0,50,6.0,603.0,0\n"
		path := filepath.Join(dir, sym+"-5m-2026-01.csv")
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("write kline failed: %v", err)
		}
	}
	writeKline("BTCUSDT")
	writeKline("NVDAUSDT")

	dl := data.NewDownloader(cacheDir)
	dl.SetOffline(true)

	// 1. When exchangeFilters is populated, symbols absent from it (e.g. TRADIFI_PERPETUAL) must be skipped.
	exFilters := map[string]model.ExchangeFilter{
		"BTCUSDT": {
			TickSize:    decimal.NewFromFloat(0.1),
			StepSize:    decimal.NewFromFloat(0.001),
			MinNotional: decimal.NewFromFloat(5.0),
			MinQty:      decimal.NewFromFloat(0.001),
		},
	}
	candles, filters := loadCandlesParallel(context.Background(), dl, []string{"BTCUSDT", "NVDAUSDT"}, "5m", from, to, exFilters)
	if len(candles) != 1 || candles["BTCUSDT"] == nil || candles["NVDAUSDT"] != nil {
		t.Fatalf("expected only BTCUSDT loaded when exchangeFilters excludes NVDAUSDT, got %v", candles)
	}
	if _, ok := filters["NVDAUSDT"]; ok {
		t.Fatalf("did not expect synthetic filter for excluded NVDAUSDT")
	}

	// 2. When exchangeFilters is nil/empty, fallback synthetic filters allow offline standalone runs.
	candlesFallback, filtersFallback := loadCandlesParallel(context.Background(), dl, []string{"BTCUSDT"}, "5m", from, to, nil)
	if len(candlesFallback) != 1 || !filtersFallback["BTCUSDT"].TickSize.Equal(decimal.NewFromFloat(0.01)) {
		t.Fatalf("expected fallback synthetic filter when exchangeFilters is nil, got %+v", filtersFallback)
	}
}
