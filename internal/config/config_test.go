package config

import (
	"os"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	_ = os.Unsetenv("BINANCE_ENV")
	_ = os.Unsetenv("DRY_RUN_MODE")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.Env != EnvTestnet {
		t.Errorf("expected default env testnet, got %s", cfg.Env)
	}
	if !cfg.DryRun {
		t.Errorf("expected default dry run true")
	}
	if cfg.RestBaseURL != "https://testnet.binancefuture.com" {
		t.Errorf("expected testnet REST URL, got %s", cfg.RestBaseURL)
	}
	if cfg.MaxSlots != 3 {
		t.Errorf("expected 3 slots, got %d", cfg.MaxSlots)
	}
	if cfg.RetestLookbackBars != 4 {
		t.Errorf("expected 4 retest lookback bars, got %d", cfg.RetestLookbackBars)
	}
	if cfg.MaxWatchedCoins != 20 {
		t.Errorf("expected 20 max watched coins, got %d", cfg.MaxWatchedCoins)
	}
}

func TestLoadConfigMainnet(t *testing.T) {
	_ = os.Setenv("BINANCE_ENV", "mainnet")
	defer func() { _ = os.Unsetenv("BINANCE_ENV") }()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.Env != EnvMainnet {
		t.Errorf("expected env mainnet, got %s", cfg.Env)
	}
	if cfg.RestBaseURL != "https://fapi.binance.com" {
		t.Errorf("expected mainnet REST URL, got %s", cfg.RestBaseURL)
	}
	if cfg.WSPublicURL != "wss://fstream.binance.com/public/stream" {
		t.Errorf("expected mainnet public WS URL, got %s", cfg.WSPublicURL)
	}
}

func TestValidationErrors(t *testing.T) {
	// 1. Invalid DRY_RUN_MODE
	_ = os.Setenv("DRY_RUN_MODE", "invalid_typo")
	_, err := LoadConfig()
	if err == nil {
		t.Errorf("expected error for invalid DRY_RUN_MODE")
	}
	_ = os.Unsetenv("DRY_RUN_MODE")

	// 2. Invalid Drawdown (positive number)
	_ = os.Setenv("MAX_DAILY_DRAWDOWN_PCT", "0.05")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for positive drawdown")
	}
	_ = os.Unsetenv("MAX_DAILY_DRAWDOWN_PCT")

	// 3. Live mode without credentials
	_ = os.Setenv("DRY_RUN_MODE", "false")
	_ = os.Setenv("BINANCE_API_KEY", "")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for live mode with empty API key")
	}
	_ = os.Unsetenv("DRY_RUN_MODE")

	// 4. Invalid MIN_VOLUME_24H_USDT
	_ = os.Setenv("MIN_VOLUME_24H_USDT", "not_a_number")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for non-numeric MIN_VOLUME_24H_USDT")
	}
	_ = os.Unsetenv("MIN_VOLUME_24H_USDT")

	// 5. Invalid RECV_WINDOW_MS
	_ = os.Setenv("RECV_WINDOW_MS", "500") // < 1000ms
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for RECV_WINDOW_MS < 1000")
	}
	_ = os.Unsetenv("RECV_WINDOW_MS")
}

func TestMaxDailyDrawdownDryRunAndLiveClamping(t *testing.T) {
	_ = os.Setenv("BINANCE_API_KEY", "dummy_key")
	_ = os.Setenv("BINANCE_SECRET_KEY", "dummy_secret")
	defer func() {
		_ = os.Unsetenv("BINANCE_API_KEY")
		_ = os.Unsetenv("BINANCE_SECRET_KEY")
		_ = os.Unsetenv("DRY_RUN_MODE")
		_ = os.Unsetenv("MAX_DAILY_DRAWDOWN_PCT")
	}()

	// 1. Dry-Run mode: -1.0 is preserved
	_ = os.Setenv("DRY_RUN_MODE", "true")
	_ = os.Setenv("MAX_DAILY_DRAWDOWN_PCT", "-1.0")
	cfgDry, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error in dry run: %v", err)
	}
	if cfgDry.MaxDailyDrawdown != -1.0 {
		t.Errorf("expected -1.0 in dry-run, got %f", cfgDry.MaxDailyDrawdown)
	}

	// 2. Live mode: -1.0 is clamped to -0.05
	_ = os.Setenv("DRY_RUN_MODE", "false")
	_ = os.Setenv("MAX_DAILY_DRAWDOWN_PCT", "-1.0")
	cfgLive, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error in live mode: %v", err)
	}
	if cfgLive.MaxDailyDrawdown != -0.05 {
		t.Errorf("expected live mode to clamp to -0.05, got %f", cfgLive.MaxDailyDrawdown)
	}
}
