package config

import (
	"os"
	"testing"
	"time"
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
	if cfg.AllowedSide != "AUTO" {
		t.Errorf("expected default AllowedSide AUTO, got %s", cfg.AllowedSide)
	}
	if cfg.MaxFrenzyRVOL != 2.50 {
		t.Errorf("expected default MaxFrenzyRVOL 2.50, got %f", cfg.MaxFrenzyRVOL)
	}
	if cfg.MaxCounterImpulseATR != 1.50 {
		t.Errorf("expected default MaxCounterImpulseATR 1.50, got %f", cfg.MaxCounterImpulseATR)
	}
	if cfg.Tier2ProfitLockATR != 1.80 {
		t.Errorf("expected default Tier2ProfitLockATR 1.80, got %f", cfg.Tier2ProfitLockATR)
	}
	if cfg.Tier2ProfitLockOffsetATR != 0.90 {
		t.Errorf("expected default Tier2ProfitLockOffsetATR 0.90, got %f", cfg.Tier2ProfitLockOffsetATR)
	}
	if cfg.Tier1BEEnabled {
		t.Errorf("expected default Tier1BEEnabled false, got true")
	}
	if !cfg.TrailingEnabled {
		t.Errorf("expected default TrailingEnabled true, got false")
	}
	if cfg.TrailingActivateATR != 2.00 {
		t.Errorf("expected default TrailingActivateATR 2.00, got %f", cfg.TrailingActivateATR)
	}
	if cfg.TrailingDistanceATR != 0.80 {
		t.Errorf("expected default TrailingDistanceATR 0.80, got %f", cfg.TrailingDistanceATR)
	}
	if cfg.MaxHoldingDuration != 4*time.Hour {
		t.Errorf("expected default MaxHoldingDuration 4h, got %v", cfg.MaxHoldingDuration)
	}
	if cfg.MinNATRPct != 0.75 {
		t.Errorf("expected default MinNATRPct 0.75, got %f", cfg.MinNATRPct)
	}
	if cfg.MaxNATRPct != 1.20 {
		t.Errorf("expected default MaxNATRPct 1.20, got %f", cfg.MaxNATRPct)
	}
	if cfg.MaxScannerVWAPDistATR != 2.50 {
		t.Errorf("expected default MaxScannerVWAPDistATR 2.50, got %f", cfg.MaxScannerVWAPDistATR)
	}
	if cfg.MinEMA200DistanceATR != 0.15 {
		t.Errorf("expected default MinEMA200DistanceATR 0.15, got %f", cfg.MinEMA200DistanceATR)
	}
	if cfg.MaxEMA200DistanceATR != 3.20 {
		t.Errorf("expected default MaxEMA200DistanceATR 3.20, got %f", cfg.MaxEMA200DistanceATR)
	}
	if cfg.MinSignalRVOL != 0.0 {
		t.Errorf("expected default MinSignalRVOL 0.0, got %f", cfg.MinSignalRVOL)
	}
	if cfg.BTCCounterMomentumPct != 0.25 {
		t.Errorf("expected default BTCCounterMomentumPct 0.25, got %f", cfg.BTCCounterMomentumPct)
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

	// 6. Invalid ALLOWED_SIDE
	_ = os.Setenv("ALLOWED_SIDE", "SIDEWAYS")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for invalid ALLOWED_SIDE")
	}
	_ = os.Unsetenv("ALLOWED_SIDE")

	// 7. Invalid MAX_FRENZY_RVOL
	_ = os.Setenv("MAX_FRENZY_RVOL", "-1.0")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for negative MAX_FRENZY_RVOL")
	}
	_ = os.Unsetenv("MAX_FRENZY_RVOL")

	// 8. Invalid MAX_COUNTER_IMPULSE_ATR
	_ = os.Setenv("MAX_COUNTER_IMPULSE_ATR", "abc")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for non-numeric MAX_COUNTER_IMPULSE_ATR")
	}
	_ = os.Unsetenv("MAX_COUNTER_IMPULSE_ATR")
}

func TestAllowedSideAndFrenzyConfig(t *testing.T) {
	_ = os.Setenv("ALLOWED_SIDE", "short_only")
	_ = os.Setenv("MAX_FRENZY_RVOL", "3.25")
	_ = os.Setenv("MAX_COUNTER_IMPULSE_ATR", "1.75")
	defer func() {
		_ = os.Unsetenv("ALLOWED_SIDE")
		_ = os.Unsetenv("MAX_FRENZY_RVOL")
		_ = os.Unsetenv("MAX_COUNTER_IMPULSE_ATR")
	}()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AllowedSide != "SHORT_ONLY" {
		t.Errorf("expected SHORT_ONLY (uppercased), got %s", cfg.AllowedSide)
	}
	if cfg.MaxFrenzyRVOL != 3.25 {
		t.Errorf("expected 3.25, got %f", cfg.MaxFrenzyRVOL)
	}
	if cfg.MaxCounterImpulseATR != 1.75 {
		t.Errorf("expected 1.75, got %f", cfg.MaxCounterImpulseATR)
	}
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

func TestMaxHoldingDurationConfig(t *testing.T) {
	_ = os.Setenv("MAX_HOLDING_DURATION", "6h")
	defer func() { _ = os.Unsetenv("MAX_HOLDING_DURATION") }()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}
	if cfg.MaxHoldingDuration != 6*time.Hour {
		t.Errorf("expected 6h, got %v", cfg.MaxHoldingDuration)
	}
}

func TestInlineCommentSanitizationAndBoolParsing(t *testing.T) {
	_ = os.Setenv("TIER1_BE_ENABLED", "True # inline comment")
	_ = os.Setenv("MIN_VOLUME_24H_USDT", "40000000 # 40M volume")
	defer func() {
		_ = os.Unsetenv("TIER1_BE_ENABLED")
		_ = os.Unsetenv("MIN_VOLUME_24H_USDT")
	}()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config with inline comments: %v", err)
	}
	if !cfg.Tier1BEEnabled {
		t.Errorf("expected Tier1BEEnabled to be true despite comment and uppercase, got false")
	}
	if cfg.MinVolume24hUSDT != 40000000.0 {
		t.Errorf("expected MinVolume24hUSDT to be 40000000, got %f", cfg.MinVolume24hUSDT)
	}
}

func TestRunnerBECushionATRConfig(t *testing.T) {
	_ = os.Setenv("RUNNER_BE_CUSHION_ATR", "0.45")
	defer func() { _ = os.Unsetenv("RUNNER_BE_CUSHION_ATR") }()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}
	if cfg.RunnerBECushionATR != 0.45 {
		t.Errorf("expected RunnerBECushionATR to be 0.45, got %f", cfg.RunnerBECushionATR)
	}
}

func TestFeeAndSlippageValidation(t *testing.T) {
	_ = os.Setenv("MAKER_FEE_RATE", "-0.0005")
	_, err := LoadConfig()
	if err == nil {
		t.Fatalf("expected error for negative MAKER_FEE_RATE")
	}
	_ = os.Unsetenv("MAKER_FEE_RATE")

	_ = os.Setenv("TAKER_FEE_RATE", "0.10")
	_, err = LoadConfig()
	if err == nil {
		t.Fatalf("expected error for excessive TAKER_FEE_RATE")
	}
	_ = os.Unsetenv("TAKER_FEE_RATE")

	_ = os.Setenv("SLIPPAGE_RATE", "-0.001")
	_, err = LoadConfig()
	if err == nil {
		t.Fatalf("expected error for negative SLIPPAGE_RATE")
	}
	_ = os.Unsetenv("SLIPPAGE_RATE")
}

func TestNaNAndInfFloatValidation(t *testing.T) {
	nanFields := []string{
		"MAX_DAILY_DRAWDOWN_PCT",
		"ACCOUNT_LEVERAGE",
		"MIN_VOLUME_24H_USDT",
		"MAX_SPREAD_PCT",
		"MIN_NATR_PCT",
		"SL_ATR_MULTIPLIER",
		"TP_ATR_MULTIPLIER",
		"BREAK_EVEN_TRIGGER_ATR",
		"PARTIAL_TP1_RATIO",
		"BTC_MAX_5M_DUMP_PCT",
	}

	for _, field := range nanFields {
		for _, invalidVal := range []string{"NaN", "nan", "+Inf", "-Inf", "Inf"} {
			_ = os.Setenv(field, invalidVal)
			_, err := LoadConfig()
			if err == nil {
				t.Fatalf("expected error for field %s with value %s, but got nil", field, invalidVal)
			}
			_ = os.Unsetenv(field)
		}
	}
}

func TestTimeframeConfig(t *testing.T) {
	// 1. Default (5m)
	_ = os.Unsetenv("TIMEFRAME")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Timeframe != "5m" || cfg.CandleDuration != 5*time.Minute {
		t.Errorf("expected default 5m, got %s / %v", cfg.Timeframe, cfg.CandleDuration)
	}
	if cfg.MaxDataStaleness != 330*time.Second {
		t.Errorf("expected 330s MaxDataStaleness for 5m, got %v", cfg.MaxDataStaleness)
	}

	// 2. 15m
	_ = os.Setenv("TIMEFRAME", "15m")
	cfg15, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg15.Timeframe != "15m" || cfg15.CandleDuration != 15*time.Minute {
		t.Errorf("expected 15m, got %s / %v", cfg15.Timeframe, cfg15.CandleDuration)
	}
	if cfg15.MaxDataStaleness != (15*time.Minute + 30*time.Second) {
		t.Errorf("expected 930s MaxDataStaleness for 15m, got %v", cfg15.MaxDataStaleness)
	}

	// 3. Invalid
	_ = os.Setenv("TIMEFRAME", "7m")
	_, err = LoadConfig()
	if err == nil {
		t.Errorf("expected error for invalid timeframe 7m, got nil")
	}
	_ = os.Unsetenv("TIMEFRAME")
}

func TestScannerRankingModeConfig(t *testing.T) {
	_ = os.Unsetenv("SCANNER_RANKING_MODE")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ScannerRankingMode != "pullback" {
		t.Fatalf("expected default ScannerRankingMode='pullback', got %q", cfg.ScannerRankingMode)
	}

	_ = os.Setenv("SCANNER_RANKING_MODE", "RVOL # override")
	cfgRVOL, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error for rvol mode: %v", err)
	}
	if cfgRVOL.ScannerRankingMode != "rvol" {
		t.Fatalf("expected normalized 'rvol', got %q", cfgRVOL.ScannerRankingMode)
	}

	_ = os.Setenv("SCANNER_RANKING_MODE", "unknown")
	if _, err := LoadConfig(); err == nil {
		t.Fatalf("expected error for invalid SCANNER_RANKING_MODE")
	}
	_ = os.Unsetenv("SCANNER_RANKING_MODE")
}
