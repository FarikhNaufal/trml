package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type Environment string

const (
	EnvTestnet Environment = "testnet"
	EnvMainnet Environment = "mainnet"
)

type Config struct {
	Env              Environment
	APIKey           string
	SecretKey        string
	DryRun           bool
	DatabasePath     string
	LogLevel         string
	HTTPPort         string

	// URL Bursa
	RestBaseURL      string
	WSPublicURL      string
	WSMarketURL      string
	WSPrivateBaseURL string
	WSAPIOrderURL    string

	// Tipe Order Entry ("MARKET" atau "LIMIT")
	EntryOrderType   string

	// Parameter Biaya & Slippage
	MakerFeeRate     decimal.Decimal
	TakerFeeRate     decimal.Decimal
	SlippageRate     decimal.Decimal

	// Parameter Risiko Terkunci
	InitialEquity      decimal.Decimal
	RiskPerTradePct    decimal.Decimal
	MaxDailyDrawdown   float64
	MaxSlots           int
	MaxSlotMarginFrac  decimal.Decimal
	MaxMarginAvailFrac decimal.Decimal
	Leverage           float64

	// Parameter Scanner
	MinVolume24hUSDT float64
	MinFallbackRVOL  float64
	MaxSpreadPct     float64
	MinNATRPct            float64
	MaxNATRPct            float64
	MaxScannerVWAPDistATR float64
	MaxWatchedCoins       int

	// Parameter Strategi VWAP Pullback / Retest with Orderflow Bounce
	PullbackToleranceATR float64
	MinBounceZScore      float64
	MinEMA200DistanceATR float64
	LookbackImpulseBars  int
	RetestLookbackBars   int
	MinImpulseATR        float64
	MaxEntryDistanceATR  float64
	MinOIDeltaPct        float64
	MinOIDeltaLongPct    float64
	MinOIDeltaShortPct   float64
	SLATRMultiplier          float64
	TPATRMultiplier          float64
	BreakEvenTriggerATR      float64
	Tier1BEEnabled           bool
	Tier1BETriggerLongATR    float64
	Tier1BETriggerShortATR   float64
	Tier2ProfitLockATR       float64
	Tier2ProfitLockOffsetATR float64
	MinBounceZScoreLong      float64
	MinBounceZScoreShort     float64
	MinSLDistancePct         decimal.Decimal
	PartialTPEnabled         bool
	PartialTP1Ratio          float64
	PartialTP1ATRMult        float64
	TrailingEnabled          bool
	TrailingActivateATR      float64
	TrailingDistanceATR      float64
	BTCGateEnabled           bool
	BTCMax5mDumpPct          float64
	BTCMax5mPumpPct          float64
	BTCGatePersistenceBars   int
	BTCCounterMomentumPct    float64
	AllowedSide              string
	MaxFrenzyRVOL            float64
	MaxCounterImpulseATR     float64
	MinSignalRVOL            float64
	CooldownAfterExitMinutes int
	CooldownLossMinutes      int
	CooldownProfitMinutes    int
	VWAPSessionWarmupMinutes int

	// Batas Waktu & Staleness
	ScannerInterval   time.Duration
	MaxBookStaleness  time.Duration
	MaxMarkStaleness  time.Duration
	MaxDataStaleness  time.Duration
	MaxAccountStale   time.Duration
	MinOIInterval     time.Duration
	MaxOIInterval     time.Duration
	MaxOIStaleness    time.Duration
	LimitOrderTimeout  time.Duration
	MaxHoldingDuration time.Duration
	RecvWindowMS       int64

	// Keamanan Dashboard & AI Copilot
	DashboardPIN     string
	GeminiAPIKey     string
	GeminiModel      string
	GeminiTimeoutSec int
	AIRateLimitRPM   int
}

// loadDotEnv membaca file .env secara otomatis ke environment variable proses
func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		return
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		// [H-06] Bersihkan komentar inline (#) jika nilai tidak diapit kutip ganda/tunggal
		if strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`) && len(val) >= 2 {
			val = val[1 : len(val)-1]
		} else if strings.HasPrefix(val, `'`) && strings.HasSuffix(val, `'`) && len(val) >= 2 {
			val = val[1 : len(val)-1]
		} else {
			if idx := strings.Index(val, "#"); idx != -1 {
				val = strings.TrimSpace(val[:idx])
			}
			val = strings.Trim(val, `"'`+"`")
		}

		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
}

func LoadConfig() (*Config, error) {
	loadDotEnv(".env")

	envStr := strings.ToLower(getEnv("BINANCE_ENV", "testnet"))
	var env Environment
	var restURL, wsPublic, wsMarket, wsPrivate, wsAPI string

	switch envStr {
	case "mainnet", "prod", "production":
		env = EnvMainnet
		restURL = "https://fapi.binance.com"
		wsPublic = "wss://fstream.binance.com/public/stream"
		wsMarket = "wss://fstream.binance.com/market/stream"
		wsPrivate = "wss://fstream.binance.com/private/ws"
		wsAPI = "wss://ws-fapi.binance.com/ws-fapi/v1"
	default:
		env = EnvTestnet
		restURL = "https://testnet.binancefuture.com"
		wsPublic = "wss://stream.binancefuture.com/stream"
		wsMarket = "wss://stream.binancefuture.com/stream"
		wsPrivate = "wss://stream.binancefuture.com/ws"
		wsAPI = ""
	}

	// C4 Fix: Strict validation untuk DRY_RUN_MODE (no silent live execution)
	dryRunStr := strings.TrimSpace(strings.ToLower(getEnv("DRY_RUN_MODE", "true")))
	var dryRun bool
	switch dryRunStr {
	case "true", "1", "yes":
		dryRun = true
	case "false", "0", "no":
		dryRun = false
	default:
		return nil, fmt.Errorf("config: invalid DRY_RUN_MODE '%s' (must be 'true' or 'false')", dryRunStr)
	}

	apiKey := strings.TrimSpace(getEnv("BINANCE_API_KEY", ""))
	secretKey := strings.TrimSpace(getEnv("BINANCE_SECRET_KEY", ""))

	// Credential Guard: Jika mode Live aktif, wajib ada API Key & Secret Key non-placeholder
	if !dryRun {
		if apiKey == "" || secretKey == "" || strings.Contains(apiKey, "your_binance_api_key") {
			return nil, fmt.Errorf("config CRITICAL: DRY_RUN_MODE=false (live money) requires valid BINANCE_API_KEY and BINANCE_SECRET_KEY")
		}
	}

	dbPath := getEnv("SQLITE_DB_PATH", "./data/trml.db")
	logLevel := strings.ToUpper(getEnv("LOG_LEVEL", "INFO"))

	makerFee, err := decimal.NewFromString(getEnv("MAKER_FEE_RATE", "0.0002"))
	if err != nil {
		return nil, fmt.Errorf("config: invalid MAKER_FEE_RATE: %w", err)
	}
	takerFee, err := decimal.NewFromString(getEnv("TAKER_FEE_RATE", "0.0005"))
	if err != nil {
		return nil, fmt.Errorf("config: invalid TAKER_FEE_RATE: %w", err)
	}
	slippage, err := decimal.NewFromString(getEnv("SLIPPAGE_RATE", "0.0002"))
	if err != nil {
		return nil, fmt.Errorf("config: invalid SLIPPAGE_RATE: %w", err)
	}

	initEqStr := getEnv("INITIAL_EQUITY", "1000.0")
	initEq, err := decimal.NewFromString(initEqStr)
	if err != nil || initEq.LessThanOrEqual(decimal.Zero) {
		initEq = decimal.NewFromFloat(1000.0)
	}

	riskPct, err := decimal.NewFromString(getEnv("RISK_PER_TRADE_PCT", "0.01"))
	if err != nil || riskPct.LessThanOrEqual(decimal.Zero) || riskPct.GreaterThan(decimal.NewFromFloat(0.05)) {
		return nil, fmt.Errorf("config: invalid RISK_PER_TRADE_PCT '%s' (must be between 0.001 and 0.05)", getEnv("RISK_PER_TRADE_PCT", "0.01"))
	}

	marginFrac, err := decimal.NewFromString(getEnv("MAX_SLOT_MARGIN_FRACTION", "0.30"))
	if err != nil || marginFrac.LessThanOrEqual(decimal.Zero) || marginFrac.GreaterThan(decimal.NewFromFloat(0.50)) {
		return nil, fmt.Errorf("config: invalid MAX_SLOT_MARGIN_FRACTION '%s' (must be between 0.05 and 0.50)", getEnv("MAX_SLOT_MARGIN_FRACTION", "0.30"))
	}

	// C5 Fix: Validasi numerik drawdown, slots, leverage
	ddStr := getEnv("MAX_DAILY_DRAWDOWN_PCT", "-0.03")
	ddFloat, err := strconv.ParseFloat(ddStr, 64)
	if err != nil || ddFloat >= 0 || ddFloat < -1.00 {
		return nil, fmt.Errorf("config: invalid MAX_DAILY_DRAWDOWN_PCT '%s' (must be negative float between -0.01 and -1.00)", ddStr)
	}

	// Live safety guard: jika mode live (dryRun=false) dan drawdown di-set lebih longgar dari -0.05,
	// paksa batas pengaman live ke -0.05 (-5.0%) untuk mencegah kerugian fatal uang riil.
	if !dryRun && ddFloat < -0.05 {
		slog.Warn("live mode active: clamping MAX_DAILY_DRAWDOWN_PCT to safe live limit of -0.05 (-5%)", "configured", ddFloat)
		ddFloat = -0.05
	}

	slotsStr := getEnv("MAX_CONCURRENT_SLOTS", "3")
	slots, err := strconv.Atoi(slotsStr)
	if err != nil || slots < 1 || slots > 10 {
		return nil, fmt.Errorf("config: invalid MAX_CONCURRENT_SLOTS '%s' (must be integer between 1 and 10)", slotsStr)
	}

	levStr := getEnv("ACCOUNT_LEVERAGE", "5.0")
	lev, err := strconv.ParseFloat(levStr, 64)
	if err != nil || lev < 1.0 || lev > 20.0 {
		return nil, fmt.Errorf("config: invalid ACCOUNT_LEVERAGE '%s' (must be float between 1.0 and 20.0)", levStr)
	}

	entryOrderType := strings.ToUpper(strings.TrimSpace(getEnv("ENTRY_ORDER_TYPE", "MARKET")))
	if entryOrderType != "MARKET" && entryOrderType != "LIMIT" {
		return nil, fmt.Errorf("config: invalid ENTRY_ORDER_TYPE '%s' (must be 'MARKET' or 'LIMIT')", entryOrderType)
	}

	defaultMinVol := 35_000_000.0
	if env == EnvTestnet {
		defaultMinVol = 0.0
	}
	minVolStr := getEnv("MIN_VOLUME_24H_USDT", strconv.FormatFloat(defaultMinVol, 'f', 1, 64))
	minVol, err := strconv.ParseFloat(minVolStr, 64)
	if err != nil || minVol < 0.0 {
		return nil, fmt.Errorf("config: invalid MIN_VOLUME_24H_USDT '%s' (must be a non-negative float)", minVolStr)
	}

	recvWinStr := getEnv("RECV_WINDOW_MS", "5000")
	recvWin, err := strconv.ParseInt(recvWinStr, 10, 64)
	if err != nil || recvWin < 1000 || recvWin > 60000 {
		return nil, fmt.Errorf("config: invalid RECV_WINDOW_MS '%s' (must be integer between 1000 and 60000)", recvWinStr)
	}

	pullbackTolStr := getEnv("PULLBACK_TOLERANCE_ATR", "0.35")
	pullbackTol, err := strconv.ParseFloat(pullbackTolStr, 64)
	if err != nil || pullbackTol <= 0 || pullbackTol > 2.0 {
		return nil, fmt.Errorf("config: invalid PULLBACK_TOLERANCE_ATR '%s' (must be float between 0.05 and 2.0)", pullbackTolStr)
	}

	minBounceZStr := getEnv("MIN_BOUNCE_ZSCORE", "0.40")
	minBounceZ, err := strconv.ParseFloat(minBounceZStr, 64)
	if err != nil || minBounceZ <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_BOUNCE_ZSCORE '%s' (must be positive float)", minBounceZStr)
	}

	minEMA200DistStr := getEnv("MIN_EMA200_DISTANCE_ATR", "0.15")
	minEMA200Dist, err := strconv.ParseFloat(minEMA200DistStr, 64)
	if err != nil || minEMA200Dist < 0 || minEMA200Dist > 5.0 {
		return nil, fmt.Errorf("config: invalid MIN_EMA200_DISTANCE_ATR '%s' (must be float between 0.0 and 5.0)", minEMA200DistStr)
	}

	minBounceZLongStr := getEnv("MIN_BOUNCE_ZSCORE_LONG", "0.40")
	minBounceZLong, err := strconv.ParseFloat(minBounceZLongStr, 64)
	if err != nil || minBounceZLong <= 0 {
		minBounceZLong = minBounceZ
	}

	minBounceZShortStr := getEnv("MIN_BOUNCE_ZSCORE_SHORT", minBounceZStr)
	minBounceZShort, err := strconv.ParseFloat(minBounceZShortStr, 64)
	if err != nil || minBounceZShort <= 0 {
		minBounceZShort = minBounceZ
	}

	lookbackBarsStr := getEnv("LOOKBACK_IMPULSE_BARS", "18")
	lookbackBars, err := strconv.Atoi(lookbackBarsStr)
	if err != nil || lookbackBars < 2 || lookbackBars > 100 {
		return nil, fmt.Errorf("config: invalid LOOKBACK_IMPULSE_BARS '%s' (must be integer between 2 and 100)", lookbackBarsStr)
	}

	retestLookbackBarsStr := getEnv("RETEST_LOOKBACK_BARS", "4")
	retestLookbackBars, err := strconv.Atoi(retestLookbackBarsStr)
	if err != nil || retestLookbackBars < 1 || retestLookbackBars > 20 {
		return nil, fmt.Errorf("config: invalid RETEST_LOOKBACK_BARS '%s' (must be integer between 1 and 20)", retestLookbackBarsStr)
	}

	minImpulseATRStr := getEnv("MIN_IMPULSE_ATR", "0.6")
	minImpulseATR, err := strconv.ParseFloat(minImpulseATRStr, 64)
	if err != nil || minImpulseATR <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_IMPULSE_ATR '%s' (must be positive float)", minImpulseATRStr)
	}

	maxEntryDistStr := getEnv("MAX_ENTRY_DISTANCE_ATR", "1.20")
	maxEntryDist, err := strconv.ParseFloat(maxEntryDistStr, 64)
	if err != nil || maxEntryDist <= 0 || maxEntryDist > 5.0 {
		return nil, fmt.Errorf("config: invalid MAX_ENTRY_DISTANCE_ATR '%s' (must be float between 0.1 and 5.0)", maxEntryDistStr)
	}

	availMarginFrac, err := decimal.NewFromString(getEnv("MAX_MARGIN_AVAIL_FRAC", "0.90"))
	if err != nil || availMarginFrac.LessThanOrEqual(decimal.Zero) || availMarginFrac.GreaterThan(decimal.NewFromFloat(1.0)) {
		return nil, fmt.Errorf("config: invalid MAX_MARGIN_AVAIL_FRAC '%s' (must be between 0.10 and 1.00)", getEnv("MAX_MARGIN_AVAIL_FRAC", "0.90"))
	}

	minFallbackRVOLStr := getEnv("MIN_FALLBACK_RVOL", "0.60")
	minFallbackRVOL, err := strconv.ParseFloat(minFallbackRVOLStr, 64)
	if err != nil || minFallbackRVOL <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_FALLBACK_RVOL '%s' (must be positive float)", minFallbackRVOLStr)
	}

	maxSpreadStr := getEnv("MAX_SPREAD_PCT", "0.06")
	maxSpread, err := strconv.ParseFloat(maxSpreadStr, 64)
	if err != nil || maxSpread <= 0 || maxSpread > 1.0 {
		return nil, fmt.Errorf("config: invalid MAX_SPREAD_PCT '%s' (must be float between 0.001 and 1.0)", maxSpreadStr)
	}

	minNATRStr := getEnv("MIN_NATR_PCT", "0.75")
	minNATR, err := strconv.ParseFloat(minNATRStr, 64)
	if err != nil || minNATR <= 0 || minNATR > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_NATR_PCT '%s' (must be float between 0.01 and 10.0)", minNATRStr)
	}

	maxNATRStr := getEnv("MAX_NATR_PCT", "2.50")
	maxNATR, err := strconv.ParseFloat(maxNATRStr, 64)
	if err != nil || maxNATR < minNATR || maxNATR > 20.0 {
		return nil, fmt.Errorf("config: invalid MAX_NATR_PCT '%s' (must be float >= MIN_NATR_PCT and <= 20.0)", maxNATRStr)
	}

	maxScannerVWAPDistStr := getEnv("MAX_SCANNER_VWAP_DIST_ATR", "2.50")
	maxScannerVWAPDist, err := strconv.ParseFloat(maxScannerVWAPDistStr, 64)
	if err != nil || maxScannerVWAPDist < 0 || maxScannerVWAPDist > 20.0 {
		return nil, fmt.Errorf("config: invalid MAX_SCANNER_VWAP_DIST_ATR '%s' (must be float between 0.0 and 20.0)", maxScannerVWAPDistStr)
	}

	maxWatchedStr := getEnv("MAX_WATCHED_COINS", "20")
	maxWatched, err := strconv.Atoi(maxWatchedStr)
	if err != nil || maxWatched < 1 || maxWatched > 50 {
		return nil, fmt.Errorf("config: invalid MAX_WATCHED_COINS '%s' (must be integer between 1 and 50)", maxWatchedStr)
	}

	minOIDeltaStr := getEnv("MIN_OI_DELTA_PCT", "0.00")
	minOIDelta, err := strconv.ParseFloat(minOIDeltaStr, 64)
	if err != nil || minOIDelta < -5.0 || minOIDelta > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_OI_DELTA_PCT '%s' (must be float between -5.0 and 10.0)", minOIDeltaStr)
	}

	minOIDeltaLongStr := getEnv("MIN_OI_DELTA_LONG_PCT", "0.00")
	minOIDeltaLong, err := strconv.ParseFloat(minOIDeltaLongStr, 64)
	if err != nil || minOIDeltaLong < -5.0 || minOIDeltaLong > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_OI_DELTA_LONG_PCT '%s' (must be float between -5.0 and 10.0)", minOIDeltaLongStr)
	}

	minOIDeltaShortStr := getEnv("MIN_OI_DELTA_SHORT_PCT", "-0.02")
	minOIDeltaShort, err := strconv.ParseFloat(minOIDeltaShortStr, 64)
	if err != nil || minOIDeltaShort < -5.0 || minOIDeltaShort > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_OI_DELTA_SHORT_PCT '%s' (must be float between -5.0 and 10.0)", minOIDeltaShortStr)
	}

	slMultStr := getEnv("SL_ATR_MULTIPLIER", "1.5")
	slMult, err := strconv.ParseFloat(slMultStr, 64)
	if err != nil || slMult <= 0.1 || slMult > 10.0 {
		return nil, fmt.Errorf("config: invalid SL_ATR_MULTIPLIER '%s' (must be float between 0.1 and 10.0)", slMultStr)
	}

	tpMultStr := getEnv("TP_ATR_MULTIPLIER", "2.25")
	tpMult, err := strconv.ParseFloat(tpMultStr, 64)
	if err != nil || tpMult <= 0.1 || tpMult > 20.0 {
		return nil, fmt.Errorf("config: invalid TP_ATR_MULTIPLIER '%s' (must be float between 0.1 and 20.0)", tpMultStr)
	}

	beTriggerStr := getEnv("BREAK_EVEN_TRIGGER_ATR", "1.15")
	beTrigger, err := strconv.ParseFloat(beTriggerStr, 64)
	if err != nil || beTrigger <= 0.1 || beTrigger > 10.0 {
		return nil, fmt.Errorf("config: invalid BREAK_EVEN_TRIGGER_ATR '%s' (must be float between 0.1 and 10.0)", beTriggerStr)
	}

	tier1BEEnabled := parseBoolEnv(getEnv("TIER1_BE_ENABLED", "false"))
	t1BETriggerLongStr := getEnv("TIER1_BE_TRIGGER_LONG_ATR", "0.70")
	t1BETriggerLong, err := strconv.ParseFloat(t1BETriggerLongStr, 64)
	if err != nil || t1BETriggerLong <= 0.05 || t1BETriggerLong > 10.0 {
		t1BETriggerLong = 0.70
	}

	t1BETriggerShortStr := getEnv("TIER1_BE_TRIGGER_SHORT_ATR", "0.90")
	t1BETriggerShort, err := strconv.ParseFloat(t1BETriggerShortStr, 64)
	if err != nil || t1BETriggerShort <= 0.05 || t1BETriggerShort > 10.0 {
		t1BETriggerShort = 0.90
	}

	t2ProfitLockStr := getEnv("TIER2_PROFIT_LOCK_ATR", "1.80")
	t2ProfitLock, err := strconv.ParseFloat(t2ProfitLockStr, 64)
	if err != nil || t2ProfitLock <= 0.1 || t2ProfitLock > 10.0 {
		t2ProfitLock = 1.80
	}

	t2OffsetStr := getEnv("TIER2_PROFIT_LOCK_OFFSET_ATR", "0.90")
	t2Offset, err := strconv.ParseFloat(t2OffsetStr, 64)
	if err != nil || t2Offset <= 0.05 || t2Offset > 10.0 {
		t2Offset = 0.90
	}

	minSLDistPctStr := getEnv("MIN_SL_DISTANCE_PCT", "0.0150")
	minSLDistPct, err := decimal.NewFromString(minSLDistPctStr)
	if err != nil || minSLDistPct.LessThan(decimal.Zero) || minSLDistPct.GreaterThan(decimal.NewFromFloat(0.20)) {
		return nil, fmt.Errorf("config: invalid MIN_SL_DISTANCE_PCT '%s' (must be between 0.00 and 0.20)", minSLDistPctStr)
	}

	partialTPEnabled := parseBoolEnv(getEnv("PARTIAL_TP_ENABLED", "true"))

	partialTP1RatioStr := getEnv("PARTIAL_TP1_RATIO", "0.50")
	partialTP1Ratio, err := strconv.ParseFloat(partialTP1RatioStr, 64)
	if err != nil || partialTP1Ratio <= 0.05 || partialTP1Ratio >= 0.95 {
		return nil, fmt.Errorf("config: invalid PARTIAL_TP1_RATIO '%s' (must be float between 0.05 and 0.95)", partialTP1RatioStr)
	}

	partialTP1ATRMultStr := getEnv("PARTIAL_TP1_ATR_MULT", "1.20")
	partialTP1ATRMult, err := strconv.ParseFloat(partialTP1ATRMultStr, 64)
	if err != nil || partialTP1ATRMult <= 0.1 || partialTP1ATRMult > 10.0 {
		return nil, fmt.Errorf("config: invalid PARTIAL_TP1_ATR_MULT '%s' (must be float between 0.1 and 10.0)", partialTP1ATRMultStr)
	}

	trailingEnabled := parseBoolEnv(getEnv("TRAILING_ENABLED", "true"))

	trailingActivateATRStr := getEnv("TRAILING_ACTIVATE_ATR", "2.00")
	trailingActivateATR, err := strconv.ParseFloat(trailingActivateATRStr, 64)
	if err != nil || trailingActivateATR <= 0.1 || trailingActivateATR > 10.0 {
		trailingActivateATR = 2.00
	}

	trailingDistATRStr := getEnv("TRAILING_DISTANCE_ATR", "0.80")
	trailingDistATR, err := strconv.ParseFloat(trailingDistATRStr, 64)
	if err != nil || trailingDistATR <= 0.05 || trailingDistATR > 10.0 {
		trailingDistATR = 0.80
	}

	btcGateEnabledStr := getEnv("BTC_GATE_ENABLED", "true")
	btcGateEnabled := parseBoolEnv(btcGateEnabledStr)

	btcMaxDumpStr := getEnv("BTC_MAX_5M_DUMP_PCT", "0.60")
	btcMaxDump, err := strconv.ParseFloat(btcMaxDumpStr, 64)
	if err != nil || btcMaxDump <= 0.05 || btcMaxDump > 10.0 {
		return nil, fmt.Errorf("config: invalid BTC_MAX_5M_DUMP_PCT '%s' (must be positive float between 0.05 and 10.0)", btcMaxDumpStr)
	}

	btcMaxPumpStr := getEnv("BTC_MAX_5M_PUMP_PCT", "0.60")
	btcMaxPump, err := strconv.ParseFloat(btcMaxPumpStr, 64)
	if err != nil || btcMaxPump <= 0.05 || btcMaxPump > 10.0 {
		return nil, fmt.Errorf("config: invalid BTC_MAX_5M_PUMP_PCT '%s' (must be positive float between 0.05 and 10.0)", btcMaxPumpStr)
	}

	btcPersistenceBarsStr := getEnv("BTC_GATE_PERSISTENCE_BARS", "1")
	btcPersistenceBars, err := strconv.Atoi(btcPersistenceBarsStr)
	if err != nil || btcPersistenceBars < 1 || btcPersistenceBars > 20 {
		btcPersistenceBars = 1
	}

	allowedSide := strings.ToUpper(strings.TrimSpace(getEnv("ALLOWED_SIDE", "AUTO")))
	if allowedSide != "AUTO" && allowedSide != "SHORT_ONLY" && allowedSide != "LONG_ONLY" && allowedSide != "BOTH" {
		return nil, fmt.Errorf("config: invalid ALLOWED_SIDE '%s' (must be 'AUTO', 'SHORT_ONLY', 'LONG_ONLY', or 'BOTH')", allowedSide)
	}

	maxFrenzyRVOLStr := getEnv("MAX_FRENZY_RVOL", "2.50")
	maxFrenzyRVOL, err := strconv.ParseFloat(maxFrenzyRVOLStr, 64)
	if err != nil || maxFrenzyRVOL <= 0 {
		return nil, fmt.Errorf("config: invalid MAX_FRENZY_RVOL '%s' (must be positive float)", maxFrenzyRVOLStr)
	}

	maxCounterImpulseATRStr := getEnv("MAX_COUNTER_IMPULSE_ATR", "1.50")
	maxCounterImpulseATR, err := strconv.ParseFloat(maxCounterImpulseATRStr, 64)
	if err != nil || maxCounterImpulseATR <= 0 {
		return nil, fmt.Errorf("config: invalid MAX_COUNTER_IMPULSE_ATR '%s' (must be positive float)", maxCounterImpulseATRStr)
	}

	btcCounterMomStr := getEnv("BTC_COUNTER_MOMENTUM_PCT", "0.25")
	btcCounterMom, err := strconv.ParseFloat(btcCounterMomStr, 64)
	if err != nil || btcCounterMom < 0.0 || btcCounterMom > 5.0 {
		return nil, fmt.Errorf("config: invalid BTC_COUNTER_MOMENTUM_PCT '%s' (must be float between 0.0 and 5.0)", btcCounterMomStr)
	}

	minSignalRVOLStr := getEnv("MIN_SIGNAL_RVOL", "0.75")
	minSignalRVOL, err := strconv.ParseFloat(minSignalRVOLStr, 64)
	if err != nil || minSignalRVOL < 0.0 || minSignalRVOL > 5.0 {
		return nil, fmt.Errorf("config: invalid MIN_SIGNAL_RVOL '%s' (must be float between 0.0 and 5.0)", minSignalRVOLStr)
	}

	httpPort := getEnv("HTTP_PORT", "2020")
	dashPIN := strings.TrimSpace(getEnv("DASHBOARD_PIN", ""))
	geminiKey := strings.TrimSpace(getEnv("GEMINI_API_KEY", ""))
	geminiModel := strings.TrimSpace(getEnv("GEMINI_MODEL", "gemini-2.5-flash"))

	geminiTimeoutStr := getEnv("GEMINI_TIMEOUT_SEC", "25")
	geminiTimeout, err := strconv.Atoi(geminiTimeoutStr)
	if err != nil || geminiTimeout <= 0 {
		geminiTimeout = 25
	}

	aiRateLimitStr := getEnv("AI_RATE_LIMIT_RPM", "10")
	aiRateLimit, err := strconv.Atoi(aiRateLimitStr)
	if err != nil || aiRateLimit <= 0 {
		aiRateLimit = 10
	}

	maxHoldingDurationStr := getEnv("MAX_HOLDING_DURATION", "4h")
	maxHoldingDuration, err := time.ParseDuration(maxHoldingDurationStr)
	if err != nil || maxHoldingDuration < 15*time.Minute {
		maxHoldingDuration = 4 * time.Hour
	}

	cooldownMinsStr := getEnv("COOLDOWN_AFTER_EXIT_MINUTES", "20")
	cooldownMins, err := strconv.Atoi(cooldownMinsStr)
	if err != nil || cooldownMins < 0 {
		cooldownMins = 20
	}

	cooldownLossStr := getEnv("COOLDOWN_AFTER_LOSS_MINUTES", strconv.Itoa(cooldownMins))
	cooldownLossMins, err := strconv.Atoi(cooldownLossStr)
	if err != nil || cooldownLossMins < 0 {
		cooldownLossMins = cooldownMins
	}

	cooldownProfitStr := getEnv("COOLDOWN_AFTER_WIN_MINUTES", "10")
	cooldownProfitMins, err := strconv.Atoi(cooldownProfitStr)
	if err != nil || cooldownProfitMins < 0 {
		cooldownProfitMins = 10
	}

	vwapWarmupMinsStr := getEnv("VWAP_SESSION_WARMUP_MINUTES", "30")
	vwapWarmupMins, err := strconv.Atoi(vwapWarmupMinsStr)
	if err != nil || vwapWarmupMins < 0 {
		vwapWarmupMins = 30
	}

	return &Config{
		Env:                  env,
		APIKey:               apiKey,
		SecretKey:            secretKey,
		DryRun:               dryRun,
		DatabasePath:         dbPath,
		LogLevel:             logLevel,
		HTTPPort:             httpPort,
		EntryOrderType:       entryOrderType,
		RestBaseURL:          restURL,
		WSPublicURL:          wsPublic,
		WSMarketURL:          wsMarket,
		WSPrivateBaseURL:     wsPrivate,
		WSAPIOrderURL:        wsAPI,
		MakerFeeRate:         makerFee,
		TakerFeeRate:         takerFee,
		SlippageRate:         slippage,
		InitialEquity:        initEq,
		RiskPerTradePct:      riskPct,
		MaxDailyDrawdown:     ddFloat,
		MaxSlots:             slots,
		MaxSlotMarginFrac:    marginFrac,
		MaxMarginAvailFrac:   availMarginFrac,
		Leverage:             lev,
		MinVolume24hUSDT:     minVol,
		MinFallbackRVOL:      minFallbackRVOL,
		MaxSpreadPct:         maxSpread,
		MinNATRPct:               minNATR,
		MaxNATRPct:               maxNATR,
		MaxScannerVWAPDistATR:    maxScannerVWAPDist,
		MaxWatchedCoins:          maxWatched,
		PullbackToleranceATR:     pullbackTol,
		MinBounceZScore:          minBounceZ,
		MinEMA200DistanceATR:     minEMA200Dist,
		MinBounceZScoreLong:      minBounceZLong,
		MinBounceZScoreShort:     minBounceZShort,
		LookbackImpulseBars:      lookbackBars,
		RetestLookbackBars:       retestLookbackBars,
		MinImpulseATR:            minImpulseATR,
		MaxEntryDistanceATR:      maxEntryDist,
		MinOIDeltaPct:            minOIDelta,
		MinOIDeltaLongPct:        minOIDeltaLong,
		MinOIDeltaShortPct:       minOIDeltaShort,
		SLATRMultiplier:          slMult,
		TPATRMultiplier:          tpMult,
		BreakEvenTriggerATR:      beTrigger,
		Tier1BEEnabled:           tier1BEEnabled,
		Tier1BETriggerLongATR:    t1BETriggerLong,
		Tier1BETriggerShortATR:   t1BETriggerShort,
		Tier2ProfitLockATR:       t2ProfitLock,
		Tier2ProfitLockOffsetATR: t2Offset,
		MinSLDistancePct:         minSLDistPct,
		PartialTPEnabled:         partialTPEnabled,
		PartialTP1Ratio:          partialTP1Ratio,
		PartialTP1ATRMult:        partialTP1ATRMult,
		TrailingEnabled:          trailingEnabled,
		TrailingActivateATR:      trailingActivateATR,
		TrailingDistanceATR:      trailingDistATR,
		BTCGateEnabled:           btcGateEnabled,
		BTCMax5mDumpPct:          btcMaxDump,
		BTCMax5mPumpPct:          btcMaxPump,
		BTCGatePersistenceBars:   btcPersistenceBars,
		BTCCounterMomentumPct:    btcCounterMom,
		AllowedSide:              allowedSide,
		MaxFrenzyRVOL:            maxFrenzyRVOL,
		MaxCounterImpulseATR:     maxCounterImpulseATR,
		MinSignalRVOL:            minSignalRVOL,
		CooldownAfterExitMinutes: cooldownMins,
		CooldownLossMinutes:      cooldownLossMins,
		CooldownProfitMinutes:    cooldownProfitMins,
		VWAPSessionWarmupMinutes: vwapWarmupMins,

		ScannerInterval:    15 * time.Minute,
		MaxBookStaleness:   5 * time.Second,
		MaxMarkStaleness:   15 * time.Second,
		MaxDataStaleness:   330 * time.Second,
		MaxAccountStale:    60 * time.Second,
		MinOIInterval:      1 * time.Minute,
		MaxOIInterval:      8 * time.Minute,
		MaxOIStaleness:     330 * time.Second,
		LimitOrderTimeout:  45 * time.Second,
		MaxHoldingDuration: maxHoldingDuration,
		RecvWindowMS:       recvWin,

		DashboardPIN:     dashPIN,
		GeminiAPIKey:     geminiKey,
		GeminiModel:      geminiModel,
		GeminiTimeoutSec: geminiTimeout,
		AIRateLimitRPM:   aiRateLimit,
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		val = strings.TrimSpace(val)
		if idx := strings.Index(val, "#"); idx != -1 {
			val = strings.TrimSpace(val[:idx])
		}
		if val != "" {
			return val
		}
	}
	return defaultVal
}

func parseBoolEnv(val string) bool {
	clean := strings.ToLower(strings.TrimSpace(val))
	return clean == "true" || clean == "1" || clean == "yes" || clean == "y"
}
