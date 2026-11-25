package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"math"
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
	Env          Environment
	APIKey       string
	SecretKey    string
	DryRun       bool
	DatabasePath string
	LogLevel     string
	HTTPPort     string

	// URL Bursa
	RestBaseURL      string
	WSPublicURL      string
	WSMarketURL      string
	WSPrivateBaseURL string
	WSAPIOrderURL    string

	// Tipe Order Entry ("MARKET" atau "LIMIT")
	EntryOrderType string

	// Parameter Biaya & Slippage
	MakerFeeRate decimal.Decimal
	TakerFeeRate decimal.Decimal
	SlippageRate decimal.Decimal

	// Parameter Risiko Terkunci
	InitialEquity         decimal.Decimal
	RiskPerTradePct       decimal.Decimal
	MaxDailyDrawdown      float64
	MaxSlots              int
	MaxSlotMarginFrac     decimal.Decimal
	MaxMarginAvailFrac    decimal.Decimal
	Leverage              float64
	EquityModeCompounding bool

	// Parameter Scanner
	MinVolume24hUSDT      float64
	MinFallbackRVOL       float64
	MaxSpreadPct          float64
	MinNATRPct            float64
	MaxNATRPct            float64
	MaxScannerVWAPDistATR float64
	MaxWatchedCoins       int
	MinRVOL               float64
	ScannerRankingMode    string

	// Parameter Strategi VWAP Pullback / Retest with Orderflow Bounce
	EMAPeriod                int
	PullbackToleranceATR     float64
	MinBounceZScore          float64
	MinEMA200DistanceATR     float64
	MaxEMA200DistanceATR     float64
	LookbackImpulseBars      int
	RetestLookbackBars       int
	MinImpulseATR            float64
	MaxEntryDistanceATR      float64
	MinOIDeltaPct            float64
	MinOIDeltaLongPct        float64
	MinOIDeltaShortPct       float64
	DisableOIFilter          bool
	SLATRMultiplier          float64
	TPATRMultiplier          float64
	BreakEvenTriggerATR      float64
	Tier1BEEnabled           bool
	Tier1BETriggerLongATR    float64
	Tier1BETriggerShortATR   float64
	Tier2ProfitLockEnabled   bool
	Tier2ProfitLockATR       float64
	Tier2ProfitLockOffsetATR float64
	MinBounceZScoreLong      float64
	MinBounceZScoreShort     float64
	MinSLDistancePct         decimal.Decimal
	PartialTPEnabled         bool
	PartialTP1Ratio          float64
	PartialTP1ATRMult        float64
	RunnerBECushionATR       float64
	TrailingEnabled          bool
	TrailingActivateATR      float64
	TrailingDistanceATR      float64
	BTCGateEnabled           bool
	BTCMax5mDumpPct          float64
	BTCMax5mPumpPct          float64
	BTCGatePersistenceBars   int
	BTCCounterMomentumPct    float64
	BTCChopRiskMult          float64
	AllowedSide              string
	MaxFrenzyRVOL            float64
	MaxCounterImpulseATR     float64
	MinSignalRVOL            float64
	CooldownAfterExitMinutes int
	CooldownLossMinutes      int
	CooldownLossSet          bool
	CooldownProfitMinutes    int
	CooldownProfitSet        bool
	VWAPSessionWarmupMinutes int
	VWAPSessionWarmupSet     bool
	BlockedUTCHours          map[int]bool

	MinOIDeltaLongSet        bool
	MinOIDeltaShortSet       bool
	MinEMASlopeLongSet       bool

	// Option C: Optimasi Seleksi Entry & Eksit Sisi LONG
	MinCandleBodyRatioLong   float64
	MinCandleBodyRatioShort  float64
	MaxAdverseRetestATRShort float64
	MaxEMASlopeShort         float64
	MaxPullbackRVOLLong      float64
	MinImpulseATRLong        float64
	MinEMASlopeLong          float64
	RequireCleanPullbackLong bool
	PartialTP1ATRMultLong      float64
	SLATRMultiplierLong        float64
	MinNATRLongPct             float64
	BTCChopBlockLong           bool
	BTCChopBlockShort          bool
	DisablePanicDumpFrenzyLong bool
	BTCTier2LongSymmetric      bool

	// Timeframe Operasional
	Timeframe      string
	CandleDuration time.Duration

	// Batas Waktu & Staleness
	ScannerInterval      time.Duration
	MaxBookStaleness     time.Duration
	MaxMarkStaleness     time.Duration
	MaxDataStaleness     time.Duration
	MaxAccountStale      time.Duration
	MinOIInterval        time.Duration
	MaxOIInterval        time.Duration
	MaxOIStaleness       time.Duration
	LimitOrderTimeout     time.Duration
	MaxHoldingDuration    time.Duration
	EntryRepegMaxAttempts int
	EntryRepegMaxDriftPct float64
	OrphanPositionPolicy  string
	RecvWindowMS          int64

	// Keamanan Dashboard & AI Copilot
	DashboardPIN     string
	GeminiAPIKey     string
	GeminiModel      string
	GeminiTimeoutSec int
	AIRateLimitRPM   int

	// Flag Isolasi Perubahan Engine Commit a7f3eda (default: false = perilaku 9815362)
	BTCGateShortOnly    bool
	SLGapFillEnabled    bool
	DeductPendingMargin bool

	// Strategi Sekunder: 1-Bar Liquidity Sweep SHORT-ONLY (Additive, default: disabled)
	Sweep SweepStrategyConfig

	// Multi-Regime Strategy Suite (all opt-in; VWAPStrategyEnabled defaults to true)
	VWAPStrategyEnabled          bool
	RelResidual                  RelResidualStrategyConfig
	VolBreakout                  VolBreakoutStrategyConfig
	MacroRouter                  MacroRegimeRouterConfig
	MaxAdverseFundingRate        float64
	MinShortFundingRate          float64
	MaxLongFundingRate           float64
	RollingVWAPBars              int
	MaxNewEntriesPerBar          int
	PortfolioClusterSLCount      int
	PortfolioClusterCooldownMins int
	ScannerExcludeTopVolRank     int
	HTFEMAPeriod                 int
	MarketBreadthLowThreshold    float64
	MarketBreadthLowRiskMult     float64
	StaleLossExitDuration        time.Duration
	MacroRangeRiskMult           float64
	BreakoutPriorityFirst        bool
	HTFEMAMaxRisingSlopePct      float64
	DDThrottleThresholdPct       float64
	DDThrottleRiskMult           float64
	ResetPeakEquityOnBoot        bool
	MacroVolExpansionRatio       float64
	MacroVolExpansionRiskMult    float64
	RS4hWeakThresholdPct         float64
	RS4hWeakRiskMult             float64
	RS4hStrongThresholdPct       float64
	RS4hStrongRiskMult           float64
	DDThrottleTier2ThresholdPct  float64
	DDThrottleTier2RiskMult      float64
	MaxEffectiveRiskPerTradePct  float64
	EMAPullback                  EMAPullbackStrategyConfig
	VWAPBreakdown                VWAPBreakdownStrategyConfig
	StructureBreak               StructureBreakStrategyConfig
}

// SweepStrategyConfig memuat parameter strategi sekunder 1-Bar Liquidity Sweep SHORT-ONLY.
type SweepStrategyConfig struct {
	Enabled                  bool
	LookbackBars             int
	MinUpperWickRatio        float64
	MaxLowerWickRatio        float64
	MaxDeltaZScore           float64
	ScannerAllowExtendedVWAP bool
	ScannerMaxExtraCoins     int
}

// RelResidualStrategyConfig holds opt-in settings for cross-sectional relative residual alpha.
type RelResidualStrategyConfig struct {
	Enabled           bool
	Mode              string
	LookbackBars      int
	MinRelZ           float64
	MinVWAPStretchATR float64
	MinDeltaZ         float64
	BypassBTCTier2    bool
}

// VolBreakoutStrategyConfig holds opt-in settings for volatility compression breakout alpha.
type VolBreakoutStrategyConfig struct {
	Enabled               bool
	LookbackBars          int
	MaxCompressionRatio   float64
	MinRVOL               float64
	MinBodyRatio          float64
	MinDeltaZ             float64
	AlignMacroTrend       bool
	MaxEMA200DistATR      float64
	MaxVWAPDistATR        float64
	MaxNATRPct            float64
	RiskMult              float64
	EntryMarket           bool
	ScannerEnabled        bool
	MinNATRPct            float64
	MinVolume24hUSDT      float64
	MaxWatchedCoins       int
	ExcludeTopVolRank     int
	MaxScannerVWAPDistATR float64
}

// MacroRegimeRouterConfig holds opt-in settings for 1h BTC macro regime routing.
type MacroRegimeRouterConfig struct {
	Enabled             bool
	TrendERThreshold    float64
	VolShockRatio       float64
	VWAPOnlyInRange     bool
	BreakoutOnlyInTrend bool
	RelOnlyInRange      bool
}

// EMAPullbackStrategyConfig holds opt-in settings for stacked EMA trend continuation pullback alpha.
type EMAPullbackStrategyConfig struct {
	Enabled          bool
	FastPeriod       int
	SlowPeriod       int
	RetestLookback   int
	TouchTolATR      float64
	TouchTolSet      bool
	MinBodyRatio     float64
	MinBodySet       bool
	MinDeltaZ        float64
	MaxEMA200DistATR float64
	RiskMult         float64
}

// VWAPBreakdownStrategyConfig holds opt-in settings for failed relief VWAP breakdown alpha.
type VWAPBreakdownStrategyConfig struct {
	Enabled         bool
	LookbackBars    int
	MinAboveBars    int
	MinRVOL         float64
	MinBodyRatio    float64
	MinBodySet      bool
	MinDeltaZ       float64
	MaxBelowVWAPATR float64
	RiskMult        float64
}

// StructureBreakStrategyConfig holds opt-in settings for 5-bar pivot market-structure break alpha.
type StructureBreakStrategyConfig struct {
	Enabled          bool
	LookbackBars     int
	MinBounceATR     float64
	MinRVOL          float64
	MinBodyRatio     float64
	MinBodySet       bool
	MinDeltaZ        float64
	MaxEMA200DistATR float64
	RiskMult         float64
}

// loadDotEnvWithOverwrite membaca file .env secara otomatis ke environment variable proses.
func loadDotEnvWithOverwrite(filepath string, overwrite bool) {
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

		if overwrite || os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
}

func loadDotEnv(filepath string) {
	loadDotEnvWithOverwrite(filepath, false)
}

func LoadConfig() (*Config, error) {
	return LoadConfigFile(".env")
}

func LoadConfigFile(envPath string) (*Config, error) {
	if envPath != "" {
		if envPath == ".env" {
			loadDotEnv(envPath)
		} else {
			loadDotEnvWithOverwrite(envPath, true)
		}
	}

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

	apiKey := strings.TrimSpace(getRawEnv("BINANCE_API_KEY", ""))
	secretKey := strings.TrimSpace(getRawEnv("BINANCE_SECRET_KEY", ""))

	// Credential Guard: Jika mode Live aktif, wajib ada API Key & Secret Key non-placeholder
	if !dryRun {
		if apiKey == "" || secretKey == "" || strings.Contains(apiKey, "your_binance_api_key") {
			return nil, fmt.Errorf("config CRITICAL: DRY_RUN_MODE=false (live money) requires valid BINANCE_API_KEY and BINANCE_SECRET_KEY")
		}
	}

	dbPath := getRawEnv("SQLITE_DB_PATH", "./data/trml.db")
	logLevel := strings.ToUpper(getEnv("LOG_LEVEL", "INFO"))

	makerFee, err := decimal.NewFromString(getEnv("MAKER_FEE_RATE", "0.0002"))
	if err != nil || makerFee.IsNegative() || makerFee.GreaterThan(decimal.NewFromFloat(0.05)) {
		return nil, fmt.Errorf("config: invalid MAKER_FEE_RATE '%s' (must be between 0.0 and 0.05)", getEnv("MAKER_FEE_RATE", "0.0002"))
	}
	takerFee, err := decimal.NewFromString(getEnv("TAKER_FEE_RATE", "0.0005"))
	if err != nil || takerFee.IsNegative() || takerFee.GreaterThan(decimal.NewFromFloat(0.05)) {
		return nil, fmt.Errorf("config: invalid TAKER_FEE_RATE '%s' (must be between 0.0 and 0.05)", getEnv("TAKER_FEE_RATE", "0.0005"))
	}
	slippage, err := decimal.NewFromString(getEnv("SLIPPAGE_RATE", "0.0002"))
	if err != nil || slippage.IsNegative() || slippage.GreaterThan(decimal.NewFromFloat(0.05)) {
		return nil, fmt.Errorf("config: invalid SLIPPAGE_RATE '%s' (must be between 0.0 and 0.05)", getEnv("SLIPPAGE_RATE", "0.0002"))
	}

	initEqStr := getEnv("INITIAL_EQUITY", "125.0")
	initEq, err := decimal.NewFromString(initEqStr)
	if err != nil || initEq.LessThanOrEqual(decimal.Zero) {
		return nil, fmt.Errorf("config: invalid INITIAL_EQUITY '%s' (must be positive decimal)", initEqStr)
	}

	compounding, err := parseBoolEnvStrict("EQUITY_MODE_COMPOUNDING", getEnv("EQUITY_MODE_COMPOUNDING", "false"))
	if err != nil {
		return nil, err
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
	ddFloat, err := parseFloatEnv(ddStr)
	if err != nil || ddFloat >= 0 || ddFloat < -1.00 {
		return nil, fmt.Errorf("config: invalid MAX_DAILY_DRAWDOWN_PCT '%s' (must be negative float between -0.01 and -1.00)", ddStr)
	}

	// Live safety guard: jika mode live (dryRun=false) dan drawdown di-set lebih longgar dari -0.10,
	// paksa batas pengaman live ke -0.10 (-10.0%) sementara tetap menjaga profil QMAX (-0.075).
	if !dryRun && ddFloat < -0.10 {
		slog.Warn("live mode active: clamping MAX_DAILY_DRAWDOWN_PCT to safe live limit of -0.10 (-10%)", "configured", ddFloat)
		ddFloat = -0.10
	}

	slotsStr := getEnv("MAX_CONCURRENT_SLOTS", "3")
	slots, err := strconv.Atoi(slotsStr)
	if err != nil || slots < 1 || slots > 16 {
		return nil, fmt.Errorf("config: invalid MAX_CONCURRENT_SLOTS '%s' (must be integer between 1 and 16)", slotsStr)
	}

	levStr := getEnv("ACCOUNT_LEVERAGE", "5.0")
	lev, err := parseFloatEnv(levStr)
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
	minVol, err := parseFloatEnv(minVolStr)
	if err != nil || minVol < 0.0 {
		return nil, fmt.Errorf("config: invalid MIN_VOLUME_24H_USDT '%s' (must be a non-negative float)", minVolStr)
	}

	recvWinStr := getEnv("RECV_WINDOW_MS", "5000")
	recvWin, err := strconv.ParseInt(recvWinStr, 10, 64)
	if err != nil || recvWin < 1000 || recvWin > 60000 {
		return nil, fmt.Errorf("config: invalid RECV_WINDOW_MS '%s' (must be integer between 1000 and 60000)", recvWinStr)
	}

	pullbackTolStr := getEnv("PULLBACK_TOLERANCE_ATR", "0.35")
	pullbackTol, err := parseFloatEnv(pullbackTolStr)
	if err != nil || pullbackTol <= 0 || pullbackTol > 2.0 {
		return nil, fmt.Errorf("config: invalid PULLBACK_TOLERANCE_ATR '%s' (must be float between 0.05 and 2.0)", pullbackTolStr)
	}

	minBounceZStr := getEnv("MIN_BOUNCE_ZSCORE", "0.40")
	minBounceZ, err := parseFloatEnv(minBounceZStr)
	if err != nil || minBounceZ <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_BOUNCE_ZSCORE '%s' (must be positive float)", minBounceZStr)
	}

	emaPeriodStr := getEnv("EMA_PERIOD", "200")
	emaPeriod, err := strconv.Atoi(emaPeriodStr)
	if err != nil || emaPeriod < 10 || emaPeriod > 1000 {
		return nil, fmt.Errorf("config: invalid EMA_PERIOD '%s' (must be integer between 10 and 1000)", emaPeriodStr)
	}

	minEMA200DistStr := getEnv("MIN_EMA200_DISTANCE_ATR", "0.15")
	minEMA200Dist, err := parseFloatEnv(minEMA200DistStr)
	if err != nil || minEMA200Dist < 0 || minEMA200Dist > 5.0 {
		return nil, fmt.Errorf("config: invalid MIN_EMA200_DISTANCE_ATR '%s' (must be float between 0.0 and 5.0)", minEMA200DistStr)
	}

	maxEMA200DistStr := getEnv("MAX_EMA200_DISTANCE_ATR", "3.20")
	maxEMA200Dist, err := parseFloatEnv(maxEMA200DistStr)
	if err != nil || maxEMA200Dist <= 0 || maxEMA200Dist > 20.0 {
		return nil, fmt.Errorf("config: invalid MAX_EMA200_DISTANCE_ATR '%s' (must be float between 0.1 and 20.0)", maxEMA200DistStr)
	}

	minBounceZLong := minBounceZ
	if minBounceZLongStr, isSet := lookupCleanEnv("MIN_BOUNCE_ZSCORE_LONG"); isSet {
		parsed, pErr := parseFloatEnv(minBounceZLongStr)
		if pErr != nil || parsed <= 0 {
			return nil, fmt.Errorf("config: invalid MIN_BOUNCE_ZSCORE_LONG '%s' (must be positive float)", minBounceZLongStr)
		}
		minBounceZLong = parsed
	}

	minBounceZShort := minBounceZ
	if minBounceZShortStr, isSet := lookupCleanEnv("MIN_BOUNCE_ZSCORE_SHORT"); isSet {
		parsed, pErr := parseFloatEnv(minBounceZShortStr)
		if pErr != nil || parsed <= 0 {
			return nil, fmt.Errorf("config: invalid MIN_BOUNCE_ZSCORE_SHORT '%s' (must be positive float)", minBounceZShortStr)
		}
		minBounceZShort = parsed
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
	minImpulseATR, err := parseFloatEnv(minImpulseATRStr)
	if err != nil || minImpulseATR <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_IMPULSE_ATR '%s' (must be positive float)", minImpulseATRStr)
	}

	maxEntryDistStr := getEnv("MAX_ENTRY_DISTANCE_ATR", "1.20")
	maxEntryDist, err := parseFloatEnv(maxEntryDistStr)
	if err != nil || maxEntryDist <= 0 || maxEntryDist > 5.0 {
		return nil, fmt.Errorf("config: invalid MAX_ENTRY_DISTANCE_ATR '%s' (must be float between 0.1 and 5.0)", maxEntryDistStr)
	}

	availMarginFrac, err := decimal.NewFromString(getEnv("MAX_MARGIN_AVAIL_FRAC", "0.90"))
	if err != nil || availMarginFrac.LessThanOrEqual(decimal.Zero) || availMarginFrac.GreaterThan(decimal.NewFromFloat(1.0)) {
		return nil, fmt.Errorf("config: invalid MAX_MARGIN_AVAIL_FRAC '%s' (must be between 0.10 and 1.00)", getEnv("MAX_MARGIN_AVAIL_FRAC", "0.90"))
	}

	minFallbackRVOLStr := getEnv("MIN_FALLBACK_RVOL", "0.60")
	minFallbackRVOL, err := parseFloatEnv(minFallbackRVOLStr)
	if err != nil || minFallbackRVOL <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_FALLBACK_RVOL '%s' (must be positive float)", minFallbackRVOLStr)
	}

	maxSpreadStr := getEnv("MAX_SPREAD_PCT", "0.06")
	maxSpread, err := parseFloatEnv(maxSpreadStr)
	if err != nil || maxSpread <= 0 || maxSpread > 1.0 {
		return nil, fmt.Errorf("config: invalid MAX_SPREAD_PCT '%s' (must be float between 0.001 and 1.0)", maxSpreadStr)
	}

	minNATRStr := getEnv("MIN_NATR_PCT", "0.75")
	minNATR, err := parseFloatEnv(minNATRStr)
	if err != nil || minNATR <= 0 || minNATR > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_NATR_PCT '%s' (must be float between 0.01 and 10.0)", minNATRStr)
	}

	maxNATRStr := getEnv("MAX_NATR_PCT", "1.20")
	maxNATR, err := parseFloatEnv(maxNATRStr)
	if err != nil || maxNATR < minNATR || maxNATR > 20.0 {
		return nil, fmt.Errorf("config: invalid MAX_NATR_PCT '%s' (must be float >= MIN_NATR_PCT and <= 20.0)", maxNATRStr)
	}

	maxScannerVWAPDistStr := getEnv("MAX_SCANNER_VWAP_DIST_ATR", "2.50")
	maxScannerVWAPDist, err := parseFloatEnv(maxScannerVWAPDistStr)
	if err != nil || maxScannerVWAPDist < 0 || maxScannerVWAPDist > 20.0 {
		return nil, fmt.Errorf("config: invalid MAX_SCANNER_VWAP_DIST_ATR '%s' (must be float between 0.0 and 20.0)", maxScannerVWAPDistStr)
	}

	maxWatchedStr := getEnv("MAX_WATCHED_COINS", "20")
	maxWatched, err := strconv.Atoi(maxWatchedStr)
	if err != nil || maxWatched < 1 || maxWatched > 150 {
		return nil, fmt.Errorf("config: invalid MAX_WATCHED_COINS '%s' (must be integer between 1 and 150)", maxWatchedStr)
	}

	minOIDeltaStr := getEnv("MIN_OI_DELTA_PCT", "0.00")
	minOIDelta, err := parseFloatEnv(minOIDeltaStr)
	if err != nil || minOIDelta < -5.0 || minOIDelta > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_OI_DELTA_PCT '%s' (must be float between -5.0 and 10.0)", minOIDeltaStr)
	}

	minOIDeltaLongStr, minOIDeltaLongSet := lookupCleanEnv("MIN_OI_DELTA_LONG_PCT")
	if !minOIDeltaLongSet {
		minOIDeltaLongStr = "0.00"
	}
	minOIDeltaLong, err := parseFloatEnv(minOIDeltaLongStr)
	if err != nil || minOIDeltaLong < -5.0 || minOIDeltaLong > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_OI_DELTA_LONG_PCT '%s' (must be float between -5.0 and 10.0)", minOIDeltaLongStr)
	}

	minOIDeltaShortStr, minOIDeltaShortSet := lookupCleanEnv("MIN_OI_DELTA_SHORT_PCT")
	if !minOIDeltaShortSet {
		minOIDeltaShortStr = "-0.02"
	}
	minOIDeltaShort, err := parseFloatEnv(minOIDeltaShortStr)
	if err != nil || minOIDeltaShort < -5.0 || minOIDeltaShort > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_OI_DELTA_SHORT_PCT '%s' (must be float between -5.0 and 10.0)", minOIDeltaShortStr)
	}

	disableOIFilter, err := parseBoolEnvStrict("DISABLE_OI_FILTER", getEnv("DISABLE_OI_FILTER", "false"))
	if err != nil {
		return nil, err
	}

	slMultStr := getEnv("SL_ATR_MULTIPLIER", "1.5")
	slMult, err := parseFloatEnv(slMultStr)
	if err != nil || slMult <= 0.1 || slMult > 10.0 {
		return nil, fmt.Errorf("config: invalid SL_ATR_MULTIPLIER '%s' (must be float between 0.1 and 10.0)", slMultStr)
	}

	tpMultStr := getEnv("TP_ATR_MULTIPLIER", "2.25")
	tpMult, err := parseFloatEnv(tpMultStr)
	if err != nil || tpMult <= 0.1 || tpMult > 20.0 {
		return nil, fmt.Errorf("config: invalid TP_ATR_MULTIPLIER '%s' (must be float between 0.1 and 20.0)", tpMultStr)
	}

	beTriggerStr := getEnv("BREAK_EVEN_TRIGGER_ATR", "1.15")
	beTrigger, err := parseFloatEnv(beTriggerStr)
	if err != nil || beTrigger <= 0.1 || beTrigger > 10.0 {
		return nil, fmt.Errorf("config: invalid BREAK_EVEN_TRIGGER_ATR '%s' (must be float between 0.1 and 10.0)", beTriggerStr)
	}

	tier1BEStr := getEnv("FLAG_TIER1_EARLY_BE", getEnv("TIER1_BE_ENABLED", "false"))
	tier1BEEnabled, err := parseBoolEnvStrict("FLAG_TIER1_EARLY_BE / TIER1_BE_ENABLED", tier1BEStr)
	if err != nil {
		return nil, err
	}
	btcGateShortOnly, err := parseBoolEnvStrict("FLAG_BTC_GATE_SHORT_ONLY", getEnv("FLAG_BTC_GATE_SHORT_ONLY", "true")) // default true (best practice)
	if err != nil {
		return nil, err
	}
	slGapFillEnabled, err := parseBoolEnvStrict("FLAG_SL_GAP_FILL", getEnv("FLAG_SL_GAP_FILL", "false"))
	if err != nil {
		return nil, err
	}
	deductPendingMargin, err := parseBoolEnvStrict("FLAG_DEDUCT_PENDING_MARGIN", getEnv("FLAG_DEDUCT_PENDING_MARGIN", "false"))
	if err != nil {
		return nil, err
	}
	t1BETriggerLongStr := getEnv("TIER1_BE_TRIGGER_LONG_ATR", "")
	var t1BETriggerLong float64
	if t1BETriggerLongStr != "" {
		val, err := parseFloatEnv(t1BETriggerLongStr)
		if err != nil || val < 0.05 || val > 10.0 {
			return nil, fmt.Errorf("config: invalid TIER1_BE_TRIGGER_LONG_ATR '%s' (must be float between 0.05 and 10.0)", t1BETriggerLongStr)
		}
		t1BETriggerLong = val
	}
	if t1BETriggerLong == 0 {
		t1BETriggerLong = beTrigger
	}

	t1BETriggerShortStr := getEnv("TIER1_BE_TRIGGER_SHORT_ATR", "")
	var t1BETriggerShort float64
	if t1BETriggerShortStr != "" {
		val, err := parseFloatEnv(t1BETriggerShortStr)
		if err != nil || val < 0.05 || val > 10.0 {
			return nil, fmt.Errorf("config: invalid TIER1_BE_TRIGGER_SHORT_ATR '%s' (must be float between 0.05 and 10.0)", t1BETriggerShortStr)
		}
		t1BETriggerShort = val
	}
	if t1BETriggerShort == 0 {
		t1BETriggerShort = beTrigger
	}

	tier2ProfitLockEnabled, err := parseBoolEnvStrict("TIER2_PROFIT_LOCK_ENABLED", getEnv("TIER2_PROFIT_LOCK_ENABLED", "false"))
	if err != nil {
		return nil, err
	}

	t2ProfitLockStr := getEnv("TIER2_PROFIT_LOCK_ATR", "1.80")
	t2ProfitLock, err := parseFloatEnv(t2ProfitLockStr)
	if err != nil || t2ProfitLock <= 0.1 || t2ProfitLock > 10.0 {
		return nil, fmt.Errorf("config: invalid TIER2_PROFIT_LOCK_ATR '%s' (must be float between 0.1 and 10.0)", t2ProfitLockStr)
	}

	t2OffsetStr := getEnv("TIER2_PROFIT_LOCK_OFFSET_ATR", "0.90")
	t2Offset, err := parseFloatEnv(t2OffsetStr)
	if err != nil || t2Offset <= 0.05 || t2Offset > 10.0 {
		return nil, fmt.Errorf("config: invalid TIER2_PROFIT_LOCK_OFFSET_ATR '%s' (must be float between 0.05 and 10.0)", t2OffsetStr)
	}

	minSLDistPctStr := getEnv("MIN_SL_DISTANCE_PCT", "0.0150")
	minSLDistPct, err := decimal.NewFromString(minSLDistPctStr)
	if err != nil || minSLDistPct.LessThan(decimal.Zero) || minSLDistPct.GreaterThan(decimal.NewFromFloat(0.20)) {
		return nil, fmt.Errorf("config: invalid MIN_SL_DISTANCE_PCT '%s' (must be between 0.00 and 0.20)", minSLDistPctStr)
	}

	partialTPEnabled, err := parseBoolEnvStrict("PARTIAL_TP_ENABLED", getEnv("PARTIAL_TP_ENABLED", "true"))
	if err != nil {
		return nil, err
	}

	partialTP1RatioStr := getEnv("PARTIAL_TP1_RATIO", "0.50")
	partialTP1Ratio, err := parseFloatEnv(partialTP1RatioStr)
	if err != nil || partialTP1Ratio <= 0.05 || partialTP1Ratio >= 0.95 {
		return nil, fmt.Errorf("config: invalid PARTIAL_TP1_RATIO '%s' (must be float between 0.05 and 0.95)", partialTP1RatioStr)
	}

	partialTP1ATRMultStr := getEnv("PARTIAL_TP1_ATR_MULT", "1.20")
	partialTP1ATRMult, err := parseFloatEnv(partialTP1ATRMultStr)
	if err != nil || partialTP1ATRMult <= 0.1 || partialTP1ATRMult > 10.0 {
		return nil, fmt.Errorf("config: invalid PARTIAL_TP1_ATR_MULT '%s' (must be float between 0.1 and 10.0)", partialTP1ATRMultStr)
	}

	runnerBECushionATRStr := getEnv("RUNNER_BE_CUSHION_ATR", "0.35")
	runnerBECushionATR, err := parseFloatEnv(runnerBECushionATRStr)
	if err != nil || runnerBECushionATR < 0.0 || runnerBECushionATR > 5.0 {
		return nil, fmt.Errorf("config: invalid RUNNER_BE_CUSHION_ATR '%s' (must be float between 0.0 and 5.0)", runnerBECushionATRStr)
	}

	trailingEnabled, err := parseBoolEnvStrict("TRAILING_ENABLED", getEnv("TRAILING_ENABLED", "true"))
	if err != nil {
		return nil, err
	}

	trailingActivateATRStr := getEnv("TRAILING_ACTIVATE_ATR", "2.00")
	trailingActivateATR, err := parseFloatEnv(trailingActivateATRStr)
	if err != nil || trailingActivateATR <= 0.1 || trailingActivateATR > 10.0 {
		return nil, fmt.Errorf("config: invalid TRAILING_ACTIVATE_ATR '%s' (must be float between 0.1 and 10.0)", trailingActivateATRStr)
	}

	trailingDistATRStr := getEnv("TRAILING_DISTANCE_ATR", "0.80")
	trailingDistATR, err := parseFloatEnv(trailingDistATRStr)
	if err != nil || trailingDistATR <= 0.05 || trailingDistATR > 10.0 {
		return nil, fmt.Errorf("config: invalid TRAILING_DISTANCE_ATR '%s' (must be float between 0.05 and 10.0)", trailingDistATRStr)
	}

	btcGateEnabledStr := getEnv("BTC_GATE_ENABLED", "true")
	btcGateEnabled, err := parseBoolEnvStrict("BTC_GATE_ENABLED", btcGateEnabledStr)
	if err != nil {
		return nil, err
	}

	btcMaxDumpStr := getEnv("BTC_MAX_5M_DUMP_PCT", "0.60")
	btcMaxDump, err := parseFloatEnv(btcMaxDumpStr)
	if err != nil || btcMaxDump <= 0.05 || btcMaxDump > 10.0 {
		return nil, fmt.Errorf("config: invalid BTC_MAX_5M_DUMP_PCT '%s' (must be positive float between 0.05 and 10.0)", btcMaxDumpStr)
	}

	btcMaxPumpStr := getEnv("BTC_MAX_5M_PUMP_PCT", "0.60")
	btcMaxPump, err := parseFloatEnv(btcMaxPumpStr)
	if err != nil || btcMaxPump <= 0.05 || btcMaxPump > 10.0 {
		return nil, fmt.Errorf("config: invalid BTC_MAX_5M_PUMP_PCT '%s' (must be positive float between 0.05 and 10.0)", btcMaxPumpStr)
	}

	btcPersistenceBarsStr := getEnv("BTC_GATE_PERSISTENCE_BARS", "1")
	btcPersistenceBars, err := strconv.Atoi(btcPersistenceBarsStr)
	if err != nil || btcPersistenceBars < 1 || btcPersistenceBars > 20 {
		return nil, fmt.Errorf("config: invalid BTC_GATE_PERSISTENCE_BARS '%s' (must be integer between 1 and 20)", btcPersistenceBarsStr)
	}

	allowedSide := strings.ToUpper(strings.TrimSpace(getEnv("ALLOWED_SIDE", "AUTO")))
	if allowedSide != "AUTO" && allowedSide != "SHORT_ONLY" && allowedSide != "LONG_ONLY" && allowedSide != "BOTH" {
		return nil, fmt.Errorf("config: invalid ALLOWED_SIDE '%s' (must be 'AUTO', 'SHORT_ONLY', 'LONG_ONLY', or 'BOTH')", allowedSide)
	}

	maxFrenzyRVOLStr := getEnv("MAX_FRENZY_RVOL", "2.50")
	maxFrenzyRVOL, err := parseFloatEnv(maxFrenzyRVOLStr)
	if err != nil || maxFrenzyRVOL <= 0 {
		return nil, fmt.Errorf("config: invalid MAX_FRENZY_RVOL '%s' (must be positive float)", maxFrenzyRVOLStr)
	}

	maxCounterImpulseATRStr := getEnv("MAX_COUNTER_IMPULSE_ATR", "1.50")
	maxCounterImpulseATR, err := parseFloatEnv(maxCounterImpulseATRStr)
	if err != nil || maxCounterImpulseATR <= 0 {
		return nil, fmt.Errorf("config: invalid MAX_COUNTER_IMPULSE_ATR '%s' (must be positive float)", maxCounterImpulseATRStr)
	}

	btcCounterMomStr := getEnv("BTC_COUNTER_MOMENTUM_PCT", "0.25")
	btcCounterMom, err := parseFloatEnv(btcCounterMomStr)
	if err != nil || btcCounterMom < 0.0 || btcCounterMom > 5.0 {
		return nil, fmt.Errorf("config: invalid BTC_COUNTER_MOMENTUM_PCT '%s' (must be float between 0.0 and 5.0)", btcCounterMomStr)
	}

	minSignalRVOLStr := getEnv("MIN_SIGNAL_RVOL", "0.0")
	minSignalRVOL, err := parseFloatEnv(minSignalRVOLStr)
	if err != nil || minSignalRVOL < 0.0 || minSignalRVOL > 5.0 {
		return nil, fmt.Errorf("config: invalid MIN_SIGNAL_RVOL '%s' (must be float between 0.0 and 5.0)", minSignalRVOLStr)
	}

	httpPort := getEnv("HTTP_PORT", "2020")
	dashPIN := getRawEnv("DASHBOARD_PIN", "")
	geminiKey := getRawEnv("GEMINI_API_KEY", "")
	geminiModel := strings.TrimSpace(getEnv("GEMINI_MODEL", "gemini-2.5-flash"))

	geminiTimeoutStr := getEnv("GEMINI_TIMEOUT_SEC", "25")
	geminiTimeout, err := strconv.Atoi(geminiTimeoutStr)
	if err != nil || geminiTimeout <= 0 {
		return nil, fmt.Errorf("config: invalid GEMINI_TIMEOUT_SEC '%s' (must be positive integer)", geminiTimeoutStr)
	}

	aiRateLimitStr := getEnv("AI_RATE_LIMIT_RPM", "10")
	aiRateLimit, err := strconv.Atoi(aiRateLimitStr)
	if err != nil || aiRateLimit <= 0 {
		return nil, fmt.Errorf("config: invalid AI_RATE_LIMIT_RPM '%s' (must be positive integer)", aiRateLimitStr)
	}

	maxHoldingDurationStr := getEnv("MAX_HOLDING_DURATION", "4h")
	maxHoldingDuration, err := time.ParseDuration(maxHoldingDurationStr)
	if err != nil || maxHoldingDuration < 15*time.Minute {
		return nil, fmt.Errorf("config: invalid MAX_HOLDING_DURATION '%s' (must be duration >= 15m)", maxHoldingDurationStr)
	}

	limitOrderTimeoutStr := getEnv("LIMIT_ORDER_TIMEOUT", "150s")
	limitOrderTimeout, err := time.ParseDuration(limitOrderTimeoutStr)
	if err != nil {
		if sec, secErr := strconv.Atoi(limitOrderTimeoutStr); secErr == nil && sec >= 5 {
			limitOrderTimeout = time.Duration(sec) * time.Second
		} else {
			return nil, fmt.Errorf("config: invalid LIMIT_ORDER_TIMEOUT '%s' (must be duration >= 5s or integer seconds >= 5)", limitOrderTimeoutStr)
		}
	}
	if limitOrderTimeout < 5*time.Second {
		return nil, fmt.Errorf("config: invalid LIMIT_ORDER_TIMEOUT '%s' (must be >= 5s)", limitOrderTimeoutStr)
	}

	entryRepegMaxAttemptsStr := getEnv("ENTRY_REPEG_MAX_ATTEMPTS", "3")
	entryRepegMaxAttempts, err := strconv.Atoi(entryRepegMaxAttemptsStr)
	if err != nil || entryRepegMaxAttempts < 0 || entryRepegMaxAttempts > 10 {
		return nil, fmt.Errorf("config: invalid ENTRY_REPEG_MAX_ATTEMPTS '%s' (must be integer between 0 and 10)", entryRepegMaxAttemptsStr)
	}

	entryRepegMaxDriftStr := getEnv("ENTRY_REPEG_MAX_DRIFT_PCT", "0.0005")
	entryRepegMaxDriftPct, err := parseFloatEnv(entryRepegMaxDriftStr)
	if err != nil || entryRepegMaxDriftPct <= 0 || entryRepegMaxDriftPct > 0.01 {
		return nil, fmt.Errorf("config: invalid ENTRY_REPEG_MAX_DRIFT_PCT '%s' (must be float between 0 and 0.01)", entryRepegMaxDriftStr)
	}

	orphanPolicy := strings.ToUpper(getEnv("ORPHAN_POSITION_POLICY", "CLOSE"))
	if orphanPolicy != "ADOPT" && orphanPolicy != "CLOSE" {
		return nil, fmt.Errorf("config: invalid ORPHAN_POSITION_POLICY '%s' (must be 'ADOPT' or 'CLOSE')", orphanPolicy)
	}

	cooldownMinsStr := getEnv("COOLDOWN_AFTER_EXIT_MINUTES", "20")
	cooldownMins, err := strconv.Atoi(cooldownMinsStr)
	if err != nil || cooldownMins < 0 {
		return nil, fmt.Errorf("config: invalid COOLDOWN_AFTER_EXIT_MINUTES '%s' (must be integer >= 0)", cooldownMinsStr)
	}

	cooldownLossStr := getEnv("COOLDOWN_AFTER_LOSS_MINUTES", strconv.Itoa(cooldownMins))
	cooldownLossMins, err := strconv.Atoi(cooldownLossStr)
	if err != nil || cooldownLossMins < 0 {
		return nil, fmt.Errorf("config: invalid COOLDOWN_AFTER_LOSS_MINUTES '%s' (must be integer >= 0)", cooldownLossStr)
	}

	cooldownProfitStr := getEnv("COOLDOWN_AFTER_WIN_MINUTES", "10")
	cooldownProfitMins, err := strconv.Atoi(cooldownProfitStr)
	if err != nil || cooldownProfitMins < 0 {
		return nil, fmt.Errorf("config: invalid COOLDOWN_AFTER_WIN_MINUTES '%s' (must be integer >= 0)", cooldownProfitStr)
	}

	vwapWarmupMinsStr := getEnv("VWAP_SESSION_WARMUP_MINUTES", "30")
	vwapWarmupMins, err := strconv.Atoi(vwapWarmupMinsStr)
	if err != nil || vwapWarmupMins < 0 {
		return nil, fmt.Errorf("config: invalid VWAP_SESSION_WARMUP_MINUTES '%s' (must be integer >= 0)", vwapWarmupMinsStr)
	}

	blockedHours := make(map[int]bool)
	if blockedRaw := strings.TrimSpace(getEnv("BLOCKED_UTC_HOURS", "")); blockedRaw != "" {
		for _, part := range strings.Split(blockedRaw, ",") {
			p := strings.TrimSpace(part)
			if p == "" {
				continue
			}
			hr, hErr := strconv.Atoi(p)
			if hErr != nil || hr < 0 || hr > 23 {
				return nil, fmt.Errorf("config: invalid BLOCKED_UTC_HOURS entry '%s' (must be integer 0..23)", p)
			}
			blockedHours[hr] = true
		}
	}

	btcChopRiskMultStr := getEnv("BTC_CHOP_RISK_MULT", "1.0")
	btcChopRiskMult, err := parseFloatEnv(btcChopRiskMultStr)
	if err != nil || btcChopRiskMult <= 0.0 || btcChopRiskMult > 1.0 {
		return nil, fmt.Errorf("config: invalid BTC_CHOP_RISK_MULT '%s' (must be float in (0.0, 1.0])", btcChopRiskMultStr)
	}

	minCandleBodyRatioLongStr := getEnv("MIN_CANDLE_BODY_RATIO_LONG", "0.0")
	minCandleBodyRatioLong, err := parseFloatEnv(minCandleBodyRatioLongStr)
	if err != nil || minCandleBodyRatioLong < 0.0 || minCandleBodyRatioLong > 1.0 {
		return nil, fmt.Errorf("config: invalid MIN_CANDLE_BODY_RATIO_LONG '%s' (must be float between 0.0 and 1.0)", minCandleBodyRatioLongStr)
	}

	maxPullbackRVOLLongStr := getEnv("MAX_PULLBACK_RVOL_LONG", "0.0")
	maxPullbackRVOLLong, err := parseFloatEnv(maxPullbackRVOLLongStr)
	if err != nil || maxPullbackRVOLLong < 0.0 || maxPullbackRVOLLong > 10.0 {
		return nil, fmt.Errorf("config: invalid MAX_PULLBACK_RVOL_LONG '%s' (must be float between 0.0 and 10.0)", maxPullbackRVOLLongStr)
	}

	minImpulseATRLongStr := getEnv("MIN_IMPULSE_ATR_LONG", "0.0")
	minImpulseATRLong, err := parseFloatEnv(minImpulseATRLongStr)
	if err != nil || minImpulseATRLong < 0.0 || minImpulseATRLong > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_IMPULSE_ATR_LONG '%s' (must be float between 0.0 and 10.0)", minImpulseATRLongStr)
	}

	minEMASlopeLongStr, minEMASlopeLongSet := lookupCleanEnv("MIN_EMA_SLOPE_LONG")
	if !minEMASlopeLongSet {
		minEMASlopeLongStr = "0.0"
	}
	minEMASlopeLong, err := parseFloatEnv(minEMASlopeLongStr)
	if err != nil || minEMASlopeLong < -1.0 || minEMASlopeLong > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_EMA_SLOPE_LONG '%s' (must be float between -1.0 and 10.0)", minEMASlopeLongStr)
	}

	requireCleanPullbackLong, err := parseBoolEnvStrict("REQUIRE_CLEAN_PULLBACK_LONG", getEnv("REQUIRE_CLEAN_PULLBACK_LONG", "false"))
	if err != nil {
		return nil, err
	}

	partialTP1ATRMultLongStr := getEnv("PARTIAL_TP1_ATR_MULT_LONG", "0.0")
	partialTP1ATRMultLong, err := parseFloatEnv(partialTP1ATRMultLongStr)
	if err != nil || partialTP1ATRMultLong < 0.0 || partialTP1ATRMultLong > 10.0 {
		return nil, fmt.Errorf("config: invalid PARTIAL_TP1_ATR_MULT_LONG '%s' (must be float between 0.0 and 10.0)", partialTP1ATRMultLongStr)
	}

	slMultLongStr := getEnv("SL_ATR_MULTIPLIER_LONG", "0.0")
	slMultLong, err := parseFloatEnv(slMultLongStr)
	if err != nil || slMultLong < 0.0 || slMultLong > 10.0 {
		return nil, fmt.Errorf("config: invalid SL_ATR_MULTIPLIER_LONG '%s' (must be float between 0.0 and 10.0)", slMultLongStr)
	}

	minNATRLongStr := getEnv("MIN_NATR_LONG_PCT", "0.0")
	minNATRLong, err := parseFloatEnv(minNATRLongStr)
	if err != nil || minNATRLong < 0.0 || minNATRLong > 10.0 {
		return nil, fmt.Errorf("config: invalid MIN_NATR_LONG_PCT '%s' (must be float between 0.0 and 10.0)", minNATRLongStr)
	}

	btcChopBlockLong, err := parseBoolEnvStrict("BTC_CHOP_BLOCK_LONG", getEnv("BTC_CHOP_BLOCK_LONG", "false"))
	if err != nil {
		return nil, err
	}

	btcChopBlockShort, err := parseBoolEnvStrict("BTC_CHOP_BLOCK_SHORT", getEnv("BTC_CHOP_BLOCK_SHORT", "false"))
	if err != nil {
		return nil, err
	}

	disablePanicDumpFrenzyLong, err := parseBoolEnvStrict("DISABLE_PANIC_DUMP_FRENZY_LONG", getEnv("DISABLE_PANIC_DUMP_FRENZY_LONG", "false"))
	if err != nil {
		return nil, err
	}

	btcTier2LongSymmetric, err := parseBoolEnvStrict("BTC_TIER2_LONG_SYMMETRIC", getEnv("BTC_TIER2_LONG_SYMMETRIC", "false"))
	if err != nil {
		return nil, err
	}

	minRVOLStr := getEnv("MIN_RVOL", getEnv("SCANNER_MIN_RVOL", "1.80"))
	minRVOL, err := parseFloatEnv(minRVOLStr)
	if err != nil || minRVOL <= 0 {
		return nil, fmt.Errorf("config: invalid MIN_RVOL '%s' (must be positive float)", minRVOLStr)
	}

	scannerRankingMode := strings.ToLower(strings.TrimSpace(getEnv("SCANNER_RANKING_MODE", "pullback")))
	if scannerRankingMode == "" {
		scannerRankingMode = "pullback"
	}
	if scannerRankingMode != "pullback" && scannerRankingMode != "rvol" {
		return nil, fmt.Errorf("config: invalid SCANNER_RANKING_MODE '%s' (must be 'pullback' or 'rvol')", scannerRankingMode)
	}

	scannerInterval := 15 * time.Minute
	if sIntStr := getEnv("SCANNER_INTERVAL_MINUTES", getEnv("SCANNER_INTERVAL", "")); sIntStr != "" {
		if sIntMins, err := strconv.Atoi(sIntStr); err == nil && sIntMins > 0 {
			scannerInterval = time.Duration(sIntMins) * time.Minute
		} else if dur, err := time.ParseDuration(sIntStr); err == nil && dur > 0 {
			scannerInterval = dur
		} else {
			return nil, fmt.Errorf("config: invalid SCANNER_INTERVAL '%s' (must be positive minutes or duration)", sIntStr)
		}
	}

	timeframeStr := strings.ToLower(strings.TrimSpace(getEnv("TIMEFRAME", getEnv("KLINE_INTERVAL", "5m"))))
	if timeframeStr == "" {
		timeframeStr = "5m"
	}
	candleDur, err := ParseTimeframeToDuration(timeframeStr)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	maxDataStaleness := candleDur + 30*time.Second
	maxOIStaleness := candleDur + 30*time.Second
	maxOIInterval := candleDur * 2
	if maxOIInterval < 8*time.Minute {
		maxOIInterval = 8 * time.Minute
	}

	sweepEnabled, err := parseBoolEnvStrict("SWEEP_STRATEGY_ENABLED", getEnv("SWEEP_STRATEGY_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	sweepLookbackStr := getEnv("SWEEP_LOOKBACK_BARS", "12")
	sweepLookback, err := strconv.Atoi(sweepLookbackStr)
	if err != nil || sweepLookback < 3 || sweepLookback > 50 {
		return nil, fmt.Errorf("config: invalid SWEEP_LOOKBACK_BARS '%s' (must be integer between 3 and 50)", sweepLookbackStr)
	}
	sweepMinUpperWickStr := getEnv("SWEEP_MIN_UPPER_WICK_RATIO", "0.25")
	sweepMinUpperWick, err := parseFloatEnv(sweepMinUpperWickStr)
	if err != nil || sweepMinUpperWick < 0.05 || sweepMinUpperWick > 0.90 {
		return nil, fmt.Errorf("config: invalid SWEEP_MIN_UPPER_WICK_RATIO '%s' (must be float between 0.05 and 0.90)", sweepMinUpperWickStr)
	}
	sweepMaxLowerWickStr := getEnv("SWEEP_MAX_LOWER_WICK_RATIO", "0.45")
	sweepMaxLowerWick, err := parseFloatEnv(sweepMaxLowerWickStr)
	if err != nil || sweepMaxLowerWick < 0.05 || sweepMaxLowerWick > 0.95 {
		return nil, fmt.Errorf("config: invalid SWEEP_MAX_LOWER_WICK_RATIO '%s' (must be float between 0.05 and 0.95)", sweepMaxLowerWickStr)
	}
	sweepMaxDeltaZStr := getEnv("SWEEP_MAX_DELTA_ZSCORE", "-0.25")
	sweepMaxDeltaZ, err := parseFloatEnv(sweepMaxDeltaZStr)
	if err != nil || sweepMaxDeltaZ < -5.0 || sweepMaxDeltaZ > 5.0 {
		return nil, fmt.Errorf("config: invalid SWEEP_MAX_DELTA_ZSCORE '%s' (must be float between -5.0 and 5.0)", sweepMaxDeltaZStr)
	}
	sweepAllowExtVWAP, err := parseBoolEnvStrict("SWEEP_SCANNER_ALLOW_EXTENDED_VWAP", getEnv("SWEEP_SCANNER_ALLOW_EXTENDED_VWAP", "false"))
	if err != nil {
		return nil, err
	}
	sweepMaxExtraStr := getEnv("SWEEP_SCANNER_MAX_EXTRA_COINS", "15")
	sweepMaxExtra, err := strconv.Atoi(sweepMaxExtraStr)
	if err != nil || sweepMaxExtra < 0 || sweepMaxExtra > 50 {
		return nil, fmt.Errorf("config: invalid SWEEP_SCANNER_MAX_EXTRA_COINS '%s' (must be integer between 0 and 50)", sweepMaxExtraStr)
	}

	vwapStrategyEnabled, err := parseBoolEnvStrict("VWAP_STRATEGY_ENABLED", getEnv("VWAP_STRATEGY_ENABLED", "true"))
	if err != nil {
		return nil, err
	}

	relEnabled, err := parseBoolEnvStrict("REL_STRATEGY_ENABLED", getEnv("REL_STRATEGY_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	relMode := strings.ToUpper(strings.TrimSpace(getEnv("REL_MODE", "MEANREV")))
	if relMode != "MEANREV" && relMode != "MOMENTUM" {
		return nil, fmt.Errorf("config: invalid REL_MODE '%s' (must be MEANREV or MOMENTUM)", relMode)
	}
	relLookbackStr := getEnv("REL_LOOKBACK_BARS", "12")
	relLookback, err := strconv.Atoi(relLookbackStr)
	if err != nil || relLookback < 3 || relLookback > 72 {
		return nil, fmt.Errorf("config: invalid REL_LOOKBACK_BARS '%s' (must be integer between 3 and 72)", relLookbackStr)
	}
	relMinZStr := getEnv("REL_MIN_Z", "1.80")
	relMinZ, err := parseFloatEnv(relMinZStr)
	if err != nil || relMinZ < 0.3 || relMinZ > 10.0 {
		return nil, fmt.Errorf("config: invalid REL_MIN_Z '%s' (must be float between 0.3 and 10.0)", relMinZStr)
	}
	relStretchStr := getEnv("REL_MIN_VWAP_STRETCH_ATR", "1.20")
	relStretch, err := parseFloatEnv(relStretchStr)
	if err != nil || relStretch < 0.0 || relStretch > 10.0 {
		return nil, fmt.Errorf("config: invalid REL_MIN_VWAP_STRETCH_ATR '%s' (must be float between 0.0 and 10.0)", relStretchStr)
	}
	relDeltaZStr := getEnv("REL_MIN_DELTA_Z", "0.20")
	relDeltaZ, err := parseFloatEnv(relDeltaZStr)
	if err != nil || relDeltaZ < -3.0 || relDeltaZ > 5.0 {
		return nil, fmt.Errorf("config: invalid REL_MIN_DELTA_Z '%s' (must be float between -3.0 and 5.0)", relDeltaZStr)
	}
	relBypassTier2, err := parseBoolEnvStrict("REL_BYPASS_BTC_TIER2", getEnv("REL_BYPASS_BTC_TIER2", "true"))
	if err != nil {
		return nil, err
	}

	boEnabled, err := parseBoolEnvStrict("BREAKOUT_STRATEGY_ENABLED", getEnv("BREAKOUT_STRATEGY_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	boLookbackStr := getEnv("BREAKOUT_LOOKBACK_BARS", "20")
	boLookback, err := strconv.Atoi(boLookbackStr)
	if err != nil || boLookback < 5 || boLookback > 100 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_LOOKBACK_BARS '%s' (must be integer between 5 and 100)", boLookbackStr)
	}
	boMaxCompStr := getEnv("BREAKOUT_MAX_COMPRESSION", "0.90")
	boMaxComp, err := parseFloatEnv(boMaxCompStr)
	if err != nil || boMaxComp < 0.30 || boMaxComp > 2.00 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MAX_COMPRESSION '%s' (must be float between 0.30 and 2.00)", boMaxCompStr)
	}
	boMinRVOLStr := getEnv("BREAKOUT_MIN_RVOL", "1.50")
	boMinRVOL, err := parseFloatEnv(boMinRVOLStr)
	if err != nil || boMinRVOL < 0.50 || boMinRVOL > 10.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MIN_RVOL '%s' (must be float between 0.50 and 10.0)", boMinRVOLStr)
	}
	boMinBodyStr := getEnv("BREAKOUT_MIN_BODY_RATIO", "0.50")
	boMinBody, err := parseFloatEnv(boMinBodyStr)
	if err != nil || boMinBody < 0.10 || boMinBody > 0.95 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MIN_BODY_RATIO '%s' (must be float between 0.10 and 0.95)", boMinBodyStr)
	}
	boMinDeltaZStr := getEnv("BREAKOUT_MIN_DELTA_Z", "0.75")
	boMinDeltaZ, err := parseFloatEnv(boMinDeltaZStr)
	if err != nil || boMinDeltaZ < 0.0 || boMinDeltaZ > 5.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MIN_DELTA_Z '%s' (must be float between 0.0 and 5.0)", boMinDeltaZStr)
	}
	boAlignMacroTrend, err := parseBoolEnvStrict("BREAKOUT_ALIGN_MACRO_TREND", getEnv("BREAKOUT_ALIGN_MACRO_TREND", "false"))
	if err != nil {
		return nil, err
	}
	boMaxEMA200DistStr := getEnv("BREAKOUT_MAX_EMA200_DIST_ATR", "0.0")
	boMaxEMA200Dist, err := parseFloatEnv(boMaxEMA200DistStr)
	if err != nil || boMaxEMA200Dist < 0.0 || boMaxEMA200Dist > 20.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MAX_EMA200_DIST_ATR '%s' (must be float between 0.0 and 20.0)", boMaxEMA200DistStr)
	}
	boMaxVWAPDistStr := getEnv("BREAKOUT_MAX_VWAP_DIST_ATR", "0.0")
	boMaxVWAPDist, err := parseFloatEnv(boMaxVWAPDistStr)
	if err != nil || boMaxVWAPDist < 0.0 || boMaxVWAPDist > 20.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MAX_VWAP_DIST_ATR '%s' (must be float between 0.0 and 20.0)", boMaxVWAPDistStr)
	}
	boMaxNATRStr := getEnv("BREAKOUT_MAX_NATR_PCT", "0.0")
	boMaxNATR, err := parseFloatEnv(boMaxNATRStr)
	if err != nil || boMaxNATR < 0.0 || boMaxNATR > 20.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MAX_NATR_PCT '%s' (must be float between 0.0 and 20.0)", boMaxNATRStr)
	}
	boRiskMultStr := getEnv("BREAKOUT_RISK_MULT", "1.0")
	boRiskMult, err := parseFloatEnv(boRiskMultStr)
	if err != nil || boRiskMult < 0.05 || boRiskMult > 5.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_RISK_MULT '%s' (must be float between 0.05 and 5.0)", boRiskMultStr)
	}
	boEntryMarket, err := parseBoolEnvStrict("BREAKOUT_ENTRY_MARKET", getEnv("BREAKOUT_ENTRY_MARKET", "false"))
	if err != nil {
		return nil, err
	}
	boScannerEnabled, err := parseBoolEnvStrict("BREAKOUT_SCANNER_ENABLED", getEnv("BREAKOUT_SCANNER_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	boMinNATRStr := getEnv("BREAKOUT_MIN_NATR_PCT", "0.0")
	boMinNATR, err := parseFloatEnv(boMinNATRStr)
	if err != nil || boMinNATR < 0.0 || boMinNATR > 20.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MIN_NATR_PCT '%s' (must be float between 0.0 and 20.0)", boMinNATRStr)
	}
	boMinVolStr := getEnv("BREAKOUT_MIN_VOLUME_24H_USDT", "0.0")
	boMinVol, err := parseFloatEnv(boMinVolStr)
	if err != nil || boMinVol < 0.0 || boMinVol > 1e12 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MIN_VOLUME_24H_USDT '%s' (must be float >= 0)", boMinVolStr)
	}
	boMaxWatchedStr := getEnv("BREAKOUT_MAX_WATCHED_COINS", "0")
	boMaxWatched, err := strconv.Atoi(boMaxWatchedStr)
	if err != nil || boMaxWatched < 0 || boMaxWatched > 300 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MAX_WATCHED_COINS '%s' (must be integer between 0 and 300)", boMaxWatchedStr)
	}
	boExclTopStr := getEnv("BREAKOUT_EXCLUDE_TOP_VOL_RANK", "-1")
	boExclTop, err := strconv.Atoi(boExclTopStr)
	if err != nil || boExclTop < -1 || boExclTop > 100 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_EXCLUDE_TOP_VOL_RANK '%s' (must be integer between -1 and 100)", boExclTopStr)
	}
	boMaxScanVWAPStr := getEnv("BREAKOUT_MAX_SCANNER_VWAP_DIST_ATR", "0.0")
	boMaxScanVWAP, err := parseFloatEnv(boMaxScanVWAPStr)
	if err != nil || boMaxScanVWAP < 0.0 || boMaxScanVWAP > 50.0 {
		return nil, fmt.Errorf("config: invalid BREAKOUT_MAX_SCANNER_VWAP_DIST_ATR '%s' (must be float between 0.0 and 50.0)", boMaxScanVWAPStr)
	}

	minBodyShortStr := getEnv("MIN_CANDLE_BODY_RATIO_SHORT", "0.0")
	minBodyShort, err := parseFloatEnv(minBodyShortStr)
	if err != nil || minBodyShort < 0.0 || minBodyShort > 1.0 {
		return nil, fmt.Errorf("config: invalid MIN_CANDLE_BODY_RATIO_SHORT '%s' (must be float between 0.0 and 1.0)", minBodyShortStr)
	}
	maxAdvRetestShortStr := getEnv("MAX_ADVERSE_RETEST_ATR_SHORT", "0.0")
	maxAdvRetestShort, err := parseFloatEnv(maxAdvRetestShortStr)
	if err != nil || maxAdvRetestShort < 0.0 || maxAdvRetestShort > 5.0 {
		return nil, fmt.Errorf("config: invalid MAX_ADVERSE_RETEST_ATR_SHORT '%s' (must be float between 0.0 and 5.0)", maxAdvRetestShortStr)
	}
	maxEMASlopeShortStr := getEnv("MAX_EMA_SLOPE_SHORT", "0.0")
	maxEMASlopeShort, err := parseFloatEnv(maxEMASlopeShortStr)
	if err != nil || maxEMASlopeShort < 0.0 || maxEMASlopeShort > 5.0 {
		return nil, fmt.Errorf("config: invalid MAX_EMA_SLOPE_SHORT '%s' (must be float between 0.0 and 5.0)", maxEMASlopeShortStr)
	}

	macroEnabled, err := parseBoolEnvStrict("MACRO_REGIME_ROUTER_ENABLED", getEnv("MACRO_REGIME_ROUTER_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	macroERStr := getEnv("MACRO_TREND_ER_THRESHOLD", "0.30")
	macroER, err := parseFloatEnv(macroERStr)
	if err != nil || macroER < 0.05 || macroER > 0.95 {
		return nil, fmt.Errorf("config: invalid MACRO_TREND_ER_THRESHOLD '%s' (must be float between 0.05 and 0.95)", macroERStr)
	}
	macroVolRatioStr := getEnv("MACRO_VOL_SHOCK_RATIO", "1.50")
	macroVolRatio, err := parseFloatEnv(macroVolRatioStr)
	if err != nil || macroVolRatio < 1.05 || macroVolRatio > 5.00 {
		return nil, fmt.Errorf("config: invalid MACRO_VOL_SHOCK_RATIO '%s' (must be float between 1.05 and 5.00)", macroVolRatioStr)
	}
	macroVWAPRange, err := parseBoolEnvStrict("MACRO_VWAP_ONLY_IN_RANGE", getEnv("MACRO_VWAP_ONLY_IN_RANGE", "false"))
	if err != nil {
		return nil, err
	}
	macroBOTrend, err := parseBoolEnvStrict("MACRO_BREAKOUT_ONLY_IN_TREND", getEnv("MACRO_BREAKOUT_ONLY_IN_TREND", "false"))
	if err != nil {
		return nil, err
	}
	macroRelRange, err := parseBoolEnvStrict("MACRO_REL_ONLY_IN_RANGE", getEnv("MACRO_REL_ONLY_IN_RANGE", "false"))
	if err != nil {
		return nil, err
	}

	maxAdvFundStr := getEnv("MAX_ADVERSE_FUNDING_RATE", "0.0005")
	maxAdvFund, err := parseFloatEnv(maxAdvFundStr)
	if err != nil || maxAdvFund < 0.0 || maxAdvFund > 0.05 {
		return nil, fmt.Errorf("config: invalid MAX_ADVERSE_FUNDING_RATE '%s' (must be float between 0.0 and 0.05)", maxAdvFundStr)
	}
	minShortFundStr := getEnv("MIN_SHORT_FUNDING_RATE", "-1.0")
	minShortFund, err := parseFloatEnv(minShortFundStr)
	if err != nil || minShortFund < -1.0 || minShortFund > 0.05 {
		return nil, fmt.Errorf("config: invalid MIN_SHORT_FUNDING_RATE '%s' (must be float between -1.0 and 0.05)", minShortFundStr)
	}
	maxLongFundStr := getEnv("MAX_LONG_FUNDING_RATE", "1.0")
	maxLongFund, err := parseFloatEnv(maxLongFundStr)
	if err != nil || maxLongFund < -0.05 || maxLongFund > 1.0 {
		return nil, fmt.Errorf("config: invalid MAX_LONG_FUNDING_RATE '%s' (must be float between -0.05 and 1.0)", maxLongFundStr)
	}
	rollingVWAPStr := getEnv("ROLLING_VWAP_BARS", "0")
	rollingVWAPBars, err := strconv.Atoi(rollingVWAPStr)
	if err != nil || rollingVWAPBars < 0 || rollingVWAPBars > 1000 {
		return nil, fmt.Errorf("config: invalid ROLLING_VWAP_BARS '%s' (must be integer between 0 and 1000)", rollingVWAPStr)
	}
	maxEntriesBarStr := getEnv("MAX_NEW_ENTRIES_PER_BAR", "0")
	maxEntriesBar, err := strconv.Atoi(maxEntriesBarStr)
	if err != nil || maxEntriesBar < 0 || maxEntriesBar > 50 {
		return nil, fmt.Errorf("config: invalid MAX_NEW_ENTRIES_PER_BAR '%s' (must be integer between 0 and 50)", maxEntriesBarStr)
	}
	clusterSLCntStr := getEnv("PORTFOLIO_CLUSTER_SL_COUNT", "0")
	clusterSLCnt, err := strconv.Atoi(clusterSLCntStr)
	if err != nil || clusterSLCnt < 0 || clusterSLCnt > 20 {
		return nil, fmt.Errorf("config: invalid PORTFOLIO_CLUSTER_SL_COUNT '%s' (must be integer between 0 and 20)", clusterSLCntStr)
	}
	clusterCoolStr := getEnv("PORTFOLIO_CLUSTER_COOLDOWN_MINUTES", "0")
	clusterCool, err := strconv.Atoi(clusterCoolStr)
	if err != nil || clusterCool < 0 || clusterCool > 1440 {
		return nil, fmt.Errorf("config: invalid PORTFOLIO_CLUSTER_COOLDOWN_MINUTES '%s' (must be integer between 0 and 1440)", clusterCoolStr)
	}
	exclTopVolStr := getEnv("SCANNER_EXCLUDE_TOP_VOL_RANK", "0")
	exclTopVol, err := strconv.Atoi(exclTopVolStr)
	if err != nil || exclTopVol < 0 || exclTopVol > 100 {
		return nil, fmt.Errorf("config: invalid SCANNER_EXCLUDE_TOP_VOL_RANK '%s' (must be integer between 0 and 100)", exclTopVolStr)
	}
	htfEMAStr := getEnv("HTF_EMA_PERIOD", "0")
	htfEMA, err := strconv.Atoi(htfEMAStr)
	if err != nil || htfEMA < 0 || htfEMA > 500 {
		return nil, fmt.Errorf("config: invalid HTF_EMA_PERIOD '%s' (must be integer between 0 and 500)", htfEMAStr)
	}
	breadthThreshStr := getEnv("MARKET_BREADTH_LOW_THRESHOLD", "0.0")
	breadthThresh, err := parseFloatEnv(breadthThreshStr)
	if err != nil || breadthThresh < 0.0 || breadthThresh > 0.95 {
		return nil, fmt.Errorf("config: invalid MARKET_BREADTH_LOW_THRESHOLD '%s' (must be float between 0.0 and 0.95)", breadthThreshStr)
	}
	breadthMultStr := getEnv("MARKET_BREADTH_LOW_RISK_MULT", "1.0")
	breadthMult, err := parseFloatEnv(breadthMultStr)
	if err != nil || breadthMult < 0.0 || breadthMult > 1.0 {
		return nil, fmt.Errorf("config: invalid MARKET_BREADTH_LOW_RISK_MULT '%s' (must be float between 0.0 and 1.0)", breadthMultStr)
	}
	var staleLossDur time.Duration
	if staleLossStr := strings.TrimSpace(getEnv("STALE_LOSS_EXIT_DURATION", "")); staleLossStr != "" && staleLossStr != "0" {
		d, dErr := time.ParseDuration(staleLossStr)
		if dErr != nil || (d != 0 && d < 15*time.Minute) {
			return nil, fmt.Errorf("config: invalid STALE_LOSS_EXIT_DURATION '%s' (must be duration >= 15m or empty/0)", staleLossStr)
		}
		staleLossDur = d
	}
	macroRangeMultStr := getEnv("MACRO_RANGE_RISK_MULT", "1.0")
	macroRangeMult, err := parseFloatEnv(macroRangeMultStr)
	if err != nil || macroRangeMult < 0.05 || macroRangeMult > 1.0 {
		return nil, fmt.Errorf("config: invalid MACRO_RANGE_RISK_MULT '%s' (must be float between 0.05 and 1.0)", macroRangeMultStr)
	}
	boPriorityFirst, err := parseBoolEnvStrict("BREAKOUT_PRIORITY_FIRST", getEnv("BREAKOUT_PRIORITY_FIRST", "false"))
	if err != nil {
		return nil, err
	}
	htfMaxSlopeStr := getEnv("HTF_EMA_MAX_RISING_SLOPE_PCT", "0.0")
	htfMaxSlope, err := parseFloatEnv(htfMaxSlopeStr)
	if err != nil || htfMaxSlope < 0.0 || htfMaxSlope > 10.0 {
		return nil, fmt.Errorf("config: invalid HTF_EMA_MAX_RISING_SLOPE_PCT '%s' (must be float between 0.0 and 10.0)", htfMaxSlopeStr)
	}
	ddThrottleThreshStr := getEnv("DD_THROTTLE_THRESHOLD_PCT", "0.0")
	ddThrottleThresh, err := parseFloatEnv(ddThrottleThreshStr)
	if err != nil || ddThrottleThresh < 0.0 || ddThrottleThresh > 0.80 {
		return nil, fmt.Errorf("config: invalid DD_THROTTLE_THRESHOLD_PCT '%s' (must be float between 0.0 and 0.80)", ddThrottleThreshStr)
	}
	ddThrottleMultStr := getEnv("DD_THROTTLE_RISK_MULT", "1.0")
	ddThrottleMult, err := parseFloatEnv(ddThrottleMultStr)
	if err != nil || ddThrottleMult < 0.10 || ddThrottleMult > 1.0 {
		return nil, fmt.Errorf("config: invalid DD_THROTTLE_RISK_MULT '%s' (must be float between 0.10 and 1.0)", ddThrottleMultStr)
	}
	resetPeakOnBoot, err := parseBoolEnvStrict("RESET_PEAK_EQUITY_ON_BOOT", getEnv("RESET_PEAK_EQUITY_ON_BOOT", "false"))
	if err != nil {
		return nil, err
	}
	macroVolExpRatio, err := parseFloatEnv(getEnv("MACRO_VOL_EXPANSION_RATIO", "0.0"))
	if err != nil || macroVolExpRatio < 0.0 || macroVolExpRatio > 5.0 {
		return nil, fmt.Errorf("config: invalid MACRO_VOL_EXPANSION_RATIO")
	}
	macroVolExpMult, err := parseFloatEnv(getEnv("MACRO_VOL_EXPANSION_RISK_MULT", "1.0"))
	if err != nil || macroVolExpMult < 0.1 || macroVolExpMult > 3.0 {
		return nil, fmt.Errorf("config: invalid MACRO_VOL_EXPANSION_RISK_MULT")
	}
	rs4hWeakThresh, err := parseFloatEnv(getEnv("RS4H_WEAK_THRESHOLD_PCT", "0.0"))
	if err != nil || rs4hWeakThresh < -50.0 || rs4hWeakThresh > 0.0 {
		return nil, fmt.Errorf("config: invalid RS4H_WEAK_THRESHOLD_PCT")
	}
	rs4hWeakMult, err := parseFloatEnv(getEnv("RS4H_WEAK_RISK_MULT", "1.0"))
	if err != nil || rs4hWeakMult < 0.1 || rs4hWeakMult > 3.0 {
		return nil, fmt.Errorf("config: invalid RS4H_WEAK_RISK_MULT")
	}
	rs4hStrongThresh, err := parseFloatEnv(getEnv("RS4H_STRONG_THRESHOLD_PCT", "0.0"))
	if err != nil || rs4hStrongThresh < 0.0 || rs4hStrongThresh > 50.0 {
		return nil, fmt.Errorf("config: invalid RS4H_STRONG_THRESHOLD_PCT")
	}
	rs4hStrongMult, err := parseFloatEnv(getEnv("RS4H_STRONG_RISK_MULT", "1.0"))
	if err != nil || rs4hStrongMult < 0.0 || rs4hStrongMult > 3.0 {
		return nil, fmt.Errorf("config: invalid RS4H_STRONG_RISK_MULT")
	}
	ddTier2Thresh, err := parseFloatEnv(getEnv("DD_THROTTLE_TIER2_THRESHOLD_PCT", "0.0"))
	if err != nil || ddTier2Thresh < 0.0 || ddTier2Thresh > 0.90 {
		return nil, fmt.Errorf("config: invalid DD_THROTTLE_TIER2_THRESHOLD_PCT")
	}
	ddTier2Mult, err := parseFloatEnv(getEnv("DD_THROTTLE_TIER2_RISK_MULT", "1.0"))
	if err != nil || ddTier2Mult < 0.1 || ddTier2Mult > 1.0 {
		return nil, fmt.Errorf("config: invalid DD_THROTTLE_TIER2_RISK_MULT")
	}
	maxEffRiskPct, err := parseFloatEnv(getEnv("MAX_EFFECTIVE_RISK_PER_TRADE_PCT", "0.0"))
	if err != nil || maxEffRiskPct < 0.0 || maxEffRiskPct > 0.20 {
		return nil, fmt.Errorf("config: invalid MAX_EFFECTIVE_RISK_PER_TRADE_PCT")
	}

	emaPullEnabled, err := parseBoolEnvStrict("EMA_PULLBACK_ENABLED", getEnv("EMA_PULLBACK_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	emaPullFast, err := strconv.Atoi(getEnv("EMA_PULLBACK_FAST", "20"))
	if err != nil || emaPullFast < 5 || emaPullFast > 100 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_FAST")
	}
	emaPullSlow, err := strconv.Atoi(getEnv("EMA_PULLBACK_SLOW", "50"))
	if err != nil || emaPullSlow <= emaPullFast || emaPullSlow > 200 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_SLOW")
	}
	emaPullRetest, err := strconv.Atoi(getEnv("EMA_PULLBACK_RETEST_BARS", "3"))
	if err != nil || emaPullRetest < 1 || emaPullRetest > 15 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_RETEST_BARS")
	}
	emaPullTouchATR, err := parseFloatEnv(getEnv("EMA_PULLBACK_TOUCH_ATR", "0.30"))
	if err != nil || emaPullTouchATR < 0.0 || emaPullTouchATR > 3.0 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_TOUCH_ATR")
	}
	emaPullMinBody, err := parseFloatEnv(getEnv("EMA_PULLBACK_MIN_BODY", "0.35"))
	if err != nil || emaPullMinBody < 0.0 || emaPullMinBody > 1.0 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_MIN_BODY")
	}
	emaPullMinDeltaZ, err := parseFloatEnv(getEnv("EMA_PULLBACK_MIN_DELTA_Z", "0.65"))
	if err != nil || emaPullMinDeltaZ < 0.0 || emaPullMinDeltaZ > 5.0 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_MIN_DELTA_Z")
	}
	emaPullMaxEMA200, err := parseFloatEnv(getEnv("EMA_PULLBACK_MAX_EMA200_ATR", "5.0"))
	if err != nil || emaPullMaxEMA200 < 0.5 || emaPullMaxEMA200 > 20.0 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_MAX_EMA200_ATR")
	}
	emaPullRiskMult, err := parseFloatEnv(getEnv("EMA_PULLBACK_RISK_MULT", "1.0"))
	if err != nil || emaPullRiskMult < 0.05 || emaPullRiskMult > 5.0 {
		return nil, fmt.Errorf("config: invalid EMA_PULLBACK_RISK_MULT")
	}

	vwapBrkEnabled, err := parseBoolEnvStrict("VWAP_BREAKDOWN_ENABLED", getEnv("VWAP_BREAKDOWN_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	vwapBrkLookback, err := strconv.Atoi(getEnv("VWAP_BREAKDOWN_LOOKBACK", "6"))
	if err != nil || vwapBrkLookback < 2 || vwapBrkLookback > 30 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_LOOKBACK")
	}
	vwapBrkMinAbove, err := strconv.Atoi(getEnv("VWAP_BREAKDOWN_MIN_ABOVE", "2"))
	if err != nil || vwapBrkMinAbove < 1 || vwapBrkMinAbove > 20 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_MIN_ABOVE")
	}
	vwapBrkMinRVOL, err := parseFloatEnv(getEnv("VWAP_BREAKDOWN_MIN_RVOL", "1.15"))
	if err != nil || vwapBrkMinRVOL < 0.1 || vwapBrkMinRVOL > 10.0 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_MIN_RVOL")
	}
	vwapBrkMinBody, err := parseFloatEnv(getEnv("VWAP_BREAKDOWN_MIN_BODY", "0.45"))
	if err != nil || vwapBrkMinBody < 0.0 || vwapBrkMinBody > 1.0 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_MIN_BODY")
	}
	vwapBrkMinDeltaZ, err := parseFloatEnv(getEnv("VWAP_BREAKDOWN_MIN_DELTA_Z", "0.65"))
	if err != nil || vwapBrkMinDeltaZ < 0.0 || vwapBrkMinDeltaZ > 5.0 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_MIN_DELTA_Z")
	}
	vwapBrkMaxBelow, err := parseFloatEnv(getEnv("VWAP_BREAKDOWN_MAX_BELOW_ATR", "1.50"))
	if err != nil || vwapBrkMaxBelow < 0.1 || vwapBrkMaxBelow > 10.0 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_MAX_BELOW_ATR")
	}
	vwapBrkRiskMult, err := parseFloatEnv(getEnv("VWAP_BREAKDOWN_RISK_MULT", "1.0"))
	if err != nil || vwapBrkRiskMult < 0.05 || vwapBrkRiskMult > 5.0 {
		return nil, fmt.Errorf("config: invalid VWAP_BREAKDOWN_RISK_MULT")
	}

	structBrkEnabled, err := parseBoolEnvStrict("STRUCTURE_BREAK_ENABLED", getEnv("STRUCTURE_BREAK_ENABLED", "false"))
	if err != nil {
		return nil, err
	}
	structBrkLookback, err := strconv.Atoi(getEnv("STRUCTURE_BREAK_LOOKBACK", "20"))
	if err != nil || structBrkLookback < 6 || structBrkLookback > 60 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_LOOKBACK")
	}
	structBrkMinBounce, err := parseFloatEnv(getEnv("STRUCTURE_BREAK_MIN_BOUNCE_ATR", "0.80"))
	if err != nil || structBrkMinBounce < 0.1 || structBrkMinBounce > 10.0 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_MIN_BOUNCE_ATR")
	}
	structBrkMinRVOL, err := parseFloatEnv(getEnv("STRUCTURE_BREAK_MIN_RVOL", "1.15"))
	if err != nil || structBrkMinRVOL < 0.1 || structBrkMinRVOL > 10.0 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_MIN_RVOL")
	}
	structBrkMinBody, err := parseFloatEnv(getEnv("STRUCTURE_BREAK_MIN_BODY", "0.45"))
	if err != nil || structBrkMinBody < 0.0 || structBrkMinBody > 1.0 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_MIN_BODY")
	}
	structBrkMinDeltaZ, err := parseFloatEnv(getEnv("STRUCTURE_BREAK_MIN_DELTA_Z", "0.60"))
	if err != nil || structBrkMinDeltaZ < 0.0 || structBrkMinDeltaZ > 5.0 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_MIN_DELTA_Z")
	}
	structBrkMaxEMA200, err := parseFloatEnv(getEnv("STRUCTURE_BREAK_MAX_EMA200_ATR", "4.50"))
	if err != nil || structBrkMaxEMA200 < 0.5 || structBrkMaxEMA200 > 20.0 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_MAX_EMA200_ATR")
	}
	structBrkRiskMult, err := parseFloatEnv(getEnv("STRUCTURE_BREAK_RISK_MULT", "1.0"))
	if err != nil || structBrkRiskMult < 0.05 || structBrkRiskMult > 5.0 {
		return nil, fmt.Errorf("config: invalid STRUCTURE_BREAK_RISK_MULT")
	}

	return &Config{
		Env:                      env,
		APIKey:                   apiKey,
		SecretKey:                secretKey,
		DryRun:                   dryRun,
		DatabasePath:             dbPath,
		LogLevel:                 logLevel,
		HTTPPort:                 httpPort,
		EntryOrderType:           entryOrderType,
		RestBaseURL:              restURL,
		WSPublicURL:              wsPublic,
		WSMarketURL:              wsMarket,
		WSPrivateBaseURL:         wsPrivate,
		WSAPIOrderURL:            wsAPI,
		MakerFeeRate:             makerFee,
		TakerFeeRate:             takerFee,
		SlippageRate:             slippage,
		InitialEquity:            initEq,
		RiskPerTradePct:          riskPct,
		MaxDailyDrawdown:         ddFloat,
		MaxSlots:                 slots,
		MaxSlotMarginFrac:        marginFrac,
		MaxMarginAvailFrac:       availMarginFrac,
		Leverage:                 lev,
		EquityModeCompounding:    compounding,
		MinVolume24hUSDT:         minVol,
		MinFallbackRVOL:          minFallbackRVOL,
		MinRVOL:                  minRVOL,
		ScannerRankingMode:       scannerRankingMode,
		MaxSpreadPct:             maxSpread,
		MinNATRPct:               minNATR,
		MaxNATRPct:               maxNATR,
		MaxScannerVWAPDistATR:    maxScannerVWAPDist,
		MaxWatchedCoins:          maxWatched,
		EMAPeriod:                emaPeriod,
		PullbackToleranceATR:     pullbackTol,
		MinBounceZScore:          minBounceZ,
		MinEMA200DistanceATR:     minEMA200Dist,
		MaxEMA200DistanceATR:     maxEMA200Dist,
		MinBounceZScoreLong:      minBounceZLong,
		MinBounceZScoreShort:     minBounceZShort,
		LookbackImpulseBars:      lookbackBars,
		RetestLookbackBars:       retestLookbackBars,
		MinImpulseATR:            minImpulseATR,
		MaxEntryDistanceATR:      maxEntryDist,
		MinOIDeltaPct:            minOIDelta,
		MinOIDeltaLongPct:        minOIDeltaLong,
		MinOIDeltaLongSet:        minOIDeltaLongSet,
		MinOIDeltaShortPct:       minOIDeltaShort,
		MinOIDeltaShortSet:       minOIDeltaShortSet,
		DisableOIFilter:          disableOIFilter,
		SLATRMultiplier:          slMult,
		TPATRMultiplier:          tpMult,
		BreakEvenTriggerATR:      beTrigger,
		Tier1BEEnabled:           tier1BEEnabled,
		Tier1BETriggerLongATR:    t1BETriggerLong,
		Tier1BETriggerShortATR:   t1BETriggerShort,
		Tier2ProfitLockEnabled:   tier2ProfitLockEnabled,
		Tier2ProfitLockATR:       t2ProfitLock,
		Tier2ProfitLockOffsetATR: t2Offset,
		MinSLDistancePct:         minSLDistPct,
		PartialTPEnabled:         partialTPEnabled,
		PartialTP1Ratio:          partialTP1Ratio,
		PartialTP1ATRMult:        partialTP1ATRMult,
		RunnerBECushionATR:       runnerBECushionATR,
		TrailingEnabled:          trailingEnabled,
		TrailingActivateATR:      trailingActivateATR,
		TrailingDistanceATR:      trailingDistATR,
		BTCGateEnabled:           btcGateEnabled,
		BTCMax5mDumpPct:          btcMaxDump,
		BTCMax5mPumpPct:          btcMaxPump,
		BTCGatePersistenceBars:   btcPersistenceBars,
		BTCCounterMomentumPct:    btcCounterMom,
		BTCChopRiskMult:          btcChopRiskMult,
		AllowedSide:              allowedSide,
		MaxFrenzyRVOL:            maxFrenzyRVOL,
		MaxCounterImpulseATR:     maxCounterImpulseATR,
		MinSignalRVOL:            minSignalRVOL,
		CooldownAfterExitMinutes: cooldownMins,
		CooldownLossMinutes:      cooldownLossMins,
		CooldownLossSet:          true,
		CooldownProfitMinutes:    cooldownProfitMins,
		CooldownProfitSet:        true,
		VWAPSessionWarmupMinutes: vwapWarmupMins,
		VWAPSessionWarmupSet:     true,
		BlockedUTCHours:          blockedHours,

		MinCandleBodyRatioLong:   minCandleBodyRatioLong,
		MinCandleBodyRatioShort:  minBodyShort,
		MaxAdverseRetestATRShort: maxAdvRetestShort,
		MaxEMASlopeShort:         maxEMASlopeShort,
		MaxPullbackRVOLLong:      maxPullbackRVOLLong,
		MinImpulseATRLong:        minImpulseATRLong,
		MinEMASlopeLong:          minEMASlopeLong,
		MinEMASlopeLongSet:       minEMASlopeLongSet,
		RequireCleanPullbackLong: requireCleanPullbackLong,
		PartialTP1ATRMultLong:      partialTP1ATRMultLong,
		SLATRMultiplierLong:        slMultLong,
		MinNATRLongPct:             minNATRLong,
		BTCChopBlockLong:           btcChopBlockLong,
		BTCChopBlockShort:          btcChopBlockShort,
		DisablePanicDumpFrenzyLong: disablePanicDumpFrenzyLong,
		BTCTier2LongSymmetric:      btcTier2LongSymmetric,

		Timeframe:            timeframeStr,
		CandleDuration:       candleDur,

		ScannerInterval:      scannerInterval,
		MaxBookStaleness:     5 * time.Second,
		MaxMarkStaleness:     15 * time.Second,
		MaxDataStaleness:     maxDataStaleness,
		MaxAccountStale:      60 * time.Second,
		MinOIInterval:        1 * time.Minute,
		MaxOIInterval:        maxOIInterval,
		MaxOIStaleness:       maxOIStaleness,
		LimitOrderTimeout:     limitOrderTimeout,
		MaxHoldingDuration:    maxHoldingDuration,
		EntryRepegMaxAttempts: entryRepegMaxAttempts,
		EntryRepegMaxDriftPct: entryRepegMaxDriftPct,
		OrphanPositionPolicy:  orphanPolicy,
		RecvWindowMS:          recvWin,

		DashboardPIN:     dashPIN,
		GeminiAPIKey:     geminiKey,
		GeminiModel:      geminiModel,
		GeminiTimeoutSec: geminiTimeout,
		AIRateLimitRPM:   aiRateLimit,

		BTCGateShortOnly:    btcGateShortOnly,
		SLGapFillEnabled:    slGapFillEnabled,
		DeductPendingMargin: deductPendingMargin,

		Sweep: SweepStrategyConfig{
			Enabled:                  sweepEnabled,
			LookbackBars:             sweepLookback,
			MinUpperWickRatio:        sweepMinUpperWick,
			MaxLowerWickRatio:        sweepMaxLowerWick,
			MaxDeltaZScore:           sweepMaxDeltaZ,
			ScannerAllowExtendedVWAP: sweepAllowExtVWAP,
			ScannerMaxExtraCoins:     sweepMaxExtra,
		},
		VWAPStrategyEnabled: vwapStrategyEnabled,
		RelResidual: RelResidualStrategyConfig{
			Enabled:           relEnabled,
			Mode:              relMode,
			LookbackBars:      relLookback,
			MinRelZ:           relMinZ,
			MinVWAPStretchATR: relStretch,
			MinDeltaZ:         relDeltaZ,
			BypassBTCTier2:    relBypassTier2,
		},
		VolBreakout: VolBreakoutStrategyConfig{
			Enabled:               boEnabled,
			LookbackBars:          boLookback,
			MaxCompressionRatio:   boMaxComp,
			MinRVOL:               boMinRVOL,
			MinBodyRatio:          boMinBody,
			MinDeltaZ:             boMinDeltaZ,
			AlignMacroTrend:       boAlignMacroTrend,
			MaxEMA200DistATR:      boMaxEMA200Dist,
			MaxVWAPDistATR:        boMaxVWAPDist,
			MaxNATRPct:            boMaxNATR,
			RiskMult:              boRiskMult,
			EntryMarket:           boEntryMarket,
			ScannerEnabled:        boScannerEnabled,
			MinNATRPct:            boMinNATR,
			MinVolume24hUSDT:      boMinVol,
			MaxWatchedCoins:       boMaxWatched,
			ExcludeTopVolRank:     boExclTop,
			MaxScannerVWAPDistATR: boMaxScanVWAP,
		},
		MacroRouter: MacroRegimeRouterConfig{
			Enabled:             macroEnabled,
			TrendERThreshold:    macroER,
			VolShockRatio:       macroVolRatio,
			VWAPOnlyInRange:     macroVWAPRange,
			BreakoutOnlyInTrend: macroBOTrend,
			RelOnlyInRange:      macroRelRange,
		},
		MaxAdverseFundingRate:        maxAdvFund,
		MinShortFundingRate:          minShortFund,
		MaxLongFundingRate:           maxLongFund,
		RollingVWAPBars:              rollingVWAPBars,
		MaxNewEntriesPerBar:          maxEntriesBar,
		PortfolioClusterSLCount:      clusterSLCnt,
		PortfolioClusterCooldownMins: clusterCool,
		ScannerExcludeTopVolRank:     exclTopVol,
		HTFEMAPeriod:                 htfEMA,
		MarketBreadthLowThreshold:    breadthThresh,
		MarketBreadthLowRiskMult:     breadthMult,
		StaleLossExitDuration:        staleLossDur,
		MacroRangeRiskMult:           macroRangeMult,
		BreakoutPriorityFirst:        boPriorityFirst,
		HTFEMAMaxRisingSlopePct:      htfMaxSlope,
		DDThrottleThresholdPct:       ddThrottleThresh,
		DDThrottleRiskMult:           ddThrottleMult,
		ResetPeakEquityOnBoot:        resetPeakOnBoot,
		MacroVolExpansionRatio:       macroVolExpRatio,
		MacroVolExpansionRiskMult:    macroVolExpMult,
		RS4hWeakThresholdPct:         rs4hWeakThresh,
		RS4hWeakRiskMult:             rs4hWeakMult,
		RS4hStrongThresholdPct:       rs4hStrongThresh,
		RS4hStrongRiskMult:           rs4hStrongMult,
		DDThrottleTier2ThresholdPct:  ddTier2Thresh,
		DDThrottleTier2RiskMult:      ddTier2Mult,
		MaxEffectiveRiskPerTradePct:  maxEffRiskPct,
		EMAPullback: EMAPullbackStrategyConfig{
			Enabled:          emaPullEnabled,
			FastPeriod:       emaPullFast,
			SlowPeriod:       emaPullSlow,
			RetestLookback:   emaPullRetest,
			TouchTolATR:      emaPullTouchATR,
			TouchTolSet:      true,
			MinBodyRatio:     emaPullMinBody,
			MinBodySet:       true,
			MinDeltaZ:        emaPullMinDeltaZ,
			MaxEMA200DistATR: emaPullMaxEMA200,
			RiskMult:         emaPullRiskMult,
		},
		VWAPBreakdown: VWAPBreakdownStrategyConfig{
			Enabled:         vwapBrkEnabled,
			LookbackBars:    vwapBrkLookback,
			MinAboveBars:    vwapBrkMinAbove,
			MinRVOL:         vwapBrkMinRVOL,
			MinBodyRatio:    vwapBrkMinBody,
			MinBodySet:      true,
			MinDeltaZ:       vwapBrkMinDeltaZ,
			MaxBelowVWAPATR: vwapBrkMaxBelow,
			RiskMult:        vwapBrkRiskMult,
		},
		StructureBreak: StructureBreakStrategyConfig{
			Enabled:          structBrkEnabled,
			LookbackBars:     structBrkLookback,
			MinBounceATR:     structBrkMinBounce,
			MinRVOL:          structBrkMinRVOL,
			MinBodyRatio:     structBrkMinBody,
			MinBodySet:       true,
			MinDeltaZ:        structBrkMinDeltaZ,
			MaxEMA200DistATR: structBrkMaxEMA200,
			RiskMult:         structBrkRiskMult,
		},
	}, nil
}

func getRawEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		val = strings.TrimSpace(val)
		if val != "" {
			return val
		}
	}
	return defaultVal
}

func cleanEnvValue(val string) string {
	val = strings.TrimSpace(val)
	if idx := strings.Index(val, "#"); idx != -1 {
		val = strings.TrimSpace(val[:idx])
	}
	return val
}

func lookupCleanEnv(key string) (string, bool) {
	raw, exists := os.LookupEnv(key)
	if !exists {
		return "", false
	}
	clean := cleanEnvValue(raw)
	if clean == "" {
		return "", false
	}
	return clean, true
}

func getEnv(key, defaultVal string) string {
	if val, ok := lookupCleanEnv(key); ok {
		return val
	}
	return defaultVal
}

func parseBoolEnvStrict(key, val string) (bool, error) {
	clean := strings.ToLower(strings.TrimSpace(val))
	switch clean {
	case "true", "1", "yes", "y":
		return true, nil
	case "false", "0", "no", "n":
		return false, nil
	default:
		return false, fmt.Errorf("config: invalid boolean for %s '%s' (expected true/false)", key, val)
	}
}

func parseFloatEnv(val string) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("value cannot be NaN or Inf")
	}
	return v, nil
}

// ParseTimeframeToDuration mengonversi string timeframe Binance (e.g. "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d")
// menjadi time.Duration yang ekuivalen.
func ParseTimeframeToDuration(tf string) (time.Duration, error) {
	tf = strings.ToLower(strings.TrimSpace(tf))
	switch tf {
	case "1m":
		return 1 * time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return 1 * time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid TIMEFRAME '%s' (allowed: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 1d)", tf)
	}
}
