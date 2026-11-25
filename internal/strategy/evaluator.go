package strategy

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/config"
	"trml/internal/indicator"
	"trml/internal/model"
)

type EvaluatorConfig struct {
	CandleDuration        time.Duration
	MaxDailyDrawdownPct   float64
	MaxSlots              int
	MaxBookStaleness      time.Duration
	MaxMarkStaleness      time.Duration
	MaxDataStaleness      time.Duration
	MinOIInterval         time.Duration
	MaxOIInterval         time.Duration
	MaxOIStaleness        time.Duration
	MaxSpreadPct          float64
	MinNATRPct            float64
	MaxNATRPct            float64
	EMAPeriod             int
	ATRPeriod             int
	DeltaZScoreWindow     int
	MinBarDeltaZScore     float64
	MinOIDeltaPct         float64
	MinOIDeltaLongPct     float64
	MinOIDeltaLongSet     bool
	MinOIDeltaShortPct    float64
	MinOIDeltaShortSet    bool
	DisableOIFilter       bool
	MaxAdverseFundingRate float64

	// Parameter Strategi VWAP Pullback / Retest with Orderflow Bounce
	PullbackToleranceATR float64
	MinBounceZScore      float64
	MinBounceZScoreLong  float64
	MinBounceZScoreShort float64
	MinEMA200DistanceATR float64
	MaxEMA200DistanceATR float64
	LookbackImpulseBars  int
	RetestLookbackBars   int
	MinImpulseATR        float64
	MaxEntryDistanceATR  float64
	EntryOrderType       string

	// Option C: Optimasi Seleksi Entry Sisi LONG & SHORT
	MinCandleBodyRatioLong     float64
	MinCandleBodyRatioShort    float64
	MaxAdverseRetestATRShort   float64
	MaxEMASlopeShort           float64
	MaxPullbackRVOLLong        float64
	MinImpulseATRLong          float64
	MinEMASlopeLong            float64
	MinEMASlopeLongSet         bool
	RequireCleanPullbackLong   bool
	MinNATRLongPct             float64
	BTCChopBlockLong           bool
	BTCChopBlockShort          bool
	DisablePanicDumpFrenzyLong bool
	BTCTier2LongSymmetric      bool

	// Gerbang Makro Bitcoin (Two-Tiered Soft Veto)
	BTCGateEnabled         bool
	BTCGateShortOnly       bool
	BTCMax5mDumpPct        float64
	BTCMax5mPumpPct        float64
	BTCGatePersistenceBars int
	BTCCounterMomentumPct  float64
	BTCChopRiskMult        float64 // Optional risk multiplier when BTC macro regime is choppy/high-vol (1.0 = disabled)
	AllowedSide            string
	MaxFrenzyRVOL          float64
	MaxCounterImpulseATR   float64
	MinSignalRVOL          float64

	CooldownAfterExitMinutes int
	CooldownLossMinutes      int
	CooldownLossSet          bool
	CooldownProfitMinutes    int
	CooldownProfitSet        bool
	VWAPSessionWarmupMinutes int
	VWAPSessionWarmupSet     bool
	BlockedUTCHours          map[int]bool // Optional UTC hours [0..23] to skip new entries

	// Random Entry Benchmark (Case C)
	RandomEntrySeed int64
	RandomEntryProb float64

	// Secondary Strategy: 1-Bar Liquidity Sweep SHORT-ONLY
	Sweep SweepConfig

	// Multi-Regime Strategy Suite (all opt-in; DisableVWAP defaults to false so zero-value preserves VWAP)
	DisableVWAP     bool
	RelResidual     RelResidualConfig
	VolBreakout     VolBreakoutConfig
	EMAPullback     EMAPullbackConfig
	VWAPBreakdown   VWAPBreakdownConfig
	StructureBreak  StructureBreakConfig
	MacroRouter     MacroRegimeRouterConfig
	FundingEnvelope           FundingEnvelopeConfig
	RollingVWAPBars           int
	HTFEMAPeriod              int
	MarketBreadthLowThreshold float64
	MarketBreadthLowRiskMult  float64
	BreakoutPriorityFirst     bool
	HTFEMAMaxRisingSlopePct   float64
	DDThrottleThresholdPct      float64
	DDThrottleRiskMult          float64
	RS4hWeakThresholdPct        float64
	RS4hWeakRiskMult            float64
	RS4hStrongThresholdPct      float64
	RS4hStrongRiskMult          float64
	DDThrottleTier2ThresholdPct float64
	DDThrottleTier2RiskMult     float64
	MaxEffectiveRiskPerTradePct float64

	Sizing SizingConfig
}

// FromAppConfig builds a unified EvaluatorConfig from application Config (SSOT for cmd/bot & runner).
func FromAppConfig(cfg *config.Config) EvaluatorConfig {
	if cfg == nil {
		return EvaluatorConfig{}
	}
	return EvaluatorConfig{
		CandleDuration:             cfg.CandleDuration,
		MaxDailyDrawdownPct:        cfg.MaxDailyDrawdown,
		MaxSlots:                   cfg.MaxSlots,
		MaxBookStaleness:           cfg.MaxBookStaleness,
		MaxMarkStaleness:           cfg.MaxMarkStaleness,
		MaxDataStaleness:           cfg.MaxDataStaleness,
		MinOIInterval:              cfg.MinOIInterval,
		MaxOIInterval:              cfg.MaxOIInterval,
		MaxOIStaleness:             cfg.MaxOIStaleness,
		MaxSpreadPct:               cfg.MaxSpreadPct,
		MinNATRPct:                 cfg.MinNATRPct,
		MaxNATRPct:                 cfg.MaxNATRPct,
		EMAPeriod:                  cfg.EMAPeriod,
		ATRPeriod:                  14,
		DeltaZScoreWindow:          50,
		MinBarDeltaZScore:          cfg.MinBounceZScore,
		MinOIDeltaPct:              cfg.MinOIDeltaPct,
		MinOIDeltaLongPct:          cfg.MinOIDeltaLongPct,
		MinOIDeltaLongSet:          cfg.MinOIDeltaLongSet,
		MinOIDeltaShortPct:         cfg.MinOIDeltaShortPct,
		MinOIDeltaShortSet:         cfg.MinOIDeltaShortSet,
		MaxAdverseFundingRate:      cfg.MaxAdverseFundingRate,
		PullbackToleranceATR:       cfg.PullbackToleranceATR,
		MinBounceZScore:            cfg.MinBounceZScore,
		MinBounceZScoreLong:        cfg.MinBounceZScoreLong,
		MinBounceZScoreShort:       cfg.MinBounceZScoreShort,
		MinEMA200DistanceATR:       cfg.MinEMA200DistanceATR,
		MaxEMA200DistanceATR:       cfg.MaxEMA200DistanceATR,
		LookbackImpulseBars:        cfg.LookbackImpulseBars,
		RetestLookbackBars:         cfg.RetestLookbackBars,
		MinImpulseATR:              cfg.MinImpulseATR,
		MaxEntryDistanceATR:        cfg.MaxEntryDistanceATR,
		EntryOrderType:             cfg.EntryOrderType,
		DisableOIFilter:            cfg.DisableOIFilter,
		MinCandleBodyRatioLong:     cfg.MinCandleBodyRatioLong,
		MinCandleBodyRatioShort:    cfg.MinCandleBodyRatioShort,
		MaxAdverseRetestATRShort:   cfg.MaxAdverseRetestATRShort,
		MaxEMASlopeShort:           cfg.MaxEMASlopeShort,
		MaxPullbackRVOLLong:        cfg.MaxPullbackRVOLLong,
		MinImpulseATRLong:          cfg.MinImpulseATRLong,
		MinEMASlopeLong:            cfg.MinEMASlopeLong,
		MinEMASlopeLongSet:         cfg.MinEMASlopeLongSet,
		RequireCleanPullbackLong:   cfg.RequireCleanPullbackLong,
		MinNATRLongPct:             cfg.MinNATRLongPct,
		BTCChopBlockLong:           cfg.BTCChopBlockLong,
		BTCChopBlockShort:          cfg.BTCChopBlockShort,
		DisablePanicDumpFrenzyLong: cfg.DisablePanicDumpFrenzyLong,
		BTCTier2LongSymmetric:      cfg.BTCTier2LongSymmetric,
		BTCGateEnabled:             cfg.BTCGateEnabled,
		BTCGateShortOnly:           cfg.BTCGateShortOnly,
		BTCMax5mDumpPct:            cfg.BTCMax5mDumpPct,
		BTCMax5mPumpPct:            cfg.BTCMax5mPumpPct,
		BTCGatePersistenceBars:     cfg.BTCGatePersistenceBars,
		BTCCounterMomentumPct:      cfg.BTCCounterMomentumPct,
		BTCChopRiskMult:            cfg.BTCChopRiskMult,
		AllowedSide:                cfg.AllowedSide,
		MaxFrenzyRVOL:              cfg.MaxFrenzyRVOL,
		MaxCounterImpulseATR:       cfg.MaxCounterImpulseATR,
		MinSignalRVOL:              cfg.MinSignalRVOL,
		CooldownAfterExitMinutes:   cfg.CooldownAfterExitMinutes,
		CooldownLossMinutes:        cfg.CooldownLossMinutes,
		CooldownLossSet:            cfg.CooldownLossSet,
		CooldownProfitMinutes:      cfg.CooldownProfitMinutes,
		CooldownProfitSet:          cfg.CooldownProfitSet,
		VWAPSessionWarmupMinutes:   cfg.VWAPSessionWarmupMinutes,
		VWAPSessionWarmupSet:       cfg.VWAPSessionWarmupSet,
		BlockedUTCHours:            cfg.BlockedUTCHours,
		Sweep: SweepConfig{
			Enabled:           cfg.Sweep.Enabled,
			LookbackBars:      cfg.Sweep.LookbackBars,
			MinUpperWickRatio: cfg.Sweep.MinUpperWickRatio,
			MaxLowerWickRatio: cfg.Sweep.MaxLowerWickRatio,
			MaxDeltaZScore:    cfg.Sweep.MaxDeltaZScore,
		},
		DisableVWAP: !cfg.VWAPStrategyEnabled,
		RelResidual: RelResidualConfig{
			Enabled:           cfg.RelResidual.Enabled,
			Mode:              cfg.RelResidual.Mode,
			LookbackBars:      cfg.RelResidual.LookbackBars,
			MinRelZ:           cfg.RelResidual.MinRelZ,
			MinVWAPStretchATR: cfg.RelResidual.MinVWAPStretchATR,
			MinDeltaZ:         cfg.RelResidual.MinDeltaZ,
			BypassBTCTier2:    cfg.RelResidual.BypassBTCTier2,
		},
		VolBreakout: VolBreakoutConfig{
			Enabled:             cfg.VolBreakout.Enabled,
			LookbackBars:        cfg.VolBreakout.LookbackBars,
			MaxCompressionRatio: cfg.VolBreakout.MaxCompressionRatio,
			MinRVOL:             cfg.VolBreakout.MinRVOL,
			MinBodyRatio:        cfg.VolBreakout.MinBodyRatio,
			MinDeltaZ:           cfg.VolBreakout.MinDeltaZ,
			AlignMacroTrend:     cfg.VolBreakout.AlignMacroTrend,
			MaxEMA200DistATR:    cfg.VolBreakout.MaxEMA200DistATR,
			MaxVWAPDistATR:      cfg.VolBreakout.MaxVWAPDistATR,
			MaxNATRPct:          cfg.VolBreakout.MaxNATRPct,
			MinNATRPct:          cfg.VolBreakout.MinNATRPct,
			RiskMult:            cfg.VolBreakout.RiskMult,
			EntryMarket:         cfg.VolBreakout.EntryMarket,
		},
		EMAPullback: EMAPullbackConfig{
			Enabled:          cfg.EMAPullback.Enabled,
			FastPeriod:       cfg.EMAPullback.FastPeriod,
			SlowPeriod:       cfg.EMAPullback.SlowPeriod,
			RetestLookback:   cfg.EMAPullback.RetestLookback,
			TouchTolATR:      cfg.EMAPullback.TouchTolATR,
			TouchTolSet:      cfg.EMAPullback.TouchTolSet,
			MinBodyRatio:     cfg.EMAPullback.MinBodyRatio,
			MinBodySet:       cfg.EMAPullback.MinBodySet,
			MinDeltaZ:        cfg.EMAPullback.MinDeltaZ,
			MaxEMA200DistATR: cfg.EMAPullback.MaxEMA200DistATR,
			RiskMult:         cfg.EMAPullback.RiskMult,
		},
		VWAPBreakdown: VWAPBreakdownConfig{
			Enabled:         cfg.VWAPBreakdown.Enabled,
			LookbackBars:    cfg.VWAPBreakdown.LookbackBars,
			MinAboveBars:    cfg.VWAPBreakdown.MinAboveBars,
			MinRVOL:         cfg.VWAPBreakdown.MinRVOL,
			MinBodyRatio:    cfg.VWAPBreakdown.MinBodyRatio,
			MinBodySet:      cfg.VWAPBreakdown.MinBodySet,
			MinDeltaZ:       cfg.VWAPBreakdown.MinDeltaZ,
			MaxBelowVWAPATR: cfg.VWAPBreakdown.MaxBelowVWAPATR,
			RiskMult:        cfg.VWAPBreakdown.RiskMult,
		},
		StructureBreak: StructureBreakConfig{
			Enabled:          cfg.StructureBreak.Enabled,
			LookbackBars:     cfg.StructureBreak.LookbackBars,
			MinBounceATR:     cfg.StructureBreak.MinBounceATR,
			MinRVOL:          cfg.StructureBreak.MinRVOL,
			MinBodyRatio:     cfg.StructureBreak.MinBodyRatio,
			MinBodySet:       cfg.StructureBreak.MinBodySet,
			MinDeltaZ:        cfg.StructureBreak.MinDeltaZ,
			MaxEMA200DistATR: cfg.StructureBreak.MaxEMA200DistATR,
			RiskMult:         cfg.StructureBreak.RiskMult,
		},
		MacroRouter: MacroRegimeRouterConfig{
			Enabled:              cfg.MacroRouter.Enabled,
			TrendERThreshold:     cfg.MacroRouter.TrendERThreshold,
			VolShockRatio:        cfg.MacroRouter.VolShockRatio,
			VWAPOnlyInRange:      cfg.MacroRouter.VWAPOnlyInRange,
			BreakoutOnlyInTrend:  cfg.MacroRouter.BreakoutOnlyInTrend,
			RelOnlyInRange:       cfg.MacroRouter.RelOnlyInRange,
			RangeRiskMult:        cfg.MacroRangeRiskMult,
			VolExpansionRatio:    cfg.MacroVolExpansionRatio,
			VolExpansionRiskMult: cfg.MacroVolExpansionRiskMult,
		},
		FundingEnvelope: FundingEnvelopeConfig{
			MinShortEnabled: cfg.MinShortFundingRate > -0.99,
			MinShortRate:    cfg.MinShortFundingRate,
			MaxLongEnabled:  cfg.MaxLongFundingRate < 0.99,
			MaxLongRate:     cfg.MaxLongFundingRate,
		},
		RollingVWAPBars:             cfg.RollingVWAPBars,
		HTFEMAPeriod:                cfg.HTFEMAPeriod,
		MarketBreadthLowThreshold:   cfg.MarketBreadthLowThreshold,
		MarketBreadthLowRiskMult:    cfg.MarketBreadthLowRiskMult,
		BreakoutPriorityFirst:       cfg.BreakoutPriorityFirst,
		HTFEMAMaxRisingSlopePct:     cfg.HTFEMAMaxRisingSlopePct,
		DDThrottleThresholdPct:      cfg.DDThrottleThresholdPct,
		DDThrottleRiskMult:          cfg.DDThrottleRiskMult,
		RS4hWeakThresholdPct:        cfg.RS4hWeakThresholdPct,
		RS4hWeakRiskMult:            cfg.RS4hWeakRiskMult,
		RS4hStrongThresholdPct:      cfg.RS4hStrongThresholdPct,
		RS4hStrongRiskMult:          cfg.RS4hStrongRiskMult,
		DDThrottleTier2ThresholdPct: cfg.DDThrottleTier2ThresholdPct,
		DDThrottleTier2RiskMult:     cfg.DDThrottleTier2RiskMult,
		MaxEffectiveRiskPerTradePct: cfg.MaxEffectiveRiskPerTradePct,
		Sizing: SizingConfig{
			RiskPerTradePct:                cfg.RiskPerTradePct,
			MaxSlotMarginFraction:          cfg.MaxSlotMarginFrac,
			AvailableBalanceBufferFraction: cfg.MaxMarginAvailFrac,
			AccountLeverage:                cfg.Leverage,
			SLMultiplier:                   cfg.SLATRMultiplier,
			SLMultiplierLong:               cfg.SLATRMultiplierLong,
			TPMultiplier:                   cfg.TPATRMultiplier,
			TP1Multiplier:                  cfg.PartialTP1ATRMult,
			TP1MultiplierLong:              cfg.PartialTP1ATRMultLong,
			MinSLDistancePct:               cfg.MinSLDistancePct,
			MakerFeeRate:                   cfg.MakerFeeRate,
			TakerFeeRate:                   cfg.TakerFeeRate,
			SlippageRate:                   cfg.SlippageRate,
			EntryOrderType:                 cfg.EntryOrderType,
			DisablePartialTP:               !cfg.PartialTPEnabled,
		},
	}
}

// VWAPPullbackAlpha implements the primary 5m VWAP Pullback / Retest with Orderflow Bounce strategy (L1).
type VWAPPullbackAlpha struct{}

// ID returns the canonical identifier of the primary VWAP Pullback strategy.
func (VWAPPullbackAlpha) ID() string {
	return StrategyIDVWAPPullback
}

// Evaluate checks the 5 primary VWAP Pullback/Retest confirmations using shared context.
func (VWAPPullbackAlpha) Evaluate(ctx sharedMarketContext, snap model.MarketSnapshot, cfg EvaluatorConfig) AlphaCandidate {
	latest := ctx.Latest
	ema200 := ctx.EMA200
	atr14 := ctx.ATR14
	vwap := ctx.VWAP

	if rej, ok := checkPrimaryNATRBounds(ctx, cfg, "VWAP"); !ok {
		return rej
	}

	// Filter Anti-Chasing: Batasi jarak maksimal penutupan candle dari garis VWAP
	maxEntryDistATR := cfg.MaxEntryDistanceATR
	if maxEntryDistATR <= 0 {
		maxEntryDistATR = 1.00
	}
	distFromVWAP := math.Abs(latest.Close - vwap)
	if distFromVWAP > maxEntryDistATR*atr14 {
		distATR := distFromVWAP / atr14
		return AlphaCandidate{
			Reason:    "ENTRY_TOO_FAR_FROM_VWAP",
			SubReason: fmt.Sprintf("DIST_%.2f_ATR_MAX_%.2f_ATR", distATR, maxEntryDistATR),
		}
	}

	// Filter Anti-Overextension: Batasi jarak maksimal penutupan candle dari EMA200 (Anti-Pucuk/Dasar)
	if cfg.MaxEMA200DistanceATR > 0 {
		distFromEMA := math.Abs(latest.Close - ema200)
		if distFromEMA > cfg.MaxEMA200DistanceATR*atr14 {
			distATR := distFromEMA / atr14
			return AlphaCandidate{
				Reason:    "ENTRY_TOO_FAR_FROM_EMA200",
				SubReason: fmt.Sprintf("DIST_%.2f_ATR_MAX_%.2f_ATR", distATR, cfg.MaxEMA200DistanceATR),
			}
		}
	}

	// Defaults jika konfigurasi belum terisi
	pullbackTol := cfg.PullbackToleranceATR
	if pullbackTol <= 0 {
		pullbackTol = 0.35
	}
	minBounceZ := cfg.MinBounceZScore
	if minBounceZ == 0 {
		minBounceZ = cfg.MinBarDeltaZScore
	}
	if minBounceZ == 0 {
		minBounceZ = 1.20
	}
	minBounceZLong := cfg.MinBounceZScoreLong
	if minBounceZLong == 0 {
		minBounceZLong = minBounceZ
	}
	minBounceZShort := cfg.MinBounceZScoreShort
	if minBounceZShort == 0 {
		minBounceZShort = minBounceZ
	}
	lookback := cfg.LookbackImpulseBars
	if lookback <= 0 {
		lookback = 18
	}
	minImpulseATR := cfg.MinImpulseATR
	if minImpulseATR <= 0 {
		minImpulseATR = 1.0
	}

	// KONFIRMASI 2: Deteksi Prior Impulse
	// Dalam N candle terakhir (eksklusif bar terkini), harga harus sempat berekspansi di atas VWAP (Long) atau di bawah VWAP (Short)
	startIdx := len(snap.ClosedCandles) - 1 - lookback
	if startIdx < 0 {
		startIdx = 0
	}
	endIdx := len(snap.ClosedCandles) - 1

	var hasPriorBullishImpulse bool
	var hasPriorBearishImpulse bool
	latestImpulseIdxLong := -1
	latestImpulseIdxShort := -1
	minImpulseATRLong := cfg.MinImpulseATRLong
	if minImpulseATRLong <= 0 {
		minImpulseATRLong = minImpulseATR
	}
	bullishImpulseThreshold := vwap + (minImpulseATRLong * atr14)
	bullishInvalidationThreshold := vwap + (minImpulseATR * atr14)
	bearishImpulseThreshold := vwap - (minImpulseATR * atr14)

	for i := startIdx; i < endIdx; i++ {
		c := snap.ClosedCandles[i]
		if c.High >= bullishImpulseThreshold {
			hasPriorBullishImpulse = true
			latestImpulseIdxLong = i
		}
		if c.Low <= bearishImpulseThreshold {
			hasPriorBearishImpulse = true
			latestImpulseIdxShort = i
		}
	}

	// [OP-01] Validasi Invalidation Impulse: Impulse gugur jika terjadi pembalikan rezim (Close bar lawan menembus threshold).
	validBullishImpulse := hasPriorBullishImpulse
	if validBullishImpulse && latestImpulseIdxLong >= 0 {
		for i := latestImpulseIdxLong + 1; i < endIdx; i++ {
			if snap.ClosedCandles[i].Close < bearishImpulseThreshold {
				validBullishImpulse = false
				break
			}
		}
	}

	validBearishImpulse := hasPriorBearishImpulse
	if validBearishImpulse && latestImpulseIdxShort >= 0 {
		for i := latestImpulseIdxShort + 1; i < endIdx; i++ {
			if snap.ClosedCandles[i].Close > bullishInvalidationThreshold {
				validBearishImpulse = false
				break
			}
		}
	}

	// KONFIRMASI 3: Zona Pullback Retest (Sentuhan Garis VWAP Dua Arah dengan Urutan Kronologis)
	tolDist := pullbackTol * atr14
	maxAdverseRetestDist := 0.75 * atr14 // Batas bawah: jangan biarkan pullback anjlok jebol > 0.75 ATR menembus VWAP
	maxAdverseRetestShort := maxAdverseRetestDist
	if cfg.MaxAdverseRetestATRShort > 0 {
		maxAdverseRetestShort = cfg.MaxAdverseRetestATRShort * atr14
	}
	var longTestedVWAP bool
	var shortTestedVWAP bool

	retestLookback := cfg.RetestLookbackBars
	if retestLookback <= 0 {
		retestLookback = 4
	}
	if len(snap.ClosedCandles) < retestLookback {
		retestLookback = len(snap.ClosedCandles)
	}
	for i := len(snap.ClosedCandles) - retestLookback; i < len(snap.ClosedCandles); i++ {
		c := snap.ClosedCandles[i]
		cameFromAbove := (c.Open >= vwap) || (i > 0 && snap.ClosedCandles[i-1].Close >= vwap)
		cameFromBelow := (c.Open <= vwap) || (i > 0 && snap.ClosedCandles[i-1].Close <= vwap)

		if cameFromAbove && c.Low <= vwap+tolDist && c.Low >= vwap-maxAdverseRetestDist && (latestImpulseIdxLong >= 0 && i >= latestImpulseIdxLong) {
			if cfg.RequireCleanPullbackLong && c.Low < vwap-0.20*atr14 {
				// Pullback pierced below VWAP more than 0.20 ATR - not a clean bounce
			} else {
				longTestedVWAP = true
			}
		}
		if cameFromBelow && c.High >= vwap-tolDist && c.High <= vwap+maxAdverseRetestShort && (latestImpulseIdxShort >= 0 && i >= latestImpulseIdxShort) {
			shortTestedVWAP = true
		}
	}

	// Option C: Filter volume jual agresif saat pullback sisi LONG
	if longTestedVWAP && cfg.MaxPullbackRVOLLong > 0 && latestImpulseIdxLong >= 0 {
		for k := latestImpulseIdxLong + 1; k < len(snap.ClosedCandles)-1; k++ {
			c := snap.ClosedCandles[k]
			if c.Close < c.Open {
				rvolK := indicator.CalculateRVOL(snap.ClosedCandles[:k+1], 20)
				if rvolK > cfg.MaxPullbackRVOLLong {
					longTestedVWAP = false
					break
				}
			}
		}
	}

	// KONFIRMASI 4: Pantulan Candlestick & Agresi Orderflow
	if !ctx.DeltaZOK {
		return AlphaCandidate{Reason: "INVALID_ZSCORE_BASELINE"}
	}
	barDeltaZScore := ctx.BarDeltaZScore

	latestRange := latest.High - latest.Low
	upperWick := latest.High - math.Max(latest.Open, latest.Close)
	lowerWick := math.Min(latest.Open, latest.Close) - latest.Low

	// Symmetrical Counter-Trend Wick Rejection:
	// Long: Tolak jika upper wick >= 50% dari total range (rejection keras dari seller / Shooting Star).
	isBullishShape := (latest.Close > latest.Open) && (latestRange <= 0 || upperWick < latestRange*0.50)
	if isBullishShape && cfg.MinCandleBodyRatioLong > 0 && latestRange > 0 {
		body := latest.Close - latest.Open
		if (body / latestRange) < cfg.MinCandleBodyRatioLong {
			isBullishShape = false
		}
	}

	// Short: Tolak jika lower wick >= 50% dari total range (rejection keras dari buyer / Hammer).
	isBearishShape := (latest.Close < latest.Open) && (latestRange <= 0 || lowerWick < latestRange*0.50)
	if isBearishShape && cfg.MinCandleBodyRatioShort > 0 && latestRange > 0 {
		body := latest.Open - latest.Close
		if (body / latestRange) < cfg.MinCandleBodyRatioShort {
			isBearishShape = false
		}
	}

	longBounce := isBullishShape && (latest.Close >= vwap) && (barDeltaZScore >= minBounceZLong)
	shortBounce := isBearishShape && (latest.Close <= vwap) && (barDeltaZScore <= -minBounceZShort)

	// KONFIRMASI 5: Posisi Derivatif (OI Delta & Funding Rate)
	if !ctx.OIDeltaOK {
		return AlphaCandidate{Reason: "INVALID_OI_DELTA"}
	}

	// Filter Tren Struktural & Anti-Chop EMA200
	minEMADist := cfg.MinEMA200DistanceATR * atr14
	emaTrendLong := (latest.Close > ema200) && (minEMADist <= 0 || (latest.Close-ema200) >= minEMADist)
	if emaTrendLong && (cfg.MinEMASlopeLong > 0 || cfg.MinEMASlopeLongSet) && len(snap.ClosedCandles) >= cfg.EMAPeriod+20 {
		emaPrev := indicator.CalculateEMA(snap.ClosedCandles[:len(snap.ClosedCandles)-20], cfg.EMAPeriod)
		if !math.IsNaN(emaPrev) && !math.IsInf(emaPrev, 0) && emaPrev > 0 {
			slope := (ema200 - emaPrev) / (20.0 * atr14)
			if slope < cfg.MinEMASlopeLong {
				emaTrendLong = false
			}
		}
	}
	emaTrendShort := (latest.Close < ema200) && (minEMADist <= 0 || (ema200-latest.Close) >= minEMADist)
	if emaTrendShort && cfg.MaxEMASlopeShort > 0 && len(snap.ClosedCandles) >= cfg.EMAPeriod+20 {
		emaPrev := indicator.CalculateEMA(snap.ClosedCandles[:len(snap.ClosedCandles)-20], cfg.EMAPeriod)
		if !math.IsNaN(emaPrev) && !math.IsInf(emaPrev, 0) && emaPrev > 0 {
			slope := (ema200 - emaPrev) / (20.0 * atr14)
			if slope > cfg.MaxEMASlopeShort {
				emaTrendShort = false
			}
		}
	}

	longValid := emaTrendLong &&
		validBullishImpulse &&
		longTestedVWAP &&
		longBounce &&
		ctx.OIValidLong &&
		(snap.FundingRate <= cfg.MaxAdverseFundingRate)

	shortValid := emaTrendShort &&
		validBearishImpulse &&
		shortTestedVWAP &&
		shortBounce &&
		ctx.OIValidShort &&
		(snap.FundingRate >= -cfg.MaxAdverseFundingRate)

	if longValid {
		return AlphaCandidate{Side: model.SideLong, Approved: true}
	}
	if shortValid {
		return AlphaCandidate{Side: model.SideShort, Approved: true}
	}

	var longFails []string
	if latest.Close <= ema200 {
		longFails = append(longFails, "TREND_BELOW_EMA200")
	} else if !emaTrendLong {
		longFails = append(longFails, "EMA200_CHOP_ZONE")
	}
	if !validBullishImpulse {
		longFails = append(longFails, "NO_PRIOR_IMPULSE")
	}
	if !longTestedVWAP {
		longFails = append(longFails, "VWAP_NOT_TESTED")
	}
	if !isBullishShape {
		longFails = append(longFails, "CANDLE_NOT_BULLISH")
	}
	if !(latest.Close >= vwap) {
		longFails = append(longFails, "CLOSE_BELOW_VWAP")
	}
	if !(barDeltaZScore >= minBounceZLong) {
		longFails = append(longFails, "WEAK_DELTA_ZSCORE")
	}
	if !ctx.OIValidLong {
		longFails = append(longFails, "LOW_OI_DELTA")
	}
	if snap.FundingRate > cfg.MaxAdverseFundingRate {
		longFails = append(longFails, "ADVERSE_FUNDING")
	}

	var shortFails []string
	if latest.Close >= ema200 {
		shortFails = append(shortFails, "TREND_ABOVE_EMA200")
	} else if !emaTrendShort {
		shortFails = append(shortFails, "EMA200_CHOP_ZONE")
	}
	if !validBearishImpulse {
		shortFails = append(shortFails, "NO_PRIOR_IMPULSE")
	}
	if !shortTestedVWAP {
		shortFails = append(shortFails, "VWAP_NOT_TESTED")
	}
	if !isBearishShape {
		shortFails = append(shortFails, "CANDLE_NOT_BEARISH")
	}
	if !(latest.Close <= vwap) {
		shortFails = append(shortFails, "CLOSE_ABOVE_VWAP")
	}
	if !(barDeltaZScore <= -minBounceZShort) {
		shortFails = append(shortFails, "WEAK_DELTA_ZSCORE")
	}
	if !ctx.OIValidShort {
		shortFails = append(shortFails, "LOW_OI_DELTA")
	}
	if snap.FundingRate < -cfg.MaxAdverseFundingRate {
		shortFails = append(shortFails, "ADVERSE_FUNDING")
	}

	subReason := strings.Join(shortFails, "|")
	if latest.Close >= ema200 {
		subReason = strings.Join(longFails, "|")
	}
	return AlphaCandidate{
		Reason:    "CONDITIONS_NOT_MET",
		SubReason: subReason,
	}
}

// EvaluateSignal mengevaluasi strategi utama VWAP Pullback/Retest secara terisolasi (backward-compatible).
// Mengembalikan (intent, valid, rejectReason, rejectSubReason) untuk observabilitas audit SQLite.
func EvaluateSignal(
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	activeSlots int,
	isDailyBreached bool,
	hasActiveSymbol bool,
	effectiveEquity, availBal decimal.Decimal,
	cfg EvaluatorConfig,
) (model.OrderIntent, bool, string, string) {
	isolatedCfg := cfg
	isolatedCfg.Sweep.Enabled = false
	isolatedCfg.VolBreakout.Enabled = false
	isolatedCfg.RelResidual.Enabled = false
	isolatedCfg.EMAPullback.Enabled = false
	isolatedCfg.VWAPBreakdown.Enabled = false
	isolatedCfg.StructureBreak.Enabled = false
	dec := EvaluatePortfolio(
		snap,
		btcSnap,
		activeSlots,
		isDailyBreached,
		hasActiveSymbol,
		effectiveEquity,
		availBal,
		SymbolScope{AllowPrimary: true, AllowSweep: false},
		isolatedCfg,
	)
	return dec.Intent, dec.Approved, dec.Reason, dec.SubReason
}
