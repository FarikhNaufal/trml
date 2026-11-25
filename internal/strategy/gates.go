package strategy

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/indicator"
	"trml/internal/model"
)

type sharedMarketContext struct {
	Latest         model.Candle
	EMA200         float64
	ATR14          float64
	NATRPct        float64
	VWAP           float64
	SpreadPct      float64
	BarDeltaZScore float64
	DeltaZOK       bool
	OIDeltaPct     float64
	OIDeltaOK      bool
	OIValidLong    bool
	OIValidShort   bool
}

func isFiniteFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func isFinitePositive(v float64) bool {
	return v > 0 && isFiniteFloat(v)
}

func checkPrimaryNATRBounds(ctx sharedMarketContext, cfg EvaluatorConfig, prefix string) (AlphaCandidate, bool) {
	if cfg.MinNATRPct > 0 && ctx.NATRPct < cfg.MinNATRPct {
		return AlphaCandidate{
			Reason:    "NATR_OUT_OF_BOUNDS",
			SubReason: fmt.Sprintf("%s_NATR_%.2f%%_BELOW_MIN_%.2f%%", prefix, ctx.NATRPct, cfg.MinNATRPct),
		}, false
	}
	if cfg.MaxNATRPct > 0 && ctx.NATRPct > cfg.MaxNATRPct {
		return AlphaCandidate{
			Reason:    "NATR_OUT_OF_BOUNDS",
			SubReason: fmt.Sprintf("%s_NATR_%.2f%%_ABOVE_MAX_%.2f%%", prefix, ctx.NATRPct, cfg.MaxNATRPct),
		}, false
	}
	return AlphaCandidate{}, true
}

func isWinningExit(reason string) bool {
	switch strings.ToUpper(strings.TrimSpace(reason)) {
	case "TAKE_PROFIT", "TRAILING_STOP", "PROFIT_LOCK":
		return true
	default:
		return false
	}
}

// evaluateSharedPreChecks runs shared risk, staleness, liquidity, OI, and indicator baseline gates ONCE per closed bar.
// NOTE: BarDeltaZScore and OIDeltaPct are populated lazily without returning early on failure so individual alphas
// retain exact legacy reject-reason precedence (e.g. warmup/distance checks before z-score/OI errors).
func evaluateSharedPreChecks(
	snap model.MarketSnapshot,
	activeSlots int,
	isDailyBreached bool,
	hasActiveSymbol bool,
	cfg EvaluatorConfig,
) (sharedMarketContext, bool, string, string) {
	var ctx sharedMarketContext

	// Gatekeeper 1: Circuit Breaker, Slot Cap, Active Symbol, Cooldown
	if isDailyBreached {
		return ctx, false, "CIRCUIT_BREAKER_ACTIVE", ""
	}
	if activeSlots >= cfg.MaxSlots {
		return ctx, false, "SLOTS_FULL", ""
	}
	if hasActiveSymbol {
		return ctx, false, "SYMBOL_ALREADY_ACTIVE", ""
	}
	if !snap.LastExitTime.IsZero() {
		cooldownMins := cfg.CooldownAfterExitMinutes
		if isWinningExit(snap.LastExitReason) {
			if cfg.CooldownProfitSet || cfg.CooldownProfitMinutes > 0 {
				cooldownMins = cfg.CooldownProfitMinutes
			}
		} else {
			if cfg.CooldownLossSet || cfg.CooldownLossMinutes > 0 {
				cooldownMins = cfg.CooldownLossMinutes
			}
		}

		if cooldownMins > 0 {
			cooldownDuration := time.Duration(cooldownMins) * time.Minute
			if snap.Now.Sub(snap.LastExitTime) < cooldownDuration {
				remainingSec := int(cooldownDuration.Seconds() - snap.Now.Sub(snap.LastExitTime).Seconds())
				return ctx, false, "SYMBOL_IN_COOLDOWN", fmt.Sprintf("COOLDOWN_ACTIVE_%ds_REMAINING", remainingSec)
			}
		}
	}

	// Gatekeeper 2: Validasi Kesegaran BBO, Mark Price, Funding
	if snap.BookUpdatedAt.IsZero() || snap.Now.Sub(snap.BookUpdatedAt) > cfg.MaxBookStaleness {
		return ctx, false, "STALE_BBO_FEED", ""
	}
	if snap.MarkPriceUpdatedAt.IsZero() || snap.Now.Sub(snap.MarkPriceUpdatedAt) > cfg.MaxMarkStaleness {
		return ctx, false, "STALE_MARK_PRICE_FEED", ""
	}
	if snap.FundingUpdatedAt.IsZero() || snap.Now.Sub(snap.FundingUpdatedAt) > cfg.MaxDataStaleness {
		return ctx, false, "STALE_FUNDING_FEED", ""
	}
	if snap.BestBid <= 0 || snap.BestAsk <= snap.BestBid || math.IsNaN(snap.BestBid) || math.IsNaN(snap.BestAsk) || math.IsInf(snap.BestBid, 0) || math.IsInf(snap.BestAsk, 0) {
		return ctx, false, "INVALID_BID_ASK", ""
	}
	spreadPct := ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
	if math.IsNaN(spreadPct) || math.IsInf(spreadPct, 0) || spreadPct > cfg.MaxSpreadPct {
		return ctx, false, "SPREAD_EXCEEDED", fmt.Sprintf("SPREAD_%.3f%%_MAX_%.3f%%", spreadPct, cfg.MaxSpreadPct)
	}
	ctx.SpreadPct = spreadPct

	// Gatekeeper 3: Validasi Jarak Waktu Sampel Delta Open Interest (Solusi P1-6)
	if !cfg.DisableOIFilter {
		if snap.PrevOpenInterest <= 0 || snap.CurrOpenInterest <= 0 ||
			math.IsNaN(snap.PrevOpenInterest) || math.IsInf(snap.PrevOpenInterest, 0) ||
			math.IsNaN(snap.CurrOpenInterest) || math.IsInf(snap.CurrOpenInterest, 0) ||
			snap.PrevOIUpdatedAt.IsZero() {
			return ctx, false, "MISSING_OI_DATA", "OI_WARMING_UP"
		}
		if snap.CurrOIUpdatedAt.IsZero() || snap.Now.Sub(snap.CurrOIUpdatedAt) > cfg.MaxOIStaleness {
			return ctx, false, "STALE_OI_SAMPLE", ""
		}
		oiInterval := snap.CurrOIUpdatedAt.Sub(snap.PrevOIUpdatedAt)
		if oiInterval < cfg.MinOIInterval || oiInterval > cfg.MaxOIInterval {
			return ctx, false, "INVALID_OI_INTERVAL", fmt.Sprintf("INTERVAL_%s_OUTSIDE_%s_%s", oiInterval, cfg.MinOIInterval, cfg.MaxOIInterval)
		}
	}

	if len(snap.ClosedCandles) < cfg.EMAPeriod {
		return ctx, false, "INSUFFICIENT_CANDLE_HISTORY", ""
	}
	ctx.Latest = snap.ClosedCandles[len(snap.ClosedCandles)-1]

	// KONFIRMASI 1: Filter Tren Makro (EMA 200 mature)
	ema200 := indicator.CalculateEMA(snap.ClosedCandles, cfg.EMAPeriod)
	if math.IsNaN(ema200) || math.IsInf(ema200, 0) || ema200 <= 0 {
		return ctx, false, "INVALID_EMA", ""
	}
	ctx.EMA200 = ema200

	// ATR14 & NATR
	atr14 := indicator.CalculateATR(snap.ClosedCandles, cfg.ATRPeriod)
	if math.IsNaN(atr14) || math.IsInf(atr14, 0) || atr14 <= 0 {
		return ctx, false, "INVALID_ATR", ""
	}
	ctx.ATR14 = atr14

	natrPct := (atr14 / ctx.Latest.Close) * 100.0
	effectiveMinNATR := cfg.MinNATRPct
	if cfg.VolBreakout.Enabled && cfg.VolBreakout.MinNATRPct > 0 && (effectiveMinNATR <= 0 || cfg.VolBreakout.MinNATRPct < effectiveMinNATR) {
		effectiveMinNATR = cfg.VolBreakout.MinNATRPct
	}
	effectiveMaxNATR := cfg.MaxNATRPct
	if cfg.VolBreakout.Enabled && cfg.VolBreakout.MaxNATRPct > 0 && cfg.VolBreakout.MaxNATRPct > effectiveMaxNATR {
		effectiveMaxNATR = cfg.VolBreakout.MaxNATRPct
	}
	if !isFiniteFloat(natrPct) || natrPct < effectiveMinNATR || (effectiveMaxNATR > 0 && natrPct > effectiveMaxNATR) {
		return ctx, false, "NATR_OUT_OF_BOUNDS", fmt.Sprintf("NATR_%.2f%%_OUTSIDE_%.2f%%_%.2f%%", natrPct, effectiveMinNATR, effectiveMaxNATR)
	}
	ctx.NATRPct = natrPct

	vwap := snap.CurrentSessionVWAP
	if cfg.RollingVWAPBars > 0 && len(snap.ClosedCandles) > 0 {
		vwap = indicator.CalculateRollingVWAP(snap.ClosedCandles, cfg.RollingVWAPBars)
	}
	if !isFinitePositive(vwap) {
		return ctx, false, "INVALID_VWAP", ""
	}
	ctx.VWAP = vwap

	// Precompute BarDeltaZScore once for all alphas
	zScore, zOk := indicator.CalculateBarDeltaZScore(snap.ClosedBarDeltasUSDT, cfg.DeltaZScoreWindow)
	if zOk && !math.IsNaN(zScore) && !math.IsInf(zScore, 0) {
		ctx.BarDeltaZScore = zScore
		ctx.DeltaZOK = true
	}

	// Precompute normalized OI delta and directional validity once for all alphas
	oiDeltaOK := true
	var oiDeltaPct float64
	if !cfg.DisableOIFilter {
		basisSec := 300.0
		if cfg.CandleDuration > 0 {
			basisSec = cfg.CandleDuration.Seconds()
		}
		rawOIDeltaPct := ((snap.CurrOpenInterest - snap.PrevOpenInterest) / snap.PrevOpenInterest) * 100.0
		if math.IsNaN(rawOIDeltaPct) || math.IsInf(rawOIDeltaPct, 0) {
			oiDeltaOK = false
		} else {
			oiDeltaPct = rawOIDeltaPct
			if !snap.PrevOIUpdatedAt.IsZero() && !snap.CurrOIUpdatedAt.IsZero() {
				elapsedSec := snap.CurrOIUpdatedAt.Sub(snap.PrevOIUpdatedAt).Seconds()
				if elapsedSec >= 60.0 && elapsedSec <= (basisSec*2.0) {
					oiDeltaPct = rawOIDeltaPct * (basisSec / elapsedSec)
					if math.IsNaN(oiDeltaPct) || math.IsInf(oiDeltaPct, 0) {
						oiDeltaOK = false
					}
				}
			}
		}
	}
	ctx.OIDeltaPct = oiDeltaPct
	ctx.OIDeltaOK = oiDeltaOK

	if oiDeltaOK {
		minOIDeltaLong := cfg.MinOIDeltaLongPct
		if !cfg.MinOIDeltaLongSet && minOIDeltaLong == 0 {
			minOIDeltaLong = cfg.MinOIDeltaPct
		}
		minOIDeltaShort := cfg.MinOIDeltaShortPct
		if !cfg.MinOIDeltaShortSet && minOIDeltaShort == 0 {
			minOIDeltaShort = cfg.MinOIDeltaPct
		}

		// Soft OI Bypass dengan Capitulation Floor:
		// Bypass keterlambatan update API Open Interest HANYA JIKA terdapat konfirmasi orderflow kuat (Z-Score >= 1.20).
		const minCapitulationOIDeltaShort = -0.10 // Max OI drop -0.10% untuk Short
		const minCapitulationOIDeltaLong = -0.05  // Max OI drop -0.05% untuk Long

		ctx.OIValidLong = cfg.DisableOIFilter ||
			(oiDeltaPct >= minOIDeltaLong) ||
			(ctx.DeltaZOK && ctx.BarDeltaZScore >= 1.20 && oiDeltaPct >= minCapitulationOIDeltaLong)
		ctx.OIValidShort = cfg.DisableOIFilter ||
			(oiDeltaPct >= minOIDeltaShort) ||
			(ctx.DeltaZOK && ctx.BarDeltaZScore <= -1.20 && oiDeltaPct >= minCapitulationOIDeltaShort)
	}

	return ctx, true, "", ""
}

func evaluateEarlyAllowedSide(latestClose, ema200 float64, cfg EvaluatorConfig) (bool, string, string) {
	earlyAllowedSide := strings.ToUpper(strings.TrimSpace(cfg.AllowedSide))
	if earlyAllowedSide == "SHORT_ONLY" && latestClose > ema200 {
		return false, "SIDE_DISABLED_SHORT_ONLY", "LONG_DISABLED_BY_CONFIG"
	}
	if earlyAllowedSide == "LONG_ONLY" && latestClose < ema200 {
		return false, "SIDE_DISABLED_LONG_ONLY", "SHORT_DISABLED_BY_CONFIG"
	}
	return true, "", ""
}

func evaluateSessionTimeGuards(snap model.MarketSnapshot, cfg EvaluatorConfig) (bool, string, string) {
	warmupMins := cfg.VWAPSessionWarmupMinutes
	if !cfg.VWAPSessionWarmupSet && warmupMins <= 0 {
		warmupMins = 30
	}
	utcNow := snap.Now.UTC()
	minsSinceMidnight := utcNow.Hour()*60 + utcNow.Minute()
	if warmupMins > 0 && minsSinceMidnight < warmupMins {
		return false, "VWAP_SESSION_WARMUP", fmt.Sprintf("UTC_%02d:%02d_IN_WARMUP_%dm", utcNow.Hour(), utcNow.Minute(), warmupMins)
	}
	if len(cfg.BlockedUTCHours) > 0 && cfg.BlockedUTCHours[utcNow.Hour()] {
		return false, "UTC_HOUR_BLOCKED", fmt.Sprintf("UTC_HOUR_%02d_BLOCKED_BY_CONFIG", utcNow.Hour())
	}
	return true, "", ""
}

func evaluateSideAndPostFilters(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	targetSide model.Side,
	cfg EvaluatorConfig,
) (bool, string, string) {
	return evaluateSideAndPostFiltersWithOpts(ctx, snap, btcSnap, targetSide, cfg, false)
}

func evaluateSideAndPostFiltersWithOpts(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	targetSide model.Side,
	cfg EvaluatorConfig,
	bypassBTCTier2 bool,
) (bool, string, string) {
	// KONFIRMASI 5B: Filter Arah Eksekusi (ALLOWED_SIDE)
	allowedSide := strings.ToUpper(strings.TrimSpace(cfg.AllowedSide))
	if allowedSide == "SHORT_ONLY" && targetSide == model.SideLong {
		return false, "SIDE_DISABLED_SHORT_ONLY", "LONG_DISABLED_BY_CONFIG"
	}
	if allowedSide == "LONG_ONLY" && targetSide == model.SideShort {
		return false, "SIDE_DISABLED_LONG_ONLY", "SHORT_DISABLED_BY_CONFIG"
	}

	// Filter volatilitas minimum khusus LONG agar SHORT tetap bisa menangkap koin ber-NATR lebih rendah.
	if targetSide == model.SideLong && cfg.MinNATRLongPct > 0 && ctx.NATRPct < cfg.MinNATRLongPct {
		return false, "NATR_OUT_OF_BOUNDS", fmt.Sprintf("LONG_NATR_%.2f%%_BELOW_MIN_%.2f%%", ctx.NATRPct, cfg.MinNATRLongPct)
	}
	if cfg.FundingEnvelope.MinShortEnabled && targetSide == model.SideShort && snap.FundingRate < cfg.FundingEnvelope.MinShortRate {
		return false, "FUNDING_CROWDING_FILTER", fmt.Sprintf("SHORT_FR_%.5f_BELOW_MIN_%.5f", snap.FundingRate, cfg.FundingEnvelope.MinShortRate)
	}
	if cfg.FundingEnvelope.MaxLongEnabled && targetSide == model.SideLong && snap.FundingRate > cfg.FundingEnvelope.MaxLongRate {
		return false, "FUNDING_CROWDING_FILTER", fmt.Sprintf("LONG_FR_%.5f_ABOVE_MAX_%.5f", snap.FundingRate, cfg.FundingEnvelope.MaxLongRate)
	}

	// Higher-Timeframe (1h) Completed-Bar EMA Trend Alignment Filter
	if cfg.HTFEMAPeriod > 0 {
		hourly := CompletedHourlyCandles(snap.ClosedCandles)
		minHourly := cfg.HTFEMAPeriod
		if cfg.HTFEMAMaxRisingSlopePct > 0 {
			minHourly = cfg.HTFEMAPeriod + 24
		}
		if len(hourly) < minHourly {
			return false, "HTF_EMA_WARMING_UP", fmt.Sprintf("HOURLY_%d_LT_%d", len(hourly), minHourly)
		}
		htfEMA := indicator.CalculateEMA(hourly, cfg.HTFEMAPeriod)
		if !isFinitePositive(htfEMA) {
			return false, "INVALID_HTF_EMA", ""
		}
		if targetSide == model.SideShort && ctx.Latest.Close >= htfEMA {
			return false, "HTF_EMA_TREND_MISMATCH", fmt.Sprintf("CLOSE_%.4f_ABOVE_1H_EMA%d_%.4f", ctx.Latest.Close, cfg.HTFEMAPeriod, htfEMA)
		}
		if targetSide == model.SideLong && ctx.Latest.Close <= htfEMA {
			return false, "HTF_EMA_TREND_MISMATCH", fmt.Sprintf("CLOSE_%.4f_BELOW_1H_EMA%d_%.4f", ctx.Latest.Close, cfg.HTFEMAPeriod, htfEMA)
		}
		if cfg.HTFEMAMaxRisingSlopePct > 0 {
			prevEMA := indicator.CalculateEMA(hourly[:len(hourly)-24], cfg.HTFEMAPeriod)
			if !isFinitePositive(prevEMA) {
				return false, "INVALID_HTF_EMA", ""
			}
			slopePct := ((htfEMA - prevEMA) / prevEMA) * 100.0
			if !isFiniteFloat(slopePct) {
				return false, "INVALID_HTF_EMA", ""
			}
			if targetSide == model.SideShort && slopePct > cfg.HTFEMAMaxRisingSlopePct {
				return false, "HTF_EMA_SLOPE_RISING", fmt.Sprintf("1H_EMA%d_24H_SLOPE_%.2f%%_GT_%.2f%%", cfg.HTFEMAPeriod, slopePct, cfg.HTFEMAMaxRisingSlopePct)
			}
			if targetSide == model.SideLong && slopePct < -cfg.HTFEMAMaxRisingSlopePct {
				return false, "HTF_EMA_SLOPE_FALLING", fmt.Sprintf("1H_EMA%d_24H_SLOPE_%.2f%%_LT_-%.2f%%", cfg.HTFEMAPeriod, slopePct, cfg.HTFEMAMaxRisingSlopePct)
			}
		}
	}

	// Cross-Sectional Market Breadth Capitulation Hard Veto (when risk multiplier <= 0)
	if cfg.MarketBreadthLowThreshold > 0 && snap.MarketBreadthValid && targetSide == model.SideShort &&
		snap.MarketBreadthPct < cfg.MarketBreadthLowThreshold && cfg.MarketBreadthLowRiskMult <= 0 {
		return false, "MARKET_BREADTH_CAPITULATION_VETO", fmt.Sprintf("BREADTH_%.1f%%_BELOW_MIN_%.1f%%", snap.MarketBreadthPct*100.0, cfg.MarketBreadthLowThreshold*100.0)
	}

	// KONFIRMASI 5C & 5D: Proteksi Anti-Frenzy / Explosive Counter-Impulse Filter & Min Signal RVOL
	maxFrenzyRVOL := cfg.MaxFrenzyRVOL
	if maxFrenzyRVOL <= 0 {
		maxFrenzyRVOL = 2.50
	}
	maxCounterImpulseATR := cfg.MaxCounterImpulseATR
	if maxCounterImpulseATR <= 0 {
		maxCounterImpulseATR = 1.50
	}

	checkLookback := 3
	if len(snap.ClosedCandles) < checkLookback {
		checkLookback = len(snap.ClosedCandles)
	}
	latestRVOL := indicator.CalculateRVOL(snap.ClosedCandles, 20)

	if cfg.MinSignalRVOL > 0 && latestRVOL < cfg.MinSignalRVOL {
		return false, "LOW_SIGNAL_RVOL", fmt.Sprintf("RVOL_%.2f_BELOW_MIN_%.2f", latestRVOL, cfg.MinSignalRVOL)
	}

	rvol := latestRVOL
	hasHighRVOL := rvol >= maxFrenzyRVOL
	if !hasHighRVOL {
		for i := len(snap.ClosedCandles) - checkLookback; i < len(snap.ClosedCandles); i++ {
			if indicator.CalculateRVOL(snap.ClosedCandles[:i+1], 20) >= maxFrenzyRVOL {
				hasHighRVOL = true
				rvol = indicator.CalculateRVOL(snap.ClosedCandles[:i+1], 20)
				break
			}
		}
	}
	if hasHighRVOL {
		for i := len(snap.ClosedCandles) - checkLookback; i < len(snap.ClosedCandles); i++ {
			c := snap.ClosedCandles[i]
			barRange := c.High - c.Low
			if targetSide == model.SideShort && c.Close > c.Open && barRange >= (maxCounterImpulseATR*ctx.ATR14) {
				return false, "EXPLOSIVE_PUMP_FRENZY", fmt.Sprintf("RVOL_%.2f_AND_EXPANSION_BAR_%.2f_ATR", rvol, barRange/ctx.ATR14)
			}
			if targetSide == model.SideLong && !cfg.DisablePanicDumpFrenzyLong && c.Close < c.Open && barRange >= (maxCounterImpulseATR*ctx.ATR14) {
				return false, "PANIC_DUMP_FRENZY", fmt.Sprintf("RVOL_%.2f_AND_DUMP_BAR_%.2f_ATR", rvol, barRange/ctx.ATR14)
			}
		}
	}

	// KONFIRMASI 6: Gerbang Makro Bitcoin (Two-Tiered Soft Veto & Micro Counter-Momentum Guard)
	if cfg.BTCGateEnabled && btcSnap != nil && len(btcSnap.ClosedCandles) > 0 && snap.Symbol != "BTCUSDT" {
		latestBTC := btcSnap.ClosedCandles[len(btcSnap.ClosedCandles)-1]
		var btc5mChangePct float64
		if latestBTC.Open > 0 {
			btc5mChangePct = ((latestBTC.Close - latestBTC.Open) / latestBTC.Open) * 100.0
		}

		maxDump := cfg.BTCMax5mDumpPct
		if maxDump <= 0 {
			maxDump = 0.60
		}
		maxPump := cfg.BTCMax5mPumpPct
		if maxPump <= 0 {
			maxPump = 0.60
		}

		// TIER 1: Extreme Flash Crash / Pump Circuit Breaker (Hard Veto pada Anomali Ekstrem)
		if targetSide == model.SideLong && btc5mChangePct < -maxDump {
			return false, "BTC_FLASH_DUMP", fmt.Sprintf("BTC_5M_DUMP_%.2f%%_MAX_%.2f%%", btc5mChangePct, -maxDump)
		}
		if targetSide == model.SideShort && btc5mChangePct > maxPump {
			return false, "BTC_FLASH_PUMP", fmt.Sprintf("BTC_5M_PUMP_%.2f%%_MAX_%.2f%%", btc5mChangePct, maxPump)
		}

		// TIER 1.5: Micro Counter-Momentum Guard (Mencegah Entry Saat Bar 5m BTC Sedang Memantul Kencang)
		if cfg.BTCCounterMomentumPct > 0 {
			if targetSide == model.SideShort && btc5mChangePct >= cfg.BTCCounterMomentumPct {
				return false, "BTC_COUNTER_MOMENTUM_PUMP", fmt.Sprintf("BTC_5M_PUMP_%.2f%%_EXCEEDS_COUNTER_LIMIT_%.2f%%", btc5mChangePct, cfg.BTCCounterMomentumPct)
			}
			if targetSide == model.SideLong && btc5mChangePct <= -cfg.BTCCounterMomentumPct {
				return false, "BTC_COUNTER_MOMENTUM_DUMP", fmt.Sprintf("BTC_5M_DUMP_%.2f%%_EXCEEDS_COUNTER_LIMIT_%.2f%%", btc5mChangePct, -cfg.BTCCounterMomentumPct)
			}
		}

		// TIER 2: Symmetric Directional Bias (Soft Veto) - Best Practice
		tier2Applies := allowedSide != "BOTH"
		if !cfg.BTCGateShortOnly {
			tier2Applies = allowedSide == "AUTO" || allowedSide == ""
		}
		if tier2Applies && !bypassBTCTier2 {
			resolveBTCVWAP := func() float64 {
				if cfg.RollingVWAPBars > 0 && len(btcSnap.ClosedCandles) > 0 {
					return indicator.CalculateRollingVWAP(btcSnap.ClosedCandles, cfg.RollingVWAPBars)
				}
				return btcSnap.CurrentSessionVWAP
			}
			if targetSide == model.SideShort && len(btcSnap.ClosedCandles) >= cfg.EMAPeriod {
				btcEMA := indicator.CalculateEMA(btcSnap.ClosedCandles, cfg.EMAPeriod)
				btcVWAP := resolveBTCVWAP()
				if !isFinitePositive(btcEMA) {
					return false, "INVALID_BTC_EMA", ""
				}
				if !isFiniteFloat(btcVWAP) {
					return false, "INVALID_BTC_VWAP", ""
				}
				if latestBTC.Close > btcEMA && (btcVWAP <= 0 || latestBTC.Close > btcVWAP) {
					return false, "BTC_TREND_BULLISH_SHORT_VETO", fmt.Sprintf("BTC_CLOSE_%.1f_ABOVE_EMA_%.1f_AND_VWAP_%.1f", latestBTC.Close, btcEMA, btcVWAP)
				}

				persistenceBars := cfg.BTCGatePersistenceBars
				if persistenceBars <= 0 {
					persistenceBars = 3
				}
				if len(btcSnap.ClosedCandles) >= persistenceBars {
					for p := 0; p < persistenceBars; p++ {
						idx := len(btcSnap.ClosedCandles) - 1 - p
						c := btcSnap.ClosedCandles[idx]
						if c.Close > btcEMA && (btcVWAP <= 0 || c.Close > btcVWAP) {
							return false, "BTC_TREND_SHORT_VETO_PERSISTENCE", fmt.Sprintf("BTC_BAR_%d_AGO_CLOSE_%.1f_ABOVE_EMA_%.1f_REQUIRED_%d_BARS", p, c.Close, btcEMA, persistenceBars)
						}
					}
				}
			}

			if targetSide == model.SideLong && len(btcSnap.ClosedCandles) >= cfg.EMAPeriod {
				btcEMA := indicator.CalculateEMA(btcSnap.ClosedCandles, cfg.EMAPeriod)
				btcVWAP := resolveBTCVWAP()
				if !isFinitePositive(btcEMA) {
					return false, "INVALID_BTC_EMA", ""
				}
				if !isFiniteFloat(btcVWAP) {
					return false, "INVALID_BTC_VWAP", ""
				}
				btcBearish := latestBTC.Close < btcEMA || (btcVWAP > 0 && latestBTC.Close < btcVWAP)
				if cfg.BTCTier2LongSymmetric {
					btcBearish = latestBTC.Close < btcEMA && (btcVWAP <= 0 || latestBTC.Close < btcVWAP)
				}
				if btcBearish {
					return false, "BTC_TREND_BEARISH_LONG_VETO", fmt.Sprintf("BTC_CLOSE_%.1f_BELOW_EMA_%.1f_OR_VWAP_%.1f", latestBTC.Close, btcEMA, btcVWAP)
				}

				persistenceBars := cfg.BTCGatePersistenceBars
				if persistenceBars <= 0 {
					persistenceBars = 3
				}
				if len(btcSnap.ClosedCandles) >= persistenceBars {
					for p := 0; p < persistenceBars; p++ {
						idx := len(btcSnap.ClosedCandles) - 1 - p
						c := btcSnap.ClosedCandles[idx]
						barBearish := c.Close < btcEMA || (btcVWAP > 0 && c.Close < btcVWAP)
						if cfg.BTCTier2LongSymmetric {
							barBearish = c.Close < btcEMA && (btcVWAP <= 0 || c.Close < btcVWAP)
						}
						if barBearish {
							return false, "BTC_TREND_LONG_VETO_PERSISTENCE", fmt.Sprintf("BTC_BAR_%d_AGO_CLOSE_%.1f_BELOW_EMA_%.1f_REQUIRED_%d_BARS", p, c.Close, btcEMA, persistenceBars)
						}
					}
				}
			}
		}
	}

	if targetSide == model.SideLong && cfg.BTCChopBlockLong && btcSnap != nil && snap.Symbol != "BTCUSDT" &&
		isBTCChopHighVol(btcSnap.ClosedCandles, cfg.EMAPeriod, cfg.ATRPeriod) {
		return false, "BTC_CHOP_LONG_VETO", "BTC_CHOP_HIGH_VOL_REGIME"
	}
	if targetSide == model.SideShort && cfg.BTCChopBlockShort && btcSnap != nil && snap.Symbol != "BTCUSDT" &&
		isBTCChopHighVol(btcSnap.ClosedCandles, cfg.EMAPeriod, cfg.ATRPeriod) {
		return false, "BTC_CHOP_SHORT_VETO", "BTC_CHOP_HIGH_VOL_REGIME"
	}

	return true, "", ""
}

func resolveEntryAndBuildIntent(
	ctx sharedMarketContext,
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	targetSide model.Side,
	strategyID string,
	effectiveEquity, availBal decimal.Decimal,
	cfg EvaluatorConfig,
) (model.OrderIntent, bool, string, string) {
	forceBreakoutMarket := strategyID == StrategyIDVolBreakout && cfg.VolBreakout.EntryMarket
	isMarket := forceBreakoutMarket || strings.ToUpper(cfg.EntryOrderType) == "MARKET"
	var rawEntryPrice float64
	if targetSide == model.SideLong {
		if isMarket {
			rawEntryPrice = snap.BestAsk
		} else {
			rawEntryPrice = snap.BestBid
		}
	} else {
		if isMarket {
			rawEntryPrice = snap.BestBid
		} else {
			rawEntryPrice = snap.BestAsk
		}
	}

	sizingCfg := cfg.Sizing
	if forceBreakoutMarket {
		sizingCfg.EntryOrderType = "MARKET"
	} else if sizingCfg.EntryOrderType == "" {
		sizingCfg.EntryOrderType = cfg.EntryOrderType
	}
	if strategyID == StrategyIDVolBreakout && isFinitePositive(cfg.VolBreakout.RiskMult) && cfg.VolBreakout.RiskMult != 1.0 {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.VolBreakout.RiskMult))
	} else if strategyID == StrategyIDEMAPullback && isFinitePositive(cfg.EMAPullback.RiskMult) && cfg.EMAPullback.RiskMult != 1.0 {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.EMAPullback.RiskMult))
	} else if strategyID == StrategyIDVWAPBreakdown && isFinitePositive(cfg.VWAPBreakdown.RiskMult) && cfg.VWAPBreakdown.RiskMult != 1.0 {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.VWAPBreakdown.RiskMult))
	} else if strategyID == StrategyIDStructureBreak && isFinitePositive(cfg.StructureBreak.RiskMult) && cfg.StructureBreak.RiskMult != 1.0 {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.StructureBreak.RiskMult))
	}
	if isFinitePositive(cfg.BTCChopRiskMult) && cfg.BTCChopRiskMult < 1.0 && btcSnap != nil &&
		isBTCChopHighVol(btcSnap.ClosedCandles, cfg.EMAPeriod, cfg.ATRPeriod) {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.BTCChopRiskMult))
	}
	if cfg.MarketBreadthLowThreshold > 0 && snap.MarketBreadthValid && targetSide == model.SideShort &&
		snap.MarketBreadthPct < cfg.MarketBreadthLowThreshold && isFinitePositive(cfg.MarketBreadthLowRiskMult) && cfg.MarketBreadthLowRiskMult < 1.0 {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.MarketBreadthLowRiskMult))
	}
	if btcSnap != nil && (cfg.RS4hWeakThresholdPct < 0 || cfg.RS4hStrongThresholdPct > 0) {
		bars4h := 16
		if cfg.CandleDuration > 0 {
			if b := int((4 * time.Hour) / cfg.CandleDuration); b > 0 {
				bars4h = b
			}
		}
		if len(snap.ClosedCandles) > bars4h && len(btcSnap.ClosedCandles) > bars4h {
			altNow := snap.ClosedCandles[len(snap.ClosedCandles)-1].Close
			altPrev := snap.ClosedCandles[len(snap.ClosedCandles)-1-bars4h].Close
			btcNow := btcSnap.ClosedCandles[len(btcSnap.ClosedCandles)-1].Close
			btcPrev := btcSnap.ClosedCandles[len(btcSnap.ClosedCandles)-1-bars4h].Close
			if isFinitePositive(altPrev) && isFinitePositive(btcPrev) && isFinitePositive(altNow) && isFinitePositive(btcNow) {
				rs4hPct := (((altNow - altPrev) / altPrev) - ((btcNow - btcPrev) / btcPrev)) * 100.0
				if isFiniteFloat(rs4hPct) && targetSide == model.SideShort {
					if cfg.RS4hWeakThresholdPct < 0 && rs4hPct <= cfg.RS4hWeakThresholdPct &&
						isFinitePositive(cfg.RS4hWeakRiskMult) && cfg.RS4hWeakRiskMult != 1.0 {
						sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.RS4hWeakRiskMult))
					} else if cfg.RS4hStrongThresholdPct > 0 && rs4hPct >= cfg.RS4hStrongThresholdPct {
						if !isFinitePositive(cfg.RS4hStrongRiskMult) {
							return model.OrderIntent{}, false, "RS4H_STRONG_OUTPERFORMER_VETO", ""
						}
						if cfg.RS4hStrongRiskMult != 1.0 {
							sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.RS4hStrongRiskMult))
						}
					}
				}
			}
		}
	}
	if cfg.DDThrottleTier2ThresholdPct > 0 && isFinitePositive(cfg.DDThrottleTier2RiskMult) && cfg.DDThrottleTier2RiskMult < 1.0 &&
		snap.PortfolioDrawdownPct >= cfg.DDThrottleTier2ThresholdPct {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.DDThrottleTier2RiskMult))
	} else if cfg.DDThrottleThresholdPct > 0 && isFinitePositive(cfg.DDThrottleRiskMult) && cfg.DDThrottleRiskMult < 1.0 &&
		snap.PortfolioDrawdownPct >= cfg.DDThrottleThresholdPct {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.DDThrottleRiskMult))
	}
	if isFinitePositive(cfg.MaxEffectiveRiskPerTradePct) {
		capDec := decimal.NewFromFloat(cfg.MaxEffectiveRiskPerTradePct)
		if sizingCfg.RiskPerTradePct.GreaterThan(capDec) {
			sizingCfg.RiskPerTradePct = capDec
		}
	}

	intent, ok := BuildSafeDecimalOrderIntent(snap, effectiveEquity, availBal, targetSide, rawEntryPrice, ctx.ATR14, sizingCfg)
	if !ok {
		return model.OrderIntent{}, false, "SIZING_FAILED", ""
	}
	intent.StrategyID = NormalizeStrategyID(strategyID)
	if forceBreakoutMarket {
		intent.EntryOrderType = "MARKET"
	}
	return intent, true, "SIGNAL_APPROVED", ""
}

func isBTCChopHighVol(btcCandles []model.Candle, emaPeriod, atrPeriod int) bool {
	n := len(btcCandles)
	if n < 30 {
		return false
	}
	lastClose := btcCandles[n-1].Close
	if lastClose <= 0 {
		return false
	}
	if emaPeriod <= 0 || n < emaPeriod {
		emaPeriod = n / 2
	}
	if atrPeriod <= 0 {
		atrPeriod = 14
	}
	ema := indicator.CalculateEMA(btcCandles, emaPeriod)
	atr := indicator.CalculateATR(btcCandles, atrPeriod)
	natrPct := (atr / lastClose) * 100.0
	if natrPct >= 1.20 {
		return true
	}
	if ema > 0 {
		emaDistPct := math.Abs((lastClose-ema)/ema) * 100.0
		if emaDistPct <= 0.15 {
			return true
		}
		if n >= 8 {
			flips := 0
			prevAbove := btcCandles[n-7].Close >= ema
			for i := n - 6; i < n; i++ {
				curAbove := btcCandles[i].Close >= ema
				if curAbove != prevAbove {
					flips++
					prevAbove = curAbove
				}
			}
			if flips >= 2 {
				return true
			}
		}
	}
	return false
}
