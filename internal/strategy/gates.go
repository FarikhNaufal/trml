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
			if cfg.CooldownProfitMinutes > 0 {
				cooldownMins = cfg.CooldownProfitMinutes
			}
		} else {
			if cfg.CooldownLossMinutes > 0 {
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
	if math.IsNaN(natrPct) || math.IsInf(natrPct, 0) || natrPct < cfg.MinNATRPct || natrPct > cfg.MaxNATRPct {
		return ctx, false, "NATR_OUT_OF_BOUNDS", fmt.Sprintf("NATR_%.2f%%_OUTSIDE_%.2f%%_%.2f%%", natrPct, cfg.MinNATRPct, cfg.MaxNATRPct)
	}
	ctx.NATRPct = natrPct

	vwap := snap.CurrentSessionVWAP
	if math.IsNaN(vwap) || math.IsInf(vwap, 0) || vwap <= 0 {
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
		if minOIDeltaLong == 0 {
			minOIDeltaLong = cfg.MinOIDeltaPct
		}
		minOIDeltaShort := cfg.MinOIDeltaShortPct
		if minOIDeltaShort == 0 {
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
	if warmupMins <= 0 {
		warmupMins = 30
	}
	utcNow := snap.Now.UTC()
	minsSinceMidnight := utcNow.Hour()*60 + utcNow.Minute()
	if minsSinceMidnight < warmupMins {
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
		if tier2Applies {
			if targetSide == model.SideShort && len(btcSnap.ClosedCandles) >= cfg.EMAPeriod {
				btcEMA := indicator.CalculateEMA(btcSnap.ClosedCandles, cfg.EMAPeriod)
				btcVWAP := btcSnap.CurrentSessionVWAP
				if math.IsNaN(btcEMA) || math.IsInf(btcEMA, 0) || btcEMA <= 0 {
					return false, "INVALID_BTC_EMA", ""
				}
				if math.IsNaN(btcVWAP) || math.IsInf(btcVWAP, 0) {
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
				btcVWAP := btcSnap.CurrentSessionVWAP
				if math.IsNaN(btcEMA) || math.IsInf(btcEMA, 0) || btcEMA <= 0 {
					return false, "INVALID_BTC_EMA", ""
				}
				if math.IsNaN(btcVWAP) || math.IsInf(btcVWAP, 0) {
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
						barBearish := c.Close < btcEMA
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
	isMarket := strings.ToUpper(cfg.EntryOrderType) == "MARKET"
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
	if sizingCfg.EntryOrderType == "" {
		sizingCfg.EntryOrderType = cfg.EntryOrderType
	}
	if cfg.BTCChopRiskMult > 0 && cfg.BTCChopRiskMult < 1.0 && btcSnap != nil &&
		isBTCChopHighVol(btcSnap.ClosedCandles, cfg.EMAPeriod, cfg.ATRPeriod) {
		sizingCfg.RiskPerTradePct = sizingCfg.RiskPerTradePct.Mul(decimal.NewFromFloat(cfg.BTCChopRiskMult))
	}

	intent, ok := BuildSafeDecimalOrderIntent(snap, effectiveEquity, availBal, targetSide, rawEntryPrice, ctx.ATR14, sizingCfg)
	if !ok {
		return model.OrderIntent{}, false, "SIZING_FAILED", ""
	}
	intent.StrategyID = NormalizeStrategyID(strategyID)
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
