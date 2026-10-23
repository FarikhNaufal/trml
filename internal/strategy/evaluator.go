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

type EvaluatorConfig struct {
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
	MinOIDeltaShortPct    float64
	MaxAdverseFundingRate float64

	// Parameter Strategi VWAP Pullback / Retest with Orderflow Bounce
	PullbackToleranceATR float64
	MinBounceZScore      float64
	LookbackImpulseBars  int
	RetestLookbackBars   int
	MinImpulseATR        float64
	MaxEntryDistanceATR  float64
	EntryOrderType       string

	// Gerbang Makro Bitcoin (Two-Tiered Soft Veto)
	BTCGateEnabled  bool
	BTCMax5mDumpPct float64
	BTCMax5mPumpPct float64

	Sizing SizingConfig
}

// EvaluateSignal mengevaluasi seluruh gatekeeper risiko dan 5 konfirmasi VWAP Pullback/Retest
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
	// Gatekeeper 1: Circuit Breaker & Slot Cap
	if isDailyBreached {
		return model.OrderIntent{}, false, "CIRCUIT_BREAKER_ACTIVE", ""
	}
	if activeSlots >= cfg.MaxSlots {
		return model.OrderIntent{}, false, "SLOTS_FULL", ""
	}
	if hasActiveSymbol {
		return model.OrderIntent{}, false, "SYMBOL_ALREADY_ACTIVE", ""
	}

	// Gatekeeper 2: Validasi Kesegaran BBO, Mark Price, Funding
	if snap.BookUpdatedAt.IsZero() || snap.Now.Sub(snap.BookUpdatedAt) > cfg.MaxBookStaleness {
		return model.OrderIntent{}, false, "STALE_BBO_FEED", ""
	}
	if snap.MarkPriceUpdatedAt.IsZero() || snap.Now.Sub(snap.MarkPriceUpdatedAt) > cfg.MaxMarkStaleness {
		return model.OrderIntent{}, false, "STALE_MARK_PRICE_FEED", ""
	}
	if snap.FundingUpdatedAt.IsZero() || snap.Now.Sub(snap.FundingUpdatedAt) > cfg.MaxDataStaleness {
		return model.OrderIntent{}, false, "STALE_FUNDING_FEED", ""
	}
	if snap.BestBid <= 0 || snap.BestAsk <= snap.BestBid {
		return model.OrderIntent{}, false, "INVALID_BID_ASK", ""
	}
	spreadPct := ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
	if spreadPct > cfg.MaxSpreadPct {
		return model.OrderIntent{}, false, "SPREAD_EXCEEDED", fmt.Sprintf("SPREAD_%.3f%%_MAX_%.3f%%", spreadPct, cfg.MaxSpreadPct)
	}

	// Gatekeeper 3: Validasi Jarak Waktu Sampel Delta Open Interest (Solusi P1-6)
	if snap.PrevOpenInterest <= 0 || snap.CurrOpenInterest <= 0 || snap.PrevOIUpdatedAt.IsZero() {
		return model.OrderIntent{}, false, "MISSING_OI_DATA", "OI_WARMING_UP"
	}
	if snap.CurrOIUpdatedAt.IsZero() || snap.Now.Sub(snap.CurrOIUpdatedAt) > cfg.MaxOIStaleness {
		return model.OrderIntent{}, false, "STALE_OI_SAMPLE", ""
	}
	oiInterval := snap.CurrOIUpdatedAt.Sub(snap.PrevOIUpdatedAt)
	if oiInterval < cfg.MinOIInterval || oiInterval > cfg.MaxOIInterval {
		return model.OrderIntent{}, false, "INVALID_OI_INTERVAL", fmt.Sprintf("INTERVAL_%s_OUTSIDE_%s_%s", oiInterval, cfg.MinOIInterval, cfg.MaxOIInterval)
	}

	if len(snap.ClosedCandles) < cfg.EMAPeriod {
		return model.OrderIntent{}, false, "INSUFFICIENT_CANDLE_HISTORY", ""
	}
	latest := snap.ClosedCandles[len(snap.ClosedCandles)-1]

	// KONFIRMASI 1: Filter Tren Makro (EMA 200 mature)
	ema200 := indicator.CalculateEMA(snap.ClosedCandles, cfg.EMAPeriod)

	// ATR14 & NATR
	atr14 := indicator.CalculateATR(snap.ClosedCandles, cfg.ATRPeriod)
	if atr14 <= 0 {
		return model.OrderIntent{}, false, "INVALID_ATR", ""
	}
	natrPct := (atr14 / latest.Close) * 100.0
	if natrPct < cfg.MinNATRPct || natrPct > cfg.MaxNATRPct {
		return model.OrderIntent{}, false, "NATR_OUT_OF_BOUNDS", fmt.Sprintf("NATR_%.2f%%_OUTSIDE_%.2f%%_%.2f%%", natrPct, cfg.MinNATRPct, cfg.MaxNATRPct)
	}

	vwap := snap.CurrentSessionVWAP
	if vwap <= 0 {
		return model.OrderIntent{}, false, "INVALID_VWAP", ""
	}

	// Filter Anti-Chasing: Batasi jarak maksimal penutupan candle dari garis VWAP
	maxEntryDistATR := cfg.MaxEntryDistanceATR
	if maxEntryDistATR <= 0 {
		maxEntryDistATR = 1.00
	}
	distFromVWAP := math.Abs(latest.Close - vwap)
	if distFromVWAP > maxEntryDistATR*atr14 {
		distATR := distFromVWAP / atr14
		return model.OrderIntent{}, false, "ENTRY_TOO_FAR_FROM_VWAP", fmt.Sprintf("DIST_%.2f_ATR_MAX_%.2f_ATR", distATR, maxEntryDistATR)
	}

	// Defaults jika konfigurasi belum terisi
	pullbackTol := cfg.PullbackToleranceATR
	if pullbackTol <= 0 {
		pullbackTol = 0.35
	}
	minBounceZ := cfg.MinBounceZScore
	if minBounceZ <= 0 {
		minBounceZ = 1.20
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
	bullishImpulseThreshold := vwap + (minImpulseATR * atr14)
	bearishImpulseThreshold := vwap - (minImpulseATR * atr14)

	for i := startIdx; i < endIdx; i++ {
		c := snap.ClosedCandles[i]
		if c.High >= bullishImpulseThreshold {
			hasPriorBullishImpulse = true
		}
		if c.Low <= bearishImpulseThreshold {
			hasPriorBearishImpulse = true
		}
	}

	// KONFIRMASI 3: Zona Pullback Retest (Sentuhan Garis VWAP dengan Toleransi ATR)
	// Memeriksa apakah dalam 4 bar terakhir (20 menit), harga sempat menguji zona toleransi VWAP
	tolDist := pullbackTol * atr14
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
		if c.Low <= vwap+tolDist {
			longTestedVWAP = true
		}
		if c.High >= vwap-tolDist {
			shortTestedVWAP = true
		}
	}

	// KONFIRMASI 4: Pantulan Candlestick & Agresi Orderflow
	barDeltaZScore, zOk := indicator.CalculateBarDeltaZScore(snap.ClosedBarDeltasUSDT, cfg.DeltaZScoreWindow)
	if !zOk {
		return model.OrderIntent{}, false, "INVALID_ZSCORE_BASELINE", ""
	}

	longBounce := (latest.Close > latest.Open) && (latest.Close >= vwap) && (barDeltaZScore >= minBounceZ)
	shortBounce := (latest.Close < latest.Open) && (latest.Close <= vwap) && (barDeltaZScore <= -minBounceZ)

	// KONFIRMASI 5: Posisi Derivatif (OI Delta & Funding Rate)
	oiDeltaPct := ((snap.CurrOpenInterest - snap.PrevOpenInterest) / snap.PrevOpenInterest) * 100.0

	minOIDeltaLong := cfg.MinOIDeltaLongPct
	if minOIDeltaLong == 0 {
		minOIDeltaLong = cfg.MinOIDeltaPct
	}
	minOIDeltaShort := cfg.MinOIDeltaShortPct
	if minOIDeltaShort == 0 {
		minOIDeltaShort = cfg.MinOIDeltaPct
	}

	longValid := (latest.Close > ema200) &&
		hasPriorBullishImpulse &&
		longTestedVWAP &&
		longBounce &&
		(oiDeltaPct >= minOIDeltaLong) &&
		(snap.FundingRate <= cfg.MaxAdverseFundingRate)

	shortValid := (latest.Close < ema200) &&
		hasPriorBearishImpulse &&
		shortTestedVWAP &&
		shortBounce &&
		(oiDeltaPct >= minOIDeltaShort) &&
		(snap.FundingRate >= -cfg.MaxAdverseFundingRate)

	var targetSide model.Side
	var rawEntryPrice float64

	isMarket := strings.ToUpper(cfg.EntryOrderType) == "MARKET"

	if longValid {
		targetSide = model.SideLong
		if isMarket {
			rawEntryPrice = snap.BestAsk
		} else {
			rawEntryPrice = snap.BestBid
		}
	} else if shortValid {
		targetSide = model.SideShort
		if isMarket {
			rawEntryPrice = snap.BestBid
		} else {
			rawEntryPrice = snap.BestAsk
		}
	} else {
		var longFails []string
		if latest.Close <= ema200 {
			longFails = append(longFails, "TREND_BELOW_EMA200")
		}
		if !hasPriorBullishImpulse {
			longFails = append(longFails, "NO_PRIOR_IMPULSE")
		}
		if !longTestedVWAP {
			longFails = append(longFails, "VWAP_NOT_TESTED")
		}
		if !(latest.Close > latest.Open) {
			longFails = append(longFails, "CANDLE_NOT_BULLISH")
		}
		if !(latest.Close >= vwap) {
			longFails = append(longFails, "CLOSE_BELOW_VWAP")
		}
		if !(barDeltaZScore >= minBounceZ) {
			longFails = append(longFails, "WEAK_DELTA_ZSCORE")
		}
		if !(oiDeltaPct >= minOIDeltaLong) {
			longFails = append(longFails, "LOW_OI_DELTA")
		}
		if snap.FundingRate > cfg.MaxAdverseFundingRate {
			longFails = append(longFails, "ADVERSE_FUNDING")
		}

		var shortFails []string
		if latest.Close >= ema200 {
			shortFails = append(shortFails, "TREND_ABOVE_EMA200")
		}
		if !hasPriorBearishImpulse {
			shortFails = append(shortFails, "NO_PRIOR_IMPULSE")
		}
		if !shortTestedVWAP {
			shortFails = append(shortFails, "VWAP_NOT_TESTED")
		}
		if !(latest.Close < latest.Open) {
			shortFails = append(shortFails, "CANDLE_NOT_BEARISH")
		}
		if !(latest.Close <= vwap) {
			shortFails = append(shortFails, "CLOSE_ABOVE_VWAP")
		}
		if !(barDeltaZScore <= -minBounceZ) {
			shortFails = append(shortFails, "WEAK_DELTA_ZSCORE")
		}
		if !(oiDeltaPct >= minOIDeltaShort) {
			shortFails = append(shortFails, "LOW_OI_DELTA")
		}
		if snap.FundingRate < -cfg.MaxAdverseFundingRate {
			shortFails = append(shortFails, "ADVERSE_FUNDING")
		}

		var subReason string
		if latest.Close >= ema200 {
			subReason = strings.Join(longFails, "|")
		} else {
			subReason = strings.Join(shortFails, "|")
		}
		return model.OrderIntent{}, false, "CONDITIONS_NOT_MET", subReason
	}

	// KONFIRMASI 6: Gerbang Makro Bitcoin (Two-Tiered Soft Veto)
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
			return model.OrderIntent{}, false, "BTC_FLASH_DUMP", fmt.Sprintf("BTC_5M_DUMP_%.2f%%_MAX_%.2f%%", btc5mChangePct, -maxDump)
		}
		if targetSide == model.SideShort && btc5mChangePct > maxPump {
			return model.OrderIntent{}, false, "BTC_FLASH_PUMP", fmt.Sprintf("BTC_5M_PUMP_%.2f%%_MAX_%.2f%%", btc5mChangePct, maxPump)
		}

		// TIER 2: Symmetric Directional Bias (Soft Veto)
		// 1. Posisi SHORT hanya diizinkan jika tren BTC memang tidak bullish kuat.
		// Jika BTC berada di atas EMA dan di atas Session VWAP, veto posisi SHORT altcoin!
		if targetSide == model.SideShort && len(btcSnap.ClosedCandles) >= cfg.EMAPeriod {
			btcEMA := indicator.CalculateEMA(btcSnap.ClosedCandles, cfg.EMAPeriod)
			btcVWAP := btcSnap.CurrentSessionVWAP
			if latestBTC.Close > btcEMA && (btcVWAP <= 0 || latestBTC.Close > btcVWAP) {
				return model.OrderIntent{}, false, "BTC_TREND_BULLISH_SHORT_VETO", fmt.Sprintf("BTC_CLOSE_%.1f_ABOVE_EMA_%.1f_AND_VWAP_%.1f", latestBTC.Close, btcEMA, btcVWAP)
			}
		}

		// 2. Posisi LONG hanya diizinkan jika tren BTC tidak bearish lemah/menurun.
		// Jika BTC berada di bawah EMA dan di bawah Session VWAP, veto posisi LONG altcoin!
		if targetSide == model.SideLong && len(btcSnap.ClosedCandles) >= cfg.EMAPeriod {
			btcEMA := indicator.CalculateEMA(btcSnap.ClosedCandles, cfg.EMAPeriod)
			btcVWAP := btcSnap.CurrentSessionVWAP
			if latestBTC.Close < btcEMA && (btcVWAP <= 0 || latestBTC.Close < btcVWAP) {
				return model.OrderIntent{}, false, "BTC_TREND_BEARISH_LONG_VETO", fmt.Sprintf("BTC_CLOSE_%.1f_BELOW_EMA_%.1f_AND_VWAP_%.1f", latestBTC.Close, btcEMA, btcVWAP)
			}
		}
	}

	intent, ok := BuildSafeDecimalOrderIntent(snap, effectiveEquity, availBal, targetSide, rawEntryPrice, atr14, cfg.Sizing)
	if !ok {
		return model.OrderIntent{}, false, "SIZING_FAILED", ""
	}
	return intent, true, "SIGNAL_APPROVED", ""
}

