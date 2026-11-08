package protection

import (
	"math"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

// ProtectionTier mengidentifikasi tingkat perlindungan posisi aktif secara hierarkis.
type ProtectionTier int

const (
	// Tier0InitialSL: Stop Loss awal berbasis ATR tanpa modifikasi.
	Tier0InitialSL ProtectionTier = 0
	// Tier1BreakEven: Rem Dini / Early BE Shield atau perpindahan SL ke BE pasca TP1.
	Tier1BreakEven ProtectionTier = 1
	// Tier2ProfitLock: Penguncian laba runner saat MFE mencapai threshold tertentu (misal +1.80x ATR).
	Tier2ProfitLock ProtectionTier = 2
	// Tier3TrailingRatchet: Trailing Stop dinamis yang mengunci profit seiring puncak harga baru (peak price).
	Tier3TrailingRatchet ProtectionTier = 3
)

// ProtectionConfig memuat seluruh parameter ambang batas perlindungan posisi.
type ProtectionConfig struct {
	Tier1BEEnabled           bool
	Tier1BETriggerLongATR    float64
	Tier1BETriggerShortATR   float64
	Tier2ProfitLockEnabled   bool
	Tier2ProfitLockATR       float64
	Tier2ProfitLockOffsetATR float64
	TrailingEnabled          bool
	TrailingActivateATR      float64
	TrailingDistanceATR      float64
	PartialTPEnabled         bool
	MinBreakEvenOffsetPct    float64
	RunnerBECushionATR       float64
	TakerFeeRate             decimal.Decimal
	SlippageRate             decimal.Decimal
}

// PositionSnapshot adalah representasi netral dari posisi terbuka yang dievaluasi.
type PositionSnapshot struct {
	Symbol         string
	Side           model.Side
	EntryPrice     decimal.Decimal
	CurrentSLPrice decimal.Decimal
	InitialSLPrice decimal.Decimal
	PeakPrice      decimal.Decimal
	ATRAtEntry     decimal.Decimal
	ProtectionTier ProtectionTier
	IsTP1Hit       bool
	TickSize       decimal.Decimal
}

// ProtectionDecision memuat hasil keputusan evaluasi perlindungan.
type ProtectionDecision struct {
	NewSLPrice    decimal.Decimal
	NewTier       ProtectionTier
	ShouldUpdate  bool
	TriggerReason string
}

// EvaluatePositionProtection adalah Single Source of Truth (SSOT) untuk penyesuaian Stop Loss dinamis,
// Break-Even, Runner Profit Lock, dan Trailing Stop. Fungsi ini bersifat pure mathematical state engine
// dan dipakai secara seragam oleh SimBroker (Backtest), DryRunBroker, dan BinanceLiveBroker.
func EvaluatePositionProtection(pos PositionSnapshot, currentPrice decimal.Decimal, cfg ProtectionConfig) ProtectionDecision {
	if pos.ATRAtEntry.LessThanOrEqual(decimal.Zero) || pos.EntryPrice.LessThanOrEqual(decimal.Zero) {
		return ProtectionDecision{ShouldUpdate: false}
	}

	// 1. Fallback default parameter jika bernilai non-positif
	t1LongTrigger := cfg.Tier1BETriggerLongATR
	if t1LongTrigger <= 0 {
		t1LongTrigger = 0.70
	}
	t1ShortTrigger := cfg.Tier1BETriggerShortATR
	if t1ShortTrigger <= 0 {
		t1ShortTrigger = 0.90
	}
	t2Trigger := cfg.Tier2ProfitLockATR
	if t2Trigger <= 0 {
		t2Trigger = 1.80
	}
	t2Offset := cfg.Tier2ProfitLockOffsetATR
	if t2Offset <= 0 {
		t2Offset = 0.90
	}
	trailActivate := cfg.TrailingActivateATR
	if trailActivate <= 0 {
		trailActivate = 1.50
	}
	trailDist := cfg.TrailingDistanceATR
	if trailDist <= 0 {
		trailDist = 0.35
	}

	// Offset rate untuk Break-Even (menutup round-trip fee + slippage)
	minOffset := decimal.NewFromFloat(math.Max(cfg.MinBreakEvenOffsetPct, 0.0010))
	feeOffset := cfg.TakerFeeRate.Mul(decimal.NewFromInt(2)).Add(cfg.SlippageRate)
	beOffsetRate := decimal.Max(minOffset, feeOffset)

	isShort := pos.Side == model.SideShort
	roundUpForQuantize := isShort

	// 2. Evaluasi Arah LONG
	if pos.Side == model.SideLong {
		trailActivateDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailActivate))
		trailDistAmt := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailDist))
		t2TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2Trigger))
		t2OffsetDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2Offset))
		t1TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t1LongTrigger))

		// Tier 3: Trailing Stop Ratchet untuk Runner
		canTrail := cfg.TrailingEnabled && pos.PeakPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(trailActivateDist)
		if cfg.PartialTPEnabled {
			canTrail = canTrail && pos.IsTP1Hit
		}
		if canTrail {
			rawSL := pos.PeakPrice.Sub(trailDistAmt)
			targetSL := quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
			// Ratchet Guard: SL Long wajib lebih tinggi dari SL saat ini
			if targetSL.GreaterThan(pos.CurrentSLPrice) {
				return ProtectionDecision{
					NewSLPrice:    targetSL,
					NewTier:       Tier3TrailingRatchet,
					ShouldUpdate:  true,
					TriggerReason: "TRAILING_STOP",
				}
			}
		}

		// Tier 2: Runner Profit Lock (+1.80x ATR MFE -> Kunci SL ke Entry + 0.90x ATR)
		if cfg.Tier2ProfitLockEnabled && pos.ProtectionTier < Tier2ProfitLock && currentPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(t2TriggerDist) {
			rawSL := pos.EntryPrice.Add(t2OffsetDist)
			targetSL := quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
			if targetSL.GreaterThan(pos.CurrentSLPrice) {
				return ProtectionDecision{
					NewSLPrice:    targetSL,
					NewTier:       Tier2ProfitLock,
					ShouldUpdate:  true,
					TriggerReason: "TIER2_PROFIT_LOCK",
				}
			}
		}

		// Tier 1: Rem Dini / Early BE Shield (+0.70x ATR MFE -> Pindahkan SL ke Entry + Fee Buffer)
		if cfg.Tier1BEEnabled && pos.ProtectionTier < Tier1BreakEven && currentPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(t1TriggerDist) {
			rawSL := pos.EntryPrice.Mul(decimal.NewFromInt(1).Add(beOffsetRate))
			targetSL := quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
			if targetSL.GreaterThan(pos.CurrentSLPrice) {
				return ProtectionDecision{
					NewSLPrice:    targetSL,
					NewTier:       Tier1BreakEven,
					ShouldUpdate:  true,
					TriggerReason: "TIER1_EARLY_BE",
				}
			}
		}

		return ProtectionDecision{ShouldUpdate: false}
	}

	// 3. Evaluasi Arah SHORT
	trailActivateDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailActivate))
	trailDistAmt := pos.ATRAtEntry.Mul(decimal.NewFromFloat(trailDist))
	t2TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2Trigger))
	t2OffsetDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t2Offset))
	t1TriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(t1ShortTrigger))

	// Tier 3: Trailing Stop Ratchet untuk Runner
	canTrail := cfg.TrailingEnabled && pos.EntryPrice.Sub(pos.PeakPrice).GreaterThanOrEqual(trailActivateDist)
	if cfg.PartialTPEnabled {
		canTrail = canTrail && pos.IsTP1Hit
	}
	if canTrail {
		rawSL := pos.PeakPrice.Add(trailDistAmt)
		targetSL := quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
		// Ratchet Guard: SL Short wajib lebih rendah dari SL saat ini
		if targetSL.LessThan(pos.CurrentSLPrice) {
			return ProtectionDecision{
				NewSLPrice:    targetSL,
				NewTier:       Tier3TrailingRatchet,
				ShouldUpdate:  true,
				TriggerReason: "TRAILING_STOP",
			}
		}
	}

	// Tier 2: Runner Profit Lock (+1.80x ATR MFE -> Kunci SL ke Entry - 0.90x ATR)
	if cfg.Tier2ProfitLockEnabled && pos.ProtectionTier < Tier2ProfitLock && pos.EntryPrice.Sub(currentPrice).GreaterThanOrEqual(t2TriggerDist) {
		rawSL := pos.EntryPrice.Sub(t2OffsetDist)
		targetSL := quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
		if targetSL.LessThan(pos.CurrentSLPrice) {
			return ProtectionDecision{
				NewSLPrice:    targetSL,
				NewTier:       Tier2ProfitLock,
				ShouldUpdate:  true,
				TriggerReason: "TIER2_PROFIT_LOCK",
			}
		}
	}

	// Tier 1: Rem Dini / Early BE Shield (+0.90x ATR MFE -> Pindahkan SL ke Entry - Fee Buffer)
	if cfg.Tier1BEEnabled && pos.ProtectionTier < Tier1BreakEven && pos.EntryPrice.Sub(currentPrice).GreaterThanOrEqual(t1TriggerDist) {
		rawSL := pos.EntryPrice.Mul(decimal.NewFromInt(1).Sub(beOffsetRate))
		targetSL := quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
		if targetSL.LessThan(pos.CurrentSLPrice) {
			return ProtectionDecision{
				NewSLPrice:    targetSL,
				NewTier:       Tier1BreakEven,
				ShouldUpdate:  true,
				TriggerReason: "TIER1_EARLY_BE",
			}
		}
	}

	return ProtectionDecision{ShouldUpdate: false}
}

// CalculateBreakEvenSL menghitung harga SL Break-Even saat TP1 tersentuh, lengkap dengan buffer fee / cushion.
func CalculateBreakEvenSL(pos PositionSnapshot, cfg ProtectionConfig) decimal.Decimal {
	minOffset := decimal.NewFromFloat(math.Max(cfg.MinBreakEvenOffsetPct, 0.0010))
	feeOffset := cfg.TakerFeeRate.Mul(decimal.NewFromInt(2)).Add(cfg.SlippageRate)
	beOffsetRate := decimal.Max(minOffset, feeOffset)

	isShort := pos.Side == model.SideShort
	roundUpForQuantize := isShort

	var rawSL decimal.Decimal
	if cfg.RunnerBECushionATR > 0 && pos.ATRAtEntry.GreaterThan(decimal.Zero) {
		cushion := pos.ATRAtEntry.Mul(decimal.NewFromFloat(cfg.RunnerBECushionATR))
		if pos.Side == model.SideLong {
			rawSL = pos.EntryPrice.Sub(cushion)
			if pos.InitialSLPrice.GreaterThan(decimal.Zero) && rawSL.LessThan(pos.InitialSLPrice) {
				rawSL = pos.InitialSLPrice
			}
		} else {
			rawSL = pos.EntryPrice.Add(cushion)
			if pos.InitialSLPrice.GreaterThan(decimal.Zero) && rawSL.GreaterThan(pos.InitialSLPrice) {
				rawSL = pos.InitialSLPrice
			}
		}
	} else {
		if pos.Side == model.SideLong {
			rawSL = pos.EntryPrice.Mul(decimal.NewFromInt(1).Add(beOffsetRate))
		} else {
			rawSL = pos.EntryPrice.Mul(decimal.NewFromInt(1).Sub(beOffsetRate))
		}
	}

	// Defensive Ratchet Guard: Pastikan beSL tidak pernah melompat mundur dibandingkan SL aktif saat ini
	if pos.CurrentSLPrice.GreaterThan(decimal.Zero) {
		if pos.Side == model.SideLong && rawSL.LessThan(pos.CurrentSLPrice) {
			rawSL = pos.CurrentSLPrice
		} else if pos.Side == model.SideShort && rawSL.GreaterThan(pos.CurrentSLPrice) {
			rawSL = pos.CurrentSLPrice
		}
	}

	return quantizePriceSafely(rawSL, pos.TickSize, roundUpForQuantize)
}

func quantizePriceSafely(price, tickSize decimal.Decimal, roundUp bool) decimal.Decimal {
	if tickSize.GreaterThan(decimal.Zero) {
		if qSL, err := model.QuantizePrice(price, tickSize, roundUp); err == nil {
			return qSL
		}
	}
	return price
}

