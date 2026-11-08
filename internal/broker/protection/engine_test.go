package protection

import (
	"testing"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestEvaluatePositionProtection_Long(t *testing.T) {
	cfg := ProtectionConfig{
		Tier1BEEnabled:           true,
		Tier1BETriggerLongATR:    0.70,
		Tier1BETriggerShortATR:   0.90,
		Tier2ProfitLockEnabled:   true,
		Tier2ProfitLockATR:       1.80,
		Tier2ProfitLockOffsetATR: 0.90,
		TrailingEnabled:          true,
		TrailingActivateATR:      1.50,
		TrailingDistanceATR:      0.35,
		PartialTPEnabled:         true,
		MinBreakEvenOffsetPct:    0.0010,
		TakerFeeRate:             decimal.NewFromFloat(0.0005),
		SlippageRate:             decimal.NewFromFloat(0.0002),
	}

	entryPrice := decimal.NewFromFloat(100.0)
	initialSL := decimal.NewFromFloat(98.0)
	atr := decimal.NewFromFloat(2.0)
	tickSize := decimal.NewFromFloat(0.01)

	// 1. Harga belum mencapai trigger apa pun (MFE < 0.70 ATR = 101.4)
	pos := PositionSnapshot{
		Symbol:         "TESTUSDT",
		Side:           model.SideLong,
		EntryPrice:     entryPrice,
		CurrentSLPrice: initialSL,
		InitialSLPrice: initialSL,
		PeakPrice:      decimal.NewFromFloat(100.5),
		ATRAtEntry:     atr,
		ProtectionTier: Tier0InitialSL,
		IsTP1Hit:       false,
		TickSize:       tickSize,
	}

	dec := EvaluatePositionProtection(pos, decimal.NewFromFloat(100.5), cfg)
	if dec.ShouldUpdate {
		t.Fatalf("expected no update below Tier 1, got %+v", dec)
	}

	// 2. Harga mencapai Tier 1 Early BE (+0.70 ATR = 101.4)
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(101.5), cfg)
	if !dec.ShouldUpdate || dec.NewTier != Tier1BreakEven {
		t.Fatalf("expected Tier 1 BE, got %+v", dec)
	}
	if !dec.NewSLPrice.GreaterThan(entryPrice) {
		t.Fatalf("expected SL > entryPrice for BE, got %s", dec.NewSLPrice)
	}

	// Terapkan Tier 1
	pos.CurrentSLPrice = dec.NewSLPrice
	pos.ProtectionTier = dec.NewTier

	// 3. Harga mencapai Tier 2 Profit Lock (+1.80 ATR = 103.6)
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(103.7), cfg)
	if !dec.ShouldUpdate || dec.NewTier != Tier2ProfitLock {
		t.Fatalf("expected Tier 2 Profit Lock, got %+v", dec)
	}
	expectedT2SL := entryPrice.Add(atr.Mul(decimal.NewFromFloat(0.90))) // 100 + 1.8 = 101.8
	if !dec.NewSLPrice.Equal(expectedT2SL) {
		t.Fatalf("expected T2 SL %s, got %s", expectedT2SL, dec.NewSLPrice)
	}

	// Terapkan Tier 2
	pos.CurrentSLPrice = dec.NewSLPrice
	pos.ProtectionTier = dec.NewTier

	// 4. Trailing Stop Runner (hanya aktif setelah TP1 Hit)
	pos.PeakPrice = decimal.NewFromFloat(104.0) // 104 - 100 = 4.0 >= 1.50 ATR (3.0)
	pos.IsTP1Hit = false
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(103.8), cfg)
	if dec.ShouldUpdate && dec.NewTier == Tier3TrailingRatchet {
		t.Fatalf("trailing should NOT trigger before TP1 hit when PartialTPEnabled")
	}

	pos.IsTP1Hit = true
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(103.8), cfg)
	if !dec.ShouldUpdate || dec.NewTier != Tier3TrailingRatchet {
		t.Fatalf("expected Tier 3 Trailing Ratchet, got %+v", dec)
	}
	// Target SL = Peak(104.0) - TrailDist(0.35 * 2.0 = 0.7) = 103.3
	expectedTrailSL := decimal.NewFromFloat(103.3)
	if !dec.NewSLPrice.Equal(expectedTrailSL) {
		t.Fatalf("expected Trailing SL %s, got %s", expectedTrailSL, dec.NewSLPrice)
	}

	// Terapkan Tier 3
	pos.CurrentSLPrice = dec.NewSLPrice
	pos.ProtectionTier = dec.NewTier

	// 5. Ratchet Guard: Harga turun (Peak tetap 104, current turun ke 102), SL tidak boleh mundur!
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(102.0), cfg)
	if dec.ShouldUpdate {
		t.Fatalf("ratchet guard violated: SL attempted to update downwards: %+v", dec)
	}
}

func TestEvaluatePositionProtection_Short(t *testing.T) {
	cfg := ProtectionConfig{
		Tier1BEEnabled:           true,
		Tier1BETriggerLongATR:    0.70,
		Tier1BETriggerShortATR:   0.90,
		Tier2ProfitLockEnabled:   true,
		Tier2ProfitLockATR:       1.80,
		Tier2ProfitLockOffsetATR: 0.90,
		TrailingEnabled:          true,
		TrailingActivateATR:      1.50,
		TrailingDistanceATR:      0.35,
		PartialTPEnabled:         true,
		MinBreakEvenOffsetPct:    0.0010,
		TakerFeeRate:             decimal.NewFromFloat(0.0005),
		SlippageRate:             decimal.NewFromFloat(0.0002),
	}

	entryPrice := decimal.NewFromFloat(100.0)
	initialSL := decimal.NewFromFloat(102.0)
	atr := decimal.NewFromFloat(2.0)
	tickSize := decimal.NewFromFloat(0.01)

	// 1. Initial State Short
	pos := PositionSnapshot{
		Symbol:         "TESTUSDT",
		Side:           model.SideShort,
		EntryPrice:     entryPrice,
		CurrentSLPrice: initialSL,
		InitialSLPrice: initialSL,
		PeakPrice:      decimal.NewFromFloat(99.5),
		ATRAtEntry:     atr,
		ProtectionTier: Tier0InitialSL,
		IsTP1Hit:       false,
		TickSize:       tickSize,
	}

	dec := EvaluatePositionProtection(pos, decimal.NewFromFloat(99.5), cfg)
	if dec.ShouldUpdate {
		t.Fatalf("expected no update below Tier 1, got %+v", dec)
	}

	// 2. Short Tier 1 Early BE (+0.90 ATR = 98.2)
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(98.1), cfg)
	if !dec.ShouldUpdate || dec.NewTier != Tier1BreakEven {
		t.Fatalf("expected Short Tier 1 BE, got %+v", dec)
	}
	if !dec.NewSLPrice.LessThan(entryPrice) {
		t.Fatalf("expected Short SL < entryPrice for BE, got %s", dec.NewSLPrice)
	}

	// Terapkan Tier 1
	pos.CurrentSLPrice = dec.NewSLPrice
	pos.ProtectionTier = dec.NewTier

	// 3. Short Tier 2 Profit Lock (+1.80 ATR = 96.4)
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(96.3), cfg)
	if !dec.ShouldUpdate || dec.NewTier != Tier2ProfitLock {
		t.Fatalf("expected Short Tier 2 Profit Lock, got %+v", dec)
	}
	expectedT2SL := entryPrice.Sub(atr.Mul(decimal.NewFromFloat(0.90))) // 100 - 1.8 = 98.2
	if !dec.NewSLPrice.Equal(expectedT2SL) {
		t.Fatalf("expected Short T2 SL %s, got %s", expectedT2SL, dec.NewSLPrice)
	}

	// Terapkan Tier 2
	pos.CurrentSLPrice = dec.NewSLPrice
	pos.ProtectionTier = dec.NewTier

	// 4. Short Trailing Stop Runner (Peak = 95.0, distance = 5.0 >= 3.0)
	pos.PeakPrice = decimal.NewFromFloat(95.0)
	pos.IsTP1Hit = true
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(95.5), cfg)
	if !dec.ShouldUpdate || dec.NewTier != Tier3TrailingRatchet {
		t.Fatalf("expected Short Trailing Ratchet, got %+v", dec)
	}
	// Target SL = Peak(95.0) + TrailDist(0.7) = 95.7
	expectedTrailSL := decimal.NewFromFloat(95.7)
	if !dec.NewSLPrice.Equal(expectedTrailSL) {
		t.Fatalf("expected Short Trailing SL %s, got %s", expectedTrailSL, dec.NewSLPrice)
	}

	// 5. Short Ratchet Guard: Harga naik balik ke 98.0, SL tidak boleh mundur!
	pos.CurrentSLPrice = dec.NewSLPrice
	dec = EvaluatePositionProtection(pos, decimal.NewFromFloat(98.0), cfg)
	if dec.ShouldUpdate {
		t.Fatalf("ratchet guard violated: Short SL attempted to update upwards: %+v", dec)
	}
}

func TestCalculateBreakEvenSL(t *testing.T) {
	cfg := ProtectionConfig{
		MinBreakEvenOffsetPct: 0.0010,
		TakerFeeRate:          decimal.NewFromFloat(0.0005),
		SlippageRate:          decimal.NewFromFloat(0.0002),
		RunnerBECushionATR:    0.0,
	}

	longPos := PositionSnapshot{
		Side:           model.SideLong,
		EntryPrice:     decimal.NewFromFloat(100.0),
		CurrentSLPrice: decimal.NewFromFloat(98.0),
		TickSize:       decimal.NewFromFloat(0.01),
	}
	longBE := CalculateBreakEvenSL(longPos, cfg)
	if !longBE.GreaterThan(longPos.EntryPrice) {
		t.Fatalf("expected Long BE > 100.0, got %s", longBE)
	}

	shortPos := PositionSnapshot{
		Side:           model.SideShort,
		EntryPrice:     decimal.NewFromFloat(100.0),
		CurrentSLPrice: decimal.NewFromFloat(102.0),
		TickSize:       decimal.NewFromFloat(0.01),
	}
	shortBE := CalculateBreakEvenSL(shortPos, cfg)
	if !shortBE.LessThan(shortPos.EntryPrice) {
		t.Fatalf("expected Short BE < 100.0, got %s", shortBE)
	}
}

