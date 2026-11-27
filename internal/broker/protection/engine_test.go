package protection

import (
	"testing"
	"time"

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

func TestShouldTriggerStaleLossExit(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	entry := decimal.NewFromFloat(100.0)
	staleDur := 6 * time.Hour

	if ShouldTriggerStaleLossExit(model.SideLong, entry, decimal.NewFromFloat(98.0), t0, t0.Add(5*time.Hour+59*time.Minute), staleDur) {
		t.Fatalf("expected false before staleDur elapsed")
	}
	if !ShouldTriggerStaleLossExit(model.SideLong, entry, decimal.NewFromFloat(99.5), t0, t0.Add(6*time.Hour), staleDur) {
		t.Fatalf("expected true for underwater LONG at staleDur")
	}
	if ShouldTriggerStaleLossExit(model.SideLong, entry, entry, t0, t0.Add(6*time.Hour), staleDur) {
		t.Fatalf("expected false at exact breakeven for LONG")
	}
	if ShouldTriggerStaleLossExit(model.SideLong, entry, decimal.NewFromFloat(101.0), t0, t0.Add(7*time.Hour), staleDur) {
		t.Fatalf("expected false for profitable LONG")
	}
	if !ShouldTriggerStaleLossExit(model.SideShort, entry, decimal.NewFromFloat(100.5), t0, t0.Add(6*time.Hour), staleDur) {
		t.Fatalf("expected true for underwater SHORT at staleDur")
	}
	if ShouldTriggerStaleLossExit(model.SideShort, entry, decimal.NewFromFloat(99.0), t0, t0.Add(6*time.Hour), staleDur) {
		t.Fatalf("expected false for profitable SHORT")
	}
	if ShouldTriggerStaleLossExit(model.SideShort, entry, decimal.NewFromFloat(105.0), t0, t0.Add(6*time.Hour), 0) {
		t.Fatalf("expected false when staleDur <= 0")
	}
}

func TestUpdatePeakAndDrawdownPct(t *testing.T) {
	peak, dd := UpdatePeakAndDrawdownPct(decimal.NewFromFloat(100.0), decimal.NewFromFloat(125.0))
	if !peak.Equal(decimal.NewFromFloat(125.0)) || dd != 0.0 {
		t.Fatalf("expected new peak 125 and dd=0, got peak=%s dd=%f", peak, dd)
	}
	peak, dd = UpdatePeakAndDrawdownPct(peak, decimal.NewFromFloat(100.0))
	if !peak.Equal(decimal.NewFromFloat(125.0)) || dd < 0.1999 || dd > 0.2001 {
		t.Fatalf("expected peak 125 and dd=0.20, got peak=%s dd=%f", peak, dd)
	}
	peak, dd = UpdatePeakAndDrawdownPct(decimal.Zero, decimal.Zero)
	if !peak.IsZero() || dd != 0.0 {
		t.Fatalf("expected dd=0 when zero equity, got peak=%s dd=%f", peak, dd)
	}
}

func TestResolveStrategyExitProfiles(t *testing.T) {
	baseProt := ProtectionConfig{
		Tier1BEEnabled:         true,
		Tier2ProfitLockEnabled: true,
		TrailingEnabled:        true,
		TrailingActivateATR:    2.0,
		TrailingDistanceATR:    1.0,
		PartialTPEnabled:       true,
	}
	profiles := map[string]StrategyExitProfile{
		"MAJOR_RSMOM_SHORT": {
			HasOverride:            true,
			PartialTPEnabled:       false,
			Tier1BEEnabled:         false,
			Tier2ProfitLockEnabled: false,
			TrailingEnabled:        true,
			TrailingActivateATR:    2.5,
			TrailingDistanceATR:    1.2,
			StaleLossExitDuration:  0,
			MaxHoldingDuration:     48 * time.Hour,
		},
	}

	// Base QMAX strategy retains base untouched
	gotBase := ResolveProtectionConfig(baseProt, profiles, "VWAP_PULLBACK")
	if !gotBase.PartialTPEnabled || !gotBase.Tier1BEEnabled || gotBase.TrailingActivateATR != 2.0 {
		t.Fatalf("expected base QMAX protection untouched, got %+v", gotBase)
	}
	if d := ResolveMaxHoldingDuration(18*time.Hour, profiles, "VWAP_PULLBACK"); d != 18*time.Hour {
		t.Fatalf("expected base max holding 18h, got %v", d)
	}
	if s := ResolveStaleLossExitDuration(6*time.Hour, profiles, "VWAP_PULLBACK"); s != 6*time.Hour {
		t.Fatalf("expected base stale loss 6h, got %v", s)
	}

	// MajorPod strategy overrides cleanly, including explicit 0s stale loss
	gotMaj := ResolveProtectionConfig(baseProt, profiles, "MAJOR_RSMOM_SHORT")
	if gotMaj.PartialTPEnabled || gotMaj.Tier1BEEnabled || gotMaj.Tier2ProfitLockEnabled || !gotMaj.TrailingEnabled {
		t.Fatalf("unexpected MajorPod protection override flags: %+v", gotMaj)
	}
	if gotMaj.TrailingActivateATR != 2.5 || gotMaj.TrailingDistanceATR != 1.2 {
		t.Fatalf("unexpected MajorPod trailing params: %+v", gotMaj)
	}
	if d := ResolveMaxHoldingDuration(18*time.Hour, profiles, "MAJOR_RSMOM_SHORT"); d != 48*time.Hour {
		t.Fatalf("expected MajorPod max holding 48h, got %v", d)
	}
	if s := ResolveStaleLossExitDuration(6*time.Hour, profiles, "MAJOR_RSMOM_SHORT"); s != 0 {
		t.Fatalf("expected MajorPod stale loss 0, got %v", s)
	}
}

