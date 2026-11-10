package strategy

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

// TestTier2Veto_ShortOnly_Vs_Auto_Vs_Both memverifikasi bahwa Tier 2 Soft Veto
// aktif melindungi posisi SHORT baik saat ALLOWED_SIDE=AUTO maupun SHORT_ONLY ketika BTC bullish,
// dan hanya di-bypass jika user secara eksplisit menyetel ALLOWED_SIDE=BOTH.
func TestTier2Veto_ShortOnly_Vs_Auto_Vs_Both(t *testing.T) {
	now := time.Now().UTC()
	snapShort, cfg := helperCreateValidShortSnapshot(now)
	cfg.BTCGateEnabled = true
	cfg.BTCGatePersistenceBars = 1

	// Setup BTC dalam kondisi Bullish Stabil:
	// Tren naik perlahan dari 80000 ke 84000 (EMA ~83000, VWAP = 82000)
	btcCandles := make([]model.Candle, 55)
	for i := 0; i < 55; i++ {
		p := 80000.0 + float64(i)*75.0 // Naik bertahap
		btcCandles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:       p,
			High:       p + 30.0,
			Low:        p - 10.0,
			Close:      p + 20.0,
			VolumeBase: 100.0,
		}
	}
	btcSnap := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandles,
		CurrentSessionVWAP: 82000.0,
		Now:                now,
	}

	// 1. Uji dengan ALLOWED_SIDE = "AUTO" -> Harus di-VETO
	cfgAuto := cfg
	cfgAuto.AllowedSide = "AUTO"
	_, okAuto, reasonAuto, _ := EvaluateSignal(
		snapShort, &btcSnap, 0, false, false,
		decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgAuto,
	)
	if okAuto || reasonAuto != "BTC_TREND_BULLISH_SHORT_VETO" {
		t.Fatalf("ekspektasi AUTO memblokir sinyal dengan BTC_TREND_BULLISH_SHORT_VETO, dapat ok=%v, reason=%s", okAuto, reasonAuto)
	}

	// 2. Uji dengan ALLOWED_SIDE = "SHORT_ONLY"
	// Skenario A: Default (BTCGateShortOnly = false / 9815362) -> Tier 2 bypass (SIGNAL_APPROVED)
	cfgShortDefault := cfg
	cfgShortDefault.AllowedSide = "SHORT_ONLY"
	cfgShortDefault.BTCGateShortOnly = false
	_, okShortDef, reasonShortDef, _ := EvaluateSignal(
		snapShort, &btcSnap, 0, false, false,
		decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShortDefault,
	)
	if !okShortDef || reasonShortDef != "SIGNAL_APPROVED" {
		t.Fatalf("ekspektasi SHORT_ONLY dengan flag default meloloskan sinyal (9815362 parity), dapat ok=%v, reason=%s", okShortDef, reasonShortDef)
	}

	// Skenario B: Flag Aktif (BTCGateShortOnly = true) -> Harus di-VETO dengan BTC_TREND_BULLISH_SHORT_VETO
	cfgShortVeto := cfg
	cfgShortVeto.AllowedSide = "SHORT_ONLY"
	cfgShortVeto.BTCGateShortOnly = true
	_, okShortVeto, reasonShortVeto, _ := EvaluateSignal(
		snapShort, &btcSnap, 0, false, false,
		decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShortVeto,
	)
	if okShortVeto || reasonShortVeto != "BTC_TREND_BULLISH_SHORT_VETO" {
		t.Fatalf("ekspektasi SHORT_ONLY dengan BTCGateShortOnly memblokir sinyal, dapat ok=%v, reason=%s", okShortVeto, reasonShortVeto)
	}

	// 3. Uji dengan ALLOWED_SIDE = "BOTH" -> Harus lolos karena user secara manual ingin bypass Tier 2
	cfgBoth := cfg
	cfgBoth.AllowedSide = "BOTH"
	_, okBoth, reasonBoth, _ := EvaluateSignal(
		snapShort, &btcSnap, 0, false, false,
		decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgBoth,
	)
	if !okBoth || reasonBoth != "SIGNAL_APPROVED" {
		t.Fatalf("ekspektasi BOTH meloloskan sinyal (Tier 2 bypass), dapat ok=%v, reason=%s", okBoth, reasonBoth)
	}
}

// TestTier2Veto_LongOnly memverifikasi bahwa posisi LONG pada ALLOWED_SIDE=LONG_ONLY
// juga terlindungi oleh veto Tier 2 ketika BTC sedang bearish (di bawah EMA & VWAP).
func TestTier2Veto_LongOnly(t *testing.T) {
	now := time.Now().UTC()
	snapLong, cfg := helperCreateValidLongSnapshot(now)
	cfg.BTCGateEnabled = true
	cfg.BTCGatePersistenceBars = 1

	// Setup BTC dalam kondisi Bearish Stabil:
	// Tren turun perlahan dari 84000 ke 80000 (EMA ~81000, VWAP = 83000)
	btcCandles := make([]model.Candle, 55)
	for i := 0; i < 55; i++ {
		p := 84000.0 - float64(i)*75.0 // Turun bertahap
		btcCandles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:       p,
			High:       p + 10.0,
			Low:        p - 30.0,
			Close:      p - 20.0,
			VolumeBase: 100.0,
		}
	}
	btcSnap := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandles,
		CurrentSessionVWAP: 83000.0,
		Now:                now,
	}

	// Uji dengan ALLOWED_SIDE = "LONG_ONLY" dan BTCGateShortOnly = true -> Harus di-VETO dengan BTC_TREND_BEARISH_LONG_VETO
	cfgLongOnly := cfg
	cfgLongOnly.AllowedSide = "LONG_ONLY"
	cfgLongOnly.BTCGateShortOnly = true
	_, okLongOnly, reasonLongOnly, _ := EvaluateSignal(
		snapLong, &btcSnap, 0, false, false,
		decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLongOnly,
	)
	if okLongOnly || reasonLongOnly != "BTC_TREND_BEARISH_LONG_VETO" {
		t.Fatalf("ekspektasi LONG_ONLY memblokir sinyal dengan BTC_TREND_BEARISH_LONG_VETO, dapat ok=%v, reason=%s", okLongOnly, reasonLongOnly)
	}

	// Uji dengan ALLOWED_SIDE = "BOTH" -> Harus lolos
	cfgBoth := cfg
	cfgBoth.AllowedSide = "BOTH"
	_, okBoth, reasonBoth, _ := EvaluateSignal(
		snapLong, &btcSnap, 0, false, false,
		decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgBoth,
	)
	if !okBoth || reasonBoth != "SIGNAL_APPROVED" {
		t.Fatalf("ekspektasi BOTH meloloskan sinyal LONG (Tier 2 bypass), dapat ok=%v, reason=%s", okBoth, reasonBoth)
	}
}
