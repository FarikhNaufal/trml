package strategy

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestBuildSafeDecimalOrderIntent(t *testing.T) {
	snap := model.MarketSnapshot{
		Symbol: "SOLUSDT",
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: time.Now().UTC(),
	}

	cfg := SizingConfig{
		RiskPerTradePct:       decimal.NewFromFloat(0.01),
		MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
		AccountLeverage:       5.0,
		SLMultiplier:          1.5,
		TPMultiplier:          3.0,
		MakerFeeRate:          decimal.NewFromFloat(0.0002),
		TakerFeeRate:          decimal.NewFromFloat(0.0005),
		SlippageRate:          decimal.NewFromFloat(0.0002),
	}

	equity := decimal.NewFromFloat(1000.0)
	avail := decimal.NewFromFloat(1000.0)
	rawEntry := 150.0
	atr14 := 3.0

	intent, ok := BuildSafeDecimalOrderIntent(snap, equity, avail, model.SideLong, rawEntry, atr14, cfg)
	if !ok {
		t.Fatalf("expected successful order intent creation")
	}

	if intent.Side != model.SideLong {
		t.Errorf("expected SideLong, got %s", intent.Side)
	}
	if !intent.LimitPrice.Equal(decimal.NewFromFloat(150.00)) {
		t.Errorf("expected 150.00, got %s", intent.LimitPrice)
	}
	// SL = 150 - (1.5 * 3) = 145.50
	if !intent.SLPrice.Equal(decimal.NewFromFloat(145.50)) {
		t.Errorf("expected SL 145.50, got %s", intent.SLPrice)
	}
	// TP = 150 + (3.0 * 3) = 159.00
	if !intent.TPPrice.Equal(decimal.NewFromFloat(159.00)) {
		t.Errorf("expected TP 159.00, got %s", intent.TPPrice)
	}
	if intent.Qty.LessThanOrEqual(decimal.Zero) {
		t.Errorf("expected positive Qty, got %s", intent.Qty)
	}
}

func TestEvaluatorStalenessGates(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        30 * time.Second,
		MaxSpreadPct:          0.03,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.50,
		MinOIDeltaPct:         0.20,
		MaxAdverseFundingRate: 0.0005,
	}

	// Skenario 1: Book Stale (> 5s)
	snapStaleBook := model.MarketSnapshot{
		Symbol:        "BTCUSDT",
		Now:           now,
		BookUpdatedAt: now.Add(-10 * time.Second), // 10s > 5s
	}

	_, ok, reason, _ := EvaluateSignal(snapStaleBook, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok || reason != "STALE_BBO_FEED" {
		t.Errorf("expected STALE_BBO_FEED, got ok=%v, reason=%s", ok, reason)
	}

	// Skenario 2: OI Warm-Up (PrevOI belum ada, sample 0)
	snapOIWarmup := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		Now:                now,
		BookUpdatedAt:      now,
		MarkPriceUpdatedAt: now,
		FundingUpdatedAt:   now,
		BestBid:            100.0,
		BestAsk:            100.01,
		CurrOpenInterest:   1000.0,
		CurrOIUpdatedAt:    now,
		PrevOpenInterest:   0.0,
		PrevOIUpdatedAt:    time.Time{}, // Zero timestamp
	}
	_, okWarm, reasonWarm, subReasonWarm := EvaluateSignal(snapOIWarmup, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okWarm || reasonWarm != "MISSING_OI_DATA" || subReasonWarm != "OI_WARMING_UP" {
		t.Errorf("expected MISSING_OI_DATA / OI_WARMING_UP, got ok=%v, reason=%s, subReason=%s", okWarm, reasonWarm, subReasonWarm)
	}
}

func TestEvaluateSignalVWAPPullbackLong(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	// 60 candles uptrend: EMA50 will be around 95-100, VWAP = 105
	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       95.0 + float64(i)*0.15,
			High:       96.0 + float64(i)*0.15,
			Low:        94.5 + float64(i)*0.15,
			Close:      95.5 + float64(i)*0.15,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = 100.0 + float64(i%7)*15.0 // Non-zero variance
	}

	vwap := 105.0

	// Impulse candle at i=55 (High >= VWAP + 1.0*ATR = 106.5)
	candles[55].High = 107.5
	candles[55].Close = 107.0

	// Latest candle (i=59): Pullback to VWAP and bounces green
	// Low <= VWAP + 0.35*ATR (105 + 0.525 = 105.525), Close > Open, Close >= VWAP
	candles[59].Open = 105.1
	candles[59].Low = 104.9 // mencium VWAP
	candles[59].High = 106.0
	candles[59].Close = 105.8 // memantul hijau di atas VWAP
	barDeltas[59] = 2000.0    // strong positive delta spike -> high Z-score

	snap := model.MarketSnapshot{
		Symbol:               "SOLUSDT",
		BestBid:              105.70,
		BestAsk:              105.72,
		BookUpdatedAt:        now.Add(-1 * time.Second),
		MarkPrice:            105.71,
		MarkPriceUpdatedAt:   now.Add(-2 * time.Second),
		FundingRate:          0.0001,
		FundingUpdatedAt:     now.Add(-10 * time.Second),
		PrevOpenInterest:     10000.0,
		PrevOIUpdatedAt:      now.Add(-5 * time.Minute),
		CurrOpenInterest:     10050.0, // +0.5%
		CurrOIUpdatedAt:      now.Add(-30 * time.Second),
		CurrentSessionVWAP:   vwap,
		ClosedCandles:        candles,
		ClosedBarDeltasUSDT:  barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	intent, ok, reason, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !ok || reason != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED for Long Pullback, got ok=%v, reason=%s", ok, reason)
	}
	if intent.Side != model.SideLong {
		t.Errorf("expected SideLong, got %s", intent.Side)
	}
	if !intent.LimitPrice.Equal(decimal.NewFromFloat(105.70)) {
		t.Errorf("expected LimitPrice 105.70, got %s", intent.LimitPrice)
	}
}

func TestEvaluateSignalVWAPPullbackShort(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	// 60 candles downtrend: EMA50 will be around 110, VWAP = 100
	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       115.0 - float64(i)*0.25,
			High:       115.5 - float64(i)*0.25,
			Low:        114.0 - float64(i)*0.25,
			Close:      114.5 - float64(i)*0.25,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = -100.0 - float64(i%7)*15.0 // Non-zero variance
	}

	vwap := 100.0

	// Impulse drop candle at i=55 (Low <= VWAP - 1.0*ATR = 98.5)
	candles[55].Low = 97.0
	candles[55].Close = 97.5

	// Latest candle (i=59): Retest up to VWAP and rejected red
	// High >= VWAP - 0.35*ATR (100 - 0.5 = 99.5), Close < Open, Close <= VWAP
	candles[59].Open = 99.9
	candles[59].High = 100.2 // mencium atap VWAP
	candles[59].Low = 99.0
	candles[59].Close = 99.2 // ditolak merah di bawah VWAP
	barDeltas[59] = -2000.0  // strong negative delta spike -> low negative Z-score

	snap := model.MarketSnapshot{
		Symbol:               "SOLUSDT",
		BestBid:              99.28,
		BestAsk:              99.30,
		BookUpdatedAt:        now.Add(-1 * time.Second),
		MarkPrice:            99.29,
		MarkPriceUpdatedAt:   now.Add(-2 * time.Second),
		FundingRate:          -0.0001,
		FundingUpdatedAt:     now.Add(-10 * time.Second),
		PrevOpenInterest:     10000.0,
		PrevOIUpdatedAt:      now.Add(-5 * time.Minute),
		CurrOpenInterest:     10050.0,
		CurrOIUpdatedAt:      now.Add(-30 * time.Second),
		CurrentSessionVWAP:   vwap,
		ClosedCandles:        candles,
		ClosedBarDeltasUSDT:  barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	intent, ok, reason, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !ok || reason != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED for Short Pullback, got ok=%v, reason=%s", ok, reason)
	}
	if intent.Side != model.SideShort {
		t.Errorf("expected SideShort, got %s", intent.Side)
	}
	if !intent.LimitPrice.Equal(decimal.NewFromFloat(99.30)) {
		t.Errorf("expected LimitPrice 99.30, got %s", intent.LimitPrice)
	}
}

func TestEvaluateSignalRejectsNoImpulse(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	// Flat candles without impulse
	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       100.0,
			High:       100.2,
			Low:        99.8,
			Close:      100.1,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = 100.0 + float64(i%7)*15.0 // Non-zero variance
	}

	snap := model.MarketSnapshot{
		Symbol:               "SOLUSDT",
		BestBid:              100.10,
		BestAsk:              100.12,
		BookUpdatedAt:        now.Add(-1 * time.Second),
		MarkPrice:            100.11,
		MarkPriceUpdatedAt:   now.Add(-2 * time.Second),
		FundingRate:          0.0001,
		FundingUpdatedAt:     now.Add(-10 * time.Second),
		PrevOpenInterest:     10000.0,
		PrevOIUpdatedAt:      now.Add(-5 * time.Minute),
		CurrOpenInterest:     10050.0,
		CurrOIUpdatedAt:      now.Add(-30 * time.Second),
		CurrentSessionVWAP:   100.0,
		ClosedCandles:        candles,
		ClosedBarDeltasUSDT:  barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	_, ok, reason, subReason := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok || reason != "CONDITIONS_NOT_MET" {
		t.Fatalf("expected CONDITIONS_NOT_MET due to no prior impulse, got ok=%v, reason=%s", ok, reason)
	}
	if !strings.Contains(subReason, "NO_PRIOR_IMPULSE") {
		t.Errorf("expected subReason to contain NO_PRIOR_IMPULSE, got %s", subReason)
	}
}

func TestEvaluateSignalVWAPPullbackMarketOrder(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		EntryOrderType:        "MARKET",
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	// Long setup
	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       95.0 + float64(i)*0.15,
			High:       96.0 + float64(i)*0.15,
			Low:        94.5 + float64(i)*0.15,
			Close:      95.5 + float64(i)*0.15,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = 100.0 + float64(i%7)*15.0
	}
	vwap := 105.0
	candles[55].High = 107.5
	candles[55].Close = 107.0

	candles[59].Open = 105.1
	candles[59].Low = 104.9
	candles[59].High = 106.0
	candles[59].Close = 105.8
	barDeltas[59] = 2000.0

	snapLong := model.MarketSnapshot{
		Symbol:               "SOLUSDT",
		BestBid:              105.70,
		BestAsk:              105.72,
		BookUpdatedAt:        now.Add(-1 * time.Second),
		MarkPrice:            105.71,
		MarkPriceUpdatedAt:   now.Add(-2 * time.Second),
		FundingRate:          0.0001,
		FundingUpdatedAt:     now.Add(-10 * time.Second),
		PrevOpenInterest:     10000.0,
		PrevOIUpdatedAt:      now.Add(-5 * time.Minute),
		CurrOpenInterest:     10050.0,
		CurrOIUpdatedAt:      now.Add(-30 * time.Second),
		CurrentSessionVWAP:   vwap,
		ClosedCandles:        candles,
		ClosedBarDeltasUSDT:  barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	intentLong, ok, reason, _ := EvaluateSignal(snapLong, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !ok || reason != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED for Long Market, got ok=%v, reason=%s", ok, reason)
	}
	// Under MARKET entry, LONG must execute at BestAsk (105.72), NOT BestBid (105.70)
	if !intentLong.LimitPrice.Equal(decimal.NewFromFloat(105.72)) {
		t.Errorf("expected MARKET Long price BestAsk 105.72, got %s", intentLong.LimitPrice)
	}
}

func TestEvaluateSignalRejectsEntryTooFarFromVWAP(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   18,
		MinImpulseATR:         1.0,
		MaxEntryDistanceATR:   0.85,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       95.0 + float64(i)*0.15,
			High:       96.0 + float64(i)*0.15,
			Low:        94.5 + float64(i)*0.15,
			Close:      95.5 + float64(i)*0.15,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = 100.0 + float64(i%7)*15.0
	}
	vwap := 105.0
	candles[55].High = 107.5
	candles[55].Close = 107.0

	// Candle 59 closes at 108.0 (3.0 away from VWAP = 105.0, ATR is ~1.5 -> dist is ~2.0 ATR > 0.85 ATR)
	candles[59].Open = 105.1
	candles[59].Low = 104.9
	candles[59].High = 108.5
	candles[59].Close = 108.0
	barDeltas[59] = 2000.0

	snap := model.MarketSnapshot{
		Symbol:               "SOLUSDT",
		BestBid:              107.98,
		BestAsk:              108.00,
		BookUpdatedAt:        now.Add(-1 * time.Second),
		MarkPrice:            107.99,
		MarkPriceUpdatedAt:   now.Add(-2 * time.Second),
		FundingRate:          0.0001,
		FundingUpdatedAt:     now.Add(-10 * time.Second),
		PrevOpenInterest:     10000.0,
		PrevOIUpdatedAt:      now.Add(-5 * time.Minute),
		CurrOpenInterest:     10050.0,
		CurrOIUpdatedAt:      now.Add(-30 * time.Second),
		CurrentSessionVWAP:   vwap,
		ClosedCandles:        candles,
		ClosedBarDeltasUSDT:  barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	_, ok, reason, subReason := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok || reason != "ENTRY_TOO_FAR_FROM_VWAP" {
		t.Fatalf("expected ENTRY_TOO_FAR_FROM_VWAP, got ok=%v, reason=%s", ok, reason)
	}
	if !strings.Contains(subReason, "MAX_0.85_ATR") {
		t.Errorf("expected subReason to contain MAX_0.85_ATR, got %s", subReason)
	}
}

func TestEvaluateSignalBTCMacroGateFlashDumpVetoLong(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		BTCGateEnabled:        true,
		BTCMax5mDumpPct:       0.60,
		BTCMax5mPumpPct:       0.60,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.5,
			TPMultiplier:          2.25,
		},
	}

	// 1. Setup Altcoin Long yang valid
	candles := make([]model.Candle, 60)
	deltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		p := 100.0 + float64(i)*0.1
		candles[i] = model.Candle{
			OpenTime:  now.Add(time.Duration(i-60) * 5 * time.Minute),
			CloseTime: now.Add(time.Duration(i-59) * 5 * time.Minute),
			Open:      p, High: p + 1.0, Low: p - 0.5, Close: p + 0.5,
			VolumeBase: 1000.0,
		}
		deltas[i] = 100.0 + float64(i%7)*15.0
	}
	candles[50].High = 110.0 // Prior bullish impulse
	candles[58].Low = 104.5  // Retest VWAP
	// Bar terkini bounce
	candles[59] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      105.0, High: 106.0, Low: 104.9, Close: 105.8,
		VolumeBase: 2000.0,
	}
	deltas[59] = 2000.0 // Z-score tinggi

	altSnap := model.MarketSnapshot{
		Symbol:             "ALTUSDT",
		BestBid:            105.70,
		BestAsk:            105.72,
		BookUpdatedAt:      now,
		MarkPriceUpdatedAt: now,
		FundingUpdatedAt:   now,
		CurrentSessionVWAP: 105.0,
		ClosedCandles:      candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:   1000.0,
		CurrOpenInterest:   1050.0,
		PrevOIUpdatedAt:    now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:    now,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	// 2. BTC Snapshot sedang FLASH DUMP (-0.95% > 0.60%)
	btcCandles := make([]model.Candle, 55)
	for i := 0; i < 54; i++ {
		btcCandles[i] = model.Candle{
			OpenTime:  now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:      84000.0, High: 84100.0, Low: 83900.0, Close: 84000.0,
			VolumeBase: 100.0,
		}
	}
	// Bar 5m terakhir dump tajam
	btcCandles[54] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      84000.0, High: 84050.0, Low: 83100.0, Close: 83200.0, // -0.95%
		VolumeBase: 500.0,
	}
	btcSnapDump := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandles,
		CurrentSessionVWAP: 84000.0,
		Now:                now,
	}

	// Evaluasi harus di-VETO oleh Tier 1: BTC_FLASH_DUMP
	_, okDump, reasonDump, subReasonDump := EvaluateSignal(altSnap, &btcSnapDump, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okDump || reasonDump != "BTC_FLASH_DUMP" {
		t.Fatalf("expected BTC_FLASH_DUMP veto for Long during BTC crash, got ok=%v, reason=%s", okDump, reasonDump)
	}
	if !strings.Contains(subReasonDump, "BTC_5M_DUMP") {
		t.Errorf("expected subReason to contain BTC_5M_DUMP, got %s", subReasonDump)
	}

	// 3. Jika BTC normal / tenang (Open 84000, Close 84010 = +0.01%, tidak bearish), Long harus DISETUJUI (idiosinkronik altcoin)
	btcCandles[54].Close = 84010.0
	btcSnapNormal := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandles,
		CurrentSessionVWAP: 84000.0,
		Now:                now,
	}
	intentNormal, okNormal, reasonNormal, _ := EvaluateSignal(altSnap, &btcSnapNormal, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !okNormal || reasonNormal != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED for Long during calm BTC, got ok=%v, reason=%s", okNormal, reasonNormal)
	}
	if intentNormal.Side != model.SideLong {
		t.Errorf("expected SideLong, got %s", intentNormal.Side)
	}
}

func TestEvaluateSignalBTCMacroGateTrendBullishVetoShort(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		BTCGateEnabled:        true,
		BTCMax5mDumpPct:       0.60,
		BTCMax5mPumpPct:       0.60,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.5,
			TPMultiplier:          2.25,
		},
	}

	// 1. Setup Altcoin Short yang valid secara teknikal
	candles := make([]model.Candle, 60)
	deltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		p := 105.0 - float64(i)*0.1
		candles[i] = model.Candle{
			OpenTime:  now.Add(time.Duration(i-60) * 5 * time.Minute),
			CloseTime: now.Add(time.Duration(i-59) * 5 * time.Minute),
			Open:      p, High: p + 0.5, Low: p - 1.0, Close: p - 0.5,
			VolumeBase: 1000.0,
		}
		deltas[i] = -100.0 - float64(i%7)*15.0
	}
	candles[50].Low = 95.0   // Prior bearish impulse
	candles[58].High = 99.5  // Retest VWAP
	// Bar terkini bounce ke bawah
	candles[59] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      99.5, High: 99.6, Low: 99.1, Close: 99.2,
		VolumeBase: 2000.0,
	}
	deltas[59] = -2000.0 // Delta Z-score negatif kuat

	altSnapShort := model.MarketSnapshot{
		Symbol:             "TIAUSDT",
		BestBid:            99.20,
		BestAsk:            99.21,
		BookUpdatedAt:      now,
		MarkPriceUpdatedAt: now,
		FundingUpdatedAt:   now,
		CurrentSessionVWAP: 99.5,
		ClosedCandles:      candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:   1000.0,
		CurrOpenInterest:   1050.0,
		PrevOIUpdatedAt:    now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:    now,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	// 2. BTC sedang Bullish Uptrend kuat (Close 85000 > EMA 84000 dan > VWAP 84200)
	btcCandlesBull := make([]model.Candle, 55)
	for i := 0; i < 55; i++ {
		p := 83000.0 + float64(i)*40.0 // Tren naik stabil
		btcCandlesBull[i] = model.Candle{
			OpenTime:  now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:      p, High: p + 30.0, Low: p - 10.0, Close: p + 20.0,
			VolumeBase: 100.0,
		}
	}
	btcSnapBull := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandlesBull,
		CurrentSessionVWAP: 84200.0, // VWAP di bawah Close
		Now:                now,
	}

	// Sinyal SHORT altcoin harus di-VETO oleh Tier 2 (BTC_TREND_BULLISH_SHORT_VETO)
	_, okShort, reasonShort, subReasonShort := EvaluateSignal(altSnapShort, &btcSnapBull, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okShort || reasonShort != "BTC_TREND_BULLISH_SHORT_VETO" {
		t.Fatalf("expected BTC_TREND_BULLISH_SHORT_VETO for Short during bullish BTC drift, got ok=%v, reason=%s", okShort, reasonShort)
	}
	if !strings.Contains(subReasonShort, "BTC_CLOSE") {
		t.Errorf("expected subReason to contain BTC_CLOSE, got %s", subReasonShort)
	}
}

func TestEvaluateSignalBTCMacroGateTrendBearishVetoLong(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		BTCGateEnabled:        true,
		BTCMax5mDumpPct:       0.60,
		BTCMax5mPumpPct:       0.60,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.5,
			TPMultiplier:          2.25,
		},
	}

	// 1. Setup Altcoin Long yang valid
	candles := make([]model.Candle, 60)
	deltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		p := 100.0 + float64(i)*0.1
		candles[i] = model.Candle{
			OpenTime:  now.Add(time.Duration(i-60) * 5 * time.Minute),
			CloseTime: now.Add(time.Duration(i-59) * 5 * time.Minute),
			Open:      p, High: p + 1.0, Low: p - 0.5, Close: p + 0.5,
			VolumeBase: 1000.0,
		}
		deltas[i] = 100.0 + float64(i%7)*15.0
	}
	candles[50].High = 110.0 // Prior bullish impulse
	candles[58].Low = 104.5  // Retest VWAP
	candles[59] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      105.0, High: 106.0, Low: 104.9, Close: 105.8,
		VolumeBase: 2000.0,
	}
	deltas[59] = 200.0

	altSnapLong := model.MarketSnapshot{
		Symbol:              "NEARUSDT",
		BestBid:             105.78,
		BestAsk:             105.80,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  104.5,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:   1000.0,
		CurrOpenInterest:   1050.0,
		PrevOIUpdatedAt:    now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:    now,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	// 2. BTC sedang Bearish Downtrend (Close 82000 < EMA 84000 dan < VWAP 84200)
	btcCandlesBear := make([]model.Candle, 55)
	for i := 0; i < 55; i++ {
		p := 85000.0 - float64(i)*50.0 // Tren turun perlahan
		btcCandlesBear[i] = model.Candle{
			OpenTime:  now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:      p, High: p + 10.0, Low: p - 30.0, Close: p - 20.0,
			VolumeBase: 100.0,
		}
	}
	btcSnapBear := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandlesBear,
		CurrentSessionVWAP: 84200.0, // VWAP di atas Close
		Now:                now,
	}

	// Sinyal LONG altcoin harus di-VETO oleh Tier 2 (BTC_TREND_BEARISH_LONG_VETO)
	_, okLong, reasonLong, subReasonLong := EvaluateSignal(altSnapLong, &btcSnapBear, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okLong || reasonLong != "BTC_TREND_BEARISH_LONG_VETO" {
		t.Fatalf("expected BTC_TREND_BEARISH_LONG_VETO for Long during bearish BTC, got ok=%v, reason=%s", okLong, reasonLong)
	}
	if !strings.Contains(subReasonLong, "BTC_CLOSE") {
		t.Errorf("expected subReason to contain BTC_CLOSE, got %s", subReasonLong)
	}
}

func helperCreateValidLongSnapshot(now time.Time) (model.MarketSnapshot, EvaluatorConfig) {
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         0.20,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		BTCGateEnabled:        false,
		AllowedSide:           "BOTH",
		MaxFrenzyRVOL:         2.50,
		MaxCounterImpulseATR:  1.50,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       95.0 + float64(i)*0.15,
			High:       96.0 + float64(i)*0.15,
			Low:        94.5 + float64(i)*0.15,
			Close:      95.5 + float64(i)*0.15,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = 100.0 + float64(i%7)*15.0
	}

	vwap := 105.0
	candles[55].High = 107.5
	candles[55].Close = 107.0

	candles[59].Open = 105.1
	candles[59].Low = 104.9
	candles[59].High = 106.0
	candles[59].Close = 105.8
	candles[59].VolumeBase = 1000.0
	barDeltas[59] = 2000.0

	snap := model.MarketSnapshot{
		Symbol:              "SOLUSDT",
		BestBid:             105.70,
		BestAsk:             105.72,
		BookUpdatedAt:       now.Add(-1 * time.Second),
		MarkPrice:           105.71,
		MarkPriceUpdatedAt:  now.Add(-2 * time.Second),
		FundingRate:         0.0001,
		FundingUpdatedAt:    now.Add(-10 * time.Second),
		PrevOpenInterest:    10000.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOpenInterest:    10050.0,
		CurrOIUpdatedAt:     now.Add(-30 * time.Second),
		CurrentSessionVWAP:  vwap,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}
	return snap, cfg
}

func helperCreateValidShortSnapshot(now time.Time) (model.MarketSnapshot, EvaluatorConfig) {
	cfg := EvaluatorConfig{
		MaxSlots:              3,
		MaxBookStaleness:      5 * time.Second,
		MaxMarkStaleness:      15 * time.Second,
		MaxDataStaleness:      330 * time.Second,
		MinOIInterval:         4 * time.Minute,
		MaxOIInterval:         7 * time.Minute,
		MaxOIStaleness:        300 * time.Second,
		MaxSpreadPct:          0.05,
		MinNATRPct:            0.20,
		MaxNATRPct:            5.00,
		EMAPeriod:             50,
		ATRPeriod:             14,
		DeltaZScoreWindow:     50,
		MinBarDeltaZScore:     1.20,
		MinOIDeltaPct:         -0.10,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		MinBounceZScore:       1.20,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		BTCGateEnabled:        false,
		AllowedSide:           "BOTH",
		MaxFrenzyRVOL:         2.50,
		MaxCounterImpulseATR:  1.50,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.0,
			TPMultiplier:          2.0,
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	candles := make([]model.Candle, 60)
	barDeltas := make([]float64, 60)
	for i := 0; i < 60; i++ {
		candles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(-60+i) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(-59+i) * 5 * time.Minute),
			Open:       115.0 - float64(i)*0.25,
			High:       115.5 - float64(i)*0.25,
			Low:        114.0 - float64(i)*0.25,
			Close:      114.5 - float64(i)*0.25,
			VolumeBase: 1000.0,
		}
		barDeltas[i] = -100.0 - float64(i%7)*15.0
	}

	vwap := 100.0
	candles[55].Low = 97.0
	candles[55].Close = 97.5

	candles[59].Open = 99.9
	candles[59].High = 100.2
	candles[59].Low = 99.0
	candles[59].Close = 99.2
	candles[59].VolumeBase = 1000.0
	barDeltas[59] = -2000.0

	snap := model.MarketSnapshot{
		Symbol:              "SOLUSDT",
		BestBid:             99.28,
		BestAsk:             99.30,
		BookUpdatedAt:       now.Add(-1 * time.Second),
		MarkPrice:           99.29,
		MarkPriceUpdatedAt:  now.Add(-2 * time.Second),
		FundingRate:         -0.0001,
		FundingUpdatedAt:    now.Add(-10 * time.Second),
		PrevOpenInterest:    10000.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOpenInterest:    10050.0,
		CurrOIUpdatedAt:     now.Add(-30 * time.Second),
		CurrentSessionVWAP:  vwap,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: barDeltas,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}
	return snap, cfg
}

func TestEvaluateSignal_AllowedSide(t *testing.T) {
	now := time.Now().UTC()

	// 1. SHORT_ONLY blocks LONG
	snapLong, cfgLong := helperCreateValidLongSnapshot(now)
	cfgLong.AllowedSide = "SHORT_ONLY"
	_, ok, reason, subReason := EvaluateSignal(snapLong, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if ok || reason != "SIDE_DISABLED_SHORT_ONLY" || subReason != "LONG_DISABLED_BY_CONFIG" {
		t.Fatalf("expected SIDE_DISABLED_SHORT_ONLY, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}

	// 2. LONG_ONLY blocks SHORT
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	cfgShort.AllowedSide = "LONG_ONLY"
	_, ok, reason, subReason = EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if ok || reason != "SIDE_DISABLED_LONG_ONLY" || subReason != "SHORT_DISABLED_BY_CONFIG" {
		t.Fatalf("expected SIDE_DISABLED_LONG_ONLY, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}

	// 3. BOTH allows SHORT
	cfgShort.AllowedSide = "BOTH"
	_, ok, reason, _ = EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if !ok || reason != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED with BOTH, got ok=%v, reason=%s", ok, reason)
	}
}

func TestEvaluateSignal_AntiFrenzyPumpVeto(t *testing.T) {
	now := time.Now().UTC()
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	cfgShort.MaxFrenzyRVOL = 2.50
	cfgShort.MaxCounterImpulseATR = 1.50

	// Buat candle i=58 menjadi green expansion bar yang besar (> 1.5 * ATR)
	// ATR rata-rata ~1.5, jadi expansion 3.5 sudah > 2.0x ATR
	snapShort.ClosedCandles[58].Open = 96.0
	snapShort.ClosedCandles[58].Close = 99.8
	snapShort.ClosedCandles[58].High = 100.0
	snapShort.ClosedCandles[58].Low = 95.8
	// RVOL lonjak di atas 2.50 (misal 3.50)
	snapShort.ClosedCandles[59].VolumeBase = 3500.0

	_, ok, reason, subReason := EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if ok || reason != "EXPLOSIVE_PUMP_FRENZY" {
		t.Fatalf("expected EXPLOSIVE_PUMP_FRENZY, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}
	if !strings.Contains(subReason, "RVOL_") || !strings.Contains(subReason, "EXPANSION_BAR_") {
		t.Errorf("expected subReason to contain RVOL_ and EXPANSION_BAR_, got %s", subReason)
	}
}

func TestEvaluateSignal_AntiFrenzyDumpVeto(t *testing.T) {
	now := time.Now().UTC()
	snapLong, cfgLong := helperCreateValidLongSnapshot(now)
	cfgLong.MaxFrenzyRVOL = 2.50
	cfgLong.MaxCounterImpulseATR = 1.50

	// Buat candle i=58 menjadi red dump bar yang besar (> 1.5 * ATR)
	snapLong.ClosedCandles[58].Open = 108.0
	snapLong.ClosedCandles[58].Close = 103.5
	snapLong.ClosedCandles[58].High = 108.2
	snapLong.ClosedCandles[58].Low = 103.3
	// RVOL lonjak di atas 2.50
	snapLong.ClosedCandles[59].VolumeBase = 3500.0

	_, ok, reason, subReason := EvaluateSignal(snapLong, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if ok || reason != "PANIC_DUMP_FRENZY" {
		t.Fatalf("expected PANIC_DUMP_FRENZY, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}
	if !strings.Contains(subReason, "RVOL_") || !strings.Contains(subReason, "DUMP_BAR_") {
		t.Errorf("expected subReason to contain RVOL_ and DUMP_BAR_, got %s", subReason)
	}
}

func TestEvaluateSignal_BTCGate_BearishLongVeto_OrCondition(t *testing.T) {
	now := time.Now().UTC()
	snapLong, cfgLong := helperCreateValidLongSnapshot(now)
	cfgLong.AllowedSide = "AUTO"
	cfgLong.BTCGateEnabled = true
	cfgLong.BTCMax5mDumpPct = 0.60
	cfgLong.BTCMax5mPumpPct = 0.60

	// 1. Kasus A: BTC berada di bawah EMA (83000 < 84000), meskipun di atas VWAP (83000 > 82000)
	btcCandlesA := make([]model.Candle, 55)
	for i := 0; i < 55; i++ {
		p := 85000.0 - float64(i)*50.0 // Tren turun, Close terakhir ~82300, EMA ~84000
		btcCandlesA[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:       p, High: p + 10.0, Low: p - 30.0, Close: p - 20.0,
			VolumeBase: 100.0,
		}
	}
	btcSnapA := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandlesA,
		CurrentSessionVWAP: 82000.0, // VWAP di bawah close (close > VWAP), tapi close < EMA
		Now:                now,
	}

	_, okA, reasonA, _ := EvaluateSignal(snapLong, &btcSnapA, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if okA || reasonA != "BTC_TREND_BEARISH_LONG_VETO" {
		t.Fatalf("expected BTC_TREND_BEARISH_LONG_VETO when BTC below EMA (even if above VWAP), got ok=%v, reason=%s", okA, reasonA)
	}

	// 2. Kasus B: BTC berada di atas EMA (85000 > 84000), tapi di bawah VWAP (85000 < 86000)
	btcCandlesB := make([]model.Candle, 55)
	for i := 0; i < 55; i++ {
		p := 83000.0 + float64(i)*50.0 // Tren naik perlahan, EMA ~84000, Close ~85700
		btcCandlesB[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:       p, High: p + 30.0, Low: p - 10.0, Close: p + 20.0,
			VolumeBase: 100.0,
		}
	}
	btcSnapB := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandlesB,
		CurrentSessionVWAP: 87000.0, // VWAP di atas close (close < VWAP), meskipun close > EMA
		Now:                now,
	}

	_, okB, reasonB, _ := EvaluateSignal(snapLong, &btcSnapB, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if okB || reasonB != "BTC_TREND_BEARISH_LONG_VETO" {
		t.Fatalf("expected BTC_TREND_BEARISH_LONG_VETO when BTC below VWAP (even if above EMA), got ok=%v, reason=%s", okB, reasonB)
	}

	// 3. Kasus C: ALLOWED_SIDE="BOTH" mengizinkan LONG meskipun tren BTC bearish (Tier 2 di-bypass)
	cfgLong.AllowedSide = "BOTH"
	_, okC, reasonC, _ := EvaluateSignal(snapLong, &btcSnapA, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if !okC || reasonC != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED when ALLOWED_SIDE=BOTH (bypassing Tier 2 soft veto), got ok=%v, reason=%s", okC, reasonC)
	}
}





