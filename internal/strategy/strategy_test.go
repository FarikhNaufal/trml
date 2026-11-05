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
		CurrOpenInterest:    10050.0, // +0.5%
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
		Symbol:              "SOLUSDT",
		BestBid:             100.10,
		BestAsk:             100.12,
		BookUpdatedAt:       now.Add(-1 * time.Second),
		MarkPrice:           100.11,
		MarkPriceUpdatedAt:  now.Add(-2 * time.Second),
		FundingRate:         0.0001,
		FundingUpdatedAt:    now.Add(-10 * time.Second),
		PrevOpenInterest:    10000.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOpenInterest:    10050.0,
		CurrOIUpdatedAt:     now.Add(-30 * time.Second),
		CurrentSessionVWAP:  100.0,
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
		Symbol:              "SOLUSDT",
		BestBid:             107.98,
		BestAsk:             108.00,
		BookUpdatedAt:       now.Add(-1 * time.Second),
		MarkPrice:           107.99,
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

	_, ok, reason, subReason := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok || reason != "ENTRY_TOO_FAR_FROM_VWAP" {
		t.Fatalf("expected ENTRY_TOO_FAR_FROM_VWAP, got ok=%v, reason=%s", ok, reason)
	}
	if !strings.Contains(subReason, "MAX_0.85_ATR") {
		t.Errorf("expected subReason to contain MAX_0.85_ATR, got %s", subReason)
	}
}

func TestEvaluateSignalEntryTooFarFromEMA200(t *testing.T) {
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
		MaxEntryDistanceATR:   1.50,
		MaxEMA200DistanceATR:  3.20,
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

	// Candle 59 closes at 110.0 (dist from EMA ~100.0 is ~10.0, ATR is ~1.5 -> dist is ~6.6 ATR > 3.20 ATR)
	candles[59].Open = 105.0
	candles[59].Low = 104.8
	candles[59].High = 110.5
	candles[59].Close = 110.0
	barDeltas[59] = 2000.0

	snap := model.MarketSnapshot{
		Symbol:              "SOLUSDT",
		BestBid:             109.98,
		BestAsk:             110.00,
		BookUpdatedAt:       now.Add(-1 * time.Second),
		MarkPrice:           109.99,
		MarkPriceUpdatedAt:  now.Add(-2 * time.Second),
		FundingRate:         0.0001,
		FundingUpdatedAt:    now.Add(-10 * time.Second),
		PrevOpenInterest:    10000.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOpenInterest:    10050.0,
		CurrOIUpdatedAt:     now.Add(-30 * time.Second),
		CurrentSessionVWAP:  109.8, // VWAP dekat sehingga tidak kena ENTRY_TOO_FAR_FROM_VWAP
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

	_, ok, reason, subReason := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok || reason != "ENTRY_TOO_FAR_FROM_EMA200" {
		t.Fatalf("expected ENTRY_TOO_FAR_FROM_EMA200, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}
	if !strings.Contains(subReason, "MAX_3.20_ATR") {
		t.Errorf("expected subReason to contain MAX_3.20_ATR, got %s", subReason)
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
		Symbol:              "ALTUSDT",
		BestBid:             105.70,
		BestAsk:             105.72,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  105.0,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:    1000.0,
		CurrOpenInterest:    1050.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
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
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     84000.0, High: 84100.0, Low: 83900.0, Close: 84000.0,
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
	for i := 50; i <= 54; i++ {
		btcCandles[i].Close = 84010.0
	}
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
	candles[50].Low = 95.0  // Prior bearish impulse
	candles[58].High = 99.5 // Retest VWAP
	// Bar terkini bounce ke bawah
	candles[59] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      99.5, High: 99.6, Low: 99.1, Close: 99.2,
		VolumeBase: 2000.0,
	}
	deltas[59] = -2000.0 // Delta Z-score negatif kuat

	altSnapShort := model.MarketSnapshot{
		Symbol:              "TIAUSDT",
		BestBid:             99.20,
		BestAsk:             99.21,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  99.5,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:    1000.0,
		CurrOpenInterest:    1050.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
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
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     p, High: p + 30.0, Low: p - 10.0, Close: p + 20.0,
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
		PrevOpenInterest:    1000.0,
		CurrOpenInterest:    1050.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
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
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     p, High: p + 10.0, Low: p - 30.0, Close: p - 20.0,
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
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     p, High: p + 10.0, Low: p - 30.0, Close: p - 20.0,
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
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     p, High: p + 30.0, Low: p - 10.0, Close: p + 20.0,
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

func TestEvaluateSignal_AsymmetricHurdle(t *testing.T) {
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
		MinBounceZScore:       1.20,
		MinBounceZScoreLong:   1.50, // LONG hurdle tinggi
		MinBounceZScoreShort:  1.20, // SHORT hurdle standar
		MinOIDeltaLongPct:     0.12, // LONG OI hurdle tinggi
		MinOIDeltaShortPct:    -0.02,
		MaxAdverseFundingRate: 0.0005,
		PullbackToleranceATR:  0.35,
		LookbackImpulseBars:   12,
		MinImpulseATR:         1.0,
		AllowedSide:           "BOTH",
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.5,
			TPMultiplier:          2.25,
		},
	}

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
		deltas[i] = 100.0
	}
	candles[50].High = 110.0 // Prior bullish impulse
	candles[58].Low = 104.5  // Retest VWAP
	candles[59] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      105.0, High: 106.0, Low: 104.9, Close: 105.8,
		VolumeBase: 2000.0,
	}

	snap := model.MarketSnapshot{
		Symbol:              "ALTUSDT",
		BestBid:             105.70,
		BestAsk:             105.72,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  105.0,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:    1000.0,
		CurrOpenInterest:    1000.8, // +0.08% OI (cukup untuk Short -0.02%, tapi < 0.12% Long)
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	// 1. Z-score 1.35 (antara 1.20 dan 1.50) -> LONG ditolak karena hurdle 1.50 belum tercapai
	deltas[59] = 100.0 // baseline deviasi rendah
	// Hitung delta agar zscore ~1.35
	// Tapi cara termudah adalah set zscore langsung lewat deltas
	// ClosedBarDeltasUSDT: deltas[0..49]=0, deltas[50..58]=0, delta[59]
	for i := 0; i < 59; i++ {
		deltas[i] = 0.0
	}
	// Standar deviasi = 0 jika semua 0, jadi beri sedikit variasi
	for i := 0; i < 59; i++ {
		if i%2 == 0 {
			deltas[i] = 10.0
		} else {
			deltas[i] = -10.0
		}
	}
	// Stdev ~10.0. delta[59] = 13.5 -> Z-Score ~1.35
	deltas[59] = 13.5
	snap.ClosedBarDeltasUSDT = deltas

	_, okLong, reasonLong, subLong := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okLong {
		t.Fatalf("expected LONG to fail asymmetric hurdle (Z=1.35 < 1.50 and OI=+0.08%% < +0.12%%), got approved")
	}
	if reasonLong != "CONDITIONS_NOT_MET" || (!strings.Contains(subLong, "WEAK_DELTA_ZSCORE") && !strings.Contains(subLong, "LOW_OI_DELTA")) {
		t.Errorf("expected failure due to weak delta or low OI, got reason=%s subReason=%s", reasonLong, subLong)
	}

	// 2. Ketika Z-Score dinaikkan ke > 1.50 (delta 20.0) dan OI dinaikkan ke +0.15% (1001.5)
	deltas[59] = 20.0
	snap.ClosedBarDeltasUSDT = deltas
	snap.CurrOpenInterest = 1001.5 // +0.15%

	intentApproved, okApproved, reasonApproved, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !okApproved || reasonApproved != "SIGNAL_APPROVED" {
		t.Fatalf("expected LONG approved after meeting higher hurdle (Z=2.0, OI=+0.15%%), got ok=%v, reason=%s", okApproved, reasonApproved)
	}
	if intentApproved.Side != model.SideLong {
		t.Errorf("expected SideLong, got %s", intentApproved.Side)
	}
}

func TestEvaluateSignal_BTCGatePersistenceBuffer(t *testing.T) {
	now := time.Now().UTC()
	cfg := EvaluatorConfig{
		MaxSlots:               3,
		MaxBookStaleness:       5 * time.Second,
		MaxMarkStaleness:       15 * time.Second,
		MaxDataStaleness:       330 * time.Second,
		MinOIInterval:          4 * time.Minute,
		MaxOIInterval:          7 * time.Minute,
		MaxOIStaleness:         300 * time.Second,
		MaxSpreadPct:           0.05,
		MinNATRPct:             0.20,
		MaxNATRPct:             5.00,
		EMAPeriod:              50,
		ATRPeriod:              14,
		DeltaZScoreWindow:      50,
		MinBarDeltaZScore:      1.20,
		MinOIDeltaPct:          -0.10,
		MaxAdverseFundingRate:  0.0005,
		PullbackToleranceATR:   0.35,
		MinBounceZScore:        1.20,
		LookbackImpulseBars:    12,
		MinImpulseATR:          1.0,
		BTCGateEnabled:         true,
		BTCMax5mDumpPct:        0.60,
		BTCMax5mPumpPct:        0.60,
		BTCGatePersistenceBars: 3,
		AllowedSide:            "AUTO",
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.01),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.30),
			AccountLeverage:       5.0,
			SLMultiplier:          1.5,
			TPMultiplier:          2.25,
		},
	}

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
	candles[50].High = 110.0
	candles[58].Low = 104.5
	candles[59] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      105.0, High: 106.0, Low: 104.9, Close: 105.8,
		VolumeBase: 2000.0,
	}
	deltas[59] = 2000.0

	altSnap := model.MarketSnapshot{
		Symbol:              "ALTUSDT",
		BestBid:             105.70,
		BestAsk:             105.72,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		CurrentSessionVWAP:  105.0,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
		PrevOpenInterest:    1000.0,
		CurrOpenInterest:    1050.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinQty:      decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	// Buat 55 candle BTC di mana candle 0..53 berada di bawah EMA (84000)
	// dan hanya candle 54 yang memantul ke atas EMA (84050) -> Whipsaw Trap!
	btcCandles := make([]model.Candle, 55)
	for i := 0; i < 54; i++ {
		btcCandles[i] = model.Candle{
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     84000.0, High: 84020.0, Low: 83980.0, Close: 84000.0,
			VolumeBase: 100.0,
		}
	}
	btcCandles[54] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      84000.0, High: 84100.0, Low: 84000.0, Close: 84050.0, // Pantulan 1 bar
		VolumeBase: 200.0,
	}

	btcSnap := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandles,
		CurrentSessionVWAP: 84000.0,
		Now:                now,
	}

	// Evaluasi Long: Harus di-VETO oleh BTC_TREND_LONG_VETO_PERSISTENCE
	_, okWhipsaw, reasonWhipsaw, _ := EvaluateSignal(altSnap, &btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okWhipsaw || reasonWhipsaw != "BTC_TREND_LONG_VETO_PERSISTENCE" {
		t.Fatalf("expected BTC_TREND_LONG_VETO_PERSISTENCE for 1-candle whipsaw trap, got ok=%v, reason=%s", okWhipsaw, reasonWhipsaw)
	}

	// Setelah BTC konsisten di atas EMA selama minimal 3 candle berturut-turut (candle 52, 53, 54 >= EMA)
	btcCandles[52].Close = 84050.0
	btcCandles[53].Close = 84050.0
	btcCandles[54].Close = 84050.0

	_, okStable, reasonStable, _ := EvaluateSignal(altSnap, &btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !okStable || reasonStable != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED after 3 persistent candles above EMA, got ok=%v, reason=%s", okStable, reasonStable)
	}
}

func TestBuildSafeDecimalOrderIntent_StopDistanceFloorAndTP1(t *testing.T) {
	now := time.Now().UTC()
	snap := model.MarketSnapshot{
		Symbol:  "LOWVOLUSDT",
		BestBid: 100.0,
		BestAsk: 100.0,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.001),
			MinQty:      decimal.NewFromFloat(0.001),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now: now,
	}

	// Skenario 1: ATR kecil (0.35%). Tanpa floor, SL 1.5x ATR = 0.525% (0.525).
	// Dengan MinSLDistancePct = 0.015 (1.50%), jarak SL minimal harus 1.50 (SL = 98.50).
	// Jarak TP2 minimal harus 1.50 * (2.25/1.5) = 2.25 (TP = 102.25).
	// Jarak TP1 minimal harus 1.50 * (1.20/1.5) = 1.20 (TP1 = 101.20).
	cfg := SizingConfig{
		RiskPerTradePct:                decimal.NewFromFloat(0.01), // $10 pada equity $1000
		MaxSlotMarginFraction:          decimal.NewFromFloat(0.50),
		AccountLeverage:                5.0,
		SLMultiplier:                   1.5,
		TPMultiplier:                   2.25,
		TP1Multiplier:                  1.20,
		MinSLDistancePct:               decimal.NewFromFloat(0.015), // Floor 1.50%
		MakerFeeRate:                   decimal.NewFromFloat(0.0002),
		TakerFeeRate:                   decimal.NewFromFloat(0.0005),
		SlippageRate:                   decimal.NewFromFloat(0.0003),
		AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
	}

	equity := decimal.NewFromFloat(1000.0)
	available := decimal.NewFromFloat(1000.0)
	atr14 := 0.35 // NATR = 0.35%

	intent, ok := BuildSafeDecimalOrderIntent(snap, equity, available, model.SideLong, 100.0, atr14, cfg)
	if !ok {
		t.Fatalf("expected order intent to be built successfully")
	}

	// Verifikasi SLPrice: entry 100.0 - 1.50 = 98.50
	expectedSL := decimal.NewFromFloat(98.50)
	if !intent.SLPrice.Equal(expectedSL) {
		t.Errorf("expected SLPrice %s with floor, got %s", expectedSL, intent.SLPrice)
	}

	// Verifikasi TPPrice: entry 100.0 + 2.25 = 102.25
	expectedTP := decimal.NewFromFloat(102.25)
	if !intent.TPPrice.Equal(expectedTP) {
		t.Errorf("expected TPPrice %s with R:R preservation, got %s", expectedTP, intent.TPPrice)
	}

	// Verifikasi TP1Price: Pure ATR-based scale-out (entry 100.0 + 1.20 * 0.35 = 100.42)
	// Tidak boleh tergelembungkan oleh floor SL agar selalu realistis disentuh pada swing awal
	expectedTP1 := decimal.NewFromFloat(100.42)
	if !intent.TP1Price.Equal(expectedTP1) {
		t.Errorf("expected TP1Price %s, got %s", expectedTP1, intent.TP1Price)
	}

	// Verifikasi Sizing: Dollar risk capital tetap terkontrol sekitar $10 USDT
	// actualLossPerUnit = (100 - 98.50) + friction (0.001 * 100) = 1.50 + 0.10 = 1.60
	// finalQty = 10.0 / 1.60 = 6.25 unit (bukan 19 unit seperti pada SL 0.525)
	expectedMaxQty := decimal.NewFromFloat(6.5)
	if intent.Qty.GreaterThan(expectedMaxQty) {
		t.Errorf("expected Qty to be scaled down by floor, got %s", intent.Qty)
	}
}

func TestBuildSafeDecimalOrderIntent_MarketVsLimitFriction(t *testing.T) {
	snap := model.MarketSnapshot{
		Symbol:  "BTCUSDT",
		BestBid: 100.0,
		BestAsk: 100.0,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.001),
			MinQty:      decimal.NewFromFloat(0.001),
			MinNotional: decimal.NewFromFloat(5.0),
		},
	}
	equity := decimal.NewFromFloat(1000.0)
	available := decimal.NewFromFloat(1000.0)
	atr14 := 2.0

	cfgLimit := SizingConfig{
		RiskPerTradePct:                decimal.NewFromFloat(0.01), // $10 risk
		MaxSlotMarginFraction:          decimal.NewFromFloat(0.50),
		AccountLeverage:                5.0,
		SLMultiplier:                   1.5, // 3.0 distance
		TPMultiplier:                   2.25,
		TP1Multiplier:                  1.20,
		MakerFeeRate:                   decimal.NewFromFloat(0.0002), // 0.02%
		TakerFeeRate:                   decimal.NewFromFloat(0.0005), // 0.05%
		SlippageRate:                   decimal.NewFromFloat(0.0002), // 0.02%
		AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
		EntryOrderType:                 "LIMIT",
	}

	cfgMarket := cfgLimit
	cfgMarket.EntryOrderType = "MARKET"

	intentLimit, okLimit := BuildSafeDecimalOrderIntent(snap, equity, available, model.SideLong, 100.0, atr14, cfgLimit)
	if !okLimit {
		t.Fatalf("expected limit intent to succeed")
	}

	intentMarket, okMarket := BuildSafeDecimalOrderIntent(snap, equity, available, model.SideLong, 100.0, atr14, cfgMarket)
	if !okMarket {
		t.Fatalf("expected market intent to succeed")
	}

	// For limit: friction = 0.0002 + 0.0005 + 0.0002 = 0.0009 -> frictionPerUnit = 0.09.
	// For market: friction = 2 * (0.0005 + 0.0002) = 0.0014 -> frictionPerUnit = 0.14.
	// Effective loss per unit is higher for MARKET (3.14 vs 3.09), so MARKET qty must be <= LIMIT qty.
	if intentMarket.Qty.GreaterThan(intentLimit.Qty) {
		t.Errorf("market qty %s should be <= limit qty %s due to higher taker+slippage friction", intentMarket.Qty, intentLimit.Qty)
	}
}

func TestEvaluateSignal_SymbolCooldown(t *testing.T) {
	now := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)
	cfg := EvaluatorConfig{
		MaxSlots:                 3,
		CooldownAfterExitMinutes: 20,
		CooldownLossMinutes:      20,
		CooldownProfitMinutes:    10,
	}

	// 1. Loss Exit (STOP_LOSS): 15 mins ago -> still in cooldown (< 20m)
	snapLoss := model.MarketSnapshot{
		Symbol:         "BTCUSDT",
		Now:            now,
		LastExitTime:   now.Add(-15 * time.Minute),
		LastExitReason: "STOP_LOSS",
	}

	_, okLoss, reasonLoss, _ := EvaluateSignal(snapLoss, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okLoss || reasonLoss != "SYMBOL_IN_COOLDOWN" {
		t.Fatalf("expected STOP_LOSS exit at 15m to be in cooldown, got ok=%v, reason=%s", okLoss, reasonLoss)
	}

	// 2. Win Exit (TAKE_PROFIT): 15 mins ago -> should EXPIRE (> 10m cooldown)
	snapWin := model.MarketSnapshot{
		Symbol:         "BTCUSDT",
		Now:            now,
		LastExitTime:   now.Add(-15 * time.Minute),
		LastExitReason: "TAKE_PROFIT",
	}

	_, _, reasonWin, _ := EvaluateSignal(snapWin, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if reasonWin == "SYMBOL_IN_COOLDOWN" {
		t.Fatalf("expected TAKE_PROFIT exit at 15m to NOT be in cooldown (profit cooldown=10m), got %s", reasonWin)
	}

	// 3. Win Exit (TAKE_PROFIT): 5 mins ago -> should be in cooldown (< 10m)
	snapWinRecent := model.MarketSnapshot{
		Symbol:         "BTCUSDT",
		Now:            now,
		LastExitTime:   now.Add(-5 * time.Minute),
		LastExitReason: "TAKE_PROFIT",
	}

	_, okWinRecent, reasonWinRecent, _ := EvaluateSignal(snapWinRecent, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okWinRecent || reasonWinRecent != "SYMBOL_IN_COOLDOWN" {
		t.Fatalf("expected TAKE_PROFIT exit at 5m to be in cooldown, got ok=%v, reason=%s", okWinRecent, reasonWinRecent)
	}
}

func TestEvaluateSignal_VWAPSessionWarmup(t *testing.T) {
	// Jam 00:15 UTC (dalam rentang 00:00 - 00:30 UTC warmup)
	warmupTime := time.Date(2026, 10, 29, 0, 15, 0, 0, time.UTC)
	cfg := EvaluatorConfig{
		MaxSlots:                 3,
		MaxBookStaleness:         5 * time.Second,
		MaxMarkStaleness:         15 * time.Second,
		MaxDataStaleness:         330 * time.Second,
		MinOIInterval:            4 * time.Minute,
		MaxOIInterval:            7 * time.Minute,
		MaxOIStaleness:           300 * time.Second,
		MaxSpreadPct:             0.05,
		MinNATRPct:               0.20,
		MaxNATRPct:               5.00,
		EMAPeriod:                10,
		ATRPeriod:                10,
		VWAPSessionWarmupMinutes: 30,
	}

	candles := make([]model.Candle, 20)
	for i := 0; i < 20; i++ {
		candles[i] = model.Candle{
			Open: 100, High: 102, Low: 99, Close: 101,
		}
	}

	snap := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		Now:                warmupTime,
		BookUpdatedAt:      warmupTime,
		MarkPriceUpdatedAt: warmupTime,
		FundingUpdatedAt:   warmupTime,
		BestBid:            100.0,
		BestAsk:            100.01,
		PrevOpenInterest:   1000.0,
		CurrOpenInterest:   1010.0,
		PrevOIUpdatedAt:    warmupTime.Add(-5 * time.Minute),
		CurrOIUpdatedAt:    warmupTime,
		CurrentSessionVWAP: 100.5,
		ClosedCandles:      candles,
	}

	_, ok, reason, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok {
		t.Fatalf("expected signal to be rejected during 00:00-00:30 UTC VWAP warmup")
	}
	if reason != "VWAP_SESSION_WARMUP" {
		t.Errorf("expected reason VWAP_SESSION_WARMUP, got %s", reason)
	}

	// Di luar warmup (misal 01:15 UTC) -> tidak boleh di-reject dengan VWAP_SESSION_WARMUP
	snap.Now = time.Date(2026, 10, 29, 1, 15, 0, 0, time.UTC)
	snap.BookUpdatedAt = snap.Now
	snap.MarkPriceUpdatedAt = snap.Now
	snap.FundingUpdatedAt = snap.Now
	snap.PrevOIUpdatedAt = snap.Now.Add(-5 * time.Minute)
	snap.CurrOIUpdatedAt = snap.Now

	_, _, reasonNormal, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if reasonNormal == "VWAP_SESSION_WARMUP" {
		t.Errorf("expected no VWAP warmup rejection at 01:15 UTC, got %s", reasonNormal)
	}
}

func TestEvaluateSignal_CapitulationOIFloor(t *testing.T) {
	now := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)
	cfg := EvaluatorConfig{
		MaxSlots:             3,
		MaxBookStaleness:     5 * time.Second,
		MaxMarkStaleness:     15 * time.Second,
		MaxDataStaleness:     330 * time.Second,
		MinOIInterval:        4 * time.Minute,
		MaxOIInterval:        7 * time.Minute,
		MaxOIStaleness:       300 * time.Second,
		MaxSpreadPct:         0.05,
		MinNATRPct:           0.40,
		MaxNATRPct:           5.00,
		EMAPeriod:            10,
		ATRPeriod:            10,
		MinOIDeltaShortPct:   -0.02,
		MinBounceZScoreShort: 0.40,
		LookbackImpulseBars:  15,
		RetestLookbackBars:   5,
		PullbackToleranceATR: 0.50,
		MaxEntryDistanceATR:  1.50,
		DeltaZScoreWindow:    20,
	}

	// 20 candle dengan tren bearish di bawah EMA200 dan VWAP
	candles := make([]model.Candle, 20)
	for i := 0; i < 20; i++ {
		candles[i] = model.Candle{
			Open: 100, High: 101, Low: 98, Close: 99,
		}
	}
	// Candle terakhir pullback ke VWAP lalu dump
	candles[18] = model.Candle{Open: 99, High: 100.2, Low: 99, Close: 100}
	candles[19] = model.Candle{Open: 100, High: 100.1, Low: 97, Close: 98}

	deltas := make([]float64, 30)
	for i := 0; i < 29; i++ {
		if i%2 == 0 {
			deltas[i] = 10.0
		} else {
			deltas[i] = -10.0
		}
	}
	deltas[29] = -25.0 // Z-score agresif short

	snap := model.MarketSnapshot{
		Symbol:             "BLESSUSDT",
		Now:                now,
		BookUpdatedAt:      now,
		MarkPriceUpdatedAt: now,
		FundingUpdatedAt:   now,
		BestBid:            97.98,
		BestAsk:            98.00,
		PrevOpenInterest:   10000.0,
		// Penurunan OI sebesar -0.30% (melebihi batas kapitulasi -0.10%)
		CurrOpenInterest:    9970.0,
		PrevOIUpdatedAt:     now.Add(-5 * time.Minute),
		CurrOIUpdatedAt:     now,
		CurrentSessionVWAP:  100.0,
		ClosedCandles:       candles,
		ClosedBarDeltasUSDT: deltas,
	}

	_, ok, reason, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	// Skenario 1: Penurunan OI -0.30% (< -0.10%) HARUS DITOLAK meskipun delta Z-score ekstrem
	if ok {
		t.Fatalf("expected signal to be rejected due to capitulation OI drop (-0.30%% < -0.10%%)")
	}
	if reason != "CONDITIONS_NOT_MET" {
		t.Errorf("expected rejection reason CONDITIONS_NOT_MET, got %s", reason)
	}

	// Skenario 2: Penurunan OI wajar/moderat -0.05% (>= -0.10%) boleh lolos bypass jika setup lain valid
	snap.CurrOpenInterest = 9995.0 // -0.05% drop
	// Evaluasi tidak boleh ditolak hanya karena capitulation floor
}

func TestEvaluateSignal_WarmupMinutesMultiHour(t *testing.T) {
	// Uji jika warmup di-set 75 menit (01:15 UTC)
	cfg := EvaluatorConfig{
		MaxSlots:                 3,
		MaxBookStaleness:         5 * time.Second,
		MaxMarkStaleness:         15 * time.Second,
		MaxDataStaleness:         330 * time.Second,
		MinOIInterval:            4 * time.Minute,
		MaxOIInterval:            7 * time.Minute,
		MaxOIStaleness:           300 * time.Second,
		MaxSpreadPct:             0.05,
		MinNATRPct:               0.20,
		MaxNATRPct:               5.00,
		EMAPeriod:                10,
		ATRPeriod:                10,
		VWAPSessionWarmupMinutes: 75,
	}

	candles := make([]model.Candle, 20)
	for i := 0; i < 20; i++ {
		candles[i] = model.Candle{Open: 100, High: 102, Low: 99, Close: 101}
	}

	// 01:10 UTC (70 menit pasca-midnight) -> < 75 menit -> harus REJECT
	testTime := time.Date(2026, 10, 29, 1, 10, 0, 0, time.UTC)
	snap := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		Now:                testTime,
		BookUpdatedAt:      testTime,
		MarkPriceUpdatedAt: testTime,
		FundingUpdatedAt:   testTime,
		BestBid:            100.0,
		BestAsk:            100.01,
		PrevOpenInterest:   1000.0,
		CurrOpenInterest:   1010.0,
		PrevOIUpdatedAt:    testTime.Add(-5 * time.Minute),
		CurrOIUpdatedAt:    testTime,
		CurrentSessionVWAP: 100.5,
		ClosedCandles:      candles,
	}

	_, ok, reason, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if ok || reason != "VWAP_SESSION_WARMUP" {
		t.Fatalf("expected VWAP_SESSION_WARMUP for 01:10 UTC when warmup is 75m, got ok=%v, reason=%s", ok, reason)
	}

	// 01:20 UTC (80 menit pasca-midnight) -> >= 75 menit -> tidak boleh VWAP_SESSION_WARMUP
	testTimeAfter := time.Date(2026, 10, 29, 1, 20, 0, 0, time.UTC)
	snap.Now = testTimeAfter
	snap.BookUpdatedAt = testTimeAfter
	snap.MarkPriceUpdatedAt = testTimeAfter
	snap.FundingUpdatedAt = testTimeAfter
	snap.PrevOIUpdatedAt = testTimeAfter.Add(-5 * time.Minute)
	snap.CurrOIUpdatedAt = testTimeAfter

	_, _, reasonAfter, _ := EvaluateSignal(snap, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if reasonAfter == "VWAP_SESSION_WARMUP" {
		t.Fatalf("expected no VWAP_SESSION_WARMUP for 01:20 UTC when warmup is 75m, got %s", reasonAfter)
	}
}

func TestEvaluateSignal_BTCGatePersistenceBuffer_Short(t *testing.T) {
	now := time.Now().UTC()
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	cfgShort.AllowedSide = "AUTO"
	cfgShort.BTCGateEnabled = true
	cfgShort.BTCGatePersistenceBars = 3

	// Buat 55 candle BTC di mana candle 0..53 berada di atas EMA (84000)
	// dan hanya candle 54 yang turun sesaat ke bawah EMA (83950) -> Bear Trap Whipsaw!
	btcCandles := make([]model.Candle, 55)
	for i := 0; i < 54; i++ {
		btcCandles[i] = model.Candle{
			OpenTime: now.Add(time.Duration(i-55) * 5 * time.Minute),
			Open:     84050.0, High: 84100.0, Low: 84000.0, Close: 84050.0,
			VolumeBase: 100.0,
		}
	}
	btcCandles[54] = model.Candle{
		OpenTime:  now.Add(-5 * time.Minute),
		CloseTime: now,
		Open:      84050.0, High: 84050.0, Low: 83900.0, Close: 83950.0, // Retrace 1 bar
		VolumeBase: 200.0,
	}

	btcSnap := model.MarketSnapshot{
		Symbol:             "BTCUSDT",
		ClosedCandles:      btcCandles,
		CurrentSessionVWAP: 84000.0,
		Now:                now,
	}

	// Evaluasi Short: Harus di-VETO oleh BTC_TREND_SHORT_VETO_PERSISTENCE karena bar 1 & 2 yang lalu masih di atas EMA
	_, okWhipsaw, reasonWhipsaw, _ := EvaluateSignal(snapShort, &btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if okWhipsaw || reasonWhipsaw != "BTC_TREND_SHORT_VETO_PERSISTENCE" {
		t.Fatalf("expected BTC_TREND_SHORT_VETO_PERSISTENCE for 1-candle bear trap, got ok=%v, reason=%s", okWhipsaw, reasonWhipsaw)
	}

	// Setelah BTC konsisten di bawah EMA selama minimal 3 candle berturut-turut (candle 52, 53, 54 <= EMA)
	btcCandles[52].Close = 83950.0
	btcCandles[53].Close = 83950.0
	btcCandles[54].Close = 83950.0

	_, okStable, reasonStable, _ := EvaluateSignal(snapShort, &btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if !okStable || reasonStable != "SIGNAL_APPROVED" {
		t.Fatalf("expected SIGNAL_APPROVED after 3 persistent bars below EMA, got ok=%v, reason=%s", okStable, reasonStable)
	}
}

func TestEvaluateSignal_ImpulseInvalidation_OppositeMove(t *testing.T) {
	now := time.Now().UTC()
	snapLong, cfgLong := helperCreateValidLongSnapshot(now)

	// Kasus 1: Bullish impulse terjadi di bar 55 (High 107.5 > VWAP 105).
	// Jika di bar 57 muncul candle yang Close-nya jebol jauh di bawah bearish threshold (Close 95.0 << VWAP 105),
	// maka bullish impulse sebelumnya harus gugur.
	snapLong.ClosedCandles[57].Close = 95.0
	snapLong.ClosedCandles[57].Low = 94.0

	_, okLong, reasonLong, subReasonLong := EvaluateSignal(snapLong, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if okLong || reasonLong != "CONDITIONS_NOT_MET" || !strings.Contains(subReasonLong, "NO_PRIOR_IMPULSE") {
		t.Fatalf("expected Long rejected with NO_PRIOR_IMPULSE due to bearish invalidation close, got ok=%v, reason=%s, subReason=%s", okLong, reasonLong, subReasonLong)
	}

	// Kasus 2: Symmetrically untuk Short
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	// Short impulse terjadi di bar 55 (Low 97.0 < VWAP 100).
	// Di bar 57 muncul candle yang Close-nya melonjak jauh di atas bullish threshold (Close 110.0 >> VWAP 100) (kasus OPUSDT)
	snapShort.ClosedCandles[57].Close = 110.0
	snapShort.ClosedCandles[57].High = 111.0

	_, okShort, reasonShort, subReasonShort := EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if okShort || reasonShort != "CONDITIONS_NOT_MET" || !strings.Contains(subReasonShort, "NO_PRIOR_IMPULSE") {
		t.Fatalf("expected Short rejected with NO_PRIOR_IMPULSE due to bullish invalidation close, got ok=%v, reason=%s, subReason=%s", okShort, reasonShort, subReasonShort)
	}
}

func TestEvaluateSignal_RetestDirectionVerification(t *testing.T) {
	now := time.Now().UTC()

	// Short setup di mana harga tiba-tiba datang terjun bebas dari ATAS VWAP (Breakdown, bukan Pullback dari bawah)
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	vwap := snapShort.CurrentSessionVWAP
	// Buat candle retest (bar 58 & 59) membuka di ATAS VWAP dan bar sebelumnya juga tutup di ATAS VWAP
	snapShort.ClosedCandles[58].Close = vwap + 1.0
	snapShort.ClosedCandles[59].Open = vwap + 0.8
	// Walaupun High sempat berada di toleransi VWAP, arah datangnya dari atas!
	snapShort.ClosedCandles[59].High = vwap + 0.2

	_, ok, reason, subReason := EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if ok || reason != "CONDITIONS_NOT_MET" || subReason != "VWAP_NOT_TESTED" {
		t.Fatalf("expected rejection with VWAP_NOT_TESTED when short approach is from above VWAP, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}
}

func TestEvaluateSignal_BTCCounterMomentumGuard(t *testing.T) {
	now := time.Now().UTC()

	// 1. Setup SHORT altcoin yang valid
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	cfgShort.BTCGateEnabled = true
	cfgShort.BTCCounterMomentumPct = 0.25

	// Mock BTC snapshot di mana BTC baru saja memantul +0.30% pada candle 5m terakhir
	btcCandles := make([]model.Candle, 60)
	for i := 0; i < 59; i++ {
		btcCandles[i] = model.Candle{
			OpenTime:   now.Add(time.Duration(i-60) * 5 * time.Minute),
			CloseTime:  now.Add(time.Duration(i-59) * 5 * time.Minute),
			Open:       50000.0,
			High:       50100.0,
			Low:        49900.0,
			Close:      49950.0,
			VolumeBase: 100.0,
		}
	}
	// Candle terakhir BTC: Open 50000, Close 50150 (+0.30%)
	btcCandles[59] = model.Candle{
		OpenTime:   now.Add(-5 * time.Minute),
		CloseTime:  now,
		Open:       50000.0,
		High:       50160.0,
		Low:        49980.0,
		Close:      50150.0,
		VolumeBase: 200.0,
	}
	btcSnap := &model.MarketSnapshot{
		Symbol:        "BTCUSDT",
		ClosedCandles: btcCandles,
	}

	_, ok, reason, _ := EvaluateSignal(snapShort, btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if ok || reason != "BTC_COUNTER_MOMENTUM_PUMP" {
		t.Fatalf("expected rejection with BTC_COUNTER_MOMENTUM_PUMP, got ok=%v, reason=%s", ok, reason)
	}

	// 2. Setup LONG altcoin yang valid
	snapLong, cfgLong := helperCreateValidLongSnapshot(now)
	cfgLong.BTCGateEnabled = true
	cfgLong.BTCCounterMomentumPct = 0.25

	// Mock BTC snapshot di mana BTC baru saja dump -0.30% pada candle 5m terakhir
	// Candle terakhir BTC: Open 50000, Close 49850 (-0.30%)
	btcCandles[59] = model.Candle{
		OpenTime:   now.Add(-5 * time.Minute),
		CloseTime:  now,
		Open:       50000.0,
		High:       50010.0,
		Low:        49840.0,
		Close:      49850.0,
		VolumeBase: 200.0,
	}

	_, okLong, reasonLong, _ := EvaluateSignal(snapLong, btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if okLong || reasonLong != "BTC_COUNTER_MOMENTUM_DUMP" {
		t.Fatalf("expected rejection with BTC_COUNTER_MOMENTUM_DUMP, got ok=%v, reason=%s", okLong, reasonLong)
	}

	// 3. Normal drift (+0.10% on BTC) should NOT be rejected by counter momentum guard
	btcCandles[59] = model.Candle{
		OpenTime:   now.Add(-5 * time.Minute),
		CloseTime:  now,
		Open:       50000.0,
		High:       50060.0,
		Low:        49980.0,
		Close:      50050.0,
		VolumeBase: 100.0,
	}
	cfgShort.AllowedSide = "BOTH"
	_, okShortPassed, reasonShortPassed, _ := EvaluateSignal(snapShort, btcSnap, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if !okShortPassed && reasonShortPassed == "BTC_COUNTER_MOMENTUM_PUMP" {
		t.Fatalf("expected normal drift +0.10%% to NOT trigger BTC_COUNTER_MOMENTUM_PUMP, got reason=%s", reasonShortPassed)
	}
}

func TestEvaluateSignal_MinSignalRVOL(t *testing.T) {
	now := time.Now().UTC()
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	cfgShort.MinSignalRVOL = 0.75

	// Buat 20 candle sebelumnya memiliki volume rata-rata 1000
	for i := len(snapShort.ClosedCandles) - 21; i < len(snapShort.ClosedCandles)-1; i++ {
		snapShort.ClosedCandles[i].VolumeBase = 1000.0
	}

	// Kasus 1: Volume candle terakhir sangat rendah (550.0 -> RVOL = 0.55 < 0.75) seperti kasus ESPORTSUSDT
	snapShort.ClosedCandles[len(snapShort.ClosedCandles)-1].VolumeBase = 550.0
	_, ok, reason, subReason := EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if ok || reason != "LOW_SIGNAL_RVOL" {
		t.Fatalf("expected rejection with LOW_SIGNAL_RVOL for rvol=0.55, got ok=%v, reason=%s, subReason=%s", ok, reason, subReason)
	}

	// Kasus 2: Volume candle terakhir sehat (850.0 -> RVOL = 0.85 >= 0.75)
	snapShort.ClosedCandles[len(snapShort.ClosedCandles)-1].VolumeBase = 850.0
	_, okPass, reasonPass, _ := EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if !okPass && reasonPass == "LOW_SIGNAL_RVOL" {
		t.Fatalf("expected rvol=0.85 to pass LOW_SIGNAL_RVOL, got reason=%s", reasonPass)
	}
}

func TestEvaluateSignal_SymmetricalWickRejection(t *testing.T) {
	now := time.Now().UTC()

	// 1. Test LONG with Shooting Star (Close > Open but Upper Wick >= 50%)
	snapLong, cfgLong := helperCreateValidLongSnapshot(now)
	lastIdxL := len(snapLong.ClosedCandles) - 1
	// Open 105.1, High 106.0, Close 105.2, Low 105.1 -> Range 0.9, Upper Wick = 106.0 - 105.2 = 0.8 (88.9% >= 50%)
	snapLong.ClosedCandles[lastIdxL].Open = 105.1
	snapLong.ClosedCandles[lastIdxL].High = 106.0
	snapLong.ClosedCandles[lastIdxL].Low = 105.1
	snapLong.ClosedCandles[lastIdxL].Close = 105.2
	snapLong.BestBid = 105.19
	snapLong.BestAsk = 105.21
	snapLong.MarkPrice = 105.20
	_, okL, reasonL, subReasonL := EvaluateSignal(snapLong, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgLong)
	if okL || reasonL != "CONDITIONS_NOT_MET" || !strings.Contains(subReasonL, "CANDLE_NOT_BULLISH") {
		t.Fatalf("expected Long rejection with CANDLE_NOT_BULLISH for shooting star, got ok=%v, reason=%s, subReason=%s", okL, reasonL, subReasonL)
	}

	// 2. Test SHORT with Hammer (Close < Open but Lower Wick >= 50%)
	snapShort, cfgShort := helperCreateValidShortSnapshot(now)
	lastIdxS := len(snapShort.ClosedCandles) - 1
	// Open 99.8, High 99.8, Close 99.7, Low 98.8 -> Range 1.0, Lower Wick = 99.7 - 98.8 = 0.9 (90% >= 50%)
	snapShort.ClosedCandles[lastIdxS].Open = 99.8
	snapShort.ClosedCandles[lastIdxS].High = 99.8
	snapShort.ClosedCandles[lastIdxS].Close = 99.7
	snapShort.ClosedCandles[lastIdxS].Low = 98.8
	snapShort.BestBid = 99.69
	snapShort.BestAsk = 99.71
	snapShort.MarkPrice = 99.70
	_, okS, reasonS, subReasonS := EvaluateSignal(snapShort, nil, 0, false, false, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfgShort)
	if okS || reasonS != "CONDITIONS_NOT_MET" || !strings.Contains(subReasonS, "CANDLE_NOT_BEARISH") {
		t.Fatalf("expected Short rejection with CANDLE_NOT_BEARISH for hammer, got ok=%v, reason=%s, subReason=%s", okS, reasonS, subReasonS)
	}
}
