package strategy

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
	"trml/internal/model"
)

func TestMirrorSymmetry_EvaluatorAndBroker(t *testing.T) {
	// Konfigurasi simetris penuh untuk memvalidasi fisika engine tanpa bias arah
	cfg := EvaluatorConfig{
		PullbackToleranceATR: 0.50,
		MinBounceZScore:      0.75,
		MinBounceZScoreLong:  0.75,
		MinBounceZScoreShort: 0.75,
		MinEMA200DistanceATR: 0.0, // Nonaktifkan agar tidak sensitif bias tren global
		MaxEMA200DistanceATR: 10.0,
		LookbackImpulseBars:  10,
		RetestLookbackBars:   5,
		MinImpulseATR:        0.50,
		MaxEntryDistanceATR:  1.50,
		EntryOrderType:       "LIMIT",
		MaxSlots:             3,
		MaxSpreadPct:         0.10,
		EMAPeriod:            20,
		ATRPeriod:            14,
		DeltaZScoreWindow:    20,
		MinNATRPct:           0.10,
		MaxNATRPct:           5.0,
		BTCGateEnabled:       false,
		AllowedSide:          "BOTH",
		MaxFrenzyRVOL:        10.0,
		MinSignalRVOL:        0.0,
		DisableOIFilter:      true,
		Sizing: SizingConfig{
			RiskPerTradePct:       decimal.NewFromFloat(0.015),
			MaxSlotMarginFraction: decimal.NewFromFloat(0.20),
			AccountLeverage:       8.0,
			SLMultiplier:          1.50,
			TPMultiplier:          3.00,
			TP1Multiplier:         1.00,
			MinSLDistancePct:      decimal.NewFromFloat(0.005),
			MakerFeeRate:          decimal.NewFromFloat(0.0002),
			TakerFeeRate:          decimal.NewFromFloat(0.0005),
			SlippageRate:          decimal.NewFromFloat(0.0002),
		},
	}

	filter := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}

	now := time.Now().UTC().Truncate(5 * time.Minute)

	// Buat urutan candle bearish (Short Setup)
	// Baseline harga sekitar 100.0, ATR ~ 1.0
	var origCandles []model.Candle
	var origDeltas []float64

	// 1. Warmup candles mendatar di 100.0
	for i := 0; i < 30; i++ {
		tm := now.Add(time.Duration(i-35) * 5 * time.Minute)
		origCandles = append(origCandles, model.Candle{
			OpenTime:        tm,
			CloseTime:       tm.Add(5 * time.Minute),
			Open:            100.0,
			High:            100.5,
			Low:             99.5,
			Close:           100.0,
			VolumeBase:      1000.0,
			QuoteVolumeUSDT: 100000.0,
		})
		dVal := 1000.0
		if i%2 == 0 {
			dVal = -1000.0
		}
		origDeltas = append(origDeltas, dVal)
	}

	// 2. Bearish Impulse (harga jatuh ke 98.0, di bawah VWAP 100.0 sebesar 2 ATR)
	impulseTime := now.Add(-5 * 5 * time.Minute)
	origCandles = append(origCandles, model.Candle{
		OpenTime:        impulseTime,
		CloseTime:       impulseTime.Add(5 * time.Minute),
		Open:            99.8,
		High:            100.0,
		Low:             98.0,
		Close:           98.5,
		VolumeBase:      2000.0,
		QuoteVolumeUSDT: 200000.0,
	})
	origDeltas = append(origDeltas, -50000.0)

	// 3. Pullback ke VWAP dari bawah (harga naik kembali menguji 99.8)
	for i := 4; i >= 2; i-- {
		tm := now.Add(time.Duration(-i) * 5 * time.Minute)
		origCandles = append(origCandles, model.Candle{
			OpenTime:        tm,
			CloseTime:       tm.Add(5 * time.Minute),
			Open:            98.5 + float64(4-i)*0.4,
			High:            99.9,
			Low:             98.4,
			Close:           99.7,
			VolumeBase:      1200.0,
			QuoteVolumeUSDT: 120000.0,
		})
		origDeltas = append(origDeltas, 10000.0)
	}

	// 4. Bar Sinyal Terkini: Bearish Bounce (Close < Open, Close <= VWAP, Delta Z-score sangat negatif)
	signalTime := now.Add(-1 * 5 * time.Minute)
	origCandles = append(origCandles, model.Candle{
		OpenTime:        signalTime,
		CloseTime:       signalTime.Add(5 * time.Minute),
		Open:            99.8,
		High:            99.9,
		Low:             99.1,
		Close:           99.2,
		VolumeBase:      3000.0,
		QuoteVolumeUSDT: 300000.0,
	})
	// Delta negatif tajam agar Z-score <= -0.75
	origDeltas = append(origDeltas, -150000.0)

	snapShort := model.MarketSnapshot{
		Symbol:              "TESTUSDT",
		Filter:              filter,
		Now:                 now,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		MarkPrice:           99.2,
		BestBid:             99.19,
		BestAsk:             99.21,
		CurrentSessionVWAP:  99.8,
		ClosedCandles:       origCandles,
		ClosedBarDeltasUSDT: origDeltas,
	}

	intentShort, approvedShort, reasonShort, _ := EvaluateSignal(
		snapShort,
		nil,
		0,
		false,
		false,
		decimal.NewFromFloat(1000.0),
		decimal.NewFromFloat(1000.0),
		cfg,
	)

	if !approvedShort {
		t.Fatalf("Sanity 6b: expected SHORT signal approved, got rejected: %s", reasonShort)
	}
	if intentShort.Side != model.SideShort {
		t.Fatalf("Sanity 6b: expected SHORT intent, got %s", intentShort.Side)
	}

	// --- SEKARANG BALIKKAN HARGA DATA (TES CERMIN / MIRROR TEST) ---
	// P' = 200.0 - P
	// High' = 200.0 - Low
	// Low' = 200.0 - High
	// Delta' = -Delta
	const K = 200.0
	var mirrorCandles []model.Candle
	var mirrorDeltas []float64

	for _, c := range origCandles {
		mC := model.Candle{
			OpenTime:        c.OpenTime,
			CloseTime:       c.CloseTime,
			Open:            K - c.Open,
			High:            K - c.Low,
			Low:             K - c.High,
			Close:           K - c.Close,
			VolumeBase:      c.VolumeBase,
			QuoteVolumeUSDT: c.QuoteVolumeUSDT,
		}
		mirrorCandles = append(mirrorCandles, mC)
	}
	for _, d := range origDeltas {
		mirrorDeltas = append(mirrorDeltas, -d)
	}

	snapLong := model.MarketSnapshot{
		Symbol:              "TESTUSDT",
		Filter:              filter,
		Now:                 now,
		BookUpdatedAt:       now,
		MarkPriceUpdatedAt:  now,
		FundingUpdatedAt:    now,
		MarkPrice:           K - snapShort.MarkPrice,
		BestBid:             K - snapShort.BestAsk,
		BestAsk:             K - snapShort.BestBid,
		CurrentSessionVWAP:  K - snapShort.CurrentSessionVWAP,
		ClosedCandles:       mirrorCandles,
		ClosedBarDeltasUSDT: mirrorDeltas,
	}

	intentLong, approvedLong, reasonLong, _ := EvaluateSignal(
		snapLong,
		nil,
		0,
		false,
		false,
		decimal.NewFromFloat(1000.0),
		decimal.NewFromFloat(1000.0),
		cfg,
	)

	if !approvedLong {
		t.Fatalf("Sanity 6b: expected LONG signal on mirrored data, got rejected: %s", reasonLong)
	}
	if intentLong.Side != model.SideLong {
		t.Fatalf("Sanity 6b: expected LONG intent on mirrored data, got %s", intentLong.Side)
	}

	// Periksa simetri kuantitatif harga:
	// LimitPrice_Long + LimitPrice_Short harus = K (200.0)
	sumEntry := intentShort.LimitPrice.Add(intentLong.LimitPrice)
	if !sumEntry.Equal(decimal.NewFromFloat(K)) {
		t.Errorf("Sanity 6b Asymmetry: sum of entry prices (%s + %s = %s) != %.2f",
			intentShort.LimitPrice, intentLong.LimitPrice, sumEntry, K)
	}

	// Jarak SL dan TP harus identik dalam R dan tick
	shortSLDist := intentShort.SLPrice.Sub(intentShort.LimitPrice).Abs()
	longSLDist := intentLong.LimitPrice.Sub(intentLong.SLPrice).Abs()
	if !shortSLDist.Equal(longSLDist) {
		t.Errorf("Sanity 6b Asymmetry: SL distance mismatch Short=%s Long=%s", shortSLDist, longSLDist)
	}

	shortTPDist := intentShort.LimitPrice.Sub(intentShort.TPPrice).Abs()
	longTPDist := intentLong.TPPrice.Sub(intentLong.LimitPrice).Abs()
	if !shortTPDist.Equal(longTPDist) {
		t.Errorf("Sanity 6b Asymmetry: TP distance mismatch Short=%s Long=%s", shortTPDist, longTPDist)
	}

	// --- SIMBROKER EXECUTION MIRROR TEST ---
	brokerCfg := simbroker.SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
		MaxSlots:                3,
		MakerFeeRate:            decimal.Zero,
		TakerFeeRate:            decimal.Zero,
		SlippageRate:            decimal.Zero,
		SignalDelayBars:         1,
		LimitTimeoutBars:        1,
		PenetrationTicks:        1,
		IntrabarConflictSLFirst: true,
		PartialTPEnabled:        false,
	}

	bShort := simbroker.NewSimBroker(brokerCfg)
	bShort.ExchangeFilters["TESTUSDT"] = filter
	bLong := simbroker.NewSimBroker(brokerCfg)
	bLong.ExchangeFilters["TESTUSDT"] = filter

	_, _ = bShort.SubmitLimitEntry(intentShort)
	_, _ = bLong.SubmitLimitEntry(intentLong)

	// Simulasikan 2 bar berikutnya:
	// Bar 1: Fill order
	// Short fills at intentShort.LimitPrice
	cFillShort := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Symbol:    "TESTUSDT",
		Open:      intentShort.LimitPrice.InexactFloat64(),
		High:      intentShort.LimitPrice.InexactFloat64() + 0.5,
		Low:       intentShort.LimitPrice.InexactFloat64() - 0.5,
		Close:     intentShort.LimitPrice.InexactFloat64(),
	}
	cFillLong := data.HistoricalCandle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Symbol:    "TESTUSDT",
		Open:      intentLong.LimitPrice.InexactFloat64(),
		High:      intentLong.LimitPrice.InexactFloat64() + 0.5,
		Low:       intentLong.LimitPrice.InexactFloat64() - 0.5,
		Close:     intentLong.LimitPrice.InexactFloat64(),
	}

	bShort.ProcessCandle(cFillShort, 0)
	bLong.ProcessCandle(cFillLong, 0)

	// Bar 2: Market reaches TP
	cTPShort := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10 * time.Minute),
		Symbol:    "TESTUSDT",
		Open:      intentShort.LimitPrice.InexactFloat64(),
		High:      intentShort.LimitPrice.InexactFloat64() + 0.1,
		Low:       intentShort.TPPrice.InexactFloat64() - 0.5, // Tembus TP Short
		Close:     intentShort.TPPrice.InexactFloat64(),
	}
	cTPLong := data.HistoricalCandle{
		OpenTime:  now.Add(5 * time.Minute),
		CloseTime: now.Add(10 * time.Minute),
		Symbol:    "TESTUSDT",
		Open:      intentLong.LimitPrice.InexactFloat64(),
		High:      intentLong.TPPrice.InexactFloat64() + 0.5, // Tembus TP Long
		Low:       intentLong.LimitPrice.InexactFloat64() - 0.1,
		Close:     intentLong.TPPrice.InexactFloat64(),
	}

	bShort.ProcessCandle(cTPShort, 1)
	bLong.ProcessCandle(cTPLong, 1)

	if len(bShort.ClosedTrades) != 1 || len(bLong.ClosedTrades) != 1 {
		t.Fatalf("Sanity 6b: expected 1 closed trade on each side, got Short=%d Long=%d",
			len(bShort.ClosedTrades), len(bLong.ClosedTrades))
	}

	trShort := bShort.ClosedTrades[0]
	trLong := bLong.ClosedTrades[0]

	if trShort.ExitReason != trLong.ExitReason {
		t.Errorf("Sanity 6b: exit reason mismatch Short=%s Long=%s", trShort.ExitReason, trLong.ExitReason)
	}

	// Gross PnL harus identik karena jarak TP sama
	if math.Abs(trShort.GrossPnL.Sub(trLong.GrossPnL).InexactFloat64()) > 0.001 {
		t.Errorf("Sanity 6b: Gross PnL mismatch Short=%s Long=%s", trShort.GrossPnL, trLong.GrossPnL)
	}
}
