package strategy

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/config"
	"trml/internal/model"
)

func makeSynthetic4hSeries(t0 time.Time, count int, closeFn func(i int) float64) []model.Candle {
	out := make([]model.Candle, count)
	for i := 0; i < count; i++ {
		c := closeFn(i)
		o := t0.Add(time.Duration(i) * 4 * time.Hour)
		out[i] = model.Candle{
			OpenTime:          o,
			CloseTime:         o.Add(4*time.Hour - time.Millisecond),
			Open:              c * 1.002,
			High:              c * 1.015,
			Low:               c * 0.985,
			Close:             c,
			VolumeBase:        1000.0,
			QuoteVolumeUSDT:   1_500_000.0,
			TakerBuyQuoteUSDT: 700_000.0,
		}
	}
	return out
}

func baseCTAEvaluatorConfig(mode string) EvaluatorConfig {
	return EvaluatorConfig{
		CandleDuration:   15 * time.Minute,
		MaxSpreadPct:     0.20,
		MaxBookStaleness: 5 * time.Second,
		MaxMarkStaleness: 15 * time.Second,
		MaxDataStaleness: 20 * time.Minute,
		CTAPod: CTAPodConfig{
			ShortEnabled:        true,
			LongEnabled:         false,
			Mode:                mode,
			TopN:                25,
			MinVolume24hUSDT:    25_000_000,
			MaxSlots:            3,
			ShortRiskPct:        decimal.NewFromFloat(0.0060),
			LongRiskPct:         decimal.NewFromFloat(0.0060),
			DonchianLookback:    20,
			EMAFast:             12,
			EMASlow:             36,
			EMATrend:            200,
			ShortMinFundingRate: -0.0002,
			LongMaxFundingRate:  0.0002,
			SLATRMultiplier:     3.0,
			TPATRMultiplier:     0.0,
			MinSLDistancePct:    decimal.NewFromFloat(0.020),
			TrailingEnabled:     true,
			ChandelierTrailing:  true,
			TrailingDistanceATR: 3.0,
			MaxHoldingDuration:  240 * time.Hour,
			StaleLossDuration:   0,
			CooldownMinutes:     240,
			EntryOrderType:      "MARKET",
		},
		Sizing: SizingConfig{
			MaxSlotMarginFraction:          decimal.NewFromFloat(0.22),
			AvailableBalanceBufferFraction: decimal.NewFromFloat(0.90),
			AccountLeverage:                10.0,
			MakerFeeRate:                   decimal.NewFromFloat(0.0002),
			TakerFeeRate:                   decimal.NewFromFloat(0.0005),
			SlippageRate:                   decimal.NewFromFloat(0.0002),
		},
	}
}

func makeCTASnap(sym string, now time.Time, lastClose, fr float64) model.MarketSnapshot {
	return model.MarketSnapshot{
		Symbol: sym,
		Filter: model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.01),
			MinQty:      decimal.NewFromFloat(0.01),
			MinNotional: decimal.NewFromFloat(5.0),
		},
		Now:                now,
		BestBid:            lastClose * 0.9998,
		BestAsk:            lastClose * 1.0002,
		BookUpdatedAt:      now,
		MarkPrice:          lastClose,
		MarkPriceUpdatedAt: now,
		FundingRate:        fr,
		FundingUpdatedAt:   now,
	}
}

func TestIs4HourCloseTimestamp(t *testing.T) {
	if !Is4HourCloseTimestamp(time.Date(2025, 1, 1, 3, 59, 59, 999_000_000, time.UTC)) {
		t.Fatalf("expected 03:59:59.999 to normalize to 04:00:00 UTC 4h close")
	}
	if !Is4HourCloseTimestamp(time.Date(2025, 1, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected 08:00:00 UTC to be 4h close")
	}
	if Is4HourCloseTimestamp(time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)) {
		t.Fatalf("did not expect 02:00:00 UTC to be 4h close")
	}
	if Is4HourCloseTimestamp(time.Date(2025, 1, 1, 4, 15, 0, 0, time.UTC)) {
		t.Fatalf("did not expect 04:15:00 UTC to be 4h close")
	}
}

func TestResampleCompleted4HourlyStrict_AndMerge(t *testing.T) {
	t0 := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	var sub []model.Candle
	// Complete 16 x 15m bars for 00:00..04:00 UTC + incomplete 15 bars for 04:00..08:00 UTC
	for i := 0; i < 31; i++ {
		o := t0.Add(time.Duration(i) * 15 * time.Minute)
		sub = append(sub, model.Candle{
			OpenTime:        o,
			CloseTime:       o.Add(15*time.Minute - time.Millisecond),
			Open:            100.0 + float64(i),
			High:            101.0 + float64(i),
			Low:             99.0 + float64(i),
			Close:           100.5 + float64(i),
			QuoteVolumeUSDT: 100_000,
		})
	}
	bars := ResampleCompleted4HourlyStrict(sub, 15*time.Minute)
	if len(bars) != 1 {
		t.Fatalf("expected exactly 1 complete 4h bar, got %d", len(bars))
	}
	if !bars[0].OpenTime.Equal(t0) || math.Abs(bars[0].Close-115.5) > 1e-9 || math.Abs(bars[0].QuoteVolumeUSDT-1_600_000) > 1e-6 {
		t.Fatalf("unexpected 4h bar values: %+v", bars[0])
	}

	merged := MergeCompleted4HourlyCandle(bars, sub, 15*time.Minute, 10)
	if len(merged) != 1 {
		t.Fatalf("expected idempotent merge keeping 1 bar, got %d", len(merged))
	}
}

func TestEvaluateCTA4hCandidate_DON20_ShortAndGuards(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c4h := makeSynthetic4hSeries(t0, 60, func(i int) float64 {
		if i < 59 {
			return 100.0 - float64(i)*0.2
		}
		return 82.0 // sharp breakdown below prev20 low and EMA12 < EMA36
	})
	now := t0.Add(60 * 4 * time.Hour)
	cfg := baseCTAEvaluatorConfig("DON20")

	snap := makeCTASnap("SOLUSDT", now, 82.0, 0.0001)
	cand := EvaluateCTA4hCandidate(snap, c4h, false, false, cfg)
	if !cand.Approved || cand.Side != model.SideShort || cand.StrategyID != StrategyIDCTA4hShort || cand.Score <= 0 {
		t.Fatalf("expected DON20 SHORT approved, got %+v", cand)
	}

	// Crowded negative funding rejected
	badFR := makeCTASnap("SOLUSDT", now, 82.0, -0.0005)
	rejFR := EvaluateCTA4hCandidate(badFR, c4h, false, false, cfg)
	if rejFR.Approved || rejFR.Reason != "CTA_SHORT_FUNDING_TOO_NEGATIVE" {
		t.Fatalf("expected CTA_SHORT_FUNDING_TOO_NEGATIVE, got %+v", rejFR)
	}

	// Active One-Way lock rejected
	rejAct := EvaluateCTA4hCandidate(snap, c4h, true, false, cfg)
	if rejAct.Approved || rejAct.Reason != "SYMBOL_ALREADY_ACTIVE" {
		t.Fatalf("expected SYMBOL_ALREADY_ACTIVE, got %+v", rejAct)
	}

	// Cooldown rejected
	cdSnap := snap
	cdSnap.LastExitTime = now.Add(-2 * time.Hour)
	rejCD := EvaluateCTA4hCandidate(cdSnap, c4h, false, false, cfg)
	if rejCD.Approved || rejCD.Reason != "SYMBOL_IN_COOLDOWN" {
		t.Fatalf("expected SYMBOL_IN_COOLDOWN, got %+v", rejCD)
	}
}

func TestEvaluateCTA4hCandidate_EMACross_Short(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// First 80 bars at 200 (keeps EMA200 > price), flat 100 for 147 bars so EMA12 & EMA36 converge to 100, bounce 110 on bars 227..228 so prevEMA12 > prevEMA36, then drop to 70 on bar 229 for fresh cross < EMA200
	c4h := makeSynthetic4hSeries(t0, 230, func(i int) float64 {
		if i < 80 {
			return 200.0
		}
		if i < 227 {
			return 100.0
		}
		if i < 229 {
			return 110.0
		}
		return 70.0
	})
	cfg := baseCTAEvaluatorConfig("EMA_CROSS")
	now := t0.Add(230 * 4 * time.Hour)
	snap := makeCTASnap("ETHUSDT", now, 70.0, 0.0001)

	cand := EvaluateCTA4hCandidate(snap, c4h, false, false, cfg)
	if !cand.Approved || cand.Side != model.SideShort || cand.StrategyID != StrategyIDCTA4hShort {
		t.Fatalf("expected EMA_CROSS SHORT approved on fresh cross < EMA200, got %+v", cand)
	}

	// Subsequent bar without fresh cross must reject
	c4hNext := append(append([]model.Candle(nil), c4h...), model.Candle{
		OpenTime:  t0.Add(230 * 4 * time.Hour),
		CloseTime: t0.Add(231*4*time.Hour - time.Millisecond),
		Open:      80.0,
		High:      81.0,
		Low:       78.0,
		Close:     79.0,
	})
	rejNoCross := EvaluateCTA4hCandidate(makeCTASnap("ETHUSDT", now.Add(4*time.Hour), 79.0, 0.0001), c4hNext, false, false, cfg)
	if rejNoCross.Approved || rejNoCross.Reason != "CTA_NO_4H_EMA_CROSS" {
		t.Fatalf("expected CTA_NO_4H_EMA_CROSS on already-crossed bar, got %+v", rejNoCross)
	}
}

func TestBuildCTA4hOrderIntent_NoTPAndLiquidationGuard(t *testing.T) {
	now := time.Date(2025, 3, 1, 4, 0, 0, 0, time.UTC)
	cfg := baseCTAEvaluatorConfig("DON20")
	snap := makeCTASnap("BTCUSDT", now, 100.0, 0.0)

	// Safe ATR (2.0% -> SL 6.0% <= 8.0% max at 10x lev)
	candOK := CTA4hCandidate{
		Symbol:     "BTCUSDT",
		Side:       model.SideShort,
		StrategyID: StrategyIDCTA4hShort,
		ATR14_4h:   2.0,
		Approved:   true,
	}
	intent, ok, reason := BuildCTA4hOrderIntent(candOK, snap, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if !ok || reason != "SIGNAL_APPROVED" {
		t.Fatalf("expected BuildCTA4hOrderIntent ok, got ok=%v reason=%s", ok, reason)
	}
	if !intent.TPPrice.IsZero() {
		t.Fatalf("expected TPPrice=0 when TPATRMultiplier=0, got %s", intent.TPPrice)
	}
	if intent.StrategyID != StrategyIDCTA4hShort || intent.EntryOrderType != "MARKET" {
		t.Fatalf("unexpected intent metadata: %+v", intent)
	}

	// Wide ATR (3.5% -> 3*3.5% = 10.5% > 8.0% max at 10x lev) must fail liquidation guard cleanly
	candTooWide := candOK
	candTooWide.ATR14_4h = 3.5
	_, okWide, rejWide := BuildCTA4hOrderIntent(candTooWide, snap, decimal.NewFromFloat(1000), decimal.NewFromFloat(1000), cfg)
	if okWide || rejWide != "SIZING_FAILED" {
		t.Fatalf("expected SIZING_FAILED for 10.5%% SL at 10x lev, got ok=%v reason=%s", okWide, rejWide)
	}
}

func TestBuildAllPodExitProfiles(t *testing.T) {
	cfg := &config.Config{}
	if p := BuildAllPodExitProfiles(cfg); p != nil {
		t.Fatalf("expected nil when all pods disabled, got %+v", p)
	}
	cfg.MajorPod.ShortEnabled = true
	cfg.MajorPod.TrailingEnabled = true
	cfg.MajorPod.TrailingActivateATR = 2.5
	cfg.MajorPod.TrailingDistanceATR = 1.2
	cfg.MajorPod.MaxHoldingDuration = 48 * time.Hour

	pMaj := BuildAllPodExitProfiles(cfg)
	if len(pMaj) != 2 || pMaj[StrategyIDMajorRSMomShort].ChandelierTrailing {
		t.Fatalf("expected only MajorPod profiles without Chandelier, got %+v", pMaj)
	}

	cfg.CTAPod.ShortEnabled = true
	cfg.CTAPod.TrailingEnabled = true
	cfg.CTAPod.ChandelierTrailing = true
	cfg.CTAPod.TrailingDistanceATR = 3.0
	cfg.CTAPod.MaxHoldingDuration = 240 * time.Hour

	pAll := BuildAllPodExitProfiles(cfg)
	if len(pAll) != 4 || !pAll[StrategyIDCTA4hShort].ChandelierTrailing || pAll[StrategyIDCTA4hShort].TrailingDistanceATR != 3.0 {
		t.Fatalf("expected merged MajorPod+CTAPod profiles, got %+v", pAll)
	}
}
