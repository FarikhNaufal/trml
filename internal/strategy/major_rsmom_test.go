package strategy

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/config"
	"trml/internal/model"
)

func buildSynthetic15mHourlySeries(
	start time.Time,
	hours int,
	closeAtHour func(h int) float64,
	highAtHour func(h int) float64,
	lowAtHour func(h int) float64,
	deltaAtHour func(h int) float64,
) []model.Candle {
	out := make([]model.Candle, 0, hours*4)
	for h := 0; h < hours; h++ {
		hc := closeAtHour(h)
		hh := hc * 1.003
		hl := hc * 0.997
		if highAtHour != nil {
			hh = highAtHour(h)
		}
		if lowAtHour != nil {
			hl = lowAtHour(h)
		}
		qvTotal := 10_000_000.0
		deltaTotal := 0.0
		if deltaAtHour != nil {
			deltaTotal = deltaAtHour(h)
		}
		takerQ := 0.5 * (deltaTotal + qvTotal)
		for q := 0; q < 4; q++ {
			openT := start.Add(time.Duration(h)*time.Hour + time.Duration(q)*15*time.Minute)
			closeT := openT.Add(15*time.Minute - time.Millisecond)
			cHigh := hc * 1.001
			cLow := hc * 0.999
			cClose := hc
			if q == 0 {
				cHigh = hh
			} else if q == 1 {
				cLow = hl
			}
			if cHigh < cClose {
				cHigh = cClose
			}
			if cLow > cClose {
				cLow = cClose
			}
			out = append(out, model.Candle{
				OpenTime:          openT,
				CloseTime:         closeT,
				Open:              hc,
				High:              cHigh,
				Low:               cLow,
				Close:             cClose,
				VolumeBase:        (qvTotal / 4.0) / hc,
				QuoteVolumeUSDT:   qvTotal / 4.0,
				TakerBuyQuoteUSDT: takerQ / 4.0,
			})
		}
	}
	return out
}

func makeMajorTestCfg() EvaluatorConfig {
	return EvaluatorConfig{
		CandleDuration:   15 * time.Minute,
		MaxBookStaleness: 5 * time.Second,
		MaxMarkStaleness: 15 * time.Second,
		MaxDataStaleness: 16 * time.Minute,
		MaxSpreadPct:     0.15,
		MajorPod: MajorPodConfig{
			ShortEnabled:        true,
			LongEnabled:         true,
			Symbols:             DefaultMajorPodSymbols(),
			MaxSlots:            3,
			ShortRiskPct:        decimal.NewFromFloat(0.0060),
			LongRiskPct:         decimal.NewFromFloat(0.0055),
			RSWindowHours:       24,
			BreakoutLookbackHrs: 12,
			ShortRSMinPct:       3.0,
			LongRSMinPct:        4.5,
			LongRSMaxPct:        20.0,
			MinDeltaZ:           0.40,
			LongMaxFundingRate:  0.0001,
			ShortMinFundingRate: -1.0,
			SLATRMultiplier:     2.0,
			TPATRMultiplier:     6.0,
			MinSLDistancePct:    decimal.NewFromFloat(0.0100),
			TrailingEnabled:     true,
			TrailingActivateATR: 2.5,
			TrailingDistanceATR: 1.2,
			MaxHoldingDuration:  48 * time.Hour,
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

func TestResampleCompletedHourlyStrict_DropsIncompleteHour(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	bars := buildSynthetic15mHourlySeries(t0, 3, func(h int) float64 { return 100.0 + float64(h) }, nil, nil, nil)
	// Truncate last bar of hour 2 (leave only 3 of 4 bars in trailing hour)
	partial := bars[:len(bars)-1]

	hourly := ResampleCompletedHourlyStrict(partial, 15*time.Minute)
	if len(hourly) != 2 {
		t.Fatalf("expected 2 complete hourly bars when trailing hour only has 3/4 sub-bars, got %d", len(hourly))
	}
}

func TestIsHourCloseTimestamp(t *testing.T) {
	if !IsHourCloseTimestamp(time.Date(2026, 10, 1, 13, 59, 59, 999_000_000, time.UTC)) {
		t.Fatalf("expected :59:59.999 to be treated as hour close")
	}
	if !IsHourCloseTimestamp(time.Date(2026, 10, 1, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected exact :00:00 to be treated as hour close")
	}
	if IsHourCloseTimestamp(time.Date(2026, 10, 1, 14, 14, 59, 999_000_000, time.UTC)) {
		t.Fatalf("did not expect :14:59.999 to be treated as hour close")
	}
}

func TestEvaluateMajorRSMomCandidate_ShortAndLongSignalsAndGates(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	const totalH = 260
	endEval := start.Add(totalH * time.Hour)

	// Case 1: Pod #2 SHORT approved (BTC < 1h EMA200, SOL RS24h <= -3%, breakdown 12h LL, negative deltaZ)
	btcBearCandles := buildSynthetic15mHourlySeries(
		start, totalH,
		func(h int) float64 {
			if h < 230 {
				return 65000.0
			}
			return 64000.0 - float64(h-230)*10.0 // gently down, below EMA200
		},
		nil, nil, nil,
	)
	solBearCandles := buildSynthetic15mHourlySeries(
		start, totalH,
		func(h int) float64 {
			if h < totalH-1 {
				return 100.0
			}
			return 94.0 // -6% drop vs ~-0.5% BTC => RS24h ~ -5.5% <= -3%, below 12h LL (99.7) & below EMA200
		},
		nil,
		func(h int) float64 {
			if h == totalH-1 {
				return 93.5
			}
			return 99.7
		},
		func(h int) float64 {
			if h == totalH-1 {
				return -3_000_000.0 // strongly negative delta outlier
			}
			if h%2 == 0 {
				return 100_000.0
			}
			return -100_000.0
		},
	)

	cfg := makeMajorTestCfg()
	f := model.ExchangeFilter{
		TickSize:    decimal.NewFromFloat(0.01),
		StepSize:    decimal.NewFromFloat(0.1),
		MinQty:      decimal.NewFromFloat(0.1),
		MinNotional: decimal.NewFromFloat(5.0),
	}
	btcSnap := &model.MarketSnapshot{Symbol: "BTCUSDT", Now: endEval, ClosedCandles: btcBearCandles}
	solSnap := model.MarketSnapshot{
		Symbol:             "SOLUSDT",
		Filter:             f,
		Now:                endEval,
		BestBid:            93.99,
		BestAsk:            94.01,
		BookUpdatedAt:      endEval,
		MarkPrice:          94.0,
		MarkPriceUpdatedAt: endEval,
		FundingRate:        0.0001,
		FundingUpdatedAt:   endEval,
		ClosedCandles:      solBearCandles,
	}

	cand := EvaluateMajorRSMomCandidate(solSnap, btcSnap, false, false, cfg)
	if !cand.Approved || cand.Side != model.SideShort || cand.StrategyID != StrategyIDMajorRSMomShort {
		t.Fatalf("expected approved MAJOR_RSMOM_SHORT, got %+v", cand)
	}
	if cand.RS24hPct > -3.0 || cand.DeltaZ1h > -0.40 || cand.ATR14_1h <= 0 {
		t.Fatalf("unexpected candidate metrics: %+v", cand)
	}

	intent, okBuild, rBuild := BuildMajorRSMomOrderIntent(cand, solSnap, decimal.NewFromFloat(200.0), decimal.NewFromFloat(180.0), cfg)
	if !okBuild || rBuild != "SIGNAL_APPROVED" {
		t.Fatalf("expected BuildMajorRSMomOrderIntent success, got ok=%v reason=%s", okBuild, rBuild)
	}
	if intent.EntryOrderType != "MARKET" || !intent.TP1Price.IsZero() || intent.StrategyID != StrategyIDMajorRSMomShort {
		t.Fatalf("unexpected MajorPod intent fields: %+v", intent)
	}
	if math.Abs(intent.ATRAtEntry.InexactFloat64()-cand.ATR14_1h) > 1e-6 {
		t.Fatalf("expected ATRAtEntry=%f (1h ATR), got %s", cand.ATR14_1h, intent.ATRAtEntry)
	}

	// Case 2: Mutual exclusivity — under BTC < EMA200, Pod #3 LONG does NOT fire even if a coin pumps
	suiBullInBear := buildSynthetic15mHourlySeries(
		start, totalH,
		func(h int) float64 {
			if h < totalH-1 {
				return 1.00
			}
			return 1.07 // +7% pump while BTC bearish
		},
		nil, nil,
		func(h int) float64 {
			if h == totalH-1 {
				return 3_000_000.0
			}
			if h%2 == 0 {
				return 100_000.0
			}
			return -100_000.0
		},
	)
	suiSnap := solSnap
	suiSnap.Symbol = "SUIUSDT"
	suiSnap.BestBid = 1.0699
	suiSnap.BestAsk = 1.0701
	suiSnap.MarkPrice = 1.07
	suiSnap.ClosedCandles = suiBullInBear
	resExcl := EvaluateMajorRSMomCandidate(suiSnap, btcSnap, false, false, cfg)
	if resExcl.Approved {
		t.Fatalf("expected LONG blocked when BTC < 1h EMA200, got %+v", resExcl)
	}

	// Case 3: Pod #3 LONG approved under BTC > 1h EMA200 + funding <= +1bp
	btcBullCandles := buildSynthetic15mHourlySeries(
		start, totalH,
		func(h int) float64 {
			if h < 230 {
				return 64000.0
			}
			return 65000.0 + float64(h-230)*20.0
		},
		nil, nil, nil,
	)
	btcBullSnap := &model.MarketSnapshot{Symbol: "BTCUSDT", Now: endEval, ClosedCandles: btcBullCandles}
	candLong := EvaluateMajorRSMomCandidate(suiSnap, btcBullSnap, false, false, cfg)
	if !candLong.Approved || candLong.Side != model.SideLong || candLong.StrategyID != StrategyIDMajorRSMomLong {
		t.Fatalf("expected approved MAJOR_RSMOM_LONG, got %+v", candLong)
	}

	// Case 4: Funding > +1.0bp rejects LONG with MAJOR_LONG_FUNDING_CAPPED
	suiCrowded := suiSnap
	suiCrowded.FundingRate = 0.00015
	candFunding := EvaluateMajorRSMomCandidate(suiCrowded, btcBullSnap, false, false, cfg)
	if candFunding.Approved || candFunding.Reason != "MAJOR_LONG_FUNDING_CAPPED" {
		t.Fatalf("expected MAJOR_LONG_FUNDING_CAPPED, got %+v", candFunding)
	}

	// Case 5: RS24h > +20% rejects LONG as overextended
	suiMoon := buildSynthetic15mHourlySeries(
		start, totalH,
		func(h int) float64 {
			if h < totalH-1 {
				return 1.00
			}
			return 1.25
		},
		nil, nil,
		func(h int) float64 {
			if h == totalH-1 {
				return 3_000_000.0
			}
			if h%2 == 0 {
				return 100_000.0
			}
			return -100_000.0
		},
	)
	suiMoonSnap := suiSnap
	suiMoonSnap.BestBid = 1.2499
	suiMoonSnap.BestAsk = 1.2501
	suiMoonSnap.ClosedCandles = suiMoon
	candExt := EvaluateMajorRSMomCandidate(suiMoonSnap, btcBullSnap, false, false, cfg)
	if candExt.Approved || candExt.Reason != "MAJOR_LONG_RS_OVEREXTENDED" {
		t.Fatalf("expected MAJOR_LONG_RS_OVEREXTENDED, got %+v", candExt)
	}

	// Case 6: Cooldown enforced cleanly
	suiCooldown := suiSnap
	suiCooldown.LastExitTime = endEval.Add(-2 * time.Hour) // < 240m
	candCD := EvaluateMajorRSMomCandidate(suiCooldown, btcBullSnap, false, false, cfg)
	if candCD.Approved || candCD.Reason != "SYMBOL_IN_COOLDOWN" {
		t.Fatalf("expected SYMBOL_IN_COOLDOWN, got %+v", candCD)
	}
}

func TestBuildMajorPodExitProfiles(t *testing.T) {
	cfg := &config.Config{}
	if p := BuildMajorPodExitProfiles(cfg); p != nil {
		t.Fatalf("expected nil profiles when disabled, got %+v", p)
	}
	cfg.MajorPod = config.MajorPodStrategyConfig{
		ShortEnabled:        true,
		TrailingEnabled:     true,
		TrailingActivateATR: 2.5,
		TrailingDistanceATR: 1.2,
		MaxHoldingDuration:  48 * time.Hour,
		StaleLossDuration:   0,
	}
	profiles := BuildMajorPodExitProfiles(cfg)
	if len(profiles) != 2 || !profiles[StrategyIDMajorRSMomShort].HasOverride || !profiles[StrategyIDMajorRSMomLong].HasOverride {
		t.Fatalf("unexpected profiles: %+v", profiles)
	}
}

func TestMergeCompletedHourlyCandleAndPrecedence(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// First 180 hours at 100.0, next 320 hours at 50.0 -> 500h EMA200 (~52.04) > 51.0 while 320h EMA200 (~50.01) < 51.0.
	full15m := buildSynthetic15mHourlySeries(start, 500, func(h int) float64 {
		if h < 180 {
			return 100.0
		}
		if h < 499 {
			return 50.0
		}
		return 51.0
	}, nil, nil, nil)

	var merged []model.Candle
	for i := 0; i < len(full15m); i += 4 {
		tailStart := max(0, i-8)
		merged = MergeCompletedHourlyCandle(merged, full15m[tailStart:i+4], 15*time.Minute, MajorPodHourlyHistoryCap)
	}
	if len(merged) != 500 {
		t.Fatalf("expected 500 merged 1h candles, got %d", len(merged))
	}
	// Re-merging duplicate sub-bars must remain idempotent at 500.
	merged2 := MergeCompletedHourlyCandle(merged, full15m[len(full15m)-8:], 15*time.Minute, MajorPodHourlyHistoryCap)
	if len(merged2) != 500 {
		t.Fatalf("expected idempotent 500 bars, got %d", len(merged2))
	}

	// Verify ClosedHourlyCandles overrides truncated 320h ClosedCandles in EvaluateMajorRSMomCandidate.
	endEval := start.Add(500 * time.Hour)
	btcSnap := &model.MarketSnapshot{
		Symbol:              "BTCUSDT",
		Now:                 endEval,
		ClosedCandles:       full15m[len(full15m)-320*4:], // truncated 320h -> EMA200 ~50.01 (< 51.0)
		ClosedHourlyCandles: merged,                       // full 500h -> EMA200 ~52.04 (> 51.0)
	}
	sol15m := buildSynthetic15mHourlySeries(start, 500, func(h int) float64 {
		if h < 499 {
			return 100.0
		}
		return 115.0
	}, nil, nil, func(h int) float64 {
		if h == 499 {
			return 3_000_000.0
		}
		if h%2 == 0 {
			return 100_000.0
		}
		return -100_000.0
	})
	solSnap := model.MarketSnapshot{
		Symbol:              "SOLUSDT",
		Now:                 endEval,
		BestBid:             114.99,
		BestAsk:             115.01,
		BookUpdatedAt:       endEval,
		MarkPrice:           115.0,
		MarkPriceUpdatedAt:  endEval,
		FundingRate:         0.0,
		FundingUpdatedAt:    endEval,
		ClosedCandles:       sol15m,
		ClosedHourlyCandles: MergeCompletedHourlyCandle(nil, sol15m, 15*time.Minute, MajorPodHourlyHistoryCap),
	}
	cfg := EvaluatorConfig{
		CandleDuration: 15 * time.Minute,
		MajorPod: MajorPodConfig{
			ShortEnabled:        true,
			LongEnabled:         true,
			Symbols:             []string{"BTCUSDT", "SOLUSDT"},
			ShortRSMinPct:       3.0,
			LongRSMinPct:        4.5,
			LongRSMaxPct:        20.0,
			RSWindowHours:       24,
			BreakoutLookbackHrs: 12,
			MinDeltaZ:           0.40,
			LongMaxFundingRate:  0.0001,
			ShortMinFundingRate: -1.0,
		},
	}
	cand := EvaluateMajorRSMomCandidate(solSnap, btcSnap, false, false, cfg)
	if cand.Approved || cand.Side != model.SideShort {
		// BTC Close 51.0 < full 500h EMA200 (~52.04) => evaluates SHORT side and blocks LONG.
		t.Fatalf("expected BTC full 500h EMA200 to keep regime SHORT (blocking LONG), got %+v", cand)
	}
}

func TestMajorRSMom_Rejects1hStaleBTCAndNaNFunding(t *testing.T) {
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	endSym := base.Add(220 * time.Hour)
	btc15m := buildSynthetic15mHourlySeries(base, 219, func(h int) float64 { return 100.0 + float64(h)*0.1 }, nil, nil, nil)
	sol15m := buildSynthetic15mHourlySeries(base, 220, func(h int) float64 {
		if h < 219 {
			return 100.0
		}
		return 112.0
	}, nil, nil, func(h int) float64 {
		if h == 219 {
			return 2_000_000.0
		}
		if h%2 == 0 {
			return 100_000.0
		}
		return -100_000.0
	})
	cfg := EvaluatorConfig{
		CandleDuration: 15 * time.Minute,
		MajorPod: MajorPodConfig{
			ShortEnabled:        true,
			LongEnabled:         true,
			Symbols:             []string{"BTCUSDT", "SOLUSDT"},
			ShortRSMinPct:       3.0,
			LongRSMinPct:        4.5,
			LongRSMaxPct:        20.0,
			RSWindowHours:       24,
			BreakoutLookbackHrs: 12,
			MinDeltaZ:           0.40,
			LongMaxFundingRate:  0.0001,
			ShortMinFundingRate: -1.0,
		},
	}
	btcSnap := &model.MarketSnapshot{
		Symbol:              "BTCUSDT",
		Now:                 endSym,
		ClosedCandles:       btc15m,
		ClosedHourlyCandles: MergeCompletedHourlyCandle(nil, btc15m, 15*time.Minute, MajorPodHourlyHistoryCap),
	}
	solSnap := model.MarketSnapshot{
		Symbol:              "SOLUSDT",
		Now:                 endSym,
		BestBid:             111.99,
		BestAsk:             112.01,
		BookUpdatedAt:       endSym,
		MarkPrice:           112.0,
		MarkPriceUpdatedAt:  endSym,
		FundingRate:         0.0,
		FundingUpdatedAt:    endSym,
		ClosedCandles:       sol15m,
		ClosedHourlyCandles: MergeCompletedHourlyCandle(nil, sol15m, 15*time.Minute, MajorPodHourlyHistoryCap),
	}

	// 1. BTC 1h history lagging 1 full hour behind SOL must be strictly rejected
	staleCand := EvaluateMajorRSMomCandidate(solSnap, btcSnap, false, false, cfg)
	if staleCand.Reason != "STALE_BTC_1H_BAR" {
		t.Fatalf("expected 1h-stale BTC bar rejected with STALE_BTC_1H_BAR, got %+v", staleCand)
	}

	// 2. Align BTC to 220h above EMA200 and pass NaN FundingRate on LONG -> INVALID_FUNDING_RATE
	btcFull := buildSynthetic15mHourlySeries(base, 220, func(h int) float64 { return 100.0 + float64(h)*0.1 }, nil, nil, nil)
	btcSnap.ClosedCandles = btcFull
	btcSnap.ClosedHourlyCandles = MergeCompletedHourlyCandle(nil, btcFull, 15*time.Minute, MajorPodHourlyHistoryCap)
	solSnap.FundingRate = math.NaN()
	nanCand := EvaluateMajorRSMomCandidate(solSnap, btcSnap, false, false, cfg)
	if nanCand.Reason != "INVALID_FUNDING_RATE" {
		t.Fatalf("expected NaN FundingRate rejected with INVALID_FUNDING_RATE, got %+v", nanCand)
	}
}
