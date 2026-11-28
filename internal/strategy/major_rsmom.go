package strategy

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
	"trml/internal/config"
	"trml/internal/indicator"
	"trml/internal/model"
)

// MajorRSMomCandidate represents the causal 1h evaluation outcome for Pod #2 (SHORT) / Pod #3 (LONG).
type MajorRSMomCandidate struct {
	Symbol      string
	Side        model.Side
	StrategyID  string
	RS24hPct    float64
	AbsRS24hPct float64
	Close1h     float64
	EMA200_1h   float64
	ATR14_1h    float64
	DeltaZ1h    float64
	FundingRate float64
	HourOpenUTC time.Time
	Approved    bool
	Reason      string
	SubReason   string
}

func normalizeBoundaryTime(t time.Time) time.Time {
	if !t.IsZero() && t.UnixMilli()%1000 == 999 {
		return t.Add(time.Millisecond)
	}
	return t
}

// IsHourCloseTimestamp reports whether t corresponds to an exact UTC hour boundary (including Binance ...59.999 bar close).
func IsHourCloseTimestamp(t time.Time) bool {
	if t.IsZero() {
		return false
	}
	norm := normalizeBoundaryTime(t).UTC()
	return norm.Minute() == 0 && norm.Second() == 0
}

// ResampleCompletedHourlyStrict deterministically aggregates closed sub-hourly (or 1h) candles into complete UTC 1h bars.
// When expectedSubStep is strictly less than 1 hour, a bucket must contain all expected sub-bars to be emitted, matching
// causal multi-timeframe research parity and preventing partial-hour bar leakage.
func ResampleCompletedHourlyStrict(candles []model.Candle, expectedSubStep time.Duration) []model.Candle {
	return resampleCompletedIntervalStrict(candles, time.Hour, expectedSubStep)
}

// MajorPodHourlyHistoryCap retains 1000 completed 1h bars (~41.7 days) so 1h EMA(200) converges cleanly (<0.04% seed residual).
const MajorPodHourlyHistoryCap = 1000

// MergeCompletedHourlyCandle resamples recent sub-hourly bars into completed UTC 1h candles and appends strictly newer bars onto existing, capped to maxBars.
func MergeCompletedHourlyCandle(existing []model.Candle, recentSubCandles []model.Candle, expectedSubStep time.Duration, maxBars int) []model.Candle {
	return mergeCompletedIntervalCandle(existing, recentSubCandles, time.Hour, expectedSubStep, maxBars)
}

func alignHourlyBTCBar(btcH []model.Candle, symHour model.Candle) (int, bool) {
	if len(btcH) == 0 {
		return -1, false
	}
	symClose := normalizeBoundaryTime(symHour.CloseTime)
	if symClose.IsZero() {
		symClose = symHour.OpenTime.Add(time.Hour)
	}

	bestIdx := -1
	for i := len(btcH) - 1; i >= 0; i-- {
		btcClose := normalizeBoundaryTime(btcH[i].CloseTime)
		if btcClose.IsZero() {
			btcClose = btcH[i].OpenTime.Add(time.Hour)
		}
		if !btcClose.After(symClose) {
			bestIdx = i
			break
		}
	}
	if bestIdx < 0 {
		return -1, false
	}
	if symHour.OpenTime.Sub(btcH[bestIdx].OpenTime) >= time.Hour {
		return bestIdx, false
	}
	return bestIdx, true
}

func computeHourlyDeltas(hourly []model.Candle) []float64 {
	deltas := make([]float64, len(hourly))
	for i, c := range hourly {
		d := 2.0*c.TakerBuyQuoteUSDT - c.QuoteVolumeUSDT
		if math.IsNaN(d) || math.IsInf(d, 0) {
			d = 0.0
		}
		deltas[i] = d
	}
	return deltas
}

// EvaluateMajorRSMomCandidate evaluates a closed 1h bar on a liquid major symbol for Pod #2 (SHORT) or Pod #3 (LONG).
func EvaluateMajorRSMomCandidate(
	snap model.MarketSnapshot,
	btcSnap *model.MarketSnapshot,
	hasActiveSymbol bool,
	isDailyBreached bool,
	cfg EvaluatorConfig,
) MajorRSMomCandidate {
	cand := MajorRSMomCandidate{
		Symbol:      snap.Symbol,
		StrategyID:  StrategyIDMajorRSMomShort,
		FundingRate: snap.FundingRate,
	}
	if !cfg.MajorPod.IsEnabled() {
		cand.Reason = "MAJOR_POD_DISABLED"
		return cand
	}
	if snap.Symbol == "BTCUSDT" {
		cand.Reason = "BENCHMARK_SYMBOL_SKIPPED"
		return cand
	}
	if ok, reason, sub := evaluatePodPreGuards(snap, hasActiveSymbol, isDailyBreached, cfg.MajorPod.CooldownMinutes, cfg); !ok {
		cand.Reason = reason
		cand.SubReason = sub
		return cand
	}

	rsWin := cfg.MajorPod.RSWindowHours
	if rsWin <= 0 {
		rsWin = 24
	}
	breakLB := cfg.MajorPod.BreakoutLookbackHrs
	if breakLB <= 0 {
		breakLB = 12
	}

	symH := snap.ClosedHourlyCandles
	if len(symH) == 0 {
		symH = ResampleCompletedHourlyStrict(snap.ClosedCandles, cfg.CandleDuration)
	}
	minSymBars := max(205, max(rsWin+2, max(breakLB+2, 52)))
	if len(symH) < minSymBars {
		cand.Reason = "INSUFFICIENT_1H_HISTORY"
		cand.SubReason = fmt.Sprintf("1H_BARS_%d_LT_%d", len(symH), minSymBars)
		return cand
	}

	last := len(symH) - 1
	cand.HourOpenUTC = symH[last].OpenTime
	cand.Close1h = symH[last].Close

	if btcSnap == nil || (len(btcSnap.ClosedCandles) == 0 && len(btcSnap.ClosedHourlyCandles) == 0) {
		cand.Reason = "INSUFFICIENT_BTC_1H_HISTORY"
		cand.SubReason = "BTC_SNAPSHOT_EMPTY"
		return cand
	}
	btcH := btcSnap.ClosedHourlyCandles
	if len(btcH) == 0 {
		btcH = ResampleCompletedHourlyStrict(btcSnap.ClosedCandles, cfg.CandleDuration)
	}
	minBTCBars := max(205, rsWin+2)
	if len(btcH) < minBTCBars {
		cand.Reason = "INSUFFICIENT_BTC_1H_HISTORY"
		cand.SubReason = fmt.Sprintf("BTC_1H_BARS_%d_LT_%d", len(btcH), minBTCBars)
		return cand
	}

	bi, aligned := alignHourlyBTCBar(btcH, symH[last])
	if bi < 0 || !aligned {
		cand.Reason = "STALE_BTC_1H_BAR"
		return cand
	}
	if bi < max(200, rsWin) {
		cand.Reason = "INSUFFICIENT_BTC_1H_ALIGNMENT"
		cand.SubReason = fmt.Sprintf("BTC_ALIGNED_IDX_%d", bi)
		return cand
	}

	symEMA200, okEMA := indicator.CalculateEMASafe(symH, 200)
	if !okEMA || !isFinitePositive(symEMA200) {
		cand.Reason = "INVALID_1H_EMA"
		return cand
	}
	symATR14, okATR := indicator.CalculateATRSafe(symH, 14)
	if !okATR || !isFinitePositive(symATR14) {
		cand.Reason = "INVALID_1H_ATR"
		return cand
	}
	btcSeries := btcH[:bi+1]
	btcEMA200, okBTCEMA := indicator.CalculateEMASafe(btcSeries, 200)
	if !okBTCEMA || !isFinitePositive(btcEMA200) {
		cand.Reason = "INVALID_BTC_1H_EMA"
		return cand
	}

	hourlyDeltas := computeHourlyDeltas(symH)
	dz, dzOK := indicator.CalculateBarDeltaZScore(hourlyDeltas, 50)
	if !dzOK || !isFiniteFloat(dz) {
		cand.Reason = "INVALID_1H_DELTA_ZSCORE"
		return cand
	}

	symPrev := symH[last-rsWin].Close
	btcClose := btcH[bi].Close
	btcPrev := btcH[bi-rsWin].Close
	if !isFinitePositive(symPrev) || !isFinitePositive(btcClose) || !isFinitePositive(btcPrev) {
		cand.Reason = "INVALID_1H_RETURN_BASELINE"
		return cand
	}

	symRet := (symH[last].Close - symPrev) / symPrev
	btcRet := (btcClose - btcPrev) / btcPrev
	rsPct := (symRet - btcRet) * 100.0
	if !isFiniteFloat(rsPct) {
		cand.Reason = "INVALID_RS24H"
		return cand
	}

	cand.EMA200_1h = symEMA200
	cand.ATR14_1h = symATR14
	cand.DeltaZ1h = dz
	cand.RS24hPct = rsPct
	cand.AbsRS24hPct = math.Abs(rsPct)

	prevHH := symH[last-breakLB].High
	prevLL := symH[last-breakLB].Low
	for k := last - breakLB + 1; k < last; k++ {
		if symH[k].High > prevHH {
			prevHH = symH[k].High
		}
		if symH[k].Low < prevLL {
			prevLL = symH[k].Low
		}
	}

	symClose := symH[last].Close
	if btcClose < btcEMA200 {
		cand.Side = model.SideShort
		cand.StrategyID = StrategyIDMajorRSMomShort
		if !cfg.MajorPod.ShortEnabled {
			cand.Reason = "MAJOR_SHORT_DISABLED"
			return cand
		}
		if rsPct > -cfg.MajorPod.ShortRSMinPct {
			cand.Reason = "MAJOR_SHORT_RS_NOT_WEAK"
			cand.SubReason = fmt.Sprintf("RS24H_%.2f%%_GT_-%.2f%%", rsPct, cfg.MajorPod.ShortRSMinPct)
			return cand
		}
		if symClose >= symEMA200 {
			cand.Reason = "MAJOR_SHORT_ABOVE_1H_EMA200"
			return cand
		}
		if symClose >= prevLL {
			cand.Reason = "MAJOR_SHORT_NO_12H_BREAKDOWN"
			return cand
		}
		if dz > -cfg.MajorPod.MinDeltaZ {
			cand.Reason = "MAJOR_SHORT_WEAK_DELTA_Z"
			cand.SubReason = fmt.Sprintf("DZ_%.2f_GT_-%.2f", dz, cfg.MajorPod.MinDeltaZ)
			return cand
		}
		if cfg.MajorPod.ShortMinFundingRate > -0.99 {
			if !isFiniteFloat(snap.FundingRate) {
				cand.Reason = "INVALID_FUNDING_RATE"
				return cand
			}
			if snap.FundingRate < cfg.MajorPod.ShortMinFundingRate {
				cand.Reason = "MAJOR_SHORT_FUNDING_TOO_NEGATIVE"
				cand.SubReason = fmt.Sprintf("FR_%.5f_LT_%.5f", snap.FundingRate, cfg.MajorPod.ShortMinFundingRate)
				return cand
			}
		}
		cand.Approved = true
		cand.Reason = "SIGNAL_APPROVED"
		cand.SubReason = fmt.Sprintf("RS24H_%.2f%%_DZ_%.2f", rsPct, dz)
		return cand
	}

	if btcClose > btcEMA200 {
		cand.Side = model.SideLong
		cand.StrategyID = StrategyIDMajorRSMomLong
		if !cfg.MajorPod.LongEnabled {
			cand.Reason = "MAJOR_LONG_DISABLED"
			return cand
		}
		if rsPct < cfg.MajorPod.LongRSMinPct {
			cand.Reason = "MAJOR_LONG_RS_NOT_STRONG"
			cand.SubReason = fmt.Sprintf("RS24H_%.2f%%_LT_%.2f%%", rsPct, cfg.MajorPod.LongRSMinPct)
			return cand
		}
		if cfg.MajorPod.LongRSMaxPct > 0 && rsPct > cfg.MajorPod.LongRSMaxPct {
			cand.Reason = "MAJOR_LONG_RS_OVEREXTENDED"
			cand.SubReason = fmt.Sprintf("RS24H_%.2f%%_GT_%.2f%%", rsPct, cfg.MajorPod.LongRSMaxPct)
			return cand
		}
		if symClose <= symEMA200 {
			cand.Reason = "MAJOR_LONG_BELOW_1H_EMA200"
			return cand
		}
		if symClose <= prevHH {
			cand.Reason = "MAJOR_LONG_NO_12H_BREAKOUT"
			return cand
		}
		if dz < cfg.MajorPod.MinDeltaZ {
			cand.Reason = "MAJOR_LONG_WEAK_DELTA_Z"
			cand.SubReason = fmt.Sprintf("DZ_%.2f_LT_%.2f", dz, cfg.MajorPod.MinDeltaZ)
			return cand
		}
		if !isFiniteFloat(snap.FundingRate) {
			cand.Reason = "INVALID_FUNDING_RATE"
			return cand
		}
		if snap.FundingRate > cfg.MajorPod.LongMaxFundingRate {
			cand.Reason = "MAJOR_LONG_FUNDING_CAPPED"
			cand.SubReason = fmt.Sprintf("FR_%.5f_GT_%.5f", snap.FundingRate, cfg.MajorPod.LongMaxFundingRate)
			return cand
		}
		cand.Approved = true
		cand.Reason = "SIGNAL_APPROVED"
		cand.SubReason = fmt.Sprintf("RS24H_+%.2f%%_DZ_+%.2f", rsPct, dz)
		return cand
	}

	cand.Reason = "BTC_1H_NEUTRAL_EMA200"
	return cand
}

// BuildMajorRSMomOrderIntent builds an OrderIntent for an approved MajorPod candidate using the 1h ATR and
// pod-specific risk & exit envelope. Global portfolio drawdown throttling applies, while 15m mid-cap multipliers do not.
func BuildMajorRSMomOrderIntent(
	cand MajorRSMomCandidate,
	snap model.MarketSnapshot,
	effectiveEquity, availBal decimal.Decimal,
	cfg EvaluatorConfig,
) (model.OrderIntent, bool, string) {
	if !cand.Approved {
		return model.OrderIntent{}, false, cand.Reason
	}

	entryType := strings.ToUpper(strings.TrimSpace(cfg.MajorPod.EntryOrderType))
	if entryType == "" {
		entryType = "MARKET"
	}
	isMarket := entryType == "MARKET"

	var rawEntry float64
	if cand.Side == model.SideLong {
		if isMarket {
			rawEntry = snap.BestAsk
		} else {
			rawEntry = snap.BestBid
		}
	} else {
		if isMarket {
			rawEntry = snap.BestBid
		} else {
			rawEntry = snap.BestAsk
		}
	}

	riskPct := cfg.MajorPod.ShortRiskPct
	if cand.Side == model.SideLong {
		riskPct = cfg.MajorPod.LongRiskPct
	}
	riskPct = applyPortfolioDDThrottleAndCap(riskPct, snap, cfg)

	slMult := cfg.MajorPod.SLATRMultiplier
	if slMult <= 0 {
		slMult = 2.0
	}
	tpMult := cfg.MajorPod.TPATRMultiplier
	if tpMult <= 0 {
		tpMult = 6.0
	}

	sizingCfg := SizingConfig{
		RiskPerTradePct:                riskPct,
		MaxSlotMarginFraction:          cfg.Sizing.MaxSlotMarginFraction,
		AvailableBalanceBufferFraction: cfg.Sizing.AvailableBalanceBufferFraction,
		AccountLeverage:                cfg.Sizing.AccountLeverage,
		SLMultiplier:                   slMult,
		SLMultiplierLong:               0,
		TPMultiplier:                   tpMult,
		MinSLDistancePct:               cfg.MajorPod.MinSLDistancePct,
		MakerFeeRate:                   cfg.Sizing.MakerFeeRate,
		TakerFeeRate:                   cfg.Sizing.TakerFeeRate,
		SlippageRate:                   cfg.Sizing.SlippageRate,
		EntryOrderType:                 entryType,
		DisablePartialTP:               true,
	}

	intent, ok := BuildSafeDecimalOrderIntent(snap, effectiveEquity, availBal, cand.Side, rawEntry, cand.ATR14_1h, sizingCfg)
	if !ok {
		return model.OrderIntent{}, false, "SIZING_FAILED"
	}
	intent.StrategyID = NormalizeStrategyID(cand.StrategyID)
	intent.EntryOrderType = entryType
	return intent, true, "SIGNAL_APPROVED"
}

// BuildMajorPodExitProfiles constructs per-strategy exit overrides for Pod #2 and Pod #3 so brokers preserve
// Pod #1 (QMAX) rules unchanged while applying 1h trailing and 48h max-hold rules exclusively to MajorPod positions.
func BuildMajorPodExitProfiles(cfg *config.Config) map[string]protection.StrategyExitProfile {
	if cfg == nil || !cfg.MajorPod.IsEnabled() {
		return nil
	}
	prof := protection.StrategyExitProfile{
		HasOverride:            true,
		PartialTPEnabled:       false,
		Tier1BEEnabled:         false,
		Tier2ProfitLockEnabled: false,
		TrailingEnabled:        cfg.MajorPod.TrailingEnabled,
		TrailingActivateATR:    cfg.MajorPod.TrailingActivateATR,
		TrailingDistanceATR:    cfg.MajorPod.TrailingDistanceATR,
		StaleLossExitDuration:  cfg.MajorPod.StaleLossDuration,
		MaxHoldingDuration:     cfg.MajorPod.MaxHoldingDuration,
	}
	return map[string]protection.StrategyExitProfile{
		StrategyIDMajorRSMomShort: prof,
		StrategyIDMajorRSMomLong:  prof,
	}
}
