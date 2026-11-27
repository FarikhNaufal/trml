package strategy

import (
	"fmt"
	"math"
	"sort"
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

type hourlyResampleBucket struct {
	candle   model.Candle
	count    int
	seenOpen map[int64]bool
}

// ResampleCompletedHourlyStrict deterministically aggregates closed sub-hourly (or 1h) candles into complete UTC 1h bars.
// When expectedSubStep is strictly less than 1 hour, a bucket must contain all expected sub-bars to be emitted, matching
// causal multi-timeframe research parity and preventing partial-hour bar leakage.
func ResampleCompletedHourlyStrict(candles []model.Candle, expectedSubStep time.Duration) []model.Candle {
	if len(candles) == 0 {
		return nil
	}

	requiredBars := 0
	if expectedSubStep > 0 && expectedSubStep < time.Hour && (time.Hour%expectedSubStep == 0) {
		requiredBars = int(time.Hour / expectedSubStep)
	}

	var inferredStep time.Duration
	var prevOpen time.Time
	for _, c := range candles {
		ref := c.OpenTime
		if ref.IsZero() {
			ref = c.CloseTime
		}
		if ref.IsZero() {
			continue
		}
		if !prevOpen.IsZero() && ref.After(prevOpen) {
			step := ref.Sub(prevOpen)
			if inferredStep == 0 || step < inferredStep {
				inferredStep = step
			}
		}
		prevOpen = ref
	}

	buckets := make(map[int64]*hourlyResampleBucket, len(candles)/4+1)
	keys := make([]int64, 0, len(candles)/4+1)

	for _, c := range candles {
		ref := c.OpenTime
		if ref.IsZero() {
			ref = c.CloseTime
		}
		if ref.IsZero() || !isFinitePositive(c.Open) || !isFinitePositive(c.High) ||
			!isFinitePositive(c.Low) || !isFinitePositive(c.Close) || c.Low > c.High {
			continue
		}
		volBase := c.VolumeBase
		if !isFiniteFloat(volBase) || volBase < 0 {
			volBase = 0
		}
		quoteVol := c.QuoteVolumeUSDT
		if !isFiniteFloat(quoteVol) || quoteVol < 0 {
			quoteVol = 0
		}
		takerQuote := c.TakerBuyQuoteUSDT
		if !isFiniteFloat(takerQuote) || takerQuote < 0 {
			takerQuote = 0
		}

		hourKey := (ref.UTC().Unix() / 3600) * 3600
		cClose := c.CloseTime
		if cClose.IsZero() && inferredStep > 0 {
			cClose = c.OpenTime.Add(inferredStep)
		}

		b, ok := buckets[hourKey]
		if !ok {
			openT := time.Unix(hourKey, 0).UTC()
			b = &hourlyResampleBucket{
				candle: model.Candle{
					OpenTime:          openT,
					CloseTime:         cClose,
					Open:              c.Open,
					High:              c.High,
					Low:               c.Low,
					Close:             c.Close,
					VolumeBase:        volBase,
					QuoteVolumeUSDT:   quoteVol,
					TakerBuyQuoteUSDT: takerQuote,
				},
				seenOpen: make(map[int64]bool, 4),
			}
			if !c.OpenTime.IsZero() {
				b.seenOpen[c.OpenTime.UTC().UnixMilli()] = true
			}
			b.count = 1
			buckets[hourKey] = b
			keys = append(keys, hourKey)
			continue
		}

		if !c.OpenTime.IsZero() {
			ms := c.OpenTime.UTC().UnixMilli()
			if b.seenOpen[ms] {
				continue
			}
			b.seenOpen[ms] = true
		}

		if c.High > b.candle.High {
			b.candle.High = c.High
		}
		if c.Low < b.candle.Low {
			b.candle.Low = c.Low
		}
		b.candle.Close = c.Close
		if cClose.After(b.candle.CloseTime) {
			b.candle.CloseTime = cClose
		}
		b.candle.VolumeBase += volBase
		b.candle.QuoteVolumeUSDT += quoteVol
		b.candle.TakerBuyQuoteUSDT += takerQuote
		b.count++
	}

	if len(keys) == 0 {
		return nil
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]model.Candle, 0, len(keys))
	for _, k := range keys {
		b := buckets[k]
		if requiredBars > 0 && b.count != requiredBars {
			continue
		}
		hourAlmostDone := b.candle.OpenTime.Add(time.Hour - time.Minute)
		if !b.candle.CloseTime.IsZero() && b.candle.CloseTime.Before(hourAlmostDone) {
			continue
		}
		out = append(out, b.candle)
	}
	return out
}

// MajorPodHourlyHistoryCap retains 1000 completed 1h bars (~41.7 days) so 1h EMA(200) converges cleanly (<0.04% seed residual).
const MajorPodHourlyHistoryCap = 1000

// MergeCompletedHourlyCandle resamples recent sub-hourly bars into completed UTC 1h candles and appends strictly newer bars onto existing, capped to maxBars.
func MergeCompletedHourlyCandle(existing []model.Candle, recentSubCandles []model.Candle, expectedSubStep time.Duration, maxBars int) []model.Candle {
	completed := ResampleCompletedHourlyStrict(recentSubCandles, expectedSubStep)
	if len(completed) == 0 {
		return existing
	}

	var lastOpen time.Time
	if len(existing) > 0 {
		lastOpen = existing[len(existing)-1].OpenTime
	}
	appended := 0
	for _, c := range completed {
		if !lastOpen.IsZero() && !c.OpenTime.After(lastOpen) {
			continue
		}
		appended++
	}
	if appended == 0 {
		if maxBars > 0 && len(existing) > maxBars {
			return append([]model.Candle(nil), existing[len(existing)-maxBars:]...)
		}
		return existing
	}

	out := make([]model.Candle, len(existing), len(existing)+appended)
	copy(out, existing)
	for _, c := range completed {
		if !lastOpen.IsZero() && !c.OpenTime.After(lastOpen) {
			continue
		}
		out = append(out, c)
		lastOpen = c.OpenTime
	}
	if maxBars > 0 && len(out) > maxBars {
		out = append([]model.Candle(nil), out[len(out)-maxBars:]...)
	}
	return out
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
	if isDailyBreached {
		cand.Reason = "CIRCUIT_BREAKER_ACTIVE"
		return cand
	}
	if hasActiveSymbol {
		cand.Reason = "SYMBOL_ALREADY_ACTIVE"
		return cand
	}
	if cfg.MajorPod.CooldownMinutes > 0 && !snap.LastExitTime.IsZero() {
		cdDur := time.Duration(cfg.MajorPod.CooldownMinutes) * time.Minute
		if snap.Now.Sub(snap.LastExitTime) < cdDur {
			remSec := int(cdDur.Seconds() - snap.Now.Sub(snap.LastExitTime).Seconds())
			cand.Reason = "SYMBOL_IN_COOLDOWN"
			cand.SubReason = fmt.Sprintf("COOLDOWN_ACTIVE_%ds_REMAINING", remSec)
			return cand
		}
	}

	if snap.BookUpdatedAt.IsZero() || (cfg.MaxBookStaleness > 0 && snap.Now.Sub(snap.BookUpdatedAt) > cfg.MaxBookStaleness) {
		cand.Reason = "STALE_BBO_FEED"
		return cand
	}
	if snap.MarkPriceUpdatedAt.IsZero() || (cfg.MaxMarkStaleness > 0 && snap.Now.Sub(snap.MarkPriceUpdatedAt) > cfg.MaxMarkStaleness) {
		cand.Reason = "STALE_MARK_PRICE_FEED"
		return cand
	}
	if snap.FundingUpdatedAt.IsZero() || (cfg.MaxDataStaleness > 0 && snap.Now.Sub(snap.FundingUpdatedAt) > cfg.MaxDataStaleness) {
		cand.Reason = "STALE_FUNDING_FEED"
		return cand
	}
	if !isFinitePositive(snap.BestBid) || !isFinitePositive(snap.BestAsk) || snap.BestAsk <= snap.BestBid {
		cand.Reason = "INVALID_BID_ASK"
		return cand
	}
	spreadPct := ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
	if !isFiniteFloat(spreadPct) || (cfg.MaxSpreadPct > 0 && spreadPct > cfg.MaxSpreadPct) {
		cand.Reason = "SPREAD_EXCEEDED"
		cand.SubReason = fmt.Sprintf("SPREAD_%.3f%%_MAX_%.3f%%", spreadPct, cfg.MaxSpreadPct)
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
	if cfg.DDThrottleTier2ThresholdPct > 0 && isFinitePositive(cfg.DDThrottleTier2RiskMult) && cfg.DDThrottleTier2RiskMult < 1.0 &&
		snap.PortfolioDrawdownPct >= cfg.DDThrottleTier2ThresholdPct {
		riskPct = riskPct.Mul(decimal.NewFromFloat(cfg.DDThrottleTier2RiskMult))
	} else if cfg.DDThrottleThresholdPct > 0 && isFinitePositive(cfg.DDThrottleRiskMult) && cfg.DDThrottleRiskMult < 1.0 &&
		snap.PortfolioDrawdownPct >= cfg.DDThrottleThresholdPct {
		riskPct = riskPct.Mul(decimal.NewFromFloat(cfg.DDThrottleRiskMult))
	}
	if isFinitePositive(cfg.MaxEffectiveRiskPerTradePct) {
		capDec := decimal.NewFromFloat(cfg.MaxEffectiveRiskPerTradePct)
		if riskPct.GreaterThan(capDec) {
			riskPct = capDec
		}
	}

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
