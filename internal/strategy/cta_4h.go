package strategy

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker/protection"
	"trml/internal/config"
	"trml/internal/indicator"
	"trml/internal/model"
)

// CTAPod4hHistoryCap retains 600 completed 4h bars (100 days) so 4h EMA(200) converges cleanly (<0.04% seed residual).
const CTAPod4hHistoryCap = 600

// CTA4hCandidate represents the causal 4h evaluation outcome for Pod #4 (4h CTA Trend Following).
type CTA4hCandidate struct {
	Symbol      string
	Side        model.Side
	StrategyID  string
	Score       float64
	Close4h     float64
	ATR14_4h    float64
	EMA12_4h    float64
	EMA36_4h    float64
	EMA200_4h   float64
	FundingRate float64
	BarOpenUTC  time.Time
	Approved    bool
	Reason      string
	SubReason   string
}

// Is4HourCloseTimestamp reports whether t corresponds to an exact UTC 4-hour boundary
// (00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC, including Binance ...59.999 bar close).
func Is4HourCloseTimestamp(t time.Time) bool {
	if t.IsZero() {
		return false
	}
	norm := normalizeBoundaryTime(t).UTC()
	return norm.Hour()%4 == 0 && norm.Minute() == 0 && norm.Second() == 0
}

type intervalResampleBucket struct {
	candle   model.Candle
	count    int
	seenOpen map[int64]bool
}

func resampleCompletedIntervalStrict(candles []model.Candle, targetDur, expectedSubStep time.Duration) []model.Candle {
	if len(candles) == 0 || targetDur <= 0 {
		return nil
	}
	bucketSecs := int64(targetDur / time.Second)
	if bucketSecs <= 0 {
		return nil
	}

	requiredBars := 0
	if expectedSubStep > 0 && expectedSubStep < targetDur && (targetDur%expectedSubStep == 0) {
		requiredBars = int(targetDur / expectedSubStep)
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

	estBuckets := len(candles)/4 + 1
	if requiredBars > 4 {
		estBuckets = len(candles)/requiredBars + 1
	}
	buckets := make(map[int64]*intervalResampleBucket, estBuckets)
	keys := make([]int64, 0, estBuckets)

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

		bucketKey := (ref.UTC().Unix() / bucketSecs) * bucketSecs
		cClose := c.CloseTime
		if cClose.IsZero() && inferredStep > 0 {
			cClose = c.OpenTime.Add(inferredStep)
		}

		b, ok := buckets[bucketKey]
		if !ok {
			openT := time.Unix(bucketKey, 0).UTC()
			capSeen := 4
			if requiredBars > capSeen {
				capSeen = requiredBars
			}
			b = &intervalResampleBucket{
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
				seenOpen: make(map[int64]bool, capSeen),
			}
			if !c.OpenTime.IsZero() {
				b.seenOpen[c.OpenTime.UTC().UnixMilli()] = true
			}
			b.count = 1
			buckets[bucketKey] = b
			keys = append(keys, bucketKey)
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
		almostDone := b.candle.OpenTime.Add(targetDur - time.Minute)
		if !b.candle.CloseTime.IsZero() && b.candle.CloseTime.Before(almostDone) {
			continue
		}
		out = append(out, b.candle)
	}
	return out
}

func mergeCompletedIntervalCandle(existing, recentSubCandles []model.Candle, targetDur, expectedSubStep time.Duration, maxBars int) []model.Candle {
	completed := resampleCompletedIntervalStrict(recentSubCandles, targetDur, expectedSubStep)
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

// ResampleCompleted4HourlyStrict deterministically aggregates closed sub-4h candles into complete UTC 4h bars.
func ResampleCompleted4HourlyStrict(candles []model.Candle, expectedSubStep time.Duration) []model.Candle {
	return resampleCompletedIntervalStrict(candles, 4*time.Hour, expectedSubStep)
}

// MergeCompleted4HourlyCandle resamples recent sub-4h bars into completed UTC 4h candles and appends strictly newer bars onto existing, capped to maxBars.
func MergeCompleted4HourlyCandle(existing, recentSubCandles []model.Candle, expectedSubStep time.Duration, maxBars int) []model.Candle {
	return mergeCompletedIntervalCandle(existing, recentSubCandles, 4*time.Hour, expectedSubStep, maxBars)
}

func evaluatePodPreGuards(
	snap model.MarketSnapshot,
	hasActiveSymbol, isDailyBreached bool,
	cooldownMinutes int,
	cfg EvaluatorConfig,
) (ok bool, reason, subReason string) {
	if isDailyBreached {
		return false, "CIRCUIT_BREAKER_ACTIVE", ""
	}
	if hasActiveSymbol {
		return false, "SYMBOL_ALREADY_ACTIVE", ""
	}
	if cooldownMinutes > 0 && !snap.LastExitTime.IsZero() {
		cdDur := time.Duration(cooldownMinutes) * time.Minute
		if snap.Now.Sub(snap.LastExitTime) < cdDur {
			remSec := int(cdDur.Seconds() - snap.Now.Sub(snap.LastExitTime).Seconds())
			return false, "SYMBOL_IN_COOLDOWN", fmt.Sprintf("COOLDOWN_ACTIVE_%ds_REMAINING", remSec)
		}
	}
	if snap.BookUpdatedAt.IsZero() || (cfg.MaxBookStaleness > 0 && snap.Now.Sub(snap.BookUpdatedAt) > cfg.MaxBookStaleness) {
		return false, "STALE_BBO_FEED", ""
	}
	if snap.MarkPriceUpdatedAt.IsZero() || (cfg.MaxMarkStaleness > 0 && snap.Now.Sub(snap.MarkPriceUpdatedAt) > cfg.MaxMarkStaleness) {
		return false, "STALE_MARK_PRICE_FEED", ""
	}
	if snap.FundingUpdatedAt.IsZero() || (cfg.MaxDataStaleness > 0 && snap.Now.Sub(snap.FundingUpdatedAt) > cfg.MaxDataStaleness) {
		return false, "STALE_FUNDING_FEED", ""
	}
	if !isFinitePositive(snap.BestBid) || !isFinitePositive(snap.BestAsk) || snap.BestAsk <= snap.BestBid {
		return false, "INVALID_BID_ASK", ""
	}
	spreadPct := ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
	if !isFiniteFloat(spreadPct) || (cfg.MaxSpreadPct > 0 && spreadPct > cfg.MaxSpreadPct) {
		return false, "SPREAD_EXCEEDED", fmt.Sprintf("SPREAD_%.3f%%_MAX_%.3f%%", spreadPct, cfg.MaxSpreadPct)
	}
	return true, "", ""
}

func applyPortfolioDDThrottleAndCap(riskPct decimal.Decimal, snap model.MarketSnapshot, cfg EvaluatorConfig) decimal.Decimal {
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
	return riskPct
}

func calculateEMALastTwo(candles []model.Candle, period int) (prev, curr float64, ok bool) {
	if period <= 0 || len(candles) < period+1 {
		return 0, 0, false
	}
	sum := 0.0
	for i := 0; i < period; i++ {
		c := candles[i].Close
		if !isFinitePositive(c) {
			return 0, 0, false
		}
		sum += c
	}
	ema := sum / float64(period)
	if !isFinitePositive(ema) {
		return 0, 0, false
	}
	k := 2.0 / float64(period+1)
	lastIdx := len(candles) - 1
	for i := period; i <= lastIdx; i++ {
		if i == lastIdx {
			prev = ema
		}
		c := candles[i].Close
		if !isFinitePositive(c) {
			return 0, 0, false
		}
		ema = (c-ema)*k + ema
	}
	if !isFinitePositive(prev) || !isFinitePositive(ema) {
		return 0, 0, false
	}
	return prev, ema, true
}

// EvaluateCTA4hCandidate evaluates a closed 4h bar on a top-liquid symbol for Pod #4 (4h CTA Trend Following).
func EvaluateCTA4hCandidate(
	snap model.MarketSnapshot,
	candles4h []model.Candle,
	hasActiveSymbol bool,
	isDailyBreached bool,
	cfg EvaluatorConfig,
) CTA4hCandidate {
	cand := CTA4hCandidate{
		Symbol:      snap.Symbol,
		StrategyID:  StrategyIDCTA4hShort,
		FundingRate: snap.FundingRate,
	}
	if !cfg.CTAPod.IsEnabled() {
		cand.Reason = "CTA_POD_DISABLED"
		return cand
	}
	if ok, reason, sub := evaluatePodPreGuards(snap, hasActiveSymbol, isDailyBreached, cfg.CTAPod.CooldownMinutes, cfg); !ok {
		cand.Reason = reason
		cand.SubReason = sub
		return cand
	}

	sym4h := candles4h
	if len(sym4h) == 0 {
		sym4h = ResampleCompleted4HourlyStrict(snap.ClosedCandles, cfg.CandleDuration)
	}

	mode := strings.ToUpper(strings.TrimSpace(cfg.CTAPod.Mode))
	if mode == "" {
		mode = "DON20"
	}
	emaFast := cfg.CTAPod.EMAFast
	if emaFast <= 0 {
		emaFast = 12
	}
	emaSlow := cfg.CTAPod.EMASlow
	if emaSlow <= 0 {
		emaSlow = 36
	}
	emaTrend := cfg.CTAPod.EMATrend
	if emaTrend <= 0 {
		emaTrend = 200
	}
	donLB := cfg.CTAPod.DonchianLookback
	if mode == "DON40" && donLB <= 0 {
		donLB = 40
	} else if donLB <= 0 {
		donLB = 20
	}

	var minBars int
	if mode == "EMA_CROSS" {
		minBars = max(emaTrend+1, max(emaSlow+2, 42))
	} else {
		minBars = max(donLB+2, max(emaSlow+2, 42))
	}
	if len(sym4h) < minBars {
		cand.Reason = "INSUFFICIENT_4H_HISTORY"
		cand.SubReason = fmt.Sprintf("4H_BARS_%d_LT_%d", len(sym4h), minBars)
		return cand
	}

	last := len(sym4h) - 1
	cand.BarOpenUTC = sym4h[last].OpenTime
	cand.Close4h = sym4h[last].Close

	atr4h, okATR := indicator.CalculateATRSafe(sym4h, 14)
	if !okATR || !isFinitePositive(atr4h) {
		cand.Reason = "INVALID_4H_ATR"
		return cand
	}
	prevE12, e12, ok12 := calculateEMALastTwo(sym4h, emaFast)
	prevE36, e36, ok36 := calculateEMALastTwo(sym4h, emaSlow)
	if !ok12 || !ok36 {
		cand.Reason = "INVALID_4H_EMA"
		return cand
	}
	cand.ATR14_4h = atr4h
	cand.EMA12_4h = e12
	cand.EMA36_4h = e36

	checkFunding := func(isLong bool) (bool, string, string) {
		if !isFiniteFloat(snap.FundingRate) {
			return false, "INVALID_FUNDING_RATE", ""
		}
		if isLong && cfg.CTAPod.LongMaxFundingRate < 0.99 && snap.FundingRate > cfg.CTAPod.LongMaxFundingRate {
			return false, "CTA_LONG_FUNDING_CAPPED", fmt.Sprintf("FR_%.5f_GT_%.5f", snap.FundingRate, cfg.CTAPod.LongMaxFundingRate)
		}
		if !isLong && cfg.CTAPod.ShortMinFundingRate > -0.99 && snap.FundingRate < cfg.CTAPod.ShortMinFundingRate {
			return false, "CTA_SHORT_FUNDING_TOO_NEGATIVE", fmt.Sprintf("FR_%.5f_LT_%.5f", snap.FundingRate, cfg.CTAPod.ShortMinFundingRate)
		}
		return true, "", ""
	}

	c4 := sym4h[last].Close
	if mode == "EMA_CROSS" {
		e200, ok200 := indicator.CalculateEMASafe(sym4h, emaTrend)
		if !ok200 || !isFinitePositive(e200) {
			cand.Reason = "INVALID_4H_EMA200"
			return cand
		}
		cand.EMA200_4h = e200

		longCross := prevE12 <= prevE36 && e12 > e36 && c4 > e200
		shortCross := prevE12 >= prevE36 && e12 < e36 && c4 < e200

		if longCross {
			cand.Side = model.SideLong
			cand.StrategyID = StrategyIDCTA4hLong
			if !cfg.CTAPod.LongEnabled {
				cand.Reason = "CTA_LONG_DISABLED"
				return cand
			}
			if okFR, r, sub := checkFunding(true); !okFR {
				cand.Reason = r
				cand.SubReason = sub
				return cand
			}
			cand.Score = (e12 - e36) / atr4h
			cand.Approved = true
			cand.Reason = "SIGNAL_APPROVED"
			cand.SubReason = fmt.Sprintf("4H_EMA_CROSS_LONG_SCORE_%.2f", cand.Score)
			return cand
		}
		if shortCross {
			cand.Side = model.SideShort
			cand.StrategyID = StrategyIDCTA4hShort
			if !cfg.CTAPod.ShortEnabled {
				cand.Reason = "CTA_SHORT_DISABLED"
				return cand
			}
			if okFR, r, sub := checkFunding(false); !okFR {
				cand.Reason = r
				cand.SubReason = sub
				return cand
			}
			cand.Score = (e36 - e12) / atr4h
			cand.Approved = true
			cand.Reason = "SIGNAL_APPROVED"
			cand.SubReason = fmt.Sprintf("4H_EMA_CROSS_SHORT_SCORE_%.2f", cand.Score)
			return cand
		}
		cand.Reason = "CTA_NO_4H_EMA_CROSS"
		return cand
	}

	// Donchian mode ("DON20" | "DON40" | "DONCHIAN")
	prevHH := sym4h[last-donLB].High
	prevLL := sym4h[last-donLB].Low
	for k := last - donLB + 1; k < last; k++ {
		if sym4h[k].High > prevHH {
			prevHH = sym4h[k].High
		}
		if sym4h[k].Low < prevLL {
			prevLL = sym4h[k].Low
		}
	}
	if !isFinitePositive(prevHH) || !isFinitePositive(prevLL) {
		cand.Reason = "INVALID_4H_DONCHIAN"
		return cand
	}

	if c4 > prevHH && e12 > e36 {
		cand.Side = model.SideLong
		cand.StrategyID = StrategyIDCTA4hLong
		if !cfg.CTAPod.LongEnabled {
			cand.Reason = "CTA_LONG_DISABLED"
			return cand
		}
		if okFR, r, sub := checkFunding(true); !okFR {
			cand.Reason = r
			cand.SubReason = sub
			return cand
		}
		cand.Score = (c4 - prevHH) / atr4h
		cand.Approved = true
		cand.Reason = "SIGNAL_APPROVED"
		cand.SubReason = fmt.Sprintf("4H_DON%d_LONG_SCORE_%.2f", donLB, cand.Score)
		return cand
	}
	if c4 < prevLL && e12 < e36 {
		cand.Side = model.SideShort
		cand.StrategyID = StrategyIDCTA4hShort
		if !cfg.CTAPod.ShortEnabled {
			cand.Reason = "CTA_SHORT_DISABLED"
			return cand
		}
		if okFR, r, sub := checkFunding(false); !okFR {
			cand.Reason = r
			cand.SubReason = sub
			return cand
		}
		cand.Score = (prevLL - c4) / atr4h
		cand.Approved = true
		cand.Reason = "SIGNAL_APPROVED"
		cand.SubReason = fmt.Sprintf("4H_DON%d_SHORT_SCORE_%.2f", donLB, cand.Score)
		return cand
	}

	cand.Reason = "CTA_NO_4H_DONCHIAN_BREAK"
	return cand
}

// BuildCTA4hOrderIntent builds a defensive OrderIntent for an approved Pod #4 candidate using 4h ATR,
// portfolio DD throttling, optional disabled hard TP, and the standard leverage liquidation safety guard.
func BuildCTA4hOrderIntent(
	cand CTA4hCandidate,
	snap model.MarketSnapshot,
	effectiveEquity, availBal decimal.Decimal,
	cfg EvaluatorConfig,
) (model.OrderIntent, bool, string) {
	if !cand.Approved {
		return model.OrderIntent{}, false, cand.Reason
	}

	entryType := strings.ToUpper(strings.TrimSpace(cfg.CTAPod.EntryOrderType))
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

	riskPct := cfg.CTAPod.ShortRiskPct
	if cand.Side == model.SideLong {
		riskPct = cfg.CTAPod.LongRiskPct
	}
	riskPct = applyPortfolioDDThrottleAndCap(riskPct, snap, cfg)

	slMult := cfg.CTAPod.SLATRMultiplier
	if slMult <= 0 {
		slMult = 3.0
	}
	tpMult := cfg.CTAPod.TPATRMultiplier
	disableTP := tpMult <= 0

	sizingCfg := SizingConfig{
		RiskPerTradePct:                riskPct,
		MaxSlotMarginFraction:          cfg.Sizing.MaxSlotMarginFraction,
		AvailableBalanceBufferFraction: cfg.Sizing.AvailableBalanceBufferFraction,
		AccountLeverage:                cfg.Sizing.AccountLeverage,
		SLMultiplier:                   slMult,
		TPMultiplier:                   tpMult,
		MinSLDistancePct:               cfg.CTAPod.MinSLDistancePct,
		MakerFeeRate:                   cfg.Sizing.MakerFeeRate,
		TakerFeeRate:                   cfg.Sizing.TakerFeeRate,
		SlippageRate:                   cfg.Sizing.SlippageRate,
		EntryOrderType:                 entryType,
		DisablePartialTP:               true,
		DisableTakeProfit:              disableTP,
	}

	intent, ok := BuildSafeDecimalOrderIntent(snap, effectiveEquity, availBal, cand.Side, rawEntry, cand.ATR14_4h, sizingCfg)
	if !ok {
		return model.OrderIntent{}, false, "SIZING_FAILED"
	}
	intent.StrategyID = NormalizeStrategyID(cand.StrategyID)
	intent.EntryOrderType = entryType
	return intent, true, "SIGNAL_APPROVED"
}

// BuildCTAPodExitProfiles constructs per-strategy Chandelier exit overrides for Pod #4.
func BuildCTAPodExitProfiles(cfg *config.Config) map[string]protection.StrategyExitProfile {
	if cfg == nil || !cfg.CTAPod.IsEnabled() {
		return nil
	}
	prof := protection.StrategyExitProfile{
		HasOverride:            true,
		PartialTPEnabled:       false,
		Tier1BEEnabled:         false,
		Tier2ProfitLockEnabled: false,
		TrailingEnabled:        cfg.CTAPod.TrailingEnabled,
		ChandelierTrailing:     cfg.CTAPod.ChandelierTrailing,
		TrailingActivateATR:    cfg.CTAPod.TrailingActivateATR,
		TrailingDistanceATR:    cfg.CTAPod.TrailingDistanceATR,
		StaleLossExitDuration:  cfg.CTAPod.StaleLossDuration,
		MaxHoldingDuration:     cfg.CTAPod.MaxHoldingDuration,
	}
	return map[string]protection.StrategyExitProfile{
		StrategyIDCTA4hShort: prof,
		StrategyIDCTA4hLong:  prof,
	}
}

// BuildAllPodExitProfiles merges per-strategy exit overrides across all enabled auxiliary pods (MajorPod + CTAPod).
func BuildAllPodExitProfiles(cfg *config.Config) map[string]protection.StrategyExitProfile {
	maj := BuildMajorPodExitProfiles(cfg)
	cta := BuildCTAPodExitProfiles(cfg)
	if len(maj) == 0 && len(cta) == 0 {
		return nil
	}
	out := make(map[string]protection.StrategyExitProfile, len(maj)+len(cta))
	for k, v := range maj {
		out[k] = v
	}
	for k, v := range cta {
		out[k] = v
	}
	return out
}
