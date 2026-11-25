package strategy

import (
	"math"
	"sort"
	"time"

	"trml/internal/indicator"
	"trml/internal/model"
)

// MacroRegime labels the 1h-horizon macro market environment derived from BTC candles.
type MacroRegime string

const (
	MacroRegimeUnknown   MacroRegime = "UNKNOWN"
	MacroRegimeTrendUp   MacroRegime = "TREND_UP"
	MacroRegimeTrendDown MacroRegime = "TREND_DOWN"
	MacroRegimeRange     MacroRegime = "RANGE"
	MacroRegimeVolShock  MacroRegime = "VOL_SHOCK"
)

// MacroRegimeMetrics reports macro regime classification and underlying quantitative features.
type MacroRegimeMetrics struct {
	Regime           MacroRegime
	EfficiencyRatio  float64
	VolRatio1h       float64
	Return24hPct     float64
	HourlyBars       int
}

// ResampleToHourly deterministically aggregates sub-hourly or hourly closed candles into UTC 1h bars.
func ResampleToHourly(candles []model.Candle) []model.Candle {
	if len(candles) == 0 {
		return nil
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

	buckets := make(map[int64]model.Candle, len(candles)/4+1)
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
		existing, ok := buckets[hourKey]
		if !ok {
			openT := time.Unix(hourKey, 0).UTC()
			buckets[hourKey] = model.Candle{
				OpenTime:          openT,
				CloseTime:         cClose,
				Open:              c.Open,
				High:              c.High,
				Low:               c.Low,
				Close:             c.Close,
				VolumeBase:        volBase,
				QuoteVolumeUSDT:   quoteVol,
				TakerBuyQuoteUSDT: takerQuote,
			}
			keys = append(keys, hourKey)
			continue
		}
		if c.High > existing.High {
			existing.High = c.High
		}
		if c.Low < existing.Low {
			existing.Low = c.Low
		}
		existing.Close = c.Close
		if cClose.After(existing.CloseTime) {
			existing.CloseTime = cClose
		}
		existing.VolumeBase += volBase
		existing.QuoteVolumeUSDT += quoteVol
		existing.TakerBuyQuoteUSDT += takerQuote
		buckets[hourKey] = existing
	}

	if len(keys) == 0 {
		return nil
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]model.Candle, 0, len(keys))
	for _, k := range keys {
		out = append(out, buckets[k])
	}
	return out
}

// CompletedHourlyCandles resamples candles to UTC 1h bars and drops an incomplete trailing hour.
func CompletedHourlyCandles(candles []model.Candle) []model.Candle {
	hourly := ResampleToHourly(candles)
	if len(hourly) >= 1 {
		lastH := hourly[len(hourly)-1]
		hourAlmostDone := lastH.OpenTime.Add(time.Hour - time.Minute)
		if !lastH.CloseTime.IsZero() && lastH.CloseTime.Before(hourAlmostDone) {
			hourly = hourly[:len(hourly)-1]
		}
	}
	return hourly
}

// ClassifyMacroRegime computes 24h Kaufman Efficiency Ratio and 1h ATR expansion ratio on BTC
// to separate Trending, Range/Mean-Reverting, and Volatility-Shock macro environments.
func ClassifyMacroRegime(btcCandles []model.Candle, trendERThreshold, volShockRatio float64) MacroRegimeMetrics {
	if !isFinitePositive(trendERThreshold) {
		trendERThreshold = 0.30
	}
	if !isFinitePositive(volShockRatio) {
		volShockRatio = 1.50
	}
	hourly := CompletedHourlyCandles(btcCandles)
	n := len(hourly)
	metrics := MacroRegimeMetrics{
		Regime:     MacroRegimeUnknown,
		HourlyBars: n,
	}
	if n < 26 {
		return metrics
	}

	const erWindow = 24
	startIdx := n - 1 - erWindow
	startClose := hourly[startIdx].Close
	endClose := hourly[n-1].Close
	if !isFinitePositive(startClose) || !isFinitePositive(endClose) {
		return metrics
	}

	pathLen := 0.0
	for i := startIdx + 1; i < n; i++ {
		if !isFiniteFloat(hourly[i].Close) || !isFiniteFloat(hourly[i-1].Close) {
			return metrics
		}
		pathLen += math.Abs(hourly[i].Close - hourly[i-1].Close)
	}
	if pathLen > 0 && isFiniteFloat(pathLen) {
		er := math.Abs(endClose-startClose) / pathLen
		if !isFiniteFloat(er) {
			return metrics
		}
		metrics.EfficiencyRatio = er
	}
	ret24h := ((endClose - startClose) / startClose) * 100.0
	if !isFiniteFloat(ret24h) {
		return metrics
	}
	metrics.Return24hPct = ret24h

	atrShort := indicator.CalculateATR(hourly, 14)
	longPeriod := 48
	if n < longPeriod+1 {
		longPeriod = 24
	}
	atrLong := indicator.CalculateATR(hourly, longPeriod)
	if !isFiniteFloat(atrShort) || !isFiniteFloat(atrLong) {
		return metrics
	}
	if atrShort > 0 && atrLong > 0 {
		volRatio := atrShort / atrLong
		if !isFiniteFloat(volRatio) {
			return metrics
		}
		metrics.VolRatio1h = volRatio
	}

	switch {
	case metrics.VolRatio1h >= volShockRatio:
		metrics.Regime = MacroRegimeVolShock
	case metrics.EfficiencyRatio >= trendERThreshold && metrics.Return24hPct >= 0:
		metrics.Regime = MacroRegimeTrendUp
	case metrics.EfficiencyRatio >= trendERThreshold && metrics.Return24hPct < 0:
		metrics.Regime = MacroRegimeTrendDown
	default:
		metrics.Regime = MacroRegimeRange
	}
	return metrics
}
