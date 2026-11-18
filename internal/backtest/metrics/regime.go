package metrics

import (
	"sort"

	"trml/internal/backtest/data"
	"trml/internal/indicator"
	"trml/internal/model"
)

const (
	RegimeBTCBull    = "BTC_BULL"
	RegimeBTCBear    = "BTC_BEAR"
	RegimeBTCChopVol = "BTC_CHOP_HIGH_VOL"
	RegimeUnknown    = "BTC_UNCLASSIFIED"
)

// ClassifyBTCRegimes stratifies trade outcomes by benchmark BTC market regime at each trade's entry time.
// Uses only closed benchmark bars with CloseTime <= trade.EntryTime (zero look-ahead).
func ClassifyBTCRegimes(trades []AggregatedTrade, btcCandles []data.HistoricalCandle) []SegmentStats {
	if len(trades) == 0 || len(btcCandles) == 0 {
		return nil
	}

	sortedBTC := make([]data.HistoricalCandle, len(btcCandles))
	copy(sortedBTC, btcCandles)
	sort.Slice(sortedBTC, func(i, j int) bool {
		return sortedBTC[i].CloseTime.Before(sortedBTC[j].CloseTime)
	})

	modelBTC := make([]model.Candle, len(sortedBTC))
	for i := range sortedBTC {
		modelBTC[i] = sortedBTC[i].ToModelCandle()
	}

	bull := newSegment(RegimeBTCBull)
	bear := newSegment(RegimeBTCBear)
	chop := newSegment(RegimeBTCChopVol)
	unknown := newSegment(RegimeUnknown)

	for _, t := range trades {
		idx := sort.Search(len(sortedBTC), func(i int) bool {
			return sortedBTC[i].CloseTime.After(t.EntryTime)
		})
		if idx < 30 {
			unknown.addTrade(t)
			continue
		}
		start := idx - 250
		if start < 0 {
			start = 0
		}
		window := modelBTC[start:idx]
		regime := detectWindowRegime(window)
		switch regime {
		case RegimeBTCBull:
			bull.addTrade(t)
		case RegimeBTCBear:
			bear.addTrade(t)
		case RegimeBTCChopVol:
			chop.addTrade(t)
		default:
			unknown.addTrade(t)
		}
	}

	out := []SegmentStats{
		bull.finalize(),
		bear.finalize(),
		chop.finalize(),
	}
	if unknown.stat.TotalTrades > 0 {
		out = append(out, unknown.finalize())
	}
	return out
}

func detectWindowRegime(window []model.Candle) string {
	n := len(window)
	if n < 30 {
		return RegimeUnknown
	}
	lastClose := window[n-1].Close
	if lastClose <= 0 {
		return RegimeUnknown
	}

	emaPeriod := 200
	if n < emaPeriod {
		emaPeriod = n / 2
	}
	ema := indicator.CalculateEMA(window, emaPeriod)
	atr := indicator.CalculateATR(window, 14)
	vwap := indicator.CalculateSessionVWAP(window)
	natrPct := (atr / lastClose) * 100.0

	// High volatility chop or hugging EMA200 across recent bars.
	emaDistPct := 0.0
	if ema > 0 {
		emaDistPct = ((lastClose - ema) / ema) * 100.0
	}
	if natrPct >= 1.20 || (ema > 0 && emaDistPct >= -0.15 && emaDistPct <= 0.15) {
		return RegimeBTCChopVol
	}

	// Count recent side flips across EMA in last 6 bars.
	if n >= 8 && ema > 0 {
		flips := 0
		prevAbove := window[n-7].Close >= ema
		for i := n - 6; i < n; i++ {
			curAbove := window[i].Close >= ema
			if curAbove != prevAbove {
				flips++
				prevAbove = curAbove
			}
		}
		if flips >= 2 {
			return RegimeBTCChopVol
		}
	}

	if lastClose > ema && (vwap <= 0 || lastClose >= vwap) {
		return RegimeBTCBull
	}
	return RegimeBTCBear
}
