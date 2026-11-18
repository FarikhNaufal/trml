package replay

import (
	"math"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/indicator"
	"trml/internal/model"
	"trml/internal/strategy"
)

// OIMode constants control Open Interest lookup strictness during signal replay.
const (
	OIModeAuto   = "auto"
	OIModeStrict = "strict"
	OIModeOff    = "off"
)

// NormalizeOIMode maps empty/unknown OI modes safely to OIModeAuto.
func NormalizeOIMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case OIModeStrict:
		return OIModeStrict
	case OIModeOff:
		return OIModeOff
	default:
		return OIModeAuto
	}
}

// CalcRollingBufferCap returns a bar-history buffer capacity large enough for a full
// 24h volume window plus warmup bars, and never less than 1000 bars.
func CalcRollingBufferCap(candleDur time.Duration, warmupBars int) int {
	bars24h := 288
	if candleDur > 0 {
		bars24h = int((24 * time.Hour) / candleDur)
	}
	if bars24h <= 0 {
		bars24h = 288
	}
	capNeeded := bars24h + warmupBars
	if capNeeded < 1000 {
		capNeeded = 1000
	}
	return capNeeded
}

// BuildBTCMacroSnapshot constructs the benchmark macro snapshot used by BTCGate.
func BuildBTCMacroSnapshot(
	benchmarkSym string,
	simTime time.Time,
	btcCandles []model.Candle,
	btcDeltas []float64,
	baseSpreadPct float64,
) *model.MarketSnapshot {
	if len(btcCandles) < 30 {
		return nil
	}
	lastBTC := btcCandles[len(btcCandles)-1]
	vwapBTC := indicator.CalculateSessionVWAP(btcCandles)
	btcSpread := calculateDynamicSpread(btcCandles, lastBTC.Close, baseSpreadPct)
	snap := model.MarketSnapshot{
		Symbol:              benchmarkSym,
		Now:                 simTime,
		MarkPrice:           lastBTC.Close,
		BestBid:             lastBTC.Close * (1.0 - btcSpread/2.0),
		BestAsk:             lastBTC.Close * (1.0 + btcSpread/2.0),
		CurrentSessionVWAP:  vwapBTC,
		ClosedCandles:       btcCandles,
		ClosedBarDeltasUSDT: btcDeltas,
	}
	return &snap
}

type oiResolutionStatus uint8

const (
	oiStatusDisabled oiResolutionStatus = iota
	oiStatusCovered
	oiStatusFallback
	oiStatusMissing
)

// BuildSymbolSnapshot builds the strategy evaluation MarketSnapshot for sym at simTime
// and returns an adjusted EvaluatorConfig along with OI resolution status.
func BuildSymbolSnapshot(
	sym string,
	simTime time.Time,
	cList []model.Candle,
	deltas []float64,
	filter model.ExchangeFilter,
	exitInfo lastExitInfo,
	baseSpreadPct float64,
	fundingRate float64,
	oiMode string,
	oiMgr *data.MetricsOIDownloader,
	evalCfg strategy.EvaluatorConfig,
) (model.MarketSnapshot, strategy.EvaluatorConfig, oiResolutionStatus, bool) {
	if len(cList) == 0 {
		return model.MarketSnapshot{}, evalCfg, oiStatusDisabled, false
	}
	lastC := cList[len(cList)-1]
	sessionVWAP := indicator.CalculateSessionVWAP(cList)
	if sessionVWAP <= 0 || math.IsNaN(sessionVWAP) {
		return model.MarketSnapshot{}, evalCfg, oiStatusDisabled, false
	}

	if filter.TickSize.LessThanOrEqual(decimal.Zero) {
		filter = model.ExchangeFilter{
			TickSize:    decimal.NewFromFloat(0.01),
			StepSize:    decimal.NewFromFloat(0.1),
			MinNotional: decimal.NewFromFloat(5.0),
		}
	}

	dynSpread := calculateDynamicSpread(cList, lastC.Close, baseSpreadPct)
	snap := model.MarketSnapshot{
		Symbol:              sym,
		Filter:              filter,
		Now:                 simTime,
		BestBid:             lastC.Close * (1.0 - dynSpread/2.0),
		BestAsk:             lastC.Close * (1.0 + dynSpread/2.0),
		BookUpdatedAt:       simTime,
		MarkPrice:           lastC.Close,
		MarkPriceUpdatedAt:  simTime,
		FundingRate:         fundingRate,
		FundingUpdatedAt:    simTime,
		CurrentSessionVWAP:  sessionVWAP,
		ClosedCandles:       cList,
		ClosedBarDeltasUSDT: deltas,
		LastExitTime:        exitInfo.time,
		LastExitReason:      exitInfo.reason,
	}

	mode := NormalizeOIMode(oiMode)
	adjCfg := evalCfg
	status := oiStatusDisabled

	switch mode {
	case OIModeOff:
		adjCfg.DisableOIFilter = true
		status = oiStatusDisabled
	case OIModeStrict:
		adjCfg.DisableOIFilter = false
		if oiMgr != nil {
			if prevOI, currOI, prevT, currT, ok := oiMgr.LookupOpenInterestStrict(
				sym, simTime, evalCfg.MinOIInterval, evalCfg.MaxOIInterval, evalCfg.MaxOIStaleness,
			); ok {
				snap.PrevOpenInterest = prevOI
				snap.CurrOpenInterest = currOI
				snap.PrevOIUpdatedAt = prevT
				snap.CurrOIUpdatedAt = currT
				status = oiStatusCovered
			} else {
				status = oiStatusMissing
			}
		} else {
			status = oiStatusMissing
		}
	default: // OIModeAuto
		if oiMgr != nil {
			if prevOI, currOI, prevT, currT, ok := oiMgr.LookupOpenInterestStrict(
				sym, simTime, evalCfg.MinOIInterval, evalCfg.MaxOIInterval, evalCfg.MaxOIStaleness,
			); ok {
				snap.PrevOpenInterest = prevOI
				snap.CurrOpenInterest = currOI
				snap.PrevOIUpdatedAt = prevT
				snap.CurrOIUpdatedAt = currT
				status = oiStatusCovered
			} else {
				adjCfg.DisableOIFilter = true
				status = oiStatusFallback
			}
		} else {
			adjCfg.DisableOIFilter = true
			status = oiStatusFallback
		}
	}

	return snap, adjCfg, status, true
}

// calculateDynamicSpread estimates bid-ask spread dynamically from short-window NATR
// combined with intrabar high-low range stress to capture wick volatility slippage.
func calculateDynamicSpread(cList []model.Candle, lastClose float64, fallbackSpread float64) float64 {
	if len(cList) < 14 || lastClose <= 0 {
		if fallbackSpread > 0 {
			return fallbackSpread
		}
		return 0.0002
	}
	atr := indicator.CalculateATR(cList, 14)
	natrPct := atr / lastClose
	lastBar := cList[len(cList)-1]
	wickRangePct := 0.0
	if lastBar.High > lastBar.Low {
		wickRangePct = (lastBar.High - lastBar.Low) / lastClose
	}
	return math.Max(0.0001, math.Min(0.0008, 0.00012+natrPct*0.02+wickRangePct*0.005))
}

