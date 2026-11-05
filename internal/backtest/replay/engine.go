package replay

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
	"trml/internal/indicator"
	"trml/internal/model"
	"trml/internal/strategy"
)

type ReplayConfig struct {
	WarmupBars   int     // jumlah bar awal untuk warmup indikator (default 200)
	SpreadPct    float64 // default estimasi spread (0.02% = 0.0002)
	FundingRate  float64 // default estimasi funding (0.01% = 0.0001)
	BenchmarkSym string  // default "BTCUSDT"
}

type lastExitInfo struct {
	time   time.Time
	reason string
}

type ReplayEngine struct {
	cfg          ReplayConfig
	evaluatorCfg strategy.EvaluatorConfig
	broker       *simbroker.SimBroker
	candleMap    map[string][]data.HistoricalCandle
	filters      map[string]model.ExchangeFilter
	lastExits    map[string]lastExitInfo

	// Signal observability
	TotalSignalsEvaluated int64
	SignalsApproved       int64
	SignalsRejected       int64
	RejectReasons         map[string]int64
}

func NewReplayEngine(
	cfg ReplayConfig,
	evaluatorCfg strategy.EvaluatorConfig,
	broker *simbroker.SimBroker,
	candleMap map[string][]data.HistoricalCandle,
	filters map[string]model.ExchangeFilter,
) *ReplayEngine {
	if cfg.WarmupBars <= 0 {
		cfg.WarmupBars = 200
	}
	if cfg.SpreadPct <= 0 {
		cfg.SpreadPct = 0.0002
	}
	if cfg.BenchmarkSym == "" {
		cfg.BenchmarkSym = "BTCUSDT"
	}
	if evaluatorCfg.MaxDailyDrawdownPct >= 0 {
		evaluatorCfg.MaxDailyDrawdownPct = -0.05
	}

	return &ReplayEngine{
		cfg:           cfg,
		evaluatorCfg:  evaluatorCfg,
		broker:        broker,
		candleMap:     candleMap,
		filters:       filters,
		lastExits:     make(map[string]lastExitInfo),
		RejectReasons: make(map[string]int64),
	}
}

// Run mengeksekusi replay tersinkronisasi bar-demi-bar lintas seluruh simbol.
func (r *ReplayEngine) Run(ctx context.Context) error {
	// 1. Kumpulkan seluruh timestamp unik candle close
	timeMap := make(map[int64]time.Time)
	for _, candles := range r.candleMap {
		for _, c := range candles {
			timeMap[c.CloseTime.UnixMilli()] = c.CloseTime
		}
	}

	if len(timeMap) == 0 {
		return fmt.Errorf("no candle data available for replay")
	}

	timeline := make([]time.Time, 0, len(timeMap))
	for _, t := range timeMap {
		timeline = append(timeline, t)
	}
	sort.Slice(timeline, func(i, j int) bool {
		return timeline[i].Before(timeline[j])
	})

	// Indeks posisi candle per simbol untuk akses O(1)
	candleIndices := make(map[string]int)
	rollingCandles := make(map[string][]model.Candle)
	rollingDeltas := make(map[string][]float64)

	// Inisialisasi filter bursa pada simbroker
	for sym, f := range r.filters {
		r.broker.SetExchangeFilter(sym, f)
	}

	// 2. Loop Utama Waktu Simulasi (Bar-by-Bar Synchronized Step)
	for barIdx, simTime := range timeline {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// A. Update SimBroker pada setiap candle aktif di timestamp ini
		// dan tambahkan ke rolling buffers
		for sym, candles := range r.candleMap {
			idx := candleIndices[sym]
			if idx < len(candles) && candles[idx].CloseTime.Equal(simTime) {
				c := candles[idx]

				// Simbroker mengevaluasi fill pending order dan exit ladder posisi aktif
				r.broker.ProcessCandle(c, int64(barIdx))

				// Update rolling buffer
				mCandle := c.ToModelCandle()
				rollingCandles[sym] = append(rollingCandles[sym], mCandle)
				rollingDeltas[sym] = append(rollingDeltas[sym], c.BarDeltaUSDT)

				// Batasi panjang rolling buffer agar hemat memori (maks 300 bar)
				if len(rollingCandles[sym]) > 300 {
					rollingCandles[sym] = rollingCandles[sym][len(rollingCandles[sym])-300:]
				}
				if len(rollingDeltas[sym]) > 300 {
					rollingDeltas[sym] = rollingDeltas[sym][len(rollingDeltas[sym])-300:]
				}

				candleIndices[sym]++
			}
		}

		// Update data last exit dari closed trades broker
		r.broker.Mu.Lock()
		for _, tr := range r.broker.ClosedTrades {
			r.lastExits[tr.Symbol] = lastExitInfo{
				time:   tr.ExitTime,
				reason: tr.ExitReason,
			}
		}
		r.broker.Mu.Unlock()

		// Warmup period: lewati evaluasi sinyal jika belum mencapai batas warmup bar
		if barIdx < r.cfg.WarmupBars {
			continue
		}

		// B. Siapkan BTC Macro Snapshot
		var btcSnap *model.MarketSnapshot
		if r.evaluatorCfg.BTCGateEnabled {
			if btcCandles, ok := rollingCandles[r.cfg.BenchmarkSym]; ok && len(btcCandles) >= 30 {
				lastBTC := btcCandles[len(btcCandles)-1]
				vwapBTC := indicator.CalculateSessionVWAP(btcCandles)
				bSnap := model.MarketSnapshot{
					Symbol:              r.cfg.BenchmarkSym,
					Now:                 simTime,
					MarkPrice:           lastBTC.Close,
					BestBid:             lastBTC.Close * (1.0 - r.cfg.SpreadPct/2.0),
					BestAsk:             lastBTC.Close * (1.0 + r.cfg.SpreadPct/2.0),
					CurrentSessionVWAP:  vwapBTC,
					ClosedCandles:       btcCandles,
					ClosedBarDeltasUSDT: rollingDeltas[r.cfg.BenchmarkSym],
				}
				btcSnap = &bSnap
			}
		}

		// C. Evaluasi Sinyal per Simbol
		activeSlots := r.broker.GetActiveSlotCount()
		availBal := r.broker.GetAvailableBalanceUSDT()
		totEq := r.broker.GetEffectiveTotalEquityUSDT()
		isDailyBreached := r.broker.IsCircuitBreakerTripped() || (r.broker.GetDailyTotalDrawdownPct() <= r.evaluatorCfg.MaxDailyDrawdownPct)

		for sym, cList := range rollingCandles {
			// Lewati benchmark symbol jika bukan target trading
			if sym == r.cfg.BenchmarkSym && !r.isSymbolTarget(sym) {
				continue
			}

			// Syarat data bar mencukupi untuk EMA200
			if len(cList) < r.evaluatorCfg.EMAPeriod {
				continue
			}

			hasActive := r.broker.HasActiveOrderOrPosition(sym)
			lastC := cList[len(cList)-1]
			sessionVWAP := indicator.CalculateSessionVWAP(cList)
			if sessionVWAP <= 0 || math.IsNaN(sessionVWAP) {
				continue
			}

			filter, ok := r.filters[sym]
			if !ok {
				filter = model.ExchangeFilter{
					TickSize:    decimal.NewFromFloat(0.01),
					StepSize:    decimal.NewFromFloat(0.1),
					MinNotional: decimal.NewFromFloat(5.0),
				}
			}

			exitInfo := r.lastExits[sym]

			snap := model.MarketSnapshot{
				Symbol:              sym,
				Filter:              filter,
				Now:                 simTime,
				BestBid:             lastC.Close * (1.0 - r.cfg.SpreadPct/2.0),
				BestAsk:             lastC.Close * (1.0 + r.cfg.SpreadPct/2.0),
				BookUpdatedAt:       simTime,
				MarkPrice:           lastC.Close,
				MarkPriceUpdatedAt:  simTime,
				FundingRate:         r.cfg.FundingRate,
				FundingUpdatedAt:    simTime,
				CurrentSessionVWAP:  sessionVWAP,
				ClosedCandles:       cList,
				ClosedBarDeltasUSDT: rollingDeltas[sym],
				LastExitTime:        exitInfo.time,
				LastExitReason:      exitInfo.reason,
			}

			r.TotalSignalsEvaluated++

			intent, approved, reason, _ := strategy.EvaluateSignal(
				snap,
				btcSnap,
				activeSlots,
				isDailyBreached,
				hasActive,
				totEq,
				availBal,
				r.evaluatorCfg,
			)

			if approved {
				r.SignalsApproved++
				_, _ = r.broker.SubmitLimitEntry(intent)
				// Perbarui active slot count seketika agar tidak overflow
				activeSlots = r.broker.GetActiveSlotCount()
			} else {
				r.SignalsRejected++
				r.RejectReasons[reason]++
			}
		}
	}

	return nil
}

func (r *ReplayEngine) isSymbolTarget(symbol string) bool {
	// Simbol target trading valid jika terdaftar di filters
	_, ok := r.filters[symbol]
	return ok
}
