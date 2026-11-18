package validation

import (
	"math"
	"math/rand"
	"sort"

	"trml/internal/backtest/metrics"
)

// MonteCarloResult stores trade-sequence shuffling & bootstrap distribution statistics.
type MonteCarloResult struct {
	Iterations       int     `json:"iterations"`
	Seed             int64   `json:"seed"`
	TradesCount      int     `json:"trades_count"`
	MeanMaxDDPct     float64 `json:"mean_max_dd_pct"`
	P50MaxDDPct      float64 `json:"p50_max_dd_pct"`
	P95MaxDDPct      float64 `json:"p95_max_dd_pct"`
	P99MaxDDPct      float64 `json:"p99_max_dd_pct"`
	RiskOfRuin25Pct  float64 `json:"risk_of_ruin_25_pct"`
	RiskOfRuin50Pct  float64 `json:"risk_of_ruin_50_pct"`
	ExpectancyMean   float64 `json:"expectancy_mean"`
	ExpectancyCI95Lo float64 `json:"expectancy_ci95_low"`
	ExpectancyCI95Hi float64 `json:"expectancy_ci95_high"`
}

// RunMonteCarlo performs deterministic trade-order shuffling to estimate tail drawdown
// and bootstrap sampling to estimate expectancy confidence intervals.
func RunMonteCarlo(pnls []float64, initialEquity float64, iterations int, seed int64) MonteCarloResult {
	n := len(pnls)
	res := MonteCarloResult{
		Iterations:  iterations,
		Seed:        seed,
		TradesCount: n,
	}
	if n == 0 || initialEquity <= 0 || iterations <= 0 {
		return res
	}

	rng := rand.New(rand.NewSource(seed))
	shuffled := make([]float64, n)
	ddSamples := make([]float64, iterations)
	bootMeans := make([]float64, iterations)

	var sumDD, sumExp float64
	var ruin25Count, ruin50Count int

	for it := 0; it < iterations; it++ {
		copy(shuffled, pnls)
		rng.Shuffle(n, func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})

		eq := initialEquity
		peak := initialEquity
		maxDDPct := 0.0
		for _, p := range shuffled {
			eq += p
			if eq > peak {
				peak = eq
			}
			if peak > 0 {
				ddPct := ((peak - eq) / peak) * 100.0
				if ddPct > maxDDPct {
					maxDDPct = ddPct
				}
			}
		}
		ddSamples[it] = maxDDPct
		sumDD += maxDDPct
		if maxDDPct >= 25.0 {
			ruin25Count++
		}
		if maxDDPct >= 50.0 {
			ruin50Count++
		}

		// Bootstrap with replacement for mean expectancy CI
		bootSum := 0.0
		for j := 0; j < n; j++ {
			bootSum += pnls[rng.Intn(n)]
		}
		meanExp := bootSum / float64(n)
		bootMeans[it] = meanExp
		sumExp += meanExp
	}

	sort.Float64s(ddSamples)
	sort.Float64s(bootMeans)

	res.MeanMaxDDPct = sumDD / float64(iterations)
	res.P50MaxDDPct = percentileSorted(ddSamples, 0.50)
	res.P95MaxDDPct = percentileSorted(ddSamples, 0.95)
	res.P99MaxDDPct = percentileSorted(ddSamples, 0.99)
	res.RiskOfRuin25Pct = (float64(ruin25Count) / float64(iterations)) * 100.0
	res.RiskOfRuin50Pct = (float64(ruin50Count) / float64(iterations)) * 100.0
	res.ExpectancyMean = sumExp / float64(iterations)
	res.ExpectancyCI95Lo = percentileSorted(bootMeans, 0.025)
	res.ExpectancyCI95Hi = percentileSorted(bootMeans, 0.975)
	return res
}

// ExtractAggregatedPnLs collects setup-level net PnLs from a BacktestSummary.
func ExtractAggregatedPnLs(summary metrics.BacktestSummary) []float64 {
	out := make([]float64, 0, len(summary.Trades))
	for _, t := range summary.Trades {
		out = append(out, t.NetPnL)
	}
	return out
}

func percentileSorted(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Floor(q * float64(len(sorted)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
