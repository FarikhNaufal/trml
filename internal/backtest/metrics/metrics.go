package metrics

import (
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"trml/internal/backtest/simbroker"
)

type BacktestSummary struct {
	TotalTrades        int            `json:"total_trades"`
	WinsCount          int            `json:"wins_count"`
	FullWinsCount      int            `json:"full_wins_count"`
	PartialWinsCount   int            `json:"partial_wins_count"`
	BreakEvenCount     int            `json:"break_even_count"`
	LossesCount        int            `json:"losses_count"`
	WinRatePct         float64        `json:"win_rate_pct"`
	AdjustedWinRatePct float64        `json:"adjusted_win_rate_pct"` // Win / (Win + Loss) mengabaikan BE scratch
	AvgWinUSDT         float64        `json:"avg_win_usdt"`
	AvgLossUSDT        float64        `json:"avg_loss_usdt"`
	PayoffRatio        float64        `json:"payoff_ratio"` // AvgWin / AvgLoss
	BreakEvenWinRate   float64        `json:"break_even_win_rate"`
	ProfitFactor       float64        `json:"profit_factor"` // Gross Profit / Gross Loss
	TotalNetPnL        float64        `json:"total_net_pnl"`
	TotalGrossPnL      float64        `json:"total_gross_pnl"`
	TotalFees          float64        `json:"total_fees"`
	TotalFunding       float64        `json:"total_funding"`
	FeeDragPct         float64        `json:"fee_drag_pct"` // TotalFees / GrossProfit
	ExpectancyUSDT     float64        `json:"expectancy_usdt"`
	CI95LowerUSDT      float64        `json:"ci95_lower_usdt"`
	CI95UpperUSDT      float64        `json:"ci95_upper_usdt"`
	MaxDrawdownPct     float64        `json:"max_drawdown_pct"`
	MaxDrawdownUSDT    float64        `json:"max_drawdown_usdt"`
	MaxLosingStreak    int               `json:"max_losing_streak"`
	ExitDistribution   map[string]int     `json:"exit_distribution"`
	SampleWarning      bool               `json:"sample_warning"`
	Trades             []AggregatedTrade  `json:"trades,omitempty"`
	Segmentation       SegmentationReport `json:"segmentation,omitempty"`
}

// AggregatedTrade merepresentasikan 1 trade setup utuh (menggabungkan leg partial TP1 dan leg runner).
type AggregatedTrade struct {
	SetupID     string    `json:"setup_id"`
	Symbol      string    `json:"symbol"`
	Side        string    `json:"side"`
	EntryPrice  float64   `json:"entry_price"`
	ATRAtEntry  float64   `json:"atr_at_entry"`
	NATRPct     float64   `json:"natr_pct"`
	EntryTime   time.Time `json:"entry_time"`
	ExitTime    time.Time `json:"exit_time"`
	GrossPnL    float64   `json:"gross_pnl"`
	NetPnL      float64   `json:"net_pnl"`
	TotalFees   float64   `json:"total_fees"`
	FundingPaid float64   `json:"funding_paid"`
	ExitReasons []string  `json:"exit_reasons"`
	HoldingBars int       `json:"holding_bars"`
}

// CalculateMetrics mengagregasikan seluruh record trade mentah menjadi metrik performa statistik standar.
func CalculateMetrics(rawTrades []simbroker.SimTradeRecord, initialEquity float64) BacktestSummary {
	if len(rawTrades) == 0 {
		return BacktestSummary{
			SampleWarning:    true,
			ExitDistribution: make(map[string]int),
		}
	}

	// 1. Agregasikan leg partial TP1 dan runner yang berasal dari setup yang sama
	// Untuk SimBroker, trade ID berawalan SIM_{symbol}_{timestamp}
	// Setup dikelompokkan per simbol dan waktu entry yang sama
	type setupKey struct {
		symbol    string
		entryNano int64
	}
	grouped := make(map[setupKey]*AggregatedTrade)
	var orderedKeys []setupKey

	for _, tr := range rawTrades {
		key := setupKey{
			symbol:    tr.Symbol,
			entryNano: tr.EntryTime.UnixNano(),
		}
		agg, exists := grouped[key]
		if !exists {
			ePrice, _ := tr.EntryPrice.Float64()
			atr, _ := tr.ATRAtEntry.Float64()
			natr := 0.0
			if ePrice > 0 {
				natr = (atr / ePrice) * 100.0
			}
			agg = &AggregatedTrade{
				SetupID:     tr.ID,
				Symbol:      tr.Symbol,
				Side:        string(tr.Side),
				EntryPrice:  ePrice,
				ATRAtEntry:  atr,
				NATRPct:     natr,
				EntryTime:   tr.EntryTime,
				ExitTime:    tr.ExitTime,
				ExitReasons: make([]string, 0),
			}
			grouped[key] = agg
			orderedKeys = append(orderedKeys, key)
		}

		gPnL, _ := tr.GrossPnL.Float64()
		nPnL, _ := tr.NetPnL.Float64()
		fee, _ := tr.FeeTotal.Float64()
		funding, _ := tr.FundingTotal.Float64()

		agg.GrossPnL += gPnL
		agg.NetPnL += nPnL
		agg.TotalFees += fee
		agg.FundingPaid += funding
		agg.ExitReasons = append(agg.ExitReasons, tr.ExitReason)
		if tr.ExitTime.After(agg.ExitTime) {
			agg.ExitTime = tr.ExitTime
		}
		if tr.HoldingBars > agg.HoldingBars {
			agg.HoldingBars = tr.HoldingBars
		}
	}

	trades := make([]AggregatedTrade, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		trades = append(trades, *grouped[key])
	}

	// 2. Hitung Metrik Inti
	res := BacktestSummary{
		TotalTrades:      len(trades),
		ExitDistribution: make(map[string]int),
		SampleWarning:    len(trades) < 200,
		Trades:           trades,
	}

	var sumWin, sumLoss float64
	var grossProfit, grossLoss float64
	var pnlList []float64

	currentLosingStreak := 0
	peakEquity := initialEquity
	currentEquity := initialEquity
	maxDDUSDT := 0.0
	maxDDPct := 0.0

	for _, t := range trades {
		pnl := t.NetPnL
		pnlList = append(pnlList, pnl)
		res.TotalNetPnL += pnl
		res.TotalGrossPnL += t.GrossPnL
		res.TotalFees += t.TotalFees
		res.TotalFunding += t.FundingPaid

		if t.GrossPnL > 0 {
			grossProfit += t.GrossPnL
		} else {
			grossLoss += math.Abs(t.GrossPnL)
		}

		// Kategori Win / Loss / BE
		hasTP1 := false
		finalReason := ""
		for _, r := range t.ExitReasons {
			if r == "PARTIAL_TAKE_PROFIT" {
				hasTP1 = true
			} else {
				finalReason = r
			}
			res.ExitDistribution[r]++
		}
		if finalReason == "" && len(t.ExitReasons) > 0 {
			finalReason = t.ExitReasons[0]
		}

		if pnl > 0.0001 {
			res.WinsCount++
			sumWin += pnl
			currentLosingStreak = 0
			if hasTP1 && (finalReason == "TAKE_PROFIT" || finalReason == "TRAILING_STOP" || finalReason == "PROFIT_LOCK") {
				res.FullWinsCount++
			} else {
				res.PartialWinsCount++
			}
		} else if math.Abs(pnl) <= 0.0001 || (strings.Contains(finalReason, "BREAK_EVEN") && pnl >= -0.05) {
			res.BreakEvenCount++
			currentLosingStreak = 0
		} else {
			res.LossesCount++
			sumLoss += math.Abs(pnl)
			currentLosingStreak++
			if currentLosingStreak > res.MaxLosingStreak {
				res.MaxLosingStreak = currentLosingStreak
			}
		}

		// Drawdown Tracking
		currentEquity += pnl
		if currentEquity > peakEquity {
			peakEquity = currentEquity
		}
		ddUSDT := peakEquity - currentEquity
		if ddUSDT > maxDDUSDT {
			maxDDUSDT = ddUSDT
		}
		if peakEquity > 0 {
			ddPct := (ddUSDT / peakEquity) * 100.0
			if ddPct > maxDDPct {
				maxDDPct = ddPct
			}
		}
	}

	res.MaxDrawdownUSDT = maxDDUSDT
	res.MaxDrawdownPct = maxDDPct

	if res.TotalTrades > 0 {
		res.WinRatePct = (float64(res.WinsCount) / float64(res.TotalTrades)) * 100.0
		res.ExpectancyUSDT = res.TotalNetPnL / float64(res.TotalTrades)
	}

	adjustedDenom := res.WinsCount + res.LossesCount
	if adjustedDenom > 0 {
		res.AdjustedWinRatePct = (float64(res.WinsCount) / float64(adjustedDenom)) * 100.0
	}

	if res.WinsCount > 0 {
		res.AvgWinUSDT = sumWin / float64(res.WinsCount)
	}
	if res.LossesCount > 0 {
		res.AvgLossUSDT = sumLoss / float64(res.LossesCount)
	}

	if res.AvgLossUSDT > 0 {
		res.PayoffRatio = res.AvgWinUSDT / res.AvgLossUSDT
		res.BreakEvenWinRate = (1.0 / (1.0 + res.PayoffRatio)) * 100.0
	}

	if grossLoss > 0 {
		res.ProfitFactor = grossProfit / grossLoss
	} else if grossProfit > 0 {
		res.ProfitFactor = 99.0 // No loss
	}

	if grossProfit > 0 {
		res.FeeDragPct = (res.TotalFees / grossProfit) * 100.0
	}

	// 3. Monte Carlo Bootstrap untuk 95% Confidence Interval Expectancy
	ciLower, ciUpper := BootstrapConfidenceInterval(pnlList, 1000, 0.95)
	res.CI95LowerUSDT = ciLower
	res.CI95UpperUSDT = ciUpper

	// 4. Analisis Segmentasi Trade (Bebas Path-Dependency)
	res.Segmentation = CalculateSegmentation(trades)

	return res
}

// BootstrapConfidenceInterval menghitung interval kepercayaan non-parametrik via Monte Carlo resampling.
func BootstrapConfidenceInterval(samples []float64, iterations int, confidence float64) (float64, float64) {
	n := len(samples)
	if n < 5 || iterations <= 0 {
		return 0.0, 0.0
	}

	rnd := rand.New(rand.NewSource(42)) // Deterministic seed
	means := make([]float64, iterations)

	for i := 0; i < iterations; i++ {
		sum := 0.0
		for j := 0; j < n; j++ {
			sum += samples[rnd.Intn(n)]
		}
		means[i] = sum / float64(n)
	}

	sort.Float64s(means)

	alpha := (1.0 - confidence) / 2.0
	lowerIdx := int(float64(iterations) * alpha)
	upperIdx := int(float64(iterations) * (1.0 - alpha))

	if lowerIdx < 0 {
		lowerIdx = 0
	}
	if upperIdx >= iterations {
		upperIdx = iterations - 1
	}

	return means[lowerIdx], means[upperIdx]
}
