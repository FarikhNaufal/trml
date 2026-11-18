package metrics

import (
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
)

type BacktestSummary struct {
	InitialEquity      float64             `json:"initial_equity"`
	FinalEquity        float64             `json:"final_equity"`
	TotalReturnPct     float64             `json:"total_return_pct"`
	CAGR               float64             `json:"cagr_pct"`
	TotalTrades        int                 `json:"total_trades"`
	WinsCount          int                 `json:"wins_count"`
	FullWinsCount      int                 `json:"full_wins_count"`
	PartialWinsCount   int                 `json:"partial_wins_count"`
	BreakEvenCount     int                 `json:"break_even_count"`
	LossesCount        int                 `json:"losses_count"`
	WinRatePct         float64             `json:"win_rate_pct"`
	AdjustedWinRatePct float64             `json:"adjusted_win_rate_pct"`
	AvgWinUSDT         float64             `json:"avg_win_usdt"`
	AvgLossUSDT        float64             `json:"avg_loss_usdt"`
	PayoffRatio        float64             `json:"payoff_ratio"`
	BreakEvenWinRate   float64             `json:"break_even_win_rate"`
	ProfitFactor       float64             `json:"profit_factor"`
	NetProfitFactor    float64             `json:"net_profit_factor"`
	TotalNetPnL        float64             `json:"total_net_pnl"`
	TotalGrossPnL      float64             `json:"total_gross_pnl"`
	TotalFees          float64             `json:"total_fees"`
	TotalFunding       float64             `json:"total_funding"`
	FeeDragPct         float64             `json:"fee_drag_pct"`
	ExpectancyUSDT     float64             `json:"expectancy_usdt"`
	CI95LowerUSDT      float64             `json:"ci95_lower_usdt"`
	CI95UpperUSDT      float64             `json:"ci95_upper_usdt"`
	MaxDrawdownPct     float64             `json:"max_drawdown_pct"`
	MaxDrawdownUSDT    float64             `json:"max_drawdown_usdt"`
	MTMMaxDrawdownPct  float64             `json:"mtm_max_drawdown_pct"`
	MTMMaxDrawdownUSDT float64             `json:"mtm_max_drawdown_usdt"`
	UlcerIndex         float64             `json:"ulcer_index"`
	SharpeRatio        float64             `json:"sharpe_ratio"`
	SortinoRatio       float64             `json:"sortino_ratio"`
	CalmarRatio        float64             `json:"calmar_ratio"`
	RecoveryFactor     float64             `json:"recovery_factor"`
	SQN                float64             `json:"sqn"`
	AvgRMultiple       float64             `json:"avg_r_multiple"`
	ExposurePct        float64             `json:"exposure_pct"`
	MaxLosingStreak    int                 `json:"max_losing_streak"`
	ExitDistribution   map[string]int      `json:"exit_distribution"`
	SampleWarning      bool                `json:"sample_warning"`
	Trades             []AggregatedTrade   `json:"trades,omitempty"`
	Segmentation       SegmentationReport  `json:"segmentation,omitempty"`
	RegimeSegments     []SegmentStats      `json:"regime_segments,omitempty"`
	TopSymbols         []SymbolAttribution `json:"top_symbols,omitempty"`
	BottomSymbols      []SymbolAttribution `json:"bottom_symbols,omitempty"`
	ScannerAudit       *ScannerAudit       `json:"scanner_audit,omitempty"`
	ExecutionAudit     *ExecutionAudit     `json:"execution_audit,omitempty"`
}

// AggregatedTrade merepresentasikan 1 trade setup utuh (menggabungkan leg partial TP1 dan leg runner).
type AggregatedTrade struct {
	SetupID     string    `json:"setup_id"`
	Symbol      string    `json:"symbol"`
	Side        string    `json:"side"`
	EntryPrice  float64   `json:"entry_price"`
	InitialQty  float64   `json:"initial_qty"`
	SLPrice     float64   `json:"sl_price"`
	RiskUSDT    float64   `json:"risk_usdt"`
	RMultiple   float64   `json:"r_multiple"`
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
	return CalculateMetricsV2(rawTrades, nil, initialEquity, nil, nil, nil)
}

// CalculateMetricsV2 builds full Backtest v2 risk-adjusted, MTM drawdown, regime-stratified, and scanner telemetry metrics.
func CalculateMetricsV2(
	rawTrades []simbroker.SimTradeRecord,
	equityCurve []simbroker.EquityPoint,
	initialEquity float64,
	btcCandles []data.HistoricalCandle,
	scanAudit *ScannerAudit,
	execAudit *ExecutionAudit,
) BacktestSummary {
	if len(rawTrades) == 0 {
		out := BacktestSummary{
			InitialEquity:    initialEquity,
			FinalEquity:      initialEquity,
			SampleWarning:    true,
			ExitDistribution: make(map[string]int),
			ScannerAudit:     scanAudit,
			ExecutionAudit:   execAudit,
		}
		applyEquityCurveMetrics(&out, equityCurve, initialEquity, nil)
		return out
	}

	trades := aggregateSetupTrades(rawTrades)
	res := BacktestSummary{
		InitialEquity:    initialEquity,
		TotalTrades:      len(trades),
		ExitDistribution: make(map[string]int),
		SampleWarning:    len(trades) < 200,
		Trades:           trades,
		ScannerAudit:     scanAudit,
		ExecutionAudit:   execAudit,
	}

	var sumWin, sumLoss float64
	var grossProfit, grossLoss float64
	pnlList := make([]float64, 0, len(trades))
	rMultiples := make([]float64, 0, len(trades))

	currentLosingStreak := 0
	peakEquity := initialEquity
	currentEquity := initialEquity
	maxDDUSDT := 0.0
	maxDDPct := 0.0

	for i := range trades {
		t := &trades[i]
		if t.RiskUSDT > 0 {
			t.RMultiple = t.NetPnL / t.RiskUSDT
			rMultiples = append(rMultiples, t.RMultiple)
		}
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

	res.FinalEquity = initialEquity + res.TotalNetPnL
	if initialEquity > 0 {
		res.TotalReturnPct = (res.TotalNetPnL / initialEquity) * 100.0
	}
	res.MaxDrawdownUSDT = maxDDUSDT
	res.MaxDrawdownPct = maxDDPct

	if res.TotalTrades > 0 {
		res.WinRatePct = (float64(res.WinsCount) / float64(res.TotalTrades)) * 100.0
		res.ExpectancyUSDT = res.TotalNetPnL / float64(res.TotalTrades)
	}
	if adj := res.WinsCount + res.LossesCount; adj > 0 {
		res.AdjustedWinRatePct = (float64(res.WinsCount) / float64(adj)) * 100.0
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
		res.ProfitFactor = 99.0
	}
	if sumLoss > 0 {
		res.NetProfitFactor = sumWin / sumLoss
	} else if sumWin > 0 {
		res.NetProfitFactor = 99.0
	}
	if grossProfit > 0 {
		res.FeeDragPct = (res.TotalFees / grossProfit) * 100.0
	}
	if len(rMultiples) > 0 {
		sumR := 0.0
		for _, r := range rMultiples {
			sumR += r
		}
		res.AvgRMultiple = sumR / float64(len(rMultiples))
		res.SQN = computeSQN(rMultiples)
	} else {
		res.SQN = computeSQN(pnlList)
	}

	ciLower, ciUpper := BootstrapConfidenceInterval(pnlList, 1000, 0.95)
	res.CI95LowerUSDT = ciLower
	res.CI95UpperUSDT = ciUpper

	res.Segmentation = CalculateSegmentation(trades)
	res.RegimeSegments = ClassifyBTCRegimes(trades, btcCandles)
	res.TopSymbols, res.BottomSymbols = CalculateSymbolAttribution(trades, 5)

	applyEquityCurveMetrics(&res, equityCurve, initialEquity, trades)
	return res
}

func aggregateSetupTrades(rawTrades []simbroker.SimTradeRecord) []AggregatedTrade {
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
			slPrice, _ := tr.SLPrice.Float64()
			initQty, _ := tr.InitialQty.Float64()
			if initQty <= 0 {
				initQty, _ = tr.Qty.Float64()
			}
			atr, _ := tr.ATRAtEntry.Float64()
			natr := 0.0
			if ePrice > 0 {
				natr = (atr / ePrice) * 100.0
			}
			riskUSDT := 0.0
			if ePrice > 0 && slPrice > 0 && initQty > 0 {
				riskUSDT = math.Abs(ePrice-slPrice) * initQty
			}
			agg = &AggregatedTrade{
				SetupID:     tr.ID,
				Symbol:      tr.Symbol,
				Side:        string(tr.Side),
				EntryPrice:  ePrice,
				InitialQty:  initQty,
				SLPrice:     slPrice,
				RiskUSDT:    riskUSDT,
				ATRAtEntry:  atr,
				NATRPct:     natr,
				EntryTime:   tr.EntryTime,
				ExitTime:    tr.ExitTime,
				ExitReasons: make([]string, 0, 2),
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
	return trades
}

func applyEquityCurveMetrics(res *BacktestSummary, curve []simbroker.EquityPoint, initialEquity float64, trades []AggregatedTrade) {
	if len(curve) > 0 {
		peak := initialEquity
		if peak <= 0 {
			peak = curve[0].EffectiveEquity
		}
		maxMTMDDUSDT := 0.0
		maxMTMDDPct := 0.0
		var sumSqDD float64
		exposedBars := 0

		dailyEq := make(map[string]float64)
		var dailyOrder []string

		for _, pt := range curve {
			eq := pt.EffectiveEquity
			if eq <= 0 {
				eq = pt.RealizedEquity
			}
			if eq > peak {
				peak = eq
			}
			ddUSDT := peak - eq
			ddPct := 0.0
			if peak > 0 {
				ddPct = (ddUSDT / peak) * 100.0
			}
			if ddUSDT > maxMTMDDUSDT {
				maxMTMDDUSDT = ddUSDT
			}
			if ddPct > maxMTMDDPct {
				maxMTMDDPct = ddPct
			}
			sumSqDD += ddPct * ddPct
			if pt.OpenPositions > 0 {
				exposedBars++
			}

			day := pt.Time.UTC().Format("2006-01-02")
			if _, seen := dailyEq[day]; !seen {
				dailyOrder = append(dailyOrder, day)
			}
			dailyEq[day] = eq
		}

		res.MTMMaxDrawdownUSDT = maxMTMDDUSDT
		res.MTMMaxDrawdownPct = maxMTMDDPct
		res.UlcerIndex = math.Sqrt(sumSqDD / float64(len(curve)))
		res.ExposurePct = (float64(exposedBars) / float64(len(curve))) * 100.0

		// Compute daily returns for Sharpe and Sortino (annualized sqrt(365) for 24/7 crypto).
		if len(dailyOrder) >= 2 && initialEquity > 0 {
			rets := make([]float64, 0, len(dailyOrder))
			prev := initialEquity
			for _, day := range dailyOrder {
				cur := dailyEq[day]
				if prev > 0 {
					rets = append(rets, (cur-prev)/prev)
				}
				prev = cur
			}
			res.SharpeRatio, res.SortinoRatio = computeSharpeAndSortino(rets, 365.0)
		}
	} else {
		res.MTMMaxDrawdownUSDT = res.MaxDrawdownUSDT
		res.MTMMaxDrawdownPct = res.MaxDrawdownPct
	}

	// Span days from curve first/last or trade timestamps for CAGR
	spanDays := estimateSpanDays(curve, trades)
	if initialEquity > 0 && res.FinalEquity > 0 && spanDays >= 1.0 {
		ratio := res.FinalEquity / initialEquity
		// Annualized CAGR when window >= 7 days; avoid extreme exponent extrapolation on tiny smoke tests.
		if spanDays >= 7.0 {
			res.CAGR = (math.Pow(ratio, 365.0/spanDays) - 1.0) * 100.0
		} else {
			res.CAGR = (ratio - 1.0) * (365.0 / spanDays) * 100.0
		}
	}

	maxDDPctRef := res.MTMMaxDrawdownPct
	if maxDDPctRef <= 0 {
		maxDDPctRef = res.MaxDrawdownPct
	}
	if maxDDPctRef > 0 {
		res.CalmarRatio = res.CAGR / maxDDPctRef
	}
	maxDDUSDT := res.MTMMaxDrawdownUSDT
	if maxDDUSDT <= 0 {
		maxDDUSDT = res.MaxDrawdownUSDT
	}
	if maxDDUSDT > 0 {
		res.RecoveryFactor = res.TotalNetPnL / maxDDUSDT
	}
}

func estimateSpanDays(curve []simbroker.EquityPoint, trades []AggregatedTrade) float64 {
	if len(curve) >= 2 {
		d := curve[len(curve)-1].Time.Sub(curve[0].Time).Hours() / 24.0
		if d > 0 {
			return d
		}
	}
	if len(trades) > 0 {
		minT := trades[0].EntryTime
		maxT := trades[0].ExitTime
		for _, t := range trades[1:] {
			if t.EntryTime.Before(minT) {
				minT = t.EntryTime
			}
			if t.ExitTime.After(maxT) {
				maxT = t.ExitTime
			}
		}
		d := maxT.Sub(minT).Hours() / 24.0
		if d > 0 {
			return d
		}
	}
	return 0
}

func computeSharpeAndSortino(rets []float64, periodsPerYear float64) (sharpe, sortino float64) {
	n := float64(len(rets))
	if n < 2 {
		return 0, 0
	}
	mean := 0.0
	for _, r := range rets {
		mean += r
	}
	mean /= n

	var sumSq, sumDownSq float64
	for _, r := range rets {
		d := r - mean
		sumSq += d * d
		if r < 0 {
			sumDownSq += r * r
		}
	}
	std := math.Sqrt(sumSq / (n - 1.0))
	downDev := math.Sqrt(sumDownSq / n)
	ann := math.Sqrt(periodsPerYear)
	if std > 1e-12 {
		sharpe = (mean / std) * ann
	}
	if downDev > 1e-12 {
		sortino = (mean / downDev) * ann
	}
	return sharpe, sortino
}

func computeSQN(samples []float64) float64 {
	n := len(samples)
	if n < 2 {
		return 0
	}
	sum := 0.0
	for _, x := range samples {
		sum += x
	}
	mean := sum / float64(n)
	sumSq := 0.0
	for _, x := range samples {
		d := x - mean
		sumSq += d * d
	}
	std := math.Sqrt(sumSq / float64(n-1))
	if std <= 1e-12 {
		return 0
	}
	return math.Sqrt(float64(n)) * (mean / std)
}

// BootstrapConfidenceInterval menghitung interval kepercayaan non-parametrik via Monte Carlo resampling.
func BootstrapConfidenceInterval(samples []float64, iterations int, confidence float64) (float64, float64) {
	n := len(samples)
	if n < 5 || iterations <= 0 {
		return 0.0, 0.0
	}

	rnd := rand.New(rand.NewSource(42))
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
