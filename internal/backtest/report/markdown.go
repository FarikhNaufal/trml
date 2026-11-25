package report

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"trml/internal/backtest/metrics"
	"trml/internal/backtest/simbroker"
	"trml/internal/backtest/validation"
)

type ReportMeta struct {
	RunID              string
	CommitHash         string
	DataRangeFrom      string
	DataRangeTo        string
	Symbols            []string
	UniverseDisclosure string
	ExecutionMode      string
	FeeModel           string
	FeaturesMode       string
	Trades             []simbroker.SimTradeRecord
	MonteCarlo         *validation.MonteCarloResult
	StressScenarios    []validation.StressScenario
	WalkForward        *validation.WalkForwardReport
}

func formatSignedUSD(val float64) string {
	if val > 0.0001 {
		return fmt.Sprintf("+$%.2f", val)
	} else if val < -0.0001 {
		return fmt.Sprintf("-$%.2f", -val)
	}
	return "$0.00"
}

// GenerateMarkdownReport menghasilkan teks laporan markdown v2 lengkap yang dapat langsung disimpan atau dicetak.
func GenerateMarkdownReport(summary metrics.BacktestSummary, meta ReportMeta, rejectReasons map[string]int64) string {
	var sb strings.Builder

	sb.WriteString("========================================================================================\n")
	sb.WriteString("TRML BACKTEST v2 REPORT (POINT-IN-TIME BINANCE GROUND TRUTH)\n")
	sb.WriteString("========================================================================================\n")
	if meta.RunID != "" {
		fmt.Fprintf(&sb, "Run ID           : %s\n", meta.RunID)
	}
	fmt.Fprintf(&sb, "Waktu Eksekusi   : %s UTC\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "Commit Hash      : %s\n", meta.CommitHash)
	fmt.Fprintf(&sb, "Rentang Data     : %s s/d %s UTC\n", meta.DataRangeFrom, meta.DataRangeTo)
	if meta.UniverseDisclosure != "" {
		fmt.Fprintf(&sb, "Semesta & Filter : %s\n", meta.UniverseDisclosure)
	}
	fmt.Fprintf(&sb, "Daftar Simbol    : %d simbol (%s)\n", len(meta.Symbols), truncateSymbolList(meta.Symbols, 12))
	fmt.Fprintf(&sb, "Model Eksekusi   : %s\n", meta.ExecutionMode)
	fmt.Fprintf(&sb, "Struktur Fee     : %s\n", meta.FeeModel)
	fmt.Fprintf(&sb, "Fitur & Fidelity : %s\n", meta.FeaturesMode)
	sb.WriteString("----------------------------------------------------------------------------------------\n\n")

	renderCoreSummarySection(&sb, summary, meta.MonteCarlo)
	renderExitAndHoldingSection(&sb, summary)
	renderMicrostructureAndScannerSection(&sb, summary)
	renderTradeLogSection(&sb, meta.Trades)
	renderRegimeAndSegmentationSection(&sb, summary)
	renderSymbolAttributionSection(&sb, summary)
	renderRejectsAndValidationSection(&sb, rejectReasons, meta.WalkForward, meta.StressScenarios)
	renderConclusionSection(&sb, summary)

	sb.WriteString("\n========================================================================================\n")
	return sb.String()
}

func truncateSymbolList(symbols []string, maxDisplay int) string {
	if len(symbols) == 0 {
		return "-"
	}
	if len(symbols) <= maxDisplay {
		return strings.Join(symbols, ", ")
	}
	return fmt.Sprintf("%s, ... (+%d lainnya)", strings.Join(symbols[:maxDisplay], ", "), len(symbols)-maxDisplay)
}

func renderCoreSummarySection(sb *strings.Builder, summary metrics.BacktestSummary, mc *validation.MonteCarloResult) {
	sb.WriteString("### 1. Ringkasan Kinerja & Risk-Adjusted Metrics\n\n")
	sb.WriteString("| Metrik Performa | Nilai Hasil Uji | Keterangan Standar |\n")
	sb.WriteString("| :--- | :--- | :--- |\n")
	fmt.Fprintf(sb, "| **Total Trade Selesai** | `%d` setup | TP1 + Runner diagregasi 1 setup |\n", summary.TotalTrades)
	fmt.Fprintf(sb, "| **Win Rate (All-Inclusive)** | `%.2f%%` | %d Win / %d Loss / %d BE |\n", summary.WinRatePct, summary.WinsCount, summary.LossesCount, summary.BreakEvenCount)
	fmt.Fprintf(sb, "| **Adjusted Win Rate (Pure)** | `%.2f%%` | Win / (Win + Loss), mengabaikan BE |\n", summary.AdjustedWinRatePct)
	fmt.Fprintf(sb, "| **Payoff Ratio (Avg W/L)** | `%.2f` | Rata-rata Win ($%.2f) / Loss ($%.2f) |\n", summary.PayoffRatio, summary.AvgWinUSDT, summary.AvgLossUSDT)
	fmt.Fprintf(sb, "| **Break-Even Win Rate** | `%.2f%%` | Ambang batas minimum impas |\n", summary.BreakEvenWinRate)
	fmt.Fprintf(sb, "| **Profit Factor (Gross)** | `%.2f` | Total Profit Kotor / Loss Kotor |\n", summary.ProfitFactor)
	fmt.Fprintf(sb, "| **Net Profit Factor** | `%.2f` | Total Profit Bersih / Loss Bersih (pasca fee & funding) |\n", summary.NetProfitFactor)
	fmt.Fprintf(sb, "| **Total Net PnL** | `%s USDT` (%.2f%%) | Modal Awal $%.2f -> Akhir $%.2f |\n", formatSignedUSD(summary.TotalNetPnL), summary.TotalReturnPct, summary.InitialEquity, summary.FinalEquity)
	fmt.Fprintf(sb, "| **Net Expectancy per Trade** | `%s USDT` (%.2f R) | Rata-rata bersih per setup & R-Multiple |\n", formatSignedUSD(summary.ExpectancyUSDT), summary.AvgRMultiple)
	fmt.Fprintf(sb, "| **95%% Bootstrap CI** | `[%s, %s]` | Interval kepercayaan 1.000 iterasi |\n", formatSignedUSD(summary.CI95LowerUSDT), formatSignedUSD(summary.CI95UpperUSDT))
	fmt.Fprintf(sb, "| **MTM Max Drawdown** | `-%.2f%%` (-$%.2f) | Peak-to-trough floating timeline (Ulcer %.2f) |\n", summary.MTMMaxDrawdownPct, summary.MTMMaxDrawdownUSDT, summary.UlcerIndex)
	fmt.Fprintf(sb, "| **Closed-Trade Max Drawdown** | `-%.2f%%` (-$%.2f) | Berdasarkan trade terealisasi saja |\n", summary.MaxDrawdownPct, summary.MaxDrawdownUSDT)
	fmt.Fprintf(sb, "| **Sharpe / Sortino / Calmar** | `%.2f / %.2f / %.2f` | Annualized (365d crypto), CAGR %.1f%% |\n", summary.SharpeRatio, summary.SortinoRatio, summary.CalmarRatio, summary.CAGR)
	fmt.Fprintf(sb, "| **Recovery Factor / SQN** | `%.2f / %.2f` | Time-in-Market Exposure %.1f%% |\n", summary.RecoveryFactor, summary.SQN, summary.ExposurePct)
	fmt.Fprintf(sb, "| **Losing Streak Terpanjang** | `%d` trade berturut | Rekor kerugian berurutan |\n", summary.MaxLosingStreak)
	fmt.Fprintf(sb, "| **Fee & Friction Drag** | `%.2f%%` | Total fee ($%.2f) vs Profit Kotor |\n", summary.FeeDragPct, summary.TotalFees)
	fmt.Fprintf(sb, "| **Net Funding Fee** | `%s USDT` | Total pendanaan Binance (+ diterima / - dibayar) |\n", formatSignedUSD(-summary.TotalFunding))
	if mc != nil && mc.Iterations > 0 {
		fmt.Fprintf(sb, "| **Monte Carlo p95 / p99 DD** | `-%.2f%% / -%.2f%%` | %d shuffles (Risk >=25%% DD: %.1f%%) |\n",
			mc.P95MaxDDPct, mc.P99MaxDDPct, mc.Iterations, mc.RiskOfRuin25Pct)
	}
	sb.WriteString("\n")
}

func renderExitAndHoldingSection(sb *strings.Builder, summary metrics.BacktestSummary) {
	sb.WriteString("### 2. Distribusi Alasan Keluar (Exit Ladder) & Lama Tahan\n\n")
	sb.WriteString("| Alasan Exit | Jumlah Frekuensi | Persentase |\n")
	sb.WriteString("| :--- | :--- | :--- |\n")
	totalExits := 0
	reasons := make([]string, 0, len(summary.ExitDistribution))
	for r, cnt := range summary.ExitDistribution {
		totalExits += cnt
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		cnt := summary.ExitDistribution[reason]
		pct := 0.0
		if totalExits > 0 {
			pct = (float64(cnt) / float64(totalExits)) * 100.0
		}
		fmt.Fprintf(sb, "| `%s` | %d | %.1f%% |\n", reason, cnt, pct)
	}
	sb.WriteString("\n")

	if len(summary.Segmentation.HoldingSegments) > 0 {
		renderSegmentTable(sb, "Bucket Holding Time", summary.Segmentation.HoldingSegments)
	}
}

func renderMicrostructureAndScannerSection(sb *strings.Builder, summary metrics.BacktestSummary) {
	if summary.ExecutionAudit == nil && summary.ScannerAudit == nil {
		return
	}
	sb.WriteString("### 3. Audit Mikrostruktur Eksekusi & Scanner Dinamis\n\n")
	if ea := summary.ExecutionAudit; ea != nil {
		sb.WriteString("| Telemetri Eksekusi | Nilai |\n| :--- | :--- |\n")
		fmt.Fprintf(sb, "| Orders Submitted / Filled | %d / %d (Market=%d) |\n", ea.Submitted, ea.Filled, ea.MarketFilled)
		fmt.Fprintf(sb, "| Timed Out / GTX Repeg / Repeg Aborted | %d / %d / %d |\n", ea.TimedOut, ea.Repegged, ea.RepegAborted)
		fmt.Fprintf(sb, "| Unsplittable TP1 / SL-First Conflict / Delist Settled | %d / %d / %d |\n", ea.UnsplittableTP1, ea.SLFirstConflicts, ea.DelistSettled)
		fmt.Fprintf(sb, "| Sub-Bar 1m Steps | %d steps |\n", ea.SubBarExecCount)
		fmt.Fprintf(sb, "| OI 5m Coverage | %.1f%% (covered=%d fallback=%d miss=%d) |\n\n",
			ea.OICoveragePct, ea.OICoveredCount, ea.OIFallbackCount, ea.OIMissingCount)
	}
	if sa := summary.ScannerAudit; sa != nil && sa.CyclesRun > 0 {
		sb.WriteString("| Telemetri Scanner | Nilai |\n| :--- | :--- |\n")
		fmt.Fprintf(sb, "| Siklus Scan / Fallback Cycles | %d / %d |\n", sa.CyclesRun, sa.FallbackCycles)
		fmt.Fprintf(sb, "| Rata-rata Kandidat Valid / Dipilih | %.1f / %.1f koin |\n", sa.AvgCandidatesValid, sa.AvgSelectedCoins)
		fmt.Fprintf(sb, "| Watchlist Turnover | %.1f%% |\n", sa.WatchlistTurnover)
		fmt.Fprintf(sb, "| Total Reject Scanner (Vol / Spread / NATR / VWAP) | %d / %d / %d / %d |\n\n",
			sa.TotalVolRejected, sa.TotalSpreadReject, sa.TotalNATRReject, sa.TotalVWAPReject)
	}
}

func renderTradeLogSection(sb *strings.Builder, trades []simbroker.SimTradeRecord) {
	if len(trades) == 0 {
		return
	}
	sb.WriteString("### 4. Log Eksekusi Trade Selesai (Maks 50 Terbaru/Awal)\n\n")
	sb.WriteString("| No | Waktu Entry (UTC) | Simbol | Posisi | Entry Price | Exit Price | Alasan Exit | Net PnL | Fee | Funding |\n")
	sb.WriteString("| :- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
	maxDisplay := 50
	for i, tr := range trades {
		if i >= maxDisplay {
			fmt.Fprintf(sb, "| ... | *Menampilkan %d dari %d trade total* | | | | | | | | |\n", maxDisplay, len(trades))
			break
		}
		pnlStr := fmt.Sprintf("+$%s", tr.NetPnL.StringFixed(2))
		if tr.NetPnL.IsNegative() {
			pnlStr = fmt.Sprintf("-$%s", tr.NetPnL.Abs().StringFixed(2))
		} else if tr.NetPnL.IsZero() {
			pnlStr = "$0.00"
		}
		fmt.Fprintf(sb, "| %d | %s | `%s` | %s | %s | %s | `%s` | `%s` | $%.4f | $%.4f |\n",
			i+1,
			tr.EntryTime.UTC().Format("2006-01-02 15:04"),
			tr.Symbol,
			tr.Side,
			tr.EntryPrice.StringFixed(4),
			tr.ExitPrice.StringFixed(4),
			tr.ExitReason,
			pnlStr,
			tr.FeeTotal.InexactFloat64(),
			tr.FundingTotal.InexactFloat64(),
		)
	}
	sb.WriteString("\n")
}

func renderRegimeAndSegmentationSection(sb *strings.Builder, summary metrics.BacktestSummary) {
	hasRegimes := len(summary.RegimeSegments) > 0
	hasSeg := summary.TotalTrades > 0 && len(summary.Segmentation.NATRSegments) > 0
	if !hasRegimes && !hasSeg {
		return
	}
	sb.WriteString("### 5. Stratifikasi Market Regime & Segmentasi Trade\n\n")
	if hasRegimes {
		renderSegmentTable(sb, "BTC Macro Regime Saat Entry", summary.RegimeSegments)
	}
	if hasSeg {
		if len(summary.Segmentation.StrategySegments) > 0 {
			renderSegmentTable(sb, "Strategi (Alpha Attribution)", summary.Segmentation.StrategySegments)
		}
		renderSegmentTable(sb, "Kategori NATR", summary.Segmentation.NATRSegments)
		renderSegmentTable(sb, "Sesi Pasar (UTC)", summary.Segmentation.SessionSegments)
		renderSegmentTable(sb, "Arah Posisi", summary.Segmentation.SideSegments)
		if len(summary.Segmentation.EntryTypeSegments) > 0 {
			renderSegmentTable(sb, "Tipe Eksekusi Entry (Clean Limit vs GTX Repeg)", summary.Segmentation.EntryTypeSegments)
		}
	}
}

func renderSegmentTable(sb *strings.Builder, colTitle string, segs []metrics.SegmentStats) {
	fmt.Fprintf(sb, "| %s | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Net PF (Gross) |\n", colTitle)
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
	for _, seg := range segs {
		fmt.Fprintf(sb, "| `%s` | %d | %d W / %d L | %.1f%% | `%s` | `%s` | %.2f (%.2f) |\n",
			seg.Name, seg.TotalTrades, seg.Wins, seg.Losses, seg.WinRatePct, formatSignedUSD(seg.TotalNetPnL), formatSignedUSD(seg.AvgPnLUSDT), seg.NetProfitFactor, seg.ProfitFactor)
	}
	sb.WriteString("\n")
}

func renderSymbolAttributionSection(sb *strings.Builder, summary metrics.BacktestSummary) {
	if len(summary.TopSymbols) == 0 && len(summary.BottomSymbols) == 0 {
		return
	}
	sb.WriteString("### 6. Atribusi Kontribusi Simbol (Top 5 vs Bottom 5)\n\n")
	renderAttribTable(sb, "Top Contributors", summary.TopSymbols)
	renderAttribTable(sb, "Worst Drags", summary.BottomSymbols)
}

func renderAttribTable(sb *strings.Builder, label string, rows []metrics.SymbolAttribution) {
	if len(rows) == 0 {
		return
	}
	fmt.Fprintf(sb, "| %s | Trades | Win Rate | Net PnL | Total Fee |\n", label)
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- |\n")
	for _, r := range rows {
		fmt.Fprintf(sb, "| `%s` | %d | %.1f%% | `%s` | $%.2f |\n",
			r.Symbol, r.Trades, r.WinRatePct, formatSignedUSD(r.TotalNetPnL), r.TotalFees)
	}
	sb.WriteString("\n")
}

func renderRejectsAndValidationSection(
	sb *strings.Builder,
	rejectReasons map[string]int64,
	wfa *validation.WalkForwardReport,
	stress []validation.StressScenario,
) {
	if len(rejectReasons) > 0 {
		sb.WriteString("### 7. Audit Gatekeeper Sinyal (Top Filter Rejections)\n\n")
		sb.WriteString("| Alasan Penolakan Sinyal | Jumlah Sinyal Disaring |\n")
		sb.WriteString("| :--- | :--- |\n")
		type rejectEntry struct {
			reason string
			cnt    int64
		}
		var sortedRejects []rejectEntry
		for reason, cnt := range rejectReasons {
			sortedRejects = append(sortedRejects, rejectEntry{reason, cnt})
		}
		sort.Slice(sortedRejects, func(i, j int) bool {
			if sortedRejects[i].cnt != sortedRejects[j].cnt {
				return sortedRejects[i].cnt > sortedRejects[j].cnt
			}
			return sortedRejects[i].reason < sortedRejects[j].reason
		})
		for _, e := range sortedRejects {
			fmt.Fprintf(sb, "| `%s` | %d |\n", e.reason, e.cnt)
		}
		sb.WriteString("\n")
	}

	if wfa != nil && len(wfa.Folds) > 0 {
		sb.WriteString("### 7B. Walk-Forward Validation (Rolling IS vs OOS)\n\n")
		sb.WriteString("| Fold | In-Sample Range | Out-of-Sample Range | IS Exp | OOS Trades | OOS Exp | Degradasi | Verdict |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, f := range wfa.Folds {
			fmt.Fprintf(sb, "| #%d | %s..%s | %s..%s | `%s` | %d | `%s` | %.1f%% | **%s** |\n",
				f.FoldIndex+1,
				f.ISFrom.Format("06-01-02"), f.ISTo.Format("06-01-02"),
				f.OOSFrom.Format("06-01-02"), f.OOSTo.Format("06-01-02"),
				formatSignedUSD(f.ISSummary.ExpectancyUSDT),
				f.OOSSummary.TotalTrades,
				formatSignedUSD(f.OOSSummary.ExpectancyUSDT),
				f.DegradationPct, f.Verdict)
		}
		fmt.Fprintf(sb, "\n- **Stitched OOS Overall Verdict:** `%s` (Mean Degradation: `%.1f%%`, OOS Net PnL: `%s`)\n\n",
			wfa.OverallVerdict, wfa.MeanDegradationPct, formatSignedUSD(wfa.StitchedOOSSummary.TotalNetPnL))
	}

	if len(stress) > 0 {
		sb.WriteString("### 7C. Friction & Slippage Stress Matrix\n\n")
		sb.WriteString("| Skenario Friksi | Slippage (bps) | Trades | Win Rate | Net PnL | Net PF (Gross) | MTM MaxDD |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, sc := range stress {
			fmt.Fprintf(sb, "| `%s` | %.1f bps | %d | %.1f%% | `%s` | %.2f (%.2f) | -%.2f%% |\n",
				sc.Label, sc.SlippageBps, sc.Summary.TotalTrades, sc.Summary.WinRatePct,
				formatSignedUSD(sc.Summary.TotalNetPnL), sc.Summary.NetProfitFactor, sc.Summary.ProfitFactor, sc.Summary.MTMMaxDrawdownPct)
		}
		sb.WriteString("\n")
	}
}

func renderConclusionSection(sb *strings.Builder, summary metrics.BacktestSummary) {
	sb.WriteString("### 8. Evaluasi & Kepatuhan Batas Statistik\n\n")
	if summary.SampleWarning {
		sb.WriteString("> [!WARNING] **Sample Size Alert:** Total trade (< 200 setup) belum memenuhi syarat signifikansi statistik penuh. Perluas rentang tanggal pengujian.\n\n")
	}

	if summary.ExpectancyUSDT > 0 && summary.CI95LowerUSDT > 0 && summary.ProfitFactor >= 1.30 {
		sb.WriteString("> [!TIP] **KESIMPULAN: STRATEGI VALID DENGAN EXPECTANCY POSITIF.**\n")
		sb.WriteString("> Strategi terbukti memiliki edge statistik yang melampaui seluruh potongan fee dan slippage bursa.\n")
	} else if summary.ExpectancyUSDT > 0 && summary.CI95LowerUSDT > 0 && summary.ProfitFactor >= 1.0 {
		sb.WriteString("> [!NOTE] **KESIMPULAN: EXPECTANCY POSITIF TERKONFIRMASI (PF MODERAT < 1.30).**\n")
		sb.WriteString("> Interval kepercayaan 95% sudah sepenuhnya positif di atas nol, namun Profit Factor masih di bawah ambang ideal 1.30.\n")
	} else if summary.ExpectancyUSDT > 0 && summary.ProfitFactor >= 1.0 {
		sb.WriteString("> [!IMPORTANT] **KESIMPULAN: EXPECTANCY POSITIF TIPIS (BORDERLINE).**\n")
		sb.WriteString("> Interval 95% menyentuh wilayah negatif. Disarankan menjalankan `backtest ablate` untuk membuang filter yang menambah friction drag.\n")
	} else {
		sb.WriteString("> [!CAUTION] **KESIMPULAN: EXPECTANCY NEGATIF ATAU PROFIT FACTOR < 1.0.**\n")
		sb.WriteString("> Biaya transaksi dan slippage mengikis sebagian besar keuntungan pada konfigurasi timeframe uji. Strategi tidak siap untuk live modal riil tanpa optimasi.\n")
	}
}
