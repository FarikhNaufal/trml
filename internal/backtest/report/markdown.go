package report

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"trml/internal/backtest/metrics"
	"trml/internal/backtest/simbroker"
)

type ReportMeta struct {
	CommitHash    string
	DataRangeFrom string
	DataRangeTo   string
	Symbols       []string
	ExecutionMode string
	FeeModel      string
	FeaturesMode  string
	Trades        []simbroker.SimTradeRecord
}

func formatSignedUSD(val float64) string {
	if val > 0.0001 {
		return fmt.Sprintf("+$%.2f", val)
	} else if val < -0.0001 {
		return fmt.Sprintf("-$%.2f", -val)
	}
	return "$0.00"
}

// GenerateMarkdownReport menghasilkan teks laporan markdown lengkap yang dapat langsung disimpan atau dicetak.
func GenerateMarkdownReport(summary metrics.BacktestSummary, meta ReportMeta, rejectReasons map[string]int64) string {
	var sb strings.Builder

	sb.WriteString("========================================================================================\n")
	sb.WriteString("TRML BACKTEST ENGINE REPORT (PESSIMISTIC 100% BINANCE GROUND TRUTH)\n")
	sb.WriteString("========================================================================================\n")
	fmt.Fprintf(&sb, "Waktu Eksekusi   : %s UTC\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "Commit Hash      : %s\n", meta.CommitHash)
	fmt.Fprintf(&sb, "Rentang Data     : %s s/d %s UTC\n", meta.DataRangeFrom, meta.DataRangeTo)
	fmt.Fprintf(&sb, "Daftar Simbol    : %s\n", strings.Join(meta.Symbols, ", "))
	fmt.Fprintf(&sb, "Model Eksekusi   : %s\n", meta.ExecutionMode)
	fmt.Fprintf(&sb, "Struktur Fee     : %s\n", meta.FeeModel)
	fmt.Fprintf(&sb, "Fitur Aktif      : %s\n", meta.FeaturesMode)
	sb.WriteString("----------------------------------------------------------------------------------------\n\n")

	sb.WriteString("### 1. Ringkasan Kinerja & Metrik Eksekusi\n\n")
	sb.WriteString("| Metrik Performa | Nilai Hasil Uji | Keterangan Standar |\n")
	sb.WriteString("| :--- | :--- | :--- |\n")
	fmt.Fprintf(&sb, "| **Total Trade Selesai** | `%d` setup | TP1 + Runner diagregasi 1 setup |\n", summary.TotalTrades)
	fmt.Fprintf(&sb, "| **Win Rate (All-Inclusive)** | `%.2f%%` | %d Win / %d Loss / %d BE |\n", summary.WinRatePct, summary.WinsCount, summary.LossesCount, summary.BreakEvenCount)
	fmt.Fprintf(&sb, "| **Adjusted Win Rate (Pure)** | `%.2f%%` | Win / (Win + Loss), mengabaikan BE |\n", summary.AdjustedWinRatePct)
	fmt.Fprintf(&sb, "| **Payoff Ratio (Avg W/L)** | `%.2f` | Rata-rata Win ($%.2f) / Loss ($%.2f) |\n", summary.PayoffRatio, summary.AvgWinUSDT, summary.AvgLossUSDT)
	fmt.Fprintf(&sb, "| **Break-Even Win Rate** | `%.2f%%` | Ambang batas minimum impas |\n", summary.BreakEvenWinRate)
	fmt.Fprintf(&sb, "| **Profit Factor (Gross)** | `%.2f` | Total Profit Kotor / Loss Kotor |\n", summary.ProfitFactor)
	fmt.Fprintf(&sb, "| **Net Profit Factor** | `%.2f` | Total Profit Bersih / Loss Bersih (pasca fee & funding) |\n", summary.NetProfitFactor)
	fmt.Fprintf(&sb, "| **Total Net PnL** | `%s USDT` | Setelah potongan fee & slippage |\n", formatSignedUSD(summary.TotalNetPnL))
	fmt.Fprintf(&sb, "| **Net Expectancy per Trade** | `%s USDT` | Rata-rata keuntungan bersih per trade |\n", formatSignedUSD(summary.ExpectancyUSDT))
	fmt.Fprintf(&sb, "| **95%% Bootstrap CI** | `[%s, %s]` | Interval kepercayaan 1.000 iterasi |\n", formatSignedUSD(summary.CI95LowerUSDT), formatSignedUSD(summary.CI95UpperUSDT))
	fmt.Fprintf(&sb, "| **Max Drawdown (Peak-to-Trough)** | `-%.2f%%` (-$%.2f) | Penurunan modal terdalam |\n", summary.MaxDrawdownPct, summary.MaxDrawdownUSDT)
	fmt.Fprintf(&sb, "| **Losing Streak Terpanjang** | `%d` trade berturut | Rekor kerugian berurutan |\n", summary.MaxLosingStreak)
	fmt.Fprintf(&sb, "| **Fee & Friction Drag** | `%.2f%%` | Total fee ($%.2f) vs Profit Kotor |\n", summary.FeeDragPct, summary.TotalFees)
	fmt.Fprintf(&sb, "| **Net Funding Fee** | `%s USDT` | Total pendanaan Binance dibayar/diterima |\n\n", formatSignedUSD(summary.TotalFunding))

	sb.WriteString("### 2. Distribusi Alasan Keluar (Exit Ladder)\n\n")
	sb.WriteString("| Alasan Exit | Jumlah Frekuensi | Persentase |\n")
	sb.WriteString("| :--- | :--- | :--- |\n")
	totalExits := 0
	for _, cnt := range summary.ExitDistribution {
		totalExits += cnt
	}
	for reason, cnt := range summary.ExitDistribution {
		pct := 0.0
		if totalExits > 0 {
			pct = (float64(cnt) / float64(totalExits)) * 100.0
		}
		fmt.Fprintf(&sb, "| `%s` | %d | %.1f%% |\n", reason, cnt, pct)
	}
	sb.WriteString("\n")

	if len(meta.Trades) > 0 {
		sb.WriteString("### 3. Log Eksekusi Trade Selesai\n\n")
		sb.WriteString("| No | Waktu Entry (UTC) | Simbol | Posisi | Entry Price | Exit Price | Alasan Exit | Net PnL | Fee | Funding |\n")
		sb.WriteString("| :- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		maxDisplay := 50
		for i, tr := range meta.Trades {
			if i >= maxDisplay {
				fmt.Fprintf(&sb, "| ... | *Menampilkan %d dari %d trade total* | | | | | | | | |\n", maxDisplay, len(meta.Trades))
				break
			}
			pnlStr := fmt.Sprintf("+$%s", tr.NetPnL.StringFixed(2))
			if tr.NetPnL.IsNegative() {
				pnlStr = fmt.Sprintf("-$%s", tr.NetPnL.Abs().StringFixed(2))
			} else if tr.NetPnL.IsZero() {
				pnlStr = "$0.00"
			}
			fmt.Fprintf(&sb, "| %d | %s | `%s` | %s | %s | %s | `%s` | `%s` | $%.4f | $%.4f |\n",
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

	// 4. Analisis Segmentasi Trade Bebas Path-Dependency
	if summary.TotalTrades > 0 && len(summary.Segmentation.NATRSegments) > 0 {
		sb.WriteString("### 4. Analisis Segmentasi Trade (Bebas Path-Dependency)\n\n")

		// Segmentasi NATR
		sb.WriteString("#### A. Segmentasi Berdasarkan Volatilitas (NATR)\n\n")
		sb.WriteString("| Kategori NATR | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, seg := range summary.Segmentation.NATRSegments {
			fmt.Fprintf(&sb, "| `%s` | %d | %d W / %d L | %.1f%% | `%s` | `%s` | %.2f |\n",
				seg.Name, seg.TotalTrades, seg.Wins, seg.Losses, seg.WinRatePct, formatSignedUSD(seg.TotalNetPnL), formatSignedUSD(seg.AvgPnLUSDT), seg.ProfitFactor)
		}
		sb.WriteString("\n")

		// Segmentasi Jam / Sesi
		sb.WriteString("#### B. Segmentasi Berdasarkan Sesi Waktu (UTC)\n\n")
		sb.WriteString("| Sesi Pasar | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, seg := range summary.Segmentation.SessionSegments {
			fmt.Fprintf(&sb, "| `%s` | %d | %d W / %d L | %.1f%% | `%s` | `%s` | %.2f |\n",
				seg.Name, seg.TotalTrades, seg.Wins, seg.Losses, seg.WinRatePct, formatSignedUSD(seg.TotalNetPnL), formatSignedUSD(seg.AvgPnLUSDT), seg.ProfitFactor)
		}
		sb.WriteString("\n")

		// Segmentasi Posisi Arah
		sb.WriteString("#### C. Segmentasi Berdasarkan Arah Posisi (LONG vs SHORT)\n\n")
		sb.WriteString("| Arah Posisi | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |\n")
		sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
		for _, seg := range summary.Segmentation.SideSegments {
			fmt.Fprintf(&sb, "| `%s` | %d | %d W / %d L | %.1f%% | `%s` | `%s` | %.2f |\n",
				seg.Name, seg.TotalTrades, seg.Wins, seg.Losses, seg.WinRatePct, formatSignedUSD(seg.TotalNetPnL), formatSignedUSD(seg.AvgPnLUSDT), seg.ProfitFactor)
		}
		sb.WriteString("\n")
	}

	if len(rejectReasons) > 0 {
		sb.WriteString("### 5. Audit Gatekeeper Sinyal (Top Filter Rejections)\n\n")
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
			fmt.Fprintf(&sb, "| `%s` | %d |\n", e.reason, e.cnt)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("### 6. Evaluasi & Kepatuhan Batas Statistik\n\n")
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
		sb.WriteString("> Biaya transaksi dan slippage mengikis sebagian besar keuntungan pada timeframe 5m. Strategi tidak siap untuk live modal riil tanpa optimasi.\n")
	}

	sb.WriteString("\n========================================================================================\n")
	return sb.String()
}
