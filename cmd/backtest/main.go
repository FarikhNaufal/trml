package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/backtest/data"
	"trml/internal/backtest/metrics"
	"trml/internal/backtest/replay"
	"trml/internal/backtest/report"
	"trml/internal/backtest/simbroker"
	"trml/internal/config"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/strategy"
)

func main() {
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "data":
		handleDataCommand(os.Args[2:])
	case "run":
		handleRunCommand(os.Args[2:])
	case "ablate":
		handleAblateCommand(os.Args[2:])
	case "help", "--help", "-h":
		printHelp()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println("TRML Backtest Engine - 100% Pure Binance Ground Truth")
	fmt.Println("\nPenggunaan Utama:")
	fmt.Println("  go run ./cmd/backtest run [opsi waktu] [--equity <USDT>] [--symbols <koin1,koin2,...>] [--out <report.md>]")
	fmt.Println("\nOpsi Rentang Waktu (Pilih salah satu):")
	fmt.Println("  --today         Simulasi hari berjalan (Today UTC + 288 bar warmup)")
	fmt.Println("  --past-week     Simulasi 1 minggu terakhir (7 hari penuh)")
	fmt.Println("  --past-month    Simulasi 1 bulan kalender penuh terakhir")
	fmt.Println("  --from/--to     Rentang tanggal kustom (format: YYYY-MM-DD)")
	fmt.Println("\nParameter Tambahan:")
	fmt.Println("  --equity        Modal awal simulasi USDT (default: 125.0)")
	fmt.Println("  --interval      Candlestick interval (e.g. 5m, 15m, 1h, default dari env atau 5m)")
	fmt.Println("  --symbols       Daftar koin spesifik (jika kosong, auto-scan semua koin pasar dari cache)")
	fmt.Println("  --scanner       Dynamic rolling market scanner (default: true, gunakan --scanner=false untuk matikan)")
	fmt.Println("  --out           Simpan laporan hasil ke file markdown")
	fmt.Println("  --env           File konfigurasi environment (default: backtest.env)")
	fmt.Println("\nContoh Perintah Singkat:")
	fmt.Println("  go run ./cmd/backtest run --today")
	fmt.Println("  go run ./cmd/backtest run --past-week")
	fmt.Println("  go run ./cmd/backtest run --past-month")
	fmt.Println("  go run ./cmd/backtest run --symbols SOLUSDT,DOGEUSDT --past-week")
	fmt.Println("  go run ./cmd/backtest ablate --past-month")
}

func handleDataCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: backtest data [fetch|verify] [options]")
		os.Exit(1)
	}

	sub := args[0]
	fs := flag.NewFlagSet("data "+sub, flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "", "Comma-separated symbols (e.g. SOLUSDT,DOGEUSDT). If omitted with --scanner, automatically scans all available market coins")
	intervalFlag := fs.String("interval", "", "Candle interval (e.g. 5m, 15m, 1h). Default from env or 5m")
	envFlag := fs.String("env", "backtest.env", "Environment config file path (default: backtest.env)")
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Use past completed 1 month range (e.g. 2026-10-01 to 2026-10-31)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	_ = fs.Parse(args[1:])

	interval := "5m"
	if cfg, _ := loadConfigForBacktest(*envFlag); cfg != nil && cfg.Timeframe != "" {
		interval = cfg.Timeframe
	}
	if *intervalFlag != "" {
		interval = *intervalFlag
	}
	candleDur, tfErr := config.ParseTimeframeToDuration(interval)
	if tfErr != nil {
		candleDur = 5 * time.Minute
	}

	var from, to time.Time
	var err error
	if *pastMonthFlag {
		from, to = getPastMonthRange()
	} else {
		from, err = time.Parse("2006-01-02", *fromFlag)
		if err != nil {
			slog.Error("Invalid --from format", "err", err)
			os.Exit(1)
		}
		to, err = time.Parse("2006-01-02", *toFlag)
		if err != nil {
			slog.Error("Invalid --to format", "err", err)
			os.Exit(1)
		}
		to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	var symbols []string
	if *symbolsFlag != "" {
		symbols = splitSymbols(*symbolsFlag)
	} else {
		symbols = []string{"SOLUSDT", "DOGEUSDT", "NEARUSDT"}
	}
	downloader := data.NewDownloader(*cacheDirFlag)

	switch sub {
	case "fetch":
		fmt.Printf("Mengunduh candlestick %s resmi dari data.binance.vision untuk %d simbol [%s s/d %s]...\n",
			interval, len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
		ctx := context.Background()
		for _, sym := range symbols {
			fmt.Printf("--> Fetching %s...\n", sym)
			candles, err := downloader.FetchCandleRange(ctx, sym, interval, from, to)
			if err != nil {
				slog.Error("Gagal fetch simbol", "symbol", sym, "err", err)
				continue
			}
			report := data.ValidateCandles(sym, candles, candleDur)
			fmt.Printf("    Sukses: %d bars dimuat (Gaps: %d, ZeroVol: %d)\n", report.TotalBars, report.GapsCount, report.ZeroVolCount)
		}
		fmt.Println("Selesai mengunduh data.")

	case "verify":
		fmt.Printf("Memverifikasi integritas data cache (%s) untuk %d simbol [%s s/d %s]...\n",
			interval, len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
		ctx := context.Background()
		for _, sym := range symbols {
			candles, err := downloader.FetchCandleRange(ctx, sym, interval, from, to)
			if err != nil {
				fmt.Printf("[GAGAL] %s: %v\n", sym, err)
				continue
			}
			report := data.ValidateCandles(sym, candles, candleDur)
			fmt.Printf("[OK] %s: %d bars (%s s/d %s) | Gaps: %d | Duplikat: %d | ZeroVol: %d\n",
				sym, report.TotalBars, report.StartTime.Format("2006-01-02"), report.EndTime.Format("2006-01-02"),
				report.GapsCount, report.Duplicates, report.ZeroVolCount)
			if len(report.Warnings) > 0 {
				for _, w := range report.Warnings {
					fmt.Printf("    * Peringatan: %s\n", w)
				}
			}
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown data subcommand: %s\n", sub)
		os.Exit(1)
	}
}

func handleRunCommand(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "", "Comma-separated symbols (e.g. SOLUSDT,DOGEUSDT). If omitted with --scanner, automatically scans all available market coins")
	intervalFlag := fs.String("interval", "", "Candle interval (e.g. 5m, 15m, 1h). Default from env or 5m")
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Simulate 1 month backwards (last full calendar month)")
	pastWeekFlag := fs.Bool("past-week", false, "Simulate past 1 week (latest 7 days: 2026-10-29 to 2026-11-04)")
	todayFlag := fs.Bool("today", false, "Simulate today (ongoing UTC day: 2026-11-05 with full live data)")
	scannerFlag := fs.Bool("scanner", true, "Enable dynamic rolling market scanner (default: true, use --scanner=false to disable)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	outFlag := fs.String("out", "", "Output report markdown file path")
	csvOutFlag := fs.String("csv-out", "", "Output trades CSV file path")
	jsonOutFlag := fs.String("json-out", "", "Output raw metrics and trades JSON file path")
	equityFlag := fs.Float64("equity", 0.0, "Initial Equity USDT (default from INITIAL_EQUITY in backtest.env, or 125.0)")
	envFlag := fs.String("env", "backtest.env", "Environment config file path (default: backtest.env)")
	unconstrainedFlag := fs.Bool("unconstrained", false, "Mode evaluasi sinyal murni (unconstrained 1 R per setup, abaikan slot portofolio & cooldown)")
	compoundingFlag := fs.Bool("compounding", false, "Aktifkan mode compounding equity (position sizing dinamis mengikuti pertumbuhan saldo)")
	randomSeedFlag := fs.Int64("random-seed", 0, "Pseudo-random seed for random entry mode (Case C benchmark)")
	randomProbFlag := fs.Float64("random-prob", 0.0, "Probability per scanner coin per bar for random entry mode")
	knockoutDeltaFlag := fs.Bool("knockout-delta", false, "Disable delta z-score filter for ablation study")
	knockoutBTCFlag := fs.Bool("knockout-btc", false, "Disable BTC gate for ablation study")
	knockoutRVOLFlag := fs.Bool("knockout-rvol", false, "Disable RVOL filter for ablation study")
	knockoutNATRFlag := fs.Bool("knockout-natr", false, "Disable NATR filter for ablation study")
	flagBTCGateShortOnly := fs.Bool("flag-btc-gate-short-only", true, "Flag 1: Enable Tier 2 BTC Gate for SHORT_ONLY (default: true = best practice)")
	flagTier1EarlyBE := fs.Bool("flag-tier1-early-be", false, "Flag 2: Enable Tier 1 Early BE before TP1 (default: false = behavior 9815362)")
	flagSLGapFill := fs.Bool("flag-sl-gap-fill", false, "Flag 3: Enable SL gap fill at candle open + slippage (default: false = behavior 9815362)")
	flagDeductPendingMargin := fs.Bool("flag-deduct-pending-margin", false, "Flag 4: Deduct available margin intra-bar upon limit submit (default: false = behavior 9815362)")
	offlineFlag := fs.Bool("offline", false, "Strict 100% offline mode: rely solely on local disk cache with zero network calls")
	_ = fs.Parse(args)

	cfg, err := loadConfigForBacktest(*envFlag)
	if err != nil {
		slog.Error("Gagal memuat konfigurasi backtest", "env", *envFlag, "err", err)
		os.Exit(1)
	}

	initialEquity := 125.0
	if cfg != nil && cfg.InitialEquity.GreaterThan(decimal.Zero) {
		initialEquity = cfg.InitialEquity.InexactFloat64()
	}
	if *equityFlag > 0 {
		initialEquity = *equityFlag
	}

	var from, to time.Time
	if *todayFlag {
		from, to = getTodayRange()
	} else if *pastWeekFlag {
		from, to = getPastWeekRange()
	} else if *pastMonthFlag {
		from, to = getPastMonthRange()
	} else {
		from, err = time.Parse("2006-01-02", *fromFlag)
		if err != nil {
			slog.Error("Invalid --from format", "err", err)
			os.Exit(1)
		}
		to, err = time.Parse("2006-01-02", *toFlag)
		if err != nil {
			slog.Error("Invalid --to format", "err", err)
			os.Exit(1)
		}
		to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	ctx := context.Background()
	interval := "5m"
	if cfg != nil && cfg.Timeframe != "" {
		interval = cfg.Timeframe
	}
	if *intervalFlag != "" {
		interval = *intervalFlag
	}
	candleDur, tfErr := config.ParseTimeframeToDuration(interval)
	if tfErr != nil {
		candleDur = 5 * time.Minute
	}

	scannerCfg := buildDefaultScannerConfig(*envFlag)
	scannerCfg.Timeframe = interval
	scannerCfg.CandleDuration = candleDur

	var symbols []string
	if *symbolsFlag != "" {
		symbols = splitSymbols(*symbolsFlag)
	} else if *offlineFlag {
		var err error
		symbols, err = data.DiscoverMarketSymbolsOffline(*cacheDirFlag)
		if err != nil {
			slog.Warn("Gagal discover simbol dari cache lokal, fallback koin utama", "err", err)
			symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
		}
	} else if *scannerFlag {
		var err error
		symbols, err = data.DiscoverMarketSymbols(ctx, *cacheDirFlag, scannerCfg.MinVolume24hUSDT)
		if err != nil {
			slog.Warn("Gagal discover simbol real Binance, menggunakan fallback koin utama", "err", err)
			symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
		}
	} else {
		symbols = []string{"SOLUSDT", "DOGEUSDT", "NEARUSDT"}
	}
	hasBTC := false
	for _, s := range symbols {
		if s == "BTCUSDT" {
			hasBTC = true
			break
		}
	}
	if !hasBTC {
		symbols = append([]string{"BTCUSDT"}, symbols...)
	}

	downloader := data.NewDownloader(*cacheDirFlag)
	if *offlineFlag {
		downloader.SetOffline(true)
	}

	// Muat filter resmi bursa Binance Futures (tickSize, stepSize, minNotional)
	fmt.Printf("Memuat filter bursa resmi Binance Futures (exchangeInfo)...\n")
	var exchangeFilters map[string]model.ExchangeFilter
	if *offlineFlag {
		exchangeFilters, err = data.LoadExchangeFiltersOffline(*cacheDirFlag)
	} else {
		exchangeFilters, err = data.LoadExchangeFilters(ctx, *cacheDirFlag)
	}
	if err != nil {
		slog.Warn("Gagal memuat exchange filters resmi dari Binance API, menggunakan fail-safe", "err", err)
	}

	fmt.Printf("Memuat data candlestick %s untuk %d simbol [%s s/d %s] (16 parallel workers)...\n",
		interval, len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
	candleMap, filters := loadCandlesParallel(ctx, downloader, symbols, interval, from, to, exchangeFilters)

	if len(candleMap) == 0 {
		slog.Error("Tidak ada data candle yang tersedia untuk backtest. Jalankan 'backtest data fetch' terlebih dahulu.")
		os.Exit(1)
	}

	// Evaluator Config dari konfigurasi backtest
	evaluatorCfg := buildDefaultEvaluatorConfig(*envFlag)
	evaluatorCfg.CandleDuration = candleDur
	if *randomSeedFlag != 0 || *randomProbFlag > 0 {
		evaluatorCfg.RandomEntrySeed = *randomSeedFlag
		evaluatorCfg.RandomEntryProb = *randomProbFlag
	}
	if *knockoutDeltaFlag {
		evaluatorCfg.MinBounceZScore = -999.0
		evaluatorCfg.MinBounceZScoreLong = -999.0
		evaluatorCfg.MinBounceZScoreShort = -999.0
	}
	if *knockoutBTCFlag {
		evaluatorCfg.BTCGateEnabled = false
	}
	if *knockoutRVOLFlag {
		evaluatorCfg.MinSignalRVOL = 0.0
		evaluatorCfg.MaxFrenzyRVOL = 999.0
	}
	if *knockoutNATRFlag {
		evaluatorCfg.MinNATRPct = 0.0
		evaluatorCfg.MaxNATRPct = 999.0
	}

	// Muat data tingkat pendanaan (funding rate) historis resmi Binance
	fundingMgr := data.NewFundingManager(*cacheDirFlag)
	if *offlineFlag {
		fundingMgr.SetOffline(true)
	}
	fmt.Printf("Memuat data tingkat pendanaan (funding rate) historis Binance untuk %d simbol...\n", len(symbols))
	_ = fundingMgr.LoadFundingRates(ctx, symbols, from, to)

	// Inisialisasi SimBroker pesimistis dengan fee & leverage dinamis dari environment
	brokerCfg := buildSimBrokerConfig(*envFlag, initialEquity)
	if *compoundingFlag || cfg.EquityModeCompounding {
		brokerCfg.EquityModeCompounding = true
		fmt.Printf("Mode Compounding AKTIF: Ukuran posisi dinamis mengikuti equity berjalan.\n")
	} else {
		fmt.Printf("Mode Compounding NON-AKTIF: Ukuran posisi flat berdasarkan modal awal $%.2f.\n", initialEquity)
	}

	// Terapkan isolasi 4 flag engine (CLI override env)
	evaluatorCfg.BTCGateShortOnly = *flagBTCGateShortOnly
	if *flagTier1EarlyBE {
		brokerCfg.Tier1BEEnabled = true
	}
	if *flagSLGapFill {
		brokerCfg.SLGapFillEnabled = true
	}

	if *unconstrainedFlag {
		brokerCfg.MaxSlots = 1000
		evaluatorCfg.MaxSlots = 1000
		evaluatorCfg.CooldownAfterExitMinutes = 0
		evaluatorCfg.CooldownLossMinutes = 0
		evaluatorCfg.CooldownProfitMinutes = 0
	}
	sim := simbroker.NewSimBroker(brokerCfg)

	// Inisialisasi Replay Engine
	warmupBars := 200
	if *todayFlag {
		warmupBars = 288 // Seluruh 288 bar kemarin digunakan murni untuk warmup indikator
	}
	scannerInterval := 15 * time.Minute
	if cfg.ScannerInterval > 0 {
		scannerInterval = cfg.ScannerInterval
	}
	replayCfg := replay.ReplayConfig{
		WarmupBars:            warmupBars,
		SpreadPct:             evaluatorCfg.MaxSpreadPct,
		FundingRate:           0.0001,
		FundingMgr:            fundingMgr,
		BenchmarkSym:          "BTCUSDT",
		DynamicScannerEnabled: *scannerFlag,
		ScannerInterval:       scannerInterval,
		ScannerCfg:            scannerCfg,
		UnconstrainedMode:     *unconstrainedFlag,
		DeductPendingMargin:   *flagDeductPendingMargin || (cfg != nil && cfg.DeductPendingMargin),
		EquityModeCompounding: brokerCfg.EquityModeCompounding,
	}
	engine := replay.NewReplayEngine(replayCfg, evaluatorCfg, sim, candleMap, filters)

	fmt.Println("\nMemulai simulasi replay candlestick bar-demi-bar...")
	startTime := time.Now()
	if err := engine.Run(ctx); err != nil {
		slog.Error("Replay engine execution error", "err", err)
		os.Exit(1)
	}
	elapsed := time.Since(startTime)
	fmt.Printf("Simulasi selesai dalam %v!\n\n", elapsed)

	// Hitung metrik performa statistik
	summary := metrics.CalculateMetrics(sim.ClosedTrades, initialEquity)

	// Buat metadata laporan
	metaFrom := from.Format("2006-01-02")
	if *todayFlag {
		metaFrom = time.Now().UTC().Format("2006-01-02") + " (Today)"
	}
	makerBps := evaluatorCfg.Sizing.MakerFeeRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	takerBps := evaluatorCfg.Sizing.TakerFeeRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	slipBps := evaluatorCfg.Sizing.SlippageRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	feeModelStr := fmt.Sprintf("Maker %.1f bps, Taker %.1f bps, Slippage %.1f bps | Real Binance Funding & Dynamic Spread", makerBps, takerBps, slipBps)

	execMode := "LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage Active"
	if *unconstrainedFlag {
		execMode = "UNCONSTRAINED SIGNAL EVALUATION (1 R flat, unlimited concurrent slots, 0 cooldown)"
	}

	meta := report.ReportMeta{
		CommitHash:    getGitCommitHash(),
		DataRangeFrom: metaFrom,
		DataRangeTo:   to.Format("2006-01-02"),
		Symbols:       symbols,
		ExecutionMode: execMode,
		FeeModel:      feeModelStr,
		FeaturesMode:  "Kline 5m + Exact Net Bar Delta + Real Binance Filters & Funding",
		Trades:        sim.ClosedTrades,
	}

	reportMarkdown := report.GenerateMarkdownReport(summary, meta, engine.RejectReasons)

	// Cetak ke layar
	fmt.Println(reportMarkdown)

	// Simpan ke file jika diminta
	if *outFlag != "" {
		outDir := filepath.Dir(*outFlag)
		_ = os.MkdirAll(outDir, 0755)
		if err := os.WriteFile(*outFlag, []byte(reportMarkdown), 0644); err != nil {
			slog.Error("Gagal menulis file output laporan", "path", *outFlag, "err", err)
		} else {
			fmt.Printf("Laporan berhasil disimpan ke: %s\n", *outFlag)
		}
	}

	if *csvOutFlag != "" {
		outDir := filepath.Dir(*csvOutFlag)
		_ = os.MkdirAll(outDir, 0755)
		f, err := os.Create(*csvOutFlag)
		if err != nil {
			slog.Error("Gagal membuat file output CSV", "path", *csvOutFlag, "err", err)
		} else {
			defer f.Close()
			w := csv.NewWriter(f)
			_ = w.Write([]string{
				"ID", "Symbol", "Side", "EntryTime", "ExitTime", "EntryPrice", "ExitPrice",
				"Qty", "InitialQty", "SLPrice", "TPPrice", "TP1Price", "GrossPnL", "NetPnL",
				"FeeTotal", "FundingTotal", "ExitReason", "MAE", "MFE", "HoldingBars", "ATRAtEntry",
			})
			for _, tr := range sim.ClosedTrades {
				_ = w.Write([]string{
					tr.ID,
					tr.Symbol,
					string(tr.Side),
					tr.EntryTime.Format("2006-01-02 15:04:05"),
					tr.ExitTime.Format("2006-01-02 15:04:05"),
					tr.EntryPrice.String(),
					tr.ExitPrice.String(),
					tr.Qty.String(),
					tr.InitialQty.String(),
					tr.SLPrice.String(),
					tr.TPPrice.String(),
					tr.TP1Price.String(),
					tr.GrossPnL.String(),
					tr.NetPnL.String(),
					tr.FeeTotal.String(),
					tr.FundingTotal.String(),
					tr.ExitReason,
					fmt.Sprintf("%.4f", tr.MAE),
					fmt.Sprintf("%.4f", tr.MFE),
					strconv.Itoa(tr.HoldingBars),
					tr.ATRAtEntry.String(),
				})
			}
			w.Flush()
			fmt.Printf("Daftar trade berhasil disimpan ke: %s\n", *csvOutFlag)
		}
	}

	if *jsonOutFlag != "" {
		outDir := filepath.Dir(*jsonOutFlag)
		_ = os.MkdirAll(outDir, 0755)
		type jsonReport struct {
			Summary       metrics.BacktestSummary     `json:"summary"`
			RejectReasons map[string]int64            `json:"reject_reasons"`
			Trades        []simbroker.SimTradeRecord `json:"trades"`
		}
		data, err := json.MarshalIndent(jsonReport{
			Summary:       summary,
			RejectReasons: engine.RejectReasons,
			Trades:        sim.ClosedTrades,
		}, "", "  ")
		if err != nil {
			slog.Error("Gagal serialize json report", "err", err)
		} else {
			_ = os.WriteFile(*jsonOutFlag, data, 0644)
			fmt.Printf("Data JSON berhasil disimpan ke: %s\n", *jsonOutFlag)
		}
	}
}

func handleAblateCommand(args []string) {
	fs := flag.NewFlagSet("ablate", flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "", "Comma-separated symbols (e.g. SOLUSDT,DOGEUSDT). If omitted with --scanner, automatically scans all available market coins")
	intervalFlag := fs.String("interval", "", "Candle interval (e.g. 5m, 15m, 1h). Default from env or 5m")
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Simulate 1 month backwards (last full calendar month)")
	scannerFlag := fs.Bool("scanner", true, "Enable dynamic rolling market scanner (default: true, use --scanner=false to disable)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	envFlag := fs.String("env", "backtest.env", "Environment config file path (default: backtest.env)")
	equityFlag := fs.Float64("equity", 0.0, "Initial Equity USDT (default from INITIAL_EQUITY in backtest.env, or 125.0)")
	compoundingFlag := fs.Bool("compounding", false, "Aktifkan mode compounding equity")
	unconstrainedFlag := fs.Bool("unconstrained", true, "Mode sinyal independen unconstrained (default: true)")
	offlineFlag := fs.Bool("offline", false, "Strict 100% offline mode: rely solely on local disk cache with zero network calls")
	_ = fs.Parse(args)

	cfg, err := loadConfigForBacktest(*envFlag)
	if err != nil {
		slog.Error("Gagal memuat konfigurasi backtest", "env", *envFlag, "err", err)
		os.Exit(1)
	}

	initialEquity := 125.0
	if cfg != nil && cfg.InitialEquity.GreaterThan(decimal.Zero) {
		initialEquity = cfg.InitialEquity.InexactFloat64()
	}
	if *equityFlag > 0 {
		initialEquity = *equityFlag
	}

	var from, to time.Time
	if *pastMonthFlag {
		from, to = getPastMonthRange()
	} else {
		from, _ = time.Parse("2006-01-02", *fromFlag)
		to, _ = time.Parse("2006-01-02", *toFlag)
		to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	downloader := data.NewDownloader(*cacheDirFlag)
	if *offlineFlag {
		downloader.SetOffline(true)
	}
	ctx := context.Background()

	interval := "5m"
	if cfg != nil && cfg.Timeframe != "" {
		interval = cfg.Timeframe
	}
	if *intervalFlag != "" {
		interval = *intervalFlag
	}
	candleDur, tfErr := config.ParseTimeframeToDuration(interval)
	if tfErr != nil {
		candleDur = 5 * time.Minute
	}

	scannerCfg := buildDefaultScannerConfig(*envFlag)
	scannerCfg.Timeframe = interval
	scannerCfg.CandleDuration = candleDur
	var symbols []string
	if *symbolsFlag != "" {
		symbols = splitSymbols(*symbolsFlag)
	} else if *offlineFlag {
		symbols, _ = data.DiscoverMarketSymbolsOffline(*cacheDirFlag)
	} else if *scannerFlag {
		symbols, _ = data.DiscoverMarketSymbols(ctx, *cacheDirFlag, scannerCfg.MinVolume24hUSDT)
	} else {
		symbols = []string{"SOLUSDT", "DOGEUSDT", "NEARUSDT"}
	}

	var exchangeFilters map[string]model.ExchangeFilter
	if *offlineFlag {
		exchangeFilters, _ = data.LoadExchangeFiltersOffline(*cacheDirFlag)
	} else {
		exchangeFilters, _ = data.LoadExchangeFilters(ctx, *cacheDirFlag)
	}

	fmt.Printf("Memuat data candlestick %s untuk %d simbol [%s s/d %s] (16 parallel workers)...\n",
		interval, len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
	candleMap, filters := loadCandlesParallel(ctx, downloader, symbols, interval, from, to, exchangeFilters)

	if len(candleMap) == 0 {
		fmt.Println("Tidak ada data candle lokal. Jalankan 'backtest data fetch' terlebih dahulu.")
		return
	}

	fundingMgr := data.NewFundingManager(*cacheDirFlag)
	if *offlineFlag {
		fundingMgr.SetOffline(true)
	}
	_ = fundingMgr.LoadFundingRates(ctx, symbols, from, to)

	fmt.Println("========================================================================================")
	fmt.Println("TRML ABLATION TEST RUNNER (FILTER KNOCKOUT ANALYSIS)")
	fmt.Println("========================================================================================")
	fmt.Println("Mengukur dampak bersih tiap filter terhadap Win Rate dan Expectancy...")
	fmt.Println()

	type experiment struct {
		name       string
		modifierFn func(cfg *strategy.EvaluatorConfig)
	}

	experiments := []experiment{
		{name: "Baseline (Semua 7 Filter Aktif)", modifierFn: func(cfg *strategy.EvaluatorConfig) {}},
		{name: "Knockout 1: Tanpa Delta Z-score", modifierFn: func(cfg *strategy.EvaluatorConfig) {
			cfg.MinBounceZScore = -999.0
			cfg.MinBounceZScoreLong = -999.0
			cfg.MinBounceZScoreShort = -999.0
		}},
		{name: "Knockout 2: Tanpa BTC Macro Gate", modifierFn: func(cfg *strategy.EvaluatorConfig) {
			cfg.BTCGateEnabled = false
		}},
		{name: "Knockout 3: Tanpa RVOL Filter", modifierFn: func(cfg *strategy.EvaluatorConfig) {
			cfg.MinSignalRVOL = 0.0
			cfg.MaxFrenzyRVOL = 999.0
		}},
		{name: "Knockout 4: Tanpa NATR Cap", modifierFn: func(cfg *strategy.EvaluatorConfig) {
			cfg.MinNATRPct = 0.0
			cfg.MaxNATRPct = 999.0
		}},
	}

	fmt.Printf("| %-38s | %-10s | %-12s | %-12s | %-14s |\n", "Eksperimen Filter", "Trades", "Win Rate", "Net PnL", "Expectancy/Tr")
	fmt.Println("|:---------------------------------------|:-----------|:-------------|:-------------|:---------------|")

	scannerInterval := 15 * time.Minute
	if cfg != nil && cfg.ScannerInterval > 0 {
		scannerInterval = cfg.ScannerInterval
	}

	for _, exp := range experiments {
		evalCfg := buildDefaultEvaluatorConfig(*envFlag)
		evalCfg.CandleDuration = candleDur
		exp.modifierFn(&evalCfg)

		brokerCfg := buildSimBrokerConfig(*envFlag, initialEquity)
		if *unconstrainedFlag {
			brokerCfg.MaxSlots = 1000
			brokerCfg.MaxDailyDrawdownPct = -1.0
		}
		if *compoundingFlag || (cfg != nil && cfg.EquityModeCompounding) {
			brokerCfg.EquityModeCompounding = true
		}
		sim := simbroker.NewSimBroker(brokerCfg)

		replayCfg := replay.ReplayConfig{
			WarmupBars:            200,
			SpreadPct:             evalCfg.MaxSpreadPct,
			FundingRate:           0.0001,
			FundingMgr:            fundingMgr,
			BenchmarkSym:          "BTCUSDT",
			DynamicScannerEnabled: *scannerFlag,
			ScannerInterval:       scannerInterval,
			ScannerCfg:            scannerCfg,
			UnconstrainedMode:     *unconstrainedFlag,
		}
		eng := replay.NewReplayEngine(replayCfg, evalCfg, sim, candleMap, filters)
		_ = eng.Run(ctx)

		summary := metrics.CalculateMetrics(sim.ClosedTrades, initialEquity)
		fmt.Printf("| %-38s | %-10d | %-10.2f%%  | $%-10.2f | $%-12.2f |\n",
			exp.name, summary.TotalTrades, summary.WinRatePct, summary.TotalNetPnL, summary.ExpectancyUSDT)
	}
	fmt.Println("========================================================================================")
}

func getPastMonthRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	firstOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	lastOfPastMonth := firstOfThisMonth.Add(-time.Nanosecond)
	firstOfPastMonth := time.Date(lastOfPastMonth.Year(), lastOfPastMonth.Month(), 1, 0, 0, 0, 0, time.UTC)
	return firstOfPastMonth, lastOfPastMonth
}

func getPastWeekRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	// Kemarin sebagai penutupan hari penuh bursa terakhir
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 23, 59, 59, 0, time.UTC)
	// 7 hari ke belakang dari kemarin
	from := yesterday.AddDate(0, 0, -6)
	from = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	return from, yesterday
}

func getTodayRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	// Warmup 1 hari sebelum hari ini (kemarin 00:00:00 UTC) untuk mengisi 288 bar indikator
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	return yesterday, now
}

func loadCandlesParallel(
	ctx context.Context,
	downloader *data.Downloader,
	symbols []string,
	interval string,
	from, to time.Time,
	exchangeFilters map[string]model.ExchangeFilter,
) (map[string][]data.HistoricalCandle, map[string]model.ExchangeFilter) {
	candleMap := make(map[string][]data.HistoricalCandle)
	filters := make(map[string]model.ExchangeFilter)
	var mu sync.Mutex

	total := len(symbols)
	if total == 0 {
		return candleMap, filters
	}

	taskChan := make(chan string, total)
	for _, s := range symbols {
		taskChan <- s
	}
	close(taskChan)

	numWorkers := 16
	if total < numWorkers {
		numWorkers = total
	}

	var wg sync.WaitGroup
	var completed int32

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sym := range taskChan {
				candles, err := downloader.FetchCandleRange(ctx, sym, interval, from, to)
				curr := atomic.AddInt32(&completed, 1)

				if err != nil || len(candles) == 0 {
					slog.Debug("Gagal memuat candle simbol", "symbol", sym, "err", err)
					continue
				}

				var ef model.ExchangeFilter
				if f, ok := exchangeFilters[sym]; ok {
					ef = f
				} else {
					ef = model.ExchangeFilter{
						TickSize:    decimal.NewFromFloat(0.01),
						StepSize:    decimal.NewFromFloat(0.1),
						MinNotional: decimal.NewFromFloat(5.0),
						MinQty:      decimal.NewFromFloat(0.1),
					}
				}

				mu.Lock()
				candleMap[sym] = candles
				filters[sym] = ef
				mu.Unlock()

				fmt.Printf("[%3d/%3d] %-12s: %5d bar dimuat (tick: %s, step: %s)\n",
					curr, total, sym, len(candles), ef.TickSize, ef.StepSize)
			}
		}()
	}
	wg.Wait()
	return candleMap, filters
}

func splitSymbols(s string) []string {
	if strings.HasSuffix(s, ".txt") || strings.HasSuffix(s, ".csv") {
		if content, err := os.ReadFile(s); err == nil {
			s = string(content)
		}
	}
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	res := make([]string, 0, len(fields))
	for _, p := range fields {
		c := strings.TrimSpace(p)
		if c != "" {
			res = append(res, c)
		}
	}
	return res
}

func loadConfigForBacktest(envPath string) (*config.Config, error) {
	if envPath != "" {
		if _, err := os.Stat(envPath); err == nil {
			return config.LoadConfigFile(envPath)
		}
	}
	if _, err := os.Stat("backtest.env"); err == nil {
		return config.LoadConfigFile("backtest.env")
	}
	return config.LoadConfig()
}

func buildSimBrokerConfig(envPath string, equity float64) simbroker.SimBrokerConfig {
	cfg, err := loadConfigForBacktest(envPath)
	if err != nil || cfg == nil {
		eqDec := decimal.NewFromFloat(125.0)
		if equity > 0 {
			eqDec = decimal.NewFromFloat(equity)
		}
		return simbroker.SimBrokerConfig{
			InitialEquityUSDT:       eqDec,
			MaxSlots:                3,
			MaxDailyDrawdownPct:     -0.05,
			AccountLeverage:         8.0,
			MakerFeeRate:            decimal.NewFromFloat(0.0002),
			TakerFeeRate:            decimal.NewFromFloat(0.0005),
			SlippageRate:            decimal.NewFromFloat(0.0002),
			SignalDelayBars:         1,
			LimitTimeoutBars:        1,
			PenetrationTicks:        1,
			IntrabarConflictSLFirst: true,
			PartialTPEnabled:        true,
			PartialTP1Ratio:         0.50,
			PartialTP1ATRMult:       1.00,
			Tier1BEEnabled:          true,
			Tier2ProfitLockATR:      1.55,
			Tier2ProfitLockOffsetATR: 0.75,
			TrailingEnabled:         true,
			TrailingActivateATR:     1.10,
			TrailingDistanceATR:     0.55,
			MaxHoldingDuration:      4 * time.Hour,
			EquityModeCompounding:   false,
		}
	}

	eqDec := decimal.NewFromFloat(125.0)
	if equity > 0 {
		eqDec = decimal.NewFromFloat(equity)
	} else if cfg.InitialEquity.GreaterThan(decimal.Zero) {
		eqDec = cfg.InitialEquity
	}

	maxDD := cfg.MaxDailyDrawdown
	if maxDD >= 0 {
		maxDD = -0.05
	}
	pRatio := cfg.PartialTP1Ratio
	if pRatio <= 0 {
		pRatio = 0.50
	}
	tp1Mult := cfg.PartialTP1ATRMult
	if tp1Mult <= 0 {
		tp1Mult = 1.00
	}
	trailAct := cfg.TrailingActivateATR
	if trailAct <= 0 {
		trailAct = 1.10
	}
	trailDist := cfg.TrailingDistanceATR
	if trailDist <= 0 {
		trailDist = 0.55
	}

	maxHold := 4 * time.Hour
	if cfg.MaxHoldingDuration > 0 {
		maxHold = cfg.MaxHoldingDuration
	}

	sigDelay := 1
	if s := os.Getenv("SIGNAL_DELAY_BARS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			sigDelay = v
		}
	}
	penTicks := 1
	if s := os.Getenv("PENETRATION_TICKS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			penTicks = v
		}
	}

	return simbroker.SimBrokerConfig{
		InitialEquityUSDT:       eqDec,
		MaxSlots:                cfg.MaxSlots,
		MaxDailyDrawdownPct:     maxDD,
		AccountLeverage:         cfg.Leverage,
		MakerFeeRate:            cfg.MakerFeeRate,
		TakerFeeRate:            cfg.TakerFeeRate,
		SlippageRate:            cfg.SlippageRate,
		SLGapFillEnabled:        cfg.SLGapFillEnabled,
		SignalDelayBars:         sigDelay,
		LimitTimeoutBars:        1,
		PenetrationTicks:        penTicks,
		IntrabarConflictSLFirst: true,
		PartialTPEnabled:        cfg.PartialTPEnabled,
		PartialTP1Ratio:         pRatio,
		PartialTP1ATRMult:       tp1Mult,
		Tier1BEEnabled:          cfg.Tier1BEEnabled,
		Tier2ProfitLockEnabled:  cfg.Tier2ProfitLockEnabled,
		Tier2ProfitLockATR:      cfg.Tier2ProfitLockATR,
		Tier2ProfitLockOffsetATR: cfg.Tier2ProfitLockOffsetATR,
		RunnerBECushionATR:      cfg.RunnerBECushionATR,
		TrailingEnabled:         cfg.TrailingEnabled,
		TrailingActivateATR:     trailAct,
		TrailingDistanceATR:     trailDist,
		MaxHoldingDuration:      maxHold,
		EquityModeCompounding:   cfg.EquityModeCompounding,
	}
}

func buildDefaultEvaluatorConfig(envPath string) strategy.EvaluatorConfig {
	cfg, err := loadConfigForBacktest(envPath)
	if err != nil {
		// Fallback default Revisi v6
		return strategy.EvaluatorConfig{
			CandleDuration:           5 * time.Minute,
			MaxDailyDrawdownPct:      -0.05,
			MaxSlots:                 3,
			MaxSpreadPct:             0.0006,
			MinNATRPct:               0.0050,
			MaxNATRPct:               0.0180,
			EMAPeriod:                200,
			ATRPeriod:                14,
			DeltaZScoreWindow:        50,
			MinBarDeltaZScore:        0.85,
			DisableOIFilter:          true,
			MinBounceZScore:          0.85,
			PullbackToleranceATR:     0.25,
			MinEMA200DistanceATR:     0.50,
			MaxEMA200DistanceATR:     3.00,
			LookbackImpulseBars:      18,
			RetestLookbackBars:       6,
			MinImpulseATR:            0.75,
			MaxEntryDistanceATR:      1.15,
			EntryOrderType:           "LIMIT",
			BTCGateEnabled:           true,
			BTCMax5mDumpPct:          -0.0100,
			BTCMax5mPumpPct:          0.0100,
			BTCGatePersistenceBars:   3,
			BTCCounterMomentumPct:    0.0030,
			AllowedSide:              "BOTH",
			MaxFrenzyRVOL:            2.50,
			MaxCounterImpulseATR:     0.80,
			MinSignalRVOL:            0.75,
			CooldownAfterExitMinutes: 30,
			CooldownLossMinutes:      60,
			CooldownProfitMinutes:    30,
			VWAPSessionWarmupMinutes: 60,
			Sizing: strategy.SizingConfig{
				RiskPerTradePct:                decimal.NewFromFloat(0.02),
				MaxSlotMarginFraction:          decimal.NewFromFloat(0.25),
				AvailableBalanceBufferFraction: decimal.NewFromFloat(0.10),
				AccountLeverage:                8.0,
				SLMultiplier:                   1.75,
				TPMultiplier:                   3.00,
				TP1Multiplier:                  1.00,
				MinSLDistancePct:               decimal.NewFromFloat(0.0150),
				MakerFeeRate:                   decimal.NewFromFloat(0.0002),
				TakerFeeRate:                   decimal.NewFromFloat(0.0005),
				SlippageRate:                   decimal.NewFromFloat(0.0005),
				EntryOrderType:                 "LIMIT",
			},
		}
	}

	maxDD := cfg.MaxDailyDrawdown
	if maxDD >= 0 {
		maxDD = -0.05
	}

	var randomSeed int64
	if seedStr := os.Getenv("RANDOM_ENTRY_SEED"); seedStr != "" {
		if s, err := strconv.ParseInt(seedStr, 10, 64); err == nil {
			randomSeed = s
		}
	}
	var randomProb float64
	if probStr := os.Getenv("RANDOM_ENTRY_PROB"); probStr != "" {
		if p, err := strconv.ParseFloat(probStr, 64); err == nil {
			randomProb = p
		}
	}

	return strategy.EvaluatorConfig{
		CandleDuration:           cfg.CandleDuration,
		RandomEntrySeed:          randomSeed,
		RandomEntryProb:          randomProb,
		MaxDailyDrawdownPct:      maxDD,
		MaxSlots:                 cfg.MaxSlots,
		MaxBookStaleness:         cfg.MaxBookStaleness,
		MaxMarkStaleness:         cfg.MaxMarkStaleness,
		MaxDataStaleness:         cfg.MaxDataStaleness,
		MinOIInterval:            cfg.MinOIInterval,
		MaxOIInterval:            cfg.MaxOIInterval,
		MaxOIStaleness:           cfg.MaxOIStaleness,
		MaxSpreadPct:             cfg.MaxSpreadPct,
		MinNATRPct:               cfg.MinNATRPct,
		MaxNATRPct:               cfg.MaxNATRPct,
		EMAPeriod:                cfg.EMAPeriod,
		ATRPeriod:                14,
		DeltaZScoreWindow:        50,
		MinBarDeltaZScore:        cfg.MinBounceZScore,
		MinOIDeltaPct:            cfg.MinOIDeltaPct,
		MinOIDeltaLongPct:        cfg.MinOIDeltaLongPct,
		MinOIDeltaShortPct:       cfg.MinOIDeltaShortPct,
		DisableOIFilter:          true,
		MaxAdverseFundingRate:    0.0005,
		PullbackToleranceATR:     cfg.PullbackToleranceATR,
		MinBounceZScore:          cfg.MinBounceZScore,
		MinBounceZScoreLong:      cfg.MinBounceZScoreLong,
		MinBounceZScoreShort:     cfg.MinBounceZScoreShort,
		MinEMA200DistanceATR:     cfg.MinEMA200DistanceATR,
		MaxEMA200DistanceATR:     cfg.MaxEMA200DistanceATR,
		LookbackImpulseBars:      cfg.LookbackImpulseBars,
		RetestLookbackBars:       cfg.RetestLookbackBars,
		MinImpulseATR:            cfg.MinImpulseATR,
		MaxEntryDistanceATR:      cfg.MaxEntryDistanceATR,
		EntryOrderType:           cfg.EntryOrderType,
		MinCandleBodyRatioLong:   cfg.MinCandleBodyRatioLong,
		MaxPullbackRVOLLong:      cfg.MaxPullbackRVOLLong,
		MinImpulseATRLong:        cfg.MinImpulseATRLong,
		MinEMASlopeLong:          cfg.MinEMASlopeLong,
		RequireCleanPullbackLong: cfg.RequireCleanPullbackLong,
		BTCGateEnabled:           cfg.BTCGateEnabled,
		BTCGateShortOnly:         cfg.BTCGateShortOnly,
		BTCMax5mDumpPct:          cfg.BTCMax5mDumpPct,
		BTCMax5mPumpPct:          cfg.BTCMax5mPumpPct,
		BTCGatePersistenceBars:   cfg.BTCGatePersistenceBars,
		BTCCounterMomentumPct:    cfg.BTCCounterMomentumPct,
		AllowedSide:              cfg.AllowedSide,
		MaxFrenzyRVOL:            cfg.MaxFrenzyRVOL,
		MaxCounterImpulseATR:     cfg.MaxCounterImpulseATR,
		MinSignalRVOL:            cfg.MinSignalRVOL,
		CooldownAfterExitMinutes: cfg.CooldownAfterExitMinutes,
		CooldownLossMinutes:      cfg.CooldownLossMinutes,
		CooldownProfitMinutes:    cfg.CooldownProfitMinutes,
		VWAPSessionWarmupMinutes: cfg.VWAPSessionWarmupMinutes,
		Sizing: strategy.SizingConfig{
			RiskPerTradePct:                cfg.RiskPerTradePct,
			MaxSlotMarginFraction:          cfg.MaxSlotMarginFrac,
			AvailableBalanceBufferFraction: cfg.MaxMarginAvailFrac,
			AccountLeverage:                cfg.Leverage,
			SLMultiplier:                   cfg.SLATRMultiplier,
			SLMultiplierLong:               cfg.SLATRMultiplierLong,
			TPMultiplier:                   cfg.TPATRMultiplier,
			TP1Multiplier:                  cfg.PartialTP1ATRMult,
			TP1MultiplierLong:              cfg.PartialTP1ATRMultLong,
			MinSLDistancePct:               cfg.MinSLDistancePct,
			MakerFeeRate:                   cfg.MakerFeeRate,
			TakerFeeRate:                   cfg.TakerFeeRate,
			SlippageRate:                   cfg.SlippageRate,
			EntryOrderType:                 cfg.EntryOrderType,
		},
	}
}

func buildDefaultScannerConfig(envPath string) scanner.ScannerConfig {
	cfg, err := loadConfigForBacktest(envPath)
	if err == nil && cfg != nil {
		minRVOL := cfg.MinRVOL
		if minRVOL <= 0 {
			minRVOL = cfg.MinSignalRVOL
		}
		if minRVOL <= 0 {
			minRVOL = 1.80
		}
		return scanner.ScannerConfig{
			Timeframe:             cfg.Timeframe,
			CandleDuration:        cfg.CandleDuration,
			MinVolume24hUSDT:      cfg.MinVolume24hUSDT,
			MaxSpreadPct:          cfg.MaxSpreadPct,
			MinRVOL:               minRVOL,
			MinFallbackRVOL:       cfg.MinFallbackRVOL,
			RVOLLookback:          20,
			MinNATRPct:            cfg.MinNATRPct,
			MaxNATRPct:            cfg.MaxNATRPct,
			MaxScannerVWAPDistATR: cfg.MaxScannerVWAPDistATR,
			ATRPeriod:             14,
			EMAPeriod:             200,
			MaxWatchedCoins:       cfg.MaxWatchedCoins,
			BootstrapCount:        250,
		}
	}
	return scanner.ScannerConfig{
		Timeframe:             "5m",
		CandleDuration:        5 * time.Minute,
		MinVolume24hUSDT:      25000000.0,
		MaxSpreadPct:          0.0006,
		MinRVOL:               1.80,
		MinFallbackRVOL:       0.60,
		RVOLLookback:          20,
		MinNATRPct:            0.0050,
		MaxNATRPct:            0.0180,
		MaxScannerVWAPDistATR: 2.50,
		ATRPeriod:             14,
		EMAPeriod:             200,
		MaxWatchedCoins:       20,
		BootstrapCount:        250,
	}
}

func getGitCommitHash() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	out, err := cmd.Output()
	if err == nil {
		h := strings.TrimSpace(string(out))
		if h != "" {
			return h
		}
	}
	return "8c35d9e"
}

func isCompoundingConfigured(envPath string) bool {
	if os.Getenv("EQUITY_MODE_COMPOUNDING") == "true" {
		return true
	}
	if envPath == "" {
		envPath = "backtest.env"
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		return false
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == "EQUITY_MODE_COMPOUNDING" {
			val := strings.ToLower(strings.TrimSpace(parts[1]))
			return val == "true" || val == "1"
		}
	}
	return false
}
