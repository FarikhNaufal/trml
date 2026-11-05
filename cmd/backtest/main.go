package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
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
	fmt.Println("\nPenggunaan Sederhana:")
	fmt.Println("  go run ./cmd/backtest run --symbols <koin1,koin2,...> [--equity <USDT>] [--scanner] [--out <report.md>]")
	fmt.Println("\nParameter Utama:")
	fmt.Println("  --symbols   Daftar koin yang ingin diuji dipisah koma (contoh: SOLUSDT,DOGEUSDT,NEARUSDT)")
	fmt.Println("  --equity    Modal simulasi USDT (contoh: 1000 atau 500, default: 1000)")
	fmt.Println("  --scanner   Aktifkan scanner dinamis pasar (menyeleksi koin aktif secara otomatis)")
	fmt.Println("\nContoh Langsung:")
	fmt.Println("  go run ./cmd/backtest run --symbols SOLUSDT,DOGEUSDT,NEARUSDT --equity 1000 --scanner")
}

func handleDataCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: backtest data [fetch|verify] [options]")
		os.Exit(1)
	}

	sub := args[0]
	fs := flag.NewFlagSet("data "+sub, flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "", "Comma-separated symbols (e.g. SOLUSDT,DOGEUSDT). If omitted with --scanner, automatically scans all available market coins")
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Use past completed 1 month range (e.g. 2026-10-01 to 2026-10-31)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	_ = fs.Parse(args[1:])

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
		fmt.Printf("Mengunduh candlestick 5m resmi dari data.binance.vision untuk %d simbol [%s s/d %s]...\n",
			len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
		ctx := context.Background()
		for _, sym := range symbols {
			fmt.Printf("--> Fetching %s...\n", sym)
			candles, err := downloader.FetchCandleRange(ctx, sym, "5m", from, to)
			if err != nil {
				slog.Error("Gagal fetch simbol", "symbol", sym, "err", err)
				continue
			}
			report := data.ValidateCandles(sym, candles, 5*time.Minute)
			fmt.Printf("    Sukses: %d bars dimuat (Gaps: %d, ZeroVol: %d)\n", report.TotalBars, report.GapsCount, report.ZeroVolCount)
		}
		fmt.Println("Selesai mengunduh data.")

	case "verify":
		fmt.Printf("Memverifikasi integritas data cache untuk %d simbol [%s s/d %s]...\n",
			len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
		ctx := context.Background()
		for _, sym := range symbols {
			candles, err := downloader.FetchCandleRange(ctx, sym, "5m", from, to)
			if err != nil {
				fmt.Printf("[GAGAL] %s: %v\n", sym, err)
				continue
			}
			report := data.ValidateCandles(sym, candles, 5*time.Minute)
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
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Simulate 1 month backwards (last full calendar month)")
	pastWeekFlag := fs.Bool("past-week", false, "Simulate past 1 week (latest 7 days: 2026-10-29 to 2026-11-04)")
	todayFlag := fs.Bool("today", false, "Simulate today (ongoing UTC day: 2026-11-05 with full live data)")
	scannerFlag := fs.Bool("scanner", false, "Enable dynamic rolling market scanner (selects active coins every interval)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	outFlag := fs.String("out", "", "Output report markdown file path")
	equityFlag := fs.Float64("equity", 125.0, "Initial Equity USDT (default 125.0)")
	envFlag := fs.String("env", "backtest.env", "Environment config file path (default: backtest.env)")
	_ = fs.Parse(args)

	var from, to time.Time
	var err error
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

	var symbols []string
	if *symbolsFlag != "" {
		symbols = splitSymbols(*symbolsFlag)
	} else if *scannerFlag {
		symbols = getAvailableCachedSymbols(*cacheDirFlag)
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
	ctx := context.Background()

	// Muat filter resmi bursa Binance Futures (tickSize, stepSize, minNotional)
	fmt.Printf("Memuat filter bursa resmi Binance Futures (exchangeInfo)...\n")
	exchangeFilters, err := data.LoadExchangeFilters(ctx, *cacheDirFlag)
	if err != nil {
		slog.Warn("Gagal memuat exchange filters resmi dari Binance API, menggunakan fail-safe", "err", err)
	}

	candleMap := make(map[string][]data.HistoricalCandle)
	filters := make(map[string]model.ExchangeFilter)

	fmt.Printf("Memuat data candlestick 5m untuk %d simbol [%s s/d %s]...\n",
		len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
	for _, sym := range symbols {
		candles, err := downloader.FetchCandleRange(ctx, sym, "5m", from, to)
		if err != nil {
			slog.Error("Gagal memuat candle simbol", "symbol", sym, "err", err)
			continue
		}
		candleMap[sym] = candles
		if ef, ok := exchangeFilters[sym]; ok {
			filters[sym] = ef
		} else {
			filters[sym] = model.ExchangeFilter{
				TickSize:    decimal.NewFromFloat(0.01),
				StepSize:    decimal.NewFromFloat(0.1),
				MinNotional: decimal.NewFromFloat(5.0),
				MinQty:      decimal.NewFromFloat(0.1),
			}
		}
		fmt.Printf("  -> %s: %d bar dimuat (tick: %s, step: %s)\n", sym, len(candles), filters[sym].TickSize, filters[sym].StepSize)
	}

	if len(candleMap) == 0 {
		slog.Error("Tidak ada data candle yang tersedia untuk backtest. Jalankan 'backtest data fetch' terlebih dahulu.")
		os.Exit(1)
	}

	// Evaluator Config dari konfigurasi backtest
	evaluatorCfg := buildDefaultEvaluatorConfig(*envFlag)

	// Muat data tingkat pendanaan (funding rate) historis resmi Binance
	fundingMgr := data.NewFundingManager(*cacheDirFlag)
	fmt.Printf("Memuat data tingkat pendanaan (funding rate) historis Binance untuk %d simbol...\n", len(symbols))
	_ = fundingMgr.LoadFundingRates(ctx, symbols, from, to)

	// Inisialisasi SimBroker pesimistis dengan fee & leverage dinamis dari environment
	brokerCfg := simbroker.SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(*equityFlag),
		MaxSlots:                evaluatorCfg.MaxSlots,
		MaxDailyDrawdownPct:     evaluatorCfg.MaxDailyDrawdownPct,
		AccountLeverage:         evaluatorCfg.Sizing.AccountLeverage,
		MakerFeeRate:            evaluatorCfg.Sizing.MakerFeeRate,
		TakerFeeRate:            evaluatorCfg.Sizing.TakerFeeRate,
		SlippageRate:            evaluatorCfg.Sizing.SlippageRate,
		SignalDelayBars:         1,
		LimitTimeoutBars:        1,
		PenetrationTicks:        1,
		IntrabarConflictSLFirst: true, // pesimistis default
		PartialTPEnabled:        true,
		PartialTP1Ratio:         0.50,
		PartialTP1ATRMult:       evaluatorCfg.Sizing.TP1Multiplier,
		TrailingEnabled:         true,
		TrailingActivateATR:     1.10,
		TrailingDistanceATR:     0.55,
		MaxHoldingDuration:      4 * time.Hour,
		EquityModeCompounding:   false, // fixed equity
	}
	sim := simbroker.NewSimBroker(brokerCfg)

	// Inisialisasi Replay Engine
	warmupBars := 200
	if *todayFlag {
		warmupBars = 288 // Seluruh 288 bar kemarin digunakan murni untuk warmup indikator
	}
	replayCfg := replay.ReplayConfig{
		WarmupBars:            warmupBars,
		SpreadPct:             evaluatorCfg.MaxSpreadPct,
		FundingRate:           0.0001,
		FundingMgr:            fundingMgr,
		BenchmarkSym:          "BTCUSDT",
		DynamicScannerEnabled: *scannerFlag,
		ScannerInterval:       15 * time.Minute,
		ScannerCfg:            buildDefaultScannerConfig(*envFlag),
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
	summary := metrics.CalculateMetrics(sim.ClosedTrades, *equityFlag)

	// Buat metadata laporan
	metaFrom := from.Format("2006-01-02")
	if *todayFlag {
		metaFrom = time.Now().UTC().Format("2006-01-02") + " (Today)"
	}
	makerBps := evaluatorCfg.Sizing.MakerFeeRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	takerBps := evaluatorCfg.Sizing.TakerFeeRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	slipBps := evaluatorCfg.Sizing.SlippageRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	feeModelStr := fmt.Sprintf("Maker %.1f bps, Taker %.1f bps, Slippage %.1f bps | Real Binance Funding & Dynamic Spread", makerBps, takerBps, slipBps)

	meta := report.ReportMeta{
		CommitHash:    getGitCommitHash(),
		DataRangeFrom: metaFrom,
		DataRangeTo:   to.Format("2006-01-02"),
		Symbols:       symbols,
		ExecutionMode: "LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage Active",
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
}

func handleAblateCommand(args []string) {
	fs := flag.NewFlagSet("ablate", flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "", "Comma-separated symbols (e.g. SOLUSDT,DOGEUSDT). If omitted with --scanner, automatically scans all available market coins")
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Simulate 1 month backwards (last full calendar month)")
	scannerFlag := fs.Bool("scanner", false, "Enable dynamic rolling market scanner (selects active coins every interval)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	envFlag := fs.String("env", "backtest.env", "Environment config file path (default: backtest.env)")
	_ = fs.Parse(args)

	var from, to time.Time
	if *pastMonthFlag {
		from, to = getPastMonthRange()
	} else {
		from, _ = time.Parse("2006-01-02", *fromFlag)
		to, _ = time.Parse("2006-01-02", *toFlag)
		to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	var symbols []string
	if *symbolsFlag != "" {
		symbols = splitSymbols(*symbolsFlag)
	} else if *scannerFlag {
		symbols = getAvailableCachedSymbols(*cacheDirFlag)
	} else {
		symbols = []string{"SOLUSDT", "DOGEUSDT", "NEARUSDT"}
	}
	downloader := data.NewDownloader(*cacheDirFlag)
	ctx := context.Background()

	exchangeFilters, _ := data.LoadExchangeFilters(ctx, *cacheDirFlag)

	candleMap := make(map[string][]data.HistoricalCandle)
	filters := make(map[string]model.ExchangeFilter)
	for _, sym := range symbols {
		candles, err := downloader.FetchCandleRange(ctx, sym, "5m", from, to)
		if err == nil && len(candles) > 0 {
			candleMap[sym] = candles
			if ef, ok := exchangeFilters[sym]; ok {
				filters[sym] = ef
			} else {
				filters[sym] = model.ExchangeFilter{
					TickSize:    decimal.NewFromFloat(0.01),
					StepSize:    decimal.NewFromFloat(0.1),
					MinNotional: decimal.NewFromFloat(5.0),
				}
			}
		}
	}

	if len(candleMap) == 0 {
		fmt.Println("Tidak ada data candle lokal. Jalankan 'backtest data fetch' terlebih dahulu.")
		return
	}

	fundingMgr := data.NewFundingManager(*cacheDirFlag)
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

	for _, exp := range experiments {
		evalCfg := buildDefaultEvaluatorConfig(*envFlag)
		exp.modifierFn(&evalCfg)

		brokerCfg := simbroker.SimBrokerConfig{
			InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
			MaxSlots:                evalCfg.MaxSlots,
			MaxDailyDrawdownPct:     evalCfg.MaxDailyDrawdownPct,
			AccountLeverage:         evalCfg.Sizing.AccountLeverage,
			MakerFeeRate:            evalCfg.Sizing.MakerFeeRate,
			TakerFeeRate:            evalCfg.Sizing.TakerFeeRate,
			SlippageRate:            evalCfg.Sizing.SlippageRate,
			SignalDelayBars:         1,
			LimitTimeoutBars:        1,
			PenetrationTicks:        1,
			IntrabarConflictSLFirst: true,
			PartialTPEnabled:        true,
			PartialTP1Ratio:         0.50,
			PartialTP1ATRMult:       evalCfg.Sizing.TP1Multiplier,
			TrailingEnabled:         true,
			TrailingActivateATR:     1.10,
			TrailingDistanceATR:     0.55,
			MaxHoldingDuration:      4 * time.Hour,
		}
		sim := simbroker.NewSimBroker(brokerCfg)

		replayCfg := replay.ReplayConfig{
			WarmupBars:            200,
			SpreadPct:             evalCfg.MaxSpreadPct,
			FundingRate:           0.0001,
			FundingMgr:            fundingMgr,
			BenchmarkSym:          "BTCUSDT",
			DynamicScannerEnabled: *scannerFlag,
			ScannerInterval:       15 * time.Minute,
			ScannerCfg:            buildDefaultScannerConfig(*envFlag),
		}
		eng := replay.NewReplayEngine(replayCfg, evalCfg, sim, candleMap, filters)
		_ = eng.Run(ctx)

		summary := metrics.CalculateMetrics(sim.ClosedTrades, 1000.0)
		fmt.Printf("| %-38s | %-10d | %-10.2f%%  | $%-10.2f | $%-12.2f |\n",
			exp.name, summary.TotalTrades, summary.WinRatePct, summary.TotalNetPnL, summary.ExpectancyUSDT)
	}
	fmt.Println("========================================================================================")
}

// Top20Universe mendefinisikan 20 instrumen futures terlikuid di Binance
var Top20Universe = []string{
	"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT",
	"XRPUSDT", "AVAXUSDT", "NEARUSDT", "LINKUSDT", "SUIUSDT",
	"1000PEPEUSDT", "WIFUSDT", "ADAUSDT", "APTUSDT", "OPUSDT",
	"ARBUSDT", "INJUSDT", "TIAUSDT", "FETUSDT", "RENDERUSDT",
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

func getAvailableCachedSymbols(cacheDir string) []string {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return append([]string(nil), Top20Universe...)
	}
	var syms []string
	for _, e := range entries {
		name := strings.ToUpper(e.Name())
		if e.IsDir() && strings.HasSuffix(name, "USDT") {
			syms = append(syms, name)
		}
	}
	if len(syms) == 0 {
		return append([]string(nil), Top20Universe...)
	}
	sort.Strings(syms)
	return syms
}

func splitSymbols(s string) []string {
	parts := strings.Split(s, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
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

func buildDefaultEvaluatorConfig(envPath string) strategy.EvaluatorConfig {
	cfg, err := loadConfigForBacktest(envPath)
	if err != nil {
		// Fallback default Revisi v6
		return strategy.EvaluatorConfig{
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

	return strategy.EvaluatorConfig{
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
		EMAPeriod:                200,
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
		BTCGateEnabled:           cfg.BTCGateEnabled,
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
			TPMultiplier:                   cfg.TPATRMultiplier,
			TP1Multiplier:                  cfg.PartialTP1ATRMult,
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
		return scanner.ScannerConfig{
			MinVolume24hUSDT:      cfg.MinVolume24hUSDT,
			MaxSpreadPct:          cfg.MaxSpreadPct,
			MinRVOL:               1.80,
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
