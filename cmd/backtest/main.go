package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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
	fmt.Println("\nUsage:")
	fmt.Println("  backtest data fetch   --symbols <sym1,sym2> --from <YYYY-MM-DD> --to <YYYY-MM-DD> [--cache-dir <path>]")
	fmt.Println("  backtest data verify  --symbols <sym1,sym2> --from <YYYY-MM-DD> --to <YYYY-MM-DD> [--cache-dir <path>]")
	fmt.Println("  backtest run          --symbols <sym1,sym2> --from <YYYY-MM-DD> --to <YYYY-MM-DD> [--out <report.md>] [--equity <USDT>]")
	fmt.Println("  backtest ablate       --symbols <sym1,sym2> --from <YYYY-MM-DD> --to <YYYY-MM-DD>")
	fmt.Println("\nContoh:")
	fmt.Println("  backtest data fetch --symbols BTCUSDT,ETHUSDT,SOLUSDT --from 2024-01-01 --to 2024-06-30")
	fmt.Println("  backtest run --symbols BTCUSDT,ETHUSDT,SOLUSDT --from 2024-01-01 --to 2024-06-30 --out ./report_h1_2024.md")
}

func handleDataCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: backtest data [fetch|verify] [options]")
		os.Exit(1)
	}

	sub := args[0]
	fs := flag.NewFlagSet("data "+sub, flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "BTCUSDT,ETHUSDT,SOLUSDT", "Comma-separated symbols")
	fromFlag := fs.String("from", "2024-01-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2024-06-30", "End date (YYYY-MM-DD)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	_ = fs.Parse(args[1:])

	from, err := time.Parse("2006-01-02", *fromFlag)
	if err != nil {
		slog.Error("Invalid --from format", "err", err)
		os.Exit(1)
	}
	to, err := time.Parse("2006-01-02", *toFlag)
	if err != nil {
		slog.Error("Invalid --to format", "err", err)
		os.Exit(1)
	}
	to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	symbols := splitSymbols(*symbolsFlag)
	downloader := data.NewDownloader(*cacheDirFlag)

	switch sub {
	case "fetch":
		fmt.Printf("Mengunduh candlestick 5m resmi dari data.binance.vision untuk %d simbol...\n", len(symbols))
		ctx := context.Background()
		for _, sym := range symbols {
			fmt.Printf("--> Fetching %s [%s s/d %s]...\n", sym, *fromFlag, *toFlag)
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
		fmt.Printf("Memverifikasi integritas data cache untuk %d simbol...\n", len(symbols))
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
	symbolsFlag := fs.String("symbols", "BTCUSDT,ETHUSDT,SOLUSDT", "Comma-separated symbols")
	fromFlag := fs.String("from", "2024-01-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2024-06-30", "End date (YYYY-MM-DD)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	outFlag := fs.String("out", "", "Output report markdown file path")
	equityFlag := fs.Float64("equity", 1000.0, "Initial Equity USDT")
	_ = fs.Parse(args)

	from, err := time.Parse("2006-01-02", *fromFlag)
	if err != nil {
		slog.Error("Invalid --from format", "err", err)
		os.Exit(1)
	}
	to, err := time.Parse("2006-01-02", *toFlag)
	if err != nil {
		slog.Error("Invalid --to format", "err", err)
		os.Exit(1)
	}
	to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	symbols := splitSymbols(*symbolsFlag)
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

	candleMap := make(map[string][]data.HistoricalCandle)
	filters := make(map[string]model.ExchangeFilter)

	fmt.Printf("Memuat data candlestick 5m untuk %d simbol...\n", len(symbols))
	for _, sym := range symbols {
		candles, err := downloader.FetchCandleRange(ctx, sym, "5m", from, to)
		if err != nil {
			slog.Error("Gagal memuat candle simbol", "symbol", sym, "err", err)
			continue
		}
		candleMap[sym] = candles
		filters[sym] = getStandardFilter(sym)
		fmt.Printf("  -> %s: %d bar dimuat\n", sym, len(candles))
	}

	if len(candleMap) == 0 {
		slog.Error("Tidak ada data candle yang tersedia untuk backtest. Jalankan 'backtest data fetch' terlebih dahulu.")
		os.Exit(1)
	}

	// Inisialisasi SimBroker pesimistis
	brokerCfg := simbroker.SimBrokerConfig{
		InitialEquityUSDT:       decimal.NewFromFloat(*equityFlag),
		MaxSlots:                3,
		MaxDailyDrawdownPct:     -0.05,
		AccountLeverage:         8.0,
		MakerFeeRate:            decimal.NewFromFloat(0.0002), // 2.0 bps
		TakerFeeRate:            decimal.NewFromFloat(0.0005), // 5.0 bps
		SlippageRate:            decimal.NewFromFloat(0.0005), // 5.0 bps
		SignalDelayBars:         1,
		LimitTimeoutBars:        1,
		PenetrationTicks:        1,
		IntrabarConflictSLFirst: true, // pesimistis default
		PartialTPEnabled:        true,
		PartialTP1Ratio:         0.50,
		PartialTP1ATRMult:       1.00,
		TrailingEnabled:         true,
		TrailingActivateATR:     1.10,
		TrailingDistanceATR:     0.55,
		MaxHoldingDuration:      4 * time.Hour,
		EquityModeCompounding:   false, // fixed equity
	}
	sim := simbroker.NewSimBroker(brokerCfg)

	// Evaluator Config dari konfigurasi produksi
	evaluatorCfg := buildDefaultEvaluatorConfig()

	// Inisialisasi Replay Engine
	replayCfg := replay.ReplayConfig{
		WarmupBars:   200,
		SpreadPct:    0.0002,
		FundingRate:  0.0001,
		BenchmarkSym: "BTCUSDT",
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
	meta := report.ReportMeta{
		CommitHash:    getGitCommitHash(),
		DataRangeFrom: *fromFlag,
		DataRangeTo:   *toFlag,
		Symbols:       symbols,
		ExecutionMode: "LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage 5 bps",
		FeeModel:      "Maker 2.0 bps, Taker 5.0 bps, Funding Active",
		FeaturesMode:  "Kline 5m + Exact Net Bar Delta (Binance Ground Truth)",
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
	symbolsFlag := fs.String("symbols", "BTCUSDT,ETHUSDT,SOLUSDT", "Comma-separated symbols")
	fromFlag := fs.String("from", "2024-01-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2024-06-30", "End date (YYYY-MM-DD)")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	_ = fs.Parse(args)

	from, _ := time.Parse("2006-01-02", *fromFlag)
	to, _ := time.Parse("2006-01-02", *toFlag)
	to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	symbols := splitSymbols(*symbolsFlag)
	downloader := data.NewDownloader(*cacheDirFlag)
	ctx := context.Background()

	candleMap := make(map[string][]data.HistoricalCandle)
	filters := make(map[string]model.ExchangeFilter)
	for _, sym := range symbols {
		candles, err := downloader.FetchCandleRange(ctx, sym, "5m", from, to)
		if err == nil && len(candles) > 0 {
			candleMap[sym] = candles
			filters[sym] = getStandardFilter(sym)
		}
	}

	if len(candleMap) == 0 {
		fmt.Println("Tidak ada data candle lokal. Jalankan 'backtest data fetch' terlebih dahulu.")
		return
	}

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
		brokerCfg := simbroker.SimBrokerConfig{
			InitialEquityUSDT:       decimal.NewFromFloat(1000.0),
			MaxSlots:                3,
			MaxDailyDrawdownPct:     -0.05,
			AccountLeverage:         8.0,
			MakerFeeRate:            decimal.NewFromFloat(0.0002),
			TakerFeeRate:            decimal.NewFromFloat(0.0005),
			SlippageRate:            decimal.NewFromFloat(0.0005),
			SignalDelayBars:         1,
			LimitTimeoutBars:        1,
			PenetrationTicks:        1,
			IntrabarConflictSLFirst: true,
			PartialTPEnabled:        true,
			PartialTP1Ratio:         0.50,
			PartialTP1ATRMult:       1.00,
			TrailingEnabled:         true,
			TrailingActivateATR:     1.10,
			TrailingDistanceATR:     0.55,
			MaxHoldingDuration:      4 * time.Hour,
		}
		sim := simbroker.NewSimBroker(brokerCfg)
		evalCfg := buildDefaultEvaluatorConfig()
		exp.modifierFn(&evalCfg)

		replayCfg := replay.ReplayConfig{
			WarmupBars:   200,
			SpreadPct:    0.0002,
			FundingRate:  0.0001,
			BenchmarkSym: "BTCUSDT",
		}
		eng := replay.NewReplayEngine(replayCfg, evalCfg, sim, candleMap, filters)
		_ = eng.Run(ctx)

		summary := metrics.CalculateMetrics(sim.ClosedTrades, 1000.0)
		fmt.Printf("| %-38s | %-10d | %-10.2f%%  | $%-10.2f | $%-12.2f |\n",
			exp.name, summary.TotalTrades, summary.WinRatePct, summary.TotalNetPnL, summary.ExpectancyUSDT)
	}
	fmt.Println("========================================================================================")
}

func splitSymbols(s string) []string {
	parts := strings.Split(s, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		clean := strings.TrimSpace(p)
		if clean != "" {
			res = append(res, clean)
		}
	}
	return res
}

func getStandardFilter(symbol string) model.ExchangeFilter {
	// Standard Binance Futures Filter (bisa disesuaikan per pair)
	tick := decimal.NewFromFloat(0.01)
	step := decimal.NewFromFloat(0.001)

	if strings.HasPrefix(symbol, "BTC") {
		tick = decimal.NewFromFloat(0.10)
		step = decimal.NewFromFloat(0.001)
	} else if strings.HasPrefix(symbol, "ETH") {
		tick = decimal.NewFromFloat(0.01)
		step = decimal.NewFromFloat(0.01)
	} else if strings.HasPrefix(symbol, "SOL") {
		tick = decimal.NewFromFloat(0.01)
		step = decimal.NewFromFloat(0.1)
	}

	return model.ExchangeFilter{
		TickSize:    tick,
		StepSize:    step,
		MinNotional: decimal.NewFromFloat(5.0),
		MinQty:      step,
	}
}

func buildDefaultEvaluatorConfig() strategy.EvaluatorConfig {
	cfg, err := config.LoadConfig()
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
