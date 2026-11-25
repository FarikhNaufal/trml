package main

import (
	"context"
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

	"trml/internal/backtest/data"
	"trml/internal/backtest/replay"
	"trml/internal/backtest/runner"
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
	case "walkforward":
		handleWalkForwardCommand(os.Args[2:])
	case "stress":
		handleStressCommand(os.Args[2:])
	case "history":
		handleHistoryCommand(os.Args[2:])
	case "help", "--help", "-h":
		printHelp()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println("TRML Backtest v2 Engine - Point-in-Time Binance Ground Truth")
	fmt.Println("\nSubcommands:")
	fmt.Println("  data [fetch|verify|import-legacy]   Download, audit, or convert legacy CSV -> .bin + backtest.db")
	fmt.Println("  run                                 Full single-window simulation + MTM/Regime/Monte Carlo report")
	fmt.Println("  walkforward                         Rolling IS/OOS Walk-Forward Validation (--train-days=45 --test-days=15 --step-days=15)")
	fmt.Println("  stress                              Friction stress matrix across escalating slippage/penetration assumptions")
	fmt.Println("  ablate                              Filter knockout analysis measuring per-filter expectancy impact")
	fmt.Println("  history                             Inspect persisted backtest runs in data/backtest/backtest.db")
	fmt.Println("\nOpsi Umum:")
	fmt.Println("  --today / --past-week / --past-month / --from --to")
	fmt.Println("  --exec-tf=5m|1m     --oi-mode=auto|strict|off     --universe=pit|cache|legacy-ticker")
	fmt.Println("  --offline           --allow-rest-fallback         --equity <USDT>     --compounding")
	fmt.Println("  --out <file.md>     --csv-out <trades.csv>        --equity-csv-out <eq.csv>  --json-out <file.json>")
}

type commonFlagRefs struct {
	symbols       *string
	interval      *string
	from          *string
	to            *string
	pastMonth     *bool
	pastWeek      *bool
	today         *bool
	scanner       *bool
	cacheDir      *string
	env           *string
	equity        *float64
	offline       *bool
	universe      *string
	allowREST     *bool
	execTF        *string
	oiMode        *string
	scannerRank   *string
	maxWatched    *int
	compounding   *bool
	unconstrained *bool
	outMD         *string
	csvOut        *string
	eqCSVOut      *string
	jsonOut       *string
}

func registerCommonFlags(fs *flag.FlagSet, defaultUnconstrained bool) *commonFlagRefs {
	return &commonFlagRefs{
		symbols:       fs.String("symbols", "", "Comma-separated symbols (if omitted, resolves via --universe)"),
		interval:      fs.String("interval", "", "Candle interval (e.g. 5m, 15m, 1h). Default from env or 5m"),
		from:          fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)"),
		to:            fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)"),
		pastMonth:     fs.Bool("past-month", false, "Use past full calendar month"),
		pastWeek:      fs.Bool("past-week", false, "Use latest 7 completed days"),
		today:         fs.Bool("today", false, "Simulate today UTC (+288 bar yesterday warmup)"),
		scanner:       fs.Bool("scanner", true, "Enable dynamic rolling market scanner"),
		cacheDir:      fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory"),
		env:           fs.String("env", "backtest.env", "Environment config file path"),
		equity:        fs.Float64("equity", 0.0, "Initial Equity USDT (override env)"),
		offline:       fs.Bool("offline", false, "Strict offline mode: zero network requests"),
		universe:      fs.String("universe", "pit", "Universe discovery: pit | cache | legacy-ticker"),
		allowREST:     fs.Bool("allow-rest-fallback", false, "Allow rate-limited REST fallback if Vision returns 404"),
		execTF:        fs.String("exec-tf", "", "Broker execution resolution: 1m (sub-bar parity) or match candle interval (default)"),
		oiMode:        fs.String("oi-mode", replay.OIModeAuto, "Historical 5m OI mode: auto | strict | off"),
		scannerRank:   fs.String("scanner-rank", "", "Scanner ranking mode override (pullback, rvol)"),
		maxWatched:    fs.Int("max-watched", 0, "Override MAX_WATCHED_COINS"),
		compounding:   fs.Bool("compounding", false, "Enable compounding position sizing"),
		unconstrained: fs.Bool("unconstrained", defaultUnconstrained, "Unconstrained 1R signal evaluation (ignore slots/cooldown)"),
		outMD:         fs.String("out", "", "Output Markdown report path"),
		csvOut:        fs.String("csv-out", "", "Output trades CSV path"),
		eqCSVOut:      fs.String("equity-csv-out", "", "Output MTM equity curve CSV path"),
		jsonOut:       fs.String("json-out", "", "Output full JSON report path"),
	}
}

func visitedFlags(fs *flag.FlagSet) map[string]bool {
	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	return visited
}

func (f *commonFlagRefs) toRunnerOptions(visited map[string]bool) runner.CommonOptions {
	from, to := resolveDateRange(*f.today, *f.pastWeek, *f.pastMonth, *f.from, *f.to)
	var todayStartUTC time.Time
	if *f.today {
		ref := to.UTC()
		todayStartUTC = time.Date(ref.Year(), ref.Month(), ref.Day(), 0, 0, 0, 0, time.UTC)
	}
	var overrideCompounding *bool
	if visited != nil && visited["compounding"] {
		overrideCompounding = f.compounding
	}
	return runner.CommonOptions{
		EnvPath:             *f.env,
		CacheDir:            *f.cacheDir,
		SymbolsCSV:          *f.symbols,
		Interval:            *f.interval,
		ExecTF:              *f.execTF,
		OIMode:              *f.oiMode,
		UniverseMode:        *f.universe,
		From:                from,
		To:                  to,
		InitialEquity:       *f.equity,
		Offline:             *f.offline,
		AllowRESTFallback:   *f.allowREST || *f.today,
		DynamicScanner:      *f.scanner,
		ScannerRanking:      *f.scannerRank,
		MaxWatched:          *f.maxWatched,
		WarmupBars:          200,
		TodayMode:           *f.today,
		TodayStartUTC:       todayStartUTC,
		OverrideCompounding: overrideCompounding,
		Unconstrained:       *f.unconstrained,
		Compounding:         *f.compounding,
		CommitHash:          getGitCommitHash(),
		BenchmarkSym:        "BTCUSDT",
		SaveToDB:            true,
		OutMarkdown:         *f.outMD,
		OutTradesCSV:        *f.csvOut,
		OutEquityCSV:        *f.eqCSVOut,
		OutJSON:             *f.jsonOut,
	}
}

func resolveDateRange(today, pastWeek, pastMonth bool, fromStr, toStr string) (time.Time, time.Time) {
	if today {
		return getTodayRange()
	}
	if pastWeek {
		return getPastWeekRange()
	}
	if pastMonth {
		return getPastMonthRange()
	}
	from, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		slog.Error("Invalid --from format", "err", err)
		os.Exit(1)
	}
	to, err := time.Parse("2006-01-02", toStr)
	if err != nil {
		slog.Error("Invalid --to format", "err", err)
		os.Exit(1)
	}
	to = to.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	return from, to
}

func handleDataCommand(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: backtest data [fetch|verify|import-legacy] [options]")
		os.Exit(1)
	}
	sub := args[0]
	fs := flag.NewFlagSet("data "+sub, flag.ExitOnError)
	symbolsFlag := fs.String("symbols", "", "Comma-separated symbols (default: resolves point-in-time universe)")
	intervalFlag := fs.String("interval", "", "Candle interval (default from env or 5m)")
	envFlag := fs.String("env", "backtest.env", "Environment config file path")
	fromFlag := fs.String("from", "2026-10-01", "Start date (YYYY-MM-DD)")
	toFlag := fs.String("to", "2026-10-31", "End date (YYYY-MM-DD)")
	pastMonthFlag := fs.Bool("past-month", false, "Use past full calendar month")
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	universeFlag := fs.String("universe", "pit", "Universe discovery: pit | cache | legacy-ticker")
	withOIFlag := fs.Bool("with-oi", false, "Download/verify 5m Open Interest archives from daily/metrics")
	execTFFlag := fs.String("exec-tf", "", "Optional execution sub-bar timeframe to also fetch (e.g. 1m)")
	allowRESTFlag := fs.Bool("allow-rest-fallback", false, "Allow rate-limited REST fallback if Vision returns 404")
	offlineFlag := fs.Bool("offline", false, "Strict offline mode: zero network requests")
	_ = fs.Parse(args[1:])

	dirs := data.ResolveBacktestDirs(*cacheDirFlag)
	store, dbErr := data.OpenBacktestStore(dirs.DBPath)
	if dbErr != nil {
		slog.Warn("gagal membuka backtest.db", "err", dbErr)
	} else {
		defer func() { _ = store.Close() }()
	}

	if sub == "import-legacy" {
		fmt.Printf("Mengimpor cache CSV lama dari %s ke %s + %s ...\n", dirs.LegacyCSV, dirs.StoreDir, dirs.DBPath)
		stats, err := data.ImportLegacyCSVCache(context.Background(), dirs, store, time.Now().UTC())
		if err != nil {
			slog.Error("Import legacy cache gagal", "err", err)
			os.Exit(1)
		}
		fmt.Printf("Import selesai: %d simbol, %d file kline (%d bars), %d file funding, %d file hari berjalan dilewati.\n",
			stats.SymbolsScanned, stats.KlineFiles, stats.KlineBars, stats.FundingFiles, stats.SkippedPartial)
		return
	}

	cfg, _ := runner.LoadConfigForBacktest(*envFlag)
	interval, candleDur, _ := runner.ResolveBacktestScannerConfig(cfg, *intervalFlag)
	from, to := resolveDateRange(false, false, *pastMonthFlag, *fromFlag, *toFlag)

	ctx := context.Background()
	httpClient := data.NewBacktestHTTPClient(data.BacktestHTTPConfig{AllowRESTFallback: *allowRESTFlag})
	symbols, _, univRes := runner.ResolveDatasetSymbols(ctx, *symbolsFlag, *universeFlag, true, *offlineFlag, *cacheDirFlag, 15_000_000.0, httpClient, store)
	if univRes.DelistedCount > 0 || univRes.BlocklistedTradFi > 0 {
		fmt.Printf("Universe (%s): %d simbol (%d active crypto, %d delisted/archived, %d TradFi blocklisted)\n",
			*universeFlag, len(symbols), univRes.ActiveCryptoCount, univRes.DelistedCount, univRes.BlocklistedTradFi)
	}

	dl := data.NewDownloader(*cacheDirFlag)
	dl.SetHTTPClient(httpClient)
	dl.SetStore(store)
	if *offlineFlag {
		dl.SetOffline(true)
	}

	var oiDownloader *data.MetricsOIDownloader
	if *withOIFlag {
		oiDownloader = data.NewMetricsOIDownloader(*cacheDirFlag, httpClient, store)
		if *offlineFlag {
			oiDownloader.SetOffline(true)
		}
	}

	switch sub {
	case "fetch":
		workers := 8
		if *allowRESTFlag {
			workers = 4
		}
		if len(symbols) < workers {
			workers = len(symbols)
		}
		if workers < 1 {
			workers = 1
		}
		fmt.Printf("Mengunduh candlestick %s dari data.binance.vision untuk %d simbol [%s s/d %s] (%d workers)...\n",
			interval, len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"), workers)
		execSubTF := strings.ToLower(strings.TrimSpace(*execTFFlag))

		taskCh := make(chan string, len(symbols))
		for _, sym := range symbols {
			taskCh <- sym
		}
		close(taskCh)

		var printMu sync.Mutex
		var wg sync.WaitGroup
		var doneCount atomic.Int32
		totalSyms := len(symbols)

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for sym := range taskCh {
					candles, err := dl.FetchCandleRange(ctx, sym, interval, from, to)
					curIdx := doneCount.Add(1)
					if err != nil {
						continue
					}
					rep := data.ValidateCandles(sym, candles, candleDur)
					extraMsg := ""
					if execSubTF == "1m" && !strings.EqualFold(interval, "1m") {
						if c1m, err1m := dl.FetchCandleRange(ctx, sym, "1m", from, to); err1m == nil {
							extraMsg += fmt.Sprintf(" | 1m: %d bars", len(c1m))
						}
					}
					if oiDownloader != nil {
						if pts, oiErr := oiDownloader.FetchOpenInterestRange(ctx, sym, from, to); oiErr == nil {
							extraMsg += fmt.Sprintf(" | OI 5m: %d pts", len(pts))
						}
					}
					printMu.Lock()
					fmt.Printf("    [%3d/%3d OK] %-12s: %d bars (Gaps: %d, ZeroVol: %d, Anomalies: %d)%s\n",
						curIdx, totalSyms, sym, rep.TotalBars, rep.GapsCount, rep.ZeroVolCount, rep.OHLCAnomalies, extraMsg)
					printMu.Unlock()
				}
			}()
		}
		wg.Wait()
	case "verify":
		fmt.Printf("Memverifikasi cache (%s) untuk %d simbol [%s s/d %s]...\n",
			interval, len(symbols), from.Format("2006-01-02"), to.Format("2006-01-02"))
		if oiDownloader != nil {
			_ = oiDownloader.LoadOpenInterestRange(ctx, symbols, from, to)
		}
		minOI, maxOI, maxStale := 4*time.Minute, 7*time.Minute, 10*time.Minute
		if cfg != nil {
			evalCfg := strategy.FromAppConfig(cfg)
			if evalCfg.MinOIInterval > 0 {
				minOI = evalCfg.MinOIInterval
			}
			if evalCfg.MaxOIInterval > 0 {
				maxOI = evalCfg.MaxOIInterval
			}
			if evalCfg.MaxOIStaleness > 0 {
				maxStale = evalCfg.MaxOIStaleness
			}
		}
		for _, sym := range symbols {
			candles, err := dl.FetchCandleRange(ctx, sym, interval, from, to)
			if err != nil {
				fmt.Printf("[GAGAL] %s: %v\n", sym, err)
				continue
			}
			rep := data.ValidateCandlesWithOI(sym, candles, candleDur, oiDownloader, minOI, maxOI, maxStale)
			oiInfo := ""
			if oiDownloader != nil {
				oiInfo = fmt.Sprintf(" | OI Cov: %.1f%%", rep.OICoveragePct)
			}
			fmt.Printf("[OK] %s: %d bars | Gaps: %d | Duplikat: %d | ZeroVol: %d | Anomali: %d%s\n",
				sym, rep.TotalBars, rep.GapsCount, rep.Duplicates, rep.ZeroVolCount, rep.OHLCAnomalies, oiInfo)
			for _, w := range rep.Warnings {
				fmt.Printf("    * Peringatan: %s\n", w)
			}
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown data subcommand: %s\n", sub)
		os.Exit(1)
	}
}

func handleRunCommand(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cf := registerCommonFlags(fs, false)
	randomSeedFlag := fs.Int64("random-seed", 0, "Pseudo-random seed for random entry benchmark")
	randomProbFlag := fs.Float64("random-prob", 0.0, "Random entry probability per coin per bar")
	knockoutDeltaFlag := fs.Bool("knockout-delta", false, "Disable delta z-score filter")
	knockoutBTCFlag := fs.Bool("knockout-btc", false, "Disable BTC macro gate")
	knockoutRVOLFlag := fs.Bool("knockout-rvol", false, "Disable RVOL filter")
	knockoutNATRFlag := fs.Bool("knockout-natr", false, "Disable NATR filter")
	flagBTCGateShortOnly := fs.Bool("flag-btc-gate-short-only", true, "Enable Tier 2 BTC Gate for SHORT_ONLY")
	flagTier1EarlyBE := fs.Bool("flag-tier1-early-be", false, "Force enable Tier 1 Early BE before TP1")
	flagSLGapFill := fs.Bool("flag-sl-gap-fill", false, "Force enable SL gap fill at candle open + slippage")
	flagDeductPendingMargin := fs.Bool("flag-deduct-pending-margin", false, "Deduct available margin intra-bar upon limit submit")
	_ = fs.Parse(args)
	visited := visitedFlags(fs)

	ctx := context.Background()
	common := cf.toRunnerOptions(visited)
	intervalLabel := common.Interval
	if intervalLabel == "" {
		intervalLabel = "auto"
	}
	execLabel := common.ExecTF
	if execLabel == "" {
		execLabel = "auto"
	}
	fmt.Printf("Menyiapkan dataset (%s, exec=%s, oi=%s, universe=%s)...\n", intervalLabel, execLabel, common.OIMode, common.UniverseMode)
	ds, err := runner.PrepareDataset(ctx, common)
	if err != nil {
		slog.Error("Gagal menyiapkan dataset", "err", err)
		os.Exit(1)
	}
	defer ds.Close()
	fmt.Printf("Dataset siap (%s, exec=%s, oi=%s, %d simbol)\n", ds.Interval, ds.ExecTF, ds.OIMode, len(ds.CandleMap))

	runOpts := runner.RunOptions{
		CommonOptions:       common,
		RandomSeed:          *randomSeedFlag,
		RandomProb:          *randomProbFlag,
		KnockoutDelta:       *knockoutDeltaFlag,
		KnockoutBTC:         *knockoutBTCFlag,
		KnockoutRVOL:        *knockoutRVOLFlag,
		KnockoutNATR:        *knockoutNATRFlag,
		BTCGateShortOnly:    *flagBTCGateShortOnly,
		Tier1EarlyBE:        *flagTier1EarlyBE,
		SLGapFill:           *flagSLGapFill,
		DeductPendingMargin: *flagDeductPendingMargin,
	}
	if visited["flag-btc-gate-short-only"] {
		runOpts.OverrideBTCGateShortOnly = flagBTCGateShortOnly
	}
	if visited["flag-tier1-early-be"] {
		runOpts.OverrideTier1EarlyBE = flagTier1EarlyBE
	}
	if visited["flag-sl-gap-fill"] {
		runOpts.OverrideSLGapFill = flagSLGapFill
	}
	if visited["flag-deduct-pending-margin"] {
		runOpts.OverrideDeductPendingMargin = flagDeductPendingMargin
	}
	if visited["compounding"] {
		runOpts.OverrideCompounding = cf.compounding
	}

	start := time.Now()
	out, err := runner.ExecuteRun(ctx, ds, runOpts)
	if err != nil {
		slog.Error("Replay gagal", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Simulasi selesai dalam %v (Run ID: %s)\n", time.Since(start), out.RunID)
	fmt.Println(out.Markdown)
}

func handleWalkForwardCommand(args []string) {
	fs := flag.NewFlagSet("walkforward", flag.ExitOnError)
	cf := registerCommonFlags(fs, false)
	trainDays := fs.Int("train-days", 45, "In-Sample window length in days")
	testDays := fs.Int("test-days", 15, "Out-of-Sample test window length in days")
	stepDays := fs.Int("step-days", 15, "Rolling step length in days")
	_ = fs.Parse(args)
	visited := visitedFlags(fs)

	ctx := context.Background()
	common := cf.toRunnerOptions(visited)
	ds, err := runner.PrepareDataset(ctx, common)
	if err != nil {
		slog.Error("Gagal menyiapkan dataset WFA", "err", err)
		os.Exit(1)
	}
	defer ds.Close()

	wfaRes, md, err := runner.ExecuteWalkForward(ctx, ds, runner.WalkForwardOptions{
		CommonOptions: common,
		TrainDays:     *trainDays,
		TestDays:      *testDays,
		StepDays:      *stepDays,
	})
	if err != nil {
		slog.Error("Walk-Forward Validation gagal", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Walk-Forward selesai: %d folds | Verdict: %s | OOS Net PnL: $%.2f\n\n",
		len(wfaRes.Folds), wfaRes.OverallVerdict, wfaRes.StitchedOOSSummary.TotalNetPnL)
	fmt.Println(md)
}

func handleStressCommand(args []string) {
	fs := flag.NewFlagSet("stress", flag.ExitOnError)
	cf := registerCommonFlags(fs, false)
	multStr := fs.String("multipliers", "1.0,1.5,2.0,3.0", "Comma-separated slippage multipliers")
	testPen2 := fs.Bool("test-pen2", true, "Also test 2x slippage combined with 2 penetration ticks")
	_ = fs.Parse(args)
	visited := visitedFlags(fs)

	var mults []float64
	for _, p := range strings.Split(*multStr, ",") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(p), 64); err == nil && v > 0 {
			mults = append(mults, v)
		}
	}

	ctx := context.Background()
	common := cf.toRunnerOptions(visited)
	ds, err := runner.PrepareDataset(ctx, common)
	if err != nil {
		slog.Error("Gagal menyiapkan dataset stress", "err", err)
		os.Exit(1)
	}
	defer ds.Close()

	scenarios, md, err := runner.ExecuteStress(ctx, ds, runner.StressOptions{
		CommonOptions: common,
		Multipliers:   mults,
		TestPen2:      *testPen2,
	})
	if err != nil {
		slog.Error("Stress test gagal", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Friction Stress Matrix selesai (%d skenario)\n\n", len(scenarios))
	fmt.Println(md)
}

func handleAblateCommand(args []string) {
	fs := flag.NewFlagSet("ablate", flag.ExitOnError)
	cf := registerCommonFlags(fs, true)
	_ = fs.Parse(args)
	visited := visitedFlags(fs)

	ctx := context.Background()
	common := cf.toRunnerOptions(visited)
	common.SaveToDB = false
	ds, err := runner.PrepareDataset(ctx, common)
	if err != nil {
		slog.Error("Gagal menyiapkan dataset ablasi", "err", err)
		os.Exit(1)
	}
	defer ds.Close()

	fmt.Println("========================================================================================")
	fmt.Println("TRML ABLATION TEST RUNNER (FILTER KNOCKOUT ANALYSIS)")
	fmt.Println("========================================================================================")
	fmt.Printf("| %-38s | %-8s | %-10s | %-12s | %-12s | %-10s |\n",
		"Eksperimen Filter", "Trades", "Win Rate", "Net PnL", "Expectancy", "MTM MaxDD")
	fmt.Println("|:---------------------------------------|:---------|:-----------|:-------------|:-------------|:-----------|")

	type expCase struct {
		name string
		opts runner.RunOptions
	}
	cases := []expCase{
		{name: "Baseline (Semua Filter Aktif)", opts: runner.RunOptions{CommonOptions: common, BTCGateShortOnly: true}},
		{name: "Knockout 1: Tanpa Delta Z-score", opts: runner.RunOptions{CommonOptions: common, BTCGateShortOnly: true, KnockoutDelta: true}},
		{name: "Knockout 2: Tanpa BTC Macro Gate", opts: runner.RunOptions{CommonOptions: common, BTCGateShortOnly: true, KnockoutBTC: true}},
		{name: "Knockout 3: Tanpa RVOL Filter", opts: runner.RunOptions{CommonOptions: common, BTCGateShortOnly: true, KnockoutRVOL: true}},
		{name: "Knockout 4: Tanpa NATR Cap", opts: runner.RunOptions{CommonOptions: common, BTCGateShortOnly: true, KnockoutNATR: true}},
	}

	for _, c := range cases {
		out, err := runner.ExecuteRun(ctx, ds, c.opts)
		if err != nil {
			fmt.Printf("| %-38s | ERROR: %v |\n", c.name, err)
			continue
		}
		s := out.Summary
		fmt.Printf("| %-38s | %-8d | %-8.2f%%  | $%-10.2f | $%-10.2f | -%.2f%% |\n",
			c.name, s.TotalTrades, s.WinRatePct, s.TotalNetPnL, s.ExpectancyUSDT, s.MTMMaxDrawdownPct)
	}
	fmt.Println("========================================================================================")
}

func handleHistoryCommand(args []string) {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	cacheDirFlag := fs.String("cache-dir", filepath.Join("data", "backtest", "cache"), "Cache directory")
	limitFlag := fs.Int("limit", 15, "Number of recent runs to show")
	_ = fs.Parse(args)

	dirs := data.ResolveBacktestDirs(*cacheDirFlag)
	store, err := data.OpenBacktestStore(dirs.DBPath)
	if err != nil {
		slog.Error("Gagal membuka backtest.db", "err", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()

	runs, err := store.ListRecentRuns(context.Background(), *limitFlag)
	if err != nil {
		slog.Error("Gagal query backtest_runs", "err", err)
		os.Exit(1)
	}
	if len(runs) == 0 {
		fmt.Println("Belum ada histori run tersimpan di backtest.db.")
		return
	}
	fmt.Printf("| %-26s | %-11s | %-21s | %-6s | %-8s | %-10s | %-8s |\n",
		"Run ID", "Mode", "Range", "Trades", "WR%", "Net PnL", "MTM DD%")
	fmt.Println("|:---------------------------|:------------|:----------------------|:-------|:---------|:-----------|:---------|")
	for _, r := range runs {
		wrStr := fmt.Sprintf("%.1f%%", r.WinRatePct)
		pnlStr := fmt.Sprintf("$%.2f", r.NetPnLUSDT)
		ddStr := fmt.Sprintf("-%.2f%%", r.MTMMaxDDPct)
		fmt.Printf("| %-26s | %-11s | %s..%s | %-6d | %-8s | %-10s | %-8s |\n",
			r.RunID, r.Mode, r.FromUTC, r.ToUTC, r.TotalTrades, wrStr, pnlStr, ddStr)
	}
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
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 23, 59, 59, 0, time.UTC)
	from := yesterday.AddDate(0, 0, -6)
	from = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	return from, yesterday
}

func getTodayRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	return yesterday, now
}

func getGitCommitHash() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	out, err := cmd.Output()
	if err == nil {
		if h := strings.TrimSpace(string(out)); h != "" {
			return h
		}
	}
	return "8c35d9e"
}

// --- Thin Test-Compatibility Delegates (Logic lives in internal/backtest/runner) ---

func loadConfigForBacktest(envPath string) (*config.Config, error) {
	return runner.LoadConfigForBacktest(envPath)
}

func resolveBacktestScannerConfig(cfg *config.Config, intervalOverride string) (string, time.Duration, scanner.ScannerConfig) {
	return runner.ResolveBacktestScannerConfig(cfg, intervalOverride)
}

func buildSimBrokerConfig(envPath string, equity float64) simbroker.SimBrokerConfig {
	return runner.BuildSimBrokerConfig(envPath, equity)
}

func buildDefaultEvaluatorConfig(envPath string) strategy.EvaluatorConfig {
	return runner.BuildDefaultEvaluatorConfig(envPath)
}

func loadCandlesParallel(
	ctx context.Context,
	downloader *data.Downloader,
	symbols []string,
	interval string,
	from, to time.Time,
	exchangeFilters map[string]model.ExchangeFilter,
) (map[string][]data.HistoricalCandle, map[string]model.ExchangeFilter) {
	return runner.LoadCandlesParallelWithCatalog(ctx, downloader, symbols, interval, from, to, data.CatalogedExchangeInfo{
		ActiveFilters: exchangeFilters,
	}, nil, false)
}

func isCompoundingConfigured(envPath string) bool {
	if os.Getenv("EQUITY_MODE_COMPOUNDING") == "true" {
		return true
	}
	if envPath == "" {
		envPath = "backtest.env"
	}
	raw, err := os.ReadFile(envPath)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(raw), "\n") {
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
