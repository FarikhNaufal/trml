package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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
	"trml/internal/backtest/validation"
	"trml/internal/config"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/strategy"
)

// CommonOptions holds shared CLI/runner parameters across run, walkforward, stress, and ablate.
type CommonOptions struct {
	EnvPath           string
	CacheDir          string
	SymbolsCSV        string
	Interval          string
	ExecTF            string
	OIMode            string
	UniverseMode      string
	From              time.Time
	To                time.Time
	InitialEquity     float64
	Offline           bool
	AllowRESTFallback bool
	DynamicScanner    bool
	ScannerRanking    string
	MaxWatched        int
	WarmupBars        int
	Unconstrained     bool
	Compounding       bool
	CommitHash        string
	BenchmarkSym      string
	SaveToDB          bool

	// Report export paths (optional)
	OutMarkdown  string
	OutTradesCSV string
	OutEquityCSV string
	OutJSON      string
}

// RunOptions configures a single full backtest run (optionally with knockout/override flags).
type RunOptions struct {
	CommonOptions
	RandomSeed                  int64
	RandomProb                  float64
	KnockoutDelta               bool
	KnockoutBTC                 bool
	KnockoutRVOL                bool
	KnockoutNATR                bool
	BTCGateShortOnly            bool
	Tier1EarlyBE                bool
	SLGapFill                   bool
	DeductPendingMargin         bool
	OverrideBTCGateShortOnly    *bool
	OverrideTier1EarlyBE        *bool
	OverrideSLGapFill           *bool
	OverrideDeductPendingMargin *bool
	OverrideCompounding         *bool
}

// WalkForwardOptions configures rolling Walk-Forward validation.
type WalkForwardOptions struct {
	CommonOptions
	TrainDays int
	TestDays  int
	StepDays  int
}

// StressOptions configures escalating friction stress scenarios.
type StressOptions struct {
	CommonOptions
	Multipliers []float64
	TestPen2    bool
}

// LoadedDataset bundles point-in-time universe, OHLCV, optional 1m sub-bars, funding, OI, and SQLite store.
type LoadedDataset struct {
	AppCfg        *config.Config
	Interval      string
	CandleDur     time.Duration
	ExecTF        string
	OIMode        string
	ScannerCfg    scanner.ScannerConfig
	InitialEquity float64
	Symbols       []string
	Catalog       data.CatalogedExchangeInfo
	UniverseRes   data.UniverseResolution
	CandleMap     map[string][]data.HistoricalCandle
	ExecCandles   map[string][]data.HistoricalCandle
	Filters       map[string]model.ExchangeFilter
	FundingMgr    *data.FundingManager
	OIMgr         *data.MetricsOIDownloader
	Store         *data.BacktestStore
	HTTPClient    *data.BacktestHTTPClient
}

// Close releases SQLite connections associated with the loaded dataset.
func (d *LoadedDataset) Close() {
	if d != nil && d.Store != nil {
		_ = d.Store.Close()
		d.Store = nil
	}
}

// RunOutput holds results, metrics, Monte Carlo telemetry, and formatted markdown output.
type RunOutput struct {
	RunID         string
	Summary       metrics.BacktestSummary
	MonteCarlo    validation.MonteCarloResult
	RejectReasons map[string]int64
	Trades        []simbroker.SimTradeRecord
	EquityCurve   []simbroker.EquityPoint
	Markdown      string
	Meta          report.ReportMeta
}

// PrepareDataset loads configuration, universe, klines, optional 1m execution bars, funding, and 5m OI.
func PrepareDataset(ctx context.Context, opts CommonOptions) (*LoadedDataset, error) {
	if opts.CacheDir == "" {
		opts.CacheDir = filepath.Join("data", "backtest", "cache")
	}
	if opts.BenchmarkSym == "" {
		opts.BenchmarkSym = "BTCUSDT"
	}
	if opts.UniverseMode == "" {
		opts.UniverseMode = "pit"
	}
	oiMode := replay.NormalizeOIMode(opts.OIMode)

	appCfg, err := LoadConfigForBacktest(opts.EnvPath)
	if err != nil {
		return nil, fmt.Errorf("failed loading backtest config (%s): %w", opts.EnvPath, err)
	}

	initEq := 125.0
	if appCfg != nil && appCfg.InitialEquity.GreaterThan(decimal.Zero) {
		initEq = appCfg.InitialEquity.InexactFloat64()
	}
	if opts.InitialEquity > 0 {
		initEq = opts.InitialEquity
	}

	interval, candleDur, scannerCfg := ResolveBacktestScannerConfig(appCfg, opts.Interval)
	if opts.ScannerRanking != "" {
		scannerCfg.RankingMode = opts.ScannerRanking
	}
	if opts.MaxWatched > 0 {
		scannerCfg.MaxWatchedCoins = opts.MaxWatched
	}

	dirs := data.ResolveBacktestDirs(opts.CacheDir)
	store, dbErr := data.OpenBacktestStore(dirs.DBPath)
	if dbErr != nil {
		slog.Warn("gagal membuka backtest.db; hasil tidak disimpan ke sqlite", "err", dbErr)
	}

	httpClient := data.NewBacktestHTTPClient(data.BacktestHTTPConfig{AllowRESTFallback: opts.AllowRESTFallback})
	symbols, cat, univRes := ResolveDatasetSymbols(
		ctx, opts.SymbolsCSV, opts.UniverseMode, opts.DynamicScanner,
		opts.Offline, opts.CacheDir, scannerCfg.MinVolume24hUSDT, httpClient, store,
	)

	dl := data.NewDownloader(opts.CacheDir)
	dl.SetHTTPClient(httpClient)
	dl.SetStore(store)
	if opts.Offline {
		dl.SetOffline(true)
	}

	allowDelisted := strings.EqualFold(strings.TrimSpace(opts.UniverseMode), "pit")
	candleMap, filters := LoadCandlesParallelWithCatalog(ctx, dl, symbols, interval, opts.From, opts.To, cat, store, allowDelisted)
	if len(candleMap) == 0 {
		if store != nil {
			_ = store.Close()
		}
		return nil, fmt.Errorf("no candle data available in [%s..%s]", opts.From.Format("2006-01-02"), opts.To.Format("2006-01-02"))
	}

	execTF := strings.ToLower(strings.TrimSpace(opts.ExecTF))
	if execTF == "" {
		execTF = interval
	}
	var execCandles map[string][]data.HistoricalCandle
	if execTF == "1m" && !strings.EqualFold(interval, "1m") {
		execCandles, _ = LoadCandlesParallelWithCatalog(ctx, dl, symbols, "1m", opts.From, opts.To, cat, store, allowDelisted)
		if len(execCandles) == 0 {
			slog.Warn("Sub-bar 1m cache kosong; fallback ke eksekusi bar utama", "interval", interval)
			execCandles = nil
			execTF = interval
		}
	}

	fundingMgr := data.NewFundingManager(opts.CacheDir)
	fundingMgr.SetHTTPClient(httpClient)
	if opts.Offline {
		fundingMgr.SetOffline(true)
	}
	_ = fundingMgr.LoadFundingRates(ctx, symbols, opts.From, opts.To)

	var oiMgr *data.MetricsOIDownloader
	if oiMode != replay.OIModeOff {
		oiMgr = data.NewMetricsOIDownloader(opts.CacheDir, httpClient, store)
		if opts.Offline {
			oiMgr.SetOffline(true)
		}
		if oiErr := oiMgr.LoadOpenInterestRange(ctx, symbols, opts.From, opts.To); oiErr != nil {
			slog.Warn("Open Interest 5m load incomplete", "mode", oiMode, "err", oiErr)
		}
	}

	return &LoadedDataset{
		AppCfg:        appCfg,
		Interval:      interval,
		CandleDur:     candleDur,
		ExecTF:        execTF,
		OIMode:        oiMode,
		ScannerCfg:    scannerCfg,
		InitialEquity: initEq,
		Symbols:       symbols,
		Catalog:       cat,
		UniverseRes:   univRes,
		CandleMap:     candleMap,
		ExecCandles:   execCandles,
		Filters:       filters,
		FundingMgr:    fundingMgr,
		OIMgr:         oiMgr,
		Store:         store,
		HTTPClient:    httpClient,
	}, nil
}

// ExecuteRun executes a complete single-window Backtest v2 pipeline, persists to backtest.db, and exports reports.
func ExecuteRun(ctx context.Context, ds *LoadedDataset, opts RunOptions) (*RunOutput, error) {
	if ds == nil {
		return nil, fmt.Errorf("nil dataset")
	}

	evalCfg, brokerCfg, replayCfg := BuildEngineConfigs(ds, opts)
	sim := simbroker.NewSimBroker(brokerCfg)
	eng := replay.NewReplayEngine(replayCfg, evalCfg, sim, ds.CandleMap, ds.Filters)

	if err := eng.Run(ctx); err != nil {
		return nil, err
	}

	execAudit := BuildExecutionAudit(sim.Stats, eng.Stats)
	scanAudit := BuildScannerAudit(eng.Stats.ScannerStats)
	summary := metrics.CalculateMetricsV2(
		sim.ClosedTrades, sim.EquityCurve, ds.InitialEquity,
		ds.CandleMap[ds.getBenchmarkSym(opts.BenchmarkSym)], scanAudit, execAudit,
	)

	mc := validation.RunMonteCarlo(validation.ExtractAggregatedPnLs(summary), ds.InitialEquity, 1000, 42)
	runID := generateRunID("run")

	meta := buildReportMeta(runID, ds, opts.CommonOptions, brokerCfg, evalCfg, execAudit, sim.ClosedTrades, &mc, nil, nil)
	md := report.GenerateMarkdownReport(summary, meta, eng.RejectReasons)

	out := &RunOutput{
		RunID:         runID,
		Summary:       summary,
		MonteCarlo:    mc,
		RejectReasons: eng.RejectReasons,
		Trades:        sim.ClosedTrades,
		EquityCurve:   sim.EquityCurve,
		Markdown:      md,
		Meta:          meta,
	}

	if opts.SaveToDB && ds.Store != nil {
		if err := persistRunResult(ctx, ds.Store, runID, "run", ds, opts.CommonOptions, brokerCfg.SLGapFillEnabled, replayCfg.DeductPendingMargin, summary, sim.ClosedTrades, sim.EquityCurve, nil); err != nil {
			slog.Warn("gagal menyimpan run ke backtest.db", "run_id", runID, "err", err)
		}
	}

	if err := exportArtifacts(opts.CommonOptions, md, report.JSONReportPayload{
		RunID:         runID,
		CommitHash:    meta.CommitHash,
		DataRangeFrom: meta.DataRangeFrom,
		DataRangeTo:   meta.DataRangeTo,
		Summary:       summary,
		RejectReasons: eng.RejectReasons,
		MonteCarlo:    &mc,
		Trades:        sim.ClosedTrades,
		EquityCurve:   sim.EquityCurve,
	}, sim.ClosedTrades, sim.EquityCurve); err != nil {
		return out, err
	}
	return out, nil
}

// ExecuteWalkForward runs rolling IS/OOS validation, saves folds to backtest.db, and exports reports.
func ExecuteWalkForward(ctx context.Context, ds *LoadedDataset, opts WalkForwardOptions) (*validation.WalkForwardReport, string, error) {
	if ds == nil {
		return nil, "", fmt.Errorf("nil dataset")
	}
	btcShortOnly := ds.AppCfg == nil || ds.AppCfg.BTCGateShortOnly
	runOpts := RunOptions{CommonOptions: opts.CommonOptions, BTCGateShortOnly: btcShortOnly}
	evalCfg, brokerCfg, replayCfg := BuildEngineConfigs(ds, runOpts)

	bench := ds.getBenchmarkSym(opts.BenchmarkSym)
	wfaRes, err := validation.RunWalkForward(ctx, validation.WalkForwardConfig{
		TrainDays:     opts.TrainDays,
		TestDays:      opts.TestDays,
		StepDays:      opts.StepDays,
		From:          opts.From,
		To:            opts.To,
		InitialEquity: ds.InitialEquity,
		BenchmarkSym:  bench,
		ReplayCfg:     replayCfg,
		EvalCfg:       evalCfg,
		BrokerCfg:     brokerCfg,
		CandleMap:     ds.CandleMap,
		Filters:       ds.Filters,
	})
	if err != nil {
		return nil, "", err
	}

	mc := validation.RunMonteCarlo(validation.ExtractAggregatedPnLs(wfaRes.StitchedOOSSummary), ds.InitialEquity, 1000, 42)
	runID := generateRunID("wfa")

	var storedFolds []data.WalkForwardFoldRecord
	for _, f := range wfaRes.Folds {
		storedFolds = append(storedFolds, data.WalkForwardFoldRecord{
			FoldIndex:      f.FoldIndex,
			IsFromUTC:      f.ISFrom.UTC().Format("2006-01-02"),
			IsToUTC:        f.ISTo.UTC().Format("2006-01-02"),
			OOSFromUTC:     f.OOSFrom.UTC().Format("2006-01-02"),
			OOSToUTC:       f.OOSTo.UTC().Format("2006-01-02"),
			ISExpectancy:   f.ISSummary.ExpectancyUSDT,
			OOSExpectancy:  f.OOSSummary.ExpectancyUSDT,
			DegradationPct: f.DegradationPct,
			Verdict:        f.Verdict,
		})
	}
	if opts.SaveToDB && ds.Store != nil {
		if err := persistRunResult(ctx, ds.Store, runID, "walkforward", ds, opts.CommonOptions, brokerCfg.SLGapFillEnabled, replayCfg.DeductPendingMargin, wfaRes.StitchedOOSSummary, nil, wfaRes.StitchedOOSCurve, storedFolds); err != nil {
			slog.Warn("gagal menyimpan walkforward run ke backtest.db", "run_id", runID, "err", err)
		}
	}

	meta := buildReportMeta(runID, ds, opts.CommonOptions, brokerCfg, evalCfg, wfaRes.StitchedOOSSummary.ExecutionAudit, nil, &mc, nil, &wfaRes)
	md := report.GenerateMarkdownReport(wfaRes.StitchedOOSSummary, meta, nil)

	if err := exportArtifacts(opts.CommonOptions, md, report.JSONReportPayload{
		RunID:         runID,
		CommitHash:    meta.CommitHash,
		DataRangeFrom: meta.DataRangeFrom,
		DataRangeTo:   meta.DataRangeTo,
		Summary:       wfaRes.StitchedOOSSummary,
		MonteCarlo:    &mc,
		WalkForward:   &wfaRes,
		EquityCurve:   wfaRes.StitchedOOSCurve,
	}, nil, wfaRes.StitchedOOSCurve); err != nil {
		return &wfaRes, md, err
	}
	return &wfaRes, md, nil
}

// ExecuteStress runs the friction stress matrix across slippage multipliers and generates a report.
func ExecuteStress(ctx context.Context, ds *LoadedDataset, opts StressOptions) ([]validation.StressScenario, string, error) {
	if ds == nil {
		return nil, "", fmt.Errorf("nil dataset")
	}
	btcShortOnly := ds.AppCfg == nil || ds.AppCfg.BTCGateShortOnly
	runOpts := RunOptions{CommonOptions: opts.CommonOptions, BTCGateShortOnly: btcShortOnly}
	evalCfg, brokerCfg, replayCfg := BuildEngineConfigs(ds, runOpts)
	bench := ds.getBenchmarkSym(opts.BenchmarkSym)

	scenarios, err := validation.RunFrictionStressMatrix(ctx, validation.StressConfig{
		BaseReplayCfg: replayCfg,
		EvalCfg:       evalCfg,
		BaseBrokerCfg: brokerCfg,
		CandleMap:     ds.CandleMap,
		Filters:       ds.Filters,
		BenchmarkSym:  bench,
		Multipliers:   opts.Multipliers,
		TestPen2:      opts.TestPen2,
	})
	if err != nil {
		return nil, "", err
	}

	baseSummary := metrics.BacktestSummary{}
	if len(scenarios) > 0 {
		baseSummary = scenarios[0].Summary
	}
	runID := generateRunID("stress")
	meta := buildReportMeta(runID, ds, opts.CommonOptions, brokerCfg, evalCfg, baseSummary.ExecutionAudit, nil, nil, scenarios, nil)
	md := report.GenerateMarkdownReport(baseSummary, meta, nil)

	if err := exportArtifacts(opts.CommonOptions, md, report.JSONReportPayload{
		RunID:           runID,
		CommitHash:      meta.CommitHash,
		DataRangeFrom:   meta.DataRangeFrom,
		DataRangeTo:     meta.DataRangeTo,
		Summary:         baseSummary,
		StressScenarios: scenarios,
	}, nil, nil); err != nil {
		return scenarios, md, err
	}
	return scenarios, md, nil
}

// BuildEngineConfigs translates dataset & run flags into aligned EvaluatorConfig, SimBrokerConfig, and ReplayConfig.
func BuildEngineConfigs(ds *LoadedDataset, opts RunOptions) (strategy.EvaluatorConfig, simbroker.SimBrokerConfig, replay.ReplayConfig) {
	evalCfg := BuildDefaultEvaluatorConfig(opts.EnvPath)
	if ds.AppCfg != nil && (opts.EnvPath == "" || ds.AppCfg.Sweep.Enabled || len(ds.AppCfg.BlockedUTCHours) > 0 || ds.AppCfg.BTCChopBlockShort || ds.AppCfg.BTCChopRiskMult != 1.0 || ds.AppCfg.MaxSlots > 0) {
		appEval := strategy.FromAppConfig(ds.AppCfg)
		if appEval.Sweep.Enabled {
			evalCfg.Sweep = appEval.Sweep
		}
		if len(appEval.BlockedUTCHours) > 0 {
			evalCfg.BlockedUTCHours = appEval.BlockedUTCHours
		}
		if appEval.BTCChopBlockShort {
			evalCfg.BTCChopBlockShort = true
		}
		if appEval.BTCChopRiskMult > 0 && appEval.BTCChopRiskMult < 1.0 {
			evalCfg.BTCChopRiskMult = appEval.BTCChopRiskMult
		}
		if appEval.MaxSlots > 0 {
			evalCfg.MaxSlots = appEval.MaxSlots
		}
		evalCfg.BTCGateShortOnly = appEval.BTCGateShortOnly
	}
	evalCfg.CandleDuration = ds.CandleDur
	evalCfg.DisableOIFilter = ds.OIMode == replay.OIModeOff

	if opts.RandomSeed != 0 || opts.RandomProb > 0 {
		evalCfg.RandomEntrySeed = opts.RandomSeed
		evalCfg.RandomEntryProb = opts.RandomProb
	}
	if opts.KnockoutDelta {
		evalCfg.MinBounceZScore = -999.0
		evalCfg.MinBounceZScoreLong = -999.0
		evalCfg.MinBounceZScoreShort = -999.0
	}
	if opts.KnockoutBTC {
		evalCfg.BTCGateEnabled = false
		evalCfg.BTCChopBlockLong = false
		evalCfg.BTCChopBlockShort = false
		evalCfg.BTCChopRiskMult = 1.0
	}
	if opts.KnockoutRVOL {
		evalCfg.MinSignalRVOL = 0.0
		evalCfg.MaxFrenzyRVOL = 999.0
	}
	if opts.KnockoutNATR {
		evalCfg.MinNATRPct = 0.0
		evalCfg.MinNATRLongPct = 0.0
		evalCfg.MaxNATRPct = 999.0
	}
	if opts.OverrideBTCGateShortOnly != nil {
		evalCfg.BTCGateShortOnly = *opts.OverrideBTCGateShortOnly
	} else if ds.AppCfg == nil && opts.BTCGateShortOnly {
		evalCfg.BTCGateShortOnly = true
	}

	brokerCfg := BuildSimBrokerConfig(opts.EnvPath, ds.InitialEquity)
	if ds.AppCfg != nil && ds.AppCfg.MaxSlots > 0 {
		brokerCfg.MaxSlots = ds.AppCfg.MaxSlots
	}
	if opts.OverrideCompounding != nil {
		brokerCfg.EquityModeCompounding = *opts.OverrideCompounding
	} else if opts.Compounding || (ds.AppCfg != nil && ds.AppCfg.EquityModeCompounding) {
		brokerCfg.EquityModeCompounding = true
	}
	if opts.OverrideTier1EarlyBE != nil {
		brokerCfg.Tier1BEEnabled = *opts.OverrideTier1EarlyBE
	} else if opts.Tier1EarlyBE {
		brokerCfg.Tier1BEEnabled = true
	}
	if opts.OverrideSLGapFill != nil {
		brokerCfg.SLGapFillEnabled = *opts.OverrideSLGapFill
	} else if opts.SLGapFill || (ds.AppCfg != nil && ds.AppCfg.SLGapFillEnabled) {
		brokerCfg.SLGapFillEnabled = true
	}
	if opts.Unconstrained {
		brokerCfg.MaxSlots = 1000
		brokerCfg.MaxDailyDrawdownPct = -1.0
		evalCfg.MaxSlots = 1000
		evalCfg.CooldownAfterExitMinutes = 0
		evalCfg.CooldownLossMinutes = 0
		evalCfg.CooldownProfitMinutes = 0
	}

	warmup := opts.WarmupBars
	if warmup <= 0 {
		warmup = 200
	}
	scanInterval := 15 * time.Minute
	if ds.AppCfg != nil && ds.AppCfg.ScannerInterval > 0 {
		scanInterval = ds.AppCfg.ScannerInterval
	}

	deductPending := opts.DeductPendingMargin || (ds.AppCfg != nil && ds.AppCfg.DeductPendingMargin)
	if opts.OverrideDeductPendingMargin != nil {
		deductPending = *opts.OverrideDeductPendingMargin
	}

	replayCfg := replay.ReplayConfig{
		WarmupBars:            warmup,
		SpreadPct:             evalCfg.MaxSpreadPct,
		FundingRate:           0.0001,
		FundingMgr:            ds.FundingMgr,
		OIMode:                ds.OIMode,
		OIMgr:                 ds.OIMgr,
		ExecCandleMap:         ds.ExecCandles,
		BenchmarkSym:          ds.getBenchmarkSym(opts.BenchmarkSym),
		DynamicScannerEnabled: opts.DynamicScanner,
		ScannerInterval:       scanInterval,
		ScannerCfg:            ds.ScannerCfg,
		UnconstrainedMode:     opts.Unconstrained,
		DeductPendingMargin:   deductPending,
		EquityModeCompounding: brokerCfg.EquityModeCompounding,
	}
	return evalCfg, brokerCfg, replayCfg
}

// BuildExecutionAudit adapts broker & replay telemetry into metrics.ExecutionAudit.
func BuildExecutionAudit(bStats simbroker.SimExecutionStats, rStats replay.ReplayStats) *metrics.ExecutionAudit {
	return &metrics.ExecutionAudit{
		SimExecutionStats: bStats,
		SubBarExecCount:   rStats.SubBarExecCount,
		DelistForceClosed: rStats.DelistForceClosed,
		OICoveredCount:    rStats.OICoveredCount,
		OIFallbackCount:   rStats.OIFallbackCount,
		OIMissingCount:    rStats.OIMissingCount,
		OICoveragePct:     rStats.OICoveragePct,
	}
}

// BuildScannerAudit adapts replay scanner telemetry into metrics.ScannerAudit.
func BuildScannerAudit(s replay.ScannerAuditStats) *metrics.ScannerAudit {
	return &metrics.ScannerAudit{
		CyclesRun:          s.CyclesRun,
		FallbackCycles:     s.FallbackCycles,
		AvgCandidatesValid: s.AvgCandidatesValid,
		AvgSelectedCoins:   s.AvgSelectedCoins,
		WatchlistTurnover:  s.WatchlistTurnover,
		TotalVolRejected:   s.TotalVolRejected,
		TotalSpreadReject:  s.TotalSpreadReject,
		TotalNATRReject:    s.TotalNATRReject,
		TotalVWAPReject:    s.TotalVWAPReject,
	}
}

func (d *LoadedDataset) getBenchmarkSym(override string) string {
	if override != "" {
		return override
	}
	return "BTCUSDT"
}

func generateRunID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%s", prefix, time.Now().UTC().Format("20060102-150405"), hex.EncodeToString(b[:]))
}

func buildReportMeta(
	runID string,
	ds *LoadedDataset,
	opts CommonOptions,
	brokerCfg simbroker.SimBrokerConfig,
	evalCfg strategy.EvaluatorConfig,
	execAudit *metrics.ExecutionAudit,
	trades []simbroker.SimTradeRecord,
	mc *validation.MonteCarloResult,
	stress []validation.StressScenario,
	wfa *validation.WalkForwardReport,
) report.ReportMeta {
	makerBps := evalCfg.Sizing.MakerFeeRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	takerBps := evalCfg.Sizing.TakerFeeRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	slipBps := evalCfg.Sizing.SlippageRate.Mul(decimal.NewFromFloat(10000)).InexactFloat64()
	feeModel := fmt.Sprintf("Maker %.1f bps, Taker %.1f bps, Slippage %.1f bps | Real Binance Funding & Dynamic Spread", makerBps, takerBps, slipBps)

	execMode := fmt.Sprintf("%s (%s step), Penetration %d Tick, Intrabar SL-First, Repeg=%d, Timeout=%s",
		brokerCfg.EntryOrderType, ds.ExecTF, brokerCfg.PenetrationTicks, brokerCfg.MaxEntryRepegAttempts, brokerCfg.LimitOrderTimeout)
	if opts.Unconstrained {
		execMode = "UNCONSTRAINED SIGNAL EVALUATION (1 R flat, unlimited concurrent slots, 0 cooldown)"
	}

	oiCov := 0.0
	if execAudit != nil {
		oiCov = execAudit.OICoveragePct
	}
	univInfo := fmt.Sprintf("mode=%s | %d loaded (%d active, %d delisted, %d TradFi blocklisted)",
		opts.UniverseMode, len(ds.CandleMap), ds.UniverseRes.ActiveCryptoCount, ds.UniverseRes.DelistedCount, ds.UniverseRes.BlocklistedTradFi)

	return report.ReportMeta{
		RunID:              runID,
		CommitHash:         opts.CommitHash,
		DataRangeFrom:      opts.From.UTC().Format("2006-01-02"),
		DataRangeTo:        opts.To.UTC().Format("2006-01-02"),
		Symbols:            ds.Symbols,
		UniverseDisclosure: univInfo,
		ExecutionMode:      execMode,
		FeeModel:           feeModel,
		FeaturesMode:       fmt.Sprintf("Kline %s (Exec %s) + Exact Net Bar Delta + OI %s (%.1f%% cov) + Real Binance Filters & Funding", ds.Interval, ds.ExecTF, ds.OIMode, oiCov),
		Trades:             trades,
		MonteCarlo:         mc,
		StressScenarios:    stress,
		WalkForward:        wfa,
	}
}

func persistRunResult(
	ctx context.Context,
	store *data.BacktestStore,
	runID, mode string,
	ds *LoadedDataset,
	opts CommonOptions,
	slGapFill, deductPendingMargin bool,
	summary metrics.BacktestSummary,
	trades []simbroker.SimTradeRecord,
	curve []simbroker.EquityPoint,
	folds []data.WalkForwardFoldRecord,
) error {
	maxSlots := 0
	btcChopBlockShort := false
	btcChopRiskMult := 1.0
	var blockedUTCHours []int
	if ds.AppCfg != nil {
		maxSlots = ds.AppCfg.MaxSlots
		btcChopBlockShort = ds.AppCfg.BTCChopBlockShort
		btcChopRiskMult = ds.AppCfg.BTCChopRiskMult
		for h := 0; h < 24; h++ {
			if ds.AppCfg.BlockedUTCHours[h] {
				blockedUTCHours = append(blockedUTCHours, h)
			}
		}
	}
	cfgBytes, _ := json.Marshal(map[string]any{
		"interval":              ds.Interval,
		"exec_tf":               ds.ExecTF,
		"oi_mode":               ds.OIMode,
		"universe":              opts.UniverseMode,
		"compounding":           opts.Compounding,
		"symbols_cnt":           len(ds.Symbols),
		"max_slots":             maxSlots,
		"sweep_enabled":         ds.AppCfg != nil && ds.AppCfg.Sweep.Enabled,
		"sweep_ext_vwap":        ds.AppCfg != nil && ds.AppCfg.Sweep.ScannerAllowExtendedVWAP,
		"blocked_utc_hours":     blockedUTCHours,
		"btc_chop_block_short":  btcChopBlockShort,
		"btc_chop_risk_mult":    btcChopRiskMult,
		"sl_gap_fill":           slGapFill,
		"deduct_pending_margin": deductPendingMargin,
	})
	oiCov := 0.0
	if summary.ExecutionAudit != nil {
		oiCov = summary.ExecutionAudit.OICoveragePct
	}
	runRec := data.BacktestRunRecord{
		RunID:         runID,
		CommitHash:    opts.CommitHash,
		Mode:          mode,
		FromUTC:       opts.From.UTC().Format("2006-01-02"),
		ToUTC:         opts.To.UTC().Format("2006-01-02"),
		ExecTF:        ds.ExecTF,
		OIMode:        ds.OIMode,
		UniverseMode:  opts.UniverseMode,
		TotalTrades:   summary.TotalTrades,
		WinRatePct:    summary.WinRatePct,
		NetPnLUSDT:    summary.TotalNetPnL,
		ProfitFactor:  summary.ProfitFactor,
		MTMMaxDDPct:   summary.MTMMaxDrawdownPct,
		ExpectancyUSD: summary.ExpectancyUSDT,
		OICoveragePct: oiCov,
		ConfigJSON:    string(cfgBytes),
	}

	storedTrades := make([]data.StoredRunTrade, 0, len(trades))
	for _, tr := range trades {
		storedTrades = append(storedTrades, data.StoredRunTrade{
			Symbol:      tr.Symbol,
			Side:        string(tr.Side),
			EntryTimeMs: tr.EntryTime.UnixMilli(),
			ExitTimeMs:  tr.ExitTime.UnixMilli(),
			EntryPrice:  tr.EntryPrice.InexactFloat64(),
			ExitPrice:   tr.ExitPrice.InexactFloat64(),
			Qty:         tr.Qty.InexactFloat64(),
			NetPnLUSDT:  tr.NetPnL.InexactFloat64(),
			FeeUSDT:     tr.FeeTotal.InexactFloat64(),
			FundingUSDT: tr.FundingTotal.InexactFloat64(),
			ExitReason:  tr.ExitReason,
		})
	}
	storedCurve := make([]data.StoredEquityPoint, 0, len(curve))
	for _, pt := range curve {
		storedCurve = append(storedCurve, data.StoredEquityPoint{
			TimeMs:          pt.Time.UnixMilli(),
			RealizedEquity:  pt.RealizedEquity,
			EffectiveEquity: pt.EffectiveEquity,
			UsedMargin:      pt.UsedMargin,
			OpenPositions:   pt.OpenPositions,
		})
	}
	return store.SaveBacktestRun(ctx, runRec, storedTrades, storedCurve, folds)
}

func exportArtifacts(
	opts CommonOptions,
	md string,
	jsonPayload report.JSONReportPayload,
	trades []simbroker.SimTradeRecord,
	curve []simbroker.EquityPoint,
) error {
	if opts.OutMarkdown != "" {
		if err := os.MkdirAll(filepath.Dir(opts.OutMarkdown), 0755); err != nil {
			return err
		}
		if err := data.WriteFileAtomic(opts.OutMarkdown, []byte(md), 0644); err != nil {
			return err
		}
	}
	if opts.OutTradesCSV != "" {
		if err := report.WriteTradesCSV(opts.OutTradesCSV, trades); err != nil {
			return err
		}
	}
	if opts.OutEquityCSV != "" {
		if err := report.WriteEquityCurveCSV(opts.OutEquityCSV, curve); err != nil {
			return err
		}
	}
	if opts.OutJSON != "" {
		if err := report.WriteJSONReport(opts.OutJSON, jsonPayload); err != nil {
			return err
		}
	}
	return nil
}

// --- Shared Config & Universe Loaders (reused by cmd/backtest & tests) ---

// LoadConfigForBacktest safely loads config/env files even when live trading API keys are absent.
func LoadConfigForBacktest(envPath string) (*config.Config, error) {
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

// ResolveBacktestScannerConfig determines active interval, bar duration, and ScannerConfig.
func ResolveBacktestScannerConfig(cfg *config.Config, intervalOverride string) (string, time.Duration, scanner.ScannerConfig) {
	interval := "5m"
	if cfg != nil && cfg.Timeframe != "" {
		interval = cfg.Timeframe
	}
	if intervalOverride != "" {
		interval = intervalOverride
	}
	candleDur, tfErr := config.ParseTimeframeToDuration(interval)
	if tfErr != nil || candleDur <= 0 {
		candleDur = 5 * time.Minute
	}
	scannerCfg := scanner.FromAppConfig(cfg, 250)
	scannerCfg.Timeframe = interval
	scannerCfg.CandleDuration = candleDur
	return interval, candleDur, scannerCfg
}

// SplitSymbols parses comma/newline-delimited symbols or a symbol list file (.txt/.csv).
func SplitSymbols(s string) []string {
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
		if c := strings.TrimSpace(strings.ToUpper(p)); c != "" {
			res = append(res, c)
		}
	}
	return res
}

// EnsureBenchmarkSymbol guarantees BTCUSDT is included in the dataset for macro regime gates.
func EnsureBenchmarkSymbol(symbols []string) []string {
	for _, s := range symbols {
		if s == "BTCUSDT" {
			return symbols
		}
	}
	return append([]string{"BTCUSDT"}, symbols...)
}

// ResolveDatasetSymbols resolves point-in-time historical, cached, or explicit symbol lists.
func ResolveDatasetSymbols(
	ctx context.Context,
	symbolsFlag, universeMode string,
	scannerEnabled, offline bool,
	cacheDir string,
	minVolume24h float64,
	httpClient *data.BacktestHTTPClient,
	store *data.BacktestStore,
) ([]string, data.CatalogedExchangeInfo, data.UniverseResolution) {
	var cat data.CatalogedExchangeInfo
	if offline {
		cat, _ = data.LoadExchangeFiltersCatalogOffline(cacheDir)
	} else {
		cat, _ = data.LoadExchangeFiltersCatalog(ctx, cacheDir, httpClient, false)
	}

	var univRes data.UniverseResolution
	var symbols []string

	if strings.TrimSpace(symbolsFlag) != "" {
		symbols = SplitSymbols(symbolsFlag)
	} else if !scannerEnabled {
		symbols = []string{"SOLUSDT", "DOGEUSDT", "NEARUSDT"}
	} else {
		switch strings.ToLower(strings.TrimSpace(universeMode)) {
		case "cache":
			var err error
			symbols, err = data.DiscoverMarketSymbolsOffline(cacheDir)
			if err != nil {
				symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
			}
		case "legacy-ticker":
			if offline {
				symbols, _ = data.DiscoverMarketSymbolsOffline(cacheDir)
			} else {
				symbols, _ = data.DiscoverMarketSymbols(ctx, cacheDir, minVolume24h)
			}
		default: // "pit" (Point-in-Time S3 historical universe)
			var err error
			univRes, err = data.DiscoverPointInTimeUniverse(ctx, cacheDir, httpClient, store, offline)
			if err == nil && len(univRes.Symbols) > 0 {
				symbols = univRes.Symbols
			} else {
				symbols, err = data.DiscoverMarketSymbolsOffline(cacheDir)
				if err != nil {
					symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
				}
			}
		}
	}

	symbols = EnsureBenchmarkSymbol(symbols)
	return symbols, cat, univRes
}

// LoadCandlesParallelWithCatalog loads historical OHLCV bars and exchange filters concurrently.
func LoadCandlesParallelWithCatalog(
	ctx context.Context,
	downloader *data.Downloader,
	symbols []string,
	interval string,
	from, to time.Time,
	cat data.CatalogedExchangeInfo,
	store *data.BacktestStore,
	allowInferredDelisted bool,
) (map[string][]data.HistoricalCandle, map[string]model.ExchangeFilter) {
	candleMap := make(map[string][]data.HistoricalCandle)
	filters := make(map[string]model.ExchangeFilter)
	toPersist := make(map[string]data.StoredFilterMeta)
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

	numWorkers := 8
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
				if cat.Blocklisted != nil && cat.Blocklisted[sym] {
					atomic.AddInt32(&completed, 1)
					continue
				}
				_, hasActive := cat.ActiveFilters[sym]
				if len(cat.ActiveFilters) > 0 && !hasActive && !allowInferredDelisted {
					atomic.AddInt32(&completed, 1)
					continue
				}

				candles, err := downloader.FetchCandleRange(ctx, sym, interval, from, to)
				atomic.AddInt32(&completed, 1)
				if err != nil || len(candles) == 0 {
					continue
				}

				var ef model.ExchangeFilter
				isInferred := false
				src := "exchange_info"
				if f, ok := cat.ActiveFilters[sym]; ok {
					ef = f
				} else if allowInferredDelisted {
					ef = data.InferFilterFromCandles(candles)
					isInferred = true
					src = "inferred_ohlcv"
				} else {
					ef = model.ExchangeFilter{
						TickSize:    decimal.NewFromFloat(0.01),
						StepSize:    decimal.NewFromFloat(0.1),
						MinNotional: decimal.NewFromFloat(5.0),
						MinQty:      decimal.NewFromFloat(0.1),
					}
					isInferred = true
					src = "default_fallback"
				}

				mu.Lock()
				candleMap[sym] = candles
				filters[sym] = ef
				toPersist[sym] = data.StoredFilterMeta{
					Filter:   ef,
					Inferred: isInferred,
					Source:   src,
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if store != nil && len(toPersist) > 0 {
		_ = store.UpsertExchangeFilters(ctx, toPersist)
	}
	return candleMap, filters
}

// BuildSimBrokerConfig translates env/config values into SimBrokerConfig with full Live parity.
func BuildSimBrokerConfig(envPath string, equity float64) simbroker.SimBrokerConfig {
	cfg, err := LoadConfigForBacktest(envPath)
	if err != nil || cfg == nil {
		eqDec := decimal.NewFromFloat(125.0)
		if equity > 0 {
			eqDec = decimal.NewFromFloat(equity)
		}
		return simbroker.SimBrokerConfig{
			InitialEquityUSDT:        eqDec,
			MaxSlots:                 3,
			MaxDailyDrawdownPct:      -0.05,
			AccountLeverage:          8.0,
			EntryOrderType:           "LIMIT",
			MakerFeeRate:             decimal.NewFromFloat(0.0002),
			TakerFeeRate:             decimal.NewFromFloat(0.0005),
			SlippageRate:             decimal.NewFromFloat(0.0002),
			SignalDelayBars:          1,
			LimitTimeoutBars:         1,
			LimitOrderTimeout:        150 * time.Second,
			PenetrationTicks:         1,
			IntrabarConflictSLFirst:  true,
			PartialTPEnabled:         true,
			PartialTP1Ratio:          0.50,
			PartialTP1ATRMult:        1.00,
			Tier1BEEnabled:           true,
			Tier1BETriggerLongATR:    0.70,
			Tier1BETriggerShortATR:   0.90,
			MinBreakEvenOffsetPct:    0.0010,
			Tier2ProfitLockATR:       1.55,
			Tier2ProfitLockOffsetATR: 0.75,
			TrailingEnabled:          true,
			TrailingActivateATR:      1.10,
			TrailingDistanceATR:      0.55,
			MaxHoldingDuration:       4 * time.Hour,
			EquityModeCompounding:    false,
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

	entryOrderType := cfg.EntryOrderType
	if entryOrderType == "" {
		entryOrderType = "LIMIT"
	}
	limitTimeout := cfg.LimitOrderTimeout
	if limitTimeout <= 0 {
		limitTimeout = 150 * time.Second
	}

	return simbroker.SimBrokerConfig{
		InitialEquityUSDT:        eqDec,
		MaxSlots:                 cfg.MaxSlots,
		MaxDailyDrawdownPct:      maxDD,
		AccountLeverage:          cfg.Leverage,
		EntryOrderType:           entryOrderType,
		MakerFeeRate:             cfg.MakerFeeRate,
		TakerFeeRate:             cfg.TakerFeeRate,
		SlippageRate:             cfg.SlippageRate,
		SLGapFillEnabled:         cfg.SLGapFillEnabled,
		SignalDelayBars:          sigDelay,
		LimitTimeoutBars:         1,
		LimitOrderTimeout:        limitTimeout,
		MaxEntryRepegAttempts:    cfg.EntryRepegMaxAttempts,
		MaxEntryRepegDriftPct:    cfg.EntryRepegMaxDriftPct,
		PenetrationTicks:         penTicks,
		IntrabarConflictSLFirst:  true,
		PartialTPEnabled:         cfg.PartialTPEnabled,
		PartialTP1Ratio:          pRatio,
		PartialTP1ATRMult:        tp1Mult,
		Tier1BEEnabled:           cfg.Tier1BEEnabled,
		Tier1BETriggerLongATR:    cfg.Tier1BETriggerLongATR,
		Tier1BETriggerShortATR:   cfg.Tier1BETriggerShortATR,
		MinBreakEvenOffsetPct:    0.0010,
		Tier2ProfitLockEnabled:   cfg.Tier2ProfitLockEnabled,
		Tier2ProfitLockATR:       cfg.Tier2ProfitLockATR,
		Tier2ProfitLockOffsetATR: cfg.Tier2ProfitLockOffsetATR,
		RunnerBECushionATR:       cfg.RunnerBECushionATR,
		TrailingEnabled:          cfg.TrailingEnabled,
		TrailingActivateATR:      trailAct,
		TrailingDistanceATR:      trailDist,
		MaxHoldingDuration:       maxHold,
		EquityModeCompounding:    cfg.EquityModeCompounding,
	}
}

// BuildDefaultEvaluatorConfig builds EvaluatorConfig aligned with config/env.
func BuildDefaultEvaluatorConfig(envPath string) strategy.EvaluatorConfig {
	cfg, err := LoadConfigForBacktest(envPath)
	if err != nil {
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
			DisableOIFilter:          false,
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

	evalCfg := strategy.FromAppConfig(cfg)
	maxDD := cfg.MaxDailyDrawdown
	if maxDD >= 0 {
		maxDD = -0.05
	}
	evalCfg.MaxDailyDrawdownPct = maxDD

	if seedStr := os.Getenv("RANDOM_ENTRY_SEED"); seedStr != "" {
		if s, err := strconv.ParseInt(seedStr, 10, 64); err == nil {
			evalCfg.RandomEntrySeed = s
		}
	}
	if probStr := os.Getenv("RANDOM_ENTRY_PROB"); probStr != "" {
		if p, err := strconv.ParseFloat(probStr, 64); err == nil {
			evalCfg.RandomEntryProb = p
		}
	}

	return evalCfg
}
