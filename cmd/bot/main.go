package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker"
	"trml/internal/config"
	"trml/internal/exchange"
	"trml/internal/indicator"
	"trml/internal/model"
	"trml/internal/scanner"
	"trml/internal/storage"
	"trml/internal/strategy"
	"trml/internal/web"
	"trml/internal/ws"
)

const BenchmarkSymbol = "BTCUSDT"

type BotEngine struct {
	cfg          *config.Config
	repo         storage.Repository
	client       *exchange.Client
	broker       broker.ExecutionBroker
	dryRunBroker *broker.DryRunBroker
	liveBroker   *broker.BinanceLiveBroker

	statesMu               sync.RWMutex
	activeStates           map[string]*model.SymbolState
	watchedCoins           []string
	primaryWatchedSet      map[string]bool
	breakoutWatchedSet     map[string]bool
	sweepExtraWatchedSet   map[string]bool
	majorPodSymbols        []string
	majorPodSet            map[string]bool
	lastStreamSymbols      []string
	hasCompletedScan       bool
	lastScanStats          scanner.ScanStats
	lastScanAt             time.Time
	hasScanStats           bool
	marketBreadthPct       float64
	marketBreadthValid     bool
	marketBreadthUpdatedAt time.Time

	filtersMu       sync.RWMutex
	exchangeFilters map[string]model.ExchangeFilter

	cooldownMu         sync.RWMutex
	cooldownExits      map[string]storage.RecentExitRecord
	majorCooldownExits map[string]storage.RecentExitRecord

	clusterSL  *strategy.ClusterSLTracker
	barEntries *strategy.BarEntryTracker

	btcSyncMu      sync.Mutex
	btcBarCh       chan struct{}
	lastBTCOpen    time.Time
	btcWaitTimeout time.Duration

	evaluatorCfg strategy.EvaluatorConfig
	scannerCfg   scanner.ScannerConfig

	verifiedMu      sync.RWMutex
	verifiedSymbols map[string]bool
	oneWayVerified  bool
	streamCancel    context.CancelFunc
	publicDynStream *ws.ShardedDynamicStream
	marketDynStream *ws.ShardedDynamicStream
	streamMu        sync.Mutex
	streamWg        sync.WaitGroup

	oiPollRunning   atomic.Bool
	oiBootMu        sync.Mutex
	oiBootAttemptAt map[string]time.Time

	majorBatchMu         sync.Mutex
	majorBatchHour       time.Time
	majorBatchEntries    map[string]majorBatchEntry
	majorBatchTimer      *time.Timer
	majorBatchDispatched map[int64]bool
	majorBatchWindow     time.Duration

	shuttingDown atomic.Bool
	evalSubmitMu sync.Mutex
	evalWg       sync.WaitGroup
}

type majorBatchEntry struct {
	candle model.Candle
	state  *model.SymbolState
}

type podSlotCounter interface {
	GetActiveSlotCountByFilter(match func(strategyID string) bool) int
}

func main() {
	fmt.Println("==================================================================")
	fmt.Println("TRML Binance USDⓈ-M Futures Intraday Bot (Go Modular v2.1)")
	fmt.Println("==================================================================")

	// 1. Muat Konfigurasi
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", "err", err)
		os.Exit(1)
	}

	// 2. Setup Structured Logger
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "WARN":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	slog.Info("configuration loaded",
		"env", cfg.Env,
		"dryRun", cfg.DryRun,
		"entryOrderType", cfg.EntryOrderType,
		"restURL", cfg.RestBaseURL,
		"wsPublic", cfg.WSPublicURL,
		"wsMarket", cfg.WSMarketURL,
	)

	// 3. Inisialisasi Database SQLite WAL
	db, err := storage.OpenDB(cfg.DatabasePath)
	if err != nil {
		slog.Error("failed to initialize sqlite database", "path", cfg.DatabasePath, "err", err)
		os.Exit(1)
	}
	defer func() {
		_ = db.Close()
		slog.Info("database connection closed")
	}()
	repo := storage.NewRepository(db)

	stateMode := "LIVE"
	if cfg.DryRun {
		stateMode = "DRY_RUN"
	}

	// Inisialisasi Dedicated Read-Only Web Reader (Zero Lock Contention + Isolasi Mode DRY_RUN / LIVE)
	webReader, err := web.OpenReadOnlyReader(cfg.DatabasePath, cfg.Leverage, stateMode)
	if err != nil {
		slog.Error("failed to initialize read-only web reader", "path", cfg.DatabasePath, "err", err)
		os.Exit(1)
	}
	defer func() {
		_ = webReader.Close()
		slog.Info("web reader closed")
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 4. Inisialisasi Client REST & Broker
	client := exchange.NewClient(cfg.RestBaseURL, cfg.APIKey, cfg.SecretKey)
	if cfg.RecvWindowMS > 0 {
		client.RecvWindow = cfg.RecvWindowMS
	}
	evalCfg := strategy.FromAppConfig(cfg)
	htfEMACap := evalCfg.HTFEMAPeriod
	if evalCfg.MajorPod.IsEnabled() && htfEMACap < 200 {
		htfEMACap = 200
	}
	bootstrapBars := strategy.CalcRequiredHistoryBars(
		cfg.CandleDuration,
		200,
		htfEMACap,
		evalCfg.MarketBreadthLowThreshold > 0,
	)
	effectiveMaxSlots := cfg.MaxSlots
	if evalCfg.MajorPod.IsEnabled() && evalCfg.MajorPod.MaxSlots > 0 {
		effectiveMaxSlots += evalCfg.MajorPod.MaxSlots
	}
	majorExitProfiles := strategy.BuildMajorPodExitProfiles(cfg)
	var majorSyms []string
	var majorSet map[string]bool
	if evalCfg.MajorPod.IsEnabled() {
		majorSet = make(map[string]bool, len(evalCfg.MajorPod.Symbols))
		for _, s := range evalCfg.MajorPod.Symbols {
			if s == "" || s == BenchmarkSymbol || majorSet[s] {
				continue
			}
			majorSet[s] = true
			majorSyms = append(majorSyms, s)
		}
	}
	engine := &BotEngine{
		cfg:                  cfg,
		repo:                 repo,
		client:               client,
		activeStates:         make(map[string]*model.SymbolState),
		primaryWatchedSet:    make(map[string]bool),
		breakoutWatchedSet:   make(map[string]bool),
		sweepExtraWatchedSet: make(map[string]bool),
		majorPodSymbols:      majorSyms,
		majorPodSet:          majorSet,
		exchangeFilters:      make(map[string]model.ExchangeFilter),
		cooldownExits:        make(map[string]storage.RecentExitRecord),
		majorCooldownExits:   make(map[string]storage.RecentExitRecord),
		verifiedSymbols:      make(map[string]bool),
		oiBootAttemptAt:      make(map[string]time.Time),
		clusterSL:            strategy.NewClusterSLTracker(cfg.PortfolioClusterSLCount, time.Duration(cfg.PortfolioClusterCooldownMins)*time.Minute),
		barEntries:           strategy.NewBarEntryTracker(cfg.MaxNewEntriesPerBar),
		btcWaitTimeout:       1500 * time.Millisecond,
		majorBatchWindow:     600 * time.Millisecond,
		evaluatorCfg:         evalCfg,
		scannerCfg:           scanner.FromAppConfig(cfg, bootstrapBars),
	}

	// [COOLDOWN-PERSISTENCE] Hydrate cooldown state dari trade_ledger (terisolasi per mode) untuk mencegah amnesia saat restart
	cooldownLookbackMins := max(cfg.CooldownLossMinutes, max(cfg.CooldownProfitMinutes, max(cfg.CooldownAfterExitMinutes, 20)))
	if cfg.MajorPod.IsEnabled() && cfg.MajorPod.CooldownMinutes > cooldownLookbackMins {
		cooldownLookbackMins = cfg.MajorPod.CooldownMinutes
	}
	cooldownCutoff := time.Now().UTC().Add(-time.Duration(cooldownLookbackMins) * time.Minute)
	if cfg.MajorPod.IsEnabled() {
		if pod1Exits, err := repo.GetRecentExitsByPodForMode(ctx, stateMode, false, cooldownCutoff); err == nil && len(pod1Exits) > 0 {
			engine.cooldownMu.Lock()
			for sym, exitRec := range pod1Exits {
				engine.cooldownExits[sym] = exitRec
			}
			engine.cooldownMu.Unlock()
			slog.Info("hydrated Pod #1 cooldowns from trade ledger", "mode", stateMode, "count", len(pod1Exits), "since", cooldownCutoff)
		}
		if majExits, err := repo.GetRecentExitsByPodForMode(ctx, stateMode, true, cooldownCutoff); err == nil && len(majExits) > 0 {
			engine.cooldownMu.Lock()
			for sym, exitRec := range majExits {
				engine.majorCooldownExits[sym] = exitRec
			}
			engine.cooldownMu.Unlock()
			slog.Info("hydrated MajorPod cooldowns from trade ledger", "mode", stateMode, "count", len(majExits), "since", cooldownCutoff)
		}
	} else if recentExits, err := repo.GetRecentExitsForMode(ctx, stateMode, cooldownCutoff); err == nil && len(recentExits) > 0 {
		engine.cooldownMu.Lock()
		for sym, exitRec := range recentExits {
			engine.cooldownExits[sym] = exitRec
		}
		engine.cooldownMu.Unlock()
		slog.Info("hydrated active cooldowns from trade ledger", "mode", stateMode, "count", len(recentExits), "since", cooldownCutoff)
	}
	if cfg.PortfolioClusterSLCount > 0 {
		slLookback := strategy.DefaultClusterSLWindow + time.Duration(cfg.PortfolioClusterCooldownMins)*time.Minute
		if slTimes, err := repo.GetRecentStopLossTimesForMode(ctx, stateMode, time.Now().UTC().Add(-slLookback)); err == nil && len(slTimes) > 0 {
			engine.clusterSL.Hydrate(slTimes)
			if paused, until := engine.clusterSL.IsPaused(time.Now().UTC()); paused {
				slog.Warn("hydrated active portfolio cluster SL cooldown from trade ledger", "mode", stateMode, "slCount", len(slTimes), "pausedUntil", until)
			}
		}
	}

	if cfg.DryRun {
		slog.Info("initializing Heuristic Dry-Run Volume-Through-Limit Simulator")
		initialEquity := cfg.InitialEquity
		if initialEquity.LessThanOrEqual(decimal.Zero) {
			initialEquity = decimal.NewFromFloat(1000.0)
		}

		now := time.Now().UTC()
		epochDay := now.Unix() / 86400
		startEq, realEq, tripped, found, _ := repo.GetDailyStateForMode(ctx, "DRY_RUN", epochDay)
		if found && startEq.GreaterThan(decimal.Zero) {
			slog.Info("recovered daily state from database", "mode", "DRY_RUN", "startEquity", startEq, "realizedEquity", realEq, "breakerTripped", tripped)
			if realEq.GreaterThan(decimal.Zero) {
				initialEquity = realEq
			}
		} else {
			// [C-05] Jika hari baru belum memiliki record DRY_RUN, warisi realized equity DRY_RUN dari sesi terakhir agar akumulasi profit tidak reset
			_, lastRealEq, _, _, latestFound, _ := repo.GetLatestDailyStateForMode(ctx, "DRY_RUN")
			if latestFound && lastRealEq.GreaterThan(decimal.Zero) {
				slog.Info("recovered daily state from previous session", "mode", "DRY_RUN", "realizedEquity", lastRealEq)
				initialEquity = lastRealEq
			}
		}

		makerFeeFloat, _ := cfg.MakerFeeRate.Float64()
		takerFeeFloat, _ := cfg.TakerFeeRate.Float64()
		slippageFloat, _ := cfg.SlippageRate.Float64()

		dryRunCfg := broker.DryRunConfig{
			MaxSlots:                 effectiveMaxSlots,
			MaxDailyDrawdownPct:      cfg.MaxDailyDrawdown,
			AccountLeverage:          cfg.Leverage,
			BreakEvenTriggerATR:      cfg.BreakEvenTriggerATR,
			Tier1BEEnabled:           cfg.Tier1BEEnabled,
			Tier1BETriggerLongATR:    cfg.Tier1BETriggerLongATR,
			Tier1BETriggerShortATR:   cfg.Tier1BETriggerShortATR,
			Tier2ProfitLockEnabled:   cfg.Tier2ProfitLockEnabled,
			Tier2ProfitLockATR:       cfg.Tier2ProfitLockATR,
			Tier2ProfitLockOffsetATR: cfg.Tier2ProfitLockOffsetATR,
			PartialTPEnabled:         cfg.PartialTPEnabled,
			PartialTP1Ratio:          cfg.PartialTP1Ratio,
			PartialTP1ATRMult:        cfg.PartialTP1ATRMult,
			RunnerBECushionATR:       cfg.RunnerBECushionATR,
			TrailingEnabled:          cfg.TrailingEnabled,
			TrailingActivateATR:      cfg.TrailingActivateATR,
			TrailingDistanceATR:      cfg.TrailingDistanceATR,
			MinBreakEvenOffsetPct:    0.0010,
			LimitOrderTimeout:        cfg.LimitOrderTimeout,
			MaxHoldingDuration:       cfg.MaxHoldingDuration,
			StaleLossExitDuration:    cfg.StaleLossExitDuration,
			MaxMarkStaleness:         cfg.MaxMarkStaleness,
			MakerFeeRate:             makerFeeFloat,
			TakerFeeRate:             takerFeeFloat,
			SlippageRate:             slippageFloat,
			SLGapFillEnabled:         cfg.SLGapFillEnabled,
			EntryOrderType:           cfg.EntryOrderType,
			MaxEntryRepegAttempts:    resolveBrokerRepegAttempts(cfg.EntryRepegMaxAttempts),
			MaxEntryRepegDriftPct:    cfg.EntryRepegMaxDriftPct,
			StrategyExitProfiles:     majorExitProfiles,
		}
		engine.dryRunBroker = broker.NewDryRunBroker(initialEquity, dryRunCfg)
		defer engine.dryRunBroker.Close()
		if !cfg.ResetPeakEquityOnBoot {
			if peakEq, okPeak, errPeak := repo.GetPeakEquityForMode(ctx, "DRY_RUN"); errPeak == nil && okPeak {
				engine.dryRunBroker.SetPeakEffectiveEquityUSDT(peakEq)
				if dd := engine.dryRunBroker.GetDrawdownFromPeakPct(); cfg.DDThrottleThresholdPct > 0 && dd >= cfg.DDThrottleThresholdPct {
					slog.Warn("hydrated dry-run peak equity puts current equity inside drawdown throttle (set RESET_PEAK_EQUITY_ON_BOOT=true if starting a fresh capital baseline)",
						"peakEquity", peakEq, "currentEquity", engine.dryRunBroker.GetEffectiveTotalEquityUSDT(), "drawdownPct", dd*100.0)
				}
			}
		}
		engine.dryRunBroker.OnStrategyCooldownRecordedFn = func(symbol, strategyID string, closedAt time.Time, exitReason string) {
			engine.recordSymbolExitForStrategy(symbol, strategyID, closedAt, exitReason)
		}
		engine.dryRunBroker.OnTradeClosedFn = func(rec broker.TradeClosedRecord) {
			slog.Info("TRADE CLOSED (DRY-RUN)", "symbol", rec.Symbol, "side", rec.Side, "strategyID", rec.StrategyID, "pnl", rec.NetPnL, "reason", rec.ExitReason, "maePct", rec.MAE, "mfePct", rec.MFE, "signalAuditID", rec.SignalAuditID)
			_ = repo.LogTradeForMode(context.Background(), "DRY_RUN", rec.ClientOrderID, rec.Symbol, rec.Side, rec.Qty, rec.EntryPrice, rec.ExitPrice, rec.SLPrice, rec.TPPrice, rec.NetPnL, rec.ExitReason, rec.OpenedAt, rec.ClosedAt, rec.MAE, rec.MFE, rec.SignalAuditID, rec.StrategyID)
			pureRealEq := engine.dryRunBroker.GetPureRealizedEquityClosedTradesUSDT()
			engine.dryRunBroker.Mu.Lock()
			epochDay := engine.dryRunBroker.SessionDayEpochUTC
			startEq := engine.dryRunBroker.SessionStartEquity
			breakerHit := engine.dryRunBroker.CircuitBreakerTripped
			engine.dryRunBroker.Mu.Unlock()
			if epochDay <= 0 {
				epochDay = time.Now().UTC().Unix() / 86400
			}
			if startEq.GreaterThan(decimal.Zero) {
				_ = repo.SaveDailyStateForMode(context.Background(), "DRY_RUN", epochDay, startEq, pureRealEq, breakerHit)
			}
		}

		engine.dryRunBroker.OnPositionSavedFn = func(pos broker.VirtualOpenPosition) {
			_ = repo.SaveDryRunPosition(context.Background(), storage.StoredDryRunPosition{
				ClientOrderID:    pos.ClientOrderID,
				Symbol:           pos.Symbol,
				Side:             string(pos.Side),
				StrategyID:       pos.StrategyID,
				Qty:              pos.Qty,
				InitialQty:       pos.InitialQty,
				EntryPrice:       pos.EntryPrice,
				EntryFee:         pos.EntryFee,
				FundingPaid:      pos.FundingPaid,
				LastFundingEpoch: pos.LastFundingEpoch,
				CurrentSLPrice:   pos.CurrentSLPrice,
				InitialSLPrice:   pos.InitialSLPrice,
				TPPrice:          pos.TPPrice,
				TP1Price:         pos.TP1Price,
				IsTP1Hit:         pos.IsTP1Hit,
				ATRAtEntry:       pos.ATRAtEntry,
				LastMarkPrice:    pos.LastMarkPrice,
				MarkUpdatedAt:    pos.MarkUpdatedAt,
				OpenedAt:         pos.OpenTime,
				IsBreakEvenMoved: pos.IsBreakEvenMoved,
				ProtectionTier:   pos.ProtectionTier,
				SignalAuditID:    pos.SignalAuditID,
				MaxMAE:           pos.MaxMAE,
				MaxMFE:           pos.MaxMFE,
				PeakPrice:        pos.PeakPrice,
			})
		}

		engine.dryRunBroker.OnPositionDeletedFn = func(clientOrderIDOrSymbol string) {
			_ = repo.DeleteDryRunPosition(context.Background(), clientOrderIDOrSymbol)
		}
		engine.dryRunBroker.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
			engine.filtersMu.RLock()
			defer engine.filtersMu.RUnlock()
			f, ok := engine.exchangeFilters[symbol]
			return f, ok
		}
		engine.dryRunBroker.GetBestQuoteFn = engine.getBestQuote

		// Pulihkan posisi terbuka dry-run dari database jika ada (tahan restart/rebuild)
		if storedPositions, err := repo.GetAllDryRunPositions(ctx); err == nil && len(storedPositions) > 0 {
			for _, sp := range storedPositions {
				engine.dryRunBroker.RestoreOpenPosition(&broker.VirtualOpenPosition{
					ClientOrderID:    sp.ClientOrderID,
					Symbol:           sp.Symbol,
					Side:             model.Side(sp.Side),
					StrategyID:       sp.StrategyID,
					Qty:              sp.Qty,
					InitialQty:       sp.InitialQty,
					EntryPrice:       sp.EntryPrice,
					EntryFee:         sp.EntryFee,
					CurrentSLPrice:   sp.CurrentSLPrice,
					InitialSLPrice:   sp.InitialSLPrice,
					TPPrice:          sp.TPPrice,
					TP1Price:         sp.TP1Price,
					IsTP1Hit:         sp.IsTP1Hit,
					ATRAtEntry:       sp.ATRAtEntry,
					LastMarkPrice:    sp.LastMarkPrice,
					MarkUpdatedAt:    sp.MarkUpdatedAt,
					OpenTime:         sp.OpenedAt,
					FundingPaid:      sp.FundingPaid,
					LastFundingEpoch: sp.LastFundingEpoch,
					IsBreakEvenMoved: sp.IsBreakEvenMoved,
					ProtectionTier:   sp.ProtectionTier,
					SignalAuditID:    sp.SignalAuditID,
					MaxMAE:           sp.MaxMAE,
					MaxMFE:           sp.MaxMFE,
					PeakPrice:        sp.PeakPrice,
				})
				slog.Info("restored dry-run active position from database",
					"symbol", sp.Symbol, "clientOrderID", sp.ClientOrderID, "side", sp.Side,
					"qty", sp.Qty, "entryPrice", sp.EntryPrice, "slPrice", sp.CurrentSLPrice,
					"tpPrice", sp.TPPrice, "signalAuditID", sp.SignalAuditID)
			}
		}

		engine.broker = engine.dryRunBroker
	} else {
		slog.Info("initializing Binance Live Execution Broker")
		liveCfg := broker.LiveBrokerConfig{
			MaxSlots:                 effectiveMaxSlots,
			AccountLeverage:          cfg.Leverage,
			MaxDailyDrawdownPct:      cfg.MaxDailyDrawdown,
			MaxAccountStaleness:      cfg.MaxAccountStale,
			LimitOrderTimeout:        cfg.LimitOrderTimeout,
			MaxHoldingDuration:       cfg.MaxHoldingDuration,
			StaleLossExitDuration:    cfg.StaleLossExitDuration,
			MaxMarkStaleness:         cfg.MaxMarkStaleness,
			BreakEvenTriggerATR:      cfg.BreakEvenTriggerATR,
			Tier1BEEnabled:           cfg.Tier1BEEnabled,
			Tier1BETriggerLongATR:    cfg.Tier1BETriggerLongATR,
			Tier1BETriggerShortATR:   cfg.Tier1BETriggerShortATR,
			Tier2ProfitLockEnabled:   cfg.Tier2ProfitLockEnabled,
			Tier2ProfitLockATR:       cfg.Tier2ProfitLockATR,
			Tier2ProfitLockOffsetATR: cfg.Tier2ProfitLockOffsetATR,
			PartialTPEnabled:         cfg.PartialTPEnabled,
			PartialTP1Ratio:          cfg.PartialTP1Ratio,
			PartialTP1ATRMult:        cfg.PartialTP1ATRMult,
			RunnerBECushionATR:       cfg.RunnerBECushionATR,
			TrailingEnabled:          cfg.TrailingEnabled,
			TrailingActivateATR:      cfg.TrailingActivateATR,
			TrailingDistanceATR:      cfg.TrailingDistanceATR,
			MinBreakEvenOffsetPct:    0.0010,
			SlippageRate:             cfg.SlippageRate.InexactFloat64(),
			TakerFeeRate:             cfg.TakerFeeRate.InexactFloat64(),
			MakerFeeRate:             cfg.MakerFeeRate.InexactFloat64(),
			EntryOrderType:           cfg.EntryOrderType,
			MaxEntryRepegAttempts:    resolveBrokerRepegAttempts(cfg.EntryRepegMaxAttempts),
			MaxEntryRepegDriftPct:    cfg.EntryRepegMaxDriftPct,
			StrategyExitProfiles:     majorExitProfiles,
		}
		engine.liveBroker = broker.NewBinanceLiveBroker(liveCfg)
		defer engine.liveBroker.Close()
		if cfg.EntryOrderType == "MARKET" {
			engine.liveBroker.PlaceEntryFn = client.PlaceMarketEntry
		} else {
			engine.liveBroker.PlaceEntryFn = client.PlaceLimitEntry
		}
		engine.liveBroker.PlaceMarketEntryFn = client.PlaceMarketEntry
		engine.liveBroker.PlaceAlgoOrderFn = client.PlaceAlgoOrder
		engine.liveBroker.MarketCloseFn = client.PlaceMarketClose
		engine.liveBroker.PlaceLimitCloseFn = client.PlaceLimitClose
		engine.liveBroker.CancelOrderFn = client.CancelOrder
		engine.liveBroker.CancelOrderWithResultFn = client.CancelOrderWithResult
		engine.liveBroker.CancelAlgoOrderFn = client.CancelAlgoOrder
		engine.liveBroker.VerifyAlgoOrderDetailsFn = func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (broker.AlgoOrderVerifyResult, error) {
			var res broker.AlgoOrderVerifyResult
			algoOrds, err := client.GetOpenAlgoOrders(ctx, symbol)
			if err != nil {
				return res, err
			}
			for _, ao := range algoOrds {
				if slAlgoID != 0 && (ao.AlgoID == slAlgoID || (clientSLID != "" && ao.ClientAlgoID == clientSLID)) {
					res.SLAlive = true
					res.SLClosePosition = ao.ClosePosition
					if q, qErr := decimal.NewFromString(strings.TrimSpace(ao.Quantity)); qErr == nil && q.GreaterThan(decimal.Zero) {
						res.SLQty = q
					}
				}
				if tpAlgoID != 0 && (ao.AlgoID == tpAlgoID || (clientTPID != "" && ao.ClientAlgoID == clientTPID)) {
					res.TPAlive = true
					res.TPClosePosition = ao.ClosePosition
					if q, qErr := decimal.NewFromString(strings.TrimSpace(ao.Quantity)); qErr == nil && q.GreaterThan(decimal.Zero) {
						res.TPQty = q
					}
				}
			}
			if slAlgoID != 0 && !res.SLAlive {
				if openOrds, ordErr := client.GetOpenOrders(ctx, symbol); ordErr == nil {
					for _, oo := range openOrds {
						if oo.OrderID == slAlgoID || (clientSLID != "" && oo.ClientOrderID == clientSLID) {
							res.SLAlive = true
							res.SLClosePosition = oo.ClosePosition
							origQ, errOrig := decimal.NewFromString(strings.TrimSpace(oo.OrigQty))
							execQ, errExec := decimal.NewFromString(strings.TrimSpace(oo.ExecutedQty))
							if errOrig == nil {
								remQ := origQ
								if errExec == nil && execQ.GreaterThan(decimal.Zero) {
									remQ = origQ.Sub(execQ)
								}
								if remQ.GreaterThan(decimal.Zero) {
									res.SLQty = remQ
								}
							}
							break
						}
					}
				}
			}
			return res, nil
		}
		engine.liveBroker.VerifyAlgoOrderFn = func(ctx context.Context, symbol string, slAlgoID, tpAlgoID int64, clientSLID, clientTPID string) (bool, bool, error) {
			details, err := engine.liveBroker.VerifyAlgoOrderDetailsFn(ctx, symbol, slAlgoID, tpAlgoID, clientSLID, clientTPID)
			if err != nil {
				return false, false, err
			}
			return details.SLAlive, details.TPAlive, nil
		}
		engine.liveBroker.QueryOrderFn = client.QueryOrder
		engine.liveBroker.FetchAccountFn = client.FetchAccountBalance
		engine.liveBroker.ConfirmPositionZeroFn = func(ctx context.Context, symbol string) (bool, error) {
			posRisks, err := client.GetPositionRisk(ctx, symbol)
			if err != nil {
				return false, err
			}
			matched := false
			for _, pr := range posRisks {
				if pr.Symbol != symbol {
					continue
				}
				matched = true
				if pr.PositionAmt.Abs().GreaterThan(decimal.Zero) {
					return false, nil
				}
			}
			if !matched {
				return false, fmt.Errorf("symbol %s missing from positionRisk response", symbol)
			}
			return true, nil
		}
		engine.liveBroker.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
			engine.filtersMu.RLock()
			defer engine.filtersMu.RUnlock()
			f, ok := engine.exchangeFilters[symbol]
			return f, ok
		}
		engine.liveBroker.GetBestQuoteFn = engine.getBestQuote
		engine.liveBroker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, positionSide model.Side, qty decimal.Decimal, reason string) error {
			slog.Warn("TRIGGERING EMERGENCY MARKET CLOSE", "symbol", symbol, "positionSide", positionSide, "qty", qty, "reason", reason)
			return client.EmergencyMarketClose(ctx, symbol, positionSide, qty)
		}
		engine.liveBroker.OnPositionSavedFn = func(pos broker.LiveOpenPosition) {
			_ = repo.SaveLivePosition(context.Background(), storage.StoredLivePosition{
				Symbol:         pos.Symbol,
				Side:           string(pos.Side),
				StrategyID:     pos.StrategyID,
				Qty:            pos.Qty,
				InitialQty:     pos.InitialQty,
				EntryPrice:     pos.EntryPrice,
				SLOrderID:      pos.SLOrderID,
				SLClientAlgoID: pos.SLClientAlgoID,
				SLPrice:        pos.SLPrice,
				InitialSLPrice: pos.InitialSLPrice,
				TPOrderID:        pos.TPOrderID,
				TPClientAlgoID:   pos.TPClientAlgoID,
				TPPrice:          pos.TPPrice,
				TP1Price:         pos.TP1Price,
				TP1OrderID:       pos.TP1OrderID,
				TP1ClientOrderID: pos.TP1ClientOrderID,
				IsTP1Hit:         pos.IsTP1Hit,
				ATRAtEntry:     pos.ATRAtEntry,
				ProtectionTier: pos.ProtectionTier,
				SignalAuditID:  pos.SignalAuditID,
				OpenedAt:       pos.OpenTime,
				MaxMAE:         pos.MaxMAE,
				MaxMFE:         pos.MaxMFE,
				UpdatedAt:      pos.UpdatedAt,
				PeakPrice:      pos.PeakPrice,
			})
		}
		engine.liveBroker.OnPositionDeletedFn = func(symbol string) {
			_ = repo.DeleteLivePosition(context.Background(), symbol)
		}
		engine.liveBroker.OnStrategyCooldownRecordedFn = func(symbol, strategyID string, closedAt time.Time, exitReason string) {
			engine.recordSymbolExitForStrategy(symbol, strategyID, closedAt, exitReason)
		}
		engine.liveBroker.OnTradeClosedFn = func(rec broker.TradeClosedRecord) {
			slog.Info("TRADE CLOSED (LIVE)", "symbol", rec.Symbol, "side", rec.Side, "strategyID", rec.StrategyID, "pnl", rec.NetPnL, "reason", rec.ExitReason, "maePct", rec.MAE, "mfePct", rec.MFE, "signalAuditID", rec.SignalAuditID)
			_ = repo.LogTradeForMode(context.Background(), "LIVE", rec.ClientOrderID, rec.Symbol, rec.Side, rec.Qty, rec.EntryPrice, rec.ExitPrice, rec.SLPrice, rec.TPPrice, rec.NetPnL, rec.ExitReason, rec.OpenedAt, rec.ClosedAt, rec.MAE, rec.MFE, rec.SignalAuditID, rec.StrategyID)
		}
		engine.broker = engine.liveBroker
	}

	// 5. Sinkronisasi Jam Bursa & Pemulihan State Harian
	if err := client.SyncServerTime(ctx); err != nil {
		slog.Warn("failed to sync server time with exchange, falling back to local time", "err", err)
	} else {
		slog.Info("server time synced successfully", "timeOffsetMs", client.GetTimeOffset())
	}

	todayEpochDay := engine.exchangeNow().Unix() / 86400
	startEq, realEq, breakerHit, found, err := repo.GetDailyStateForMode(ctx, stateMode, todayEpochDay)
	if err == nil && found {
		slog.Info("recovered today's daily state from storage",
			"mode", stateMode, "epochDay", todayEpochDay, "sessionStartEquity", startEq, "realizedEquity", realEq, "breakerHit", breakerHit)
		if engine.liveBroker != nil {
			engine.liveBroker.Mu.Lock()
			engine.liveBroker.SessionStartEpochDay = todayEpochDay
			engine.liveBroker.SessionStartEquityUSDT = startEq
			engine.liveBroker.CircuitBreakerTripped = breakerHit
			engine.liveBroker.Mu.Unlock()
		} else if engine.dryRunBroker != nil {
			engine.dryRunBroker.Mu.Lock()
			engine.dryRunBroker.SessionDayEpochUTC = todayEpochDay
			engine.dryRunBroker.SessionStartEquity = startEq
			engine.dryRunBroker.CircuitBreakerTripped = breakerHit
			engine.dryRunBroker.Mu.Unlock()
		}
	} else if err == nil && !found && engine.dryRunBroker != nil {
		// [C-05] Hari baru tanpa record DRY_RUN: warisi realized equity hari sebelumnya sebagai sessionStartEquity hari ini
		_, lastRealEq, _, _, latestFound, errLatest := repo.GetLatestDailyStateForMode(ctx, "DRY_RUN")
		if errLatest == nil && latestFound && lastRealEq.GreaterThan(decimal.Zero) {
			slog.Info("starting new daily session inheriting previous day's equity",
				"mode", "DRY_RUN", "epochDay", todayEpochDay, "sessionStartEquity", lastRealEq)
			_ = repo.SaveDailyStateForMode(ctx, "DRY_RUN", todayEpochDay, lastRealEq, lastRealEq, false)
			engine.dryRunBroker.Mu.Lock()
			engine.dryRunBroker.SessionDayEpochUTC = todayEpochDay
			engine.dryRunBroker.SessionStartEquity = lastRealEq
			engine.dryRunBroker.CircuitBreakerTripped = false
			engine.dryRunBroker.Mu.Unlock()
		} else {
			engine.dryRunBroker.Mu.Lock()
			engine.dryRunBroker.SessionDayEpochUTC = todayEpochDay
			startEqInit := engine.dryRunBroker.SessionStartEquity
			realEqInit := engine.dryRunBroker.RealizedEquityUSDT
			engine.dryRunBroker.Mu.Unlock()
			_ = repo.SaveDailyStateForMode(ctx, "DRY_RUN", todayEpochDay, startEqInit, realEqInit, false)
		}
	}

	// 5b. Seeding Saldo Akun & Rekonsiliasi Posisi Live Saat Boot (TD-01, TD-02 & P0-2)
	if engine.liveBroker != nil {
		if !cfg.ResetPeakEquityOnBoot {
			if peakEq, okPeak, errPeak := repo.GetPeakEquityForMode(ctx, "LIVE"); errPeak == nil && okPeak {
				engine.liveBroker.SetPeakEffectiveEquityUSDT(peakEq)
			}
		}
		initCtx, initCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := engine.liveBroker.RefreshAccount(initCtx); err != nil {
			slog.Error("failed to seed initial live account balance via REST", "err", err)
		} else {
			slog.Info("initial live account balance seeded successfully via REST",
				"walletBalance", engine.liveBroker.WalletBalanceUSDT,
				"availBalance", engine.liveBroker.AvailableBalanceUSDT,
				"unrealizedPnL", engine.liveBroker.UnrealizedPnLUSDT,
			)
			engine.liveBroker.Mu.Lock()
			sDay := engine.liveBroker.SessionStartEpochDay
			sEq := engine.liveBroker.SessionStartEquityUSDT
			effEq := engine.liveBroker.WalletBalanceUSDT.Add(engine.liveBroker.UnrealizedPnLUSDT)
			breaker := engine.liveBroker.CircuitBreakerTripped
			peakEq := engine.liveBroker.PeakEffectiveEquityUSDT
			engine.liveBroker.Mu.Unlock()
			if sDay > 0 && sEq.GreaterThan(decimal.Zero) {
				_ = repo.SaveDailyStateForMode(ctx, "LIVE", sDay, sEq, effEq, breaker)
			}
			if !cfg.ResetPeakEquityOnBoot && cfg.DDThrottleThresholdPct > 0 {
				if dd := engine.liveBroker.GetDrawdownFromPeakPct(); dd >= cfg.DDThrottleThresholdPct {
					slog.Warn("hydrated live peak equity puts current balance inside drawdown throttle (set RESET_PEAK_EQUITY_ON_BOOT=true after external withdrawals)",
						"peakEquity", peakEq, "currentEquity", effEq, "drawdownPct", dd*100.0)
				}
			}
		}
		initCancel()

		slog.Info("reconciling live open positions with Binance exchange...")
		storedLivePositions, storedPositionsLoadErr := repo.GetAllLivePositions(ctx)
		if storedPositionsLoadErr != nil {
			slog.Error("failed to load stored live positions from database; orphan emergency auto-close disabled", "err", storedPositionsLoadErr)
		}

		var err error

		var exchangePositions []exchange.PositionRiskResponse
		for posAttempt := 1; posAttempt <= 2; posAttempt++ {
			posCtx, posCancel := context.WithTimeout(ctx, 10*time.Second)
			exchangePositions, err = client.GetPositionRisk(posCtx, "")
			posCancel()
			if err == nil {
				break
			}
			if posAttempt < 2 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if err != nil {
			slog.Error("failed to fetch position risk from Binance at boot reconciliation after retries", "err", err)
			storedSymSet := make(map[string]bool, len(storedLivePositions))
			for _, sp := range storedLivePositions {
				storedSymSet[sp.Symbol] = true
				engine.liveBroker.RestoreOpenPosition(&broker.LiveOpenPosition{
					Symbol:         sp.Symbol,
					Side:           model.Side(sp.Side),
					StrategyID:     sp.StrategyID,
					Qty:            sp.Qty,
					InitialQty:     sp.InitialQty,
					ProtectedQty:   sp.Qty,
					EntryPrice:     sp.EntryPrice,
					SLOrderID:      sp.SLOrderID,
					SLClientAlgoID: sp.SLClientAlgoID,
					SLPrice:        sp.SLPrice,
					InitialSLPrice: sp.InitialSLPrice,
					TPOrderID:        sp.TPOrderID,
					TPClientAlgoID:   sp.TPClientAlgoID,
					TPPrice:          sp.TPPrice,
					TP1Price:         sp.TP1Price,
					TP1OrderID:       sp.TP1OrderID,
					TP1ClientOrderID: sp.TP1ClientOrderID,
					IsTP1Hit:         sp.IsTP1Hit,
					ATRAtEntry:     sp.ATRAtEntry,
					ProtectionTier: sp.ProtectionTier,
					SignalAuditID:  sp.SignalAuditID,
					OpenTime:       sp.OpenedAt,
					MaxMAE:         sp.MaxMAE,
					MaxMFE:         sp.MaxMFE,
					PeakPrice:      sp.PeakPrice,
					UpdatedAt:      sp.UpdatedAt,
				})
			}
			// Tandai posisi exchange yang belum tercatat di DB (hasil RefreshAccount) sebagai ProtectionFailed agar RepairFailedProtections langsung mengamankan SL-nya.
			engine.liveBroker.Mu.Lock()
			for sym, qty := range engine.liveBroker.ActivePositions {
				if qty.GreaterThan(decimal.Zero) && !storedSymSet[sym] {
					if _, hasProt := engine.liveBroker.Protections[sym]; !hasProt {
						side := engine.liveBroker.PositionSides[sym]
						ePrice := engine.liveBroker.PositionEntryPrices[sym]
						slog.Warn("boot reconcile fallback: marking DB-absent live position as ProtectionFailed for immediate SL protection",
							"symbol", sym, "side", side, "qty", qty, "entryPrice", ePrice)
						engine.liveBroker.Protections[sym] = &model.LivePositionProtection{
							Symbol:     sym,
							Side:       side,
							SLStatus:   model.ProtectionFailed,
							InitialQty: qty,
						}
					}
				}
			}
			engine.liveBroker.Mu.Unlock()
		} else {
			// 0. Rekonsiliasi & batalkan/adopsi pending entry orders dari downtime sebelumnya
			ctxOpen, cancelOpen := context.WithTimeout(ctx, 15*time.Second)
			allOpenOrders, errOpen := client.GetOpenOrders(ctxOpen, "")
			cancelOpen()
			if errOpen == nil && len(allOpenOrders) > 0 {
				for _, ord := range allOpenOrders {
					// Identifikasi entry non-reduceOnly agar tidak tertinggal tanpa manajemen
					if !ord.ReduceOnly && ord.Type != "STOP_MARKET" && ord.Type != "STOP" && ord.Type != "TAKE_PROFIT_MARKET" {
						slog.Warn("FOUND PENDING ENTRY ORDER FROM DOWNTIME - ATTEMPTING CANCELLATION",
							"symbol", ord.Symbol, "orderID", ord.OrderID, "clientOrderID", ord.ClientOrderID, "side", ord.Side, "price", ord.Price, "origQty", ord.OrigQty)
						ctxCancel, cancelC := context.WithTimeout(ctx, 5*time.Second)
						cancelErr := client.CancelOrder(ctxCancel, ord.Symbol, ord.OrderID, ord.ClientOrderID)
						cancelC()
						if cancelErr != nil {
							shouldAdopt := true
							ordSide := model.SideLong
							if ord.Side == "SELL" {
								ordSide = model.SideShort
							}
							tQty, _ := decimal.NewFromString(ord.OrigQty)
							cQty, _ := decimal.NewFromString(ord.ExecutedQty)
							lPrice, _ := decimal.NewFromString(ord.Price)
							var ordStatus model.OrderLifecycleStatus
							switch ord.Status {
							case "NEW":
								ordStatus = model.StatusNew
							case "PARTIALLY_FILLED":
								ordStatus = model.StatusPartiallyFilled
							case "FILLED":
								ordStatus = model.StatusFilled
							default:
								ordStatus = model.StatusNew
							}
							if ord.ClientOrderID != "" {
								ctxQ, cancelQ := context.WithTimeout(ctx, 5*time.Second)
								qRec, qErr := client.QueryOrder(ctxQ, ord.Symbol, ord.ClientOrderID)
								cancelQ()
								if qErr == nil && qRec != nil {
									if qRec.Status == model.StatusFilled || qRec.Status == model.StatusCanceled || qRec.Status == model.StatusExpired || qRec.Status == model.StatusRejected {
										slog.Info("downtime pending entry cancel returned error because order already finalized on Binance; skipping pending adoption",
											"symbol", ord.Symbol, "clientOrderID", ord.ClientOrderID, "status", qRec.Status)
										shouldAdopt = false
									} else {
										ordStatus = qRec.Status
										cQty = qRec.CumulativeFilledQty
									}
								}
							}
							if shouldAdopt {
								slog.Warn("failed to cancel pending entry order; adopting order into live broker", "symbol", ord.Symbol, "orderID", ord.OrderID, "err", cancelErr)
								engine.liveBroker.AdoptPendingEntryRecord(&model.LiveOrderRecord{
									ClientOrderID:       ord.ClientOrderID,
									ExchangeOrderID:     ord.OrderID,
									Symbol:              ord.Symbol,
									Side:                ordSide,
									TargetQty:           tQty,
									CumulativeFilledQty: cQty,
									LimitPrice:          lPrice,
									Status:              ordStatus,
									IsEntry:             true,
									CreatedAt:           time.UnixMilli(ord.Time).UTC(),
								})
							}
						} else {
							slog.Info("successfully canceled stale pending entry order from downtime", "symbol", ord.Symbol, "orderID", ord.OrderID)
						}
					}
				}
			}

			// Refresh posisi bursa SETELAH pembatalan/adopsi open orders agar data posisi 100% akurat
			ctxPos, cancelPos := context.WithTimeout(ctx, 10*time.Second)
			freshPositions, errFresh := client.GetPositionRisk(ctxPos, "")
			cancelPos()
			if errFresh == nil {
				exchangePositions = freshPositions
			} else {
				slog.Error("failed to refresh position risk after order reconciliation; using cached positions", "err", errFresh)
			}

			activeOnExchange := make(map[string]exchange.PositionRiskResponse)
			for _, ep := range exchangePositions {
				if ep.PositionAmt.Abs().GreaterThan(decimal.Zero) {
					activeOnExchange[ep.Symbol] = ep
				}
			}

			storedMap := make(map[string]storage.StoredLivePosition)
			for _, sp := range storedLivePositions {
				storedMap[sp.Symbol] = sp
			}

			// 1. Rekonsiliasi posisi tersimpan
			for sym, sp := range storedMap {
				if ep, exists := activeOnExchange[sym]; exists {
					// Verifikasi apakah SL bursa masih aktif untuk posisi tersimpan ini (hanya reset jika query API sukses & order absen)
					if sp.SLOrderID != 0 {
						ctxVer, cancelVer := context.WithTimeout(ctx, 5*time.Second)
						algoOrds, algoErr := client.GetOpenAlgoOrders(ctxVer, sym)
						cancelVer()
						if algoErr == nil {
							slFound := false
							for _, ao := range algoOrds {
								if ao.AlgoID == sp.SLOrderID || (sp.SLClientAlgoID != "" && ao.ClientAlgoID == sp.SLClientAlgoID) {
									slFound = true
									break
								}
							}
							if !slFound {
								slog.Warn("stored SL order not found active on Binance; marking unverified for repair", "symbol", sym, "storedSLID", sp.SLOrderID)
								sp.SLOrderID = 0
								sp.SLClientAlgoID = ""
							}
						} else {
							slog.Warn("failed to verify stored SL order via GetOpenAlgoOrders; retaining stored SL metadata", "symbol", sym, "storedSLID", sp.SLOrderID, "err", algoErr)
						}
					}
					if errOpen == nil && sp.TP1OrderID != 0 {
						tp1Found := false
						for _, oo := range allOpenOrders {
							if oo.Symbol == sym && (oo.OrderID == sp.TP1OrderID || (sp.TP1ClientOrderID != "" && oo.ClientOrderID == sp.TP1ClientOrderID)) {
								tp1Found = true
								break
							}
						}
						if !tp1Found {
							slog.Warn("stored resting TP1 limit order not found on Binance; clearing stale TP1OrderID", "symbol", sym, "tp1OrderID", sp.TP1OrderID)
							sp.TP1OrderID = 0
							sp.TP1ClientOrderID = ""
						}
					} else if errOpen == nil && sp.TP1OrderID == 0 && !sp.IsTP1Hit {
						for _, oo := range allOpenOrders {
							if oo.Symbol == sym && oo.ReduceOnly && (strings.HasPrefix(oo.ClientOrderID, "ALGO_TP1_") || strings.HasSuffix(oo.ClientOrderID, "_TP1")) {
								sp.TP1OrderID = oo.OrderID
								sp.TP1ClientOrderID = oo.ClientOrderID
								slog.Info("recovered active resting TP1 limit order from Binance openOrders at boot",
									"symbol", sym, "tp1OrderID", sp.TP1OrderID, "tp1ClientOrderID", sp.TP1ClientOrderID)
								break
							}
						}
					}

					engine.liveBroker.RestoreOpenPosition(&broker.LiveOpenPosition{
						Symbol:           sp.Symbol,
						Side:             model.Side(sp.Side),
						StrategyID:       sp.StrategyID,
						Qty:              ep.PositionAmt.Abs(),
						InitialQty:       sp.InitialQty,
						ProtectedQty:     sp.Qty,
						EntryPrice:       ep.EntryPrice,
						SLOrderID:        sp.SLOrderID,
						SLClientAlgoID:   sp.SLClientAlgoID,
						SLPrice:          sp.SLPrice,
						InitialSLPrice:   sp.InitialSLPrice,
						TPOrderID:        sp.TPOrderID,
						TPClientAlgoID:   sp.TPClientAlgoID,
						TPPrice:          sp.TPPrice,
						TP1Price:         sp.TP1Price,
						TP1OrderID:       sp.TP1OrderID,
						TP1ClientOrderID: sp.TP1ClientOrderID,
						IsTP1Hit:         sp.IsTP1Hit,
						ATRAtEntry:       sp.ATRAtEntry,
						ProtectionTier:   sp.ProtectionTier,
						SignalAuditID:    sp.SignalAuditID,
						OpenTime:         sp.OpenedAt,
						MaxMAE:           sp.MaxMAE,
						MaxMFE:           sp.MaxMFE,
						PeakPrice:        sp.PeakPrice,
						UpdatedAt:        sp.UpdatedAt,
					})
					slog.Info("reconciled & restored active live position from database and exchange",
						"symbol", sp.Symbol, "qty", ep.PositionAmt.Abs(), "entryPrice", ep.EntryPrice,
						"slPrice", sp.SLPrice, "tpPrice", sp.TPPrice)
				} else {
					slog.Warn("live position closed on exchange during downtime; canceling leftover protection orders, applying exit cooldown and cleaning up stored state", "symbol", sym)
					for _, algoID := range []int64{sp.SLOrderID, sp.TPOrderID} {
						if algoID != 0 {
							ctxC, cancelC := context.WithTimeout(ctx, 5*time.Second)
							_ = client.CancelAlgoOrder(ctxC, sym, algoID)
							cancelC()
						}
					}
					if sp.TP1OrderID != 0 {
						ctxC, cancelC := context.WithTimeout(ctx, 5*time.Second)
						_ = client.CancelOrder(ctxC, sym, sp.TP1OrderID, sp.TP1ClientOrderID)
						cancelC()
					}
					engine.recordSymbolExit(sym, time.Now().UTC(), "OFFLINE_CLOSED")
					_ = repo.DeleteLivePosition(ctx, sym)
				}
			}

			// 2. Deteksi posisi orphan di exchange yang belum tercatat di DB
			for sym, ep := range activeOnExchange {
				if _, exists := storedMap[sym]; !exists {
					side := model.SideLong
					if ep.PositionAmt.IsNegative() {
						side = model.SideShort
					}
					slog.Warn("DETECTED UNMANAGED LIVE POSITION ON BINANCE (missing from DB)",
						"symbol", sym, "side", side, "qty", ep.PositionAmt.Abs(), "entryPrice", ep.EntryPrice)

					// Periksa apakah posisi memiliki Stop Loss aktif di Binance (cek Algo Orders & Normal Orders)
					ctxOrphan, cancelOrphan := context.WithTimeout(ctx, 10*time.Second)
					algoOrds, algoErr := client.GetOpenAlgoOrders(ctxOrphan, sym)
					openOrds, ordErr := client.GetOpenOrders(ctxOrphan, sym)
					cancelOrphan()

					var hasActiveSL bool
					var slAlgoID int64
					var slClientID string
					var activeSLPrice decimal.Decimal

					expectedExitSide := "SELL"
					if side == model.SideShort {
						expectedExitSide = "BUY"
					}

					// 1. Cek dari open algo orders (primary path karena Stop Loss live dibuat via POST /fapi/v1/algoOrder)
					if algoErr == nil {
						for i := range algoOrds {
							ord := &algoOrds[i]
							if ord.Side == expectedExitSide && (ord.OrderType == "STOP_MARKET" || ord.OrderType == "STOP") {
								tpDec, err := decimal.NewFromString(ord.TriggerPrice)
								if err == nil && tpDec.GreaterThan(decimal.Zero) {
									hasActiveSL = true
									slAlgoID = ord.AlgoID
									slClientID = ord.ClientAlgoID
									activeSLPrice = tpDec
									break
								}
							}
						}
					}

					// 2. Fallback cek open normal orders jika belum ditemukan
					if !hasActiveSL && ordErr == nil {
						for i := range openOrds {
							ord := &openOrds[i]
							if ord.Side == expectedExitSide && (ord.Type == "STOP_MARKET" || ord.Type == "STOP") {
								spDec, err := decimal.NewFromString(ord.StopPrice)
								if err == nil && spDec.GreaterThan(decimal.Zero) {
									hasActiveSL = true
									slAlgoID = ord.OrderID
									slClientID = ord.ClientOrderID
									activeSLPrice = spDec
									break
								}
							}
						}
					}

					if shouldEmergencyCloseOrphanPosition(storedPositionsLoadErr, hasActiveSL, algoErr, ordErr, cfg.OrphanPositionPolicy) {
						// OPSI B (Default): Posisi unmanaged yang terkonfirmasi tanpa Stop Loss aktif ditutup seketika untuk melindungi modal
						slog.Error("CRITICAL: UNMANAGED LIVE POSITION HAS NO ACTIVE STOP LOSS ORDER ON BINANCE - EXECUTING EMERGENCY MARKET CLOSE (Option B)",
							"symbol", sym, "side", side, "qty", ep.PositionAmt.Abs())
						ctxEmg, cancelEmg := context.WithTimeout(ctx, 10*time.Second)
						closeErr := client.EmergencyMarketClose(ctxEmg, sym, side, ep.PositionAmt.Abs())
						cancelEmg()
						if closeErr != nil {
							slog.Error("FAILED TO EMERGENCY CLOSE UNMANAGED ORPHAN POSITION", "symbol", sym, "err", closeErr)
						} else {
							slog.Info("EMERGENCY MARKET CLOSE SUCCESSFUL FOR UNMANAGED ORPHAN POSITION", "symbol", sym)
							for _, ao := range algoOrds {
								if ao.AlgoID != 0 {
									ctxC, cancelC := context.WithTimeout(ctx, 5*time.Second)
									_ = client.CancelAlgoOrder(ctxC, sym, ao.AlgoID)
									cancelC()
								}
							}
							for _, oo := range openOrds {
								if oo.ReduceOnly && oo.OrderID != 0 {
									ctxC, cancelC := context.WithTimeout(ctx, 5*time.Second)
									_ = client.CancelOrder(ctxC, sym, oo.OrderID, oo.ClientOrderID)
									cancelC()
								}
							}
							engine.liveBroker.ClearSymbolState(sym)
						}
					} else {
						// Posisi memiliki SL aktif, DB gagal dimuat, verifikasi gagal (fail-safe: jangan blind close), atau policy=ADOPT
						if hasActiveSL {
							slog.Info("unmanaged live position has active SL on Binance; adopting with existing protection",
								"symbol", sym, "slOrderID", slAlgoID, "slPrice", activeSLPrice)
						} else if storedPositionsLoadErr != nil {
							slog.Error("failed to load stored live positions from database; skipping orphan emergency close and adopting safely for SL protection",
								"symbol", sym, "dbErr", storedPositionsLoadErr)
						} else if algoErr != nil || ordErr != nil {
							slog.Error("failed to verify open SL/algo orders for unmanaged live position; adopting safely for SL repair instead of blind emergency close",
								"symbol", sym, "algoErr", algoErr, "ordErr", ordErr)
						} else {
							slog.Warn("adopting unmanaged live position WITHOUT active SL order (policy=ADOPT)", "symbol", sym)
						}

						engine.liveBroker.RestoreOpenPosition(&broker.LiveOpenPosition{
							Symbol:         sym,
							Side:           side,
							Qty:            ep.PositionAmt.Abs(),
							InitialQty:     ep.PositionAmt.Abs(),
							EntryPrice:     ep.EntryPrice,
							SLOrderID:      slAlgoID,
							SLClientAlgoID: slClientID,
							SLPrice:        activeSLPrice,
							InitialSLPrice: activeSLPrice,
							OpenTime:       time.Now().UTC(),
							UpdatedAt:      time.Now().UTC(),
						})
					}
				}
			}
		}
	}

	// 6. Jalankan Siklus Scanning Pertama
	slog.Info("running initial market scan & bootstrap...")
	engine.runMarketScan(ctx)

	// 6. Mulai Background Workers
	var wg sync.WaitGroup

	// Worker 1: Periodic Scanner (menggunakan cfg.ScannerInterval, disinkronkan aman setelah candle close)
	scannerInterval := cfg.ScannerInterval
	if scannerInterval <= 0 {
		scannerInterval = 15 * time.Minute
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			delay := nextSafeScanDelay(engine.exchangeNow(), scannerInterval, cfg.CandleDuration)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				slog.Info("running periodic market scan...")
				engine.runMarketScan(ctx)
			}
		}
	}()

	// Worker 2: Periodic Open Interest Poller (skip jika DISABLE_OI_FILTER=true)
	if !cfg.DisableOIFilter {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					engine.pollOpenInterest(ctx)
				}
			}
		}()
	} else {
		slog.Info("open interest poller disabled via DISABLE_OI_FILTER=true")
	}

	// Worker 3: Live Mode User Data Stream & Keepalive (jika bukan Dry-Run)
	if !cfg.DryRun && cfg.APIKey != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.runUserDataStreamSupervisor(ctx)
		}()

		// Worker 3b: Periodic REST Account Balance Poller (Solusi P0 Anti-Staleness)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(25 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if engine.liveBroker != nil {
						pollCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
						if err := engine.liveBroker.RefreshAccount(pollCtx); err != nil {
							slog.Warn("periodic live account balance sync failed", "err", err)
						} else {
							engine.liveBroker.Mu.Lock()
							sDay := engine.liveBroker.SessionStartEpochDay
							sEq := engine.liveBroker.SessionStartEquityUSDT
							breaker := engine.liveBroker.CircuitBreakerTripped
							engine.liveBroker.Mu.Unlock()
							effEq := engine.liveBroker.GetEffectiveTotalEquityUSDT()
							if sDay > 0 && sEq.GreaterThan(decimal.Zero) {
								_ = repo.SaveDailyStateForMode(ctx, "LIVE", sDay, sEq, effEq, breaker)
							}
						}
						cancel()
					}
				}
			}
		}()
	}

	// Worker 4: Independent Watchdog untuk Broker Time-Stop & Order Timeouts (decoupled dari WS ticks)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var lastSavedDryDay int64
		var lastSavedDryBreaker bool
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				engine.broker.CheckTimeouts(time.Now().UTC())
				if engine.dryRunBroker != nil {
					_ = engine.dryRunBroker.IsCircuitBreakerTripped()
					engine.dryRunBroker.Mu.Lock()
					sDay := engine.dryRunBroker.SessionDayEpochUTC
					sEq := engine.dryRunBroker.SessionStartEquity
					breaker := engine.dryRunBroker.CircuitBreakerTripped
					engine.dryRunBroker.Mu.Unlock()
					if sDay > 0 && (sDay != lastSavedDryDay || breaker != lastSavedDryBreaker) {
						pureEq := engine.dryRunBroker.GetPureRealizedEquityClosedTradesUSDT()
						_ = repo.SaveDailyStateForMode(ctx, "DRY_RUN", sDay, sEq, pureEq, breaker)
						lastSavedDryDay = sDay
						lastSavedDryBreaker = breaker
					}
				}
			}
		}
	}()

	// Worker 5: Daily Database Maintenance & Pruning (Setiap hari tepat pukul 00:05 UTC / 07:05 WIB)
	wg.Add(1)
	go func() {
		defer wg.Done()
		now := time.Now().UTC()
		nextRun := time.Date(now.Year(), now.Month(), now.Day(), 0, 5, 0, 0, time.UTC)
		if !now.Before(nextRun) {
			nextRun = nextRun.Add(24 * time.Hour)
		}
		delay := time.Until(nextRun)
		slog.Info("daily database maintenance scheduled", "nextRunUTC", nextRun.Format(time.RFC3339), "waitDuration", delay.Round(time.Minute))

		timer := time.NewTimer(delay)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if pruned, err := engine.repo.PruneOldSignalAuditLogs(ctx, 7); err == nil && pruned > 0 {
					slog.Info("daily signal audit log maintenance completed", "prunedRejectedSignals", pruned)
				}
				timer.Reset(24 * time.Hour)
			}
		}
	}()

	// Worker 6: Monorepo PWA & Web Dashboard Server (Zero Lock Contention)
	var webServer *web.Server
	if cfg.HTTPPort != "" {
		envStr := "DRY-RUN"
		if !cfg.DryRun {
			envStr = "LIVE"
		}
		webServer, err = web.NewServer(web.ServerConfig{
			Port:                 cfg.HTTPPort,
			Env:                  envStr,
			MaxSlots:             effectiveMaxSlots,
			MaxDailyDrawdown:     cfg.MaxDailyDrawdown,
			Reader:               webReader,
			Broker:               engine.broker,
			GetScannerCoinsFn:    engine.getScannerCoins,
			GetTelemetryFn:       engine.getTelemetry,
			GetScanDiagnosticsFn: engine.getScanDiagnostics,
			IsCircuitBreaker: func() bool {
				if engine.dryRunBroker != nil {
					engine.dryRunBroker.Mu.Lock()
					defer engine.dryRunBroker.Mu.Unlock()
					return engine.dryRunBroker.CircuitBreakerTripped
				}
				if engine.liveBroker != nil {
					engine.liveBroker.Mu.Lock()
					defer engine.liveBroker.Mu.Unlock()
					return engine.liveBroker.CircuitBreakerTripped
				}
				return false
			},
			Repo:             engine.repo,
			DashboardPIN:     cfg.DashboardPIN,
			GeminiAPIKey:     cfg.GeminiAPIKey,
			GeminiModel:      cfg.GeminiModel,
			GeminiTimeoutSec: cfg.GeminiTimeoutSec,
			AIRateLimitRPM:   cfg.AIRateLimitRPM,
		})
		if err != nil {
			slog.Error("failed to create web server", "err", err)
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := webServer.Start(); err != nil {
				slog.Error("web dashboard server encountered error", "err", err)
			}
		}()
	}

	slog.Info("bot initialized and running", "activeSlots", engine.broker.GetActiveSlotCount(), "httpPort", cfg.HTTPPort)

	// Tunggu sinyal shutdown
	<-ctx.Done()
	slog.Info("shutdown signal received, initiating graceful termination...")
	engine.shuttingDown.Store(true)
	engine.majorBatchMu.Lock()
	if engine.majorBatchTimer != nil {
		engine.majorBatchTimer.Stop()
		engine.majorBatchTimer = nil
	}
	engine.majorBatchMu.Unlock()

	if webServer != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
		_ = webServer.Shutdown(shutdownCtx)
		cancelShutdown()
	}

	engine.streamMu.Lock()
	if engine.streamCancel != nil {
		engine.streamCancel()
	}
	engine.streamMu.Unlock()

	// Tunggu worker utama (termasuk scan / restartMarketStream) berhenti dulu sebelum streamWg.Wait()
	// agar tidak terjadi concurrent WaitGroup Add/Wait saat shutdown.
	wg.Wait()
	engine.streamWg.Wait()
	engine.evalWg.Wait()
	slog.Info("bot terminated gracefully")
}

func (e *BotEngine) getScannerCoins() []web.ScannerCoinDTO {
	e.statesMu.RLock()
	coins := slices.Clone(e.watchedCoins)
	e.statesMu.RUnlock()

	result := make([]web.ScannerCoinDTO, 0, len(coins))
	for _, sym := range coins {
		e.statesMu.RLock()
		st, exists := e.activeStates[sym]
		e.statesMu.RUnlock()
		if !exists || st == nil {
			continue
		}

		st.Mu.Lock()
		mark := st.MarkPrice
		vwap := st.CurrentSessionVWAP
		if e.evaluatorCfg.RollingVWAPBars > 0 && len(st.ClosedCandles) > 0 {
			if rv := indicator.CalculateRollingVWAP(st.ClosedCandles, e.evaluatorCfg.RollingVWAPBars); rv > 0 {
				vwap = rv
			}
		}
		spread := 0.0
		if st.BestBid > 0 && st.BestAsk > 0 {
			spread = ((st.BestAsk - st.BestBid) / st.BestBid) * 100.0
		}
		funding := st.FundingRate
		candleLen := len(st.ClosedCandles)
		var natr, rvol float64
		if candleLen >= 14 {
			atr := indicator.CalculateATR(st.ClosedCandles, 14)
			if mark > 0 {
				natr = (atr / mark) * 100.0
			}
		}
		if candleLen >= 20 {
			rvol = indicator.CalculateRVOL(st.ClosedCandles, 20)
		}
		lastUpdate := st.MarkPriceUpdatedAt
		st.Mu.Unlock()

		distVWAP := 0.0
		if vwap > 0 && mark > 0 {
			distVWAP = ((mark - vwap) / vwap) * 100.0
		}

		result = append(result, web.ScannerCoinDTO{
			Symbol:        sym,
			MarkPrice:     mark,
			SessionVWAP:   vwap,
			DistVWAPPct:   distVWAP,
			SpreadPct:     spread,
			FundingRate:   funding,
			NATRPct:       natr,
			RVOL:          rvol,
			LastUpdateUTC: lastUpdate.UTC().Format(time.RFC3339),
		})
	}
	return result
}

func (e *BotEngine) getTelemetry() *web.TelemetryDTO {
	e.statesMu.RLock()
	btcSt := e.activeStates[BenchmarkSymbol]
	breadthPct := e.marketBreadthPct
	breadthValid := e.marketBreadthValid
	e.statesMu.RUnlock()

	var btcCandles []model.Candle
	if btcSt != nil {
		btcSt.Mu.Lock()
		btcCandles = slices.Clone(btcSt.ClosedCandles)
		btcSt.Mu.Unlock()
	}

	macro := strategy.ClassifyMacroRegime(
		btcCandles,
		e.evaluatorCfg.MacroRouter.TrendERThreshold,
		e.evaluatorCfg.MacroRouter.VolShockRatio,
	)

	clusterPaused := false
	clusterUntilUTC := ""
	if e.clusterSL != nil {
		if paused, until := e.clusterSL.IsPaused(e.exchangeNow()); paused {
			clusterPaused = true
			if !until.IsZero() {
				clusterUntilUTC = until.UTC().Format(time.RFC3339)
			}
		}
	}

	return &web.TelemetryDTO{
		MacroRegime:          string(macro.Regime),
		MacroER:              macro.EfficiencyRatio,
		MacroVolRatio:        macro.VolRatio1h,
		MacroReturn24hPct:    macro.Return24hPct,
		RangeRiskMult:        e.evaluatorCfg.MacroRouter.RangeRiskMult,
		VolExpansionRatio:    e.evaluatorCfg.MacroRouter.VolExpansionRatio,
		VolExpansionRiskMult: e.evaluatorCfg.MacroRouter.VolExpansionRiskMult,
		MarketBreadthPct:     breadthPct,
		MarketBreadthValid:   breadthValid,
		ClusterSLPaused:      clusterPaused,
		ClusterSLUntilUTC:    clusterUntilUTC,
		Timeframe:            e.cfg.Timeframe,
		ScannerInterval:      e.cfg.ScannerInterval.String(),
		AllowedSide:          e.cfg.AllowedSide,
		DisableOIFilter:      e.cfg.DisableOIFilter,
	}
}

func (e *BotEngine) getScanDiagnostics() *web.ScanDiagnosticsDTO {
	e.statesMu.RLock()
	has := e.hasScanStats
	st := e.lastScanStats
	scanAt := e.lastScanAt
	rankingMode := e.scannerCfg.RankingMode
	e.statesMu.RUnlock()
	if !has {
		return nil
	}
	scannedAtStr := ""
	if !scanAt.IsZero() {
		scannedAtStr = scanAt.UTC().Format(time.RFC3339)
	}
	return &web.ScanDiagnosticsDTO{
		TotalPerpUSDT:     st.TotalPerpUSDT,
		VolRejected:       st.VolRejected,
		BookMissing:       st.BookMissing,
		SpreadRejected:    st.SpreadRejected,
		PreFilterPassed:   st.PreFilterPassed,
		KlineFetchFailed:  st.KlineFetchFailed,
		KlineShort:        st.KlineFetchOrShort,
		KlineInvalidOrGap: st.KlineGapOrInvalid,
		ATRInvalid:        st.ATRInvalid,
		NATRRejected:      st.NATRRejected,
		SideRejected:      st.SideRejected,
		VWAPDistRejected:  st.VWAPDistRejected,
		CandidatesValid:   st.CandidatesValid,
		PrimaryPassed:     st.PrimaryPassed,
		FallbackUsed:      st.FallbackUsed,
		SelectedCount:     st.SelectedCount,
		BreakoutCount:     st.BreakoutCount,
		SweepExtraCount:   st.SweepExtraCount,
		RankingMode:       rankingMode,
		ScannedAtUTC:      scannedAtStr,
	}
}

const scanFetchTimeout = 2 * time.Minute

func (e *BotEngine) exchangeNow() time.Time {
	now := time.Now().UTC()
	if e.client != nil {
		if offsetMs := e.client.GetTimeOffset(); offsetMs != 0 {
			now = now.Add(time.Duration(offsetMs) * time.Millisecond)
		}
	}
	return now
}

// nextSafeScanDelay menjadwalkan scan setelah penutupan candle agar restart stream WS tidak menabrak event bar close.
func nextSafeScanDelay(now time.Time, scanInterval, candleDur time.Duration) time.Duration {
	if scanInterval <= 0 {
		scanInterval = 15 * time.Minute
	}
	offset := 25 * time.Second
	if candleDur > 0 && candleDur <= time.Minute {
		offset = 10 * time.Second
	} else if candleDur > 0 && offset >= candleDur/2 {
		offset = candleDur / 4
	}
	if offset >= scanInterval {
		offset = scanInterval / 3
	}
	utcNow := now.UTC()
	target := utcNow.Truncate(scanInterval).Add(offset)
	for !target.After(utcNow) {
		target = target.Add(scanInterval)
	}
	return target.Sub(utcNow)
}

func (e *BotEngine) verifyLiveWatchlist(ctx context.Context, candidates []string) []string {
	return e.verifyLiveSymbolList(ctx, candidates, true)
}

func (e *BotEngine) verifyLiveSymbolList(ctx context.Context, candidates []string, updateWatchedCoins bool) []string {
	monitoredSet := make(map[string]bool)
	if e.broker != nil {
		for _, ms := range e.broker.GetMonitoredSymbols() {
			monitoredSet[ms] = true
		}
	}

	e.verifiedMu.RLock()
	oneWayOK := e.oneWayVerified
	e.verifiedMu.RUnlock()
	if !oneWayOK {
		owCtx, owCancel := context.WithTimeout(ctx, 10*time.Second)
		owErr := e.client.EnsureOneWayMode(owCtx)
		owCancel()
		if owErr != nil {
			slog.Error("account One-Way mode check failed; keeping only actively monitored symbols without enabling new entries", "err", owErr)
			var fallback []string
			e.statesMu.Lock()
			for _, sym := range candidates {
				if monitoredSet[sym] {
					fallback = append(fallback, sym)
				} else if sym != BenchmarkSymbol && !e.majorPodSet[sym] {
					delete(e.activeStates, sym)
					delete(e.primaryWatchedSet, sym)
					delete(e.breakoutWatchedSet, sym)
					delete(e.sweepExtraWatchedSet, sym)
				}
			}
			if updateWatchedCoins {
				e.watchedCoins = fallback
			}
			e.statesMu.Unlock()
			return fallback
		}
		e.verifiedMu.Lock()
		e.oneWayVerified = true
		e.verifiedMu.Unlock()
	}

	targetLev := int(math.Round(e.cfg.Leverage))
	verifiedWatched := make([]string, 0, len(candidates))
	for _, sym := range candidates {
		e.verifiedMu.RLock()
		alreadyVerified := e.verifiedSymbols[sym]
		e.verifiedMu.RUnlock()

		if alreadyVerified {
			e.statesMu.Lock()
			if st, exists := e.activeStates[sym]; exists {
				st.Mu.Lock()
				st.IsLeverageIsolatedVerified = true
				st.Mu.Unlock()
			}
			e.statesMu.Unlock()
			verifiedWatched = append(verifiedWatched, sym)
			continue
		}

		verifyCtx, verifyCancel := context.WithTimeout(ctx, 15*time.Second)
		ok, err := e.client.InitAndVerifySymbolMarginAndLeverage(verifyCtx, sym, targetLev)
		verifyCancel()
		if err != nil || !ok {
			if monitoredSet[sym] {
				// Pertahankan stream simbol yang sedang punya posisi/order aktif agar monitoring markPrice/SL/TP tidak putus;
				// tetap biarkan IsLeverageIsolatedVerified=false agar tidak membuka entry baru sebelum terverifikasi.
				slog.Warn("symbol setup failed on actively monitored symbol; keeping stream feed without enabling new entries", "symbol", sym, "err", err)
				verifiedWatched = append(verifiedWatched, sym)
			} else {
				slog.Warn("symbol setup failed; excluding unverified symbol from live trading", "symbol", sym, "err", err)
				e.statesMu.Lock()
				if sym != BenchmarkSymbol && !e.majorPodSet[sym] {
					delete(e.activeStates, sym)
				}
				if updateWatchedCoins {
					delete(e.primaryWatchedSet, sym)
					delete(e.breakoutWatchedSet, sym)
					delete(e.sweepExtraWatchedSet, sym)
				}
				e.statesMu.Unlock()
			}
		} else {
			e.verifiedMu.Lock()
			e.verifiedSymbols[sym] = true
			e.verifiedMu.Unlock()
			e.statesMu.Lock()
			if st, exists := e.activeStates[sym]; exists {
				st.Mu.Lock()
				st.IsLeverageIsolatedVerified = true
				st.Mu.Unlock()
			}
			e.statesMu.Unlock()
			verifiedWatched = append(verifiedWatched, sym)
		}
	}
	if updateWatchedCoins {
		e.statesMu.Lock()
		e.watchedCoins = verifiedWatched
		e.statesMu.Unlock()
	}
	return verifiedWatched
}

func shouldRetainWatchlistOnFetchOutage(hadPriorWatchlist, noScannedCandidates bool, stats scanner.ScanStats) bool {
	return hadPriorWatchlist && noScannedCandidates &&
		stats.PreFilterPassed > 0 &&
		stats.KlineFetchFailed > 0 &&
		(stats.KlineFetchFailed+stats.KlineFetchOrShort) == stats.PreFilterPassed
}

func (e *BotEngine) runMarketScan(ctx context.Context) {
	scanCtx, scanCancel := context.WithTimeout(ctx, scanFetchTimeout)
	defer scanCancel()

	filters, err := e.client.FetchExchangeInfo(scanCtx)
	if err != nil {
		slog.Error("failed to fetch exchange info", "err", err)
		return
	}
	e.filtersMu.Lock()
	e.exchangeFilters = filters
	e.filtersMu.Unlock()

	e.statesMu.RLock()
	for sym, st := range e.activeStates {
		if f, ok := filters[sym]; ok && st != nil {
			st.Mu.Lock()
			st.Filter = f
			st.Mu.Unlock()
		}
	}
	e.statesMu.RUnlock()

	volMap, err := e.client.Fetch24hTickers(scanCtx)
	if err != nil {
		slog.Error("failed to fetch 24h tickers", "err", err)
		return
	}

	bookMap, err := e.client.FetchBookTickers(scanCtx)
	if err != nil {
		slog.Error("failed to fetch book tickers", "err", err)
		return
	}

	var klineFetchFailures atomic.Int64
	var klineSampleMu sync.Mutex
	var klineErrorSamples []string
	fetchKlinesFn := func(symbol string, limit int) []model.Candle {
		k, err := e.client.FetchKlines(scanCtx, symbol, e.cfg.Timeframe, limit)
		if err != nil {
			klineFetchFailures.Add(1)
			klineSampleMu.Lock()
			if len(klineErrorSamples) < 3 {
				klineErrorSamples = append(klineErrorSamples, fmt.Sprintf("%s: %v", symbol, err))
			}
			klineSampleMu.Unlock()
			slog.Debug("failed to fetch klines for symbol", "symbol", symbol, "err", err)
			return nil
		}
		return k
	}

	serverTime := e.exchangeNow()

	// C6 FIX: Hindari menahan statesMu write-lock saat melakukan REST fetch klines serial
	e.statesMu.RLock()
	hadPriorWatchlist := len(e.watchedCoins) > 0
	localStates := make(map[string]*model.SymbolState, len(e.activeStates))
	for k, v := range e.activeStates {
		localStates[k] = v
	}
	e.statesMu.RUnlock()

	scanRes := scanner.RunBulkMarketScannerPartitioned(
		e.scannerCfg,
		localStates,
		filters,
		volMap,
		bookMap,
		fetchKlinesFn,
		serverTime,
	)
	scanStats := scanRes.Stats

	if failCnt := klineFetchFailures.Load(); failCnt > 0 {
		slog.Warn("scanner: kline fetch errors during scan", "failedCount", failCnt, "samples", klineErrorSamples)
	}

	slog.Info("scanner diagnostics",
		"totalPerpUSDT", scanStats.TotalPerpUSDT,
		"volRejected", scanStats.VolRejected,
		"bookMissing", scanStats.BookMissing,
		"spreadRejected", scanStats.SpreadRejected,
		"preFilterPassed", scanStats.PreFilterPassed,
		"klineFetchFailed", scanStats.KlineFetchFailed,
		"klineShort", scanStats.KlineFetchOrShort,
		"klineInvalidOrGap", scanStats.KlineGapOrInvalid,
		"atrInvalid", scanStats.ATRInvalid,
		"natrRejected", scanStats.NATRRejected,
		"sideRejected", scanStats.SideRejected,
		"vwapDistRejected", scanStats.VWAPDistRejected,
		"candidatesValid", scanStats.CandidatesValid,
		"primaryPassed", scanStats.PrimaryPassed,
		"fallbackUsed", scanStats.FallbackUsed,
		"selectedCount", scanStats.SelectedCount,
		"breakoutCount", scanStats.BreakoutCount,
		"sweepExtraCount", scanStats.SweepExtraCount,
		"marketBreadthPct", scanRes.MarketBreadthPct,
		"marketBreadthValid", scanRes.MarketBreadthValid,
		"marketBreadthTotal", scanRes.MarketBreadthTotal,
		"rankingMode", e.scannerCfg.RankingMode,
	)

	if scanCtx.Err() != nil && hadPriorWatchlist {
		slog.Warn("market scan did not finish cleanly before context ended; retaining prior watchlist", "err", scanCtx.Err(), "partialSelected", len(scanRes.Primary)+len(scanRes.Breakout)+len(scanRes.SweepExtra))
		return
	}

	e.statesMu.Lock()
	oldWatched := make([]string, len(e.watchedCoins))
	copy(oldWatched, e.watchedCoins)

	for k, v := range localStates {
		if existing, exists := e.activeStates[k]; !exists {
			if v != nil {
				v.Mu.Lock()
				if v.LastEvaluatedCandleOpenTime.IsZero() && !v.LastProcessedCandleOpenTime.IsZero() {
					v.LastEvaluatedCandleOpenTime = v.LastProcessedCandleOpenTime
				}
				v.Mu.Unlock()
			}
			e.activeStates[k] = v
			e.cooldownMu.RLock()
			if exitRec, ok := e.cooldownExits[k]; ok {
				v.SetLastExit(exitRec.ClosedAt, exitRec.ExitReason)
			}
			e.cooldownMu.RUnlock()
		} else {
			e.cooldownMu.RLock()
			if exitRec, ok := e.cooldownExits[k]; ok {
				existing.SetLastExit(exitRec.ClosedAt, exitRec.ExitReason)
			}
			e.cooldownMu.RUnlock()
		}
	}
	// Pertahankan simbol yang sedang memiliki posisi terbuka atau order aktif agar stream feed tidak putus
	primarySet := make(map[string]bool, len(scanRes.Primary))
	breakoutSet := make(map[string]bool, len(scanRes.Breakout))
	sweepExtraSet := make(map[string]bool, len(scanRes.SweepExtra))
	finalWatched := make([]string, 0, len(scanRes.Primary)+len(scanRes.Breakout)+len(scanRes.SweepExtra)+3)
	for _, sym := range scanRes.Primary {
		if !primarySet[sym] {
			primarySet[sym] = true
			finalWatched = append(finalWatched, sym)
		}
	}
	for _, sym := range scanRes.Breakout {
		breakoutSet[sym] = true
		if !slices.Contains(finalWatched, sym) {
			finalWatched = append(finalWatched, sym)
		}
	}
	for _, sym := range scanRes.SweepExtra {
		if !primarySet[sym] && !sweepExtraSet[sym] {
			sweepExtraSet[sym] = true
			if !slices.Contains(finalWatched, sym) {
				finalWatched = append(finalWatched, sym)
			}
		}
	}
	if e.broker != nil {
		for _, sym := range e.broker.GetMonitoredSymbols() {
			if !slices.Contains(finalWatched, sym) {
				finalWatched = append(finalWatched, sym)
			}
		}
	}

	noScannedCandidates := len(primarySet) == 0 && len(breakoutSet) == 0 && len(sweepExtraSet) == 0
	allKlinesFailed := shouldRetainWatchlistOnFetchOutage(hadPriorWatchlist, noScannedCandidates, scanStats)
	var prunedSymbols []string
	if allKlinesFailed {
		slog.Warn("all pre-filtered kline fetches failed; retaining prior watchlist",
			"preFilterPassed", scanStats.PreFilterPassed,
			"klineFetchFailed", scanStats.KlineFetchFailed,
			"klineShort", scanStats.KlineFetchOrShort)
		retained := slices.Clone(oldWatched)
		if e.broker != nil {
			for _, sym := range e.broker.GetMonitoredSymbols() {
				if !slices.Contains(retained, sym) {
					retained = append(retained, sym)
				}
			}
		}
		finalWatched = retained
		e.watchedCoins = finalWatched
	} else {
		e.watchedCoins = finalWatched
		e.primaryWatchedSet = primarySet
		e.breakoutWatchedSet = breakoutSet
		e.sweepExtraWatchedSet = sweepExtraSet
		e.hasCompletedScan = true
		// M3 FIX: Pangkas simbol lama dari activeStates yang sudah tidak di-watch dan tidak memiliki posisi/order aktif
		watchedSet := make(map[string]bool, len(finalWatched)+1+len(e.majorPodSet))
		for _, sym := range finalWatched {
			watchedSet[sym] = true
		}
		for sym := range e.majorPodSet {
			watchedSet[sym] = true
		}
		watchedSet[BenchmarkSymbol] = true // Jaga BenchmarkSymbol agar tidak terhapus dari activeStates
		for sym := range e.activeStates {
			if !watchedSet[sym] {
				delete(e.activeStates, sym)
				prunedSymbols = append(prunedSymbols, sym)
			}
		}
	}
	if e.evaluatorCfg.MarketBreadthLowThreshold > 0 && scanRes.MarketBreadthValid {
		e.marketBreadthPct = scanRes.MarketBreadthPct
		e.marketBreadthValid = true
		e.marketBreadthUpdatedAt = serverTime
	}
	e.lastScanStats = scanStats
	e.lastScanAt = serverTime
	e.hasScanStats = true
	e.statesMu.Unlock()

	if len(prunedSymbols) > 0 {
		e.oiBootMu.Lock()
		for _, sym := range prunedSymbols {
			delete(e.oiBootAttemptAt, sym)
		}
		e.oiBootMu.Unlock()
	}

	// Inisialisasi dan periodik re-sinkronisasi state BTCUSDT sebagai benchmark makro (P2-3)
	e.statesMu.Lock()
	btcSt, exists := e.activeStates[BenchmarkSymbol]
	if !exists {
		filter, ok := filters[BenchmarkSymbol]
		if !ok {
			filter = model.ExchangeFilter{
				TickSize:    decimal.NewFromFloat(0.1),
				StepSize:    decimal.NewFromFloat(0.001),
				MinQty:      decimal.NewFromFloat(0.001),
				MinNotional: decimal.NewFromFloat(5.0),
			}
		}
		btcSt = model.NewSymbolState(BenchmarkSymbol, filter, e.scannerCfg.BootstrapCount, e.scannerCfg.DeltaWindow+1)
		btcSt.TargetOIInterval = e.cfg.CandleDuration
		btcSt.IsLeverageIsolatedVerified = true
		e.activeStates[BenchmarkSymbol] = btcSt
	}
	e.statesMu.Unlock()

	btcSt.Mu.Lock()
	btcCandleLen := len(btcSt.ClosedCandles)
	btcSt.Mu.Unlock()

	btcFetchCount := 100
	minBTCWarmup := e.scannerCfg.BootstrapCount
	if minBTCWarmup <= 0 {
		minBTCWarmup = 1000
	} else if minBTCWarmup > 4500 {
		minBTCWarmup = 4500
	}
	if !exists || btcCandleLen < minBTCWarmup-5 {
		btcFetchCount = minBTCWarmup
	}
	klines, kErr := e.client.FetchKlines(scanCtx, BenchmarkSymbol, e.cfg.Timeframe, btcFetchCount)
	if kErr == nil && len(klines) > 0 {
		// [C-03] Filter candle yang belum tutup agar tidak merusak state makro BTC
		filteredKlines, vErr := model.ValidateAndFilterCandles(klines, e.exchangeNow(), e.cfg.CandleDuration)
		if vErr != nil {
			slog.Warn("periodic BTC klines validation failed", "err", vErr, "rawCount", len(klines))
		} else if len(filteredKlines) > 0 {
			btcSt.Mu.Lock()
			for _, k := range filteredKlines {
				btcSt.ApplyClosedCandleLocked(k, e.scannerCfg.BootstrapCount, e.scannerCfg.DeltaWindow+1)
			}
			btcSt.Mu.Unlock()
			e.notifyBTCCandleClosed(filteredKlines[len(filteredKlines)-1].OpenTime)
		}
	} else if kErr != nil {
		slog.Warn("periodic BTC klines sync failed", "err", kErr)
	}
	if e.evaluatorCfg.MajorPod.IsEnabled() {
		e.syncHourlyCandlesForSymbol(scanCtx, BenchmarkSymbol, btcSt, e.exchangeNow())
	}

	var majorStreamSymbols []string
	if e.evaluatorCfg.MajorPod.IsEnabled() {
		majorStreamSymbols = e.syncMajorPodStates(scanCtx, filters, bookMap, serverTime)
	}

	slog.Info("market scan completed", "selectedCount", len(finalWatched), "symbols", finalWatched, "majorPodCount", len(majorStreamSymbols))

	// Jika mode Live, verifikasi One-Way mode dan leverage untuk simbol terpilih (M11 FIX & M4 Cache)
	if !e.cfg.DryRun {
		finalWatched = e.verifyLiveWatchlist(ctx, finalWatched)
		if len(majorStreamSymbols) > 0 {
			majorStreamSymbols = e.verifyLiveSymbolList(ctx, majorStreamSymbols, false)
		}
	} else {
		e.statesMu.Lock()
		for _, sym := range finalWatched {
			if st, exists := e.activeStates[sym]; exists {
				st.Mu.Lock()
				st.IsLeverageIsolatedVerified = true
				st.Mu.Unlock()
			}
		}
		for _, sym := range majorStreamSymbols {
			if st, exists := e.activeStates[sym]; exists {
				st.Mu.Lock()
				st.IsLeverageIsolatedVerified = true
				st.Mu.Unlock()
			}
		}
		e.statesMu.Unlock()
	}

	// Segera muat Open Interest awal untuk simbol terpilih agar tidak MISSING_OI_DATA (skip bila filter OI nonaktif)
	if e.cfg == nil || !e.cfg.DisableOIFilter {
		e.pollOpenInterest(ctx)
	}

	streamSymbols := slices.Clone(finalWatched)
	for _, sym := range majorStreamSymbols {
		if !slices.Contains(streamSymbols, sym) {
			streamSymbols = append(streamSymbols, sym)
		}
	}

	e.statesMu.Lock()
	prevStreamSymbols := slices.Clone(e.lastStreamSymbols)
	if len(prevStreamSymbols) == 0 && len(oldWatched) > 0 {
		prevStreamSymbols = slices.Clone(oldWatched)
	}
	e.statesMu.Unlock()

	// Diff check simbol stream (P2-2: Anti-WebSocket Churn)
	symbolsChanged := len(prevStreamSymbols) != len(streamSymbols)
	if !symbolsChanged {
		oldSet := make(map[string]bool, len(prevStreamSymbols))
		for _, s := range prevStreamSymbols {
			oldSet[s] = true
		}
		for _, s := range streamSymbols {
			if !oldSet[s] {
				symbolsChanged = true
				break
			}
		}
	}

	e.streamMu.Lock()
	hasActiveStream := (e.streamCancel != nil)
	e.streamMu.Unlock()

	if symbolsChanged || !hasActiveStream {
		slog.Info("watched symbols changed, updating market streams", "previous", prevStreamSymbols, "current", streamSymbols)
		e.statesMu.Lock()
		e.lastStreamSymbols = slices.Clone(streamSymbols)
		e.statesMu.Unlock()
		e.restartMarketStream(ctx, streamSymbols)
	} else {
		slog.Info("watched symbols unchanged, retaining active market streams", "count", len(streamSymbols))
	}
}

func (e *BotEngine) syncMajorPodStates(
	scanCtx context.Context,
	filters map[string]model.ExchangeFilter,
	bookMap map[string]model.BookQuote,
	serverTime time.Time,
) []string {
	if !e.evaluatorCfg.MajorPod.IsEnabled() {
		return nil
	}
	bootCap := e.scannerCfg.BootstrapCount
	if bootCap < 1000 {
		bootCap = 1000
	}
	deltaCap := e.scannerCfg.DeltaWindow + 1
	if deltaCap < 51 {
		deltaCap = 51
	}

	targetSyms := e.majorPodSymbols
	if len(targetSyms) == 0 {
		for _, sym := range e.evaluatorCfg.MajorPod.Symbols {
			if sym != "" && sym != BenchmarkSymbol && !slices.Contains(targetSyms, sym) {
				targetSyms = append(targetSyms, sym)
			}
		}
	}

	activeMajor := make([]string, 0, len(targetSyms))
	for _, sym := range targetSyms {
		if sym == "" || sym == BenchmarkSymbol {
			continue
		}
		e.statesMu.Lock()
		st, exists := e.activeStates[sym]
		if !exists {
			f, okF := filters[sym]
			if !okF {
				f = model.ExchangeFilter{
					TickSize:    decimal.NewFromFloat(0.01),
					StepSize:    decimal.NewFromFloat(0.1),
					MinQty:      decimal.NewFromFloat(0.1),
					MinNotional: decimal.NewFromFloat(5.0),
				}
			}
			st = model.NewSymbolState(sym, f, bootCap, deltaCap)
			st.TargetOIInterval = e.cfg.CandleDuration
			if q, hasQ := bookMap[sym]; hasQ {
				st.BestBid = q.BestBid
				st.BestAsk = q.BestAsk
				st.BookUpdatedAt = serverTime
			}
			e.cooldownMu.RLock()
			if exitRec, okC := e.cooldownExits[sym]; okC {
				st.SetLastExit(exitRec.ClosedAt, exitRec.ExitReason)
			}
			e.cooldownMu.RUnlock()
			e.activeStates[sym] = st
		} else {
			if q, hasQ := bookMap[sym]; hasQ {
				st.Mu.Lock()
				if st.BookUpdatedAt.IsZero() {
					st.BestBid = q.BestBid
					st.BestAsk = q.BestAsk
					st.BookUpdatedAt = serverTime
				}
				st.Mu.Unlock()
			}
		}
		e.statesMu.Unlock()

		st.Mu.Lock()
		cLen := len(st.ClosedCandles)
		hasGap := st.HasRecentGapLocked(e.cfg.CandleDuration)
		st.Mu.Unlock()

		fetchLimit := 24
		if !exists || hasGap || cLen < bootCap-5 {
			fetchLimit = bootCap
		}
		if e.client != nil {
			klines, kErr := e.client.FetchKlines(scanCtx, sym, e.cfg.Timeframe, fetchLimit)
			if kErr != nil {
				slog.Warn("major pod klines sync failed", "symbol", sym, "err", kErr)
			} else if len(klines) > 0 {
				if bsErr := scanner.BootstrapSymbolState(st, klines, e.exchangeNow(), bootCap, deltaCap, e.cfg.CandleDuration); bsErr != nil {
					slog.Warn("major pod klines bootstrap failed", "symbol", sym, "err", bsErr)
				}
			}
			e.syncHourlyCandlesForSymbol(scanCtx, sym, st, e.exchangeNow())
		}

		st.Mu.Lock()
		if st.LastEvaluatedCandleOpenTime.IsZero() && !st.LastProcessedCandleOpenTime.IsZero() {
			st.LastEvaluatedCandleOpenTime = st.LastProcessedCandleOpenTime
		}
		st.Mu.Unlock()
		activeMajor = append(activeMajor, sym)
	}
	return activeMajor
}

const (
	oiPollWorkers       = 5
	oiRequestTimeout    = 6 * time.Second
	oiBootstrapCooldown = 2 * time.Minute
)

func (e *BotEngine) pollOpenInterest(ctx context.Context) {
	if !e.oiPollRunning.CompareAndSwap(false, true) {
		slog.Warn("previous open interest poll still running, skipping overlapping tick")
		return
	}
	defer e.oiPollRunning.Store(false)

	e.statesMu.RLock()
	symbols := make([]string, len(e.watchedCoins))
	copy(symbols, e.watchedCoins)
	e.statesMu.RUnlock()
	if len(symbols) == 0 {
		return
	}

	workerCount := oiPollWorkers
	if len(symbols) < workerCount {
		workerCount = len(symbols)
	}

	symCh := make(chan string, len(symbols))
	for _, sym := range symbols {
		symCh <- sym
	}
	close(symCh)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sym := range symCh {
				if ctx.Err() != nil {
					return
				}
				e.pollSymbolOpenInterest(ctx, sym)
			}
		}()
	}
	wg.Wait()
}

func (e *BotEngine) canAttemptOIBootstrap(sym string, now time.Time) bool {
	e.oiBootMu.Lock()
	defer e.oiBootMu.Unlock()
	if e.oiBootAttemptAt == nil {
		e.oiBootAttemptAt = make(map[string]time.Time)
	}
	last, exists := e.oiBootAttemptAt[sym]
	if exists && now.Sub(last) < oiBootstrapCooldown {
		return false
	}
	e.oiBootAttemptAt[sym] = now
	return true
}

func (e *BotEngine) pollSymbolOpenInterest(ctx context.Context, sym string) {
	e.statesMu.RLock()
	st := e.activeStates[sym]
	e.statesMu.RUnlock()
	if st == nil {
		return
	}

	nowCheck := e.exchangeNow()
	var maxOIInterval time.Duration
	oiPeriod := "5m"
	if e.cfg != nil {
		maxOIInterval = e.cfg.MaxOIInterval
		oiPeriod = exchange.ResolveOIStatsPeriod(e.cfg.Timeframe)
	}
	st.Mu.Lock()
	needsBootstrap := st.NeedsOIBootstrapLocked(nowCheck, maxOIInterval)
	st.Mu.Unlock()

	if needsBootstrap && e.canAttemptOIBootstrap(sym, nowCheck) {
		histCtx, histCancel := context.WithTimeout(ctx, oiRequestTimeout)
		hist, hErr := e.client.FetchOpenInterestHistWithPeriod(histCtx, sym, oiPeriod, 4)
		histCancel()

		if hErr == nil && len(hist) > 0 {
			samples := make([]model.OISample, 0, len(hist))
			for _, item := range hist {
				val, pErr := strconv.ParseFloat(item.SumOpenInterest, 64) // Pengawal 1: unit kontrak sumOpenInterest
				if pErr == nil && val > 0 {
					samples = append(samples, model.OISample{
						Value:        val,
						Timestamp:    time.UnixMilli(item.Timestamp).UTC(), // Pengawal 4: Timestamp asli bursa dipertahankan
						Bootstrapped: true,                                 // Pengawal 5: Flag Bootstrapped
					})
				}
			}
			if len(samples) > 0 {
				seedNow := e.exchangeNow()
				st.Mu.Lock()
				accepted := st.SeedOpenInterestBootstrapLocked(samples, seedNow)
				st.Mu.Unlock()
				if accepted > 0 {
					slog.Info("bootstrapped historical open interest", "symbol", sym, "period", oiPeriod, "rawCount", len(samples), "acceptedCount", accepted)
				} else {
					slog.Warn("historical open interest samples rejected by age window", "symbol", sym, "period", oiPeriod, "rawCount", len(samples))
				}
			}
		} else if hErr != nil {
			// Pengawal 2: Graceful degradation ke polling riil jika gagal / kosong
			slog.Warn("historical open interest bootstrap failed", "symbol", sym, "err", hErr)
		}
	}

	liveCtx, liveCancel := context.WithTimeout(ctx, oiRequestTimeout)
	oi, err := e.client.FetchOpenInterest(liveCtx, sym)
	liveCancel()
	if err != nil {
		slog.Warn("open interest live poll failed", "symbol", sym, "err", err)
		return
	}

	sampleNow := e.exchangeNow()
	st.Mu.Lock()
	st.RecordOpenInterestLocked(oi, sampleNow)
	st.Mu.Unlock()
}

func buildMarketStreamSubscriptions(symbols []string, timeframe string, includeAggTrade bool) (publicStreams, marketStreams []string) {
	allStreamSymbols := make([]string, 0, len(symbols)+1)
	hasBTC := false
	for _, sym := range symbols {
		if sym == BenchmarkSymbol {
			hasBTC = true
		}
		allStreamSymbols = append(allStreamSymbols, sym)
	}
	if !hasBTC {
		allStreamSymbols = append(allStreamSymbols, BenchmarkSymbol)
	}

	for _, sym := range allStreamSymbols {
		lower := strings.ToLower(sym)
		publicStreams = append(publicStreams, fmt.Sprintf("%s@bookTicker", lower))
		if includeAggTrade {
			marketStreams = append(marketStreams, fmt.Sprintf("%s@aggTrade", lower))
		}
		marketStreams = append(marketStreams,
			fmt.Sprintf("%s@markPrice@1s", lower),
			fmt.Sprintf("%s@kline_%s", lower, timeframe),
		)
	}
	return publicStreams, marketStreams
}

func (e *BotEngine) restartMarketStream(parentCtx context.Context, symbols []string) {
	e.streamMu.Lock()
	defer e.streamMu.Unlock()

	if parentCtx == nil || parentCtx.Err() != nil {
		return
	}

	includeAggTrade := e.cfg == nil || e.cfg.DryRun
	timeframe := "5m"
	if e.cfg != nil && e.cfg.Timeframe != "" {
		timeframe = e.cfg.Timeframe
	}
	publicStreams, marketStreams := buildMarketStreamSubscriptions(symbols, timeframe, includeAggTrade)

	// Differential Update: pertahankan koneksi WS tetap hidup bila sudah ada, hanya kirim UNSUBSCRIBE/SUBSCRIBE
	if e.publicDynStream != nil && e.marketDynStream != nil && e.streamCancel != nil {
		pubErr := e.publicDynStream.UpdateStreams(parentCtx, publicStreams)
		mktErr := e.marketDynStream.UpdateStreams(parentCtx, marketStreams)
		if pubErr == nil && mktErr == nil {
			return
		}
		slog.Warn("differential WS update failed, falling back to clean stream restart", "pubErr", pubErr, "mktErr", mktErr)
	}

	if e.streamCancel != nil {
		e.streamCancel()
	}

	streamCtx, cancel := context.WithCancel(parentCtx)
	publicCtx, publicCancel := context.WithCancel(streamCtx)
	marketCtx, marketCancel := context.WithCancel(streamCtx)
	e.streamCancel = func() {
		publicCancel()
		marketCancel()
		cancel()
	}

	handlers := ws.EventHandlers{
		OnBookTicker: func(symbol string, bestBid, bestAsk float64, now time.Time) {
			e.statesMu.RLock()
			st := e.activeStates[symbol]
			e.statesMu.RUnlock()
			var markPrice float64
			if st != nil {
				st.Mu.Lock()
				st.BestBid = bestBid
				st.BestAsk = bestAsk
				st.BookUpdatedAt = now
				markPrice = e.freshCachedMarkPriceLocked(st, now)
				st.Mu.Unlock()
			}
			e.broker.OnRealtimeTick(symbol, bestBid, bestAsk, markPrice, 0, 0, false, now)
		},
		OnAggTrade: func(symbol string, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {
			e.statesMu.RLock()
			st := e.activeStates[symbol]
			e.statesMu.RUnlock()
			var bestBid, bestAsk, markPrice float64
			if st != nil {
				st.Mu.Lock()
				bestBid = st.BestBid
				bestAsk = st.BestAsk
				markPrice = e.freshCachedMarkPriceLocked(st, now)
				st.Mu.Unlock()
			}
			e.broker.OnRealtimeTick(symbol, bestBid, bestAsk, markPrice, tradePrice, tradeQty, isBuyerMaker, now)
		},
		OnMarkPrice: func(symbol string, markPrice, fundingRate float64, now time.Time) {
			e.statesMu.RLock()
			st := e.activeStates[symbol]
			e.statesMu.RUnlock()
			var bestBid, bestAsk float64
			if st != nil {
				st.Mu.Lock()
				st.MarkPrice = markPrice
				st.MarkPriceUpdatedAt = now
				st.FundingRate = fundingRate
				st.FundingUpdatedAt = now
				bestBid = st.BestBid
				bestAsk = st.BestAsk
				st.Mu.Unlock()
			}
			e.broker.OnRealtimeTick(symbol, bestBid, bestAsk, markPrice, 0, 0, false, now)
			if e.dryRunBroker != nil {
				e.dryRunBroker.ApplyFundingTick(symbol, markPrice, fundingRate, now)
			}
		},
		OnKlineClosed: func(symbol string, candle model.Candle, isClosed bool) {
			if !isClosed {
				return
			}
			e.processClosedKlineWS(streamCtx, symbol, candle, timeframe)
		},
	}
	handlers.OnKline5m = handlers.OnKlineClosed

	publicMgr := ws.NewStreamManager(handlers)
	publicMgr.NowFn = e.exchangeNow
	publicDyn := ws.NewShardedDynamicStream(publicMgr, e.cfg.WSPublicURL, publicStreams, maxBookTickerStreamsPerConn)
	e.publicDynStream = publicDyn
	e.streamWg.Add(1)
	go func() {
		defer e.streamWg.Done()
		publicDyn.Start(publicCtx)
	}()

	marketMgr := ws.NewStreamManager(handlers)
	marketMgr.NowFn = e.exchangeNow
	marketDyn := ws.NewShardedDynamicStream(marketMgr, e.cfg.WSMarketURL, marketStreams, maxMarketStreamsPerConn)
	e.marketDynStream = marketDyn
	e.streamWg.Add(1)
	go func() {
		defer e.streamWg.Done()
		marketDyn.Start(marketCtx)
	}()
}

func (e *BotEngine) processClosedKlineWS(streamCtx context.Context, symbol string, candle model.Candle, timeframe string) {
	slog.Info("candle closed received", "symbol", symbol, "close", candle.Close, "openTime", candle.OpenTime.Format(time.RFC3339), "timeframe", timeframe)
	e.statesMu.RLock()
	st := e.activeStates[symbol]
	e.statesMu.RUnlock()
	if st == nil {
		return
	}

	expectedStep := 5 * time.Minute
	if e.cfg != nil && e.cfg.CandleDuration > 0 {
		expectedStep = e.cfg.CandleDuration
	}

	st.Mu.Lock()
	hasGap := !st.LastProcessedCandleOpenTime.IsZero() && candle.OpenTime.Sub(st.LastProcessedCandleOpenTime) > expectedStep
	var okApplied bool
	var alreadyIngestedLatest bool
	if !hasGap {
		okApplied = st.ApplyClosedCandleLocked(candle, e.scannerCfg.BootstrapCount, e.scannerCfg.DeltaWindow+1)
		if !okApplied && !st.LastProcessedCandleOpenTime.IsZero() && candle.OpenTime.Equal(st.LastProcessedCandleOpenTime) && !st.HasRecentGapLocked(expectedStep) {
			alreadyIngestedLatest = true
		}
	}
	shouldDispatch := false
	if !hasGap && (okApplied || alreadyIngestedLatest) {
		shouldDispatch = st.MarkCandleEvaluatedLocked(candle.OpenTime)
		if okApplied {
			e.updateHourlyHistoryFromStateLocked(symbol, st, candle, expectedStep, false)
		}
	}
	st.Mu.Unlock()

	if hasGap {
		e.evalWg.Add(1)
		go func(c model.Candle, s *model.SymbolState, sym string) {
			defer e.evalWg.Done()
			e.handleGappedClosedCandle(streamCtx, sym, s, c)
		}(candle, st, symbol)
		return
	}

	if !shouldDispatch {
		slog.Debug("closed candle skipped by monotonic deduplication", "symbol", symbol, "openTime", candle.OpenTime)
		return
	}

	if symbol == BenchmarkSymbol {
		// State BTCUSDT telah terbarui sebagai benchmark makro, bangunkan altcoin evaluator yang menunggu bar sama
		e.notifyBTCCandleClosed(candle.OpenTime)
		return
	}

	e.dispatchEvaluationsForClosedCandle(symbol, candle, st)
}

func (e *BotEngine) dispatchEvaluationsForClosedCandle(symbol string, candle model.Candle, st *model.SymbolState) {
	if e.evaluatorCfg.MajorPod.IsEnabled() && e.isMajorPodSymbol(symbol) {
		if strategy.IsHourCloseTimestamp(candle.CloseTime) {
			e.enqueueMajorPodHourCandle(candle, st)
		}
		if !e.isInPod1WatchSets(symbol) {
			return
		}
	}

	// Lepas evaluasi sinyal dan penulisan SQLite audit log dari read-loop WebSocket
	e.evalWg.Add(1)
	go func(c model.Candle, s *model.SymbolState) {
		defer e.evalWg.Done()
		e.evaluateAndLogClosedCandle(c, s)
	}(candle, st)
}

const (
	maxBookTickerStreamsPerConn = 10
	maxMarketStreamsPerConn     = 90
)

func shouldEmergencyCloseOrphanPosition(storedDBLoadErr error, hasActiveSL bool, algoErr, ordErr error, orphanPolicy string) bool {
	if storedDBLoadErr != nil {
		return false
	}
	return !hasActiveSL && algoErr == nil && ordErr == nil && orphanPolicy == "CLOSE"
}

func resolveBrokerRepegAttempts(attempts int) int {
	if attempts <= 0 {
		return -1
	}
	return attempts
}

func (e *BotEngine) getBestQuote(symbol string) (float64, float64, bool) {
	e.statesMu.RLock()
	st := e.activeStates[symbol]
	e.statesMu.RUnlock()
	if st == nil {
		return 0, 0, false
	}

	var maxStale time.Duration
	if e.cfg != nil && e.cfg.MaxBookStaleness > 0 {
		maxStale = e.cfg.MaxBookStaleness
	}

	st.Mu.Lock()
	bid := st.BestBid
	ask := st.BestAsk
	updatedAt := st.BookUpdatedAt
	st.Mu.Unlock()

	if bid <= 0 || ask <= 0 || updatedAt.IsZero() {
		return 0, 0, false
	}
	if maxStale > 0 && e.exchangeNow().Sub(updatedAt) > maxStale {
		return 0, 0, false
	}
	return bid, ask, true
}

func (e *BotEngine) handleGappedClosedCandle(ctx context.Context, symbol string, st *model.SymbolState, triggerCandle model.Candle) {
	if st == nil || e.client == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}

	expectedStep := 5 * time.Minute
	tf := "5m"
	if e.cfg != nil {
		if e.cfg.CandleDuration > 0 {
			expectedStep = e.cfg.CandleDuration
		}
		if e.cfg.Timeframe != "" {
			tf = e.cfg.Timeframe
		}
	}
	bootCap := e.scannerCfg.BootstrapCount
	if bootCap <= 0 {
		bootCap = 1000
	}
	deltaCap := e.scannerCfg.DeltaWindow + 1
	if deltaCap <= 1 {
		deltaCap = 51
	}

	st.Mu.Lock()
	prevOpen := st.LastProcessedCandleOpenTime
	if prevOpen.IsZero() || triggerCandle.OpenTime.Before(prevOpen) {
		st.Mu.Unlock()
		return
	}
	if triggerCandle.OpenTime.Equal(prevOpen) {
		shouldDispatch := !st.HasRecentGapLocked(expectedStep) && st.MarkCandleEvaluatedLocked(triggerCandle.OpenTime)
		st.Mu.Unlock()
		if !shouldDispatch {
			return
		}
		if symbol == BenchmarkSymbol {
			e.notifyBTCCandleClosed(triggerCandle.OpenTime)
			return
		}
		e.evaluateAndLogClosedCandle(triggerCandle, st)
		return
	}
	gapStep := triggerCandle.OpenTime.Sub(prevOpen)
	if gapStep <= expectedStep {
		okApplied := gapStep == expectedStep && st.ApplyClosedCandleLocked(triggerCandle, bootCap, deltaCap)
		shouldDispatch := okApplied && !st.HasRecentGapLocked(expectedStep) && st.MarkCandleEvaluatedLocked(triggerCandle.OpenTime)
		st.Mu.Unlock()
		if !shouldDispatch {
			return
		}
		if symbol == BenchmarkSymbol {
			e.notifyBTCCandleClosed(triggerCandle.OpenTime)
			return
		}
		e.evaluateAndLogClosedCandle(triggerCandle, st)
		return
	}
	st.Mu.Unlock()

	missing := int(gapStep/expectedStep) + 2
	limit := missing + 5
	if limit < 50 {
		limit = 50
	}
	if limit > bootCap {
		limit = bootCap
	}

	bCtx, bCancel := context.WithTimeout(ctx, 6*time.Second)
	raw, err := e.client.FetchKlines(bCtx, symbol, tf, limit)
	bCancel()
	if err != nil {
		slog.Warn("ws kline gap detected; REST backfill failed (fail-closed)", "symbol", symbol, "prevOpen", prevOpen, "triggerOpen", triggerCandle.OpenTime, "err", err)
		return
	}
	valid, vErr := model.ValidateAndFilterCandles(raw, e.exchangeNow(), expectedStep)
	if vErr != nil {
		slog.Warn("ws kline gap backfill validation failed (fail-closed)", "symbol", symbol, "err", vErr)
		return
	}

	st.Mu.Lock()
	if len(valid) > 0 {
		if len(st.ClosedCandles) == 0 || (len(valid) >= len(st.ClosedCandles) && !valid[0].OpenTime.After(st.LastProcessedCandleOpenTime)) {
			st.ResetAndApplyCandlesLocked(valid, bootCap, deltaCap)
		} else if !valid[0].OpenTime.After(st.LastProcessedCandleOpenTime) || valid[0].OpenTime.Sub(st.LastProcessedCandleOpenTime) == expectedStep {
			for _, c := range valid {
				st.ApplyClosedCandleLocked(c, bootCap, deltaCap)
			}
		} else {
			st.Mu.Unlock()
			slog.Warn("ws kline gap backfill does not bridge prior history (fail-closed)",
				"symbol", symbol, "prevOpen", st.LastProcessedCandleOpenTime, "backfillStart", valid[0].OpenTime)
			return
		}
	}
	if triggerCandle.OpenTime.After(st.LastProcessedCandleOpenTime) && triggerCandle.OpenTime.Sub(st.LastProcessedCandleOpenTime) == expectedStep {
		st.ApplyClosedCandleLocked(triggerCandle, bootCap, deltaCap)
	}
	contiguous := !st.LastProcessedCandleOpenTime.Before(triggerCandle.OpenTime) && !st.HasRecentGapLocked(expectedStep)
	if contiguous {
		e.updateHourlyHistoryFromStateLocked(symbol, st, triggerCandle, expectedStep, true)
	}
	shouldDispatch := contiguous && st.MarkCandleEvaluatedLocked(triggerCandle.OpenTime)
	lastOpen := st.LastProcessedCandleOpenTime
	st.Mu.Unlock()

	if !contiguous {
		slog.Warn("ws kline state remains non-contiguous after backfill; skipping evaluation",
			"symbol", symbol, "lastOpen", lastOpen, "triggerOpen", triggerCandle.OpenTime)
		return
	}

	slog.Info("ws kline gap repaired via REST backfill", "symbol", symbol, "triggerOpen", triggerCandle.OpenTime.Format(time.RFC3339))
	if !shouldDispatch {
		return
	}
	if symbol == BenchmarkSymbol {
		e.notifyBTCCandleClosed(triggerCandle.OpenTime)
		return
	}
	e.dispatchEvaluationsForClosedCandle(symbol, triggerCandle, st)
}

func (e *BotEngine) attachSignalAuditID(symbol, clientOrderID string, signalAuditID int64) {
	if signalAuditID <= 0 || e.broker == nil {
		return
	}
	if attacher, ok := e.broker.(interface {
		AttachSignalAuditID(symbol, clientOrderID string, signalAuditID int64)
	}); ok {
		attacher.AttachSignalAuditID(symbol, clientOrderID, signalAuditID)
	}
}

func (e *BotEngine) recordSymbolExit(symbol string, closedAt time.Time, exitReason string) {
	e.recordSymbolExitForStrategy(symbol, "", closedAt, exitReason)
}

func (e *BotEngine) recordSymbolExitForStrategy(symbol, strategyID string, closedAt time.Time, exitReason string) {
	if symbol == "" || exitReason == "" || exitReason == "PARTIAL_TAKE_PROFIT" {
		return
	}
	isMajor := strategy.IsMajorPodStrategy(strategyID) ||
		(strings.TrimSpace(strategyID) == "" && e.evaluatorCfg.MajorPod.IsEnabled() && e.isMajorPodSymbol(symbol) && !e.isInPod1WatchSets(symbol))

	e.cooldownMu.Lock()
	if isMajor {
		if e.majorCooldownExits == nil {
			e.majorCooldownExits = make(map[string]storage.RecentExitRecord)
		}
		prev, exists := e.majorCooldownExits[symbol]
		if !exists || !closedAt.Before(prev.ClosedAt) {
			e.majorCooldownExits[symbol] = storage.RecentExitRecord{
				Symbol:     symbol,
				StrategyID: strategyID,
				ClosedAt:   closedAt,
				ExitReason: exitReason,
			}
		}
	} else {
		if e.cooldownExits == nil {
			e.cooldownExits = make(map[string]storage.RecentExitRecord)
		}
		prev, exists := e.cooldownExits[symbol]
		if !exists || !closedAt.Before(prev.ClosedAt) {
			e.cooldownExits[symbol] = storage.RecentExitRecord{
				Symbol:     symbol,
				StrategyID: strategyID,
				ClosedAt:   closedAt,
				ExitReason: exitReason,
			}
		}
	}
	e.cooldownMu.Unlock()

	if !isMajor && e.clusterSL != nil {
		e.clusterSL.RecordExit(closedAt, exitReason)
	}

	if !isMajor {
		e.statesMu.RLock()
		st := e.activeStates[symbol]
		e.statesMu.RUnlock()
		if st != nil {
			st.SetLastExit(closedAt, exitReason)
		}
	}
}

func (e *BotEngine) freshCachedMarkPriceLocked(st *model.SymbolState, now time.Time) float64 {
	if st == nil || st.MarkPrice <= 0 || st.MarkPriceUpdatedAt.IsZero() {
		return 0
	}
	if e.cfg != nil && e.cfg.MaxMarkStaleness > 0 && now.Sub(st.MarkPriceUpdatedAt) > e.cfg.MaxMarkStaleness {
		return 0
	}
	return st.MarkPrice
}

func (e *BotEngine) notifyBTCCandleClosed(openTime time.Time) {
	if e == nil || openTime.IsZero() {
		return
	}
	e.btcSyncMu.Lock()
	if openTime.After(e.lastBTCOpen) {
		e.lastBTCOpen = openTime
	}
	if e.btcBarCh != nil {
		close(e.btcBarCh)
		e.btcBarCh = nil
	}
	e.btcSyncMu.Unlock()
}

func (e *BotEngine) waitForBTCCandle(targetOpenTime time.Time, maxWait time.Duration) bool {
	if e == nil || targetOpenTime.IsZero() || maxWait <= 0 {
		return false
	}
	deadline := time.Now().Add(maxWait)
	for {
		e.btcSyncMu.Lock()
		if !e.lastBTCOpen.Before(targetOpenTime) {
			e.btcSyncMu.Unlock()
			return true
		}
		if e.btcBarCh == nil {
			e.btcBarCh = make(chan struct{})
		}
		ch := e.btcBarCh
		e.btcSyncMu.Unlock()

		rem := time.Until(deadline)
		if rem <= 0 {
			return false
		}
		timer := time.NewTimer(rem)
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
			return false
		}
	}
}

func (e *BotEngine) getMarketBreadth(now time.Time) (float64, bool) {
	if e.evaluatorCfg.MarketBreadthLowThreshold <= 0 {
		return 0, false
	}
	e.statesMu.RLock()
	if e.marketBreadthValid && !e.marketBreadthUpdatedAt.IsZero() && now.Sub(e.marketBreadthUpdatedAt) <= 2*time.Hour {
		pct := e.marketBreadthPct
		e.statesMu.RUnlock()
		return pct, true
	}
	snapshotStates := make(map[string]*model.SymbolState, len(e.activeStates))
	for sym, st := range e.activeStates {
		if sym != BenchmarkSymbol && st != nil {
			snapshotStates[sym] = st
		}
	}
	e.statesMu.RUnlock()

	seriesMap := make(map[string][]model.Candle, len(snapshotStates))
	for sym, st := range snapshotStates {
		st.Mu.Lock()
		if len(st.ClosedCandles) >= 200 {
			candlesCopy := make([]model.Candle, len(st.ClosedCandles))
			copy(candlesCopy, st.ClosedCandles)
			seriesMap[sym] = candlesCopy
		}
		st.Mu.Unlock()
	}
	pct, valid, _ := strategy.ComputeMarketBreadthFromMap(seriesMap, BenchmarkSymbol, strategy.DefaultMinBreadthSymbols)
	return pct, valid
}

func (e *BotEngine) evaluateAndLogClosedCandle(candle model.Candle, st *model.SymbolState) {
	st.Mu.Lock()
	symName := st.Symbol
	verified := st.IsLeverageIsolatedVerified
	st.Mu.Unlock()
	if e.cfg != nil && !e.cfg.DryRun && !verified {
		slog.Warn("skipping signal evaluation on unverified live symbol", "symbol", symName)
		return
	}

	if e.cfg != nil && !e.cfg.DryRun && e.liveBroker != nil && e.liveBroker.IsAccountStale(e.exchangeNow()) {
		rCtx, rCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = e.liveBroker.RefreshAccount(rCtx)
		rCancel()
	}

	scope := strategy.DefaultSymbolScope()
	e.statesMu.RLock()
	if e.hasCompletedScan || len(e.primaryWatchedSet) > 0 || len(e.breakoutWatchedSet) > 0 || len(e.sweepExtraWatchedSet) > 0 {
		inSweepExtra := e.sweepExtraWatchedSet[symName]
		inPrimary := e.primaryWatchedSet[symName]
		inBreakout := e.breakoutWatchedSet[symName]
		scope = strategy.SymbolScope{
			AllowPrimary:          inPrimary,
			AllowBreakout:         inBreakout,
			BreakoutScopeExplicit: e.scannerCfg.BreakoutScannerEnabled,
			AllowSweep:            inPrimary || inSweepExtra,
		}
	}
	e.statesMu.RUnlock()

	preNow := e.exchangeNow()
	breadthPct, breadthValid := e.getMarketBreadth(preNow)

	if e.btcWaitTimeout > 0 && strategy.NeedsBTCMacroSnapshot(e.evaluatorCfg) {
		e.statesMu.RLock()
		hasBTC := e.activeStates[BenchmarkSymbol] != nil
		e.statesMu.RUnlock()
		if hasBTC {
			e.waitForBTCCandle(candle.OpenTime, e.btcWaitTimeout)
		}
	}

	// Serialisasi evaluasi sinyal + reservasi slot/margin agar tidak terjadi race condition,
	// sambil menjaga I/O REST live tetap berjalan di luar lock agar simbol lain tidak ikut tertahan/stale.
	e.evalSubmitMu.Lock()
	now := e.exchangeNow()
	snap := st.Snapshot(now)
	if e.evaluatorCfg.RollingVWAPBars > 0 && len(snap.ClosedCandles) > 0 {
		if rv := indicator.CalculateRollingVWAP(snap.ClosedCandles, e.evaluatorCfg.RollingVWAPBars); rv > 0 {
			snap.CurrentSessionVWAP = rv
		}
	}
	snap.MarketBreadthPct = breadthPct
	snap.MarketBreadthValid = breadthValid

	// Sinkronkan cooldown exit terbaru (termasuk posisi yang baru saja tutup atau tutup saat downtime)
	e.cooldownMu.RLock()
	if exitRec, exists := e.cooldownExits[snap.Symbol]; exists && exitRec.ClosedAt.After(snap.LastExitTime) {
		snap.LastExitTime = exitRec.ClosedAt
		snap.LastExitReason = exitRec.ExitReason
	}
	e.cooldownMu.RUnlock()

	accountStale := false
	if e.cfg != nil && !e.cfg.DryRun && e.liveBroker != nil && e.liveBroker.IsAccountStale(now) {
		accountStale = true
	}

	var btcSnap *model.MarketSnapshot
	if strategy.NeedsBTCMacroSnapshot(e.evaluatorCfg) {
		e.statesMu.RLock()
		if btcSt := e.activeStates[BenchmarkSymbol]; btcSt != nil {
			s := btcSt.Snapshot(now)
			if e.evaluatorCfg.RollingVWAPBars > 0 && len(s.ClosedCandles) > 0 {
				if rv := indicator.CalculateRollingVWAP(s.ClosedCandles, e.evaluatorCfg.RollingVWAPBars); rv > 0 {
					s.CurrentSessionVWAP = rv
				}
			}
			btcSnap = &s
		}
		e.statesMu.RUnlock()
	}

	btcStaleOrMissing := false
	btcStaleSubReason := ""
	if strategy.NeedsBTCMacroSnapshot(e.evaluatorCfg) && symName != BenchmarkSymbol {
		if btcSnap == nil || len(btcSnap.ClosedCandles) == 0 {
			btcStaleOrMissing = true
			btcStaleSubReason = "BTC_SNAPSHOT_EMPTY"
		} else {
			latestBTCOpen := btcSnap.ClosedCandles[len(btcSnap.ClosedCandles)-1].OpenTime
			if latestBTCOpen.Before(candle.OpenTime) {
				btcStaleOrMissing = true
				btcStaleSubReason = fmt.Sprintf("BTC_LATEST_%s_BEFORE_ALT_%s",
					latestBTCOpen.UTC().Format(time.RFC3339),
					candle.OpenTime.UTC().Format(time.RFC3339))
			}
		}
	}

	activeSlots := e.getActiveNonMajorSlots()
	availBal := e.broker.GetAvailableBalanceUSDT()
	totEq := e.broker.GetEffectiveTotalEquityUSDT()
	snap.PortfolioDrawdownPct = e.broker.GetDrawdownFromPeakPct()
	sizingEquity := totEq
	if e.cfg != nil && !e.cfg.EquityModeCompounding && e.cfg.InitialEquity.GreaterThan(decimal.Zero) {
		sizingEquity = e.cfg.InitialEquity
	}

	// C3 & M1 FIX: Evaluasi sinyal dengan equity, available balance, status drawdown, dan active symbol riil
	maxDailyDD := e.evaluatorCfg.MaxDailyDrawdownPct
	if e.cfg != nil && e.cfg.MaxDailyDrawdown < 0 {
		maxDailyDD = e.cfg.MaxDailyDrawdown
	}
	isDailyBreached := e.broker.IsCircuitBreakerTripped() || (e.broker.GetDailyTotalDrawdownPct() <= maxDailyDD)
	hasActiveSymbol := e.broker.HasActiveOrderOrPosition(snap.Symbol)

	var intent model.OrderIntent
	var ok bool
	var reason, subReason string
	strategyID := strategy.StrategyIDVWAPPullback
	if accountStale {
		reason = "STALE_ACCOUNT_FEED"
		subReason = "LIVE_ACCOUNT_SNAPSHOT_EXPIRED"
	} else if paused, until := e.clusterSL.IsPaused(now); paused {
		reason = "PORTFOLIO_CLUSTER_SL_COOLDOWN"
		subReason = fmt.Sprintf("PAUSED_UNTIL_%s", until.UTC().Format(time.RFC3339))
	} else if e.barEntries != nil && !e.barEntries.CanEnter(candle.OpenTime) {
		reason = "BAR_ENTRY_CAP_REACHED"
	} else if btcStaleOrMissing {
		reason = "STALE_BTC_MACRO_CANDLE"
		subReason = btcStaleSubReason
	} else {
		dec := strategy.EvaluatePortfolio(
			snap,
			btcSnap,
			activeSlots,
			isDailyBreached,
			hasActiveSymbol,
			sizingEquity,
			availBal,
			scope,
			e.evaluatorCfg,
		)
		intent = dec.Intent
		ok = dec.Approved
		reason = dec.Reason
		subReason = dec.SubReason
		strategyID = dec.StrategyID
	}

	needLiveNetworkSubmit := false
	liveOrigLimit := intent.LimitPrice
	liveStartAttempt := 0
	if ok && reason == "SIGNAL_APPROVED" {
		if e.liveBroker != nil {
			prepared, prepErr := e.liveBroker.PreparePreSubmitIntent(intent)
			if prepErr != nil {
				slog.Warn("ENTRY ORDER PRE-SUBMIT REJECTED", "symbol", intent.Symbol, "err", prepErr)
				ok = false
				reason = "BROKER_REJECTED"
				subReason = prepErr.Error()
			} else {
				if !prepared.LimitPrice.Equal(intent.LimitPrice) {
					liveStartAttempt = 1
				}
				intent = prepared
				reserved, reserveErr := e.liveBroker.ReserveEntryIntent(intent)
				if !reserved || reserveErr != nil {
					slog.Warn("ENTRY RESERVATION REJECTED BY BROKER", "symbol", intent.Symbol, "err", reserveErr)
					ok = false
					reason = "BROKER_REJECTED"
					if reserveErr != nil {
						subReason = reserveErr.Error()
					}
				} else {
					needLiveNetworkSubmit = true
					if e.barEntries != nil {
						e.barEntries.RecordEntry(candle.OpenTime)
					}
				}
			}
		} else {
			slog.Info("SIGNAL APPROVED! Submitting limit entry to broker...", "symbol", intent.Symbol, "side", intent.Side, "strategyID", strategyID, "qty", intent.Qty, "price", intent.LimitPrice)
			reserved, submitErr := e.broker.SubmitLimitEntry(intent)
			if reserved && submitErr == nil {
				slog.Info("ENTRY ORDER ACCEPTED BY BROKER", "symbol", intent.Symbol, "clientOrderID", intent.ClientOrderID)
				if e.barEntries != nil {
					e.barEntries.RecordEntry(candle.OpenTime)
				}
			} else {
				slog.Warn("ENTRY ORDER REJECTED BY BROKER", "symbol", intent.Symbol, "err", submitErr)
				ok = false
				reason = "BROKER_REJECTED"
				if submitErr != nil {
					subReason = submitErr.Error()
				}
			}
		}
	}
	e.evalSubmitMu.Unlock()

	if needLiveNetworkSubmit && e.liveBroker != nil {
		slog.Info("SIGNAL APPROVED! Submitting reserved live entry to exchange...", "symbol", intent.Symbol, "side", intent.Side, "strategyID", strategyID, "qty", intent.Qty, "price", intent.LimitPrice)
		submitted, submitErr := e.liveBroker.SubmitReservedEntryWithParams(intent, liveOrigLimit, liveStartAttempt)
		if submitted && submitErr == nil {
			slog.Info("ENTRY ORDER ACCEPTED BY BROKER", "symbol", intent.Symbol, "clientOrderID", intent.ClientOrderID)
		} else {
			if e.barEntries != nil {
				e.barEntries.ReleaseEntry(candle.OpenTime)
			}
			slog.Warn("ENTRY ORDER REJECTED BY BROKER", "symbol", intent.Symbol, "err", submitErr)
			ok = false
			reason = "BROKER_REJECTED"
			if submitErr != nil {
				subReason = submitErr.Error()
			}
		}
	}

	// Simpan audit log lengkap dengan metrik analitik riil
	decision := "REJECT"
	if ok {
		decision = "APPROVE"
	}
	ema200 := indicator.CalculateEMA(snap.ClosedCandles, e.evaluatorCfg.EMAPeriod)
	if math.IsNaN(ema200) || math.IsInf(ema200, 0) {
		ema200 = 0.0
	}
	atr14 := indicator.CalculateATR(snap.ClosedCandles, e.evaluatorCfg.ATRPeriod)
	if math.IsNaN(atr14) || math.IsInf(atr14, 0) {
		atr14 = 0.0
	}
	zScore, _ := indicator.CalculateBarDeltaZScore(snap.ClosedBarDeltasUSDT, e.evaluatorCfg.DeltaZScoreWindow)
	if math.IsNaN(zScore) || math.IsInf(zScore, 0) {
		zScore = 0.0
	}
	var oiDelta float64
	if snap.PrevOpenInterest > 0 {
		oiDelta = ((snap.CurrOpenInterest - snap.PrevOpenInterest) / snap.PrevOpenInterest) * 100.0
		if math.IsNaN(oiDelta) || math.IsInf(oiDelta, 0) {
			oiDelta = 0.0
		}
	}

	spreadPct := 0.0
	if snap.BestBid > 0 && snap.BestAsk > snap.BestBid {
		spreadPct = ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
		if math.IsNaN(spreadPct) || math.IsInf(spreadPct, 0) {
			spreadPct = 0.0
		}
	}
	rvol := indicator.CalculateRVOL(snap.ClosedCandles, 20)
	if math.IsNaN(rvol) || math.IsInf(rvol, 0) {
		rvol = 0.0
	}

	lookback := e.evaluatorCfg.LookbackImpulseBars
	if lookback <= 0 {
		lookback = 18
	}
	startIdx := len(snap.ClosedCandles) - 1 - lookback
	if startIdx < 0 {
		startIdx = 0
	}
	endIdx := len(snap.ClosedCandles) - 1
	impulseATRMult := 0.0
	for i := startIdx; i < endIdx; i++ {
		c := snap.ClosedCandles[i]
		if atr14 > 0 {
			var mult float64
			if candle.Close >= ema200 {
				mult = (c.High - snap.CurrentSessionVWAP) / atr14
			} else {
				mult = (snap.CurrentSessionVWAP - c.Low) / atr14
			}
			if mult > impulseATRMult {
				impulseATRMult = mult
			}
		}
	}

	// Pengawal 5: Tandai subReason dengan OI_BOOTSTRAPPED jika sampel menggunakan data historis bursa
	if snap.IsOIBootstrapped {
		if subReason == "" {
			subReason = "OI_BOOTSTRAPPED"
		} else {
			subReason += "|OI_BOOTSTRAPPED"
		}
	}

	evaluatedSide := "NONE"
	if ok {
		evaluatedSide = string(intent.Side)
	} else if strategyID == strategy.StrategyIDLiquiditySweep {
		evaluatedSide = "SHORT"
	} else if candle.Close > ema200 {
		evaluatedSide = "LONG"
	} else if candle.Close < ema200 {
		evaluatedSide = "SHORT"
	}
	slog.Info("signal evaluated", "symbol", snap.Symbol, "side", evaluatedSide, "strategyID", strategyID, "decision", decision, "reason", reason, "subReason", subReason, "oiBootstrapped", snap.IsOIBootstrapped)

	var limitPriceFloat, slPriceFloat, tpPriceFloat float64
	if ok {
		limitPriceFloat, _ = intent.LimitPrice.Float64()
		slPriceFloat, _ = intent.SLPrice.Float64()
		tpPriceFloat, _ = intent.TPPrice.Float64()
	}

	sigID, err := e.repo.LogSignalAudit(
		context.Background(),
		candle.OpenTime,
		snap.Symbol,
		evaluatedSide,
		candle.Close,
		ema200,
		snap.CurrentSessionVWAP,
		atr14,
		zScore,
		oiDelta,
		snap.FundingRate,
		spreadPct,
		rvol,
		impulseATRMult,
		limitPriceFloat,
		slPriceFloat,
		tpPriceFloat,
		decision,
		reason,
		subReason,
		strategyID,
	)
	if err != nil {
		slog.Error("failed to log signal audit", "symbol", snap.Symbol, "err", err)
	}

	if ok && reason == "SIGNAL_APPROVED" && sigID > 0 {
		e.attachSignalAuditID(intent.Symbol, intent.ClientOrderID, sigID)
	}
}

func (e *BotEngine) isMajorPodSymbol(symbol string) bool {
	if e == nil || symbol == "" || symbol == BenchmarkSymbol {
		return false
	}
	if len(e.majorPodSet) > 0 {
		return e.majorPodSet[symbol]
	}
	for _, s := range e.evaluatorCfg.MajorPod.Symbols {
		if s == symbol {
			return true
		}
	}
	return false
}

func (e *BotEngine) isInPod1WatchSets(symbol string) bool {
	e.statesMu.RLock()
	defer e.statesMu.RUnlock()
	if !e.hasCompletedScan && len(e.primaryWatchedSet) == 0 && len(e.breakoutWatchedSet) == 0 && len(e.sweepExtraWatchedSet) == 0 {
		return true
	}
	return e.primaryWatchedSet[symbol] || e.breakoutWatchedSet[symbol] || e.sweepExtraWatchedSet[symbol]
}

func (e *BotEngine) syncHourlyCandlesForSymbol(ctx context.Context, sym string, st *model.SymbolState, serverTime time.Time) {
	if e == nil || st == nil || e.client == nil || !e.evaluatorCfg.MajorPod.IsEnabled() {
		return
	}
	st.Mu.Lock()
	needSync := len(st.ClosedHourlyCandles) < strategy.MajorPodHourlyHistoryCap-5 || st.HasRecentHourlyGapLocked(48*time.Hour)
	st.Mu.Unlock()
	if !needSync {
		return
	}
	klines, kErr := e.client.FetchKlines(ctx, sym, "1h", strategy.MajorPodHourlyHistoryCap)
	if kErr != nil {
		slog.Warn("major pod 1h klines sync failed", "symbol", sym, "err", kErr)
		return
	}
	if len(klines) == 0 {
		return
	}
	valid, vErr := model.ValidateAndFilterCandles(klines, serverTime, time.Hour)
	if vErr != nil || len(valid) == 0 {
		slog.Warn("major pod 1h klines validation failed", "symbol", sym, "err", vErr, "rawCount", len(klines))
		return
	}
	st.Mu.Lock()
	st.SetHourlyCandlesLocked(valid, strategy.MajorPodHourlyHistoryCap)
	st.Mu.Unlock()
}

func (e *BotEngine) updateHourlyHistoryFromStateLocked(symbol string, st *model.SymbolState, refCandle model.Candle, expectedStep time.Duration, fullResample bool) {
	if e == nil || st == nil || !e.evaluatorCfg.MajorPod.IsEnabled() {
		return
	}
	if symbol != BenchmarkSymbol && !e.isMajorPodSymbol(symbol) {
		return
	}
	if !fullResample && !strategy.IsHourCloseTimestamp(refCandle.CloseTime) {
		return
	}
	subCandles := st.ClosedCandles
	if !fullResample {
		subTail := 16
		if expectedStep > 0 && expectedStep < time.Hour {
			if req := int(time.Hour/expectedStep) + 4; req > subTail {
				subTail = req
			}
		}
		if len(subCandles) > subTail {
			subCandles = subCandles[len(subCandles)-subTail:]
		}
	}
	st.ClosedHourlyCandles = strategy.MergeCompletedHourlyCandle(
		st.ClosedHourlyCandles, subCandles, expectedStep, strategy.MajorPodHourlyHistoryCap,
	)
}

func (e *BotEngine) getActiveNonMajorSlots() int {
	if e == nil || e.broker == nil {
		return 0
	}
	if e.evaluatorCfg.MajorPod.IsEnabled() {
		if fc, ok := e.broker.(podSlotCounter); ok {
			return fc.GetActiveSlotCountByFilter(func(sid string) bool {
				return !strategy.IsMajorPodStrategy(sid)
			})
		}
	}
	return e.broker.GetActiveSlotCount()
}

func (e *BotEngine) getActiveMajorSlots() int {
	if e == nil || e.broker == nil {
		return 0
	}
	if fc, ok := e.broker.(podSlotCounter); ok {
		return fc.GetActiveSlotCountByFilter(strategy.IsMajorPodStrategy)
	}
	return e.broker.GetActiveSlotCount()
}

func (e *BotEngine) expectedMajorPodCount() int {
	e.statesMu.RLock()
	defer e.statesMu.RUnlock()
	if len(e.majorPodSymbols) > 0 {
		cnt := 0
		for _, s := range e.majorPodSymbols {
			if s != "" && s != BenchmarkSymbol {
				cnt++
			}
		}
		if cnt > 0 {
			return cnt
		}
	}
	cnt := 0
	for _, s := range e.evaluatorCfg.MajorPod.Symbols {
		if s != "" && s != BenchmarkSymbol {
			cnt++
		}
	}
	return cnt
}

func normalizeHourBatchTime(t time.Time) time.Time {
	if t.UnixMilli()%1000 == 999 {
		t = t.Add(time.Millisecond)
	}
	return t.UTC().Truncate(time.Minute)
}

func (e *BotEngine) enqueueMajorPodHourCandle(candle model.Candle, st *model.SymbolState) {
	if e == nil || st == nil || e.shuttingDown.Load() {
		return
	}
	hourEnd := normalizeHourBatchTime(candle.CloseTime)
	hourKey := hourEnd.Unix()
	batchWin := e.majorBatchWindow
	if batchWin <= 0 {
		batchWin = 600 * time.Millisecond
	}
	expectedCnt := e.expectedMajorPodCount()

	var flushHour time.Time
	var flushItems []majorBatchEntry

	e.majorBatchMu.Lock()
	if e.majorBatchDispatched == nil {
		e.majorBatchDispatched = make(map[int64]bool)
	}
	if e.majorBatchDispatched[hourKey] {
		e.majorBatchMu.Unlock()
		if !e.shuttingDown.Load() {
			e.evalWg.Add(1)
			go func(c model.Candle, s *model.SymbolState, h time.Time) {
				defer e.evalWg.Done()
				e.flushMajorPodBatchEntries(h, []majorBatchEntry{{candle: c, state: s}})
			}(candle, st, hourEnd)
		}
		return
	}

	if !e.majorBatchHour.IsZero() && hourEnd.After(e.majorBatchHour) && len(e.majorBatchEntries) > 0 {
		if e.majorBatchTimer != nil {
			e.majorBatchTimer.Stop()
			e.majorBatchTimer = nil
		}
		e.majorBatchDispatched[e.majorBatchHour.Unix()] = true
		flushHour = e.majorBatchHour
		for _, ent := range e.majorBatchEntries {
			flushItems = append(flushItems, ent)
		}
		e.majorBatchEntries = make(map[string]majorBatchEntry)
		e.majorBatchHour = time.Time{}
	}

	if e.majorBatchHour.IsZero() || !hourEnd.Equal(e.majorBatchHour) {
		e.majorBatchHour = hourEnd
		e.majorBatchEntries = make(map[string]majorBatchEntry, max(expectedCnt, 4))
		if e.majorBatchTimer != nil {
			e.majorBatchTimer.Stop()
		}
		capturedHour := hourEnd
		e.majorBatchTimer = time.AfterFunc(batchWin, func() {
			e.triggerMajorBatchTimeout(capturedHour)
		})
	}

	sym := st.Symbol
	e.majorBatchEntries[sym] = majorBatchEntry{candle: candle, state: st}
	var readyHour time.Time
	var readyItems []majorBatchEntry
	if expectedCnt > 0 && len(e.majorBatchEntries) >= expectedCnt {
		if e.majorBatchTimer != nil {
			e.majorBatchTimer.Stop()
			e.majorBatchTimer = nil
		}
		e.majorBatchDispatched[hourEnd.Unix()] = true
		readyHour = hourEnd
		for _, ent := range e.majorBatchEntries {
			readyItems = append(readyItems, ent)
		}
		e.majorBatchEntries = make(map[string]majorBatchEntry)
		e.majorBatchHour = time.Time{}
	}
	if len(e.majorBatchDispatched) > 8 {
		var keys []int64
		for k := range e.majorBatchDispatched {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, oldK := range keys[:len(keys)-4] {
			delete(e.majorBatchDispatched, oldK)
		}
	}
	e.majorBatchMu.Unlock()

	if len(flushItems) > 0 && !e.shuttingDown.Load() {
		e.evalWg.Add(1)
		go func(h time.Time, items []majorBatchEntry) {
			defer e.evalWg.Done()
			e.flushMajorPodBatchEntries(h, items)
		}(flushHour, flushItems)
	}
	if len(readyItems) > 0 && !e.shuttingDown.Load() {
		e.evalWg.Add(1)
		go func(h time.Time, items []majorBatchEntry) {
			defer e.evalWg.Done()
			e.flushMajorPodBatchEntries(h, items)
		}(readyHour, readyItems)
	}
}

func (e *BotEngine) triggerMajorBatchTimeout(targetHour time.Time) {
	if e == nil || e.shuttingDown.Load() {
		return
	}
	var items []majorBatchEntry
	e.majorBatchMu.Lock()
	if !e.majorBatchHour.Equal(targetHour) || len(e.majorBatchEntries) == 0 {
		e.majorBatchMu.Unlock()
		return
	}
	if e.majorBatchDispatched == nil {
		e.majorBatchDispatched = make(map[int64]bool)
	}
	e.majorBatchDispatched[targetHour.Unix()] = true
	for _, ent := range e.majorBatchEntries {
		items = append(items, ent)
	}
	e.majorBatchEntries = make(map[string]majorBatchEntry)
	e.majorBatchHour = time.Time{}
	e.majorBatchTimer = nil
	e.majorBatchMu.Unlock()

	if len(items) > 0 && !e.shuttingDown.Load() {
		e.evalWg.Add(1)
		defer e.evalWg.Done()
		e.flushMajorPodBatchEntries(targetHour, items)
	}
}

type majorBatchAuditRecord struct {
	entry         majorBatchEntry
	snap          model.MarketSnapshot
	cand          strategy.MajorRSMomCandidate
	intent        model.OrderIntent
	approved      bool
	decision      string
	reason        string
	subReason     string
	needLiveNet   bool
	liveOrigLimit decimal.Decimal
	liveStartAtt  int
}

func (e *BotEngine) flushMajorPodBatchEntries(batchHour time.Time, items []majorBatchEntry) {
	if e == nil || len(items) == 0 || e.shuttingDown.Load() || e.broker == nil {
		return
	}

	refOpenTime := items[0].candle.OpenTime
	for _, it := range items {
		if it.candle.OpenTime.After(refOpenTime) {
			refOpenTime = it.candle.OpenTime
		}
	}
	if e.btcWaitTimeout > 0 {
		e.statesMu.RLock()
		hasBTC := e.activeStates[BenchmarkSymbol] != nil
		e.statesMu.RUnlock()
		if hasBTC {
			e.waitForBTCCandle(refOpenTime, e.btcWaitTimeout)
		}
	}

	if e.cfg != nil && !e.cfg.DryRun && e.liveBroker != nil && e.liveBroker.IsAccountStale(e.exchangeNow()) {
		rCtx, rCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = e.liveBroker.RefreshAccount(rCtx)
		rCancel()
	}

	slices.SortFunc(items, func(a, b majorBatchEntry) int {
		return strings.Compare(a.state.Symbol, b.state.Symbol)
	})

	e.evalSubmitMu.Lock()
	now := e.exchangeNow()
	var btcSnap *model.MarketSnapshot
	e.statesMu.RLock()
	if btcSt := e.activeStates[BenchmarkSymbol]; btcSt != nil {
		s := btcSt.Snapshot(now)
		btcSnap = &s
	}
	e.statesMu.RUnlock()

	btcStaleOrMissing := false
	btcStaleSub := ""
	if btcSnap == nil || len(btcSnap.ClosedCandles) == 0 {
		btcStaleOrMissing = true
		btcStaleSub = "BTC_SNAPSHOT_EMPTY"
	} else {
		latestBTCOpen := btcSnap.ClosedCandles[len(btcSnap.ClosedCandles)-1].OpenTime
		if latestBTCOpen.Before(refOpenTime) {
			btcStaleOrMissing = true
			btcStaleSub = fmt.Sprintf("BTC_LATEST_%s_BEFORE_%s",
				latestBTCOpen.UTC().Format(time.RFC3339),
				refOpenTime.UTC().Format(time.RFC3339))
		}
	}

	accountStale := e.cfg != nil && !e.cfg.DryRun && e.liveBroker != nil && e.liveBroker.IsAccountStale(now)
	totEq := e.broker.GetEffectiveTotalEquityUSDT()
	availBal := e.broker.GetAvailableBalanceUSDT()
	portDD := e.broker.GetDrawdownFromPeakPct()
	sizingEquity := totEq
	if e.cfg != nil && !e.cfg.EquityModeCompounding && e.cfg.InitialEquity.GreaterThan(decimal.Zero) {
		sizingEquity = e.cfg.InitialEquity
	}
	maxDailyDD := e.evaluatorCfg.MaxDailyDrawdownPct
	if e.cfg != nil && e.cfg.MaxDailyDrawdown < 0 {
		maxDailyDD = e.cfg.MaxDailyDrawdown
	}
	isDailyBreached := e.broker.IsCircuitBreakerTripped() || (e.broker.GetDailyTotalDrawdownPct() <= maxDailyDD)

	var approved []majorBatchAuditRecord
	var results []majorBatchAuditRecord

	for _, it := range items {
		it.state.Mu.Lock()
		verified := it.state.IsLeverageIsolatedVerified
		it.state.Mu.Unlock()
		if e.cfg != nil && !e.cfg.DryRun && !verified {
			slog.Warn("skipping major pod evaluation on unverified live symbol", "symbol", it.state.Symbol)
			continue
		}

		snap := it.state.Snapshot(now)
		snap.PortfolioDrawdownPct = portDD
		snap.LastExitTime = time.Time{}
		snap.LastExitReason = ""
		e.cooldownMu.RLock()
		if exitRec, exists := e.majorCooldownExits[snap.Symbol]; exists {
			snap.LastExitTime = exitRec.ClosedAt
			snap.LastExitReason = exitRec.ExitReason
		} else if exitRec, exists := e.cooldownExits[snap.Symbol]; exists && e.isMajorPodSymbol(snap.Symbol) && !e.isInPod1WatchSets(snap.Symbol) {
			// Legacy fallback when MajorPod history was hydrated without strategy separation
			if strategy.IsMajorPodStrategy(exitRec.StrategyID) {
				snap.LastExitTime = exitRec.ClosedAt
				snap.LastExitReason = exitRec.ExitReason
			}
		}
		e.cooldownMu.RUnlock()

		var cand strategy.MajorRSMomCandidate
		if accountStale {
			cand = strategy.MajorRSMomCandidate{Symbol: snap.Symbol, StrategyID: strategy.StrategyIDMajorRSMomShort, Reason: "STALE_ACCOUNT_FEED", SubReason: "LIVE_ACCOUNT_SNAPSHOT_EXPIRED"}
		} else if btcStaleOrMissing {
			cand = strategy.MajorRSMomCandidate{Symbol: snap.Symbol, StrategyID: strategy.StrategyIDMajorRSMomShort, Reason: "STALE_BTC_MACRO_CANDLE", SubReason: btcStaleSub}
		} else {
			cand = strategy.EvaluateMajorRSMomCandidate(snap, btcSnap, e.broker.HasActiveOrderOrPosition(snap.Symbol), isDailyBreached, e.evaluatorCfg)
		}

		rec := majorBatchAuditRecord{
			entry:     it,
			snap:      snap,
			cand:      cand,
			decision:  "REJECT",
			reason:    cand.Reason,
			subReason: cand.SubReason,
		}
		if cand.Approved {
			approved = append(approved, rec)
		} else {
			results = append(results, rec)
		}
	}

	slices.SortFunc(approved, func(a, b majorBatchAuditRecord) int {
		if a.cand.AbsRS24hPct != b.cand.AbsRS24hPct {
			if a.cand.AbsRS24hPct > b.cand.AbsRS24hPct {
				return -1
			}
			return 1
		}
		return strings.Compare(a.cand.Symbol, b.cand.Symbol)
	})

	maxMajorSlots := e.evaluatorCfg.MajorPod.MaxSlots
	if maxMajorSlots <= 0 {
		maxMajorSlots = 3
	}
	majorSlots := e.getActiveMajorSlots()

	for i := range approved {
		rec := approved[i]
		if majorSlots >= maxMajorSlots {
			rec.decision = "REJECT"
			rec.reason = "MAJOR_POD_SLOTS_FULL"
			rec.subReason = fmt.Sprintf("SLOTS_%d_MAX_%d", majorSlots, maxMajorSlots)
			results = append(results, rec)
			continue
		}
		if e.broker.HasActiveOrderOrPosition(rec.cand.Symbol) {
			rec.decision = "REJECT"
			rec.reason = "SYMBOL_ALREADY_ACTIVE"
			results = append(results, rec)
			continue
		}

		intent, okBuild, buildReason := strategy.BuildMajorRSMomOrderIntent(rec.cand, rec.snap, sizingEquity, availBal, e.evaluatorCfg)
		if !okBuild {
			rec.decision = "REJECT"
			rec.reason = buildReason
			results = append(results, rec)
			continue
		}

		if e.liveBroker != nil {
			prepared, prepErr := e.liveBroker.PreparePreSubmitIntent(intent)
			if prepErr != nil {
				rec.decision = "REJECT"
				rec.reason = "BROKER_REJECTED"
				rec.subReason = prepErr.Error()
				results = append(results, rec)
				continue
			}
			startAttempt := 0
			if !prepared.LimitPrice.Equal(intent.LimitPrice) {
				startAttempt = 1
			}
			reserved, reserveErr := e.liveBroker.ReserveEntryIntent(prepared)
			if !reserved || reserveErr != nil {
				rec.decision = "REJECT"
				rec.reason = "BROKER_REJECTED"
				if reserveErr != nil {
					rec.subReason = reserveErr.Error()
				}
				results = append(results, rec)
				continue
			}
			rec.intent = prepared
			rec.approved = true
			rec.decision = "APPROVE"
			rec.reason = "SIGNAL_APPROVED"
			rec.needLiveNet = true
			rec.liveOrigLimit = intent.LimitPrice
			rec.liveStartAtt = startAttempt
			majorSlots = e.getActiveMajorSlots()
			availBal = e.broker.GetAvailableBalanceUSDT()
		} else {
			reserved, submitErr := e.broker.SubmitLimitEntry(intent)
			if reserved && submitErr == nil {
				rec.intent = intent
				rec.approved = true
				rec.decision = "APPROVE"
				rec.reason = "SIGNAL_APPROVED"
				majorSlots = e.getActiveMajorSlots()
				availBal = e.broker.GetAvailableBalanceUSDT()
				slog.Info("MAJOR POD ENTRY ACCEPTED", "symbol", intent.Symbol, "side", intent.Side, "strategyID", intent.StrategyID, "rs24hPct", rec.cand.RS24hPct, "qty", intent.Qty, "price", intent.LimitPrice)
			} else {
				rec.decision = "REJECT"
				rec.reason = "BROKER_REJECTED"
				if submitErr != nil {
					rec.subReason = submitErr.Error()
				}
			}
		}
		results = append(results, rec)
	}
	e.evalSubmitMu.Unlock()

	for i := range results {
		rec := &results[i]
		if rec.needLiveNet && e.liveBroker != nil {
			submitted, submitErr := e.liveBroker.SubmitReservedEntryWithParams(rec.intent, rec.liveOrigLimit, rec.liveStartAtt)
			if submitted && submitErr == nil {
				slog.Info("MAJOR POD LIVE ENTRY SUBMITTED", "symbol", rec.intent.Symbol, "side", rec.intent.Side, "strategyID", rec.intent.StrategyID, "rs24hPct", rec.cand.RS24hPct)
			} else {
				rec.approved = false
				rec.decision = "REJECT"
				rec.reason = "BROKER_REJECTED"
				if submitErr != nil {
					rec.subReason = submitErr.Error()
				}
			}
		}

		evalSide := string(rec.cand.Side)
		if evalSide == "" {
			evalSide = "NONE"
		}
		spreadPct := 0.0
		if rec.snap.BestBid > 0 && rec.snap.BestAsk > rec.snap.BestBid {
			spreadPct = ((rec.snap.BestAsk - rec.snap.BestBid) / rec.snap.BestBid) * 100.0
		}
		var limF, slF, tpF float64
		if rec.approved {
			limF, _ = rec.intent.LimitPrice.Float64()
			slF, _ = rec.intent.SLPrice.Float64()
			tpF, _ = rec.intent.TPPrice.Float64()
		}
		if e.repo != nil {
			sigID, logErr := e.repo.LogSignalAudit(
				context.Background(),
				batchHour,
				rec.snap.Symbol,
				evalSide,
				rec.cand.Close1h,
				rec.cand.EMA200_1h,
				rec.snap.CurrentSessionVWAP,
				rec.cand.ATR14_1h,
				rec.cand.DeltaZ1h,
				0.0,
				rec.snap.FundingRate,
				spreadPct,
				0.0,
				rec.cand.RS24hPct,
				limF,
				slF,
				tpF,
				rec.decision,
				rec.reason,
				rec.subReason,
				strategy.NormalizeStrategyID(rec.cand.StrategyID),
			)
			if logErr != nil {
				slog.Error("failed to log major pod signal audit", "symbol", rec.snap.Symbol, "err", logErr)
			} else if rec.approved && sigID > 0 {
				e.attachSignalAuditID(rec.intent.Symbol, rec.intent.ClientOrderID, sigID)
			}
		}
	}
}

func (e *BotEngine) runUserDataStreamSupervisor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		listenKey, err := e.client.CreateListenKey(ctx)
		if err != nil {
			slog.Error("failed to create user data stream listenKey; retrying in 5s", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		maskedKey := listenKey
		if len(listenKey) > 12 {
			maskedKey = listenKey[:6] + "..." + listenKey[len(listenKey)-4:]
		}
		slog.Info("user data stream listenKey created", "listenKey", maskedKey)

		sessionCtx, sessionCancel := context.WithCancel(ctx)
		var sessionWg sync.WaitGroup

		// Keepalive setiap 30 menit; jika gagal, batalkan session agar loop membuat kunci baru
		sessionWg.Add(1)
		go func(lKey string) {
			defer sessionWg.Done()
			ticker := time.NewTicker(30 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-sessionCtx.Done():
					return
				case <-ticker.C:
					if err := e.client.KeepAliveListenKey(sessionCtx); err != nil {
						slog.Warn("failed to keepalive listenKey; cycling user stream session...", "err", err)
						sessionCancel()
						return
					}
					slog.Debug("listenKey refreshed successfully")
				}
			}
		}(listenKey)

		// C7 & 2026 Spec: Gunakan query param listenKey & events untuk Mainnet, fallback ke legacy path untuk Testnet
		userStreamURL := ws.BuildUserDataStreamURL(e.cfg.WSPrivateBaseURL, listenKey, e.cfg.Env == config.EnvMainnet)
		handlers := ws.EventHandlers{
			OnOrderTradeUpdateDetailed: func(evt model.OrderTradeUpdateEvent) {
				slog.Info("USER ORDER UPDATE", "symbol", evt.Symbol, "orderID", evt.ExchangeOrderID, "status", evt.Status, "cumQty", evt.CumFilledQty, "avgPrice", evt.AvgPrice, "realizedProfit", evt.RealizedProfit, "fee", evt.Commission, "feeAsset", evt.CommissionAsset, "isMaker", evt.IsMaker)
				if e.liveBroker != nil {
					e.liveBroker.HandleUserDataOrderUpdateDetailed(evt)
				}
			},
			OnAccountUpdateEvent: func(evt model.AccountUpdateEvent) {
				slog.Debug("USER ACCOUNT UPDATE", "wallet", evt.WalletBalance, "unrealizedPnL", evt.UnrealizedPnL, "avail", evt.AvailableBalance, "positionsCount", len(evt.Positions))
				var breakerTripped bool
				effEq := evt.WalletBalance.Add(evt.UnrealizedPnL)
				startEquity := effEq
				if e.liveBroker != nil {
					e.liveBroker.HandleAccountUpdateEvent(evt)
					e.liveBroker.Mu.Lock()
					breakerTripped = e.liveBroker.CircuitBreakerTripped
					effEq = e.liveBroker.WalletBalanceUSDT.Add(e.liveBroker.UnrealizedPnLUSDT)
					startEquity = effEq
					if e.liveBroker.SessionStartEquityUSDT.GreaterThan(decimal.Zero) {
						startEquity = e.liveBroker.SessionStartEquityUSDT
					}
					e.liveBroker.Mu.Unlock()
				}
				epochDay := evt.EventTime.UTC().Unix() / 86400
				if epochDay > 0 && startEquity.GreaterThan(decimal.Zero) {
					_ = e.repo.SaveDailyStateForMode(ctx, "LIVE", epochDay, startEquity, effEq, breakerTripped)
				}
			},
			OnAlgoOrderUpdate: func(evt model.AlgoOrderUpdateEvent) {
				slog.Info("USER ALGO UPDATE", "symbol", evt.Symbol, "algoID", evt.AlgoID, "clientAlgoID", evt.ClientAlgoID, "type", evt.OrderType, "status", evt.AlgoStatus, "triggerPrice", evt.TriggerPrice)
				if e.liveBroker != nil {
					e.liveBroker.HandleAlgoOrderUpdate(evt)
				}
			},
			OnConnected: func() {
				if e.liveBroker == nil {
					return
				}
				sessionWg.Add(1)
				go func() {
					defer sessionWg.Done()
					rCtx, rCancel := context.WithTimeout(sessionCtx, 8*time.Second)
					defer rCancel()
					if err := e.liveBroker.RefreshAccount(rCtx); err != nil {
						slog.Warn("user stream reconnect account reconciliation failed", "err", err)
					}
				}()
			},
			OnListenKeyExpired: func() {
				slog.Warn("user data stream received listenKeyExpired, restarting session with fresh key")
				sessionCancel()
			},
		}

		streamMgr := ws.NewStreamManager(handlers)
		streamMgr.MaxConsecutiveDialFailures = 5
		streamMgr.StartSupervisedStream(sessionCtx, userStreamURL)

		// Saat session berhenti/batal, batalkan seluruh child goroutine session & tunggu sebelum hapus listenKey.
		sessionCancel()
		sessionWg.Wait()
		delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = e.client.DeleteListenKey(delCtx, listenKey)
		delCancel()
	}
}
