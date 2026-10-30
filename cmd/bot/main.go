package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
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

	statesMu     sync.RWMutex
	activeStates map[string]*model.SymbolState
	watchedCoins []string

	filtersMu       sync.RWMutex
	exchangeFilters map[string]model.ExchangeFilter

	cooldownMu    sync.RWMutex
	cooldownExits map[string]time.Time

	evaluatorCfg strategy.EvaluatorConfig
	scannerCfg   scanner.ScannerConfig

	verifiedMu      sync.RWMutex
	verifiedSymbols map[string]bool
	streamCancel    context.CancelFunc
	streamMu        sync.Mutex
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

	// Inisialisasi Dedicated Read-Only Web Reader (Zero Lock Contention)
	webReader, err := web.OpenReadOnlyReader(cfg.DatabasePath, cfg.Leverage)
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
	engine := &BotEngine{
		cfg:             cfg,
		repo:            repo,
		client:          client,
		activeStates:    make(map[string]*model.SymbolState),
		exchangeFilters: make(map[string]model.ExchangeFilter),
		cooldownExits:   make(map[string]time.Time),
		verifiedSymbols: make(map[string]bool),
		evaluatorCfg: strategy.EvaluatorConfig{
			MaxSlots:              cfg.MaxSlots,
			MaxBookStaleness:      cfg.MaxBookStaleness,
			MaxMarkStaleness:      cfg.MaxMarkStaleness,
			MaxDataStaleness:      cfg.MaxDataStaleness,
			MinOIInterval:         cfg.MinOIInterval,
			MaxOIInterval:         cfg.MaxOIInterval,
			MaxOIStaleness:        cfg.MaxOIStaleness,
			MaxSpreadPct:          cfg.MaxSpreadPct,
			MinNATRPct:            cfg.MinNATRPct,
			MaxNATRPct:            cfg.MaxNATRPct,
			EMAPeriod:             200,
			ATRPeriod:             14,
			DeltaZScoreWindow:     50,
			MinBarDeltaZScore:     cfg.MinBounceZScore,
			MinOIDeltaPct:         cfg.MinOIDeltaPct,
			MinOIDeltaLongPct:     cfg.MinOIDeltaLongPct,
			MinOIDeltaShortPct:    cfg.MinOIDeltaShortPct,
			MaxAdverseFundingRate:  0.0005,
			PullbackToleranceATR:   cfg.PullbackToleranceATR,
			MinBounceZScore:        cfg.MinBounceZScore,
			MinBounceZScoreLong:    cfg.MinBounceZScoreLong,
			MinBounceZScoreShort:   cfg.MinBounceZScoreShort,
			MinEMA200DistanceATR:   cfg.MinEMA200DistanceATR,
			LookbackImpulseBars:    cfg.LookbackImpulseBars,
			RetestLookbackBars:     cfg.RetestLookbackBars,
			MinImpulseATR:          cfg.MinImpulseATR,
			MaxEntryDistanceATR:    cfg.MaxEntryDistanceATR,
			EntryOrderType:         cfg.EntryOrderType,
			BTCGateEnabled:         cfg.BTCGateEnabled,
			BTCMax5mDumpPct:        cfg.BTCMax5mDumpPct,
			BTCMax5mPumpPct:        cfg.BTCMax5mPumpPct,
			BTCGatePersistenceBars: cfg.BTCGatePersistenceBars,
			AllowedSide:            cfg.AllowedSide,
			MaxFrenzyRVOL:            cfg.MaxFrenzyRVOL,
			MaxCounterImpulseATR:     cfg.MaxCounterImpulseATR,
			CooldownAfterExitMinutes: cfg.CooldownAfterExitMinutes,
			VWAPSessionWarmupMinutes: cfg.VWAPSessionWarmupMinutes,
			Sizing: strategy.SizingConfig{
				RiskPerTradePct:               cfg.RiskPerTradePct,
				MaxSlotMarginFraction:         cfg.MaxSlotMarginFrac,
				AvailableBalanceBufferFraction: cfg.MaxMarginAvailFrac,
				AccountLeverage:               cfg.Leverage,
				SLMultiplier:                  cfg.SLATRMultiplier,
				TPMultiplier:                  cfg.TPATRMultiplier,
				TP1Multiplier:                 cfg.PartialTP1ATRMult,
				MinSLDistancePct:              cfg.MinSLDistancePct,
				MakerFeeRate:                  cfg.MakerFeeRate,
				TakerFeeRate:                  cfg.TakerFeeRate,
				SlippageRate:                  cfg.SlippageRate,
				EntryOrderType:                cfg.EntryOrderType,
			},
		},
		scannerCfg: scanner.ScannerConfig{
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
			BootstrapCount:        1000,
			DeltaWindow:           50,
		},
	}

	// [COOLDOWN-PERSISTENCE] Hydrate cooldown state dari trade_ledger untuk mencegah amnesia saat restart
	cooldownMins := cfg.CooldownAfterExitMinutes
	if cooldownMins <= 0 {
		cooldownMins = 30
	}
	cooldownCutoff := time.Now().UTC().Add(-time.Duration(cooldownMins) * time.Minute)
	if recentExits, err := repo.GetRecentExits(ctx, cooldownCutoff); err == nil && len(recentExits) > 0 {
		engine.cooldownMu.Lock()
		for sym, exitTime := range recentExits {
			engine.cooldownExits[sym] = exitTime
		}
		engine.cooldownMu.Unlock()
		slog.Info("hydrated active cooldowns from trade ledger", "count", len(recentExits), "since", cooldownCutoff)
	}

	if cfg.DryRun {
		slog.Info("initializing Heuristic Dry-Run Volume-Through-Limit Simulator")
		initialEquity := cfg.InitialEquity
		if initialEquity.LessThanOrEqual(decimal.Zero) {
			initialEquity = decimal.NewFromFloat(1000.0)
		}

		now := time.Now().UTC()
		epochDay := now.Unix() / 86400
		startEq, realEq, tripped, found, _ := repo.GetDailyState(ctx, epochDay)
		if found && startEq.GreaterThan(decimal.Zero) {
			slog.Info("recovered daily state from database", "startEquity", startEq, "realizedEquity", realEq, "breakerTripped", tripped)
			initialEquity = realEq
		} else {
			// [C-05] Jika hari baru belum memiliki record, warisi realized equity dari sesi terakhir agar akumulasi profit tidak reset
			_, lastRealEq, _, _, latestFound, _ := repo.GetLatestDailyState(ctx)
			if latestFound && lastRealEq.GreaterThan(decimal.Zero) {
				slog.Info("recovered daily state from previous session", "realizedEquity", lastRealEq)
				initialEquity = lastRealEq
			}
		}

		makerFeeFloat, _ := cfg.MakerFeeRate.Float64()
		takerFeeFloat, _ := cfg.TakerFeeRate.Float64()
		slippageFloat, _ := cfg.SlippageRate.Float64()

		dryRunCfg := broker.DryRunConfig{
			MaxSlots:                 cfg.MaxSlots,
			MaxDailyDrawdownPct:      cfg.MaxDailyDrawdown,
			AccountLeverage:          cfg.Leverage,
			BreakEvenTriggerATR:      cfg.BreakEvenTriggerATR,
			Tier1BEEnabled:           cfg.Tier1BEEnabled,
			Tier1BETriggerLongATR:    cfg.Tier1BETriggerLongATR,
			Tier1BETriggerShortATR:   cfg.Tier1BETriggerShortATR,
			Tier2ProfitLockATR:       cfg.Tier2ProfitLockATR,
			Tier2ProfitLockOffsetATR: cfg.Tier2ProfitLockOffsetATR,
			PartialTPEnabled:         cfg.PartialTPEnabled,
			PartialTP1Ratio:          cfg.PartialTP1Ratio,
			PartialTP1ATRMult:        cfg.PartialTP1ATRMult,
			TrailingEnabled:          cfg.TrailingEnabled,
			TrailingActivateATR:      cfg.TrailingActivateATR,
			TrailingDistanceATR:      cfg.TrailingDistanceATR,
			MinBreakEvenOffsetPct:    0.0010,
			LimitOrderTimeout:        cfg.LimitOrderTimeout,
			MaxHoldingDuration:       cfg.MaxHoldingDuration,
			MaxMarkStaleness:         cfg.MaxMarkStaleness,
			MakerFeeRate:             makerFeeFloat,
			TakerFeeRate:             takerFeeFloat,
			SlippageRate:             slippageFloat,
			EntryOrderType:           cfg.EntryOrderType,
		}
		engine.dryRunBroker = broker.NewDryRunBroker(initialEquity, dryRunCfg)
		defer engine.dryRunBroker.Close()
		engine.dryRunBroker.OnTradeClosedFn = func(rec broker.TradeClosedRecord) {
			slog.Info("TRADE CLOSED (DRY-RUN)", "symbol", rec.Symbol, "side", rec.Side, "pnl", rec.NetPnL, "reason", rec.ExitReason, "maePct", rec.MAE, "mfePct", rec.MFE, "signalAuditID", rec.SignalAuditID)
			_ = repo.LogTrade(context.Background(), rec.ClientOrderID, rec.Symbol, rec.Side, rec.Qty, rec.EntryPrice, rec.ExitPrice, rec.SLPrice, rec.TPPrice, rec.NetPnL, rec.ExitReason, rec.OpenedAt, rec.ClosedAt, rec.MAE, rec.MFE, rec.SignalAuditID)
			if rec.ExitReason != "PARTIAL_TAKE_PROFIT" {
				engine.cooldownMu.Lock()
				engine.cooldownExits[rec.Symbol] = rec.ClosedAt
				engine.cooldownMu.Unlock()

				engine.statesMu.RLock()
				st := engine.activeStates[rec.Symbol]
				engine.statesMu.RUnlock()
				if st != nil {
					st.SetLastExitTime(rec.ClosedAt)
				}
			}
			pureRealEq := engine.dryRunBroker.GetPureRealizedEquityClosedTradesUSDT()
			epochDay := time.Now().UTC().Unix() / 86400
			engine.dryRunBroker.Mu.Lock()
			startEq := engine.dryRunBroker.SessionStartEquity
			breakerHit := engine.dryRunBroker.CircuitBreakerTripped
			engine.dryRunBroker.Mu.Unlock()
			_ = repo.SaveDailyState(context.Background(), epochDay, startEq, pureRealEq, breakerHit)
		}

		engine.dryRunBroker.OnPositionSavedFn = func(pos broker.VirtualOpenPosition) {
			_ = repo.SaveDryRunPosition(context.Background(), storage.StoredDryRunPosition{
				ClientOrderID:    pos.ClientOrderID,
				Symbol:           pos.Symbol,
				Side:             string(pos.Side),
				Qty:              pos.Qty,
				InitialQty:       pos.InitialQty,
				EntryPrice:       pos.EntryPrice,
				EntryFee:         pos.EntryFee,
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

		// Pulihkan posisi terbuka dry-run dari database jika ada (tahan restart/rebuild)
		if storedPositions, err := repo.GetAllDryRunPositions(ctx); err == nil && len(storedPositions) > 0 {
			for _, sp := range storedPositions {
				engine.dryRunBroker.RestoreOpenPosition(&broker.VirtualOpenPosition{
					ClientOrderID:    sp.ClientOrderID,
					Symbol:           sp.Symbol,
					Side:             model.Side(sp.Side),
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
			MaxSlots:                 cfg.MaxSlots,
			MaxDailyDrawdownPct:      cfg.MaxDailyDrawdown,
			MaxAccountStaleness:      cfg.MaxAccountStale,
			LimitOrderTimeout:        cfg.LimitOrderTimeout,
			MaxHoldingDuration:       cfg.MaxHoldingDuration,
			BreakEvenTriggerATR:      cfg.BreakEvenTriggerATR,
			Tier1BEEnabled:           cfg.Tier1BEEnabled,
			Tier1BETriggerLongATR:    cfg.Tier1BETriggerLongATR,
			Tier1BETriggerShortATR:   cfg.Tier1BETriggerShortATR,
			Tier2ProfitLockATR:       cfg.Tier2ProfitLockATR,
			Tier2ProfitLockOffsetATR: cfg.Tier2ProfitLockOffsetATR,
			PartialTPEnabled:         cfg.PartialTPEnabled,
			PartialTP1Ratio:          cfg.PartialTP1Ratio,
			PartialTP1ATRMult:        cfg.PartialTP1ATRMult,
			TrailingEnabled:          cfg.TrailingEnabled,
			TrailingActivateATR:      cfg.TrailingActivateATR,
			TrailingDistanceATR:      cfg.TrailingDistanceATR,
			MinBreakEvenOffsetPct:    0.0010,
		}
		engine.liveBroker = broker.NewBinanceLiveBroker(liveCfg)
		if cfg.EntryOrderType == "MARKET" {
			engine.liveBroker.PlaceEntryFn = client.PlaceMarketEntry
		} else {
			engine.liveBroker.PlaceEntryFn = client.PlaceLimitEntry
		}
		engine.liveBroker.PlaceAlgoOrderFn = client.PlaceAlgoOrder
		engine.liveBroker.MarketCloseFn = client.PlaceMarketClose
		engine.liveBroker.CancelOrderFn = client.CancelOrder
		engine.liveBroker.CancelAlgoOrderFn = client.CancelAlgoOrder
		engine.liveBroker.QueryOrderFn = client.QueryOrder
		engine.liveBroker.GetExchangeFilterFn = func(symbol string) (model.ExchangeFilter, bool) {
			engine.filtersMu.RLock()
			defer engine.filtersMu.RUnlock()
			f, ok := engine.exchangeFilters[symbol]
			return f, ok
		}
		engine.liveBroker.EmergencyMarketCloseFn = func(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, reason string) error {
			slog.Warn("TRIGGERING EMERGENCY MARKET CLOSE", "symbol", symbol, "side", side, "qty", qty, "reason", reason)
			return client.EmergencyMarketClose(ctx, symbol, side, qty)
		}
		engine.liveBroker.OnPositionSavedFn = func(pos broker.LiveOpenPosition) {
			_ = repo.SaveLivePosition(context.Background(), storage.StoredLivePosition{
				Symbol:         pos.Symbol,
				Side:           string(pos.Side),
				Qty:            pos.Qty,
				InitialQty:     pos.InitialQty,
				EntryPrice:     pos.EntryPrice,
				SLOrderID:      pos.SLOrderID,
				SLClientAlgoID: pos.SLClientAlgoID,
				SLPrice:        pos.SLPrice,
				InitialSLPrice: pos.InitialSLPrice,
				TPOrderID:      pos.TPOrderID,
				TPClientAlgoID: pos.TPClientAlgoID,
				TPPrice:        pos.TPPrice,
				TP1Price:       pos.TP1Price,
				IsTP1Hit:       pos.IsTP1Hit,
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
		engine.liveBroker.OnTradeClosedFn = func(rec broker.TradeClosedRecord) {
			slog.Info("TRADE CLOSED (LIVE)", "symbol", rec.Symbol, "side", rec.Side, "pnl", rec.NetPnL, "reason", rec.ExitReason, "maePct", rec.MAE, "mfePct", rec.MFE, "signalAuditID", rec.SignalAuditID)
			_ = repo.LogTrade(context.Background(), rec.ClientOrderID, rec.Symbol, rec.Side, rec.Qty, rec.EntryPrice, rec.ExitPrice, rec.SLPrice, rec.TPPrice, rec.NetPnL, rec.ExitReason, rec.OpenedAt, rec.ClosedAt, rec.MAE, rec.MFE, rec.SignalAuditID)
			if rec.ExitReason != "PARTIAL_TAKE_PROFIT" {
				engine.cooldownMu.Lock()
				engine.cooldownExits[rec.Symbol] = rec.ClosedAt
				engine.cooldownMu.Unlock()

				engine.statesMu.RLock()
				st := engine.activeStates[rec.Symbol]
				engine.statesMu.RUnlock()
				if st != nil {
					st.SetLastExitTime(rec.ClosedAt)
				}
			}
		}
		engine.broker = engine.liveBroker
	}

	// 5. Sinkronisasi Jam Bursa & Pemulihan State Harian
	if err := client.SyncServerTime(ctx); err != nil {
		slog.Warn("failed to sync server time with exchange, falling back to local time", "err", err)
	} else {
		slog.Info("server time synced successfully", "timeOffsetMs", client.TimeOffset)
	}

	todayEpochDay := time.Now().UTC().Unix() / 86400
	startEq, realEq, breakerHit, found, err := repo.GetDailyState(ctx, todayEpochDay)
	if err == nil && found {
		slog.Info("recovered today's daily state from storage",
			"epochDay", todayEpochDay, "sessionStartEquity", startEq, "realizedEquity", realEq, "breakerHit", breakerHit)
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
	} else if err == nil && !found {
		// [C-05] Hari baru tanpa record: warisi realized equity hari sebelumnya sebagai sessionStartEquity hari ini
		_, lastRealEq, _, _, latestFound, errLatest := repo.GetLatestDailyState(ctx)
		if errLatest == nil && latestFound && lastRealEq.GreaterThan(decimal.Zero) {
			slog.Info("starting new daily session inheriting previous day's equity",
				"epochDay", todayEpochDay, "sessionStartEquity", lastRealEq)
			_ = repo.SaveDailyState(ctx, todayEpochDay, lastRealEq, lastRealEq, false)
			if engine.liveBroker != nil {
				engine.liveBroker.Mu.Lock()
				engine.liveBroker.SessionStartEpochDay = todayEpochDay
				engine.liveBroker.SessionStartEquityUSDT = lastRealEq
				engine.liveBroker.CircuitBreakerTripped = false
				engine.liveBroker.Mu.Unlock()
			} else if engine.dryRunBroker != nil {
				engine.dryRunBroker.Mu.Lock()
				engine.dryRunBroker.SessionDayEpochUTC = todayEpochDay
				engine.dryRunBroker.SessionStartEquity = lastRealEq
				engine.dryRunBroker.CircuitBreakerTripped = false
				engine.dryRunBroker.Mu.Unlock()
			}
		}
	}

	// 5b. Rekonsiliasi Posisi Live Saat Boot (TD-01 & TD-02)
	if engine.liveBroker != nil {
		slog.Info("reconciling live open positions with Binance exchange...")
		storedLivePositions, err := repo.GetAllLivePositions(ctx)
		if err != nil {
			slog.Error("failed to load stored live positions from database", "err", err)
		}

		exchangePositions, err := client.GetPositionRisk(ctx, "")
		if err != nil {
			slog.Error("failed to fetch position risk from Binance at boot reconciliation", "err", err)
			for _, sp := range storedLivePositions {
				engine.liveBroker.RestoreOpenPosition(&broker.LiveOpenPosition{
					Symbol:         sp.Symbol,
					Side:           model.Side(sp.Side),
					Qty:            sp.Qty,
					InitialQty:     sp.InitialQty,
					EntryPrice:     sp.EntryPrice,
					SLOrderID:      sp.SLOrderID,
					SLClientAlgoID: sp.SLClientAlgoID,
					SLPrice:        sp.SLPrice,
					InitialSLPrice: sp.InitialSLPrice,
					TPOrderID:      sp.TPOrderID,
					TPClientAlgoID: sp.TPClientAlgoID,
					TPPrice:        sp.TPPrice,
					TP1Price:       sp.TP1Price,
					IsTP1Hit:       sp.IsTP1Hit,
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
		} else {
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
					engine.liveBroker.RestoreOpenPosition(&broker.LiveOpenPosition{
						Symbol:         sp.Symbol,
						Side:           model.Side(sp.Side),
						Qty:            ep.PositionAmt.Abs(),
						InitialQty:     sp.InitialQty,
						EntryPrice:     ep.EntryPrice,
						SLOrderID:      sp.SLOrderID,
						SLClientAlgoID: sp.SLClientAlgoID,
						SLPrice:        sp.SLPrice,
						InitialSLPrice: sp.InitialSLPrice,
						TPOrderID:      sp.TPOrderID,
						TPClientAlgoID: sp.TPClientAlgoID,
						TPPrice:        sp.TPPrice,
						TP1Price:       sp.TP1Price,
						IsTP1Hit:       sp.IsTP1Hit,
						ATRAtEntry:     sp.ATRAtEntry,
						ProtectionTier: sp.ProtectionTier,
						SignalAuditID:  sp.SignalAuditID,
						OpenTime:       sp.OpenedAt,
						MaxMAE:         sp.MaxMAE,
						MaxMFE:         sp.MaxMFE,
						PeakPrice:      sp.PeakPrice,
						UpdatedAt:      sp.UpdatedAt,
					})
					slog.Info("reconciled & restored active live position from database and exchange",
						"symbol", sp.Symbol, "qty", ep.PositionAmt.Abs(), "entryPrice", ep.EntryPrice,
						"slPrice", sp.SLPrice, "tpPrice", sp.TPPrice)
				} else {
					slog.Warn("live position closed on exchange during downtime; cleaning up stored state", "symbol", sym)
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
					engine.liveBroker.RestoreOpenPosition(&broker.LiveOpenPosition{
						Symbol:     sym,
						Side:       side,
						Qty:        ep.PositionAmt.Abs(),
						EntryPrice: ep.EntryPrice,
						OpenTime:   time.Now().UTC(),
						UpdatedAt:  time.Now().UTC(),
					})
				}
			}
		}
	}

	// 6. Jalankan Siklus Scanning Pertama
	slog.Info("running initial market scan & bootstrap...")
	engine.runMarketScan(ctx)

	// 6. Mulai Background Workers
	var wg sync.WaitGroup

	// Worker 1: Periodic 15-minute Scanner
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				slog.Info("running periodic market scan...")
				engine.runMarketScan(ctx)
			}
		}
	}()

	// Worker 2: Periodic Open Interest Poller (setiap 1 menit agar sampel selalu segar)
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

	// Worker 3: Live Mode User Data Stream & Keepalive (jika bukan Dry-Run)
	if !cfg.DryRun && cfg.APIKey != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.runUserDataStreamSupervisor(ctx)
		}()
	}

	// Worker 4: Independent Watchdog untuk Broker Time-Stop & Order Timeouts (decoupled dari WS ticks)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				engine.broker.CheckTimeouts(time.Now().UTC())
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
			Port:              cfg.HTTPPort,
			Env:               envStr,
			MaxSlots:          cfg.MaxSlots,
			MaxDailyDrawdown:  cfg.MaxDailyDrawdown,
			Reader:            webReader,
			Broker:            engine.broker,
			GetScannerCoinsFn: engine.getScannerCoins,
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

	wg.Wait()
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


func (e *BotEngine) runMarketScan(ctx context.Context) {
	filters, err := e.client.FetchExchangeInfo(ctx)
	if err != nil {
		slog.Error("failed to fetch exchange info", "err", err)
		return
	}
	e.filtersMu.Lock()
	e.exchangeFilters = filters
	e.filtersMu.Unlock()

	volMap, err := e.client.Fetch24hTickers(ctx)
	if err != nil {
		slog.Error("failed to fetch 24h tickers", "err", err)
		return
	}

	bookMap, err := e.client.FetchBookTickers(ctx)
	if err != nil {
		slog.Error("failed to fetch book tickers", "err", err)
		return
	}

	fetchKlinesFn := func(symbol string, limit int) []model.Candle {
		k, err := e.client.FetchKlines(ctx, symbol, "5m", limit)
		if err != nil {
			slog.Debug("failed to fetch klines for symbol", "symbol", symbol, "err", err)
			return nil
		}
		return k
	}

	serverTime := time.Now().UTC()

	// C6 FIX: Hindari menahan statesMu write-lock saat melakukan REST fetch klines serial
	e.statesMu.RLock()
	localStates := make(map[string]*model.SymbolState, len(e.activeStates))
	for k, v := range e.activeStates {
		localStates[k] = v
	}
	e.statesMu.RUnlock()

	selected := scanner.RunBulkMarketScanner(
		e.scannerCfg,
		localStates,
		filters,
		volMap,
		bookMap,
		fetchKlinesFn,
		serverTime,
	)

	e.statesMu.Lock()
	oldWatched := make([]string, len(e.watchedCoins))
	copy(oldWatched, e.watchedCoins)

	for k, v := range localStates {
		if existing, exists := e.activeStates[k]; !exists {
			e.activeStates[k] = v
			e.cooldownMu.RLock()
			if exitTime, ok := e.cooldownExits[k]; ok {
				v.SetLastExitTime(exitTime)
			}
			e.cooldownMu.RUnlock()
		} else {
			e.cooldownMu.RLock()
			if exitTime, ok := e.cooldownExits[k]; ok {
				existing.SetLastExitTime(exitTime)
			}
			e.cooldownMu.RUnlock()
		}
	}
	// Pertahankan simbol yang sedang memiliki posisi terbuka atau order aktif agar stream feed tidak putus
	finalWatched := make([]string, 0, len(selected)+3)
	finalWatched = append(finalWatched, selected...)
	if e.dryRunBroker != nil {
		e.dryRunBroker.Mu.Lock()
		for sym := range e.dryRunBroker.OpenPositions {
			if !slices.Contains(finalWatched, sym) {
				finalWatched = append(finalWatched, sym)
			}
		}
		for sym := range e.dryRunBroker.PendingOrders {
			if !slices.Contains(finalWatched, sym) {
				finalWatched = append(finalWatched, sym)
			}
		}
		e.dryRunBroker.Mu.Unlock()
	} else if e.liveBroker != nil {
		e.liveBroker.Mu.Lock()
		for sym := range e.liveBroker.ActivePositions {
			if !slices.Contains(finalWatched, sym) {
				finalWatched = append(finalWatched, sym)
			}
		}
		for _, ord := range e.liveBroker.OrdersByClient {
			if ord.IsEntry && (ord.Status == model.StatusNew || ord.Status == model.StatusPendingNew) && !slices.Contains(finalWatched, ord.Symbol) {
				finalWatched = append(finalWatched, ord.Symbol)
			}
		}
		e.liveBroker.Mu.Unlock()
	}

	if len(finalWatched) > 0 {
		e.watchedCoins = finalWatched
		// M3 FIX: Pangkas simbol lama dari activeStates yang sudah tidak di-watch dan tidak memiliki posisi/order aktif
		watchedSet := make(map[string]bool, len(finalWatched)+1)
		for _, sym := range finalWatched {
			watchedSet[sym] = true
		}
		watchedSet[BenchmarkSymbol] = true // Jaga BenchmarkSymbol agar tidak terhapus dari activeStates
		for sym := range e.activeStates {
			if !watchedSet[sym] {
				delete(e.activeStates, sym)
			}
		}
	}
	e.statesMu.Unlock()

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
		btcSt.IsLeverageIsolatedVerified = true
		e.activeStates[BenchmarkSymbol] = btcSt
	}
	e.statesMu.Unlock()

	btcFetchCount := 100
	if !exists {
		btcFetchCount = e.scannerCfg.BootstrapCount
	}
	klines, kErr := e.client.FetchKlines(ctx, BenchmarkSymbol, "5m", btcFetchCount)
	if kErr == nil && len(klines) > 0 {
		// [C-03] Filter candle yang belum tutup agar tidak merusak state makro BTC
		filteredKlines, vErr := model.ValidateAndFilterCandles(klines, time.Now().UTC())
		if vErr == nil && len(filteredKlines) > 0 {
			btcSt.Mu.Lock()
			for _, k := range filteredKlines {
				btcSt.ApplyClosedCandleLocked(k, e.scannerCfg.BootstrapCount, e.scannerCfg.DeltaWindow+1)
			}
			btcSt.Mu.Unlock()
		}
	} else if kErr != nil {
		slog.Warn("periodic BTC klines sync failed", "err", kErr)
	}

	slog.Info("market scan completed", "selectedCount", len(finalWatched), "symbols", finalWatched)

	// Jika mode Live, verifikasi One-Way mode dan leverage untuk simbol terpilih (M11 FIX & M4 Cache)
	if !e.cfg.DryRun {
		for _, sym := range finalWatched {
			e.verifiedMu.RLock()
			alreadyVerified := e.verifiedSymbols[sym]
			e.verifiedMu.RUnlock()

			if alreadyVerified {
				e.statesMu.Lock()
				if st, exists := e.activeStates[sym]; exists {
					st.IsLeverageIsolatedVerified = true
				}
				e.statesMu.Unlock()
				continue
			}
			ok, err := e.client.InitAndVerifySymbol(ctx, sym, int(e.cfg.Leverage))
			if err != nil || !ok {
				slog.Warn("symbol setup failed; ignoring symbol", "symbol", sym, "err", err)
			} else {
				e.verifiedMu.Lock()
				e.verifiedSymbols[sym] = true
				e.verifiedMu.Unlock()
				e.statesMu.Lock()
				if st, exists := e.activeStates[sym]; exists {
					st.IsLeverageIsolatedVerified = true
				}
				e.statesMu.Unlock()
			}
		}
	} else {
		e.statesMu.Lock()
		for _, sym := range finalWatched {
			if st, exists := e.activeStates[sym]; exists {
				st.IsLeverageIsolatedVerified = true
			}
		}
		e.statesMu.Unlock()
	}

	// Segera muat Open Interest awal untuk simbol terpilih agar tidak MISSING_OI_DATA
	e.pollOpenInterest(ctx)

	// Diff check simbol watched (P2-2: Anti-WebSocket Churn)
	symbolsChanged := false
	if len(oldWatched) != len(finalWatched) {
		symbolsChanged = true
	} else {
		oldSet := make(map[string]bool, len(oldWatched))
		for _, s := range oldWatched {
			oldSet[s] = true
		}
		for _, s := range finalWatched {
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
		slog.Info("watched symbols changed, updating market streams", "previous", oldWatched, "current", finalWatched)
		e.restartMarketStream(ctx, finalWatched)
	} else {
		slog.Info("watched symbols unchanged, retaining active market streams", "count", len(finalWatched))
	}
}

func (e *BotEngine) pollOpenInterest(ctx context.Context) {
	e.statesMu.RLock()
	symbols := make([]string, len(e.watchedCoins))
	copy(symbols, e.watchedCoins)
	e.statesMu.RUnlock()

	now := time.Now().UTC()
	for _, sym := range symbols {
		e.statesMu.RLock()
		st := e.activeStates[sym]
		e.statesMu.RUnlock()
		if st == nil {
			continue
		}

		// Pengawal 3: Bootstrap histori OI jika buffer masih kurang (< 2 sampel)
		st.Mu.Lock()
		needsBootstrap := len(st.OISamples) < 2
		st.Mu.Unlock()

		if needsBootstrap {
			hist, hErr := e.client.FetchOpenInterestHist(ctx, sym, 3)
			if hErr == nil && len(hist) > 0 {
				samples := make([]model.OISample, 0, len(hist))
				for _, item := range hist {
					val, pErr := strconv.ParseFloat(item.SumOpenInterest, 64) // Pengawal 1: unit kontrak sumOpenInterest
					if pErr == nil && val > 0 {
						samples = append(samples, model.OISample{
							Value:        val,
							Timestamp:    time.UnixMilli(item.Timestamp).UTC(), // Pengawal 4: Timestamp asli bursa dipertahankan
							Bootstrapped: true,                                  // Pengawal 5: Flag Bootstrapped
						})
					}
				}
				if len(samples) > 0 {
					st.Mu.Lock()
					st.SeedOpenInterestBootstrapLocked(samples, now)
					st.Mu.Unlock()
					slog.Info("bootstrapped historical open interest", "symbol", sym, "samplesCount", len(samples))
				}
			} else if hErr != nil {
				// Pengawal 2: Graceful degradation ke polling riil jika 404 / kosong
				slog.Debug("historical open interest unavailable, falling back to live poll", "symbol", sym, "err", hErr)
			}
		}

		oi, err := e.client.FetchOpenInterest(ctx, sym)
		if err != nil {
			continue
		}
		st.Mu.Lock()
		st.RecordOpenInterestLocked(oi, now)
		st.Mu.Unlock()
	}
}

func (e *BotEngine) restartMarketStream(parentCtx context.Context, symbols []string) {
	e.streamMu.Lock()
	defer e.streamMu.Unlock()

	if e.streamCancel != nil {
		e.streamCancel()
	}

	if len(symbols) == 0 {
		return
	}

	streamCtx, cancel := context.WithCancel(parentCtx)
	publicCtx, publicCancel := context.WithCancel(streamCtx)
	marketCtx, marketCancel := context.WithCancel(streamCtx)
	e.streamCancel = func() {
		publicCancel()
		marketCancel()
		cancel()
	}

	var publicStreams, marketStreams []string
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
		marketStreams = append(marketStreams,
			fmt.Sprintf("%s@aggTrade", lower),
			fmt.Sprintf("%s@markPrice@1s", lower),
			fmt.Sprintf("%s@kline_5m", lower),
		)
	}

	publicURL := ws.BuildCombinedStreamURL(e.cfg.WSPublicURL, publicStreams)
	marketURL := ws.BuildCombinedStreamURL(e.cfg.WSMarketURL, marketStreams)

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
				markPrice = st.MarkPrice
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
				markPrice = st.MarkPrice
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
		},
		OnKline5m: func(symbol string, candle model.Candle, isClosed bool) {
			if !isClosed {
				return
			}
			slog.Info("5m candle closed received", "symbol", symbol, "close", candle.Close, "openTime", candle.OpenTime.Format(time.RFC3339))
			e.statesMu.RLock()
			st := e.activeStates[symbol]
			e.statesMu.RUnlock()
			if st == nil {
				return
			}

			st.Mu.Lock()
			okApplied := st.ApplyClosedCandleLocked(candle, e.scannerCfg.BootstrapCount, e.scannerCfg.DeltaWindow+1)
			st.Mu.Unlock()

			if !okApplied {
				slog.Debug("closed candle skipped by monotonic deduplication", "symbol", symbol, "openTime", candle.OpenTime)
				return
			}

			if symbol == BenchmarkSymbol {
				// State BTCUSDT telah terbarui sebagai benchmark makro, jangan perdagangkan BTC
				return
			}

			now := time.Now().UTC()
			snap := st.Snapshot(now)

			// Evaluasi Sinyal
			activeSlots := e.broker.GetActiveSlotCount()
			availBal := e.broker.GetAvailableBalanceUSDT()
			totEq := e.broker.GetEffectiveTotalEquityUSDT()

			// C3 & M1 FIX: Evaluasi sinyal dengan equity, available balance, status drawdown, dan active symbol riil
			isDailyBreached := e.broker.GetDailyTotalDrawdownPct() <= e.cfg.MaxDailyDrawdown
			hasActiveSymbol := e.broker.HasActiveOrderOrPosition(snap.Symbol)

			var btcSnap *model.MarketSnapshot
			if e.cfg.BTCGateEnabled {
				e.statesMu.RLock()
				if btcSt := e.activeStates[BenchmarkSymbol]; btcSt != nil {
					s := btcSt.Snapshot(now)
					btcSnap = &s
				}
				e.statesMu.RUnlock()
			}

			intent, ok, reason, subReason := strategy.EvaluateSignal(
				snap,
				btcSnap,
				activeSlots,
				isDailyBreached,
				hasActiveSymbol,
				totEq,
				availBal,
				e.evaluatorCfg,
			)

			// Simpan audit log lengkap dengan metrik analitik riil
			decision := "REJECT"
			if ok {
				decision = "APPROVE"
			}
			ema200 := indicator.CalculateEMA(snap.ClosedCandles, e.evaluatorCfg.EMAPeriod)
			atr14 := indicator.CalculateATR(snap.ClosedCandles, e.evaluatorCfg.ATRPeriod)
			zScore, _ := indicator.CalculateBarDeltaZScore(snap.ClosedBarDeltasUSDT, e.evaluatorCfg.DeltaZScoreWindow)
			var oiDelta float64
			if snap.PrevOpenInterest > 0 {
				oiDelta = ((snap.CurrOpenInterest - snap.PrevOpenInterest) / snap.PrevOpenInterest) * 100.0
			}

			spreadPct := 0.0
			if snap.BestBid > 0 && snap.BestAsk > snap.BestBid {
				spreadPct = ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
			}
			rvol := indicator.CalculateRVOL(snap.ClosedCandles, 20)

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
			} else if candle.Close > ema200 {
				evaluatedSide = "LONG"
			} else if candle.Close < ema200 {
				evaluatedSide = "SHORT"
			}
			slog.Info("signal evaluated", "symbol", snap.Symbol, "side", evaluatedSide, "decision", decision, "reason", reason, "subReason", subReason, "oiBootstrapped", snap.IsOIBootstrapped)

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
			)
			if err != nil {
				slog.Error("failed to log signal audit", "symbol", snap.Symbol, "err", err)
			}

			if ok && reason == "SIGNAL_APPROVED" {
				intent.SignalAuditID = sigID
				slog.Info("SIGNAL APPROVED! Spawning async broker submission...", "symbol", intent.Symbol, "side", intent.Side, "qty", intent.Qty, "price", intent.LimitPrice, "signalAuditID", sigID)
				go func(targetIntent model.OrderIntent) {
					reserved, err := e.broker.SubmitLimitEntry(targetIntent)
					if reserved && err == nil {
						slog.Info("ENTRY ORDER ACCEPTED BY BROKER", "symbol", targetIntent.Symbol, "clientOrderID", targetIntent.ClientOrderID)
					} else {
						slog.Warn("ENTRY ORDER REJECTED BY BROKER", "symbol", targetIntent.Symbol, "err", err)
					}
				}(intent)
			}
		},
	}

	publicMgr := ws.NewStreamManager(handlers)
	go publicMgr.StartSupervisedStream(publicCtx, publicURL)

	marketMgr := ws.NewStreamManager(handlers)
	go marketMgr.StartSupervisedStream(marketCtx, marketURL)
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

		// Keepalive setiap 30 menit; jika gagal, batalkan session agar loop membuat kunci baru
		go func(lKey string) {
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
			OnOrderTradeUpdate: func(clientOrderID string, exchangeOrderID int64, symbol string, status model.OrderLifecycleStatus, cumFilledQty, avgPrice, realizedProfit decimal.Decimal) {
				slog.Info("USER ORDER UPDATE", "symbol", symbol, "orderID", exchangeOrderID, "status", status, "cumQty", cumFilledQty, "avgPrice", avgPrice, "realizedProfit", realizedProfit)
				if e.liveBroker != nil {
					e.liveBroker.HandleUserDataOrderUpdate(clientOrderID, exchangeOrderID, symbol, status, cumFilledQty, avgPrice, realizedProfit)
				}
			},
			OnAccountUpdate: func(walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, eventTime time.Time) {
				slog.Debug("USER ACCOUNT UPDATE", "wallet", walletBal, "unrealizedPnL", unrealizedPnL, "avail", availBal)
				var breakerTripped bool
				startEquity := walletBal.Add(unrealizedPnL)
				if e.liveBroker != nil {
					e.liveBroker.HandleAccountUpdate(walletBal, unrealizedPnL, availBal, positions, eventTime)
					e.liveBroker.Mu.Lock()
					breakerTripped = e.liveBroker.CircuitBreakerTripped
					if e.liveBroker.SessionStartEquityUSDT.GreaterThan(decimal.Zero) {
						startEquity = e.liveBroker.SessionStartEquityUSDT
					}
					e.liveBroker.Mu.Unlock()
				}
				epochDay := eventTime.Unix() / 86400
				_ = e.repo.SaveDailyState(ctx, epochDay, startEquity, walletBal.Add(unrealizedPnL), breakerTripped)
			},
			OnListenKeyExpired: func() {
				slog.Warn("user data stream received listenKeyExpired, restarting session with fresh key")
				sessionCancel()
			},
		}

		streamMgr := ws.NewStreamManager(handlers)
		streamMgr.StartSupervisedStream(sessionCtx, userStreamURL)

		// Saat session berhenti/batal, hapus listenKey lama sebelum membuat yang baru
		delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = e.client.DeleteListenKey(delCtx, listenKey)
		delCancel()
		sessionCancel()
	}
}
