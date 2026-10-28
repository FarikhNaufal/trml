package test

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/broker"
	"trml/internal/model"
	"trml/internal/storage"
)

func TestDryRunBrokerFullCrashRecoveryIntegration(t *testing.T) {
	ctx := context.Background()

	// 1. Inisialisasi Database SQLite WAL di memori
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer func() { _ = db.Close() }()

	repo := storage.NewRepository(db)

	cfg := broker.DryRunConfig{
		MaxSlots:            3,
		AccountLeverage:     8.0,
		Tier1BEEnabled:      true,
		BreakEvenTriggerATR: 1.15,
		EntryOrderType:      "MARKET",
		TakerFeeRate:        0.0005,
		MakerFeeRate:        0.0002,
		SlippageRate:        0.0002,
	}

	// =========================================================================
	// TAHAP 1: Bot Instance 1 Berjalan Normal
	// =========================================================================
	b1 := broker.NewDryRunBroker(decimal.NewFromFloat(125.0), cfg)
	b1.OnPositionSavedFn = func(pos broker.VirtualOpenPosition) {
		_ = repo.SaveDryRunPosition(ctx, storage.StoredDryRunPosition{
			ClientOrderID:    pos.ClientOrderID,
			Symbol:           pos.Symbol,
			Side:             string(pos.Side),
			Qty:              pos.Qty,
			EntryPrice:       pos.EntryPrice,
			EntryFee:         pos.EntryFee,
			CurrentSLPrice:   pos.CurrentSLPrice,
			TPPrice:          pos.TPPrice,
			ATRAtEntry:       pos.ATRAtEntry,
			LastMarkPrice:    pos.LastMarkPrice,
			MarkUpdatedAt:    pos.MarkUpdatedAt,
			OpenedAt:         pos.OpenTime,
			IsBreakEvenMoved: pos.IsBreakEvenMoved,
			SignalAuditID:    pos.SignalAuditID,
			MaxMAE:           pos.MaxMAE,
			MaxMFE:           pos.MaxMFE,
		})
	}
	b1.OnPositionDeletedFn = func(clientOrderIDOrSymbol string) {
		_ = repo.DeleteDryRunPosition(ctx, clientOrderIDOrSymbol)
	}
	b1.OnTradeClosedFn = func(rec broker.TradeClosedRecord) {
		_ = repo.LogTrade(ctx, rec.ClientOrderID, rec.Symbol, rec.Side, rec.Qty, rec.EntryPrice, rec.ExitPrice, rec.SLPrice, rec.TPPrice, rec.NetPnL, rec.ExitReason, rec.OpenedAt, rec.ClosedAt, rec.MAE, rec.MFE, rec.SignalAuditID)
	}

	now := time.Now().UTC()

	// Trade 1: NEARUSDT SHORT (Signal #4369)
	intentNEAR := model.OrderIntent{
		ClientOrderID: "SIM-NEARUSDT-4369",
		Symbol:        "NEARUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromInt(39),
		LimitPrice:    decimal.NewFromFloat(4.4600),
		SLPrice:       decimal.NewFromFloat(4.5050),
		TPPrice:       decimal.NewFromFloat(4.3930),
		ATRAtEntry:    decimal.NewFromFloat(0.029676),
		CreatedAt:     now,
		SignalAuditID: 4369,
	}

	// Trade 2: QNTUSDT SHORT
	intentQNT := model.OrderIntent{
		ClientOrderID: "SIM-QNTUSDT-4370",
		Symbol:        "QNTUSDT",
		Side:          model.SideShort,
		Qty:           decimal.NewFromInt(2),
		LimitPrice:    decimal.NewFromFloat(180.0),
		SLPrice:       decimal.NewFromFloat(183.0),
		TPPrice:       decimal.NewFromFloat(175.0),
		ATRAtEntry:    decimal.NewFromFloat(2.0),
		CreatedAt:     now,
		SignalAuditID: 4370,
	}

	ok, err := b1.SubmitLimitEntry(intentNEAR)
	if !ok || err != nil {
		t.Fatalf("failed to submit NEAR: %v", err)
	}
	ok, err = b1.SubmitLimitEntry(intentQNT)
	if !ok || err != nil {
		t.Fatalf("failed to submit QNT: %v", err)
	}

	// Harga NEAR turun ke 4.4259 -> bergerak 0.0341 (> 1.15 * 0.029676 = 0.034127) -> trigger Break-Even
	// BE offset rate: min 0.0010 -> SL baru = 4.46 * (1 - 0.0012) = 4.4546
	b1.OnRealtimeTick("NEARUSDT", 4.4250, 4.4260, 4.4255, 4.4255, 10.0, true, now.Add(time.Second))

	// Tunggu queue event memproses ke SQLite
	time.Sleep(50 * time.Millisecond)

	// Verifikasi isi database SQLite
	stored1, err := repo.GetAllDryRunPositions(ctx)
	if err != nil || len(stored1) != 2 {
		t.Fatalf("expected 2 stored positions, got %d, err=%v", len(stored1), err)
	}

	var nearStored *storage.StoredDryRunPosition
	for i := range stored1 {
		if stored1[i].Symbol == "NEARUSDT" {
			nearStored = &stored1[i]
		}
	}
	if nearStored == nil || !nearStored.IsBreakEvenMoved {
		t.Fatalf("expected NEAR to have break-even moved in DB: %+v", nearStored)
	}

	// =========================================================================
	// TAHAP 2: SIMULASI CRASH / DOCKER REBUILD (Instance 1 Mati Tiba-tiba)
	// =========================================================================
	b1.Close()

	// =========================================================================
	// TAHAP 3: Bot Instance 2 Menyala Kembali (Recovery State)
	// =========================================================================
	b2 := broker.NewDryRunBroker(decimal.NewFromFloat(125.0), cfg)
	defer b2.Close()
	b2.OnPositionSavedFn = b1.OnPositionSavedFn
	b2.OnPositionDeletedFn = b1.OnPositionDeletedFn
	b2.OnTradeClosedFn = b1.OnTradeClosedFn

	// Pulihkan posisi dari database
	recoveredPositions, err := repo.GetAllDryRunPositions(ctx)
	if err != nil || len(recoveredPositions) != 2 {
		t.Fatalf("recovery failed: expected 2 positions, got %d", len(recoveredPositions))
	}

	for _, sp := range recoveredPositions {
		b2.RestoreOpenPosition(&broker.VirtualOpenPosition{
			ClientOrderID:    sp.ClientOrderID,
			Symbol:           sp.Symbol,
			Side:             model.Side(sp.Side),
			Qty:              sp.Qty,
			EntryPrice:       sp.EntryPrice,
			EntryFee:         sp.EntryFee,
			CurrentSLPrice:   sp.CurrentSLPrice,
			TPPrice:          sp.TPPrice,
			ATRAtEntry:       sp.ATRAtEntry,
			LastMarkPrice:    sp.LastMarkPrice,
			MarkUpdatedAt:    sp.MarkUpdatedAt,
			OpenTime:         sp.OpenedAt,
			IsBreakEvenMoved: sp.IsBreakEvenMoved,
			SignalAuditID:    sp.SignalAuditID,
			MaxMAE:           sp.MaxMAE,
			MaxMFE:           sp.MaxMFE,
		})
	}

	// Verifikasi instance 2 mengenali posisi dan slot aktif
	if b2.GetActiveSlotCount() != 2 {
		t.Errorf("expected 2 active slots on b2, got %d", b2.GetActiveSlotCount())
	}
	if !b2.HasActiveOrderOrPosition("NEARUSDT") {
		t.Errorf("expected b2 to have active NEARUSDT")
	}
	if !b2.HasActiveOrderOrPosition("QNTUSDT") {
		t.Errorf("expected b2 to have active QNTUSDT")
	}

	activeDTOs := b2.GetActivePositions()
	if len(activeDTOs) != 2 {
		t.Fatalf("expected 2 active position DTOs on dashboard, got %d", len(activeDTOs))
	}

	// =========================================================================
	// TAHAP 4: NEARUSDT Menyentuh Take Profit di Instance 2
	// =========================================================================
	// TP NEAR adalah 4.3930. Mark price menyentuh 4.3920
	b2.OnRealtimeTick("NEARUSDT", 4.3920, 4.3930, 4.3920, 4.3920, 50.0, true, now.Add(2*time.Minute))

	// Tunggu FIFO queue menulis ke database
	time.Sleep(50 * time.Millisecond)

	// Verifikasi di DB: NEAR harus terhapus, QNT masih ada
	storedAfterTP, _ := repo.GetAllDryRunPositions(ctx)
	if len(storedAfterTP) != 1 {
		t.Fatalf("expected 1 stored position after TP, got %d", len(storedAfterTP))
	}
	if storedAfterTP[0].Symbol != "QNTUSDT" {
		t.Fatalf("expected remaining position to be QNTUSDT, got %s", storedAfterTP[0].Symbol)
	}

	// =========================================================================
	// TAHAP 5: SIMULASI REBOOT KEDUA KALI (Instance 2 Mati, Instance 3 Menyala)
	// =========================================================================
	b3 := broker.NewDryRunBroker(decimal.NewFromFloat(125.0), cfg)
	defer b3.Close()
	b3.OnPositionSavedFn = b1.OnPositionSavedFn
	b3.OnPositionDeletedFn = b1.OnPositionDeletedFn
	b3.OnTradeClosedFn = b1.OnTradeClosedFn

	rebootStored, _ := repo.GetAllDryRunPositions(ctx)
	if len(rebootStored) != 1 || rebootStored[0].Symbol != "QNTUSDT" {
		t.Fatalf("reboot check failed: expected only QNTUSDT, got %d", len(rebootStored))
	}

	// Pulihkan QNT ke b3
	for _, sp := range rebootStored {
		b3.RestoreOpenPosition(&broker.VirtualOpenPosition{
			ClientOrderID:    sp.ClientOrderID,
			Symbol:           sp.Symbol,
			Side:             model.Side(sp.Side),
			Qty:              sp.Qty,
			EntryPrice:       sp.EntryPrice,
			EntryFee:         sp.EntryFee,
			CurrentSLPrice:   sp.CurrentSLPrice,
			TPPrice:          sp.TPPrice,
			ATRAtEntry:       sp.ATRAtEntry,
			LastMarkPrice:    sp.LastMarkPrice,
			MarkUpdatedAt:    sp.MarkUpdatedAt,
			OpenTime:         sp.OpenedAt,
			IsBreakEvenMoved: sp.IsBreakEvenMoved,
			SignalAuditID:    sp.SignalAuditID,
		})
	}

	// QNT menyentuh Stop Loss (SL 183.0, mark price melompat ke 183.1)
	b3.OnRealtimeTick("QNTUSDT", 183.1, 183.2, 183.1, 183.1, 5.0, true, now.Add(5*time.Minute))

	time.Sleep(50 * time.Millisecond)

	finalStored, _ := repo.GetAllDryRunPositions(ctx)
	if len(finalStored) != 0 {
		t.Fatalf("expected 0 open positions in DB after both closed, got %d", len(finalStored))
	}
}
