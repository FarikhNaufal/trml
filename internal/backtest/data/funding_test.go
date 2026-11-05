package data

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestFundingManager_LookupAndSettlement(t *testing.T) {
	fm := NewFundingManager("")

	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t8 := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	t16 := time.Date(2026, 10, 1, 16, 0, 0, 0, time.UTC)

	rates := []HistoricalFundingRate{
		{FundingTime: t0, FundingRate: 0.0001, IntervalHours: 8},
		{FundingTime: t8, FundingRate: -0.0002, IntervalHours: 8},
		{FundingTime: t16, FundingRate: 0.0003, IntervalHours: 8},
	}

	fm.SetRatesForTesting("SOLUSDT", rates)

	// Test 1: Before t0
	rBefore := fm.GetFundingRate("SOLUSDT", time.Date(2026, 9, 30, 23, 50, 0, 0, time.UTC))
	if rBefore != 0.0001 {
		t.Fatalf("expected 0.0001 before t0, got %f", rBefore)
	}

	// Test 2: Exactly at t8
	rAtT8 := fm.GetFundingRate("SOLUSDT", t8)
	if rAtT8 != -0.0002 {
		t.Fatalf("expected -0.0002 at t8, got %f", rAtT8)
	}

	// Test 3: Between t8 and t16
	rMid := fm.GetFundingRate("SOLUSDT", time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	if rMid != -0.0002 {
		t.Fatalf("expected -0.0002 between t8 and t16, got %f", rMid)
	}

	// Test 4: CheckSettlement during candle covering 08:00
	cOpen := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	cClose := time.Date(2026, 10, 1, 8, 4, 59, 0, time.UTC)
	rate, settled := fm.CheckSettlement("SOLUSDT", cOpen, cClose)
	if !settled || rate != -0.0002 {
		t.Fatalf("expected settlement at 08:00 candle with -0.0002, got settled=%v rate=%f", settled, rate)
	}

	// Test 5: CheckSettlement during non-settlement candle
	cOpen2 := time.Date(2026, 10, 1, 8, 5, 0, 0, time.UTC)
	cClose2 := time.Date(2026, 10, 1, 8, 9, 59, 0, time.UTC)
	_, settled2 := fm.CheckSettlement("SOLUSDT", cOpen2, cClose2)
	if settled2 {
		t.Fatalf("did not expect settlement at 08:05 candle")
	}
}

func TestFundingManager_FetchLiveAPIFundingRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "funding_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fm := NewFundingManager(tmpDir)
	ctx := context.Background()

	// Fetch last 2 days of SOLUSDT
	to := time.Now().UTC()
	from := to.AddDate(0, 0, -2)

	rates, err := fm.fetchLiveAPIFundingRange(ctx, "SOLUSDT", from, to)
	if err != nil {
		t.Fatalf("fetchLiveAPIFundingRange failed: %v", err)
	}

	if len(rates) == 0 {
		t.Fatal("expected at least 1 funding rate from Binance API")
	}

	for _, r := range rates {
		if r.FundingTime.IsZero() {
			t.Fatalf("invalid zero funding time in %+v", r)
		}
	}
}
