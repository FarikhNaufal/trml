package data

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadExchangeFilters(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "filters_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ctx := context.Background()
	filters, err := LoadExchangeFilters(ctx, tmpDir)
	if err != nil {
		t.Skipf("LoadExchangeFilters network unavailable or rate limited (status 418): %v", err)
	}

	if len(filters) == 0 {
		t.Fatal("expected at least 1 filter loaded from Binance Futures")
	}

	btcFilter, ok := filters["BTCUSDT"]
	if !ok {
		t.Fatal("BTCUSDT filter not found")
	}
	if btcFilter.TickSize.IsZero() || btcFilter.StepSize.IsZero() {
		t.Fatalf("invalid BTC filter: %+v", btcFilter)
	}

	// Verify file was cached
	cacheFile := filepath.Join(tmpDir, "exchange_info.json")
	if _, statErr := os.Stat(cacheFile); statErr != nil {
		t.Fatalf("cache file not created: %v", statErr)
	}

	// Second run should read from cache
	cachedFilters, err := LoadExchangeFilters(ctx, tmpDir)
	if err != nil {
		t.Fatalf("second LoadExchangeFilters failed: %v", err)
	}
	if len(cachedFilters) != len(filters) {
		t.Fatalf("expected %d filters, got %d", len(filters), len(cachedFilters))
	}
}
