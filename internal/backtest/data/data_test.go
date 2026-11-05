package data

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseBinanceKlineCSV(t *testing.T) {
	csvData := `open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1704067200000,42000.0,42500.0,41900.0,42400.0,10.0,1704067499999,420000.0,500,6.0,252000.0,0
1704067500000,42400.0,42600.0,42300.0,42500.0,8.0,1704067799999,340000.0,400,3.0,127500.0,0
`
	candles, err := ParseBinanceKlineCSV(strings.NewReader(csvData), "BTCUSDT")
	if err != nil {
		t.Fatalf("unexpected error parsing CSV: %v", err)
	}

	if len(candles) != 2 {
		t.Fatalf("expected 2 candles, got %d", len(candles))
	}

	c1 := candles[0]
	if c1.Open != 42000.0 || c1.High != 42500.0 || c1.Low != 41900.0 || c1.Close != 42400.0 {
		t.Errorf("candle 1 OHLC mismatch: %+v", c1)
	}

	// Bar 1 Delta: 2 * 252,000 - 420,000 = +84,000 USDT
	expectedDelta1 := 2.0*252000.0 - 420000.0
	if c1.BarDeltaUSDT != expectedDelta1 {
		t.Errorf("expected delta %f, got %f", expectedDelta1, c1.BarDeltaUSDT)
	}

	// Bar 2 Delta: 2 * 127,500 - 340,000 = -85,000 USDT
	c2 := candles[1]
	expectedDelta2 := 2.0*127500.0 - 340000.0
	if c2.BarDeltaUSDT != expectedDelta2 {
		t.Errorf("expected delta %f, got %f", expectedDelta2, c2.BarDeltaUSDT)
	}
}

func TestValidateCandles(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	step := 5 * time.Minute

	candles := []HistoricalCandle{
		{
			OpenTime:  t0,
			CloseTime: t0.Add(step - time.Millisecond),
			Open:      100,
			High:      105,
			Low:       95,
			Close:     102,
			Volume:    1000,
		},
		{
			// Gap of 10 minutes instead of 5 minutes
			OpenTime:  t0.Add(2 * step),
			CloseTime: t0.Add(3*step - time.Millisecond),
			Open:      102,
			High:      106,
			Low:       101,
			Close:     104,
			Volume:    0, // zero volume
		},
	}

	report := ValidateCandles("BTCUSDT", candles, step)
	if report.TotalBars != 2 {
		t.Errorf("expected 2 bars, got %d", report.TotalBars)
	}
	if report.GapsCount != 1 {
		t.Errorf("expected 1 gap, got %d", report.GapsCount)
	}
	if report.ZeroVolCount != 1 {
		t.Errorf("expected 1 zero volume, got %d", report.ZeroVolCount)
	}
}

func TestDownloader_FetchMonthlyMock(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "backtest_cache_*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	csvContent := `1704067200000,100,105,95,102,50,1704067499999,5000,10,30,3000,0`

	// Buat zip mock
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("BTCUSDT-5m-2024-01.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte(csvContent))
	_ = zw.Close()

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "BTCUSDT-5m-2024-01.zip") {
			rw.Header().Set("Content-Type", "application/zip")
			_, _ = rw.Write(buf.Bytes())
			return
		}
		http.NotFound(rw, req)
	}))
	defer server.Close()

	dl := NewDownloader(tmpDir)
	dl.baseURL = server.URL

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	candles, err := dl.FetchCandleRange(context.Background(), "BTCUSDT", "5m", from, to)
	if err != nil {
		t.Fatalf("fetch candle range failed: %v", err)
	}

	if len(candles) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(candles))
	}

	if candles[0].Close != 102 {
		t.Errorf("expected close 102, got %f", candles[0].Close)
	}
}
