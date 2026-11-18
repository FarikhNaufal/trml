package data

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestBacktestHTTPClient_429RetryAnd418IPBanBreaker(t *testing.T) {
	var hits atomic.Int32
	banUntilMs := time.Now().UTC().Add(5 * time.Minute).UnixMilli()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if r.URL.Path == "/retry429" {
			if n == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`ok`))
			return
		}
		if r.URL.Path == "/ban418" {
			w.WriteHeader(418)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"code":-1003,"msg":"banned until %d"}`, banUntilMs)))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	// 1. Vision 404 returns without retry
	client := NewBacktestHTTPClient(BacktestHTTPConfig{AllowRESTFallback: false, MaxRetries: 2})
	_, status, err := client.GetVision(context.Background(), ts.URL+"/missing")
	if err != nil || status != 404 {
		t.Fatalf("expected clean 404 without error, got status=%d err=%v", status, err)
	}

	// 2. REST blocked when AllowRESTFallback=false
	if _, _, err := client.GetREST(context.Background(), ts.URL+"/retry429", 10, false); err == nil {
		t.Fatalf("expected REST blocked when AllowRESTFallback=false")
	}

	// 3. HTTP 429 retries and succeeds
	client.SetAllowRESTFallback(true)
	body, status, err := client.GetREST(context.Background(), ts.URL+"/retry429", 10, false)
	if err != nil || status != 200 || string(body) != "ok" {
		t.Fatalf("expected 429 retry success, got status=%d err=%v body=%q", status, err, string(body))
	}

	// 4. HTTP 418 trips breaker fail-closed; subsequent call does not hit server
	beforeHits := hits.Load()
	_, status418, err418 := client.GetREST(context.Background(), ts.URL+"/ban418", 10, false)
	if err418 == nil || status418 != 418 {
		t.Fatalf("expected 418 error, got status=%d err=%v", status418, err418)
	}
	if banned, _ := client.IsIPBanned(); !banned {
		t.Fatalf("expected client IP ban flag set")
	}
	_, _, errBlocked := client.GetREST(context.Background(), ts.URL+"/retry429", 10, false)
	if errBlocked == nil {
		t.Fatalf("expected fail-closed rejection after IP ban")
	}
	if hits.Load() != beforeHits+1 {
		t.Fatalf("expected zero network hits after 418 breaker tripped, hits went from %d to %d", beforeHits, hits.Load())
	}
}

func TestCache_AtomicWriteBinaryCRCAndLegacyImport(t *testing.T) {
	tmpDir := t.TempDir()
	legacyDir := filepath.Join(tmpDir, "cache")
	dirs := ResolveBacktestDirs(legacyDir)

	// 1. Legacy finished month CSV + ongoing month CSV
	solDir := filepath.Join(legacyDir, "SOLUSDT", "5m")
	if err := os.MkdirAll(solDir, 0755); err != nil {
		t.Fatal(err)
	}
	refNow := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	tOct := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	csvOct := fmt.Sprintf(
		"open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n"+
			"%d,150.0,152.0,149.0,151.0,100.0,%d,15050.0,10,60.0,9030.0,0\n"+
			"%d,150.0,148.0,149.0,151.0,100.0,%d,15050.0,10,60.0,9030.0,0\n",
		tOct.UnixMilli(), tOct.Add(5*time.Minute-time.Millisecond).UnixMilli(),
		tOct.Add(5*time.Minute).UnixMilli(), tOct.Add(10*time.Minute-time.Millisecond).UnixMilli(), // invalid high < close -> filtered out
	)
	if err := WriteFileAtomic(filepath.Join(solDir, "SOLUSDT-5m-2026-10.csv"), []byte(csvOct), 0644); err != nil {
		t.Fatal(err)
	}
	// Ongoing month (2026-11) should be skipped during permanent .bin import
	tNov := time.Date(2026, 11, 18, 12, 5, 0, 0, time.UTC) // unclosed relative to refNow!
	csvNov := fmt.Sprintf("%d,151,152,150,151.5,50,%d,7500,5,25,3750,0\n",
		tNov.UnixMilli(), tNov.Add(5*time.Minute).UnixMilli())
	if err := WriteFileAtomic(filepath.Join(solDir, "SOLUSDT-5m-2026-11.csv"), []byte(csvNov), 0644); err != nil {
		t.Fatal(err)
	}

	store, err := OpenBacktestStore(dirs.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	stats, err := ImportLegacyCSVCache(context.Background(), dirs, store, refNow)
	if err != nil {
		t.Fatalf("ImportLegacyCSVCache failed: %v", err)
	}
	if stats.KlineFiles != 1 || stats.KlineBars != 1 || stats.SkippedPartial != 1 {
		t.Fatalf("unexpected import stats: %+v", stats)
	}

	binPath := filepath.Join(dirs.StoreDir, "SOLUSDT", "5m", "2026-10.bin")
	bars, err := ReadCompactBarsBin(binPath, "SOLUSDT")
	if err != nil || len(bars) != 1 || math.Abs(bars[0].BarDeltaUSDT-(2*9030.0-15050.0)) > 1e-6 {
		t.Fatalf("unexpected binary read result: len=%d err=%v bars=%+v", len(bars), err, bars)
	}

	// Corrupt byte triggers CRC mismatch error
	rawBin, _ := os.ReadFile(binPath)
	rawBin[len(rawBin)-1] ^= 0xFF
	_ = os.WriteFile(binPath, rawBin, 0644)
	if _, err := ReadCompactBarsBin(binPath, "SOLUSDT"); err == nil {
		t.Fatalf("expected CRC mismatch error on corrupted .bin")
	}
}

func TestUniverse_S3PaginationBlocklistTradFiAndInferDelisted(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("continuation-token")
		w.Header().Set("Content-Type", "application/xml")
		if token == "" {
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>page2</NextContinuationToken>
  <CommonPrefixes><Prefix>data/futures/um/monthly/klines/AAPLUSDT/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>data/futures/um/monthly/klines/BTCUSDT/</Prefix></CommonPrefixes>
</ListBucketResult>`))
			return
		}
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes><Prefix>data/futures/um/monthly/klines/DELISTEDOLDUSDT/</Prefix></CommonPrefixes>
</ListBucketResult>`))
	}))
	defer ts.Close()

	client := NewBacktestHTTPClient(BacktestHTTPConfig{})
	s3Syms, err := DiscoverS3HistoricalSymbols(context.Background(), client, ts.URL)
	if err != nil {
		t.Fatalf("DiscoverS3HistoricalSymbols failed: %v", err)
	}
	if len(s3Syms) != 3 || !slices.Contains(s3Syms, "DELISTEDOLDUSDT") {
		t.Fatalf("expected 3 paginated symbols including DELISTEDOLDUSDT, got %v", s3Syms)
	}

	exchJSON := []byte(`{
		"symbols": [
			{"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001"}]},
			{"symbol":"AAPLUSDT","contractType":"TRADIFI_PERPETUAL","status":"TRADING","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.01","minQty":"0.01"}]}
		]
	}`)
	cat, err := ParseExchangeInfoCatalog(exchJSON)
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := t.TempDir()
	store, err := OpenBacktestStore(filepath.Join(tmpDir, "backtest.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	res := ReconcileHistoricalUniverse(context.Background(), s3Syms, cat, nil, store)
	if slices.Contains(res.Symbols, "AAPLUSDT") {
		t.Fatalf("expected AAPLUSDT blocklisted, got %v", res.Symbols)
	}
	if !slices.Contains(res.Symbols, "DELISTEDOLDUSDT") || res.DelistedCount != 1 || res.BlocklistedTradFi != 1 {
		t.Fatalf("expected DELISTEDOLDUSDT retained as delisted crypto, res=%+v", res)
	}

	inferred := InferFilterFromCandles([]HistoricalCandle{
		{Open: 1.2345, High: 1.2500, Low: 1.2100, Close: 1.2300, Volume: 123.45},
	})
	if inferred.TickSize.String() != "0.0001" || inferred.StepSize.String() != "0.01" {
		t.Fatalf("unexpected inferred filter precision: %+v", inferred)
	}
}

func TestMetricsOI_StrictZeroLookaheadLookup(t *testing.T) {
	tmpDir := t.TempDir()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("SOLUSDT-metrics-2026-10-01.csv")
	_, _ = w.Write([]byte("create_time,symbol,sum_open_interest,sum_open_interest_value\n" +
		"2026-10-01 09:55:00,SOLUSDT,1000.0,150000.0\n" +
		"2026-10-01 10:00:00,SOLUSDT,1050.0,157500.0\n" +
		"2026-10-01 10:05:00,SOLUSDT,2000.0,300000.0\n"))
	_ = zw.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if slices.Contains([]string{"/daily/metrics/SOLUSDT/SOLUSDT-metrics-2026-10-01.zip"}, r.URL.Path) {
			_, _ = rw.Write(buf.Bytes())
			return
		}
		http.NotFound(rw, r)
	}))
	defer srv.Close()

	oiMgr := NewMetricsOIDownloader(tmpDir, nil, nil)
	oiMgr.baseURL = srv.URL

	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 1, 23, 59, 59, 0, time.UTC)
	if err := oiMgr.LoadOpenInterestRange(context.Background(), []string{"SOLUSDT"}, from, to); err != nil {
		t.Fatal(err)
	}

	// Query at 10:02:00 UTC -> must resolve 10:00:00 (1050) and prev 09:55:00 (1000), NEVER 10:05:00 (2000)!
	simTime := time.Date(2026, 10, 1, 10, 2, 0, 0, time.UTC)
	prevOI, currOI, _, currTime, ok := oiMgr.LookupOpenInterestStrict("SOLUSDT", simTime, 4*time.Minute, 10*time.Minute, 6*time.Minute)
	if !ok || prevOI != 1000.0 || currOI != 1050.0 || !currTime.Equal(time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected OI lookup: ok=%v prev=%.1f curr=%.1f time=%v", ok, prevOI, currOI, currTime)
	}

	// Stale at 10:20:00 (> 6m maxStaleness after 10:05) -> must return ok=false
	_, _, _, _, staleOK := oiMgr.LookupOpenInterestStrict("SOLUSDT", time.Date(2026, 10, 1, 10, 20, 0, 0, time.UTC), 4*time.Minute, 10*time.Minute, 6*time.Minute)
	if staleOK {
		t.Fatalf("expected stale OI lookup rejected")
	}
}

func TestFunding_NoPreHistoryLookaheadAndHalfOpenBoundary(t *testing.T) {
	fm := NewFundingManager(t.TempDir())
	t8 := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	fm.SetRatesForTesting("ETHUSDT", []HistoricalFundingRate{
		{FundingTime: t8, FundingRate: -0.0099, IntervalHours: 8},
	})

	// Before first sample: must NOT peek at -0.0099, returns 0.0001 baseline
	if r := fm.GetFundingRate("ETHUSDT", t8.Add(-1*time.Hour)); r != 0.0001 {
		t.Fatalf("expected 0.0001 baseline before history start, got %v", r)
	}

	// Adjacent bar ending exactly at 08:00:00.000 UTC must NOT trigger settlement twice
	_, settledPrev := fm.CheckSettlement("ETHUSDT", t8.Add(-5*time.Minute), t8)
	rateCur, settledCur := fm.CheckSettlement("ETHUSDT", t8, t8.Add(5*time.Minute))
	if settledPrev {
		t.Fatalf("expected previous bar [07:55, 08:00) NOT to double-settle 08:00 event")
	}
	if !settledCur || rateCur != -0.0099 {
		t.Fatalf("expected [08:00, 08:05) to settle -0.0099, got settled=%v rate=%v", settledCur, rateCur)
	}
}

func TestValidateCandlesWithOI_CoverageAndWarning(t *testing.T) {
	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	candles := make([]HistoricalCandle, 10)
	pts := make([]OpenInterestPoint, 0, 5)
	for i := range candles {
		candles[i] = HistoricalCandle{
			Symbol:    "SOLUSDT",
			OpenTime:  t0.Add(time.Duration(i) * 5 * time.Minute),
			CloseTime: t0.Add(time.Duration(i+1) * 5 * time.Minute),
			Open:      100,
			High:      101,
			Low:       99,
			Close:     100.5,
			Volume:    100,
		}
		if i < 5 {
			pts = append(pts, OpenInterestPoint{
				Symbol:          "SOLUSDT",
				Timestamp:       candles[i].CloseTime,
				SumOpenInterest: 1000 + float64(i*10),
			})
		}
	}
	oiMgr := NewMetricsOIDownloader(t.TempDir(), nil, nil)
	oiMgr.SetSeriesForTesting("SOLUSDT", pts)

	rep := ValidateCandlesWithOI("SOLUSDT", candles, 5*time.Minute, oiMgr, 4*time.Minute, 7*time.Minute, 10*time.Minute)
	// Candles 1,2,3,4,5 (at 5*5m..6*5m still within 10m staleness of point 4) covered; 0 misses PrevOI; trailing miss staleness.
	if rep.OICoveragePct <= 0 || rep.OICoveragePct >= 90.0 {
		t.Fatalf("expected partial OI coverage between 0 and 90%%, got %.2f%%", rep.OICoveragePct)
	}
	if len(rep.Warnings) == 0 {
		t.Fatalf("expected low OI coverage warning")
	}
}

func TestBacktestStore_SaveAndListRuns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "backtest.db")
	store, err := OpenBacktestStore(dbPath)
	if err != nil {
		t.Fatalf("OpenBacktestStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	run := BacktestRunRecord{
		RunID:         "run-20261118-test1",
		CommitHash:    "abc1234",
		Mode:          "run",
		FromUTC:       "2026-10-01",
		ToUTC:         "2026-10-31",
		ExecTF:        "5m",
		OIMode:        "auto",
		UniverseMode:  "pit",
		TotalTrades:   12,
		WinRatePct:    58.33,
		NetPnLUSDT:    42.5,
		ProfitFactor:  1.45,
		MTMMaxDDPct:   4.2,
		ExpectancyUSD: 3.54,
		OICoveragePct: 95.0,
	}
	trades := []StoredRunTrade{
		{Symbol: "BTCUSDT", Side: "LONG", EntryTimeMs: 1000, ExitTimeMs: 2000, EntryPrice: 50000, ExitPrice: 50500, Qty: 0.1, NetPnLUSDT: 42.5, FeeUSDT: 1.0, ExitReason: "TAKE_PROFIT"},
	}
	curve := []StoredEquityPoint{
		{TimeMs: 1000, RealizedEquity: 1000, EffectiveEquity: 1005, UsedMargin: 50, OpenPositions: 1},
		{TimeMs: 2000, RealizedEquity: 1042.5, EffectiveEquity: 1042.5, UsedMargin: 0, OpenPositions: 0},
	}
	folds := []WalkForwardFoldRecord{
		{FoldIndex: 0, IsFromUTC: "2026-09-01", IsToUTC: "2026-10-15", OOSFromUTC: "2026-10-16", OOSToUTC: "2026-10-31", ISExpectancy: 4.0, OOSExpectancy: 3.5, DegradationPct: 12.5, Verdict: "PASS"},
	}

	if err := store.SaveBacktestRun(ctx, run, trades, curve, folds); err != nil {
		t.Fatalf("SaveBacktestRun failed: %v", err)
	}
	got, err := store.ListRecentRuns(ctx, 5)
	if err != nil {
		t.Fatalf("ListRecentRuns failed: %v", err)
	}
	if len(got) != 1 || got[0].RunID != run.RunID || got[0].MTMMaxDDPct != 4.2 {
		t.Fatalf("unexpected persisted runs: %+v", got)
	}
}


