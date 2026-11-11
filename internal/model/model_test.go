package model

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestQuantization(t *testing.T) {
	// Valid price rounding
	p := decimal.NewFromFloat(50.126)
	tick := decimal.NewFromFloat(0.01)

	down, err := QuantizePrice(p, tick, false)
	if err != nil || !down.Equal(decimal.NewFromFloat(50.12)) {
		t.Errorf("expected 50.12, got %s (err: %v)", down, err)
	}

	up, err := QuantizePrice(p, tick, true)
	if err != nil || !up.Equal(decimal.NewFromFloat(50.13)) {
		t.Errorf("expected 50.13, got %s (err: %v)", up, err)
	}

	// Fail closed on tick <= 0
	_, err = QuantizePrice(p, decimal.Zero, false)
	if err == nil {
		t.Errorf("expected error for tickSize <= 0")
	}

	// Fail closed on stepSize <= 0
	_, err = QuantizeQtyFloor(decimal.NewFromFloat(10), decimal.NewFromFloat(-1))
	if err == nil {
		t.Errorf("expected error for stepSize <= 0")
	}
}

func TestValidateAndFilterCandles(t *testing.T) {
	now := time.Now().UTC()
	c1 := Candle{
		OpenTime:        now.Add(-10 * time.Minute),
		CloseTime:       now.Add(-5 * time.Minute),
		Open:            100,
		High:            105,
		Low:             95,
		Close:           102,
		VolumeBase:      10,
		QuoteVolumeUSDT: 1000,
	}
	c2 := Candle{
		OpenTime:        now.Add(-5 * time.Minute),
		CloseTime:       now,
		Open:            102,
		High:            108,
		Low:             101,
		Close:           107,
		VolumeBase:      12,
		QuoteVolumeUSDT: 1200,
	}

	valid, err := ValidateAndFilterCandles([]Candle{c1, c2}, now)
	if err != nil || len(valid) != 2 {
		t.Fatalf("expected 2 valid candles, got %d (err: %v)", len(valid), err)
	}

	// Active bar discard test
	cActive := Candle{
		OpenTime:  now,
		CloseTime: now.Add(5 * time.Minute),
		Open:      107,
		High:      110,
		Low:       106,
		Close:     109,
	}
	valid2, err := ValidateAndFilterCandles([]Candle{c1, c2, cActive}, now)
	if err != nil || len(valid2) != 2 {
		t.Fatalf("expected active bar discarded, got %d", len(valid2))
	}

	// Invalid OHLC bounds: High < Open
	cBadOHLC := Candle{
		OpenTime:        now.Add(-5 * time.Minute),
		CloseTime:       now,
		Open:            105,
		High:            102, // High < Open!
		Low:             95,
		Close:           100,
		VolumeBase:      10,
		QuoteVolumeUSDT: 1000,
	}
	_, err = ValidateAndFilterCandles([]Candle{cBadOHLC}, now)
	if err == nil {
		t.Errorf("expected error for High < Open")
	}

	// Test NaN and Inf in OHLC and Volume
	cNaNPrice := c1
	cNaNPrice.Open = math.NaN()
	if _, err := ValidateAndFilterCandles([]Candle{cNaNPrice}, now); err == nil {
		t.Errorf("expected error for NaN Open price")
	}

	cInfPrice := c1
	cInfPrice.High = math.Inf(1)
	if _, err := ValidateAndFilterCandles([]Candle{cInfPrice}, now); err == nil {
		t.Errorf("expected error for Inf High price")
	}

	cNaNVol := c1
	cNaNVol.VolumeBase = math.NaN()
	if _, err := ValidateAndFilterCandles([]Candle{cNaNVol}, now); err == nil {
		t.Errorf("expected error for NaN VolumeBase")
	}
}

func TestExactTickQuantization(t *testing.T) {
	p := decimal.NewFromFloat(50.10)
	tick := decimal.NewFromFloat(0.10)

	down, err := QuantizePrice(p, tick, false)
	if err != nil || !down.Equal(p) {
		t.Errorf("expected 50.10, got %s", down)
	}

	up, err := QuantizePrice(p, tick, true)
	if err != nil || !up.Equal(p) {
		t.Errorf("expected exact tick not to jump on roundUp, got %s", up)
	}
}

func TestOppositeSideAndClientOrderID(t *testing.T) {
	if OppositeSide(SideLong) != SideShort {
		t.Errorf("expected SideShort")
	}
	if OppositeSide(SideShort) != SideLong {
		t.Errorf("expected SideLong")
	}

	now := time.Now().UTC()
	id1 := GenerateDeterministicClientOrderID("BTCUSDT", SideLong, now)
	id2 := GenerateDeterministicClientOrderID("BTCUSDT", SideLong, now)
	if id1 == id2 {
		t.Errorf("expected unique client order IDs on rapid generation")
	}
	if len(id1) > 36 {
		t.Errorf("client order ID exceeds Binance 36 chars limit: %s (len %d)", id1, len(id1))
	}

	longSym := "1000000BABYDOGEUSDT"
	idLong := GenerateDeterministicClientOrderID(longSym, SideShort, now)
	if len(idLong) > 36 {
		t.Errorf("long symbol client order ID exceeds Binance 36 chars limit: %s (len %d)", idLong, len(idLong))
	}
}

func TestRecordOpenInterest(t *testing.T) {
	st := NewSymbolState("BTCUSDT", ExchangeFilter{}, 10, 10)
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Sample 0 at t=0: warm-up dimulai (PrevOI belum diset agar tidak menghasilkan interval 0s semu)
	st.RecordOpenInterestLocked(1000.0, baseTime)
	if st.CurrOpenInterest != 1000.0 {
		t.Fatalf("expected curr OI 1000, got %f", st.CurrOpenInterest)
	}
	if st.PrevOpenInterest != 0 || !st.PrevOIUpdatedAt.IsZero() {
		t.Fatalf("expected zero prev OI on sample 0 warm-up, got %f", st.PrevOpenInterest)
	}

	// Minute 1: poll at t=1m (transisi warm-up, interval nyata 1m)
	st.RecordOpenInterestLocked(1010.0, baseTime.Add(1*time.Minute))
	if st.PrevOpenInterest != 1000.0 {
		t.Fatalf("expected prev OI 1000 after 1m, got %f", st.PrevOpenInterest)
	}
	if diff := st.CurrOIUpdatedAt.Sub(st.PrevOIUpdatedAt); diff != 1*time.Minute {
		t.Fatalf("expected 1m diff at t=1m, got %v", diff)
	}

	// Minute 2, 3: poll continuously
	st.RecordOpenInterestLocked(1020.0, baseTime.Add(2*time.Minute))
	st.RecordOpenInterestLocked(1030.0, baseTime.Add(3*time.Minute))
	if st.CurrOpenInterest != 1030.0 {
		t.Fatalf("expected curr OI 1030, got %f", st.CurrOpenInterest)
	}
	if diff := st.CurrOIUpdatedAt.Sub(st.PrevOIUpdatedAt); diff != 3*time.Minute {
		t.Fatalf("expected 3m diff at t=3m warm-up, got %v", diff)
	}

	// Minute 5: poll at t=5m
	st.RecordOpenInterestLocked(1050.0, baseTime.Add(5*time.Minute))
	if st.CurrOpenInterest != 1050.0 {
		t.Fatalf("expected curr OI 1050, got %f", st.CurrOpenInterest)
	}
	// Best sample for t=5m is sample at t=0 (value 1000)
	if st.PrevOpenInterest != 1000.0 {
		t.Fatalf("expected prev OI 1000 at t=5m, got %f", st.PrevOpenInterest)
	}
	if diff := st.CurrOIUpdatedAt.Sub(st.PrevOIUpdatedAt); diff != 5*time.Minute {
		t.Fatalf("expected 5m interval at t=5m, got %v", diff)
	}

	// Minute 6: poll at t=6m
	st.RecordOpenInterestLocked(1060.0, baseTime.Add(6*time.Minute))
	// Best sample for t=6m is sample at t=1 (value 1010)
	if st.PrevOpenInterest != 1010.0 {
		t.Fatalf("expected prev OI 1010 at t=6m, got %f", st.PrevOpenInterest)
	}
	if diff := st.CurrOIUpdatedAt.Sub(st.PrevOIUpdatedAt); diff != 5*time.Minute {
		t.Fatalf("expected 5m interval at t=6m, got %v", diff)
	}

	// Stale purge test: 3 jam kemudian koin di-scan lagi
	st.RecordOpenInterestLocked(2000.0, baseTime.Add(3*time.Hour))
	if st.CurrOpenInterest != 2000.0 {
		t.Fatalf("expected curr OI 2000, got %f", st.CurrOpenInterest)
	}
	// Sampel purba harus di-purge sehingga koin kembali ke fase warm-up bersih
	if st.PrevOpenInterest != 0 || !st.PrevOIUpdatedAt.IsZero() {
		t.Fatalf("expected zero prev OI after 3h stale purge, got %f", st.PrevOpenInterest)
	}
}

func TestSeedOpenInterestBootstrap(t *testing.T) {
	st := NewSymbolState("ETHUSDT", ExchangeFilter{}, 10, 10)
	baseTime := time.Date(2026, 1, 1, 12, 10, 0, 0, time.UTC)

	// Pengawal 1, 2, 4, 5: Masukkan sampel historis (T-5m dan T)
	histSamples := []OISample{
		{Value: 50000.0, Timestamp: baseTime.Add(-5 * time.Minute)}, // T-5m
		{Value: 50250.0, Timestamp: baseTime},                       // T
	}

	st.SeedOpenInterestBootstrapLocked(histSamples, baseTime)

	// Verifikasi bootstrap sukses seketika (0 detik latency)
	if st.CurrOpenInterest != 50250.0 {
		t.Fatalf("expected curr OI 50250.0, got %f", st.CurrOpenInterest)
	}
	if st.PrevOpenInterest != 50000.0 {
		t.Fatalf("expected prev OI 50000.0, got %f", st.PrevOpenInterest)
	}
	// Pengawal 4: Timestamp T-5m bursa dipertahankan
	if !st.PrevOIUpdatedAt.Equal(baseTime.Add(-5 * time.Minute)) {
		t.Fatalf("expected PrevOIUpdatedAt %v, got %v", baseTime.Add(-5*time.Minute), st.PrevOIUpdatedAt)
	}
	// Pengawal 5: IsOIBootstrapped aktif
	if !st.IsOIBootstrapped {
		t.Fatalf("expected IsOIBootstrapped true")
	}
	snap := st.Snapshot(baseTime)
	if !snap.IsOIBootstrapped {
		t.Fatalf("expected snapshot IsOIBootstrapped true")
	}

	// Pengawal 3: Jika buffer sudah terisi (len >= 2), seed berikutnya ditolak (tidak menimpa)
	newHist := []OISample{
		{Value: 99999.0, Timestamp: baseTime.Add(-5 * time.Minute)},
	}
	st.SeedOpenInterestBootstrapLocked(newHist, baseTime)
	if st.PrevOpenInterest == 99999.0 {
		t.Fatalf("guardrail 3 failed: bootstrap overwrote existing buffer")
	}

	// Setelah live polling berjalan beberapa menit dan menggantikan sampel historis
	st.RecordOpenInterestLocked(50300.0, baseTime.Add(1*time.Minute))
	st.RecordOpenInterestLocked(50400.0, baseTime.Add(5*time.Minute))
	// Di menit ke-5 dari polling riil, sampel tertua riil (t=1m) berumur 4m -> PrevOI dari riil
	st.RecordOpenInterestLocked(50500.0, baseTime.Add(6*time.Minute))
	// Sampel di t=1m adalah riil (Bootstrapped = false)
	if st.IsOIBootstrapped {
		t.Fatalf("expected IsOIBootstrapped false after live polling aged into place")
	}
}

func TestValidateAndFilterCandles_15m(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	step := 15 * time.Minute
	c1 := Candle{
		OpenTime:        now.Add(-2 * step),
		CloseTime:       now.Add(-step).Add(-time.Millisecond),
		Open:            100,
		High:            105,
		Low:             95,
		Close:           102,
		VolumeBase:      10,
		QuoteVolumeUSDT: 1000,
	}
	c2 := Candle{
		OpenTime:        now.Add(-step),
		CloseTime:       now.Add(-time.Millisecond),
		Open:            102,
		High:            108,
		Low:             101,
		Close:           107,
		VolumeBase:      15,
		QuoteVolumeUSDT: 1500,
	}

	valid, err := ValidateAndFilterCandles([]Candle{c1, c2}, now, step)
	if err != nil {
		t.Fatalf("unexpected error validating 15m candles: %v", err)
	}
	if len(valid) != 2 {
		t.Fatalf("expected 2 valid 15m candles, got %d", len(valid))
	}
}

func TestRecordOpenInterest_15m(t *testing.T) {
	baseTime := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	st := NewSymbolState("BTCUSDT", ExchangeFilter{}, 10, 10)
	st.TargetOIInterval = 15 * time.Minute

	st.RecordOpenInterestLocked(1000.0, baseTime)
	st.RecordOpenInterestLocked(1050.0, baseTime.Add(15*time.Minute))

	if st.CurrOpenInterest != 1050.0 {
		t.Fatalf("expected curr OI 1050, got %f", st.CurrOpenInterest)
	}
	if st.PrevOpenInterest != 1000.0 {
		t.Fatalf("expected prev OI 1000 at t=15m, got %f", st.PrevOpenInterest)
	}
	if diff := st.CurrOIUpdatedAt.Sub(st.PrevOIUpdatedAt); diff != 15*time.Minute {
		t.Fatalf("expected 15m interval, got %v", diff)
	}
}

func TestSeedOpenInterestBootstrap_DelayedBinanceBars(t *testing.T) {
	// Simulasi kondisi produksi pukul 14:23:28 UTC:
	// Binance openInterestHist (period=5m, limit=3) mengembalikan bar 14:10, 14:15, 14:20.
	now := time.Date(2026, 11, 11, 14, 23, 28, 0, time.UTC)
	st := NewSymbolState("ZECUSDT", ExchangeFilter{}, 10, 10)
	st.TargetOIInterval = 5 * time.Minute

	histSamples := []OISample{
		{Value: 488000.0, Timestamp: time.Date(2026, 11, 11, 14, 10, 0, 0, time.UTC)}, // umur 13m28s (> 10m, dibuang)
		{Value: 487600.0, Timestamp: time.Date(2026, 11, 11, 14, 15, 0, 0, time.UTC)}, // umur 8m28s (diterima)
		{Value: 487000.0, Timestamp: time.Date(2026, 11, 11, 14, 20, 0, 0, time.UTC)}, // umur 3m28s (diterima)
	}

	accepted := st.SeedOpenInterestBootstrapLocked(histSamples, now)
	if accepted != 2 {
		t.Fatalf("expected 2 accepted bootstrap samples, got %d", accepted)
	}
	if st.PrevOpenInterest != 487600.0 || st.CurrOpenInterest != 487000.0 {
		t.Fatalf("unexpected OI pair after bootstrap: prev=%.1f curr=%.1f", st.PrevOpenInterest, st.CurrOpenInterest)
	}

	// Diikuti live poll pada menit yang sama -> tidak boleh jatuh kembali ke OI_WARMING_UP
	st.RecordOpenInterestLocked(487250.0, now)
	if st.PrevOpenInterest <= 0 || st.PrevOIUpdatedAt.IsZero() {
		t.Fatalf("expected valid PrevOpenInterest after bootstrap+live poll, got %f", st.PrevOpenInterest)
	}
	if interval := st.CurrOIUpdatedAt.Sub(st.PrevOIUpdatedAt); interval < 1*time.Minute || interval > 10*time.Minute {
		t.Fatalf("expected OI interval within [1m, 10m], got %v", interval)
	}
	if st.NeedsOIBootstrapLocked(now, 10*time.Minute) {
		t.Fatalf("expected NeedsOIBootstrapLocked false when valid pair exists")
	}

	// Kasus lebih ketat: bar 14:20 belum terbit di Binance, hanya 14:10 dan 14:15 yang ada,
	// lalu live poll masuk di 14:23:28 -> tetap langsung membentuk pasangan 8m28s <= 10m.
	stLag := NewSymbolState("ZROUSDT", ExchangeFilter{}, 10, 10)
	stLag.TargetOIInterval = 5 * time.Minute
	acceptedLag := stLag.SeedOpenInterestBootstrapLocked(histSamples[:2], now)
	if acceptedLag != 1 {
		t.Fatalf("expected 1 accepted lagging bar (14:15), got %d", acceptedLag)
	}
	stLag.RecordOpenInterestLocked(10250.0, now)
	if stLag.PrevOpenInterest != 487600.0 {
		t.Fatalf("expected lagging bar 487600.0 as PrevOI after live poll, got %f", stLag.PrevOpenInterest)
	}
}

func TestRecordOpenInterest_RecoverAfterStall(t *testing.T) {
	t0 := time.Date(2026, 11, 11, 13, 49, 0, 0, time.UTC)
	st := NewSymbolState("AINUSDT", ExchangeFilter{}, 10, 10)
	st.TargetOIInterval = 5 * time.Minute

	st.RecordOpenInterestLocked(1000.0, t0)
	// Simulasi jeda jaringan 10 menit -> pulih di 13:59 (13:49 berumur 10m, tepat di batas maxInterval)
	tRecover1 := time.Date(2026, 11, 11, 13, 59, 0, 0, time.UTC)
	st.RecordOpenInterestLocked(1020.0, tRecover1)
	if st.PrevOpenInterest != 1000.0 {
		t.Fatalf("expected 13:49 sample to remain usable at 10m edge, got %f", st.PrevOpenInterest)
	}

	// 1 menit kemudian di 14:00: 13:49 sekarang berumur 11m (> 10m), tetapi 13:59 berumur 1m ->
	// PrevOpenInterest HARUS beralih ke 13:59 (1020.0), bukan direset ke 0 oleh elemen [0] basi.
	tRecover2 := time.Date(2026, 11, 11, 14, 0, 0, 0, time.UTC)
	st.RecordOpenInterestLocked(1030.0, tRecover2)
	if st.PrevOpenInterest != 1020.0 {
		t.Fatalf("expected transition recovery to use 13:59 (1020.0), got %f", st.PrevOpenInterest)
	}
	if !st.PrevOIUpdatedAt.Equal(tRecover1) {
		t.Fatalf("expected PrevOIUpdatedAt %v, got %v", tRecover1, st.PrevOIUpdatedAt)
	}
}

func TestSeedOpenInterestBootstrap_SingleSampleDoesNotSelfPair(t *testing.T) {
	now := time.Date(2026, 11, 11, 14, 23, 0, 0, time.UTC)
	st := NewSymbolState("LITUSDT", ExchangeFilter{}, 10, 10)
	st.TargetOIInterval = 5 * time.Minute

	hist := []OISample{
		{Value: 25000.0, Timestamp: now.Add(-5 * time.Minute)},
	}
	accepted := st.SeedOpenInterestBootstrapLocked(hist, now)
	if accepted != 1 {
		t.Fatalf("expected 1 accepted sample, got %d", accepted)
	}
	if st.PrevOpenInterest != 0 || !st.PrevOIUpdatedAt.IsZero() {
		t.Fatalf("expected PrevOpenInterest 0 when only 1 sample exists (avoid INTERVAL_0s), got %f at %v", st.PrevOpenInterest, st.PrevOIUpdatedAt)
	}
	if !st.NeedsOIBootstrapLocked(now, 10*time.Minute) {
		t.Fatalf("expected NeedsOIBootstrapLocked true when only 1 sample exists")
	}
}
