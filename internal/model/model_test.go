package model

import (
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

