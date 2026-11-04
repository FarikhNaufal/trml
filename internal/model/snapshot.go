package model

import (
	"math"
	"sync"
	"time"
)

type SymbolState struct {
	Mu                         sync.Mutex
	Symbol                     string
	Filter                     ExchangeFilter
	IsLeverageIsolatedVerified bool

	BestBid       float64
	BestAsk       float64
	BookUpdatedAt time.Time

	MarkPrice          float64
	MarkPriceUpdatedAt time.Time

	FundingRate      float64
	FundingUpdatedAt time.Time

	PrevOpenInterest float64
	CurrOpenInterest float64
	PrevOIUpdatedAt  time.Time
	CurrOIUpdatedAt  time.Time
	IsOIBootstrapped bool
	OISamples        []OISample

	SessionEpochDayUTC      int64
	CumulativeTypicalPVBase float64
	CumulativeVolumeBase    float64
	CurrentSessionVWAP      float64

	LastProcessedCandleOpenTime time.Time
	ClosedCandles               []Candle
	ClosedBarDeltasUSDT         []float64
	LastExitTime                time.Time
	LastExitReason              string
}

type OISample struct {
	Value        float64
	Timestamp    time.Time
	Bootstrapped bool
}

func NewSymbolState(symbol string, filter ExchangeFilter, bootstrapCap, deltaCap int) *SymbolState {
	return &SymbolState{
		Symbol:              symbol,
		Filter:              filter,
		ClosedCandles:       make([]Candle, 0, bootstrapCap),
		ClosedBarDeltasUSDT: make([]float64, 0, deltaCap),
		OISamples:           make([]OISample, 0, 30),
	}
}

// RecordOpenInterestLocked mencatat sampel Open Interest dan menyinkronkan selisih 5 menit
func (s *SymbolState) RecordOpenInterestLocked(oi float64, now time.Time) {
	if oi <= 0 {
		return
	}

	// 1. Bersihkan sampel basi (> 15 menit) agar simbol yang sempat keluar scanner tidak menyimpan sampel purba
	cutoff := now.Add(-15 * time.Minute)
	validCount := 0
	for _, sample := range s.OISamples {
		if sample.Timestamp.After(cutoff) {
			s.OISamples[validCount] = sample
			validCount++
		}
	}
	s.OISamples = s.OISamples[:validCount]

	s.OISamples = append(s.OISamples, OISample{Value: oi, Timestamp: now, Bootstrapped: false})
	if len(s.OISamples) > 30 {
		s.OISamples = s.OISamples[len(s.OISamples)-30:]
	}

	s.CurrOpenInterest = oi
	s.CurrOIUpdatedAt = now

	// 2. Jika sampel baru 1 (warm-up startup/koin baru), biarkan PrevOI kosong
	// agar evaluator menolak dengan MISSING_OI_DATA bukan melempar INTERVAL_0s
	if len(s.OISamples) < 2 {
		s.PrevOpenInterest = 0
		s.PrevOIUpdatedAt = time.Time{}
		s.IsOIBootstrapped = false
		return
	}

	// 3. Cari sampel terdekat dengan target 5 menit lalu (antara 3m50s dan 6m50s)
	targetTime := now.Add(-5 * time.Minute)
	var bestSample *OISample
	bestDiff := 100 * time.Hour

	for i := range s.OISamples {
		sample := &s.OISamples[i]
		age := now.Sub(sample.Timestamp)
		if age < 3*time.Minute+50*time.Second || age > 6*time.Minute+50*time.Second {
			continue
		}
		diff := sample.Timestamp.Sub(targetTime)
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			bestSample = sample
		}
	}

	if bestSample != nil {
		s.PrevOpenInterest = bestSample.Value
		s.PrevOIUpdatedAt = bestSample.Timestamp
		s.IsOIBootstrapped = bestSample.Bootstrapped
	} else {
		// 4. Fase transisi (< 4m): gunakan sampel tertua jika umurnya sudah minimal 1 menit dan maksimal 6.5 menit (Pengawal 2)
		oldest := s.OISamples[0]
		age := now.Sub(oldest.Timestamp)
		if age >= 1*time.Minute && age <= 6*time.Minute+30*time.Second {
			s.PrevOpenInterest = oldest.Value
			s.PrevOIUpdatedAt = oldest.Timestamp
			s.IsOIBootstrapped = oldest.Bootstrapped
		} else {
			// Sampel tertua masih < 1 menit atau sudah melebihi 6.5 menit (degrade ke warming up)
			s.PrevOpenInterest = 0
			s.PrevOIUpdatedAt = time.Time{}
			s.IsOIBootstrapped = false
		}
	}
}

// SeedOpenInterestBootstrapLocked menginjeksi sampel historis Open Interest jika buffer belum siap (len < 2)
// Mengimplementasikan 5 Pengawal Ketat:
// 1. Nilai menggunakan unit kontrak (sumOpenInterest).
// 2. Filter data: hanya menerima bar yang berumur <= 6.5m dan bukan masa depan (Pengawal 2).
// 3. Seed hanya jika buffer len(OISamples) < 2 (tidak menimpa data polling riil).
// 4. Timestamp asli dari bursa dipertahankan.
// 5. Tandai sampel sebagai Bootstrapped untuk audit trail.
func (s *SymbolState) SeedOpenInterestBootstrapLocked(samples []OISample, now time.Time) {
	if len(s.OISamples) >= 2 {
		return // Pengawal 3: buffer sudah terisi data polling riil
	}

	validSamples := make([]OISample, 0, len(samples))
	for _, sample := range samples {
		if sample.Value <= 0 {
			continue
		}
		age := now.Sub(sample.Timestamp)
		// Pengawal 2: Hanya ambil sampel valid (tidak purba > 6.5m dan tidak di masa depan > 1m)
		if age < -1*time.Minute || age > 6*time.Minute+30*time.Second {
			continue
		}
		sample.Bootstrapped = true // Pengawal 5: Tandai sebagai Bootstrapped
		validSamples = append(validSamples, sample)
	}

	if len(validSamples) == 0 {
		return
	}

	// Jika ada sampel polling berjalan, pertahankan di belakang
	merged := make([]OISample, 0, len(validSamples)+len(s.OISamples))
	merged = append(merged, validSamples...)
	merged = append(merged, s.OISamples...)
	if len(merged) > 30 {
		merged = merged[len(merged)-30:]
	}
	s.OISamples = merged

	// Set CurrOpenInterest ke sampel terbaru
	latest := s.OISamples[len(s.OISamples)-1]
	s.CurrOpenInterest = latest.Value
	s.CurrOIUpdatedAt = latest.Timestamp

	// Resolusi PrevOpenInterest (Pengawal 4: cari sampel ~5m lalu)
	targetTime := now.Add(-5 * time.Minute)
	var bestSample *OISample
	bestDiff := 100 * time.Hour

	for i := range s.OISamples {
		sample := &s.OISamples[i]
		age := now.Sub(sample.Timestamp)
		if age < 3*time.Minute+50*time.Second || age > 6*time.Minute+50*time.Second {
			continue
		}
		diff := sample.Timestamp.Sub(targetTime)
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			bestSample = sample
		}
	}

	if bestSample != nil {
		s.PrevOpenInterest = bestSample.Value
		s.PrevOIUpdatedAt = bestSample.Timestamp
		s.IsOIBootstrapped = bestSample.Bootstrapped
	} else if len(s.OISamples) >= 2 {
		oldest := s.OISamples[0]
		age := now.Sub(oldest.Timestamp)
		if age >= 1*time.Minute && age <= 6*time.Minute+30*time.Second {
			s.PrevOpenInterest = oldest.Value
			s.PrevOIUpdatedAt = oldest.Timestamp
			s.IsOIBootstrapped = oldest.Bootstrapped
		}
	}
}

func (s *SymbolState) ApplyClosedCandleLocked(candle Candle, bootstrapCap int, deltaCap int) bool {
	if !s.LastProcessedCandleOpenTime.IsZero() && !candle.OpenTime.After(s.LastProcessedCandleOpenTime) {
		return false
	}
	s.LastProcessedCandleOpenTime = candle.OpenTime

	epochDay := candle.OpenTime.UTC().Unix() / 86400
	if epochDay != s.SessionEpochDayUTC {
		s.SessionEpochDayUTC = epochDay
		s.CumulativeTypicalPVBase = 0.0
		s.CumulativeVolumeBase = 0.0
		s.CurrentSessionVWAP = 0.0
	}

	typicalPrice := (candle.High + candle.Low + candle.Close) / 3.0
	if !math.IsNaN(typicalPrice) && !math.IsInf(typicalPrice, 0) &&
		!math.IsNaN(candle.VolumeBase) && !math.IsInf(candle.VolumeBase, 0) && candle.VolumeBase >= 0 {
		s.CumulativeTypicalPVBase += typicalPrice * candle.VolumeBase
		s.CumulativeVolumeBase += candle.VolumeBase
		if s.CumulativeVolumeBase > 0 {
			vwap := s.CumulativeTypicalPVBase / s.CumulativeVolumeBase
			if !math.IsNaN(vwap) && !math.IsInf(vwap, 0) && vwap > 0 {
				s.CurrentSessionVWAP = vwap
			}
		}
	}

	barDeltaUSDT := (2.0 * candle.TakerBuyQuoteUSDT) - candle.QuoteVolumeUSDT
	if math.IsNaN(barDeltaUSDT) || math.IsInf(barDeltaUSDT, 0) {
		barDeltaUSDT = 0.0
	}

	s.ClosedCandles = AppendCandleWithCap(s.ClosedCandles, candle, bootstrapCap)
	s.ClosedBarDeltasUSDT = AppendFloatWithCap(s.ClosedBarDeltasUSDT, barDeltaUSDT, deltaCap)
	return true
}

type MarketSnapshot struct {
	Symbol              string
	Filter              ExchangeFilter
	Now                 time.Time
	BestBid             float64
	BestAsk             float64
	BookUpdatedAt       time.Time
	MarkPrice           float64
	MarkPriceUpdatedAt  time.Time
	FundingRate         float64
	FundingUpdatedAt    time.Time
	PrevOpenInterest    float64
	CurrOpenInterest    float64
	PrevOIUpdatedAt     time.Time
	CurrOIUpdatedAt     time.Time
	IsOIBootstrapped    bool
	CurrentSessionVWAP  float64
	ClosedCandles       []Candle
	ClosedBarDeltasUSDT []float64
	LastExitTime        time.Time
	LastExitReason      string
}

type BookQuote struct {
	BestBid float64
	BestAsk float64
}

// Snapshot mengembalikan salinan data snapshot immutable yang aman dari data race
func (s *SymbolState) Snapshot(now time.Time) MarketSnapshot {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	candlesCopy := make([]Candle, len(s.ClosedCandles))
	copy(candlesCopy, s.ClosedCandles)

	deltasCopy := make([]float64, len(s.ClosedBarDeltasUSDT))
	copy(deltasCopy, s.ClosedBarDeltasUSDT)

	return MarketSnapshot{
		Symbol:              s.Symbol,
		Filter:              s.Filter,
		Now:                 now,
		BestBid:             s.BestBid,
		BestAsk:             s.BestAsk,
		BookUpdatedAt:       s.BookUpdatedAt,
		MarkPrice:           s.MarkPrice,
		MarkPriceUpdatedAt:  s.MarkPriceUpdatedAt,
		FundingRate:         s.FundingRate,
		FundingUpdatedAt:    s.FundingUpdatedAt,
		PrevOpenInterest:    s.PrevOpenInterest,
		CurrOpenInterest:    s.CurrOpenInterest,
		PrevOIUpdatedAt:     s.PrevOIUpdatedAt,
		CurrOIUpdatedAt:     s.CurrOIUpdatedAt,
		IsOIBootstrapped:    s.IsOIBootstrapped,
		CurrentSessionVWAP:  s.CurrentSessionVWAP,
		ClosedCandles:       candlesCopy,
		ClosedBarDeltasUSDT: deltasCopy,
		LastExitTime:        s.LastExitTime,
		LastExitReason:      s.LastExitReason,
	}
}

func (s *SymbolState) SetLastExit(t time.Time, reason string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.LastExitTime = t
	s.LastExitReason = reason
}

func (s *SymbolState) SetLastExitTime(t time.Time) {
	s.SetLastExit(t, "")
}
