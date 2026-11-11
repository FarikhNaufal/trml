package model

import (
	"math"
	"sort"
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
	TargetOIInterval time.Duration
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

func (s *SymbolState) oiIntervalConfig() (interval, maxInterval, minAge, maxAge time.Duration) {
	interval = s.TargetOIInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	maxInterval = interval * 2
	if maxInterval < 8*time.Minute {
		maxInterval = 8 * time.Minute
	}
	minAge = time.Duration(float64(interval) * 0.75)
	maxAge = time.Duration(float64(interval) * 1.35)
	return interval, maxInterval, minAge, maxAge
}

// NeedsOIBootstrapLocked menentukan apakah state OI masih membutuhkan seeding data historis
// (buffer belum punya 2 sampel usable, PrevOI kosong, atau PrevOI sudah melewati batas maxInterval).
func (s *SymbolState) NeedsOIBootstrapLocked(now time.Time, maxInterval time.Duration) bool {
	if maxInterval <= 0 {
		_, maxInterval, _, _ = s.oiIntervalConfig()
	}
	if len(s.OISamples) < 2 || s.PrevOpenInterest <= 0 || s.PrevOIUpdatedAt.IsZero() {
		return true
	}
	return now.Sub(s.PrevOIUpdatedAt) > maxInterval
}

func (s *SymbolState) pruneAndNormalizeOISamplesLocked(now time.Time, maxInterval time.Duration) {
	cutoff := now.Add(-maxInterval)
	byTS := make(map[int64]OISample, len(s.OISamples))
	for _, sample := range s.OISamples {
		if sample.Value <= 0 || sample.Timestamp.Before(cutoff) {
			continue
		}
		key := sample.Timestamp.UnixMilli()
		if existing, exists := byTS[key]; !exists || (existing.Bootstrapped && !sample.Bootstrapped) {
			byTS[key] = sample
		}
	}

	cleaned := make([]OISample, 0, len(byTS))
	for _, sample := range byTS {
		cleaned = append(cleaned, sample)
	}
	sort.Slice(cleaned, func(i, j int) bool {
		return cleaned[i].Timestamp.Before(cleaned[j].Timestamp)
	})
	if len(cleaned) > 30 {
		cleaned = cleaned[len(cleaned)-30:]
	}
	s.OISamples = cleaned
}

func isBetterOICandidate(best *OISample, bestDiff, bestAge time.Duration, cand *OISample, candDiff, candAge, minAge time.Duration) bool {
	if best == nil {
		return true
	}
	if candDiff != bestDiff {
		return candDiff < bestDiff
	}
	candMature := candAge >= minAge
	bestMature := bestAge >= minAge
	if candMature != bestMature {
		return candMature
	}
	return best.Bootstrapped && !cand.Bootstrapped
}

func (s *SymbolState) resolvePrevOpenInterestLocked() {
	if len(s.OISamples) < 2 {
		s.PrevOpenInterest = 0
		s.PrevOIUpdatedAt = time.Time{}
		s.IsOIBootstrapped = false
		return
	}

	interval, maxInterval, minAge, maxAge := s.oiIntervalConfig()
	latest := s.OISamples[len(s.OISamples)-1]
	refTime := s.CurrOIUpdatedAt
	if refTime.IsZero() {
		refTime = latest.Timestamp
	}
	targetTime := refTime.Add(-interval)

	// Tier 1: Cari sampel terdekat dengan target interval dalam jendela utama [0.75*interval, 1.35*interval]
	var tier1Best *OISample
	var tier1Diff, tier1Age time.Duration

	// Tier 2 (Transisi / Pemulihan): Cari sampel valid dalam [1m, maxInterval], mengutamakan yang terdekat ke interval
	var tier2Best *OISample
	var tier2Diff, tier2Age time.Duration

	for i := 0; i < len(s.OISamples)-1; i++ {
		sample := &s.OISamples[i]
		if !sample.Timestamp.Before(refTime) {
			continue
		}
		age := refTime.Sub(sample.Timestamp)
		if age > maxInterval {
			continue
		}
		diff := sample.Timestamp.Sub(targetTime)
		if diff < 0 {
			diff = -diff
		}
		if age >= minAge && age <= maxAge {
			if isBetterOICandidate(tier1Best, tier1Diff, tier1Age, sample, diff, age, minAge) {
				tier1Best, tier1Diff, tier1Age = sample, diff, age
			}
			continue
		}
		if age >= 1*time.Minute && isBetterOICandidate(tier2Best, tier2Diff, tier2Age, sample, diff, age, minAge) {
			tier2Best, tier2Diff, tier2Age = sample, diff, age
		}
	}

	chosen := tier1Best
	if chosen == nil {
		chosen = tier2Best
	}
	if chosen != nil {
		s.PrevOpenInterest = chosen.Value
		s.PrevOIUpdatedAt = chosen.Timestamp
		s.IsOIBootstrapped = chosen.Bootstrapped
		return
	}

	s.PrevOpenInterest = 0
	s.PrevOIUpdatedAt = time.Time{}
	s.IsOIBootstrapped = false
}

// RecordOpenInterestLocked mencatat sampel Open Interest dan menyinkronkan selisih interval candle
func (s *SymbolState) RecordOpenInterestLocked(oi float64, now time.Time) {
	if oi <= 0 {
		return
	}

	_, maxInterval, _, _ := s.oiIntervalConfig()
	s.OISamples = append(s.OISamples, OISample{Value: oi, Timestamp: now, Bootstrapped: false})
	s.pruneAndNormalizeOISamplesLocked(now, maxInterval)

	if len(s.OISamples) > 0 {
		latest := s.OISamples[len(s.OISamples)-1]
		s.CurrOpenInterest = latest.Value
		s.CurrOIUpdatedAt = latest.Timestamp
	} else {
		s.CurrOpenInterest = oi
		s.CurrOIUpdatedAt = now
	}

	// Jika sampel baru 1 (warm-up startup/koin baru), biarkan PrevOI kosong
	// agar evaluator menolak dengan MISSING_OI_DATA bukan melempar INTERVAL_0s
	s.resolvePrevOpenInterestLocked()
}

// SeedOpenInterestBootstrapLocked menginjeksi sampel historis Open Interest jika buffer belum siap.
// Mengembalikan jumlah sampel historis yang diterima setelah filter usia & validasi.
func (s *SymbolState) SeedOpenInterestBootstrapLocked(samples []OISample, now time.Time) int {
	_, maxInterval, _, _ := s.oiIntervalConfig()
	if !s.NeedsOIBootstrapLocked(now, maxInterval) {
		return 0 // Pengawal 3: buffer sudah terisi pasangan valid
	}

	validSamples := make([]OISample, 0, len(samples))
	for _, sample := range samples {
		if sample.Value <= 0 {
			continue
		}
		age := now.Sub(sample.Timestamp)
		// Terima sampel dalam batas MaxOIInterval (dan toleransi clock skew -1m) agar bar 5m Binance tetap terserap
		if age < -1*time.Minute || age > maxInterval {
			continue
		}
		sample.Bootstrapped = true // Pengawal 5: Tandai sebagai Bootstrapped
		validSamples = append(validSamples, sample)
	}

	if len(validSamples) == 0 {
		return 0
	}

	s.OISamples = append(s.OISamples, validSamples...)
	s.pruneAndNormalizeOISamplesLocked(now, maxInterval)

	if len(s.OISamples) > 0 {
		latest := s.OISamples[len(s.OISamples)-1]
		s.CurrOpenInterest = latest.Value
		s.CurrOIUpdatedAt = latest.Timestamp
	}

	s.resolvePrevOpenInterestLocked()
	return len(validSamples)
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
