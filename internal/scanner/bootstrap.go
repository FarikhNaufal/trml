package scanner

import (
	"time"

	"trml/internal/model"
)

// BootstrapSymbolState memuat hingga BootstrapCount bar untuk konvergensi EMA200 dan menghitung VWAP Midnight-UTC (Solusi P1-3 & P1-4)
func BootstrapSymbolState(state *model.SymbolState, candles1000 []model.Candle, serverTime time.Time, bootstrapCap, deltaCap int, expectedStep ...time.Duration) error {
	targetStep := 5 * time.Minute
	if len(expectedStep) > 0 && expectedStep[0] > 0 {
		targetStep = expectedStep[0]
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()

	valid, err := model.ValidateAndFilterCandles(candles1000, serverTime, expectedStep...)
	if err != nil {
		return err
	}
	if len(valid) == 0 {
		return nil
	}

	needRebuild := len(state.ClosedCandles) == 0 ||
		state.HasRecentGapLocked(targetStep) ||
		(valid[0].OpenTime.After(state.LastProcessedCandleOpenTime) && valid[0].OpenTime.Sub(state.LastProcessedCandleOpenTime) > targetStep) ||
		(len(valid) >= len(state.ClosedCandles) && !valid[0].OpenTime.After(state.LastProcessedCandleOpenTime))

	if needRebuild {
		state.ResetAndApplyCandlesLocked(valid, bootstrapCap, deltaCap)
		return nil
	}
	for _, c := range valid {
		state.ApplyClosedCandleLocked(c, bootstrapCap, deltaCap)
	}
	return nil
}
