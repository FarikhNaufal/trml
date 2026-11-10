package scanner

import (
	"time"

	"trml/internal/model"
)

// BootstrapSymbolState memuat hingga 1000 bar untuk konvergensi EMA200 dan menghitung VWAP Midnight-UTC (Solusi P1-3 & P1-4)
func BootstrapSymbolState(state *model.SymbolState, candles1000 []model.Candle, serverTime time.Time, bootstrapCap, deltaCap int, expectedStep ...time.Duration) error {
	state.Mu.Lock()
	defer state.Mu.Unlock()

	valid, err := model.ValidateAndFilterCandles(candles1000, serverTime, expectedStep...)
	if err != nil {
		return err
	}
	for _, c := range valid {
		state.ApplyClosedCandleLocked(c, bootstrapCap, deltaCap)
	}
	return nil
}
