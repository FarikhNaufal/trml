package data

import (
	"time"

	"trml/internal/model"
)

// HistoricalCandle merepresentasikan data candlestick 5m/1m lengkap dari arsip Binance Vision
// beserta volume taker untuk rekonstruksi orderflow delta secara matematis eksak.
type HistoricalCandle struct {
	Symbol              string    `json:"symbol"`
	OpenTime            time.Time `json:"open_time"`
	CloseTime           time.Time `json:"close_time"`
	Open                float64   `json:"open"`
	High                float64   `json:"high"`
	Low                 float64   `json:"low"`
	Close               float64   `json:"close"`
	Volume              float64   `json:"volume"`                 // Base asset volume
	QuoteVolume         float64   `json:"quote_volume"`           // Quote asset (USDT) volume
	TradeCount          int64     `json:"trade_count"`            // Jumlah transaksi pada bar
	TakerBuyBaseVolume  float64   `json:"taker_buy_base_volume"`  // Volume beli agresif taker (base)
	TakerBuyQuoteVolume float64   `json:"taker_buy_quote_volume"` // Volume beli agresif taker (USDT)
	BarDeltaUSDT        float64   `json:"bar_delta_usdt"`         // Net Orderflow Delta = 2*TakerBuyQuote - QuoteVolume
}

// ToModelCandle mengonversi HistoricalCandle ke model.Candle standar yang dipakai oleh evaluator produksi.
func (c *HistoricalCandle) ToModelCandle() model.Candle {
	return model.Candle{
		OpenTime:          c.OpenTime,
		CloseTime:         c.CloseTime,
		Open:              c.Open,
		High:              c.High,
		Low:               c.Low,
		Close:             c.Close,
		VolumeBase:        c.Volume,
		QuoteVolumeUSDT:   c.QuoteVolume,
		TakerBuyQuoteUSDT: c.TakerBuyQuoteVolume,
	}
}

// DataValidationResult menyimpan hasil audit kualitas data historis.
type DataValidationResult struct {
	Symbol       string    `json:"symbol"`
	TotalBars    int       `json:"total_bars"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	GapsCount    int       `json:"gaps_count"`
	ZeroVolCount int       `json:"zero_vol_count"`
	Duplicates   int       `json:"duplicates"`
	Warnings     []string  `json:"warnings"`
}
