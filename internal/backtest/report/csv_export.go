package report

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"trml/internal/backtest/data"
	"trml/internal/backtest/simbroker"
)

// WriteTradesCSV atomically writes simulated trade execution legs to CSV.
func WriteTradesCSV(path string, trades []simbroker.SimTradeRecord) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{
		"ID", "Symbol", "Side", "EntryTime", "ExitTime", "EntryPrice", "ExitPrice",
		"Qty", "InitialQty", "SLPrice", "TPPrice", "TP1Price", "GrossPnL", "NetPnL",
		"FeeTotal", "FundingTotal", "ExitReason", "MAE", "MFE", "HoldingBars", "ATRAtEntry",
	}); err != nil {
		return err
	}
	for _, tr := range trades {
		if err := w.Write([]string{
			tr.ID,
			tr.Symbol,
			string(tr.Side),
			tr.EntryTime.UTC().Format("2006-01-02 15:04:05"),
			tr.ExitTime.UTC().Format("2006-01-02 15:04:05"),
			tr.EntryPrice.String(),
			tr.ExitPrice.String(),
			tr.Qty.String(),
			tr.InitialQty.String(),
			tr.SLPrice.String(),
			tr.TPPrice.String(),
			tr.TP1Price.String(),
			tr.GrossPnL.String(),
			tr.NetPnL.String(),
			tr.FeeTotal.String(),
			tr.FundingTotal.String(),
			tr.ExitReason,
			fmt.Sprintf("%.4f", tr.MAE),
			fmt.Sprintf("%.4f", tr.MFE),
			strconv.Itoa(tr.HoldingBars),
			tr.ATRAtEntry.String(),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return data.WriteFileAtomic(path, buf.Bytes(), 0644)
}

// WriteEquityCurveCSV atomically writes timeline mark-to-market equity samples to CSV.
func WriteEquityCurveCSV(path string, curve []simbroker.EquityPoint) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{"TimeUTC", "RealizedEquity", "EffectiveEquity", "UsedMargin", "OpenPositions"}); err != nil {
		return err
	}
	for _, pt := range curve {
		if err := w.Write([]string{
			pt.Time.UTC().Format("2006-01-02 15:04:05"),
			fmt.Sprintf("%.4f", pt.RealizedEquity),
			fmt.Sprintf("%.4f", pt.EffectiveEquity),
			fmt.Sprintf("%.4f", pt.UsedMargin),
			strconv.Itoa(pt.OpenPositions),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return data.WriteFileAtomic(path, buf.Bytes(), 0644)
}
