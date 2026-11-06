package simbroker

import (
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type SimBrokerConfig struct {
	InitialEquityUSDT       decimal.Decimal
	MaxSlots                int
	MaxDailyDrawdownPct     float64
	AccountLeverage         float64
	MakerFeeRate            decimal.Decimal // default 0.0002 (2.0 bps)
	TakerFeeRate            decimal.Decimal // default 0.0005 (5.0 bps)
	SlippageRate            decimal.Decimal // default 0.0005 (5.0 bps pada market SL/exit)
	SignalDelayBars         int             // delay bar sebelum limit entry aktif (default 1)
	LimitTimeoutBars        int             // batas bar sebelum limit order expired (default 1)
	PenetrationTicks        int             // syarat tembus tick agar limit order terisi (default 1)
	IntrabarConflictSLFirst bool            // pesimistis default: jika SL dan TP tersentuh di bar yg sama, SL kena duluan
	PartialTPEnabled        bool            // default true
	PartialTP1Ratio         float64         // default 0.50
	PartialTP1ATRMult       float64         // default 1.00
	Tier1BEEnabled          bool            // default true
	MinBreakEvenOffsetPct   float64         // default 0.0010
	RunnerBECushionATR      float64         // default 0.0
	TrailingEnabled         bool            // default true
	TrailingActivateATR     float64         // default 1.10
	TrailingDistanceATR     float64         // default 0.55
	MaxHoldingDuration      time.Duration   // default 4 hours (48 bars 5m)
	EquityModeCompounding   bool            // true: compounding, false: fixed start equity
}

type SimTradeRecord struct {
	ID            string          `json:"id"`
	Symbol        string          `json:"symbol"`
	Side          model.Side      `json:"side"`
	EntryTime     time.Time       `json:"entry_time"`
	ExitTime      time.Time       `json:"exit_time"`
	EntryPrice    decimal.Decimal `json:"entry_price"`
	ExitPrice     decimal.Decimal `json:"exit_price"`
	Qty           decimal.Decimal `json:"qty"`
	InitialQty    decimal.Decimal `json:"initial_qty"`
	SLPrice       decimal.Decimal `json:"sl_price"`
	TPPrice       decimal.Decimal `json:"tp_price"`
	TP1Price      decimal.Decimal `json:"tp1_price"`
	GrossPnL      decimal.Decimal `json:"gross_pnl"`
	NetPnL        decimal.Decimal `json:"net_pnl"`
	FeeTotal      decimal.Decimal `json:"fee_total"`
	FundingTotal  decimal.Decimal `json:"funding_total"`
	SlippageTotal decimal.Decimal `json:"slippage_total"`
	ExitReason    string          `json:"exit_reason"`
	MAE           float64         `json:"mae"`
	MFE           float64         `json:"mfe"`
	HoldingBars   int             `json:"holding_bars"`
	SignalAuditID int64           `json:"signal_audit_id"`
	ATRAtEntry    decimal.Decimal `json:"atr_at_entry"`
}

type SimPendingOrder struct {
	Intent      model.OrderIntent
	PlacedBar   int64
	PlacedTime  time.Time
	TimeoutTime time.Time
}

type SimPosition struct {
	Symbol         string
	Side           model.Side
	Qty            decimal.Decimal
	InitialQty     decimal.Decimal
	EntryPrice     decimal.Decimal
	EntryFee       decimal.Decimal
	FundingPaid    decimal.Decimal
	CurrentSLPrice decimal.Decimal
	InitialSLPrice decimal.Decimal
	TPPrice        decimal.Decimal
	TP1Price       decimal.Decimal
	IsTP1Hit       bool
	ATRAtEntry     decimal.Decimal
	PeakPrice      decimal.Decimal
	OpenTime       time.Time
	OpenBarIndex   int64
	MaxMAE         float64
	MaxMFE         float64
	SignalAuditID  int64
	ProtectionTier int
	LastClosePrice decimal.Decimal
}
