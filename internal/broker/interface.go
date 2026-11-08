package broker

import (
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type ActivePositionDTO struct {
	Symbol           string  `json:"symbol"`
	Side             string  `json:"side"`
	Qty              string  `json:"qty"`
	EntryPrice       float64 `json:"entry_price"`
	MarkPrice        float64 `json:"mark_price"`
	SLPrice          float64 `json:"sl_price"`
	TPPrice          float64 `json:"tp_price"`
	UnrealizedPnL    float64 `json:"unrealized_pnl"`
	UnrealizedROEPct float64 `json:"unrealized_roe_pct"`
	OpenTimeUTC      string  `json:"open_time_utc"`
	DurationSec      int64   `json:"duration_sec"`
	IsBreakEven      bool    `json:"is_break_even"`
	ProtectionTier   int     `json:"protection_tier"`
}

type ExecutionBroker interface {
	SubmitLimitEntry(intent model.OrderIntent) (bool, error)
	OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time)
	GetEffectiveTotalEquityUSDT() decimal.Decimal
	GetAvailableBalanceUSDT() decimal.Decimal
	GetDailyTotalDrawdownPct() float64
	IsCircuitBreakerTripped() bool
	GetActiveSlotCount() int
	HasActiveOrderOrPosition(symbol string) bool
	GetMonitoredSymbols() []string
	CheckTimeouts(now time.Time)
	GetActivePositions() []ActivePositionDTO
}
