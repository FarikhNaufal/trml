package model

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

type Side string

const (
	SideLong  Side = "LONG"
	SideShort Side = "SHORT"
)

func OppositeSide(s Side) Side {
	switch s {
	case SideLong:
		return SideShort
	case SideShort:
		return SideLong
	default:
		return ""
	}
}

type OrderLifecycleStatus string

const (
	StatusPendingNew      OrderLifecycleStatus = "PENDING_NEW"
	StatusNew             OrderLifecycleStatus = "NEW"
	StatusPartiallyFilled OrderLifecycleStatus = "PARTIALLY_FILLED"
	StatusFilled          OrderLifecycleStatus = "FILLED"
	StatusCanceled        OrderLifecycleStatus = "CANCELED"
	StatusRejected        OrderLifecycleStatus = "REJECTED"
	StatusExpired         OrderLifecycleStatus = "EXPIRED"
)

type ProtectionOrderStatus string

const (
	ProtectionPending ProtectionOrderStatus = "PENDING"
	ProtectionAcked   ProtectionOrderStatus = "ACKED"
	ProtectionFailed  ProtectionOrderStatus = "FAILED"
)

type OrderIntent struct {
	ClientOrderID string
	Symbol        string
	Side          Side
	Qty           decimal.Decimal
	LimitPrice    decimal.Decimal
	SLPrice       decimal.Decimal
	TPPrice       decimal.Decimal
	TP1Price      decimal.Decimal
	ATRAtEntry    decimal.Decimal
	CreatedAt     time.Time
	SignalAuditID int64
}

type LiveOrderRecord struct {
	ClientOrderID       string
	ExchangeOrderID     int64
	Symbol              string
	Side                Side
	IsEntry             bool
	IsAlgo              bool
	Status              OrderLifecycleStatus
	TargetQty           decimal.Decimal
	CumulativeFilledQty decimal.Decimal
	LimitPrice          decimal.Decimal
	AvgPrice            decimal.Decimal
	SLPrice             decimal.Decimal
	InitialSLPrice      decimal.Decimal
	TPPrice             decimal.Decimal
	TP1Price            decimal.Decimal
	ATRAtEntry          decimal.Decimal
	CreatedAt           time.Time
	SignalAuditID       int64
}

type LivePositionProtection struct {
	Symbol          string
	Side            Side
	PlanGeneration  uint64
	ProtectedQty    decimal.Decimal
	InitialQty      decimal.Decimal
	SLOrderID       int64
	SLClientAlgoID  string
	SLStatus        ProtectionOrderStatus
	SLPrice         decimal.Decimal
	InitialSLPrice  decimal.Decimal
	TPOrderID       int64
	TPClientAlgoID  string
	TPStatus        ProtectionOrderStatus
	TPPrice         decimal.Decimal
	TP1Price        decimal.Decimal
	IsTP1Hit        bool
	LastAttemptTime time.Time
	LastError       string
	SLRetryCount    int
	TPRetryCount    int
	LastTPAttempt   time.Time
	SignalAuditID   int64
}

type ProtectionPlanAction struct {
	Symbol         string
	Generation     uint64
	CancelSLID     int64
	CancelTPID     int64
	NeedNewSL      bool
	NeedNewTP      bool
	ExitSide       Side
	TargetQty      decimal.Decimal
	SLPrice        decimal.Decimal
	TPPrice        decimal.Decimal
	ClientSLAlgoID string
	ClientTPAlgoID string
}

type LivePosition struct {
	Symbol        string
	Side          Side
	Qty           decimal.Decimal
	EntryPrice    decimal.Decimal
	UnrealizedPnL decimal.Decimal
}

var clientOrderNonce uint64

func GenerateDeterministicClientOrderID(symbol string, side Side, candleCloseTime time.Time) string {
	n := atomic.AddUint64(&clientOrderNonce, 1) % 1000
	id := fmt.Sprintf("BOT_%s_%s_%d_%03d", symbol, side, candleCloseTime.UTC().Unix(), n)
	if len(id) > 36 {
		s := "L"
		if side == SideShort {
			s = "S"
		}
		sym := symbol
		// Batasi panjang simbol agar total ClientOrderID maksimal 36 karakter (syarat Binance).
		// Format: B_<sym>_<s:1>_<unix:10>_<nonce:3> -> Total selain sym adalah 19 karakter.
		// Maka batas aman panjang sym adalah 36 - 19 = 17 karakter.
		if len(sym) > 17 {
			sym = sym[:17]
		}
		id = fmt.Sprintf("B_%s_%s_%d_%03d", sym, s, candleCloseTime.UTC().Unix(), n)
	}
	return id
}
