package strategy

import (
	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type SizingConfig struct {
	RiskPerTradePct                decimal.Decimal
	MaxSlotMarginFraction          decimal.Decimal
	AccountLeverage                float64
	SLMultiplier                   float64
	TPMultiplier                   float64
	TP1Multiplier                  float64
	MinSLDistancePct               decimal.Decimal
	MakerFeeRate                   decimal.Decimal
	TakerFeeRate                   decimal.Decimal
	SlippageRate                   decimal.Decimal
	AvailableBalanceBufferFraction decimal.Decimal
}

// BuildSafeDecimalOrderIntent menghitung ukuran lot presisi dengan proteksi friksi fee, slippage,
// clamp alokasi margin slot, dan kuantisasi fail-closed (Solusi P0-4, P1-9)
func BuildSafeDecimalOrderIntent(
	snap model.MarketSnapshot,
	effectiveEquity, availableBalance decimal.Decimal,
	side model.Side,
	rawEntry float64,
	atr14 float64,
	cfg SizingConfig,
) (model.OrderIntent, bool) {
	if effectiveEquity.LessThanOrEqual(decimal.Zero) || availableBalance.LessThanOrEqual(decimal.Zero) {
		return model.OrderIntent{}, false
	}

	entryPrice, err := model.QuantizePrice(decimal.NewFromFloat(rawEntry), snap.Filter.TickSize, side == model.SideShort)
	if err != nil {
		return model.OrderIntent{}, false
	}

	atrDec := decimal.NewFromFloat(atr14)
	slDist := atrDec.Mul(decimal.NewFromFloat(cfg.SLMultiplier))

	// Stop Distance Floor: Lindungi SL agar tidak diletakkan di dalam zona noise (< MinSLDistancePct)
	if cfg.MinSLDistancePct.GreaterThan(decimal.Zero) {
		minSLDist := entryPrice.Mul(cfg.MinSLDistancePct)
		if slDist.LessThan(minSLDist) {
			slDist = minSLDist
		}
	}

	tpDist := atrDec.Mul(decimal.NewFromFloat(cfg.TPMultiplier))
	// Jaga rasio Risk-to-Reward: pastikan tpDist tidak lebih kecil dari slDist * (TPMultiplier / SLMultiplier)
	if cfg.SLMultiplier > 0 && cfg.TPMultiplier > 0 {
		minTPDist := slDist.Mul(decimal.NewFromFloat(cfg.TPMultiplier / cfg.SLMultiplier))
		if tpDist.LessThan(minTPDist) {
			tpDist = minTPDist
		}
	}

	tp1Mult := cfg.TP1Multiplier
	if tp1Mult <= 0 {
		tp1Mult = 1.20
	}
	// TP1 adalah mekanisme de-risking cepat (Scale-Out 50% + Geser SL ke BE).
	// Jarak TP1 HARUS selalu mengacu murni pada volatilitas alami koin (ATR14)
	// dan TIDAK BOLEH digelembungkan oleh slDist floor agar selalu realistis disentuh pada swing awal.
	tp1Dist := atrDec.Mul(decimal.NewFromFloat(tp1Mult))

	var slPrice, tpPrice, tp1Price decimal.Decimal
	if side == model.SideLong {
		slPrice, err = model.QuantizePrice(entryPrice.Sub(slDist), snap.Filter.TickSize, false)
		if err != nil {
			return model.OrderIntent{}, false
		}
		tpPrice, err = model.QuantizePrice(entryPrice.Add(tpDist), snap.Filter.TickSize, true)
		if err != nil {
			return model.OrderIntent{}, false
		}
		tp1Price, err = model.QuantizePrice(entryPrice.Add(tp1Dist), snap.Filter.TickSize, true)
		if err != nil {
			return model.OrderIntent{}, false
		}
	} else {
		slPrice, err = model.QuantizePrice(entryPrice.Add(slDist), snap.Filter.TickSize, true)
		if err != nil {
			return model.OrderIntent{}, false
		}
		tpPrice, err = model.QuantizePrice(entryPrice.Sub(tpDist), snap.Filter.TickSize, false)
		if err != nil {
			return model.OrderIntent{}, false
		}
		tp1Price, err = model.QuantizePrice(entryPrice.Sub(tp1Dist), snap.Filter.TickSize, false)
		if err != nil {
			return model.OrderIntent{}, false
		}
	}

	// 1. Kerugian Efektif per Unit (termasuk friksi Maker Fee + Taker Fee + Slippage)
	totalFrictionRate := cfg.MakerFeeRate.Add(cfg.TakerFeeRate).Add(cfg.SlippageRate)
	frictionPerUnit := entryPrice.Mul(totalFrictionRate)
	actualSLDist := entryPrice.Sub(slPrice).Abs()
	effectiveLossPerUnit := actualSLDist.Add(frictionPerUnit)
	if effectiveLossPerUnit.LessThanOrEqual(decimal.Zero) {
		return model.OrderIntent{}, false
	}

	riskCapitalUSDT := effectiveEquity.Mul(cfg.RiskPerTradePct)
	qtyByRisk := riskCapitalUSDT.Div(effectiveLossPerUnit)

	// 2. Batasi (Clamp) berdasarkan Margin Slot & Available Balance:
	// MaxMarginSlot = min(Avail * Buffer, Equity * MaxSlotMarginFraction)
	maxMarginByEquity := effectiveEquity.Mul(cfg.MaxSlotMarginFraction)
	bufferFrac := cfg.AvailableBalanceBufferFraction
	if bufferFrac.LessThanOrEqual(decimal.Zero) {
		bufferFrac = decimal.NewFromFloat(0.90)
	}
	maxMarginByAvail := availableBalance.Mul(bufferFrac)
	allowedMargin := decimal.Min(maxMarginByEquity, maxMarginByAvail)
	maxNotional := allowedMargin.Mul(decimal.NewFromFloat(cfg.AccountLeverage))
	qtyByMargin := maxNotional.Div(entryPrice)

	rawQty := decimal.Min(qtyByRisk, qtyByMargin)
	finalQty, err := model.QuantizeQtyFloor(rawQty, snap.Filter.StepSize)
	if err != nil {
		return model.OrderIntent{}, false
	}

	// 3. Validasi Filter Bursa (MinQty & MinNotional)
	notional := finalQty.Mul(entryPrice)
	if finalQty.LessThan(snap.Filter.MinQty) || notional.LessThan(snap.Filter.MinNotional) {
		return model.OrderIntent{}, false
	}

	return model.OrderIntent{
		ClientOrderID: model.GenerateDeterministicClientOrderID(snap.Symbol, side, snap.Now),
		Symbol:        snap.Symbol,
		Side:          side,
		Qty:           finalQty,
		LimitPrice:    entryPrice,
		SLPrice:       slPrice,
		TPPrice:       tpPrice,
		TP1Price:      tp1Price,
		ATRAtEntry:    atrDec,
		CreatedAt:     snap.Now,
	}, true
}
