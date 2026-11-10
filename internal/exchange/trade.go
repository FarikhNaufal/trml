package exchange

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type OrderResponse struct {
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Status        string `json:"status"`
	AvgPrice      string `json:"avgPrice"`
	ExecutedQty   string `json:"executedQty"`
	CumQuote      string `json:"cumQuote"`
}

type QueryOrderResponse struct {
	ClientOrderID string `json:"clientOrderId"`
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Status        string `json:"status"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	ReduceOnly    bool   `json:"reduceOnly"`
	Price         string `json:"price"`
	AvgPrice      string `json:"avgPrice"`
	StopPrice     string `json:"stopPrice"`
	CumQuote      string `json:"cumQuote"`
	Time          int64  `json:"time"`
}

type AlgoOrderResponse struct {
	AlgoID       int64  `json:"algoId"`
	ClientAlgoID string `json:"clientAlgoId"`
	Code         int    `json:"code"`
	Msg          string `json:"msg"`
}

type QueryAlgoOrderResponse struct {
	AlgoID       int64  `json:"algoId"`
	ClientAlgoID string `json:"clientAlgoId"`
	AlgoType     string `json:"algoType"`
	OrderType    string `json:"orderType"`
	Symbol       string `json:"symbol"`
	Side         string `json:"side"`
	Quantity     string `json:"quantity"`
	AlgoStatus   string `json:"algoStatus"`
	TriggerPrice string `json:"triggerPrice"`
	Price        string `json:"price"`
	ReduceOnly   bool   `json:"reduceOnly"`
}

type DualSidePositionResponse struct {
	DualSidePosition bool `json:"dualSidePosition"`
}

// PlaceLimitEntry mengirimkan order LIMIT GTX ke Binance Futures (POST /fapi/v1/order)
func (c *Client) PlaceLimitEntry(ctx context.Context, intent model.OrderIntent) (int64, error) {
	params := url.Values{}
	params.Set("symbol", intent.Symbol)
	if intent.Side == model.SideLong {
		params.Set("side", "BUY")
	} else {
		params.Set("side", "SELL")
	}
	params.Set("type", "LIMIT")
	params.Set("timeInForce", "GTX") // Post-Only
	params.Set("quantity", intent.Qty.String())
	params.Set("price", intent.LimitPrice.String())
	params.Set("newClientOrderId", intent.ClientOrderID)

	respBytes, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		return 0, fmt.Errorf("place entry: %w (body: %s)", err, string(respBytes))
	}

	var resp OrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return 0, fmt.Errorf("place entry: failed to decode response: %w", err)
	}

	return resp.OrderID, nil
}

// PlaceMarketEntry mengirimkan order MARKET ke Binance Futures (POST /fapi/v1/order)
func (c *Client) PlaceMarketEntry(ctx context.Context, intent model.OrderIntent) (int64, error) {
	params := url.Values{}
	params.Set("symbol", intent.Symbol)
	if intent.Side == model.SideLong {
		params.Set("side", "BUY")
	} else {
		params.Set("side", "SELL")
	}
	params.Set("type", "MARKET")
	params.Set("quantity", intent.Qty.String())
	params.Set("newClientOrderId", intent.ClientOrderID)

	respBytes, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		return 0, fmt.Errorf("place market entry: %w (body: %s)", err, string(respBytes))
	}

	var resp OrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return 0, fmt.Errorf("place market entry: failed to decode response: %w", err)
	}

	return resp.OrderID, nil
}

// PlaceAlgoOrder mengirimkan order proteksi STOP_MARKET atau TAKE_PROFIT_MARKET (POST /fapi/v1/algoOrder)
func (c *Client) PlaceAlgoOrder(
	ctx context.Context,
	symbol string,
	side model.Side,
	orderType string,
	qty, triggerPrice decimal.Decimal,
	clientAlgoID string,
) (int64, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	if side == model.SideLong {
		params.Set("side", "BUY")
	} else {
		params.Set("side", "SELL")
	}
	params.Set("algoType", "CONDITIONAL")
	params.Set("orderType", orderType) // STOP_MARKET / TAKE_PROFIT_MARKET
	params.Set("triggerPrice", triggerPrice.String())
	params.Set("quantity", qty.String())
	workingType := "MARK_PRICE"
	if orderType == "TAKE_PROFIT_MARKET" {
		workingType = "CONTRACT_PRICE"
	}
	params.Set("workingType", workingType)
	params.Set("reduceOnly", "true")
	params.Set("clientAlgoId", clientAlgoID)

	respBytes, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/algoOrder", params, true)
	if err != nil {
		return 0, fmt.Errorf("place algo order: %w (body: %s)", err, string(respBytes))
	}

	var resp AlgoOrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return 0, fmt.Errorf("place algo order: failed to decode response: %w", err)
	}

	if resp.AlgoID == 0 && resp.Code != 0 {
		return 0, fmt.Errorf("place algo order rejected by exchange: code=%d msg=%s", resp.Code, resp.Msg)
	}

	return resp.AlgoID, nil
}

// CancelOrder membatalkan pesanan reguler (DELETE /fapi/v1/order)
func (c *Client) CancelOrder(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	if exchangeOrderID != 0 {
		params.Set("orderId", strconv.FormatInt(exchangeOrderID, 10))
	} else if clientOrderID != "" {
		params.Set("origClientOrderId", clientOrderID)
	}

	_, err := c.DoRequest(ctx, http.MethodDelete, "/fapi/v1/order", params, true)
	if err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}
	return nil
}

// CancelAlgoOrder membatalkan pesanan algo conditional (DELETE /fapi/v1/algoOrder)
func (c *Client) CancelAlgoOrder(ctx context.Context, symbol string, algoID int64) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("algoId", strconv.FormatInt(algoID, 10))

	_, err := c.DoRequest(ctx, http.MethodDelete, "/fapi/v1/algoOrder", params, true)
	if err != nil {
		return fmt.Errorf("cancel algo order: %w", err)
	}
	return nil
}

// EmergencyMarketClose mengeksekusi penutupan posisi darurat dengan order MARKET reduceOnly.
// Menerima positionSide (SideLong untuk posisi beli, SideShort untuk posisi jual).
// Untuk menutup posisi LONG, bursa menerima order SELL.
// Untuk menutup posisi SHORT, bursa menerima order BUY.
func (c *Client) EmergencyMarketClose(ctx context.Context, symbol string, positionSide model.Side, qty decimal.Decimal) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	if positionSide == model.SideLong {
		params.Set("side", "SELL")
	} else {
		params.Set("side", "BUY")
	}
	params.Set("type", "MARKET")
	params.Set("reduceOnly", "true")
	params.Set("quantity", qty.String())
	params.Set("newClientOrderId", fmt.Sprintf("live_emg_%s_%d", symbol, time.Now().UnixNano()))

	_, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		return fmt.Errorf("emergency market close: %w", err)
	}
	return nil
}

// PlaceMarketClose mengirimkan order MARKET penutupan parsial/penuh dengan reduceOnly=true (POST /fapi/v1/order)
func (c *Client) PlaceMarketClose(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal, clientOrderID string) (int64, decimal.Decimal, decimal.Decimal, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	if side == model.SideLong {
		params.Set("side", "BUY")
	} else {
		params.Set("side", "SELL")
	}
	params.Set("type", "MARKET")
	params.Set("reduceOnly", "true")
	params.Set("quantity", qty.String())
	if clientOrderID != "" {
		params.Set("newClientOrderId", clientOrderID)
	}

	respBytes, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		return 0, decimal.Zero, decimal.Zero, fmt.Errorf("place market close: %w (body: %s)", err, string(respBytes))
	}

	var resp OrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return 0, decimal.Zero, decimal.Zero, fmt.Errorf("place market close: failed to decode response: %w", err)
	}

	avgPrice, _ := decimal.NewFromString(resp.AvgPrice)
	executedQty, _ := decimal.NewFromString(resp.ExecutedQty)
	return resp.OrderID, avgPrice, executedQty, nil
}

// PlaceLimitClose mengirimkan order LIMIT (Post-Only GTX atau fallback GTC reduceOnly) untuk penutupan parsial/TP (POST /fapi/v1/order)
func (c *Client) PlaceLimitClose(ctx context.Context, symbol string, side model.Side, qty, price decimal.Decimal, clientOrderID string) (int64, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	if side == model.SideLong {
		params.Set("side", "BUY")
	} else {
		params.Set("side", "SELL")
	}
	params.Set("type", "LIMIT")
	params.Set("timeInForce", "GTX") // Post-Only (Maker)
	params.Set("reduceOnly", "true")
	params.Set("quantity", qty.String())
	params.Set("price", price.String())
	if clientOrderID != "" {
		params.Set("newClientOrderId", clientOrderID)
	}

	respBytes, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		// Jika GTX ditolak karena harga sudah melintasi spread, fallback ke GTC reduceOnly
		errStr := err.Error() + " " + string(respBytes)
		if strings.Contains(errStr, "-2010") || strings.Contains(errStr, "-5022") || strings.Contains(errStr, "Post Only") {
			params.Set("timeInForce", "GTC")
			respBytes, err = c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
		}
		if err != nil {
			return 0, fmt.Errorf("place limit close: %w (body: %s)", err, string(respBytes))
		}
	}

	var resp OrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return 0, fmt.Errorf("place limit close: failed to decode response: %w", err)
	}

	return resp.OrderID, nil
}

// InitAndVerifySymbol memverifikasi One-Way mode, isolasi margin, dan leverage 5x (Solusi P0-5 & P0-7)
func (c *Client) InitAndVerifySymbol(ctx context.Context, symbol string, leverage int) (bool, error) {
	// 1. Verifikasi One-Way Mode (GET /fapi/v1/positionSide/dual)
	dualRespBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/positionSide/dual", nil, true)
	if err != nil {
		return false, fmt.Errorf("failed to query position mode: %w", err)
	}
	var dualResp DualSidePositionResponse
	if err := sonic.Unmarshal(dualRespBytes, &dualResp); err == nil && dualResp.DualSidePosition {
		// Jika Hedge Mode aktif, coba ubah ke One-Way Mode
		changeParams := url.Values{}
		changeParams.Set("dualSidePosition", "false")
		_, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/positionSide/dual", changeParams, true)
		if err != nil {
			return false, fmt.Errorf("strategy requires One-Way Mode; failed to disable dualSidePosition: %w", err)
		}
	}

	// 2. Set Margin Type ISOLATED (abaikan error jika sudah ISOLATED / code -4046)
	marginParams := url.Values{}
	marginParams.Set("symbol", symbol)
	marginParams.Set("marginType", "ISOLATED")
	_, err = c.DoRequest(ctx, http.MethodPost, "/fapi/v1/marginType", marginParams, true)
	if err != nil {
		var apiErr *APIError
		isAlreadyIsolated := errors.As(err, &apiErr) && apiErr.Code == -4046
		if !isAlreadyIsolated && !strings.Contains(err.Error(), "-4046") && !strings.Contains(err.Error(), "No need to change") {
			return false, fmt.Errorf("failed to set margin type to ISOLATED: %w", err)
		}
	}

	// 3. Set Leverage 5x
	levParams := url.Values{}
	levParams.Set("symbol", symbol)
	levParams.Set("leverage", strconv.Itoa(leverage))
	_, err = c.DoRequest(ctx, http.MethodPost, "/fapi/v1/leverage", levParams, true)
	if err != nil {
		return false, fmt.Errorf("failed to set leverage: %w", err)
	}

	return true, nil
}

// QueryOrder mengambil status order terkini dari bursa (GET /fapi/v1/order)
func (c *Client) QueryOrder(ctx context.Context, symbol, clientOrderID string) (*model.LiveOrderRecord, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("origClientOrderId", clientOrderID)

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/order", params, true)
	if err != nil {
		return nil, fmt.Errorf("query order %s: %w", clientOrderID, err)
	}

	var resp QueryOrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("query order decode: %w", err)
	}

	var side model.Side
	switch resp.Side {
	case "BUY":
		side = model.SideLong
	case "SELL":
		side = model.SideShort
	}

	var status model.OrderLifecycleStatus
	switch resp.Status {
	case "NEW":
		status = model.StatusNew
	case "PARTIALLY_FILLED":
		status = model.StatusPartiallyFilled
	case "FILLED":
		status = model.StatusFilled
	case "CANCELED":
		status = model.StatusCanceled
	case "REJECTED":
		status = model.StatusRejected
	case "EXPIRED":
		status = model.StatusExpired
	default:
		slog.Warn("exchange: unknown order status encountered, defaulting to EXPIRED", "rawStatus", resp.Status)
		status = model.StatusExpired
	}

	origQty, _ := decimal.NewFromString(resp.OrigQty)
	cumQty, _ := decimal.NewFromString(resp.ExecutedQty)
	lPrice, _ := decimal.NewFromString(resp.Price)
	avgP, _ := decimal.NewFromString(resp.AvgPrice)

	createdAt := time.Now().UTC()
	if resp.Time > 0 {
		createdAt = time.UnixMilli(resp.Time).UTC()
	}

	return &model.LiveOrderRecord{
		ClientOrderID:       resp.ClientOrderID,
		ExchangeOrderID:     resp.OrderID,
		Symbol:              resp.Symbol,
		Side:                side,
		IsEntry:             !resp.ReduceOnly,
		IsAlgo:              false,
		Status:              status,
		TargetQty:           origQty,
		CumulativeFilledQty: cumQty,
		LimitPrice:          lPrice,
		AvgPrice:            avgP,
		CreatedAt:           createdAt,
	}, nil
}

type PositionRiskResponse struct {
	Symbol           string          `json:"symbol"`
	PositionAmt      decimal.Decimal `json:"positionAmt"`
	EntryPrice       decimal.Decimal `json:"entryPrice"`
	MarkPrice        decimal.Decimal `json:"markPrice"`
	UnrealizedProfit decimal.Decimal `json:"unRealizedProfit"`
	LiquidationPrice decimal.Decimal `json:"liquidationPrice"`
	Leverage         string          `json:"leverage"`
	MarginType       string          `json:"marginType"`
	IsolatedMargin   string          `json:"isolatedMargin"`
	PositionSide     string          `json:"positionSide"`
	UpdateTime       int64           `json:"updateTime"`
}

// GetPositionRisk mengambil posisi risiko akun dari GET /fapi/v2/positionRisk (TD-02)
func (c *Client) GetPositionRisk(ctx context.Context, symbol string) ([]PositionRiskResponse, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", symbol)
	}

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v2/positionRisk", params, true)
	if err != nil {
		return nil, fmt.Errorf("get position risk: %w", err)
	}

	var resp []PositionRiskResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("get position risk decode: %w", err)
	}

	return resp, nil
}

// GetOpenAlgoOrders mengambil semua pesanan algo/conditional terbuka dari GET /fapi/v1/openAlgoOrders
func (c *Client) GetOpenAlgoOrders(ctx context.Context, symbol string) ([]QueryAlgoOrderResponse, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", symbol)
	}

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/openAlgoOrders", params, true)
	if err != nil {
		return nil, fmt.Errorf("get open algo orders: %w", err)
	}

	var resp []QueryAlgoOrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("get open algo orders decode: %w", err)
	}

	return resp, nil
}

// GetOpenOrders mengambil semua pesanan terbuka (termasuk algo/conditional) dari GET /fapi/v1/openOrders (TD-02)
func (c *Client) GetOpenOrders(ctx context.Context, symbol string) ([]QueryOrderResponse, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", symbol)
	}

	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v1/openOrders", params, true)
	if err != nil {
		return nil, fmt.Errorf("get open orders: %w", err)
	}

	var resp []QueryOrderResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("get open orders decode: %w", err)
	}

	return resp, nil
}

type BinanceAccountResponse struct {
	FeeTier               int    `json:"feeTier"`
	CanTrade              bool   `json:"canTrade"`
	TotalWalletBalance    string `json:"totalWalletBalance"`
	TotalUnrealizedProfit string `json:"totalUnrealizedProfit"`
	AvailableBalance      string `json:"availableBalance"`
	Assets                []struct {
		Asset            string `json:"asset"`
		WalletBalance    string `json:"walletBalance"`
		UnrealizedProfit string `json:"unrealizedProfit"`
		AvailableBalance string `json:"availableBalance"`
	} `json:"assets"`
	Positions []struct {
		Symbol           string `json:"symbol"`
		PositionAmt      string `json:"positionAmt"`
		EntryPrice       string `json:"entryPrice"`
		UnrealizedProfit string `json:"unrealizedProfit"`
		PositionSide     string `json:"positionSide"`
	} `json:"positions"`
}

// FetchAccountBalance mengambil informasi saldo dompet riil, margin tersedia, unrealized PnL,
// dan posisi terbuka langsung dari REST API Binance (GET /fapi/v2/account).
// Solusi P0 untuk mencegah "account data is stale" saat market sepi dan WebSocket idle.
func (c *Client) FetchAccountBalance(ctx context.Context) (walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, err error) {
	respBytes, err := c.DoRequest(ctx, http.MethodGet, "/fapi/v2/account", nil, true)
	if err != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, nil, fmt.Errorf("fetch account: %w", err)
	}

	var resp BinanceAccountResponse
	if err := sonic.Unmarshal(respBytes, &resp); err != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, nil, fmt.Errorf("fetch account decode: %w (body: %s)", err, string(respBytes))
	}

	// 1. Ambil saldo agregat akun
	if wb, err := decimal.NewFromString(resp.TotalWalletBalance); err == nil {
		walletBal = wb
	}
	if up, err := decimal.NewFromString(resp.TotalUnrealizedProfit); err == nil {
		unrealizedPnL = up
	}
	if ab, err := decimal.NewFromString(resp.AvailableBalance); err == nil {
		availBal = ab
	}

	// Fallback ke aset USDT jika saldo agregat belum terisi
	for _, a := range resp.Assets {
		if strings.EqualFold(a.Asset, "USDT") {
			if walletBal.IsZero() {
				if wb, err := decimal.NewFromString(a.WalletBalance); err == nil {
					walletBal = wb
				}
			}
			if unrealizedPnL.IsZero() {
				if up, err := decimal.NewFromString(a.UnrealizedProfit); err == nil {
					unrealizedPnL = up
				}
			}
			if availBal.IsZero() {
				if ab, err := decimal.NewFromString(a.AvailableBalance); err == nil {
					availBal = ab
				}
			}
			break
		}
	}

	// 2. Ekstrak posisi terbuka aktif (non-zero positionAmt)
	positions = make(map[string]model.LivePosition)
	for _, p := range resp.Positions {
		amt, err := decimal.NewFromString(p.PositionAmt)
		if err != nil || amt.IsZero() {
			continue
		}
		var side model.Side
		if amt.GreaterThan(decimal.Zero) {
			side = model.SideLong
		} else {
			side = model.SideShort
		}

		var ep, up decimal.Decimal
		if pEp, err := decimal.NewFromString(p.EntryPrice); err == nil {
			ep = pEp
		}
		if pUp, err := decimal.NewFromString(p.UnrealizedProfit); err == nil {
			up = pUp
		}

		positions[p.Symbol] = model.LivePosition{
			Symbol:        p.Symbol,
			Side:          side,
			Qty:           amt.Abs(),
			EntryPrice:    ep,
			UnrealizedPnL: up,
		}
	}

	return walletBal, unrealizedPnL, availBal, positions, nil
}
