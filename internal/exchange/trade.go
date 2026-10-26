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
}

type QueryOrderResponse struct {
	ClientOrderID string `json:"clientOrderId"`
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Status        string `json:"status"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	ReduceOnly    bool   `json:"reduceOnly"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	Time          int64  `json:"time"`
}

type AlgoOrderResponse struct {
	AlgoID       int64  `json:"algoId"`
	ClientAlgoID string `json:"clientAlgoId"`
	Code         int    `json:"code"`
	Msg          string `json:"msg"`
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

// EmergencyMarketClose mengeksekusi penutupan posisi darurat dengan order MARKET reduceOnly
func (c *Client) EmergencyMarketClose(ctx context.Context, symbol string, side model.Side, qty decimal.Decimal) error {
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

	_, err := c.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		return fmt.Errorf("emergency market close: %w", err)
	}
	return nil
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
	if resp.Side == "BUY" {
		side = model.SideLong
	} else if resp.Side == "SELL" {
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
		CreatedAt:           createdAt,
	}, nil
}

