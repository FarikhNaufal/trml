package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestClientSignatureAndAuth(t *testing.T) {
	var receivedKey, receivedSig, receivedTimestamp string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedKey = r.Header.Get("X-MBX-APIKEY")
		_ = r.ParseForm()
		receivedSig = r.Form.Get("signature")
		receivedTimestamp = r.Form.Get("timestamp")

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId": 1234567, "status": "NEW"}`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "TEST_API_KEY", "TEST_SECRET_KEY")
	params := url.Values{}
	params.Set("symbol", "BTCUSDT")

	resp, err := client.DoRequest(context.Background(), http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil {
		t.Fatalf("unexpected request error: %v (resp: %s)", err, string(resp))
	}

	if receivedKey != "TEST_API_KEY" {
		t.Errorf("expected X-MBX-APIKEY 'TEST_API_KEY', got: %s", receivedKey)
	}
	if receivedSig == "" {
		t.Errorf("expected non-empty HMAC signature")
	}
	if receivedTimestamp == "" {
		t.Errorf("expected non-empty timestamp")
	}
}

func TestMarketDataEndpoints(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			_, _ = w.Write([]byte(`{
				"symbols": [{
					"symbol": "BTCUSDT",
					"contractType": "PERPETUAL",
					"status": "TRADING",
					"filters": [
						{"filterType": "PRICE_FILTER", "tickSize": "0.10"},
						{"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
						{"filterType": "MIN_NOTIONAL", "notional": "5.0"}
					]
				}]
			}`))
		case "/fapi/v1/ticker/24hr":
			_, _ = w.Write([]byte(`[{"symbol": "BTCUSDT", "quoteVolume": "125000000.50"}]`))
		case "/fapi/v1/ticker/bookTicker":
			_, _ = w.Write([]byte(`[{"symbol": "BTCUSDT", "bidPrice": "65000.10", "askPrice": "65000.20"}]`))
		case "/fapi/v1/klines":
			_, _ = w.Write([]byte(`[
				[1700000000000, "65000.0", "65100.0", "64900.0", "65050.0", "100.0", 1700000299999, "6500000.0", 500, "60.0", "3900000.0", "0"]
			]`))
		case "/fapi/v1/openInterest":
			_, _ = w.Write([]byte(`{"symbol": "BTCUSDT", "openInterest": "25000.5", "time": 1700000000000}`))
		case "/futures/data/openInterestHist":
			_, _ = w.Write([]byte(`[
				{"symbol": "BTCUSDT", "sumOpenInterest": "24950.0", "timestamp": 1699999700000},
				{"symbol": "BTCUSDT", "sumOpenInterest": "25000.5", "timestamp": 1700000000000}
			]`))
		case "/fapi/v1/listenKey":
			if r.Method == http.MethodPost {
				_, _ = w.Write([]byte(`{"listenKey": "test_listen_key_12345"}`))
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case "/fapi/v1/order":
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(`{
					"clientOrderId": "ENTRY_BTC_123",
					"orderId": 888999,
					"symbol": "BTCUSDT",
					"side": "BUY",
					"status": "FILLED",
					"origQty": "0.5",
					"executedQty": "0.5",
					"reduceOnly": false,
					"time": 1700000000000
				}`))
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case "/fapi/v2/positionRisk":
			_, _ = w.Write([]byte(`[
				{
					"symbol": "BTCUSDT",
					"positionAmt": "-0.050",
					"entryPrice": "81000.0",
					"markPrice": "80850.0",
					"unRealizedProfit": "7.500",
					"liquidationPrice": "95000.0",
					"leverage": "8",
					"marginType": "isolated",
					"positionSide": "BOTH"
				}
			]`))
		case "/fapi/v1/openOrders":
			_, _ = w.Write([]byte(`[
				{
					"clientOrderId": "ALGO_SL_BTCUSDT_1",
					"orderId": 991122,
					"symbol": "BTCUSDT",
					"side": "BUY",
					"status": "NEW",
					"origQty": "0.050",
					"price": "0",
					"stopPrice": "81500.0",
					"reduceOnly": true
				}
			]`))
		case "/fapi/v1/openAlgoOrders":
			_, _ = w.Write([]byte(`[
				{
					"algoId": 881122,
					"clientAlgoId": "ALGO_SL_BTCUSDT_2",
					"algoType": "CONDITIONAL",
					"orderType": "STOP_MARKET",
					"symbol": "BTCUSDT",
					"side": "SELL",
					"quantity": "0.100",
					"algoStatus": "NEW",
					"triggerPrice": "81000.0",
					"reduceOnly": true
				}
			]`))
		case "/fapi/v2/account":
			_, _ = w.Write([]byte(`{
				"feeTier": 0,
				"canTrade": true,
				"totalWalletBalance": "1500.50",
				"totalUnrealizedProfit": "25.30",
				"availableBalance": "1200.00",
				"assets": [
					{
						"asset": "USDT",
						"walletBalance": "1500.50",
						"unrealizedProfit": "25.30",
						"availableBalance": "1200.00"
					}
				],
				"positions": [
					{
						"symbol": "BTCUSDT",
						"positionAmt": "0.100",
						"entryPrice": "65000.00",
						"unrealizedProfit": "25.30",
						"positionSide": "BOTH"
					}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	ctx := context.Background()

	// 1. ExchangeInfo
	filters, err := client.FetchExchangeInfo(ctx)
	if err != nil || len(filters) == 0 {
		t.Fatalf("fetch exchange info failed: %v", err)
	}
	btcFilter, ok := filters["BTCUSDT"]
	if !ok || btcFilter.TickSize.String() != "0.1" {
		t.Errorf("expected BTCUSDT tickSize 0.1, got %v", btcFilter)
	}

	// 2. 24h Tickers
	volMap, err := client.Fetch24hTickers(ctx)
	if err != nil || volMap["BTCUSDT"] <= 100000000.0 {
		t.Fatalf("fetch 24h tickers failed: %v, vol: %f", err, volMap["BTCUSDT"])
	}

	// 3. Book Tickers
	bookMap, err := client.FetchBookTickers(ctx)
	if err != nil || bookMap["BTCUSDT"].BestBid != 65000.10 {
		t.Fatalf("fetch book tickers failed: %v, book: %+v", err, bookMap["BTCUSDT"])
	}

	// 4. Klines
	klines, err := client.FetchKlines(ctx, "BTCUSDT", "5m", 1)
	if err != nil || len(klines) != 1 || klines[0].Close != 65050.0 {
		t.Fatalf("fetch klines failed: %v", err)
	}

	// 5. Open Interest
	oi, err := client.FetchOpenInterest(ctx, "BTCUSDT")
	if err != nil || oi != 25000.5 {
		t.Fatalf("fetch open interest failed: %v, oi: %f", err, oi)
	}

	// 5b. Open Interest History (Bootstrap)
	hist, err := client.FetchOpenInterestHist(ctx, "BTCUSDT", 2)
	if err != nil || len(hist) != 2 || hist[0].SumOpenInterest != "24950.0" {
		t.Fatalf("fetch open interest hist failed: %v, hist: %+v", err, hist)
	}

	// 6. ListenKey
	lKey, err := client.CreateListenKey(ctx)
	if err != nil || lKey != "test_listen_key_12345" {
		t.Fatalf("create listenKey failed: %v", err)
	}
	err = client.KeepAliveListenKey(ctx)
	if err != nil {
		t.Fatalf("keepalive listenKey failed: %v", err)
	}

	// 7. QueryOrder
	ordRec, err := client.QueryOrder(ctx, "BTCUSDT", "ENTRY_BTC_123")
	if err != nil {
		t.Fatalf("query order failed: %v", err)
	}
	if ordRec.ExchangeOrderID != 888999 || ordRec.Status != model.StatusFilled || !ordRec.IsEntry {
		t.Errorf("unexpected query order result: %+v", ordRec)
	}

	// 8. GetPositionRisk (TD-02)
	posRisk, err := client.GetPositionRisk(ctx, "BTCUSDT")
	if err != nil || len(posRisk) != 1 || posRisk[0].Symbol != "BTCUSDT" {
		t.Fatalf("get position risk failed: %v, pos: %+v", err, posRisk)
	}

	// 9. GetOpenOrders (TD-02)
	openOrds, err := client.GetOpenOrders(ctx, "BTCUSDT")
	if err != nil || len(openOrds) != 1 || openOrds[0].OrderID != 991122 || openOrds[0].ClientOrderID != "ALGO_SL_BTCUSDT_1" {
		t.Fatalf("get open orders failed: %v, orders: %+v", err, openOrds)
	}

	// 9b. GetOpenAlgoOrders
	openAlgoOrds, err := client.GetOpenAlgoOrders(ctx, "BTCUSDT")
	if err != nil || len(openAlgoOrds) != 1 || openAlgoOrds[0].AlgoID != 881122 || openAlgoOrds[0].TriggerPrice != "81000.0" {
		t.Fatalf("get open algo orders failed: %v, orders: %+v", err, openAlgoOrds)
	}

	// 10. FetchAccountBalance (P0-2 Anti-Staleness)
	wb, up, ab, pos, err := client.FetchAccountBalance(ctx)
	if err != nil {
		t.Fatalf("fetch account balance failed: %v", err)
	}
	if !wb.Equal(decimal.RequireFromString("1500.50")) {
		t.Errorf("expected wallet balance 1500.50, got %s", wb)
	}
	if !up.Equal(decimal.RequireFromString("25.30")) {
		t.Errorf("expected unrealized PnL 25.30, got %s", up)
	}
	if !ab.Equal(decimal.RequireFromString("1200.00")) {
		t.Errorf("expected available balance 1200.00, got %s", ab)
	}
	if p, ok := pos["BTCUSDT"]; !ok || p.Side != model.SideLong || !p.Qty.Equal(decimal.RequireFromString("0.100")) {
		t.Errorf("unexpected position map: %+v", pos)
	}
}

func TestCircuitBreakerHTTP418(t *testing.T) {
	reqCount := 0
	futureBanMs := time.Now().Add(10 * time.Minute).UnixMilli()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"code":-1003,"msg":"IP: 202.154.12.9 is banned until %d. Please use the Binance UI or webhooks to perform this action."}`, futureBanMs)))
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")

	// Panggilan pertama: harus menerima HTTP 418 dan TIDAK melakukan retry
	_, err := client.DoRequest(context.Background(), http.MethodGet, "/fapi/v1/openInterest", nil, false)
	if err == nil {
		t.Fatalf("expected error from 418, got nil")
	}

	if reqCount != 1 {
		t.Errorf("expected exactly 1 request (no retry on 418), got %d", reqCount)
	}

	banned, until := client.IsIPBanned()
	if !banned {
		t.Fatalf("expected client to be flagged as IP banned")
	}
	if until.UnixMilli() != futureBanMs {
		t.Errorf("expected ban until %d, got %d", futureBanMs, until.UnixMilli())
	}

	// Panggilan kedua: harus langsung ditolak oleh circuit breaker TANPA memanggil HTTP server
	_, err2 := client.DoRequest(context.Background(), http.MethodGet, "/fapi/v1/openInterest", nil, false)
	if err2 == nil {
		t.Fatalf("expected circuit breaker error on second request, got nil")
	}
	if !strings.Contains(err2.Error(), "outgoing requests paused due to active Binance IP ban") {
		t.Errorf("unexpected error message: %v", err2)
	}

	if reqCount != 1 {
		t.Errorf("expected reqCount to remain 1 after second request, got %d", reqCount)
	}
}

func TestPlaceAlgoOrder_IdempotencyOpenAlgoOrders(t *testing.T) {
	var postAlgoAttempts int
	var openAlgoQueries int
	var regularOrderQueries int

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/algoOrder":
			postAlgoAttempts++
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":-1000,"msg":"internal error"}`))
		case "/fapi/v1/openAlgoOrders":
			openAlgoQueries++
			_, _ = w.Write([]byte(`[
				{"algoId":778899,"clientAlgoId":"ALGO_SL_BTC_1","symbol":"BTCUSDT","algoStatus":"NEW"}
			]`))
		case "/fapi/v1/order":
			regularOrderQueries++
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	algoID, err := client.PlaceAlgoOrder(context.Background(), "BTCUSDT", model.SideShort, "STOP_MARKET", decimal.NewFromFloat(0.1), decimal.NewFromFloat(85000), "ALGO_SL_BTC_1")
	if err != nil {
		t.Fatalf("expected algo idempotency recovery without error, got: %v", err)
	}
	if algoID != 778899 {
		t.Fatalf("expected recovered algoId 778899, got %d", algoID)
	}
	if postAlgoAttempts != 1 || openAlgoQueries != 1 {
		t.Fatalf("expected 1 algo POST and 1 openAlgoOrders query, got post=%d openAlgo=%d", postAlgoAttempts, openAlgoQueries)
	}
	if regularOrderQueries != 0 {
		t.Fatalf("expected 0 /fapi/v1/order queries for algo idempotency, got %d", regularOrderQueries)
	}
}

func TestPlaceAlgoOrder_RejectsClientAlgoIDExceeding36Chars(t *testing.T) {
	client := NewClient("http://127.0.0.1:0", "KEY", "SECRET")
	tooLongID := strings.Repeat("A", 37)
	_, err := client.PlaceAlgoOrder(context.Background(), "1000PEPEUSDT", model.SideShort, "STOP_MARKET", decimal.NewFromFloat(10), decimal.NewFromFloat(0.01), tooLongID)
	if err == nil || !strings.Contains(err.Error(), "exceeds 36 chars") {
		t.Fatalf("expected error rejecting clientAlgoId > 36 chars, got: %v", err)
	}
}
