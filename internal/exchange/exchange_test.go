package exchange

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
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

func TestInitAndVerifySymbol_FailClosedOnInvalidDualSideJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/positionSide/dual" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{invalid_json`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	ok, err := client.InitAndVerifySymbol(context.Background(), "BTCUSDT", 8)
	if ok || err == nil || !strings.Contains(err.Error(), "decode position mode") {
		t.Fatalf("expected fail-closed JSON decode error, got ok=%v err=%v", ok, err)
	}
}

func TestInitAndVerifySymbol_VerifiesAndFailsClosedOnRiskMismatch(t *testing.T) {
	marginType := "CROSSED"
	lev := "8"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/positionSide/dual":
			_, _ = w.Write([]byte(`{"dualSidePosition":false}`))
		case "/fapi/v1/marginType", "/fapi/v1/leverage":
			_, _ = w.Write([]byte(`{"code":200,"msg":"success"}`))
		case "/fapi/v2/positionRisk":
			_, _ = w.Write([]byte(fmt.Sprintf(`[{"symbol":"BTCUSDT","positionAmt":"0","marginType":"%s","leverage":"%s"}]`, marginType, lev)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	if ok, err := client.InitAndVerifySymbol(context.Background(), "BTCUSDT", 8); ok || err == nil || !strings.Contains(err.Error(), "marginType") {
		t.Fatalf("expected mismatch marginType error, got ok=%v err=%v", ok, err)
	}

	marginType = "isolated"
	lev = "10"
	if ok, err := client.InitAndVerifySymbol(context.Background(), "BTCUSDT", 8); ok || err == nil || !strings.Contains(err.Error(), "leverage") {
		t.Fatalf("expected mismatch leverage error, got ok=%v err=%v", ok, err)
	}

	lev = "8"
	if ok, err := client.InitAndVerifySymbol(context.Background(), "BTCUSDT", 8); !ok || err != nil {
		t.Fatalf("expected InitAndVerifySymbol success when verified isolated & 8x, got ok=%v err=%v", ok, err)
	}
}

func TestExchangeSignaturePlacement_AlgoType_1021_And_USDTBasis(t *testing.T) {
	var lastAlgoBody, lastMarketBody, lastGetQuery string
	var orderAttempts atomic.Int32
	var timeSynced atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		rawBody := string(bodyBytes)
		switch r.URL.Path {
		case "/fapi/v1/time":
			timeSynced.Store(true)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"serverTime":%d}`, time.Now().UnixMilli())))
		case "/fapi/v1/algoOrder":
			lastAlgoBody = rawBody
			_, _ = w.Write([]byte(`{"algoId":778899,"clientAlgoId":"ALGO_SL_1"}`))
		case "/fapi/v1/order":
			if r.Method == http.MethodGet {
				lastGetQuery = r.URL.RawQuery
				_, _ = w.Write([]byte(`{"orderId":10,"clientOrderId":"IDEMP_EXP","status":"EXPIRED","executedQty":"0"}`))
				return
			}
			parsedForm, _ := url.ParseQuery(rawBody)
			if parsedForm.Get("newClientOrderId") == "RETRY_1021" && orderAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}`))
				return
			}
			lastMarketBody = rawBody
			_, _ = w.Write([]byte(`{"orderId":555,"status":"FILLED","avgPrice":"100.0","executedQty":"1.0"}`))
		case "/fapi/v2/account":
			// TotalWalletBalance includes $15 BNB, while USDT asset is $125.00
			_, _ = w.Write([]byte(`{
				"totalWalletBalance":"140.00",
				"totalUnrealizedProfit":"2.00",
				"availableBalance":"135.00",
				"assets":[
					{"asset":"BNB","walletBalance":"0.025"},
					{"asset":"USDT","walletBalance":"125.00","unrealizedProfit":"2.00","availableBalance":"120.00"}
				],
				"positions":[]
			}`))
		case "/fapi/v1/positionSide/dual":
			_, _ = w.Write([]byte(`{"dualSidePosition":false}`))
		case "/fapi/v1/marginType", "/fapi/v1/leverage":
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/fapi/v2/positionRisk":
			_, _ = w.Write([]byte(`[]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	ctx := context.Background()

	// 1. PlaceAlgoOrder uses type=STOP_MARKET (not orderType) and signature= at very end
	algoID, err := client.PlaceAlgoOrder(ctx, "BTCUSDT", model.SideShort, "STOP_MARKET", decimal.NewFromFloat(0.1), decimal.NewFromFloat(60000), "ALGO_SL_1")
	if err != nil || algoID != 778899 {
		t.Fatalf("PlaceAlgoOrder failed: algoID=%d err=%v", algoID, err)
	}
	parsedAlgo, _ := url.ParseQuery(lastAlgoBody)
	if parsedAlgo.Get("type") != "STOP_MARKET" || parsedAlgo.Get("orderType") != "" {
		t.Fatalf("expected type=STOP_MARKET and no orderType param, got body=%s", lastAlgoBody)
	}
	if !strings.Contains(lastAlgoBody, "&signature=") || strings.Contains(strings.Split(lastAlgoBody, "&signature=")[1], "&") {
		t.Fatalf("expected &signature=<hex> strictly at end of algoOrder body, got: %s", lastAlgoBody)
	}

	// 2. -1021 triggers SyncServerTime and retries successfully + PlaceMarketClose sends newOrderRespType=RESULT
	_, _, _, err = client.PlaceMarketClose(ctx, "ETHUSDT", model.SideShort, decimal.NewFromFloat(1.0), "RETRY_1021")
	if err != nil || !timeSynced.Load() || orderAttempts.Load() != 2 {
		t.Fatalf("expected -1021 auto resync and retry success, synced=%v attempts=%d err=%v", timeSynced.Load(), orderAttempts.Load(), err)
	}
	if !strings.Contains(lastMarketBody, "newOrderRespType=RESULT") || strings.Contains(strings.Split(lastMarketBody, "&signature=")[1], "&") {
		t.Fatalf("expected newOrderRespType=RESULT and trailing signature, got: %s", lastMarketBody)
	}

	// 3. FetchAccountBalance uses USDT asset balance (125.00), ignoring TotalWalletBalance (140.00 w/ BNB)
	wb, up, ab, _, err := client.FetchAccountBalance(ctx)
	if err != nil || !wb.Equal(decimal.NewFromFloat(125.0)) || !up.Equal(decimal.NewFromFloat(2.0)) || !ab.Equal(decimal.NewFromFloat(120.0)) {
		t.Fatalf("expected USDT basis 125/2/120, got wb=%s up=%s ab=%s err=%v", wb, up, ab, err)
	}

	// 4. InitAndVerifySymbol fails closed when positionRisk omits symbol
	if ok, err := client.InitAndVerifySymbol(ctx, "BTCUSDT", 8); ok || err == nil || !strings.Contains(err.Error(), "missing from positionRisk") {
		t.Fatalf("expected fail-closed missing symbol error, got ok=%v err=%v", ok, err)
	}

	// 5. verifyPostOrderIdempotency rejects EXPIRED zero-fill orders and keeps trailing signature on GET query
	idempParams := url.Values{}
	idempParams.Set("symbol", "BTCUSDT")
	idempParams.Set("newClientOrderId", "IDEMP_EXP")
	if _, ok := client.verifyPostOrderIdempotency(ctx, "/fapi/v1/order", idempParams); ok {
		t.Fatalf("expected verifyPostOrderIdempotency to reject EXPIRED zero-fill order")
	}
	if !strings.Contains(lastGetQuery, "&signature=") || strings.Contains(strings.Split(lastGetQuery, "&signature=")[1], "&") {
		t.Fatalf("expected GET query trailing signature, got: %s", lastGetQuery)
	}
}

func TestDoRequest_PostOrderFailClosedOnAmbiguousNetworkAnd5xx(t *testing.T) {
	var postOrderAttempts atomic.Int32
	var postAlgoAttempts atomic.Int32
	var getOrderMode atomic.Int32 // 0: 502 error (ambiguous), 1: -2013 not found, 2: FILLED exists

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/order":
			if r.Method == http.MethodPost {
				n := postOrderAttempts.Add(1)
				if getOrderMode.Load() == 1 && n == 2 {
					_, _ = w.Write([]byte(`{"orderId":9001,"clientOrderId":"CID_1","status":"FILLED","avgPrice":"100","executedQty":"1"}`))
					return
				}
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"code":0,"msg":"bad gateway"}`))
				return
			}
			// GET /fapi/v1/order idempotency check
			switch getOrderMode.Load() {
			case 0:
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`upstream unavailable`))
			case 1:
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-2013,"msg":"Order does not exist."}`))
			case 2:
				_, _ = w.Write([]byte(`{"orderId":9002,"clientOrderId":"CID_2","status":"FILLED","avgPrice":"100","executedQty":"1"}`))
			}
		case "/fapi/v1/algoOrder":
			if r.Method == http.MethodPost {
				postAlgoAttempts.Add(1)
				w.WriteHeader(http.StatusGatewayTimeout)
				_, _ = w.Write([]byte(`timeout`))
				return
			}
		case "/fapi/v1/openAlgoOrders":
			// Even empty array or error must not fail-open retry POST /algoOrder
			_, _ = w.Write([]byte(`[]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	ctx := context.Background()

	// 1. Ambiguous 502 + GET 503 -> MUST abort after 1 POST attempt (fail closed!)
	params := url.Values{}
	params.Set("symbol", "BTCUSDT")
	params.Set("newClientOrderId", "CID_AMB")
	if _, err := client.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true); err == nil {
		t.Fatalf("expected error on ambiguous POST /order 502")
	}
	if postOrderAttempts.Load() != 1 {
		t.Fatalf("expected fail-closed 1 POST /order attempt on ambiguous verify error, got %d", postOrderAttempts.Load())
	}

	// 2. Explicit -2013 -> allowed to retry once and succeed
	postOrderAttempts.Store(0)
	getOrderMode.Store(1)
	if _, err := client.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true); err != nil {
		t.Fatalf("expected retry to succeed after explicit -2013 not found, got %v", err)
	}
	if postOrderAttempts.Load() != 2 {
		t.Fatalf("expected 2 POST /order attempts after -2013, got %d", postOrderAttempts.Load())
	}

	// 3. Confirmed exists in GET -> returns existing body without retrying POST
	postOrderAttempts.Store(0)
	getOrderMode.Store(2)
	body, err := client.DoRequest(ctx, http.MethodPost, "/fapi/v1/order", params, true)
	if err != nil || !strings.Contains(string(body), "9002") || postOrderAttempts.Load() != 1 {
		t.Fatalf("expected existing order body with 1 POST attempt, body=%s err=%v attempts=%d", string(body), err, postOrderAttempts.Load())
	}

	// 4. POST /algoOrder ambiguous 504 -> MUST abort after 1 attempt
	algoParams := url.Values{}
	algoParams.Set("symbol", "BTCUSDT")
	algoParams.Set("clientAlgoId", "ALGO_AMB")
	if _, err := client.DoRequest(ctx, http.MethodPost, "/fapi/v1/algoOrder", algoParams, true); err == nil {
		t.Fatalf("expected error on ambiguous POST /algoOrder 504")
	}
	if postAlgoAttempts.Load() != 1 {
		t.Fatalf("expected fail-closed 1 POST /algoOrder attempt, got %d", postAlgoAttempts.Load())
	}
}

func TestPlaceLimitClose_GTXFallsBackToGTCAndReturnsActualClientOrderID(t *testing.T) {
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/fapi/v1/order" && r.Method == http.MethodPost {
			n := attempts.Add(1)
			rawBody := make([]byte, 4096)
			readN, _ := r.Body.Read(rawBody)
			vals, _ := url.ParseQuery(string(rawBody[:readN]))
			if n == 1 {
				if vals.Get("timeInForce") != "GTX" {
					t.Fatalf("expected first attempt GTX, got %s", vals.Get("timeInForce"))
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-5022,"msg":"Post Only order will be rejected"}`))
				return
			}
			if vals.Get("timeInForce") != "GTC" {
				t.Fatalf("expected fallback attempt GTC, got %s", vals.Get("timeInForce"))
			}
			cid := vals.Get("newClientOrderId")
			_, _ = w.Write([]byte(fmt.Sprintf(`{"orderId":55123,"clientOrderId":%q,"status":"NEW"}`, cid)))
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	oid, actualCID, err := client.PlaceLimitClose(context.Background(), "ETHUSDT", model.SideLong, decimal.NewFromFloat(1.0), decimal.NewFromFloat(2500), "ORIG_TP1_CID")
	if err != nil || oid != 55123 {
		t.Fatalf("PlaceLimitClose fallback failed: oid=%d actualCID=%q err=%v", oid, actualCID, err)
	}
	if actualCID == "" || actualCID == "ORIG_TP1_CID" || !strings.HasPrefix(actualCID, "ALGO_TP1_") {
		t.Fatalf("expected actual regenerated ALGO_TP1 clientOrderId returned, got %q", actualCID)
	}
}

func TestEnsureOneWayMode_And_InitAndVerifySymbolMarginAndLeverage(t *testing.T) {
	var dualCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/positionSide/dual":
			dualCalls.Add(1)
			_, _ = w.Write([]byte(`{"dualSidePosition":false}`))
		case "/fapi/v1/marginType", "/fapi/v1/leverage":
			_, _ = w.Write([]byte(`{"code":200}`))
		case "/fapi/v2/positionRisk":
			_, _ = w.Write([]byte(`[{"symbol":"SOLUSDT","marginType":"isolated","leverage":"8"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "KEY", "SECRET")
	ctx := context.Background()
	if err := client.EnsureOneWayMode(ctx); err != nil {
		t.Fatalf("EnsureOneWayMode failed: %v", err)
	}
	if dualCalls.Load() != 1 {
		t.Fatalf("expected 1 dual position check, got %d", dualCalls.Load())
	}
	ok, err := client.InitAndVerifySymbolMarginAndLeverage(ctx, "SOLUSDT", 8)
	if err != nil || !ok {
		t.Fatalf("InitAndVerifySymbolMarginAndLeverage failed: ok=%v err=%v", ok, err)
	}
	if dualCalls.Load() != 1 {
		t.Fatalf("expected InitAndVerifySymbolMarginAndLeverage NOT to call /positionSide/dual, total dual calls=%d", dualCalls.Load())
	}
}

func TestFetchExchangeInfo_ParsesMarketLotSizeAndMaxQty(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"symbols": [{
				"symbol": "DOGEUSDT",
				"contractType": "PERPETUAL",
				"status": "TRADING",
				"filters": [
					{"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
					{"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1", "maxQty": "10000000"},
					{"filterType": "MARKET_LOT_SIZE", "stepSize": "10", "minQty": "5", "maxQty": "250000"},
					{"filterType": "MIN_NOTIONAL", "notional": "5.0"}
				]
			}]
		}`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "", "")
	filters, err := client.FetchExchangeInfo(context.Background())
	if err != nil {
		t.Fatalf("FetchExchangeInfo failed: %v", err)
	}
	ef, ok := filters["DOGEUSDT"]
	if !ok {
		t.Fatalf("expected DOGEUSDT in filters")
	}
	if !ef.MaxQty.Equal(decimal.NewFromInt(10000000)) ||
		!ef.MarketStepSize.Equal(decimal.NewFromInt(10)) ||
		!ef.MarketMinQty.Equal(decimal.NewFromInt(5)) ||
		!ef.MarketMaxQty.Equal(decimal.NewFromInt(250000)) {
		t.Fatalf("unexpected parsed MARKET_LOT_SIZE / MaxQty fields: %+v", ef)
	}
}

func TestFetchKlines_PaginatesBeyond1500(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	totalBars := 2200
	step := 5 * time.Minute
	var pageCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/klines" {
			http.NotFound(w, r)
			return
		}
		pageCalls.Add(1)
		lim, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		endMsStr := r.URL.Query().Get("endTime")
		endIdx := totalBars
		if endMsStr != "" {
			endMs, _ := strconv.ParseInt(endMsStr, 10, 64)
			for idx := totalBars - 1; idx >= 0; idx-- {
				otMs := base.Add(time.Duration(idx) * step).UnixMilli()
				if otMs <= endMs {
					endIdx = idx + 1
					break
				}
			}
		}
		startIdx := endIdx - lim
		if startIdx < 0 {
			startIdx = 0
		}
		var sb strings.Builder
		sb.WriteByte('[')
		for i := startIdx; i < endIdx; i++ {
			if i > startIdx {
				sb.WriteByte(',')
			}
			ot := base.Add(time.Duration(i) * step)
			ct := ot.Add(step - time.Millisecond)
			fmt.Fprintf(&sb, `[%d,"100.0","101.0","99.0","100.5","10",%d,"1000.0",5,"5","500.0","0"]`, ot.UnixMilli(), ct.UnixMilli())
		}
		sb.WriteByte(']')
		_, _ = w.Write([]byte(sb.String()))
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "", "")
	candles, err := client.FetchKlines(context.Background(), "ETHUSDT", "5m", 2000)
	if err != nil {
		t.Fatalf("FetchKlines pagination failed: %v", err)
	}
	if pageCalls.Load() != 2 || len(candles) != 2000 {
		t.Fatalf("expected 2 page calls and 2000 candles, got pages=%d len=%d", pageCalls.Load(), len(candles))
	}
	for i := 1; i < len(candles); i++ {
		if candles[i].OpenTime.Sub(candles[i-1].OpenTime) != step {
			t.Fatalf("non-contiguous candle at %d: prev=%v cur=%v", i, candles[i-1].OpenTime, candles[i].OpenTime)
		}
	}
}

func TestFetchOpenInterestHistWithPeriod_AndResolvePeriod(t *testing.T) {
	if ResolveOIStatsPeriod("1m") != "5m" || ResolveOIStatsPeriod("5m") != "5m" || ResolveOIStatsPeriod("15m") != "15m" || ResolveOIStatsPeriod("1h") != "1h" {
		t.Fatalf("unexpected ResolveOIStatsPeriod mappings")
	}
	var gotPeriod string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPeriod = r.URL.Query().Get("period")
		_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","sumOpenInterest":"12345.0","timestamp":1700000000000}]`))
	}))
	defer ts.Close()

	client := NewClient(ts.URL, "", "")
	res, err := client.FetchOpenInterestHistWithPeriod(context.Background(), "BTCUSDT", "15m", 4)
	if err != nil || len(res) != 1 || gotPeriod != "15m" {
		t.Fatalf("expected period=15m and 1 item, got period=%q len=%d err=%v", gotPeriod, len(res), err)
	}
}
