package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

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
}
