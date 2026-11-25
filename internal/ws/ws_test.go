package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
	"trml/internal/model"
)

func TestWebSocketPayloadParsing(t *testing.T) {
	var bookTickerParsed, aggTradeParsed, markParsed, klineParsed, orderParsed, accountParsed int32

	handlers := EventHandlers{
		OnBookTicker: func(symbol string, bestBid, bestAsk float64, now time.Time) {
			if symbol == "BTCUSDT" && bestBid > 65000.0 && bestAsk > 65000.0 {
				atomic.AddInt32(&bookTickerParsed, 1)
			}
		},
		OnAggTrade: func(symbol string, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {
			if symbol == "BTCUSDT" && tradePrice == 65000.2 && isBuyerMaker {
				atomic.AddInt32(&aggTradeParsed, 1)
			}
		},
		OnMarkPrice: func(symbol string, markPrice, fundingRate float64, now time.Time) {
			if symbol == "BTCUSDT" && markPrice == 65001.0 && fundingRate == 0.0001 {
				atomic.AddInt32(&markParsed, 1)
			}
		},
		OnKline5m: func(symbol string, candle model.Candle, isClosed bool) {
			if symbol == "BTCUSDT" && isClosed && candle.Close == 65050.0 {
				atomic.AddInt32(&klineParsed, 1)
			}
		},
		OnOrderTradeUpdate: func(clientOrderID string, exchangeOrderID int64, symbol string, status model.OrderLifecycleStatus, cumFilledQty, avgPrice, realizedProfit decimal.Decimal) {
			if clientOrderID == "ORDER_123" && status == model.StatusFilled && cumFilledQty.Equal(decimal.NewFromFloat(1.5)) && avgPrice.Equal(decimal.NewFromFloat(65000.0)) {
				atomic.AddInt32(&orderParsed, 1)
			}
		},
		OnAccountUpdate: func(walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, eventTime time.Time) {
			if walletBal.Equal(decimal.NewFromFloat(10000.0)) && positions["BTCUSDT"].Qty.Equal(decimal.NewFromFloat(0.5)) {
				atomic.AddInt32(&accountParsed, 1)
			}
		},
	}

	mgr := NewStreamManager(handlers)

	// 1. BookTicker payload
	rawBook := []byte(`{"e":"bookTicker","u":123456,"s":"BTCUSDT","b":"65000.1","B":"10","a":"65000.5","A":"5","T":1700000000000,"E":1700000000000}`)
	mgr.ProcessMessage(rawBook)

	// 2. AggTrade payload
	rawAgg := []byte(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":123,"p":"65000.2","q":"0.8","f":1,"l":2,"T":1700000000000,"m":true}`)
	mgr.ProcessMessage(rawAgg)

	// 3. MarkPrice payload
	rawMark := []byte(`{"e":"markPriceUpdate","E":1700000000000,"s":"BTCUSDT","p":"65001.0","P":"65000.0","r":"0.0001","T":1700000000000}`)
	mgr.ProcessMessage(rawMark)

	// 4. Kline payload
	rawKline := []byte(`{"e":"kline","E":1700000000000,"s":"BTCUSDT","k":{"t":1700000000000,"T":1700000300000,"s":"BTCUSDT","i":"5m","f":1,"L":10,"o":"65000.0","c":"65050.0","h":"65060.0","l":"64990.0","v":"100.0","n":50,"x":true,"q":"6500000.0","V":"60.0","Q":"3900000.0","B":"0"}}`)
	mgr.ProcessMessage(rawKline)

	// 5. ORDER_TRADE_UPDATE payload
	rawOTU := []byte(`{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"T":1700000000000,"o":{"s":"BTCUSDT","c":"ORDER_123","S":"BUY","o":"LIMIT","f":"GTC","q":"1.5","p":"65000.0","ap":"65000.0","sp":"0","x":"TRADE","X":"FILLED","i":999888,"l":"1.5","z":"1.5","L":"65000.0","N":"USDT","n":"1.0","T":1700000000000,"t":1,"b":"0","a":"0","m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"LIMIT","ps":"BOTH","cp":false,"rp":"0","pP":false,"si":0,"ss":0}}`)
	mgr.ProcessMessage(rawOTU)

	// 6. ACCOUNT_UPDATE payload
	rawAU := []byte(`{"e":"ACCOUNT_UPDATE","E":1700000000000,"T":1700000000000,"a":{"m":"ORDER","B":[{"a":"USDT","wb":"10000.0","cw":"10000.0","bc":"0"}],"P":[{"s":"BTCUSDT","pa":"0.5","ep":"64500.0","cr":"0","up":"250.0","mt":"isolated","iw":"0","ps":"BOTH"}]}}`)
	mgr.ProcessMessage(rawAU)

	if atomic.LoadInt32(&bookTickerParsed) != 1 {
		t.Errorf("bookTicker parsing failed")
	}
	if atomic.LoadInt32(&aggTradeParsed) != 1 {
		t.Errorf("aggTrade parsing failed")
	}
	if atomic.LoadInt32(&markParsed) != 1 {
		t.Errorf("markPrice parsing failed")
	}
	if atomic.LoadInt32(&klineParsed) != 1 {
		t.Errorf("kline parsing failed")
	}
	if atomic.LoadInt32(&orderParsed) != 1 {
		t.Errorf("order trade update parsing failed")
	}
	if atomic.LoadInt32(&accountParsed) != 1 {
		t.Errorf("account update parsing failed")
	}
}

func TestBuildUserDataStreamURL(t *testing.T) {
	listenKey := "mockListenKey12345"

	// 1. Mainnet 2026 Spec: wss://fstream.binance.com/private/ws?listenKey=<key>&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,ALGO_UPDATE,listenKeyExpired
	mainnetURL := BuildUserDataStreamURL("wss://fstream.binance.com/private/ws", listenKey, true)
	expectedMainnet := "wss://fstream.binance.com/private/ws?listenKey=mockListenKey12345&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,ALGO_UPDATE,listenKeyExpired"
	if mainnetURL != expectedMainnet {
		t.Errorf("expected mainnet user stream URL '%s', got '%s'", expectedMainnet, mainnetURL)
	}

	// 2. Testnet Legacy Spec: wss://stream.binancefuture.com/ws/<key>
	testnetURL := BuildUserDataStreamURL("wss://stream.binancefuture.com/ws", listenKey, false)
	expectedTestnet := "wss://stream.binancefuture.com/ws/mockListenKey12345"
	if testnetURL != expectedTestnet {
		t.Errorf("expected testnet user stream URL '%s', got '%s'", expectedTestnet, testnetURL)
	}
}

func TestKlineClosed_DynamicInterval(t *testing.T) {
	var parsed15m int32
	var capturedInterval string
	mgr := NewStreamManager(EventHandlers{
		OnKlineClosed: func(symbol string, candle model.Candle, isClosed bool) {
			atomic.AddInt32(&parsed15m, 1)
			capturedInterval = "15m"
		},
	})

	raw15m := []byte(`{"e":"kline","E":1700000000000,"s":"SOLUSDT","k":{"t":1700000000000,"T":1700000900000,"s":"SOLUSDT","i":"15m","f":1,"L":10,"o":"150.0","c":"152.0","h":"153.0","l":"149.0","v":"1000.0","n":50,"x":true,"q":"151000.0","V":"600.0","Q":"90600.0","B":"0"}}`)
	mgr.ProcessMessage(raw15m)

	if atomic.LoadInt32(&parsed15m) != 1 || capturedInterval != "15m" {
		t.Fatalf("expected 15m kline to be parsed via OnKlineClosed, got count=%d, interval=%s", parsed15m, capturedInterval)
	}
}

func TestDynamicStream_DifferentialSubscribeUnsubscribe(t *testing.T) {
	var connCount atomic.Int32
	ctrlCh := make(chan map[string]any, 4)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connCount.Add(1)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

		for {
			_, raw, err := c.Read(r.Context())
			if err != nil {
				return
			}
			var msg map[string]any
			if json.Unmarshal(raw, &msg) == nil {
				ctrlCh <- msg
			}
		}
	}))
	defer ts.Close()

	wsBase := "ws" + strings.TrimPrefix(ts.URL, "http") + "/stream"
	mgr := NewStreamManager(EventHandlers{})
	ds := NewDynamicStream(mgr, wsBase, []string{"btcusdt@bookTicker", "solusdt@bookTicker"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ds.Start(ctx)

	// Tunggu koneksi tersambung sebelum UpdateStreams
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ds.mu.Lock()
		connected := ds.conn != nil
		ds.mu.Unlock()
		if connected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Update dengan delta: solusdt dihapus, ethusdt ditambahkan, btcusdt dipertahankan
	if err := ds.UpdateStreams(ctx, []string{"btcusdt@bookTicker", "ethusdt@bookTicker"}); err != nil {
		t.Fatalf("UpdateStreams failed: %v", err)
	}

	var methods []string
	timeout := time.After(2 * time.Second)
	for len(methods) < 2 {
		select {
		case msg := <-ctrlCh:
			m, _ := msg["method"].(string)
			methods = append(methods, m)
		case <-timeout:
			t.Fatalf("timeout waiting for control frames, got methods=%v", methods)
		}
	}

	if connCount.Load() != 1 {
		t.Fatalf("expected 1 persistent connection without teardown, got %d", connCount.Load())
	}
	if methods[0] != "UNSUBSCRIBE" || methods[1] != "SUBSCRIBE" {
		t.Fatalf("expected [UNSUBSCRIBE SUBSCRIBE], got %v", methods)
	}
}

func TestShardedDynamicStream_PartitionAndDifferentialUpdate(t *testing.T) {
	var totalConns atomic.Int32
	type ctrlEvent struct {
		url    string
		method string
		params []string
	}
	ctrlCh := make(chan ctrlEvent, 32)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalConns.Add(1)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

		for {
			_, raw, err := c.Read(r.Context())
			if err != nil {
				return
			}
			var msg struct {
				Method string   `json:"method"`
				Params []string `json:"params"`
			}
			if json.Unmarshal(raw, &msg) == nil && msg.Method != "" {
				ctrlCh <- ctrlEvent{url: r.URL.RawQuery, method: msg.Method, params: msg.Params}
			}
		}
	}))
	defer ts.Close()

	wsBase := "ws" + strings.TrimPrefix(ts.URL, "http") + "/public/stream"
	mgr := NewStreamManager(EventHandlers{})
	initial := []string{
		"a@bookTicker",
		"b@bookTicker",
		"c@bookTicker",
		"d@bookTicker",
		"e@bookTicker",
	}
	sds := NewShardedDynamicStream(mgr, wsBase, initial, 2)
	if sds.ShardCount() != 3 {
		t.Fatalf("expected 3 shards for 5 streams with maxPerShard=2, got %d", sds.ShardCount())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sds.Start(ctx)

	waitConnected := func(expected int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			sds.mu.Lock()
			ready := len(sds.shards) == expected
			for _, sh := range sds.shards {
				sh.ds.mu.Lock()
				if sh.ds.conn == nil {
					ready = false
				}
				sh.ds.mu.Unlock()
			}
			sds.mu.Unlock()
			if ready {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for %d connected shards", expected)
	}
	waitConnected(3)

	// Ganti 1 stream (d -> f) tanpa mengubah jumlah shard; tidak boleh ada reconnect penuh!
	updated := []string{
		"a@bookTicker",
		"b@bookTicker",
		"c@bookTicker",
		"f@bookTicker",
		"e@bookTicker",
	}
	if err := sds.UpdateStreams(ctx, updated); err != nil {
		t.Fatalf("UpdateStreams diff failed: %v", err)
	}
	if sds.ShardCount() != 3 {
		t.Fatalf("expected ShardCount=3 after same-size diff update, got %d", sds.ShardCount())
	}

	var gotUnsub, gotSub bool
	deadline := time.After(2 * time.Second)
	for !(gotUnsub && gotSub) {
		select {
		case ev := <-ctrlCh:
			if ev.method == "UNSUBSCRIBE" && len(ev.params) == 1 && ev.params[0] == "d@bookTicker" {
				gotUnsub = true
			}
			if ev.method == "SUBSCRIBE" && len(ev.params) == 1 && ev.params[0] == "f@bookTicker" {
				gotSub = true
			}
		case <-deadline:
			t.Fatalf("timeout waiting for sharded UNSUBSCRIBE(d) + SUBSCRIBE(f), unsub=%v sub=%v", gotUnsub, gotSub)
		}
	}
	if totalConns.Load() != 3 {
		t.Fatalf("expected zero reconnects across shards (still 3 total connections), got %d", totalConns.Load())
	}

	// Shrink menjadi 2 stream -> harus menyusut ke 1 shard
	if err := sds.UpdateStreams(ctx, []string{"a@bookTicker", "b@bookTicker"}); err != nil {
		t.Fatalf("UpdateStreams shrink failed: %v", err)
	}
	if sds.ShardCount() != 1 {
		t.Fatalf("expected ShardCount=1 after shrink, got %d", sds.ShardCount())
	}

	// Grow kembali menjadi 5 stream -> menambah 2 shard baru
	if err := sds.UpdateStreams(ctx, initial); err != nil {
		t.Fatalf("UpdateStreams grow failed: %v", err)
	}
	if sds.ShardCount() != 3 {
		t.Fatalf("expected ShardCount=3 after grow, got %d", sds.ShardCount())
	}
	waitConnected(3)
}

func TestStreamManager_PingAndIdleWatchdog(t *testing.T) {
	t.Run("idle connection triggers watchdog disconnect", func(t *testing.T) {
		var mu sync.Mutex
		var conns int
		disconnected := make(chan struct{}, 4)

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			mu.Lock()
			conns++
			mu.Unlock()
			defer func() {
				_ = c.Close(websocket.StatusNormalClosure, "")
				select {
				case disconnected <- struct{}{}:
				default:
				}
			}()
			for {
				if _, _, err := c.Read(r.Context()); err != nil {
					return
				}
			}
		}))
		defer ts.Close()

		mgr := NewStreamManager(EventHandlers{})
		mgr.IdleTimeout = 100 * time.Millisecond
		mgr.PingInterval = 500 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/stream?streams=btcusdt@bookTicker"
		c, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		err = mgr.readLoop(ctx, c, time.Now())
		_ = c.Close(websocket.StatusNormalClosure, "")
		if err == nil || ctx.Err() != nil {
			t.Fatalf("expected readLoop internal idle watchdog exit before parent ctx deadline, got err=%v ctxErr=%v", err, ctx.Err())
		}
		select {
		case <-disconnected:
		case <-time.After(1 * time.Second):
			t.Fatalf("expected server to observe client disconnect after idle timeout")
		}
	})

	t.Run("active frames and periodic ping stay healthy", func(t *testing.T) {
		var parsedTicks atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

			go func() {
				for {
					if _, _, err := c.Read(r.Context()); err != nil {
						return
					}
				}
			}()

			payload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","s":"BTCUSDT","b":"65000.0","a":"65001.0"}}`)
			ticker := time.NewTicker(40 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					if err := c.Write(r.Context(), websocket.MessageText, payload); err != nil {
						return
					}
				}
			}
		}))
		defer ts.Close()

		mgr := NewStreamManager(EventHandlers{
			OnBookTicker: func(symbol string, bestBid, bestAsk float64, now time.Time) {
				parsedTicks.Add(1)
			},
		})
		mgr.IdleTimeout = 250 * time.Millisecond
		mgr.PingInterval = 30 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
		defer cancel()

		wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/stream?streams=btcusdt@bookTicker"
		c, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		err = mgr.readLoop(ctx, c, time.Now())
		_ = c.Close(websocket.StatusNormalClosure, "")
		if err == nil || ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("expected clean context expiry under healthy pings + frames, got err=%v ctxErr=%v", err, ctx.Err())
		}
		if parsedTicks.Load() < 3 {
			t.Fatalf("expected >= 3 bookTicker frames processed while periodic Ping active, got %d", parsedTicks.Load())
		}
	})

	t.Run("busy stream skips Ping and survives even without server Pong", func(t *testing.T) {
		var parsedTicks atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

			// Sengaja tidak memanggil c.Read() agar tidak ada Pong otomatis dari server:
			// jika client tetap mengirim Ping saat data aktif, PingTimeout 30ms akan memutus koneksi.
			payload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","s":"BTCUSDT","b":"65000.0","a":"65001.0"}}`)
			ticker := time.NewTicker(15 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					if err := c.Write(r.Context(), websocket.MessageText, payload); err != nil {
						return
					}
				}
			}
		}))
		defer ts.Close()

		mgr := NewStreamManager(EventHandlers{
			OnBookTicker: func(symbol string, bestBid, bestAsk float64, now time.Time) {
				parsedTicks.Add(1)
			},
		})
		mgr.IdleTimeout = 400 * time.Millisecond
		mgr.PingInterval = 35 * time.Millisecond
		mgr.PingTimeout = 30 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
		defer cancel()

		wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/stream?streams=btcusdt@bookTicker"
		c, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		err = mgr.readLoop(ctx, c, time.Now())
		_ = c.Close(websocket.StatusNormalClosure, "")
		if err == nil || ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("expected busy stream to survive without sending unnecessary pings, got err=%v ctxErr=%v", err, ctx.Err())
		}
		if parsedTicks.Load() < 5 {
			t.Fatalf("expected >= 5 bookTicker frames, got %d", parsedTicks.Load())
		}
	})

	t.Run("silent dead socket triggers idle Ping timeout before IdleTimeout", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
			<-r.Context().Done()
		}))
		defer ts.Close()

		mgr := NewStreamManager(EventHandlers{})
		mgr.IdleTimeout = 2 * time.Second
		mgr.PingInterval = 40 * time.Millisecond
		mgr.PingTimeout = 40 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer cancel()

		wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/stream?streams=btcusdt@bookTicker"
		c, _, err := websocket.Dial(ctx, wsURL, nil)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		start := time.Now()
		err = mgr.readLoop(ctx, c, start)
		elapsed := time.Since(start)
		_ = c.Close(websocket.StatusNormalClosure, "")
		if err == nil || ctx.Err() != nil {
			t.Fatalf("expected idle Ping timeout exit well before parent ctx, got err=%v ctxErr=%v elapsed=%s", err, ctx.Err(), elapsed)
		}
	})
}

func TestKlineClosed_RejectsInvalidOHLCAndNegativeTBQ(t *testing.T) {
	var accepted atomic.Int32
	mgr := NewStreamManager(EventHandlers{
		OnKlineClosed: func(symbol string, candle model.Candle, isClosed bool) {
			accepted.Add(1)
		},
	})

	// 1. M7 invalid OHLC: High (151.0) < Open (152.0)
	invalidOHLC := []byte(`{"e":"kline","E":1700000000000,"s":"SOLUSDT","k":{"t":1700000000000,"T":1700000300000,"s":"SOLUSDT","i":"5m","f":1,"L":10,"o":"152.0","c":"150.5","h":"151.0","l":"149.0","v":"1000.0","n":50,"x":true,"q":"151000.0","V":"600.0","Q":"90600.0","B":"0"}}`)
	mgr.ProcessMessage(invalidOHLC)

	// 2. Negative TakerBuyQuoteUSDT
	negativeTBQ := []byte(`{"e":"kline","E":1700000000000,"s":"SOLUSDT","k":{"t":1700000000000,"T":1700000300000,"s":"SOLUSDT","i":"5m","f":1,"L":10,"o":"150.0","c":"152.0","h":"153.0","l":"149.0","v":"1000.0","n":50,"x":true,"q":"151000.0","V":"600.0","Q":"-10.0","B":"0"}}`)
	mgr.ProcessMessage(negativeTBQ)

	// 3. +Inf float value
	infPrice := []byte(`{"e":"kline","E":1700000000000,"s":"SOLUSDT","k":{"t":1700000000000,"T":1700000300000,"s":"SOLUSDT","i":"5m","f":1,"L":10,"o":"150.0","c":"152.0","h":"+Inf","l":"149.0","v":"1000.0","n":50,"x":true,"q":"151000.0","V":"600.0","Q":"90600.0","B":"0"}}`)
	mgr.ProcessMessage(infPrice)

	if accepted.Load() != 0 {
		t.Fatalf("expected 0 invalid klines accepted, got %d", accepted.Load())
	}

	// 4. Valid OHLC should be accepted
	validKline := []byte(`{"e":"kline","E":1700000000000,"s":"SOLUSDT","k":{"t":1700000000000,"T":1700000300000,"s":"SOLUSDT","i":"5m","f":1,"L":10,"o":"150.0","c":"152.0","h":"153.0","l":"149.0","v":"1000.0","n":50,"x":true,"q":"151000.0","V":"600.0","Q":"90600.0","B":"0"}}`)
	mgr.ProcessMessage(validKline)
	if accepted.Load() != 1 {
		t.Fatalf("expected 1 valid kline accepted, got %d", accepted.Load())
	}
}

func TestWebSocketDetailedUserDataEvents(t *testing.T) {
	var gotOrderEvt model.OrderTradeUpdateEvent
	var gotAcctEvt model.AccountUpdateEvent
	var orderCalled, acctCalled atomic.Int32

	mgr := NewStreamManager(EventHandlers{
		OnOrderTradeUpdateDetailed: func(evt model.OrderTradeUpdateEvent) {
			gotOrderEvt = evt
			orderCalled.Add(1)
		},
		OnAccountUpdateEvent: func(evt model.AccountUpdateEvent) {
			gotAcctEvt = evt
			acctCalled.Add(1)
		},
	})

	rawOTU := []byte(`{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"T":1700000000000,"o":{"s":"ETHUSDT","c":"ORD_DET_1","S":"SELL","o":"LIMIT","f":"GTX","q":"2.0","p":"3000.0","ap":"3000.0","sp":"0","x":"TRADE","X":"PARTIALLY_FILLED","i":888777,"l":"0.75","z":"0.75","L":"3000.0","N":"USDT","n":"0.45","T":1700000000000,"t":1,"b":"0","a":"0","m":true,"R":false,"wt":"CONTRACT_PRICE","ot":"LIMIT","ps":"BOTH","cp":false,"rp":"12.50","pP":false,"si":0,"ss":0}}`)
	mgr.ProcessMessage(rawOTU)

	if orderCalled.Load() != 1 {
		t.Fatalf("expected OnOrderTradeUpdateDetailed to fire once")
	}
	if !gotOrderEvt.HasFillMeta || !gotOrderEvt.IsMaker || gotOrderEvt.CommissionAsset != "USDT" {
		t.Fatalf("unexpected order metadata: %+v", gotOrderEvt)
	}
	if !gotOrderEvt.LastFilledQty.Equal(decimal.NewFromFloat(0.75)) || !gotOrderEvt.Commission.Equal(decimal.NewFromFloat(0.45)) || !gotOrderEvt.RealizedProfit.Equal(decimal.NewFromFloat(12.50)) {
		t.Fatalf("unexpected order quantities/commission: %+v", gotOrderEvt)
	}

	rawAU := []byte(`{"e":"ACCOUNT_UPDATE","E":1700000000000,"T":1700000000000,"a":{"m":"ORDER","B":[{"a":"USDT","wb":"500.0","cw":"420.0","bc":"0"}],"P":[{"s":"ETHUSDT","pa":"0.75","ep":"3000.0","cr":"0","up":"15.25","mt":"isolated","iw":"0","ps":"BOTH"}]}}`)
	mgr.ProcessMessage(rawAU)

	if acctCalled.Load() != 1 {
		t.Fatalf("expected OnAccountUpdateEvent to fire once")
	}
	if gotAcctEvt.IsFullSnapshot {
		t.Fatalf("expected WebSocket ACCOUNT_UPDATE IsFullSnapshot=false")
	}
	if !gotAcctEvt.WalletBalance.Equal(decimal.NewFromFloat(500.0)) || !gotAcctEvt.CrossWallet.Equal(decimal.NewFromFloat(420.0)) {
		t.Fatalf("unexpected balances: %+v", gotAcctEvt)
	}
	if pos, ok := gotAcctEvt.Positions["ETHUSDT"]; !ok || !pos.UnrealizedPnL.Equal(decimal.NewFromFloat(15.25)) {
		t.Fatalf("expected ETHUSDT position in account event, got %+v", gotAcctEvt.Positions)
	}
}

func TestStreamManager_HealthyIdlePingKeepsConnectionAlive(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		for {
			// Read loop lets coder/websocket respond to Pings with Pongs while sending 0 text frames
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	mgr := NewStreamManager(EventHandlers{})
	mgr.IdleTimeout = 120 * time.Millisecond
	mgr.PingInterval = 40 * time.Millisecond
	mgr.PingTimeout = 30 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 280*time.Millisecond)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	err = mgr.readLoop(ctx, c, time.Now())
	_ = c.CloseNow()
	if err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("expected idle user-data-style stream to survive >280ms via Pong liveness without idle watchdog disconnect, got err=%v ctxErr=%v", err, ctx.Err())
	}
}

func TestWebSocketAlgoUpdateAndExpiredInMatch(t *testing.T) {
	var algoEvt model.AlgoOrderUpdateEvent
	var orderEvt model.OrderTradeUpdateEvent
	var algoCalls, orderCalls atomic.Int32

	mgr := NewStreamManager(EventHandlers{
		OnAlgoOrderUpdate: func(evt model.AlgoOrderUpdateEvent) {
			algoEvt = evt
			algoCalls.Add(1)
		},
		OnOrderTradeUpdateDetailed: func(evt model.OrderTradeUpdateEvent) {
			orderEvt = evt
			orderCalls.Add(1)
		},
	})

	rawAlgo := []byte(`{"e":"ALGO_UPDATE","T":1750515742297,"E":1750515742303,"o":{"caid":"ALGO_SL_SOLUSDT_1","aid":918273,"at":"CONDITIONAL","o":"STOP_MARKET","s":"SOLUSDT","S":"SELL","sp":"95.5","X":"CANCELED"}}`)
	mgr.ProcessMessage(rawAlgo)
	if algoCalls.Load() != 1 || algoEvt.Symbol != "SOLUSDT" || algoEvt.AlgoID != 918273 || algoEvt.ClientAlgoID != "ALGO_SL_SOLUSDT_1" || algoEvt.AlgoStatus != "CANCELED" || !algoEvt.TriggerPrice.Equal(decimal.NewFromFloat(95.5)) {
		t.Fatalf("unexpected ALGO_UPDATE parse result (calls=%d): %+v", algoCalls.Load(), algoEvt)
	}

	rawSTP := []byte(`{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"T":1700000000000,"o":{"s":"BTCUSDT","c":"ENTRY_STP","X":"EXPIRED_IN_MATCH","i":445566,"l":"0","z":"0","ap":"0","L":"0","N":"USDT","n":"0","m":false,"rp":"0"}}`)
	mgr.ProcessMessage(rawSTP)
	if orderCalls.Load() != 1 || orderEvt.Status != model.StatusExpired {
		t.Fatalf("expected EXPIRED_IN_MATCH mapped to StatusExpired, got calls=%d status=%s", orderCalls.Load(), orderEvt.Status)
	}
}

func TestDynamicStream_ConcurrentUpdateStreamsDuringDialAndActiveSync(t *testing.T) {
	var subsMu sync.Mutex
	serverSubs := make(map[string]bool)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subsMu.Lock()
		for _, s := range strings.Split(r.URL.Query().Get("streams"), "/") {
			if s != "" {
				serverSubs[s] = true
			}
		}
		subsMu.Unlock()

		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

		for {
			_, raw, err := c.Read(r.Context())
			if err != nil {
				return
			}
			var msg struct {
				Method string   `json:"method"`
				Params []string `json:"params"`
			}
			if json.Unmarshal(raw, &msg) == nil {
				subsMu.Lock()
				for _, p := range msg.Params {
					if msg.Method == "SUBSCRIBE" {
						serverSubs[p] = true
					} else if msg.Method == "UNSUBSCRIBE" {
						delete(serverSubs, p)
					}
				}
				subsMu.Unlock()
			}
		}
	}))
	defer ts.Close()

	wsBase := "ws" + strings.TrimPrefix(ts.URL, "http") + "/stream"
	mgr := NewStreamManager(EventHandlers{})
	ds := NewDynamicStream(mgr, wsBase, []string{"btcusdt@bookTicker"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ds.Start(ctx)

	var updWg sync.WaitGroup
	sets := [][]string{
		{"btcusdt@bookTicker", "ethusdt@bookTicker"},
		{"btcusdt@bookTicker", "solusdt@bookTicker", "ethusdt@bookTicker"},
		{"btcusdt@bookTicker", "adausdt@bookTicker"},
	}
	for i := 0; i < 6; i++ {
		updWg.Add(1)
		target := sets[i%len(sets)]
		go func(st []string) {
			defer updWg.Done()
			_ = ds.UpdateStreams(ctx, st)
		}(target)
	}
	updWg.Wait()

	finalTarget := []string{"btcusdt@bookTicker", "xrpusdt@bookTicker", "dogeusdt@bookTicker"}
	if err := ds.UpdateStreams(ctx, finalTarget); err != nil {
		t.Fatalf("final UpdateStreams failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		subsMu.Lock()
		matched := len(serverSubs) == len(finalTarget)
		for _, st := range finalTarget {
			if !serverSubs[st] {
				matched = false
				break
			}
		}
		subsMu.Unlock()
		if matched {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}

	subsMu.Lock()
	defer subsMu.Unlock()
	t.Fatalf("server subscriptions %v did not converge to final target %v", serverSubs, finalTarget)
}
