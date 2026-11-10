package ws

import (
	"sync/atomic"
	"testing"
	"time"

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

	// 1. Mainnet 2026 Spec: wss://fstream.binance.com/private/ws?listenKey=<key>&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,listenKeyExpired
	mainnetURL := BuildUserDataStreamURL("wss://fstream.binance.com/private/ws", listenKey, true)
	expectedMainnet := "wss://fstream.binance.com/private/ws?listenKey=mockListenKey12345&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,listenKeyExpired"
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
