package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
	"trml/internal/model"
)

type EventHandlers struct {
	OnBookTicker       func(symbol string, bestBid, bestAsk float64, now time.Time)
	OnAggTrade         func(symbol string, price, qty float64, isBuyerMaker bool, now time.Time)
	OnMarkPrice        func(symbol string, markPrice, fundingRate float64, now time.Time)
	OnKline5m          func(symbol string, candle model.Candle, isClosed bool)
	OnKlineClosed      func(symbol string, candle model.Candle, isClosed bool)
	OnOrderTradeUpdate func(clientOrderID string, exchangeOrderID int64, symbol string, status model.OrderLifecycleStatus, cumFilledQty, avgPrice, realizedProfit decimal.Decimal)
	OnAccountUpdate    func(walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, eventTime time.Time)
	OnListenKeyExpired func()
}

type StreamManager struct {
	Handlers EventHandlers
}

func NewStreamManager(handlers EventHandlers) *StreamManager {
	return &StreamManager{Handlers: handlers}
}

// BuildCombinedStreamURL merangkai URL combined stream Binance Futures
func BuildCombinedStreamURL(baseURL string, streams []string) string {
	cleanBase := strings.TrimRight(baseURL, "/")
	if !strings.HasSuffix(cleanBase, "/stream") {
		cleanBase += "/stream"
	}
	if len(streams) == 0 {
		return cleanBase
	}
	return fmt.Sprintf("%s?streams=%s", cleanBase, strings.Join(streams, "/"))
}

// BuildUserDataStreamURL merangkai URL private user data stream sesuai spesifikasi Binance 2026 untuk Mainnet dan legacy path untuk Testnet
func BuildUserDataStreamURL(baseURL, listenKey string, isMainnet bool) string {
	cleanBase := strings.TrimRight(baseURL, "/")
	if isMainnet {
		return fmt.Sprintf("%s?listenKey=%s&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,listenKeyExpired", cleanBase, listenKey)
	}
	return fmt.Sprintf("%s/%s", cleanBase, listenKey)
}

// StartSupervisedStream menjalankan koneksi WebSocket dengan auto-reconnect backoff dan reset timer
func (m *StreamManager) StartSupervisedStream(ctx context.Context, streamURL string) {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			slog.Info("ws: supervised stream terminating", "url", streamURL)
			return
		default:
			startTime := time.Now()
			err := m.ConnectAndStream(ctx, streamURL)
			if time.Since(startTime) > 5*time.Second {
				// Reset backoff jika koneksi sempat terhubung & bertahan > 5 detik
				backoff = 1 * time.Second
			}
			if err != nil && ctx.Err() == nil {
				// R1: Tambahkan randomized jitter 0-30% pada backoff untuk mencegah thundering herd
				jitter := time.Duration(rand.Int64N(int64(backoff/3) + 1))
				waitDuration := backoff + jitter
				slog.Warn("ws: connection lost, reconnecting...", "url", streamURL, "backoff", backoff, "waitWithJitter", waitDuration, "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(waitDuration):
					backoff *= 2
					if backoff > 3*time.Second {
						backoff = 3 * time.Second
					}
				}
			}
		}
	}
}

func (m *StreamManager) readLoop(ctx context.Context, conn *websocket.Conn, connStart time.Time) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Idle watchdog: 60 detik batas maksimal tanpa frame sebelum socket dianggap mati
			readCtx, readCancel := context.WithTimeout(ctx, 60*time.Second)
			typ, msg, err := conn.Read(readCtx)
			readCancel()
			if err != nil {
				return fmt.Errorf("ws: read error (connAge=%s): %w", time.Since(connStart).Round(time.Millisecond), err)
			}
			if typ == websocket.MessageText {
				m.ProcessMessage(msg)
			}
		}
	}
}

// ConnectAndStream membuka koneksi WebSocket menggunakan dial timeout dan idle watchdog
func (m *StreamManager) ConnectAndStream(ctx context.Context, streamURL string) error {
	opts := &websocket.DialOptions{
		HTTPClient: http.DefaultClient,
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, streamURL, opts)
	dialCancel()
	if err != nil {
		return fmt.Errorf("ws: dial error for %s: %w", streamURL, err)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "closing") }()

	connStart := time.Now()
	slog.Info("ws: connected successfully", "url", streamURL)
	return m.readLoop(ctx, conn, connStart)
}

// DynamicStream mengelola koneksi WebSocket gabungan dengan pembaruan SUBSCRIBE/UNSUBSCRIBE diferensial
// tanpa memutus koneksi untuk simbol yang tetap berada di watchlist.
type DynamicStream struct {
	mgr     *StreamManager
	baseURL string

	mu      sync.Mutex
	sendMu  sync.Mutex
	streams []string
	conn    *websocket.Conn
	reqID   atomic.Int64
}

func NewDynamicStream(mgr *StreamManager, baseURL string, initialStreams []string) *DynamicStream {
	copied := make([]string, len(initialStreams))
	copy(copied, initialStreams)
	return &DynamicStream{
		mgr:     mgr,
		baseURL: baseURL,
		streams: copied,
	}
}

func diffStreams(current, target []string) (added, removed []string) {
	curSet := make(map[string]bool, len(current))
	for _, s := range current {
		curSet[s] = true
	}
	tgtSet := make(map[string]bool, len(target))
	for _, s := range target {
		tgtSet[s] = true
		if !curSet[s] {
			added = append(added, s)
		}
	}
	for _, s := range current {
		if !tgtSet[s] {
			removed = append(removed, s)
		}
	}
	return added, removed
}

// Start menjalankan supervisor loop reconnect dengan backoff dan jitter serta memakai daftar stream terbaru saat reconnect.
func (d *DynamicStream) Start(ctx context.Context) {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			slog.Info("ws: dynamic supervised stream terminating", "baseURL", d.baseURL)
			return
		default:
			startTime := time.Now()
			err := d.connectAndStreamOnce(ctx)
			if time.Since(startTime) > 5*time.Second {
				backoff = 1 * time.Second
			}
			if err != nil && ctx.Err() == nil {
				jitter := time.Duration(rand.Int64N(int64(backoff/3) + 1))
				waitDuration := backoff + jitter
				d.mu.Lock()
				nStreams := len(d.streams)
				d.mu.Unlock()
				slog.Warn("ws: dynamic connection lost, reconnecting...",
					"baseURL", d.baseURL, "streams", nStreams, "connAge", time.Since(startTime).Round(time.Millisecond),
					"backoff", backoff, "waitWithJitter", waitDuration, "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(waitDuration):
					backoff *= 2
					if backoff > 3*time.Second {
						backoff = 3 * time.Second
					}
				}
			}
		}
	}
}

func (d *DynamicStream) connectAndStreamOnce(ctx context.Context) error {
	d.mu.Lock()
	dialed := make([]string, len(d.streams))
	copy(dialed, d.streams)
	d.mu.Unlock()

	streamURL := BuildCombinedStreamURL(d.baseURL, dialed)
	opts := &websocket.DialOptions{
		HTTPClient: http.DefaultClient,
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, streamURL, opts)
	dialCancel()
	if err != nil {
		return fmt.Errorf("ws: dial error for %s: %w", streamURL, err)
	}
	defer func() {
		d.mu.Lock()
		if d.conn == conn {
			d.conn = nil
		}
		d.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "closing")
	}()

	d.mu.Lock()
	d.conn = conn
	latest := make([]string, len(d.streams))
	copy(latest, d.streams)
	d.mu.Unlock()

	// Sinkronkan diff bila UpdateStreams terjadi selama proses dial
	added, removed := diffStreams(dialed, latest)
	if len(removed) > 0 {
		_ = d.sendControlMsg(ctx, conn, "UNSUBSCRIBE", removed)
	}
	if len(added) > 0 {
		_ = d.sendControlMsg(ctx, conn, "SUBSCRIBE", added)
	}

	connStart := time.Now()
	slog.Info("ws: dynamic stream connected", "baseURL", d.baseURL, "streamCount", len(latest))
	return d.mgr.readLoop(ctx, conn, connStart)
}

// UpdateStreams memperbarui langganan secara diferensial via SUBSCRIBE/UNSUBSCRIBE tanpa memutus soket aktif.
func (d *DynamicStream) UpdateStreams(ctx context.Context, newStreams []string) error {
	d.mu.Lock()
	added, removed := diffStreams(d.streams, newStreams)
	if len(added) == 0 && len(removed) == 0 {
		d.mu.Unlock()
		return nil
	}
	copied := make([]string, len(newStreams))
	copy(copied, newStreams)
	d.streams = copied
	conn := d.conn
	d.mu.Unlock()

	if conn == nil {
		return nil
	}

	if len(removed) > 0 {
		if err := d.sendControlMsg(ctx, conn, "UNSUBSCRIBE", removed); err != nil {
			return err
		}
	}
	if len(added) > 0 {
		if err := d.sendControlMsg(ctx, conn, "SUBSCRIBE", added); err != nil {
			return err
		}
	}
	slog.Info("ws: applied differential stream update", "baseURL", d.baseURL, "added", len(added), "removed", len(removed))
	return nil
}

func (d *DynamicStream) sendControlMsg(ctx context.Context, conn *websocket.Conn, method string, params []string) error {
	if len(params) == 0 {
		return nil
	}
	payload := struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int64    `json:"id"`
	}{
		Method: method,
		Params: params,
		ID:     d.reqID.Add(1),
	}
	raw, err := sonic.Marshal(payload)
	if err != nil {
		return err
	}

	d.sendMu.Lock()
	defer d.sendMu.Unlock()
	wCtx, wCancel := context.WithTimeout(ctx, 5*time.Second)
	defer wCancel()
	return conn.Write(wCtx, websocket.MessageText, raw)
}

// ProcessMessage membedah JSON payload dari Binance dengan penanganan parse error fail-closed (Solusi M4)
func (m *StreamManager) ProcessMessage(msg []byte) {
	now := time.Now().UTC()

	// 1. Cek Wrapper stream kombinasi (misal {"stream":"...","data":{...}})
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	payload := msg
	if err := sonic.Unmarshal(msg, &wrapper); err == nil && len(wrapper.Data) > 0 {
		payload = wrapper.Data
	}

	// 2. Cek event type "e"
	var baseEvent struct {
		EventType string `json:"e"`
		EventTime int64  `json:"E"`
		Symbol    string `json:"s"`
	}
	if err := sonic.Unmarshal(payload, &baseEvent); err != nil {
		return
	}

	switch baseEvent.EventType {
	case "bookTicker":
		var bk struct {
			Symbol  string `json:"s"`
			BestBid string `json:"b"`
			BidQty  string `json:"B"`
			BestAsk string `json:"a"`
			AskQty  string `json:"A"`
		}
		if err := sonic.Unmarshal(payload, &bk); err == nil && m.Handlers.OnBookTicker != nil {
			bid, err1 := strconv.ParseFloat(bk.BestBid, 64)
			ask, err2 := strconv.ParseFloat(bk.BestAsk, 64)
			if err1 == nil && err2 == nil && bid > 0 && ask > 0 && bid <= ask {
				m.Handlers.OnBookTicker(bk.Symbol, bid, ask, now)
			}
		}

	case "aggTrade":
		var at struct {
			Symbol       string `json:"s"`
			Price        string `json:"p"`
			Qty          string `json:"q"`
			IsBuyerMaker bool   `json:"m"`
		}
		if err := sonic.Unmarshal(payload, &at); err == nil && m.Handlers.OnAggTrade != nil {
			p, err1 := strconv.ParseFloat(at.Price, 64)
			q, err2 := strconv.ParseFloat(at.Qty, 64)
			if err1 == nil && err2 == nil && p > 0 && q > 0 {
				m.Handlers.OnAggTrade(at.Symbol, p, q, at.IsBuyerMaker, now)
			}
		}

	case "markPriceUpdate":
		var mp struct {
			Symbol      string `json:"s"`
			MarkPrice   string `json:"p"`
			IndexPrice  string `json:"P"`
			FundingRate string `json:"r"`
		}
		if err := sonic.Unmarshal(payload, &mp); err == nil && m.Handlers.OnMarkPrice != nil {
			p, err1 := strconv.ParseFloat(mp.MarkPrice, 64)
			r, err2 := strconv.ParseFloat(mp.FundingRate, 64)
			if err1 == nil && err2 == nil && p > 0 {
				m.Handlers.OnMarkPrice(mp.Symbol, p, r, now)
			}
		}

	case "kline":
		var ke struct {
			Symbol string `json:"s"`
			Kline  struct {
				OpenTime          int64  `json:"t"`
				CloseTime         int64  `json:"T"`
				Interval          string `json:"i"`
				FirstTradeID      int64  `json:"f"`
				LastTradeID       int64  `json:"L"`
				Open              string `json:"o"`
				High              string `json:"h"`
				Low               string `json:"l"`
				Close             string `json:"c"`
				VolumeBase        string `json:"v"`
				TakerBuyBaseVol   string `json:"V"`
				NumTrades         int64  `json:"n"`
				IsClosed          bool   `json:"x"`
				QuoteVolumeUSDT   string `json:"q"`
				TakerBuyQuoteUSDT string `json:"Q"`
			} `json:"k"`
		}
		hasKlineHandler := m.Handlers.OnKlineClosed != nil || m.Handlers.OnKline5m != nil
		if err := json.Unmarshal(payload, &ke); err == nil && hasKlineHandler {
			o, errO := strconv.ParseFloat(ke.Kline.Open, 64)
			h, errH := strconv.ParseFloat(ke.Kline.High, 64)
			l, errL := strconv.ParseFloat(ke.Kline.Low, 64)
			c, errC := strconv.ParseFloat(ke.Kline.Close, 64)
			vb, errV := strconv.ParseFloat(ke.Kline.VolumeBase, 64)
			qv, errQ := strconv.ParseFloat(ke.Kline.QuoteVolumeUSDT, 64)
			tbq, errT := strconv.ParseFloat(ke.Kline.TakerBuyQuoteUSDT, 64)

			if errO == nil && errH == nil && errL == nil && errC == nil && errV == nil && errQ == nil && errT == nil {
				if o > 0 && h > 0 && l > 0 && c > 0 && l <= h && vb >= 0 && qv >= 0 {
					candle := model.Candle{
						OpenTime:          time.UnixMilli(ke.Kline.OpenTime).UTC(),
						CloseTime:         time.UnixMilli(ke.Kline.CloseTime).UTC(),
						Open:              o,
						High:              h,
						Low:               l,
						Close:             c,
						VolumeBase:        vb,
						QuoteVolumeUSDT:   qv,
						TakerBuyQuoteUSDT: tbq,
					}
					if m.Handlers.OnKlineClosed != nil {
						m.Handlers.OnKlineClosed(ke.Symbol, candle, ke.Kline.IsClosed)
					} else if m.Handlers.OnKline5m != nil && (ke.Kline.Interval == "5m" || ke.Kline.Interval == "") {
						m.Handlers.OnKline5m(ke.Symbol, candle, ke.Kline.IsClosed)
					}
				}
			}
		}

	case "ORDER_TRADE_UPDATE":
		var otu struct {
			Order struct {
				Symbol          string `json:"s"`
				ClientOrderID   string `json:"c"`
				ExchangeOrderID int64  `json:"i"`
				Status          string `json:"X"`
				CumFilledQty    string `json:"z"`
				AvgPrice        string `json:"ap"`
				LastPrice       string `json:"L"`
				RealizedProfit  string `json:"rp"`
			} `json:"o"`
		}
		if err := sonic.Unmarshal(payload, &otu); err == nil && m.Handlers.OnOrderTradeUpdate != nil {
			cumQty, err := decimal.NewFromString(otu.Order.CumFilledQty)
			if err != nil {
				slog.Error("ws: failed to parse cumFilledQty in ORDER_TRADE_UPDATE", "raw", otu.Order.CumFilledQty, "err", err)
				cumQty = decimal.Zero
			}
			avgPrice, err := decimal.NewFromString(otu.Order.AvgPrice)
			if err != nil || avgPrice.IsZero() {
				avgPrice, _ = decimal.NewFromString(otu.Order.LastPrice)
			}
			realizedProfit, _ := decimal.NewFromString(otu.Order.RealizedProfit)

			var status model.OrderLifecycleStatus
			switch otu.Order.Status {
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
				status = model.OrderLifecycleStatus(otu.Order.Status)
			}
			m.Handlers.OnOrderTradeUpdate(otu.Order.ClientOrderID, otu.Order.ExchangeOrderID, otu.Order.Symbol, status, cumQty, avgPrice, realizedProfit)
		}

	case "ACCOUNT_UPDATE":
		var au struct {
			Account struct {
				Balances []struct {
					Asset         string `json:"a"`
					WalletBalance string `json:"wb"`
					CrossWallet   string `json:"cw"`
				} `json:"B"`
				Positions []struct {
					Symbol        string `json:"s"`
					PositionAmt   string `json:"pa"`
					EntryPrice    string `json:"ep"`
					UnrealizedPnL string `json:"up"`
				} `json:"P"`
			} `json:"a"`
		}
		if err := sonic.Unmarshal(payload, &au); err == nil && m.Handlers.OnAccountUpdate != nil {
			var walletBal, unRealizedTotal, availBal decimal.Decimal
			positions := make(map[string]model.LivePosition)

			for _, b := range au.Account.Balances {
				if b.Asset == "USDT" {
					if wb, err := decimal.NewFromString(b.WalletBalance); err == nil {
						walletBal = wb
					}
					if cw, err := decimal.NewFromString(b.CrossWallet); err == nil {
						availBal = cw
					}
				}
			}
			for _, p := range au.Account.Positions {
				amt, errAmt := decimal.NewFromString(p.PositionAmt)
				ep, errEP := decimal.NewFromString(p.EntryPrice)
				up, errUP := decimal.NewFromString(p.UnrealizedPnL)

				if errAmt == nil && errEP == nil && errUP == nil {
					unRealizedTotal = unRealizedTotal.Add(up)

					side := model.SideLong
					if amt.IsNegative() {
						side = model.SideShort
					}
					positions[p.Symbol] = model.LivePosition{
						Symbol:        p.Symbol,
						Side:          side,
						Qty:           amt.Abs(),
						EntryPrice:    ep,
						UnrealizedPnL: up,
					}
				}
			}

			eventTime := time.UnixMilli(baseEvent.EventTime).UTC()
			m.Handlers.OnAccountUpdate(walletBal, unRealizedTotal, availBal, positions, eventTime)
		}

	case "listenKeyExpired":
		slog.Warn("ws: listenKeyExpired event received from exchange")
		if m.Handlers.OnListenKeyExpired != nil {
			m.Handlers.OnListenKeyExpired()
		}

	default:
		// Jika format bookTicker payload raw tanpa "e": "bookTicker" (format alternatif bursa)
		if strings.Contains(string(payload), `"b":`) && strings.Contains(string(payload), `"a":`) && baseEvent.Symbol != "" {
			var bk struct {
				Symbol  string `json:"s"`
				BestBid string `json:"b"`
				BestAsk string `json:"a"`
			}
			if err := sonic.Unmarshal(payload, &bk); err == nil && m.Handlers.OnBookTicker != nil {
				bid, err1 := strconv.ParseFloat(bk.BestBid, 64)
				ask, err2 := strconv.ParseFloat(bk.BestAsk, 64)
				if err1 == nil && err2 == nil && bid > 0 && ask > 0 && bid <= ask {
					m.Handlers.OnBookTicker(bk.Symbol, bid, ask, now)
				}
			}
		}
	}
}
