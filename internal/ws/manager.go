package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
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
	OnBookTicker               func(symbol string, bestBid, bestAsk float64, now time.Time)
	OnAggTrade                 func(symbol string, price, qty float64, isBuyerMaker bool, now time.Time)
	OnMarkPrice                func(symbol string, markPrice, fundingRate float64, now time.Time)
	OnKline5m                  func(symbol string, candle model.Candle, isClosed bool)
	OnKlineClosed              func(symbol string, candle model.Candle, isClosed bool)
	OnOrderTradeUpdate         func(clientOrderID string, exchangeOrderID int64, symbol string, status model.OrderLifecycleStatus, cumFilledQty, avgPrice, realizedProfit decimal.Decimal)
	OnOrderTradeUpdateDetailed func(evt model.OrderTradeUpdateEvent)
	OnAccountUpdate            func(walletBal, unrealizedPnL, availBal decimal.Decimal, positions map[string]model.LivePosition, eventTime time.Time)
	OnAccountUpdateEvent       func(evt model.AccountUpdateEvent)
	OnAlgoOrderUpdate          func(evt model.AlgoOrderUpdateEvent)
	OnConnected                func()
	OnListenKeyExpired         func()
}

type StreamManager struct {
	Handlers                   EventHandlers
	IdleTimeout                time.Duration
	PingInterval               time.Duration
	PingTimeout                time.Duration
	MaxConsecutiveDialFailures int
	NowFn                      func() time.Time
}

const (
	defaultWSIdleTimeout  = 60 * time.Second
	defaultWSPingInterval = 25 * time.Second
	defaultWSPingTimeout  = 10 * time.Second
)

func isInvalidWSFloat(v float64) bool {
	return math.IsNaN(v) || math.IsInf(v, 0)
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
		return fmt.Sprintf("%s?listenKey=%s&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,ALGO_UPDATE,listenKeyExpired", cleanBase, listenKey)
	}
	return fmt.Sprintf("%s/%s", cleanBase, listenKey)
}

// StartSupervisedStream menjalankan koneksi WebSocket dengan auto-reconnect backoff dan reset timer
func (m *StreamManager) StartSupervisedStream(ctx context.Context, streamURL string) {
	backoff := 1 * time.Second
	consecutiveFailures := 0
	for {
		select {
		case <-ctx.Done():
			slog.Info("ws: supervised stream terminating", "url", streamURL)
			return
		default:
			startTime := time.Now()
			err := m.ConnectAndStream(ctx, streamURL)
			if time.Since(startTime) > 5*time.Second {
				// Reset backoff dan counter gagal jika koneksi sempat terhubung & bertahan > 5 detik
				backoff = 1 * time.Second
				consecutiveFailures = 0
			} else if err != nil && ctx.Err() == nil {
				consecutiveFailures++
				if m.MaxConsecutiveDialFailures > 0 && consecutiveFailures >= m.MaxConsecutiveDialFailures {
					slog.Warn("ws: reached max consecutive supervised stream failures; yielding to caller for session reset",
						"url", streamURL, "failures", consecutiveFailures, "err", err)
					return
				}
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
	return m.readLoopWithControl(ctx, conn, connStart, nil, nil, 0)
}

func (m *StreamManager) readLoopWithControl(
	ctx context.Context,
	conn *websocket.Conn,
	connStart time.Time,
	onControlFrame func([]byte) bool,
	onWatchdogTick func(now time.Time) bool,
	watchdogOverride time.Duration,
) error {
	idleTimeout := m.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultWSIdleTimeout
	}
	pingInterval := m.PingInterval
	if pingInterval <= 0 {
		pingInterval = defaultWSPingInterval
	}
	pingTimeout := m.PingTimeout
	if pingTimeout <= 0 {
		pingTimeout = defaultWSPingTimeout
	}

	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()

	var lastFrameNano atomic.Int64
	lastFrameNano.Store(time.Now().UnixNano())

	watchdogInterval := idleTimeout / 4
	if watchdogInterval < 20*time.Millisecond {
		watchdogInterval = 20 * time.Millisecond
	} else if watchdogInterval > 10*time.Second {
		watchdogInterval = 10 * time.Second
	}
	if watchdogOverride > 0 && watchdogOverride < watchdogInterval {
		watchdogInterval = watchdogOverride
		if watchdogInterval < 10*time.Millisecond {
			watchdogInterval = 10 * time.Millisecond
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchdogTicker := time.NewTicker(watchdogInterval)
		pingTicker := time.NewTicker(pingInterval)
		defer watchdogTicker.Stop()
		defer pingTicker.Stop()

		for {
			select {
			case <-readCtx.Done():
				return
			case <-watchdogTicker.C:
				now := time.Now()
				lastAt := time.Unix(0, lastFrameNano.Load())
				if now.Sub(lastAt) > idleTimeout {
					slog.Warn("ws: idle watchdog timeout triggered",
						"connAge", time.Since(connStart).Round(time.Millisecond),
						"idleFor", now.Sub(lastAt).Round(time.Millisecond))
					readCancel()
					return
				}
				if onWatchdogTick != nil && onWatchdogTick(now) {
					readCancel()
					return
				}
			case <-pingTicker.C:
				lastAt := time.Unix(0, lastFrameNano.Load())
				// Kirim Ping hanya jika koneksi memang idle >= pingInterval agar stream sibuk tidak terputus palsu oleh antrean Pong
				if time.Since(lastAt) < pingInterval {
					continue
				}
				pCtx, pCancel := context.WithTimeout(readCtx, pingTimeout)
				err := conn.Ping(pCtx)
				pCancel()
				if err == nil {
					lastFrameNano.Store(time.Now().UnixNano())
				} else if readCtx.Err() == nil && time.Since(time.Unix(0, lastFrameNano.Load())) >= pingInterval {
					slog.Debug("ws: idle ping failed, resetting connection", "err", err)
					readCancel()
					return
				}
			}
		}
	}()
	defer wg.Wait()

	for {
		typ, msg, err := conn.Read(readCtx)
		if err != nil {
			readCancel()
			return fmt.Errorf("ws: read error (connAge=%s): %w", time.Since(connStart).Round(time.Millisecond), err)
		}
		lastFrameNano.Store(time.Now().UnixNano())
		if typ == websocket.MessageText {
			if onControlFrame != nil && onControlFrame(msg) {
				continue
			}
			m.ProcessMessage(msg)
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
	defer func() { _ = conn.CloseNow() }()

	connStart := time.Now()
	slog.Info("ws: connected successfully", "url", streamURL)
	if m.Handlers.OnConnected != nil {
		m.Handlers.OnConnected()
	}
	return m.readLoop(ctx, conn, connStart)
}

const defaultWSControlAckTimeout = 15 * time.Second

type pendingControlReq struct {
	id     int64
	method string
	params []string
	sentAt time.Time
}

// DynamicStream mengelola koneksi WebSocket gabungan dengan pembaruan SUBSCRIBE/UNSUBSCRIBE diferensial
// tanpa memutus koneksi untuk simbol yang tetap berada di watchlist.
type DynamicStream struct {
	mgr               *StreamManager
	baseURL           string
	ControlAckTimeout time.Duration

	sendMu        sync.Mutex // Strict lock order: sendMu -> mu
	mu            sync.Mutex
	streams       []string
	conn          *websocket.Conn
	syncedConn    *websocket.Conn
	syncedStreams []string
	pendingReqs   map[int64]pendingControlReq
	connCancel    context.CancelFunc
	connAborted   bool
	reqID         atomic.Int64
}

func NewDynamicStream(mgr *StreamManager, baseURL string, initialStreams []string) *DynamicStream {
	copied := make([]string, len(initialStreams))
	copy(copied, initialStreams)
	return &DynamicStream{
		mgr:         mgr,
		baseURL:     baseURL,
		streams:     copied,
		pendingReqs: make(map[int64]pendingControlReq),
	}
}

func (d *DynamicStream) effectiveControlAckTimeout() time.Duration {
	if d.ControlAckTimeout > 0 {
		return d.ControlAckTimeout
	}
	return defaultWSControlAckTimeout
}

func removeStreamsFromSlice(cur []string, toRemove []string) []string {
	removeSet := make(map[string]bool, len(toRemove))
	for _, s := range toRemove {
		removeSet[s] = true
	}
	kept := make([]string, 0, len(cur))
	for _, s := range cur {
		if !removeSet[s] {
			kept = append(kept, s)
		}
	}
	return kept
}

func addStreamsToSlice(cur []string, toAdd []string) []string {
	existing := make(map[string]bool, len(cur))
	out := make([]string, 0, len(cur)+len(toAdd))
	for _, s := range cur {
		if !existing[s] {
			existing[s] = true
			out = append(out, s)
		}
	}
	for _, s := range toAdd {
		if !existing[s] {
			existing[s] = true
			out = append(out, s)
		}
	}
	return out
}

func (d *DynamicStream) rollbackControlReqLocked(req pendingControlReq) {
	switch req.method {
	case "SUBSCRIBE":
		d.syncedStreams = removeStreamsFromSlice(d.syncedStreams, req.params)
	case "UNSUBSCRIBE":
		d.syncedStreams = addStreamsToSlice(d.syncedStreams, req.params)
	default:
		d.syncedStreams = nil
	}
}

func (d *DynamicStream) handleControlFrame(raw []byte) bool {
	var envelope struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
		Event  string          `json:"e"`
		ID     *int64          `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		} `json:"error"`
		Code *int   `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := sonic.Unmarshal(raw, &envelope); err != nil {
		return false
	}
	if envelope.Stream != "" || len(envelope.Data) > 0 || envelope.Event != "" {
		return false
	}
	hasErr := envelope.Error != nil || (envelope.Code != nil && *envelope.Code != 0)
	if envelope.ID == nil && !hasErr && len(envelope.Result) == 0 {
		return false
	}

	d.mu.Lock()
	var req pendingControlReq
	var found bool
	var reqID int64
	if envelope.ID != nil {
		reqID = *envelope.ID
		req, found = d.pendingReqs[reqID]
		if found {
			delete(d.pendingReqs, reqID)
		}
	}
	if !hasErr {
		d.mu.Unlock()
		return true
	}

	errCode := 0
	errMsg := envelope.Msg
	if envelope.Error != nil {
		errCode = envelope.Error.Code
		errMsg = envelope.Error.Msg
	} else if envelope.Code != nil {
		errCode = *envelope.Code
	}
	if found {
		d.rollbackControlReqLocked(req)
	} else {
		d.syncedStreams = nil
	}
	d.connAborted = true
	cancel := d.connCancel
	d.mu.Unlock()

	slog.Warn("ws: control request rejected by server; rolling back synced streams and reconnecting",
		"baseURL", d.baseURL, "reqID", reqID, "method", req.method, "code", errCode, "msg", errMsg)
	if cancel != nil {
		cancel()
	}
	return true
}

func (d *DynamicStream) checkControlAckTimeout(now time.Time) bool {
	timeout := d.effectiveControlAckTimeout()
	d.mu.Lock()
	if len(d.pendingReqs) == 0 {
		d.mu.Unlock()
		return false
	}
	var expiredIDs []int64
	for id, req := range d.pendingReqs {
		if now.Sub(req.sentAt) > timeout {
			expiredIDs = append(expiredIDs, id)
		}
	}
	if len(expiredIDs) == 0 {
		d.mu.Unlock()
		return false
	}
	for _, id := range expiredIDs {
		req := d.pendingReqs[id]
		d.rollbackControlReqLocked(req)
		delete(d.pendingReqs, id)
	}
	d.connAborted = true
	d.mu.Unlock()

	slog.Warn("ws: control ACK timeout exceeded; forcing dynamic stream reconnect",
		"baseURL", d.baseURL, "timeout", timeout, "expiredRequests", len(expiredIDs))
	return true
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
	const initialBackoff = 300 * time.Millisecond
	backoff := initialBackoff
	for {
		select {
		case <-ctx.Done():
			slog.Info("ws: dynamic supervised stream terminating", "baseURL", d.baseURL)
			return
		default:
			startTime := time.Now()
			err := d.connectAndStreamOnce(ctx)
			if time.Since(startTime) > 5*time.Second {
				backoff = initialBackoff
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
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	defer func() {
		d.sendMu.Lock()
		d.mu.Lock()
		if d.conn == conn {
			d.conn = nil
			d.connCancel = nil
		}
		if d.syncedConn == conn {
			d.syncedConn = nil
			d.syncedStreams = nil
			d.pendingReqs = make(map[int64]pendingControlReq)
			d.connAborted = false
		}
		d.mu.Unlock()
		d.sendMu.Unlock()
		_ = conn.CloseNow()
	}()

	d.sendMu.Lock()
	d.mu.Lock()
	d.conn = conn
	d.connCancel = connCancel
	d.syncedConn = conn
	d.syncedStreams = append([]string(nil), dialed...)
	d.pendingReqs = make(map[int64]pendingControlReq)
	d.connAborted = false
	latest := make([]string, len(d.streams))
	copy(latest, d.streams)
	d.mu.Unlock()
	_, _, syncErr := d.syncConnStreamsLocked(connCtx, conn, latest)
	d.sendMu.Unlock()
	if syncErr != nil {
		return fmt.Errorf("ws: failed to sync initial stream diff: %w", syncErr)
	}

	connStart := time.Now()
	slog.Info("ws: dynamic stream connected", "baseURL", d.baseURL, "streamCount", len(latest))
	ackWatchInterval := d.effectiveControlAckTimeout() / 4
	return d.mgr.readLoopWithControl(connCtx, conn, connStart, d.handleControlFrame, d.checkControlAckTimeout, ackWatchInterval)
}

// UpdateStreams memperbarui langganan secara diferensial via SUBSCRIBE/UNSUBSCRIBE tanpa memutus soket aktif.
func (d *DynamicStream) UpdateStreams(ctx context.Context, newStreams []string) error {
	d.mu.Lock()
	addedTarget, removedTarget := diffStreams(d.streams, newStreams)
	needResync := d.conn != nil && d.syncedConn == d.conn
	if needResync {
		addedSynced, removedSynced := diffStreams(d.syncedStreams, newStreams)
		needResync = len(addedSynced) > 0 || len(removedSynced) > 0
	}
	if len(addedTarget) == 0 && len(removedTarget) == 0 && !needResync {
		d.mu.Unlock()
		return nil
	}
	copied := make([]string, len(newStreams))
	copy(copied, newStreams)
	d.streams = copied
	d.mu.Unlock()

	d.sendMu.Lock()
	defer d.sendMu.Unlock()

	d.mu.Lock()
	conn := d.conn
	synced := d.syncedConn
	target := make([]string, len(d.streams))
	copy(target, d.streams)
	d.mu.Unlock()

	if conn == nil || synced != conn {
		return nil
	}

	addedN, removedN, err := d.syncConnStreamsLocked(ctx, conn, target)
	if err != nil {
		return err
	}
	if addedN > 0 || removedN > 0 {
		slog.Info("ws: applied differential stream update", "baseURL", d.baseURL, "added", addedN, "removed", removedN)
	}
	return nil
}

func (d *DynamicStream) syncConnStreamsLocked(ctx context.Context, conn *websocket.Conn, target []string) (int, int, error) {
	d.mu.Lock()
	curSynced := make([]string, len(d.syncedStreams))
	copy(curSynced, d.syncedStreams)
	d.mu.Unlock()

	added, removed := diffStreams(curSynced, target)
	if len(removed) > 0 {
		if err := d.writeControlMsgLocked(ctx, conn, "UNSUBSCRIBE", removed); err != nil {
			return 0, 0, err
		}
		d.mu.Lock()
		if d.syncedConn == conn && !d.connAborted {
			d.syncedStreams = removeStreamsFromSlice(d.syncedStreams, removed)
		}
		d.mu.Unlock()
	}
	if len(added) > 0 {
		if err := d.writeControlMsgLocked(ctx, conn, "SUBSCRIBE", added); err != nil {
			return 0, len(removed), err
		}
		d.mu.Lock()
		if d.syncedConn == conn && !d.connAborted {
			d.syncedStreams = addStreamsToSlice(d.syncedStreams, added)
		}
		d.mu.Unlock()
	}
	return len(added), len(removed), nil
}

func (d *DynamicStream) sendControlMsg(ctx context.Context, conn *websocket.Conn, method string, params []string) error {
	d.sendMu.Lock()
	defer d.sendMu.Unlock()
	return d.writeControlMsgLocked(ctx, conn, method, params)
}

func (d *DynamicStream) writeControlMsgLocked(ctx context.Context, conn *websocket.Conn, method string, params []string) error {
	if len(params) == 0 {
		return nil
	}
	id := d.reqID.Add(1)
	payload := struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int64    `json:"id"`
	}{
		Method: method,
		Params: params,
		ID:     id,
	}
	raw, err := sonic.Marshal(payload)
	if err != nil {
		return err
	}

	paramsCopy := make([]string, len(params))
	copy(paramsCopy, params)
	d.mu.Lock()
	if d.pendingReqs == nil {
		d.pendingReqs = make(map[int64]pendingControlReq)
	}
	if d.syncedConn == conn {
		d.pendingReqs[id] = pendingControlReq{
			id:     id,
			method: method,
			params: paramsCopy,
			sentAt: time.Now(),
		}
	}
	d.mu.Unlock()

	wCtx, wCancel := context.WithTimeout(ctx, 5*time.Second)
	defer wCancel()
	if err := conn.Write(wCtx, websocket.MessageText, raw); err != nil {
		d.mu.Lock()
		delete(d.pendingReqs, id)
		d.mu.Unlock()
		return err
	}
	return nil
}

type streamShard struct {
	ds      *DynamicStream
	cancel  context.CancelFunc
	streams []string
}

// ShardedDynamicStream memecah daftar stream berfrekuensi tinggi (mis. @bookTicker) menjadi beberapa koneksi
// DynamicStream dengan ukuran maksimal maxPerShard per soket agar tidak melewati batas throughput bursa.
type ShardedDynamicStream struct {
	mgr         *StreamManager
	baseURL     string
	maxPerShard int

	mu      sync.Mutex
	ctx     context.Context
	started bool
	wg      sync.WaitGroup
	shards  []*streamShard
}

func NewShardedDynamicStream(mgr *StreamManager, baseURL string, initialStreams []string, maxPerShard int) *ShardedDynamicStream {
	chunks := chunkStreams(initialStreams, maxPerShard)
	shards := make([]*streamShard, 0, len(chunks))
	for _, ch := range chunks {
		copied := make([]string, len(ch))
		copy(copied, ch)
		shards = append(shards, &streamShard{
			ds:      NewDynamicStream(mgr, baseURL, copied),
			streams: copied,
		})
	}
	return &ShardedDynamicStream{
		mgr:         mgr,
		baseURL:     baseURL,
		maxPerShard: maxPerShard,
		shards:      shards,
	}
}

func chunkStreams(streams []string, maxPerShard int) [][]string {
	if len(streams) == 0 {
		return nil
	}
	if maxPerShard <= 0 || len(streams) <= maxPerShard {
		return [][]string{streams}
	}
	chunks := make([][]string, 0, (len(streams)+maxPerShard-1)/maxPerShard)
	for start := 0; start < len(streams); start += maxPerShard {
		end := start + maxPerShard
		if end > len(streams) {
			end = len(streams)
		}
		chunks = append(chunks, streams[start:end])
	}
	return chunks
}

func (s *ShardedDynamicStream) ShardCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.shards)
}

func (s *ShardedDynamicStream) Start(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.started = true
	for _, sh := range s.shards {
		if sh.cancel == nil && len(sh.streams) > 0 {
			shCtx, shCancel := context.WithCancel(ctx)
			sh.cancel = shCancel
			ds := sh.ds
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				ds.Start(shCtx)
			}()
		}
	}
	s.mu.Unlock()

	<-ctx.Done()

	s.mu.Lock()
	for _, sh := range s.shards {
		if sh.cancel != nil {
			sh.cancel()
			sh.cancel = nil
		}
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// UpdateStreams memperbarui langganan lintas shard sambil mempertahankan stream lama di shard asalnya
// sehingga simbol yang tetap aktif tidak terkena pemutusan atau pindah koneksi.
func (s *ShardedDynamicStream) UpdateStreams(ctx context.Context, newStreams []string) error {
	targetSet := make(map[string]bool, len(newStreams))
	uniqueTargets := make([]string, 0, len(newStreams))
	for _, st := range newStreams {
		if !targetSet[st] {
			targetSet[st] = true
			uniqueTargets = append(uniqueTargets, st)
		}
	}

	s.mu.Lock()
	existingOwner := make(map[string]bool, len(uniqueTargets))
	updatedSlices := make([][]string, len(s.shards))
	for i, sh := range s.shards {
		kept := make([]string, 0, len(sh.streams))
		for _, st := range sh.streams {
			if targetSet[st] && !existingOwner[st] {
				if s.maxPerShard <= 0 || len(kept) < s.maxPerShard {
					kept = append(kept, st)
					existingOwner[st] = true
				}
			}
		}
		updatedSlices[i] = kept
	}

	pendingAdds := make([]string, 0)
	for _, st := range uniqueTargets {
		if !existingOwner[st] {
			pendingAdds = append(pendingAdds, st)
		}
	}

	type newShardSpec struct {
		idx int
	}
	var newShardsToStart []newShardSpec

	for _, st := range pendingAdds {
		placed := false
		for i := range updatedSlices {
			if len(updatedSlices[i]) > 0 && (s.maxPerShard <= 0 || len(updatedSlices[i]) < s.maxPerShard) {
				updatedSlices[i] = append(updatedSlices[i], st)
				placed = true
				break
			}
		}
		if !placed {
			for i := range updatedSlices {
				if len(updatedSlices[i]) == 0 {
					updatedSlices[i] = append(updatedSlices[i], st)
					placed = true
					break
				}
			}
		}
		if !placed {
			idx := len(s.shards)
			s.shards = append(s.shards, &streamShard{})
			updatedSlices = append(updatedSlices, []string{st})
			newShardsToStart = append(newShardsToStart, newShardSpec{idx: idx})
		}
	}

	isNewIdx := make(map[int]bool, len(newShardsToStart))
	for _, spec := range newShardsToStart {
		isNewIdx[spec.idx] = true
	}

	type shardUpdateJob struct {
		ds     *DynamicStream
		target []string
	}
	var jobs []shardUpdateJob
	keptShards := make([]*streamShard, 0, len(s.shards))
	for i, sh := range s.shards {
		nextSlice := updatedSlices[i]
		if len(nextSlice) == 0 {
			if sh.cancel != nil {
				sh.cancel()
				sh.cancel = nil
			}
			continue
		}
		sh.streams = nextSlice
		if isNewIdx[i] {
			sh.ds = NewDynamicStream(s.mgr, s.baseURL, nextSlice)
			if s.started && s.ctx != nil && s.ctx.Err() == nil {
				shCtx, shCancel := context.WithCancel(s.ctx)
				sh.cancel = shCancel
				ds := sh.ds
				s.wg.Add(1)
				go func() {
					defer s.wg.Done()
					ds.Start(shCtx)
				}()
			}
		} else {
			jobs = append(jobs, shardUpdateJob{ds: sh.ds, target: nextSlice})
		}
		keptShards = append(keptShards, sh)
	}
	s.shards = keptShards
	s.mu.Unlock()

	var firstErr error
	for _, j := range jobs {
		if err := j.ds.UpdateStreams(ctx, j.target); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ProcessMessage membedah JSON payload dari Binance dengan penanganan parse error fail-closed (Solusi M4)
func (m *StreamManager) ProcessMessage(msg []byte) {
	now := time.Now().UTC()
	if m.NowFn != nil {
		now = m.NowFn()
	}

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
				validFloats := !isInvalidWSFloat(o) && !isInvalidWSFloat(h) && !isInvalidWSFloat(l) && !isInvalidWSFloat(c) &&
					!isInvalidWSFloat(vb) && !isInvalidWSFloat(qv) && !isInvalidWSFloat(tbq)
				validOHLC := o > 0 && h > 0 && l > 0 && c > 0 && l <= h &&
					h >= math.Max(o, c) && l <= math.Min(o, c) &&
					vb >= 0 && qv >= 0 && tbq >= 0
				if validFloats && validOHLC {
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
				Side            string `json:"S"`
				ClientOrderID   string `json:"c"`
				ExchangeOrderID int64  `json:"i"`
				Status          string `json:"X"`
				CumFilledQty    string `json:"z"`
				LastFilledQty   string `json:"l"`
				AvgPrice        string `json:"ap"`
				LastPrice       string `json:"L"`
				RealizedProfit  string `json:"rp"`
				Commission      string `json:"n"`
				CommissionAsset string `json:"N"`
				IsMaker         bool   `json:"m"`
			} `json:"o"`
		}
		if err := sonic.Unmarshal(payload, &otu); err == nil && (m.Handlers.OnOrderTradeUpdate != nil || m.Handlers.OnOrderTradeUpdateDetailed != nil) {
			cumQty, err := decimal.NewFromString(otu.Order.CumFilledQty)
			if err != nil {
				slog.Error("ws: failed to parse cumFilledQty in ORDER_TRADE_UPDATE", "raw", otu.Order.CumFilledQty, "err", err)
				cumQty = decimal.Zero
			}
			lastQty, err := decimal.NewFromString(otu.Order.LastFilledQty)
			if err != nil {
				lastQty = decimal.Zero
			}
			avgPrice, err := decimal.NewFromString(otu.Order.AvgPrice)
			if err != nil || avgPrice.IsZero() {
				avgPrice, _ = decimal.NewFromString(otu.Order.LastPrice)
			}
			realizedProfit, _ := decimal.NewFromString(otu.Order.RealizedProfit)
			commission, _ := decimal.NewFromString(otu.Order.Commission)

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
			case "EXPIRED", "EXPIRED_IN_MATCH":
				status = model.StatusExpired
			default:
				status = model.OrderLifecycleStatus(otu.Order.Status)
			}
			if m.Handlers.OnOrderTradeUpdateDetailed != nil {
				m.Handlers.OnOrderTradeUpdateDetailed(model.OrderTradeUpdateEvent{
					ClientOrderID:   otu.Order.ClientOrderID,
					ExchangeOrderID: otu.Order.ExchangeOrderID,
					Symbol:          otu.Order.Symbol,
					Status:          status,
					CumFilledQty:    cumQty,
					LastFilledQty:   lastQty,
					AvgPrice:        avgPrice,
					RealizedProfit:  realizedProfit,
					Commission:      commission,
					CommissionAsset: otu.Order.CommissionAsset,
					IsMaker:         otu.Order.IsMaker,
					HasFillMeta:     true,
				})
			}
			if m.Handlers.OnOrderTradeUpdate != nil {
				m.Handlers.OnOrderTradeUpdate(otu.Order.ClientOrderID, otu.Order.ExchangeOrderID, otu.Order.Symbol, status, cumQty, avgPrice, realizedProfit)
			}
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
		if err := sonic.Unmarshal(payload, &au); err == nil && (m.Handlers.OnAccountUpdate != nil || m.Handlers.OnAccountUpdateEvent != nil) {
			var walletBal, unRealizedTotal, crossWallet decimal.Decimal
			positions := make(map[string]model.LivePosition)

			for _, b := range au.Account.Balances {
				if b.Asset == "USDT" {
					if wb, err := decimal.NewFromString(b.WalletBalance); err == nil {
						walletBal = wb
					}
					if cw, err := decimal.NewFromString(b.CrossWallet); err == nil {
						crossWallet = cw
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

			// Gunakan timestamp penerimaan lokal UTC agar tidak terjadi false stale rejection akibat clock skew jam bursa vs jam lokal
			eventTime := time.Now().UTC()
			if m.Handlers.OnAccountUpdateEvent != nil {
				m.Handlers.OnAccountUpdateEvent(model.AccountUpdateEvent{
					WalletBalance:  walletBal,
					CrossWallet:    crossWallet,
					UnrealizedPnL:  unRealizedTotal,
					Positions:      positions,
					EventTime:      eventTime,
					IsFullSnapshot: false,
				})
			}
			if m.Handlers.OnAccountUpdate != nil {
				m.Handlers.OnAccountUpdate(walletBal, unRealizedTotal, crossWallet, positions, eventTime)
			}
		}

	case "ALGO_UPDATE":
		type algoPayload struct {
			Symbol       string `json:"s"`
			Side         string `json:"S"`
			AlgoID       int64  `json:"aid"`
			ClientAlgoID string `json:"caid"`
			OrderType    string `json:"o"`
			AlgoStatus   string `json:"X"`
			StopPrice    string `json:"sp"`
			TriggerPrice string `json:"tp"`
		}
		var au struct {
			Order    *algoPayload `json:"o"`
			AltOrder *algoPayload `json:"ao"`
		}
		if err := sonic.Unmarshal(payload, &au); err == nil && m.Handlers.OnAlgoOrderUpdate != nil {
			inner := au.Order
			if inner == nil {
				inner = au.AltOrder
			}
			if inner != nil && inner.Symbol != "" {
				trig := decimal.Zero
				if inner.StopPrice != "" {
					trig, _ = decimal.NewFromString(inner.StopPrice)
				} else if inner.TriggerPrice != "" {
					trig, _ = decimal.NewFromString(inner.TriggerPrice)
				}
				m.Handlers.OnAlgoOrderUpdate(model.AlgoOrderUpdateEvent{
					Symbol:       inner.Symbol,
					AlgoID:       inner.AlgoID,
					ClientAlgoID: inner.ClientAlgoID,
					OrderType:    inner.OrderType,
					AlgoStatus:   inner.AlgoStatus,
					TriggerPrice: trig,
				})
			}
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
