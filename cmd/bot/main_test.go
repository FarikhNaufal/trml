package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"trml/internal/config"
	"trml/internal/exchange"
	"trml/internal/model"
)

func TestPollOpenInterest_ConcurrentAndPartialFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	var mu sync.Mutex
	histCalls := make(map[string]int)
	liveCalls := make(map[string]int)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/futures/data/openInterestHist":
			mu.Lock()
			histCalls[sym]++
			mu.Unlock()

			if sym == "FAILUSDT" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
				return
			}
			resp := []exchange.OpenInterestHistItem{
				{Symbol: sym, SumOpenInterest: "1000.0", Timestamp: now.Add(-13 * time.Minute).UnixMilli()},
				{Symbol: sym, SumOpenInterest: "1010.0", Timestamp: now.Add(-8 * time.Minute).UnixMilli()},
				{Symbol: sym, SumOpenInterest: "1020.0", Timestamp: now.Add(-3 * time.Minute).UnixMilli()},
			}
			_ = json.NewEncoder(w).Encode(resp)

		case "/fapi/v1/openInterest":
			mu.Lock()
			liveCalls[sym]++
			mu.Unlock()

			if sym == "FAILUSDT" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"symbol":       sym,
				"openInterest": "1050.0",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := exchange.NewClient(ts.URL, "", "")
	engine := &BotEngine{
		cfg: &config.Config{
			CandleDuration: 5 * time.Minute,
			MaxOIInterval:  10 * time.Minute,
		},
		client:          client,
		activeStates:    make(map[string]*model.SymbolState),
		watchedCoins:    []string{"ZECUSDT", "FAILUSDT", "ETHUSDT"},
		oiBootAttemptAt: make(map[string]time.Time),
	}

	for _, sym := range engine.watchedCoins {
		st := model.NewSymbolState(sym, model.ExchangeFilter{}, 10, 10)
		st.TargetOIInterval = 5 * time.Minute
		engine.activeStates[sym] = st
	}

	// Putaran 1: FAILUSDT gagal, ZECUSDT & ETHUSDT harus tetap sukses bootstrap + live poll
	engine.pollOpenInterest(context.Background())

	for _, sym := range []string{"ZECUSDT", "ETHUSDT"} {
		st := engine.activeStates[sym]
		st.Mu.Lock()
		prevOI := st.PrevOpenInterest
		currOI := st.CurrOpenInterest
		needsBoot := st.NeedsOIBootstrapLocked(time.Now().UTC(), engine.cfg.MaxOIInterval)
		st.Mu.Unlock()

		if prevOI <= 0 || currOI != 1050.0 {
			t.Fatalf("%s expected valid OI pair (prev > 0, curr == 1050), got prev=%f curr=%f", sym, prevOI, currOI)
		}
		if needsBoot {
			t.Fatalf("%s should no longer need OI bootstrap", sym)
		}
	}

	// Putaran 2 segera sesudahnya: FAILUSDT dalam cooldown 2m -> tidak memborbardir openInterestHist,
	// tetapi tetap mencoba live poll; ZECUSDT/ETHUSDT tidak bootstrap ulang.
	engine.pollOpenInterest(context.Background())

	mu.Lock()
	zecHist := histCalls["ZECUSDT"]
	failHist := histCalls["FAILUSDT"]
	failLive := liveCalls["FAILUSDT"]
	mu.Unlock()

	if zecHist != 1 {
		t.Fatalf("expected ZECUSDT openInterestHist called once, got %d", zecHist)
	}
	if failHist != 1 {
		t.Fatalf("expected FAILUSDT openInterestHist throttled to 1 call within cooldown, got %d", failHist)
	}
	if failLive < 2 {
		t.Fatalf("expected FAILUSDT live poll attempted on both ticks, got %d", failLive)
	}

	// Simulasi cooldown habis untuk FAILUSDT -> NeedsOIBootstrapLocked tetap true dan bootstrap bisa dicoba lagi
	engine.oiBootMu.Lock()
	engine.oiBootAttemptAt["FAILUSDT"] = time.Now().UTC().Add(-3 * time.Minute)
	engine.oiBootMu.Unlock()

	engine.pollOpenInterest(context.Background())

	mu.Lock()
	failHistAfter := histCalls["FAILUSDT"]
	mu.Unlock()
	if failHistAfter != 2 {
		t.Fatalf("expected FAILUSDT openInterestHist retried after cooldown, got %d", failHistAfter)
	}

	// Guard anti-overlap: bila oiPollRunning sudah true, panggilan berikutnya langsung skip tanpa memanggil API
	liveBefore := liveCalls["ZECUSDT"]
	engine.oiPollRunning.Store(true)
	engine.pollOpenInterest(context.Background())
	engine.oiPollRunning.Store(false)

	mu.Lock()
	liveAfter := liveCalls["ZECUSDT"]
	mu.Unlock()
	if liveAfter != liveBefore {
		t.Fatalf("expected overlapping pollOpenInterest to skip, calls changed from %d to %d", liveBefore, liveAfter)
	}
}
