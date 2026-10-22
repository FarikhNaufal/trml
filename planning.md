# IMPLEMENTATION & EXECUTION PLAN: BINANCE USDⓈ-M FUTURES INTRADAY BOT
**Strategi:** Confluence VWAP Pullback / Retest with Orderflow Bounce (Two-Way LONG & SHORT)  
**Dokumen Referensi:** [scaffold.md](file:///Users/naufal/Projects/trml/scaffold.md) (v2.1.0) & [audit_strategi_binance_futures_bot_v2.md](file:///Users/naufal/Projects/trml/audit_strategi_binance_futures_bot_v2.md)  
**Status Target:** Production-Grade Modular Go Architecture (Testnet -> Mainnet)  
**Terakhir Diperbarui:** 16 Oktober 2026 (Pivot ke VWAP Pullback Strategy)  

---

## 1. RINGKASAN & PRINSIP UTAMA

Rencana kerja ini mengeksekusi pemecahan implementasi monolitik dari [pseudo.go](file:///Users/naufal/Projects/trml/pseudo.go) ke dalam arsitektur modular idiomatik Go yang bersih (*clean architecture*), bebas duplikasi (*Don't Repeat Yourself - DRY*), mudah diuji (*test-driven*), dan aman dari *race condition* (`-race`).

### Rationale Pivot Strategi: Dari "Pure Breakout" ke "VWAP Pullback / Retest"
Berdasarkan bukti empiris soak test 4 jam di Binance Mainnet:
1. **Eliminasi Bull Trap di Altcoin:** 70% breakout candle 5m di altcoin adalah *liquidity hunt* sesaat yang langsung berbalik arah (*bull trap*). Membeli di pucuk candle terbukti merugikan.
2. **Eliminasi Adverse Selection pada Limit Order:** Pada breakout kuat, limit order pasif di Best Bid tertinggal (*timeout*), sementara yang terisi hanyalah order saat pasar berbalik turun (*adverse selection*).
3. **Keunggulan VWAP Pullback / Retest:** Menunggu koin yang sedang tren untuk terkoreksi (pullback) menguji garis keseimbangan institusi (Session VWAP), lalu masuk hanya saat ada pantulan volume agresif (Orderflow Z-Score). Limit order terisi mulus karena dipasang saat pasar berada di level VWAP, dengan Stop Loss yang sangat rapat di bawah lantai VWAP.

### Fakta Nyata & Bukti Empiris Terverifikasi
Seluruh modul dibangun dan diverifikasi di atas bukti ril matematis dan fungsional bursa:
1. **Presisi Numerik Terverifikasi:** `QuantizePrice` dan `QuantizeQtyFloor` terbukti *fail-closed* menolak `tickSize <= 0` atau `stepSize <= 0` (Lolos di `TestQuantizeFiltersFailClosed`).
2. **Pencocokan Agresor aggTrade Valid:** Transaksi agresor dengan arah yang salah (`isBuyerMaker == false` pada limit buy di bid) terbukti ditolak dan tidak mengisi order (Lolos di `TestDryRunAggressorMatching`).
3. **Pemantauan Partial Fill Seketika:** Order yang terisi sebagian langsung dipromosikan ke open position, diproteksi SL/TP, dan dihitung risikonya sebelum batas 45 detik (Lolos di `TestDryRunPartialFillStopLossTrigger`).
4. **Two-Phase Commit & Safe SL-First Live Broker:** Order SL baru dipasang dan terkonfirmasi (`ACKED`) *sebelum* SL lama dibatalkan, mencegah celah *naked position* (Lolos di `TestLiveBrokerTwoPhaseAndSafeSL`).
5. **Pembatalan Sibling Algo Order:** Saat order exit SL terisi penuh, order TP pasangannya langsung dibatalkan otomatis (Lolos di `TestLiveBrokerExitSiblingCancellation`).
6. **Timeout 45s Entry Live:** Order entry LIMIT GTX yang kedaluwarsa dibatalkan otomatis (Lolos di `TestLiveBrokerEntryTimeoutCancellation`).
7. **Kontrak Resmi Binance 2026:**
   - REST Trade Algo: `POST /fapi/v1/algoOrder` (`STOP_MARKET`, `TAKE_PROFIT_MARKET`, `algoType: "CONDITIONAL"`).
   - Mode Posisi: Wajib One-Way Mode (`GET /fapi/v1/positionSide/dual` -> `dualSidePosition: false`) agar `reduceOnly = true` valid.
   - Pemisahan Endpoint WS: `/public` (`@bookTicker`), `/market` (`@aggTrade`, `@kline_5m`, `@markPrice`), dan `/private` (`ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`).


---

## 2. ASPEK BEST PRACTICE MODULAR & ANTI-DUPLIKASI (DRY PRINCIPLE)

### 2.1 Aspek Best Practice Modular Architecture (Clean & Idiomatic Go)
Untuk menjamin codebase maintainable, scalable, dan mudah diaudit secara institusional:

1. **Separation of Concerns (SoC) & 5 Lapisan Arsitektur Bersih:**
   - **Layer 1: Domain Core (`internal/model`):**
     * Berisi entitas domain murni (`Candle`, `ExchangeFilter`, `OrderIntent`, `LiveOrderRecord`, `SymbolState`, `MarketSnapshot`, `BookQuote`).
     * Bersifat agnostik: tidak bergantung pada pustaka eksternal bursa, database, atau framework I/O.
     * Menyediakan metode `st.Snapshot(now)` yang menghasilkan salinan *deep copy* immutable agar layer bisnis dapat bekerja tanpa memegang mutex.
   - **Layer 2: Domain Math & Indicators (`internal/indicator`):**
     * Berisi perhitungan matematis murni (EMA200, Wilder's RMA ATR14, RVOL20, BarDelta Z-Score, VWAP).
     * Murni deterministik, *stateless*, tanpa efek samping (*pure functions*), dan tanpa pemanggilan I/O.
   - **Layer 3: Business Logic & Policies (`internal/strategy`, `internal/scanner`, `internal/broker`):**
     * `internal/strategy`: Evaluasi 4-faktor konfirmasi teknikal, validasi staleness gatekeeper, dan kalkulasi ukuran lot (1% risk, friction fee, margin clamp 30%).
     * `internal/scanner`: Penyaringan makro likuiditas 24h, spread, RVOL, NATR, dan bootstrap riwayat 1000 candle.
     * `internal/broker`: Engine eksekusi dua mode (Simulator *Volume-Through-Limit* DryRun dan State Machine *Two-Phase Commit* Live).
   - **Layer 4: Infrastructure & Adapters (`internal/storage`, `internal/exchange`, `internal/ws`, `internal/config`):**
     * `internal/storage`: Persistensi SQLite WAL (DDL migration, daily equity baseline, trade ledger, signal audit).
     * `internal/exchange`: REST HTTP Client Binance (autentikasi HMAC-SHA256, keepalive, order placement, algo orders, market data).
     * `internal/ws`: WebSocket Supervisor (koneksi multi-stream, heartbeat ping/pong, parsing event payload, auto-reconnect backoff).
     * `internal/config`: Parsing konfigurasi `.env`, validasi batas risiko, dan pemetaan URL Testnet vs Mainnet.
   - **Layer 5: Application / Composition Root (`cmd/bot/main.go`):**
     * Mengikat seluruh komponen (*dependency injection*), mengelola sinyal sistem operasi (`SIGINT`, `SIGTERM`), dan menjalankan *graceful shutdown*.

2. **High Cohesion & Loose Coupling:**
   - Setiap paket hanya memiliki satu tanggung jawab domain tunggal (*Single Responsibility Principle*).
   - Antar-paket dihubungkan melalui kontrak antarmuka (`ExecutionBroker`, `Repository`) atau fungsional injeksi (`PlaceEntryFn`, `CancelOrderFn`), bukan kopling langsung ke struct konkrit.
   - Modul bisnis (`strategy`, `broker`) dapat diuji 100% terisolasi tanpa koneksi bursa nyata atau dependensi jaringan.

3. **Strict Dependency & Import Hierarchy (Anti Circular Dependency):**
   - Alur impor hanya mengalir ke satu arah (hirarki piramida):
     $$\text{cmd/bot} \longrightarrow \text{internal/\{broker, strategy, scanner, exchange, ws, storage, config\}} \longrightarrow \text{internal/indicator} \longrightarrow \text{internal/model}$$
   - Paket `internal/model` berada di dasar paling bawah dan **dilarang keras** mengimpor paket `internal/*` mana pun.

4. **Interface Segregation & Package Encapsulation:**
   - Antarmuka dibuat ramping dan fokus (`ExecutionBroker` hanya mengekspos metode yang relevan untuk bot).
   - Helper internal, struct pembungkus, dan konstanta lokal dibuat *unexported* (huruf awal kecil) untuk menjaga API publik tetap bersih.

5. **Concurrency Safety & Mutex Discipline (Zero-Lock I/O):**
   - **Aturan Baku Mutex:** Dilarang keras memanggil operasi jaringan (HTTP/WebSocket) atau database write di dalam blok penguncian `sync.Mutex`.
   - Mutex hanya digunakan untuk memutasi state in-memory atau membuat snapshot (`Snapshot()`). Setelah snapshot disalin, mutex langsung dilepas sebelum evaluasi sinyal atau eksekusi order dilakukan.

---

### 2.2 Aturan Baku Anti-Duplikasi Kode & Fungsi (DRY - Don't Repeat Yourself)

Untuk mencegah divergensi logika, bug redundan, dan pemborosan pemeliharaan:

1. **Anti-Duplikasi Model Data & Status Enums (Single Source of Truth - SSOT):**
   - Seluruh model data domain (`Candle`, `ExchangeFilter`, `Side`, `OrderLifecycleStatus`, `OrderIntent`, `LiveOrderRecord`, `SymbolState`, `MarketSnapshot`, `BookQuote`) hanya didefinisikan satu kali di `internal/model/`.
   - Dilarang membuat struct bayangan atau enum alternatif di paket lain.

2. **Anti-Duplikasi Kuantisasi Presisi Bursa:**
   - Seluruh logika kuantisasi harga dan lot bursa (`QuantizePrice`, `QuantizeQtyFloor`) dipusatkan secara eksklusif di `internal/model/filter.go`.
   - Dilarang menuliskan fungsi pembulatan ad-hoc, math floor manual, atau formatting lot di luar `internal/model/filter.go`.

3. **Anti-Duplikasi Algoritma Matematika & Indikator:**
   - Seluruh rumus teknikal (EMA200, Wilder's RMA ATR14, Session VWAP, RVOL20, Mean/StdDev, BarDelta Z-Score) dipusatkan di `internal/indicator/`.
   - Modul `internal/scanner` (saat warm-up simbol) dan `internal/strategy` (saat evaluasi real-time) memanggil fungsi yang sama persis, menjamin konsistensi matematis 100%.

4. **Anti-Duplikasi Manipulasi Ring Buffer & Slice:**
   - Fungsi pemotongan kapasitas slice (`AppendCandleWithCap`, `AppendFloatWithCap`) didefinisikan secara tunggal di `internal/model/candle.go`.

5. **Anti-Duplikasi Autentikasi REST & Signer Binance:**
   - Pembuatan tanda tangan HMAC-SHA256, query parameter parsing, dan penambahan header `X-MBX-APIKEY` hanya ada di `internal/exchange/client.go` (`DoRequest`).

6. **Anti-Duplikasi URL Builder & JSON Parsing WebSocket:**
   - Perakitan URL multi-stream (`BuildCombinedStreamURL`) dan pembedahan payload JSON Binance (`ProcessMessage`) hanya ada di `internal/ws/manager.go`.

7. **Anti-Duplikasi Operasi Snapshotting State:**
   - Fungsi pembuatan deep copy snapshot immutable disentralisasi di metode `st.Snapshot(now)` pada `internal/model/snapshot.go`.

8. **Pengutamaan Standard Library Go (No Reinventing the Wheel):**
   - Menghindari pembuatan struct helper ad-hoc jika pustaka standar Go sudah menyediakannya (contoh ril: menggunakan `strings.NewReader` bawaan Go daripada membuat struct `dummyReader` kustom).

9. **Checklist Audit Anti-Duplikasi untuk Code Review:**
   - Jalankan `go vet ./...` untuk memastikan tidak ada *shadowed variables*.
   - Jalankan `grep -rn "func Calculate" internal/` untuk memverifikasi seluruh kalkulasi berada di `internal/indicator/`.
   - Jalankan `grep -rn "type Candle struct" internal/` untuk memastikan model hanya ada di `internal/model/candle.go`.

---

### 2.3 Spesifikasi Algoritma: VWAP Pullback / Retest with Orderflow Bounce (Two-Way)

Strategi ini mengombinasikan tren makro, harga ekuilibrium mikro (Session VWAP), dan agresi orderflow tick-level:

```
[1. Macro Trend]         Close > EMA200 (Long) / Close < EMA200 (Short)
[2. Prior Impulse]       High >= VWAP + 1.0x ATR (Long) / Low <= VWAP - 1.0x ATR (Short) dalam 12 bar terakhir
[3. Retest Pullback]     Low <= VWAP + (0.35x ATR) (Long) / High >= VWAP - (0.35x ATR) (Short)
[4. Orderflow Bounce]    Candle Bullish + BarDeltaZScore >= +1.20 (Long)
                         Candle Bearish + BarDeltaZScore <= -1.20 (Short)
[5. Derivatives Filter]  OI Delta 5m >= -0.10% (No capital flight) & Funding Rate wajar (|FR| <= 0.05%)
[6. Execution & Risk]    Limit Order di BestBid (Long) / BestAsk (Short)
                         SL: 1.0x ATR dari entry (Ruang nafas noise yang sehat)
                         TP: +2.0R target (R:R 1:2) & Break-Even Plus saat mencapai +1.0R
```

#### Aturan Matematis Sinyal:

1. **LONG (Bullish Pullback & Bounce):**
   * **Trend Filter:** `latest.Close > ema200`
   * **Prior Impulse Filter:** $\exists \, t \in [T-12, T-1] \text{ s.t. } \text{Candle}[t].\text{High} \ge \text{VWAP} + (1.0 \times \text{ATR14})$
   * **Pullback Zone Test:** `latest.Low <= VWAP + (0.35*ATR)` $\lor$ `prevBar.Low <= VWAP + (0.35*ATR)`
   * **Bounce Confirmation:** `latest.Close > latest.Open` (Candle Hijau) $\land$ `latest.Close >= VWAP`
   * **Orderflow Bounce:** `BarDeltaZScore >= +1.20`
   * **Derivatives Sanity:** `oiDeltaPct >= -0.10%` $\land$ `fundingRate <= +0.05%`
   * **Target Entry:** `snap.BestBid` (Limit Post-Only di area VWAP)
   * **Stop Loss:** $\text{Entry} - (1.0 \times \text{ATR14})$
   * **Take Profit:** $\text{Entry} + 2.0 \times (\text{Entry} - \text{SL})$

2. **SHORT (Bearish Pullback & Rejection):**
   * **Trend Filter:** `latest.Close < ema200`
   * **Prior Impulse Filter:** $\exists \, t \in [T-12, T-1] \text{ s.t. } \text{Candle}[t].\text{Low} \le \text{VWAP} - (1.0 \times \text{ATR14})$
   * **Pullback Zone Test:** `latest.High >= VWAP - (0.35*ATR)` $\lor$ `prevBar.High >= VWAP - (0.35*ATR)`
   * **Rejection Confirmation:** `latest.Close < latest.Open` (Candle Merah) $\land$ `latest.Close <= VWAP`
   * **Orderflow Rejection:** `BarDeltaZScore <= -1.20`
   * **Derivatives Sanity:** `oiDeltaPct >= -0.10%` $\land$ `fundingRate >= -0.05%`
   * **Target Entry:** `snap.BestAsk` (Limit Post-Only di area VWAP)
   * **Stop Loss:** $\text{Entry} + (1.0 \times \text{ATR14})$
   * **Take Profit:** $\text{Entry} - 2.0 \times (\text{SL} - \text{Entry})$

---

## 3. ARSITEKTUR STRUKTUR DIREKTORI HASIL MODULARISASI

```text
trml/
├── cmd/
│   └── bot/
│       └── main.go                 # Entrypoint bot, inisialisasi modul, scanner worker, WS supervisor, graceful shutdown
├── internal/
│   ├── config/
│   │   ├── config.go               # Parser .env, validasi batas risiko, pemetaan URL Testnet vs Mainnet
│   │   └── config_test.go          # Unit test parsing & validasi config (LULUS)
│   ├── model/
│   │   ├── candle.go               # Struct Candle, validasi monotonic, ring buffer helpers (AppendCandleWithCap)
│   │   ├── filter.go               # Struct ExchangeFilter & kuantisasi presisi fail-closed (QuantizePrice, QuantizeQtyFloor)
│   │   ├── order.go                # Struct OrderIntent, LiveOrderRecord, ProtectionPlan, OrderLifecycleStatus
│   │   ├── snapshot.go             # Struct SymbolState, MarketSnapshot, BookQuote, thread-safe Snapshot() method
│   │   └── model_test.go           # Unit test presisi kuantisasi & validasi candle (LULUS)
│   ├── storage/
│   │   ├── sqlite.go               # Inisialisasi SQLite mode WAL, migrasi schema DDL tabel
│   │   ├── repository.go           # Repository account_daily_state, trade_ledger, signal_audit_log
│   │   └── sqlite_test.go          # Integration test persistensi SQLite & rollover baseline harian (LULUS)
│   ├── indicator/
│   │   ├── vwap.go                 # Session VWAP berbasis VolumeBase & EpochDayUTC (00:00:00 UTC), RVOL20, Envelope
│   │   ├── ema.go                  # EMA200 mature (1000 bars) & Wilder's RMA ATR14
│   │   ├── orderflow.go            # Bar Delta Z-Score (50 baseline bar eksklusif bar t) & Mean/StdDev
│   │   └── indicator_test.go       # Unit test akurasi indikator matematis (LULUS)
│   ├── scanner/
│   │   ├── scanner.go              # Bulk 24hr ticker & bookTicker filter, perankingan Top 10 RVOL
│   │   ├── bootstrap.go            # Bootstrap 1000 candle kline 5m tertutup untuk warm-up state
│   │   └── scanner_test.go         # Test penyaringan simbol, volume, spread, dan NATR (LULUS)
│   ├── strategy/
│   │   ├── sizing.go               # Kalkulasi ukuran posisi (1% risk + friksi fee/slippage + clamp margin 30%)
│   │   ├── evaluator.go            # Evaluator 4 konfirmasi sinyal & staleness gatekeeper (book <= 5s, mark <= 15s, OI 4-7m)
│   │   └── strategy_test.go        # Test skenario sinyal valid, stale data, dan clamp margin (LULUS)
│   ├── broker/
│   │   ├── interface.go            # Kontrak antarmuka ExecutionBroker
│   │   ├── dryrun.go               # Simulator Heuristic Volume-Through-Limit (Aggressor matching, partial fill tracking)
│   │   ├── live.go                 # Two-Phase Entry Commit, Safe SL-First, Exit Ledger, Sibling Cancel, 45s Timeout
│   │   └── broker_test.go          # Suite test konkurensi broker dengan flag -race (LULUS)
│   ├── exchange/
│   │   ├── client.go               # REST HTTP Client (Signed HMAC-SHA256, keepalive, strings.NewReader)
│   │   ├── trade.go                # Order entry GTX, algo order SL/TP, cancel, One-Way mode & 5x isolated verification
│   │   ├── market.go               # REST query exchangeInfo, 24h ticker, bookTicker, klines, openInterest, listenKey
│   │   └── exchange_test.go        # Mock/Integration test panggilan REST API bursa (LULUS)
│   └── ws/
│       ├── manager.go              # Supervisor koneksi WebSocket multi-stream, auto-reconnect backoff, URL builder
│       └── ws_test.go              # Test framing & parsing event streaming (LULUS)
├── test/
│   └── acceptance_test.go          # 20 Acceptance Tests end-to-end terintegrasi (LULUS)
├── .env.example                    # Template variabel lingkungan
├── Dockerfile                      # Multi-stage lightweight build
├── go.mod                          # Go 1.26 toolchain & dependencies
├── go.sum
├── pseudo.go                       # Referensi monolitik terverifikasi
└── pseudo_test.go                  # Suite test referensi monolitik
```

---

## 4. STATUS TAHAPAN EKSEKUSI (PHASED EXECUTION TRACKER)

### FASE 1: Fondasi Core, Model Data, dan Konfigurasi
- [x] Buat file `.env.example` dan paket `internal/config/config.go`
  - Mendukung `BINANCE_ENV=testnet` dan `BINANCE_ENV=mainnet`
  - Parser kredensial API Key, Secret Key, DB Path, Fee rates, dan Log level
- [x] Buat paket `internal/model/`
  - `candle.go`: Definisi `Candle`, `ValidateAndFilterCandles`, helper `AppendCandleWithCap`, `AppendFloatWithCap`
  - `filter.go`: Definisi `ExchangeFilter`, `QuantizePrice` (fail-closed), dan `QuantizeQtyFloor` (fail-closed)
  - `order.go`: Definisi `OrderIntent`, `LiveOrderRecord`, `LivePositionProtection`, `ProtectionPlanAction`
  - `snapshot.go`: Definisi `SymbolState`, `MarketSnapshot`, `BookQuote`, dan metode thread-safe `Snapshot()`
- [x] **Checkpoint Verifikasi 1:** `go test -v -race ./internal/model/... ./internal/config/...` (LULUS)

### FASE 2: Observabilitas & Persistensi SQLite WAL
- [x] Tambahkan dependensi `modernc.org/sqlite` ke `go.mod`
- [x] Buat paket `internal/storage/sqlite.go`
  - Inisialisasi database mode WAL (`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;`)
  - Eksekusi DDL migrasi tabel `account_daily_state`, `trade_ledger`, dan `signal_audit_log`
- [x] Buat `internal/storage/repository.go`
  - Simpan & pulihkan baseline harian UTC (`SessionStartEquity`, `CircuitBreakerHit`) untuk toleransi restart
  - Pencatatan transaksi real-time ke `trade_ledger` beserta metrik MAE/MFE
  - Pencatatan audit keputusan sinyal ke `signal_audit_log`
- [x] **Checkpoint Verifikasi 2:** `go test -v -race ./internal/storage/...` (LULUS)

### FASE 3: Modul Kuantitatif (Indikator & Market Scanner)
- [x] Buat paket `internal/indicator/`
  - `vwap.go`: Akumulasi `SessionVWAP` berbasis base volume sejak `00:00:00 UTC`, `CalculateRVOL`, `CalculateEnvelope`
  - `ema.go`: `CalculateEMA` (1000 bar) dan Wilder's Smoothing `CalculateATR` (ATR14)
  - `orderflow.go`: `CalculateMeanAndStdDev` dan BarDelta Z-Score baseline 50 candle eksklusif bar t
- [x] Buat paket `internal/scanner/`
  - `scanner.go`: `RunBulkMarketScanner` memfilter simbol PERPETUAL, volume 24h, spread $\le 0.03\%$, RVOL $\ge 1.80$, dan NATR 0.35%–2.50%
  - `bootstrap.go`: `BootstrapSymbolState` memuat histori 1000 candle tertutup
- [x] **Checkpoint Verifikasi 3:** `go test -v -race ./internal/indicator/... ./internal/scanner/...` (LULUS)

### FASE 4: Modul Strategi VWAP Pullback/Retest & Sizing Risiko
- [x] Buat paket `internal/strategy/`
  - `sizing.go`: `BuildSafeDecimalOrderIntent` (1.0% risiko, friksi maker/taker/slippage, clamp margin slot 30%, filter bursa fail-closed, formula SL berbasis pivot pantulan)
  - `evaluator.go`: `EvaluateSignal` (Gatekeeper circuit breaker, slot cap, staleness, dual-timestamp OI interval, logika 5-tahap VWAP Pullback & Orderflow Bounce)
- [x] **Migrasi Strategi ke VWAP Pullback / Retest (Two-Way LONG & SHORT):**
  - Implementasi prior impulse check ($N$-bar lookback $\ge 1.0\times\text{ATR}$)
  - Implementasi pullback retest zone touch ($\text{VWAP} \pm 0.35\times\text{ATR}$)
  - Implementasi orderflow bounce confirmation ($Z\text{-score} \ge +0.80$ Long / $\le -0.80$ Short)
  - Update unit tests di `strategy_test.go`
- [x] **Checkpoint Verifikasi 4 (Pivot):** `go test -v -race ./internal/strategy/...` (LULUS)


### FASE 5: Execution Broker Engine (Dry-Run & Live State Machine)
- [x] Buat paket `internal/broker/`
  - `interface.go`: Interface `ExecutionBroker`
  - `dryrun.go`: `DryRunBroker` dengan pencocokan agresor `isBuyerMaker`, pemantauan kuantitas partial fill seketika, mark price staleness, dan circuit breaker
  - `live.go`: `BinanceLiveBroker` dengan Two-Phase Commit Entry, Safe SL-First Protection Plan, Exit Order Ledger, pembatalan sibling algo order, dan timeout manager 45 detik
- [x] **Checkpoint Verifikasi 5:** `go test -v -race ./internal/broker/...` (LULUS)

### FASE 6: Exchange REST & WebSocket Client Adapters
- [x] Tambahkan dependensi `github.com/coder/websocket` dan `github.com/bytedance/sonic`
- [x] Buat paket `internal/exchange/`
  - `client.go`: HTTP REST client dengan autentikasi HMAC-SHA256, keepalive, dan standard `strings.NewReader`
  - `trade.go`: Pemanggilan endpoint `POST /fapi/v1/order` (GTX limit), `POST /fapi/v1/algoOrder` (SL/TP STOP_MARKET / TAKE_PROFIT_MARKET), `DELETE /fapi/v1/algoOrder`, `GET /fapi/v1/positionSide/dual`, dan `InitAndVerifySymbol`
  - `market.go`: REST endpoint market data (`exchangeInfo`, `ticker/24hr`, `ticker/bookTicker`, `klines`, `openInterest`, `listenKey`)
- [x] Buat paket `internal/ws/`
  - `manager.go`: Supervisor WebSocket dengan reconnect backoff, heartbeat, helper `BuildCombinedStreamURL`, dan parser event streaming
- [x] **Checkpoint Verifikasi 6:** `go test -v -race ./internal/exchange/... ./internal/ws/...` (LULUS)

### FASE 7: Main Entrypoint & Orchestration
- [x] Buat `cmd/bot/main.go`
  - Inisialisasi database SQLite WAL dan config `.env`
  - Verifikasi startup: One-Way Mode, margin type Isolated, dan leverage 5x
  - Background worker periodic scanner setiap 15 menit
  - Background worker periodic Open Interest poller setiap 5 menit
  - Koneksi WebSocket multi-stream supervisor terpusat ke broker
  - User data stream supervisor (listenKey + 30m keepalive) untuk mode Live
  - Lock-free signal evaluation pada closed 5m kline
  - Graceful shutdown menangani sinyal OS (`SIGINT`, `SIGTERM`)
- [x] **Checkpoint Verifikasi 7:** `go build -o bin/bot ./cmd/bot` (LULUS)

### FASE 8: Testnet Integration & Soak Verification
- [x] Acceptance test end-to-end terintegrasi bebas data race (`test/acceptance_test.go`)
- [ ] Pengisian kredensial Binance USDⓈ-M Futures Testnet di file `.env`
- [ ] Menjalankan bot di lingkungan Testnet (`DRY_RUN_MODE=false`) atau Dry-Run Simulator di atas feed pasar live
- [ ] Memantau pencatatan trade ledger dan log audit sinyal di file database SQLite

---

## 5. MATRIKS 20 ACCEPTANCE TESTS TERVERIFIKASI (-race PASS)

| No | Nama Acceptance Test | Modul Target | File Pengujian | Status |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Reset VWAP Midnight-UTC | `internal/indicator` | `indicator_test.go` | PASS |
| 2 | Bobot Volume Base VWAP | `internal/indicator` | `indicator_test.go` | PASS |
| 3 | Konsistensi Bar Delta USDT | `internal/indicator` | `indicator_test.go` | PASS |
| 4 | Deduplikasi Monotonik Candle | `internal/model` | `model_test.go` | PASS |
| 5 | Staleness BookTicker $\le 5$s | `internal/strategy` | `strategy_test.go` | PASS |
| 6 | Staleness Mark Price $\le 15$s | `internal/strategy` | `strategy_test.go` | PASS |
| 7 | Validasi Interval Delta OI 4–7m | `internal/strategy` | `strategy_test.go` | PASS |
| 8 | Konvergensi EMA200 1000-Bar | `internal/indicator` | `indicator_test.go` | PASS |
| 9 | Kuantisasi Filter Fail-Closed | `internal/model` | `model_test.go` | PASS |
| 10 | Margin-Cap 30% pada NATR Rendah | `internal/strategy` | `strategy_test.go` | PASS |
| 11 | Reservasi Slot Atomik In-Memory | `internal/broker` | `broker_test.go` | PASS |
| 12 | Aggressor Matching `isBuyerMaker` | `internal/broker` | `broker_test.go` | PASS |
| 13 | Pemantauan Instan Partial Fill | `internal/broker` | `broker_test.go` | PASS |
| 14 | Two-Phase Commit Live Entry | `internal/broker` | `broker_test.go` | PASS |
| 15 | Safe SL-First Protection Replacement | `internal/broker` | `broker_test.go` | PASS |
| 16 | Eskalasi Darurat Emergency Close | `internal/broker` | `broker_test.go` | PASS |
| 17 | Pembatalan Sibling Algo Order Exit | `internal/broker` | `broker_test.go` | PASS |
| 18 | Timeout 45 Detik Entry Live | `internal/broker` | `broker_test.go` | PASS |
| 19 | Sinkronisasi Akun & Drawdown UTC | `internal/broker` | `broker_test.go` | PASS |
| 20 | Verifikasi One-Way & Leverage 5x | `internal/exchange`| `exchange_test.go` | PASS |

---

## 6. PANDUAN PENGOPERASIAN & EKSEKUSI

### 1. Menjalankan Seluruh Test Suite (Race Detector Aktif)
```bash
go test -count=1 -v -race ./...
```

### 2. Melakukan Kompilasi Executable Bot
```bash
go build -o bin/bot ./cmd/bot
```

### 3. Menjalankan Bot dalam Mode Dry-Run (Simulator Real-time)
```bash
cp .env.example .env
# Set DRY_RUN_MODE=true di .env
./bin/bot
```

### 4. Menjalankan Bot di Binance USDⓈ-M Futures Testnet
```bash
# Isi BINANCE_API_KEY dan BINANCE_SECRET_KEY di .env
# Set BINANCE_ENV=testnet dan DRY_RUN_MODE=false
./bin/bot
```

---

## 7. RESOLUSI AUDIT INSTITUSIONAL & HARDENING KEAMANAN LENGKAP

### Temuan Critical (C1–C7) — Status: 100% RESOLVED
| Kode | Temuan Audit | Tindakan Perbaikan Yang Telah Diimplementasikan | Status |
| :--- | :--- | :--- | :--- |
| **C1** | Emergency Close gagal karena qty=0 dan side hardcoded | Mengirim `action.ExitSide` dan `action.TargetQty` riil ke `EmergencyMarketClose` dengan `reduceOnly=true`. | **RESOLVED** |
| **C2** | Emergency Close error di-silently ignore | Menambahkan retry hingga 3x berjarak 250ms dan eskalasi log alert kritikal jika tetap gagal. | **RESOLVED** |
| **C3** | Parameter `effectiveEquity` & `availBal` tertukar di `EvaluateSignal` | Mengoreksi urutan argument: parameter 5 diisi `totEq`, parameter 6 diisi `availBal`. | **RESOLVED** |
| **C4** | Typo `DRY_RUN_MODE` default ke mode LIVE diam-diam | Parser ketat `strconv.ParseBool` dengan fail-closed: typo langsung mengembalikan error fatal dan bot batal start. | **RESOLVED** |
| **C5** | Error parsing numerik dibuang (drawdown, leverage, slots) | Validasi eksplisit rentang nilai numerik (drawdown wajib $-0.01$ s.d $-0.30$, slots 1–10, leverage 1–20). | **RESOLVED** |
| **C6** | Scanner menahan write-lock `statesMu` selama REST klines serial | Melepas lock sebelum eksekusi REST klines scanner; pembaruan state dilakukan secara atomik di akhir. | **RESOLVED** |
| **C7** | URL User Data Stream menggunakan base public stream | Diarahkan ke `WSPrivateBaseURL` (`wss://fstream.binance.com/ws` / `stream.binancefuture.com/ws`). | **RESOLVED** |

### Temuan High (H1–H6 & Runtime) — Status: 100% RESOLVED
| Kode | Temuan Audit | Tindakan Perbaikan Yang Telah Diimplementasikan | Status |
| :--- | :--- | :--- | :--- |
| **H1** | Tidak ada time-stop posisi di mode Live | Diimplementasikan `CheckMaxHoldingTimeouts` pada `live.go` yang memantau umur posisi dan mengeksekusi market close otomatis. | **RESOLVED** |
| **H2** | ActivePositions leak pada posisi kuantitas nol | Parser `ACCOUNT_UPDATE` meneruskan posisi nol dan membersihkan map `ActivePositions`, `PositionOpenTimes`, dan metadata posisi. | **RESOLVED** |
| **H3** | Entry-timeout cancel gagal diabaikan berisiko phantom position | Pada `CheckPendingEntryTimeouts`, record hanya dihapus jika cancel berhasil atau order sudah tiada (error -2011). | **RESOLVED** |
| **H4** | SL/TP fill sebelum registrasi order / unknown-order | Saat order tidak ditemukan di memori, `HandleUserDataOrderUpdate` memanggil `QueryOrderFn` dan merekonsiliasi record ke memori. | **RESOLVED** |
| **H5** | Proteksi konkurensi (generation check) | Pengecekan generasi plan proteksi serial di bawah mutex mengeliminasi goroutine collision. | **RESOLVED** |
| **H6** | Pemulihan State Harian (Drawdown baseline & persistensi) | Saat startup, `GetDailyState` dipanggil untuk me-restore modal awal hari ini. Dry-run kini juga mencatat state harian ke SQLite. | **RESOLVED** |
| **H-Sync** | `SyncServerTime` tidak dipanggil di runtime | `client.SyncServerTime(ctx)` dipanggil secara eksplisit saat startup `main.go` sebelum trading dimulai. | **RESOLVED** |
| **H-API** | Ketiadaan structured `APIError` dan retry 429/5xx | Dibuat struct `APIError` terstruktur dan retry dengan bounded exponential backoff untuk HTTP 429 dan 5xx di `DoRequest`. | **RESOLVED** |
| **H-Key** | Lifecycle `listenKey` tanpa re-create/delete | Ditambahkan auto-recreate jika keepalive gagal, serta `DeleteListenKey` via graceful shutdown hook. | **RESOLVED** |

### Temuan Medium (M1–M11) — Status: 100% RESOLVED
| Kode | Temuan Audit | Tindakan Perbaikan Yang Telah Diimplementasikan | Status |
| :--- | :--- | :--- | :--- |
| **M1** | `isDailyBreached` & `hasActiveSymbol` hardcoded `false, false` | Dihubungkan ke `e.broker.GetDailyTotalDrawdownPct() <= e.cfg.MaxDailyDrawdown` dan `e.broker.HasActiveOrderOrPosition(snap.Symbol)`. | **RESOLVED** |
| **M2** | Partial-fill double-count slot | `GetActiveSlotCount()` pada live dan dry-run broker kini menghitung *set of distinct symbols*. | **RESOLVED** |
| **M3** | VWAP session tidak di-reset ke 0 hari baru | `CurrentSessionVWAP` di-reset ke `0.0` secara eksplisit saat deteksi pergantian hari kalender UTC. | **RESOLVED** |
| **M4** | Parse error WS diabaikan (nilai 0 ke risk) | Seluruh parsing numerik WS (`strconv.ParseFloat`, `decimal.NewFromString`) kini divalidasi fail-closed. | **RESOLVED** |
| **M5** | Kolisi ClientOrderID pada re-entry satu bar candle | Ditambahkan atomic nonce 4-digit unik dengan pemangkasan aman ke batas maksimal 36 karakter Binance. | **RESOLVED** |
| **M6** | QuantizePrice Ceil/Floor off-by-one epsilon | Digantikan dengan matematika modulo decimal eksak (`price.Mod(tickSize)`), bebas floating jitter. | **RESOLVED** |
| **M6b**| Break-even offset tidak mencakup slippage exit | Ditambahkan parameter slippage rate ke dalam kalkulasi `beOffsetRate`. | **RESOLVED** |
| **M7** | Konsistensi OHLC candle belum divalidasi penuh | Ditambahkan validasi ketat `High >= max(Open, Close)` dan `Low <= min(Open, Close)`. | **RESOLVED** |
| **M8** | Watchlist drop-watch tidak pernah dievicted | Scanner melakukan re-bootstrap otomatis saat simbol dipilih kembali untuk mencegah data gap. | **RESOLVED** |
| **M9** | Kolom analitik audit log bernilai 0 di database | Nilai `ema200`, `atr14`, `bar_delta_zscore`, dan `oi_delta_pct` dikalkulasi riil sebelum ditulis ke SQLite. | **RESOLVED** |
| **M10**| Bootstrap error diabaikan | `BootstrapSymbolState` kini mencatat warning log jika gagal memuat klines awal. | **RESOLVED** |
| **M11**| `IsLeverageIsolatedVerified` tidak pernah diset | Di-update menjadi `true` pada state simbol setelah `InitAndVerifySymbol` sukses. | **RESOLVED** |
| **M-Pragma**| SQLite PRAGMA hilang saat recycle koneksi | PRAGMA WAL, synchronous, dan busy_timeout disematkan ke DSN connection string SQLite. | **RESOLVED** |

### Temuan Low (L1–L9) — Status: 100% RESOLVED
| Kode | Temuan Audit | Tindakan Perbaikan Yang Telah Diimplementasikan | Status |
| :--- | :--- | :--- | :--- |
| **L1** | EMA/ATR return 0.0 tanpa ok-flag | Ditambahkan fungsi `CalculateEMASafe` dan `CalculateATRSafe` dengan flag boolean `ok`. | **RESOLVED** |
| **L2** | `OppositeSide` fallback ke `SideLong` | Diubah menjadi switch statement eksplisit fail-closed (panic pada invalid side). | **RESOLVED** |
| **L3** | `io.ReadAll` tanpa batas ukuran body | Dibatasi dengan `io.LimitReader(resp.Body, 10<<20)` (maksimal 10MB per respons). | **RESOLVED** |
| **L4** | Timestamp RFC3339 tanpa sub-detik | Menggunakan format `time.RFC3339Nano` untuk pencatatan riwayat trade dan audit log. | **RESOLVED** |
| **L5** | `sort.Slice` non-stabil pada scanner | Diganti dengan `sort.SliceStable` disertai tie-break alfabetis berdasarkan nama simbol. | **RESOLVED** |
| **L6** | Parse error `MIN_VOLUME_24H_USDT` diabaikan | Parsing float ketat dengan validasi fail-closed non-negatif saat startup. | **RESOLVED** |
| **L7** | Ketiadaan index pada `signal_audit_log` | Ditambahkan composite index `(symbol, candle_open_time_utc)` dan index `created_at_utc`. | **RESOLVED** |
| **L8** | `err == sql.ErrNoRows` bukan `errors.Is` | Diganti dengan `errors.Is(err, sql.ErrNoRows)` sesuai idiomatik Go modern. | **RESOLVED** |
| **L9** | Klarifikasi endpoint `/fapi/v1/algoOrder` | Terverifikasi sesuai spesifikasi resmi Binance Futures 2024–2026 untuk order kondisional (`STOP_MARKET`). | **VERIFIED** |

### Temuan Penyempurnaan Ringan (R1–R3) — Status: 100% RESOLVED
| Kode | Temuan Audit | Tindakan Perbaikan Yang Telah Diimplementasikan | Status |
| :--- | :--- | :--- | :--- |
| **R1** | Backoff WS tanpa jitter | Ditambahkan randomized jitter 0–33% via `math/rand/v2` pada reconnect backoff WebSocket untuk mencegah *thundering herd / reconnect storm*. | **RESOLVED** |
| **R2** | `recvWindow 5000ms` hardcoded | Nilai `RECV_WINDOW_MS` kini terbaca dari `.env` (default 5000, rentang sah 1000–60000) dan diinjeksikan dinamis ke `Client.RecvWindow`. | **RESOLVED** |
| **R3** | Verifikasi `/fapi/v1/algoOrder` Binance | Terverifikasi 100% valid terhadap Binance USDS-M Futures Algo API specs: wajib untuk `STOP_MARKET` (`algoType=CONDITIONAL`). | **VERIFIED** |


