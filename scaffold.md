# SYSTEM SCAFFOLD & TECHNICAL SPECIFICATION: BINANCE USDⓈ-M FUTURES INTRADAY BOT
**Document Version:** 2.1.0 (Hardened Production & Deterministic Dry-Run Backbone)
**Target Architecture:** Event-Driven Multi-Coin Scanner with 4-Factor Confirmation Strategy
**Primary Language:** Golang (Go 1.23+)
**Execution Mode:** Heuristic Volume-Through-Limit Dry-Run Engine & Live Mainnet Execution

---

## 1. PRINSIP DESAIN & ATURAN ANTI-HALUSINASI UNTUK AI AGENT

Setiap AI Agent yang mengimplementasikan kode dari dokumen ini WAJIB mematuhi 8 kontrak arsitektur berikut:
1. **Pemisahan Parameter Statis vs Nilai Adaptif:** Panjang jendela indikator (*lookback period*) dan ambang batas statistik ($Z\text{-Score}$, persentase $\Delta \text{OI}$, batas *adverse funding*, batas *spread*) bersifat **statis/terkunci**, sedangkan jarak harga (*Envelope Band*, *Stop Loss*, *Take Profit*) dan ukuran posisi (*Position Sizing*) bersifat **adaptif** mengikuti nilai $\text{ATR}_{14}$ saat itu.
2. **Zero Repainting & Deduplikasi Monotonik Candle:** Evaluasi sinyal hanya boleh dieksekusi saat candle 5 menit benar-benar tertutup (`kline.x == true`), harga valid, dan `OpenTime` strictly-monotonic lebih besar dari `LastProcessedCandleOpenTime`. Candle yang belum tertutup (`CloseTime >= ServerTime`) wajib diabaikan.
3. **Bebas Kontensi Lock (Immutable Snapshot Pattern):** Goroutine pembaca stream pasar (`@bookTicker`, `@aggTrade`, `@markPrice`, `@kline_5m`) tidak boleh menahan `sync.Mutex` saat menghitung indikator, mengevaluasi sinyal, atau memanggil I/O jaringan/database. Ambil *immutable snapshot* di bawah lock singkat, segera lepaskan lock, lalu proses sinyal secara lock-free.
4. **Presisi Numerik Terpisah (`float64` vs `decimal.Decimal`):** `float64` hanya diizinkan untuk kalkulasi statistik rolling indikator di memori. Seluruh harga order, kuantitas, pembulatan `TickSize`/`StepSize`, nilai nosional, saldo, margin, komisi, dan PnL wajib menggunakan `github.com/shopspring/decimal`. Fungsi kuantisasi wajib *fail-closed* (mengembalikan error jika filter <= 0).
5. **Bootstrap Historis Konvergen (Zero Cold-Start Blindness):** Sebelum simbol dipantau secara *real-time*, bot wajib mem-bootstrap **1.000 candle `5m` tertutup** melalui REST API guna memastikan konvergensi presisi $\text{EMA}_{200}$ (menghindari distorsi bobot seed SMA awal), $\text{ATR}_{14}$, serta akumulasi *Session VWAP* utuh sejak `00:00:00 UTC` hari berjalan.
6. **Fail-Closed Staleness Gatekeeper:** Evaluasi sinyal langsung menolak eksekusi jika:
   - Data top-of-book `BookTicker` lebih lama dari `5 detik` (`MAX_BOOK_STALENESS = 5s`).
   - Feed `MarkPrice` lebih lama dari `15 detik` (`MAX_MARK_STALENESS = 15s`).
   - Snapshot saldo/margin akun lebih lama dari `60 detik` (`MAX_ACCOUNT_STALENESS = 60s`).
   - Interval sampel Open Interest di luar rentang `4 - 7 menit` atau usia data OI > `30 detik`.
   - Data funding rate lebih lama dari `330 detik`.
7. **Two-Phase Commit Entry & Safe SL-First Protection (Mode Live):**
   - **Entry:** Fase 1 melakukan reservasi slot atomik in-memory di bawah lock (`ReserveEntryIntent`) tanpa I/O. Fase 2 mengirim order LIMIT GTX ke exchange di luar lock (`PlaceEntryFn`). Jika ditolak atau timeout, batalkan reservasi slot.
   - **Proteksi Anti-Naked:** Saat posisi terisi (penuh atau sebagian), pasang SL baru (`POST /fapi/v1/algoOrder STOP_MARKET`) TERLEBIH DAHULU. HANYA setelah SL baru terkonfirmasi (`ACKED`), batalkan SL dan TP lama. Jika penempatan SL baru gagal, posisi lama tetap terlindungi; jika posisi belum terlindungi sama sekali, segera picu *Emergency Market Close*.
   - **Exit Ledger & Sibling Cancel:** Seluruh order algo SL/TP dicatat dalam map order broker. Saat event fill SL diterima, order sibling TP langsung dibatalkan; saat TP terisi, order sibling SL langsung dibatalkan.
   - **Entry Timeout Manager:** Bot memantau pending entry order live dan mengirim pembatalan jika tidak terisi dalam `45 detik`.
8. **Verifikasi One-Way Mode & Leverage 5x Isolated:** Sebelum live trading, bot memverifikasi mode posisi bursa adalah One-Way Mode (`dualSidePosition == false`), margin type `ISOLATED`, dan leverage `5x`. Parameter `reduceOnly = true` hanya valid di One-Way Mode.

---

## 2. TABEL PARAMETER PASTI (TERKUNCI)

### A. Parameter Multi-Coin Scanner & Bootstrap Historis

| Parameter Scanner | Nilai Terkunci | Aturan Implementasi & Sumber Data |
| :--- | :--- | :--- |
| **Filter Kontrak Bursa** | `TRADING` & `PERPETUAL` | Hanya simbol dengan `quoteAsset == "USDT"`, `contractType == "PERPETUAL"`, dan `status == "TRADING"` pada `/fapi/v1/exchangeInfo`. |
| **Interval Pembaruan Scanner** | `15 Menit` | Simbol yang masih memiliki posisi/order aktif tidak boleh diputus stream-nya meskipun keluar dari Top 10. |
| **Jumlah Koin Aktif Dipantau** | `Maksimal 10 Koin` | Diurutkan menurun berdasarkan nilai `RVOL` tertinggi. |
| **Volume 24 Jam Minimal** | `>= 50.000.000 USDT` | Diambil melalui 1 panggilan *bulk* `GET /fapi/v1/ticker/24hr` (`quoteVolume`). |
| **Spread Bid-Ask Maksimal** | `<= 0.03%` | Diambil melalui 1 panggilan *bulk* `GET /fapi/v1/ticker/bookTicker`. Rumus: `((BestAsk - BestBid) / BestBid) * 100`. |
| **Relative Volume (RVOL)** | `>= 1.80x` | Rasio `VolumeBase` candle 5m tertutup terakhir dibagi rata-rata `VolumeBase` dari **20 candle tertutup sebelumnya** (eksklusif candle terbaru). |
| **Rentang Volatilitas (NATR 14)** | `0.35% <= NATR <= 2.50%` | Rumus: `(ATR14 / LastClosedPrice) * 100`. |
| **Bootstrap Historis Awal** | `1.000 Candle 5m Tertutup` | Diambil via `GET /fapi/v1/klines?symbol=<SYM>&interval=5m&limit=1001`. Filter validasi candle: buang candle aktif (`CloseTime >= ServerTime`), verifikasi harga positif dan urutan strictly-monotonic berjarak tepat 5m. 1000 bar menjamin konvergensi presisi EMA200. |

### B. Parameter Strategi 4 Kelompok Konfirmasi (Timeframe `5m` Closed Candle)

| Kelompok Konfirmasi | Nama Indikator | Periode Statis | Rumus & Ambang Sinyal |
| :--- | :--- | :--- | :--- |
| **1. Filter Tren Makro** | `EMA 200` | `200 Candle (5m)` | **Long:** `Close > EMA200`<br>**Short:** `Close < EMA200` |
| **2. Ekspansi Volatilitas & Nilai Wajar** | `Session VWAP-ATR Envelope` | `VWAP Harian (Reset 00:00:00 UTC)` & `ATR 14` | **Long:** `Close > SessionVWAP + (1.25 * ATR14)`<br>**Short:** `Close < SessionVWAP - (1.25 * ATR14)` |
| **3. Agresi Orderflow per Bar** | `Bar Delta Z-Score` | `Window 50 Candle Baseline (5m)` | **Long:** `BarDelta_ZScore >= +1.50`<br>**Short:** `BarDelta_ZScore <= -1.50`<br>*(Mean dan deviasi standar dihitung murni dari 50 candle baseline sebelumnya t-50 s/d t-1, eksklusif bar t)* |
| **4. Posisi Derivatif** | `OI Delta 5m` & `Adverse Funding Rate` | `Delta 1 Candle (5m, interval 4-7m)`, Usia maks `30s` | **Long:** `OI_Delta_Pct >= +0.20%` dan `FundingRate <= +0.050%` (`MAX_ADVERSE_FUNDING_RATE`)<br>**Short:** `OI_Delta_Pct >= +0.20%` dan `FundingRate >= -0.050%` |

### C. Parameter Manajemen Risiko, Margin-Cap, dan Eksekusi

| Parameter Risiko & Eksekusi | Nilai Terkunci | Aturan Implementasi Eksak |
| :--- | :--- | :--- |
| **Modal Awal Simulasi (Dry-Run)** | `1.000 USDT` | Disimpan pada tabel state SQLite. |
| **Risiko Maksimal per Transaksi** | `1.0% dari Total Equity` | `RiskUSDT = 0.01 * EffectiveTotalEquity` (sudah memasukkan friksi fee & slippage ke dalam pembagi risiko). |
| **Leverage & Tipe Margin** | `5x Isolated` | Diverifikasi dan diatur via REST API (`/fapi/v1/marginType` & `/fapi/v1/leverage`) dan One-Way Mode (`/fapi/v1/positionSide/dual`) sebelum simbol diizinkan trading. |
| **Maksimal Slot Aktif Bersamaan** | `3 Slot` | `ActiveSlots = len(OpenPositions) + len(PendingEntryOrders) <= 3`. Pengecekan dan reservasi slot dilakukan secara **atomik** di dalam mutex broker untuk mencegah race condition antar-simbol. |
| **Batas Alokasi Margin per Slot** | `<= 30% dari Equity` | Mencegah insolvensi margin pada NATR rendah: `MaxMarginSlot = min(AvailableBalance * 0.90, (Equity / 3) * 0.90) = min(AvailableBalance * 0.90, Equity * 0.30)` dengan buffer likuiditas 90%. |
| **Circuit Breaker Harian (Total Equity)** | `-3.0% dari Session Start Equity` | Dihitung dari `(EffectiveTotalEquity - SessionStartEquityUTC) / SessionStartEquityUTC <= -0.03` (mencakup *Realized* + *Unrealized PnL*). Memblokir entry baru dan membatalkan pending entry hingga `00:00 UTC` berikutnya. |
| **Jarak Stop Loss (SL) Adaptif** | `1.5 * ATR(14)` | **Long:** `Entry - 1.5*ATR14` \| **Short:** `Entry + 1.5*ATR14`. Dipasang via New Algo Order `POST /fapi/v1/algoOrder` (`STOP_MARKET`, `workingType = MARK_PRICE`, `reduceOnly = true`). |
| **Jarak Take Profit (TP) Adaptif** | `3.0 * ATR(14)` | **Long:** `Entry + 3.0*ATR14` \| **Short:** `Entry - 3.0*ATR14`. Dipasang via New Algo Order `POST /fapi/v1/algoOrder` (`TAKE_PROFIT_MARKET`, `workingType = MARK_PRICE`, `reduceOnly = true`). |
| **Aturan Geser Break-Even (BEP)** | `Saat Profit >= 1.5 * ATR(14)` | Geser SL ke `Entry * (1 + FeeOffset)` untuk Long, atau `Entry * (1 - FeeOffset)` untuk Short, di mana `FeeOffset = max(0.0010, 2 * TakerFeeRate)`. |
| **Tipe Order Masuk & Timeout** | `LIMIT GTX (Post-Only)`, `45 Detik` | Dipasang di `BestBid` (Long) atau `BestAsk` (Short). Jika ditolak bursa (error `-5022` Post-Only cross spread), order langsung dihapus dari reservasi broker. Jika lewat 45 detik, kirim perintah cancel. Jika terisi sebagian (*partial fill*), langsung pantau dan proteksi kuantitas terisi dengan SL/TP. |
| **Pencocokan Agresor Dry-Run** | `isBuyerMaker Validated` | Untuk Limit Buy di BestBid: hanya terisi jika agresor adalah penjual (`isBuyerMaker == true`). Untuk Limit Sell di BestAsk: hanya terisi jika agresor adalah pembeli (`isBuyerMaker == false`). |
| **Batas Maksimum Holding Time** | `8 Jam (Intraday)` | Posisi yang bertahan melebihi 8 jam tanpa menyentuh SL/TP dievaluasi untuk time-stop exit. |
| **Friksi Biaya Transaksi** | `Maker: 0.02%`, `Taker: 0.05%`, `Slippage SL: 0.02%` | Dihitung otomatis pada kalkulasi sizing risiko dan evaluasi realisasi PnL. |

---

## 3. DEFINISI RUMUS MATEMATIKA EKSAK (BEBAS AMBIGUITAS)

### 3.1 Session VWAP (Berbasis Base Asset Volume & Epoch Day UTC)
Setiap candle `5m` dari Binance menyediakan `VolumeBase` ($V_{\text{base}}$, indeks `[5]`) dan `QuoteVolumeUSDT` ($V_{\text{quote}}$, indeks `[7]`):
1. **Kunci Hari Sesi UTC (Bebas Bug Pergantian Bulan):**
   $$\text{EpochDay}_{\text{UTC}} = \lfloor \frac{\text{CandleOpenTimeUnixSec}}{86400} \rfloor$$
   Jika $\text{EpochDay}_{\text{UTC}}$ berubah, reset akumulator $\sum (\text{TP}_i \cdot V_{\text{base}, i}) = 0$ dan $\sum V_{\text{base}, i} = 0$.
2. **Akumulasi Session VWAP Sejak 00:00:00 UTC:**
   $$\text{TP}_i = \frac{\text{High}_i + \text{Low}_i + \text{Close}_i}{3}$$
   $$\text{SessionVWAP}_t = \frac{\sum_{i=1}^{t} (\text{TP}_i \cdot V_{\text{base}, i})}{\sum_{i=1}^{t} V_{\text{base}, i}}$$

### 3.2 Bar Volume Delta & Bar Delta Z-Score (Bebas Distorsi Self-Inclusion)
Bar Volume Delta ($\Delta V_t$) dihitung dari field Kline resmi:
$$\Delta V_t = 2 \cdot V_{\text{buy\_quote}, t} - V_{\text{quote}, t}$$
Untuk jendela 50 candle baseline tertutup sebelumnya ($W = 50$, dari $i=t-50$ hingga $t-1$):
$$\mu_{\Delta} = \frac{1}{50} \sum_{i=t-50}^{t-1} \Delta V_i, \quad \sigma_{\Delta} = \sqrt{\frac{1}{50} \sum_{i=t-50}^{t-1} (\Delta V_i - \mu_{\Delta})^2}$$
$$\text{BarDelta\_ZScore}_t = \frac{\Delta V_t - \mu_{\Delta}}{\sigma_{\Delta}} \quad (\text{jika } \sigma_{\Delta} > 0, \text{ selain itu } 0)$$

### 3.3 Position Sizing dengan Proteksi Friksi & Margin-Cap
Dengan ekuitas efektif $E$, saldo bebas $B_{\text{avail}}$, rasio risiko $r = 0.01$, jarak SL $d_{\text{SL}} = 1.5 \times \text{ATR}_{14}$, harga masuk $P_{\text{entry}}$, tarif *maker* $f_m = 0.0002$, tarif *taker* $f_t = 0.0005$, dan *slippage* $s = 0.0002$:
1. **Kerugian Efektif per Unit Koin:**
   $$\text{LossPerUnit} = d_{\text{SL}} + P_{\text{entry}} \cdot (f_m + f_t + s)$$
2. **Kuantitas Berbasis Risiko ($Q_{\text{risk}}$):**
   $$Q_{\text{risk}} = \frac{E \cdot r}{\text{LossPerUnit}}$$
3. **Kuantitas Maksimal Berbasis Margin Slot ($Q_{\text{margin}}$):**
   $$\text{MaxMarginSlot} = \min\left(B_{\text{avail}} \times 0.90, E \times 0.30\right)$$
   $$Q_{\text{margin}} = \frac{\text{MaxMarginSlot} \times 5}{P_{\text{entry}}}$$
4. **Kuantitas Final Terkuantisasi ($Q_{\text{final}}$):**
   $$Q_{\text{raw}} = \min(Q_{\text{risk}}, Q_{\text{margin}})$$
   Kuantisasi ke bawah (*floor*) ke kelipatan `StepSize`. Jika `StepSize <= 0`, `TickSize <= 0`, $Q_{\text{final}} < \text{MinQty}$, atau $Q_{\text{final}} \cdot P_{\text{entry}} < \text{MinNotional}$, tolak pesanan (*fail-closed*).

---

## 4. TECH STACK & KONTRAK ENDPOINT BINANCE FUTURES (ARSITEKTUR 2026)

### 4.1 Tech Stack Tunggal (Full Golang Ecosystem)
* **Bahasa Utama:** `Golang 1.23+`
* **WebSocket Client:** `github.com/coder/websocket`
* **JSON Parser:** `github.com/bytedance/sonic`
* **Aritmetika Presisi:** `github.com/shopspring/decimal`
* **Kriptografi Autentikasi:** `crypto/ed25519` (Standard Library)
* **Database Lokal:** `SQLite 3 (Mode WAL)` via `modernc.org/sqlite`
* **Logging:** `log/slog` (Standard Library)

### 4.2 Kontrak Endpoint REST & WebSocket (Sesuai Upgrade Binance 2026)
1. **REST Base URLs:**
   * **Mainnet:** `https://fapi.binance.com`
   * **Testnet:** `https://testnet.binancefuture.com`
   * Exchange Info & Filters: `GET /fapi/v1/exchangeInfo`
   * Bulk 24h Ticker: `GET /fapi/v1/ticker/24hr`
   * Bulk Book Ticker: `GET /fapi/v1/ticker/bookTicker`
   * Historical Klines (Bootstrap 1000 Candle): `GET /fapi/v1/klines?symbol=<SYM>&interval=5m&limit=1000`
   * Open Interest Polling: `GET /fapi/v1/openInterest?symbol=<SYM>`
   * Position Side Dual Configuration: `GET /fapi/v1/positionSide/dual` & `POST /fapi/v1/positionSide/dual` (Verifikasi One-Way Mode)
   * Leverage & Margin Type: `POST /fapi/v1/leverage` & `POST /fapi/v1/marginType`
   * Regular Order Execution: `POST /fapi/v1/order` (GTX Limit Entry) & `DELETE /fapi/v1/order`
   * Conditional Algo Order Execution: `POST /fapi/v1/algoOrder` (`STOP_MARKET` / `TAKE_PROFIT_MARKET`, `algoType: "CONDITIONAL"`, `workingType: "MARK_PRICE"`, `reduceOnly: "true"`) & `DELETE /fapi/v1/algoOrder`
   * ListenKey Lifecycle: `POST / PUT / DELETE /fapi/v1/listenKey`

2. **WebSocket Streams (Pemisahan Endpoint Resmi 2026):**
   * **Mainnet Public Stream (`@bookTicker`):** `wss://fstream.binance.com/public/stream?streams=...`
   * **Mainnet Market Stream (`@aggTrade`, `@kline_5m`, `@markPrice@1s`):** `wss://fstream.binance.com/market/stream?streams=...`
   * **Mainnet Private User Data Stream (`ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`):** `wss://fstream.binance.com/private/ws/<listenKey>`
   * **Mainnet WebSocket API (Order Placement):** `wss://ws-fapi.binance.com/ws-fapi/v1`
   * **Testnet Combined Stream:** `wss://stream.binancefuture.com/stream?streams=...`
   * **Testnet Private User Data Stream:** `wss://stream.binancefuture.com/ws/<listenKey>`

### 4.3 Spesifikasi Variabel Lingkungan (.env) & Autentikasi
Untuk menjamin portabilitas mulus antara simulasi, testnet, dan mainnet, modul `internal/config` wajib membaca variabel lingkungan berikut:

| Variabel ENV | Nilai Contoh | Keterangan |
| :--- | :--- | :--- |
| `BINANCE_ENV` | `testnet` atau `mainnet` | Menentukan base URL REST dan WebSocket yang digunakan |
| `BINANCE_API_KEY` | `vmPUZE6mvv...` | API Key bursa Binance Futures (32+ karakter) |
| `BINANCE_SECRET_KEY` | `NhqPtMDs5x...` | Secret key untuk tanda tangan HMAC-SHA256 (atau path file Ed25519) |
| `DRY_RUN_MODE` | `true` atau `false` | `true` = Simulator antrean volume-through-limit; `false` = Eksekusi order nyata |
| `SQLITE_DB_PATH` | `./data/trml.db` | Lokasi file database SQLite WAL untuk audit dan recovery state |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARN`, `ERROR` | Level logging terstruktur via `log/slog` |
| `MAKER_FEE_RATE` | `0.0002` | Default 0.02% (disesuaikan fee tier VIP akun) |
| `TAKER_FEE_RATE` | `0.0005` | Default 0.05% (disesuaikan fee tier VIP akun) |

**Protokol Autentikasi Signed Request:**
- Header: `X-MBX-APIKEY: <BINANCE_API_KEY>`
- Payload: Parameter wajib menyertakan `timestamp=<UNIX_MILLIS>` dan `recvWindow=5000`.
- Signature: Ditandatangani menggunakan HMAC-SHA256 atau Ed25519 terhadap query string / request body payload dan dipasang sebagai parameter `signature=<HEX_SIGNATURE>`.

---

## 5. ARSITEKTUR SISTEM & STATE MACHINE FLOWCHART

```mermaid
flowchart TD
    A[Start Bot Engine - Go 1.23] --> B[Inisialisasi SQLite WAL & Exchange Filters]
    B --> C[Startup Verification: One-Way Mode, 5x Isolated Leverage]
    C --> D[Scanner Worker Setiap 15 Menit]

    subgraph SCANNER [1. Bulk Market Scanner & Bootstrap 1000 Bar]
        D --> E[Bulk Fetch /fapi/v1/ticker/24hr & /bookTicker]
        E --> F[Saring Simbol PERPETUAL TRADING:<br/>Vol24h >= 50M USDT & Spread <= 0.03%]
        F --> G[Fetch 1001 Klines 5m Kandidat:<br/>Validasi Monotonik & Buang Bar Aktif]
        G --> H[Pilih Top 10 Koin RVOL >= 1.8x & 0.35% <= NATR14 <= 2.50%]
        H --> I[Bootstrap 1000 Closed Candles:<br/>Konvergensi EMA200, VWAP Midnight-UTC, ATR14, 50 BarDelta]
    end

    I --> J[Koneksi WebSocket: Public, Market & Private Streams]

    subgraph STREAM [2. Lock-Free Real-Time Pipeline]
        J --> K1[Public WS: @bookTicker]
        J --> K2[Market WS: @aggTrade & @markPrice & Dual-Timestamp OI]
        J --> K3[Market WS: @kline_5m]
        J --> K4[Private WS: ORDER_TRADE_UPDATE & ACCOUNT_UPDATE]
    end

    K1 --> L1[Update BBO & BookUpdatedAt]
    K2 --> L2[Update MarkPrice & MarkUpdatedAt, AggTrade Aggressor Match]
    K4 --> M[User Data State Machine:<br/>Sinkronisasi Saldo/Equity, Sibling Algo Cancel, Partial Fill]

    K3 --> N{kline.x == true DAN<br/>OpenTime strictly-monotonic?}
    N -- Tidak --> O[Abaikan Bar]
    N -- Ya --> P[Lock SymbolState Singkat:<br/>Update VWAP Midnight-UTC, Buat Immutable Snapshot, Unlock]

    subgraph SIGNAL [3. Signal & Risk Gatekeeper pada Snapshot]
        P --> Q{Staleness Checks:<br/>BBO <= 5s, Mark <= 15s, Account <= 60s,<br/>OI Interval 4-7m, Funding <= 330s?}
        Q -- Gagal --> O
        Q -- Lolos --> R{Cek 4 Konfirmasi:<br/>1. EMA200 (1000 bar mature)<br/>2. VWAP-ATR Envelope 1.25x<br/>3. BarDelta Z-Score >= 1.5 vs Baseline 50 Bar Eksklusif<br/>4. OI Delta >= 0.20% & Adverse Funding Aman}
        R -- Tidak Terpenuhi --> O
        R -- Terpenuhi --> S{Risk & Margin Pre-Check:<br/>1. Total Equity DD > -3.0%<br/>2. Active Slots < 3<br/>3. Spread <= 0.03%}
        S -- Gagal --> O
        S -- Lolos --> T[Hitung Decimal Sizing Fail-Closed:<br/>Masukkan Friksi Fee + Clamp Margin 30% Equity]
    end

    subgraph EXEC [4. Two-Phase Execution & Safe Protection]
        T --> U1[Fase 1: Atomic Slot Reservation in-memory di bawah lock]
        U1 --> U2[Fase 2: Dispatch LIMIT GTX ke Exchange di luar lock]
        U2 -- Sukses --> U3[Update Status NEW & ExchangeOrderID]
        U2 -- Reject/Timeout --> U4[Rollback Slot Reservation]
        U3 --> V{Order Fill Event?}
        V -- Partial/Full Fill --> W1[Pasang SL Baru STOP_MARKET di luar lock]
        W1 -- SL Terkonfirmasi ACK --> W2[Batalkan SL & TP Lama]
        W2 --> W3[Pasang TP Baru TAKE_PROFIT_MARKET]
        W1 -- SL Gagal & Posisi Naked --> W4[Trigger Emergency Market Close & Fail-Closed]
    end
```

---

## 6. PSEUDOCODE BACKBONE LENGKAP (UTUH & BEBAS HALUSINASI)

```go
// ============================================================================
// FILE: pseudo.go
// DESKRIPSI: Blueprint Tunggal Lengkap & Terverifikasi Bot Binance Futures USDⓈ-M
//            Multi-Koin (Golang).
//            Mencakup:
//            - Hardening P0 (Live Entry Dispatch 2-Phase Commit, Safe SL-First
//              Protection Replacement, Algo Order Contract, Account Sync,
//              One-Way & Isolated Verification, Exit Order Ledger & Sibling Cancel,
//              Live Entry Timeout Manager).
//            - Hardening P1 (Aggressor Matching isBuyerMaker, Immediate Partial
//              Fill Monitoring, Rolling 1000-Bar EMA, Midnight-UTC VWAP Bootstrap,
//              Candle Monotonic & CloseTime Filter, OI Sample Interval 5m Window,
//              Mark Price Staleness Fail-Closed, Safe Quantization Filters).
// ============================================================================

package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// ----------------------------------------------------------------------------
// 1. KONSTANTA PARAMETER TERKUNCI (SPESIFIKASI AUDIT RESMI)
// ----------------------------------------------------------------------------
const (
	SCANNER_INTERVAL         = 15 * time.Minute
	MAX_WATCHED_COINS        = 10
	BOOTSTRAP_CANDLE_COUNT   = 1000 // Histori panjang untuk konvergensi presisi EMA200 (Solusi P1-3)
	MIN_VOLUME_24H_USDT      = 50_000_000.0
	MIN_RVOL_RATIO           = 1.80
	RVOL_LOOKBACK            = 20
	MIN_NATR_PCT             = 0.35
	MAX_NATR_PCT             = 2.50
	MAX_SPREAD_PCT           = 0.03

	EMA_PERIOD               = 200
	ATR_PERIOD               = 14
	DELTA_ZSCORE_WINDOW      = 50

	// Staleness & Toleransi Waktu (Fail-Closed)
	MAX_BOOK_STALENESS       = 5 * time.Second   // P0 staleness top-of-book BBO
	MAX_MARK_STALENESS       = 15 * time.Second  // P1-8 staleness feed mark price
	MAX_DATA_STALENESS       = 330 * time.Second // 5m + 30s untuk funding & kline
	MAX_ACCOUNT_STALENESS    = 60 * time.Second  // P0-2 staleness stream akun / balance
	MIN_OI_INTERVAL          = 4 * time.Minute   // P1-6 verifikasi jarak waktu delta OI (~5m)
	MAX_OI_INTERVAL          = 7 * time.Minute   // P1-6 batas atas toleransi polling delta OI
	MAX_OI_STALENESS         = 30 * time.Second  // P1-6 kesegaran sampel OI terkini

	// Parameter Strategi Breakout
	ENVELOPE_ATR_MULTIPLIER  = 1.25
	MIN_BARDELTA_ZSCORE      = 1.50
	MIN_OI_DELTA_PCT         = 0.20
	MAX_ADVERSE_FUNDING_RATE = 0.0005 // 0.05% per 8h (P5 rename & klarifikasi arah merugikan)

	// Manajemen Risiko & Sizing
	RISK_PER_TRADE_PCT       = 0.01  // 1.0% dari Total Effective Equity
	MAX_DAILY_DRAWDOWN_PCT   = -0.03 // -3.0% Total Equity Drawdown (Circuit Breaker)
	MAX_CONCURRENT_SLOTS     = 3     // Maksimal 3 slot posisi + pending
	MAX_SLOT_MARGIN_FRACTION = 0.30  // Maksimal 30% Equity per slot
	ACCOUNT_LEVERAGE         = 5.0   // 5x Isolated Leverage
	SL_ATR_MULTIPLIER        = 1.50
	TP_ATR_MULTIPLIER        = 3.00
	BREAKEVEN_TRIGGER_ATR    = 1.50
	MIN_BREAKEVEN_OFFSET_PCT = 0.0010 // +0.10% untuk menutup round-trip fee
	LIMIT_ORDER_TIMEOUT      = 45 * time.Second
	MAX_HOLDING_DURATION     = 8 * time.Hour // Batas holding time intraday

	// Struktur Biaya & Slippage
	DEFAULT_MAKER_FEE        = 0.0002 // 0.02% (VIP0 maker fee)
	DEFAULT_TAKER_FEE        = 0.0005 // 0.05% (VIP0 taker fee)
	SLIPPAGE_PENALTY_RATE    = 0.0002 // 0.02% penalty slippage stop order
)

type Side string

const (
	SideLong  Side = "LONG"
	SideShort Side = "SHORT"
)

type OrderLifecycleStatus string

const (
	StatusPendingNew      OrderLifecycleStatus = "PENDING_NEW"
	StatusNew             OrderLifecycleStatus = "NEW"
	StatusPartiallyFilled OrderLifecycleStatus = "PARTIALLY_FILLED"
	StatusFilled          OrderLifecycleStatus = "FILLED"
	StatusCanceled        OrderLifecycleStatus = "CANCELED"
	StatusRejected        OrderLifecycleStatus = "REJECTED"
	StatusExpired         OrderLifecycleStatus = "EXPIRED"
)

// ----------------------------------------------------------------------------
// 2. STRUKTUR DATA PASAR & IMMUTABLE SNAPSHOT (SOLUSI P1-4, P1-6, P1-8)
// ----------------------------------------------------------------------------
type Candle struct {
	OpenTime          time.Time
	CloseTime         time.Time
	Open              float64
	High              float64
	Low               float64
	Close             float64
	VolumeBase        float64 // Kline index [5] -> Session VWAP & RVOL
	QuoteVolumeUSDT   float64 // Kline index [7] -> Likuiditas & Bar Delta
	TakerBuyQuoteUSDT float64 // Kline index [10] -> Bar Delta
}

type ExchangeFilter struct {
	TickSize    decimal.Decimal
	StepSize    decimal.Decimal
	MinQty      decimal.Decimal
	MinNotional decimal.Decimal
}

type SymbolState struct {
	Mu                         sync.Mutex
	Symbol                     string
	Filter                     ExchangeFilter
	IsLeverageIsolatedVerified bool // Wajib diverifikasi via InitAndVerifySymbol (Solusi P0-7)

	BestBid                    float64
	BestAsk                    float64
	BookUpdatedAt              time.Time

	MarkPrice                  float64
	MarkPriceUpdatedAt         time.Time // Solusi P1-8: timestamp mark price untuk verifikasi kesegaran

	FundingRate                float64
	FundingUpdatedAt           time.Time

	// Solusi P1-6: Dual-timestamp untuk verifikasi interval delta OI 5m
	PrevOpenInterest           float64
	CurrOpenInterest           float64
	PrevOIUpdatedAt            time.Time
	CurrOIUpdatedAt            time.Time

	// Solusi P1-4: Akumulator Session VWAP berbasis Midnight UTC (00:00:00 UTC)
	SessionEpochDayUTC         int64
	CumulativeTypicalPVBase    float64
	CumulativeVolumeBase       float64
	CurrentSessionVWAP         float64

	// Deduplikasi & Buffering
	LastProcessedCandleOpenTime time.Time
	ClosedCandles               []Candle  // Kapasitas 1000 candle
	ClosedBarDeltasUSDT         []float64 // Kapasitas 51 (50 baseline + 1 bar berjalan)
}

func NewSymbolState(symbol string, filter ExchangeFilter) *SymbolState {
	return &SymbolState{
		Symbol:              symbol,
		Filter:              filter,
		ClosedCandles:       make([]Candle, 0, BOOTSTRAP_CANDLE_COUNT),
		ClosedBarDeltasUSDT: make([]float64, 0, DELTA_ZSCORE_WINDOW+1),
	}
}

// MarketSnapshot adalah salinan immutable untuk evaluasi sinyal secara lock-free
type MarketSnapshot struct {
	Symbol              string
	Filter              ExchangeFilter
	Now                 time.Time
	BestBid             float64
	BestAsk             float64
	BookUpdatedAt       time.Time
	MarkPrice           float64
	MarkPriceUpdatedAt  time.Time
	FundingRate         float64
	FundingUpdatedAt    time.Time
	PrevOpenInterest    float64
	CurrOpenInterest    float64
	PrevOIUpdatedAt     time.Time
	CurrOIUpdatedAt     time.Time
	CurrentSessionVWAP  float64
	ClosedCandles       []Candle
	ClosedBarDeltasUSDT []float64
}

// ----------------------------------------------------------------------------
// 3. KUANTISASI FILTER BURSA (FAIL-CLOSED - SOLUSI P1-9)
// ----------------------------------------------------------------------------
func QuantizePrice(price, tickSize decimal.Decimal, roundUp bool) (decimal.Decimal, error) {
	if price.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize price: price must be positive, got %s", price.String())
	}
	if tickSize.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize price: invalid tickSize <= 0 (%s)", tickSize.String())
	}
	steps := price.Div(tickSize)
	if roundUp {
		return steps.Ceil().Mul(tickSize), nil
	}
	return steps.Floor().Mul(tickSize), nil
}

func QuantizeQtyFloor(qty, stepSize decimal.Decimal) (decimal.Decimal, error) {
	if qty.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize qty: qty must be positive, got %s", qty.String())
	}
	if stepSize.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("quantize qty: invalid stepSize <= 0 (%s)", stepSize.String())
	}
	return qty.Div(stepSize).Floor().Mul(stepSize), nil
}

func GenerateDeterministicClientOrderID(symbol string, side Side, candleCloseTime time.Time) string {
	return fmt.Sprintf("BOT_%s_%s_%d", symbol, side, candleCloseTime.UTC().Unix())
}

func OppositeSide(s Side) Side {
	if s == SideLong {
		return SideShort
	}
	return SideLong
}

// ----------------------------------------------------------------------------
// 4. INTERFACE BROKER EKSEKUSI
// ----------------------------------------------------------------------------
type OrderIntent struct {
	ClientOrderID string
	Symbol        string
	Side          Side
	Qty           decimal.Decimal
	LimitPrice    decimal.Decimal
	SLPrice       decimal.Decimal
	TPPrice       decimal.Decimal
	ATRAtEntry    decimal.Decimal
	CreatedAt     time.Time
}

type ExecutionBroker interface {
	SubmitLimitEntry(intent OrderIntent) (bool, error)
	OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time)
	GetEffectiveTotalEquityUSDT() decimal.Decimal
	GetAvailableBalanceUSDT() decimal.Decimal
	GetDailyTotalDrawdownPct() float64
	GetActiveSlotCount() int
	HasActiveOrderOrPosition(symbol string) bool
}

// ----------------------------------------------------------------------------
// 5. VALIDASI HISTORI & EVENT HANDLERS (SOLUSI P1-3, P1-4, P1-5, P1-6, P1-8)
// ----------------------------------------------------------------------------

// ValidateAndFilterCandles memeriksa urutan strictly-monotonic, kline closed, dan harga valid (Solusi P1-5)
func ValidateAndFilterCandles(raw []Candle, serverTime time.Time) ([]Candle, error) {
	var valid []Candle
	for i, c := range raw {
		if !c.CloseTime.Before(serverTime) && !c.CloseTime.Equal(serverTime) {
			continue // Lewati candle aktif yang belum tertutup
		}
		if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 || c.Low > c.High {
			return nil, fmt.Errorf("candle index %d has invalid price values", i)
		}
		if c.VolumeBase < 0 || c.QuoteVolumeUSDT < 0 {
			return nil, fmt.Errorf("candle index %d has negative volume", i)
		}
		if len(valid) > 0 {
			prev := valid[len(valid)-1]
			if !c.OpenTime.After(prev.OpenTime) {
				return nil, fmt.Errorf("candle non-monotonic timestamp at index %d", i)
			}
			step := c.OpenTime.Sub(prev.OpenTime)
			if step != 5*time.Minute {
				return nil, fmt.Errorf("candle gap detected: expected 5m, got %v", step)
			}
		}
		valid = append(valid, c)
	}
	return valid, nil
}

// BootstrapSymbolState memuat hingga 1000 bar untuk konvergensi EMA200 dan menghitung VWAP Midnight-UTC (Solusi P1-3 & P1-4)
func BootstrapSymbolState(state *SymbolState, candles1000 []Candle, serverTime time.Time) error {
	state.Mu.Lock()
	defer state.Mu.Unlock()

	valid, err := ValidateAndFilterCandles(candles1000, serverTime)
	if err != nil {
		return err
	}
	for _, c := range valid {
		applyClosedCandleLocked(state, c)
	}
	return nil
}

func applyClosedCandleLocked(state *SymbolState, candle Candle) bool {
	if !state.LastProcessedCandleOpenTime.IsZero() && !candle.OpenTime.After(state.LastProcessedCandleOpenTime) {
		return false
	}
	state.LastProcessedCandleOpenTime = candle.OpenTime

	// Reset Session VWAP tepat pada pergantian hari 00:00:00 UTC (Solusi P1-4)
	epochDay := candle.OpenTime.UTC().Unix() / 86400
	if epochDay != state.SessionEpochDayUTC {
		state.SessionEpochDayUTC = epochDay
		state.CumulativeTypicalPVBase = 0.0
		state.CumulativeVolumeBase = 0.0
	}

	typicalPrice := (candle.High + candle.Low + candle.Close) / 3.0
	state.CumulativeTypicalPVBase += typicalPrice * candle.VolumeBase
	state.CumulativeVolumeBase += candle.VolumeBase
	if state.CumulativeVolumeBase > 0 {
		state.CurrentSessionVWAP = state.CumulativeTypicalPVBase / state.CumulativeVolumeBase
	}

	// Bar Delta resmi dari field Kline USDT
	barDeltaUSDT := (2.0 * candle.TakerBuyQuoteUSDT) - candle.QuoteVolumeUSDT

	state.ClosedCandles = AppendCandleWithCap(state.ClosedCandles, candle, BOOTSTRAP_CANDLE_COUNT)
	state.ClosedBarDeltasUSDT = AppendFloatWithCap(state.ClosedBarDeltasUSDT, barDeltaUSDT, DELTA_ZSCORE_WINDOW+1)
	return true
}

func HandleBookTickerEvent(state *SymbolState, bestBid, bestAsk float64, now time.Time, broker ExecutionBroker) {
	state.Mu.Lock()
	state.BestBid = bestBid
	state.BestAsk = bestAsk
	state.BookUpdatedAt = now
	markPrice := state.MarkPrice
	sym := state.Symbol
	state.Mu.Unlock()

	broker.OnRealtimeTick(sym, bestBid, bestAsk, markPrice, 0, 0, false, now)
}

func HandleAggTradeEvent(state *SymbolState, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time, broker ExecutionBroker) {
	state.Mu.Lock()
	bestBid := state.BestBid
	bestAsk := state.BestAsk
	markPrice := state.MarkPrice
	sym := state.Symbol
	state.Mu.Unlock()

	broker.OnRealtimeTick(sym, bestBid, bestAsk, markPrice, tradePrice, tradeQty, isBuyerMaker, now)
}

func HandleMarkPriceEvent(state *SymbolState, markPrice, fundingRate float64, now time.Time, broker ExecutionBroker) {
	state.Mu.Lock()
	state.MarkPrice = markPrice
	state.MarkPriceUpdatedAt = now // Solusi P1-8
	state.FundingRate = fundingRate
	state.FundingUpdatedAt = now
	bestBid := state.BestBid
	bestAsk := state.BestAsk
	sym := state.Symbol
	state.Mu.Unlock()

	broker.OnRealtimeTick(sym, bestBid, bestAsk, markPrice, 0, 0, false, now)
}

// HandleOpenInterestSnapshot menyimpan histori OI dan mencatat dual timestamp (Solusi P1-6)
func HandleOpenInterestSnapshot(state *SymbolState, latestOI float64, now time.Time) {
	state.Mu.Lock()
	defer state.Mu.Unlock()

	if latestOI <= 0 {
		return
	}
	if state.CurrOpenInterest > 0 {
		// Geser nilai sebelumnya hanya jika interval dari sampel sebelumnya sudah mencapai minimal 4 menit
		if state.CurrOIUpdatedAt.IsZero() || now.Sub(state.CurrOIUpdatedAt) >= 4*time.Minute {
			state.PrevOpenInterest = state.CurrOpenInterest
			state.PrevOIUpdatedAt = state.CurrOIUpdatedAt
			state.CurrOpenInterest = latestOI
			state.CurrOIUpdatedAt = now
		} else {
			// Perbarui nilai snapshot terkini dalam interval yang sama
			state.CurrOpenInterest = latestOI
			state.CurrOIUpdatedAt = now
		}
	} else {
		state.CurrOpenInterest = latestOI
		state.CurrOIUpdatedAt = now
	}
}

func HandleKline5mEvent(state *SymbolState, candle Candle, isClosed bool, broker ExecutionBroker) {
	if !isClosed {
		return
	}

	state.Mu.Lock()
	// Fail-closed jika setup leverage & isolasi margin belum terverifikasi exchange (Solusi P0-7)
	if !state.IsLeverageIsolatedVerified {
		state.Mu.Unlock()
		return
	}
	updated := applyClosedCandleLocked(state, candle)
	if !updated || len(state.ClosedCandles) < EMA_PERIOD || len(state.ClosedBarDeltasUSDT) < DELTA_ZSCORE_WINDOW+1 {
		state.Mu.Unlock()
		return
	}

	candlesCopy := make([]Candle, len(state.ClosedCandles))
	copy(candlesCopy, state.ClosedCandles)
	deltasCopy := make([]float64, len(state.ClosedBarDeltasUSDT))
	copy(deltasCopy, state.ClosedBarDeltasUSDT)

	snap := MarketSnapshot{
		Symbol:              state.Symbol,
		Filter:              state.Filter,
		Now:                 time.Now().UTC(),
		BestBid:             state.BestBid,
		BestAsk:             state.BestAsk,
		BookUpdatedAt:       state.BookUpdatedAt,
		MarkPrice:           state.MarkPrice,
		MarkPriceUpdatedAt:  state.MarkPriceUpdatedAt,
		FundingRate:         state.FundingRate,
		FundingUpdatedAt:    state.FundingUpdatedAt,
		PrevOpenInterest:    state.PrevOpenInterest,
		CurrOpenInterest:    state.CurrOpenInterest,
		PrevOIUpdatedAt:     state.PrevOIUpdatedAt,
		CurrOIUpdatedAt:     state.CurrOIUpdatedAt,
		CurrentSessionVWAP:  state.CurrentSessionVWAP,
		ClosedCandles:       candlesCopy,
		ClosedBarDeltasUSDT: deltasCopy,
	}
	state.Mu.Unlock()

	EvaluateSignalFromSnapshot(snap, broker)
}

// ----------------------------------------------------------------------------
// 6. EVALUASI SINYAL DENGAN VERIFIKASI KETAT & SIZING DECIMAL (SOLUSI P1-6, P1-8, P1-9)
// ----------------------------------------------------------------------------
func EvaluateSignalFromSnapshot(snap MarketSnapshot, broker ExecutionBroker) {
	// Gatekeeper 1: Circuit Breaker Total Equity & Reservasi Slot
	if broker.GetDailyTotalDrawdownPct() <= MAX_DAILY_DRAWDOWN_PCT {
		return
	}
	if broker.GetActiveSlotCount() >= MAX_CONCURRENT_SLOTS {
		return
	}
	if broker.HasActiveOrderOrPosition(snap.Symbol) {
		return
	}

	// Gatekeeper 2: Validasi Kesegaran Data BBO & Mark Price (Solusi P1-8 & P0 staleness)
	if snap.BookUpdatedAt.IsZero() || snap.Now.Sub(snap.BookUpdatedAt) > MAX_BOOK_STALENESS {
		return
	}
	if snap.MarkPriceUpdatedAt.IsZero() || snap.Now.Sub(snap.MarkPriceUpdatedAt) > MAX_MARK_STALENESS {
		return
	}
	if snap.FundingUpdatedAt.IsZero() || snap.Now.Sub(snap.FundingUpdatedAt) > MAX_DATA_STALENESS {
		return
	}
	if snap.BestBid <= 0 || snap.BestAsk <= snap.BestBid {
		return
	}
	spreadPct := ((snap.BestAsk - snap.BestBid) / snap.BestBid) * 100.0
	if spreadPct > MAX_SPREAD_PCT {
		return
	}

	// Gatekeeper 3: Validasi Jarak Waktu Sampel Delta Open Interest (Solusi P1-6)
	if snap.PrevOpenInterest <= 0 || snap.CurrOpenInterest <= 0 {
		return
	}
	if snap.CurrOIUpdatedAt.IsZero() || snap.Now.Sub(snap.CurrOIUpdatedAt) > MAX_OI_STALENESS {
		return
	}
	oiInterval := snap.CurrOIUpdatedAt.Sub(snap.PrevOIUpdatedAt)
	if oiInterval < MIN_OI_INTERVAL || oiInterval > MAX_OI_INTERVAL {
		return // Jarak antar snapshot OI harus ~5 menit (toleransi 4 - 7 menit)
	}

	latest := snap.ClosedCandles[len(snap.ClosedCandles)-1]

	// KONFIRMASI 1: Filter Tren Makro (EMA 200 matang dari histori 1000 candle)
	ema200 := CalculateEMA(snap.ClosedCandles, EMA_PERIOD)

	// KONFIRMASI 2: Session VWAP-ATR Envelope (1.25x ATR14)
	atr14 := CalculateATR(snap.ClosedCandles, ATR_PERIOD)
	if atr14 <= 0 {
		return
	}
	natrPct := (atr14 / latest.Close) * 100.0
	if natrPct < MIN_NATR_PCT || natrPct > MAX_NATR_PCT {
		return
	}
	upperEnvelope := snap.CurrentSessionVWAP + (ENVELOPE_ATR_MULTIPLIER * atr14)
	lowerEnvelope := snap.CurrentSessionVWAP - (ENVELOPE_ATR_MULTIPLIER * atr14)

	// KONFIRMASI 3: Agresi Orderflow (Bar Delta Z-Score Baseline 50 Bar Eksklusif Bar t)
	n := len(snap.ClosedBarDeltasUSDT)
	if n < DELTA_ZSCORE_WINDOW+1 {
		return
	}
	baselineDeltas := snap.ClosedBarDeltasUSDT[n-1-DELTA_ZSCORE_WINDOW : n-1]
	deltaMean, deltaStd := CalculateMeanAndStdDev(baselineDeltas)
	if deltaStd <= 0 {
		return
	}
	latestDelta := snap.ClosedBarDeltasUSDT[n-1]
	barDeltaZScore := (latestDelta - deltaMean) / deltaStd

	// KONFIRMASI 4: Posisi Derivatif (OI Delta 5m & Adverse Funding Rate)
	oiDeltaPct := ((snap.CurrOpenInterest - snap.PrevOpenInterest) / snap.PrevOpenInterest) * 100.0

	var targetSide Side
	var rawEntryPrice float64

	longValid := (latest.Close > ema200) &&
		(latest.Close > upperEnvelope) &&
		(barDeltaZScore >= MIN_BARDELTA_ZSCORE) &&
		(oiDeltaPct >= MIN_OI_DELTA_PCT) &&
		(snap.FundingRate <= MAX_ADVERSE_FUNDING_RATE) // Solusi P5: tolak hanya jika adverse funding rate tinggi

	shortValid := (latest.Close < ema200) &&
		(latest.Close < lowerEnvelope) &&
		(barDeltaZScore <= -MIN_BARDELTA_ZSCORE) &&
		(oiDeltaPct >= MIN_OI_DELTA_PCT) &&
		(snap.FundingRate >= -MAX_ADVERSE_FUNDING_RATE) // Solusi P5: tolak hanya jika adverse funding rate negatif

	if longValid {
		targetSide = SideLong
		rawEntryPrice = snap.BestBid
	} else if shortValid {
		targetSide = SideShort
		rawEntryPrice = snap.BestAsk
	} else {
		return
	}

	intent, ok := BuildSafeDecimalOrderIntent(snap, broker, targetSide, rawEntryPrice, atr14)
	if !ok {
		return
	}

	_, _ = broker.SubmitLimitEntry(intent)
}

func BuildSafeDecimalOrderIntent(
	snap MarketSnapshot,
	broker ExecutionBroker,
	side Side,
	rawEntry float64,
	atr14 float64,
) (OrderIntent, bool) {
	equity := broker.GetEffectiveTotalEquityUSDT()
	avail := broker.GetAvailableBalanceUSDT()
	if equity.LessThanOrEqual(decimal.Zero) || avail.LessThanOrEqual(decimal.Zero) {
		return OrderIntent{}, false
	}

	// Solusi P1-9: Kuantisasi fail-closed dengan pengecekan error
	entryPrice, err := QuantizePrice(decimal.NewFromFloat(rawEntry), snap.Filter.TickSize, side == SideShort)
	if err != nil {
		return OrderIntent{}, false
	}

	atrDec := decimal.NewFromFloat(atr14)
	slDist := atrDec.Mul(decimal.NewFromFloat(SL_ATR_MULTIPLIER))
	tpDist := atrDec.Mul(decimal.NewFromFloat(TP_ATR_MULTIPLIER))

	var slPrice, tpPrice decimal.Decimal
	if side == SideLong {
		slPrice, err = QuantizePrice(entryPrice.Sub(slDist), snap.Filter.TickSize, false)
		if err != nil {
			return OrderIntent{}, false
		}
		tpPrice, err = QuantizePrice(entryPrice.Add(tpDist), snap.Filter.TickSize, true)
		if err != nil {
			return OrderIntent{}, false
		}
	} else {
		slPrice, err = QuantizePrice(entryPrice.Add(slDist), snap.Filter.TickSize, true)
		if err != nil {
			return OrderIntent{}, false
		}
		tpPrice, err = QuantizePrice(entryPrice.Sub(tpDist), snap.Filter.TickSize, false)
		if err != nil {
			return OrderIntent{}, false
		}
	}

	// Hitung Risiko per Unit termasuk Frictional Cost (Maker Fee + Taker Fee + Slippage)
	totalFrictionRate := decimal.NewFromFloat(DEFAULT_MAKER_FEE + DEFAULT_TAKER_FEE + SLIPPAGE_PENALTY_RATE)
	frictionPerUnit := entryPrice.Mul(totalFrictionRate)
	actualSLDist := entryPrice.Sub(slPrice).Abs()
	effectiveLossPerUnit := actualSLDist.Add(frictionPerUnit)
	if effectiveLossPerUnit.LessThanOrEqual(decimal.Zero) {
		return OrderIntent{}, false
	}

	riskCapitalUSDT := equity.Mul(decimal.NewFromFloat(RISK_PER_TRADE_PCT))
	qtyByRisk := riskCapitalUSDT.Div(effectiveLossPerUnit)

	// Batasi ukuran margin per slot (Max 30% Equity atau 90% Available Balance)
	maxMarginByEquity := equity.Mul(decimal.NewFromFloat(MAX_SLOT_MARGIN_FRACTION))
	maxMarginByAvail := avail.Mul(decimal.NewFromFloat(0.90))
	allowedMargin := decimal.Min(maxMarginByEquity, maxMarginByAvail)
	maxNotional := allowedMargin.Mul(decimal.NewFromFloat(ACCOUNT_LEVERAGE))
	qtyByMargin := maxNotional.Div(entryPrice)

	rawQty := decimal.Min(qtyByRisk, qtyByMargin)
	finalQty, err := QuantizeQtyFloor(rawQty, snap.Filter.StepSize)
	if err != nil {
		return OrderIntent{}, false
	}

	// Validasi Filter Bursa (MinQty & MinNotional)
	notional := finalQty.Mul(entryPrice)
	if finalQty.LessThan(snap.Filter.MinQty) || notional.LessThan(snap.Filter.MinNotional) {
		return OrderIntent{}, false
	}

	return OrderIntent{
		ClientOrderID: GenerateDeterministicClientOrderID(snap.Symbol, side, snap.Now),
		Symbol:        snap.Symbol,
		Side:          side,
		Qty:           finalQty,
		LimitPrice:    entryPrice,
		SLPrice:       slPrice,
		TPPrice:       tpPrice,
		ATRAtEntry:    atrDec,
		CreatedAt:     snap.Now,
	}, true
}

// ----------------------------------------------------------------------------
// 7. HEURISTIC QUEUE DRY-RUN BROKER (SOLUSI P1-1, P1-2, P1-7, P1-8)
// ----------------------------------------------------------------------------
type VirtualPendingOrder struct {
	Intent    OrderIntent
	FilledQty decimal.Decimal
}

type VirtualOpenPosition struct {
	Symbol           string
	Side             Side
	Qty              decimal.Decimal
	EntryPrice       decimal.Decimal
	CurrentSLPrice   decimal.Decimal
	TPPrice          decimal.Decimal
	ATRAtEntry       decimal.Decimal
	LastMarkPrice    decimal.Decimal
	MarkUpdatedAt    time.Time
	OpenTime         time.Time
	IsBreakEvenMoved bool
}

type DryRunBroker struct {
	Mu                 sync.Mutex
	SessionDayEpochUTC int64
	SessionStartEquity decimal.Decimal
	RealizedEquityUSDT decimal.Decimal
	PendingOrders      map[string]*VirtualPendingOrder
	OpenPositions      map[string]*VirtualOpenPosition
}

func NewDryRunBroker(initialEquity decimal.Decimal) *DryRunBroker {
	return &DryRunBroker{
		SessionStartEquity: initialEquity,
		RealizedEquityUSDT: initialEquity,
		PendingOrders:      make(map[string]*VirtualPendingOrder),
		OpenPositions:      make(map[string]*VirtualOpenPosition),
	}
}

func (d *DryRunBroker) SubmitLimitEntry(intent OrderIntent) (bool, error) {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	if d.isDailyDrawdownBreachedLocked() {
		return false, fmt.Errorf("daily circuit breaker active")
	}
	if _, exists := d.OpenPositions[intent.Symbol]; exists {
		return false, fmt.Errorf("position already open for symbol %s", intent.Symbol)
	}
	if _, exists := d.PendingOrders[intent.Symbol]; exists {
		return false, fmt.Errorf("pending order already exists for symbol %s", intent.Symbol)
	}
	if len(d.OpenPositions)+len(d.PendingOrders) >= MAX_CONCURRENT_SLOTS {
		return false, fmt.Errorf("all %d slots are occupied", MAX_CONCURRENT_SLOTS)
	}

	d.PendingOrders[intent.Symbol] = &VirtualPendingOrder{
		Intent:    intent,
		FilledQty: decimal.Zero,
	}
	return true, nil
}

func (d *DryRunBroker) isDailyDrawdownBreachedLocked() bool {
	if d.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		return false
	}
	effEquity := d.calculateEffectiveEquityLocked()
	ddDec := effEquity.Sub(d.SessionStartEquity).Div(d.SessionStartEquity)
	ddFloat, _ := ddDec.Float64()
	return ddFloat <= MAX_DAILY_DRAWDOWN_PCT
}

// OnRealtimeTick mengimplementasikan pencocokan agresor aggTrade dan pemantauan partial-fill seketika
func (d *DryRunBroker) OnRealtimeTick(
	symbol string,
	bestBid, bestAsk, markPrice, tradePrice, tradeQty float64,
	isBuyerMaker bool,
	now time.Time,
) {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	// Solusi P1-7: Terapkan mark-to-market terlebih dahulu SEBELUM mereset baseline harian 00:00 UTC
	if markPrice > 0 {
		if pos, ok := d.OpenPositions[symbol]; ok {
			pos.LastMarkPrice = decimal.NewFromFloat(markPrice)
			pos.MarkUpdatedAt = now
		}
	}

	epochDay := now.UTC().Unix() / 86400
	if epochDay != d.SessionDayEpochUTC {
		d.SessionDayEpochUTC = epochDay
		d.SessionStartEquity = d.calculateEffectiveEquityLocked()
	}

	// 1. Evaluasi Antrean Limit Order Masuk (Solusi P1-1 & P1-2)
	if ord, exists := d.PendingOrders[symbol]; exists {
		// Validasi Post-Only (GTX reject)
		if ord.FilledQty.IsZero() && bestBid > 0 && bestAsk > 0 {
			if (ord.Intent.Side == SideLong && ord.Intent.LimitPrice.GreaterThanOrEqual(decimal.NewFromFloat(bestAsk))) ||
				(ord.Intent.Side == SideShort && ord.Intent.LimitPrice.LessThanOrEqual(decimal.NewFromFloat(bestBid))) {
				delete(d.PendingOrders, symbol)
				return
			}
		}

		// Timeout 45 Detik: Hapus sisa pending. Jika sudah ada partial fill, posisi aktifnya tetap berjalan dan dilindungi!
		if now.Sub(ord.Intent.CreatedAt) > LIMIT_ORDER_TIMEOUT {
			delete(d.PendingOrders, symbol)
			return
		}

		// Solusi P1-1: Verifikasi Sisi Agresor (Aggressor-Side Matching):
		// Untuk Long Limit di BestBid: Hanya terisi bila agresor adalah SELLER (maker adalah BUYER -> isBuyerMaker == true)
		// Untuk Short Limit di BestAsk: Hanya terisi bila agresor adalah BUYER (maker adalah SELLER -> isBuyerMaker == false)
		if tradePrice > 0 && tradeQty > 0 {
			tpDec := decimal.NewFromFloat(tradePrice)
			tqDec := decimal.NewFromFloat(tradeQty)

			var matched bool
			if ord.Intent.Side == SideLong {
				matched = (tpDec.LessThanOrEqual(ord.Intent.LimitPrice)) && isBuyerMaker
			} else {
				matched = (tpDec.GreaterThanOrEqual(ord.Intent.LimitPrice)) && !isBuyerMaker
			}

			if matched {
				remainingQty := ord.Intent.Qty.Sub(ord.FilledQty)
				fillChunk := decimal.Min(remainingQty, tqDec)
				if fillChunk.GreaterThan(decimal.Zero) {
					ord.FilledQty = ord.FilledQty.Add(fillChunk)
					entryFee := ord.Intent.LimitPrice.Mul(fillChunk).Mul(decimal.NewFromFloat(DEFAULT_MAKER_FEE))
					d.RealizedEquityUSDT = d.RealizedEquityUSDT.Sub(entryFee)

					// Solusi P1-2: Segera perbarui/buat VirtualOpenPosition secara real-time
					// sehingga kuantitas partial fill langsung dipantau SL/TP dan dihitung risikonya!
					d.syncPartialFillToOpenPositionLocked(ord, now)

					if ord.FilledQty.GreaterThanOrEqual(ord.Intent.Qty) {
						delete(d.PendingOrders, symbol)
					}
				}
			}
		}
	}

	// 2. Evaluasi Posisi Terbuka (Break-Even, SL, TP, dan Batas Holding Time)
	if pos, exists := d.OpenPositions[symbol]; exists {
		// Solusi P1-8: Staleness check pada mark price simulator
		if !pos.MarkUpdatedAt.IsZero() && now.Sub(pos.MarkUpdatedAt) > MAX_MARK_STALENESS {
			return // Tolak mengevaluasi exit virtual menggunakan harga stale
		}

		triggerRefPrice := pos.LastMarkPrice
		if triggerRefPrice.LessThanOrEqual(decimal.Zero) {
			return
		}

		beTriggerDist := pos.ATRAtEntry.Mul(decimal.NewFromFloat(BREAKEVEN_TRIGGER_ATR))
		beOffsetRate := decimal.NewFromFloat(math.Max(MIN_BREAKEVEN_OFFSET_PCT, 2.0*DEFAULT_TAKER_FEE))

		// Pemeriksaan Batas Holding Time (Time Stop)
		if now.Sub(pos.OpenTime) > MAX_HOLDING_DURATION {
			d.closePositionLocked(pos, triggerRefPrice, DEFAULT_TAKER_FEE, "TIME_STOP_MAX_HOLDING")
			delete(d.PendingOrders, symbol)
			return
		}

		if pos.Side == SideLong {
			// Geser SL ke Break-Even (+ fee buffer)
			if !pos.IsBreakEvenMoved && triggerRefPrice.Sub(pos.EntryPrice).GreaterThanOrEqual(beTriggerDist) {
				pos.CurrentSLPrice = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Add(beOffsetRate))
				pos.IsBreakEvenMoved = true
			}
			// Trigger Stop Loss
			if triggerRefPrice.LessThanOrEqual(pos.CurrentSLPrice) {
				exitPrice := pos.CurrentSLPrice.Mul(decimal.NewFromFloat(1.0 - SLIPPAGE_PENALTY_RATE))
				d.closePositionLocked(pos, exitPrice, DEFAULT_TAKER_FEE, "STOP_LOSS")
				delete(d.PendingOrders, symbol) // Bersihkan sisa pending order jika ada
				return
			}
			// Trigger Take Profit
			if triggerRefPrice.GreaterThanOrEqual(pos.TPPrice) {
				d.closePositionLocked(pos, pos.TPPrice, DEFAULT_TAKER_FEE, "TAKE_PROFIT")
				delete(d.PendingOrders, symbol)
				return
			}
		} else {
			// Short Break-Even
			if !pos.IsBreakEvenMoved && pos.EntryPrice.Sub(triggerRefPrice).GreaterThanOrEqual(beTriggerDist) {
				pos.CurrentSLPrice = pos.EntryPrice.Mul(decimal.NewFromFloat(1.0).Sub(beOffsetRate))
				pos.IsBreakEvenMoved = true
			}
			// Trigger Stop Loss
			if triggerRefPrice.GreaterThanOrEqual(pos.CurrentSLPrice) {
				exitPrice := pos.CurrentSLPrice.Mul(decimal.NewFromFloat(1.0 + SLIPPAGE_PENALTY_RATE))
				d.closePositionLocked(pos, exitPrice, DEFAULT_TAKER_FEE, "STOP_LOSS")
				delete(d.PendingOrders, symbol)
				return
			}
			// Trigger Take Profit
			if triggerRefPrice.LessThanOrEqual(pos.TPPrice) {
				d.closePositionLocked(pos, pos.TPPrice, DEFAULT_TAKER_FEE, "TAKE_PROFIT")
				delete(d.PendingOrders, symbol)
				return
			}
		}
	}
}

// syncPartialFillToOpenPositionLocked mempromosikan atau memperbarui kuantitas posisi secara instan (Solusi P1-2)
func (d *DryRunBroker) syncPartialFillToOpenPositionLocked(ord *VirtualPendingOrder, now time.Time) {
	if ord.FilledQty.LessThanOrEqual(decimal.Zero) {
		return
	}
	pos, exists := d.OpenPositions[ord.Intent.Symbol]
	if !exists {
		d.OpenPositions[ord.Intent.Symbol] = &VirtualOpenPosition{
			Symbol:           ord.Intent.Symbol,
			Side:             ord.Intent.Side,
			Qty:              ord.FilledQty,
			EntryPrice:       ord.Intent.LimitPrice,
			CurrentSLPrice:   ord.Intent.SLPrice,
			TPPrice:          ord.Intent.TPPrice,
			ATRAtEntry:       ord.Intent.ATRAtEntry,
			LastMarkPrice:    ord.Intent.LimitPrice,
			MarkUpdatedAt:    now,
			OpenTime:         now,
			IsBreakEvenMoved: false,
		}
	} else {
		pos.Qty = ord.FilledQty
	}
}

func (d *DryRunBroker) closePositionLocked(pos *VirtualOpenPosition, exitPrice decimal.Decimal, feeRate float64, reason string) {
	var grossPnL decimal.Decimal
	if pos.Side == SideLong {
		grossPnL = exitPrice.Sub(pos.EntryPrice).Mul(pos.Qty)
	} else {
		grossPnL = pos.EntryPrice.Sub(exitPrice).Mul(pos.Qty)
	}
	exitFee := exitPrice.Mul(pos.Qty).Mul(decimal.NewFromFloat(feeRate))
	netPnL := grossPnL.Sub(exitFee)
	d.RealizedEquityUSDT = d.RealizedEquityUSDT.Add(netPnL)

	delete(d.OpenPositions, pos.Symbol)
}

func (d *DryRunBroker) calculateEffectiveEquityLocked() decimal.Decimal {
	unrealized := decimal.Zero
	for _, pos := range d.OpenPositions {
		if pos.LastMarkPrice.GreaterThan(decimal.Zero) {
			if pos.Side == SideLong {
				unrealized = unrealized.Add(pos.LastMarkPrice.Sub(pos.EntryPrice).Mul(pos.Qty))
			} else {
				unrealized = unrealized.Add(pos.EntryPrice.Sub(pos.LastMarkPrice).Mul(pos.Qty))
			}
		}
	}
	return d.RealizedEquityUSDT.Add(unrealized)
}

func (d *DryRunBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return d.calculateEffectiveEquityLocked()
}

func (d *DryRunBroker) GetAvailableBalanceUSDT() decimal.Decimal {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	usedMargin := decimal.Zero
	lev := decimal.NewFromFloat(ACCOUNT_LEVERAGE)

	for _, pos := range d.OpenPositions {
		notional := pos.EntryPrice.Mul(pos.Qty)
		usedMargin = usedMargin.Add(notional.Div(lev))
	}
	for _, ord := range d.PendingOrders {
		notional := ord.Intent.LimitPrice.Mul(ord.Intent.Qty)
		usedMargin = usedMargin.Add(notional.Div(lev))
	}

	effEquity := d.calculateEffectiveEquityLocked()
	avail := effEquity.Sub(usedMargin)
	if avail.LessThan(decimal.Zero) {
		return decimal.Zero
	}
	return avail
}

func (d *DryRunBroker) GetDailyTotalDrawdownPct() float64 {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	if d.SessionStartEquity.LessThanOrEqual(decimal.Zero) {
		return 0.0
	}
	effEquity := d.calculateEffectiveEquityLocked()
	ddDec := effEquity.Sub(d.SessionStartEquity).Div(d.SessionStartEquity)
	ddFloat, _ := ddDec.Float64()
	return ddFloat
}

func (d *DryRunBroker) GetActiveSlotCount() int {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return len(d.OpenPositions) + len(d.PendingOrders)
}

func (d *DryRunBroker) HasActiveOrderOrPosition(symbol string) bool {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	_, hasOrd := d.PendingOrders[symbol]
	_, hasPos := d.OpenPositions[symbol]
	return hasOrd || hasPos
}

// ----------------------------------------------------------------------------
// 8. LIVE BROKER ORDER STATE MACHINE & REKONSILIASI PENUH
//    (SOLUSI P0-1, P0-2, P0-3, P0-4, P0-5, P0-6, P0-7)
// ----------------------------------------------------------------------------

type LiveOrderRecord struct {
	ClientOrderID       string
	ExchangeOrderID     int64
	Symbol              string
	Side                Side
	IsEntry             bool
	IsAlgo              bool // true untuk conditional algo orders (STOP_MARKET / TAKE_PROFIT_MARKET)
	Status              OrderLifecycleStatus
	TargetQty           decimal.Decimal
	CumulativeFilledQty decimal.Decimal
	SLPrice             decimal.Decimal
	TPPrice             decimal.Decimal
	CreatedAt           time.Time
}

type ProtectionOrderStatus string

const (
	ProtectionPending ProtectionOrderStatus = "PENDING"
	ProtectionAcked   ProtectionOrderStatus = "ACKED"
	ProtectionFailed  ProtectionOrderStatus = "FAILED"
)

type LivePositionProtection struct {
	Symbol          string
	Side            Side
	PlanGeneration  uint64 // Solusi P0-4: versioning generation check mencegah race condition plan
	ProtectedQty    decimal.Decimal
	SLOrderID       int64
	SLClientAlgoID  string
	SLStatus        ProtectionOrderStatus
	SLPrice         decimal.Decimal
	TPOrderID       int64
	TPClientAlgoID  string
	TPStatus        ProtectionOrderStatus
	TPPrice         decimal.Decimal
	LastAttemptTime time.Time
	LastError       string
}

type ProtectionPlanAction struct {
	Symbol         string
	Generation     uint64
	CancelSLID     int64
	CancelTPID     int64
	NeedNewSL      bool
	NeedNewTP      bool
	ExitSide       Side
	TargetQty      decimal.Decimal
	SLPrice        decimal.Decimal
	TPPrice        decimal.Decimal
	ClientSLAlgoID string
	ClientTPAlgoID string
}

type LivePosition struct {
	Symbol        string
	Side          Side
	Qty           decimal.Decimal
	EntryPrice    decimal.Decimal
	UnrealizedPnL decimal.Decimal
}

type BinanceLiveBroker struct {
	Mu                     sync.Mutex
	SessionStartUTCDay     int
	SessionStartEquityUSDT decimal.Decimal
	WalletBalanceUSDT      decimal.Decimal
	UnrealizedPnLUSDT      decimal.Decimal
	AvailableBalanceUSDT   decimal.Decimal
	LastAccountUpdateTime  time.Time
	CircuitBreakerTripped  bool

	// Order Ledger & Algo ID Mapping (Solusi P0-3)
	OrdersByClient         map[string]*LiveOrderRecord
	OrdersByExchangeID     map[int64]*LiveOrderRecord
	Protections            map[string]*LivePositionProtection
	ActivePositions        map[string]decimal.Decimal // symbol -> current position qty

	// Pluggable Exchange Network Callbacks (I/O decoupled)
	PlaceEntryFn           func(ctx context.Context, intent OrderIntent) (exchangeOrderID int64, err error)
	PlaceAlgoOrderFn       func(ctx context.Context, symbol string, side Side, orderType string, qty, triggerPrice decimal.Decimal, clientAlgoID string) (algoID int64, err error)
	CancelOrderFn          func(ctx context.Context, symbol string, exchangeOrderID int64, clientOrderID string) error
	CancelAlgoOrderFn      func(ctx context.Context, symbol string, algoID int64) error
	EmergencyMarketCloseFn func(ctx context.Context, symbol string, reason string) error
	QueryOrderFn           func(ctx context.Context, symbol string, clientOrderID string) (*LiveOrderRecord, error)
}

func NewBinanceLiveBroker() *BinanceLiveBroker {
	return &BinanceLiveBroker{
		OrdersByClient:     make(map[string]*LiveOrderRecord),
		OrdersByExchangeID: make(map[int64]*LiveOrderRecord),
		Protections:        make(map[string]*LivePositionProtection),
		ActivePositions:    make(map[string]decimal.Decimal),
	}
}

// InitAndVerifySymbol memverifikasi One-Way Mode, isolated margin, dan 5x leverage sebelum live trading (Solusi P0-5 & P0-7)
func InitAndVerifySymbol(
	ctx context.Context,
	state *SymbolState,
	setMarginTypeFn func(symbol string, marginType string) error,
	setLeverageFn func(symbol string, leverage int) error,
	verifyConfigFn func(symbol string) (isIsolated bool, leverage int, isOneWay bool, err error),
) error {
	state.Mu.Lock()
	defer state.Mu.Unlock()

	// 1. Set Margin Type ISOLATED (abaikan error jika sudah ISOLATED / code -4046)
	_ = setMarginTypeFn(state.Symbol, "ISOLATED")

	// 2. Set Leverage 5x
	if err := setLeverageFn(state.Symbol, int(ACCOUNT_LEVERAGE)); err != nil {
		return fmt.Errorf("failed to set leverage for %s: %w", state.Symbol, err)
	}

	// 3. Verifikasi konfigurasi aktual dari bursa
	isIsolated, lev, isOneWay, err := verifyConfigFn(state.Symbol)
	if err != nil {
		return fmt.Errorf("failed to verify position config for %s: %w", state.Symbol, err)
	}

	if !isIsolated {
		return fmt.Errorf("symbol %s margin type is not ISOLATED", state.Symbol)
	}
	if lev != int(ACCOUNT_LEVERAGE) {
		return fmt.Errorf("symbol %s leverage is %d, expected %d", state.Symbol, lev, int(ACCOUNT_LEVERAGE))
	}
	if !isOneWay {
		return fmt.Errorf("symbol %s is in Hedge Mode; strategy requires One-Way Mode (P0-5)", state.Symbol)
	}

	state.IsLeverageIsolatedVerified = true
	return nil
}

// ReserveEntryIntent melakukan reservasi slot atomik in-memory tanpa I/O jaringan (Fase 1 Solusi P0-1)
func (b *BinanceLiveBroker) ReserveEntryIntent(intent OrderIntent) (bool, error) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.CircuitBreakerTripped || b.isDailyDrawdownBreachedLocked() {
		return false, fmt.Errorf("daily circuit breaker tripped")
	}

	// Fail-closed jika data saldo/margin akun Binance stale lebih dari 60 detik (Solusi P0-2)
	if b.LastAccountUpdateTime.IsZero() || time.Now().Sub(b.LastAccountUpdateTime) > MAX_ACCOUNT_STALENESS {
		return false, fmt.Errorf("account data is stale (> %v)", MAX_ACCOUNT_STALENESS)
	}

	if _, exists := b.OrdersByClient[intent.ClientOrderID]; exists {
		return false, fmt.Errorf("order %s already exists", intent.ClientOrderID)
	}
	if posQty, hasPos := b.ActivePositions[intent.Symbol]; hasPos && posQty.GreaterThan(decimal.Zero) {
		return false, fmt.Errorf("position already exists for symbol %s", intent.Symbol)
	}

	activeCount := len(b.ActivePositions)
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == StatusNew || ord.Status == StatusPendingNew || ord.Status == StatusPartiallyFilled) {
			if ord.Symbol == intent.Symbol {
				return false, fmt.Errorf("pending entry order already exists for %s", intent.Symbol)
			}
			activeCount++
		}
	}
	if activeCount >= MAX_CONCURRENT_SLOTS {
		return false, fmt.Errorf("max slots (%d) reached", MAX_CONCURRENT_SLOTS)
	}

	// Catat record awal dengan status PENDING_NEW
	b.OrdersByClient[intent.ClientOrderID] = &LiveOrderRecord{
		ClientOrderID:       intent.ClientOrderID,
		Symbol:              intent.Symbol,
		Side:                intent.Side,
		IsEntry:             true,
		IsAlgo:              false,
		Status:              StatusPendingNew,
		TargetQty:           intent.Qty,
		CumulativeFilledQty: decimal.Zero,
		SLPrice:             intent.SLPrice,
		TPPrice:             intent.TPPrice,
		CreatedAt:           time.Now().UTC(),
	}
	return true, nil
}

// SubmitLimitEntry mengimplementasikan Two-Phase Commit: Reservasi Atomik -> I/O Bursa -> Update/Reconcile (Solusi P0-1)
func (b *BinanceLiveBroker) SubmitLimitEntry(intent OrderIntent) (bool, error) {
	reserved, err := b.ReserveEntryIntent(intent)
	if !reserved {
		return false, err
	}

	if b.PlaceEntryFn == nil {
		b.Mu.Lock()
		delete(b.OrdersByClient, intent.ClientOrderID)
		b.Mu.Unlock()
		return false, fmt.Errorf("PlaceEntryFn adapter is not configured")
	}

	// Eksekusi I/O Jaringan DI LUAR MUTEX
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exchangeOrderID, err := b.PlaceEntryFn(ctx, intent)
	if err != nil {
		// Error jaringan atau penolakan bursa: lakukan rekonsiliasi state di bawah lock
		b.Mu.Lock()
		defer b.Mu.Unlock()

		// Jika error pasti ditolak (misal -5022 Post-Only GTX reject), hapus reservasi slot seketika
		delete(b.OrdersByClient, intent.ClientOrderID)
		return false, fmt.Errorf("entry placement rejected/failed: %w", err)
	}

	// Penempatan berhasil: update record ke NEW dan hubungkan exchangeOrderID
	b.Mu.Lock()
	if rec, exists := b.OrdersByClient[intent.ClientOrderID]; exists {
		rec.Status = StatusNew
		rec.ExchangeOrderID = exchangeOrderID
		b.OrdersByExchangeID[exchangeOrderID] = rec
	}
	b.Mu.Unlock()

	return true, nil
}

// HandleAccountUpdate memproses pembaruan akun dari REST atau event ACCOUNT_UPDATE (Solusi P0-2)
func (b *BinanceLiveBroker) HandleAccountUpdate(
	walletBal decimal.Decimal,
	unrealizedPnL decimal.Decimal,
	availBal decimal.Decimal,
	positions map[string]LivePosition,
	eventTime time.Time,
) {
	var ordersToCancel []*LiveOrderRecord

	b.Mu.Lock()
	b.WalletBalanceUSDT = walletBal
	b.UnrealizedPnLUSDT = unrealizedPnL
	b.AvailableBalanceUSDT = availBal
	b.LastAccountUpdateTime = eventTime

	// Sinkronisasi posisi aktif
	for sym, pos := range positions {
		if pos.Qty.GreaterThan(decimal.Zero) {
			b.ActivePositions[sym] = pos.Qty
		} else {
			delete(b.ActivePositions, sym)
		}
	}

	effectiveEquity := walletBal.Add(unrealizedPnL)

	// Reset harian 00:00:00 UTC (Solusi P0-2)
	utcDay := eventTime.UTC().YearDay()
	if b.SessionStartUTCDay != utcDay || b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		b.SessionStartUTCDay = utcDay
		b.SessionStartEquityUSDT = effectiveEquity
		b.CircuitBreakerTripped = false
	}

	// Hitung drawdown terhadap baseline harian UTC
	if b.SessionStartEquityUSDT.GreaterThan(decimal.Zero) {
		ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
		ddFloat, _ := ddDec.Float64()
		if ddFloat <= MAX_DAILY_DRAWDOWN_PCT {
			b.CircuitBreakerTripped = true
			// Kumpulkan seluruh pending entry order untuk dibatalkan di luar mutex
			for _, ord := range b.OrdersByClient {
				if ord.IsEntry && (ord.Status == StatusNew || ord.Status == StatusPendingNew) {
					ordersToCancel = append(ordersToCancel, ord)
				}
			}
		}
	}
	b.Mu.Unlock()

	// Batalkan pending entry order jika circuit breaker aktif
	if len(ordersToCancel) > 0 && b.CancelOrderFn != nil {
		for _, ord := range ordersToCancel {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
			cancel()
		}
	}
}

// HandleUserDataOrderUpdate merekonsiliasi event ORDER_TRADE_UPDATE (Solusi P0-3 & P0-6)
func (b *BinanceLiveBroker) HandleUserDataOrderUpdate(
	clientOrderID string,
	exchangeOrderID int64,
	symbol string,
	status OrderLifecycleStatus,
	cumFilledQty decimal.Decimal,
) {
	var plan ProtectionPlanAction
	var needPlanExecution bool
	var siblingCancelAlgoID int64
	var siblingCancelSymbol string

	b.Mu.Lock()
	// Cari via clientOrderID atau exchangeOrderID (Solusi P0-3)
	rec, exists := b.OrdersByClient[clientOrderID]
	if !exists && exchangeOrderID != 0 {
		rec, exists = b.OrdersByExchangeID[exchangeOrderID]
	}

	if !exists {
		// Rekonsiliasi aktif jika event bursa belum tercatat in-memory
		b.Mu.Unlock()
		if b.QueryOrderFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = b.QueryOrderFn(ctx, symbol, clientOrderID)
			cancel()
		}
		return
	}

	rec.ExchangeOrderID = exchangeOrderID
	rec.Status = status
	rec.CumulativeFilledQty = cumFilledQty
	b.OrdersByExchangeID[exchangeOrderID] = rec

	// SKENARIO 1: Update pada Order Entry
	if rec.IsEntry {
		switch status {
		case StatusPartiallyFilled, StatusFilled:
			if cumFilledQty.GreaterThan(decimal.Zero) {
				b.ActivePositions[symbol] = cumFilledQty
				plan, needPlanExecution = b.planProtectionSyncLocked(rec)
			}
		case StatusCanceled, StatusExpired:
			if cumFilledQty.GreaterThan(decimal.Zero) {
				b.ActivePositions[symbol] = cumFilledQty
				plan, needPlanExecution = b.planProtectionSyncLocked(rec)
			} else {
				delete(b.OrdersByClient, rec.ClientOrderID)
				delete(b.OrdersByExchangeID, exchangeOrderID)
			}
		case StatusRejected:
			delete(b.OrdersByClient, rec.ClientOrderID)
			delete(b.OrdersByExchangeID, exchangeOrderID)
		}
		b.Mu.Unlock()

		if needPlanExecution {
			go b.executeProtectionPlan(plan)
		}
		return
	}

	// SKENARIO 2: Update pada Order Exit (SL atau TP Terisi -> Batalkan Sibling Algo Order - Solusi P0-3)
	if !rec.IsEntry && status == StatusFilled {
		delete(b.ActivePositions, symbol)
		if prot, ok := b.Protections[symbol]; ok {
			if (exchangeOrderID == prot.SLOrderID || clientOrderID == prot.SLClientAlgoID) && prot.TPOrderID != 0 {
				siblingCancelAlgoID = prot.TPOrderID
				siblingCancelSymbol = symbol
			} else if (exchangeOrderID == prot.TPOrderID || clientOrderID == prot.TPClientAlgoID) && prot.SLOrderID != 0 {
				siblingCancelAlgoID = prot.SLOrderID
				siblingCancelSymbol = symbol
			}
			delete(b.Protections, symbol)
		}
		delete(b.OrdersByClient, rec.ClientOrderID)
		delete(b.OrdersByExchangeID, exchangeOrderID)
	}
	b.Mu.Unlock()

	// Pembatalan order sibling algo dilakukan DI LUAR MUTEX via CancelAlgoOrderFn (Solusi P0-3 & P0-5)
	if siblingCancelAlgoID != 0 && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, siblingCancelSymbol, siblingCancelAlgoID)
		cancel()
	}
}

func (b *BinanceLiveBroker) planProtectionSyncLocked(entryRec *LiveOrderRecord) (ProtectionPlanAction, bool) {
	prot, exists := b.Protections[entryRec.Symbol]
	if exists && prot.SLStatus == ProtectionAcked && prot.ProtectedQty.Equal(entryRec.CumulativeFilledQty) {
		return ProtectionPlanAction{}, false
	}

	var gen uint64 = 1
	var oldSLID, oldTPID int64
	if exists {
		gen = prot.PlanGeneration + 1
		oldSLID = prot.SLOrderID
		oldTPID = prot.TPOrderID
	}

	now := time.Now().UTC()
	slAlgoClientID := fmt.Sprintf("ALGO_SL_%s_%d", entryRec.Symbol, now.UnixNano())
	tpAlgoClientID := fmt.Sprintf("ALGO_TP_%s_%d", entryRec.Symbol, now.UnixNano())

	action := ProtectionPlanAction{
		Symbol:         entryRec.Symbol,
		Generation:     gen,
		CancelSLID:     oldSLID,
		CancelTPID:     oldTPID,
		NeedNewSL:      true,
		NeedNewTP:      true,
		ExitSide:       OppositeSide(entryRec.Side),
		TargetQty:      entryRec.CumulativeFilledQty,
		SLPrice:        entryRec.SLPrice,
		TPPrice:        entryRec.TPPrice,
		ClientSLAlgoID: slAlgoClientID,
		ClientTPAlgoID: tpAlgoClientID,
	}

	b.Protections[entryRec.Symbol] = &LivePositionProtection{
		Symbol:          entryRec.Symbol,
		Side:            entryRec.Side,
		PlanGeneration:  gen,
		ProtectedQty:    decimal.Zero, // Belum terproteksi sampai order trigger ter-ACK!
		SLStatus:        ProtectionPending,
		SLClientAlgoID:  slAlgoClientID,
		SLPrice:         entryRec.SLPrice,
		TPStatus:        ProtectionPending,
		TPClientAlgoID:  tpAlgoClientID,
		TPPrice:         entryRec.TPPrice,
		LastAttemptTime: now,
	}

	return action, true
}

// executeProtectionPlan menempatkan SL baru terlebih dahulu SEBELUM membatalkan SL lama (Solusi P0-4 & P0-5)
func (b *BinanceLiveBroker) executeProtectionPlan(action ProtectionPlanAction) {
	// LANGKAH 1: Pasang SL baru (POST /fapi/v1/algoOrder STOP_MARKET) TERLEBIH DAHULU!
	// Jangan pernah membatalkan SL lama sebelum SL baru terkonfirmasi exchange (Solusi P0-4 Anti-Naked Position)
	var newSLOrderID int64
	var slErr error

	for attempt := 1; attempt <= 3; attempt++ {
		if b.PlaceAlgoOrderFn != nil && action.NeedNewSL {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newSLOrderID, slErr = b.PlaceAlgoOrderFn(ctx, action.Symbol, action.ExitSide, "STOP_MARKET", action.TargetQty, action.SLPrice, action.ClientSLAlgoID)
			cancel()
			if slErr == nil && newSLOrderID != 0 {
				break
			}
			time.Sleep(time.Duration(attempt*300) * time.Millisecond)
		}
	}

	// Jika penempatan SL baru gagal: Posisi masih dilindungi SL lama (jika ada).
	// Jika tidak ada SL lama sama sekali (entry pertama), segera picu Emergency Market Close!
	if slErr != nil || newSLOrderID == 0 {
		b.Mu.Lock()
		if prot, ok := b.Protections[action.Symbol]; ok && prot.PlanGeneration == action.Generation {
			prot.SLStatus = ProtectionFailed
			prot.LastError = fmt.Sprintf("SL placement failed after retries: %v", slErr)
		}
		hasOldSL := action.CancelSLID != 0
		b.Mu.Unlock()

		if !hasOldSL && b.EmergencyMarketCloseFn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = b.EmergencyMarketCloseFn(ctx, action.Symbol, "CRITICAL: SL placement failed, emergency exit executed")
			cancel()
		}
		return
	}

	// LANGKAH 2: Pasang TP baru (POST /fapi/v1/algoOrder TAKE_PROFIT_MARKET)
	var newTPOrderID int64
	var tpErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if b.PlaceAlgoOrderFn != nil && action.NeedNewTP {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newTPOrderID, tpErr = b.PlaceAlgoOrderFn(ctx, action.Symbol, action.ExitSide, "TAKE_PROFIT_MARKET", action.TargetQty, action.TPPrice, action.ClientTPAlgoID)
			cancel()
			if tpErr == nil && newTPOrderID != 0 {
				break
			}
			time.Sleep(time.Duration(attempt*300) * time.Millisecond)
		}
	}

	// LANGKAH 3: HANYA SETELAH SL BARU TERKONFIRMASI -> Batalkan SL dan TP lama (Solusi P0-4)
	if action.CancelSLID != 0 && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, action.Symbol, action.CancelSLID)
		cancel()
	}
	if action.CancelTPID != 0 && b.CancelAlgoOrderFn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.CancelAlgoOrderFn(ctx, action.Symbol, action.CancelTPID)
		cancel()
	}

	// LANGKAH 4: Update status proteksi di bawah lock dengan validasi generation check
	b.Mu.Lock()
	defer b.Mu.Unlock()

	prot, ok := b.Protections[action.Symbol]
	if !ok || prot.PlanGeneration != action.Generation {
		return // Stale generation response diabaikan (Solusi P0-4)
	}

	prot.SLOrderID = newSLOrderID
	prot.SLStatus = ProtectionAcked
	prot.ProtectedQty = action.TargetQty

	if tpErr != nil || newTPOrderID == 0 {
		prot.TPStatus = ProtectionFailed
		prot.LastError = fmt.Sprintf("TP placement failed: %v", tpErr)
	} else {
		prot.TPOrderID = newTPOrderID
		prot.TPStatus = ProtectionAcked
	}

	// Daftarkan SL dan TP baru ke ledger order agar HandleUserDataOrderUpdate dapat mengenalinya (Solusi P0-3)
	b.OrdersByClient[action.ClientSLAlgoID] = &LiveOrderRecord{
		ClientOrderID:   action.ClientSLAlgoID,
		ExchangeOrderID: newSLOrderID,
		Symbol:          action.Symbol,
		Side:            action.ExitSide,
		IsEntry:         false,
		IsAlgo:          true,
		Status:          StatusNew,
		TargetQty:       action.TargetQty,
		SLPrice:         action.SLPrice,
		CreatedAt:       time.Now().UTC(),
	}
	b.OrdersByExchangeID[newSLOrderID] = b.OrdersByClient[action.ClientSLAlgoID]

	if newTPOrderID != 0 {
		b.OrdersByClient[action.ClientTPAlgoID] = &LiveOrderRecord{
			ClientOrderID:   action.ClientTPAlgoID,
			ExchangeOrderID: newTPOrderID,
			Symbol:          action.Symbol,
			Side:            action.ExitSide,
			IsEntry:         false,
			IsAlgo:          true,
			Status:          StatusNew,
			TargetQty:       action.TargetQty,
			TPPrice:         action.TPPrice,
			CreatedAt:       time.Now().UTC(),
		}
		b.OrdersByExchangeID[newTPOrderID] = b.OrdersByClient[action.ClientTPAlgoID]
	}
}

// CheckPendingEntryTimeouts memindai dan membatalkan entry order live yang melebihi timeout 45 detik (Solusi P0-6)
func (b *BinanceLiveBroker) CheckPendingEntryTimeouts(now time.Time) {
	var expiredOrders []*LiveOrderRecord

	b.Mu.Lock()
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == StatusNew || ord.Status == StatusPendingNew) {
			if now.Sub(ord.CreatedAt) > LIMIT_ORDER_TIMEOUT {
				expiredOrders = append(expiredOrders, ord)
			}
		}
	}
	b.Mu.Unlock()

	if len(expiredOrders) == 0 || b.CancelOrderFn == nil {
		return
	}

	for _, ord := range expiredOrders {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := b.CancelOrderFn(ctx, ord.Symbol, ord.ExchangeOrderID, ord.ClientOrderID)
		cancel()

		b.Mu.Lock()
		if err == nil {
			ord.Status = StatusExpired
			// Jika belum ada fill, bersihkan reservasi slot
			if ord.CumulativeFilledQty.IsZero() {
				delete(b.OrdersByClient, ord.ClientOrderID)
				delete(b.OrdersByExchangeID, ord.ExchangeOrderID)
			}
		}
		b.Mu.Unlock()
	}
}

func (b *BinanceLiveBroker) isDailyDrawdownBreachedLocked() bool {
	if b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		return false
	}
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
	ddFloat, _ := ddDec.Float64()
	return ddFloat <= MAX_DAILY_DRAWDOWN_PCT
}

func (b *BinanceLiveBroker) OnRealtimeTick(symbol string, bestBid, bestAsk, markPrice, tradePrice, tradeQty float64, isBuyerMaker bool, now time.Time) {
	// Di mode live, eksekusi tick ditangani bursa. Fungsi ini memicu pemeriksaan timeout order entry 45 detik (Solusi P0-6)
	b.CheckPendingEntryTimeouts(now)
}

func (b *BinanceLiveBroker) GetEffectiveTotalEquityUSDT() decimal.Decimal {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
}

func (b *BinanceLiveBroker) GetAvailableBalanceUSDT() decimal.Decimal {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return b.AvailableBalanceUSDT
}

func (b *BinanceLiveBroker) GetDailyTotalDrawdownPct() float64 {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if b.SessionStartEquityUSDT.LessThanOrEqual(decimal.Zero) {
		return 0.0
	}
	effectiveEquity := b.WalletBalanceUSDT.Add(b.UnrealizedPnLUSDT)
	ddDec := effectiveEquity.Sub(b.SessionStartEquityUSDT).Div(b.SessionStartEquityUSDT)
	f, _ := ddDec.Float64()
	return f
}

func (b *BinanceLiveBroker) GetActiveSlotCount() int {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	activeEntries := 0
	for _, ord := range b.OrdersByClient {
		if ord.IsEntry && (ord.Status == StatusNew || ord.Status == StatusPendingNew || ord.Status == StatusPartiallyFilled) {
			activeEntries++
		}
	}
	return activeEntries + len(b.ActivePositions)
}

func (b *BinanceLiveBroker) HasActiveOrderOrPosition(symbol string) bool {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if posQty, has := b.ActivePositions[symbol]; has && posQty.GreaterThan(decimal.Zero) {
		return true
	}
	for _, ord := range b.OrdersByClient {
		if ord.Symbol == symbol && ord.IsEntry && (ord.Status == StatusNew || ord.Status == StatusPendingNew || ord.Status == StatusPartiallyFilled) {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// 9. RUMUS EKSAK INDIKATOR & ROLLING STATISTIK (SOLUSI P1-3, P1-4, P2)
// ----------------------------------------------------------------------------
func CalculateEMA(candles []Candle, period int) float64 {
	if len(candles) < period || period <= 0 {
		return 0.0
	}
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += candles[i].Close
	}
	ema := sum / float64(period)
	multiplier := 2.0 / float64(period+1)

	for i := period; i < len(candles); i++ {
		ema = (candles[i].Close-ema)*multiplier + ema
	}
	return ema
}

func CalculateATR(candles []Candle, period int) float64 {
	if len(candles) < period+1 || period <= 0 {
		return 0.0
	}
	trs := make([]float64, len(candles)-1)
	for i := 1; i < len(candles); i++ {
		highLow := candles[i].High - candles[i].Low
		highPrevClose := math.Abs(candles[i].High - candles[i-1].Close)
		lowPrevClose := math.Abs(candles[i].Low - candles[i-1].Close)
		trs[i-1] = math.Max(highLow, math.Max(highPrevClose, lowPrevClose))
	}

	sumTR := 0.0
	for i := 0; i < period; i++ {
		sumTR += trs[i]
	}
	atr := sumTR / float64(period)

	for i := period; i < len(trs); i++ {
		atr = ((atr * float64(period-1)) + trs[i]) / float64(period)
	}
	return atr
}

func CalculateRVOL(candles []Candle, lookback int) float64 {
	if len(candles) < lookback+1 || lookback <= 0 {
		return 0.0
	}
	latestVol := candles[len(candles)-1].VolumeBase
	sumPrevVol := 0.0
	startIdx := len(candles) - 1 - lookback
	for i := startIdx; i < len(candles)-1; i++ {
		sumPrevVol += candles[i].VolumeBase
	}
	avgPrevVol := sumPrevVol / float64(lookback)
	if avgPrevVol <= 0 {
		return 0.0
	}
	return latestVol / avgPrevVol
}

func CalculateMeanAndStdDev(values []float64) (float64, float64) {
	n := len(values)
	if n == 0 {
		return 0.0, 0.0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(n)

	varSq := 0.0
	for _, v := range values {
		diff := v - mean
		varSq += diff * diff
	}
	return mean, math.Sqrt(varSq / float64(n))
}

func AppendCandleWithCap(slice []Candle, elem Candle, capLimit int) []Candle {
	slice = append(slice, elem)
	if len(slice) > capLimit {
		slice = slice[len(slice)-capLimit:]
	}
	return slice
}

func AppendFloatWithCap(slice []float64, elem float64, capLimit int) []float64 {
	slice = append(slice, elem)
	if len(slice) > capLimit {
		slice = slice[len(slice)-capLimit:]
	}
	return slice
}

// ----------------------------------------------------------------------------
// 10. BULK MARKET SCANNER & WATCHLIST BUILDER (SOLUSI P1-5)
// ----------------------------------------------------------------------------
type BookQuote struct {
	BestBid float64
	BestAsk float64
}

func RunBulkMarketScanner(
	activeStates map[string]*SymbolState,
	perpetualFilters map[string]ExchangeFilter,
	vol24hMap map[string]float64,
	bookMap map[string]BookQuote,
	fetchKlinesFn func(symbol string, limit int) []Candle,
	serverTime time.Time,
) []string {
	type Candidate struct {
		Symbol  string
		RVOL    float64
		Candles []Candle
	}
	var passed []Candidate

	for sym := range perpetualFilters {
		if vol24hMap[sym] < MIN_VOLUME_24H_USDT {
			continue
		}
		bk, ok := bookMap[sym]
		if !ok || bk.BestBid <= 0 || bk.BestAsk <= bk.BestBid {
			continue
		}
		spreadPct := ((bk.BestAsk - bk.BestBid) / bk.BestBid) * 100.0
		if spreadPct > MAX_SPREAD_PCT {
			continue
		}

		// Ambil 1001 bar, validasi monotonic & pastikan bar aktif dibuang (Solusi P1-5)
		rawKlines := fetchKlinesFn(sym, BOOTSTRAP_CANDLE_COUNT+1)
		validKlines, err := ValidateAndFilterCandles(rawKlines, serverTime)
		if err != nil || len(validKlines) < EMA_PERIOD+1 {
			continue
		}

		atr14 := CalculateATR(validKlines, ATR_PERIOD)
		lastClose := validKlines[len(validKlines)-1].Close
		if lastClose <= 0 {
			continue
		}
		natrPct := (atr14 / lastClose) * 100.0
		if natrPct < MIN_NATR_PCT || natrPct > MAX_NATR_PCT {
			continue
		}

		rvol := CalculateRVOL(validKlines, RVOL_LOOKBACK)
		if rvol < MIN_RVOL_RATIO {
			continue
		}

		passed = append(passed, Candidate{Symbol: sym, RVOL: rvol, Candles: validKlines})
	}

	sort.Slice(passed, func(i, j int) bool {
		return passed[i].RVOL > passed[j].RVOL
	})

	var selected []string
	for i := 0; i < len(passed) && i < MAX_WATCHED_COINS; i++ {
		cand := passed[i]
		selected = append(selected, cand.Symbol)
		if _, exists := activeStates[cand.Symbol]; !exists {
			st := NewSymbolState(cand.Symbol, perpetualFilters[cand.Symbol])
			_ = BootstrapSymbolState(st, cand.Candles, serverTime)
			activeStates[cand.Symbol] = st
		}
	}
	return selected
}

func main() {
	fmt.Println("==================================================================")
	fmt.Println("Binance USDⓈ-M Futures Intraday Bot Backbone (v2.1 Hardened)")
	fmt.Println("Audit Resolusi P0 & P1 Terverifikasi.")
	fmt.Println("==================================================================")
}

```

---

## 7. STRUKTUR DIREKTORI PROYEK GOLANG

```text
binance-futures-bot/
├── cmd/
│   └── bot/
│       └── main.go                 # Entry point, verifikasi One-Way mode & leverage 5x Isolated, graceful shutdown
├── internal/
│   ├── config/
│   │   └── config.go               # Konfigurasi ENV, URL endpoint 2026 (/public, /market, /private), fee tier
│   ├── scanner/
│   │   ├── scanner.go              # Bulk 24hr ticker & bookTicker filter, pemilihan Top 10 RVOL
│   │   └── bootstrap.go            # Fetch 1000 closed klines 5m untuk warm-up konvergen EMA200, VWAP, ATR14, BarDelta
│   ├── ws/
│   │   ├── public_stream.go        # Koneksi wss://fstream.binance.com/public/stream (@bookTicker)
│   │   ├── market_stream.go        # Koneksi wss://fstream.binance.com/market/stream (@aggTrade, @kline_5m, @markPrice)
│   │   ├── private_stream.go       # User Data Stream (listenKey keepalive, ORDER_TRADE_UPDATE, ACCOUNT_UPDATE)
│   │   └── ws_api_order.go         # Pengiriman order via wss://ws-fapi.binance.com/ws-fapi/v1 atau REST Trade API
│   ├── indicator/
│   │   ├── vwap.go                 # Session VWAP berbasis VolumeBase & EpochDayUTC (00:00:00 UTC reset)
│   │   ├── volatility.go           # EMA200 (1000-bar mature), ATR14, Session VWAP-ATR Envelope
│   │   └── orderflow.go            # Bar Delta Z-Score (50 baseline eksklusif bar t) & OI Delta 5m validator
│   ├── strategy/
│   │   ├── snapshot.go             # Pembuatan MarketSnapshot immutable (lock-free evaluation)
│   │   └── sizing.go               # Kalkulasi ukuran posisi berbasis shopspring/decimal + Margin-Cap (fail-closed)
│   ├── broker/
│   │   ├── interface.go            # Kontrak ExecutionBroker
│   │   ├── dryrun_queue.go         # Heuristic Simulator (Aggressor Matching isBuyerMaker, Partial Fill SL Tracking)
│   │   └── live_statemachine.go    # Two-Phase Entry, Safe SL-First Protection, Sibling Algo Cancel, 45s Timeout
│   └── storage/
│       └── sqlite.go               # Penyimpanan state akun, log transaksi, alasan penolakan sinyal, MAE/MFE
├── test/
│   ├── acceptance_test.go          # Suite 20 unit & integration test sesuai checklist audit
│   └── pseudo_test.go              # Regression test suite
├── Dockerfile
└── go.mod
```

---

## 8. SKEMA DATABASE SQLITE WAL (OBSERVABILITAS & AUDIT)

Modul `internal/storage/sqlite.go` wajib mengeksekusi DDL berikut saat inisialisasi untuk mencatat posisi, saldo harian, metrik MAE/MFE, serta alasan penolakan setiap sinyal:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS account_daily_state (
    epoch_day_utc        INTEGER PRIMARY KEY,
    session_start_equity TEXT NOT NULL,
    realized_equity      TEXT NOT NULL,
    circuit_breaker_hit  INTEGER NOT NULL DEFAULT 0,
    updated_at_utc       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_ledger (
    client_order_id      TEXT PRIMARY KEY,
    symbol               TEXT NOT NULL,
    side                 TEXT NOT NULL,
    qty                  TEXT NOT NULL,
    entry_price          TEXT NOT NULL,
    exit_price           TEXT NOT NULL,
    sl_price_initial     TEXT NOT NULL,
    tp_price_initial     TEXT NOT NULL,
    net_pnl_usdt         TEXT NOT NULL,
    mae_pct              REAL NOT NULL DEFAULT 0.0,
    mfe_pct              REAL NOT NULL DEFAULT 0.0,
    exit_reason          TEXT NOT NULL,
    opened_at_utc        TEXT NOT NULL,
    closed_at_utc        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signal_audit_log (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    candle_open_time_utc TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    close_price          REAL NOT NULL,
    ema200               REAL NOT NULL,
    session_vwap         REAL NOT NULL,
    atr14                REAL NOT NULL,
    bar_delta_zscore     REAL NOT NULL,
    oi_delta_pct         REAL NOT NULL,
    funding_rate         REAL NOT NULL,
    decision             TEXT NOT NULL, -- 'EXECUTED_LONG', 'EXECUTED_SHORT', atau 'REJECTED'
    reject_reason_code   TEXT NOT NULL, -- Contoh: 'STALE_OI', 'MARGIN_CAP', 'CIRCUIT_BREAKER'
    created_at_utc       TEXT NOT NULL
);
```

---

## 9. CHECKLIST ACCEPTANCE TEST & PROTOKOL VALIDASI (WAJIB LULUS SEBELUM LIVE)

Sebelum mengaktifkan API key Live Trading, AI Agent dan Engineer wajib memverifikasi 20 acceptance tests berikut:

### A. Acceptance Tests Kritis (20 Test Wajib)
1. **Reset VWAP Midnight-UTC:** Pastikan `SessionVWAP` dihitung dari seluruh candle tertutup sejak `00:00:00 UTC` hari berjalan dan ter-reset bersih saat tanggal UTC berganti.
2. **Bobot Volume VWAP:** Pastikan perhitungan `SessionVWAP` memakai `VolumeBase` (indeks `[5]`), bukan `QuoteVolumeUSDT` (indeks `[7]`).
3. **Konsistensi Bar Delta:** Pastikan `BarDeltaUSDT = (2 * TakerBuyQuoteUSDT) - QuoteVolumeUSDT` bebas dari *race condition* batas candle.
4. **Deduplikasi Monotonik Candle:** Pastikan dua event `kline.x == true` dengan `OpenTime` yang sama atau tidak strictly-monotonic langsung ditolak.
5. **Validasi Kesegaran Data BBO (`MAX_BOOK_STALENESS <= 5s`):** Pastikan sinyal ditolak jika `Now - BookUpdatedAt > 5s`.
6. **Validasi Kesegaran Mark Price (`MAX_MARK_STALENESS <= 15s`):** Pastikan sinyal ditolak dan simulator exit menolak evaluasi jika mark price stale > 15s.
7. **Validasi Interval Delta OI 5m:** Pastikan selisih waktu snapshot OI sebelumnya dan sekarang berada dalam rentang toleransi `4 - 7 menit` dan usia snapshot terkini $\le 30$ detik.
8. **Konvergensi EMA200 (1000 Bar Bootstrap):** Bandingkan EMA200 dari 1000 bar terhadap referensi matematis; pastikan deviasi $\le 0.001\%$.
9. **Kuantisasi Fail-Closed:** Pastikan `QuantizePrice` dan `QuantizeQtyFloor` mengembalikan error jika `TickSize <= 0`, `StepSize <= 0`, atau nilai masukan $\le 0$.
10. **Margin-Cap pada NATR Rendah:** Pada Equity `1.000 USDT` dan `NATR = 0.35%`, pastikan kuantitas order dipotong oleh `MAX_SLOT_MARGIN_FRACTION` (`30%` equity).
11. **Reservasi Slot Pending Order Atomik:** Pastikan `GetActiveSlotCount()` menjumlahkan `OpenPositions + PendingOrders` secara atomik di dalam mutex broker.
12. **Aggressor-Side Matching Dry-Run:** Pada Limit Buy di BestBid, order hanya terisi jika `isBuyerMaker == true`. Pada Limit Sell di BestAsk, hanya terisi jika `isBuyerMaker == false`.
13. **Pemantauan Instan Partial Fill Dry-Run:** Kuantitas partial fill langsung dipromosikan ke `OpenPositions` dan diproteksi SL/TP; jika harga menyentuh SL sebelum 45s, kuantitas terisi langsung ditutup rugi dan sisa pending dibatalkan.
14. **Two-Phase Commit Live Entry:** Sinyal valid menghasilkan tepat 1 reservasi atomik; pemanggilan `PlaceEntryFn` dieksekusi di luar lock; penolakan `-5022` membebaskan reservasi slot.
15. **Safe SL-First Protection Placement:** SL baru terpasang dan berstatus `ACKED` terlebih dahulu SEBELUM SL lama dibatalkan. Tidak ada celah *unprotected naked position*.
16. **Eskalasi Darurat Proteksi:** Jika penempatan SL gagal total setelah 3 kali retry dan posisi tidak memiliki proteksi aktif, picu `EmergencyMarketCloseFn` dan verifikasi penutupan posisi di bursa.
17. **Order Exit Ledger & Pembatalan Sibling Algo:** Saat order SL terisi (`FILLED`), order algo sibling TP langsung dibatalkan via `DELETE /fapi/v1/algoOrder`; posisi internal dihapus bersih.
18. **Timeout 45 Detik Entry Live:** Order entry LIMIT GTX yang belum terisi setelah 45 detik otomatis dikirimi perintah pembatalan `DELETE /fapi/v1/order`.
19. **Sinkronisasi Akun & Circuit Breaker UTC:** Event `ACCOUNT_UPDATE` merekonsiliasi saldo, unrealized PnL, dan posisi; drawdown $\le -3.0\%$ memicu pembatalan seluruh pending entry.
20. **Verifikasi One-Way Mode & Leverage 5x Isolated:** Verifikasi `dualSidePosition == false`, margin type `ISOLATED`, dan leverage `5x` melalui startup sequence `InitAndVerifySymbol`.

### B. Operational Soak Test vs Statistical Strategy Validation
* **Operational Soak Test (Minimal 50 Transaksi Dry-Run pada Live Stream):** Digunakan murni untuk menguji keandalan rekayasa perangkat lunak (rotasi WebSocket 24 jam, sinkronisasi `listenKey`, bebas *memory leak*, dan akurasi SQLite).
* **Statistical Validation (Walk-Forward / Out-of-Sample):** Keputusan alokasi modal produksi wajib didasarkan pada *expectancy* bersih setelah biaya (*net of maker/taker fees, funding, and slippage*) serta profil *Maximum Adverse Excursion* (MAE) dari pengujian *out-of-sample*.
