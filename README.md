# TRML - Binance USDⓈ-M Futures Intraday Trading Bot (Go Modular v2.1)

TRML adalah bot trading algoritmik intraday berkecepatan tinggi untuk **Binance USDⓈ-M Futures**, dibangun dengan arsitektur modular Golang, konkurensi nir-kontensi (*zero-contention*), dan dashboard observabilitas Web/PWA real-time.

---

## 🚀 Fitur Utama

- **VWAP Pullback & Retest with Orderflow Bounce:**
  - Konfirmasi 5 lapis: Tren EMA200, Posisi Relatif Nilai Wajar VWAP (Session Midnight UTC), Bar Delta Z-Score, Pertumbuhan Open Interest (OI), dan Filter Anti-Chasing (Maks 0.85 ATR).
  - Gerbang Makro Bitcoin (Two-Tiered Soft Veto): Mencegah altcoin entry saat BTC mengalami dump/pump liar.
- **Manajemen Risiko Terukur:**
  - Sizing risiko dinamis (default: 1% equity per trade).
  - Stop Loss berbasis ATR ($1.5\times$ ATR) dan Take Profit ($2.25\times$ ATR / R:R 1:1.5).
  - Trailing Break-Even otomatis ($1.15\times$ ATR) untuk mengunci posisi modal (+ buffer fee bursa).
  - Circuit Breaker Harian: Menghentikan trading otomatis jika drawdown mencapai $\ge 3\%$.
  - Batas maksimal 3 slot posisi aktif bersamaan.
- **Simulasi Heuristic Dry-Run Realistis:**
  - Menghitung fee maker/taker riil Binance Futures secara *round-trip*.
  - Pemodelan *slippage* dan eksekusi berbasis *mark price* serta orderbook *bookTicker*.
- **Observabilitas Real-Time (Web Dashboard & PWA):**
  - Antarmuka Binance Dark Theme responsif (Desktop, Tablet, Mobile PWA).
  - Visualisasi KPI Portofolio, Radar Pantauan Top 10 Koin, Signal Audit Feed, dan Trade Ledger.
  - Multi-Reader SQLite (WAL Mode) terisolasi agar pembacaan web tidak mengganggu engine trading.

---

## 🏗️ Arsitektur Sistem

```text
       ┌─────────────────── Binance Futures API & WebSockets ───────────────────┐
       │ (aggTrade, markPrice@1s, kline_5m, bookTicker, Open Interest REST API)  │
       └──────────────────────────────────┬─────────────────────────────────────┘
                                          │
                                          ▼
                         ┌─────────────────────────────────┐
                         │       TRML Core Engine          │
                         │  - Scanner (Top 10 High RVOL)   │
                         │  - Strategy Evaluator (5 Gates) │
                         │  - Execution Broker (Live/Dry)  │
                         └────────────────┬────────────────┘
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │      SQLite Database (data/trml.db)   │
                      │   - WAL Mode & Single-Writer Mutex    │
                      │   - Signal Audit Log & Trade Ledger   │
                      └───────────────────┬───────────────────┘
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │    Read-Only Web Dashboard (:8080)    │
                      │   - PWA Static Assets (Embedded FS)   │
                      │   - REST API & Real-Time Polling      │
                      └───────────────────────────────────────┘
```

---

## 📋 Persyaratan Sistem

- **Go:** Versi `1.22` atau lebih baru
- **SQLite:** Dukungan CGO / modernc sqlite (sudah termasuk via Go module)
- **Koneksi Internet:** Akses stabil ke `fapi.binance.com` dan `fstream.binance.com`

---

## ⚙️ Instalasi & Konfigurasi

1. **Clone Repositori:**
   ```bash
   git clone https://github.com/FarikhNaufal/trml.git
   cd trml
   ```

2. **Siapkan Konfigurasi Lingkungan (`.env`):**
   Salin berkas template `.env.example` ke `.env`:
   ```bash
   cp .env.example .env
   ```
   Sesuaikan parameter di dalam `.env`:
   ```env
   # Kredensial Binance (Hanya diperlukan jika DRY_RUN=false)
   BINANCE_API_KEY=your_api_key_here
   BINANCE_API_SECRET=your_api_secret_here

   # Mode Operasi (true = Simulasi Dry-Run, false = Real Live Trading)
   DRY_RUN=true
   INITIAL_EQUITY=1000.0

   # Manajemen Risiko
   RISK_PER_TRADE_PCT=0.01
   MAX_SLOTS=3
   MAX_DAILY_DRAWDOWN_PCT=0.03
   SL_ATR_MULTIPLIER=1.5
   TP_ATR_MULTIPLIER=2.25
   BREAK_EVEN_TRIGGER_ATR=1.15

   # Port Dashboard
   HTTP_PORT=8080
   ```

3. **Unduh Dependensi:**
   ```bash
   go mod download
   ```

4. **Jalankan Pengujian Unit:**
   ```bash
   go test -v ./...
   ```

---

## 🖥️ Menjalankan Bot

### Mode Pengembangan:
```bash
go run ./cmd/bot
```

### Mode Kompilasi Binary (Produksi):
```bash
go build -o ./bin/bot ./cmd/bot
./bin/bot
```

Setelah berjalan, buka dashboard di browser:
👉 **`http://localhost:8080/`**

---

## 📊 Struktur Proyek

```text
├── cmd/
│   └── bot/                # Entrypoint aplikasi utama
├── internal/
│   ├── broker/             # Broker eksekusi (Dry-run simulator & Binance Live)
│   ├── config/             # Pengelola environment dan konfigurasi sistem
│   ├── exchange/           # Klien REST API Binance Futures
│   ├── indicator/          # Perhitungan teknikal (EMA, ATR, VWAP, Delta Z-Score)
│   ├── model/              # Struktur data inti, order intent, dan snapshot pasar
│   ├── scanner/            # Pemindaian pasar Top 10 volatile & bootstrap klines
│   ├── storage/            # Single-writer SQLite persistent database
│   ├── strategy/           # Evaluator 5 konfirmasi sinyal & kalkulator sizing
│   ├── web/                # HTTP Server, REST API read-only, dan PWA embedded assets
│   └── ws/                 # WebSocket stream manager Binance dengan auto-reconnect
├── test/                   # Acceptance & end-to-end integration tests
├── .env.example            # Template konfigurasi environment
├── .gitignore              # Proteksi secrets (.env, database, binaries)
└── README.md
```

---

## 🔒 Keamanan & Lisensi

- Berkas `.env` dan database `data/` telah dikecualikan di `.gitignore` untuk mencegah kebocoran kunci API dan histori perdagangan riil.
- Didistribusikan di bawah lisensi MIT. Gunakan dengan bijak dan selalu uji di mode `DRY_RUN=true` sebelum beralih ke real trading.
