========================================================================================
TRML BACKTEST ENGINE REPORT (PESSIMISTIC 100% BINANCE GROUND TRUTH)
========================================================================================
Waktu Eksekusi   : 2026-11-05T18:43:25Z UTC
Commit Hash      : 34e5fba
Rentang Data     : 2024-01-01 s/d 2024-01-31 UTC
Daftar Simbol    : BTCUSDT, SOLUSDT
Model Eksekusi   : LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage 5 bps
Struktur Fee     : Maker 2.0 bps, Taker 5.0 bps, Funding Active
Fitur Aktif      : Kline 5m + Exact Net Bar Delta (Binance Ground Truth)
----------------------------------------------------------------------------------------

### 1. Ringkasan Kinerja & Metrik Eksekusi

| Metrik Performa | Nilai Hasil Uji | Keterangan Standar |
| :--- | :--- | :--- |
| **Total Trade Selesai** | `1` setup | TP1 + Runner diagregasi 1 setup |
| **Win Rate (All-Inclusive)** | `100.00%` | 1 Win / 0 Loss / 0 BE |
| **Adjusted Win Rate (Pure)** | `100.00%` | Win / (Win + Loss), mengabaikan BE |
| **Payoff Ratio (Avg W/L)** | `0.00` | Rata-rata Win ($6.11) / Loss ($0.00) |
| **Break-Even Win Rate** | `0.00%` | Ambang batas minimum impas |
| **Profit Factor** | `99.00` | Total Profit Kotor / Loss Kotor |
| **Total Net PnL** | `+$6.11 USDT` | Setelah potongan fee & slippage |
| **Net Expectancy per Trade** | `+$6.11 USDT` | Rata-rata keuntungan bersih per trade |
| **95% Bootstrap CI** | `[+$0.00, +$0.00]` | Interval kepercayaan 1.000 iterasi |
| **Max Drawdown (Peak-to-Trough)** | `-0.00%` (-$0.00) | Penurunan modal terdalam |
| **Losing Streak Terpanjang** | `0` trade berturut | Rekor kerugian berurutan |
| **Fee & Friction Drag** | `6.49%` | Total fee ($0.42) vs Profit Kotor |

### 2. Distribusi Alasan Keluar (Exit Ladder)

| Alasan Exit | Jumlah Frekuensi | Persentase |
| :--- | :--- | :--- |
| `PARTIAL_TAKE_PROFIT` | 1 | 50.0% |
| `TRAILING_STOP` | 1 | 50.0% |

### 3. Audit Gatekeeper Sinyal (Top Filter Rejections)

| Alasan Penolakan Sinyal | Jumlah Sinyal Disaring |
| :--- | :--- |
| `CONDITIONS_NOT_MET` | 834 |
| `BTC_TREND_BULLISH_SHORT_VETO` | 2 |
| `BTC_TREND_BEARISH_LONG_VETO` | 7 |
| `NATR_OUT_OF_BOUNDS` | 13604 |
| `ENTRY_TOO_FAR_FROM_VWAP` | 2775 |
| `ENTRY_TOO_FAR_FROM_EMA200` | 138 |
| `LOW_SIGNAL_RVOL` | 4 |
| `SYMBOL_ALREADY_ACTIVE` | 2 |
| `SYMBOL_IN_COOLDOWN` | 1 |
| `BTC_TREND_SHORT_VETO_PERSISTENCE` | 3 |
| `BTC_TREND_LONG_VETO_PERSISTENCE` | 1 |
| `VWAP_SESSION_WARMUP` | 84 |

### 4. Evaluasi & Kepatuhan Batas Statistik

> [!WARNING] **Sample Size Alert:** Total trade (< 200 setup) belum memenuhi syarat signifikansi statistik penuh. Perluas rentang tanggal pengujian.

> [!IMPORTANT] **KESIMPULAN: EXPECTANCY POSITIF TIPIS (BORDERLINE).**
> Interval 95% menyentuh wilayah negatif. Disarankan menjalankan `backtest ablate` untuk membuang filter yang menambah friction drag.

========================================================================================
