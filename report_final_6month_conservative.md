========================================================================================
TRML BACKTEST ENGINE REPORT (PESSIMISTIC 100% BINANCE GROUND TRUTH)
========================================================================================
Waktu Eksekusi   : 2026-11-06T07:11:32Z UTC
Commit Hash      : a2c4295
Rentang Data     : 2026-05-01 s/d 2026-10-31 UTC
Daftar Simbol    : 1000PEPEUSDT, 1000SHIBUSDT, AAPLUSDT, AAVEUSDT, ADAUSDT, AEROUSDT, AGTUSDT, AINUSDT, AIOUSDT, AKEUSDT, ALTUSDT, AMDUSDT, AMZNUSDT, APRUSDT, APTUSDT, ARBUSDT, ARIAUSDT, ARMUSDT, ASTERUSDT, ATOMUSDT, AVAXUSDT, AVGOUSDT, BABAUSDT, BATUSDT, BCHUSDT, BEAMXUSDT, BEUSDT, BIRBUSDT, BMTUSDT, BNBUSDT, BRUSDT, BTCUSDT, BTWUSDT, BZUSDT, CBRSUSDT, CLANKERUSDT, CLUSDT, COHRUSDT, COINUSDT, COMPUSDT, CRCLUSDT, CRVUSDT, CRWVUSDT, CTRUSDT, CYBERUSDT, DASHUSDT, DOGEUSDT, DOTUSDT, DRAMUSDT, ELSAUSDT, ENAUSDT, ERAUSDT, ESPORTSUSDT, ETCUSDT, ETHFIUSDT, ETHUSDT, EVAAUSDT, EWYUSDT, FARTCOINUSDT, FETUSDT, FILUSDT, FLUIDUSDT, FOGOUSDT, GOOGLUSDT, GTCUSDT, HBARUSDT, HOODUSDT, HOTUSDT, HUMAUSDT, HYPEUSDT, ICPUSDT, IDOLUSDT, INJUSDT, INTCUSDT, IOSTUSDT, JCTUSDT, LINKUSDT, LITEUSDT, LITUSDT, LSKUSDT, LTCUSDT, LYNUSDT, MAGMAUSDT, METAUSDT, MEWUSDT, MRVLUSDT, MSFTUSDT, MSTRUSDT, MUUSDT, NATGASUSDT, NBISUSDT, NEARUSDT, NVDAUSDT, ONDOUSDT, ONUSDT, OPUSDT, ORCLUSDT, PARTIUSDT, PAXGUSDT, PENGUUSDT, PHAUSDT, PLTRUSDT, PUMPUSDT, QCOMUSDT, QNTUSDT, QQQUSDT, QUSDT, RENDERUSDT, RKLBUSDT, SNDKUSDT, SOLUSDT, SOXLUSDT, SPCXUSDT, SPYUSDT, STRKUSDT, STXUSDT, SUIUSDT, TAOUSDT, TIAUSDT, TRUMPUSDT, TRXUSDT, TSLAUSDT, TSMUSDT, UAIUSDT, UNIUSDT, USELESSUSDT, VVVUSDT, WDCUSDT, WIFUSDT, WLDUSDT, WLFIUSDT, WUSDT, XAGUSDT, XAUTUSDT, XAUUSDT, XLMUSDT, XMRUSDT, XPLUSDT, XRPUSDT, XTZUSDT, ZECUSDT, ZROUSDT
Model Eksekusi   : LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage Active
Struktur Fee     : Maker 2.0 bps, Taker 5.0 bps, Slippage 2.0 bps | Real Binance Funding & Dynamic Spread
Fitur Aktif      : Kline 5m + Exact Net Bar Delta + Real Binance Filters & Funding
----------------------------------------------------------------------------------------

### 1. Ringkasan Kinerja & Metrik Eksekusi

| Metrik Performa | Nilai Hasil Uji | Keterangan Standar |
| :--- | :--- | :--- |
| **Total Trade Selesai** | `811` setup | TP1 + Runner diagregasi 1 setup |
| **Win Rate (All-Inclusive)** | `58.20%` | 472 Win / 339 Loss / 0 BE |
| **Adjusted Win Rate (Pure)** | `58.20%` | Win / (Win + Loss), mengabaikan BE |
| **Payoff Ratio (Avg W/L)** | `0.77` | Rata-rata Win ($1.34) / Loss ($1.75) |
| **Break-Even Win Rate** | `56.59%` | Ambang batas minimum impas |
| **Profit Factor** | `1.20` | Total Profit Kotor / Loss Kotor |
| **Total Net PnL** | `+$40.32 USDT` | Setelah potongan fee & slippage |
| **Net Expectancy per Trade** | `+$0.05 USDT` | Rata-rata keuntungan bersih per trade |
| **95% Bootstrap CI** | `[-$0.06, +$0.16]` | Interval kepercayaan 1.000 iterasi |
| **Max Drawdown (Peak-to-Trough)** | `-19.65%` (-$29.42) | Penurunan modal terdalam |
| **Losing Streak Terpanjang** | `9` trade berturut | Rekor kerugian berurutan |
| **Fee & Friction Drag** | `10.58%` | Total fee ($71.32) vs Profit Kotor |
| **Net Funding Fee** | `-$0.55 USDT` | Total pendanaan Binance dibayar/diterima |

### 2. Distribusi Alasan Keluar (Exit Ladder)

| Alasan Exit | Jumlah Frekuensi | Persentase |
| :--- | :--- | :--- |
| `PARTIAL_TAKE_PROFIT` | 458 | 36.1% |
| `TRAILING_STOP` | 380 | 29.9% |
| `BREAK_EVEN` | 75 | 5.9% |
| `STOP_LOSS` | 329 | 25.9% |
| `TIME_STOP_MAX_HOLDING` | 18 | 1.4% |
| `TAKE_PROFIT` | 9 | 0.7% |

### 3. Log Eksekusi Trade Selesai

| No | Waktu Entry (UTC) | Simbol | Posisi | Entry Price | Exit Price | Alasan Exit | Net PnL | Fee | Funding |
| :- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 2026-05-02 03:30 | `BRUSDT` | SHORT | 0.1748 | 0.1724 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0304 | $-0.0079 |
| 2 | 2026-05-02 03:30 | `BRUSDT` | SHORT | 0.1748 | 0.1718 | `TRAILING_STOP` | `+$0.88` | $0.0371 | $-0.0097 |
| 3 | 2026-05-02 10:25 | `MAGMAUSDT` | SHORT | 0.2301 | 0.2272 | `PARTIAL_TAKE_PROFIT` | `+$0.57` | $0.0332 | $0.0000 |
| 4 | 2026-05-02 10:25 | `MAGMAUSDT` | SHORT | 0.2301 | 0.2270 | `TRAILING_STOP` | `+$0.74` | $0.0407 | $0.0000 |
| 5 | 2026-05-03 02:55 | `ONUSDT` | SHORT | 0.1350 | 0.1339 | `PARTIAL_TAKE_PROFIT` | `+$0.50` | $0.0469 | $0.0000 |
| 6 | 2026-05-03 02:55 | `ONUSDT` | SHORT | 0.1350 | 0.1337 | `TRAILING_STOP` | `+$0.72` | $0.0574 | $0.0000 |
| 7 | 2026-05-05 01:05 | `DASHUSDT` | SHORT | 47.0600 | 46.4000 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0301 | $0.0000 |
| 8 | 2026-05-05 01:05 | `DASHUSDT` | SHORT | 47.0600 | 47.0294 | `BREAK_EVEN` | `-$0.00` | $0.0372 | $0.0000 |
| 9 | 2026-05-06 14:00 | `XMRUSDT` | SHORT | 419.0700 | 424.2348 | `STOP_LOSS` | `-$1.87` | $0.1015 | $0.0000 |
| 10 | 2026-05-06 16:55 | `BRUSDT` | SHORT | 0.1670 | 0.1644 | `PARTIAL_TAKE_PROFIT` | `+$0.59` | $0.0274 | $0.0000 |
| 11 | 2026-05-06 16:55 | `BRUSDT` | SHORT | 0.1670 | 0.1645 | `TRAILING_STOP` | `+$0.68` | $0.0337 | $0.0000 |
| 12 | 2026-05-06 22:20 | `DASHUSDT` | SHORT | 53.2900 | 52.8000 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0448 | $0.0000 |
| 13 | 2026-05-06 22:20 | `DASHUSDT` | SHORT | 53.2900 | 51.8004 | `TRAILING_STOP` | `+$2.15` | $0.0540 | $0.0000 |
| 14 | 2026-05-07 00:45 | `ZECUSDT` | SHORT | 549.9700 | 555.6111 | `STOP_LOSS` | `-$1.64` | $0.1055 | $0.0000 |
| 15 | 2026-05-07 02:15 | `LYNUSDT` | SHORT | 0.0650 | 0.0657 | `STOP_LOSS` | `-$1.77` | $0.1058 | $0.0000 |
| 16 | 2026-05-07 04:35 | `TIAUSDT` | SHORT | 0.3770 | 0.3813 | `STOP_LOSS` | `-$1.80` | $0.1056 | $0.0000 |
| 17 | 2026-05-07 12:45 | `UAIUSDT` | SHORT | 0.3250 | 0.3220 | `PARTIAL_TAKE_PROFIT` | `+$0.54` | $0.0443 | $0.0000 |
| 18 | 2026-05-07 12:45 | `UAIUSDT` | SHORT | 0.3250 | 0.3218 | `TRAILING_STOP` | `+$0.72` | $0.0542 | $0.0000 |
| 19 | 2026-05-07 15:00 | `ARBUSDT` | SHORT | 0.1261 | 0.1251 | `PARTIAL_TAKE_PROFIT` | `+$0.50` | $0.0470 | $0.0000 |
| 20 | 2026-05-07 15:00 | `ARBUSDT` | SHORT | 0.1261 | 0.1251 | `TRAILING_STOP` | `+$0.60` | $0.0574 | $0.0000 |
| 21 | 2026-05-07 16:00 | `INTCUSDT` | SHORT | 111.0400 | 110.0700 | `PARTIAL_TAKE_PROFIT` | `+$0.45` | $0.0386 | $0.0000 |
| 22 | 2026-05-07 16:00 | `INTCUSDT` | SHORT | 111.0400 | 109.8720 | `TRAILING_STOP` | `+$0.87` | $0.0617 | $0.0000 |
| 23 | 2026-05-08 15:25 | `DASHUSDT` | SHORT | 49.6900 | 49.2800 | `PARTIAL_TAKE_PROFIT` | `+$0.52` | $0.0470 | $-0.0067 |
| 24 | 2026-05-08 15:25 | `DASHUSDT` | SHORT | 49.6900 | 49.2398 | `TRAILING_STOP` | `+$0.70` | $0.0574 | $-0.0082 |
| 25 | 2026-05-10 06:40 | `VVVUSDT` | SHORT | 15.3460 | 15.2160 | `PARTIAL_TAKE_PROFIT` | `+$0.52` | $0.0469 | $0.0000 |
| 26 | 2026-05-10 06:40 | `VVVUSDT` | SHORT | 15.3460 | 15.2300 | `TRAILING_STOP` | `+$0.57` | $0.0575 | $0.0000 |
| 27 | 2026-05-10 11:20 | `VVVUSDT` | SHORT | 15.3070 | 15.1840 | `PARTIAL_TAKE_PROFIT` | `+$0.49` | $0.0469 | $0.0000 |
| 28 | 2026-05-10 11:20 | `VVVUSDT` | SHORT | 15.3070 | 15.1930 | `TRAILING_STOP` | `+$0.56` | $0.0574 | $0.0000 |
| 29 | 2026-05-10 11:30 | `ONUSDT` | SHORT | 0.1617 | 0.1650 | `STOP_LOSS` | `-$1.88` | $0.0625 | $0.0000 |
| 30 | 2026-05-10 12:30 | `VVVUSDT` | SHORT | 15.2500 | 15.1340 | `PARTIAL_TAKE_PROFIT` | `+$0.47` | $0.0469 | $0.0000 |
| 31 | 2026-05-10 12:30 | `VVVUSDT` | SHORT | 15.2500 | 15.0230 | `TRAILING_STOP` | `+$1.17` | $0.0571 | $0.0000 |
| 32 | 2026-05-10 14:25 | `LYNUSDT` | SHORT | 0.0557 | 0.0552 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0441 | $0.0000 |
| 33 | 2026-05-10 14:25 | `LYNUSDT` | SHORT | 0.0557 | 0.0557 | `BREAK_EVEN` | `-$0.00` | $0.0544 | $0.0000 |
| 34 | 2026-05-10 15:15 | `ONUSDT` | SHORT | 0.1612 | 0.1594 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0371 | $-0.0027 |
| 35 | 2026-05-10 15:15 | `ONUSDT` | SHORT | 0.1612 | 0.1595 | `TRAILING_STOP` | `+$0.63` | $0.0454 | $-0.0032 |
| 36 | 2026-05-10 21:15 | `AVAXUSDT` | SHORT | 10.0560 | 10.1650 | `STOP_LOSS` | `-$1.63` | $0.0993 | $0.0000 |
| 37 | 2026-05-10 21:15 | `TAOUSDT` | SHORT | 316.7700 | 321.5643 | `STOP_LOSS` | `-$1.87` | $0.0836 | $0.0000 |
| 38 | 2026-05-11 14:35 | `ICPUSDT` | SHORT | 3.4160 | 3.3730 | `PARTIAL_TAKE_PROFIT` | `+$0.53` | $0.0308 | $0.0000 |
| 39 | 2026-05-11 14:35 | `ICPUSDT` | SHORT | 3.4160 | 3.3697 | `TRAILING_STOP` | `+$0.79` | $0.0426 | $0.0000 |
| 40 | 2026-05-11 17:15 | `XPLUSDT` | SHORT | 0.1033 | 0.1045 | `STOP_LOSS` | `-$1.79` | $0.1058 | $0.0000 |
| 41 | 2026-05-12 12:25 | `TIAUSDT` | SHORT | 0.4482 | 0.4446 | `PARTIAL_TAKE_PROFIT` | `+$0.49` | $0.0468 | $0.0000 |
| 42 | 2026-05-12 12:25 | `TIAUSDT` | SHORT | 0.4482 | 0.4414 | `TRAILING_STOP` | `+$1.20` | $0.0571 | $0.0000 |
| 43 | 2026-05-12 14:15 | `INTCUSDT` | SHORT | 123.6500 | 122.2000 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0343 | $0.0000 |
| 44 | 2026-05-12 14:15 | `INTCUSDT` | SHORT | 123.6500 | 119.4839 | `TRAILING_STOP` | `+$2.04` | $0.0422 | $0.0000 |
| 45 | 2026-05-12 16:25 | `QUSDT` | SHORT | 0.0148 | 0.0150 | `STOP_LOSS` | `-$1.88` | $0.0839 | $0.0000 |
| 46 | 2026-05-12 15:50 | `NEARUSDT` | SHORT | 1.5450 | 1.5300 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0430 | $0.0041 |
| 47 | 2026-05-12 15:50 | `NEARUSDT` | SHORT | 1.5450 | 1.5273 | `TRAILING_STOP` | `+$0.84` | $0.0547 | $0.0053 |
| 48 | 2026-05-12 17:30 | `SUIUSDT` | SHORT | 1.2533 | 1.2429 | `PARTIAL_TAKE_PROFIT` | `+$0.51` | $0.0469 | $0.0000 |
| 49 | 2026-05-12 17:30 | `SUIUSDT` | SHORT | 1.2533 | 1.2407 | `TRAILING_STOP` | `+$0.77` | $0.0573 | $0.0000 |
| 50 | 2026-05-13 05:45 | `GTCUSDT` | SHORT | 0.1388 | 0.1372 | `PARTIAL_TAKE_PROFIT` | `+$0.57` | $0.0365 | $0.0000 |
| ... | *Menampilkan 50 dari 1269 trade total* | | | | | | | | |

### 4. Analisis Segmentasi Trade (Bebas Path-Dependency)

#### A. Segmentasi Berdasarkan Volatilitas (NATR)

| Kategori NATR | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `NATR < 0.80%` | 432 | 254 W / 178 L | 58.8% | `+$11.45` | `+$0.03` | 1.19 |
| `NATR >= 0.80%` | 379 | 218 W / 161 L | 57.5% | `+$28.88` | `+$0.08` | 1.21 |

#### B. Segmentasi Berdasarkan Sesi Waktu (UTC)

| Sesi Pasar | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `Asia (00:00 - 08:00 UTC)` | 249 | 150 W / 99 L | 60.2% | `+$27.60` | `+$0.11` | 1.29 |
| `London (08:00 - 14:00 UTC)` | 163 | 92 W / 71 L | 56.4% | `+$0.04` | `+$0.00` | 1.12 |
| `New York (14:00 - 21:00 UTC)` | 366 | 207 W / 159 L | 56.6% | `-$1.75` | `-$0.00` | 1.12 |
| `NY Close / Asia Early (21:00 - 24:00 UTC)` | 33 | 23 W / 10 L | 69.7% | `+$14.43` | `+$0.44` | 2.00 |

#### C. Segmentasi Berdasarkan Arah Posisi (LONG vs SHORT)

| Arah Posisi | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `LONG Positions` | 0 | 0 W / 0 L | 0.0% | `$0.00` | `$0.00` | 0.00 |
| `SHORT Positions` | 811 | 472 W / 339 L | 58.2% | `+$40.32` | `+$0.05` | 1.20 |

### 5. Audit Gatekeeper Sinyal (Top Filter Rejections)

| Alasan Penolakan Sinyal | Jumlah Sinyal Disaring |
| :--- | :--- |
| `ENTRY_TOO_FAR_FROM_EMA200` | 20621 |
| `SYMBOL_ALREADY_ACTIVE` | 6305 |
| `LOW_SIGNAL_RVOL` | 135 |
| `BTC_COUNTER_MOMENTUM_PUMP` | 46 |
| `EXPLOSIVE_PUMP_FRENZY` | 102 |
| `SIZING_FAILED` | 16 |
| `CONDITIONS_NOT_MET` | 88283 |
| `SIDE_DISABLED_SHORT_ONLY` | 751 |
| `NATR_OUT_OF_BOUNDS` | 4788 |
| `SYMBOL_IN_COOLDOWN` | 1066 |
| `SPREAD_EXCEEDED` | 52 |
| `ENTRY_TOO_FAR_FROM_VWAP` | 104651 |
| `VWAP_SESSION_WARMUP` | 3739 |

### 6. Evaluasi & Kepatuhan Batas Statistik

> [!IMPORTANT] **KESIMPULAN: EXPECTANCY POSITIF TIPIS (BORDERLINE).**
> Interval 95% menyentuh wilayah negatif. Disarankan menjalankan `backtest ablate` untuk membuang filter yang menambah friction drag.

========================================================================================
