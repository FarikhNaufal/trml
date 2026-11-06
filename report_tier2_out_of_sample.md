========================================================================================
TRML BACKTEST ENGINE REPORT (PESSIMISTIC 100% BINANCE GROUND TRUTH)
========================================================================================
Waktu Eksekusi   : 2026-11-06T07:19:14Z UTC
Commit Hash      : 6e84dff
Rentang Data     : 2026-09-01 s/d 2026-10-31 UTC
Daftar Simbol    : 1000PEPEUSDT, 1000SHIBUSDT, AAOIUSDT, AAPLUSDT, AAVEUSDT, ADAUSDT, AEROUSDT, AGTUSDT, AINUSDT, AIOUSDT, AKEUSDT, ALTUSDT, AMDUSDT, AMZNUSDT, ANTHROPICUSDT, APPUSDT, APRUSDT, APTUSDT, ARBUSDT, ARIAUSDT, ARMUSDT, ASTERUSDT, ATOMUSDT, AVAXUSDT, AVGOUSDT, AXTIUSDT, BABAUSDT, BATUSDT, BCHUSDT, BEAMXUSDT, BEUSDT, BIRBUSDT, BMNRUSDT, BMTUSDT, BNBUSDT, BRUSDT, BTCUSDT, BTWUSDT, BZUSDT, CAPUSDT, CBRSUSDT, CLANKERUSDT, CLUSDT, COHRUSDT, COINUSDT, COMPUSDT, CRCLUSDT, CRVUSDT, CRWVUSDT, CSOPSKHYNIX2LUSDT, CTRUSDT, CXMTUSDT, CYBERUSDT, DASHUSDT, DELLUSDT, DOGEUSDT, DOTUSDT, DRAMUSDT, ELSAUSDT, ENAUSDT, ERAUSDT, ESPORTSUSDT, ETCUSDT, ETHFIUSDT, ETHUSDT, EVAAUSDT, EWYUSDT, FARTCOINUSDT, FETUSDT, FILUSDT, FLUIDUSDT, FOGOUSDT, GOOGLUSDT, GTCUSDT, HBARUSDT, HOODUSDT, HOTUSDT, HUMAUSDT, HYPEUSDT, ICPUSDT, IDOLUSDT, INJUSDT, INTCUSDT, INTWUSDT, IOSTUSDT, IRENUSDT, JCTUSDT, KORUUSDT, KSTRUSDT, LINKUSDT, LITEUSDT, LITUSDT, LSKUSDT, LTCUSDT, LYNUSDT, MAGMAUSDT, METAUSDT, MEWUSDT, MINIMAXUSDT, MRNAUSDT, MRVLUSDT, MSFTUSDT, MSTRUSDT, MUUSDT, MUUUSDT, MVLLUSDT, NATGASUSDT, NBISUSDT, NEARUSDT, NVDAUSDT, ONDOUSDT, ONUSDT, OPUSDT, ORCLUSDT, PARTIUSDT, PAXGUSDT, PENGUUSDT, PHAUSDT, PLTRUSDT, PONSUSDT, PUMPUSDT, QCOMUSDT, QNTUSDT, QQQUSDT, QUSDT, RENDERUSDT, RKLBUSDT, SAMSUNGUSDT, SKDDUSDT, SKHYNIXUSDT, SKHYUSDT, SKUUUSDT, SNDKUSDT, SNXXUSDT, SOLUSDT, SOXLUSDT, SOXSUSDT, SPCXUSDT, SPYUSDT, SQQQUSDT, STRKUSDT, STXUSDT, SUIUSDT, TAOUSDT, TIAUSDT, TQQQUSDT, TRUMPUSDT, TRXUSDT, TSLAUSDT, TSMUSDT, UAIUSDT, UNITREEUSDT, UNIUSDT, USELESSUSDT, UVXYUSDT, VVVUSDT, WDCUSDT, WIFUSDT, WLDUSDT, WLFIUSDT, WUSDT, XAGUSDT, XAUTUSDT, XAUUSDT, XLMUSDT, XMRUSDT, XPLUSDT, XRPUSDT, XTZUSDT, ZECUSDT, ZHIPUUSDT, ZHONGJIUSDT, ZROUSDT
Model Eksekusi   : LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage Active
Struktur Fee     : Maker 2.0 bps, Taker 5.0 bps, Slippage 2.0 bps | Real Binance Funding & Dynamic Spread
Fitur Aktif      : Kline 5m + Exact Net Bar Delta + Real Binance Filters & Funding
----------------------------------------------------------------------------------------

### 1. Ringkasan Kinerja & Metrik Eksekusi

| Metrik Performa | Nilai Hasil Uji | Keterangan Standar |
| :--- | :--- | :--- |
| **Total Trade Selesai** | `329` setup | TP1 + Runner diagregasi 1 setup |
| **Win Rate (All-Inclusive)** | `59.27%` | 195 Win / 134 Loss / 0 BE |
| **Adjusted Win Rate (Pure)** | `59.27%` | Win / (Win + Loss), mengabaikan BE |
| **Payoff Ratio (Avg W/L)** | `0.75` | Rata-rata Win ($1.34) / Loss ($1.78) |
| **Break-Even Win Rate** | `56.99%` | Ambang batas minimum impas |
| **Profit Factor** | `1.23` | Total Profit Kotor / Loss Kotor |
| **Total Net PnL** | `+$23.45 USDT` | Setelah potongan fee & slippage |
| **Net Expectancy per Trade** | `+$0.07 USDT` | Rata-rata keuntungan bersih per trade |
| **95% Bootstrap CI** | `[-$0.10, +$0.25]` | Interval kepercayaan 1.000 iterasi |
| **Max Drawdown (Peak-to-Trough)** | `-19.12%` (-$25.50) | Penurunan modal terdalam |
| **Losing Streak Terpanjang** | `7` trade berturut | Rekor kerugian berurutan |
| **Fee & Friction Drag** | `10.50%` | Total fee ($29.32) vs Profit Kotor |
| **Net Funding Fee** | `-$0.24 USDT` | Total pendanaan Binance dibayar/diterima |

### 2. Distribusi Alasan Keluar (Exit Ladder)

| Alasan Exit | Jumlah Frekuensi | Persentase |
| :--- | :--- | :--- |
| `TAKE_PROFIT` | 1 | 0.2% |
| `STOP_LOSS` | 131 | 25.1% |
| `PARTIAL_TAKE_PROFIT` | 192 | 36.9% |
| `TRAILING_STOP` | 156 | 29.9% |
| `BREAK_EVEN` | 36 | 6.9% |
| `TIME_STOP_MAX_HOLDING` | 5 | 1.0% |

### 3. Log Eksekusi Trade Selesai

| No | Waktu Entry (UTC) | Simbol | Posisi | Entry Price | Exit Price | Alasan Exit | Net PnL | Fee | Funding |
| :- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 2026-09-01 19:10 | `FETUSDT` | SHORT | 0.1535 | 0.1553 | `STOP_LOSS` | `-$1.88` | $0.1048 | $0.0000 |
| 2 | 2026-09-02 03:30 | `AKEUSDT` | SHORT | 0.0087 | 0.0087 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0454 | $0.0000 |
| 3 | 2026-09-02 03:30 | `AKEUSDT` | SHORT | 0.0087 | 0.0087 | `TRAILING_STOP` | `+$0.63` | $0.0555 | $0.0000 |
| 4 | 2026-09-02 14:00 | `CRCLUSDT` | SHORT | 88.0900 | 89.1078 | `STOP_LOSS` | `-$1.84` | $0.1057 | $0.0000 |
| 5 | 2026-09-02 14:35 | `ZECUSDT` | SHORT | 812.1900 | 805.7700 | `PARTIAL_TAKE_PROFIT` | `+$0.48` | $0.0464 | $0.0000 |
| 6 | 2026-09-02 14:35 | `ZECUSDT` | SHORT | 812.1900 | 811.5423 | `BREAK_EVEN` | `+$0.01` | $0.0580 | $0.0000 |
| 7 | 2026-09-03 13:30 | `BTWUSDT` | SHORT | 0.4435 | 0.4533 | `STOP_LOSS` | `-$1.87` | $0.0583 | $0.0000 |
| 8 | 2026-09-03 14:05 | `MUUUSDT` | SHORT | 29.4800 | 28.9800 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0245 | $0.0000 |
| 9 | 2026-09-03 14:05 | `MUUUSDT` | SHORT | 29.4800 | 29.4659 | `BREAK_EVEN` | `-$0.01` | $0.0309 | $0.0000 |
| 10 | 2026-09-04 10:30 | `BTWUSDT` | SHORT | 0.4491 | 0.4548 | `STOP_LOSS` | `-$1.87` | $0.0990 | $0.0000 |
| 11 | 2026-09-04 12:00 | `BTWUSDT` | SHORT | 0.4487 | 0.4536 | `STOP_LOSS` | `-$1.74` | $0.1057 | $0.0000 |
| 12 | 2026-09-04 13:30 | `AXTIUSDT` | SHORT | 57.6600 | 57.0500 | `PARTIAL_TAKE_PROFIT` | `+$0.51` | $0.0361 | $0.0000 |
| 13 | 2026-09-04 13:35 | `UNIUSDT` | SHORT | 6.2340 | 6.1660 | `PARTIAL_TAKE_PROFIT` | `+$0.51` | $0.0346 | $0.0000 |
| 14 | 2026-09-04 13:30 | `AXTIUSDT` | SHORT | 57.6600 | 56.3213 | `TRAILING_STOP` | `+$1.56` | $0.0476 | $0.0000 |
| 15 | 2026-09-04 13:35 | `UNIUSDT` | SHORT | 6.2340 | 6.2292 | `BREAK_EVEN` | `+$0.00` | $0.0480 | $0.0000 |
| 16 | 2026-09-04 13:55 | `XMRUSDT` | SHORT | 528.7800 | 522.9400 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0378 | $0.0000 |
| 17 | 2026-09-04 13:55 | `XMRUSDT` | SHORT | 528.7800 | 522.9946 | `TRAILING_STOP` | `+$0.68` | $0.0463 | $0.0000 |
| 18 | 2026-09-04 19:45 | `XPLUSDT` | SHORT | 0.0945 | 0.0936 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0463 | $-0.0033 |
| 19 | 2026-09-04 19:45 | `XPLUSDT` | SHORT | 0.0945 | 0.0937 | `TRAILING_STOP` | `+$0.63` | $0.0567 | $-0.0041 |
| 20 | 2026-09-06 15:15 | `PUMPUSDT` | SHORT | 0.0040 | 0.0039 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0469 | $0.0000 |
| 21 | 2026-09-06 15:15 | `PUMPUSDT` | SHORT | 0.0040 | 0.0039 | `TRAILING_STOP` | `+$1.76` | $0.0568 | $0.0000 |
| 22 | 2026-09-07 02:00 | `DASHUSDT` | SHORT | 69.6900 | 70.7041 | `STOP_LOSS` | `-$1.88` | $0.0869 | $0.0000 |
| 23 | 2026-09-07 06:45 | `USELESSUSDT` | SHORT | 0.2160 | 0.2127 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0275 | $0.0000 |
| 24 | 2026-09-07 06:45 | `USELESSUSDT` | SHORT | 0.2160 | 0.2158 | `BREAK_EVEN` | `+$0.00` | $0.0340 | $0.0000 |
| 25 | 2026-09-07 15:35 | `TRUMPUSDT` | SHORT | 2.2540 | 2.2350 | `PARTIAL_TAKE_PROFIT` | `+$0.52` | $0.0470 | $0.0000 |
| 26 | 2026-09-07 15:35 | `TRUMPUSDT` | SHORT | 2.2540 | 2.2384 | `TRAILING_STOP` | `+$0.51` | $0.0575 | $-0.0023 |
| 27 | 2026-09-08 03:50 | `ARBUSDT` | SHORT | 0.1686 | 0.1706 | `STOP_LOSS` | `-$1.88` | $0.1023 | $0.0000 |
| 28 | 2026-09-08 14:10 | `SOXLUSDT` | SHORT | 122.3900 | 124.3849 | `STOP_LOSS` | `-$1.87` | $0.0780 | $0.0000 |
| 29 | 2026-09-08 16:45 | `ZROUSDT` | SHORT | 1.1244 | 1.1367 | `STOP_LOSS` | `-$1.75` | $0.1058 | $0.0000 |
| 30 | 2026-09-08 17:45 | `DASHUSDT` | SHORT | 62.6700 | 62.1100 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0462 | $0.0000 |
| 31 | 2026-09-08 17:45 | `DASHUSDT` | SHORT | 62.6700 | 62.1424 | `TRAILING_STOP` | `+$0.63` | $0.0565 | $0.0000 |
| 32 | 2026-09-08 20:05 | `UAIUSDT` | SHORT | 0.7244 | 0.7163 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0372 | $0.0000 |
| 33 | 2026-09-08 20:05 | `UAIUSDT` | SHORT | 0.7244 | 0.7167 | `TRAILING_STOP` | `+$0.65` | $0.0458 | $0.0000 |
| 34 | 2026-09-09 00:35 | `KORUUSDT` | SHORT | 23.9000 | 24.1748 | `STOP_LOSS` | `-$1.81` | $0.1046 | $0.0000 |
| 35 | 2026-09-09 05:00 | `UAIUSDT` | SHORT | 0.7259 | 0.7184 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0398 | $0.0000 |
| 36 | 2026-09-09 05:00 | `UAIUSDT` | SHORT | 0.7259 | 0.7189 | `TRAILING_STOP` | `+$0.63` | $0.0495 | $0.0000 |
| 37 | 2026-09-09 10:20 | `PONSUSDT` | SHORT | 0.7847 | 0.7741 | `PARTIAL_TAKE_PROFIT` | `+$0.57` | $0.0310 | $0.0000 |
| 38 | 2026-09-09 10:20 | `PONSUSDT` | SHORT | 0.7847 | 0.7842 | `BREAK_EVEN` | `-$0.00` | $0.0390 | $0.0000 |
| 39 | 2026-09-09 14:35 | `XPLUSDT` | SHORT | 0.0969 | 0.0961 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0456 | $0.0000 |
| 40 | 2026-09-09 14:45 | `FARTCOINUSDT` | SHORT | 0.1663 | 0.1647 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0434 | $0.0000 |
| 41 | 2026-09-09 14:35 | `XPLUSDT` | SHORT | 0.0969 | 0.0960 | `TRAILING_STOP` | `+$0.74` | $0.0557 | $0.0000 |
| 42 | 2026-09-09 14:45 | `FARTCOINUSDT` | SHORT | 0.1663 | 0.1648 | `TRAILING_STOP` | `+$0.62` | $0.0530 | $0.0000 |
| 43 | 2026-09-10 01:50 | `ETHFIUSDT` | SHORT | 0.6035 | 0.6104 | `STOP_LOSS` | `-$1.83` | $0.1058 | $0.0000 |
| 44 | 2026-09-10 08:00 | `AKEUSDT` | SHORT | 0.0154 | 0.0152 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0443 | $0.0000 |
| 45 | 2026-09-10 08:00 | `AKEUSDT` | SHORT | 0.0154 | 0.0153 | `TRAILING_STOP` | `+$0.60` | $0.0542 | $0.0000 |
| 46 | 2026-09-10 12:55 | `ETHFIUSDT` | SHORT | 0.6184 | 0.6100 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0313 | $0.0000 |
| 47 | 2026-09-10 12:55 | `ETHFIUSDT` | SHORT | 0.6184 | 0.6102 | `TRAILING_STOP` | `+$0.69` | $0.0383 | $0.0000 |
| 48 | 2026-09-10 13:10 | `INJUSDT` | SHORT | 5.9840 | 5.9330 | `PARTIAL_TAKE_PROFIT` | `+$0.52` | $0.0466 | $0.0000 |
| 49 | 2026-09-10 13:10 | `INJUSDT` | SHORT | 5.9840 | 5.9172 | `TRAILING_STOP` | `+$0.86` | $0.0573 | $0.0000 |
| 50 | 2026-09-10 13:55 | `MRVLUSDT` | SHORT | 229.2100 | 232.1764 | `STOP_LOSS` | `-$1.56` | $0.0810 | $0.0000 |
| ... | *Menampilkan 50 dari 521 trade total* | | | | | | | | |

### 4. Analisis Segmentasi Trade (Bebas Path-Dependency)

#### A. Segmentasi Berdasarkan Volatilitas (NATR)

| Kategori NATR | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `NATR < 0.80%` | 177 | 107 W / 70 L | 60.5% | `+$12.60` | `+$0.07` | 1.26 |
| `NATR >= 0.80%` | 152 | 88 W / 64 L | 57.9% | `+$10.85` | `+$0.07` | 1.20 |

#### B. Segmentasi Berdasarkan Sesi Waktu (UTC)

| Sesi Pasar | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `Asia (00:00 - 08:00 UTC)` | 105 | 55 W / 50 L | 52.4% | `-$18.16` | `-$0.17` | 0.90 |
| `London (08:00 - 14:00 UTC)` | 68 | 42 W / 26 L | 61.8% | `+$14.47` | `+$0.21` | 1.46 |
| `New York (14:00 - 21:00 UTC)` | 147 | 91 W / 56 L | 61.9% | `+$23.10` | `+$0.16` | 1.39 |
| `NY Close / Asia Early (21:00 - 24:00 UTC)` | 9 | 7 W / 2 L | 77.8% | `+$4.05` | `+$0.45` | 2.40 |

#### C. Segmentasi Berdasarkan Arah Posisi (LONG vs SHORT)

| Arah Posisi | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `LONG Positions` | 0 | 0 W / 0 L | 0.0% | `$0.00` | `$0.00` | 0.00 |
| `SHORT Positions` | 329 | 195 W / 134 L | 59.3% | `+$23.45` | `+$0.07` | 1.23 |

### 5. Audit Gatekeeper Sinyal (Top Filter Rejections)

| Alasan Penolakan Sinyal | Jumlah Sinyal Disaring |
| :--- | :--- |
| `SIZING_FAILED` | 3 |
| `LOW_SIGNAL_RVOL` | 62 |
| `EXPLOSIVE_PUMP_FRENZY` | 50 |
| `ENTRY_TOO_FAR_FROM_VWAP` | 50252 |
| `CONDITIONS_NOT_MET` | 40701 |
| `SIDE_DISABLED_SHORT_ONLY` | 362 |
| `NATR_OUT_OF_BOUNDS` | 2317 |
| `SYMBOL_ALREADY_ACTIVE` | 2381 |
| `SYMBOL_IN_COOLDOWN` | 443 |
| `SPREAD_EXCEEDED` | 28 |
| `ENTRY_TOO_FAR_FROM_EMA200` | 11214 |
| `VWAP_SESSION_WARMUP` | 1775 |
| `BTC_COUNTER_MOMENTUM_PUMP` | 23 |

### 6. Evaluasi & Kepatuhan Batas Statistik

> [!IMPORTANT] **KESIMPULAN: EXPECTANCY POSITIF TIPIS (BORDERLINE).**
> Interval 95% menyentuh wilayah negatif. Disarankan menjalankan `backtest ablate` untuk membuang filter yang menambah friction drag.

========================================================================================
