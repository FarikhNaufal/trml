========================================================================================
TRML BACKTEST ENGINE REPORT (PESSIMISTIC 100% BINANCE GROUND TRUTH)
========================================================================================
Waktu Eksekusi   : 2026-11-06T07:25:56Z UTC
Commit Hash      : ee5223d
Rentang Data     : 2026-10-01 s/d 2026-10-31 UTC
Daftar Simbol    : 1000PEPEUSDT, 1000SHIBUSDT, AAOIUSDT, AAPLUSDT, AAVEUSDT, ADAUSDT, AEROUSDT, AGTUSDT, AINUSDT, AIOUSDT, AKEUSDT, ALTUSDT, AMDUSDT, AMZNUSDT, ANTHROPICUSDT, APPUSDT, APRUSDT, APTUSDT, ARBUSDT, ARIAUSDT, ARMUSDT, ASTERUSDT, ATOMUSDT, AVAXUSDT, AVGOUSDT, AXTIUSDT, BABAUSDT, BATUSDT, BCHUSDT, BEAMXUSDT, BEUSDT, BIRBUSDT, BMNRUSDT, BMTUSDT, BNBUSDT, BRUSDT, BTCUSDT, BTWUSDT, BZUSDT, CAPUSDT, CBRSUSDT, CLANKERUSDT, CLUSDT, COHRUSDT, COINUSDT, COMPUSDT, CRCLUSDT, CRVUSDT, CRWVUSDT, CSOPSKHYNIX2LUSDT, CTRUSDT, CXMTUSDT, CYBERUSDT, DASHUSDT, DELLUSDT, DOGEUSDT, DOTUSDT, DRAMUSDT, ELSAUSDT, ENAUSDT, ERAUSDT, ESPORTSUSDT, ETCUSDT, ETHFIUSDT, ETHUSDT, EVAAUSDT, EWYUSDT, FARTCOINUSDT, FETUSDT, FILUSDT, FLUIDUSDT, FOGOUSDT, GOOGLUSDT, GTCUSDT, HBARUSDT, HOODUSDT, HOTUSDT, HUMAUSDT, HYPEUSDT, ICPUSDT, IDOLUSDT, INJUSDT, INTCUSDT, INTWUSDT, IOSTUSDT, IRENUSDT, JCTUSDT, KORUUSDT, KSTRUSDT, LINKUSDT, LITEUSDT, LITUSDT, LSKUSDT, LTCUSDT, LYNUSDT, MAGMAUSDT, METAUSDT, MEWUSDT, MINIMAXUSDT, MRNAUSDT, MRVLUSDT, MSFTUSDT, MSTRUSDT, MUUSDT, MUUUSDT, MVLLUSDT, NATGASUSDT, NBISUSDT, NEARUSDT, NVDAUSDT, ONDOUSDT, ONUSDT, OPUSDT, ORCLUSDT, PARTIUSDT, PAXGUSDT, PENGUUSDT, PHAUSDT, PLTRUSDT, PONSUSDT, PUMPUSDT, QCOMUSDT, QNTUSDT, QQQUSDT, QUSDT, RENDERUSDT, RKLBUSDT, SAMSUNGUSDT, SKDDUSDT, SKHYNIXUSDT, SKHYUSDT, SKUUUSDT, SNDKUSDT, SNXXUSDT, SOLUSDT, SOXLUSDT, SOXSUSDT, SPCXUSDT, SPYUSDT, SQQQUSDT, STRKUSDT, STXUSDT, SUIUSDT, TAOUSDT, TIAUSDT, TQQQUSDT, TRUMPUSDT, TRXUSDT, TSLAUSDT, TSMUSDT, UAIUSDT, UNITREEUSDT, UNIUSDT, USELESSUSDT, UVXYUSDT, VVVUSDT, WDCUSDT, WIFUSDT, WLDUSDT, WLFIUSDT, WUSDT, XAGUSDT, XAUTUSDT, XAUUSDT, XLMUSDT, XMRUSDT, XPLUSDT, XRPUSDT, XTZUSDT, ZECUSDT, ZHIPUUSDT, ZHONGJIUSDT, ZROUSDT
Model Eksekusi   : LIMIT Penetration 1 Tick, Intrabar SL-First, Slippage Active
Struktur Fee     : Maker 2.0 bps, Taker 5.0 bps, Slippage 2.0 bps | Real Binance Funding & Dynamic Spread
Fitur Aktif      : Kline 5m + Exact Net Bar Delta + Real Binance Filters & Funding
----------------------------------------------------------------------------------------

### 1. Ringkasan Kinerja & Metrik Eksekusi

| Metrik Performa | Nilai Hasil Uji | Keterangan Standar |
| :--- | :--- | :--- |
| **Total Trade Selesai** | `137` setup | TP1 + Runner diagregasi 1 setup |
| **Win Rate (All-Inclusive)** | `59.85%` | 82 Win / 55 Loss / 0 BE |
| **Adjusted Win Rate (Pure)** | `59.85%` | Win / (Win + Loss), mengabaikan BE |
| **Payoff Ratio (Avg W/L)** | `0.80` | Rata-rata Win ($1.41) / Loss ($1.75) |
| **Break-Even Win Rate** | `55.40%` | Ambang batas minimum impas |
| **Profit Factor** | `1.34` | Total Profit Kotor / Loss Kotor |
| **Total Net PnL** | `+$19.22 USDT` | Setelah potongan fee & slippage |
| **Net Expectancy per Trade** | `+$0.14 USDT` | Rata-rata keuntungan bersih per trade |
| **95% Bootstrap CI** | `[-$0.14, +$0.43]` | Interval kepercayaan 1.000 iterasi |
| **Max Drawdown (Peak-to-Trough)** | `-9.53%` (-$14.72) | Penurunan modal terdalam |
| **Losing Streak Terpanjang** | `6` trade berturut | Rekor kerugian berurutan |
| **Fee & Friction Drag** | `9.72%` | Total fee ($11.91) vs Profit Kotor |
| **Net Funding Fee** | `-$0.07 USDT` | Total pendanaan Binance dibayar/diterima |

### 2. Distribusi Alasan Keluar (Exit Ladder)

| Alasan Exit | Jumlah Frekuensi | Persentase |
| :--- | :--- | :--- |
| `TAKE_PROFIT` | 1 | 0.5% |
| `PARTIAL_TAKE_PROFIT` | 79 | 36.6% |
| `TRAILING_STOP` | 64 | 29.6% |
| `STOP_LOSS` | 52 | 24.1% |
| `BREAK_EVEN` | 15 | 6.9% |
| `TIME_STOP_MAX_HOLDING` | 5 | 2.3% |

### 3. Log Eksekusi Trade Selesai

| No | Waktu Entry (UTC) | Simbol | Posisi | Entry Price | Exit Price | Alasan Exit | Net PnL | Fee | Funding |
| :- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 2026-10-01 17:55 | `OPUSDT` | SHORT | 0.1288 | 0.1278 | `PARTIAL_TAKE_PROFIT` | `+$0.47` | $0.0470 | $0.0000 |
| 2 | 2026-10-01 17:55 | `OPUSDT` | SHORT | 0.1288 | 0.1271 | `TRAILING_STOP` | `+$1.00` | $0.0572 | $0.0000 |
| 3 | 2026-10-01 17:25 | `AGTUSDT` | SHORT | 0.0255 | 0.0261 | `STOP_LOSS` | `-$1.88` | $0.0574 | $0.0000 |
| 4 | 2026-10-02 04:40 | `BRUSDT` | SHORT | 0.7066 | 0.7147 | `STOP_LOSS` | `-$1.83` | $0.1057 | $0.0000 |
| 5 | 2026-10-02 05:35 | `PHAUSDT` | SHORT | 0.0720 | 0.0714 | `PARTIAL_TAKE_PROFIT` | `+$0.49` | $0.0470 | $0.0000 |
| 6 | 2026-10-02 05:35 | `PHAUSDT` | SHORT | 0.0720 | 0.0714 | `TRAILING_STOP` | `+$0.61` | $0.0574 | $0.0000 |
| 7 | 2026-10-02 14:55 | `ENAUSDT` | SHORT | 0.2447 | 0.2428 | `PARTIAL_TAKE_PROFIT` | `+$0.48` | $0.0468 | $0.0000 |
| 8 | 2026-10-02 14:55 | `ENAUSDT` | SHORT | 0.2447 | 0.2445 | `BREAK_EVEN` | `+$0.01` | $0.0577 | $0.0000 |
| 9 | 2026-10-02 18:50 | `AKEUSDT` | SHORT | 0.0319 | 0.0315 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0279 | $0.0000 |
| 10 | 2026-10-02 18:50 | `AKEUSDT` | SHORT | 0.0319 | 0.0315 | `TRAILING_STOP` | `+$0.65` | $0.0341 | $0.0000 |
| 11 | 2026-10-04 12:55 | `ZROUSDT` | SHORT | 2.0173 | 2.0021 | `PARTIAL_TAKE_PROFIT` | `+$0.46` | $0.0469 | $0.0000 |
| 12 | 2026-10-04 12:55 | `ZROUSDT` | SHORT | 2.0173 | 1.9914 | `TRAILING_STOP` | `+$1.00` | $0.0572 | $0.0000 |
| 13 | 2026-10-04 19:55 | `PUMPUSDT` | SHORT | 0.0064 | 0.0064 | `PARTIAL_TAKE_PROFIT` | `+$0.50` | $0.0470 | $-0.0034 |
| 14 | 2026-10-04 19:55 | `PUMPUSDT` | SHORT | 0.0064 | 0.0064 | `TRAILING_STOP` | `+$0.59` | $0.0574 | $-0.0041 |
| 15 | 2026-10-04 23:20 | `PONSUSDT` | SHORT | 0.3999 | 0.3968 | `PARTIAL_TAKE_PROFIT` | `+$0.49` | $0.0468 | $-0.0174 |
| 16 | 2026-10-04 23:20 | `PONSUSDT` | SHORT | 0.3999 | 0.3970 | `TRAILING_STOP` | `+$0.57` | $0.0576 | $-0.0215 |
| 17 | 2026-10-05 00:50 | `AIOUSDT` | SHORT | 0.0428 | 0.0437 | `STOP_LOSS` | `-$1.88` | $0.0627 | $0.0000 |
| 18 | 2026-10-05 08:35 | `AIOUSDT` | SHORT | 0.0443 | 0.0438 | `PARTIAL_TAKE_PROFIT` | `+$0.58` | $0.0328 | $0.0000 |
| 19 | 2026-10-05 08:35 | `AIOUSDT` | SHORT | 0.0443 | 0.0426 | `TRAILING_STOP` | `+$2.21` | $0.0394 | $0.0000 |
| 20 | 2026-10-05 15:00 | `PUMPUSDT` | SHORT | 0.0064 | 0.0063 | `PARTIAL_TAKE_PROFIT` | `+$0.51` | $0.0470 | $0.0000 |
| 21 | 2026-10-05 15:00 | `PUMPUSDT` | SHORT | 0.0064 | 0.0064 | `BREAK_EVEN` | `+$0.00` | $0.0577 | $0.0000 |
| 22 | 2026-10-06 03:25 | `AINUSDT` | SHORT | 0.0556 | 0.0569 | `STOP_LOSS` | `-$1.88` | $0.0575 | $0.0000 |
| 23 | 2026-10-06 16:35 | `PHAUSDT` | SHORT | 0.0733 | 0.0725 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0392 | $0.0000 |
| 24 | 2026-10-06 16:35 | `PHAUSDT` | SHORT | 0.0733 | 0.0733 | `BREAK_EVEN` | `+$0.00` | $0.0483 | $0.0000 |
| 25 | 2026-10-07 02:25 | `MAGMAUSDT` | SHORT | 0.2150 | 0.2177 | `STOP_LOSS` | `-$1.87` | $0.1004 | $0.0000 |
| 26 | 2026-10-07 02:25 | `CRVUSDT` | SHORT | 0.3536 | 0.3521 | `TIME_STOP_MAX_HOLDING` | `+$0.48` | $0.0922 | $0.0000 |
| 27 | 2026-10-07 02:25 | `STRKUSDT` | SHORT | 0.0508 | 0.0507 | `TIME_STOP_MAX_HOLDING` | `+$0.18` | $0.0943 | $-0.0043 |
| 28 | 2026-10-07 10:30 | `MAGMAUSDT` | SHORT | 0.2161 | 0.2183 | `STOP_LOSS` | `-$1.66` | $0.1058 | $0.0000 |
| 29 | 2026-10-07 14:15 | `PONSUSDT` | SHORT | 0.3984 | 0.3933 | `PARTIAL_TAKE_PROFIT` | `+$0.57` | $0.0329 | $0.0000 |
| 30 | 2026-10-07 14:15 | `PONSUSDT` | SHORT | 0.3984 | 0.3931 | `TRAILING_STOP` | `+$0.74` | $0.0403 | $0.0000 |
| 31 | 2026-10-07 14:30 | `SOXLUSDT` | SHORT | 156.7700 | 155.1700 | `PARTIAL_TAKE_PROFIT` | `+$0.45` | $0.0327 | $0.0000 |
| 32 | 2026-10-07 14:30 | `SOXLUSDT` | SHORT | 156.7700 | 155.1610 | `TRAILING_STOP` | `+$0.75` | $0.0545 | $0.0000 |
| 33 | 2026-10-07 14:05 | `AXTIUSDT` | SHORT | 80.5300 | 82.0564 | `STOP_LOSS` | `-$1.74` | $0.0628 | $0.0000 |
| 34 | 2026-10-07 16:00 | `MVLLUSDT` | SHORT | 40.6900 | 40.1800 | `PARTIAL_TAKE_PROFIT` | `+$0.53` | $0.0311 | $0.0000 |
| 35 | 2026-10-07 16:00 | `MVLLUSDT` | SHORT | 40.6900 | 40.6581 | `BREAK_EVEN` | `+$0.01` | $0.0427 | $0.0000 |
| 36 | 2026-10-07 16:45 | `LSKUSDT` | SHORT | 0.2386 | 0.2365 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0468 | $0.0000 |
| 37 | 2026-10-07 16:45 | `LSKUSDT` | SHORT | 0.2386 | 0.2364 | `TRAILING_STOP` | `+$0.71` | $0.0574 | $0.0000 |
| 38 | 2026-10-07 21:30 | `PUMPUSDT` | SHORT | 0.0063 | 0.0062 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0440 | $0.0000 |
| 39 | 2026-10-07 21:30 | `PUMPUSDT` | SHORT | 0.0063 | 0.0062 | `TRAILING_STOP` | `+$1.11` | $0.0535 | $0.0000 |
| 40 | 2026-10-08 02:45 | `AINUSDT` | SHORT | 0.0415 | 0.0412 | `PARTIAL_TAKE_PROFIT` | `+$0.55` | $0.0454 | $0.0000 |
| 41 | 2026-10-08 02:45 | `AINUSDT` | SHORT | 0.0415 | 0.0412 | `TRAILING_STOP` | `+$0.68` | $0.0554 | $0.0000 |
| 42 | 2026-10-08 14:20 | `FARTCOINUSDT` | SHORT | 0.1590 | 0.1577 | `PARTIAL_TAKE_PROFIT` | `+$0.50` | $0.0470 | $0.0000 |
| 43 | 2026-10-08 14:20 | `FARTCOINUSDT` | SHORT | 0.1590 | 0.1566 | `TRAILING_STOP` | `+$1.17` | $0.0571 | $0.0000 |
| 44 | 2026-10-08 16:10 | `BRUSDT` | SHORT | 0.4946 | 0.5070 | `STOP_LOSS` | `-$1.87` | $0.0518 | $0.0000 |
| 45 | 2026-10-09 02:00 | `ZHONGJIUSDT` | SHORT | 121.2700 | 119.7900 | `PARTIAL_TAKE_PROFIT` | `+$0.56` | $0.0337 | $0.0000 |
| 46 | 2026-10-09 02:00 | `ZHONGJIUSDT` | SHORT | 121.2700 | 119.1538 | `TRAILING_STOP` | `+$1.02` | $0.0419 | $0.0000 |
| 47 | 2026-10-09 15:25 | `LITUSDT` | SHORT | 3.6135 | 3.6561 | `STOP_LOSS` | `-$1.88` | $0.1059 | $0.0000 |
| 48 | 2026-10-09 16:40 | `NEARUSDT` | SHORT | 4.7170 | 4.7670 | `STOP_LOSS` | `-$1.65` | $0.1031 | $0.0000 |
| 49 | 2026-10-10 05:45 | `GTCUSDT` | SHORT | 0.1845 | 0.1871 | `STOP_LOSS` | `-$1.88` | $0.0878 | $0.0000 |
| 50 | 2026-10-11 14:30 | `APRUSDT` | SHORT | 0.0733 | 0.0725 | `PARTIAL_TAKE_PROFIT` | `+$0.57` | $0.0375 | $0.0000 |
| ... | *Menampilkan 50 dari 216 trade total* | | | | | | | | |

### 4. Analisis Segmentasi Trade (Bebas Path-Dependency)

#### A. Segmentasi Berdasarkan Volatilitas (NATR)

| Kategori NATR | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `NATR < 0.80%` | 68 | 43 W / 25 L | 63.2% | `+$13.56` | `+$0.20` | 1.50 |
| `NATR >= 0.80%` | 69 | 39 W / 30 L | 56.5% | `+$5.66` | `+$0.08` | 1.21 |

#### B. Segmentasi Berdasarkan Sesi Waktu (UTC)

| Sesi Pasar | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `Asia (00:00 - 08:00 UTC)` | 52 | 32 W / 20 L | 61.5% | `+$4.87` | `+$0.09` | 1.27 |
| `London (08:00 - 14:00 UTC)` | 25 | 15 W / 10 L | 60.0% | `+$9.15` | `+$0.37` | 1.65 |
| `New York (14:00 - 21:00 UTC)` | 56 | 32 W / 24 L | 57.1% | `+$3.79` | `+$0.07` | 1.23 |
| `NY Close / Asia Early (21:00 - 24:00 UTC)` | 4 | 3 W / 1 L | 75.0% | `+$1.41` | `+$0.35` | 1.95 |

#### C. Segmentasi Berdasarkan Arah Posisi (LONG vs SHORT)

| Arah Posisi | Total Trade | Win / Loss | Win Rate | Net PnL (USDT) | Avg PnL | Profit Factor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `LONG Positions` | 0 | 0 W / 0 L | 0.0% | `$0.00` | `$0.00` | 0.00 |
| `SHORT Positions` | 137 | 82 W / 55 L | 59.9% | `+$19.22` | `+$0.14` | 1.34 |

### 5. Audit Gatekeeper Sinyal (Top Filter Rejections)

| Alasan Penolakan Sinyal | Jumlah Sinyal Disaring |
| :--- | :--- |
| `BTC_COUNTER_MOMENTUM_PUMP` | 14 |
| `SPREAD_EXCEEDED` | 15 |
| `CONDITIONS_NOT_MET` | 18540 |
| `ENTRY_TOO_FAR_FROM_EMA200` | 6492 |
| `SYMBOL_IN_COOLDOWN` | 171 |
| `LOW_SIGNAL_RVOL` | 30 |
| `EXPLOSIVE_PUMP_FRENZY` | 26 |
| `ENTRY_TOO_FAR_FROM_VWAP` | 23855 |
| `SIDE_DISABLED_SHORT_ONLY` | 166 |
| `SYMBOL_ALREADY_ACTIVE` | 1098 |
| `NATR_OUT_OF_BOUNDS` | 1098 |
| `VWAP_SESSION_WARMUP` | 990 |

### 6. Evaluasi & Kepatuhan Batas Statistik

> [!WARNING] **Sample Size Alert:** Total trade (< 200 setup) belum memenuhi syarat signifikansi statistik penuh. Perluas rentang tanggal pengujian.

> [!IMPORTANT] **KESIMPULAN: EXPECTANCY POSITIF TIPIS (BORDERLINE).**
> Interval 95% menyentuh wilayah negatif. Disarankan menjalankan `backtest ablate` untuk membuang filter yang menambah friction drag.

========================================================================================
