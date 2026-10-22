# AUDIT STRATEGI & TEKNIS — BINANCE USDⓈ-M FUTURES BOT

**Tanggal review:** 16 Oktober 2026  
**Berkas yang diaudit:** `pseudo.go` dan `scaffold.md` (versi spesifikasi v2.0.0)  
**Tujuan:** menilai apakah logika strategi masuk akal, apakah kode semu konsisten dengan spesifikasi, dan apa yang harus diperbaiki sebelum dry-run atau live trading.

## 1. Kesimpulan eksekutif

**Status: BELUM SIAP untuk live trading.** Arsitektur event-driven dan pemisahan indikator, sizing, dry-run, dan live broker adalah fondasi yang wajar. Versi v2 juga telah memperbaiki beberapa masalah versi sebelumnya: VWAP menggunakan base volume, BarDelta memakai field kline yang eksplisit, candle dideduplikasi, snapshot dibuat di bawah lock, dan sizing menggunakan `decimal.Decimal`.

Namun, ada dua kelompok masalah yang berbeda:

1. **Strategi belum tervalidasi secara statistik.** Tidak ada backtest, walk-forward, out-of-sample, data trade, atau hasil biaya aktual yang membuktikan bahwa kombinasi EMA200 + VWAP/ATR envelope + BarDelta Z-score + OI delta + funding mempunyai expectancy positif.
2. **Implementasi belum aman sebagai bot produksi.** Jalur live order belum lengkap di `pseudo.go`; jalur proteksi dapat menandai posisi “terproteksi” meskipun pemasangan SL/TP gagal; simulasi fill terlalu heuristik; data stale/gap belum ditangani sepenuhnya; dan state circuit breaker dapat kehilangan baseline saat restart.

Jadi, **jangan aktifkan live trading berdasarkan scaffold ini saja**. Perbaiki blocker P0, buat aplikasi executable dengan tes nyata, lalu validasi edge strategi terpisah dari tes operasional.

## 2. Ringkasan prioritas

| Prioritas | Temuan | Dampak |
|---|---|---|
| **P0 — blocker live** | Implementasi `BinanceLiveBroker` tidak lengkap dan `main()` hanya mencetak pesan | Kode ini belum menjadi bot yang benar-benar dapat menjalankan strategi dan order live |
| **P0 — blocker live** | SL/TP bisa gagal tetapi dicatat seolah sudah terpasang | Posisi live berpotensi tidak memiliki proteksi server-side |
| **P0 — blocker validasi** | Dry-run mengasumsikan fill berdasarkan trade-through tanpa antrean orderbook yang memadai | Profit factor, fill rate, dan expectancy dry-run berpotensi bias |
| **P0/P1** | Bootstrap dapat memasukkan candle yang belum tutup saat hanya menerima 250 candle; tidak ada validasi close time atau gap | EMA/ATR/VWAP/sinyal dapat salah dan candle final dapat terlewat karena deduplikasi |
| **P1** | EMA200 dihitung ulang dari buffer 250 candle dengan seed SMA 200 dan hanya 50 update sesudahnya | EMA sensitif terhadap seed dan belum cukup warm-up untuk dipercaya tanpa pengujian |
| **P1** | Session VWAP bootstrap hanya menghitung candle yang tersedia | Pada jam UTC setelah 20:50, jendela 250 candle tidak mencakup seluruh sesi sejak 00:00 UTC |
| **P1** | `BookUpdatedAt` tidak dibawa ke snapshot/tidak divalidasi | Sinyal dapat memakai best bid/ask yang stale walaupun spesifikasi menjanjikan fail-closed |
| **P1** | OI delta tidak memiliki timestamp terpisah untuk sampel sebelumnya | Nilainya tidak dijamin benar-benar perubahan OI tepat selama 5 menit |
| **P1** | Circuit breaker daily baseline tidak aman saat restart dan pending entry tidak dibatalkan setelah breaker aktif | Batas rugi harian dapat salah hitung atau posisi baru masih bisa terbuka dari order pending |
| **P1** | Broker check (active symbol/slot/margin) dan submit order tidak atomik; dry-run submit bisa menimpa order simbol yang sama | Race condition dan perubahan state yang tidak konsisten |
| **P1** | Callback API `PlaceTriggerFn`/`CancelOrderFn` dipanggil saat mutex live broker masih terkunci; hasil/error tidak ditangani | Deadlock, lock panjang, atau kegagalan proteksi tidak terdeteksi |
| **P1** | Funding, adverse execution TP, dan sejumlah kondisi margin tidak dimodelkan di dry-run | Hasil simulasi bukan representasi PnL live yang cukup andal |
| **P2 — desain strategi** | Passive LIMIT di BestBid/BestAsk setelah candle breakout + timeout 45 detik | Bisa mengurangi fee, tetapi rawan tidak terisi atau hanya terisi saat momentum berbalik; harus diuji sebagai bagian dari strategi |
| **P2 — desain strategi** | BarDelta Z-score menghitung mean/std menggunakan candle terkini itu sendiri | Nilai Z-score current-bar terpengaruh oleh observasi yang sedang diuji; ambang 1.5 perlu dikalibrasi ulang |
| **P2 — desain strategi** | Tidak ada cooldown/re-arm atau time exit yang eksplisit | Sinyal breakout berulang dapat menimbulkan re-entry berkali-kali setelah posisi selesai |
| **P2 — portofolio** | Maksimal 3 slot bukan pengendali korelasi pasar | Tiga altcoin dapat memiliki eksposur yang sangat berkorelasi dan rugi bersamaan |

## 3. Audit strategi trading: apakah aturan entry masuk akal?

### 3.1 Bentuk strategi

Spesifikasi menggunakan candle 5 menit dan menggabungkan:

- **Trend:** Close di atas/bawah EMA200.
- **Breakout:** Close melewati Session VWAP ± 1,25 × ATR14.
- **Flow:** BarDelta Z-score ≥ +1,5 untuk long atau ≤ −1,5 untuk short.
- **Derivatif:** OI delta positif minimal 0,20% dan funding tidak terlalu merugikan sisi yang dipilih.
- **Exit:** SL 1,5 × ATR, TP 3 × ATR, lalu geser SL ke break-even-plus ketika profit mencapai 1R.

Ini **koheren sebagai hipotesis strategi momentum/breakout**: tren, ekspansi harga, agresi taker, dan pertumbuhan open interest memberi beberapa jenis konfirmasi. Namun, menyebut konfirmasi ini “orthogonal” belum terbukti. Semua fitur bisa terdorong oleh kejadian pasar yang sama dan karenanya sangat berkorelasi. Empat filter tidak otomatis berarti empat sumber informasi independen.

### 3.2 Tidak ada bukti edge pada berkas

Tidak tersedia data trade, backtest, periode uji, perbandingan baseline, distribusi MAE/MFE, fill rate, atau hasil net setelah fee/funding/slippage. Karena itu, review ini tidak dapat menyatakan strategi menguntungkan atau tidak menguntungkan. Angka `1.25 ATR`, `Z = 1.5`, `OI +0.20%`, `SL = 1.5 ATR`, dan `TP = 3 ATR` saat ini adalah **parameter hipotesis**, bukan parameter yang telah dibuktikan optimal.

Rasio reward:risk teoritis 2:1 tidak cukup untuk menyimpulkan profitabilitas. Breakeven trigger, missed fills, partial fills, komisi, funding, gap, dan slippage mengubah distribusi hasil nyata.

### 3.3 Entry LIMIT post-only bisa tidak cocok dengan sinyal breakout

Sinyal dibuat setelah candle close menembus envelope, tetapi entry long dipasang di BestBid dan short di BestAsk, dengan timeout 45 detik. Pendekatan ini menghemat taker fee bila berhasil menjadi maker, namun membawa trade-off penting:

- Harga dapat terus bergerak menjauhi limit, sehingga order tidak terisi dan momentum yang dideteksi terlewat.
- Long di BestBid cenderung terisi ketika ada tekanan jual yang menyentuh/menembus harga bid. Untuk strategi breakout, fill tersebut dapat terjadi justru saat momentum melemah.
- Book dapat berubah antara snapshot sinyal dan pengiriman order, sehingga order GTX berpotensi ditolak.

Ini bukan otomatis salah, tetapi **jenis entry merupakan bagian inti dari strategi** dan harus ikut diuji. Backtest memakai harga candle close atau menganggap semua limit terisi akan memberi hasil yang tidak setara dengan kebijakan ini. Uji terpisah: post-only limit, market/taker, dan kebijakan limit dengan aturan repricing/timeout yang eksplisit.

### 3.4 BarDelta Z-score: sebaiknya gunakan baseline candle sebelumnya

Kode mengambil 50 nilai `ClosedBarDeltasUSDT`, menghitung mean/std dari seluruh 50 nilai, lalu menstandarkan nilai terakhir dari kumpulan yang sama. Ini tidak menggunakan data masa depan, tetapi observasi yang sedang diuji ikut menggeser mean dan standar deviasinya sendiri. Akibatnya, Z-score current-bar tidak ekuivalen dengan “seberapa ekstrem candle ini dibanding 50 candle sebelumnya”.

Rekomendasi: simpan setidaknya 51 delta; hitung mean/std dari 50 bar sebelumnya, lalu hitung Z-score delta terbaru terhadap baseline itu. Setelah perubahan, **kalibrasi ulang ambang Z = 1,5** menggunakan out-of-sample; jangan mengasumsikan hasilnya tetap sama.

Selain itu, istilah `BarDelta Z-score` lebih tepat daripada `CVD`: rumus yang dipakai mengukur delta taker-buy minus taker-sell pada satu candle, bukan cumulative volume delta yang diakumulasikan lintas candle.

### 3.5 OI delta belum tentu benar-benar delta 5 menit

`HandleOpenInterestSnapshot` hanya menggeser nilai `PrevOpenInterest` dan `CurrOpenInterest`. Timestamp sampel sebelumnya tidak disimpan, sedangkan evaluator membandingkan dua nilai terbaru pada saat candle ditutup. Tanpa scheduler yang ketat dan sinkron dengan candle, selisih itu belum tentu interval tepat 5 menit.

Rekomendasi: simpan timestamp bagi setiap sampel OI; hitung perubahan dari sampel yang memang berjarak sesuai interval yang ditentukan; tolak sinyal jika interval tidak sesuai toleransi. Jangan mengartikan data REST terbaru sebagai nilai “OI candle sebelumnya” tanpa menyimpan dan memverifikasi waktu datanya.

### 3.6 EMA200: bootstrap 250 bar terlalu pendek untuk menghitungnya secara stabil dengan implementasi ini

`CalculateEMA` membuat seed berupa SMA 200 candle pertama, lalu melakukan EMA hanya atas candle yang tersisa. Dengan 250 candle, hanya ada 50 langkah EMA setelah seed. Untuk periode 200, faktor peluruhan pengaruh seed per update adalah `1 − 2/201`; setelah 50 update, faktor tersebut sekitar **0,606**. Ini adalah faktor sisa pengaruh kondisi awal, bukan berarti nilai EMA salah sebesar 60,6%, tetapi menunjukkan pengaruh seed masih dapat berarti.

Karena fungsi juga menghitung ulang EMA dari ring buffer 250 bar setiap sinyal, nilai tidak dipelihara sebagai EMA rekursif berkelanjutan. Rekomendasi: bootstrap dengan riwayat lebih panjang dan uji konvergensi EMA, atau simpan state EMA yang diperbarui setiap candle tertutup. Pastikan hasil cocok terhadap benchmark indikator yang dipilih.

### 3.7 Session VWAP tidak selalu mencakup seluruh sesi harian saat bootstrap

Fungsi VWAP sendiri memakai typical price berbobot base volume, dan reset Epoch Day UTC mengatasi masalah pergantian tanggal. Namun, bootstrap hanya membawa 250 candle 5m (sekitar 20 jam 50 menit). Jika bot mulai setelah 20:50 UTC, riwayat itu tidak memuat candle dari awal hari 00:00 UTC. VWAP yang dihasilkan hanya menghitung bagian akhir sesi, bukan VWAP harian sesuai definisi dokumen.

Rekomendasi: saat bootstrap, ambil semua candle tertutup dari 00:00 UTC hari tersebut hingga waktu kini, ditambah riwayat lebih lama untuk EMA/ATR; atau ambil data sesi terpisah dan rekonstruksi VWAP. Jangan menggunakan satu jumlah candle tetap untuk dua kebutuhan riwayat yang berbeda.

### 3.8 RVOL hanya mendeteksi candle terakhir yang tinggi, bukan volume yang sudah dinormalisasi waktu

`CalculateRVOL` benar-benar membandingkan volume candle tertutup terakhir dengan rata-rata 20 candle tertutup sebelumnya. Artinya ini adalah relative volume rolling pada bar terakhir, bukan baseline time-of-day yang menyesuaikan pola volume intraday. Ini boleh digunakan, tetapi akibatnya scanner bisa memilih koin karena satu spike yang baru saja selesai; saat scanner berjalan tiap 15 menit, ranking RVOL dapat cepat usang.

Rekomendasi: uji apakah RVOL seperti ini benar-benar meningkatkan expectancy, dan bandingkan dengan normalisasi pada slot waktu yang sama serta persistensi volume selama beberapa candle. Jangan mengklaim threshold 1,8x efektif sebelum diuji.

### 3.9 OI naik tidak mengungkap sisi siapa yang membuka posisi

Open interest meningkat berarti jumlah kontrak terbuka meningkat, tetapi sendiri tidak memberi tahu apakah pembeli atau penjual yang “lebih dominan”—setiap kontrak terbuka mempunyai kedua sisi. Menggabungkannya dengan BarDelta dan breakout masuk akal sebagai konfirmasi partisipasi, tetapi OI delta bukan bukti mandiri bahwa “long baru” atau “short baru” yang mendorong harga.

## 4. Audit eksekusi dan proteksi — blocker utama

### P0.1 — `pseudo.go` bukan bot produksi yang dapat dijalankan

Pada berkas yang disertakan, `main()` hanya mencetak bahwa scaffold dimuat. Tidak ada koneksi REST/WebSocket, scheduler scanner, inisialisasi akun, pemulihan state SQLite, atau wiring stream ke broker. `BinanceLiveBroker` juga tidak mengimplementasikan method lengkap yang dibutuhkan `ExecutionBroker` seperti `SubmitLimitEntry`, `OnRealtimeTick`, pembacaan equity/available balance, dan penghitung slot. Variabel `IsLeverageIsolatedVerified` dimulai `false`, sementara tidak ada kode startup di berkas ini yang mengubahnya menjadi `true`; akibatnya event candle akan dilewati jika scaffold dipakai sendirian.

**Tindakan:** sebut artefak ini sebagai blueprint/pseudocode, bukan “complete production bot”. Implementasikan wiring runtime, inisialisasi dan restore state, adapter live yang lengkap, dan integration test sebelum uji strategi.

### P0.2 — posisi dapat dianggap terproteksi meski SL/TP gagal dipasang

`PlaceTriggerFn` hanya mengembalikan `int64`, tanpa error/ack yang terdefinisi. `syncReduceOnlyProtectionLocked` tetap menyimpan `ProtectedQty` meskipun `slOrderID` atau `tpOrderID` nol. Pada event berikutnya, pemeriksaan `ProtectedQty.Equal(...)` akan return lebih awal, meskipun proteksi sebenarnya gagal.

**Tindakan wajib:** representasikan SL dan TP sebagai state terpisah (`PENDING`, `ACKED`, `FAILED`, `CANCELED`, `TRIGGERED`). Jangan tandai quantity terproteksi penuh sebelum kedua order dikonfirmasi dari bursa. Jika pemasangan SL gagal, lakukan retry terkontrol dan aktifkan jalur emergency (misalnya close/reduce-only yang telah diuji) sambil menghentikan entry baru pada simbol tersebut. Error, response ID, dan client ID harus disimpan.

### P0.3 — callback jaringan dipanggil saat mutex broker masih dipegang

`HandleUserDataOrderUpdate` memanggil `syncReduceOnlyProtectionLocked` di bawah `b.Mu`; fungsi itu memanggil `CancelOrderFn` dan `PlaceTriggerFn` sebelum lock dilepas. Bila callback melakukan I/O lambat atau menunggu event yang membutuhkan mutex yang sama, event loop dapat macet atau deadlock. Ini bertentangan dengan prinsip snapshot/lock singkat pada spesifikasi.

**Tindakan:** di dalam lock hanya ubah state dan buat command intent; lepas lock; jalankan I/O di worker; proses hasilnya melalui event yang idempoten. Jangan memanggil jaringan di dalam critical section.

### P0.4 — bootstrap belum memastikan candle terakhir benar-benar tertutup

`Candle` tidak membawa `CloseTime`/status closed. Scanner meminta 251 candle dan memotong ke 250 jika hasil lebih panjang, tetapi jika hasil tepat 250, ia menerima semuanya. Tidak ada validasi bahwa candle terakhir sudah ditutup. Bila candle aktif ikut masuk, `LastProcessedCandleOpenTime` maju ke candle tersebut dan event `kline.x == true` untuk waktu buka yang sama akan ditolak oleh deduplikasi.

**Tindakan:** minta setidaknya jumlah candle yang diperlukan lalu validasi waktu penutupan terhadap server time; buang setiap candle yang belum tertutup. Tambahkan timestamp close dan tes kasus response hanya 250 bar, koneksi putus, candle duplikat, serta candle terlambat.

### P0.5 — simulasi “volume-through-limit” tidak mengestimasi queue fill yang memadai

Di `VirtualPendingOrder` ada `CumulativeVolumeBeyondLimit`, tetapi variabel itu tidak digunakan. Setiap `aggTrade` yang harganya melewati limit dapat menambah fill sebesar `min(tradeQty, remainingQty)`. Simulasi tidak memakai `isBuyerMaker`, posisi antrean sebelum order, ukuran antrean pada level harga, atau trade IDs untuk menghindari pemrosesan ganda. Jadi “trade menembus limit” bukan bukti bahwa seluruh volume trade tersedia untuk order virtual ini.

**Dampak:** dry-run dapat memberi fill yang terlalu optimistis dan menghitung expectancy/PF yang tidak realistis. Ini membuat dry-run tidak cukup sebagai alat validasi edge.

**Tindakan:** gunakan order-book replay dengan bid/ask depth dan asumsi queue yang eksplisit, atau gunakan hasil dry-run hanya untuk smoke/operational test, bukan bukti profitabilitas. Bandingkan fill simulator dengan histori fill nyata bila tersedia.

### P1.1 — BookTicker stale tidak memblokir sinyal

`SymbolState` menyimpan `BookUpdatedAt`, tetapi `MarketSnapshot` tidak membawa timestamp tersebut dan `EvaluateSignalFromSnapshot` tidak melakukan age check pada best bid/ask. Maka bagian fail-closed yang diklaim dalam `scaffold.md` belum dilaksanakan untuk top-of-book.

**Tindakan:** sertakan `BookUpdatedAt`, tolak entry jika timestamp terlalu lama, validasi monotonic/clock skew, dan jangan membiarkan cached quote disamarkan sebagai quote baru.

### P1.2 — tidak ada deteksi gap candle/backfill saat WebSocket putus

Deduplikasi menolak candle yang sama atau lebih lama, tetapi candle baru yang melompat lebih dari satu interval akan diterima tanpa mendeteksi data 5m yang hilang. VWAP, BarDelta, ATR, dan EMA akan melewatkan bar yang hilang. Setelah reconnect, backfill REST harus mengisi semua candle tertutup yang hilang sebelum sinyal dilanjutkan.

### P1.3 — circuit breaker daily dapat di-reset oleh restart

`DryRunBroker.SessionDayEpochUTC` dan `SessionStartEquity` adalah field in-memory. Pada tick pertama setelah objek baru dibuat, epoch default berbeda dari hari berjalan, sehingga kode menginisialisasi baseline dari equity saat itu. Tidak ada kode pemulihan `account_daily_state` pada `pseudo.go`, walaupun `scaffold.md` mensyaratkan penyimpanan SQLite. Restart sesudah rugi bisa membuat daily drawdown seolah kembali ke 0%.

Selain itu, circuit breaker memblokir sinyal baru, tetapi tidak terlihat membatalkan pending entry yang masih bisa terisi setelah ambang tercapai. Jika definisi circuit breaker dimaksudkan melindungi batas kerugian harian secara kuat, perlu ada tindakan pada order pending dan aturan eksplisit untuk posisi terbuka.

**Tindakan:** persist dan restore baseline, flag circuit-breaker, trade/order state; baseline tidak boleh dibuat ulang jika tanggal sesi yang sama telah tersimpan. Ketika breaker aktif, blokir entry, batalkan pending entry, dan terapkan kebijakan posisi terbuka yang eksplisit.

### P1.4 — race pemesanan dan duplicate order belum ditutup

Evaluator memeriksa `GetActiveSlotCount()` dan `HasActiveOrderOrPosition()` dalam pemanggilan terpisah, lalu memanggil `SubmitLimitEntry`. `DryRunBroker.SubmitLimitEntry` mengunci state untuk batas slot, tetapi tidak menolak simbol yang sudah memiliki pending/open position; penugasan map `PendingOrders[symbol] = ...` dapat menimpa order pending yang sama. Event evaluasi untuk snapshot berbeda dapat selesai dalam urutan yang berbeda.

**Tindakan:** pindahkan validasi duplicate symbol, slot, daily breaker, equity/margin, dan reservasi order ke satu operasi atomik di broker. Gunakan status `RESERVED -> SUBMITTING -> ACKED/...` serta unique client order ID/idempotency. Jangan hanya mengandalkan pre-check di evaluator.

### P1.5 — state proteksi partial fill belum aman terhadap kegagalan, urutan event, dan ID

- Kuantitas kumulatif pada record selalu ditimpa oleh event terbaru; tidak ada guard agar nilai tidak mundur jika event terlambat/replayed.
- Order SL/TP lama dibatalkan sebelum order pengganti berhasil dikonfirmasi, sehingga ada jendela waktu tanpa perlindungan.
- ID dari order kondisional/algo dan ID order yang muncul setelah trigger tidak dibedakan secara eksplisit. Perbandingan ID untuk cancel-sibling harus diverifikasi terhadap jenis ID yang benar-benar dikirim dalam event Binance.
- Callback placement/cancel tidak mengembalikan error dan state machine tidak menangani timeout/unknown outcome.

**Tindakan:** gunakan event version/update time, monotonic cumulative fill, clientAlgoId/algoId/orderId yang terpisah bila applicable, query rekonsiliasi setelah reconnect, dan proteksi yang tidak pernah dinyatakan sukses tanpa konfirmasi.

### P1.6 — simulasi PnL tidak memasukkan semua friksi futures

Dry-run membebankan fee maker saat entry dan fee taker saat exit, namun tidak memasukkan funding payment selama posisi terbuka. TP market diperlakukan seolah terisi tepat pada `TPPrice` tanpa penalti slippage, sementara SL diberi slippage tetap. Mark price yang cache-nya stale juga dapat terus diteruskan dari handler book/trade ke broker tanpa timestamp sumber; tidak ada batas umur mark price di broker. Ini cenderung membuat PnL simulasi berbeda dari real execution.

**Tindakan:** catat funding berdasarkan waktu/rate/arah/nominal; modelkan spread dan market slippage pada semua market exit; beri timestamp pada MarkPrice; jangan terus menggunakan nilai mark yang stale; dan cocokkan biaya/filled price dengan account trade history.

### P1.7 — quantization fail-open jika filter tidak valid

`QuantizePrice` dan `QuantizeQtyFloor` mengembalikan nilai input mentah saat tick/step size `<= 0`. Dalam sistem live, filter yang kosong/tidak termuat seharusnya menyebabkan order ditolak, bukan melewati pembulatan.

**Tindakan:** fail-closed jika tick/step/minimum filter tidak valid; verifikasi seluruh filter relevan untuk symbol dan jangan mengirim order sampai ExchangeInfo termuat serta lolos validasi.

## 5. Risk management & portofolio

### 5.1 Batas rugi per trade adalah target, bukan jaminan

Sizing memakai stop distance aktual setelah kuantisasi dan menambahkan perkiraan komisi/slippage pada pembagi risiko; ini lebih baik daripada sekadar membagi equity dengan jarak SL. Namun, risiko real tetap dapat melampaui 1% akibat gap, spread melebar, slippage tak terduga, perubahan fill, funding, dan kegagalan stop. Karena itu, nama yang akurat adalah “risiko estimasi pada kondisi model”, bukan hard guarantee.

### 5.2 Tiga slot tidak membatasi korelasi

Maksimum tiga posisi membatasi jumlah posisi, bukan korelasi. Tiga koin altcoin dapat bergerak sangat searah saat BTC bergerak tajam. Tambahkan pengukuran total risk-to-stop, eksposur long/short, korelasi/cluster, dan batas portofolio. Semua ukuran harus diuji—tidak tepat menetapkan angka baru tanpa data.

### 5.3 Breaker -3% belum merupakan hard loss cap

Jika tiga slot berisi posisi dengan risiko sekitar 1% masing-masing, ketiganya dapat mencapai stop sebelum atau saat circuit breaker dievaluasi. Kerugian setelah slippage/fee dapat melewati −3%. Karena implementasi hanya gate entry baru, breaker bukan mekanisme yang menjamin equity tidak turun melewati angka itu. Definisikan apakah breaker sekadar menghentikan entry, membatalkan pending, atau juga memicu exit/hedging; uji kebijakan yang dipilih.

### 5.4 Kebutuhan margin masih berupa estimasi

Clamp berdasarkan `equity × 30%` dan `available balance × 90%`, lalu mengalikan leverage 5x, merupakan batas konservatif awal tetapi bukan replika penuh perhitungan margin bursa. `GetAvailableBalanceUSDT` dry-run hanya menghitung equity efektif dikurangi estimasi initial margin; ia tidak memuat seluruh maintenance-margin bracket, fee reserve, atau account-specific state. Live sizing harus memeriksa account balance/available balance dan margin rules aktual, bukan hanya model lokal.

## 6. Endpoint dan operasional Binance

Spesifikasi v2 sudah menyebut pemisahan WebSocket `public`, `market`, dan `private`. Binance mengumumkan migrasi endpoint USDⓈ-M pada 6 Maret 2026 dan retirement URL WebSocket lama pada **23 April 2026**. Pada tanggal audit ini, endpoint stream lama tidak boleh diasumsikan masih berjalan. Lihat [pengumuman resmi Binance](https://www.binance.com/en/support/announcement/detail/ebf9b0aa9eca4ff3804eef6fb09ba32a).

Tetap lakukan verifikasi endpoint/method berdasarkan dokumentasi USDⓈ-M terbaru tepat sebelum implementasi. Berkas pseudocode tidak menunjukkan konektor API lengkap, skema signing Ed25519, request/response contract, handling unknown request outcome, reconnect, atau recovery dari state bursa. Adapter live juga perlu memastikan apakah order SL/TP yang digunakan termasuk conditional/algo order pada versi API saat implementasi, dan memetakan client ID/order ID dengan benar. Jangan memakai kontrak endpoint dari produk Binance lain (misalnya Portfolio Margin) sebagai pengganti dokumentasi USDⓈ-M.

## 7. Urutan perbaikan yang direkomendasikan

### Tahap A — sebelum strategi dinilai

1. **Bekukan klaim produksi.** Tandai berkas sebagai scaffold sampai `main` dan semua adapter benar-benar berfungsi.
2. **Perbaiki bootstrap data:** validasi candle close time, ambil history EMA lebih panjang, bangun VWAP dari 00:00 UTC, deduplikasi dan backfill gap setelah reconnect.
3. **Perbaiki stream freshness:** sertakan timestamp bid/ask dan mark price; stale di semua input yang memengaruhi entry/exit harus fail-closed.
4. **Perbaiki live protection:** hasil order placement harus punya error/ack; SL wajib terkonfirmasi; handle partial fills/cancel races; jangan melakukan I/O di bawah mutex.
5. **Perbaiki idempotency/order reservation:** atomic reserve slot, duplicate symbol guard, durable state, restore/reconcile pada startup/reconnect.
6. **Perbaiki dry-run fills dan PnL:** jangan klaim queue realism sebelum queue, fill, funding, fees, slippage, partial fill, cancel latency dan market gaps dimodelkan.

### Tahap B — validasi edge strategi

1. Tulis definisi strategi final tanpa ambigu: candle yang menjadi baseline Z-score, interval OI, cara funding dibaca, jenis entry order, timeout/repricing, dan kapan sinyal boleh re-arm.
2. Backtest multi-simbol menggunakan data yang sama dengan aturan live; masukkan maker/taker fees sesuai akun, spread, latency, funding, partial/no-fill, slippage dan delisting/contract status.
3. Pisahkan periode in-sample, validation, dan out-of-sample; jalankan walk-forward tanpa memilih parameter berdasarkan periode test.
4. Bandingkan ablation: baseline EMA+VWAP envelope, lalu tambah BarDelta, lalu OI/funding. Jika fitur tambahan tidak memperbaiki net expectancy atau drawdown out-of-sample, pertimbangkan menghapusnya.
5. Laporkan metrik minimal: jumlah signal dan filled trades; fill rate; expectancy/trade setelah semua friksi; profit factor; max drawdown; average win/loss; exposure time; hasil per simbol/regime; MAE/MFE; distribusi run-up dan gap stop.
6. Jalankan dry-run live-stream untuk menguji sistem, tetapi **jangan menggunakan 50 transaksi sebagai bukti statistik bahwa strategi profitabel**. Dokumen sendiri sudah tepat membedakan operational soak test dari statistical validation.
7. Baru setelah unit test, integration test, reconnect/recovery test, `go test -race`, dan evaluasi out-of-sample lolos, mulai dengan ukuran live terkecil dan API key tanpa withdrawal permission.

## 8. Acceptance tests tambahan sebelum live

1. Bootstrap menerima 250 candle saja dan latest candle masih aktif → candle tersebut tidak boleh masuk buffer tertutup.
2. Bootstrap pada 22:30 UTC → Session VWAP sama dengan rekonstruksi dari seluruh candle tertutup sejak 00:00 UTC.
3. Bandingkan EMA200 scaffold dengan implementasi referensi dari long history; ukur error pada berbagai regime.
4. Satu bar Delta ekstrem diuji terhadap Z-score baseline yang tidak memasukkan bar itu sendiri.
5. OI snapshot berjarak 1 menit, 5 menit, dan 8 menit → hanya interval valid yang boleh memberi sinyal “OI delta 5m”.
6. BookTicker berhenti, tetapi kline terus datang → entry wajib ditolak karena quote stale.
7. Candle timestamp lompat 5m → backfill terjadi sebelum sinyal berikutnya.
8. Dua evaluator simbol berjalan serentak ketika hanya tersisa satu slot → tidak mungkin lebih dari tiga slot atau margin yang ditentukan terpakai.
9. Submit order pada simbol yang sudah memiliki pending order → order lama tidak boleh tertimpa diam-diam.
10. SL placement berhasil, TP gagal → state tidak boleh melaporkan proteksi lengkap; harus retry/eskalasi.
11. Cancel SL/TP berjalan lambat → handler event tetap responsif dan tidak deadlock.
12. Cumulative fill event datang dengan nilai turun/stale → filled quantity tidak boleh menurun.
13. Restart di tengah hari setelah daily loss → daily baseline dan breaker dipulihkan, bukan diset ulang menjadi 0% drawdown.
14. Daily breaker aktif saat ada pending entry → pending entry dibatalkan dan status cancellation direkonsiliasi.
15. Stream mark price stale sementara book/trade stream hidup → nilai cached tidak boleh diperlakukan sebagai mark price baru.
16. Funding timestamp, fee tier, tick size/step size, min notional atau leverage configuration tidak tersedia → no new orders.
17. Exit order terpicu → sibling benar-benar dibatalkan dan rekonsiliasi memastikan posisi bursa sudah flat atau kuantitas residual ditangani.
18. Simulasi TP/SL meliputi gap melewati trigger, stop slippage, TP market slippage, dan fee aktual.

## 9. Pertanyaan yang harus dijawab AI reviewer berikutnya

Minta agent menjawab dengan referensi file/line dan test yang dapat direproduksi, bukan opini:

1. Apakah ia bisa menunjuk bukti out-of-sample bahwa setiap filter menambah net expectancy?
2. Bagaimana tepatnya sistem memastikan hanya candle tertutup dan tidak pernah melewatkan candle saat reconnect?
3. Berapa banyak history yang dipakai untuk EMA200 dan bagaimana error terhadap benchmark diukur?
4. Dari timestamp mana tepatnya `OI_Delta_5m` dihitung?
5. Apakah `BookUpdatedAt` dan mark-price age benar-benar dipakai oleh risk gate?
6. Bagaimana sistem membuktikan SL/TP telah diakui bursa pada seluruh partial-fill path, dan apa tindakan jika SL gagal?
7. Apakah ada I/O atau callback jaringan di bawah mutex?
8. Bagaimana fill simulator memperhitungkan order queue dan kenapa `CumulativeVolumeBeyondLimit` belum dipakai?
9. Bagaimana daily breaker bertahan terhadap restart dan pending order?
10. Apakah endpoint dan lifecycle conditional order sudah diverifikasi terhadap dokumentasi USDⓈ-M Futures terbaru, bukan dokumentasi produk lain?

## 10. Putusan akhir

**Strategi:** masuk akal sebagai hipotesis momentum breakout, tetapi belum terbukti punya edge; jangan mengunci parameter sebagai “tervalidasi” tanpa data.  
**Dry-run:** berguna setelah diperbaiki untuk menguji event pipeline dan operasional, tetapi fill simulator sekarang terlalu optimistis/heuristik untuk dijadikan ukuran profitabilitas.  
**Live bot:** belum aman. Perbaiki seluruh blocker P0 dan state/account reconciliation sebelum API live diizinkan membuka posisi.

Audit ini menilai isi dokumen dan pseudocode yang diberikan. Audit ini **bukan** hasil backtest dan tidak menyatakan probabilitas profit/kerugian yang belum diukur.

## Sumber eksternal yang diverifikasi

- Binance, *USDⓈ-M Futures WebSocket System Upgrade Notice* (6 Maret 2026; legacy URL retired 23 April 2026): https://www.binance.com/en/support/announcement/detail/ebf9b0aa9eca4ff3804eef6fb09ba32a
- Binance Developer Docs, Futures (USDⓈ-M) Introduction, last modified 15 October 2026: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/Introduction
- Binance Developer Docs, Futures Kline schema (field mapping for quote volume and taker-buy quote volume should be verified against the active USDⓈ-M documentation at implementation time): https://developers.binance.com/en/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
