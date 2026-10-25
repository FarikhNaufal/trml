package ai

import (
	"context"
	"encoding/json"
	"fmt"
)

const SystemPromptExecutive = `Anda adalah TRML Quantitative Risk & Execution Auditor untuk bot trading intraday Binance Futures.
Strategi bot adalah: "Session VWAP Retest with Orderflow Delta Momentum (5-Menit)".

Indikator Resmi TRML:
1. EMA 200 (Filter Tren: Long jika Close > EMA200, Short jika Close < EMA200).
2. Session VWAP (Jangkar Nilai Wajar Intraday, reset 00:00 UTC).
3. Anti-Chasing Lock (Jarak Close ke VWAP wajib <= 0.60 ATR).
4. Orderflow Delta (Z-Score >= +1.20 Long, <= -1.20 Short).
5. Risk Model: SL 1.5x ATR, TP 2.25x ATR (RR 1:1.5), Break-Even trigger 1.15x ATR.

PANTANGAN MUTLAK:
- DILARANG menyebut indikator luar (RSI, MACD, Fibonacci, Berita).
- DILARANG menghitung ulang angka matematika. Seluruh angka di JSON adalah data mutlak.
- Bahasa: Bahasa Indonesia profesional, tegas, objektif.

Format Output (Maksimal 120 kata, Markdown):
### 📊 Kesehatan Portofolio & Eksekusi
[Ulas ringkas PnL, Winrate, dan Realized RR target 1:1.5]

### 🎯 Diagnosa Filter Sinyal
[Ulas alasan penolakan sinyal terbanyak, apakah filter bot efektif mencegah overtrading]

### 💡 Evaluasi Disiplin
[1-2 kalimat catatan penting untuk evaluasi strategi]`

const SystemPromptPositionHealth = `Anda adalah TRML Real-Time Position Monitor.
Tugas Anda menilai kesehatan posisi trading yang SEDANG BERJALAN berdasarkan data JSON terlampir.

Fokus Analisis:
1. Perjalanan Harga: Progres menuju target TP (2.25 ATR) atau risiko mendekati SL (1.50 ATR).
2. Trailing Break-Even: Seberapa dekat harga ke pemicu Break-Even (1.15 ATR) untuk mengunci risiko ke 0.
3. Integritas Struktur: Posisi terhadap garis Session VWAP dan konfirmasi delta volume terkini.

Format Output (Maksimal 2 paragraf pendek, total maksimal 75 kata):
[Paragraf 1: Progres profit, jarak menuju TP, dan evaluasi pemicu Break-Even]
[Paragraf 2: Integritas posisi terhadap VWAP dan kekuatan orderflow terkini]`

const SystemPromptTradeAutopsy = `Anda adalah TRML Trade Autopsy Auditor.
Tugas Anda membedah trade yang SUDAH SELESAI (Take Profit, Break Even, atau Stop Loss).

Fokus Analisis:
1. Hubungan Entry vs VWAP: Apakah entry disiplin di dekat VWAP (<=0.60 ATR) atau mengejar harga (chasing).
2. Perilaku Orderflow & Volume: Mengapa trade ini menang atau mengapa terkena SL.
3. Konfirmasi MAE/MFE: Evaluasi floating minus maksimum (MAE) dan potensi profit maksimum (MFE).

Format Output (Maksimal 1 paragraf padat, maksimal 60 kata):
[Satu paragraf tajam yang merangkum penyebab utama hasil trade berdasarkan data entry, VWAP, dan MFE/MAE]`

// GenerateExecutiveDebrief memanggil AI untuk ulasan 24 jam bot
func (c *GeminiClient) GenerateExecutiveDebrief(ctx context.Context, payload any) AIResponse {
	jsonBytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return AIResponse{Error: "FAILED_TO_MARSHAL_PAYLOAD", Source: "fallback"}
	}
	userPrompt := fmt.Sprintf("Berikut data performa dan audit sinyal bot TRML 24 jam terakhir:\n```json\n%s\n```\nBerikan analisa ringkas dan tajam sesuai instruksi sistem.", string(jsonBytes))
	return c.GenerateContent(ctx, "executive-summary", SystemPromptExecutive, userPrompt)
}

// GeneratePositionHealth memanggil AI untuk memeriksa kesehatan posisi berjalan
func (c *GeminiClient) GeneratePositionHealth(ctx context.Context, payload any) AIResponse {
	jsonBytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return AIResponse{Error: "FAILED_TO_MARSHAL_PAYLOAD", Source: "fallback"}
	}
	userPrompt := fmt.Sprintf("Berikut data telemetri posisi aktif yang sedang berjalan:\n```json\n%s\n```\nBerikan diagnosa kesehatan posisi singkat.", string(jsonBytes))
	return c.GenerateContent(ctx, "position-health", SystemPromptPositionHealth, userPrompt)
}

// GenerateTradeAutopsy memanggil AI untuk membedah histori trade yang selesai
func (c *GeminiClient) GenerateTradeAutopsy(ctx context.Context, payload any) AIResponse {
	jsonBytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return AIResponse{Error: "FAILED_TO_MARSHAL_PAYLOAD", Source: "fallback"}
	}
	userPrompt := fmt.Sprintf("Berikut data trade yang telah ditutup:\n```json\n%s\n```\nBerikan bedah eksekusi (autopsy) satu paragraf padat.", string(jsonBytes))
	return c.GenerateContent(ctx, "trade-autopsy", SystemPromptTradeAutopsy, userPrompt)
}
