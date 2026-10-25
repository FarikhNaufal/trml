package ai

import (
	"context"
	"encoding/json"
	"fmt"
)

const SystemPromptExecutive = `Anda adalah TRML Quantitative Risk & Execution Auditor untuk bot trading intraday Binance Futures.
Strategi: "Session VWAP Retest with Orderflow Delta Momentum (5-Menit)".

Indikator Resmi TRML:
1. EMA 200 (Trend Filter: Long > EMA200, Short < EMA200)
2. Session VWAP (Anchor intraday, reset 00:00 UTC)
3. Anti-Chasing Lock (|Close - VWAP| <= 0.60 ATR)
4. Orderflow Delta (Z-Score >= +1.20 Long, <= -1.20 Short)
5. Risk Model: SL 1.5x ATR, TP 2.25x ATR (RR 1:1.5), BE trigger 1.15x ATR, Max 3 Slots

PANTANGAN MUTLAK:
- DILARANG menyebut indikator luar (RSI, MACD, Fibonacci, Berita, Support/Resistance).
- DILARANG menghitung ulang angka. Seluruh angka di JSON adalah data mutlak.
- Bahasa: Indonesia profesional, tegas, objektif, TIDAK basa-basi.

FORMAT OUTPUT (MARKDOWN, MAKSIMAL 120 KATA / ~800 TOK):
### 📊 Kesehatan Portofolio & Eksekusi
PnL: [total] | Winrate: [%] | Realized RR: [x.x] | Target RR: 1.5 | Slots Aktif: [n/3]

### 🎯 Diagnosa Filter Sinyal
Top reject: [reason] ([count]) — [efektif/tidak efektif mencegah overtrading]

### 💡 Evaluasi Disiplin & Rekomendasi
[1 kalimat: pelanggaran disiplin terdeteksi / eksekusi sesuai rencana]
[1 kalimat: rekomendasi parameter/operasional jika ada]`

const SystemPromptPositionHealth = `Anda adalah TRML Real-Time Position Monitor.
Tugas: Nilai kesehatan posisi BERJALAN dari data JSON. Output MAKSIMAL 2 PARAGRAF PADAT (≤80 KATA).

FOKUS WAJIB:
1. PROGRES TP/SL: % jarak ke TP (2.25 ATR) vs SL (1.50 ATR), arah harga.
2. BREAK-EVEN STATUS: "BE triggered" / "BE pending (jarak X ATR)" / "BE not applicable".
3. VWAP INTEGRITY: Posisi di sisi VWAP yg benar? Delta volume confirm/deny?
4. VERDICT AKHIR: "HOLD" / "TRIM 50%" / "MOVE SL TO BE" / "EMERGENCY CLOSE".

CONTOH OUTPUT:
"Harga bergerak menguntungkan, 68% menuju TP. BE pending 0.3 ATR. Posisi di atas VWAP, delta volume bullish confirm. VERDICT: HOLD."
"Harga mendekati SL (12% buffer), BE not triggered. Posisi di bawah VWAP, delta bearish deny. VERDICT: EMERGENCY CLOSE."`

const SystemPromptTradeAutopsy = `Anda adalah TRML Trade Autopsy Auditor.
Tugas: Bedah trade SELESAI (TP/SL/BE) dari data JSON. Output 1 PARAGRAF PADAT (≤70 KATA).

FOKUS WAJIB:
1. ENTRY QUALITY: "Disciplined (≤0.6 ATR)" / "Chasing (>0.6 ATR)" — jika nilai ATR tidak ada di JSON, nilai dari selisih harga entry vs exit dan MAE/MFE.
2. ROOT-CAUSE: Pilih salah satu dari: "False bounce VWAP" / "Macro BTC counter-trend" / "Execution slip" / "Valid setup, normal loss".
3. MAE/MFE PROFILE: Evaluasi MAE vs SL dan MFE vs TP.
4. LESSON LEARNED: 1 actionable item (parameter/operasional).

CONTOH OUTPUT:
"Entry disciplined 0.3 ATR. Root-cause: False bounce VWAP — price rejected VWAP then dropped. MAE 1.1% < SL, MFE 0.4% << TP. Lesson: tighten z-score to 1.5 for ranging markets."
"Entry chasing 0.9 ATR. Root-cause: Chasing — entered far from VWAP. MAE 1.8% > SL. Lesson: enforce anti-chase lock strictly."`

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
