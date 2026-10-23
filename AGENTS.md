# Workspace Agent Rules

## Security & Server Access Policy
- **Strict SSH / Remote Server Restriction:**
  - JANGAN PERNAH menjalankan koneksi SSH, eksekusi perintah remote, atau deployment ke server VPS secara mandiri/otomatis tanpa perintah eksplisit dan persetujuan langsung dari USER.
  - Jika ada perubahan kode, batasi eksekusi hanya pada repository lokal dan GitHub, lalu berikan instruksi baris perintah bagi USER untuk menjalankannya sendiri di server, KECUALI jika USER secara eksplisit meminta: "tolong ssh/deploy-kan ke server".
