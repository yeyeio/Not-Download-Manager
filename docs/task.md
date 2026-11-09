# TASK: Comprehensive Rust Download Manager Performance Audit & IDM Benchmark

Bertindak sebagai Senior Rust Performance Engineer, Network Engineer, dan Software Reliability Engineer.

Saya memiliki aplikasi download manager yang dibuat menggunakan Rust dan bertujuan menyaingi Internet Download Manager (IDM).

Tugas kamu adalah melakukan audit menyeluruh terhadap implementasi yang sudah ada, mengukur performanya, mengidentifikasi bottleneck, dan membandingkan hasilnya dengan IDM.

**PENTING:**
- Jangan langsung melakukan refactoring atau optimasi.
- Jangan mengubah source code sebelum audit awal selesai.
- Jangan mengasumsikan arsitektur proyek berdasarkan nama file.
- Pelajari repository dan implementasi aktual terlebih dahulu.
- Semua kesimpulan harus berdasarkan source code, hasil pengujian, atau profiling.
- Jangan mengarang benchmark atau mengklaim IDM kalah tanpa pengukuran.
- Pertahankan seluruh fitur dan perilaku aplikasi yang sudah ada.

## PHASE 1 — Repository & Architecture Audit

Pelajari seluruh repository.

Identifikasi:

1. Struktur Cargo workspace.
2. Crate dan dependency yang digunakan.
3. HTTP client implementation.
4. Async runtime dan task management.
5. Download worker architecture.
6. Multi-connection implementation.
7. Dynamic segmentation algorithm.
8. Connection pooling.
9. Retry dan backoff mechanism.
10. Pause/resume implementation.
11. File writing mechanism.
12. Database dan metadata persistence.
13. Crash recovery.
14. Checksum/integrity verification.
15. Browser integration jika tersedia.

Periksa apakah implementasi menggunakan:
- Tokio atau async runtime lain.
- Reqwest, Hyper, atau HTTP client lain.
- Sequential atau concurrent file writing.
- Mutex/RwLock yang berpotensi menjadi bottleneck.
- Unbounded channels.
- Excessive task spawning.
- Excessive memory allocation.
- Unnecessary cloning.
- Blocking I/O di async runtime.

Buat architecture map berdasarkan kode aktual.

Output:
- Architecture overview.
- Dependency analysis.
- Critical code paths.
- Potential performance bottlenecks.
- Reliability concerns.

Cantumkan file path dan nomor baris untuk setiap temuan.

## PHASE 2 — Download Algorithm Audit

Audit algoritma download secara mendalam.

### A. HTTP compatibility

Periksa:
- HEAD request.
- Content-Length detection.
- Accept-Ranges handling.
- HTTP Range Requests.
- HTTP 206 validation.
- Content-Range validation.
- Redirect handling.
- Content-Encoding.
- ETag dan Last-Modified.
- Authentication dan cookies.
- Signed URL expiration.

Pastikan engine tidak melakukan segmented download secara keliru ketika server tidak mendukung HTTP Range.

### B. Segmentation

Evaluasi:
- Initial segment allocation.
- Segment size calculation.
- Dynamic splitting.
- Work stealing.
- Worker allocation.
- Segment scheduling.
- Segment completion tracking.
- Overlap prevention.
- Missing-byte prevention.

Verifikasi bahwa seluruh rentang byte:
- Tercover.
- Tidak overlap.
- Tidak ada gap.
- Tidak ditulis oleh lebih dari satu worker secara bersamaan.

### C. Adaptive concurrency

Periksa:
- Jumlah worker aktif.
- Throughput masing-masing worker.
- Throughput total.
- Worker idle time.
- Retry rate.
- HTTP 429 dan 503 handling.
- Connection timeout.
- Congestion response.

Evaluasi apakah concurrency control benar-benar adaptif atau hanya menggunakan jumlah koneksi tetap.

### D. Storage engine

Periksa:
- Random-access file writing.
- File preallocation.
- Write buffering.
- Disk flush.
- Lock contention.
- I/O synchronization.
- Checkpoint consistency.
- Crash recovery.

Berikan rekomendasi berdasarkan implementasi aktual, bukan asumsi.

## PHASE 3 — Build & Static Analysis

Jalankan pemeriksaan berikut:

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Kemudian:
- Periksa dependency yang tidak digunakan.
- Periksa unsafe Rust.
- Periksa panic dan unwrap pada critical path.
- Periksa deadlocks.
- Periksa race conditions.
- Periksa memory leaks dan unbounded resource growth.
- Periksa error handling.

Jangan memperbaiki error secara otomatis sebelum membuat laporan awal.

## PHASE 4 — Controlled Benchmark Environment

Buat benchmark environment lokal yang terkontrol.

Gunakan HTTP server lokal atau dedicated benchmark server yang menyediakan file statis.

Jangan hanya mengandalkan download dari internet publik.

Siapkan file:
- 100 MB.
- 1 GB.
- 5 GB.

Jika resource memungkinkan, tambahkan 10 GB.

Pastikan:
- Server mendukung HTTP Range.
- File sumber tidak berubah.
- Tidak ada kompresi yang mengubah byte range.
- Bandwidth server mencukupi.
- Disk tidak menjadi bottleneck.
- Network baseline tercatat.

Buat dua kelompok pengujian:

A. HTTP server lokal untuk menguji overhead engine, segmentation, scheduling, dan storage.

B. Remote server yang dikontrol untuk menguji network throughput, latency, dan concurrency.

## PHASE 5 — IDM Comparative Benchmark

Bandingkan aplikasi ini dengan:

1. Internet Download Manager.
2. Aria2.
3. Aplikasi Rust yang sedang diaudit.

Gunakan mesin, file, server, dan kondisi jaringan yang sama.

Uji konfigurasi:
- 1 connection.
- 4 connections.
- 8 connections.
- 16 connections.
- 32 connections jika didukung.

Lakukan minimal 5 pengulangan untuk setiap konfigurasi.

Catat:

| Metric | Description |
|---|---|
| Average throughput | MB/s |
| Peak throughput | MB/s |
| Completion time | Seconds |
| CPU utilization | Percentage |
| RAM consumption | MB |
| Disk write throughput | MB/s |
| Active connections | Count |
| Retry count | Count |
| Worker idle time | Percentage |
| Final file size | Bytes |
| SHA-256 | Integrity |

Gunakan median sebagai hasil utama dan tampilkan variasi hasil antar-pengulangan.

Jangan membandingkan hasil yang berasal dari kondisi server atau bandwidth berbeda.

Jika IDM tidak tersedia di environment, jangan mengarang hasilnya. Siapkan benchmark harness dan tandai perbandingan IDM sebagai pending.

## PHASE 6 — Reliability & Stress Testing

Uji skenario berikut:

1. Download normal.
2. Pause dan resume.
3. Aplikasi dihentikan paksa ketika download berlangsung.
4. Internet terputus.
5. Server mengembalikan HTTP 429.
6. Server mengembalikan HTTP 503.
7. Server tidak mendukung Range.
8. Server mengubah ETag.
9. Download URL redirect.
10. File berukuran besar.
11. Banyak download secara bersamaan.
12. Disk memiliki throughput terbatas.
13. Server memperlambat salah satu worker.

Verifikasi:
- Tidak ada file corruption.
- Tidak ada missing bytes.
- Tidak ada duplicated bytes.
- Resume tidak mengulang seluruh file tanpa alasan.
- Tidak ada task yang menggantung.
- Tidak ada worker yang berjalan setelah task dibatalkan.
- Metadata tetap konsisten setelah crash.

## PHASE 7 — Profiling

Jika ditemukan bottleneck, lakukan profiling.

Gunakan tool yang sesuai:
- cargo flamegraph.
- Criterion.
- tokio-console.
- perf pada Linux.
- Windows Performance Recorder/Analyzer pada Windows.
- Process Explorer.
- Disk I/O monitoring.

Identifikasi:
- CPU hotspots.
- Allocation hotspots.
- Lock contention.
- Async scheduling overhead.
- Network bottleneck.
- Disk bottleneck.
- Excessive syscalls.

Jangan mengoptimasi kode yang tidak terbukti menjadi bottleneck.

## PHASE 8 — Performance Scoring

Buat penilaian berdasarkan lima kategori:

1. Download throughput.
2. CPU/RAM efficiency.
3. Dynamic segmentation quality.
4. Reliability.
5. Recovery performance.

Gunakan bobot:

| Category | Weight |
|---|---:|
| Throughput | 35% |
| Resource efficiency | 15% |
| Segmentation | 20% |
| Reliability | 20% |
| Recovery | 10% |

Jika suatu kategori belum diuji, tandai NOT TESTED. Jangan memberikan nilai final seolah-olah seluruh kategori sudah terukur.

## PHASE 9 — Final Report

Buat laporan:

`docs/performance-audit.md`

Struktur:

1. Executive Summary.
2. Repository Architecture.
3. Implementation Review.
4. HTTP and Segmentation Audit.
5. Performance Benchmark.
6. IDM Comparison.
7. CPU/RAM/Disk Analysis.
8. Reliability Testing.
9. Identified Bottlenecks.
10. Optimization Recommendations.
11. Priority Classification.
12. Final Assessment.

Setiap finding harus memiliki:
- Severity: Critical, High, Medium, Low.
- Evidence.
- Affected file and line.
- Performance impact.
- Recommended fix.
- Expected benefit.
- Verification method.

## EXECUTION RULES

- Audit first, optimize later.
- Never fabricate benchmark results.
- Never silently change application behavior.
- Never remove existing features to improve benchmark scores.
- Preserve a clean Git working tree or create a dedicated audit branch.
- Record machine specifications and environment.
- Run tests before and after any approved optimization.
- Do not run destructive tests against production servers.
- Do not use third-party servers without authorization.

**Final objective:**

Tentukan secara objektif apakah Rust download manager ini sudah kompetitif dengan IDM, bagian mana yang masih tertinggal, dan optimasi apa yang memberikan peningkatan performa terbesar berdasarkan bukti pengujian.