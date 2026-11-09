use app::db::Database;
use app::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use app::types::{DownloadItem, DownloadSettings, DownloadStatus};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct TempDir(PathBuf);
impl TempDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_bench_{}_{}_{}",
            name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::create_dir_all(&p);
        TempDir(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn measure_reconciliation(n: usize) -> Duration {
    let td = TempDir::new(&format!("rec_{}", n));
    let db_path = td.path().join("bench.db");
    let db = Database::open(&db_path).unwrap();

    // Populate n tasks in a single setup transaction
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        let now = chrono::Local::now().timestamp();

        for i in 1..=n {
            let status = if i % 3 == 0 {
                "Downloading"
            } else if i % 3 == 1 {
                "Paused"
            } else {
                "Completed"
            };
            tx.execute(
                "INSERT INTO downloads (id, url, filename, save_path, total_size, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    i as i64,
                    format!("https://example.com/file_{}.dat", i),
                    format!("file_{}.dat", i),
                    td.path().join(format!("file_{}.dat", i)).to_string_lossy().to_string(),
                    5_000_000i64,
                    status,
                    now,
                    now,
                ],
            ).unwrap();
        }
        tx.commit().unwrap();
    }

    // Time reconcile_and_load_all()
    let t0 = Instant::now();
    let loaded = db.reconcile_and_load_all().unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(loaded.len(), n);
    elapsed
}

#[tokio::main]
async fn main() {
    println!("===============================================================");
    println!(" HYPERDOWNLOAD PHASE 4.1 — RELIABILITY & PERFORMANCE BENCHMARK ");
    println!("===============================================================");

    // 1. Startup Reconciliation Latency Scaling (10, 100, 1,000, 10,000 tasks)
    println!("\n--- 1. Startup Reconciliation Latency (Single-Transaction Batched) ---");
    let scales = [10, 100, 1000, 10000];
    for &n in &scales {
        let d = measure_reconciliation(n);
        println!("Tasks: {:5} | Latency: {:8.3} ms ({:.4?})", n, d.as_secs_f64() * 1000.0, d);
    }

    // 2. Delete Completion Latency & Filesystem Cleanup Success Rate
    println!("\n--- 2. Delete Lifecycle Latency & Filesystem Cleanup Rate ---");
    let td = TempDir::new("del_bench");
    let engine = spawn_download_engine();
    let mut evt_rx = engine.event_receiver;

    let test_count = 50;
    let mut cleanup_successes = 0;
    let mut total_delete_duration = Duration::ZERO;

    for i in 1..=test_count {
        let save_path = td.path().join(format!("file_{}.dat", i));
        let partinfo_path = td.path().join(format!("file_{}.dat.partinfo", i));

        std::fs::write(&save_path, vec![0x55; 1024 * 1024]).unwrap();
        std::fs::write(&partinfo_path, b"{\"dummy\": true}").unwrap();

        let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
            id: i,
            url: "http://127.0.0.1:9999/dummy".into(),
            save_path: save_path.clone(),
            connections: 1,
        });

        let t_del = Instant::now();
        engine.cmd_sender.send(EngineCommand::DeleteDownload {
            id: i,
            cleanup_files: true,
        }).unwrap();

        // Await confirmation
        while let Some(evt) = evt_rx.recv().await {
            if let EngineEvent::Deleted { id, cleanup_success, .. } = evt {
                if id == i {
                    total_delete_duration += t_del.elapsed();
                    if cleanup_success && !save_path.exists() && !partinfo_path.exists() {
                        cleanup_successes += 1;
                    }
                    break;
                }
            }
        }
    }

    let avg_del_ms = (total_delete_duration.as_secs_f64() * 1000.0) / test_count as f64;
    let cleanup_rate = (cleanup_successes as f64 / test_count as f64) * 100.0;
    println!("Tested: {} tasks | Avg Delete Latency: {:.3} ms | Filesystem Cleanup: {:.1}% ({}/{})",
        test_count, avg_del_ms, cleanup_rate, cleanup_successes, test_count);

    // 3. Batch Delete Completed SQL Performance
    println!("\n--- 3. Delete Completed Atomic Batch Query Performance ---");
    let td_db = TempDir::new("del_completed_bench");
    let db_path = td_db.path().join("completed.db");
    let db = Database::open(&db_path).unwrap();

    for i in 1..=1000 {
        let status = if i % 2 == 0 { DownloadStatus::Completed } else { DownloadStatus::Paused };
        let item = DownloadItem {
            id: i,
            url: format!("https://example.com/{}.dat", i),
            filename: format!("{}.dat", i),
            status,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
    }
    let t_dc = Instant::now();
    let deleted = db.delete_completed_downloads().unwrap();
    let d_dc = t_dc.elapsed();
    println!("Deleted: {} completed records in {:.3} ms (Remaining: {})",
        deleted, d_dc.as_secs_f64() * 1000.0, db.get_download_count().unwrap());

    println!("\n===============================================================");
    println!(" BENCHMARK COMPLETE ");
    println!("===============================================================");
}
