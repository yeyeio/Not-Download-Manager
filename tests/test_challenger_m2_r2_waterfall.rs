//! Isolated SQLite Waterfall Test
//! Runs in its own process so HYPERDOWNLOAD_DB_PATH does not conflict with parallel tests.

use app::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_waterfall_isolated_{}_{}_{}",
            name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::create_dir_all(&p);
        TestDir(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn wait_for_event<F>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<EngineEvent>,
    timeout: Duration,
    mut predicate: F,
) -> Option<EngineEvent>
where
    F: FnMut(&EngineEvent) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        while let Ok(evt) = rx.try_recv() {
            if predicate(&evt) {
                return Some(evt);
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

#[tokio::test]
async fn test_sqlite_waterfall_delete_unlinks_files() {
    let td = TestDir::new("sqlite_waterfall");
    let db_path = td.path().join("waterfall_test.db");
    let id = 8003u64;
    let save_path = td.path().join("db_download.bin");
    let partinfo_path = td.path().join("db_download.bin.partinfo");
    let tmp_path = td.path().join("db_download.bin.tmp");

    std::fs::write(&save_path, b"downloaded bytes from prior session").unwrap();
    std::fs::write(&partinfo_path, b"{\"version\":1}").unwrap();
    std::fs::write(&tmp_path, b"temp bytes").unwrap();

    // Create database and insert row
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                filename TEXT NOT NULL,
                save_path TEXT NOT NULL,
                total_size INTEGER,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                error_message TEXT,
                etag TEXT,
                last_modified TEXT,
                expected_sha256 TEXT,
                hash_status TEXT DEFAULT 'NotConfigured'
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO downloads (id, url, filename, save_path, status, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![id as i64, "http://example.com/file.bin", "db_download.bin", save_path.to_str().unwrap(), "Paused", 0, 0],
        )
        .unwrap();
    }

    // Set HYPERDOWNLOAD_DB_PATH environment variable for engine to locate
    unsafe {
        std::env::set_var("HYPERDOWNLOAD_DB_PATH", db_path.to_str().unwrap());
    }

    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;

    // Delete without Start (never registered in known_paths or INACTIVE_SAVE_PATHS)
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    let deleted = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
    })
    .await;

    assert!(deleted.is_some(), "Must emit Deleted via SQLite waterfall");
    match deleted.unwrap() {
        EngineEvent::Deleted { cleanup_success, error, .. } => {
            assert!(cleanup_success, "Cleanup must succeed: error={:?}", error);
        }
        _ => unreachable!(),
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Files must be unlinked
    assert!(!save_path.exists(), "Target file must be unlinked via SQLite lookup");
    assert!(!partinfo_path.exists(), ".partinfo must be unlinked via SQLite lookup");
    assert!(!tmp_path.exists(), ".tmp must be unlinked via SQLite lookup");

    // Clean up env var
    unsafe {
        std::env::remove_var("HYPERDOWNLOAD_DB_PATH");
    }
}
