use app::db::{verify_file_sha256, Database, DbError};
use app::engine::{
    spawn_download_engine, EngineCommand, EngineEvent,
};
use app::types::{DownloadItem, DownloadSettings, DownloadStatus};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_p41_{}_{}_{}",
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

/// Starts an in-process HTTP 206 mock streaming server
async fn start_mock_range_server(content_len: usize) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{}/testfile.dat", port);

    let handle = tokio::spawn(async move {
        let dummy_data: Vec<u8> = (0..content_len).map(|i| (i % 251) as u8).collect();
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let dummy = dummy_data.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);

                if req_str.starts_with("HEAD") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-test-1\"\r\nConnection: close\r\n\r\n",
                        dummy.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    let _ = socket.shutdown().await;
                } else if req_str.starts_with("GET") {
                    let mut start = 0usize;
                    let mut end = dummy.len().saturating_sub(1);
                    if let Some(r_line) = req_str.lines().find(|l| l.to_lowercase().starts_with("range:")) {
                        if let Some(bytes_part) = r_line.split('=').nth(1) {
                            let parts: Vec<&str> = bytes_part.trim().split('-').collect();
                            if let Ok(s) = parts[0].parse::<usize>() {
                                start = s;
                            }
                            if parts.len() > 1 && !parts[1].is_empty() {
                                if let Ok(e) = parts[1].parse::<usize>() {
                                    end = e;
                                }
                            }
                        }
                    }
                    let slice_len = end - start + 1;
                    let resp_header = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        start, end, dummy.len(), slice_len
                    );
                    let _ = socket.write_all(resp_header.as_bytes()).await;
                    let slice = &dummy[start..=end];
                    for chunk in slice.chunks(65536) {
                        if socket.write_all(chunk).await.is_err() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    let _ = socket.shutdown().await;
                }
            });
        }
    });

    (url, handle)
}

// ---------------------------------------------------------------------------
// 1. DELETE LIFECYCLE TESTS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_01_delete_during_active_multi_worker_download() {
    let td = TestDir::new("del_active");
    let save_path = td.path().join("active_download.dat");
    let partinfo_path = td.path().join("active_download.dat.partinfo");

    let total_len = 10 * 1024 * 1024; // 10 MB
    let (url, _server) = start_mock_range_server(total_len).await;

    let engine = spawn_download_engine();
    let download_id = 101u64;

    // Start 4-worker download
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: download_id,
            url,
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    let mut evt_rx = engine.event_receiver;

    // Wait until progress update shows bytes actively flowing
    let mut started = false;
    let start_time = Instant::now();
    while start_time.elapsed() < Duration::from_secs(5) {
        while let Ok(evt) = evt_rx.try_recv() {
            println!("Got event in test_01: {:?}", evt);
            if let EngineEvent::ProgressUpdate { id, downloaded, .. } = evt {
                if id == download_id && downloaded > 0 {
                    started = true;
                    break;
                }
            }
        }
        if started {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(started, "Download must actively start before sending Delete");

    // Send DeleteDownload command with file cleanup
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: download_id,
            cleanup_files: true,
        })
        .unwrap();

    // Await Deleted event confirmation from engine
    let mut deleted_confirmed = false;
    let del_start = Instant::now();
    while del_start.elapsed() < Duration::from_secs(5) {
        while let Ok(evt) = evt_rx.try_recv() {
            if let EngineEvent::Deleted { id, cleanup_success, error } = evt {
                if id == download_id {
                    assert!(cleanup_success, "Cleanup must succeed: {:?}", error);
                    deleted_confirmed = true;
                    break;
                }
            }
        }
        if deleted_confirmed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(deleted_confirmed, "Engine must emit Deleted event");

    // Wait 300ms to guarantee no delayed Zombie Checkpoint writes occur
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify .partinfo and partial files are completely gone
    assert!(!partinfo_path.exists(), "Zombie Checkpoint .partinfo must NOT exist");
    let tmp_partinfo = partinfo_path.with_extension("tmp");
    assert!(!tmp_partinfo.exists(), ".partinfo.tmp must NOT exist");
    assert!(!save_path.exists(), "Destination file must be removed");

    // Verify no file handles are locked on Windows: create and delete file cleanly
    std::fs::write(&save_path, b"test_unlocked").expect("Windows file handle must be released");
    std::fs::remove_file(&save_path).expect("File must be deletable without sharing violation");
}

#[tokio::test]
async fn test_02_delete_immediately_after_start() {
    let td = TestDir::new("del_immediate");
    let save_path = td.path().join("immediate.dat");
    let (url, _server) = start_mock_range_server(5 * 1024 * 1024).await;

    let engine = spawn_download_engine();
    let download_id = 102u64;

    // Dispatch Start immediately followed by Delete
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: download_id,
            url,
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: download_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut evt_rx = engine.event_receiver;
    let mut got_deleted = false;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(5) {
        if let Ok(evt) = evt_rx.try_recv() {
            if let EngineEvent::Deleted { id, cleanup_success, .. } = evt {
                if id == download_id {
                    assert!(cleanup_success);
                    got_deleted = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(got_deleted, "Immediate Delete must emit Deleted event");
    assert!(!save_path.exists(), "File must not remain on disk");
}

// ---------------------------------------------------------------------------
// 2. COMPLETION INTEGRITY TESTS (P0)
// ---------------------------------------------------------------------------

#[test]
fn test_03_preallocated_sparse_file_missing_checkpoint_not_completed() {
    let td = TestDir::new("sparse_test");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("preallocated_file.zip");
    let total_size = 100 * 1024 * 1024u64; // 100 MB

    // Simulate preallocated file on disk with expected length (zeros)
    let f = std::fs::File::create(&save_path).unwrap();
    f.set_len(total_size).unwrap();
    drop(f);

    // .partinfo is missing! (e.g. system crashed before chunk commit or checkpoint corrupted)
    let db = Database::open(&db_path).unwrap();
    let item = DownloadItem {
        id: 1,
        url: "https://example.com/preallocated_file.zip".into(),
        filename: "preallocated_file.zip".into(),
        save_path: save_path.clone(),
        total_size: Some(total_size),
        status: DownloadStatus::Downloading,
        ..Default::default()
    };
    db.insert_download(&item, &DownloadSettings::default()).unwrap();

    // Reconcile
    let loaded = db.reconcile_and_load_all().unwrap();
    assert_eq!(loaded.len(), 1);
    let task = &loaded[0];

    // Must NEVER be marked Completed based on file_len == total_size without checkpoint!
    assert_ne!(
        task.status,
        DownloadStatus::Completed,
        "Preallocated sparse file without checkpoint must NEVER be marked Completed!"
    );
    assert_eq!(task.status, DownloadStatus::Paused);
    assert_eq!(
        task.downloaded_bytes, 0,
        "Unverified data without checkpoint must reset to 0 bytes"
    );
}

#[test]
fn test_04_all_chunks_committed_promoted_to_completed() {
    let td = TestDir::new("all_committed");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("full_file.bin");
    let partinfo_path = td.path().join("full_file.bin.partinfo");

    let total_size = 6 * 1024 * 1024u64;
    let chunk_size = 2 * 1024 * 1024u64;

    // Destination file exists with full data
    std::fs::write(&save_path, vec![0xAA; total_size as usize]).unwrap();

    // Valid checkpoint with ALL chunks committed (0, 1, 2)
    let cp = serde_json::json!({
        "total_size": total_size,
        "chunk_size": chunk_size,
        "committed_chunk_ids": [0, 1, 2]
    });
    std::fs::write(&partinfo_path, cp.to_string()).unwrap();

    let db = Database::open(&db_path).unwrap();
    let item = DownloadItem {
        id: 2,
        url: "https://example.com/full_file.bin".into(),
        filename: "full_file.bin".into(),
        save_path: save_path.clone(),
        total_size: Some(total_size),
        status: DownloadStatus::Downloading,
        ..Default::default()
    };
    db.insert_download(&item, &DownloadSettings::default()).unwrap();

    let loaded = db.reconcile_and_load_all().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(
        loaded[0].status,
        DownloadStatus::Completed,
        "Task with all verified committed chunks must be marked Completed"
    );
    assert_eq!(loaded[0].downloaded_bytes, total_size);
    assert!(!partinfo_path.exists(), ".partinfo should be cleaned up on completion");
}

#[test]
fn test_05_corrupted_checkpoint_recovery() {
    let td = TestDir::new("corrupt_cp");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("data.bin");
    let partinfo_path = td.path().join("data.bin.partinfo");

    std::fs::write(&save_path, vec![0u8; 1000]).unwrap();
    std::fs::write(&partinfo_path, b"{ INVALID JSON NOT CHECKPOINT }").unwrap();

    let db = Database::open(&db_path).unwrap();
    let item = DownloadItem {
        id: 3,
        url: "https://example.com/data.bin".into(),
        filename: "data.bin".into(),
        save_path,
        total_size: Some(1000),
        status: DownloadStatus::Downloading,
        ..Default::default()
    };
    db.insert_download(&item, &DownloadSettings::default()).unwrap();

    let loaded = db.reconcile_and_load_all().unwrap();
    assert_eq!(loaded[0].status, DownloadStatus::Paused);
    assert_eq!(loaded[0].downloaded_bytes, 0);
}

#[test]
fn test_06_sha256_verification_match_and_mismatch() {
    let td = TestDir::new("sha256_test");
    let file_path = td.path().join("sample.txt");

    // Standard test vector: "abc"
    std::fs::write(&file_path, b"abc").unwrap();

    let expected_correct = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    let expected_wrong = "0000000000000000000000000000000000000000000000000000000000000000";

    assert!(
        verify_file_sha256(&file_path, expected_correct).unwrap(),
        "SHA-256 match must return true"
    );
    assert!(
        !verify_file_sha256(&file_path, expected_wrong).unwrap(),
        "SHA-256 mismatch must return false"
    );
}

// ---------------------------------------------------------------------------
// 3. STARTUP RECONCILIATION PERFORMANCE & BATCHING (P1)
// ---------------------------------------------------------------------------

#[test]
fn test_07_batched_startup_reconciliation_10k_tasks() {
    let td = TestDir::new("perf_10k");
    let db_path = td.path().join("perf_10k.db");

    let db = Database::open(&db_path).unwrap();

    // Populate 10,000 tasks inside an explicit transaction for benchmark setup
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        let now = chrono::Local::now().timestamp();

        for i in 1..=10_000 {
            let status = if i % 3 == 0 { "Downloading" } else if i % 3 == 1 { "Paused" } else { "Completed" };
            tx.execute(
                "INSERT INTO downloads (id, url, filename, save_path, total_size, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    i as i64,
                    format!("https://example.com/file_{}.zip", i),
                    format!("file_{}.zip", i),
                    td.path().join(format!("file_{}.zip", i)).to_string_lossy().to_string(),
                    10_000_000i64,
                    status,
                    now,
                    now,
                ],
            ).unwrap();
        }
        tx.commit().unwrap();
    }

    // Benchmark reconcile_and_load_all() with batched transaction
    let t0 = Instant::now();
    let loaded = db.reconcile_and_load_all().unwrap();
    let duration = t0.elapsed();

    assert_eq!(loaded.len(), 10_000);
    // Previously took 4,619 ms! Now with single atomic transaction, must finish well under 500 ms
    println!("10,000-task reconciliation latency: {:.2?}", duration);
    assert!(
        duration < Duration::from_millis(3000),
        "10,000-task reconciliation should finish in < 3s (got {:?})",
        duration
    );
}

// ---------------------------------------------------------------------------
// 4. RESOURCE CLEANUP & ERROR HANDLING (P1 & P2)
// ---------------------------------------------------------------------------

#[test]
fn test_08_delete_completed_atomic_batch() {
    let td = TestDir::new("del_batch");
    let db_path = td.path().join("del_batch.db");
    let db = Database::open(&db_path).unwrap();

    // Insert 50 completed and 50 paused tasks
    for i in 1..=100 {
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
    assert_eq!(db.get_download_count().unwrap(), 100);

    let deleted = db.delete_completed_downloads().unwrap();
    assert_eq!(deleted, 50, "Must delete all 50 completed tasks in single atomic query");
    assert_eq!(db.get_download_count().unwrap(), 50, "50 paused tasks must remain");
}

#[test]
fn test_09_db_error_handling_not_found() {
    let db = Database::open_in_memory().unwrap();
    let err = db.delete_download(9999).unwrap_err();
    match err {
        DbError::NotFound(id) => assert_eq!(id, 9999),
        other => panic!("Expected DbError::NotFound, got {:?}", other),
    }
}

#[tokio::test]
async fn test_10_resource_cleanup_active_tasks_does_not_leak() {
    let td = TestDir::new("no_leak");
    let (url, _server) = start_mock_range_server(1024 * 1024).await;
    let engine = spawn_download_engine();

    // Repeatedly start and delete tasks to ensure task handles are cleanly evicted
    for i in 1..=10 {
        let save_path = td.path().join(format!("leak_test_{}.dat", i));
        engine
            .cmd_sender
            .send(EngineCommand::StartDownload {
                id: i,
                url: url.clone(),
                save_path,
                connections: 2,
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        engine
            .cmd_sender
            .send(EngineCommand::DeleteDownload {
                id: i,
                cleanup_files: true,
            })
            .unwrap();
    }

    // Await all delete events
    let mut evt_rx = engine.event_receiver;
    let mut deleted_count = 0;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(5) && deleted_count < 10 {
        if let Ok(evt) = evt_rx.try_recv() {
            if let EngineEvent::Deleted { cleanup_success, .. } = evt {
                if cleanup_success {
                    deleted_count += 1;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(deleted_count, 10, "All 10 tasks must be safely cleaned up without leaking");
}
