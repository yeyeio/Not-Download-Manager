//! Phase 4.5 Remediation Integration Test Suite
//! Verifies:
//! - FINDING-4.4-01: Cooperative SHA-256 cancellation during 100 MB file verification,
//!   immediate Windows file-handle release, and successful file deletion without OS error 32.
//! - FINDING-4.4-02: Exactly-once terminal event ownership and race elimination between
//!   DeleteDownload and Finished/Progress across all critical finalization interleavings.

use not_download_manager::db::{self, compute_sha256};
use not_download_manager::engine::{
    clear_test_hooks, register_test_barrier, set_task_expected_sha256, spawn_download_engine,
    EngineCommand, EngineEvent, TestHookPoint,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Barrier, Notify};

struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_p45_{}_{}_{}",
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
        for _ in 0..10 {
            if std::fs::remove_dir_all(&self.0).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// In-process HTTP 206 mock streaming server.
async fn start_mock_range_server(
    data: Arc<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Binding loopback listener must succeed with BypassSandbox");
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{}/testfile.bin", port);

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let dummy = data.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);

                if req_str.starts_with("HEAD") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-p45\"\r\nConnection: close\r\n\r\n",
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
                    let slice_len = end.saturating_sub(start) + 1;
                    let resp_header = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        start, end, dummy.len(), slice_len
                    );
                    let _ = socket.write_all(resp_header.as_bytes()).await;
                    if start <= end && end < dummy.len() {
                        let _ = socket.write_all(&dummy[start..=end]).await;
                    }
                    let _ = socket.shutdown().await;
                }
            });
        }
    });

    (url, handle)
}

// ============================================================================
// R3 TEST 1: Cooperative SHA-256 Cancellation on 100 MB File (FINDING-4.4-01)
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_hashing_100mb_deletion_cooperative_abort_and_windows_unlink() {
    let td = TestDir::new("sha256_100mb_abort");
    let file_path = td.path().join("download_100mb.bin");

    // 100 MB dataset: 100 * 1024 * 1024 = 104,857,600 bytes
    // (approx 1,600 reads of 64 KiB)
    const FILE_SIZE: usize = 100 * 1024 * 1024;
    let dummy_data: Arc<Vec<u8>> = Arc::new((0..FILE_SIZE).map(|i| (i % 251) as u8).collect());
    let expected_sha256 = compute_sha256(&dummy_data);

    let (url, _server) = start_mock_range_server(dummy_data).await;
    let mut engine = spawn_download_engine();

    let task_id = 101u64;

    // Register expected SHA-256
    set_task_expected_sha256(task_id, Some(expected_sha256));

    // Install notification on first read of hashing
    let hash_started_notify = Arc::new(Notify::new());
    {
        let mut hook = db::HASH_CHUNK_NOTIFY.lock().unwrap();
        *hook = Some(hash_started_notify.clone());
    }
    db::HASH_READ_ITERATIONS.store(0, Ordering::SeqCst);

    // Start download with SHA-256 configured
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path: file_path.clone(),
            connections: 4,
        })
        .unwrap();

    // Spawn deleter waiting for hashing to actively begin
    let tx_del = engine.cmd_sender.clone();
    let deleter_handle = tokio::spawn(async move {
        hash_started_notify.notified().await;
        // Hashing is confirmed actively in-flight holding the file open!
        tx_del
            .send(EngineCommand::DeleteDownload {
                id: task_id,
                cleanup_files: true,
            })
            .unwrap();
    });

    let mut deleted_events = Vec::new();
    let mut finished_events = Vec::new();

    let timeout_dur = Duration::from_secs(15);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(500), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                } if id == task_id => {
                    deleted_events.push((cleanup_success, error));
                    break;
                }
                EngineEvent::Finished { id, .. } if id == task_id => {
                    finished_events.push(id);
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    deleter_handle.await.unwrap();

    // Reset test hook
    {
        let mut hook = db::HASH_CHUNK_NOTIFY.lock().unwrap();
        *hook = None;
    }

    // 1. Verify hashing really stopped cooperatively and did NOT finish all 1,600 chunks
    let chunks_read = db::HASH_READ_ITERATIONS.load(Ordering::SeqCst);
    assert!(
        chunks_read > 0,
        "Hashing must have actively started (read > 0 chunks)"
    );
    assert!(
        chunks_read < 1000,
        "Hashing must have stopped cooperatively well before EOF (got {} chunks)",
        chunks_read
    );

    // 2. Exactly one Deleted event emitted
    assert_eq!(
        deleted_events.len(),
        1,
        "Exactly one Deleted event must be emitted"
    );
    let (cleanup_success, err) = &deleted_events[0];

    // 3. Cleanup success must be true, zero errors
    assert!(
        *cleanup_success,
        "File cleanup must succeed on Windows without error: {:?}",
        err
    );
    assert!(err.is_none(), "Error message must be None on successful deletion");

    // 4. Zero Finished events emitted
    assert_eq!(
        finished_events.len(),
        0,
        "No Finished event should be emitted when Delete is dispatched during verification"
    );

    // 5. File must be deleted from disk on Windows (no ERROR_SHARING_VIOLATION)
    assert!(
        !file_path.exists(),
        "Destination 100 MB file must be deleted from disk"
    );
}

// ============================================================================
// R3 TEST 2: Interleaving — Delete Right Before Finalization
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interleaving_delete_right_before_finalization() {
    let td = TestDir::new("delete_before_final");
    let file_path = td.path().join("before_final.bin");

    const FILE_SIZE: usize = 128 * 1024;
    let dummy_data: Arc<Vec<u8>> = Arc::new((0..FILE_SIZE).map(|i| (i % 251) as u8).collect());
    let (url, _server) = start_mock_range_server(dummy_data).await;
    let mut engine = spawn_download_engine();

    let task_id = 201u64;

    // Barrier with 2 parties: coordinator hook and test thread
    let barrier = Arc::new(Barrier::new(2));
    register_test_barrier(task_id, TestHookPoint::BeforeFinalization, barrier.clone());

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path: file_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until coordinator reaches BeforeFinalization
    barrier.wait().await;

    // Dispatch DeleteDownload before coordinator enters finalize_coordinator
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut deleted_events = Vec::new();
    let mut finished_events = Vec::new();

    let timeout_dur = Duration::from_secs(5);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(300), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                } if id == task_id => {
                    deleted_events.push((cleanup_success, error));
                    break;
                }
                EngineEvent::Finished { id, .. } if id == task_id => {
                    finished_events.push(id);
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    clear_test_hooks(task_id);

    assert_eq!(deleted_events.len(), 1, "Exactly one Deleted event required");
    assert!(deleted_events[0].0, "cleanup_success must be true");
    assert_eq!(finished_events.len(), 0, "No Finished event allowed");
    assert!(!file_path.exists(), "File must be removed from disk");
}

// ============================================================================
// R3 TEST 3: Interleaving — Delete Right After Coordinator Starts Finalization
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interleaving_delete_right_after_finalization_start() {
    let td = TestDir::new("delete_after_final_start");
    let file_path = td.path().join("after_final_start.bin");

    const FILE_SIZE: usize = 128 * 1024;
    let dummy_data: Arc<Vec<u8>> = Arc::new((0..FILE_SIZE).map(|i| (i % 251) as u8).collect());
    let (url, _server) = start_mock_range_server(dummy_data).await;
    let mut engine = spawn_download_engine();

    let task_id = 301u64;

    let barrier = Arc::new(Barrier::new(2));
    register_test_barrier(
        task_id,
        TestHookPoint::AfterFinalizationStart,
        barrier.clone(),
    );

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path: file_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until coordinator starts finalize_coordinator
    barrier.wait().await;

    // Dispatch DeleteDownload while coordinator is at start of finalization
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut deleted_events = Vec::new();
    let mut finished_events = Vec::new();

    let timeout_dur = Duration::from_secs(5);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(300), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                } if id == task_id => {
                    deleted_events.push((cleanup_success, error));
                    break;
                }
                EngineEvent::Finished { id, .. } if id == task_id => {
                    finished_events.push(id);
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    clear_test_hooks(task_id);

    assert_eq!(deleted_events.len(), 1, "Exactly one Deleted event required");
    assert!(deleted_events[0].0, "cleanup_success must be true");
    assert_eq!(finished_events.len(), 0, "No Finished event allowed");
    assert!(!file_path.exists(), "File must be removed from disk");
}

// ============================================================================
// R3 TEST 4: Interleaving — Delete During Terminal Event Preparation
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interleaving_delete_during_terminal_event_prep() {
    let td = TestDir::new("delete_during_prep");
    let file_path = td.path().join("during_prep.bin");

    const FILE_SIZE: usize = 128 * 1024;
    let dummy_data: Arc<Vec<u8>> = Arc::new((0..FILE_SIZE).map(|i| (i % 251) as u8).collect());
    let (url, _server) = start_mock_range_server(dummy_data).await;
    let mut engine = spawn_download_engine();

    let task_id = 401u64;

    let barrier = Arc::new(Barrier::new(2));
    register_test_barrier(
        task_id,
        TestHookPoint::DuringTerminalEventPrep,
        barrier.clone(),
    );

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path: file_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until coordinator starts preparing Finished terminal event
    barrier.wait().await;

    // Dispatch DeleteDownload during terminal event preparation
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut deleted_events = Vec::new();
    let mut finished_events = Vec::new();
    let mut progress_after_delete = 0usize;

    let timeout_dur = Duration::from_secs(5);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(300), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                } if id == task_id => {
                    deleted_events.push((cleanup_success, error));
                    break;
                }
                EngineEvent::Finished { id, .. } if id == task_id => {
                    finished_events.push(id);
                }
                EngineEvent::ProgressUpdate { id, .. } if id == task_id => {
                    if !deleted_events.is_empty() {
                        progress_after_delete += 1;
                    }
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    clear_test_hooks(task_id);

    assert_eq!(deleted_events.len(), 1, "Exactly one Deleted event required");
    assert!(deleted_events[0].0, "cleanup_success must be true");
    assert_eq!(finished_events.len(), 0, "No Finished event allowed");
    assert_eq!(
        progress_after_delete, 0,
        "Zero ProgressUpdate allowed after Deleted"
    );
    assert!(!file_path.exists(), "File must be removed from disk");
}

// ============================================================================
// R3 TEST 5: Interleaving — Delete Concurrently with Finished Commit
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interleaving_delete_concurrent_with_finished() {
    let td = TestDir::new("delete_concurrent_finished");
    let file_path = td.path().join("concurrent_finished.bin");

    const FILE_SIZE: usize = 128 * 1024;
    let dummy_data: Arc<Vec<u8>> = Arc::new((0..FILE_SIZE).map(|i| (i % 251) as u8).collect());
    let (url, _server) = start_mock_range_server(dummy_data).await;
    let mut engine = spawn_download_engine();

    let task_id = 501u64;

    let barrier = Arc::new(Barrier::new(2));
    register_test_barrier(
        task_id,
        TestHookPoint::BeforeTerminalEmit,
        barrier.clone(),
    );

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path: file_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until coordinator is right at the atomic commit gate
    barrier.wait().await;

    // Dispatch DeleteDownload concurrently with finalization commit
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut deleted_events = Vec::new();
    let mut finished_events = Vec::new();

    let timeout_dur = Duration::from_secs(5);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(300), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                } if id == task_id => {
                    deleted_events.push((cleanup_success, error));
                }
                EngineEvent::Finished { id, .. } if id == task_id => {
                    finished_events.push(id);
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    clear_test_hooks(task_id);

    // Exactly one Deleted event must be emitted
    assert_eq!(
        deleted_events.len(),
        1,
        "Exactly one Deleted event must be emitted"
    );
    assert!(deleted_events[0].0, "Cleanup must be successful");

    // Finished can either be 0 (if Delete preempted before atomic commit)
    // or 1 (if Finished committed first, followed by inactive Delete)
    assert!(
        finished_events.len() <= 1,
        "At most one Finished event allowed"
    );

    // In all interleaving cases, the file must be removed from disk
    assert!(!file_path.exists(), "File must be removed from disk");
}

// ============================================================================
// R3 TEST 6: Interleaving — Pause/Cancel During Finalization
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interleaving_pause_cancel_during_finalization() {
    let td = TestDir::new("pause_cancel_final");
    let file_path = td.path().join("pause_cancel_final.bin");

    const FILE_SIZE: usize = 128 * 1024;
    let dummy_data: Arc<Vec<u8>> = Arc::new((0..FILE_SIZE).map(|i| (i % 251) as u8).collect());
    let (url, _server) = start_mock_range_server(dummy_data).await;
    let mut engine = spawn_download_engine();

    let task_id = 601u64;

    let barrier = Arc::new(Barrier::new(2));
    register_test_barrier(
        task_id,
        TestHookPoint::AfterFinalizationStart,
        barrier.clone(),
    );

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path: file_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until coordinator starts finalization of complete download
    barrier.wait().await;

    // Dispatch PauseDownload during finalization
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: task_id })
        .unwrap();

    let mut finished_events = Vec::new();
    let mut paused_events = Vec::new();

    let timeout_dur = Duration::from_secs(5);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(300), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Finished { id, .. } if id == task_id => {
                    finished_events.push(id);
                    break;
                }
                EngineEvent::Paused { id } if id == task_id => {
                    paused_events.push(id);
                    break;
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    clear_test_hooks(task_id);

    // Because download was 100% finished and in finalization, it completes Finished cleanly
    assert_eq!(
        finished_events.len() + paused_events.len(),
        1,
        "Exactly one terminal event must be emitted"
    );
}
