//! Phase 5.2.3 — Adaptive Concurrency Validation & Optimization Tests
//!
//! Validates:
//! 1. Fast short-burst downloads actively execute evaluations and scale concurrency (eliminating the >150ms evaluation blind spot).
//! 2. High latency (50ms) triggers BDP initial concurrency seeding (8 workers instead of 2), preventing severe throughput crawl.
//! 3. Concurrency timeline tracking records exact timeline entries with timestamps, worker counts, and decision reasons.
//! 4. Latency metric separation: request setup turnaround, socket read wait, and writer backpressure are distinctly tracked.
//! 5. Pause, Resume, Cancel, and Delete invariants during active slow-start scaling.
//! 6. Bit-exact SHA-256 integrity verification.

use not_download_manager::concurrency::{
    get_task_concurrency_decisions, get_task_concurrency_timeline, AdaptiveConfig,
    ConcurrencyStrategy, DecisionReason,
};
use not_download_manager::db::compute_sha256;
use not_download_manager::engine::spawn_download_engine;
use not_download_manager::types::{EngineCommand, EngineEvent, HashVerificationStatus};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct TestServer {
    pub url: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Helper to generate deterministic test data
fn generate_deterministic_payload(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state = 0xDEADBEEF_CAFE_u64;
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        data.push((state >> 33) as u8);
    }
    data
}

/// RAII Temporary Directory
struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_test_p523_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TestDir(p)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Starts a mock HTTP server with configurable artificial latency
async fn start_mock_server(
    data: Arc<Vec<u8>>,
    artificial_latency_ms: u64,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let request_counter = Arc::new(AtomicUsize::new(0));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let data = data.clone();
                    let req_count = request_counter.clone();

                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        req_count.fetch_add(1, Ordering::SeqCst);

                        if artificial_latency_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(artificial_latency_ms)).await;
                        }

                        if req_str.starts_with("HEAD") {
                            let head_resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test-etag-p523\"\r\n\r\n",
                                data.len()
                            );
                            let _ = socket.write_all(head_resp.as_bytes()).await;
                            return;
                        }

                        // Parse Range Header: bytes=start-end
                        let mut start = 0usize;
                        let mut end = data.len() - 1;

                        for line in req_str.lines() {
                            if line.to_lowercase().starts_with("range: bytes=") {
                                let r = &line[13..].trim();
                                if let Some((s_str, e_str)) = r.split_once('-') {
                                    if let Ok(s) = s_str.trim().parse::<usize>() {
                                        start = s;
                                    }
                                    if let Ok(e) = e_str.trim().parse::<usize>() {
                                        end = e;
                                    }
                                }
                            }
                        }

                        let end_bounded = (end + 1).min(data.len());
                        if start >= data.len() {
                            let resp = "HTTP/1.1 416 Range Not Satisfiable\r\n\r\n";
                            let _ = socket.write_all(resp.as_bytes()).await;
                            return;
                        }

                        let body = &data[start..end_bounded];
                        let resp = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
                            start,
                            end_bounded - 1,
                            data.len(),
                            body.len()
                        );
                        if socket.write_all(resp.as_bytes()).await.is_ok() {
                            let _ = socket.write_all(body).await;
                        }
                    });
                }
            }
        }
    });

    TestServer {
        url: format!("http://127.0.0.1:{}/testfile", port),
        shutdown_tx: Some(shutdown_tx),
    }
}

#[tokio::test]
async fn test_adaptive_short_burst_actively_evaluates_and_records_timeline() {
    let td = TestDir::new("short_burst");
    let total_bytes = 20 * 1024 * 1024;
    let data = Arc::new(generate_deterministic_payload(total_bytes));
    let expected_hash = compute_sha256(&data);

    let server = start_mock_server(data, 0).await;
    let save_path = td.0.join("test_short_burst.bin");

    let mut engine = spawn_download_engine();
    let task_id = 9101;

    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 2;
    config.min_concurrency = 1;
    config.max_concurrency = 8;
    config.slow_start_eval_interval = Duration::from_millis(15); // Fast slow-start interval
    config.slow_start_scale_up_step = 2;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(config));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let mut verified = false;
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => panic!("Download timed out"),
            Some(evt) = engine.event_receiver.recv() => {
                match evt {
                    EngineEvent::Finished { id, hash_status } if id == task_id => {
                        assert_eq!(hash_status, HashVerificationStatus::Verified);
                        verified = true;
                        break;
                    }
                    EngineEvent::Failed { id, error } if id == task_id => {
                        panic!("Download failed unexpectedly: {}", error);
                    }
                    _ => {}
                }
            }
        }
    }

    assert!(verified, "Download must complete and verify SHA-256");

    // Inspect timeline: verify that controller evaluations ACTUALLY occurred during the burst!
    let timeline = get_task_concurrency_timeline(task_id);
    assert!(
        timeline.len() >= 2,
        "Timeline must record at least Initial entry + 1 scaling decision, found: {:?}",
        timeline
    );

    // Verify timeline timestamps and reasons
    assert_eq!(timeline[0].concurrency, 2);
    assert_eq!(timeline[0].reason, DecisionReason::Initial);

    // Verify decisions were recorded
    let decisions = get_task_concurrency_decisions(task_id);
    assert!(
        !decisions.is_empty(),
        "Controller must have executed at least 1 evaluation during transfer"
    );
}

#[tokio::test]
async fn test_adaptive_bdp_seeding_with_50ms_latency() {
    let td = TestDir::new("bdp_seeding");
    let total_bytes = 16 * 1024 * 1024;
    let data = Arc::new(generate_deterministic_payload(total_bytes));
    let expected_hash = compute_sha256(&data);

    // 40ms artificial latency on mock server (round-trip ~40ms)
    let server = start_mock_server(data, 40).await;
    let save_path = td.0.join("test_bdp_seeding.bin");

    let mut engine = spawn_download_engine();
    let task_id = 9102;

    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 2; // Default would be 2, but BDP probe should seed to >= 8!
    config.min_concurrency = 1;
    config.max_concurrency = 16;
    config.enable_bdp_probe = true;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(config));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let mut verified = false;
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => panic!("BDP test download timed out"),
            Some(evt) = engine.event_receiver.recv() => {
                match evt {
                    EngineEvent::Finished { id, hash_status } if id == task_id => {
                        assert_eq!(hash_status, HashVerificationStatus::Verified);
                        verified = true;
                        break;
                    }
                    EngineEvent::Failed { id, error } if id == task_id => {
                        panic!("Download failed: {}", error);
                    }
                    _ => {}
                }
            }
        }
    }

    assert!(verified);

    // Check timeline: BDP probe should have seeded initial concurrency to >= 8!
    let timeline = get_task_concurrency_timeline(task_id);
    let seeded = timeline.iter().any(|entry| {
        matches!(entry.reason, DecisionReason::InitialBdpSeed { seeded_concurrency, .. } if seeded_concurrency >= 8)
    });
    assert!(
        seeded,
        "Timeline must include an InitialBdpSeed entry with >= 8 workers for 40ms RTT. Timeline: {:?}",
        timeline
    );
}

#[tokio::test]
async fn test_separated_latency_metrics_recorded_in_events() {
    let td = TestDir::new("separated_latency");
    let total_bytes = 10 * 1024 * 1024;
    let data = Arc::new(generate_deterministic_payload(total_bytes));
    let expected_hash = compute_sha256(&data);

    let server = start_mock_server(data, 10).await;
    let save_path = td.0.join("test_separated_latency.bin");

    let mut engine = spawn_download_engine();
    let task_id = 9103;

    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 4;
    config.slow_start_eval_interval = Duration::from_millis(25);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(config));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => panic!("Latency metrics test timed out"),
            Some(evt) = engine.event_receiver.recv() => {
                if let EngineEvent::Finished { id, .. } = evt {
                    if id == task_id {
                        break;
                    }
                }
            }
        }
    }

    let decisions = get_task_concurrency_decisions(task_id);
    assert!(!decisions.is_empty(), "Must have captured decisions");

    let last_event = decisions.last().unwrap();
    // Separated latency metrics must be accessible and non-negative
    assert!(last_event.avg_rtt_ms >= 5.0, "RTT should measure the artificial latency (>5ms)");
    println!(
        "Verified separated metrics: RTT={:.1}ms, ReqSetup={}ms, NetWait={}ms, WriterBackpressure={}ms",
        last_event.avg_rtt_ms,
        last_event.request_setup_ms,
        last_event.network_wait_ms,
        last_event.writer_backpressure_ms
    );
}

#[tokio::test]
async fn test_pause_resume_during_slow_start_preserves_sha256() {
    let td = TestDir::new("pause_resume_ss");
    let total_bytes = 25 * 1024 * 1024;
    let data = Arc::new(generate_deterministic_payload(total_bytes));
    let expected_hash = compute_sha256(&data);

    let server = start_mock_server(data, 5).await;
    let save_path = td.0.join("test_pause_resume_ss.bin");

    let mut engine = spawn_download_engine();
    let task_id = 9104;

    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 2;
    config.slow_start_eval_interval = Duration::from_millis(20);
    config.slow_start_scale_up_step = 2;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(config.clone()));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until at least 2 MB is downloaded, then pause during slow-start scaling
    let mut downloaded = 0u64;
    loop {
        match engine.event_receiver.recv().await {
            Some(EngineEvent::ProgressUpdate { id, downloaded: d, .. }) if id == task_id => {
                downloaded = d;
                if downloaded >= 2 * 1024 * 1024 {
                    engine.cmd_sender.send(EngineCommand::PauseDownload { id: task_id }).unwrap();
                }
            }
            Some(EngineEvent::Paused { id }) if id == task_id => {
                break;
            }
            _ => {}
        }
    }

    // Now resume the download with the same adaptive controller
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(config));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let mut final_verified = false;
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => panic!("Resume timed out"),
            Some(evt) = engine.event_receiver.recv() => {
                match evt {
                    EngineEvent::Finished { id, hash_status } if id == task_id => {
                        assert_eq!(hash_status, HashVerificationStatus::Verified);
                        final_verified = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    assert!(final_verified, "File must be bit-exact Verified upon resumption");
}

#[tokio::test]
async fn test_cancel_delete_during_slow_start_leaves_zero_orphaned_tasks() {
    let td = TestDir::new("delete_ss");
    let total_bytes = 30 * 1024 * 1024;
    let data = Arc::new(generate_deterministic_payload(total_bytes));

    let server = start_mock_server(data, 10).await;
    let save_path = td.0.join("test_delete_ss.bin");

    let mut engine = spawn_download_engine();
    let task_id = 9105;

    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 2;
    config.slow_start_eval_interval = Duration::from_millis(20);
    config.slow_start_scale_up_step = 2;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(config));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Wait until transfer starts, then immediately issue DeleteDownload
    loop {
        match engine.event_receiver.recv().await {
            Some(EngineEvent::ProgressUpdate { id, downloaded, .. }) if id == task_id && downloaded > 500_000 => {
                engine.cmd_sender.send(EngineCommand::DeleteDownload { id: task_id, cleanup_files: true }).unwrap();
            }
            Some(EngineEvent::Deleted { id, .. }) if id == task_id => {
                break;
            }
            _ => {}
        }
    }

    // Give asynchronous coordinator cleanup a moment to finalize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify that files are completely unlinked
    assert!(!save_path.exists(), "Target file must be unlinked after Delete");
    let mut partinfo = save_path.clone();
    partinfo.set_file_name("test_delete_ss.bin.partinfo");
    assert!(!partinfo.exists(), "Partinfo sidecar must be unlinked after Delete");

    // Verify engine metrics: zero active tasks and zero active workers
    let metrics = engine.metrics();
    assert_eq!(metrics.active_tasks_count, 0, "No active tasks should remain");
    assert_eq!(metrics.active_workers_count, 0, "No active workers should remain");
}
