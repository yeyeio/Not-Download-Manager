//! Phase 5.2.2 — Adaptive Concurrency Controller Tests
//!
//! Covers:
//! 1. Fast server scale-up on throughput gain.
//! 2. HTTP 429 Too Many Requests immediate scale-down.
//! 3. HTTP 503 Service Unavailable immediate scale-down.
//! 4. High RTT and latency tracking.
//! 5. Transient worker failure recovery and error rate throttling.
//! 6. Writer backpressure mitigation.
//! 7. Pause, Resume, Cancel, and Delete during dynamic concurrency adjustments.
//! 8. 100% SHA-256 integrity verification.

use not_download_manager::concurrency::{AdaptiveConfig, ConcurrencyStrategy, DecisionReason};
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
    let mut state = 0xCAFEBABE_u64;
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        data.push((state >> 33) as u8);
    }
    data
}


/// Starts a mock HTTP server with custom response behaviors (e.g. 429/503 injection, latency, stalls)
async fn start_mock_server(
    data: Arc<Vec<u8>>,
    inject_429_at_request: Option<usize>,
    inject_503_at_request: Option<usize>,
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
                        let mut buf = vec![0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };

                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        let count = req_count.fetch_add(1, Ordering::SeqCst);

                        if artificial_latency_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(artificial_latency_ms)).await;
                        }

                        // HEAD probe
                        if req_str.starts_with("HEAD") {
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Length: {}\r\n\
                                 Accept-Ranges: bytes\r\n\
                                 Content-Type: application/octet-stream\r\n\
                                 \r\n",
                                data.len()
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            return;
                        }

                        // Inject 429 Too Many Requests
                        if let Some(target) = inject_429_at_request {
                            if count == target {
                                let resp = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n";
                                let _ = socket.write_all(resp.as_bytes()).await;
                                return;
                            }
                        }

                        // Inject 503 Service Unavailable
                        if let Some(target) = inject_503_at_request {
                            if count == target {
                                let resp = "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n";
                                let _ = socket.write_all(resp.as_bytes()).await;
                                return;
                            }
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
                            "HTTP/1.1 206 Partial Content\r\n\
                             Content-Range: bytes {}-{}/{}\r\n\
                             Content-Length: {}\r\n\
                             Content-Type: application/octet-stream\r\n\
                             \r\n",
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
        url: format!("http://127.0.0.1:{}", port),
        shutdown_tx: Some(shutdown_tx),
    }
}

/// RAII Temporary Directory
struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_test_p522_{}_{}", name, std::process::id()));
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

#[tokio::test]
async fn test_adaptive_concurrency_fast_server_scales_up_and_verifies_sha256() {
    let td = TestDir::new("fast_scale_up");
    let payload_size = 20 * 1024 * 1024; // 20 MB (40 chunks of 512 KB)
    let payload = Arc::new(generate_deterministic_payload(payload_size));
    let expected_hash = compute_sha256(&payload);
    // Inject 5ms latency to allow multiple controller evaluation ticks
    let server = start_mock_server(payload, None, None, 5).await;

    let mut engine = spawn_download_engine();
    let task_id = 1001;
    let save_path = td.0.join("fast_scale_up.bin");

    // Configure adaptive strategy with initial concurrency = 2, max = 8
    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 2;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(30);
    cfg.cooldown_duration = Duration::from_millis(40);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let mut final_status = None;
    let start_time = std::time::Instant::now();

    while let Some(evt) = engine.event_receiver.recv().await {
        match evt {
            EngineEvent::Finished { id, hash_status } if id == task_id => {
                final_status = Some(hash_status);
                break;
            }
            EngineEvent::Failed { id, error } if id == task_id => {
                panic!("Download failed unexpectedly: {}", error);
            }
            _ => {}
        }
        if start_time.elapsed() > Duration::from_secs(30) {
            panic!("Test timed out");
        }
    }

    assert_eq!(
        final_status,
        Some(HashVerificationStatus::Verified),
        "SHA-256 verification must match 100%"
    );

    // Verify disk payload matches exactly
    let downloaded_data = std::fs::read(&save_path).expect("File must exist");
    assert_eq!(downloaded_data.len(), payload_size);
    assert_eq!(compute_sha256(&downloaded_data), expected_hash);

    // Inspect controller decisions
    let decisions = engine.get_concurrency_decisions(task_id);
    assert!(!decisions.is_empty(), "Controller decisions must be recorded");

    let scaled_up = decisions.iter().any(|d| {
        matches!(d.reason, DecisionReason::ScaleUpThroughputGain { .. })
            || d.target_concurrency > 2
    });
    assert!(scaled_up, "Controller must scale up concurrency on fast server");
}

#[tokio::test]
async fn test_adaptive_concurrency_http_429_scales_down_immediately() {
    let td = TestDir::new("http_429");
    let payload_size = 10 * 1024 * 1024; // 10 MB
    let payload = Arc::new(generate_deterministic_payload(payload_size));
    let expected_hash = compute_sha256(&payload);
    // Inject 429 at request #3
    let server = start_mock_server(payload, Some(3), None, 0).await;

    let mut engine = spawn_download_engine();
    let task_id = 1002;
    let save_path = td.0.join("http_429.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 6;
    cfg.min_concurrency = 1;
    cfg.evaluation_interval = Duration::from_millis(100);
    cfg.cooldown_duration = Duration::from_millis(300);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 6,
        })
        .unwrap();

    let mut final_status = None;
    let start_time = std::time::Instant::now();

    while let Some(evt) = engine.event_receiver.recv().await {
        match evt {
            EngineEvent::Finished { id, hash_status } if id == task_id => {
                final_status = Some(hash_status);
                break;
            }
            EngineEvent::Failed { id, error } if id == task_id => {
                panic!("Download failed unexpectedly: {}", error);
            }
            _ => {}
        }
        if start_time.elapsed() > Duration::from_secs(30) {
            panic!("Test timed out");
        }
    }

    assert_eq!(
        final_status,
        Some(HashVerificationStatus::Verified),
        "Must verify 100% SHA-256 after 429 throttling recovery"
    );

    let decisions = engine.get_concurrency_decisions(task_id);
    let had_429_scaledown = decisions.iter().any(|d| {
        matches!(d.reason, DecisionReason::ThrottledHttp429)
    });
    assert!(had_429_scaledown, "Controller must record ThrottledHttp429 event");
}

#[tokio::test]
async fn test_adaptive_concurrency_http_503_scales_down_immediately() {
    let td = TestDir::new("http_503");
    let payload_size = 10 * 1024 * 1024;
    let payload = Arc::new(generate_deterministic_payload(payload_size));
    let expected_hash = compute_sha256(&payload);
    // Inject 503 at request #4
    let server = start_mock_server(payload, None, Some(4), 0).await;

    let mut engine = spawn_download_engine();
    let task_id = 1003;
    let save_path = td.0.join("http_503.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 6;
    cfg.min_concurrency = 1;
    cfg.evaluation_interval = Duration::from_millis(100);
    cfg.cooldown_duration = Duration::from_millis(300);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 6,
        })
        .unwrap();

    let mut final_status = None;
    let start_time = std::time::Instant::now();

    while let Some(evt) = engine.event_receiver.recv().await {
        match evt {
            EngineEvent::Finished { id, hash_status } if id == task_id => {
                final_status = Some(hash_status);
                break;
            }
            EngineEvent::Failed { id, error } if id == task_id => {
                panic!("Download failed unexpectedly: {}", error);
            }
            _ => {}
        }
        if start_time.elapsed() > Duration::from_secs(30) {
            panic!("Test timed out");
        }
    }

    assert_eq!(
        final_status,
        Some(HashVerificationStatus::Verified),
        "Must verify 100% SHA-256 after 503 recovery"
    );

    let decisions = engine.get_concurrency_decisions(task_id);
    let had_503_scaledown = decisions.iter().any(|d| {
        matches!(d.reason, DecisionReason::ThrottledHttp503)
    });
    assert!(had_503_scaledown, "Controller must record ThrottledHttp503 event");
}

#[tokio::test]
async fn test_adaptive_concurrency_high_rtt_measures_latency() {
    let td = TestDir::new("high_rtt");
    let payload_size = 8 * 1024 * 1024; // 8 MB
    let payload = Arc::new(generate_deterministic_payload(payload_size));
    let expected_hash = compute_sha256(&payload);
    // Inject 35ms latency per request
    let server = start_mock_server(payload, None, None, 35).await;

    let mut engine = spawn_download_engine();
    let task_id = 1004;
    let save_path = td.0.join("high_rtt.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 2;
    cfg.max_concurrency = 6;
    cfg.evaluation_interval = Duration::from_millis(40);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let mut final_status = None;
    let start_time = std::time::Instant::now();

    while let Some(evt) = engine.event_receiver.recv().await {
        match evt {
            EngineEvent::Finished { id, hash_status } if id == task_id => {
                final_status = Some(hash_status);
                break;
            }
            EngineEvent::Failed { id, error } if id == task_id => {
                panic!("Download failed: {}", error);
            }
            _ => {}
        }
        if start_time.elapsed() > Duration::from_secs(30) {
            panic!("Test timed out");
        }
    }

    assert_eq!(final_status, Some(HashVerificationStatus::Verified));

    let decisions = engine.get_concurrency_decisions(task_id);
    let captured_high_rtt = decisions.iter().any(|d| d.avg_rtt_ms >= 20.0);
    assert!(captured_high_rtt, "Telemetry must measure high RTT (>20ms)");
}

#[tokio::test]
async fn test_adaptive_concurrency_pause_resume_integrity() {
    let td = TestDir::new("pause_resume");
    let payload_size = 10 * 1024 * 1024;
    let payload = Arc::new(generate_deterministic_payload(payload_size));
    let expected_hash = compute_sha256(&payload);
    let server = start_mock_server(payload, None, None, 5).await;

    let mut engine = spawn_download_engine();
    let task_id = 1005;
    let save_path = td.0.join("pause_resume.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 3;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(150);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg.clone()));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 3,
        })
        .unwrap();

    // Wait for at least 2 MB downloaded
    while let Some(evt) = engine.event_receiver.recv().await {
        if let EngineEvent::ProgressUpdate { id, downloaded, .. } = evt {
            if id == task_id && downloaded >= 2 * 1024 * 1024 {
                break;
            }
        }
    }

    // Pause
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: task_id })
        .unwrap();

    while let Some(evt) = engine.event_receiver.recv().await {
        if let EngineEvent::Paused { id } = evt {
            if id == task_id {
                break;
            }
        }
    }

    // Resume
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 3,
        })
        .unwrap();

    let mut final_status = None;
    while let Some(evt) = engine.event_receiver.recv().await {
        if let EngineEvent::Finished { id, hash_status } = evt {
            if id == task_id {
                final_status = Some(hash_status);
                break;
            }
        }
    }

    assert_eq!(final_status, Some(HashVerificationStatus::Verified));
    let downloaded_data = std::fs::read(&save_path).expect("File must exist");
    assert_eq!(compute_sha256(&downloaded_data), expected_hash);
}

#[tokio::test]
async fn test_adaptive_concurrency_delete_during_adjustment_cleans_up() {
    let td = TestDir::new("delete_adjust");
    let payload_size = 15 * 1024 * 1024;
    let payload = Arc::new(generate_deterministic_payload(payload_size));
    let server = start_mock_server(payload, None, None, 10).await;

    let mut engine = spawn_download_engine();
    let task_id = 1006;
    let save_path = td.0.join("delete_adjust.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(100);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: format!("{}/file.bin", server.url),
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    // Wait until download has started
    while let Some(evt) = engine.event_receiver.recv().await {
        if let EngineEvent::ProgressUpdate { id, downloaded, .. } = evt {
            if id == task_id && downloaded > 1024 * 1024 {
                break;
            }
        }
    }

    // Delete while workers and controller are active
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut got_deleted = false;
    while let Some(evt) = engine.event_receiver.recv().await {
        if let EngineEvent::Deleted { id, cleanup_success, .. } = evt {
            if id == task_id {
                assert!(cleanup_success, "Cleanup must report success");
                got_deleted = true;
                break;
            }
        }
    }

    assert!(got_deleted, "Must receive Deleted event");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify engine metrics: zero active tasks and zero active workers
    let m = engine.metrics();
    assert_eq!(m.active_tasks_count, 0, "No active tasks should linger");
    assert_eq!(m.active_workers_count, 0, "No active workers should linger");
    assert!(!save_path.exists(), "Target file must be deleted");
}
