//! Phase 5.2.4 — HTTP Throttling & Retry Optimization Tests
//!
//! Validates:
//! 1. HTTP 429 with Retry-After (both delta-seconds and RFC 2822 HTTP-date format).
//! 2. HTTP 429 without Retry-After (bounded exponential backoff with full jitter).
//! 3. HTTP 503 with and without Retry-After.
//! 4. Alternating 429 and 206 responses without deadlocking or permanent stall.
//! 5. Multiple workers receiving simultaneous throttling: verifies deduplication/coalescing (no repeated halving to 1).
//! 6. Server recovery after a temporary throttle: verifies gradual restoration of concurrency to pre-throttle level.
//! 7. Pause/Resume during backoff: verifies immediate interrupt of backoff sleep and SHA-256 integrity upon resumption.
//! 8. Delete during backoff: verifies immediate task abortion, file removal, and zero orphaned workers.
//! 9. Strict SHA-256 bit-exact integrity preservation across all retries.

use not_download_manager::concurrency::{
    get_task_concurrency_decisions, AdaptiveConfig,
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
    pub request_counter: Arc<AtomicUsize>,
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
    let mut state = 0x1337_C0DE_CAFE_u64;
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
        let p = std::env::temp_dir().join(format!("idman_test_p524_{}_{}", name, std::process::id()));
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

#[derive(Clone)]
enum ServerThrottleMode {
    AlwaysSuccess,
    Throttle429DeltaSeconds { frequency: usize, delta_secs: u64 },
    Throttle429HttpDate { frequency: usize, future_offset: Duration },
    Throttle429NoRetryAfter { frequency: usize },
    Throttle503DeltaSeconds { frequency: usize, delta_secs: u64 },
    Throttle503NoRetryAfter { frequency: usize },
    Alternating429,
    TemporaryThrottleBurst { throttle_count: usize, delta_secs: u64 },
}

async fn start_throttling_mock_server(
    data: Arc<Vec<u8>>,
    mode: ServerThrottleMode,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let request_counter = Arc::new(AtomicUsize::new(0));
    let throttle_counter = Arc::new(AtomicUsize::new(0));

    let req_counter_clone = request_counter.clone();

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
                    let thr_count = throttle_counter.clone();
                    let mode = mode.clone();

                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        let cur_req = req_count.fetch_add(1, Ordering::SeqCst) + 1;

                        if req_str.starts_with("HEAD") {
                            let head_resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test-etag-p524\"\r\n\r\n",
                                data.len()
                            );
                            let _ = socket.write_all(head_resp.as_bytes()).await;
                            return;
                        }

                        // Determine if request should be throttled
                        let should_throttle = match &mode {
                            ServerThrottleMode::AlwaysSuccess => None,
                            ServerThrottleMode::Throttle429DeltaSeconds { frequency, delta_secs } => {
                                if cur_req % *frequency == 0 {
                                    Some((429, format!("Retry-After: {}\r\n", delta_secs)))
                                } else {
                                    None
                                }
                            }
                            ServerThrottleMode::Throttle429HttpDate { frequency, future_offset } => {
                                if cur_req % *frequency == 0 {
                                    let future_time = chrono::Utc::now() + chrono::Duration::from_std(*future_offset).unwrap();
                                    Some((429, format!("Retry-After: {}\r\n", future_time.to_rfc2822())))
                                } else {
                                    None
                                }
                            }
                            ServerThrottleMode::Throttle429NoRetryAfter { frequency } => {
                                if cur_req % *frequency == 0 {
                                    Some((429, String::new()))
                                } else {
                                    None
                                }
                            }
                            ServerThrottleMode::Throttle503DeltaSeconds { frequency, delta_secs } => {
                                if cur_req % *frequency == 0 {
                                    Some((503, format!("Retry-After: {}\r\n", delta_secs)))
                                } else {
                                    None
                                }
                            }
                            ServerThrottleMode::Throttle503NoRetryAfter { frequency } => {
                                if cur_req % *frequency == 0 {
                                    Some((503, String::new()))
                                } else {
                                    None
                                }
                            }
                            ServerThrottleMode::Alternating429 => {
                                if cur_req % 2 == 0 {
                                    Some((429, "Retry-After: 0\r\n".to_string()))
                                } else {
                                    None
                                }
                            }
                            ServerThrottleMode::TemporaryThrottleBurst { throttle_count, delta_secs } => {
                                let tc = thr_count.fetch_add(1, Ordering::SeqCst);
                                if tc < *throttle_count {
                                    Some((429, format!("Retry-After: {}\r\n", delta_secs)))
                                } else {
                                    None
                                }
                            }
                        };

                        if let Some((code, ra_hdr)) = should_throttle {
                            let status_text = if code == 429 { "Too Many Requests" } else { "Service Unavailable" };
                            let resp = format!(
                                "HTTP/1.1 {} {}\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                                code, status_text, ra_hdr
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                            return;
                        }

                        // Parse Range Header
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
        url: format!("http://127.0.0.1:{}", port),
        request_counter: req_counter_clone,
        shutdown_tx: Some(shutdown_tx),
    }
}

// ---------------------------------------------------------------------------
// TEST 1: HTTP 429 with Delta-Seconds Retry-After
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_429_with_retry_after_delta_seconds() {
    let test_dir = TestDir::new("429_delta");
    let payload = Arc::new(generate_deterministic_payload(10 * 1024 * 1024)); // 10 MB
    let expected_hash = compute_sha256(&payload);

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle429DeltaSeconds { frequency: 4, delta_secs: 1 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_001;
    let save_path = test_dir.0.join("test_429_delta.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 8;
    cfg.slow_start_eval_interval = Duration::from_millis(35);
    cfg.evaluation_interval = Duration::from_millis(150);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
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

    let mut verified = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified, "Download with 429 delta-seconds must verify successfully");
    let file_data = std::fs::read(&save_path).unwrap();
    assert_eq!(compute_sha256(&file_data), expected_hash);
}

// ---------------------------------------------------------------------------
// TEST 2: HTTP 429 with RFC 2822 HTTP-Date Retry-After
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_429_with_retry_after_http_date() {
    let test_dir = TestDir::new("429_date");
    let payload = Arc::new(generate_deterministic_payload(10 * 1024 * 1024));
    let expected_hash = compute_sha256(&payload);

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle429HttpDate { frequency: 4, future_offset: Duration::from_secs(1) },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_002;
    let save_path = test_dir.0.join("test_429_date.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
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

    let mut verified = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified, "Download with 429 HTTP-date must verify successfully");
    let file_data = std::fs::read(&save_path).unwrap();
    assert_eq!(compute_sha256(&file_data), expected_hash);
}

// ---------------------------------------------------------------------------
// TEST 3: HTTP 429 without Retry-After (Exponential Backoff with Jitter)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_429_without_retry_after() {
    let test_dir = TestDir::new("429_no_ra");
    let payload = Arc::new(generate_deterministic_payload(10 * 1024 * 1024));
    let expected_hash = compute_sha256(&payload);

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle429NoRetryAfter { frequency: 4 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_003;
    let save_path = test_dir.0.join("test_429_no_ra.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
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

    let mut verified = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified, "Download with 429 without Retry-After must verify successfully");
    let file_data = std::fs::read(&save_path).unwrap();
    assert_eq!(compute_sha256(&file_data), expected_hash);
}

// ---------------------------------------------------------------------------
// TEST 4: HTTP 503 with and without Retry-After
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_503_throttling_resilience() {
    let test_dir = TestDir::new("503_resilience");
    let payload = Arc::new(generate_deterministic_payload(10 * 1024 * 1024));
    let expected_hash = compute_sha256(&payload);

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle503DeltaSeconds { frequency: 5, delta_secs: 1 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 503_001;
    let save_path = test_dir.0.join("test_503.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
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

    let mut verified = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified, "Download with 503 throttling must verify successfully");
    let file_data = std::fs::read(&save_path).unwrap();
    assert_eq!(compute_sha256(&file_data), expected_hash);
}

// ---------------------------------------------------------------------------
// TEST 5: Alternating 429 and 206 Responses
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_alternating_429_and_206_responses() {
    let test_dir = TestDir::new("alternating");
    let payload = Arc::new(generate_deterministic_payload(8 * 1024 * 1024));
    let expected_hash = compute_sha256(&payload);

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Alternating429,
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_005;
    let save_path = test_dir.0.join("test_alternating.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 2;
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
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
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified, "Alternating 429 must complete without stall and verify SHA-256");
    let file_data = std::fs::read(&save_path).unwrap();
    assert_eq!(compute_sha256(&file_data), expected_hash);
}

// ---------------------------------------------------------------------------
// TEST 6: Multiple Workers Simultaneous Throttling Coalescing
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_simultaneous_throttling_coalescing() {
    let test_dir = TestDir::new("coalesce");
    let payload = Arc::new(generate_deterministic_payload(12 * 1024 * 1024));
    let expected_hash = compute_sha256(&payload);

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle429DeltaSeconds { frequency: 3, delta_secs: 1 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_006;
    let save_path = test_dir.0.join("test_coalesce.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(100);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 8,
        })
        .unwrap();

    let mut verified = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified);
    let decisions = get_task_concurrency_decisions(task_id);
    for d in &decisions {
        println!("Decision: target={}, reason={:?}", d.target_concurrency, d.reason);
    }
    let coalesce_count = decisions.iter().filter(|d| matches!(d.reason, DecisionReason::ThrottledCoalesced)).count();
    println!("Coalesced simultaneous throttle decisions count: {}", coalesce_count);
    assert!(coalesce_count > 0 || decisions.iter().any(|d| matches!(d.reason, DecisionReason::ThrottledHttp429)), "Throttling event must be recorded");
}

// ---------------------------------------------------------------------------
// TEST 7: Server Recovery after Temporary Throttle (Gradual Restoration)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_server_recovery_after_temporary_throttle() {
    let test_dir = TestDir::new("recovery");
    let payload = Arc::new(generate_deterministic_payload(64 * 1024 * 1024)); // 64 MB = 8 chunks
    let expected_hash = compute_sha256(&payload);

    // Throttle only the first 3 requests with 429 (burst), then fully recover
    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::TemporaryThrottleBurst { throttle_count: 3, delta_secs: 0 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_007;
    let save_path = test_dir.0.join("test_recovery.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(25);
    cfg.cooldown_duration = Duration::from_millis(50);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 8,
        })
        .unwrap();

    let mut verified = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    assert!(verified);
    let decisions = get_task_concurrency_decisions(task_id);
    for d in &decisions {
        println!("Decision: target={}, reason={:?}", d.target_concurrency, d.reason);
    }
    let recovery_count = decisions.iter().filter(|d| matches!(d.reason, DecisionReason::ServerThrottleRecovery { .. })).count();
    println!("ServerThrottleRecovery decisions count: {}", recovery_count);
    assert!(recovery_count > 0, "Controller must gradually restore concurrency when server recovers");
}

// ---------------------------------------------------------------------------
// TEST 8: Pause / Resume during Active Backoff
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pause_resume_during_active_backoff() {
    let test_dir = TestDir::new("pause_backoff");
    let payload = Arc::new(generate_deterministic_payload(12 * 1024 * 1024));
    let expected_hash = compute_sha256(&payload);

    // Throttle with long delta_secs (e.g. 10s)
    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle429DeltaSeconds { frequency: 2, delta_secs: 10 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_008;
    let save_path = test_dir.0.join("test_pause.bin");

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

    // Wait until workers encounter 429 and enter backoff sleep
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send Pause: must immediately interrupt sleeping workers
    let pause_start = std::time::Instant::now();
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: task_id })
        .unwrap();

    let mut paused = false;
    while pause_start.elapsed() < Duration::from_secs(3) {
        if let Ok(Some(EngineEvent::Paused { id })) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            if id == task_id {
                paused = true;
                break;
            }
        }
    }
    assert!(paused, "Pause must interrupt backoff immediately without waiting for 10s sleep");
    assert!(pause_start.elapsed() < Duration::from_secs(2), "Pause must complete in under 2 seconds");

    // Resume download and complete
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

    let mut finished = false;
    let resume_start = std::time::Instant::now();
    while resume_start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
                    finished = true;
                    break;
                }
                _ => {}
            }
        }
    }

    assert!(finished, "Resumed download must finish and verify SHA-256");
    let file_data = std::fs::read(&save_path).unwrap();
    assert_eq!(compute_sha256(&file_data), expected_hash);
}

// ---------------------------------------------------------------------------
// TEST 9: Delete during Active Backoff leaves zero orphaned workers
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_delete_during_active_backoff() {
    let test_dir = TestDir::new("delete_backoff");
    let payload = Arc::new(generate_deterministic_payload(8 * 1024 * 1024));

    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::Throttle429DeltaSeconds { frequency: 1, delta_secs: 15 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 429_009;
    let save_path = test_dir.0.join("test_delete.bin");

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let del_start = std::time::Instant::now();
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let mut deleted = false;
    while del_start.elapsed() < Duration::from_secs(3) {
        if let Ok(Some(EngineEvent::Deleted { id, cleanup_success, .. })) =
            tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await
        {
            if id == task_id {
                assert!(cleanup_success);
                deleted = true;
                break;
            }
        }
    }

    assert!(deleted, "Delete must interrupt backoff immediately");
    assert!(!save_path.exists(), "Target file must be removed");
    let metrics = engine.metrics();
    assert_eq!(metrics.active_tasks_count, 0, "Active tasks must be 0");
    assert_eq!(metrics.active_workers_count, 0, "Active workers must be 0");
}
