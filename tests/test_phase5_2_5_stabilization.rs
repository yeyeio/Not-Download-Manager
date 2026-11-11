//! Integration and Regression Test Suite for Phase 5.2.5: Adaptive Concurrency Stabilization Audit
//!
//! Validates:
//! 1. Windowed error rate calculation (eliminating false HighErrorRate scale-downs after recovery)
//! 2. AIMD hill-climbing plateau stability (preventing C=1 <-> C=2 cyclic oscillations)
//! 3. Full concurrency restoration back to initial pre-throttle baseline
//! 4. Synchronization of controller cooldown with server-mandated Retry-After
//! 5. Fast recovery under burst throttling with 100% bit-exact SHA-256 verification

use not_download_manager::concurrency::{
    get_task_concurrency_decisions, AdaptiveConfig, AdaptiveConcurrencyController,
    ConcurrencyStrategy, ControllerPhase, DecisionReason, LastAction, TaskTelemetryCollector,
};
use not_download_manager::db::compute_sha256;
use not_download_manager::engine::{
    spawn_download_engine, EngineCommand, EngineEvent,
};
use not_download_manager::types::HashVerificationStatus;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test Utilities
// ---------------------------------------------------------------------------

struct TestDir(pub PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_p525_{}_{}", name, std::process::id()));
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

fn generate_deterministic_payload(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state = 0x12345678u64;
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        data.push((state >> 33) as u8);
    }
    data
}



// ---------------------------------------------------------------------------
// UNIT TEST 1: Windowed Error Rate Prevents False HighErrorRate Trapping
// ---------------------------------------------------------------------------
#[test]
fn test_windowed_error_rate_prevents_false_high_error_rate() {
    let collector = TaskTelemetryCollector::new(501);

    // Interval 1: 5 requests, 3 transmission errors
    collector.record_retry_or_error(0);
    collector.record_retry_or_error(1);
    collector.record_retry_or_error(2);
    collector.record_rtt(0, Duration::from_millis(20));
    collector.record_rtt(1, Duration::from_millis(20));

    let snap1 = collector.take_snapshot(1_000_000, Some(10_000_000), 10, 4, 4);
    assert!(snap1.error_rate > 0.05, "Interval 1 must reflect active errors");
    assert_eq!(snap1.delta_retries, 3);

    // Interval 2: Clean traffic resumes: 10 successful requests, 0 errors
    for i in 0..10 {
        collector.record_rtt(i % 4, Duration::from_millis(15));
    }
    let snap2 = collector.take_snapshot(3_000_000, Some(10_000_000), 8, 4, 4);
    assert_eq!(snap2.delta_retries, 0, "Interval 2 must have 0 delta retries");
    assert_eq!(snap2.error_rate, 0.0, "Interval 2 error rate must reset to 0.0 despite lifetime errors");

    // Controller evaluation during Interval 2 must NOT scale down for HighErrorRate
    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 4;
    let mut ctrl = AdaptiveConcurrencyController::new(501, ConcurrencyStrategy::Adaptive(config));
    ctrl.phase = ControllerPhase::CongestionAvoidance;
    ctrl.throttle_cooldown_until = Instant::now() - Duration::from_millis(100);

    let evt = ctrl.evaluate(&snap2);
    assert!(!matches!(evt.reason, DecisionReason::HighErrorRate { .. }), "Clean window must not trigger HighErrorRate");
}

// ---------------------------------------------------------------------------
// UNIT TEST 2: AIMD Plateau Stability Prevents C=1 <-> C=2 Oscillation
// ---------------------------------------------------------------------------
#[test]
fn test_aimd_plateau_stability_prevents_oscillation() {
    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 2;
    config.cooldown_duration = Duration::ZERO;
    config.throughput_gain_threshold = 0.05; // 5%

    let mut ctrl = AdaptiveConcurrencyController::new(502, ConcurrencyStrategy::Adaptive(config));
    ctrl.phase = ControllerPhase::CongestionAvoidance;
    ctrl.current_concurrency = 2;
    ctrl.last_action = LastAction::ScaledUp;
    ctrl.last_action_throughput = 10_000_000.0;
    ctrl.smoothed_throughput = 10_100_000.0; // Only +1% gain (less than 5% threshold, but not a drop)

    let snap = collector_dummy_snap(2, 10_100_000.0);
    let evt = ctrl.evaluate(&snap);

    // Must lock steady at SteadyOptimal and NOT decrement back to 1
    assert_eq!(evt.target_concurrency, 2, "Plateau without drop must maintain concurrency at 2");
    assert!(matches!(evt.reason, DecisionReason::SteadyOptimal));
    assert_eq!(ctrl.last_action, LastAction::Maintained);
}

// ---------------------------------------------------------------------------
// UNIT TEST 3: Full Concurrency Restoration to Pre-Throttle Baseline
// ---------------------------------------------------------------------------
#[test]
fn test_full_concurrency_restoration_to_pre_throttle_baseline() {
    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 8;
    config.min_concurrency = 1;
    config.max_concurrency = 16;

    let mut ctrl = AdaptiveConcurrencyController::new(503, ConcurrencyStrategy::Adaptive(config));
    assert_eq!(ctrl.current_concurrency, 8);

    // Throttle burst cuts 8 -> 4
    let mut snap = collector_dummy_snap(8, 20_000_000.0);
    snap.delta_http_429 = 1;
    let evt1 = ctrl.evaluate(&snap);
    assert_eq!(evt1.target_concurrency, 4);
    assert_eq!(ctrl.pre_throttle_concurrency, 8, "Target baseline must be recorded as 8");

    // Advance past cooldown
    ctrl.throttle_cooldown_until = Instant::now() - Duration::from_millis(10);
    snap.delta_http_429 = 0;
    snap.delta_bytes = 2_000_000;

    // Gradual restoration steps +1 per clean window
    let evt2 = ctrl.evaluate(&snap);
    assert_eq!(evt2.target_concurrency, 5);
    let evt3 = ctrl.evaluate(&snap);
    assert_eq!(evt3.target_concurrency, 6);
    let evt4 = ctrl.evaluate(&snap);
    assert_eq!(evt4.target_concurrency, 7);
    let evt5 = ctrl.evaluate(&snap);
    assert_eq!(evt5.target_concurrency, 8, "Must fully restore all the way to pre-throttle baseline 8");
    assert_eq!(ctrl.pre_throttle_concurrency, 0, "Recovery complete");
    assert_eq!(ctrl.last_action, LastAction::Maintained, "Action must be Maintained to settle cleanly");
}

// ---------------------------------------------------------------------------
// UNIT TEST 4: Cooldown Hold During Active Server Throttle Window
// ---------------------------------------------------------------------------
#[test]
fn test_cooldown_hold_during_active_server_throttle_window() {
    let mut config = AdaptiveConfig::default();
    config.initial_concurrency = 4;
    let mut ctrl = AdaptiveConcurrencyController::new(504, ConcurrencyStrategy::Adaptive(config));

    // Throttled with 1000ms Retry-After
    let mut snap = collector_dummy_snap(4, 15_000_000.0);
    snap.delta_http_429 = 1;
    let deadline = Instant::now() + Duration::from_millis(1000);
    snap.server_throttle_until = Some(deadline);

    let evt1 = ctrl.evaluate(&snap);
    assert_eq!(evt1.target_concurrency, 2);
    assert!(ctrl.throttle_cooldown_until >= deadline);

    // During active cooldown, subsequent evaluation with delta_http_429 = 0 must HOLD steady
    snap.delta_http_429 = 0;
    let evt2 = ctrl.evaluate(&snap);
    assert_eq!(evt2.target_concurrency, 2);
    assert!(matches!(evt2.reason, DecisionReason::CooldownActive));
}

fn collector_dummy_snap(c: usize, tp: f64) -> not_download_manager::concurrency::TelemetrySnapshot {
    not_download_manager::concurrency::TelemetrySnapshot {
        timestamp: Instant::now(),
        aggregate_throughput_bps: tp,
        smoothed_throughput_bps: tp,
        active_concurrency: c,
        target_concurrency: c,
        avg_rtt: Duration::from_millis(10),
        total_idle_time: Duration::ZERO,
        total_request_setup_time: Duration::ZERO,
        total_network_wait_time: Duration::ZERO,
        total_writer_backpressure_time: Duration::ZERO,
        retry_count: 0,
        error_rate: 0.0,
        http_429_count: 0,
        http_503_count: 0,
        delta_http_429: 0,
        delta_http_503: 0,
        delta_retries: 0,
        delta_bytes: 1_000_000,
        server_throttle_until: None,
        sleeping_workers: 0,
        total_backoff_duration: Duration::ZERO,
        writer_queue_depth: 5,
        in_flight_writes: 5,
        progress_ratio: 0.5,
        remaining_chunks: 50,
    }
}

struct TestServer {
    pub url: String,
    pub shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[derive(Clone)]
enum ServerThrottleMode {
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
                        let _cur_req = req_count.fetch_add(1, Ordering::SeqCst) + 1;

                        if req_str.starts_with("HEAD") {
                            let head_resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test-etag-p525\"\r\n\r\n",
                                data.len()
                            );
                            let _ = socket.write_all(head_resp.as_bytes()).await;
                            return;
                        }

                        let should_throttle = match &mode {
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
        url: format!("http://127.0.0.1:{}/test.bin", port),
        shutdown_tx: Some(shutdown_tx),
    }
}

// ---------------------------------------------------------------------------
// INTEGRATION TEST 5: Fast Throttle Burst Recovery & 100% SHA-256
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_throttle_burst_fast_recovery_integration() {
    let test_dir = TestDir::new("fast_burst");
    let payload = Arc::new(generate_deterministic_payload(64 * 1024 * 1024)); // 64 MB
    let expected_hash = compute_sha256(&payload);

    // Mock server: throttle the first 4 requests with 429 Retry-After 0, then 100% 206 OK
    let server = start_throttling_mock_server(
        payload.clone(),
        ServerThrottleMode::TemporaryThrottleBurst { throttle_count: 4, delta_secs: 0 },
    )
    .await;

    let mut engine = spawn_download_engine();
    let task_id = 525_001;
    let save_path = test_dir.0.join("burst_recovery.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.evaluation_interval = Duration::from_millis(25);
    cfg.cooldown_duration = Duration::from_millis(50);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    let t0 = Instant::now();
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
    while t0.elapsed() < Duration::from_secs(10) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } if id == task_id => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
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

    let elapsed = t0.elapsed();
    println!("Elapsed duration: {:?}", elapsed);
    assert!(verified, "Download must finish and verify SHA-256");
    assert!(elapsed < Duration::from_secs(8), "Burst recovery must complete under stabilized controller (took {:?})", elapsed);

    let decisions = get_task_concurrency_decisions(task_id);
    for d in &decisions {
        println!("Decision: target={}, reason={:?}", d.target_concurrency, d.reason);
    }
    let recovery_decisions = decisions.iter().filter(|d| matches!(d.reason, DecisionReason::ServerThrottleRecovery { .. })).count();
    println!("Total recovery events: {}", recovery_decisions);
    assert!(recovery_decisions > 0, "Recovery state machine must trigger");
}
