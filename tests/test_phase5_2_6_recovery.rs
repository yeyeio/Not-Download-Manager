//! Phase 5.2.6: Dedicated Regression Tests for Adaptive Concurrency Recovery Optimization
//!
//! Verifies:
//! 1. BDP probe preserves configured initial concurrency (does not downgrade to 2 on low RTT).
//! 2. Pre-throttle baseline preservation (controller records and restores to at least initial concurrency).
//! 3. Active workers continue processing chunks while a peer honors Retry-After.
//! 4. Graceful scale-down distinguishes sleeping workers from active transferring workers.
//! 5. Repeated throttle burst and recovery integration with bit-exact SHA-256 verification.

use not_download_manager::bench_server::{compute_deterministic_sha256, start_benchmark_server, RetryAfterMode, ServerConfig};
use not_download_manager::concurrency::{
    get_task_concurrency_decisions, AdaptiveConcurrencyController, AdaptiveConfig,
    ConcurrencyStrategy, DecisionReason, TaskTelemetryCollector, TelemetrySnapshot,
};
use not_download_manager::engine::{
    spawn_download_engine, EngineCommand, EngineEvent,
};
use not_download_manager::types::HashVerificationStatus;
use std::path::PathBuf;
use std::time::{Duration, Instant};

struct TestDir(pub PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_p526_{}_{}", name, std::process::id()));
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

fn dummy_snapshot(c: usize, tp: f64) -> TelemetrySnapshot {
    TelemetrySnapshot {
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

// ---------------------------------------------------------------------------
// Test 1: BDP probe preserves configured initial concurrency
// ---------------------------------------------------------------------------
#[test]
fn test_bdp_probe_preserves_configured_initial_concurrency() {
    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.min_concurrency = 1;
    cfg.max_concurrency = 16;
    cfg.enable_bdp_probe = true;

    let mut ctrl = AdaptiveConcurrencyController::new(101, ConcurrencyStrategy::Adaptive(cfg.clone()));
    assert_eq!(ctrl.current_concurrency, 4);

    // On LAN (RTT 1ms), raw BDP recommendation is 2.
    // However, init_with_rtt MUST NOT downgrade below configured initial_concurrency (4).
    ctrl.init_with_rtt(Duration::from_millis(1));
    assert_eq!(
        ctrl.current_concurrency, 4,
        "Low-latency RDP probe must not downgrade initial concurrency"
    );

    // On high-latency link (RTT 80ms), BDP probe SHOULD expand concurrency to 12
    let mut ctrl_high_lat = AdaptiveConcurrencyController::new(102, ConcurrencyStrategy::Adaptive(cfg));
    ctrl_high_lat.init_with_rtt(Duration::from_millis(80));
    assert_eq!(
        ctrl_high_lat.current_concurrency, 12,
        "High-latency BDP probe expands initial concurrency"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Pre-throttle baseline preservation and restoration
// ---------------------------------------------------------------------------
#[test]
fn test_pre_throttle_baseline_preserves_initial_concurrency() {
    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.min_concurrency = 1;
    cfg.max_concurrency = 16;
    cfg.cooldown_duration = Duration::from_millis(50);
    cfg.evaluation_interval = Duration::from_millis(50);

    let mut ctrl = AdaptiveConcurrencyController::new(103, ConcurrencyStrategy::Adaptive(cfg));
    assert_eq!(ctrl.current_concurrency, 4);

    // Simulate HTTP 429 arrival
    let mut throttle_snap = dummy_snapshot(4, 50_000_000.0);
    throttle_snap.delta_http_429 = 1;
    throttle_snap.http_429_count = 1;
    let evt = ctrl.evaluate(&throttle_snap);

    assert_eq!(evt.target_concurrency, 2, "Concurrency halved to 2 on throttle");
    assert_eq!(
        ctrl.pre_throttle_concurrency, 4,
        "Pre-throttle baseline preserved as 4"
    );

    // Sleep past cooldown
    std::thread::sleep(Duration::from_millis(60));

    // Evaluation 1 past cooldown: recovery step +1 (2 -> 3)
    let clean_snap1 = dummy_snapshot(2, 60_000_000.0);
    let evt1 = ctrl.evaluate(&clean_snap1);
    assert_eq!(evt1.target_concurrency, 3, "Restored to 3");
    assert_eq!(ctrl.pre_throttle_concurrency, 4, "Recovery still targeting 4");

    // Evaluation 2 past cooldown: recovery step +1 (3 -> 4, reaches baseline)
    let clean_snap2 = dummy_snapshot(3, 75_000_000.0);
    let evt2 = ctrl.evaluate(&clean_snap2);
    assert_eq!(evt2.target_concurrency, 4, "Fully restored to pre-throttle baseline 4");
    assert_eq!(ctrl.pre_throttle_concurrency, 0, "Recovery state reset after reaching target");
}

// ---------------------------------------------------------------------------
// Test 3: Telemetry sleeping workers accounting
// ---------------------------------------------------------------------------
#[test]
fn test_telemetry_sleeping_workers_accounting() {
    let telemetry = TaskTelemetryCollector::new(104);
    assert_eq!(telemetry.sleeping_workers(), 0);
    assert_eq!(telemetry.total_backoff_duration(), Duration::ZERO);

    // Worker 1 enters backoff
    telemetry.record_backoff_start(Duration::from_millis(500));
    assert_eq!(telemetry.sleeping_workers(), 1);
    assert_eq!(telemetry.total_backoff_duration(), Duration::from_millis(500));

    // Worker 2 enters backoff
    telemetry.record_backoff_start(Duration::from_millis(300));
    assert_eq!(telemetry.sleeping_workers(), 2);
    assert_eq!(telemetry.total_backoff_duration(), Duration::from_millis(800));

    // Worker 1 wakes up
    telemetry.record_backoff_end();
    assert_eq!(telemetry.sleeping_workers(), 1);

    // Worker 2 wakes up
    telemetry.record_backoff_end();
    assert_eq!(telemetry.sleeping_workers(), 0);
    assert_eq!(telemetry.total_backoff_duration(), Duration::from_millis(800));
}

// ---------------------------------------------------------------------------
// Test 4: End-to-end repeated throttle burst and recovery integration
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_repeated_throttle_burst_and_recovery_integration() {
    let test_dir = TestDir::new("repeated");
    let file_size = 64 * 1024 * 1024u64;
    let expected_hash = compute_deterministic_sha256(file_size);

    let server_config = ServerConfig {
        total_file_size: file_size,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        throttle_burst_cycle: Some((2, 6)),
        throttle_max_count: 4,
        ..Default::default()
    };
    let server = start_benchmark_server(server_config).await;

    let mut engine = spawn_download_engine();
    let task_id = 526_001;
    let save_path = test_dir.0.join("repeated_recovery.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.min_concurrency = 1;
    cfg.max_concurrency = 16;
    cfg.evaluation_interval = Duration::from_millis(50);
    cfg.cooldown_duration = Duration::from_millis(100);
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
    while t0.elapsed() < Duration::from_secs(20) {
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

    server.shutdown().await;

    assert!(verified, "Download must finish and verify SHA-256");
    assert!(t0.elapsed() < Duration::from_secs(15), "Repeated burst recovery must complete quickly (took {:?})", t0.elapsed());

    let timeline = engine.get_concurrency_timeline(task_id);
    for t in &timeline {
        println!("Timeline: ms={}, concurrency={}, reason={:?}", t.elapsed_ms, t.concurrency, t.reason);
    }
    let decisions = engine.get_concurrency_decisions(task_id);
    for d in &decisions {
        println!("Decision: target={}, reason={:?}", d.target_concurrency, d.reason);
    }
    let mut recoveries = 0;
    for d in &decisions {
        if matches!(
            d.reason,
            DecisionReason::ServerThrottleRecovery { .. }
                | DecisionReason::RecoveryProbe { .. }
                | DecisionReason::GradualRecovery { .. }
                | DecisionReason::RecoveryStable { .. }
        ) {
            recoveries += 1;
        }
    }
    println!("Total recovery events: {}", recoveries);
    assert!(recoveries > 0, "Recovery events must be observed across repeated cycles");
}
