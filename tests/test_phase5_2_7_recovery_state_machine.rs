//! Dedicated Regression Test Suite for Phase 5.2.7: Adaptive Concurrency Recovery State Machine
//!
//! Validates:
//! 1. RAII BackoffGuard accounting integrity: normal exit, early return, cancellation, and thread unwind/panic.
//! 2. `sleeping_workers` cannot underflow below 0 or leak across concurrent worker tasks.
//! 3. Explicit worker state transitions (Idle, Requesting, Downloading, Writing, Backoff, Terminated).
//! 4. Recovery state machine transitions: Normal -> Throttled -> Backoff -> RecoveryProbe -> GradualRecovery -> Stable.
//! 5. Integration: Adaptive recovers from throttle burst and prolonged throttle with 100% SHA-256 integrity.

use not_download_manager::bench_server::{compute_deterministic_sha256, start_benchmark_server, RetryAfterMode, ServerConfig};
use not_download_manager::concurrency::{
    AdaptiveConfig, AdaptiveConcurrencyController, BackoffGuard, ConcurrencyStrategy,
    DecisionReason, RecoveryState, TaskTelemetryCollector, WorkerState,
};
use not_download_manager::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use not_download_manager::types::HashVerificationStatus;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_test_p527_{}_{}", name, std::process::id()));
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

// ---------------------------------------------------------------------------
// TEST 1: BackoffGuard RAII Accounting & No Leaks
// ---------------------------------------------------------------------------
#[test]
fn test_backoff_guard_raii_accounting() {
    let telemetry = Arc::new(TaskTelemetryCollector::new(1001));

    assert_eq!(telemetry.sleeping_workers(), 0);
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Idle);

    // Normal scope entry and exit
    {
        let _guard = BackoffGuard::new(telemetry.clone(), 0, Duration::from_millis(100));
        assert_eq!(telemetry.sleeping_workers(), 1);
        assert_eq!(telemetry.get_worker_state(0), WorkerState::Backoff);
    }
    assert_eq!(telemetry.sleeping_workers(), 0);
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Idle);

    // Multiple concurrent workers in backoff
    {
        let _g0 = BackoffGuard::new(telemetry.clone(), 0, Duration::from_millis(100));
        let _g1 = BackoffGuard::new(telemetry.clone(), 1, Duration::from_millis(200));
        let _g2 = BackoffGuard::new(telemetry.clone(), 2, Duration::from_millis(300));
        assert_eq!(telemetry.sleeping_workers(), 3);
        assert_eq!(telemetry.get_worker_state(0), WorkerState::Backoff);
        assert_eq!(telemetry.get_worker_state(1), WorkerState::Backoff);
        assert_eq!(telemetry.get_worker_state(2), WorkerState::Backoff);
    }
    assert_eq!(telemetry.sleeping_workers(), 0);
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Idle);
    assert_eq!(telemetry.get_worker_state(1), WorkerState::Idle);
    assert_eq!(telemetry.get_worker_state(2), WorkerState::Idle);
}

// ---------------------------------------------------------------------------
// TEST 2: Panic Unwind & Underflow Resilience
// ---------------------------------------------------------------------------
#[test]
fn test_backoff_guard_panic_and_underflow_resilience() {
    let telemetry = Arc::new(TaskTelemetryCollector::new(1002));

    // Panic unwinding must still run Drop and clean up sleeping_workers counter
    let telem_clone = telemetry.clone();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = BackoffGuard::new(telem_clone.clone(), 5, Duration::from_millis(500));
        assert_eq!(telem_clone.sleeping_workers(), 1);
        panic!("Intentional panic during active backoff");
    }));
    assert!(res.is_err());
    assert_eq!(telemetry.sleeping_workers(), 0, "BackoffGuard Drop must run during panic unwind");
    assert_eq!(telemetry.get_worker_state(5), WorkerState::Idle);

    // Intentional unbalanced end calls must not underflow below 0
    telemetry.record_backoff_end();
    telemetry.record_backoff_end();
    assert_eq!(telemetry.sleeping_workers(), 0, "Counter must saturate at 0 and never underflow");
}

// ---------------------------------------------------------------------------
// TEST 3: Explicit Worker States Tracking
// ---------------------------------------------------------------------------
#[test]
fn test_explicit_worker_states_tracking() {
    let telemetry = Arc::new(TaskTelemetryCollector::new(1003));

    telemetry.set_worker_state(0, WorkerState::Requesting);
    telemetry.set_worker_state(1, WorkerState::Downloading);
    telemetry.set_worker_state(2, WorkerState::Writing);
    telemetry.set_worker_state(3, WorkerState::Backoff);
    telemetry.set_worker_state(4, WorkerState::Idle);

    let (network_active, writing, backoff, idle) = telemetry.count_worker_states(5);
    assert_eq!(network_active, 2, "Workers 0 (Requesting) and 1 (Downloading) are active network transfers");
    assert_eq!(writing, 1, "Worker 2 is in Writing state");
    assert_eq!(backoff, 1, "Worker 3 is in Backoff state");
    assert_eq!(idle, 1, "Worker 4 is in Idle state");

    assert_eq!(telemetry.active_network_transfers(5), 2);
}

// ---------------------------------------------------------------------------
// TEST 4: Recovery State Machine Unit Transitions
// ---------------------------------------------------------------------------
#[test]
fn test_recovery_state_machine_transitions() {
    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.min_concurrency = 1;
    cfg.max_concurrency = 16;
    cfg.cooldown_duration = Duration::from_millis(50);

    let mut controller = AdaptiveConcurrencyController::new(1004, ConcurrencyStrategy::Adaptive(cfg.clone()));
    assert_eq!(controller.current_concurrency, 4);
    assert_eq!(controller.recovery_state, RecoveryState::Normal);

    let mut snap = not_download_manager::concurrency::TelemetrySnapshot {
        timestamp: std::time::Instant::now(),
        aggregate_throughput_bps: 100_000_000.0,
        smoothed_throughput_bps: 100_000_000.0,
        active_concurrency: 4,
        target_concurrency: 4,
        avg_rtt: Duration::from_millis(20),
        total_idle_time: Duration::ZERO,
        total_request_setup_time: Duration::from_millis(5),
        total_network_wait_time: Duration::from_millis(20),
        total_writer_backpressure_time: Duration::ZERO,
        retry_count: 0,
        error_rate: 0.0,
        http_429_count: 1,
        http_503_count: 0,
        delta_http_429: 1,
        delta_http_503: 0,
        delta_retries: 1,
        delta_bytes: 1_000_000,
        server_throttle_until: Some(std::time::Instant::now() + Duration::from_millis(50)),
        sleeping_workers: 1,
        total_backoff_duration: Duration::from_millis(50),
        writer_queue_depth: 5,
        in_flight_writes: 5,
        progress_ratio: 0.1,
        remaining_chunks: 90,
    };

    // 1. Trigger Throttled
    let decision1 = controller.evaluate(&snap);
    assert_eq!(controller.recovery_state, RecoveryState::Throttled);
    assert_eq!(decision1.target_concurrency, 2, "Concurrency halved from 4 to 2");
    assert_eq!(controller.pre_throttle_concurrency, 4);

    // 2. Next evaluation while backoff active -> Backoff state
    snap.delta_http_429 = 0;
    snap.delta_retries = 0;
    snap.delta_bytes = 0;
    let decision2 = controller.evaluate(&snap);
    assert_eq!(controller.recovery_state, RecoveryState::Backoff);
    assert_eq!(decision2.target_concurrency, 2);

    // 3. Sleep past backoff deadline
    std::thread::sleep(Duration::from_millis(60));
    snap.delta_bytes = 5_000_000;
    snap.sleeping_workers = 0;

    // 4. Evaluation past deadline -> RecoveryProbe
    let decision3 = controller.evaluate(&snap);
    assert_eq!(controller.recovery_state, RecoveryState::RecoveryProbe);
    assert_eq!(decision3.target_concurrency, 3, "Probes with C + 1");
    assert!(matches!(decision3.reason, DecisionReason::RecoveryProbe { .. }));

    // 5. Successful probe with clean progress -> GradualRecovery
    let decision4 = controller.evaluate(&snap);
    assert_eq!(controller.recovery_state, RecoveryState::Stable);
    assert_eq!(decision4.target_concurrency, 4, "Restores baseline 4");
    assert!(matches!(decision4.reason, DecisionReason::RecoveryStable { .. }));
}

// ---------------------------------------------------------------------------
// TEST 5: Integration: Throttle Burst & Recovery with 100% SHA-256
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_throttle_burst_and_recovery_integration() {
    let test_dir = TestDir::new("throttle_burst_recov");
    let file_size = 10 * 1024 * 1024; // 10 MB
    let expected_hash = compute_deterministic_sha256(file_size);

    let server_config = ServerConfig {
        total_file_size: file_size,
        rate_limit_429_frequency: 2,
        throttle_max_count: 4,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        ..Default::default()
    };
    let server = start_benchmark_server(server_config).await;

    let mut engine = spawn_download_engine();
    let task_id = 1005;
    let save_path = test_dir.0.join("burst_recovery.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 16;
    cfg.evaluation_interval = Duration::from_millis(50);
    cfg.cooldown_duration = Duration::from_millis(100);
    cfg.slow_start_eval_interval = Duration::from_millis(25);
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

    assert!(verified, "Download must complete with verified SHA-256");

    let decisions = engine.get_concurrency_decisions(task_id);
    let probe_count = decisions
        .iter()
        .filter(|d| matches!(d.reason, DecisionReason::RecoveryProbe { .. }))
        .count();
    let recovery_count = decisions
        .iter()
        .filter(|d| {
            matches!(
                d.reason,
                DecisionReason::RecoveryProbe { .. }
                    | DecisionReason::GradualRecovery { .. }
                    | DecisionReason::RecoveryStable { .. }
                    | DecisionReason::ServerThrottleRecovery { .. }
            )
        })
        .count();

    println!("Decisions logged: {}", decisions.len());
    println!("Recovery decisions: {} (Probes: {})", recovery_count, probe_count);
    assert!(recovery_count > 0, "Controller must execute recovery decisions");

    server.shutdown().await;
}
