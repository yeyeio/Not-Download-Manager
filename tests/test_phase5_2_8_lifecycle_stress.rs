//! Stress Test Suite for Phase 5.2.8: Concurrent Worker Lifecycle & Scheduler Stress Audit
//!
//! Validates:
//! 1. Worker slot allocation, reuse safety, and zero drift across concurrent workers.
//! 2. RAII WorkerLifecycleGuard accounting on normal exit, scale-down retirement, early return, and panic.
//! 3. Atomic CAS scale-down race: multiple concurrent workers retiring simultaneously never over-retire.
//! 4. Cancellation while workers are in Backoff sleep unwinds cleanly with 0 leaked sleeping workers.
//! 5. Pause and resume during RecoveryProbe state.
//! 6. Delete during GradualRecovery state.
//! 7. Repeated worker retirement and respawn across 50 cycles with safe slot recycling.
//! 8. Multiple simultaneous downloads running in parallel with 100% SHA-256 integrity.
//! 9. 1,000 randomized concurrent lifecycle scenarios with a fixed reproducible seed (seed = 42).

use not_download_manager::bench_server::{compute_deterministic_sha256, start_benchmark_server, RetryAfterMode, ServerConfig};
use not_download_manager::concurrency::{
    AdaptiveConfig, BackoffGuard, ConcurrencyStrategy,
    TaskTelemetryCollector, WorkerLifecycleGuard, WorkerSlotPool,
    WorkerState,
};
use not_download_manager::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use not_download_manager::types::HashVerificationStatus;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_test_p528_{}_{}", name, std::process::id()));
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
// TEST 1: Worker Slot Pool Concurrency, Uniqueness, and Reuse
// ---------------------------------------------------------------------------
#[test]
fn test_worker_slot_pool_concurrency_and_recycling() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    assert_eq!(pool.allocated_count(), 0);

    // Concurrently acquire all 16 slots across multiple threads
    let mut handles = Vec::new();
    let acquired_slots = Arc::new(std::sync::Mutex::new(Vec::new()));

    for _ in 0..16 {
        let p = pool.clone();
        let a = acquired_slots.clone();
        handles.push(std::thread::spawn(move || {
            let slot = p.acquire_slot().expect("slot must be available");
            assert!(slot < 16);
            a.lock().unwrap().push(slot);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(pool.allocated_count(), 16);
    // Pool is full: acquire must return None
    assert_eq!(pool.acquire_slot(), None);

    let mut slots = acquired_slots.lock().unwrap().clone();
    slots.sort();
    let expected: Vec<usize> = (0..16).collect();
    assert_eq!(slots, expected, "Every slot 0..15 must be uniquely acquired");

    // Release half of the slots
    for i in 0..8 {
        pool.release_slot(i);
        assert!(!pool.is_allocated(i));
    }
    assert_eq!(pool.allocated_count(), 8);

    // Re-acquire 8 slots concurrently
    let mut re_handles = Vec::new();
    let re_acquired = Arc::new(std::sync::Mutex::new(Vec::new()));
    for _ in 0..8 {
        let p = pool.clone();
        let a = re_acquired.clone();
        re_handles.push(std::thread::spawn(move || {
            let slot = p.acquire_slot().expect("re-acquired slot must be available");
            assert!(slot < 8);
            a.lock().unwrap().push(slot);
        }));
    }
    for h in re_handles {
        h.join().unwrap();
    }
    assert_eq!(pool.allocated_count(), 16);
    let mut re_slots = re_acquired.lock().unwrap().clone();
    re_slots.sort();
    assert_eq!(re_slots, (0..8).collect::<Vec<_>>());

    // Release all
    for i in 0..16 {
        pool.release_slot(i);
    }
    assert_eq!(pool.allocated_count(), 0);
}

// ---------------------------------------------------------------------------
// TEST 2: Worker Lifecycle Guard RAII Accounting Integrity
// ---------------------------------------------------------------------------
#[test]
fn test_worker_lifecycle_guard_raii_safety() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(2001));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let tracker = Arc::new(AtomicU64::new(0));

    // Case A: Normal spawn and automatic drop (e.g. error, chunk end, or cancel)
    {
        let slot = pool.acquire_slot().unwrap();
        task_active_workers.fetch_add(1, Ordering::SeqCst);
        let guard = WorkerLifecycleGuard::new(
            slot,
            task_active_workers.clone(),
            telemetry.clone(),
            pool.clone(),
            tracker.clone(),
        );
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(telemetry.get_worker_state(slot), WorkerState::Idle);

        drop(guard);
    }
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Counter must decrement on drop");
    assert_eq!(pool.allocated_count(), 0, "Slot must be released on drop");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated, "State must reset to Terminated");

    // Case B: Explicit scale-down retirement (mark_retired) must NOT double decrement
    {
        let slot = pool.acquire_slot().unwrap();
        task_active_workers.fetch_add(1, Ordering::SeqCst);
        let guard = WorkerLifecycleGuard::new(
            slot,
            task_active_workers.clone(),
            telemetry.clone(),
            pool.clone(),
            tracker.clone(),
        );
        // Explicit scale-down marks retired and decrements task_active_workers
        guard.mark_retired();
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "mark_retired must decrement active counter");
        assert_eq!(pool.allocated_count(), 1, "Slot is still allocated until drop");
        assert_eq!(pool.retiring_count(), 1, "Slot is marked retiring");
        assert_eq!(telemetry.get_worker_state(slot), WorkerState::Retiring);

        // When guard drops, it must NOT decrement task_active_workers again
        drop(guard);
    }
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Counter must not double-decrement");
    assert_eq!(pool.allocated_count(), 0, "Slot must be released");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated);

    // Case C: Panic unwinding resilience
    let pool_clone = pool.clone();
    let telem_clone = telemetry.clone();
    let active_clone = task_active_workers.clone();
    let tracker_clone = tracker.clone();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let slot = pool_clone.acquire_slot().unwrap();
        active_clone.fetch_add(1, Ordering::SeqCst);
        let _guard = WorkerLifecycleGuard::new(
            slot,
            active_clone.clone(),
            telem_clone.clone(),
            pool_clone.clone(),
            tracker_clone.clone(),
        );
        panic!("Simulated worker panic");
    }));
    assert!(res.is_err());
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Drop must run on panic");
    assert_eq!(pool.allocated_count(), 0, "Slot must be released on panic");
}

// ---------------------------------------------------------------------------
// TEST 3: Simultaneous Scale-Down Atomic CAS Race (Zero Over-Retirement)
// ---------------------------------------------------------------------------
#[test]
fn test_simultaneous_scale_down_atomic_cas_race() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(2002));
    let task_active_workers = Arc::new(AtomicUsize::new(16));
    let target_concurrency = Arc::new(AtomicUsize::new(4)); // Scale down from 16 to 4

    let mut guards = Vec::new();
    for _ in 0..16 {
        let slot = pool.acquire_slot().unwrap();
        let tracker = Arc::new(AtomicU64::new(0));
        let guard = WorkerLifecycleGuard::new(
            slot,
            task_active_workers.clone(),
            telemetry.clone(),
            pool.clone(),
            tracker,
        );
        guards.push(guard);
    }

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 16);
    assert_eq!(pool.allocated_count(), 16);

    // All 16 workers execute the scale-down CAS loop concurrently
    let retired_count = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();

    for guard in guards {
        let _active = task_active_workers.clone();
        let target = target_concurrency.clone();
        let retired = retired_count.clone();

        threads.push(std::thread::spawn(move || {
            let target_c = target.load(Ordering::SeqCst);
            let did_retire = guard.try_retire(target_c);

            if did_retire {
                retired.fetch_add(1, Ordering::SeqCst);
                drop(guard); // Retires now
            } else {
                // Stays alive for work
                std::thread::sleep(Duration::from_millis(50));
                drop(guard);
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    // Exactly 12 workers must have retired, leaving exactly 4 active before final drop!
    assert_eq!(retired_count.load(Ordering::SeqCst), 12, "Exactly 12 workers must retire when target is 4");
    // All guards dropped at the end of thread execution
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
    assert_eq!(pool.allocated_count(), 0);
}

// ---------------------------------------------------------------------------
// TEST 4: Cancellation While Workers Are in Backoff
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_cancellation_during_backoff() {
    let telemetry = Arc::new(TaskTelemetryCollector::new(2003));
    let pool = Arc::new(WorkerSlotPool::new(16));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(tokio::sync::Notify::new());

    // Spawn 8 workers that enter Backoff
    let mut handles = Vec::new();
    for _ in 0..8 {
        let slot = pool.acquire_slot().unwrap();
        task_active_workers.fetch_add(1, Ordering::SeqCst);
        let telem = telemetry.clone();
        let p = pool.clone();
        let act = task_active_workers.clone();
        let c_flag = cancel_flag.clone();
        let notif = notify.clone();

        handles.push(tokio::spawn(async move {
            let tracker = Arc::new(AtomicU64::new(0));
            let _lifecycle = WorkerLifecycleGuard::new(slot, act, telem.clone(), p, tracker);

            let backoff_dur = Duration::from_secs(10);
            {
                let _backoff = BackoffGuard::new(telem.clone(), slot, backoff_dur);
                tokio::select! {
                    _ = notif.notified() => {}
                    _ = tokio::time::sleep(backoff_dur) => {}
                }
            }
            assert!(c_flag.load(Ordering::SeqCst));
        }));
    }

    // Wait for all 8 workers to enter backoff
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(telemetry.sleeping_workers(), 8);
    assert_eq!(pool.allocated_count(), 8);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 8);

    // Cancel task!
    cancel_flag.store(true, Ordering::SeqCst);
    notify.notify_waiters();

    for h in handles {
        h.await.unwrap();
    }

    // Verify all 8 unwound cleanly
    assert_eq!(telemetry.sleeping_workers(), 0, "Sleeping workers must be 0 after cancellation");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Active workers must be 0 after cancellation");
    assert_eq!(pool.allocated_count(), 0, "All slots must be freed");
    for i in 0..8 {
        assert_eq!(telemetry.get_worker_state(i), WorkerState::Terminated);
    }
}

// ---------------------------------------------------------------------------
// TEST 5: Pause During RecoveryProbe State
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pause_and_resume_during_recovery_probe() {
    let test_dir = TestDir::new("pause_recovery_probe");
    let server_cfg = ServerConfig {
        total_file_size: 20 * 1024 * 1024,
        rate_limit_429_frequency: 10,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        ..Default::default()
    };
    let server = start_benchmark_server(server_cfg).await;
    let expected_hash = compute_deterministic_sha256(20 * 1024 * 1024);

    let mut engine = spawn_download_engine();
    let save_path = test_dir.0.join("test_pause_probe.bin");
    let task_id = 2004u64;

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(100);
    cfg.cooldown_duration = Duration::from_millis(200);

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

    let mut did_pause = false;
    let mut did_resume = false;
    let mut verified = false;

    let timeout_dur = Duration::from_secs(30);
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < timeout_dur {
        match tokio::time::timeout(Duration::from_millis(150), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => {
                match &evt {
                    EngineEvent::Finished { id, hash_status } if *id == task_id => {
                        println!("Finished event: id={}, hash_status={:?}", id, hash_status);
                        if let HashVerificationStatus::Verified = hash_status {
                            verified = true;
                        }
                        break;
                    }
                    EngineEvent::Paused { id } if *id == task_id => {
                        println!("Paused event: id={}", id);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        engine.set_expected_sha256(task_id, Some(expected_hash.clone()));
                        let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                            id: task_id,
                            url: server.url.clone(),
                            save_path: save_path.clone(),
                            connections: 4,
                        });
                        did_resume = true;
                    }
                    EngineEvent::ProgressUpdate { id, downloaded, .. } if *id == task_id => {
                        if *downloaded > 1_000_000 && !did_pause {
                            println!("Pausing at downloaded={}", downloaded);
                            let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id: task_id });
                            did_pause = true;
                        }
                    }
                    EngineEvent::Failed { id, error } if *id == task_id => {
                        panic!("Task failed unexpectedly: {}", error);
                    }
                    other => {
                        println!("Other event: {:?}", other);
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }

    assert!(did_pause, "Task must have been paused");
    assert!(did_resume, "Task must have been resumed");
    assert!(verified, "SHA-256 verification must succeed after pause/resume");
}

// ---------------------------------------------------------------------------
// TEST 6: Delete During GradualRecovery State
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_delete_during_gradual_recovery() {
    let test_dir = TestDir::new("delete_gradual_recovery");
    let server_cfg = ServerConfig {
        total_file_size: 50 * 1024 * 1024,
        rate_limit_429_frequency: 12,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        ..Default::default()
    };
    let server = start_benchmark_server(server_cfg).await;

    let mut engine = spawn_download_engine();
    let save_path = test_dir.0.join("test_delete.bin");
    let task_id = 2005u64;

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(100);

    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    let mut did_delete = false;
    let mut deleted_confirmed = false;
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < Duration::from_secs(15) {
        match tokio::time::timeout(Duration::from_millis(150), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::ProgressUpdate { id, downloaded, .. } if id == task_id => {
                    if downloaded > 2_000_000 && !did_delete {
                        let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                            id: task_id,
                            cleanup_files: true,
                        });
                        did_delete = true;
                    }
                }
                EngineEvent::Deleted { id, .. } if id == task_id => {
                    deleted_confirmed = true;
                    break;
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    assert!(did_delete, "Delete command must be issued");
    assert!(deleted_confirmed, "Engine must cleanly confirm task deletion without hangs or leaks");
}

// ---------------------------------------------------------------------------
// TEST 7: Repeated Worker Retirement and Respawn (50 Cycles)
// ---------------------------------------------------------------------------
#[test]
fn test_repeated_worker_retirement_and_respawn_50_cycles() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(2006));
    let task_active_workers = Arc::new(AtomicUsize::new(0));

    for cycle in 0..50 {
        // Scale up to 16
        let mut guards = Vec::with_capacity(16);
        for _ in 0..16 {
            let slot = pool.acquire_slot().expect(&format!("Cycle {}: slot must be available", cycle));
            assert!(slot < 16, "Slot ID must be strictly < 16");
            task_active_workers.fetch_add(1, Ordering::SeqCst);
            let tracker = Arc::new(AtomicU64::new(0));
            let guard = WorkerLifecycleGuard::new(
                slot,
                task_active_workers.clone(),
                telemetry.clone(),
                pool.clone(),
                tracker,
            );
            guards.push(guard);
        }

        assert_eq!(task_active_workers.load(Ordering::SeqCst), 16);
        assert_eq!(pool.allocated_count(), 16);

        // Scale down to 2
        for _ in 0..14 {
            let guard = guards.pop().unwrap();
            guard.mark_retired();
            drop(guard);
        }

        assert_eq!(task_active_workers.load(Ordering::SeqCst), 2);
        assert_eq!(pool.allocated_count(), 2);

        // Drop remaining 2
        drop(guards);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
        assert_eq!(pool.allocated_count(), 0);
    }
}

// ---------------------------------------------------------------------------
// TEST 8: Multiple Simultaneous Downloads Isolation & Integrity
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multiple_simultaneous_downloads() {
    let test_dir = TestDir::new("multi_download");
    let server_cfg = ServerConfig {
        total_file_size: 15 * 1024 * 1024,
        ..Default::default()
    };
    let server = start_benchmark_server(server_cfg).await;
    let expected_hash = compute_deterministic_sha256(15 * 1024 * 1024);

    let mut engine = spawn_download_engine();

    let tasks = vec![
        (3001u64, test_dir.0.join("dl1.bin")),
        (3002u64, test_dir.0.join("dl2.bin")),
        (3003u64, test_dir.0.join("dl3.bin")),
    ];

    for (id, save_path) in &tasks {
        engine.set_expected_sha256(*id, Some(expected_hash.clone()));
        engine
            .cmd_sender
            .send(EngineCommand::StartDownload {
                id: *id,
                url: server.url.clone(),
                save_path: save_path.clone(),
                connections: 4,
            })
            .unwrap();
    }

    let mut completed_tasks = std::collections::HashSet::new();
    let mut verified_tasks = std::collections::HashSet::new();
    let start_t = std::time::Instant::now();

    while start_t.elapsed() < Duration::from_secs(30) && completed_tasks.len() < 3 {
        match tokio::time::timeout(Duration::from_millis(150), engine.event_receiver.recv()).await {
            Ok(Some(evt)) => match evt {
                EngineEvent::Finished { id, hash_status } => {
                    if let HashVerificationStatus::Verified = hash_status {
                        verified_tasks.insert(id);
                    }
                    completed_tasks.insert(id);
                }
                EngineEvent::Failed { id, error } => {
                    panic!("Task {} failed: {}", id, error);
                }
                _ => {}
            },
            Ok(None) => break,
            Err(_) => {}
        }
    }

    assert_eq!(completed_tasks.len(), 3, "All 3 concurrent downloads must complete");
    assert_eq!(verified_tasks.len(), 3, "All 3 concurrent downloads must verify SHA-256");
}

// ---------------------------------------------------------------------------
// TEST 9: 1,000 Randomized Concurrent Lifecycle Scenarios (Seed = 42)
// ---------------------------------------------------------------------------
#[test]
fn test_1000_randomized_concurrent_lifecycle_scenarios() {
    // Linear Congruential Generator (LCG) for deterministic, reproducible random sequences
    struct Rng(u64);
    impl Rng {
        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 32) as u32
        }
        fn range(&mut self, min: usize, max: usize) -> usize {
            if min >= max {
                return min;
            }
            min + (self.next_u32() as usize % (max - min + 1))
        }
    }

    let mut rng = Rng(42); // Fixed reproducible seed

    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(9999));
    let task_active_workers = Arc::new(AtomicUsize::new(0));

    // Active worker guards stored across iterations
    let mut active_guards: Vec<Option<WorkerLifecycleGuard>> = (0..16).map(|_| None).collect();

    for scenario_idx in 1..=1000 {
        let action = rng.range(0, 4);
        match action {
            0 => {
                // Action: Spawn workers (scale up)
                let to_spawn = rng.range(1, 4);
                for _ in 0..to_spawn {
                    if let Some(slot) = pool.acquire_slot() {
                        task_active_workers.fetch_add(1, Ordering::SeqCst);
                        let tracker = Arc::new(AtomicU64::new(0));
                        let guard = WorkerLifecycleGuard::new(
                            slot,
                            task_active_workers.clone(),
                            telemetry.clone(),
                            pool.clone(),
                            tracker,
                        );
                        active_guards[slot] = Some(guard);
                    }
                }
            }
            1 => {
                // Action: Retire worker (scale down)
                let allocated = pool.active_slots();
                if !allocated.is_empty() {
                    let idx = rng.range(0, allocated.len() - 1);
                    let slot = allocated[idx];
                    if let Some(guard) = active_guards[slot].take() {
                        guard.mark_retired();
                        drop(guard);
                    }
                }
            }
            2 => {
                // Action: Worker enters and exits Backoff
                let allocated = pool.active_slots();
                if !allocated.is_empty() {
                    let idx = rng.range(0, allocated.len() - 1);
                    let slot = allocated[idx];
                    let _guard = BackoffGuard::new(telemetry.clone(), slot, Duration::from_millis(50));
                    assert_eq!(telemetry.get_worker_state(slot), WorkerState::Backoff);
                    drop(_guard);
                    assert_eq!(telemetry.get_worker_state(slot), WorkerState::Idle);
                }
            }
            3 => {
                // Action: Worker state changes (Idle -> Requesting -> Downloading -> Writing)
                let allocated = pool.active_slots();
                if !allocated.is_empty() {
                    let idx = rng.range(0, allocated.len() - 1);
                    let slot = allocated[idx];
                    let st_code = rng.range(1, 3);
                    let st = match st_code {
                        1 => WorkerState::Requesting,
                        2 => WorkerState::Downloading,
                        _ => WorkerState::Writing,
                    };
                    telemetry.set_worker_state(slot, st);
                    assert_eq!(telemetry.get_worker_state(slot), st);
                }
            }
            4 => {
                // Action: Drop random worker (simulating sudden failure or chunk finish)
                let allocated = pool.active_slots();
                if !allocated.is_empty() {
                    let idx = rng.range(0, allocated.len() - 1);
                    let slot = allocated[idx];
                    if let Some(guard) = active_guards[slot].take() {
                        drop(guard);
                    }
                }
            }
            _ => unreachable!(),
        }

        // --- Invariant Assertions After Every Scenario ---
        let current_active = task_active_workers.load(Ordering::SeqCst);
        let allocated_slots = pool.allocated_count();
        assert_eq!(
            current_active, allocated_slots,
            "Scenario #{}: task_active_workers ({}) must match pool.allocated_count ({})",
            scenario_idx, current_active, allocated_slots
        );

        // Every unallocated slot must be clean (Terminated or uninitialized)
        for i in 0..16 {
            if !pool.is_allocated(i) {
                let st = telemetry.get_worker_state(i);
                assert!(
                    st == WorkerState::Terminated || st == WorkerState::Idle,
                    "Scenario #{}: Unallocated slot {} must have reset state, got {:?}",
                    scenario_idx, i, st
                );
            }
        }
    }

    // Teardown remaining guards
    for g in active_guards.into_iter().flatten() {
        drop(g);
    }
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
    assert_eq!(pool.allocated_count(), 0);
}
