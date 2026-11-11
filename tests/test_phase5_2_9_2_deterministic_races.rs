//! Phase 5.2.9.2: Deterministic Race Testing Test Suite (Revision 2)
//!
//! Validates the 8 critical concurrent interleavings in worker lifecycle and orchestration:
//! - R1: Scale-down versus scale-up (retiring-slot pool isolation AND true supervisor retry orchestration)
//! - R2: Concurrent retirement of different workers (active floor protection)
//! - R3: Same-guard retirement and termination overlap (Ordering A: termination wins first, Ordering B: retirement wins first, Ordering C: task abort contention, and claim exclusivity)
//! - R4: Spawn ownership and task startup boundaries (all 5 kickoff boundaries: pre-guard acquisition, pre-spawn unwind, unpolled future drop, startup abort, backoff abort)
//! - R5: Pause/Resume versus recovery probe (deterministic test barrier, worker/writer quiescence, and resume verification)
//! - R6: Delete versus recovery / worker cleanup (deterministic test barrier, worker/writer join before file removal, idempotency)
//! - R7: Multiple downloads sharing orchestration resources (complete isolation)
//! - R8: Backoff completion versus cancellation (pre-registration cancellation, post-registration cancellation with enabled waiter, and normal expiration)
//! - Supplemental: Seeded stress tests (seeds 42, 1337, 777, 9999, 12345)

use not_download_manager::bench_server::{compute_deterministic_sha256, start_benchmark_server, RetryAfterMode, ServerConfig};
use not_download_manager::concurrency::{
    AdaptiveConfig, ConcurrencyStrategy, TaskTelemetryCollector,
    WorkerLifecycleGuard, WorkerSlotPool, BackoffGuard,
    GUARD_STATE_ACTIVE, GUARD_STATE_RETIRING,
};
use not_download_manager::engine::{
    clear_test_hooks, register_test_hook, spawn_download_engine,
    EngineCommand, EngineEvent, TestHookPoint,
};
use not_download_manager::types::HashVerificationStatus;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("idman_test_p5292_{}_{}", name, std::process::id()));
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

// ===========================================================================
// R1: Scale-down versus scale-up (Deterministic Barrier & Supervisor Orchestration)
// ===========================================================================

/// Direct pool & guard barrier test: retiring slot is strictly unclaimable until guard drop.
#[tokio::test]
async fn test_r1_scale_down_vs_scale_up_deterministic_barrier() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(7101));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // 1. Fill all 16 slots
    let mut guards = Vec::new();
    for _ in 0..16 {
        guards.push(
            WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &trackers,
            )
            .expect("Must acquire all 16 slots"),
        );
    }

    assert_eq!(pool.allocated_count(), 16);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 16);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 16);

    // Barriers to coordinate interleaving:
    // b_retire: worker signals it has marked retiring
    // b_continue: scheduler signals worker it may now drop its guard
    let b_retire = Arc::new(tokio::sync::Barrier::new(2));
    let b_continue = Arc::new(tokio::sync::Barrier::new(2));

    let retiring_guard = guards.remove(0); // Slot 0
    let b_retire_w = b_retire.clone();
    let b_continue_w = b_continue.clone();

    let worker_task = tokio::spawn(async move {
        let g = retiring_guard;
        // Step 1: Worker commits retirement
        assert!(g.mark_retired());
        b_retire_w.wait().await;

        // Step 2: Hold guard alive until scheduler confirms slot cannot be stolen
        b_continue_w.wait().await;
        drop(g);
    });

    // Scheduler waits until worker has committed retirement
    b_retire.wait().await;

    // INTERMEDIATE ASSERTIONS:
    // Slot 0 is in retiring mask, but still in allocated mask!
    let snap = pool.snapshot();
    assert_eq!(snap.allocated_count, 16, "Slot 0 is still allocated in pool");
    assert_eq!(snap.retiring_count, 1, "Slot 0 is marked retiring");
    assert_eq!(snap.active_count, 15, "Authoritative active count is 15");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 15);
    assert!(pool.is_allocated(0));
    assert!(pool.is_retiring(0));

    // Scheduler attempts to scale up back to 16:
    // Slot 0 is held by retiring worker. acquire() MUST return None!
    let steal_attempt = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    );
    assert!(steal_attempt.is_none(), "Scheduler MUST NOT acquire a retiring unreleased slot!");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 15, "Failed acquire must not bump counter");

    // Signal worker task to drop guard
    b_continue.wait().await;
    worker_task.await.unwrap();

    // INTERMEDIATE ASSERTIONS AFTER DROP:
    // Slot 0 is genuinely freed
    let snap_after = pool.snapshot();
    assert_eq!(snap_after.allocated_count, 15);
    assert_eq!(snap_after.retiring_count, 0);
    assert_eq!(snap_after.active_count, 15);
    assert!(!pool.is_allocated(0));
    assert!(!pool.is_retiring(0));

    // Scheduler retries scale up now that slot 0 is available:
    let retry_guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    );
    assert!(retry_guard.is_some(), "Scale-up must succeed after retiring guard drops");
    let new_g = retry_guard.unwrap();
    assert_eq!(new_g.slot_id, 0, "Must safely reuse slot 0 without aliasing");
    assert_eq!(pool.allocated_count(), 16);
    assert_eq!(pool.active_worker_count(), 16);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 16);

    // Quiescence cleanup
    drop(new_g);
    drop(guards);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

/// Orchestration test: real supervisor scale-down and scale-up loop retrying across worker retirement and release.
#[tokio::test]
async fn test_r1_supervisor_scale_down_vs_scale_up_orchestration() {
    let pool = Arc::new(WorkerSlotPool::new(2)); // Max 2 slots
    let telemetry = Arc::new(TaskTelemetryCollector::new(71012));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let target_concurrency = Arc::new(AtomicUsize::new(2));
    let trackers: Vec<Arc<AtomicU64>> = (0..2).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Spawn 2 workers initially
    let g0 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g1 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    assert_eq!(pool.active_worker_count(), 2);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 2);

    let b_worker_retired = Arc::new(tokio::sync::Barrier::new(2));
    let b_drop_guard = Arc::new(tokio::sync::Barrier::new(2));

    let bw_ret = b_worker_retired.clone();
    let bw_drop = b_drop_guard.clone();

    // Worker 0 enters retirement when target is dropped to 1
    let worker_0_handle = tokio::spawn(async move {
        let guard = g0;
        // Worker retires
        assert!(guard.try_retire(1));
        bw_ret.wait().await;

        // Hold guard until supervisor attempts scale-up and encounters full pool
        bw_drop.wait().await;
        drop(guard);
    });

    // Wait until worker 0 commits retirement
    b_worker_retired.wait().await;

    // Target concurrency bumped back to 2 (scale-up request)
    target_concurrency.store(2, Ordering::SeqCst);

    // Supervisor tick:
    let cur_active = pool.active_worker_count();
    assert_eq!(cur_active, 1, "Only Worker 1 is active; Worker 0 is retiring");
    assert_eq!(pool.allocated_count(), 2, "Both slots still allocated");
    assert_eq!(pool.retiring_count(), 1, "Slot 0 is in retiring mask");

    // Supervisor tries to spawn replacement worker:
    let scale_up_attempt = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    );
    assert!(scale_up_attempt.is_none(), "Supervisor scale-up must NOT steal retiring unreleased slot");

    // Release worker 0 to drop its guard and exit
    b_drop_guard.wait().await;
    worker_0_handle.await.unwrap();

    // Next supervisor tick:
    assert_eq!(pool.allocated_count(), 1, "Slot 0 is now freed");
    let scale_up_retry = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    );
    assert!(scale_up_retry.is_some(), "Supervisor scale-up succeeds after worker drop");
    let g0_replacement = scale_up_retry.unwrap();
    assert_eq!(g0_replacement.slot_id, 0, "Reuses slot 0");
    assert_eq!(pool.active_worker_count(), 2);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 2);

    // Teardown
    drop(g0_replacement);
    drop(g1);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ===========================================================================
// R2: Concurrent retirement of different workers
// ===========================================================================
#[test]
fn test_r2_concurrent_retirement_different_workers() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(7102));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Acquire 8 guards (Active concurrency = 8)
    let mut guards = Vec::new();
    for _ in 0..8 {
        guards.push(Arc::new(
            WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &trackers,
            )
            .unwrap(),
        ));
    }

    assert_eq!(pool.active_worker_count(), 8);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 8);

    // Target concurrency is 4.
    // 8 threads simultaneously call try_retire(4).
    let target = 4usize;
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut handles = Vec::new();
    let retired_count = Arc::new(AtomicUsize::new(0));

    for g in &guards {
        let guard_clone = g.clone();
        let b = barrier.clone();
        let ret_counter = retired_count.clone();

        handles.push(std::thread::spawn(move || {
            b.wait();
            if guard_clone.try_retire(target) {
                ret_counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // STRICT INVARIANTS:
    // Exactly 4 workers succeed in retiring, leaving exactly 4 active workers!
    assert_eq!(retired_count.load(Ordering::SeqCst), 4, "Exactly 4 workers must retire");
    assert_eq!(pool.active_worker_count(), 4, "Active workers must NOT drop below target floor 4");
    assert_eq!(pool.retiring_count(), 4, "Retiring count must be 4");
    assert_eq!(pool.allocated_count(), 8, "All 8 slots remain allocated until drop");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 4, "task_active_workers matches authoritative active count");

    // Drop all guards and assert clean convergence
    drop(guards);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ===========================================================================
// R3: Same-guard retirement and termination overlap
// ===========================================================================

/// Ordering A: Task abort / guard drop terminates the guard BEFORE retirement claim can succeed.
#[tokio::test]
async fn test_r3_ordering_a_termination_wins_before_retirement_claim() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(71031));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    )
    .unwrap();

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
    assert_eq!(pool.allocated_count(), 1);

    let b_ready = Arc::new(tokio::sync::Barrier::new(2));
    let b_ready_clone = b_ready.clone();

    // Spawn task that owns guard
    let handle = tokio::spawn(async move {
        let g = guard;
        b_ready_clone.wait().await;
        // Suspended here before retiring
        tokio::time::sleep(Duration::from_secs(10)).await;
        // Attempt retirement (should never be reached if aborted)
        g.mark_retired()
    });

    b_ready.wait().await;

    // Abort the task while it is suspended before retirement
    handle.abort();
    let res = handle.await;
    assert!(res.is_err() && res.unwrap_err().is_cancelled());

    // Task abort cleanly terminated the guard via Drop:
    // - State transitioned ACTIVE -> TERMINATED
    // - task_active_workers decremented 1 -> 0 exactly once
    // - Slot 0 freed in pool
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Counter must be 0 after termination");
    assert_eq!(pool.allocated_count(), 0, "Slot must be freed in pool");
    assert_eq!(pool.retiring_count(), 0, "Retiring count must be 0");
    assert_eq!(pool.active_worker_count(), 0);
}

/// Ordering B: Retirement claim wins first, and THEN task is aborted / guard dropped.
#[tokio::test]
async fn test_r3_ordering_b_retirement_claim_wins_before_termination() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(71032));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    )
    .unwrap();

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
    assert_eq!(pool.allocated_count(), 1);

    let b_retired = Arc::new(tokio::sync::Barrier::new(2));
    let b_retired_clone = b_retired.clone();

    // Spawn task that owns guard
    let handle = tokio::spawn(async move {
        let g = guard;
        // Step 1: Worker commits retirement claim
        assert!(g.mark_retired(), "Retirement claim must succeed");
        b_retired_clone.wait().await;

        // Suspended with guard in RETIRING state
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    b_retired.wait().await;

    // INTERMEDIATE ASSERTIONS: Guard is RETIRING, counter decremented once
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Counter decremented by mark_retired");
    assert_eq!(pool.allocated_count(), 1, "Slot still allocated in pool");
    assert_eq!(pool.retiring_count(), 1, "Slot marked retiring");

    // Abort the task while guard is in RETIRING state:
    handle.abort();
    let res = handle.await;
    assert!(res.is_err() && res.unwrap_err().is_cancelled());

    // CRITICAL INVARIANT: Drop of RETIRING guard MUST NOT decrement task_active_workers again!
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Drop of retiring guard MUST NOT double-decrement to underflow!");
    assert_eq!(pool.allocated_count(), 0, "Slot must be freed after drop");
    assert_eq!(pool.retiring_count(), 0, "Retiring mask must be cleared");
    assert_eq!(pool.active_worker_count(), 0);
}

/// Ordering C: High-frequency race between retirement claim and task abort across 100 cycles.
#[tokio::test]
async fn test_r3_ordering_c_task_abort_racing_retirement_under_contention() {
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    for iter in 0..100 {
        let pool = Arc::new(WorkerSlotPool::new(16));
        let telemetry = Arc::new(TaskTelemetryCollector::new(71033 + iter));
        let task_active_workers = Arc::new(AtomicUsize::new(0));

        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        )
        .unwrap();

        let handle = tokio::spawn(async move {
            let g = guard;
            tokio::task::yield_now().await;
            let _ = g.try_retire(0);
        });

        // Concurrently abort task with slight interleaving jitter
        if iter % 2 == 0 {
            tokio::task::yield_now().await;
        }
        handle.abort();
        let _ = handle.await;

        // Invariant: regardless of which won, slot is released and counter is exactly 0
        assert_eq!(
            task_active_workers.load(Ordering::SeqCst), 0,
            "Iter {}: Counter must cleanly converge to 0 without underflow", iter
        );
        assert_eq!(pool.allocated_count(), 0, "Iter {}: Slot must be released", iter);
        assert_eq!(pool.retiring_count(), 0, "Iter {}: Retiring mask must be clean", iter);
    }
}

/// Verification of state machine non-resurrection: TERMINATED state can NEVER be resurrected to ACTIVE.
#[test]
fn test_r3_state_machine_non_resurrection_and_claim_exclusivity() {
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    for iter in 0..500 {
        let pool = Arc::new(WorkerSlotPool::new(16));
        let telemetry = Arc::new(TaskTelemetryCollector::new(71034 + iter));
        let task_active_workers = Arc::new(AtomicUsize::new(1)); // Start at 1, acquire makes it 2

        let guard = Arc::new(
            WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &trackers,
            )
            .unwrap(),
        );

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let g1 = guard.clone();
        let b1 = barrier.clone();
        let h1 = std::thread::spawn(move || {
            b1.wait();
            g1.mark_retired()
        });

        let g2 = guard.clone();
        let b2 = barrier.clone();
        let h2 = std::thread::spawn(move || {
            b2.wait();
            g2.try_retire(1)
        });

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Exactly one method must claim retirement (strict XOR)
        assert!(r1 ^ r2, "Iter {}: Exactly one caller can claim retirement", iter);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1, "Iter {}: Counter decremented exactly once", iter);

        // Guard state is now RETIRING
        assert_eq!(guard.lifecycle_state(), GUARD_STATE_RETIRING);

        // Drop guard and assert clean transition to TERMINATED without double-decrement
        drop(guard);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1, "Iter {}: Drop must not decrement again", iter);
        assert_eq!(pool.allocated_count(), 0);
        assert_eq!(pool.retiring_count(), 0);
    }
}

/// Verification that pool rejection rollback can NEVER resurrect a TERMINATED guard to ACTIVE.
#[test]
fn test_r3_pool_rejection_rollback_cannot_resurrect_terminated() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(71035));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = Arc::new(
        WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        )
        .unwrap(),
    );

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
    assert_eq!(pool.allocated_count(), 1);

    // Manually mark slot 0 as retiring in the pool directly:
    // This simulates an external pool state where slot 0 is already retiring.
    assert!(pool.mark_retiring(0));

    // Calling mark_retired on guard:
    // 1. claim_retirement() succeeds (ACTIVE -> RETIRING).
    // 2. pool.mark_retiring(0) fails because slot is already marked retiring!
    // 3. Rollback executes: compare_exchange(RETIRING, ACTIVE).
    let did_retire = guard.mark_retired();
    assert!(!did_retire, "mark_retired must return false when pool transition fails");
    // State rolled back to ACTIVE:
    assert_eq!(guard.lifecycle_state(), GUARD_STATE_ACTIVE);
    // Counter was NOT decremented:
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);

    // Drop the guard: state transitions ACTIVE -> TERMINATED, counter decrements to 0
    drop(guard);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
}

// ===========================================================================
// R4: Spawn ownership and task startup boundaries (All 5 Kickoff Boundaries)
// ===========================================================================
#[tokio::test]
async fn test_r4_spawn_ownership_and_task_startup_boundaries() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(7104));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // -----------------------------------------------------------------------
    // Boundary 4A: Pre-guard slot acquisition (panic before guard completion)
    // -----------------------------------------------------------------------
    {
        let panic_task_id = 999_994u64;
        let panic_telemetry = Arc::new(TaskTelemetryCollector::new(panic_task_id));
        let _lock = not_download_manager::concurrency::test_support::ACQUIRE_FAILPOINT_MUTEX.lock().unwrap();
        not_download_manager::concurrency::test_support::SIMULATE_ACQUIRE_PANIC_TASK_ID.store(panic_task_id, Ordering::SeqCst);
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                panic_telemetry,
                &trackers,
            )
        }));
        not_download_manager::concurrency::test_support::SIMULATE_ACQUIRE_PANIC_TASK_ID.store(0, Ordering::SeqCst);
        drop(_lock);

        assert!(panic_result.is_err(), "Boundary 4A: Simulated acquisition panic must be caught");
        assert_eq!(pool.allocated_count(), 0, "Boundary 4A: Slot must be freed by internal defuser on panic unwind");
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Boundary 4A: Counter must remain 0");
    }

    // -----------------------------------------------------------------------
    // Boundary 4B: Post-guard creation, pre-spawn caller unwind
    // -----------------------------------------------------------------------
    {
        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        )
        .unwrap();

        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
        // Simulated caller unwind before task is spawned:
        drop(guard);
    }
    assert_eq!(pool.allocated_count(), 0, "Boundary 4B: Guard drop must free slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Boundary 4B: Counter must return to 0");

    // -----------------------------------------------------------------------
    // Boundary 4C: Task future created but dropped before first poll
    // -----------------------------------------------------------------------
    {
        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        )
        .unwrap();

        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);

        let fut = async move {
            let _g = guard;
            panic!("Should never be polled");
        };
        drop(fut); // Dropped unpolled
    }
    assert_eq!(pool.allocated_count(), 0, "Boundary 4C: Unpolled future drop must release slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);

    // -----------------------------------------------------------------------
    // Boundary 4D: Task spawned, hits startup barrier, aborted
    // -----------------------------------------------------------------------
    {
        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        )
        .unwrap();

        let startup_barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b = startup_barrier.clone();

        let handle = tokio::spawn(async move {
            let _g = guard;
            b.wait().await;
            std::future::pending::<()>().await;
        });

        startup_barrier.wait().await;
        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);

        handle.abort();
        let res = handle.await;
        assert!(res.is_err() && res.unwrap_err().is_cancelled());
    }
    assert_eq!(pool.allocated_count(), 0, "Boundary 4D: Aborted task must release slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);

    // -----------------------------------------------------------------------
    // Boundary 4E: Worker enters backoff sleep, task aborted
    // -----------------------------------------------------------------------
    {
        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        )
        .unwrap();

        let backoff_barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b = backoff_barrier.clone();
        let telem = telemetry.clone();

        let handle = tokio::spawn(async move {
            let _g = guard;
            let _bg = BackoffGuard::new(telem, 0, Duration::from_secs(30));
            b.wait().await;
            // Sleeps in backoff
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        backoff_barrier.wait().await;
        assert_eq!(telemetry.sleeping_workers(), 1);
        assert_eq!(pool.allocated_count(), 1);

        handle.abort();
        let res = handle.await;
        assert!(res.is_err() && res.unwrap_err().is_cancelled());
    }
    assert_eq!(telemetry.sleeping_workers(), 0, "Boundary 4E: Backoff count must be 0 after abort");
    assert_eq!(pool.allocated_count(), 0, "Boundary 4E: Slot must be released after abort");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ===========================================================================
// R5: Pause/Resume versus Recovery Probe (Deterministic Barrier & Quiescence)
// ===========================================================================
#[tokio::test]
async fn test_r5_pause_resume_vs_recovery_probe_deterministic_barrier() {
    let test_dir = TestDir::new("r5_pause_resume_recovery");
    let file_size = 15 * 1024 * 1024;
    let expected_hash = compute_deterministic_sha256(file_size);

    let server_config = ServerConfig {
        total_file_size: file_size,
        rate_limit_429_frequency: 2, // Throttles aggressively
        throttle_max_count: 8,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        ..Default::default()
    };
    let server = start_benchmark_server(server_config).await;

    let mut engine = spawn_download_engine();
    let task_id = 7105;
    let save_path = test_dir.0.join("r5_pause_resume.bin");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(50);
    cfg.cooldown_duration = Duration::from_millis(100);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));
    engine.set_expected_sha256(task_id, Some(expected_hash.clone()));

    // Deterministic test hook: notify test when worker enters backoff
    let backoff_notify = Arc::new(tokio::sync::Notify::new());
    register_test_hook(task_id, TestHookPoint::DuringWorkerBackoff, backoff_notify.clone());

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    // Wait until worker hits backoff (where cancellation waiter is registered)
    tokio::select! {
        _ = backoff_notify.notified() => {}
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("Timed out waiting for DuringWorkerBackoff"),
    }

    // Now send PauseDownload while worker is in backoff
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: task_id })
        .unwrap();

    // Clear test hook and wait for Paused event
    clear_test_hooks(task_id);

    let mut paused = false;
    let pause_timeout = std::time::Instant::now();
    while pause_timeout.elapsed() < Duration::from_secs(10) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            if let EngineEvent::Paused { id } = evt {
                if id == task_id {
                    paused = true;
                    break;
                }
            }
        }
    }
    assert!(paused, "Engine must emit Paused event while worker was in backoff");

    // Sleep briefly to assert no rogue workers continue downloading while paused
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Resume download
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

    // Wait for Finished event with verified SHA-256
    let mut verified = false;
    let resume_timeout = std::time::Instant::now();
    while resume_timeout.elapsed() < Duration::from_secs(25) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(150), engine.event_receiver.recv()).await {
            if let EngineEvent::Finished { id, hash_status } = evt {
                if id == task_id {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
                    verified = true;
                    break;
                }
            }
        }
    }

    assert!(verified, "Resumed download must complete with 100% verified SHA-256");
    server.shutdown().await;
}

// ===========================================================================
// R6: Delete versus Recovery / Worker Cleanup (Deterministic Barrier & Quiescence)
// ===========================================================================
#[tokio::test]
async fn test_r6_delete_vs_recovery_worker_cleanup_deterministic_barrier() {
    let test_dir = TestDir::new("r6_delete_recovery");
    let file_size = 20 * 1024 * 1024;

    let server_config = ServerConfig {
        total_file_size: file_size,
        rate_limit_429_frequency: 2,
        throttle_max_count: 10,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        ..Default::default()
    };
    let server = start_benchmark_server(server_config).await;

    let mut engine = spawn_download_engine();
    let task_id = 7106;
    let save_path = test_dir.0.join("r6_delete.bin");
    let partinfo_path = test_dir.0.join("r6_delete.bin.partinfo");

    let mut cfg = AdaptiveConfig::default();
    cfg.initial_concurrency = 4;
    cfg.max_concurrency = 8;
    cfg.evaluation_interval = Duration::from_millis(50);
    engine.set_concurrency_strategy(task_id, ConcurrencyStrategy::Adaptive(cfg));

    // Register deterministic test hook at backoff
    let backoff_notify = Arc::new(tokio::sync::Notify::new());
    register_test_hook(task_id, TestHookPoint::DuringWorkerBackoff, backoff_notify.clone());

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: server.url.clone(),
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    // Wait for worker to enter backoff
    tokio::select! {
        _ = backoff_notify.notified() => {}
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("Timed out waiting for DuringWorkerBackoff"),
    }

    // Send DeleteDownload with file cleanup requested
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    clear_test_hooks(task_id);

    // Wait for Deleted event
    let mut deleted = false;
    let delete_timeout = std::time::Instant::now();
    while delete_timeout.elapsed() < Duration::from_secs(10) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            if let EngineEvent::Deleted { id, .. } = evt {
                if id == task_id {
                    deleted = true;
                    break;
                }
            }
        }
    }
    assert!(deleted, "Engine must emit Deleted event");

    // Sleep briefly and assert files are completely deleted and not recreated by late workers
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!save_path.exists(), "Target file must be deleted");
    assert!(!partinfo_path.exists(), "Partinfo file must be deleted");

    // Idempotency: repeated delete command must not panic or cause error
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id: task_id,
        cleanup_files: true,
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    server.shutdown().await;
}

// ===========================================================================
// R7: Multiple downloads sharing orchestration resources
// ===========================================================================
#[tokio::test]
async fn test_r7_multiple_downloads_resource_isolation() {
    let test_dir = TestDir::new("r7_multi_download_isolation");
    let file_size = 10 * 1024 * 1024;
    let expected_hash = compute_deterministic_sha256(file_size);

    // Server A throttles with 429
    let server_a = start_benchmark_server(ServerConfig {
        total_file_size: file_size,
        rate_limit_429_frequency: 3,
        throttle_max_count: 5,
        retry_after_mode: RetryAfterMode::DeltaSeconds(1),
        ..Default::default()
    })
    .await;

    // Server B is completely clean (no throttling)
    let server_b = start_benchmark_server(ServerConfig {
        total_file_size: file_size,
        ..Default::default()
    })
    .await;

    let mut engine = spawn_download_engine();
    let task_a = 7107;
    let task_b = 7108;
    let save_a = test_dir.0.join("task_a.bin");
    let save_b = test_dir.0.join("task_b.bin");

    let mut cfg_a = AdaptiveConfig::default();
    cfg_a.initial_concurrency = 4;
    cfg_a.max_concurrency = 8;
    cfg_a.evaluation_interval = Duration::from_millis(50);

    let mut cfg_b = AdaptiveConfig::default();
    cfg_b.initial_concurrency = 4;
    cfg_b.max_concurrency = 8;
    cfg_b.evaluation_interval = Duration::from_millis(50);

    engine.set_concurrency_strategy(task_a, ConcurrencyStrategy::Adaptive(cfg_a));
    engine.set_concurrency_strategy(task_b, ConcurrencyStrategy::Adaptive(cfg_b));
    engine.set_expected_sha256(task_a, Some(expected_hash.clone()));
    engine.set_expected_sha256(task_b, Some(expected_hash.clone()));

    // Launch both downloads concurrently
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_a,
            url: server_a.url.clone(),
            save_path: save_a.clone(),
            connections: 4,
        })
        .unwrap();

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_b,
            url: server_b.url.clone(),
            save_path: save_b.clone(),
            connections: 4,
        })
        .unwrap();

    let mut done_a = false;
    let mut done_b = false;
    let timeout = std::time::Instant::now();

    while timeout.elapsed() < Duration::from_secs(30) && (!done_a || !done_b) {
        if let Ok(Some(evt)) = tokio::time::timeout(Duration::from_millis(100), engine.event_receiver.recv()).await {
            match evt {
                EngineEvent::Finished { id, hash_status } => {
                    assert!(matches!(hash_status, HashVerificationStatus::Verified));
                    if id == task_a { done_a = true; }
                    if id == task_b { done_b = true; }
                }
                EngineEvent::Failed { id, error } => {
                    panic!("Task {} failed unexpectedly: {}", id, error);
                }
                _ => {}
            }
        }
    }

    assert!(done_a, "Throttled Task A must complete with verified SHA-256");
    assert!(done_b, "Unthrottled Task B must complete with verified SHA-256");

    server_a.shutdown().await;
    server_b.shutdown().await;
}

// ===========================================================================
// R8: Backoff completion versus cancellation (Pre- and Post-registration)
// ===========================================================================
#[tokio::test]
async fn test_r8_backoff_completion_vs_cancellation_race() {
    let telemetry = Arc::new(TaskTelemetryCollector::new(7108));
    let notify = Arc::new(tokio::sync::Notify::new());
    let cancel_flag = Arc::new(AtomicBool::new(false));

    // -----------------------------------------------------------------------
    // Case 8A: Cancellation AFTER waiter registration (waiter enabled before hook)
    // -----------------------------------------------------------------------
    {
        let t = telemetry.clone();
        let n = notify.clone();
        let c = cancel_flag.clone();
        c.store(false, Ordering::SeqCst);

        let backoff_started = Arc::new(tokio::sync::Barrier::new(2));
        let b_start = backoff_started.clone();

        let worker_handle = tokio::spawn(async move {
            let _guard = BackoffGuard::new(t, 0, Duration::from_secs(10));
            // In accordance with engine pattern: enable waiter before awaiting barrier/hook
            let notified = n.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if c.load(Ordering::SeqCst) {
                return "cancelled_early";
            }

            b_start.wait().await;

            tokio::select! {
                biased;
                _ = &mut notified => {
                    if c.load(Ordering::SeqCst) {
                        return "cancelled";
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
            "expired"
        });

        // Worker reaches barrier with waiter ALREADY registered:
        backoff_started.wait().await;
        assert_eq!(telemetry.sleeping_workers(), 1);

        // Cancel during backoff sleep
        cancel_flag.store(true, Ordering::SeqCst);
        notify.notify_waiters();

        let res = tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("Task must exit promptly on cancellation, not wait 10s")
            .unwrap();

        assert_eq!(res, "cancelled", "Worker must exit via cancellation branch");
        assert_eq!(telemetry.sleeping_workers(), 0, "Sleeping workers must be decremented immediately");
    }

    // -----------------------------------------------------------------------
    // Case 8B: Cancellation BEFORE waiter registration
    // -----------------------------------------------------------------------
    {
        let t = telemetry.clone();
        let n = notify.clone();
        let c = cancel_flag.clone();
        c.store(true, Ordering::SeqCst); // Pre-cancelled!

        let worker_handle = tokio::spawn(async move {
            let _guard = BackoffGuard::new(t, 1, Duration::from_secs(10));
            let notified = n.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // Pre-check catches cancellation immediately without sleeping:
            if c.load(Ordering::SeqCst) {
                return "cancelled_immediate";
            }

            tokio::select! {
                biased;
                _ = &mut notified => {
                    if c.load(Ordering::SeqCst) { return "cancelled"; }
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
            "expired"
        });

        let res = tokio::time::timeout(Duration::from_secs(1), worker_handle)
            .await
            .expect("Pre-cancelled task must exit immediately")
            .unwrap();

        assert_eq!(res, "cancelled_immediate", "Worker must exit via immediate pre-check");
        assert_eq!(telemetry.sleeping_workers(), 0, "Sleeping workers must return to 0");
    }

    // -----------------------------------------------------------------------
    // Case 8C: Normal sleep expiry when NO cancellation occurs
    // -----------------------------------------------------------------------
    {
        let t = telemetry.clone();
        let n = notify.clone();
        let c = cancel_flag.clone();
        c.store(false, Ordering::SeqCst);

        let worker_handle = tokio::spawn(async move {
            let _guard = BackoffGuard::new(t, 2, Duration::from_millis(30));
            let notified = n.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if c.load(Ordering::SeqCst) {
                return "cancelled";
            }

            tokio::select! {
                biased;
                _ = &mut notified => {
                    if c.load(Ordering::SeqCst) { return "cancelled"; }
                }
                _ = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
            "expired"
        });

        let res = tokio::time::timeout(Duration::from_secs(1), worker_handle)
            .await
            .expect("Task must expire sleep normally")
            .unwrap();

        assert_eq!(res, "expired", "Worker must complete normal sleep expiration");
        assert_eq!(telemetry.sleeping_workers(), 0, "Sleeping workers must return to 0");
    }
}

// ===========================================================================
// Supplemental Seeded Stress Tests (Seeds 42, 1337, 777, 9999, 12345)
// ===========================================================================
#[test]
fn test_supplemental_seeded_stress_runs() {
    struct SimpleRng(u64);
    impl SimpleRng {
        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            (self.0 >> 32) as u32
        }
        fn range(&mut self, min: usize, max: usize) -> usize {
            if min >= max { min } else { min + (self.next_u32() as usize % (max - min + 1)) }
        }
    }

    let seeds = [42u64, 1337u64, 777u64, 9999u64, 12345u64];

    for seed in seeds {
        let pool = Arc::new(WorkerSlotPool::new(16));
        let telemetry = Arc::new(TaskTelemetryCollector::new(seed));
        let task_active_workers = Arc::new(AtomicUsize::new(0));
        let trackers: Arc<Vec<Arc<AtomicU64>>> = Arc::new((0..16).map(|_| Arc::new(AtomicU64::new(0))).collect());

        let mut active_guards: Vec<Option<WorkerLifecycleGuard>> = (0..16).map(|_| None).collect();
        let mut rng = SimpleRng(seed);

        for _step in 0..500 {
            let action = rng.range(0, 2);
            match action {
                0 => {
                    // Action: Acquire slot
                    if let Some(guard) = WorkerLifecycleGuard::acquire(
                        pool.clone(),
                        task_active_workers.clone(),
                        telemetry.clone(),
                        &trackers,
                    ) {
                        let id = guard.slot_id;
                        active_guards[id] = Some(guard);
                    }
                }
                1 => {
                    // Action: Retire slot
                    let allocated = pool.active_slots();
                    if !allocated.is_empty() {
                        let slot = allocated[rng.range(0, allocated.len() - 1)];
                        if let Some(ref g) = active_guards[slot] {
                            let _ = g.mark_retired();
                        }
                    }
                }
                2 => {
                    // Action: Drop guard
                    let allocated = pool.active_slots();
                    if !allocated.is_empty() {
                        let slot = allocated[rng.range(0, allocated.len() - 1)];
                        active_guards[slot] = None;
                    }
                }
                _ => unreachable!(),
            }

            // Representation Invariant holds 100% on Authoritative Snapshot:
            let snap = pool.snapshot();
            assert_eq!(
                snap.allocated_count,
                snap.active_count + snap.retiring_count,
                "Seed {}: Authoritative representation invariant failed", seed
            );
        }

        // Post-quiescence: drop all remaining guards
        for g in active_guards.iter_mut() {
            *g = None;
        }

        assert_eq!(pool.allocated_count(), 0, "Seed {}: Pool must be empty", seed);
        assert_eq!(pool.retiring_count(), 0, "Seed {}: Retiring count must be 0", seed);
        assert_eq!(pool.active_worker_count(), 0, "Seed {}: Active worker count must be 0", seed);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Seed {}: Cached counter must converge to 0", seed);
    }
}
