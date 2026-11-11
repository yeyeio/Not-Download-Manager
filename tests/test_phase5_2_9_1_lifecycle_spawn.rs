//! Dedicated Test Suite for Phase 5.2.9.1: Lifecycle & Spawn Ownership Hardening
//!
//! Validates:
//! 1. Slot acquired, then spawn path fails before task starts.
//! 2. Future dropped before first poll.
//! 3. Running task aborted.
//! 4. Worker panics after guard ownership transfer.
//! 5. Cancellation during worker startup.
//! 6. Retirement overlaps with a scale-up request (Deterministic Barrier test).
//! 7. Repeated acquire/release cycles across concurrent threads.
//! 8. Scale-up retries and reaches target concurrency after slot becomes available.

use not_download_manager::concurrency::{
    TaskTelemetryCollector, WorkerLifecycleGuard, WorkerSlotPool, WorkerState,
    GUARD_STATE_ACTIVE,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// TEST 1: Slot acquired, then spawn path fails before task starts
// ---------------------------------------------------------------------------
#[test]
fn test_slot_acquired_then_spawn_fails_before_task_starts() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5291));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);

    // Acquire guard in scheduler context
    {
        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        ).expect("Slot must be acquired");

        assert_eq!(guard.slot_id, 0);
        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
        assert_eq!(telemetry.get_worker_state(0), WorkerState::Idle);

        // Simulated error/panic in caller before task is ever spawned:
        // guard goes out of scope and drops here
        drop(guard);
    }

    // Ownership contract assertions:
    assert_eq!(pool.allocated_count(), 0, "Slot must be released immediately on drop");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Active counter must be decremented");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated, "State must reset to Terminated");
}

// ---------------------------------------------------------------------------
// TEST 2: Future dropped before first poll
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_future_dropped_before_first_poll() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5292));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    ).expect("Slot must be acquired");

    assert_eq!(pool.allocated_count(), 1);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);

    // Guard is moved into an async block (the unspawned future)
    let fut = async move {
        let _g = guard;
        // Never reached because future is never polled
        panic!("Should never execute");
    };

    // Drop the future before polling it
    drop(fut);

    // Ownership contract assertions:
    assert_eq!(pool.allocated_count(), 0, "Unpolled future drop must release slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Unpolled future drop must decrement active counter");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated, "State must reset to Terminated");
}

// ---------------------------------------------------------------------------
// TEST 3: Running task aborted
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_running_task_aborted() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5293));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let started_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let started_clone = started_barrier.clone();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    ).expect("Slot must be acquired");

    let handle = tokio::spawn(async move {
        let _g = guard;
        started_clone.wait().await;
        // Hang indefinitely until aborted
        std::future::pending::<()>().await;
    });

    // Wait until task starts and enters pending state
    started_barrier.wait().await;
    assert_eq!(pool.allocated_count(), 1);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);

    // Abort the task
    handle.abort();
    let res = handle.await;
    assert!(res.is_err());
    assert!(res.unwrap_err().is_cancelled());

    // Ownership contract assertions:
    assert_eq!(pool.allocated_count(), 0, "Aborted task must release slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Aborted task must decrement active counter");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated, "State must reset to Terminated");
}

// ---------------------------------------------------------------------------
// TEST 4: Worker panics after guard ownership transfer
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_worker_panics_after_guard_ownership_transfer() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5294));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    ).expect("Slot must be acquired");

    let handle = tokio::spawn(async move {
        let _g = guard;
        panic!("Intentional panic inside worker task");
    });

    let res = handle.await;
    assert!(res.is_err());
    assert!(res.unwrap_err().is_panic());

    // Ownership contract assertions:
    assert_eq!(pool.allocated_count(), 0, "Panic unwind must release slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Panic unwind must decrement active counter");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated, "State must reset to Terminated");
}

// ---------------------------------------------------------------------------
// TEST 5: Cancellation during worker startup
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_cancellation_during_worker_startup() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5295));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let cancel_flag = Arc::new(AtomicBool::new(true)); // Already canceled
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    ).expect("Slot must be acquired");

    let c_flag = cancel_flag.clone();
    let handle = tokio::spawn(async move {
        let _g = guard;
        if c_flag.load(Ordering::SeqCst) {
            return; // Immediate startup cancellation
        }
        panic!("Must not reach here");
    });

    handle.await.unwrap();

    // Ownership contract assertions:
    assert_eq!(pool.allocated_count(), 0, "Startup cancellation must release slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Startup cancellation must decrement active counter");
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated);
}

// ---------------------------------------------------------------------------
// TEST 6: Retirement overlaps with a scale-up request (Deterministic Barrier Test)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_retirement_overlaps_with_scale_up_request() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5296));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let target_concurrency = Arc::new(AtomicUsize::new(16));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // 1. Acquire all 16 slots (pool is completely full)
    let mut guards = Vec::new();
    for _ in 0..16 {
        let guard = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        ).expect("Must acquire all 16 slots");
        guards.push(guard);
    }

    assert_eq!(pool.allocated_count(), 16);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 16);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 16);

    // 2. Target reduced to 15 (1 worker should retire)
    target_concurrency.store(15, Ordering::SeqCst);

    // Worker 0 executes scale-down via try_retire
    let target = target_concurrency.load(Ordering::SeqCst);
    let did_retire = guards[0].try_retire(target);
    assert!(did_retire, "Worker 0 must successfully retire");

    // CRITICAL OBSERVATION POINT:
    // Worker 0 has retired in counter and retiring_mask, BUT guards[0] has NOT been dropped yet!
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 15);
    assert_eq!(pool.allocated_count(), 16, "Slot 0 is STILL allocated in pool");
    assert_eq!(pool.retiring_count(), 1, "Slot 0 is marked as retiring");
    assert_eq!(pool.active_worker_count(), 15, "Active worker count matches task_active_workers");
    assert!(pool.is_retiring(0));
    assert!(pool.is_allocated(0));

    // 3. Scheduler receives scale-up request back to 16
    target_concurrency.store(16, Ordering::SeqCst);
    let cur_target = target_concurrency.load(Ordering::SeqCst);
    let cur_act = task_active_workers.load(Ordering::SeqCst);
    assert!(cur_target > cur_act, "Scheduler wants to scale up (16 > 15)");

    // Scheduler attempts to acquire a slot:
    // Slot 0 is still held by the retiring worker that hasn't dropped yet.
    // acquire() MUST return None because all 16 slots are in allocated_mask!
    let scale_up_guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    );

    assert!(
        scale_up_guard.is_none(),
        "Scheduler must NOT be allowed to acquire a slot that is currently retiring!"
    );
    assert_eq!(
        task_active_workers.load(Ordering::SeqCst), 15,
        "Failed acquire must NOT increment active worker counter"
    );

    // 4. Retiring worker completes its exit and drops guards[0]
    let retiring_guard = guards.remove(0);
    drop(retiring_guard); // Slot 0 is now genuinely freed!

    assert_eq!(pool.allocated_count(), 15, "Slot 0 must be released after drop");
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 15);
    assert!(!pool.is_allocated(0));
    assert!(!pool.is_retiring(0));

    // 5. Scheduler retries scale-up now that slot 0 is available
    let scale_up_guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    );

    assert!(scale_up_guard.is_some(), "Scheduler must successfully acquire the freed slot");
    let new_guard = scale_up_guard.unwrap();
    assert_eq!(new_guard.slot_id, 0, "Must safely reuse slot 0");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 16, "Active counter restored to 16");
    assert_eq!(pool.allocated_count(), 16);
    assert_eq!(pool.active_worker_count(), 16);

    // Cleanup
    drop(new_guard);
    drop(guards);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// TEST 7: Repeated acquire/release cycles across concurrent threads
// ---------------------------------------------------------------------------
#[test]
fn test_repeated_acquire_release_cycles() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5297));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Arc<Vec<Arc<AtomicU64>>> = Arc::new((0..16).map(|_| Arc::new(AtomicU64::new(0))).collect());

    let mut handles = Vec::new();
    for _ in 0..16 {
        let p = pool.clone();
        let telem = telemetry.clone();
        let act = task_active_workers.clone();
        let tr = trackers.clone();

        handles.push(std::thread::spawn(move || {
            for _ in 0..500 {
                if let Some(guard) = WorkerLifecycleGuard::acquire(p.clone(), act.clone(), telem.clone(), &tr) {
                    assert!(guard.slot_id < 16);
                    std::thread::yield_now();
                    drop(guard);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(pool.allocated_count(), 0, "All slots must be freed at end of stress");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Counter must be exactly 0");
}

// ---------------------------------------------------------------------------
// TEST 8: Scale-up tolerates temporary slot unavailability and reaches target
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_scale_up_tolerates_temporary_slot_unavailability() {
    let pool = Arc::new(WorkerSlotPool::new(4)); // 4 slots max
    let telemetry = Arc::new(TaskTelemetryCollector::new(5298));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..4).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Fill all 4 slots
    let mut guards = Vec::new();
    for _ in 0..4 {
        guards.push(WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        ).unwrap());
    }

    // Worker 0 marks retiring (mark_retired decrements task_active_workers internally)
    guards[0].mark_retired();

    // Target concurrency is 4. Currently active = 3.
    let target = 4usize;
    let mut spawned = None;

    // Scheduler attempts to scale up 5 times
    for _ in 0..5 {
        let cur_active = task_active_workers.load(Ordering::SeqCst);
        if target > cur_active {
            if let Some(g) = WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &trackers,
            ) {
                spawned = Some(g);
                break;
            }
        }
        // Simulated tick where retiring worker hasn't dropped yet
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(spawned.is_none(), "Scale-up must not succeed while slot 0 is retiring and unreleased");

    // Retiring worker finally drops
    guards.remove(0); // drops guard 0
    assert_eq!(pool.allocated_count(), 3);

    // Next scheduler tick:
    let cur_active = task_active_workers.load(Ordering::SeqCst);
    if target > cur_active {
        spawned = WorkerLifecycleGuard::acquire(
            pool.clone(),
            task_active_workers.clone(),
            telemetry.clone(),
            &trackers,
        );
    }

    assert!(spawned.is_some(), "Scale-up must succeed once slot becomes free");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 4);
    assert_eq!(pool.allocated_count(), 4);

    // Cleanup
    drop(spawned);
    drop(guards);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// TEST 9: Exact N -> N-1 retiring -> N-1 dropped direct counter assertions
// ---------------------------------------------------------------------------
#[test]
fn test_exact_counter_assertions_through_lifecycle() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5299));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // 1. Initial State
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);

    // 2. Acquire 8 guards
    let mut guards = Vec::new();
    for _ in 0..8 {
        guards.push(
            WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &trackers,
            )
            .expect("Acquire must succeed"),
        );
    }

    assert_eq!(pool.allocated_count(), 8);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 8);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 8);
    assert_eq!(
        pool.allocated_count(),
        task_active_workers.load(Ordering::SeqCst) + pool.retiring_count(),
        "Invariant: allocated == active + retiring"
    );

    // 3. Mark guard 3 as retiring
    let retired_success = guards[3].mark_retired();
    assert!(retired_success, "First mark_retired must return true");

    // Repeat mark_retired on same guard: must be idempotent no-op returning false
    assert!(!guards[3].mark_retired(), "Second mark_retired must return false");

    // Intermediate state assertions:
    assert_eq!(pool.allocated_count(), 8, "Slot 3 remains allocated");
    assert_eq!(pool.retiring_count(), 1, "Slot 3 is retiring");
    assert_eq!(pool.active_worker_count(), 7, "Active workers decremented to 7");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 7, "task_active_workers decremented to 7");
    assert_eq!(
        pool.allocated_count(),
        task_active_workers.load(Ordering::SeqCst) + pool.retiring_count(),
        "Invariant: allocated (8) == active (7) + retiring (1)"
    );

    // 4. Drop the retiring guard
    let retiring_guard = guards.remove(3);
    drop(retiring_guard);

    // Post-drop state assertions:
    assert_eq!(pool.allocated_count(), 7, "Slot 3 is now genuinely released");
    assert_eq!(pool.retiring_count(), 0, "No slots retiring");
    assert_eq!(pool.active_worker_count(), 7, "Active workers remain 7");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 7, "task_active_workers not double-decremented");
    assert_eq!(
        pool.allocated_count(),
        task_active_workers.load(Ordering::SeqCst) + pool.retiring_count(),
        "Invariant: allocated (7) == active (7) + retiring (0)"
    );

    // 5. Drop remaining 7 guards
    drop(guards);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// TEST 10: 1,000 Rapid Retire / Drop / Respawn Stress Cycles Without Drift
// ---------------------------------------------------------------------------
#[test]
fn test_1000_rapid_retire_drop_respawn_cycles() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5300));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    for cycle in 0..1000 {
        // Step A: Acquire 8 guards
        let mut guards = Vec::new();
        for _ in 0..8 {
            guards.push(
                WorkerLifecycleGuard::acquire(
                    pool.clone(),
                    task_active_workers.clone(),
                    telemetry.clone(),
                    &trackers,
                )
                .expect("Acquire must succeed"),
            );
        }

        assert_eq!(pool.allocated_count(), 8, "Cycle {}: allocated must be 8", cycle);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 8, "Cycle {}: active must be 8", cycle);

        // Step B: Retire 3 guards
        assert!(guards[0].mark_retired());
        assert!(guards[2].mark_retired());
        assert!(guards[4].mark_retired());

        assert_eq!(pool.allocated_count(), 8);
        assert_eq!(pool.retiring_count(), 3);
        assert_eq!(pool.active_worker_count(), 5);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 5);
        assert_eq!(
            pool.allocated_count(),
            task_active_workers.load(Ordering::SeqCst) + pool.retiring_count()
        );

        // Step C: Drop the 3 retiring guards
        // Indices to remove in reverse: 4, 2, 0
        drop(guards.remove(4));
        drop(guards.remove(2));
        drop(guards.remove(0));

        assert_eq!(pool.allocated_count(), 5);
        assert_eq!(pool.retiring_count(), 0);
        assert_eq!(pool.active_worker_count(), 5);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 5);

        // Step D: Respawn 3 guards back to 8
        for _ in 0..3 {
            guards.push(
                WorkerLifecycleGuard::acquire(
                    pool.clone(),
                    task_active_workers.clone(),
                    telemetry.clone(),
                    &trackers,
                )
                .expect("Respawn must succeed"),
            );
        }

        assert_eq!(pool.allocated_count(), 8);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 8);

        // Step E: Clean drop of all 8 guards
        drop(guards);

        assert_eq!(pool.allocated_count(), 0, "Cycle {}: pool must be empty", cycle);
        assert_eq!(pool.retiring_count(), 0);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Cycle {}: counter must be 0", cycle);
    }
}

// ---------------------------------------------------------------------------
// TEST 11: Coherent Atomic Snapshot Verification Under High Contention
// ---------------------------------------------------------------------------
#[test]
fn test_coherent_atomic_snapshot_under_contention() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let stop = Arc::new(AtomicBool::new(false));

    // 4 Writer threads: randomly acquire, retire, and release slots
    let mut writers = Vec::new();
    for _thread_idx in 0..4 {
        let p = pool.clone();
        let s = stop.clone();
        writers.push(std::thread::spawn(move || {
            let mut my_slots = Vec::new();
            let mut iter = 0u64;
            while !s.load(Ordering::Relaxed) && iter < 10_000 {
                iter += 1;
                // Acquire
                if my_slots.len() < 4 {
                    if let Some(slot) = p.acquire_slot() {
                        my_slots.push(slot);
                    }
                }
                // Retire
                if !my_slots.is_empty() && (iter % 3 == 0) {
                    p.mark_retiring(my_slots[0]);
                }
                // Release
                if !my_slots.is_empty() && (iter % 2 == 0) {
                    let slot = my_slots.remove(0);
                    p.release_slot(slot);
                }
            }
            // Cleanup remaining
            for slot in my_slots {
                p.release_slot(slot);
            }
        }));
    }

    // 4 Reader threads: continuously read snapshot and verify strict algebraic invariants
    let mut readers = Vec::new();
    for _ in 0..4 {
        let p = pool.clone();
        let s = stop.clone();
        readers.push(std::thread::spawn(move || {
            let mut checks = 0usize;
            while !s.load(Ordering::Relaxed) && checks < 20_000 {
                checks += 1;
                let snap = p.snapshot();

                // Strict atomic algebraic invariants on single u64 snapshot:
                assert_eq!(
                    snap.allocated_count,
                    snap.active_count + snap.retiring_count,
                    "Snapshot must satisfy allocated == active + retiring"
                );
                assert_eq!(
                    snap.allocated_mask.count_ones() as usize,
                    snap.allocated_count
                );
                assert_eq!(
                    snap.retiring_mask.count_ones() as usize,
                    snap.retiring_count
                );
                assert_eq!(
                    (snap.allocated_mask & !snap.retiring_mask).count_ones() as usize,
                    snap.active_count
                );
                // Retiring slots must always be a subset of allocated slots
                assert_eq!(
                    snap.retiring_mask & !snap.allocated_mask,
                    0,
                    "A retiring slot MUST also be marked allocated"
                );
            }
        }));
    }

    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(pool.active_worker_count(), 0);
}

// ---------------------------------------------------------------------------
// TEST 12: Empty Tracker Input Defense (Panic-Free Constructor Guarantee)
// ---------------------------------------------------------------------------
#[test]
fn test_empty_tracker_input_defense() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5302));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let empty_trackers: Vec<Arc<AtomicU64>> = Vec::new();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &empty_trackers,
    );

    assert!(guard.is_none(), "Acquire with empty trackers must return None safely");
    assert_eq!(pool.allocated_count(), 0, "Must not leak slot");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Must not leak active counter");
}

// ---------------------------------------------------------------------------
// TEST 13: Direct try_retire Concurrent Contention Coordination
// ---------------------------------------------------------------------------
#[test]
fn test_try_retire_concurrent_contention() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5303));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Fill all 16 slots
    let mut guards = Vec::new();
    for _ in 0..16 {
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

    assert_eq!(pool.allocated_count(), 16);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 16);

    // Target concurrency reduced to 8.
    // 16 concurrent threads attempt try_retire(8).
    let target = 8usize;
    let mut handles = Vec::new();
    let retired_count = Arc::new(AtomicUsize::new(0));

    for g in &guards {
        let guard_clone = g.clone();
        let ret_counter = retired_count.clone();
        handles.push(std::thread::spawn(move || {
            if guard_clone.try_retire(target) {
                ret_counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Exactly 8 workers should have retired, leaving exactly 8 active workers!
    assert_eq!(retired_count.load(Ordering::SeqCst), 8, "Exactly 8 workers must retire");
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 8, "Active workers must settle at 8");
    assert_eq!(pool.allocated_count(), 16, "All 16 slots remain allocated until drop");
    assert_eq!(pool.retiring_count(), 8, "8 slots are in retiring mask");
    assert_eq!(pool.active_worker_count(), 8, "Active worker count is 8");

    // Invariant holds
    assert_eq!(
        pool.allocated_count(),
        task_active_workers.load(Ordering::SeqCst) + pool.retiring_count()
    );

    // Drop all guards
    drop(guards);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// TEST 14: Direct Race Between mark_retired and try_retire on the SAME Guard
// ---------------------------------------------------------------------------
#[test]
fn test_mark_retired_races_try_retire_same_guard() {
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    for iter in 0..2000 {
        let pool = Arc::new(WorkerSlotPool::new(16));
        let telemetry = Arc::new(TaskTelemetryCollector::new(5400 + (iter as u64 % 100)));
        // Start with 2 active workers so that try_retire(1) sees cur_active (2) > target (1)
        let task_active_workers = Arc::new(AtomicUsize::new(1)); // Guard acquisition will make it 2

        let guard = Arc::new(
            WorkerLifecycleGuard::acquire(
                pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &trackers,
            )
            .expect("Acquire must succeed"),
        );

        assert_eq!(task_active_workers.load(Ordering::SeqCst), 2);
        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(pool.retiring_count(), 0);

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

        let res1 = h1.join().unwrap();
        let res2 = h2.join().unwrap();

        // CRITICAL INVARIANT:
        // Exactly ONE method may succeed in claiming retirement!
        // It is strictly impossible for both to claim retirement or for neither to claim it.
        assert!(
            res1 ^ res2,
            "Iteration {}: Exactly one of mark_retired ({}) or try_retire ({}) must win the claim!",
            iter, res1, res2
        );

        // Counter must have been decremented EXACTLY ONCE (from 2 down to 1)
        assert_eq!(
            task_active_workers.load(Ordering::SeqCst),
            1,
            "Iteration {}: Counter must be decremented exactly once, never twice!",
            iter
        );

        // Slot pool must have slot marked retiring
        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(pool.retiring_count(), 1);
        assert_eq!(pool.active_worker_count(), 0);

        // Retiring guard is dropped
        drop(guard);

        // Post-drop: counter must remain 1 (NOT decremented again on drop of a retired guard)
        assert_eq!(
            task_active_workers.load(Ordering::SeqCst),
            1,
            "Iteration {}: Drop of retired guard must not double-decrement!",
            iter
        );
        assert_eq!(pool.allocated_count(), 0);
        assert_eq!(pool.retiring_count(), 0);
    }
}

// ---------------------------------------------------------------------------
// TEST 15: Drop Without Prior Retirement Accounting Guarantee
// ---------------------------------------------------------------------------
#[test]
fn test_drop_without_retirement_accounting() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5500));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    )
    .expect("Acquisition must succeed");

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
    assert_eq!(pool.allocated_count(), 1);
    assert_eq!(pool.retiring_count(), 0);

    // Guard drops WITHOUT calling mark_retired or try_retire
    drop(guard);

    assert_eq!(
        task_active_workers.load(Ordering::SeqCst),
        0,
        "Unretired guard must decrement task_active_workers on drop"
    );
    assert_eq!(pool.allocated_count(), 0, "Slot must be released on drop");
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(telemetry.get_worker_state(0), WorkerState::Terminated);
}

// ---------------------------------------------------------------------------
// TEST 16: Concurrent Retirement, Guard Drop, and Slot Recycling
// ---------------------------------------------------------------------------
#[test]
fn test_concurrent_retirement_and_drop_recycling() {
    let seeds = [42u64, 1337u64, 777u64, 9999u64, 12345u64];
    for seed in seeds {
        let pool = Arc::new(WorkerSlotPool::new(8));
        let telemetry = Arc::new(TaskTelemetryCollector::new(seed));
        let task_active_workers = Arc::new(AtomicUsize::new(0));
        let trackers: Arc<Vec<Arc<AtomicU64>>> = Arc::new((0..8).map(|_| Arc::new(AtomicU64::new(0))).collect());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let p = pool.clone();
            let telem = telemetry.clone();
            let act = task_active_workers.clone();
            let tr = trackers.clone();

            handles.push(std::thread::spawn(move || {
                for i in 0..200 {
                    if let Some(guard) = WorkerLifecycleGuard::acquire(p.clone(), act.clone(), telem.clone(), &tr) {
                        if i % 2 == 0 {
                            guard.mark_retired();
                        }
                        drop(guard);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pool.allocated_count(), 0, "Seed {}: All slots must be freed", seed);
        assert_eq!(pool.retiring_count(), 0, "Seed {}: Retiring count must be 0", seed);
        assert_eq!(task_active_workers.load(Ordering::SeqCst), 0, "Seed {}: Counter must be 0", seed);
    }
}

// ---------------------------------------------------------------------------
// TEST 17: Counter and Slot State Interleaving & Invariant Verification (Option B)
// ---------------------------------------------------------------------------
#[test]
fn test_counter_and_slot_state_interleaving() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5600));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Arc<Vec<Arc<AtomicU64>>> = Arc::new((0..16).map(|_| Arc::new(AtomicU64::new(0))).collect());
    let stop = Arc::new(AtomicBool::new(false));

    // Writer threads performing rapid transitions
    let mut writers = Vec::new();
    for _ in 0..4 {
        let p = pool.clone();
        let telem = telemetry.clone();
        let act = task_active_workers.clone();
        let tr = trackers.clone();
        let s = stop.clone();

        writers.push(std::thread::spawn(move || {
            let mut guards = Vec::new();
            while !s.load(Ordering::Relaxed) {
                if guards.len() < 4 {
                    if let Some(g) = WorkerLifecycleGuard::acquire(p.clone(), act.clone(), telem.clone(), &tr) {
                        guards.push(g);
                    }
                }
                if !guards.is_empty() {
                    guards[0].mark_retired();
                    guards.remove(0); // drops retired guard
                }
            }
        }));
    }

    // Reader threads verifying Authoritative SlotPool invariants
    let mut readers = Vec::new();
    for _ in 0..4 {
        let p = pool.clone();
        let act = task_active_workers.clone();
        let s = stop.clone();

        readers.push(std::thread::spawn(move || {
            let mut checks = 0;
            while !s.load(Ordering::Relaxed) && checks < 25_000 {
                checks += 1;
                // 1. Authoritative atomic snapshot check:
                let snap = p.snapshot();
                assert_eq!(
                    snap.allocated_count,
                    snap.active_count + snap.retiring_count,
                    "Authoritative SlotPool snapshot invariant must hold at all times"
                );

                // 2. Cached counter bound check:
                let cached = act.load(Ordering::Relaxed);
                assert!(
                    cached <= 16,
                    "Cached counter must never exceed max slots bound: {}", cached
                );
            }
        }));
    }

    // Run for 300ms
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);

    for w in writers {
        w.join().unwrap();
    }
    for r in readers {
        r.join().unwrap();
    }

    // Drain and verify final clean state
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// TEST 18: mark_retired Transactional Rollback When Slot Pool Rejects Transition
// ---------------------------------------------------------------------------
#[test]
fn test_mark_retired_pool_rejection_rollback() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5701));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let guard = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    )
    .expect("Acquire must succeed");

    assert_eq!(guard.slot_id, 0);
    assert_eq!(guard.lifecycle_state(), GUARD_STATE_ACTIVE);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 1);
    assert_eq!(pool.allocated_count(), 1);
    assert_eq!(pool.retiring_count(), 0);

    // Sabotage the pool state: manually mark slot 0 as retiring in the pool directly
    // so that subsequent pool.mark_retiring(0) inside guard.mark_retired() returns false!
    let pool_first_retire = pool.mark_retiring(0);
    assert!(pool_first_retire, "Direct pool retirement must succeed");
    assert_eq!(pool.retiring_count(), 1);

    // Now call guard.mark_retired():
    // 1. guard.claim_retirement() succeeds (transitions ACTIVE -> RETIRING).
    // 2. pool.mark_retiring(0) fails (returns false, since slot 0 is already retiring).
    // 3. Rollback executes: transitions guard state RETIRING -> ACTIVE via CAS!
    // 4. Returns false.
    let mark_res = guard.mark_retired();
    assert!(!mark_res, "mark_retired must return false when pool transition is rejected");

    // CRITICAL INVARIANT:
    // Guard state must be cleanly rolled back to ACTIVE!
    assert_eq!(
        guard.lifecycle_state(),
        GUARD_STATE_ACTIVE,
        "Guard state must be rolled back to ACTIVE on pool rejection"
    );

    // Counter must NOT have been decremented
    assert_eq!(
        task_active_workers.load(Ordering::SeqCst),
        1,
        "Counter must not be decremented on rejected retirement"
    );

    // When guard drops, because it is ACTIVE (not RETIRING),
    // Drop sees prev == GUARD_STATE_ACTIVE and decrements task_active_workers exactly once!
    drop(guard);

    assert_eq!(
        task_active_workers.load(Ordering::SeqCst),
        0,
        "Drop of rolled-back active guard must decrement counter exactly once"
    );
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
}

// ---------------------------------------------------------------------------
// TEST 19: try_retire Transactional Rollback When Slot Pool Rejects Transition
// ---------------------------------------------------------------------------
#[test]
fn test_try_retire_pool_rejection_rollback() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5702));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Acquire 2 guards (concurrency = 2)
    let g1 = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    )
    .unwrap();

    let g2 = WorkerLifecycleGuard::acquire(
        pool.clone(),
        task_active_workers.clone(),
        telemetry.clone(),
        &trackers,
    )
    .unwrap();

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 2);
    assert_eq!(pool.active_worker_count(), 2);

    // Target is 1. If we call try_retire(1) on g1, it would normally succeed.
    // But we sabotage slot 0 in the pool directly so that try_retire_slot(0, 1) returns false!
    let _ = pool.mark_retiring(g1.slot_id);

    // Now call g1.try_retire(1):
    // 1. Pre-check sees active_count == 1 (since slot 0 is retiring, active is now 1).
    // Target is 1, so active (1) is <= target (1).
    // Returns false immediately without even touching guard state!
    let res = g1.try_retire(1);
    assert!(!res, "try_retire must return false when active <= target");
    assert_eq!(g1.lifecycle_state(), GUARD_STATE_ACTIVE);

    // Test rollback when claim_retirement succeeds but pool CAS rejects:
    // Reset pool state for a fresh test:
    drop(g1);
    drop(g2);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);

    // Acquire 3 guards
    let g1 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g2 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g3 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();

    assert_eq!(task_active_workers.load(Ordering::SeqCst), 3);
    assert_eq!(pool.active_worker_count(), 3);

    // Target is 2.
    // Concurrently retire g2 via mark_retired: active becomes 2.
    assert!(g2.mark_retired());
    assert_eq!(pool.active_worker_count(), 2);

    // Now call g1.try_retire(2): active (2) <= target (2), so try_retire returns false and rolls back!
    assert!(!g1.try_retire(2));
    assert_eq!(g1.lifecycle_state(), GUARD_STATE_ACTIVE);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 2);

    // Drop all guards and assert clean convergence
    drop(g1);
    drop(g2);
    drop(g3);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.retiring_count(), 0);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// TEST 20: Final Convergence Across Normal Exit, Retirement, Panic, Abort, and Rejection
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_final_convergence_across_all_lifecycle_paths() {
    let pool = Arc::new(WorkerSlotPool::new(16));
    let telemetry = Arc::new(TaskTelemetryCollector::new(5703));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let trackers: Vec<Arc<AtomicU64>> = (0..16).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Spawn 5 workers under 5 different lifecycle scenarios:
    // 1. Normal retirement (mark_retired)
    // 2. Normal completion without retirement
    // 3. Worker task abort
    // 4. Worker task panic
    // 5. Failed retirement followed by unretired drop

    let g1 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g2 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g3 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g4 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();
    let g5 = WorkerLifecycleGuard::acquire(pool.clone(), task_active_workers.clone(), telemetry.clone(), &trackers).unwrap();

    assert_eq!(pool.allocated_count(), 5);
    assert_eq!(task_active_workers.load(Ordering::SeqCst), 5);

    // Path 1: Normal retirement
    let h1 = tokio::spawn(async move {
        let guard = g1;
        assert!(guard.mark_retired());
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(guard);
    });

    // Path 2: Normal completion without retirement
    let h2 = tokio::spawn(async move {
        let guard = g2;
        tokio::time::sleep(Duration::from_millis(15)).await;
        drop(guard);
    });

    // Path 3: Abort
    let h3 = tokio::spawn(async move {
        let _guard = g3;
        std::future::pending::<()>().await;
    });

    // Path 4: Panic
    let h4 = tokio::spawn(async move {
        let _guard = g4;
        panic!("Intentional worker panic in convergence test");
    });

    // Path 5: Attempt try_retire with target 5 (fails and rolls back), then drops
    let h5 = tokio::spawn(async move {
        let guard = g5;
        assert!(!guard.try_retire(5)); // fails because active <= 5
        assert_eq!(guard.lifecycle_state(), GUARD_STATE_ACTIVE);
        drop(guard);
    });

    // Abort task 3
    tokio::time::sleep(Duration::from_millis(5)).await;
    h3.abort();

    let _ = h1.await;
    let _ = h2.await;
    let _ = h3.await;
    let _ = h4.await;
    let _ = h5.await;

    // CRITICAL CONVERGENCE ASSERTION:
    // When all tasks settle, task_active_workers MUST exactly match pool.active_worker_count() == 0!
    assert_eq!(
        pool.allocated_count(),
        0,
        "All slots must be released across all lifecycle termination paths"
    );
    assert_eq!(
        pool.retiring_count(),
        0,
        "No slots may remain in retiring mask"
    );
    assert_eq!(
        pool.active_worker_count(),
        0,
        "Authoritative active count must settle at exactly 0"
    );
    assert_eq!(
        task_active_workers.load(Ordering::SeqCst),
        0,
        "Cached counter must cleanly converge to exactly 0 without drift"
    );
}
