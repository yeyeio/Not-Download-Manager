//! Empirical Concurrency Primitives Stress Harness
//! Milestone 1 Challenger 2
//!
//! Stress-tests:
//! 1. ActiveTaskGuard rapid ID reuse isolation (100 threads)
//! 2. Mutex poisoning resilience (ActiveTaskGuard::drop recovery via unwrap_or_else)
//! 3. TaskControl mark_terminal_emitted atomic event race (10 & 100 threads)
//! 4. Edge cases & boundary conditions (u64::MAX, out-of-order drops, non-existent keys)

use not_download_manager::types::{ActiveTaskGuard, TaskControl};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

// ============================================================================
// Challenge 1: ActiveTaskGuard Rapid Task ID Reuse Scenario (100 Threads)
// ============================================================================

/// Scenario 1A: 100 concurrent threads hammering a shared active_tasks map,
/// each thread rapidly reusing task IDs, creating older and newer instances,
/// delaying the drop of older guards, and asserting that the newer instance is NEVER evicted.
#[test]
fn test_challenge_active_task_guard_rapid_id_reuse_100_threads_hammer() {
    const NUM_THREADS: usize = 100;
    const OPS_PER_THREAD: usize = 1000;

    let active_tasks = Arc::new(Mutex::new(HashMap::new()));
    let next_instance_id = Arc::new(AtomicU64::new(1));
    let start_barrier = Arc::new(Barrier::new(NUM_THREADS));

    let mut handles = Vec::with_capacity(NUM_THREADS);

    for thread_idx in 0..NUM_THREADS {
        let active_tasks = active_tasks.clone();
        let next_instance_id = next_instance_id.clone();
        let start_barrier = start_barrier.clone();

        handles.push(thread::spawn(move || {
            start_barrier.wait();
            let task_id = thread_idx as u64;

            for _iter in 0..OPS_PER_THREAD {
                // Step 1: Create instance 1 for this task ID
                let inst_1 = next_instance_id.fetch_add(1, Ordering::SeqCst);
                active_tasks
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(task_id, TaskControl::new(inst_1));
                let guard_1 = ActiveTaskGuard {
                    id: task_id,
                    instance_id: inst_1,
                    active_tasks: active_tasks.clone(),
                };

                // Step 2: Rapidly reuse task ID with instance 2
                let inst_2 = next_instance_id.fetch_add(1, Ordering::SeqCst);
                active_tasks
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(task_id, TaskControl::new(inst_2));
                let guard_2 = ActiveTaskGuard {
                    id: task_id,
                    instance_id: inst_2,
                    active_tasks: active_tasks.clone(),
                };

                // Step 3: Delayed drop of older guard 1
                drop(guard_1);

                // Step 4: EMPIRICAL ASSERTION:
                // Instance 2 must NOT have been evicted by older guard 1's drop!
                {
                    let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    let entry = map
                        .get(&task_id)
                        .expect("Task must still be present in active_tasks after stale guard drop");
                    assert_eq!(
                        entry.instance_id, inst_2,
                        "Active task instance must match newer instance 2, never evicted by guard 1"
                    );
                }

                // Step 5: Drop matching guard 2
                drop(guard_2);

                // Step 6: Verify clean eviction upon matching guard drop
                {
                    let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    assert!(
                        !map.contains_key(&task_id),
                        "Matching guard drop must cleanly evict the task entry"
                    );
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("Thread should not panic");
    }

    // Verify map is completely empty after all 100,000 operations
    let final_map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        final_map.is_empty(),
        "active_tasks map must be 100% empty after all guards drop"
    );
}

/// Scenario 1B: Shared Reused ID Pool (10 Task IDs) across 100 threads.
/// Multiple threads interleave creations and drops on the exact same task IDs.
#[test]
fn test_challenge_active_task_guard_shared_reused_pool_100_threads() {
    const NUM_THREADS: usize = 100;
    const NUM_SHARED_TASKS: usize = 10;
    const OPS_PER_THREAD: usize = 300;

    struct TaskSlot {
        curr_inst: u64,
        curr_guard: Option<ActiveTaskGuard>,
        older_guards: Vec<ActiveTaskGuard>,
    }

    let active_tasks = Arc::new(Mutex::new(HashMap::new()));
    let next_instance_id = Arc::new(AtomicU64::new(1));
    let start_barrier = Arc::new(Barrier::new(NUM_THREADS));

    // Per-slot mutex to simulate download coordinator sequencing per task ID
    let slots: Arc<Vec<Mutex<TaskSlot>>> = Arc::new(
        (0..NUM_SHARED_TASKS)
            .map(|_| {
                Mutex::new(TaskSlot {
                    curr_inst: 0,
                    curr_guard: None,
                    older_guards: Vec::new(),
                })
            })
            .collect(),
    );

    let mut handles = Vec::with_capacity(NUM_THREADS);

    for thread_idx in 0..NUM_THREADS {
        let active_tasks = active_tasks.clone();
        let next_instance_id = next_instance_id.clone();
        let start_barrier = start_barrier.clone();
        let slots = slots.clone();

        handles.push(thread::spawn(move || {
            start_barrier.wait();

            for iter in 0..OPS_PER_THREAD {
                let slot_idx = (thread_idx * 13 + iter) % NUM_SHARED_TASKS;
                let task_id = slot_idx as u64;

                let mut slot = slots[slot_idx].lock().unwrap();

                // Allocate next monotonic instance
                let inst = next_instance_id.fetch_add(1, Ordering::SeqCst);
                active_tasks
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(task_id, TaskControl::new(inst));

                let new_guard = ActiveTaskGuard {
                    id: task_id,
                    instance_id: inst,
                    active_tasks: active_tasks.clone(),
                };

                // Move previous curr_guard to older_guards
                if let Some(old) = slot.curr_guard.take() {
                    slot.older_guards.push(old);
                }
                slot.curr_inst = inst;
                slot.curr_guard = Some(new_guard);

                // Now drop an older guard if we have any accumulated
                if !slot.older_guards.is_empty() {
                    let old_guard = slot.older_guards.remove(0);
                    let old_inst = old_guard.instance_id;
                    assert!(old_inst < slot.curr_inst);

                    // Drop older guard
                    drop(old_guard);

                    // Invariant check: current active instance must STILL be in active_tasks!
                    let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    let entry = map.get(&task_id).unwrap_or_else(|| {
                        panic!(
                            "Task {} was evicted by older guard drop (old_inst={}, curr_inst={})",
                            task_id, old_inst, slot.curr_inst
                        )
                    });
                    assert_eq!(
                        entry.instance_id, slot.curr_inst,
                        "Instance in map must remain curr_inst"
                    );
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("Thread should not panic");
    }

    // Clean up all remaining guards in slots
    for slot_mutex in slots.iter() {
        let mut slot = slot_mutex.lock().unwrap();
        slot.older_guards.clear();
        slot.curr_guard = None;
    }

    // Now active_tasks must be completely empty
    let final_map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        final_map.is_empty(),
        "All shared slots cleaned up, active_tasks must be empty (len was {})",
        final_map.len()
    );
}

/// Stress test with out-of-order drop interleaving across multiple threads
#[test]
fn test_challenge_active_task_guard_out_of_order_drops_interleaving() {
    let active_tasks = Arc::new(Mutex::new(HashMap::new()));
    let task_id = 999u64;

    // We create a chain of 100 successive instances on the SAME task_id
    let mut guards = Vec::new();
    for inst in 1..=100u64 {
        {
            let mut map = active_tasks.lock().unwrap();
            map.insert(task_id, TaskControl::new(inst));
        }
        guards.push(ActiveTaskGuard {
            id: task_id,
            instance_id: inst,
            active_tasks: active_tasks.clone(),
        });
    }

    // Map currently holds instance 100
    assert_eq!(
        active_tasks.lock().unwrap().get(&task_id).unwrap().instance_id,
        100
    );

    // Drop instances 1..99 in arbitrary / reverse order:
    // None of these drops should remove instance 100!
    for i in (0..99).rev() {
        drop(guards.remove(i));
        let map = active_tasks.lock().unwrap();
        assert!(
            map.contains_key(&task_id),
            "Task must still exist after dropping stale guard"
        );
        assert_eq!(
            map.get(&task_id).unwrap().instance_id,
            100,
            "Task instance in map must remain 100"
        );
    }

    // Finally, drop the matching guard 100
    assert_eq!(guards.len(), 1);
    drop(guards.pop());

    // Map must now be empty for this task
    assert!(
        active_tasks.lock().unwrap().get(&task_id).is_none(),
        "Dropping the matching guard (instance 100) must evict the task"
    );
}

// ============================================================================
// Challenge 2: Mutex Poisoning Resilience
// ============================================================================

#[test]
fn test_challenge_active_task_guard_mutex_poisoning_recovery() {
    let active_tasks = Arc::new(Mutex::new(HashMap::new()));
    let task_id_1 = 101u64;
    let task_id_2 = 102u64;

    // Insert task 1 (inst 10) and task 2 (inst 20)
    {
        let mut map = active_tasks.lock().unwrap();
        map.insert(task_id_1, TaskControl::new(10));
        map.insert(task_id_2, TaskControl::new(20));
    }

    // Create guards for both tasks
    let guard_1 = ActiveTaskGuard {
        id: task_id_1,
        instance_id: 10,
        active_tasks: active_tasks.clone(),
    };
    let guard_2_stale = ActiveTaskGuard {
        id: task_id_2,
        instance_id: 19, // Stale! Current is 20
        active_tasks: active_tasks.clone(),
    };

    // Deliberately poison the mutex via thread panic
    let tasks_for_poison = active_tasks.clone();
    let poison_result = thread::spawn(move || {
        let _guard = tasks_for_poison.lock().unwrap();
        panic!("Intentional panic to poison the active_tasks mutex");
    })
    .join();

    assert!(poison_result.is_err(), "Thread must have panicked");
    assert!(
        active_tasks.is_poisoned(),
        "active_tasks Mutex must now be poisoned"
    );

    // Dropping guard_2_stale on a POISONED mutex must NOT panic,
    // and must NOT evict task_id_2 because instance_id (19 != 20)
    let drop_stale_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(guard_2_stale);
    }));
    assert!(
        drop_stale_result.is_ok(),
        "Dropping stale guard on poisoned mutex must not panic"
    );

    // Verify task_id_2 is still intact despite poisoned mutex
    {
        let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            map.contains_key(&task_id_2),
            "Task 2 must not be evicted by stale guard on poisoned mutex"
        );
        assert_eq!(map.get(&task_id_2).unwrap().instance_id, 20);
    }

    // Dropping guard_1 (matching instance_id 10) on POISONED mutex must NOT panic,
    // and MUST successfully remove task_id_1
    let drop_matching_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(guard_1);
    }));
    assert!(
        drop_matching_result.is_ok(),
        "Dropping matching guard on poisoned mutex must not panic"
    );

    // Verify task_id_1 was cleanly removed, and task_id_2 remains
    {
        let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            !map.contains_key(&task_id_1),
            "Matching guard drop on poisoned mutex must evict task 1"
        );
        assert!(
            map.contains_key(&task_id_2),
            "Task 2 must still remain in map"
        );
    }
}

/// Concurrent frenzy: 50 threads dropping guards while the mutex is continuously poisoned
#[test]
fn test_challenge_active_task_guard_concurrent_poisoning_frenzy() {
    const NUM_TASKS: usize = 50;
    let active_tasks = Arc::new(Mutex::new(HashMap::new()));

    // Populate 50 tasks
    {
        let mut map = active_tasks.lock().unwrap();
        for id in 0..NUM_TASKS as u64 {
            map.insert(id, TaskControl::new(1));
        }
    }

    // Create 50 matching guards
    let guards: Vec<_> = (0..NUM_TASKS as u64)
        .map(|id| {
            Some(ActiveTaskGuard {
                id,
                instance_id: 1,
                active_tasks: active_tasks.clone(),
            })
        })
        .collect();
    let guards = Arc::new(Mutex::new(guards));

    let barrier = Arc::new(Barrier::new(NUM_TASKS + 1));
    let mut handles = Vec::new();

    // Spawn 1 poisoner thread that locks and panics
    let tasks_poison = active_tasks.clone();
    let barrier_poison = barrier.clone();
    handles.push(thread::spawn(move || {
        barrier_poison.wait();
        let _lock = tasks_poison.lock().unwrap_or_else(|p| p.into_inner());
        panic!("Mid-frenzy deliberate poison panic!");
    }));

    // Spawn 50 threads each dropping one guard
    for i in 0..NUM_TASKS {
        let guards = guards.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            // Retrieve and drop guard
            let guard = {
                let mut list = guards.lock().unwrap_or_else(|p| p.into_inner());
                list[i].take()
            };
            if let Some(g) = guard {
                drop(g);
            }
        }));
    }

    // Join all threads; poisoner panicked, but all dropper threads MUST succeed
    let mut drop_panics = 0;
    for (idx, h) in handles.into_iter().enumerate() {
        let res = h.join();
        if idx == 0 {
            assert!(res.is_err(), "Poisoner thread was expected to panic");
        } else if res.is_err() {
            drop_panics += 1;
        }
    }

    assert_eq!(
        drop_panics, 0,
        "No dropper thread should panic on poisoned mutex"
    );

    // All 50 tasks must be cleanly removed; 0 leaked entries
    let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        map.is_empty(),
        "All 50 tasks should be cleanly evicted; map len was {}",
        map.len()
    );
}

// ============================================================================
// Challenge 3: TaskControl mark_terminal_emitted Atomic Event Race
// ============================================================================

#[test]
fn test_challenge_task_control_terminal_emitted_10_threads_race() {
    const NUM_THREADS: usize = 10;
    const NUM_ROUNDS: usize = 2000;

    for _round in 0..NUM_ROUNDS {
        let ctrl = TaskControl::new(100);
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let winners = Arc::new(AtomicUsize::new(0));
        let losers = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(NUM_THREADS);

        for _ in 0..NUM_THREADS {
            let ctrl = ctrl.clone();
            let barrier = barrier.clone();
            let winners = winners.clone();
            let losers = losers.clone();

            handles.push(thread::spawn(move || {
                barrier.wait();
                if ctrl.mark_terminal_emitted() {
                    winners.fetch_add(1, Ordering::SeqCst);
                } else {
                    losers.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            winners.load(Ordering::SeqCst),
            1,
            "Exactly ONE thread must receive true from mark_terminal_emitted"
        );
        assert_eq!(
            losers.load(Ordering::SeqCst),
            NUM_THREADS - 1,
            "Exactly {} threads must receive false",
            NUM_THREADS - 1
        );
    }
}

#[test]
fn test_challenge_task_control_terminal_emitted_100_threads_stampede() {
    const NUM_THREADS: usize = 100;
    const NUM_ROUNDS: usize = 200;

    for _round in 0..NUM_ROUNDS {
        let ctrl = TaskControl::new(555);
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let winners = Arc::new(AtomicUsize::new(0));
        let losers = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(NUM_THREADS);

        for _ in 0..NUM_THREADS {
            let ctrl = ctrl.clone();
            let barrier = barrier.clone();
            let winners = winners.clone();
            let losers = losers.clone();

            handles.push(thread::spawn(move || {
                barrier.wait();
                if ctrl.mark_terminal_emitted() {
                    winners.fetch_add(1, Ordering::SeqCst);
                } else {
                    losers.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            winners.load(Ordering::SeqCst),
            1,
            "Stampede: exactly ONE thread must win out of 100"
        );
        assert_eq!(
            losers.load(Ordering::SeqCst),
            99,
            "Stampede: exactly 99 threads must lose out of 100"
        );
    }
}

// ============================================================================
// Challenge 4: Adversarial Edge Cases & Boundary Conditions
// ============================================================================

#[test]
fn test_challenge_active_task_guard_edge_cases() {
    let active_tasks = Arc::new(Mutex::new(HashMap::new()));

    // Case 1: Drop guard for a key that does NOT exist in map
    {
        let guard_missing = ActiveTaskGuard {
            id: 12345,
            instance_id: 1,
            active_tasks: active_tasks.clone(),
        };
        drop(guard_missing); // Must not panic or alter anything
    }
    assert!(active_tasks.lock().unwrap().is_empty());

    // Case 2: instance_id = 0 (boundary)
    {
        {
            let mut map = active_tasks.lock().unwrap();
            map.insert(0, TaskControl::new(0));
        }
        let guard_zero = ActiveTaskGuard {
            id: 0,
            instance_id: 0,
            active_tasks: active_tasks.clone(),
        };
        drop(guard_zero);
        assert!(active_tasks.lock().unwrap().is_empty());
    }

    // Case 3: instance_id = u64::MAX (boundary)
    {
        {
            let mut map = active_tasks.lock().unwrap();
            map.insert(u64::MAX, TaskControl::new(u64::MAX));
        }
        let guard_max = ActiveTaskGuard {
            id: u64::MAX,
            instance_id: u64::MAX,
            active_tasks: active_tasks.clone(),
        };
        drop(guard_max);
        assert!(active_tasks.lock().unwrap().is_empty());
    }

    // Case 4: Guard with newer instance_id drops when map still holds an older instance_id
    // (e.g. if registration was aborted or out of order)
    {
        {
            let mut map = active_tasks.lock().unwrap();
            map.insert(77, TaskControl::new(10));
        }
        let guard_newer = ActiveTaskGuard {
            id: 77,
            instance_id: 20, // 20 > 10
            active_tasks: active_tasks.clone(),
        };
        drop(guard_newer);
        // Map must still hold instance 10!
        let map = active_tasks.lock().unwrap();
        assert_eq!(map.get(&77).unwrap().instance_id, 10);
    }
}
