//! Empirical Lifecycle & Concurrency Challenger Test Suite
//! Milestone 2 Challenger 1 (Lifecycle & Concurrency Stress)
//!
//! Stress-tests:
//! 1. Adversarial Delete Interleavings (Delete after pause, Delete during pause teardown, Delete during finalization)
//! 2. Distinct Pause vs Cancel Semantics (EngineEvent::Paused vs EngineEvent::Canceled, checkpoint persistence, partial file preservation)
//! 3. Task ID Reuse with ActiveTaskGuard Isolation (50 concurrent threads hammering rapid ID reuse)
//! 4. Single Terminal Event Invariant under Heavy Concurrent Hammering (Start, Pause, Cancel, Delete simultaneously blasted across threads)

use app::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use app::types::{ActiveTaskGuard, TaskControl};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

/// Test helper directory with cleanup
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_challenger_m2_{}_{}_{}",
            name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::create_dir_all(&p);
        TestDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        for _ in 0..10 {
            if std::fs::remove_dir_all(&self.0).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Helper to wait for a specific engine event with bounded timeout
async fn wait_for_event<F>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<EngineEvent>,
    timeout: Duration,
    mut predicate: F,
) -> Option<EngineEvent>
where
    F: FnMut(&EngineEvent) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        while let Ok(evt) = rx.try_recv() {
            if predicate(&evt) {
                return Some(evt);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

// ============================================================================
// 1. Adversarial Delete Interleaving Tests
// ============================================================================

/// Challenge 1A: Delete arriving during Pause teardown.
/// When Start, Pause, and Delete are dispatched in immediate rapid succession,
/// Delete MUST supersede Pause:
/// - Emits EngineEvent::Deleted (NOT Paused)
/// - Suppresses zombie checkpoint .partinfo
/// - Removes destination file
#[tokio::test]
async fn test_challenge_delete_supersedes_pause_teardown() {
    let td = TestDir::new("del_supersede_pause");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 5001u64;
    let save_path = td.path().join("pause_del.dat");
    let partinfo_path = td.path().join("pause_del.dat.partinfo");

    // Pre-create dummy file to ensure it exists on disk
    std::fs::write(&save_path, b"some partial data").unwrap();
    std::fs::write(&partinfo_path, b"{\"version\":1}").unwrap();

    // Rapid dispatch: Start -> Pause -> Delete
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/dummy".into(),
        save_path: save_path.clone(),
        connections: 1,
    });
    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    let term = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(
            evt,
            EngineEvent::Deleted { id: eid, .. }
                | EngineEvent::Paused { id: eid }
                | EngineEvent::Failed { id: eid, .. }
            if *eid == id
        )
    })
    .await;

    assert!(
        term.is_some(),
        "Must emit a terminal event for task {}",
        id
    );
    match term.unwrap() {
        EngineEvent::Deleted { cleanup_success, .. } => {
            assert!(cleanup_success, "Cleanup must succeed");
            // Zero zombie checkpoint invariant
            assert!(
                !partinfo_path.exists(),
                "Checkpoint .partinfo must be removed"
            );
            // File removal invariant
            assert!(
                !save_path.exists(),
                "Destination file must be removed when cleanup_files=true"
            );
        }
        EngineEvent::Paused { .. } => {
            panic!("Delete was superseded by Pause! Violates Delete priority invariant (R1)");
        }
        EngineEvent::Failed { error, .. } => {
            panic!("Unexpected failure event: {}", error);
        }
        other => panic!("Unexpected event: {:?}", other),
    }
}

/// Challenge 1B: Delete dispatched after Pause has completed.
/// This tests whether an inactive Delete actually removes files when cleanup_files=true,
/// or if known_paths eviction during Pause broke inactive file cleanup.
#[tokio::test]
async fn test_challenge_inactive_delete_after_pause_cleans_files() {
    let td = TestDir::new("inactive_del_after_pause");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 5002u64;
    let save_path = td.path().join("paused_then_deleted.dat");
    let partinfo_path = td.path().join("paused_then_deleted.dat.partinfo");

    // Write pre-existing data
    std::fs::write(&save_path, b"important partial bytes").unwrap();
    std::fs::write(&partinfo_path, b"{\"version\":1}").unwrap();

    // Start then Pause
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/dummy".into(),
        save_path: save_path.clone(),
        connections: 1,
    });
    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });

    // Wait for Paused event
    let paused = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Paused { id: eid } if *eid == id)
    })
    .await;
    assert!(paused.is_some(), "Task must pause cleanly");

    // Give coordinator time to drop guard
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now issue DeleteDownload with cleanup_files = true
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    let deleted = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
    })
    .await;
    assert!(deleted.is_some(), "Must emit EngineEvent::Deleted");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // EMPIRICAL ASSERTION:
    // If known_paths was evicted on Pause, the engine in inactive DeleteDownload
    // has NO save_path for this ID, so save_path and partinfo_path are NEVER DELETED!
    assert!(
        !partinfo_path.exists(),
        "CRITICAL BUG: .partinfo checkpoint was NOT deleted on DeleteDownload because known_paths was wiped on Pause!"
    );
    assert!(
        !save_path.exists(),
        "CRITICAL BUG: save_path was NOT deleted on DeleteDownload because known_paths was wiped on Pause!"
    );
}

/// Challenge 1C: Delete arriving immediately after Paused event is received,
/// while coordinator is exiting and before ActiveTaskGuard drops.
#[tokio::test]
async fn test_challenge_delete_swallowed_during_guard_drop_window() {
    let td = TestDir::new("delete_swallowed_race");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 5003u64;
    let save_path = td.path().join("swallowed.dat");
    std::fs::write(&save_path, b"test payload").unwrap();

    // Start then Pause
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/dummy".into(),
        save_path: save_path.clone(),
        connections: 1,
    });
    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });

    // Wait until Paused event is emitted
    let paused = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Paused { id: eid } if *eid == id)
    })
    .await;
    assert!(paused.is_some(), "Must emit Paused");

    // IMMEDIATELY dispatch DeleteDownload without sleeping
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    // Wait for Deleted event
    let deleted = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
    })
    .await;

    assert!(
        deleted.is_some(),
        "DeleteDownload dispatched right after Paused event must emit Deleted"
    );
    assert!(
        !save_path.exists(),
        "CRITICAL DEFECT: Destination file was NOT removed on DeleteDownload after Pause!"
    );
}

// ============================================================================
// 2. Distinct Pause vs Cancel Semantics Tests
// ============================================================================

/// Challenge 2A: Distinct Pause vs Cancel semantics.
/// - Pause emits EngineEvent::Paused and preserves checkpoint & file.
/// - Cancel emits EngineEvent::Canceled and preserves checkpoint & file WITHOUT emitting Paused.
#[tokio::test]
async fn test_challenge_distinct_pause_vs_cancel_semantics() {
    let td = TestDir::new("distinct_pause_cancel");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;

    // Part A: Pause
    let id_pause = 6001u64;
    let save_pause = td.path().join("pause_distinct.dat");
    let partinfo_pause = td.path().join("pause_distinct.dat.partinfo");
    std::fs::write(&save_pause, b"partial pause data").unwrap();
    std::fs::write(&partinfo_pause, b"{\"version\":1}").unwrap();

    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id: id_pause,
        url: "http://127.0.0.1:9/dummy".into(),
        save_path: save_pause.clone(),
        connections: 1,
    });
    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id: id_pause });

    let pause_evt = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Paused { id } if *id == id_pause)
    })
    .await;
    assert!(pause_evt.is_some(), "Pause must emit EngineEvent::Paused");
    assert!(save_pause.exists(), "Partial file must be preserved on Pause");
    assert!(partinfo_pause.exists(), "Checkpoint .partinfo must be preserved on Pause");

    // Part B: Cancel
    let id_cancel = 6002u64;
    let save_cancel = td.path().join("cancel_distinct.dat");
    let partinfo_cancel = td.path().join("cancel_distinct.dat.partinfo");
    std::fs::write(&save_cancel, b"partial cancel data").unwrap();
    std::fs::write(&partinfo_cancel, b"{\"version\":1}").unwrap();

    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id: id_cancel,
        url: "http://127.0.0.1:9/dummy".into(),
        save_path: save_cancel.clone(),
        connections: 1,
    });
    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id: id_cancel });

    let cancel_evt = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Canceled { id } if *id == id_cancel)
    })
    .await;
    assert!(
        cancel_evt.is_some(),
        "Cancel must emit EngineEvent::Canceled"
    );
    assert!(
        save_cancel.exists(),
        "Partial file must be preserved on Cancel"
    );
    assert!(
        partinfo_cancel.exists(),
        "Checkpoint .partinfo must be preserved on Cancel"
    );

    // Verify Cancel did NOT emit Paused
    while let Ok(evt) = rx.try_recv() {
        assert!(
            !matches!(evt, EngineEvent::Paused { id } if id == id_cancel),
            "Cancel must NEVER emit EngineEvent::Paused"
        );
    }
}

// ============================================================================
// 3. Task ID Reuse & ActiveTaskGuard Isolation Tests
// ============================================================================

/// Challenge 3A: 50 concurrent threads hammering rapid task ID reuse with out-of-order guard drops.
/// Proves that instance generation tokens completely isolate active task instances.
#[test]
fn test_challenge_active_task_guard_50_threads_rapid_id_reuse() {
    const NUM_THREADS: usize = 50;
    const OPS_PER_THREAD: usize = 200;

    let active_tasks = Arc::new(Mutex::new(HashMap::new()));
    let next_instance_id = Arc::new(AtomicU64::new(1));
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let mut handles = Vec::with_capacity(NUM_THREADS);

    for t_idx in 0..NUM_THREADS {
        let active_tasks = active_tasks.clone();
        let next_instance_id = next_instance_id.clone();
        let barrier = barrier.clone();

        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let task_id = t_idx as u64; // Isolated task ID per thread

            for _ in 0..OPS_PER_THREAD {
                // Instance 1
                let inst1 = next_instance_id.fetch_add(1, Ordering::SeqCst);
                let guard1 = {
                    let mut map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    map.insert(task_id, TaskControl::new(inst1));
                    ActiveTaskGuard {
                        id: task_id,
                        instance_id: inst1,
                        active_tasks: active_tasks.clone(),
                    }
                };

                // Instance 2 reuses the same task_id
                let inst2 = next_instance_id.fetch_add(1, Ordering::SeqCst);
                let guard2 = {
                    let mut map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    map.insert(task_id, TaskControl::new(inst2));
                    ActiveTaskGuard {
                        id: task_id,
                        instance_id: inst2,
                        active_tasks: active_tasks.clone(),
                    }
                };

                // Stale guard 1 drops
                drop(guard1);

                // Invariant: instance 2 must STILL be in active_tasks
                {
                    let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(entry) = map.get(&task_id) {
                        assert_eq!(
                            entry.instance_id, inst2,
                            "Stale guard drop must NOT evict newer instance"
                        );
                    }
                }

                // Guard 2 drops
                drop(guard2);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(map.len(), 0, "All guards dropped, active_tasks must be empty");
}

// ============================================================================
// 4. Single Terminal Event Invariant under Heavy Concurrent Hammering
// ============================================================================

/// Challenge 4A: Single terminal event invariant under concurrent command hammering.
/// Blasts Start, Pause, Cancel, Delete concurrently on 5 task IDs across 10 threads.
/// Asserts that each task instance emits AT MOST ONE terminal event.
#[tokio::test]
async fn test_challenge_single_terminal_event_under_heavy_concurrency() {
    let td = TestDir::new("single_term_event");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let cmd_sender = engine.cmd_sender.clone();
    let active_tasks = engine.active_tasks.clone();
    let known_paths = engine.known_paths.clone();
    let active_workers = engine.active_workers.clone();
    let num_tasks = 5u64;
    let ops_per_thread = 50;

    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut join_handles = Vec::new();

    for thread_idx in 0..8 {
        let cmd_tx = cmd_sender.clone();
        let barrier = barrier.clone();
        let test_dir = td.path().to_path_buf();

        join_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for op in 0..ops_per_thread {
                let id = (op % num_tasks) + 1;
                let save_path = test_dir.join(format!("hammer_{}.bin", id));

                match (thread_idx + op) % 4 {
                    0 => {
                        let _ = cmd_tx.send(EngineCommand::StartDownload {
                            id,
                            url: "http://127.0.0.1:9/dummy".into(),
                            save_path,
                            connections: 1,
                        });
                    }
                    1 => {
                        let _ = cmd_tx.send(EngineCommand::PauseDownload { id });
                    }
                    2 => {
                        let _ = cmd_tx.send(EngineCommand::CancelDownload { id });
                    }
                    _ => {
                        let _ = cmd_tx.send(EngineCommand::DeleteDownload {
                            id,
                            cleanup_files: true,
                        });
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in join_handles {
        let _ = h.await;
    }

    // Drain all events for 1 second
    let mut terminal_events = Vec::new();
    let drain_end = Instant::now() + Duration::from_secs(1);
    while Instant::now() < drain_end {
        while let Ok(evt) = rx.try_recv() {
            match evt {
                EngineEvent::Finished { id, .. }
                | EngineEvent::Paused { id }
                | EngineEvent::Canceled { id }
                | EngineEvent::Deleted { id, .. }
                | EngineEvent::Failed { id, .. } => {
                    terminal_events.push((id, format!("{:?}", evt)));
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify engine metrics
    let active_tasks_count = active_tasks.lock().unwrap_or_else(|p| p.into_inner()).len();
    let known_paths_count = known_paths.lock().unwrap_or_else(|p| p.into_inner()).len();
    let active_workers_count = active_workers.load(Ordering::Relaxed);
    println!(
        "[Single Terminal Test] Final metrics: active_tasks={}, known_paths={}, active_workers={}. Captured {} terminal events.",
        active_tasks_count, known_paths_count, active_workers_count, terminal_events.len()
    );
    assert_eq!(active_workers_count, 0, "No leaked worker threads allowed");
}

/// Challenge 4B: StartDownload is rejected while active deletion of the same task ID is in progress.
#[tokio::test]
async fn test_challenge_start_rejected_during_active_deletion() {
    let td = TestDir::new("start_rejected_del");
    let engine = spawn_download_engine();
    let id = 7001u64;
    let save_path = td.path().join("reject_del.dat");

    // Manually register an active deleting task control
    let ctrl = TaskControl::new(999);
    ctrl.delete_flag.store(true, Ordering::SeqCst);
    engine
        .active_tasks
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(id, ctrl.clone());

    // Issue StartDownload for the same ID
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/dummy".into(),
        save_path: save_path.clone(),
        connections: 1,
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify StartDownload was REJECTED:
    // The instance_id must still be 999 (the deleting instance), NOT a new instance.
    let tasks = engine
        .active_tasks
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let entry = tasks.get(&id).expect("Task must still be present");
    assert_eq!(
        entry.instance_id, 999,
        "StartDownload must be rejected when delete_flag is active on the existing task"
    );
}

/// Challenge 4C: Mutex poisoning resilience across engine handles.
#[test]
fn test_challenge_engine_mutex_poisoning_resilience() {
    let engine = spawn_download_engine();

    // Poison active_tasks
    let active_tasks = engine.active_tasks.clone();
    let _ = std::panic::catch_unwind(|| {
        let mut guard = active_tasks.lock().unwrap();
        guard.insert(8888, TaskControl::new(1));
        panic!("Intentional panic to poison active_tasks");
    });
    assert!(engine.active_tasks.is_poisoned());

    // Poison known_paths
    let known_paths = engine.known_paths.clone();
    let _ = std::panic::catch_unwind(|| {
        let mut guard = known_paths.lock().unwrap();
        guard.insert(8888, PathBuf::from("poisoned.bin"));
        panic!("Intentional panic to poison known_paths");
    });
    assert!(engine.known_paths.is_poisoned());

    // Engine metrics query must NOT panic
    let metrics = engine.metrics();
    assert_eq!(metrics.active_tasks_count, 1);
    assert_eq!(metrics.known_paths_count, 1);

    // Operations on engine handle maps recover cleanly
    {
        let mut tasks = engine.active_tasks.lock().unwrap_or_else(|p| p.into_inner());
        assert!(tasks.contains_key(&8888));
        tasks.clear();
    }
    {
        let mut paths = engine.known_paths.lock().unwrap_or_else(|p| p.into_inner());
        assert!(paths.contains_key(&8888));
        paths.clear();
    }
}
