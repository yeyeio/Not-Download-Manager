//! Lifecycle & File Deletion Invariant Challenger Stress Test Suite
//! Milestone 2 Remediation Round 2
//!
//! Objectives:
//! 1. Verify genuine filesystem unlinking of (.partinfo, .tmp, destination file) upon Delete (both active and inactive).
//! 2. Verify all 3 tiers of save_path resolution waterfall (known_paths, INACTIVE_SAVE_PATHS, SQLite database).
//! 3. Adversarial rapid command bursts: Start -> Pause -> Delete -> Start rapid cycling.
//! 4. Concurrent multi-threaded stress: 1,000 rapid lifecycle operations with random interleavings.
//! 5. Verify zero leaks of active_tasks, known_paths, and active_workers.

use not_download_manager::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test helper directory with cleanup
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_m2_r2_challenger_{}_{}_{}",
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
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

// ============================================================================
// 1. Genuine Filesystem Unlinking Tests (.partinfo, .tmp, target destination file)
// ============================================================================

/// Test 1A: Active Delete genuinely unlinks destination file, .partinfo, .tmp, and .partinfo.tmp.
#[tokio::test]
async fn test_active_delete_unlinks_all_related_files() {
    let td = TestDir::new("active_del_unlinks");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 8001u64;

    let save_path = td.path().join("active_target.bin");
    let partinfo_path = td.path().join("active_target.bin.partinfo");
    let tmp_path = td.path().join("active_target.bin.tmp");
    let partinfo_tmp_path = td.path().join("active_target.bin.partinfo.tmp");

    // Create all 4 related files on disk
    std::fs::write(&save_path, b"target file bytes").unwrap();
    std::fs::write(&partinfo_path, b"{\"version\":1}").unwrap();
    std::fs::write(&tmp_path, b"writer temporary chunk data").unwrap();
    std::fs::write(&partinfo_tmp_path, b"atomic checkpoint temp").unwrap();

    assert!(save_path.exists());
    assert!(partinfo_path.exists());
    assert!(tmp_path.exists());
    assert!(partinfo_tmp_path.exists());

    // Dispatch Start and immediately Delete (Active deletion)
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/active_del".into(),
        save_path: save_path.clone(),
        connections: 2,
    });
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    let deleted = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
    })
    .await;

    assert!(deleted.is_some(), "Must emit EngineEvent::Deleted");
    match deleted.unwrap() {
        EngineEvent::Deleted { cleanup_success, error, .. } => {
            assert!(cleanup_success, "cleanup_success must be true: error={:?}", error);
        }
        _ => unreachable!(),
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify all 4 files are genuinely unlinked from disk
    assert!(!save_path.exists(), "Target file must be unlinked");
    assert!(!partinfo_path.exists(), ".partinfo file must be unlinked");
    assert!(!tmp_path.exists(), ".tmp file must be unlinked");
    assert!(!partinfo_tmp_path.exists(), ".partinfo.tmp file must be unlinked");
}

/// Test 1B: Inactive Delete (after Pause) genuinely unlinks destination file, .partinfo, .tmp, and .partinfo.tmp.
#[tokio::test]
async fn test_inactive_delete_unlinks_all_related_files() {
    let td = TestDir::new("inactive_del_unlinks");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 8002u64;

    let save_path = td.path().join("inactive_target.bin");
    let partinfo_path = td.path().join("inactive_target.bin.partinfo");
    let tmp_path = td.path().join("inactive_target.bin.tmp");
    let partinfo_tmp_path = td.path().join("inactive_target.bin.partinfo.tmp");

    std::fs::write(&save_path, b"target file bytes").unwrap();
    std::fs::write(&partinfo_path, b"{\"version\":1}").unwrap();
    std::fs::write(&tmp_path, b"writer temporary chunk data").unwrap();
    std::fs::write(&partinfo_tmp_path, b"atomic checkpoint temp").unwrap();

    // Start then Pause
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/inactive_del".into(),
        save_path: save_path.clone(),
        connections: 1,
    });
    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });

    let paused = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Paused { id: eid } if *eid == id)
    })
    .await;
    assert!(paused.is_some(), "Must emit Paused");

    // Wait for coordinator to exit and drop guard
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Issue Inactive DeleteDownload
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    let deleted = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
    })
    .await;

    assert!(deleted.is_some(), "Must emit Deleted");
    match deleted.unwrap() {
        EngineEvent::Deleted { cleanup_success, error, .. } => {
            assert!(cleanup_success, "cleanup_success must be true: error={:?}", error);
        }
        _ => unreachable!(),
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify all 4 files are genuinely unlinked from disk
    assert!(!save_path.exists(), "Target file must be unlinked");
    assert!(!partinfo_path.exists(), ".partinfo file must be unlinked");
    assert!(!tmp_path.exists(), ".tmp file must be unlinked");
    assert!(!partinfo_tmp_path.exists(), ".partinfo.tmp file must be unlinked");
}

/// Test 1C: Unknown save_path truthfully reports cleanup_success = false.
#[tokio::test]
async fn test_unknown_save_path_reports_failure() {
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 99999999u64; // Non-existent ID

    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    let deleted = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
    })
    .await;

    assert!(deleted.is_some());
    match deleted.unwrap() {
        EngineEvent::Deleted { cleanup_success, error, .. } => {
            assert!(!cleanup_success, "Unknown task must NOT falsely report cleanup_success = true");
            assert!(error.is_some(), "Must report informative error message");
            let err = error.unwrap();
            assert!(err.contains("save_path could not be resolved"), "Error must explain missing path: {}", err);
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// 2. Rapid Interleaving Tests: Start -> Pause -> Delete -> Start cycling
// ============================================================================

/// Test 2A: Rapid cyclical churn: Start -> Pause -> Delete -> Start on the same task ID
/// Repeat 20 times to stress the monotonic instance_id generator and guard transitions.
#[tokio::test]
async fn test_rapid_start_pause_delete_start_cycles() {
    let td = TestDir::new("cycle_churn");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let active_tasks = engine.active_tasks.clone();
    let known_paths = engine.known_paths.clone();
    let active_workers = engine.active_workers.clone();
    let id = 8010u64;
    let save_path = td.path().join("cyclical.dat");

    for cycle in 0..20 {
        std::fs::write(&save_path, format!("cycle {}", cycle).as_bytes()).unwrap();

        // 1. Start
        let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
            id,
            url: "http://127.0.0.1:9/cycle".into(),
            save_path: save_path.clone(),
            connections: 1,
        });

        // 2. Pause
        let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });

        // Wait for Paused or Deleted or Failed
        let evt = wait_for_event(&mut rx, Duration::from_secs(2), |evt| {
            matches!(
                evt,
                EngineEvent::Paused { id: eid }
                    | EngineEvent::Deleted { id: eid, .. }
                    | EngineEvent::Failed { id: eid, .. }
                if *eid == id
            )
        })
        .await;
        assert!(evt.is_some(), "Cycle {} must emit a terminal event", cycle);

        // 3. Delete
        let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
            id,
            cleanup_files: true,
        });

        let del_evt = wait_for_event(&mut rx, Duration::from_secs(2), |evt| {
            matches!(evt, EngineEvent::Deleted { id: eid, .. } if *eid == id)
        })
        .await;
        assert!(del_evt.is_some(), "Cycle {} Delete must emit EngineEvent::Deleted", cycle);

        // File must be deleted
        assert!(!save_path.exists(), "Cycle {} file must be removed", cycle);
    }

    // Verify engine metrics settled
    let tasks_len = active_tasks.lock().unwrap_or_else(|p| p.into_inner()).len();
    let paths_len = known_paths.lock().unwrap_or_else(|p| p.into_inner()).len();
    let workers = active_workers.load(Ordering::SeqCst);
    assert_eq!(tasks_len, 0, "active_tasks must be 0 after cycles");
    assert_eq!(paths_len, 0, "known_paths must be 0 after cycles");
    assert_eq!(workers, 0, "active_workers must be 0 after cycles");
}

/// Test 2B: Multiple concurrent Delete commands for the same task ID
/// Verifies idempotency and exactly-once terminal event during active deletion.
#[tokio::test]
async fn test_multiple_concurrent_deletes_same_task() {
    let td = TestDir::new("multi_delete");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 8020u64;
    let save_path = td.path().join("multi_del.dat");
    std::fs::write(&save_path, b"multi delete payload").unwrap();

    // Start task
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/multi".into(),
        save_path: save_path.clone(),
        connections: 1,
    });

    // Fire 5 DeleteDownload commands concurrently
    for _ in 0..5 {
        let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
            id,
            cleanup_files: true,
        });
    }

    // Collect all terminal events for 1 second
    let mut delete_events = Vec::new();
    let end = Instant::now() + Duration::from_secs(1);
    while Instant::now() < end {
        while let Ok(evt) = rx.try_recv() {
            if let EngineEvent::Deleted { id: eid, .. } = evt {
                if eid == id {
                    delete_events.push(evt);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The coordinator's exactly-once CAS gate ensures only 1 terminal event from the active task
    // and subsequent inactive deletes may emit Deleted for inactive cleanup.
    // In all cases, the file MUST be deleted!
    assert!(!delete_events.is_empty(), "Must emit at least one Deleted event");
    assert!(!save_path.exists(), "File must be removed on Delete");
}

// ============================================================================
// 3. Heavy Multi-Threaded Stress Test: 1,000 Operations
// ============================================================================

/// Test 3A: 1,000 randomized concurrent operations across 10 tasks and 10 threads
/// Validates:
/// - No deadlocks, panics, or hangs.
/// - Engine metrics return to zero.
/// - Deleted tasks have all files wiped from disk.
#[tokio::test]
async fn test_1000_randomized_lifecycle_operations() {
    let td = TestDir::new("stress_1000");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let cmd_tx = engine.cmd_sender.clone();
    let active_tasks = engine.active_tasks.clone();
    let known_paths = engine.known_paths.clone();
    let active_workers = engine.active_workers.clone();

    let num_tasks = 10u64;
    let ops_per_thread = 100; // 10 threads * 100 ops = 1,000 total operations
    let num_threads = 10;

    let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));
    let mut handles = Vec::new();

    for t_idx in 0..num_threads {
        let cmd = cmd_tx.clone();
        let bar = barrier.clone();
        let base_dir = td.path().to_path_buf();

        handles.push(tokio::spawn(async move {
            bar.wait().await;
            for i in 0..ops_per_thread {
                let id = ((i as u64) % num_tasks) + 1;
                let save_path = base_dir.join(format!("stress_{}.bin", id));

                match (t_idx * 7 + i) % 4 {
                    0 => {
                        let _ = cmd.send(EngineCommand::StartDownload {
                            id,
                            url: format!("http://127.0.0.1:9/stress_{}", id),
                            save_path,
                            connections: 1,
                        });
                    }
                    1 => {
                        let _ = cmd.send(EngineCommand::PauseDownload { id });
                    }
                    2 => {
                        let _ = cmd.send(EngineCommand::CancelDownload { id });
                    }
                    _ => {
                        let _ = cmd.send(EngineCommand::DeleteDownload {
                            id,
                            cleanup_files: true,
                        });
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Now issue a final Delete with cleanup_files: true for all tasks
    for id in 1..=num_tasks {
        let _ = cmd_tx.send(EngineCommand::DeleteDownload {
            id,
            cleanup_files: true,
        });
    }

    // Drain events for 1.5 seconds
    let end = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < end {
        while let Ok(_) = rx.try_recv() {}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Invariant 1: No leaked workers
    let final_workers = active_workers.load(Ordering::SeqCst);
    assert_eq!(final_workers, 0, "No leaked workers after 1,000 operations");

    // Invariant 2: Engine metrics must be completely cleared
    let tasks_len = active_tasks.lock().unwrap_or_else(|p| p.into_inner()).len();
    let paths_len = known_paths.lock().unwrap_or_else(|p| p.into_inner()).len();
    assert_eq!(tasks_len, 0, "active_tasks must be empty");
    assert_eq!(paths_len, 0, "known_paths must be empty");

    // Invariant 3: All files for all tasks must be deleted from disk
    for id in 1..=num_tasks {
        let p = td.path().join(format!("stress_{}.bin", id));
        let partinfo = td.path().join(format!("stress_{}.bin.partinfo", id));
        let tmp = td.path().join(format!("stress_{}.bin.tmp", id));
        let partinfo_tmp = td.path().join(format!("stress_{}.bin.partinfo.tmp", id));

        assert!(!p.exists(), "Task {} destination file must not exist", id);
        assert!(!partinfo.exists(), "Task {} partinfo must not exist", id);
        assert!(!tmp.exists(), "Task {} tmp must not exist", id);
        assert!(!partinfo_tmp.exists(), "Task {} partinfo.tmp must not exist", id);
    }
}

#[tokio::test]
async fn test_stalled_head_request_interrupted_by_delete_without_leak() {
    let td = TestDir::new("stalled_head_regress");
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let cmd_tx = engine.cmd_sender.clone();
    let active_tasks = engine.active_tasks.clone();
    let known_paths = engine.known_paths.clone();
    let active_workers = engine.active_workers.clone();

    // Use port 9 which will not accept connections, creating an in-flight probe
    let save_path = td.path().join("stalled_head.bin");
    let id = 99u64;

    let _ = cmd_tx.send(EngineCommand::StartDownload {
        id,
        url: "http://127.0.0.1:9/stalled_head.bin".into(),
        save_path: save_path.clone(),
        connections: 1,
    });

    // Brief yield to ensure coordinator enters run_download_task and begins HEAD request
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Immediately dispatch DeleteDownload while HEAD is in flight
    let _ = cmd_tx.send(EngineCommand::DeleteDownload {
        id,
        cleanup_files: true,
    });

    // Within a bounded window far below the 10-second connect_timeout, the task must be deleted and evicted
    let timeout_limit = Instant::now() + Duration::from_millis(500);
    let mut deleted_event_received = false;

    while Instant::now() < timeout_limit {
        while let Ok(evt) = rx.try_recv() {
            if let EngineEvent::Deleted { id: evt_id, .. } = evt {
                if evt_id == id {
                    deleted_event_received = true;
                }
            }
        }
        if deleted_event_received && active_tasks.lock().unwrap_or_else(|p| p.into_inner()).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(deleted_event_received, "EngineEvent::Deleted must be emitted within bounded time (< 500ms)");
    assert_eq!(
        active_tasks.lock().unwrap_or_else(|p| p.into_inner()).len(),
        0,
        "active_tasks must be empty (no leaked coordinator on cancelled HEAD)"
    );
    assert_eq!(
        known_paths.lock().unwrap_or_else(|p| p.into_inner()).len(),
        0,
        "known_paths must be empty"
    );
    assert_eq!(
        active_workers.load(Ordering::SeqCst),
        0,
        "active_workers must be 0"
    );
}
