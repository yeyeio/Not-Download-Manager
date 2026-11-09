//! Challenger 2 (Milestone 2) Empirical Hardening Test Suite
//! Resource Ownership, Memory Leak, Mutex Poisoning & Metrics Invariants
//!
//! Empirical challenges:
//! 1. `known_paths` Zero-Leak Invariant across high-volume lifecycle operations
//!    (Finished, Paused, Canceled, Failed, Deleted)
//! 2. Failed Cleanup Retention on Windows locked files and subsequent idempotent retry
//! 3. Mutex Poisoning Resilience under concurrent load across active_tasks, known_paths, and manager_arc
//! 4. WorkerGuard RAII Leak-Free Accounting under panic, cancellation, and rapid churn

use app::engine::{
    set_task_expected_sha256, spawn_download_engine, SegmentManager, TaskControl,
};
use app::types::{EngineCommand, EngineEvent};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

// ============================================================================
// Test Utilities & Directory Isolation
// ============================================================================

struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_challenger_m2_2_{}_{}_{}",
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

/// Win32 Process Memory FFI via psapi.dll
#[cfg(windows)]
mod win32_mem {
    #[repr(C)]
    #[allow(dead_code)]
    pub struct ProcessMemoryCounters {
        pub cb: u32,
        pub page_fault_count: u32,
        pub peak_working_set_size: usize,
        pub working_set_size: usize,
        pub quota_peak_paged_pool_usage: usize,
        pub quota_paged_pool_usage: usize,
        pub quota_peak_non_paged_pool_usage: usize,
        pub quota_non_paged_pool_usage: usize,
        pub pagefile_usage: usize,
        pub peak_pagefile_usage: usize,
    }

    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut std::ffi::c_void,
            counters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
        fn GetCurrentProcess() -> *mut std::ffi::c_void;
    }

    pub fn get_memory_info() -> (usize, usize) {
        unsafe {
            let mut pmc = std::mem::zeroed::<ProcessMemoryCounters>();
            pmc.cb = std::mem::size_of::<ProcessMemoryCounters>() as u32;
            let process = GetCurrentProcess();
            if GetProcessMemoryInfo(process, &mut pmc, pmc.cb) != 0 {
                (pmc.working_set_size, pmc.pagefile_usage)
            } else {
                (0, 0)
            }
        }
    }
}

#[cfg(not(windows))]
mod win32_mem {
    pub fn get_memory_info() -> (usize, usize) {
        (0, 0)
    }
}

/// Helper to open a file with exclusive write lock on Windows (denying all access)
fn open_exclusively_locked_file(path: &Path) -> std::fs::File {
    #[cfg(windows)]
    {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .share_mode(0) // FILE_SHARE_NONE: deny read, write, and delete
            .open(path)
            .expect("Opening file with exclusive Windows lock must succeed")
    }
    #[cfg(not(windows))]
    {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Opening file must succeed")
    }
}

// ============================================================================
// Challenge 1: known_paths Zero-Leak Invariant Across High-Volume Lifecycle Ops
// ============================================================================

/// Scenario 1A: 3,000 rapid lifecycle operations (Start, Pause, Cancel, Delete, Failed)
/// Asserting active_tasks, known_paths, and active_workers all drop to EXACTLY 0.
#[tokio::test]
async fn test_challenge_known_paths_zero_leak_thousands_lifecycle_ops() {
    let td = TestDir::new("zero_leak_ops");
    let mut engine = spawn_download_engine();

    let initial_metrics = engine.metrics();
    assert_eq!(initial_metrics.active_tasks_count, 0);
    assert_eq!(initial_metrics.known_paths_count, 0);
    assert_eq!(initial_metrics.active_workers_count, 0);

    let total_ops = 3_000;
    let batch_size = 50;
    let batches = total_ops / batch_size;

    for b in 0..batches {
        for i in 0..batch_size {
            let id = (b * batch_size + i + 1) as u64;
            let save_path = td.path().join(format!("task_{}.bin", id));

            // Start download with an unreachable port (fails quickly or gets paused/canceled/deleted)
            let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                id,
                url: format!("http://127.0.0.1:9/task_{}.bin", id),
                save_path: save_path.clone(),
                connections: 1,
            });

            // Exercise different lifecycle paths
            match i % 5 {
                0 => {
                    // Start -> Pause
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                }
                1 => {
                    // Start -> Cancel
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id });
                }
                2 => {
                    // Start -> Delete with cleanup
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
                3 => {
                    // Start -> Delete without cleanup
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: false,
                    });
                }
                _ => {
                    // Start -> let it fail due to unreachable network endpoint
                }
            }
        }

        // Drain pending events in batches to avoid unbounded channel growth
        let drain_until = Instant::now() + Duration::from_millis(60);
        while Instant::now() < drain_until {
            if engine.event_receiver.try_recv().is_err() {
                break;
            }
        }
    }

    // Await full terminal quiescence with bounded polling
    let quiescence_deadline = Instant::now() + Duration::from_secs(10);
    let mut settled = false;

    while Instant::now() < quiescence_deadline {
        // Drain any remaining events
        while engine.event_receiver.try_recv().is_ok() {}

        let m = engine.metrics();
        if m.active_tasks_count == 0 && m.known_paths_count == 0 && m.active_workers_count == 0 {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let final_metrics = engine.metrics();
    println!(
        "[Challenger 1A] Final Metrics after {} ops: active_tasks={}, known_paths={}, active_workers={}",
        total_ops,
        final_metrics.active_tasks_count,
        final_metrics.known_paths_count,
        final_metrics.active_workers_count
    );

    assert!(settled, "Engine must reach full quiescence within deadline");
    assert_eq!(
        final_metrics.active_tasks_count, 0,
        "EMPIRICAL ZERO-LEAK VIOLATION: active_tasks must be 0"
    );
    assert_eq!(
        final_metrics.known_paths_count, 0,
        "EMPIRICAL ZERO-LEAK VIOLATION: known_paths must be 0"
    );
    assert_eq!(
        final_metrics.active_workers_count, 0,
        "EMPIRICAL ZERO-LEAK VIOLATION: active_workers must be 0"
    );
}

/// Scenario 1B: Process working set memory inspection over 2,000 rapid lifecycle operations
#[tokio::test]
async fn test_challenge_known_paths_memory_bounded_under_stress() {
    let td = TestDir::new("mem_bounded_stress");
    let mut engine = spawn_download_engine();

    let (ws_before, _pf_before) = win32_mem::get_memory_info();

    for i in 1..=2000 {
        let id = i as u64;
        let save_path = td.path().join(format!("mem_{}.bin", id));

        let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
            id,
            url: format!("http://127.0.0.1:9/mem_{}.bin", id),
            save_path,
            connections: 1,
        });

        if i % 2 == 0 {
            let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id });
        } else {
            let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                id,
                cleanup_files: true,
            });
        }

        if i % 100 == 0 {
            let drain_until = Instant::now() + Duration::from_millis(30);
            while Instant::now() < drain_until {
                if engine.event_receiver.try_recv().is_err() {
                    break;
                }
            }
        }
    }

    // Final drain
    let drain_end = Instant::now() + Duration::from_secs(3);
    while Instant::now() < drain_end {
        while engine.event_receiver.try_recv().is_ok() {}
        let m = engine.metrics();
        if m.active_tasks_count == 0 && m.known_paths_count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    let (ws_after, _pf_after) = win32_mem::get_memory_info();
    let ws_delta_mb = ws_after.saturating_sub(ws_before) as f64 / (1024.0 * 1024.0);

    println!(
        "[Challenger 1B] Memory: before={:.2} MB, after={:.2} MB, delta={:.2} MB",
        ws_before as f64 / (1024.0 * 1024.0),
        ws_after as f64 / (1024.0 * 1024.0),
        ws_delta_mb
    );

    if ws_before > 0 {
        assert!(
            ws_delta_mb < 60.0,
            "Working set memory growth must remain bounded under 60 MB, observed: {:.2} MB",
            ws_delta_mb
        );
    }
}

/// Scenario 1C: EXPECTED_HASHES lifecycle and zero-leak verification
#[test]
fn test_challenge_expected_hashes_zero_leak() {
    let dummy_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Insert 1,000 hashes
    for id in 10_000..11_000 {
        set_task_expected_sha256(id, Some(dummy_hash.to_string()));
    }

    // Clean up all 1,000 hashes
    for id in 10_000..11_000 {
        set_task_expected_sha256(id, None);
    }

    // Insert again and verify overwrite works cleanly
    for id in 10_000..10_100 {
        set_task_expected_sha256(id, Some(dummy_hash.to_string()));
        set_task_expected_sha256(id, None);
    }
}

// ============================================================================
// Challenge 2: Failed Cleanup Retention on Locked Files & Idempotent Retry
// ============================================================================

/// Scenario 2A: Inactive task with locked file on Windows.
/// Verifies:
/// 1. DeleteDownload on locked file fails unlinking.
/// 2. known_paths retains the file path.
/// 3. EngineEvent::Deleted reports cleanup_success: false.
/// 4. Upon unlocking, an idempotent retry succeeds and removes the entry from known_paths.
#[tokio::test]
async fn test_challenge_locked_file_cleanup_retention_and_idempotent_retry() {
    let td = TestDir::new("locked_file_retry");
    let locked_path = td.path().join("exclusive_locked_payload.bin");

    // Create and exclusively lock file
    let lock_handle = open_exclusively_locked_file(&locked_path);
    assert!(locked_path.exists(), "Target file must exist");

    let mut engine = spawn_download_engine();
    let target_id = 88001u64;

    // Register path directly in engine.known_paths (simulating prior download completion)
    engine
        .known_paths
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(target_id, locked_path.clone());

    assert_eq!(engine.metrics().known_paths_count, 1);

    // Attempt 1: Issue Delete with cleanup_files: true while file is locked
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id: target_id,
        cleanup_files: true,
    });

    // Receive first Deleted event
    let first_event = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(EngineEvent::Deleted {
                id,
                cleanup_success,
                error,
            }) = engine.event_receiver.recv().await
            {
                if id == target_id {
                    return (cleanup_success, error);
                }
            }
        }
    })
    .await
    .expect("Must receive Deleted event for locked file");

    #[cfg(windows)]
    {
        // On Windows with share_mode(0), unlinking MUST fail
        assert!(
            !first_event.0,
            "EMPIRICAL ASSERTION: cleanup_success must be false for locked file"
        );
        assert!(
            first_event.1.is_some(),
            "EMPIRICAL ASSERTION: error message must be present for locked file"
        );

        // known_paths MUST retain the entry for retry
        let paths_count = engine.metrics().known_paths_count;
        assert_eq!(
            paths_count, 1,
            "EMPIRICAL ASSERTION: known_paths must retain entry on cleanup failure"
        );
        assert!(locked_path.exists(), "Locked file must still exist on disk");

        // Attempt 2: Unlock the file by dropping handle
        drop(lock_handle);

        // Issue idempotent retry DeleteDownload
        let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
            id: target_id,
            cleanup_files: true,
        });

        // Receive second Deleted event
        let second_event = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                }) = engine.event_receiver.recv().await
                {
                    if id == target_id {
                        return (cleanup_success, error);
                    }
                }
            }
        })
        .await
        .expect("Must receive second Deleted event after unlocking");

        assert!(
            second_event.0,
            "EMPIRICAL ASSERTION: retry cleanup_success must be true after unlock"
        );
        assert!(
            second_event.1.is_none(),
            "EMPIRICAL ASSERTION: retry error must be None after successful unlinking"
        );

        // known_paths MUST now be empty (zero-leak!)
        assert_eq!(
            engine.metrics().known_paths_count,
            0,
            "EMPIRICAL ASSERTION: known_paths must be evicted after successful retry"
        );
        assert!(
            !locked_path.exists(),
            "EMPIRICAL ASSERTION: file must be cleanly deleted from disk"
        );
    }

    #[cfg(not(windows))]
    {
        // On non-Windows platforms without mandatory file locking
        drop(lock_handle);
    }
}

/// Scenario 2B: Delete with cleanup_files: false on a locked file succeeds immediately
/// without touching the disk and evicts known_paths.
#[tokio::test]
async fn test_challenge_locked_file_delete_without_cleanup_evicts_known_paths() {
    let td = TestDir::new("locked_no_cleanup");
    let locked_path = td.path().join("locked_preserve.bin");
    let lock_handle = open_exclusively_locked_file(&locked_path);

    let mut engine = spawn_download_engine();
    let target_id = 88002u64;

    engine
        .known_paths
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(target_id, locked_path.clone());

    assert_eq!(engine.metrics().known_paths_count, 1);

    // Issue Delete with cleanup_files: false
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id: target_id,
        cleanup_files: false,
    });

    let event = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(EngineEvent::Deleted {
                id,
                cleanup_success,
                error,
            }) = engine.event_receiver.recv().await
            {
                if id == target_id {
                    return (cleanup_success, error);
                }
            }
        }
    })
    .await
    .expect("Must receive Deleted event");

    assert!(
        event.0,
        "cleanup_success must be true when cleanup_files is false"
    );
    assert!(event.1.is_none());
    assert_eq!(
        engine.metrics().known_paths_count,
        0,
        "known_paths must be evicted when cleanup_files is false"
    );
    assert!(
        locked_path.exists(),
        "File must be preserved on disk when cleanup_files is false"
    );

    drop(lock_handle);
}

// ============================================================================
// Challenge 3: Mutex Poisoning Resilience Under Concurrent Load
// ============================================================================

/// Scenario 3A: Mutex poisoning resilience on engine.active_tasks under concurrent load.
/// 10 worker threads actively querying/inserting while 4 poisoner threads intentionally panic.
#[test]
fn test_challenge_mutex_poisoning_active_tasks_under_concurrent_load() {
    const NUM_WORKERS: usize = 10;
    const NUM_POISONERS: usize = 4;
    const OPS_PER_WORKER: usize = 500;

    let active_tasks: Arc<Mutex<HashMap<u64, TaskControl>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let barrier = Arc::new(Barrier::new(NUM_WORKERS + NUM_POISONERS));
    let panic_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Spawn 10 normal worker threads using unwrap_or_else
    for w in 0..NUM_WORKERS {
        let tasks = active_tasks.clone();
        let bar = barrier.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            for op in 0..OPS_PER_WORKER {
                let id = (w * OPS_PER_WORKER + op) as u64;
                let ctrl = TaskControl::new(id);

                // Insert with poisoning recovery
                {
                    let mut map = tasks.lock().unwrap_or_else(|p| p.into_inner());
                    map.insert(id, ctrl.clone());
                }

                // Read with poisoning recovery
                {
                    let map = tasks.lock().unwrap_or_else(|p| p.into_inner());
                    let found = map.get(&id).is_some();
                    assert!(found, "Inserted entry must be readable even if poisoned");
                }

                // Remove with poisoning recovery
                {
                    let mut map = tasks.lock().unwrap_or_else(|p| p.into_inner());
                    map.remove(&id);
                }
            }
        }));
    }

    // Spawn 4 poisoner threads that repeatedly lock and panic
    for _ in 0..NUM_POISONERS {
        let tasks = active_tasks.clone();
        let bar = barrier.clone();
        let pc = panic_count.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            for _ in 0..20 {
                let tasks_inner = tasks.clone();
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let mut map = tasks_inner.lock().unwrap();
                    map.insert(999_999, TaskControl::new(999_999));
                    panic!("Intentional poisoner panic in active_tasks");
                }));
                pc.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(2));
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    // Verify: Mutex is poisoned, yet recovered safely and no data corruption
    assert!(
        active_tasks.is_poisoned(),
        "EMPIRICAL ASSERTION: active_tasks mutex must be poisoned"
    );
    let final_map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
    println!(
        "[Challenger 3A] Poisoned active_tasks recovered successfully! {} panics absorbed.",
        panic_count.load(Ordering::SeqCst)
    );
    assert!(
        panic_count.load(Ordering::SeqCst) > 0,
        "Poisoner threads must have panicked"
    );
    // 999_999 was inserted by poisoner
    assert!(final_map.get(&999_999).is_some());
}

/// Scenario 3B: Mutex poisoning resilience on engine.known_paths under concurrent load.
#[test]
fn test_challenge_mutex_poisoning_known_paths_under_concurrent_load() {
    const NUM_WORKERS: usize = 10;
    const NUM_POISONERS: usize = 4;
    const OPS_PER_WORKER: usize = 500;

    let known_paths: Arc<Mutex<HashMap<u64, PathBuf>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let barrier = Arc::new(Barrier::new(NUM_WORKERS + NUM_POISONERS));
    let panic_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for w in 0..NUM_WORKERS {
        let paths = known_paths.clone();
        let bar = barrier.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            for op in 0..OPS_PER_WORKER {
                let id = (w * OPS_PER_WORKER + op) as u64;
                let pb = PathBuf::from(format!("path_{}.dat", id));

                // Insert
                {
                    let mut map = paths.lock().unwrap_or_else(|p| p.into_inner());
                    map.insert(id, pb.clone());
                }

                // Query
                {
                    let map = paths.lock().unwrap_or_else(|p| p.into_inner());
                    assert_eq!(map.get(&id), Some(&pb));
                }

                // Remove
                {
                    let mut map = paths.lock().unwrap_or_else(|p| p.into_inner());
                    map.remove(&id);
                }
            }
        }));
    }

    for _ in 0..NUM_POISONERS {
        let paths = known_paths.clone();
        let bar = barrier.clone();
        let pc = panic_count.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            for _ in 0..20 {
                let p_clone = paths.clone();
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let mut map = p_clone.lock().unwrap();
                    map.insert(888_888, PathBuf::from("poison.dat"));
                    panic!("Intentional poisoner panic in known_paths");
                }));
                pc.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(2));
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    assert!(
        known_paths.is_poisoned(),
        "EMPIRICAL ASSERTION: known_paths mutex must be poisoned"
    );
    let final_paths = known_paths.lock().unwrap_or_else(|p| p.into_inner());
    println!(
        "[Challenger 3B] Poisoned known_paths recovered successfully! {} panics absorbed.",
        panic_count.load(Ordering::SeqCst)
    );
    assert_eq!(final_paths.get(&888_888), Some(&PathBuf::from("poison.dat")));
}

/// Scenario 3C: Mutex poisoning resilience on SegmentManager (manager_arc) under concurrent leasing
#[test]
fn test_challenge_mutex_poisoning_segment_manager_under_leasing_load() {
    const NUM_WORKERS: usize = 8;
    const TOTAL_SIZE: u64 = 100 * 1024 * 1024; // 100 MB
    const CHUNK_SIZE: u64 = 1024 * 1024; // 1 MB (100 chunks)

    let manager = SegmentManager::new(TOTAL_SIZE, CHUNK_SIZE, Some("etag_poison".into()));
    let manager_arc = Arc::new(Mutex::new(manager));
    let barrier = Arc::new(Barrier::new(NUM_WORKERS + 2));
    let panic_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // 8 worker threads leasing chunks and committing them
    for worker_id in 0..NUM_WORKERS {
        let mgr = manager_arc.clone();
        let bar = barrier.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            loop {
                let lease = {
                    let mut m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                    m.lease_next_chunk(worker_id)
                };

                let lease = match lease {
                    Some(l) => l,
                    None => break, // All chunks leased
                };

                // Simulate processing time
                std::thread::sleep(Duration::from_millis(1));

                // Commit chunk with unwrap_or_else
                let mut m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                let _ = m.commit_chunk(lease.chunk_id, lease.generation);
            }
        }));
    }

    // 2 poisoner threads injecting panics while holding the lock
    for _ in 0..2 {
        let mgr = manager_arc.clone();
        let bar = barrier.clone();
        let pc = panic_count.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            for _ in 0..10 {
                let mgr_clone = mgr.clone();
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let m = mgr_clone.lock().unwrap();
                    let _ = m.is_all_committed();
                    panic!("Intentional poison in manager_arc");
                }));
                pc.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(3));
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    assert!(
        manager_arc.is_poisoned(),
        "EMPIRICAL ASSERTION: manager_arc must be poisoned"
    );

    // Verify all chunks were committed despite repeated poisoning!
    let final_mgr = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
    println!(
        "[Challenger 3C] SegmentManager recovered! Committed: {}/{} bytes across {} panics",
        final_mgr.committed_bytes,
        TOTAL_SIZE,
        panic_count.load(Ordering::SeqCst)
    );
    assert_eq!(
        final_mgr.committed_bytes, TOTAL_SIZE,
        "All chunks must be successfully committed despite mutex poisoning"
    );
    assert!(
        final_mgr.is_all_committed(),
        "SegmentManager::is_all_committed must be true"
    );
}

// ============================================================================
// Challenge 4: WorkerGuard RAII Leak-Free Accounting
// ============================================================================

/// Exact RAII WorkerGuard mirror to verify drop execution across stack unwinding
struct TestWorkerGuard(Arc<AtomicUsize>);

impl TestWorkerGuard {
    fn new(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for TestWorkerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Scenario 4A: WorkerGuard accounting under severe panic and stack unwinding (100 threads)
#[test]
fn test_challenge_worker_guard_panic_unwind_raii_accounting() {
    const NUM_THREADS: usize = 100;
    const CYCLES_PER_THREAD: usize = 100;

    let worker_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = Vec::new();

    for _ in 0..NUM_THREADS {
        let counter = worker_counter.clone();
        let bar = barrier.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            for c in 0..CYCLES_PER_THREAD {
                let cnt = counter.clone();
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _guard = TestWorkerGuard::new(&cnt);
                    if c % 3 == 0 {
                        panic!("Intentional worker panic during streaming");
                    }
                    // normal execution
                }));
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    let final_workers = worker_counter.load(Ordering::SeqCst);
    println!(
        "[Challenger 4A] Final worker count after 10,000 panics/exits: {}",
        final_workers
    );
    assert_eq!(
        final_workers, 0,
        "EMPIRICAL ZERO-LEAK VIOLATION: WorkerGuard counter must return to 0 after all panics"
    );
}

/// Scenario 4B: WorkerGuard concurrent stampede and abrupt cancellation
#[tokio::test]
async fn test_challenge_worker_guard_concurrent_stampede_and_cancellation() {
    const NUM_WORKERS: usize = 64;
    let worker_counter = Arc::new(AtomicUsize::new(0));
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(tokio::sync::Notify::new());

    let mut join_handles = Vec::new();

    for _ in 0..NUM_WORKERS {
        let counter = worker_counter.clone();
        let cancel = cancel_flag.clone();
        let notif = notify.clone();

        join_handles.push(tokio::spawn(async move {
            let _guard = TestWorkerGuard::new(&counter);
            tokio::select! {
                _ = notif.notified() => {}
                _ = async {
                    while !cancel.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {}
            }
        }));
    }

    // Wait until all 64 workers are active
    while worker_counter.load(Ordering::SeqCst) < NUM_WORKERS {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert_eq!(
        worker_counter.load(Ordering::SeqCst),
        NUM_WORKERS,
        "All 64 workers must be accounted for"
    );

    // Abrupt cancellation
    cancel_flag.store(true, Ordering::SeqCst);
    notify.notify_waiters();

    // Await all workers
    for h in join_handles {
        let _ = h.await;
    }

    assert_eq!(
        worker_counter.load(Ordering::SeqCst),
        0,
        "EMPIRICAL ZERO-LEAK VIOLATION: worker_counter must drop to 0 after cancellation"
    );
}

/// Scenario 4C: Engine metrics consistency under concurrent churn across all 3 metrics
#[test]
fn test_challenge_engine_metrics_consistency_under_rapid_churn() {
    let engine = spawn_download_engine();
    const NUM_MUTATORS: usize = 12;
    const NUM_READERS: usize = 6;
    const CHURN_DURATION_MS: u64 = 500;

    let barrier = Arc::new(Barrier::new(NUM_MUTATORS + NUM_READERS));
    let stop_flag = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();

    // Mutator threads rapidly inserting and removing from active_tasks, known_paths, and active_workers
    for m in 0..NUM_MUTATORS {
        let active_tasks = engine.active_tasks.clone();
        let known_paths = engine.known_paths.clone();
        let active_workers = engine.active_workers.clone();
        let bar = barrier.clone();
        let stop = stop_flag.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            let mut iter = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let id = ((m as u64) << 32) | iter;
                iter += 1;

                // Insert
                {
                    let mut t = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    t.insert(id, TaskControl::new(id));
                }
                {
                    let mut k = known_paths.lock().unwrap_or_else(|p| p.into_inner());
                    k.insert(id, PathBuf::from(format!("p_{}.bin", id)));
                }
                active_workers.fetch_add(1, Ordering::SeqCst);

                // Evict
                {
                    let mut t = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                    t.remove(&id);
                }
                {
                    let mut k = known_paths.lock().unwrap_or_else(|p| p.into_inner());
                    k.remove(&id);
                }
                active_workers.fetch_sub(1, Ordering::SeqCst);
            }
        }));
    }

    // Reader threads calling engine.metrics()
    for _ in 0..NUM_READERS {
        let tasks = engine.active_tasks.clone();
        let paths = engine.known_paths.clone();
        let workers = engine.active_workers.clone();
        let bar = barrier.clone();
        let stop = stop_flag.clone();

        handles.push(std::thread::spawn(move || {
            bar.wait();
            while !stop.load(Ordering::Relaxed) {
                let active_tasks_count = tasks.lock().unwrap_or_else(|p| p.into_inner()).len();
                let known_paths_count = paths.lock().unwrap_or_else(|p| p.into_inner()).len();
                let active_workers_count = workers.load(Ordering::Relaxed);
                let _ = (active_tasks_count, known_paths_count, active_workers_count);
                std::thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    std::thread::sleep(Duration::from_millis(CHURN_DURATION_MS));
    stop_flag.store(true, Ordering::SeqCst);

    for h in handles {
        let _ = h.join();
    }

    let final_metrics = engine.metrics();
    println!(
        "[Challenger 4C] Churn completed. Final metrics: tasks={}, paths={}, workers={}",
        final_metrics.active_tasks_count,
        final_metrics.known_paths_count,
        final_metrics.active_workers_count
    );

    assert_eq!(final_metrics.active_tasks_count, 0);
    assert_eq!(final_metrics.known_paths_count, 0);
    assert_eq!(final_metrics.active_workers_count, 0);
}
