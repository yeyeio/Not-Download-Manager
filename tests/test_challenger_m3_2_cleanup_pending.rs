//! Challenger 2 (Milestone 3) Dedicated Empirical Verification Test Suite
//! CleanupPending State, Idempotent Retry Action, Action Guards & Event Integrity Stress Verification
//!
//! Empirical challenges:
//! 1. File unlinking failure simulation & Windows file lock (os error 32):
//!    - Transitions to `DownloadStatus::CleanupPending`
//!    - SQLite row is NOT deleted
//!    - In-memory item is NOT removed from `downloads`
//!    - Error description is captured and stored in `cleanup_errors` and item description
//! 2. Idempotent Retry:
//!    - Triggering `retry_cleanup` dispatches `EngineCommand::DeleteDownload { cleanup_files: true }`
//!    - Zero coordinator threads or network workers spawned (active_tasks == 0, active_workers == 0)
//!    - When file lock is released, retry unlinks file, deletes SQLite row, purges from `downloads`,
//!      and records ID in `deleted_task_ids`
//! 3. Action Guards:
//!    - `resume_selected()` and `redownload_selected()` are rejected while status is `CleanupPending`
//!    - `pause_selected()` and `cancel_selected()` do not corrupt `CleanupPending`
//! 4. Event Integrity:
//!    - `EngineEvent::Finished` stores `HashVerificationStatus` in SQLite (`Verified`, `VerificationFailed`)
//!    - Late `ProgressUpdate` packets do not overwrite `Paused`, `Completed`, `Canceled`, or `CleanupPending`
//! 5. Adversarial Stress Testing:
//!    - 50 concurrent tasks with file locks entering `CleanupPending`
//!    - 1,000 late `ProgressUpdate` packets blasted at terminal/cleanup states
//!    - Rapid retry cycles under locked vs unlocked files
//!    - Complete SQLite and UI state reconciliation invariants

use app::db::Database;
use app::engine::{spawn_download_engine, DownloadEngineHandle};
use app::types::{
    Category, DownloadItem, DownloadSettings, DownloadStatus, EngineCommand, EngineEvent,
    HashVerificationStatus,
};
use app::ui::IdmApp;
use chrono::Local;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

// ============================================================================
// Test Utilities & Isolation
// ============================================================================

struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_challenger_m3_2_{}_{}_{}",
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

/// Helper to open a file with exclusive lock on Windows (denying delete and write)
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

/// Creates a test DownloadItem
fn make_test_item(id: u64, path: PathBuf, status: DownloadStatus) -> DownloadItem {
    DownloadItem {
        id,
        url: format!("https://example.com/item_{}.dat", id),
        filename: format!("item_{}.dat", id),
        category: Category::General,
        save_path: path,
        total_size: Some(10_485_760),
        downloaded_bytes: 0,
        status,
        transfer_rate: 0.0,
        time_left_secs: None,
        resume_capable: true,
        date_added: Local::now(),
        max_connections: 4,
        segments: vec![],
        description: String::new(),
        queue: Some("Main".to_string()),
        ..Default::default()
    }
}

/// Helper to create a controlled test IdmApp with explicit channels and in-memory DB
fn create_test_app(
    db: Arc<Database>,
) -> (
    IdmApp,
    tokio::sync::mpsc::UnboundedSender<EngineEvent>,
    tokio::sync::mpsc::UnboundedReceiver<EngineCommand>,
) {
    let td = TestDir::new("app_env");
    let dummy_db_path = td.path().join("dummy.db");
    unsafe {
        std::env::set_var("HYPERDOWNLOAD_DB_PATH", dummy_db_path);
    }

    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let (evt_tx, evt_rx) = tokio::sync::mpsc::unbounded_channel();
    let engine_handle = DownloadEngineHandle {
        cmd_sender: cmd_tx,
        event_receiver: evt_rx,
        active_tasks: Arc::new(Mutex::new(HashMap::new())),
        known_paths: Arc::new(Mutex::new(HashMap::new())),
        active_workers: Arc::new(AtomicUsize::new(0)),
    };

    let mut app = IdmApp::new(None);
    app.engine = engine_handle;
    app.db = db;
    app.downloads.clear();
    app.reconcile_rx = None;
    app.is_reconciling = false;
    app.deleted_task_ids.clear();
    app.deleting_task_ids.clear();
    app.cleanup_errors.clear();
    app.task_versions.clear();
    app.selected_id = None;
    app.next_id = 1;

    (app, evt_tx, cmd_rx)
}

// ============================================================================
// 1. File Unlinking Failure & CleanupPending State Transition Tests
// ============================================================================

#[test]
fn test_unlinking_failure_transitions_to_cleanup_pending_mock_event() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, evt_tx, _cmd_rx) = create_test_app(db.clone());

    let item = make_test_item(101, PathBuf::from("dummy_path_101.dat"), DownloadStatus::Downloading);
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    app.downloads.push(item);
    app.selected_id = Some(101);

    // Simulate engine reporting deletion failed due to Windows file lock (os error 32)
    let failure_err = "The process cannot access the file because it is being used by another process. (os error 32)";
    let evt = EngineEvent::Deleted {
        id: 101,
        cleanup_success: false,
        error: Some(failure_err.to_string()),
    };
    evt_tx.send(evt).unwrap();
    app.poll_engine_events();

    // In-memory validations
    assert_eq!(app.downloads.len(), 1, "Item must NOT be removed from in-memory downloads");
    assert_eq!(app.downloads[0].id, 101);
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending, "Status must transition to CleanupPending");
    assert_eq!(app.downloads[0].transfer_rate, 0.0);
    assert_eq!(app.downloads[0].time_left_secs, None);
    assert!(app.downloads[0].description.contains("Cleanup failed"), "Description must reflect cleanup failure");
    assert_eq!(app.cleanup_errors.get(&101).map(|s| s.as_str()), Some(failure_err), "Error description must be captured");
    assert!(!app.deleted_task_ids.contains(&101), "ID must NOT be marked in deleted tombstones");

    // SQLite validations
    assert!(db.download_exists(101).unwrap(), "SQLite row must NOT be deleted");
    let items_in_db = db.reconcile_and_load_all().unwrap();
    assert_eq!(items_in_db.len(), 1);
    assert_eq!(items_in_db[0].status, DownloadStatus::CleanupPending, "SQLite status must be persisted as CleanupPending");
}

#[tokio::test]
async fn test_unlinking_failure_with_real_engine_and_windows_file_lock() {
    let td = TestDir::new("real_lock_failure");
    let locked_file_path = td.path().join("real_locked_file.bin");

    // Hold exclusive Windows file lock
    let lock_handle = open_exclusively_locked_file(&locked_file_path);
    assert!(locked_file_path.exists());

    let db_path = td.path().join("test_lock.db");
    unsafe {
        std::env::set_var("HYPERDOWNLOAD_DB_PATH", &db_path);
    }
    let db = Arc::new(Database::open(&db_path).unwrap());

    // Spawn real download engine
    let engine = spawn_download_engine();

    let mut app = IdmApp::new(None);
    app.db = db.clone();
    app.downloads.clear();
    app.reconcile_rx = None;
    app.is_reconciling = false;

    // Connect app directly to real engine
    let real_cmd_sender = engine.cmd_sender.clone();
    let mut real_event_receiver = engine.event_receiver;
    app.engine.cmd_sender = real_cmd_sender.clone();

    let target_id = 202u64;
    let item = make_test_item(target_id, locked_file_path.clone(), DownloadStatus::Paused);
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    app.downloads.push(item);
    app.selected_id = Some(target_id);

    // Register path into engine known_paths so engine knows which file to unlink
    engine
        .known_paths
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(target_id, locked_file_path.clone());

    // Trigger delete_selected from UI
    app.delete_selected();
    assert!(app.deleting_task_ids.contains(&target_id));

    // Await engine event with timeout
    let evt = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(event) = real_event_receiver.recv().await {
                if let EngineEvent::Deleted { id, .. } = &event {
                    if *id == target_id {
                        return event;
                    }
                }
            }
        }
    })
    .await
    .expect("Real engine must emit Deleted event within 3 seconds");

    match &evt {
        EngineEvent::Deleted { cleanup_success, error, .. } => {
            #[cfg(windows)]
            {
                assert!(!cleanup_success, "Cleanup must fail empirically due to Windows file lock");
                assert!(error.is_some(), "Error string must be reported by engine");
            }
        }
        _ => panic!("Expected Deleted event"),
    }

    // Forward event into app's event channel and poll
    let (app_evt_tx, app_evt_rx) = tokio::sync::mpsc::unbounded_channel();
    app.engine.event_receiver = app_evt_rx;
    app_evt_tx.send(evt).unwrap();
    app.poll_engine_events();

    #[cfg(windows)]
    {
        // Assert UI state
        assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);
        assert!(app.cleanup_errors.get(&target_id).is_some());
        assert!(db.download_exists(target_id).unwrap(), "Database row must NOT be deleted");
    }

    // Clean up file lock
    drop(lock_handle);
}

// ============================================================================
// 2. Idempotent Retry Action & Worker Absence Tests
// ============================================================================

#[tokio::test]
async fn test_retry_cleanup_flow_and_no_workers_spawned() {
    let td = TestDir::new("retry_cleanup_flow");
    let file_path = td.path().join("retry_target.bin");

    // Create locked file
    let lock_handle = open_exclusively_locked_file(&file_path);

    let db_path = td.path().join("retry.db");
    unsafe {
        std::env::set_var("HYPERDOWNLOAD_DB_PATH", &db_path);
    }
    let db = Arc::new(Database::open(&db_path).unwrap());
    let mut engine = spawn_download_engine();

    let target_id = 303u64;
    let item = make_test_item(target_id, file_path.clone(), DownloadStatus::CleanupPending);
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    db.update_status(target_id, &DownloadStatus::CleanupPending).unwrap();

    let mut app = IdmApp::new(None);
    app.db = db.clone();
    app.downloads.push(item);
    app.selected_id = Some(target_id);
    app.cleanup_errors.insert(target_id, "File in use (os error 32)".into());
    app.reconcile_rx = None;
    app.is_reconciling = false;

    // Connect app to engine
    app.engine.cmd_sender = engine.cmd_sender.clone();
    engine
        .known_paths
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(target_id, file_path.clone());

    // 1. Trigger retry_cleanup while file is STILL locked
    app.retry_cleanup_selected();
    assert!(app.deleting_task_ids.contains(&target_id));
    assert_eq!(app.downloads[0].status, DownloadStatus::Error("Cleaning up...".into()));

    // Verify engine metrics: NO download coordinators, NO network workers spawned!
    let metrics = engine.metrics();
    assert_eq!(metrics.active_tasks_count, 0, "No download coordinator task must be spawned for cleanup retry");
    assert_eq!(metrics.active_workers_count, 0, "No network workers must be spawned for cleanup retry");

    // Engine receives DeleteDownload and processes on inactive task
    let evt1 = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(EngineEvent::Deleted { id, cleanup_success, error }) = engine.event_receiver.recv().await {
                if id == target_id {
                    return (cleanup_success, error);
                }
            }
        }
    })
    .await
    .expect("Engine must emit Deleted event for first retry");

    #[cfg(windows)]
    {
        assert!(!evt1.0, "First retry must fail unlinking while file lock is active");
        // Re-inject into app
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        app.engine.event_receiver = rx;
        tx.send(EngineEvent::Deleted { id: target_id, cleanup_success: evt1.0, error: evt1.1 }).unwrap();
        app.poll_engine_events();

        // Must still be in CleanupPending
        assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);
        assert!(db.download_exists(target_id).unwrap());
    }

    // 2. Now RELEASE file lock
    drop(lock_handle);

    // 3. Trigger second retry_cleanup
    app.retry_cleanup(target_id);
    assert!(app.deleting_task_ids.contains(&target_id));

    // Verify again: ZERO worker threads
    let metrics2 = engine.metrics();
    assert_eq!(metrics2.active_tasks_count, 0);
    assert_eq!(metrics2.active_workers_count, 0);

    let evt2 = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(EngineEvent::Deleted { id, cleanup_success, error }) = engine.event_receiver.recv().await {
                if id == target_id {
                    return (cleanup_success, error);
                }
            }
        }
    })
    .await
    .expect("Engine must emit Deleted event for second retry");

    assert!(evt2.0, "Second retry must succeed after file lock is released");

    // Deliver to UI
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
    app.engine.event_receiver = rx2;
    tx2.send(EngineEvent::Deleted { id: target_id, cleanup_success: evt2.0, error: evt2.1 }).unwrap();
    app.poll_engine_events();

    // Verify task is completely expunged from UI and SQLite, and recorded in deleted_task_ids
    assert_eq!(app.downloads.len(), 0, "Task must be purged from in-memory downloads");
    assert!(app.deleted_task_ids.contains(&target_id), "ID must be recorded in deleted_task_ids");
    assert!(!app.deleting_task_ids.contains(&target_id));
    assert!(app.cleanup_errors.get(&target_id).is_none());
    assert!(!db.download_exists(target_id).unwrap(), "Task row must be deleted from SQLite");
    assert!(!file_path.exists(), "Target file on disk must be unlinked");
}

// ============================================================================
// 3. Action Guards on CleanupPending State
// ============================================================================

#[test]
fn test_action_guards_reject_lifecycle_actions_on_cleanup_pending() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, _evt_tx, mut cmd_rx) = create_test_app(db.clone());

    let item = make_test_item(401, PathBuf::from("guarded_item.dat"), DownloadStatus::CleanupPending);
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    app.downloads.push(item);
    app.selected_id = Some(401);

    // 1. Test resume_selected()
    app.resume_selected();
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending, "resume_selected must be rejected");
    assert!(cmd_rx.try_recv().is_err(), "No engine command must be dispatched on rejected resume");

    // 2. Test redownload_selected()
    app.redownload_selected();
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending, "redownload_selected must be rejected");
    assert!(cmd_rx.try_recv().is_err(), "No engine command must be dispatched on rejected redownload");

    // 3. Test pause_selected()
    app.pause_selected();
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending, "pause_selected must be a no-op");
    assert!(cmd_rx.try_recv().is_err(), "No command dispatched");

    // 4. Test cancel_selected()
    app.cancel_selected();
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending, "cancel_selected must be a no-op");
    assert!(cmd_rx.try_recv().is_err(), "No command dispatched");
}

// ============================================================================
// 4. Event Integrity: Finished & Late Progress Protection
// ============================================================================

#[test]
fn test_finished_event_persists_hash_verification_status_in_sqlite() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, evt_tx, _cmd_rx) = create_test_app(db.clone());

    // Test case A: HashVerificationStatus::Verified
    let item1 = make_test_item(501, PathBuf::from("verified.dat"), DownloadStatus::Downloading);
    db.insert_download(&item1, &DownloadSettings::default()).unwrap();
    app.downloads.push(item1);

    evt_tx.send(EngineEvent::Finished {
        id: 501,
        hash_status: HashVerificationStatus::Verified,
    }).unwrap();
    app.poll_engine_events();

    assert_eq!(app.downloads[0].status, DownloadStatus::Completed);
    assert_eq!(app.downloads[0].hash_status, HashVerificationStatus::Verified);

    let (_, hs1) = db.get_hash_status(501).unwrap();
    assert_eq!(hs1, HashVerificationStatus::Verified, "Verified status must be persisted in SQLite");

    // Test case B: HashVerificationStatus::VerificationFailed
    let item2 = make_test_item(502, PathBuf::from("failed_hash.dat"), DownloadStatus::Downloading);
    db.insert_download(&item2, &DownloadSettings::default()).unwrap();
    app.downloads.push(item2);

    evt_tx.send(EngineEvent::Finished {
        id: 502,
        hash_status: HashVerificationStatus::VerificationFailed,
    }).unwrap();
    app.poll_engine_events();

    assert_eq!(app.downloads[1].status, DownloadStatus::Completed);
    assert_eq!(app.downloads[1].hash_status, HashVerificationStatus::VerificationFailed);

    let (_, hs2) = db.get_hash_status(502).unwrap();
    assert_eq!(hs2, HashVerificationStatus::VerificationFailed, "VerificationFailed status must be persisted in SQLite");
}

#[test]
fn test_late_progress_update_does_not_clobber_terminal_or_paused_states() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, evt_tx, _cmd_rx) = create_test_app(db.clone());

    let test_statuses = vec![
        (601, DownloadStatus::Paused),
        (602, DownloadStatus::Completed),
        (603, DownloadStatus::Canceled),
        (604, DownloadStatus::CleanupPending),
    ];

    for (id, status) in &test_statuses {
        let mut item = make_test_item(*id, PathBuf::from(format!("task_{}.dat", id)), status.clone());
        item.downloaded_bytes = 10_000_000;
        item.transfer_rate = 0.0;
        item.time_left_secs = None;
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
        app.downloads.push(item);
    }

    // Blast out-of-order late ProgressUpdates for each task
    for (id, _) in &test_statuses {
        evt_tx.send(EngineEvent::ProgressUpdate {
            id: *id,
            downloaded: 3_000_000, // Stale lower byte count
            speed: 850_000.0,      // In-flight speed
            time_left: Some(15),   // Stale time left
            segments: vec![],
        }).unwrap();
    }
    app.poll_engine_events();

    // Verify all 4 states remained completely immune to progress clobbering
    for (id, original_status) in &test_statuses {
        let item = app.downloads.iter().find(|d| d.id == *id).unwrap();
        assert_eq!(&item.status, original_status, "Status for #{} must NOT be clobbered", id);
        assert_eq!(item.downloaded_bytes, 10_000_000, "Downloaded bytes for #{} must NOT be overwritten", id);
        assert_eq!(item.transfer_rate, 0.0, "Transfer rate for #{} must remain 0.0", id);
        assert_eq!(item.time_left_secs, None, "Time left for #{} must remain None", id);
    }
}

// ============================================================================
// 5. Adversarial Stress & High-Volume Interleavings
// ============================================================================

#[test]
fn test_stress_50_tasks_cleanup_pending_storm_and_late_progress_hammering() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, evt_tx, mut cmd_rx) = create_test_app(db.clone());

    const TASK_COUNT: usize = 50;

    // 1. Populate 50 tasks
    for i in 1..=TASK_COUNT {
        let id = i as u64;
        let item = make_test_item(id, PathBuf::from(format!("stress_{}.bin", id)), DownloadStatus::Downloading);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
        app.downloads.push(item);
    }
    assert_eq!(app.downloads.len(), TASK_COUNT);

    // 2. Blast failure Deleted events for all 50 tasks
    for i in 1..=TASK_COUNT {
        let id = i as u64;
        evt_tx.send(EngineEvent::Deleted {
            id,
            cleanup_success: false,
            error: Some(format!("Locked resource #{} (os error 32)", id)),
        }).unwrap();
    }
    app.poll_engine_events();

    // Verify all 50 entered CleanupPending
    for i in 1..=TASK_COUNT {
        let id = i as u64;
        let item = &app.downloads[i - 1];
        assert_eq!(item.id, id);
        assert_eq!(item.status, DownloadStatus::CleanupPending);
        assert!(app.cleanup_errors.contains_key(&id));
        assert!(db.download_exists(id).unwrap(), "Database row for #{} must exist", id);
    }

    // 3. Blast 1,000 late ProgressUpdate packets across random task IDs
    for seq in 0..1000 {
        let target_id = (seq % TASK_COUNT as u64) + 1;
        evt_tx.send(EngineEvent::ProgressUpdate {
            id: target_id,
            downloaded: 100 * seq,
            speed: 500_000.0,
            time_left: Some(60),
            segments: vec![],
        }).unwrap();
    }
    app.poll_engine_events();

    // Verify ZERO tasks corrupted
    for item in &app.downloads {
        assert_eq!(item.status, DownloadStatus::CleanupPending, "Progress storm must not corrupt CleanupPending");
        assert_eq!(item.transfer_rate, 0.0);
        assert_eq!(item.time_left_secs, None);
    }

    // 4. Batch retry cleanup for all 50 tasks
    for i in 1..=TASK_COUNT {
        let id = i as u64;
        app.retry_cleanup(id);
    }

    // Verify 50 DeleteDownload commands dispatched with cleanup_files: true
    let mut delete_cmds_received = 0;
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let EngineCommand::DeleteDownload { cleanup_files, .. } = cmd {
            assert!(cleanup_files);
            delete_cmds_received += 1;
        }
    }
    assert_eq!(delete_cmds_received, TASK_COUNT);

    // 5. Simulate successful deletion for all 50 tasks
    for i in 1..=TASK_COUNT {
        let id = i as u64;
        evt_tx.send(EngineEvent::Deleted {
            id,
            cleanup_success: true,
            error: None,
        }).unwrap();
    }
    app.poll_engine_events();

    // Verify complete purging: UI downloads empty, SQLite empty, all IDs in tombstones
    assert_eq!(app.downloads.len(), 0, "All 50 tasks must be purged from UI");
    assert_eq!(app.deleted_task_ids.len(), TASK_COUNT, "All 50 tasks must be in deleted_task_ids");
    assert_eq!(app.deleting_task_ids.len(), 0);
    assert_eq!(app.cleanup_errors.len(), 0);

    for i in 1..=TASK_COUNT {
        let id = i as u64;
        assert!(!db.download_exists(id).unwrap(), "Database row #{} must be deleted", id);
    }
}

#[test]
fn test_retry_cleanup_double_click_debounce_guard() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, _evt_tx, mut cmd_rx) = create_test_app(db.clone());

    let item = make_test_item(701, PathBuf::from("debounce.dat"), DownloadStatus::CleanupPending);
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    app.downloads.push(item);
    app.selected_id = Some(701);

    // First click: triggers retry
    app.retry_cleanup_selected();
    assert_eq!(app.downloads[0].status, DownloadStatus::Error("Cleaning up...".into()));
    assert!(app.deleting_task_ids.contains(&701));

    let cmd1 = cmd_rx.try_recv().expect("First click must dispatch DeleteDownload");
    match cmd1 {
        EngineCommand::DeleteDownload { id, cleanup_files } => {
            assert_eq!(id, 701);
            assert!(cleanup_files);
        }
        _ => panic!("Expected DeleteDownload"),
    }

    // Rapid double-click while in Cleaning up... status: must NOT dispatch duplicate command
    app.retry_cleanup_selected();
    assert!(cmd_rx.try_recv().is_err(), "Rapid repeat click must be debounced while cleaning up");
}

#[test]
fn test_reconciliation_does_not_revert_cleanup_pending_task() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, _evt_tx, _cmd_rx) = create_test_app(db.clone());

    // In SQLite and in-memory, task is in CleanupPending
    let mut item = make_test_item(801, PathBuf::from("rec_test.dat"), DownloadStatus::CleanupPending);
    item.description = "Cleanup failed: Lock held".into();
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    db.update_status(801, &DownloadStatus::CleanupPending).unwrap();
    app.downloads.push(item);

    // Background reconciliation returns a snapshot where task 801 was recorded as Downloading/Paused
    let stale_snapshot = vec![
        make_test_item(801, PathBuf::from("rec_test.dat"), DownloadStatus::Paused),
    ];
    let (tx, rx) = std::sync::mpsc::channel();
    tx.send(Ok(stale_snapshot)).unwrap();
    app.is_reconciling = true;
    app.reconcile_rx = Some(rx);

    app.check_background_reconciliation();

    // Verify task status was NOT reverted to Paused
    assert_eq!(app.downloads.len(), 1);
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending, "Reconciliation must preserve CleanupPending");
    assert_eq!(app.downloads[0].description, "Cleanup failed: Lock held");
}

#[test]
fn test_delete_completed_ignores_cleanup_pending_tasks() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let (mut app, _evt_tx, _cmd_rx) = create_test_app(db.clone());

    let item_completed = make_test_item(901, PathBuf::from("done.dat"), DownloadStatus::Completed);
    let item_cleanup = make_test_item(902, PathBuf::from("pending.dat"), DownloadStatus::CleanupPending);

    db.insert_download(&item_completed, &DownloadSettings::default()).unwrap();
    db.insert_download(&item_cleanup, &DownloadSettings::default()).unwrap();
    db.update_status(902, &DownloadStatus::CleanupPending).unwrap();

    app.downloads.push(item_completed);
    app.downloads.push(item_cleanup);

    // Call delete_completed()
    app.delete_completed();

    // Completed item 901 removed, CleanupPending item 902 preserved
    assert_eq!(app.downloads.len(), 1);
    assert_eq!(app.downloads[0].id, 902);
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);

    assert!(app.deleted_task_ids.contains(&901));
    assert!(!app.deleted_task_ids.contains(&902));

    assert!(!db.download_exists(901).unwrap(), "Completed item must be deleted from DB");
    assert!(db.download_exists(902).unwrap(), "CleanupPending item must NOT be deleted from DB");
}
