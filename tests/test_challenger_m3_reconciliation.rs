//! Phase 4.3 Milestone M3 — Empirical Challenger Verification Suite
//!
//! Adversarial stress testing and empirical verification of startup reconciliation
//! races, tombstone isolation, and state preservation invariants.

use not_download_manager::db::Database;
use not_download_manager::types::{
    Category, DownloadItem, DownloadSettings, DownloadStatus,
    EngineEvent, HashVerificationStatus,
};
use not_download_manager::ui::IdmApp;
use chrono::Local;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Global lock for tests touching environment variables
static ENV_MUTEX: Mutex<()> = Mutex::new(());
static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TempTestDir(PathBuf);

impl TempTestDir {
    fn new(prefix: &str) -> Self {
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "hyperdownload_challenger_m3_{}_{}_{}",
            prefix,
            std::process::id(),
            count
        ));
        let _ = std::fs::create_dir_all(&path);
        TempTestDir(path)
    }

    #[allow(dead_code)]
    fn path(&self) -> &Path {
        &self.0
    }

    fn db_path(&self) -> PathBuf {
        self.0.join("test_downloads.db")
    }
}

impl Drop for TempTestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn make_item(id: u64, status: DownloadStatus, url: Option<&str>) -> DownloadItem {
    let u = url.unwrap_or("https://example.com/testfile.dat").to_string();
    let fname = format!("file_{}.dat", id);
    DownloadItem {
        id,
        url: u,
        filename: fname.clone(),
        category: Category::General,
        save_path: PathBuf::from(format!("downloads/{}", fname)),
        total_size: Some(10_000_000),
        downloaded_bytes: 0,
        status,
        transfer_rate: 0.0,
        time_left_secs: None,
        resume_capable: true,
        date_added: Local::now(),
        max_connections: 8,
        segments: vec![],
        description: format!("Item #{}", id),
        queue: Some("Main".to_string()),
        expected_sha256: None,
        hash_status: HashVerificationStatus::NotConfigured,
        ..Default::default()
    }
}

fn create_app_with_db(db_path: &Path) -> IdmApp {
    let _guard = ENV_MUTEX.lock().unwrap();
    // Set environment variable so IdmApp::new opens the target DB
    unsafe {
        std::env::set_var("HYPERDOWNLOAD_DB_PATH", db_path.to_str().unwrap());
    }
    let app = IdmApp::new(None);
    unsafe {
        std::env::remove_var("HYPERDOWNLOAD_DB_PATH");
    }
    app
}

// ============================================================================
// TEST 1: Concurrent Task Addition While Reconciliation is Computing
// ============================================================================
#[test]
fn test_challenger_m3_concurrent_task_addition_id_space_and_settings_preservation() {
    let test_dir = TempTestDir::new("concurrent_add");
    let db_path = test_dir.db_path();

    // 1. Seed database with 50 existing tasks (IDs 1..=50)
    let db = Database::open(&db_path).expect("Failed to open test DB");
    for id in 1..=50 {
        let item = make_item(id, DownloadStatus::Paused, Some(&format!("https://legacy.com/item_{}.bin", id)));
        let settings = DownloadSettings {
            download_id: id,
            max_connections: 4,
            chunk_size: 1024 * 1024,
            speed_limit: None,
            retry_limit: 3,
        };
        db.insert_download(&item, &settings).unwrap();
    }
    drop(db);

    // 2. Instantiate IdmApp
    let mut app = create_app_with_db(&db_path);

    // Assert: next_id MUST start above max_existing_id (50) -> next_id == 51
    assert_eq!(
        app.next_id, 51,
        "Invariant: app.next_id must be initialized to max_existing_id + 1"
    );

    // Intercept/replace reconciliation channel to hold the snapshot deterministically
    let (tx, rx) = std::sync::mpsc::channel();
    app.reconcile_rx = Some(rx);
    app.is_reconciling = true;

    // 3. User adds 25 tasks concurrently while reconciliation is in flight
    let mut added_user_urls = Vec::new();
    for i in 0..25 {
        let id = app.next_id;
        app.next_id += 1;
        let url = format!("https://cdn.example.com/user_download_{}.iso", id);
        added_user_urls.push((id, url.clone()));

        let mut user_item = make_item(id, DownloadStatus::Downloading, Some(&url));
        user_item.max_connections = 16;
        user_item.description = format!("User added high-priority item {}", id);
        user_item.downloaded_bytes = 100_000 * (i as u64 + 1);

        let user_settings = DownloadSettings {
            download_id: id,
            max_connections: 16,
            chunk_size: 4 * 1024 * 1024,
            speed_limit: Some(5_000_000),
            retry_limit: 10,
        };

        app.db.insert_download(&user_item, &user_settings).unwrap();
        app.mark_task_mutated(id);
        app.downloads.push(user_item);
    }

    assert_eq!(app.next_id, 76);
    assert_eq!(app.downloads.len(), 25);

    // 4. Background reconciliation thread delivers snapshot containing the 50 legacy items
    let legacy_db = Database::open(&db_path).unwrap();
    let legacy_items = legacy_db.reconcile_and_load_all().unwrap();
    assert_eq!(legacy_items.len(), 75); // DB has 50 legacy + 25 user-added
    // Simulate snapshot that only contained the original 50 items
    let snapshot_50: Vec<DownloadItem> = legacy_items.into_iter().filter(|d| d.id <= 50).collect();
    assert_eq!(snapshot_50.len(), 50);

    tx.send(Ok(snapshot_50)).unwrap();

    // 5. Run reconciliation merge
    app.check_background_reconciliation();

    // 6. Assertions
    assert_eq!(
        app.downloads.len(),
        75,
        "Total downloads must be exactly 50 legacy + 25 user added"
    );

    // Check no primary key collisions: all IDs must be unique
    let id_set: HashSet<u64> = app.downloads.iter().map(|d| d.id).collect();
    assert_eq!(id_set.len(), 75, "All 75 task IDs must be strictly unique!");
    for id in 1..=75 {
        assert!(id_set.contains(&id), "Missing ID #{}", id);
    }

    // Invariant: next_id must remain >= 76
    assert!(
        app.next_id >= 76,
        "next_id must never regress (expected >= 76, got {})",
        app.next_id
    );

    // Verify all 25 user tasks retained exact URLs, settings, and mutated progress
    for (expected_id, expected_url) in added_user_urls {
        let item = app.downloads.iter().find(|d| d.id == expected_id).expect("User task missing");
        assert_eq!(item.url, expected_url);
        assert_eq!(item.max_connections, 16);
        assert_eq!(item.status, DownloadStatus::Downloading);
        assert!(item.downloaded_bytes > 0);
    }
}

// ============================================================================
// TEST 2: Deletion of Existing Task While Reconciliation is Computing
// ============================================================================
#[test]
fn test_challenger_m3_deletion_tombstone_isolation_and_no_resurrection() {
    let test_dir = TempTestDir::new("delete_tombstone");
    let db_path = test_dir.db_path();

    let db = Database::open(&db_path).unwrap();
    for id in 1..=5 {
        let item = make_item(id, DownloadStatus::Paused, None);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
    }
    drop(db);

    let mut app = create_app_with_db(&db_path);

    // Hold reconciliation snapshot
    let (tx, rx) = std::sync::mpsc::channel();
    app.reconcile_rx = Some(rx);
    app.is_reconciling = true;

    // Simulate snapshot prepared from initial state: tasks 1, 2, 3, 4, 5
    let stale_snapshot = vec![
        make_item(1, DownloadStatus::Paused, None),
        make_item(2, DownloadStatus::Paused, None),
        make_item(3, DownloadStatus::Paused, None),
        make_item(4, DownloadStatus::Paused, None),
        make_item(5, DownloadStatus::Paused, None),
    ];

    // While reconciliation is pending:
    // User deletes task 2 (purged via tombstone)
    app.deleted_task_ids.insert(2);
    app.db.delete_download(2).unwrap();

    // User also deletes task 3
    app.deleted_task_ids.insert(3);
    app.db.delete_download(3).unwrap();

    // User initiates deletion for task 4 (still in deleting_task_ids)
    app.deleting_task_ids.insert(4);
    app.mark_task_mutated(4);

    // Stale snapshot arrives containing all 5 tasks
    tx.send(Ok(stale_snapshot)).unwrap();
    app.check_background_reconciliation();

    // Assert: Tasks 2 and 3 MUST NOT be resurrected in app.downloads
    let remaining_ids: Vec<u64> = app.downloads.iter().map(|d| d.id).collect();
    assert!(!remaining_ids.contains(&2), "Task 2 was resurrected! Tombstone violated.");
    assert!(!remaining_ids.contains(&3), "Task 3 was resurrected! Tombstone violated.");
    assert!(!remaining_ids.contains(&4), "Task 4 was resurrected! In-flight delete violated.");

    assert_eq!(remaining_ids, vec![1, 5], "Only tasks 1 and 5 should be present");

    // DB asserts: tasks 2 and 3 do not exist in SQLite
    assert!(!app.db.download_exists(2).unwrap(), "Task 2 should not exist in DB");
    assert!(!app.db.download_exists(3).unwrap(), "Task 3 should not exist in DB");
    assert!(app.db.download_exists(1).unwrap(), "Task 1 must exist in DB");
    assert!(app.db.download_exists(5).unwrap(), "Task 5 must exist in DB");
}

// ============================================================================
// TEST 3: Bulk Deletion (`delete_completed`) While Reconciliation is Computing
// ============================================================================
#[test]
fn test_challenger_m3_bulk_delete_completed_purges_permanently_during_reconciliation() {
    let test_dir = TempTestDir::new("bulk_delete");
    let db_path = test_dir.db_path();

    let db = Database::open(&db_path).unwrap();
    // 10 tasks: 5 Completed (even IDs 2, 4, 6, 8, 10), 5 Paused (odd IDs 1, 3, 5, 7, 9)
    for id in 1..=10 {
        let status = if id % 2 == 0 {
            DownloadStatus::Completed
        } else {
            DownloadStatus::Paused
        };
        let item = make_item(id, status, None);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
    }
    drop(db);

    let mut app = create_app_with_db(&db_path);

    // Hold snapshot
    let (tx, rx) = std::sync::mpsc::channel();
    app.reconcile_rx = Some(rx);
    app.is_reconciling = true;

    // Snapshot was captured before delete_completed
    let mut stale_items = Vec::new();
    for id in 1..=10 {
        let status = if id % 2 == 0 {
            DownloadStatus::Completed
        } else {
            DownloadStatus::Paused
        };
        stale_items.push(make_item(id, status, None));
    }

    // User also completes a new task 11 in memory during startup
    let item_11 = make_item(11, DownloadStatus::Completed, None);
    app.db.insert_download(&item_11, &DownloadSettings::default()).unwrap();
    app.downloads.push(item_11);

    // User triggers bulk deletion of completed downloads
    app.delete_completed();

    // Verify DB purged completed items immediately
    let check_conn = rusqlite::Connection::open(&db_path).unwrap();
    let completed_count: i64 = check_conn
        .query_row(
            "SELECT COUNT(*) FROM downloads WHERE status = 'Completed'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(completed_count, 0, "All completed rows must be removed from DB");
    drop(check_conn);

    // Now stale snapshot arrives with all 10 original items (including completed 2, 4, 6, 8, 10)
    tx.send(Ok(stale_items)).unwrap();
    app.check_background_reconciliation();

    // Assert: Completed items are PERMANENTLY PURGED and NEVER RESURRECTED
    let final_ids: Vec<u64> = app.downloads.iter().map(|d| d.id).collect();
    assert_eq!(
        final_ids,
        vec![1, 3, 5, 7, 9],
        "Only the 5 non-completed tasks must remain!"
    );

    for id in [2, 4, 6, 8, 10, 11] {
        assert!(
            !final_ids.contains(&id),
            "Completed task #{} was resurrected after delete_completed!",
            id
        );
        assert!(
            !app.db.download_exists(id).unwrap(),
            "Completed task #{} must not exist in SQLite",
            id
        );
    }
}

// ============================================================================
// TEST 4: User Mutation Preservation Against Stale Snapshot
// ============================================================================
#[test]
fn test_challenger_m3_mutation_preservation_against_stale_snapshot() {
    let test_dir = TempTestDir::new("mutation_preservation");
    let db_path = test_dir.db_path();

    let db = Database::open(&db_path).unwrap();
    // Task 1: Paused in DB
    let item1 = make_item(1, DownloadStatus::Paused, None);
    db.insert_download(&item1, &DownloadSettings::default()).unwrap();

    // Task 2: Downloading in DB
    let item2 = make_item(2, DownloadStatus::Downloading, None);
    db.insert_download(&item2, &DownloadSettings::default()).unwrap();

    // Task 3: Paused in DB
    let item3 = make_item(3, DownloadStatus::Paused, None);
    db.insert_download(&item3, &DownloadSettings::default()).unwrap();
    drop(db);

    let mut app = create_app_with_db(&db_path);

    let (tx, rx) = std::sync::mpsc::channel();
    app.reconcile_rx = Some(rx);
    app.is_reconciling = true;

    // Simulate in-memory user interaction before reconciliation finishes:
    // 1. Task 1 was started by user: now Downloading with 5MB downloaded and speed 2MB/s
    let mut mut_item1 = make_item(1, DownloadStatus::Downloading, None);
    mut_item1.total_size = None; // Metadata not yet discovered by user session
    mut_item1.downloaded_bytes = 5_000_000;
    mut_item1.transfer_rate = 2_000_000.0;
    mut_item1.time_left_secs = Some(15);
    app.downloads.push(mut_item1);
    app.mark_task_mutated(1);

    // 2. Task 2 was paused by user: now Paused
    let mut mut_item2 = make_item(2, DownloadStatus::Paused, None);
    mut_item2.downloaded_bytes = 3_000_000;
    mut_item2.transfer_rate = 0.0;
    mut_item2.time_left_secs = None;
    app.downloads.push(mut_item2);
    app.mark_task_mutated(2);

    // 3. Task 3 was canceled by user: now Canceled
    let mut mut_item3 = make_item(3, DownloadStatus::Canceled, None);
    mut_item3.downloaded_bytes = 1_000_000;
    mut_item3.transfer_rate = 0.0;
    mut_item3.time_left_secs = None;
    app.downloads.push(mut_item3);
    app.mark_task_mutated(3);

    // Stale DB snapshot arrives with pre-mutation values
    let stale_snapshot = vec![
        {
            let mut it = make_item(1, DownloadStatus::Paused, None);
            it.downloaded_bytes = 0;
            it.total_size = Some(20_000_000);
            it.expected_sha256 = Some("abcdef1234567890".into());
            it.hash_status = HashVerificationStatus::Verified;
            it
        },
        {
            let mut it = make_item(2, DownloadStatus::Downloading, None);
            it.downloaded_bytes = 1_000_000;
            it.transfer_rate = 500_000.0;
            it
        },
        {
            let mut it = make_item(3, DownloadStatus::Paused, None);
            it.downloaded_bytes = 1_000_000;
            it
        },
    ];

    tx.send(Ok(stale_snapshot)).unwrap();
    app.check_background_reconciliation();

    assert_eq!(app.downloads.len(), 3);

    // Task 1 assertions: must retain user's Downloading state and progress, but backfill metadata
    let t1 = app.downloads.iter().find(|d| d.id == 1).unwrap();
    assert_eq!(t1.status, DownloadStatus::Downloading, "Task 1 status must remain Downloading");
    assert_eq!(t1.downloaded_bytes, 5_000_000, "Task 1 bytes must not regress to 0");
    assert_eq!(t1.transfer_rate, 2_000_000.0);
    assert_eq!(t1.time_left_secs, Some(15));
    assert_eq!(t1.total_size, Some(20_000_000), "Missing total_size must be backfilled");
    assert_eq!(t1.expected_sha256, Some("abcdef1234567890".into()), "Expected hash must be backfilled");
    assert_eq!(t1.hash_status, HashVerificationStatus::Verified, "Hash status must be backfilled");

    // Task 2 assertions: must retain user's Paused state (not regressed to Downloading)
    let t2 = app.downloads.iter().find(|d| d.id == 2).unwrap();
    assert_eq!(t2.status, DownloadStatus::Paused, "Task 2 status must remain Paused");
    assert_eq!(t2.transfer_rate, 0.0);

    // Task 3 assertions: must retain user's Canceled state (not regressed to Paused)
    let t3 = app.downloads.iter().find(|d| d.id == 3).unwrap();
    assert_eq!(t3.status, DownloadStatus::Canceled, "Task 3 status must remain Canceled");
}

// ============================================================================
// TEST 5: Clean Startup With Empty DB — Sample Suppression & Baseline
// ============================================================================
#[test]
fn test_challenger_m3_empty_db_clean_startup_sample_suppression_and_baseline() {
    // Sub-case A: User adds a task during startup -> Sample downloads MUST BE SUPPRESSED
    {
        let test_dir = TempTestDir::new("clean_start_suppressed");
        let db_path = test_dir.db_path();
        let db = Database::open(&db_path).unwrap();
        assert_eq!(db.get_download_count().unwrap(), 0);
        drop(db);

        let mut app = create_app_with_db(&db_path);
        let (tx, rx) = std::sync::mpsc::channel();
        app.reconcile_rx = Some(rx);
        app.is_reconciling = true;

        // User adds a task during startup
        let user_task = make_item(app.next_id, DownloadStatus::Queued, Some("https://example.com/custom.zip"));
        app.next_id += 1;
        app.db.insert_download(&user_task, &DownloadSettings::default()).unwrap();
        app.downloads.push(user_task);

        // Reconciler completes with empty list
        tx.send(Ok(vec![])).unwrap();
        app.check_background_reconciliation();

        assert_eq!(
            app.downloads.len(),
            1,
            "Sample downloads must be SUPPRESSED when user adds a download during startup"
        );
        assert_eq!(app.downloads[0].url, "https://example.com/custom.zip");
        assert_eq!(app.downloads[0].id, 1);
    }

    // Sub-case B: Baseline — User adds nothing -> Sample downloads ARE populated
    {
        let test_dir = TempTestDir::new("clean_start_baseline");
        let db_path = test_dir.db_path();
        let db = Database::open(&db_path).unwrap();
        assert_eq!(db.get_download_count().unwrap(), 0);
        drop(db);

        let mut app = create_app_with_db(&db_path);
        let (tx, rx) = std::sync::mpsc::channel();
        app.reconcile_rx = Some(rx);
        app.is_reconciling = true;

        // Reconciler completes with empty list
        tx.send(Ok(vec![])).unwrap();
        app.check_background_reconciliation();

        assert_eq!(
            app.downloads.len(),
            16,
            "Sample downloads must be populated on clean start when user adds nothing"
        );
        assert_eq!(app.downloads[0].id, 1);
        assert_eq!(app.downloads[15].id, 16);
    }
}

// ============================================================================
// TEST 6: CleanupPending State Survives Reconciliation and Allows Retry
// ============================================================================
#[test]
fn test_challenger_m3_cleanup_pending_survives_reconciliation_and_allows_retry() {
    let test_dir = TempTestDir::new("cleanup_pending_survives");
    let db_path = test_dir.db_path();

    let db = Database::open(&db_path).unwrap();
    let item = make_item(100, DownloadStatus::Downloading, None);
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    drop(db);

    let mut app = create_app_with_db(&db_path);
    let (tx, rx) = std::sync::mpsc::channel();
    app.reconcile_rx = Some(rx);
    app.is_reconciling = true;

    // Simulate task 100 in memory transitioning to CleanupPending due to file lock
    let mut pending_item = make_item(100, DownloadStatus::CleanupPending, None);
    pending_item.description = "Cleanup failed: os error 32".into();
    app.downloads.push(pending_item);
    app.mark_task_mutated(100);
    app.cleanup_errors.insert(100, "os error 32".into());
    app.db.update_status(100, &DownloadStatus::CleanupPending).unwrap();

    // Stale snapshot arrives with task 100 as Downloading
    let stale_snapshot = vec![make_item(100, DownloadStatus::Downloading, None)];
    tx.send(Ok(stale_snapshot)).unwrap();
    app.check_background_reconciliation();

    // Assert: CleanupPending is NOT clobbered by stale Downloading status
    assert_eq!(app.downloads.len(), 1);
    assert_eq!(app.downloads[0].id, 100);
    assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);
    assert!(app.cleanup_errors.contains_key(&100));

    // Now test retry cleanup flow
    app.selected_id = Some(100);
    app.retry_cleanup_selected();

    assert!(app.deleting_task_ids.contains(&100));

    // Simulate engine confirming deletion
    let (evt_tx, evt_rx) = tokio::sync::mpsc::unbounded_channel();
    app.engine.event_receiver = evt_rx;

    evt_tx.send(EngineEvent::Deleted {
        id: 100,
        cleanup_success: true,
        error: None,
    }).unwrap();

    app.poll_engine_events();

    // Assert task 100 is completely purged
    assert_eq!(app.downloads.len(), 0);
    assert!(app.deleted_task_ids.contains(&100));
    assert!(!app.db.download_exists(100).unwrap());
}

// ============================================================================
// TEST 8: Reproduction of Bug — Deleted Task Resurrected via Sample Downloads
// ============================================================================
#[test]
fn test_reproduce_bug_deleted_task_resurrected_via_sample_injection() {
    let test_dir = TempTestDir::new("reproduce_resurrection_bug");
    let db_path = test_dir.db_path();

    // User had 1 download in their database (ID: 1)
    let db = Database::open(&db_path).unwrap();
    let item = make_item(1, DownloadStatus::Completed, Some("https://mysite.com/my_document.pdf"));
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    assert_eq!(db.get_download_count().unwrap(), 1);
    drop(db);

    let mut app = create_app_with_db(&db_path);
    let (tx, rx) = std::sync::mpsc::channel();
    app.reconcile_rx = Some(rx);
    app.is_reconciling = true;

    // Snapshot was captured by background thread containing [Task 1]
    let stale_snapshot = vec![make_item(1, DownloadStatus::Completed, Some("https://mysite.com/my_document.pdf"))];

    // During startup, user deletes Task 1 via bulk deletion or single delete
    app.delete_completed();
    // Or single delete:
    app.deleted_task_ids.insert(1);
    let _ = app.db.delete_download(1);

    assert_eq!(app.db.get_download_count().unwrap(), 0);
    assert!(app.deleted_task_ids.contains(&1));

    // Deliver snapshot
    tx.send(Ok(stale_snapshot)).unwrap();
    app.check_background_reconciliation();

    // BUG VERIFICATION:
    // User deleted task 1, but check_background_reconciliation triggers populate_default_sample_downloads()
    // because app.downloads.is_empty() && db.get_download_count() == 0!
    // As a result, ID 1 is resurrected as Kubuntu ISO!
    let id_1_present = app.downloads.iter().any(|d| d.id == 1);
    println!("Is Task 1 present in app.downloads after deletion? {}", id_1_present);
    if id_1_present {
        let task_1 = app.downloads.iter().find(|d| d.id == 1).unwrap();
        println!("Task 1 details: id={}, url={}, filename={}", task_1.id, task_1.url, task_1.filename);
    }

    // Invariant: Deleted task must NOT be resurrected in UI or DB!
    assert!(
        !id_1_present,
        "CRITICAL BUG CONFIRMED: Deleted task #1 was resurrected by populate_default_sample_downloads()!"
    );
    assert_eq!(
        app.downloads.len(),
        0,
        "No sample downloads should be injected when user has deleted tasks!"
    );
}
#[test]
fn test_challenger_m3_randomized_stress_500_interleavings() {
    let test_dir = TempTestDir::new("random_stress");
    let db_path = test_dir.db_path();

    // Deterministic PRNG implementation (Xoshiro256++)
    struct SimplePrng {
        s: [u64; 4],
    }
    impl SimplePrng {
        fn new(seed: u64) -> Self {
            let mut s = [0u64; 4];
            let mut z = seed;
            for i in 0..4 {
                z = z.wrapping_add(0x9E3779B97F4A7C15);
                let mut x = z;
                x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
                s[i] = x ^ (x >> 31);
            }
            Self { s }
        }
        fn next_u64(&mut self) -> u64 {
            let result = (self.s[0].wrapping_add(self.s[3]))
                .rotate_left(23)
                .wrapping_add(self.s[0]);
            let t = self.s[1] << 17;
            self.s[2] ^= self.s[0];
            self.s[3] ^= self.s[1];
            self.s[1] ^= self.s[2];
            self.s[0] ^= self.s[3];
            self.s[2] ^= t;
            self.s[3] = self.s[3].rotate_left(45);
            result
        }
        fn next_range(&mut self, min: usize, max: usize) -> usize {
            if min >= max {
                return min;
            }
            min + (self.next_u64() as usize % (max - min + 1))
        }
    }

    let seeds = [
        0x1234_5678_9ABC_DEF0u64,
        0xFEDC_BA98_7654_3210u64,
        0xDEAD_BEEF_CAFE_BABEu64,
        0x0123_4567_89AB_CDEFu64,
    ];

    for &seed in &seeds {
        let mut prng = SimplePrng::new(seed);

        for iter in 0..125 { // 4 seeds * 125 = 500 total interleavings
            // Setup DB with random number of initial tasks (1 to 15)
            let _ = std::fs::remove_file(&db_path);
            let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
            let _ = std::fs::remove_file(db_path.with_extension("db-shm"));
            let db = Database::open(&db_path).unwrap();
            let initial_count = prng.next_range(1, 15);

            for id in 1..=initial_count as u64 {
                let status = match prng.next_range(0, 3) {
                    0 => DownloadStatus::Paused,
                    1 => DownloadStatus::Downloading,
                    2 => DownloadStatus::Completed,
                    _ => DownloadStatus::Canceled,
                };
                let it = make_item(id, status, None);
                db.insert_download(&it, &DownloadSettings::default()).unwrap();
            }
            drop(db);

            let mut app = create_app_with_db(&db_path);
            let (tx, rx) = std::sync::mpsc::channel();
            app.reconcile_rx = Some(rx);
            app.is_reconciling = true;

            // Generate stale snapshot of the initial DB items
            let stale_db = Database::open(&db_path).unwrap();
            let stale_snapshot = stale_db.reconcile_and_load_all().unwrap();
            drop(stale_db);

            // Perform randomized user actions BEFORE reconciliation snapshot arrives
            let action_count = prng.next_range(2, 8);
            let mut expected_deleted_ids = HashSet::new();

            if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                println!("--- DEBUG ITER 41 ---");
                println!("initial_count: {}", initial_count);
                println!("initial snapshot IDs: {:?}", stale_snapshot.iter().map(|d| d.id).collect::<Vec<_>>());
            }

            for step in 0..action_count {
                let action = prng.next_range(0, 4);
                if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                    println!("step {}: action {}", step, action);
                }
                match action {
                    0 => {
                        // User adds a task
                        let id = app.next_id;
                        app.next_id += 1;
                        let item = make_item(id, DownloadStatus::Paused, None);
                        app.db.insert_download(&item, &DownloadSettings::default()).unwrap();
                        app.mark_task_mutated(id);
                        app.downloads.push(item);
                        if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                            println!("  added task {}", id);
                        }
                    }
                    1 => {
                        // User deletes an existing task
                        let target_id = prng.next_range(1, initial_count) as u64;
                        app.deleted_task_ids.insert(target_id);
                        let _ = app.db.delete_download(target_id);
                        app.downloads.retain(|d| d.id != target_id);
                        expected_deleted_ids.insert(target_id);
                        if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                            println!("  deleted task {}", target_id);
                        }
                    }
                    2 => {
                        // User bulk deletes completed
                        app.delete_completed();
                        if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                            println!("  delete_completed called");
                        }
                    }
                    3 => {
                        // User mutates a task status
                        let mut target_id = None;
                        if let Some(d) = app.downloads.first_mut() {
                            d.status = DownloadStatus::Paused;
                            d.downloaded_bytes += 500;
                            target_id = Some(d.id);
                        }
                        if let Some(id) = target_id {
                            app.mark_task_mutated(id);
                        }
                        if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                            println!("  mutated task {:?}", target_id);
                        }
                    }
                    _ => {}
                }
            }

            if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                println!("expected_deleted_ids: {:?}", expected_deleted_ids);
                println!("app.deleted_task_ids: {:?}", app.deleted_task_ids);
                println!("app.downloads before rec: {:?}", app.downloads.iter().map(|d| d.id).collect::<Vec<_>>());
            }

            // Deliver stale snapshot
            tx.send(Ok(stale_snapshot)).unwrap();
            app.check_background_reconciliation();

            if iter == 41 && seed == 0x1234_5678_9ABC_DEF0 {
                println!("app.downloads after rec: {:?}", app.downloads.iter().map(|d| d.id).collect::<Vec<_>>());
            }

            // Invariant assertions:
            // 1. No duplicate IDs (No primary key collisions)
            let mut seen = HashSet::new();
            for d in &app.downloads {
                assert!(
                    seen.insert(d.id),
                    "Seed {:#X}, iter {}: Duplicate ID #{} found in downloads!",
                    seed, iter, d.id
                );
            }

            // 2. No tombstone resurrecting
            for deleted_id in &expected_deleted_ids {
                assert!(
                    !seen.contains(deleted_id),
                    "Seed {:#X}, iter {}: Deleted task #{} was resurrected!",
                    seed, iter, deleted_id
                );
            }

            // 3. next_id strictly advances
            let max_id = app.downloads.iter().map(|d| d.id).max().unwrap_or(0);
            assert!(
                app.next_id > max_id,
                "Seed {:#X}, iter {}: next_id ({}) must be strictly greater than max_id ({})",
                seed, iter, app.next_id, max_id
            );
        }
    }
}
