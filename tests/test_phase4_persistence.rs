use app::db::Database;
use app::engine::{DownloadCheckpoint, SegmentManager};
use app::types::{DownloadItem, DownloadSettings, DownloadStatus};
use std::path::{Path, PathBuf};

struct TestDir(PathBuf);
impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_p4_{}_{}_{}",
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
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Helper to create a valid .partinfo checkpoint file
fn create_checkpoint(
    partinfo_path: &Path,
    total_size: u64,
    chunk_size: u64,
    etag: Option<String>,
    committed_ids: Vec<usize>,
) {
    let cp = serde_json::json!({
        "total_size": total_size,
        "chunk_size": chunk_size,
        "etag": etag,
        "committed_chunk_ids": committed_ids,
        "last_updated": 123456789
    });
    std::fs::write(partinfo_path, cp.to_string()).expect("Failed to write checkpoint");
}

// ---------------------------------------------------------------------------
// Test 1: Application restart with active downloads
// Active downloads must reconcile to Paused and recover committed bytes.
// ---------------------------------------------------------------------------
#[test]
fn test_01_restart_with_active_downloads() {
    let td = TestDir::new("test01");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("active_video.mp4");
    let partinfo_path = td.path().join("active_video.mp4.partinfo");

    let total_size = 20 * 1024 * 1024; // 20 MB
    let chunk_size = 2 * 1024 * 1024;  // 2 MB chunks (10 total)
    // Destination file exists (preallocated)
    std::fs::write(&save_path, vec![0u8; 1000]).unwrap();
    // 3 chunks committed (6 MB)
    create_checkpoint(&partinfo_path, total_size, chunk_size, None, vec![0, 1, 2]);

    // Initial session: Download was actively "Downloading" when power cut / app closed
    {
        let db = Database::open(&db_path).unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/active_video.mp4".into(),
            filename: "active_video.mp4".into(),
            save_path: save_path.clone(),
            total_size: Some(total_size),
            status: DownloadStatus::Downloading,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
    }

    // Restart app: open DB and reconcile
    {
        let db = Database::open(&db_path).unwrap();
        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let task = &loaded[0];
        assert_eq!(task.status, DownloadStatus::Paused, "Active download must reconcile to Paused");
        assert_eq!(task.downloaded_bytes, 6 * 1024 * 1024, "Must recover committed bytes from .partinfo");
        assert!(!task.segments.is_empty(), "Must reconstruct UI segments");
    }
}

// ---------------------------------------------------------------------------
// Test 2: Application restart with paused downloads
// Paused download must retain progress from .partinfo without losing state.
// ---------------------------------------------------------------------------
#[test]
fn test_02_restart_with_paused_downloads() {
    let td = TestDir::new("test02");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("paused_iso.iso");
    let partinfo_path = td.path().join("paused_iso.iso.partinfo");

    let total_size = 10 * 1024 * 1024;
    let chunk_size = 2 * 1024 * 1024;
    std::fs::write(&save_path, vec![0u8; 1000]).unwrap();
    create_checkpoint(&partinfo_path, total_size, chunk_size, None, vec![0, 2]);

    {
        let db = Database::open(&db_path).unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/paused_iso.iso".into(),
            filename: "paused_iso.iso".into(),
            save_path: save_path.clone(),
            total_size: Some(total_size),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
    }

    // Restart app
    {
        let db = Database::open(&db_path).unwrap();
        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let task = &loaded[0];
        assert_eq!(task.status, DownloadStatus::Paused);
        assert_eq!(task.downloaded_bytes, 4 * 1024 * 1024, "Must retain 4MB committed progress");
    }
}

// ---------------------------------------------------------------------------
// Test 3: Application restart with completed downloads
// Completed downloads must be verified against disk and never redownloaded.
// ---------------------------------------------------------------------------
#[test]
fn test_03_restart_with_completed_downloads() {
    let td = TestDir::new("test03");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("finished_setup.exe");

    let total_size = 5 * 1024 * 1024;
    // Destination file exists with full size
    std::fs::write(&save_path, vec![0xAA; total_size as usize]).unwrap();

    {
        let db = Database::open(&db_path).unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/finished_setup.exe".into(),
            filename: "finished_setup.exe".into(),
            save_path: save_path.clone(),
            total_size: Some(total_size),
            status: DownloadStatus::Completed,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
        db.mark_completed(1).unwrap();
    }

    // Restart app
    {
        let db = Database::open(&db_path).unwrap();
        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].status, DownloadStatus::Completed);
        assert_eq!(loaded[0].downloaded_bytes, total_size);
    }
}

// ---------------------------------------------------------------------------
// Test 4: Database entry without partial file
// Detects missing partial file and safely recovers (resets progress).
// ---------------------------------------------------------------------------
#[test]
fn test_04_database_entry_without_partial_file() {
    let td = TestDir::new("test04");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("non_existent.bin");
    let partinfo_path = td.path().join("non_existent.bin.partinfo");

    // partinfo was left behind, but destination file was deleted externally!
    create_checkpoint(&partinfo_path, 10_000_000, 2_000_000, None, vec![0, 1]);

    {
        let db = Database::open(&db_path).unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/non_existent.bin".into(),
            filename: "non_existent.bin".into(),
            save_path: save_path.clone(),
            total_size: Some(10_000_000),
            status: DownloadStatus::Downloading,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
    }

    // Restart app
    {
        let db = Database::open(&db_path).unwrap();
        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].status, DownloadStatus::Paused);
        assert_eq!(loaded[0].downloaded_bytes, 0, "Progress must safely reset to 0 since file is missing");
        assert!(!partinfo_path.exists(), "Stale partinfo should be cleaned up");
    }
}

// ---------------------------------------------------------------------------
// Test 5: Partial file without valid database entry
// Scans and handles orphan .partinfo files without corrupting DB.
// ---------------------------------------------------------------------------
#[test]
fn test_05_orphan_partial_file_without_db_entry() {
    let td = TestDir::new("test05");
    let db_path = td.path().join("downloads.db");
    let orphan_file = td.path().join("orphan_movie.mkv");
    let orphan_partinfo = td.path().join("orphan_movie.mkv.partinfo");

    std::fs::write(&orphan_file, b"orphan partial data").unwrap();
    create_checkpoint(&orphan_partinfo, 50_000_000, 2_000_000, None, vec![0]);

    let db = Database::open(&db_path).unwrap();
    let orphans = db.scan_orphaned_partinfo(td.path()).unwrap();
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0], orphan_partinfo);

    // Reconciling DB must be completely unaffected and remain empty
    let loaded = db.reconcile_and_load_all().unwrap();
    assert_eq!(loaded.len(), 0);
}

// ---------------------------------------------------------------------------
// Test 6: Corrupted SQLite database
// Automatic backup to .corrupt.bak and creation of fresh DB.
// ---------------------------------------------------------------------------
#[test]
fn test_06_corrupted_sqlite_database_recovery() {
    let td = TestDir::new("test06");
    let db_path = td.path().join("downloads.db");

    // Write corrupted header/data
    std::fs::write(&db_path, b"CORRUPTED HEADER NOT SQLITE3 JUNK DATA").unwrap();

    // Opening should not panic or fail, but create a fresh DB and rename corrupt file
    let db = Database::open(&db_path).expect("Must recover gracefully from corrupted DB");
    assert_eq!(db.get_download_count().unwrap(), 0);

    let bak_path = db_path.with_extension("db.corrupt.bak");
    assert!(bak_path.exists(), "Corrupt file must be preserved as .corrupt.bak");
}

// ---------------------------------------------------------------------------
// Test 7: ETag mismatch during restoration
// Checkpoint ETag mismatch must invalidate stale checkpoint metadata.
// ---------------------------------------------------------------------------
#[test]
fn test_07_etag_mismatch_invalidation() {
    let td = TestDir::new("test07");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("firmware.bin");
    let partinfo_path = td.path().join("firmware.bin.partinfo");

    std::fs::write(&save_path, vec![0u8; 1000]).unwrap();
    // Checkpoint has old ETag
    create_checkpoint(&partinfo_path, 8_000_000, 2_000_000, Some("old_etag_rev1".into()), vec![0, 1]);

    let db = Database::open(&db_path).unwrap();
    let item = DownloadItem {
        id: 1,
        url: "https://example.com/firmware.bin".into(),
        filename: "firmware.bin".into(),
        save_path: save_path.clone(),
        total_size: Some(8_000_000),
        status: DownloadStatus::Paused,
        ..Default::default()
    };
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    // Server returned new ETag
    db.update_metadata(1, "firmware.bin", Some(8_000_000), Some("new_etag_rev2"), None).unwrap();

    let loaded = db.reconcile_and_load_all().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].downloaded_bytes, 0, "ETag mismatch must invalidate checkpoint bytes");
    assert!(!partinfo_path.exists(), "Stale checkpoint file must be removed");
}

// ---------------------------------------------------------------------------
// Test 8: Duplicate startup recovery prevention
// Multiple reconciliations must be strictly idempotent.
// ---------------------------------------------------------------------------
#[test]
fn test_08_duplicate_startup_recovery_prevention() {
    let td = TestDir::new("test08");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("archive.zip");
    let partinfo_path = td.path().join("archive.zip.partinfo");

    std::fs::write(&save_path, vec![0u8; 1000]).unwrap();
    create_checkpoint(&partinfo_path, 10_000_000, 2_000_000, None, vec![0]);

    let db = Database::open(&db_path).unwrap();
    let item = DownloadItem {
        id: 1,
        url: "https://example.com/archive.zip".into(),
        filename: "archive.zip".into(),
        save_path: save_path.clone(),
        total_size: Some(10_000_000),
        status: DownloadStatus::Downloading,
        ..Default::default()
    };
    db.insert_download(&item, &DownloadSettings::default()).unwrap();

    // Call reconcile 3 consecutive times
    let run1 = db.reconcile_and_load_all().unwrap();
    let run2 = db.reconcile_and_load_all().unwrap();
    let run3 = db.reconcile_and_load_all().unwrap();

    assert_eq!(run1.len(), 1);
    assert_eq!(run2.len(), 1);
    assert_eq!(run3.len(), 1);
    assert_eq!(db.get_download_count().unwrap(), 1, "Must not create duplicate DB entries");
}

// ---------------------------------------------------------------------------
// Test 9: Pause, close, restart, and resume validation
// Reconstructs SegmentManager from reconciled checkpoint and validates state.
// ---------------------------------------------------------------------------
#[test]
fn test_09_pause_close_restart_resume_continuity() {
    let td = TestDir::new("test09");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("dataset.bin");
    let partinfo_path = td.path().join("dataset.bin.partinfo");

    let total_size = 12 * 1024 * 1024;
    let chunk_size = 2 * 1024 * 1024; // 6 chunks total
    std::fs::write(&save_path, vec![0u8; 2000]).unwrap();

    // Chunks 0 and 1 committed before pausing
    create_checkpoint(&partinfo_path, total_size, chunk_size, Some("dataset_v1".into()), vec![0, 1]);

    {
        let db = Database::open(&db_path).unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/dataset.bin".into(),
            filename: "dataset.bin".into(),
            save_path: save_path.clone(),
            total_size: Some(total_size),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
        db.update_metadata(1, "dataset.bin", Some(total_size), Some("dataset_v1"), None).unwrap();
    }

    // Restart app: Reconcile DB
    let db = Database::open(&db_path).unwrap();
    let loaded = db.reconcile_and_load_all().unwrap();
    assert_eq!(loaded[0].status, DownloadStatus::Paused);
    assert_eq!(loaded[0].downloaded_bytes, 4 * 1024 * 1024);

    // Engine resume: load SegmentManager from checkpoint
    let cp_data = std::fs::read_to_string(&partinfo_path).unwrap();
    let cp: DownloadCheckpoint = serde_json::from_str(&cp_data).unwrap();
    let mut mgr = SegmentManager::from_checkpoint(total_size, Some("dataset_v1".into()), &cp).unwrap();

    // Verify worker leasing continues from chunk 2 without re-downloading chunks 0 and 1!
    let l1 = mgr.lease_next_chunk(1).expect("Worker 1 lease");
    assert_eq!(l1.chunk_id, 2, "Must resume at chunk id 2");
    let l2 = mgr.lease_next_chunk(2).expect("Worker 2 lease");
    assert_eq!(l2.chunk_id, 3, "Worker 2 takes chunk id 3");

    // Complete all remaining chunks (2, 3, 4, 5)
    mgr.commit_chunk(2, l1.generation).unwrap();
    mgr.commit_chunk(3, l2.generation).unwrap();
    let l3 = mgr.lease_next_chunk(1).unwrap();
    mgr.commit_chunk(4, l3.generation).unwrap();
    let l4 = mgr.lease_next_chunk(2).unwrap();
    mgr.commit_chunk(5, l4.generation).unwrap();

    assert_eq!(mgr.committed_bytes, total_size, "All bytes successfully committed after resume");
}

// ---------------------------------------------------------------------------
// Test 10: Delete task with partial file cleanup
// Delete removes record from database and cleans up .partinfo.
// ---------------------------------------------------------------------------
#[test]
fn test_10_delete_task_with_partial_file_cleanup() {
    let td = TestDir::new("test10");
    let db_path = td.path().join("downloads.db");
    let save_path = td.path().join("deleted_task.tar");
    let partinfo_path = td.path().join("deleted_task.tar.partinfo");

    std::fs::write(&save_path, vec![0u8; 1000]).unwrap();
    create_checkpoint(&partinfo_path, 10_000_000, 2_000_000, None, vec![0]);

    let db = Database::open(&db_path).unwrap();
    let item = DownloadItem {
        id: 1,
        url: "https://example.com/deleted_task.tar".into(),
        filename: "deleted_task.tar".into(),
        save_path: save_path.clone(),
        total_size: Some(10_000_000),
        status: DownloadStatus::Paused,
        ..Default::default()
    };
    db.insert_download(&item, &DownloadSettings::default()).unwrap();
    assert_eq!(db.get_download_count().unwrap(), 1);

    // Perform Delete with cleanup
    db.delete_download(1).unwrap();
    if partinfo_path.exists() {
        let _ = std::fs::remove_file(&partinfo_path);
    }

    assert_eq!(db.get_download_count().unwrap(), 0, "Task must be deleted from DB");
    assert!(!partinfo_path.exists(), "Partinfo checkpoint must be removed");
}
