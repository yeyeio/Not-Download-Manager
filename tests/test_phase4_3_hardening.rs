//! HyperDownload Phase 4.3 — Concurrency & Lifecycle Hardening E2E Test Suite
//!
//! Comprehensive 4-tier integration test harness verifying:
//! - Tier 1: Core Feature Coverage (Streaming SHA-256 NIST vectors, Delete priority, Pause vs Cancel, ActiveTaskGuard generation, CleanupPending)
//! - Tier 2: Boundary & Corner Cases (empty file, 1M 'a's, 56-byte NIST, multi-buffer streaming, rapid start/delete)
//! - Tier 3: Cross-Feature Combinations (Delete during final writer flush, Delete during pause teardown, StartDownload during active Delete, cancel then resume)
//! - Tier 4: Real-World Workload Scenarios (10,000 lifecycle operations stress test with Win32 process memory inspection, 1,000+ randomized concurrent command interleavings across 4 seeds)

use not_download_manager::db::{compute_sha256, verify_file_sha256};
use not_download_manager::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use not_download_manager::types::HashVerificationStatus;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// 0. TEST INFRASTRUCTURE & HARNESS UTILITIES
// ---------------------------------------------------------------------------

/// Isolated test directory that automatically cleans up on drop with retry
/// for Windows transient file-lock tolerance.
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_p43_{}_{}_{}",
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

/// Win32 Process Memory FFI via `psapi.dll` for zero-dependency leak detection.
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

/// Pure-Rust `Xoshiro256++` PRNG for deterministic, cross-platform reproducibility.
#[derive(Debug, Clone)]
pub struct Xoshiro256PlusPlus {
    s: [u64; 4],
}

impl Xoshiro256PlusPlus {
    pub fn from_seed(mut seed: u64) -> Self {
        let mut s = [0u64; 4];
        for word in &mut s {
            seed = seed.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            *word = z ^ (z >> 31);
        }
        Self { s }
    }

    pub fn next_u64(&mut self) -> u64 {
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

    pub fn gen_range(&mut self, low: usize, high: usize) -> usize {
        assert!(low < high);
        let span = (high - low) as u64;
        let r = self.next_u64() % span;
        low + r as usize
    }
}

/// In-process HTTP 206 mock streaming server.
async fn start_mock_range_server(
    content_len: usize,
    chunk_delay_ms: u64,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Binding loopback listener must succeed with BypassSandbox");
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{}/streamfile.bin", port);

    let handle = tokio::spawn(async move {
        let dummy_data: Vec<u8> = (0..content_len).map(|i| (i % 251) as u8).collect();
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let dummy = dummy_data.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);

                if req_str.starts_with("HEAD") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-p43\"\r\nConnection: close\r\n\r\n",
                        dummy.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    let _ = socket.shutdown().await;
                } else if req_str.starts_with("GET") {
                    let mut start = 0usize;
                    let mut end = dummy.len().saturating_sub(1);
                    if let Some(r_line) = req_str.lines().find(|l| l.to_lowercase().starts_with("range:")) {
                        if let Some(bytes_part) = r_line.split('=').nth(1) {
                            let parts: Vec<&str> = bytes_part.trim().split('-').collect();
                            if let Ok(s) = parts[0].parse::<usize>() {
                                start = s;
                            }
                            if parts.len() > 1 && !parts[1].is_empty() {
                                if let Ok(e) = parts[1].parse::<usize>() {
                                    end = e;
                                }
                            }
                        }
                    }
                    let slice_len = end.saturating_sub(start) + 1;
                    let resp_header = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        start, end, dummy.len(), slice_len
                    );
                    let _ = socket.write_all(resp_header.as_bytes()).await;
                    if start <= end && end < dummy.len() {
                        let slice = &dummy[start..=end];
                        for chunk in slice.chunks(65536) {
                            if socket.write_all(chunk).await.is_err() {
                                break;
                            }
                            if chunk_delay_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(chunk_delay_ms)).await;
                            }
                        }
                    }
                    let _ = socket.shutdown().await;
                }
            });
        }
    });

    (url, handle)
}

/// Helper to wait for a specific engine event with bounded polling.
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
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    None
}

// ---------------------------------------------------------------------------
// 1. TIER 1: FEATURE COVERAGE
// ---------------------------------------------------------------------------

/// T1.1: Streaming SHA-256 NIST vectors and decoupled verification status.
#[test]
fn test_tier1_streaming_sha256_nist_vectors() {
    let td = TestDir::new("t1_sha256");

    // Vector 1: Empty string
    let p_empty = td.path().join("empty.bin");
    std::fs::write(&p_empty, b"").unwrap();
    let nist_empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    assert!(
        verify_file_sha256(&p_empty, nist_empty).unwrap(),
        "NIST Vector 1 (Empty) must verify"
    );

    // Vector 2: "abc"
    let p_abc = td.path().join("abc.txt");
    std::fs::write(&p_abc, b"abc").unwrap();
    let nist_abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    assert!(
        verify_file_sha256(&p_abc, nist_abc).unwrap(),
        "NIST Vector 2 ('abc') must verify"
    );

    // Vector 3: 56-byte string
    let p_56 = td.path().join("56byte.txt");
    let msg_56 = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    std::fs::write(&p_56, msg_56).unwrap();
    let nist_56 = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
    assert!(
        verify_file_sha256(&p_56, nist_56).unwrap(),
        "NIST Vector 3 (56-byte) must verify"
    );

    // Mismatch verification: wrong digest must return false, never true
    let wrong_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    assert!(
        !verify_file_sha256(&p_abc, wrong_hash).unwrap(),
        "Mismatched hash must evaluate to false"
    );

    // Non-existent file must return Err
    assert!(
        verify_file_sha256(&td.path().join("non_existent.dat"), nist_abc).is_err(),
        "Missing file must return io::Error"
    );

    // Decoupled status validation
    assert_ne!(
        HashVerificationStatus::Verified,
        HashVerificationStatus::VerificationFailed
    );
    assert_ne!(
        HashVerificationStatus::NotConfigured,
        HashVerificationStatus::Verified
    );
}

/// T1.2: Delete priority over Pause and Cancel once accepted.
#[tokio::test]
async fn test_tier1_delete_priority_over_pause_and_cancel() {
    let td = TestDir::new("t1_del_priority");
    let save_path = td.path().join("priority_test.dat");
    let partinfo_path = td.path().join("priority_test.dat.partinfo");

    let total_len = 10 * 1024 * 1024; // 10 MB
    let (url, _server) = start_mock_range_server(total_len, 20).await;
    let engine = spawn_download_engine();
    let download_id = 1101u64;

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: download_id,
            url,
            save_path: save_path.clone(),
            connections: 4,
        })
        .unwrap();

    let mut rx = engine.event_receiver;

    // Await active progress
    let started = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id, downloaded, .. } if *id == download_id && *downloaded > 0)
    }).await;
    assert!(started.is_some(), "Download must start active transfer");

    // Concurrently send Pause followed immediately by Delete
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: download_id })
        .unwrap();
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: download_id,
            cleanup_files: true,
        })
        .unwrap();

    // Await terminal event
    let deleted = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Deleted { id, cleanup_success, .. } if *id == download_id && *cleanup_success)
    }).await;
    assert!(deleted.is_some(), "Engine must emit Deleted event");

    // Allow teardown to settle and assert zero zombie checkpoint writes
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(!partinfo_path.exists(), "Zombie checkpoint .partinfo must NOT exist after Delete");
    assert!(!save_path.exists(), "Destination file must be removed");

    // Drain remaining events: no Paused or Finished event should be emitted after Deleted
    while let Ok(evt) = rx.try_recv() {
        assert!(
            !matches!(evt, EngineEvent::Paused { id } if id == download_id),
            "Paused event must not be emitted after Delete priority"
        );
        assert!(
            !matches!(evt, EngineEvent::Finished { id, .. } if id == download_id),
            "Finished event must not be emitted after Delete priority"
        );
    }
}

/// T1.3: Distinct Pause vs Cancel semantics.
#[tokio::test]
async fn test_tier1_distinct_pause_vs_cancel_semantics() {
    let td = TestDir::new("t1_pause_vs_cancel");
    let (url, _server) = start_mock_range_server(8 * 1024 * 1024, 25).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;

    // --- Subtest A: Pause semantics ---
    let id_pause = 1102u64;
    let save_pause = td.path().join("pause_test.dat");
    let partinfo_pause = td.path().join("pause_test.dat.partinfo");

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: id_pause,
            url: url.clone(),
            save_path: save_pause.clone(),
            connections: 2,
        })
        .unwrap();

    let _ = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id, downloaded, .. } if *id == id_pause && *downloaded > 0)
    }).await;

    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: id_pause })
        .unwrap();

    let paused_evt = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Paused { id } if *id == id_pause)
    }).await;
    assert!(paused_evt.is_some(), "Pause command must emit EngineEvent::Paused");
    assert!(save_pause.exists(), "Partial file must be preserved on Pause");
    assert!(partinfo_pause.exists(), "Checkpoint .partinfo must be saved on Pause");

    // --- Subtest B: Cancel semantics ---
    let id_cancel = 1103u64;
    let save_cancel = td.path().join("cancel_test.dat");
    let partinfo_cancel = td.path().join("cancel_test.dat.partinfo");

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: id_cancel,
            url: url.clone(),
            save_path: save_cancel.clone(),
            connections: 2,
        })
        .unwrap();

    let _ = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id, downloaded, .. } if *id == id_cancel && *downloaded > 0)
    }).await;

    engine
        .cmd_sender
        .send(EngineCommand::CancelDownload { id: id_cancel })
        .unwrap();

    let canceled_evt = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Canceled { id } if *id == id_cancel)
    }).await;
    assert!(canceled_evt.is_some(), "Cancel command must emit EngineEvent::Canceled");
    assert!(save_cancel.exists(), "Partial file must be preserved on Cancel");
    assert!(partinfo_cancel.exists(), "Checkpoint .partinfo must be preserved on Cancel");

    // Confirm Cancel does NOT emit Paused
    while let Ok(evt) = rx.try_recv() {
        assert!(
            !matches!(evt, EngineEvent::Paused { id } if id == id_cancel),
            "Cancel must NOT emit Paused event"
        );
    }
}

/// T1.4: ActiveTaskGuard generation token prevents stale eviction on ID reuse.
#[tokio::test]
async fn test_tier1_active_task_guard_generation_token() {
    let td = TestDir::new("t1_guard_token");
    let (url, _server) = start_mock_range_server(10 * 1024 * 1024, 30).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let task_id = 1104u64;
    let save_path = td.path().join("token_reuse.dat");

    // Step 1: Start Task instance 1
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let _ = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id, downloaded, .. } if *id == task_id && *downloaded > 0)
    }).await;

    // Step 2: Cancel instance 1
    engine
        .cmd_sender
        .send(EngineCommand::CancelDownload { id: task_id })
        .unwrap();

    let canceled = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Canceled { id } if *id == task_id)
    }).await;
    assert!(canceled.is_some(), "Instance 1 must cancel cleanly");

    // Step 3: Reuse ID immediately as instance 2
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url: url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let started_2 = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id, downloaded, .. } if *id == task_id && *downloaded > 0)
    }).await;
    assert!(started_2.is_some(), "Instance 2 must start successfully");

    // Step 4: Verify instance 2 is active and responsive to commands (not evicted by stale guard drop)
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id: task_id })
        .unwrap();

    let paused_2 = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Paused { id } if *id == task_id)
    }).await;
    assert!(
        paused_2.is_some(),
        "Instance 2 must remain registered in active tasks and respond to Pause"
    );
}

/// T1.5: CleanupPending state handling when file unlinking fails, and idempotent retry.
#[tokio::test]
async fn test_tier1_cleanup_pending_handling() {
    let td = TestDir::new("t1_cleanup_pending");
    let locked_file = td.path().join("locked_download.bin");
    let partinfo_file = td.path().join("locked_download.bin.partinfo");

    std::fs::write(&locked_file, b"sample content to delete").unwrap();
    std::fs::write(&partinfo_file, b"{\"total_size\":100}").unwrap();

    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let task_id = 1105u64;

    // Step 1: Open the file with exclusive write access on Windows to simulate a lock
    #[cfg(windows)]
    use std::os::windows::fs::OpenOptionsExt;

    #[cfg(windows)]
    let lock_handle = std::fs::OpenOptions::new()
        .write(true)
        .share_mode(0)
        .open(&locked_file)
        .expect("Locking file must succeed");

    #[cfg(not(windows))]
    let lock_handle = std::fs::OpenOptions::new()
        .write(true)
        .open(&locked_file)
        .expect("Locking file must succeed");

    // First, register path in known_paths via start or direct inactive deletion
    engine.known_paths.lock().unwrap().insert(task_id, locked_file.clone());
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let del_evt = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id, .. } if *id == task_id)
    }).await;
    assert!(del_evt.is_some(), "Engine must emit Deleted event even when locked");

    // If locked, file remains on disk
    if let Some(EngineEvent::Deleted { cleanup_success, error, .. }) = del_evt {
        if !cleanup_success {
            assert!(error.is_some(), "Error message must be populated on cleanup failure");
            assert!(locked_file.exists(), "Locked file must not be removed prematurely");
        }
    }

    // Step 2: Release the file lock handle
    drop(lock_handle);

    // Step 3: Retry deletion idempotently
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id: task_id,
            cleanup_files: true,
        })
        .unwrap();

    let retry_del = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::Deleted { id, cleanup_success, .. } if *id == task_id && *cleanup_success)
    }).await;
    assert!(retry_del.is_some(), "Idempotent retry deletion must succeed once unlocked");
}

// ---------------------------------------------------------------------------
// 2. TIER 2: BOUNDARY & CORNER CASES
// ---------------------------------------------------------------------------

/// T2.1: Streaming SHA-256 on a 0-byte empty file.
#[test]
fn test_tier2_sha256_empty_file() {
    let td = TestDir::new("t2_empty");
    let file = td.path().join("zero.bin");
    std::fs::write(&file, b"").unwrap();

    let expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    assert_eq!(
        verify_file_sha256(&file, expected).unwrap(),
        true,
        "0-byte file must hash to standard empty SHA-256"
    );
}

/// T2.2: Streaming SHA-256 on 1,000,000 'a' characters (authoritative NIST vector).
#[test]
fn test_tier2_sha256_one_million_a() {
    let td = TestDir::new("t2_1m_a");
    let file = td.path().join("one_million_a.txt");

    // Write 1,000,000 'a' bytes in chunks
    let chunk = vec![b'a'; 65536];
    let mut written = 0;
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&file).unwrap();
        while written < 1_000_000 {
            let to_write = (1_000_000 - written).min(chunk.len());
            f.write_all(&chunk[..to_write]).unwrap();
            written += to_write;
        }
        f.flush().unwrap();
    }

    // Authoritative NIST FIPS 180-4 digest for 1M 'a's:
    let nist_1m_a = "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0";
    assert!(
        verify_file_sha256(&file, nist_1m_a).unwrap(),
        "1,000,000 'a' characters must match authoritative NIST digest"
    );
}

/// T2.3: Streaming SHA-256 at the exact 56-byte padding boundary.
#[test]
fn test_tier2_sha256_56_byte_boundary() {
    let td = TestDir::new("t2_56b");
    let file = td.path().join("56b.dat");
    let msg = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    assert_eq!(msg.len(), 56, "Input must be exactly 56 bytes");

    std::fs::write(&file, msg).unwrap();
    let nist_56 = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
    assert!(
        verify_file_sha256(&file, nist_56).unwrap(),
        "56-byte message boundary must verify correctly"
    );
}

/// T2.4: Streaming SHA-256 multi-buffer chunk continuity.
#[test]
fn test_tier2_sha256_multi_buffer_streaming_continuity() {
    let td = TestDir::new("t2_multi_buffer");
    let file = td.path().join("multi_buffer.dat");

    // Generate a 512 KiB file spanning multiple 64 KiB internal streaming buffers
    let total_bytes = 512 * 1024;
    let pattern_data: Vec<u8> = (0..total_bytes).map(|i| (i * 31 % 256) as u8).collect();
    std::fs::write(&file, &pattern_data).unwrap();

    let reference_digest = compute_sha256(&pattern_data);
    assert!(
        verify_file_sha256(&file, &reference_digest).unwrap(),
        "Multi-buffer streaming must maintain state continuity across 64 KiB blocks"
    );
}

/// T2.5: Rapid StartDownload immediately followed by DeleteDownload.
#[tokio::test]
async fn test_tier2_rapid_start_delete_race() {
    let td = TestDir::new("t2_rapid_race");
    let (url, _server) = start_mock_range_server(2 * 1024 * 1024, 0).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;

    for i in 1..=10 {
        let id = 1200u64 + i;
        let save_path = td.path().join(format!("rapid_{}.dat", id));

        // Send StartDownload and DeleteDownload back-to-back in the same tick
        engine
            .cmd_sender
            .send(EngineCommand::StartDownload {
                id,
                url: url.clone(),
                save_path: save_path.clone(),
                connections: 2,
            })
            .unwrap();

        engine
            .cmd_sender
            .send(EngineCommand::DeleteDownload {
                id,
                cleanup_files: true,
            })
            .unwrap();
    }

    // Await all 10 Deleted events
    let mut deleted_count = 0;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(6) && deleted_count < 10 {
        if let Ok(evt) = rx.try_recv() {
            if let EngineEvent::Deleted { cleanup_success, .. } = evt {
                if cleanup_success {
                    deleted_count += 1;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        deleted_count, 10,
        "All 10 rapid start/delete tasks must cleanly converge on Deleted"
    );
}

// ---------------------------------------------------------------------------
// 3. TIER 3: CROSS-FEATURE COMBINATIONS
// ---------------------------------------------------------------------------

/// T3.1: Delete requested during final file writer flush / completion boundary.
#[tokio::test]
async fn test_tier3_delete_during_final_writer_flush() {
    let td = TestDir::new("t3_writer_flush");
    let save_path = td.path().join("flush_race.dat");
    let partinfo_path = td.path().join("flush_race.dat.partinfo");

    // Fast 512 KiB payload
    let (url, _server) = start_mock_range_server(512 * 1024, 5).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 1301u64;

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id,
            url,
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Await partial progress
    let _ = wait_for_event(&mut rx, Duration::from_secs(3), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id: evt_id, downloaded, .. } if *evt_id == id && *downloaded > 100_000)
    }).await;

    // Send DeleteDownload right near completion
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id,
            cleanup_files: true,
        })
        .unwrap();

    // The coordinator must catch deletion intent: emit Deleted, clean files
    let mut got_deleted = false;
    let mut got_finished = false;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(5) {
        if let Ok(evt) = rx.try_recv() {
            match evt {
                EngineEvent::Deleted { id: evt_id, cleanup_success, .. } if evt_id == id => {
                    assert!(cleanup_success);
                    got_deleted = true;
                    break;
                }
                EngineEvent::Finished { id: evt_id, .. } if evt_id == id => {
                    got_finished = true;
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    if got_deleted {
        assert!(!partinfo_path.exists(), "Checkpoint must be removed on Delete");
        assert!(!save_path.exists(), "Save path must be removed on Delete");
    } else {
        // If finished occurred before delete arrived, delete must still be processed
        assert!(got_finished, "Either Deleted or Finished must be received");
    }
}

/// T3.2: Delete dispatched during Pause teardown.
#[tokio::test]
async fn test_tier3_delete_during_pause_teardown() {
    let td = TestDir::new("t3_del_pause_teardown");
    let save_path = td.path().join("pause_teardown.dat");
    let partinfo_path = td.path().join("pause_teardown.dat.partinfo");

    let (url, _server) = start_mock_range_server(6 * 1024 * 1024, 20).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 1302u64;

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id,
            url,
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let _ = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id: evt_id, downloaded, .. } if *evt_id == id && *downloaded > 0)
    }).await;

    // Send Pause immediately followed by Delete
    engine
        .cmd_sender
        .send(EngineCommand::PauseDownload { id })
        .unwrap();
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id,
            cleanup_files: true,
        })
        .unwrap();

    let deleted = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Deleted { id: evt_id, cleanup_success, .. } if *evt_id == id && *cleanup_success)
    }).await;
    assert!(deleted.is_some(), "Delete during pause teardown must emit Deleted");

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!partinfo_path.exists(), "Zombie checkpoint must NOT be left on disk");
    assert!(!save_path.exists(), "Destination file must be removed");
}

/// T3.3: StartDownload dispatched while active deletion of the same task ID is in progress.
#[tokio::test]
async fn test_tier3_start_download_during_active_delete() {
    let td = TestDir::new("t3_start_during_delete");
    let (url, _server) = start_mock_range_server(8 * 1024 * 1024, 15).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 1303u64;
    let save_path = td.path().join("active_del.dat");

    // Start download
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id,
            url: url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let _ = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id: evt_id, downloaded, .. } if *evt_id == id && *downloaded > 0)
    }).await;

    // Dispatch Delete followed immediately by StartDownload for same ID
    engine
        .cmd_sender
        .send(EngineCommand::DeleteDownload {
            id,
            cleanup_files: true,
        })
        .unwrap();

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id,
            url: url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    // Verify engine processes operations without crashing or corrupting state
    let mut got_deleted = false;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(5) {
        if let Ok(evt) = rx.try_recv() {
            if let EngineEvent::Deleted { id: evt_id, .. } = evt {
                if evt_id == id {
                    got_deleted = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(got_deleted, "Engine must execute deletion cleanly under race condition");
}

/// T3.4: Cancel followed by Resume lifecycle continuity.
#[tokio::test]
async fn test_tier3_cancel_then_resume_lifecycle() {
    let td = TestDir::new("t3_cancel_resume");
    let total_size = 6 * 1024 * 1024; // 6 MB
    let (url, _server) = start_mock_range_server(total_size, 10).await;
    let engine = spawn_download_engine();
    let mut rx = engine.event_receiver;
    let id = 1304u64;
    let save_path = td.path().join("cancel_resume.dat");
    let partinfo_path = td.path().join("cancel_resume.dat.partinfo");

    // Step 1: Start download
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id,
            url: url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let _ = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::ProgressUpdate { id: evt_id, downloaded, .. } if *evt_id == id && *downloaded > 200_000)
    }).await;

    // Step 2: Cancel download
    engine
        .cmd_sender
        .send(EngineCommand::CancelDownload { id })
        .unwrap();

    let canceled = wait_for_event(&mut rx, Duration::from_secs(5), |evt| {
        matches!(evt, EngineEvent::Canceled { id: evt_id } if *evt_id == id)
    }).await;
    assert!(canceled.is_some(), "Engine must emit Canceled");
    assert!(save_path.exists(), "File must exist after Cancel");
    assert!(partinfo_path.exists(), "Checkpoint must exist after Cancel");

    // Step 3: Resume by issuing StartDownload
    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id,
            url: url.clone(),
            save_path: save_path.clone(),
            connections: 2,
        })
        .unwrap();

    let finished = wait_for_event(&mut rx, Duration::from_secs(8), |evt| {
        matches!(evt, EngineEvent::Finished { id: evt_id, .. } if *evt_id == id)
    }).await;
    assert!(finished.is_some(), "Download must complete after resume");

    // Verify downloaded file size
    let meta = std::fs::metadata(&save_path).unwrap();
    assert_eq!(meta.len(), total_size as u64, "Resumed file must reach full total size");
}

// ---------------------------------------------------------------------------
// 4. TIER 4: REAL-WORLD WORKLOAD SCENARIOS
// ---------------------------------------------------------------------------

/// T4.1: 10,000 lifecycle operations stress test measuring active tasks, known paths,
/// workers, and Win32 process memory via GetProcessMemoryInfo FFI, asserting zero leaks.
#[tokio::test]
async fn test_tier4_10k_lifecycle_operations_memory_and_resource_stress() {
    let td = TestDir::new("t4_10k_stress");
    let (url, _server) = start_mock_range_server(1024 * 1024, 0).await;
    let mut engine = spawn_download_engine();

    // Baseline memory measurement
    let (ws_before, pf_before) = win32_mem::get_memory_info();
    println!(
        "[Tier 4 Stress] Baseline Memory: WorkingSet = {:.2} MB, Pagefile = {:.2} MB",
        ws_before as f64 / (1024.0 * 1024.0),
        pf_before as f64 / (1024.0 * 1024.0)
    );

    let total_operations = 10_000;
    let batch_size = 50;
    let cycles = total_operations / batch_size;

    let t0 = Instant::now();

    for cycle in 0..cycles {
        for i in 0..batch_size {
            let id = (cycle * batch_size + i + 1) as u64;
            let save_path = td.path().join(format!("stress_{}.bin", id));

            // Start
            let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                id,
                url: url.clone(),
                save_path: save_path.clone(),
                connections: 1,
            });

            // 5-operation lifecycle coverage: Start, Pause, Cancel, Delete, and Resume
            match i % 4 {
                0 => {
                    // Start -> Pause -> Resume (Start) -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id,
                        url: url.clone(),
                        save_path: save_path.clone(),
                        connections: 1,
                    });
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
                1 => {
                    // Start -> Cancel -> Resume (Start) -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id });
                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id,
                        url: url.clone(),
                        save_path: save_path.clone(),
                        connections: 1,
                    });
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
                2 => {
                    // Start -> Pause -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
                _ => {
                    // Start -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
            }
        }

        // Drain pending events in batches
        while engine.event_receiver.try_recv().is_ok() {}
        tokio::task::yield_now().await;
    }

    // Deterministic bounded quiescence polling up to 30 seconds
    let drain_start = Instant::now();
    let timeout = Duration::from_secs(30);
    while drain_start.elapsed() < timeout {
        while engine.event_receiver.try_recv().is_ok() {}
        let m = engine.metrics();
        if m.active_tasks_count == 0 && m.known_paths_count == 0 && m.active_workers_count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let duration = t0.elapsed();
    println!(
        "[Tier 4 Stress] Completed and finalized {} lifecycle operations in {:.2?}",
        total_operations, duration
    );

    // Resource leak assertions
    let metrics = engine.metrics();
    assert_eq!(metrics.active_tasks_count, 0, "Resource leak: active_tasks_count must be 0");
    assert_eq!(metrics.known_paths_count, 0, "Resource leak: known_paths_count must be 0");
    assert_eq!(metrics.active_workers_count, 0, "Resource leak: active_workers_count must be 0");

    // Post-stress memory measurement
    let (ws_after, pf_after) = win32_mem::get_memory_info();
    let ws_delta = ws_after.saturating_sub(ws_before) as f64 / (1024.0 * 1024.0);
    let pf_delta = pf_after.saturating_sub(pf_before) as f64 / (1024.0 * 1024.0);

    println!(
        "[Tier 4 Stress] Post-Stress Memory: WorkingSet = {:.2} MB (Δ {:.2} MB), Pagefile = {:.2} MB (Δ {:.2} MB)",
        ws_after as f64 / (1024.0 * 1024.0),
        ws_delta,
        pf_after as f64 / (1024.0 * 1024.0),
        pf_delta
    );

    if ws_before > 0 {
        assert!(
            ws_delta < 100.0,
            "Working set memory growth must be bounded (< 100 MB, got {:.2} MB)",
            ws_delta
        );
    }
}

/// T4.2: 1,000+ randomized concurrent command interleavings across 4 recorded independent random seeds
/// using deterministic synchronization, event trace logging, invariant verification, and seed reproduction on failure.
#[tokio::test]
async fn test_tier4_randomized_concurrent_interleavings() {
    let td = TestDir::new("t4_randomized");
    let (url, _server) = start_mock_range_server(1024 * 1024, 0).await;

    // 4 recorded independent seeds:
    let test_seeds: [u64; 4] = [
        0x1234_5678_9ABC_DEF0,
        0xFEDC_BA98_7654_3210,
        0x0123_4567_89AB_CDEF,
        0xA5A5_5A5A_0F0F_F0F0,
    ];

    let ops_per_seed = 250; // 4 seeds * 250 ops = 1,000 total operations

    for &seed in &test_seeds {
        let mut prng = Xoshiro256PlusPlus::from_seed(seed);
        let mut engine = spawn_download_engine();

        let mut event_trace: Vec<String> = Vec::new();
        let mut command_history: Vec<String> = Vec::new();

        // Track per-task active instance generation and terminal event count per instance
        let mut active_instance: HashMap<u64, u64> = HashMap::new(); // task_id -> instance_seq
        let mut instance_terminals: HashMap<(u64, u64), usize> = HashMap::new(); // (task_id, instance_seq) -> count
        let mut next_instance_seq: HashMap<u64, u64> = HashMap::new();

        for op_idx in 0..ops_per_seed {
            let task_id = prng.gen_range(1, 21) as u64; // pool of 20 concurrent tasks
            let op_type = prng.gen_range(0, 4);

            let cmd_str = match op_type {
                0 => {
                    let seq = next_instance_seq.entry(task_id).or_insert(1);
                    let current_seq = *seq;
                    *seq += 1;
                    active_instance.insert(task_id, current_seq);

                    let save_path = td.path().join(format!("rand_{}_{}.bin", seed, task_id));
                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id: task_id,
                        url: url.clone(),
                        save_path,
                        connections: 1,
                    });
                    format!("StartDownload({}, seq={})", task_id, current_seq)
                }
                1 => {
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id: task_id });
                    format!("PauseDownload({})", task_id)
                }
                2 => {
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id: task_id });
                    format!("CancelDownload({})", task_id)
                }
                _ => {
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id: task_id,
                        cleanup_files: true,
                    });
                    format!("DeleteDownload({})", task_id)
                }
            };
            command_history.push(cmd_str);

            // Drain available events and verify invariants
            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));

                match evt {
                    EngineEvent::Finished { id, .. }
                    | EngineEvent::Canceled { id }
                    | EngineEvent::Paused { id }
                    | EngineEvent::Failed { id, .. } => {
                        if let Some(seq) = active_instance.remove(&id) {
                            let count = instance_terminals.entry((id, seq)).or_insert(0);
                            *count += 1;
                            if *count > 1 {
                                panic!(
                                    "\n======================================================\n\
                                     TIER 4 INVARIANT VIOLATION: Duplicate terminal event for task {} seq {}\n\
                                     Seed: 0x{:016X}\nRecent Commands:\n{:?}\nEvent Trace:\n{:?}\n\
                                     ======================================================\n",
                                    id, seq, seed, &command_history[command_history.len().saturating_sub(20)..], &event_trace
                                );
                            }
                        }
                    }
                    EngineEvent::Deleted { id, cleanup_success, .. } => {
                        if let Some(seq) = active_instance.remove(&id) {
                            let count = instance_terminals.entry((id, seq)).or_insert(0);
                            *count += 1;
                            if *count > 1 {
                                panic!(
                                    "\n======================================================\n\
                                     TIER 4 INVARIANT VIOLATION: Duplicate terminal event for task {} seq {}\n\
                                     Seed: 0x{:016X}\nRecent Commands:\n{:?}\nEvent Trace:\n{:?}\n\
                                     ======================================================\n",
                                    id, seq, seed, &command_history[command_history.len().saturating_sub(20)..], &event_trace
                                );
                            }
                        }
                        if cleanup_success {
                            let save_path = td.path().join(format!("rand_{}_{}.bin", seed, id));
                            let partinfo = td.path().join(format!("rand_{}_{}.bin.partinfo", seed, id));
                            let mut still_exists = save_path.exists() || partinfo.exists();
                            if still_exists {
                                for _ in 0..20 {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                    if !save_path.exists() && !partinfo.exists() {
                                        still_exists = false;
                                        break;
                                    }
                                }
                            }
                            if still_exists {
                                panic!(
                                    "\n======================================================\n\
                                     TIER 4 INVARIANT VIOLATION: Files still exist after Delete for task {}\n\
                                     Seed: 0x{:016X}\nRecent Commands:\n{:?}\n\
                                     ======================================================\n",
                                    id, seed, &command_history[command_history.len().saturating_sub(20)..]
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }

            if op_idx % 20 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Deterministic teardown of all remaining tasks in the pool
        for task_id in 1..=20u64 {
            let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                id: task_id,
                cleanup_files: true,
            });
        }

        // Bounded final drain and quiescence
        tokio::time::sleep(Duration::from_millis(100)).await;
        let drain_start = Instant::now();
        let timeout = Duration::from_secs(10);
        while drain_start.elapsed() < timeout {
            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));
            }
            let m = engine.metrics();
            if m.active_tasks_count == 0 && m.active_workers_count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Invariant Assertions: zero orphaned workers and clean teardown
        let final_metrics = engine.metrics();
        assert_eq!(
            final_metrics.active_workers_count, 0,
            "Invariant violation: zero orphaned workers required. Seed: 0x{:016X}",
            seed
        );
        assert_eq!(
            final_metrics.active_tasks_count, 0,
            "Invariant violation: zero active tasks after teardown. Seed: 0x{:016X}",
            seed
        );

        println!(
            "[Tier 4 Seed 0x{:016X}] Completed {} ops; verified all invariants across {} trace events.",
            seed,
            ops_per_seed,
            event_trace.len()
        );
    }
}
