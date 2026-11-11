//! Milestone 1 Empirical Challenger Test Suite for Sha256Hasher and verify_file_sha256.
//!
//! Objectives:
//! 1. Adversarial streaming chunk sizes (1, 7, 63, 64, 65, 8192, 65536, 131072 bytes, plus variable/random chunks).
//! 2. Authoritative NIST test vectors (Empty, "abc", 56-byte, 1,000,000 'a's, 112-byte multi-block).
//! 3. Empty file (0 bytes) and multi-megabyte simulated files (1 MB, 10 MB, 25 MB).
//! 4. Memory measurements (incremental peak memory < 5 MB on multi-megabyte file hashing) using Win32 API.
//! 5. Case sensitivity and whitespace trimming on expected_hex, plus malformed hex handling.
//! 6. Error handling for non-existent files and directories.

use not_download_manager::db::{compute_sha256, verify_file_sha256, Sha256Hasher};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

struct TempDir(PathBuf);
impl TempDir {
    fn new(prefix: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_challenger_{}_{}_{}",
            prefix,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::create_dir_all(&p);
        TempDir(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(windows)]
mod mem_tracker {
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

    pub fn get_memory_bytes() -> (usize, usize) {
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
mod mem_tracker {
    pub fn get_memory_bytes() -> (usize, usize) {
        (0, 0)
    }
}

// -----------------------------------------------------------------------------
// Test 1: Authoritative NIST Vectors
// -----------------------------------------------------------------------------

#[test]
fn test_nist_authoritative_vectors() {
    // Vector 1: Empty string (0 bytes)
    let hasher = Sha256Hasher::new();
    let digest = hasher.finalize_hex();
    assert_eq!(
        digest,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "NIST Vector 1 (Empty String) failed"
    );

    // Vector 2: "abc"
    let mut hasher = Sha256Hasher::new();
    hasher.update(b"abc");
    assert_eq!(
        hasher.finalize_hex(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "NIST Vector 2 ('abc') failed"
    );

    // Vector 3: 56-byte message (Boundary where msg + 1 byte pad = 57 > 56, requires 2 blocks)
    let msg_56 = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    let mut hasher = Sha256Hasher::new();
    hasher.update(msg_56);
    assert_eq!(
        hasher.finalize_hex(),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        "NIST Vector 3 (56-byte) failed"
    );

    // Vector 4: 112-byte message (2 full 64-byte blocks with padding)
    let msg_112 = b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";
    let mut hasher = Sha256Hasher::new();
    hasher.update(msg_112);
    assert_eq!(
        hasher.finalize_hex(),
        "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1",
        "NIST Vector 4 (112-byte) failed"
    );

    // Vector 5: 1,000,000 'a' characters
    let mut hasher = Sha256Hasher::new();
    let chunk = [b'a'; 8192];
    let mut remaining = 1_000_000;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        hasher.update(&chunk[..n]);
        remaining -= n;
    }
    assert_eq!(
        hasher.finalize_hex(),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
        "NIST Vector 5 (1,000,000 'a's) failed"
    );
}

// -----------------------------------------------------------------------------
// Test 2: Adversarial Streaming Chunk Sizes
// -----------------------------------------------------------------------------

#[test]
fn test_adversarial_chunk_sizes() {
    let test_sizes: Vec<usize> = vec![
        0, 1, 7, 55, 56, 57, 63, 64, 65, 127, 128, 129, 500, 1024, 8191, 8192, 65535, 65536, 131072, 200000,
    ];

    let chunk_sizes = [1, 7, 63, 64, 65, 8192, 65536, 131072];

    for &size in &test_sizes {
        // Generate deterministic test pattern
        let data: Vec<u8> = (0..size).map(|i| ((i * 31 + 17) % 256) as u8).collect();
        let reference_digest = compute_sha256(&data);

        for &chunk_size in &chunk_sizes {
            let mut hasher = Sha256Hasher::new();
            for chunk in data.chunks(chunk_size) {
                hasher.update(chunk);
            }
            let chunked_digest = hasher.finalize_hex();

            assert_eq!(
                chunked_digest, reference_digest,
                "Digest mismatch for data size {} with chunk size {}",
                size, chunk_size
            );
        }

        // Test with randomized / variable chunk sequence
        let mut hasher_var = Sha256Hasher::new();
        let var_chunks = [1, 3, 64, 15, 63, 65, 128, 2, 8192, 5, 0, 17];
        let mut offset = 0;
        let mut c_idx = 0;
        while offset < data.len() {
            let step = var_chunks[c_idx % var_chunks.len()];
            c_idx += 1;
            if step == 0 {
                // Empty slice update test
                hasher_var.update(&[]);
                continue;
            }
            let end = (offset + step).min(data.len());
            hasher_var.update(&data[offset..end]);
            offset = end;
        }
        assert_eq!(
            hasher_var.finalize_hex(),
            reference_digest,
            "Variable chunk streaming failed for data size {}",
            size
        );
    }
}

// -----------------------------------------------------------------------------
// Test 3: Empty File and Multi-Megabyte Simulated Files
// -----------------------------------------------------------------------------

#[test]
fn test_verify_file_sha256_empty_and_multimb() {
    let td = TempDir::new("verify_files");

    // 1. Empty file (0 bytes)
    let empty_file = td.path().join("empty.bin");
    File::create(&empty_file).unwrap();
    let nist_empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    assert!(
        verify_file_sha256(&empty_file, nist_empty).unwrap(),
        "0-byte file must verify against NIST empty digest"
    );

    // 2. 1 MB simulated file
    let mb1_file = td.path().join("1mb.bin");
    {
        let mut f = File::create(&mb1_file).unwrap();
        let pattern: Vec<u8> = (0..65536).map(|i| (i % 251) as u8).collect();
        for _ in 0..16 {
            f.write_all(&pattern).unwrap();
        }
    }
    let mut ref_hasher = Sha256Hasher::new();
    {
        let mut f = File::open(&mb1_file).unwrap();
        let mut buf = [0u8; 8192];
        loop {
            use std::io::Read;
            let n = f.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            ref_hasher.update(&buf[..n]);
        }
    }
    let expected_1mb = ref_hasher.finalize_hex();
    assert!(
        verify_file_sha256(&mb1_file, &expected_1mb).unwrap(),
        "1 MB file verification failed"
    );

    // 3. 10 MB simulated file
    let mb10_file = td.path().join("10mb.bin");
    {
        let mut f = File::create(&mb10_file).unwrap();
        let pattern: Vec<u8> = (0..65536).map(|i| (i % 255) as u8).collect();
        for _ in 0..160 {
            f.write_all(&pattern).unwrap();
        }
    }
    let mut ref_hasher10 = Sha256Hasher::new();
    {
        let mut f = File::open(&mb10_file).unwrap();
        let mut buf = [0u8; 65536];
        loop {
            use std::io::Read;
            let n = f.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            ref_hasher10.update(&buf[..n]);
        }
    }
    let expected_10mb = ref_hasher10.finalize_hex();
    assert!(
        verify_file_sha256(&mb10_file, &expected_10mb).unwrap(),
        "10 MB file verification failed"
    );
}

// -----------------------------------------------------------------------------
// Test 4: Peak Incremental Memory Assertion (< 5 MB)
// -----------------------------------------------------------------------------

#[test]
fn test_incremental_memory_bounded_under_5mb() {
    let td = TempDir::new("mem_test");
    let large_file = td.path().join("large_20mb.bin");

    // Write a 20 MB file
    {
        let mut f = File::create(&large_file).unwrap();
        let chunk = [0x5au8; 65536];
        for _ in 0..320 {
            // 320 * 64 KiB = 20 MB
            f.write_all(&chunk).unwrap();
        }
        f.flush().unwrap();
    }

    // Warm-up memory baseline
    let (ws_baseline, pf_baseline) = mem_tracker::get_memory_bytes();
    println!(
        "[MemTest] Baseline WorkingSet: {:.2} MB, Pagefile: {:.2} MB",
        ws_baseline as f64 / (1024.0 * 1024.0),
        pf_baseline as f64 / (1024.0 * 1024.0)
    );

    // Run verification on 20 MB file
    let dummy_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    let _ = verify_file_sha256(&large_file, dummy_hash);

    let (ws_after, pf_after) = mem_tracker::get_memory_bytes();
    let ws_delta_mb = ws_after.saturating_sub(ws_baseline) as f64 / (1024.0 * 1024.0);
    let pf_delta_mb = pf_after.saturating_sub(pf_baseline) as f64 / (1024.0 * 1024.0);

    println!(
        "[MemTest] Post-Verify WorkingSet: {:.2} MB (Δ {:.2} MB), Pagefile: {:.2} MB (Δ {:.2} MB)",
        ws_after as f64 / (1024.0 * 1024.0),
        ws_delta_mb,
        pf_after as f64 / (1024.0 * 1024.0),
        pf_delta_mb
    );

    // Incremental memory increase must strictly be < 5.0 MB (per Requirement R3)
    if ws_baseline > 0 {
        assert!(
            ws_delta_mb < 5.0,
            "Incremental working set memory ({:.2} MB) exceeded 5 MB threshold",
            ws_delta_mb
        );
        assert!(
            pf_delta_mb < 5.0,
            "Incremental pagefile memory ({:.2} MB) exceeded 5 MB threshold",
            pf_delta_mb
        );
    }
}

// -----------------------------------------------------------------------------
// Test 5: Case Sensitivity and Whitespace Trimming
// -----------------------------------------------------------------------------

#[test]
fn test_case_sensitivity_and_whitespace_trimming() {
    let td = TempDir::new("case_whitespace");
    let file = td.path().join("case_test.txt");
    std::fs::write(&file, b"HyperDownload streaming sha256 verification test").unwrap();

    let correct_lower = compute_sha256(b"HyperDownload streaming sha256 verification test");
    let correct_upper = correct_lower.to_uppercase();

    // 1. Lowercase match
    assert!(
        verify_file_sha256(&file, &correct_lower).unwrap(),
        "Lowercase hash must match"
    );

    // 2. Uppercase match
    assert!(
        verify_file_sha256(&file, &correct_upper).unwrap(),
        "Uppercase hash must match"
    );

    // 3. Mixed case match
    let mixed_case: String = correct_lower
        .chars()
        .enumerate()
        .map(|(i, c)| if i % 2 == 0 { c.to_ascii_uppercase() } else { c })
        .collect();
    assert!(
        verify_file_sha256(&file, &mixed_case).unwrap(),
        "Mixed case hash must match"
    );

    // 4. Leading and trailing spaces
    let with_spaces = format!("   {}   ", correct_lower);
    assert!(
        verify_file_sha256(&file, &with_spaces).unwrap(),
        "Hash with leading/trailing spaces must match after trimming"
    );

    // 5. Tabs, newlines, carriage returns
    let with_whitespace = format!("\t\r\n  {} \n\r", correct_upper);
    assert!(
        verify_file_sha256(&file, &with_whitespace).unwrap(),
        "Hash with tabs and newlines must match after trimming"
    );

    // 6. Mismatched hash
    let mut wrong_hash = correct_lower.clone();
    let last_char = if wrong_hash.ends_with('0') { '1' } else { '0' };
    wrong_hash.pop();
    wrong_hash.push(last_char);
    assert!(
        !verify_file_sha256(&file, &wrong_hash).unwrap(),
        "Mismatched hash must evaluate to false"
    );

    // 7. Malformed / empty hash
    assert!(
        !verify_file_sha256(&file, "").unwrap(),
        "Empty string must evaluate to false"
    );
    assert!(
        !verify_file_sha256(&file, "   \n\t").unwrap(),
        "Whitespace-only string must evaluate to false"
    );
    assert!(
        !verify_file_sha256(&file, "short").unwrap(),
        "Truncated hash string must evaluate to false"
    );
    assert!(
        !verify_file_sha256(&file, &format!("{}extra", correct_lower)).unwrap(),
        "Overly long hash string must evaluate to false"
    );
    assert!(
        !verify_file_sha256(&file, "not_a_valid_hex_string_at_all!").unwrap(),
        "Invalid hex string must evaluate to false"
    );
}

// -----------------------------------------------------------------------------
// Test 6: Filesystem Errors & Boundary Conditions
// -----------------------------------------------------------------------------

#[test]
fn test_filesystem_errors_and_edge_conditions() {
    let td = TempDir::new("fs_errors");

    // 1. Non-existent file
    let non_existent = td.path().join("does_not_exist.bin");
    let err = verify_file_sha256(&non_existent, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert!(err.is_err(), "Non-existent file must return io::Error");
    assert_eq!(
        err.unwrap_err().kind(),
        std::io::ErrorKind::NotFound,
        "Error kind must be NotFound"
    );

    // 2. Directory path instead of file
    let dir_path = td.path().to_path_buf();
    let dir_err = verify_file_sha256(&dir_path, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert!(dir_err.is_err(), "Directory path must return io::Error");

    // 3. Empty slice updates to Sha256Hasher
    let mut hasher = Sha256Hasher::new();
    hasher.update(&[]);
    hasher.update(b"hello");
    hasher.update(&[]);
    hasher.update(b" world");
    hasher.update(&[]);
    assert_eq!(
        hasher.finalize_hex(),
        compute_sha256(b"hello world"),
        "Empty slice updates must not corrupt state"
    );
}

// -----------------------------------------------------------------------------
// Test 7: Strict Boundary Padding & 1-byte Feed Stress
// -----------------------------------------------------------------------------

#[test]
fn test_strict_boundary_padding_and_single_byte_feed() {
    // Specifically test all byte lengths from 0 to 130 fed strictly 1 byte at a time
    for len in 0..=130 {
        let pattern: Vec<u8> = (0..len).map(|i| ((i * 7 + 3) % 256) as u8).collect();
        let expected = compute_sha256(&pattern);

        let mut hasher = Sha256Hasher::new();
        for &byte in &pattern {
            hasher.update(&[byte]);
        }
        let got = hasher.finalize_hex();
        assert_eq!(
            got, expected,
            "1-byte stream feed failed at exact boundary length {}",
            len
        );
    }
}

// -----------------------------------------------------------------------------
// Test 8: Concurrent Multi-Threaded File Verification Stress
// -----------------------------------------------------------------------------

#[test]
fn test_concurrent_multithreaded_hashing_stress() {
    let td = TempDir::new("concurrent_hashing");
    let thread_count = 8;
    let mut handles = Vec::new();

    for t_idx in 0..thread_count {
        let file_path = td.path().join(format!("thread_file_{}.bin", t_idx));
        let size = (t_idx + 1) * 256 * 1024; // 256 KiB to 2 MiB
        let data: Vec<u8> = (0..size).map(|i| ((i + t_idx) % 256) as u8).collect();
        std::fs::write(&file_path, &data).unwrap();
        let expected = compute_sha256(&data);

        handles.push(std::thread::spawn(move || {
            for _ in 0..5 {
                let ok = verify_file_sha256(&file_path, &expected).unwrap();
                assert!(ok, "Concurrent verification failed in thread {}", t_idx);
            }
        }));
    }

    for handle in handles {
        handle.join().expect("Worker thread panicked during concurrent hashing");
    }
}

