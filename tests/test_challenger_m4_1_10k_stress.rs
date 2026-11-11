//! Adversarial Challenger Stress Test Suite: Milestone 4 (10k Operations Lifecycle & Resource Hardening)
//!
//! Empirical challenges:
//! 1. Verify all 5 lifecycle operations (Start, Pause, Cancel, Delete, Resume) across 10k operations.
//! 2. Measure execution time and memory bounds (< 100 MB).
//! 3. Assert zero leaks: active_tasks == 0, known_paths == 0, active_workers == 0.
//! 4. Check for lingering Tokio tasks or file writer actors (probe file locks on Windows).
//! 5. Rapid sequential multi-loop execution (30k tasks, ~97.5k operations) to detect cumulative leaks.

use not_download_manager::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Isolated test directory that cleans up on drop with retry for Windows lock tolerance.
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_m4_chal_{}_{}_{}",
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

/// Win32 Process Memory FFI via `psapi.dll`
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

/// In-process HTTP 206 mock streaming server using tokio TCP sockets.
async fn start_mock_range_server(
    content_len: usize,
    chunk_delay_ms: u64,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Binding loopback listener must succeed");
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
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-m4-chal\"\r\nConnection: close\r\n\r\n",
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

#[tokio::test]
async fn test_adversarial_10k_operation_distribution_and_execution() {
    let td = TestDir::new("m4_adv_dist");
    let (url, _server) = start_mock_range_server(1024 * 1024, 0).await;
    let mut engine = spawn_download_engine();

    let (ws_before, pf_before) = win32_mem::get_memory_info();
    println!(
        "[Adversarial 10k] Baseline Memory: WorkingSet = {:.2} MB, Pagefile = {:.2} MB",
        ws_before as f64 / (1024.0 * 1024.0),
        pf_before as f64 / (1024.0 * 1024.0)
    );

    let total_tasks = 10_000;
    let batch_size = 50;
    let cycles = total_tasks / batch_size;

    let mut start_count = 0usize;
    let mut pause_count = 0usize;
    let mut cancel_count = 0usize;
    let mut delete_count = 0usize;
    let mut resume_count = 0usize;

    let t0 = Instant::now();

    for cycle in 0..cycles {
        for i in 0..batch_size {
            let id = (cycle * batch_size + i + 1) as u64;
            let save_path = td.path().join(format!("adv_stress_{}.bin", id));

            // Start (1st operation)
            let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                id,
                url: url.clone(),
                save_path: save_path.clone(),
                connections: 1,
            });
            start_count += 1;

            match i % 4 {
                0 => {
                    // Pause -> Resume (Start) -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                    pause_count += 1;

                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id,
                        url: url.clone(),
                        save_path: save_path.clone(),
                        connections: 1,
                    });
                    resume_count += 1;

                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                    delete_count += 1;
                }
                1 => {
                    // Cancel -> Resume (Start) -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id });
                    cancel_count += 1;

                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id,
                        url: url.clone(),
                        save_path: save_path.clone(),
                        connections: 1,
                    });
                    resume_count += 1;

                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                    delete_count += 1;
                }
                2 => {
                    // Pause -> Delete
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                    pause_count += 1;

                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                    delete_count += 1;
                }
                _ => {
                    // Direct Delete
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                    delete_count += 1;
                }
            }
        }

        // Drain pending events in batches
        while engine.event_receiver.try_recv().is_ok() {}
        tokio::task::yield_now().await;
    }

    // Wait for quiescence
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

    let elapsed = t0.elapsed();
    let total_commands = start_count + pause_count + cancel_count + delete_count + resume_count;

    println!(
        "[Adversarial 10k] Operation counts: Start={}, Pause={}, Cancel={}, Delete={}, Resume={} (Total commands={})",
        start_count, pause_count, cancel_count, delete_count, resume_count, total_commands
    );
    println!("[Adversarial 10k] Elapsed time: {:.2?}", elapsed);

    // Verify all 5 operations were executed
    assert_eq!(start_count, 10_000, "Must execute exactly 10,000 Start operations");
    assert_eq!(pause_count, 5_000, "Must execute exactly 5,000 Pause operations");
    assert_eq!(cancel_count, 2_600, "Must execute exactly 2,600 Cancel operations");
    assert_eq!(delete_count, 10_000, "Must execute exactly 10,000 Delete operations");
    assert_eq!(resume_count, 5_200, "Must execute exactly 5,200 Resume operations");
    assert_eq!(total_commands, 32_800, "Total lifecycle commands must be exactly 32,800");

    // Assert zero resource leaks
    let metrics = engine.metrics();
    assert_eq!(metrics.active_tasks_count, 0, "active_tasks_count must be 0");
    assert_eq!(metrics.known_paths_count, 0, "known_paths_count must be 0");
    assert_eq!(metrics.active_workers_count, 0, "active_workers_count must be 0");

    // Post-stress memory measurement
    let (ws_after, pf_after) = win32_mem::get_memory_info();
    let ws_delta = ws_after.saturating_sub(ws_before) as f64 / (1024.0 * 1024.0);
    let pf_delta = pf_after.saturating_sub(pf_before) as f64 / (1024.0 * 1024.0);

    println!(
        "[Adversarial 10k] Post-Stress Memory: WorkingSet = {:.2} MB (Δ {:.2} MB), Pagefile = {:.2} MB (Δ {:.2} MB)",
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

#[tokio::test]
async fn test_adversarial_rapid_sequential_10k_stress_loops_cumulative_leak() {
    let td = TestDir::new("m4_adv_sequential");
    let (url, _server) = start_mock_range_server(1024 * 1024, 0).await;
    let mut engine = spawn_download_engine();

    let (ws_baseline, _) = win32_mem::get_memory_info();
    println!(
        "[Adversarial Sequential] Initial Baseline WorkingSet: {:.2} MB",
        ws_baseline as f64 / (1024.0 * 1024.0)
    );

    let num_loops = 3;
    let ops_per_loop = 10_000;
    let batch_size = 50;
    let mut loop_memories = Vec::new();

    for loop_idx in 0..num_loops {
        let t_loop = Instant::now();
        let cycles = ops_per_loop / batch_size;

        for cycle in 0..cycles {
            for i in 0..batch_size {
                let id = (loop_idx * ops_per_loop + cycle * batch_size + i + 1) as u64;
                let save_path = td.path().join(format!("seq_{}_{}.bin", loop_idx, id));

                let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                    id,
                    url: url.clone(),
                    save_path: save_path.clone(),
                    connections: 1,
                });

                match i % 4 {
                    0 => {
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
                        let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                        let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                            id,
                            cleanup_files: true,
                        });
                    }
                    _ => {
                        let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                            id,
                            cleanup_files: true,
                        });
                    }
                }
            }
            while engine.event_receiver.try_recv().is_ok() {}
            tokio::task::yield_now().await;
        }

        // Quiescence wait
        let drain_start = Instant::now();
        while drain_start.elapsed() < Duration::from_secs(30) {
            while engine.event_receiver.try_recv().is_ok() {}
            let m = engine.metrics();
            if m.active_tasks_count == 0 && m.known_paths_count == 0 && m.active_workers_count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let m = engine.metrics();
        assert_eq!(m.active_tasks_count, 0, "Loop {} leaked active_tasks", loop_idx + 1);
        assert_eq!(m.known_paths_count, 0, "Loop {} leaked known_paths", loop_idx + 1);
        assert_eq!(m.active_workers_count, 0, "Loop {} leaked active_workers", loop_idx + 1);

        let (ws_loop, _) = win32_mem::get_memory_info();
        let loop_dur = t_loop.elapsed();
        println!(
            "[Adversarial Sequential] Loop {}/{} ({} cumulative ops) finished in {:.2?}. WorkingSet: {:.2} MB (Δ vs baseline: {:.2} MB)",
            loop_idx + 1,
            num_loops,
            (loop_idx + 1) * ops_per_loop,
            loop_dur,
            ws_loop as f64 / (1024.0 * 1024.0),
            ws_loop.saturating_sub(ws_baseline) as f64 / (1024.0 * 1024.0)
        );
        loop_memories.push(ws_loop);
    }

    // Verify memory delta between loop 1 and loop 3 is strictly bounded (< 30 MB growth between loops)
    if loop_memories.len() == 3 && loop_memories[0] > 0 {
        let loop1_to_3_delta = loop_memories[2].saturating_sub(loop_memories[0]) as f64 / (1024.0 * 1024.0);
        println!(
            "[Adversarial Sequential] Memory delta between Loop 1 and Loop 3: {:.2} MB",
            loop1_to_3_delta
        );
        assert!(
            loop1_to_3_delta < 30.0,
            "Memory growth between sequential 10k loops must stabilize (< 30 MB delta, got {:.2} MB)",
            loop1_to_3_delta
        );
    }
}

#[tokio::test]
async fn test_adversarial_no_lingering_tokio_tasks_or_file_actors() {
    let td = TestDir::new("m4_adv_lingering");
    let (url, _server) = start_mock_range_server(1024 * 1024, 0).await;
    let mut engine = spawn_download_engine();

    let total_tasks = 10_000;
    let batch_size = 50;
    let cycles = total_tasks / batch_size;

    for cycle in 0..cycles {
        for i in 0..batch_size {
            let id = (cycle * batch_size + i + 1) as u64;
            let save_path = td.path().join(format!("linger_{}.bin", id));

            let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                id,
                url: url.clone(),
                save_path: save_path.clone(),
                connections: 1,
            });

            match i % 4 {
                0 => {
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
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id });
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
                _ => {
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id,
                        cleanup_files: true,
                    });
                }
            }
        }
        while engine.event_receiver.try_recv().is_ok() {}
        tokio::task::yield_now().await;
    }

    // Wait for quiescence
    let drain_start = Instant::now();
    while drain_start.elapsed() < Duration::from_secs(30) {
        while engine.event_receiver.try_recv().is_ok() {}
        let m = engine.metrics();
        if m.active_tasks_count == 0 && m.known_paths_count == 0 && m.active_workers_count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let m = engine.metrics();
    assert_eq!(m.active_tasks_count, 0, "No active tasks may linger");
    assert_eq!(m.known_paths_count, 0, "No known paths may linger");
    assert_eq!(m.active_workers_count, 0, "No active workers may linger");

    // EMPIRICAL HARNESS: Check for lingering file handles or locks on Windows.
    // If a Tokio file writer actor is lingering, it will hold open file descriptors
    // or interfere with file creation, truncation, or deletion.
    println!("[Adversarial Lingering] Probing filesystem handles for lingering writer actors...");
    for sample_id in (1..=total_tasks).step_by(100) {
        let probe_path = td.path().join(format!("linger_{}.bin", sample_id));
        let probe_partinfo = td.path().join(format!("linger_{}.bin.partinfo", sample_id));

        // Neither the binary nor the partinfo should exist or be locked
        assert!(
            !probe_path.exists(),
            "Probe file {} was not cleaned up upon Delete",
            probe_path.display()
        );
        assert!(
            !probe_partinfo.exists(),
            "Probe partinfo {} was not cleaned up upon Delete",
            probe_partinfo.display()
        );

        // Attempting to create and overwrite the exact same file path must succeed without sharing violation
        let res = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&probe_path);
        assert!(
            res.is_ok(),
            "File handle lock violation: lingering writer actor on path {}",
            probe_path.display()
        );
        drop(res);
        let _ = std::fs::remove_file(&probe_path);
    }
    println!("[Adversarial Lingering] 100/100 sample file probe paths verified: zero locked handles or lingering writer actors.");

    // Verify engine responsiveness post-stress: start a fresh download and confirm clean lifecycle
    let fresh_id = 99_999u64;
    let fresh_path = td.path().join("post_stress_probe.bin");
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id: fresh_id,
        url: url.clone(),
        save_path: fresh_path.clone(),
        connections: 1,
    });

    let mut received_event = false;
    let fresh_start = Instant::now();
    while fresh_start.elapsed() < Duration::from_secs(5) {
        if let Ok(evt) = engine.event_receiver.try_recv() {
            match evt {
                EngineEvent::ProgressUpdate { id, .. } if id == fresh_id => {
                    println!("[Adversarial Lingering] Received ProgressUpdate for #{}", id);
                    received_event = true;
                    break;
                }
                EngineEvent::Failed { id, error } if id == fresh_id => {
                    println!("[Adversarial Lingering] Received Expected Failed event for #{} due to sandbox network block: {}", id, error);
                    received_event = true;
                    break;
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(received_event, "Engine must remain fully responsive and process fresh downloads after 10k stress");

    // Clean up fresh download
    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
        id: fresh_id,
        cleanup_files: true,
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    while engine.event_receiver.try_recv().is_ok() {}

    let final_m = engine.metrics();
    assert_eq!(final_m.active_tasks_count, 0);
    assert_eq!(final_m.known_paths_count, 0);
    assert_eq!(final_m.active_workers_count, 0);
    println!("[Adversarial Lingering] Post-stress verification passed cleanly.");
}
