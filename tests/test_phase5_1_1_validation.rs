//! Phase 5.1.1 Validation Test Suite
//! Verifies:
//! - R1: Bandwidth throttling accuracy at 100 Mbps (~12.5 MB/s) over >= 10s interval
//! - R1: Bandwidth throttling accuracy at 500 Mbps (~62.5 MB/s) over >= 10s interval
//! - R3: Writer Actor profiling instrumentation captures queue depth, seeks, latencies, and throughput

use not_download_manager::bench_server::{compute_deterministic_sha256, start_benchmark_server, ServerConfig};
use not_download_manager::engine::{
    get_task_writer_metrics, set_task_expected_sha256, spawn_download_engine, EngineCommand,
    EngineEvent,
};
use not_download_manager::types::HashVerificationStatus;
use std::path::{Path, PathBuf};
use std::time::Instant;

struct BenchTestDir(PathBuf);

impl BenchTestDir {
    fn new(name: &str) -> Self {
        // Use Drive D: if available to preserve space, otherwise fallback to temp_dir
        let base = if Path::new("D:\\idman_bench_p51").exists() {
            PathBuf::from("D:\\idman_bench_p51")
        } else {
            std::env::temp_dir()
        };

        let p = base.join(format!(
            "bench_p511_{}_{}_{}",
            name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::create_dir_all(&p);
        BenchTestDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for BenchTestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Helper to run a full NDM download against the benchmark server and wait for completion.
async fn run_bench_download(
    task_id: u64,
    url: String,
    save_path: PathBuf,
    concurrency: usize,
    expected_hash: Option<String>,
) -> (f64, HashVerificationStatus) {
    let mut engine = spawn_download_engine();
    set_task_expected_sha256(task_id, expected_hash);

    let start = Instant::now();

    engine
        .cmd_sender
        .send(EngineCommand::StartDownload {
            id: task_id,
            url,
            save_path,
            connections: concurrency,
        })
        .expect("StartDownload command send failed");

    let mut final_status = HashVerificationStatus::NotConfigured;

    while let Some(evt) = engine.event_receiver.recv().await {
        match evt {
            EngineEvent::Finished { id, hash_status } if id == task_id => {
                final_status = hash_status;
                break;
            }
            EngineEvent::Failed { id, error } if id == task_id => {
                panic!("Download failed unexpectedly: {}", error);
            }
            _ => {}
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    (elapsed, final_status)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_bandwidth_throttling_100_mbps_10_seconds() {
    // 100 Mbps = 100,000,000 bits/sec = 12,500,000 bytes/sec
    // 125,000,000 bytes takes exactly 10.0 seconds at theoretical maximum
    const TOTAL_BYTES: u64 = 125_000_000;
    let expected_hash = compute_deterministic_sha256(TOTAL_BYTES);

    let server_config = ServerConfig {
        port: 0,
        total_file_size: TOTAL_BYTES,
        throttle_mbps: Some(100.0),
        latency_ms: 0,
        max_connections: Some(16),
        ..Default::default()
    };

    let server = start_benchmark_server(server_config).await;
    let td = BenchTestDir::new("throttle_100mbps");
    let save_path = td.path().join("download_100mbps.bin");

    let task_id = 1001u64;
    // Download with 8 connections to verify aggregate bandwidth across all connections
    let (elapsed, status) = run_bench_download(
        task_id,
        server.url.clone(),
        save_path.clone(),
        8,
        Some(expected_hash.clone()),
    )
    .await;

    server.shutdown().await;

    let total_mb_decimal = TOTAL_BYTES as f64 / 1_000_000.0;
    let throughput_mb_s = total_mb_decimal / elapsed;
    let throughput_mib_s = (TOTAL_BYTES as f64 / (1024.0 * 1024.0)) / elapsed;

    println!(
        "\n--- [100 Mbps Validation Test Result] ---"
    );
    println!("Total Elapsed Time:   {:.3} s", elapsed);

    let writer_metrics = get_task_writer_metrics(task_id).expect("Writer metrics must be recorded");
    let dl_secs = writer_metrics.actor_lifetime_ms / 1000.0;
    let dl_mb_s = total_mb_decimal / dl_secs;

    println!("Writer Actor Lifetime:{:.3} s", dl_secs);
    println!("Writer Disk Write:    {:.2} MB/s", writer_metrics.disk_write_mb_s);
    println!("Download Throughput:  {:.2} MB/s (target: 12.50 MB/s +- 5%)", dl_mb_s);
    println!("Decimal Throughput:   {:.2} MB/s (wall-clock with SHA-256)", throughput_mb_s);
    println!("Binary Throughput:    {:.2} MiB/s", throughput_mib_s);
    println!("SHA-256 Status:       {:?}", status);
    println!("Writer Actor Seeks:   {}", writer_metrics.total_seek_operations);
    println!("Writer Max Queue:     {}", writer_metrics.max_queue_depth);
    println!("Writer Write Latency: {:.1} us", writer_metrics.avg_write_latency_us);
    println!("Channel Recv Wait:    {:.2} ms", writer_metrics.total_channel_recv_wait_time_us as f64 / 1000.0);
    println!("Actor Active Time:    {:.2} ms", writer_metrics.total_actor_active_time_us as f64 / 1000.0);

    // Verify interval >= 9.5s (nominal 10s with +-5% tolerance)
    assert!(
        dl_secs >= 9.5,
        "Download time {:.2}s must be >= 9.5s to verify over >= 10s interval",
        dl_secs
    );

    // Verify throughput within 12.5 MB/s +- 5% (11.875 MB/s to 13.125 MB/s)
    let min_allowed = 12.50 * 0.95; // 11.875
    let max_allowed = 12.50 * 1.05; // 13.125
    assert!(
        dl_mb_s >= min_allowed && dl_mb_s <= max_allowed,
        "Throughput {:.2} MB/s must be within [{:.2}, {:.2}] MB/s (+- 5%)",
        dl_mb_s,
        min_allowed,
        max_allowed
    );

    assert_eq!(status, HashVerificationStatus::Verified);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_bandwidth_throttling_500_mbps_10_seconds() {
    // 500 Mbps = 500,000,000 bits/sec = 62,500,000 bytes/sec
    // 625,000,000 bytes takes exactly 10.0 seconds at theoretical maximum
    const TOTAL_BYTES: u64 = 625_000_000;
    let expected_hash = compute_deterministic_sha256(TOTAL_BYTES);

    let server_config = ServerConfig {
        port: 0,
        total_file_size: TOTAL_BYTES,
        throttle_mbps: Some(500.0),
        latency_ms: 0,
        max_connections: Some(16),
        ..Default::default()
    };

    let server = start_benchmark_server(server_config).await;
    let td = BenchTestDir::new("throttle_500mbps");
    let save_path = td.path().join("download_500mbps.bin");

    let task_id = 1002u64;
    // Download with 8 connections
    let (elapsed, status) = run_bench_download(
        task_id,
        server.url.clone(),
        save_path.clone(),
        8,
        Some(expected_hash.clone()),
    )
    .await;

    server.shutdown().await;

    let total_mb_decimal = TOTAL_BYTES as f64 / 1_000_000.0;
    let throughput_mb_s = total_mb_decimal / elapsed;
    let throughput_mib_s = (TOTAL_BYTES as f64 / (1024.0 * 1024.0)) / elapsed;

    let writer_metrics = get_task_writer_metrics(task_id).expect("Writer metrics must be recorded");
    let dl_secs = writer_metrics.actor_lifetime_ms / 1000.0;
    let dl_mb_s = total_mb_decimal / dl_secs;

    println!(
        "\n--- [500 Mbps Validation Test Result] ---"
    );
    println!("Total Elapsed Time:   {:.3} s", elapsed);
    println!("Writer Actor Lifetime:{:.3} s", dl_secs);
    println!("Writer Disk Write:    {:.2} MB/s", writer_metrics.disk_write_mb_s);
    println!("Download Throughput:  {:.2} MB/s (target: 62.50 MB/s +- 5%)", dl_mb_s);
    println!("Decimal Throughput:   {:.2} MB/s (wall-clock with SHA-256)", throughput_mb_s);
    println!("Binary Throughput:    {:.2} MiB/s", throughput_mib_s);
    println!("SHA-256 Status:       {:?}", status);
    println!("Writer Actor Seeks:   {}", writer_metrics.total_seek_operations);
    println!("Writer Max Queue:     {}", writer_metrics.max_queue_depth);
    println!("Writer Write Latency: {:.1} us", writer_metrics.avg_write_latency_us);
    println!("Channel Recv Wait:    {:.2} ms", writer_metrics.total_channel_recv_wait_time_us as f64 / 1000.0);
    println!("Actor Active Time:    {:.2} ms", writer_metrics.total_actor_active_time_us as f64 / 1000.0);

    // Verify interval >= 9.5s (nominal 10s with +-5% tolerance)
    assert!(
        dl_secs >= 9.5,
        "Download time {:.2}s must be >= 9.5s to verify over >= 10s interval",
        dl_secs
    );

    // Verify throughput within 62.5 MB/s +- 5% (59.375 MB/s to 65.625 MB/s)
    let min_allowed = 62.50 * 0.95; // 59.375
    let max_allowed = 62.50 * 1.05; // 65.625
    assert!(
        dl_mb_s >= min_allowed && dl_mb_s <= max_allowed,
        "Throughput {:.2} MB/s must be within [{:.2}, {:.2}] MB/s (+- 5%)",
        dl_mb_s,
        min_allowed,
        max_allowed
    );

    assert_eq!(status, HashVerificationStatus::Verified);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_writer_actor_profiling_verification() {
    const TOTAL_BYTES: u64 = 20 * 1024 * 1024; // 20 MiB
    let expected_hash = compute_deterministic_sha256(TOTAL_BYTES);

    let server_config = ServerConfig {
        port: 0,
        total_file_size: TOTAL_BYTES,
        throttle_mbps: None, // Unrestricted LAN
        latency_ms: 0,
        ..Default::default()
    };

    let server = start_benchmark_server(server_config).await;
    let td = BenchTestDir::new("writer_profiling");
    let save_path = td.path().join("download_profiling.bin");

    let task_id = 1003u64;
    let (elapsed, status) = run_bench_download(
        task_id,
        server.url.clone(),
        save_path.clone(),
        8,
        Some(expected_hash),
    )
    .await;

    server.shutdown().await;
    assert_eq!(status, HashVerificationStatus::Verified);

    let m = get_task_writer_metrics(task_id).expect("Writer metrics must exist");
    println!("\n--- [Writer Actor Profile (20 MiB Unrestricted LAN)] ---");
    println!("Task ID:                {}", m.task_id);
    println!("Total Commands:         {}", m.total_commands_processed);
    println!("WriteBytes Commands:    {}", m.total_write_bytes_commands);
    println!("CommitChunk Commands:   {}", m.total_commit_commands);
    println!("Bytes Written:          {} bytes", m.total_bytes_written);
    println!("Seek Operations:        {}", m.total_seek_operations);
    println!("Consecutive Seeks Saved:{}", m.consecutive_seeks_avoided);
    println!("Max Channel Queue Depth:{}", m.max_queue_depth);
    println!("Avg Channel Queue Depth:{:.2}", m.avg_queue_depth);
    println!("Avg Seek Latency:       {:.2} us", m.avg_seek_latency_us);
    println!("Avg Write Latency:      {:.2} us", m.avg_write_latency_us);
    println!("Avg ACK Latency:        {:.2} us", m.avg_ack_latency_us);
    println!("Disk Write Speed:       {:.2} MB/s", m.disk_write_mb_s);
    println!("Command Throughput:     {:.1} cmds/s", m.command_throughput_per_sec);
    println!("Actor Active Time:      {:.2} ms", m.total_actor_active_time_us as f64 / 1000.0);
    println!("Channel Recv Wait Time: {:.2} ms", m.total_channel_recv_wait_time_us as f64 / 1000.0);
    println!("Actor Lifetime:         {:.2} ms", m.actor_lifetime_ms);

    assert_eq!(m.task_id, task_id);
    assert_eq!(m.total_bytes_written, TOTAL_BYTES);
    assert!(m.total_commands_processed > 0);
    assert!(m.total_write_bytes_commands > 0);
    assert!(m.total_commit_commands > 0);
    assert!(m.disk_write_mb_s > 0.0);
    assert!(m.command_throughput_per_sec > 0.0);
    assert!(elapsed > 0.0);
}
