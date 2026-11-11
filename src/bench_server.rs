//! Controlled Local HTTP Benchmark Server for HyperDownload (NDM) Performance Baseline
//!
//! Supports:
//! - Full RFC 7233 HTTP Range requests (206 Partial Content)
//! - HTTP HEAD & GET
//! - Deterministic mathematical payload generation (0 bytes disk, 64 KiB fixed RAM)
//! - Configurable Bandwidth Throttling (token bucket / chunk pacing)
//! - Configurable Response Latency
//! - Configurable Connection Limits (via Semaphore)
//! - HTTP 429 & 503 simulation
//! - Connection interruption simulation (simulating broken TCP socket mid-stream)
//! - Real-time metrics counters

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};

#[cfg(windows)]
#[link(name = "winmm")]
unsafe extern "system" {
    fn timeBeginPeriod(uPeriod: u32) -> u32;
    fn timeEndPeriod(uPeriod: u32) -> u32;
}

/// RAII Guard ensuring Windows timer interrupt resolution is set to 1ms
/// for high-precision sleep and token bucket pacing during benchmarks.
pub struct WindowsTimerResolutionGuard;

impl WindowsTimerResolutionGuard {
    pub fn new() -> Self {
        #[cfg(windows)]
        unsafe {
            timeBeginPeriod(1);
        }
        Self
    }
}

impl Drop for WindowsTimerResolutionGuard {
    fn drop(&mut self) {
        #[cfg(windows)]
        unsafe {
            timeEndPeriod(1);
        }
    }
}

/// Shared Global Token Bucket with debt pacing for exact aggregate bandwidth throttling across all connections.
///
/// Converts configured Mbps (megabits per second, decimal) to bytes per second:
/// - 100 Mbps = 100,000,000 bits/s = 12,500,000 bytes/s (12.50 MB/s decimal = 11.92 MiB/s)
/// - 500 Mbps = 500,000,000 bits/s = 62,500,000 bytes/s (62.50 MB/s decimal = 59.60 MiB/s)
pub struct GlobalTokenBucket {
    available_tokens: f64,
    max_tokens: f64,
    bytes_per_sec: f64,
    last_update: Instant,
}

impl GlobalTokenBucket {
    pub fn new(mbps: f64) -> Self {
        let bytes_per_sec = (mbps * 1_000_000.0) / 8.0;
        let max_tokens = (bytes_per_sec * 0.05).max(65536.0); // 50ms burst maximum
        Self {
            available_tokens: max_tokens,
            max_tokens,
            bytes_per_sec,
            last_update: Instant::now(),
        }
    }

    /// Acquires tokens for `bytes_needed`. Returns `Some(delay)` if caller must sleep
    /// to maintain the aggregate bandwidth limit.
    pub fn acquire(&mut self, bytes_needed: usize) -> Option<Duration> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;

        // Replenish tokens based on real elapsed wall time
        self.available_tokens =
            (self.available_tokens + elapsed * self.bytes_per_sec).min(self.max_tokens);

        self.available_tokens -= bytes_needed as f64;

        if self.available_tokens < 0.0 {
            let deficit = -self.available_tokens;
            let wait_secs = deficit / self.bytes_per_sec;
            Some(Duration::from_secs_f64(wait_secs))
        } else {
            None
        }
    }
}

/// Generates deterministic bytes for any stream offset without whole-file allocation.
#[inline(always)]
pub fn get_pattern_chunk(offset: u64, buf: &mut [u8]) {
    for (i, b) in buf.iter_mut().enumerate() {
        let idx = (offset + i as u64) as usize;
        *b = ((idx.wrapping_mul(131).wrapping_add(17)) ^ (idx >> 5)) as u8;
    }
}

/// Computes the exact SHA-256 hexadecimal string of the deterministic pattern for `total_len`.
pub fn compute_deterministic_sha256(total_len: u64) -> String {
    use crate::db::Sha256Hasher;
    let mut hasher = Sha256Hasher::new();
    let mut buf = [0u8; 65536];
    let mut curr = 0u64;
    while curr < total_len {
        let remaining = (total_len - curr) as usize;
        let chunk_len = remaining.min(buf.len());
        get_pattern_chunk(curr, &mut buf[..chunk_len]);
        hasher.update(&buf[..chunk_len]);
        curr += chunk_len as u64;
    }
    hasher.finalize_hex()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryAfterMode {
    DeltaSeconds(u64),
    HttpDateOffset(Duration),
    VariableSeconds(u64, u64),
    None,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub port: u16,
    pub total_file_size: u64,
    pub throttle_mbps: Option<f64>,
    pub latency_ms: u64,
    pub max_connections: Option<usize>,
    pub rate_limit_429_frequency: usize,
    pub server_busy_503_frequency: usize,
    pub interrupt_after_bytes: Option<u64>,
    pub retry_after_mode: RetryAfterMode,
    pub throttle_max_count: usize,
    pub throttle_burst_cycle: Option<(usize, usize)>,
    pub prolonged_throttle_window: Option<Duration>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 0,
            total_file_size: 100 * 1024 * 1024,
            throttle_mbps: None,
            latency_ms: 0,
            max_connections: None,
            rate_limit_429_frequency: 0,
            server_busy_503_frequency: 0,
            interrupt_after_bytes: None,
            retry_after_mode: RetryAfterMode::DeltaSeconds(1),
            throttle_max_count: 0,
            throttle_burst_cycle: None,
            prolonged_throttle_window: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct ServerMetrics {
    pub total_requests: AtomicUsize,
    pub head_requests: AtomicUsize,
    pub get_requests: AtomicUsize,
    pub range_requests: AtomicUsize,
    pub rate_limit_429_count: AtomicUsize,
    pub server_busy_503_count: AtomicUsize,
    pub interrupted_connections: AtomicUsize,
    pub total_bytes_served: AtomicU64,
    pub active_connections: AtomicUsize,
    pub peak_connections: AtomicUsize,
    pub first_request_time: std::sync::Mutex<Option<Instant>>,
}

pub struct BenchmarkServerHandle {
    pub addr: SocketAddr,
    pub url: String,
    pub metrics: Arc<ServerMetrics>,
    pub shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub server_task: tokio::task::JoinHandle<()>,
    pub _timer_guard: Arc<WindowsTimerResolutionGuard>,
}

impl BenchmarkServerHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.server_task.await;
    }
}

/// Spawns the benchmark server asynchronously on a loopback address.
pub async fn start_benchmark_server(config: ServerConfig) -> BenchmarkServerHandle {
    let timer_guard = Arc::new(WindowsTimerResolutionGuard::new());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .expect("Binding loopback listener must succeed");
    let addr = listener.local_addr().expect("Local address available");
    let url = format!("http://127.0.0.1:{}/benchmark.bin", addr.port());

    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_metrics = metrics.clone();
    let conn_semaphore = config
        .max_connections
        .map(|limit| Arc::new(Semaphore::new(limit)));

    let rate_limiter = config
        .throttle_mbps
        .filter(|&mbps| mbps > 0.0)
        .map(|mbps| Arc::new(Mutex::new(GlobalTokenBucket::new(mbps))));

    let server_task = tokio::spawn(async move {
        let config = Arc::new(config);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    break;
                }
                accept_res = listener.accept() => {
                    let (socket, _) = match accept_res {
                        Ok(conn) => conn,
                        Err(_) => break,
                    };

                    let metrics = server_metrics.clone();
                    let config = config.clone();
                    let sem = conn_semaphore.clone();
                    let limiter = rate_limiter.clone();

                    tokio::spawn(async move {
                        let _permit = match sem {
                            Some(s) => match s.acquire_owned().await {
                                Ok(p) => Some(p),
                                Err(_) => return,
                            },
                            None => None,
                        };

                        let current_active = metrics.active_connections.fetch_add(1, Ordering::SeqCst) + 1;
                        metrics.peak_connections.fetch_max(current_active, Ordering::SeqCst);

                        handle_connection(socket, config, metrics.clone(), limiter).await;

                        metrics.active_connections.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            }
        }
    });

    BenchmarkServerHandle {
        addr,
        url,
        metrics,
        shutdown_tx: Some(shutdown_tx),
        server_task,
        _timer_guard: timer_guard,
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    config: Arc<ServerConfig>,
    metrics: Arc<ServerMetrics>,
    rate_limiter: Option<Arc<Mutex<GlobalTokenBucket>>>,
) {
    let mut buf = vec![0u8; 4096];
    let n = match socket.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let req_str = String::from_utf8_lossy(&buf[..n]);
    let req_id = metrics.total_requests.fetch_add(1, Ordering::SeqCst) + 1;

    // Latency injection
    if config.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(config.latency_ms)).await;
    }

    let format_retry_after = |mode: &RetryAfterMode| -> String {
        match mode {
            RetryAfterMode::DeltaSeconds(secs) => format!("Retry-After: {}\r\n", secs),
            RetryAfterMode::HttpDateOffset(dur) => {
                let future_time = chrono::Utc::now() + chrono::Duration::from_std(*dur).unwrap_or(chrono::Duration::seconds(1));
                format!("Retry-After: {}\r\n", future_time.to_rfc2822())
            }
            RetryAfterMode::VariableSeconds(min_s, max_s) => {
                let range = max_s.saturating_sub(*min_s).max(1);
                let secs = min_s + (req_id as u64 % (range + 1));
                format!("Retry-After: {}\r\n", secs)
            }
            RetryAfterMode::None => String::new(),
        }
    };

    let total_len = config.total_file_size;

    if req_str.starts_with("HEAD") {
        metrics.head_requests.fetch_add(1, Ordering::Relaxed);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-bench-{}\"\r\nLast-Modified: Mon, 10 Nov 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n",
            total_len, total_len
        );
        let _ = socket.write_all(resp.as_bytes()).await;
        let _ = socket.shutdown().await;
        return;
    }

    // Prolonged Throttle & Server Recovery Simulation
    if let Some(window) = config.prolonged_throttle_window {
        let is_in_window = {
            let mut t_lock = metrics.first_request_time.lock().unwrap_or_else(|p| p.into_inner());
            let t0 = *t_lock.get_or_insert_with(Instant::now);
            Instant::now() < t0 + window
        };

        if is_in_window {
            metrics.rate_limit_429_count.fetch_add(1, Ordering::Relaxed);
            let ra_header = format_retry_after(&config.retry_after_mode);
            let resp = format!(
                "HTTP/1.1 429 Too Many Requests\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                ra_header
            );
            let _ = socket.write_all(resp.as_bytes()).await;
            let _ = socket.shutdown().await;
            return;
        }
    }

    // Repeated Throttle Burst & Recovery Simulation
    if let Some((burst_count, cycle_period)) = config.throttle_burst_cycle {
        if cycle_period > 0 && (req_id % cycle_period) < burst_count {
            let current_count = metrics.rate_limit_429_count.load(Ordering::Relaxed);
            if config.throttle_max_count == 0 || current_count < config.throttle_max_count {
                metrics.rate_limit_429_count.fetch_add(1, Ordering::Relaxed);
                let ra_header = format_retry_after(&config.retry_after_mode);
                let resp = format!(
                    "HTTP/1.1 429 Too Many Requests\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                    ra_header
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                let _ = socket.shutdown().await;
                return;
            }
        }
    }

    // HTTP 429 Simulation
    if config.rate_limit_429_frequency > 0 && req_id % config.rate_limit_429_frequency == 0 {
        let current_count = metrics.rate_limit_429_count.load(Ordering::Relaxed);
        if config.throttle_max_count == 0 || current_count < config.throttle_max_count {
            metrics.rate_limit_429_count.fetch_add(1, Ordering::Relaxed);
            let ra_header = format_retry_after(&config.retry_after_mode);
            let resp = format!(
                "HTTP/1.1 429 Too Many Requests\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                ra_header
            );
            let _ = socket.write_all(resp.as_bytes()).await;
            let _ = socket.shutdown().await;
            return;
        }
    }

    // HTTP 503 Simulation
    if config.server_busy_503_frequency > 0 && req_id % config.server_busy_503_frequency == 0 {
        let current_count = metrics.server_busy_503_count.load(Ordering::Relaxed);
        if config.throttle_max_count == 0 || current_count < config.throttle_max_count {
            metrics.server_busy_503_count.fetch_add(1, Ordering::Relaxed);
            let ra_header = format_retry_after(&config.retry_after_mode);
            let resp = format!(
                "HTTP/1.1 503 Service Unavailable\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                ra_header
            );
            let _ = socket.write_all(resp.as_bytes()).await;
            let _ = socket.shutdown().await;
            return;
        }
    }

    if req_str.starts_with("GET") {
        metrics.get_requests.fetch_add(1, Ordering::Relaxed);

        let mut is_range = false;
        let mut slice_start = 0u64;
        let mut slice_end = total_len.saturating_sub(1);

        if let Some(r_line) = req_str.lines().find(|l| l.to_lowercase().starts_with("range:")) {
            if let Some(bytes_part) = r_line.split('=').nth(1) {
                let parts: Vec<&str> = bytes_part.trim().split('-').collect();
                if let Ok(s) = parts[0].parse::<u64>() {
                    slice_start = s;
                    is_range = true;
                }
                if parts.len() > 1 && !parts[1].is_empty() {
                    if let Ok(e) = parts[1].parse::<u64>() {
                        slice_end = e.min(total_len.saturating_sub(1));
                        is_range = true;
                    }
                }
            }
        }

        let slice_len = if slice_start <= slice_end {
            slice_end - slice_start + 1
        } else {
            0
        };

        let headers = if is_range {
            metrics.range_requests.fetch_add(1, Ordering::Relaxed);
            format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-bench-{}\"\r\nLast-Modified: Mon, 10 Nov 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n",
                slice_start, slice_end, total_len, slice_len, total_len
            )
        } else {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-bench-{}\"\r\nLast-Modified: Mon, 10 Nov 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n",
                total_len, total_len
            )
        };

        if let Err(_) = socket.write_all(headers.as_bytes()).await {
            return;
        }

        // Stream body with 64 KiB buffer
        let mut chunk_buf = [0u8; 65536];
        let mut curr = slice_start;
        let mut sent_on_conn = 0u64;

        while curr <= slice_end {
            let remaining = (slice_end - curr + 1) as usize;
            let to_send = remaining.min(chunk_buf.len());

            // Bandwidth throttling via global rate limiter across all active connections
            if let Some(ref limiter) = rate_limiter {
                let wait_delay = {
                    let mut l = limiter.lock().await;
                    l.acquire(to_send)
                };
                if let Some(delay) = wait_delay {
                    tokio::time::sleep(delay).await;
                }
            }

            get_pattern_chunk(curr, &mut chunk_buf[..to_send]);

            if let Err(_) = socket.write_all(&chunk_buf[..to_send]).await {
                break;
            }

            curr += to_send as u64;
            sent_on_conn += to_send as u64;
            metrics.total_bytes_served.fetch_add(to_send as u64, Ordering::Relaxed);

            // Connection Interruption simulation
            if let Some(interrupt_limit) = config.interrupt_after_bytes {
                if sent_on_conn >= interrupt_limit {
                    metrics.interrupted_connections.fetch_add(1, Ordering::Relaxed);
                    // Abrupt drop without shutdown to simulate connection drop
                    return;
                }
            }
        }

        let _ = socket.shutdown().await;
    }
}
