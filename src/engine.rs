use crate::types::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

pub const DEFAULT_CHUNK_SIZE: u64 = 2 * 1024 * 1024; // 2 MB default chunk size

pub fn get_configured_chunk_size() -> u64 {
    if let Ok(val) = std::env::var("NOT_DOWNLOAD_MANAGER_CHUNK_SIZE")
        .or_else(|_| std::env::var("HYPERDOWNLOAD_CHUNK_SIZE"))
    {
        if let Ok(size) = val.parse::<u64>() {
            return size.max(524_288);
        }
    }
    DEFAULT_CHUNK_SIZE
}
pub const MAX_CHUNK_RETRIES: u32 = 5;

pub use crate::types::{
    ActiveTaskGuard, EngineCommand, EngineEvent, EngineMetrics, TaskControl,
};

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
static EXPECTED_HASHES: std::sync::LazyLock<Mutex<HashMap<u64, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
static LAST_WRITER_METRICS: std::sync::LazyLock<Mutex<HashMap<u64, WriterActorMetrics>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Configurable threshold in bytes for chunk coalescing in the Writer Actor.
/// Default: 0 (disabled to evaluate pure R2 sequential cursor tracking).
pub static COALESCE_THRESHOLD_BYTES: AtomicUsize = AtomicUsize::new(0);

pub fn set_coalesce_threshold_bytes(bytes: usize) {
    COALESCE_THRESHOLD_BYTES.store(bytes, Ordering::Relaxed);
}

pub fn get_coalesce_threshold_bytes() -> usize {
    COALESCE_THRESHOLD_BYTES.load(Ordering::Relaxed)
}

/// Allows disabling sequential cursor optimization for A/B benchmarking against Phase 5.1.1 baseline.
pub static DISABLE_CURSOR_OPTIMIZATION: AtomicBool = AtomicBool::new(false);

pub fn set_disable_cursor_optimization(disabled: bool) {
    DISABLE_CURSOR_OPTIMIZATION.store(disabled, Ordering::Relaxed);
}

pub fn get_disable_cursor_optimization() -> bool {
    DISABLE_CURSOR_OPTIMIZATION.load(Ordering::Relaxed)
}

/// Diagnostic profiling metrics for the Writer Actor in Phase 5.1.1 & Phase 5.2.1
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WriterActorMetrics {
    pub task_id: u64,
    pub total_commands_processed: u64,
    pub total_write_bytes_commands: u64,
    pub total_commit_commands: u64,
    pub total_bytes_written: u64,

    // R2 & R3 Seek and Coalescing Metrics
    pub total_seek_operations: u64,
    pub consecutive_seeks_avoided: u64,
    pub adjacent_writes_count: u64,
    pub non_adjacent_writes_count: u64,
    pub coalesced_writes_count: u64,
    pub coalesced_bytes_saved: u64,

    // R1 Separated Queue & In-flight Metrics
    pub max_queue_depth: usize,
    pub avg_queue_depth: f64,
    pub max_channel_queue_depth: usize,
    pub avg_channel_queue_depth: f64,
    pub max_in_flight_commands: usize,
    pub avg_in_flight_commands: f64,
    pub queue_depth_samples: u64,

    // Independent Latency Timers (microseconds)
    pub total_seek_time_us: u64,
    pub total_write_time_us: u64,
    pub total_ack_time_us: u64,
    pub total_flush_time_us: u64,
    pub total_channel_recv_wait_time_us: u64,
    pub total_actor_active_time_us: u64,
    pub actor_lifetime_ms: f64,

    // Averages and Speeds
    pub avg_seek_latency_us: f64,
    pub avg_write_latency_us: f64,
    pub avg_ack_latency_us: f64,
    pub disk_write_mb_s: f64,
    pub command_throughput_per_sec: f64,
}

pub fn get_task_writer_metrics(id: u64) -> Option<WriterActorMetrics> {
    LAST_WRITER_METRICS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&id)
        .cloned()
}

pub fn record_task_writer_metrics(id: u64, metrics: WriterActorMetrics) {
    let mut map = LAST_WRITER_METRICS
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if map.len() >= 64 {
        if let Some(&first_key) = map.keys().next() {
            map.remove(&first_key);
        }
    }
    map.insert(id, metrics);
}

pub fn clear_task_writer_metrics(id: u64) {
    let mut map = LAST_WRITER_METRICS
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    map.remove(&id);
}

pub fn set_task_expected_sha256(id: u64, hash: Option<String>) {
    let mut map = EXPECTED_HASHES.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(h) = hash {
        map.insert(id, h);
    } else {
        map.remove(&id);
    }
}

fn lookup_expected_sha256(id: u64) -> Option<String> {
    for db_name in &["not_download_manager.db", "downloads.db", "idman.db"] {
        if Path::new(db_name).exists() {
            if let Ok(db) = crate::db::Database::open(db_name) {
                if let Ok((expected, _)) = db.get_hash_status(id) {
                    if let Some(h) = expected {
                        if !h.trim().is_empty() {
                            return Some(h);
                        }
                    }
                }
            }
        }
    }
    None
}

pub static INACTIVE_SAVE_PATHS: std::sync::LazyLock<Mutex<HashMap<u64, PathBuf>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn register_inactive_save_path(id: u64, path: PathBuf) {
    INACTIVE_SAVE_PATHS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(id, path);
}

fn lookup_save_path(id: u64) -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(env_db) = std::env::var("NOT_DOWNLOAD_MANAGER_DB_PATH")
        .or_else(|_| std::env::var("HYPERDOWNLOAD_DB_PATH"))
    {
        candidates.push(PathBuf::from(env_db));
    }
    candidates.push(PathBuf::from("not_download_manager.db"));
    candidates.push(PathBuf::from("downloads.db"));
    candidates.push(PathBuf::from("idman.db"));
    let dl_dir = crate::types::dirs_or_fallback();
    candidates.push(dl_dir.join("not_download_manager.db"));
    candidates.push(dl_dir.join("downloads.db"));
    candidates.push(dl_dir.join("idman.db"));

    for db_path in candidates {
        if db_path.exists() {
            if let Ok(db) = crate::db::Database::open(&db_path) {
                if let Ok(path) = db.get_save_path(id) {
                    return Some(path);
                }
            }
        }
    }
    None
}

struct WorkerGuard(Arc<AtomicUsize>);

impl WorkerGuard {
    fn new(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub enum EngineError {
    Transient {
        reason: String,
        retry_after_secs: Option<u64>,
    },
    Fatal(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Transient { reason, .. } => write!(f, "Transient error: {}", reason),
            EngineError::Fatal(reason) => write!(f, "Fatal error: {}", reason),
        }
    }
}

impl std::error::Error for EngineError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChunkStatus {
    Pending,
    Leased { worker_id: usize, generation: u64 },
    Committed,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkChunk {
    pub id: usize,
    pub start: u64,
    pub end: u64, // Half-open interval [start, end)
    pub status: ChunkStatus,
    pub retries: u32,
}

#[derive(Debug, Clone)]
pub struct ChunkLease {
    pub chunk_id: usize,
    pub start: u64,
    pub end: u64,
    pub generation: u64,
}

fn default_cp_version() -> u32 {
    1
}

fn default_cp_chunk_size() -> u64 {
    DEFAULT_CHUNK_SIZE
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LegacySegment {
    pub segment_index: usize,
    pub start_byte: u64,
    pub end_byte: u64,
    pub current_byte: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadCheckpoint {
    #[serde(default = "default_cp_version")]
    pub version: u32,
    pub total_size: u64,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default = "default_cp_chunk_size")]
    pub chunk_size: u64,
    #[serde(default)]
    pub committed_chunk_ids: Vec<usize>,
    #[serde(default)]
    pub segments: Option<Vec<LegacySegment>>,
}

pub struct SegmentManager {
    pub total_size: u64,
    pub chunk_size: u64,
    pub chunks: Vec<WorkChunk>,
    pub pending_queue: VecDeque<usize>,
    pub generation_counter: u64,
    pub committed_bytes: u64,
    pub etag: Option<String>,
}

impl SegmentManager {
    pub fn new(total_size: u64, chunk_size: u64, etag: Option<String>) -> Self {
        let chunk_size = chunk_size.max(524_288); // Minimum 512 KB
        let num_chunks = ((total_size + chunk_size - 1) / chunk_size) as usize;
        let mut chunks = Vec::with_capacity(num_chunks);
        let mut pending_queue = VecDeque::with_capacity(num_chunks);

        for i in 0..num_chunks {
            let start = i as u64 * chunk_size;
            let end = ((i as u64 + 1) * chunk_size).min(total_size);
            chunks.push(WorkChunk {
                id: i,
                start,
                end,
                status: ChunkStatus::Pending,
                retries: 0,
            });
            pending_queue.push_back(i);
        }

        Self {
            total_size,
            chunk_size,
            chunks,
            pending_queue,
            generation_counter: 0,
            committed_bytes: 0,
            etag,
        }
    }

    pub fn from_checkpoint(
        total_size: u64,
        etag: Option<String>,
        checkpoint: &DownloadCheckpoint,
    ) -> Result<Self, String> {
        if checkpoint.total_size != total_size {
            return Err("Checkpoint total_size mismatch".into());
        }
        if checkpoint.etag.is_some() && etag.is_some() && checkpoint.etag != etag {
            return Err("Remote ETag changed: checkpoint invalidated".into());
        }

        let chunk_size = checkpoint.chunk_size.max(524_288);
        let num_chunks = ((total_size + chunk_size - 1) / chunk_size) as usize;
        let mut chunks = Vec::with_capacity(num_chunks);
        let mut pending_queue = VecDeque::new();
        let mut committed_bytes = 0u64;

        let mut committed_set: HashSet<usize> =
            checkpoint.committed_chunk_ids.iter().cloned().collect();

        if committed_set.is_empty() {
            if let Some(legacy_segs) = &checkpoint.segments {
                for i in 0..num_chunks {
                    let start = i as u64 * chunk_size;
                    let end = ((i as u64 + 1) * chunk_size).min(total_size);
                    for seg in legacy_segs {
                        if start >= seg.start_byte && end <= seg.current_byte {
                            committed_set.insert(i);
                            break;
                        }
                    }
                }
            }
        }

        for i in 0..num_chunks {
            let start = i as u64 * chunk_size;
            let end = ((i as u64 + 1) * chunk_size).min(total_size);
            let len = end - start;

            if committed_set.contains(&i) {
                chunks.push(WorkChunk {
                    id: i,
                    start,
                    end,
                    status: ChunkStatus::Committed,
                    retries: 0,
                });
                committed_bytes += len;
            } else {
                chunks.push(WorkChunk {
                    id: i,
                    start,
                    end,
                    status: ChunkStatus::Pending,
                    retries: 0,
                });
                pending_queue.push_back(i);
            }
        }

        Ok(Self {
            total_size,
            chunk_size,
            chunks,
            pending_queue,
            generation_counter: 0,
            committed_bytes,
            etag,
        })
    }

    pub fn lease_next_chunk(&mut self, worker_id: usize) -> Option<ChunkLease> {
        while let Some(chunk_id) = self.pending_queue.pop_front() {
            if let Some(chunk) = self.chunks.get_mut(chunk_id) {
                if chunk.status == ChunkStatus::Pending {
                    self.generation_counter += 1;
                    let lease_gen = self.generation_counter;
                    chunk.status = ChunkStatus::Leased {
                        worker_id,
                        generation: lease_gen,
                    };
                    return Some(ChunkLease {
                        chunk_id,
                        start: chunk.start,
                        end: chunk.end,
                        generation: lease_gen,
                    });
                }
            }
        }
        None
    }

    pub fn commit_chunk(&mut self, chunk_id: usize, ack_generation: u64) -> Result<bool, String> {
        if let Some(chunk) = self.chunks.get_mut(chunk_id) {
            match chunk.status {
                ChunkStatus::Leased { generation, .. } => {
                    if generation != ack_generation {
                        return Err(format!(
                            "Stale ACK rejected: chunk {} leased with gen {}, received ACK for gen {}",
                            chunk_id, generation, ack_generation
                        ));
                    }
                    chunk.status = ChunkStatus::Committed;
                    let len = chunk.end - chunk.start;
                    self.committed_bytes += len;
                    Ok(true)
                }
                ChunkStatus::Committed => Ok(false), // Idempotent
                ChunkStatus::Pending => Err(format!(
                    "Invalid ACK: chunk {} was pending but received commit ACK",
                    chunk_id
                )),
            }
        } else {
            Err(format!("Chunk id {} out of bounds", chunk_id))
        }
    }

    pub fn return_failed_chunk(
        &mut self,
        chunk_id: usize,
        expected_generation: u64,
        is_fatal: bool,
    ) -> Result<(), String> {
        if let Some(chunk) = self.chunks.get_mut(chunk_id) {
            if let ChunkStatus::Leased { generation, .. } = chunk.status {
                if generation != expected_generation {
                    return Err(format!(
                        "Stale failure rejected: chunk {} active gen is {}, reported for gen {}",
                        chunk_id, generation, expected_generation
                    ));
                }
            }
            if is_fatal {
                return Err(format!("Fatal error encountered on chunk {}", chunk_id));
            }
            chunk.retries += 1;
            if chunk.retries > MAX_CHUNK_RETRIES {
                return Err(format!(
                    "Chunk {} exceeded maximum retries ({})",
                    chunk_id, MAX_CHUNK_RETRIES
                ));
            }
            chunk.status = ChunkStatus::Pending;
            self.pending_queue.push_front(chunk_id);
            Ok(())
        } else {
            Err(format!("Chunk id {} out of bounds", chunk_id))
        }
    }

    pub fn is_all_committed(&self) -> bool {
        self.committed_bytes >= self.total_size
    }

    pub fn to_checkpoint(&self) -> DownloadCheckpoint {
        let committed_chunk_ids: Vec<usize> = self
            .chunks
            .iter()
            .filter(|c| c.status == ChunkStatus::Committed)
            .map(|c| c.id)
            .collect();

        DownloadCheckpoint {
            version: 1,
            total_size: self.total_size,
            etag: self.etag.clone(),
            chunk_size: self.chunk_size,
            committed_chunk_ids,
            segments: None,
        }
    }

    pub fn generate_ui_segments(
        &self,
        conn_count: usize,
        active_worker_progress: &[(usize, u64, u64)], // (worker_id, current_byte, end_byte)
    ) -> Vec<SegmentProgress> {
        let conn_count = conn_count.clamp(1, 32);
        let slice_len = (self.total_size + conn_count as u64 - 1) / conn_count as u64;
        let mut ui_segs = Vec::with_capacity(conn_count);

        for i in 0..conn_count {
            let s_start = i as u64 * slice_len;
            let s_end = ((i as u64 + 1) * slice_len).min(self.total_size);

            // Compute committed bytes falling in this display slice
            let mut slice_committed = 0u64;
            for c in &self.chunks {
                if c.status == ChunkStatus::Committed {
                    let overlap_start = c.start.max(s_start);
                    let overlap_end = c.end.min(s_end);
                    if overlap_end > overlap_start {
                        slice_committed += overlap_end - overlap_start;
                    }
                }
            }

            // Include in-flight received bytes
            for &(_wid, curr, _end) in active_worker_progress {
                if curr > s_start && curr <= s_end {
                    let active_in_slice = curr.min(s_end) - s_start;
                    if active_in_slice > slice_committed {
                        slice_committed = active_in_slice;
                    }
                }
            }

            let curr = (s_start + slice_committed).min(s_end);
            ui_segs.push(SegmentProgress {
                segment_index: i,
                start_byte: s_start,
                end_byte: s_end,
                current_byte: curr,
                speed: 0.0,
            });
        }

        ui_segs
    }
}

pub enum WriteCommand {
    WriteBytes {
        offset: u64,
        data: bytes::Bytes,
    },
    CommitChunk {
        chunk_id: usize,
        generation: u64,
    },
}

#[derive(Debug)]
pub struct CommitAck {
    pub chunk_id: usize,
    pub generation: u64,
    pub success: bool,
}

pub struct DownloadEngineHandle {
    pub cmd_sender: mpsc::UnboundedSender<EngineCommand>,
    pub event_receiver: mpsc::UnboundedReceiver<EngineEvent>,
    pub active_tasks: Arc<Mutex<HashMap<u64, TaskControl>>>,
    pub known_paths: Arc<Mutex<HashMap<u64, PathBuf>>>,
    pub active_workers: Arc<AtomicUsize>,
}

impl DownloadEngineHandle {
    pub fn metrics(&self) -> EngineMetrics {
        let active_tasks_count = self
            .active_tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len();
        let known_paths_count = self
            .known_paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len();
        let active_workers_count = self.active_workers.load(Ordering::Relaxed);

        EngineMetrics {
            active_tasks_count,
            known_paths_count,
            active_workers_count,
        }
    }

    pub fn set_expected_sha256(&self, id: u64, hash: Option<String>) {
        set_task_expected_sha256(id, hash);
    }

    pub fn get_writer_metrics(&self, id: u64) -> Option<WriterActorMetrics> {
        get_task_writer_metrics(id)
    }

    pub fn set_concurrency_strategy(&self, id: u64, strategy: crate::concurrency::ConcurrencyStrategy) {
        crate::concurrency::set_task_concurrency_strategy(id, strategy);
    }

    pub fn get_concurrency_decisions(&self, id: u64) -> Vec<crate::concurrency::ConcurrencyDecisionEvent> {
        crate::concurrency::get_task_concurrency_decisions(id)
    }

    pub fn get_concurrency_timeline(&self, id: u64) -> Vec<crate::concurrency::ConcurrencyTimelineEntry> {
        crate::concurrency::get_task_concurrency_timeline(id)
    }

    pub fn get_telemetry_summary(&self, id: u64) -> Option<crate::concurrency::TaskTelemetrySummary> {
        crate::concurrency::get_task_telemetry_summary(id)
    }
}

pub fn spawn_download_engine() -> DownloadEngineHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<EngineCommand>();
    let (evt_tx, evt_rx) = mpsc::unbounded_channel::<EngineEvent>();

    let active_tasks: Arc<Mutex<HashMap<u64, TaskControl>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let known_paths: Arc<Mutex<HashMap<u64, PathBuf>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let active_workers = Arc::new(AtomicUsize::new(0));

    let active_tasks_engine = active_tasks.clone();
    let known_paths_engine = known_paths.clone();
    let active_workers_engine = active_workers.clone();

    std::thread::spawn(move || {
        let workers = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .clamp(4, 16);

        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Failed to create tokio runtime: {}", e);
                return;
            }
        };

        rt.block_on(async move {
            let shared_client = Arc::new(
                reqwest::Client::builder()
                    .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 NotDownloadManager/1.0 (compatible; IDMan/6.42)")
                    .connect_timeout(Duration::from_secs(10))
                    .pool_max_idle_per_host(32)
                    .pool_idle_timeout(Duration::from_secs(90))
                    .tcp_keepalive(Duration::from_secs(60))
                    .tcp_nodelay(true)
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
            );

            let active_tasks = active_tasks_engine;
            let known_paths = known_paths_engine;
            let active_workers = active_workers_engine;

            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    EngineCommand::StartDownload {
                        id,
                        url,
                        save_path,
                        connections,
                    } => {
                        // Prevent concurrent tasks writing to the exact same destination file/partinfo
                        let path_busy = {
                            let paths = known_paths.lock().unwrap_or_else(|p| p.into_inner());
                            let tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                            paths.iter().any(|(other_id, other_path)| {
                                *other_id != id
                                    && other_path == &save_path
                                    && tasks.get(other_id).map_or(false, |c| !c.terminal_emitted.load(Ordering::SeqCst))
                            })
                        };

                        if path_busy {
                            let _ = evt_tx.send(EngineEvent::Failed {
                                id,
                                error: "Destination path is currently in use by another active download task".into(),
                            });
                            continue;
                        }

                        let ctrl_to_run = {
                            let mut tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                            if let Some(existing) = tasks.get(&id) {
                                if existing.delete_flag.load(Ordering::SeqCst) {
                                    // FINDING-4.2-04: Active deletion in progress. Reject StartDownload
                                    // to prevent concurrent coordinator writing and file-unlinking race.
                                    None
                                } else if !existing.terminal_emitted.load(Ordering::SeqCst) {
                                    // Already actively downloading or finalizing: reject duplicate start
                                    None
                                } else {
                                    // Prior instance has finalized: allocate new monotonic instance_id
                                    let instance_id = NEXT_INSTANCE_ID.fetch_add(1, Ordering::SeqCst);
                                    let ctrl = TaskControl::new(instance_id);
                                    tasks.insert(id, ctrl.clone());
                                    Some(ctrl)
                                }
                            } else {
                                let instance_id = NEXT_INSTANCE_ID.fetch_add(1, Ordering::SeqCst);
                                let ctrl = TaskControl::new(instance_id);
                                tasks.insert(id, ctrl.clone());
                                Some(ctrl)
                            }
                        };

                        if let Some(ctrl) = ctrl_to_run {
                            known_paths
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .insert(id, save_path.clone());
                            INACTIVE_SAVE_PATHS
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .insert(id, save_path.clone());

                            let evt_sender = evt_tx.clone();
                            let client = shared_client.clone();
                            let tasks_ref = active_tasks.clone();
                            let paths_ref = known_paths.clone();
                            let workers_ref = active_workers.clone();
                            tokio::spawn(async move {
                                run_download_task(
                                    id,
                                    url,
                                    save_path,
                                    connections,
                                    ctrl,
                                    evt_sender,
                                    client,
                                    tasks_ref,
                                    paths_ref,
                                    workers_ref,
                                )
                                .await;
                            });
                        }
                    }
                    EngineCommand::PauseDownload { id } => {
                        if let Some(ctrl) = active_tasks
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .get(&id)
                        {
                            ctrl.pause_flag.store(true, Ordering::SeqCst);
                            ctrl.notify.notify_waiters();
                        }
                    }
                    EngineCommand::CancelDownload { id } => {
                        if let Some(ctrl) = active_tasks
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .get(&id)
                        {
                            ctrl.cancel_flag.store(true, Ordering::SeqCst);
                            ctrl.notify.notify_waiters();
                        }
                    }
                    EngineCommand::DeleteDownload { id, cleanup_files } => {
                        let maybe_ctrl = active_tasks
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .get(&id)
                            .cloned();

                        if let Some(ctrl) = maybe_ctrl {
                            // Coordinator is actively coordinating this task: delegate deletion cleanup and event emission
                            ctrl.cleanup_files.store(cleanup_files, Ordering::SeqCst);
                            ctrl.delete_flag.store(true, Ordering::SeqCst);
                            ctrl.cancel_flag.store(true, Ordering::SeqCst);
                            ctrl.pause_flag.store(true, Ordering::SeqCst);
                            ctrl.notify.notify_waiters();
                        } else {
                            // Inactive task OR coordinator that has already finalized: clean up files directly if requested
                            let mut paths = known_paths.lock().unwrap_or_else(|p| p.into_inner());
                            let save_path = paths
                                .get(&id)
                                .cloned()
                                .or_else(|| {
                                    INACTIVE_SAVE_PATHS
                                        .lock()
                                        .unwrap_or_else(|p| p.into_inner())
                                        .get(&id)
                                        .cloned()
                                })
                                .or_else(|| lookup_save_path(id));

                            let mut cleanup_success = true;
                            let mut err_msg = None;

                            if cleanup_files {
                                if let Some(path) = &save_path {
                                    let mut partinfo = path.clone();
                                    partinfo.set_file_name(format!(
                                        "{}.partinfo",
                                        path.file_name().unwrap_or_default().to_string_lossy()
                                    ));
                                    let _ = tokio::fs::remove_file(&partinfo).await;
                                    let tmp_partinfo = partinfo.with_extension("tmp");
                                    let _ = tokio::fs::remove_file(&tmp_partinfo).await;
                                    let dot_tmp = PathBuf::from(format!("{}.tmp", partinfo.display()));
                                    let _ = tokio::fs::remove_file(&dot_tmp).await;

                                    if path.exists() {
                                        if let Err(e) = tokio::fs::remove_file(path).await {
                                            cleanup_success = false;
                                            err_msg = Some(format!(
                                                "Failed to delete {}: {}",
                                                path.display(),
                                                e
                                            ));
                                        }
                                    }
                                } else {
                                    // Save path unknown: cannot truthfully report cleanup success
                                    cleanup_success = false;
                                    err_msg = Some(format!(
                                        "Cannot clean up files for task #{}: save_path could not be resolved from known_paths or database",
                                        id
                                    ));
                                }
                            }

                            // FINDING-4.2-05: Remove on successful cleanup, retain on failure for retry
                            if cleanup_success {
                                paths.remove(&id);
                                INACTIVE_SAVE_PATHS
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .remove(&id);
                            } else {
                                if let Some(path) = &save_path {
                                    paths.insert(id, path.clone());
                                }
                            }

                            let _ = evt_tx.send(EngineEvent::Deleted {
                                id,
                                cleanup_success,
                                error: err_msg,
                            });
                        }
                    }
                }
            }
        });
    });

    DownloadEngineHandle {
        cmd_sender: cmd_tx,
        event_receiver: evt_rx,
        active_tasks,
        known_paths,
        active_workers,
    }
}

pub fn parse_content_range_header(header: &str) -> Result<(u64, u64, Option<u64>), String> {
    let s = header.trim();
    if !s.starts_with("bytes ") {
        return Err("Must start with 'bytes '".into());
    }
    let rest = &s[6..];
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 2 {
        return Err("Missing '/' separator".into());
    }
    let range_parts: Vec<&str> = parts[0].split('-').collect();
    if range_parts.len() != 2 {
        return Err("Missing '-' in byte range".into());
    }
    let start = range_parts[0].parse::<u64>().map_err(|e| e.to_string())?;
    let end = range_parts[1].parse::<u64>().map_err(|e| e.to_string())?;
    let total = if parts[1] == "*" {
        None
    } else {
        Some(parts[1].parse::<u64>().map_err(|e| e.to_string())?)
    };
    Ok((start, end, total))
}

pub fn validate_http_206(
    resp: &reqwest::Response,
    expected_start: u64,
    expected_end: u64,
    expected_total: Option<u64>,
) -> Result<(), EngineError> {
    if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(EngineError::Fatal(format!(
            "Server returned HTTP {} instead of 206 Partial Content",
            resp.status()
        )));
    }

    let cr_header = match resp.headers().get(reqwest::header::CONTENT_RANGE) {
        Some(v) => match v.to_str() {
            Ok(s) => s,
            Err(_) => {
                return Err(EngineError::Fatal(
                    "Invalid Content-Range header encoding".into(),
                ))
            }
        },
        None => {
            return Err(EngineError::Fatal(
                "Missing Content-Range header in HTTP 206 response".into(),
            ))
        }
    };

    let (s_start, s_end, s_total) = parse_content_range_header(cr_header)
        .map_err(|e| EngineError::Fatal(format!("Malformed Content-Range header '{}': {}", cr_header, e)))?;

    if s_start != expected_start || s_end != expected_end.saturating_sub(1) {
        return Err(EngineError::Fatal(format!(
            "Content-Range mismatch: expected bytes {}-{}, server returned {}-{}",
            expected_start,
            expected_end.saturating_sub(1),
            s_start,
            s_end
        )));
    }

    if let Some(total) = expected_total {
        if let Some(st) = s_total {
            if st != total {
                return Err(EngineError::Fatal(format!(
                    "Server total size changed: expected {}, server returned {}",
                    total, st
                )));
            }
        }
    }

    if let Some(cl) = resp.content_length() {
        let expected_len = expected_end - expected_start;
        if cl != expected_len {
            return Err(EngineError::Fatal(format!(
                "Content-Length mismatch: expected {} bytes, server returned {} bytes",
                expected_len, cl
            )));
        }
    }

    Ok(())
}

/// RFC 7231 / RFC 9110 Retry-After header parser supporting:
/// 1. Delta-seconds (decimal integer, e.g. "120")
/// 2. IMF-fixdate / RFC 2822 HTTP-date (e.g. "Wed, 21 Oct 2026 07:28:00 GMT")
/// 3. RFC 850 / asctime legacy date formats
pub fn parse_retry_after_header(header_val: &str, now: std::time::SystemTime) -> Option<Duration> {
    let trimmed = header_val.trim();
    if trimmed.is_empty() {
        return None;
    }

    // 1. Delta-seconds
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    // 2. RFC 2822 / RFC 7231 IMF-fixdate
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(trimmed) {
        let sys_time: std::time::SystemTime = dt.into();
        return match sys_time.duration_since(now) {
            Ok(dur) => Some(dur),
            Err(_) => Some(Duration::ZERO), // Past date means retry immediately
        };
    }

    // 3. RFC 850 format: "Sunday, 06-Nov-94 08:49:37 GMT"
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%A, %d-%b-%y %H:%M:%S GMT") {
        let sys_time: std::time::SystemTime = dt.and_utc().into();
        return match sys_time.duration_since(now) {
            Ok(dur) => Some(dur),
            Err(_) => Some(Duration::ZERO),
        };
    }

    // 4. asctime format: "Sun Nov  6 08:49:37 1994"
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%a %b %e %H:%M:%S %Y") {
        let sys_time: std::time::SystemTime = dt.and_utc().into();
        return match sys_time.duration_since(now) {
            Ok(dur) => Some(dur),
            Err(_) => Some(Duration::ZERO),
        };
    }

    None
}

/// Generates a fast pseudo-random float in [0.0, 1.0) without external dependencies
pub fn pseudo_random_jitter() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0x4d595df4d0f33173);
    let mut s = SEED.load(Ordering::Relaxed);
    if s == 0 {
        s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x123456789abcdef0);
    }
    s ^= s >> 12;
    s ^= s << 25;
    s ^= s >> 27;
    let val = s.wrapping_mul(0x2545F4914F6CDD1D);
    SEED.store(val, Ordering::Relaxed);
    (val >> 11) as f64 / (1u64 << 53) as f64
}

/// Computes backoff duration with bounded exponential backoff and jitter
pub fn compute_throttling_backoff(
    retry_after: Option<Duration>,
    consecutive_retries: u32,
    base_delay: Duration,
    max_delay: Duration,
) -> Duration {
    if let Some(ra) = retry_after {
        if ra.is_zero() {
            // Immediate retry requested by server: minimal desynchronization delay
            let jitter_ms = (pseudo_random_jitter() * 20.0) as u64;
            Duration::from_millis(jitter_ms).min(max_delay)
        } else {
            // Obey Retry-After with a small 0..200ms jitter to desynchronize concurrent workers
            let jitter_ms = (pseudo_random_jitter() * 200.0) as u64;
            (ra + Duration::from_millis(jitter_ms)).min(max_delay)
        }
    } else {
        // Bounded exponential backoff with full jitter:
        // delay = uniform_random(base_delay, min(max_delay, base_delay * 2^retries))
        let factor = 1u64.checked_shl(consecutive_retries.min(6)).unwrap_or(64);
        let exp_limit = (base_delay * factor as u32).min(max_delay);
        let min_ms = base_delay.as_millis() as f64;
        let max_ms = exp_limit.as_millis() as f64;
        let jitter = pseudo_random_jitter();
        let chosen_ms = min_ms + jitter * (max_ms - min_ms);
        Duration::from_millis(chosen_ms as u64).min(max_delay)
    }
}

async fn write_checkpoint_atomic(path: &Path, cp: &DownloadCheckpoint) {
    let tmp_path = path.with_extension("tmp");
    if let Ok(json) = serde_json::to_string(cp) {
        if tokio::fs::write(&tmp_path, json).await.is_ok() {
            let _ = tokio::fs::rename(&tmp_path, path).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestHookPoint {
    BeforeFinalization,
    AfterFinalizationStart,
    DuringTerminalEventPrep,
    BeforeTerminalEmit,
    DuringWorkerBackoff,
}

static TEST_HOOKS: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<(u64, TestHookPoint), Arc<tokio::sync::Notify>>,
    >,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

static TEST_BARRIERS: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<(u64, TestHookPoint), Arc<tokio::sync::Barrier>>,
    >,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

pub fn register_test_hook(task_id: u64, point: TestHookPoint, notify: Arc<tokio::sync::Notify>) {
    TEST_HOOKS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert((task_id, point), notify);
}

pub fn register_test_barrier(
    task_id: u64,
    point: TestHookPoint,
    barrier: Arc<tokio::sync::Barrier>,
) {
    TEST_BARRIERS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert((task_id, point), barrier);
}

pub fn clear_test_hooks(task_id: u64) {
    TEST_HOOKS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|(id, _), _| *id != task_id);
    TEST_BARRIERS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|(id, _), _| *id != task_id);
}

async fn trigger_test_hook(task_id: u64, point: TestHookPoint) {
    let maybe_notify = {
        TEST_HOOKS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(task_id, point))
            .cloned()
    };
    if let Some(notify) = maybe_notify {
        notify.notify_waiters();
    }
    let maybe_barrier = {
        TEST_BARRIERS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(task_id, point))
            .cloned()
    };
    if let Some(barrier) = maybe_barrier {
        barrier.wait().await;
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FinalizeIntent {
    Finished { hash_status: HashVerificationStatus },
    Paused,
    Canceled,
    Deleted,
    Failed(String),
}

async fn execute_deletion_cleanup_and_emit(
    id: u64,
    ctrl: &TaskControl,
    save_path: &Path,
    partinfo_path: &Path,
    evt_sender: &mpsc::UnboundedSender<EngineEvent>,
    known_paths: &Arc<Mutex<HashMap<u64, PathBuf>>>,
    active_tasks: &Arc<Mutex<HashMap<u64, TaskControl>>>,
) {
    if !ctrl.mark_terminal_emitted() {
        return;
    }

    EXPECTED_HASHES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&id);
    clear_task_writer_metrics(id);

    let mut cleanup_success = true;
    let mut err_msg = None;

    // Suppress and remove checkpoint files unconditionally on Delete
    let _ = tokio::fs::remove_file(partinfo_path).await;
    let tmp_partinfo = partinfo_path.with_extension("tmp");
    let _ = tokio::fs::remove_file(&tmp_partinfo).await;
    let dot_tmp = PathBuf::from(format!("{}.tmp", partinfo_path.display()));
    let _ = tokio::fs::remove_file(&dot_tmp).await;

    if ctrl.cleanup_files.load(Ordering::SeqCst) {
        if save_path.exists() {
            if let Err(e) = tokio::fs::remove_file(save_path).await {
                cleanup_success = false;
                err_msg = Some(format!("Failed to delete {}: {}", save_path.display(), e));
            }
        }
    }

    // FINDING-4.2-05: Remove on successful cleanup, retain on failure for retry
    if cleanup_success {
        known_paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
        INACTIVE_SAVE_PATHS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
    } else {
        known_paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, save_path.to_path_buf());
    }

    // Atomically release coordinator ownership in active_tasks before sending Deleted
    {
        let mut tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.remove(&id);
    }

    let _ = evt_sender.send(EngineEvent::Deleted {
        id,
        cleanup_success,
        error: err_msg,
    });
}

async fn finalize_coordinator(
    id: u64,
    intent: FinalizeIntent,
    ctrl: &TaskControl,
    save_path: &Path,
    partinfo_path: &Path,
    manager_arc: Option<&Arc<Mutex<SegmentManager>>>,
    evt_sender: &mpsc::UnboundedSender<EngineEvent>,
    known_paths: &Arc<Mutex<HashMap<u64, PathBuf>>>,
    active_tasks: &Arc<Mutex<HashMap<u64, TaskControl>>>,
    total_size: Option<u64>,
    last_downloaded: u64,
    conn_count: usize,
) {
    trigger_test_hook(id, TestHookPoint::AfterFinalizationStart).await;

    // 1. Delete strictly preempts all other intents once requested
    if ctrl.delete_flag.load(Ordering::SeqCst) || matches!(intent, FinalizeIntent::Deleted) {
        execute_deletion_cleanup_and_emit(
            id,
            ctrl,
            save_path,
            partinfo_path,
            evt_sender,
            known_paths,
            active_tasks,
        )
        .await;
        return;
    }

    let effective_intent = if ctrl.cancel_flag.load(Ordering::SeqCst) {
        FinalizeIntent::Canceled
    } else if ctrl.pause_flag.load(Ordering::SeqCst) {
        FinalizeIntent::Paused
    } else {
        intent
    };

    // 2. Deterministic terminal execution
    match effective_intent {
        FinalizeIntent::Deleted => {
            execute_deletion_cleanup_and_emit(
                id,
                ctrl,
                save_path,
                partinfo_path,
                evt_sender,
                known_paths,
                active_tasks,
            )
            .await;
        }
        FinalizeIntent::Paused => {
            if ctrl.delete_flag.load(Ordering::SeqCst) {
                execute_deletion_cleanup_and_emit(
                    id,
                    ctrl,
                    save_path,
                    partinfo_path,
                    evt_sender,
                    known_paths,
                    active_tasks,
                )
                .await;
                return;
            }

            if let Some(mgr) = manager_arc {
                let cp = {
                    mgr.lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .to_checkpoint()
                };
                write_checkpoint_atomic(partinfo_path, &cp).await;
            }

            trigger_test_hook(id, TestHookPoint::BeforeTerminalEmit).await;

            {
                let mut tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                if ctrl.delete_flag.load(Ordering::SeqCst) {
                    drop(tasks);
                    execute_deletion_cleanup_and_emit(
                        id,
                        ctrl,
                        save_path,
                        partinfo_path,
                        evt_sender,
                        known_paths,
                        active_tasks,
                    )
                    .await;
                    return;
                }
                if !ctrl.mark_terminal_emitted() {
                    return;
                }
                tasks.remove(&id);
            }

            EXPECTED_HASHES
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            known_paths
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            let _ = evt_sender.send(EngineEvent::Paused { id });
        }
        FinalizeIntent::Canceled => {
            if ctrl.delete_flag.load(Ordering::SeqCst) {
                execute_deletion_cleanup_and_emit(
                    id,
                    ctrl,
                    save_path,
                    partinfo_path,
                    evt_sender,
                    known_paths,
                    active_tasks,
                )
                .await;
                return;
            }

            if let Some(mgr) = manager_arc {
                let cp = {
                    mgr.lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .to_checkpoint()
                };
                write_checkpoint_atomic(partinfo_path, &cp).await;
            }

            trigger_test_hook(id, TestHookPoint::BeforeTerminalEmit).await;

            {
                let mut tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                if ctrl.delete_flag.load(Ordering::SeqCst) {
                    drop(tasks);
                    execute_deletion_cleanup_and_emit(
                        id,
                        ctrl,
                        save_path,
                        partinfo_path,
                        evt_sender,
                        known_paths,
                        active_tasks,
                    )
                    .await;
                    return;
                }
                if !ctrl.mark_terminal_emitted() {
                    return;
                }
                tasks.remove(&id);
            }

            EXPECTED_HASHES
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            known_paths
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            let _ = evt_sender.send(EngineEvent::Canceled { id });
        }
        FinalizeIntent::Finished { hash_status } => {
            trigger_test_hook(id, TestHookPoint::DuringTerminalEventPrep).await;

            if ctrl.delete_flag.load(Ordering::SeqCst) {
                execute_deletion_cleanup_and_emit(
                    id,
                    ctrl,
                    save_path,
                    partinfo_path,
                    evt_sender,
                    known_paths,
                    active_tasks,
                )
                .await;
                return;
            }

            let _ = tokio::fs::remove_file(partinfo_path).await;
            let tmp_partinfo = partinfo_path.with_extension("tmp");
            let _ = tokio::fs::remove_file(&tmp_partinfo).await;

            let final_downloaded = total_size.unwrap_or(last_downloaded);
            let final_ui_segs = if let Some(mgr) = manager_arc {
                let m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                m.generate_ui_segments(conn_count, &[])
            } else {
                Vec::new()
            };

            trigger_test_hook(id, TestHookPoint::BeforeTerminalEmit).await;

            {
                let mut tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                if ctrl.delete_flag.load(Ordering::SeqCst) {
                    drop(tasks);
                    execute_deletion_cleanup_and_emit(
                        id,
                        ctrl,
                        save_path,
                        partinfo_path,
                        evt_sender,
                        known_paths,
                        active_tasks,
                    )
                    .await;
                    return;
                }
                if !ctrl.mark_terminal_emitted() {
                    return;
                }
                tasks.remove(&id);
            }

            let final_conn_segs: Vec<SegmentProgress> = (0..conn_count.max(1))
                .map(|i| SegmentProgress {
                    segment_index: i,
                    start_byte: 0,
                    end_byte: 100,
                    current_byte: 100,
                    speed: 0.0,
                })
                .collect();

            let _ = evt_sender.send(EngineEvent::ProgressUpdate {
                id,
                downloaded: final_downloaded,
                speed: 0.0,
                time_left: Some(0),
                segments: final_ui_segs,
                connection_segments: final_conn_segs,
            });

            EXPECTED_HASHES
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            known_paths
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            let _ = evt_sender.send(EngineEvent::Finished { id, hash_status });
        }
        FinalizeIntent::Failed(error) => {
            if ctrl.delete_flag.load(Ordering::SeqCst) {
                execute_deletion_cleanup_and_emit(
                    id,
                    ctrl,
                    save_path,
                    partinfo_path,
                    evt_sender,
                    known_paths,
                    active_tasks,
                )
                .await;
                return;
            }

            trigger_test_hook(id, TestHookPoint::BeforeTerminalEmit).await;

            {
                let mut tasks = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
                if ctrl.delete_flag.load(Ordering::SeqCst) {
                    drop(tasks);
                    execute_deletion_cleanup_and_emit(
                        id,
                        ctrl,
                        save_path,
                        partinfo_path,
                        evt_sender,
                        known_paths,
                        active_tasks,
                    )
                    .await;
                    return;
                }
                if !ctrl.mark_terminal_emitted() {
                    return;
                }
                tasks.remove(&id);
            }

            EXPECTED_HASHES
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            known_paths
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            let _ = evt_sender.send(EngineEvent::Failed { id, error });
        }
    }
}

const CHUNK_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn spawn_worker_task(
    lifecycle_guard: crate::concurrency::WorkerLifecycleGuard,
    mgr: Arc<Mutex<SegmentManager>>,
    client: Arc<reqwest::Client>,
    url: String,
    write_tx: mpsc::Sender<WriteCommand>,
    in_flight: Arc<AtomicUsize>,
    cancel_flag: Arc<AtomicBool>,
    delete_flag: Arc<AtomicBool>,
    pause_flag: Arc<AtomicBool>,
    error_flag: Arc<AtomicBool>,
    fatal_err: Arc<Mutex<Option<String>>>,
    tracker: Arc<AtomicU64>,
    chunk_bounds: Arc<(AtomicU64, AtomicU64)>,
    worker_total_bytes: Arc<AtomicU64>,
    total: Option<u64>,
    active_workers: Arc<AtomicUsize>,
    notify: Arc<tokio::sync::Notify>,
    target_concurrency: Arc<AtomicUsize>,
    _task_active_workers: Arc<AtomicUsize>,
    telemetry: Arc<crate::concurrency::TaskTelemetryCollector>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _worker_guard = WorkerGuard::new(&active_workers);
        let lifecycle_guard = lifecycle_guard;
        let worker_id = lifecycle_guard.slot_id;

        while !cancel_flag.load(Ordering::SeqCst)
            && !pause_flag.load(Ordering::SeqCst)
            && !delete_flag.load(Ordering::SeqCst)
            && !error_flag.load(Ordering::Relaxed)
        {
            // Graceful scale-down: if live active workers exceed target concurrency, retire safely before leasing!
            let target = target_concurrency.load(Ordering::SeqCst);
            if lifecycle_guard.try_retire(target) {
                break;
            }

            // Pull next work chunk from SegmentManager
            telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Idle);
            let idle_start = std::time::Instant::now();
            let lease = {
                let mut m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                m.lease_next_chunk(worker_id)
            };
            telemetry.record_idle(worker_id, idle_start.elapsed());

            let lease = match lease {
                Some(l) => l,
                None => {
                    break; // No more pending chunks; lifecycle_guard drops and cleans up
                }
            };

            chunk_bounds.0.store(lease.start, Ordering::Relaxed);
            chunk_bounds.1.store(lease.end, Ordering::Relaxed);
            tracker.store(lease.start, Ordering::Relaxed);

            // Execute chunk download with exponential backoff and HTTP validation
            let mut retries = 0;
            let mut chunk_succeeded = false;

            while retries <= MAX_CHUNK_RETRIES
                && !cancel_flag.load(Ordering::SeqCst)
                && !pause_flag.load(Ordering::SeqCst)
                && !delete_flag.load(Ordering::SeqCst)
            {
                let range_hdr = format!("bytes={}-{}", lease.start, lease.end - 1);
                let req_start = std::time::Instant::now();
                telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Requesting);
                let send_fut = client.get(&url).header(reqwest::header::RANGE, range_hdr).send();
                tokio::pin!(send_fut);
                let resp = loop {
                    if cancel_flag.load(Ordering::SeqCst)
                        || pause_flag.load(Ordering::SeqCst)
                        || delete_flag.load(Ordering::SeqCst)
                        || error_flag.load(Ordering::Relaxed)
                    {
                        return;
                    }

                    tokio::select! {
                        biased;
                        _ = notify.notified() => {}
                        res = &mut send_fut => {
                            break res;
                        }
                    }
                };

                let resp = match resp {
                    Ok(r) => {
                        let setup_dur = req_start.elapsed();
                        telemetry.record_request_setup(worker_id, setup_dur);
                        telemetry.record_rtt(worker_id, setup_dur);
                        r
                    }
                    Err(_e) => {
                        telemetry.record_retry_or_error(worker_id);
                        retries += 1;
                        let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                        {
                            let _guard = crate::concurrency::BackoffGuard::new(telemetry.clone(), worker_id, delay);
                            tokio::select! {
                                biased;
                                _ = notify.notified() => {
                                    if cancel_flag.load(Ordering::SeqCst)
                                        || pause_flag.load(Ordering::SeqCst)
                                        || delete_flag.load(Ordering::SeqCst)
                                        || error_flag.load(Ordering::Relaxed)
                                    {
                                        return;
                                    }
                                }
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                        continue;
                    }
                };

                let status = resp.status();

                // Handle transient throttling (429, 502, 503, 504)
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    telemetry.record_http_429(worker_id);
                } else if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    telemetry.record_http_503(worker_id);
                }

                if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                    || status == reqwest::StatusCode::BAD_GATEWAY
                    || status == reqwest::StatusCode::GATEWAY_TIMEOUT
                {
                    retries += 1;
                    if retries > MAX_CHUNK_RETRIES * 3 {
                        *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) =
                            Some(format!("Server throttle retries exceeded on HTTP {}", status));
                        error_flag.store(true, Ordering::SeqCst);
                        notify.notify_waiters();
                        let mut m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                        let _ = m.return_failed_chunk(lease.chunk_id, lease.generation, true);
                        return;
                    }

                    let retry_after_dur = resp
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| parse_retry_after_header(s, std::time::SystemTime::now()));

                    let backoff = compute_throttling_backoff(
                        retry_after_dur,
                        retries,
                        Duration::from_millis(250),
                        Duration::from_secs(30),
                    );

                    {
                        let _guard = crate::concurrency::BackoffGuard::new(telemetry.clone(), worker_id, backoff);
                        let notified = notify.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();

                        if cancel_flag.load(Ordering::SeqCst)
                            || pause_flag.load(Ordering::SeqCst)
                            || delete_flag.load(Ordering::SeqCst)
                            || error_flag.load(Ordering::Relaxed)
                        {
                            return;
                        }

                        trigger_test_hook(telemetry.task_id, TestHookPoint::DuringWorkerBackoff).await;
                        tokio::select! {
                            biased;
                            _ = &mut notified => {
                                if cancel_flag.load(Ordering::SeqCst)
                                    || pause_flag.load(Ordering::SeqCst)
                                    || delete_flag.load(Ordering::SeqCst)
                                    || error_flag.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                            }
                            _ = tokio::time::sleep(backoff) => {}
                        }
                    }
                    continue;
                }

                // Validate HTTP 206 and Content-Range strictly
                if let Err(e) = validate_http_206(&resp, lease.start, lease.end, total) {
                    match e {
                        EngineError::Fatal(msg) => {
                            *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) = Some(msg);
                            error_flag.store(true, Ordering::SeqCst);
                            notify.notify_waiters();
                            let mut m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                            let _ = m.return_failed_chunk(lease.chunk_id, lease.generation, true);
                            return;
                        }
                        EngineError::Transient { .. } => {
                            telemetry.record_retry_or_error(worker_id);
                            retries += 1;
                            let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                            {
                                let _guard = crate::concurrency::BackoffGuard::new(telemetry.clone(), worker_id, delay);
                                tokio::select! {
                                    biased;
                                    _ = notify.notified() => {
                                        if cancel_flag.load(Ordering::SeqCst)
                                            || pause_flag.load(Ordering::SeqCst)
                                            || delete_flag.load(Ordering::SeqCst)
                                            || error_flag.load(Ordering::Relaxed)
                                        {
                                            return;
                                        }
                                    }
                                    _ = tokio::time::sleep(delay) => {}
                                }
                            }
                            continue;
                        }
                    }
                }

                // Stream chunk bytes with interruptible cancellation and inactivity timeout
                let mut resp = resp;
                let mut curr = lease.start;
                let mut stream_err = false;
                telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Downloading);

                loop {
                    if cancel_flag.load(Ordering::SeqCst)
                        || pause_flag.load(Ordering::SeqCst)
                        || delete_flag.load(Ordering::SeqCst)
                        || error_flag.load(Ordering::Relaxed)
                    {
                        return;
                    }

                    let net_start = std::time::Instant::now();
                    let chunk_res = tokio::select! {
                        biased;
                        _ = notify.notified() => {
                            if cancel_flag.load(Ordering::SeqCst)
                                || pause_flag.load(Ordering::SeqCst)
                                || delete_flag.load(Ordering::SeqCst)
                                || error_flag.load(Ordering::Relaxed)
                            {
                                return;
                            }
                            continue;
                        }
                        res = tokio::time::timeout(CHUNK_READ_TIMEOUT, resp.chunk()) => {
                            match res {
                                Ok(Ok(Some(bytes))) => Some(Ok(bytes)),
                                Ok(Ok(None)) => None,
                                Ok(Err(e)) => Some(Err(e)),
                                Err(_) => {
                                    stream_err = true;
                                    break;
                                }
                            }
                        }
                    };
                    telemetry.record_network_wait(worker_id, net_start.elapsed());

                    let chunk_bytes = match chunk_res {
                        Some(Ok(b)) => b,
                        Some(Err(_)) => {
                            stream_err = true;
                            break;
                        }
                        None => break, // EOF reached for chunk
                    };

                    let len = chunk_bytes.len() as u64;
                    telemetry.record_bytes(worker_id, len);
                    worker_total_bytes.fetch_add(len, Ordering::Relaxed);

                    let bp_start = std::time::Instant::now();
                    telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Writing);
                    in_flight.fetch_add(1, Ordering::Relaxed);
                    if write_tx
                        .send(WriteCommand::WriteBytes {
                            offset: curr,
                            data: chunk_bytes,
                        })
                        .await
                        .is_err()
                    {
                        in_flight.fetch_sub(1, Ordering::Relaxed);
                        error_flag.store(true, Ordering::SeqCst);
                        notify.notify_waiters();
                        return;
                    }
                    telemetry.record_writer_backpressure(worker_id, bp_start.elapsed());
                    telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Downloading);

                    curr += len;
                    tracker.store(curr, Ordering::Relaxed);
                }

                if stream_err || curr < lease.end {
                    telemetry.record_retry_or_error(worker_id);
                    retries += 1;
                    let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                    {
                        let _guard = crate::concurrency::BackoffGuard::new(telemetry.clone(), worker_id, delay);
                        tokio::select! {
                            biased;
                            _ = notify.notified() => {
                                if cancel_flag.load(Ordering::SeqCst)
                                    || pause_flag.load(Ordering::SeqCst)
                                    || delete_flag.load(Ordering::SeqCst)
                                    || error_flag.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                            }
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                    continue;
                }

                // Chunk completely streamed! Send commit command to Writer Actor
                telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Writing);
                let bp_start = std::time::Instant::now();
                in_flight.fetch_add(1, Ordering::Relaxed);
                if write_tx
                    .send(WriteCommand::CommitChunk {
                        chunk_id: lease.chunk_id,
                        generation: lease.generation,
                    })
                    .await
                    .is_err()
                {
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                    error_flag.store(true, Ordering::SeqCst);
                    notify.notify_waiters();
                    return;
                }
                telemetry.record_writer_backpressure(worker_id, bp_start.elapsed());
                telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Idle);

                chunk_succeeded = true;
                break;
            }

            if !chunk_succeeded
                && !cancel_flag.load(Ordering::SeqCst)
                && !pause_flag.load(Ordering::SeqCst)
                && !delete_flag.load(Ordering::SeqCst)
            {
                let mut m = mgr.lock().unwrap_or_else(|p| p.into_inner());
                if let Err(e) = m.return_failed_chunk(lease.chunk_id, lease.generation, false) {
                    *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) = Some(e);
                    error_flag.store(true, Ordering::SeqCst);
                    notify.notify_waiters();
                    return;
                }
            }
        }
        telemetry.set_worker_state(worker_id, crate::concurrency::WorkerState::Terminated);
    })
}

async fn run_download_task(
    id: u64,
    url: String,
    save_path: PathBuf,
    connections: usize,
    ctrl: TaskControl,
    evt_sender: mpsc::UnboundedSender<EngineEvent>,
    client: Arc<reqwest::Client>,
    active_tasks: Arc<Mutex<HashMap<u64, TaskControl>>>,
    known_paths: Arc<Mutex<HashMap<u64, PathBuf>>>,
    active_workers: Arc<AtomicUsize>,
) {
    let _guard = ActiveTaskGuard {
        id,
        instance_id: ctrl.instance_id,
        active_tasks: active_tasks.clone(),
    };
    let cancel_flag = ctrl.cancel_flag.clone();
    let delete_flag = ctrl.delete_flag.clone();
    let pause_flag = ctrl.pause_flag.clone();

    if delete_flag.load(Ordering::SeqCst)
        || cancel_flag.load(Ordering::SeqCst)
        || pause_flag.load(Ordering::SeqCst)
    {
        let mut partinfo = save_path.clone();
        partinfo.set_file_name(format!(
            "{}.partinfo",
            save_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("download")
        ));
        let initial_intent = if delete_flag.load(Ordering::SeqCst) {
            FinalizeIntent::Deleted
        } else if cancel_flag.load(Ordering::SeqCst) {
            FinalizeIntent::Canceled
        } else {
            FinalizeIntent::Paused
        };
        finalize_coordinator(
            id,
            initial_intent,
            &ctrl,
            &save_path,
            &partinfo,
            None,
            &evt_sender,
            &known_paths,
            &active_tasks,
            None,
            0,
            1,
        )
        .await;
        return;
    }

    // 1. Probe HEAD for content length, resume capability, and ETag with interruptible cancellation
    let head_start = std::time::Instant::now();
    let head_fut = client.head(&url).send();
    tokio::pin!(head_fut);
    let head_resp = loop {
        if delete_flag.load(Ordering::SeqCst)
            || cancel_flag.load(Ordering::SeqCst)
            || pause_flag.load(Ordering::SeqCst)
        {
            let mut partinfo = save_path.clone();
            partinfo.set_file_name(format!(
                "{}.partinfo",
                save_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("download")
            ));
            let initial_intent = if delete_flag.load(Ordering::SeqCst) {
                FinalizeIntent::Deleted
            } else if cancel_flag.load(Ordering::SeqCst) {
                FinalizeIntent::Canceled
            } else {
                FinalizeIntent::Paused
            };
            finalize_coordinator(
                id,
                initial_intent,
                &ctrl,
                &save_path,
                &partinfo,
                None,
                &evt_sender,
                &known_paths,
                &active_tasks,
                None,
                0,
                1,
            )
            .await;
            return;
        }

        tokio::select! {
            biased;
            _ = ctrl.notify.notified() => {}
            res = &mut head_fut => {
                break res;
            }
        }
    };
    let head_duration = head_start.elapsed();
    let (mut total_size, mut resume_capable, mut discovered_name, mut server_etag) =
        (None, false, None, None);

    match head_resp {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Some(cl) = resp.headers().get(reqwest::header::CONTENT_LENGTH) {
                    if let Ok(cl_str) = cl.to_str() {
                        total_size = cl_str.parse::<u64>().ok();
                    }
                }
                if let Some(ar) = resp.headers().get(reqwest::header::ACCEPT_RANGES) {
                    if let Ok(ar_str) = ar.to_str() {
                        resume_capable = ar_str.eq_ignore_ascii_case("bytes");
                    }
                }
                if let Some(et) = resp.headers().get(reqwest::header::ETAG) {
                    if let Ok(et_str) = et.to_str() {
                        server_etag = Some(et_str.to_string());
                    }
                }
            }
        }
        Err(ref e) => {
            eprintln!("[DEBUG HEAD ERROR] {:?}", e);
        }
    }

    // Determine filename
    if discovered_name.is_none() {
        if let Ok(parsed_url) = reqwest::Url::parse(&url) {
            if let Some(segments) = parsed_url.path_segments() {
                if let Some(last) = segments.last() {
                    if !last.is_empty() {
                        discovered_name = Some(last.to_string());
                    }
                }
            }
        }
    }
    let filename = discovered_name.unwrap_or_else(|| format!("download_{}", id));

    let conn_count = if resume_capable && total_size.unwrap_or(0) >= 524_288 {
        connections.clamp(1, 32)
    } else {
        1
    };

    let mut partinfo_path = save_path.clone();
    partinfo_path.set_file_name(format!(
        "{}.partinfo",
        save_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("download")
    ));

    // 2. Initialize SegmentManager from checkpoint or clean start
    let manager = if let Some(total) = total_size {
        let mut loaded_mgr = None;
        if resume_capable && save_path.exists() && partinfo_path.exists() {
            if let Ok(data) = tokio::fs::read_to_string(&partinfo_path).await {
                if let Ok(cp) = serde_json::from_str::<DownloadCheckpoint>(&data) {
                    if let Ok(mgr) = SegmentManager::from_checkpoint(total, server_etag.clone(), &cp) {
                        loaded_mgr = Some(mgr);
                    }
                }
            }
        }
        loaded_mgr.unwrap_or_else(|| {
            SegmentManager::new(total, get_configured_chunk_size(), server_etag.clone())
        })
    } else {
        SegmentManager::new(0, get_configured_chunk_size(), server_etag.clone())
    };

    let is_resumed = manager.committed_bytes > 0;

    // Send metadata discovered
    let initial_ui_segs = manager.generate_ui_segments(conn_count, &[]);
    let _ = evt_sender.send(EngineEvent::MetadataDiscovered {
        id,
        filename: filename.clone(),
        total_size,
        resume_capable,
        segments: initial_ui_segs,
    });

    if delete_flag.load(Ordering::SeqCst)
        || cancel_flag.load(Ordering::SeqCst)
        || pause_flag.load(Ordering::SeqCst)
    {
        let initial_intent = if delete_flag.load(Ordering::SeqCst) {
            FinalizeIntent::Deleted
        } else if cancel_flag.load(Ordering::SeqCst) {
            FinalizeIntent::Canceled
        } else {
            FinalizeIntent::Paused
        };
        finalize_coordinator(
            id,
            initial_intent,
            &ctrl,
            &save_path,
            &partinfo_path,
            Some(&Arc::new(Mutex::new(manager))),
            &evt_sender,
            &known_paths,
            &active_tasks,
            total_size,
            0,
            conn_count,
        )
        .await;
        return;
    }

    // 3. Storage preallocation & setup
    if let Some(parent) = save_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let truncate_file = !is_resumed;
    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate_file)
        .open(&save_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            finalize_coordinator(
                id,
                FinalizeIntent::Failed(format!("Failed to open file: {}", e)),
                &ctrl,
                &save_path,
                &partinfo_path,
                None,
                &evt_sender,
                &known_paths,
                &active_tasks,
                total_size,
                0,
                conn_count,
            )
            .await;
            return;
        }
    };

    if !is_resumed {
        if let Some(total) = total_size {
            if let Err(e) = file.set_len(total).await {
                finalize_coordinator(
                    id,
                    FinalizeIntent::Failed(format!("Failed to preallocate file: {}", e)),
                    &ctrl,
                    &save_path,
                    &partinfo_path,
                    None,
                    &evt_sender,
                    &known_paths,
                    &active_tasks,
                    total_size,
                    0,
                    conn_count,
                )
                .await;
                return;
            }
        }
    }

    // 4. Writer Actor with explicit ACK protocol & Phase 5.2.1 Sequential & Coalescing Optimizations
    let (write_tx, mut write_rx) = mpsc::channel::<WriteCommand>(256);
    let (ack_tx, mut ack_rx) = mpsc::channel::<CommitAck>(256);
    let in_flight_write_commands = Arc::new(AtomicUsize::new(0));
    let in_flight_writer = in_flight_write_commands.clone();
    let write_tx_monitor = write_tx.downgrade();
    let write_tx_monitor_coordinator = write_tx.downgrade();

    let writer_handle = tokio::spawn(async move {
        let actor_start = std::time::Instant::now();
        let mut file_cursor: Option<u64> = None;
        let mut total_cmds = 0u64;
        let mut write_cmds = 0u64;
        let mut commit_cmds = 0u64;
        let mut bytes_written = 0u64;
        let mut seek_ops = 0u64;
        let mut seeks_avoided = 0u64;
        let mut adjacent_writes = 0u64;
        let mut non_adjacent_writes = 0u64;
        let mut coalesced_writes = 0u64;
        let mut coalesced_bytes = 0u64;

        // R1 separated queue tracking
        let mut max_channel_queue = 0usize;
        let mut sum_channel_queue = 0u64;
        let mut max_in_flight = 0usize;
        let mut sum_in_flight = 0u64;
        let mut queue_samples = 0u64;

        let mut total_seek_us = 0u64;
        let mut total_write_us = 0u64;
        let mut total_ack_us = 0u64;
        let mut total_recv_wait_us = 0u64;
        let mut total_active_us = 0u64;

        loop {
            let t_recv_start = std::time::Instant::now();
            let cmd = write_rx.recv().await;
            total_recv_wait_us += t_recv_start.elapsed().as_micros() as u64;

            let cmd = match cmd {
                Some(c) => c,
                None => break,
            };

            // Sample queue depths accurately upon dequeue
            let chan_depth = write_tx_monitor
                .upgrade()
                .map(|s| 256usize.saturating_sub(s.capacity()))
                .unwrap_or(0);
            let in_flight = in_flight_writer.fetch_sub(1, Ordering::Relaxed);
            if chan_depth > max_channel_queue {
                max_channel_queue = chan_depth;
            }
            if in_flight > max_in_flight {
                max_in_flight = in_flight;
            }
            sum_channel_queue += chan_depth as u64;
            sum_in_flight += in_flight as u64;
            queue_samples += 1;
            total_cmds += 1;

            let t_cmd_start = std::time::Instant::now();
            match cmd {
                WriteCommand::WriteBytes { offset, data } => {
                    write_cmds += 1;
                    let coalesce_thresh = get_coalesce_threshold_bytes();

                    // R3: Opportunistic chunk coalescing if threshold configured and multiple contiguous chunks queued
                    if coalesce_thresh > 0 && data.len() < coalesce_thresh {
                        let mut coalesced_buf = Vec::with_capacity(coalesce_thresh.min(512 * 1024));
                        coalesced_buf.extend_from_slice(&data);
                        let mut next_expected_offset = offset + data.len() as u64;
                        let mut staged_commits = Vec::new();
                        let mut non_adjacent_next = None;

                        while coalesced_buf.len() < coalesce_thresh {
                            match write_rx.try_recv() {
                                Ok(WriteCommand::WriteBytes { offset: n_off, data: n_data }) => {
                                    let _ = in_flight_writer.fetch_sub(1, Ordering::Relaxed);
                                    let cd = write_tx_monitor
                                        .upgrade()
                                        .map(|s| 256usize.saturating_sub(s.capacity()))
                                        .unwrap_or(0);
                                    if cd > max_channel_queue { max_channel_queue = cd; }
                                    sum_channel_queue += cd as u64;
                                    queue_samples += 1;
                                    total_cmds += 1;

                                    if n_off == next_expected_offset {
                                        coalesced_buf.extend_from_slice(&n_data);
                                        next_expected_offset += n_data.len() as u64;
                                        coalesced_writes += 1;
                                        coalesced_bytes += n_data.len() as u64;
                                        write_cmds += 1;
                                        adjacent_writes += 1;
                                    } else {
                                        non_adjacent_writes += 1;
                                        non_adjacent_next = Some((n_off, n_data));
                                        break;
                                    }
                                }
                                Ok(WriteCommand::CommitChunk { chunk_id, generation }) => {
                                    let _ = in_flight_writer.fetch_sub(1, Ordering::Relaxed);
                                    total_cmds += 1;
                                    commit_cmds += 1;
                                    staged_commits.push((chunk_id, generation));
                                }
                                Err(_) => break, // Channel buffer empty, proceed immediately to write
                            }
                        }

                        // Write coalesced buffer
                        let write_len = coalesced_buf.len() as u64;
                        let need_seek = if get_disable_cursor_optimization() {
                            true
                        } else {
                            match file_cursor {
                                Some(curr) => curr != offset,
                                None => true,
                            }
                        };

                        if need_seek {
                            seek_ops += 1;
                            let t_seek = std::time::Instant::now();
                            if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                                return Err(e);
                            }
                            total_seek_us += t_seek.elapsed().as_micros() as u64;
                        } else {
                            seeks_avoided += 1;
                        }

                        let t_write = std::time::Instant::now();
                        if let Err(e) = file.write_all(&coalesced_buf).await {
                            return Err(e);
                        }
                        total_write_us += t_write.elapsed().as_micros() as u64;

                        file_cursor = Some(offset + write_len);
                        bytes_written += write_len;

                        // Commit ACKs sent strictly AFTER disk write completion
                        for (cid, chunk_gen) in staged_commits {
                            let t_ack = std::time::Instant::now();
                            let _ = ack_tx.send(CommitAck {
                                chunk_id: cid,
                                generation: chunk_gen,
                                success: true,
                            }).await;
                            total_ack_us += t_ack.elapsed().as_micros() as u64;
                        }

                        // Process non-adjacent chunk popped during drain
                        if let Some((na_off, na_data)) = non_adjacent_next {
                            write_cmds += 1;
                            let na_len = na_data.len() as u64;
                            let na_seek = if get_disable_cursor_optimization() {
                                true
                            } else {
                                match file_cursor {
                                    Some(curr) => curr != na_off,
                                    None => true,
                                }
                            };
                            if na_seek {
                                seek_ops += 1;
                                let t_seek = std::time::Instant::now();
                                if let Err(e) = file.seek(std::io::SeekFrom::Start(na_off)).await {
                                    return Err(e);
                                }
                                total_seek_us += t_seek.elapsed().as_micros() as u64;
                            } else {
                                seeks_avoided += 1;
                            }

                            let t_write = std::time::Instant::now();
                            if let Err(e) = file.write_all(&na_data).await {
                                return Err(e);
                            }
                            total_write_us += t_write.elapsed().as_micros() as u64;

                            file_cursor = Some(na_off + na_len);
                            bytes_written += na_len;
                        }
                    } else {
                        // R2: Sequential Write Cursor Optimization
                        let data_len = data.len() as u64;

                        if let Some(curr) = file_cursor {
                            if offset == curr {
                                adjacent_writes += 1;
                            } else {
                                non_adjacent_writes += 1;
                            }
                        } else {
                            non_adjacent_writes += 1;
                        }

                        let need_seek = if get_disable_cursor_optimization() {
                            true
                        } else {
                            match file_cursor {
                                Some(curr) => curr != offset,
                                None => true,
                            }
                        };

                        if need_seek {
                            seek_ops += 1;
                            let t_seek = std::time::Instant::now();
                            if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                                return Err(e);
                            }
                            total_seek_us += t_seek.elapsed().as_micros() as u64;
                        } else {
                            seeks_avoided += 1;
                        }

                        let t_write = std::time::Instant::now();
                        if let Err(e) = file.write_all(&data).await {
                            return Err(e);
                        }
                        total_write_us += t_write.elapsed().as_micros() as u64;

                        file_cursor = Some(offset + data_len);
                        bytes_written += data_len;
                    }
                }
                WriteCommand::CommitChunk { chunk_id, generation } => {
                    commit_cmds += 1;
                    let t_ack = std::time::Instant::now();
                    let _ = ack_tx
                        .send(CommitAck {
                            chunk_id,
                            generation,
                            success: true,
                        })
                        .await;
                    total_ack_us += t_ack.elapsed().as_micros() as u64;
                }
            }
            total_active_us += t_cmd_start.elapsed().as_micros() as u64;
        }

        let t_flush = std::time::Instant::now();
        if let Err(e) = file.flush().await {
            return Err(e);
        }
        let flush_time_us = t_flush.elapsed().as_micros() as u64;

        let lifetime_ms = actor_start.elapsed().as_secs_f64() * 1000.0;
        let lifetime_secs = (lifetime_ms / 1000.0).max(0.001);

        let final_metrics = WriterActorMetrics {
            task_id: id,
            total_commands_processed: total_cmds,
            total_write_bytes_commands: write_cmds,
            total_commit_commands: commit_cmds,
            total_bytes_written: bytes_written,
            total_seek_operations: seek_ops,
            consecutive_seeks_avoided: seeks_avoided,
            adjacent_writes_count: adjacent_writes,
            non_adjacent_writes_count: non_adjacent_writes,
            coalesced_writes_count: coalesced_writes,
            coalesced_bytes_saved: coalesced_bytes,
            max_queue_depth: max_channel_queue,
            avg_queue_depth: if queue_samples > 0 {
                sum_channel_queue as f64 / queue_samples as f64
            } else {
                0.0
            },
            max_channel_queue_depth: max_channel_queue,
            avg_channel_queue_depth: if queue_samples > 0 {
                sum_channel_queue as f64 / queue_samples as f64
            } else {
                0.0
            },
            max_in_flight_commands: max_in_flight,
            avg_in_flight_commands: if queue_samples > 0 {
                sum_in_flight as f64 / queue_samples as f64
            } else {
                0.0
            },
            queue_depth_samples: queue_samples,
            total_seek_time_us: total_seek_us,
            total_write_time_us: total_write_us,
            total_ack_time_us: total_ack_us,
            total_flush_time_us: flush_time_us,
            total_channel_recv_wait_time_us: total_recv_wait_us,
            total_actor_active_time_us: total_active_us,
            actor_lifetime_ms: lifetime_ms,
            avg_seek_latency_us: if seek_ops > 0 {
                total_seek_us as f64 / seek_ops as f64
            } else {
                0.0
            },
            avg_write_latency_us: if write_cmds > 0 {
                total_write_us as f64 / write_cmds as f64
            } else {
                0.0
            },
            avg_ack_latency_us: if commit_cmds > 0 {
                total_ack_us as f64 / commit_cmds as f64
            } else {
                0.0
            },
            disk_write_mb_s: (bytes_written as f64 / (1024.0 * 1024.0)) / lifetime_secs,
            command_throughput_per_sec: total_cmds as f64 / lifetime_secs,
        };

        record_task_writer_metrics(id, final_metrics);

        Ok::<(), std::io::Error>(())
    });

    // Wrap manager in Arc<Mutex> for single-source-of-truth concurrent coordination
    let manager_arc = Arc::new(Mutex::new(manager));
    let error_flag = Arc::new(AtomicBool::new(false));
    let done_flag = Arc::new(AtomicBool::new(false));
    let fatal_error_msg = Arc::new(std::sync::Mutex::new(None::<String>));

    let task_strat = crate::concurrency::get_task_concurrency_strategy(id);
    let initial_concurrency = match &task_strat {
        Some(crate::concurrency::ConcurrencyStrategy::Fixed(c)) => (*c).clamp(1, 16),
        Some(crate::concurrency::ConcurrencyStrategy::Adaptive(cfg)) => {
            if cfg.enable_bdp_probe {
                crate::concurrency::seed_initial_concurrency_from_rtt(
                    head_duration,
                    cfg.min_concurrency,
                    cfg.max_concurrency,
                ).max(cfg.initial_concurrency)
            } else {
                cfg.initial_concurrency.clamp(cfg.min_concurrency, cfg.max_concurrency)
            }
        }
        None => connections.clamp(1, 16),
    };
    let initial_concurrency = if resume_capable && total_size.is_some() {
        let chunk_count = {
            let m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
            m.chunks.len()
        };
        initial_concurrency.min(chunk_count.max(1)).max(1)
    } else {
        1
    };

    let target_concurrency = Arc::new(AtomicUsize::new(initial_concurrency));
    let task_active_workers = Arc::new(AtomicUsize::new(0));
    let slot_pool = Arc::new(crate::concurrency::WorkerSlotPool::new(16));
    let telemetry = Arc::new(crate::concurrency::TaskTelemetryCollector::new(id));
    let mut controller = crate::concurrency::AdaptiveConcurrencyController::new(
        id,
        task_strat.unwrap_or(crate::concurrency::ConcurrencyStrategy::Fixed(initial_concurrency)),
    );
    controller.init_with_rtt(head_duration);

    // Shared atomic tracker for active worker in-flight byte offsets (sized for up to 16 workers)
    let worker_trackers: Arc<Vec<Arc<AtomicU64>>> = Arc::new(
        (0..16)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect(),
    );
    let worker_chunk_bounds: Arc<Vec<Arc<(AtomicU64, AtomicU64)>>> = Arc::new(
        (0..16)
            .map(|_| Arc::new((AtomicU64::new(0), AtomicU64::new(0))))
            .collect(),
    );
    let worker_total_bytes: Arc<Vec<Arc<AtomicU64>>> = Arc::new(
        (0..16)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect(),
    );

    // 5. Worker Pool
    let mut worker_handles = Vec::with_capacity(initial_concurrency);
    let mut worker_write_tx: Option<mpsc::Sender<WriteCommand>> = Some(write_tx.clone());

    if resume_capable && total_size.is_some() {
        for _ in 0..initial_concurrency {
            let guard = crate::concurrency::WorkerLifecycleGuard::acquire(
                slot_pool.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
                &worker_trackers,
            ).expect("Initial worker slots must be available");
            let slot_id = guard.slot_id;
            let handle = spawn_worker_task(
                guard,
                manager_arc.clone(),
                client.clone(),
                url.clone(),
                worker_write_tx.as_ref().unwrap().clone(),
                in_flight_write_commands.clone(),
                cancel_flag.clone(),
                delete_flag.clone(),
                pause_flag.clone(),
                error_flag.clone(),
                fatal_error_msg.clone(),
                worker_trackers[slot_id % 16].clone(),
                worker_chunk_bounds[slot_id % 16].clone(),
                worker_total_bytes[slot_id % 16].clone(),
                total_size,
                active_workers.clone(),
                ctrl.notify.clone(),
                target_concurrency.clone(),
                task_active_workers.clone(),
                telemetry.clone(),
            );
            worker_handles.push(handle);
        }
    } else {
        // Single stream download for unknown size or unresumable streams
        let client = client.clone();
        let url = url.clone();
        let write_tx = write_tx.clone();
        let in_flight = in_flight_write_commands.clone();
        let cancel_flag = cancel_flag.clone();
        let delete_flag = delete_flag.clone();
        let pause_flag = pause_flag.clone();
        let error_flag = error_flag.clone();
        let done_flag = done_flag.clone();
        let fatal_err = fatal_error_msg.clone();
        let tracker = worker_trackers[0].clone();
        let single_worker_bytes = worker_total_bytes[0].clone();
        let expected_total = total_size;
        let active_workers = active_workers.clone();
        let notify = ctrl.notify.clone();

        let handle = tokio::spawn(async move {
            let _worker_guard = WorkerGuard::new(&active_workers);
            let get_fut = client.get(&url).send();
            tokio::pin!(get_fut);
            let resp = loop {
                if cancel_flag.load(Ordering::SeqCst)
                    || pause_flag.load(Ordering::SeqCst)
                    || delete_flag.load(Ordering::SeqCst)
                    || error_flag.load(Ordering::Relaxed)
                {
                    return;
                }

                tokio::select! {
                    biased;
                    _ = notify.notified() => {}
                    res = &mut get_fut => {
                        match res {
                            Ok(r) => break r,
                            Err(e) => {
                                *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) =
                                    Some(format!("GET request failed: {}", e));
                                error_flag.store(true, Ordering::SeqCst);
                                notify.notify_waiters();
                                return;
                            }
                        }
                    }
                }
            };

            let status = resp.status();
            if !status.is_success() {
                *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(format!("Server returned HTTP {}", status));
                error_flag.store(true, Ordering::SeqCst);
                notify.notify_waiters();
                return;
            }

            let mut resp = resp;
            let mut curr = 0u64;

            loop {
                if cancel_flag.load(Ordering::SeqCst)
                    || pause_flag.load(Ordering::SeqCst)
                    || delete_flag.load(Ordering::SeqCst)
                    || error_flag.load(Ordering::Relaxed)
                {
                    return;
                }

                let chunk_opt = tokio::select! {
                    biased;
                    _ = notify.notified() => {
                        if cancel_flag.load(Ordering::SeqCst)
                            || pause_flag.load(Ordering::SeqCst)
                            || delete_flag.load(Ordering::SeqCst)
                            || error_flag.load(Ordering::Relaxed)
                        {
                            return;
                        }
                        continue;
                    }
                    res = tokio::time::timeout(CHUNK_READ_TIMEOUT, resp.chunk()) => {
                        match res {
                            Ok(Ok(opt)) => opt,
                            Ok(Err(e)) => {
                                *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) =
                                    Some(format!("Network chunk error: {}", e));
                                error_flag.store(true, Ordering::SeqCst);
                                notify.notify_waiters();
                                return;
                            }
                            Err(_) => {
                                *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) =
                                    Some("Network stream read inactivity timeout (30s)".to_string());
                                error_flag.store(true, Ordering::SeqCst);
                                notify.notify_waiters();
                                return;
                            }
                        }
                    }
                };

                let chunk_bytes = match chunk_opt {
                    Some(b) => b,
                    None => break, // EOF reached
                };

                let len = chunk_bytes.len() as u64;
                in_flight.fetch_add(1, Ordering::Relaxed);
                if write_tx
                    .send(WriteCommand::WriteBytes {
                        offset: curr,
                        data: chunk_bytes,
                    })
                    .await
                    .is_err()
                {
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                    error_flag.store(true, Ordering::SeqCst);
                    notify.notify_waiters();
                    return;
                }

                curr += len;
                single_worker_bytes.fetch_add(len, Ordering::Relaxed);
                tracker.store(curr, Ordering::Relaxed);
            }

            if let Some(exp) = expected_total {
                if curr < exp {
                    *fatal_err.lock().unwrap_or_else(|p| p.into_inner()) =
                        Some(format!("Download incomplete: got {}/{} bytes", curr, exp));
                    error_flag.store(true, Ordering::SeqCst);
                    return;
                }
            }

            in_flight.fetch_add(1, Ordering::Relaxed);
            let _ = write_tx
                .send(WriteCommand::CommitChunk {
                    chunk_id: 0,
                    generation: 1,
                })
                .await;
            done_flag.store(true, Ordering::SeqCst);
        });

        worker_handles.push(handle);
    }

    // Drop coordinator writer handle so writer actor shuts down after all chunks
    drop(write_tx);

    // 6. Coordinator Loop (ACK processing, atomic checkpointing, and UI ticker)
    let mut last_emit = std::time::Instant::now();
    let mut last_checkpoint = std::time::Instant::now();
    let mut last_downloaded: u64 = 0;
    let mut last_total_network_bytes: u64 = 0;
    let mut worker_last_bytes = [0u64; 16];
    let mut worker_smoothed_speeds = [0.0f64; 16];
    let mut smoothed_speed: f64 = 0.0;
    let mut interval = tokio::time::interval(Duration::from_millis(25));

    let mut last_controller_eval = std::time::Instant::now();

    let notify = ctrl.notify.clone();

    loop {
        tokio::select! {
            _ = notify.notified() => {}

            // Process Commit ACKs from Writer Actor
            Some(ack) = ack_rx.recv() => {
                if ack.success {
                    let mut m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
                    let _ = m.commit_chunk(ack.chunk_id, ack.generation);
                }
            }

            _ = interval.tick() => {}
        }

        // Sample queue depth & in flight writes for telemetry
        let current_q_depth = write_tx_monitor_coordinator
            .upgrade()
            .map(|s| 256usize.saturating_sub(s.capacity()))
            .unwrap_or(0);
        let current_in_flight = in_flight_write_commands.load(Ordering::Relaxed);
        telemetry.update_writer_metrics(current_q_depth, current_in_flight);

        // Adaptive Concurrency Evaluation
        let eval_interval = controller.current_eval_interval();
        if last_controller_eval.elapsed() >= eval_interval && resume_capable && total_size.is_some() {
            let (rem_chunks, committed) = {
                let m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
                (m.pending_queue.len(), m.committed_bytes)
            };
            let cur_active = slot_pool.active_worker_count();
            let cur_target = target_concurrency.load(Ordering::Relaxed);

            let snapshot = telemetry.take_snapshot(
                committed,
                total_size,
                rem_chunks,
                cur_active,
                cur_target,
            );

            let decision = controller.evaluate(&snapshot);
            crate::concurrency::record_task_concurrency_decision(decision.clone());

            if decision.target_concurrency != cur_target {
                target_concurrency.store(decision.target_concurrency, Ordering::SeqCst);

                if decision.target_concurrency > cur_active {
                    let to_spawn = decision.target_concurrency - cur_active;
                    if let Some(ref w_tx) = worker_write_tx {
                        for _ in 0..to_spawn {
                            if let Some(guard) = crate::concurrency::WorkerLifecycleGuard::acquire(
                                slot_pool.clone(),
                                task_active_workers.clone(),
                                telemetry.clone(),
                                &worker_trackers,
                            ) {
                                let slot_id = guard.slot_id;
                                let handle = spawn_worker_task(
                                    guard,
                                    manager_arc.clone(),
                                    client.clone(),
                                    url.clone(),
                                    w_tx.clone(),
                                    in_flight_write_commands.clone(),
                                    cancel_flag.clone(),
                                    delete_flag.clone(),
                                    pause_flag.clone(),
                                    error_flag.clone(),
                                    fatal_error_msg.clone(),
                                    worker_trackers[slot_id % 16].clone(),
                                    worker_chunk_bounds[slot_id % 16].clone(),
                                    worker_total_bytes[slot_id % 16].clone(),
                                    total_size,
                                    active_workers.clone(),
                                    ctrl.notify.clone(),
                                    target_concurrency.clone(),
                                    task_active_workers.clone(),
                                    telemetry.clone(),
                                );
                                worker_handles.push(handle);
                            }
                        }
                    }
                }
            }

            last_controller_eval = std::time::Instant::now();
        }

        // Teardown check (Delete > Cancel > Pause > Error)
        if delete_flag.load(Ordering::SeqCst)
            || cancel_flag.load(Ordering::SeqCst)
            || pause_flag.load(Ordering::SeqCst)
            || error_flag.load(Ordering::Relaxed)
        {
            drop(worker_write_tx.take());

            // 1. Drain remaining in-flight ACKs
            while let Ok(ack) = ack_rx.try_recv() {
                if ack.success {
                    let mut m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
                    let _ = m.commit_chunk(ack.chunk_id, ack.generation);
                }
            }

            // 2. Await all workers to release network sockets
            for handle in worker_handles {
                let _ = handle.await;
            }

            // 3. Await Writer Actor to flush and close file handle
            let _ = writer_handle.await;

            // 4. Determine intent with strict priority
            let intent = if delete_flag.load(Ordering::SeqCst) {
                FinalizeIntent::Deleted
            } else if cancel_flag.load(Ordering::SeqCst) {
                FinalizeIntent::Canceled
            } else if pause_flag.load(Ordering::SeqCst) {
                FinalizeIntent::Paused
            } else {
                let err = fatal_error_msg
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .unwrap_or_else(|| "Download failed".into());
                FinalizeIntent::Failed(err)
            };

            let cur_conn = task_active_workers.load(Ordering::Relaxed).max(1);
            finalize_coordinator(
                id,
                intent,
                &ctrl,
                &save_path,
                &partinfo_path,
                Some(&manager_arc),
                &evt_sender,
                &known_paths,
                &active_tasks,
                total_size,
                last_downloaded,
                cur_conn,
            )
            .await;
            return;
        }

        // Check completion
        let (is_done, committed, _total) = {
            let m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
            (m.is_all_committed(), m.committed_bytes, m.total_size)
        };

        if (is_done && total_size.is_some()) || done_flag.load(Ordering::Relaxed) {
            break;
        }

        // Periodic crash-consistent checkpoint flush every 1 second
        if last_checkpoint.elapsed() >= Duration::from_secs(1) {
            let cp = {
                manager_arc
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .to_checkpoint()
            };
            write_checkpoint_atomic(&partinfo_path, &cp).await;
            last_checkpoint = std::time::Instant::now();
        }

        // Compute speed and UI progress throttled to a smooth 200ms emission interval
        if last_emit.elapsed() >= Duration::from_millis(200) {
            let dt = last_emit.elapsed().as_secs_f64();
            let current_total_network_bytes: u64 = worker_total_bytes.iter().map(|b| b.load(Ordering::Relaxed)).sum();
            let network_delta = current_total_network_bytes.saturating_sub(last_total_network_bytes);
            let instant_speed = if dt > 0.0 {
                network_delta as f64 / dt
            } else {
                0.0
            };
            last_total_network_bytes = current_total_network_bytes;

            // Smooth speed using exponential moving average (EMA) to prevent flickering between 0 and spikes
            smoothed_speed = if smoothed_speed <= 0.0 {
                instant_speed
            } else if instant_speed > 0.0 {
                smoothed_speed * 0.70 + instant_speed * 0.30
            } else {
                // Stalled or waiting: decay smoothly
                smoothed_speed * 0.85
            };

            let time_left = if let Some(tot) = total_size {
                if smoothed_speed > 100.0 {
                    Some((tot.saturating_sub(committed) as f64 / smoothed_speed) as u64)
                } else {
                    None
                }
            } else {
                None
            };

            let cur_conn = task_active_workers.load(Ordering::Relaxed).max(1);
            let active_pairs: Vec<(usize, u64, u64)> = worker_trackers[..cur_conn.min(16)]
                .iter()
                .enumerate()
                .map(|(w, t)| (w, t.load(Ordering::Relaxed), 0))
                .collect();

            let ui_segs = {
                let m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
                m.generate_ui_segments(cur_conn, &active_pairs)
            };

            let conn_segs: Vec<SegmentProgress> = (0..cur_conn)
                .map(|w| {
                    let slot = w % 16;
                    let w_bytes = worker_total_bytes[slot].load(Ordering::Relaxed);
                    let w_delta = w_bytes.saturating_sub(worker_last_bytes[slot]);
                    let w_instant = if dt > 0.0 { w_delta as f64 / dt } else { 0.0 };
                    worker_smoothed_speeds[slot] = if worker_smoothed_speeds[slot] <= 0.0 {
                        w_instant
                    } else if w_instant > 0.0 {
                        worker_smoothed_speeds[slot] * 0.70 + w_instant * 0.30
                    } else {
                        worker_smoothed_speeds[slot] * 0.85
                    };
                    worker_last_bytes[slot] = w_bytes;

                    SegmentProgress {
                        segment_index: w,
                        start_byte: 0,
                        end_byte: 0,
                        current_byte: w_bytes,
                        speed: worker_smoothed_speeds[slot],
                    }
                })
                .collect();

            let _ = evt_sender.send(EngineEvent::ProgressUpdate {
                id,
                downloaded: committed,
                speed: smoothed_speed,
                time_left,
                segments: ui_segs,
                connection_segments: conn_segs,
            });

            last_emit = std::time::Instant::now();
            last_downloaded = committed;
        }
    }

    // Drain remaining ACKs
    while let Ok(ack) = ack_rx.try_recv() {
        if ack.success {
            let mut m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
            let _ = m.commit_chunk(ack.chunk_id, ack.generation);
        }
    }

    // Record final telemetry summary for task analysis and metrics
    {
        let (rem_chunks, _) = {
            let m = manager_arc.lock().unwrap_or_else(|p| p.into_inner());
            (m.pending_queue.len(), m.committed_bytes)
        };
        let cur_active = task_active_workers.load(Ordering::Relaxed);
        let cur_target = target_concurrency.load(Ordering::Relaxed);
        let total_backoff_ms = telemetry.total_backoff_duration().as_millis() as u64;
        let sleeping = telemetry.sleeping_workers();

        let decisions = crate::concurrency::get_task_concurrency_decisions(id);
        let mut first_throttle_ms: Option<u64> = None;
        let mut throttle_dur_ms = 0u64;
        let mut recovery_dur_ms = 0u64;

        for d in &decisions {
            match d.reason {
                crate::concurrency::DecisionReason::ThrottledHttp429
                | crate::concurrency::DecisionReason::ThrottledHttp503
                | crate::concurrency::DecisionReason::ThrottledCoalesced => {
                    if first_throttle_ms.is_none() {
                        first_throttle_ms = Some(d.elapsed_ms);
                    }
                }
                crate::concurrency::DecisionReason::ServerThrottleRecovery { .. }
                | crate::concurrency::DecisionReason::RecoveryProbe { .. }
                | crate::concurrency::DecisionReason::GradualRecovery { .. }
                | crate::concurrency::DecisionReason::RecoveryStable { .. } => {
                    if let Some(t0) = first_throttle_ms.take() {
                        let dur = d.elapsed_ms.saturating_sub(t0);
                        throttle_dur_ms += dur;
                        recovery_dur_ms += dur;
                    }
                }
                _ => {}
            }
        }
        if let Some(t0) = first_throttle_ms {
            let last_t = decisions.last().map(|d| d.elapsed_ms).unwrap_or(t0);
            throttle_dur_ms += last_t.saturating_sub(t0);
        }

        crate::concurrency::record_task_telemetry_summary(
            id,
            crate::concurrency::TaskTelemetrySummary {
                task_id: id,
                total_backoff_duration_ms: total_backoff_ms,
                sleeping_workers: sleeping,
                active_workers: cur_active,
                configured_concurrency: cur_target,
                available_chunks: rem_chunks,
                retries: 0,
                server_throttle_duration_ms: throttle_dur_ms,
                recovery_duration_ms: recovery_dur_ms,
            },
        );
    }

    drop(worker_write_tx.take());

    // Await all workers
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Check delete boundary after worker await
    if delete_flag.load(Ordering::SeqCst) {
        let _ = writer_handle.await;
        finalize_coordinator(
            id,
            FinalizeIntent::Deleted,
            &ctrl,
            &save_path,
            &partinfo_path,
            Some(&manager_arc),
            &evt_sender,
            &known_paths,
            &active_tasks,
            total_size,
            last_downloaded,
            conn_count,
        )
        .await;
        return;
    }

    // Await Writer Actor flush
    let writer_res = writer_handle.await;

    // Check delete boundary after writer await
    if delete_flag.load(Ordering::SeqCst) {
        finalize_coordinator(
            id,
            FinalizeIntent::Deleted,
            &ctrl,
            &save_path,
            &partinfo_path,
            Some(&manager_arc),
            &evt_sender,
            &known_paths,
            &active_tasks,
            total_size,
            last_downloaded,
            conn_count,
        )
        .await;
        return;
    }

    // Determine completion intent
    let intent = match writer_res {
        Ok(Ok(())) => {
            // Off-thread streaming SHA-256 verification (FINDING-4.2-10 & FINDING-4.4-01)
            let maybe_expected = EXPECTED_HASHES
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&id)
                .cloned()
                .or_else(|| lookup_expected_sha256(id));

            if let Some(expected_hex) = maybe_expected.filter(|h| !h.trim().is_empty()) {
                let verify_path = save_path.clone();
                let notify_del = ctrl.notify.clone();
                let del_flag = ctrl.delete_flag.clone();
                let cancel_flag = ctrl.cancel_flag.clone();
                let del_flag_worker = ctrl.delete_flag.clone();
                let mut verify_fut = tokio::task::spawn_blocking(move || {
                    crate::db::verify_file_sha256_cancellable(
                        &verify_path,
                        &expected_hex,
                        Some(&del_flag_worker),
                    )
                });

                tokio::select! {
                    biased;
                    _ = async {
                        while !del_flag.load(Ordering::SeqCst) && !cancel_flag.load(Ordering::SeqCst) {
                            notify_del.notified().await;
                        }
                    } => {
                        // Cooperative cancellation: wait for blocking task to release file handle
                        let _ = (&mut verify_fut).await;
                        if del_flag.load(Ordering::SeqCst) {
                            FinalizeIntent::Deleted
                        } else {
                            FinalizeIntent::Canceled
                        }
                    }
                    verify_res = &mut verify_fut => {
                        if del_flag.load(Ordering::SeqCst) {
                            FinalizeIntent::Deleted
                        } else if cancel_flag.load(Ordering::SeqCst) {
                            FinalizeIntent::Canceled
                        } else {
                            match verify_res {
                                Ok(Ok(crate::db::HashVerifyOutcome::Verified)) => {
                                    FinalizeIntent::Finished {
                                        hash_status: HashVerificationStatus::Verified,
                                    }
                                }
                                Ok(Ok(crate::db::HashVerifyOutcome::VerificationFailed)) => {
                                    FinalizeIntent::Finished {
                                        hash_status: HashVerificationStatus::VerificationFailed,
                                    }
                                }
                                Ok(Ok(crate::db::HashVerifyOutcome::Cancelled)) => {
                                    if del_flag.load(Ordering::SeqCst) {
                                        FinalizeIntent::Deleted
                                    } else {
                                        FinalizeIntent::Canceled
                                    }
                                }
                                Ok(Err(e)) => {
                                    eprintln!("SHA-256 verification I/O error for #{}: {}", id, e);
                                    FinalizeIntent::Finished {
                                        hash_status: HashVerificationStatus::VerificationFailed,
                                    }
                                }
                                Err(e) => {
                                    eprintln!("SHA-256 verification task error for #{}: {}", id, e);
                                    FinalizeIntent::Finished {
                                        hash_status: HashVerificationStatus::VerificationFailed,
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                FinalizeIntent::Finished {
                    hash_status: HashVerificationStatus::NotConfigured,
                }
            }
        }
        Ok(Err(e)) => FinalizeIntent::Failed(format!("Disk write error: {}", e)),
        Err(e) => FinalizeIntent::Failed(format!("Writer task failed: {}", e)),
    };

    trigger_test_hook(id, TestHookPoint::BeforeFinalization).await;

    finalize_coordinator(
        id,
        intent,
        &ctrl,
        &save_path,
        &partinfo_path,
        Some(&manager_arc),
        &evt_sender,
        &known_paths,
        &active_tasks,
        total_size,
        last_downloaded,
        conn_count,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_manager_partitioning_invariants() {
        let total_size = 104_857_600u64; // 100 MB
        let chunk_size = 2 * 1024 * 1024u64; // 2 MB
        let mgr = SegmentManager::new(total_size, chunk_size, Some("test_etag".into()));

        assert_eq!(mgr.chunks.len(), 50);
        let mut prev_end = 0u64;
        for (i, chunk) in mgr.chunks.iter().enumerate() {
            assert_eq!(chunk.id, i);
            assert_eq!(chunk.start, prev_end);
            assert!(chunk.end > chunk.start);
            assert!(chunk.end <= total_size);
            assert_eq!(chunk.status, ChunkStatus::Pending);
            prev_end = chunk.end;
        }
        assert_eq!(prev_end, total_size, "Intervals must cover total_size exactly");
    }

    #[test]
    fn test_dynamic_leasing_and_stale_ack_rejection() {
        let total = 10_000_000u64;
        let mut mgr = SegmentManager::new(total, 2_000_000, None);

        // Worker 0 leases first chunk
        let lease_0 = mgr.lease_next_chunk(0).expect("Should lease chunk 0");
        assert_eq!(lease_0.chunk_id, 0);
        assert_eq!(lease_0.generation, 1);

        // Simulate transient failure and re-queue
        mgr.return_failed_chunk(lease_0.chunk_id, lease_0.generation, false)
            .expect("Should re-queue");

        // Worker 1 leases re-queued chunk 0
        let lease_1 = mgr.lease_next_chunk(1).expect("Should lease chunk 0 again");
        assert_eq!(lease_1.chunk_id, 0);
        assert_eq!(lease_1.generation, 2, "Generation must increment");

        // Worker 0 attempts stale commit with generation 1 -> MUST BE REJECTED!
        let stale_res = mgr.commit_chunk(0, 1);
        assert!(stale_res.is_err(), "Stale ACK must be rejected");

        // Worker 1 commits with generation 2 -> MUST SUCCEED!
        let valid_res = mgr.commit_chunk(0, 2);
        assert!(valid_res.is_ok(), "Valid generation ACK must succeed");
        assert_eq!(mgr.committed_bytes, 2_000_000);
    }

    #[test]
    fn test_committed_bytes_checkpoint_persistence() {
        let total = 6_000_000u64;
        let mut mgr = SegmentManager::new(total, 2_000_000, Some("etag_123".into()));

        let l0 = mgr.lease_next_chunk(0).unwrap();
        let _l1 = mgr.lease_next_chunk(1).unwrap();
        mgr.commit_chunk(l0.chunk_id, l0.generation).unwrap();

        assert_eq!(mgr.committed_bytes, 2_000_000);
        let cp = mgr.to_checkpoint();
        assert_eq!(cp.committed_chunk_ids, vec![0]);

        // Reconstruct from checkpoint
        let restored = SegmentManager::from_checkpoint(total, Some("etag_123".into()), &cp).unwrap();
        assert_eq!(restored.committed_bytes, 2_000_000);
        assert_eq!(restored.chunks[0].status, ChunkStatus::Committed);
        assert_eq!(restored.chunks[1].status, ChunkStatus::Pending);

        // Checkpoint with wrong ETag must fail
        let etag_mismatch = SegmentManager::from_checkpoint(total, Some("etag_456".into()), &cp);
        assert!(etag_mismatch.is_err());
    }

    #[test]
    fn test_content_range_parser() {
        let (s, e, t) = parse_content_range_header("bytes 0-1048575/1048576").unwrap();
        assert_eq!(s, 0);
        assert_eq!(e, 1048575);
        assert_eq!(t, Some(1048576));

        let (s2, e2, t2) = parse_content_range_header("bytes 100-200/*").unwrap();
        assert_eq!(s2, 100);
        assert_eq!(e2, 200);
        assert_eq!(t2, None);

        assert!(parse_content_range_header("invalid_header").is_err());
    }

    #[test]
    fn test_download_engine_metrics_api() {
        let engine = spawn_download_engine();
        let m = engine.metrics();
        assert_eq!(m.active_tasks_count, 0);
        assert_eq!(m.known_paths_count, 0);
        assert_eq!(m.active_workers_count, 0);

        // Simulate active task and known path insertion
        let ctrl = TaskControl::new(101);
        engine.active_tasks.lock().unwrap_or_else(|p| p.into_inner()).insert(1, ctrl);
        engine.known_paths.lock().unwrap_or_else(|p| p.into_inner()).insert(1, PathBuf::from("test.dat"));
        engine.active_workers.fetch_add(4, Ordering::SeqCst);

        let m2 = engine.metrics();
        assert_eq!(m2.active_tasks_count, 1);
        assert_eq!(m2.known_paths_count, 1);
        assert_eq!(m2.active_workers_count, 4);

        // Clean up
        engine.active_tasks.lock().unwrap_or_else(|p| p.into_inner()).clear();
        engine.known_paths.lock().unwrap_or_else(|p| p.into_inner()).clear();
        engine.active_workers.store(0, Ordering::SeqCst);
    }

    #[test]
    fn test_worker_guard_raii_accounting() {
        let counter = Arc::new(AtomicUsize::new(0));
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        {
            let _g1 = WorkerGuard::new(&counter);
            assert_eq!(counter.load(Ordering::SeqCst), 1);
            {
                let _g2 = WorkerGuard::new(&counter);
                assert_eq!(counter.load(Ordering::SeqCst), 2);
            }
            assert_eq!(counter.load(Ordering::SeqCst), 1);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_expected_hashes_lifecycle() {
        let id = 99999u64;
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string();

        set_task_expected_sha256(id, Some(hash.clone()));
        assert_eq!(
            EXPECTED_HASHES.lock().unwrap_or_else(|p| p.into_inner()).get(&id),
            Some(&hash)
        );

        set_task_expected_sha256(id, None);
        assert_eq!(
            EXPECTED_HASHES.lock().unwrap_or_else(|p| p.into_inner()).get(&id),
            None
        );
    }

    #[test]
    fn test_mutex_poisoning_resilience() {
        let poisoned_map: Mutex<HashMap<u64, PathBuf>> = Mutex::new(HashMap::new());

        // Intentionally poison the mutex
        let _ = std::panic::catch_unwind(|| {
            let mut guard = poisoned_map.lock().unwrap();
            guard.insert(42, PathBuf::from("fail.dat"));
            panic!("Intentional panic to poison mutex");
        });

        assert!(poisoned_map.is_poisoned(), "Mutex must be poisoned");

        // Verify .unwrap_or_else(|p| p.into_inner()) recovers safely
        let mut recovered = poisoned_map.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(recovered.get(&42), Some(&PathBuf::from("fail.dat")));
        recovered.insert(43, PathBuf::from("recovered.dat"));
        assert_eq!(recovered.len(), 2);
    }

    #[test]
    fn test_parse_retry_after_delta_seconds() {
        let now = std::time::SystemTime::now();
        assert_eq!(parse_retry_after_header("120", now), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after_header("0", now), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after_header("  5  ", now), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_parse_retry_after_http_date() {
        let fixed_now = chrono::DateTime::parse_from_rfc2822("Wed, 21 Oct 2026 07:28:00 GMT")
            .unwrap()
            .into();

        // 60 seconds into future
        let future_str = "Wed, 21 Oct 2026 07:29:00 GMT";
        let parsed = parse_retry_after_header(future_str, fixed_now);
        assert_eq!(parsed, Some(Duration::from_secs(60)));

        // Past date returns Duration::ZERO
        let past_str = "Wed, 21 Oct 2026 07:20:00 GMT";
        let parsed_past = parse_retry_after_header(past_str, fixed_now);
        assert_eq!(parsed_past, Some(Duration::ZERO));
    }

    #[test]
    fn test_parse_retry_after_invalid_formats() {
        let now = std::time::SystemTime::now();
        assert_eq!(parse_retry_after_header("", now), None);
        assert_eq!(parse_retry_after_header("invalid_date_or_number", now), None);
        assert_eq!(parse_retry_after_header("-15", now), None);
    }

    #[test]
    fn test_compute_throttling_backoff_with_retry_after() {
        let ra = Some(Duration::from_secs(2));
        let backoff = compute_throttling_backoff(ra, 0, Duration::from_millis(250), Duration::from_secs(10));
        assert!(backoff >= Duration::from_secs(2));
        assert!(backoff <= Duration::from_millis(2250)); // 2s + max 200ms jitter
    }

    #[test]
    fn test_compute_throttling_backoff_without_retry_after_exponential_jitter() {
        let base = Duration::from_millis(200);
        let max = Duration::from_secs(5);
        for retries in 0..5 {
            let backoff = compute_throttling_backoff(None, retries, base, max);
            assert!(backoff >= base);
            assert!(backoff <= max);
        }
    }
}
