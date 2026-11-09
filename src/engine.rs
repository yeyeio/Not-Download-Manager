use crate::types::*;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

pub const DEFAULT_CHUNK_SIZE: u64 = 2 * 1024 * 1024; // 2 MB default chunk size

pub fn get_configured_chunk_size() -> u64 {
    if let Ok(val) = std::env::var("HYPERDOWNLOAD_CHUNK_SIZE") {
        if let Ok(size) = val.parse::<u64>() {
            return size.max(524_288);
        }
    }
    DEFAULT_CHUNK_SIZE
}
pub const MAX_CHUNK_RETRIES: u32 = 5;

#[derive(Debug)]
pub enum EngineCommand {
    StartDownload {
        id: u64,
        url: String,
        save_path: PathBuf,
        connections: usize,
    },
    PauseDownload {
        id: u64,
    },
    CancelDownload {
        id: u64,
    },
    DeleteDownload {
        id: u64,
        cleanup_files: bool,
    },
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
    MetadataDiscovered {
        id: u64,
        filename: String,
        total_size: Option<u64>,
        resume_capable: bool,
        segments: Vec<SegmentProgress>,
    },
    ProgressUpdate {
        id: u64,
        downloaded: u64,
        speed: f64,
        time_left: Option<u64>,
        segments: Vec<SegmentProgress>,
    },
    Finished {
        id: u64,
    },
    Failed {
        id: u64,
        error: String,
    },
    Paused {
        id: u64,
    },
    Canceled {
        id: u64,
    },
    Deleted {
        id: u64,
        cleanup_success: bool,
        error: Option<String>,
    },
}

#[derive(Clone)]
pub struct TaskControl {
    pub cancel_flag: Arc<AtomicBool>,
    pub delete_flag: Arc<AtomicBool>,
    pub cleanup_files: Arc<AtomicBool>,
    pub notify: Arc<tokio::sync::Notify>,
}

impl TaskControl {
    pub fn new() -> Self {
        Self {
            cancel_flag: Arc::new(AtomicBool::new(false)),
            delete_flag: Arc::new(AtomicBool::new(false)),
            cleanup_files: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

pub struct ActiveTaskGuard {
    pub id: u64,
    pub active_tasks: Arc<Mutex<std::collections::HashMap<u64, TaskControl>>>,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = self.active_tasks.lock() {
            map.remove(&self.id);
        }
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
}

pub fn spawn_download_engine() -> DownloadEngineHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<EngineCommand>();
    let (evt_tx, evt_rx) = mpsc::unbounded_channel::<EngineEvent>();

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
                    .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 IDMan/6.42")
                    .pool_max_idle_per_host(32)
                    .pool_idle_timeout(Duration::from_secs(90))
                    .tcp_keepalive(Duration::from_secs(60))
                    .tcp_nodelay(true)
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
            );

            let active_tasks: Arc<Mutex<std::collections::HashMap<u64, TaskControl>>> =
                Arc::new(Mutex::new(std::collections::HashMap::new()));
            let known_paths: Arc<Mutex<std::collections::HashMap<u64, PathBuf>>> =
                Arc::new(Mutex::new(std::collections::HashMap::new()));

            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    EngineCommand::StartDownload {
                        id,
                        url,
                        save_path,
                        connections,
                    } => {
                        let ctrl = TaskControl::new();
                        active_tasks.lock().unwrap().insert(id, ctrl.clone());
                        known_paths.lock().unwrap().insert(id, save_path.clone());

                        let evt_sender = evt_tx.clone();
                        let client = shared_client.clone();
                        let tasks_ref = active_tasks.clone();
                        tokio::spawn(async move {
                            run_download_task(id, url, save_path, connections, ctrl, evt_sender, client, tasks_ref).await;
                        });
                    }
                    EngineCommand::PauseDownload { id } => {
                        if let Some(ctrl) = active_tasks.lock().unwrap().get(&id) {
                            ctrl.cancel_flag.store(true, Ordering::SeqCst);
                            ctrl.notify.notify_waiters();
                        }
                    }
                    EngineCommand::CancelDownload { id } => {
                        if let Some(ctrl) = active_tasks.lock().unwrap().get(&id) {
                            ctrl.cancel_flag.store(true, Ordering::SeqCst);
                            ctrl.notify.notify_waiters();
                        }
                    }
                    EngineCommand::DeleteDownload { id, cleanup_files } => {
                        let maybe_ctrl = active_tasks.lock().unwrap().get(&id).cloned();
                        if let Some(ctrl) = maybe_ctrl {
                            // Active task: signal Delete so coordinator orchestrates safe teardown & cleanup
                            ctrl.cleanup_files.store(cleanup_files, Ordering::SeqCst);
                            ctrl.delete_flag.store(true, Ordering::SeqCst);
                            ctrl.cancel_flag.store(true, Ordering::SeqCst);
                            ctrl.notify.notify_waiters();
                        } else {
                            // Inactive task: clean up files directly if requested
                            let save_path = known_paths.lock().unwrap().remove(&id);
                            let mut cleanup_success = true;
                            let mut err_msg = None;

                            if cleanup_files {
                                if let Some(path) = save_path {
                                    let mut partinfo = path.clone();
                                    partinfo.set_file_name(format!("{}.partinfo", path.file_name().unwrap_or_default().to_string_lossy()));
                                    let _ = tokio::fs::remove_file(&partinfo).await;
                                    let tmp_partinfo = partinfo.with_extension("tmp");
                                    let _ = tokio::fs::remove_file(&tmp_partinfo).await;
                                    if path.exists() {
                                        if let Err(e) = tokio::fs::remove_file(&path).await {
                                            cleanup_success = false;
                                            err_msg = Some(format!("Failed to delete {}: {}", path.display(), e));
                                        }
                                    }
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

async fn write_checkpoint_atomic(path: &Path, cp: &DownloadCheckpoint) {
    let tmp_path = path.with_extension("tmp");
    if let Ok(json) = serde_json::to_string(cp) {
        if tokio::fs::write(&tmp_path, json).await.is_ok() {
            let _ = tokio::fs::rename(&tmp_path, path).await;
        }
    }
}

async fn run_download_task(
    id: u64,
    url: String,
    save_path: PathBuf,
    connections: usize,
    ctrl: TaskControl,
    evt_sender: mpsc::UnboundedSender<EngineEvent>,
    client: Arc<reqwest::Client>,
    active_tasks: Arc<Mutex<std::collections::HashMap<u64, TaskControl>>>,
) {
    let _guard = ActiveTaskGuard {
        id,
        active_tasks: active_tasks.clone(),
    };
    let cancel_flag = ctrl.cancel_flag.clone();
    let delete_flag = ctrl.delete_flag.clone();

    if delete_flag.load(Ordering::SeqCst) {
        if ctrl.cleanup_files.load(Ordering::SeqCst) {
            let mut partinfo = save_path.clone();
            partinfo.set_file_name(format!("{}.partinfo", save_path.file_name().unwrap_or_default().to_string_lossy()));
            let _ = tokio::fs::remove_file(&partinfo).await;
            let _ = tokio::fs::remove_file(partinfo.with_extension("tmp")).await;
            let _ = tokio::fs::remove_file(&save_path).await;
        }
        let _ = evt_sender.send(EngineEvent::Deleted { id, cleanup_success: true, error: None });
        return;
    }
    if cancel_flag.load(Ordering::SeqCst) {
        return;
    }
    // 1. Probe HEAD for content length, resume capability, and ETag
    let head_resp = client.head(&url).send().await;
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

    if delete_flag.load(Ordering::SeqCst) {
        if ctrl.cleanup_files.load(Ordering::SeqCst) {
            let _ = tokio::fs::remove_file(&partinfo_path).await;
            let _ = tokio::fs::remove_file(partinfo_path.with_extension("tmp")).await;
            let _ = tokio::fs::remove_file(&save_path).await;
        }
        let _ = evt_sender.send(EngineEvent::Deleted { id, cleanup_success: true, error: None });
        return;
    }
    if cancel_flag.load(Ordering::SeqCst) {
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
            let _ = evt_sender.send(EngineEvent::Failed {
                id,
                error: format!("Failed to open file: {}", e),
            });
            return;
        }
    };

    if !is_resumed {
        if let Some(total) = total_size {
            if let Err(e) = file.set_len(total).await {
                let _ = evt_sender.send(EngineEvent::Failed {
                    id,
                    error: format!("Failed to preallocate file: {}", e),
                });
                return;
            }
        }
    }

    // 4. Writer Actor with explicit ACK protocol
    let (write_tx, mut write_rx) = mpsc::channel::<WriteCommand>(256);
    let (ack_tx, mut ack_rx) = mpsc::channel::<CommitAck>(256);

    let writer_handle = tokio::spawn(async move {
        while let Some(cmd) = write_rx.recv().await {
            match cmd {
                WriteCommand::WriteBytes { offset, data } => {
                    if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                        return Err(e);
                    }
                    if let Err(e) = file.write_all(&data).await {
                        return Err(e);
                    }
                }
                WriteCommand::CommitChunk { chunk_id, generation } => {
                    let _ = ack_tx
                        .send(CommitAck {
                            chunk_id,
                            generation,
                            success: true,
                        })
                        .await;
                }
            }
        }
        if let Err(e) = file.flush().await {
            return Err(e);
        }
        Ok::<(), std::io::Error>(())
    });

    // Wrap manager in Arc<Mutex> for single-source-of-truth concurrent coordination
    let manager_arc = Arc::new(Mutex::new(manager));
    let error_flag = Arc::new(AtomicBool::new(false));
    let done_flag = Arc::new(AtomicBool::new(false));
    let fatal_error_msg = Arc::new(std::sync::Mutex::new(None::<String>));

    // Shared atomic tracker for active worker in-flight byte offsets (for smooth UI speed tracking)
    let worker_trackers: Arc<Vec<Arc<AtomicU64>>> = Arc::new(
        (0..conn_count)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect(),
    );

    // 5. Worker Pool
    let mut worker_handles = Vec::with_capacity(conn_count);

    if resume_capable && total_size.is_some() {
        for worker_id in 0..conn_count {
            let mgr = manager_arc.clone();
            let client = client.clone();
            let url = url.clone();
            let write_tx = write_tx.clone();
            let cancel_flag = cancel_flag.clone();
            let delete_flag = delete_flag.clone();
            let error_flag = error_flag.clone();
            let fatal_err = fatal_error_msg.clone();
            let tracker = worker_trackers[worker_id].clone();
            let total = total_size;

            let handle = tokio::spawn(async move {
                while !cancel_flag.load(Ordering::SeqCst) && !delete_flag.load(Ordering::SeqCst) && !error_flag.load(Ordering::Relaxed) {
                    // Pull next work chunk from SegmentManager
                    let lease = {
                        let mut m = mgr.lock().unwrap();
                        m.lease_next_chunk(worker_id)
                    };

                    let lease = match lease {
                        Some(l) => l,
                        None => break, // No more pending chunks
                    };

                    tracker.store(lease.start, Ordering::Relaxed);

                    // Execute chunk download with exponential backoff and HTTP validation
                    let mut retries = 0;
                    let mut chunk_succeeded = false;

                    while retries <= MAX_CHUNK_RETRIES && !cancel_flag.load(Ordering::SeqCst) && !delete_flag.load(Ordering::SeqCst) {
                        let range_hdr = format!("bytes={}-{}", lease.start, lease.end - 1);
                        let req = client.get(&url).header(reqwest::header::RANGE, range_hdr);

                        let resp = match req.send().await {
                            Ok(r) => r,
                            Err(_e) => {
                                retries += 1;
                                let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                                tokio::time::sleep(delay).await;
                                continue;
                            }
                        };

                        let status = resp.status();

                        // Handle transient throttling (429, 502, 503, 504)
                        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                            || status == reqwest::StatusCode::BAD_GATEWAY
                            || status == reqwest::StatusCode::GATEWAY_TIMEOUT
                        {
                            let retry_after = resp
                                .headers()
                                .get(reqwest::header::RETRY_AFTER)
                                .and_then(|h| h.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(1);
                            tokio::time::sleep(Duration::from_secs(retry_after.min(10))).await;
                            retries += 1;
                            continue;
                        }

                        // Validate HTTP 206 and Content-Range strictly
                        if let Err(e) = validate_http_206(&resp, lease.start, lease.end, total) {
                            match e {
                                EngineError::Fatal(msg) => {
                                    *fatal_err.lock().unwrap() = Some(msg);
                                    error_flag.store(true, Ordering::SeqCst);
                                    let mut m = mgr.lock().unwrap();
                                    let _ = m.return_failed_chunk(lease.chunk_id, lease.generation, true);
                                    return;
                                }
                                EngineError::Transient { .. } => {
                                    retries += 1;
                                    tokio::time::sleep(Duration::from_millis(100 * (1 << retries.min(6)))).await;
                                    continue;
                                }
                            }
                        }

                        // Stream chunk bytes
                        let mut resp = resp;
                        let mut curr = lease.start;
                        let mut stream_err = false;

                        while let Some(chunk_res) = resp.chunk().await.transpose() {
                            if cancel_flag.load(Ordering::SeqCst) || delete_flag.load(Ordering::SeqCst) || error_flag.load(Ordering::Relaxed) {
                                return;
                            }

                            let chunk_bytes = match chunk_res {
                                Ok(b) => b,
                                Err(_) => {
                                    stream_err = true;
                                    break;
                                }
                            };

                            let len = chunk_bytes.len() as u64;
                            if write_tx
                                .send(WriteCommand::WriteBytes {
                                    offset: curr,
                                    data: chunk_bytes,
                                })
                                .await
                                .is_err()
                            {
                                error_flag.store(true, Ordering::SeqCst);
                                return;
                            }

                            curr += len;
                            tracker.store(curr, Ordering::Relaxed);
                        }

                        if stream_err || curr < lease.end {
                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(100 * (1 << retries.min(6)))).await;
                            continue;
                        }

                        // Chunk completely streamed! Send commit command to Writer Actor
                        if write_tx
                            .send(WriteCommand::CommitChunk {
                                chunk_id: lease.chunk_id,
                                generation: lease.generation,
                            })
                            .await
                            .is_err()
                        {
                            error_flag.store(true, Ordering::SeqCst);
                            return;
                        }

                        chunk_succeeded = true;
                        break;
                    }

                    if !chunk_succeeded && !cancel_flag.load(Ordering::SeqCst) && !delete_flag.load(Ordering::SeqCst) {
                        let mut m = mgr.lock().unwrap();
                        if let Err(e) = m.return_failed_chunk(lease.chunk_id, lease.generation, false) {
                            *fatal_err.lock().unwrap() = Some(e);
                            error_flag.store(true, Ordering::SeqCst);
                            return;
                        }
                    }
                }
            });

            worker_handles.push(handle);
        }
    } else {
        // Single stream download for unknown size or unresumable streams
        let client = client.clone();
        let url = url.clone();
        let write_tx = write_tx.clone();
        let cancel_flag = cancel_flag.clone();
        let delete_flag = delete_flag.clone();
        let error_flag = error_flag.clone();
        let done_flag = done_flag.clone();
        let fatal_err = fatal_error_msg.clone();
        let tracker = worker_trackers[0].clone();
        let expected_total = total_size;

        let handle = tokio::spawn(async move {
            let resp = match client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    *fatal_err.lock().unwrap() = Some(format!("GET request failed: {}", e));
                    error_flag.store(true, Ordering::SeqCst);
                    return;
                }
            };

            let status = resp.status();
            if !status.is_success() {
                *fatal_err.lock().unwrap() = Some(format!("Server returned HTTP {}", status));
                error_flag.store(true, Ordering::SeqCst);
                return;
            }

            let mut resp = resp;
            let mut curr = 0u64;

            while let Some(chunk_res) = resp.chunk().await.transpose() {
                if cancel_flag.load(Ordering::SeqCst) || delete_flag.load(Ordering::SeqCst) || error_flag.load(Ordering::Relaxed) {
                    return;
                }

                let chunk_bytes = match chunk_res {
                    Ok(b) => b,
                    Err(e) => {
                        *fatal_err.lock().unwrap() = Some(format!("Network chunk error: {}", e));
                        error_flag.store(true, Ordering::SeqCst);
                        return;
                    }
                };

                let len = chunk_bytes.len() as u64;
                if write_tx
                    .send(WriteCommand::WriteBytes {
                        offset: curr,
                        data: chunk_bytes,
                    })
                    .await
                    .is_err()
                {
                    error_flag.store(true, Ordering::SeqCst);
                    return;
                }

                curr += len;
                tracker.store(curr, Ordering::Relaxed);
            }

            if let Some(exp) = expected_total {
                if curr < exp {
                    *fatal_err.lock().unwrap() =
                        Some(format!("Download incomplete: got {}/{} bytes", curr, exp));
                    error_flag.store(true, Ordering::SeqCst);
                    return;
                }
            }

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
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    let notify = ctrl.notify.clone();

    loop {
        tokio::select! {
            _ = notify.notified() => {}

            // Process Commit ACKs from Writer Actor
            Some(ack) = ack_rx.recv() => {
                if ack.success {
                    let mut m = manager_arc.lock().unwrap();
                    let _ = m.commit_chunk(ack.chunk_id, ack.generation);
                }
            }

            _ = interval.tick() => {}
        }

        // P0: Safe Delete Lifecycle
        if delete_flag.load(Ordering::SeqCst) {
            // 1. Drain remaining in-flight ACKs
            while let Ok(ack) = ack_rx.try_recv() {
                if ack.success {
                    let mut m = manager_arc.lock().unwrap();
                    let _ = m.commit_chunk(ack.chunk_id, ack.generation);
                }
            }

            // 2. Await all workers to release network sockets
            for handle in worker_handles {
                let _ = handle.await;
            }

            // 3. Await Writer Actor to flush and close file handle on Windows
            let _ = writer_handle.await;

            // 4. Suppress all future checkpoint writes!
            let mut cleanup_success = true;
            let mut err_msg = None;
            if ctrl.cleanup_files.load(Ordering::SeqCst) {
                let _ = tokio::fs::remove_file(&partinfo_path).await;
                let tmp_partinfo = partinfo_path.with_extension("tmp");
                let _ = tokio::fs::remove_file(&tmp_partinfo).await;
                if save_path.exists() {
                    if let Err(e) = tokio::fs::remove_file(&save_path).await {
                        cleanup_success = false;
                        err_msg = Some(format!("Failed to delete {}: {}", save_path.display(), e));
                    }
                }
            }

            let _ = evt_sender.send(EngineEvent::Deleted {
                id,
                cleanup_success,
                error: err_msg,
            });
            return;
        }

        // Safe Pause / Cancel Lifecycle
        if cancel_flag.load(Ordering::SeqCst) {
            while let Ok(ack) = ack_rx.try_recv() {
                if ack.success {
                    let mut m = manager_arc.lock().unwrap();
                    let _ = m.commit_chunk(ack.chunk_id, ack.generation);
                }
            }

            for handle in worker_handles {
                let _ = handle.await;
            }

            let _ = writer_handle.await;

            let cp = { manager_arc.lock().unwrap().to_checkpoint() };
            write_checkpoint_atomic(&partinfo_path, &cp).await;

            let _ = evt_sender.send(EngineEvent::Paused { id });
            return;
        }

        if error_flag.load(Ordering::Relaxed) {
            for handle in worker_handles {
                let _ = handle.await;
            }
            let _ = writer_handle.await;
            let err = fatal_error_msg
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "Download failed".into());
            let _ = evt_sender.send(EngineEvent::Failed { id, error: err });
            return;
        }

        // Check completion
        let (is_done, committed, _total) = {
            let m = manager_arc.lock().unwrap();
            (m.is_all_committed(), m.committed_bytes, m.total_size)
        };

        if (is_done && total_size.is_some()) || done_flag.load(Ordering::Relaxed) {
            break;
        }

        // Periodic crash-consistent checkpoint flush every 1 second
        if last_checkpoint.elapsed() >= Duration::from_secs(1) {
            let cp = { manager_arc.lock().unwrap().to_checkpoint() };
            write_checkpoint_atomic(&partinfo_path, &cp).await;
            last_checkpoint = std::time::Instant::now();
        }

        // Compute speed and UI progress
        let dt = last_emit.elapsed().as_secs_f64();
        let speed = if dt > 0.0 {
            (committed.saturating_sub(last_downloaded)) as f64 / dt
        } else {
            0.0
        };

        let time_left = if let Some(tot) = total_size {
            if speed > 100.0 {
                Some((tot.saturating_sub(committed) as f64 / speed) as u64)
            } else {
                None
            }
        } else {
            None
        };

        let active_pairs: Vec<(usize, u64, u64)> = worker_trackers
            .iter()
            .enumerate()
            .map(|(w, t)| (w, t.load(Ordering::Relaxed), 0))
            .collect();

        let ui_segs = {
            let m = manager_arc.lock().unwrap();
            m.generate_ui_segments(conn_count, &active_pairs)
        };

        let _ = evt_sender.send(EngineEvent::ProgressUpdate {
            id,
            downloaded: committed,
            speed,
            time_left,
            segments: ui_segs,
        });

        last_emit = std::time::Instant::now();
        last_downloaded = committed;
    }

    // Drain remaining ACKs
    while let Ok(ack) = ack_rx.try_recv() {
        if ack.success {
            let mut m = manager_arc.lock().unwrap();
            let _ = m.commit_chunk(ack.chunk_id, ack.generation);
        }
    }

    // Await all workers
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Await Writer Actor flush
    match writer_handle.await {
        Ok(Ok(())) => {
            // Clean up checkpoint file on successful completion
            let _ = tokio::fs::remove_file(&partinfo_path).await;
            let tmp_partinfo = partinfo_path.with_extension("tmp");
            let _ = tokio::fs::remove_file(&tmp_partinfo).await;

            let final_downloaded = total_size.unwrap_or(last_downloaded);
            let final_ui_segs = {
                let m = manager_arc.lock().unwrap();
                m.generate_ui_segments(conn_count, &[])
            };

            let _ = evt_sender.send(EngineEvent::ProgressUpdate {
                id,
                downloaded: final_downloaded,
                speed: 0.0,
                time_left: Some(0),
                segments: final_ui_segs,
            });

            let _ = evt_sender.send(EngineEvent::Finished { id });
        }
        Ok(Err(e)) => {
            let _ = evt_sender.send(EngineEvent::Failed {
                id,
                error: format!("Disk write error: {}", e),
            });
        }
        Err(e) => {
            let _ = evt_sender.send(EngineEvent::Failed {
                id,
                error: format!("Writer task failed: {}", e),
            });
        }
    }
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
}
