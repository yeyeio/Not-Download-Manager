use crate::types::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DownloadCheckpoint {
    total_size: u64,
    segments: Vec<SegmentProgress>,
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
                    .pool_idle_timeout(std::time::Duration::from_secs(90))
                    .tcp_keepalive(std::time::Duration::from_secs(60))
                    .tcp_nodelay(true)
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
            );

            let mut active_cancels: std::collections::HashMap<u64, Arc<AtomicBool>> =
                std::collections::HashMap::new();

            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    EngineCommand::StartDownload {
                        id,
                        url,
                        save_path,
                        connections,
                    } => {
                        let cancel_flag = Arc::new(AtomicBool::new(false));
                        active_cancels.insert(id, cancel_flag.clone());

                        let evt_sender = evt_tx.clone();
                        let client = shared_client.clone();
                        tokio::spawn(async move {
                            run_download_task(id, url, save_path, connections, cancel_flag, evt_sender, client).await;
                        });
                    }
                    EngineCommand::PauseDownload { id } | EngineCommand::CancelDownload { id } => {
                        if let Some(flag) = active_cancels.get(&id) {
                            flag.store(true, Ordering::SeqCst);
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

async fn run_download_task(
    id: u64,
    url: String,
    save_path: PathBuf,
    connections: usize,
    cancel_flag: Arc<AtomicBool>,
    evt_sender: mpsc::UnboundedSender<EngineEvent>,
    client: Arc<reqwest::Client>,
) {
    // 1. Probe HEAD or GET for content length & resume capability
    let head_resp = client.head(&url).send().await;
    let (mut total_size, mut resume_capable, mut discovered_name) = (None, false, None);

    if let Ok(resp) = head_resp {
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

    // Prepare segments according to recovered IDM mechanics
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

    let mut resumed = false;
    let mut segments = Vec::new();

    if resume_capable && save_path.exists() && partinfo_path.exists() {
        if let Ok(data) = tokio::fs::read_to_string(&partinfo_path).await {
            if let Ok(cp) = serde_json::from_str::<DownloadCheckpoint>(&data) {
                if Some(cp.total_size) == total_size && cp.segments.len() == conn_count {
                    segments = cp.segments;
                    resumed = true;
                }
            }
        }
    }

    if !resumed {
        if let Some(total) = total_size {
            let chunk_size = total / conn_count as u64;
            for i in 0..conn_count {
                let start = i as u64 * chunk_size;
                let end = if i == conn_count - 1 {
                    total
                } else {
                    (i as u64 + 1) * chunk_size
                };
                segments.push(SegmentProgress {
                    segment_index: i,
                    start_byte: start,
                    end_byte: end,
                    current_byte: start,
                });
            }
        } else {
            segments.push(SegmentProgress {
                segment_index: 0,
                start_byte: 0,
                end_byte: 0,
                current_byte: 0,
            });
        }
    }

    let _ = evt_sender.send(EngineEvent::MetadataDiscovered {
        id,
        filename: filename.clone(),
        total_size,
        resume_capable,
        segments: segments.clone(),
    });

    // 2. Storage preallocation & setup
    if let Some(parent) = save_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let truncate_file = !resumed;
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
                error: format!("Failed to create file: {}", e),
            });
            return;
        }
    };

    if !resumed {
        if let Some(total) = total_size {
            if let Err(e) = file.set_len(total).await {
                let _ = evt_sender.send(EngineEvent::Failed {
                    id,
                    error: format!("Failed to preallocate file length: {}", e),
                });
                return;
            }
        }
    }

    // 3. Storage Writer Actor (Bounded queue for direct async disk I/O)
    let (write_tx, mut write_rx) = mpsc::channel::<(u64, bytes::Bytes)>(256);

    let writer_handle = tokio::spawn(async move {
        while let Some((offset, chunk)) = write_rx.recv().await {
            if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                return Err(e);
            }
            if let Err(e) = file.write_all(&chunk).await {
                return Err(e);
            }
        }
        if let Err(e) = file.flush().await {
            return Err(e);
        }
        Ok::<(), std::io::Error>(())
    });

    // 4. Spawn parallel workers or single worker
    let error_flag = Arc::new(AtomicBool::new(false));
    let mut progress_trackers = Vec::new();
    let mut worker_handles = Vec::new();

    if conn_count > 1 && total_size.is_some() {
        for seg in &segments {
            let tracker = Arc::new(AtomicU64::new(seg.current_byte));
            progress_trackers.push(tracker.clone());

            let client = client.clone();
            let url = url.clone();
            let start = seg.current_byte;
            let end = seg.end_byte;
            let write_tx = write_tx.clone();
            let cancel_flag = cancel_flag.clone();
            let error_flag = error_flag.clone();

            let handle = tokio::spawn(async move {
                if start >= end {
                    tracker.store(end, Ordering::Relaxed);
                    return Ok(());
                }

                let range_header = format!("bytes={}-{}", start, end.saturating_sub(1));
                let resp = match client
                    .get(&url)
                    .header(reqwest::header::RANGE, range_header)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        error_flag.store(true, Ordering::Relaxed);
                        return Err(format!("Range request failed: {}", e));
                    }
                };

                let status = resp.status();
                if status != reqwest::StatusCode::PARTIAL_CONTENT && status != reqwest::StatusCode::OK {
                    error_flag.store(true, Ordering::Relaxed);
                    return Err(format!("Server returned HTTP {}", status));
                }

                let mut resp = resp;
                let mut curr = start;

                while let Some(chunk_res) = resp.chunk().await.transpose() {
                    if cancel_flag.load(Ordering::SeqCst) || error_flag.load(Ordering::Relaxed) {
                        return Ok(());
                    }

                    let chunk = match chunk_res {
                        Ok(c) => c,
                        Err(e) => {
                            error_flag.store(true, Ordering::Relaxed);
                            return Err(format!("Network chunk error: {}", e));
                        }
                    };

                    let len = chunk.len() as u64;
                    if write_tx.send((curr, chunk)).await.is_err() {
                        error_flag.store(true, Ordering::Relaxed);
                        return Err("Writer actor channel closed".to_string());
                    }

                    curr += len;
                    tracker.store(curr, Ordering::Relaxed);
                }

                if curr < end {
                    error_flag.store(true, Ordering::Relaxed);
                    return Err(format!(
                        "Segment incomplete: got {}/{} bytes",
                        curr - start,
                        end - start
                    ));
                }

                Ok::<(), String>(())
            });

            worker_handles.push(handle);
        }
    } else {
        let tracker = Arc::new(AtomicU64::new(0));
        progress_trackers.push(tracker.clone());

        let client = client.clone();
        let url = url.clone();
        let write_tx = write_tx.clone();
        let cancel_flag = cancel_flag.clone();
        let error_flag = error_flag.clone();
        let expected_total = total_size;

        let handle = tokio::spawn(async move {
            let resp = match client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    error_flag.store(true, Ordering::Relaxed);
                    return Err(format!("GET request failed: {}", e));
                }
            };

            let status = resp.status();
            if !status.is_success() {
                error_flag.store(true, Ordering::Relaxed);
                return Err(format!("Server returned HTTP {}", status));
            }

            let mut resp = resp;
            let mut curr = 0u64;

            while let Some(chunk_res) = resp.chunk().await.transpose() {
                if cancel_flag.load(Ordering::SeqCst) || error_flag.load(Ordering::Relaxed) {
                    return Ok(());
                }

                let chunk = match chunk_res {
                    Ok(c) => c,
                    Err(e) => {
                        error_flag.store(true, Ordering::Relaxed);
                        return Err(format!("Network chunk error: {}", e));
                    }
                };

                let len = chunk.len() as u64;
                if write_tx.send((curr, chunk)).await.is_err() {
                    error_flag.store(true, Ordering::Relaxed);
                    return Err("Writer actor channel closed".to_string());
                }

                curr += len;
                tracker.store(curr, Ordering::Relaxed);
            }

            if let Some(exp) = expected_total {
                if curr < exp {
                    error_flag.store(true, Ordering::Relaxed);
                    return Err(format!("Download incomplete: got {}/{} bytes", curr, exp));
                }
            }

            Ok::<(), String>(())
        });

        worker_handles.push(handle);
    }

    // Drop coordinator writer sender handle
    drop(write_tx);

    // 5. Progress Ticker & Completion Coordination
    let mut last_emit = std::time::Instant::now();
    let mut last_downloaded: u64 = 0;
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

    let all_workers_done = async {
        let mut first_error = None;
        for handle in worker_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(format!("Task join error: {}", e));
                    }
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    };
    tokio::pin!(all_workers_done);

    let mut download_error: Option<String> = None;

    loop {
        tokio::select! {
            res = &mut all_workers_done => {
                match res {
                    Ok(()) => {},
                    Err(e) => {
                        download_error = Some(e);
                    }
                }
                break;
            }
            _ = interval.tick() => {
                if cancel_flag.load(Ordering::SeqCst) {
                    if let Some(total) = total_size {
                        let cp = DownloadCheckpoint {
                            total_size: total,
                            segments: segments.clone(),
                        };
                        if let Ok(json) = serde_json::to_string(&cp) {
                            let _ = tokio::fs::write(&partinfo_path, json).await;
                        }
                    }
                    return;
                }

                let mut downloaded: u64 = 0;
                for (i, tracker) in progress_trackers.iter().enumerate() {
                    let curr = tracker.load(Ordering::Relaxed);
                    if i < segments.len() {
                        segments[i].current_byte = curr;
                        downloaded += curr.saturating_sub(segments[i].start_byte);
                    }
                }

                let dt = last_emit.elapsed().as_secs_f64();
                let speed = if dt > 0.0 {
                    (downloaded.saturating_sub(last_downloaded)) as f64 / dt
                } else {
                    0.0
                };

                let time_left = if let Some(tot) = total_size {
                    if speed > 100.0 {
                        Some((tot.saturating_sub(downloaded) as f64 / speed) as u64)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let _ = evt_sender.send(EngineEvent::ProgressUpdate {
                    id,
                    downloaded,
                    speed,
                    time_left,
                    segments: segments.clone(),
                });

                last_emit = std::time::Instant::now();
                last_downloaded = downloaded;
            }
        }
    }

    if let Some(err) = download_error {
        let _ = evt_sender.send(EngineEvent::Failed {
            id,
            error: err,
        });
        return;
    }

    if cancel_flag.load(Ordering::SeqCst) {
        if let Some(total) = total_size {
            let cp = DownloadCheckpoint {
                total_size: total,
                segments: segments.clone(),
            };
            if let Ok(json) = serde_json::to_string(&cp) {
                let _ = tokio::fs::write(&partinfo_path, json).await;
            }
        }
        return;
    }

    match writer_handle.await {
        Ok(Ok(())) => {
            // Clean up checkpoint file on successful completion
            let _ = tokio::fs::remove_file(&partinfo_path).await;

            for (i, tracker) in progress_trackers.iter().enumerate() {
                if i < segments.len() {
                    segments[i].current_byte = tracker.load(Ordering::Relaxed);
                }
            }
            let final_downloaded = total_size.unwrap_or(last_downloaded);
            let _ = evt_sender.send(EngineEvent::ProgressUpdate {
                id,
                downloaded: final_downloaded,
                speed: 0.0,
                time_left: Some(0),
                segments: segments.clone(),
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
