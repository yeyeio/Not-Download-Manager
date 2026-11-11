#![allow(dead_code)]

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Category {
    All,
    Compressed,
    Documents,
    Music,
    Programs,
    Video,
    General,
}

impl Category {
    pub fn name(&self) -> &'static str {
        match self {
            Category::All => "All Downloads",
            Category::Compressed => "Compressed",
            Category::Documents => "Documents",
            Category::Music => "Music",
            Category::Programs => "Programs",
            Category::Video => "Video",
            Category::General => "General",
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            Category::All => "All",
            Category::Compressed => "Compressed",
            Category::Documents => "Documents",
            Category::Music => "Music",
            Category::Programs => "Programs",
            Category::Video => "Video",
            Category::General => "General",
        }
    }

    pub fn from_extension(ext: &str) -> Self {
        let ext = ext.to_uppercase();
        match ext.as_str() {
            "ZIP" | "RAR" | "7Z" | "TAR" | "GZ" | "BZ2" | "ISO" | "CAB" | "ARJ" | "LZH" | "Z" => {
                Category::Compressed
            }
            "DOC" | "DOCX" | "PDF" | "TXT" | "XLS" | "XLSX" | "PPT" | "PPTX" | "RTF" | "CHM" => {
                Category::Documents
            }
            "MP3" | "WAV" | "WMA" | "MPA" | "OGG" | "AAC" | "M4A" | "FLAC" => Category::Music,
            "EXE" | "MSI" | "APK" | "DEB" | "RPM" | "BAT" | "CMD" | "COM" => Category::Programs,
            "MP4" | "MKV" | "AVI" | "MPG" | "MPEG" | "RM" | "WEBM" | "FLV" | "WMV" | "MOV"
            | "3GP" | "M4V" => Category::Video,
            _ => Category::General,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashVerificationStatus {
    NotConfigured,
    Verified,
    VerificationFailed,
}

impl Default for HashVerificationStatus {
    fn default() -> Self {
        HashVerificationStatus::NotConfigured
    }
}

impl HashVerificationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HashVerificationStatus::NotConfigured => "NotConfigured",
            HashVerificationStatus::Verified => "Verified",
            HashVerificationStatus::VerificationFailed => "VerificationFailed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "Verified" => HashVerificationStatus::Verified,
            "VerificationFailed" => HashVerificationStatus::VerificationFailed,
            _ => HashVerificationStatus::NotConfigured,
        }
    }

    pub fn display_text(&self) -> &'static str {
        match self {
            HashVerificationStatus::NotConfigured => "Not configured",
            HashVerificationStatus::Verified => "SHA-256 Verified",
            HashVerificationStatus::VerificationFailed => "SHA-256 Verification Failed",
        }
    }
}

impl std::fmt::Display for HashVerificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
    Connecting,
    Downloading,
    Paused,
    Completed,
    Error(String),
    Canceled,
    CleanupPending,
}

impl DownloadStatus {
    pub fn text(&self) -> String {
        match self {
            DownloadStatus::Queued => "Queued".to_string(),
            DownloadStatus::Connecting => "Connecting...".to_string(),
            DownloadStatus::Downloading => "Downloading".to_string(),
            DownloadStatus::Paused => "Paused".to_string(),
            DownloadStatus::Completed => "Complete".to_string(),
            DownloadStatus::Error(err) => format!("Error: {}", err),
            DownloadStatus::Canceled => "Canceled".to_string(),
            DownloadStatus::CleanupPending => "Cleanup Pending".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentProgress {
    pub segment_index: usize,
    pub start_byte: u64,
    pub end_byte: u64,
    pub current_byte: u64,
    #[serde(default)]
    pub speed: f64,
}

impl Default for SegmentProgress {
    fn default() -> Self {
        Self {
            segment_index: 0,
            start_byte: 0,
            end_byte: 0,
            current_byte: 0,
            speed: 0.0,
        }
    }
}

impl SegmentProgress {
    pub fn ratio(&self) -> f32 {
        if self.end_byte <= self.start_byte {
            return 1.0;
        }
        let total = (self.end_byte - self.start_byte) as f32;
        let done = (self.current_byte.saturating_sub(self.start_byte)) as f32;
        (done / total).clamp(0.0, 1.0)
    }
}

#[derive(Debug, Clone)]
pub struct DownloadItem {
    pub id: u64,
    pub url: String,
    pub filename: String,
    pub category: Category,
    pub save_path: PathBuf,
    pub total_size: Option<u64>,
    pub downloaded_bytes: u64,
    pub status: DownloadStatus,
    pub transfer_rate: f64, // bytes/sec
    pub time_left_secs: Option<u64>,
    pub resume_capable: bool,
    pub date_added: DateTime<Local>,
    pub max_connections: usize,
    pub segments: Vec<SegmentProgress>,
    pub connection_segments: Vec<SegmentProgress>,
    pub description: String,
    pub queue: Option<String>,
    pub last_try_date: Option<DateTime<Local>>,
    pub parent_page: Option<String>,
    pub referer: Option<String>,
    pub expected_sha256: Option<String>,
    pub hash_status: HashVerificationStatus,
}

impl Default for DownloadItem {
    fn default() -> Self {
        Self {
            id: 0,
            url: String::new(),
            filename: String::new(),
            category: Category::General,
            save_path: PathBuf::new(),
            total_size: None,
            downloaded_bytes: 0,
            status: DownloadStatus::Queued,
            transfer_rate: 0.0,
            time_left_secs: None,
            resume_capable: true,
            date_added: Local::now(),
            max_connections: 8,
            segments: Vec::new(),
            connection_segments: Vec::new(),
            description: String::new(),
            queue: None,
            last_try_date: None,
            parent_page: None,
            referer: None,
            expected_sha256: None,
            hash_status: HashVerificationStatus::NotConfigured,
        }
    }
}

// ============================================================================
// Engine Commands & Events
// ============================================================================

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, PartialEq)]
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
        connection_segments: Vec<SegmentProgress>,
    },
    Finished {
        id: u64,
        hash_status: HashVerificationStatus,
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

// ============================================================================
// Concurrency Control & Ownership Primitives
// ============================================================================

#[derive(Clone)]
pub struct TaskControl {
    pub instance_id: u64,
    pub cancel_flag: Arc<AtomicBool>,
    pub delete_flag: Arc<AtomicBool>,
    pub pause_flag: Arc<AtomicBool>,
    pub cleanup_files: Arc<AtomicBool>,
    pub error_flag: Arc<AtomicBool>,
    pub terminal_emitted: Arc<AtomicBool>,
    pub notify: Arc<tokio::sync::Notify>,
}

impl TaskControl {
    pub fn new(instance_id: u64) -> Self {
        Self {
            instance_id,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            delete_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            cleanup_files: Arc::new(AtomicBool::new(false)),
            error_flag: Arc::new(AtomicBool::new(false)),
            terminal_emitted: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Atomically marks that a terminal event has been emitted.
    /// Returns true if this call succeeded in setting the flag (first terminal event),
    /// or false if a terminal event was already emitted for this task instance.
    pub fn mark_terminal_emitted(&self) -> bool {
        self.terminal_emitted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

impl Default for TaskControl {
    fn default() -> Self {
        Self::new(0)
    }
}

pub struct ActiveTaskGuard {
    pub id: u64,
    pub instance_id: u64,
    pub active_tasks: Arc<Mutex<HashMap<u64, TaskControl>>>,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        let mut map = self.active_tasks.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = map.get(&self.id) {
            if entry.instance_id == self.instance_id {
                map.remove(&self.id);
            }
        }
    }
}

// ============================================================================
// Engine Observability Metrics
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EngineMetrics {
    pub active_tasks_count: usize,
    pub known_paths_count: usize,
    pub active_workers_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSettings {
    pub download_id: u64,
    pub max_connections: usize,
    pub chunk_size: u64,
    pub speed_limit: Option<u64>,
    pub retry_limit: u32,
}

impl Default for DownloadSettings {
    fn default() -> Self {
        Self {
            download_id: 0,
            max_connections: 8,
            chunk_size: 2 * 1024 * 1024,
            speed_limit: None,
            retry_limit: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub id: String,
    pub name: String,
    pub visible: bool,
    pub width: f32,
}

pub fn default_columns() -> Vec<ColumnConfig> {
    vec![
        ColumnConfig { id: "filename".into(), name: "File Name".into(), visible: true, width: 260.0 },
        ColumnConfig { id: "queue".into(), name: "Q".into(), visible: true, width: 35.0 },
        ColumnConfig { id: "size".into(), name: "Size".into(), visible: true, width: 85.0 },
        ColumnConfig { id: "status".into(), name: "Status".into(), visible: true, width: 130.0 },
        ColumnConfig { id: "time_left".into(), name: "Time left".into(), visible: true, width: 80.0 },
        ColumnConfig { id: "transfer_rate".into(), name: "Transfer rate".into(), visible: true, width: 95.0 },
        ColumnConfig { id: "date_added".into(), name: "Date Added".into(), visible: true, width: 155.0 },
        ColumnConfig { id: "description".into(), name: "Description".into(), visible: true, width: 120.0 },
        ColumnConfig { id: "save_to".into(), name: "Save To".into(), visible: false, width: 180.0 },
        ColumnConfig { id: "last_try_date".into(), name: "Last Try Date".into(), visible: false, width: 155.0 },
        ColumnConfig { id: "parent_page".into(), name: "Parent web page".into(), visible: false, width: 160.0 },
        ColumnConfig { id: "referer".into(), name: "Referer".into(), visible: false, width: 160.0 },
    ]
}

impl DownloadItem {
    pub fn file_icon(&self) -> &'static str {
        let ext = self.filename.split('.').last().unwrap_or("").to_lowercase();
        match ext.as_str() {
            "iso" | "img" => "ISO",
            "exe" | "bat" | "cmd" | "com" => "EXE",
            "msi" => "MSI",
            "apk" => "APK",
            "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" => "ZIP",
            "pdf" | "doc" | "docx" | "txt" | "rtf" | "xls" | "xlsx" | "ppt" | "pptx" => "DOC",
            "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" => "AUDIO",
            "mp4" | "mkv" | "avi" | "webm" | "mov" | "wmv" | "flv" | "3gp" | "m4v" => "VIDEO",
            _ => "FILE",
        }
    }

    pub fn progress_ratio(&self) -> f32 {
        if let Some(total) = self.total_size {
            if total > 0 {
                return (self.downloaded_bytes as f32 / total as f32).clamp(0.0, 1.0);
            }
        }
        if self.status == DownloadStatus::Completed {
            1.0
        } else {
            0.0
        }
    }

    pub fn formatted_size(&self) -> String {
        if let Some(total) = self.total_size {
            format_bytes(total)
        } else {
            "-".to_string()
        }
    }

    pub fn formatted_downloaded(&self) -> String {
        format_bytes(self.downloaded_bytes)
    }

    pub fn formatted_speed(&self) -> String {
        if self.status == DownloadStatus::Downloading && self.transfer_rate > 0.0 {
            format!("{}/s", format_bytes(self.transfer_rate as u64))
        } else {
            "-".to_string()
        }
    }

    pub fn formatted_time_left(&self) -> String {
        if self.status == DownloadStatus::Completed {
            "".to_string()
        } else if let Some(secs) = self.time_left_secs {
            if secs >= 3600 {
                format!("{} hr {} min", secs / 3600, (secs % 3600) / 60)
            } else if secs >= 60 {
                format!("{} min {} sec", secs / 60, secs % 60)
            } else {
                format!("{} sec", secs)
            }
        } else {
            "-".to_string()
        }
    }
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64).replace('.', ",")
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64).replace('.', ",")
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64).replace('.', ",")
    } else {
        format!("{} Bytes", bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteCredential {
    pub site_url: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    // General
    pub launch_on_startup: bool,
    pub auto_start_clipboard: bool,
    pub capture_chrome: bool,
    pub capture_firefox: bool,
    pub capture_edge: bool,
    pub capture_opera: bool,
    pub capture_brave: bool,
    pub browser_alt_key_prevent: bool,

    // File Types
    pub default_file_types: String,
    pub dont_download_from: String,

    // Save To
    pub default_download_dir: PathBuf,
    pub dir_compressed: PathBuf,
    pub dir_documents: PathBuf,
    pub dir_music: PathBuf,
    pub dir_programs: PathBuf,
    pub dir_video: PathBuf,
    pub temp_dir: PathBuf,
    pub remember_last_folder: bool,

    // Connection
    pub connection_type: String,
    pub default_connections: usize,
    pub speed_limiter_enabled: bool,
    pub speed_limit_kbps: u64,
    pub enable_download_limits: bool,
    pub download_limit_mb: u64,
    pub download_limit_hours: u64,
    pub max_simultaneous_downloads: usize,

    // Downloads
    pub show_start_dialog: bool,
    pub show_complete_dialog: bool,
    pub show_progress_dialog: bool,
    pub hide_complete_for_queue: bool,
    pub virus_scanner_path: String,
    pub virus_scanner_params: String,

    // Proxy
    pub use_http_proxy: bool,
    pub http_proxy_host: String,
    pub http_proxy_port: u16,
    pub use_socks_proxy: bool,
    pub socks_proxy_host: String,
    pub socks_proxy_port: u16,
    pub proxy_user: String,
    pub proxy_pass: String,
    pub proxy_exceptions: String,

    // Site Logins
    pub site_logins: Vec<SiteCredential>,

    // Dial-Up
    pub dial_up_connection: String,
    pub redial_attempts: u32,
    pub time_between_redials: u32,

    // Sounds
    pub play_sound_complete: bool,
    pub play_sound_error: bool,
    pub play_sound_queue: bool,
}

impl Default for AppConfig {
    fn default() -> Self {
        let download_dir = dirs_or_fallback();
        let temp = std::env::temp_dir();
        Self {
            launch_on_startup: true,
            auto_start_clipboard: false,
            capture_chrome: true,
            capture_firefox: true,
            capture_edge: true,
            capture_opera: true,
            capture_brave: true,
            browser_alt_key_prevent: true,

            default_file_types: "3GP 7Z AAC ACE AIF APK ARJ ASF AVI BIN BZ2 EXE GZ GZIP IMG ISO LZH M4A M4V MKV MOV MP3 MP4 MPA MPE MPEG MPG MSI MSU OGG OGV PDF PLJ PPS PPT QT R0* R1* RA RAR RM RMVB SEA SIT SITX TAR TIF TIFF WAV WMA WMV Z ZIP".to_string(),
            dont_download_from: "*.update.microsoft.com\n*.windowsupdate.com".to_string(),

            default_download_dir: download_dir.clone(),
            dir_compressed: download_dir.join("Compressed"),
            dir_documents: download_dir.join("Documents"),
            dir_music: download_dir.join("Music"),
            dir_programs: download_dir.join("Programs"),
            dir_video: download_dir.join("Video"),
            temp_dir: temp,
            remember_last_folder: true,

            connection_type: "High speed: Direct connection (Ethernet/Cable) / Wi-Fi / 4G/5G".to_string(),
            default_connections: 8, // Function 0x4fee00
            speed_limiter_enabled: false,
            speed_limit_kbps: 500,
            enable_download_limits: false,
            download_limit_mb: 5000,
            download_limit_hours: 1,
            max_simultaneous_downloads: 4,

            show_start_dialog: true,
            show_complete_dialog: true,
            show_progress_dialog: true,
            hide_complete_for_queue: false,
            virus_scanner_path: "".to_string(),
            virus_scanner_params: "-Scan -ScanType 3 -File \"%1\"".to_string(),

            use_http_proxy: false,
            http_proxy_host: "".to_string(),
            http_proxy_port: 8080,
            use_socks_proxy: false,
            socks_proxy_host: "".to_string(),
            socks_proxy_port: 1080,
            proxy_user: "".to_string(),
            proxy_pass: "".to_string(),
            proxy_exceptions: "localhost, 127.0.0.1, *.local".to_string(),

            site_logins: vec![
                SiteCredential {
                    site_url: "https://rapidgator.net".to_string(),
                    username: "premium_user".to_string(),
                    password: "••••••••••••".to_string(),
                },
                SiteCredential {
                    site_url: "https://mega.nz".to_string(),
                    username: "pro_account@example.com".to_string(),
                    password: "••••••••••••".to_string(),
                },
            ],

            dial_up_connection: "(None)".to_string(),
            redial_attempts: 10,
            time_between_redials: 5,

            play_sound_complete: true,
            play_sound_error: true,
            play_sound_queue: true,
        }
    }
}

pub fn dirs_or_fallback() -> PathBuf {
    if let Ok(user_profile) = std::env::var("USERPROFILE") {
        PathBuf::from(user_profile).join("Downloads")
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join("Downloads")
    } else {
        PathBuf::from("./Downloads")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_status_cleanup_pending() {
        assert_eq!(DownloadStatus::CleanupPending.text(), "Cleanup Pending");
        assert_eq!(DownloadStatus::Queued.text(), "Queued");
        assert_eq!(DownloadStatus::Connecting.text(), "Connecting...");
        assert_eq!(DownloadStatus::Downloading.text(), "Downloading");
        assert_eq!(DownloadStatus::Paused.text(), "Paused");
        assert_eq!(DownloadStatus::Completed.text(), "Complete");
        assert_eq!(DownloadStatus::Canceled.text(), "Canceled");
        assert_eq!(DownloadStatus::Error("err".into()).text(), "Error: err");
    }

    #[test]
    fn test_hash_verification_status_conversions() {
        assert_eq!(HashVerificationStatus::default(), HashVerificationStatus::NotConfigured);
        assert_eq!(HashVerificationStatus::NotConfigured.as_str(), "NotConfigured");
        assert_eq!(HashVerificationStatus::Verified.as_str(), "Verified");
        assert_eq!(HashVerificationStatus::VerificationFailed.as_str(), "VerificationFailed");

        assert_eq!(HashVerificationStatus::from_str("NotConfigured"), HashVerificationStatus::NotConfigured);
        assert_eq!(HashVerificationStatus::from_str("Verified"), HashVerificationStatus::Verified);
        assert_eq!(HashVerificationStatus::from_str("VerificationFailed"), HashVerificationStatus::VerificationFailed);
        assert_eq!(HashVerificationStatus::from_str("anything_else"), HashVerificationStatus::NotConfigured);

        assert_eq!(HashVerificationStatus::NotConfigured.display_text(), "Not configured");
        assert_eq!(HashVerificationStatus::Verified.display_text(), "SHA-256 Verified");
        assert_eq!(HashVerificationStatus::VerificationFailed.display_text(), "SHA-256 Verification Failed");

        assert_eq!(format!("{}", HashVerificationStatus::Verified), "Verified");
    }

    #[test]
    fn test_task_control_terminal_emitted_atomic() {
        let ctrl = TaskControl::new(42);
        assert_eq!(ctrl.instance_id, 42);
        assert!(ctrl.mark_terminal_emitted(), "First call must succeed");
        assert!(!ctrl.mark_terminal_emitted(), "Subsequent call must return false");
        assert!(!ctrl.mark_terminal_emitted(), "Third call must return false");
    }

    #[test]
    fn test_active_task_guard_instance_id_isolation() {
        let active_tasks = Arc::new(Mutex::new(HashMap::new()));
        let task_id = 100u64;

        // Register instance 2 in active_tasks
        let ctrl2 = TaskControl::new(2);
        active_tasks.lock().unwrap().insert(task_id, ctrl2);

        // Guard representing old instance 1 drops
        {
            let _old_guard = ActiveTaskGuard {
                id: task_id,
                instance_id: 1,
                active_tasks: active_tasks.clone(),
            };
        }

        // Active task instance 2 must NOT be evicted by old guard
        {
            let map = active_tasks.lock().unwrap();
            assert!(map.contains_key(&task_id), "Instance 2 must not be evicted by guard 1");
            assert_eq!(map.get(&task_id).unwrap().instance_id, 2);
        }

        // Guard matching instance 2 drops
        {
            let _current_guard = ActiveTaskGuard {
                id: task_id,
                instance_id: 2,
                active_tasks: active_tasks.clone(),
            };
        }

        // Now entry must be cleanly removed
        assert!(active_tasks.lock().unwrap().is_empty(), "Matching guard drop must evict entry");
    }

    #[test]
    fn test_active_task_guard_poisoned_mutex_resilience() {
        let active_tasks = Arc::new(Mutex::new(HashMap::new()));
        let task_id = 200u64;
        let ctrl = TaskControl::new(10);
        active_tasks.lock().unwrap().insert(task_id, ctrl);

        // Intentionally poison the mutex in another thread
        let tasks_clone = active_tasks.clone();
        let _ = std::thread::spawn(move || {
            let _lock = tasks_clone.lock().unwrap();
            panic!("Intentional panic to poison mutex");
        }).join();

        assert!(active_tasks.is_poisoned(), "Mutex must be poisoned");

        // ActiveTaskGuard drop must recover poisoned lock and remove entry without panic
        {
            let _guard = ActiveTaskGuard {
                id: task_id,
                instance_id: 10,
                active_tasks: active_tasks.clone(),
            };
        }

        // Verify entry was removed
        let map = active_tasks.lock().unwrap_or_else(|p| p.into_inner());
        assert!(!map.contains_key(&task_id), "Guard must successfully evict entry even with poisoned mutex");
    }

    #[test]
    fn test_engine_metrics_defaults() {
        let metrics = EngineMetrics::default();
        assert_eq!(metrics.active_tasks_count, 0);
        assert_eq!(metrics.known_paths_count, 0);
        assert_eq!(metrics.active_workers_count, 0);
    }
}
