#![allow(dead_code)]

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
            Category::All => "📁",
            Category::Compressed => "🗜",
            Category::Documents => "📄",
            Category::Music => "🎵",
            Category::Programs => "💻",
            Category::Video => "🎬",
            Category::General => "📦",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
    Connecting,
    Downloading,
    Paused,
    Completed,
    Error(String),
    Canceled,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentProgress {
    pub segment_index: usize,
    pub start_byte: u64,
    pub end_byte: u64,
    pub current_byte: u64,
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
    pub description: String,
    pub queue: Option<String>,
    pub last_try_date: Option<DateTime<Local>>,
    pub parent_page: Option<String>,
    pub referer: Option<String>,
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
            description: String::new(),
            queue: None,
            last_try_date: None,
            parent_page: None,
            referer: None,
        }
    }
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
            "iso" | "img" => "💽",
            "exe" | "bat" | "cmd" | "com" => "⚙️",
            "msi" => "📦",
            "apk" => "📱",
            "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" => "🗜️",
            "pdf" | "doc" | "docx" | "txt" | "rtf" | "xls" | "xlsx" | "ppt" | "pptx" => "📄",
            "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" => "🎵",
            "mp4" | "mkv" | "avi" | "webm" | "mov" | "wmv" | "flv" | "3gp" | "m4v" => "🎬",
            _ => "📄",
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

fn dirs_or_fallback() -> PathBuf {
    if let Ok(user_profile) = std::env::var("USERPROFILE") {
        PathBuf::from(user_profile).join("Downloads")
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join("Downloads")
    } else {
        PathBuf::from("./Downloads")
    }
}
