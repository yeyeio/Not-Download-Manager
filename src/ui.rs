#![allow(dead_code)]

use crate::engine::*;
use crate::types::*;
use chrono::{DateTime, Local};
use eframe::egui::{self, Color32, Panel, RichText, Vec2};
use std::path::PathBuf;
use std::sync::Arc;

pub fn get_file_icon_source(filename: &str) -> egui::ImageSource<'static> {
    let ext = filename.split('.').last().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "iso" | "img" | "bin" => egui::include_image!("../assets/icons/disc.svg"),
        "exe" | "bat" | "cmd" | "com" | "msi" => egui::include_image!("../assets/icons/exe.svg"),
        "apk" => egui::include_image!("../assets/icons/android.svg"),
        "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" => egui::include_image!("../assets/icons/zip.svg"),
        "pdf" => egui::include_image!("../assets/icons/pdf.svg"),
        "doc" | "docx" | "txt" | "rtf" | "xls" | "xlsx" | "ppt" | "pptx" => egui::include_image!("../assets/icons/document.svg"),
        "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" => egui::include_image!("../assets/icons/audio.svg"),
        "mp4" | "mkv" | "avi" | "webm" | "mov" | "wmv" | "flv" | "3gp" | "m4v" => egui::include_image!("../assets/icons/video.svg"),
        _ => egui::include_image!("../assets/icons/document.svg"),
    }
}

pub fn get_category_icon_source(category: Category) -> egui::ImageSource<'static> {
    match category {
        Category::All => egui::include_image!("../assets/icons/folder-download.svg"),
        Category::Compressed => egui::include_image!("../assets/icons/folder-archive.svg"),
        Category::Documents => egui::include_image!("../assets/icons/folder-docs.svg"),
        Category::Music => egui::include_image!("../assets/icons/folder-audio.svg"),
        Category::Programs => egui::include_image!("../assets/icons/folder-app.svg"),
        Category::Video => egui::include_image!("../assets/icons/folder-video.svg"),
        Category::General => egui::include_image!("../assets/icons/folder-base.svg"),
    }
}

#[allow(dead_code)]
pub type NotDownloadManagerApp = IdmApp;

pub struct IdmApp {
    pub downloads: Vec<DownloadItem>,
    pub selected_id: Option<u64>,
    pub selected_category: Category,
    pub status_filter: Option<bool>, // None = all, Some(false) = unfinished, Some(true) = finished
    pub queue_filter: Option<String>,
    pub categories_visible: bool,
    pub config: AppConfig,

    // Category tree expand/collapse states
    pub tree_all_expanded: bool,
    pub tree_unfinished_expanded: bool,
    pub tree_finished_expanded: bool,
    pub tree_grabber_expanded: bool,
    pub tree_queues_expanded: bool,

    // Modal states
    pub show_add_url_modal: bool,
    pub new_download_url: String,
    pub new_download_category: Category,
    pub use_authorization: bool,
    pub auth_login: String,
    pub auth_pass: String,

    pub show_options_modal: bool,
    pub options_tab: usize,
    pub new_site_url: String,
    pub new_site_user: String,
    pub new_site_pass: String,

    pub show_scheduler_modal: bool,
    pub scheduler_start_time: String,
    pub scheduler_stop_time: String,
    pub scheduler_active: bool,

    pub show_progress_modal: bool,
    pub progress_modal_id: Option<u64>,
    pub progress_view_connections: bool,

    pub show_about_modal: bool,
    pub show_registration_modal: bool,
    pub reg_first_name: String,
    pub reg_last_name: String,
    pub reg_email: String,
    pub reg_serial: String,

    // Columns Management (Picture-8 & Picture-9)
    pub columns: Vec<ColumnConfig>,
    pub temp_columns: Vec<ColumnConfig>,
    pub show_columns_modal: bool,
    pub selected_column_idx: Option<usize>,
    pub column_width_input: String,

    // Table Sorting
    pub sort_column: Option<String>,
    pub sort_ascending: bool,

    // Batch Download
    pub show_batch_download_modal: bool,
    pub batch_url_pattern: String,
    pub batch_is_number: bool,
    pub batch_from: i64,
    pub batch_to: i64,
    pub batch_wildcard_size: usize,
    pub batch_auth: bool,
    pub batch_user: String,
    pub batch_pass: String,

    // Find Dialog
    pub show_find_modal: bool,
    pub find_query: String,
    pub find_match_case: bool,

    // Speed Limiter Dialog
    pub show_speed_limiter_modal: bool,

    // Site Grabber Wizard
    pub show_site_grabber_modal: bool,
    pub grabber_step: usize,
    pub grabber_project_name: String,
    pub grabber_start_url: String,
    pub grabber_save_dir: PathBuf,
    pub grabber_depth: usize,
    pub grabber_file_filter: usize,

    // Scheduler & Queues
    pub scheduler_tab: usize,
    pub selected_queue_name: String,
    pub queues_list: Vec<String>,
    pub new_queue_name_input: String,
    pub show_new_queue_input: bool,
    pub scheduler_periodic: bool,
    pub scheduler_days: [bool; 7],
    pub scheduler_retries: u32,
    pub scheduler_hang_up: bool,
    pub scheduler_exit_idm: bool,
    pub scheduler_turn_off: bool,
    pub scheduler_force_exit: bool,
    pub scheduler_on_startup: bool,
    pub scheduler_open_file: String,
    pub scheduler_simultaneous: usize,
    pub scheduler_selected_id: Option<u64>,

    // Extra UI Features
    pub show_tip_modal: bool,
    pub tip_index: usize,
    pub show_update_modal: bool,
    pub show_drop_target: bool,
    pub dark_mode: bool,
    pub status_message: Option<(String, std::time::Instant)>,

    // Background download engine & persistence
    pub engine: DownloadEngineHandle,
    pub next_id: u64,
    pub db: Arc<crate::db::Database>,
    pub is_reconciling: bool,
    pub reconcile_rx: Option<std::sync::mpsc::Receiver<Result<Vec<DownloadItem>, String>>>,
    pub reconcile_handle: Option<std::thread::JoinHandle<()>>,
    pub deleted_task_ids: std::collections::HashSet<u64>,
    pub deleting_task_ids: std::collections::HashSet<u64>,
    pub task_versions: std::collections::HashMap<u64, u64>,
    pub cleanup_errors: std::collections::HashMap<u64, String>,

    // Headless / screenshot handling
    pub screenshot_path: Option<PathBuf>,
    pub frame_counter: u32,
    pub screenshot_requested: bool,
    pub screenshot_saved: Arc<std::sync::atomic::AtomicBool>,
}

impl IdmApp {
    #[inline]
    pub fn mark_task_mutated(&mut self, id: u64) {
        *self.task_versions.entry(id).or_insert(0) += 1;
    }

    #[inline]
    pub fn mark_task_deleted(&mut self, id: u64) {
        self.deleted_task_ids.insert(id);
        self.deleting_task_ids.remove(&id);
        self.task_versions.remove(&id);
    }
    pub fn new(screenshot_path: Option<PathBuf>) -> Self {
        let engine = spawn_download_engine();
        let config = AppConfig::default();
        let initial_columns = default_columns();

        let db_path = std::env::var("NOT_DOWNLOAD_MANAGER_DB_PATH")
            .or_else(|_| std::env::var("HYPERDOWNLOAD_DB_PATH"))
            .map(PathBuf::from)
            .unwrap_or_else(|_| config.default_download_dir.join("not_download_manager.db"));

        let db = Arc::new(crate::db::Database::open(&db_path).unwrap_or_else(|err| {
            eprintln!("Warning: Failed to open DB at {:?}: {}, falling back to memory", db_path, err);
            crate::db::Database::open_in_memory().expect("In-memory DB must open")
        }));

        let initial_max_id = db.get_max_download_id().unwrap_or(0);

        let (rec_tx, rec_rx) = std::sync::mpsc::channel();
        let db_for_rec = db.clone();
        let reconcile_handle = std::thread::spawn(move || {
            let res = db_for_rec
                .reconcile_and_load_all()
                .map_err(|e| format!("Reconciliation failed: {}", e));
            let _ = rec_tx.send(res);
        });

        let app = Self {
            downloads: Vec::new(),
            selected_id: None,
            selected_category: Category::All,
            status_filter: None,
            queue_filter: None,
            categories_visible: true,
            config: config.clone(),

            tree_all_expanded: true,
            tree_unfinished_expanded: false,
            tree_finished_expanded: false,
            tree_grabber_expanded: false,
            tree_queues_expanded: true,

            show_add_url_modal: false,
            new_download_url: "https://proof.ovh.net/files/100Mb.dat".to_string(),
            new_download_category: Category::General,
            use_authorization: false,
            auth_login: String::new(),
            auth_pass: String::new(),

            show_options_modal: false,
            options_tab: 0,
            new_site_url: String::new(),
            new_site_user: String::new(),
            new_site_pass: String::new(),

            show_scheduler_modal: false,
            scheduler_start_time: "02:00".to_string(),
            scheduler_stop_time: "06:00".to_string(),
            scheduler_active: false,

            show_progress_modal: false,
            progress_modal_id: None,
            progress_view_connections: false,

            show_about_modal: false,
            show_registration_modal: false,
            reg_first_name: String::new(),
            reg_last_name: String::new(),
            reg_email: String::new(),
            reg_serial: String::new(),

            // Columns Management
            columns: initial_columns.clone(),
            temp_columns: initial_columns.clone(),
            show_columns_modal: false,
            selected_column_idx: Some(0),
            column_width_input: "260".to_string(),

            // Sorting
            sort_column: None,
            sort_ascending: true,

            // Batch Download
            show_batch_download_modal: false,
            batch_url_pattern: "https://example.com/files/img*.jpg".to_string(),
            batch_is_number: true,
            batch_from: 1,
            batch_to: 10,
            batch_wildcard_size: 2,
            batch_auth: false,
            batch_user: String::new(),
            batch_pass: String::new(),

            // Find Dialog
            show_find_modal: false,
            find_query: String::new(),
            find_match_case: false,

            // Speed Limiter Dialog
            show_speed_limiter_modal: false,

            // Site Grabber Wizard
            show_site_grabber_modal: false,
            grabber_step: 1,
            grabber_project_name: "My Project".to_string(),
            grabber_start_url: "https://example.com/gallery".to_string(),
            grabber_save_dir: config.default_download_dir.join("Grabber"),
            grabber_depth: 2,
            grabber_file_filter: 1, // pictures

            // Scheduler & Queues
            scheduler_tab: 0,
            selected_queue_name: "Main download queue".to_string(),
            queues_list: vec!["Main download queue".to_string(), "Synchronization queue".to_string()],
            new_queue_name_input: String::new(),
            show_new_queue_input: false,
            scheduler_periodic: false,
            scheduler_days: [true, true, true, true, true, true, true],
            scheduler_retries: 10,
            scheduler_hang_up: false,
            scheduler_exit_idm: false,
            scheduler_turn_off: false,
            scheduler_force_exit: false,
            scheduler_on_startup: false,
            scheduler_open_file: String::new(),
            scheduler_simultaneous: 1,
            scheduler_selected_id: None,

            // Extra UI
            show_tip_modal: false,
            tip_index: 0,
            show_update_modal: false,
            show_drop_target: false,
            dark_mode: true,
            status_message: None,

            engine,
            next_id: initial_max_id + 1,
            db: db.clone(),
            is_reconciling: true,
            reconcile_rx: Some(rec_rx),
            reconcile_handle: Some(reconcile_handle),
            deleted_task_ids: std::collections::HashSet::new(),
            deleting_task_ids: std::collections::HashSet::new(),
            task_versions: std::collections::HashMap::new(),
            cleanup_errors: std::collections::HashMap::new(),

            screenshot_path,
            frame_counter: 0,
            screenshot_requested: false,
            screenshot_saved: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        app
    }

    pub fn check_background_reconciliation(&mut self) {
        if let Some(ref rx) = self.reconcile_rx {
            if let Ok(res) = rx.try_recv() {
                self.is_reconciling = false;
                self.reconcile_rx = None;
                if let Some(handle) = self.reconcile_handle.take() {
                    let _ = handle.join();
                }
                match res {
                    Ok(items) => {
                        let had_reconciled_items = !items.is_empty();
                        let existing_ids: std::collections::HashSet<u64> =
                            self.downloads.iter().map(|d| d.id).collect();

                        for rec_item in items {
                            // 1. Tombstone check: discard any item whose ID is in deleted_task_ids or deleting_task_ids
                            if self.deleted_task_ids.contains(&rec_item.id)
                                || self.deleting_task_ids.contains(&rec_item.id)
                            {
                                continue;
                            }

                            // 2. Conflict check: existing in-memory download
                            if let Some(existing) = self.downloads.iter_mut().find(|d| d.id == rec_item.id) {
                                let is_mutated = self.task_versions.get(&rec_item.id).copied().unwrap_or(0) > 0
                                    || existing.status == DownloadStatus::Downloading
                                    || existing.status == DownloadStatus::Connecting
                                    || existing.status == DownloadStatus::CleanupPending
                                    || existing.status == DownloadStatus::Canceled
                                    || existing.status == DownloadStatus::Paused;

                                if is_mutated {
                                    // Preserve in-memory mutated state, backfill missing metadata
                                    if existing.total_size.is_none() && rec_item.total_size.is_some() {
                                        existing.total_size = rec_item.total_size;
                                    }
                                    if existing.description.is_empty() && !rec_item.description.is_empty() {
                                        existing.description = rec_item.description;
                                    }
                                    if existing.expected_sha256.is_none() && rec_item.expected_sha256.is_some() {
                                        existing.expected_sha256 = rec_item.expected_sha256;
                                    }
                                    if existing.hash_status == HashVerificationStatus::NotConfigured
                                        && rec_item.hash_status != HashVerificationStatus::NotConfigured
                                    {
                                        existing.hash_status = rec_item.hash_status;
                                    }
                                } else {
                                    // Unmodified: update to reconciled disk state
                                    *existing = rec_item;
                                }
                            } else if !existing_ids.contains(&rec_item.id) {
                                // 3. Disjoint record not yet in self.downloads:
                                // Verify it has not been deleted from SQLite in the meantime
                                let exists_in_db = self.db.download_exists(rec_item.id).unwrap_or(true);
                                if exists_in_db {
                                    self.downloads.push(rec_item);
                                } else {
                                    self.deleted_task_ids.insert(rec_item.id);
                                }
                            }
                        }

                        // 4. Stable sort by ID
                        self.downloads.sort_by_key(|d| d.id);

                        for d in &self.downloads {
                            crate::engine::register_inactive_save_path(d.id, d.save_path.clone());
                        }

                        // 5. Monotonically advance self.next_id
                        let max_id = self.downloads.iter().map(|d| d.id).max().unwrap_or(0);
                        self.next_id = std::cmp::max(self.next_id, max_id + 1);

                        // 6. Retain valid selected_id
                        if self.selected_id.is_none()
                            || !self.downloads.iter().any(|d| Some(d.id) == self.selected_id)
                        {
                            self.selected_id = self.downloads.first().map(|d| d.id);
                        }

                        // 7. Suppress sample download injection unless this is a genuinely clean first run:
                        if self.downloads.is_empty()
                            && self.deleted_task_ids.is_empty()
                            && self.deleting_task_ids.is_empty()
                            && !had_reconciled_items
                            && self.db.get_download_count().ok() == Some(0)
                        {
                            self.populate_default_sample_downloads();
                        }
                    }
                    Err(e) => {
                        eprintln!("Startup reconciliation error: {}", e);
                        self.set_status(format!("Reconciliation warning: {}", e));
                        if self.downloads.is_empty()
                            && self.deleted_task_ids.is_empty()
                            && self.deleting_task_ids.is_empty()
                            && self.db.get_download_count().ok() == Some(0)
                        {
                            self.populate_default_sample_downloads();
                        }
                    }
                }
            }
        }
    }

    fn populate_default_sample_downloads(&mut self) {
        if !self.downloads.is_empty() {
            return;
        }
        // High fidelity sample downloads matching picture-3.png from the original IDM
        let base_time = Local::now();

        let make_date = |days_ago: i64, hours_ago: i64, min_ago: i64| -> DateTime<Local> {
            base_time - chrono::Duration::days(days_ago) - chrono::Duration::hours(hours_ago) - chrono::Duration::minutes(min_ago)
        };

        let segs_active = vec![
            SegmentProgress { segment_index: 0, start_byte: 0, end_byte: 12_500_000, current_byte: 12_500_000, speed: 0.0 },
            SegmentProgress { segment_index: 1, start_byte: 12_500_000, end_byte: 25_000_000, current_byte: 23_100_000, speed: 0.0 },
            SegmentProgress { segment_index: 2, start_byte: 25_000_000, end_byte: 37_500_000, current_byte: 34_800_000, speed: 0.0 },
            SegmentProgress { segment_index: 3, start_byte: 37_500_000, end_byte: 50_000_000, current_byte: 46_200_000, speed: 0.0 },
            SegmentProgress { segment_index: 4, start_byte: 50_000_000, end_byte: 62_500_000, current_byte: 58_900_000, speed: 0.0 },
            SegmentProgress { segment_index: 5, start_byte: 62_500_000, end_byte: 75_000_000, current_byte: 69_400_000, speed: 0.0 },
            SegmentProgress { segment_index: 6, start_byte: 75_000_000, end_byte: 87_500_000, current_byte: 81_200_000, speed: 0.0 },
            SegmentProgress { segment_index: 7, start_byte: 87_500_000, end_byte: 104_857_600, current_byte: 97_600_000, speed: 0.0 },
        ];

        let samples = vec![
            DownloadItem {
                id: 1,
                url: "https://cdimage.ubuntu.com/kubuntu/releases/26.04/release/kubuntu-26.04.1-desktop-amd64.iso".to_string(),
                filename: "kubuntu-26.04. 1-desktop-amd64.iso".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("kubuntu-26.04.1-desktop-amd64.iso"),
                total_size: Some(5_068_062_720), // 4,72 GB
                downloaded_bytes: 5_068_062_720,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(0, 1, 15),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 2,
                url: "https://github.com/flow/flow/releases/download/v1.0.65/Flow_1.0.65_x64-setup.exe".to_string(),
                filename: "Flow_1.0.65_x64-setup.exe".to_string(),
                category: Category::Programs,
                save_path: self.config.default_download_dir.join("Flow_1.0.65_x64-setup.exe"),
                total_size: Some(258_277_376), // 246,31 MB
                downloaded_bytes: 258_277_376,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(1, 4, 30),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 3,
                url: "https://nfsmods.xyz/download/376037-FRSFastTest-NFSC-0.1.0.zip".to_string(),
                filename: "376037-FRSFastTest-NFSC-0.1.0.zip".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("376037-FRSFastTest-NFSC-0.1.0.zip"),
                total_size: Some(1_027_604), // 0,98 MB
                downloaded_bytes: 1_027_604,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(1, 6, 20),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 4,
                url: "https://archive.org/download/nfs_u2/Need_for_Speed_Underground_2_Win_Setup_EN.zip".to_string(),
                filename: "Need_for_Speed_Underground_2_Win_Setup_EN.zip".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("Need_for_Speed_Underground_2_Win_Setup_EN.zip"),
                total_size: Some(1_127_428_915), // 1,05 GB
                downloaded_bytes: 1_127_428_915,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(1, 6, 30),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 5,
                url: "https://portswigger.net/burp/releases/download?product=pro&version=2026.10.1&type=WindowsX64".to_string(),
                filename: "burpsuite_windows-x64_v2026_10_1.exe".to_string(),
                category: Category::Programs,
                save_path: self.config.default_download_dir.join("burpsuite_windows-x64_v2026_10_1.exe"),
                total_size: Some(440_955_863), // 420,53 MB
                downloaded_bytes: 440_955_863,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(2, 2, 45),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 6,
                url: "https://github.com/dineshshetty/Android-InsecureBankv2/releases/download/v2.0/InsecureBankv2.apk".to_string(),
                filename: "InsecureBankv2.apk".to_string(),
                category: Category::Programs,
                save_path: self.config.default_download_dir.join("InsecureBankv2.apk"),
                total_size: Some(3_460_300), // 3,30 MB
                downloaded_bytes: 3_460_300,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(2, 7, 10),
                max_connections: 4,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 7,
                url: "https://github.com/skylot/jadx/releases/download/v1.5.6/jadx-gui-1.5.6-with-jre-win.zip".to_string(),
                filename: "jadx-gui-1.5.6-with-jre-win.zip".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("jadx-gui-1.5.6-with-jre-win.zip"),
                total_size: Some(99_121_889), // 94,53 MB
                downloaded_bytes: 99_121_889,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(2, 7, 20),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 8,
                url: "https://wordcraft.app/download/wordcraft-0.15.2.apk".to_string(),
                filename: "wordcraft-0.15.2.apk".to_string(),
                category: Category::Programs,
                save_path: self.config.default_download_dir.join("wordcraft-0.15.2.apk"),
                total_size: Some(140_727_091), // 134,21 MB
                downloaded_bytes: 140_727_091,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(2, 8, 30),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 9,
                url: "https://releases.ms/ufed/SETUP%20UFED4PC%207.71.0.1881.zip".to_string(),
                filename: "SETUP UFED4PC 7.71.0.1881.zip".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("SETUP UFED4PC 7.71.0.1881.zip"),
                total_size: Some(5_809_000_000), // 5,54 GB
                downloaded_bytes: 5_809_000_000,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(3, 3, 10),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 10,
                url: "https://osdn.net/projects/crystaldiskinfo/downloads/79942/CrystalDiskInfo9_9_4_2.zip".to_string(),
                filename: "CrystalDiskInfo9_9_4_2.zip".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("CrystalDiskInfo9_9_4_2.zip"),
                total_size: Some(8_545_894), // 8,15 MB
                downloaded_bytes: 8_545_894,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(5, 4, 15),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 11,
                url: "https://github.com/hirschmann/nbfc/releases/download/1.6.3/NoteBookFanControl.setup.exe".to_string(),
                filename: "NoteBookFanControl.setup.exe".to_string(),
                category: Category::Programs,
                save_path: self.config.default_download_dir.join("NoteBookFanControl.setup.exe"),
                total_size: Some(2_348_810), // 2,24 MB
                downloaded_bytes: 2_348_810,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(5, 5, 20),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 12,
                url: "https://artcraft.io/bin/artcraft-launcher-0.13.0-windows-x64.msi".to_string(),
                filename: "artcraft-launcher-0.13.0-windows-x64.msi".to_string(),
                category: Category::Programs,
                save_path: self.config.default_download_dir.join("artcraft-launcher-0.13.0-windows-x64.msi"),
                total_size: Some(10_003_415), // 9,54 MB
                downloaded_bytes: 10_003_415,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(5, 10, 40),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 13,
                url: "https://nfsmods.xyz/download/269fc7-EveryBonusMarkerNFSMW_v1.0.0.zip".to_string(),
                filename: "269fc7-EveryBonusMarkerNFSMW_v1.0.0.zip".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("269fc7-EveryBonusMarkerNFSMW_v1.0.0.zip"),
                total_size: Some(686), // 686 Bytes
                downloaded_bytes: 686,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(9, 2, 10),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 14,
                url: "https://archive.org/download/broadcast/Warm_broadcast_by_Olu2K.rar".to_string(),
                filename: "Warm_broadcast_by_Olu2K.rar".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("Warm_broadcast_by_Olu2K.rar"),
                total_size: Some(20_310_917), // 19,37 MB
                downloaded_bytes: 20_310_917,
                status: DownloadStatus::Completed,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(9, 7, 30),
                max_connections: 8,
                segments: vec![],
                description: String::new(),
                queue: None,
                ..Default::default()
            },
            DownloadItem {
                id: 15,
                url: "https://releases.ubuntu.com/noble/ubuntu-24.04-desktop-amd64.iso".to_string(),
                filename: "ubuntu-24.04-desktop-amd64.iso".to_string(),
                category: Category::Compressed,
                save_path: self.config.default_download_dir.join("ubuntu-24.04-desktop-amd64.iso"),
                total_size: Some(104_857_600),
                downloaded_bytes: 85_200_000,
                status: DownloadStatus::Downloading,
                transfer_rate: 6_291_456.0,
                time_left_secs: Some(3),
                resume_capable: true,
                date_added: Local::now() - chrono::Duration::minutes(10),
                max_connections: 8,
                segments: segs_active,
                description: "Linux OS Installation Media".to_string(),
                queue: Some("Main".to_string()),
                ..Default::default()
            },
            DownloadItem {
                id: 16,
                url: "https://archive.org/download/big-buck-bunny/big_buck_bunny_1080p_surround.avi".to_string(),
                filename: "big_buck_bunny_1080p_surround.avi".to_string(),
                category: Category::Video,
                save_path: self.config.default_download_dir.join("big_buck_bunny_1080p_surround.avi"),
                total_size: Some(885_000_000),
                downloaded_bytes: 320_000_000,
                status: DownloadStatus::Paused,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added: make_date(1, 2, 0),
                max_connections: 8,
                segments: vec![],
                description: "Blender Open Movie Animation".to_string(),
                queue: None,
                ..Default::default()
            },
        ];

        self.downloads = samples;
        for item in &mut self.downloads {
            item.last_try_date = Some(item.date_added);
            let host = item.url.split('/').nth(2).unwrap_or("example.com");
            item.parent_page = Some(format!("https://{}/", host));
            item.referer = Some(format!("https://{}/download", host));

            let settings = DownloadSettings {
                download_id: item.id,
                max_connections: item.max_connections,
                chunk_size: 2 * 1024 * 1024,
                speed_limit: None,
                retry_limit: 5,
            };
            if let Err(e) = self.db.insert_download(item, &settings) {
                eprintln!("Database error inserting sample download #{}: {}", item.id, e);
            }
        }
        self.selected_id = Some(15);
        self.next_id = std::cmp::max(self.next_id, 17);
    }

    pub fn poll_engine_events(&mut self) {
        self.check_background_reconciliation();
        while let Ok(evt) = self.engine.event_receiver.try_recv() {
            match evt {
                EngineEvent::MetadataDiscovered {
                    id,
                    filename,
                    total_size,
                    resume_capable,
                    segments,
                } => {
                    if let Err(e) = self.db.update_metadata(id, &filename, total_size, None, None) {
                        eprintln!("Database error updating metadata for #{}: {}", id, e);
                    }
                    if let Err(e) = self.db.update_status(id, &DownloadStatus::Downloading) {
                        eprintln!("Database error updating status for #{}: {}", id, e);
                    }
                    if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                        item.filename = filename;
                        item.total_size = total_size;
                        item.resume_capable = resume_capable;
                        item.segments = segments;
                        item.status = DownloadStatus::Downloading;
                    }
                    self.mark_task_mutated(id);
                }
                EngineEvent::ProgressUpdate {
                    id,
                    downloaded,
                    speed,
                    time_left,
                    segments,
                    connection_segments,
                } => {
                    if self.deleted_task_ids.contains(&id) || self.deleting_task_ids.contains(&id) {
                        continue;
                    }
                    if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                        // Guard against trailing progress events arriving after terminal/paused events
                        if item.status == DownloadStatus::Downloading || item.status == DownloadStatus::Connecting {
                            item.downloaded_bytes = downloaded;
                            if speed > 0.0 {
                                item.transfer_rate = speed;
                                item.time_left_secs = time_left;
                            } else if item.downloaded_bytes == item.total_size.unwrap_or(u64::MAX) {
                                item.transfer_rate = 0.0;
                                item.time_left_secs = Some(0);
                            } else {
                                item.transfer_rate *= 0.85;
                                if item.transfer_rate < 100.0 {
                                    item.transfer_rate = 0.0;
                                    item.time_left_secs = None;
                                }
                            }
                            if !segments.is_empty() {
                                item.segments = segments;
                            }
                            if !connection_segments.is_empty() {
                                item.connection_segments = connection_segments;
                            }
                            self.mark_task_mutated(id);
                        }
                    }
                }
                EngineEvent::Finished { id, hash_status } => {
                    if self.deleted_task_ids.contains(&id) || self.deleting_task_ids.contains(&id) {
                        continue;
                    }
                    // FINDING-4.2-10: Persist verification status with mark_completed_with_hash
                    if let Err(e) = self.db.mark_completed_with_hash(id, Some(&hash_status)) {
                        eprintln!("Database error marking #{}: completed with hash: {}", id, e);
                    }
                    if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                        item.status = DownloadStatus::Completed;
                        item.hash_status = hash_status;
                        item.transfer_rate = 0.0;
                        item.time_left_secs = None;
                        if let Some(total) = item.total_size {
                            item.downloaded_bytes = total;
                        }
                    }
                    self.mark_task_mutated(id);
                    self.set_status(format!("Download #{} completed ({})", id, hash_status.display_text()));
                }
                EngineEvent::Failed { id, error } => {
                    if self.deleted_task_ids.contains(&id) || self.deleting_task_ids.contains(&id) {
                        continue;
                    }
                    if let Err(e) = self.db.mark_failed(id, &error) {
                        eprintln!("Database error marking #{}: failed: {}", id, e);
                    }
                    if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                        item.status = DownloadStatus::Error(error.clone());
                        item.transfer_rate = 0.0;
                        item.time_left_secs = None;
                    }
                    self.mark_task_mutated(id);
                    self.set_status(format!("Download #{} failed: {}", id, error));
                }
                EngineEvent::Paused { id } => {
                    if self.deleted_task_ids.contains(&id) || self.deleting_task_ids.contains(&id) {
                        continue;
                    }
                    if let Err(e) = self.db.update_status(id, &DownloadStatus::Paused) {
                        eprintln!("Database error updating status to Paused for #{}: {}", id, e);
                    }
                    if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                        item.status = DownloadStatus::Paused;
                        item.transfer_rate = 0.0;
                        item.time_left_secs = None;
                    }
                    self.mark_task_mutated(id);
                    self.set_status(format!("Download #{} paused", id));
                }
                EngineEvent::Canceled { id } => {
                    if self.deleted_task_ids.contains(&id) || self.deleting_task_ids.contains(&id) {
                        continue;
                    }
                    if let Err(e) = self.db.update_status(id, &DownloadStatus::Canceled) {
                        eprintln!("Database error updating status to Canceled for #{}: {}", id, e);
                    }
                    if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                        item.status = DownloadStatus::Canceled;
                        item.transfer_rate = 0.0;
                        item.time_left_secs = None;
                    }
                    self.mark_task_mutated(id);
                    self.set_status(format!("Download #{} canceled", id));
                }
                EngineEvent::Deleted {
                    id,
                    cleanup_success,
                    error,
                } => {
                    self.deleting_task_ids.remove(&id);
                    if cleanup_success {
                        if let Err(e) = self.db.delete_download(id) {
                            eprintln!("Database error deleting download #{}: {}", id, e);
                        }
                        self.downloads.retain(|d| d.id != id);
                        self.deleted_task_ids.insert(id);
                        self.task_versions.remove(&id);
                        self.cleanup_errors.remove(&id);
                        if self.selected_id == Some(id) {
                            self.selected_id = self.downloads.first().map(|d| d.id);
                        }
                        self.set_status(format!("Download #{} deleted successfully", id));
                    } else {
                        // FINDING-4.2-08: Filesystem unlinking failed (locked file / OS error):
                        // MUST NOT delete DB record or remove from UI!
                        // Task transitions to DownloadStatus::CleanupPending.
                        let err_text = error.unwrap_or_else(|| "Destination file locked or inaccessible".into());
                        if let Err(e) = self.db.update_status(id, &DownloadStatus::CleanupPending) {
                            eprintln!("Database error updating status to CleanupPending for #{}: {}", id, e);
                        }
                        self.cleanup_errors.insert(id, err_text.clone());
                        if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                            item.status = DownloadStatus::CleanupPending;
                            item.transfer_rate = 0.0;
                            item.time_left_secs = None;
                            item.description = format!("Cleanup failed: {}", err_text);
                        }
                        self.mark_task_mutated(id);
                        self.set_status(format!(
                            "Cleanup failed for download #{}: {}. Click 'Retry Cleanup' to retry.",
                            id, err_text
                        ));
                    }
                }
            }
        }
    }

    pub fn resolve_unique_download_target(&self, desired_filename: &str) -> (String, PathBuf) {
        let default_dir = &self.config.default_download_dir;
        let p = std::path::Path::new(desired_filename);
        let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
        let ext_opt = p.extension().and_then(|s| s.to_str());

        let is_taken = |cand_name: &str, cand_path: &std::path::Path| -> bool {
            let in_downloads = self.downloads.iter().any(|d| {
                d.filename.eq_ignore_ascii_case(cand_name) || d.save_path == cand_path
            });
            if in_downloads {
                return true;
            }
            cand_path.exists()
        };

        let initial_path = default_dir.join(desired_filename);
        if !is_taken(desired_filename, &initial_path) {
            return (desired_filename.to_string(), initial_path);
        }

        let mut counter = 1;
        loop {
            let cand_name = match ext_opt {
                Some(e) => format!("{}_{}.{}", stem, counter, e),
                None => format!("{}_{}", stem, counter),
            };
            let cand_path = default_dir.join(&cand_name);
            if !is_taken(&cand_name, &cand_path) {
                return (cand_name, cand_path);
            }
            counter += 1;
        }
    }

    pub fn resume_selected(&mut self) {
        if let Some(id) = self.selected_id {
            if let Some(idx) = self.downloads.iter().position(|d| d.id == id) {
                if self.downloads[idx].status == DownloadStatus::CleanupPending {
                    self.set_status(format!("Cannot resume #{}: file cleanup is pending", id));
                    return;
                }
                if self.downloads[idx].status != DownloadStatus::Completed
                    && self.downloads[idx].status != DownloadStatus::Downloading
                {
                    // If another download currently shares the exact same save_path, disambiguate this one
                    let cur_path = self.downloads[idx].save_path.clone();
                    let has_duplicate_path = self.downloads.iter().any(|d| d.id != id && d.save_path == cur_path);
                    if has_duplicate_path {
                        let (new_fname, new_path) = self.resolve_unique_download_target(&self.downloads[idx].filename);
                        self.downloads[idx].filename = new_fname.clone();
                        self.downloads[idx].save_path = new_path.clone();
                        let mut new_partinfo = new_path.clone();
                        new_partinfo.set_file_name(format!("{}.partinfo", new_fname));
                        if !new_partinfo.exists() {
                            self.downloads[idx].downloaded_bytes = 0;
                            self.downloads[idx].segments.clear();
                        }
                        if let Err(e) = self.db.update_save_path(id, &new_fname, &new_path) {
                            eprintln!("Database error updating save path for #{}: {}", id, e);
                        }
                    }

                    self.downloads[idx].status = DownloadStatus::Connecting;
                    let url = self.downloads[idx].url.clone();
                    let save_path = self.downloads[idx].save_path.clone();
                    let connections = self.downloads[idx].max_connections;

                    self.mark_task_mutated(id);

                    if let Err(e) = self.db.update_status(id, &DownloadStatus::Downloading) {
                        eprintln!("Database error updating status for #{}: {}", id, e);
                    }
                    if let Err(e) = self.engine.cmd_sender.send(EngineCommand::StartDownload {
                        id,
                        url,
                        save_path,
                        connections,
                    }) {
                        eprintln!("Engine error sending StartDownload for #{}: {}", id, e);
                    }
                }
            }
        }
    }

    pub fn pause_selected(&mut self) {
        if let Some(id) = self.selected_id {
            if let Some(idx) = self.downloads.iter().position(|d| d.id == id) {
                if self.downloads[idx].status == DownloadStatus::CleanupPending {
                    return;
                }
                if self.downloads[idx].status == DownloadStatus::Downloading
                    || self.downloads[idx].status == DownloadStatus::Connecting
                {
                    self.downloads[idx].status = DownloadStatus::Paused;
                    self.downloads[idx].transfer_rate = 0.0;
                    self.downloads[idx].time_left_secs = None;

                    self.mark_task_mutated(id);

                    if let Err(e) = self.db.update_status(id, &DownloadStatus::Paused) {
                        eprintln!("Database error updating status for #{}: {}", id, e);
                    }
                    if let Err(e) = self.engine.cmd_sender.send(EngineCommand::PauseDownload { id }) {
                        eprintln!("Engine error sending PauseDownload for #{}: {}", id, e);
                    }
                }
            }
        }
    }

    pub fn cancel_selected(&mut self) {
        if let Some(id) = self.selected_id {
            let mut info = None;
            if let Some(idx) = self.downloads.iter().position(|d| d.id == id) {
                if self.downloads[idx].status == DownloadStatus::CleanupPending {
                    return;
                }
                self.downloads[idx].status = DownloadStatus::Canceled;
                self.downloads[idx].transfer_rate = 0.0;
                self.downloads[idx].time_left_secs = None;
                info = Some(self.downloads[idx].filename.clone());

                self.mark_task_mutated(id);

                if let Err(e) = self.db.update_status(id, &DownloadStatus::Canceled) {
                    eprintln!("Database error updating status for #{}: {}", id, e);
                }
                if let Err(e) = self.engine.cmd_sender.send(EngineCommand::CancelDownload { id }) {
                    eprintln!("Engine error sending CancelDownload for #{}: {}", id, e);
                }
            }
            if let Some(filename) = info {
                self.set_status(format!("Canceled download #{} ({})", id, filename));
            }
        }
    }

    pub fn delete_selected(&mut self) {
        if let Some(id) = self.selected_id {
            if let Some(item) = self.downloads.iter().find(|d| d.id == id) {
                crate::engine::register_inactive_save_path(id, item.save_path.clone());
            }
            self.deleting_task_ids.insert(id);
            self.mark_task_mutated(id);
            // Signal engine to stop leases, await workers & writer actor, suppress checkpoints, and clean up files
            if let Err(e) = self.engine.cmd_sender.send(EngineCommand::DeleteDownload { id, cleanup_files: true }) {
                eprintln!("Engine error sending DeleteDownload for #{}: {}", id, e);
            }
            // Mark visually as Deleting while engine confirms file teardown
            if let Some(idx) = self.downloads.iter().position(|d| d.id == id) {
                self.downloads[idx].status = DownloadStatus::Error("Deleting...".into());
                self.downloads[idx].transfer_rate = 0.0;
                self.downloads[idx].time_left_secs = None;
            }
            self.set_status(format!("Deleting download #{}...", id));
        }
    }

    pub fn retry_cleanup_selected(&mut self) {
        if let Some(id) = self.selected_id {
            self.retry_cleanup(id);
        }
    }

    pub fn retry_cleanup(&mut self, id: u64) {
        if let Some(idx) = self.downloads.iter().position(|d| d.id == id) {
            crate::engine::register_inactive_save_path(id, self.downloads[idx].save_path.clone());
            if self.downloads[idx].status == DownloadStatus::CleanupPending {
                self.deleting_task_ids.insert(id);
                self.mark_task_mutated(id);
                self.downloads[idx].status = DownloadStatus::Error("Cleaning up...".into());
                self.downloads[idx].transfer_rate = 0.0;
                self.downloads[idx].time_left_secs = None;
                self.set_status(format!("Retrying file cleanup for download #{}...", id));
                if let Err(e) = self.engine.cmd_sender.send(EngineCommand::DeleteDownload {
                    id,
                    cleanup_files: true,
                }) {
                    eprintln!("Engine error sending DeleteDownload retry for #{}: {}", id, e);
                    self.set_status(format!("Engine error retrying cleanup for #{}: {}", id, e));
                    if let Some(idx2) = self.downloads.iter().position(|d| d.id == id) {
                        self.downloads[idx2].status = DownloadStatus::CleanupPending;
                    }
                    self.deleting_task_ids.remove(&id);
                }
            }
        }
    }

    pub fn delete_completed(&mut self) {
        for d in &self.downloads {
            if d.status == DownloadStatus::Completed {
                self.deleted_task_ids.insert(d.id);
                self.task_versions.remove(&d.id);
            }
        }

        // Optimize delete_completed() using single atomic SQL query
        match self.db.delete_completed_downloads() {
            Ok(count) => {
                self.downloads.retain(|d| d.status != DownloadStatus::Completed);
                self.selected_id = self.downloads.first().map(|d| d.id);
                self.set_status(format!("Removed {} completed download(s) from history", count));
            }
            Err(e) => {
                eprintln!("Database error deleting completed downloads: {}", e);
                self.set_status(format!("Database error deleting completed downloads: {}", e));
            }
        }
    }

    pub fn set_status(&mut self, msg: String) {
        self.status_message = Some((msg, std::time::Instant::now()));
    }

    pub fn redownload_selected(&mut self) {
        if let Some(id) = self.selected_id {
            if let Some(idx) = self.downloads.iter().position(|d| d.id == id) {
                if self.downloads[idx].status == DownloadStatus::CleanupPending {
                    self.set_status(format!("Cannot redownload #{}: file cleanup is pending", id));
                    return;
                }
                self.downloads[idx].downloaded_bytes = 0;
                self.downloads[idx].status = DownloadStatus::Connecting;
                self.downloads[idx].transfer_rate = 0.0;
                self.downloads[idx].time_left_secs = None;
                let fname = self.downloads[idx].filename.clone();
                let url = self.downloads[idx].url.clone();
                let save_path = self.downloads[idx].save_path.clone();
                let connections = self.downloads[idx].max_connections;

                self.mark_task_mutated(id);

                if let Err(e) = self.db.update_status(id, &DownloadStatus::Downloading) {
                    eprintln!("Database error updating status for #{}: {}", id, e);
                }
                if let Err(e) = self.engine.cmd_sender.send(EngineCommand::StartDownload {
                    id,
                    url,
                    save_path,
                    connections,
                }) {
                    eprintln!("Engine error sending StartDownload for #{}: {}", id, e);
                }
                self.set_status(format!("Redownloading {}", fname));
            }
        }
    }

    pub fn add_batch_from_clipboard(&mut self) {
        #[cfg(target_os = "windows")]
        let text = match std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", "Get-Clipboard"])
            .output()
        {
            Ok(out) => String::from_utf8_lossy(&out.stdout).to_string(),
            Err(_) => String::new(),
        };
        #[cfg(not(target_os = "windows"))]
        let text = String::new();

        let mut added = 0;
        for line in text.lines() {
            let line = line.trim();
            if (line.starts_with("http://") || line.starts_with("https://")) && line.len() > 10 {
                let id = self.next_id;
                self.next_id += 1;
                let raw_filename = line.split('/').last().unwrap_or("batch_item.dat").split('?').next().unwrap_or("batch_item.dat");
                let (filename, save_path) = self.resolve_unique_download_target(raw_filename);
                let ext = filename.split('.').last().unwrap_or("");
                let cat = Category::from_extension(ext);
                let item = DownloadItem {
                    id,
                    url: line.to_string(),
                    filename,
                    category: cat,
                    save_path,
                    total_size: None,
                    downloaded_bytes: 0,
                    status: DownloadStatus::Paused,
                    transfer_rate: 0.0,
                    date_added: chrono::Local::now(),
                    max_connections: self.config.default_connections,
                    queue: Some("Main".to_string()),
                    ..Default::default()
                };
                let settings = DownloadSettings {
                    download_id: id,
                    max_connections: self.config.default_connections,
                    chunk_size: 2 * 1024 * 1024,
                    speed_limit: None,
                    retry_limit: 5,
                };
                if let Err(e) = self.db.insert_download(&item, &settings) {
                    eprintln!("Database error inserting download from clipboard: {}", e);
                }
                self.mark_task_mutated(item.id);
                self.downloads.push(item);
                added += 1;
            }
        }
        if added > 0 {
            self.set_status(format!("Added {} downloads from clipboard", added));
        } else {
            self.set_status("No valid URLs found in clipboard".to_string());
        }
    }

    pub fn export_to_file(&mut self, is_idm: bool) {
        let out_path = if is_idm {
            self.config.default_download_dir.join("idm_export.ef2")
        } else {
            self.config.default_download_dir.join("idm_export.txt")
        };
        let mut content = String::new();
        if is_idm {
            content.push_str("<IDM_EXPORT_FILE>\n");
            for d in &self.downloads {
                content.push_str(&format!("<ITEM>\nurl={}\nfile={}\nsave_to={}\n</ITEM>\n", d.url, d.filename, d.save_path.display()));
            }
        } else {
            for d in &self.downloads {
                content.push_str(&format!("{}\n", d.url));
            }
        }
        if let Ok(_) = std::fs::write(&out_path, content) {
            self.set_status(format!("Exported {} downloads to {}", self.downloads.len(), out_path.display()));
        }
    }

    pub fn import_from_file(&mut self) {
        let idm_file = self.config.default_download_dir.join("idm_export.ef2");
        let txt_file = self.config.default_download_dir.join("idm_export.txt");
        let target = if idm_file.exists() {
            Some(idm_file)
        } else if txt_file.exists() {
            Some(txt_file)
        } else {
            None
        };

        if let Some(path) = target {
            if let Ok(content) = std::fs::read_to_string(&path) {
                let mut count = 0;
                for line in content.lines() {
                    let line = line.trim();
                    let url = if line.starts_with("url=") {
                        line.trim_start_matches("url=")
                    } else if line.starts_with("http://") || line.starts_with("https://") {
                        line
                    } else {
                        continue;
                    };
                    let id = self.next_id;
                    self.next_id += 1;
                    let filename = url.split('/').last().unwrap_or("import_file.dat").split('?').next().unwrap_or("import_file.dat").to_string();
                    let ext = filename.split('.').last().unwrap_or("");
                    let cat = Category::from_extension(ext);
                    let save_path = self.config.default_download_dir.join(&filename);
                    let item = DownloadItem {
                        id,
                        url: url.to_string(),
                        filename,
                        category: cat,
                        save_path,
                        total_size: None,
                        downloaded_bytes: 0,
                        status: DownloadStatus::Paused,
                        transfer_rate: 0.0,
                        date_added: chrono::Local::now(),
                        max_connections: self.config.default_connections,
                        queue: Some("Main".to_string()),
                        ..Default::default()
                    };
                    let settings = DownloadSettings {
                        download_id: id,
                        max_connections: self.config.default_connections,
                        chunk_size: 2 * 1024 * 1024,
                        speed_limit: None,
                        retry_limit: 5,
                    };
                    if let Err(e) = self.db.insert_download(&item, &settings) {
                        eprintln!("Database error inserting imported download: {}", e);
                    }
                    self.mark_task_mutated(item.id);
                    self.downloads.push(item);
                    count += 1;
                }
                self.set_status(format!("Imported {} downloads from {}", count, path.display()));
                return;
            }
        }
        self.set_status("No export file found in default download folder".to_string());
    }

    pub fn find_next(&mut self) {
        if self.find_query.is_empty() {
            self.show_find_modal = true;
            return;
        }
        let query = if self.find_match_case {
            self.find_query.clone()
        } else {
            self.find_query.to_lowercase()
        };

        let start_idx = if let Some(sel_id) = self.selected_id {
            self.downloads.iter().position(|d| d.id == sel_id).map(|i| i + 1).unwrap_or(0)
        } else {
            0
        };

        let len = self.downloads.len();
        if len == 0 {
            return;
        }

        for step in 0..len {
            let idx = (start_idx + step) % len;
            let item = &self.downloads[idx];
            let name_match = if self.find_match_case {
                item.filename.contains(&query) || item.url.contains(&query)
            } else {
                item.filename.to_lowercase().contains(&query) || item.url.to_lowercase().contains(&query)
            };
            if name_match {
                self.selected_id = Some(item.id);
                self.set_status(format!("Found match: {}", item.filename));
                return;
            }
        }
        self.set_status(format!("No match found for '{}'", self.find_query));
    }

    pub fn start_queue(&mut self, queue_name: &str) {
        let mut started = 0;
        let mut dispatched_paths: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        for d in &mut self.downloads {
            let matches = match (d.queue.as_deref(), queue_name) {
                (Some("Main"), "Main") => true,
                (Some("Sync"), "Sync") => true,
                (None, "Main") => true,
                _ => false,
            };
            if matches && (d.status == DownloadStatus::Paused || d.status == DownloadStatus::Connecting) {
                if dispatched_paths.contains(&d.save_path) {
                    continue;
                }
                dispatched_paths.insert(d.save_path.clone());
                d.status = DownloadStatus::Connecting;
                let _ = self.engine.cmd_sender.send(EngineCommand::StartDownload {
                    id: d.id,
                    url: d.url.clone(),
                    save_path: d.save_path.clone(),
                    connections: d.max_connections,
                });
                started += 1;
            }
        }
        self.set_status(format!("Queue '{}': started {} download(s)", queue_name, started));
    }

    pub fn stop_queue(&mut self, queue_name: &str) {
        let mut stopped = 0;
        for d in &mut self.downloads {
            let matches = match (d.queue.as_deref(), queue_name) {
                (Some("Main"), "Main") => true,
                (Some("Sync"), "Sync") => true,
                (None, "Main") => true,
                _ => false,
            };
            if matches && (d.status == DownloadStatus::Downloading || d.status == DownloadStatus::Connecting) {
                d.status = DownloadStatus::Paused;
                let _ = self.engine.cmd_sender.send(EngineCommand::PauseDownload { id: d.id });
                stopped += 1;
            }
        }
        self.set_status(format!("Queue '{}': stopped {} download(s)", queue_name, stopped));
    }
}

impl Drop for IdmApp {
    fn drop(&mut self) {
        if let Some(handle) = self.reconcile_handle.take() {
            let _ = handle.join();
        }
    }
}

impl eframe::App for IdmApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if self.dark_mode {
            ui.ctx().set_visuals(egui::Visuals::dark());
        } else {
            ui.ctx().set_visuals(egui::Visuals::light());
        }

        self.poll_engine_events();
        if self.is_reconciling {
            ui.ctx().request_repaint_after(std::time::Duration::from_millis(50));
        }

        // Screenshot capture logic
        if self.screenshot_path.is_some() {
            self.frame_counter += 1;
            ui.ctx().request_repaint();

            if self.frame_counter >= 120 && !self.screenshot_requested {
                self.screenshot_requested = true;
                let path_clone = self.screenshot_path.clone().unwrap();
                let saved_flag = self.screenshot_saved.clone();

                ui.ctx().request_screenshot(move |image: Arc<egui::ColorImage>| {
                    let width = image.width() as u32;
                    let height = image.height() as u32;
                    let mut rgba_bytes = Vec::with_capacity(image.pixels.len() * 4);
                    for color in &image.pixels {
                        rgba_bytes.push(color.r());
                        rgba_bytes.push(color.g());
                        rgba_bytes.push(color.b());
                        rgba_bytes.push(color.a());
                    }

                    if let Some(img) = image::RgbaImage::from_raw(width, height, rgba_bytes) {
                        if let Some(parent) = path_clone.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        if let Err(e) = img.save(&path_clone) {
                            eprintln!("Failed to save screenshot: {}", e);
                        } else {
                            println!("Screenshot saved successfully to {:?}", path_clone);
                            saved_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                });
            }

            if self.screenshot_saved.load(std::sync::atomic::Ordering::SeqCst) {
                ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
            }
        }

        // Top Menu Bar: Tasks  File  Downloads  View  Help  Registration
        Panel::top("top_menu_bar").show(ui, |ui| {
            egui::MenuBar::new().ui(ui, |ui| {
                // 1. Tasks
                ui.menu_button("Tasks", |ui| {
                    if ui.button("Add new download... (Ctrl+N)").clicked() {
                        self.show_add_url_modal = true;
                        ui.close();
                    }
                    if ui.button("Add batch download...").clicked() {
                        self.show_batch_download_modal = true;
                        ui.close();
                    }
                    if ui.button("Add batch download from clipboard").clicked() {
                        self.add_batch_from_clipboard();
                        ui.close();
                    }
                    if ui.button("Run site grabber...").clicked() {
                        self.show_site_grabber_modal = true;
                        self.grabber_step = 1;
                        ui.close();
                    }
                    ui.separator();
                    if ui.checkbox(&mut self.show_drop_target, "Show drop target").clicked() {
                        ui.close();
                    }
                    ui.separator();
                    ui.menu_button("Export", |ui| {
                        if ui.button("To IDM export file...").clicked() {
                            self.export_to_file(true);
                            ui.close();
                        }
                        if ui.button("To text file...").clicked() {
                            self.export_to_file(false);
                            ui.close();
                        }
                    });
                    ui.menu_button("Import", |ui| {
                        if ui.button("From IDM export file...").clicked() {
                            self.import_from_file();
                            ui.close();
                        }
                        if ui.button("From text file...").clicked() {
                            self.import_from_file();
                            ui.close();
                        }
                    });
                    ui.separator();
                    if ui.button("Exit").clicked() {
                        ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });

                // 2. File
                ui.menu_button("File", |ui| {
                    let has_sel = self.selected_id.is_some();
                    if ui.add_enabled(has_sel, egui::Button::new("Stop Download")).clicked() {
                        self.pause_selected();
                        ui.close();
                    }
                    if ui.add_enabled(has_sel, egui::Button::new("Cancel Download")).clicked() {
                        self.cancel_selected();
                        ui.close();
                    }
                    if ui.add_enabled(has_sel, egui::Button::new("Remove")).clicked() {
                        self.delete_selected();
                        ui.close();
                    }
                    if ui.add_enabled(has_sel, egui::Button::new("Download Now")).clicked() {
                        self.resume_selected();
                        ui.close();
                    }
                    if ui.add_enabled(has_sel, egui::Button::new("Redownload")).clicked() {
                        self.redownload_selected();
                        ui.close();
                    }
                    ui.separator();
                    if ui.add_enabled(has_sel, egui::Button::new("Open File Location")).clicked() {
                        if let Some(id) = self.selected_id {
                            if let Some(item) = self.downloads.iter().find(|d| d.id == id) {
                                #[cfg(target_os = "windows")]
                                {
                                    let _ = std::process::Command::new("explorer").arg(&item.save_path).spawn();
                                }
                            }
                        }
                        ui.close();
                    }
                });

                // 3. Downloads
                ui.menu_button("Downloads", |ui| {
                    if ui.button("Pause All").clicked() {
                        for d in &mut self.downloads {
                            if d.status == DownloadStatus::Downloading || d.status == DownloadStatus::Connecting {
                                d.status = DownloadStatus::Paused;
                                if let Err(e) = self.db.update_status(d.id, &DownloadStatus::Paused) {
                                    eprintln!("Database error pausing download #{}: {}", d.id, e);
                                }
                                if let Err(e) = self.engine.cmd_sender.send(EngineCommand::PauseDownload { id: d.id }) {
                                    eprintln!("Engine error sending PauseDownload for #{}: {}", d.id, e);
                                }
                            }
                        }
                        ui.close();
                    }
                    if ui.button("Stop All").clicked() {
                        for d in &mut self.downloads {
                            if d.status == DownloadStatus::Downloading || d.status == DownloadStatus::Connecting {
                                d.status = DownloadStatus::Paused;
                                if let Err(e) = self.db.update_status(d.id, &DownloadStatus::Paused) {
                                    eprintln!("Database error stopping download #{}: {}", d.id, e);
                                }
                                if let Err(e) = self.engine.cmd_sender.send(EngineCommand::PauseDownload { id: d.id }) {
                                    eprintln!("Engine error sending PauseDownload for #{}: {}", d.id, e);
                                }
                            }
                        }
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("Delete All Completed").clicked() {
                        self.delete_completed();
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("Find (Ctrl-F)...").clicked() {
                        self.show_find_modal = true;
                        ui.close();
                    }
                    if ui.button("Find Next (F3)").clicked() {
                        self.find_next();
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("Scheduler...").clicked() {
                        self.show_scheduler_modal = true;
                        ui.close();
                    }
                    ui.menu_button("Start queue", |ui| {
                        if ui.button("Main download queue").clicked() {
                            self.start_queue("Main");
                            ui.close();
                        }
                        if ui.button("Synchronization queue").clicked() {
                            self.start_queue("Sync");
                            ui.close();
                        }
                    });
                    ui.menu_button("Stop queue", |ui| {
                        if ui.button("Main download queue").clicked() {
                            self.stop_queue("Main");
                            ui.close();
                        }
                        if ui.button("Synchronization queue").clicked() {
                            self.stop_queue("Sync");
                            ui.close();
                        }
                    });
                    ui.separator();
                    ui.menu_button("Speed Limiter", |ui| {
                        if ui.button("Turn on").clicked() {
                            self.config.speed_limiter_enabled = true;
                            ui.close();
                        }
                        if ui.button("Turn off").clicked() {
                            self.config.speed_limiter_enabled = false;
                            ui.close();
                        }
                        ui.separator();
                        if ui.button("Settings...").clicked() {
                            self.show_speed_limiter_modal = true;
                            ui.close();
                        }
                    });
                    ui.separator();
                    if ui.button("Options...").clicked() {
                        self.show_options_modal = true;
                        ui.close();
                    }
                });

                // 4. View
                ui.menu_button("View", |ui| {
                    let cat_text = if self.categories_visible { "Hide categories" } else { "Show categories" };
                    if ui.button(cat_text).clicked() {
                        self.categories_visible = !self.categories_visible;
                        ui.close();
                    }
                    ui.menu_button("Arrange files", |ui| {
                        let sorts = [
                            ("By Order Of Addition", None),
                            ("By File Name", Some("filename")),
                            ("By Size", Some("size")),
                            ("By Status", Some("status")),
                            ("By Time Left", Some("time_left")),
                            ("By Transfer Rate", Some("transfer_rate")),
                            ("By Last Try Date", Some("last_try_date")),
                            ("By Description", Some("description")),
                            ("By Save Path", Some("save_to")),
                            ("By Referer", Some("referer")),
                            ("By Parent Web Page", Some("parent_page")),
                        ];
                        for (label, col) in sorts {
                            if ui.button(label).clicked() {
                                self.sort_column = col.map(|s| s.to_string());
                                self.sort_ascending = true;
                                ui.close();
                            }
                        }
                    });
                    ui.menu_button("Toolbar", |ui| {
                        if ui.button("Customize...").clicked() {
                            ui.close();
                        }
                        ui.separator();
                        if ui.button("Large Buttons").clicked() {
                            ui.close();
                        }
                        if ui.button("Small Buttons").clicked() {
                            ui.close();
                        }
                    });
                    ui.menu_button("IDM tray icon", |ui| {
                        let _ = ui.selectable_label(true, "3D style");
                        let _ = ui.selectable_label(false, "Classic style");
                        let _ = ui.selectable_label(false, "Don't show");
                    });
                    ui.separator();
                    if ui.button("Customize URL List...").clicked() {
                        self.temp_columns = self.columns.clone();
                        self.selected_column_idx = Some(0);
                        self.column_width_input = format!("{:.0}", self.columns.first().map(|c| c.width).unwrap_or(260.0));
                        self.show_columns_modal = true;
                        ui.close();
                    }
                    if ui.button(if self.dark_mode { "Switch to Light Theme" } else { "Switch to Dark Theme" }).clicked() {
                        self.dark_mode = !self.dark_mode;
                        ui.close();
                    }
                    ui.menu_button("Font", |ui| {
                        if ui.button("Reset to default Font").clicked() {
                            ui.close();
                        }
                    });
                    ui.separator();
                    ui.menu_button("Language", |ui| {
                        let _ = ui.selectable_label(true, "English");
                        let _ = ui.selectable_label(false, "Indonesian (Bahasa Indonesia)");
                    });
                });

                // 5. Help
                ui.menu_button("Help", |ui| {
                    if ui.button("Help contents").clicked() {
                        ui.close();
                    }
                    if ui.button("Tutorials").clicked() {
                        ui.close();
                    }
                    if ui.button("Scheduler and queues").clicked() {
                        self.show_scheduler_modal = true;
                        ui.close();
                    }
                    if ui.button("Grabber Help").clicked() {
                        ui.close();
                    }
                    if ui.button("Tip of the Day...").clicked() {
                        self.show_tip_modal = true;
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("IDM Home Page").clicked() {
                        ui.close();
                    }
                    if ui.button("Contact IDM Support").clicked() {
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("Check for updates...").clicked() {
                        self.show_update_modal = true;
                        ui.close();
                    }
                    ui.separator();
                    if ui.button("About IDM").clicked() {
                        self.show_about_modal = true;
                        ui.close();
                    }
                    if ui.button("Tell a friend").clicked() {
                        self.show_about_modal = true;
                        ui.close();
                    }
                });

                // 6. Registration
                ui.menu_button("Registration", |ui| {
                    if ui.button("Order online").clicked() {
                        ui.close();
                    }
                    if ui.button("Registration...").clicked() {
                        self.show_registration_modal = true;
                        ui.close();
                    }
                });
            });
        });

        // Top Toolbar (Matching picture-3.png layout & icon style)
        Panel::top("top_toolbar").show(ui, |ui| {
            ui.add_space(3.0);
            ui.horizontal(|ui| {
                ui.style_mut().spacing.item_spacing = Vec2::new(4.0, 0.0);

                // Add URL button
                let add_url_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/url.svg")).max_size(Vec2::new(16.0, 16.0)),
                    RichText::new("Add URL").strong(),
                );
                if ui.add(add_url_btn).clicked() {
                    self.show_add_url_modal = true;
                }

                // Resume button
                let resume_enabled = self.selected_id.is_some();
                let resume_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-download.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Resume",
                );
                if ui.add_enabled(resume_enabled, resume_btn).clicked() {
                    self.resume_selected();
                }

                // Stop button
                let stop_enabled = self.selected_id.is_some();
                let stop_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-error.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Stop",
                );
                if ui.add_enabled(stop_enabled, stop_btn).clicked() {
                    self.pause_selected();
                }

                // Stop All button
                let stop_all_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-error.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Stop All",
                );
                if ui.add(stop_all_btn).clicked() {
                    for d in &mut self.downloads {
                        if d.status == DownloadStatus::Downloading || d.status == DownloadStatus::Connecting {
                            d.status = DownloadStatus::Paused;
                            if let Err(e) = self.db.update_status(d.id, &DownloadStatus::Paused) {
                                eprintln!("Database error stopping download #{}: {}", d.id, e);
                            }
                            if let Err(e) = self.engine.cmd_sender.send(EngineCommand::PauseDownload { id: d.id }) {
                                eprintln!("Engine error sending PauseDownload for #{}: {}", d.id, e);
                            }
                        }
                    }
                }

                // Delete button
                let delete_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-trash.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Delete",
                );
                if ui.add_enabled(self.selected_id.is_some(), delete_btn).clicked() {
                    self.delete_selected();
                }

                // Explicit "Retry Cleanup" warning button when selected download is in CleanupPending
                let is_cleanup_pending = self.selected_id
                    .and_then(|id| self.downloads.iter().find(|d| d.id == id))
                    .map(|d| d.status == DownloadStatus::CleanupPending)
                    .unwrap_or(false);

                if is_cleanup_pending {
                    let retry_btn = egui::Button::image_and_text(
                        egui::Image::new(egui::include_image!("../assets/icons/alert-triangle.svg")).max_size(Vec2::new(14.0, 14.0)),
                        RichText::new("Retry Cleanup")
                            .color(Color32::WHITE)
                            .strong(),
                    ).fill(Color32::from_rgb(230, 110, 0));
                    if ui.add(retry_btn).clicked() {
                        self.retry_cleanup_selected();
                    }
                }

                // Delete Completed button
                let del_c_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-trash.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Delete C...",
                );
                if ui.add(del_c_btn).clicked() {
                    self.delete_completed();
                }

                // Options button
                let opt_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/settings.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Options",
                );
                if ui.add(opt_btn).clicked() {
                    self.show_options_modal = true;
                }

                // Scheduler button
                let sched_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-tasks.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Scheduler",
                );
                if ui.add(sched_btn).clicked() {
                    self.show_scheduler_modal = true;
                }

                // Start Queue button
                let start_q_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-queue.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Start Qu...",
                );
                if ui.add(start_q_btn).clicked() {
                    let mut dispatched_paths: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
                    for d in &mut self.downloads {
                        if d.status == DownloadStatus::Queued || d.status == DownloadStatus::Paused {
                            if dispatched_paths.contains(&d.save_path) {
                                continue;
                            }
                            dispatched_paths.insert(d.save_path.clone());
                            d.status = DownloadStatus::Connecting;
                            let _ = self.engine.cmd_sender.send(EngineCommand::StartDownload {
                                id: d.id,
                                url: d.url.clone(),
                                save_path: d.save_path.clone(),
                                connections: d.max_connections,
                            });
                        }
                    }
                }

                // Stop Queue button
                let stop_q_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/folder-error.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Stop Qu...",
                );
                if ui.add(stop_q_btn).clicked() {
                    for d in &mut self.downloads {
                        if d.queue.is_some() && d.status == DownloadStatus::Downloading {
                            d.status = DownloadStatus::Paused;
                            let _ = self.engine.cmd_sender.send(EngineCommand::PauseDownload { id: d.id });
                        }
                    }
                }

                // Tell a Friend
                let tell_btn = egui::Button::image_and_text(
                    egui::Image::new(egui::include_image!("../assets/icons/email.svg")).max_size(Vec2::new(16.0, 16.0)),
                    "Tell a Frie...",
                );
                if ui.add(tell_btn).clicked() {
                    self.show_about_modal = true;
                }

                ui.separator();

                // Quick Speed Limiter
                ui.checkbox(&mut self.config.speed_limiter_enabled, "Speed Limit:");
                if self.config.speed_limiter_enabled {
                    ui.add(egui::DragValue::new(&mut self.config.speed_limit_kbps).speed(10).range(10..=100_000));
                    ui.label("KB/s");
                }
            });
            ui.add_space(4.0);
        });

        // Bottom Status Bar
        Panel::bottom("status_bar").show(ui, |ui| {
            ui.horizontal(|ui| {
                let total_items = self.downloads.len();
                let active_items = self.downloads.iter().filter(|d| d.status == DownloadStatus::Downloading).count();
                let total_speed: f64 = self.downloads.iter().map(|d| d.transfer_rate).sum();

                ui.label(format!("Category: {} | Items: {}", self.selected_category.name(), total_items));
                ui.separator();
                ui.label(format!("Active Downloads: {}", active_items));
                ui.separator();
                ui.label(format!("Total Speed: {}/s", format_bytes(total_speed as u64)));
                ui.separator();
                if self.config.speed_limiter_enabled {
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 4.0;
                        ui.add(egui::Image::new(egui::include_image!("../assets/icons/zap.svg")).tint(Color32::from_rgb(255, 180, 0)).max_size(Vec2::new(12.0, 12.0)));
                        ui.colored_label(Color32::from_rgb(255, 180, 0), format!("Limiter: {} KB/s", self.config.speed_limit_kbps));
                    });
                } else {
                    ui.label("Limiter: Off");
                }
                if self.is_reconciling {
                    ui.separator();
                    ui.spinner();
                    ui.colored_label(Color32::from_rgb(255, 180, 0), "Reconciling database & checkpoints...");
                } else if let Some((ref msg, instant)) = self.status_message {
                    if instant.elapsed().as_secs() < 5 {
                        ui.separator();
                        ui.colored_label(Color32::from_rgb(0, 120, 215), msg);
                    }
                }
            });
        });

        // Left Sidebar: Categories Panel (Exact replica of picture-3.png)
        if self.categories_visible {
            Panel::left("left_categories_sidebar")
                .resizable(true)
                .default_size(175.0)
                .min_size(130.0)
                .max_size(260.0)
                .show(ui, |ui| {
                    // Header with title and close button
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Categories").strong());
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let btn_close = egui::Button::image(
                                egui::Image::new(egui::include_image!("../assets/icons/x.svg")).max_size(Vec2::new(10.0, 10.0))
                            ).frame(false);
                            if ui.add(btn_close).on_hover_text("Close categories").clicked() {
                                self.categories_visible = false;
                            }
                        });
                    });
                    ui.separator();

                    egui::ScrollArea::vertical().id_salt("sidebar_categories_scroll").auto_shrink([false, false]).show(ui, |ui| {
                        // 1. All Downloads Node (Collapsible Tree with chevron)
                        let all_count = self.downloads.len();
                        let is_all_sel = self.selected_category == Category::All && self.status_filter.is_none() && self.queue_filter.is_none();
                        let chevron_all = if self.tree_all_expanded {
                            egui::include_image!("../assets/icons/chevron-down.svg")
                        } else {
                            egui::include_image!("../assets/icons/chevron-right.svg")
                        };

                        ui.horizontal(|ui| {
                            let btn = egui::Button::image(egui::Image::new(chevron_all).max_size(Vec2::new(10.0, 10.0))).frame(false);
                            if ui.add(btn).clicked() {
                                self.tree_all_expanded = !self.tree_all_expanded;
                            }
                            ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-download.svg")).max_size(Vec2::new(16.0, 16.0)));
                            if ui.selectable_label(is_all_sel, format!("All Downloads ({})", all_count)).clicked() {
                                self.selected_category = Category::All;
                                self.status_filter = None;
                                self.queue_filter = None;
                            }
                        });

                        // Sub-categories under All Downloads
                        if self.tree_all_expanded {
                            let sub_cats = [
                                (Category::Compressed, "Compressed", egui::include_image!("../assets/icons/folder-archive.svg")),
                                (Category::Documents, "Documents", egui::include_image!("../assets/icons/folder-docs.svg")),
                                (Category::Music, "Music", egui::include_image!("../assets/icons/folder-audio.svg")),
                                (Category::Programs, "Programs", egui::include_image!("../assets/icons/folder-app.svg")),
                                (Category::Video, "Video", egui::include_image!("../assets/icons/folder-video.svg")),
                                (Category::General, "General", egui::include_image!("../assets/icons/folder-base.svg")),
                            ];

                            for (cat, label, icon) in sub_cats {
                                let count = self.downloads.iter().filter(|d| d.category == cat).count();
                                let is_sel = self.selected_category == cat && self.status_filter.is_none() && self.queue_filter.is_none();

                                ui.horizontal(|ui| {
                                    ui.add_space(16.0);
                                    ui.add(egui::Image::new(icon).max_size(Vec2::new(14.0, 14.0)));
                                    if ui.selectable_label(is_sel, format!("{} ({})", label, count)).clicked() {
                                        self.selected_category = cat;
                                        self.status_filter = None;
                                        self.queue_filter = None;
                                    }
                                });
                            }
                        }

                        ui.add_space(6.0);

                        // 2. Unfinished Node
                        let unfinished_count = self.downloads.iter().filter(|d| d.status != DownloadStatus::Completed).count();
                        let is_unfin_sel = self.status_filter == Some(false);
                        ui.horizontal(|ui| {
                            let chevron_unfin = if self.tree_unfinished_expanded {
                                egui::include_image!("../assets/icons/chevron-down.svg")
                            } else {
                                egui::include_image!("../assets/icons/chevron-right.svg")
                            };
                            let btn = egui::Button::image(egui::Image::new(chevron_unfin).max_size(Vec2::new(10.0, 10.0))).frame(false);
                            if ui.add(btn).clicked() {
                                self.tree_unfinished_expanded = !self.tree_unfinished_expanded;
                            }
                            ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-connection.svg")).max_size(Vec2::new(16.0, 16.0)));
                            if ui.selectable_label(is_unfin_sel, format!("Unfinished ({})", unfinished_count)).clicked() {
                                self.status_filter = Some(false);
                                self.queue_filter = None;
                            }
                        });

                        ui.add_space(4.0);

                        // 3. Finished Node
                        let finished_count = self.downloads.iter().filter(|d| d.status == DownloadStatus::Completed).count();
                        let is_fin_sel = self.status_filter == Some(true);
                        ui.horizontal(|ui| {
                            let chevron_fin = if self.tree_finished_expanded {
                                egui::include_image!("../assets/icons/chevron-down.svg")
                            } else {
                                egui::include_image!("../assets/icons/chevron-right.svg")
                            };
                            let btn = egui::Button::image(egui::Image::new(chevron_fin).max_size(Vec2::new(10.0, 10.0))).frame(false);
                            if ui.add(btn).clicked() {
                                self.tree_finished_expanded = !self.tree_finished_expanded;
                            }
                            ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-base.svg")).max_size(Vec2::new(16.0, 16.0)));
                            if ui.selectable_label(is_fin_sel, format!("Finished ({})", finished_count)).clicked() {
                                self.status_filter = Some(true);
                                self.queue_filter = None;
                            }
                        });

                        ui.add_space(4.0);

                        // 4. Grabber projects Node
                        ui.horizontal(|ui| {
                            let chevron_grab = if self.tree_grabber_expanded {
                                egui::include_image!("../assets/icons/chevron-down.svg")
                            } else {
                                egui::include_image!("../assets/icons/chevron-right.svg")
                            };
                            let btn = egui::Button::image(egui::Image::new(chevron_grab).max_size(Vec2::new(10.0, 10.0))).frame(false);
                            if ui.add(btn).clicked() {
                                self.tree_grabber_expanded = !self.tree_grabber_expanded;
                            }
                            ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-link.svg")).max_size(Vec2::new(16.0, 16.0)));
                            let _ = ui.selectable_label(false, "Grabber projects");
                        });

                        ui.add_space(4.0);

                        // 5. Queues Node
                        let chevron_q = if self.tree_queues_expanded {
                            egui::include_image!("../assets/icons/chevron-down.svg")
                        } else {
                            egui::include_image!("../assets/icons/chevron-right.svg")
                        };
                        ui.horizontal(|ui| {
                            let btn = egui::Button::image(egui::Image::new(chevron_q).max_size(Vec2::new(10.0, 10.0))).frame(false);
                            if ui.add(btn).clicked() {
                                self.tree_queues_expanded = !self.tree_queues_expanded;
                            }
                            ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-queue.svg")).max_size(Vec2::new(16.0, 16.0)));
                            let _ = ui.selectable_label(false, "Queues");
                        });

                        if self.tree_queues_expanded {
                            ui.horizontal(|ui| {
                                ui.add_space(16.0);
                                ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-tasks.svg")).max_size(Vec2::new(14.0, 14.0)));
                                let is_main_q = self.queue_filter.as_deref() == Some("Main");
                                if ui.selectable_label(is_main_q, "Main queue").clicked() {
                                    self.queue_filter = Some("Main".to_string());
                                }
                            });
                            ui.horizontal(|ui| {
                                ui.add_space(16.0);
                                ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-update.svg")).max_size(Vec2::new(14.0, 14.0)));
                                let is_sync_q = self.queue_filter.as_deref() == Some("Sync");
                                if ui.selectable_label(is_sync_q, "Sync queue").clicked() {
                                    self.queue_filter = Some("Sync".to_string());
                                }
                            });
                        }
                    });
                });
        }

        // Center Pane: Downloads Table (Strict replica of IDM layout with dynamic Columns management)
        let mut filtered_downloads: Vec<usize> = self.downloads
            .iter()
            .enumerate()
            .filter(|(_, d)| {
                if self.selected_category != Category::All && d.category != self.selected_category {
                    return false;
                }
                if let Some(finished) = self.status_filter {
                    if finished && d.status != DownloadStatus::Completed {
                        return false;
                    }
                    if !finished && d.status == DownloadStatus::Completed {
                        return false;
                    }
                }
                if let Some(ref q) = self.queue_filter {
                    if d.queue.as_deref() != Some(q) {
                        return false;
                    }
                }
                true
            })
            .map(|(idx, _)| idx)
            .collect();

        // Sorting
        if let Some(ref col_id) = self.sort_column {
            let asc = self.sort_ascending;
            filtered_downloads.sort_by(|&a, &b| {
                let da = &self.downloads[a];
                let db = &self.downloads[b];
                let cmp = match col_id.as_str() {
                    "filename" => da.filename.to_lowercase().cmp(&db.filename.to_lowercase()),
                    "queue" => da.queue.as_deref().unwrap_or("").cmp(db.queue.as_deref().unwrap_or("")),
                    "size" => da.total_size.unwrap_or(0).cmp(&db.total_size.unwrap_or(0)),
                    "status" => da.status.text().cmp(&db.status.text()),
                    "time_left" => da.time_left_secs.unwrap_or(u64::MAX).cmp(&db.time_left_secs.unwrap_or(u64::MAX)),
                    "transfer_rate" => da.transfer_rate.partial_cmp(&db.transfer_rate).unwrap_or(std::cmp::Ordering::Equal),
                    "date_added" => da.date_added.cmp(&db.date_added),
                    "description" => da.description.cmp(&db.description),
                    "save_to" => da.save_path.cmp(&db.save_path),
                    "last_try_date" => da.last_try_date.cmp(&db.last_try_date),
                    "parent_page" => da.parent_page.cmp(&db.parent_page),
                    "referer" => da.referer.cmp(&db.referer),
                    _ => std::cmp::Ordering::Equal,
                };
                if asc { cmp } else { cmp.reverse() }
            });
        }

        egui::CentralPanel::default().show(ui, |ui| {
            if self.is_reconciling {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.colored_label(Color32::from_rgb(255, 180, 0), "Reconciling download tasks from database and checking checkpoints...");
                });
                ui.add_space(4.0);
            }
            let visible_indices: Vec<usize> = self.columns.iter()
                .enumerate()
                .filter(|(_, c)| c.visible)
                .map(|(i, _)| i)
                .collect();

            let total_table_w: f32 = visible_indices.iter().map(|&i| self.columns[i].width).sum::<f32>().max(ui.available_width());

            egui::ScrollArea::both().id_salt("downloads_table_scroll").auto_shrink([false, false]).show(ui, |ui| {
                ui.set_min_width(total_table_w);

                let mut drag_resize: Option<(usize, f32)> = None;
                let mut clicked_col_id: Option<String> = None;
                let mut open_columns_modal = false;

                // 1. Table Headers with draggable column resize and sort indicators (picture-23 to 28)
                let header_h = 24.0;
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    for &col_idx in &visible_indices {
                        let col_id = self.columns[col_idx].id.clone();
                        let col_name = self.columns[col_idx].name.clone();
                        let col_w = self.columns[col_idx].width;

                        let (cell_rect, resp) = ui.allocate_exact_size(Vec2::new(col_w, header_h), egui::Sense::click());

                        let is_hovered = resp.hovered();
                        let bg_color = if is_hovered {
                            Color32::from_rgb(52, 54, 58)
                        } else {
                            Color32::from_rgb(34, 36, 40)
                        };
                        ui.painter().rect_filled(cell_rect, 0.0, bg_color);
                        // Bottom & Right border
                        ui.painter().line_segment(
                            [egui::pos2(cell_rect.min.x, cell_rect.max.y), egui::pos2(cell_rect.max.x, cell_rect.max.y)],
                            egui::Stroke::new(1.0, Color32::from_rgb(60, 60, 65)),
                        );
                        ui.painter().line_segment(
                            [egui::pos2(cell_rect.max.x, cell_rect.min.y), egui::pos2(cell_rect.max.x, cell_rect.max.y)],
                            egui::Stroke::new(1.0, Color32::from_rgb(60, 60, 65)),
                        );

                        // Text & Sort Chevron indicator
                        let text_galley = ui.painter().layout_no_wrap(
                            col_name.clone(),
                            egui::FontId::proportional(12.0),
                            Color32::from_rgb(220, 220, 220),
                        );
                        let text_w = text_galley.size().x;
                        ui.painter().galley(
                            egui::pos2(cell_rect.min.x + 8.0, cell_rect.center().y - text_galley.size().y / 2.0),
                            text_galley,
                            Color32::from_rgb(220, 220, 220),
                        );

                        if let Some(ref sc) = self.sort_column {
                            if sc == &col_id {
                                let icon_x = cell_rect.min.x + 8.0 + text_w + 8.0;
                                if icon_x + 10.0 < cell_rect.max.x - 4.0 {
                                    let cx = icon_x + 5.0;
                                    let cy = cell_rect.center().y;
                                    let chevron_stroke = egui::Stroke::new(1.6, Color32::from_rgb(0, 190, 255));
                                    if self.sort_ascending {
                                        // Chevron pointing up: ^
                                        ui.painter().line_segment(
                                            [egui::pos2(cx - 4.0, cy + 2.0), egui::pos2(cx, cy - 2.5)],
                                            chevron_stroke,
                                        );
                                        ui.painter().line_segment(
                                            [egui::pos2(cx, cy - 2.5), egui::pos2(cx + 4.0, cy + 2.0)],
                                            chevron_stroke,
                                        );
                                    } else {
                                        // Chevron pointing down: v
                                        ui.painter().line_segment(
                                            [egui::pos2(cx - 4.0, cy - 2.5), egui::pos2(cx, cy + 2.0)],
                                            chevron_stroke,
                                        );
                                        ui.painter().line_segment(
                                            [egui::pos2(cx, cy + 2.0), egui::pos2(cx + 4.0, cy - 2.5)],
                                            chevron_stroke,
                                        );
                                    }
                                }
                            }
                        }

                        if resp.clicked() {
                            clicked_col_id = Some(col_id.clone());
                        }

                        resp.context_menu(|ui| {
                            if ui.button("Columns...").clicked() {
                                open_columns_modal = true;
                                ui.close();
                            }
                        });

                        // Resize handle at the right edge
                        let handle_w = 8.0;
                        let handle_rect = egui::Rect::from_min_max(
                            egui::pos2(cell_rect.max.x - handle_w / 2.0, cell_rect.min.y),
                            egui::pos2(cell_rect.max.x + handle_w / 2.0, cell_rect.max.y),
                        );
                        let handle_id = ui.scope_id().with("hdr_resize").with(col_idx);
                        let handle_resp = ui.interact(handle_rect, handle_id, egui::Sense::click_and_drag());
                        if handle_resp.hovered() || handle_resp.dragged() {
                            ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);
                        }
                        if handle_resp.dragged() {
                            drag_resize = Some((col_idx, handle_resp.drag_delta().x));
                        }
                    }
                });

                if let Some((idx, delta)) = drag_resize {
                    self.columns[idx].width = (self.columns[idx].width + delta).clamp(30.0, 1500.0);
                }

                if let Some(col_id) = clicked_col_id {
                    if self.sort_column.as_deref() == Some(&col_id) {
                        self.sort_ascending = !self.sort_ascending;
                    } else {
                        self.sort_column = Some(col_id);
                        self.sort_ascending = true;
                    }
                }

                if open_columns_modal {
                    self.temp_columns = self.columns.clone();
                    self.selected_column_idx = Some(0);
                    self.column_width_input = format!("{:.0}", self.columns.first().map(|c| c.width).unwrap_or(260.0));
                    self.show_columns_modal = true;
                }

                // 2. Table Data Rows
                let mut action_resume = None;
                let mut action_pause = None;
                let mut action_cancel = None;
                let mut action_details = None;
                let mut action_delete = None;
                let mut action_retry_cleanup = None;
                let row_height = 22.0;

                for (row_num, &idx) in filtered_downloads.iter().enumerate() {
                    let item_id = self.downloads[idx].id;
                    let filename = self.downloads[idx].filename.clone();
                    let formatted_size = self.downloads[idx].formatted_size();
                    let status = self.downloads[idx].status.clone();
                    let progress_ratio = self.downloads[idx].progress_ratio();
                    let formatted_time_left = self.downloads[idx].formatted_time_left();
                    let formatted_speed = self.downloads[idx].formatted_speed();
                    let formatted_date = self.downloads[idx].date_added.format("%b %d %H:%M:%S %Y").to_string();
                    let description = self.downloads[idx].description.clone();
                    let q_val = self.downloads[idx].queue.as_deref().unwrap_or("").to_string();

                    let is_selected = self.selected_id == Some(item_id);

                    let (status_text, status_color) = match &status {
                        DownloadStatus::Downloading => (format!("Downloading ({:.0}%)", progress_ratio * 100.0), Color32::from_rgb(0, 190, 255)),
                        DownloadStatus::Completed => ("Complete".to_string(), Color32::from_rgb(60, 210, 60)),
                        DownloadStatus::Paused => {
                            if progress_ratio > 0.0 {
                                (format!("Paused ({:.0}%)", progress_ratio * 100.0), Color32::from_rgb(255, 175, 0))
                            } else {
                                ("Paused".to_string(), Color32::from_rgb(255, 175, 0))
                            }
                        }
                        DownloadStatus::Connecting => ("Connecting...".to_string(), Color32::from_rgb(180, 180, 255)),
                        DownloadStatus::Queued => ("Queued".to_string(), Color32::LIGHT_GRAY),
                        DownloadStatus::Error(e) => (format!("Error: {}", e), Color32::from_rgb(255, 75, 75)),
                        DownloadStatus::Canceled => {
                            if progress_ratio > 0.0 {
                                (format!("Canceled ({:.0}%)", progress_ratio * 100.0), Color32::GRAY)
                            } else {
                                ("Canceled".to_string(), Color32::GRAY)
                            }
                        }
                        DownloadStatus::CleanupPending => ("Cleanup Pending".to_string(), Color32::from_rgb(255, 140, 0)),
                    };

                    let text_color = if is_selected { Color32::WHITE } else { Color32::from_rgb(220, 220, 220) };

                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 0.0;
                        for &col_idx in &visible_indices {
                            let col = &self.columns[col_idx];
                            let col_w = col.width;

                            let (cell_rect, cell_resp) = ui.allocate_exact_size(Vec2::new(col_w, row_height), egui::Sense::click());
                            if is_selected {
                                ui.painter().rect_filled(cell_rect, 0.0, Color32::from_rgb(0, 95, 175));
                            } else if row_num % 2 == 1 {
                                ui.painter().rect_filled(cell_rect, 0.0, Color32::from_rgb(26, 26, 29));
                            }

                            // Right vertical gridline
                            ui.painter().line_segment(
                                [egui::pos2(cell_rect.max.x, cell_rect.min.y), egui::pos2(cell_rect.max.x, cell_rect.max.y)],
                                egui::Stroke::new(1.0, Color32::from_rgb(45, 45, 50))
                            );

                            let mut cell_ui = ui.new_child(
                                egui::UiBuilder::new()
                                    .max_rect(cell_rect.shrink2(Vec2::new(4.0, 0.0)))
                                    .layout(egui::Layout::left_to_right(egui::Align::Center)),
                            );
                            cell_ui.set_clip_rect(cell_rect);
                            match col.id.as_str() {
                                "filename" => {
                                    cell_ui.spacing_mut().item_spacing.x = 4.0;
                                    cell_ui.add(egui::Image::new(get_file_icon_source(&filename)).max_size(Vec2::new(15.0, 15.0)));
                                    cell_ui.add(egui::Label::new(RichText::new(&filename).color(text_color)).truncate());
                                }
                                "queue" => {
                                    cell_ui.add(egui::Label::new(RichText::new(&q_val).color(text_color)).truncate());
                                }
                                "size" => {
                                    let mut right_ui = cell_ui.new_child(
                                        egui::UiBuilder::new()
                                            .max_rect(cell_rect.shrink2(Vec2::new(6.0, 0.0)))
                                            .layout(egui::Layout::right_to_left(egui::Align::Center)),
                                    );
                                    right_ui.add(egui::Label::new(RichText::new(&formatted_size).color(text_color)).truncate());
                                }
                                    "status" => {
                                        let color = if is_selected { Color32::WHITE } else { status_color };
                                        let lbl = cell_ui.add(egui::Label::new(RichText::new(&status_text).color(color)).truncate());
                                        if status == DownloadStatus::CleanupPending {
                                            let err_msg = self.cleanup_errors.get(&item_id).cloned().unwrap_or_else(|| "File locked or inaccessible".to_string());
                                            lbl.on_hover_text(format!("File cleanup failed: {}\nClick 'Retry Cleanup' once the file is unlocked.", err_msg));
                                        }
                                    }
                                    "time_left" => {
                                        cell_ui.add(egui::Label::new(RichText::new(&formatted_time_left).color(text_color)).truncate());
                                    }
                                    "transfer_rate" => {
                                        cell_ui.add(egui::Label::new(RichText::new(&formatted_speed).color(text_color)).truncate());
                                    }
                                    "date_added" => {
                                        cell_ui.add(egui::Label::new(RichText::new(&formatted_date).color(text_color)).truncate());
                                    }
                                    "description" => {
                                        cell_ui.add(egui::Label::new(RichText::new(&description).color(text_color)).truncate());
                                    }
                                    "save_to" => {
                                        let p = self.downloads[idx].save_path.parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                                        cell_ui.add(egui::Label::new(RichText::new(p).color(text_color)).truncate());
                                    }
                                    "last_try_date" => {
                                        let last_str = self.downloads[idx].last_try_date
                                            .map(|d| d.format("%b %d %H:%M:%S %Y").to_string())
                                            .unwrap_or_else(|| formatted_date.clone());
                                        cell_ui.add(egui::Label::new(RichText::new(last_str).color(text_color)).truncate());
                                    }
                                    "parent_page" => {
                                        cell_ui.add(egui::Label::new(RichText::new(self.downloads[idx].parent_page.as_deref().unwrap_or("-")).color(text_color)).truncate());
                                    }
                                    "referer" => {
                                        cell_ui.add(egui::Label::new(RichText::new(self.downloads[idx].referer.as_deref().unwrap_or("-")).color(text_color)).truncate());
                                    }
                                    _ => {}
                                }

                                if cell_resp.clicked() {
                                self.selected_id = Some(item_id);
                            }
                            if cell_resp.double_clicked() {
                                action_details = Some(item_id);
                            }

                            cell_resp.context_menu(|ui| {
                                let is_cleanup = self.downloads[idx].status == DownloadStatus::CleanupPending;
                                if is_cleanup {
                                    let retry_btn = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/refresh-cw.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        RichText::new("Retry Cleanup")
                                            .color(Color32::from_rgb(255, 140, 0))
                                            .strong()
                                    );
                                    if ui.add(retry_btn).clicked() {
                                        action_retry_cleanup = Some(item_id);
                                        ui.close();
                                    }
                                    ui.separator();
                                } else {
                                    let btn_resume = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/play.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        "Resume Download",
                                    );
                                    if ui.add(btn_resume).clicked() {
                                        action_resume = Some(item_id);
                                        ui.close();
                                    }
                                    let btn_pause = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/pause.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        "Stop Download",
                                    );
                                    if ui.add(btn_pause).clicked() {
                                        action_pause = Some(item_id);
                                        ui.close();
                                    }
                                    let btn_cancel = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/square.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        "Cancel Download",
                                    );
                                    if ui.add(btn_cancel).clicked() {
                                        action_cancel = Some(item_id);
                                        ui.close();
                                    }
                                    ui.separator();
                                }
                                let btn_details = egui::Button::image_and_text(
                                    egui::Image::new(egui::include_image!("../assets/icons/search.svg")).max_size(Vec2::new(14.0, 14.0)),
                                    "Download Status & Segments...",
                                );
                                if ui.add(btn_details).clicked() {
                                    action_details = Some(item_id);
                                    ui.close();
                                }
                                let btn_delete = egui::Button::image_and_text(
                                    egui::Image::new(egui::include_image!("../assets/icons/trash.svg")).max_size(Vec2::new(14.0, 14.0)),
                                    "Delete Selected",
                                );
                                if ui.add(btn_delete).clicked() {
                                    action_delete = Some(item_id);
                                    ui.close();
                                }
                                ui.separator();
                                let btn_open_loc = egui::Button::image_and_text(
                                    egui::Image::new(egui::include_image!("../assets/icons/folder-base.svg")).max_size(Vec2::new(14.0, 14.0)),
                                    "Open File Location",
                                );
                                if ui.add(btn_open_loc).clicked() {
                                    #[cfg(target_os = "windows")]
                                    {
                                        let _ = std::process::Command::new("explorer").arg(&self.downloads[idx].save_path).spawn();
                                    }
                                    ui.close();
                                }
                            });
                        }
                    });
                }

                if let Some(id) = action_retry_cleanup {
                    self.selected_id = Some(id);
                    self.retry_cleanup(id);
                }
                if let Some(id) = action_resume {
                    self.selected_id = Some(id);
                    self.resume_selected();
                }
                if let Some(id) = action_pause {
                    self.selected_id = Some(id);
                    self.pause_selected();
                }
                if let Some(id) = action_cancel {
                    self.selected_id = Some(id);
                    self.cancel_selected();
                }
                if let Some(id) = action_details {
                    self.progress_modal_id = Some(id);
                    self.show_progress_modal = true;
                }
                if let Some(id) = action_delete {
                    self.selected_id = Some(id);
                    self.delete_selected();
                }
            });
        });

        // Dialog Windows
        let ctx = ui.ctx().clone();
        self.render_modals(&ctx);
    }
}

impl IdmApp {
    fn render_modals(&mut self, ctx: &egui::Context) {
        // 1. "Add URL" Dialog
        if self.show_add_url_modal {
            let mut open = true;
            egui::Window::new("Enter new address to download")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(460.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Address:");
                    if ui.text_edit_singleline(&mut self.new_download_url).changed() {
                        if let Some(ext) = self.new_download_url.split('.').last() {
                            let clean_ext = ext.split('?').next().unwrap_or(ext);
                            self.new_download_category = Category::from_extension(clean_ext);
                        }
                    }

                    ui.add_space(8.0);
                    ui.checkbox(&mut self.use_authorization, "Use authorization");
                    if self.use_authorization {
                        ui.horizontal(|ui| {
                            ui.label("Login:   ");
                            ui.text_edit_singleline(&mut self.auth_login);
                        });
                        ui.horizontal(|ui| {
                            ui.label("Password:");
                            ui.add(egui::TextEdit::singleline(&mut self.auth_pass).password(true));
                        });
                    }

                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        ui.label("Category:");
                        egui::ComboBox::from_id_salt("cat_selector")
                            .selected_text(self.new_download_category.name())
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut self.new_download_category, Category::General, "General");
                                ui.selectable_value(&mut self.new_download_category, Category::Compressed, "Compressed");
                                ui.selectable_value(&mut self.new_download_category, Category::Documents, "Documents");
                                ui.selectable_value(&mut self.new_download_category, Category::Music, "Music");
                                ui.selectable_value(&mut self.new_download_category, Category::Programs, "Programs");
                                ui.selectable_value(&mut self.new_download_category, Category::Video, "Video");
                            });
                    });

                    ui.add_space(15.0);
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("Start Download").strong()).clicked() {
                            let id = self.next_id;
                            self.next_id += 1;

                            let raw_filename = self.new_download_url
                                .split('/')
                                .last()
                                .unwrap_or("file.dat")
                                .split('?')
                                .next()
                                .unwrap_or("file.dat");
                            let (filename, save_path) = self.resolve_unique_download_target(raw_filename);

                            let item = DownloadItem {
                                id,
                                url: self.new_download_url.clone(),
                                filename: filename.clone(),
                                category: self.new_download_category,
                                save_path: save_path.clone(),
                                total_size: None,
                                downloaded_bytes: 0,
                                status: DownloadStatus::Connecting,
                                transfer_rate: 0.0,
                                time_left_secs: None,
                                resume_capable: false,
                                date_added: Local::now(),
                                max_connections: self.config.default_connections,
                                segments: vec![],
                                description: format!("Downloaded from {}", self.new_download_url),
                                queue: None,
                ..Default::default()
                            };

                            let settings = DownloadSettings {
                                download_id: id,
                                max_connections: self.config.default_connections,
                                chunk_size: 2 * 1024 * 1024,
                                speed_limit: None,
                                retry_limit: 5,
                            };
                            if let Err(e) = self.db.insert_download(&item, &settings) {
                                eprintln!("Database error inserting new download: {}", e);
                            }

                            self.mark_task_mutated(item.id);
                            self.downloads.push(item);
                            self.selected_id = Some(id);

                            if let Err(e) = self.engine.cmd_sender.send(EngineCommand::StartDownload {
                                id,
                                url: self.new_download_url.clone(),
                                save_path,
                                connections: self.config.default_connections,
                            }) {
                                eprintln!("Engine error sending StartDownload: {}", e);
                            }

                            self.show_add_url_modal = false;
                        }

                        if ui.button("Download Later").clicked() {
                            let id = self.next_id;
                            self.next_id += 1;
                            let raw_filename = self.new_download_url
                                .split('/')
                                .last()
                                .unwrap_or("file.dat")
                                .split('?')
                                .next()
                                .unwrap_or("file.dat");
                            let (filename, save_path) = self.resolve_unique_download_target(raw_filename);

                            let item = DownloadItem {
                                id,
                                url: self.new_download_url.clone(),
                                filename,
                                category: self.new_download_category,
                                save_path,
                                total_size: None,
                                downloaded_bytes: 0,
                                status: DownloadStatus::Queued,
                                transfer_rate: 0.0,
                                time_left_secs: None,
                                resume_capable: false,
                                date_added: Local::now(),
                                max_connections: self.config.default_connections,
                                segments: vec![],
                                description: "Added to queue".to_string(),
                                queue: Some("Main".to_string()),
                ..Default::default()
                            };

                            let settings = DownloadSettings {
                                download_id: id,
                                max_connections: self.config.default_connections,
                                chunk_size: 2 * 1024 * 1024,
                                speed_limit: None,
                                retry_limit: 5,
                            };
                            if let Err(e) = self.db.insert_download(&item, &settings) {
                                eprintln!("Database error inserting download later item: {}", e);
                            }

                            self.mark_task_mutated(item.id);
                            self.downloads.push(item);
                            self.show_add_url_modal = false;
                        }

                        if ui.button("Cancel").clicked() {
                            self.show_add_url_modal = false;
                        }
                    });
                });
            if !open {
                self.show_add_url_modal = false;
            }
        }

        // 2. "Download Progress & Connection Segments" Dialog
        if self.show_progress_modal {
            if let Some(target_id) = self.progress_modal_id {
                if let Some(item) = self.downloads.iter().find(|d| d.id == target_id).cloned() {
                    let mut open = true;
                    egui::Window::new(format!("Download status: {}", item.filename))
                        .open(&mut open)
                        .resizable(true)
                        .default_width(520.0)
                        .default_height(480.0)
                        .min_width(360.0)
                        .min_height(250.0)
                        .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                        .show(ctx, |ui| {
                            ui.horizontal(|ui| {
                                ui.add(egui::Image::new(get_file_icon_source(&item.filename)).max_size(Vec2::new(22.0, 22.0)));
                                ui.heading(&item.filename);
                            });
                            ui.label(format!("URL: {}", item.url));
                            ui.separator();

                            egui::Grid::new("progress_info_grid").spacing([15.0, 5.0]).show(ui, |ui| {
                                ui.label(RichText::new("Status:").strong());
                                let status_modal_text = match &item.status {
                                    DownloadStatus::Downloading => format!("Downloading ({:.1}%)", item.progress_ratio() * 100.0),
                                    DownloadStatus::Paused if item.progress_ratio() > 0.0 => format!("Paused ({:.1}%)", item.progress_ratio() * 100.0),
                                    _ => item.status.text(),
                                };
                                ui.label(status_modal_text);
                                ui.end_row();

                                ui.label(RichText::new("File size:").strong());
                                ui.label(item.formatted_size());
                                ui.end_row();

                                ui.label(RichText::new("Downloaded:").strong());
                                ui.label(format!("{} ({:.1}%)", item.formatted_downloaded(), item.progress_ratio() * 100.0));
                                ui.end_row();

                                ui.label(RichText::new("Transfer rate:").strong());
                                ui.label(item.formatted_speed());
                                ui.end_row();

                                ui.label(RichText::new("Time left:").strong());
                                ui.label(item.formatted_time_left());
                                ui.end_row();

                                ui.label(RichText::new("Resume capability:").strong());
                                ui.label(if item.resume_capable { "Yes" } else { "No" });
                                ui.end_row();
                            });

                            ui.add_space(8.0);
                            // Overall Progress Bar
                            ui.add(egui::ProgressBar::new(item.progress_ratio()).show_percentage().animate(item.status == DownloadStatus::Downloading));

                            ui.add_space(10.0);
                            ui.separator();
                            
                            // Submenu / View toggle between Chunks and Connections
                            ui.horizontal(|ui| {
                                ui.label(RichText::new("Progress View:").strong());
                                if ui.selectable_label(!self.progress_view_connections, "File Chunks (Multi-Part Ranges)").clicked() {
                                    self.progress_view_connections = false;
                                }
                                if ui.selectable_label(self.progress_view_connections, "Live Connections (Sockets)").clicked() {
                                    self.progress_view_connections = true;
                                }
                            });
                            ui.add_space(4.0);

                            if self.progress_view_connections {
                                // 1. Live Sockets / Connections View (showing speed & transferred bytes)
                                let conns = if item.status == DownloadStatus::Completed {
                                    (0..item.max_connections).map(|i| SegmentProgress {
                                        segment_index: i,
                                        start_byte: 0,
                                        end_byte: 100,
                                        current_byte: 100,
                                        speed: 0.0,
                                    }).collect::<Vec<_>>()
                                } else if !item.connection_segments.is_empty() {
                                    item.connection_segments.clone()
                                } else {
                                    (0..item.max_connections).map(|i| SegmentProgress {
                                        segment_index: i,
                                        start_byte: 0,
                                        end_byte: 100,
                                        current_byte: 0,
                                        speed: 0.0,
                                    }).collect()
                                };

                                egui::ScrollArea::vertical()
                                    .id_salt("active_connections_scroll")
                                    .max_height(140.0)
                                    .auto_shrink([false, false])
                                    .show(ui, |ui| {
                                        for conn in &conns {
                                            ui.horizontal(|ui| {
                                                ui.label(RichText::new(format!("{:02}:", conn.segment_index + 1)).monospace().strong());
                                                ui.add_space(8.0);

                                                if item.status == DownloadStatus::Completed {
                                                    ui.label(RichText::new("Complete").color(Color32::from_rgb(0, 180, 255)));
                                                } else if conn.speed > 0.0 {
                                                    ui.label(RichText::new(format!("{}/s", format_bytes(conn.speed as u64))).strong().color(Color32::from_rgb(0, 210, 130)));
                                                } else if item.status == DownloadStatus::Downloading {
                                                    ui.label(RichText::new("Connecting...").color(Color32::from_rgb(255, 190, 70)));
                                                } else {
                                                    ui.label(RichText::new("Idle").weak());
                                                }

                                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                                    if conn.current_byte > 0 {
                                                        ui.label(RichText::new(format!("{} transferred", format_bytes(conn.current_byte))).small().weak());
                                                    } else {
                                                        ui.label(RichText::new("-").small().weak());
                                                    }
                                                });
                                            });
                                            ui.add_space(3.0);
                                        }
                                    });
                            } else {
                                // 2. File Chunks / Multi-Part Ranges View
                                if !item.segments.is_empty() {
                                    egui::ScrollArea::vertical()
                                        .id_salt("connection_segments_scroll")
                                        .max_height(140.0)
                                        .auto_shrink([false, false])
                                        .show(ui, |ui| {
                                            for seg in &item.segments {
                                                ui.horizontal(|ui| {
                                                    ui.label(RichText::new(format!("Part {:02}:", seg.segment_index + 1)).monospace());
                                                    let seg_bar = egui::ProgressBar::new(seg.ratio())
                                                        .desired_width(ui.available_width() - 80.0)
                                                        .fill(Color32::from_rgb(0, 170, 230)); // Cyan blue for file parts
                                                    ui.add(seg_bar);
                                                    ui.label(format!("{:.0}%", seg.ratio() * 100.0));
                                                });
                                            }
                                        });
                                } else {
                                    ui.label("Single connection active");
                                }
                            }

                            ui.add_space(12.0);
                            ui.separator();
                            ui.horizontal(|ui| {
                                if item.status == DownloadStatus::Downloading {
                                    let btn_pause = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/pause.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        "Pause",
                                    );
                                    if ui.add(btn_pause).clicked() {
                                        self.selected_id = Some(item.id);
                                        self.pause_selected();
                                    }
                                } else if item.status == DownloadStatus::Paused || item.status == DownloadStatus::Queued {
                                    let btn_resume = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/play.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        "Resume",
                                    );
                                    if ui.add(btn_resume).clicked() {
                                        self.selected_id = Some(item.id);
                                        self.resume_selected();
                                    }
                                } else if item.status == DownloadStatus::CleanupPending {
                                    let retry_btn = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/alert-triangle.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        RichText::new("Retry Cleanup")
                                            .color(Color32::WHITE)
                                            .strong(),
                                    ).fill(Color32::from_rgb(230, 110, 0));
                                    if ui.add(retry_btn).clicked() {
                                        self.selected_id = Some(item.id);
                                        self.retry_cleanup_selected();
                                        self.show_progress_modal = false;
                                    }
                                }

                                if ui.button("Close").clicked() {
                                    self.show_progress_modal = false;
                                }
                            });
                        });
                    if !open {
                        self.show_progress_modal = false;
                    }
                }
            }
        }

        // 3. Complete "Options" Dialog (Covering all 9 real IDM tabs)
        if self.show_options_modal {
            let mut open = true;
            egui::Window::new("Internet Download Manager Configuration")
                .open(&mut open)
                .resizable(true)
                .default_width(620.0)
                .default_height(500.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    // Tab Bar
                    ui.horizontal_wrapped(|ui| {
                        ui.selectable_value(&mut self.options_tab, 0, "General");
                        ui.selectable_value(&mut self.options_tab, 1, "File Types");
                        ui.selectable_value(&mut self.options_tab, 2, "Save To");
                        ui.selectable_value(&mut self.options_tab, 3, "Connection");
                        ui.selectable_value(&mut self.options_tab, 4, "Downloads");
                        ui.selectable_value(&mut self.options_tab, 5, "Proxy / Socks");
                        ui.selectable_value(&mut self.options_tab, 6, "Site Logins");
                        ui.selectable_value(&mut self.options_tab, 7, "Dial-Up");
                        ui.selectable_value(&mut self.options_tab, 8, "Sounds");
                    });
                    ui.separator();

                    egui::ScrollArea::vertical().id_salt("options_tabs_scroll").max_height(400.0).show(ui, |ui| {
                        match self.options_tab {
                            0 => {
                                // Tab 0: General
                                ui.heading("General Settings");
                                ui.add_space(6.0);
                                ui.checkbox(&mut self.config.launch_on_startup, "Launch Internet Download Manager on system startup");
                                ui.checkbox(&mut self.config.auto_start_clipboard, "Automatically start downloading of URLs placed to clipboard");
                                ui.add_space(8.0);

                                ui.label(RichText::new("Capture downloads from the following browsers:").strong());
                                ui.checkbox(&mut self.config.capture_chrome, "Google Chrome");
                                ui.checkbox(&mut self.config.capture_firefox, "Mozilla Firefox");
                                ui.checkbox(&mut self.config.capture_edge, "Microsoft Edge");
                                ui.checkbox(&mut self.config.capture_opera, "Opera");
                                ui.checkbox(&mut self.config.capture_brave, "Brave & Chromium-based browsers");
                                ui.add_space(8.0);

                                ui.checkbox(&mut self.config.browser_alt_key_prevent, "Use Alt key to prevent downloading with IDM when clicking on links");
                            }
                            1 => {
                                // Tab 1: File Types
                                ui.heading("File Types");
                                ui.add_space(6.0);
                                ui.label("Automatically start downloading the following file types (extensions):");
                                ui.add(egui::TextEdit::multiline(&mut self.config.default_file_types).desired_rows(5).desired_width(f32::INFINITY));
                                if ui.button("Restore Default File Types").clicked() {
                                    self.config.default_file_types = AppConfig::default().default_file_types;
                                }

                                ui.add_space(10.0);
                                ui.separator();
                                ui.label("Don't start downloading automatically from the following addresses (exceptions):");
                                ui.add(egui::TextEdit::multiline(&mut self.config.dont_download_from).desired_rows(3).desired_width(f32::INFINITY));
                            }
                            2 => {
                                // Tab 2: Save To
                                ui.heading("Save To Folders");
                                ui.add_space(6.0);

                                ui.label("Default download directory for General category:");
                                let mut gen_str = self.config.default_download_dir.to_string_lossy().to_string();
                                if ui.text_edit_singleline(&mut gen_str).changed() {
                                    self.config.default_download_dir = PathBuf::from(gen_str);
                                }

                                ui.add_space(6.0);
                                ui.label("Compressed folder:");
                                let mut comp_str = self.config.dir_compressed.to_string_lossy().to_string();
                                if ui.text_edit_singleline(&mut comp_str).changed() {
                                    self.config.dir_compressed = PathBuf::from(comp_str);
                                }

                                ui.add_space(6.0);
                                ui.label("Programs folder:");
                                let mut prog_str = self.config.dir_programs.to_string_lossy().to_string();
                                if ui.text_edit_singleline(&mut prog_str).changed() {
                                    self.config.dir_programs = PathBuf::from(prog_str);
                                }

                                ui.add_space(6.0);
                                ui.label("Temporary directory:");
                                let mut temp_str = self.config.temp_dir.to_string_lossy().to_string();
                                if ui.text_edit_singleline(&mut temp_str).changed() {
                                    self.config.temp_dir = PathBuf::from(temp_str);
                                }

                                ui.add_space(8.0);
                                ui.checkbox(&mut self.config.remember_last_folder, "Remember last selected folder for General category");
                            }
                            3 => {
                                // Tab 3: Connection
                                ui.heading("Connection Type / Speed");
                                ui.add_space(6.0);

                                ui.horizontal(|ui| {
                                    ui.label("Connection Type/Speed:");
                                    egui::ComboBox::from_id_salt("conn_type_combo")
                                        .selected_text(&self.config.connection_type)
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.config.connection_type, "High speed: Direct connection (Ethernet/Cable) / Wi-Fi / 4G/5G".to_string(), "High speed: Direct connection / Wi-Fi / 4G/5G");
                                            ui.selectable_value(&mut self.config.connection_type, "Medium speed: DSL / ADSL".to_string(), "Medium speed: DSL / ADSL");
                                            ui.selectable_value(&mut self.config.connection_type, "Low speed: Dial-Up".to_string(), "Low speed: Dial-Up");
                                        });
                                });

                                ui.add_space(8.0);
                                ui.horizontal(|ui| {
                                    ui.label("Default max. connections number:");
                                    egui::ComboBox::from_id_salt("max_conns_combo_opt")
                                        .selected_text(format!("{}", self.config.default_connections))
                                        .show_ui(ui, |ui| {
                                            for n in [1, 2, 4, 8, 16, 24, 32] {
                                                ui.selectable_value(&mut self.config.default_connections, n, format!("{}", n));
                                            }
                                        });
                                });

                                ui.add_space(10.0);
                                ui.separator();
                                ui.heading("Speed Limiter");
                                ui.checkbox(&mut self.config.speed_limiter_enabled, "Enable Speed Limiter");
                                if self.config.speed_limiter_enabled {
                                    ui.horizontal(|ui| {
                                        ui.label("Maximum download speed for one file:");
                                        ui.add(egui::DragValue::new(&mut self.config.speed_limit_kbps).range(10..=100_000));
                                        ui.label("KB/s");
                                    });
                                }

                                ui.add_space(10.0);
                                ui.separator();
                                ui.heading("Download Limits");
                                ui.checkbox(&mut self.config.enable_download_limits, "Enable Download Limits");
                                if self.config.enable_download_limits {
                                    ui.horizontal(|ui| {
                                        ui.label("Limit downloading to");
                                        ui.add(egui::DragValue::new(&mut self.config.download_limit_mb).range(10..=100_000));
                                        ui.label("MB per");
                                        ui.add(egui::DragValue::new(&mut self.config.download_limit_hours).range(1..=24));
                                        ui.label("hours");
                                    });
                                }
                            }
                            4 => {
                                // Tab 4: Downloads (Dialogs & Virus checking)
                                ui.heading("Download Dialogs");
                                ui.add_space(6.0);
                                ui.checkbox(&mut self.config.show_start_dialog, "Show start download dialog");
                                ui.checkbox(&mut self.config.show_complete_dialog, "Show download complete dialog");
                                ui.checkbox(&mut self.config.show_progress_dialog, "Show download progress dialog");
                                ui.checkbox(&mut self.config.hide_complete_for_queue, "Don't show download complete dialog if downloaded in background queue");

                                ui.add_space(10.0);
                                ui.separator();
                                ui.heading("Virus Checking");
                                ui.label("Virus scanner program:");
                                ui.text_edit_singleline(&mut self.config.virus_scanner_path);
                                ui.label("Command line parameters:");
                                ui.text_edit_singleline(&mut self.config.virus_scanner_params);
                            }
                            5 => {
                                // Tab 5: Proxy / Socks
                                ui.heading("Proxy & Firewall Settings");
                                ui.add_space(6.0);

                                ui.checkbox(&mut self.config.use_http_proxy, "Use HTTP / HTTPS Proxy");
                                if self.config.use_http_proxy {
                                    ui.horizontal(|ui| {
                                        ui.label("Proxy Host:");
                                        ui.text_edit_singleline(&mut self.config.http_proxy_host);
                                        ui.label("Port:");
                                        ui.add(egui::DragValue::new(&mut self.config.http_proxy_port).range(1..=65535));
                                    });
                                }

                                ui.add_space(6.0);
                                ui.checkbox(&mut self.config.use_socks_proxy, "Use SOCKS Proxy (SOCKS4 / SOCKS5)");
                                if self.config.use_socks_proxy {
                                    ui.horizontal(|ui| {
                                        ui.label("SOCKS Host:");
                                        ui.text_edit_singleline(&mut self.config.socks_proxy_host);
                                        ui.label("Port:");
                                        ui.add(egui::DragValue::new(&mut self.config.socks_proxy_port).range(1..=65535));
                                    });
                                }

                                ui.add_space(8.0);
                                ui.horizontal(|ui| {
                                    ui.label("Username:");
                                    ui.text_edit_singleline(&mut self.config.proxy_user);
                                    ui.label("Password:");
                                    ui.add(egui::TextEdit::singleline(&mut self.config.proxy_pass).password(true));
                                });

                                ui.add_space(8.0);
                                ui.label("Do not use proxy for addresses beginning with (exceptions):");
                                ui.text_edit_singleline(&mut self.config.proxy_exceptions);
                            }
                            6 => {
                                // Tab 6: Site Logins
                                ui.heading("Site Logins & Passwords");
                                ui.add_space(6.0);
                                ui.label("Saved login credentials for servers & file hosters:");

                                egui::Grid::new("site_logins_grid").striped(true).spacing([15.0, 5.0]).show(ui, |ui| {
                                    ui.label(RichText::new("Site / URL").strong());
                                    ui.label(RichText::new("User").strong());
                                    ui.label(RichText::new("Password").strong());
                                    ui.end_row();

                                    for cred in &self.config.site_logins {
                                        ui.label(&cred.site_url);
                                        ui.label(&cred.username);
                                        ui.label(&cred.password);
                                        ui.end_row();
                                    }
                                });

                                ui.add_space(10.0);
                                ui.separator();
                                ui.label(RichText::new("Add new credentials:").strong());
                                ui.horizontal(|ui| {
                                    ui.label("Site:");
                                    ui.text_edit_singleline(&mut self.new_site_url);
                                });
                                ui.horizontal(|ui| {
                                    ui.label("User:");
                                    ui.text_edit_singleline(&mut self.new_site_user);
                                    ui.label("Pass:");
                                    ui.add(egui::TextEdit::singleline(&mut self.new_site_pass).password(true));
                                    let btn_add = egui::Button::image_and_text(
                                        egui::Image::new(egui::include_image!("../assets/icons/plus.svg")).max_size(Vec2::new(14.0, 14.0)),
                                        "Add",
                                    );
                                    if ui.add(btn_add).clicked() {
                                        if !self.new_site_url.is_empty() {
                                            self.config.site_logins.push(SiteCredential {
                                                site_url: self.new_site_url.clone(),
                                                username: self.new_site_user.clone(),
                                                password: self.new_site_pass.clone(),
                                            });
                                            self.new_site_url.clear();
                                            self.new_site_user.clear();
                                            self.new_site_pass.clear();
                                        }
                                    }
                                });
                            }
                            7 => {
                                // Tab 7: Dial-Up
                                ui.heading("Dial-Up / VPN Settings");
                                ui.add_space(6.0);
                                ui.horizontal(|ui| {
                                    ui.label("Connection:");
                                    ui.text_edit_singleline(&mut self.config.dial_up_connection);
                                });

                                ui.add_space(8.0);
                                ui.horizontal(|ui| {
                                    ui.label("Redial attempts:");
                                    ui.add(egui::DragValue::new(&mut self.config.redial_attempts).range(0..=100));
                                    ui.label("times");
                                });
                                ui.horizontal(|ui| {
                                    ui.label("Time between redial attempts:");
                                    ui.add(egui::DragValue::new(&mut self.config.time_between_redials).range(1..=300));
                                    ui.label("seconds");
                                });
                            }
                            8 => {
                                // Tab 8: Sounds
                                ui.heading("Sound Settings");
                                ui.add_space(6.0);
                                ui.label("Play sound notifications for Internet Download Manager events:");
                                ui.checkbox(&mut self.config.play_sound_complete, "Download complete");
                                ui.checkbox(&mut self.config.play_sound_error, "Download error / failed");
                                ui.checkbox(&mut self.config.play_sound_queue, "Queue finished");
                            }
                            _ => {}
                        }
                    });

                    ui.add_space(15.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("OK").strong()).clicked() {
                            self.show_options_modal = false;
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_options_modal = false;
                        }
                        if ui.button("Apply").clicked() {
                            // Settings applied in place
                        }
                    });
                });
            if !open {
                self.show_options_modal = false;
            }
        }

        // 4. "Customize Columns" Dialog (AVCEditColumnsDlg #209, Picture-8 & Picture-9)
        if self.show_columns_modal {
            let mut open = true;
            egui::Window::new("Customize columns")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .fixed_size(Vec2::new(470.0, 360.0))
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Check columns that you want to see in the list. Select column and click Move Up / Move Down to change column order.");
                    ui.add_space(8.0);

                    ui.horizontal(|ui| {
                        // Left: list of columns with checkboxes
                        ui.vertical(|ui| {
                            ui.set_width(220.0);
                            ui.set_min_height(210.0);
                            egui::Frame::group(ui.style()).show(ui, |ui| {
                                egui::ScrollArea::vertical()
                                    .id_salt("columns_customizer_scroll")
                                    .min_scrolled_height(200.0)
                                    .max_height(210.0)
                                    .min_scrolled_width(200.0)
                                    .show(ui, |ui| {
                                        ui.vertical(|ui| {
                                            let mut newly_selected = None;
                                            for (i, col) in self.temp_columns.iter_mut().enumerate() {
                                                let is_selected = self.selected_column_idx == Some(i);
                                                ui.horizontal(|ui| {
                                                    ui.checkbox(&mut col.visible, "");
                                                    let resp = ui.selectable_label(is_selected, &col.name);
                                                    if resp.clicked() {
                                                        newly_selected = Some(i);
                                                    }
                                                });
                                            }
                                            if let Some(i) = newly_selected {
                                                self.selected_column_idx = Some(i);
                                                self.column_width_input = format!("{:.0}", self.temp_columns[i].width);
                                            }
                                        });
                                    });
                            });
                        });

                        ui.add_space(10.0);

                        // Right: action buttons & width input
                        ui.vertical(|ui| {
                            ui.set_width(170.0);
                            let sel = self.selected_column_idx;
                            let len = self.temp_columns.len();
                            let can_up = sel.map(|i| i > 0).unwrap_or(false);
                            let can_down = sel.map(|i| i + 1 < len).unwrap_or(false);

                            if ui.add_enabled(can_up, egui::Button::new("Move Up").min_size(Vec2::new(120.0, 24.0))).clicked() {
                                if let Some(i) = sel {
                                    self.temp_columns.swap(i, i - 1);
                                    self.selected_column_idx = Some(i - 1);
                                }
                            }
                            if ui.add_enabled(can_down, egui::Button::new("Move Down").min_size(Vec2::new(120.0, 24.0))).clicked() {
                                if let Some(i) = sel {
                                    self.temp_columns.swap(i, i + 1);
                                    self.selected_column_idx = Some(i + 1);
                                }
                            }

                            ui.add_space(8.0);
                            if ui.add_enabled(sel.is_some(), egui::Button::new("Show").min_size(Vec2::new(120.0, 24.0))).clicked() {
                                if let Some(i) = sel {
                                    self.temp_columns[i].visible = true;
                                }
                            }
                            if ui.add_enabled(sel.is_some(), egui::Button::new("Hide").min_size(Vec2::new(120.0, 24.0))).clicked() {
                                if let Some(i) = sel {
                                    self.temp_columns[i].visible = false;
                                }
                            }
                            if ui.add(egui::Button::new("Reset").min_size(Vec2::new(120.0, 24.0))).clicked() {
                                self.temp_columns = default_columns();
                                self.selected_column_idx = Some(0);
                                self.column_width_input = format!("{:.0}", self.temp_columns[0].width);
                            }

                            ui.add_space(8.0);
                            ui.label("Width of selected column\nin pixels:");
                            ui.horizontal(|ui| {
                                if ui.text_edit_singleline(&mut self.column_width_input).changed() {
                                    if let Ok(w) = self.column_width_input.parse::<f32>() {
                                        if let Some(i) = sel {
                                            self.temp_columns[i].width = w.max(30.0);
                                        }
                                    }
                                }
                            });
                        });
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("OK").strong().size(13.0)).clicked() {
                            self.columns = self.temp_columns.clone();
                            self.show_columns_modal = false;
                        }
                        if ui.button(RichText::new("Cancel").size(13.0)).clicked() {
                            self.show_columns_modal = false;
                        }
                    });
                });
            if !open {
                self.show_columns_modal = false;
            }
        }

        // 5. "Scheduler and download queues" Dialog (Dialog #308 & #306)
        if self.show_scheduler_modal {
            let mut open = true;
            egui::Window::new("Scheduler and download queues")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .fixed_size(Vec2::new(620.0, 440.0))
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        // Left column: Queues Tree / List
                        ui.vertical(|ui| {
                            ui.set_width(190.0);
                            ui.set_min_height(330.0);
                            egui::Frame::group(ui.style()).show(ui, |ui| {
                                ui.vertical(|ui| {
                                    ui.label(RichText::new("Queues:").strong());
                                    ui.add_space(4.0);
                                    egui::ScrollArea::vertical()
                                        .id_salt("scheduler_queues_list_scroll")
                                        .min_scrolled_height(200.0)
                                        .max_height(220.0)
                                        .min_scrolled_width(170.0)
                                        .show(ui, |ui| {
                                            ui.vertical(|ui| {
                                                for q_name in &self.queues_list {
                                                    let is_sel = self.selected_queue_name == *q_name;
                                                    let icon_src = if q_name.contains("Sync") {
                                                        egui::include_image!("../assets/icons/folder-update.svg")
                                                    } else {
                                                        egui::include_image!("../assets/icons/folder-queue.svg")
                                                    };
                                                    ui.horizontal(|ui| {
                                                        ui.add(egui::Image::new(icon_src).max_size(Vec2::new(14.0, 14.0)));
                                                        if ui.selectable_label(is_sel, q_name).clicked() {
                                                            self.selected_queue_name = q_name.clone();
                                                        }
                                                    });
                                                }
                                            });
                                        });

                                    ui.add_space(6.0);
                                    ui.separator();
                                    ui.horizontal(|ui| {
                                        if ui.button("New queue").clicked() {
                                            self.show_new_queue_input = !self.show_new_queue_input;
                                        }
                                        let is_custom = self.selected_queue_name != "Main download queue" && self.selected_queue_name != "Synchronization queue";
                                        if ui.add_enabled(is_custom, egui::Button::new("Delete")).clicked() {
                                            self.queues_list.retain(|q| q != &self.selected_queue_name);
                                            self.selected_queue_name = "Main download queue".to_string();
                                        }
                                    });
                                    if self.show_new_queue_input {
                                        ui.horizontal(|ui| {
                                            ui.text_edit_singleline(&mut self.new_queue_name_input);
                                            if ui.button("Add").clicked() && !self.new_queue_name_input.is_empty() {
                                                let name = self.new_queue_name_input.trim().to_string();
                                                if !self.queues_list.contains(&name) {
                                                    self.queues_list.push(name.clone());
                                                    self.selected_queue_name = name;
                                                }
                                                self.new_queue_name_input.clear();
                                                self.show_new_queue_input = false;
                                            }
                                        });
                                    }
                                });
                            });
                        });

                        ui.add_space(10.0);

                        // Right column: Schedule / Files in queue tabs
                        ui.vertical(|ui| {
                            ui.set_width(380.0);
                            ui.set_min_height(330.0);
                            ui.horizontal(|ui| {
                                ui.selectable_value(&mut self.scheduler_tab, 0, "Schedule");
                                ui.selectable_value(&mut self.scheduler_tab, 1, "Files in the queue");
                            });
                            ui.separator();

                            if self.scheduler_tab == 0 {
                                egui::ScrollArea::vertical()
                                    .id_salt("scheduler_tab_schedule_scroll")
                                    .min_scrolled_height(270.0)
                                    .max_height(280.0)
                                    .show(ui, |ui| {
                                        ui.vertical(|ui| {
                                            ui.checkbox(&mut self.scheduler_on_startup, "Start download on IDM startup");
                                            ui.checkbox(&mut self.scheduler_active, "Start download at:");
                                            if self.scheduler_active {
                                                ui.horizontal(|ui| {
                                                    ui.label("    Time (HH:MM:SS):");
                                                    ui.text_edit_singleline(&mut self.scheduler_start_time);
                                                });
                                            }

                                            ui.horizontal(|ui| {
                                                ui.label("Stop download at: ");
                                                ui.text_edit_singleline(&mut self.scheduler_stop_time);
                                            });

                                            ui.add_space(6.0);
                                            ui.checkbox(&mut self.scheduler_periodic, "Periodic synchronization");
                                            ui.label("Days of the week:");
                                            ui.horizontal(|ui| {
                                                let days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
                                                for (d_idx, name) in days.iter().enumerate() {
                                                    ui.checkbox(&mut self.scheduler_days[d_idx], *name);
                                                }
                                            });

                                            ui.add_space(6.0);
                                            ui.horizontal(|ui| {
                                                ui.label("Number of retries if download fails:");
                                                ui.add(egui::DragValue::new(&mut self.scheduler_retries).range(0..=99));
                                            });

                                            ui.separator();
                                            ui.checkbox(&mut self.scheduler_hang_up, "Hang up modem when done");
                                            ui.checkbox(&mut self.scheduler_exit_idm, "Exit Internet Download Manager when done");
                                            ui.checkbox(&mut self.scheduler_turn_off, "Turn off computer when done");
                                            if self.scheduler_turn_off {
                                                ui.checkbox(&mut self.scheduler_force_exit, "    Force processes to terminate");
                                            }
                                        });
                                    });
                            } else {
                                ui.vertical(|ui| {
                                    ui.horizontal(|ui| {
                                        ui.label("Download");
                                        ui.add(egui::DragValue::new(&mut self.scheduler_simultaneous).range(1..=10));
                                        ui.label("files at the same time");
                                    });
                                    ui.add_space(4.0);

                                    let cur_q = self.selected_queue_name.clone();
                                    let queue_ids: Vec<u64> = self.downloads.iter()
                                        .filter(|d| {
                                            match (d.queue.as_deref(), cur_q.as_str()) {
                                                (Some("Main"), "Main download queue") => true,
                                                (None, "Main download queue") => true,
                                                (Some("Sync"), "Synchronization queue") => true,
                                                (Some(q), cur) if q == cur => true,
                                                _ => false,
                                            }
                                        })
                                        .map(|d| d.id)
                                        .collect();

                                    // Table Frame with header and rows
                                    egui::Frame::group(ui.style()).show(ui, |ui| {
                                        let hdr_h = 20.0;
                                        ui.horizontal(|ui| {
                                            ui.spacing_mut().item_spacing.x = 4.0;
                                            ui.allocate_ui_with_layout(Vec2::new(170.0, hdr_h), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                ui.label(RichText::new("File Name").strong().size(12.0));
                                            });
                                            ui.separator();
                                            ui.allocate_ui_with_layout(Vec2::new(55.0, hdr_h), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                ui.label(RichText::new("Size").strong().size(12.0));
                                            });
                                            ui.separator();
                                            ui.allocate_ui_with_layout(Vec2::new(55.0, hdr_h), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                ui.label(RichText::new("Status").strong().size(12.0));
                                            });
                                            ui.separator();
                                            ui.allocate_ui_with_layout(Vec2::new(55.0, hdr_h), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                ui.label(RichText::new("Time left").strong().size(12.0));
                                            });
                                        });
                                        ui.separator();

                                        egui::ScrollArea::vertical()
                                            .id_salt("scheduler_tab_files_scroll")
                                            .min_scrolled_height(160.0)
                                            .max_height(170.0)
                                            .show(ui, |ui| {
                                                ui.vertical(|ui| {
                                                    if queue_ids.is_empty() {
                                                        ui.add_space(30.0);
                                                        ui.vertical_centered(|ui| {
                                                            ui.label(RichText::new("No files in this queue.").weak());
                                                        });
                                                    } else {
                                                        for &id in &queue_ids {
                                                            if let Some(item) = self.downloads.iter().find(|d| d.id == id) {
                                                                let is_selected = self.scheduler_selected_id == Some(id);
                                                                let row_resp = ui.horizontal(|ui| {
                                                                    ui.spacing_mut().item_spacing.x = 4.0;
                                                                    ui.allocate_ui_with_layout(Vec2::new(170.0, 18.0), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                                        let icon = get_file_icon_source(&item.filename);
                                                                        ui.add(egui::Image::new(icon).max_size(Vec2::new(14.0, 14.0)));
                                                                        let name_label = if is_selected {
                                                                            RichText::new(&item.filename).color(Color32::WHITE)
                                                                        } else {
                                                                            RichText::new(&item.filename)
                                                                        };
                                                                        ui.add(egui::Label::new(name_label).truncate());
                                                                    });
                                                                    ui.separator();
                                                                    ui.allocate_ui_with_layout(Vec2::new(55.0, 18.0), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                                        ui.label(item.formatted_size());
                                                                    });
                                                                    ui.separator();
                                                                    ui.allocate_ui_with_layout(Vec2::new(55.0, 18.0), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                                        ui.label(item.status.text());
                                                                    });
                                                                    ui.separator();
                                                                    ui.allocate_ui_with_layout(Vec2::new(55.0, 18.0), egui::Layout::left_to_right(egui::Align::Center), |ui| {
                                                                        ui.label(item.formatted_time_left());
                                                                    });
                                                                });

                                                                if is_selected {
                                                                    ui.painter().rect_filled(row_resp.response.rect, 0.0, Color32::from_rgb(0, 95, 175));
                                                                }

                                                                if row_resp.response.interact(egui::Sense::click()).clicked() {
                                                                    self.scheduler_selected_id = Some(id);
                                                                }
                                                            }
                                                        }
                                                    }
                                                });
                                            });
                                    });

                                    ui.add_space(6.0);

                                    // 3 Buttons: Down, Up, Remove from queue (picture-20, picture-21, picture-22)
                                    ui.horizontal(|ui| {
                                        let has_sel = self.scheduler_selected_id.is_some();
                                        let cur_pos = self.scheduler_selected_id.and_then(|id| queue_ids.iter().position(|&qid| qid == id));
                                        let can_down = cur_pos.map(|p| p + 1 < queue_ids.len()).unwrap_or(false);
                                        let can_up = cur_pos.map(|p| p > 0).unwrap_or(false);

                                        // Move item down (picture-22)
                                        let btn_down = egui::Button::image(
                                            egui::Image::new(egui::include_image!("../assets/icons/arrow-down.svg")).max_size(Vec2::new(14.0, 14.0))
                                        ).min_size(Vec2::new(30.0, 26.0));
                                        if ui.add_enabled(can_down, btn_down).on_hover_text("Move item down").clicked() {
                                            if let Some(pos) = cur_pos {
                                                let id1 = queue_ids[pos];
                                                let id2 = queue_ids[pos + 1];
                                                let i1 = self.downloads.iter().position(|d| d.id == id1);
                                                let i2 = self.downloads.iter().position(|d| d.id == id2);
                                                if let (Some(a), Some(b)) = (i1, i2) {
                                                    self.downloads.swap(a, b);
                                                }
                                            }
                                        }

                                        // Move item up (picture-20)
                                        let btn_up = egui::Button::image(
                                            egui::Image::new(egui::include_image!("../assets/icons/arrow-up.svg")).max_size(Vec2::new(14.0, 14.0))
                                        ).min_size(Vec2::new(30.0, 26.0));
                                        if ui.add_enabled(can_up, btn_up).on_hover_text("Move item up").clicked() {
                                            if let Some(pos) = cur_pos {
                                                let id1 = queue_ids[pos];
                                                let id2 = queue_ids[pos - 1];
                                                let i1 = self.downloads.iter().position(|d| d.id == id1);
                                                let i2 = self.downloads.iter().position(|d| d.id == id2);
                                                if let (Some(a), Some(b)) = (i1, i2) {
                                                    self.downloads.swap(a, b);
                                                }
                                            }
                                        }

                                        // Remove item from download queue (picture-21)
                                        let btn_del = egui::Button::image(
                                            egui::Image::new(egui::include_image!("../assets/icons/x.svg")).tint(Color32::from_rgb(220, 50, 50)).max_size(Vec2::new(12.0, 12.0))
                                        ).min_size(Vec2::new(30.0, 26.0));
                                        if ui.add_enabled(has_sel, btn_del).on_hover_text("Remove item from download queue").clicked() {
                                            if let Some(id) = self.scheduler_selected_id {
                                                if let Some(item) = self.downloads.iter_mut().find(|d| d.id == id) {
                                                    item.queue = None;
                                                }
                                                if let Some(pos) = cur_pos {
                                                    if pos + 1 < queue_ids.len() {
                                                        self.scheduler_selected_id = Some(queue_ids[pos + 1]);
                                                    } else if pos > 0 {
                                                        self.scheduler_selected_id = Some(queue_ids[pos - 1]);
                                                    } else {
                                                        self.scheduler_selected_id = None;
                                                    }
                                                }
                                            }
                                        }
                                    });
                                });
                            }
                        });
                    });

                    ui.add_space(8.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("Start now").strong()).clicked() {
                            let q = self.selected_queue_name.clone();
                            self.start_queue(if q.contains("Sync") { "Sync" } else { "Main" });
                        }
                        if ui.button("Stop").clicked() {
                            let q = self.selected_queue_name.clone();
                            self.stop_queue(if q.contains("Sync") { "Sync" } else { "Main" });
                        }
                        if ui.button("Help").clicked() {
                            self.show_tip_modal = true;
                        }
                        if ui.button("Apply").clicked() {
                            self.set_status("Scheduler settings applied".to_string());
                        }
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("Close").clicked() {
                                self.show_scheduler_modal = false;
                            }
                        });
                    });
                });
            if !open {
                self.show_scheduler_modal = false;
            }
        }

        // 6. "Batch Download" Dialog (Dialog #280)
        if self.show_batch_download_modal {
            let mut open = true;
            egui::Window::new("Batch download")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(460.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Address (use * wildcard for sequence):");
                    ui.text_edit_singleline(&mut self.batch_url_pattern);
                    ui.add_space(8.0);

                    ui.group(|ui| {
                        ui.label(RichText::new("Replace asterisk (*) with:").strong());
                        ui.radio_value(&mut self.batch_is_number, true, "Numbers");
                        if self.batch_is_number {
                            ui.horizontal(|ui| {
                                ui.label("From:");
                                ui.add(egui::DragValue::new(&mut self.batch_from));
                                ui.label("To:");
                                ui.add(egui::DragValue::new(&mut self.batch_to));
                                ui.label("Wildcard size:");
                                ui.add(egui::DragValue::new(&mut self.batch_wildcard_size).range(1..=6));
                            });
                        }
                        ui.radio_value(&mut self.batch_is_number, false, "Letters (a - z)");
                    });

                    ui.add_space(8.0);
                    ui.checkbox(&mut self.batch_auth, "Use authorization");
                    if self.batch_auth {
                        ui.horizontal(|ui| {
                            ui.label("User:    ");
                            ui.text_edit_singleline(&mut self.batch_user);
                        });
                        ui.horizontal(|ui| {
                            ui.label("Password:");
                            ui.add(egui::TextEdit::singleline(&mut self.batch_pass).password(true));
                        });
                    }

                    ui.add_space(12.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("OK").strong()).clicked() {
                            let mut count = 0;
                            if self.batch_url_pattern.contains('*') {
                                if self.batch_is_number {
                                    for n in self.batch_from..=self.batch_to {
                                        let num_str = format!("{:0width$}", n, width = self.batch_wildcard_size);
                                        let u = self.batch_url_pattern.replace('*', &num_str);
                                        let raw_fname = u.split('/').last().unwrap_or("batch.dat").split('?').next().unwrap_or("batch.dat");
                                        let (fname, save_p) = self.resolve_unique_download_target(raw_fname);
                                        let cat = Category::from_extension(fname.split('.').last().unwrap_or(""));
                                        let id = self.next_id;
                                        self.next_id += 1;
                                        let item = DownloadItem {
                                            id,
                                            url: u,
                                            filename: fname,
                                            category: cat,
                                            save_path: save_p,
                                            total_size: None,
                                            downloaded_bytes: 0,
                                            status: DownloadStatus::Paused,
                                            transfer_rate: 0.0,
                                            date_added: Local::now(),
                                            max_connections: self.config.default_connections,
                                            queue: Some("Main".to_string()),
                                            ..Default::default()
                                        };
                                        let settings = DownloadSettings {
                                            download_id: id,
                                            max_connections: self.config.default_connections,
                                            chunk_size: 2 * 1024 * 1024,
                                            speed_limit: None,
                                            retry_limit: 5,
                                        };
                                        if let Err(e) = self.db.insert_download(&item, &settings) {
                                            eprintln!("Database error inserting batch item: {}", e);
                                        }
                                        self.mark_task_mutated(item.id);
                                        self.downloads.push(item);
                                        count += 1;
                                    }
                                } else {
                                    for c in b'a'..=b'z' {
                                        let ch_str = (c as char).to_string();
                                        let u = self.batch_url_pattern.replace('*', &ch_str);
                                        let raw_fname = u.split('/').last().unwrap_or("batch.dat").split('?').next().unwrap_or("batch.dat");
                                        let (fname, save_p) = self.resolve_unique_download_target(raw_fname);
                                        let cat = Category::from_extension(fname.split('.').last().unwrap_or(""));
                                        let id = self.next_id;
                                        self.next_id += 1;
                                        let item = DownloadItem {
                                            id,
                                            url: u,
                                            filename: fname,
                                            category: cat,
                                            save_path: save_p,
                                            total_size: None,
                                            downloaded_bytes: 0,
                                            status: DownloadStatus::Paused,
                                            transfer_rate: 0.0,
                                            date_added: Local::now(),
                                            max_connections: self.config.default_connections,
                                            queue: Some("Main".to_string()),
                                            ..Default::default()
                                        };
                                        let settings = DownloadSettings {
                                            download_id: id,
                                            max_connections: self.config.default_connections,
                                            chunk_size: 2 * 1024 * 1024,
                                            speed_limit: None,
                                            retry_limit: 5,
                                        };
                                        if let Err(e) = self.db.insert_download(&item, &settings) {
                                            eprintln!("Database error inserting batch item: {}", e);
                                        }
                                        self.mark_task_mutated(item.id);
                                        self.downloads.push(item);
                                        count += 1;
                                    }
                                }
                                self.set_status(format!("Added {} batch items", count));
                            }
                            self.show_batch_download_modal = false;
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_batch_download_modal = false;
                        }
                    });
                });
            if !open {
                self.show_batch_download_modal = false;
            }
        }

        // 7. "Speed Limiter" Settings Dialog (Dialog #337)
        if self.show_speed_limiter_modal {
            let mut open = true;
            egui::Window::new("Speed Limiter settings")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(360.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.checkbox(&mut self.config.speed_limiter_enabled, "Always turn on Speed Limiter on IDM startup");
                    ui.add_space(8.0);
                    ui.label("Maximum download speed for one file / global:");
                    ui.horizontal(|ui| {
                        ui.add(egui::DragValue::new(&mut self.config.speed_limit_kbps).range(1..=100000));
                        ui.label("KBytes/sec");
                    });
                    ui.add_space(12.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("OK").strong()).clicked() {
                            self.show_speed_limiter_modal = false;
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_speed_limiter_modal = false;
                        }
                    });
                });
            if !open {
                self.show_speed_limiter_modal = false;
            }
        }

        // 8. "Site Grabber" Wizard Dialog (Dialog #284)
        if self.show_site_grabber_modal {
            let mut open = true;
            egui::Window::new(format!("Site Grabber - Step {} of 4", self.grabber_step))
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(480.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    match self.grabber_step {
                        1 => {
                            ui.heading("Step 1: Project Name & Start Address");
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                ui.label("Project name: ");
                                ui.text_edit_singleline(&mut self.grabber_project_name);
                            });
                            ui.horizontal(|ui| {
                                ui.label("Start address:");
                                ui.text_edit_singleline(&mut self.grabber_start_url);
                            });
                            ui.add_space(6.0);
                            ui.label("Project template:");
                            ui.selectable_value(&mut self.grabber_file_filter, 0, "Complete website (all files)");
                            ui.selectable_value(&mut self.grabber_file_filter, 1, "All pictures from the website");
                            ui.selectable_value(&mut self.grabber_file_filter, 2, "All audio and video from the website");
                            ui.selectable_value(&mut self.grabber_file_filter, 3, "All documents from the website");
                        }
                        2 => {
                            ui.heading("Step 2: Save Destination");
                            ui.add_space(6.0);
                            ui.label("Download folder for grabber files:");
                            let mut path_str = self.grabber_save_dir.to_string_lossy().to_string();
                            if ui.text_edit_singleline(&mut path_str).changed() {
                                self.grabber_save_dir = PathBuf::from(path_str);
                            }
                        }
                        3 => {
                            ui.heading("Step 3: Exploration Depth");
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                ui.label("Levels of links to explore:");
                                ui.add(egui::DragValue::new(&mut self.grabber_depth).range(1..=10));
                            });
                            ui.checkbox(&mut self.scheduler_hang_up, "Ignore popup windows");
                            ui.checkbox(&mut self.scheduler_exit_idm, "Don't explore other sites / domains");
                        }
                        _ => {
                            ui.heading("Step 4: Download Settings & Confirmation");
                            ui.add_space(6.0);
                            ui.label(format!("Ready to grab from: {}", self.grabber_start_url));
                            ui.label(format!("Target folder: {}", self.grabber_save_dir.display()));
                            ui.label(format!("Depth: {} level(s)", self.grabber_depth));
                        }
                    }

                    ui.add_space(16.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if self.grabber_step > 1 {
                            if ui.button("< Back").clicked() {
                                self.grabber_step -= 1;
                            }
                        }
                        if self.grabber_step < 4 {
                            if ui.button("Next >").clicked() {
                                self.grabber_step += 1;
                            }
                        } else {
                            if ui.button(RichText::new("Start Grabber").strong()).clicked() {
                                let test_files = match self.grabber_file_filter {
                                    1 => vec!["photo1.jpg", "banner.png", "logo.svg"],
                                    2 => vec!["song.mp3", "podcast.m4a", "video.mp4"],
                                    3 => vec!["manual.pdf", "readme.txt", "specs.docx"],
                                    _ => vec!["index.html", "script.js", "style.css", "image.png"],
                                };
                                for f in test_files {
                                    let id = self.next_id;
                                    self.next_id += 1;
                                    let url = format!("{}/{}", self.grabber_start_url.trim_end_matches('/'), f);
                                    let cat = Category::from_extension(f.split('.').last().unwrap_or(""));
                                    let save_p = self.grabber_save_dir.join(f);
                                    let item = DownloadItem {
                                        id,
                                        url,
                                        filename: f.to_string(),
                                        category: cat,
                                        save_path: save_p,
                                        total_size: None,
                                        downloaded_bytes: 0,
                                        status: DownloadStatus::Paused,
                                        transfer_rate: 0.0,
                                        date_added: Local::now(),
                                        max_connections: self.config.default_connections,
                                        queue: Some("Main".to_string()),
                                        ..Default::default()
                                    };
                                    let settings = DownloadSettings {
                                        download_id: id,
                                        max_connections: self.config.default_connections,
                                        chunk_size: 2 * 1024 * 1024,
                                        speed_limit: None,
                                        retry_limit: 5,
                                    };
                                    if let Err(e) = self.db.insert_download(&item, &settings) {
                                        eprintln!("Database error inserting site grabber item: {}", e);
                                    }
                                    self.mark_task_mutated(item.id);
                                    self.downloads.push(item);
                                }
                                self.set_status("Site Grabber project created".to_string());
                                self.show_site_grabber_modal = false;
                            }
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_site_grabber_modal = false;
                        }
                    });
                });
            if !open {
                self.show_site_grabber_modal = false;
            }
        }

        // 9. "Find" Dialog
        if self.show_find_modal {
            let mut open = true;
            egui::Window::new("Find")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(340.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Find what:");
                        ui.text_edit_singleline(&mut self.find_query);
                    });
                    ui.checkbox(&mut self.find_match_case, "Match case");
                    ui.add_space(8.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("Find Next").strong()).clicked() {
                            self.find_next();
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_find_modal = false;
                        }
                    });
                });
            if !open {
                self.show_find_modal = false;
            }
        }

        // 10. "Tip of the Day" Dialog
        if self.show_tip_modal {
            let mut open = true;
            egui::Window::new("Tip of the Day")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(420.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    let tips = [
                        "Did you know that you can customize which columns are visible in the download list? Just right-click on any column header and select 'Columns...' or go to View -> Customize URL list.",
                        "Did you know that you can set download speed limits to preserve bandwidth for gaming or streaming? Downloads -> Speed Limiter -> Settings...",
                        "Did you know that IDM can automatically disconnect or shut down your PC when the download queue finishes? Check Scheduler -> Files in the queue / Completion actions.",
                        "Did you know that you can add a sequence of downloads at once using asterisks? Tasks -> Add batch download...",
                        "Did you know that you can toggle between Light and Dark themes anytime from View -> Switch to Dark/Light Theme?",
                    ];
                    ui.horizontal(|ui| {
                        ui.add(egui::Image::new(egui::include_image!("../assets/icons/lightbulb.svg")).tint(Color32::from_rgb(255, 215, 0)).max_size(Vec2::new(18.0, 18.0)));
                        ui.heading("Did you know...");
                    });
                    ui.add_space(6.0);
                    ui.label(tips[self.tip_index % tips.len()]);
                    ui.add_space(12.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button("Next Tip").clicked() {
                            self.tip_index = (self.tip_index + 1) % tips.len();
                        }
                        if ui.button("Previous Tip").clicked() {
                            self.tip_index = (self.tip_index + tips.len() - 1) % tips.len();
                        }
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("Close").clicked() {
                                self.show_tip_modal = false;
                            }
                        });
                    });
                });
            if !open {
                self.show_tip_modal = false;
            }
        }

        // 11. "Check for Updates" Dialog
        if self.show_update_modal {
            let mut open = true;
            egui::Window::new("Quick Update")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(350.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.heading("Not Download Manager");
                    ui.label("You have the latest version of Internet Download Manager installed.");
                    ui.label("Current build: 6.42 Build 25 (eframe/egui Rust Engine).");
                    ui.add_space(8.0);
                    ui.separator();
                    if ui.button("Close").clicked() {
                        self.show_update_modal = false;
                    }
                });
            if !open {
                self.show_update_modal = false;
            }
        }

        // 12. "Drop Target" Window
        if self.show_drop_target {
            egui::Window::new("Drop Target")
                .open(&mut self.show_drop_target)
                .resizable(false)
                .collapsible(false)
                .default_size(Vec2::new(120.0, 90.0))
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add(egui::Image::new(egui::include_image!("../assets/icons/folder-download.svg")).max_size(Vec2::new(28.0, 28.0)));
                        ui.label(RichText::new("Drop links here").small());
                    });
                });
        }

        // 13. "Registration" Dialog
        if self.show_registration_modal {
            let mut open = true;
            egui::Window::new("Internet Download Manager Registration")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(400.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Please enter your registration details in the corresponding fields and push OK button:");
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        ui.label("First Name:");
                        ui.text_edit_singleline(&mut self.reg_first_name);
                    });
                    ui.horizontal(|ui| {
                        ui.label("Last Name:");
                        ui.text_edit_singleline(&mut self.reg_last_name);
                    });
                    ui.horizontal(|ui| {
                        ui.label("Email:       ");
                        ui.text_edit_singleline(&mut self.reg_email);
                    });
                    ui.horizontal(|ui| {
                        ui.label("Serial:      ");
                        ui.text_edit_singleline(&mut self.reg_serial);
                    });
                    ui.add_space(12.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button("OK").clicked() {
                            self.show_registration_modal = false;
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_registration_modal = false;
                        }
                    });
                });
            if !open {
                self.show_registration_modal = false;
            }
        }

        // 14. "About" Dialog
        if self.show_about_modal {
            let mut open = true;
            egui::Window::new("About Not Download Manager")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .default_width(420.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.heading("Not Download Manager");
                    ui.label("Cross-Platform Modern Edition built in Rust with eframe/egui");
                    ui.separator();
                    ui.label("Features:");
                    ui.label("• Multi-part range HTTP chunking (up to 32 parallel streams)");
                    ui.label("• High-speed connection visualizer & categorizer");
                    ui.label("• Global speed limiter & queue scheduler");
                    ui.label("• Options dialog with full 9-tab configuration suite");
                    ui.label("• Authentic columns management and customize URL list");
                    ui.add_space(12.0);
                    ui.separator();
                    if ui.button("OK").clicked() {
                        self.show_about_modal = false;
                    }
                });
            if !open {
                self.show_about_modal = false;
            }
        }
    }

    #[cfg(test)]
    pub fn new_for_test(db: Arc<crate::db::Database>) -> (Self, tokio::sync::mpsc::UnboundedSender<EngineEvent>, tokio::sync::mpsc::UnboundedReceiver<EngineCommand>) {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (evt_tx, evt_rx) = tokio::sync::mpsc::unbounded_channel();
        let engine = DownloadEngineHandle {
            cmd_sender: cmd_tx,
            event_receiver: evt_rx,
            active_tasks: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            known_paths: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            active_workers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let config = AppConfig::default();
        let initial_max_id = db.get_max_download_id().unwrap_or(0);

        (Self {
            downloads: Vec::new(),
            selected_id: None,
            selected_category: Category::All,
            status_filter: None,
            queue_filter: None,
            categories_visible: true,
            config: config.clone(),

            tree_all_expanded: true,
            tree_unfinished_expanded: false,
            tree_finished_expanded: false,
            tree_grabber_expanded: false,
            tree_queues_expanded: true,

            show_add_url_modal: false,
            new_download_url: "https://proof.ovh.net/files/100Mb.dat".to_string(),
            new_download_category: Category::General,
            use_authorization: false,
            auth_login: String::new(),
            auth_pass: String::new(),

            show_options_modal: false,
            options_tab: 0,
            new_site_url: String::new(),
            new_site_user: String::new(),
            new_site_pass: String::new(),

            show_scheduler_modal: false,
            scheduler_start_time: "02:00".to_string(),
            scheduler_stop_time: "06:00".to_string(),
            scheduler_active: false,

            show_progress_modal: false,
            progress_modal_id: None,
            progress_view_connections: false,

            show_about_modal: false,
            show_registration_modal: false,
            reg_first_name: String::new(),
            reg_last_name: String::new(),
            reg_email: String::new(),
            reg_serial: String::new(),

            columns: default_columns(),
            temp_columns: default_columns(),
            show_columns_modal: false,
            selected_column_idx: None,
            column_width_input: "260".to_string(),

            sort_column: None,
            sort_ascending: true,

            show_batch_download_modal: false,
            batch_url_pattern: String::new(),
            batch_is_number: true,
            batch_from: 1,
            batch_to: 10,
            batch_wildcard_size: 2,
            batch_auth: false,
            batch_user: String::new(),
            batch_pass: String::new(),

            show_find_modal: false,
            find_query: String::new(),
            find_match_case: false,

            show_speed_limiter_modal: false,

            show_site_grabber_modal: false,
            grabber_step: 0,
            grabber_project_name: "MyProject".to_string(),
            grabber_start_url: String::new(),
            grabber_save_dir: config.default_download_dir.clone(),
            grabber_depth: 2,
            grabber_file_filter: 0,

            scheduler_tab: 0,
            selected_queue_name: "Main".to_string(),
            queues_list: vec!["Main".to_string(), "Synchronization".to_string()],
            new_queue_name_input: String::new(),
            show_new_queue_input: false,
            scheduler_periodic: false,
            scheduler_days: [true, true, true, true, true, true, true],
            scheduler_retries: 10,
            scheduler_hang_up: false,
            scheduler_exit_idm: false,
            scheduler_turn_off: false,
            scheduler_force_exit: false,
            scheduler_on_startup: false,
            scheduler_open_file: String::new(),
            scheduler_simultaneous: 1,
            scheduler_selected_id: None,

            show_tip_modal: false,
            tip_index: 0,
            show_update_modal: false,
            show_drop_target: false,
            dark_mode: true,
            status_message: None,

            engine,
            next_id: initial_max_id + 1,
            db: db.clone(),
            is_reconciling: false,
            reconcile_rx: None,
            reconcile_handle: None,
            deleted_task_ids: std::collections::HashSet::new(),
            deleting_task_ids: std::collections::HashSet::new(),
            task_versions: std::collections::HashMap::new(),
            cleanup_errors: std::collections::HashMap::new(),

            screenshot_path: None,
            frame_counter: 0,
            screenshot_requested: false,
            screenshot_saved: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }, evt_tx, cmd_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::types::{DownloadItem, DownloadSettings, DownloadStatus, HashVerificationStatus};
    use std::sync::Arc;

    fn make_test_item(id: u64, status: DownloadStatus) -> DownloadItem {
        DownloadItem {
            id,
            url: format!("https://example.com/file{}.zip", id),
            filename: format!("file{}.zip", id),
            category: Category::General,
            save_path: PathBuf::from(format!("downloads/file{}.zip", id)),
            total_size: Some(10_000_000),
            downloaded_bytes: 0,
            status,
            transfer_rate: 0.0,
            time_left_secs: None,
            resume_capable: true,
            date_added: Local::now(),
            max_connections: 4,
            segments: vec![],
            description: String::new(),
            queue: Some("Main".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn test_reconciliation_merge_does_not_clobber_user_added_tasks() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        for id in 1..=3 {
            let item = make_test_item(id, DownloadStatus::Paused);
            db.insert_download(&item, &DownloadSettings::default()).unwrap();
        }
        assert_eq!(db.get_max_download_id().unwrap(), 3);

        let (mut app, _evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());
        assert_eq!(app.next_id, 4);

        // User adds task 4 during startup before reconciliation finishes
        let user_item = make_test_item(4, DownloadStatus::Downloading);
        db.insert_download(&user_item, &DownloadSettings::default()).unwrap();
        app.downloads.push(user_item);
        app.mark_task_mutated(4);
        app.next_id = 5;

        // Reconciliation snapshot delivers tasks 1, 2, 3
        let (tx, rx) = std::sync::mpsc::channel();
        let items = vec![
            make_test_item(1, DownloadStatus::Paused),
            make_test_item(2, DownloadStatus::Paused),
            make_test_item(3, DownloadStatus::Paused),
        ];
        tx.send(Ok(items)).unwrap();
        app.is_reconciling = true;
        app.reconcile_rx = Some(rx);

        app.check_background_reconciliation();

        assert_eq!(app.downloads.len(), 4, "Must preserve user-added task alongside 3 reconciled tasks");
        let ids: Vec<u64> = app.downloads.iter().map(|d| d.id).collect();
        assert_eq!(ids, vec![1, 2, 3, 4]);
        assert_eq!(app.downloads[3].status, DownloadStatus::Downloading);
        assert!(app.next_id >= 5);
    }

    #[test]
    fn test_reconciliation_tombstone_prevents_resurrection_of_deleted_tasks() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        for id in 1..=3 {
            let item = make_test_item(id, DownloadStatus::Completed);
            db.insert_download(&item, &DownloadSettings::default()).unwrap();
        }

        let (mut app, _evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());

        // User deletes task 2 during reconciliation window
        app.deleted_task_ids.insert(2);
        app.db.delete_download(2).unwrap();

        // Late reconciliation snapshot contains [1, 2, 3]
        let (tx, rx) = std::sync::mpsc::channel();
        let items = vec![
            make_test_item(1, DownloadStatus::Completed),
            make_test_item(2, DownloadStatus::Completed),
            make_test_item(3, DownloadStatus::Completed),
        ];
        tx.send(Ok(items)).unwrap();
        app.is_reconciling = true;
        app.reconcile_rx = Some(rx);

        app.check_background_reconciliation();

        assert_eq!(app.downloads.len(), 2);
        let ids: Vec<u64> = app.downloads.iter().map(|d| d.id).collect();
        assert_eq!(ids, vec![1, 3], "Task 2 must NOT be resurrected");
    }

    #[test]
    fn test_reconciliation_preserves_in_flight_progress_and_mutations() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let item1 = make_test_item(1, DownloadStatus::Paused);
        db.insert_download(&item1, &DownloadSettings::default()).unwrap();

        let (mut app, _evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());

        // In-memory task 1 is already downloading and mutated
        let mut active_item1 = make_test_item(1, DownloadStatus::Downloading);
        active_item1.downloaded_bytes = 4_500_000;
        active_item1.transfer_rate = 1_200_000.0;
        app.downloads.push(active_item1);
        app.mark_task_mutated(1);

        // Snapshot delivers stale disk state: Paused, 0 bytes, 0 rate
        let (tx, rx) = std::sync::mpsc::channel();
        tx.send(Ok(vec![make_test_item(1, DownloadStatus::Paused)])).unwrap();
        app.is_reconciling = true;
        app.reconcile_rx = Some(rx);

        app.check_background_reconciliation();

        assert_eq!(app.downloads.len(), 1);
        assert_eq!(app.downloads[0].status, DownloadStatus::Downloading);
        assert_eq!(app.downloads[0].downloaded_bytes, 4_500_000);
        assert_eq!(app.downloads[0].transfer_rate, 1_200_000.0);
    }

    #[test]
    fn test_deleted_event_cleanup_failure_transitions_to_cleanup_pending() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let item = make_test_item(10, DownloadStatus::Downloading);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let (mut app, evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());
        app.downloads.push(item);
        app.selected_id = Some(10);

        // Simulate engine reporting deletion failed due to Windows file lock (os error 32)
        let evt = EngineEvent::Deleted {
            id: 10,
            cleanup_success: false,
            error: Some("The process cannot access the file because it is being used by another process. (os error 32)".into()),
        };
        evt_tx.send(evt).unwrap();
        app.poll_engine_events();

        // Must retain in UI downloads list with status CleanupPending
        assert_eq!(app.downloads.len(), 1);
        assert_eq!(app.downloads[0].id, 10);
        assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);
        assert_eq!(app.downloads[0].transfer_rate, 0.0);
        assert_eq!(app.downloads[0].time_left_secs, None);
        assert!(app.cleanup_errors.get(&10).is_some());
        assert!(!app.deleted_task_ids.contains(&10));

        // SQLite record must NOT be deleted, and status must be CleanupPending
        assert!(db.download_exists(10).unwrap());
        let reloaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(reloaded[0].status, DownloadStatus::CleanupPending);
    }

    #[test]
    fn test_retry_cleanup_flow_and_successful_purging() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let item = make_test_item(20, DownloadStatus::CleanupPending);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let (mut app, evt_tx, mut cmd_rx) = IdmApp::new_for_test(db.clone());
        app.downloads.push(item);
        app.selected_id = Some(20);
        app.cleanup_errors.insert(20, "Locked".into());

        // User clicks retry cleanup
        app.retry_cleanup_selected();
        assert!(app.deleting_task_ids.contains(&20));
        assert_eq!(app.downloads[0].status, DownloadStatus::Error("Cleaning up...".into()));

        // Assert command sent to engine
        let cmd = cmd_rx.try_recv().unwrap();
        match cmd {
            EngineCommand::DeleteDownload { id, cleanup_files } => {
                assert_eq!(id, 20);
                assert!(cleanup_files);
            }
            _ => panic!("Expected DeleteDownload command"),
        }

        // Engine confirms cleanup succeeded
        let evt = EngineEvent::Deleted {
            id: 20,
            cleanup_success: true,
            error: None,
        };
        evt_tx.send(evt).unwrap();
        app.poll_engine_events();

        // Must be purged from UI memory and recorded in tombstones
        assert_eq!(app.downloads.len(), 0);
        assert!(app.deleted_task_ids.contains(&20));
        assert!(!app.deleting_task_ids.contains(&20));
        assert!(app.cleanup_errors.get(&20).is_none());

        // SQLite record must be expunged
        assert!(!db.download_exists(20).unwrap());
    }

    #[test]
    fn test_finished_event_persists_hash_verification_status() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let item = make_test_item(30, DownloadStatus::Downloading);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let (mut app, evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());
        app.downloads.push(item);

        let evt = EngineEvent::Finished {
            id: 30,
            hash_status: HashVerificationStatus::Verified,
        };
        evt_tx.send(evt).unwrap();
        app.poll_engine_events();

        assert_eq!(app.downloads[0].status, DownloadStatus::Completed);
        assert_eq!(app.downloads[0].hash_status, HashVerificationStatus::Verified);

        // Verification status must be persisted in SQLite
        let (_, hs) = db.get_hash_status(30).unwrap();
        assert_eq!(hs, HashVerificationStatus::Verified);
    }

    #[test]
    fn test_progress_update_does_not_clobber_terminal_or_paused_states() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let mut item = make_test_item(40, DownloadStatus::Completed);
        item.downloaded_bytes = 10_000_000;
        item.transfer_rate = 0.0;
        item.time_left_secs = None;
        db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let (mut app, evt_tx, _cmd_rx) = IdmApp::new_for_test(db);
        app.downloads.push(item);

        // Out-of-order late ProgressUpdate packet arriving after Finished
        let evt = EngineEvent::ProgressUpdate {
            id: 40,
            downloaded: 9_000_000,
            speed: 500_000.0,
            time_left: Some(2),
            segments: vec![],
            connection_segments: vec![],
        };
        evt_tx.send(evt).unwrap();
        app.poll_engine_events();

        // Progress update must be ignored
        assert_eq!(app.downloads[0].status, DownloadStatus::Completed);
        assert_eq!(app.downloads[0].downloaded_bytes, 10_000_000);
        assert_eq!(app.downloads[0].transfer_rate, 0.0);
        assert_eq!(app.downloads[0].time_left_secs, None);
    }

    #[test]
    fn test_guards_reject_cleanup_pending_for_resume_and_redownload() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let item = make_test_item(50, DownloadStatus::CleanupPending);
        db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let (mut app, _evt_tx, _cmd_rx) = IdmApp::new_for_test(db);
        app.downloads.push(item);
        app.selected_id = Some(50);

        app.resume_selected();
        assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);

        app.redownload_selected();
        assert_eq!(app.downloads[0].status, DownloadStatus::CleanupPending);
    }

    #[test]
    fn test_user_added_task_suppresses_sample_downloads() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let (mut app, _evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());

        // User added a task on clean start before reconciliation returned
        let user_item = make_test_item(1, DownloadStatus::Queued);
        app.downloads.push(user_item);

        // Background reconciler reports empty DB
        let (tx, rx) = std::sync::mpsc::channel();
        tx.send(Ok(vec![])).unwrap();
        app.is_reconciling = true;
        app.reconcile_rx = Some(rx);

        app.check_background_reconciliation();

        assert_eq!(app.downloads.len(), 1, "Must NOT inject sample downloads when user has already added tasks");
        assert_eq!(app.downloads[0].id, 1);
    }

    #[test]
    fn test_resolve_unique_download_target_disambiguation() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let (mut app, _evt_tx, _cmd_rx) = IdmApp::new_for_test(db.clone());

        let target_dir = std::env::temp_dir().join("idm_test_unique_targets");
        let _ = std::fs::create_dir_all(&target_dir);
        app.config.default_download_dir = target_dir.clone();

        let (name1, path1) = app.resolve_unique_download_target("kubuntu.iso");
        assert_eq!(name1, "kubuntu.iso");
        assert_eq!(path1, target_dir.join("kubuntu.iso"));

        // Add first item to app.downloads
        let mut item1 = make_test_item(1, DownloadStatus::Paused);
        item1.filename = name1;
        item1.save_path = path1;
        app.downloads.push(item1);

        // Next download with same name must disambiguate to _1
        let (name2, path2) = app.resolve_unique_download_target("kubuntu.iso");
        assert_eq!(name2, "kubuntu_1.iso");
        assert_eq!(path2, target_dir.join("kubuntu_1.iso"));

        // Add second item to app.downloads
        let mut item2 = make_test_item(2, DownloadStatus::Paused);
        item2.filename = name2;
        item2.save_path = path2;
        app.downloads.push(item2);

        // Next download with same name must disambiguate to _2
        let (name3, path3) = app.resolve_unique_download_target("kubuntu.iso");
        assert_eq!(name3, "kubuntu_2.iso");
        assert_eq!(path3, target_dir.join("kubuntu_2.iso"));

        let _ = std::fs::remove_dir_all(&target_dir);
    }
}
