use std::path::{Path, PathBuf};
use std::sync::Mutex;
use chrono::{Local, TimeZone};
use rusqlite::{params, Connection, OptionalExtension};

use crate::types::{
    Category, DownloadItem, DownloadSettings, DownloadStatus, HashVerificationStatus,
    SegmentProgress,
};

pub fn hash_status_to_str(status: HashVerificationStatus) -> &'static str {
    status.as_str()
}

pub fn hash_status_from_str(s: &str) -> HashVerificationStatus {
    HashVerificationStatus::from_str(s)
}

#[derive(Debug)]
pub enum DbError {
    Sqlite(rusqlite::Error),
    Corrupted(String),
    NotFound(u64),
    Io(std::io::Error),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Sqlite(e) => write!(f, "SQLite error: {}", e),
            DbError::Corrupted(msg) => write!(f, "Database corruption error: {}", msg),
            DbError::NotFound(id) => write!(f, "Download id {} not found", id),
            DbError::Io(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for DbError {}

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::Sqlite(e)
    }
}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::Io(e)
    }
}

pub struct Database {
    conn: Mutex<Connection>,
    db_path: PathBuf,
}

impl Database {
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let db_path = path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Attempt opening SQLite database, handle corruption gracefully
        let conn = match Connection::open(&db_path) {
            Ok(c) => {
                let integrity: Result<String, _> = c.query_row("PRAGMA integrity_check", [], |r| r.get(0));
                match integrity {
                    Ok(val) if val == "ok" => c,
                    _ => {
                        drop(c);
                        let bak_path = db_path.with_extension("db.corrupt.bak");
                        let _ = std::fs::rename(&db_path, &bak_path);
                        Connection::open(&db_path)?
                    }
                }
            }
            Err(_) => {
                // If corrupted, backup corrupted file and create fresh
                let bak_path = db_path.with_extension("db.corrupt.bak");
                let _ = std::fs::rename(&db_path, &bak_path);
                Connection::open(&db_path)?
            }
        };

        let db = Self {
            conn: Mutex::new(conn),
            db_path,
        };

        db.init_and_migrate()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn: Mutex::new(conn),
            db_path: PathBuf::from(":memory:"),
        };
        db.init_and_migrate()?;
        Ok(db)
    }

    fn init_and_migrate(&self) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();

        // Performance & safety pragmas
        let _ = conn.busy_timeout(std::time::Duration::from_secs(5));
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )?;

        let user_version: u32 = conn.query_row(
            "PRAGMA user_version",
            [],
            |row| row.get(0),
        )?;

        if user_version < 1 {
            let tx = conn.unchecked_transaction()?;

            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    save_path TEXT NOT NULL,
                    total_size INTEGER,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    error_message TEXT,
                    etag TEXT,
                    last_modified TEXT,
                    expected_sha256 TEXT,
                    hash_status TEXT DEFAULT 'NotConfigured'
                );

                CREATE TABLE IF NOT EXISTS download_settings (
                    download_id INTEGER PRIMARY KEY,
                    max_connections INTEGER NOT NULL DEFAULT 8,
                    chunk_size INTEGER NOT NULL DEFAULT 2097152,
                    speed_limit INTEGER,
                    retry_limit INTEGER NOT NULL DEFAULT 5,
                    FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
                CREATE INDEX IF NOT EXISTS idx_downloads_created_at ON downloads(created_at);
                PRAGMA user_version = 1;",
            )?;

            tx.commit()?;
        } else {
            // Backward-compatible column addition for pre-existing version 1 databases
            let has_hash_status: bool = {
                let mut stmt = conn.prepare("PRAGMA table_info(downloads)")?;
                let mut rows = stmt.query([])?;
                let mut found = false;
                while let Some(row) = rows.next()? {
                    let col_name: String = row.get(1)?;
                    if col_name == "hash_status" {
                        found = true;
                        break;
                    }
                }
                found
            };
            if !has_hash_status {
                conn.execute(
                    "ALTER TABLE downloads ADD COLUMN hash_status TEXT DEFAULT 'NotConfigured'",
                    [],
                )?;
            }
        }

        Ok(())
    }

    /// Reconciles persisted download tasks with actual disk state and `.partinfo` metadata.
    pub fn reconcile_and_load_all(&self) -> Result<Vec<DownloadItem>, DbError> {
        let mut conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT 
                d.id, d.url, d.filename, d.save_path, d.total_size, d.status,
                d.created_at, d.updated_at, d.completed_at, d.error_message,
                d.etag, d.last_modified, d.expected_sha256, d.hash_status,
                s.max_connections, s.chunk_size, s.speed_limit, s.retry_limit
             FROM downloads d
             LEFT JOIN download_settings s ON d.id = s.download_id
             ORDER BY d.id ASC"
        )?;

        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let url: String = row.get(1)?;
            let filename: String = row.get(2)?;
            let save_path_str: String = row.get(3)?;
            let total_size: Option<i64> = row.get(4)?;
            let status_str: String = row.get(5)?;
            let created_at: i64 = row.get(6)?;
            let _updated_at: i64 = row.get(7)?;
            let _completed_at: Option<i64> = row.get(8)?;
            let error_message: Option<String> = row.get(9)?;
            let etag: Option<String> = row.get(10)?;
            let _last_modified: Option<String> = row.get(11)?;
            let expected_sha256: Option<String> = row.get(12)?;
            let hash_status_str: Option<String> = row.get(13)?;
            let max_connections: Option<i64> = row.get(14)?;
            let _chunk_size: Option<i64> = row.get(15)?;
            let _speed_limit: Option<i64> = row.get(16)?;
            let _retry_limit: Option<i64> = row.get(17)?;

            let hash_status = hash_status_str
                .as_deref()
                .map(hash_status_from_str)
                .unwrap_or(HashVerificationStatus::NotConfigured);

            Ok((
                id as u64,
                url,
                filename,
                PathBuf::from(save_path_str),
                total_size.map(|s| s as u64),
                status_str,
                created_at,
                error_message,
                max_connections.unwrap_or(8) as usize,
                etag,
                expected_sha256,
                hash_status,
            ))
        })?;

        let mut items = Vec::new();
        let mut updates_to_apply: Vec<(u64, String, Option<String>, Option<(String, String)>)> = Vec::new();
        let mut seen_paths: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

        for row_res in rows {
            let (id, url, filename, save_path, total_size, status_str, created_at, error_msg, max_conns, etag, expected_sha256, hash_status) = row_res?;

            // Disambiguate duplicate save_path across distinct downloads in the database
            let (resolved_filename, resolved_save_path, is_collision) = if seen_paths.contains(&save_path) {
                let parent = save_path.parent().unwrap_or_else(|| std::path::Path::new(""));
                let p = std::path::Path::new(&filename);
                let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
                let ext_opt = p.extension().and_then(|s| s.to_str());
                let mut counter = 1;
                loop {
                    let cand_name = match ext_opt {
                        Some(e) => format!("{}_{}.{}", stem, counter, e),
                        None => format!("{}_{}", stem, counter),
                    };
                    let cand_path = parent.join(&cand_name);
                    if !seen_paths.contains(&cand_path) {
                        break (cand_name, cand_path, true);
                    }
                    counter += 1;
                }
            } else {
                (filename.clone(), save_path.clone(), false)
            };
            seen_paths.insert(resolved_save_path.clone());

            let mut partinfo_path = resolved_save_path.clone();
            partinfo_path.set_file_name(format!("{}.partinfo", resolved_save_path.file_name().unwrap_or_default().to_string_lossy()));

            let file_exists = resolved_save_path.exists();
            let partinfo_exists = partinfo_path.exists();
            let file_len = if file_exists { std::fs::metadata(&resolved_save_path).map(|m| m.len()).unwrap_or(0) } else { 0 };

            let path_update = if is_collision {
                Some((resolved_filename.clone(), resolved_save_path.to_string_lossy().to_string()))
            } else {
                None
            };

            // Determine reconciled status and committed downloaded bytes
            let (reconciled_status, downloaded_bytes, segments) = match status_str.as_str() {
                "Completed" => {
                    if file_exists {
                        let final_bytes = total_size.unwrap_or(file_len);
                        (DownloadStatus::Completed, final_bytes, Vec::new())
                    } else {
                        // File was moved or deleted
                        (DownloadStatus::Error("Destination file missing".into()), 0, Vec::new())
                    }
                }
                "Downloading" => {
                    // Application shutdown interrupted active download -> reconcile
                    if file_exists && partinfo_exists {
                        let (committed, segs) = read_partinfo_committed_bytes(&partinfo_path, &resolved_save_path, total_size, etag.as_deref());
                        // Only mark as Completed if all chunks are committed and file_len matches expected total_size
                        if Some(committed) == total_size && total_size.is_some() && file_len == committed && committed > 0 {
                            updates_to_apply.push((id, "Completed".into(), None, path_update));
                            let _ = std::fs::remove_file(&partinfo_path);
                            (DownloadStatus::Completed, committed, Vec::new())
                        } else {
                            updates_to_apply.push((id, "Paused".into(), None, path_update));
                            (DownloadStatus::Paused, committed, segs)
                        }
                    } else if !file_exists && partinfo_exists {
                        let _ = std::fs::remove_file(&partinfo_path);
                        updates_to_apply.push((id, "Paused".into(), Some("Partial file missing on disk; progress reset".into()), path_update));
                        (DownloadStatus::Paused, 0, Vec::new())
                    } else if file_exists && !partinfo_exists {
                        // Sparse/preallocated file exists without valid checkpoint evidence.
                        // NEVER mark as Completed based purely on file size!
                        updates_to_apply.push((id, "Paused".into(), Some("Checkpoint missing; unverified file data reset".into()), path_update));
                        (DownloadStatus::Paused, 0, Vec::new())
                    } else {
                        updates_to_apply.push((id, "Paused".into(), None, path_update));
                        (DownloadStatus::Paused, 0, Vec::new())
                    }
                }
                "Paused" => {
                    if is_collision && path_update.is_some() {
                        updates_to_apply.push((id, "Paused".into(), None, path_update));
                    }
                    if file_exists && partinfo_exists {
                        let (committed, segs) = read_partinfo_committed_bytes(&partinfo_path, &resolved_save_path, total_size, etag.as_deref());
                        (DownloadStatus::Paused, committed, segs)
                    } else if !file_exists && partinfo_exists {
                        let _ = std::fs::remove_file(&partinfo_path);
                        (DownloadStatus::Paused, 0, Vec::new())
                    } else {
                        (DownloadStatus::Paused, 0, Vec::new())
                    }
                }
                "Error" => {
                    let err = error_msg.unwrap_or_else(|| "Unknown download error".into());
                    (DownloadStatus::Error(err), 0, Vec::new())
                }
                "Canceled" => {
                    if file_exists && partinfo_exists {
                        let (committed, segs) = read_partinfo_committed_bytes(&partinfo_path, &resolved_save_path, total_size, etag.as_deref());
                        (DownloadStatus::Canceled, committed, segs)
                    } else {
                        (DownloadStatus::Canceled, 0, Vec::new())
                    }
                }
                "CleanupPending" => {
                    (DownloadStatus::CleanupPending, 0, Vec::new())
                }
                _ => (DownloadStatus::Queued, 0, Vec::new()),
            };

            let date_added = Local.timestamp_opt(created_at, 0).single().unwrap_or_else(Local::now);
            let ext = resolved_save_path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let category = Category::from_extension(ext);

            items.push(DownloadItem {
                id,
                url,
                filename: resolved_filename,
                category,
                save_path: resolved_save_path,
                total_size,
                downloaded_bytes,
                status: reconciled_status,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added,
                max_connections: max_conns,
                segments,
                connection_segments: Vec::new(),
                description: String::new(),
                queue: None,
                last_try_date: None,
                parent_page: None,
                referer: None,
                expected_sha256,
                hash_status,
            });
        }

        // Apply reconciled updates back to database inside a single atomic transaction
        drop(stmt);
        if !updates_to_apply.is_empty() {
            let tx = conn.transaction()?;
            for (up_id, up_status, up_err, path_update) in updates_to_apply {
                let now = Local::now().timestamp();
                if let Some((new_fn, new_sp)) = path_update {
                    tx.execute(
                        "UPDATE downloads SET status = ?1, error_message = ?2, filename = ?3, save_path = ?4, updated_at = ?5 WHERE id = ?6",
                        params![up_status, up_err, new_fn, new_sp, now, up_id as i64],
                    )?;
                } else {
                    tx.execute(
                        "UPDATE downloads SET status = ?1, error_message = ?2, updated_at = ?3 WHERE id = ?4",
                        params![up_status, up_err, now, up_id as i64],
                    )?;
                }
            }
            tx.commit()?;
        }

        Ok(items)
    }

    /// Inserts a new download and its settings in an atomic transaction.
    pub fn insert_download(&self, item: &DownloadItem, settings: &DownloadSettings) -> Result<u64, DbError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let now = Local::now().timestamp();
        let status_str = match &item.status {
            DownloadStatus::Queued => "Queued",
            DownloadStatus::Connecting | DownloadStatus::Downloading => "Downloading",
            DownloadStatus::Paused => "Paused",
            DownloadStatus::Completed => "Completed",
            DownloadStatus::Error(_) => "Error",
            DownloadStatus::Canceled => "Canceled",
            DownloadStatus::CleanupPending => "CleanupPending",
        };
        let error_msg = match &item.status {
            DownloadStatus::Error(e) => Some(e.as_str()),
            _ => None,
        };
        let hash_status_str = hash_status_to_str(item.hash_status);

        let id = if item.id > 0 {
            tx.execute(
                "INSERT INTO downloads (
                    id, url, filename, save_path, total_size, status,
                    created_at, updated_at, error_message, expected_sha256, hash_status
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    item.id as i64,
                    item.url,
                    item.filename,
                    item.save_path.to_string_lossy().to_string(),
                    item.total_size.map(|s| s as i64),
                    status_str,
                    now,
                    now,
                    error_msg,
                    item.expected_sha256.as_deref(),
                    hash_status_str,
                ],
            )?;
            item.id
        } else {
            tx.execute(
                "INSERT INTO downloads (
                    url, filename, save_path, total_size, status,
                    created_at, updated_at, error_message, expected_sha256, hash_status
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    item.url,
                    item.filename,
                    item.save_path.to_string_lossy().to_string(),
                    item.total_size.map(|s| s as i64),
                    status_str,
                    now,
                    now,
                    error_msg,
                    item.expected_sha256.as_deref(),
                    hash_status_str,
                ],
            )?;
            tx.last_insert_rowid() as u64
        };

        tx.execute(
            "INSERT INTO download_settings (
                download_id, max_connections, chunk_size, speed_limit, retry_limit
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(download_id) DO UPDATE SET
                max_connections = excluded.max_connections,
                chunk_size = excluded.chunk_size,
                speed_limit = excluded.speed_limit,
                retry_limit = excluded.retry_limit",
            params![
                id as i64,
                settings.max_connections as i64,
                settings.chunk_size as i64,
                settings.speed_limit.map(|s| s as i64),
                settings.retry_limit as i64,
            ],
        )?;

        tx.commit()?;
        Ok(id)
    }

    /// Scans a directory for orphaned .partinfo files without DB records.
    pub fn scan_orphaned_partinfo(&self, dir: &Path) -> Result<Vec<PathBuf>, DbError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT save_path FROM downloads")?;
        let known_paths: std::collections::HashSet<PathBuf> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .filter_map(|r| r.ok().map(PathBuf::from))
            .collect();

        let mut orphans = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_file() {
                    let fname = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
                    if fname.ends_with(".partinfo") {
                        let base_name = &fname[..fname.len() - 9]; // strip .partinfo
                        let base_path = p.with_file_name(base_name);
                        if !known_paths.contains(&base_path) {
                            orphans.push(p);
                        }
                    }
                }
            }
        }
        Ok(orphans)
    }

    pub fn update_metadata(
        &self,
        id: u64,
        filename: &str,
        total_size: Option<u64>,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let rows = conn.execute(
            "UPDATE downloads 
             SET filename = ?1, total_size = ?2, etag = ?3, last_modified = ?4, updated_at = ?5 
             WHERE id = ?6",
            params![
                filename,
                total_size.map(|s| s as i64),
                etag,
                last_modified,
                now,
                id as i64,
            ],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn update_status(&self, id: u64, status: &DownloadStatus) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let (status_str, err_msg) = match status {
            DownloadStatus::Queued => ("Queued", None),
            DownloadStatus::Connecting | DownloadStatus::Downloading => ("Downloading", None),
            DownloadStatus::Paused => ("Paused", None),
            DownloadStatus::Completed => ("Completed", None),
            DownloadStatus::Error(e) => ("Error", Some(e.as_str())),
            DownloadStatus::Canceled => ("Canceled", None),
            DownloadStatus::CleanupPending => ("CleanupPending", None),
        };

        let rows = conn.execute(
            "UPDATE downloads 
             SET status = ?1, error_message = ?2, updated_at = ?3 
             WHERE id = ?4",
            params![status_str, err_msg, now, id as i64],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn update_save_path(&self, id: u64, filename: &str, save_path: &Path) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let rows = conn.execute(
            "UPDATE downloads 
             SET filename = ?1, save_path = ?2, updated_at = ?3 
             WHERE id = ?4",
            params![
                filename,
                save_path.to_string_lossy().to_string(),
                now,
                id as i64,
            ],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn mark_completed(&self, id: u64) -> Result<(), DbError> {
        self.mark_completed_with_hash(id, None)
    }

    pub fn mark_completed_with_hash(
        &self,
        id: u64,
        hash_status: Option<&HashVerificationStatus>,
    ) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let rows = if let Some(hs) = hash_status {
            let hs_str = hash_status_to_str(*hs);
            conn.execute(
                "UPDATE downloads 
                 SET status = 'Completed', completed_at = ?1, updated_at = ?2, error_message = NULL, hash_status = ?3 
                 WHERE id = ?4",
                params![now, now, hs_str, id as i64],
            )?
        } else {
            conn.execute(
                "UPDATE downloads 
                 SET status = 'Completed', completed_at = ?1, updated_at = ?2, error_message = NULL 
                 WHERE id = ?3",
                params![now, now, id as i64],
            )?
        };

        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn update_hash_status(
        &self,
        id: u64,
        status: &HashVerificationStatus,
    ) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let status_str = hash_status_to_str(*status);
        let rows = conn.execute(
            "UPDATE downloads 
             SET hash_status = ?1, updated_at = ?2 
             WHERE id = ?3",
            params![status_str, now, id as i64],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn update_expected_sha256(
        &self,
        id: u64,
        expected_sha256: Option<&str>,
    ) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let rows = conn.execute(
            "UPDATE downloads 
             SET expected_sha256 = ?1, updated_at = ?2 
             WHERE id = ?3",
            params![expected_sha256, now, id as i64],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn get_hash_status(&self, id: u64) -> Result<(Option<String>, HashVerificationStatus), DbError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT expected_sha256, hash_status FROM downloads WHERE id = ?1",
            params![id as i64],
            |row| {
                let exp: Option<String> = row.get(0)?;
                let status_str: Option<String> = row.get(1)?;
                let hs = status_str
                    .as_deref()
                    .map(hash_status_from_str)
                    .unwrap_or(HashVerificationStatus::NotConfigured);
                Ok((exp, hs))
            },
        )
        .optional()?
        .ok_or(DbError::NotFound(id))
    }

    pub fn get_save_path(&self, id: u64) -> Result<PathBuf, DbError> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        conn.query_row(
            "SELECT save_path FROM downloads WHERE id = ?1",
            rusqlite::params![id as i64],
            |row| {
                let s: String = row.get(0)?;
                Ok(PathBuf::from(s))
            },
        )
        .optional()?
        .ok_or(DbError::NotFound(id))
    }

    pub fn mark_failed(&self, id: u64, error_msg: &str) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let rows = conn.execute(
            "UPDATE downloads 
             SET status = 'Error', error_message = ?1, updated_at = ?2 
             WHERE id = ?3",
            params![error_msg, now, id as i64],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn delete_download(&self, id: u64) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM downloads WHERE id = ?1",
            params![id as i64],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
    }

    pub fn delete_completed_downloads(&self) -> Result<usize, DbError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute("DELETE FROM downloads WHERE status = 'Completed'", [])?;
        Ok(rows)
    }

    /// Retries a database operation on SQLITE_BUSY or SQLITE_LOCKED with exponential backoff.
    pub fn execute_with_retry<F, T>(&self, mut op: F) -> Result<T, DbError>
    where
        F: FnMut() -> Result<T, DbError>,
    {
        let mut retries = 0;
        loop {
            match op() {
                Ok(val) => return Ok(val),
                Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(err, _)))
                    if err.code == rusqlite::ffi::ErrorCode::DatabaseBusy
                        || err.code == rusqlite::ffi::ErrorCode::DatabaseLocked =>
                {
                    if retries >= 5 {
                        return Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(
                            err,
                            Some("Database busy retry limit exceeded".into()),
                        )));
                    }
                    retries += 1;
                    std::thread::sleep(std::time::Duration::from_millis(10 * (1 << retries)));
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn get_download_count(&self) -> Result<usize, DbError> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM downloads", [], |r| r.get(0))?;
        Ok(count as usize)
    }

    /// Returns the maximum download ID currently in the database, or 0 if empty.
    /// Fast O(1) B-tree lookup without disk metadata or checkpoint validation.
    pub fn get_max_download_id(&self) -> Result<u64, DbError> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let max_id: Option<i64> = conn.query_row(
            "SELECT COALESCE(MAX(id), 0) FROM downloads",
            [],
            |row| row.get(0),
        )?;
        Ok(max_id.unwrap_or(0) as u64)
    }

    /// Checks if a download record exists in the database.
    pub fn download_exists(&self, id: u64) -> Result<bool, DbError> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM downloads WHERE id = ?1)",
            params![id as i64],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    pub fn get_settings(&self, download_id: u64) -> Result<DownloadSettings, DbError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT download_id, max_connections, chunk_size, speed_limit, retry_limit 
             FROM download_settings WHERE download_id = ?1",
            params![download_id as i64],
            |row| {
                let did: i64 = row.get(0)?;
                let max_conns: i64 = row.get(1)?;
                let chunk_size: i64 = row.get(2)?;
                let speed_limit: Option<i64> = row.get(3)?;
                let retry_limit: i64 = row.get(4)?;
                Ok(DownloadSettings {
                    download_id: did as u64,
                    max_connections: max_conns as usize,
                    chunk_size: chunk_size as u64,
                    speed_limit: speed_limit.map(|s| s as u64),
                    retry_limit: retry_limit as u32,
                })
            },
        ).optional()?
        .ok_or(DbError::NotFound(download_id))
    }
}

/// Helper to read committed byte count from a `.partinfo` file safely.
fn read_partinfo_committed_bytes(
    partinfo_path: &Path,
    save_path: &Path,
    total_size: Option<u64>,
    db_etag: Option<&str>,
) -> (u64, Vec<SegmentProgress>) {
    // If the partial destination file is missing, we cannot resume partial data safely.
    if !save_path.exists() {
        return (0, Vec::new());
    }

    if let Ok(data) = std::fs::read_to_string(partinfo_path) {
        if let Ok(cp) = serde_json::from_str::<serde_json::Value>(&data) {
            // ETag verification: If checkpoint ETag mismatches DB/server ETag, invalidate checkpoint
            let cp_etag = cp.get("etag").and_then(|v| v.as_str());
            if let (Some(d_tag), Some(c_tag)) = (db_etag, cp_etag) {
                if !d_tag.is_empty() && !c_tag.is_empty() && d_tag != c_tag {
                    let _ = std::fs::remove_file(partinfo_path);
                    return (0, Vec::new());
                }
            }

            // Total size consistency
            let cp_tot = cp.get("total_size").and_then(|v| v.as_u64());
            if let (Some(d_tot), Some(c_tot)) = (total_size, cp_tot) {
                if d_tot > 0 && c_tot > 0 && d_tot != c_tot {
                    let _ = std::fs::remove_file(partinfo_path);
                    return (0, Vec::new());
                }
            }

            let chunk_size = cp.get("chunk_size").and_then(|v| v.as_u64()).unwrap_or(2 * 1024 * 1024);
            let tot = total_size.or(cp_tot).unwrap_or(0);

            if let Some(committed_ids) = cp.get("committed_chunk_ids").and_then(|v| v.as_array()) {
                let committed_set: std::collections::HashSet<usize> = committed_ids
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as usize))
                    .collect();

                let mut total_committed = 0u64;
                for &id in &committed_set {
                    let start = id as u64 * chunk_size;
                    let end = ((id as u64 + 1) * chunk_size).min(tot);
                    total_committed += end.saturating_sub(start);
                }

                // Generate UI segment representation (up to 8 segments)
                let num_display = 8usize;
                let slice_size = if tot > 0 { (tot + num_display as u64 - 1) / num_display as u64 } else { 1 };
                let mut segs = Vec::with_capacity(num_display);

                for i in 0..num_display {
                    let start = i as u64 * slice_size;
                    let end = ((i as u64 + 1) * slice_size).min(tot);
                    if start >= tot { break; }

                    // Count how many bytes in this slice are covered by committed chunks
                    let mut slice_done = 0u64;
                    for &id in &committed_set {
                        let c_start = id as u64 * chunk_size;
                        let c_end = ((id as u64 + 1) * chunk_size).min(tot);
                        let overlap_start = start.max(c_start);
                        let overlap_end = end.min(c_end);
                        if overlap_end > overlap_start {
                            slice_done += overlap_end - overlap_start;
                        }
                    }

                    segs.push(SegmentProgress {
                        segment_index: i,
                        start_byte: start,
                        end_byte: end,
                        current_byte: (start + slice_done).min(end),
                        speed: 0.0,
                    });
                }

                return (total_committed, segs);
            }
        }
    }
    (0, Vec::new())
}

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const SHA256_INITIAL_H: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

fn transform_sha256_block(h: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for (i, w_val) in w.iter_mut().take(16).enumerate() {
        *w_val = u32::from_be_bytes([
            block[i * 4],
            block[i * 4 + 1],
            block[i * 4 + 2],
            block[i * 4 + 3],
        ]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
    }

    let mut a = h[0];
    let mut b = h[1];
    let mut c = h[2];
    let mut d = h[3];
    let mut e = h[4];
    let mut f = h[5];
    let mut g = h[6];
    let mut h_val = h[7];

    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let temp1 = h_val
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(SHA256_K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let temp2 = s0.wrapping_add(maj);

        h_val = g;
        g = f;
        f = e;
        e = d.wrapping_add(temp1);
        d = c;
        c = b;
        b = a;
        a = temp1.wrapping_add(temp2);
    }

    h[0] = h[0].wrapping_add(a);
    h[1] = h[1].wrapping_add(b);
    h[2] = h[2].wrapping_add(c);
    h[3] = h[3].wrapping_add(d);
    h[4] = h[4].wrapping_add(e);
    h[5] = h[5].wrapping_add(f);
    h[6] = h[6].wrapping_add(g);
    h[7] = h[7].wrapping_add(h_val);
}

/// Pure-Rust streaming SHA-256 Hasher complying with FIPS 180-4.
///
/// Maintains fixed 64-byte internal buffering and consumes streaming chunks with
/// bounded stack memory (< 5 MB peak incremental memory on any file size).
#[derive(Clone, Debug)]
pub struct Sha256Hasher {
    h: [u32; 8],
    buf: [u8; 64],
    buf_len: usize,
    total_len: u64,
}

impl Default for Sha256Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256Hasher {
    pub fn new() -> Self {
        Self {
            h: SHA256_INITIAL_H,
            buf: [0u8; 64],
            buf_len: 0,
            total_len: 0,
        }
    }

    /// Appends incoming byte slice to the streaming hasher.
    pub fn update(&mut self, mut data: &[u8]) {
        self.total_len = self.total_len.wrapping_add(data.len() as u64);

        if self.buf_len > 0 {
            let to_fill = 64 - self.buf_len;
            if data.len() < to_fill {
                self.buf[self.buf_len..self.buf_len + data.len()].copy_from_slice(data);
                self.buf_len += data.len();
                return;
            } else {
                self.buf[self.buf_len..64].copy_from_slice(&data[..to_fill]);
                transform_sha256_block(&mut self.h, &self.buf);
                self.buf_len = 0;
                data = &data[to_fill..];
            }
        }

        let chunks = data.chunks_exact(64);
        let remainder = chunks.remainder();
        for chunk in chunks {
            let mut block = [0u8; 64];
            block.copy_from_slice(chunk);
            transform_sha256_block(&mut self.h, &block);
        }

        if !remainder.is_empty() {
            self.buf[..remainder.len()].copy_from_slice(remainder);
            self.buf_len = remainder.len();
        }
    }

    /// Finalizes the hash calculation and returns the 32-byte digest according to FIPS 180-4 padding.
    pub fn finalize(mut self) -> [u8; 32] {
        let bit_len = self.total_len.wrapping_mul(8);

        self.buf[self.buf_len] = 0x80;
        self.buf_len += 1;

        if self.buf_len > 56 {
            self.buf[self.buf_len..64].fill(0);
            transform_sha256_block(&mut self.h, &self.buf);
            self.buf[..56].fill(0);
        } else {
            self.buf[self.buf_len..56].fill(0);
        }

        self.buf[56..64].copy_from_slice(&bit_len.to_be_bytes());
        transform_sha256_block(&mut self.h, &self.buf);

        let mut digest = [0u8; 32];
        for (i, &val) in self.h.iter().enumerate() {
            digest[i * 4..(i + 1) * 4].copy_from_slice(&val.to_be_bytes());
        }
        digest
    }

    /// Finalizes the hash calculation and formats the digest as a 64-character lowercase hexadecimal string.
    pub fn finalize_hex(self) -> String {
        let digest = self.finalize();
        let mut hex = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write;
            let _ = write!(&mut hex, "{:02x}", byte);
        }
        hex
    }
}

/// Convenience function to compute SHA-256 for a byte slice in memory.
pub fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256Hasher::new();
    hasher.update(data);
    hasher.finalize_hex()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashVerifyOutcome {
    Verified,
    VerificationFailed,
    Cancelled,
}

pub static HASH_CHUNK_NOTIFY: std::sync::LazyLock<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Notify>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(None));
pub static HASH_READ_ITERATIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Verifies whether the file at `path` matches the `expected_hex` SHA-256 digest
/// using a streaming 64 KiB reusable buffer with optional cancellation support.
pub fn verify_file_sha256_cancellable(
    path: &Path,
    expected_hex: &str,
    cancel_flag: Option<&std::sync::atomic::AtomicBool>,
) -> Result<HashVerifyOutcome, std::io::Error> {
    use std::fs::File;
    use std::io::Read;
    use std::sync::atomic::Ordering;

    let mut file = File::open(path)?;
    let mut hasher = Sha256Hasher::new();
    let mut buffer = [0u8; 65536];

    loop {
        if let Some(flag) = cancel_flag {
            if flag.load(Ordering::SeqCst) {
                return Ok(HashVerifyOutcome::Cancelled);
            }
        }
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        HASH_READ_ITERATIONS.fetch_add(1, Ordering::SeqCst);
        if let Ok(guard) = HASH_CHUNK_NOTIFY.lock() {
            if let Some(notify) = guard.as_ref() {
                notify.notify_waiters();
            }
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let computed = hasher.finalize_hex();
    if computed.eq_ignore_ascii_case(expected_hex.trim()) {
        Ok(HashVerifyOutcome::Verified)
    } else {
        Ok(HashVerifyOutcome::VerificationFailed)
    }
}

/// Verifies whether the file at `path` matches the `expected_hex` SHA-256 digest
/// using a streaming 64 KiB reusable buffer without whole-file heap allocation (< 5 MB peak RAM).
pub fn verify_file_sha256(path: &Path, expected_hex: &str) -> Result<bool, std::io::Error> {
    match verify_file_sha256_cancellable(path, expected_hex, None)? {
        HashVerifyOutcome::Verified => Ok(true),
        HashVerifyOutcome::VerificationFailed | HashVerifyOutcome::Cancelled => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(name: &str) -> Self {
            let p = std::env::temp_dir().join(format!("idm_test_{}_{}_{}", name, std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)));
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

    #[test]
    fn test_db_schema_version_and_migration() {
        let db = Database::open_in_memory().expect("Should open in memory");
        let conn = db.conn.lock().unwrap();
        let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(version, 1, "Schema version must be 1");
    }

    #[test]
    fn test_db_insert_and_reconcile() {
        let db = Database::open_in_memory().expect("Should open in memory");
        let item = DownloadItem {
            id: 0,
            url: "https://example.com/file.zip".into(),
            filename: "file.zip".into(),
            save_path: PathBuf::from("downloads/file.zip"),
            total_size: Some(10_000_000),
            status: DownloadStatus::Downloading,
            ..Default::default()
        };
        let settings = DownloadSettings::default();

        let id = db.insert_download(&item, &settings).expect("Insert must succeed");
        assert_eq!(id, 1);

        // Reconcile: since file and .partinfo don't exist on disk, Downloading must reconcile to Paused
        let reconciled = db.reconcile_and_load_all().expect("Reconcile must succeed");
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].status, DownloadStatus::Paused);
        assert_eq!(reconciled[0].id, 1);
    }

    #[test]
    fn test_db_mark_completed_and_delete() {
        let db = Database::open_in_memory().expect("Should open in memory");
        let item = DownloadItem {
            id: 0,
            url: "https://example.com/test.bin".into(),
            filename: "test.bin".into(),
            save_path: PathBuf::from("downloads/test.bin"),
            total_size: Some(5_000_000),
            status: DownloadStatus::Queued,
            ..Default::default()
        };
        let settings = DownloadSettings::default();

        let id = db.insert_download(&item, &settings).unwrap();
        db.mark_completed(id).unwrap();

        let loaded = db.reconcile_and_load_all().unwrap();
        // File missing on disk, so completed without file transitions to Error("Destination file missing")
        assert_eq!(loaded[0].status, DownloadStatus::Error("Destination file missing".into()));

        // Delete download
        db.delete_download(id).unwrap();
        assert_eq!(db.get_download_count().unwrap(), 0);
    }

    #[test]
    fn test_db_etag_mismatch_invalidation() {
        let temp_dir = TempDir::new("etag");
        let target_file = temp_dir.path().join("etag_test.dat");
        let partinfo_file = temp_dir.path().join("etag_test.dat.partinfo");

        // Write partial dummy data
        std::fs::write(&target_file, vec![0u8; 1000]).unwrap();

        // Write checkpoint with old etag
        let checkpoint = serde_json::json!({
            "total_size": 10000,
            "chunk_size": 1000,
            "etag": "old_etag_123",
            "committed_chunk_ids": [0]
        });
        std::fs::write(&partinfo_file, checkpoint.to_string()).unwrap();

        // Database has new etag
        let db = Database::open_in_memory().unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/etag.dat".into(),
            filename: "etag_test.dat".into(),
            save_path: target_file.clone(),
            total_size: Some(10000),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();
        db.update_metadata(1, "etag_test.dat", Some(10000), Some("new_etag_456"), None).unwrap();

        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        // ETag mismatch invalidates checkpoint: committed bytes reset to 0
        assert_eq!(loaded[0].downloaded_bytes, 0);
    }

    #[test]
    fn test_db_missing_partial_file_recovery() {
        let temp_dir = TempDir::new("missing");
        let target_file = temp_dir.path().join("missing.dat");
        let partinfo_file = temp_dir.path().join("missing.dat.partinfo");

        // partinfo exists, but target_file is missing
        let checkpoint = serde_json::json!({
            "total_size": 10000,
            "chunk_size": 1000,
            "committed_chunk_ids": [0, 1, 2]
        });
        std::fs::write(&partinfo_file, checkpoint.to_string()).unwrap();

        let db = Database::open_in_memory().unwrap();
        let item = DownloadItem {
            id: 1,
            url: "https://example.com/missing.dat".into(),
            filename: "missing.dat".into(),
            save_path: target_file,
            total_size: Some(10000),
            status: DownloadStatus::Downloading,
            ..Default::default()
        };
        db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded[0].status, DownloadStatus::Paused);
        assert_eq!(loaded[0].downloaded_bytes, 0);
    }

    #[test]
    fn test_db_corruption_recovery() {
        let temp_dir = TempDir::new("corrupt");
        let db_path = temp_dir.path().join("corrupt_test.db");

        // Write corrupt garbage into db file
        std::fs::write(&db_path, b"NOT A SQLITE FILE GARBAGE TRASH JUNK DATA!").unwrap();

        // Database::open should backup corrupt file to .corrupt.bak and create fresh DB
        let db = Database::open(&db_path).expect("Should recover from corruption");
        assert_eq!(db.get_download_count().unwrap(), 0);

        let bak_path = db_path.with_extension("db.corrupt.bak");
        assert!(bak_path.exists(), "Corrupt backup file must exist");
    }

    #[test]
    fn test_sha256_standard_vectors() {
        assert_eq!(
            compute_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            compute_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            compute_sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn test_delete_completed_downloads() {
        let db = Database::open_in_memory().unwrap();
        let item1 = DownloadItem {
            id: 1,
            url: "https://example.com/file1.zip".into(),
            filename: "file1.zip".into(),
            status: DownloadStatus::Completed,
            ..Default::default()
        };
        let item2 = DownloadItem {
            id: 2,
            url: "https://example.com/file2.zip".into(),
            filename: "file2.zip".into(),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        db.insert_download(&item1, &DownloadSettings::default()).unwrap();
        db.insert_download(&item2, &DownloadSettings::default()).unwrap();
        assert_eq!(db.get_download_count().unwrap(), 2);

        let deleted = db.delete_completed_downloads().unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(db.get_download_count().unwrap(), 1);
    }

    #[test]
    fn test_sha256_one_million_a() {
        let mut hasher = Sha256Hasher::new();
        let chunk = [b'a'; 1000];
        for _ in 0..1000 {
            hasher.update(&chunk);
        }
        assert_eq!(
            hasher.finalize_hex(),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn test_sha256_multi_buffer_chunking_continuity() {
        let message = b"The quick brown fox jumps over the lazy dog";
        let expected = compute_sha256(message);

        // Feed data byte by byte
        let mut hasher1 = Sha256Hasher::new();
        for &b in message {
            hasher1.update(&[b]);
        }
        assert_eq!(hasher1.finalize_hex(), expected);

        // Feed data in 5-byte chunks
        let mut hasher5 = Sha256Hasher::new();
        for chunk in message.chunks(5) {
            hasher5.update(chunk);
        }
        assert_eq!(hasher5.finalize_hex(), expected);
    }

    #[test]
    fn test_verify_file_sha256_streaming_and_case_insensitive() {
        let temp_dir = TempDir::new("verify_sha256");
        let file_path = temp_dir.path().join("test_verify.bin");

        // Write 130 KiB file (spans multiple 64 KiB buffers)
        let data: Vec<u8> = (0..133120).map(|i| (i % 256) as u8).collect();
        std::fs::write(&file_path, &data).unwrap();

        let expected_lower = compute_sha256(&data);
        let expected_upper = expected_lower.to_uppercase();
        let expected_padded = format!("  {} \n", expected_lower);

        assert!(verify_file_sha256(&file_path, &expected_lower).unwrap());
        assert!(verify_file_sha256(&file_path, &expected_upper).unwrap());
        assert!(verify_file_sha256(&file_path, &expected_padded).unwrap());

        let wrong_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(!verify_file_sha256(&file_path, wrong_hash).unwrap());

        let nonexistent = temp_dir.path().join("does_not_exist.bin");
        assert!(verify_file_sha256(&nonexistent, &expected_lower).is_err());
    }

    #[test]
    fn test_db_hash_status_persistence_and_update() {
        let db = Database::open_in_memory().unwrap();
        let expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let item = DownloadItem {
            id: 0,
            url: "https://example.com/file_with_hash.zip".into(),
            filename: "file_with_hash.zip".into(),
            save_path: PathBuf::from("downloads/file_with_hash.zip"),
            total_size: Some(1024),
            status: DownloadStatus::Queued,
            expected_sha256: Some(expected.to_string()),
            hash_status: HashVerificationStatus::NotConfigured,
            ..Default::default()
        };
        let settings = DownloadSettings::default();
        let id = db.insert_download(&item, &settings).unwrap();

        let (exp, status) = db.get_hash_status(id).unwrap();
        assert_eq!(exp.as_deref(), Some(expected));
        assert_eq!(status, HashVerificationStatus::NotConfigured);

        // Update to Verified
        db.update_hash_status(id, &HashVerificationStatus::Verified).unwrap();
        let (_, status_v) = db.get_hash_status(id).unwrap();
        assert_eq!(status_v, HashVerificationStatus::Verified);

        // Mark completed with hash status VerificationFailed
        db.mark_completed_with_hash(id, Some(&HashVerificationStatus::VerificationFailed)).unwrap();
        let (_, status_f) = db.get_hash_status(id).unwrap();
        assert_eq!(status_f, HashVerificationStatus::VerificationFailed);
    }

    #[test]
    fn test_db_migration_from_v1_without_hash_status() {
        let temp_dir = TempDir::new("v1_migrate");
        let db_path = temp_dir.path().join("legacy_v1.db");

        // Create legacy DB lacking hash_status column
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    save_path TEXT NOT NULL,
                    total_size INTEGER,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    error_message TEXT,
                    etag TEXT,
                    last_modified TEXT,
                    expected_sha256 TEXT
                );
                CREATE TABLE download_settings (
                    download_id INTEGER PRIMARY KEY,
                    max_connections INTEGER NOT NULL DEFAULT 8,
                    chunk_size INTEGER NOT NULL DEFAULT 2097152,
                    speed_limit INTEGER,
                    retry_limit INTEGER NOT NULL DEFAULT 5,
                    FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE CASCADE
                );
                PRAGMA user_version = 1;",
            ).unwrap();

            conn.execute(
                "INSERT INTO downloads (id, url, filename, save_path, total_size, status, created_at, updated_at)
                 VALUES (1, 'https://example.com/legacy.bin', 'legacy.bin', 'legacy.bin', 100, 'Paused', 1000, 1000)",
                [],
            ).unwrap();
        }

        // Open with our Database: should detect missing hash_status, alter table, keep user_version = 1
        let db = Database::open(&db_path).unwrap();
        {
            let conn = db.conn.lock().unwrap();
            let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
            assert_eq!(version, 1, "Schema version must remain 1");
        }

        let items = db.reconcile_and_load_all().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].hash_status, HashVerificationStatus::NotConfigured);
    }

    #[test]
    fn test_db_cleanup_pending_persistence() {
        let db = Database::open_in_memory().unwrap();
        let item = DownloadItem {
            id: 0,
            url: "https://example.com/cleanup.bin".into(),
            filename: "cleanup.bin".into(),
            save_path: PathBuf::from("downloads/cleanup.bin"),
            status: DownloadStatus::CleanupPending,
            ..Default::default()
        };
        let id = db.insert_download(&item, &DownloadSettings::default()).unwrap();

        let loaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded[0].id, id);
        assert_eq!(loaded[0].status, DownloadStatus::CleanupPending);

        // Update status to Paused then back to CleanupPending
        db.update_status(id, &DownloadStatus::Paused).unwrap();
        let loaded2 = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded2[0].status, DownloadStatus::Paused);

        db.update_status(id, &DownloadStatus::CleanupPending).unwrap();
        let loaded3 = db.reconcile_and_load_all().unwrap();
        assert_eq!(loaded3[0].status, DownloadStatus::CleanupPending);
    }

    #[test]
    fn test_db_get_max_download_id_and_exists() {
        let db = Database::open_in_memory().unwrap();
        assert_eq!(db.get_max_download_id().unwrap(), 0);
        assert!(!db.download_exists(1).unwrap());

        let item1 = DownloadItem {
            id: 42,
            url: "https://example.com/item42.bin".into(),
            filename: "item42.bin".into(),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        db.insert_download(&item1, &DownloadSettings::default()).unwrap();

        assert_eq!(db.get_max_download_id().unwrap(), 42);
        assert!(db.download_exists(42).unwrap());
        assert!(!db.download_exists(43).unwrap());

        let item2 = DownloadItem {
            id: 100,
            url: "https://example.com/item100.bin".into(),
            filename: "item100.bin".into(),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        db.insert_download(&item2, &DownloadSettings::default()).unwrap();

        assert_eq!(db.get_max_download_id().unwrap(), 100);
        assert!(db.download_exists(100).unwrap());

        db.delete_download(100).unwrap();
        assert!(!db.download_exists(100).unwrap());
        assert_eq!(db.get_max_download_id().unwrap(), 42);
    }

    #[test]
    fn test_db_reconcile_duplicate_save_path_disambiguation() {
        let db = Database::open_in_memory().unwrap();
        let target_path = PathBuf::from("D:/Downloads/test_iso.iso");

        let item1 = DownloadItem {
            id: 1,
            url: "https://example.com/test_iso.iso".into(),
            filename: "test_iso.iso".into(),
            save_path: target_path.clone(),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        let item2 = DownloadItem {
            id: 2,
            url: "https://example.com/test_iso.iso".into(),
            filename: "test_iso.iso".into(),
            save_path: target_path.clone(),
            status: DownloadStatus::Paused,
            ..Default::default()
        };
        let item3 = DownloadItem {
            id: 3,
            url: "https://example.com/test_iso.iso".into(),
            filename: "test_iso.iso".into(),
            save_path: target_path.clone(),
            status: DownloadStatus::Paused,
            ..Default::default()
        };

        db.insert_download(&item1, &DownloadSettings::default()).unwrap();
        db.insert_download(&item2, &DownloadSettings::default()).unwrap();
        db.insert_download(&item3, &DownloadSettings::default()).unwrap();

        let reconciled = db.reconcile_and_load_all().unwrap();
        assert_eq!(reconciled.len(), 3);

        assert_eq!(reconciled[0].id, 1);
        assert_eq!(reconciled[0].filename, "test_iso.iso");
        assert_eq!(reconciled[0].save_path, target_path);

        assert_eq!(reconciled[1].id, 2);
        assert_eq!(reconciled[1].filename, "test_iso_1.iso");
        assert_eq!(reconciled[1].save_path, PathBuf::from("D:/Downloads/test_iso_1.iso"));

        assert_eq!(reconciled[2].id, 3);
        assert_eq!(reconciled[2].filename, "test_iso_2.iso");
        assert_eq!(reconciled[2].save_path, PathBuf::from("D:/Downloads/test_iso_2.iso"));

        // Verify that the changes were persisted in the database table
        let reloaded = db.reconcile_and_load_all().unwrap();
        assert_eq!(reloaded[1].filename, "test_iso_1.iso");
        assert_eq!(reloaded[2].filename, "test_iso_2.iso");
    }
}
