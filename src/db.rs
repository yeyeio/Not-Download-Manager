use std::path::{Path, PathBuf};
use std::sync::Mutex;
use chrono::{Local, TimeZone};
use rusqlite::{params, Connection, OptionalExtension};

use crate::types::{Category, DownloadItem, DownloadSettings, DownloadStatus, SegmentProgress};

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
                    expected_sha256 TEXT
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
                d.etag, d.last_modified, d.expected_sha256,
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
            let _expected_sha256: Option<String> = row.get(12)?;
            let max_connections: Option<i64> = row.get(13)?;
            let _chunk_size: Option<i64> = row.get(14)?;
            let _speed_limit: Option<i64> = row.get(15)?;
            let _retry_limit: Option<i64> = row.get(16)?;

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
            ))
        })?;

        let mut items = Vec::new();
        let mut updates_to_apply: Vec<(u64, String, Option<String>)> = Vec::new();

        for row_res in rows {
            let (id, url, filename, save_path, total_size, status_str, created_at, error_msg, max_conns, etag) = row_res?;

            let mut partinfo_path = save_path.clone();
            partinfo_path.set_file_name(format!("{}.partinfo", save_path.file_name().unwrap_or_default().to_string_lossy()));

            let file_exists = save_path.exists();
            let partinfo_exists = partinfo_path.exists();
            let file_len = if file_exists { std::fs::metadata(&save_path).map(|m| m.len()).unwrap_or(0) } else { 0 };

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
                        let (committed, segs) = read_partinfo_committed_bytes(&partinfo_path, &save_path, total_size, etag.as_deref());
                        // Only mark as Completed if all chunks are committed and file_len matches expected total_size
                        if Some(committed) == total_size && total_size.is_some() && file_len == committed && committed > 0 {
                            updates_to_apply.push((id, "Completed".into(), None));
                            let _ = std::fs::remove_file(&partinfo_path);
                            (DownloadStatus::Completed, committed, Vec::new())
                        } else {
                            updates_to_apply.push((id, "Paused".into(), None));
                            (DownloadStatus::Paused, committed, segs)
                        }
                    } else if !file_exists && partinfo_exists {
                        let _ = std::fs::remove_file(&partinfo_path);
                        updates_to_apply.push((id, "Paused".into(), Some("Partial file missing on disk; progress reset".into())));
                        (DownloadStatus::Paused, 0, Vec::new())
                    } else if file_exists && !partinfo_exists {
                        // Sparse/preallocated file exists without valid checkpoint evidence.
                        // NEVER mark as Completed based purely on file size!
                        updates_to_apply.push((id, "Paused".into(), Some("Checkpoint missing; unverified file data reset".into())));
                        (DownloadStatus::Paused, 0, Vec::new())
                    } else {
                        updates_to_apply.push((id, "Paused".into(), None));
                        (DownloadStatus::Paused, 0, Vec::new())
                    }
                }
                "Paused" => {
                    if file_exists && partinfo_exists {
                        let (committed, segs) = read_partinfo_committed_bytes(&partinfo_path, &save_path, total_size, etag.as_deref());
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
                        let (committed, segs) = read_partinfo_committed_bytes(&partinfo_path, &save_path, total_size, etag.as_deref());
                        (DownloadStatus::Canceled, committed, segs)
                    } else {
                        (DownloadStatus::Canceled, 0, Vec::new())
                    }
                }
                _ => (DownloadStatus::Queued, 0, Vec::new()),
            };

            let date_added = Local.timestamp_opt(created_at, 0).single().unwrap_or_else(Local::now);
            let ext = save_path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let category = Category::from_extension(ext);

            items.push(DownloadItem {
                id,
                url,
                filename,
                category,
                save_path,
                total_size,
                downloaded_bytes,
                status: reconciled_status,
                transfer_rate: 0.0,
                time_left_secs: None,
                resume_capable: true,
                date_added,
                max_connections: max_conns,
                segments,
                description: String::new(),
                queue: None,
                last_try_date: None,
                parent_page: None,
                referer: None,
            });
        }

        // Apply reconciled updates back to database inside a single atomic transaction
        drop(stmt);
        if !updates_to_apply.is_empty() {
            let tx = conn.transaction()?;
            for (up_id, up_status, up_err) in updates_to_apply {
                let now = Local::now().timestamp();
                tx.execute(
                    "UPDATE downloads SET status = ?1, error_message = ?2, updated_at = ?3 WHERE id = ?4",
                    params![up_status, up_err, now, up_id as i64],
                )?;
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
        };
        let error_msg = match &item.status {
            DownloadStatus::Error(e) => Some(e.as_str()),
            _ => None,
        };

        let id = if item.id > 0 {
            tx.execute(
                "INSERT INTO downloads (
                    id, url, filename, save_path, total_size, status,
                    created_at, updated_at, error_message
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
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
                ],
            )?;
            item.id
        } else {
            tx.execute(
                "INSERT INTO downloads (
                    url, filename, save_path, total_size, status,
                    created_at, updated_at, error_message
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    item.url,
                    item.filename,
                    item.save_path.to_string_lossy().to_string(),
                    item.total_size.map(|s| s as i64),
                    status_str,
                    now,
                    now,
                    error_msg,
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

    pub fn mark_completed(&self, id: u64) -> Result<(), DbError> {
        let conn = self.conn.lock().unwrap();
        let now = Local::now().timestamp();
        let rows = conn.execute(
            "UPDATE downloads 
             SET status = 'Completed', completed_at = ?1, updated_at = ?2, error_message = NULL 
             WHERE id = ?3",
            params![now, now, id as i64],
        )?;
        if rows == 0 {
            return Err(DbError::NotFound(id));
        }
        Ok(())
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
                    });
                }

                return (total_committed, segs);
            }
        }
    }
    (0, Vec::new())
}

/// Computes SHA-256 hash for arbitrary data (pure-Rust standard FIPS 180-4 implementation).
pub fn compute_sha256(data: &[u8]) -> String {
    let k: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0x00);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, w_val) in w.iter_mut().take(16).enumerate() {
            *w_val = u32::from_be_bytes([chunk[i * 4], chunk[i * 4 + 1], chunk[i * 4 + 2], chunk[i * 4 + 3]]);
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
            let temp1 = h_val.wrapping_add(s1).wrapping_add(ch).wrapping_add(k[i]).wrapping_add(w[i]);
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

    format!(
        "{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}",
        h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]
    )
}

/// Verifies whether the file at `path` matches the `expected_hex` SHA-256 digest.
pub fn verify_file_sha256(path: &Path, expected_hex: &str) -> Result<bool, std::io::Error> {
    let bytes = std::fs::read(path)?;
    let computed = compute_sha256(&bytes);
    Ok(computed.eq_ignore_ascii_case(expected_hex.trim()))
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
}
