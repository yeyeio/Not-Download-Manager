# HyperDownload Phase 4 — Persistent Download Database

Implement persistent download task management for HyperDownload.

Read the existing:
- `src/engine.rs`
- `src/types.rs`
- `src/ui.rs`
- `docs/advanced-validation-report.md`
- `docs/phase3-independent-audit.md`

The Phase 3 download engine must remain functionally unchanged unless a minimal integration change is necessary.

## Objective

Replace volatile GUI-only download task storage with a persistent SQLite database.

When the application closes and restarts, it must restore:
- Active downloads
- Paused downloads
- Queued downloads
- Completed downloads
- Failed downloads
- Download metadata and history

## Database Design

Use SQLite.

Create a versioned schema containing:

### downloads
- id
- url
- filename
- save_path
- total_size
- status
- created_at
- updated_at
- completed_at
- error_message
- etag
- last_modified
- expected_sha256 (nullable)

### download_settings
- download_id
- max_connections
- chunk_size
- speed_limit
- retry_limit

Keep the database schema independent of egui.

Use migrations rather than destructive schema recreation.

## Startup Recovery

When HyperDownload starts:

1. Open the database.
2. Load all non-deleted tasks.
3. Inspect corresponding destination files and `.partinfo` metadata.
4. Reconcile database status with actual engine state.
5. Detect missing partial files.
6. Detect completed downloads.
7. Detect stale or invalid checkpoints.
8. Restore eligible downloads to the GUI.

Never assume that database status alone proves file completion.

## Resume Behavior

- Paused downloads must be resumable.
- Interrupted downloads must be recoverable.
- Completed downloads must not be downloaded again.
- ETag mismatch must invalidate stale resume metadata.
- Missing partial files must trigger safe recovery.
- Failed downloads must preserve their error reason.

## UI Integration

Preserve the existing egui layout and visual design.

Replace hardcoded sample tasks with database-backed tasks.

Ensure UI actions update the engine and database consistently:
- Add
- Start
- Pause
- Resume
- Cancel
- Delete
- Retry

Define distinct semantics for cancel and delete.

## Reliability

- Use database transactions for related state changes.
- Avoid blocking the Tokio runtime with synchronous database operations.
- Prevent duplicate downloads caused by startup recovery.
- Handle database corruption safely.
- Do not delete partial files unless explicitly required by the operation.
- Keep checkpoint metadata and database task identity consistent.

## Tests

Test:
1. Application restart with active downloads.
2. Application restart with paused downloads.
3. Application restart with completed downloads.
4. Database entry without partial file.
5. Partial file without valid database entry.
6. Corrupted SQLite database.
7. ETag mismatch during restoration.
8. Duplicate startup recovery prevention.
9. Pause, close, restart, resume.
10. Delete task with partial file cleanup.

Verify SHA-256 for recovered downloads.

## Deliverables

- SQLite implementation.
- Versioned migrations.
- UI integration.
- Startup recovery mechanism.
- Unit and integration tests.
- `docs/phase4-persistence-report.md`.

Do not redesign the GUI.
Do not rewrite the Phase 3 engine.
Do not claim persistence is complete until application restart tests pass.