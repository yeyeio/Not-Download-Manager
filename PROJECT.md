# Project: HyperDownload Phase 4.3 — Concurrency & Lifecycle Hardening

## Architecture
HyperDownload is a high-performance multithreaded download manager built in Rust (2024 edition) with Tokio, Rusqlite, and egui/eframe.
Phase 4.3 hardens lifecycle determinism, concurrency ownership, memory safety, and verification integrity while strictly preserving:
- Phase 3 dynamic chunk leasing (`SegmentManager`, `lease_next_chunk`, generation-validated `commit_chunk`, `return_failed_chunk`)
- Phase 4 SQLite WAL persistence and `.partinfo` checkpoint serialization.

### Architectural Boundaries
1. **Types & Models (`src/types.rs`)**:
   - `DownloadStatus`: `Pending`, `Downloading`, `Paused`, `Canceled`, `Completed`, `Failed`, and `CleanupPending`.
   - `HashVerificationStatus`: `NotConfigured`, `Verified`, `VerificationFailed`.
   - `EngineEvent`: `Started`, `Progress`, `Paused`, `Canceled`, `Finished { id, hash_status }`, `Failed`, `Deleted { id, cleanup_success, error }`, `MetadataDiscovered`.
   - `TaskControl`: `instance_id: u64`, `cancel_flag`, `delete_flag`, `error_flag`, `terminal_emitted: AtomicBool`, `notify: Arc<Notify>`.
2. **Database & Storage (`src/db.rs`)**:
   - SQLite schema version 1 with backward-compatible columns (`hash_status`, `expected_sha256`).
   - Pure-Rust streaming `Sha256Hasher` processing 64 KiB blocks with FIPS 180-4 padding (< 5 MB peak RAM).
   - Streaming `verify_file_sha256` API.
3. **Download Engine (`src/engine.rs`)**:
   - Monotonic `instance_id` token generation.
   - `ActiveTaskGuard`: evicts from `active_tasks` ONLY if registered instance_id matches.
   - Mutex poisoning resilience (`unwrap_or_else`).
   - Deterministic `known_paths` cleanup on terminal exits; retained on failed cleanup.
   - Teardown deletion checks after worker joins, writer await, and ACK drains.
   - Centralized deterministic finalization routine: single convergence, exactly-once terminal event.
   - Decoupled off-thread streaming SHA-256 verification on completion.
   - Network client timeouts (10s connect, 30s read inactivity) and interruptible worker streaming.
   - Observability metrics: `engine.metrics()` (`active_tasks_count`, `known_paths_count`, `active_workers_count`).
4. **User Interface & App State (`src/ui.rs`)**:
   - Background startup reconciliation merge algorithm using task IDs and mutation/deletion sets.
   - `CleanupPending` non-terminal state handling and idempotent retry deletion without coordinator.
   - Terminal event status preservation.
5. **E2E Hardening & Stress Test Track (`tests/test_phase4_3_hardening.rs`)**:
   - 10,000 lifecycle operations stress test measuring tasks, paths, workers, and Win32 process memory.
   - 1,000+ randomized concurrent command interleavings across multiple recorded seeds with deterministic synchronization.
   - Complete NIST SHA-256 test vectors and decoupled verification assertions.

---

## Feature Inventory
Every feature from the Survey phase is mapped to an assigned milestone:

| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Explicit Task Lifecycle State Machine | Manage coordinator states with Delete priority | M2 | Survey 1 & 2 |
| 2 | Delete Priority over Pause/Cancel | Ongoing Pause/Cancel switches to Delete if requested; suppresses zombie checkpoint | M2 | FINDING-4.2-02 |
| 3 | Post-Loop Delete Boundary Check | Coordinator checks delete_flag after chunk loop break during writer flush | M2 | FINDING-4.2-01 |
| 4 | Teardown Asynchronous Boundary Checks | Check delete_flag after each async await boundary (worker joins, writer await, ACK drain) | M2 | Survey 1 & 2 |
| 5 | Deterministic Single Finalization Routine | Centralized convergence ensuring exactly one terminal event per task instance | M2 | ORIGINAL_REQUEST R1 |
| 6 | Distinct Pause Semantics | Suspends download, flushes atomic .partinfo checkpoint, emits Paused | M2 | FINDING-4.2-03 |
| 7 | Distinct Cancel Semantics | Cancels download, preserves checkpoint, emits Canceled, requires resume/retry | M2 | FINDING-4.2-03 |
| 8 | Task-Instance Generation Tokens | Monotonic instance_id on TaskControl & ActiveTaskGuard preventing stale eviction on ID reuse | M2 | FINDING-4.2-04 |
| 9 | StartDownload Deletion Serialization / Rejection | Serialize or reject StartDownload while same ID is deleting | M2 | ORIGINAL_REQUEST R2 |
| 10 | Mutex Poisoning Handling | Explicit handling of poisoned locks on active_tasks and shared maps | M2 | FINDING-4.2-04 |
| 11 | Deterministic `known_paths` Eviction | Remove entries on terminal states; retain only when cleanup fails for retry | M2 | FINDING-4.2-05 |
| 12 | Streaming `Sha256Hasher` | Pure-Rust FIPS 180-4 incremental hasher processing 64 KiB blocks (< 5 MB peak RAM) | M1 | FINDING-4.2-06 |
| 13 | Streaming File Verification API | Streaming file verification without std::fs::read whole-file loading | M1 | FINDING-4.2-06 |
| 14 | Runtime Verification Decoupling | Decouple completion from verification: NotConfigured, Verified, VerificationFailed | M1, M2 | FINDING-4.2-10 |
| 15 | Persisted Hash Status | Store hash verification status and expected hash in SQLite database | M1 | FINDING-4.2-10 |
| 16 | Safe Background Reconciliation Merge | Merge reconciliation snapshot with UI using ID sets and mutation/deletion tracking | M3 | FINDING-4.2-07 |
| 17 | Prevention of Deleted Record Resurrection | Reconciliation snapshot cannot re-insert tasks deleted while reconciliation was pending | M3 | FINDING-4.2-07 |
| 18 | `CleanupPending` Non-Terminal State | When filesystem unlinking fails, preserve SQLite row, UI item, and cleanup metadata | M3 | FINDING-4.2-08 |
| 19 | Idempotent Cleanup Retry Action | Retry filesystem deletion for CleanupPending items without spawning a coordinator | M3 | FINDING-4.2-08 |
| 20 | Partial Cleanup State Tracking | Record which files were unlinked (.partinfo, .tmp) and which remain locked | M3 | FINDING-4.2-08 |
| 21 | Configurable Connect & Inactivity Timeouts | reqwest::Client with 10s connect timeout and 30s read inactivity timeout | M2 | FINDING-4.2-09 |
| 22 | Interruptible Worker Stream Teardown | Worker chunk streaming uses select! on cancellation/deletion to abort immediately | M2 | FINDING-4.2-09 |
| 23 | Randomized Concurrency Stress Testing | Concurrency test suite with >=1,000 interleavings across multiple recorded seeds | M4, E2E | ORIGINAL_REQUEST R5 |
| 24 | Resource & Memory Leak Stress Testing | 10,000 lifecycle operations test asserting 0 leaked entries and 0 unmanaged workers | M4, E2E | ORIGINAL_REQUEST R5 |
| 25 | Reliability Hardening Report Deliverable | docs/phase4.3-reliability-hardening-report.md with root causes, remediations, and results | M4 | ORIGINAL_REQUEST R5 |

---

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Core Types, Streaming SHA-256 & DB Persistence | `src/types.rs`, `src/db.rs`: `HashVerificationStatus`, `CleanupPending`, streaming `Sha256Hasher`, streaming `verify_file_sha256`, SQLite columns, NIST vector tests | none | DONE (24 lib tests, 10 persistence tests, 5 hardening SHA-256 tests, CLEAN audit) |
| M2 | Concurrency Ownership & Lifecycle Finalization | `src/engine.rs`: Monotonic instance tokens, ActiveTaskGuard validation, mutex unwrap, known_paths eviction, teardown delete checks, centralized finalization, distinct Pause/Cancel, network timeouts, engine metrics | M1 | DONE (28 lib, 10 persistence, 8/8 lifecycle challenger, 11/11 resource challenger, 6/6 stress pass; CLEAN audit) |
| M3 | UI Startup Reconciliation & CleanupPending State | `src/ui.rs`: Non-blocking reconciliation merge algorithm, deleted tombstone tracking, CleanupPending UI state & retry action, terminal event updates | M1, M2 | DONE (38 lib, 10 persistence, 8/8 reconciliation challenger, 10/10 cleanup challenger pass; CLEAN audit) |
| M4 | Final Milestone: E2E Test Pass & Reliability Report | Pass 100% of E2E test suite (Tiers 1-4), adversarial hardening (Tier 5), generate `docs/phase4.3-reliability-hardening-report.md` | M1, M2, M3, E2E | IN_PROGRESS |
| E2E | E2E Testing Track | Design and publish `TEST_INFRA.md`, build `tests/test_phase4_3_hardening.rs` covering Tiers 1-4, publish `TEST_READY.md` | M1 | DONE (TEST_READY.md published, 16 integration tests) |

---

## Interface Contracts

### Types ↔ DB / Engine / UI
- `DownloadStatus::CleanupPending`
- `HashVerificationStatus`: `NotConfigured`, `Verified`, `VerificationFailed`
- `EngineEvent::Canceled { id: u64 }`
- `EngineEvent::Finished { id: u64, hash_status: HashVerificationStatus }`
- `EngineMetrics`:
  ```rust
  pub struct EngineMetrics {
      pub active_tasks_count: usize,
      pub known_paths_count: usize,
      pub active_workers_count: usize,
  }
  ```

### Engine ↔ Coordinator
- `TaskControl`:
  ```rust
  pub struct TaskControl {
      pub instance_id: u64,
      pub cancel_flag: Arc<AtomicBool>,
      pub delete_flag: Arc<AtomicBool>,
      pub pause_flag: Arc<AtomicBool>,
      pub error_flag: Arc<AtomicBool>,
      pub terminal_emitted: Arc<AtomicBool>,
      pub notify: Arc<Notify>,
  }
  ```
- `ActiveTaskGuard`:
  ```rust
  pub struct ActiveTaskGuard {
      pub id: u64,
      pub instance_id: u64,
      pub active_tasks: Arc<Mutex<HashMap<u64, TaskControl>>>,
  }
  ```

### DB ↔ Hashing
- `Sha256Hasher`:
  - `pub fn new() -> Self`
  - `pub fn update(&mut self, data: &[u8])`
  - `pub fn finalize(self) -> [u8; 32]`
  - `pub fn finalize_hex(self) -> String`
- `verify_file_sha256(path: &Path, expected_hex: &str) -> Result<bool, std::io::Error>`: uses 64 KiB reusable buffer, off the UI thread.

---

## Code Layout
- `src/types.rs`: Shared enums, structs, events, commands
- `src/db.rs`: Database queries, schema migrations, streaming SHA-256 implementation
- `src/engine.rs`: Task coordinator, chunk leasing orchestration, network download workers, file writer actor, lifecycle finalization, engine metrics
- `src/ui.rs`: Application state, background reconciliation merge, user event dispatch, rendering
- `tests/test_phase4_persistence.rs`: Phase 4 persistence integration tests (preserved)
- `tests/test_phase4_1_remediation.rs`: Phase 4.1 remediation integration tests (preserved)
- `tests/test_phase4_3_hardening.rs`: Phase 4.3 E2E and concurrency stress test suite
