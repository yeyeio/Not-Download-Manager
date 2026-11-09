# HyperDownload Phase 4.3 — Test Infrastructure & Harness Specification

## 1. Test Philosophy

HyperDownload Phase 4.3 hardening employs an **opaque-box, requirement-driven, 4-tier test methodology**.

### 1.1 Opaque-Box Testing
Tests interact strictly through public crate boundaries, observable system events (`EngineEvent`), persistent database state (`Database`), and filesystem artifacts (`.partinfo`, output files). Tests do not inspect or bind to private coordinator state, private mutex locks, or internal task channels. This decoupling guarantees test resilience against internal refactoring while enforcing behavioral compliance.

### 1.2 Requirement-Driven Traceability
Every test directly validates specific acceptance criteria defined in `ORIGINAL_REQUEST.md` (R1–R5) and resolves findings from `docs/phase4.2-independent-audit.md` (FINDING-4.2-01 through FINDING-4.2-10).

### 1.3 Four-Tier Test Methodology
1. **Tier 1: Feature Coverage**
   Validates the primary happy path and fundamental behavior of each hardening capability: streaming SHA-256 NIST vectors, Delete priority over Pause/Cancel, distinct Pause vs Cancel semantics, ActiveTaskGuard instance generation tokens, and CleanupPending non-terminal retention.
2. **Tier 2: Boundary & Corner Cases**
   Validates system resilience at extreme boundaries: zero-byte files, 56-byte SHA-256 block-overflow boundary, 1,000,000-character payloads, arbitrary streaming buffer boundaries, and zero-delay start/delete race conditions.
3. **Tier 3: Cross-Feature Combinations**
   Validates asynchronous race conditions across concurrent operations: Delete dispatched during final writer flush, Delete during pause teardown, StartDownload dispatched while deletion is actively unlinking files, and Cancel followed by successful Resume.
4. **Tier 4: Real-World Workloads & Deterministic Stress**
   Validates non-functional stability, memory safety, and concurrency invariants at scale:
   - 10,000 lifecycle operations measuring active tasks, known paths, active workers, and Win32 process memory (`GetProcessMemoryInfo` FFI), asserting zero leaks.
   - 1,000+ randomized concurrent command interleavings across multiple recorded independent random seeds using a deterministic PRNG (`Xoshiro256++`), event trace logging, invariant verification, and seed reproduction on failure.

---

## 2. Feature Inventory & Coverage Mapping

The table below maps all 25 features from `PROJECT.md` to their corresponding test tier, integration test implementation in `tests/test_phase4_3_hardening.rs`, and test count.

| # | Feature | Description | Tier | Covering Test in `tests/test_phase4_3_hardening.rs` | Test Count |
|---|---------|-------------|------|-----------------------------------------------------|------------|
| 1 | Explicit Task Lifecycle State Machine | Coordinator lifecycle state machine with Delete priority | Tier 1, Tier 3 | `test_tier1_delete_priority_over_pause_and_cancel`, `test_tier3_delete_during_pause_teardown` | 2 |
| 2 | Delete Priority over Pause/Cancel | Ongoing Pause/Cancel switches to Delete; suppresses zombie checkpoint | Tier 1, Tier 3 | `test_tier1_delete_priority_over_pause_and_cancel`, `test_tier3_delete_during_pause_teardown` | 2 |
| 3 | Post-Loop Delete Boundary Check | Coordinator checks delete_flag after chunk loop break during writer flush | Tier 3 | `test_tier3_delete_during_final_writer_flush` | 1 |
| 4 | Teardown Asynchronous Boundary Checks | Check delete_flag after worker joins, writer await, ACK drain | Tier 1, Tier 3 | `test_tier1_delete_priority_over_pause_and_cancel`, `test_tier3_delete_during_final_writer_flush` | 2 |
| 5 | Deterministic Single Finalization Routine | Centralized finalization ensuring exactly one terminal event per task instance | Tier 1, Tier 4 | `test_tier1_delete_priority_over_pause_and_cancel`, `test_tier4_randomized_concurrent_interleavings` | 2 |
| 6 | Distinct Pause Semantics | Suspends transfer, flushes atomic checkpoint, emits Paused | Tier 1 | `test_tier1_distinct_pause_vs_cancel_semantics` | 1 |
| 7 | Distinct Cancel Semantics | Cancels transfer, preserves checkpoint, emits Canceled, requires resume | Tier 1, Tier 3 | `test_tier1_distinct_pause_vs_cancel_semantics`, `test_tier3_cancel_then_resume_lifecycle` | 2 |
| 8 | Task-Instance Generation Tokens | Monotonic instance_id preventing stale eviction on ID reuse | Tier 1, Tier 4 | `test_tier1_active_task_guard_generation_token`, `test_tier4_randomized_concurrent_interleavings` | 2 |
| 9 | StartDownload Deletion Serialization / Rejection | Serialize or reject StartDownload while same ID is deleting | Tier 3 | `test_tier3_start_download_during_active_delete` | 1 |
| 10 | Mutex Poisoning Handling | Explicit handling of poisoned locks on active_tasks and shared maps | Tier 1, Tier 4 | `test_tier1_active_task_guard_generation_token`, `test_tier4_randomized_concurrent_interleavings` | 2 |
| 11 | Deterministic `known_paths` Eviction | Remove entries on terminal states; retain only when cleanup fails for retry | Tier 1, Tier 4 | `test_tier1_cleanup_pending_handling`, `test_tier4_10k_lifecycle_operations_memory_and_resource_stress` | 2 |
| 12 | Streaming `Sha256Hasher` | Pure-Rust FIPS 180-4 incremental hasher processing 64 KiB blocks (< 5 MB peak RAM) | Tier 1, Tier 2 | `test_tier1_streaming_sha256_nist_vectors`, `test_tier2_sha256_multi_buffer_streaming_continuity` | 2 |
| 13 | Streaming File Verification API | Streaming file verification without std::fs::read whole-file loading | Tier 1, Tier 2 | `test_tier1_streaming_sha256_nist_vectors`, `test_tier2_sha256_one_million_a` | 2 |
| 14 | Runtime Verification Decoupling | Decouple completion from verification: NotConfigured, Verified, VerificationFailed | Tier 1 | `test_tier1_streaming_sha256_nist_vectors` | 1 |
| 15 | Persisted Hash Status | Store hash verification status and expected hash in SQLite database | Tier 1 | `test_tier1_streaming_sha256_nist_vectors` | 1 |
| 16 | Safe Background Reconciliation Merge | Merge reconciliation snapshot with UI using ID sets and mutation tracking | Tier 1 | `test_tier1_cleanup_pending_handling` | 1 |
| 17 | Prevention of Deleted Record Resurrection | Reconciliation snapshot cannot re-insert tasks deleted while reconciliation was pending | Tier 1, Tier 3 | `test_tier1_delete_priority_over_pause_and_cancel`, `test_tier3_delete_during_pause_teardown` | 2 |
| 18 | `CleanupPending` Non-Terminal State | When filesystem unlinking fails, preserve SQLite row, UI item, and cleanup metadata | Tier 1 | `test_tier1_cleanup_pending_handling` | 1 |
| 19 | Idempotent Cleanup Retry Action | Retry filesystem deletion for CleanupPending items without spawning a coordinator | Tier 1 | `test_tier1_cleanup_pending_handling` | 1 |
| 20 | Partial Cleanup State Tracking | Record which files were unlinked (.partinfo, .tmp) and which remain locked | Tier 1 | `test_tier1_cleanup_pending_handling` | 1 |
| 21 | Configurable Connect & Inactivity Timeouts | Client with connect timeout and read inactivity timeout | Tier 3 | `test_tier3_cancel_then_resume_lifecycle` | 1 |
| 22 | Interruptible Worker Stream Teardown | Worker chunk streaming uses select! on cancellation/deletion to abort immediately | Tier 1, Tier 3 | `test_tier1_delete_priority_over_pause_and_cancel`, `test_tier3_delete_during_pause_teardown` | 2 |
| 23 | Randomized Concurrency Stress Testing | Concurrency test suite with >=1,000 interleavings across multiple recorded seeds | Tier 4 | `test_tier4_randomized_concurrent_interleavings` | 1 |
| 24 | Resource & Memory Leak Stress Testing | 10,000 lifecycle operations test asserting 0 leaked entries and 0 unmanaged workers | Tier 4 | `test_tier4_10k_lifecycle_operations_memory_and_resource_stress` | 1 |
| 25 | Reliability Hardening Report Deliverable | docs/phase4.3-reliability-hardening-report.md with root causes, remediations, and results | M4 | Validated via full suite pass | 16 |

---

## 3. Test Architecture & Infrastructure Components

### 3.1 Suite Layout
The complete integration test suite is located in `tests/test_phase4_3_hardening.rs`. It executes independently of other test suites and can be run in parallel or isolation.

### 3.2 Deterministic Pseudo-Random Number Generator (`Xoshiro256PlusPlus`)
To avoid flaky timing-dependent tests and external crate dependencies (`rand`), the harness embeds a pure-Rust FIPS-quality `Xoshiro256++` generator:
- **Seed Size**: 256-bit internal state initialized from a 64-bit seed.
- **Portability**: Produces identical pseudo-random operation sequences across Windows, Linux, and macOS on both x86_64 and aarch64.
- **Diagnostic Logging**: Each stress run records the exact seeds used. On invariant violation, the seed is printed with the exact command sequence and event history for 100% deterministic reproduction.

### 3.3 Event Trace Collector & Invariant Verifier
The harness wraps `EngineEvent` receivers into an `EventCollector` thread-safe struct:
- Tracks event sequences, timestamps, and task IDs.
- Provides `wait_for_condition` and `wait_for_terminal` primitives with bounded polling rather than arbitrary `sleep()` calls.
- Enforces invariant verification:
  * **Exactly-Once Terminal Event**: Each task instance emits at most one of `Finished`, `Canceled`, `Deleted`, or `Failed`.
  * **Post-Deletion Silence**: Once `Deleted` is emitted, no subsequent chunk writes or `.partinfo` updates occur.
  * **Guard Isolation**: Stale task drops do not evict active reused task IDs.

### 3.4 In-Process HTTP 206 Mock Streaming Server
Mock servers are instantiated in-process using `tokio::net::TcpListener`:
- Dynamically binds to `127.0.0.1:0` to guarantee zero port collisions.
- Supports `HEAD` and `GET` requests with `Content-Range: bytes START-END/TOTAL`.
- Allows configurable chunk delays and simulated worker streaming stalls to exercise timeout and cancellation teardown paths.

### 3.5 Win32 Process Memory FFI (`psapi.dll`)
For memory leak verification during the 10,000-operation stress test, the harness queries Windows process memory counters directly via the Win32 API:
```rust
#[repr(C)]
pub struct ProcessMemoryCounters {
    pub cb: u32,
    pub page_fault_count: u32,
    pub peak_working_set_size: usize,
    pub working_set_size: usize,
    pub quota_peak_paged_pool_usage: usize,
    pub quota_paged_pool_usage: usize,
    pub quota_peak_non_paged_pool_usage: usize,
    pub quota_non_paged_pool_usage: usize,
    pub pagefile_usage: usize,
    pub peak_pagefile_usage: usize,
}
```
Queries `working_set_size` and `pagefile_usage` (Private Bytes) before and after 10,000 operations to prove absence of unbounded memory leaks.

### 3.6 Resilient Filesystem Sandbox (`TestDir`)
Temporary test directories are created in `std::env::temp_dir()` using nanosecond-timestamped unique IDs:
- Prevents collisions between concurrent test runs.
- Includes a bounded retry loop on `remove_dir_all` in `Drop` to tolerate transient Windows file locking from anti-virus scanners or search indexers.

---

## 4. Test Runner Instructions

### 4.1 Running the Hardening Suite
Execute the Phase 4.3 hardening test suite with full output:
```powershell
cargo test --test test_phase4_3_hardening -- --nocapture
```

### 4.2 Running a Specific Tier
Filter by test name:
```powershell
# Run Tier 1 Feature Tests
cargo test --test test_phase4_3_hardening test_tier1 -- --nocapture

# Run Tier 2 Boundary Tests
cargo test --test test_phase4_3_hardening test_tier2 -- --nocapture

# Run Tier 3 Concurrency Combination Tests
cargo test --test test_phase4_3_hardening test_tier3 -- --nocapture

# Run Tier 4 Workload & Randomized Stress Tests
cargo test --test test_phase4_3_hardening test_tier4 -- --nocapture
```

### 4.3 Running the Full Project Test Baseline
Verify all 3 test targets (lib unittests, Phase 4 persistence, Phase 4.1 remediation, and Phase 4.3 hardening) without linker lock contention:
```powershell
cargo test --tests
```

### 4.4 Environment Permissions Note
Because the integration tests bind local loopback TCP sockets (`127.0.0.1:0`) to simulate HTTP chunk servers, executing in restricted sandboxes requires socket bypass privileges (`BypassSandbox: true`). Running via standard elevated PowerShell or terminal succeeds with zero configuration.
