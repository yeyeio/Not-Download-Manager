# HyperDownload Phase 4.3 — Test Suite Ready Notification

**Status**: READY FOR VERIFICATION  
**Author**: `teamwork_test_writer_e2e_1`  
**Date**: 2026-11-09T09:00:00Z  
**Target Milestone**: Phase 4.3 Concurrency & Lifecycle Hardening (Tiers 1–4)

---

## 1. Test Suite Deliverables

1. **Test Infrastructure Specification (`TEST_INFRA.md`)**:
   - Location: `TEST_INFRA.md` (project root)
   - Scope: Opaque-box methodology, 4-tier test architecture, feature inventory mapping covering all 25 features from `PROJECT.md`, runner instructions, and deterministic PRNG specification.

2. **Phase 4.3 Hardening Integration Test Suite (`tests/test_phase4_3_hardening.rs`)**:
   - Location: `tests/test_phase4_3_hardening.rs`
   - Scope: 16 comprehensive integration tests covering Tiers 1 through 4.
   - Compilation: Compiles cleanly with zero errors and zero warnings (`cargo check --test test_phase4_3_hardening`).

---

## 2. Test Suite Architecture & Inventory

### Tier 1: Feature Coverage (5 Tests)
- `test_tier1_streaming_sha256_nist_vectors`: FIPS 180-4 standard test vectors (Empty, `"abc"`, 56-byte), hash mismatch assertion, missing file handling, and decoupled `HashVerificationStatus` states.
- `test_tier1_delete_priority_over_pause_and_cancel`: Delete priority over ongoing Pause/Cancel, suppression of zombie checkpoint `.partinfo`, and clean filesystem unlinking.
- `test_tier1_distinct_pause_vs_cancel_semantics`: Distinct `EngineEvent::Paused` (preserves partial file + checkpoint) vs `EngineEvent::Canceled` (preserves partial file + checkpoint without emitting `Paused`).
- `test_tier1_active_task_guard_generation_token`: Task ID reuse guard isolation; prevents stale guard drop from evicting active task instances.
- `test_tier1_cleanup_pending_handling`: `CleanupPending` non-terminal state on transient file lock, database row preservation, and idempotent deletion retry.

### Tier 2: Boundary & Corner Cases (5 Tests)
- `test_tier2_sha256_empty_file`: 0-byte file hashing against authoritative NIST empty digest.
- `test_tier2_sha256_one_million_a`: 1,000,000 `'a'` characters against authoritative NIST digest (`cdc76e5c...`).
- `test_tier2_sha256_56_byte_boundary`: 56-byte message boundary testing SHA-256 two-block padding overflow.
- `test_tier2_sha256_multi_buffer_streaming_continuity`: Multi-chunk patterned payload across 64 KiB internal streaming buffers.
- `test_tier2_rapid_start_delete_race`: Rapid back-to-back `StartDownload` and `DeleteDownload` across 10 tasks in rapid succession.

### Tier 3: Cross-Feature Combinations (4 Tests)
- `test_tier3_delete_during_final_writer_flush`: Delete boundary check catching deletion intent during writer flush and suppressing `Finished`.
- `test_tier3_delete_during_pause_teardown`: Delete arriving during pause teardown, superseding pause and removing checkpoints.
- `test_tier3_start_download_during_active_delete`: Rejection/serialization of `StartDownload` while active deletion unlinks files.
- `test_tier3_cancel_then_resume_lifecycle`: Cancel preserves state; subsequent `StartDownload` resumes from checkpoint and completes cleanly.

### Tier 4: Real-World Workload Scenarios (2 Tests)
- `test_tier4_10k_lifecycle_operations_memory_and_resource_stress`: 10,000 lifecycle operations (Start, Pause, Cancel, Delete, Resume) measuring active tasks, workers, and Win32 process memory via `GetProcessMemoryInfo` FFI.
- `test_tier4_randomized_concurrent_interleavings`: 1,000+ randomized concurrent command interleavings across 4 recorded independent random seeds (`0x1234_5678_9ABC_DEF0`, `0xFEDC_BA98_7654_3210`, `0x0123_4567_89AB_CDEF`, `0xA5A5_5A5A_0F0F_F0F0`) with deterministic `Xoshiro256++` PRNG, event trace logging, invariant verification, and seed reproduction on failure.

---

## 3. How to Run the Tests

Execute the Phase 4.3 Hardening Test Suite:
```powershell
cargo test --test test_phase4_3_hardening -- --nocapture
```

Execute Full Test Baseline (Unit Tests, Persistence, Remediation, Hardening):
```powershell
cargo test --tests
```

*Note on Network Permissions*: Integration tests run an in-process HTTP 206 mock streaming server dynamically bound to `127.0.0.1:0`. In sandboxed environments requiring network privileges, run with loopback socket access (`BypassSandbox: true`).

---

## 4. Current Pass/Fail Status & Implementation Defect Escalations

### Current Execution Results:
- **Total Tests**: 16
- **Passed**: 12 (75%)
- **Failed**: 4 (25%) — all 4 failures directly confirm outstanding audit findings targeted for Milestone 2 remediation.

### Escalated Implementation Defects (for M2 Implementer):
1. **FINDING-4.2-03 (Distinct Cancel Semantics Missing)**:
   - *Failing Tests*: `test_tier1_distinct_pause_vs_cancel_semantics`, `test_tier3_cancel_then_resume_lifecycle`.
   - *Observation*: `src/engine.rs:1215-1234`: When `ctrl.cancel_flag` is set, the coordinator unconditionally emits `EngineEvent::Paused { id }` instead of `EngineEvent::Canceled { id }`.
   - *Required Fix*: Distinguish Pause vs Cancel in `TaskControl` and centralized finalization routine, emitting `EngineEvent::Canceled` and persisting Canceled status on `CancelDownload`.
2. **FINDING-4.2-04 (ActiveTaskGuard Stale Eviction on ID Reuse)**:
   - *Failing Test*: `test_tier1_active_task_guard_generation_token`.
   - *Observation*: `src/engine.rs:103-107`: `ActiveTaskGuard::drop` calls `self.active_tasks.lock().unwrap().remove(&self.id)` keyed only by `u64 id`. When instance 1 drops after instance 2 has already reused the ID, it evicts instance 2.
   - *Required Fix*: Introduce monotonic `instance_id: u64` token on `TaskControl` and `ActiveTaskGuard`, removing from map only if `registered.instance_id == guard.instance_id`.
3. **FINDING-4.2-05 (known_paths Resource Leak)**:
   - *Failing Test*: `test_tier4_10k_lifecycle_operations_memory_and_resource_stress`.
   - *Observation*: `src/engine.rs:553`: `known_paths` entries are only evicted in inactive `DeleteDownload`. Tasks that complete, pause, fail, or are deleted while active permanently leak their `PathBuf` entries.
   - *Required Fix*: Implement deterministic `known_paths` eviction on terminal states while retaining only on failed cleanup for retry.
