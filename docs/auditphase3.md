# HyperDownload Phase 3 — Independent Code & Reliability Audit

Perform an independent verification of the Phase 3 implementation.

Read:
- `src/engine.rs`
- `src/types.rs`
- `src/ui.rs`
- `docs/advanced-validation-report.md`
- `benchmark/`

Do not modify source code during this audit.

## 1. SegmentManager Correctness
Verify:
- Lease ownership.
- Generation invalidation.
- Stale ACK rejection.
- Concurrent chunk acquisition.
- Duplicate lease prevention.
- Chunk state transitions.
- Complete byte-range coverage.

Use concurrency stress tests where appropriate.

## 2. Writer Durability
Inspect the actual write and acknowledgment ordering.

Determine whether:
- `write_all()` is followed by the required flush operation.
- Checkpoints can reference data not yet durable.
- Writer failures are propagated to the SegmentManager.
- Partial writes are recoverable.
- Windows and Linux filesystem semantics are handled correctly.

Clearly distinguish written, flushed, and durable data.

## 3. Crash Recovery
Test forced termination at:
- During chunk writing.
- Immediately after writer ACK.
- During checkpoint serialization.
- Immediately before atomic rename.
- Immediately after atomic rename.

Verify final file SHA-256 and checkpoint consistency.

## 4. Network Resilience
Test:
- TCP reset during a chunk.
- Repeated HTTP 429.
- HTTP 503 with Retry-After.
- Server ETag changes.
- Missing Content-Range.
- Incorrect Content-Range.
- Truncated response bodies.
- Server returning HTTP 200 to a Range request.

## 5. Benchmark Reproducibility
Re-run:
- Uniform bandwidth.
- One slow worker.
- Multiple slow workers.
- Intermittent network failures.
- Chunk sizes 512 KB, 1 MB, 2 MB, and 4 MB.

Verify benchmark calculations, especially median throughput and duration.

## 6. Deliverables
Create `docs/phase3-independent-audit.md`.

For every finding, include:
- Source file and line number.
- Actual implementation behavior.
- Reproduction steps.
- Severity.
- Pass/fail status.
- Recommended correction.

Do not award a production-ready verdict unless all critical reliability tests pass.

Do not change application behavior or UI during this audit.