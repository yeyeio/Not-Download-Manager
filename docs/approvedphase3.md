# APPROVED — HyperDownload Phase 3 Implementation

The Fixed-Size Chunks with Dynamic Lease Scheduling architecture is approved.

Begin implementation in the existing HyperDownload repository.

Before modifying code, preserve the current implementation in Git or create a dedicated feature branch.

## Architecture

Implement a centralized SegmentManager as the single source of truth.

Use:
- Fixed-size byte chunks.
- Dynamic worker assignment.
- Per-chunk lease ownership.
- Lease generation numbers.
- Explicit Pending, Leased, Committed, and Failed states.
- Per-chunk committed status.

Start with a configurable 2 MB chunk size.

## Mandatory Correctness Requirements

1. Use half-open intervals `[start, end)`.
2. Never allow overlapping active leases.
3. Never allow missing byte ranges.
4. Reject stale ACKs using lease generation.
5. Separate received bytes, written bytes, and durable committed bytes.
6. Do not mark a chunk committed before receiving a valid writer acknowledgment.
7. Validate HTTP 206, Content-Range, total size, and response body length.
8. Reject HTTP 200 responses to range requests without writing their body at the requested offset.
9. Classify transient and permanent errors.
10. Implement bounded retries with exponential backoff, jitter, and Retry-After support.
11. Persist committed chunk state through crash-consistent checkpointing.
12. Validate ETag or Last-Modified before resuming.

## Writer Protocol

Use an explicit write request and acknowledgment protocol.

The acknowledgment must identify:
- chunk_id
- lease_generation
- bytes_written

The writer must report errors rather than silently dropping failed writes.

Define the required flush/durability policy and document its guarantees on Windows and Linux.

## Testing

Implement deterministic tests for:
- Uniform network speeds.
- One throttled worker.
- Multiple idle workers.
- Worker failure and lease recovery.
- Stale ACK rejection.
- Invalid Content-Range.
- HTTP 200 range fallback.
- Partial writer failure.
- Checkpoint corruption.
- Forced process termination.
- Remote ETag changes.
- Pause and resume.
- Missing and overlapping byte ranges.

For every completed test file, verify SHA-256 against the original source.

## Benchmark

Compare:
- Existing static segmentation.
- Dynamic fixed-size scheduling.

Test chunk sizes:
- 512 KB
- 1 MB
- 2 MB
- 4 MB

Record:
- Total duration
- Aggregate throughput
- Worker utilization
- Idle time
- Number of HTTP requests
- Retry count
- Peak RAM
- CPU usage
- SHA-256 result

Include uniform and impaired network scenarios.

## Implementation Rules

- Preserve the existing egui GUI and its layout.
- Avoid unrelated refactoring.
- Keep SegmentManager independent of UI code.
- Keep checkpoint format versioned.
- Do not claim crash consistency without testing forced termination.
- Do not claim performance competitiveness against IDM without an apples-to-apples benchmark.

## Deliverables

1. Implemented source code.
2. Unit and integration tests.
3. Updated architecture documentation.
4. Benchmark comparison.
5. Updated `docs/advanced-validation-report.md`.
6. List of unresolved risks.

Implement in small stages. Run tests after each stage and report failures honestly.