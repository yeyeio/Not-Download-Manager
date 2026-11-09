# HyperDownload Phase 3 — Dynamic Segmentation Implementation

You are working on HyperDownload, a Rust-based download manager targeting Windows and Linux.

Read the existing source code and `advanced-validation-report.md` before making any changes.

## Objective

Replace the current static equal-range partitioning implementation with a genuine dynamic segmentation coordinator.

The current implementation divides the file into fixed ranges at download startup. Workers cannot redistribute unfinished work when other workers finish early. This creates head-of-line blocking.

## Required Architecture

Implement a centralized `SegmentManager` responsible for:

1. Tracking pending, active, completed, and failed byte ranges.
2. Assigning work to available workers.
3. Detecting idle workers.
4. Redistributing unfinished ranges when workers become idle.
5. Splitting large remaining ranges into smaller ranges.
6. Preventing overlapping or missing byte ranges.
7. Tracking completed bytes independently from downloaded network bytes.

Use half-open byte intervals `[start, end)` consistently.

## Scheduling Requirements

- Workers must not be permanently tied to their initial segments.
- An idle worker should request additional work.
- A slow worker must not block all other workers from contributing to the remaining bytes.
- Segment splitting must respect minimum and maximum chunk sizes.
- Avoid excessive tiny HTTP requests.
- Support configurable maximum concurrency.
- Handle workers that fail while processing a segment.
- Failed ranges must return to the pending queue when retry policy allows.

## Correctness Requirements

- No overlapping byte ranges.
- No missing byte ranges.
- No writing outside the destination file boundaries.
- Every completed range must be accounted for exactly once.
- Validate HTTP 206 and Content-Range before writing.
- Never write a full HTTP 200 response at an arbitrary segment offset.
- Preserve existing pause, cancel, and resume functionality.
- Do not mark a download completed until all ranges are verified.

## Testing Requirements

Create deterministic tests for:

1. Uniform worker speeds.
2. One worker throttled to 5 MB/s.
3. One worker disconnected during transfer.
4. Multiple workers becoming idle.
5. Segment splitting at file boundaries.
6. Concurrent segment assignment.
7. Overlap and missing-range detection.
8. Pause and resume during redistribution.

Use the existing advanced benchmark server where possible.

Compare:
- Static segmentation
- Dynamic segmentation

Record:
- Total download duration
- Aggregate throughput
- Worker idle time
- Number of range requests
- Number of segment redistributions
- SHA-256 integrity

## Constraints

- Preserve the existing GUI and user experience.
- Do not rewrite unrelated modules.
- Do not introduce unnecessary dependencies.
- Keep the engine modular and testable.
- Do not claim performance improvements without benchmark evidence.

## Deliverables

1. Implementation changes.
2. Unit and integration tests.
3. Architecture explanation.
4. Benchmark results comparing static and dynamic scheduling.
5. Updated `advanced-validation-report.md`.
6. List of remaining limitations.

Before coding, explain the proposed architecture and identify risks in the existing implementation. Then implement and test it.