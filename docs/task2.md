# TASK: HyperDownload Phase 2 — Advanced Validation & IDM Competitive Benchmark

The initial optimization phase has been completed. The application now supports actual HTTP Range requests, concurrent workers, preallocated storage, checkpointing, and resume.

Do NOT immediately modify the source code.

Your task is to independently validate the post-optimization implementation and determine whether it is genuinely competitive with Internet Download Manager.

## 1. Audit the actual implementation

Inspect the current source code.

Verify:
- Whether segmentation is genuinely dynamic or only static partitioning.
- Whether workers can steal or redistribute unfinished work.
- Whether concurrency adapts to throughput and server throttling.
- Whether checkpoint metadata is crash-consistent.
- Whether concurrent file writes are safe.
- Whether pause, cancel, and resume have distinct semantics.
- Whether HTTP 206 and Content-Range are validated.
- Whether ETag changes invalidate old checkpoints.
- Whether partial writes are recovered correctly.

Provide source file paths and line numbers.

## 2. Advanced segmentation benchmark

Build a controlled server capable of:
- Different bandwidth limits per connection.
- Artificial network latency.
- Random connection delays.
- Random connection failures.
- HTTP 429 and 503 responses.
- Variable segment response times.

Test:
- 1, 4, 8, 16, and 32 workers.
- One deliberately slow worker.
- One worker disconnected during download.
- Unequal throughput between workers.

Determine whether dynamic splitting and work redistribution actually improve completion time.

Do not claim dynamic scheduling if the implementation only uses fixed segment ranges.

## 3. Large-file benchmark

Test:
- 100 MB.
- 1 GB.
- 5 GB.
- 10 GB if hardware permits.

Use at least five repetitions.

Record:
- Median throughput.
- Minimum and maximum throughput.
- Completion time.
- CPU usage.
- RAM usage.
- Disk throughput.
- Number of HTTP Range requests.
- Actual concurrent sockets.
- SHA-256 verification.

## 4. Crash recovery validation

Test the following independently:

A. Normal pause/resume.

B. Forced process termination at 25%, 50%, and 75%.

C. Restart application and restore task metadata.

D. Resume after restart.

E. Corrupt or truncate checkpoint metadata.

F. Change ETag between interruption and resume.

G. Delete temporary file while retaining metadata.

For every case, verify that no corrupted final file is marked Completed.

## 5. IDM apples-to-apples benchmark

Run on the same Windows machine.

Use:
- Same source server.
- Same file.
- Same file size.
- Same network conditions.
- Same number of repetitions.
- Release build for HyperDownload.
- Identical output filesystem.

Measure IDM and HyperDownload separately.

Capture server logs to verify actual Range requests.

Do not infer IDM throughput from reverse-engineering observations.

If IDM cannot be measured reliably, clearly mark the comparison as incomplete.

## 6. Performance analysis

Explain why the current localhost benchmark shows lower throughput when connection count increases.

Determine whether the cause is:
- HTTP worker overhead.
- Tokio scheduling.
- Disk seek/write contention.
- Server limitations.
- Benchmark methodology.
- Network saturation.

Use profiling rather than speculation.

## 7. Final report

Create:

docs/advanced-validation-report.md

Include:
1. Executive summary.
2. Actual dynamic segmentation assessment.
3. Large-file benchmark.
4. Crash recovery test results.
5. IDM comparison.
6. Resource utilization.
7. Bottlenecks.
8. Reliability findings.
9. Prioritized optimization plan.
10. Final competitiveness verdict.

Use the following verdicts:

- NOT COMPETITIVE
- FUNCTIONALLY COMPETITIVE
- PERFORMANCE COMPETITIVE
- FULLY VALIDATED AGAINST IDM

Do not award a higher verdict without evidence.

Do not fabricate results.

Do not make source code changes during this validation phase unless a separate fix is explicitly approved.

The objective is to establish whether HyperDownload is genuinely competitive, not merely to improve its benchmark score.