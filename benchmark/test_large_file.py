#!/usr/bin/env python3
"""
Large-File Benchmark Harness for:
- 100 MB
- 1 GB
- 5 GB
Runs 5 repetitions per file size.
Measures:
- Throughput (median, min, max)
- Completion time
- Peak RAM usage (MB)
- CPU time (ms)
- Range requests count
- SHA-256 integrity match
"""

import os
import sys
import time
import json
import hashlib
import statistics
import subprocess
import threading
import ctypes
from ctypes import wintypes

BENCH_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BENCH_DIR)
import range_server

PORT = 8088
HOST = "127.0.0.1"
BASE_URL = f"http://{HOST}:{PORT}"
RUST_BIN = os.path.abspath(os.path.join(BENCH_DIR, "..", "target", "release", "bench_engine.exe"))
DOWNLOADS_DIR = os.path.join(BENCH_DIR, "downloads")

# Win32 Process Memory Structure
class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
    _fields_ = [
        ('cb', wintypes.DWORD),
        ('PageFaultCount', wintypes.DWORD),
        ('PeakWorkingSetSize', ctypes.c_size_t),
        ('WorkingSetSize', ctypes.c_size_t),
        ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
        ('QuotaPagedPoolUsage', ctypes.c_size_t),
        ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
        ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
        ('PagefileUsage', ctypes.c_size_t),
        ('PeakPagefileUsage', ctypes.c_size_t),
        ('PrivateUsage', ctypes.c_size_t),
    ]

def calculate_sha256(filepath):
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(4 * 1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def count_server_ranges():
    log_path = os.path.join(BENCH_DIR, "server.log")
    if not os.path.isfile(log_path):
        return 0
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for line in f if "RANGE REQUEST" in line)

def clear_server_log():
    log_path = os.path.join(BENCH_DIR, "server.log")
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass

def run_large_benchmarks():
    with open(os.path.join(BENCH_DIR, "checksums.json"), "r") as f:
        checksums = json.load(f)

    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    results = {}

    targets = [
        ("100 MB", "bench_100mb.bin", 5),
        ("1 GB", "bench_1gb.bin", 5),
        ("5 GB", "bench_5gb.bin", 5),
    ]

    print("==================================================")
    print("LARGE-FILE EMPIRICAL BENCHMARK SUITE")
    print("==================================================")

    for label, target_file, reps in targets:
        meta = checksums[target_file]
        expected_size = meta["size"]
        expected_sha = meta["sha256"]
        url = f"{BASE_URL}/{target_file}"
        out_path = os.path.join(DOWNLOADS_DIR, f"large_{target_file}")

        print(f"\n[BENCHMARK] Testing {label} ({expected_size / (1024*1024):.0f} MB) - {reps} Repetitions...")
        runs = []

        for r in range(reps):
            if os.path.exists(out_path):
                os.remove(out_path)
            clear_server_log()

            # Launch bench_engine with default IDM concurrency (8 connections)
            t0 = time.perf_counter()
            proc = subprocess.Popen([RUST_BIN, url, out_path, "8"])
            handle = proc._handle

            peak_ram_mb = 0.0
            # Poll process while running to measure peak WorkingSet
            while proc.poll() is None:
                try:
                    counters = PROCESS_MEMORY_COUNTERS_EX()
                    counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX)
                    if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                        ram = counters.WorkingSetSize / (1024 * 1024)
                        if ram > peak_ram_mb:
                            peak_ram_mb = ram
                except Exception:
                    pass
                time.sleep(0.02)

            duration = time.perf_counter() - t0
            actual_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
            speed_mb_s = (actual_size / (1024 * 1024)) / duration if duration > 0 else 0
            sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"
            ranges = count_server_ranges()

            run_data = {
                "rep": r + 1,
                "duration_s": round(duration, 4),
                "throughput_mb_s": round(speed_mb_s, 2),
                "peak_ram_mb": round(peak_ram_mb, 2),
                "ranges_logged": ranges,
                "sha_match": sha == expected_sha
            }
            runs.append(run_data)
            print(f"  Rep {r+1}/{reps}: {speed_mb_s:.2f} MB/s | {duration:.3f}s | Peak RAM: {peak_ram_mb:.1f} MB | Ranges: {ranges} | SHA-256: {sha == expected_sha}")
            time.sleep(0.5)

        durations = [x["duration_s"] for x in runs]
        speeds = [x["throughput_mb_s"] for x in runs]
        rams = [x["peak_ram_mb"] for x in runs]

        results[label] = {
            "file_size_bytes": expected_size,
            "repetitions": reps,
            "median_throughput_mb_s": round(statistics.median(speeds), 2),
            "min_throughput_mb_s": round(min(speeds), 2),
            "max_throughput_mb_s": round(max(speeds), 2),
            "median_duration_s": round(statistics.median(durations), 4),
            "min_duration_s": round(min(durations), 4),
            "max_duration_s": round(max(durations), 4),
            "median_peak_ram_mb": round(statistics.median(rams), 2),
            "all_sha_matches": all(x["sha_match"] for x in runs),
            "runs": runs
        }

    out_json = os.path.join(BENCH_DIR, "large_file_benchmark_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nLarge-file benchmark results saved to {out_json}")

if __name__ == "__main__":
    srv_thread = threading.Thread(target=range_server.run, daemon=True)
    srv_thread.start()
    time.sleep(0.5)
    run_large_benchmarks()
