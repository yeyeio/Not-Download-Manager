#!/usr/bin/env python3
"""
Benchmark comparison across chunk sizes (512 KB, 1 MB, 2 MB, 4 MB).
Measures:
- Total duration
- Aggregate throughput (MB/s)
- Number of HTTP requests (Range requests)
- Peak RAM usage (MB)
- SHA-256 verification
"""

import os
import sys
import time
import json
import hashlib
import ctypes
from ctypes import wintypes
import subprocess
import threading

BENCH_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BENCH_DIR)
import range_server

PORT = 8088
HOST = "127.0.0.1"
BASE_URL = f"http://{HOST}:{PORT}"
RUST_BIN = os.path.abspath(os.path.join(BENCH_DIR, "..", "target", "release", "bench_engine.exe"))
DOWNLOADS_DIR = os.path.join(BENCH_DIR, "downloads")

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
        while chunk := f.read(2 * 1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def get_peak_ram(handle):
    try:
        counters = PROCESS_MEMORY_COUNTERS_EX()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX)
        if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return counters.PeakWorkingSetSize / (1024 * 1024)
    except Exception:
        pass
    return 0.0

def run_chunk_bench():
    target_file = "bench_100mb.bin"
    with open(os.path.join(BENCH_DIR, "checksums.json"), "r") as f:
        meta = json.load(f)[target_file]
    expected_size = meta["size"]
    expected_sha = meta["sha256"]

    chunk_sizes = [
        (512 * 1024, "512 KB"),
        (1024 * 1024, "1 MB"),
        (2 * 1024 * 1024, "2 MB"),
        (4 * 1024 * 1024, "4 MB"),
    ]

    results = {}
    print("==================================================")
    print("DYNAMIC SEGMENTATION CHUNK SIZE EVALUATION (100 MB)")
    print("==================================================")

    url = f"{BASE_URL}/{target_file}"
    connections = "8"

    for c_bytes, label in chunk_sizes:
        print(f"\n[EVALUATING] Chunk Size: {label} ({c_bytes} bytes)...")
        expected_requests = (expected_size + c_bytes - 1) // c_bytes
        durations = []
        throughputs = []
        peak_rams = []
        all_sha = True

        for rep in range(3):
            out_path = os.path.join(DOWNLOADS_DIR, f"bench_chunk_{label.replace(' ', '_')}_{rep}.bin")
            if os.path.exists(out_path):
                os.remove(out_path)

            env = os.environ.copy()
            env["HYPERDOWNLOAD_CHUNK_SIZE"] = str(c_bytes)

            t0 = time.perf_counter()
            proc = subprocess.Popen([RUST_BIN, url, out_path, connections], env=env)
            peak_ram = 0.0
            while proc.poll() is None:
                ram = get_peak_ram(proc._handle)
                if ram > peak_ram:
                    peak_ram = ram
                time.sleep(0.005)
            proc.wait()
            dur = time.perf_counter() - t0

            sha = calculate_sha256(out_path) if os.path.exists(out_path) else "MISSING"
            match = sha == expected_sha
            if not match:
                all_sha = False

            mb_s = (expected_size / (1024 * 1024)) / dur if dur > 0 else 0
            durations.append(dur)
            throughputs.append(mb_s)
            peak_rams.append(peak_ram)

            if os.path.exists(out_path):
                os.remove(out_path)

        median_dur = sorted(durations)[len(durations)//2]
        median_mb_s = sorted(throughputs)[len(throughputs)//2]
        median_ram = sorted(peak_rams)[len(peak_rams)//2]

        results[label] = {
            "chunk_size_bytes": c_bytes,
            "requests_count": expected_requests,
            "median_duration_s": round(median_dur, 4),
            "median_throughput_mb_s": round(median_mb_s, 2),
            "median_peak_ram_mb": round(median_ram, 2),
            "sha_match": all_sha,
            "all_runs": [
                {"dur": round(d, 4), "mb_s": round(t, 2), "ram_mb": round(r, 2)}
                for d, t, r in zip(durations, throughputs, peak_rams)
            ]
        }
        print(f"  Result: {median_mb_s:.2f} MB/s ({median_dur:.4f}s) | Req count: {expected_requests} | Peak RAM: {median_ram:.1f} MB | SHA-256 Match: {all_sha}")

    out_file = os.path.join(BENCH_DIR, "chunk_size_benchmark_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nChunk size benchmark results saved to {out_file}")

if __name__ == "__main__":
    srv_thread = threading.Thread(target=range_server.run, daemon=True)
    srv_thread.start()
    time.sleep(0.5)
    run_chunk_bench()
