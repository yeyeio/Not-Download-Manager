#!/usr/bin/env python3
"""
Test Harness for Advanced Segmentation & Network Impairment Evaluation.
Measures:
1. Normal workers across tiers.
2. One deliberately slow worker (Head-of-line segment blocking test).
3. One worker disconnected mid-download (Failure / retry test).
4. HTTP 429/503 response handling.
"""

import os
import sys
import time
import json
import hashlib
import subprocess
import threading

BENCH_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BENCH_DIR)
import advanced_server

PORT = 8089
HOST = "127.0.0.1"
BASE_URL = f"http://{HOST}:{PORT}"
RUST_BIN = os.path.abspath(os.path.join(BENCH_DIR, "..", "target", "release", "bench_engine.exe"))
DOWNLOADS_DIR = os.path.join(BENCH_DIR, "downloads")

def calculate_sha256(filepath):
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def run_test():
    target_file = "bench_100mb.bin"
    with open(os.path.join(BENCH_DIR, "checksums.json"), "r") as f:
        meta = json.load(f)[target_file]
    expected_size = meta["size"]
    expected_sha = meta["sha256"]

    results = {}
    print("==================================================")
    print("ADVANCED SEGMENTATION & IMPAIRMENT BENCHMARK")
    print("==================================================")

    # Test 1: Uniform Baseline
    print("\n--- TEST 1: Uniform Baseline (1, 4, 8, 16, 32 workers) ---")
    results["uniform"] = {}
    for conn in [1, 4, 8, 16, 32]:
        out_path = os.path.join(DOWNLOADS_DIR, f"uniform_{conn}.bin")
        if os.path.exists(out_path):
            os.remove(out_path)
        url = f"{BASE_URL}/{target_file}"
        t0 = time.perf_counter()
        proc = subprocess.run([RUST_BIN, url, out_path, str(conn)], capture_output=True, text=True)
        dur = time.perf_counter() - t0
        actual_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
        sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"
        mb_s = (actual_size / (1024 * 1024)) / dur if dur > 0 else 0
        results["uniform"][str(conn)] = {
            "success": proc.returncode == 0 and sha == expected_sha,
            "duration": round(dur, 4),
            "mb_s": round(mb_s, 2),
            "sha_match": sha == expected_sha
        }
        print(f"  {conn} workers: {mb_s:.2f} MB/s ({dur:.3f}s) | Success: {sha == expected_sha}")

    # Test 2: One Deliberately Slow Worker (Testing Dynamic Splitting / Work Stealing)
    # With 4 workers on 100MB, segment 1 starts at 26214400. We throttle it to 5000 KB/s (~5 MB/s).
    # Expected behavior:
    # If DYNAMIC SPLITTING / WORK STEALING exists:
    #   Workers 0, 2, 3 finish in ~0.04s, then split worker 1's remaining 25MB into 4 sub-slices.
    #   Download finishes in ~1.5s - 2.0s.
    # If STATIC PARTITIONING (NO work stealing):
    #   Workers 0, 2, 3 finish and terminate immediately.
    #   Worker 1 downloads its 25MB alone at 5 MB/s. Total time = ~5.0 seconds!
    print("\n--- TEST 2: One Deliberately Slow Worker (Head-of-Line Blocking Test) ---")
    chunk_size = expected_size // 4
    slow_start = chunk_size  # Segment 1: bytes 26214400..52428799
    slow_url = f"{BASE_URL}/{target_file}?slow_start={slow_start}&slow_kb=5000"
    out_path = os.path.join(DOWNLOADS_DIR, "slow_worker_test.bin")
    if os.path.exists(out_path):
        os.remove(out_path)

    t0 = time.perf_counter()
    proc = subprocess.run([RUST_BIN, slow_url, out_path, "4"], capture_output=True, text=True)
    dur = time.perf_counter() - t0
    actual_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"
    mb_s = (actual_size / (1024 * 1024)) / dur if dur > 0 else 0

    results["slow_worker"] = {
        "success": proc.returncode == 0 and sha == expected_sha,
        "duration": round(dur, 4),
        "mb_s": round(mb_s, 2),
        "sha_match": sha == expected_sha,
        "analysis": "Static segmentation blocked by slowest worker" if dur > 4.5 else "Dynamic work stealing active"
    }
    print(f"  Result: {dur:.3f}s ({mb_s:.2f} MB/s) | SHA-256 Match: {sha == expected_sha}")
    print(f"  Diagnosis: {results['slow_worker']['analysis']}")

    # Test 3: One Worker Disconnected Mid-Download (TCP Reset)
    print("\n--- TEST 3: One Worker Disconnected Mid-Download ---")
    advanced_server.clear_impairments()
    drop_url = f"{BASE_URL}/{target_file}?drop_start={slow_start}&drop_after=1048576"
    out_path = os.path.join(DOWNLOADS_DIR, "drop_worker_test.bin")
    if os.path.exists(out_path):
        os.remove(out_path)

    t0 = time.perf_counter()
    proc = subprocess.run([RUST_BIN, drop_url, out_path, "4"], capture_output=True, text=True)
    dur = time.perf_counter() - t0
    actual_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"

    results["drop_worker"] = {
        "return_code": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "duration": round(dur, 4),
        "survived": proc.returncode == 0 and sha == expected_sha,
        "sha_match": sha == expected_sha
    }
    print(f"  Return code: {proc.returncode} | Survived: {proc.returncode == 0} | SHA Match: {sha == expected_sha}")
    print(f"  Stderr output: {proc.stderr.strip()}")

    # Test 4: HTTP 503 / 429 Error on One Segment
    print("\n--- TEST 4: HTTP 503 / 429 on One Segment ---")
    advanced_server.clear_impairments()
    err_url = f"{BASE_URL}/{target_file}?fail_start={slow_start}"
    out_path = os.path.join(DOWNLOADS_DIR, "err_worker_test.bin")
    if os.path.exists(out_path):
        os.remove(out_path)

    t0 = time.perf_counter()
    proc = subprocess.run([RUST_BIN, err_url, out_path, "4"], capture_output=True, text=True)
    dur = time.perf_counter() - t0
    actual_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"

    results["error_response"] = {
        "return_code": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "duration": round(dur, 4),
        "survived": proc.returncode == 0 and sha == expected_sha,
        "sha_match": sha == expected_sha
    }
    print(f"  Return code: {proc.returncode} | Survived: {proc.returncode == 0} | SHA Match: {sha == expected_sha}")
    print(f"  Stderr output: {proc.stderr.strip()}")

    # Save results
    out_json = os.path.join(BENCH_DIR, "advanced_segmentation_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nAdvanced segmentation results saved to {out_json}")

if __name__ == "__main__":
    srv_thread = threading.Thread(target=advanced_server.run, daemon=True)
    srv_thread.start()
    time.sleep(0.5)
    run_test()
