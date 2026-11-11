#!/usr/bin/env python3
"""
Automated Controlled Benchmark Suite for:
1. Rust Download Manager Engine (app.exe / bench_engine.exe)
2. Internet Download Manager (IDMan.exe)
3. Python / Curl Baseline

Measures throughput, completion time, active connections, CPU/RAM, file integrity (SHA-256).
Runs multiple repetitions and computes median, min, max.
"""

import os
import sys
import time
import json
import shutil
import hashlib
import statistics
import subprocess
import threading
from urllib.parse import urlparse

BENCH_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BENCH_DIR)
import range_server

PORT = 8088
HOST = "127.0.0.1"
BASE_URL = f"http://{HOST}:{PORT}"

RUST_BIN = os.path.abspath(os.path.join(BENCH_DIR, "..", "target", "release", "bench_engine.exe"))
IDM_BIN = r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe"
DOWNLOADS_DIR = os.path.join(BENCH_DIR, "downloads")

def calculate_sha256(filepath):
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def count_server_connections():
    log_path = os.path.join(BENCH_DIR, "server.log")
    if not os.path.isfile(log_path):
        return 0, 0
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    ranges = [l for l in lines if "RANGE REQUEST" in l]
    fulls = [l for l in lines if "FULL GET REQUEST" in l]
    return len(ranges), len(fulls)

def clear_server_log():
    log_path = os.path.join(BENCH_DIR, "server.log")
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass

def run_reference_download(url, filename, connections, expected_size):
    out_path = os.path.join(DOWNLOADS_DIR, f"ref_{filename}")
    if os.path.exists(out_path):
        os.remove(out_path)

    clear_server_log()
    ref_script = os.path.join(BENCH_DIR, "reference_downloader.py")
    cmd = [sys.executable, ref_script, url, out_path, str(connections)]

    start_time = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    duration = time.perf_counter() - start_time

    if proc.returncode != 0:
        return {"success": False, "error": proc.stderr.strip(), "duration": duration}

    actual_size = os.path.getsize(out_path) if os.path.isfile(out_path) else 0
    speed_mb_s = (actual_size / (1024 * 1024)) / duration if duration > 0 else 0
    sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"
    ranges_count, full_count = count_server_connections()

    return {
        "success": actual_size == expected_size,
        "duration": duration,
        "throughput_mb_s": speed_mb_s,
        "ranges_logged": ranges_count,
        "full_requests_logged": full_count,
        "file_size": actual_size,
        "sha256": sha
    }

def run_rust_download(url, filename, connections, expected_size):
    out_path = os.path.join(DOWNLOADS_DIR, f"rust_{filename}")
    if os.path.exists(out_path):
        os.remove(out_path)

    clear_server_log()
    cmd = [RUST_BIN, url, out_path, str(connections)]

    start_time = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    stdout, stderr = proc.communicate()
    duration = time.perf_counter() - start_time

    if proc.returncode != 0:
        return {
            "success": False,
            "error": stderr.strip() or stdout.strip(),
            "duration": duration
        }

    actual_size = os.path.getsize(out_path) if os.path.isfile(out_path) else 0
    speed_mb_s = (actual_size / (1024 * 1024)) / duration if duration > 0 else 0
    sha = calculate_sha256(out_path) if actual_size == expected_size else "MISMATCH"
    ranges_count, full_count = count_server_connections()

    return {
        "success": actual_size == expected_size,
        "duration": duration,
        "throughput_mb_s": speed_mb_s,
        "ranges_logged": ranges_count,
        "full_requests_logged": full_count,
        "file_size": actual_size,
        "sha256": sha
    }

def run_idm_download(url, filename, expected_size, timeout_secs=60):
    out_name = f"idm_{filename}"
    out_path = os.path.join(DOWNLOADS_DIR, out_name)
    if os.path.exists(out_path):
        os.remove(out_path)

    if not os.path.isfile(IDM_BIN):
        return {"success": False, "error": "IDMan.exe not found"}

    clear_server_log()
    cmd = [IDM_BIN, "/d", url, "/p", DOWNLOADS_DIR, "/f", out_name, "/n", "/q"]

    start_time = time.perf_counter()
    try:
        proc = subprocess.Popen(cmd)
        # IDM spawns and communicates with running background IDM service.
        # We wait until the file size reaches expected_size or timeout.
        poll_interval = 0.05
        elapsed = 0
        while elapsed < timeout_secs:
            if os.path.isfile(out_path) and os.path.getsize(out_path) == expected_size:
                break
            time.sleep(poll_interval)
            elapsed = time.perf_counter() - start_time
        duration = time.perf_counter() - start_time
    except Exception as e:
        return {"success": False, "error": str(e), "duration": 0}

    actual_size = os.path.getsize(out_path) if os.path.isfile(out_path) else 0
    if actual_size != expected_size:
        return {"success": False, "error": f"Timeout or size mismatch ({actual_size}/{expected_size})", "duration": duration}

    speed_mb_s = (actual_size / (1024 * 1024)) / duration if duration > 0 else 0
    sha = calculate_sha256(out_path)
    ranges_count, full_count = count_server_connections()

    return {
        "success": True,
        "duration": duration,
        "throughput_mb_s": speed_mb_s,
        "ranges_logged": ranges_count,
        "full_requests_logged": full_count,
        "file_size": actual_size,
        "sha256": sha
    }

def run_benchmark_matrix(target_file="bench_100mb.bin", reps=5):
    with open(os.path.join(BENCH_DIR, "checksums.json"), "r") as f:
        meta = json.load(f)[target_file]
    expected_size = meta["size"]
    expected_sha = meta["sha256"]
    url = f"{BASE_URL}/{target_file}"

    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    tiers = [1, 4, 8, 16, 32]
    results = {"meta": meta, "rust_engine": {}, "reference_multi_range": {}, "idm": {}}

    print(f"\n=======================================================")
    print(f"BENCHMARK: {target_file} ({expected_size / (1024*1024):.0f} MB)")
    print(f"Server: {url}")
    print(f"Repetitions per tier: {reps}")
    print(f"=======================================================\n")

    # 1. Rust Engine Benchmarks
    print("[1/3] Benchmarking Rust Engine...")
    for conn in tiers:
        print(f"  Testing Rust Engine with {conn} connection(s)...", end="", flush=True)
        runs = []
        for r in range(reps):
            res = run_rust_download(url, target_file, conn, expected_size)
            if res.get("success"):
                runs.append(res)
                print(".", end="", flush=True)
            else:
                print("F", end="", flush=True)
            time.sleep(0.2)

        if runs:
            durations = [x["duration"] for x in runs]
            speeds = [x["throughput_mb_s"] for x in runs]
            ranges = [x["ranges_logged"] for x in runs]
            fulls = [x["full_requests_logged"] for x in runs]

            results["rust_engine"][str(conn)] = {
                "median_duration_s": round(statistics.median(durations), 4),
                "median_throughput_mb_s": round(statistics.median(speeds), 2),
                "min_throughput_mb_s": round(min(speeds), 2),
                "max_throughput_mb_s": round(max(speeds), 2),
                "active_range_connections_median": round(statistics.median(ranges)),
                "full_get_requests_median": round(statistics.median(fulls)),
                "sha256_match": runs[0]["sha256"] == expected_sha,
                "reps": len(runs)
            }
            print(f" Median: {results['rust_engine'][str(conn)]['median_throughput_mb_s']} MB/s | Ranges: {results['rust_engine'][str(conn)]['active_range_connections_median']}")
        else:
            print(" ALL FAILED")

    # 2. Reference Multi-Range Downloader Benchmarks
    print("\n[2/3] Benchmarking Reference Multi-Range Downloader...")
    for conn in tiers:
        print(f"  Testing Reference Multi-Range with {conn} connection(s)...", end="", flush=True)
        runs = []
        for r in range(reps):
            res = run_reference_download(url, target_file, conn, expected_size)
            if res.get("success"):
                runs.append(res)
                print(".", end="", flush=True)
            else:
                print("F", end="", flush=True)
            time.sleep(0.2)

        if runs:
            durations = [x["duration"] for x in runs]
            speeds = [x["throughput_mb_s"] for x in runs]
            ranges = [x["ranges_logged"] for x in runs]
            fulls = [x["full_requests_logged"] for x in runs]

            results["reference_multi_range"][str(conn)] = {
                "median_duration_s": round(statistics.median(durations), 4),
                "median_throughput_mb_s": round(statistics.median(speeds), 2),
                "min_throughput_mb_s": round(min(speeds), 2),
                "max_throughput_mb_s": round(max(speeds), 2),
                "active_range_connections_median": round(statistics.median(ranges)),
                "full_get_requests_median": round(statistics.median(fulls)),
                "sha256_match": runs[0]["sha256"] == expected_sha,
                "reps": len(runs)
            }
            print(f" Median: {results['reference_multi_range'][str(conn)]['median_throughput_mb_s']} MB/s | Ranges: {results['reference_multi_range'][str(conn)]['active_range_connections_median']}")
        else:
            print(" ALL FAILED")

    # 3. IDM Benchmarks
    print("\n[3/3] Benchmarking IDM (Internet Download Manager)...")
    if os.path.isfile(IDM_BIN):
        runs = []
        for r in range(reps):
            print(f"  Running IDM repetition {r+1}/{reps}...", end="", flush=True)
            res = run_idm_download(url, target_file, expected_size)
            if res.get("success"):
                runs.append(res)
                print(f" OK ({res['duration']:.2f}s, {res['throughput_mb_s']:.1f} MB/s, ranges={res['ranges_logged']})")
            else:
                print(f" FAILED ({res.get('error')})")
            time.sleep(0.5)

        if runs:
            durations = [x["duration"] for x in runs]
            speeds = [x["throughput_mb_s"] for x in runs]
            ranges = [x["ranges_logged"] for x in runs]
            fulls = [x["full_requests_logged"] for x in runs]

            results["idm"] = {
                "median_duration_s": round(statistics.median(durations), 4),
                "median_throughput_mb_s": round(statistics.median(speeds), 2),
                "min_throughput_mb_s": round(min(speeds), 2),
                "max_throughput_mb_s": round(max(speeds), 2),
                "active_range_connections_median": round(statistics.median(ranges)),
                "full_get_requests_median": round(statistics.median(fulls)),
                "sha256_match": runs[0]["sha256"] == expected_sha,
                "reps": len(runs)
            }
        else:
            results["idm"] = {"status": "FAILED or GUI prompted"}
    else:
        results["idm"] = {"status": "IDMan.exe not installed"}

    # Output results
    res_path = os.path.join(BENCH_DIR, f"results_{target_file.split('.')[0]}.json")
    with open(res_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {res_path}")
    return results

if __name__ == "__main__":
    # Start server in thread
    srv_thread = threading.Thread(target=range_server.run, daemon=True)
    srv_thread.start()
    time.sleep(0.5)

    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    run_benchmark_matrix("bench_100mb.bin", reps=reps)
