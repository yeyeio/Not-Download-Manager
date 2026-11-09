#!/usr/bin/env python3
"""
Reference Parallel Multi-Connection HTTP Range Downloader.
Used to benchmark true multi-connection HTTP range performance against the single-connection Rust prototype.
"""

import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

def download_range(url, start, end, out_path):
    req = urllib.request.Request(url)
    req.add_header("Range", f"bytes={start}-{end}")
    with urllib.request.urlopen(req) as resp:
        data = resp.read()
    with open(out_path, "r+b") as f:
        f.seek(start)
        f.write(data)
    return len(data)

def run_download(url, out_path, num_connections=8):
    # 1. Get file size via HEAD
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req) as resp:
        size = int(resp.headers.get("Content-Length", 0))

    if size == 0:
        raise ValueError("File size is 0 or Content-Length missing")

    # 2. Preallocate file
    with open(out_path, "wb") as f:
        f.seek(size - 1)
        f.write(b"\0")

    # 3. Partition segments
    chunk_size = size // num_connections
    segments = []
    for i in range(num_connections):
        start = i * chunk_size
        end = size - 1 if i == num_connections - 1 else (i + 1) * chunk_size - 1
        segments.append((start, end))

    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_connections) as pool:
        futures = [pool.submit(download_range, url, seg[0], seg[1], out_path) for seg in segments]
        total_downloaded = sum(f.result() for f in futures)
    duration = time.perf_counter() - start_time

    speed_mb_s = (total_downloaded / (1024 * 1024)) / duration if duration > 0 else 0
    return {
        "connections": num_connections,
        "size": total_downloaded,
        "duration": duration,
        "throughput_mb_s": speed_mb_s
    }

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: reference_downloader.py <url> <out_path> <conns>")
        sys.exit(1)
    res = run_download(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    print(f"REF_RESULT: conns={res['connections']} bytes={res['size']} duration={res['duration']:.4f} speed={res['throughput_mb_s']:.2f}MB/s")
