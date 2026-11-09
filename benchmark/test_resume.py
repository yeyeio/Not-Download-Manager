#!/usr/bin/env python3
import os
import sys
import time
import json
import hashlib
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
DOWNLOADS_DIR = os.path.join(BENCH_DIR, "downloads")

def calculate_sha256(filepath):
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def test_resume():
    target_file = "bench_100mb.bin"
    with open(os.path.join(BENCH_DIR, "checksums.json"), "r") as f:
        meta = json.load(f)[target_file]
    expected_size = meta["size"]
    expected_sha = meta["sha256"]
    url = f"{BASE_URL}/{target_file}"
    out_path = os.path.join(DOWNLOADS_DIR, "resume_test.bin")
    partinfo_path = os.path.join(DOWNLOADS_DIR, "resume_test.bin.partinfo")

    if os.path.exists(out_path):
        os.remove(out_path)
    if os.path.exists(partinfo_path):
        os.remove(partinfo_path)

    print("=== TEST RESUME & CHECKPOINTING ===")
    
    # 1. Download directly to test fresh start
    print("1. Running standard download with 8 connections...")
    cmd = [RUST_BIN, url, out_path, "8"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Download failed: {res.stderr}"
    assert os.path.getsize(out_path) == expected_size
    assert calculate_sha256(out_path) == expected_sha
    print("   Standard download passed! SHA256 matches perfectly.")

    # 2. Simulate partial download with checkpoint
    print("2. Simulating interrupted download with a .partinfo checkpoint...")
    os.remove(out_path)
    # Pre-create file with 50MB downloaded (first 4 segments complete, remaining 4 halfway)
    chunk_size = expected_size // 8
    # Read first 50MB from bench_100mb.bin
    src_path = os.path.join(BENCH_DIR, target_file)
    with open(src_path, "rb") as src, open(out_path, "wb") as dst:
        half_data = src.read(expected_size // 2)
        dst.write(half_data)
        # Pad to full size
        dst.seek(expected_size - 1)
        dst.write(b"\0")

    # Segments state: segments 0..3 complete, segments 4..7 at start
    segs = []
    for i in range(8):
        start = i * chunk_size
        end = expected_size if i == 7 else (i + 1) * chunk_size
        curr = end if i < 4 else start
        segs.append({
            "segment_index": i,
            "start_byte": start,
            "end_byte": end,
            "current_byte": curr
        })

    cp = {
        "total_size": expected_size,
        "segments": segs
    }
    with open(partinfo_path, "w", encoding="utf-8") as f:
        json.dump(cp, f)

    print(f"   Created mock checkpoint with 4/8 segments complete ({os.path.getsize(partinfo_path)} bytes).")

    # 3. Resume download
    print("3. Executing bench_engine on existing partial download...")
    t0 = time.perf_counter()
    res = subprocess.run(cmd, capture_output=True, text=True)
    t_resume = time.perf_counter() - t0

    assert res.returncode == 0, f"Resume failed: {res.stderr}"
    print(f"   Output: {res.stdout.strip()}")
    assert os.path.getsize(out_path) == expected_size, f"Size mismatch: {os.path.getsize(out_path)} vs {expected_size}"
    final_sha = calculate_sha256(out_path)
    assert final_sha == expected_sha, f"SHA mismatch after resume! {final_sha} != {expected_sha}"
    assert not os.path.exists(partinfo_path), "Partinfo checkpoint was not cleaned up!"

    print(f"   RESUME SUCCESS: File restored to 100% integrity in {t_resume:.4f}s with exact SHA-256 match!")
    print("=== RESUME VERIFICATION COMPLETED ===")

if __name__ == "__main__":
    srv_thread = threading.Thread(target=range_server.run, daemon=True)
    srv_thread.start()
    time.sleep(0.5)
    test_resume()
