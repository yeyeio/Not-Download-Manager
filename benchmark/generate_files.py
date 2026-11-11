#!/usr/bin/env python3
"""
Generate deterministic test files and record their SHA-256 hashes.
"""

import os
import hashlib
import json

DIR = os.path.abspath(os.path.dirname(__file__))

FILES = {
    "bench_100mb.bin": 100 * 1024 * 1024,      # 104,857,600 bytes
    "bench_1gb.bin": 1 * 1024 * 1024 * 1024,    # 1,073,741,824 bytes
    "bench_5gb.bin": 5 * 1024 * 1024 * 1024,    # 5,368,709,120 bytes
}

def generate_file(filename, size_bytes):
    path = os.path.join(DIR, filename)
    if os.path.isfile(path) and os.path.getsize(path) == size_bytes:
        print(f"File {filename} already exists with correct size ({size_bytes} bytes).")
        # calculate sha256
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                sha.update(chunk)
        return sha.hexdigest()

    print(f"Generating {filename} ({size_bytes} bytes)...")
    sha = hashlib.sha256()
    # Use deterministic pattern of 1MB blocks
    pattern = (b"IDMAN_BENCHMARK_BLOCK_" * 40960)[:1024 * 1024] # 1MB block
    written = 0
    with open(path, "wb") as f:
        while written < size_bytes:
            to_write = min(len(pattern), size_bytes - written)
            chunk = pattern[:to_write]
            f.write(chunk)
            sha.update(chunk)
            written += to_write

    digest = sha.hexdigest()
    print(f"Done: {filename} -> SHA256: {digest}")
    return digest

def main():
    hashes = {}
    for name, size in FILES.items():
        hashes[name] = {
            "size": size,
            "sha256": generate_file(name, size)
        }
    with open(os.path.join(DIR, "checksums.json"), "w") as f:
        json.dump(hashes, f, indent=2)
    print("Checksums recorded in checksums.json")

if __name__ == "__main__":
    main()
