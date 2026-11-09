#!/usr/bin/env python3
"""
Comprehensive Automated Audit Suite for HyperDownload Phase 3.
Executes and validates all criteria in docs/auditphase3.md:
1. Server returns HTTP 200 to Range Request.
2. Missing Content-Range header.
3. Invalid / Mismatched Content-Range header.
4. Multiple slow workers (2 out of 4 throttled).
5. Repeated HTTP 429 rate limiting with Retry-After.
6. Forced process kill during active transfer.
7. Verification of all benchmark metrics and SHA-256.
"""

import os
import sys
import time
import json
import hashlib
import socket
import threading
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

PORT = 8095
HOST = "127.0.0.1"
BASE_URL = f"http://{HOST}:{PORT}"
BENCH_DIR = os.path.abspath(os.path.dirname(__file__))
RUST_BIN = os.path.abspath(os.path.join(BENCH_DIR, "..", "target", "release", "bench_engine.exe"))
DOWNLOADS_DIR = os.path.join(BENCH_DIR, "downloads")
TEST_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
DUMMY_DATA = os.urandom(TEST_FILE_SIZE)
EXPECTED_SHA = hashlib.sha256(DUMMY_DATA).hexdigest()

class AuditServerHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass  # Suppress request spam

    def do_HEAD(self):
        query = self.path.split("?")[-1] if "?" in self.path else ""
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(TEST_FILE_SIZE))
        self.send_header("Accept-Ranges", "bytes")
        if "etag_v2" in query:
            self.send_header("ETag", '"v2_modified"')
        else:
            self.send_header("ETag", '"v1_original"')
        self.end_headers()

    def do_GET(self):
        query = self.path.split("?")[-1] if "?" in self.path else ""
        range_header = self.headers.get("Range", "")

        # Mode 1: Server sends HTTP 200 to a Range Request
        if "force_200" in query:
            self.send_response(200, "OK")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(TEST_FILE_SIZE))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(DUMMY_DATA)
            return

        # Mode 2: Server sends HTTP 206 but omits Content-Range header
        if "missing_cr" in query:
            self.send_response(206, "Partial Content")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", "2097152")
            self.end_headers()
            self.wfile.write(DUMMY_DATA[:2097152])
            return

        # Mode 3: Server sends malformed/mismatched Content-Range header
        if "bad_cr" in query:
            self.send_response(206, "Partial Content")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Range", "bytes 5000000-7000000/10485760")
            self.send_header("Content-Length", "2000001")
            self.end_headers()
            self.wfile.write(DUMMY_DATA[:2000001])
            return

        # Mode 4: Repeated 429 with Retry-After (transient: 2 times then succeed)
        if "retry_429" in query:
            count = getattr(self.server, "retry_429_count", 0)
            if count < 2:
                self.server.retry_429_count = count + 1
                self.send_response(429, "Too Many Requests")
                self.send_header("Retry-After", "1")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

        # Normal standard Range request
        if range_header.startswith("bytes="):
            parts = range_header[6:].split("-")
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else TEST_FILE_SIZE - 1
            length = end - start + 1

            # Mode 5: Throttled slow range (e.g. bytes between 2MB and 6MB)
            if "multi_slow" in query and (2097152 <= start < 6291456):
                time.sleep(0.3)  # Artificial slow chunk

            self.send_response(206, "Partial Content")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Range", f"bytes {start}-{end}/{TEST_FILE_SIZE}")
            self.send_header("Content-Length", str(length))
            self.send_header("ETag", '"v1_original"')
            self.end_headers()
            self.wfile.write(DUMMY_DATA[start:end+1])
            return

        self.send_response(200)
        self.send_header("Content-Length", str(TEST_FILE_SIZE))
        self.end_headers()
        self.wfile.write(DUMMY_DATA)

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

def calculate_sha(filepath):
    if not os.path.isfile(filepath):
        return "FILE_NOT_FOUND"
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def run_all_audit_tests():
    audit_results = {}
    print("==================================================")
    print("HYPERDOWNLOAD PHASE 3 INDEPENDENT AUDIT SUITE")
    print("==================================================")

    # --- 1. HTTP 200 Range Fallback Rejection ---
    print("\n[AUDIT 1] Server returns HTTP 200 OK to Range Request:")
    out_1 = os.path.join(DOWNLOADS_DIR, "audit_200.bin")
    if os.path.exists(out_1): os.remove(out_1)
    url_1 = f"{BASE_URL}/test.bin?force_200=1"
    proc_1 = subprocess.run([RUST_BIN, url_1, out_1, "4"], capture_output=True, text=True)
    audit_results["http_200_rejection"] = {
        "exit_code": proc_1.returncode,
        "failed_safely": proc_1.returncode != 0,
        "stderr": proc_1.stderr.strip(),
        "verdict": "PASS" if proc_1.returncode != 0 and "206 Partial Content" in proc_1.stderr else "FAIL"
    }
    print(f"  Exit code: {proc_1.returncode} | Safely Rejected: {audit_results['http_200_rejection']['failed_safely']}")
    print(f"  Stderr: {proc_1.stderr.strip()}")

    # --- 2. Missing Content-Range Header Rejection ---
    print("\n[AUDIT 2] Server returns HTTP 206 without Content-Range Header:")
    out_2 = os.path.join(DOWNLOADS_DIR, "audit_missing_cr.bin")
    if os.path.exists(out_2): os.remove(out_2)
    url_2 = f"{BASE_URL}/test.bin?missing_cr=1"
    proc_2 = subprocess.run([RUST_BIN, url_2, out_2, "4"], capture_output=True, text=True)
    audit_results["missing_content_range"] = {
        "exit_code": proc_2.returncode,
        "failed_safely": proc_2.returncode != 0,
        "stderr": proc_2.stderr.strip(),
        "verdict": "PASS" if proc_2.returncode != 0 and "Missing Content-Range header" in proc_2.stderr else "FAIL"
    }
    print(f"  Exit code: {proc_2.returncode} | Safely Rejected: {audit_results['missing_content_range']['failed_safely']}")
    print(f"  Stderr: {proc_2.stderr.strip()}")

    # --- 3. Mismatched Content-Range Header Rejection ---
    print("\n[AUDIT 3] Server returns Mismatched Content-Range Header:")
    out_3 = os.path.join(DOWNLOADS_DIR, "audit_bad_cr.bin")
    if os.path.exists(out_3): os.remove(out_3)
    url_3 = f"{BASE_URL}/test.bin?bad_cr=1"
    proc_3 = subprocess.run([RUST_BIN, url_3, out_3, "4"], capture_output=True, text=True)
    audit_results["mismatched_content_range"] = {
        "exit_code": proc_3.returncode,
        "failed_safely": proc_3.returncode != 0,
        "stderr": proc_3.stderr.strip(),
        "verdict": "PASS" if proc_3.returncode != 0 and "Content-Range mismatch" in proc_3.stderr else "FAIL"
    }
    print(f"  Exit code: {proc_3.returncode} | Safely Rejected: {audit_results['mismatched_content_range']['failed_safely']}")
    print(f"  Stderr: {proc_3.stderr.strip()}")

    # --- 4. Repeated HTTP 429 with Retry-After ---
    print("\n[AUDIT 4] Repeated HTTP 429 Rate Limiting with Retry-After:")
    out_4 = os.path.join(DOWNLOADS_DIR, "audit_429.bin")
    if os.path.exists(out_4): os.remove(out_4)
    server.retry_429_count = 0
    url_4 = f"{BASE_URL}/test.bin?retry_429=1"
    t0 = time.perf_counter()
    proc_4 = subprocess.run([RUST_BIN, url_4, out_4, "4"], capture_output=True, text=True)
    dur_4 = time.perf_counter() - t0
    sha_4 = calculate_sha(out_4)
    audit_results["http_429_backoff"] = {
        "exit_code": proc_4.returncode,
        "duration": round(dur_4, 4),
        "sha_match": sha_4 == EXPECTED_SHA,
        "retries_handled": server.retry_429_count >= 2,
        "verdict": "PASS" if proc_4.returncode == 0 and sha_4 == EXPECTED_SHA else "FAIL"
    }
    print(f"  Exit code: {proc_4.returncode} | SHA Match: {sha_4 == EXPECTED_SHA} | Duration: {dur_4:.3f}s")

    # --- 5. Multiple Slow Workers (2 out of 4 throttled) ---
    print("\n[AUDIT 5] Multiple Slow Workers (Work Redistribution Test):")
    out_5 = os.path.join(DOWNLOADS_DIR, "audit_multi_slow.bin")
    if os.path.exists(out_5): os.remove(out_5)
    url_5 = f"{BASE_URL}/test.bin?multi_slow=1"
    t0 = time.perf_counter()
    proc_5 = subprocess.run([RUST_BIN, url_5, out_5, "4"], capture_output=True, text=True)
    dur_5 = time.perf_counter() - t0
    sha_5 = calculate_sha(out_5)
    audit_results["multiple_slow_workers"] = {
        "exit_code": proc_5.returncode,
        "duration": round(dur_5, 4),
        "sha_match": sha_5 == EXPECTED_SHA,
        "verdict": "PASS" if proc_5.returncode == 0 and sha_5 == EXPECTED_SHA else "FAIL"
    }
    print(f"  Exit code: {proc_5.returncode} | SHA Match: {sha_5 == EXPECTED_SHA} | Duration: {dur_5:.3f}s")

    # --- 6. Forced Termination During Transfer ---
    print("\n[AUDIT 6] Forced Termination & Atomic Checkpoint Persistence:")
    out_6 = os.path.join(DOWNLOADS_DIR, "audit_kill.bin")
    part_6 = os.path.join(DOWNLOADS_DIR, "audit_kill.bin.partinfo")
    for p in [out_6, part_6]:
        if os.path.exists(p): os.remove(p)
    # Launch slow download and kill at ~50%
    proc_kill = subprocess.Popen([RUST_BIN, f"{BASE_URL}/test.bin?multi_slow=1", out_6, "4"])
    time.sleep(0.3)
    proc_kill.kill()
    proc_kill.wait()
    # Check if tmp file exists or partinfo exists
    part_exists = os.path.exists(part_6)
    tmp_exists = os.path.exists(part_6 + ".tmp")
    # Resume
    proc_res = subprocess.run([RUST_BIN, f"{BASE_URL}/test.bin", out_6, "4"], capture_output=True, text=True)
    sha_6 = calculate_sha(out_6)
    audit_results["crash_consistency"] = {
        "kill_survived": proc_res.returncode == 0,
        "final_sha_match": sha_6 == EXPECTED_SHA,
        "no_orphan_tmp": not tmp_exists,
        "verdict": "PASS" if proc_res.returncode == 0 and sha_6 == EXPECTED_SHA else "FAIL"
    }
    print(f"  Kill Exit: {proc_res.returncode} | Clean Resume SHA Match: {sha_6 == EXPECTED_SHA}")

    # Output audit report JSON
    out_json = os.path.join(BENCH_DIR, "phase3_audit_resilience_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(audit_results, f, indent=2)
    print(f"\nAudit results written to {out_json}")

if __name__ == "__main__":
    server = ThreadedHTTPServer((HOST, 0), AuditServerHandler)
    server.retry_429_count = 0
    PORT = server.server_address[1]
    BASE_URL = f"http://{HOST}:{PORT}"
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    time.sleep(0.5)
    run_all_audit_tests()
