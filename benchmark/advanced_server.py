#!/usr/bin/env python3
"""
Advanced Multi-Threaded HTTP Server for Network Impairment & Dynamic Segmentation Testing.
Supports:
- RFC 7233 Range Requests (bytes=start-end)
- Bandwidth throttling per connection
- Artificial latency
- Deliberate connection resets / drops
- HTTP 429 Too Many Requests & 503 Service Unavailable
- Variable segment response times
- Custom ETag / Last-Modified
"""

import os
import re
import sys
import time
import socket
import logging
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

PORT = 8089
HOST = "127.0.0.1"
SERVE_DIR = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(SERVE_DIR, "advanced_server.log"), mode="w", encoding="utf-8")
    ]
)

DROPPED_RANGES = set()
FAILED_RANGES = set()

def clear_impairments():
    DROPPED_RANGES.clear()
    FAILED_RANGES.clear()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

class ImpairmentRangeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        logging.info("%s - %s", self.client_address[0], format % args)

    def do_HEAD(self):
        file_path, file_size, query = self._resolve_file()
        if not file_path:
            return

        etag = query.get("etag", [f'"{file_size}-{int(os.path.getmtime(file_path))}"'])[0]

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", etag)
        self.send_header("Connection", "keep-alive")
        self.end_headers()

    def do_GET(self):
        file_path, file_size, query = self._resolve_file()
        if not file_path:
            return

        # Check for artificial server-wide error
        if "err503" in query:
            logging.info("--> RETURNING 503 Service Unavailable")
            self.send_response(503, "Service Unavailable")
            self.send_header("Retry-After", "2")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if "err429" in query:
            logging.info("--> RETURNING 429 Too Many Requests")
            self.send_response(429, "Too Many Requests")
            self.send_header("Retry-After", "2")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        # Artificial latency
        if "latency_ms" in query:
            delay = float(query["latency_ms"][0]) / 1000.0
            time.sleep(delay)

        range_header = self.headers.get("Range")
        etag = query.get("etag", [f'"{file_size}-{int(os.path.getmtime(file_path))}"'])[0]

        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header.strip())
            if match:
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else file_size - 1
                if start >= file_size or end >= file_size or start > end:
                    self.send_error(416, "Requested Range Not Satisfiable")
                    return

                length = end - start + 1
                logging.info("--> RANGE REQUEST: bytes %d-%d/%d (length: %d)", start, end, file_size, length)

                # Check if this range should return a transient error
                if "fail_start" in query:
                    fs = int(query["fail_start"][0])
                    if (start == fs or (start <= fs and end >= fs)) and fs not in FAILED_RANGES:
                        FAILED_RANGES.add(fs)
                        logging.info("--> SIMULATING 503 for range starting at %d (target %d)", start, fs)
                        self.send_response(503, "Service Unavailable")
                        self.send_header("Retry-After", "1")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return

                self.send_response(206, "Partial Content")
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
                self.send_header("Content-Length", str(length))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("ETag", etag)
                self.send_header("Connection", "keep-alive")
                self.end_headers()

                self._stream_with_impairments(file_path, start, length, query)
                return

        logging.info("--> FULL GET REQUEST: 0-%d", file_size - 1)
        self.send_response(200, "OK")
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", etag)
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        self._stream_with_impairments(file_path, 0, file_size, query)

    def _resolve_file(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        rel_path = parsed.path.lstrip("/")
        file_path = os.path.join(SERVE_DIR, rel_path)
        if not os.path.isfile(file_path):
            self.send_error(404, f"File not found: {rel_path}")
            return None, 0, {}
        return file_path, os.path.getsize(file_path), query

    def _stream_with_impairments(self, file_path, offset, length, query):
        chunk_size = 64 * 1024
        bytes_sent = 0

        # Check slow segment condition
        slow_start = int(query.get("slow_start", [-1])[0])
        slow_kb_s = float(query.get("slow_kb", [0])[0])
        drop_start = int(query.get("drop_start", [-1])[0])
        drop_after = int(query.get("drop_after", [2 * 1024 * 1024])[0])

        is_slow = (slow_start >= 0 and slow_kb_s > 0 and (
            offset == slow_start or 
            (offset >= slow_start and offset < slow_start + 25 * 1024 * 1024) or 
            (offset <= slow_start and offset + length > slow_start)
        ))
        is_drop = (drop_start >= 0 and drop_start not in DROPPED_RANGES and (
            offset == drop_start or (offset <= drop_start and offset + length > drop_start)
        ))

        with open(file_path, "rb") as f:
            f.seek(offset)
            while bytes_sent < length:
                to_read = min(chunk_size, length - bytes_sent)
                chunk = f.read(to_read)
                if not chunk:
                    break

                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except Exception as e:
                    logging.warning("Connection closed/broken: %s", e)
                    break

                bytes_sent += len(chunk)

                if is_drop and bytes_sent >= drop_after:
                    DROPPED_RANGES.add(drop_start)
                    logging.info("--> SIMULATING FORCIBLE DISCONNECT at %d bytes on segment start %d", bytes_sent, offset)
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                    except Exception:
                        pass
                    return

                if is_slow:
                    # throttle to slow_kb_s KB/s
                    # chunk is 64KB -> time required = 64KB / (slow_kb_s * 1024)
                    sleep_time = len(chunk) / (slow_kb_s * 1024.0)
                    time.sleep(sleep_time)

def run(port=PORT):
    server_address = (HOST, port)
    httpd = ThreadedHTTPServer(server_address, ImpairmentRangeHandler)
    logging.info("Serving files on http://%s:%d", HOST, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    run(p)
