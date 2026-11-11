#!/usr/bin/env python3
"""
High-Performance Multi-Threaded HTTP Server with full HTTP Range (RFC 7233) support.
Logs every request, Range header, and connection for benchmark auditing.
"""

import os
import re
import sys
import time
import socket
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

PORT = 8088
HOST = "127.0.0.1"
SERVE_DIR = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(SERVE_DIR, "server.log"), mode="w", encoding="utf-8")
    ]
)

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

class RangeRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        logging.info("%s - %s", self.client_address[0], format % args)

    def do_HEAD(self):
        file_path, file_size = self._resolve_file()
        if not file_path:
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", f'"{file_size}-{int(os.path.getmtime(file_path))}"')
        self.send_header("Connection", "keep-alive")
        self.end_headers()

    def do_GET(self):
        file_path, file_size = self._resolve_file()
        if not file_path:
            return

        range_header = self.headers.get("Range")

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

                self.send_response(206, "Partial Content")
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
                self.send_header("Content-Length", str(length))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Connection", "keep-alive")
                self.end_headers()

                self._stream_file_slice(file_path, start, length)
                return

        logging.info("--> FULL GET REQUEST: 0-%d (no Range header)", file_size - 1)
        self.send_response(200, "OK")
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        self._stream_file_slice(file_path, 0, file_size)

    def _resolve_file(self):
        rel_path = self.path.split("?")[0].lstrip("/")
        file_path = os.path.join(SERVE_DIR, rel_path)
        if not os.path.isfile(file_path):
            self.send_error(404, f"File not found: {rel_path}")
            return None, 0
        return file_path, os.path.getsize(file_path)

    def _stream_file_slice(self, file_path, offset, length):
        chunk_size = 64 * 1024  # 64 KB chunk
        bytes_sent = 0
        try:
            with open(file_path, "rb") as f:
                f.seek(offset)
                while bytes_sent < length:
                    to_read = min(chunk_size, length - bytes_sent)
                    data = f.read(to_read)
                    if not data:
                        break
                    self.wfile.write(data)
                    bytes_sent += len(data)
        except (ConnectionResetError, BrokenPipeError):
            logging.warning("Client disconnected prematurely after %d bytes", bytes_sent)

def run():
    server = ThreadedHTTPServer((HOST, PORT), RangeRequestHandler)
    logging.info("Serving files from %s on http://%s:%d", SERVE_DIR, HOST, PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    run()
