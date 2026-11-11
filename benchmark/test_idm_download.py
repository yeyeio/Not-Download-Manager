#!/usr/bin/env python3
import os
import sys
import time
import subprocess
import threading

BENCH_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BENCH_DIR)
import range_server

IDM_BIN = r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe"
DOWNLOADS_IDM = os.path.join(BENCH_DIR, "downloads_idm")
os.makedirs(DOWNLOADS_IDM, exist_ok=True)

# Start server
t = threading.Thread(target=range_server.run, daemon=True)
t.start()
time.sleep(0.5)

url = "http://127.0.0.1:8088/bench_100mb.bin"
out_name = "test_idm.bin"
out_path = os.path.join(DOWNLOADS_IDM, out_name)

print("Launching IDM...")
cmd = [IDM_BIN, "/d", url, "/p", DOWNLOADS_IDM, "/f", out_name, "/n", "/q"]
subprocess.Popen(cmd)

for i in range(20):
    time.sleep(0.5)
    if os.path.isfile(out_path):
        print(f"File found! Size: {os.path.getsize(out_path)} bytes")
        if os.path.getsize(out_path) == 104857600:
            print("Download completed successfully!")
            break
    else:
        # Check if anything else in directory
        items = os.listdir(DOWNLOADS_IDM)
        if items:
            print(f"Items in dir: {items}")
print("Done check.")
