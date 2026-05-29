#!/usr/bin/env python3
"""Control + static server for the M.4 T1.7 stage-3 live Shaka MMTP E2E.

Serves the shaka-player repo (so uncompiled Shaka + the harness page load
same-origin) and three control endpoints the harness page drives:

  GET  /__fingerprint  -> relay cert SHA-256 hex, read from E2E_FINGERPRINT
                          (the orchestration computes it locally from the cert
                          PEM via openssl — no TLS connection, no MITM surface).
  GET  /__replay       -> replay the captured MMTP packets once, as UDP unicast,
                          into the publisher's listener; returns when done.
  POST /__result       -> persist the harness's JSON result to RESULT_FILE.

Everything is same-origin, so the page needs no CORS and never fetches the
self-signed relay cert over plain TLS (the WebTransport leg pins it via
serverCertificateHashes from /__fingerprint).

Env:
  E2E_REPO_ROOT   shaka-player repo root to serve statically (required)
  E2E_CAPTURE     path to the capture JSON ({"packets_hex":[...]}) (required)
  E2E_RESULT      path to write the POSTed result JSON (required)
  E2E_FINGERPRINT path to a file holding the relay cert SHA-256 hex (required)
  E2E_PUB_HOST    publisher UDP host (default 127.0.0.1)
  E2E_PUB_PORT    publisher UDP port (default 5004)
  E2E_PORT        port to listen on (default 8097)
  E2E_REPLAY_DELAY_MS  per-packet pacing in ms (default 3)
"""
import http.server
import json
import os
import socket
import sys
import time

REPO_ROOT = os.environ["E2E_REPO_ROOT"]
CAPTURE = os.environ["E2E_CAPTURE"]
RESULT = os.environ["E2E_RESULT"]
FINGERPRINT_FILE = os.environ["E2E_FINGERPRINT"]
PUB_HOST = os.environ.get("E2E_PUB_HOST", "127.0.0.1")
PUB_PORT = int(os.environ.get("E2E_PUB_PORT", "5004"))
PORT = int(os.environ.get("E2E_PORT", "8097"))
REPLAY_DELAY = float(os.environ.get("E2E_REPLAY_DELAY_MS", "3")) / 1000.0


def read_fingerprint():
    with open(FINGERPRINT_FILE) as f:
        return f.read().strip()


def replay_capture():
    cap = json.load(open(CAPTURE))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    n = 0
    for h in cap["packets_hex"]:
        s.sendto(bytes.fromhex(h), (PUB_HOST, PUB_PORT))
        n += 1
        time.sleep(REPLAY_DELAY)
    s.close()
    return n


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=REPO_ROOT, **kw)

    def log_message(self, fmt, *args):
        # Quiet static-file noise; keep control-endpoint lines.
        if any(p in self.path for p in ("/__fingerprint", "/__replay", "/__result")):
            sys.stderr.write("[serve] %s %s\n" % (self.command, self.path))

    def _send(self, code, body=b"", ctype="text/plain"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        # The Karma vehicle's test page runs on a different origin (Karma's
        # server) and fetches /__fingerprint + /__replay cross-origin, so the
        # control responses must be CORS-readable. Harmless for the same-origin
        # standalone harness.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):
        if self.path == "/__fingerprint":
            try:
                fp = read_fingerprint()
                self._send(200, fp.encode("ascii"))
            except Exception as e:  # noqa: BLE001
                self._send(500, ("fingerprint read failed: %s" % e).encode())
            return
        if self.path == "/__replay":
            try:
                n = replay_capture()
                sys.stderr.write("[serve] replayed %d packets -> %s:%d\n"
                                 % (n, PUB_HOST, PUB_PORT))
                self._send(200, ("replayed %d" % n).encode())
            except Exception as e:  # noqa: BLE001
                self._send(500, ("replay failed: %s" % e).encode())
            return
        super().do_GET()

    def do_POST(self):
        if self.path == "/__result":
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            with open(RESULT, "wb") as f:
                f.write(body)
            sys.stderr.write("[serve] wrote result (%d bytes) -> %s\n"
                             % (len(body), RESULT))
            self._send(200, b"ok")
            return
        self._send(404, b"not found")


if __name__ == "__main__":
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    sys.stderr.write("[serve] root=%s port=%d capture=%s\n"
                     % (REPO_ROOT, PORT, CAPTURE))
    httpd.serve_forever()
