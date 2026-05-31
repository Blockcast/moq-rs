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
import urllib.parse

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


def _ft_fi(h):
    """(fragment_type, fragmentation_indicator) from an MMTP packet hex.
    FT: 0=Init, 2=MFU. FI: 0=complete, 1=first, 2=middle, 3=last."""
    flags = bytes.fromhex(h)[14]
    return (flags >> 4) & 0xf, (flags >> 1) & 0x3


def _segment_units(packets):
    """Group packets into units: each non-MFU (Init) is its own unit; MFU
    fragments are coalesced into WHOLE frames (FI=0 alone, or FI=1..FI=3).
    Returns a list of (kind, [packets]) with kind 'init' or 'frame'.

    Frame-level (not fragment-level) is the correct granularity for loss/
    reorder: dropping or reordering a partial frame corrupts it (no FEC), which
    is unrecoverable and not what these flows test."""
    units, cur = [], []
    for h in packets:
        ft, fi = _ft_fi(h)
        if ft != 2:  # Init / other — flush any open frame, emit standalone
            if cur:
                units.append(("frame", cur))
                cur = []
            units.append(("init", [h]))
            continue
        cur.append(h)
        if fi in (0, 3):  # complete (0) or last fragment (3) → frame done
            units.append(("frame", cur))
            cur = []
    if cur:
        units.append(("frame", cur))
    return units


def replay_capture(start=0, drop=0, reorder=0):
    """Replay the captured packets as UDP, with optional adversarial shaping.

    start:   begin at packet index `start` (mid-GOP join; pre-RAP frames the
             RAP gate must drop, until the next re-sent Init MPU + RAP).
    drop:    drop every `drop`-th WHOLE frame (loss). Init MPUs are never
             dropped (the transmuxer needs one to seed).
    reorder: reverse WHOLE frames within each window of `reorder` frames
             (out-of-order delivery). Init MPUs anchor the windows.
    """
    cap = json.load(open(CAPTURE))
    packets = cap["packets_hex"][start:]

    if (drop and drop > 1) or (reorder and reorder > 1):
        units = _segment_units(packets)
        if drop and drop > 1:
            kept, frame_i = [], 0
            for kind, pkts in units:
                if kind == "frame":
                    frame_i += 1
                    if frame_i % drop == 0:
                        continue
                kept.append((kind, pkts))
            units = kept
        if reorder and reorder > 1:
            out, buf = [], []
            def flush():
                out.extend(reversed(buf))
                buf.clear()
            for kind, pkts in units:
                if kind == "frame":
                    buf.append((kind, pkts))
                    if len(buf) >= reorder:
                        flush()
                else:
                    flush()
                    out.append((kind, pkts))
            flush()
            units = out
        packets = [h for _, pkts in units for h in pkts]

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    n = 0
    for h in packets:
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
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/__replay":
            try:
                q = urllib.parse.parse_qs(parsed.query)
                start = int(q.get("start", ["0"])[0])
                drop = int(q.get("drop", ["0"])[0])
                reorder = int(q.get("reorder", ["0"])[0])
                n = replay_capture(start, drop, reorder)
                sys.stderr.write(
                    "[serve] replayed %d packets (start=%d drop=%d reorder=%d)"
                    " -> %s:%d\n" % (n, start, drop, reorder, PUB_HOST, PUB_PORT))
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
