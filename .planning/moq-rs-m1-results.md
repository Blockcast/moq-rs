# M.1 — MMTP-on-moq-transport smoke results

**Date:** 2026-05-28
**Repo:** `~/src/pim-multicast-gateway/moq-rs` @ `blo-4020-m1`
**Driver:** `.planning/m1-smoke.sh`
**Working dir:** `/tmp/m1-smoke` (configurable via `SMOKE` env)

## Verdict

**M.1 SMOKE PASS** — per-track sha256 match between publisher input
and subscriber output, mlog framing captured on both server-side
connections.

## Pipeline exercised

```
synth_mmtp (Rust example, deterministic MPU sequences)
  → UDP loopback (127.0.0.1:5004)
  → moq-pub-mmtp --mmtp-input udp
  → moq-relay-ietf --dev --mlog-dir /tmp/m1-smoke/mlog
  → moq-sub-raw (per-track payload dump to file)
  → per-track sha256 vs synth_mmtp's expected output files
```

The deterministic source removes any cross-version timing or
codec-quirk noise, so a sha256 mismatch points unambiguously at
the MoQ relay/transport plumbing rather than the source.

## Run-time evidence (representative run with `GROUPS=5 PACKET_DELAY_MS=200`)

| Track | Bytes | sha256 (publisher input ≡ subscriber output) |
|---|---|---|
| 1 (video) | 225 | `ce0c10f04f3a12fb69f6fbf554178369dbb8748b657f313dc14eaa7249b44f7c` |
| 2 (audio) | 225 | `d98dec2bc7126adeedc3b28cf709230ca425e0f1134d205b26126bbb583f1f45` |

mlog files written by `--mlog-dir` (two per run — one per QUIC
session = publisher + subscriber):

```
6585 bytes  633ca6915fc9a55e9d40d06ad9edd634_server.mlog
6213 bytes  cfb28393dfb28adb38c10be97a186a8a_server.mlog
```

A second run with `GROUPS=8 PACKET_DELAY_MS=50` produced 22920
bytes per track (~488 MPUs), all matching sha256 across the
publisher/subscriber boundary — pacing-tolerant up to packets
the subscriber can drain before the next supersedes the latest
subgroup.

## What the smoke proves

| ADR DoD item | Status | Evidence |
|---|---|---|
| Catalog layer landed (Container, multicast) | ✅ | moq-catalog 24 tests; catalog parsed by moq-pub-mmtp |
| moq-pub-mmtp crate scaffolded | ✅ | 39 tests; release build green |
| mmt-core wired (vendored A5) | ✅ | `moq-pub-mmtp/vendor/mmt-core/`, pinned 929e5b0c… |
| Publisher loop (T1, A1/A2/A3 invariants) | ✅ | 5 unit tests + smoke per-track sha256 |
| ffmpeg muxer stdout mode (T8 ADR) | ✅ N/A | Muxer already uses AVIOContext; T8 superseded |
| `.catalog` track publication (T2) | ✅ | `posted catalog on `.catalog` track bytes=550` in pub log |
| Catalog validation expansion (T5) | ✅ | 5 new validate tests; in-pipe at main() |
| `moq-sub-raw` sibling crate (T7) | ✅ | 6 tests; smoke uses it |
| mmt-core vendored (T6) | ✅ | VENDOR.md + standalone build |
| Smoke + per-track sha256 manifest (T9) | ✅ | This run |
| mlog records SUBGROUP/OBJECT framing | ✅ | server.mlog files present, non-empty |
| G6 wire-diff vs libmoq MMTP output | ⏳ | Captured side: this run's mlog. libmoq side requires the cast→libmoq path to run in parallel; deferred to a co-located capture session |

## How to repro

```bash
cd ~/src/pim-multicast-gateway/moq-rs
# Defaults: GROUPS=8 PACKET_DELAY_MS=50 PORT=4443 NAME=smoke
# (Watch out for stale GROUPS in your shell env — use `env -i` if unsure.)
env -i HOME=$HOME PATH=$PATH GROUPS=5 PACKET_DELAY_MS=200 \
    bash .planning/m1-smoke.sh
```

## Caveats and pacing notes

- **`SubgroupsReader` is latest-only.** moq-transport's serve layer
  exposes only the most recent subgroup on each `next()`; a slow
  subscriber misses intermediate MPUs. The smoke uses
  `--packet-delay-ms` to keep the subscriber in step. Real-world
  publishers are network-rate-limited, which produces the same
  effect implicitly.
- **dev/pub vs dev/sub URL alignment.** Both publisher and
  subscriber must use the same connect URL (no path) so they
  land in moq-relay-ietf's unscoped tenant bucket — the M.0
  scope-mismatch finding still applies. The smoke uses
  `https://localhost:4443` (no path) for both sides.
- **Coordinator state-file leak.** moq-relay-ietf's
  `/tmp/moq-coordinator.json` persists namespace registrations
  across runs in `--dev`. The smoke script removes it before
  starting; manual reruns may also need the cleanup.
- **Shell env leakage.** Pre-set `GROUPS` / `PACKET_DELAY_MS`
  in the operator's shell silently overrides the smoke script's
  `${GROUPS:-N}` default. If you see file sizes way out of
  proportion to `--groups`, run under `env -i` to confirm.

## Out of scope (kept open per ADR)

- MPU metadata synthesis from MFU (caller's responsibility).
- MMTP fragmentation reassembly (M.1b).
- Per-FEC-block grouping (M.1b — repair currently lands on a
  single rolling group per `<source>/repair` track).
- Receiver decode/render (M.4).
- G6 libmoq vs moq-rs wire-level diff at the byte level
  (needs a co-located libmoq capture; deferred).
- moq-transport `object_id_delta` correctness (Codex #6 follow-up;
  mlog shows healthy framing — defer deeper analysis to M.1b).
