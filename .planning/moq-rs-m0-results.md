# M.0 — moq-rs baseline test results

**Date:** 2026-05-28
**Repo:** `~/src/pim-multicast-gateway/moq-rs` @ `f9f51dc` (Cloudflare v0.7.17)
**Build:** `cargo build --release` → 2m 08s, 0 warnings, all 9 crates green.
**Cert:** `dev/cert` → `dev/localhost.{crt,key}` (valid until 2026-06-07).
**Logs:** `.planning/m0-logs/`

## Summary

| Test | Status | Notes |
| --- | --- | --- |
| T1 — clock pub/sub | **PASS** | Sub received 16-object subgroups, matched publisher emission |
| T2 — fMP4 (BBB) pub/sub with `--catalog` | **PASS** (after scope fix) | Catalog parsed, 32 groups received over 10s, both video + audio tracks |
| T3 — Cloudflare interop relay | Not run (skipped per plan; local pass sufficient) | — |
| T4 — moq-js browser client | Not run | Out of scope for M.0 |

## T1 — clock smoke (`moq-clock-ietf`)

Command:
```
RUST_LOG=info ./target/release/moq-clock-ietf --tls-disable-verify --publish https://localhost:4443
RUST_LOG=info ./target/release/moq-clock-ietf --tls-disable-verify          https://localhost:4443
```
Result: subscriber printed every tick that the publisher emitted (9 lines captured in 9s window). Final subgroup-complete log line shows `group_id=54, subgroup_id=0, 16 objects sent / 16 objects received`. SETUP + PUBLISH_NAMESPACE + SUBSCRIBE + group delivery all healthy.

## T2 — fMP4 pub/sub (`moq-pub` + `moq-sub`)

Source: Big Buck Bunny 320×180 H.264 + AAC, fragmented via `ffmpeg -movflags cmaf+separate_moof+delay_moov+skip_trailer+frag_every_frame`.

Catalog received by subscriber:
```
streamingFormat: 1, streamingFormatVersion: "0.2", packaging: cmaf
tracks:
  1.m4s  init=0.mp4  codec=avc1.42C00D  320×180
  2.m4s  init=0.mp4  codec=mp4a.40.2    48kHz stereo  bitrate=159997
```

Steady-state delivery (last second of 10s window): groups arriving at ~2 Hz (every 500ms), with two subgroups per group (24 + 12 objects ≈ video frames + audio frames). Sub exited cleanly via `timeout`.

### Scope-mismatch finding (dev-scripts disagree on tenant scope)

**Not a spec issue.** The MoQT draft-14 wire is fine; this is `moq-relay-ietf`'s Cloudflare-specific multi-tenant scope feature layered on top of the spec — WebTransport connect-URL path → tenant scope → per-scope namespace bucket with `ReadWrite` permissions. The dev scripts use different connect URLs for pub vs sub, which lands them in different scopes for a same-machine smoke test.

- `dev/pub`: connect URL = `https://localhost:4443` (no path) → scope = `<unscoped>`, registers namespace at `(UNSCOPED, /bbb)` in `moq-relay-ietf::Locals`.
- `dev/sub`: connect URL = `https://localhost:4443/$NAME` (path = `/bbb`) → scope = `/bbb`, looks up `(/bbb, /bbb)` in `Locals`. Miss.
- Producer-side `serve_subscribe()` falls through to `remotes.route()`, which queries the file coordinator, finds the announce registered at the same relay URL, attempts to "route to remote", and fails with `namespace not found`.
- Subscriber sees `failed to subscribe to catalog track: Closed(0)` → `Error: media error / closed, code=0`.

Reproduction with debug logs (`.planning/m0-logs/relay-debug.log`):
```
03:15:33.492467  DEBUG  consumer: namespace registered in locals namespace=/bbb
03:15:33.493026  INFO   file_coordinator: registering namespace: /bbb scope=  relay_url=https://[::]:4443/
03:15:37.516583  DEBUG  relay: scope resolved connection_path="/bbb" scope_id=/bbb permissions=ReadWrite
03:15:37.519524  DEBUG  file_coordinator: looking up namespace: /bbb scope=/bbb
03:15:37.519669  ERROR  producer: failed to route to remote: namespace not found
```

Fix used for T2: invoked the subscriber with `--name bbb https://localhost:4443` (no `/bbb` in URL). Both sides land in the unscoped bucket and the lookup succeeds.

Worth filing against `cloudflare/moq-rs` as a docs/dev-scripts nit — pub and sub should connect to the same URL in the smoke-test workflow. The relay behavior itself is intentional (multi-tenant scoping is a feature, not a bug).

### Secondary finding — file coordinator state leak

`moq-relay-ietf::file_coordinator` writes namespace registrations to `/tmp/moq-coordinator.json`. If the publisher dies abruptly (e.g. its broadcast was rejected and ffmpeg gets a broken pipe), the coordinator entry can persist for that PID/process, causing the next pub announce on the same namespace to be rejected with `duplicate`.

For local dev, this is annoying but recoverable: stop all moq-* processes, `rm /tmp/moq-coordinator.json`, restart. Not a blocker for production (the file coordinator is `--dev` mode; production uses Redis-backed `moq-api`).

## Other observations from this session

1. The `dev/relay` script uses `cargo run` (debug profile), which triggers a 60s+ recompile on first run even after `cargo build --release`. For repeat smoke runs, invoking `./target/release/moq-relay-ietf ...` directly is much faster.
2. `moq-pub` emits the catalog under the track name `.catalog` (literal, not under `commonTrackFields`); `moq-sub` requires `--catalog` to find it. Default subscriber behavior (without `--catalog`) looks for `0.mp4` and `{n}.m4s` directly. Two valid usage modes; not a bug, just a flag to remember.
3. The catalog uses `streamingFormat: 1` and `streamingFormatVersion: "0.2"`. Going to need to check whether that's `0.2` of moq-rs's catalog or `0.2` of an IETF draft — likely the former. (See `moq-catalog/src/lib.rs:18`.)

## What this confirms for the migration plan (BLO-4020)

- **M.0 baseline established.** IETF draft-14 stack works end-to-end on this checkout with the documented quirks above.
- **G2 (publisher container) is the main work.** Upstream `moq-pub` is firmly fMP4-coupled (`media.rs` is mp4-parser + cmaf-track creator); MMTP support is a new container module, not a small patch.
- **G1 (catalog `TrackPackaging::Mmtp` variant)** is the smallest first concrete change. One-line enum addition + serde rename + any consumers that match on the enum.
- **G6 (wire-format diff between libmoq draft-14 and moq-rs draft-14)** remains open — need to capture a cast-bridge wire trace and diff against today's `moq-pub` wire trace. Pending.
- **M.4 is "replace, not extend".** moq-lite is a different wire and isn't worth bridging to draft-14; the IETF stack will need its own client library (or we contribute one upstream). This narrows M.4 to a clear-cut migration: build/adopt a draft-14 client for hang-mmt-fec/moqtail receivers, retire moq-lite.

## Pending (next iteration)

- File the upstream issue for the `dev/pub` vs `dev/sub` scope mismatch (or send a PR aligning the scripts; trivial fix).
- Run G6 wire-capture diff: capture cast's current libmoq output and `moq-pub`'s draft-14 output to qlog/pcap, diff at the MoQT frame level.
- Draft M.1 ADR after G6 evidence: pick among `moq-pub --mmtp` (extend upstream) vs `moq-pub-mmtp` (sibling crate) vs cast direct integration.
