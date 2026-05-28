# M.1b §B1=C — Raw-passthrough fragmentation contract: smoke results

**Date:** 2026-05-28
**Repo:** `~/src/pim-multicast-gateway/moq-rs` @ `blo-4020-m1b-frag`
**Driver:** `.planning/m1-smoke.sh` (extended with `FRAGMENT=N` env knob)
**Tracking:** BLO-8047 §B1 (M.1b umbrella under BLO-4020)

## Verdict

**SMOKE PASS** end-to-end at `FRAGMENT=3` (Init + 3 MFU fragments per MPU). Per-track sha256 matches publisher input vs subscriber output. Baseline (`FRAGMENT=0`) hashes still match M.1 results — no regression to the M.1 path.

## What this pins

B1 was originally framed in the ADR as *"MMTP fragmentation reassembly — if `fragmentation_indicator != 0`, publisher errors. Defer reassembly to M.1b"*. Re-review on 2026-05-28 overturned that direction:

| Layer | Bytes |
|---|---|
| Ethernet MTU | 1500 |
| -IPv4 / -UDP | -28 |
| Max UDP payload (LAN/direct) | 1472 |
| -AMT (RFC 7450) outer IP+UDP+AMT-data | -36 |
| Max UDP payload (AMT tunnel) | ~1436 |
| -MMTP hdr (12) -MPU hdr (8) | -20 |
| **MFU body per fragment (AMT floor)** | **~1416** |

I-frame fragment counts (typical, moderate quality):

| Resolution | Codec | I-frame | Fragments |
|---|---|---|---|
| 1080p | H.264 | 80-200 KB | 60-150 |
| 4K | H.265 | 300 KB - 1.5 MB | 220-1100 |
| 8K | H.265 | 1-4 MB | 750-2900 |

Erroring on `fragmentation_indicator != 0` would reject every video stream above 1080p audio. Both real receivers (`@moq/hang` in moqtail, Shaka Player via WASM) already consume raw MMTP packets and reassemble themselves using `mmt-core::MfuReassembler` (vendored at `moq-pub-mmtp/vendor/mmt-core/src/reassembler.rs`). Publisher-side reassembly would force them to undo it before re-CMAF for MSE — strictly worse.

The locked contract: **the publisher emits each MMTP packet — Init *and* every MFU fragment — as a separate MoQ object in the `(packet_id, mpu_sequence)` subgroup; the publisher does NOT interpret FI; the receiver reassembles**.

## Pipeline exercised

```
synth_mmtp --fragment 3 (Init + 3 MFU fragments per MPU, FI=0,1,2,3)
  → UDP loopback (127.0.0.1:5004)
  → moq-pub-mmtp --mmtp-input udp
  → moq-relay-ietf --dev --mlog-dir
  → moq-sub-raw (per-track payload dump to file)
  → per-track sha256 vs synth_mmtp's expected output files
```

For one MPU at `mpu_seq=N`, four packets traverse the pipeline:

| Packet | FragmentType | FI | fragment_counter |
|---|---|---|---|
| 0 | Init | 0 | 0 |
| 1 | Mfu | 1 (first) | 0 |
| 2 | Mfu | 2 (middle) | 1 |
| 3 | Mfu | 3 (last) | 2 |

All four share the same `mpu_sequence` and land in the single subgroup keyed by `(packet_id, mpu_sequence)`. The dispatch fn issues exactly one `create_group(mpu_seq, 0, priority)` call per MPU; the next `mpu_seq` opens a new subgroup.

## Run-time evidence

### Baseline (`FRAGMENT=0`, `GROUPS=5`, `PACKET_DELAY_MS=200`)

Per-MPU emission: 1 Init packet per (track, `mpu_seq`). 5 × 1 × 45 B ≈ 225 B/track.

| Track | Bytes | sha256 (publisher input ≡ subscriber output) |
|---|---|---|
| 1 (video) | 225 | `ce0c10f04f3a12fb69f6fbf554178369dbb8748b657f313dc14eaa7249b44f7c` |
| 2 (audio) | 225 | `d98dec2bc7126adeedc3b28cf709230ca425e0f1134d205b26126bbb583f1f45` |

Hashes match the M.1 results documented in `.planning/moq-rs-m1-results.md` — no regression from the synth_mmtp CLI refactor (added `--fragment` flag, defaulted 0).

### Fragmented run (`FRAGMENT=3`, `GROUPS=5`, `PACKET_DELAY_MS=200`)

Per-MPU emission: 1 Init + 3 MFU fragments per (track, `mpu_seq`). 5 × 4 × ~48 B ≈ 960 B/track. Exactly 4× the baseline byte count (matches the 4-packet-per-MPU ratio).

| Track | Bytes | sha256 (publisher input ≡ subscriber output) |
|---|---|---|
| 1 (video) | 960 | `2e5cd5ce633e0c4eb3d430d3aa38800f2b4b2c1a75fbbc8bd60c9ba2bcd39662` |
| 2 (audio) | 960 | `ecdfb0cee281b4eb0d0fd8dd00fb73d3a79b9990f99caa4e0097adc2fffb6cff` |

mlog file sizes confirm the increased object density (one server.mlog per QUIC session):

| Smoke run | mlog total | Notes |
|---|---|---|
| `FRAGMENT=0` | 6191 + 6569 = 12,760 B | 5 SUBGROUP frames per session (5 MPUs × 1 packet each) |
| `FRAGMENT=3` | 13057 + 12707 = 25,764 B | 5 SUBGROUP frames + 4 OBJECTs per SUBGROUP (~2× total) |

The roughly-2× mlog growth tracks the 4× object growth modulo the per-OBJECT vs per-SUBGROUP overhead — internally consistent.

## What the smoke proves

| Contract | Status | Evidence |
|---|---|---|
| Parser accepts FI ∈ {0,1,2,3} cleanly | ✅ | `mmtp_parse::tests::accepts_fragmented_mfu_packets_at_fi_1_2_3` |
| Dispatch routes Init + N MFU fragments to one subgroup | ✅ | `publish::tests::fragmented_mfu_packets_share_one_subgroup_raw_passthrough` |
| Publisher does NOT interpret FI | ✅ | `PacketRouting` has no FI field; sha256 byte-equality at the subscriber |
| `synth_mmtp` emits valid fragmented MMTP packet sequences | ✅ | `tests::build_fragmented_mpu_emits_init_plus_n_mfu_fragments` |
| End-to-end pipeline preserves bytes verbatim under fragmentation | ✅ | Per-track sha256 match at `FRAGMENT=3` |
| Default M.1 path unaffected by CLI refactor | ✅ | Per-track sha256 match at `FRAGMENT=0`, hashes identical to M.1 results |
| Receiver-side reassembly responsibility documented | ✅ | ADR §"NOT in scope for M.1" amended (line 288); BLO-8047 §B1 captures the long-form rationale |

## How to repro

```bash
cd ~/src/pim-multicast-gateway/moq-rs
# Baseline (regression check):
env -i HOME=$HOME PATH=$PATH SMOKE=/tmp/m1-smoke-frag0 \
    GROUPS=5 PACKET_DELAY_MS=200 FRAGMENT=0 \
    bash .planning/m1-smoke.sh

# Fragmented (B1=C contract):
env -i HOME=$HOME PATH=$PATH SMOKE=/tmp/m1-smoke-frag3 \
    GROUPS=5 PACKET_DELAY_MS=200 FRAGMENT=3 \
    bash .planning/m1-smoke.sh
```

`env -i` is documented in `.planning/moq-rs-m1-results.md` — pre-set shell variables (`GROUPS`, `PACKET_DELAY_MS`, etc.) silently override the script defaults; the clean env avoids that footgun.

## Out of scope (M.1b leftovers — see BLO-8047)

- **B2 — per-FEC-block (SBN) grouping for repair tracks.** Repair lands per-source-MPU today; spec wants per-FEC-block (SBN) grouping. Independent of B1.
- **B3 — `object_id_delta` correctness check.** Codex #6 follow-up. Investigate from the mlog dumps now captured.
- **B4 — G6 byte-diff vs libmoq.** Co-located libmoq + moq-pub-mmtp capture, frame-level diff for IETF draft-15+ tracking.

## Files changed in this milestone

- `moq-pub-mmtp/src/mmtp_parse.rs` — added `accepts_fragmented_mfu_packets_at_fi_1_2_3` test + `synth_mfu_fragment_packet` helper.
- `moq-pub-mmtp/src/publish.rs` — added `fragmented_mfu_packets_share_one_subgroup_raw_passthrough` test.
- `moq-pub-mmtp/examples/synth_mmtp.rs` — added `build_mfu_fragment_packet`, `build_fragmented_mpu_sequence` helpers, `--fragment N` CLI flag, and `tests::build_fragmented_mpu_emits_init_plus_n_mfu_fragments`.
- `.planning/m1-smoke.sh` — added `FRAGMENT=N` env knob and wired it through to `synth_mmtp --fragment`.
- `.planning/moq-rs-m1-adr.md` — replaced line 288 "errors if FI != 0" with the raw-passthrough contract; updated CODEX summary at line 326 to mark the fragmentation TODO closed.

Net: ~150 lines added, ~3 lines edited. 0 lines deleted. Zero changes to dispatch logic or any production runtime path — B1=C closed the contract by tests, documentation, and a smoke-time toggle, not by behavior change.
