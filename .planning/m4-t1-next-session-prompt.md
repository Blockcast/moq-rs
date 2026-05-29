# M.4 Track 1 (Shaka MMTP container) — session handoff

**Date:** 2026-05-29
**Umbrella:** BLO-4020 M.4 (receiver migration). Track 1 = Shaka MMTP container.
**Prereq done:** M.1 (PR #1), M.1b §B1/§B3/§B4, M.4 T0 (PR #5, draft-16 bump).

This session built the foundational, fully-tested receiver pieces of T1, ran a
`/gstack-plan-eng-review` on the remainder, absorbed an outside-voice (codex)
review, wrote Draft Wave-1, and migrated the receiver to the spec's three-level
mapping. Nothing has been pushed — all work is on local branches.

## Repos / branches (NONE pushed)

| Repo | Path | Branch | Base |
|---|---|---|---|
| moq-rs | `~/src/pim-multicast-gateway/moq-rs` | `blo-4020-m4-t1` | off `main` (= blo-4020-m1) |
| shaka-player | `~/src/pim-multicast-gateway/shaka-player` | `blo-4020-m4-t1-shaka-mmtp` | off `ac6c57c42` (transportFactory PR) |
| moqcast-draft | `~/src/moqcast-draft` | `blo-4020-m4-mmt-receiver-contracts` | off `feat/group-align-formula` |

## DONE this session (commits)

**moq-rs** (`blo-4020-m4-t1`):
- `97ceba1` examples/reassembler_vectors.rs — Rust→JSON parity generator (T1.3a/Q3)
- `15cae33` examples/mmtp_packet_vectors.rs — MMTP packet parity generator (T1.1)
- `80c24cc` add MPU sequence vectors to mmtp_packet_vectors (T1.5a)

**shaka-player** (`blo-4020-m4-t1-shaka-mmtp`):
- `bd947ae81` T1.1 `shaka.msf.MMTPParser` (parse MMTP[12]+[FEC 4]+MPU[8]+payload)
- `c189e8241` T1.3b `shaka.msf.MfuReassembler` (pure-JS, parity-pinned)
- `8d2e5d9f9` T1.CAP reassembler required cap + eviction (no timers)
- `d6ff1aec8` T1.5a `shaka.msf.MmtpTrackProcessor` (observe-first parse→reassemble→dump)
- `25ffab3e4` T1.4 `packaging==='mmtp'` branch in `processTrack_` → `observeMmtpTrack_`
- `1ffe08897` B-MIG-recv: re-key reassembly by MoQ (group,subgroup)+object_id

**moqcast-draft** (`blo-4020-m4-mmt-receiver-contracts`):
- `66b2ee1` mmt-00 Wave-1: raw-passthrough, subgroup-per-MFU (mapping B), object_id reconstruction

**Test state:** full Shaka `shaka.msf` suite **127 green**, eslint clean. Run:
`cd shaka-player && python3 build/gendeps.py && python3 build/test.py --no-build --quick --browsers ChromeHeadless --filter 'shaka.msf'`
(google-chrome present; `gendeps.py` needed once after adding a new goog.provide.)

## Decisions of record

- **D1** Hybrid: mirror the LOC receive path; add a `multicast.endpoints[].tracks[]`
  mapping layer only if the real CMSF catalog needs it (settle at T1.7).
- **D2 → REVERSED by codex #7:** object_id reconstruction is **unconditional**
  (don't rely on relay re-sequencing). Tracked as T1.5b-uncond (not yet built).
- **D3** T1.7-first: the real MFU payload format (fragmented-MP4 passthrough vs
  raw-NAL needing CMAF wrap) is unknown; **do not write T1.2/Init code before T1.7**.
- **D4** Draft co-evolves in waves; Wave-1 done.
- **Object ID ↔ MFU mapping = B (three-level):** Group=MPU, Subgroup=MFU,
  Object=fragment. Chosen over flat (A). Consequence: the validated M.1 flat wire
  is now interim/non-conformant; publisher + relay must migrate (below).
- **Codex absorbed:** observe-first T1.5a (done), timing task (T1.5c), FEC-repair
  behavior (T1.FEC), bounded resources (T1.CAP done), version-pin (T1.VER),
  staged T1.7 with real FFmpeg golden fixtures.

## Architecture (traced this session)

The MMTP receive path mirrors the LOC path in `shaka-player/lib/msf/msf_parser.js`:
`processTrack_` (line ~651, `isLoC`/`isMmtp` branch) → per-object `subscribeToTrack`
callback. For MMTP the callback (`observeMmtpTrack_`) routes each MoQ object through
`MmtpTrackProcessor.process(obj.data, obj.location)` and logs records. **Observe-first:
no SegmentReference / MSE / transmux yet** — that's gated on T1.7.

`obj.location` carries `{group, subgroup, object}` (from `msf_tracks_manager.js:278`).
Mapping B uses these as the reassembly key.

## PENDING (next session) — priority order

1. **B-MIG-pub** (moq-pub-mmtp, `publish.rs`): emit **subgroup-per-MFU** (currently
   single subgroup per MPU at `create_group(mpu_seq, 0, ...)`). Needs the FFmpeg
   `moq_mmt` muxer's MFU/sample boundaries. Until this lands, receiver↔publisher
   can't be smoke-tested under B (the synth's 1-MFU/MPU case still works on subgroup 0).
2. **T1.7 (staged smoke)** — capture publisher objects → replay parser fixtures →
   relay dump → Shaka ingest dump → MSE playback. **Discovers the real MFU payload
   format** (gates T1.2 + Init handling). Use real FFmpeg `moq_mmt`, not just synth.
3. **T1.2 (MmtpTransmuxer)** + **Init-segment handling** + **playback wiring**
   (replace observe-first dump with SegmentReference emission). Blocked on T1.7.
   T1.2 approach: thin-delegate to `LocTransmuxer` vs passthrough — decide from T1.7.
4. **T1.5c (timing)** — derive startTime/duration/PTS-DTS + A/V sync for MMTP
   (codex #5; the observe path has no timing yet).
5. **T1.5b-uncond** — unconditional object_id reconstruction (audit `msf_tracks_manager`
   object_id decode; ensure absolute, not raw delta).
6. **B-MIG-relay** — relay/`SubgroupsReader` concurrent-subgroup delivery (the
   "latest-subgroup-wins" hazard; spec B §4.3 now requires it). + regenerate
   `moqcast-draft/draft-ramadan-moq-mmt-00.xml` via `mmark` (not installed locally).
7. **T1.FEC** — define receiver behavior for Repair packets (classify/drop/log).
8. **T1.VER** — assert draft-16 negotiated across publisher/relay/Shaka/catalog.
9. **Draft Wave-2** — MFU payload format + Init→init-segment mapping (after T1.7).
10. **T1.6** — config a `@blockcast/transport` WebTransport-compatible factory
    (`transportFactory` plug-point already exists at `msf_transport.js:128`; Shaka
    side is config-only, real work is in the `ssm-transport` package).

Full task list (19 items) in `~/.gstack/projects/cloudflare-moq-rs/tasks-eng-review-*.jsonl`.

## Parity-harness pattern (reuse it)

Rust generators emit JSON fixtures from canonical `mmt_core` code; JS asserts
byte-equality. Non-circular. Regenerate:
- `cargo run -p moq-pub-mmtp --example reassembler_vectors -- ../shaka-player/test/test/assets/mfu_reassembler_vectors.json`
- `cargo run -p moq-pub-mmtp --example mmtp_packet_vectors -- ../shaka-player/test/test/assets/mmtp_packet_vectors.json`

Note: under mapping B the JS reassembler keys by (group,subgroup); the Rust
`mmt_core::MfuReassembler` keys by mpu_sequence. Parity is preserved at the
**concat** level only (vectors fed as group=mpu_seq, subgroup=0, object=counter).

## CONSTRAINTS (carry-forward)
- TDD strict (RED before GREEN). No timers, no magic numbers / silent defaults
  (reassembler cap is required; observe cap is a flagged provisional constant).
- `origin = cloudflare/moq-rs` read-only; never push there. Confirm destination
  before pushing any branch.
- Edit shaka-player TypeScript/JS source + `gendeps.py`; never hand-edit `dist/`.
- Do NOT write T1.2/Init/playback code before T1.7 reveals the MFU format (D3).
