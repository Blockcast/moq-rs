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

## SESSION 2 UPDATE (2026-05-29) — T1.7 done, transport prereq done, ordering corrected

This session completed T1.7's core discovery and the concurrent-subgroup transport
prerequisite. The PENDING list below is partly SUPERSEDED — read this first.

**T1.7 (real `moq_mmt` wire format) — DONE** (source-read of the FFmpeg fork's
`libavformat/moqenc_mmt.c` + a live capture, validated by the muxer's own INFO
traces). Full detail in memory `ffmpeg-moq-mmt-multicast-wire-format.md`. Key facts:
- Multicast MMTP leg = **Init (FT=0) + MFU (FT=2) only**; FT=1/moof never emitted.
- MFU DU header (with `sample_number`) on **first fragment only**; and
  `sample_number` is **always 1** (one sample per movie fragment) → useless as a
  discriminator.
- **MMTP `timestamp` is the per-MFU key**: constant across a frame's fragments,
  distinct/monotonic per frame, on every packet. Frame 0's timestamp is 0 → map
  `timestamp → per-group index ≥ 1` (0 reserved for Init), don't use raw timestamp.
- MFU payload = **AVCC length-prefixed NAL units** (not fMP4). → T1.2 transmux must
  CMAF-wrap raw NAL (the harder branch of D3), NOT passthrough.

**B-MIG-pub design — SETTLED** (architect decisions this session):
- Group = mpu_sequence (done). Subgroup 0 = FT=INIT (kept, draft §4.3). MFU
  subgroups = per-group index keyed by the **per-sample MMTP timestamp**
  (loss-robust; survives first-fragment loss). Object = MMTP packet, FI order.
- FT=Fragment never on wire → error if seen.

**Transport concurrent-subgroup prerequisite — DONE** (was mis-listed as priority
6 "B-MIG-relay"; it's actually a PREREQUISITE for B-MIG-pub). Implemented +
tested in `moq-transport/src/serve/subgroup.rs` (TDD, 5 tests, full crate 114
pass, clippy-clean, workspace builds). `Subgroups` now delivers all subgroups of a
group (was latest-wins) with an opt-in `set_history_window(groups)` group-window
prune. See `.planning/m4-b-mig-transport-subgroups-design.md` for the design +
"Implementation status / OPEN".
- **OPEN (deferred to B-MIG-pub by decision):** `set_history_window` is opt-in;
  unset = retain-all. `main.rs` (publisher) + the relay receive path MUST wire it
  from a config source (window value source TBD — likely a catalog field) or they
  leak. Do not run the publisher long until wired.

**Corrected next-task order:** B-MIG-pub (now unblocked: build timestamp-keyed
subgroup-per-MFU in publish.rs + wire `set_history_window` from catalog/config) →
T1.7 staged smoke E2E → T1.2 (CMAF-wrap AVCC NAL) → relay receive-path B + window.

### B-MIG-pub progress (commits this session)

- `553a92d` feat(moq-transport): deliver all subgroups per group + group-window
  prune (the prerequisite — done, 5 tests, 114 crate pass).
- `dc12d1f` docs(planning): T1.7 findings + transport-subgroups design note.
- `a348ea3` feat(moq-pub-mmtp): **Mapping B subgroup-per-MFU dispatch** — DONE.
  PacketRouting surfaces MMTP `timestamp`; TrackState holds subgroup 0 (Init) +
  per-group `HashMap<timestamp, Group>` for MFUs + counter; dispatch routes
  Init→subgroup 0, MFU→timestamp-keyed subgroup (≥1), Fragment→error; A1 relaxed,
  A2 kept. 42 moq-pub-mmtp tests pass, clippy clean.

**B-MIG-pub — COMPLETE** (`47ac1b3`). Window wiring landed: `subgroupHistoryGroups`
added to `MulticastConfig` (global), `build_state_map` reads it and calls
`set_history_window` on each source + repair writer, config-or-throw (errors if
absent for MMTP tracks, rejects < 1). Smoke catalog updated. moq-pub-mmtp 43 pass,
moq-catalog 24 pass, clippy clean. Future: per-track override on `MulticastTrackRef`
if audio/video group-rate disparity ever needs it.

### T1.7 staged smoke — stages 1+2 DONE (real capture → replay)

- **Captured** the real `moq_mmt` multicast MMTP leg on loopback (needs `sudo ip
  link set lo multicast on` + `ip route replace 239/8 dev lo`; capturer
  `/tmp/mmtp_cap2.py`; ffmpeg `moq_mmt -moq_enabled 0 -multicast_enabled 1
  -mcast_container mmtp`). Confirmed: Init(FT0)+MFU(FT2) only, per-MFU timestamps.
- **Replay test** (`ea1a5b5`, moq-pub-mmtp `replays_real_moq_mmt_capture_into_mapping_b_subgroups`):
  real packets through `route()`+`dispatch()` → asserts Mapping-B subgroups
  (Init→0, MFU 1..M by timestamp, fragmented MFUs share a subgroup). Fixture:
  `moq-pub-mmtp/tests/assets/moq_mmt_capture.json` (119 pkts, headers verbatim,
  payloads truncated to 16B). 44 tests pass.
- **Real bug found + fixed**: the resent Init carried a stale `mpu_seq=0` →
  looked like an MPU-seq regression after later groups. FFmpeg fix on branch
  `blo-4020-m4-t1.7-init-mpu-seq` (commit `cb57ae0bf22`, `moqenc_mmt.c`): stamp
  the resent Init with its keyframe group (`gop_count-1`). Re-capture verified
  `I0 M0 I1 M1 I2 M2` alignment. **NOT pushed** (confirm FFmpeg-fork destination
  before pushing); ffmpeg rebuilt locally (`build-native`).

### NEXT: T1.7 stage 3 (relay + Shaka E2E) — was:

With B-MIG-pub done, the next task is the staged end-to-end smoke (handoff PENDING
item 2): real FFmpeg `moq_mmt` multicast → `moq-pub-mmtp` → relay → Shaka observe
dump, verifying the Mapping-B wire (subgroup 0 = Init, subgroups 1..M keyed by the
per-MFU timestamp, objects in FI order). The capture recipe is proven (see memory
`ffmpeg-moq-mmt-multicast-wire-format.md`): `moq_mmt -moq_enabled 0
-multicast_enabled 1 -mcast_container mmtp`. Note loopback multicast on `lo` did
not deliver to a local joiner — use a real interface or feed `moq-pub-mmtp` from a
captured packet file (its stdin length-prefixed input mode). Then T1.2 (CMAF-wrap
AVCC NAL MFUs) and the relay receive-path B + window.

---

## PENDING (next session) — priority order [PARTLY SUPERSEDED — see Session 2 update]

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
