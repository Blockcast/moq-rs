# Sub-project B — Publisher Audio (M.4 MMTP-over-MoQ)

Status: SCOPING (read-only investigation). Date: 2026-06-01.
Citations are `file:line` and marked VERIFIED (read in code today) or ASSUMED (needs empirical confirm).

## Summary

The headline finding **overturns the stale memory ground-truth** that the `moq_mmt` muxer emits
ONLY video on the multicast leg. As of the current tree, the entire publish stack is already
multi-track and audio-aware:

- **FFmpeg muxer** (`FFmpeg/libavformat/moqenc_mmt.c`) loops over all input streams, assigns audio
  `packet_id=2`, builds a per-stream fMP4→MMTP Init, runs the SAME MFU fragment/emit loop for audio
  packets, and `send_to_multicast()`s those audio MMTP packets. It also sets audio
  codec/samplerate/channels/initData + audio FEC into the MoQ catalog. Default audio codec = AAC.
- **moq-pub-mmtp** (`moq-rs/moq-pub-mmtp/`) is fully catalog-driven and routes purely by
  `packet_id` → MoQ track via a `HashMap<u16, TrackState>` built from
  `catalog.multicast.endpoints[].tracks[]`. Its Mapping-B `dispatch()` is media-type-agnostic.
- **moq-catalog** (`moq-rs/moq-catalog/`) already has every audio field
  (`codec`, `mimeType`, `samplerate`, `channelConfig`, `bitrate`, `lang`).

**So Sub-project B is mostly "verify + lock the contract," not "build audio packetization."** The
new code is small: a handful of publisher correctness fixes, and confirming the
publisher-authored catalog declares the audio track end-to-end. The bulk of B's value is freezing
the **C-facing contract** (audio track name, catalog fields, MMTP→AAC shape, shared NTP-short PTS
clock) that Sub-project C (Shaka receiver + A/V sync) builds on.

The single biggest must-verify: **does the standard publisher invocation actually map an audio
input stream today?** The code is wired for it and codec-matrix tests imply it has been exercised
(`moqenc_mmt.c:153-155`), but this must be confirmed empirically before declaring B "free."

## Current state (video baseline) — VERIFIED in moqenc_mmt.c

The muxer is a generic fMP4-fragmenting MMTP packager, not video-only.

- **Multi-stream init / packet_id**: header loops `for (i=0; i<s->nb_streams && i<MAX_STREAMS; i++)`,
  assigns `stream->packet_id = i+1` (0 reserved). Stream 0 (video)→pid 1, stream 1 (audio)→pid 2.
  VERIFIED `moqenc_mmt.c:2716-2727`.
- **Per-stream fMP4 muxer**: each stream gets its own `fmp4_ctx`, `frag_every_frame` for audio+video.
  VERIFIED `moqenc_mmt.c:2215-2287`.
- **Per-stream Init MMTP** (`MMTP FT=INIT/MPU` wrapping `ftyp+moov`): built per stream, sent once to
  multicast at header time, cached in `stream->init_mmtp_data` for resend.
  VERIFIED `moqenc_mmt.c:2754-2813, 3187-3199`.
- **MMTP kinds**: Init (FT=0) + MFU (FT=2). FT=1 (moof) not emitted in MMTP packaging. MFU payload =
  fMP4 sample bytes as-is. FI∈{0,1,2,3} computed at `4340-4347`. VERIFIED `moqenc_mmt.c:4441-4444`.
- **mpu_sequence = MoQ group_id**: increments on video keyframe OR every audio fragment.
  VERIFIED `moqenc_mmt.c:3809-3820`.
- **MMTP timestamp = AU PTS (NTP-short, media time)**, per-sample, monotonic — the Mapping-B
  subgroup key AND the shared A/V clock. `sample_pts_us = base_pts_microseconds + i*sample_duration_us;
  timestamp = us_to_ntp_short(sample_pts_us)`. VERIFIED `moqenc_mmt.c:4228-4244`; `us_to_ntp_short`
  masks seconds to low 16 bits (wrap ~18.2h) `moqenc_mmt.c:160-167`. Deliberate spec deviation (PTS,
  not send-time) documented at `4229-4242`.

### Audio is ALREADY wired in the muxer (the key correction)

- **`should_flush = is_video || is_audio`** — the MFU build/emit block runs for audio packets.
  VERIFIED `moqenc_mmt.c:3785-3789`.
- **Every audio AU is a RAP**: `is_keyframe_flags[i] = is_audio ? 1 : has_idr`.
  VERIFIED `moqenc_mmt.c:4196`.
- **Audio sample duration**: `sample_duration_us = (1024*1e6)/sample_rate` (AAC 1024 spf); base PTS
  from tfdt/`pkt->pts`. VERIFIED `moqenc_mmt.c:4098-4102, 4064-4087`.
- **Audio MMTP → multicast send**: the emit loop calls `send_to_multicast(ctx, mmtp_data, mmtp_size)`
  for every source packet regardless of media type; audio MFUs built with `stream->packet_id`(=2).
  VERIFIED `moqenc_mmt.c:4569-4570, 4380-4383`.
- **Audio FEC**: audio uses `fec_audio_k/repair/depth` (4/2/2 ≈ 40 ms) vs video 32/8/30.
  VERIFIED `moqenc_mmt.c:1473-1481, 4543-4546`; per-stream FEC encoder `2735, 2823-2824`.
- **Audio catalog config**: `moq_publish_set_audio_container/config/description` set codec, samplerate,
  channels, and initData (full ftyp+moov; AudioSpecificConfig recovered consumer-side from
  moov.…stsd.esds). VERIFIED `moqenc_mmt.c:3381-3438`. Audio FEC catalog + `<audio>/repair` track
  `2964-3006`.
- **Audio init resend on MoQ leg** (no keyframes): at each video-GOP boundary the muxer flags audio
  `pending_init_resend` so MoQ LargestObject late-joiners get the audio Init.
  VERIFIED `moqenc_mmt.c:3705-3717`.
- **CLI knobs**: `-audio_track_name` (default "audio"), `-fec_audio_k/repair/depth`, default codec aac.
  VERIFIED via `ffmpeg -h muxer=moq_mmt`.

### moq-pub-mmtp is already multi-track (no code change for audio)

- **`build_state_map`** builds `HashMap<u16, TrackState>` keyed by `track_ref.packet_id` from
  `catalog.multicast.endpoints[].tracks[]`; resolves each ref against `catalog.tracks` by name; errors
  on duplicate packet_id or missing track; auto-creates a `<name>/repair` sibling.
  VERIFIED `moq-pub-mmtp/src/main.rs:110-200`.
- **`dispatch`** (Mapping B) routes by `(packet_id, mpu_sequence, MFU timestamp)`: subgroup 0 = Init
  (FT=Init), subgroups 1..M = one MFU each keyed by per-sample MMTP timestamp. Media-type-agnostic.
  VERIFIED `moq-pub-mmtp/src/publish.rs:147-230` + struct doc `70-122`.
- **UDP datagram = one MMTP packet** (no length prefix). VERIFIED `main.rs:305-378`.
- Requires `catalog.multicast.subgroupHistoryGroups` (config-or-throw). VERIFIED `main.rs:157-168`.

⇒ moq-pub-mmtp publishes an audio MoQ track **automatically** the moment the catalog lists an audio
track ref with `packetId=2`. No Rust change required.

### moq-catalog already has the audio fields

- `Track { name, namespace?, initTrack?, initData?, packaging?, container?, renderGroup?, altGroup?,
  selectionParams, temporalId?, spatialId? }`. VERIFIED `moq-catalog/src/lib.rs:40-75`.
- `SelectionParam { codec?, mimeType?, framerate?, bitrate?, width?, height?, samplerate?,
  channelConfig?, displayWidth?, displayHeight?, lang? }`. VERIFIED `lib.rs:523-557`.
- `TrackPackaging::{Cmaf, Loc, Mmtp}` (MSF keys selection on `packaging=="mmtp"`).
  VERIFIED `lib.rs:476-491`.
- `Container::{Isobmff, Mmtp, Mfu, FecRepair}`. VERIFIED `lib.rs:499-521`.
- Multicast ext: `MulticastTrackRef { name, packetId:u16 }`, `MulticastEndpoint { protocol?,
  sourceAddress?, groupAddress, port, tracks[], bandwidth?, networkSource? }`,
  `MulticastConfig { endpoints?, subgroupHistoryGroups?, … }`.
  VERIFIED `moq-catalog/src/multicast.rs:53-118`.

⇒ No schema change needed. An AAC audio track is just a `Track` whose `selectionParams.codec` =
`mp4a.40.2`, `samplerate`/`channelConfig` set, `packaging=mmtp`, `container=mmtp`, plus a
`MulticastTrackRef{name:"audio", packetId:2}` in the endpoint.

## Files to change (with file:line anchors)

Publisher (`FFmpeg/libavformat/moqenc_mmt.c`) — small correctness items, NOT new packetization:

- **`moqenc_mmt.c:3493` (and the twin at the per-endpoint block ~3585)** — the multicast endpoint
  `tracks` string is hardcoded `"video:%d,audio:%d"` and `audio_pid` defaults to 2 **even when no
  audio stream is mapped**. Result: the catalog advertises an `audio` MulticastTrackRef at
  packet_id=2 that may have no corresponding `catalog.tracks["audio"]` entry (the audio catalog
  branch at `3381` only runs when an audio AVStream exists). moq-pub-mmtp would then `bail!`
  ("references track `audio` not present in catalog.tracks") — OR, worse, if a stale `audio` track
  entry exists, announce a silent track. **Fix**: build `mc_tracks` conditionally — include
  `audio:%d` only when an audio stream is present. MEDIUM, correctness/robustness. [VERIFIED bug
  surface; confirm runtime impact in step 1.]
- **`moqenc_mmt.c:4101`** — `sample_rate = st->codecpar->sample_rate > 0 ? … : 48000;` hardcoded
  48 kHz fallback violates the repo "no defaults / error on bad init" rule; a wrong rate corrupts
  every audio MMTP timestamp. **Fix**: error if `sample_rate <= 0`. Same pattern at `3383-3384`
  (sr/ch fallbacks in catalog branch). SMALL, correctness. [DECISION: error vs keep.]
- **`moqenc_mmt.c:1771`** — multicast-FEC branch uses `mc_is_audio = (stream == &ctx->streams[1])`
  (positional). Elsewhere the codec_type test is used (`4543` via `is_audio`, `1867-1868`). **Fix**:
  switch `1771` to the `codec_type==AUDIO` test for order-independence. SMALL, robustness.
- **Multicast audio Init resend cadence** — audio Init is sent to multicast once at header time
  (`3187-3188`); the keyframe-driven multicast Init resend at `3733-3737` resends the *video*
  stream's init only. **Confirm** whether audio `init_mmtp_data` is ever re-sent on the multicast
  leg; if not, add an audio Init multicast resend on a derived cadence (piggyback the video GOP
  boundary, mirroring the MoQ `pending_init_resend`). MEDIUM, multicast late-joiner correctness.
  [OPEN — see Risks #4.]

moq-pub-mmtp: **NO code change.** Catalog-driven; routes audio automatically once the catalog lists
it. (Optional: a test fixture exercising a two-track audio+video catalog — `main.rs:380+` test mod
already builds multi-endpoint catalogs and would extend cleanly.)

moq-catalog: **NO schema change.** All audio fields exist.

## Implementation plan (ordered)

1. **Verify the publisher emits audio MMTP on multicast end-to-end.** Run `FFmpeg/build-native/ffmpeg`
   with both a video and an audio input mapped (e.g. `-f lavfi -i testsrc2 -f lavfi -i sine` or a real
   AAC source) + `-multicast_enabled true -mcast_container mmtp`. Capture the UDP and assert:
   packet_id∈{1,2}; pid=2 carries FT=0 Init + FT=2 MFUs; pid=2 timestamps are per-AU monotonic
   NTP-short; small AUs are mostly FI=0. Also dump the published catalog and confirm a `tracks["audio"]`
   entry + a `MulticastTrackRef{audio,2}`.
2. **Fix the publisher correctness items** (conditional `audio:%d`; sample_rate error; streams[1]
   index; audio multicast Init resend). Small diffs in moqenc_mmt.c.
3. **Confirm moq-pub-mmtp republishes audio** by feeding the capture/live UDP: assert it announces a
   distinct MoQ `audio` track with subgroup 0 = Init, 1..M = per-AU MFUs, plus `audio/repair`.
4. **Freeze the C-facing contract** (section below) for Sub-project C.
5. **E2E** mirroring the video E2E.

## C-facing contract (audio track + catalog + clock) — what Sub-project C receives

- **MoQ audio track name**: `audio` (from `-audio_track_name`, default "audio"). Repair sibling
  `audio/repair` (publisher-internal naming, NOT in catalog.tracks — moq-pub-mmtp auto-creates it).
  VERIFIED `moqenc_mmt.c:2982`, `moq-pub-mmtp/src/main.rs:172-176`.
- **Codec**: AAC. Codec string from `ff_make_codec_str` → `mp4a.40.2` for AAC-LC. VERIFIED codec set
  at `moqenc_mmt.c:3411-3417`; emitted into `selectionParams.codec`. Opus = OUT OF SCOPE (Risks #2).
- **Catalog fields C reads** (all under the `audio` Track):
  - `packaging = "mmtp"`, `container = "mmtp"` (MSF selects on `packaging`).
  - `selectionParams.codec = "mp4a.40.2"`, `.samplerate` (e.g. 48000), `.channelConfig` (channel
    count as string, e.g. "2"). VERIFIED set via `moq_publish_set_audio_config(...,sr,ch)` at `3411`.
  - `initData` = base64 of the audio `ftyp+moov` (full init segment). AudioSpecificConfig is inside
    `moov.trak.mdia.minf.stbl.stsd.<entry>.esds` — C recovers ASC by mp4-atom walk (NOT from a
    separate field). VERIFIED `moqenc_mmt.c:3428-3438` + comment `3420-3427`.
  - FEC params (if `-fec_enable`): algo=raptorq, k=`fec_audio_k`(4), p=`fec_audio_repair`(2),
    symbolSize=`fec_symbol_size`, interleaveDepth in **ms** = `fec_audio_depth*(spf/sr*1000)`,
    repairTrack=`audio/repair`. VERIFIED `moqenc_mmt.c:2969-3006`.
  - Multicast: `multicast.endpoints[].tracks[]` contains `{name:"audio", packetId:2}`.
    VERIFIED `moqenc_mmt.c:3493` (but see Files-to-change #1: currently unconditional).
- **MMTP→sample shape C must transmux**:
  - packet_id=2. Init=FT=0 (`ftyp+mmpu+moov` for the AAC track). MFU=FT=2, FI∈{0,1,2,3}.
  - Each audio AU (AAC frame, 1024 samples) = one MMTP "sample"; small AUs usually one packet (FI=0).
  - **MFU payload = raw fMP4 sample bytes = raw AAC access unit (NOT ADTS-framed)** — it's the mdat
    sample as movenc wrote it. ASSUMED raw-AAC-no-ADTS (consistent with LOC audio using `extradata`
    AudioSpecificConfig at `moqenc_mmt.c:3057-3060`); C should CONFIRM by inspecting esds + absence of
    0xFFF ADTS syncword.
  - **group_id (mpu_sequence) increments every audio fragment** (VERIFIED `3818-3820`) → for audio
    "group == one AU/fragment", unlike video "group == GOP". C must not assume GOP semantics on audio.
- **Shared A/V clock**: BOTH tracks stamp MMTP `timestamp = us_to_ntp_short(AU PTS in media-time µs)`
  (VERIFIED both media types share `moqenc_mmt.c:4244`). Same media-time reference, monotonic per
  frame → C aligns audio vs video by comparing these NTP-short PTS values. Treat the timestamp as
  **media PTS, not arrival time** (deviation noted at `4229-4242`).

## Risks & open decisions

1. **Does the standard invocation map an audio input today?** Code is wired (VERIFIED), exercised by
   codec-matrix per `moqenc_mmt.c:153-155` (ASSUMED). #1 empirical check (plan step 1).
2. **AAC vs Opus** — **DECISION (2026-06-01): defer Opus; scope B → C → A/V-sync to AAC
   (`mp4a.40.2`) only.** VERIFIED: publisher is AAC-only (default `AV_CODEC_ID_AAC` `:4952`; moov
   `stsd` builds `{avc1,hvc1,av01,mp4a}` `:3316` — no Opus sample-entry / `dOps`). Adding Opus =
   net-new muxer work (Opus sample-entry + `dOps` in the Init MPU, Opus MFU framing). Shaka
   **already** supports Opus (`loc_parser.js:372` → 960/sr; `lib/transmuxer/opus.js`) and the catalog
   `codec` field is generic — so Opus stays *additive* later, no schema change. **GUARDRAIL for
   C / A-V-sync: derive audio frame duration from the codec — do NOT hardcode AAC's `1024/sr`**
   (reuse Shaka's `loc_parser` `opus → 960/sr` mapping). This is the one place AAC-first could
   silently bake in rework. Follow-up ticket "Opus on the MMTP audio path" when a consumer needs it.
   Recommendation posted on BLO-8644.
3. **Subgroup-key validity for audio**: per-AU NTP-short timestamps at 48 kHz/1024-spf are ~21.3 ms
   apart; frac resolution ~15 µs → keys stay distinct+monotonic. LOW risk; confirm no collision at
   higher rates. VERIFIED math `160-167` + `4101-4102`.
4. **Multicast audio Init resend cadence** (no audio keyframes): audio Init may be multicast only once
   at header time → late multicast joiners miss it. OPEN; Files-to-change #4.
5. **Unconditional `audio:2` in catalog endpoint** (`3493`): catalog can advertise an audio track ref
   with no backing `catalog.tracks` entry when no audio is mapped → moq-pub-mmtp `bail!`. VERIFIED bug
   surface; Files-to-change #1.
6. **Hardcoded sample_rate/channel fallbacks** (`4101`, `3383-3384`): violate no-defaults; DECISION
   error vs keep.
7. **streams[1]==audio positional assumption** (`1771`): brittle; prefer codec_type test.

## Verification plan

Mirrors the existing video E2E (`.planning/m4-t1.7-e2e/`):

1. **Publisher UDP capture**: custom ffmpeg with audio+video mapped, `-multicast_enabled true
   -mcast_container mmtp`, tee multicast to pcap; assert packet_id∈{1,2}, FT∈{0,2}, pid=2 monotonic
   per-AU NTP-short timestamps, FI mostly 0; dump+assert catalog audio Track + `MulticastTrackRef`.
2. **moq-pub-mmtp republish**: feed capture/live UDP; assert distinct MoQ `audio` track, subgroup 0 =
   Init, 1..M = per-AU MFUs, plus `audio/repair`.
3. **Catalog assert**: fetch catalog; assert audio Track with `packaging=mmtp`, `container=mmtp`,
   `codec=mp4a.40.2`, `samplerate`, `channelConfig`, `initData`, and (if FEC) audio FEC params.
4. **Shaka transmux smoke** (handoff to C): subscribe `audio`, transmux MMTP→AAC, feed MSE; assert
   audio decodes and A/V align via the shared NTP-short PTS.
