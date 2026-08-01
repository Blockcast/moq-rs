# Audio+video MMTP capture fixture (`moq_mmt_capture_av.json`) — BLO-8706

Unblocker fixture for **BLO-8702 Sub-project C** Work area 3 (receiver audio E2E).
The pre-existing `moq_mmt_capture_full.json` is **video-only** (`packet_id=1`); this
one carries **both** video (`packet_id=1`) and AAC audio (`packet_id=2`).

## Where the fixture lives

**Not tracked in git.** `moq_mmt_capture_av.json` (696 KB) and
`moq_mmt_capture_full.json` (564 KB) were untracked by BLO-19847 — together they
were 1.3 MB of the 2.9 MB `.planning/` tree, and `.gitignore` now keeps them out.
Get one back either way:

```
# restore the exact bytes that were on main @ 594ad43
git fetch origin tag planning-archive-2026-08-01
git restore --source=planning-archive-2026-08-01 --worktree -- .planning/m4-t1.7-e2e/moq_mmt_capture_av.json

# or regenerate the a/v one from scratch (devbox + Blockcast ffmpeg fork)
FFMPEG=~/src/pim-multicast-gateway/FFmpeg/build-native/ffmpeg \
  bash .planning/m4-t1.7-e2e/make_av_capture.sh
```

`m4-t1.7-e2e.sh` and `m4-t1.7-karma.sh` still default `CAPTURE` to the in-tree
path, so a restored file is picked up with no flags; they abort with these same
instructions when it is absent.

Note the regeneration path needs the fork's `moq_mmt` muxer — the system
`/usr/bin/ffmpeg` does **not** have it, so the archive tag is the reliable
route for `moq_mmt_capture_full.json`, whose original capture predates
`make_av_capture.sh`.

## Provenance

Generated on devbox with the custom Blockcast ffmpeg fork
(`~/src/pim-multicast-gateway/FFmpeg/build-native/ffmpeg`, which has the `moq_mmt`
muxer — the system `/usr/bin/ffmpeg` does **not**). Reproducible via
[`make_av_capture.sh`](./make_av_capture.sh):

```
FFMPEG=~/src/pim-multicast-gateway/FFmpeg/build-native/ffmpeg \
  bash .planning/m4-t1.7-e2e/make_av_capture.sh
```

Muxer command (testsrc2 video + 440 Hz sine AAC, 4 s, shared PTS epoch):

```
ffmpeg -f lavfi -i testsrc2=size=320x240:rate=15 \
       -f lavfi -i sine=frequency=440:sample_rate=48000 \
       -map 0:v -map 1:a -t 4 \
       -c:v libx264 -g 15 -pix_fmt yuv420p -profile:v baseline -preset ultrafast \
       -c:a aac -ar 48000 -ac 2 -b:a 128k \
       -f moq_mmt -moq_enabled 0 -multicast_enabled 1 -mcast_container mmtp \
       -mcast_addr 127.0.0.1 -mcast_port 5000 /dev/null
```

`moqenc_mmt.c` assigns `stream->packet_id = stream_index + 1`, so stream 0 (video)
→ `packet_id=1` and stream 1 (AAC) → `packet_id=2` — exactly the C contract. Both
lavfi sources start at PTS 0, so audio/video share the `base_pts` epoch the
shared-clock A/V sync depends on. Capture is unicast on `127.0.0.1:5000` (the
muxer just opens `udp://...`; `serve.py` replays unicast too, so multicast routing
is irrelevant to the fixture).

## Inventory (this fixture)

```
packet_id 1 (video): 247 packets — 5 Init (ftyp/moov, avcC), 242 MFU
packet_id 2 (audio): 189 packets — 1 Init (ftyp/moov, mp4a), 188 MFU
total: 436 packets, ~4 s
```

`packet_id` is bytes `[2:4]` (big-endian) of each datagram (MMTP header word 0,
e.g. `01 00 00 02` = pid 2).

## Validation done on devbox

- **Structure**: both tracks have Init (codec box present: `avcC` video, `mp4a`
  audio) + MFU; AAC frame timestamps advance ~1398 NTP-short units/frame =
  1024 samples @ 48 kHz on the shared epoch.
- **Publisher ingest**: `moq-pub-mmtp` with a 2-track catalog
  (`v`→pid1, `audio`→pid2) parses the catalog (`track_count=2`), ingests all 436
  datagrams with **no error/panic**, connects to the relay, announces `/smoke`,
  `ANNOUNCE_OK`.
- **Video path E2E**: the **video-only slice** of this capture passes the full
  `m4-t1.7-e2e.sh` relay→Shaka harness (**29 segments, init 769 B, monotonic**),
  i.e. the video packets are good through the real pipeline.

## Pairing requirements (important for the audio E2E)

1. **Use the 2-track catalog.** A capture containing `packet_id=2` fed to a
   **video-only** catalog **breaks the video path** (the publisher does not
   gracefully ignore an unmapped `packet_id`; the harness produced 0 segments).
   So this fixture is a **separate file** — do not point the default (video-only)
   harness at it.
2. **`channelConfig` must be a JSON string** in the catalog
   (`"channelConfig":"2"`), not an integer — the moq-catalog parser rejects the
   integer form (`invalid type: integer 2, expected a string`).
3. **Full audio→Shaka playback** still requires the WA1 receiver audio branch in
   `processMmtpTrack_` (devbox Shaka currently asserts video-only). That proof is
   C/WA3, now unblocked by this fixture.

## Open finding for B / BLO-8644

The C-facing contract states *"audio Init resent on the video-keyframe
init-resend."* In this capture the **audio Init appears once** (at stream start),
while the **video Init repeats 5×** (per keyframe, `-init_repeat_gop 1`). The muxer
is **not** resending audio Init per video keyframe in this config. Harmless for the
deterministic replay-from-0 harness (Shaka gets the audio Init up front), but a
real publisher-side gap for late-joiner safety — B should verify the muxer's
audio-init-resend behavior against the contract.
