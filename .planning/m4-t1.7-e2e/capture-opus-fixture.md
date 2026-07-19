# Audio+video Opus MMTP capture fixture

`moq_mmt_capture_opus.json` carries video on MMTP `packet_id=1` and Opus audio
on `packet_id=2`. Regenerate it with the Blockcast FFmpeg build:

```sh
AUDIO_CODEC=opus FFMPEG=/path/to/blockcast-ffmpeg \
  bash .planning/m4-t1.7-e2e/make_av_capture.sh
```

Both synthetic inputs begin at PTS zero. The harness catalog declares
`codec=opus`, `samplerate=48000`, and `channelConfig=2`; Shaka must expose those
same values on the active variant. The T1.7 run fails if the audio track is
absent, if the codec differs, if metadata is missing/defaulted, or if playback
does not buffer and advance:

The committed fixture contains 448 MMTP datagrams: 247 video packets on
`packet_id=1` and 201 Opus packets on `packet_id=2`. Its Init MPUs contain both
the video `avcC` box and the Opus `Opus`/`dOps` sample-entry boxes.

```sh
SHAKA_ROOT=/path/to/shaka-player AUDIO_CODEC=opus \
  HARNESS=demo/play-mmtp-load.html .planning/m4-t1.7-e2e.sh
```
