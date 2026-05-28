Resume M.1 of BLO-4020 (Cloudflare moq-rs MMTP migration). Working directory:
/home/oramadan/src/pim-multicast-gateway/moq-rs

State as of 2026-05-28 (Lane A T1-T4 + T6 all DONE — paused per prompt):
- 30 unit tests green across moq-pub-mmtp; moq-catalog still at 19 = 49 total.
- Release build green, zero warnings.
- T6 ✅ mmt-core vendored at libmmt commit 929e5b0c... under
  moq-pub-mmtp/vendor/mmt-core/, VENDOR.md + explicit deps, builds standalone.
- T1 ✅ publisher loop with spec-true grouping. publish.rs has:
    * TrackSubgroups + SubgroupWrite traits abstracting moq-transport for tests
    * TrackState<T> + RepairSink<T> data structures
    * dispatch() enforcing A1 (Init-only first packet of new MPU), A2 (MPU
      monotonicity hard-error), A3 (unknown packet_id hard-error)
    * 5 dispatch unit tests RED-first then GREEN
  main.rs wires Tracks::new → produce → build_state_map → publish_catalog_track
  → tokio::select! over session.run + publisher.announce + run_publisher.
  6 build_state_map + priority_for_container tests pin the wiring contract.
- T2 ✅ .catalog track at startup. publish_catalog_track() creates `.catalog`
  track, opens group 0 subgroup 0 at priority 127, writes full catalog JSON
  as one object. Sibling test pins track registration.
- T3 ✅ FEC repair routing. build_state_map auto-creates `<source>/repair`
  sibling track for every catalog source. Dispatch's Repair branch routes
  to RepairSink at priority 7. Repair group_id MIRRORS source MPU group_id
  so receiver can correlate by MPU sequence. 5 RED-first repair tests +
  build_state_map repair-registration test.
- T4 ✅ UDP input mode. run_udp_loop() binds tokio::net::UdpSocket, recv_from
  loop, each datagram = one MMTP packet (no length prefix). recv_one_udp_packet
  extracted as testable helper. RED-first integration test sends one synth
  MPU Init packet via UdpSocket pair, asserts dispatch happened.

BRANCH NOTE: still on `main` with all M.0 + M.1 work UNCOMMITTED. User has
been working on main throughout; their "go on" after the prior prompt was
explicit consent. Modified files: Cargo.lock, Cargo.toml, moq-catalog/{Cargo.toml,
src/lib.rs}, moq-pub/src/media.rs. Untracked: .planning/, moq-catalog/src/
multicast.rs, moq-pub-mmtp/. If user wants to commit before continuing, ask
which logical chunks to split into commits.

READ FIRST (in order):
1. /home/oramadan/src/pim-multicast-gateway/moq-rs/.planning/moq-rs-m1-adr.md
   — full ADR with A1-A5/C1 decisions, Implementation Tasks T1-T9, and the
   GSTACK REVIEW REPORT footer.
2. /home/oramadan/src/pim-multicast-gateway/moq-rs/.planning/moq-rs-m0-results.md
   — M.0 baseline (relay works; dev/pub vs dev/sub scope-mismatch documented).
3. ~/src/moqcast-draft/draft-ramadan-moq-mmt-00.md §3.1, §4.1, §4.3, §5, §7.2,
   §11.1 — normative spec for MMTP-on-MoQ wire + catalog + repair priority.
4. ~/src/moqcast-draft/draft-ramadan-moq-multicast-00.md §4.1, §4.2 — multicast
   catalog extension shape (already implemented in moq-catalog/src/multicast.rs).
5. /home/oramadan/src/pim-multicast-gateway/moq-rs/moq-pub-mmtp/src/
   {main.rs,cli.rs,framing.rs,mmtp_parse.rs,publish.rs}
   — current crate state. publish.rs holds the dispatch core; main.rs holds
   the wiring + the .catalog publication + the repair-sibling creation.
6. /home/oramadan/src/pim-multicast-gateway/moq-rs/moq-pub-mmtp/vendor/mmt-core/
   src/header.rs — vendored canonical MmtpHeader + MpuHeader parsers.

REMAINING M.1 SCOPE (resume in any order; T9 depends on T1-T8):

T5 — Catalog validation expansion (P1, ~20min CC)
  Codex #10 + A4: tighten moq-catalog::Root parsing for the multicast extension.
  RED tests FIRST in moq-catalog/src/lib.rs or a new validation module:
    (a) duplicate packet_id across multicast.endpoints[].tracks[] → error.
    (b) multicast.endpoints[].tracks[].name not in catalog.tracks[].name → error.
    (c) commonTrackFields expansion: a track inherits namespace/packaging/render_group
        from common when the track-level field is None.
    (d) namespace ≠ --name CLI flag → warn or error (consistency check).
    (e) Container::FecRepair appearing in catalog.tracks[] is a M.1b error
        (sources only in M.1 — repair tracks are publisher-derived).
  Most of these duplicate checks build_state_map already does at the
  publisher level — promote to library-level validation so subscribers can
  reject bad catalogs too. Keep build_state_map's runtime checks as a
  defense in depth.

T7 — moq-sub-raw new crate (P1, ~45min CC)
  Codex #12 + Q4: create moq-sub-raw/ sibling crate. Reads object payloads
  from named tracks, writes each track's raw bytes to disk so we can sha256
  the publisher's input vs subscriber's output per-track (NOT concatenated
  stdout). Files:
    * moq-sub-raw/Cargo.toml
    * moq-sub-raw/src/{main.rs, cli.rs, subscribe.rs}
  CLI:
    moq-sub-raw --name <ns> --track <name> --output <path> <URL>
  Repeat --track/--output pairs for multiple tracks. Use moq-sub's
  main.rs as the session template (mirrors moq-pub's pattern).
  RED-first tests for the subscribe loop using mock SubgroupsReader.

T8 — ffmpeg moq_mmt stdout output mode (P1, ~3-4h CC)
  CROSS-MODEL #1: NOT IN THIS REPO. The fork lives elsewhere — likely at
  ~/src/ffmpeg-* or wherever the moqenc_mmt.c / libavformat fork is. New
  muxer flag `-moq_mmt_stdout 1` emits length-prefixed MMTP packets to
  stdout instead of muxing to QUIC via libmoq. Output pipes cleanly into
  `moq-pub-mmtp --mmtp-input stdin`. Verify with a real source +
  `ffmpeg -f moq_mmt -moq_mmt_stdout 1 - | moq-pub-mmtp ...`.

T9 — End-to-end smoke + mlog verification (P2, ~20min CC)
  A5 G6 + Codex #7, #8: write .planning/m1-smoke.sh that drives:
    ffmpeg(real source) → moq-pub-mmtp → moq-relay-ietf(--dev, --mlog-dir)
    → moq-sub-raw → per-track files
  Per-track sha256 manifest matches between publisher input and subscriber
  output (one hash per track, NOT concatenated). mlog (NOT qlog) records
  valid SUBGROUP/OBJECT framing per draft-14+. G6 wire-diff compares to
  current cast/libmoq MMTP output (NOT M.0 fMP4). Record verdict in
  .planning/moq-rs-m1-results.md.

CONSTRAINTS (carry-forward from prior session):
- TDD strict per superpowers/test-driven-development: RED test, watch fail,
  GREEN minimal impl, repeat. T1-T4 all followed this — keep the cadence.
- Use mmt-core types: MmtpHeader, MpuHeader, PacketType, FragmentType::Init.
- moq-transport SubgroupsWriter::create drops subgroups silently if group_id
  ≤ latest (subgroup.rs:116-128) — A2 monotonicity catches; don't lean on writer.
- Publisher connect URL: NO path component (root). Subscriber too (M.0 finding).
- Path-deps OK for moq-* workspace crates; mmt-core is vendored.
- Run cargo test after each TDD cycle; do not batch.

ASK BEFORE: anything outside T5/T7/T8/T9 scope. Cross-AI tensions or new
design decisions stop and ask. Push back on anything that violates the
ADR's locked decisions A1-A5/C1.
