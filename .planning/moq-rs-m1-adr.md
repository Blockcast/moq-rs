# M.1 ADR — MMTP container on IETF moq-transport (draft-14+)

**Issue:** [BLO-4020](https://paperclip.blockcast.net/BLO/issues/BLO-4020) M.1 phase.
**Date:** 2026-05-28
**Status:** Draft — awaiting sign-off on option choice.
**Prereqs:** M.0 baseline complete (`.planning/moq-rs-m0-results.md`). Wire is confirmed spec-compliant.

## What M.1 has to deliver

Per umbrella: "MMTP layer on top of `moq-transport`. Custom catalog format + frame framing for raw MMTP passthrough."

Concretely, three pieces:

1. **Catalog**: `moq-catalog::TrackPackaging` gains an MMTP variant + the multicast extension fields (`MulticastConfig`/`MulticastEndpoint`/`MulticastTrackRef`).
2. **Publisher**: something that takes MMTP packets and writes them as raw MoQ object payloads on draft-14+ wire.
3. **Subscriber demo** (smoke only): consume MMTP packets via `moq-sub` (or a new MMTP-aware variant) to prove the round trip.

## Survey: what already exists, what doesn't

### MMTP framing — already done

`hang-mmt-fec/rs/hang/src/container/producer.rs` shows MMTP-on-MoQ is **raw passthrough**:

```rust
pub enum ContainerMode {
    Legacy,  // [VarInt timestamp_us][payload]
    Mmtp,    // raw MMTP packet bytes, parser reads 12-byte header at byte 0
    Loc,     // [LOC ext kv pairs][payload]
}
// raw_passthrough = mode == ContainerMode::Mmtp
```

The MoQ object payload is literally an MMTP packet. No hang prefix, no length field, no extra header — the receiver parses the MMTP packet directly. AL-FEC repair symbols travel as additional MMTP packets in the same group.

### MMTP packetization — already done (in ffmpeg)

Cast emits MMTP via ffmpeg's `moq_mmt` muxer (out of tree, in our ffmpeg fork): MPU + MFU split + AL-FEC repair, all packetized into MMTP. The muxer hands packets off to libmoq via C ABI (`moqenc_mmt.c`), which talks moq-lite over QUIC/WebTransport.

The packetization itself is fine. **What needs to change is just the transport library** — swap libmoq (talking moq-lite wire) for moq-transport (talking IETF draft-14+ wire).

### What's missing in moq-rs upstream

- `moq-catalog::TrackPackaging::Mmtp` variant. Currently `{Cmaf, Loc}` — one-line add.
- `moq-catalog::MulticastConfig` + friends. Doesn't exist — needs to be ported from hang-mmt-fec, aligned with draft-ramadan-moq-multicast §7.2.
- Publisher binary that accepts MMTP packets (vs `moq-pub` which only accepts fMP4 frames via stdin into the mp4 parser at `moq-pub/src/media.rs`). New binary or new mode.

## Three approaches

### Option A — Sibling crate `moq-pub-mmtp`, stdin input

A new crate next to `moq-pub` in this workspace. Takes one MMTP packet per line on stdin (or length-prefixed framing — pick one), opens a moq-transport session, announces the namespace, writes each packet as a MoQ object on the relevant track. Catalog is built from CLI flags (`--track id=1,packet-id=1,codec=hev1...` style) or read from a JSON file pointed at by a flag.

- **Pros:** Fast to prototype. Doesn't touch upstream moq-pub. Catalog extension lives in a new local module that we own. Easy to point existing `moq_mmt` ffmpeg muxer at it (pipe stdout from ffmpeg into the binary).
- **Cons:** Diverges from upstream long-term. Eventually we still want to contribute back. Doesn't yet eliminate the libmoq C hop on cast's side — that's M.2.
- **Effort:** ~3-5 days for the publisher binary + catalog port + a smoke test. New code, almost no integration with existing moq-rs internals.

### Option B — Extend upstream `moq-pub` with `--mmtp` mode

Patch `moq-pub/src/media.rs` (or add a sibling module `media_mmtp.rs`) so `moq-pub --mmtp ...` reads MMTP packets instead of fMP4 frames. Patch `moq-catalog/src/lib.rs` to add the `Mmtp` packaging variant + multicast extension.

- **Pros:** Single binary, less code we own, free draft-version upgrades as upstream bumps. Cleanest long-term story.
- **Cons:** Coordination with Cloudflare maintainers — they may not accept multicast catalog extensions (it's not in `draft-ietf-moq-catalogformat-01`). Slower iteration; PR review cycles. If the extension shape changes (and it will, as drafts evolve), we re-PR.
- **Effort:** Same code volume as Option A, plus indeterminate upstream review time. Higher risk on schedule.

### Option C — Cast bridge integrates moq-transport directly, retire ffmpeg `moq_mmt`+libmoq

Replace cast's ffmpeg `moq_mmt` muxer (and the libmoq C hop) with a native Rust pipeline: ingest at the H.264/HEVC NALU layer, packetize MMTP in Rust, write directly to moq-transport.

- **Pros:** Eliminates two hops (the muxer + the C ABI). Single Rust binary all the way through. Best steady-state performance.
- **Cons:** Big-bang rewrite. Huge surface area (MMTP packetizer in Rust = new code, currently lives in ffmpeg C). Reimplements correctness work already proven in `moq_mmt`. Doesn't gate on M.1 — this is really M.2, not M.1.
- **Effort:** Weeks. Out of scope for M.1.

## Recommendation

**Option A now (M.1), with a clean migration path to Option B later (M.1b).**

Rationale:
- We need M.1 to land fast — the gating cost is the catalog + publisher; that's small.
- Option A has minimal blast radius and zero upstream-coordination latency. We learn whether the approach works in days.
- The MMTP packetization stays in ffmpeg's `moq_mmt` muxer (already correctness-proven). We're just swapping the transport library underneath.
- Once we have working code and a stable catalog shape, we can either keep `moq-pub-mmtp` as a separate crate (clean separation) or PR a `--mmtp` mode to upstream `moq-pub` (Option B-as-followup). The catalog extension may stay local indefinitely — the `draft-ramadan-moq-multicast` extension shape isn't in `draft-ietf-moq-catalogformat-01` and upstream may not want it.

**Out of scope for M.1, scheduled for later phases:**
- Option C (cast direct integration) → M.2 ("cast bridge port" per umbrella).
- Receiver migration to IETF draft-14+ → M.4.
- Production relay swap → M.3.

## Design — Option A in detail

### A1. New `TrackPackaging::Mmtp` in `moq-catalog`

One change to `moq-catalog/src/lib.rs`:

```rust
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub enum TrackPackaging {
    #[serde(rename = "cmaf")]
    #[default]
    Cmaf,
    #[serde(rename = "loc")]
    Loc,
    #[serde(rename = "mmtp")]
    Mmtp,
}
```

Any `match` on `TrackPackaging` in the moq-catalog crate itself (none today; the enum has no other users in moq-catalog code) gets the new arm. `moq-pub`'s default fMP4 path is unaffected (it sets `Cmaf` explicitly).

Local-only first. If/when we PR upstream, drop this in.

### A2. Multicast catalog extension

New file `moq-catalog/src/multicast.rs` ported from `hang-mmt-fec/rs/hang/src/catalog/root.rs`:

```rust
// Per draft-ramadan-moq-multicast §7.2
pub struct MulticastEndpoint {
    pub protocol: String,                // "ssm" | "asm"
    pub source_address: Option<String>,  // SSM source IP
    pub group_address: String,
    pub port: u16,
    pub tracks: Vec<MulticastTrackRef>,
    pub bandwidth: Option<u64>,
}

pub struct MulticastTrackRef {
    pub name: String,                    // matches catalog track name
    pub packet_id: u16,                  // MMTP packet_id, 0=signaling, 1+=media
}

pub struct MulticastConfig {
    pub group_address: Option<String>,   // simple form (per-broadcast)
    pub port: Option<u16>,
    pub source_address: Option<String>,
    pub network_source: Option<Vec<NetworkSource>>,  // AMT relays
    pub endpoints: Option<Vec<MulticastEndpoint>>,   // extended form (per-track)
}
```

Then `moq-catalog::Root` gains an optional `multicast: Option<MulticastConfig>` field, `#[serde(skip_serializing_if = "Option::is_none")]`.

For M.1 the field is read-only metadata — the publisher emits it, subscribers may use it for native multicast discovery (out of scope here). No relay-side logic.

### A3. New crate `moq-pub-mmtp`

Layout:
```
moq-pub-mmtp/
  Cargo.toml          # depends on moq-transport, moq-native-ietf, moq-catalog, clap, anyhow, tokio
  src/
    main.rs           # CLI entry
    cli.rs            # clap args
    catalog.rs        # builds moq_catalog::Root with TrackPackaging::Mmtp + multicast{}
    publisher.rs      # MMTP ingestion → MoQ object writer
```

Add `"moq-pub-mmtp"` to root `Cargo.toml` workspace members.

CLI:
```
moq-pub-mmtp --name <broadcast-name> <URL> \
  --catalog-json <path>         # full catalog JSON (preferred, copies cast's emission)
  --bind <ADDR>                 # UDP receive socket (default [::]:0 client-side QUIC)
  --tls-disable-verify
  --tls-cert/--tls-key/--tls-root
  --mmtp-input <stdin|udp:port> # where to read MMTP packets from
```

The `--catalog-json` flag is the simplest interface. Cast already builds catalog JSON for its current pipeline; we point our binary at the same JSON and it announces the right tracks. Future iteration can build catalog from flags.

`--mmtp-input` defaults to `stdin` with length-prefixed framing (4-byte BE length + bytes). UDP option for the case where cast's `moq_mmt` muxer is configured to emit MMTP to UDP loopback (matches the multicast path).

Per-track lifecycle:
1. Parse catalog JSON. Build per-track moq-transport `TrackProducer` keyed by `multicast.endpoints[].tracks[].packet_id`.
2. Open moq-transport session to relay URL. Announce namespace = broadcast name.
3. Loop: read one MMTP packet, dispatch to the track keyed by `packet_id` (parsed from MMTP header byte 4-5).
4. Write packet bytes verbatim as the MoQ object payload — no length prefix, no timestamp, no transformation. This mirrors `raw_passthrough` in hang-mmt-fec.

Group boundaries: ffmpeg's `moq_mmt` muxer marks group starts (keyframes). For Option A, the simplest approach is to derive group boundaries from MMTP MPU sequence numbers (each new MPU = new group, matching hang-mmt-fec's behavior for video). For first cut, accept that the muxer needs to signal explicit group boundaries via an out-of-band hint (UDP framing variant) or the publisher splits on MPU boundary heuristically. Document the choice in the publisher source.

### A4. Smoke test (M.1 verification)

End-to-end pipeline:
```
ffmpeg (real source) → moq_mmt muxer → MMTP packets via stdout
  → moq-pub-mmtp → moq-relay-ietf (M.0 baseline, --dev, unscoped)
  → moq-sub --raw  → file
```

Need a `--raw` flag on `moq-sub` (or a new minimal subscriber) that reads object payloads and writes them out without trying to parse fMP4. Verification: feed the captured MMTP stream into `hang-mmt-fec`'s existing MMTP parser (or moqtail-rs's MMTP parser) and confirm packet framing is intact + AL-FEC payload IDs visible.

Out of scope: render the video. M.1 proves bytes flow correctly; rendering is M.4.

### A5. Wire-format check (G6 from M.0 survey)

While running A4, capture qlog (the relay already supports `--qlog-dir`) on both publisher and subscriber. Compare the on-the-wire SUBGROUP/OBJECT framing against an independently-captured `moq-pub --fMP4` qlog from M.0 T2 (same draft, same relay). Confirm the difference is only in object payload contents, not in framing/header structure. If framing differs, we have an upstream-version drift to chase.

### A6. Forward-port discipline (drafts 14 → 15 → 16+)

Pin `moq-pub-mmtp` to use the moq-transport workspace dep (not a published crate version). When upstream bumps drafts in moq-transport, this crate inherits it for free. Catalog extension stays in moq-catalog/src/multicast.rs as an optional struct on `Root` — schema version is independent of the wire draft.

## Out of scope (explicit non-goals for M.1)

- Touching `moq-pub/src/media.rs` (fMP4 path stays untouched).
- Touching `moq-relay-ietf/` (relay is byte-transparent for object payloads).
- Touching cast's ffmpeg `moq_mmt` muxer (it already produces correct MMTP).
- A draft for an "MMTP-on-MoQ" IETF I-D (long game — comes after M.1 proves out).
- Multicast send-side (relay → multicast tree) — that's a separate piece on the umbrella project, not this migration.
- Receiver work (M.4).
- Production deployment (M.3).

## Risks

- **G6 wire divergence between libmoq and moq-transport.** If qlog comparison shows framing differences beyond payload, M.1 grows. Mitigation: capture early, in parallel with the publisher build.
- **Catalog interop with moqtail/moq-lib (eventually).** The `multicast.endpoints[]` shape we port is hang-mmt-fec's. moqtail's `carp-catalog` may have drifted. Mitigation: cross-check `libmmt/packages/container/src/carp-catalog.ts` before finalizing the moq-catalog field shape.
- **Object-grouping semantics for MMTP-without-MPU-hints.** If ffmpeg's `moq_mmt` muxer doesn't expose MPU boundary signals on stdout, we need either a length-prefix-with-flags framing or a UDP-with-marker variant. Mitigation: start with stdin length-prefixed framing + an optional `--mpu-marker` byte at packet head; iterate based on muxer output capability.

## Definition of done

- [x] **Catalog layer landed** — `moq-catalog::Container` enum + `Track.container` field per draft-ramadan-moq-mmt §11.1; `multicast` module per draft-ramadan-moq-multicast §4.1+§4.2.3; 19 unit tests green.
- [x] **moq-pub-mmtp crate scaffolded** — workspace member, `--help` documents catalog flag + input modes; 11 publisher-side unit tests green (framing + MMTP header routing).
- [x] **mmt-core wired** via path-dep `../../libmmt/mmt-core` (A5 decision: vendor next — see Implementation Tasks).
- [x] **M.0 baseline + UX caveat** documented (`.planning/moq-rs-m0-results.md`): scope-mismatch between dev/pub and dev/sub identified; relay wire is spec-compliant.
- [ ] **Publisher loop (task #8)** — MMTP packet dispatch with A1/A2/A3 guards (see Implementation Tasks).
- [ ] **ffmpeg `moq_mmt` stdout mode** — new muxer flag emits length-prefixed MMTP to stdout/FIFO (CROSS-MODEL #1 decision).
- [ ] **`.catalog` track publication** — publisher writes catalog JSON as first object of group 0 on the `.catalog` track (CROSS-MODEL #3).
- [ ] **Catalog validation expansion** — commonTrackFields expansion, multicast↔tracks cross-check, dup-packet_id detection, namespace↔`--name` check (Codex #10 + A4).
- [ ] **`moq-sub-raw` sibling crate** — minimal raw subscriber writing per-track output files (Q4 decision).
- [ ] **`mmt-core` vendored** at pinned commit under `moq-pub-mmtp/vendor/` (A5 execution).
- [ ] **Smoke test passes**: ffmpeg → moq-pub-mmtp → moq-relay-ietf → moq-sub-raw with **per-track sha256 hash match** (NOT concatenated stdout) and **mlog-level verification** (NOT qlog).
- [ ] **G6 wire-diff** captured against **current cast/libmoq MMTP output** (NOT against M.0 fMP4), at mlog level.
- [ ] `.planning/moq-rs-m1-results.md` records the run + mlog locations + per-track hashes + libmoq diff verdict.

## Open questions for sign-off

_Resolved 2026-05-28 via `/gstack-plan-eng-review` (see GSTACK REVIEW REPORT at end of file)._

Original drafting questions are preserved here for history. The decisions taken during the engineering review supersede these:

1. **Option choice.** A (sibling crate, recommended) vs B (upstream first) vs split. → A locked in.
2. **Catalog-JSON vs CLI flags.** → Catalog-JSON only for M.1.
3. **Group boundary signal.** → Explicit MPU-sequence from MMTP/MPU headers via libmmt mmt-core; ffmpeg muxer gets a stdout output mode (CROSS-MODEL #1).
4. **`moq-sub --raw` smoke flag.** → Sibling crate `moq-sub-raw`, not an upstream patch.

## Implementation Tasks

Synthesized from the engineering review's findings. Each task derives from a specific finding above. Run with Claude Code or Codex; checkbox as you ship.

- [ ] **T1 (P1, human: ~2h / CC: ~30min)** — moq-pub-mmtp — Publisher loop with spec-true grouping
  - Surfaced by: A1, A2, Codex #5, #6
  - Files: `moq-pub-mmtp/src/main.rs`, new `moq-pub-mmtp/src/publish.rs`
  - Verify: `cargo test -p moq-pub-mmtp` — all new unit tests RED-first then GREEN; manual: `moq-pub-mmtp` connects to relay, announces, accepts SUBSCRIBE
- [ ] **T2 (P1, human: ~30min / CC: ~10min)** — moq-pub-mmtp — Publish `.catalog` track on startup
  - Surfaced by: Codex #3
  - Files: `moq-pub-mmtp/src/publish.rs`
  - Verify: `moq-sub --catalog --name <ns> https://localhost:4443` retrieves the JSON
- [ ] **T3 (P1, human: ~1h / CC: ~20min)** — moq-pub-mmtp — FEC repair routing to `/repair` tracks at priority 7
  - Surfaced by: A3
  - Files: `moq-pub-mmtp/src/publish.rs`
  - Verify: repair packets land on `<track>/repair`; subscriber sees both tracks; priority 7 confirmed in mlog
- [ ] **T4 (P1, human: ~30min / CC: ~10min)** — moq-pub-mmtp — Stdin AND UDP input paths
  - Surfaced by: C1
  - Files: `moq-pub-mmtp/src/main.rs`, `moq-pub-mmtp/src/framing.rs` (UDP datagram variant)
  - Verify: smoke runs both `--mmtp-input=stdin` and `--mmtp-input=udp`
- [ ] **T5 (P1, human: ~1h / CC: ~20min)** — moq-catalog + moq-pub-mmtp — Catalog validation expansion
  - Surfaced by: A4 + Codex #10
  - Files: `moq-catalog/src/lib.rs`, `moq-catalog/src/multicast.rs`, `moq-pub-mmtp/src/catalog_load.rs`
  - Verify: 5 new unit tests RED-first covering each rejection path
- [ ] **T6 (P1, human: ~30min / CC: ~10min)** — moq-pub-mmtp — Vendor mmt-core under `vendor/`
  - Surfaced by: A5 execution
  - Files: copy from `libmmt/mmt-core/` to `moq-pub-mmtp/vendor/mmt-core/`; update `Cargo.toml` path; add `vendor/mmt-core/VERSION` with pinned upstream commit
  - Verify: `cargo build -p moq-pub-mmtp` from a checkout WITHOUT a sibling libmmt
- [ ] **T7 (P1, human: ~2h / CC: ~45min)** — moq-sub-raw — New sibling crate
  - Surfaced by: Q4 + Codex #12
  - Files: `moq-sub-raw/Cargo.toml`, `moq-sub-raw/src/main.rs`, `moq-sub-raw/src/cli.rs`, `moq-sub-raw/src/subscribe.rs`
  - Verify: subscribes to a named track list, writes per-track object payloads to disk, exits cleanly on EOF / SIGINT
- [ ] **T8 (P1, human: ~1-2 days / CC: ~3-4h)** — ffmpeg fork — Add `moq_mmt` stdout output mode
  - Surfaced by: CROSS-MODEL #1
  - Files: `libavformat/moqenc_mmt.c` (in ffmpeg fork)
  - Verify: `ffmpeg -i <src> -f moq_mmt -moq_mmt_stdout 1 -` emits length-prefixed MMTP frames; pipes cleanly into moq-pub-mmtp
- [ ] **T9 (P2, human: ~1h / CC: ~20min)** — moq-pub-mmtp — End-to-end smoke test script
  - Surfaced by: A5 G6 + Codex #7, #8
  - Files: new `.planning/m1-smoke.sh`, `.planning/moq-rs-m1-results.md`
  - Verify: per-track sha256 manifest matches between publisher input and subscriber output; mlog (not qlog) records valid SUBGROUP/OBJECT framing; G6 diff against libmoq's current MMTP output captured

## NOT in scope for M.1
- **Object 0 = MPU metadata invariant ENFORCEMENT only** — the publisher errors on violation but does not synthesize MPU metadata from MFU data. Caller's responsibility (cast / ffmpeg).
- **MMTP fragmentation reassembly** — **raw-passthrough contract** (locked 2026-05-28, BLO-8047 §B1). The publisher emits each MMTP packet — Init *and* every MFU fragment with `fragmentation_indicator` ∈ {0, 1, 2, 3} — as a separate MoQ object in the `(packet_id, mpu_sequence)` subgroup; the publisher does **not** interpret FI. Receivers reassemble using `mmt-core::MfuReassembler` (vendored at `moq-pub-mmtp/vendor/mmt-core/src/reassembler.rs`).

  *The earlier draft of this ADR planned* "if FI != 0, publisher errors. Defer reassembly to M.1b". Overturned by dimensional math: AMT MTU floor ≈ 1416 bytes after IP/UDP/AMT/MMTP/MPU overhead, so a 4K I-frame needs ~220-1100 fragments and an 8K I-frame ~750-2900. An error-on-FI-non-zero rule would reject every video stream above 1080p audio. Both real receivers (`@moq/hang` in moqtail, Shaka via a WASM shim) already consume raw MMTP packets and reassemble themselves; publisher-side reassembly would force them to undo it before re-CMAF for MSE — strictly worse.

  Pinned by `mmtp_parse::tests::accepts_fragmented_mfu_packets_at_fi_1_2_3` and `publish::tests::fragmented_mfu_packets_share_one_subgroup_raw_passthrough`. M.1 smoke exercises the path via `synth_mmtp --fragment N`.
- **FEC source-block-correct grouping** — repair packets in M.1 land on a single rolling group per repair track. Per-FEC-block grouping (parsing Source/Repair FEC Payload ID) is M.1b.
- **Receiver-side decode/render** — that's M.4.
- **Subscriber `.catalog` auto-discovery** — moq-sub-raw takes track names explicitly. M.4 will wire catalog-driven discovery into real players.
- **Production relay swap** — M.3.
- **moqtail-rs draft-16 interop** — defer until upstream moq-rs bumps to draft-16.
- **MFU mode specific receiver semantics** — publisher accepts `container: mfu` identically to `mmtp`. Receiver distinguishes in M.4.
- **moq-transport object_id_delta correctness** — flagged by Codex #6 as potentially broken at moq-transport level. Verified in T9 mlog; if confirmed broken, becomes upstream issue + M.1b fix.

## What already exists
- **Catalog (moq-catalog)**: `Container` enum + `multicast` extension landed in this checkout (19 tests). Spec-aligned per draft-ramadan-moq-mmt §11.1 and draft-ramadan-moq-multicast §4.1+§4.2.3.
- **MMTP framing parser**: `moq-pub-mmtp::framing::read_one_frame` length-prefix decoder (7 tests).
- **MMTP+MPU header parser**: `moq-pub-mmtp::mmtp_parse::route` via libmmt mmt-core (4 tests).
- **Session/publisher pattern**: cribbed directly from `moq-pub/src/main.rs` (canonical Tracks → Publisher::connect → tokio::select).
- **moq-relay-ietf**: M.0 baseline confirmed working end-to-end via T1 (clock) + T2 (fMP4) smoke tests.
- **mmt-core (libmmt)**: canonical Rust ISO/IEC 23008-1 parser; reused via path-dep (will be vendored — T6).
- **ffmpeg `moq_mmt` muxer**: produces correct MMTP packets today; only the output sink needs the new stdout mode (T8).

## Worktree parallelization

Sequential implementation, no parallelization opportunity. T1 depends on T6 (vendor) for clean isolated build; T2 depends on T1 (publisher loop); T3 + T4 depend on T1; T7 (moq-sub-raw) can run parallel to T1/T2/T3/T4 since it's a separate crate; T8 (ffmpeg fork) runs in a different repo entirely (parallel). T9 (smoke) depends on T1–T8.

Suggested lanes:
- Lane A: T6 → T1 → T2 → T3 → T4 → T9 (publisher path, sequential per-task)
- Lane B: T7 (subscriber, independent crate)
- Lane C: T8 (ffmpeg fork, different repo)
- Merge at T9 (smoke).

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | — | not run (intentional — scope locked at umbrella BLO-4020) |
| Codex Review | `/codex review` | Independent 2nd opinion | 1 | issues_found | 12 findings, 9 actionable (folded into Implementation Tasks) |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 1 | clean | A1/A2/A3 + A4/A5 + C1 (6 decisions taken); 0 unresolved |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | n/a (backend) |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | n/a (internal infra crate) |

**CODEX:** 12 findings, 9 folded into Implementation Tasks (T1–T9). The remaining 3 were originally tagged "M.1b TODO":

- **MMTP fragmentation reassembly** — *resolved* 2026-05-28 as a raw-passthrough architectural pushback (see §"NOT in scope for M.1" above and BLO-8047 §B1). Closed by contract, not by code.
- **Object_id_delta verification** — open, tracked as BLO-8047 §B3.
- **FEC source-block (SBN) grouping for repair** — open, tracked as BLO-8047 §B2.
**CROSS-MODEL:** Strong agreement on shape (sibling crate, mmt-core reuse, spec-true grouping). Tensions on **#1 input pipeline** and **#3 .catalog publication** — both resolved in favor of Codex's read, scope expanded with explicit tasks (T8, T2).
**UNRESOLVED:** 0.
**VERDICT:** ENG CLEARED — ready to implement T1–T9. Codex outside voice ran clean after fold-in.

_Eng review: 2026-05-28 by Claude Opus 4.7 (1M context) via /gstack-plan-eng-review. Codex outside voice ran read-only against the ADR + the spec drafts at ~/src/moqcast-draft._
