# M.4 ADR — Receiver-side draft-14+ migration & MMTP-on-MoQ playback

**Issue:** [BLO-4020](https://paperclip.blockcast.net/BLO/issues/BLO-4020) M.4 phase.
**Date:** 2026-05-28
**Status:** **DRAFT — awaiting sign-off.**
**Prereqs:**
- M.1 complete (PR #1 — MMTP publisher on IETF moq-transport draft-14).
- M.1b §B1=C complete (PR #2 — raw-passthrough fragmentation contract).
- M.1b §B3 forensics complete (PR #3 — upstream `object_id_delta` bug at `moq-transport/src/session/subscribed.rs:281`).
- M.1b §B4 complete (PR #4 — moq-lite vs IETF wire format diff).
- M.1b §B2 deferred to M.4 prerequisite.

## TL;DR — the scope just got much smaller than expected

The original BLO-4020 framing assumed receiver migration meant rewriting hang-mmt-fec, moqtail, and Shaka onto the IETF wire. Inventory of the codebase reveals **most of that work is already done**:

- **moqtail** is already on IETF MoQ Transport **draft-16** with MMTP container parsing already wired via `@blockcast/mmt-container`. The only M.4 gap is tier-switching fallback + verifying draft-14↔draft-16 version negotiation against `moq-pub-mmtp`.
- **Shaka** (Blockcast fork) has full MSF integration speaking **drafts 14 AND 16** with ALPN negotiation, plus the LOC parser/transmuxer pattern that the MMTP equivalent can mirror.
- **hang-mmt-fec** is the actual migration burden — it speaks moq-lite for production paths, and its IETF subscriber `hard-rejects multi-object subgroups` (`rs/moq-lite/src/ietf/subscriber.rs:673-676`). But **moqtail and Shaka together cover the production receiver set**; hang-mmt-fec migration is now a "legacy receiver" question, not a blocker.

This ADR locks the scope around **two small surgical tracks** and proposes deferring the hang-mmt-fec migration.

## Inventory findings (input to the decisions below)

### moqtail (`moqtail-private/`, used by `packages/moq-server/public/players/moqtail.html`)

- **Wire protocol**: IETF MoQ Transport **draft-16** (`0xff000010`) — `moqtail-private/libs/moqtail-ts/src/client/client.ts`, `apps/client-js/src/lib/player.ts:29-30`.
- **MoQ library**: `moqtail` npm package at `moqtail-private/libs/moqtail-ts/` (v0.10.1). NOT `@moq/hang` / moq-lite.
- **Player**: `moqtail-private/apps/client-js/src/lib/player.ts` (~3300 LOC).
- **MMTP container parsing**: imports `MmtpContainerParser`, `MediaRouter`, `SessionManager`, `setupFecWorkerForTrack` from `@blockcast/mmt-container` (`player.ts:40-51`).
- **MFU reassembly**: in JS, via `@blockcast/mmt-container` + WASM FEC decoder (sync path) + `FecWorkerClient` (Worker path) — `player.ts:2832-2925`.
- **Backends**: MSE (default; CMAF/LOC/MMTP via `MmtpToCmaf` wrap) at `lib/mse-backend.ts`; WebCodecs (raw bitstream + `VideoDecoder`/`AudioDecoder` + AudioWorklet) at `lib/webcodecs-backend.ts`. Backend selection via URL `?backend=` or UI (`app.tsx:240,262,757-768`).
- **Multi-track wiring foundation**: `multi-track-wiring.ts:1-92` (Wave-1 base/delta/repair subscription pattern). **Tier-switching across resolutions is NOT implemented.**
- **Catalog**: `CMSFCatalog` from `moqtail/model` (Blockcast CMSF per draft-ietf-moq-catalogformat-00/01); `endpoint-tracks.ts:32-81` discovers multicast endpoints + maps `packetId → track`.
- **Open question raised by agent**: no draft-14 code path identified. Is moqtail draft-16-only? If so, it cannot connect to `moq-pub-mmtp` (draft-14) without protocol negotiation extending to v14, OR `moq-pub-mmtp` learning v16.

### Shaka (`shaka-player/`, submodule)

- **MSF integration**: `shaka.msf.MSFTransport` at `lib/msf/msf_transport.js` — speaks IETF MoQ Transport drafts 14 + 16 with ALPN negotiation: `PROTOCOL_DRAFT_14 = 'moq-00'`, `PROTOCOL_DRAFT_16 = 'moqt-16'`.
- **Full MoQ session machinery**: `shaka.msf.{Reader, Writer, ControlStream, Sender, Receiver, TracksManager, PresentationTimeline}` at `lib/msf/`.
- **Pattern for container support**: `shaka.msf.LOCParser` (`lib/msf/loc_parser.js`) + `shaka.transmuxer.LocTransmuxer` (`lib/transmuxer/loc_transmuxer.js`) — implements draft-ietf-moq-loc-02. **Mirroring this pattern is the M.4 work for MMTP.**
- **Transport factory plug-point**: `MSFTransport.connect` accepts a `transportFactory` config — designed exactly for plugging `@blockcast/transport`.
- **Blockcast fork strategy**: per `shaka-player/BLOCKCAST_FORK.md`, Blockcast value-add lives in `@blockcast/transport`, not in Shaka itself. Shaka modifications should be upstreamable PRs.

### `@blockcast/transport` (`packages/ssm-transport/`)

- Unified transport manager: WebTransport + SSM multicast + AMT + DRIAD + FEC + ABR + clock sync (`transport-manager.ts`, `abr-controller.ts`, `fec-client.ts`, `clock.ts`, `amt-gateway.ts`, `driad-discovery.ts`).
- **ABR controller already exists** (`abr-controller.ts`) — this is the tier-switching infrastructure the original M.4 design called for as new work.

### hang-mmt-fec (`hang-mmt-fec/`, submodule)

- **Rust**: 16-crate workspace. Receiver-critical: `hang`, `moq-lite`, `moq-msf`, `moq-relay`, `moq-mux`.
- **Production receiver path is JS-side**, not Rust: `js/watch/src/element.ts:131` `class MoqWatch extends HTMLElement` is the `@moq/hang` web component. Uses `window.multicast.subscribeTransportAware(config)`. MMTP reassembly + FEC are JS-native via `@blockcast/mmt-fec` + `@blockcast/mmt-container`. There are NO `wasm-bindgen` deps in `rs/`.
- **Wire**: moq-lite for production paths. The parallel `rs/moq-lite/src/ietf/` module decodes IETF control messages but **subscriber explicitly hard-rejects `sub_group_id != 0`** at `rs/moq-lite/src/ietf/subscriber.rs:673-676`: `if group.sub_group_id != 0 { return Err(Error::Unsupported); }`. So even if we point hang-mmt-fec at the IETF wire today, multi-object subgroups (which is what `moq-pub-mmtp` emits per the B1=C contract) are silently dropped.
- **Catalog**: `js/watch/src/endpoint-tracks.ts` parses `catalog.multicast.endpoints[].tracks[]`.
- **FEC**: `js/watch/src/fec-repair.ts` parses MMTP repair FEC Payload ID; routes to `FecClient` interface from `@blockcast/mmt-fec/block-processor`.
- **Rust `OrderedConsumer`** (`rs/hang/src/container/consumer.rs`) wraps `moq_lite::TrackConsumer` but does NOT call `MfuReassembler` — reassembly is delegated entirely to the JS layer.

## Locked decisions

### A1 — Track 1 (Shaka MMTP container) is the primary M.4 deliverable

Shaka MSF already speaks the IETF wire `moq-pub-mmtp` emits. The only missing piece is the MMTP container parser + transmuxer — a small surgical patch that mirrors the existing LOC pattern. Track 1 produces a working browser receiver against the M.1 publisher without modifying any other receiver.

### A2 — Track 2 (moqtail tier-switching + version-negotiation verification) is the secondary M.4 deliverable

moqtail already has MMTP container parsing and speaks IETF draft-16. The M.4 work for moqtail is small:

- Verify `moq-pub-mmtp` (draft-14) ↔ moqtail (draft-16) version negotiation works end-to-end (the M.1 smoke does NOT exercise this — moqtail wasn't a smoke participant). If broken, choose: (a) bump `moq-pub-mmtp` to draft-16, (b) teach moqtail draft-14 fallback, (c) document the incompatibility for upstream coordination.
- Wire tier-switching: extend `multi-track-wiring.ts`'s base/delta/repair pattern into multi-resolution tier subscription; integrate with `@blockcast/transport`'s `abr-controller.ts` so FEC failure events demote tiers.

### A3 — Track 3 (hang-mmt-fec migration to IETF) is DEFERRED beyond M.4

hang-mmt-fec is a research / legacy receiver path. Production receivers are moqtail + Shaka, both of which are on the IETF side. Migrating hang-mmt-fec's `rs/moq-lite/src/ietf/subscriber.rs:673-676` to accept multi-object subgroups + porting the JS `MoqWatch` element to a IETF-native client is substantial work that does not unblock any production receiver. It's tracked separately as a future workstream.

**If hang-mmt-fec is needed for M.4 production, this decision must be flipped before sign-off.** I am marking this as the highest-impact open question below.

### A4 — Receiver-side MFU reassembly is canonical (B1=C contract)

Per the M.1b §B1 raw-passthrough contract: the publisher emits one MoQ object per MMTP packet (Init + each MFU fragment with FI ∈ {1, 2, 3}); the receiver reassembles using `MfuReassembler` semantics. M.4 receivers MUST honor this. moqtail already does (via `@blockcast/mmt-container`). Shaka MMTP needs the equivalent (Track 1, T1.1-T1.3).

### A5 — Receiver-side `object_id_delta` reconstruction is mandatory

Per the M.1b §B3 forensics, the publisher's wire encodes `object_id_delta = 0` for every object (upstream bug). `moq-relay-ietf` re-sequences on egress, so receivers traversing the relay see correct `object_id` values; direct publisher→receiver topology would break.

M.4 receivers MUST compute `object_id` from running `last_object_id + object_id_delta` themselves and MUST NOT rely on the publisher's wire being correct. This makes receivers robust to the upstream bug and to future direct-publisher topologies. moqtail's draft-16 implementation likely already does this; Shaka MSF must do it explicitly (T1.5).

### A6 — Tier-switching fallback reuses `@blockcast/transport`'s ABR controller

Per the user-surfaced concern during M.1b §B1 review: at 8K, FEC failure rate becomes non-negligible (per §B1 results, 10%+ packet loss → most blocks fail). M.4 receivers SHOULD support tier-switching fallback: subscribe to multiple-resolution tiers from the catalog; on per-block FEC failure for tier N, fall back to tier N-1's decoded frame and upscale.

Implementation: `abr-controller.ts` handles tier selection. The integration point is feeding FEC failure events from `fec-client.ts` so the controller demotes tiers when repair fails. For Track 2 (moqtail) the wiring path goes through `multi-track-wiring.ts`'s base/delta pattern; for Track 1 (Shaka) it goes through Shaka's existing ABR logic adapted to drive the `@blockcast/transport` ABR controller.

### C1 — `@blockcast/transport` is the single transport plug-point

Both Shaka MSF (via `transportFactory`) and moqtail (via `MOQtailClient`'s connection layer) must route all transport variants (WebTransport, SSM multicast, AMT tunnel, DRIAD-discovered relay paths) through `@blockcast/transport`. The factory selects per endpoint based on `multicast.endpoints[].protocol` field in the catalog.

## Implementation Tasks — Track 1 (Shaka MMTP container)

- **T1.1**: Add `shaka.msf.MMTPParser` (mirror of `LOCParser` at `lib/msf/loc_parser.js`). Parses MMTP+MPU header per ISO/IEC 23008-1 §9.2.2 + §A.3; classifies Init vs Mfu via `FragmentType`; extracts payload + per-fragment metadata (`fragmentation_indicator`, `fragment_counter`, `mpu_sequence`).
- **T1.2**: Add `shaka.transmuxer.MmtpTransmuxer` (mirror of `LocTransmuxer` at `lib/transmuxer/loc_transmuxer.js`). Maintains per-track MFU reassembly state; on each reassembled MFU, wraps into MP4/CMAF init+media segments for Shaka's MSE feed.
- **T1.3**: Decide WASM vs pure-JS for `MfuReassembler`. Recommendation: pure-JS port (~200 LOC, smaller bundle, easier debug, matches moqtail's `@blockcast/mmt-container` approach which is also JS-side).
- **T1.4**: Add multicast catalog extension parser to `shaka.msf.MSFTransport`. Reads `multicast.endpoints[].tracks[]` and routes via `@blockcast/transport`.
- **T1.5**: Implement `object_id_delta` reconstruction in `shaka.msf.Reader` per A5. Running `last_object_id` per subgroup; fold delta into absolute `object_id`.
- **T1.6**: Wire `@blockcast/transport` as `transportFactory` in `MSFTransport.connect`.
- **T1.7**: Smoke test: run Shaka Player against `moq-pub-mmtp` (via `moq-relay-ietf`); verify playback. Reuse the M.1b §B1 `synth_mmtp --fragment` source.

## Implementation Tasks — Track 2 (moqtail tier-switching + version-negotiation)

- **T2.1**: Run an interop smoke between `moq-pub-mmtp` (draft-14) and moqtail (draft-16). If broken, document failure mode + pick recovery path (T2.1a: bump `moq-pub-mmtp` to draft-16; T2.1b: add draft-14 fallback to moqtail). Prefer T2.1a (`moq-transport` already supports both).
- **T2.2**: Audit moqtail's `last_object_id` reconstruction — verify A5 is honored. If not, file as an issue in moqtail-private and fix.
- **T2.3**: Extend `multi-track-wiring.ts`'s base/delta/repair pattern to support per-resolution tiers. Subscribe to tier-N and tier-(N-1) in parallel; render tier-N when its FEC is healthy, demote on per-block FEC failure events.
- **T2.4**: Wire `@blockcast/transport`'s `abr-controller.ts` into moqtail. Subscribe `fec-client.ts` block-failure events into the ABR controller's demotion signal.
- **T2.5**: Smoke test: simulate 10%+ packet loss at a tier; assert moqtail demotes to lower tier within one I-frame boundary.

## Implementation Tasks — Track 3 (hang-mmt-fec — DEFERRED)

- **T3.1**: Fix `rs/moq-lite/src/ietf/subscriber.rs:673-676` to accept multi-object subgroups + invoke per-object decode loop.
- **T3.2**: Implement multi-object SubgroupHeader decoder in `rs/moq-lite/src/ietf/` (today the IETF module decodes SubgroupHeader but the subscriber rejects it before any object loop runs).
- **T3.3**: Port `js/watch/src/element.ts`'s `MoqWatch` element off `window.multicast.subscribeTransportAware()` onto an IETF-native client surface.
- **T3.4**: Migrate `js/watch/src/fec-repair.ts` to consume per-FEC-block (SBN) grouping from publisher (depends on M.1b §B2, also deferred).

**T3.1-T3.4 are deferred. Pick up when (a) hang-mmt-fec is needed for production M.4, or (b) M.5 (post-production) wants legacy receiver coverage.**

## NOT in scope for M.4

- **Cast-side wire change.** Cast still emits moq-lite via libmoq today (per M.1b §B4). Migrating cast to emit IETF moq-transport is M.2 (cast bridge port), separate ADR. M.4 lives entirely on the receiver side.
- **Upstream `object_id_delta` fix at cloudflare/moq-rs.** Tracked as a BLO-8047 §B3 draft (in BLO-8047 description); receiver-side reconstruction (A5) sidesteps it without waiting on upstream.
- **Per-FEC-block (SBN) grouping in the publisher** (M.1b §B2). Receiver-side correlation works fine with per-MPU grouping today; SBN grouping is a publisher-side improvement that gives the receiver finer control but isn't blocking. Continue to defer.
- **draft-17+ tracking.** Once `moq-pub-mmtp` bumps to draft-16, plumb that through Shaka MSF (already supports it) and moqtail (already on draft-16). draft-17+ is a future ADR.
- **hang-mmt-fec migration** (Track 3 above). Deferred pending need.

## Open questions for sign-off

1. **Is Track 3 (hang-mmt-fec) really deferrable?** Production receivers in pim-multicast-gateway test scenarios reference both `publisher-moq-lib.sh` (which uses `@moq/hang`) and `publisher-moqtail.sh` (which uses moqtail). If `@moq/hang` is required for production playback, T3 cannot defer. The choice here drives whether M.4 is a 2-week scope (Tracks 1+2 only) or a 6+ week scope (Tracks 1+2+3).

2. **draft-14 vs draft-16 publisher.** moqtail is draft-16; `moq-pub-mmtp` is draft-14. Either we bump `moq-pub-mmtp` (small change in `moq-transport` version negotiation) or we make moqtail draft-14-compatible. Bumping the publisher is cleaner. This affects sequencing — should happen before or alongside T2.1.

3. **Pure-JS vs WASM MFU reassembler in Shaka.** I lean pure-JS (T1.3): smaller bundle, matches moqtail's pattern, easier to debug. Counterargument: drift risk from the Rust source-of-truth.

4. **Tier-switching latency budget.** What's the acceptable lag between FEC failure detection and tier demotion? Sub-frame? One GOP? Project CLAUDE.md mentions A/V drift concerns — relevant here. Default proposal: one-GOP boundary (so the tier change aligns with a keyframe).

5. **`@blockcast/transport` as Shaka transportFactory — actual wiring.** Shaka MSF's `transportFactory` config exists, but I haven't validated end-to-end that `@blockcast/transport` can be dropped in as a factory function. If the interface mismatches, T1.6 grows.

6. **Multicast catalog extension shape in Shaka.** Currently `@blockcast/multicast` defines the JS types. Shaka would need a `lib/externs/multicast.js` shim. Trivial but worth surfacing.

## Tracking

- Umbrella: BLO-4020.
- M.4 sub-issue in paperclip: not yet created. Will be after ADR sign-off.
- Track 1 sub-issues: to be filed under M.4 once locked.
- Track 2 sub-issues: same.
- Track 3 (deferred): captured in this ADR for posterity; not file until needed.
