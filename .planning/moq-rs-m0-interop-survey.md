# M.0 — moq-rs (IETF draft-14+) baseline & MMTP gap survey

**Issue:** [BLO-4020](https://paperclip.blockcast.net/BLO/issues/BLO-4020) "Migrate cast publisher + relay + receivers to Cloudflare moq-rs (draft-14)"
**Scope:** M.0 only — upstream stack baseline + gap inventory. No code changes outside `.planning/`.
**Resumed:** 2026-05-28
**Repo state:** `~/src/pim-multicast-gateway/moq-rs` on `main` @ `f9f51dc` (Cloudflare v0.7.17, 2026-04-13).

## Why M.0 first

The umbrella claim that "moq-lite supports drafts 14-16 via version negotiation" is misleading for our purposes: **moq-lite is a separate fork lineage with its own wire format**, not the IETF draft-14+ wire. Treat moq-lite ↔ moq-rs as a non-goal — the two stacks do not interoperate at the wire level, and receivers (moq-lib, moqtail player) will need IETF clients (M.4 = replace, not extend).

M.0 establishes the IETF baseline we're building on, and inventories exactly what's missing to carry MMTP. M.1 builds on that.

## Target

Draft-14+ of `draft-ietf-moq-transport`. Cloudflare moq-rs main branch tracks draft-14 today; subsequent drafts (15/16/17) come through upstream version bumps. Our work must be forward-portable across drafts.

## Out of scope for M.0

- moq-lite interop (different wire — won't work, don't test).
- moqtail-rs interop (draft-16 — defer until upstream moq-rs bumps).
- Receiver work (M.4).
- Production deployment (M.3).
- BLO-3323 multicast catalog extension port (touches M.1 + M.2; surveyed here, ported in M.1).

## Upstream snapshot (audit)

| Crate | Purpose | LOC observation |
| --- | --- | --- |
| `moq-transport` | Protocol library (CLIENT_SETUP, PUBLISH_NAMESPACE, SUBSCRIBE, stream + datagram delivery) | Core protocol |
| `moq-native-ietf` | QUIC + TLS utilities | — |
| `moq-relay-ietf` | Production-ready relay (caching + dedup) | — |
| `moq-pub` | **fMP4-only** publisher. `media.rs` creates `.catalog` track + per-codec tracks with `TrackPackaging::Cmaf`. | 491 LOC media.rs |
| `moq-sub` | Subscriber client | — |
| `moq-clock-ietf` | Non-media smoke publisher/subscriber | — |
| `moq-catalog` | catalog-format-01 (`draft-ietf-moq-catalogformat-01`). `TrackPackaging` enum has only `Cmaf` + `Loc`. No MMTP variant. No multicast extension. | 194 LOC |
| `moq-api` | Origin discovery / relay coordination (Redis-backed) | — |
| `moq-test-client` | Test harness | — |

**Not supported upstream (per README):** `SUBSCRIBE_NAMESPACE` ("Soon"), `FETCH` ("Not Soon").

## Test matrix — upstream-only smoke

All combinations use IETF draft-14 wire. No moq-lite anywhere.

| # | Publisher | Subscriber | Container | Pass criterion |
| --- | --- | --- | --- | --- |
| T1 | `moq-clock-ietf` pub | `moq-clock-ietf` sub | non-media (clock ticks) | Subscriber prints ticks for ≥30s. Proves SETUP + PUBLISH_NAMESPACE + SUBSCRIBE end-to-end. |
| T2 | `moq-pub` (fMP4, Big Buck Bunny via `./dev/pub`) | `moq-sub` | fMP4 (`TrackPackaging::Cmaf`) | Subscriber writes a non-empty CMAF stream; ffprobe confirms keyframes + audio. |
| T3 | T2 publisher | Cloudflare public interop relay (`interop-relay.cloudflare.mediaoverquic.com:443`) | fMP4 | Optional — sanity check our checkout matches upstream behavior. Skip if local relay passes. |
| T4 | T2 publisher | [video-dev/moq-js](https://github.com/video-dev/moq-js) browser client | fMP4 | Optional — confirms a known-good third-party IETF client works against our local relay. Only run if we have time. |

Tooling: `./dev/relay`, `./dev/clock`, `./dev/pub`, `./dev/sub`. All shell scripts present in checkout.

**Stop conditions** — any failure in T1 or T2 stops M.0 and gets root-caused before M.1. T3/T4 failures get filed as upstream issues but don't block M.1.

## MMTP gap inventory (informs M.1)

What's missing from upstream moq-rs to carry MMTP traffic the way `hang-mmt-fec` does today:

### G1 — Catalog: no MMTP track packaging
`moq-catalog::TrackPackaging` is `{ Cmaf, Loc }`. We need a third variant for MMTP, plus per-codec selection params that survive the MMTP framing (codec, framerate, bitrate, init data shape). Likely shape:

```rust
pub enum TrackPackaging {
    Cmaf,
    Loc,
    Mmtp,  // new
}
```

Plus an MMTP-specific `init` shape — moq-catalog's current `init_track` / `init_data` is fMP4-shaped (`initData` = base64 ftyp+moov). MMTP needs MPU init or asset descriptor equivalents. Decision pending in M.1.

### G2 — Publisher: no MMTP container path
`moq-pub/src/media.rs` is fMP4-pipeline-only — it reads ffmpeg mp4 output, parses moof/mdat, splits per-track, writes objects as Subgroups. There is no equivalent for MMTP frames (MPU + MFU + AL-FEC repair symbols). We'll either:

  - **Option A:** Add an `mmtp` mode to `moq-pub` (new container module alongside the fMP4 one). Probably contributable upstream.
  - **Option B:** Standalone `moq-pub-mmtp` crate, sibling to `moq-pub`. Cleaner separation; no upstream dependency.
  - **Option C:** Cast bridge (`packages/cast` Rust binary) gains direct `moq-transport` dependency and stops calling libmoq C bindings. Bigger change but eliminates one hop.

M.1 picks one. Lean is (B) for prototyping, (A) for the long term.

### G3 — Multicast catalog extension (BLO-3323)
`hang-mmt-fec`'s catalog has a `multicast.endpoints[]` extension (source addr / group / port / codec / FEC params per endpoint) that drives receiver-side IGMP joins. moq-catalog has nothing equivalent. Two options:

  - Port the extension as an optional field on `moq-catalog::Track` (or top-level on `Root`).
  - Upstream the multicast catalog draft (or contribute back); needs IETF coordination.

M.1 picks one — extension first for velocity, upstream later.

### G4 — `SUBSCRIBE_NAMESPACE` not yet supported upstream
The cast bridge's announce/discovery path uses `SUBSCRIBE_NAMESPACE` (in the moq-lite world via `AnnounceInterest`). Upstream moq-rs marks `SUBSCRIBE_NAMESPACE` as "Soon" — not blocking M.0, but M.4 will need it. Track upstream progress.

### G5 — AL-FEC payload
MMTP-over-MoQ today carries RaptorQ repair symbols as siblings to source symbols in the same group (per BLO-3323 §A). There's nothing about FEC in moq-rs upstream — that's purely a payload concern (lives in the MMTP container), so it falls inside G2. No separate gap.

### G6 — Wire-format invariant: cast already emits draft-14 frames via libmoq
Cast's current FFmpeg `moq_mmt` muxer → libmoq → moq-lite path emits frames that libmoq's `Version::Draft14` arm handles. **Whether those frames are bit-identical to what `moq-transport` (Cloudflare) emits at draft-14 is the open question** — both implementations target the same spec, but spec-compliant implementations can still diverge on details (varint encoding edge cases, group ordering, parameter ordering). M.0 should produce a wire capture from T2 and a wire capture from cast's current libmoq publisher, and diff them at the frame level. Result determines whether M.2 (cast bridge port) is "swap the library" or "rewrite the publisher logic".

## Outputs

- [ ] `.planning/moq-rs-m0-interop-survey.md` — this doc (drafted)
- [ ] `.planning/moq-rs-m0-results.md` — test results + wire-capture diffs (pending T1–T2 runs)
- [ ] M.1 ADR draft — picks among G2 options A/B/C and G3 options, scoped after M.0 results

## Risks called out

- **Upstream draft churn.** moq-rs moves with the IETF draft. Anything we contribute upstream survives version bumps; anything we fork locally we own through every bump. Bias toward upstreaming the MMTP container if it's clean.
- **`SUBSCRIBE_NAMESPACE` upstream gap (G4).** M.4 may need it before upstream ships it. Track.
- **Wire divergence at draft-14 between libmoq and moq-rs (G6).** If the diff is non-trivial, M.2 grows. Capture early.

## Next actions (after this doc lands)

1. Run T1 (clock smoke) and T2 (fMP4 smoke). Cargo build the workspace first.
2. Wire-capture both upstream `moq-pub` and a cast-bridge publishing run; diff at the MoQT frame level.
3. Update `.planning/moq-rs-m0-results.md` with verdicts.
4. Draft the M.1 ADR.
