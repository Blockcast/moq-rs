# Shaka MFU reassembler → WS-1 wasm facade (Generic IFC convergence)

**Status:** Phase 0 complete (facade settled, B(b) core change landed). WS-1 pushed + **PR `Blockcast/libmmt #85`** open (stacked #82←#83←#85). Phase 1.3 (wasm bootstrap) scoped — see below. Plan is post-discovery, corrected against the codebase.
**Program:** Generic IFC reassembler — collapse 3 drifting MFU reassemblers (TS hang/moqtail, Rust, Shaka JS) onto one generic Rust IFC core in `Blockcast/libmmt`.
**Tickets:** Sub-project C = **BLO-8702** (shaka); full package deferred = **BLO-8704**.
**Date:** 2026-06-18.

---

## Goal

Replace Shaka's hand-written `shaka-player/lib/msf/mfu_reassembler.js` with the
WS-1 wasm facade (`MmtReassembler` over the generic mmt-core IFC engine), so all
three players share one reassembly core. Shaka is the **last drift** — hang and
moqtail already track the core via TS; Rust is the core.

---

## Phase 0 findings (DONE — these correct the earlier plan)

### ✅ Facade question SETTLED: `MmtReassembler` (MMT policy) is correct, NOT a generic indexed-stream facade

Reading `mfu_reassembler.js` end-to-end: Shaka's reassembler is **dual-mode** and
maps 1:1 onto the mmt-core **MMT policy** that `MmtReassembler` already wraps.

| Shaka JS behavior | mmt-core MMT policy | Status |
|---|---|---|
| sample-identity path: key `(group, movie, sample)`, order by `sampleOffset`, `hasContiguousSampleOffsets_` | offset mode: `MmtObjectKey::Sample{mpu,movie,sample}`, order by `offset`, byte-adjacency | ✅ exact (`with_sample_identity`) |
| fallback path: key `(group, subgroup)`, order by `objectId` | fallback mode: `MmtObjectKey::Fallback{mpu,timestamp}`, order by counter | ✅ same shape (see shim rule) |
| `keyAliases_`/`resolveBufferKey_` first-only-DU migration | engine alias-promotion (`test_first_only_du_header_alias_promotion`) | ✅ already in core |
| `emittedMfuKeys_` post-emit FEC dedup | engine `emitted` set | ✅ already in core |
| byte-adjacency contiguity | `orders_by_offset` byte-adjacency (C1) | ✅ |

The mmt-core engine is a **superset** of Shaka's JS reassembler.

### ✅ Correctness (keying) CLOSED — facade swap is live-correct

Ground truth: `ffmpeg-moq-mmt-multicast-wire-format.md` (empirically validated
2026-05-29 against `moqenc_mmt.c`).

- `mpu_sequence_number == MoQ group_id` (muxer increments `mpu_sequence` per
  keyframe = per group). Shim passes Shaka's `mpuSequence`; it **is** Shaka's
  `groupId`. No separate group axis needed.
- Concurrent MFUs are separated by **two independent discriminators**, both
  guaranteed by the wire format:
  - `Sample{mpu, movie, sample}` — `movie` (= `movie_fragment_sequence`) is
    distinct per frame/MFU. (`sample_number` is degenerate = always `1`, but
    harmless: `movie` carries the split; `has_sample_identity` only needs `>0`.)
  - `Fallback{mpu, timestamp}` — `timestamp` is distinct per frame, constant
    within a frame, on every packet. Separates MFUs even with no DU header.
- → core keying is **strictly ≥** Shaka's `(group, subgroup)` separation. No
  collision on the deployed FFmpeg path.

### ✅ Clock model DECIDED + LANDED (decision B(b))

- Shaka's JS reassembler is **clock-free**: no timeout, memory bounded purely by
  evict-oldest-on-arrival.
- The mmt-core engine bounded `max_buffered` **only** in `cleanup_timeouts` (host
  must drive a clock tick) → unusable by a clock-free host without regressing
  Shaka into clock-threading boilerplate.
- **Landed** on WS-1 (`libmmt` commit `2b7f7f2`): `add_fragment` now enforces
  the limit eagerly (evict-oldest-on-arrival) after the reassemble attempt;
  completing objects return before any eviction. `cleanup_timeouts` keeps the
  same enforcement (additive) for hosts wanting time-based tail-loss expiry.
  Tests: `mmt-core` 85/85, `mmt-wasm` 25/25, clippy clean.

### ❌ De-risk REFUTED: there is **zero wasm in Shaka**

Whole-tree grep — no `mmt-wasm`/`WebAssembly`/`wasm_bindgen`/`.wasm` in the MSF
path (only incidental hits in `package.json`/`karma.conf.js`/an unrelated
controller). The MMTP pipeline is pure JS + Closure.

**Consequence:** the wasm bootstrap is **greenfield**, not pre-solved. This is the
real cost center of the Shaka phase — async init + Closure-compatible module
loading must be introduced from scratch. The earlier plan's "wasm already loaded"
de-risk was wrong.

---

## Shim mapping (Shaka `Fragment` → `MfuFragmentJs`)

Per `mmtp_track_processor.js` (already extracts every field needed):

| `MfuFragmentJs::new` arg | Shaka source | Note |
|---|---|---|
| `mpu_sequence_number` | `mpu.mpuSequence` (= `groupId`) | high-bits key |
| `fragmentation_indicator` | `mpu.fragmentationIndicator` | FI 0/1/2/3 |
| `fragment_counter` | **`location.object` (objectId)** | ⚠️ NOT the MMTP fragment_counter — FFmpeg's is "fragments-remaining" (unreliable). Shaka orders fallback by objectId; pass objectId into this slot so core fallback ordering matches. |
| `data` | `parsed.payload` | post-header bytes |
| `timestamp` | `parsed.timestamp` | per-sample, distinct per MFU |
| `rap_flag` | `parsed.rapFlag` | FI=1 only |
| `.with_sample_identity(movie, sample, offset)` | `parsed.mfuDuHeader.{movieFragmentSequenceNumber, sampleNumber, offset}` | **only when** `mfuDuHeader != null` (offset mode) |

Clock: pass `performance.now()` as `now` to `add_fragment`. With B(b) landed, the
host does **not** need to drive `cleanup_timeouts` for memory bounding (optional
for tail-loss expiry only).

---

## Phases

### Phase 1 — Land WS-1 + introduce wasm into Shaka (the real cost)
1. ✅ **DONE** — `omar/ifc-ws1-wasm-facade` @ `7acfdec` pushed; **PR #85** open, base `omar/generic-ifc-reassembler-pr2` (stack #82←#83←#85). Carries the `MmtReassembler` facade + the B(b) evict-on-arrival engine change. Merges bottom-up; rebase to `main` after #83.
2. Build the `mmt-wasm` artifact Shaka consumes — see 1.3 (the **target choice is the gating decision**, not just "build it").

### Phase 1.3 — Shaka wasm bootstrap (greenfield; the real cost center)

**Build reality (confirmed):** Shaka is **Closure-compiled** (`python3 build/all.py`); **zero** ESM/dynamic-import/WebAssembly in `lib/`. Externs are first-class + auto-globbed (`build/build.py:167`). Compiled bundle served at `packages/moq-server/public/js/player-shaka-msf.js`, host page `public/players/shaka-msf.html`. `mmt-wasm` `pkg/mmt_wasm.js` today is **ESM** (`build.sh` uses `--target web`) → incompatible with the Closure bundle at runtime.

**Decided approach — `--target no-modules` + externs + one-time async init + SYNC drop-in shim** (sidesteps the ESM↔Closure mismatch):
1. **Add a `--target no-modules` mmt-wasm build** (alongside the `--target web` artifact the ESM players use). Emits a global `wasm_bindgen(init)` + classes via a plain `<script>` — no ESM. (`.d.ts` already exports `MmtReassembler`/`MfuFragmentJs`/`init`/`initSync`; `.wasm` ≈ 319 KB.)
2. **`externs/mmt_wasm.js`** declaring `wasm_bindgen`, `MmtReassembler`, `MfuFragmentJs`, `ReassembledMfuJs` so Closure type-checks + won't rename the shim's calls. Auto-picked-up by `build.py`.
3. **`<script>` in `shaka-msf.html`** loads the no-modules bundle before the player bundle.
4. **One-time `await wasm_bindgen(wasmUrl)`** (module-level memoized promise) in the MSF parser's async startup, **before** the synchronous `new shaka.msf.MmtpTrackProcessor(...)` at `msf_parser.js:694`. → the shim constructor stays **sync `(maxBufferedMfus)`**, a true drop-in for `MfuReassembler` — **no ripple to the `:694` call site** (keeps Phase 2 small).

**Open items / Phase-2 first lookups:**
- Exact MSF async entry method enclosing `msf_parser.js:694` to host the `await` (not yet surfaced).
- No-defaults gap: `maxBufferedMfus` = `MSFParser.OBSERVE_FIRST_MAX_BUFFERED_MFUS_` (Shaka constant) → make catalog-derived when wiring the shim.
- Two build targets (web ESM + no-modules) — decide ship-both vs dedicated Shaka build step.
- `.wasm` serving path + `application/wasm` MIME + the URL passed to `wasm_bindgen()`.
- `init` (async fetch) vs `initSync` (preloaded `BufferSource`, avoids a 2nd fetch); externs must cover every called symbol (Closure renaming).
- +319 KB on the Shaka player load (acceptable; note it).

**Sizing:** this bootstrap is the bulk of the Shaka effort. After it, Phase 2 (shim) is small (mapping table already derived), Phase 3 (parity) mechanical, Phase 4 (delete `mfu_reassembler.js`) trivial.

### Phase 2 — JS adapter shim behind a flag
- New module presenting `shaka.msf.MfuReassembler`'s exact surface
  (`constructor(maxBufferedMfus)`, `addFragment(fragment) → reassembled|null`,
  `flush()`) but delegating to `MmtReassembler` per the shim-mapping table.
- Flag-gate: keep the JS impl as fallback during bring-up.
- `maxBufferedMfus` / (optional) `timeout_ms` stay **catalog-derived** (facade
  `try_new` rejects 0 — no-defaults). Wire catalog params if Shaka's plumbing is
  thin (player-catalog-parity gap: §4.4.2 group formula absent in Shaka).

### Phase 3 — Parity + corpus
- Golden-compare against `test/test/assets/mfu_reassembler_vectors.json` (the
  existing pin). Note: corpus is **counter-mode-only** — add **offset-mode /
  first-only-DU vectors** via `moq-pub-mmtp/examples/reassembler_vectors.rs`.
- Assert byte-identical output JS-impl vs wasm-shim across both corpora.

### Phase 4 — Cutover + delete
- Flip the flag default to wasm, soak, then **delete `mfu_reassembler.js`** — the
  convergence payoff (3 → 1). Update `mmtp_track_processor.js` to construct the
  shim.

---

## Risks

| Risk | Mitigation |
|---|---|
| **Greenfield wasm bootstrap** (biggest effort) | Phase 1.3 dedicated; reuse mmt-wasm's existing init pattern (FEC decoder ships the same module). |
| Closure compiler vs wasm module | Adapter must satisfy `goog.provide`/annotations; async init reconciled with Closure module system. |
| Catalog param parity (no-defaults) | `maxBufferedMfus` from catalog; reject 0. Wire if missing. |
| Offset-mode untested in Shaka corpus | Phase 3 adds offset/first-only-DU vectors. |
| Stats surface gap | Optional: extend `ReassemblerStatsJs` (+`evicted_memory_limit`, `+duplicate_mfus` — core already tracks the former). |
| `mmt-wasm` versioning into Shaka | Shaka pins a built artifact; needs the WS-1-inclusive build. |

---

## Open items (not blockers)
- Extend `ReassemblerStatsJs` for the two stats Shaka tracks but the facade omits.
- Belt-and-suspenders: re-confirm `movie_fragment_sequence` distinctness vs
  `moqenc_mmt.c` HEAD (memory is 2026-05-29; timestamp discriminator makes the
  conclusion robust even if degenerate).

## Sequencing
Phase 1 is the critical path and the real cost (greenfield wasm). The facade and
its one required core change (B(b)) are **done**. Recommend: push WS-1 + open PR,
then Phase 1.3 (wasm bootstrap) as the next focused session — it sizes Phases 2–4.

## Constraints (carried)
`origin` = read-only cloudflare/moq-rs (push to `blockcast`); **libmmt origin IS
writable**. User merges — no auto-merge, no PR open without go-ahead. No submodule
pointer bumps. Commits end `Co-Authored-By: Claude Opus 4.8 (1M context)
<noreply@anthropic.com>`; PR bodies end with the Claude Code generated-with line.
`gh pr edit` hits the Projects-classic bug on Blockcast repos — use
`gh api -X PATCH repos/.../pulls/<n>`.
