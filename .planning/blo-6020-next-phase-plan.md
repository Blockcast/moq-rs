# BLO-6020 — next-phase plan (post-B.2)

**Drafted:** 2026-06-05 (kkroo/board, devbox). For Omar review — **no implementation yet**.
**Repo:** `Blockcast/multicast` (default branch `main`; CGO + `fec-raptorq` submodule).

## Where the epic actually is

The recv-pump architecture is **already built and merged behind an opt-in flag** — this is *not* greenfield. What's landed:

- `RecvArchitecture` enum (`route/recvstream.go:43`): `legacy` (default) / `pump-consumer`.
- The whole pump-consumer path exists and is unit-tested:
  - `runSlsOrchestrator` / `dispatchSlsCompletion` / `processSlsCompletion` / `drainSlsOrchestrator` (`route/recvstream.go`) — single-goroutine SLS consumer; per-message panic recovery; drain-on-shutdown; B.1.2 fallthrough Prometheus counter.
  - Orchestrator started at `route/receiver.go:401` **only when** `recvArchitecture == PumpConsumer`.
  - chanMu **already conditionally skipped** under pump-consumer (`route/receiver.go:1914` `legacyLocking := r.recvArchitecture != RecvArchitecturePumpConsumer`).
- **Phase B.2 (PR #253, in review)** — `alcReceiveWorkerCount()→1` under pump-consumer (ALC receive becomes sequential: stream pump + single TOI worker). Verified legacy no-op; merge-ready.

So the heavy lifting (building the topology) is done. What remains is **turning it on and proving it**, then deleting the legacy scaffolding.

## Remaining phases

### Phase C — flip the default to pump-consumer  *(low code risk, high validation effort)*

**Code change (small, revertible):**
1. `route/receiver.go:352` — constructor default `RecvArchitectureLegacy → RecvArchitecturePumpConsumer`. This single flip cascades correctly through every gate already in place: orchestrator start (`:401`), `alcReceiveWorkerCount()` (#253), and the `legacyLocking` skip (`:1914`).
2. Deploy config: `cmd/caddy/receiver/app.go:515` only applies `WithRecvArchitecture` when `recv_architecture` config is non-empty, so the constructor default governs unless overridden. Decide whether to *also* pin `recv_architecture: "pump-consumer"` in the receiver Caddyfile/helm values for explicitness, or rely on the constructor default. **Recommendation:** flip the constructor default (so tests + prod converge) AND leave the config override available as the canary/rollback knob.
3. Keep the legacy path **compiled in** — Phase C is a default change, not a deletion. Instant rollback = set `recv_architecture: "legacy"`.

**Validation contract (the hard part — this is the real work of Phase C):**
| # | Contract item | Where it runs | Devbox-able? |
|---|---|---|---|
| 1 | 0 DPanics across 50+ shard runs | CI 4-shard integ matrix | ❌ CI only |
| 2 | `recovery_test.go:410 unexpected EOF nonce=` → 0 occurrences under **default kernel rmem_max** (no hostNetwork / SO_RCVBUFFORCE / sysctl) | CI integ shards under load | ❌ CI only (UDP overrun is a load phenomenon) |
| 3 | `TestInitSegmentBug` rounds 0–49 pass on 5 consecutive bare-shard runs | CI | ❌ CI only |
| 4 | Sustained 50 Mbps source-rate test: zero unicast repair bytes for a 60s window in pod netns | pod/canary | ❌ cluster only |

**Critical reality:** the validation contract is **fundamentally CI/load-gated** — none of items 1–4 reproduce on devbox (the whole point of the epic is a CI-under-load drop). And the normal driver of that CI loop, **MulticastEngineer (`cd284f1d`), is down** on the Codex pool (BLO-9128/9120). So Phase C validation needs one of:
- (a) the Codex pool restored → MulticastEngineer drives it (preferred — it's the assignee), or
- (b) someone (me as kkroo / Omar) manually drives: push the flip to a branch, `/test` the integ matrix repeatedly, pull shard logs via `gh api .../jobs/<id>/logs`, and tally the contract counters by hand over many runs. Slow and manual but possible.

**Production-risk flag:** flipping the default makes pump-consumer the **production** recv path. It is built + unit-tested but **not yet proven under prod load**. This must go through **canary**, not a bare merge — the `recv_architecture: "legacy"` config knob is the rollback. Phase C is *not* "merge a one-liner"; it's "flip + canary-bake + tally the contract."

### Phase D — retire legacy + delete chanMu  *(pure simplification; only after C bakes)*

Once pump-consumer is the proven default in canary/prod for a bake period, delete the legacy scaffolding. Deletion surface (from grep):
- `chanMuMap` field (`receiver.go:124`), init (`:351`), `getChanMu` (`:2543`).
- The `legacyLocking` branches in `processSls` (`:1914–1920`, `:2048–2052`, `:2178–2185`) + dead commented chanMu refs (`:918`, `:1803`, `:1814`).
- The inline legacy fallthrough in `dispatchSlsCompletion` (`recvstream.go:150-152`) and the legacy branch of `alcReceiveWorkerCount()` (`receiver.go:417`).
- The `RecvArchitectureLegacy` enum value + `WithRecvArchitecture` legacy handling, once nothing selects it.

This is the "chanMu becomes a one-line deletion" win the epic promised — but it's **gated on C baking**, and removing the rollback knob (legacy path) should itself be a deliberate, separate PR.

## Recommendation

1. **Land B.2 (#253) now** — unblocks the epic; zero prod risk (verified legacy no-op). *(Awaiting Ally review + your merge.)*
2. **File Phase C as its own ticket** ("flip recv_architecture default → pump-consumer + validate contract + canary"), assigned to MulticastEngineer, **blocked on BLO-9128** (Codex pool). Don't flip the default until the assignee can drive the CI/canary validation loop — or until you explicitly want me to drive it manually as kkroo.
3. **File Phase D as a follow-up** ("retire legacy recv path + delete chanMu"), blocked on Phase C baking.
4. Do **not** implement the flip this session — the code change is trivial but it's worthless (and prod-risky) without the CI/canary validation that can't run on devbox with the assignee down.

## Open questions for Omar
- Do you want me to **drive Phase C's CI validation manually** (push flip to a branch, hammer `/test`, tally contract counters), or **wait for the Codex pool / MulticastEngineer**?
- Flip via **constructor default** (converges tests+prod) vs. **config-only** (prod opt-in, tests stay legacy)? Recommendation: constructor default + keep config as the canary/rollback knob.
- Canary surface: which receiver deployment + how long a bake before Phase D removes the legacy rollback?
