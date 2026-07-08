# Next session — BLO-9158 Track-A: implement read-before-commit → recall

**Resume:** `resume .planning/m4-next-session-2026-06-07-blo9158-recv-recall-tier.md`
**Drafted:** 2026-06-07. **Repo for the actual work:** `Blockcast/multicast` (`~/src/multicast`), NOT moq-rs.
**Full design is recorded in BLO-9158** (comment `dbdaa7e5`, 2026-06-07) — read it first.

---

## TL;DR — what this session settled

The "integ-lane ghost" (`TestReloadReceiverThenRequestProxy` delivers 0 bytes for a FINISHED-but-empty File post-reload) was chased to a **definitive negative result and an architecture pivot**:

- **A serveFound discriminator is DEAD.** Every serve-`tf`-reachable "is the body present" signal is contaminated by reload/object churn — `recvIdx`, `lastRcvOffset`, `tf.ReadAt`, capture-time `srcFilePopulated`. Four tried + killed (ReadTimeout-liveness, `lastRcvOffset==0`, `ReceivedIdx∩body`, `srcFilePopulated`). Proven by probe (`TestInitSegmentBug/round_0`: 1190 iters `recvIdx:"" lastRcvOffset:0 readN:0` while body on disk; `srcFilePopulated` flapped 11×true/1×false across 12 store re-creations of one TOI). Root: `servefound_ablen.go:13-15` — in-stream signals are per-instance, reset by churn; only the on-disk file is instance-independent; the **DB is the contaminated source** (it makes `realKnown()` true for the ghost via `ensureRealLen`/#239). **Do NOT attempt a 5th predicate.**
- **The fix is a recall tier that already exists.** receiver PUSHes the body to `recvPushURL/contentLocation` (Cache-Control/IMS/ETag, `plugin.go:3508-3537`) → durable copy; on a symstore miss, `serveFound` `stallActionFallthrough → invoke <DS>_<proto> (trafficops.go:2523) → reverse_proxy → parent ATS10 (trafficops.go:2337-2346) → origin`. `next` is a **cache, not origin** (verified). The bug is only that `realKnown()` **commits the header before the body is confirmed readable**, pre-empting that fallthrough → D8 hard-fail.

## THE NEXT STEP (this is net-new code — TDD, CI-gated)

**Implement read-before-commit → recall** in `cmd/caddy/receiver/plugin.go`:

1. Gate the commit (`p.prepHeaders(rw, resp); sentHeader = true`, **`:1909-1910`**, gated `if err==nil && resp!=nil` at `:1854`) on an **actual first-body read of the *current* `tf`**. Body is lazy `TeeReader(SectionReader(tf, startOffset, respLen))` at `:1841-1842` (read *after* commit today — that's the bug).
2. Insert the probe **immediately before `:1909`**, on the **current** `tf` (NOT `:1716` — too early; symstore fills over iterations, `tf` swaps at `:1892-1907`). Readable ⇒ commit+stream as today; not-readable ⇒ don't commit, `continue` (loop fills / `classifyStall`→`stallActionFallthrough`→recall).
3. **Residual knob to solve:** not-readable-at-commit → wait-loop is slow for churn (snapshot store may never fill → up to the 30s stall). **Pair the gate with the existing `tf`-swap (`:1892-1907`)** to re-resolve to a populated `tf` or shortcut to recall, rather than 30s-stall.
4. TDD: a `classifyStall`/pre-commit-style **unit test** first (FINISHED-but-empty ⇒ don't-commit→recall; readable ⇒ commit). Predicate logic is pure + fast.

## Validate (CI-load-gated — NO local repro; devbox loopback has too little loss)
`TestReloadReceiver*`, `TestPushLossyStream`, `TestAllConfigsProxy` green + **`TestInitSegmentBug` no-regression** (churn must recall, not 30s-stall) + re-apply the ghost-probe recipe to confirm fallthrough fires. The last blind fix here (`f218088` MaxStall floor) was reverted after validating all-red — **land behind a test, validate on the full integ matrix.**

## Devbox build/test recipe (also in memory: `blockcast-multicast-devbox-go-integration-test-recipe`)
```
cd ~/src/multicast   # (on a feature branch w/ WIP — use a worktree off origin/main; don't checkout there)
RQ="$PWD/fec-raptorq/c-bindings/target/release"
export LD_LIBRARY_PATH="$RQ:$LD_LIBRARY_PATH"; export CGO_LDFLAGS="-L$RQ"
go test -tags integration -run 'TestInitSegmentBug/Test_init_segment_round_0' -count=1 -v ./cmd/caddy/
```
`-tags integration` REQUIRED (cmd/caddy server tests are `//go:build integration`). Without the LD path: `libraptorq_c_bindings.so: cannot open`. Passing round ≈2.4s; stalled ≈61s.

## Tickets / tracks
- **BLO-9158** (Track A, this work) — full design in the 2026-06-07 comment; supersedes its old "fix direction (a)" (coverage-check is dead).
- **BLO-9310** (Track B, separate, do NOT conflate) — coherence-hardening: Vary-aware ROUTE `cache_key` + ETag-gate at `proxy.go:758` (`ContentLength != → ContentLength != || ETag !=`) + IMS revalidation. Closes the same-length-edit / Vary-variant gaps (pre-existing, orthogonal to the ghost).
- **Config must-verify (per deployment):** `recv_push_url` (durable-copy target, **defaults to Varnish** `trafficops.go:2485`) should co-locate with the recall target (**parent ATS10**) + cache_key parity (BLO-6812 scheme-norm) so the PUSH'd copy is recall-hot. If diverged: recall still correct via ATS→origin, but bypasses the copy (extra origin hit).

## Parked (independent)
- **#253** (recv-pump B.2, `blo-9052-recv-pump-worker`) — integ matrix flaky-red (2-for-2 on *different* test sets = flake, not regression; code is a verified legacy no-op). Merge path was undecided (re-roll for green vs admin-merge-over-flake vs stabilize-first). Stabilization = the integ-lane campaign (#254/#255/#256 + main); BLO-9158 is the dominant residual.
- **Coherence map** (3-layer: ROUTE/symstore length+TOI · cache-frontend ATS Vary/IMS/ETag · origin) is in the BLO-9158 comment.

## State (updated 2026-06-07 — Track-A implemented, in CI)
**Implemented + pushed as PR #260** (`Blockcast/multicast`): https://github.com/Blockcast/multicast/pull/260
- Branch `omar/blo-9158-read-before-commit` off `origin/main` (f519b3e), worktree `.worktrees/blo-9158-read-before-commit`. Commit `4adc304`, +135 lines, 3 files.
- **Pure predicate** `firstBodyReadCommittable(respLen, firstReadN)` in `servefound_fallback.go` (respLen<=0 ⇒ commit; >0&&N>0 ⇒ commit; >0&&N==0 ⇒ defer→recall), RED→GREEN unit-tested.
- **Wiring** in `plugin.go` right before the prepHeaders commit (worktree `:1929`): **FINISHED-scoped** read-before-commit gate. On 0-byte first-body read of a FINISHED tf → re-resolve via GetTransportObject (serve from populated instance if readable) → else `continue` → classifyStall → stallActionFallthrough → recall. Mirrors the existing alc.ERROR tf-swap.
- **Two design decisions taken** (vs the plan's open item-3): (a) **FINISHED-scoped** not unscoped — slow-fill NEEDSREPAIR path byte-for-byte unchanged; (b) **re-resolve then recall** not naive continue.

**Local validation (devbox loopback):** predicate tests + full receiver unit suite green; build+vet clean; **TestInitSegmentBug all 5 rounds ~2.2s — NO 30s-stall regression** (the make-or-break for FINISHED-scoping). TestReloadReceiverThenRequestProxy standalone + AllConfigsProxy file/TOIAfterServerRestart variants pass.

**Known-orthogonal flake (NOT a regression):** `TestAllConfigsProxy/...entity_*` reload subtests flaked locally (FEC recovery race on loopback → existing stallActionError). Proven unrelated: failing object was `state=RECEIVING` (FINISHED gate never runs), `deferring commit -> recall` fired 0×, **baseline origin/main passes these exact subtests**. Expect possible CI red here too.

**PR head `b875f51`** (advanced past my `4adc304`): `5f91398` = swap-path refine (MacBook session — on 0-read always `continue`, swap tf to a populated instance so next iter rebuilds resp, never commits stale empty resp); `b875f51` = throttle the no-swap ghost `continue` with the sibling `:1708` select{time.After(50ms);loopCtx;ctx} so the FINISHED-empty wait-for-recall doesn't CPU-busy-spin + hammer GetTransportObject(DB) for the stall window (scoped to `!swapped` so a successful swap re-iterates immediately). Local: build/vet/predicate green, TestInitSegmentBug round_0 ~2.4s no-regression.

**Open follow-up (BLO-9158, non-blocking):** latency-opt — on confirmed FINISHED-empty (no swap target), divert to fallthrough→recall immediately instead of waiting out the ~30s stall. The throttle bounds CPU but not the recall latency.

**1st CI run (4adc304):** feature-tests-{repair,mmt-fec}, unit-tests-race, wasm-build GREEN; integration-tests(file) RED = pre-existing `TestReloadReceiverFile` bad-nonce flake (`strconv.Atoi("")`) — orthogonal (my gate fired 0×, probe issues no HTTP); 4 shards never finished (slow fleet).

**NEXT:** watch PR #260 CI (now on `b875f51`) — the integ matrix under CI loss is the *only* place the live ghost reproduces (devbox loopback too clean). Ghost-relevant jobs: `integration-tests (AllConfigsProxy|entity|entity +doTrailer|file|file +doTrailer)`, `feature-tests-repair`, `feature-tests-mmt-fec`. Green there + ghost-probe fallthrough confirms the fix. If the entity flake reds, re-run / treat as orthogonal. **Do NOT land on local-green alone** (the last blind fix `f218088` was reverted after going all-red).
