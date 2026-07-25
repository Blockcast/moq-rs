# M.4 next-session prompt — T1.6 close-out + Sub-project C (2026-06-01)

> **UPDATE 2026-06-02 (T1.6 seam-prep SHIPPED + build-source gate greened):**
> **BLO-8646 done** — the Shaka WT-connect seam-prep **PR #8** squash-merged to
> `blockcast/main` (`f9d42e7d8`): `buildWebTransportOptions_`, centralized ALPN
> (`moqt-16`/`moq-00`) + DRAFT_14 policy, `msf_transport_unit.js`.
> - **`blockcast/main` was secretly red on the build-source gate from PR #5's MMTP
>   code** (PR #5 merged on UNSTABLE). `build/all.py` (the `build-base`/`build-head`
>   CI) caught 3 stacked layers that `build.py +@complete` does NOT run: ~70 ESLint,
>   16 cspell words, **91 Closure test-type errors**. All fixed in **PR #9**
>   (`3d4c671d9`) → main green. **Lesson: `build.py --name experimental +@complete`
>   compiles but skips lint/spelling/test-type — it is NOT the gate. Run
>   `python3 build/check.py` (or `build/all.py`) before trusting green.**
> - **Real defect found + fixed:** the seam-prep set `options.protocols`, which is not
>   a `WebTransportOptions` field (no-op ALPN). Flagged on BLO-8646; ME removed the dead
>   path (`e6d9a6825`) — not papered over.
> - **CI shape (carry forward):** shaka PR-bound checks are `build-base` (builds the PR
>   base) + `build-head` (builds the merged tree) + `Validate PR Title` (semantic-PR;
>   needs conventional title) + `compare`. These are **non-required** (you can merge on
>   UNSTABLE), but a code PR is only truly green when `build-head` passes. When a PR's
>   base advances, its CI does NOT auto-re-run — merge current base in + push to re-trigger.
> - **Next T1.6 work:** **BLO-8704** (cross-player `@blockcast/moq-transport` pkg +
>   hang/moqtail migration) — `blocked`, promotable now that 8646 landed. Also open:
>   BLO-8717 (FFmpeg image bake), recovery-infra BLO-8676/8677 (control-plane, not deliverables).
>
> ---
>
> **UPDATE 2026-06-02 (Sub-project C SHIPPED — shaka PR #5 merged):** C (**BLO-8702**)
> is **done** (completed 04:49Z, evidence verdict `pass`). Shaka **PR #5** squash-merged
> into `blockcast/main` at 05:39Z → tip **`c631b0d56`** `feat(msf): MMTP AAC receiver +
> shared-epoch A/V sync (#5)`. **Heads-up: PR #5 landed the whole MMTP stack (T1.2 video
> + T1.7 + C), not just C.**
> - **Reconciliation:** the PR branch was 2 behind `blockcast/main` because **PR #3**'s
>   T1.7 observe path landed in parallel (9 `add/add` conflicts on the same msf files).
>   Resolved by merging `origin/blockcast/main` into the branch with **`-X ours`** (the
>   branch is the verified superset: +1299/−67 over PR #3, incl. msf_parser.js +488 for
>   T1.2 video), merge commit `021d5e119`, then squash. Net new the merge pulled in = one
>   file (`demo/msf-minimal.html`, PR #2).
> - **Correctness catch:** PR #3 parsed `SourceFecPayloadId` **right after the MMTP
>   header** (wrong); PR #5 (`847e6abcb`) corrects it to a **packet trailer** per the
>   `moq_mmt` wire format. `blockcast/main` now carries the fix.
> - PR title was non-conventional → renamed; **Validate PR Title** green.
> - **Verified from source on the merged tree:** `build/build.py --force --name
>   experimental +@complete` = **0 errors**; `build/test.py --no-build --quick --browsers
>   ChromeHeadless --filter Mmtp` = **27/27**.
> - **Opus stays deferred:** BLO-8705 (`backlog`), BLO-8713 (`blocked`), BLO-8714 (`backlog`).
> - **Next-session watch:** (1) **BLO-8704** (cross-player `@blockcast/moq-transport`
>   package + hang/moqtail migration) was gated "after C" — now promotable `backlog→todo`.
>   (2) **BLO-8646** seam-prep PR and **BLO-8717** (FFmpeg image bake) still pending —
>   monitors `bjuwig22l` + `b7raemf88` armed. (3) C's audio E2E recapture
>   (`.planning/m4-t1.7-e2e/moq_mmt_capture_av.json`, commit `5174596`) also satisfies
>   BLO-8644's last acceptance bullet — fold when standing up the live audio E2E.
>
> ---
>
> **UPDATE 2026-06-01 23:39 (orchestration session):** Both threads are now in motion
> and the T1.6 scope was **reduced** per owner decision. Current truth:
> - **C (BLO-8702)** is `in_progress` with a **live ME run** — ME self-advanced to it
>   and autonomously opened **BLO-8705** (Opus follow-up, satisfies C acceptance #4).
>   This is the demo-critical path; let it run, don't interrupt.
> - **T1.6 (BLO-8646)** reduced to **just the Shaka-side WT-connect de-dup** ME already
>   did (uncommitted in the shared shaka tree). Now `todo`: land that seam-prep as its
>   **own** shaka PR, then `in_review`. Flipped out of the false `in_review` (it was
>   re-triggering the BLO-8676 recovery loop — no artifact existed to review).
> - **Cross-player `@blockcast/moq-transport` package + hang/moqtail migration** split
>   to **BLO-8704** (`backlog`, medium) — sequence AFTER C; promote to `todo` when C lands.
> - **Next-session watch:** (1) did ME land the BLO-8646 seam-prep PR? (2) is C
>   progressing through WA1→WA2→WA3 (audio path → shared-epoch sync → audio E2E recapture)?
>   (3) the audio E2E recapture also closes BLO-8644's last acceptance bullet — shared work.
>
> **UPDATE 2026-06-02 00:52 (fixture unblock):** C hit a real blocker — WA3 needs an
> audio-bearing MMTP capture (`packet_id=2`), and the fleet towered on provisioning the
> `moq_mmt` ffmpeg toolchain inside distroless k8s (8702→8706→8707, all blocked). Devbox
> already has that ffmpeg (`FFmpeg/build-native/ffmpeg`), so the fixture was produced +
> landed here: **`moq-rs:.planning/m4-t1.7-e2e/moq_mmt_capture_av.json`** (commit `5174596`,
> branch `blo-4020-m4-t1`; 436 pkts, pid1 video + pid2 AAC; generator `make_av_capture.sh`,
> note `capture-av-fixture.md`). Tower collapsed: **BLO-8706 done, BLO-8707 cancelled,
> BLO-8702 → todo** (ME to resume WA3). Gotchas for WA3: catalog `channelConfig` must be a
> **string**; keep the audio fixture paired with the 2-track catalog (a video-only catalog
> + pid2 breaks the publisher's video path); full audio→Shaka still needs ME's WA1 branch.
> Finding for B/8644: muxer emits audio Init **once**, not resent per video keyframe.
>
> Everything below is the original (pre-update) handoff; the package-name gate it
> describes is **approved + recorded**, and the "start with T1.6" sequencing is superseded
> by the reduction above.

**TL;DR:** B's live T1.7 E2E is GREEN and the whole 4057/framerate blocker chain is
closed (see prior handoff + BLO-8644). Two threads remain: **T1.6** (WebTransport
factory — blocked only on a package-name approval gate) and **Sub-project C**
(receiver audio + A/V sync — unblocked now that B's C-facing contract is locked, no
issue exists yet). Start with T1.6 (cheap unblock), then scope/open C.

## Repos / traps (carry forward)
- moq-rs: `origin = cloudflare` is **READ-ONLY** → push to **`blockcast`** remote; `gh` MUST use `--repo Blockcast/moq-rs`.
- shaka-player: `origin = Blockcast/shaka-player` (no trap). It's a **submodule** of pim-multicast-gateway.
- **Worktree state left by the last session** (changed from the handoff baseline):
  moq-rs is on **`blo-4020-m4-t1`** (PR #6, tip `afe82a4`); shaka submodule
  (`../shaka-player`) is on **`blo-4020-m4-t1-shaka-mmtp`** (PR #5, tip `b5c9066f0`).
  The old `blo-4020-m4-session-handoff` / `blo-4020-m4-t1.5c-ntp-wrap` branches are intact.
- Live E2E (now green, asserts playable-stream contract): `cd moq-rs && bash .planning/m4-t1.7-e2e.sh`.
- Shaka unit: `cd shaka-player && python3 build/test.py --no-build --quick --browsers ChromeHeadless --filter 'shaka.msf'`.

## Paperclip fleet
- Project: **IBC Accelerator DELTA** `f83f9a9a-8857-4dc8-8c90-fb4ac179957a`.
- Implementer: **MulticastEngineer** `cd284f1d-…` (Codex pool, `maxConcurrentRuns:1` → serial; it queues/thrashes when blocked, so unblock decisively).
- Reviewer: **Ally** = GitHub `blockcast-ci-packages` (fires on PR open/ready/reopen + issue-comment).
- CEO/lead: `4eca1725-…`. CTO/infra agent: `386c81e8-…`.
- **Standing rule (Omar):** merge clean-Ally doc PRs into the integration branch without per-PR confirmation. Code/package PRs are NOT covered — confirm landing.

## Thread 1 — T1.6 close-out (BLO-8646, `in_review`)
**Status:** package-name gate **APPROVED 2026-06-01** → `@blockcast/moq-transport`
(confirmation `8e15e01c` resolved `superseded_by_comment`; ME woken to proceed). ME
had already done the Shaka-seam prep (extracted `buildWebTransportOptions_` in
`lib/msf/msf_transport.js`, centralized ALPN `moqt-16`/`moq-00` + DRAFT_14 policy,
added `test/msf/msf_transport_unit.js` assertions). `@blockcast/transport` is taken by
`packages/ssm-transport`; `@blockcast/mmt-transport` = the mode-manager — hence the name.
- Stale dup confirmation `274735f8-…` is still `pending` (not API-resolvable; cosmetic — decision is recorded).

**Next steps (ME executing; verify on next session):**
1. ~~Approve the gate~~ **DONE** — ME should now be scaffolding + migrating.
2. ME scaffolds the **minimal** package: raw ready-`WebTransport` + negotiated ALPN
   string only (de-dup, NOT a session-level abstraction — genuinely-shared code ends
   at `wt.ready`; the 3 players have incompatible session layers). Migrate Shaka (via
   its existing `transportFactory` seam — Closure can't import the ESM module), hang/
   moq-lib, and moqtail; de-dup the 3× copy-pasted hex→bytes decode.
3. Watch for a **dedicated `@blockcast/moq-transport` PR** — none exists yet (the seam
   prep may be uncommitted in ME's workspace or folded into shaka PR #5). Ensure it
   lands as its own reviewable unit; import-map externalization for the ESM consumers.
4. Reliability noise to ignore: BLO-8676 (recovery-ownership churn for repeated
   `issue_checkout_conflict` on 8646) — control-plane, not the deliverable.
- Scoping: `.planning/m4-scoping/T1.6-transport-factory.md` (per-player file:line ledger).

## Thread 2 — Sub-project C (receiver audio + A/V sync) — **OPEN as BLO-8702** (ME, `todo`)
Created 2026-06-01 with the full scope below baked into the issue description.
**Unblocked by:** B's **locked** C-facing audio contract (BLO-8644 document
`c-facing-audio-contract`). Key invariants the receiver must depend on:
- Audio track `audio` (repair `audio/repair`); `packaging=container=mmtp`;
  `selectionParams.codec=mp4a.40.2` (AAC-LC); `samplerate`/`channelConfig` from stream
  metadata (no fallback); `initData` from `ftyp+moov`.
- Wire: audio = MMTP **packet_id=2**, video = packet_id=1; MFU `FT=2`, Init `FT=0`;
  audio Init resent on the video-keyframe init-resend (late-joiner safe).
- **Shared A/V clock (the crux):** both tracks stamp
  `timestamp = us_to_ntp_short(base_pts_us + sample_offset_us)`. Receiver must treat
  MMTP timestamps as **media PTS, not arrival time**, and compare A/V on this shared epoch.
- AAC-only this milestone; **Opus deferred** (open a follow-up ticket).

**Concrete receiver work (scope into the C issue):**
1. **Shaka MMTP audio path:** `processMmtpTrack_` (`lib/msf/msf_parser.js:566`)
   currently hard-asserts video only — `goog.asserts.assert(detectedType === VIDEO,
   'MMTP audio not yet supported (Sub-project C)')`. Add the AAC audio branch:
   audio frameDuration = `1024 / track.samplerate` (already in
   `loc_parser.js:367`, `frameDurationFromTrack`), build an audio Stream + segment
   index analogous to the video path, transmux AAC.
2. **A/V sync (TIER-2 shared-epoch):** the players already have the pattern —
   moq-lib `Sync` (shared reference timestamp, both tracks wait against it) and
   moqtail `MediaSync`. Wire audio+video to the shared `base_pts` epoch from the
   contract. CLAUDE.md anti-patterns: audio+video MUST share one MediaSync reference
   (independent pacers drift); no `setTimeout(0)` in data pumps; AudioWorklet FIFO
   capacity derived from catalog FEC params, not hardcoded.
3. **T1.5c clamp caveat (carry forward):** `W = min(computeFecTimeout, targetLatency)`
   defeats FEC recovery when block span > targetLatency — safety-net only; warn + use
   sane FEC params. (`.planning/m4-scoping/T1.5c-timing-avsync.md`.)
4. **Extend the live E2E for audio:** the harness catalog (`.planning/m4-t1.7-e2e.sh`)
   is video-only (`track v`, packet_id 1). For an audio E2E, add the `audio` track
   (packet_id 2) + a capture carrying packet_id=2 (the current
   `moq_mmt_capture_full.json` is video-only; recapture with an audio input per the
   `ffmpeg-moq-mmt-multicast-wire-format` recipe). This is also BLO-8644's last
   acceptance bullet ("audio MMTP publishes end-to-end") — coordinate so closing 8644
   and standing up C's E2E share the recapture.

**C is open as BLO-8702** (assignee MulticastEngineer, `todo`) with Work areas 1–3 +
acceptance in the description. Split into receiver-audio (WA 1+3) vs A/V-sync (WA 2)
lanes if too large for one serial run.

## Status snapshot (after last session)
| Item | Issue | State |
|---|---|---|
| B publisher audio | BLO-8644 | unblocked; PRs #5/#6 landed+green; audio-E2E acceptance bullet open → fold into C (BLO-8702) recapture |
| CQ#1 framerate | BLO-8680/8681 | **done** (satisfied) |
| WT 4057 / relay bring-up | BLO-8688/8689/8690/8691/8695 | **done** (env-only) |
| T1.6 WT factory (reduced) | BLO-8646 | **`done`** — seam-prep **PR #8** squash-merged to `blockcast/main` (`f9d42e7d8`); `protocols` no-op defect removed (`e6d9a6825`). Build-source gate greened via **PR #9** (`3d4c671d9`: lint+spelling+91 type errors). |
| T1.6 cross-player package | **BLO-8704** | `backlog`, medium, ME — greenfield `@blockcast/moq-transport` + hang/moqtail migration; **C landed → now promotable `backlog→todo`** |
| Sub-project C | **BLO-8702** | **`done`** — shaka **PR #5** squash-merged to `blockcast/main` (`c631b0d56`, 2026-06-02); whole MMTP stack (T1.2+T1.7+C); build +@complete 0 errors, Mmtp 27/27 |
| Sub-project C — Opus | BLO-8705 | `todo`, medium, ME — deferred, not built this milestone |
