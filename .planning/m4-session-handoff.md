# M.4 session handoff — post-T1.5c-1.B + 4-gate resolution (2026-06-01)

> **SUPERSEDED 2026-06-02** by `m4-next-session-prompt.md` (live rolling handoff). Since
> this doc was written: **B (BLO-8644) E2E went green**, and **Sub-project C (BLO-8702)
> SHIPPED** — shaka **PR #5** squash-merged to `blockcast/main` (`c631b0d56`); it landed
> the whole MMTP stack (T1.2 video + T1.7 + C), build `+@complete` 0 errors, Mmtp 27/27.
> Open M.4 work now: **BLO-8704** (cross-player `@blockcast/moq-transport` pkg, promotable
> after C), **BLO-8646** (T1.6 seam-prep), **BLO-8717** (FFmpeg image bake).

**TL;DR:** T1.5c-1.B (NTP-wrap unwrap) shipped. All 4 post-T1.2 targets scoped,
recommended, and decided/signed-off. MulticastEngineer (paperclip) then
implemented **T1.FEC** and **T1.5c-1.A**; **T1.6** is in review; **B** is blocked
on the live E2E (two root-caused infra gates, fixes routed). Resume by checking
whether those two gates cleared and B's E2E went green.

## Repos / traps (carry forward)
- moq-rs: `origin = cloudflare` is **READ-ONLY** → push to **`blockcast`** remote; `gh` MUST use `--repo Blockcast/moq-rs`.
- shaka-player: `origin = Blockcast/shaka-player` (no trap).
- Integration branch: **`blo-4020-m4-t1`** (tip `be2781eb`). All M.4 doc/decision PRs merged here.
- Test: `cd shaka-player && python3 build/test.py --no-build --quick --browsers ChromeHeadless --filter 'shaka.msf'`.
- Live E2E: `cd moq-rs && bash .planning/m4-t1.7-e2e.sh`.

## Paperclip (the implementation fleet)
- Project: **IBC Accelerator DELTA** `f83f9a9a-8857-4dc8-8c90-fb4ac179957a` (workspaces incl. moq-rs/shaka/libmmt/FFmpeg-via-pim).
- Implementer: **MulticastEngineer** `cd284f1d-...` (gpt-5.3-codex, Codex pool, `maxConcurrentRuns:1` → serial).
- Reviewer: **Ally** = GitHub `blockcast-ci-packages`. Fires on PR open / ready_for_review / reopen **AND on issue-comment** (comment-tag triggers a review — validated). Serial; slow under backlog.
- **Standing rule (set by Omar):** merge doc PRs into `blo-4020-m4-t1` on a clean Ally review (fix nits, hold on blockers) without per-PR confirmation.

## Target status
| Target | Issue | State | Notes |
|---|---|---|---|
| T1.5c-1.B NTP-wrap | — | **SHIPPED** | shaka #6 merged, Ally-clean, shaka.msf 149/149 |
| **T1.FEC** SS_ID trailer | BLO-8645 | **DONE** | libmmt #51 + moq-rs #11(revendor)/#13(FEC-ON) + shaka #7, all merged. **libmmt API signed off by Omar.** |
| **T1.5c-1.A** reorder buffer | BLO-8647 | **DONE** | msf_parser.js W=`computeFecTimeout`, `targetLatency` ceiling, `fecInterleaveDepthMs` field; MSFParser 8 / msf 124 green |
| **T1.6** WT factory | BLO-8646 | **in_review** | name decided → `@blockcast/moq-transport` (NOT `@blockcast/transport`=ssm-transport, NOT `@blockcast/mmt-transport`=mode-manager) |
| **B** publisher audio | BLO-8644 | **DONE** | live E2E green; last audio-E2E bullet folds into C's recapture (`moq_mmt_capture_av.json`, `5174596`) |
| **C** receiver audio + A/V sync | BLO-8702 | **SHIPPED** | shaka PR #5 → `blockcast/main` (`c631b0d56`); whole MMTP stack; build +@complete 0 err, Mmtp 27/27; Opus deferred (8705/8713/8714) |

## Active blocker chain (B's E2E) — both root-caused + routed to MulticastEngineer
`BLO-8644 → BLO-8680 (framerate, CEO) → BLO-8681 (impl, ME) → BLO-8689 (WT-validate, ME)` + `BLO-8688 (WT 4057, CTO)`.

1. **WT init 4057 (8688/8689) — fix known:** harness relay built from `blo-4020-m1` = `web-transport-proto 0.5.2` (deprecated draft Chrome 146 rejects). Fix on `blo-4020-m4-t1` = `0.6.0`. → rebuild harness relay from a 0.6.0 branch. NOT a URL/flags issue (8689 was mis-scoped). Do this FIRST.
2. **CQ#1 framerate (8680/8681) — not a decision, a propagation gap:** contract already exists — catalog `framerate: Option<u64>` (`moq-catalog/src/lib.rs:531`) ← muxer `moq_publish_set_framerate` (`moqenc_mmt.c:2956`) → Shaka `track.framerate` (`msf_parser.js:538`). The harness's published catalog just isn't carrying `framerate`. → capture the published catalog JSON, fix the producer (replay/serve.py path or the muxer WARNING-on-fractional-fps path).

Both posted as comments on the issues + `interrupt` wakes on BLO-8689/BLO-8681.

## Open sub-items (non-blocking, in the docs)
- T1.5c-1.A: anchor forward-jump clamp + negative-`startTime` tolerance (Ally #6 follow-ups) — folded into BLO-8647.
- T1.5c clamp caveat: `W=min(computeFecTimeout, targetLatency)` **defeats FEC recovery** when block span > targetLatency → safety-net only; warn + use sane FEC params.
- Opus on MMTP: deferred follow-up ticket (AAC `mp4a.40.2` only for now).
- Ally audit on moq-rs #1–5: comment-tagged, draining serially (not gating anything).

## Next session
1. Check B's E2E: did 8688 (WT 0.6.0 relay) + 8680 (catalog framerate) clear → 8644 green?
2. T1.6 (BLO-8646) review close-out → `@blockcast/moq-transport` package land.
3. ~~Sub-project C (receiver audio + A/V sync)~~ **DONE 2026-06-02** — BLO-8702, shaka PR #5 merged (`c631b0d56`); TIER-2 shared-epoch sync landed. Opus deferred (8705/8713/8714).
4. Scoping docs live in `.planning/m4-scoping/{B-publisher-audio,T1.5c-timing-avsync,T1.FEC-trailer-fix,T1.6-transport-factory}.md`.
