# M.4 next-session prompt — 2026-06-04 (BLO-8886 quiche flake: fixed at source, delivery blocked on #104 debt)

**Resume:** `resume .planning/m4-next-session-2026-06-04-blo8886.md`

## TL;DR — what happened
Picked up **BLO-8886** (CI flake: `@fails-components/webtransport-transport-http3-quiche`
prebuild-404 → flaky git-fallback → intermittent red `docker-build`). The one-line fix
(drop that package from hang's `trustedDependencies`) is **merged at source and proven
green in docker-build**, but **delivering it to pim's `docker-build` is blocked by a
pervasive #104 structural problem** that this work uncovered. The remaining work is a
new ticket — **BLO-8989** — already fully scoped + a decision made (Option B).

Also closed earlier this session: **BLO-8897** (Playwright cache `hashFiles` race, PR
pim **#684** MERGED → `9d2becf9`) and confirmed **BLO-8760** (QA verify migration) done.

## ✅ Landed
- **hang-mmt-fec #108 MERGED** → `471e269a`. Four commits:
  1. `edf4102a` — **the BLO-8886 fix**: drop `@fails-components/webtransport-transport-http3-quiche`
     from `trustedDependencies` (stops bun running its flaky native cmake build on
     `bun install` in the moq-server Docker `hang-js-builder` stage / `node:20-alpine`/musl).
     The binary is the Node server-side WT transport, used only by the `js/clock` example;
     no bundle the stage builds imports it.
  2–4. **#104-debt repairs needed just to build hang's CI green**: `check.yml` pim sibling +
     build moq-transport; regenerate `bun.lock` to record moq-transport (#104 never did);
     biome safe-fixes + a control-char `biome-ignore`. Merged **over hang's red `Check`**
     (hang `main` is **unprotected** — no required checks/reviews) since the residual layers
     (remark/cargo) are tracked separately.
- **BLO-8897 done** (pim #684, `9d2becf9`). **BLO-8760 done**. **BLO-8704 done**.

## 🔶 Held / the actual remaining work
- **pim #688** — OPEN/**draft**, BLOCKED. Bumps `hang-mmt-fec` submodule `5d36d2d4 → 471e269a`
  + syncs `packages/moq-server/public/js/.bundle-meta` (ran real `make watch`; watch bundle
  byte-identical, so only the SHA record changed; build-source gate passes).
  **docker-build (`build-images moq-server beta`) went SUCCESS on #688 — the BLO-8886 fix is
  PROVEN.** But `ci-gate` is RED because bumping the submodule past #104 trips #104's broken
  `moq-transport` `file:` dep in pim CI consumers that lack the workaround (see below).
  **#688 is reusable as-is once BLO-8989 lands** (rebase + re-run).
- **BLO-8989** (`backlog`) — **THE remaining work, fully scoped + decided (Option B).**
  Root: `hang-mmt-fec/js/lite`'s `@blockcast/moq-transport: file:../../../pim-multicast-gateway/packages/moq-transport`
  only resolves in **sibling/Docker layout**, not **submodule layout**. Every consumer that
  `bun install`s hang needs a per-consumer workaround; only 3 of ~9 have it.
  - **Has workaround:** moq-server `Dockerfile` (sed), `mmtp-e2e-test.yml` (sibling+build),
    hang `check.yml` (added in #108).
  - **Broken:** `e2e-fec-scenarios.yml` (the gating red on #688), `e2e-iwa-bridge.yml`,
    `iwa-bridge-multicast-e2e.yml`, `moq-7point-validation.yml`, `e2e-codec-matrix.yml`
    (verify), pim `make watch`, + **moqtail-private** likely (BLO-8704 migrated it — verify).
  - **DECISION = Option B** (not A). moq-transport is just the newest `file:`-based
    `@blockcast/*` cross-dep (libmmt's 5 packages + ssm-transport already work this way), so
    publishing just it is incoherent and publishing all of them is a separate org-wide
    initiative. **Implement:** one shared `ensure-cross-deps` setup step (consolidates the
    already-required libmmt + ssm + moq-transport sibling-build dance) + a **CI guard** that
    fails any workflow `bun install`-ing hang without calling it (kills the silent-regression
    mode that #104 hit). Full inventory + acceptance criteria + the A-vs-B rationale are in
    **BLO-8989's comments**.

## ▶️ First action next session
**Implement BLO-8989 Option B** (this unblocks #688 → BLO-8886):
1. Author `scripts/ensure-cross-deps.sh` in pim (build/resolve moq-transport + ssm-transport
   + libmmt mmt-* for whatever layout). Model it on the existing inline workaround in
   `.github/workflows/mmtp-e2e-test.yml` (~lines 290–355: builds the siblings, `sed`-rewrites
   the ssm-transport `file:` path).
2. Make every broken consumer call it (the 5 workflows above + `make watch`).
3. Add the CI guard (a check that greps PR workflows for `bun install` in hang without the
   setup step, or equivalent).
4. Verify by **rebasing pim #688 onto fresh main and re-running** — `ci-gate` should green
   (docker-build already does) → squash-merge → **BLO-8886 delivered, close it**.
   - **Caveat:** the e2e workflows also carry their own known-red scenarios (BLO-3323
     scenario 2/4). Confirm whether those gate `ci-gate`; if so they're a separate blocker.
5. Also finish hang's own `Check` (BLO-8989 original scope): after the moq-transport layers,
   `just ci` reaches **remark + cargo** checks (never ran before #108) — green those too.

## Traps / context (carry forward)
- **No fleet-agent auth** (`paperclipMe`/`inbox-lite` → 401). Operate as **kkroo/board**.
  Issue CRUD via MCP (`paperclipGetIssue`/`UpdateIssue`/`CreateIssue`/`AddComment`) with the
  `BLO-####` identifier works. **`status=in_progress` requires an assignee → 422** under the
  board token (can't self-assign). Company `aaced805-…`, project `f83f9a9a-…` (IBC DELTA),
  goal `d4e7ff70-…`. Board token at `~/.paperclip/auth.json` (`pcp_board_*`).
- **`gh` operates as Omar** (devbox SSH/gh auth), NOT the board token. Merges run as Omar.
- **pim `main`**: strict `ci-gate` + `enforce_admins=true` (no admin bypass). Sole required
  check = `ci-gate`. Always re-verify `headRefOid` and merge with
  `gh pr merge --squash --match-head-commit <SHA>` (atomic under a moving branch).
- **hang-mmt-fec `main` is UNPROTECTED** (branch protection 404 — no required checks/reviews).
  That's why #108 merged over its red `Check`.
- **BLO-3809 no-`synchronize`**: `check-bundle-freshness.yml` (pim build-source gate),
  hang `check.yml`, and others trigger on `[opened,reopened,ready_for_review]` only. To
  re-run them on a pushed head: post **`/test`** (pim `retrigger.yml`) or **toggle draft→ready**
  (`gh pr ready --undo` then `gh pr ready`). `mmtp-e2e-test.yml`/docker-build DO have
  `synchronize` (re-fire on push).
- **Build-source gate** (`check-bundle-freshness.yml`): any `hang-mmt-fec` submodule bump
  must regenerate the vendored bundle. It only checks `.bundle-meta` SHA == submodule SHA.
  `make watch` regenerates + writes it. **make watch trap:** hang's `file:` moq-transport dep
  breaks `make watch` in submodule layout — work around locally with
  `ln -sfn <pim-worktree> <pim-worktree>/pim-multicast-gateway` so
  `../../../pim-multicast-gateway/...` resolves to the worktree itself. Toolchain needed:
  bun (`~/.bun/bin`), cargo, wasm-pack, node, npm — all on devbox. Init submodules
  `libmmt fec-raptorq hang-mmt-fec` for the build.
- **Ally** (Paperclip Code Reviewer) is webhook/`@ally`-comment driven, NOT board/issue driven.
  Pinged on hang #108; she does **not** appear to review hang-mmt-fec PRs (no review landed).
- **moq-rs** (`blo-4020-m4-t1` @ `9c4818e`) — UNCHANGED all session; all work was in
  pim-multicast-gateway + hang-mmt-fec + Linear. `origin = cloudflare` READ-ONLY there.

## Verify
- `gh pr view 688 --repo Blockcast/pim-multicast-gateway --json state,isDraft,mergeStateStatus`
  → OPEN, draft, BLOCKED
- `gh pr view 108 --repo Blockcast/hang-mmt-fec --json state,mergeCommit` → MERGED `471e269a`
- `gh pr view 688 --repo Blockcast/pim-multicast-gateway --json statusCheckRollup` →
  `build-images (moq-server …beta)` = SUCCESS; `checkout-repos` (E2E FEC Scenarios) + 7-Point = FAILURE
- `paperclipGetIssue BLO-8989` → backlog; scope + Option-B decision in comments
- `paperclipGetIssue BLO-8886` → fix-at-source done, delivery held comment
