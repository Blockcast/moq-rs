# M.4 next-session prompt — 2026-06-04 (BLO-8886 SHIPPED; remaining = BLO-8989 hang-side `just ci`)

> ## ✅ COMPLETE — 2026-06-05 (this whole doc is now closed out)
> **BLO-8989 → done.** hang `Check` (`just ci`) is green on hang `main` **`e3ddc0c4`**
> (PR **#110**, squash-merged). The post-#108 residual was NOT "remark/cargo" as guessed
> below — it was a single **`cargo fmt --all --check`** failure on
> `rs/hang/src/container/raw_consumer.rs` + `rs/libmoq/src/test.rs` (space-indent +
> unwrapped `assert!`, #109/#104 era). `cargo fmt --all` only; the fmt fix alone greened
> the full pipeline (Check run `26989285521`, all 24 steps, `nix develop --command just ci`
> incl. — doc/shear/sort/python/tofu/`nix flake check`/`test --all-features`/build were all
> already clean, source dirs untouched since the pre-#104 baseline).
>
> **Notes for the record:** pim NOT re-pinned (hang-internal formatting, decoupled). Devbox
> can't run the deep `just ci` layers (no nix/gstreamer/ffmpeg/python — only `cargo fmt` +
> `cargo sort` reproduce locally); pull hang job logs via
> `gh api repos/Blockcast/hang-mmt-fec/actions/jobs/<id>/logs` (`--log`/`--log-failed` are
> empty on the self-hosted runner). Full recipe saved to memory
> `hang-just-ci-devbox-debug-recipe.md`.
>
> **Optional guard spun out → BLO-9100** (backlog): prevent a dangling pim `file:` dep from
> merging green-less in hang (the #104 root cause). Per Omar: leave BLO-8989 closed, track
> the guard separately. **Nothing below remains actionable.**

---

**Resume:** `resume .planning/m4-next-session-2026-06-04-blo8989-hang-just-ci.md`

## TL;DR — what shipped
**BLO-8886 is DELIVERED.** pim #688 squash-merged → pim `main` = **`a1e68edd`**, pinning hang
**`45b38bb0`** (contains BLO-8151 + #104/BLO-8704 + #108/BLO-8886 quiche fix + BLO-8968).
The handoff's plan was stale (main had moved); the bulk of the work was **converging a fork**
+ landing **BLO-8989 Option B** (pim-side). All gating CI green; the quiche prebuild-404
docker-build flake is gone at source.

**Only one task remains: BLO-8989's *hang-side* — green hang's own `Check` (`just ci`).**

## ✅ Landed this session
- **hang #109 (BLO-8968) merged to hang main** — rebased onto `471e269a`, force-pushed, merged →
  hang main tip `45b38bb0` (single tip, all 4 fixes; resolved the off-main-pin fork #687 created).
- **pim #688 merged** (`a1e68edd`): submodule bump to `45b38bb0` + `.bundle-meta` + **BLO-8989 Option B**:
  - `scripts/ensure-cross-deps.sh` (layout-independent: builds `@blockcast/moq-transport` dist +
    self-symlinks `<parent-of-hang>/pim-multicast-gateway -> pim`; touches no tracked file).
  - Wired 4 consumers: `e2e-fec-scenarios`, `mmtp-e2e-test`, `moq-7point-validation`, `e2e-codec-matrix`.
  - Vendored `@blockcast/moq-transport` into moq-server `public/vendor` in e2e-fec-scenarios +
    codec-matrix (fixed the moq-lib `framesDecoded=0` 404 the #104 bump exposed).
  - `cross-deps-guard.yml` + retrigger.yml allowlist entry.
- **Linear:** BLO-8886 → **done**. BLO-8989 → pim-side done (comment). BLO-8969 owner notified the
  staging SHA changed (78db6747 → 45b38bb0; same watch diff, now on the #104 line).
- **Memory:** `memory/hang-submodule-bump-convergence-and-104-vendor.md` (the two recurring traps).

## ▶️ Remaining work — BLO-8989 hang-side (`just ci`)
hang's `Check` workflow (`check.yml` → `nix develop --command just ci`) has been red since #104.
#108 fixed the **bun/biome** layers (check.yml sibling clone + `bun.lock` regen + biome safe-fixes).
The residual is everything `just ci` runs **after** biome — never exercised before #108. Full recipe
(hang `justfile` `ci:` @ `45b38bb0`):
1. `just check --workspace` → `bun install --frozen-lockfile` + `bun run check` (biome ✓ #108) +
   cargo (check/clippy/fmt/doc/shear/sort) + **remark** (markdown lint) — cargo+remark untested.
2. Python: `uv run ruff check py/` + `ruff format --check` + `uv sync` + `maturin develop` + `pyright`.
3. tofu: `(cd cdn && just check)`.
4. `nix flake check`.
5. `just test --all-features`, `just build`, `cargo check --no-default-features` + `--all-features`.

So it's a **full CI run**, not just "remark + cargo" (the BLO-8886 handoff oversimplified). Expect
a multi-layer fix loop needing the full nix toolchain (rust, uv/python, maturin, tofu, nix).

### First actions next session
1. **Branch from hang `main` (`45b38bb0`)** in a hang checkout (NOT a pim submodule — needs siblings).
   Sibling setup like check.yml: clone `libmmt` + `fec-raptorq` + `pim-multicast-gateway` as siblings;
   build mmt-wasm + libmmt TS. (pim now has `scripts/ensure-cross-deps.sh` if you build via pim, but
   hang `just ci` runs in the hang repo with its own check.yml sibling setup.)
2. Run `nix develop --command just ci` (devbox has nix? verify — else run the sub-steps directly:
   `just check --workspace`, then ruff/pyright, then cargo edge-cases). Capture the **first** failing
   layer, fix, repeat. Likely candidates: cargo clippy/fmt drift, remark markdown lint on docs touched
   since #104, ruff/pyright on `py/`, or `nix flake check`.
3. Open a hang PR off main, green `Check`, merge (hang main is **unprotected** — but the GOAL is green,
   not bypass). Consider adding the "no dangling pim `file:` dep" guard noted in BLO-8989's description
   to prevent the #104 premature-merge root cause from recurring.
4. Close BLO-8989 when hang `Check` is green.

## Traps / context (carry forward)
- **No fleet-agent auth** — operate as **kkroo/board**. Issue CRUD via MCP with `BLO-####`. Board token
  at `~/.paperclip/auth.json`. `status=in_progress` needs an assignee → 422 under board token; `done`
  works on already-assigned issues (BLO-8886 had Omar assigned).
- **`gh` operates as Omar** (devbox auth). Merges/force-pushes run as Omar.
- **hang `main` is UNPROTECTED** (no required checks/reviews). hang PRs merge over red; #108 + #109 both
  did. **Ally does NOT review hang-mmt-fec PRs.**
- **hang submodule git dir** (for worktrees/inspection): `pim-multicast-gateway/.git/modules/hang-mmt-fec`.
  Worktree trap: a *pim worktree's* `.git/modules` doesn't exist — use the MAIN pim repo's path.
- **The two bump traps** (full detail in `memory/hang-submodule-bump-convergence-and-104-vendor.md`):
  (1) pim can get pinned to an off-main hang PR-head — converge by rebasing the PR onto hang main +
  merging. (2) bumping past #104 needs BOTH ensure-cross-deps (bun resolution) AND moq-transport
  vendoring (served moq-lib player) — else FEC scenarios red on a 404.
- **ci-gate** (pim) is an external aggregator (`ci-gate.yml` / `ci-gate-status`): fails if ANY peer
  workflow run for the head SHA is failure/cancelled/timed_out. New PR-bound workflows (workflow_dispatch
  + pull_request) MUST be added to `retrigger.yml` `workflow_run.workflows:` or `retrigger-allowlist-sync`
  reds the `check` job. "Require up to date with base" is ON → rebase + re-run before each merge.
- **moq-rs** (`blo-4020-m1` per project config; this devbox checkout on `blo-4020-m4-t1` @ `9c4818e`) —
  UNCHANGED all session. `origin = cloudflare` READ-ONLY. All work was pim + hang + Linear.
- **Worktree cleanup**: `/tmp/pim-8989-wt` (branch `blo-8886-bump-hang-submodule-8989`) is orphaned post-merge
  — safe to `git worktree remove`. The pim #688 remote branch was auto-deleted on merge.

## Verify
- `gh pr view 688 --repo Blockcast/pim-multicast-gateway --json state,mergeCommit` → MERGED `a1e68edd`
- `git ls-tree origin/main hang-mmt-fec` (pim) → `45b38bb0…`
- `gh pr view 109 --repo Blockcast/hang-mmt-fec --json state,mergeCommit` → MERGED `45b38bb0`
- `paperclipGetIssue BLO-8886` → done; `BLO-8989` → backlog, hang-side `just ci` open (see comments)
- hang `Check` on a fresh hang PR → currently red past the biome layer (the work)
