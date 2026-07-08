# Next-session prompt — 2026-06-05 (adapter incident RCA + receiver-flake fix + Ally/Codex fleet-down)

**Resume:** `resume .planning/next-session-2026-06-05-adapter-flake-ally.md`

> Context this session started from BLO-8989 (hang `just ci`) — **that shipped early** and is unrelated to
> what follows. The bulk was: (1) RCA'ing the opencode_k8s "adapter_failed" board escalation, (2) advancing
> BLO-6020 via PR #253 triage + a receiver flake fix (#254), (3) trying to get Ally to review #254 and hitting
> a structurally-down review pipeline. Two items genuinely need a **human/ops** and gate everything else.

## ✅ Shipped / done this session
- **BLO-8989** → done. hang `Check` green on hang `main` **`e3ddc0c4`** (PR hang#110, fmt-only). pim NOT re-pinned (decoupled). Optional guard spun out → **BLO-9100** (backlog).
- **Adapter incident RCA** (board approval `9ab53bea`): split into **two** faults, corrected the CEO's conflation.
  - **Failure A** (85×, opencode_k8s only) = **expired Codex ccrotate creds** (refresh 400 / burned refresh_token). Ticketed **BLO-9120** (HIGH, backlog). Needs **ops reauth** (`codex login --device-auth` via auth-bot). The auth-bot's automated `relogin-magic.py` is itself broken (OpenAI login selector drift + magic-link poll timeout).
  - **Failure B** (`Process adapter missing command`, 70×, both adapter types, 2 tenants) = run dispatched on the `paperclip-api` tier, which doesn't load bundled k8s adapters → `getServerAdapter()` falls back to the `process` adapter. **Already fixed on paperclip master by #302 (`6d20bfb7f`)** — my draft fix paperclip#300 was redundant, **closed**. Ships only when the agent image rolls past `6d20bfb7f` (deployed image `sha-05ae9dc-k8s-vendored` predates it).
- **PR Blockcast/multicast#254** (open, off `main`, branch `blo-6020-flake-multiperiod-poll`): test-only fix — `TestPullMultiperiod` now polls its P1 segments via `waitForReceiverObject` to close the **path-vs-TOI 404** flake (`segment_11_11`→404→`expected 0, actual 1025`). gofmt-clean, `go test -c ./cmd/caddy/` compiles. **CI still red** on the *sibling* flakes (see below). `claude` bot reviewed COMMENTED; **Ally never reviewed**.
- **PR Blockcast/multicast#253** (BLO-9052, `in_review`): RCA posted — its red integ shards are the **pre-existing flake, NOT a regression** (the PR is a legacy-mode no-op; only affects opt-in pump-consumer). Re-triggered integ-tests via `/test` (run `27006455888`). Reviewer's call to merge.
- **BLO-9137** filed + assigned to **Ally** (review #254) — the reliable wake path. **No response** (Ally down).

## 🚧 The two human/ops blockers (gate the rest — NOT agent-actionable from devbox)
1. **Roll the paperclip-agent image past `6d20bfb7f`** (currently `sha-05ae9dc-k8s-vendored`). Fixes Failure B fleet-wide → unblocks **Ally's reviews** and every claude_k8s run that lands on the API tier.
2. **Reauth the Codex ccrotate pool** (BLO-9120) → unblocks opencode_k8s agents (CTO, MulticastEngineer). Dominant recovery-flood driver. Also fix `relogin-magic.py` so the pool self-heals.

Until #1, **Ally cannot review any PR** (BLO-9089 webhook gap + un-rolled image). Don't keep pinging — it's structural.

## ▶️ Receiver-flake reality (BLO-6020) — important, don't repeat the dead-end
- Only **`TestPullMultiperiod`** was test-fixable (transient path-vs-TOI 404 → polling fixes). **#254 does this.**
- **`TestReloadReceiverFile` / `TestCMAFLossPreStream` / `TestTOIAfterServerRestartProxy`** (`recovery_test.go`/`cmaf_test.go`) flake on the **`unexpected EOF nonce=` UDP-overrun** race — packets genuinely dropped under load; a `waitForReceiverObject` poll **cannot** fix them (documented on #254 + BLO-9052). Their real fix is the **BLO-6020 recv-pump refactor itself** (line-rate drain) — i.e. landing **#253 + its phases**, not test patches.
- So **green receiver integ CI is gated on BLO-6020 landing**, not on hardening tests. #254 is a small correct orthogonal win (one fewer test-level flake).

## First actions next session
1. **Check the two ops blockers first** — has the agent image been rolled past `6d20bfb7f`? Is the Codex pool reauthed (BLO-9120)? If yes → Ally reviews should flow; re-ping #254/#253 or just confirm Ally picks up BLO-9137. If no → flag to Omar/ops again; everything review-gated stays stuck.
2. **#254**: if Ally's alive, get the review + merge (test-only, can't regress; `claude` already COMMENTED). Its red integ shards are the sibling flakes — expected; merge on the green non-integration checks + Ally OK, or admin-merge per repo norms. Don't try to green the integ shards via test patches.
3. **#253 (BLO-9052)**: confirm the re-run (`27006455888`) and merge if a re-run clears the flake (reviewer's call). This is the actual path to fixing the sibling flakes long-term.
4. **BLO-9073 / BLO-8990**: still blocked — BLO-9073 needs the codec-matrix CI browser runner (not devbox-reproducible); BLO-8990 hard-blocked on BLO-8974/9114 (1a packetizer, `in_progress`). Skip unless unblocked.

## Traps / context
- **No fleet-agent auth** — operate as **kkroo/board** via MCP. Board token at `~/.paperclip/auth.json`. `gh` in `Blockcast/multicast` auths as **`kkroo`** (collaborator); paperclip/hang as Omar. Comments post as Omar (`oAfDyNGXF5wi8ozojnxTheOPYGFsWDDQ`).
- **Ally = GitHub `blockcast-ci-packages`** (rename to `ally` in flight). On a PR, a `claude`/`claude[bot]`/`greptile-apps` review is **NOT** Ally.
- **multicast repo**: default branch `main` (NOT master); remote `origin`; needs CGO + `fec-raptorq` submodule to build (present locally at `/home/oramadan/src/multicast`). `go1.25.6`.
- **paperclip repo** default branch **`master`**; receiver-tier model: workers/statefulset (`paperclip-0`) owns adapter lifecycle, `paperclip-api` replicas skip it (the Failure-B locus).
- **Devbox can't run** nix / gstreamer / ffmpeg-dev / pyright / the codec-matrix browser — CI is the harness for those. Pull self-hosted job logs via `gh api repos/<owner>/<repo>/actions/jobs/<id>/logs` (`gh run view --log` returns empty).
- Approvals queue (board): 6 pending, all human-gated (AWS RDS, Cloudflare, Play Integrity cred, 3 privileged-infra applies + governance). **Don't rubber-stamp** — Omar's call.

## Verify
- `gh pr view 254 --repo Blockcast/multicast --json reviews,statusCheckRollup` → claude COMMENTED; integ shards red (sibling flakes); look for a `blockcast-ci-packages` review.
- `gh pr view 253 --repo Blockcast/multicast --json state,statusCheckRollup` → in_review; check run `27006455888`.
- `paperclipGetIssue BLO-9120` (Codex creds) / `BLO-9089` (Ally review) / `BLO-9137` (review request) — are they progressing?
- Agent image: `kubectl -n paperclip get agents`-equiv via `mcp__paperclip__paperclipListAgents` → check `adapterConfig.image` rolled past `sha-05ae9dc`.
- `gh pr view 300 --repo Blockcast/paperclip` → CLOSED (superseded by #302). hang#110 → MERGED `e3ddc0c4`.
