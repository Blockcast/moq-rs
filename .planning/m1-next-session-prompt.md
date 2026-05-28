M.1 + most of M.1b of BLO-4020 (Cloudflare moq-rs MMTP migration) is COMPLETE,
plus M.4 ADR drafted with locked A0-A3 decisions. Four open PRs stacked on
`blockcast/blo-4020-m1`; one fork-only branch with the M.4 ADR; one upstream
issue draft awaiting external filing; one M.1b sub-task deferred. Working
directory: /home/oramadan/src/pim-multicast-gateway/moq-rs

State as of 2026-05-28 (M.1 + B1+B3+B4 closed; B2 deferred to M.4; pick M.2 / upstream / receiver next):

Branch on fork: https://github.com/Blockcast/moq-rs/tree/blo-4020-m1
Local main:     tracks blockcast/blo-4020-m1 (10 commits over upstream)
Paperclip tracking:
  - BLO-4020 (umbrella):  https://paperclip.blockcast.net/BLO/issues/BLO-4020
  - BLO-8047 (M.1b umbrella, sub of BLO-4020): same path, /BLO-8047

OPEN PULL REQUESTS
==================
All four PRs stacked on `blo-4020-m1` (PR #1 is the M.1 base). PR #1 must merge first;
then #2/#3/#4 rebase onto blockcast/main and merge in any order. They are independent.

  PR #1: BLO-4020 M.1: MMTP publisher on IETF moq-transport (draft-14+)
         https://github.com/Blockcast/moq-rs/pull/1
         10 commits, 69 unit tests + smoke green. T1-T9 landed (T8 N/A per pushback).

  PR #2: BLO-8047 §B1: raw-passthrough fragmentation contract (stacked on #1)
         https://github.com/Blockcast/moq-rs/pull/2
         1 commit (ec8e4b7). 6 files, +431 / -20. Adds 2 characterization tests,
         synth_mmtp --fragment flag + unit test, m1-smoke FRAGMENT=N env knob,
         ADR amendment, results doc. Smoke PASS at FRAGMENT=0 (regression baseline,
         hashes identical to M.1 results) and FRAGMENT=3 (new contract, 960 B/track).

  PR #3: BLO-8047 §B3: object_id_delta wire-encoding bug forensics (stacked on #1)
         https://github.com/Blockcast/moq-rs/pull/3
         1 commit (1cf8bb4). 1 file, +140. Docs-only forensics doc proving the bug
         at moq-transport/src/session/subscribed.rs:281 ("object_id_delta: 0, //
         before delta logic"). Includes mlog evidence + suggested upstream patch.

  PR #4: BLO-8047 §B4: G6 byte-diff vs libmoq — wire formats diverge by design
         https://github.com/Blockcast/moq-rs/pull/4
         1 commit (8c404c9). 1 file, +138. Docs-only static code comparison
         between cast/moq_lite and moq-pub-mmtp/IETF. Confirms B2 is moot for the
         cast path; M.4 receiver migration is a major rewrite.

PRs are stacked-on-#1. When PR #1 merges:
  1. Rebase main onto blockcast/main: git fetch blockcast && git rebase blockcast/main
  2. Rebase #2/#3/#4 onto blockcast/main: git checkout blo-4020-m1b-frag &&
     git rebase blockcast/main && git push -f blockcast HEAD:blo-4020-m1b-frag
  3. Repeat for blo-4020-m1b-obj-id-delta and blo-4020-m1b-g6-bytediff.

THIS SESSION'S WORK (2026-05-28)
=================================
- B1=C closed by architectural pushback (same shape as T8): MMTP fragmentation
  reassembly stays at the receiver via mmt-core::MfuReassembler (already vendored).
  Dimensional math: AMT MTU ≈ 1416 B per fragment → 4K I-frames need 220-1100
  fragments, 8K needs 750-2900. Erroring on FI != 0 would reject all video above
  1080p audio. Receivers (moqtail @moq/hang, Shaka via WASM) already do reassembly.
  Pinned by tests + ADR amendment + smoke at FRAGMENT=3. PR #2.

- B3 forensic confirmation of Codex #6: moq-transport's publisher hardcodes
  object_id_delta=0 for every wire object. Bug masked by moq-relay-ietf's egress
  next_object_id auto-increment; affects direct publisher-to-subscriber topology.
  Upstream issue drafted (in BLO-8047 description) but NOT filed — to be sent via
  a different channel. PR #3.

- B4 static code comparison: cast uses moq_lite (no subgroup/object_id concept);
  moq-pub-mmtp uses IETF moq-transport draft-14 (multi-object subgroups). Wire
  formats diverge by design. M.4 receivers need full multi-object subgroup decoder.
  PR #4.

- B2 re-scoped: per-FEC-block (SBN) grouping was originally framed as "high
  operational value" but moq-lite (cast's wire) doesn't use subgroups at all.
  B2 only matters once M.4 lands. Deferred to M.4 prerequisite work in BLO-8047.

DEFERRED / OUT OF M.1b SCOPE
============================
- B2 (per-FEC-block SBN grouping): deferred to M.4 prerequisite work.
- Upstream object_id_delta issue filing at cloudflare/moq-rs: draft captured in
  BLO-8047, to be filed via different channel.
- M.2 (cast bridge port): biggest blast radius of any remaining work. Replaces
  cast's FFmpeg moq_mmt + libmoq C-ABI hop with native Rust pipeline. Separate
  ADR + plan-phase needed before starting.
- M.4 (receiver migration): hang-mmt-fec / moqtail / Shaka switch to IETF
  moq-transport. Includes multi-object subgroup decoder, MfuReassembler wiring,
  per-FEC-block (SBN) grouping consumption, tier-switching fallback for
  FEC-irrecoverable 8K I-frames.

PICK ONE FOR NEXT SESSION (in priority order):
==============================================

A. PR review / merge prep (likely fastest)
   - Address review comments on any of PRs #1-#4 as they come in.
   - When #1 merges, rebase the three stacked PRs onto blockcast/main per the
     recipe below and re-push.

B. File the upstream object_id_delta issue at cloudflare/moq-rs
   - Draft body captured in BLO-8047 (latest comment). Edit / send.
   - Watch for upstream response; sync vendored moq-transport when fix lands.
   - Add a regression test in moq-pub-mmtp after the fix:
     parse FRAGMENT=3 smoke mlog, assert publisher-side subgroup_object_parsed
     events show object_id ∈ {0,1,2,3} per subgroup. Today: fails (all 0);
     after upstream fix: passes.

C. M.2 — Cast bridge port (per umbrella BLO-4020)
   - Replace cast's ffmpeg moq_mmt muxer + libmoq C-ABI hop with a native Rust
     pipeline. Biggest blast radius of any remaining work.
   - START WITH: separate ADR + plan-phase before any code. Reference B4 results
     for the wire-format constraints: cast must speak the same wire that
     moq-pub-mmtp speaks (IETF moq-transport), so M.2 = porting MMTP packetization
     into Rust + replacing moq_lite with moq_transport.

D. M.4 ADR sign-off + T0 (publisher draft-16 bump)
   - M.4 ADR drafted 2026-05-28 on branch `blo-4020-m4-adr` (commits ec8e4b7 +
     5ffd5e1 + 1b0c577). A0-A3 locked; Q3-Q8 still open (Pure-JS vs WASM
     reassembler, tier latency, transportFactory wiring, multicast extern,
     Track 3 FFI strategy, track sequencing).
   - START WITH: answer Q3-Q8, then T0 = bump moq-pub-mmtp to negotiate
     IETF moq-transport draft-16. Single change in moq-transport version
     negotiation; gates T1+T2+T3 receivers (all draft-16).
   - Track 1 (Shaka MMTP container support) recommended as first
     post-T0 implementation track — smallest, mirrors existing LOCParser/
     LocTransmuxer pattern, no other receivers touched.

E. M.4 Track 1 (Shaka MMTP) — implementation after D's T0 lands
   - shaka.msf.MMTPParser + shaka.transmuxer.MmtpTransmuxer (mirror LOC pattern).
   - Receiver-side object_id_delta reconstruction (per A5/B3 sidesteps).
   - @blockcast/transport wired as transportFactory.
   - End-to-end smoke against moq-pub-mmtp (post-T0).

READ FIRST (for any of the above):
1. .planning/moq-rs-m1-adr.md — full ADR with A1-A5/C1 decisions, T1-T9 Implementation
   Tasks, GSTACK eng review. Amended 2026-05-28 with the B1=C raw-passthrough contract.
2. .planning/moq-rs-m1-results.md — M.1 smoke verdict + DoD table.
3. .planning/moq-rs-m1b-frag-results.md — B1=C smoke verdict (FRAGMENT=0 + FRAGMENT=3).
4. .planning/moq-rs-m1b-obj-id-delta-results.md — B3 forensics + tentative upstream patch.
5. .planning/moq-rs-m1b-g6-bytediff-results.md — B4 wire-format diff + M.4 scope implications.
5b. .planning/moq-rs-m4-adr.md — M.4 ADR draft. A0-A3 locked (T0 publisher draft-16 bump
    pre-task; Track 1 Shaka MMTP container; Track 2 moqtail tier-switching; Track 3
    hang-mmt-fec migration INCLUDED in scope per session decision). Q3-Q8 still open.
    Branch: blockcast/blo-4020-m4-adr (not yet a PR).
6. moq-pub-mmtp/src/{main.rs,publish.rs,mmtp_parse.rs,framing.rs,udp.rs,cli.rs} — publisher.
7. moq-sub-raw/src/{main.rs,subscribe.rs,cli.rs} — subscriber.
8. moq-pub-mmtp/examples/synth_mmtp.rs + .planning/m1-smoke.sh — test pipeline
   (now with --fragment N + FRAGMENT=N support).
9. moq-pub-mmtp/vendor/mmt-core/src/{header.rs,reassembler.rs} — canonical MMTP/MPU
   parsers; vendored at pinned commit. Note reassembler.rs is the receiver-side
   reassembly that B1=C pins as the canonical implementation.
10. moq-transport/src/session/subscribed.rs:281 — B3 bug location; do not patch
    locally without consulting BLO-8047 §B3.

WORKFLOW NOTES
==============
- Local `main` tracks `blockcast/blo-4020-m1`. Direct commits to main push to PR #1
  automatically. For follow-up work on PRs #2/#3/#4, check out their branches:
    git checkout blo-4020-m1b-frag           # PR #2 (B1=C)
    git checkout blo-4020-m1b-obj-id-delta   # PR #3 (B3)
    git checkout blo-4020-m1b-g6-bytediff    # PR #4 (B4)
  Their upstream tracking was deliberately detached as a safety so stray pushes
  can't land on PR #1's branch. Use explicit `git push blockcast HEAD:<name>` when
  you want to push.
- `origin = cloudflare/moq-rs` is upstream and read-only; never push there.
- M.1 smoke is repeatable: `env -i HOME=$HOME PATH=$PATH bash .planning/m1-smoke.sh`.
  New env knob: `FRAGMENT=N` (default 0). N >= 1 exercises the raw-passthrough
  fragmentation path (Init + N MFU fragments per MPU).
- `/tmp/moq-coordinator.json` accumulates state across `--dev` runs of
  moq-relay-ietf. The smoke script cleans it up; ad-hoc runs may need a manual
  `rm /tmp/moq-coordinator.json` if a namespace shows as `duplicate`.

CONSTRAINTS (carry-forward)
============================
- TDD strict per superpowers/test-driven-development: RED test first, watch it
  fail, GREEN minimal impl, repeat. Characterization tests pinning existing
  behavior are OK to pass on first run if explicitly documented as such.
- Use mmt-core types: MmtpHeader, MpuHeader, PacketType, FragmentType. Vendored
  at moq-pub-mmtp/vendor/mmt-core/.
- moq-transport SubgroupsWriter::create silently drops group_id ≤ latest
  (subgroup.rs:116-128). A2 monotonicity catches this; don't lean on the writer.
- Publisher and subscriber connect URLs must match (no path) so they land in the
  same UNSCOPED tenant bucket on moq-relay-ietf (M.0 dev-scripts finding).
- SubgroupsReader surfaces only the LATEST subgroup — slow consumers miss
  intermediates. moq-sub-raw drains with a fast loop; tests pace producers when
  they need to verify multi-group sequencing.
- Run cargo test after each TDD cycle; do not batch.
- The publisher is RAW-PASSTHROUGH (B1=C): each MMTP packet — Init and every MFU
  fragment with FI ∈ {0,1,2,3} — becomes a separate MoQ object in the
  (packet_id, mpu_sequence) subgroup. Do NOT add reassembly to the publisher; the
  receiver owns reassembly via mmt-core::MfuReassembler.

ASK BEFORE
==========
- Cross-AI tensions or new design decisions: stop and ask.
- Pushing to remote branches: confirm the destination explicitly.
- Filing upstream issues at cloudflare/moq-rs: confirm before sending.
- Push back on anything that violates the ADR's locked decisions A1-A5/C1 + the
  B1=C raw-passthrough contract.
