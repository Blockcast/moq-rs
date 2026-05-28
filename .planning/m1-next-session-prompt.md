M.1 of BLO-4020 (Cloudflare moq-rs MMTP migration) is COMPLETE.
PR #1 open, 9 commits, 69 unit tests + smoke. Working directory:
/home/oramadan/src/pim-multicast-gateway/moq-rs

State as of 2026-05-28 (M.1 closed; pick from M.1b / M.2 / G6 next):

Branch on fork: https://github.com/Blockcast/moq-rs/tree/blo-4020-m1
PR #1:        https://github.com/Blockcast/moq-rs/pull/1
              "BLO-4020 M.1: MMTP publisher on IETF moq-transport (draft-14+)"
Base:         blockcast/main (latest upstream tip, rebased cleanly)

Commits on the branch (oldest → newest):
  dbf5ee1 docs(planning): M.0 moq-rs baseline test results
  6ee40ff feat(moq-catalog): Container enum + multicast catalog extension
  bde975d docs(planning): M.1 ADR + handoff prompt
  3101aca feat(moq-pub-mmtp): MMTP publisher for IETF moq-transport (draft-14+)
  3338040 docs(planning): update M.1 handoff — Lane A done, PR #1 open
  b05dfce feat(moq-catalog,moq-pub-mmtp): T5 catalog validation expansion
  509c5c8 feat(moq-sub-raw): raw per-track payload subscriber (T7)
  526f0e9 feat(moq-pub-mmtp): multicast UDP listener (T8.5)
  5e7310c feat(moq-pub-mmtp): T9 end-to-end smoke + per-track sha256 verification

Remotes:
  origin    git@github.com:cloudflare/moq-rs   (upstream, untouched)
  blockcast git@github.com:Blockcast/moq-rs.git (Blockcast fork; local main
                                                  tracks blockcast/blo-4020-m1)

Total tests across the three crates (all green, zero warnings):
  - moq-catalog: 24
  - moq-pub-mmtp: 39 + synth_mmtp example
  - moq-sub-raw: 6
  - = 69 unit + smoke pass

What landed (all T-tasks from the ADR):
- T1 ✅ publisher loop with A1/A2/A3 invariants. dispatch fn extracted
  over TrackSubgroups + SubgroupWrite traits for testability.
- T2 ✅ .catalog track at group 0 / priority 127.
- T3 ✅ auto `<name>/repair` siblings at priority 7, repair group_id
  mirrors source MPU group_id.
- T4 ✅ UDP input mode + T8.5 multicast group auto-join.
- T5 ✅ Root::validate() (duplicate packet_id, unknown track ref,
  FecRepair in catalog.tracks) + Root::expand_common_fields() +
  publisher namespace consistency check.
- T6 ✅ mmt-core vendored at libmmt 929e5b0c.
- T7 ✅ moq-sub-raw crate (drain_track_to_writer + CLI validation).
- T8 ✅ N/A — moqenc_mmt already uses AVIOContext properly; no FFmpeg
  changes needed (architecture pushback validated mid-session).
- T9 ✅ end-to-end smoke with synth_mmtp → UDP → moq-pub-mmtp →
  relay → moq-sub-raw → per-track sha256 PASS, mlog framing captured.

PICK ONE FOR NEXT SESSION (in priority order):

A. **PR #1 review / merge prep** (likely fastest)
   - Wait for / address review comments on the PR.
   - Re-rebase on blockcast/main if upstream moves.
   - Optional: squash the 9 commits if reviewer prefers fewer.
   - Land it.

B. **M.1b leftovers from the ADR's "NOT in scope for M.1" list**
   Pick any of these — they're independent, small-to-medium scope:
   - **M.1b-frag**: MMTP fragmentation reassembly. Today dispatch
     errors if `fragmentation_indicator != 0`. Add reassembly state
     per (packet_id, mpu_sequence). RED tests first: build fragmented
     packet sequences, dispatch reassembles, single object emitted
     per logical MPU.
   - **M.1b-fec-grouping**: per-FEC-block grouping for repair tracks.
     Today repair lands on a single rolling group per /repair track
     keyed to source MPU. Spec asks for per-FEC-block (SBN) grouping
     so receivers can correlate repair symbols with source blocks at
     finer grain. Requires parsing Source/Repair FEC Payload ID.
   - **M.1b-object-id-delta**: Codex #6 follow-up. Validate
     moq-transport's `object_id_delta` encoding against draft-14+
     in an mlog dump. If broken, file upstream + patch downstream.
   - **G6 byte-diff**: co-located libmoq + moq-pub-mmtp capture.
     Run cast/libmoq emission and moq-pub-mmtp side-by-side, capture
     mlog/qlog on both, diff at the SUBGROUP/OBJECT frame level.
     If framing differs beyond payload, file the diff for the
     IETF draft-15+ tracking.

C. **M.2 — Cast bridge port (per umbrella BLO-4020)**
   Replace cast's ffmpeg moq_mmt muxer + libmoq C-ABI hop with a
   native Rust pipeline. Biggest blast radius of any remaining
   work. Plan: separate ADR + plan-phase before starting.

D. **Receiver-side M.4 prep**
   Inventory the receivers (hang-mmt-fec, moqtail) and design the
   draft-14+ client they need. Out of M.1 scope per the ADR but
   gates production rollout.

READ FIRST (for any of the above):
1. .planning/moq-rs-m1-adr.md — full ADR with A1-A5/C1 decisions,
   Implementation Tasks T1-T9, GSTACK eng review.
2. .planning/moq-rs-m1-results.md — smoke verdict + DoD table + the
   open M.1b items listed above.
3. .planning/moq-rs-m0-results.md — M.0 baseline + dev/pub vs dev/sub
   scope-mismatch caveat (still applicable).
4. moq-pub-mmtp/src/{main.rs,publish.rs,mmtp_parse.rs,framing.rs,
   udp.rs,cli.rs} — publisher current state.
5. moq-sub-raw/src/{main.rs,subscribe.rs,cli.rs} — subscriber.
6. moq-pub-mmtp/examples/synth_mmtp.rs + .planning/m1-smoke.sh —
   the test pipeline.
7. moq-pub-mmtp/vendor/mmt-core/src/header.rs — canonical MMTP/MPU
   parsers; vendored at pinned commit.

WORKFLOW NOTES:
- Local `main` tracks `blockcast/blo-4020-m1`. New commits push to
  PR #1 automatically. If a follow-up should be a separate PR,
  branch off first (`git checkout -b blo-4020-m1b-frag` etc).
- `origin = cloudflare/moq-rs` is upstream and read-only; never
  push there.
- M.1 smoke is repeatable: `env -i HOME=$HOME PATH=$PATH
  bash .planning/m1-smoke.sh`. Without `env -i`, pre-set GROUPS /
  PACKET_DELAY_MS in the operator's shell silently override the
  defaults (this bit us once; documented in results.md).
- `/tmp/moq-coordinator.json` accumulates state across `--dev`
  runs of moq-relay-ietf. The smoke script cleans it up; ad-hoc
  runs may need a manual `rm /tmp/moq-coordinator.json` if a
  namespace shows as `duplicate`.

CONSTRAINTS (carry-forward — every TDD cycle this session followed):
- TDD strict per superpowers/test-driven-development: RED test first,
  watch it fail, GREEN minimal impl, repeat. The session had one slip
  (writing impl + tests together in T7) which was caught by the test
  failing — pinned the SubgroupsReader latest-only semantics as the
  documented surprise.
- Use mmt-core types: MmtpHeader, MpuHeader, PacketType,
  FragmentType::Init. Vendored at moq-pub-mmtp/vendor/mmt-core/.
- moq-transport SubgroupsWriter::create silently drops group_id ≤
  latest (subgroup.rs:116-128). A2 monotonicity catches this; don't
  lean on the writer.
- Publisher and subscriber connect URLs must match (no path) so they
  land in the same UNSCOPED tenant bucket on moq-relay-ietf
  (M.0 dev-scripts finding).
- SubgroupsReader surfaces only the LATEST subgroup — slow consumers
  miss intermediates. moq-sub-raw drains with a fast loop; tests
  pace producers when they need to verify multi-group sequencing.
- Run cargo test after each TDD cycle; do not batch.

ASK BEFORE: cross-AI tensions or new design decisions stop and ask.
Push back on anything that violates the ADR's locked decisions
A1-A5/C1.
