# Design note: moq-transport concurrent-subgroup delivery (B-MIG prerequisite)

**Date:** 2026-05-29 · **Status:** capability IMPLEMENTED + tested; publisher/receiver wiring PENDING (window source TBD)
**Context:** M.4 Track 1, Mapping B (subgroup-per-MFU). Prerequisite for B-MIG-pub.

## Implementation status (2026-05-29)

Done in `moq-transport/src/serve/subgroup.rs` (TDD, 5 tests RED→GREEN; full crate
114 pass / 0 fail; clippy-clean; workspace builds):
- `SubgroupsState` now holds `subgroups: Vec<SubgroupReader>` + `pruned: usize`
  (was a single `latest_subgroup_reader` slot + `epoch`).
- `create()` appends unconditionally (removed the latest-wins drop + the
  `Duplicate` check — both per the approved note); prunes the front by group window.
- `next()` walks an absolute `read_index` mapped to `subgroups[read_index - pruned]`;
  a reader behind the window skips to `pruned`.
- Group-window prune via opt-in `SubgroupsWriter::set_history_window(groups)`;
  prune condition `group_id + window <= newest` (additive, no unsigned underflow).
- `append()`, `latest()`, `next()`, `create()` public signatures unchanged →
  consumers (`session/subscribed.rs` egress, `session/subscribe.rs` receive) compile
  unchanged and the egress's existing per-subgroup-stream fan-out now works.

### OPEN — must resolve before the publisher runs long

`set_history_window` is **opt-in**; with it unset the writer **retains all
subgroups** (mirrors the object-level Vec). That is a change from latest-wins
(O(1)) → any long-lived writer that does NOT set a window now grows unbounded.
The two production writers — `main.rs` (publisher) and `subscribe.rs` (receiver,
incl. relay forwarding) — therefore MUST call `set_history_window` with a
config-sourced value, or they leak. Window VALUE source is undecided
(catalog field vs CLI flag vs session config) and is the gating decision for
wiring. Publisher wiring naturally folds into B-MIG-pub (which restructures
`main.rs` subgroup creation); receiver/relay wiring is downstream (relay
B-forwarding). Until wired, do not run the publisher for extended periods.

## Problem

Mapping B emits multiple subgroups per group (subgroup 0 = Init, subgroups 1..M =
one MFU each). `moq_transport::serve::Subgroups` is **latest-subgroup-wins** and
cannot carry this:

- `SubgroupsState` holds a single `latest_subgroup_reader: Option<SubgroupReader>`
  + an `epoch` counter (`serve/subgroup.rs:46-53`).
- `SubgroupsWriter::create()` **drops** any subgroup that isn't ≥ the current
  latest within a group (`:120` `Less => return Ok(writer) // dropped immediately, lul`;
  `:127` older-group drop). Only the newest survives.
- `SubgroupsReader::next()` returns `state.latest_subgroup_reader.clone()` only
  (`:184`) — no queue, intermediate subgroups skipped on epoch jumps.

The egress already fans out correctly: `session/subscribed.rs::serve_subgroups`
(`:202-232`) spawns a task + its own uni QUIC stream per subgroup yielded by
`next()`. **The only broken link is that `next()` never yields more than the
latest subgroup.** So this is a contained `serve/subgroup.rs` fix, no wire change.

## Precedent to mirror (already in the file)

The **object-level** reader one layer down already does exactly what we need:
`SubgroupState { objects: Vec<SubgroupObjectReader> }` (`:273-288`) + per-reader
`read_index` cursor; `SubgroupReader::next()` (`:410-429`) returns
`objects[read_index]`, increments, and waits on `state.modified()` when caught up.
Cloned readers each carry their own `read_index` and "run in parallel" (`:380`).
We replicate this pattern at the **subgroups** level.

## Proposed change (all in `serve/subgroup.rs`)

### State
```rust
struct SubgroupsState {
    subgroups: Vec<SubgroupReader>,   // creation order; replaces latest_subgroup_reader + epoch
    pruned: usize,                    // # of leading subgroups GC'd (see Memory bound)
    closed: Result<(), ServeError>,
}
```
`SubgroupsReader` gains `read_index: usize` and drops `epoch`.

### `create()` — stop dropping
```rust
pub fn create(&mut self, s: Subgroup) -> Result<SubgroupWriter, ServeError> {
    let (writer, reader) = SubgroupInfo { track, group_id: s.group_id,
        subgroup_id: s.subgroup_id, priority: s.priority }.produce();
    let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
    state.subgroups.push(reader);            // unconditional; no latest-wins drop
    self.last_group_id    = s.group_id;      // keep append() bookkeeping intact
    self.next_group_id    = s.group_id + 1;
    self.next_subgroup_id = s.subgroup_id + 1;
    Ok(writer)
}
```
- Removes the monotonic-drop guard (`:116-128`). MoQ permits subgroups in any
  order; the subscriber reorders by (group, subgroup, object) ids. Publisher-side
  group monotonicity is still enforced upstream by publish.rs's A2 check.
- Drops the `Duplicate` check (the object-level `create()` has none either). Note
  in source if we want to keep it (would require a scan).

### `next()` — drain the queue (mirror object-level `next()`)
```rust
pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
    loop {
        { let state = self.state.lock();
          if self.read_index < state.pruned { self.read_index = state.pruned; } // skip GC'd
          let idx = self.read_index - state.pruned;
          if idx < state.subgroups.len() {
              let r = state.subgroups[idx].clone();
              self.read_index += 1;
              return Ok(Some(r));
          }
          state.closed.clone()?;
          match state.modified() { Some(n) => n, None => return Ok(None) }
        }.await;
    }
}
```
`latest()` reimplemented over `subgroups.last()`.

### Untouched
`serve_subgroups` / `serve_subgroup` (egress) — already per-subgroup-stream
capable. No wire/draft-16 change: subgroup_id already on the wire
(`SubgroupHeader`, `header_type = SubgroupIdExt`), `object_id_delta = 0` for all
objects (receiver reconstructs absolute id per draft §4.6, already implemented).
EndOfGroup already removed "due to spec issues" (commit ecb665b) — nothing to do.

## The one real decision: memory bound

`latest-wins` was O(1) memory by throwing data away. A full-history `Vec` is
O(total subgroups for the life of the track) → for a live publisher (≈15
subgroups/group × groups/s × hours) that's an unbounded leak. The object-level
`Vec` has the same latent unboundedness but is bounded in practice by a single
subgroup's short lifetime; the subgroups `Vec` is **not** — it lives as long as
the track.

Options:

- **(A) Group-window prune (recommended for a live publisher).** When the writer
  advances to group `G`, prune subgroups of groups `< G - window_groups`
  (increment `pruned`, drop from `Vec` front). A live publisher never resends old
  groups; a reader that falls behind the window skips to `pruned` (matches MoQ
  "subscribe = current-forward"). Bounds memory to ~`window_groups` of subgroups.
  `window_groups` from catalog/config — no magic number (per repo rules).
- **(B) Min-cursor GC (precise, more complex).** Track the min `read_index` across
  all live `SubgroupsReader` clones; prune below it. Loses nothing for any live
  reader, but needs shared cursor accounting and handling of reader
  creation/drop. Heavier.
- **(C) Match object-level (retain all).** Simplest, mirrors existing code, but
  leaks on long streams. Only acceptable for short/bounded sessions.

Recommendation: **(A)**, window from config. It is the smallest change that is
both correct for live and bounded. Flag (B) as a follow-up if a use case needs
lossless very-late join.

## Blast radius

- `SubgroupsReader` consumers to re-verify: `session/subscribed.rs` (`:127,197,279`),
  `session/subscribe.rs` (`:227-228`). Preserve `next()` + `latest()` signatures;
  change is internal to `subgroup.rs`.
- `append()` (`:82-102`) — no live callers found, but kept correct (still
  produces group N / subgroup 0 each call via the bookkeeping above).
- `moq-pub-mmtp` uses only `SubgroupsWriter::create()` (adapter in `publish.rs`) —
  unaffected by the reader change; benefits from create() no longer dropping.

## Test plan (TDD, RED first)

1. **Core:** create two subgroups in one group → `reader.next()` yields *both*
   (today: only the 2nd). RED now.
2. Out-of-order create within a group → both delivered (no drop).
3. Cloned readers each receive all subgroups independently (own cursor).
4. `append()` → increasing groups, subgroup 0, all delivered.
5. **Window:** after `> window` groups, `Vec` capped; a keeping-up reader sees all;
   a lagging reader resumes at `pruned` without panic; nothing unread *within* the
   window is dropped.
6. (heavier, optional) session-level: N subgroups/group → N uni streams.

## Risks

- **Memory bound** is the substantive risk — must land with the fix, not after.
- Removing the monotonic-drop guard could surface latent ordering assumptions in
  other producers; mitigated by the consumer audit above.
- `pruned`/`read_index` index arithmetic is the bug-prone part — covered by tests 5.

## Out of scope (downstream, already mapped)

B-MIG-pub (timestamp-keyed subgroup-per-MFU in publish.rs), T1.2 CMAF-wrap of AVCC
NAL MFUs, T1.5c timing. This note is only the transport prerequisite.
