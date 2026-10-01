# moq-catalog golden fixtures

These are a mirror of libmmt's canonical MSF catalog corpus at
`packages/container/test-vectors/catalog/` (`Blockcast/libmmt`). libmmt's schema
(`packages/container/schemas/catalog.schema.json`) and this crate are two
implementations of one wire format, so the corpus is shared on purpose: every
`positive/` file must validate in both, and every `negative/` file must be
rejected by both.

**When you touch a fixture, sync it.** BLO-37534 was caused by this directory
silently falling behind libmmt: the copies here drifted, 18 newer libmmt vectors
were never picked up, and the Rust model was only ever widened to whatever the
stale copies needed. The result was a `moq_catalog::Root` that could not parse a
single catalog the fleet actually emits — including the output of hang's own
`to_golden_string()` wire serializer.

`hang-catalog-to-string.json` is the exception: it is a capture of
`hang::CatalogRoot::to_string()` output rather than a libmmt vector, kept here so
the emitter hang actually ships stays parseable. It validates against libmmt's
schema too.

## The mirror is enforced — `tests/mirror.rs`

`libmmt-negative.manifest` vendors libmmt's `catalog/negative/` listing as
`<git blob SHA>  <name>`, and `tests/mirror.rs` checks both directions of the
contract against it: nothing in `negative/` may be missing from or differ from
libmmt, and nothing in `negative-local/` may be byte-identical to a libmmt
vector. A blob SHA is a content hash, so equality is byte-identity. Vendoring the
listing keeps this a pure local comparison — no network, and no requirement that
libmmt be checked out beside this repo.

The manifest is the thing you update when you deliberately sync; regenerate it
from a libmmt checkout with the command in its header. It records the libmmt
commit it came from.

Two things the check deliberately does *not* do. It does not require every libmmt
vector to be mirrored — the unmirrored ones are listed at the bottom of this file
and each needs a validation rule this crate has yet to implement. And it reads
the directories rather than `golden.rs`'s `include_str!` tables, because a merge
can add a file to `negative/` without registering it and `mirror.rs` still has to
judge whether it is a mirror.

That directory read does not, by itself, catch an *unregistered* fixture: a file
that is a genuine libmmt vector by name and SHA is a legitimate mirror, so
`mirror.rs` passes it, and `golden.rs` never sees it because it iterates its own
tables. `golden.rs`'s `every_fixture_file_is_registered` closes that gap — it
reads all three fixture directories and fails on any file no table names, so a
fixture cannot sit in the tree testing nothing while looking covered. Deletions
were already caught, because `include_str!` fails to compile; only additions were
silent.

This exists because prose did not hold. The contract was broken twice and caught
by review both times, never by a red test: once by a hand edit, and once by the
merge at `1c9f26fb`, which carried three local-only negatives into `negative/`
while every local signal — directory/registry consistency, arities, and the
fixtures still being rejected — stayed green. No amount of care at authoring time
covers a merge.

## `negative-local/` — not mirrors

`negative/` is a byte-identical mirror, so it cannot host a fixture libmmt does
not have. `negative-local/` holds the few negatives that exist only here: the
first three because no canonical vector reaches the rule each covers, the last
because libmmt never emitted the shapes they pin:

- `endpoint-missing-protocol-and-source.json` — libmmt's
  `multicast-endpoint-missing-protocol-and-source.json` is a *double* negative:
  its `"networkSource": "direct"` is a string where the schema pins an array of
  objects, so serde rejects it before the endpoint rule runs. This one omits
  `networkSource` entirely so the endpoint rule is what fails.
- `fec-repair-overlay-missing-companions.json` — libmmt ships no vector for
  `allOf[1].then.allOf[0]`'s *required* arm (only for its `repairLayer`
  exclusion).
- `fec-repair-container-not-native.json` — libmmt pins
  `repairContainer: {"const":"native"}` but ships no negative exercising it, so
  no canonical vector reaches the `repairContainer must be native` rule.
- `legacy-common-track-fields.json`, `legacy-numeric-streaming-format.json`,
  `legacy-subgroup-history-groups.json` — pre-MSF shapes (a `commonTrackFields`
  block, a numeric `streamingFormat`, `multicast.subgroupHistoryGroups`) that the
  smoke scripts emitted until #92 moved them to the canonical catalog. They pin
  that this crate still refuses those shapes. libmmt never emitted them, so it
  has no vector for them and they cannot live in the mirror.

The first three were found by mutation-testing: the rule was neutralised, and
nothing went red. A guard with no failing mutation is a comment — if you add one,
delete it and confirm the suite fails before trusting it. The `legacy-*` three
are parse-level rejections (`deny_unknown_fields` and the type checks do the
work), so there is no rule to neutralise and nothing to mutation-test.

Not yet mirrored (each needs validation rules this crate does not implement; see
BLO-37534 follow-ups): `positive/multicast-auth-rotation.json` and the
`fec-enhancement-repair-removed`, `fec-repair-layer-geometry`,
`fec-repair-multiple-depends`, `multicast-auth-*`,
`multicast-packet-id-signaling`, `media-track-repair-layer`,
`raptorq-source-symbols-over-max` and `track-role-*` negatives.
