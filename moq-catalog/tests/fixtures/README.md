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
- `fec-repair-container-not-native.json` -- libmmt pins
  `repairContainer: {"const":"native"}` but ships no negative exercising it, so
  no canonical vector reaches the `repairContainer must be native` rule.
- `legacy-common-track-fields.json`, `legacy-numeric-streaming-format.json`,
  `legacy-subgroup-history-groups.json` -- pre-MSF shapes (a `commonTrackFields`
  block, a numeric `streamingFormat`, `multicast.subgroupHistoryGroups`) that the
  smoke scripts emitted until #92 moved them to the canonical catalog. They pin
  that this crate still refuses those shapes. libmmt never emitted them, so it
  has no vector for them and they cannot live in the mirror.

Each was found by mutation-testing: the rule was neutralised, and nothing went
red. A guard with no failing mutation is a comment — if you add one, delete it
and confirm the suite fails before trusting it.

Not yet mirrored (each needs validation rules this crate does not implement; see
BLO-37534 follow-ups): `positive/multicast-auth-rotation.json` and the
`fec-enhancement-repair-removed`, `fec-repair-layer-geometry`,
`fec-repair-multiple-depends`, `multicast-auth-*`,
`multicast-packet-id-signaling`, `media-track-repair-layer`,
`raptorq-source-symbols-over-max` and `track-role-*` negatives.
