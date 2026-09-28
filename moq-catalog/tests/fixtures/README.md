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

Not yet mirrored (each needs validation rules this crate does not implement; see
BLO-37534 follow-ups): `positive/multicast-auth-rotation.json` and the
`fec-repair-*`, `multicast-auth-*`, `multicast-packet-id-signaling`,
`media-track-repair-layer`, `raptorq-source-symbols-over-max` and `track-role-*`
negatives.
