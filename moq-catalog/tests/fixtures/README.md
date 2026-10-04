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

`hang-schema-fixture-to-string.json` and `hang-schema-fixture-to-mmtp-json.json`
are the same kind of exception, and there are two of them for a reason. hang has
**two** independent catalog emitters — `hang::Catalog::to_string()` and
`to_msf`/`to_mmtp_json` — and they emit different field sets: the golden
serializer strips `role` and `fec.repairContainer`, and requires `trackRole`.
Checking one is not checking the wire. These are the byte output of
`rs/moq-mux/examples/catalog_schema_fixture.rs` at hang-mmt-fec `main`
`12e2455e`, one catalog per emitter, 8 tracks each, captured with
`cargo run -p moq-mux --example catalog_schema_fixture`. That example is the
same one hang's own CI gate feeds to `validate-rust-catalog-schema.mjs`
(`.github/workflows/check.yml`), so it is the canonical pair rather than a
hand-built fixture — which is what makes them worth pinning here: whatever those
two emit is, by definition, what a relay receives. Like the capture above they
are hand-regenerated, because `hang` is not a workspace member here.

They were added for BLO-39866: both emitters produce a root `multicast.auth`
block that this crate rejected under `deny_unknown_fields`, and the single-track
`hang-catalog-to-string.json` capture does not carry one, so nothing here caught
it. That divergence survived BLO-37534 by exactly one week.

`non-msf/` is the other exception, and it is not a third mirror direction: see
its own section below.

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

## `non-msf/` — parse yes, validate no

These are catalogs this crate must **deserialize and round-trip** but must
**not** accept as MSF, so they fit neither `positive/` nor `negative/`.

`hang::CatalogRoot::to_string()` serializes every capture hang holds, including a
`Container::Legacy` one. A legacy track is varint-framed, carries no MMTP
packetization, and has no MSF `packaging` value it could honestly claim, so the
emitter gives it `streamingFormat: "cmaf"` rather than the MSF envelope
(BLO-37925). libmmt's schema pins `streamingFormat` to the constant `"mmtp"` and
*defines* the MSF wire, so such a catalog is outside that schema's scope rather
than in violation of it. That is why these cannot be mirrored in either
direction: `positive/`'s contract is that both implementations accept them, and
calling them `negative/` would assert libmmt ships a vector rejecting them, which
it does not and should not.

moq-sub still has to consume one. `validate_catalog()`
(`moq-sub/src/media.rs`) applies the MMTP-only `Root::validate()` *only* when
`streamingFormat == "mmtp"`, so the envelope is what decides — but a subscriber
that cannot even deserialize the catalog never reaches that gate. Before #93 this
exact capture failed on `deny_unknown_fields` at `keyframeIntervalMs`.

`golden.rs`'s `non_msf_fixtures_parse_and_round_trip_but_are_not_valid_msf`
asserts all three legs, and each was mutation-tested: neutralising the
`streamingFormat must be mmtp` rule in `Root::validate()` turns it red *and
nothing else* in this suite, reverting `src/lib.rs` to pre-#93 turns the
deserialize red, and an unregistered file in `non-msf/` turns
`every_fixture_file_is_registered` red.

`moq-sub` asserts the other half where the gate actually lives:
`accepts_the_legacy_container_capture_hang_emits` (`moq-sub/src/media.rs`) feeds
this same fixture through `validate_catalog()` and expects `Ok`. The dependency
edge only runs one way, so this crate cannot see a change to that function.

### Provenance

`hang-legacy-catalog-to-string.json` is the byte-for-byte `to_string()` output of
the legacy capture pinned by `a_legacy_container_capture_is_refused_the_msf_envelope`
in hang-mmt-fec `rs/moq-msf-fixtures/tests/mux_catalog_conformance.rs`, captured
from that repo's `main` at `8f158edc`. It is **hand-regenerated**: nothing in this
workspace produces it, because `hang` is not a workspace member here. Regenerating
it from `hang::CatalogRoot::to_string()` in CI is tracked separately on BLO-37925's
follow-ups -- that would be a new dependency edge into a crate outside this
workspace, not a tightening of this test.

Not yet mirrored (each needs validation rules this crate does not implement; see
BLO-37534 follow-ups): the `fec-enhancement-repair-removed`,
`fec-repair-layer-geometry`, `fec-repair-multiple-depends`,
`multicast-packet-id-signaling`, `media-track-repair-layer`,
`raptorq-source-symbols-over-max` and `track-role-*` negatives.

`positive/multicast-auth-rotation.json` came off that list in BLO-39866 and is
now mirrored above.

The `multicast-auth-*` negatives are a different case, and they are listed
separately because they are **not** pending work. BLO-39866 ruled that
`multicast.auth` is carried opaquely — `Root` guarantees the block survives a
round-trip and deliberately does not interpret it, because nothing in `moq-rs`
verifies provenance. A crate that implements no rule over a block cannot reject
anything for violating one, so these three can never move into `negative/`,
whose contract is that *both* implementations reject the file:

- `multicast-auth-padded-key` — `publicKey` carries `=` padding.
- `multicast-auth-trailing-pad-bits` — `publicKey` ends `...Mbh` where the
  schema's final-character class allows only `...Mbg`; the trailing two bits of
  the 43rd base64url character must be zero for a 32-byte key.
- `multicast-auth-reused-track-format` — duplicate
  `(mediaTrack, sourceSymbolFormat)` binding, caught by the schema's own
  `uniqueMulticastAuthTrackBindings` keyword.

Those first two are the argument for the ruling rather than an exception to it:
re-deriving base64url-unpadded-with-zero-trailing-bits in Rust, slightly wrong,
would make the relay *claim* to validate signing keys while accepting malformed
ones. libmmt's schema is where those vectors are enforced, and it does enforce
all three — verified against this crate's three `multicast.auth` positives in the
same change. If `moq-rs` ever grows real signature verification, that is the
change that earns these negatives, and it should mirror them then.
