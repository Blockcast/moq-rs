# M.1b §B3 — `object_id_delta` correctness check: results

**Date:** 2026-05-28
**Repo:** `~/src/pim-multicast-gateway/moq-rs` @ `blo-4020-m1b-obj-id-delta`
**Source data:** mlog dumps from the M.1b §B1 `FRAGMENT=3` smoke at `/tmp/m1-smoke-frag3/mlog/`
**Tracking:** BLO-8047 §B3, Codex #6
**Verdict:** **BUG CONFIRMED.** Upstream issue in `moq-transport/src/session/subscribed.rs:281`. The publisher hardcodes `object_id_delta = 0` for every object on the wire — the "delta logic" was never implemented. Comment on the offending line literally says so.

## Smoke evidence

Both mlog files are JSON-SEQ (qlog 0.3, RS = `0x1E` separator). Both record the relay's perspective; the relay is server for both QUIC sessions (publisher ↔ relay and relay ↔ subscriber).

**File 1 — `5a3e3812…_server.mlog`** (publisher → relay direction; relay-as-server parses incoming objects):

```bash
$ tr '\036' '\n' < /tmp/m1-smoke-frag3/mlog/5a3e3812…_server.mlog | grep -oE '"name":"moqt:[a-z_]*"' | sort | uniq -c
      4 "name":"moqt:control_message_created"
      4 "name":"moqt:control_message_parsed"
     10 "name":"moqt:subgroup_header_parsed"
     40 "name":"moqt:subgroup_object_parsed"
$ tr '\036' '\n' < /tmp/m1-smoke-frag3/mlog/5a3e3812…_server.mlog | grep -oE '"object_id":[0-9]+' | sort | uniq -c
     40 "object_id":0
```

10 subgroups × 4 objects per subgroup = 40 objects (matches `GROUPS=5 × 2 tracks` × `Init+3 MFU fragments`). **Every single one reports `object_id=0`.**

**File 2 — `f13dc3d5…_server.mlog`** (relay → subscriber direction; relay-as-server creates outgoing objects):

```bash
$ tr '\036' '\n' < /tmp/m1-smoke-frag3/mlog/f13dc3d5…_server.mlog | grep -oE '"name":"moqt:[a-z_]*"' | sort | uniq -c
      3 "name":"moqt:control_message_created"
      3 "name":"moqt:control_message_parsed"
     10 "name":"moqt:subgroup_header_created"
     40 "name":"moqt:subgroup_object_created"
$ tr '\036' '\n' < /tmp/m1-smoke-frag3/mlog/f13dc3d5…_server.mlog | grep -oE '"object_id":[0-9]+' | sort | uniq -c
     10 "object_id":0
     10 "object_id":1
     10 "object_id":2
     10 "object_id":3
```

Each subgroup egresses with sequential `object_id ∈ {0,1,2,3}` — the relay's outgoing `SubgroupWriter::next_object_id` auto-increments (per `moq-transport/src/serve/subgroup.rs:299-335`) and **masks the publisher's wire-encoding bug for any subscriber that traverses moq-relay-ietf**.

## Root cause — `moq-transport/src/session/subscribed.rs:281`

```rust
// In serve_subgroup, on the publisher → relay send path:
let mut object_count = 0;
while let Some(mut subgroup_object_reader) = subgroup_reader.next().await? {
    let subgroup_object = data::SubgroupObjectExt {
        object_id_delta: 0, // before delta logic, used to be subgroup_object_reader.object_id,
        extension_headers: subgroup_object_reader.extension_headers.clone(),
        payload_length: subgroup_object_reader.size,
        status: …,
    };
    …
    writer.encode(&subgroup_object).await?;
    …
}
```

The comment on line 281 (`before delta logic, used to be subgroup_object_reader.object_id`) confirms a previous version of the code wrote the absolute `object_id` directly into the `object_id_delta` field (also wrong, but for a different reason — that would interpret as huge deltas), and the current version simply hardcodes `0`. The actual draft-ietf-moq-transport-14 `object_id_delta` computation was never written.

Other call sites that set `object_id_delta`:

```
moq-transport/src/data/subgroup.rs:368   object_id_delta: 0,    // in SubgroupObject::new() default
moq-transport/src/data/subgroup.rs:386   object_id_delta: 0,    // in SubgroupObjectExt::new() default
moq-transport/src/session/subscribed.rs:281   object_id_delta: 0, // ← the actual publisher path; this is the bug
```

No site in `moq-transport/src/` computes a non-zero `object_id_delta`. The wire format on the publisher side is completely broken w.r.t. draft-14+.

## Impact analysis

| Subscriber topology | Affected? | Why |
|---|---|---|
| Subscriber → moq-relay-ietf → publisher | No | Relay's egress `SubgroupWriter::next_object_id` auto-increments and overwrites the broken values. The M.1 smoke passes byte-equality entirely because of this masking. |
| Subscriber → publisher (no relay) | **Yes** | Receives `object_id_delta=0` for every object; reconstructed `object_id` collapses to 0 for all objects in the subgroup. Any subscriber that uses `object_id` for ordering, deduplication, or skip-ahead breaks. |
| Multi-hop relay chains | Only first hop | Each relay's egress re-sequences. After hop 1 the wire is correct; the bug is invisible past the first hop. |
| `moq-sub-raw` (M.1 subscriber) | No | Uses `drain_track_to_writer`, which reads object payloads in subgroup order and concatenates by track. It never inspects `object_id`. This is why per-track sha256 still matches end-to-end in the M.1 smoke. |
| Any production receiver (moqtail's `@moq/hang`, Shaka via WASM) | **Likely yes** | These consumers do object-level reassembly (via `mmt-core::MfuReassembler`) and may use `object_id` for ordering. They currently sit behind moq-relay-ietf, so the bug is masked, but cannot rely on the publisher's wire if topology changes. |

## Recommended fix (upstream)

Track the last-emitted `object_id` per subgroup and compute the delta. Tentative patch shape:

```rust
// In moq-transport/src/session/subscribed.rs, inside serve_subgroup, before the loop:
let mut last_object_id: Option<u64> = None;

while let Some(mut subgroup_object_reader) = subgroup_reader.next().await? {
    let object_id_delta = match last_object_id {
        // First object: per draft-14+ §9.x, delta is from an
        // implicit start point (first_object_id_minus_1 = -1), so
        // the first object's delta equals its absolute object_id.
        None => subgroup_object_reader.object_id,
        Some(prev) => {
            // Subsequent: delta from previous object_id; spec
            // requires strictly increasing object_id, so prev < this.
            subgroup_object_reader.object_id
                .checked_sub(prev)
                .expect("subgroup object_id must strictly increase")
        }
    };
    last_object_id = Some(subgroup_object_reader.object_id);

    let subgroup_object = data::SubgroupObjectExt {
        object_id_delta,
        …
    };
    …
}
```

**Caveat — verify against draft-14+ spec text** before submitting. The exact "first object delta" convention has shifted between drafts; some versions use `object_id_delta = object_id - (first_object_id - 1)` (so the first delta is always 1 unless `first_object_id_minus_1` is in the subgroup header), others use absolute. The fix needs to match whatever moq-transport's decode side expects so encode/decode stay consistent.

The matching decode site is `moq-transport/src/data/subgroup.rs:163` and `:260` — those just store `object_id_delta` as-is; reconstruction to an absolute `object_id` happens elsewhere (or not at all in some receivers). Spec-conformant decode would maintain its own running `last_object_id` and reconstruct on parse.

## What this means for the M.1 PR and the moq-pub-mmtp publisher

**M.1 (PR #1) is NOT broken by this finding** — the per-track sha256 match end-to-end despite the bug because:
- moq-sub-raw doesn't use `object_id`.
- The smoke topology has a relay in the middle that masks the bug.

**M.1b §B1 (PR #2) is not affected either** — that PR pins the raw-passthrough fragmentation contract via tests; the contract is about wire-payload preservation, not object_id semantics. The new tests would still pass if/when the upstream fix lands.

**The fix is purely upstream** (Cloudflare/moq-rs). moq-pub-mmtp consumes `SubgroupWriter::write` correctly — the bug is one layer below, in how `SubgroupWriter` serializes to the wire via the `serve_subgroup` task in `subscribed.rs`. No moq-pub-mmtp change is needed once upstream lands a fix.

## Out of scope for B3

- **Authoring the fix** locally and patching the vendored `moq-transport` to ship a temporary forked-fix. Possible follow-up if upstream is slow, but the M.1b smoke already proves we don't need it for the current receiver set.
- **Decode-side reconstruction audit.** Codex flagged the encode side; the decode side may also need adjustment. Inspect when the upstream fix lands.
- **Spec citation.** Need to pull the exact draft-ietf-moq-transport-14 §X.Y for `object_id_delta` semantics before filing upstream. The IETF datatracker URL is `https://datatracker.ietf.org/doc/draft-ietf-moq-transport/14/`.

## Next steps

1. **File upstream issue** at `https://github.com/cloudflare/moq-rs/issues` quoting this analysis. Title: `[bug] subscribed.rs sends object_id_delta=0 for every object — draft-14+ wire format broken on publisher path`.
2. **Track the upstream fix** in BLO-8047 §B3. When it lands, sync the vendored moq-transport in this checkout.
3. **Add a regression test** in `moq-pub-mmtp` once the fix lands: parse a `FRAGMENT=3` smoke mlog, assert the publisher-side `subgroup_object_parsed` events show `object_id ∈ {0,1,2,3}` per subgroup. Today such a test would fail (all 0); after the upstream fix it would pass.
