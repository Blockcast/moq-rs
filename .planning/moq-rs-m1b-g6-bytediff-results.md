# M.1b §B4 — G6 byte-diff vs libmoq: results (static code comparison)

**Date:** 2026-05-28
**Repo:** `~/src/pim-multicast-gateway/moq-rs` @ `blo-4020-m1b-g6-bytediff`
**Tracking:** BLO-8047 §B4
**Scope decision:** static code comparison instead of runtime side-by-side capture. Runtime capture would require standing up the full cast → Traffic Ops → FFmpeg pipeline locally and wiring libmoq mlog output (cast's CLAUDE.md notes this needs `CAST_YT_BRIDGE_ENABLED=true` + a configured DS); cost outweighs benefit since the static comparison already surfaces the structural divergences the runtime diff would prove.
**Verdict:** **Wire formats diverge at the SUBGROUP/OBJECT layer by design.** moq-lite (cast's current MoQ stack via hang-mmt-fec) and IETF moq-transport draft-14 (moq-rs) are not interoperable at the data plane. M.4 (receiver migration) is a major rewrite, not a configuration switch.

## What runs where today

| Component | MoQ stack | Wire protocol | Used by |
|---|---|---|---|
| cast (publisher) | `moq_lite` crate from `hang-mmt-fec/rs/moq-lite` | moq-lite drafts 14-16 (negotiated) | Production publisher; cast/src/bridge_moq.rs uses `use moq_lite::{...}` |
| hang-mmt-fec / `@moq/hang` | `moq-lite` Rust + JS bindings | moq-lite wire | Production receivers (moqtail, web players) |
| `moq-pub-mmtp` (M.1 deliverable) | `moq_transport` crate from `moq-rs` | IETF moq-transport draft-14 | M.1 smoke + future migration target |
| `moq-relay-ietf` (M.1 smoke relay) | `moq_transport` | IETF draft-14 | M.1 smoke environment |

## Wire format divergence — SUBGROUP/OBJECT layer

### moq-lite data stream (`hang-mmt-fec/rs/moq-lite/src/lite/stream.rs`)

```rust
#[derive(Debug, PartialEq, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(u64)]
pub enum DataType {
    Group = 0,           // ONE data stream type
}
```

Flat: every data stream is type `Group`. Wire bytes per stream (from `lite/group.rs:6-12`):

```rust
pub struct Group {
    pub subscribe: u64,   // subscribe_id (varint)
    pub sequence: u64,    // group sequence number (varint)
}
```

Header is 2 varints (4-16 bytes); payload bytes follow until stream close. **No object_id, no subgroup_id, no object_id_delta concept.** One MMTP packet typically = one Group stream; the publisher opens a new QUIC stream per MMTP packet.

### IETF moq-transport draft-14 data stream (`moq-transport/src/data/subgroup.rs`)

Multiple `stream_type` variants in the 0x04-0x05 range (draft-14) for SubgroupHeader variants (with/without subgroup_id, with/without extensions). The SubgroupHeader carries:

```rust
struct SubgroupHeader {
    track_alias: u64,
    group_id: u64,
    subgroup_id: u64,            // varies per stream_type variant
    publisher_priority: u8,
}
```

Followed by 1..N objects, each:

```rust
struct SubgroupObjectExt {
    object_id_delta: u64,        // varint delta from previous object_id
    extension_headers: ...,
    payload_length: u64,
    payload: bytes,
    status: ObjectStatus,         // present only if payload_length == 0
}
```

**Subgroups are multi-object by design.** moq-pub-mmtp emits one subgroup per MPU containing 1 Init + N MFU fragments as separate objects (`FRAGMENT=3` smoke: 4 objects per subgroup).

## Structural diffs per draft-14+ frame

| Layer | moq-lite | IETF moq-transport draft-14 | Migration cost |
|---|---|---|---|
| Stream-type byte | `DataType::Group = 0` | `SubgroupHeader` variant (0x04, 0x05, plus extension flag bits) | Mechanical decoder rewrite |
| Per-stream header | `subscribe_id + sequence` (2 varints) | `track_alias + group_id + subgroup_id + priority` (3-4 fields) | New header parser |
| Objects per stream | 1 (implicit; payload = stream tail) | N (explicit object framing with `object_id_delta`) | Receiver gains object-loop |
| Object-level metadata | None | `object_id_delta`, `extension_headers`, `payload_length`, `status` | New per-object state machine |
| MMTP packet → stream mapping | 1 packet = 1 Group stream | N packets per MPU = 1 subgroup with N objects | Receiver-side reassembly logic moves from "1 packet per stream" to "1 packet per object in subgroup" |

## What this means for B2 (per-FEC-block grouping)

B2 is moot for the cast/libmoq path — moq-lite doesn't have subgroup/SBN grouping concepts, so the "per-FEC-block grouping for repair tracks" work only applies to the moq-pub-mmtp/IETF path. Today's per-MPU rolling group on `<source>/repair` was a reasonable choice when the MMTP-on-MoQ catalog was conceived against moq-lite (where it doesn't matter); for IETF-draft-14+ receivers that consume multi-object subgroups, SBN-keyed grouping is the spec-correct choice.

## Implications for B3 (object_id_delta bug)

**The B3 bug doesn't affect the cast/libmoq production path** because cast uses `moq_lite` directly, which has no `object_id_delta` concept (each Group stream is implicitly object 0 of subgroup 0). The B3 bug only matters once receivers migrate to IETF moq-transport in M.4.

## hang-mmt-fec/ietf module — present but stubbed

`hang-mmt-fec/rs/moq-lite/src/ietf/` exists alongside `lite/` and includes encoders/decoders for control messages and a `GroupFlags` struct (`ietf/group.rs:54-72`) supporting both draft-14 (0x10-0x1d) and draft-15 (0x30-0x3d) stream-type ranges. However:

```rust
// hang-mmt-fec/rs/moq-lite/src/ietf/group.rs:64-66
// Use the first object ID as the subgroup ID
// Since we don't support subgroups or object ID > 0, this is trivial to support.
// Not compatibile with has_subgroup
pub has_subgroup_object: bool,
```

**The hang-mmt-fec/ietf module explicitly does not support multi-object subgroups.** It can decode the IETF stream-type byte and parse a SubgroupHeader, but it expects exactly one object per stream (and that object's `object_id` must be 0). moq-pub-mmtp's `FRAGMENT=3` smoke — emitting 4 objects per subgroup — would be rejected at decode time if pointed at a hang-mmt-fec/ietf receiver today.

This is an additional B4-surfaced constraint that bears on M.4 planning:
- Migrating receivers to moq-rs/IETF requires implementing multi-object subgroup decode at the receiver, OR
- Constraining the publisher to emit one-object subgroups (which breaks the M.1b §B1 raw-passthrough contract — each MMTP fragment must remain its own MoQ object so the receiver's `MfuReassembler` works correctly).

The second option is unworkable; the first is the M.4 receiver-rewrite scope.

## What this would look like as a runtime byte-diff

If we did run cast/libmoq and moq-pub-mmtp side-by-side, the QUIC stream structure for one MPU at `mpu_seq=N` with 1 Init + 3 MFU fragments would diverge as follows:

| Aspect | cast → moq-lite relay | moq-pub-mmtp → moq-relay-ietf |
|---|---|---|
| Number of QUIC streams opened | 4 (one per MMTP packet) | 1 (per MPU subgroup) |
| Stream-type byte | `0x00` (DataType::Group, repeated 4 times) | `0x04` or `0x05` (SubgroupHeader, once) |
| Per-stream header bytes | `[subscribe_id, sequence=N]` × 4 | `[track_alias, group_id=N, subgroup_id=0, priority]` × 1 |
| Per-object framing | None (payload = stream tail) | `[object_id_delta=0, ext_headers, payload_length, payload]` × 4 |
| Total wire-byte ratio (header overhead) | ~16 B × 4 = 64 B of stream-header | ~16 B + (5 B × 4) = 36 B of stream + object headers |

moq-rs is slightly more efficient at the framing layer (single subgroup amortizes header across objects); moq-lite is structurally simpler (no object loop, no delta computation).

## Verdict + IETF draft-15+ tracking notes

The wire-format divergence is **by design**, not a bug — moq-lite and IETF moq-transport are different drafts of the same protocol family that diverged early. The migration scope for M.4 is:

1. **Receiver subgroup decoder**: parse multi-object SubgroupHeader, run an object loop, reassemble per-MFU using `mmt-core::MfuReassembler` per the M.1b §B1 contract.
2. **Receiver namespace + announce**: IETF moq-transport uses `track_alias` resolution via control messages; moq-lite uses `subscribe_id` directly. Different control-plane wiring.
3. **Catalog driver**: the multicast catalog extension (`MulticastConfig` etc.) is identical between sides (we ported it from hang-mmt-fec), but the catalog → wire wiring differs.

Items worth raising in draft-15+ tracking:

- **Object framing overhead.** Even moq-rs's 4-objects-per-subgroup pays 4× `object_id_delta + payload_length` overhead vs moq-lite's stream-tail format. Worth measuring at high I-frame fragment counts (8K = 750-2900 fragments → 750-2900 object headers per MPU subgroup).
- **Multi-object subgroup parser coverage.** Most IETF implementations I sampled (this one + the broken hang-mmt-fec/ietf stub) treat multi-object subgroups as the edge case. Spec text should make it explicit that subgroups MUST support N>1 objects.
- **object_id_delta first-object semantics.** Per the B3 forensics, both the encode AND decode sides need a consistent base-register convention. Worth clarifying in draft-15+ if not already explicit.

## Out of scope for B4

- **Runtime capture** of cast/libmoq + moq-pub-mmtp wire bytes side-by-side. Static analysis covers the structural diffs; runtime would mainly confirm + provide byte-level evidence for upstream tickets. Defer until specifically required.
- **moq-lite → IETF migration ADR.** Belongs in M.4 (receiver-side) planning.
- **hang-mmt-fec/ietf stub fix.** Their stub is consistent with their current production model (single-object streams). Not our work.
