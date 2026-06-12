# Vendored mmt-core

Upstream: https://github.com/Blockcast/libmmt (private)
Pinned commit: e8ccd75aab1fc7b269ea3a3e559703c1a98e66de
Vendored on: 2026-06-01
Source path in upstream: `mmt-core/`

## Why vendored

moq-pub-mmtp must build standalone (without a sibling libmmt checkout) per
M.1 ADR decision A5 (`.planning/moq-rs-m1-adr.md`).

## Local modifications

- `Cargo.toml`: replaced `{ workspace = true }` deps with explicit versions
  (`bytes = "1"`, `thiserror = "1"`); rest verbatim.
- `src/header.rs`: carries temporary review-hardening changes until the matching
  libmmt PR lands and this vendor pin is refreshed:
  - `MmtpHeaderExt::has_source_fec_trailer()` is the single source of truth for
    Source FEC Payload ID trailer presence, derived from `base.fec_type`.
  - `write_source_fec_payload_id_trailer()` debug-asserts that `fec_type` and
    `source_fec_payload_id` agree on trailer presence.
  - `MmtpHeaderExt::read_from()` documents that `source_fec_payload_id` is never
    populated on read because SS_ID is a payload trailer, not a header prefix.
  - `split_payload_and_source_fec_trailer()` uses
    `has_source_fec_trailer()` instead of duplicating the `fec_type` check.

After the libmmt hardening PR merges, re-pin to that commit, refresh from
upstream, and reduce this section back to "none beyond `Cargo.toml`" if the
post-refresh diff confirms no remaining `src/header.rs` divergence.

## Refresh procedure

```
cd ~/src/pim-multicast-gateway/libmmt && git rev-parse HEAD  # confirm intended ref
rm -rf moq-pub-mmtp/vendor/mmt-core/{src,benches}
cp -r ../../libmmt/mmt-core/{src,benches} moq-pub-mmtp/vendor/mmt-core/
# manually re-merge local Cargo.toml dep changes
# if refreshing to libmmt >= e45d644, also preserve:
# [features]
# default = ["reassembler"]
# because upstream gated the reassembler behind a cargo feature
diff -r ../../libmmt/mmt-core/src moq-pub-mmtp/vendor/mmt-core/src
diff -r ../../libmmt/mmt-core/benches moq-pub-mmtp/vendor/mmt-core/benches
```
