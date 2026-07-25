# Vendored mmt-core

Upstream: https://github.com/Blockcast/libmmt (private)
Pinned commit: a5ea68073e306132e95f7ae8610e56117ee4bc5f
Vendored on: 2026-06-12
Source path in upstream: `mmt-core/`

## Why vendored

moq-pub-mmtp must build standalone (without a sibling libmmt checkout) per
M.1 ADR decision A5 (`.planning/moq-rs-m1-adr.md`).

## Local modifications

- `Cargo.toml`: replaced `{ workspace = true }` deps with explicit versions
  (`bytes = "1"`, `thiserror = "1"`) and keeps the upstream default
  `reassembler` feature enabled for standalone builds. None beyond `Cargo.toml`.

## Refresh procedure

```
cd ~/src/pim-multicast-gateway/libmmt && git rev-parse HEAD  # confirm intended ref
rm -rf moq-pub-mmtp/vendor/mmt-core/{src,benches}
cp -r ../../libmmt/mmt-core/{src,benches} moq-pub-mmtp/vendor/mmt-core/
# manually re-merge local Cargo.toml dep changes
# preserve [features] default = ["reassembler"] for standalone builds
diff -r ../../libmmt/mmt-core/src moq-pub-mmtp/vendor/mmt-core/src
diff -r ../../libmmt/mmt-core/benches moq-pub-mmtp/vendor/mmt-core/benches
```
