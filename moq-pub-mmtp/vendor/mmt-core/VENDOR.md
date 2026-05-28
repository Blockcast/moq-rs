# Vendored mmt-core

Upstream: https://github.com/Blockcast/libmmt (private)
Pinned commit: 929e5b0c7a14f6ffe0ecd50d792fff7cdc44ba0a
Vendored on: 2026-05-28
Source path in upstream: `mmt-core/`

## Why vendored

moq-pub-mmtp must build standalone (without a sibling libmmt checkout) per
M.1 ADR decision A5 (`.planning/moq-rs-m1-adr.md`).

## Local modifications

- `Cargo.toml`: replaced `{ workspace = true }` deps with explicit versions
  (`bytes = "1"`, `thiserror = "1"`); rest verbatim.

## Refresh procedure

```
cd ~/src/pim-multicast-gateway/libmmt && git rev-parse HEAD  # confirm intended ref
rm -rf moq-pub-mmtp/vendor/mmt-core/{src,benches}
cp -r ../../libmmt/mmt-core/{src,benches} moq-pub-mmtp/vendor/mmt-core/
# manually re-merge any local Cargo.toml dep changes
```
