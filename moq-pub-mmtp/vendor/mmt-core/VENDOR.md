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

## Refresh procedure

```
cd ~/src/pim-multicast-gateway/libmmt && git rev-parse HEAD  # confirm intended ref
rm -rf moq-pub-mmtp/vendor/mmt-core/{src,benches}
cp -r ../../libmmt/mmt-core/{src,benches} moq-pub-mmtp/vendor/mmt-core/
# manually re-merge any local Cargo.toml dep changes
```
