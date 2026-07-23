<!--
SPDX-FileCopyrightText: 2025-2026 Blockcast Inc. and contributors
SPDX-License-Identifier: Apache-2.0
-->

# MMTP carriage for moq-rs

`mmtp-moq-carriage` maps complete MMTP packets to MoQT subgroup objects without
remuxing them to fMP4. The caller supplies group and subgroup identity from its
MMTP timeline and can attach compact AL-FEC metadata under an extension ID
selected by its negotiated application profile.

The crate deliberately does not assign an unregistered MoQT extension ID. It
also does not implement an FEC codec, source authentication, or path selection.
Those remain application concerns. The transport payload is opaque, allowing
DSR and `mmt-core` producers to retain their MMTP wire representation.

The fixture `tests/fixtures/moqt-16-v1.json` is the immutable draft-16 contract
shared with those producers. Its FNV-1a digest is pinned in the test suite so a
wire-contract update must be explicit.
