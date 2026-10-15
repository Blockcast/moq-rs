// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod quic;
// Re-exported so callers can name `quinn::Connection` (e.g. to read `stats()`)
// without pinning their own quinn version.
pub use quinn;
pub mod tls;
