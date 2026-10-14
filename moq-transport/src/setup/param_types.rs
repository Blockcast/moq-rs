// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

/// Setup Option Types per draft-ietf-moq-transport-20 §15.4.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum ParameterType {
    Path = 0x1,
    AuthorizationToken = 0x3,
    MaxAuthTokenCacheSize = 0x4,
    Authority = 0x5,
    /// Total Ranges the sender lets its peer put in one request's Range
    /// Filters (§10.3.1.6). Absent means 0: send no filters.
    MaxFilterRanges = 0x6,
    MOQTImplementation = 0x7,
    /// Unacknowledged REQUEST_UPDATEs the sender accepts per request stream
    /// (§10.3.1.7). Absent or 0 means unlimited.
    MaxRequestUpdates = 0x8,
}

impl From<ParameterType> for u64 {
    fn from(value: ParameterType) -> Self {
        value as u64
    }
}
