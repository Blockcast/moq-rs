// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;

use crate::serve::FullTrackName;

/// Which subscription owns a given Track Alias (draft-16 §10.1).
///
/// SUBSCRIBE and PUBLISH share one session-scoped alias namespace, so a single
/// `track_alias_map` keyed by alias resolves inbound streams/datagrams to the
/// correct receiver.
#[derive(Clone, Copy, Debug)]
pub(super) enum TrackOrigin {
    /// Alias belongs to an outbound SUBSCRIBE; carries the subscribe request id.
    Subscribe(u64),
    /// Alias belongs to an inbound PUBLISH; carries the PUBLISH request id.
    Publish(u64),
}

impl TrackOrigin {
    pub(super) fn request_id(self) -> u64 {
        match self {
            Self::Subscribe(id) | Self::Publish(id) => id,
        }
    }
}

#[derive(Default)]
pub(super) struct TrackAliasRegistry {
    by_alias: HashMap<u64, TrackOrigin>,
    by_request_id: HashMap<u64, u64>,
}

impl TrackAliasRegistry {
    pub(super) fn contains_alias(&self, alias: u64) -> bool {
        self.by_alias.contains_key(&alias)
    }

    pub(super) fn get(&self, alias: u64) -> Option<TrackOrigin> {
        self.by_alias.get(&alias).copied()
    }

    pub(super) fn insert(&mut self, alias: u64, origin: TrackOrigin) -> Result<(), ()> {
        if self.by_alias.contains_key(&alias) {
            return Err(());
        }

        if let Some(old_alias) = self.by_request_id.insert(origin.request_id(), alias) {
            self.by_alias.remove(&old_alias);
        }
        self.by_alias.insert(alias, origin);
        Ok(())
    }

    pub(super) fn remove_by_request_id(&mut self, request_id: u64) -> Option<TrackOrigin> {
        let alias = self.by_request_id.remove(&request_id)?;
        self.by_alias.remove(&alias)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.by_alias.is_empty() && self.by_request_id.is_empty()
    }
}

#[derive(Default)]
pub(super) struct SubscriberNameRegistry {
    by_name: HashMap<FullTrackName, u64>,
    by_request_id: HashMap<u64, FullTrackName>,
}

impl SubscriberNameRegistry {
    pub(super) fn contains_name(&self, name: &FullTrackName) -> bool {
        self.by_name.contains_key(name)
    }

    pub(super) fn insert(&mut self, name: FullTrackName, request_id: u64) {
        if let Some(old_name) = self.by_request_id.insert(request_id, name.clone()) {
            self.by_name.remove(&old_name);
        }
        if let Some(old_id) = self.by_name.insert(name, request_id) {
            self.by_request_id.remove(&old_id);
        }
    }

    pub(super) fn remove_by_request_id(&mut self, request_id: u64) -> Option<FullTrackName> {
        let name = self.by_request_id.remove(&request_id)?;
        self.by_name.remove(&name);
        Some(name)
    }
}
