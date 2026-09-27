// SPDX-FileCopyrightText: 2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Bounded retention of recent groups, so the relay can answer a standalone
//! FETCH from Objects it has already received (draft-ietf-moq-transport-16
//! §8.1 "Relays MAY cache Objects", §9.16.3; cloudflare/moq-rs#59, #60).
//!
//! Every track the relay receives can be retained: tracks published to it,
//! tracks it pulls through from a PUBLISH_NAMESPACE source, and tracks it pulls
//! from a remote relay. Retention starts when the track enters the relay's
//! registry and ends when the relay drops the track, so a FETCH is answered
//! locally only while the relay still holds the track.
//!
//! The relay never infers the status of an Object it did not receive. An
//! Object ID gap between retained Objects says nothing about the skipped
//! Objects (§10.4.2), and the relay does not keep Object Status, so it cannot
//! tell where a group ends except at the track's Largest Location. A FETCH is
//! answered entirely from retention only when every Location in its range is
//! retained; otherwise [`RetainedTrack::plan`] reports where the unknown part
//! begins.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::mem::{size_of, size_of_val};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::{stream::FuturesUnordered, StreamExt};
use moq_transport::{
    coding::{Location, Value, VarInt},
    data::{FetchObject, ObjectStatus},
    message::GroupOrder,
    serve::{FullTrackName, SubgroupReader, TrackReader, TrackTap, TrackTapEvent},
    session::FetchResponseObject,
};

/// The largest Object ID a Location can carry. A request whose End Location
/// Object is 0 asks for the whole group (§9.16.1), i.e. up to this ID.
const END_OF_GROUP: u64 = VarInt::MAX.into_inner();

/// Which tracks' recent groups the relay retains for FETCH.
///
/// Retention is relay policy (draft-ramadan-moq-multicast §6.2 leaves how long
/// a relay retains groups to the relay), so it is configured explicitly rather
/// than defaulted.
#[derive(Clone)]
pub struct FetchRetention {
    inner: Option<Arc<Registry>>,
}

struct Registry {
    limits: Arc<Limits>,
    tracks: Mutex<HashMap<RetentionKey, Weak<RetainedTrack>>>,
}

/// What the relay may retain. The group count alone does not bound memory, so
/// the retained bytes (see [`retained_bytes`]) are bounded per track and across
/// all tracks.
struct Limits {
    groups: NonZeroU64,
    track_bytes: NonZeroUsize,
    total_bytes: NonZeroUsize,
    /// Bytes retained across all tracks.
    used_bytes: AtomicUsize,
}

impl Limits {
    /// Take `bytes` from the registry-wide budget, or leave it unchanged and
    /// return false when they do not fit.
    fn reserve(&self, bytes: usize) -> bool {
        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|used| *used <= self.total_bytes.get())
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}

type RetentionKey = (String, FullTrackName);

impl FetchRetention {
    /// Retain nothing. Every FETCH is forwarded upstream.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Retain the Objects of the `groups` most recently arrived groups of each
    /// track, holding at most `track_bytes` per track and `total_bytes` across
    /// all tracks, as counted by [`retained_bytes`]. To make room the relay
    /// drops whole groups, least recently arrived first; an Object that does
    /// not fit even then is not retained and evicts nothing.
    pub fn new(groups: NonZeroU64, track_bytes: NonZeroUsize, total_bytes: NonZeroUsize) -> Self {
        Self {
            inner: Some(Arc::new(Registry {
                limits: Arc::new(Limits {
                    groups,
                    track_bytes,
                    total_bytes,
                    used_bytes: AtomicUsize::new(0),
                }),
                tracks: Mutex::default(),
            })),
        }
    }

    /// The configured number of groups, or `None` when retention is disabled.
    pub fn retained_groups(&self) -> Option<NonZeroU64> {
        self.inner.as_ref().map(|registry| registry.limits.groups)
    }

    /// Start retaining `reader`'s Objects for FETCH requests in `scope`.
    ///
    /// Retention lasts as long as the returned guard: hold it next to the
    /// registry entry for the track and drop it with the entry. A newer
    /// registration for the same track and scope replaces this one.
    pub(crate) fn retain(
        &self,
        scope: Option<&str>,
        reader: &TrackReader,
    ) -> Option<RetentionGuard> {
        let registry = self.inner.as_ref()?;
        let key = (
            scope.unwrap_or_default().to_string(),
            FullTrackName {
                namespace: reader.namespace.clone(),
                name: reader.name.clone(),
            },
        );
        let track = Arc::new(RetainedTrack {
            reader: reader.clone(),
            limits: registry.limits.clone(),
            groups: Mutex::default(),
        });
        registry
            .tracks
            .lock()
            .ok()?
            .insert(key.clone(), Arc::downgrade(&track));
        let task = tokio::spawn(feed(reader.tap(), Arc::downgrade(&track)));

        Some(RetentionGuard {
            registry: Arc::downgrade(registry),
            key,
            track,
            task,
        })
    }

    /// The retained track for a FETCH in `scope`, if the relay holds one.
    pub(crate) fn lookup(
        &self,
        scope: Option<&str>,
        name: &FullTrackName,
    ) -> Option<Arc<RetainedTrack>> {
        let registry = self.inner.as_ref()?;
        let key = (scope.unwrap_or_default().to_string(), name.clone());
        let mut tracks = registry.tracks.lock().ok()?;
        match tracks.get(&key).map(Weak::upgrade) {
            Some(Some(track)) => Some(track),
            Some(None) => {
                tracks.remove(&key);
                None
            }
            None => None,
        }
    }
}

/// Keeps a track's retention alive. Dropping it stops retention and frees the
/// retained Objects.
pub(crate) struct RetentionGuard {
    registry: Weak<Registry>,
    key: RetentionKey,
    track: Arc<RetainedTrack>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RetentionGuard {
    fn drop(&mut self) {
        self.task.abort();
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let Ok(mut tracks) = registry.tracks.lock() else {
            return;
        };
        // A replacement registration owns the key now; leave it alone.
        let ours = tracks
            .get(&self.key)
            .is_some_and(|current| std::ptr::eq(current.as_ptr(), Arc::as_ptr(&self.track)));
        if ours {
            tracks.remove(&self.key);
        }
    }
}

/// The retained Objects of one track.
pub(crate) struct RetainedTrack {
    /// The live track, for its Largest Location.
    reader: TrackReader,
    limits: Arc<Limits>,
    groups: Mutex<RetainedGroups>,
}

impl Drop for RetainedTrack {
    fn drop(&mut self) {
        let bytes = self
            .groups
            .get_mut()
            .map_or_else(|poisoned| poisoned.into_inner().bytes, |state| state.bytes);
        self.limits.release(bytes);
    }
}

#[derive(Default)]
struct RetainedGroups {
    /// Retained groups by Group ID.
    groups: BTreeMap<u64, RetainedGroup>,
    /// Retained Group IDs, least recently arrived first. Group ID is
    /// peer-supplied, so eviction follows arrival: an outlier Group ID ages out
    /// like any other group instead of pinning the window.
    arrival: VecDeque<u64>,
    /// The sum of every group's `bytes`.
    bytes: usize,
}

#[derive(Default)]
struct RetainedGroup {
    /// Retained Objects by Object ID.
    objects: BTreeMap<u64, FetchResponseObject>,
    /// The sum of [`retained_bytes`] over `objects`.
    bytes: usize,
}

impl RetainedGroups {
    /// Drop the least recently arrived group, returning its bytes to `limits`.
    fn evict_oldest(&mut self, limits: &Limits) {
        let Some(group_id) = self.arrival.pop_front() else {
            return;
        };
        if let Some(group) = self.groups.remove(&group_id) {
            self.bytes -= group.bytes;
            limits.release(group.bytes);
        }
    }
}

/// The memory an Object holds while retained: its payload and extension
/// header values plus the structures that carry them. Every Object costs at
/// least `size_of::<FetchResponseObject>()`, so Objects with an empty payload
/// are bounded too. Shared payload chunks are counted in full, since retention
/// may be their last owner.
fn retained_bytes(object: &FetchResponseObject) -> usize {
    let headers = object.object.extension_headers.0.as_slice();
    let header_values: usize = headers
        .iter()
        .map(|header| match &header.value {
            Value::BytesValue(bytes) => bytes.len(),
            Value::IntValue(_) => 0,
        })
        .sum();
    size_of::<FetchResponseObject>()
        + size_of_val(object.payload.as_slice())
        + object.object.payload_length
        + size_of_val(headers)
        + header_values
}

/// How a FETCH range relates to what the relay retains.
#[derive(Debug)]
pub(crate) enum FetchPlan {
    /// Every Location of the range is retained.
    Complete {
        objects: Vec<FetchResponseObject>,
        end_location: Location,
    },

    /// `objects` are the leading Objects of the range, in response order, and
    /// the next Location, `first_unknown`, is not retained. `last_location` is
    /// the last Location of the range in response order.
    Partial {
        objects: Vec<FetchResponseObject>,
        first_unknown: Location,
        last_location: Location,
        end_location: Location,
    },

    /// The range starts after the Largest Location the relay knows of.
    BeyondLargest,

    /// The relay has received nothing on the track yet.
    NothingPublished,
}

impl RetainedTrack {
    fn insert(&self, object: FetchResponseObject) {
        let Ok(mut state) = self.groups.lock() else {
            return;
        };
        let state = &mut *state;
        let group_id = object.object.group_id;
        let object_id = object.object.object_id;
        // §8.1: a relay MAY ignore a later copy of an Object it already holds.
        if state
            .groups
            .get(&group_id)
            .is_some_and(|group| group.objects.contains_key(&object_id))
        {
            return;
        }

        // Only groups that arrived before this Object's group may be dropped
        // to make room. An Object that does not fit beside its own group and
        // the groups after it is not retained and evicts nothing, so its
        // Location stays unknown and a FETCH for it goes upstream.
        let cost = retained_bytes(&object);
        let mut older_groups = state
            .arrival
            .iter()
            .position(|arrived| *arrived == group_id)
            .unwrap_or(state.arrival.len());
        let older_bytes: usize = state
            .arrival
            .iter()
            .take(older_groups)
            .filter_map(|arrived| state.groups.get(arrived))
            .map(|group| group.bytes)
            .sum();
        let fits_track = (state.bytes - older_bytes)
            .checked_add(cost)
            .is_some_and(|bytes| bytes <= self.limits.track_bytes.get());
        let fits_total = self
            .limits
            .used_bytes
            .load(Ordering::Acquire)
            .saturating_sub(older_bytes)
            .checked_add(cost)
            .is_some_and(|bytes| bytes <= self.limits.total_bytes.get());
        if !(fits_track && fits_total) {
            tracing::debug!(
                namespace = %self.reader.namespace,
                track = %self.reader.name,
                group_id,
                object_id,
                cost,
                "object exceeds the fetch retention byte budget; not retained"
            );
            return;
        }
        loop {
            let fits_track = state
                .bytes
                .checked_add(cost)
                .is_some_and(|bytes| bytes <= self.limits.track_bytes.get());
            if fits_track && self.limits.reserve(cost) {
                break;
            }
            if older_groups == 0 {
                // Another track took the shared budget since the check above.
                tracing::debug!(
                    namespace = %self.reader.namespace,
                    track = %self.reader.name,
                    group_id,
                    object_id,
                    cost,
                    "fetch retention byte budget taken by another track; not retained"
                );
                return;
            }
            state.evict_oldest(&self.limits);
            older_groups -= 1;
        }

        if !state.groups.contains_key(&group_id) {
            state.arrival.push_back(group_id);
        }
        let group = state.groups.entry(group_id).or_default();
        group.objects.insert(object_id, object);
        group.bytes += cost;
        state.bytes += cost;

        // The window holds the `groups` most recently arrived groups. Only a
        // new group can exceed it, and a new group arrives last, so this
        // Object's group is never the one dropped here.
        while state.groups.len() as u64 > self.limits.groups.get() {
            state.evict_oldest(&self.limits);
        }
    }

    /// The Largest Location the relay knows of: the live track's, or a later
    /// retained Object's.
    fn largest_location(&self) -> Option<Location> {
        let retained = self.groups.lock().ok().and_then(|state| {
            state
                .groups
                .iter()
                .next_back()
                .and_then(|(group_id, group)| {
                    group
                        .objects
                        .keys()
                        .next_back()
                        .map(|object_id| Location::new(*group_id, *object_id))
                })
        });
        match (self.reader.largest_location(), retained) {
            (Some(live), Some(retained)) => Some(live.max(retained)),
            (live, retained) => live.or(retained),
        }
    }

    /// Match a Standalone FETCH range (§9.16.1: `end` is the End Location plus
    /// one, with Object 0 meaning the whole group) against the retained Objects.
    pub(crate) fn plan(&self, start: Location, end: Location, order: GroupOrder) -> FetchPlan {
        let Some(largest) = self.largest_location() else {
            return FetchPlan::NothingPublished;
        };
        // §9.16.3: "If Start Location is greater than the Largest Object the
        // publisher MUST return REQUEST_ERROR with error code INVALID_RANGE."
        if start > largest {
            return FetchPlan::BeyondLargest;
        }

        let requested_last = inclusive_end(end);
        // §9.17 End Location: {Largest.Group, Largest.Object + 1} when the
        // request reaches beyond the Largest Object, otherwise the requested
        // End Location (which already covers the whole-group form).
        let end_location = if requested_last > largest {
            Location::new(largest.group_id, largest.object_id.saturating_add(1))
        } else {
            end
        };
        // §9.16.3: "Objects that are not yet published will not be retrieved".
        let last = requested_last.min(largest);
        if start > last {
            // Start equals End with a non-zero Object: an empty range.
            return FetchPlan::Complete {
                objects: Vec::new(),
                end_location,
            };
        }

        // §9.16.3: groups in the requested order, objects in Object ID order.
        let last_location = match order {
            GroupOrder::Descending if start.group_id != last.group_id => {
                Location::new(start.group_id, END_OF_GROUP)
            }
            _ => last,
        };
        let group_ids: Box<dyn Iterator<Item = u64>> = match order {
            GroupOrder::Descending => Box::new((start.group_id..=last.group_id).rev()),
            _ => Box::new(start.group_id..=last.group_id),
        };

        let Ok(state) = self.groups.lock() else {
            return FetchPlan::Partial {
                objects: Vec::new(),
                first_unknown: start,
                last_location,
                end_location,
            };
        };

        let mut objects = Vec::new();
        for group_id in group_ids {
            let first = if group_id == start.group_id {
                start.object_id
            } else {
                0
            };
            // Only the last requested group has a known final Object; any other
            // group runs until the first Object that is not retained.
            let final_object = if group_id == last.group_id {
                last.object_id
            } else {
                END_OF_GROUP
            };
            let retained = state.groups.get(&group_id);

            let mut object_id = first;
            loop {
                match retained.and_then(|group| group.objects.get(&object_id)) {
                    Some(object) => objects.push(object.clone()),
                    None => {
                        return FetchPlan::Partial {
                            objects,
                            first_unknown: Location::new(group_id, object_id),
                            last_location,
                            end_location,
                        }
                    }
                }
                if object_id >= final_object {
                    break;
                }
                object_id += 1;
            }
        }

        FetchPlan::Complete {
            objects,
            end_location,
        }
    }
}

/// The last requested Location of a Standalone FETCH End Location (§9.16.1).
fn inclusive_end(end: Location) -> Location {
    match end.object_id.checked_sub(1) {
        Some(object_id) => Location::new(end.group_id, object_id),
        None => Location::new(end.group_id, END_OF_GROUP),
    }
}

/// Copy every complete Object written to the track into `retained` until the
/// track's writer is gone or retention is dropped.
///
/// The tap is taken before this task first runs, so a subgroup or datagram
/// written in between is not missed, and it sees every subgroup rather than
/// only the latest one a `SubgroupsReader` would return.
async fn feed(mut tap: TrackTap, retained: Weak<RetainedTrack>) {
    let mut subgroups = FuturesUnordered::new();
    loop {
        tokio::select! {
            event = tap.next() => match event {
                Some(TrackTapEvent::Subgroup(subgroup)) => {
                    subgroups.push(feed_subgroup(subgroup, retained.clone()));
                }
                Some(TrackTapEvent::Datagram(datagram)) => {
                    let Some(retained) = retained.upgrade() else {
                        return;
                    };
                    retained.insert(FetchResponseObject {
                        object: FetchObject {
                            group_id: datagram.group_id,
                            subgroup_id: None,
                            object_id: datagram.object_id,
                            publisher_priority: datagram.priority,
                            extension_headers: datagram.extension_headers,
                            payload_length: datagram.payload.len(),
                        },
                        payload: vec![datagram.payload],
                    });
                }
                None => break,
            },
            Some(()) = subgroups.next(), if !subgroups.is_empty() => {}
        }
    }
    while subgroups.next().await.is_some() {}
}

async fn feed_subgroup(mut subgroup: SubgroupReader, track: Weak<RetainedTrack>) {
    while let Ok(Some(object)) = subgroup.next().await {
        // FETCH carries no Object Status (§10.2.1.1). A status Object is not
        // retained, so its Location stays unknown rather than being served as
        // an empty Normal Object.
        if object.status != ObjectStatus::NormalObject {
            continue;
        }

        // Wait for the whole payload; an Object that ends short is never
        // retained. The chunks are shared, not copied.
        let mut payload = Vec::new();
        let mut chunks = object.clone();
        let complete = loop {
            match chunks.read().await {
                Ok(Some(chunk)) => payload.push(chunk),
                Ok(None) => break true,
                Err(_) => break false,
            }
        };
        if !complete {
            continue;
        }

        let Some(track) = track.upgrade() else {
            return;
        };
        track.insert(FetchResponseObject {
            object: FetchObject {
                group_id: subgroup.group_id,
                subgroup_id: Some(subgroup.subgroup_id),
                object_id: object.object_id,
                publisher_priority: subgroup.priority,
                extension_headers: object.extension_headers.clone(),
                payload_length: object.size,
            },
            payload,
        });
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use moq_transport::{
        coding::{KeyValuePair, TrackNamespace},
        data::ExtensionHeaders,
        serve::{Subgroup, SubgroupsWriter, Track, TrackWriter},
    };

    use super::*;

    const WINDOW: u64 = 3;

    /// The retained bytes of every test object with one-digit IDs, whose
    /// payloads, `g{group}o{object}`, are all four bytes long.
    fn object_bytes() -> usize {
        retained_bytes(&object(0, 0))
    }

    fn limits(track_bytes: NonZeroUsize, total_bytes: NonZeroUsize) -> Arc<Limits> {
        Arc::new(Limits {
            groups: NonZeroU64::new(WINDOW).unwrap(),
            track_bytes,
            total_bytes,
            used_bytes: AtomicUsize::new(0),
        })
    }

    fn unbounded() -> Arc<Limits> {
        limits(NonZeroUsize::MAX, NonZeroUsize::MAX)
    }

    fn track_with(limits: Arc<Limits>) -> (TrackWriter, Arc<RetainedTrack>) {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let track = Arc::new(RetainedTrack {
            reader,
            limits,
            groups: Mutex::default(),
        });
        (writer, track)
    }

    fn track() -> (TrackWriter, Arc<RetainedTrack>) {
        track_with(unbounded())
    }

    fn bytes(objects: usize) -> NonZeroUsize {
        NonZeroUsize::new(objects * object_bytes()).unwrap()
    }

    fn is_complete(track: &RetainedTrack, group_id: u64, objects: u64) -> bool {
        matches!(
            track.plan(
                Location::new(group_id, 0),
                Location::new(group_id, objects),
                GroupOrder::Ascending
            ),
            FetchPlan::Complete { .. }
        )
    }

    fn is_absent(track: &RetainedTrack, group_id: u64) -> bool {
        matches!(
            track.plan(
                Location::new(group_id, 0),
                Location::new(group_id, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::Partial { ref objects, first_unknown, .. }
                if objects.is_empty() && first_unknown == Location::new(group_id, 0)
        )
    }

    /// Nothing at or after `{group_id, 0}` is retained or published.
    fn is_beyond_largest(track: &RetainedTrack, group_id: u64) -> bool {
        matches!(
            track.plan(
                Location::new(group_id, 0),
                Location::new(group_id, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::BeyondLargest
        )
    }

    fn object(group_id: u64, object_id: u64) -> FetchResponseObject {
        object_with(group_id, object_id, format!("g{group_id}o{object_id}"))
    }

    fn object_with(
        group_id: u64,
        object_id: u64,
        payload: impl Into<Bytes>,
    ) -> FetchResponseObject {
        let payload = payload.into();
        FetchResponseObject {
            object: FetchObject {
                group_id,
                subgroup_id: Some(0),
                object_id,
                publisher_priority: 240,
                extension_headers: Default::default(),
                payload_length: payload.len(),
            },
            payload: vec![payload],
        }
    }

    fn locations(objects: &[FetchResponseObject]) -> Vec<(u64, u64)> {
        objects
            .iter()
            .map(|o| (o.object.group_id, o.object.object_id))
            .collect()
    }

    fn retain(track: &RetainedTrack, group_id: u64, object_ids: impl IntoIterator<Item = u64>) {
        for object_id in object_ids {
            track.insert(object(group_id, object_id));
        }
    }

    #[test]
    fn explicit_range_inside_a_retained_group_is_complete() {
        let (_writer, track) = track();
        retain(&track, 4, 0..5);
        retain(&track, 5, 0..2);

        // Objects 1..=3 of group 4: End Location {4, 4} is exclusive.
        let FetchPlan::Complete {
            objects,
            end_location,
        } = track.plan(
            Location::new(4, 1),
            Location::new(4, 4),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a complete plan");
        };
        assert_eq!(locations(&objects), vec![(4, 1), (4, 2), (4, 3)]);
        assert_eq!(end_location, Location::new(4, 4));
    }

    #[test]
    fn whole_older_group_is_partial_because_its_end_is_unknown() {
        let (_writer, track) = track();
        retain(&track, 4, 0..5);
        retain(&track, 5, 0..2);

        // End Location {4, 0} asks for all of group 4. Nothing says object 4
        // was the last one, so object 5 has unknown status.
        let FetchPlan::Partial {
            objects,
            first_unknown,
            last_location,
            end_location,
        } = track.plan(
            Location::new(4, 0),
            Location::new(4, 0),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a partial plan");
        };
        assert_eq!(locations(&objects).len(), 5);
        assert_eq!(first_unknown, Location::new(4, 5));
        assert_eq!(last_location, Location::new(4, END_OF_GROUP));
        assert_eq!(end_location, Location::new(4, 0));
    }

    #[test]
    fn whole_group_at_the_largest_location_is_complete_up_to_it() {
        let (_writer, track) = track();
        retain(&track, 7, 0..3);

        // §9.16.3: objects not yet published are not retrieved, and §9.17 sets
        // End Location to {Largest.Group, Largest.Object + 1}.
        let FetchPlan::Complete {
            objects,
            end_location,
        } = track.plan(
            Location::new(7, 0),
            Location::new(8, 0),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a complete plan");
        };
        assert_eq!(locations(&objects), vec![(7, 0), (7, 1), (7, 2)]);
        assert_eq!(end_location, Location::new(7, 3));
    }

    #[test]
    fn an_object_id_gap_is_unknown_not_absent() {
        let (_writer, track) = track();
        retain(&track, 2, [0, 1, 3, 4]);

        let FetchPlan::Partial {
            objects,
            first_unknown,
            ..
        } = track.plan(
            Location::new(2, 0),
            Location::new(2, 5),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a partial plan");
        };
        assert_eq!(locations(&objects), vec![(2, 0), (2, 1)]);
        assert_eq!(first_unknown, Location::new(2, 2));
    }

    #[test]
    fn a_range_that_crosses_a_group_boundary_stops_at_the_boundary() {
        let (_writer, track) = track();
        retain(&track, 3, 0..2);
        retain(&track, 4, 0..2);

        let FetchPlan::Partial {
            objects,
            first_unknown,
            ..
        } = track.plan(
            Location::new(3, 0),
            Location::new(4, 2),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a partial plan");
        };
        assert_eq!(locations(&objects), vec![(3, 0), (3, 1)]);
        assert_eq!(first_unknown, Location::new(3, 2));
    }

    #[test]
    fn descending_order_starts_with_the_highest_group() {
        let (_writer, track) = track();
        retain(&track, 3, 0..2);
        retain(&track, 4, 0..2);

        let FetchPlan::Partial {
            objects,
            first_unknown,
            last_location,
            ..
        } = track.plan(
            Location::new(3, 0),
            Location::new(4, 2),
            GroupOrder::Descending,
        )
        else {
            panic!("expected a partial plan");
        };
        // Group 4 objects 0..=1 are the whole requested part of group 4, then
        // group 3 runs until its unknown end.
        assert_eq!(locations(&objects), vec![(4, 0), (4, 1), (3, 0), (3, 1)]);
        assert_eq!(first_unknown, Location::new(3, 2));
        assert_eq!(last_location, Location::new(3, END_OF_GROUP));
    }

    #[test]
    fn start_after_the_largest_location_is_beyond_largest() {
        let (_writer, track) = track();
        retain(&track, 2, 0..3);

        assert!(matches!(
            track.plan(
                Location::new(2, 3),
                Location::new(2, 0),
                GroupOrder::Ascending
            ),
            FetchPlan::BeyondLargest
        ));
    }

    #[test]
    fn an_empty_track_has_nothing_published() {
        let (_writer, track) = track();
        assert!(matches!(
            track.plan(
                Location::new(0, 0),
                Location::new(0, 0),
                GroupOrder::Ascending
            ),
            FetchPlan::NothingPublished
        ));
    }

    #[test]
    fn groups_outside_the_window_are_evicted() {
        let (_writer, track) = track();
        for group_id in 0..6 {
            retain(&track, group_id, 0..2);
        }
        // Window 3 keeps groups 3, 4 and 5.
        assert!(matches!(
            track.plan(
                Location::new(2, 0),
                Location::new(2, 2),
                GroupOrder::Ascending
            ),
            FetchPlan::Partial { ref objects, first_unknown, .. }
                if objects.is_empty() && first_unknown == Location::new(2, 0)
        ));
        assert!(matches!(
            track.plan(
                Location::new(3, 0),
                Location::new(3, 2),
                GroupOrder::Ascending
            ),
            FetchPlan::Complete { .. }
        ));
        // Eviction follows arrival, not Group ID: a late object of an evicted
        // group arrives last, so the least recently arrived group, 3, goes.
        track.insert(object(1, 0));
        assert!(is_complete(&track, 1, 1));
        assert!(is_absent(&track, 3));
        assert!(is_complete(&track, 4, 2));
        assert!(is_complete(&track, 5, 2));
    }

    #[test]
    fn an_outlier_group_id_ages_out_instead_of_disabling_retention() {
        let (_writer, track) = track();
        let outlier = VarInt::MAX.into_inner();
        track.insert(object(outlier, 0));
        for group_id in 1..=4 {
            retain(&track, group_id, 0..2);
        }

        assert!(is_complete(&track, 3, 2));
        assert!(is_complete(&track, 4, 2));
        // The outlier arrived first and was evicted, so it is no longer the
        // Largest Location either.
        assert!(matches!(
            track.plan(
                Location::new(outlier, 0),
                Location::new(outlier, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::BeyondLargest
        ));
    }

    #[test]
    fn a_group_that_never_ends_is_bounded_by_the_track_byte_budget() {
        let (_writer, track) = track_with(limits(bytes(4), NonZeroUsize::MAX));
        retain(&track, 1, 0..2);
        // Group 2 grows past the budget: group 1 goes first, then Objects that
        // do not fit beside the rest of group 2 are not retained.
        retain(&track, 2, 0..6);

        assert!(is_absent(&track, 1));
        // Nothing records Objects 4 and 5 (there is no live track here), so the
        // retained Largest Location, {2, 3}, ends the range (§9.16.3).
        let FetchPlan::Complete {
            objects,
            end_location,
        } = track.plan(
            Location::new(2, 0),
            Location::new(2, 6),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a complete plan");
        };
        assert_eq!(locations(&objects), vec![(2, 0), (2, 1), (2, 2), (2, 3)]);
        assert_eq!(end_location, Location::new(2, 4));
        assert_eq!(
            track.limits.used_bytes.load(Ordering::Acquire),
            4 * object_bytes()
        );
    }

    #[test]
    fn the_total_byte_budget_is_shared_and_released_when_a_track_drops() {
        let shared = limits(bytes(2), bytes(2));
        let (_first_writer, first) = track_with(shared.clone());
        let (_second_writer, second) = track_with(shared.clone());

        retain(&first, 1, 0..2);
        // The first track holds the whole budget and the second has nothing of
        // its own to evict, so its Object is not retained.
        second.insert(object(7, 0));
        assert!(matches!(
            second.plan(
                Location::new(7, 0),
                Location::new(7, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::NothingPublished
        ));
        assert!(is_complete(&first, 1, 2));

        drop(first);
        assert_eq!(shared.used_bytes.load(Ordering::Acquire), 0);
        second.insert(object(7, 0));
        assert!(is_complete(&second, 7, 1));
        assert_eq!(shared.used_bytes.load(Ordering::Acquire), object_bytes());
    }

    #[test]
    fn objects_without_payload_are_charged_their_extension_headers() {
        // A Normal Object may carry no payload and any number of extension
        // header bytes (§10.2.1.2), so the payload length alone bounds nothing.
        let header_object = |object_id| {
            let mut object = object_with(1, object_id, Bytes::new());
            object.object.extension_headers =
                ExtensionHeaders(vec![KeyValuePair::new_bytes(1, vec![0; 4096])]);
            object
        };
        let cost = retained_bytes(&header_object(0));
        assert!(cost > 4096);
        assert!(retained_bytes(&object_with(1, 0, Bytes::new())) > 0);

        let budget = NonZeroUsize::new(2 * cost).unwrap();
        let (_writer, track) = track_with(limits(budget, budget));
        // A publisher that never advances its Group ID.
        for object_id in 0..1000 {
            track.insert(header_object(object_id));
        }

        let FetchPlan::Complete { objects, .. } = track.plan(
            Location::new(1, 0),
            Location::new(1, 1000),
            GroupOrder::Ascending,
        ) else {
            panic!("expected a complete plan");
        };
        assert_eq!(locations(&objects), vec![(1, 0), (1, 1)]);
        assert_eq!(track.limits.used_bytes.load(Ordering::Acquire), 2 * cost);
    }

    #[test]
    fn an_object_that_cannot_be_retained_evicts_nothing() {
        let (_writer, track) = track_with(limits(bytes(4), NonZeroUsize::MAX));
        retain(&track, 1, 0..1);
        retain(&track, 2, 0..3);

        // Larger than the whole track budget.
        track.insert(object_with(3, 0, vec![0; bytes(4).get()]));
        // Fits only by dropping its own group: group 1 is too small to help.
        track.insert(object_with(2, 3, "g2o3+"));

        assert!(is_complete(&track, 1, 1));
        assert!(is_complete(&track, 2, 3));
        assert!(is_beyond_largest(&track, 3));
        assert_eq!(
            track.limits.used_bytes.load(Ordering::Acquire),
            4 * object_bytes()
        );
    }

    #[test]
    fn an_object_beyond_the_shared_budget_evicts_nothing() {
        let shared = limits(NonZeroUsize::MAX, bytes(3));
        let (_first_writer, first) = track_with(shared.clone());
        let (_second_writer, second) = track_with(shared.clone());
        retain(&first, 1, 0..2);
        retain(&second, 5, 0..1);

        // Dropping every group of the second track would free one Object's
        // bytes, less than this Object costs beside the first track's two.
        second.insert(object_with(6, 0, "g6o0g6o0"));

        assert!(is_complete(&second, 5, 1));
        assert!(is_beyond_largest(&second, 6));
        assert_eq!(
            shared.used_bytes.load(Ordering::Acquire),
            3 * object_bytes()
        );
    }

    #[test]
    fn a_duplicate_object_keeps_the_first_copy() {
        let (_writer, track) = track();
        track.insert(object(1, 0));
        let mut duplicate = object(1, 0);
        duplicate.payload = vec![Bytes::from_static(b"other")];
        duplicate.object.payload_length = 5;
        track.insert(duplicate);

        let FetchPlan::Complete { objects, .. } = track.plan(
            Location::new(1, 0),
            Location::new(1, 1),
            GroupOrder::Ascending,
        ) else {
            panic!("expected a complete plan");
        };
        assert_eq!(objects, vec![object(1, 0)]);
    }

    async fn write_group(subgroups: &mut SubgroupsWriter, group_id: u64, object_ids: &[u64]) {
        let mut subgroup = subgroups
            .create(Subgroup {
                group_id,
                subgroup_id: 0,
                priority: 240,
            })
            .unwrap();
        for object_id in object_ids {
            let payload = Bytes::from(format!("g{group_id}o{object_id}"));
            let mut writer = subgroup
                .create_with_id(*object_id, payload.len(), None)
                .unwrap();
            writer.write(payload).unwrap();
        }
    }

    /// Yield until `ready` holds. The feed task runs on the same runtime, so
    /// this waits on its progress rather than on a clock.
    async fn until(mut ready: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if ready() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never became true");
    }

    #[tokio::test]
    async fn retain_feeds_objects_from_the_live_track_until_the_guard_drops() {
        let retention = FetchRetention::new(
            NonZeroU64::new(WINDOW).unwrap(),
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let name = FullTrackName {
            namespace: reader.namespace.clone(),
            name: reader.name.clone(),
        };
        let guard = retention.retain(Some("scope"), &reader).unwrap();
        assert!(
            retention.lookup(None, &name).is_none(),
            "scopes are separate"
        );

        let mut subgroups = writer.subgroups().unwrap();
        write_group(&mut subgroups, 9, &[0, 1, 2]).await;

        let track = retention.lookup(Some("scope"), &name).unwrap();
        until(|| {
            matches!(
                track.plan(
                    Location::new(9, 0),
                    Location::new(9, 3),
                    GroupOrder::Ascending
                ),
                FetchPlan::Complete { .. }
            )
        })
        .await;
        let FetchPlan::Complete { objects, .. } = track.plan(
            Location::new(9, 0),
            Location::new(9, 3),
            GroupOrder::Ascending,
        ) else {
            unreachable!();
        };
        assert_eq!(objects, vec![object(9, 0), object(9, 1), object(9, 2)]);

        drop(track);
        drop(guard);
        assert!(retention.lookup(Some("scope"), &name).is_none());
    }

    #[tokio::test]
    async fn retain_keeps_datagram_objects() {
        let retention = FetchRetention::new(
            NonZeroU64::new(WINDOW).unwrap(),
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let name = FullTrackName {
            namespace: reader.namespace.clone(),
            name: reader.name.clone(),
        };
        let _guard = retention.retain(None, &reader).unwrap();
        let mut datagrams = writer.datagrams().unwrap();
        let track = retention.lookup(None, &name).unwrap();

        for object_id in 0..2 {
            // A datagram track keeps only the latest datagram, so wait for each
            // to be retained before writing the next.
            datagrams
                .write(moq_transport::serve::Datagram {
                    group_id: 3,
                    object_id,
                    priority: 200,
                    payload: Bytes::from_static(b"d"),
                    extension_headers: Default::default(),
                })
                .unwrap();
            until(|| {
                matches!(
                    track.plan(
                        Location::new(3, object_id),
                        Location::new(3, object_id + 1),
                        GroupOrder::Ascending
                    ),
                    FetchPlan::Complete { .. }
                )
            })
            .await;
        }

        let FetchPlan::Complete { objects, .. } = track.plan(
            Location::new(3, 0),
            Location::new(3, 2),
            GroupOrder::Ascending,
        ) else {
            panic!("expected a complete plan");
        };
        assert_eq!(objects.len(), 2);
        assert!(objects.iter().all(|o| o.object.subgroup_id.is_none()));
    }

    #[tokio::test]
    async fn a_newer_registration_replaces_the_older_one() {
        let retention = FetchRetention::new(
            NonZeroU64::new(WINDOW).unwrap(),
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );
        let (_first_writer, first) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let (_second_writer, second) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let name = FullTrackName {
            namespace: first.namespace.clone(),
            name: first.name.clone(),
        };

        let old = retention.retain(None, &first).unwrap();
        let new = retention.retain(None, &second).unwrap();
        // Dropping the replaced guard must not unregister its replacement.
        drop(old);
        let found = retention.lookup(None, &name).unwrap();
        assert!(Arc::ptr_eq(&found, &new.track));
    }

    #[test]
    fn disabled_retention_retains_nothing() {
        let (_writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        assert!(FetchRetention::disabled().retain(None, &reader).is_none());
        assert_eq!(FetchRetention::disabled().retained_groups(), None);
    }
}
