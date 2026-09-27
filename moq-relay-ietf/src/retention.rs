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
    groups: NonZeroU64,
    bytes: NonZeroUsize,
    tracks: Mutex<HashMap<RetentionKey, Weak<RetainedTrack>>>,
}

type RetentionKey = (String, FullTrackName);

impl FetchRetention {
    /// Retain nothing. Every FETCH is forwarded upstream.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Retain the Objects of the `groups` most recently arrived groups of each
    /// track, holding at most `bytes` of them per track.
    pub fn new(groups: NonZeroU64, bytes: NonZeroUsize) -> Self {
        Self {
            inner: Some(Arc::new(Registry {
                groups,
                bytes,
                tracks: Mutex::default(),
            })),
        }
    }

    /// The configured number of groups, or `None` when retention is disabled.
    pub fn retained_groups(&self) -> Option<NonZeroU64> {
        self.inner.as_ref().map(|registry| registry.groups)
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
            window: registry.groups,
            max_bytes: registry.bytes,
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
    window: NonZeroU64,
    max_bytes: NonZeroUsize,
    groups: Mutex<RetainedGroups>,
}

#[derive(Default)]
struct RetainedGroups {
    /// Retained groups by Group ID.
    groups: BTreeMap<u64, RetainedGroup>,
    /// Retained Group IDs, least recently arrived first. Group ID is
    /// peer-supplied, so eviction follows arrival: an outlier Group ID ages out
    /// like any other group instead of pinning the window, the byte budget or
    /// the Largest Location.
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
    /// Drop the least recently arrived group.
    fn evict_oldest(&mut self) {
        let Some(group_id) = self.arrival.pop_front() else {
            return;
        };
        if let Some(group) = self.groups.remove(&group_id) {
            self.bytes -= group.bytes;
        }
    }
}

/// The memory an Object holds while retained: its payload and extension
/// header values plus the structures that carry them. Shared payload chunks
/// are counted in full, since retention may be their last owner.
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
        let older_bytes: usize = state
            .arrival
            .iter()
            .take_while(|arrived| **arrived != group_id)
            .filter_map(|arrived| state.groups.get(arrived))
            .map(|group| group.bytes)
            .sum();
        let fits = |bytes: usize| {
            bytes
                .checked_add(cost)
                .is_some_and(|total| total <= self.max_bytes.get())
        };
        if !fits(state.bytes - older_bytes) {
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
        // Ends once the older groups are gone at the latest, since the check
        // above proved the rest fits.
        while !fits(state.bytes) {
            state.evict_oldest();
        }

        if !state.groups.contains_key(&group_id) {
            state.arrival.push_back(group_id);
        }
        let group = state.groups.entry(group_id).or_default();
        group.objects.insert(object_id, object);
        group.bytes += cost;
        state.bytes += cost;

        // The window holds the `window` most recently arrived groups. Only a
        // new group can exceed it, and a new group arrives last, so this
        // Object's group is never the one dropped here.
        if state.groups.len() as u64 > self.window.get() {
            state.evict_oldest();
        }
    }

    /// The Largest Location the relay knows of. The live track's is the anchor:
    /// it reports the last Object of its latest subgroup, which another
    /// subgroup of the same group can exceed, so a retained Object of that
    /// group extends it. A retained group beyond the live one never does, so
    /// an outlier Group ID cannot suppress §9.16.3 INVALID_RANGE or widen the
    /// §9.17 End Location, even before it ages out. Without a live Location the
    /// highest retained group stands in.
    fn largest_location(&self) -> Option<Location> {
        let live = self.reader.largest_location();
        let Ok(state) = self.groups.lock() else {
            return live;
        };
        let group = match live {
            Some(live) => state.groups.get_key_value(&live.group_id),
            None => state.groups.iter().next_back(),
        };
        let retained = group.and_then(|(group_id, group)| {
            group
                .objects
                .keys()
                .next_back()
                .map(|object_id| Location::new(*group_id, *object_id))
        });
        live.max(retained)
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
        coding::TrackNamespace,
        serve::{Subgroup, SubgroupsWriter, Track, TrackWriter},
    };

    use super::*;

    const WINDOW: u64 = 3;

    fn track() -> (TrackWriter, Arc<RetainedTrack>) {
        track_with_budget(usize::MAX)
    }

    fn track_with_budget(max_bytes: usize) -> (TrackWriter, Arc<RetainedTrack>) {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let track = Arc::new(RetainedTrack {
            reader,
            window: NonZeroU64::new(WINDOW).unwrap(),
            max_bytes: NonZeroUsize::new(max_bytes).unwrap(),
            groups: Mutex::default(),
        });
        (writer, track)
    }

    fn retention() -> FetchRetention {
        FetchRetention::new(NonZeroU64::new(WINDOW).unwrap(), NonZeroUsize::MAX)
    }

    fn object(group_id: u64, object_id: u64) -> FetchResponseObject {
        let payload = Bytes::from(format!("g{group_id}o{object_id}"));
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

    /// Objects `0..count` of the group are retained.
    fn is_complete(track: &RetainedTrack, group_id: u64, count: u64) -> bool {
        matches!(
            track.plan(
                Location::new(group_id, 0),
                Location::new(group_id, count),
                GroupOrder::Ascending
            ),
            FetchPlan::Complete { ref objects, .. } if !objects.is_empty()
        )
    }

    /// No Object of the group is retained, though the group is in range.
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
    fn a_far_future_group_ages_out_of_the_window() {
        let (_writer, track) = track();
        let far = VarInt::MAX.into_inner() - 1;
        retain(&track, 1, 0..2);
        // A Group ID near the varint maximum, e.g. from a publisher restarting
        // with a clock-derived base, followed by the track's usual groups.
        retain(&track, far, [0]);
        for group_id in 2..5 {
            retain(&track, group_id, 0..2);
        }

        // Window 3 keeps the three most recently arrived groups.
        for group_id in 2..5 {
            assert!(is_complete(&track, group_id, 2));
        }
        // The far group is gone, so it no longer sets the Largest Location
        // (§9.16.3 INVALID_RANGE, §9.17 End Location).
        assert!(matches!(
            track.plan(
                Location::new(far, 0),
                Location::new(far, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::BeyondLargest
        ));
        let FetchPlan::Complete { end_location, .. } = track.plan(
            Location::new(4, 0),
            Location::new(far, 1),
            GroupOrder::Ascending,
        ) else {
            panic!("expected a complete plan");
        };
        assert_eq!(end_location, Location::new(4, 2));
    }

    #[test]
    fn a_far_future_group_at_the_byte_budget_ages_out() {
        let far = 1 << 62;
        let budget = 4 * retained_bytes(&object(1, 0));
        let (_writer, track) = track_with_budget(budget);
        // The far group grows until its next Object no longer fits.
        let mut object_id = 0;
        loop {
            let before = track.groups.lock().unwrap().bytes;
            track.insert(object(far, object_id));
            if track.groups.lock().unwrap().bytes == before {
                break;
            }
            object_id += 1;
        }
        assert!(object_id > 0, "the far group is retained");
        assert!(
            track.groups.lock().unwrap().bytes + retained_bytes(&object(far, object_id)) > budget
        );

        // Once it stops being fed, the track's usual groups displace it.
        retain(&track, 1, 0..2);
        retain(&track, 2, 0..2);
        assert!(is_complete(&track, 2, 2));
        assert!(matches!(
            track.plan(
                Location::new(far, 0),
                Location::new(far, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::BeyondLargest
        ));
        let state = track.groups.lock().unwrap();
        assert!(!state.groups.contains_key(&far));
        assert!(state.bytes <= budget);
    }

    #[test]
    fn the_byte_budget_evicts_the_oldest_groups() {
        let cost = retained_bytes(&object(1, 0));
        // Room for four Objects: groups of two Objects each fit two at a time.
        let (_writer, track) = track_with_budget(4 * cost);
        retain(&track, 1, 0..2);
        retain(&track, 2, 0..2);
        retain(&track, 3, 0..2);

        assert!(matches!(
            track.plan(
                Location::new(1, 0),
                Location::new(1, 2),
                GroupOrder::Ascending
            ),
            FetchPlan::Partial { ref objects, .. } if objects.is_empty()
        ));
        assert!(matches!(
            track.plan(
                Location::new(2, 0),
                Location::new(3, 2),
                GroupOrder::Ascending
            ),
            FetchPlan::Partial { ref objects, first_unknown, .. }
                if objects.len() == 2 && first_unknown == Location::new(2, 2)
        ));
        let state = track.groups.lock().unwrap();
        assert_eq!(state.bytes, 4 * cost);
        assert_eq!(
            state.bytes,
            state
                .groups
                .values()
                .map(|group| group.bytes)
                .sum::<usize>()
        );
    }

    #[tokio::test]
    async fn a_group_larger_than_the_budget_keeps_its_leading_objects() {
        let cost = retained_bytes(&object(1, 0));
        let (writer, track) = track_with_budget(3 * cost);
        let mut subgroups = writer.subgroups().unwrap();
        // The live track knows the whole group, so its Largest Location is
        // {5, 5} even though retention holds less.
        write_group(&mut subgroups, 5, &[0, 1, 2, 3, 4, 5]).await;
        retain(&track, 5, 0..6);

        // Objects that do not fit are not retained; their status is unknown.
        let FetchPlan::Partial {
            objects,
            first_unknown,
            ..
        } = track.plan(
            Location::new(5, 0),
            Location::new(5, 6),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a partial plan");
        };
        assert_eq!(locations(&objects), vec![(5, 0), (5, 1), (5, 2)]);
        assert_eq!(first_unknown, Location::new(5, 3));

        // Eviction follows arrival, not Group ID: group 4 arrives after group
        // 5, so group 5 makes room for it.
        track.insert(object(4, 0));
        assert!(is_complete(&track, 4, 1));
        assert!(is_absent(&track, 5));
        // An Object of an earlier arrival never evicts a later one: group 6
        // fills the budget after group 4, so group 4's next Object is not
        // retained.
        write_group(&mut subgroups, 6, &[0, 1]).await;
        retain(&track, 6, 0..2);
        track.insert(object(4, 1));
        assert!(matches!(
            track.plan(
                Location::new(4, 0),
                Location::new(4, 2),
                GroupOrder::Ascending
            ),
            FetchPlan::Partial { ref objects, first_unknown, .. }
                if objects.len() == 1 && first_unknown == Location::new(4, 1)
        ));
        assert!(is_complete(&track, 6, 2));
        assert_eq!(track.groups.lock().unwrap().bytes, 3 * cost);
    }

    /// §9.16.3 and §9.17 anchor on the Largest Location. While an outlier
    /// group is still retained it must not move that anchor: a Start past the
    /// live Largest is still rejected, and the End Location still clamps.
    #[tokio::test]
    async fn a_retained_outlier_group_does_not_move_the_largest_location() {
        let (writer, track) = track();
        let mut subgroups = writer.subgroups().unwrap();
        write_group(&mut subgroups, 5, &[0, 1]).await;
        retain(&track, 5, 0..2);
        track.insert(object(1 << 62, 0));
        assert!(is_complete(&track, 5, 2));
        assert_eq!(track.largest_location(), Some(Location::new(5, 1)));

        assert!(matches!(
            track.plan(
                Location::new(6, 0),
                Location::new(6, 1),
                GroupOrder::Ascending
            ),
            FetchPlan::BeyondLargest
        ));
        let FetchPlan::Complete { end_location, .. } = track.plan(
            Location::new(5, 0),
            Location::new(9, 0),
            GroupOrder::Ascending,
        ) else {
            panic!("expected a complete plan");
        };
        assert_eq!(end_location, Location::new(5, 2));
    }

    #[tokio::test]
    async fn a_retained_object_past_the_live_latest_subgroup_is_in_range() {
        let (writer, track) = track();
        let mut subgroups = writer.subgroups().unwrap();
        // Group 5 splits its Objects across two subgroups. The live track's
        // Largest Location follows its latest subgroup, {5, 1}, though
        // subgroup 0 already carried Object 2.
        write_group(&mut subgroups, 5, &[0, 2]).await;
        let mut second = subgroups
            .create(Subgroup {
                group_id: 5,
                subgroup_id: 1,
                priority: 240,
            })
            .unwrap();
        let payload = Bytes::from("g5o1");
        second
            .create_with_id(1, payload.len(), None)
            .unwrap()
            .write(payload)
            .unwrap();
        assert_eq!(track.reader.largest_location(), Some(Location::new(5, 1)));
        retain(&track, 5, 0..3);

        let FetchPlan::Complete {
            objects,
            end_location,
        } = track.plan(
            Location::new(5, 2),
            Location::new(5, 3),
            GroupOrder::Ascending,
        )
        else {
            panic!("expected a complete plan");
        };
        assert_eq!(locations(&objects), vec![(5, 2)]);
        assert_eq!(end_location, Location::new(5, 3));
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
        let retention = retention();
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
        let retention = retention();
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
        let retention = retention();
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
