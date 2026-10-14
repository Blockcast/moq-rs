// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Serving an inbound FETCH from the Objects a publisher holds (draft-20
//! §10.13).

use bytes::BytesMut;

use super::{Publisher, SessionError, Writer};
use crate::coding::{KeyValuePairs, Location, ReasonPhrase};
use crate::data::{
    FetchHeader, FetchItem, FetchObject, FetchRangeKind, FetchStreamState, ObjectStatus,
    StreamHeaderType,
};
use crate::message::{self, GroupOrder, LocationFilter, RequestErrorCode, TrackExtensions};
use crate::serve::{SubgroupObjectReader, TrackReader, TrackReaderMode};

/// The inclusive range a FETCH serves once its filter is resolved against
/// Largest Object, plus the Group Order it is served in (§10.2.8).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FetchRange {
    pub start: Location,
    pub end: Location,
    pub descending: bool,
}

impl FetchRange {
    /// Resolve a FETCH's LOCATION_FILTER and GROUP_ORDER (§5.1.2, §10.13).
    /// Nothing published yet, a start past Largest Object, or an end before
    /// the start is INVALID_RANGE.
    pub fn resolve(
        filter: Option<LocationFilter>,
        group_order: Option<GroupOrder>,
        largest: Option<Location>,
    ) -> Result<Self, RequestErrorCode> {
        let largest = largest.ok_or(RequestErrorCode::InvalidRange)?;
        let (start, end) = match filter.unwrap_or(LocationFilter::Unfiltered) {
            LocationFilter::Unfiltered => (Location::new(0, 0), largest),
            LocationFilter::RelativeGroup(n) => (
                Location::new((largest.group_id.saturating_add(1)).saturating_sub(n), 0),
                largest,
            ),
            // The Next Object is past Largest Object by definition.
            LocationFilter::NextObject => return Err(RequestErrorCode::InvalidRange),
            LocationFilter::Absolute {
                start,
                end_group,
                end_object,
            } => {
                let end = match end_group {
                    None => largest,
                    // Without an End Object the whole End Group is requested.
                    Some(group) => Location::new(group, end_object.unwrap_or(u64::MAX)),
                };
                (start, end.min(largest))
            }
        };
        // `end` is at most Largest Object, so a start past it lands here too.
        if end < start {
            return Err(RequestErrorCode::InvalidRange);
        }
        Ok(Self {
            start,
            end,
            descending: group_order == Some(GroupOrder::Descending),
        })
    }

    /// Lowest Object ID of `group` inside the range.
    fn lo(&self, group: u64) -> u64 {
        if group == self.start.group_id {
            self.start.object_id
        } else {
            0
        }
    }

    /// Highest Object ID of `group` inside the range.
    fn hi(&self, group: u64) -> u64 {
        if group == self.end.group_id {
            self.end.object_id
        } else {
            u64::MAX
        }
    }

    pub fn contains(&self, location: Location) -> bool {
        location >= self.start && location <= self.end
    }

    /// The group that follows `group` in serving order, if still in range.
    fn next_group(&self, group: u64) -> Option<u64> {
        if self.descending {
            group.checked_sub(1).filter(|&g| g >= self.start.group_id)
        } else {
            group.checked_add(1).filter(|&g| g <= self.end.group_id)
        }
    }

    fn first(&self) -> Location {
        let group = if self.descending {
            self.end.group_id
        } else {
            self.start.group_id
        };
        Location::new(group, self.lo(group))
    }

    fn last(&self) -> Location {
        let group = if self.descending {
            self.start.group_id
        } else {
            self.end.group_id
        };
        Location::new(group, self.hi(group))
    }

    /// The Location after `at` in serving order.
    fn after(&self, at: Location) -> Option<Location> {
        if at.object_id < self.hi(at.group_id) {
            return Some(Location::new(at.group_id, at.object_id + 1));
        }
        let group = self.next_group(at.group_id)?;
        Some(Location::new(group, self.lo(group)))
    }

    /// The Location before `at` in serving order. `at` is not `first()`.
    fn before(&self, at: Location) -> Location {
        if at.object_id > self.lo(at.group_id) {
            return Location::new(at.group_id, at.object_id - 1);
        }
        let group = if self.descending {
            at.group_id + 1
        } else {
            at.group_id - 1
        };
        Location::new(group, self.hi(group))
    }

    /// Sort key that puts Locations in serving order: groups follow the
    /// Group Order, Objects ascend within a group (§10.13).
    fn order_key(&self, at: Location) -> (u64, u64) {
        let group = if self.descending {
            u64::MAX - at.group_id
        } else {
            at.group_id
        };
        (group, at.object_id)
    }
}

/// One step of a FETCH response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchStep {
    /// Serialize the held Object at this index.
    Object(usize),
    /// Everything since the prior step, up to and including this Location,
    /// has unknown status.
    Unknown(Location),
}

/// Plan the response for the Objects held at `held` (each inside `range`,
/// sorted by `range` serving order, no duplicates).
///
/// A gap on a FETCH stream says the Objects in it do not exist (§10.13), and
/// a publisher that only holds part of a track cannot say that. Every span it
/// does not hold is therefore an End of Unknown Range. A whole-group span
/// ends at Object ID 2^64-1, the largest Location a vi64 can carry.
pub fn plan_fetch(range: &FetchRange, held: &[Location]) -> Vec<FetchStep> {
    let mut steps = Vec::with_capacity(held.len() + 1);
    let mut cursor = Some(range.first());
    for (index, &at) in held.iter().enumerate() {
        if cursor != Some(at) {
            steps.push(FetchStep::Unknown(range.before(at)));
        }
        steps.push(FetchStep::Object(index));
        cursor = range.after(at);
    }
    if cursor.is_some() {
        steps.push(FetchStep::Unknown(range.last()));
    }
    steps
}

/// An Object a FETCH can serve from cache.
struct HeldObject {
    location: Location,
    subgroup_id: u64,
    priority: u8,
    reader: SubgroupObjectReader,
}

/// The Normal Objects inside `range` that `track` holds, in serving order.
async fn held_objects(track: &TrackReader, range: &FetchRange) -> Vec<HeldObject> {
    // Largest Object is known, so the mode is already set and this is immediate.
    let Ok(TrackReaderMode::Subgroups(subgroups)) = track.mode().await else {
        // ponytail: only subgroup tracks keep Objects; a datagram or stream
        // track serves an all-unknown range until those modes cache too.
        return Vec::new();
    };
    let mut held: Vec<HeldObject> = subgroups
        .cached()
        .into_iter()
        .flat_map(|subgroup| {
            let (subgroup_id, priority) = (subgroup.subgroup_id, subgroup.priority);
            let group_id = subgroup.group_id;
            subgroup
                .cached_objects()
                .into_iter()
                .map(move |reader| HeldObject {
                    location: Location::new(group_id, reader.object_id),
                    subgroup_id,
                    priority,
                    reader,
                })
        })
        .filter(|object| {
            object.reader.status == ObjectStatus::NormalObject && range.contains(object.location)
        })
        .collect();
    held.sort_by_key(|object| range.order_key(object.location));
    // A continued subgroup can hold one Location twice; serve it once.
    held.dedup_by_key(|object| object.location);
    held
}

/// An inbound FETCH waiting for the application to serve it.
pub struct Fetched {
    publisher: Publisher,
    pub msg: message::Fetch,
    responded: bool,
}

impl Fetched {
    pub(super) fn new(publisher: Publisher, msg: message::Fetch) -> Self {
        Self {
            publisher,
            msg,
            responded: false,
        }
    }

    /// Reject the FETCH with REQUEST_ERROR.
    pub fn respond_error(&mut self, error_code: u64, reason: &str) {
        self.responded = true;
        self.publisher.send_request_error(
            "fetch",
            message::RequestError {
                id: self.msg.id,
                error_code,
                retry_interval: 0,
                reason: ReasonPhrase(reason.to_string()),
            },
        );
    }

    /// Serve the FETCH from the Objects `track` holds right now.
    ///
    /// The data stream is written and FINed before FETCH_OK, which ends the
    /// request stream. FETCH_OK may follow Objects (§10.14).
    pub async fn serve(mut self, track: TrackReader) -> Result<(), SessionError> {
        // Validated when the FETCH arrived; malformed values closed the session.
        let filter = self.msg.params.location_filter()?;
        let group_order = self.msg.params.group_order()?;
        let range = match FetchRange::resolve(filter, group_order, track.largest_location()) {
            Ok(range) => range,
            Err(code) => {
                self.respond_error(code as u64, "fetch range cannot be satisfied");
                return Ok(());
            }
        };

        let mut held = held_objects(&track, &range).await;
        let locations: Vec<Location> = held.iter().map(|object| object.location).collect();
        let steps = plan_fetch(&range, &locations);

        let mut writer = Writer::new(self.publisher.open_uni().await?);
        writer
            .encode(&FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id: self.msg.id,
            })
            .await?;
        let mut state = FetchStreamState::new(range.descending);
        for step in steps {
            let mut buf = BytesMut::new();
            match step {
                FetchStep::Unknown(at) => {
                    state.encode(
                        &FetchItem::EndOfRange {
                            kind: FetchRangeKind::Unknown,
                            group_id: at.group_id,
                            object_id: at.object_id,
                        },
                        &mut buf,
                    )?;
                }
                FetchStep::Object(index) => {
                    let object = &mut held[index];
                    let payload = object.reader.read_all().await?;
                    state.encode(
                        &FetchItem::Object(FetchObject {
                            group_id: object.location.group_id,
                            subgroup_id: Some(object.subgroup_id),
                            object_id: object.location.object_id,
                            publisher_priority: object.priority,
                            properties: object.reader.extension_headers.clone(),
                            payload_length: payload.len(),
                        }),
                        &mut buf,
                    )?;
                    buf.extend_from_slice(&payload);
                }
            }
            writer.write(&buf).await?;
        }
        writer.finish()?;

        self.responded = true;
        self.publisher.send_message(message::FetchOk {
            id: self.msg.id,
            end_of_track: false,
            end_location: range.end,
            params: KeyValuePairs::default(),
            track_extensions: TrackExtensions::default(),
        });
        Ok(())
    }
}

impl Drop for Fetched {
    fn drop(&mut self) {
        if !self.responded {
            self.respond_error(RequestErrorCode::InternalError as u64, "fetch not served");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(group: u64, object: u64) -> Location {
        Location::new(group, object)
    }

    fn range(start: Location, end: Location, descending: bool) -> FetchRange {
        FetchRange {
            start,
            end,
            descending,
        }
    }

    #[test]
    fn resolve_defaults_to_zero_through_largest_ascending() {
        assert_eq!(
            FetchRange::resolve(None, None, Some(loc(4, 7))),
            Ok(range(loc(0, 0), loc(4, 7), false))
        );
    }

    #[test]
    fn resolve_clamps_the_end_to_largest_and_keeps_an_earlier_end() {
        let absolute = |end_group, end_object| LocationFilter::Absolute {
            start: loc(2, 1),
            end_group: Some(end_group),
            end_object,
        };
        let largest = Some(loc(4, 7));
        // Past Largest Object: FETCH_OK reports Largest Object instead.
        assert_eq!(
            FetchRange::resolve(Some(absolute(9, Some(0))), None, largest)
                .unwrap()
                .end,
            loc(4, 7)
        );
        assert_eq!(
            FetchRange::resolve(Some(absolute(4, None)), None, largest)
                .unwrap()
                .end,
            loc(4, 7)
        );
        // No End Object: the whole End Group.
        assert_eq!(
            FetchRange::resolve(Some(absolute(3, None)), None, largest)
                .unwrap()
                .end,
            loc(3, u64::MAX)
        );
        assert_eq!(
            FetchRange::resolve(Some(absolute(3, Some(5))), None, largest)
                .unwrap()
                .end,
            loc(3, 5)
        );
    }

    #[test]
    fn resolve_relative_group_counts_back_from_the_next_group() {
        let r = |n| {
            FetchRange::resolve(
                Some(LocationFilter::RelativeGroup(n)),
                None,
                Some(loc(4, 7)),
            )
        };
        assert_eq!(r(1).unwrap().start, loc(4, 0));
        assert_eq!(r(3).unwrap().start, loc(2, 0));
        assert_eq!(r(99).unwrap().start, loc(0, 0));
        // StartGroup=0 is the Next Group, past Largest Object.
        assert_eq!(r(0), Err(RequestErrorCode::InvalidRange));
    }

    #[test]
    fn resolve_invalid_range_cases() {
        let invalid = Err(RequestErrorCode::InvalidRange);
        assert_eq!(FetchRange::resolve(None, None, None), invalid);
        assert_eq!(
            FetchRange::resolve(Some(LocationFilter::NextObject), None, Some(loc(4, 7))),
            invalid
        );
        let start_past = LocationFilter::Absolute {
            start: loc(4, 8),
            end_group: None,
            end_object: None,
        };
        assert_eq!(
            FetchRange::resolve(Some(start_past), None, Some(loc(4, 7))),
            invalid
        );
        let end_before_start = LocationFilter::Absolute {
            start: loc(2, 5),
            end_group: Some(2),
            end_object: Some(4),
        };
        assert_eq!(
            FetchRange::resolve(Some(end_before_start), None, Some(loc(4, 7))),
            invalid
        );
    }

    #[test]
    fn resolve_reads_descending_group_order() {
        assert!(
            FetchRange::resolve(None, Some(GroupOrder::Descending), Some(loc(1, 0)))
                .unwrap()
                .descending
        );
        assert!(
            !FetchRange::resolve(None, Some(GroupOrder::Ascending), Some(loc(1, 0)))
                .unwrap()
                .descending
        );
    }

    use FetchStep::{Object, Unknown};

    #[test]
    fn fully_held_range_has_no_markers() {
        let r = range(loc(3, 0), loc(3, 2), false);
        assert_eq!(
            plan_fetch(&r, &[loc(3, 0), loc(3, 1), loc(3, 2)]),
            vec![Object(0), Object(1), Object(2)]
        );
    }

    #[test]
    fn nothing_held_is_one_unknown_range_to_the_end() {
        let r = range(loc(0, 0), loc(4, 7), false);
        assert_eq!(plan_fetch(&r, &[]), vec![Unknown(loc(4, 7))]);
    }

    #[test]
    fn ascending_gaps_become_unknown_ranges() {
        let r = range(loc(0, 0), loc(4, 7), false);
        assert_eq!(
            plan_fetch(&r, &[loc(3, 0), loc(3, 1), loc(3, 4), loc(4, 0)]),
            vec![
                // Groups 0-2 are not held.
                Unknown(loc(2, u64::MAX)),
                Object(0),
                Object(1),
                Unknown(loc(3, 3)),
                Object(2),
                // Whether group 3 ends at Object 4 is not known.
                Unknown(loc(3, u64::MAX)),
                Object(3),
                Unknown(loc(4, 7)),
            ]
        );
    }

    #[test]
    fn descending_serves_groups_downward_and_objects_upward() {
        let r = range(loc(2, 3), loc(4, 1), true);
        assert_eq!(
            plan_fetch(&r, &[loc(4, 0), loc(4, 1), loc(2, 5)]),
            vec![
                Object(0),
                Object(1),
                // Group 3 whole and group 2 below Object 5; group 2 starts at 3.
                Unknown(loc(2, 4)),
                Object(2),
                Unknown(loc(2, u64::MAX)),
            ]
        );
    }

    #[test]
    fn descending_leading_gap_ends_at_the_end_of_the_prior_group() {
        let r = range(loc(0, 0), loc(4, 1), true);
        assert_eq!(
            plan_fetch(&r, &[loc(3, 0)]),
            vec![Unknown(loc(4, 1)), Object(0), Unknown(loc(0, u64::MAX))]
        );
    }

    /// Every plan must encode: the codec rejects a Location that does not
    /// advance in the Group Order, so this checks the markers are ordered.
    #[test]
    fn plans_encode_and_decode_back() {
        for (r, held) in [
            (
                range(loc(0, 0), loc(4, 7), false),
                vec![loc(3, 0), loc(3, 4), loc(4, 0)],
            ),
            (
                range(loc(2, 3), loc(4, 1), true),
                vec![loc(4, 0), loc(4, 1), loc(2, 5)],
            ),
            (range(loc(0, 0), loc(4, 1), true), vec![]),
        ] {
            let mut state = FetchStreamState::new(r.descending);
            let mut buf = BytesMut::new();
            let mut expected = vec![];
            for step in plan_fetch(&r, &held) {
                let item = match step {
                    Unknown(at) => FetchItem::EndOfRange {
                        kind: FetchRangeKind::Unknown,
                        group_id: at.group_id,
                        object_id: at.object_id,
                    },
                    Object(i) => FetchItem::Object(FetchObject {
                        group_id: held[i].group_id,
                        subgroup_id: Some(0),
                        object_id: held[i].object_id,
                        publisher_priority: 7,
                        properties: Default::default(),
                        payload_length: 0,
                    }),
                };
                state.encode(&item, &mut buf).unwrap();
                expected.push(item);
            }
            let mut decoder = FetchStreamState::new(r.descending);
            let mut bytes = buf.freeze();
            let mut decoded = vec![];
            while bytes::Buf::has_remaining(&bytes) {
                decoded.push(decoder.decode(&mut bytes).unwrap());
            }
            assert_eq!(decoded, expected);
        }
    }

    /// Media-path check: Objects a relay holds in an open subgroup come back
    /// on a FETCH stream with their payloads, the span before and between
    /// them is unknown, and FETCH_OK reports Largest Object as End Location.
    #[tokio::test]
    async fn serves_held_objects_over_a_fetch_stream() {
        use crate::coding::{Decode, TrackName, TrackNamespace};
        use crate::message::Message;
        use crate::serve::{Subgroup, Track};
        use crate::session::{test_support::loopback_session_pair, Reader, RequestId};
        use crate::watch::Queue;
        use bytes::{Buf, Bytes};

        let (session, peer) = loopback_session_pair().await;
        let (outgoing, mut outgoing_recv) = Queue::default().split();
        let (bidi_task_tx, _bidi_task_rx) = tokio::sync::mpsc::unbounded_channel();
        let publisher = Publisher::new(outgoing, session, None, RequestId::new(0, 1), bidi_task_tx);

        let (writer, track) = Track::new(TrackNamespace::from_utf8_path("live"), "video").produce();
        let mut subgroups = writer.subgroups().unwrap();
        // Created out of order, so the response has to sort them.
        let mut group6 = subgroups
            .create(Subgroup {
                group_id: 6,
                subgroup_id: 0,
                priority: 3,
            })
            .unwrap();
        group6.write(Bytes::from("d")).unwrap();
        let mut group5 = subgroups
            .create(Subgroup {
                group_id: 5,
                subgroup_id: 0,
                priority: 3,
            })
            .unwrap();
        for payload in ["a", "bb", "ccc"] {
            group5.write(Bytes::from(payload)).unwrap();
        }
        // A second stream continuing subgroup 5/0 repeats Location 5/0.
        let mut continued = subgroups
            .create_with_metadata(
                Subgroup {
                    group_id: 5,
                    subgroup_id: 0,
                    priority: 3,
                },
                crate::serve::SubgroupStreamMetadata {
                    has_properties: true,
                    end_of_group: false,
                    first_object: false,
                },
            )
            .unwrap();
        continued.write(Bytes::from("a")).unwrap();

        // Dropping the last session handle closes the connection.
        let _session = publisher.clone();
        let fetched = Fetched::new(
            publisher,
            message::Fetch {
                id: 4,
                track_namespace: TrackNamespace::from_utf8_path("live"),
                track_name: TrackName::from("video"),
                params: KeyValuePairs::default(),
            },
        );
        let served = tokio::spawn(fetched.serve(track));

        let mut reader = Reader::new(peer.accept_uni().await.unwrap());
        let mut wire = bytes::BytesMut::new();
        while let Some(chunk) = reader.read_chunk(usize::MAX).await.unwrap() {
            wire.extend_from_slice(&chunk);
        }
        served.await.unwrap().unwrap();

        let mut wire = wire.freeze();
        let header_type = StreamHeaderType::decode(&mut wire).unwrap();
        assert_eq!(header_type, StreamHeaderType::Fetch);
        assert_eq!(
            FetchHeader::decode(header_type, &mut wire)
                .unwrap()
                .request_id,
            4
        );
        let mut state = FetchStreamState::new(false);
        let mut items = vec![];
        while wire.has_remaining() {
            match state.decode(&mut wire).unwrap() {
                FetchItem::Object(o) => {
                    let payload = wire.split_to(o.payload_length);
                    items.push(format!(
                        "{}/{}={}",
                        o.group_id,
                        o.object_id,
                        String::from_utf8_lossy(&payload)
                    ));
                    assert_eq!((o.subgroup_id, o.publisher_priority), (Some(0), 3));
                }
                FetchItem::EndOfRange {
                    kind,
                    group_id,
                    object_id,
                } => {
                    assert_eq!(kind, FetchRangeKind::Unknown);
                    items.push(format!("unknown..{group_id}/{object_id}"));
                }
            }
        }
        let max = u64::MAX;
        assert_eq!(
            items,
            [
                format!("unknown..4/{max}"),
                "5/0=a".into(),
                "5/1=bb".into(),
                "5/2=ccc".into(),
                format!("unknown..5/{max}"),
                "6/0=d".into(),
            ]
        );

        match outgoing_recv.pop().await {
            Some(Message::FetchOk(ok)) => {
                assert_eq!(
                    (ok.id, ok.end_location, ok.end_of_track),
                    (4, Location::new(6, 0), false)
                );
            }
            other => panic!("expected FETCH_OK, got {other:?}"),
        }
        drop((group5, group6, continued));
    }
}
