// SPDX-FileCopyrightText: 2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Lossless observation of everything written to a track.
//!
//! A [`SubgroupsReader`](super::SubgroupsReader) returns only the latest
//! subgroup created since it last looked, and a
//! [`DatagramsReader`](super::DatagramsReader) only the latest datagram. That
//! suits live forwarding, where a slow reader should skip ahead. A consumer
//! that must see every subgroup and datagram, such as a relay retaining recent
//! groups to answer FETCH, cannot rely on being polled between two writes.
//!
//! A [`TrackTap`] is handed each subgroup and datagram synchronously as it is
//! written, from the moment the tap is created. It holds only handles (a
//! [`SubgroupReader`] shares the subgroup's objects, a [`Datagram`] shares its
//! payload), and it ends once the track's writer is gone and every event has
//! been taken.

use std::sync::{Arc, Mutex, Weak};

use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    StreamExt,
};

use super::{Datagram, SubgroupReader};

/// Something written to a tapped track.
#[derive(Clone)]
pub enum TrackTapEvent {
    /// A subgroup was created. Its objects are read from the reader as they
    /// arrive.
    Subgroup(SubgroupReader),
    /// A datagram was written.
    Datagram(Datagram),
}

/// Receives every subgroup and datagram written to a track after
/// [`TrackReader::tap`](super::TrackReader::tap).
pub struct TrackTap {
    events: UnboundedReceiver<TrackTapEvent>,
}

impl TrackTap {
    /// The next event, or `None` once the track's writer is gone and every
    /// event has been taken.
    pub async fn next(&mut self) -> Option<TrackTapEvent> {
        self.events.next().await
    }
}

/// The write side of a track's taps, owned by whichever writer the track has.
#[derive(Clone, Default)]
pub(crate) struct TrackTaps {
    pub(super) taps: Arc<Mutex<Vec<UnboundedSender<TrackTapEvent>>>>,
}

impl TrackTaps {
    /// A handle through which readers can add taps without keeping them open.
    pub(crate) fn registrar(&self) -> TapRegistrar {
        TapRegistrar {
            taps: Arc::downgrade(&self.taps),
        }
    }

    /// Hand `event` to every open tap, forgetting taps that were dropped.
    pub(crate) fn emit(&self, event: impl Fn() -> TrackTapEvent) {
        let Ok(mut taps) = self.taps.lock() else {
            return;
        };
        taps.retain(|tap| tap.unbounded_send(event()).is_ok());
    }
}

/// Adds taps to a track, held by its readers.
#[derive(Clone)]
pub(crate) struct TapRegistrar {
    taps: Weak<Mutex<Vec<UnboundedSender<TrackTapEvent>>>>,
}

impl TapRegistrar {
    pub(crate) fn tap(&self) -> TrackTap {
        let (send, events) = unbounded();
        // With the writer already gone the sending half is dropped here, so the
        // tap ends at once.
        if let Some(taps) = self.taps.upgrade() {
            if let Ok(mut taps) = taps.lock() {
                taps.push(send);
            }
        }
        TrackTap { events }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::coding::TrackNamespace;
    use crate::serve::{ServeError, Subgroup, Track, TrackReaderMode};

    use super::*;

    fn subgroup(group_id: u64, subgroup_id: u64) -> Subgroup {
        Subgroup {
            group_id,
            subgroup_id,
            priority: 240,
        }
    }

    async fn subgroup_keys(tap: &mut TrackTap, count: usize) -> Vec<(u64, u64)> {
        let mut keys = Vec::new();
        for _ in 0..count {
            match tap.next().await {
                Some(TrackTapEvent::Subgroup(reader)) => {
                    keys.push((reader.group_id, reader.subgroup_id))
                }
                _ => panic!("expected a subgroup event"),
            }
        }
        keys
    }

    #[tokio::test]
    async fn tap_sees_every_subgroup_a_latest_reader_skips() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        // Created before the writer picks its mode.
        let mut tap = reader.tap();

        let mut subgroups = writer.subgroups().unwrap();
        for key in [(6, 0), (7, 0), (7, 1), (5, 0)] {
            let mut writer = subgroups.create(subgroup(key.0, key.1)).unwrap();
            writer.write(Bytes::from_static(b"x")).unwrap();
        }

        // The latest-wins reader only ever sees (7, 1).
        let TrackReaderMode::Subgroups(mut latest) = reader.mode().await.unwrap() else {
            panic!("expected subgroups mode");
        };
        assert_eq!(latest.next().await.unwrap().unwrap().subgroup_id, 1);

        // The tap sees all four, in creation order, including (5, 0), which
        // is older than the latest subgroup and never reaches readers.
        assert_eq!(
            subgroup_keys(&mut tap, 4).await,
            vec![(6, 0), (7, 0), (7, 1), (5, 0)]
        );
    }

    #[tokio::test]
    async fn tapped_subgroup_readers_share_the_objects() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let mut tap = reader.tap();
        let mut subgroups = writer.subgroups().unwrap();
        let mut subgroup_writer = subgroups.create(subgroup(1, 0)).unwrap();

        let Some(TrackTapEvent::Subgroup(mut tapped)) = tap.next().await else {
            panic!("expected a subgroup event");
        };
        // Objects written after the event still reach the tapped reader.
        subgroup_writer.write(Bytes::from_static(b"late")).unwrap();
        drop(subgroup_writer);
        assert_eq!(
            tapped.read_next().await.unwrap(),
            Some(Bytes::from_static(b"late"))
        );
        assert_eq!(tapped.read_next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn duplicate_subgroups_are_not_tapped() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let mut tap = reader.tap();
        let mut subgroups = writer.subgroups().unwrap();
        let _first = subgroups.create(subgroup(1, 0)).unwrap();
        assert!(matches!(
            subgroups.create(subgroup(1, 0)),
            Err(ServeError::Duplicate)
        ));
        drop(subgroups);

        assert_eq!(subgroup_keys(&mut tap, 1).await, vec![(1, 0)]);
        assert!(tap.next().await.is_none());
    }

    #[tokio::test]
    async fn tap_sees_every_datagram() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let mut tap = reader.tap();
        let mut datagrams = writer.datagrams().unwrap();
        for object_id in 0..3 {
            datagrams
                .write(Datagram {
                    group_id: 2,
                    object_id,
                    priority: 200,
                    payload: Bytes::from_static(b"d"),
                    extension_headers: Default::default(),
                })
                .unwrap();
        }
        drop(datagrams);

        let mut object_ids = Vec::new();
        while let Some(event) = tap.next().await {
            let TrackTapEvent::Datagram(datagram) = event else {
                panic!("expected a datagram event");
            };
            object_ids.push(datagram.object_id);
        }
        assert_eq!(object_ids, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn tap_ends_when_the_writer_is_gone() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let mut before = reader.tap();
        drop(writer);
        assert!(before.next().await.is_none());

        // A tap created after the writer is gone ends at once.
        assert!(reader.tap().next().await.is_none());
    }

    #[tokio::test]
    async fn dropped_taps_are_forgotten() {
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "repair").produce();
        let dropped = reader.tap();
        let mut kept = reader.tap();
        drop(dropped);

        let mut subgroups = writer.subgroups().unwrap();
        let _subgroup = subgroups.create(subgroup(3, 0)).unwrap();
        assert_eq!(subgroups.taps.taps.lock().unwrap().len(), 1);
        assert_eq!(subgroup_keys(&mut kept, 1).await, vec![(3, 0)]);
    }
}
