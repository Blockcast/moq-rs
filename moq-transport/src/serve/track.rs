// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A track is a collection of semi-reliable and semi-ordered streams, split into a [Writer] and [Reader] handle.
//!
//! A [Writer] creates streams with a sequence number and priority.
//! The sequence number is used to determine the order of streams, while the priority is used to determine which stream to transmit first.
//! This may seem counter-intuitive, but is designed for live streaming where the newest streams may be higher priority.
//! A cloned [Writer] can be used to create streams in parallel, but will error if a duplicate sequence number is used.
//!
//! A [Reader] may not receive all streams in order or at all.
//! These streams are meant to be transmitted over congested networks and the key to MoQ Tranport is to not block on them.
//! streams will be cached for a potentially limited duration added to the unreliable nature.
//! A cloned [Reader] will receive a copy of all new stream going forward (fanout).
//!
//! The track is closed with [ServeError::Closed] when all writers or readers are dropped.

use crate::watch::State;

use super::{
    Datagrams, DatagramsReader, DatagramsWriter, ObjectsWriter, ServeError, Stream, StreamReader,
    StreamWriter, Subgroups, SubgroupsReader, SubgroupsWriter,
};
use crate::coding::{Location, TrackNamespace};
use paste::paste;
use std::{num::NonZeroU64, ops::Deref, sync::Arc};

/// Static information about a track.
#[derive(Debug, Clone, PartialEq)]
pub struct Track {
    pub namespace: TrackNamespace,
    pub name: String,
}

impl Track {
    pub fn new(namespace: TrackNamespace, name: String) -> Self {
        Self { namespace, name }
    }

    pub fn produce(self) -> (TrackWriter, TrackReader) {
        // Create sharable TrackState and Info(Track)
        let (writer_track_state, reader_track_state) = State::default().split();
        let info = Arc::new(self);

        // Create TrackReader and TrackWriter with shared state and info
        let writer = TrackWriter::new(writer_track_state, info.clone());
        let reader = TrackReader::new(reader_track_state, info);

        (writer, reader)
    }
}

struct TrackState {
    /// The ReaderMode for this track. Set to None on creation.
    reader_mode: Option<TrackReaderMode>,
    /// Watchable closed state
    closed: Result<(), ServeError>,
    /// Per-track subgroup history window (group count) the publisher set
    /// (BLO-10339). `subgroups()` inherits it into the SubgroupsWriter (to
    /// enforce pruning) and the session reads it via `TrackReader` to
    /// advertise it in SUBSCRIBE_OK. `None` = unbounded retention; the
    /// `NonZeroU64` makes the ">= 1 group" invariant unrepresentable-if-violated.
    history_window_groups: Option<NonZeroU64>,
}

impl Default for TrackState {
    fn default() -> Self {
        Self {
            reader_mode: None,
            closed: Ok(()),
            history_window_groups: None,
        }
    }
}

/// Creates new streams for a track.
pub struct TrackWriter {
    state: State<TrackState>,
    pub info: Arc<Track>,
}

impl TrackWriter {
    /// Create a track with the given name (info/Track)
    fn new(state: State<TrackState>, info: Arc<Track>) -> Self {
        Self { state, info }
    }

    /// Set the per-track subgroup history window (group count) to enforce and
    /// advertise (BLO-10339). Must be called BEFORE `subgroups()` (which
    /// consumes `self`); `subgroups()` inherits this into the SubgroupsWriter,
    /// and the publisher session advertises it in SUBSCRIBE_OK so a downstream
    /// mirror bounds its retention to the same window. The `NonZeroU64` type
    /// enforces the ">= 1 group" invariant at the call boundary, so there is no
    /// runtime zero-check here; the only error is a cancelled track.
    pub fn set_history_window(&mut self, groups: NonZeroU64) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        state.history_window_groups = Some(groups);
        Ok(())
    }

    /// Create a new stream with the given priority, inserting it into the track.
    pub fn stream(self, priority: u8) -> Result<StreamWriter, ServeError> {
        // Create new StreamWriter/StreamReader pair
        let (writer, reader) = Stream {
            track: self.info.clone(),
            priority,
        }
        .produce();

        // Lock state to modify it
        let mut state = self.state.lock_mut().ok_or_else(|| {
            tracing::debug!(
                namespace = %self.info.namespace.to_utf8_path(),
                track = %self.info.name,
                "track state dropped (Cancel) in stream()"
            );
            ServeError::Cancel
        })?;

        // Set the Stream mode to TrackReaderMode::Stream
        state.reader_mode = Some(reader.into());
        Ok(writer)
    }

    // TODO: rework this whole interface for clarity?
    /// Create a new subgroups stream with the given priority, inserting it into the track.
    pub fn subgroups(self) -> Result<SubgroupsWriter, ServeError> {
        let (mut writer, reader) = Subgroups {
            track: self.info.clone(),
        }
        .produce();

        // Lock state to set the mode and read the inherited history window,
        // then drop the guard before touching the writer.
        let history_window = {
            let mut state = self.state.lock_mut().ok_or_else(|| {
                tracing::debug!(
                    namespace = %self.info.namespace.to_utf8_path(),
                    track = %self.info.name,
                    "track state dropped (Cancel) in subgroups()"
                );
                ServeError::Cancel
            })?;

            // Set the Stream mode to TrackReaderMode::Subgroups
            state.reader_mode = Some(reader.into());
            state.history_window_groups
        };

        // Inherit the Track-level window into the writer (BLO-10339), so a
        // single publisher-side `TrackWriter::set_history_window` both bounds
        // pruning here and is advertised via the TrackReader in SUBSCRIBE_OK.
        // `SubgroupsWriter::set_history_window` still takes a `u64` (it rejects
        // 0 itself); `.get()` is always >= 1, so that path never errors here.
        if let Some(window) = history_window {
            writer.set_history_window(window.get())?;
        }

        Ok(writer)
    }

    pub fn datagrams(self) -> Result<DatagramsWriter, ServeError> {
        // Lock state to modify it
        let mut state = self.state.lock_mut().ok_or_else(|| {
            tracing::debug!(
                namespace = %self.info.namespace.to_utf8_path(),
                track = %self.info.name,
                "track state dropped (Cancel) in datagrams()"
            );
            ServeError::Cancel
        })?;

        // Like `subgroups()`, inherit the publisher-set history window
        // (BLO-10339): it becomes the datagram ring depth — how many unread
        // datagrams a slow reader can still recover before supersession —
        // and the session advertises it via the TrackReader in SUBSCRIBE_OK
        // so a downstream relay mirror bounds its own retention. Without a
        // window the ring degrades to a single latest-wins slot.
        let depth = state
            .history_window_groups
            .map(|groups| usize::try_from(groups.get()).unwrap_or(usize::MAX))
            .unwrap_or(1);

        let (writer, reader) = Datagrams {
            track: self.info.clone(),
        }
        .produce_with_depth(depth);

        // Set the Stream mode to TrackReaderMode::Datagrams
        state.reader_mode = Some(reader.into());
        Ok(writer)
    }

    /// Close the track with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        tracing::debug!(
            namespace = %self.info.namespace.to_utf8_path(),
            track = %self.info.name,
            error = %err,
            "track closing"
        );
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or_else(|| {
            tracing::debug!(
                namespace = %self.info.namespace.to_utf8_path(),
                track = %self.info.name,
                "track state already dropped during close"
            );
            ServeError::Cancel
        })?;
        state.closed = Err(err);
        Ok(())
    }
}

impl Deref for TrackWriter {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Receives new streams for a track.
#[derive(Clone)]
pub struct TrackReader {
    state: State<TrackState>,
    pub info: Arc<Track>,
}

impl TrackReader {
    fn new(state: State<TrackState>, info: Arc<Track>) -> Self {
        Self { state, info }
    }

    /// Get the current mode of the track, waiting if necessary.
    pub async fn mode(&self) -> Result<TrackReaderMode, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if let Some(mode) = &state.reader_mode {
                    return Ok(mode.clone());
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Err(ServeError::Done),
                }
            }
            .await;
        }
    }

    // Returns the largest group/sequence
    pub fn largest_location(&self) -> Option<Location> {
        // We don't even know the mode yet.
        // TODO populate from SUBSCRIBE_OK
        None
    }

    /// The per-track subgroup history window the publisher set (BLO-10339), if
    /// any. Read by the publisher session to advertise it in SUBSCRIBE_OK.
    /// None = unbounded retention.
    pub fn history_window(&self) -> Option<NonZeroU64> {
        self.state.lock().history_window_groups
    }

    /// Wait until the track is closed, returning the closing error.
    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    /// Check if the track is closed or the writer has been dropped.
    /// This is used to detect stale cached TrackReaders that should not be reused.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock();

        if state.closed.is_err() {
            return true;
        }

        // Clone the mode out before dropping the TrackState lock to avoid
        // nested lock deadlocks (mode readers hold their own State locks).
        if let Some(mode) = state.reader_mode.clone() {
            // Mode has been set — the TrackWriter was consumed during the
            // Track→Subgroups/Stream/Datagrams transition. Liveness is now
            // determined by whether the mode-level writer is still alive.
            drop(state);
            return mode.is_closed();
        }

        // No mode set yet — check if the writer was abandoned before
        // transitioning to a specific mode.
        state.modified().is_none()
    }
}

impl Deref for TrackReader {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

macro_rules! track_readers {
    {$($name:ident,)*} => {
		paste! {
			#[derive(Clone)]
			pub enum TrackReaderMode {
				$($name([<$name Reader>])),*
			}

			$(impl From<[<$name Reader>]> for TrackReaderMode {
				fn from(reader: [<$name Reader >]) -> Self {
					Self::$name(reader)
				}
			})*

			impl TrackReaderMode {
				pub fn latest(&self) -> Option<(u64, u64)> {
					match self {
						$(Self::$name(reader) => reader.latest(),)*
					}
				}

				pub fn is_closed(&self) -> bool {
					match self {
						$(Self::$name(reader) => reader.is_closed(),)*
					}
				}
			}
		}
	}
}

track_readers!(Stream, Subgroups, Datagrams,);

macro_rules! track_writers {
    {$($name:ident,)*} => {
		paste! {
			pub enum TrackWriterMode {
				$($name([<$name Writer>])),*
			}

			$(impl From<[<$name Writer>]> for TrackWriterMode {
				fn from(writer: [<$name Writer>]) -> Self {
					Self::$name(writer)
				}
			})*

			impl TrackWriterMode {
				pub fn close(self, err: ServeError) -> Result<(), ServeError>{
					match self {
						$(Self::$name(writer) => writer.close(err),)*
					}
				}
			}
		}
	}
}

track_writers!(Track, Stream, Subgroups, Objects, Datagrams,);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::TrackNamespace;
    use crate::serve::Subgroup;

    // BLO-10339: the per-track history window round-trips writer → reader so the
    // publisher session can advertise it in SUBSCRIBE_OK before track mode resolves.
    // The ">= 1" invariant is enforced by the `NonZeroU64` type (a zero-window
    // simply does not compile), so no separate zero-errors test is needed.
    #[test]
    fn track_history_window_set_and_read() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (mut writer, reader) = track.produce();

        assert_eq!(reader.history_window(), None, "unset → None (unbounded)");
        let two = NonZeroU64::new(2).unwrap();
        writer.set_history_window(two).unwrap();
        assert_eq!(reader.history_window(), Some(two));
    }

    // BLO-10339: a window set on the Track BEFORE `.subgroups()` is inherited by
    // the SubgroupsWriter, so its pruning bounds memory without a second set call.
    #[tokio::test]
    async fn subgroups_inherits_track_history_window_and_prunes() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (mut writer, reader) = track.produce();

        writer
            .set_history_window(NonZeroU64::new(1).unwrap())
            .unwrap();
        let mut subgroups = writer.subgroups().expect("subgroups transition");
        for g in 0..=1u64 {
            subgroups
                .create(Subgroup {
                    group_id: g,
                    subgroup_id: 0,
                    priority: 0,
                })
                .unwrap();
        }
        drop(subgroups);

        let mut sg_reader = match reader.mode().await.expect("mode") {
            TrackReaderMode::Subgroups(r) => r,
            _ => panic!("expected subgroups mode"),
        };
        let mut got = Vec::new();
        while let Some(s) = sg_reader.next().await.unwrap() {
            got.push((s.group_id, s.subgroup_id));
        }
        assert_eq!(
            got,
            vec![(1, 0)],
            "window=1 inherited from Track → group 0 pruned once group 1 arrived"
        );
    }

    #[test]
    fn test_is_closed_false_before_mode_set() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (_writer, reader) = track.produce();
        assert!(!reader.is_closed());
    }

    #[test]
    fn test_is_closed_true_when_writer_dropped_without_mode() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();
        drop(writer);
        assert!(reader.is_closed());
    }

    #[test]
    fn test_is_closed_true_when_explicitly_closed() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();
        writer.close(ServeError::Cancel).unwrap();
        assert!(reader.is_closed());
    }

    #[test]
    fn test_is_closed_false_after_subgroups_transition_while_writer_alive() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let _subgroups_writer = writer
            .subgroups()
            .expect("subgroups transition should succeed");

        assert!(
            !reader.is_closed(),
            "track should NOT be closed while SubgroupsWriter is alive"
        );
    }

    #[test]
    fn test_is_closed_true_after_subgroups_writer_dropped() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let subgroups_writer = writer
            .subgroups()
            .expect("subgroups transition should succeed");
        drop(subgroups_writer);

        assert!(
            reader.is_closed(),
            "track should be closed after SubgroupsWriter is dropped"
        );
    }

    #[test]
    fn test_is_closed_true_after_subgroups_writer_explicitly_closed() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let subgroups_writer = writer
            .subgroups()
            .expect("subgroups transition should succeed");
        subgroups_writer.close(ServeError::Cancel).unwrap();

        assert!(
            reader.is_closed(),
            "track should be closed after SubgroupsWriter is explicitly closed"
        );
    }

    #[test]
    fn test_is_closed_false_after_stream_transition_while_writer_alive() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let _stream_writer = writer.stream(0).expect("stream transition should succeed");

        assert!(
            !reader.is_closed(),
            "track should NOT be closed while StreamWriter is alive"
        );
    }

    #[test]
    fn test_is_closed_true_after_stream_writer_dropped() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let stream_writer = writer.stream(0).expect("stream transition should succeed");
        drop(stream_writer);

        assert!(
            reader.is_closed(),
            "track should be closed after StreamWriter is dropped"
        );
    }

    #[test]
    fn test_is_closed_false_after_datagrams_transition_while_writer_alive() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let _datagrams_writer = writer
            .datagrams()
            .expect("datagrams transition should succeed");

        assert!(
            !reader.is_closed(),
            "track should NOT be closed while DatagramsWriter is alive"
        );
    }

    #[test]
    fn test_is_closed_true_after_datagrams_writer_dropped() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let datagrams_writer = writer
            .datagrams()
            .expect("datagrams transition should succeed");
        drop(datagrams_writer);

        assert!(
            reader.is_closed(),
            "track should be closed after DatagramsWriter is dropped"
        );
    }

    #[test]
    fn test_is_closed_false_while_subgroups_actively_writing() {
        let track = Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string());
        let (writer, reader) = track.produce();

        let mut subgroups_writer = writer
            .subgroups()
            .expect("subgroups transition should succeed");

        let _subgroup_writer = subgroups_writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .expect("create subgroup should succeed");

        assert!(
            !reader.is_closed(),
            "track should NOT be closed while actively writing subgroups"
        );
    }
}
