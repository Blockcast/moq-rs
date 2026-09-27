// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{fmt, sync::Arc};

use crate::watch::State;

use super::{ServeError, Track, TrackTapEvent, TrackTaps};

pub struct Datagrams {
    pub track: Arc<Track>,
}

impl Datagrams {
    pub fn produce(self) -> (DatagramsWriter, DatagramsReader) {
        let (writer, reader) = State::default().split();

        let writer = DatagramsWriter::new(writer, self.track.clone());
        let reader = DatagramsReader::new(reader, self.track);

        (writer, reader)
    }
}

struct DatagramsState {
    // The latest datagram, delivered to readers.
    latest: Option<Datagram>,

    // The largest (group, object) written. Datagrams can arrive reordered, and
    // a late one is still delivered through `latest`, but it must never move
    // the track's Largest Location backwards.
    largest: Option<(u64, u64)>,

    // Increased each time datagram changes.
    epoch: u64,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl Default for DatagramsState {
    fn default() -> Self {
        Self {
            latest: None,
            largest: None,
            epoch: 0,
            closed: Ok(()),
        }
    }
}

pub struct DatagramsWriter {
    state: State<DatagramsState>,
    pub track: Arc<Track>,
    taps: TrackTaps,
}

impl DatagramsWriter {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        Self {
            state,
            track,
            taps: TrackTaps::default(),
        }
    }

    pub(super) fn set_taps(&mut self, taps: TrackTaps) {
        self.taps = taps;
    }

    pub fn write(&mut self, datagram: Datagram) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        self.taps.emit(|| TrackTapEvent::Datagram(datagram.clone()));

        state.largest = state
            .largest
            .max(Some((datagram.group_id, datagram.object_id)));
        state.latest = Some(datagram);
        state.epoch += 1;

        Ok(())
    }

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }
}

#[derive(Clone)]
pub struct DatagramsReader {
    state: State<DatagramsState>,
    pub track: Arc<Track>,

    epoch: u64,
}

impl DatagramsReader {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        Self {
            state,
            track,
            epoch: 0,
        }
    }

    pub async fn read(&mut self) -> Result<Option<Datagram>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if self.epoch < state.epoch {
                    self.epoch = state.epoch;
                    return Ok(state.latest.clone());
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None), // No more updates will come
                }
            }
            .await;
        }
    }

    // Returns the largest group/sequence written, which a reordered datagram
    // never lowers.
    pub fn latest(&self) -> Option<(u64, u64)> {
        self.state.lock().largest
    }

    /// Check if the datagrams writer has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock();
        state.closed.is_err() || state.modified().is_none()
    }
}

/// Static information about the datagram.
#[derive(Clone)]
pub struct Datagram {
    pub group_id: u64,
    pub object_id: u64,
    pub priority: u8,
    pub payload: bytes::Bytes,

    // Extension headers (for draft-14 compliance, particularly immutable extensions)
    pub extension_headers: crate::data::ExtensionHeaders,
}

impl fmt::Debug for Datagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Datagram")
            .field("object_id", &self.object_id)
            .field("group_id", &self.group_id)
            .field("priority", &self.priority)
            .field("payload", &self.payload.len())
            .field("extension_headers", &self.extension_headers)
            .finish()
    }
}
