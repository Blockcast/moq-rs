// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{fmt, sync::Arc};

use crate::watch::State;

use super::{ServeError, Track};

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
    // The latest datagram
    latest: Option<Datagram>,

    // Increased each time datagram changes.
    epoch: u64,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl Default for DatagramsState {
    fn default() -> Self {
        Self {
            latest: None,
            epoch: 0,
            closed: Ok(()),
        }
    }
}

pub struct DatagramsWriter {
    state: State<DatagramsState>,
    pub track: Arc<Track>,
}

impl DatagramsWriter {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        Self { state, track }
    }

    pub fn write(&mut self, datagram: Datagram) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

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
    dropped: u64,
}

impl DatagramsReader {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        Self {
            state,
            track,
            epoch: 0,
            dropped: 0,
        }
    }

    pub async fn read(&mut self) -> Result<Option<Datagram>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if self.epoch < state.epoch {
                    self.dropped = self
                        .dropped
                        .saturating_add(state.epoch.saturating_sub(self.epoch).saturating_sub(1));
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

    // Returns the largest group/sequence
    pub fn latest(&self) -> Option<(u64, u64)> {
        let state = self.state.lock();
        state
            .latest
            .as_ref()
            .map(|datagram| (datagram.group_id, datagram.object_id))
    }

    /// Number of datagrams superseded before this reader observed them.
    ///
    /// Datagram tracks are intentionally latest-wins: the writer retains one
    /// payload and slow readers skip intermediate values instead of building a
    /// queue. This counter makes that raw-lossy overflow policy observable.
    pub fn dropped(&self) -> u64 {
        self.dropped
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{coding::TrackNamespace, serve::Track};
    use bytes::Bytes;

    #[tokio::test]
    async fn stalled_reader_keeps_latest_and_counts_superseded_datagrams() {
        let track = Arc::new(Track {
            namespace: TrackNamespace::from_utf8_path("test"),
            name: "shreds".to_string(),
        });
        let (mut writer, mut reader) = Datagrams { track }.produce();

        // 1M production-sized shreds is >3.5 hours of input at 78 groups/sec.
        for group_id in 0..1_000_000 {
            writer
                .write(Datagram {
                    group_id,
                    object_id: 0,
                    priority: 0,
                    payload: Bytes::from(vec![(group_id % 251) as u8; 1_228]),
                    extension_headers: Default::default(),
                })
                .unwrap();
        }

        let latest = reader.read().await.unwrap().unwrap();
        assert_eq!(latest.group_id, 999_999);
        assert_eq!(latest.payload.len(), 1_228);
        assert_eq!(reader.dropped(), 999_999);
    }
}
