// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{collections::VecDeque, fmt, sync::Arc};

use crate::watch::State;

use super::{ServeError, Track};

pub struct Datagrams {
    pub track: Arc<Track>,
}

impl Datagrams {
    /// Produce with a single-slot window (pure latest-wins).
    ///
    /// Prefer `produce_with_depth` wired to the track's history window
    /// (`TrackWriter::set_history_window`) — a depth-1 slot drops every
    /// datagram that lands while the reader is between wakeups, which at
    /// bursty ingest rates (e.g. Solana shreds arriving per-FEC-set) is
    /// most of them.
    pub fn produce(self) -> (DatagramsWriter, DatagramsReader) {
        self.produce_with_depth(1)
    }

    /// Produce with a bounded ring of `depth` retained datagrams.
    ///
    /// The ring bounds publisher-owned memory to `depth` payloads: writes
    /// beyond the window supersede the oldest unread datagram (raw-lossy,
    /// no backpressure). Readers drain in write order and observe drops
    /// via `DatagramsReader::dropped`.
    pub fn produce_with_depth(self, depth: usize) -> (DatagramsWriter, DatagramsReader) {
        let (writer, reader) = State::new(DatagramsState::with_depth(depth)).split();

        let writer = DatagramsWriter::new(writer, self.track.clone());
        let reader = DatagramsReader::new(reader, self.track);

        (writer, reader)
    }
}

struct DatagramsState {
    // The most recent `<= depth` datagrams, oldest first. Writes beyond
    // `depth` pop the oldest entry (raw-lossy supersession).
    ring: VecDeque<Datagram>,

    // Maximum retained datagrams (>= 1).
    depth: usize,

    // Total datagrams ever written. The ring holds writes
    // `[epoch - ring.len(), epoch)`; readers use their own cursor into this
    // sequence to detect and count supersession.
    epoch: u64,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl DatagramsState {
    fn with_depth(depth: usize) -> Self {
        let depth = depth.max(1);
        Self {
            ring: VecDeque::with_capacity(depth.min(1024)),
            depth,
            epoch: 0,
            closed: Ok(()),
        }
    }
}

impl Default for DatagramsState {
    fn default() -> Self {
        Self::with_depth(1)
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

        if state.ring.len() >= state.depth {
            // Raw-lossy overflow policy: supersede the oldest unread datagram
            // rather than queueing unboundedly or blocking the writer.
            state.ring.pop_front();
        }
        state.ring.push_back(datagram);
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

    // Sequence number (in the writer's `epoch` space) of the next datagram
    // this reader wants. Each cloned reader keeps its own cursor.
    next: u64,
    dropped: u64,
}

impl DatagramsReader {
    fn new(state: State<DatagramsState>, track: Arc<Track>) -> Self {
        Self {
            state,
            track,
            next: 0,
            dropped: 0,
        }
    }

    /// Read the next retained datagram in write order.
    ///
    /// If the writer has advanced past this reader's cursor (the ring
    /// superseded entries before they were read), the cursor jumps to the
    /// oldest retained datagram and the skipped count is added to
    /// `dropped`.
    pub async fn read(&mut self) -> Result<Option<Datagram>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                let oldest = state.epoch - state.ring.len() as u64;
                if self.next < oldest {
                    self.dropped = self.dropped.saturating_add(oldest - self.next);
                    self.next = oldest;
                }
                if self.next < state.epoch {
                    let idx = (self.next - oldest) as usize;
                    self.next += 1;
                    return Ok(Some(state.ring[idx].clone()));
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
            .ring
            .back()
            .map(|datagram| (datagram.group_id, datagram.object_id))
    }

    /// Number of datagrams superseded before this reader observed them.
    ///
    /// Datagram tracks are raw-lossy: the writer retains a bounded ring
    /// (`Datagrams::produce_with_depth`) and slow readers skip superseded
    /// values instead of building an unbounded queue. This counter makes
    /// that overflow policy observable.
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

    fn track() -> Arc<Track> {
        Arc::new(Track {
            namespace: TrackNamespace::from_utf8_path("test"),
            name: "shreds".to_string().into(),
        })
    }

    fn shred(group_id: u64) -> Datagram {
        Datagram {
            group_id,
            object_id: 0,
            priority: 0,
            payload: Bytes::from(vec![(group_id % 251) as u8; 1_228]),
            extension_headers: Default::default(),
        }
    }

    #[tokio::test]
    async fn depth_one_keeps_latest_and_counts_superseded_datagrams() {
        let (mut writer, mut reader) = Datagrams { track: track() }.produce();

        // 1M production-sized shreds is >3.5 hours of input at 78 groups/sec.
        for group_id in 0..1_000_000 {
            writer.write(shred(group_id)).unwrap();
        }

        let latest = reader.read().await.unwrap().unwrap();
        assert_eq!(latest.group_id, 999_999);
        assert_eq!(latest.payload.len(), 1_228);
        assert_eq!(reader.dropped(), 999_999);
    }

    #[tokio::test]
    async fn bounded_ring_retains_exactly_the_window_and_drains_in_order() {
        // The occupancy bound asserted here is external: the reader can
        // recover exactly `depth` datagrams from a stalled start, no more —
        // anything beyond the window was superseded and counted, anything
        // within it is delivered in write order (what FEC-set reassembly
        // downstream needs).
        const DEPTH: usize = 256;
        let (mut writer, mut reader) = Datagrams { track: track() }.produce_with_depth(DEPTH);

        for group_id in 0..1_000_000 {
            writer.write(shred(group_id)).unwrap();
        }
        drop(writer); // close: reader drains the retained window then EOFs

        let mut got = Vec::new();
        while let Some(datagram) = reader.read().await.unwrap() {
            got.push(datagram.group_id);
        }

        assert_eq!(got.len(), DEPTH, "reader recovers exactly the ring window");
        assert_eq!(got[0], (1_000_000 - DEPTH as u64), "oldest retained first");
        assert_eq!(*got.last().unwrap(), 999_999, "newest retained last");
        assert!(got.windows(2).all(|w| w[1] == w[0] + 1), "in write order");
        assert_eq!(reader.dropped(), 1_000_000 - DEPTH as u64);
    }

    #[tokio::test]
    async fn keeping_up_reader_loses_nothing_within_the_window() {
        // Interleaved write/read within the window: no drops.
        let (mut writer, mut reader) = Datagrams { track: track() }.produce_with_depth(4);

        for group_id in 0..32 {
            writer.write(shred(group_id)).unwrap();
            let got = reader.read().await.unwrap().unwrap();
            assert_eq!(got.group_id, group_id);
        }
        assert_eq!(reader.dropped(), 0);
    }
}
