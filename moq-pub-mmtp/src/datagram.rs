// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Datagram router: opaque pass-through of UDP datagrams onto a single MoQ
// track. Each ingested datagram becomes one native MoQ datagram. The transport
// retains only the latest payload, so a slow subscriber skips superseded
// datagrams instead of growing an unbounded queue.
//
// Unlike the MMTP router in `publish.rs`, this performs NO payload parsing and
// derives NO `/repair` sibling: the payload is carried verbatim and the
// receiver reassembles any application framing (e.g. Solana shred FEC sets)
// from the datagrams' own headers. The MoQ layer is just an ordered carrier.
//
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use moq_transport::serve::{Datagram, DatagramsWriter};

pub trait DatagramWrite {
    fn put_datagram(&mut self, group_id: u64, priority: u8, payload: Bytes) -> Result<()>;
}

impl DatagramWrite for DatagramsWriter {
    fn put_datagram(&mut self, group_id: u64, priority: u8, payload: Bytes) -> Result<()> {
        self.write(Datagram {
            group_id,
            object_id: 0,
            priority,
            payload,
            extension_headers: Default::default(),
        })
        .context("write native MoQ datagram")
    }
}

/// Per-stream state for opaque datagram publishing: one MoQ track and a
/// monotonic group counter. Generic over the datagram sink for testability.
pub struct DatagramState<T: DatagramWrite> {
    /// Catalog track name (log context).
    pub name: String,
    /// Object-level priority for datagram objects on this track.
    pub priority: u8,
    /// Latest-wins sink wired to one moq-transport TrackWriter::datagrams().
    pub sink: T,
    /// Next MoQ group_id. One group per datagram, monotonically increasing.
    pub next_group_id: u64,
}

impl<T: DatagramWrite> DatagramState<T> {
    /// Create per-stream state with no group opened yet.
    pub fn new(name: String, priority: u8, sink: T) -> Self {
        Self {
            name,
            priority,
            sink,
            next_group_id: 0,
        }
    }

    /// Publish one opaque native MoQ datagram. The sink retains one payload;
    /// newer writes supersede older unread writes for raw-lossy delivery.
    pub fn handle(&mut self, payload: Bytes) -> Result<()> {
        let group_id = self.next_group_id;
        self.sink
            .put_datagram(group_id, self.priority, payload)
            .with_context(|| {
                format!(
                    "datagram track `{}`: put_datagram({group_id}) failed",
                    self.name
                )
            })?;
        self.next_group_id = self.next_group_id.checked_add(1).ok_or_else(|| {
            anyhow!(
                "datagram track `{}`: group_id counter overflowed u64",
                self.name
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct Recorder {
        writes: Rc<RefCell<u64>>,
        latest: Rc<RefCell<Option<(u64, u8, Bytes)>>>,
    }

    impl DatagramWrite for Recorder {
        fn put_datagram(&mut self, group_id: u64, priority: u8, payload: Bytes) -> Result<()> {
            *self.writes.borrow_mut() += 1;
            *self.latest.borrow_mut() = Some((group_id, priority, payload));
            Ok(())
        }
    }

    #[test]
    fn sustained_input_retains_only_latest_datagram() {
        let rec = Recorder::default();
        let mut state = DatagramState::new("shreds".into(), 5, rec.clone());

        // 1M production-sized shreds is >3.5 hours of input at 78 groups/sec.
        for value in 0..1_000_000u64 {
            state
                .handle(Bytes::from(vec![(value % 251) as u8; 1_228]))
                .unwrap();
        }

        assert_eq!(*rec.writes.borrow(), 1_000_000);
        assert_eq!(state.next_group_id, 1_000_000);
        let latest = rec.latest.borrow();
        let (group_id, priority, payload) = latest.as_ref().unwrap();
        assert_eq!(*group_id, 999_999);
        assert_eq!(*priority, 5);
        assert_eq!(payload.len(), 1_228);
    }
}
