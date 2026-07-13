// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Datagram router: opaque pass-through of UDP datagrams onto a single MoQ
// track. Each ingested datagram becomes exactly one MoQ object in its own
// group (group_id is a monotonic counter), so `multicast.subgroupHistoryGroups`
// bounds how many recent datagrams the publisher retains.
//
// Unlike the MMTP router in `publish.rs`, this performs NO payload parsing and
// derives NO `/repair` sibling: the payload is carried verbatim and the
// receiver reassembles any application framing (e.g. Solana shred FEC sets)
// from the datagrams' own headers. The MoQ layer is just an ordered carrier.
//
// It reuses the `TrackSubgroups` / `SubgroupWrite` seam from `publish.rs`, so
// the runtime wires the real `moq_transport::serve::SubgroupsWriter` and unit
// tests drive an in-memory mock — identical to the MMTP dispatch tests.

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;

use crate::publish::{SubgroupWrite, TrackSubgroups};

/// Per-stream state for opaque datagram publishing: one MoQ track and a
/// monotonic group counter. Generic over the subgroup sink for testability.
pub struct DatagramState<T: TrackSubgroups> {
    /// Catalog track name (log context).
    pub name: String,
    /// Object-level priority for datagram objects on this track.
    pub priority: u8,
    /// Subgroup factory wired to one moq-transport TrackWriter::subgroups().
    pub sink: T,
    /// Next MoQ group_id. One group per datagram; monotonically non-decreasing
    /// as `moq_transport::serve::SubgroupsWriter::create` requires (it silently
    /// drops subgroups whose group_id <= the latest).
    pub next_group_id: u64,
}

impl<T: TrackSubgroups> DatagramState<T> {
    /// Create per-stream state with no group opened yet.
    pub fn new(name: String, priority: u8, sink: T) -> Self {
        Self {
            name,
            priority,
            sink,
            next_group_id: 0,
        }
    }

    /// Publish one datagram as a single MoQ object in a fresh group.
    ///
    /// Opening a new group per datagram (subgroup 0, one object) makes the
    /// history window a count of retained datagrams and keeps the mapping fully
    /// payload-agnostic — there is no MPU/MFU structure to key off.
    pub fn handle(&mut self, payload: Bytes) -> Result<()> {
        let group_id = self.next_group_id;
        let mut group = self
            .sink
            .create_group(group_id, 0, self.priority)
            .with_context(|| {
                format!(
                    "datagram track `{}`: create_group({group_id}) failed",
                    self.name
                )
            })?;
        group.put_object(payload)?;
        // Dropping `group` here closes the subgroup: it holds exactly one
        // complete object, so the reader sees a finished object.
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

    // In-memory sink. DatagramState drops each per-group writer after writing,
    // so the recorder must outlive them — a shared Rc<RefCell> handle the test
    // keeps a clone of. (publish.rs's MockSubgroups can retain its groups
    // directly because TrackState holds them open; this one cannot.)
    #[derive(Clone, Default)]
    struct Recorder {
        /// (group_id, subgroup_id, priority) per create_group call.
        groups_created: Rc<RefCell<Vec<(u64, u64, u8)>>>,
        /// Objects written, one inner Vec per group, in create order.
        objects: Rc<RefCell<Vec<Vec<Bytes>>>>,
    }

    struct RecGroup {
        objects: Rc<RefCell<Vec<Vec<Bytes>>>>,
        idx: usize,
    }

    impl SubgroupWrite for RecGroup {
        fn put_object(&mut self, payload: Bytes) -> Result<()> {
            self.objects.borrow_mut()[self.idx].push(payload);
            Ok(())
        }
    }

    impl TrackSubgroups for Recorder {
        type Group = RecGroup;

        fn create_group(
            &mut self,
            group_id: u64,
            subgroup_id: u64,
            priority: u8,
        ) -> Result<RecGroup> {
            self.groups_created
                .borrow_mut()
                .push((group_id, subgroup_id, priority));
            self.objects.borrow_mut().push(Vec::new());
            let idx = self.objects.borrow().len() - 1;
            Ok(RecGroup {
                objects: self.objects.clone(),
                idx,
            })
        }
    }

    #[test]
    fn each_datagram_opens_a_fresh_monotonic_group_with_one_object() {
        let rec = Recorder::default();
        let mut state = DatagramState::new("shreds".into(), 5, rec.clone());

        state.handle(Bytes::from_static(b"d0")).unwrap();
        state.handle(Bytes::from_static(b"d1")).unwrap();
        state.handle(Bytes::from_static(b"d2")).unwrap();

        // One group per datagram: group_id monotonic from 0, always subgroup 0,
        // inheriting the track priority.
        assert_eq!(
            *rec.groups_created.borrow(),
            vec![(0, 0, 5), (1, 0, 5), (2, 0, 5)],
        );
        assert_eq!(state.next_group_id, 3);

        // Each group carries exactly its one datagram, verbatim.
        let objs = rec.objects.borrow();
        assert_eq!(objs.len(), 3);
        assert_eq!(objs[0], vec![Bytes::from_static(b"d0")]);
        assert_eq!(objs[1], vec![Bytes::from_static(b"d1")]);
        assert_eq!(objs[2], vec![Bytes::from_static(b"d2")]);
    }
}
