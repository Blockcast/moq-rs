// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A stream is a stream of objects with a header, split into a [Writer] and [Reader] handle.
//!
//! A [Writer] writes an ordered stream of objects.
//! Each object can have a sequence number, allowing the reader to detect gaps objects.
//!
//! A [Reader] reads an ordered stream of objects.
//! The reader can be cloned, in which case each reader receives a copy of each object. (fanout)
//!
//! The stream is closed with [ServeError::Closed] when all writers or readers are dropped.
use std::{ops::Deref, sync::Arc};

use bytes::Bytes;

use crate::data::ObjectStatus;
use crate::watch::State;

use super::{ServeError, Track};

pub struct Subgroups {
    pub track: Arc<Track>,
}

impl Subgroups {
    pub fn produce(self) -> (SubgroupsWriter, SubgroupsReader) {
        let (writer, reader) = State::default().split();

        let writer = SubgroupsWriter::new(writer, self.track.clone());
        let reader = SubgroupsReader::new(reader, self.track);

        (writer, reader)
    }
}

impl Deref for Subgroups {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.track
    }
}

// State shared between the writer and reader.
struct SubgroupsState {
    // Created subgroups still within the history window, in creation order. Each
    // reader walks this via its own absolute cursor (SubgroupsReader::read_index),
    // so every subgroup of a group is delivered — not just the latest. Mirrors
    // the object-level SubgroupState.
    subgroups: Vec<SubgroupReader>,
    // Count of subgroups pruned off the front by the history window. A reader's
    // absolute read_index maps to `subgroups[read_index - pruned]`.
    pruned: usize,
    closed: Result<(), ServeError>,
}

impl Default for SubgroupsState {
    fn default() -> Self {
        Self {
            subgroups: Vec::new(),
            pruned: 0,
            closed: Ok(()),
        }
    }
}

pub struct SubgroupsWriter {
    pub info: Arc<Track>,
    state: State<SubgroupsState>,
    next_subgroup_id: u64, // Not in the state to avoid a lock
    next_group_id: u64,    // Not in the state to avoid a lock
    last_group_id: u64,    // Not in the state to avoid a lock
    // Retain only subgroups of the most recent `history_window_groups` group ids;
    // older groups are pruned on create(). None = retain unbounded (matching the
    // object-level stream). Live publishers MUST set this to bound memory.
    history_window_groups: Option<u64>,
}

impl SubgroupsWriter {
    fn new(state: State<SubgroupsState>, track: Arc<Track>) -> Self {
        Self {
            info: track,
            state,
            next_subgroup_id: 0,
            next_group_id: 0,
            last_group_id: 0,
            history_window_groups: None,
        }
    }

    /// Bound retained subgroup history to the most recent `groups` group ids.
    /// As newer groups arrive, subgroups of groups older than the window are
    /// pruned. Required for live publishers to keep memory bounded; without it,
    /// history grows unbounded (one subgroup-reader handle per subgroup created).
    /// `groups` must be >= 1. Assumes monotonically non-decreasing group ids
    /// (enforced upstream by the publisher's MPU-sequence A2 check).
    pub fn set_history_window(&mut self, groups: u64) {
        assert!(groups >= 1, "history window must retain at least one group");
        self.history_window_groups = Some(groups);
    }

    // Helper to increment the group by one.
    pub fn append(&mut self, priority: u8) -> Result<SubgroupWriter, ServeError> {
        let group_id;
        let subgroup_id;

        // TODO: refactor here... For now, every subgroup is mapped to a new group...
        let start_new_group = true;

        if start_new_group {
            group_id = self.next_group_id;
            subgroup_id = 0;
        } else {
            group_id = self.last_group_id;
            subgroup_id = self.next_subgroup_id;
        }

        self.create(Subgroup {
            group_id,
            subgroup_id,
            priority,
        })
    }

    /// Create a new subgroup with the given parameters, inserting it into the track.
    pub fn create(&mut self, subgroup: Subgroup) -> Result<SubgroupWriter, ServeError> {
        let subgroup = SubgroupInfo {
            track: self.info.clone(),
            group_id: subgroup.group_id,
            subgroup_id: subgroup.subgroup_id,
            priority: subgroup.priority,
        };
        let (writer, reader) = subgroup.produce();

        // Append unconditionally — every subgroup is retained and delivered, in
        // creation order. (The old latest-wins logic dropped all but the newest
        // subgroup of a group, which is incompatible with Mapping B's
        // subgroup-per-MFU.) MoQ permits subgroups in any order; the subscriber
        // reorders by (group, subgroup, object) ids.
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        self.next_subgroup_id = writer.subgroup_id + 1;
        self.next_group_id = writer.group_id + 1;
        self.last_group_id = writer.group_id;
        state.subgroups.push(reader);

        // Bound history to the most recent `window` group ids. Prune any subgroup
        // whose group is at least `window` groups behind the newest. Group ids are
        // monotonically non-decreasing (publisher A2), so prunable subgroups form a
        // contiguous prefix. The additive form avoids unsigned underflow when the
        // newest group id is smaller than the window.
        if let Some(window) = self.history_window_groups {
            let newest = writer.group_id;
            let mut k = 0;
            while k < state.subgroups.len()
                && state.subgroups[k].group_id.saturating_add(window) <= newest
            {
                k += 1;
            }
            if k > 0 {
                state.subgroups.drain(0..k);
                state.pruned += k;
            }
        }

        Ok(writer)
    }

    /// Close the segment with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }
}

impl Deref for SubgroupsWriter {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[derive(Clone)]
pub struct SubgroupsReader {
    pub info: Arc<Track>,
    state: State<SubgroupsState>,
    // Cursor into SubgroupsState::subgroups. Cloned readers inherit this index
    // but then advance in parallel — each receives every subgroup.
    read_index: usize,
}

impl SubgroupsReader {
    fn new(state: State<SubgroupsState>, track_info: Arc<Track>) -> Self {
        Self {
            info: track_info,
            state,
            read_index: 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                // read_index is absolute (counts pruned entries). A reader that
                // fell behind the prune window skips ahead to the oldest retained
                // subgroup.
                if self.read_index < state.pruned {
                    self.read_index = state.pruned;
                }
                let vec_idx = self.read_index - state.pruned;
                if vec_idx < state.subgroups.len() {
                    let subgroup = state.subgroups[vec_idx].clone();
                    self.read_index += 1;
                    return Ok(Some(subgroup));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None),
                }
            }
            .await; // Try again when the state changes
        }
    }

    // Returns the largest group/sequence
    pub fn latest(&self) -> Option<(u64, u64)> {
        let state = self.state.lock();
        state
            .subgroups
            .last()
            .map(|group| (group.group_id, group.latest()))
    }

    /// Check if the subgroups writer has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock();
        state.closed.is_err() || state.modified().is_none()
    }
}

impl Deref for SubgroupsReader {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Parameters that can be specified by the user
#[derive(Debug, Clone, PartialEq)]
pub struct Subgroup {
    // The sequence number of the group within the track.
    // NOTE: These may be received out of order or with gaps.
    pub group_id: u64,

    // The sequence number of the subgroup within the group.
    // NOTE: These may be received out of order or with gaps.
    pub subgroup_id: u64,

    // The priority of the group within the track.
    pub priority: u8,
}

/// Static information about the group
#[derive(Debug, Clone, PartialEq)]
pub struct SubgroupInfo {
    pub track: Arc<Track>,

    // The sequence number of the group within the track.
    // NOTE: These may be received out of order or with gaps.
    pub group_id: u64,

    // The sequence number of the subgroup within the group.
    // NOTE: These may be received out of order or with gaps.
    pub subgroup_id: u64,

    // The priority of the group within the track.
    pub priority: u8,
}

impl SubgroupInfo {
    pub fn produce(self) -> (SubgroupWriter, SubgroupReader) {
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupWriter::new(writer, info.clone());
        let reader = SubgroupReader::new(reader, info);

        (writer, reader)
    }
}

impl Deref for SubgroupInfo {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.track
    }
}

struct SubgroupState {
    // The data that has been received thus far.
    objects: Vec<SubgroupObjectReader>,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl Default for SubgroupState {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            closed: Ok(()),
        }
    }
}

/// Used to write data to a stream and notify readers.
pub struct SubgroupWriter {
    // Mutable stream state.
    state: State<SubgroupState>,

    // Immutable stream state.
    pub info: Arc<SubgroupInfo>,

    // The next object sequence number to use.
    next_object_id: u64,
}

impl SubgroupWriter {
    fn new(state: State<SubgroupState>, group: Arc<SubgroupInfo>) -> Self {
        Self {
            state,
            info: group,
            next_object_id: 0,
        }
    }

    /// Create the next object ID with the given payload.
    pub fn write(&mut self, payload: bytes::Bytes) -> Result<(), ServeError> {
        let mut object = self.create(payload.len(), None)?;
        object.write(payload)?;
        Ok(())
    }

    /// Write an object over multiple writes.
    ///
    /// BAD STUFF will happen if the size is wrong; this is an advanced feature.
    pub fn create(
        &mut self,
        size: usize,
        extension_headers: Option<crate::data::ExtensionHeaders>,
    ) -> Result<SubgroupObjectWriter, ServeError> {
        let (writer, reader) = SubgroupObject {
            group: self.info.clone(),
            object_id: self.next_object_id,
            status: ObjectStatus::NormalObject,
            size,
            extension_headers: extension_headers.unwrap_or_default(),
        }
        .produce();

        self.next_object_id += 1;

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        state.objects.push(reader);

        Ok(writer)
    }

    /// Close the stream with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.state.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for SubgroupWriter {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Notified when a stream has new data available.
#[derive(Clone)]
pub struct SubgroupReader {
    // Modify the stream state.
    state: State<SubgroupState>,

    // Immutable stream state.
    pub info: Arc<SubgroupInfo>,

    // The number of chunks that we've read.
    // NOTE: Cloned readers inherit this index, but then run in parallel.
    read_index: usize,
}

impl SubgroupReader {
    fn new(state: State<SubgroupState>, subgroup: Arc<SubgroupInfo>) -> Self {
        Self {
            state,
            info: subgroup,
            read_index: 0,
        }
    }

    pub fn latest(&self) -> u64 {
        let state = self.state.lock();
        state
            .objects
            .last()
            .map(|o| o.object_id)
            .unwrap_or_default()
    }

    pub async fn read_next(&mut self) -> Result<Option<Bytes>, ServeError> {
        let object = self.next().await?;
        match object {
            Some(mut object) => Ok(Some(object.read_all().await?)),
            None => Ok(None),
        }
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupObjectReader>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.read_index < state.objects.len() {
                    let object = state.objects[self.read_index].clone();
                    self.read_index += 1;
                    return Ok(Some(object));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None),
                }
            }
            .await; // Try again when the state changes
        }
    }

    pub fn pos(&self) -> usize {
        self.read_index
    }

    pub fn len(&self) -> usize {
        self.state.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for SubgroupReader {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// A subset of Object, since we use the group's info.
#[derive(Clone, PartialEq, Debug)]
pub struct SubgroupObject {
    pub group: Arc<SubgroupInfo>,

    pub object_id: u64,

    // The size of the object.
    pub size: usize,

    // Object status
    pub status: ObjectStatus,

    // Extension headers (for draft-14 compliance, particularly immutable extensions)
    pub extension_headers: crate::data::ExtensionHeaders,
}

impl SubgroupObject {
    pub fn produce(self) -> (SubgroupObjectWriter, SubgroupObjectReader) {
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupObjectWriter::new(writer, info.clone());
        let reader = SubgroupObjectReader::new(reader, info);

        (writer, reader)
    }
}

impl Deref for SubgroupObject {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.group
    }
}

struct SubgroupObjectState {
    // The data that has been received thus far.
    chunks: Vec<Bytes>,

    // Set when the writer is dropped.
    closed: Result<(), ServeError>,
}

impl Default for SubgroupObjectState {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            closed: Ok(()),
        }
    }
}

/// Used to write data to a segment and notify readers.
pub struct SubgroupObjectWriter {
    // Mutable segment state.
    state: State<SubgroupObjectState>,

    // Immutable segment state.
    pub info: Arc<SubgroupObject>,

    // The amount of promised data that has yet to be written.
    remain: usize,
}

impl SubgroupObjectWriter {
    /// Create a new segment with the given info.
    fn new(state: State<SubgroupObjectState>, object: Arc<SubgroupObject>) -> Self {
        Self {
            state,
            remain: object.size,
            info: object,
        }
    }

    /// Write a new chunk of bytes.
    pub fn write(&mut self, chunk: Bytes) -> Result<(), ServeError> {
        if chunk.len() > self.remain {
            return Err(ServeError::Size);
        }
        self.remain -= chunk.len();

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        state.chunks.push(chunk);

        Ok(())
    }

    /// Close the segment with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        if self.remain != 0 {
            return Err(ServeError::Size);
        }

        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }
}

impl Drop for SubgroupObjectWriter {
    fn drop(&mut self) {
        if self.remain == 0 {
            return;
        }

        if let Some(mut state) = self.state.lock_mut() {
            state.closed = Err(ServeError::Size);
        }
    }
}

impl Deref for SubgroupObjectWriter {
    type Target = SubgroupObject;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Notified when a segment has new data available.
#[derive(Clone)]
pub struct SubgroupObjectReader {
    // Modify the segment state.
    state: State<SubgroupObjectState>,

    // Immutable segment state.
    pub info: Arc<SubgroupObject>,

    // The number of chunks that we've read.
    // NOTE: Cloned readers inherit this index, but then run in parallel.
    index: usize,
}

impl SubgroupObjectReader {
    fn new(state: State<SubgroupObjectState>, object: Arc<SubgroupObject>) -> Self {
        Self {
            state,
            info: object,
            index: 0,
        }
    }

    /// Block until the next chunk of bytes is available.
    pub async fn read(&mut self) -> Result<Option<Bytes>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.index < state.chunks.len() {
                    let chunk = state.chunks[self.index].clone();
                    self.index += 1;
                    return Ok(Some(chunk));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None), // No more changes will come
                }
            }
            .await; // Try again when the state changes
        }
    }

    pub async fn read_all(&mut self) -> Result<Bytes, ServeError> {
        let mut chunks = Vec::new();
        while let Some(chunk) = self.read().await? {
            chunks.push(chunk);
        }

        Ok(Bytes::from(chunks.concat()))
    }
}

impl Deref for SubgroupObjectReader {
    type Target = SubgroupObject;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::TrackNamespace;

    fn track() -> Arc<Track> {
        Arc::new(Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string()))
    }

    // Mapping B emits multiple subgroups per group (subgroup 0 = Init, 1..M = MFUs).
    // The reader MUST deliver every created subgroup, not just the latest.
    #[tokio::test]
    async fn delivers_all_subgroups_in_one_group() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let _a = writer
            .create(Subgroup { group_id: 0, subgroup_id: 0, priority: 0 })
            .unwrap();
        let _b = writer
            .create(Subgroup { group_id: 0, subgroup_id: 1, priority: 0 })
            .unwrap();
        drop(writer); // close so a drained reader returns None instead of blocking

        let mut got = Vec::new();
        while let Some(s) = reader.next().await.unwrap() {
            got.push((s.group_id, s.subgroup_id));
        }
        got.sort();
        assert_eq!(
            got,
            vec![(0, 0), (0, 1)],
            "both subgroups of group 0 must be delivered (latest-wins drops subgroup 0)"
        );
    }

    // With a 1-group history window, subgroups of older groups are pruned as
    // newer groups arrive, bounding memory. A reader arriving after the prune
    // sees only the surviving (newest) group.
    #[tokio::test]
    async fn prunes_subgroups_outside_group_window() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        writer.set_history_window(1);
        let _a = writer
            .create(Subgroup { group_id: 0, subgroup_id: 0, priority: 0 })
            .unwrap();
        let _b = writer
            .create(Subgroup { group_id: 0, subgroup_id: 1, priority: 0 })
            .unwrap();
        let _c = writer
            .create(Subgroup { group_id: 1, subgroup_id: 0, priority: 0 })
            .unwrap();
        drop(writer);

        let mut got = Vec::new();
        while let Some(s) = reader.next().await.unwrap() {
            got.push((s.group_id, s.subgroup_id));
        }
        assert_eq!(
            got,
            vec![(1, 0)],
            "group 0 subgroups pruned once group 1 arrived (window=1)"
        );
    }

    // The window only affects readers that fall behind. A reader consuming each
    // subgroup as it is created must still see every one, even when an older
    // group is pruned after the reader already passed it.
    #[tokio::test]
    async fn keeping_up_reader_sees_all_despite_window() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        writer.set_history_window(1);

        let _a = writer
            .create(Subgroup { group_id: 0, subgroup_id: 0, priority: 0 })
            .unwrap();
        let s0 = reader.next().await.unwrap().expect("g0s0");
        assert_eq!((s0.group_id, s0.subgroup_id), (0, 0));

        let _b = writer
            .create(Subgroup { group_id: 0, subgroup_id: 1, priority: 0 })
            .unwrap();
        let s1 = reader.next().await.unwrap().expect("g0s1");
        assert_eq!((s1.group_id, s1.subgroup_id), (0, 1));

        // Creating group 1 prunes group 0, but the reader already consumed it.
        let _c = writer
            .create(Subgroup { group_id: 1, subgroup_id: 0, priority: 0 })
            .unwrap();
        let s2 = reader.next().await.unwrap().expect("g1s0");
        assert_eq!((s2.group_id, s2.subgroup_id), (1, 0));
    }

    // Cloned readers each carry their own cursor and independently receive every
    // subgroup (fanout).
    #[tokio::test]
    async fn cloned_readers_each_receive_all_subgroups() {
        let (mut writer, reader) = Subgroups { track: track() }.produce();
        let _a = writer
            .create(Subgroup { group_id: 0, subgroup_id: 0, priority: 0 })
            .unwrap();
        let _b = writer
            .create(Subgroup { group_id: 0, subgroup_id: 1, priority: 0 })
            .unwrap();
        drop(writer);

        let reader2 = reader.clone();
        let collect = |mut r: SubgroupsReader| async move {
            let mut got = Vec::new();
            while let Some(s) = r.next().await.unwrap() {
                got.push((s.group_id, s.subgroup_id));
            }
            got
        };
        assert_eq!(collect(reader).await, vec![(0, 0), (0, 1)]);
        assert_eq!(collect(reader2).await, vec![(0, 0), (0, 1)]);
    }

    // append() (one subgroup per new group) still works: increasing group ids,
    // subgroup 0, all delivered.
    #[tokio::test]
    async fn append_creates_increasing_groups_all_delivered() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let _a = writer.append(0).unwrap();
        let _b = writer.append(0).unwrap();
        drop(writer);

        let mut got = Vec::new();
        while let Some(s) = reader.next().await.unwrap() {
            got.push((s.group_id, s.subgroup_id));
        }
        assert_eq!(got, vec![(0, 0), (1, 0)]);
    }
}
