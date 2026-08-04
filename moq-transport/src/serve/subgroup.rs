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
use std::{
    collections::{HashSet, VecDeque},
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};

use bytes::Bytes;

use crate::data::ObjectStatus;
use crate::watch::State;

use super::{ServeError, Track};

type LargestLocation = Arc<Mutex<Option<(u64, u64)>>>;

pub struct Subgroups {
    pub track: Arc<Track>,
}

impl Subgroups {
    pub fn produce(self) -> (SubgroupsWriter, SubgroupsReader) {
        let (writer, reader) = State::default().split();
        let largest_location = Arc::new(Mutex::new(None));

        let writer = SubgroupsWriter::new(writer, self.track.clone(), largest_location.clone());
        let reader = SubgroupsReader::new(reader, self.track, largest_location);

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
    // Created subgroups that at least one active reader can still consume, plus
    // the latest subgroup for a late joiner. Cursors are absolute so pruning the
    // VecDeque front does not invalidate active readers.
    subgroups: VecDeque<SubgroupReader>,
    first_index: usize,
    // Duplicate keys retained in lockstep with subgroup payloads.
    retained_keys: HashSet<(u64, u64)>,
    // Greatest lexicographic ordering key reclaimed from the payload queue.
    // Closing every key at or below this frontier bounds retired identity state
    // while preventing a long-lived reader from observing a reused key.
    retired_through: Option<(u64, u64)>,
    reader_cursors: Vec<Weak<AtomicUsize>>,
    closed: Result<(), ServeError>,
}

impl SubgroupsState {
    fn register_reader(&mut self, read_index: usize) -> Arc<AtomicUsize> {
        let cursor = Arc::new(AtomicUsize::new(read_index));
        self.reader_cursors.push(Arc::downgrade(&cursor));
        cursor
    }

    fn prune_consumed(&mut self) {
        let mut oldest_needed = None;
        self.reader_cursors.retain(|weak| {
            let Some(cursor) = weak.upgrade() else {
                return false;
            };

            let read_index = cursor.load(Ordering::Relaxed);
            oldest_needed =
                Some(oldest_needed.map_or(read_index, |oldest: usize| oldest.min(read_index)));
            true
        });

        if self.subgroups.is_empty() {
            return;
        }

        let end_index = self.first_index + self.subgroups.len();
        let latest_index = end_index - 1;
        let retain_from = oldest_needed
            .unwrap_or(end_index)
            .clamp(self.first_index, latest_index);
        let prune_count = retain_from - self.first_index;
        if prune_count > 0 {
            for _ in 0..prune_count {
                let subgroup = self
                    .subgroups
                    .pop_front()
                    .expect("prune count is bounded by the subgroup queue");
                let key = (subgroup.group_id, subgroup.subgroup_id);
                let removed = self.retained_keys.remove(&key);
                debug_assert!(removed, "retained key must mirror subgroup payload");
                self.retired_through = Some(
                    self.retired_through
                        .map_or(key, |frontier| frontier.max(key)),
                );
            }
            self.first_index = retain_from;
        }
    }
}

impl Default for SubgroupsState {
    fn default() -> Self {
        Self {
            subgroups: VecDeque::new(),
            first_index: 0,
            retained_keys: HashSet::new(),
            retired_through: None,
            reader_cursors: Vec::new(),
            closed: Ok(()),
        }
    }
}

pub struct SubgroupsWriter {
    pub info: Arc<Track>,
    state: State<SubgroupsState>,
    largest_location: LargestLocation,
    next_subgroup_id: u64, // Not in the state to avoid a lock
    next_group_id: u64,    // Not in the state to avoid a lock
    last_group_id: u64,    // Not in the state to avoid a lock
}

impl SubgroupsWriter {
    fn new(
        state: State<SubgroupsState>,
        track: Arc<Track>,
        largest_location: LargestLocation,
    ) -> Self {
        Self {
            info: track,
            state,
            largest_location,
            next_subgroup_id: 0,
            next_group_id: 0,
            last_group_id: 0,
        }
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
    ///
    /// Subgroups may be created out of order while their keys are newer than the
    /// reclamation frontier. Once a key is reclaimed, that key and every older
    /// ordering key are closed and return [ServeError::Duplicate].
    pub fn create(&mut self, subgroup: Subgroup) -> Result<SubgroupWriter, ServeError> {
        let subgroup = SubgroupInfo {
            track: self.info.clone(),
            group_id: subgroup.group_id,
            subgroup_id: subgroup.subgroup_id,
            priority: subgroup.priority,
        };
        let (writer, reader) =
            subgroup.produce_with_largest_location(Some(self.largest_location.clone()));

        // Retain and deliver every subgroup in creation order. The previous
        // latest-wins logic kept only the newest subgroup reader, so any earlier
        // subgroup of the same group — and any subgroup arriving out of order —
        // was silently dropped before a subscriber could read it. MoQ allows a
        // group to carry multiple subgroups and permits them in any order; the
        // subscriber reorders by (group, subgroup, object) ids.
        //
        // Deliver-all and uniqueness are orthogonal. Retained keys reject exact
        // duplicates; reclaimed keys advance a single closed frontier. The
        // frontier conservatively rejects late holes so identity metadata cannot
        // grow for the lifetime of a live track.
        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        let key = (writer.group_id, writer.subgroup_id);
        if state
            .retired_through
            .is_some_and(|frontier| key <= frontier)
            || !state.retained_keys.insert(key)
        {
            return Err(ServeError::Duplicate);
        }

        self.next_subgroup_id = writer.subgroup_id.saturating_add(1);
        self.next_group_id = self.next_group_id.max(writer.group_id.saturating_add(1));
        self.last_group_id = writer.group_id;
        state.subgroups.push_back(reader);
        state.prune_consumed();

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

pub struct SubgroupsReader {
    pub info: Arc<Track>,
    state: State<SubgroupsState>,
    largest_location: LargestLocation,
    // Absolute cursor into SubgroupsState::subgroups. Active readers publish
    // their cursor so payloads can be released after every reader advances.
    read_index: usize,
    active_cursor: Option<Arc<AtomicUsize>>,
}

impl SubgroupsReader {
    fn new(
        state: State<SubgroupsState>,
        track_info: Arc<Track>,
        largest_location: LargestLocation,
    ) -> Self {
        let mut reader = Self {
            info: track_info,
            state,
            largest_location,
            read_index: 0,
            active_cursor: None,
        };
        reader.activate();
        reader
    }

    fn activate(&mut self) {
        if self.active_cursor.is_some() {
            return;
        }

        let mut state = self.state.lock().into_mut_closed();
        self.read_index = self.read_index.max(state.first_index);
        self.active_cursor = Some(state.register_reader(self.read_index));
    }

    fn advance(&mut self, read_index: usize) {
        self.read_index = read_index;
        let cursor = self
            .active_cursor
            .as_ref()
            .expect("subgroups reader must be active before advancing");

        let mut state = self.state.lock().into_mut_closed();
        cursor.store(read_index, Ordering::Relaxed);
        state.prune_consumed();
    }

    /// Stop retaining history for the reader stored as a Track mode template.
    /// A clone becomes active when a subscriber starts consuming it.
    pub(super) fn into_template(mut self) -> Self {
        self.active_cursor = None;
        self.state.lock().into_mut_closed().prune_consumed();
        self
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
        self.activate();

        loop {
            {
                let state = self.state.lock();

                let read_index = self.read_index.max(state.first_index);
                let queue_index = read_index - state.first_index;
                if queue_index < state.subgroups.len() {
                    let subgroup = state.subgroups[queue_index].clone();
                    drop(state);
                    self.advance(read_index + 1);
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

    // Returns the largest populated group/object location seen on the track.
    pub fn latest(&self) -> Option<(u64, u64)> {
        *self
            .largest_location
            .lock()
            .expect("largest location mutex poisoned")
    }

    /// Check if the subgroups writer has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock();
        state.closed.is_err() || state.modified().is_none()
    }
}

impl Clone for SubgroupsReader {
    fn clone(&self) -> Self {
        let mut state = self.state.lock().into_mut_closed();
        let read_index = if self.active_cursor.is_none() {
            // Track stores an inactive cloning template. A late joiner starts
            // at the retained tail even when another active reader pins older
            // payloads in the queue.
            state.first_index + state.subgroups.len().saturating_sub(1)
        } else {
            // Fanout from an active reader inherits that reader's position.
            self.read_index.max(state.first_index)
        };
        let active_cursor = state.register_reader(read_index);

        Self {
            info: self.info.clone(),
            state: self.state.clone(),
            largest_location: self.largest_location.clone(),
            read_index,
            active_cursor: Some(active_cursor),
        }
    }
}

impl Drop for SubgroupsReader {
    fn drop(&mut self) {
        self.active_cursor = None;
        self.state.lock().into_mut_closed().prune_consumed();
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
        self.produce_with_largest_location(None)
    }

    fn produce_with_largest_location(
        self,
        largest_location: Option<LargestLocation>,
    ) -> (SubgroupWriter, SubgroupReader) {
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupWriter::new(writer, info.clone(), largest_location);
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

    // Populated only when this subgroup belongs to a Subgroups track.
    largest_location: Option<LargestLocation>,

    // The next object sequence number to use.
    next_object_id: u64,
}

impl SubgroupWriter {
    fn new(
        state: State<SubgroupState>,
        group: Arc<SubgroupInfo>,
        largest_location: Option<LargestLocation>,
    ) -> Self {
        Self {
            state,
            info: group,
            largest_location,
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

        let object_id = self.next_object_id;
        self.next_object_id += 1;

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        state.objects.push(reader);
        drop(state);

        if let Some(largest_location) = &self.largest_location {
            let location = (self.group_id, object_id);
            let mut largest = largest_location
                .lock()
                .expect("largest location mutex poisoned");
            *largest = Some(largest.map_or(location, |current| current.max(location)));
        }

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

    pub fn latest(&self) -> Option<u64> {
        let state = self.state.lock();
        state.objects.last().map(|o| o.object_id)
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
        Arc::new(Track::new(
            TrackNamespace::from_utf8_path("ns"),
            "t".to_string(),
        ))
    }

    // A group may carry more than one subgroup. The reader MUST deliver every
    // created subgroup; latest-wins dropped all but the newest.
    #[tokio::test]
    async fn delivers_all_subgroups_in_one_group() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let _a = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let _b = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 1,
                priority: 0,
            })
            .unwrap();
        drop(writer); // close so a drained reader returns None instead of blocking

        let mut got = Vec::new();
        while let Some(s) = reader.next().await.unwrap() {
            got.push((s.group_id, s.subgroup_id));
        }
        assert_eq!(
            got,
            vec![(0, 0), (0, 1)],
            "both subgroups of group 0 must be delivered (latest-wins drops subgroup 0)"
        );
    }

    // Deliver-all retains every subgroup, so a repeated (group_id, subgroup_id)
    // would hand readers two entries with the same ordering key. create() must
    // reject the duplicate — the API cannot trust arbitrary callers.
    #[tokio::test]
    async fn create_rejects_duplicate_group_subgroup() {
        let (mut writer, _reader) = Subgroups { track: track() }.produce();
        let _first = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let dup = writer.create(Subgroup {
            group_id: 0,
            subgroup_id: 0,
            priority: 0,
        });
        assert!(
            matches!(dup, Err(ServeError::Duplicate)),
            "repeated (group,subgroup) must return ServeError::Duplicate"
        );
    }

    #[tokio::test]
    async fn consume_prune_then_recreate_is_duplicate() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let _first = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let consumed = reader.next().await.unwrap().expect("first subgroup");
        assert_eq!((consumed.group_id, consumed.subgroup_id), (0, 0));

        let _second = writer
            .create(Subgroup {
                group_id: 1,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        assert_eq!(
            reader.state.lock().first_index,
            1,
            "creating the next subgroup must prune the consumed payload"
        );
        assert_eq!(reader.state.lock().retired_through, Some((0, 0)));

        let duplicate = writer.create(Subgroup {
            group_id: 0,
            subgroup_id: 0,
            priority: 0,
        });
        assert!(
            matches!(duplicate, Err(ServeError::Duplicate)),
            "payload pruning must not release ownership of an ordering key"
        );
    }

    #[tokio::test]
    async fn create_rejects_key_behind_retired_frontier() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let _first = writer
            .create(Subgroup {
                group_id: 5,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let _consumed = reader.next().await.unwrap().expect("first subgroup");
        let _second = writer
            .create(Subgroup {
                group_id: 6,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        assert_eq!(reader.state.lock().retired_through, Some((5, 0)));

        let late_hole = writer.create(Subgroup {
            group_id: 4,
            subgroup_id: u64::MAX,
            priority: 0,
        });
        assert!(
            matches!(late_hole, Err(ServeError::Duplicate)),
            "keys behind the bounded reclamation frontier must fail closed"
        );
    }

    // Each cloned reader keeps its own cursor, so fanout still delivers the full
    // sequence to every reader.
    #[tokio::test]
    async fn cloned_readers_each_receive_all_subgroups() {
        let (mut writer, reader) = Subgroups { track: track() }.produce();
        let _a = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let _b = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 1,
                priority: 0,
            })
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

    #[tokio::test]
    async fn out_of_order_create_does_not_regress_append_group_id() {
        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let _newer = writer
            .create(Subgroup {
                group_id: 5,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let _older = writer
            .create(Subgroup {
                group_id: 4,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();
        let appended = writer.append(0).unwrap();
        assert_eq!(appended.group_id, 6);
        drop(writer);

        let mut got = Vec::new();
        while let Some(s) = reader.next().await.unwrap() {
            got.push((s.group_id, s.subgroup_id));
        }
        assert_eq!(got, vec![(5, 0), (4, 0), (6, 0)]);
    }

    #[tokio::test]
    async fn bounds_payload_and_identity_retention() {
        const SUBGROUP_COUNT: usize = 4096;

        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        for group_id in 0..SUBGROUP_COUNT {
            let created = writer.append(0).unwrap();
            assert_eq!(created.group_id, group_id as u64);

            let delivered = reader.next().await.unwrap().expect("created subgroup");
            assert_eq!(delivered.group_id, group_id as u64);
        }

        let state = reader.state.lock();
        assert_eq!(state.subgroups.len(), 1, "retain only the late-join tail");
        assert_eq!(
            state.retained_keys.len(),
            1,
            "retained identity keys must mirror the bounded payload queue"
        );
        assert_eq!(state.first_index, SUBGROUP_COUNT - 1);
        assert_eq!(
            state.retired_through,
            Some(((SUBGROUP_COUNT - 2) as u64, 0)),
            "retired identity ownership must collapse into one frontier"
        );
    }

    #[tokio::test]
    async fn dropping_lagging_reader_releases_its_backlog() {
        const SUBGROUP_COUNT: usize = 4096;

        let (mut writer, mut reader) = Subgroups { track: track() }.produce();
        let lagging = reader.clone();
        for _ in 0..SUBGROUP_COUNT {
            let _subgroup = writer.append(0).unwrap();
            let _delivered = reader.next().await.unwrap().expect("created subgroup");
        }

        assert_eq!(
            reader.state.lock().subgroups.len(),
            SUBGROUP_COUNT,
            "the lagging reader still needs the backlog"
        );
        drop(lagging);
        assert_eq!(
            reader.state.lock().subgroups.len(),
            1,
            "dropping the lagging reader releases consumed payloads"
        );
    }

    #[tokio::test]
    async fn track_template_does_not_pin_published_history() {
        const SUBGROUP_COUNT: usize = 4096;

        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string()).produce();
        let mut writer = track_writer.subgroups().unwrap();
        for _ in 0..SUBGROUP_COUNT {
            let _subgroup = writer.append(0).unwrap();
        }

        let mut reader = match track_reader.mode().await.unwrap() {
            super::super::TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };
        {
            let state = reader.state.lock();
            assert_eq!(state.subgroups.len(), 1, "template must not retain history");
            assert_eq!(state.first_index, SUBGROUP_COUNT - 1);
        }

        let latest = reader.next().await.unwrap().expect("latest subgroup");
        assert_eq!(latest.group_id, (SUBGROUP_COUNT - 1) as u64);
    }

    #[tokio::test]
    async fn track_template_clone_starts_at_tail_while_reader_lags() {
        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("ns"), "t".to_string()).produce();
        let mut writer = track_writer.subgroups().unwrap();
        let _first = writer.append(0).unwrap();

        let lagging = match track_reader.mode().await.unwrap() {
            super::super::TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };
        let _second = writer.append(0).unwrap();
        let _latest = writer.append(0).unwrap();

        assert_eq!(
            lagging.state.lock().subgroups.len(),
            3,
            "the lagging active reader must pin its unread payloads"
        );
        let mut active_clone = lagging.clone();
        let mut late_joiner = match track_reader.mode().await.unwrap() {
            super::super::TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };
        drop(writer);

        let inherited = active_clone.next().await.unwrap().expect("first subgroup");
        assert_eq!(
            inherited.group_id, 0,
            "a clone of an active reader must inherit its source cursor"
        );

        let latest = late_joiner.next().await.unwrap().expect("latest subgroup");
        assert_eq!(latest.group_id, 2);
        assert!(
            late_joiner.next().await.unwrap().is_none(),
            "a Track late joiner must not inherit another reader's backlog"
        );
    }
}
