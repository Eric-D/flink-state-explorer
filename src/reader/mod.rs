//! Lazy, random access to a canonical savepoint's keyed state, one key-group at a time.
//!
//! A [`StateReader`] parses `_metadata` once. The first time an operator is used, each of its
//! subtasks' keyed streams is opened once ([`source`]) and its header — and only its header, which
//! ends where the first key-group begins — is resolved: state names, types, value serializers,
//! POJO layouts. State ids are resolved **per subtask**, since Flink registers states lazily and
//! two subtasks may number them differently. After that every operation reads whole key-group
//! blocks, located by `KeyGroupsStateHandle.offsets`, and nothing else:
//!
//! - [`StateReader::key_page`] pages through the keys, each with all its states' values;
//! - [`StateReader::get`] finds one key by computing Flink's key-group for it
//!   ([`crate::parser::key_group`]) and reading that block alone — an empty key-group answers
//!   without any I/O;
//! - [`StateReader::scan`] tests a predicate on rendered keys under a byte budget, for search.
//!
//! Memory is bounded by the blocks in flight plus a small byte-bounded cache of recently read
//! blocks (so paging inside one key-group does not re-read it), never by the savepoint's size.
//! Values are decoded through [`crate::parser::value`] only — the single decoder — and MAP
//! entries through [`crate::parser::value::decode_map_entry`].
//!
//! The streaming export ([`crate::export_state`]) is a client of the same pieces
//! ([`OpContext`], [`SubtaskStream::block_range`], [`SubtaskStream::read_block`],
//! [`decode_entry_value`]).
//!
//! ## Order and cursors
//!
//! Keys come in **key-group order** (ascending absolute key-group, which is also subtask order),
//! and inside one key-group in **on-disk order**: the order in which each key first appears in
//! the block. A [`Cursor`] is `(subtask, key-group, ordinal)`: the ordinal-th key of that block in
//! that order. It is stable as long as the savepoint does not change, and its string form is
//! `subtask.keygroup.ordinal` (e.g. `1.64.17`).

pub(crate) mod source;

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use crate::parser::key_group::{key_group_for_hash, subtask_for_key_group, JavaKey};
use crate::parser::keyed_state::{
    count_states_in_header, extract_entry_layouts, extract_state_metadata,
    extract_value_pojo_classes, extract_value_serializer_types, key_group_block,
    parse_key_group_block, read_keyed_stream_header, BlockDefect, EntryLayout, KeyGroupParse,
    KeyedEntry,
};
use crate::parser::metadata::{Operator, Savepoint};
use crate::parser::pojo::{extract_pojo_infos, PojoInfo};
use crate::parser::state_handle::{KeyGroupsStateHandle, StateHandle, StreamStateHandle};
use crate::proto::ProtoContext;

pub(crate) use source::read_whole_stream;
use source::{inline_bytes, source_for, Source};

/// Default size of the block cache: a few key-groups of a large savepoint (~1.6 MB each on a
/// 211 MB / 128 key-group one).
pub const DEFAULT_CACHE_BYTES: usize = 8 << 20;

/// Lowercase hex of an OperatorID, as `_metadata` stores it.
pub fn hex_id(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{:02x}", b)).collect()
}

// --- public types -------------------------------------------------------------------------------

/// An operator, from `_metadata` alone (no state file is opened to list them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorInfo {
    /// Hex OperatorID: what every other call takes as `op` (a unique prefix also works).
    pub id: String,
    /// The operator's name (metadata v5+) or uid, else empty.
    pub name: String,
    pub parallelism: i32,
    pub max_parallelism: i32,
    /// Subtasks holding a keyed-state handle; 0 = no keyed state.
    pub keyed_subtasks: usize,
}

/// One keyed state of an operator, merged over the subtasks that register it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateInfo {
    pub name: String,
    /// `VALUE`, `MAP`, `LIST`, `TIMER`, …
    pub state_type: String,
    /// The value serializer type, e.g. `Long`, `Pojo`, `Map<String,Long>` (`Date` for a timer).
    pub value_type: String,
    /// A POJO value's fully-qualified class.
    pub value_class: Option<String>,
    /// How many subtasks' headers register it.
    pub subtasks: usize,
}

/// What [`StateReader::states`] found from the operator's headers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyedStates {
    /// The key serializer type (`String`, `Long`, `Tuple<String,Long>`, `Pojo`, …).
    pub key_type: String,
    /// In order of first registration, over the subtasks in order.
    pub states: Vec<StateInfo>,
    /// One message per state file that could not be opened or whose header could not be read,
    /// naming the file. Non-empty means `states` may be incomplete.
    pub errors: Vec<String>,
}

/// A position in an operator's key order: the `ordinal`-th key (in on-disk order) of absolute
/// key-group `key_group`, held by `subtask`. String form `subtask.keygroup.ordinal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct Cursor {
    pub subtask: i32,
    pub key_group: u16,
    pub ordinal: u32,
}

impl Cursor {
    /// The first key of any operator.
    pub const START: Cursor = Cursor {
        subtask: 0,
        key_group: 0,
        ordinal: 0,
    };
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.subtask, self.key_group, self.ordinal)
    }
}

impl std::str::FromStr for Cursor {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || format!("{:?} is not a cursor (subtask.keygroup.ordinal)", s);
        let mut parts = s.split('.');
        let (Some(a), Some(b), Some(c), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(bad());
        };
        Ok(Cursor {
            subtask: a.parse().map_err(|_| bad())?,
            key_group: b.parse().map_err(|_| bad())?,
            ordinal: c.parse().map_err(|_| bad())?,
        })
    }
}

/// One stored entry of one key: a state's value, or one entry of a MAP state, or one timer.
#[derive(Debug, Clone, PartialEq)]
pub struct StateEntry {
    pub state: String,
    pub state_type: String,
    pub value_type: String,
    /// MAP states: the decoded map key (each map entry is its own `StateEntry`).
    pub map_key: Option<String>,
    /// Raw namespace bytes: `VoidNamespace` (1 byte, `0x00`) for most states, a `TimeWindow` for
    /// window state, namespace + serialized map key for a MAP state, empty for a timer.
    pub namespace: Vec<u8>,
    /// Raw value bytes as stored (for a timer, its firing timestamp as 8 big-endian bytes).
    pub raw: Vec<u8>,
    /// The value decoded by [`crate::parser::value`].
    pub value: Value,
}

/// A key with every entry it has in its key-group, across all states.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRecord {
    /// The key as [`crate::parser::value::decode_key`] renders it.
    pub key: String,
    /// Absolute key-group.
    pub key_group: u16,
    pub subtask: i32,
    /// Where this key is: a [`StateReader::key_page`] from here starts with it.
    pub position: Cursor,
    /// In on-disk order.
    pub entries: Vec<StateEntry>,
}

/// What a call read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadStats {
    /// Non-empty key-group blocks visited.
    pub blocks: usize,
    /// Of those, served from the block cache.
    pub cache_hits: usize,
    /// Bytes read from storage (the stored, possibly compressed, size of the blocks not cached).
    pub bytes_read: u64,
    /// Stored size of every block visited, cached or not.
    pub block_bytes: u64,
    /// Largest (inflated) block held at once.
    pub max_block_bytes: usize,
}

/// A page of keys.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyPage {
    pub keys: Vec<KeyRecord>,
    /// Where the next page starts; `None` when there are no more keys.
    pub next: Option<Cursor>,
    pub stats: ReadStats,
    /// Key-groups that could not be read to their end mark, as `subtask N, key-group K: defect`:
    /// the keys read before the defect are in the page, the rest of that key-group is not.
    pub malformed: Vec<String>,
}

/// The result of a [`StateReader::scan`].
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPage {
    pub matches: Vec<KeyRecord>,
    /// Where to resume; `None` when the whole operator has been scanned.
    pub next: Option<Cursor>,
    pub stats: ReadStats,
    pub malformed: Vec<String>,
}

/// The answer of [`StateReader::get`].
#[derive(Debug, Clone, PartialEq)]
pub enum Lookup {
    Found {
        record: KeyRecord,
        stats: ReadStats,
    },
    /// The key is not in the savepoint: its key-group is empty (no I/O at all), or its block does
    /// not hold it.
    Absent {
        key_group: u16,
        subtask: i32,
        stats: ReadStats,
    },
    /// The key-group cannot be computed from the text (a POJO or opaque key type, or text that
    /// does not parse as the key type): use [`StateReader::scan`].
    NeedsScan {
        reason: String,
    },
}

// --- per-subtask header -------------------------------------------------------------------------

/// A keyed state as one subtask's header registers it.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedState {
    pub name: String,
    pub state_type: String,
    pub value_type: String,
    pub value_class: Option<String>,
    pub pojo: Option<PojoInfo>,
    /// For MAP states: the `Map<K,V>` key and value serializer types.
    pub map_key_type: String,
    pub map_value_type: String,
}

/// Everything one subtask's header says, resolved once.
#[derive(Debug, Clone)]
pub(crate) struct SubtaskHeader {
    pub compressed: bool,
    pub key_type: String,
    pub key_pojo: Option<PojoInfo>,
    /// By stateId.
    pub states: Vec<ResolvedState>,
    pub state_types: Vec<String>,
    /// What follows the key in each state's entries, so keys split from namespaces the same way
    /// as on the index path.
    pub layouts: Vec<EntryLayout>,
    pub num_states: usize,
}

impl SubtaskHeader {
    /// Resolve a keyed stream's header bytes.
    pub(crate) fn resolve(header: &[u8]) -> SubtaskHeader {
        let metas = extract_state_metadata(header);
        let value_types = extract_value_serializer_types(header, &metas);
        let value_pojo_classes = extract_value_pojo_classes(header, &metas);
        let pojo_infos = extract_pojo_infos(header);

        let states = metas
            .iter()
            .enumerate()
            .map(|(idx, meta)| {
                let is_timer = meta.state_type == "TIMER";
                // Mirrors `index::builder`: timers carry a synthesized firing timestamp, and must
                // not be matched against a nearby POJO snapshot.
                let value_type = if is_timer {
                    "Date".to_string()
                } else {
                    value_types.get(idx).cloned().unwrap_or_default()
                };
                let value_class = if is_timer {
                    None
                } else {
                    value_pojo_classes.get(idx).cloned().flatten()
                };
                let pojo = value_class
                    .as_ref()
                    .and_then(|cls| pojo_infos.iter().find(|p| &p.class_name == cls))
                    .cloned();
                let (map_key_type, map_value_type) = if meta.state_type == "MAP" {
                    crate::parser::value::split_map_types(&value_type)
                } else {
                    (String::new(), String::new())
                };
                ResolvedState {
                    name: meta.name.clone(),
                    state_type: meta.state_type.clone(),
                    value_type,
                    value_class,
                    pojo,
                    map_key_type,
                    map_value_type,
                }
            })
            .collect();

        let key_type = metas
            .first()
            .map_or_else(|| "?".to_string(), |m| m.key_type.clone());
        let key_pojo = if key_type == "Pojo" {
            crate::parser::keyed_state::extract_key_pojo_info(header)
        } else {
            None
        };
        SubtaskHeader {
            compressed: read_keyed_stream_header(header)
                .is_some_and(|h| h.using_key_group_compression),
            key_type,
            key_pojo,
            states,
            state_types: metas.iter().map(|m| m.state_type.clone()).collect(),
            layouts: extract_entry_layouts(header, header.len()),
            num_states: count_states_in_header(header, &metas),
        }
    }

    /// The stateId of `name` in this subtask.
    pub(crate) fn state_id(&self, name: &str) -> Option<u16> {
        self.states
            .iter()
            .position(|s| s.name == name)
            .map(|i| i as u16)
    }

    pub(crate) fn parse_ctx(
        &self,
        kg_prefix_len: usize,
        only_state: Option<u16>,
    ) -> KeyGroupParse<'_> {
        KeyGroupParse {
            num_states: self.num_states,
            key_type: &self.key_type,
            kg_prefix_len,
            key_pojo: self.key_pojo.as_ref(),
            state_types: &self.state_types,
            layouts: &self.layouts,
            only_state,
        }
    }
}

/// Decode one entry's value: a MAP entry through [`crate::parser::value::decode_map_entry`] (map
/// key from the namespace, value unwrapped from `NullableSerializer`), anything else through
/// [`crate::parser::value::decode_state_value`]. `fallback_key` keeps undecodable map keys apart.
pub(crate) fn decode_entry_value(
    state: &ResolvedState,
    namespace: &[u8],
    value: &[u8],
    proto: Option<&ProtoContext>,
    fallback_key: usize,
) -> (Option<String>, Value) {
    if state.state_type == "MAP" {
        let (k, v) = crate::parser::value::decode_map_entry(
            namespace,
            value,
            &state.map_key_type,
            &state.map_value_type,
            state.pojo.as_ref(),
            proto,
            fallback_key,
        );
        (Some(k), v)
    } else {
        (
            None,
            crate::parser::value::decode_state_value(
                value,
                &state.value_type,
                state.pojo.as_ref(),
                proto,
            ),
        )
    }
}

/// `CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix`.
pub(crate) fn kg_prefix_len(max_parallelism: i32) -> usize {
    if max_parallelism > 128 {
        2
    } else {
        1
    }
}

// --- opened operators ---------------------------------------------------------------------------

/// One subtask's keyed stream, opened and checked against its handle, with its header resolved.
#[derive(Debug)]
pub(crate) struct SubtaskStream {
    pub subtask_index: i32,
    /// Index into the operator's `subtask_states`.
    state_pos: usize,
    pub start_key_group: i32,
    pub offsets: Vec<i64>,
    pub src: Source,
    /// The file, or `subtask N (inline)`, for messages.
    pub what: String,
    pub header_len: u64,
    /// The resolved header, or why it could not be read.
    pub header: Result<SubtaskHeader, String>,
}

/// A subtask whose keyed stream could not be opened.
#[derive(Debug)]
pub(crate) struct FailedSubtask {
    pub subtask_index: i32,
    pub start_key_group: i32,
    pub num_key_groups: i32,
    pub error: String,
}

/// An operator's keyed streams, each opened once.
#[derive(Debug)]
pub(crate) struct OpContext {
    pub op_index: usize,
    /// In subtask order.
    pub streams: Vec<SubtaskStream>,
    /// In subtask order.
    pub failed: Vec<FailedSubtask>,
    /// Streams and failures by ascending first key-group: the order keys are visited in.
    units: Vec<Unit>,
}

#[derive(Debug, Clone, Copy)]
enum Unit {
    Stream(usize),
    Failed(usize),
}

/// Unwrap the delegate of a keyed handle, which may itself nest a key-groups handle.
fn effective_delegate(kg: &KeyGroupsStateHandle) -> &StreamStateHandle {
    match &kg.delegate_handle {
        StreamStateHandle::KeyGroups(nested) => &nested.delegate_handle,
        other => other,
    }
}

fn keyed_handle(op: &Operator, state_pos: usize) -> Option<&KeyGroupsStateHandle> {
    match &op.subtask_states.get(state_pos)?.managed_keyed_state {
        StateHandle::KeyGroups(kg) => Some(kg),
        _ => None,
    }
}

/// Byte length of the header: it runs from offset 0 to the first key-group.
fn header_len(offsets: &[i64], stream_len: u64) -> u64 {
    offsets
        .iter()
        .copied()
        .filter(|&o| o > 0)
        .min()
        .map(|o| o as u64)
        .unwrap_or(stream_len)
        .min(stream_len)
}

impl OpContext {
    /// Open every keyed stream of operator `op_index`, in subtask order, and resolve each header.
    /// A stream that cannot be opened becomes a [`FailedSubtask`] whose message names its file: a
    /// caller must never take a broken file for a subtask without state.
    pub(crate) fn open(savepoint: &Savepoint, op_index: usize, savepoint_dir: &Path) -> OpContext {
        let op = &savepoint.operators[op_index];
        let mut streams = Vec::new();
        let mut failed = Vec::new();
        for (state_pos, st) in op.subtask_states.iter().enumerate() {
            let StateHandle::KeyGroups(kg) = &st.managed_keyed_state else {
                continue;
            };
            let fail = |error: String| FailedSubtask {
                subtask_index: st.subtask_index,
                start_key_group: kg.start_key_group,
                num_key_groups: kg.num_key_groups,
                error,
            };
            let delegate = effective_delegate(kg);
            let mut src = match source_for(delegate, savepoint_dir) {
                Ok(Some(src)) => src,
                Ok(None) => continue,
                Err(e) => {
                    failed.push(fail(e));
                    continue;
                }
            };
            let what = match src.path() {
                Some(path) => path.display().to_string(),
                None => format!("subtask {} (inline)", st.subtask_index),
            };
            if let Err(e) =
                crate::state_file::check_offsets(&what, &kg.offsets, kg.start_key_group, src.len())
            {
                failed.push(fail(e));
                continue;
            }
            let hlen = header_len(&kg.offsets, src.len());
            let mut buf = Vec::new();
            let header = src
                .read(inline_bytes(delegate), 0, hlen, &mut buf)
                .map(SubtaskHeader::resolve);
            // Hold no file between operator open and the first block read.
            src.close();
            streams.push(SubtaskStream {
                subtask_index: st.subtask_index,
                state_pos,
                start_key_group: kg.start_key_group,
                offsets: kg.offsets.clone(),
                src,
                what,
                header_len: hlen,
                header,
            });
        }
        let mut units: Vec<(i32, Unit)> = streams
            .iter()
            .enumerate()
            .map(|(i, s)| (s.start_key_group, Unit::Stream(i)))
            .chain(
                failed
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.start_key_group, Unit::Failed(i))),
            )
            .collect();
        units.sort_by_key(|(start, _)| *start);
        OpContext {
            op_index,
            streams,
            failed,
            units: units.into_iter().map(|(_, u)| u).collect(),
        }
    }

    /// The open errors, in subtask order.
    pub(crate) fn open_errors(&self) -> Vec<String> {
        self.failed.iter().map(|f| f.error.clone()).collect()
    }

    fn unit_range(&self, unit: Unit) -> (i32, i32) {
        match unit {
            Unit::Stream(i) => {
                let s = &self.streams[i];
                (
                    s.start_key_group,
                    s.start_key_group + s.offsets.len() as i32 - 1,
                )
            }
            Unit::Failed(i) => {
                let f = &self.failed[i];
                (f.start_key_group, f.start_key_group + f.num_key_groups - 1)
            }
        }
    }

    fn unit_subtask(&self, unit: Unit) -> i32 {
        match unit {
            Unit::Stream(i) => self.streams[i].subtask_index,
            Unit::Failed(i) => self.failed[i].subtask_index,
        }
    }

    /// The unit whose key-group range holds `key_group`.
    fn unit_of(&self, key_group: i32) -> Option<Unit> {
        self.units.iter().copied().find(|&u| {
            let (s, e) = self.unit_range(u);
            s <= key_group && key_group <= e
        })
    }

    /// The first non-empty key-group at or after `from` (and the ordinal to start at in it).
    fn seek(&self, from: i32, ordinal: u32) -> Position {
        for &unit in &self.units {
            let (start, end) = self.unit_range(unit);
            if end < from {
                continue;
            }
            match unit {
                // Whether it holds keys is unknown: it must be reported, not skipped.
                Unit::Failed(i) => return Position::Failed(i),
                Unit::Stream(i) => {
                    let s = &self.streams[i];
                    for kg in start.max(from)..=end {
                        if s.block_range((kg - start) as usize).is_some() {
                            let ordinal = if kg == from { ordinal } else { 0 };
                            return Position::Block(i, kg, ordinal);
                        }
                    }
                }
            }
        }
        Position::End
    }

    /// The key type, from the first readable header.
    fn key_type(&self) -> Option<&str> {
        self.streams
            .iter()
            .find_map(|s| s.header.as_ref().ok())
            .map(|h| h.key_type.as_str())
    }
}

enum Position {
    /// Stream index, absolute key-group, ordinal.
    Block(usize, i32, u32),
    Failed(usize),
    End,
}

impl SubtaskStream {
    /// `[start, end)` of the `i`-th key-group of this stream (relative to its first), or `None`
    /// when it is empty: Flink records an offset only for a key-group holding entries
    /// (`FullSnapshotAsyncWriter.writeKVStateData`, `setKeyGroupOffset` at :147 and :190), so 0
    /// means empty. The block ends where the next non-empty key-group starts.
    pub(crate) fn block_range(&self, i: usize) -> Option<(u64, u64)> {
        let stream_len = self.src.len();
        let start = *self.offsets.get(i)?;
        if start <= 0 || (start as u64) < self.header_len || (start as u64) >= stream_len {
            return None;
        }
        let end = self.offsets[(i + 1)..]
            .iter()
            .copied()
            .find(|&o| o > start)
            .map(|o| o as u64)
            .unwrap_or(stream_len)
            .min(stream_len);
        (end > start as u64).then_some((start as u64, end))
    }

    /// The stored bytes of `[start, end)`: borrowed from `_metadata` for an inline stream, read
    /// into `buf` otherwise.
    pub(crate) fn read_block<'s>(
        &'s self,
        savepoint: &'s Savepoint,
        op_index: usize,
        (start, end): (u64, u64),
        buf: &'s mut Vec<u8>,
    ) -> Result<&'s [u8], String> {
        let inline = keyed_handle(&savepoint.operators[op_index], self.state_pos)
            .map(|kg| inline_bytes(effective_delegate(kg)))
            .unwrap_or(&[]);
        self.src.read(inline, start, end, buf)
    }
}

// --- block cache --------------------------------------------------------------------------------

type BlockKey = (usize, usize, i32);

/// Inflated blocks, least recently used evicted first, bounded in bytes.
struct BlockCache {
    capacity: usize,
    used: usize,
    tick: u64,
    blocks: HashMap<BlockKey, (Arc<[u8]>, u64)>,
}

impl BlockCache {
    fn get(&mut self, key: &BlockKey) -> Option<Arc<[u8]>> {
        self.tick += 1;
        let tick = self.tick;
        self.blocks.get_mut(key).map(|(b, t)| {
            *t = tick;
            b.clone()
        })
    }

    fn insert(&mut self, key: BlockKey, block: Arc<[u8]>) {
        if block.len() > self.capacity {
            return;
        }
        while self.used + block.len() > self.capacity {
            let Some(oldest) = self
                .blocks
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| *k)
            else {
                break;
            };
            if let Some((b, _)) = self.blocks.remove(&oldest) {
                self.used -= b.len();
            }
        }
        self.tick += 1;
        self.used += block.len();
        if let Some((old, _)) = self.blocks.insert(key, (block, self.tick)) {
            self.used -= old.len();
        }
    }
}

/// A block's inflated bytes.
enum Block<'s> {
    Borrowed(&'s [u8]),
    Shared(Arc<[u8]>),
}

impl std::ops::Deref for Block<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Block::Borrowed(b) => b,
            Block::Shared(b) => b,
        }
    }
}

/// A key's raw entries in one block, before decoding.
struct RawKey {
    key: String,
    entries: Vec<KeyedEntry>,
}

/// Parse a whole block and group its entries by key, in order of first appearance.
fn group_block(
    block: &[u8],
    key_group: u16,
    header: &SubtaskHeader,
    kg_prefix: usize,
) -> (Vec<RawKey>, Option<BlockDefect>) {
    let mut keys: Vec<RawKey> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let parsed = parse_key_group_block(
        block,
        key_group,
        &header.parse_ctx(kg_prefix, None),
        &mut |entry| match index.get(&entry.key) {
            Some(&i) => keys[i].entries.push(entry),
            None => {
                index.insert(entry.key.clone(), keys.len());
                keys.push(RawKey {
                    key: entry.key.clone(),
                    entries: vec![entry],
                });
            }
        },
    );
    (keys, parsed.err())
}

// --- the reader ---------------------------------------------------------------------------------

/// Random access to a savepoint's keyed state. See the [module documentation](self).
///
/// `Sync`: operators are opened once behind a [`OnceLock`], files are read positionally, and the
/// block cache sits behind a mutex, so one reader can serve concurrent requests.
pub struct StateReader<'a> {
    savepoint: Cow<'a, Savepoint>,
    dir: PathBuf,
    proto: Option<Arc<ProtoContext>>,
    contexts: Vec<OnceLock<OpContext>>,
    cache: Mutex<BlockCache>,
}

impl StateReader<'static> {
    /// Parse `<dir>/_metadata` and read from `dir`.
    pub fn open(dir: &Path) -> Result<StateReader<'static>, String> {
        let path = dir.join("_metadata");
        let savepoint = crate::parser::parse_metadata(&path)
            .map_err(|e| format!("{}: {}", path.display(), e))?;
        Ok(Self::build(Cow::Owned(savepoint), dir))
    }

    /// Read from a savepoint already parsed, taking ownership of it.
    pub fn from_savepoint(savepoint: Savepoint, dir: &Path) -> StateReader<'static> {
        Self::build(Cow::Owned(savepoint), dir)
    }
}

impl<'a> StateReader<'a> {
    /// Read from a borrowed, already parsed savepoint.
    pub fn new(savepoint: &'a Savepoint, dir: &Path) -> StateReader<'a> {
        Self::build(Cow::Borrowed(savepoint), dir)
    }

    fn build(savepoint: Cow<'a, Savepoint>, dir: &Path) -> StateReader<'a> {
        let contexts = (0..savepoint.operators.len())
            .map(|_| OnceLock::new())
            .collect();
        StateReader {
            savepoint,
            dir: dir.to_path_buf(),
            proto: None,
            contexts,
            cache: Mutex::new(BlockCache {
                capacity: DEFAULT_CACHE_BYTES,
                used: 0,
                tick: 0,
                blocks: HashMap::new(),
            }),
        }
    }

    /// Decode protobuf-in-POJO values with this context.
    pub fn with_proto(mut self, proto: Arc<ProtoContext>) -> Self {
        self.proto = Some(proto);
        self
    }

    /// Bound the block cache to `bytes` (0 disables it).
    pub fn with_cache_bytes(self, bytes: usize) -> Self {
        if let Ok(mut c) = self.cache.lock() {
            c.capacity = bytes;
        }
        self
    }

    pub fn savepoint(&self) -> &Savepoint {
        &self.savepoint
    }

    pub fn savepoint_dir(&self) -> &Path {
        &self.dir
    }

    /// Every operator, from `_metadata` alone.
    pub fn operators(&self) -> Vec<OperatorInfo> {
        self.savepoint
            .operators
            .iter()
            .map(|op| OperatorInfo {
                id: hex_id(&op.id),
                name: if !op.name.is_empty() {
                    op.name.clone()
                } else {
                    op.identifier_name.clone()
                },
                parallelism: op.parallelism,
                max_parallelism: op.max_parallelism,
                keyed_subtasks: op
                    .subtask_states
                    .iter()
                    .filter(|s| matches!(s.managed_keyed_state, StateHandle::KeyGroups(_)))
                    .count(),
            })
            .collect()
    }

    /// The index of the operator whose hex id is `op`, or starts with it (unambiguously).
    pub fn operator_index(&self, op: &str) -> Result<usize, String> {
        let ids: Vec<String> = self
            .savepoint
            .operators
            .iter()
            .map(|o| hex_id(&o.id))
            .collect();
        if let Some(i) = ids.iter().position(|id| id == op) {
            return Ok(i);
        }
        let matching: Vec<usize> = (0..ids.len()).filter(|&i| ids[i].starts_with(op)).collect();
        match matching.as_slice() {
            [i] => Ok(*i),
            [] => Err(format!("no operator matches {:?}", op)),
            many => Err(format!(
                "operator prefix {:?} is ambiguous: {}",
                op,
                many.iter()
                    .map(|&i| ids[i].as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    /// The operator's keyed streams, opened on first use.
    fn context(&self, op_index: usize) -> &OpContext {
        self.contexts[op_index]
            .get_or_init(|| OpContext::open(&self.savepoint, op_index, &self.dir))
    }

    /// The keyed states of `op`, from its subtasks' headers.
    pub fn states(&self, op: &str) -> Result<KeyedStates, String> {
        let ctx = self.context(self.operator_index(op)?);
        let mut out = KeyedStates {
            key_type: ctx.key_type().unwrap_or_default().to_string(),
            errors: ctx.open_errors(),
            ..Default::default()
        };
        for s in &ctx.streams {
            let h = match &s.header {
                Ok(h) => h,
                Err(e) => {
                    out.errors.push(e.clone());
                    continue;
                }
            };
            for st in &h.states {
                match out.states.iter_mut().find(|i| i.name == st.name) {
                    Some(info) => info.subtasks += 1,
                    None => out.states.push(StateInfo {
                        name: st.name.clone(),
                        state_type: st.state_type.clone(),
                        value_type: st.value_type.clone(),
                        value_class: st.value_class.clone(),
                        subtasks: 1,
                    }),
                }
            }
        }
        Ok(out)
    }

    /// A cursor must name the subtask that holds its key-group.
    fn check_cursor(&self, ctx: &OpContext, cursor: &Cursor) -> Result<(), String> {
        match ctx.unit_of(cursor.key_group as i32) {
            Some(u) if ctx.unit_subtask(u) != cursor.subtask => Err(format!(
                "cursor {} does not fit operator {}: key-group {} is held by subtask {}",
                cursor,
                hex_id(&self.savepoint.operators[ctx.op_index].id),
                cursor.key_group,
                ctx.unit_subtask(u)
            )),
            _ => Ok(()),
        }
    }

    /// Load the inflated block of absolute key-group `kg` of stream `si`.
    fn load_block(
        &self,
        ctx: &OpContext,
        si: usize,
        kg: i32,
        keep: bool,
        stats: &mut ReadStats,
    ) -> Result<Option<Block<'_>>, String> {
        let s = &ctx.streams[si];
        let Some(range) = s.block_range((kg - s.start_key_group) as usize) else {
            return Ok(None);
        };
        let header = s.header.as_ref().map_err(String::clone)?;
        let stored = range.1 - range.0;
        stats.blocks += 1;
        stats.block_bytes += stored;
        let key = (ctx.op_index, s.state_pos, kg);
        if let Some(b) = self.cache.lock().ok().and_then(|mut c| c.get(&key)) {
            stats.cache_hits += 1;
            stats.max_block_bytes = stats.max_block_bytes.max(b.len());
            return Ok(Some(Block::Shared(b)));
        }
        stats.bytes_read += stored;
        if !header.compressed {
            if let Source::Inline { .. } = s.src {
                // Borrowed from `_metadata`: nothing to copy, nothing to cache.
                let inline = keyed_handle(&self.savepoint.operators[ctx.op_index], s.state_pos)
                    .map(|h| inline_bytes(effective_delegate(h)))
                    .unwrap_or(&[]);
                let block = inline
                    .get(range.0 as usize..range.1 as usize)
                    .ok_or_else(|| format!("{}: key-group {} is out of bounds", s.what, kg))?;
                stats.max_block_bytes = stats.max_block_bytes.max(block.len());
                return Ok(Some(Block::Borrowed(block)));
            }
        }
        let mut raw = Vec::new();
        let bytes = s.read_block(&self.savepoint, ctx.op_index, range, &mut raw)?;
        let block = if header.compressed {
            let mut inflated = Vec::new();
            key_group_block(bytes, true, &mut inflated)
                .map_err(|e| format!("{}: key-group {}: snappy decode: {}", s.what, kg, e))?;
            Block::Shared(inflated.into())
        } else {
            Block::Shared(raw.into())
        };
        stats.max_block_bytes = stats.max_block_bytes.max(block.len());
        if keep {
            if let (Block::Shared(b), Ok(mut c)) = (&block, self.cache.lock()) {
                c.insert(key, b.clone());
            }
        }
        Ok(Some(block))
    }

    /// Read and group the block of `kg` in stream `si`.
    fn block_keys(
        &self,
        ctx: &OpContext,
        si: usize,
        kg: i32,
        keep: bool,
        stats: &mut ReadStats,
        malformed: &mut Vec<String>,
    ) -> Result<Vec<RawKey>, String> {
        let Some(block) = self.load_block(ctx, si, kg, keep, stats)? else {
            return Ok(Vec::new());
        };
        let s = &ctx.streams[si];
        let header = s.header.as_ref().map_err(String::clone)?;
        let max_p = self.savepoint.operators[ctx.op_index].max_parallelism;
        let (keys, defect) = group_block(&block, kg as u16, header, kg_prefix_len(max_p));
        if let Some(d) = defect {
            malformed.push(format!(
                "subtask {}, key-group {}: {}",
                s.subtask_index, kg, d
            ));
        }
        Ok(keys)
    }

    /// Decode a key's entries.
    fn record(&self, s: &SubtaskStream, kg: i32, ordinal: u32, raw: RawKey) -> KeyRecord {
        let header = s.header.as_ref().ok();
        let entries = raw
            .entries
            .into_iter()
            .enumerate()
            .map(|(i, e)| {
                let state = header.and_then(|h| h.states.get(e.state_id as usize));
                match state {
                    Some(st) => {
                        let (map_key, value) = decode_entry_value(
                            st,
                            &e.namespace,
                            &e.value,
                            self.proto.as_deref(),
                            i,
                        );
                        StateEntry {
                            state: st.name.clone(),
                            state_type: st.state_type.clone(),
                            value_type: st.value_type.clone(),
                            map_key,
                            namespace: e.namespace,
                            raw: e.value,
                            value,
                        }
                    }
                    // A stateId the header counts but whose metadata was not recognised.
                    None => StateEntry {
                        state: format!("#{}", e.state_id),
                        state_type: "?".to_string(),
                        value_type: "?".to_string(),
                        map_key: None,
                        namespace: e.namespace,
                        raw: e.value,
                        value: crate::parser::value::undecodable("?"),
                    },
                }
            })
            .collect();
        KeyRecord {
            key: raw.key,
            key_group: kg as u16,
            subtask: s.subtask_index,
            position: Cursor {
                subtask: s.subtask_index,
                key_group: kg as u16,
                ordinal,
            },
            entries,
        }
    }

    /// Up to `limit` keys from `cursor` on (see the [module documentation](self) for the order),
    /// each with all its entries, and the cursor of the next page.
    ///
    /// A subtask whose file could not be read is an `Err` naming it once the page reaches it (a
    /// page already holding keys stops before it instead, its `next` pointing there).
    pub fn key_page(&self, op: &str, cursor: &Cursor, limit: usize) -> Result<KeyPage, String> {
        let ctx = self.context(self.operator_index(op)?);
        self.check_cursor(ctx, cursor)?;
        let mut page = KeyPage {
            keys: Vec::new(),
            next: None,
            stats: ReadStats::default(),
            malformed: Vec::new(),
        };
        let mut pos = ctx.seek(cursor.key_group as i32, cursor.ordinal);
        loop {
            match pos {
                Position::End => return Ok(page),
                Position::Failed(i) => {
                    let f = &ctx.failed[i];
                    if page.keys.is_empty() {
                        return Err(f.error.clone());
                    }
                    page.next = Some(Cursor {
                        subtask: f.subtask_index,
                        key_group: f.start_key_group as u16,
                        ordinal: 0,
                    });
                    return Ok(page);
                }
                Position::Block(si, kg, ordinal) => {
                    let s = &ctx.streams[si];
                    if page.keys.len() >= limit {
                        page.next = Some(Cursor {
                            subtask: s.subtask_index,
                            key_group: kg as u16,
                            ordinal,
                        });
                        return Ok(page);
                    }
                    let keys =
                        self.block_keys(ctx, si, kg, true, &mut page.stats, &mut page.malformed)?;
                    for (ord, raw) in keys.into_iter().enumerate().skip(ordinal as usize) {
                        if page.keys.len() >= limit {
                            page.next = Some(Cursor {
                                subtask: s.subtask_index,
                                key_group: kg as u16,
                                ordinal: ord as u32,
                            });
                            return Ok(page);
                        }
                        page.keys.push(self.record(s, kg, ord as u32, raw));
                    }
                    pos = ctx.seek(kg + 1, 0);
                }
            }
        }
    }

    /// Look one key up by its rendered text: compute its key-group with Flink's assignment and
    /// read that block alone.
    ///
    /// Works for `String`, `Long`, `Int`, `Short`, `Byte`, `Char`, `Boolean`, `Float`, `Double`,
    /// `BigInteger` and `Tuple` keys of those ([`JavaKey::parse`] says which texts are accepted);
    /// other key types answer [`Lookup::NeedsScan`]. An empty key-group answers
    /// [`Lookup::Absent`] without any I/O. `Err` when the owning subtask's file could not be read,
    /// or when its block is malformed before the key was found.
    pub fn get(&self, op: &str, key_text: &str) -> Result<Lookup, String> {
        let op_index = self.operator_index(op)?;
        let ctx = self.context(op_index);
        let operator = &self.savepoint.operators[op_index];
        let Some(key_type) = ctx.key_type() else {
            return match ctx.failed.first() {
                Some(f) => Err(f.error.clone()),
                None => match ctx.streams.iter().find_map(|s| s.header.as_ref().err()) {
                    Some(e) => Err(e.clone()),
                    None => Err(format!(
                        "operator {} has no keyed state",
                        hex_id(&operator.id)
                    )),
                },
            };
        };
        let key = match JavaKey::parse(key_type, key_text) {
            Ok(k) => k,
            Err(reason) => return Ok(Lookup::NeedsScan { reason }),
        };
        let Some(canonical) = key.canonical_text(key_type) else {
            return Ok(Lookup::NeedsScan {
                reason: format!("{:?} does not render as a {} key", key_text, key_type),
            });
        };
        let kg = key_group_for_hash(key.hash_code(), operator.max_parallelism);
        let mut stats = ReadStats::default();
        let absent = |subtask, stats| Lookup::Absent {
            key_group: kg as u16,
            subtask,
            stats,
        };
        let si = match ctx.unit_of(kg) {
            None => {
                return Ok(absent(
                    subtask_for_key_group(operator.max_parallelism, operator.parallelism, kg),
                    stats,
                ))
            }
            Some(Unit::Failed(i)) => return Err(ctx.failed[i].error.clone()),
            Some(Unit::Stream(i)) => i,
        };
        let s = &ctx.streams[si];
        let mut malformed = Vec::new();
        let keys = self.block_keys(ctx, si, kg, true, &mut stats, &mut malformed)?;
        match keys
            .into_iter()
            .enumerate()
            .find(|(_, k)| k.key == canonical)
        {
            Some((ord, raw)) => Ok(Lookup::Found {
                record: self.record(s, kg, ord as u32, raw),
                stats,
            }),
            None if !malformed.is_empty() => Err(format!(
                "{}: {}; key {:?} may be in the part that could not be read",
                s.what, malformed[0], key_text
            )),
            None => Ok(absent(s.subtask_index, stats)),
        }
    }

    /// Test `matches` on every key's rendered text from `cursor` on, reading blocks until their
    /// stored size reaches `byte_budget` (at least one block per call, so a scan always moves).
    /// Returns the matching keys with all their entries, and where to resume.
    pub fn scan(
        &self,
        op: &str,
        cursor: &Cursor,
        byte_budget: u64,
        matches: &mut dyn FnMut(&str) -> bool,
    ) -> Result<ScanPage, String> {
        let ctx = self.context(self.operator_index(op)?);
        self.check_cursor(ctx, cursor)?;
        let mut page = ScanPage {
            matches: Vec::new(),
            next: None,
            stats: ReadStats::default(),
            malformed: Vec::new(),
        };
        let mut pos = ctx.seek(cursor.key_group as i32, cursor.ordinal);
        let mut visited = 0usize;
        loop {
            match pos {
                Position::End => return Ok(page),
                Position::Failed(i) => {
                    let f = &ctx.failed[i];
                    if visited == 0 {
                        return Err(f.error.clone());
                    }
                    page.next = Some(Cursor {
                        subtask: f.subtask_index,
                        key_group: f.start_key_group as u16,
                        ordinal: 0,
                    });
                    return Ok(page);
                }
                Position::Block(si, kg, ordinal) => {
                    let s = &ctx.streams[si];
                    if visited > 0 && page.stats.block_bytes >= byte_budget {
                        page.next = Some(Cursor {
                            subtask: s.subtask_index,
                            key_group: kg as u16,
                            ordinal,
                        });
                        return Ok(page);
                    }
                    // A scan walks past blocks once: it does not fill the cache.
                    let keys =
                        self.block_keys(ctx, si, kg, false, &mut page.stats, &mut page.malformed)?;
                    for (ord, raw) in keys.into_iter().enumerate().skip(ordinal as usize) {
                        if matches(&raw.key) {
                            page.matches.push(self.record(s, kg, ord as u32, raw));
                        }
                    }
                    visited += 1;
                    pos = ctx.seek(kg + 1, 0);
                }
            }
        }
    }
}
