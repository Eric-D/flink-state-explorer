//! Lazy, random access to a canonical savepoint's keyed state, one key-group at a time.
//!
//! A [`StateReader`] parses `_metadata` once. The first time an operator is used, each of its
//! subtasks' keyed streams is opened once ([`source`]) and its header — and only its header, which
//! ends where the first key-group begins — is resolved: state names, types, value serializers,
//! POJO layouts. State ids are resolved **per subtask**, since Flink registers states lazily and
//! two subtasks may number them differently. After that every operation reads whole key-group
//! blocks, located by `KeyGroupsStateHandle.offsets`, and nothing else:
//!
//! - [`StateReader::key_page`] pages through the keys, each with its states' values (at most
//!   [`StateReader::with_entry_limit`] entries per state, the rest through
//!   [`StateReader::entries`]);
//! - [`StateReader::get`] finds one key by computing Flink's key-group for it
//!   ([`crate::parser::key_group`]) and reading that block alone — an empty key-group answers
//!   without any I/O;
//! - [`StateReader::scan`] tests a predicate on rendered keys under a byte budget, for search.
//!
//! Memory is bounded by the blocks in flight plus a small byte-bounded cache of recently read
//! blocks, parsed and grouped by key (so paging inside one key-group neither re-reads nor
//! re-parses it), never by the savepoint's size. Values are decoded through
//! [`crate::parser::value`] only — the single decoder — and MAP entries through
//! [`crate::parser::value::decode_map_entry`].
//!
//! Nothing that cannot be read is ever taken for "no state": a subtask whose file cannot be read,
//! whose header does not parse or whose keyed handle is not a canonical one (incremental,
//! changelog) is reported — as [`Unreadable`] in a page, which then moves on past it, or as a
//! [`ReaderError::Unreadable`] from [`StateReader::get`] for a key it owns.
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
    count_states_in_header, extract_entry_layouts, extract_key_tuple_classes,
    extract_state_metadata, extract_value_pojo_classes, extract_value_serializer_types,
    key_group_block, parse_key_group_block, read_keyed_stream_header, BlockDefect, EntryLayout,
    KeyGroupParse, KeyedEntry,
};
use crate::parser::metadata::{Operator, Savepoint};
use crate::parser::pojo::{extract_pojo_infos, PojoInfo};
use crate::parser::state_handle::{KeyGroupsStateHandle, StateHandle, StreamStateHandle};
use crate::proto::ProtoContext;

pub(crate) use source::read_whole_stream;
use source::{inline_bytes, source_for, FilePool, Source};

/// Default size of the block cache, in bytes of parsed blocks: a few key-groups of a large
/// savepoint (~1.6 MB each on a 211 MB / 128 key-group one, a little more once parsed).
pub const DEFAULT_CACHE_BYTES: usize = 8 << 20;

/// Default most entries of one state returned with one key ([`StateReader::with_entry_limit`]).
pub const DEFAULT_ENTRY_LIMIT: usize = 1000;

/// Default most matches one [`StateReader::scan`] call returns
/// ([`StateReader::with_scan_match_limit`]).
pub const DEFAULT_SCAN_MATCH_LIMIT: usize = 1000;

/// Default most state files a reader holds open at once ([`StateReader::with_open_files`]).
pub const DEFAULT_OPEN_FILES: usize = 64;

/// Lowercase hex of an OperatorID, as `_metadata` stores it.
pub fn hex_id(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{:02x}", b)).collect()
}

// --- errors -------------------------------------------------------------------------------------

/// Why a [`StateReader`] call failed. `Display` gives the message; the variant says whose fault it
/// is (the comments suggest the HTTP status a server would answer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReaderError {
    /// `_metadata` could not be read or parsed (500).
    Metadata { path: String, error: String },
    /// No operator id is, or starts with, `op` (404).
    UnknownOperator { op: String },
    /// `op` is a prefix of several operator ids (400).
    AmbiguousOperator { op: String, candidates: Vec<String> },
    /// The operator has no keyed state (404).
    NoKeyedState { op: String },
    /// A cursor that does not fit the operator (400).
    BadCursor { cursor: String, reason: String },
    /// Nothing at that position, or no such state (404).
    NotFound { what: String },
    /// A state stream that cannot be read: missing, short or unreadable file, header that does
    /// not parse, unsupported keyed handle, block that does not inflate (500). `what` is the file,
    /// or `subtask N (inline)`; `error` names it too.
    Unreadable {
        subtask: i32,
        first_key_group: u16,
        last_key_group: u16,
        what: String,
        error: String,
    },
    /// [`StateReader::get`]: the key's block is malformed before the key was found, so the key
    /// may be in the part that could not be read (500).
    Malformed {
        subtask: i32,
        key_group: u16,
        what: String,
        defect: String,
        key: String,
    },
}

impl std::fmt::Display for ReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReaderError::Metadata { path, error } => write!(f, "{}: {}", path, error),
            ReaderError::UnknownOperator { op } => write!(f, "no operator matches {:?}", op),
            ReaderError::AmbiguousOperator { op, candidates } => write!(
                f,
                "operator prefix {:?} is ambiguous: {}",
                op,
                candidates.join(", ")
            ),
            ReaderError::NoKeyedState { op } => write!(f, "operator {} has no keyed state", op),
            ReaderError::BadCursor { reason, .. } => write!(f, "{}", reason),
            ReaderError::NotFound { what } => write!(f, "{}", what),
            ReaderError::Unreadable { error, .. } => write!(f, "{}", error),
            ReaderError::Malformed {
                what, defect, key, ..
            } => write!(
                f,
                "{}: {}; key {:?} may be in the part that could not be read",
                what, defect, key
            ),
        }
    }
}

impl std::error::Error for ReaderError {}

/// So that `?` keeps working in code returning `Result<_, String>`.
impl From<ReaderError> for String {
    fn from(e: ReaderError) -> String {
        e.to_string()
    }
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
    /// Subtasks holding a keyed-state handle (of any kind, readable or not); 0 = no keyed state.
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
    /// One message per state file that could not be opened, whose header could not be read or
    /// whose keyed handle is not supported, naming it. Non-empty means `states` may be incomplete.
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
///
/// The labels are shared (`Arc<str>`), not copied per entry.
#[derive(Debug, Clone, PartialEq)]
pub struct StateEntry {
    pub state: Arc<str>,
    pub state_type: Arc<str>,
    pub value_type: Arc<str>,
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

/// A state of a key that holds more entries than a record carries: `returned` of `total` are in
/// [`KeyRecord::entries`] (the first ones, in on-disk order), the others through
/// [`StateReader::entries`] from offset `returned`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncated {
    pub state: Arc<str>,
    pub total: usize,
    pub returned: usize,
}

/// A key with its entries in its key-group, across all states.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRecord {
    /// The key as [`crate::parser::value::decode_key`] renders it.
    pub key: String,
    /// Absolute key-group.
    pub key_group: u16,
    pub subtask: i32,
    /// Where this key is: a [`StateReader::key_page`] from here starts with it, and
    /// [`StateReader::entries`] takes it.
    pub position: Cursor,
    /// In on-disk order; at most the reader's entry limit per state.
    pub entries: Vec<StateEntry>,
    /// The states cut at the entry limit; empty = `entries` is complete.
    pub truncated: Vec<Truncated>,
}

impl KeyRecord {
    /// Whether some state's entries were cut at the entry limit.
    pub fn is_truncated(&self) -> bool {
        !self.truncated.is_empty()
    }
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

/// Key-groups a page could not read at all, reported in the page rather than failing it: a
/// subtask whose file is missing or unreadable, whose header does not parse or whose keyed handle
/// is unsupported (the whole range from where the page reached it), or one block that does not
/// inflate or cannot be read. The page goes on past them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreadable {
    pub subtask: i32,
    /// Inclusive absolute key-group range.
    pub first_key_group: u16,
    pub last_key_group: u16,
    /// The file, or `subtask N (inline)`.
    pub what: String,
    /// The message, naming the file.
    pub error: String,
}

impl Unreadable {
    fn into_error(self) -> ReaderError {
        ReaderError::Unreadable {
            subtask: self.subtask,
            first_key_group: self.first_key_group,
            last_key_group: self.last_key_group,
            what: self.what,
            error: self.error,
        }
    }
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
    /// Key-groups that could not be read at all, which the page skipped (each reported by the
    /// one page that reaches it).
    pub unreadable: Vec<Unreadable>,
}

/// The result of a [`StateReader::scan`].
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPage {
    /// At most the reader's scan match limit.
    pub matches: Vec<KeyRecord>,
    /// Where to resume; `None` when the whole operator has been scanned.
    pub next: Option<Cursor>,
    pub stats: ReadStats,
    pub malformed: Vec<String>,
    pub unreadable: Vec<Unreadable>,
}

/// How far one [`StateReader::scan_with`] may go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanOptions {
    /// Stop once the stored size of the blocks visited reaches this (at least one block is
    /// always visited, so a scan always moves).
    pub byte_budget: u64,
    /// Stop at this many matches (mid-block: `next` is then the following key); 0 is taken as 1.
    pub max_matches: usize,
    /// Do not go past this absolute key-group (inclusive); `None` = to the end.
    pub last_key_group: Option<u16>,
}

/// Where a scan or count is, in the operator's key-groups and stored bytes, as `_metadata`'s
/// offsets give them (no I/O): what is done is every key-group before the cursor's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    pub key_groups_done: u64,
    pub key_groups_total: u64,
    /// Stored size of the non-empty key-groups done / in the range.
    pub bytes_done: u64,
    pub bytes_total: u64,
}

impl Progress {
    /// Done, in percent of the bytes (of the key-groups when the range holds no bytes).
    pub fn percent(&self) -> f64 {
        let (done, total) = if self.bytes_total > 0 {
            (self.bytes_done, self.bytes_total)
        } else {
            (self.key_groups_done, self.key_groups_total)
        };
        if total == 0 {
            100.0
        } else {
            done as f64 * 100.0 / total as f64
        }
    }
}

/// Exact counts of one keyed state over part of an operator, from [`StateReader::count`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCount {
    pub state: Arc<str>,
    /// Keys holding at least one entry of the state.
    pub keys: u64,
    /// Entries stored: one per value, per MAP entry, per timer — what `--export-state` writes
    /// one row for.
    pub entries: u64,
}

/// What one [`StateReader::count`] call counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountPage {
    /// Per state, in order of first appearance.
    pub states: Vec<StateCount>,
    /// Distinct keys (a key is in one key-group, so counts of disjoint pages add up).
    pub keys: u64,
    /// The next key-group to count; `None` when the whole operator has been counted.
    pub next: Option<Cursor>,
    pub stats: ReadStats,
    pub malformed: Vec<String>,
    pub unreadable: Vec<Unreadable>,
}

/// The answer of [`StateReader::get`].
#[derive(Debug, Clone, PartialEq)]
pub enum Lookup {
    Found {
        record: KeyRecord,
        stats: ReadStats,
        /// Non-empty when the key's block is malformed: `record` holds the entries read before
        /// the defect, and the key may have more in the part that could not be read.
        malformed: Vec<String>,
    },
    /// The key is not in the savepoint: its key-group is empty (no I/O at all), or its block does
    /// not hold it.
    Absent {
        key_group: u16,
        subtask: i32,
        stats: ReadStats,
    },
    /// The key-group cannot be computed from the text (a POJO or opaque key type, a tuple class
    /// that is not Flink's own, or text that does not parse as the key type): use
    /// [`StateReader::scan`].
    NeedsScan { reason: String },
}

/// Entries of one state of one key, from [`StateReader::entries`].
#[derive(Debug, Clone, PartialEq)]
pub struct EntryPage {
    pub key: String,
    pub state: Arc<str>,
    /// From `offset`, in on-disk order.
    pub entries: Vec<StateEntry>,
    pub offset: usize,
    /// How many entries of that state the key has.
    pub total: usize,
    /// The offset of the next page, `None` when this one reaches `total`.
    pub next: Option<usize>,
    pub stats: ReadStats,
    pub malformed: Vec<String>,
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
    /// `name`, `state_type`, `value_type`, shared by every [`StateEntry`] of the state.
    labels: [Arc<str>; 3],
}

/// Everything one subtask's header says, resolved once.
#[derive(Debug, Clone)]
pub(crate) struct SubtaskHeader {
    pub compressed: bool,
    pub key_type: String,
    pub key_pojo: Option<PojoInfo>,
    /// The tuple classes of a `Tuple` key, outermost first.
    pub key_tuple_classes: Vec<String>,
    /// By stateId.
    pub states: Vec<ResolvedState>,
    pub state_types: Vec<String>,
    /// What follows the key in each state's entries, so keys split from namespaces the same way
    /// on every path.
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
                // Timers carry a synthesized firing timestamp, and must
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
                    labels: [
                        Arc::from(meta.name.as_str()),
                        Arc::from(meta.state_type.as_str()),
                        Arc::from(value_type.as_str()),
                    ],
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
        let key_tuple_classes = if key_type.starts_with("Tuple<") {
            extract_key_tuple_classes(header)
        } else {
            Vec::new()
        };
        SubtaskHeader {
            compressed: read_keyed_stream_header(header)
                .is_some_and(|h| h.using_key_group_compression),
            key_type,
            key_pojo,
            key_tuple_classes,
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

    /// Why a `Tuple` key's `hashCode()` is not known, if it is not: Flink's `TupleN` hash their
    /// fields (`Tuple2.java:173-177`), but a user subclass of one may override `hashCode()`.
    fn tuple_key_unhashable(&self) -> Option<String> {
        if !self.key_type.starts_with("Tuple<") {
            return None;
        }
        let tuples = self.key_type.matches("Tuple<").count();
        if self.key_tuple_classes.len() != tuples {
            return Some(
                "the key's tuple classes could not be read from its serializer snapshot, so its \
                 hashCode() is not known"
                    .to_string(),
            );
        }
        self.key_tuple_classes
            .iter()
            .find(|c| {
                !c.strip_prefix("org.apache.flink.api.java.tuple.Tuple")
                    .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            })
            .map(|c| {
                format!(
                    "the key class {} is not one of Flink's TupleN: it may override hashCode()",
                    c
                )
            })
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

/// A subtask whose keyed stream could not be opened, or whose keyed handle is not supported.
#[derive(Debug)]
pub(crate) struct FailedSubtask {
    pub subtask_index: i32,
    pub start_key_group: i32,
    pub num_key_groups: i32,
    /// The file, `subtask N (inline)`, or `subtask N (<handle kind>)`.
    pub what: String,
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

/// A keyed handle this reader does not read, by kind and key-group range.
fn unsupported_keyed_handle(h: &StateHandle) -> Option<(&'static str, i32, i32)> {
    Some(match h {
        StateHandle::IncrementalRemoteKeyed(x) => (
            "IncrementalRemoteKeyedStateHandle (incremental RocksDB)",
            x.start_key_group,
            x.num_key_groups,
        ),
        StateHandle::Changelog(x) => (
            "ChangelogStateBackendHandle (changelog)",
            x.start_key_group,
            x.num_key_groups,
        ),
        StateHandle::ChangelogByteIncrement(x) => (
            "InMemoryChangelogStateHandle (changelog)",
            x.start_key_group,
            x.num_key_groups,
        ),
        StateHandle::ChangelogFileIncrement(x) => (
            "ChangelogStateHandleStreamImpl (changelog)",
            x.start_key_group,
            x.num_key_groups,
        ),
        StateHandle::Null | StateHandle::KeyGroups(_) | StateHandle::OperatorStream(_) => {
            return None
        }
    })
}

/// Where a delegate's bytes are, for messages.
fn describe_delegate(delegate: &StreamStateHandle, dir: &Path, subtask: i32) -> String {
    match delegate {
        StreamStateHandle::Relative(rel) => dir.join(&rel.relative_path).display().to_string(),
        StreamStateHandle::File(f) => f.file_path.clone(),
        StreamStateHandle::Segment(s) => s.file_path.clone(),
        _ => format!("subtask {} (inline)", subtask),
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
    /// A stream that cannot be opened becomes a [`FailedSubtask`] whose message names its file,
    /// and so does a keyed handle that is not a canonical key-groups one (incremental,
    /// changelog): a caller must never take either for a subtask without state.
    pub(crate) fn open(savepoint: &Savepoint, op_index: usize, savepoint_dir: &Path) -> OpContext {
        let op = &savepoint.operators[op_index];
        let mut streams = Vec::new();
        let mut failed = Vec::new();
        for (state_pos, st) in op.subtask_states.iter().enumerate() {
            let kg = match &st.managed_keyed_state {
                StateHandle::KeyGroups(kg) => kg,
                other => {
                    if let Some((kind, start, num)) = unsupported_keyed_handle(other) {
                        failed.push(FailedSubtask {
                            subtask_index: st.subtask_index,
                            start_key_group: start,
                            num_key_groups: num,
                            what: format!("subtask {} ({})", st.subtask_index, kind),
                            error: format!(
                                "subtask {}: unsupported keyed handle {}: only canonical \
                                 savepoints (key-groups handles) are read",
                                st.subtask_index, kind
                            ),
                        });
                    }
                    continue;
                }
            };
            let delegate = effective_delegate(kg);
            let fail = |error: String| FailedSubtask {
                subtask_index: st.subtask_index,
                start_key_group: kg.start_key_group,
                num_key_groups: kg.num_key_groups,
                what: describe_delegate(delegate, savepoint_dir, st.subtask_index),
                error,
            };
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

    /// Why `unit` cannot be read at all, if it cannot: its stream failed to open, or its header.
    fn unit_error(&self, unit: Unit) -> Option<(&str, &str)> {
        match unit {
            Unit::Failed(i) => Some((&self.failed[i].what, &self.failed[i].error)),
            Unit::Stream(i) => {
                let s = &self.streams[i];
                s.header
                    .as_ref()
                    .err()
                    .map(|e| (s.what.as_str(), e.as_str()))
            }
        }
    }

    /// `unit`, unreadable from key-group `from` to its end.
    fn unreadable(&self, unit: Unit, from: i32) -> Unreadable {
        let (start, end) = self.unit_range(unit);
        let (what, error) = self.unit_error(unit).unwrap_or(("", ""));
        Unreadable {
            subtask: self.unit_subtask(unit),
            first_key_group: start.max(from) as u16,
            last_key_group: end as u16,
            what: what.to_string(),
            error: error.to_string(),
        }
    }

    /// The first non-empty key-group at or after `from` (and the ordinal to start at in it), or
    /// the first unit that cannot be read.
    fn seek(&self, from: i32, ordinal: u32) -> Position {
        for &unit in &self.units {
            let (start, end) = self.unit_range(unit);
            if end < from {
                continue;
            }
            // Whether it holds keys is unknown: it must be reported, not skipped.
            if self.unit_error(unit).is_some() {
                return Position::Unreadable(unit, start.max(from));
            }
            if let Unit::Stream(i) = unit {
                let s = &self.streams[i];
                for kg in start.max(from)..=end {
                    if s.block_range((kg - start) as usize).is_some() {
                        let ordinal = if kg == from { ordinal } else { 0 };
                        return Position::Block(i, kg, ordinal);
                    }
                }
            }
        }
        Position::End
    }

    /// Where `pos` is, as a cursor.
    fn cursor_of(&self, pos: &Position) -> Option<Cursor> {
        match *pos {
            Position::Block(si, kg, ordinal) => Some(Cursor {
                subtask: self.streams[si].subtask_index,
                key_group: kg as u16,
                ordinal,
            }),
            Position::Unreadable(unit, from) => Some(Cursor {
                subtask: self.unit_subtask(unit),
                key_group: from as u16,
                ordinal: 0,
            }),
            Position::End => None,
        }
    }

    /// The first readable header.
    fn first_header(&self) -> Option<&SubtaskHeader> {
        self.streams.iter().find_map(|s| s.header.as_ref().ok())
    }

    /// The key type, from the first readable header.
    fn key_type(&self) -> Option<&str> {
        self.first_header().map(|h| h.key_type.as_str())
    }
}

enum Position {
    /// Stream index, absolute key-group, ordinal.
    Block(usize, i32, u32),
    /// A unit that cannot be read, from this absolute key-group on.
    Unreadable(Unit, i32),
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

// --- parsed block cache -------------------------------------------------------------------------

/// `(operator index, subtask state position, absolute key-group)`.
type BlockKey = (usize, usize, i32);

/// A key's raw entries in one block, before decoding.
struct RawKey {
    key: String,
    /// `KeyedEntry.key` is left empty: it is `key`.
    entries: Vec<KeyedEntry>,
}

/// A block parsed and grouped by key, in order of first appearance.
struct GroupedBlock {
    keys: Vec<RawKey>,
    /// `subtask N, key-group K: defect` when the block could not be read to its end mark.
    defect: Option<String>,
    /// The inflated block's size.
    inflated_len: usize,
    /// What it costs in the cache, approximately.
    weight: usize,
}

/// Parsed blocks, least recently used evicted first, bounded in (approximate) bytes. A block
/// larger than the whole capacity is still kept, alone, so that paging through it does not
/// re-read and re-parse it per page: the cache then holds at most that one block.
struct BlockCache {
    capacity: usize,
    used: usize,
    tick: u64,
    blocks: HashMap<BlockKey, (Arc<GroupedBlock>, u64)>,
}

impl BlockCache {
    fn get(&mut self, key: &BlockKey) -> Option<Arc<GroupedBlock>> {
        self.tick += 1;
        let tick = self.tick;
        self.blocks.get_mut(key).map(|(b, t)| {
            *t = tick;
            b.clone()
        })
    }

    fn insert(&mut self, key: BlockKey, block: Arc<GroupedBlock>) {
        if self.capacity == 0 {
            return;
        }
        if let Some((old, _)) = self.blocks.remove(&key) {
            self.used -= old.weight;
        }
        while !self.blocks.is_empty() && self.used + block.weight > self.capacity {
            let Some(oldest) = self
                .blocks
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| *k)
            else {
                break;
            };
            if let Some((b, _)) = self.blocks.remove(&oldest) {
                self.used -= b.weight;
            }
        }
        self.tick += 1;
        self.used += block.weight;
        self.blocks.insert(key, (block, self.tick));
    }
}

/// Parse a whole block and group its entries by key, in order of first appearance.
fn group_block(
    block: &[u8],
    key_group: u16,
    header: &SubtaskHeader,
    kg_prefix: usize,
) -> (Vec<RawKey>, Option<BlockDefect>, usize) {
    let mut keys: Vec<RawKey> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut weight = block.len();
    let parsed = parse_key_group_block(
        block,
        key_group,
        &header.parse_ctx(kg_prefix, None),
        &mut |mut entry| {
            weight += std::mem::size_of::<KeyedEntry>() + entry.namespace.len() + entry.value.len();
            match index.get(&entry.key) {
                Some(&i) => {
                    entry.key = String::new();
                    keys[i].entries.push(entry);
                }
                None => {
                    let key = std::mem::take(&mut entry.key);
                    weight += std::mem::size_of::<RawKey>() + 2 * key.len();
                    index.insert(key.clone(), keys.len());
                    keys.push(RawKey {
                        key,
                        entries: vec![entry],
                    });
                }
            }
        },
    );
    (keys, parsed.err(), weight)
}

// --- the reader ---------------------------------------------------------------------------------

/// Random access to a savepoint's keyed state. See the [module documentation](self).
///
/// `Sync`: operators are opened once behind a [`OnceLock`], files are read positionally through a
/// shared pool, and the block cache sits behind a mutex, so one reader can serve concurrent
/// requests.
pub struct StateReader<'a> {
    savepoint: Cow<'a, Savepoint>,
    dir: PathBuf,
    proto: Option<Arc<ProtoContext>>,
    contexts: Vec<OnceLock<OpContext>>,
    cache: Mutex<BlockCache>,
    files: FilePool,
    entry_limit: usize,
    scan_match_limit: usize,
}

impl StateReader<'static> {
    /// Parse `<dir>/_metadata` and read from `dir`.
    pub fn open(dir: &Path) -> Result<StateReader<'static>, ReaderError> {
        let path = dir.join("_metadata");
        let savepoint =
            crate::parser::parse_metadata(&path).map_err(|e| ReaderError::Metadata {
                path: path.display().to_string(),
                error: e.to_string(),
            })?;
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
            files: FilePool::new(DEFAULT_OPEN_FILES),
            entry_limit: DEFAULT_ENTRY_LIMIT,
            scan_match_limit: DEFAULT_SCAN_MATCH_LIMIT,
        }
    }

    /// Decode protobuf-in-POJO values with this context.
    pub fn with_proto(mut self, proto: Arc<ProtoContext>) -> Self {
        self.proto = Some(proto);
        self
    }

    /// Bound the block cache to about `bytes` of parsed blocks (0 disables it). One block larger
    /// than that is still kept alone.
    pub fn with_cache_bytes(self, bytes: usize) -> Self {
        if let Ok(mut c) = self.cache.lock() {
            c.capacity = bytes;
        }
        self
    }

    /// Return at most `n` entries of one state with one key (0 is taken as 1); the rest are
    /// counted in [`KeyRecord::truncated`] and read with [`StateReader::entries`].
    pub fn with_entry_limit(mut self, n: usize) -> Self {
        self.entry_limit = n.max(1);
        self
    }

    /// Return at most `n` matches per [`StateReader::scan`] call (0 is taken as 1).
    pub fn with_scan_match_limit(mut self, n: usize) -> Self {
        self.scan_match_limit = n.max(1);
        self
    }

    /// Hold at most `n` state files open at once (0 is taken as 1).
    pub fn with_open_files(mut self, n: usize) -> Self {
        self.files.set_capacity(n);
        self
    }

    /// State files currently held open.
    pub fn open_files(&self) -> usize {
        self.files.len()
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
                    .filter(|s| {
                        matches!(s.managed_keyed_state, StateHandle::KeyGroups(_))
                            || unsupported_keyed_handle(&s.managed_keyed_state).is_some()
                    })
                    .count(),
            })
            .collect()
    }

    /// The index of the operator whose hex id is `op`, or starts with it (unambiguously).
    pub fn operator_index(&self, op: &str) -> Result<usize, ReaderError> {
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
            [] => Err(ReaderError::UnknownOperator { op: op.to_string() }),
            many => Err(ReaderError::AmbiguousOperator {
                op: op.to_string(),
                candidates: many.iter().map(|&i| ids[i].clone()).collect(),
            }),
        }
    }

    /// The operator's keyed streams, opened on first use.
    fn context(&self, op_index: usize) -> &OpContext {
        self.contexts[op_index]
            .get_or_init(|| OpContext::open(&self.savepoint, op_index, &self.dir))
    }

    /// The keyed states of `op`, from its subtasks' headers.
    pub fn states(&self, op: &str) -> Result<KeyedStates, ReaderError> {
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

    /// The POJO layout of keyed state `state`'s value (field names and types, nested POJOs), from
    /// the first readable header that registers it; `None` when the value is not a POJO or no
    /// header registers the state.
    pub fn value_pojo(&self, op: &str, state: &str) -> Result<Option<PojoInfo>, ReaderError> {
        let ctx = self.context(self.operator_index(op)?);
        Ok(ctx
            .streams
            .iter()
            .filter_map(|s| s.header.as_ref().ok())
            .find_map(|h| h.states.iter().find(|st| st.name == state))
            .and_then(|st| st.pojo.clone()))
    }

    /// A cursor must name a key-group of the operator, and the subtask that holds it.
    fn check_cursor(&self, ctx: &OpContext, cursor: &Cursor) -> Result<(), ReaderError> {
        let max_p = self.savepoint.operators[ctx.op_index]
            .max_parallelism
            .max(1);
        if cursor.key_group as i32 >= max_p {
            return Err(ReaderError::BadCursor {
                cursor: cursor.to_string(),
                reason: format!(
                    "cursor {} does not fit operator {}: it has key-groups 0..{}",
                    cursor,
                    hex_id(&self.savepoint.operators[ctx.op_index].id),
                    max_p - 1
                ),
            });
        }
        match ctx.unit_of(cursor.key_group as i32) {
            Some(u) if ctx.unit_subtask(u) != cursor.subtask => Err(ReaderError::BadCursor {
                cursor: cursor.to_string(),
                reason: format!(
                    "cursor {} does not fit operator {}: key-group {} is held by subtask {}",
                    cursor,
                    hex_id(&self.savepoint.operators[ctx.op_index].id),
                    cursor.key_group,
                    ctx.unit_subtask(u)
                ),
            }),
            _ => Ok(()),
        }
    }

    /// The parsed block of absolute key-group `kg` of stream `si` (`None`: the key-group is
    /// empty, and nothing was read), from the cache or read, inflated and parsed (and cached if
    /// `keep`). `Err`: the block cannot be read or inflated, or the stream's header is unreadable.
    fn grouped(
        &self,
        ctx: &OpContext,
        si: usize,
        kg: i32,
        keep: bool,
        stats: &mut ReadStats,
    ) -> Result<Option<Arc<GroupedBlock>>, String> {
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
            stats.max_block_bytes = stats.max_block_bytes.max(b.inflated_len);
            return Ok(Some(b));
        }
        stats.bytes_read += stored;
        let inline = keyed_handle(&self.savepoint.operators[ctx.op_index], s.state_pos)
            .map(|h| inline_bytes(effective_delegate(h)))
            .unwrap_or(&[]);
        let mut raw = Vec::new();
        let mut inflated = Vec::new();
        let bytes = s
            .src
            .read_pooled(&self.files, inline, range.0, range.1, &mut raw)?;
        let block = key_group_block(bytes, header.compressed, &mut inflated)
            .map_err(|e| format!("{}: key-group {}: snappy decode: {}", s.what, kg, e))?;
        stats.max_block_bytes = stats.max_block_bytes.max(block.len());
        let max_p = self.savepoint.operators[ctx.op_index].max_parallelism;
        let (keys, defect, weight) = group_block(block, kg as u16, header, kg_prefix_len(max_p));
        let grouped = Arc::new(GroupedBlock {
            keys,
            defect: defect.map(|d| format!("subtask {}, key-group {}: {}", s.subtask_index, kg, d)),
            inflated_len: block.len(),
            weight,
        });
        if keep {
            if let Ok(mut c) = self.cache.lock() {
                c.insert(key, grouped.clone());
            }
        }
        Ok(Some(grouped))
    }

    /// A block that could not be read, as a page reports it.
    fn unreadable_block(s: &SubtaskStream, kg: i32, error: String) -> Unreadable {
        Unreadable {
            subtask: s.subtask_index,
            first_key_group: kg as u16,
            last_key_group: kg as u16,
            what: s.what.clone(),
            error,
        }
    }

    /// Decode one raw entry; `i` is its index among the key's entries.
    fn entry(&self, header: &SubtaskHeader, i: usize, e: &KeyedEntry) -> StateEntry {
        match header.states.get(e.state_id as usize) {
            Some(st) => {
                let (map_key, value) =
                    decode_entry_value(st, &e.namespace, &e.value, self.proto.as_deref(), i);
                StateEntry {
                    state: st.labels[0].clone(),
                    state_type: st.labels[1].clone(),
                    value_type: st.labels[2].clone(),
                    map_key,
                    namespace: e.namespace.clone(),
                    raw: e.value.clone(),
                    value,
                }
            }
            // A stateId the header counts but whose metadata was not recognised.
            None => StateEntry {
                state: Arc::from(format!("#{}", e.state_id)),
                state_type: Arc::from("?"),
                value_type: Arc::from("?"),
                map_key: None,
                namespace: e.namespace.clone(),
                raw: e.value.clone(),
                value: crate::parser::value::undecodable("?"),
            },
        }
    }

    /// The label of state `state_id` in `header`.
    fn state_label(header: &SubtaskHeader, state_id: u16) -> Arc<str> {
        match header.states.get(state_id as usize) {
            Some(st) => st.labels[0].clone(),
            None => Arc::from(format!("#{}", state_id)),
        }
    }

    /// Decode a key's entries, at most the entry limit per state.
    fn record(
        &self,
        s: &SubtaskStream,
        header: &SubtaskHeader,
        kg: i32,
        ordinal: u32,
        raw: &RawKey,
    ) -> KeyRecord {
        // (stateId, entries seen), in order of first appearance: a key has a few states.
        let mut counts: Vec<(u16, usize)> = Vec::new();
        let mut entries = Vec::new();
        for (i, e) in raw.entries.iter().enumerate() {
            let seen = match counts.iter_mut().find(|(id, _)| *id == e.state_id) {
                Some((_, n)) => n,
                None => {
                    counts.push((e.state_id, 0));
                    &mut counts.last_mut().expect("pushed").1
                }
            };
            *seen += 1;
            if *seen <= self.entry_limit {
                entries.push(self.entry(header, i, e));
            }
        }
        let truncated = counts
            .into_iter()
            .filter(|&(_, n)| n > self.entry_limit)
            .map(|(id, total)| Truncated {
                state: Self::state_label(header, id),
                total,
                returned: self.entry_limit,
            })
            .collect();
        KeyRecord {
            key: raw.key.clone(),
            key_group: kg as u16,
            subtask: s.subtask_index,
            position: Cursor {
                subtask: s.subtask_index,
                key_group: kg as u16,
                ordinal,
            },
            entries,
            truncated,
        }
    }

    /// Up to `limit` keys from `cursor` on (see the [module documentation](self) for the order;
    /// a `limit` of 0 is taken as 1), each with its entries (at most the entry limit per state),
    /// and the cursor of the next page.
    ///
    /// Key-groups that cannot be read — a subtask whose file is missing or unreadable, whose
    /// header does not parse or whose keyed handle is unsupported, a block that cannot be read or
    /// inflated — are listed in [`KeyPage::unreadable`] and skipped: the page goes on past them
    /// and keeps every key read.
    pub fn key_page(
        &self,
        op: &str,
        cursor: &Cursor,
        limit: usize,
    ) -> Result<KeyPage, ReaderError> {
        let limit = limit.max(1);
        let ctx = self.context(self.operator_index(op)?);
        self.check_cursor(ctx, cursor)?;
        let mut page = KeyPage {
            keys: Vec::new(),
            next: None,
            stats: ReadStats::default(),
            malformed: Vec::new(),
            unreadable: Vec::new(),
        };
        let mut pos = ctx.seek(cursor.key_group as i32, cursor.ordinal);
        loop {
            if page.keys.len() >= limit {
                page.next = ctx.cursor_of(&pos);
                return Ok(page);
            }
            match pos {
                Position::End => return Ok(page),
                Position::Unreadable(unit, from) => {
                    page.unreadable.push(ctx.unreadable(unit, from));
                    pos = ctx.seek(ctx.unit_range(unit).1 + 1, 0);
                }
                Position::Block(si, kg, ordinal) => {
                    let s = &ctx.streams[si];
                    match self.grouped(ctx, si, kg, true, &mut page.stats) {
                        Err(e) => page.unreadable.push(Self::unreadable_block(s, kg, e)),
                        Ok(None) => {}
                        Ok(Some(block)) => {
                            let header = s.header.as_ref().expect("grouped checked it");
                            page.malformed.extend(block.defect.iter().cloned());
                            for (ord, raw) in block.keys.iter().enumerate().skip(ordinal as usize) {
                                if page.keys.len() >= limit {
                                    page.next = Some(Cursor {
                                        subtask: s.subtask_index,
                                        key_group: kg as u16,
                                        ordinal: ord as u32,
                                    });
                                    return Ok(page);
                                }
                                page.keys.push(self.record(s, header, kg, ord as u32, raw));
                            }
                        }
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
    /// `BigInteger`, `Date`, `LocalDate` and Flink `TupleN` keys of those ([`JavaKey::parse`] says
    /// which texts are accepted); other key types answer [`Lookup::NeedsScan`]. An empty key-group
    /// answers [`Lookup::Absent`] without any I/O. [`ReaderError::Unreadable`] when the owning
    /// subtask or block cannot be read, [`ReaderError::Malformed`] when its block is malformed
    /// before the key was found; a key found in a malformed block is `Found` with `malformed` set.
    pub fn get(&self, op: &str, key_text: &str) -> Result<Lookup, ReaderError> {
        let op_index = self.operator_index(op)?;
        let ctx = self.context(op_index);
        let operator = &self.savepoint.operators[op_index];
        let Some(first) = ctx.first_header() else {
            return Err(match ctx.units.first() {
                Some(&u) => ctx.unreadable(u, 0).into_error(),
                None => ReaderError::NoKeyedState {
                    op: hex_id(&operator.id),
                },
            });
        };
        let key_type = first.key_type.as_str();
        if let Some(reason) = first.tuple_key_unhashable() {
            return Ok(Lookup::NeedsScan { reason });
        }
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
        let unit = match ctx.unit_of(kg) {
            None => {
                return Ok(absent(
                    subtask_for_key_group(operator.max_parallelism, operator.parallelism, kg),
                    stats,
                ))
            }
            Some(u) => u,
        };
        if ctx.unit_error(unit).is_some() {
            let mut u = ctx.unreadable(unit, kg);
            u.first_key_group = kg as u16;
            u.last_key_group = kg as u16;
            return Err(u.into_error());
        }
        let Unit::Stream(si) = unit else {
            unreachable!("a failed unit has an error");
        };
        let s = &ctx.streams[si];
        let block = match self.grouped(ctx, si, kg, true, &mut stats) {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(absent(s.subtask_index, stats)),
            Err(e) => return Err(Self::unreadable_block(s, kg, e).into_error()),
        };
        let header = s.header.as_ref().expect("grouped checked it");
        match block.keys.iter().position(|k| k.key == canonical) {
            Some(ord) => Ok(Lookup::Found {
                record: self.record(s, header, kg, ord as u32, &block.keys[ord]),
                stats,
                malformed: block.defect.iter().cloned().collect(),
            }),
            None => match &block.defect {
                Some(d) => Err(ReaderError::Malformed {
                    subtask: s.subtask_index,
                    key_group: kg as u16,
                    what: s.what.clone(),
                    defect: d.clone(),
                    key: key_text.to_string(),
                }),
                None => Ok(absent(s.subtask_index, stats)),
            },
        }
    }

    /// Entries `offset..offset + limit` (a `limit` of 0 is taken as 1) of state `state` of the
    /// key at `position` (a [`KeyRecord::position`]), in on-disk order: the continuation of a
    /// record cut at the entry limit ([`KeyRecord::truncated`]), or any state's entries paged.
    pub fn entries(
        &self,
        op: &str,
        position: &Cursor,
        state: &str,
        offset: usize,
        limit: usize,
    ) -> Result<EntryPage, ReaderError> {
        let limit = limit.max(1);
        let ctx = self.context(self.operator_index(op)?);
        self.check_cursor(ctx, position)?;
        let kg = position.key_group as i32;
        let not_found = || ReaderError::NotFound {
            what: format!("no key at {}", position),
        };
        let unit = ctx.unit_of(kg).ok_or_else(not_found)?;
        if ctx.unit_error(unit).is_some() {
            return Err(ctx.unreadable(unit, kg).into_error());
        }
        let Unit::Stream(si) = unit else {
            unreachable!("a failed unit has an error");
        };
        let s = &ctx.streams[si];
        let mut stats = ReadStats::default();
        let block = match self.grouped(ctx, si, kg, true, &mut stats) {
            Ok(Some(b)) => b,
            Ok(None) => return Err(not_found()),
            Err(e) => return Err(Self::unreadable_block(s, kg, e).into_error()),
        };
        let raw = block
            .keys
            .get(position.ordinal as usize)
            .ok_or_else(not_found)?;
        let header = s.header.as_ref().expect("grouped checked it");
        let mut page = EntryPage {
            key: raw.key.clone(),
            state: Arc::from(state),
            entries: Vec::new(),
            offset,
            total: 0,
            next: None,
            stats: ReadStats::default(),
            malformed: block.defect.iter().cloned().collect(),
        };
        match header.state_id(state) {
            Some(id) => {
                page.state = Self::state_label(header, id);
                for (i, e) in raw.entries.iter().enumerate() {
                    if e.state_id != id {
                        continue;
                    }
                    if page.total >= offset && page.total < offset.saturating_add(limit) {
                        page.entries.push(self.entry(header, i, e));
                    }
                    page.total += 1;
                }
            }
            // Registered by another subtask only: this key has no entries of it.
            None if ctx
                .streams
                .iter()
                .any(|o| o.header.as_ref().is_ok_and(|h| h.state_id(state).is_some())) => {}
            None => {
                return Err(ReaderError::NotFound {
                    what: format!(
                        "operator {} has no keyed state {:?}",
                        hex_id(&self.savepoint.operators[ctx.op_index].id),
                        state
                    ),
                })
            }
        }
        let end = offset.saturating_add(page.entries.len());
        page.next = (end < page.total).then_some(end);
        page.stats = stats;
        Ok(page)
    }

    /// Test `matches` on every key's rendered text from `cursor` on, reading blocks until their
    /// stored size reaches `byte_budget` (at least one block per call, so a scan always moves), or
    /// until the scan match limit is reached. Returns the matching keys with their entries, and
    /// where to resume. Unreadable key-groups are listed in [`ScanPage::unreadable`] and skipped.
    pub fn scan(
        &self,
        op: &str,
        cursor: &Cursor,
        byte_budget: u64,
        matches: &mut dyn FnMut(&str) -> bool,
    ) -> Result<ScanPage, ReaderError> {
        let opts = ScanOptions {
            byte_budget,
            max_matches: self.scan_match_limit,
            last_key_group: None,
        };
        self.scan_with(op, cursor, &opts, matches)
    }

    /// The proto context values are decoded with, if any (for a client that must decode the
    /// same way, e.g. a download through [`crate::export_state`]).
    pub fn proto(&self) -> Option<&ProtoContext> {
        self.proto.as_deref()
    }

    /// The cursor of the first key at or after absolute key-group `key_group` (ordinal 0 of it,
    /// named by the subtask that holds it), for a scan restricted to a key-group range.
    pub fn cursor_at(&self, op: &str, key_group: u16) -> Result<Cursor, ReaderError> {
        let op_index = self.operator_index(op)?;
        let ctx = self.context(op_index);
        let operator = &self.savepoint.operators[op_index];
        let subtask = match ctx.unit_of(key_group as i32) {
            Some(u) => ctx.unit_subtask(u),
            None => subtask_for_key_group(
                operator.max_parallelism,
                operator.parallelism,
                key_group as i32,
            ),
        };
        Ok(Cursor {
            subtask,
            key_group,
            ordinal: 0,
        })
    }

    /// How far `at` is in key-groups `first..=last` (`last` `None` = the operator's last), from
    /// `_metadata`'s offsets alone: every key-group before `at`'s is done; `at` `None` = all.
    pub fn progress(
        &self,
        op: &str,
        at: Option<&Cursor>,
        first: u16,
        last: Option<u16>,
    ) -> Result<Progress, ReaderError> {
        let op_index = self.operator_index(op)?;
        let ctx = self.context(op_index);
        let max_p = self.savepoint.operators[op_index].max_parallelism.max(1) as i64;
        let first = first as i64;
        let last = last.map(|l| l as i64).unwrap_or(i64::MAX).min(max_p - 1);
        let mut p = Progress::default();
        if last < first {
            return Ok(p);
        }
        let done_below = at.map(|c| c.key_group as i64).unwrap_or(i64::MAX);
        p.key_groups_total = (last - first + 1) as u64;
        p.key_groups_done = (done_below.min(last + 1) - first).max(0) as u64;
        for s in &ctx.streams {
            for i in 0..s.offsets.len() {
                let kg = s.start_key_group as i64 + i as i64;
                if kg < first || kg > last {
                    continue;
                }
                if let Some((start, end)) = s.block_range(i) {
                    p.bytes_total += end - start;
                    if kg < done_below {
                        p.bytes_done += end - start;
                    }
                }
            }
        }
        Ok(p)
    }

    /// Count each state's keys and entries in whole blocks from `cursor`'s key-group on (its
    /// ordinal is ignored), until their stored size reaches `byte_budget` (at least one block per
    /// call). No value is decoded; memory is one block (not cached). Unreadable key-groups are
    /// listed and skipped, as by a page.
    pub fn count(
        &self,
        op: &str,
        cursor: &Cursor,
        byte_budget: u64,
    ) -> Result<CountPage, ReaderError> {
        let ctx = self.context(self.operator_index(op)?);
        self.check_cursor(ctx, cursor)?;
        let mut page = CountPage {
            states: Vec::new(),
            keys: 0,
            next: None,
            stats: ReadStats::default(),
            malformed: Vec::new(),
            unreadable: Vec::new(),
        };
        let mut pos = ctx.seek(cursor.key_group as i32, 0);
        let mut visited = 0usize;
        // (stateId, entries of the current key), reused per key.
        let mut per_key: Vec<(u16, u64)> = Vec::new();
        loop {
            if !matches!(pos, Position::End) && visited > 0 && page.stats.block_bytes >= byte_budget
            {
                page.next = ctx.cursor_of(&pos);
                return Ok(page);
            }
            match pos {
                Position::End => return Ok(page),
                Position::Unreadable(unit, from) => {
                    page.unreadable.push(ctx.unreadable(unit, from));
                    visited += 1;
                    pos = ctx.seek(ctx.unit_range(unit).1 + 1, 0);
                }
                Position::Block(si, kg, _) => {
                    let s = &ctx.streams[si];
                    match self.grouped(ctx, si, kg, false, &mut page.stats) {
                        Err(e) => page.unreadable.push(Self::unreadable_block(s, kg, e)),
                        Ok(None) => {}
                        Ok(Some(block)) => {
                            let header = s.header.as_ref().expect("grouped checked it");
                            page.malformed.extend(block.defect.iter().cloned());
                            for raw in &block.keys {
                                page.keys += 1;
                                per_key.clear();
                                for e in &raw.entries {
                                    match per_key.iter_mut().find(|(id, _)| *id == e.state_id) {
                                        Some((_, n)) => *n += 1,
                                        None => per_key.push((e.state_id, 1)),
                                    }
                                }
                                for &(id, n) in &per_key {
                                    let label = Self::state_label(header, id);
                                    match page.states.iter_mut().find(|c| c.state == label) {
                                        Some(c) => {
                                            c.keys += 1;
                                            c.entries += n;
                                        }
                                        None => page.states.push(StateCount {
                                            state: label,
                                            keys: 1,
                                            entries: n,
                                        }),
                                    }
                                }
                            }
                        }
                    }
                    visited += 1;
                    pos = ctx.seek(kg + 1, 0);
                }
            }
        }
    }

    /// [`StateReader::scan`] with its limits given per call ([`ScanOptions`]): a byte budget, a
    /// match cap, and an optional last key-group (a search restricted to a key-group range).
    pub fn scan_with(
        &self,
        op: &str,
        cursor: &Cursor,
        opts: &ScanOptions,
        matches: &mut dyn FnMut(&str) -> bool,
    ) -> Result<ScanPage, ReaderError> {
        let byte_budget = opts.byte_budget;
        let max_matches = opts.max_matches.max(1);
        let last = opts.last_key_group.map(i32::from).unwrap_or(i32::MAX);
        let ctx = self.context(self.operator_index(op)?);
        self.check_cursor(ctx, cursor)?;
        let mut page = ScanPage {
            matches: Vec::new(),
            next: None,
            stats: ReadStats::default(),
            malformed: Vec::new(),
            unreadable: Vec::new(),
        };
        let mut pos = ctx.seek(cursor.key_group as i32, cursor.ordinal);
        let mut visited = 0usize;
        loop {
            // Past the range: the scan is over.
            if let Position::Block(_, kg, _) | Position::Unreadable(_, kg) = pos {
                if kg > last {
                    pos = Position::End;
                }
            }
            if !matches!(pos, Position::End)
                && ((visited > 0 && page.stats.block_bytes >= byte_budget)
                    || page.matches.len() >= max_matches)
            {
                page.next = ctx.cursor_of(&pos);
                return Ok(page);
            }
            match pos {
                Position::End => return Ok(page),
                Position::Unreadable(unit, from) => {
                    page.unreadable.push(ctx.unreadable(unit, from));
                    visited += 1;
                    pos = ctx.seek(ctx.unit_range(unit).1 + 1, 0);
                }
                Position::Block(si, kg, ordinal) => {
                    let s = &ctx.streams[si];
                    // A scan walks past blocks once: it does not fill the cache.
                    match self.grouped(ctx, si, kg, false, &mut page.stats) {
                        Err(e) => page.unreadable.push(Self::unreadable_block(s, kg, e)),
                        Ok(None) => {}
                        Ok(Some(block)) => {
                            let header = s.header.as_ref().expect("grouped checked it");
                            page.malformed.extend(block.defect.iter().cloned());
                            for (ord, raw) in block.keys.iter().enumerate().skip(ordinal as usize) {
                                if page.matches.len() >= max_matches {
                                    page.next = Some(Cursor {
                                        subtask: s.subtask_index,
                                        key_group: kg as u16,
                                        ordinal: ord as u32,
                                    });
                                    return Ok(page);
                                }
                                if matches(&raw.key) {
                                    page.matches
                                        .push(self.record(s, header, kg, ord as u32, raw));
                                }
                            }
                        }
                    }
                    visited += 1;
                    pos = ctx.seek(kg + 1, 0);
                }
            }
        }
    }
}
