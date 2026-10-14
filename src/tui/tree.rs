//! The savepoint tree: operators → keys → states → values, plus operator states.
//!
//! The tree holds **navigation state only**. Keys are not loaded up front: an expanded keyed
//! operator asks the [`StateReader`] for the page of keys it shows (`key_page` from the cursor of
//! its first visible key), so every key of every subtask is reachable and memory is bounded by a
//! few pages and the reader's block cache — never by the savepoint. Keys come in the reader's
//! order: key-groups ascending, and inside one key-group in Flink's on-disk order (not sorted).
//!
//! Every value shown here was decoded by [`crate::parser::value`] (inside the reader), rendered by
//! [`value_to_display`] — the same decoding the JSON and `--export-state` exports use. A MAP's
//! rows are the entries of the object its entries make ([`crate::export::state_value`]), a LIST's
//! rows the elements of the decoded array, a POJO's rows its decoded fields. What this module adds
//! is presentation: display limits, `auto_date` (an epoch-millis `Long` shown as a date) and the
//! debug hex suffix.
//!
//! A filter (`/`) searches the keys with [`StateReader::scan`], a step at a time under a byte
//! budget ([`TreeView::search_step`]), so it shows matches as it finds them and can be cancelled;
//! an exact key is looked up with [`StateReader::get`] (its key-group computed, one block read)
//! when the key type is hashable.

use ratatui::prelude::*;
use ratatui::widgets::{Block, List, ListItem, ListState};
use serde_json::Value;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::export::state_value;
use crate::operators::{KeyedInfo, OperatorSummary};
use crate::parser::pojo::millis_to_iso;
use crate::parser::value::{split_map_types, value_to_display};
use crate::proto::ProtoContext;
use crate::reader::{Cursor, KeyRecord, Lookup, ReaderError, StateEntry, StateReader};
use crate::tui::filter::AppliedFilter;
use crate::tui::text;

/// Operator-state elements listed at once; past it, `↑`/`↓` pager rows scroll the window.
const ENTRY_PAGE_SIZE: usize = 500;
/// Rows `PageUp`/`PageDown` move by before the tree has been rendered (then: the list's height).
const DEFAULT_PAGE_ROWS: usize = 20;
/// Keys listed at once under an expanded operator.
const PARTITION_PAGE_SIZE: usize = 30;
/// Entries of an expanded MAP value listed before a "… +N more" row.
const MAP_DISPLAY_LIMIT: usize = 50;
/// Elements of an expanded LIST (or array) value listed before a "… +N more" row.
const LIST_DISPLAY_LIMIT: usize = 100;
/// A nested object or array wider than this gets rows of its own when its parent is expanded.
const NESTED_SPLIT_WIDTH: usize = 60;
/// `auto_date` shows a `Long` as a date when it lies in this range of epoch millis (2000–2040).
const AUTO_DATE_MILLIS: std::ops::Range<i64> = 946_684_800_000..2_208_988_800_000;
/// Marker shown next to Kafka connector operators/states.
const KAFKA_ICON: &str = "📨 ";
/// Marker on an operator some of whose state files could not be read (details under `i`).
const LOAD_ERROR_MARK: &str = "⚠ ";
/// Stored bytes of blocks one search step scans ([`TreeView::search_step`]).
pub const SEARCH_STEP_BYTES: u64 = 4 << 20;

/// Whether an operator-state name is a Kafka connector state (source reader / sink writer+committer).
fn is_kafka_state(name: &str) -> bool {
    matches!(
        name,
        "SourceReaderState" | "writer_raw_states" | "streaming_committer_raw_states"
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum NodeLevel {
    Operator,
    State,
    Entry,
    MapEntry,
}

/// What a row is, apart from its depth. Rows are told apart by this, never by their label: a key,
/// a value or an operator-state element may contain `↓` or `…` itself.
#[derive(Debug, Clone, Copy, PartialEq)]
enum NodeKind {
    /// Data: an operator, a key, a state, a value, an element.
    Data,
    /// `↑ a-b / n`: moving onto it scrolls its window back (keys when `state_idx` is `None`,
    /// else the elements of operator state `state_idx`).
    PagerUp,
    /// `↓ a-b / n`: moving onto it scrolls its window forward.
    PagerDown,
    /// A note, not data: "… +N more", unreadable key-groups.
    Marker,
}

#[derive(Debug, Clone)]
struct FlatNode {
    level: NodeLevel,
    kind: NodeKind,
    label: String,
    /// Breadcrumb path for status bar display
    path: String,
    op_idx: usize,
    /// An operator state's index (its header, elements and pagers).
    state_idx: Option<usize>,
    /// A key's position (its row and the rows under it).
    key: Option<Cursor>,
    /// A key's state (descriptor) index, or an operator-state element's index in its state.
    entry_idx: Option<usize>,
    /// Under a MAP state: the index of the entry a row shows, among the key's entries of it.
    item: Option<usize>,
    expanded: bool,
    has_children: bool,
}

impl FlatNode {
    fn new(level: NodeLevel, label: String, path: String, op_idx: usize) -> Self {
        FlatNode {
            level,
            kind: NodeKind::Data,
            label,
            path,
            op_idx,
            state_idx: None,
            key: None,
            entry_idx: None,
            item: None,
            expanded: false,
            has_children: false,
        }
    }

    fn kind(mut self, kind: NodeKind) -> Self {
        self.kind = kind;
        self
    }

    fn at(mut self, state_idx: Option<usize>, entry_idx: Option<usize>) -> Self {
        self.state_idx = state_idx;
        self.entry_idx = entry_idx;
        self
    }

    fn of_key(mut self, key: Cursor, entry_idx: Option<usize>) -> Self {
        self.key = Some(key);
        self.entry_idx = entry_idx;
        self
    }
}

/// The bytes behind the selected row, for the hex overlay (`x`).
#[derive(Debug, Clone, PartialEq)]
pub struct RawBytes {
    pub key_title: String,
    pub key: Vec<u8>,
    pub value_title: String,
    pub value: Vec<u8>,
}

/// How many keys an operator has, as far as is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyCount {
    /// Not counted.
    #[default]
    Unknown,
    /// The count pass is on it: `keys` so far, `percent` of the key-groups passed.
    Counting { keys: usize, percent: u8 },
    /// Every key counted.
    Exact(usize),
    /// Counted, but some key-group could not be read (or the count was stopped): a lower bound.
    AtLeast(usize),
}

/// A keyed state of an operator, as the tree labels it.
#[derive(Debug, Clone)]
struct StateDesc {
    name: String,
    state_type: String,
    value_type: String,
    /// ` → com.example.Pojo { a: Long, … }` for a POJO value, else empty.
    pojo_detail: String,
}

/// One state of one key: its entries (those the reader returned, all of them once completed),
/// how many it has, and the value they make.
#[derive(Debug, Clone)]
struct Slot {
    entries: Vec<StateEntry>,
    total: usize,
    value: Option<Value>,
}

/// A key the tree shows, built from a [`KeyRecord`] once (its values are decoded by the reader):
/// a rebuild of the rows reuses it.
#[derive(Debug, Clone)]
struct KeyRow {
    key: String,
    key_group: u16,
    /// By descriptor index.
    slots: Vec<Slot>,
}

impl KeyRow {
    fn new(record: KeyRecord, descriptors: &[StateDesc]) -> KeyRow {
        let mut slots: Vec<Slot> = descriptors
            .iter()
            .map(|_| Slot {
                entries: Vec::new(),
                total: 0,
                value: None,
            })
            .collect();
        for e in record.entries {
            if let Some(i) = descriptors.iter().position(|d| *d.name == *e.state) {
                slots[i].entries.push(e);
            }
        }
        for (i, slot) in slots.iter_mut().enumerate() {
            slot.total = record
                .truncated
                .iter()
                .find(|t| *t.state == *descriptors[i].name)
                .map_or(slot.entries.len(), |t| t.total);
            let refs: Vec<&StateEntry> = slot.entries.iter().collect();
            slot.value = state_value(&descriptors[i].state_type, &refs);
        }
        KeyRow {
            key: record.key,
            key_group: record.key_group,
            slots,
        }
    }
}

/// An ordered sequence of keys the tree pages through: every key of the operator, the keys of a
/// key-group range, or the matches of a search.
#[derive(Debug, Clone)]
struct KeySeq {
    /// The positions known so far, in order.
    positions: Vec<Cursor>,
    /// Where to go on finding keys (`None`: all found). For a browsing sequence the next
    /// `key_page`, for a search the next `scan`.
    next: Option<Cursor>,
    /// A key-group range's last key-group: a key past it ends the sequence.
    stop_kg: Option<u16>,
    /// A range's first key-group: keys before it are skipped (when its start could not be
    /// sought directly).
    start_kg: u16,
    /// Search matches: not contiguous in reader order (each fetched on its own), found by
    /// [`TreeView::search_step`] rather than by paging.
    matches: bool,
    /// Matches already in `positions` (a key found by `get` and again by the scan).
    seen: HashSet<Cursor>,
    /// First visible key, moved by the pager rows.
    offset: usize,
}

impl KeySeq {
    fn browse(from: Cursor) -> KeySeq {
        KeySeq {
            positions: Vec::new(),
            next: Some(from),
            stop_kg: None,
            start_kg: 0,
            matches: false,
            seen: HashSet::new(),
            offset: 0,
        }
    }

    fn done(&self) -> bool {
        self.next.is_none()
    }

    fn push_match(&mut self, pos: Cursor) {
        if self.seen.insert(pos) {
            self.positions.push(pos);
        }
    }
}

/// Keyed state of one operator, for display.
struct KeyedSnapshot {
    op_hex: String,
    key_type: String,
    descriptors: Vec<StateDesc>,
    /// State files and key-groups that could not be read, naming them: from the headers, then
    /// from every page that met one.
    errors: Vec<String>,
    malformed: Vec<String>,
    count: KeyCount,
    /// Every key.
    all: KeySeq,
    /// Under a filter: its matches (or range).
    filtered: Option<KeySeq>,
    /// The keys of the current window (and the next one), by position.
    rows: HashMap<Cursor, KeyRow>,
}

impl KeyedSnapshot {
    fn seq(&self) -> &KeySeq {
        self.filtered.as_ref().unwrap_or(&self.all)
    }

    fn seq_mut(&mut self) -> &mut KeySeq {
        self.filtered.as_mut().unwrap_or(&mut self.all)
    }

    /// The operator's key count when it is known exactly: counted, or paged to the end.
    fn total(&self) -> Option<usize> {
        match self.count {
            KeyCount::Exact(n) => Some(n),
            _ if self.all.done() && self.errors.is_empty() => Some(self.all.positions.len()),
            _ => None,
        }
    }

    /// A lower bound of the key count: what was paged, counted or seen.
    fn at_least(&self) -> usize {
        let counted = match self.count {
            KeyCount::Counting { keys, .. } | KeyCount::AtLeast(keys) | KeyCount::Exact(keys) => {
                keys
            }
            KeyCount::Unknown => 0,
        };
        counted.max(self.all.positions.len())
    }

    /// `1000 keys`, `≥ 60 keys`, `≥ 60 keys, counting 37%`, `? keys`.
    fn keys_text(&self) -> String {
        match (self.total(), self.count) {
            (Some(n), _) => format!("{} keys", n),
            (None, KeyCount::Counting { percent, .. }) => {
                format!("≥ {} keys, counting {}%", self.at_least(), percent)
            }
            (None, _) if self.at_least() == 0 => "? keys".to_string(),
            (None, _) => format!("≥ {} keys", self.at_least()),
        }
    }

    /// The key count on the operator row: with a filter, `matches/keys`.
    fn count_label(&self, search_percent: Option<u8>) -> String {
        match &self.filtered {
            None => self.keys_text(),
            Some(f) => {
                let total = match self.total() {
                    Some(n) => n.to_string(),
                    None if self.at_least() == 0 => "?".to_string(),
                    None => format!("≥ {}", self.at_least()),
                };
                let found = f.positions.len();
                match (f.done(), search_percent) {
                    (true, _) => format!("{}/{} keys", found, total),
                    (false, Some(p)) => format!("{}/{} keys, searching {}%", found, total, p),
                    (false, None) => format!("≥ {}/{} keys", found, total),
                }
            }
        }
    }

    /// The `/ n` of a pager row: exact once the sequence is complete.
    fn pager_total(&self) -> String {
        let seq = self.seq();
        if seq.done() {
            seq.positions.len().to_string()
        } else if self.filtered.is_none() {
            match self.total() {
                Some(n) => n.to_string(),
                None => format!("≥ {}", seq.positions.len()),
            }
        } else {
            format!("≥ {}", seq.positions.len())
        }
    }

    /// Rows under the operator (for `hide_empty`): an operator with keyed states may hold keys
    /// until it is known not to.
    fn child_estimate(&self) -> usize {
        if self.descriptors.is_empty() {
            return 0;
        }
        match &self.filtered {
            Some(f) if f.done() => f.positions.len(),
            Some(f) => f.positions.len().max(1),
            None => self.total().unwrap_or(1),
        }
    }

    fn note_page(&mut self, unreadable: &[crate::reader::Unreadable], malformed: &[String]) {
        for u in unreadable {
            if !self.errors.contains(&u.error) {
                self.errors.push(u.error.clone());
            }
        }
        for m in malformed {
            if !self.malformed.contains(m) {
                self.malformed.push(m.clone());
            }
        }
    }

    fn note_error(&mut self, e: ReaderError) {
        let e = e.to_string();
        if !self.errors.contains(&e) {
            self.errors.push(e);
        }
    }
}

struct OperatorSnapshot {
    summary: OperatorSummary,
    /// Short operator type (e.g. "KeyedBroadcastProcess", "RichFlatMap")
    operator_type: String,
    /// Non-keyed operator states (their elements stay in `summary`).
    states: Vec<StateSnapshot>,
    /// Keyed state, read through the reader.
    keyed: Option<KeyedSnapshot>,
}

impl OperatorSnapshot {
    /// Count user-visible children: keys (or matches) **plus** non-keyed operator states.
    /// Operators with only operator state (e.g. a Kafka source/sink) have no keys, so they must be
    /// counted here too — otherwise `hide_empty` would hide them and their decoded state.
    fn child_count(&self) -> usize {
        self.keyed.as_ref().map_or(0, KeyedSnapshot::child_estimate) + self.states.len()
    }

    fn load_errors(&self) -> usize {
        self.keyed.as_ref().map_or(0, |k| k.errors.len())
    }

    /// A Kafka connector operator (source or sink), detected from its operator-state names.
    fn is_kafka(&self) -> bool {
        self.states.iter().any(|s| is_kafka_state(&s.name))
    }
}

struct StateSnapshot {
    name: String,
    state_type: String,
    /// Number of elements (they are read from the operator's summary).
    len: usize,
    /// Indices of the elements that match the current filter (None = no filter, show all).
    filtered_indices: Option<Vec<usize>>,
    /// First visible element (an index into the filtered list), moved by the pager rows.
    entry_offset: usize,
}

impl StateSnapshot {
    fn visible_count(&self) -> usize {
        self.filtered_indices.as_ref().map_or(self.len, Vec::len)
    }

    /// The element index of the `vi`-th visible element.
    fn real_index(&self, vi: usize) -> usize {
        self.filtered_indices.as_ref().map_or(vi, |f| f[vi])
    }

    fn count_label(&self) -> String {
        let (visible, total) = (self.visible_count(), self.len);
        if visible == total {
            format!("{} entries", total)
        } else {
            format!("{}/{} entries", visible, total)
        }
    }
}

/// A pager row: `  ↑ 31-60 / 500`.
fn pager_label(indent: &str, arrow: char, start: usize, end: usize, of: &str) -> String {
    format!("{}{} {}-{} / {}", indent, arrow, start + 1, end, of)
}

/// A search over the keys of every keyed operator, run a step at a time.
struct Search {
    filter: AppliedFilter,
    /// Keyed operators still to scan, in order.
    queue: std::collections::VecDeque<usize>,
    /// Operators it was started on, for the progress.
    total_ops: usize,
}

pub struct TreeView {
    reader: Arc<StateReader<'static>>,
    nodes: Vec<FlatNode>,
    list_state: ListState,
    operators: Vec<OperatorSnapshot>,
    pub hide_empty: bool,
    pub debug_mode: bool,
    /// Show a `Long` value that looks like epoch millis as a date
    pub auto_date: bool,
    display_width: u16,
    /// Rows the list showed at its last render: what `PageUp`/`PageDown` move by.
    page_rows: usize,
    proto_ctx: Option<Arc<ProtoContext>>,
    search: Option<Search>,
    /// Keys received from the reader (each decoded once, by it), for tests.
    fetched: usize,
}

impl TreeView {
    /// Open `dir`: parse `_metadata`, read the operator states and the keyed headers. No key is
    /// read until an operator is expanded.
    pub fn open(dir: &Path) -> Result<TreeView, ReaderError> {
        let reader = StateReader::open(dir)?;
        let operators = crate::operators::summarize(reader.savepoint(), dir);
        Ok(TreeView::new(Arc::new(reader), operators))
    }

    /// A tree over `operators` (summaries of operators of the reader's savepoint, in any order
    /// or subset), reading their keys from `reader`.
    pub fn new(reader: Arc<StateReader<'static>>, operators: Vec<OperatorSummary>) -> Self {
        let operators: Vec<OperatorSnapshot> = operators
            .into_iter()
            .map(|summary| {
                let states = summary
                    .states
                    .iter()
                    .map(|s| StateSnapshot {
                        name: s.name.clone(),
                        state_type: s.state_type.clone(),
                        len: s.elements.len(),
                        filtered_indices: None,
                        entry_offset: 0,
                    })
                    .collect();
                let keyed = keyed_snapshot(&reader, &summary);
                OperatorSnapshot {
                    operator_type: shorten_operator_type(&summary.identifier_name),
                    summary,
                    states,
                    keyed,
                }
            })
            .collect();

        let mut tv = Self {
            reader,
            nodes: Vec::new(),
            list_state: ListState::default(),
            operators,
            hide_empty: true,
            debug_mode: false,
            auto_date: true,
            display_width: 120,
            page_rows: DEFAULT_PAGE_ROWS,
            proto_ctx: None,
            search: None,
            fetched: 0,
        };
        tv.rebuild_flat_list();
        if !tv.nodes.is_empty() {
            tv.list_state.select(Some(0));
        }
        tv
    }

    /// The reader the tree reads from.
    pub fn reader(&self) -> &Arc<StateReader<'static>> {
        &self.reader
    }

    /// Number of operators (shown or hidden).
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// The summary of operator `op_idx`.
    pub fn summary(&self, op_idx: usize) -> Option<&OperatorSummary> {
        self.operators.get(op_idx).map(|o| &o.summary)
    }

    /// Every operator summary, in order.
    pub fn summaries(&self) -> Vec<OperatorSummary> {
        self.operators.iter().map(|o| o.summary.clone()).collect()
    }

    /// The key count of operator `op_idx` (`None`: no keyed state).
    pub fn key_count(&self, op_idx: usize) -> Option<KeyCount> {
        let k = self.operators.get(op_idx)?.keyed.as_ref()?;
        Some(match k.total() {
            Some(n) => KeyCount::Exact(n),
            None => k.count,
        })
    }

    /// A lower bound of the key count of operator `op_idx`.
    pub fn keys_at_least(&self, op_idx: usize) -> usize {
        self.operators
            .get(op_idx)
            .and_then(|o| o.keyed.as_ref())
            .map_or(0, KeyedSnapshot::at_least)
    }

    /// Record what the count pass found for operator `op_idx`.
    pub fn set_key_count(&mut self, op_idx: usize, count: KeyCount) {
        if let Some(k) = self
            .operators
            .get_mut(op_idx)
            .and_then(|o| o.keyed.as_mut())
        {
            k.count = count;
        }
        self.rebuild_flat_list();
    }

    /// Keyed operators to count: `(op_idx, hex id, maxParallelism)`.
    pub fn keyed_operators(&self) -> Vec<(usize, String, i32)> {
        self.operators
            .iter()
            .enumerate()
            .filter_map(|(i, o)| {
                o.keyed
                    .as_ref()
                    .filter(|k| !k.descriptors.is_empty())
                    .map(|k| (i, k.op_hex.clone(), o.summary.max_parallelism))
            })
            .collect()
    }

    /// What the `i` overlay and the export say about the keyed state of `op_idx`.
    pub fn keyed_info(&self, op_idx: usize) -> Option<KeyedInfo> {
        let k = self.operators.get(op_idx)?.keyed.as_ref()?;
        Some(KeyedInfo {
            key_type: k.key_type.clone(),
            state_names: k.descriptors.iter().map(|d| d.name.clone()).collect(),
            keys: k
                .keys_text()
                .trim_end_matches(" keys")
                .replace(" keys,", ","),
            errors: k.errors.clone(),
            malformed: k.malformed.clone(),
        })
    }

    /// Unreadable state files / key-groups met so far, over all operators.
    pub fn load_errors(&self) -> usize {
        self.operators
            .iter()
            .map(OperatorSnapshot::load_errors)
            .sum()
    }

    // --- fetching keys -------------------------------------------------------------------------

    /// Make sure operator `op_idx`'s current window of keys is loaded: find positions up to two
    /// pages past its offset (one page of look-ahead tells whether a `↓` row is needed), read the
    /// window's keys that are not loaded yet, and drop the ones far from it.
    fn ensure_window(&mut self, op_idx: usize) {
        let reader = self.reader.clone();
        let Some(k) = self.operators[op_idx].keyed.as_mut() else {
            return;
        };
        if k.descriptors.is_empty() {
            return;
        }
        let page = PARTITION_PAGE_SIZE;
        let mut fetched = 0;

        // Browsing (all keys, or a key-group range): discover positions by paging forward.
        if !k.seq().matches {
            loop {
                let seq = k.seq();
                let want = seq.offset + 2 * page;
                let Some(next) = seq.next else { break };
                if seq.positions.len() >= want {
                    break;
                }
                let limit = (want - seq.positions.len()).max(page);
                match reader.key_page(&k.op_hex, &next, limit) {
                    Ok(p) => {
                        k.note_page(&p.unreadable, &p.malformed);
                        let (start_kg, stop_kg) = (k.seq().start_kg, k.seq().stop_kg);
                        let mut ended = false;
                        for record in p.keys {
                            if stop_kg.is_some_and(|s| record.key_group > s) {
                                ended = true;
                                break;
                            }
                            if record.key_group < start_kg {
                                continue;
                            }
                            k.seq_mut().positions.push(record.position);
                            fetched += 1;
                            k.rows
                                .insert(record.position, KeyRow::new(record, &k.descriptors));
                        }
                        k.seq_mut().next = if ended { None } else { p.next };
                    }
                    Err(e) => {
                        k.note_error(e);
                        k.seq_mut().next = None;
                    }
                }
            }
        }

        // The window's keys not loaded yet.
        let seq = k.seq();
        let start = seq.offset.min(seq.positions.len());
        let end = (start + page).min(seq.positions.len());
        let missing: Vec<usize> = (start..end)
            .filter(|&i| !k.rows.contains_key(&seq.positions[i]))
            .collect();
        let contiguous = !seq.matches;
        let mut i = 0;
        while i < missing.len() {
            // Consecutive browsing positions are consecutive keys: one page reads the run.
            let mut run = 1;
            if contiguous {
                while i + run < missing.len() && missing[i + run] == missing[i] + run {
                    run += 1;
                }
            }
            let from = k.seq().positions[missing[i]];
            match reader.key_page(&k.op_hex, &from, run) {
                Ok(p) => {
                    k.note_page(&p.unreadable, &p.malformed);
                    for record in p.keys {
                        fetched += 1;
                        k.rows
                            .insert(record.position, KeyRow::new(record, &k.descriptors));
                    }
                }
                Err(e) => k.note_error(e),
            }
            i += run;
        }

        // Keep the window, the page before and the page after it.
        let seq = k.seq();
        let lo = seq.offset.saturating_sub(page).min(seq.positions.len());
        let hi = (seq.offset + 2 * page).min(seq.positions.len());
        let keep: HashSet<Cursor> = seq.positions[lo..hi].iter().copied().collect();
        k.rows.retain(|c, _| keep.contains(c));
        self.fetched += fetched;
    }

    /// Read every entry of a MAP state the reader cut at its entry limit, so that its rows are
    /// those of the whole map.
    fn complete_slot(&mut self, op_idx: usize, key: Cursor, ei: usize) {
        let reader = self.reader.clone();
        let Some(k) = self.operators[op_idx].keyed.as_mut() else {
            return;
        };
        let Some(desc) = k.descriptors.get(ei).cloned() else {
            return;
        };
        let Some(row) = k.rows.get_mut(&key) else {
            return;
        };
        let slot = &mut row.slots[ei];
        if desc.state_type != "MAP" || slot.entries.len() >= slot.total {
            return;
        }
        let mut offset = slot.entries.len();
        let mut error = None;
        while offset < slot.total {
            match reader.entries(&k.op_hex, &key, &desc.name, offset, 10_000) {
                Ok(p) if !p.entries.is_empty() => {
                    offset += p.entries.len();
                    slot.entries.extend(p.entries);
                    if p.next.is_none() {
                        break;
                    }
                }
                Ok(_) => break,
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }
        slot.total = slot.entries.len().max(slot.total);
        let refs: Vec<&StateEntry> = slot.entries.iter().collect();
        slot.value = state_value(&desc.state_type, &refs);
        if let Some(e) = error {
            k.note_error(e);
        }
    }

    /// Before rows are built: load the windows of the expanded keyed operators, and complete
    /// the expanded MAP states that were cut.
    fn prepare(&mut self) {
        let expanded_ops: Vec<usize> = self
            .nodes
            .iter()
            .filter(|n| n.level == NodeLevel::Operator && n.kind == NodeKind::Data && n.expanded)
            .map(|n| n.op_idx)
            .collect();
        for op in expanded_ops {
            self.ensure_window(op);
        }
        let expanded_entries: Vec<(usize, Cursor, usize)> = self
            .nodes
            .iter()
            .filter(|n| n.level == NodeLevel::Entry && n.kind == NodeKind::Data && n.expanded)
            .filter_map(|n| Some((n.op_idx, n.key?, n.entry_idx?)))
            .collect();
        for (op, key, ei) in expanded_entries {
            self.complete_slot(op, key, ei);
        }
    }

    fn rebuild_flat_list(&mut self) {
        self.prepare();
        let mut nodes = Vec::new();
        let style = ValueStyle {
            debug: self.debug_mode,
            auto_date: self.auto_date,
            max_inline: self.display_width.saturating_sub(12) as usize,
        };
        let search_percent = self.search_percent();

        for (op_idx, op) in self.operators.iter().enumerate() {
            let child_count = op.child_count();
            // An operator whose files could not be read may look empty only because of that:
            // hiding it would hide the error too.
            if self.hide_empty && child_count == 0 && op.load_errors() == 0 {
                continue;
            }
            let op_expanded = self.is_expanded(NodeLevel::Operator, op_idx, None, None, None);

            let desc_count = op.keyed.as_ref().map_or(0, |k| k.descriptors.len());
            let key_type_tag = if self.debug_mode {
                op.keyed
                    .as_ref()
                    .map(|k| format!(" <{}>", k.key_type))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let type_tag = if self.debug_mode && !op.operator_type.is_empty() {
                format!(" [{}]", op.operator_type)
            } else {
                String::new()
            };
            let icon = if op.is_kafka() { KAFKA_ICON } else { "" };
            let name = &op.summary.display_name;
            let label = match &op.keyed {
                Some(k) if desc_count > 0 => format!(
                    "⯈ {}{}{} ({}{}, {} states)",
                    icon,
                    name,
                    type_tag,
                    k.count_label(search_percent.filter(|_| self.is_searching(op_idx))),
                    key_type_tag,
                    desc_count
                ),
                _ => format!("⯈ {}{}{}", icon, name, type_tag),
            };
            let label = if op.load_errors() > 0 {
                format!(
                    "{}  {}{} state file(s) not read (i: details)",
                    label,
                    LOAD_ERROR_MARK,
                    op.load_errors()
                )
            } else {
                label
            };

            let op_path = name.clone();
            let mut op_node = FlatNode::new(NodeLevel::Operator, label, op_path.clone(), op_idx);
            op_node.expanded = op_expanded;
            op_node.has_children = desc_count > 0 || !op.states.is_empty();
            nodes.push(op_node);

            if !op_expanded {
                continue;
            }

            if let Some(keyed) = op.keyed.as_ref().filter(|k| !k.descriptors.is_empty()) {
                self.key_rows(op_idx, keyed, &op_path, &style, &mut nodes);
            }

            // Technical operator states (managed-operator) — shown last, dimmed
            for (state_idx, state) in op.states.iter().enumerate() {
                let state_expanded =
                    self.is_expanded(NodeLevel::State, op_idx, Some(state_idx), None, None);
                let int_path = format!("{} > [internal] {}", op_path, state.name);
                let visible = state.visible_count();
                let mut header = FlatNode::new(
                    NodeLevel::State,
                    format!(
                        "  ⯈ {}[internal] {} [{}] ({})",
                        if is_kafka_state(&state.name) {
                            KAFKA_ICON
                        } else {
                            ""
                        },
                        state.name,
                        state.state_type,
                        state.count_label()
                    ),
                    int_path.clone(),
                    op_idx,
                )
                .at(Some(state_idx), None);
                header.expanded = state_expanded;
                header.has_children = visible > 0;
                nodes.push(header);
                if !state_expanded {
                    continue;
                }
                let page_start = state.entry_offset.min(visible);
                let page_end = (page_start + ENTRY_PAGE_SIZE).min(visible);
                let of = visible.to_string();
                let pager = |arrow: char, kind: NodeKind| {
                    FlatNode::new(
                        NodeLevel::Entry,
                        pager_label("    ", arrow, page_start, page_end, &of),
                        int_path.clone(),
                        op_idx,
                    )
                    .kind(kind)
                    .at(Some(state_idx), None)
                };
                if page_start > 0 {
                    nodes.push(pager('↑', NodeKind::PagerUp));
                }
                let elements = &op.summary.states[state_idx].elements;
                for vi in page_start..page_end {
                    // The element's real index, also under a filter.
                    let ri = state.real_index(vi);
                    nodes.push(
                        FlatNode::new(
                            NodeLevel::Entry,
                            format!("    • [{}] {}", ri, format_key_preview(&elements[ri].label)),
                            format!("{}[{}]", int_path, ri),
                            op_idx,
                        )
                        .at(Some(state_idx), Some(ri)),
                    );
                }
                if page_end < visible {
                    nodes.push(pager('↓', NodeKind::PagerDown));
                }
            }
        }

        // Preserve selection
        let selected = self.list_state.selected().unwrap_or(0);
        self.nodes = nodes;
        let max = self.nodes.len().saturating_sub(1);
        self.list_state.select(Some(selected.min(max)));
    }

    /// The rows of an expanded keyed operator: its window of keys between pager rows, each key's
    /// states when it is expanded, and a note per unreadable file or key-group.
    fn key_rows(
        &self,
        op_idx: usize,
        keyed: &KeyedSnapshot,
        op_path: &str,
        style: &ValueStyle,
        nodes: &mut Vec<FlatNode>,
    ) {
        let seq = keyed.seq();
        let known = seq.positions.len();
        let page_start = seq.offset.min(known);
        let page_end = (page_start + PARTITION_PAGE_SIZE).min(known);
        let of = keyed.pager_total();
        let pager = |arrow: char, kind: NodeKind| {
            FlatNode::new(
                NodeLevel::State,
                pager_label("  ", arrow, page_start, page_end, &of),
                op_path.to_string(),
                op_idx,
            )
            .kind(kind)
        };
        if page_start > 0 {
            nodes.push(pager('↑', NodeKind::PagerUp));
        }
        let desc_count = keyed.descriptors.len();
        for &pos in &seq.positions[page_start..page_end] {
            let Some(row) = keyed.rows.get(&pos) else {
                continue; // could not be read: the error is listed below
            };
            let expanded = self.is_expanded(NodeLevel::State, op_idx, None, Some(pos), None);
            let part_path = format!("{} > {}", op_path, row.key);
            let part_label = if self.debug_mode {
                format!(
                    "  ⯈ {} ({} states, kg:{})",
                    row.key, desc_count, row.key_group
                )
            } else {
                format!("  ⯈ {} ({} states)", row.key, desc_count)
            };
            let mut part_node =
                FlatNode::new(NodeLevel::State, part_label, part_path.clone(), op_idx)
                    .of_key(pos, None);
            part_node.expanded = expanded;
            part_node.has_children = desc_count > 0;
            nodes.push(part_node);
            if !expanded {
                continue;
            }
            for (ei, desc) in keyed.descriptors.iter().enumerate() {
                let slot = &row.slots[ei];
                let shown = slot
                    .entries
                    .iter()
                    .find(|e| !e.raw.is_empty())
                    .map(|e| e.raw.as_slice());
                let entry = entry_row(desc, slot, shown, style);
                let entry_expanded = entry.expandable
                    && self.is_expanded(NodeLevel::Entry, op_idx, None, Some(pos), Some(ei));
                // Columns, not bytes: a byte count overstates non-ASCII text, and cutting at one
                // panicked inside a multi-byte character.
                let display_lbl = if !entry_expanded && text::width(&entry.label) > style.max_inline
                {
                    text::ellipsize(&entry.label, style.max_inline, "...").into_owned()
                } else {
                    entry.label
                };
                let entry_path = format!("{} > {}", part_path, desc.name);
                let mut entry_node = FlatNode::new(
                    NodeLevel::Entry,
                    format!(
                        "    {} {}",
                        if entry.expandable { "⯈" } else { "•" },
                        display_lbl
                    ),
                    entry_path.clone(),
                    op_idx,
                )
                .of_key(pos, Some(ei));
                entry_node.expanded = entry_expanded;
                entry_node.has_children = entry.expandable;
                nodes.push(entry_node);

                if let (true, Some(value)) = (entry_expanded, &slot.value) {
                    let mut children = Vec::new();
                    child_rows(desc, value, slot, &entry_path, style, &mut children);
                    nodes.extend(children.into_iter().map(|c| {
                        let mut n = FlatNode::new(NodeLevel::MapEntry, c.label, c.path, op_idx)
                            .kind(c.kind)
                            .of_key(pos, Some(ei));
                        n.item = c.item;
                        n
                    }));
                }
            }
        }
        if page_end < known {
            nodes.push(pager('↓', NodeKind::PagerDown));
        }
        // What could not be read is said where the keys are missing.
        if page_end >= known && !keyed.errors.is_empty() {
            nodes.push(
                FlatNode::new(
                    NodeLevel::State,
                    format!(
                        "  {}{} state file(s) or key-group(s) could not be read: their keys \
                         are missing (i: details)",
                        LOAD_ERROR_MARK,
                        keyed.errors.len()
                    ),
                    op_path.to_string(),
                    op_idx,
                )
                .kind(NodeKind::Marker),
            );
        }
    }

    /// Number of rows in the tree as it is expanded now.
    pub fn row_count(&self) -> usize {
        self.nodes.len()
    }

    /// Operator index of the currently-selected node (for showing operator info).
    pub fn selected_op_idx(&self) -> Option<usize> {
        let i = self.list_state.selected()?;
        self.nodes.get(i).map(|n| n.op_idx)
    }

    /// Whether the node uniquely identified by (level, op, operator state, key, entry) is
    /// expanded. All of them matter: an entry's `entry_idx` is its state index, which repeats
    /// across keys, so without the key a map under a second expanded key would read the first
    /// key's flag.
    fn is_expanded(
        &self,
        level: NodeLevel,
        op_idx: usize,
        state_idx: Option<usize>,
        key: Option<Cursor>,
        entry_idx: Option<usize>,
    ) -> bool {
        self.nodes
            .iter()
            .find(|n| {
                n.level == level
                    && n.kind == NodeKind::Data
                    && n.op_idx == op_idx
                    && n.state_idx == state_idx
                    && n.key == key
                    && n.entry_idx == entry_idx
            })
            .map(|n| n.expanded)
            .unwrap_or(false)
    }

    /// Get the breadcrumb path of the currently selected node.
    pub fn selected_path(&self) -> &str {
        self.list_state
            .selected()
            .and_then(|i| self.nodes.get(i))
            .map(|n| n.path.as_str())
            .unwrap_or("")
    }

    pub fn proto_context(&self) -> Option<&ProtoContext> {
        self.proto_ctx.as_deref()
    }

    /// Decode protobuf-in-POJO values with `ctx`: the reader decodes values, so a new one is made
    /// over the same savepoint (its headers are read again, nothing else), and the loaded keys
    /// are read again through it.
    pub fn set_proto_context(&mut self, ctx: Arc<ProtoContext>) {
        let reader = StateReader::from_savepoint(
            self.reader.savepoint().clone(),
            self.reader.savepoint_dir(),
        )
        .with_proto(ctx.clone());
        self.set_reader(Arc::new(reader));
        self.proto_ctx = Some(ctx);
        self.rebuild_flat_list();
    }

    /// Record that the reader the tree was made with already decodes with `ctx`.
    pub fn note_proto_context(&mut self, ctx: Arc<ProtoContext>) {
        self.proto_ctx = Some(ctx);
    }

    /// Read from `reader` from now on (over the same savepoint): the loaded keys are dropped, to
    /// be read again.
    pub fn set_reader(&mut self, reader: Arc<StateReader<'static>>) {
        self.reader = reader;
        for op in &mut self.operators {
            if let Some(k) = op.keyed.as_mut() {
                k.rows.clear();
            }
        }
    }

    /// How many keys have been received from the reader since this view was created (the reader
    /// decodes each one's values once; a rebuild of the rows reads none).
    #[doc(hidden)]
    pub fn decode_calls(&self) -> usize {
        self.fetched
    }

    pub fn toggle_hide_empty(&mut self) {
        self.hide_empty = !self.hide_empty;
        self.rebuild_flat_list();
    }

    pub fn toggle_debug(&mut self) {
        self.debug_mode = !self.debug_mode;
        self.rebuild_flat_list();
    }

    pub fn toggle_auto_date(&mut self) {
        self.auto_date = !self.auto_date;
        self.rebuild_flat_list();
    }

    /// Scroll the window a pager row belongs to by up to `amount` items, keeping the cursor where
    /// it is: the keys of its operator, or the elements of its operator state. Returns how far it
    /// scrolled.
    fn scroll(&mut self, pager_idx: usize, forward: bool, amount: usize) -> usize {
        let selected_before = self.list_state.selected().unwrap_or(0);
        let (op_idx, state_idx) = {
            let n = &self.nodes[pager_idx];
            (n.op_idx, n.state_idx)
        };
        let Some(op) = self.operators.get_mut(op_idx) else {
            return 0;
        };
        let (offset, visible, page) = match state_idx {
            None => match &mut op.keyed {
                Some(k) => {
                    let seq = k.seq_mut();
                    let visible = seq.positions.len();
                    (&mut seq.offset, visible, PARTITION_PAGE_SIZE)
                }
                None => return 0,
            },
            Some(si) => match op.states.get_mut(si) {
                Some(s) => {
                    let visible = s.visible_count();
                    (&mut s.entry_offset, visible, ENTRY_PAGE_SIZE)
                }
                None => return 0,
            },
        };
        let before = *offset;
        *offset = if forward {
            before.max((before + amount).min(visible.saturating_sub(page)))
        } else {
            before.saturating_sub(amount)
        };
        let moved = before.abs_diff(*offset);

        self.rebuild_flat_list();
        // Keep cursor at same visual position
        self.list_state.select(Some(
            selected_before.min(self.nodes.len().saturating_sub(1)),
        ));
        moved
    }

    // --- filter and search ---------------------------------------------------------------------

    /// Apply a filter to the keys and the operator-state elements. Operator-state elements are
    /// filtered at once; keys are looked up (`get`, for an exact key of a hashable type), sought
    /// (a key-group range) or searched (a pattern): a search goes on with
    /// [`TreeView::search_step`].
    pub fn apply_filter(&mut self, filter: &Option<AppliedFilter>) {
        self.search = None;
        let reader = self.reader.clone();
        let mut queue = std::collections::VecDeque::new();
        let mut found = 0;
        for (op_idx, op) in self.operators.iter_mut().enumerate() {
            for (si, state) in op.states.iter_mut().enumerate() {
                state.entry_offset = 0;
                let elements = &op.summary.states[si].elements;
                state.filtered_indices = filter.as_ref().map(|f| {
                    elements
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| f.matches_str(&e.label, 0))
                        .map(|(i, _)| i)
                        .collect()
                });
            }
            let Some(k) = op.keyed.as_mut() else {
                continue;
            };
            k.all.offset = 0;
            k.filtered = None;
            let Some(f) = filter else {
                continue;
            };
            if k.descriptors.is_empty() {
                continue;
            }
            let mut seq = KeySeq::browse(Cursor::START);
            match f {
                AppliedFilter::KeyGroupRange { start, end } => {
                    seq.stop_kg = Some(*end);
                    seq.start_kg = *start;
                    let summary = &op.summary;
                    let subtask = crate::parser::key_group::subtask_for_key_group(
                        summary.max_parallelism,
                        summary.parallelism,
                        *start as i32,
                    );
                    let from = Cursor {
                        subtask,
                        key_group: *start,
                        ordinal: 0,
                    };
                    // Seek the range's start when its cursor fits; else page from the first key.
                    if reader.key_page(&k.op_hex, &from, 1).is_ok() {
                        seq.next = Some(from);
                    }
                }
                AppliedFilter::Exact(bytes) => {
                    seq.matches = true;
                    let text = String::from_utf8_lossy(bytes).into_owned();
                    match reader.get(&k.op_hex, &text) {
                        Ok(Lookup::Found { record, .. }) => {
                            seq.push_match(record.position);
                            k.rows
                                .insert(record.position, KeyRow::new(record, &k.descriptors));
                            found += 1;
                            seq.next = None;
                        }
                        Ok(Lookup::Absent { .. }) => seq.next = None,
                        // Not hashable here (a POJO key, …): scan for it.
                        Ok(Lookup::NeedsScan { .. }) => queue.push_back(op_idx),
                        Err(e) => {
                            k.note_error(e);
                            seq.next = None;
                        }
                    }
                }
                AppliedFilter::Pattern(p) => {
                    seq.matches = true;
                    // No wildcard: the key itself is most likely what was typed, and `get` finds
                    // it at once; the scan then adds the other cases (the pattern ignores case).
                    if !p.contains(['*', '?']) {
                        if let Ok(Lookup::Found { record, .. }) = reader.get(&k.op_hex, p) {
                            seq.push_match(record.position);
                            k.rows
                                .insert(record.position, KeyRow::new(record, &k.descriptors));
                            found += 1;
                        }
                    }
                    queue.push_back(op_idx);
                }
            }
            k.filtered = Some(seq);
        }
        self.fetched += found;
        if let Some(f) = filter {
            if !queue.is_empty() {
                self.search = Some(Search {
                    filter: f.clone(),
                    total_ops: queue.len(),
                    queue,
                });
            }
        }
        self.rebuild_flat_list();
    }

    /// Whether a search is still running.
    pub fn searching(&self) -> bool {
        self.search.is_some()
    }

    fn is_searching(&self, op_idx: usize) -> bool {
        self.search
            .as_ref()
            .is_some_and(|s| s.queue.contains(&op_idx))
    }

    /// How far the search is, in percent: operators done, and key-groups of the current one.
    pub fn search_percent(&self) -> Option<u8> {
        let s = self.search.as_ref()?;
        let done = s.total_ops - s.queue.len();
        let current = s.queue.front().and_then(|&op| {
            let o = &self.operators[op];
            let next = o.keyed.as_ref()?.filtered.as_ref()?.next?;
            Some(next.key_group as f64 / o.summary.max_parallelism.max(1) as f64)
        });
        let pct = (done as f64 + current.unwrap_or(0.0)) / s.total_ops.max(1) as f64 * 100.0;
        Some(pct.clamp(0.0, 99.0) as u8)
    }

    /// Stop the search: the matches found so far stay, marked as a lower bound.
    pub fn cancel_search(&mut self) {
        if self.search.take().is_some() {
            self.rebuild_flat_list();
        }
    }

    /// Scan about `byte_budget` more of the savepoint for the filter (at least one key-group).
    /// Returns whether the search goes on.
    pub fn search_step(&mut self, byte_budget: u64) -> bool {
        let Some(search) = self.search.as_mut() else {
            return false;
        };
        let Some(&op_idx) = search.queue.front() else {
            self.search = None;
            return false;
        };
        let filter = search.filter.clone();
        let reader = self.reader.clone();
        let k = self.operators[op_idx]
            .keyed
            .as_mut()
            .expect("searched operators are keyed");
        let seq = k
            .filtered
            .as_mut()
            .expect("searched operators are filtered");
        let mut finished = true;
        if let Some(from) = seq.next {
            let exact = match &filter {
                AppliedFilter::Exact(b) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            };
            let mut matches = |key: &str| match &exact {
                Some(e) => key == e,
                None => filter.matches_str(key, 0),
            };
            match reader.scan(&k.op_hex, &from, byte_budget, &mut matches) {
                Ok(page) => {
                    k.note_page(&page.unreadable, &page.malformed);
                    let seq = k.filtered.as_mut().expect("filtered");
                    let window = seq.offset..seq.offset + 2 * PARTITION_PAGE_SIZE;
                    for record in page.matches {
                        let pos = record.position;
                        let before = seq.positions.len();
                        seq.push_match(pos);
                        // The first windows are shown as the matches come: keep their rows.
                        if seq.positions.len() > before && window.contains(&before) {
                            k.rows.insert(pos, KeyRow::new(record, &k.descriptors));
                            self.fetched += 1;
                        }
                    }
                    seq.next = page.next;
                    finished = seq.next.is_none();
                }
                Err(e) => {
                    k.note_error(e);
                    k.filtered.as_mut().expect("filtered").next = None;
                }
            }
        }
        if finished {
            let s = self.search.as_mut().expect("searching");
            s.queue.pop_front();
            if s.queue.is_empty() {
                self.search = None;
            }
        }
        self.rebuild_flat_list();
        self.search.is_some()
    }

    /// Run the search to its end (tests, and headless use).
    pub fn finish_search(&mut self) {
        while self.search_step(SEARCH_STEP_BYTES) {}
    }

    // --- navigation ----------------------------------------------------------------------------

    pub fn move_up(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected == 0 {
                return;
            }
            if self.nodes[selected - 1].kind == NodeKind::PagerUp {
                // Scroll backward — cursor stays on same visual position
                self.scroll(selected - 1, false, 1);
            } else {
                self.list_state.select(Some(selected - 1));
            }
        }
    }

    pub fn move_down(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected + 1 >= self.nodes.len() {
                return;
            }
            if self.nodes[selected + 1].kind == NodeKind::PagerDown {
                // Scroll forward — cursor stays on same visual position
                self.scroll(selected + 1, true, 1);
            } else {
                self.list_state.select(Some(selected + 1));
            }
        }
    }

    /// `PageDown`: the cursor moves down a page of rows (the list's height); where it meets a `↓`
    /// pager row, that window scrolls forward instead, by what is left of the page.
    pub fn page_down(&mut self) {
        self.page(true);
    }

    /// `PageUp`: the cursor moves up a page of rows; where it meets a `↑` pager row, that window
    /// scrolls back instead, by what is left of the page.
    pub fn page_up(&mut self) {
        self.page(false);
    }

    fn page(&mut self, forward: bool) {
        let pager = if forward {
            NodeKind::PagerDown
        } else {
            NodeKind::PagerUp
        };
        let mut left = self.page_rows.max(1);
        while left > 0 {
            let Some(selected) = self.list_state.selected() else {
                return;
            };
            let next = if forward {
                selected + 1
            } else if selected > 0 {
                selected - 1
            } else {
                return;
            };
            let Some(node) = self.nodes.get(next) else {
                return;
            };
            if node.kind == pager {
                match self.scroll(next, forward, left) {
                    0 => return,
                    moved => left -= moved,
                }
            } else {
                self.list_state.select(Some(next));
                left -= 1;
            }
        }
    }

    /// Expand the selected node. Returns `false` on a leaf, which has nothing to open: the
    /// caller shows the bytes behind it ([`TreeView::selected_raw_bytes`]) instead.
    pub fn expand(&mut self) -> bool {
        let Some(selected) = self.list_state.selected() else {
            return false;
        };
        match self.nodes.get(selected) {
            Some(node) if node.has_children => {
                self.nodes[selected].expanded = true;
                self.rebuild_flat_list();
                true
            }
            _ => false,
        }
    }

    /// The bytes behind the selected row, for the hex overlay: a keyed value (from its state row
    /// or any row under it; under a MAP, the entry the row shows) or an operator-state element.
    /// `None` on any other row. Keyed bytes are the entry's as the reader returned them.
    pub fn selected_raw_bytes(&self) -> Option<RawBytes> {
        let node = self.nodes.get(self.list_state.selected()?)?;
        if node.kind != NodeKind::Data
            || !matches!(node.level, NodeLevel::Entry | NodeLevel::MapEntry)
        {
            return None;
        }
        let op = self.operators.get(node.op_idx)?;
        if let (Some(si), Some(ei)) = (node.state_idx, node.entry_idx) {
            let state = op.summary.states.get(si)?;
            let element = state.elements.get(ei)?;
            return Some(RawBytes {
                key_title: format!("{}[{}], as decoded", state.name, ei),
                key: element.label.clone().into_bytes(),
                value_title: match element.bytes {
                    Some(_) => format!("element {}[{}]", state.name, ei),
                    None => format!(
                        "no bytes: the element could not be read ({})",
                        element.label
                    ),
                },
                value: element.bytes.clone().unwrap_or_default(),
            });
        }
        let (pos, ei) = (node.key?, node.entry_idx?);
        let keyed = op.keyed.as_ref()?;
        let row = keyed.rows.get(&pos)?;
        let desc = keyed.descriptors.get(ei)?;
        let slot = row.slots.get(ei)?;
        let key_title = format!(
            "key {} (as rendered: the reader keeps no raw key bytes)",
            row.key
        );
        if desc.state_type == "MAP" {
            let i = node.item.unwrap_or(0);
            let Some(e) = slot.entries.get(i) else {
                return Some(RawBytes {
                    key_title,
                    key: row.key.clone().into_bytes(),
                    value_title: format!("{}: no value for this key", desc.name),
                    value: Vec::new(),
                });
            };
            // One entry of the map: its namespace holds the serialized map key.
            return Some(RawBytes {
                key_title: format!(
                    "{}[{}]: namespace + map key",
                    desc.name,
                    e.map_key.as_deref().unwrap_or("?")
                ),
                key: e.namespace.clone(),
                value_title: format!(
                    "{} [{}]: entry {} of {}",
                    desc.name,
                    desc.value_type,
                    i + 1,
                    slot.total
                ),
                value: e.raw.clone(),
            });
        }
        let value = slot.entries.iter().find(|e| !e.raw.is_empty());
        Some(RawBytes {
            key_title,
            key: row.key.clone().into_bytes(),
            value_title: match value {
                None => format!("{}: no value for this key", desc.name),
                Some(_) => format!("{} [{}]", desc.name, desc.value_type),
            },
            value: value.map(|e| e.raw.clone()).unwrap_or_default(),
        })
    }

    pub fn collapse(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected < self.nodes.len() {
                let node = &self.nodes[selected];
                if node.expanded {
                    self.nodes[selected].expanded = false;
                    self.rebuild_flat_list();
                } else {
                    // Move to parent
                    let (parent_level, entry_idx) = match node.level {
                        NodeLevel::MapEntry => (NodeLevel::Entry, node.entry_idx),
                        NodeLevel::Entry if node.key.is_some() => (NodeLevel::State, None),
                        NodeLevel::Entry => (NodeLevel::State, None),
                        NodeLevel::State => (NodeLevel::Operator, None),
                        NodeLevel::Operator => return,
                    };
                    let (state_idx, key) = match parent_level {
                        NodeLevel::Operator => (None, None),
                        _ => (node.state_idx, node.key),
                    };
                    let op_idx = node.op_idx;
                    if let Some(i) = self.nodes.iter().position(|n| {
                        n.level == parent_level
                            && n.kind == NodeKind::Data
                            && n.op_idx == op_idx
                            && n.state_idx == state_idx
                            && n.key == key
                            && n.entry_idx == entry_idx
                    }) {
                        self.list_state.select(Some(i));
                    }
                }
            }
        }
    }

    pub fn jump_first(&mut self) {
        if !self.nodes.is_empty() {
            self.list_state.select(Some(0));
        }
    }

    /// The last data row: never a `↓` pager row (stepping onto one scrolls) nor a note.
    pub fn jump_last(&mut self) {
        if let Some(last) = self.nodes.iter().rposition(|n| n.kind == NodeKind::Data) {
            self.list_state.select(Some(last));
        }
    }

    pub fn render(&mut self, frame: &mut Frame, block: Block, area: Rect) {
        self.page_rows = (block.inner(area).height as usize).max(1);
        if area.width != self.display_width {
            self.display_width = area.width;
            self.rebuild_flat_list();
        }
        let items: Vec<ListItem> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let style = if Some(i) == self.list_state.selected() {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else if node.kind != NodeKind::Data {
                    Style::default().fg(Color::DarkGray)
                } else {
                    match node.level {
                        NodeLevel::Operator if self.operators[node.op_idx].load_errors() > 0 => {
                            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                        }
                        NodeLevel::Operator => Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                        NodeLevel::State => Style::default().fg(Color::Green),
                        // Dim empty collections (e.g. a MapState with no entries) so they
                        // read as "nothing to expand" at a glance.
                        NodeLevel::Entry if node.label.ends_with(": empty") => {
                            Style::default().fg(Color::DarkGray)
                        }
                        NodeLevel::Entry => Style::default().fg(Color::White),
                        NodeLevel::MapEntry => Style::default().fg(Color::Gray),
                    }
                };
                ListItem::new(Line::from(node.label.clone())).style(style)
            })
            .collect();

        let list = List::new(items).block(block).highlight_symbol("▶ ");

        let mut state = self.list_state;
        frame.render_stateful_widget(list, area, &mut state);
    }
}

/// An operator's keyed state as its headers describe it (no key is read).
fn keyed_snapshot(reader: &StateReader, summary: &OperatorSummary) -> Option<KeyedSnapshot> {
    let hex = summary.operator_id_hex();
    let states = reader.states(&hex).ok()?;
    if states.states.is_empty() && states.errors.is_empty() {
        return None;
    }
    let descriptors = states
        .states
        .iter()
        .map(|s| {
            let pojo_detail = reader
                .value_pojo(&hex, &s.name)
                .ok()
                .flatten()
                .map(|p| {
                    let fields: Vec<String> = p
                        .leaf_fields("")
                        .iter()
                        .map(|(path, t)| format!("{}: {}", path, t))
                        .collect();
                    // The fully-qualified value class (package + name), then the fields.
                    format!(" → {} {{ {} }}", p.class_name, fields.join(", "))
                })
                .unwrap_or_default();
            StateDesc {
                name: s.name.clone(),
                state_type: s.state_type.clone(),
                value_type: s.value_type.clone(),
                pojo_detail,
            }
        })
        .collect();
    Some(KeyedSnapshot {
        op_hex: hex,
        key_type: states.key_type,
        descriptors,
        errors: states.errors,
        malformed: Vec::new(),
        count: KeyCount::Unknown,
        all: KeySeq::browse(Cursor::START),
        filtered: None,
        rows: HashMap::new(),
    })
}

// --- presentation of decoded values ---------------------------------------------------------

/// How values are presented: none of it changes what is decoded.
struct ValueStyle {
    debug: bool,
    auto_date: bool,
    /// Columns a state row may take before it is cut (and, for a structure, made expandable).
    max_inline: usize,
}

impl ValueStyle {
    /// A decoded value of serializer type `type_name`, as displayed: [`value_to_display`], except
    /// that with `auto_date` a `Long` in the epoch-millis range of 2000–2040 shows as a date.
    fn show(&self, v: &Value, type_name: &str) -> String {
        if self.auto_date && type_name == "Long" {
            if let Some(ms) = v.as_i64().filter(|ms| AUTO_DATE_MILLIS.contains(ms)) {
                return millis_to_iso(ms);
            }
        }
        value_to_display(v)
    }
}

/// A keyed state's row: its label and whether rows open under it.
struct EntryRow {
    label: String,
    expandable: bool,
}

/// One row under an expanded keyed state.
struct ChildRow {
    label: String,
    path: String,
    kind: NodeKind,
    /// Under a MAP: the entry it shows.
    item: Option<usize>,
}

/// The row of one (key, state). `raw` is the shown entry's bytes (debug suffix).
fn entry_row(desc: &StateDesc, slot: &Slot, raw: Option<&[u8]>, style: &ValueStyle) -> EntryRow {
    let value = slot.value.as_ref();
    let collection = |prefix: &str, fallback: &str, unit: &str, items: Option<usize>| {
        let type_label = if desc.value_type.starts_with(prefix) {
            desc.value_type.as_str()
        } else {
            fallback
        };
        let summary = match (value, items) {
            (None, _) if style.debug => "empty (no data)".to_string(),
            (None, _) | (Some(_), Some(0)) => "empty".to_string(),
            (Some(_), Some(n)) => format!("{} {}", n, unit),
            // Not a map/list: a marker saying why.
            (Some(v), None) => value_to_display(v),
        };
        EntryRow {
            label: format!(
                "{} [{}]{}: {}",
                desc.name, type_label, desc.pojo_detail, summary
            ),
            expandable: items.is_some_and(|n| n > 0),
        }
    };
    match desc.state_type.as_str() {
        "MAP" => collection(
            "Map<",
            "MAP",
            "entries",
            // A map the reader cut at its entry limit has `total` entries all the same.
            value
                .and_then(Value::as_object)
                .map(|m| m.len().max(slot.total)),
        ),
        "LIST" => collection(
            "List<",
            "LIST",
            "elements",
            value.and_then(Value::as_array).map(Vec::len),
        ),
        state_type => {
            let shown =
                value.map_or_else(|| "null".to_string(), |v| style.show(v, &desc.value_type));
            let label = match (style.debug, raw) {
                (true, Some(raw)) => format!(
                    "{} [{}]: {} (hex: {})",
                    desc.name,
                    state_type,
                    shown,
                    hex_preview(raw, 16)
                ),
                (true, None) => format!("{} [{}]: {}", desc.name, state_type, shown),
                (false, _) => format!("{}: {}", desc.name, shown),
            };
            // A structure too wide for its row opens into one row per field / element.
            let structured = match value {
                Some(Value::Object(m)) => !m.is_empty(),
                Some(Value::Array(a)) => !a.is_empty(),
                _ => false,
            };
            let expandable = structured && text::width(&label) > style.max_inline;
            EntryRow { label, expandable }
        }
    }
}

/// The rows under an expanded (key, state): a MAP's entries, a LIST's elements, a structure's
/// fields or elements — all read off the decoded `value`.
fn child_rows(
    desc: &StateDesc,
    value: &Value,
    slot: &Slot,
    path: &str,
    style: &ValueStyle,
    out: &mut Vec<ChildRow>,
) {
    const INDENT: &str = "        ";
    match (desc.state_type.as_str(), value) {
        ("MAP", Value::Object(map)) => {
            let (_, val_type) = split_map_types(&desc.value_type);
            for (k, v) in map.iter().take(MAP_DISPLAY_LIMIT) {
                out.push(ChildRow {
                    label: format!("{}{} : {}", INDENT, k, style.show(v, &val_type)),
                    path: format!("{}[{}]", path, k),
                    kind: NodeKind::Data,
                    // The last entry with that map key: the one the object kept.
                    item: slot
                        .entries
                        .iter()
                        .rposition(|e| e.map_key.as_deref() == Some(k.as_str())),
                });
            }
            more_row(map.len(), MAP_DISPLAY_LIMIT, INDENT, path, out);
        }
        ("LIST", Value::Array(items)) => {
            let elem_type = desc
                .value_type
                .strip_prefix("List<")
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or(&desc.value_type);
            for (i, v) in items.iter().take(LIST_DISPLAY_LIMIT).enumerate() {
                out.push(ChildRow {
                    label: format!("{}[{}] {}", INDENT, i, style.show(v, elem_type)),
                    path: format!("{}[{}]", path, i),
                    kind: NodeKind::Data,
                    item: None,
                });
            }
            more_row(items.len(), LIST_DISPLAY_LIMIT, INDENT, path, out);
        }
        (_, v) => structure_rows(v, path, 8, out),
    }
}

/// One row per field of an object, or per element of an array; a nested structure too wide for
/// one row gets a heading row and rows of its own, indented.
fn structure_rows(value: &Value, path: &str, indent: usize, out: &mut Vec<ChildRow>) {
    let pad = " ".repeat(indent);
    let mut field = |name: String, child_path: String, v: &Value| {
        let shown = value_to_display(v);
        let nested = matches!(v, Value::Object(m) if !m.is_empty())
            || matches!(v, Value::Array(a) if !a.is_empty());
        if nested && text::width(&shown) > NESTED_SPLIT_WIDTH {
            out.push(ChildRow {
                label: format!("{}{}:", pad, name),
                path: child_path.clone(),
                kind: NodeKind::Data,
                item: None,
            });
            structure_rows(v, &child_path, indent + 2, out);
        } else {
            out.push(ChildRow {
                label: format!("{}{}: {}", pad, name, shown),
                path: child_path,
                kind: NodeKind::Data,
                item: None,
            });
        }
    };
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                field(k.clone(), format!("{}.{}", path, k), v);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().take(LIST_DISPLAY_LIMIT).enumerate() {
                field(format!("[{}]", i), format!("{}[{}]", path, i), v);
            }
            more_row(items.len(), LIST_DISPLAY_LIMIT, &pad, path, out);
        }
        _ => {}
    }
}

/// The "… +N more" row after the first `limit` of `total` rows.
fn more_row(total: usize, limit: usize, indent: &str, path: &str, out: &mut Vec<ChildRow>) {
    if total > limit {
        out.push(ChildRow {
            label: format!("{}… +{} more", indent, total - limit),
            path: path.to_string(),
            kind: NodeKind::Marker,
            item: None,
        });
    }
}

fn hex_preview(bytes: &[u8], max: usize) -> String {
    bytes
        .iter()
        .take(max)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Shorten Flink operator identifier to a readable type tag.
/// e.g. "KeyedBroadcastProcessFunction" → "KeyedBroadcastProcess"
///      "org.example.MyRichFlatMapFunction" → "RichFlatMap"
fn shorten_operator_type(identifier: &str) -> String {
    if identifier.is_empty() {
        return String::new();
    }
    // Take last segment after '.' (strip package)
    let short = identifier.rsplit('.').next().unwrap_or(identifier);
    // Strip common suffixes
    short
        .strip_suffix("Function")
        .or_else(|| short.strip_suffix("Operator"))
        .unwrap_or(short)
        .to_string()
}

fn format_key_preview(label: &str) -> String {
    if label.chars().count() <= 120 {
        label.to_string()
    } else {
        let truncated: String = label.chars().take(117).collect();
        format!("{}…", truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn labels(value: &Value) -> Vec<String> {
        let mut out = Vec::new();
        structure_rows(value, "p", 8, &mut out);
        out.into_iter().map(|r| r.label).collect()
    }

    #[test]
    fn expanded_fields_show_the_decoder_markers_unquoted() {
        let v = json!({ "c": "A", "k": "[Kryo]", "z": "[not decoded: follows k]" });
        assert_eq!(
            labels(&v),
            [
                "        c: \"A\"",
                "        k: [Kryo]",
                "        z: [not decoded: follows k]",
            ]
        );
    }

    #[test]
    fn a_wide_nested_structure_gets_rows_of_its_own() {
        let long = "x".repeat(70);
        let v = json!({ "a": 1, "n": { "s": long, "t": [1, 2] } });
        assert_eq!(
            labels(&v),
            [
                "        a: 1".to_string(),
                "        n:".to_string(),
                format!("          s: \"{}\"", long),
                "          t: [1, 2]".to_string(),
            ]
        );
    }
}
