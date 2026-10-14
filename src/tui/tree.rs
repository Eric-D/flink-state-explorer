//! The savepoint tree: operators → keys → states → values, plus operator states.
//!
//! Every value shown here comes from [`crate::parser::value::decode_state_value`], rendered by
//! [`value_to_display`] — the same decoding the JSON and `--export-state` exports use. This module
//! decodes no bytes itself: a MAP's rows are the entries of the decoded object, a LIST's rows the
//! elements of the decoded array, a POJO's rows its decoded fields. What it adds is presentation:
//! display limits, `auto_date` (an epoch-millis `Long` shown as a date) and the debug hex suffix.

use ratatui::prelude::*;
use ratatui::widgets::{Block, List, ListItem, ListState};
use serde_json::Value;

use std::sync::Arc;

use crate::index::cache::{IndexCache, StateDescriptor, ValueRef};
use crate::parser::pojo::{millis_to_iso, PojoInfo};
use crate::parser::value::{decode_state_value, split_map_types, value_to_display};
use crate::proto::ProtoContext;
use crate::tui::filter::AppliedFilter;
use crate::tui::text;

/// Operator-state elements listed at once; past it, `↑`/`↓` pager rows scroll the window.
const ENTRY_PAGE_SIZE: usize = 500;
/// Maximum number of partitions (keys) to display at once.
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
    /// `↑ a-b / n`: moving onto it scrolls its window back (keyed partitions when `state_idx` is
    /// `None`, else the elements of operator state `state_idx`).
    PagerUp,
    /// `↓ a-b / n`: moving onto it scrolls its window forward.
    PagerDown,
    /// A note, not data: "… +N more", keys not indexed.
    Marker,
}

#[derive(Debug, Clone)]
struct FlatNode {
    level: NodeLevel,
    kind: NodeKind,
    label: String,
    /// Breadcrumb path for status bar display
    path: String,
    /// Index into the parent collection
    op_idx: usize,
    /// An operator state's index, or `op.states.len() + partition index` for a key.
    state_idx: Option<usize>,
    /// A key's descriptor index, or an operator-state element's index in its state.
    entry_idx: Option<usize>,
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
            entry_idx: None,
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
}

/// The bytes behind the selected row, for the hex overlay (`x`).
#[derive(Debug, Clone, PartialEq)]
pub struct RawBytes {
    pub key_title: String,
    pub key: Vec<u8>,
    pub value_title: String,
    pub value: Vec<u8>,
}

pub struct TreeView {
    nodes: Vec<FlatNode>,
    list_state: ListState,
    operators: Vec<OperatorSnapshot>,
    pub hide_empty: bool,
    pub debug_mode: bool,
    /// Show a `Long` value that looks like epoch millis as a date
    pub auto_date: bool,
    display_width: u16,
    proto_ctx: Option<Arc<ProtoContext>>,
}

struct OperatorSnapshot {
    display_name: String,
    /// Short operator type (e.g. "KeyedBroadcastProcess", "RichFlatMap")
    operator_type: String,
    /// Non-keyed operator states
    states: Vec<StateSnapshot>,
    /// Structured keyed state
    keyed: Option<KeyedSnapshot>,
    /// State files of this operator that could not be read: its key set is partial.
    load_errors: usize,
}

impl OperatorSnapshot {
    /// Count user-visible children: keyed partitions (filtered count if a filter is active) **plus**
    /// non-keyed operator states. Operators with only operator state (e.g. a Kafka source/sink:
    /// reader splits, writer/committer states) have no keyed partitions, so they must be counted
    /// here too — otherwise `hide_empty` would hide them and their decoded state entirely.
    fn child_count(&self) -> usize {
        self.keyed.as_ref().map_or(0, |k| {
            // A capped operator stays listed under a filter that matched none of its stored keys:
            // the key may be among the ones not indexed, and the row saying so must be reachable.
            let unsearched = (k.filtered_indices.is_some() && k.hidden_keys() > 0) as usize;
            k.visible_count() + unsearched
        }) + self.states.len()
    }

    /// A Kafka connector operator (source or sink), detected from its operator-state names.
    fn is_kafka(&self) -> bool {
        self.states.iter().any(|s| is_kafka_state(&s.name))
    }
}

impl KeyedSnapshot {
    /// Number of partitions that can be browsed: the filter's matches, or every **stored**
    /// partition. Never the savepoint's key count: past the index cap, the keys beyond
    /// `partitions` are not in memory, and paging up to that count indexed past the end.
    fn visible_count(&self) -> usize {
        match &self.filtered_indices {
            Some(indices) => indices.len(),
            None => self.partitions.len(),
        }
    }

    /// Keys of the savepoint that the index did not keep (0 below the cap).
    fn hidden_keys(&self) -> usize {
        self.total_partitions.saturating_sub(self.partitions.len())
    }

    /// The key count on the operator row. Past the cap it gives both numbers, and with a filter
    /// it says the filter searched only the stored keys.
    fn count_label(&self) -> String {
        let visible = self.visible_count();
        let (stored, total) = (self.partitions.len(), self.total_partitions);
        match (&self.filtered_indices, self.hidden_keys() > 0) {
            (Some(_), true) => format!("{} match in {} of {} keys shown", visible, stored, total),
            (None, true) => format!("{} of {} keys shown", stored, total),
            _ if visible != total => format!("{}/{} keys", visible, total),
            _ => format!("{} keys", total),
        }
    }

    /// Appended to the pager rows past the cap, so that `n / stored` is not read as the whole.
    fn pager_note(&self) -> String {
        if self.hidden_keys() == 0 {
            String::new()
        } else if self.filtered_indices.is_some() {
            format!(
                " (filter searched {} of {} keys)",
                self.partitions.len(),
                self.total_partitions
            )
        } else {
            format!(
                " ({} of {} keys shown)",
                self.partitions.len(),
                self.total_partitions
            )
        }
    }
}

/// Keyed state snapshot for display.
struct KeyedSnapshot {
    descriptors: Vec<StateDescriptor>,
    /// Each descriptor's POJO layout, converted once for the decoder.
    pojo_infos: Vec<Option<PojoInfo>>,
    /// The keys the index stored: at most `index::builder::MAX_STORED_PARTITIONS`.
    partitions: Vec<PartitionSnapshot>,
    /// The savepoint's key count, which may exceed `partitions.len()`.
    total_partitions: usize,
    partition_offset: usize,
    /// Key serializer type (e.g. "String", "Pojo")
    key_type: String,
    /// Indices into `partitions` that match the current filter (None = no filter, show all).
    filtered_indices: Option<Vec<usize>>,
}

struct PartitionSnapshot {
    key: String,
    key_group: u16,
    /// Raw value bytes per descriptor, as the index stores them (a MAP's is the per-key blob
    /// `index::builder` assembles). Decoded when a row is built, with the proto context.
    entry_raw: Vec<Option<Vec<u8>>>,
}

/// One operator-state element.
struct OpElement {
    /// The decoded element, as the index labelled it.
    label: Vec<u8>,
    key_group: u16,
    /// The element's bytes; `None` when it could not be read (the label says why).
    bytes: Option<Vec<u8>>,
}

struct StateSnapshot {
    name: String,
    state_type: String,
    elements: Vec<OpElement>,
    /// Indices into `elements` that match the current filter (None = no filter, show all).
    filtered_indices: Option<Vec<usize>>,
    /// First visible element (an index into the filtered list), moved by the pager rows.
    entry_offset: usize,
}

impl StateSnapshot {
    fn visible_count(&self) -> usize {
        self.filtered_indices
            .as_ref()
            .map_or(self.elements.len(), Vec::len)
    }

    /// The element index of the `vi`-th visible element.
    fn real_index(&self, vi: usize) -> usize {
        self.filtered_indices.as_ref().map_or(vi, |f| f[vi])
    }

    fn count_label(&self) -> String {
        let (visible, total) = (self.visible_count(), self.elements.len());
        if visible == total {
            format!("{} entries", total)
        } else {
            format!("{}/{} entries", visible, total)
        }
    }
}

/// A pager row: `  ↑ 31-60 / 500`.
fn pager_label(indent: &str, arrow: char, start: usize, end: usize, of: usize) -> String {
    format!("{}{} {}-{} / {}", indent, arrow, start + 1, end, of)
}

impl TreeView {
    pub fn new(index: &IndexCache) -> Self {
        let operators: Vec<OperatorSnapshot> = index
            .operators
            .iter()
            .map(|op| {
                // Non-keyed operator states
                let states: Vec<StateSnapshot> = op
                    .states
                    .iter()
                    .map(|s| StateSnapshot {
                        name: s.name.clone(),
                        state_type: s.state_type.clone(),
                        elements: s
                            .entries
                            .iter()
                            .map(|e| OpElement {
                                label: e.key.clone(),
                                key_group: e.key_group,
                                bytes: match &e.value_ref {
                                    ValueRef::Inline { data } => Some(data.clone()),
                                    ValueRef::File { .. } | ValueRef::Decoded => None,
                                },
                            })
                            .collect(),
                        filtered_indices: None,
                        entry_offset: 0,
                    })
                    .collect();

                // Structured keyed state
                let keyed = op.keyed_state.as_ref().map(|ks| KeyedSnapshot {
                    descriptors: ks.descriptors.clone(),
                    pojo_infos: ks
                        .descriptors
                        .iter()
                        .map(|d| d.pojo_info.as_ref().map(|p| p.to_pojo_info()))
                        .collect(),
                    partitions: ks
                        .partitions
                        .iter()
                        .map(|p| PartitionSnapshot {
                            key: p.key.clone(),
                            key_group: p.key_group,
                            entry_raw: p.values.clone(),
                        })
                        .collect(),
                    total_partitions: ks.total_keys,
                    partition_offset: 0,
                    key_type: ks.key_type.clone(),
                    filtered_indices: None,
                });

                OperatorSnapshot {
                    display_name: op.display_name.clone(),
                    operator_type: shorten_operator_type(&op.identifier_name),
                    states,
                    keyed,
                    load_errors: op.load_errors.len(),
                }
            })
            .collect();

        let mut tv = Self {
            nodes: Vec::new(),
            list_state: ListState::default(),
            operators,
            hide_empty: true,
            debug_mode: false,
            auto_date: true,
            display_width: 120,
            proto_ctx: None,
        };
        tv.rebuild_flat_list();
        if !tv.nodes.is_empty() {
            tv.list_state.select(Some(0));
        }
        tv
    }

    fn rebuild_flat_list(&mut self) {
        let mut nodes = Vec::new();
        let style = ValueStyle {
            proto: self.proto_ctx.as_deref(),
            debug: self.debug_mode,
            auto_date: self.auto_date,
            max_inline: self.display_width.saturating_sub(12) as usize,
        };

        for (op_idx, op) in self.operators.iter().enumerate() {
            let child_count = op.child_count();
            // An operator whose files could not be read may look empty only because of that:
            // hiding it would hide the error too.
            if self.hide_empty && child_count == 0 && op.load_errors == 0 {
                continue;
            }
            let op_expanded = self.is_expanded(NodeLevel::Operator, op_idx, None, None);

            let total_keys = op.keyed.as_ref().map_or(0, |k| k.total_partitions);
            let desc_count = op.keyed.as_ref().map_or(0, |k| k.descriptors.len());
            let key_type_tag = if self.debug_mode {
                op.keyed
                    .as_ref()
                    .map(|k| format!(" <{}>", k.key_type))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let key_label = format!(
                "{}{}",
                op.keyed
                    .as_ref()
                    .map(|k| k.count_label())
                    .unwrap_or_default(),
                key_type_tag
            );
            let type_tag = if self.debug_mode && !op.operator_type.is_empty() {
                format!(" [{}]", op.operator_type)
            } else {
                String::new()
            };
            let icon = if op.is_kafka() { KAFKA_ICON } else { "" };
            let label = if total_keys > 0 && desc_count > 0 {
                format!(
                    "⯈ {}{}{} ({}, {} states)",
                    icon, op.display_name, type_tag, key_label, desc_count
                )
            } else if total_keys > 0 {
                format!("⯈ {}{}{} ({})", icon, op.display_name, type_tag, key_label)
            } else {
                format!("⯈ {}{}{}", icon, op.display_name, type_tag)
            };
            let label = if op.load_errors > 0 {
                format!(
                    "{}  {}{} state file(s) not read (i: details)",
                    label, LOAD_ERROR_MARK, op.load_errors
                )
            } else {
                label
            };

            let op_path = op.display_name.clone();
            let mut op_node = FlatNode::new(NodeLevel::Operator, label, op_path.clone(), op_idx);
            op_node.expanded = op_expanded;
            op_node.has_children = child_count > 0 || !op.states.is_empty();
            nodes.push(op_node);

            if !op_expanded {
                continue;
            }

            // Keyed state partitions (user states — shown first)
            if let Some(keyed) = &op.keyed {
                let base = op.states.len();
                let visible_count = keyed.visible_count();
                let pager_note = keyed.pager_note();

                // Build the list of partition indices to show on this page
                let page_start = keyed.partition_offset.min(visible_count);
                let page_end = (page_start + PARTITION_PAGE_SIZE).min(visible_count);
                let pager = |arrow: char, kind: NodeKind| {
                    let label = pager_label("  ", arrow, page_start, page_end, visible_count);
                    FlatNode::new(
                        NodeLevel::State,
                        format!("{}{}", label, pager_note),
                        op_path.clone(),
                        op_idx,
                    )
                    .kind(kind)
                };

                if page_start > 0 {
                    nodes.push(pager('↑', NodeKind::PagerUp));
                }
                for vi in page_start..page_end {
                    // Map visible index to real partition index
                    let pi = match &keyed.filtered_indices {
                        Some(indices) => indices[vi],
                        None => vi,
                    };
                    // `visible_count` is bounded by the stored partitions, so this always holds.
                    let Some(part) = keyed.partitions.get(pi) else {
                        continue;
                    };
                    let state_idx = base + pi;
                    let expanded =
                        self.is_expanded(NodeLevel::State, op_idx, Some(state_idx), None);
                    let desc_count = keyed.descriptors.len();

                    let part_path = format!("{} > {}", op_path, part.key);
                    let part_label = if self.debug_mode {
                        format!(
                            "  ⯈ {} ({} states, kg:{})",
                            part.key, desc_count, part.key_group
                        )
                    } else {
                        format!("  ⯈ {} ({} states)", part.key, desc_count)
                    };
                    let mut part_node =
                        FlatNode::new(NodeLevel::State, part_label, part_path.clone(), op_idx)
                            .at(Some(state_idx), None);
                    part_node.expanded = expanded;
                    part_node.has_children = desc_count > 0;
                    nodes.push(part_node);

                    if !expanded {
                        continue;
                    }
                    for (ei, desc) in keyed.descriptors.iter().enumerate() {
                        let raw = part
                            .entry_raw
                            .get(ei)
                            .and_then(|v| v.as_deref())
                            .filter(|r| !r.is_empty());
                        let pojo = keyed.pojo_infos.get(ei).and_then(Option::as_ref);
                        let value =
                            raw.map(|r| decode_state_value(r, &desc.value_type, pojo, style.proto));
                        let row = entry_row(desc, value.as_ref(), raw, &style);
                        let entry_expanded = row.expandable
                            && self.is_expanded(
                                NodeLevel::Entry,
                                op_idx,
                                Some(state_idx),
                                Some(ei),
                            );
                        // Columns, not bytes: a byte count overstates non-ASCII text, and
                        // cutting at one panicked inside a multi-byte character.
                        let display_lbl =
                            if !entry_expanded && text::width(&row.label) > style.max_inline {
                                text::ellipsize(&row.label, style.max_inline, "...").into_owned()
                            } else {
                                row.label
                            };
                        let entry_path = format!("{} > {}", part_path, desc.name);
                        let mut entry_node = FlatNode::new(
                            NodeLevel::Entry,
                            format!(
                                "    {} {}",
                                if row.expandable { "⯈" } else { "•" },
                                display_lbl
                            ),
                            entry_path.clone(),
                            op_idx,
                        )
                        .at(Some(state_idx), Some(ei));
                        entry_node.expanded = entry_expanded;
                        entry_node.has_children = row.expandable;
                        nodes.push(entry_node);

                        if let (true, Some(value)) = (entry_expanded, &value) {
                            let mut children = Vec::new();
                            child_rows(desc, value, &entry_path, &style, &mut children);
                            nodes.extend(children.into_iter().map(|c| {
                                FlatNode::new(NodeLevel::MapEntry, c.label, c.path, op_idx)
                                    .kind(c.kind)
                                    .at(Some(state_idx), Some(ei))
                            }));
                        }
                    }
                }
                // Show "... N more keys" if paginated
                let remaining = visible_count.saturating_sub(page_end);
                if remaining > 0 {
                    nodes.push(pager('↓', NodeKind::PagerDown));
                } else if keyed.hidden_keys() > 0 {
                    // The end of the stored keys is not the end of the operator's keys.
                    nodes.push(
                        FlatNode::new(
                            NodeLevel::State,
                            format!(
                                "  … {} more keys not indexed (not shown, not searched by /); \
                                 --export-state reads them all",
                                keyed.hidden_keys()
                            ),
                            op_path.clone(),
                            op_idx,
                        )
                        .kind(NodeKind::Marker),
                    );
                }
            }

            // Technical operator states (managed-operator) — shown last, dimmed
            for (state_idx, state) in op.states.iter().enumerate() {
                let state_expanded =
                    self.is_expanded(NodeLevel::State, op_idx, Some(state_idx), None);
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
                let pager = |arrow: char, kind: NodeKind| {
                    FlatNode::new(
                        NodeLevel::Entry,
                        pager_label("    ", arrow, page_start, page_end, visible),
                        int_path.clone(),
                        op_idx,
                    )
                    .kind(kind)
                    .at(Some(state_idx), None)
                };
                if page_start > 0 {
                    nodes.push(pager('↑', NodeKind::PagerUp));
                }
                for vi in page_start..page_end {
                    // The element's real index, also under a filter.
                    let ri = state.real_index(vi);
                    let element = &state.elements[ri];
                    nodes.push(
                        FlatNode::new(
                            NodeLevel::Entry,
                            format!("    • [{}] {}", ri, format_key_preview(&element.label)),
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

    /// Number of rows in the tree as it is expanded now.
    pub fn row_count(&self) -> usize {
        self.nodes.len()
    }

    /// Operator index of the currently-selected node (for showing operator info).
    pub fn selected_op_idx(&self) -> Option<usize> {
        let i = self.list_state.selected()?;
        self.nodes.get(i).map(|n| n.op_idx)
    }

    /// Whether the node uniquely identified by (level, op_idx, state_idx, entry_idx) is expanded.
    /// Matching on **both** state_idx and entry_idx matters: an entry's `entry_idx` is its
    /// descriptor index, which repeats across keys (partitions), so without `state_idx` a map
    /// under a non-first expanded key would read the wrong key's flag and never expand.
    fn is_expanded(
        &self,
        level: NodeLevel,
        op_idx: usize,
        state_idx: Option<usize>,
        entry_idx: Option<usize>,
    ) -> bool {
        self.nodes
            .iter()
            .find(|n| {
                n.level == level
                    && n.kind == NodeKind::Data
                    && n.op_idx == op_idx
                    && n.state_idx == state_idx
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

    pub fn set_proto_context(&mut self, ctx: Arc<ProtoContext>) {
        self.proto_ctx = Some(ctx);
        self.rebuild_flat_list();
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

    /// Scroll the window a pager row belongs to by one step, keeping the cursor where it is:
    /// the keyed partitions of its operator, or the elements of its operator state.
    fn scroll(&mut self, pager_idx: usize, forward: bool) {
        let selected_before = self.list_state.selected().unwrap_or(0);
        let (op_idx, state_idx) = {
            let n = &self.nodes[pager_idx];
            (n.op_idx, n.state_idx)
        };
        let Some(op) = self.operators.get_mut(op_idx) else {
            return;
        };
        let (offset, visible, page) = match state_idx {
            None => match &mut op.keyed {
                Some(k) => {
                    let visible = k.visible_count();
                    (&mut k.partition_offset, visible, PARTITION_PAGE_SIZE)
                }
                None => return,
            },
            Some(si) => match op.states.get_mut(si) {
                Some(s) => {
                    let visible = s.visible_count();
                    (&mut s.entry_offset, visible, ENTRY_PAGE_SIZE)
                }
                None => return,
            },
        };
        if forward {
            if *offset < visible.saturating_sub(page) {
                *offset += 1;
            }
        } else {
            *offset = offset.saturating_sub(1);
        }

        self.rebuild_flat_list();
        // Keep cursor at same visual position
        self.list_state.select(Some(
            selected_before.min(self.nodes.len().saturating_sub(1)),
        ));
    }

    /// Apply a filter to keyed partition keys and operator-state elements.
    pub fn apply_filter(&mut self, filter: &Option<AppliedFilter>) {
        for op in &mut self.operators {
            // Filter keyed state partitions
            if let Some(keyed) = &mut op.keyed {
                keyed.partition_offset = 0;
                keyed.filtered_indices = filter.as_ref().map(|f| {
                    keyed
                        .partitions
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| f.matches_str(&p.key, p.key_group))
                        .map(|(i, _)| i)
                        .collect()
                });
            }

            // Filter non-keyed operator states
            for state in &mut op.states {
                state.entry_offset = 0;
                state.filtered_indices = filter.as_ref().map(|f| {
                    state
                        .elements
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| f.matches_bytes(&e.label, e.key_group))
                        .map(|(i, _)| i)
                        .collect()
                });
            }
        }
        self.rebuild_flat_list();
    }

    pub fn move_up(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected == 0 {
                return;
            }
            if self.nodes[selected - 1].kind == NodeKind::PagerUp {
                // Scroll backward — cursor stays on same visual position
                self.scroll(selected - 1, false);
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
                self.scroll(selected + 1, true);
            } else {
                self.list_state.select(Some(selected + 1));
            }
        }
    }

    /// Expand the selected node. On a leaf with bytes behind it (a keyed value, an
    /// operator-state element), returns them for the detail overlay instead.
    pub fn expand(&mut self) -> Option<RawBytes> {
        let selected = self.list_state.selected()?;
        let node = self.nodes.get(selected)?;
        if node.has_children {
            self.nodes[selected].expanded = true;
            self.rebuild_flat_list();
            return None;
        }
        self.selected_raw_bytes()
    }

    /// The bytes behind the selected row, for the hex overlay: a keyed value (from its state row
    /// or any row under it) or an operator-state element. `None` on any other row.
    pub fn selected_raw_bytes(&self) -> Option<RawBytes> {
        let node = self.nodes.get(self.list_state.selected()?)?;
        if node.kind != NodeKind::Data
            || !matches!(node.level, NodeLevel::Entry | NodeLevel::MapEntry)
        {
            return None;
        }
        let (si, ei) = (node.state_idx?, node.entry_idx?);
        let op = self.operators.get(node.op_idx)?;
        if let Some(state) = op.states.get(si) {
            let element = state.elements.get(ei)?;
            let label = String::from_utf8_lossy(&element.label);
            return Some(RawBytes {
                key_title: format!("{}[{}], as decoded", state.name, ei),
                key: element.label.clone(),
                value_title: match &element.bytes {
                    Some(_) => format!("element {}[{}]", state.name, ei),
                    None => format!("no bytes: the element could not be read ({})", label),
                },
                value: element.bytes.clone().unwrap_or_default(),
            });
        }
        let keyed = op.keyed.as_ref()?;
        let part = keyed.partitions.get(si - op.states.len())?;
        let desc = keyed.descriptors.get(ei)?;
        let raw = part.entry_raw.get(ei).and_then(|v| v.clone());
        let value_title = match (&raw, desc.state_type.as_str()) {
            (None, _) => format!("{}: no value for this key", desc.name),
            (Some(_), "MAP") => format!(
                "{} [{}]: the per-key blob the index builds — int count, then per entry \
                 int len + namespace (VoidNamespace + map key), int len + NullableSerializer value",
                desc.name, desc.value_type
            ),
            (Some(_), _) => format!("{} [{}]", desc.name, desc.value_type),
        };
        Some(RawBytes {
            key_title: format!(
                "key {} (as rendered: the index keeps no raw key bytes)",
                part.key
            ),
            key: part.key.clone().into_bytes(),
            value_title,
            value: raw.unwrap_or_default(),
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
                    let (parent_level, state_idx, entry_idx) = match node.level {
                        NodeLevel::MapEntry => (NodeLevel::Entry, node.state_idx, node.entry_idx),
                        NodeLevel::Entry => (NodeLevel::State, node.state_idx, None),
                        NodeLevel::State => (NodeLevel::Operator, None, None),
                        NodeLevel::Operator => return,
                    };
                    let op_idx = node.op_idx;
                    if let Some(i) = self.nodes.iter().position(|n| {
                        n.level == parent_level
                            && n.kind == NodeKind::Data
                            && n.op_idx == op_idx
                            && n.state_idx == state_idx
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

    pub fn jump_last(&mut self) {
        if !self.nodes.is_empty() {
            self.list_state.select(Some(self.nodes.len() - 1));
        }
    }

    pub fn render(&mut self, frame: &mut Frame, block: Block, area: Rect) {
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
                        NodeLevel::Operator if self.operators[node.op_idx].load_errors > 0 => {
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

// --- presentation of decoded values ---------------------------------------------------------

/// How values are presented: none of it changes what is decoded.
struct ValueStyle<'a> {
    proto: Option<&'a ProtoContext>,
    debug: bool,
    auto_date: bool,
    /// Columns a state row may take before it is cut (and, for a structure, made expandable).
    max_inline: usize,
}

impl ValueStyle<'_> {
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
}

/// The row of one (key, state): `value` is what `decode_state_value` made of `raw`, `None` when
/// the key holds no value for the state.
fn entry_row(
    desc: &StateDescriptor,
    value: Option<&Value>,
    raw: Option<&[u8]>,
    style: &ValueStyle,
) -> EntryRow {
    let collection = |prefix: &str, fallback: &str, unit: &str, items: Option<usize>| {
        let type_label = if desc.value_type.starts_with(prefix) {
            desc.value_type.as_str()
        } else {
            fallback
        };
        let summary = match (value, items) {
            (None, _) if style.debug => {
                format!("empty (no data, {} raw entries)", desc.entry_count)
            }
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
            value.and_then(Value::as_object).map(|m| m.len()),
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
    desc: &StateDescriptor,
    value: &Value,
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
            });
            structure_rows(v, &child_path, indent + 2, out);
        } else {
            out.push(ChildRow {
                label: format!("{}{}: {}", pad, name, shown),
                path: child_path,
                kind: NodeKind::Data,
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

fn format_key_preview(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(s) if s.chars().count() <= 120 => s.to_string(),
        Ok(s) => {
            let truncated: String = s.chars().take(117).collect();
            format!("{}…", truncated)
        }
        Err(_) => {
            let hex = hex_preview(key, 24);
            if key.len() > 24 {
                format!("{}…", hex)
            } else {
                hex
            }
        }
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
