use ratatui::prelude::*;
use ratatui::widgets::{Block, List, ListItem, ListState};

use std::sync::Arc;

use crate::index::cache::{CachedPojoInfo, IndexCache, StateDescriptor};
use crate::parser::pojo::{PojoField, PojoInfo};
use crate::proto::ProtoContext;
use crate::tui::filter::AppliedFilter;

/// Maximum number of entry keys to hold in memory at once (virtual scroll window).
const ENTRY_PAGE_SIZE: usize = 500;
/// Maximum number of partitions (keys) to display at once.
const PARTITION_PAGE_SIZE: usize = 30;
/// Marker shown next to Kafka connector operators/states.
const KAFKA_ICON: &str = "📨 ";

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

#[derive(Debug, Clone)]
struct FlatNode {
    level: NodeLevel,
    label: String,
    /// Breadcrumb path for status bar display
    path: String,
    /// Index into the parent collection
    op_idx: usize,
    state_idx: Option<usize>,
    #[allow(dead_code)]
    entry_idx: Option<usize>,
    expanded: bool,
    has_children: bool,
}

pub struct TreeView {
    nodes: Vec<FlatNode>,
    list_state: ListState,
    operators: Vec<OperatorSnapshot>,
    pub hide_empty: bool,
    pub debug_mode: bool,
    /// Auto-detect epoch millis as dates in untyped values
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
}

impl OperatorSnapshot {
    /// Count user-visible children: keyed partitions (filtered count if a filter is active) **plus**
    /// non-keyed operator states. Operators with only operator state (e.g. a Kafka source/sink:
    /// reader splits, writer/committer states) have no keyed partitions, so they must be counted
    /// here too — otherwise `hide_empty` would hide them and their decoded state entirely.
    fn child_count(&self) -> usize {
        self.keyed.as_ref().map_or(0, |k| k.visible_count()) + self.states.len()
    }

    /// A Kafka connector operator (source or sink), detected from its operator-state names.
    fn is_kafka(&self) -> bool {
        self.states.iter().any(|s| is_kafka_state(&s.name))
    }
}

impl KeyedSnapshot {
    /// Number of partitions visible (filtered or total).
    fn visible_count(&self) -> usize {
        match &self.filtered_indices {
            Some(indices) => indices.len(),
            None => self.total_partitions,
        }
    }
}

/// Keyed state snapshot for display.
struct KeyedSnapshot {
    descriptors: Vec<StateDescriptor>,
    partitions: Vec<PartitionSnapshot>,
    total_partitions: usize,
    partition_offset: usize,
    /// Key serializer type (e.g. "String", "Pojo")
    key_type: String,
    /// Indices into `partitions` that match the current filter (None = no filter, show all).
    filtered_indices: Option<Vec<usize>>,
}

struct PartitionSnapshot {
    key: String,
    #[allow(dead_code)]
    key_group: u16,
    /// Raw value bytes per descriptor (for MAP expansion). Labels are computed lazily in
    /// `rebuild_flat_list` (which has the proto context), so none are pre-stored here.
    entry_raw: Vec<Option<Vec<u8>>>,
    /// Descriptor types per entry (for MAP detection)
    entry_descs: Vec<(String, String)>, // (state_type, value_type)
}

struct StateSnapshot {
    name: String,
    state_type: String,
    entry_count: usize,
    entry_keys: Vec<String>,
    raw_entries: Vec<(Vec<u8>, u16)>,
    filtered_indices: Vec<usize>,
    entry_offset: usize,
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
                    .map(|s| {
                        let raw_entries: Vec<(Vec<u8>, u16)> = s
                            .entries
                            .iter()
                            .map(|e| (e.key.clone(), e.key_group))
                            .collect();
                        let entry_count = raw_entries.len();
                        let page_end = entry_count.min(ENTRY_PAGE_SIZE);
                        let entry_keys: Vec<String> = raw_entries[..page_end]
                            .iter()
                            .map(|(k, _)| format_key_preview(k))
                            .collect();
                        let filtered_indices: Vec<usize> = (0..entry_count).collect();
                        StateSnapshot {
                            name: s.name.clone(),
                            state_type: s.state_type.clone(),
                            entry_count,
                            entry_keys,
                            raw_entries,
                            filtered_indices,
                            entry_offset: 0,
                        }
                    })
                    .collect();

                // Structured keyed state
                let keyed = op.keyed_state.as_ref().map(|ks| {
                    let total = ks.total_keys;
                    let partitions: Vec<PartitionSnapshot> = ks
                        .partitions
                        .iter()
                        .map(|p| {
                            let entry_raw: Vec<Option<Vec<u8>>> = p.values.clone();
                            let entry_descs: Vec<(String, String)> = ks
                                .descriptors
                                .iter()
                                .map(|d| (d.state_type.clone(), d.value_type.clone()))
                                .collect();
                            PartitionSnapshot {
                                key: p.key.clone(),
                                key_group: p.key_group,
                                entry_raw,
                                entry_descs,
                            }
                        })
                        .collect();
                    KeyedSnapshot {
                        descriptors: ks.descriptors.clone(),
                        partitions,
                        total_partitions: total,
                        partition_offset: 0,
                        key_type: ks.key_type.clone(),
                        filtered_indices: None,
                    }
                });

                OperatorSnapshot {
                    display_name: op.display_name.clone(),
                    operator_type: shorten_operator_type(&op.identifier_name),
                    states,
                    keyed,
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

        for (op_idx, op) in self.operators.iter().enumerate() {
            let child_count = op.child_count();
            if self.hide_empty && child_count == 0 {
                continue;
            }
            let op_expanded = self
                .nodes
                .iter()
                .find(|n| n.level == NodeLevel::Operator && n.op_idx == op_idx)
                .map(|n| n.expanded)
                .unwrap_or(false);

            let visible_keys = op.keyed.as_ref().map_or(0, |k| k.visible_count());
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
            let key_label = if visible_keys != total_keys {
                format!("{}/{} keys{}", visible_keys, total_keys, key_type_tag)
            } else {
                format!("{} keys{}", total_keys, key_type_tag)
            };
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

            let op_path = op.display_name.clone();
            nodes.push(FlatNode {
                level: NodeLevel::Operator,
                label,
                path: op_path.clone(),
                op_idx,
                state_idx: None,
                entry_idx: None,
                expanded: op_expanded,
                has_children: child_count > 0 || !op.states.is_empty(),
            });

            if !op_expanded {
                continue;
            }

            // Keyed state partitions (user states — shown first)
            if let Some(keyed) = &op.keyed {
                let base = op.states.len();
                let visible_count = keyed.visible_count();

                // Build the list of partition indices to show on this page
                let page_start = keyed.partition_offset.min(visible_count);
                let page_end = (page_start + PARTITION_PAGE_SIZE).min(visible_count);

                // Show "... N earlier keys" if scrolled past start
                if page_start > 0 {
                    nodes.push(FlatNode {
                        level: NodeLevel::State,
                        label: format!("  ↑ {}-{} / {}", page_start + 1, page_end, visible_count),
                        path: op_path.clone(),
                        op_idx,
                        state_idx: None,
                        entry_idx: None,
                        expanded: false,
                        has_children: false,
                    });
                }
                for vi in page_start..page_end {
                    // Map visible index to real partition index
                    let pi = match &keyed.filtered_indices {
                        Some(indices) => indices[vi],
                        None => vi,
                    };
                    let part = &keyed.partitions[pi];
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
                    nodes.push(FlatNode {
                        level: NodeLevel::State,
                        label: part_label,
                        path: part_path.clone(),
                        op_idx,
                        state_idx: Some(state_idx),
                        entry_idx: None,
                        expanded,
                        has_children: desc_count > 0,
                    });

                    if expanded {
                        let max_inline = self.display_width.saturating_sub(12) as usize;
                        let proto_ref = self.proto_ctx.as_deref();
                        let debug = self.debug_mode;
                        let auto_date = self.auto_date;
                        let labels: Vec<String> = keyed
                            .descriptors
                            .iter()
                            .enumerate()
                            .map(|(i, desc)| {
                                let opt_raw = part.entry_raw.get(i).and_then(|v| v.as_ref());
                                format_state_entry_with_proto(
                                    desc, opt_raw, proto_ref, debug, auto_date,
                                )
                            })
                            .collect();
                        for (ei, lbl) in labels.iter().enumerate() {
                            let (stype, vtype) =
                                part.entry_descs.get(ei).cloned().unwrap_or_default();
                            let raw_opt = part.entry_raw.get(ei).and_then(|v| v.as_ref());
                            let has_raw = raw_opt.map_or(false, |r| !r.is_empty());
                            // A MAP blob starts with int32(count). An empty map (count 0, or no
                            // data at all) is a leaf labelled "empty", not a dead expandable node.
                            let map_count = raw_opt
                                .filter(|r| r.len() >= 4)
                                .map(|r| i32::from_be_bytes([r[0], r[1], r[2], r[3]]));
                            let is_map = stype == "MAP" && map_count.is_some_and(|n| n > 0);
                            let is_list = stype == "LIST" && has_raw;
                            let has_pojo = part
                                .entry_descs
                                .get(ei)
                                .map_or(false, |(_, _)| lbl.contains('{'));
                            let is_expandable =
                                is_map || is_list || (has_pojo && lbl.len() > max_inline);
                            let entry_expanded = is_expandable
                                && self.is_expanded(
                                    NodeLevel::Entry,
                                    op_idx,
                                    Some(state_idx),
                                    Some(ei),
                                );

                            // Truncate long labels
                            let display_lbl = if !entry_expanded && lbl.len() > max_inline {
                                format!("{}...}}", &lbl[..max_inline.saturating_sub(4)])
                            } else {
                                lbl.clone()
                            };

                            let entry_name = keyed
                                .descriptors
                                .get(ei)
                                .map(|d| d.name.as_str())
                                .unwrap_or("?");
                            let entry_path = format!("{} > {}", part_path, entry_name);
                            nodes.push(FlatNode {
                                level: NodeLevel::Entry,
                                path: entry_path.clone(),
                                label: format!(
                                    "    {} {}",
                                    if is_expandable { "⯈" } else { "•" },
                                    display_lbl
                                ),
                                op_idx,
                                state_idx: Some(state_idx),
                                entry_idx: Some(ei),
                                expanded: entry_expanded,
                                has_children: is_expandable,
                            });

                            if entry_expanded {
                                if is_map {
                                    // MAP: show key-value pairs
                                    let raw = part.entry_raw[ei].as_ref().unwrap();
                                    let desc = keyed.descriptors.get(ei);
                                    let pairs = parse_map_entries(
                                        raw,
                                        &vtype,
                                        desc,
                                        proto_ref,
                                        self.debug_mode,
                                    );
                                    for (mk, mv) in &pairs {
                                        nodes.push(FlatNode {
                                            level: NodeLevel::MapEntry,
                                            path: format!("{}[{}]", entry_path, mk),
                                            label: format!("        {} : {}", mk, mv),
                                            op_idx,
                                            state_idx: Some(state_idx),
                                            entry_idx: Some(ei),
                                            expanded: false,
                                            has_children: false,
                                        });
                                    }
                                } else if is_list {
                                    // LIST: show elements
                                    let raw = part.entry_raw[ei].as_ref().unwrap();
                                    let items = parse_list_entries(raw, &vtype, self.auto_date);
                                    for (idx_str, val) in &items {
                                        nodes.push(FlatNode {
                                            level: NodeLevel::MapEntry,
                                            path: format!("{}[{}]", entry_path, idx_str),
                                            label: format!("        [{}] {}", idx_str, val),
                                            op_idx,
                                            state_idx: Some(state_idx),
                                            entry_idx: Some(ei),
                                            expanded: false,
                                            has_children: false,
                                        });
                                    }
                                } else if has_pojo {
                                    // POJO: show each field on its own line
                                    if let Some(raw) =
                                        part.entry_raw.get(ei).and_then(|v| v.as_ref())
                                    {
                                        let desc = &keyed.descriptors[ei];
                                        if let Some(pojo) = &desc.pojo_info {
                                            let pi = cached_pojo_to_pojo_info(pojo);
                                            let fields =
                                                parse_pojo_fields_for_display(raw, &pi, proto_ref);
                                            for (fname, fval) in &fields {
                                                push_pretty_field(
                                                    &mut nodes,
                                                    fname,
                                                    fval,
                                                    &entry_path,
                                                    op_idx,
                                                    state_idx,
                                                    ei,
                                                    8, // base indent
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // Show "... N more keys" if paginated
                let remaining = visible_count.saturating_sub(page_end);
                if remaining > 0 {
                    nodes.push(FlatNode {
                        level: NodeLevel::State,
                        label: format!("  ↓ {}-{} / {}", page_start + 1, page_end, visible_count),
                        path: op_path.clone(),
                        op_idx,
                        state_idx: None,
                        entry_idx: None,
                        expanded: false,
                        has_children: false,
                    });
                }
            }

            // Technical operator states (managed-operator) — shown last, dimmed
            if !op.states.is_empty() {
                for (state_idx, state) in op.states.iter().enumerate() {
                    let state_expanded =
                        self.is_expanded(NodeLevel::State, op_idx, Some(state_idx), None);
                    let int_path = format!("{} > [internal] {}", op_path, state.name);
                    nodes.push(FlatNode {
                        level: NodeLevel::State,
                        path: int_path.clone(),
                        label: format!(
                            "  ⯈ {}[internal] {} [{}] ({} entries)",
                            if is_kafka_state(&state.name) {
                                KAFKA_ICON
                            } else {
                                ""
                            },
                            state.name,
                            state.state_type,
                            state.entry_count
                        ),
                        op_idx,
                        state_idx: Some(state_idx),
                        entry_idx: None,
                        expanded: state_expanded,
                        has_children: state.entry_count > 0,
                    });
                    if state_expanded {
                        for (entry_idx, key) in state.entry_keys.iter().enumerate() {
                            let global_idx = state.entry_offset + entry_idx;
                            nodes.push(FlatNode {
                                level: NodeLevel::Entry,
                                path: format!("{}[{}]", int_path, global_idx),
                                label: format!("    • [{}] {}", global_idx, key),
                                op_idx,
                                state_idx: Some(state_idx),
                                entry_idx: Some(global_idx),
                                expanded: false,
                                has_children: false,
                            });
                        }
                    }
                }
            }
        }

        // Preserve selection
        let selected = self.list_state.selected().unwrap_or(0);
        self.nodes = nodes;
        let max = self.nodes.len().saturating_sub(1);
        self.list_state.select(Some(selected.min(max)));
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

    /// Scroll partition window by one step (keeps cursor position stable).
    fn scroll_partitions(&mut self, op_idx: usize, forward: bool) {
        let selected_before = self.list_state.selected().unwrap_or(0);

        let op = match self.operators.get_mut(op_idx) {
            Some(o) => o,
            None => return,
        };
        let keyed = match &mut op.keyed {
            Some(k) => k,
            None => return,
        };

        if forward {
            let max_offset = keyed.visible_count().saturating_sub(PARTITION_PAGE_SIZE);
            if keyed.partition_offset < max_offset {
                keyed.partition_offset += 1;
            }
        } else if keyed.partition_offset > 0 {
            keyed.partition_offset -= 1;
        }

        self.rebuild_flat_list();
        // Keep cursor at same visual position
        self.list_state.select(Some(
            selected_before.min(self.nodes.len().saturating_sub(1)),
        ));
    }

    /// Apply a filter to keyed partition keys and entry-level nodes.
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
                let indices: Vec<usize> = state
                    .raw_entries
                    .iter()
                    .enumerate()
                    .filter(|(_, (key, kg))| match filter {
                        None => true,
                        Some(f) => f.matches_bytes(key, *kg),
                    })
                    .map(|(i, _)| i)
                    .collect();
                state.entry_count = indices.len();
                let page_end = indices.len().min(ENTRY_PAGE_SIZE);
                state.entry_keys = indices[..page_end]
                    .iter()
                    .map(|&i| format_key_preview(&state.raw_entries[i].0))
                    .collect();
                state.filtered_indices = indices;
                state.entry_offset = 0;
            }
        }
        self.rebuild_flat_list();
    }

    pub fn move_up(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected == 0 {
                return;
            }
            let prev = &self.nodes[selected - 1];
            if prev.label.contains('↑') {
                // Scroll backward — cursor stays on same visual position
                self.scroll_partitions(prev.op_idx, false);
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
            let next = &self.nodes[selected + 1];
            if next.label.contains('↓') {
                // Scroll forward — cursor stays on same visual position
                let op_idx = next.op_idx;
                self.scroll_partitions(op_idx, true);
            } else {
                self.list_state.select(Some(selected + 1));
            }
        }
    }

    /// Expand the selected node. Returns Some((key, value_placeholder))
    /// if the selected node is an entry (leaf), for opening in detail view.
    pub fn expand(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if let Some(selected) = self.list_state.selected() {
            if selected < self.nodes.len() {
                let node = &self.nodes[selected];
                if node.has_children {
                    self.nodes[selected].expanded = true;
                    self.rebuild_flat_list();
                    return None;
                }
                // Entry node: return key bytes for detail view
                if node.level == NodeLevel::Entry {
                    if let (Some(si), Some(display_idx)) = (node.state_idx, node.entry_idx) {
                        let op = &self.operators[node.op_idx];
                        if si < op.states.len() {
                            let state = &op.states[si];
                            // Map display index through filtered_indices
                            if display_idx < state.filtered_indices.len() {
                                let raw_idx = state.filtered_indices[display_idx];
                                if raw_idx < state.raw_entries.len() {
                                    let (key, _kg) = &state.raw_entries[raw_idx];
                                    return Some((key.clone(), Vec::new()));
                                }
                            }
                        }
                    }
                }
            }
        }
        None
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
                    match node.level {
                        NodeLevel::MapEntry => {
                            // Go to parent Entry node
                            let parent_entry = node.entry_idx;
                            for (i, n) in self.nodes.iter().enumerate() {
                                if n.level == NodeLevel::Entry
                                    && n.op_idx == node.op_idx
                                    && n.entry_idx == parent_entry
                                {
                                    self.list_state.select(Some(i));
                                    break;
                                }
                            }
                        }
                        NodeLevel::Entry | NodeLevel::State => {
                            let parent_level = if node.level == NodeLevel::Entry {
                                NodeLevel::State
                            } else {
                                NodeLevel::Operator
                            };
                            let parent_op = node.op_idx;
                            let parent_state = if parent_level == NodeLevel::State {
                                node.state_idx
                            } else {
                                None
                            };
                            for (i, n) in self.nodes.iter().enumerate() {
                                if n.level == parent_level
                                    && n.op_idx == parent_op
                                    && (parent_level == NodeLevel::Operator
                                        || n.state_idx == parent_state)
                                {
                                    self.list_state.select(Some(i));
                                    break;
                                }
                            }
                        }
                        NodeLevel::Operator => {}
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
                } else {
                    match node.level {
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

        let mut state = self.list_state.clone();
        frame.render_stateful_widget(list, area, &mut state);
    }
}

/// Format a state descriptor + its raw value for display.
pub fn cached_pojo_to_pojo_info(cached: &CachedPojoInfo) -> PojoInfo {
    PojoInfo {
        class_name: cached.class_name.clone(),
        short_name: cached
            .class_name
            .rsplit('.')
            .next()
            .unwrap_or(&cached.class_name)
            .to_string(),
        fields: cached
            .fields
            .iter()
            .map(|f| PojoField {
                name: f.name.clone(),
                type_name: f.type_name.clone(),
                nested_pojo: f
                    .nested
                    .as_ref()
                    .map(|n| Box::new(cached_pojo_to_pojo_info(n))),
            })
            .collect(),
    }
}

/// Parse POJO raw bytes into field name-value pairs for expanded display.
fn parse_pojo_fields_for_display(
    raw: &[u8],
    info: &PojoInfo,
    proto_ctx: Option<&ProtoContext>,
) -> Vec<(String, String)> {
    use crate::parser::java_deser::JavaReader;

    let mut r = JavaReader::new(raw);

    // Read PojoSerializer flags byte (0x01=null, 0x02=no_subclass, etc.)
    let flags = match r.read_byte() {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    if flags & 0x01 != 0 {
        return vec![("(value)".to_string(), "null".to_string())];
    }
    if flags & 0x08 != 0 {
        let _ = r.read_int(); // subclass tag
    } else if flags & 0x04 != 0 {
        let _ = r.read_utf(); // subclass name
    }

    let mut fields = Vec::new();
    let mut ok = true;
    for field in info.fields.iter() {
        if !ok {
            fields.push((field.name.clone(), "...".to_string()));
            continue;
        }
        let is_null = match r.read_boolean() {
            Ok(b) => b,
            Err(_) => break,
        };
        if is_null {
            fields.push((field.name.clone(), "null".to_string()));
            continue;
        }
        // For byte[] fields matching a proto pattern, try pretty decode
        let is_proto_bytes = field.type_name == "byte[]"
            && proto_ctx.map_or(false, |ctx| {
                ctx.patterns.iter().any(|p| p.bytes_field == field.name)
            });
        if is_proto_bytes {
            // Read raw bytes
            match r.read_int() {
                Ok(len) if len >= 0 && (len as usize) <= 10_000_000 => {
                    match r.read_bytes(len as usize) {
                        Ok(bytes) => {
                            // Try to find className in already-read fields
                            if let Some(ctx) = proto_ctx {
                                let class_name = fields
                                    .iter()
                                    .find(|(n, _)| ctx.patterns.iter().any(|p| p.class_field == *n))
                                    .map(|(_, v)| v.trim_matches('"').to_string());
                                if let Some(cn) = class_name {
                                    let resolve = ctx
                                        .patterns
                                        .first()
                                        .map(|p| p.resolve.as_str())
                                        .unwrap_or("auto");
                                    if let Some(decoded) = ctx.decode(&cn, &bytes, resolve, None) {
                                        // Push compact for now, pretty in expand
                                        fields.push((field.name.clone(), decoded));
                                        continue;
                                    }
                                }
                            }
                            fields.push((field.name.clone(), format!("[{} bytes]", bytes.len())));
                        }
                        Err(_) => {
                            fields.push((field.name.clone(), "?".to_string()));
                            ok = false;
                        }
                    }
                }
                _ => {
                    fields.push((field.name.clone(), "?".to_string()));
                    ok = false;
                }
            }
            continue;
        }
        match crate::parser::pojo::read_field_value_pub(
            &mut r,
            &field.type_name,
            field.nested_pojo.as_deref(),
            proto_ctx,
        ) {
            Some(val) => {
                if val.contains('?') || val.contains("...") {
                    ok = false;
                }
                fields.push((field.name.clone(), val));
            }
            None => {
                fields.push((field.name.clone(), "?".to_string()));
                ok = false;
            }
        }
    }
    fields
}

/// Parse MapSerializer raw bytes into key-value pairs for display.
///
/// Parse map entries from a blob built by the builder.
///
/// Blob format: `int(numPairs) + for each: int(nsLen) + nsBytes + int(valLen) + valBytes`
/// nsBytes = namespace containing the map user key.
/// valBytes = serialized map value.
///
/// `value_type` is e.g. "Map<Long,byte[]>" or "Map<String,Pojo>".
fn parse_map_entries(
    raw: &[u8],
    value_type: &str,
    desc: Option<&StateDescriptor>,
    proto_ctx: Option<&crate::proto::ProtoContext>,
    debug: bool,
) -> Vec<(String, String)> {
    use crate::parser::java_deser::JavaReader;

    let (key_type, val_type) = if let Some(inner) = value_type
        .strip_prefix("Map<")
        .and_then(|s| s.strip_suffix('>'))
    {
        inner.split_once(',').unwrap_or(("?", "?"))
    } else {
        ("?", "?")
    };

    if raw.len() < 4 {
        return Vec::new();
    }
    let num_entries = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
    if num_entries <= 0 || num_entries > 100_000 {
        return Vec::new();
    }

    let mut r = JavaReader::new(&raw[4..]);
    let mut pairs = Vec::new();
    let pojo_info = desc.and_then(|d| d.pojo_info.as_ref());

    for _ in 0..num_entries.min(50) {
        // Read namespace (contains the map user key)
        let ns_len = match r.read_int().ok() {
            Some(n) if n >= 0 => n as usize,
            _ => break,
        };
        let ns_bytes = match r.read_bytes(ns_len) {
            Ok(b) => b,
            Err(_) => break,
        };

        // Read value
        let val_len = match r.read_int().ok() {
            Some(n) if n >= 0 => n as usize,
            _ => break,
        };
        let val_bytes = match r.read_bytes(val_len) {
            Ok(b) => b,
            Err(_) => break,
        };

        // Decode map key from namespace bytes
        let map_key = decode_map_key(&ns_bytes, key_type.trim());

        // Decode map value
        let map_val = decode_map_value(&val_bytes, val_type.trim(), pojo_info, proto_ctx);

        let map_val = if debug {
            let val_hex: String = val_bytes
                .iter()
                .take(16)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            let ns_hex: String = ns_bytes
                .iter()
                .take(8)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            format!("{} (ns:{} val:{})", map_val, ns_hex, val_hex)
        } else {
            map_val
        };

        pairs.push((map_key, map_val));
    }

    if num_entries > 50 {
        pairs.push(("...".to_string(), format!("+{} more", num_entries - 50)));
    }
    pairs
}

/// Parse list elements from a ListSerializer value blob.
/// Format: `int(size) + serialize(element) * size`
fn parse_list_entries(raw: &[u8], value_type: &str, auto_date: bool) -> Vec<(String, String)> {
    use crate::parser::java_deser::JavaReader;

    let elem_type = value_type
        .strip_prefix("List<")
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(value_type);

    if raw.len() < 4 {
        return Vec::new();
    }
    let size = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
    if size <= 0 || size > 1_000_000 {
        return Vec::new();
    }

    let mut r = JavaReader::new(&raw[4..]);
    let mut items = Vec::new();
    let max_display = 100.min(size as usize);

    for i in 0..max_display {
        let val = match elem_type.trim() {
            "Long" => r.read_long().ok().map(|v| {
                if auto_date && v > 946_684_800_000 && v < 2_208_988_800_000 {
                    crate::parser::pojo::millis_to_iso(v)
                } else {
                    v.to_string()
                }
            }),
            "Int" | "Integer" => r.read_int().ok().map(|v| v.to_string()),
            "String" => r.read_string_value().ok().map(|s| format!("\"{}\"", s)),
            "Boolean" => r.read_boolean().ok().map(|v| v.to_string()),
            "Short" => r.read_short().ok().map(|v| v.to_string()),
            "Double" => r
                .read_long()
                .ok()
                .map(|v| format!("{}", f64::from_bits(v as u64))),
            "Float" => r
                .read_int()
                .ok()
                .map(|v| format!("{}", f32::from_bits(v as u32))),
            "Date" => r
                .read_long()
                .ok()
                .map(|v| crate::parser::pojo::millis_to_iso(v)),
            _ => None,
        };
        match val {
            Some(v) => items.push((i.to_string(), v)),
            None => {
                items.push((i.to_string(), "?".to_string()));
                break;
            }
        }
    }

    if size as usize > max_display {
        items.push((
            "...".to_string(),
            format!("+{} more", size as usize - max_display),
        ));
    }
    items
}

fn decode_map_key(ns_bytes: &[u8], key_type: &str) -> String {
    use crate::parser::java_deser::JavaReader;
    if ns_bytes.is_empty() {
        return "?".to_string();
    }
    // Skip VoidNamespace byte (0x00) at the start — the map user key follows it
    let map_key_bytes = if ns_bytes[0] == 0x00 && ns_bytes.len() > 1 {
        &ns_bytes[1..]
    } else {
        ns_bytes
    };
    let mut r = JavaReader::new(map_key_bytes);
    match key_type {
        "String" => {
            let result = r.read_string_value().ok().unwrap_or_default();
            if result.is_empty() {
                // Fallback: try UTF-8 or show hex
                let utf = String::from_utf8_lossy(map_key_bytes)
                    .trim_matches('\0')
                    .to_string();
                if utf.is_empty() || utf.chars().all(|c| c.is_control()) {
                    let hex: String = map_key_bytes
                        .iter()
                        .take(16)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join("");
                    format!("0x{}", hex)
                } else {
                    utf
                }
            } else {
                result
            }
        }
        "Long" => r
            .read_long()
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "?".to_string()),
        "Int" => r
            .read_int()
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "?".to_string()),
        t if t.starts_with("Enum<") => r
            .read_int()
            .ok()
            .map(|ordinal| {
                if let Some(name) = t
                    .strip_prefix("Enum<")
                    .and_then(|s| s.strip_suffix('>'))
                    .and_then(|s| s.split_once(':'))
                    .map(|(_, cs)| cs)
                    .and_then(|cs| cs.split(',').nth(ordinal as usize))
                {
                    name.to_string()
                } else {
                    format!("ordinal({})", ordinal)
                }
            })
            .unwrap_or_else(|| "?".to_string()),
        _ => format!("[{} bytes]", map_key_bytes.len()),
    }
}

fn decode_map_value(
    val_bytes: &[u8],
    val_type: &str,
    pojo_info: Option<&CachedPojoInfo>,
    proto_ctx: Option<&crate::proto::ProtoContext>,
) -> String {
    use crate::parser::java_deser::JavaReader;
    if val_bytes.is_empty() {
        return "null".to_string();
    }
    match val_type {
        "Long" => {
            if val_bytes.len() >= 8 {
                i64::from_be_bytes(val_bytes[..8].try_into().unwrap()).to_string()
            } else {
                format!("[{} bytes]", val_bytes.len())
            }
        }
        "Int" => {
            if val_bytes.len() >= 4 {
                i32::from_be_bytes(val_bytes[..4].try_into().unwrap()).to_string()
            } else {
                format!("[{} bytes]", val_bytes.len())
            }
        }
        "Boolean" => {
            if val_bytes.len() >= 1 {
                if val_bytes[0] != 0 {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            } else {
                "?".to_string()
            }
        }
        "String" => {
            let mut r = JavaReader::new(val_bytes);
            r.read_string_value()
                .ok()
                .map(|s| format!("\"{}\"", s))
                .unwrap_or_else(|| format!("[{} bytes]", val_bytes.len()))
        }
        "Date" => {
            if val_bytes.len() >= 8 {
                let ms = i64::from_be_bytes(val_bytes[..8].try_into().unwrap());
                crate::parser::pojo::millis_to_iso(ms)
            } else {
                format!("[{} bytes]", val_bytes.len())
            }
        }
        "Pojo" => {
            if let Some(pi) = pojo_info {
                let pojo = cached_pojo_to_pojo_info(pi);
                crate::parser::pojo::deserialize_pojo_to_json(val_bytes, &pojo, proto_ctx)
            } else {
                format!("[{} bytes]", val_bytes.len())
            }
        }
        _ => best_effort_format(val_bytes, true),
    }
}

/// Add a field to the node list, recursively splitting long JSON objects.
fn push_pretty_field(
    nodes: &mut Vec<FlatNode>,
    name: &str,
    value: &str,
    parent_path: &str,
    op_idx: usize,
    state_idx: usize,
    entry_idx: usize,
    indent: usize,
) {
    let pad: String = " ".repeat(indent);
    let path = format!("{}.{}", parent_path, name);

    if value.starts_with("{ ") && value.len() > 60 {
        // Split into sub-fields
        nodes.push(FlatNode {
            level: NodeLevel::MapEntry,
            path: path.clone(),
            label: format!("{}{}:", pad, name),
            op_idx,
            state_idx: Some(state_idx),
            entry_idx: Some(entry_idx),
            expanded: false,
            has_children: false,
        });
        for (sk, sv) in split_json_fields(value) {
            push_pretty_field(
                nodes,
                &sk,
                &sv,
                &path,
                op_idx,
                state_idx,
                entry_idx,
                indent + 2,
            );
        }
    } else {
        nodes.push(FlatNode {
            level: NodeLevel::MapEntry,
            path,
            label: format!("{}{}: {}", pad, name, value),
            op_idx,
            state_idx: Some(state_idx),
            entry_idx: Some(entry_idx),
            expanded: false,
            has_children: false,
        });
    }
}

/// Split a compact JSON `{ k1: v1, k2: v2 }` into (key, value) pairs.
fn split_json_fields(json: &str) -> Vec<(String, String)> {
    let inner = json
        .strip_prefix("{ ")
        .and_then(|s| s.strip_suffix(" }"))
        .unwrap_or(json);

    let mut result = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut current = String::new();

    for ch in inner.chars() {
        match ch {
            '"' => {
                in_string = !in_string;
                current.push(ch);
            }
            '{' | '[' if !in_string => {
                depth += 1;
                current.push(ch);
            }
            '}' | ']' if !in_string => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 && !in_string => {
                if let Some((k, v)) = current.trim().split_once(": ") {
                    result.push((k.to_string(), v.to_string()));
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        if let Some((k, v)) = current.trim().split_once(": ") {
            result.push((k.to_string(), v.to_string()));
        }
    }
    result
}

fn format_state_entry_with_proto(
    desc: &StateDescriptor,
    raw_value: Option<&Vec<u8>>,
    proto_ctx: Option<&ProtoContext>,
    debug: bool,
    auto_date: bool,
) -> String {
    match raw_value {
        Some(raw) if !raw.is_empty() && desc.state_type != "MAP" => {
            if let Some(pojo) = &desc.pojo_info {
                let pi = cached_pojo_to_pojo_info(pojo);
                let val = crate::parser::pojo::deserialize_pojo_to_json(raw, &pi, proto_ctx);
                let tag = if debug {
                    // Show POJO schema + first bytes for debugging
                    let schema: Vec<String> = pojo
                        .fields
                        .iter()
                        .map(|f| format!("{}:{}", f.name, f.type_name))
                        .collect();
                    let hex_preview: String = raw
                        .iter()
                        .take(24)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!(
                        " [{}|{}] (hex: {})",
                        desc.state_type,
                        schema.join(", "),
                        hex_preview
                    )
                } else {
                    String::new()
                };
                return format!("{}{}: {}", desc.name, tag, val);
            }
        }
        _ => {}
    }
    format_state_entry(desc, raw_value, debug, auto_date)
}

fn format_state_entry(
    desc: &StateDescriptor,
    raw_value: Option<&Vec<u8>>,
    debug: bool,
    auto_date: bool,
) -> String {
    let tag = |t: &str| {
        if debug {
            format!(" [{}]", t)
        } else {
            String::new()
        }
    };

    match raw_value {
        Some(raw) if !raw.is_empty() && desc.state_type == "MAP" => {
            let type_label = if desc.value_type.starts_with("Map<") {
                desc.value_type.clone()
            } else {
                "MAP".to_string()
            };
            let detail = &desc.pojo_detail;
            if raw.len() >= 4 {
                let n = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
                if n == 0 {
                    format!("{} [{}]{}: empty", desc.name, type_label, detail)
                } else {
                    format!("{} [{}]{}: {} entries", desc.name, type_label, detail, n)
                }
            } else {
                format!("{} [{}]{}", desc.name, type_label, detail)
            }
        }
        raw_ref if desc.state_type == "MAP" => {
            let type_label = if desc.value_type.starts_with("Map<") {
                desc.value_type.clone()
            } else {
                "MAP".to_string()
            };
            let raw_info = match raw_ref {
                Some(r) if !r.is_empty() && r.len() >= 4 => {
                    let n = i32::from_be_bytes([r[0], r[1], r[2], r[3]]);
                    format!(": {} entries", n)
                }
                Some(r) if !r.is_empty() => format!(": [{} bytes]", r.len()),
                _ if debug => format!(": empty (no data, {} raw entries)", desc.entry_count),
                _ => ": empty".to_string(),
            };
            if desc.pojo_detail.is_empty() {
                format!("{} [{}]{}", desc.name, type_label, raw_info)
            } else {
                format!(
                    "{} [{}]{}{}",
                    desc.name, type_label, desc.pojo_detail, raw_info
                )
            }
        }
        Some(raw) if !raw.is_empty() && desc.state_type == "LIST" => {
            let type_label = if desc.value_type.starts_with("List<") {
                desc.value_type.clone()
            } else {
                "LIST".to_string()
            };
            if raw.len() >= 4 {
                let n = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
                format!("{} [{}]: {} elements", desc.name, type_label, n)
            } else {
                format!("{} [{}]", desc.name, type_label)
            }
        }
        Some(raw) if !raw.is_empty() => {
            let value_str = if let Some(pojo) = &desc.pojo_info {
                let pi = cached_pojo_to_pojo_info(pojo);
                crate::parser::pojo::deserialize_pojo_to_json(raw, &pi, None)
            } else if raw[0] == 0x02 && raw.len() > 4 {
                // Likely a POJO (flags=0x02 NO_SUBCLASS) without associated pojo_info
                format_raw_pojo(raw, auto_date)
            } else {
                format_raw_value(raw, &desc.value_type, auto_date)
            };
            let is_useful = !value_str.is_empty()
                && value_str.chars().any(|c| !c.is_control() && c != '\0')
                && !value_str.starts_with('['); // "[N bytes]" is not useful
            if is_useful {
                if debug {
                    let hex: String = raw
                        .iter()
                        .take(16)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!(
                        "{} [{}]: {} (hex: {})",
                        desc.name, desc.state_type, value_str, hex
                    )
                } else {
                    format!("{}: {}", desc.name, value_str)
                }
            } else {
                // Unknown type or unreadable value: show hex
                let hex: String = raw
                    .iter()
                    .take(20)
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!(
                    "{} [{}|{}B]: {}",
                    desc.name,
                    desc.value_type,
                    raw.len(),
                    hex
                )
            }
        }
        Some(_) | None if desc.state_type == "VALUE" => {
            format!("{}{}: null", desc.name, tag("VALUE"))
        }
        _ => {
            format!("{}{}", desc.name, tag(&desc.state_type))
        }
    }
}

/// Format raw value bytes based on the serializer type.
fn format_raw_value(raw: &[u8], val_type: &str, auto_date: bool) -> String {
    if raw.is_empty() {
        return String::new();
    }
    match val_type {
        "Int" if raw.len() == 4 => i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]).to_string(),
        t if t.starts_with("Enum<") && raw.len() == 4 => {
            let ordinal = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
            if let Some(name) = t
                .strip_prefix("Enum<")
                .and_then(|s| s.strip_suffix('>'))
                .and_then(|s| s.split_once(':'))
                .map(|(_, cs)| cs)
                .and_then(|cs| cs.split(',').nth(ordinal as usize))
            {
                name.to_string()
            } else {
                format!("ordinal({})", ordinal)
            }
        }
        "Long" if raw.len() == 8 => i64::from_be_bytes(raw[..8].try_into().unwrap()).to_string(),
        "Boolean" if raw.len() == 1 => if raw[0] != 0 { "true" } else { "false" }.to_string(),
        "Double" if raw.len() == 8 => {
            format!("{}", f64::from_be_bytes(raw[..8].try_into().unwrap()))
        }
        "Float" if raw.len() == 4 => {
            format!("{}", f32::from_be_bytes(raw[..4].try_into().unwrap()))
        }
        "Short" if raw.len() == 2 => i16::from_be_bytes([raw[0], raw[1]]).to_string(),
        "String" if !raw.is_empty() => {
            // Value bytes are StringValue-serialized: decode via JavaReader
            use crate::parser::java_deser::JavaReader;
            let mut r = JavaReader::new(raw);
            match r.read_string_value() {
                Ok(s) if !s.is_empty() && s.len() > 80 => format!("{}...", &s[..77]),
                Ok(s) if !s.is_empty() => s,
                _ => {
                    // Type may be wrong (misassociated). Try best-effort by size.
                    best_effort_format(raw, auto_date)
                }
            }
        }
        "Date" if raw.len() == 8 => {
            let millis = i64::from_be_bytes(raw[..8].try_into().unwrap());
            crate::parser::pojo::millis_to_iso(millis)
        }
        "Instant" if raw.len() >= 8 => {
            let secs = i64::from_be_bytes(raw[..8].try_into().unwrap());
            let nanos = if raw.len() >= 12 {
                i32::from_be_bytes(raw[8..12].try_into().unwrap())
            } else {
                0
            };
            let total_ms = secs * 1000 + (nanos / 1_000_000) as i64;
            crate::parser::pojo::millis_to_iso(total_ms)
        }
        "LocalDateTime" if raw.len() >= 28 => {
            let y = i32::from_be_bytes(raw[0..4].try_into().unwrap());
            let mo = i32::from_be_bytes(raw[4..8].try_into().unwrap());
            let d = i32::from_be_bytes(raw[8..12].try_into().unwrap());
            let h = i32::from_be_bytes(raw[12..16].try_into().unwrap());
            let mi = i32::from_be_bytes(raw[16..20].try_into().unwrap());
            let s = i32::from_be_bytes(raw[20..24].try_into().unwrap());
            format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, mo, d, h, mi, s)
        }
        "LocalDate" if raw.len() >= 12 => {
            let y = i32::from_be_bytes(raw[0..4].try_into().unwrap());
            let mo = i32::from_be_bytes(raw[4..8].try_into().unwrap());
            let d = i32::from_be_bytes(raw[8..12].try_into().unwrap());
            format!("{:04}-{:02}-{:02}", y, mo, d)
        }
        "byte[]" if !raw.is_empty() => {
            format!("[{} bytes]", raw.len())
        }
        _ => best_effort_format(raw, auto_date),
    }
}

/// Try to interpret raw bytes as a POJO without known field structure.
/// Reads flags byte then scans fields by null-boolean prefix + heuristic value detection.
fn format_raw_pojo(raw: &[u8], auto_date: bool) -> String {
    use crate::parser::java_deser::JavaReader;

    let mut r = JavaReader::new(raw);

    // Read flags byte (0x02 = NO_SUBCLASS)
    let flags = match r.read_byte() {
        Ok(f) => f,
        Err(_) => return format!("[{} bytes]", raw.len()),
    };
    // Handle NullableSerializer wrapper
    if flags == 0x00 {
        if let Ok(f2) = r.read_byte() {
            if f2 & 0x01 != 0 {
                return "null".to_string();
            }
        }
    } else if flags & 0x01 != 0 {
        return "null".to_string();
    }

    // Scan fields: each is boolean(isNull) + value
    let mut parts = Vec::new();
    let mut field_idx = 0;
    while r.position() < raw.len() as u64 {
        let is_null = match r.read_boolean() {
            Ok(b) => b,
            Err(_) => break,
        };
        if is_null {
            parts.push(format!("f{}: null", field_idx));
            field_idx += 1;
            continue;
        }
        // Try to detect the field value type from remaining bytes
        let pos_before = r.position() as usize;
        let remaining = raw.len() - pos_before;

        // Try StringValue first (most common in domain POJOs)
        if remaining >= 1 {
            let mut r2 = JavaReader::new(&raw[pos_before..]);
            if let Ok(s) = r2.read_string_value() {
                let consumed = r2.position() as usize;
                if consumed > 0 && consumed <= remaining && s.chars().any(|c| !c.is_control()) {
                    parts.push(format!("f{}: \"{}\"", field_idx, s));
                    let _ = r.read_bytes(consumed);
                    field_idx += 1;
                    continue;
                }
            }
        }
        // Try to guess field type by remaining bytes
        // Check if this is the last field (next would be null boolean or EOF)
        if remaining == 1 {
            // Single byte left: Boolean
            if let Ok(v) = r.read_boolean() {
                parts.push(format!("f{}: {}", field_idx, v));
                field_idx += 1;
                continue;
            }
        }
        if remaining >= 4 && remaining < 8 {
            // 4 bytes: Int or Enum ordinal
            if let Ok(v) = r.read_int() {
                parts.push(format!("f{}: {}", field_idx, v));
                field_idx += 1;
                continue;
            }
        }
        if remaining >= 8 {
            if let Ok(v) = r.read_long() {
                if auto_date && v > 946_684_800_000 && v < 2_208_988_800_000 {
                    parts.push(format!(
                        "f{}: {}",
                        field_idx,
                        crate::parser::pojo::millis_to_iso(v)
                    ));
                } else {
                    parts.push(format!("f{}: {}", field_idx, v));
                }
                field_idx += 1;
                continue;
            }
        }
        // Can't determine type, show remaining as hex
        let rest = raw.len() - r.position() as usize;
        if rest > 0 {
            parts.push(format!("+{} bytes", rest));
        }
        break;
    }

    if parts.is_empty() {
        format!("[{} bytes]", raw.len())
    } else {
        format!("{{ {} }}", parts.join(", "))
    }
}

/// Best-effort value formatting when the declared type doesn't match the data.
/// Uses raw byte length to guess the most likely interpretation.
fn best_effort_format(raw: &[u8], auto_date: bool) -> String {
    match raw.len() {
        1 => {
            if raw[0] != 0 {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        2 => i16::from_be_bytes(raw[..2].try_into().unwrap()).to_string(),
        4 => i32::from_be_bytes(raw[..4].try_into().unwrap()).to_string(),
        8 => {
            let v = i64::from_be_bytes(raw[..8].try_into().unwrap());
            if auto_date && v > 946_684_800_000 && v < 2_208_988_800_000 {
                crate::parser::pojo::millis_to_iso(v)
            } else {
                v.to_string()
            }
        }
        12 => {
            let long_val = i64::from_be_bytes(raw[..8].try_into().unwrap());
            let int_val = i32::from_be_bytes(raw[8..12].try_into().unwrap());
            if auto_date && long_val > 946_684_800_000 && long_val < 2_208_988_800_000 {
                crate::parser::pojo::millis_to_iso(long_val)
            } else if auto_date && long_val > 946_684_800 && long_val < 2_208_988_800 {
                let total_ms = long_val * 1000 + (int_val / 1_000_000) as i64;
                crate::parser::pojo::millis_to_iso(total_ms)
            } else {
                // Try as 3 ints (LocalDate)
                let y = i32::from_be_bytes(raw[0..4].try_into().unwrap());
                let m = i32::from_be_bytes(raw[4..8].try_into().unwrap());
                let d = i32::from_be_bytes(raw[8..12].try_into().unwrap());
                if (1900..=2100).contains(&y) && (1..=12).contains(&m) && (1..=31).contains(&d) {
                    format!("{:04}-{:02}-{:02}", y, m, d)
                } else {
                    format!("{}+{}", long_val, int_val)
                }
            }
        }
        n if n > 0 => {
            // Try UTF-8 string (skip leading vint)
            use crate::parser::java_deser::JavaReader;
            let mut r = JavaReader::new(raw);
            if let Ok(s) = r.read_string_value() {
                if !s.is_empty() && s.chars().any(|c| !c.is_control() && c != '\0') {
                    return s;
                }
            }
            format!("[{} bytes]", n)
        }
        _ => String::new(),
    }
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
            let hex: String = key
                .iter()
                .take(24)
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            if key.len() > 24 {
                format!("{}…", hex)
            } else {
                hex
            }
        }
    }
}
