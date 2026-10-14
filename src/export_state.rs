//! Streaming export of a **single** keyed state, with a memory footprint bounded by the largest
//! key-group rather than by the size of the savepoint.
//!
//! This path deliberately bypasses [`crate::index`]: the index cache holds every key and every
//! raw value in memory (and serialises all of it to disk), which is fine for browsing a savepoint
//! in the TUI but not for a state measured in gigabytes. Here we instead:
//!
//! 1. read only the **header** of each subtask's keyed stream (it ends where the first key-group
//!    begins) to resolve the state's id, type, value serializer and POJO layout;
//! 2. seek straight to each key-group using `KeyGroupsStateHandle.offsets`, reading **one
//!    key-group at a time** into a reusable buffer;
//! 3. parse that block with a state filter, so entries belonging to other states are skipped via
//!    their length prefixes without decoding keys or copying values;
//! 4. decode each value with [`crate::parser::value::decode_state_value`] — the single decoding
//!    source of truth, shared with the TUI and the full JSON export — and write one JSON object
//!    per line.
//!
//! Rows are therefore emitted as they are read; nothing accumulates.
//!
//! Opening the streams (each file once, read positionally), resolving each subtask's header and
//! locating a key-group's bytes are [`crate::reader`]'s, shared with
//! [`crate::reader::StateReader`]; this module adds the state filter, the row shape and the sinks.

use std::io::Write;
use std::path::Path;

use serde_json::{json, Value};

use crate::parser::keyed_state::{key_group_block, parse_key_group_block};
use crate::parser::metadata::Savepoint;
use crate::proto::ProtoContext;
use crate::reader::{decode_entry_value, kg_prefix_len, OpContext};

pub use crate::reader::hex_id;

/// What an export actually read, for reporting and for asserting the bounded footprint in tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExportStats {
    pub rows: u64,
    /// Subtask keyed streams visited.
    pub subtasks: usize,
    /// Key-groups actually read (empty ones are skipped).
    pub key_groups: usize,
    /// Largest single key-group block held in memory at once, in bytes — this is the export's
    /// peak buffer, independent of the savepoint's total size.
    pub max_block_bytes: usize,
    /// True when the savepoint was written with per-key-group snapshot compression.
    pub compressed: bool,
    /// Key-groups that could not be read to their end mark (truncated, a bad length, an unknown
    /// stateId): the rows read before the defect are exported, the rest of that key-group is not.
    pub malformed_key_groups: usize,
    /// The first of them, as `subtask N, key-group K: <defect>`.
    pub first_malformed: Option<String>,
}

/// `"N state file(s) could not be read: <the first few>"`.
fn unreadable(errors: &[String]) -> String {
    format!(
        "{} state file(s) could not be read: {}",
        errors.len(),
        crate::state_file::summarize(errors, 5)
    )
}

/// Every operator holding a keyed state with this name, as `(hex operator id, state type)`.
///
/// An error, naming the files, when some state file could not be read: the answer could then be
/// incomplete.
pub fn find_state(
    savepoint: &Savepoint,
    savepoint_dir: &Path,
    state_name: &str,
) -> Result<Vec<(String, String)>, String> {
    let listing = list_states(savepoint, savepoint_dir);
    if !listing.errors.is_empty() {
        return Err(unreadable(&listing.errors));
    }
    Ok(listing
        .states
        .into_iter()
        .filter(|(_, name, _)| name == state_name)
        .map(|(op, _, state_type)| (op, state_type))
        .collect())
}

/// What [`list_states`] found, and the state files it could not read.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StateListing {
    /// `(hex operator id, state name, state type)`, in savepoint order.
    pub states: Vec<(String, String, String)>,
    /// One message per missing, unreadable or truncated state file, naming the file. Non-empty
    /// means `states` may lack some of those operators' states.
    pub errors: Vec<String>,
}

/// List every keyed state name in the savepoint, reading only the header of each subtask stream.
pub fn list_states(savepoint: &Savepoint, savepoint_dir: &Path) -> StateListing {
    let mut listing = StateListing::default();
    for (op_index, op) in savepoint.operators.iter().enumerate() {
        let ctx = OpContext::open(savepoint, op_index, savepoint_dir);
        listing.errors.extend(ctx.open_errors());
        let op_hex = hex_id(&op.id);
        let mut seen = std::collections::HashSet::new();
        // Every subtask's header, not only the first one: a first file that cannot be read must
        // not hide the operator, and a state registered lazily exists only in the subtasks that
        // touched it.
        for stream in &ctx.streams {
            let header = match &stream.header {
                Ok(h) => h,
                Err(e) => {
                    listing.errors.push(e.clone());
                    continue;
                }
            };
            for st in &header.states {
                if seen.insert(st.name.clone()) {
                    listing
                        .states
                        .push((op_hex.clone(), st.name.clone(), st.state_type.clone()));
                }
            }
        }
    }
    listing
}

/// One exported entry, handed to a [`RowSink`] as it is read.
///
/// Borrowed rather than owned: at a billion rows, allocating a row per entry is the difference
/// between an export that streams and one that churns.
pub struct StateRow<'a> {
    pub operator: &'a str,
    pub subtask: i32,
    pub state: &'a str,
    pub state_type: &'a str,
    /// The value serializer type, e.g. `Long`, `Pojo`, `Map<String,Pojo>` — a sink may use it to
    /// pick a column type.
    pub value_type: &'a str,
    pub key: &'a str,
    /// Absolute key-group.
    pub key_group: u16,
    /// For MAP states: the decoded map entry key.
    pub map_key: Option<&'a str>,
    /// Namespace bytes when they carry more than the 1-byte VoidNamespace (e.g. a window).
    pub namespace_hex: Option<&'a str>,
    pub value: &'a Value,
}

/// Where exported rows go. Implemented by [`NdjsonSink`] and, with the `parquet` feature, by
/// `ParquetSink` — the reading side is shared, so a new format is a sink and nothing else.
pub trait RowSink {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String>;
    /// Flush whatever is buffered. Always called on success.
    fn finish(&mut self) -> Result<(), String>;
}

/// One JSON object per line, keys in alphabetical order (serde_json's map ordering).
pub struct NdjsonSink<W: Write> {
    out: W,
    line: Vec<u8>,
}

impl<W: Write> NdjsonSink<W> {
    pub fn new(out: W) -> Self {
        Self {
            out,
            line: Vec::new(),
        }
    }
}

impl<W: Write> RowSink for NdjsonSink<W> {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        let mut obj = json!({
            "operator": row.operator,
            "subtask": row.subtask,
            "state": row.state,
            "state_type": row.state_type,
            "key": row.key,
            "key_group": row.key_group,
            "value": row.value,
        });
        let map = obj.as_object_mut().unwrap();
        if let Some(mk) = row.map_key {
            map.insert("map_key".into(), Value::String(mk.to_string()));
        } else if let Some(ns) = row.namespace_hex {
            map.insert("namespace_hex".into(), Value::String(ns.to_string()));
        }
        self.line.clear();
        serde_json::to_writer(&mut self.line, &obj).map_err(|e| format!("serialize row: {}", e))?;
        self.line.push(b'\n');
        self.out
            .write_all(&self.line)
            .map_err(|e| format!("write: {}", e))
    }

    fn finish(&mut self) -> Result<(), String> {
        self.out.flush().map_err(|e| format!("flush: {}", e))
    }
}

/// Stream one keyed state to NDJSON (one JSON object per line).
pub fn export_state_ndjson<W: Write>(
    savepoint: &Savepoint,
    savepoint_dir: &Path,
    operator: Option<&str>,
    state_name: &str,
    proto: Option<&ProtoContext>,
    out: &mut W,
) -> Result<ExportStats, String> {
    let mut sink = NdjsonSink::new(out);
    export_state_rows(
        savepoint,
        savepoint_dir,
        operator,
        state_name,
        proto,
        &mut sink,
    )
}

/// Stream one keyed state into any [`RowSink`].
///
/// `operator` optionally restricts the export to one operator, matched as a prefix of its hex id;
/// without it the state name must be unambiguous across operators.
pub fn export_state_rows<S: RowSink + ?Sized>(
    savepoint: &Savepoint,
    savepoint_dir: &Path,
    operator: Option<&str>,
    state_name: &str,
    proto: Option<&ProtoContext>,
    sink: &mut S,
) -> Result<ExportStats, String> {
    let mut stats = ExportStats::default();

    let candidates: Vec<usize> = (0..savepoint.operators.len())
        .filter(|&i| {
            operator.is_none_or(|want| hex_id(&savepoint.operators[i].id).starts_with(want))
        })
        .collect();
    if candidates.is_empty() {
        return Err(format!(
            "no operator matches {:?}",
            operator.unwrap_or_default()
        ));
    }

    // Resolve the state once per operator, from headers only, so an ambiguous name — or a state
    // file that cannot be read — fails before any data is read rather than halfway through a
    // multi-gigabyte export. Every subtask's stream is opened (which checks its file) and the
    // state is resolved from the first header holding it, so a bad first file is reported as
    // such instead of as "state not found".
    let mut targets: Vec<(OpContext, Vec<String>)> = Vec::new();
    // Operators without the state in their readable headers, but with files that could not be
    // read: the state may be in those.
    let mut unread_elsewhere: Vec<(String, Vec<String>)> = Vec::new();
    for op_index in candidates {
        let ctx = OpContext::open(savepoint, op_index, savepoint_dir);
        let mut errors = ctx.open_errors();
        let mut resolved = false;
        for stream in &ctx.streams {
            match &stream.header {
                Ok(header) => {
                    resolved = header.state_id(state_name).is_some();
                    if resolved {
                        break;
                    }
                }
                Err(e) => errors.push(e.clone()),
            }
        }
        match resolved {
            true => targets.push((ctx, errors)),
            false if !errors.is_empty() => {
                unread_elsewhere.push((hex_id(&savepoint.operators[op_index].id), errors))
            }
            false => {}
        }
    }

    match targets.len() {
        0 if !unread_elsewhere.is_empty() => {
            let errors: Vec<String> = unread_elsewhere.into_iter().flat_map(|(_, e)| e).collect();
            return Err(format!(
                "state {:?} not found in the keyed state that could be read; {}",
                state_name,
                unreadable(&errors)
            ));
        }
        0 => {
            return Err(format!(
                "state {:?} not found in any keyed operator",
                state_name
            ))
        }
        1 => {}
        _ => {
            let ops: Vec<String> = targets
                .iter()
                .map(|(ctx, _)| hex_id(&savepoint.operators[ctx.op_index].id))
                .collect();
            return Err(format!(
                "state {:?} exists in {} operators ({}); narrow it with --operator <hex prefix>",
                state_name,
                ops.len(),
                ops.join(", ")
            ));
        }
    }

    let (ctx, errors) = targets.remove(0);
    let op = &savepoint.operators[ctx.op_index];
    let op_hex = hex_id(&op.id);
    if !errors.is_empty() {
        return Err(format!(
            "operator {} holds {:?}, but {}; the export would miss those subtasks' rows",
            op_hex,
            state_name,
            unreadable(&errors)
        ));
    }
    if !unread_elsewhere.is_empty() {
        let ops: Vec<&str> = unread_elsewhere.iter().map(|(op, _)| op.as_str()).collect();
        let errors: Vec<String> = unread_elsewhere
            .iter()
            .flat_map(|(_, e)| e.clone())
            .collect();
        return Err(format!(
            "state {:?} is in operator {}, but operator(s) {} may hold it too: {}; pass \
             --operator {} to export it from {} alone",
            state_name,
            op_hex,
            ops.join(", "),
            unreadable(&errors),
            op_hex,
            op_hex
        ));
    }

    // Reused across every key-group: the export's entire data footprint.
    let mut raw = Vec::new();
    let mut inflated = Vec::new();

    let kg_prefix_len = kg_prefix_len(op.max_parallelism);
    let op_index = ctx.op_index;
    // By value: each subtask's file is closed once its key-groups are exported.
    for stream in ctx.streams {
        let subtask_index = stream.subtask_index;
        if stream.src.len() == 0 {
            continue;
        }
        stats.subtasks += 1;

        let header = stream.header.as_ref().map_err(String::clone)?;
        let compressed = header.compressed;
        stats.compressed |= compressed;

        // A subtask can register its states in its own order, so re-resolve per subtask rather
        // than reusing the first subtask's state id.
        // A subtask whose header does not register the state holds none of its entries: reading
        // it with another subtask's state id would decode a different state.
        let Some(state_id) = header.state_id(state_name) else {
            continue;
        };
        let sub = &header.states[state_id as usize];

        for i in 0..stream.offsets.len() {
            let Some(range) = stream.block_range(i) else {
                continue;
            };
            let bytes = stream.read_block(savepoint, op_index, range, &mut raw)?;
            let block = key_group_block(bytes, compressed, &mut inflated)
                .map_err(|e| format!("key-group {} snappy decode: {}", i, e))?;
            stats.max_block_bytes = stats.max_block_bytes.max(block.len());
            stats.key_groups += 1;

            // Absolute key-group, not the index within this subtask's range.
            let absolute_kg = (stream.start_key_group + i as i32) as u16;

            let mut err: Option<String> = None;
            let parsed = parse_key_group_block(
                block,
                absolute_kg,
                &header.parse_ctx(kg_prefix_len, Some(state_id)),
                &mut |entry| {
                    if err.is_some() {
                        return;
                    }
                    // A MAP state stores **one key-group entry per map entry**, with the user map
                    // key in the namespace and the value wrapped by NullableSerializer — it is
                    // not the blob `index::builder` assembles per key. So a map exports exploded,
                    // one row per entry, which is also what keeps rows bounded when a single key
                    // holds millions of entries.
                    let (map_key, value) = decode_entry_value(
                        sub,
                        &entry.namespace,
                        &entry.value,
                        proto,
                        stats.rows as usize,
                    );
                    // A plain VALUE state has a 1-byte VoidNamespace; anything longer carries
                    // information (e.g. a window namespace) we cannot type yet, so keep it.
                    let namespace_hex = if map_key.is_none() && entry.namespace.len() > 1 {
                        Some(
                            entry
                                .namespace
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>(),
                        )
                    } else {
                        None
                    };
                    let row = StateRow {
                        operator: &op_hex,
                        subtask: subtask_index,
                        state: &sub.name,
                        state_type: &sub.state_type,
                        value_type: &sub.value_type,
                        key: &entry.key,
                        key_group: entry.key_group,
                        map_key: map_key.as_deref(),
                        namespace_hex: namespace_hex.as_deref(),
                        value: &value,
                    };
                    if let Err(e) = sink.row(&row) {
                        err = Some(e);
                        return;
                    }
                    stats.rows += 1;
                },
            );
            if let Some(e) = err {
                return Err(e);
            }
            // The rows read before the defect are out; the rest of this key-group cannot be
            // located. Count it so the caller can say the export is incomplete.
            if let Err(defect) = parsed {
                stats.malformed_key_groups += 1;
                stats.first_malformed.get_or_insert_with(|| {
                    format!(
                        "subtask {}, key-group {}: {}",
                        subtask_index, absolute_kg, defect
                    )
                });
            }
        }
    }

    sink.finish()?;
    Ok(stats)
}
