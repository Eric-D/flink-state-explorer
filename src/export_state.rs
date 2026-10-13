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

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::parser::keyed_state::{
    extract_state_metadata, extract_value_pojo_classes, extract_value_serializer_types,
    key_group_block, parse_key_group_block, read_keyed_stream_header, KeyGroupParse,
};
use crate::parser::metadata::{Operator, Savepoint};
use crate::parser::pojo::{extract_pojo_infos, PojoInfo};
use crate::parser::state_handle::{KeyGroupsStateHandle, StateHandle, StreamStateHandle};
use crate::proto::ProtoContext;

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
}

/// A keyed state located in the savepoint, with everything needed to decode its values.
struct ResolvedState {
    state_id: u16,
    name: String,
    state_type: String,
    value_type: String,
    pojo: Option<PojoInfo>,
    key_type: String,
    key_pojo: Option<PojoInfo>,
    state_types: Vec<String>,
    num_states: usize,
    /// For MAP states: the `Map<K,V>` key and value serializer types.
    map_key_type: String,
    map_value_type: String,
}

/// Where a subtask's keyed stream lives, and how to read a byte range out of it.
enum Source<'a> {
    Mem(&'a [u8]),
    File { path: PathBuf, base: u64, len: u64 },
}

impl Source<'_> {
    fn len(&self) -> u64 {
        match self {
            Source::Mem(d) => d.len() as u64,
            Source::File { len, .. } => *len,
        }
    }

    /// Read `[start, end)` of the logical stream into `buf`.
    fn read_range(&self, start: u64, end: u64, buf: &mut Vec<u8>) -> Result<(), String> {
        let end = end.min(self.len());
        if end <= start {
            buf.clear();
            return Ok(());
        }
        let len = (end - start) as usize;
        match self {
            Source::Mem(d) => {
                buf.clear();
                buf.extend_from_slice(&d[start as usize..end as usize]);
            }
            Source::File { path, base, .. } => {
                let mut f = std::fs::File::open(path)
                    .map_err(|e| format!("open {}: {}", path.display(), e))?;
                f.seek(SeekFrom::Start(base + start))
                    .map_err(|e| format!("seek {}: {}", path.display(), e))?;
                buf.clear();
                buf.resize(len, 0);
                f.read_exact(buf)
                    .map_err(|e| format!("read {}: {}", path.display(), e))?;
            }
        }
        Ok(())
    }
}

/// Where `delegate`'s bytes are. A file-backed stream is opened first, and must be as long as its
/// handle records ([`crate::state_file::open`]): a missing, unreadable or short file is an `Err`
/// naming it, never a stream that silently yields fewer rows. `Ok(None)`: no stream at all.
fn source_for<'a>(
    delegate: &'a StreamStateHandle,
    savepoint_dir: &Path,
) -> Result<Option<Source<'a>>, String> {
    match delegate {
        StreamStateHandle::ByteStream(bs) => Ok(Some(Source::Mem(&bs.data))),
        StreamStateHandle::Relative(rel) => {
            let path = savepoint_dir.join(&rel.relative_path);
            let (_, len) = crate::state_file::open(&path, rel.state_size)?;
            Ok(Some(Source::File { path, base: 0, len }))
        }
        StreamStateHandle::File(f) => {
            let path = PathBuf::from(&f.file_path);
            let (_, len) = crate::state_file::open(&path, f.state_size)?;
            Ok(Some(Source::File { path, base: 0, len }))
        }
        // A segment handle points at a region of a shared file. We treat the key-group offsets as
        // relative to `start_pos`, which matches the offsets being recorded on the logical
        // checkpoint output stream — NOT verified against a real segmented savepoint, so an
        // export from one should be sanity-checked before being trusted.
        StreamStateHandle::Segment(seg) => {
            let path = PathBuf::from(&seg.file_path);
            let base = seg.start_pos.max(0) as u64;
            let len = seg.state_size.max(0) as u64;
            // The segment must lie within its file.
            crate::state_file::open(&path, (base + len) as i64)?;
            Ok(Some(Source::File { path, base, len }))
        }
        StreamStateHandle::KeyGroups(_)
        | StreamStateHandle::Null
        | StreamStateHandle::EmptySegment => Ok(None),
    }
}

/// One subtask's keyed stream, opened and checked against its handle.
struct KeyedStream<'a> {
    subtask_index: i32,
    kg: &'a KeyGroupsStateHandle,
    src: Source<'a>,
}

impl KeyedStream<'_> {
    /// The stream's header, which runs from offset 0 to the first key-group.
    fn header(&self) -> Result<Vec<u8>, String> {
        let mut header = Vec::new();
        self.src
            .read_range(0, header_len(&self.kg.offsets, self.src.len()), &mut header)?;
        Ok(header)
    }
}

/// Open every keyed stream of `op`, in subtask order. A stream that cannot be opened goes to the
/// second list instead, as a message naming its file: a caller must never take a broken file for
/// a subtask without state.
fn open_keyed_streams<'a>(
    op: &'a Operator,
    savepoint_dir: &Path,
) -> (Vec<KeyedStream<'a>>, Vec<String>) {
    let mut streams = Vec::new();
    let mut errors = Vec::new();
    for (subtask_index, kg) in keyed_handles(op) {
        match source_for(effective_delegate(kg), savepoint_dir) {
            Ok(Some(src)) => streams.push(KeyedStream {
                subtask_index,
                kg,
                src,
            }),
            Ok(None) => {}
            Err(e) => errors.push(e),
        }
    }
    (streams, errors)
}

/// `"N state file(s) could not be read: <the first few>"`.
fn unreadable(errors: &[String]) -> String {
    format!(
        "{} state file(s) could not be read: {}",
        errors.len(),
        crate::state_file::summarize(errors, 5)
    )
}

/// Unwrap the delegate of a keyed handle, which may itself nest a key-groups handle.
fn effective_delegate(kg: &KeyGroupsStateHandle) -> &StreamStateHandle {
    match &kg.delegate_handle {
        StreamStateHandle::KeyGroups(nested) => &nested.delegate_handle,
        other => other,
    }
}

fn keyed_handles(op: &Operator) -> Vec<(i32, &KeyGroupsStateHandle)> {
    op.subtask_states
        .iter()
        .filter_map(|st| match &st.managed_keyed_state {
            StateHandle::KeyGroups(kg) => Some((st.subtask_index, kg)),
            _ => None,
        })
        .collect()
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

/// Resolve a state by name inside one operator, reading only that operator's header.
fn resolve_state(header: &[u8], state_name: &str) -> Option<ResolvedState> {
    let metas = extract_state_metadata(header);
    let idx = metas.iter().position(|m| m.name == state_name)?;
    let meta = &metas[idx];

    let value_types = extract_value_serializer_types(header);
    let value_pojo_classes = extract_value_pojo_classes(header);
    let pojo_infos = extract_pojo_infos(header);

    let is_timer = meta.state_type == "TIMER";
    // Mirrors `index::builder`: timers carry a synthesized firing timestamp, and must not be
    // matched against a nearby POJO snapshot.
    let value_type = if is_timer {
        "Date".to_string()
    } else {
        value_types.get(idx).cloned().unwrap_or_default()
    };
    let pojo = if is_timer {
        None
    } else {
        value_pojo_classes
            .get(idx)
            .and_then(|o| o.as_ref())
            .and_then(|cls| pojo_infos.iter().find(|p| &p.class_name == cls))
            .cloned()
    };

    let key_type = meta.key_type.clone();
    let key_pojo = if key_type == "Pojo" {
        crate::parser::keyed_state::extract_key_pojo_info(header)
    } else {
        None
    };
    let state_types: Vec<String> = metas.iter().map(|m| m.state_type.clone()).collect();
    let num_states = crate::parser::keyed_state::count_states_in_header(header)
        .max(metas.len())
        .max(1);

    let (map_key_type, map_value_type) = if meta.state_type == "MAP" {
        crate::parser::value::split_map_types(&value_type)
    } else {
        (String::new(), String::new())
    };

    Some(ResolvedState {
        state_id: idx as u16,
        name: meta.name.clone(),
        state_type: meta.state_type.clone(),
        value_type,
        pojo,
        key_type,
        key_pojo,
        state_types,
        num_states,
        map_key_type,
        map_value_type,
    })
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
    for op in &savepoint.operators {
        let (streams, errors) = open_keyed_streams(op, savepoint_dir);
        listing.errors.extend(errors);
        let op_hex = hex_id(&op.id);
        let mut seen = std::collections::HashSet::new();
        // Every subtask's header, not only the first one: a first file that cannot be read must
        // not hide the operator, and a state registered lazily exists only in the subtasks that
        // touched it.
        for stream in &streams {
            let header = match stream.header() {
                Ok(h) => h,
                Err(e) => {
                    listing.errors.push(e);
                    continue;
                }
            };
            for m in extract_state_metadata(&header) {
                if seen.insert(m.name.clone()) {
                    listing.states.push((op_hex.clone(), m.name, m.state_type));
                }
            }
        }
    }
    listing
}

pub fn hex_id(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{:02x}", b)).collect()
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

    let candidates: Vec<&Operator> = savepoint
        .operators
        .iter()
        .filter(|op| operator.is_none_or(|want| hex_id(&op.id).starts_with(want)))
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
    type Target<'a> = (
        &'a Operator,
        ResolvedState,
        Vec<KeyedStream<'a>>,
        Vec<String>,
    );
    let mut targets: Vec<Target> = Vec::new();
    // Operators without the state in their readable headers, but with files that could not be
    // read: the state may be in those.
    let mut unread_elsewhere: Vec<(String, Vec<String>)> = Vec::new();
    for op in candidates {
        let (streams, mut errors) = open_keyed_streams(op, savepoint_dir);
        let mut resolved = None;
        for stream in &streams {
            match stream.header() {
                Ok(header) => {
                    resolved = resolve_state(&header, state_name);
                    if resolved.is_some() {
                        break;
                    }
                }
                Err(e) => errors.push(e),
            }
        }
        match resolved {
            Some(rs) => targets.push((op, rs, streams, errors)),
            None if !errors.is_empty() => unread_elsewhere.push((hex_id(&op.id), errors)),
            None => {}
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
            let ops: Vec<String> = targets.iter().map(|(op, ..)| hex_id(&op.id)).collect();
            return Err(format!(
                "state {:?} exists in {} operators ({}); narrow it with --operator <hex prefix>",
                state_name,
                ops.len(),
                ops.join(", ")
            ));
        }
    }

    let (op, resolved, streams, errors) = targets.remove(0);
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

    for stream in &streams {
        let (subtask_index, kg, src) = (stream.subtask_index, stream.kg, &stream.src);
        let stream_len = src.len();
        if stream_len == 0 {
            continue;
        }
        stats.subtasks += 1;

        let hlen = header_len(&kg.offsets, stream_len);
        let header = stream.header()?;
        let compressed = read_keyed_stream_header(&header)
            .map(|h| h.using_key_group_compression)
            .unwrap_or(false);
        stats.compressed |= compressed;

        // A subtask can register its states in its own order, so re-resolve per subtask rather
        // than reusing the first subtask's state id.
        let sub = resolve_state(&header, state_name).unwrap_or_else(|| ResolvedState {
            state_id: resolved.state_id,
            name: resolved.name.clone(),
            state_type: resolved.state_type.clone(),
            value_type: resolved.value_type.clone(),
            pojo: resolved.pojo.clone(),
            key_type: resolved.key_type.clone(),
            key_pojo: resolved.key_pojo.clone(),
            state_types: resolved.state_types.clone(),
            num_states: resolved.num_states,
            map_key_type: resolved.map_key_type.clone(),
            map_value_type: resolved.map_value_type.clone(),
        });

        let kg_prefix_len = if op.max_parallelism > 128 { 2 } else { 1 };

        for (i, &offset) in kg.offsets.iter().enumerate() {
            let start = offset;
            if start <= 0 || (start as u64) < hlen || (start as u64) >= stream_len {
                continue;
            }
            // The next key-group starts where this one ends; empty key-groups repeat an offset.
            let end = kg.offsets[(i + 1)..]
                .iter()
                .copied()
                .find(|&o| o > start)
                .map(|o| o as u64)
                .unwrap_or(stream_len)
                .min(stream_len);
            if end <= start as u64 {
                continue;
            }

            src.read_range(start as u64, end, &mut raw)?;
            let block = key_group_block(&raw, compressed, &mut inflated)
                .map_err(|e| format!("key-group {} snappy decode: {}", i, e))?;
            stats.max_block_bytes = stats.max_block_bytes.max(block.len());
            stats.key_groups += 1;

            // Absolute key-group, not the index within this subtask's range.
            let absolute_kg = (kg.start_key_group + i as i32) as u16;

            let mut err: Option<String> = None;
            parse_key_group_block(
                block,
                absolute_kg,
                &KeyGroupParse {
                    num_states: sub.num_states,
                    key_type: &sub.key_type,
                    kg_prefix_len,
                    key_pojo: sub.key_pojo.as_ref(),
                    state_types: &sub.state_types,
                    only_state: Some(sub.state_id),
                },
                &mut |entry| {
                    if err.is_some() {
                        return;
                    }
                    // A MAP state stores **one key-group entry per map entry**, with the user map
                    // key in the namespace and the value wrapped by NullableSerializer — it is
                    // not the blob `index::builder` assembles per key. So a map exports exploded,
                    // one row per entry, which is also what keeps rows bounded when a single key
                    // holds millions of entries.
                    let (map_key, value) = if sub.state_type == "MAP" {
                        let (k, v) = crate::parser::value::decode_map_entry(
                            &entry.namespace,
                            &entry.value,
                            &sub.map_key_type,
                            &sub.map_value_type,
                            sub.pojo.as_ref(),
                            proto,
                            stats.rows as usize,
                        );
                        (Some(k), v)
                    } else {
                        (
                            None,
                            crate::parser::value::decode_state_value(
                                &entry.value,
                                &sub.value_type,
                                sub.pojo.as_ref(),
                                proto,
                            ),
                        )
                    };
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
        }
    }

    sink.finish()?;
    Ok(stats)
}
