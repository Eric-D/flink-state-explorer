//! What a savepoint says about each operator apart from its keyed state: metadata (parallelism,
//! coordinator state, sizes) and its **operator** state (list / union / broadcast states, Kafka
//! connector splits and transactions), decoded element by element.
//!
//! Keyed state is not here: it is read lazily, key-group by key-group, by
//! [`crate::reader::StateReader`]. Operator state is small by design (it is held by every subtask
//! and redistributed on rescale), so it is read whole when the savepoint is opened.

use std::borrow::Cow;
use std::path::Path;

use crate::parser::metadata::{Operator, Savepoint, SubtaskState};
use crate::parser::operator_state::OperatorStreamElements;
use crate::parser::state_handle::{OperatorStreamStateHandle, StateHandle, StreamStateHandle};

/// One operator, without its keyed state.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorSummary {
    pub id: [u8; 16],
    pub display_name: String,
    /// Flink operator identifier (metadata v5+), else empty.
    pub identifier_name: String,
    pub parallelism: i32,
    pub max_parallelism: i32,
    pub subtask_count: usize,
    /// Keyed state data size in bytes, as the handles record it.
    pub keyed_state_size: u64,
    pub fully_finished: bool,
    pub has_coordinator_state: bool,
    /// Decoded operator-coordinator state when recognized (a Kafka source enumerator's assigned
    /// partitions); `None` if absent or unrecognized.
    pub coordinator_info: Option<String>,
    /// Non-keyed states (managed-operator, raw-operator).
    pub states: Vec<OperatorState>,
}

/// One operator state: its elements, decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorState {
    pub name: String,
    /// `operator/SPLIT_DISTRIBUTE`, `operator/UNION`, `operator/BROADCAST`, `raw-operator`.
    pub state_type: String,
    pub elements: Vec<OperatorElement>,
}

/// One operator-state element.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorElement {
    /// The element decoded for display (Kafka split, broadcast map, string, byte preview), or
    /// `[error: …]` when it could not be read.
    pub label: String,
    /// Its bytes (inflated when the stream is compressed), for the hex view; `None` when the
    /// element could not be read, or lives in a file this reader does not open.
    pub bytes: Option<Vec<u8>>,
}

/// Every operator of `savepoint`, in metadata order. Operator-state streams are read from
/// wherever their handle keeps them (inline, relative or absolute file, segment, compressed);
/// one that cannot be read gives its states a single `[error: …]` element.
pub fn summarize(savepoint: &Savepoint, savepoint_dir: &Path) -> Vec<OperatorSummary> {
    savepoint
        .operators
        .iter()
        .map(|op| summarize_operator(op, savepoint_dir))
        .collect()
}

fn summarize_operator(op: &Operator, savepoint_dir: &Path) -> OperatorSummary {
    let mut states = Vec::new();
    for sub in &op.subtask_states {
        collect_operator_states(sub, savepoint_dir, &mut states);
    }
    OperatorSummary {
        id: op.id,
        display_name: op.display_name(),
        identifier_name: op.identifier_name.clone(),
        parallelism: op.parallelism,
        max_parallelism: op.max_parallelism,
        subtask_count: op.subtask_states.len(),
        keyed_state_size: op
            .subtask_states
            .iter()
            .map(|sub| keyed_state_handle_size(&sub.managed_keyed_state))
            .sum(),
        fully_finished: op.fully_finished,
        has_coordinator_state: op.coordinator_state.is_some(),
        coordinator_info: op
            .coordinator_state
            .as_ref()
            .and_then(|cs| crate::parser::kafka::decode_enumerator_state(cs)),
        states,
    }
}

/// What the keyed state of an operator looks like so far, for [`OperatorSummary::info_lines`]:
/// the TUI knows it from the reader's headers and from what it has paged or counted.
#[derive(Debug, Clone, Default)]
pub struct KeyedInfo {
    pub key_type: String,
    pub state_names: Vec<String>,
    /// `"1000000"`, `"≥ 120 (counting 37%)"`, …
    pub keys: String,
    /// State files or key-groups that could not be read, naming them.
    pub errors: Vec<String>,
    /// Key-groups that could not be read to their end mark.
    pub malformed: Vec<String>,
}

impl OperatorSummary {
    /// The OperatorID as a 32-char hex string (Flink's `OperatorID` form).
    pub fn operator_id_hex(&self) -> String {
        crate::reader::hex_id(&self.id)
    }

    /// All extractable operator metadata as (label, value) pairs, for display/export.
    ///
    /// Note: a savepoint's metadata stores only the **OperatorID** (a hash of the uid); the
    /// operator's human name/uid appear only in metadata v5+, and the Java class is never stored.
    pub fn info_lines(&self, keyed: Option<&KeyedInfo>) -> Vec<(String, String)> {
        let id_hex = self.operator_id_hex();
        let unknown = "(not in this savepoint's metadata)".to_string();
        let mut v = vec![
            ("OperatorID (uuid)".to_string(), id_hex.clone()),
            (
                "Name".to_string(),
                if self.display_name != id_hex && !self.display_name.is_empty() {
                    self.display_name.clone()
                } else {
                    unknown.clone()
                },
            ),
            (
                "uid / identifier".to_string(),
                if self.identifier_name.is_empty() {
                    unknown.clone()
                } else {
                    self.identifier_name.clone()
                },
            ),
            ("Parallelism".to_string(), self.parallelism.to_string()),
            (
                "Max parallelism".to_string(),
                self.max_parallelism.to_string(),
            ),
            ("Subtasks".to_string(), self.subtask_count.to_string()),
            (
                "Coordinator state".to_string(),
                if self.has_coordinator_state {
                    "yes".to_string()
                } else {
                    "no".to_string()
                },
            ),
            (
                "Fully finished".to_string(),
                self.fully_finished.to_string(),
            ),
            (
                "Keyed state size".to_string(),
                format!("{} bytes", self.keyed_state_size),
            ),
        ];
        if let Some(k) = keyed.filter(|k| !k.errors.is_empty()) {
            // Right after the size it explains: the bytes are there, the keys are not.
            v.push((
                "Load errors".to_string(),
                format!(
                    "{} state file(s) or key-group(s) could not be read, their keys are missing: \
                     {}",
                    k.errors.len(),
                    crate::state_file::summarize(&k.errors, 3)
                ),
            ));
        }
        if let Some(info) = &self.coordinator_info {
            v.push(("Source enumerator".to_string(), info.clone()));
        }
        if let Some(k) = keyed.filter(|k| !k.state_names.is_empty() || !k.key_type.is_empty()) {
            v.push(("Key type".to_string(), k.key_type.clone()));
            v.push(("Keys".to_string(), k.keys.clone()));
            v.push((
                "Keyed states".to_string(),
                format!("{} [{}]", k.state_names.len(), k.state_names.join(", ")),
            ));
            if !k.malformed.is_empty() {
                v.push((
                    "Malformed key-groups".to_string(),
                    format!(
                        "{} — entries past the defect were not read (first: {})",
                        k.malformed.len(),
                        k.malformed[0]
                    ),
                ));
            }
        }
        if !self.states.is_empty() {
            let os: Vec<String> = self
                .states
                .iter()
                .map(|s| format!("{} ({})", s.name, s.state_type))
                .collect();
            v.push(("Operator states".to_string(), os.join(", ")));
        }
        v
    }
}

/// Collect non-keyed operator states (managed-operator, raw-operator).
fn collect_operator_states(
    sub: &SubtaskState,
    savepoint_dir: &Path,
    states: &mut Vec<OperatorState>,
) {
    // Managed operator state is always an `OperatorStreamStateHandle` (or nothing).
    if let Some(StateHandle::OperatorStream(os)) = &sub.managed_operator_state {
        push_operator_stream_states(os, savepoint_dir, states);
    }
    if let Some(handle) = &sub.raw_operator_state {
        let elements = raw_operator_elements(handle);
        if !elements.is_empty() {
            states.push(OperatorState {
                name: format!("raw-operator-subtask{}", sub.subtask_index),
                state_type: "raw-operator".to_string(),
                elements,
            });
        }
    }
}

/// One [`OperatorState`] per registered operator state, labelled with its distribution mode
/// (`SPLIT_DISTRIBUTE` / `UNION` / `BROADCAST`) and with one element each, decoded. The stream
/// is read wherever its delegate keeps it and inflated when written with snapshot compression
/// (`parser::operator_state`); if it cannot be read, every state with elements gets a single
/// `[error: …]` element saying why.
fn push_operator_stream_states(
    os: &OperatorStreamStateHandle,
    savepoint_dir: &Path,
    states: &mut Vec<OperatorState>,
) {
    // The stream is only read when some state has elements in it: a handle whose states are
    // all empty may point at nothing readable.
    let has_elements = os.partition_offsets.iter().any(|p| !p.offsets.is_empty());
    let stream = if has_elements {
        operator_stream_bytes(&os.delegate_handle, savepoint_dir)
    } else {
        Ok(Cow::Borrowed(&[][..]))
    };
    let elements = stream
        .as_deref()
        .map_err(String::clone)
        .and_then(|bytes| OperatorStreamElements::new(bytes, &os.partition_offsets));

    for part in &os.partition_offsets {
        let is_broadcast = part.mode == 2; // OperatorStateHandle.Mode.BROADCAST
        let entries = match &elements {
            _ if part.offsets.is_empty() => Vec::new(),
            Err(e) => vec![unread(format!("[error: {}]", e))],
            Ok(elements) => part
                .offsets
                .iter()
                .map(|&offset| match elements.element(offset) {
                    Ok(elem) => OperatorElement {
                        label: label_operator_element(&part.state_name, is_broadcast, &elem),
                        // Kept for the TUI's hex view (`x`): a partly decoded element is
                        // checked against its bytes. Inflated when the stream is compressed.
                        bytes: Some(elem.to_vec()),
                    },
                    Err(e) => unread(format!("[error: {}]", e)),
                })
                .collect(),
        };
        states.push(OperatorState {
            name: part.state_name.clone(),
            state_type: format!("operator/{}", operator_mode_label(part.mode)),
            elements: entries,
        });
    }
}

/// An operator-state stream's bytes: borrowed when inline in `_metadata`, read otherwise.
fn operator_stream_bytes<'a>(
    delegate: &'a StreamStateHandle,
    savepoint_dir: &Path,
) -> Result<Cow<'a, [u8]>, String> {
    match delegate {
        StreamStateHandle::ByteStream(bs) => Ok(Cow::Borrowed(&bs.data)),
        other => crate::reader::read_whole_stream(other, savepoint_dir).map(Cow::Owned),
    }
}

/// Kafka connector states get connector-specific decoding; broadcast maps and other
/// operator-list elements are decoded best-effort.
fn label_operator_element(state_name: &str, is_broadcast: bool, elem: &[u8]) -> String {
    match state_name {
        "SourceReaderState" => crate::parser::kafka::decode_source_reader_split(elem),
        "writer_raw_states" => crate::parser::kafka::decode_writer_state(elem),
        "streaming_committer_raw_states" => crate::parser::kafka::decode_committer_state(elem),
        _ if is_broadcast => decode_broadcast_map(elem),
        _ => None,
    }
    .unwrap_or_else(|| decode_operator_element(elem))
}

/// An element that could not be read: its label is all there is.
fn unread(label: String) -> OperatorElement {
    OperatorElement { label, bytes: None }
}

/// Flink `OperatorStateHandle.Mode` ordinal → name.
fn operator_mode_label(mode: i32) -> &'static str {
    match mode {
        0 => "SPLIT_DISTRIBUTE",
        1 => "UNION",
        2 => "BROADCAST",
        _ => "?",
    }
}

/// Best-effort decode of a broadcast-state element (`HeapBroadcastState`): `writeInt(size)` then
/// `key,value` pairs. Assumes String key/value (the common case); returns None to fall back to a
/// byte preview when the payload isn't String/String.
fn decode_broadcast_map(bytes: &[u8]) -> Option<String> {
    use crate::parser::java_deser::JavaReader;
    let mut r = JavaReader::new(bytes);
    let size = r.read_int().ok()?;
    if !(0..=100_000).contains(&size) {
        return None;
    }
    let mut pairs = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let k = r.read_string_value().ok()?;
        let v = r.read_string_value().ok()?;
        pairs.push(format!("{:?}: {:?}", k, v));
    }
    // The pairs should consume essentially all the bytes — otherwise it wasn't String/String.
    if (r.position() as usize) < bytes.len().saturating_sub(2) {
        return None;
    }
    Some(format!("{{ {} }}", pairs.join(", ")))
}

/// Best-effort decode of one operator-state element for display. Most operator `ListState`
/// elements are `StringValue`; anything else is shown as a short byte preview.
fn decode_operator_element(bytes: &[u8]) -> String {
    use crate::parser::java_deser::JavaReader;
    if bytes.is_empty() {
        return "(empty)".to_string();
    }
    let mut r = JavaReader::new(bytes);
    if let Ok(s) = r.read_string_value() {
        let consumed = r.position() as usize;
        if !s.is_empty()
            && consumed >= bytes.len().saturating_sub(1)
            && s.chars().all(|c| !c.is_control())
        {
            return format!("\"{}\"", s);
        }
    }
    let preview: String = bytes
        .iter()
        .take(16)
        .map(|b| format!("{:02x}", b))
        .collect();
    format!("[{} bytes] {}", bytes.len(), preview)
}

fn keyed_state_handle_size(handle: &StateHandle) -> u64 {
    fn stream_size(h: &StreamStateHandle) -> u64 {
        match h {
            StreamStateHandle::ByteStream(bs) => bs.data.len() as u64,
            // Flink records -1 when it could not tell the size: count it as unknown (0).
            StreamStateHandle::Relative(rel) => rel.state_size.max(0) as u64,
            StreamStateHandle::File(f) => f.state_size.max(0) as u64,
            _ => 0,
        }
    }
    match handle {
        StateHandle::KeyGroups(kg) => match &kg.delegate_handle {
            StreamStateHandle::KeyGroups(nested) => stream_size(&nested.delegate_handle),
            other => stream_size(other),
        },
        _ => 0,
    }
}

/// Raw operator state (written by a user through raw streams, rare): one element per partition
/// offset, labelled `name[i]`, with its bytes when they are inline in `_metadata`.
fn raw_operator_elements(handle: &StateHandle) -> Vec<OperatorElement> {
    let StateHandle::OperatorStream(os) = handle else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for part in &os.partition_offsets {
        for (i, &offset) in part.offsets.iter().enumerate() {
            let bytes = match &os.delegate_handle {
                StreamStateHandle::ByteStream(bs) => {
                    let start = offset.max(0) as usize;
                    let end = part
                        .offsets
                        .get(i + 1)
                        .map(|&o| o.max(0) as usize)
                        .unwrap_or(bs.data.len())
                        .min(bs.data.len());
                    (start < end).then(|| bs.data[start..end].to_vec())
                }
                _ => None,
            };
            out.push(OperatorElement {
                label: format!("{}[{}]", part.state_name, i),
                bytes,
            });
        }
    }
    out
}
