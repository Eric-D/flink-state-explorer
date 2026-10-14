//! The TUI's `E` export: every operator (with its info and decoded operator states) and every key
//! of every keyed state with its values, as one JSON document — **streamed** from the
//! [`StateReader`], a page of keys at a time, so memory stays bounded by a page (and the blocks
//! the reader holds), never by the savepoint. Every key of every subtask is written: there is no
//! cap and no `truncated` flag any more.
//!
//! Values are decoded by [`crate::parser::value`] (through the reader), the single decoder: a
//! MAP state is one object of all its entries (`map key → value`), any other state the value of
//! its first non-empty entry (a key with several windows or timers shows the first one, as the
//! TUI does).

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{json, Map, Value};

use crate::operators::{KeyedInfo, OperatorSummary};
use crate::reader::{Cursor, KeyRecord, StateEntry, StateReader};

/// Keys read per page while exporting.
const EXPORT_PAGE_KEYS: usize = 1000;

/// What [`export_json`] wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExportSummary {
    pub operators: usize,
    pub keys: usize,
    /// State files and key-groups that could not be read (their keys are missing), naming them.
    pub errors: Vec<String>,
    /// Key-groups that could not be read to their end mark.
    pub malformed: Vec<String>,
}

/// The value one key holds for one state, from its entries of that state (in on-disk order):
/// a MAP's entries gathered into one object (`map key → value`), otherwise the first entry whose
/// bytes are not empty. `None` when the key holds nothing for the state.
pub fn state_value(state_type: &str, entries: &[&StateEntry]) -> Option<Value> {
    if state_type == "MAP" {
        if entries.is_empty() {
            return None;
        }
        let mut obj = Map::new();
        for e in entries {
            obj.insert(e.map_key.clone().unwrap_or_default(), e.value.clone());
        }
        Some(Value::Object(obj))
    } else {
        entries
            .iter()
            .find(|e| !e.raw.is_empty())
            .map(|e| e.value.clone())
    }
}

/// Every entry of `state` of `record`: those it carries, then — when the reader cut the state at
/// its entry limit — the rest, read with [`StateReader::entries`].
pub fn complete_entries(
    reader: &StateReader,
    op: &str,
    record: &KeyRecord,
    state: &str,
) -> Result<Vec<StateEntry>, String> {
    let mut entries: Vec<StateEntry> = record
        .entries
        .iter()
        .filter(|e| &*e.state == state)
        .cloned()
        .collect();
    if let Some(t) = record.truncated.iter().find(|t| &*t.state == state) {
        let mut offset = entries.len();
        while offset < t.total {
            let page = reader.entries(op, &record.position, state, offset, 10_000)?;
            if page.entries.is_empty() {
                break;
            }
            offset += page.entries.len();
            entries.extend(page.entries);
            if page.next.is_none() {
                break;
            }
        }
    }
    Ok(entries)
}

/// Write the export to `output_path` (through `<output_path>.partial`, renamed on success, so a
/// failed or cancelled export never leaves a half-written file under the real name). `progress`
/// is called with the number of keys written so far; `cancel` stops it between pages.
pub fn export_json(
    reader: &StateReader,
    operators: &[OperatorSummary],
    output_path: &Path,
    cancel: &AtomicBool,
    progress: &mut dyn FnMut(usize),
) -> Result<ExportSummary, String> {
    let mut tmp = output_path.as_os_str().to_owned();
    tmp.push(".partial");
    let tmp = std::path::PathBuf::from(tmp);
    let result = (|| {
        let file =
            std::fs::File::create(&tmp).map_err(|e| format!("create {}: {}", tmp.display(), e))?;
        let mut w = std::io::BufWriter::new(file);
        let summary = write_json(reader, operators, &mut w, cancel, progress)?;
        w.flush()
            .map_err(|e| format!("write {}: {}", tmp.display(), e))?;
        Ok(summary)
    })();
    match result {
        Ok(summary) => {
            std::fs::rename(&tmp, output_path).map_err(|e| {
                format!(
                    "rename {} to {}: {}",
                    tmp.display(),
                    output_path.display(),
                    e
                )
            })?;
            Ok(summary)
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn io<E: std::fmt::Display>(e: E) -> String {
    format!("write error: {}", e)
}

fn push_unique(list: &mut Vec<String>, items: impl IntoIterator<Item = String>) {
    for i in items {
        if !list.contains(&i) {
            list.push(i);
        }
    }
}

/// Stream the export document into `w`. Layout:
///
/// ```text
/// { "checkpoint_id": …, "operators": [
///   { "name", "parallelism", "operator_states": [{name, mode, values}],
///     "state_descriptors": [{name, state_type, value_type, value_class}],
///     "keys": [ {"key", "key_group", "states": {state: value}}, … ],
///     "total_keys", "load_errors", "malformed_key_groups", "info": {…} }, … ] }
/// ```
///
/// `total_keys`, the errors and `info` come after `keys` because they are only known once every
/// key has been read.
pub fn write_json<W: Write>(
    reader: &StateReader,
    operators: &[OperatorSummary],
    w: &mut W,
    cancel: &AtomicBool,
    progress: &mut dyn FnMut(usize),
) -> Result<ExportSummary, String> {
    let mut summary = ExportSummary::default();
    write!(
        w,
        "{{\n  \"checkpoint_id\": {},\n  \"operators\": [",
        reader.savepoint().checkpoint_id
    )
    .map_err(io)?;
    for (n, op) in operators.iter().enumerate() {
        let hex = op.operator_id_hex();
        let keyed = reader.states(&hex).map_err(String::from)?;
        let mut errors = keyed.errors.clone();
        let mut malformed = Vec::new();

        let operator_states: Vec<Value> = op
            .states
            .iter()
            .map(|s| {
                let values: Vec<Value> = s
                    .elements
                    .iter()
                    .map(|e| Value::String(e.label.clone()))
                    .collect();
                json!({ "name": s.name, "mode": s.state_type, "values": values })
            })
            .collect();
        let descriptors: Vec<Value> = keyed
            .states
            .iter()
            .map(|d| {
                json!({
                    "name": d.name,
                    "state_type": d.state_type,
                    "value_type": d.value_type,
                    // Fully-qualified value class (package + name) when the value is a POJO/Enum,
                    // read from the serializer snapshot.
                    "value_class": d.value_class,
                })
            })
            .collect();
        write!(
            w,
            "{}\n    {{\n      \"name\": {},\n      \"parallelism\": {},\n      \
             \"operator_states\": {},\n      \"state_descriptors\": {},\n      \"keys\": [",
            if n > 0 { "," } else { "" },
            Value::String(op.display_name.clone()),
            op.parallelism,
            Value::Array(operator_states),
            Value::Array(descriptors),
        )
        .map_err(io)?;

        let mut total = 0usize;
        if !keyed.states.is_empty() {
            let mut cursor = Some(Cursor::START);
            while let Some(c) = cursor {
                if cancel.load(Ordering::Relaxed) {
                    return Err("export cancelled".to_string());
                }
                let page = reader
                    .key_page(&hex, &c, EXPORT_PAGE_KEYS)
                    .map_err(String::from)?;
                push_unique(&mut errors, page.unreadable.iter().map(|u| u.error.clone()));
                push_unique(&mut malformed, page.malformed.iter().cloned());
                for record in &page.keys {
                    let mut states = Map::new();
                    for d in &keyed.states {
                        let entries = if record.truncated.iter().any(|t| *t.state == *d.name) {
                            complete_entries(reader, &hex, record, &d.name)?
                        } else {
                            record
                                .entries
                                .iter()
                                .filter(|e| *e.state == *d.name)
                                .cloned()
                                .collect()
                        };
                        let refs: Vec<&StateEntry> = entries.iter().collect();
                        states.insert(
                            d.name.clone(),
                            state_value(&d.state_type, &refs).unwrap_or(Value::Null),
                        );
                    }
                    let line = json!({
                        "key": record.key,
                        "key_group": record.key_group,
                        "states": states,
                    });
                    write!(w, "{}\n        {}", if total > 0 { "," } else { "" }, line)
                        .map_err(io)?;
                    total += 1;
                }
                summary.keys += page.keys.len();
                progress(summary.keys);
                cursor = page.next;
            }
        }

        let info: Map<String, Value> = op
            .info_lines(Some(&KeyedInfo {
                key_type: keyed.key_type.clone(),
                state_names: keyed.states.iter().map(|s| s.name.clone()).collect(),
                keys: total.to_string(),
                errors: errors.clone(),
                malformed: malformed.clone(),
            }))
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        write!(
            w,
            "{}],\n      \"total_keys\": {},\n      \"load_errors\": {},\n      \
             \"malformed_key_groups\": {},\n      \"info\": {}\n    }}",
            if total > 0 { "\n      " } else { "" },
            total,
            json!(errors),
            json!(malformed),
            Value::Object(info),
        )
        .map_err(io)?;
        push_unique(&mut summary.errors, errors);
        push_unique(&mut summary.malformed, malformed);
        summary.operators += 1;
    }
    writeln!(w, "\n  ]\n}}").map_err(io)?;
    Ok(summary)
}
