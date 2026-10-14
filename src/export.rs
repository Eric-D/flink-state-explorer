//! The TUI's `E` export: every operator (with its info and decoded operator states) and every key
//! of every keyed state with its values, as one JSON document — **streamed** from the
//! [`StateReader`], a page of keys at a time, and a MAP state of one key a page of entries at a
//! time, so memory stays bounded by a page (and the blocks the reader holds — one key-group, which
//! a single hot key can make large), never by the savepoint. Every key of every subtask is
//! written: there is no cap, and `truncated` is always `false`.
//!
//! Values are decoded by [`crate::parser::value`] (through the reader), the single decoder: a
//! MAP state is one object of all its entries (`map key → value`, in on-disk order), any other
//! state the value of its first non-empty entry (a key with several windows or timers shows the
//! first one, as the TUI does).

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{json, Map, Value};

use crate::operators::{KeyedInfo, OperatorSummary};
use crate::reader::{Cursor, KeyRecord, StateEntry, StateReader};

/// Keys read per page while exporting.
const EXPORT_PAGE_KEYS: usize = 1000;

/// Entries of one key's state read per page, past those its record carries (the reader's entry
/// limit).
pub const EXPORT_ENTRY_PAGE: usize = 10_000;

/// What [`export_json`] wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExportSummary {
    pub operators: usize,
    pub keys: usize,
    /// State files and key-groups that could not be read (their keys are missing), naming them.
    pub errors: Vec<String>,
    /// Key-groups that could not be read to their end mark.
    pub malformed: Vec<String>,
    /// The most decoded entries of one key's state held at once: the reader's entry limit, or
    /// [`EXPORT_ENTRY_PAGE`] — never a whole map.
    pub max_entries_held: usize,
}

/// The value one key holds for one state, from its entries of that state (in on-disk order):
/// a MAP's entries gathered into one object (`map key → value`; an entry whose map key could
/// not be decoded is put under `#<its index>`, so that two such entries never collide),
/// otherwise the first entry whose bytes are not empty. `None` when the key holds nothing for the
/// state.
pub fn state_value(state_type: &str, entries: &[&StateEntry]) -> Option<Value> {
    if state_type == "MAP" {
        if entries.is_empty() {
            return None;
        }
        let mut obj = Map::new();
        for (i, e) in entries.iter().enumerate() {
            obj.insert(map_key_of(e, i), e.value.clone());
        }
        Some(Value::Object(obj))
    } else {
        entries
            .iter()
            .find(|e| !e.raw.is_empty())
            .map(|e| e.value.clone())
    }
}

/// The JSON key of MAP entry `e`, the `i`-th of its key's entries of that state.
pub fn map_key_of(e: &StateEntry, i: usize) -> String {
    e.map_key.clone().unwrap_or_else(|| format!("#{}", i))
}

/// How far an export is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExportProgress {
    /// Keys written so far.
    pub keys: usize,
    /// Operators written out of `operators`.
    pub operators_done: usize,
    pub operators: usize,
}

/// Write the export to `output_path` (through `<output_path>.partial`, renamed on success, so a
/// failed or cancelled export never leaves a half-written file under the real name). `progress`
/// is called after every page of keys and every operator; `cancel` stops it between pages (of
/// keys, and of one key's map entries).
pub fn export_json(
    reader: &StateReader,
    operators: &[OperatorSummary],
    output_path: &Path,
    cancel: &AtomicBool,
    progress: &mut dyn FnMut(ExportProgress),
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

fn cancelled(cancel: &AtomicBool) -> Result<(), String> {
    if cancel.load(Ordering::Relaxed) {
        Err("export cancelled".to_string())
    } else {
        Ok(())
    }
}

/// Write the value of state `state` of `record` into `w`: a MAP as one object streamed entry by
/// entry — those the record carries, then the rest a page at a time, read with
/// [`StateReader::entries`] (checking `cancel` between pages) — so a key with millions of map
/// entries holds one page of them, never the whole map; any other state as its first non-empty
/// entry, read past the record's entries only when none of those is. Returns the most entries
/// held at once.
#[allow(clippy::too_many_arguments)]
fn write_state_value<W: Write>(
    reader: &StateReader,
    op: &str,
    record: &KeyRecord,
    state: &str,
    state_type: &str,
    w: &mut W,
    cancel: &AtomicBool,
    malformed: &mut Vec<String>,
) -> Result<usize, String> {
    let carried: Vec<&StateEntry> = record
        .entries
        .iter()
        .filter(|e| &*e.state == state)
        .collect();
    let mut held = carried.len();
    let rest = record
        .truncated
        .iter()
        .find(|t| &*t.state == state)
        .map(|t| t.total)
        .filter(|&total| total > carried.len());
    if state_type != "MAP" {
        if let Some(v) = state_value(state_type, &carried) {
            write!(w, "{}", v).map_err(io)?;
            return Ok(held);
        }
        let mut offset = carried.len();
        while let Some(total) = rest.filter(|&t| offset < t) {
            cancelled(cancel)?;
            let page = reader
                .entries(op, &record.position, state, offset, EXPORT_ENTRY_PAGE)
                .map_err(String::from)?;
            push_unique(malformed, page.malformed.iter().cloned());
            held = held.max(page.entries.len());
            if let Some(e) = page.entries.iter().find(|e| !e.raw.is_empty()) {
                write!(w, "{}", e.value).map_err(io)?;
                return Ok(held);
            }
            match page.next {
                Some(n) if n > offset && n < total => offset = n,
                _ => break,
            }
        }
        write!(w, "null").map_err(io)?;
        return Ok(held);
    }

    if carried.is_empty() && rest.is_none() {
        write!(w, "null").map_err(io)?;
        return Ok(0);
    }
    let mut i = 0usize;
    let mut entry = |w: &mut W, e: &StateEntry| -> Result<(), String> {
        write!(
            w,
            "{}{}:{}",
            if i > 0 { "," } else { "" },
            Value::String(map_key_of(e, i)),
            e.value
        )
        .map_err(io)?;
        i += 1;
        Ok(())
    };
    write!(w, "{{").map_err(io)?;
    for e in &carried {
        entry(w, e)?;
    }
    let mut offset = carried.len();
    while let Some(total) = rest.filter(|&t| offset < t) {
        cancelled(cancel)?;
        let page = reader
            .entries(op, &record.position, state, offset, EXPORT_ENTRY_PAGE)
            .map_err(String::from)?;
        push_unique(malformed, page.malformed.iter().cloned());
        held = held.max(page.entries.len());
        for e in &page.entries {
            entry(w, e)?;
        }
        match page.next {
            Some(n) if n > offset && n < total => offset = n,
            _ => break,
        }
    }
    write!(w, "}}").map_err(io)?;
    Ok(held)
}

/// Stream the export document into `w`. Layout:
///
/// ```text
/// { "checkpoint_id": …, "truncated": false, "operators": [
///   { "name", "parallelism", "truncated": false, "operator_states": [{name, mode, values}],
///     "state_descriptors": [{name, state_type, value_type, value_class}],
///     "keys": [ {"key", "key_group", "states": {state: value}}, … ],
///     "total_keys", "load_errors", "malformed_key_groups", "info": {…} }, … ] }
/// ```
///
/// `truncated` is always `false` — every key is written — and kept for readers of older
/// exports. Keys come in the reader's order (key-groups ascending, on-disk order inside one),
/// not sorted; a key's states by name. `total_keys`, the errors and `info` come after `keys`
/// because they are only known once every key has been read.
pub fn write_json<W: Write>(
    reader: &StateReader,
    operators: &[OperatorSummary],
    w: &mut W,
    cancel: &AtomicBool,
    progress: &mut dyn FnMut(ExportProgress),
) -> Result<ExportSummary, String> {
    let mut summary = ExportSummary::default();
    let mut at = ExportProgress {
        keys: 0,
        operators_done: 0,
        operators: operators.len(),
    };
    write!(
        w,
        "{{\n  \"checkpoint_id\": {},\n  \"truncated\": false,\n  \"operators\": [",
        reader.savepoint().checkpoint_id
    )
    .map_err(io)?;
    for (n, op) in operators.iter().enumerate() {
        cancelled(cancel)?;
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
             \"truncated\": false,\n      \
             \"operator_states\": {},\n      \"state_descriptors\": {},\n      \"keys\": [",
            if n > 0 { "," } else { "" },
            Value::String(op.display_name.clone()),
            op.parallelism,
            Value::Array(operator_states),
            Value::Array(descriptors),
        )
        .map_err(io)?;

        // A key's states by name, as the export always listed them.
        let mut by_name: Vec<_> = keyed.states.iter().collect();
        by_name.sort_by(|a, b| a.name.cmp(&b.name));
        let mut total = 0usize;
        if !keyed.states.is_empty() {
            let mut cursor = Some(Cursor::START);
            while let Some(c) = cursor {
                cancelled(cancel)?;
                let page = reader
                    .key_page(&hex, &c, EXPORT_PAGE_KEYS)
                    .map_err(String::from)?;
                push_unique(&mut errors, page.unreadable.iter().map(|u| u.error.clone()));
                push_unique(&mut malformed, page.malformed.iter().cloned());
                for record in &page.keys {
                    write!(
                        w,
                        "{}\n        {{\"key\":{},\"key_group\":{},\"states\":{{",
                        if total > 0 { "," } else { "" },
                        Value::String(record.key.clone()),
                        record.key_group
                    )
                    .map_err(io)?;
                    for (i, d) in by_name.iter().enumerate() {
                        write!(
                            w,
                            "{}{}:",
                            if i > 0 { "," } else { "" },
                            Value::String(d.name.clone())
                        )
                        .map_err(io)?;
                        let held = write_state_value(
                            reader,
                            &hex,
                            record,
                            &d.name,
                            &d.state_type,
                            w,
                            cancel,
                            &mut malformed,
                        )?;
                        summary.max_entries_held = summary.max_entries_held.max(held);
                    }
                    write!(w, "}}}}").map_err(io)?;
                    total += 1;
                }
                summary.keys += page.keys.len();
                at.keys = summary.keys;
                progress(at);
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
        at.operators_done = summary.operators;
        progress(at);
    }
    writeln!(w, "\n  ]\n}}").map_err(io)?;
    Ok(summary)
}
