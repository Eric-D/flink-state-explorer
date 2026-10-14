//! An operator's keyed state read whole through the `StateReader` — what the in-memory index
//! used to hold, for tests that look keys and values up. Every key of every subtask, in the
//! reader's order (key-groups ascending, on-disk order inside one), with every entry of every
//! state (no entry limit).

use std::path::Path;

use flink_explorer::export::state_value;
use flink_explorer::operators::{summarize, OperatorSummary};
use flink_explorer::reader::{Cursor, KeyRecord, StateEntry, StateInfo, StateReader};
use serde_json::Value;

/// One operator's keyed state.
#[derive(Debug, Clone)]
pub struct Keyed {
    pub op_hex: String,
    pub key_type: String,
    pub states: Vec<StateInfo>,
    pub keys: Vec<KeyRecord>,
    /// Unreadable files / key-groups (from the headers and the pages), naming them.
    pub errors: Vec<String>,
    /// Malformed key-groups.
    pub malformed: Vec<String>,
}

impl Keyed {
    pub fn state(&self, name: &str) -> Option<&StateInfo> {
        self.states.iter().find(|s| s.name == name)
    }

    pub fn state_names(&self) -> Vec<&str> {
        self.states.iter().map(|s| s.name.as_str()).collect()
    }

    pub fn record(&self, key: &str) -> Option<&KeyRecord> {
        self.keys.iter().find(|k| k.key == key)
    }

    /// The entries of `state` of `key`, in on-disk order.
    pub fn entries(&self, key: &str, state: &str) -> Vec<&StateEntry> {
        self.record(key)
            .map(|r| r.entries.iter().filter(|e| &*e.state == state).collect())
            .unwrap_or_default()
    }

    /// The value `key` holds for `state`, as the TUI and the JSON export show it: a MAP's entries
    /// as one object, otherwise the first non-empty entry's value.
    pub fn value(&self, key: &str, state: &str) -> Option<Value> {
        let st = self.state(state)?;
        state_value(&st.state_type, &self.entries(key, state))
    }

    /// The raw bytes of the first non-empty entry of `state` of `key`.
    pub fn raw(&self, key: &str, state: &str) -> Option<Vec<u8>> {
        self.entries(key, state)
            .into_iter()
            .find(|e| !e.raw.is_empty())
            .map(|e| e.raw.clone())
    }

    /// How many entries of `state` there are, over every key.
    pub fn entry_count(&self, state: &str) -> usize {
        self.keys
            .iter()
            .map(|r| r.entries.iter().filter(|e| &*e.state == state).count())
            .sum()
    }

    /// The keys, in the reader's order.
    pub fn key_names(&self) -> Vec<&str> {
        self.keys.iter().map(|k| k.key.as_str()).collect()
    }
}

/// A reader with no entry limit, so that every record is complete.
pub fn reader(dir: &Path) -> StateReader<'static> {
    StateReader::open(dir)
        .expect("open savepoint")
        .with_entry_limit(usize::MAX)
}

/// Read operator `op_hex`'s keyed state whole; `None` when it has no keyed state (no state in
/// any header, and no unreadable file either).
pub fn keyed_with(reader: &StateReader, op_hex: &str) -> Option<Keyed> {
    let states = reader.states(op_hex).expect("states");
    if states.states.is_empty() && states.errors.is_empty() {
        return None;
    }
    let mut out = Keyed {
        op_hex: op_hex.to_string(),
        key_type: states.key_type,
        states: states.states,
        keys: Vec::new(),
        errors: states.errors,
        malformed: Vec::new(),
    };
    let mut cursor = Some(Cursor::START);
    while let Some(c) = cursor {
        let page = reader.key_page(op_hex, &c, 10_000).expect("key_page");
        for u in page.unreadable {
            if !out.errors.contains(&u.error) {
                out.errors.push(u.error);
            }
        }
        for m in page.malformed {
            if !out.malformed.contains(&m) {
                out.malformed.push(m);
            }
        }
        out.keys.extend(page.keys);
        cursor = page.next;
    }
    Some(out)
}

/// [`keyed_with`] on a reader of its own.
pub fn keyed(dir: &Path, op_hex: &str) -> Option<Keyed> {
    keyed_with(&reader(dir), op_hex)
}

/// Every operator of the savepoint in `dir`: its summary (operator states, info) and its keyed
/// state read whole.
pub fn operators(dir: &Path) -> Vec<(OperatorSummary, Option<Keyed>)> {
    let r = reader(dir);
    summarize(r.savepoint(), dir)
        .into_iter()
        .map(|s| {
            let k = keyed_with(&r, &s.operator_id_hex());
            (s, k)
        })
        .collect()
}

/// The first operator whose keyed state has a state named `state`.
pub fn find_with_state(
    ops: &[(OperatorSummary, Option<Keyed>)],
    state: &str,
) -> Option<(OperatorSummary, Keyed)> {
    ops.iter().find_map(|(s, k)| {
        k.as_ref()
            .filter(|k| k.state(state).is_some())
            .map(|k| (s.clone(), k.clone()))
    })
}
