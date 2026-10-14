//! `reader::StateReader` on savepoints written by a real Flink 1.20 cluster.
//!
//! Needs `tests/fixtures/savepoint-gitlab` and `savepoint-compressed` (run
//! `cd test-flink-job && mvn verify` first), so it is not in the CI list.

use std::collections::BTreeSet;
use std::path::Path;

use flink_explorer::export_state::{export_state_rows, list_states, RowSink, StateRow};
use flink_explorer::parser;
use flink_explorer::reader::{Cursor, KeyRecord, Lookup, StateReader};

/// `(state, key, map_key, value, key_group, subtask, namespace_hex)`.
type Row = (
    String,
    String,
    Option<String>,
    String,
    u16,
    i32,
    Option<String>,
);

/// An undecodable map key carries a `#n` tie-breaker whose `n` differs between the two paths
/// (row number in the export, entry number in the reader): compare without it.
fn map_key(k: Option<&str>) -> Option<String> {
    k.map(|k| match k.rfind(" #") {
        Some(i) if k.starts_with('[') => k[..i].to_string(),
        _ => k.to_string(),
    })
}

struct Collect(Vec<Row>);

impl RowSink for Collect {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        self.0.push((
            row.state.to_string(),
            row.key.to_string(),
            map_key(row.map_key),
            row.value.to_string(),
            row.key_group,
            row.subtask,
            row.namespace_hex.map(str::to_string),
        ));
        Ok(())
    }
    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }
}

fn rows_of(keys: &[KeyRecord]) -> Vec<Row> {
    let mut rows = Vec::new();
    for k in keys {
        for e in &k.entries {
            let ns = (e.map_key.is_none() && e.namespace.len() > 1)
                .then(|| e.namespace.iter().map(|b| format!("{:02x}", b)).collect());
            rows.push((
                e.state.clone(),
                k.key.clone(),
                map_key(e.map_key.as_deref()),
                e.value.to_string(),
                k.key_group,
                k.subtask,
                ns,
            ));
        }
    }
    rows.sort();
    rows
}

fn page_all(r: &StateReader, op: &str) -> Vec<KeyRecord> {
    let mut cursor = Cursor::START;
    let mut keys = Vec::new();
    loop {
        let page = r.key_page(op, &cursor, 13).unwrap();
        assert!(page.malformed.is_empty(), "{:?}", page.malformed);
        keys.extend(page.keys);
        match page.next {
            Some(n) => cursor = n,
            None => return keys,
        }
    }
}

fn check(dir: &str) {
    let dir = Path::new(dir);
    assert!(
        dir.join("_metadata").exists(),
        "run `cd test-flink-job && mvn verify` first"
    );
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let listing = list_states(&sp, dir);
    assert!(listing.errors.is_empty(), "{:?}", listing.errors);
    let r = StateReader::new(&sp, dir);
    let (mut found, mut scanned) = (0, 0);
    for op in r.operators().iter().filter(|o| o.keyed_subtasks > 0) {
        let keys = page_all(&r, &op.id);
        let distinct: BTreeSet<&str> = keys.iter().map(|k| k.key.as_str()).collect();
        assert_eq!(distinct.len(), keys.len(), "op {}: a key came twice", op.id);

        // The same rows as the streaming export of every state.
        let mut sink = Collect(Vec::new());
        for (o, state, _) in &listing.states {
            if *o == op.id {
                export_state_rows(&sp, dir, Some(o), state, None, &mut sink).unwrap();
            }
        }
        sink.0.sort();
        assert_eq!(rows_of(&keys), sink.0, "op {}", op.id);

        // Every key is found by `get`, reading its one block, or by a scan for key types
        // without a known hashCode().
        for k in &keys {
            match r.get(&op.id, &k.key).unwrap() {
                Lookup::Found { record, stats } => {
                    assert_eq!(&record, k);
                    assert_eq!(stats.blocks, 1);
                    found += 1;
                }
                Lookup::NeedsScan { .. } => {
                    let hit = r.scan(&op.id, &k.position, 0, &mut |t| t == k.key).unwrap();
                    assert_eq!(hit.matches.first(), Some(k));
                    scanned += 1;
                }
                Lookup::Absent { .. } => panic!("op {}: key {:?} absent", op.id, k.key),
            }
        }
    }
    assert!(found > 0);
    eprintln!(
        "{}: {} keys found by get, {} by scan",
        dir.display(),
        found,
        scanned
    );
}

#[test]
fn every_key_of_the_gitlab_savepoint_is_paged_and_found() {
    check("tests/fixtures/savepoint-gitlab");
}

#[test]
fn every_key_of_the_compressed_savepoint_is_paged_and_found() {
    check("tests/fixtures/savepoint-compressed");
}
