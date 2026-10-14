//! A savepoint written with **snapshot compression**: each key-group is an independent
//! Snappy-framed stream and the keyed header's `usingKeyGroupCompression` is set.
//!
//! This is the fixture the compressed read path needs: everything else only exercises a Snappy
//! frame built in a unit test, never bytes Flink wrote. Both reading paths are checked — the
//! streaming export and the reader/TUI one — and the operator-state stream, which the operator
//! backend compresses as a whole (`usingStateCompression`).
//!
//! Needs `tests/fixtures/savepoint-compressed` (run `cd test-flink-job && mvn verify` first).

use std::collections::BTreeSet;
use std::path::Path;

use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::parser;
use flink_explorer::parser::keyed_state::read_keyed_stream_header;
use flink_explorer::parser::state_handle::{StateHandle, StreamStateHandle};
use flink_explorer::reader::StateReader;
use serde_json::Value;

const COMPRESSED: &str = "tests/fixtures/savepoint-compressed";
const PLAIN: &str = "tests/fixtures/savepoint-gitlab";

fn savepoint(dir: &str) -> parser::Savepoint {
    let metadata_path = Path::new(dir).join("_metadata");
    assert!(
        metadata_path.exists(),
        "{} missing — run `cd test-flink-job && mvn verify` first",
        metadata_path.display()
    );
    parser::parse_metadata(&metadata_path).expect("parse metadata")
}

/// Whether every inline keyed stream of this savepoint declares key-group compression.
fn compression_flags(dir: &str) -> Vec<bool> {
    let sp = savepoint(dir);
    let mut flags = Vec::new();
    for op in &sp.operators {
        for st in &op.subtask_states {
            let StateHandle::KeyGroups(kg) = &st.managed_keyed_state else {
                continue;
            };
            let delegate = match &kg.delegate_handle {
                StreamStateHandle::KeyGroups(nested) => &nested.delegate_handle,
                other => other,
            };
            let StreamStateHandle::ByteStream(bs) = delegate else {
                continue;
            };
            let h = read_keyed_stream_header(&bs.data).expect("keyed header parses");
            assert_eq!(h.version, 6);
            flags.push(h.using_key_group_compression);
        }
    }
    flags
}

/// The TUI's `E` export of `dir` (streamed from the reader), written to a temp file — never into
/// the fixture.
fn export_json(dir: &str) -> Value {
    let path = Path::new(dir);
    savepoint(dir); // asserts the fixture exists
    let reader = StateReader::open(path).unwrap();
    let summaries = flink_explorer::operators::summarize(reader.savepoint(), path);
    // Unique per call: these tests run in parallel threads of one process and more than one of
    // them exports the same fixture, so a path derived only from the pid and the directory would
    // have them clobbering each other's file.
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    let tmp = std::env::temp_dir().join(format!(
        "compressed_export_{}_{}_{}.json",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed),
        dir.replace('/', "_")
    ));
    flink_explorer::export::export_json(
        &reader,
        &summaries,
        &tmp,
        &std::sync::atomic::AtomicBool::new(false),
        &mut |_| {},
    )
    .expect("export_json");
    let root: Value = serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();
    std::fs::remove_file(&tmp).ok();
    root
}

/// Keys per state, from the reader/TUI path.
fn reader_keys(dir: &str) -> std::collections::BTreeMap<String, BTreeSet<String>> {
    let root = export_json(dir);

    let mut out: std::collections::BTreeMap<String, BTreeSet<String>> = Default::default();
    for op in root["operators"].as_array().unwrap() {
        for k in op["keys"].as_array().unwrap() {
            let key = k["key"].as_str().unwrap_or_default().to_string();
            for (state, value) in k["states"].as_object().unwrap() {
                if value.is_null() {
                    continue;
                }
                out.entry(state.clone()).or_default().insert(key.clone());
            }
        }
    }
    out
}

#[test]
fn fixture_really_uses_key_group_compression() {
    let compressed = compression_flags(COMPRESSED);
    assert!(
        !compressed.is_empty(),
        "no inline keyed stream in the fixture"
    );
    assert!(
        compressed.iter().all(|&f| f),
        "every keyed stream of the compressed fixture should set usingKeyGroupCompression, got {:?}",
        compressed
    );

    // And the contrast: the regular fixture is uncompressed, so the flag is what distinguishes
    // them rather than something incidental to this build.
    assert!(
        compression_flags(PLAIN).iter().all(|&f| !f),
        "the plain fixture should not be compressed"
    );
}

#[test]
fn reader_path_decodes_compressed_key_groups() {
    let compressed = reader_keys(COMPRESSED);
    assert!(
        !compressed.is_empty(),
        "no state decoded from the compressed savepoint — key-groups were not inflated"
    );

    // Same job, same deterministic sources, so each state holds the same key set. Values can
    // differ (counters depend on how long the job ran before the savepoint), keys cannot.
    let plain = reader_keys(PLAIN);
    for (state, keys) in &plain {
        // Timers fire on wall-clock and may or may not be pending in either run.
        if state.starts_with("_timer_state/") {
            continue;
        }
        let got = compressed.get(state).unwrap_or_else(|| {
            panic!(
                "state {:?} decoded from the plain fixture but not the compressed one",
                state
            )
        });
        assert_eq!(
            got, keys,
            "state {:?} has a different key set once compressed",
            state
        );
    }
}

#[test]
fn streaming_export_matches_the_reader_path_on_compressed_bytes() {
    let sp = savepoint(COMPRESSED);
    let states = list_states(&sp, Path::new(COMPRESSED)).states;
    assert!(!states.is_empty());
    let from_reader = reader_keys(COMPRESSED);

    let mut compared = 0;
    for (op, state, state_type) in &states {
        if state.starts_with("_timer_state/") {
            continue;
        }
        let mut buf: Vec<u8> = Vec::new();
        export_state_ndjson(&sp, Path::new(COMPRESSED), Some(op), state, None, &mut buf)
            .unwrap_or_else(|e| panic!("export {}: {}", state, e));

        let streamed: BTreeSet<String> = String::from_utf8(buf)
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let row: Value = serde_json::from_str(l).unwrap();
                row["key"].as_str().unwrap_or_default().to_string()
            })
            .collect();

        if streamed.is_empty() {
            continue;
        }
        let from_reader = from_reader
            .get(state)
            .unwrap_or_else(|| panic!("state {:?} absent from the reader export", state));
        // A MAP state streams one row per entry, so its key set is a subset of (and for this
        // fixture equal to) the reader's keys; either way every streamed key must be known.
        for k in &streamed {
            assert!(
                from_reader.contains(k),
                "streamed key {:?} of {:?} ({}) is unknown to the reader path",
                k,
                state,
                state_type
            );
        }
        compared += streamed.len();
    }
    assert!(compared > 0, "no key compared");
}

/// Every operator state's `(name, mode)` → its element labels (all subtasks, sorted), from the
/// operator summaries and from the full JSON export.
fn operator_states(dir: &str) -> [std::collections::BTreeMap<(String, String), Vec<String>>; 2] {
    let path = Path::new(dir);
    let sp = savepoint(dir);
    let ops = flink_explorer::operators::summarize(&sp, path);
    let mut from_summary: std::collections::BTreeMap<(String, String), Vec<String>> =
        Default::default();
    for s in ops.iter().flat_map(|op| &op.states) {
        from_summary
            .entry((s.name.clone(), s.state_type.clone()))
            .or_default()
            .extend(s.elements.iter().map(|e| e.label.clone()));
    }

    let root = export_json(dir);
    let mut from_json: std::collections::BTreeMap<(String, String), Vec<String>> =
        Default::default();
    for op in root["operators"].as_array().unwrap() {
        for s in op["operator_states"].as_array().unwrap() {
            let key = (
                s["name"].as_str().unwrap().to_string(),
                s["mode"].as_str().unwrap().to_string(),
            );
            from_json.entry(key).or_default().extend(
                s["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string()),
            );
        }
    }
    for m in [&mut from_summary, &mut from_json] {
        m.values_mut().for_each(|v| v.sort());
    }
    [from_summary, from_json]
}

#[test]
fn operator_states_inflate_like_the_uncompressed_run() {
    // The operator backend compresses its stream too (`usingStateCompression`): without
    // inflating it every element showed as a hex preview of its Snappy chunk.
    let [compressed, compressed_json] = operator_states(COMPRESSED);
    let [plain, _] = operator_states(PLAIN);
    assert_eq!(
        compressed_json, compressed,
        "JSON export = operator summaries"
    );
    assert_eq!(compressed, plain, "same job, same operator-state elements");
    let buffer = &compressed[&(
        "operator-buffer".to_string(),
        "operator/SPLIT_DISTRIBUTE".to_string(),
    )];
    assert!(buffer.contains(&"\"user-bob\"".to_string()), "{:?}", buffer);
    let broadcast = &compressed[&(
        "broadcast-config".to_string(),
        "operator/BROADCAST".to_string(),
    )];
    assert!(
        broadcast
            .iter()
            .all(|m| m.contains("\"config-a\": \"CONFIG-A\"")),
        "{:?}",
        broadcast
    );
    assert!(
        compressed
            .values()
            .flatten()
            .all(|l| !l.contains(" bytes] ")),
        "no hex preview left: {:?}",
        compressed
    );
}
