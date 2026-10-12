//! A savepoint written with **snapshot compression**: each key-group is an independent
//! Snappy-framed stream and the keyed header's `usingKeyGroupCompression` is set.
//!
//! This is the fixture the compressed read path needs: everything else only exercises a Snappy
//! frame built in a unit test, never bytes Flink wrote. Both reading paths are checked — the
//! streaming export and the index/TUI one.
//!
//! Needs `tests/fixtures/savepoint-compressed` (run `cd test-flink-job && mvn verify` first).

use std::collections::BTreeSet;
use std::path::Path;

use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use flink_explorer::parser::keyed_state::read_keyed_stream_header;
use flink_explorer::parser::state_handle::{StateHandle, StreamStateHandle};
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

/// Keys per state, from the index/TUI path.
fn index_keys(dir: &str) -> std::collections::BTreeMap<String, BTreeSet<String>> {
    let path = Path::new(dir);
    let metadata_path = path.join("_metadata");
    let sp = savepoint(dir);
    let len = std::fs::metadata(&metadata_path).unwrap().len();
    let index = build_index(&sp, len, 0u64, path);

    let tmp = std::env::temp_dir().join(format!(
        "compressed_keys_{}_{}.json",
        std::process::id(),
        dir.replace('/', "_")
    ));
    flink_explorer::export::export_json(&index, None, &tmp).expect("export_json");
    let root: Value = serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();
    std::fs::remove_file(&tmp).ok();

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
fn index_path_decodes_compressed_key_groups() {
    let compressed = index_keys(COMPRESSED);
    assert!(
        !compressed.is_empty(),
        "no state decoded from the compressed savepoint — key-groups were not inflated"
    );

    // Same job, same deterministic sources, so each state holds the same key set. Values can
    // differ (counters depend on how long the job ran before the savepoint), keys cannot.
    let plain = index_keys(PLAIN);
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
fn streaming_export_matches_the_index_path_on_compressed_bytes() {
    let sp = savepoint(COMPRESSED);
    let states = list_states(&sp, Path::new(COMPRESSED));
    assert!(!states.is_empty());
    let index = index_keys(COMPRESSED);

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
        let from_index = index
            .get(state)
            .unwrap_or_else(|| panic!("state {:?} absent from the index export", state));
        // A MAP state streams one row per entry, so its key set is a subset of (and for this
        // fixture equal to) the index's keys; either way every streamed key must be known.
        for k in &streamed {
            assert!(
                from_index.contains(k),
                "streamed key {:?} of {:?} ({}) is unknown to the index path",
                k,
                state,
                state_type
            );
        }
        compared += streamed.len();
    }
    assert!(compared > 0, "no key compared");
}
