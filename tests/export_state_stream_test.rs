//! Streaming single-state export: parity with the full JSON export, the per-key-group
//! compression flag, and the bounded footprint.
//!
//! Needs `tests/fixtures/savepoint-gitlab` (run `cd test-flink-job && mvn verify` first).

use std::collections::HashMap;
use std::path::Path;

use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use flink_explorer::parser::keyed_state::read_keyed_stream_header;
use flink_explorer::parser::state_handle::{StateHandle, StreamStateHandle};
use serde_json::Value;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn savepoint() -> parser::Savepoint {
    let dir = Path::new(SAVEPOINT_DIR);
    let metadata_path = dir.join("_metadata");
    assert!(
        metadata_path.exists(),
        "run `cd test-flink-job && mvn verify` first"
    );
    parser::parse_metadata(&metadata_path).expect("parse metadata")
}

/// Stream one state of one operator and return its rows as parsed JSON objects.
fn stream(operator: &str, state: &str) -> Vec<Value> {
    let sp = savepoint();
    let mut buf: Vec<u8> = Vec::new();
    export_state_ndjson(
        &sp,
        Path::new(SAVEPOINT_DIR),
        Some(operator),
        state,
        None,
        &mut buf,
    )
    .unwrap_or_else(|e| panic!("export {}: {}", state, e));
    String::from_utf8(buf)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("row is valid JSON"))
        .collect()
}

/// The full export, as `{ (operator, key, state) -> value }`. Operator-scoped because timer
/// state names, and keys, repeat across operators.
fn full_export() -> HashMap<(String, String, String), Value> {
    let dir = Path::new(SAVEPOINT_DIR);
    let metadata_path = dir.join("_metadata");
    let sp = parser::parse_metadata(&metadata_path).expect("parse metadata");
    let len = std::fs::metadata(&metadata_path).unwrap().len();
    let index = build_index(&sp, len, 0u64, dir);

    // Unique per call: several tests here call this concurrently (cargo runs them in parallel
    // threads), so a pid-only path would have them overwriting each other's file.
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    let tmp = std::env::temp_dir().join(format!(
        "export_state_parity_{}_{}.json",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    flink_explorer::export::export_json(&index, None, &tmp).expect("export_json");
    let root: Value = serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();
    std::fs::remove_file(&tmp).ok();

    let mut out = HashMap::new();
    for op in root["operators"].as_array().unwrap() {
        let op_name = op["name"].as_str().unwrap_or_default().to_string();
        for k in op["keys"].as_array().unwrap() {
            let key = k["key"].as_str().unwrap_or_default().to_string();
            for (state, value) in k["states"].as_object().unwrap() {
                out.insert((op_name.clone(), key.clone(), state.clone()), value.clone());
            }
        }
    }
    out
}

#[test]
fn keyed_stream_header_is_v6_and_uncompressed() {
    let sp = savepoint();
    let mut checked = 0;
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
            let header = read_keyed_stream_header(&bs.data)
                .expect("keyed stream starts with a KeyedBackendSerializationProxy header");
            // Flink 1.20 writes KeyedBackendSerializationProxy.VERSION = 6.
            assert_eq!(header.version, 6, "operator {:?}", op.name);
            // The fixtures are generated without snapshot compression.
            assert!(
                !header.using_key_group_compression,
                "fixture unexpectedly uses key-group compression"
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no inline keyed stream found in the fixture");
}

#[test]
fn streaming_matches_full_export() {
    let full = full_export();
    let states = list_states(&savepoint(), Path::new(SAVEPOINT_DIR)).states;
    assert!(!states.is_empty());

    let mut compared = 0;
    for (op, name, state_type) in &states {
        // MAP states export exploded (one row per entry), compared separately below.
        if state_type == "MAP" {
            continue;
        }
        let rows = stream(op, name);
        for row in &rows {
            let key = row["key"].as_str().unwrap_or_default().to_string();
            let Some(expected) = full.get(&(op.clone(), key.clone(), name.clone())) else {
                panic!(
                    "streamed key {:?} of {:?}/{:?} absent from the full export",
                    key, op, name
                );
            };
            assert_eq!(
                &row["value"], expected,
                "value mismatch for state {:?} key {:?}",
                name, key
            );
            compared += 1;
        }
    }
    assert!(compared > 0, "no row compared");
}

#[test]
fn map_state_explodes_into_one_row_per_entry() {
    let full = full_export();
    let states = list_states(&savepoint(), Path::new(SAVEPOINT_DIR)).states;
    let mut compared = 0;

    for (op, name, state_type) in &states {
        if state_type != "MAP" {
            continue;
        }
        for row in stream(op, name) {
            let key = row["key"].as_str().unwrap_or_default().to_string();
            let map_key = row["map_key"]
                .as_str()
                .expect("a MAP row carries its decoded map_key")
                .to_string();
            let aggregated = full
                .get(&(op.clone(), key.clone(), name.clone()))
                .unwrap_or_else(|| panic!("key {:?} of {:?} absent from full export", key, name));
            let expected = aggregated.get(&map_key).unwrap_or_else(|| {
                panic!("map key {:?} absent from aggregated {:?}", map_key, name)
            });
            assert_eq!(
                &row["value"], expected,
                "map entry mismatch: state {:?} key {:?} map_key {:?}",
                name, key, map_key
            );
            compared += 1;
        }
    }
    assert!(compared > 0, "no MAP entry compared");
}

#[test]
fn peak_block_is_one_key_group_not_the_whole_stream() {
    let sp = savepoint();
    let mut buf: Vec<u8> = Vec::new();
    let stats = export_state_ndjson(
        &sp,
        Path::new(SAVEPOINT_DIR),
        None,
        "all-fields-pojo",
        None,
        &mut buf,
    )
    .expect("export");

    let metadata_len = std::fs::metadata(Path::new(SAVEPOINT_DIR).join("_metadata"))
        .unwrap()
        .len() as usize;
    assert!(stats.rows > 0);
    assert!(stats.key_groups >= stats.subtasks);
    // The whole point of the streaming path: the peak buffer is one key-group, well under the
    // size of the stream it was read from.
    assert!(
        stats.max_block_bytes < metadata_len,
        "peak block {} is not smaller than the metadata ({})",
        stats.max_block_bytes,
        metadata_len
    );
}

#[test]
fn every_key_group_flink_wrote_reads_to_its_end_mark() {
    // Runs end on Flink's metadata-follows flag; on blocks Flink wrote, every run is flagged and
    // every key-group ends with END_OF_KEY_GROUP_MARK, so nothing may be reported as malformed —
    // plain or Snappy-compressed, on the export path or the index path.
    for dir in [SAVEPOINT_DIR, "tests/fixtures/savepoint-compressed"] {
        let dir = Path::new(dir);
        let sp = parser::parse_metadata(&dir.join("_metadata")).expect("parse metadata");
        let states = list_states(&sp, dir).states;
        assert!(!states.is_empty(), "{}", dir.display());
        for (op, name, _) in &states {
            let stats = export_state_ndjson(&sp, dir, Some(op), name, None, &mut std::io::sink())
                .unwrap_or_else(|e| panic!("export {}: {}", name, e));
            assert_eq!(
                (stats.malformed_key_groups, stats.first_malformed),
                (0, None),
                "{} {}/{}",
                dir.display(),
                op,
                name
            );
        }
        let index = build_index(&sp, 0, 0, dir);
        for op in &index.operators {
            if let Some(ks) = &op.keyed_state {
                assert_eq!(
                    (ks.malformed_key_groups, &ks.first_malformed),
                    (0, &None),
                    "{} {}",
                    dir.display(),
                    op.operator_id_hex()
                );
            }
        }
    }
}
