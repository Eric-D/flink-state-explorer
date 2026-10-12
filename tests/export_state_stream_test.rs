//! Streaming single-state export: parity with the full JSON export, the per-key-group
//! compression flag, and the bounded footprint.
//!
//! Needs `tests/fixtures/savepoint-gitlab` (run `cd test-flink-job && mvn verify` first).

use std::collections::HashMap;
use std::path::Path;

use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use flink_explorer::parser::keyed_state::{
    key_group_block, parse_key_group_block, read_keyed_stream_header, KeyGroupParse, KeyedEntry,
};
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

    let tmp = std::env::temp_dir().join(format!("export_state_parity_{}.json", std::process::id()));
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
    let states = list_states(&savepoint(), Path::new(SAVEPOINT_DIR));
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
    let states = list_states(&savepoint(), Path::new(SAVEPOINT_DIR));
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
fn compressed_key_group_decodes_like_an_uncompressed_one() {
    use std::io::Write;

    // A minimal key-group block: stateId 0, then one entry
    // [int keyLen][kgPrefix + StringValue("ab")][int valueLen][value].
    let mut block: Vec<u8> = Vec::new();
    block.extend_from_slice(&0u16.to_be_bytes()); // stateId
    let mut key = Vec::new();
    key.push(0u8); // 1-byte key-group prefix (maxParallelism <= 128)
    key.push(3u8); // StringValue var-int length = charCount + 1
    key.extend_from_slice(b"ab");
    key.push(0u8); // VoidNamespace
    block.extend_from_slice(&(key.len() as i32).to_be_bytes());
    block.extend_from_slice(&key);
    let value = 7i64.to_be_bytes();
    block.extend_from_slice(&(value.len() as i32).to_be_bytes());
    block.extend_from_slice(&value);
    block.extend_from_slice(&0xFFFFu16.to_be_bytes()); // END_OF_KEY_GROUP_MARK

    // Snappy *framed*, as SnappyStreamCompressionDecorator writes it.
    let mut framed = Vec::new();
    {
        let mut enc = snap::write::FrameEncoder::new(&mut framed);
        enc.write_all(&block).unwrap();
        enc.flush().unwrap();
    }
    assert_ne!(
        framed, block,
        "the framed stream should differ from the raw block"
    );

    let parse = |data: &[u8]| -> Vec<KeyedEntry> {
        let mut out = Vec::new();
        parse_key_group_block(
            data,
            3,
            &KeyGroupParse {
                num_states: 1,
                key_type: "String",
                kg_prefix_len: 1,
                key_pojo: None,
                state_types: &["VALUE".to_string()],
                only_state: Some(0),
            },
            &mut |e| out.push(e),
        );
        out
    };

    let mut buf = Vec::new();
    let inflated = key_group_block(&framed, true, &mut buf).expect("snappy frame decodes");
    assert_eq!(
        inflated,
        block.as_slice(),
        "decompressed bytes differ from the original block"
    );

    let from_plain = parse(&block);
    let from_compressed = parse(inflated);
    assert_eq!(from_plain.len(), 1, "one entry expected");
    assert_eq!(from_plain[0].key, "ab");
    assert_eq!(from_plain[0].value, value.to_vec());
    assert_eq!(from_compressed.len(), from_plain.len());
    assert_eq!(from_compressed[0].key, from_plain[0].key);
    assert_eq!(from_compressed[0].value, from_plain[0].value);

    // And the header flag that selects this path.
    let mut header = Vec::new();
    header.extend_from_slice(&6i32.to_be_bytes());
    header.push(1u8);
    let h = read_keyed_stream_header(&header).expect("header parses");
    assert_eq!(h.version, 6);
    assert!(h.using_key_group_compression);
}

#[test]
fn a_state_filter_skips_other_states_entries() {
    // Two states in one block; asking for stateId 1 must yield only its entry.
    let mut block: Vec<u8> = Vec::new();
    for (state_id, key_char, val) in [(0u16, b'a', 1i64), (1u16, b'b', 2i64)] {
        block.extend_from_slice(&state_id.to_be_bytes());
        let mut key = vec![0u8, 2u8, key_char, 0u8];
        key[1] = 2; // StringValue length = 1 char + 1
        block.extend_from_slice(&(key.len() as i32).to_be_bytes());
        block.extend_from_slice(&key);
        let vb = val.to_be_bytes();
        block.extend_from_slice(&(vb.len() as i32).to_be_bytes());
        block.extend_from_slice(&vb);
    }
    block.extend_from_slice(&0xFFFFu16.to_be_bytes());

    let types = vec!["VALUE".to_string(), "VALUE".to_string()];
    let mut only_one = Vec::new();
    let ctx = |only_state| KeyGroupParse {
        num_states: 2,
        key_type: "String",
        kg_prefix_len: 1,
        key_pojo: None,
        state_types: &types,
        only_state,
    };
    parse_key_group_block(&block, 0, &ctx(Some(1)), &mut |e| only_one.push(e));
    assert_eq!(
        only_one.len(),
        1,
        "only the requested state should be emitted"
    );
    assert_eq!(only_one[0].state_id, 1);
    assert_eq!(only_one[0].key, "b");
    assert_eq!(only_one[0].value, 2i64.to_be_bytes().to_vec());

    let mut all = Vec::new();
    parse_key_group_block(&block, 0, &ctx(None), &mut |e| all.push(e));
    assert_eq!(all.len(), 2, "without a filter both states are emitted");
}
