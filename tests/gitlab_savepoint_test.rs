mod common;

use std::path::Path;

use common::keyed::{find_with_state, operators, Keyed};
use flink_explorer::operators::OperatorSummary;
use flink_explorer::parser;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

/// Every operator of the fixture, its keyed state read whole through the `StateReader`.
fn load() -> Vec<(OperatorSummary, Option<Keyed>)> {
    let dir = Path::new(SAVEPOINT_DIR);
    assert!(
        dir.join("_metadata").exists(),
        "Savepoint _metadata not found"
    );
    operators(dir)
}

/// The keyed state of the operator holding state `state_name`.
fn find_operator_with_state(
    ops: &[(OperatorSummary, Option<Keyed>)],
    state_name: &str,
) -> Option<Keyed> {
    find_with_state(ops, state_name).map(|(_, k)| k)
}

#[test]
fn savepoint_loads_successfully() {
    let dir = Path::new(SAVEPOINT_DIR);
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let ops = load();
    println!("Checkpoint ID: {}", savepoint.checkpoint_id);
    println!("Version: {}", savepoint.version);
    println!("Operators: {}", ops.len());
    assert!(!ops.is_empty());
}

#[test]
fn all_types_operator_found() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "counter")
        .expect("All Types operator (with 'counter' state) not found");

    println!(
        "All Types operator: {} keys, {} states, key_type={}",
        ks.keys.len(),
        ks.states.len(),
        ks.key_type
    );
    assert_eq!(ks.key_type, "String");
    assert!(!ks.keys.is_empty());

    let names = ks.state_names();
    println!("States: {:?}", names);

    // VALUE states
    for s in &[
        "counter",
        "label",
        "active",
        "last-seen",
        "simple-event",
        "complex-record",
    ] {
        assert!(names.contains(s), "Missing VALUE state: {}", s);
    }
    // MAP states
    for s in &["events-by-type", "timestamp-index", "flags-by-category"] {
        assert!(names.contains(s), "Missing MAP state: {}", s);
    }
    // LIST states
    for s in &["log-entries", "event-history", "large-list"] {
        assert!(names.contains(s), "Missing LIST state: {}", s);
    }
}

#[test]
fn counter_value_is_long() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "counter").unwrap();
    let key = &ks.keys[0].key;
    let raw = ks
        .raw(key, "counter")
        .unwrap_or_else(|| panic!("counter has no value for key: {}", key));
    assert_eq!(
        raw.len(),
        8,
        "counter should be 8 bytes (Long), got {}",
        raw.len()
    );
    let val = i64::from_be_bytes(raw[..8].try_into().unwrap());
    println!("counter = {} for key '{}'", val, key);
    assert!(val > 0);
    // The reader decodes it the same way.
    assert_eq!(ks.value(key, "counter"), Some(serde_json::json!(val)));
}

#[test]
fn label_value_is_string() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "label").unwrap();
    let key = &ks.keys[0].key;
    let raw = ks.raw(key, "label").expect("label has no value");
    let mut r = flink_explorer::parser::java_deser::JavaReader::new(raw.as_slice());
    let val = r.read_string_value().expect("Failed to read StringValue");
    println!("label = '{}' for key '{}'", val, key);
    assert!(
        val.starts_with("label-for-"),
        "Expected 'label-for-...', got '{}'",
        val
    );
    assert_eq!(ks.value(key, "label"), Some(serde_json::json!(val)));
}

#[test]
fn long_key_operator() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "element-count")
        .expect("Long Key operator (with 'element-count') not found");
    println!("Long Key: {} keys, key_type={}", ks.keys.len(), ks.key_type);
    assert_eq!(ks.key_type, "Long");
    for k in &ks.keys {
        assert!(
            k.key.parse::<i64>().is_ok(),
            "Long key should be numeric: {}",
            k.key
        );
    }
}

#[test]
fn pojo_key_operator() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "latest-event")
        .expect("POJO Key operator (with 'latest-event') not found");
    println!("POJO Key: {} keys, key_type={}", ks.keys.len(), ks.key_type);
    assert!(ks.key_type == "Pojo" || ks.key_type.contains("Pojo"));
    let key = &ks.keys[0].key;
    println!("Key: {}", key);
    assert!(key.contains('{'), "POJO key should be JSON-like: {}", key);
}

#[test]
fn map_state_has_entries() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "events-by-type").unwrap();
    let desc = ks.state("events-by-type").unwrap();
    let entries = ks.entry_count("events-by-type");
    println!(
        "events-by-type: state_type={} value_type={} entries={}",
        desc.state_type, desc.value_type, entries
    );
    assert_eq!(desc.state_type, "MAP");
    assert!(entries > 0, "MAP should have entries");

    // One entry per map pair, each with its decoded map key; the value is the object of them.
    let mut map_count = 0;
    for k in &ks.keys {
        let es = ks.entries(&k.key, "events-by-type");
        if es.is_empty() {
            continue;
        }
        assert!(es.iter().all(|e| e.map_key.is_some()));
        let obj = ks.value(&k.key, "events-by-type").unwrap();
        assert_eq!(obj.as_object().unwrap().len(), es.len(), "key {}", k.key);
        println!("  key={}: {} map pairs", k.key, es.len());
        map_count += es.len();
    }
    println!("Total MAP pairs across all keys: {}", map_count);
    assert!(map_count > 0, "Should have MAP pairs");
}

#[test]
fn list_state_has_entries() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "large-list").unwrap();
    let desc = ks.state("large-list").unwrap();
    println!(
        "large-list: state_type={} entries={}",
        desc.state_type,
        ks.entry_count("large-list")
    );
    assert_eq!(desc.state_type, "LIST");
}

#[test]
fn coprocess_operator() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "left-value").expect("CoProcess operator not found");
    let names = ks.state_names();
    println!("CoProcess states: {:?}", names);
    assert!(names.contains(&"left-value"));
    assert!(names.contains(&"right-value"));
    assert!(names.contains(&"join-timestamp"));
    assert!(names.contains(&"seen-keys"));
}

#[test]
fn tuple_reduce_operator() {
    let ops = load();
    let ks =
        find_operator_with_state(&ops, "running-sum").expect("Tuple Reduce operator not found");
    let names = ks.state_names();
    println!("Tuple Reduce states: {:?}", names);
    assert!(names.contains(&"running-sum"));
    assert!(names.contains(&"tuple2-state"));
    assert!(names.contains(&"tuple3-state"));

    let reducing = ks.state("running-sum").unwrap();
    println!("running-sum state_type={}", reducing.state_type);
    assert_eq!(reducing.state_type, "REDUCING");
}

#[test]
fn extra_types_operator() {
    let ops = load();
    let ks = find_operator_with_state(&ops, "byte-val").expect("Extra Types operator not found");
    let names = ks.state_names();
    println!("Extra Types states: {:?}", names);

    for s in &[
        "byte-val",
        "short-val",
        "float-val",
        "double-val",
        "int-array",
        "long-array",
        "byte-array",
        "string-array",
        "simple-string-map",
        "nullable-never-set",
    ] {
        assert!(names.contains(s), "Missing state: {}", s);
    }

    // nullable-never-set should have no entries
    assert_eq!(
        ks.entry_count("nullable-never-set"),
        0,
        "nullable-never-set should have 0 entries"
    );

    // byte-val should be 1 byte
    let key = &ks.keys[0].key;
    if let Some(raw) = ks.raw(key, "byte-val") {
        assert_eq!(raw.len(), 1, "byte-val should be 1 byte");
        println!("byte-val = {}", raw[0] as i8);
    }
}

#[test]
fn summary_report() {
    let dir = Path::new(SAVEPOINT_DIR);
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let ops = load();
    println!("\n=== SAVEPOINT SUMMARY ===");
    println!("Version: {}", savepoint.version);
    println!("Checkpoint ID: {}", savepoint.checkpoint_id);
    println!("Operators: {}", ops.len());

    let keyed: Vec<&Keyed> = ops.iter().filter_map(|(_, k)| k.as_ref()).collect();
    let total_keys: usize = keyed.iter().map(|k| k.keys.len()).sum();
    let total_states: usize = keyed.iter().map(|k| k.states.len()).sum();

    println!("Keyed operators: {}", keyed.len());
    println!("Total keys: {}", total_keys);
    println!("Total states: {}", total_states);

    let key_types: Vec<&str> = keyed
        .iter()
        .filter(|k| !k.keys.is_empty())
        .map(|k| k.key_type.as_str())
        .collect();
    println!("Key types: {:?}", key_types);

    let mut types = std::collections::HashMap::new();
    for ks in &keyed {
        for d in &ks.states {
            *types.entry(d.state_type.as_str()).or_insert(0) += 1;
        }
    }
    println!("State types: {:?}", types);
    println!("=== DONE ===");
}

#[test]
fn entry_layouts_match_what_flink_wrote() {
    // Every state's namespace width and map key type, read off headers Flink 1.20 wrote: an
    // undecodable key is split from its namespace with them, and a decoded one checked.
    use flink_explorer::parser::keyed_state::{extract_entry_layouts, extract_state_metadata};
    use flink_explorer::parser::state_handle::{StateHandle, StreamStateHandle};
    let dir = Path::new(SAVEPOINT_DIR);
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let ops = load();
    let mut checked = 0;
    for (op, (_, keyed)) in savepoint.operators.iter().zip(&ops) {
        for st in &op.subtask_states {
            let StateHandle::KeyGroups(kg) = &st.managed_keyed_state else {
                continue;
            };
            let StreamStateHandle::ByteStream(bs) = &kg.delegate_handle else {
                continue;
            };
            let header_end = kg
                .offsets
                .iter()
                .copied()
                .filter(|&o| o > 0)
                .min()
                .unwrap_or(bs.data.len() as i64) as usize;
            let metas = extract_state_metadata(&bs.data);
            let layouts = extract_entry_layouts(&bs.data, header_end);
            assert_eq!(layouts.len(), metas.len());
            for (m, l) in metas.iter().zip(&layouts) {
                // Window operators namespace by TimeWindow (two longs), everything else by
                // VoidNamespace (one byte).
                let want = if m.name == "window-contents" || m.name.ends_with("_window-timers") {
                    16
                } else {
                    1
                };
                assert_eq!(l.namespace_len, Some(want), "namespace of {}", m.name);
                let descriptor = keyed.as_ref().and_then(|ks| ks.state(&m.name)).unwrap();
                let want_map_key = descriptor
                    .value_type
                    .strip_prefix("Map<")
                    .map(|_| parser::value::split_map_types(&descriptor.value_type).0);
                assert_eq!(l.map_key_type, want_map_key, "map key of {}", m.name);
                checked += 1;
            }
        }
    }
    assert!(checked > 40, "only {} states checked", checked);
}
