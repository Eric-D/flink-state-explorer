use std::path::Path;

use flink_explorer::index::builder::build_index;
use flink_explorer::index::cache::{CachedOperator, IndexCache};
use flink_explorer::parser;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn load_index() -> IndexCache {
    let dir = Path::new(SAVEPOINT_DIR);
    let metadata_path = dir.join("_metadata");
    assert!(metadata_path.exists(), "Savepoint _metadata not found");
    let savepoint = parser::parse_metadata(&metadata_path).expect("Failed to parse metadata");
    let meta = std::fs::metadata(&metadata_path).unwrap();
    build_index(&savepoint, meta.len(), 0u64, dir)
}

/// Find an operator by checking if it has a specific state descriptor name.
fn find_operator_with_state<'a>(
    index: &'a IndexCache,
    state_name: &str,
) -> Option<&'a CachedOperator> {
    index.operators.iter().find(|o| {
        o.keyed_state.as_ref().map_or(false, |ks| {
            ks.descriptors.iter().any(|d| d.name == state_name)
        })
    })
}

#[test]
fn savepoint_loads_successfully() {
    let index = load_index();
    println!("Checkpoint ID: {}", index.checkpoint_id);
    println!("Version: {}", index.savepoint_version);
    println!("Operators: {}", index.operators.len());
    assert!(index.operators.len() > 0);
}

#[test]
fn all_types_operator_found() {
    let index = load_index();
    let op = find_operator_with_state(&index, "counter")
        .expect("All Types operator (with 'counter' state) not found");
    let ks = op.keyed_state.as_ref().unwrap();

    println!(
        "All Types operator: {} keys, {} states, key_type={}",
        ks.total_keys,
        ks.descriptors.len(),
        ks.key_type
    );
    assert_eq!(ks.key_type, "String");
    assert!(ks.total_keys > 0);

    let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
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
    let index = load_index();
    let op = find_operator_with_state(&index, "counter").unwrap();
    let ks = op.keyed_state.as_ref().unwrap();
    let idx = ks
        .descriptors
        .iter()
        .position(|d| d.name == "counter")
        .unwrap();
    let part = &ks.partitions[0];
    let raw = part.values.get(idx).and_then(|v| v.as_ref());
    assert!(raw.is_some(), "counter has no value for key: {}", part.key);
    let raw = raw.unwrap();
    assert_eq!(
        raw.len(),
        8,
        "counter should be 8 bytes (Long), got {}",
        raw.len()
    );
    let val = i64::from_be_bytes(raw[..8].try_into().unwrap());
    println!("counter = {} for key '{}'", val, part.key);
    assert!(val > 0);
}

#[test]
fn label_value_is_string() {
    let index = load_index();
    let op = find_operator_with_state(&index, "label").unwrap();
    let ks = op.keyed_state.as_ref().unwrap();
    let idx = ks
        .descriptors
        .iter()
        .position(|d| d.name == "label")
        .unwrap();
    let part = &ks.partitions[0];
    let raw = part.values.get(idx).and_then(|v| v.as_ref());
    assert!(raw.is_some(), "label has no value");
    let raw = raw.unwrap();
    let mut r = flink_explorer::parser::java_deser::JavaReader::new(raw.as_slice());
    let val = r.read_string_value().expect("Failed to read StringValue");
    println!("label = '{}' for key '{}'", val, part.key);
    assert!(
        val.starts_with("label-for-"),
        "Expected 'label-for-...', got '{}'",
        val
    );
}

#[test]
fn long_key_operator() {
    let index = load_index();
    let op = find_operator_with_state(&index, "element-count")
        .expect("Long Key operator (with 'element-count') not found");
    let ks = op.keyed_state.as_ref().unwrap();
    println!("Long Key: {} keys, key_type={}", ks.total_keys, ks.key_type);
    assert_eq!(ks.key_type, "Long");
    let part = &ks.partitions[0];
    assert!(
        part.key.parse::<i64>().is_ok(),
        "Long key should be numeric: {}",
        part.key
    );
}

#[test]
fn pojo_key_operator() {
    let index = load_index();
    let op = find_operator_with_state(&index, "latest-event")
        .expect("POJO Key operator (with 'latest-event') not found");
    let ks = op.keyed_state.as_ref().unwrap();
    println!("POJO Key: {} keys, key_type={}", ks.total_keys, ks.key_type);
    assert!(ks.key_type == "Pojo" || ks.key_type.contains("Pojo"));
    let part = &ks.partitions[0];
    println!("Key: {}", part.key);
    assert!(
        part.key.contains("{"),
        "POJO key should be JSON-like: {}",
        part.key
    );
}

#[test]
fn map_state_has_entries() {
    let index = load_index();
    let op = find_operator_with_state(&index, "events-by-type").unwrap();
    let ks = op.keyed_state.as_ref().unwrap();
    let desc = ks
        .descriptors
        .iter()
        .find(|d| d.name == "events-by-type")
        .unwrap();
    println!(
        "events-by-type: state_type={} value_type={} entries={}",
        desc.state_type, desc.value_type, desc.entry_count
    );
    assert_eq!(desc.state_type, "MAP");
    assert!(desc.entry_count > 0, "MAP should have entries");

    // Check blob format
    let idx = ks
        .descriptors
        .iter()
        .position(|d| d.name == "events-by-type")
        .unwrap();
    let mut map_count = 0;
    for part in &ks.partitions {
        if let Some(Some(raw)) = part.values.get(idx) {
            if raw.len() >= 4 {
                let count = i32::from_be_bytes(raw[..4].try_into().unwrap());
                println!(
                    "  key={}: {} map pairs, {} bytes",
                    part.key,
                    count,
                    raw.len()
                );
                map_count += count;
            }
        }
    }
    println!("Total MAP pairs across all keys: {}", map_count);
    assert!(map_count > 0, "Should have MAP pairs");
}

#[test]
fn list_state_has_entries() {
    let index = load_index();
    let op = find_operator_with_state(&index, "large-list").unwrap();
    let ks = op.keyed_state.as_ref().unwrap();
    let desc = ks
        .descriptors
        .iter()
        .find(|d| d.name == "large-list")
        .unwrap();
    println!(
        "large-list: state_type={} entries={}",
        desc.state_type, desc.entry_count
    );
    assert_eq!(desc.state_type, "LIST");
}

#[test]
fn coprocess_operator() {
    let index = load_index();
    let op = find_operator_with_state(&index, "left-value").expect("CoProcess operator not found");
    let ks = op.keyed_state.as_ref().unwrap();
    let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
    println!("CoProcess states: {:?}", names);
    assert!(names.contains(&"left-value"));
    assert!(names.contains(&"right-value"));
    assert!(names.contains(&"join-timestamp"));
    assert!(names.contains(&"seen-keys"));
}

#[test]
fn tuple_reduce_operator() {
    let index = load_index();
    let op =
        find_operator_with_state(&index, "running-sum").expect("Tuple Reduce operator not found");
    let ks = op.keyed_state.as_ref().unwrap();
    let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
    println!("Tuple Reduce states: {:?}", names);
    assert!(names.contains(&"running-sum"));
    assert!(names.contains(&"tuple2-state"));
    assert!(names.contains(&"tuple3-state"));

    let reducing = ks
        .descriptors
        .iter()
        .find(|d| d.name == "running-sum")
        .unwrap();
    println!("running-sum state_type={}", reducing.state_type);
    assert_eq!(reducing.state_type, "REDUCING");
}

#[test]
fn extra_types_operator() {
    let index = load_index();
    let op = find_operator_with_state(&index, "byte-val").expect("Extra Types operator not found");
    let ks = op.keyed_state.as_ref().unwrap();
    let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
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
    let null_desc = ks
        .descriptors
        .iter()
        .find(|d| d.name == "nullable-never-set")
        .unwrap();
    assert_eq!(
        null_desc.entry_count, 0,
        "nullable-never-set should have 0 entries"
    );

    // byte-val should be 1 byte
    let idx = ks
        .descriptors
        .iter()
        .position(|d| d.name == "byte-val")
        .unwrap();
    let part = &ks.partitions[0];
    if let Some(Some(raw)) = part.values.get(idx) {
        assert_eq!(raw.len(), 1, "byte-val should be 1 byte");
        println!("byte-val = {}", raw[0] as i8);
    }
}

#[test]
fn summary_report() {
    let index = load_index();
    println!("\n=== SAVEPOINT SUMMARY ===");
    println!("Version: {}", index.savepoint_version);
    println!("Checkpoint ID: {}", index.checkpoint_id);
    println!("Operators: {}", index.operators.len());

    let keyed_ops = index
        .operators
        .iter()
        .filter(|o| o.keyed_state.is_some())
        .count();
    let total_keys: usize = index
        .operators
        .iter()
        .filter_map(|o| o.keyed_state.as_ref())
        .map(|k| k.total_keys)
        .sum();
    let total_states: usize = index
        .operators
        .iter()
        .filter_map(|o| o.keyed_state.as_ref())
        .map(|k| k.descriptors.len())
        .sum();

    println!("Keyed operators: {}", keyed_ops);
    println!("Total keys: {}", total_keys);
    println!("Total states: {}", total_states);

    let key_types: Vec<&str> = index
        .operators
        .iter()
        .filter_map(|o| o.keyed_state.as_ref())
        .filter(|k| k.total_keys > 0)
        .map(|k| k.key_type.as_str())
        .collect();
    println!("Key types: {:?}", key_types);

    let mut types = std::collections::HashMap::new();
    for op in &index.operators {
        if let Some(ks) = &op.keyed_state {
            for d in &ks.descriptors {
                *types.entry(d.state_type.as_str()).or_insert(0) += 1;
            }
        }
    }
    println!("State types: {:?}", types);
    println!("=== DONE ===");
}
