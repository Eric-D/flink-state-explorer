//! End-to-end decoder tests against the generated savepoint: protobuf-in-POJO, tuples, primitive
//! arrays, and high-cardinality (scrollbar) coverage. All go through the production decode path
//! (`parser::value` via `export::export_json`), so they also prove export == TUI capability.
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use flink_explorer::index::builder::build_index;
use flink_explorer::index::cache::IndexCache;
use flink_explorer::parser;
use flink_explorer::profile::storage::ProtoPattern;
use flink_explorer::proto::{compiler::compile_protos, ProtoContext};
use serde_json::Value;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn load_index() -> IndexCache {
    let dir = Path::new(SAVEPOINT_DIR);
    let metadata_path = dir.join("_metadata");
    assert!(
        metadata_path.exists(),
        "run `cd test-flink-job && mvn verify` first"
    );
    let savepoint = parser::parse_metadata(&metadata_path).expect("parse metadata");
    let meta = std::fs::metadata(&metadata_path).unwrap();
    build_index(&savepoint, meta.len(), 0u64, dir)
}

/// Export the whole index to JSON (optionally with a proto context) and return the parsed value.
/// Uses a unique temp file so parallel tests don't clobber each other.
fn export(index: &IndexCache, proto: Option<&ProtoContext>) -> Value {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    let tmp = std::env::temp_dir().join(format!(
        "decode_e2e_{}_{}.json",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    flink_explorer::export::export_json(index, proto, &tmp).expect("export_json");
    let v = serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();
    std::fs::remove_file(&tmp).ok();
    v
}

/// Find the first non-null decoded value of a named state across all operators/keys.
fn first_state_value(json: &Value, state: &str) -> Option<Value> {
    for op in json["operators"].as_array()? {
        for k in op["keys"].as_array()? {
            if let Some(v) = k["states"].get(state) {
                if !v.is_null() {
                    return Some(v.clone());
                }
            }
        }
    }
    None
}

#[test]
fn tuples_decode() {
    let json = export(&load_index(), None);
    let t2 = first_state_value(&json, "tuple2-state").expect("tuple2-state");
    let t3 = first_state_value(&json, "tuple3-state").expect("tuple3-state");
    // Tuple2<String, Long>
    let a2 = t2.as_array().expect("tuple2 is array");
    assert_eq!(a2.len(), 2, "Tuple2 has 2 fields: {}", t2);
    assert!(a2[0].is_string(), "Tuple2.f0 is String");
    assert!(a2[1].is_i64(), "Tuple2.f1 is Long");
    // Tuple3<String, Integer, Boolean>
    let a3 = t3.as_array().expect("tuple3 is array");
    assert_eq!(a3.len(), 3, "Tuple3 has 3 fields: {}", t3);
    assert!(
        a3[0].is_string() && a3[1].is_i64() && a3[2].is_boolean(),
        "Tuple3 types: {}",
        t3
    );
}

#[test]
fn primitive_arrays_decode() {
    let json = export(&load_index(), None);
    let int_arr = first_state_value(&json, "int-array").expect("int-array");
    // int[]{1, 2, 3, value.length()} → first three are 1,2,3
    let a = int_arr.as_array().expect("int-array is array");
    assert!(a.len() >= 4, "int[] len: {}", int_arr);
    assert_eq!(a[0], 1);
    assert_eq!(a[1], 2);
    assert_eq!(a[2], 3);

    let long_arr = first_state_value(&json, "long-array").expect("long-array");
    let la = long_arr.as_array().expect("long-array is array");
    assert_eq!(la[0], 100, "long[]{{100,200,...}}: {}", long_arr);
    assert_eq!(la[1], 200);

    // AggregatingState accumulator double[]{sum, count}
    let avg = first_state_value(&json, "running-average").expect("running-average");
    assert!(
        avg.as_array().map_or(false, |a| a.len() == 2),
        "double[] accumulator: {}",
        avg
    );
}

#[test]
fn pojo_value_state_fully_decodes() {
    let json = export(&load_index(), None);
    // SimpleEvent: id (String), createdAt (Date), eventType (Enum), active (bool), timestamp (long)
    let se = first_state_value(&json, "simple-event").expect("simple-event");
    assert!(se["id"].is_string(), "id decoded: {}", se);
    assert!(
        se["createdAt"].is_string(),
        "createdAt (Date) decoded: {}",
        se
    );
    assert!(
        se["eventType"].is_string(),
        "eventType (Enum) decoded to name: {}",
        se
    );
    assert!(se["active"].is_boolean(), "active decoded: {}", se);
}

#[test]
fn lists_and_string_array_decode() {
    let json = export(&load_index(), None);
    // Keyed ListState: delimiter-separated elements.
    let large = first_state_value(&json, "large-list").expect("large-list");
    let la = large.as_array().expect("large-list is array");
    assert!(
        la.len() >= 500,
        "large-list should have >500 elements, got {}",
        la.len()
    );
    assert!(
        la[0].is_i64() && la[1].as_i64().unwrap() - la[0].as_i64().unwrap() == 1000,
        "large-list elements are longs spaced by 1000: {:?}..",
        &la[..2]
    );

    let logs = first_state_value(&json, "log-entries").expect("log-entries");
    assert!(
        logs.as_array().map_or(false, |a| a
            .iter()
            .any(|v| v.as_str().map_or(false, |s| s.starts_with("Entry at")))),
        "log-entries should be strings: {}",
        logs
    );

    let hist = first_state_value(&json, "event-history").expect("event-history");
    let h = hist.as_array().expect("event-history is array of POJOs");
    assert!(
        h[0].get("id").is_some() && h[0].get("eventType").is_some(),
        "List<SimpleEvent>: {}",
        hist
    );

    let sa = first_state_value(&json, "string-array").expect("string-array");
    assert_eq!(
        sa,
        serde_json::json!(["tag-a", "tag-b", "tag-c"]),
        "String[] decode"
    );
}

#[test]
fn maps_decode_keys_and_values() {
    let json = export(&load_index(), None);
    // Map<String,String>: real keys and values.
    let m = first_state_value(&json, "simple-string-map").expect("simple-string-map");
    let obj = m.as_object().expect("map is object");
    assert!(
        obj.keys().all(|k| !k.is_empty()),
        "map keys must not be empty: {}",
        m
    );
    assert!(
        obj.values()
            .any(|v| v.as_str().map_or(false, |s| !s.is_empty())),
        "map values: {}",
        m
    );
    // Map<Long,Date>: numeric string keys, ISO date values.
    let ts = first_state_value(&json, "timestamp-index").expect("timestamp-index");
    let to = ts.as_object().expect("object");
    assert!(
        to.keys().all(|k| k.parse::<i64>().is_ok()),
        "Long map keys: {}",
        ts
    );
    // Map<String,SimpleEvent>: enum-name keys, full POJO values.
    let ebt = first_state_value(&json, "events-by-type").expect("events-by-type");
    let eo = ebt.as_object().expect("object");
    assert!(
        eo.values().next().and_then(|v| v.get("id")).is_some(),
        "Map value is SimpleEvent: {}",
        ebt
    );
    // Map<Enum,Boolean>: keys are enum constant names, not ordinal(N).
    let flags = first_state_value(&json, "flags-by-category").expect("flags-by-category");
    let fo = flags.as_object().expect("object");
    assert!(
        fo.keys()
            .all(|k| !k.starts_with("ordinal(")
                && k.chars().all(|c| c.is_ascii_uppercase() || c == '_')),
        "enum map keys should be constant names, got: {}",
        flags
    );
}

#[test]
fn operator_state_names_modes_values() {
    let index = load_index();
    let mut modes = std::collections::HashMap::new();
    for op in &index.operators {
        for s in &op.states {
            modes.insert(s.name.clone(), s.state_type.clone());
        }
    }
    assert_eq!(
        modes.get("operator-buffer").map(String::as_str),
        Some("operator/SPLIT_DISTRIBUTE")
    );
    assert_eq!(
        modes.get("operator-union-buffer").map(String::as_str),
        Some("operator/UNION")
    );
    assert_eq!(
        modes.get("broadcast-config").map(String::as_str),
        Some("operator/BROADCAST")
    );
    // Operator list-state elements decode to strings.
    let op = index
        .operators
        .iter()
        .find(|o| o.states.iter().any(|s| s.name == "operator-buffer"))
        .unwrap();
    let s = op
        .states
        .iter()
        .find(|s| s.name == "operator-buffer")
        .unwrap();
    assert!(!s.entries.is_empty(), "operator-buffer has elements");
    let first = String::from_utf8_lossy(&s.entries[0].key);
    assert!(
        first.starts_with('"'),
        "element decoded as string, got {}",
        first
    );
}

#[test]
fn operator_info_in_export() {
    let json = export(&load_index(), None);
    let ops = json["operators"].as_array().unwrap();
    // Every operator carries an info block with a 32-hex OperatorID (uuid).
    assert!(
        ops.iter().all(|o| o["info"]["OperatorID (uuid)"]
            .as_str()
            .map_or(false, |s| s.len() == 32)),
        "every operator has a 32-hex OperatorID"
    );
    // The export now includes operators with NO keyed state (sources/sinks/operator-state).
    let keyed = ops
        .iter()
        .filter(|o| !o["keys"].as_array().unwrap().is_empty())
        .count();
    assert!(
        ops.len() > keyed,
        "export includes non-keyed operators ({} total, {} keyed)",
        ops.len(),
        keyed
    );
    // Operator states are exported with their distribution mode.
    let modes: Vec<String> = ops
        .iter()
        .flat_map(|o| o["operator_states"].as_array().unwrap())
        .filter_map(|s| s["mode"].as_str().map(String::from))
        .collect();
    assert!(
        modes.iter().any(|m| m.contains("BROADCAST")),
        "broadcast mode in export: {:?}",
        modes
    );
    if let Some(op) = ops.iter().find(|o| {
        o["operator_states"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "operator-buffer")
    }) {
        println!(
            "operator info sample:\n{}",
            serde_json::to_string_pretty(&op["info"]).unwrap()
        );
    }
}

#[test]
fn windows_and_timers_decode() {
    let json = export(&load_index(), None);

    // Window contents: the buffered window elements (a non-empty list).
    let wc = first_state_value(&json, "window-contents").expect("window-contents");
    assert!(
        wc.as_array().map_or(false, |a| !a.is_empty()),
        "window-contents non-empty: {}",
        wc
    );

    // A processing-time timer state decodes to an ISO timestamp string (the firing time).
    let mut found_timer = false;
    for op in json["operators"].as_array().unwrap() {
        for k in op["keys"].as_array().unwrap() {
            for (name, v) in k["states"].as_object().unwrap() {
                if name.contains("_timer_state/processing_") && v.is_string() {
                    let s = v.as_str().unwrap();
                    assert!(
                        s.contains('T') && s.ends_with('Z'),
                        "timer is an ISO date: {}",
                        s
                    );
                    found_timer = true;
                }
            }
        }
    }
    assert!(
        found_timer,
        "expected at least one decoded processing-time timer"
    );
}

#[test]
fn high_cardinality_for_scrollbar() {
    let index = load_index();
    let proto_op = index
        .operators
        .iter()
        .find(|o| {
            o.keyed_state.as_ref().map_or(false, |k| {
                k.descriptors.iter().any(|d| d.name == "proto-event")
            })
        })
        .expect("proto-event operator");
    let ks = proto_op.keyed_state.as_ref().unwrap();
    assert!(
        ks.total_keys >= 600,
        "expected >=600 keys for the scrollbar, got {}",
        ks.total_keys
    );
}

#[test]
fn protobuf_in_pojo_decodes_with_profile() {
    // Write a .proto matching the hand-encoded TestEvent and build a proto context.
    let dir: PathBuf = std::env::temp_dir().join(format!("flinkexp_proto_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("test_event.proto"),
        "syntax = \"proto3\";\npackage com.test.proto;\nmessage TestEvent { int64 id = 1; string name = 2; bool active = 3; }\n",
    )
    .unwrap();
    let pool = compile_protos(&[dir.as_path()]).expect("compile proto");

    let ctx = ProtoContext {
        pool,
        bindings: HashMap::new(),
        patterns: vec![ProtoPattern {
            class_field: "className".to_string(),
            bytes_field: "protoBytes".to_string(),
            resolve: "direct".to_string(), // className IS the proto FQN
            path_overrides: HashMap::new(),
        }],
    };

    let json = export(&load_index(), Some(&ctx));
    let pe = first_state_value(&json, "proto-event").expect("proto-event");
    // className must round-trip; protoBytes must now be the DECODED message, not "[N bytes]".
    assert_eq!(
        pe["className"],
        Value::String("com.test.proto.TestEvent".to_string())
    );
    let decoded = pe["protoBytes"]
        .as_str()
        .expect("protoBytes decoded to string");
    assert!(
        !decoded.contains("bytes]") && decoded.contains("name") && decoded.contains("evt-"),
        "protoBytes should be decoded TestEvent (name=evt-...), got: {}",
        decoded
    );
    println!("Decoded protobuf: {}", decoded);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn ttl_and_broadcast_decode() {
    let json = export(&load_index(), None);

    // TTL: value_type Ttl<Long> → { _ttlLastAccess: <iso>, value: <user value> }.
    let ttl = first_state_value(&json, "ttl-counter").expect("ttl-counter");
    assert!(
        ttl["_ttlLastAccess"]
            .as_str()
            .map_or(false, |s| s.ends_with('Z')),
        "TTL lastAccess timestamp decoded: {}",
        ttl
    );
    assert!(
        ttl["value"].is_i64(),
        "TTL inner user value decoded: {}",
        ttl
    );

    // Broadcast: HeapBroadcastState map decoded into key/value pairs.
    let mut bc = None;
    for op in json["operators"].as_array().unwrap() {
        for s in op["operator_states"].as_array().unwrap() {
            if s["name"] == "broadcast-config" {
                bc = s["values"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_str())
                    .map(String::from);
            }
        }
    }
    let bc = bc.expect("broadcast-config present");
    assert!(
        bc.contains("config-a") && bc.contains("CONFIG-A"),
        "broadcast map decoded to key/value: {}",
        bc
    );
}

#[test]
fn value_class_in_export() {
    let json = export(&load_index(), None);
    let mut classes = std::collections::HashMap::new();
    for op in json["operators"].as_array().unwrap() {
        for d in op["state_descriptors"].as_array().unwrap() {
            if let Some(c) = d["value_class"].as_str() {
                classes.insert(d["name"].as_str().unwrap().to_string(), c.to_string());
            }
        }
    }
    // Fully-qualified value class (package + name) is exposed for POJO-valued states.
    assert_eq!(
        classes.get("simple-event").map(String::as_str),
        Some("com.test.explorer.model.SimpleEvent")
    );
    assert_eq!(
        classes.get("complex-record").map(String::as_str),
        Some("com.test.explorer.model.ComplexRecord")
    );
    assert_eq!(
        classes.get("proto-event").map(String::as_str),
        Some("com.test.explorer.model.ProtoWrapper")
    );
    // Timer states are not POJOs → no spurious value_class.
    assert!(
        classes.keys().all(|k| !k.contains("_timer_state")),
        "no class on timer states: {:?}",
        classes
    );
}
