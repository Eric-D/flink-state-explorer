//! End-to-end decoder tests against the generated savepoint: protobuf-in-POJO, tuples, primitive
//! arrays, and high-cardinality (scrollbar) coverage. All go through the production decode path
//! (`parser::value` via the `StateReader` and `export::export_json`). That the TUI renders the same values is proved by
//! `tests/tui_parity_test.rs` (synthetic, in CI) and `tests/tui_gitlab_parity_test.rs` (fixture).
mod common;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use flink_explorer::parser;
use flink_explorer::profile::storage::ProtoPattern;
use flink_explorer::proto::{compiler::compile_protos, ProtoContext};
use flink_explorer::reader::StateReader;
use serde_json::Value;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn dir() -> &'static Path {
    let dir = Path::new(SAVEPOINT_DIR);
    assert!(
        dir.join("_metadata").exists(),
        "run `cd test-flink-job && mvn verify` first"
    );
    dir
}

/// Export the whole savepoint to JSON through the reader (optionally decoding protobuf with a
/// proto context) and return the parsed value. Uses a unique temp file so parallel tests don't
/// clobber each other, and never writes into the fixture.
fn export(proto: Option<&ProtoContext>) -> Value {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    let tmp = std::env::temp_dir().join(format!(
        "decode_e2e_{}_{}.json",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let mut reader = StateReader::open(dir()).unwrap();
    if let Some(ctx) = proto {
        reader = reader.with_proto(Arc::new(ProtoContext {
            pool: ctx.pool.clone(),
            bindings: ctx.bindings.clone(),
            patterns: ctx.patterns.clone(),
        }));
    }
    let summaries = flink_explorer::operators::summarize(reader.savepoint(), dir());
    flink_explorer::export::export_json(
        &reader,
        &summaries,
        &tmp,
        &AtomicBool::new(false),
        &mut |_| {},
    )
    .expect("export_json");
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
    let json = export(None);
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
    let json = export(None);
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
        avg.as_array().is_some_and(|a| a.len() == 2),
        "double[] accumulator: {}",
        avg
    );
}

#[test]
fn pojo_value_state_fully_decodes() {
    let json = export(None);
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
    let json = export(None);
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
        logs.as_array().is_some_and(|a| a
            .iter()
            .any(|v| v.as_str().is_some_and(|s| s.starts_with("Entry at")))),
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
    let json = export(None);
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
            .any(|v| v.as_str().is_some_and(|s| !s.is_empty())),
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
    let ops = flink_explorer::operators::summarize(
        &parser::parse_metadata(&dir().join("_metadata")).unwrap(),
        dir(),
    );
    let mut modes = std::collections::HashMap::new();
    for op in &ops {
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
    let op = ops
        .iter()
        .find(|o| o.states.iter().any(|s| s.name == "operator-buffer"))
        .unwrap();
    let s = op
        .states
        .iter()
        .find(|s| s.name == "operator-buffer")
        .unwrap();
    assert!(!s.elements.is_empty(), "operator-buffer has elements");
    let first = &s.elements[0].label;
    assert!(
        first.starts_with('"'),
        "element decoded as string, got {}",
        first
    );
}

#[test]
fn operator_info_in_export() {
    let json = export(None);
    let ops = json["operators"].as_array().unwrap();
    // Every operator carries an info block with a 32-hex OperatorID (uuid).
    assert!(
        ops.iter().all(|o| o["info"]["OperatorID (uuid)"]
            .as_str()
            .is_some_and(|s| s.len() == 32)),
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
    let json = export(None);

    // Window contents: the buffered window elements (a non-empty list).
    let wc = first_state_value(&json, "window-contents").expect("window-contents");
    assert!(
        wc.as_array().is_some_and(|a| !a.is_empty()),
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
    let ops = common::keyed::operators(dir());
    let (_, ks) =
        common::keyed::find_with_state(&ops, "proto-event").expect("proto-event operator");
    assert!(
        ks.keys.len() >= 600,
        "expected >=600 keys for the scrollbar, got {}",
        ks.keys.len()
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

    let json = export(Some(&ctx));
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
    let json = export(None);

    // TTL: value_type Ttl<Long> → { _ttlLastAccess: <iso>, value: <user value> }.
    let ttl = first_state_value(&json, "ttl-counter").expect("ttl-counter");
    assert!(
        ttl["_ttlLastAccess"]
            .as_str()
            .is_some_and(|s| s.ends_with('Z')),
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

/// The fields of `AllFieldsPojo` that decode, in Flink's field order (`PojoTypeInfo` sorts them
/// by name), up to and including `deepNested`.
const ALL_FIELDS_DECODED: [&str; 11] = [
    "boolArray",
    "boolBoxed",
    "boolField",
    "byteArray",
    "byteBoxed",
    "byteField",
    "charArray",
    "charBoxed",
    "charField",
    "dateField",
    "deepNested",
];

/// One `AllFieldsPojo` value, as `AllFieldsProcessFunction.buildPojo` (test-flink-job) fills it.
fn assert_all_fields_pojo(v: &Value, ctx: &str) {
    let obj = v
        .as_object()
        .unwrap_or_else(|| panic!("{}: not an object: {}", ctx, v));
    assert_eq!(obj.len(), 43, "{}: AllFieldsPojo has 43 fields", ctx);
    assert_eq!(
        obj["boolArray"],
        serde_json::json!([true, false, true, true])
    );
    assert_eq!(obj["boolBoxed"], serde_json::json!(false));
    assert_eq!(obj["boolField"], serde_json::json!(true));
    assert_eq!(obj["byteArray"], serde_json::json!("Hello"));
    assert_eq!(obj["byteBoxed"], serde_json::json!(-1));
    assert_eq!(obj["byteField"], serde_json::json!(42));
    // #25: `char[]`, `Character` and `char` decode — and no longer stop the POJO.
    assert_eq!(
        obj["charArray"],
        serde_json::json!(["X", "Y", "Z"]),
        "{}",
        ctx
    );
    assert_eq!(obj["charBoxed"], serde_json::json!("Z"), "{}", ctx);
    assert_eq!(obj["charField"], serde_json::json!("A"), "{}", ctx);
    assert!(
        obj["dateField"].as_str().is_some_and(|s| s.ends_with('Z')),
        "{}: dateField is an ISO date: {}",
        ctx,
        obj["dateField"]
    );
    // `DeepNested.innerList` is a `List` field, which Flink 1.20 serializes with Kryo (the
    // snapshot says `KryoSerializer`): its length is unknown, so decoding stops there, visibly.
    assert_eq!(
        obj["deepNested"],
        serde_json::json!({
            "innerAddress": { "city": "Lyon", "street": "456 Oak Ave", "zipCode": 69001 },
            "innerEnum": "ARCHIVE",
            "innerList": "[Kryo]",
            "label": "[not decoded: follows innerList]",
        }),
        "{}",
        ctx
    );
    // Every later field is marked, not null — although some of them are null in Flink
    // (`nullString`, `shortBoxed`, ...), none can be located any more.
    for (name, value) in obj {
        if !ALL_FIELDS_DECODED.contains(&name.as_str()) {
            assert_eq!(
                value,
                &serde_json::json!("[not decoded: follows deepNested]"),
                "{}: field {}",
                ctx,
                name
            );
        }
    }
}

#[test]
fn all_fields_pojo_decodes_past_its_char_fields_or_says_why_not() {
    // Reader path (the JSON export, and the TUI's data).
    let json = export(None);
    let mut seen = 0;
    for op in json["operators"].as_array().unwrap() {
        for k in op["keys"].as_array().unwrap() {
            let key = k["key"].as_str().unwrap_or("?");
            if let Some(v) = k["states"].get("all-fields-pojo").filter(|v| !v.is_null()) {
                assert_all_fields_pojo(v, &format!("reader all-fields-pojo {}", key));
                seen += 1;
            }
            if let Some(m) = k["states"].get("all-fields-map").filter(|v| !v.is_null()) {
                for (mk, v) in m.as_object().expect("a map") {
                    assert_all_fields_pojo(v, &format!("reader all-fields-map {} {}", key, mk));
                    seen += 1;
                }
            }
        }
    }
    assert!(
        seen >= 16,
        "expected the fixture's 8 keys in both states, saw {}",
        seen
    );

    // Streaming path (NDJSON/Parquet rows).
    let dir = Path::new(SAVEPOINT_DIR);
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    for state in ["all-fields-pojo", "all-fields-map"] {
        let mut out = Vec::new();
        flink_explorer::export_state::export_state_ndjson(
            &savepoint, dir, None, state, None, &mut out,
        )
        .expect("export");
        let rows: Vec<Value> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(!rows.is_empty());
        for row in &rows {
            assert_all_fields_pojo(&row["value"], &format!("stream {} {}", state, row["key"]));
        }
    }

    // The TUI's display string, through the same decoder: values and markers, no `null`.
    let ops = common::keyed::operators(dir);
    let (_, ks) = common::keyed::find_with_state(&ops, "all-fields-pojo").unwrap();
    let pi = StateReader::open(dir)
        .unwrap()
        .value_pojo(&ks.op_hex, "all-fields-pojo")
        .unwrap()
        .expect("POJO info");
    let key = ks
        .keys
        .iter()
        .find(|k| ks.raw(&k.key, "all-fields-pojo").is_some())
        .expect("a key with a value")
        .key
        .clone();
    let raw = ks.raw(&key, "all-fields-pojo").unwrap();
    let shown = parser::pojo::deserialize_pojo_to_json(&raw, &pi, None);
    for part in [
        "charArray: [\"X\", \"Y\", \"Z\"]",
        "charBoxed: \"Z\"",
        "charField: \"A\"",
        "innerList: [Kryo]",
        "label: [not decoded: follows innerList]",
        "stringMap: [not decoded: follows deepNested]",
    ] {
        assert!(shown.contains(part), "TUI shows {:?}: {}", part, shown);
    }
    assert!(
        !shown.contains(": null"),
        "no null value in the TUI rendering: {}",
        shown
    );
}

#[test]
fn value_class_in_export() {
    let json = export(None);
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
