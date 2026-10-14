//! Coverage audit: dumps every operator and every state the explorer reads from the
//! generated savepoint, so we can verify exhaustiveness across all stateful operator types.
mod common;

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::keyed::{find_with_state, operators, reader, Keyed};
use flink_explorer::operators::OperatorSummary;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn dir() -> &'static Path {
    let dir = Path::new(SAVEPOINT_DIR);
    assert!(
        dir.join("_metadata").exists(),
        "Savepoint _metadata not found — run `cd test-flink-job && mvn verify`"
    );
    dir
}

fn load() -> Vec<(OperatorSummary, Option<Keyed>)> {
    operators(dir())
}

/// The `E` export (streamed from the reader) of the whole fixture, written to a temp file.
fn export(name: &str) -> serde_json::Value {
    let r = reader(dir());
    let summaries = flink_explorer::operators::summarize(r.savepoint(), dir());
    let tmp = std::env::temp_dir().join(format!("{}-{}.json", name, std::process::id()));
    flink_explorer::export::export_json(&r, &summaries, &tmp, &AtomicBool::new(false), &mut |_| {})
        .expect("export_json");
    let json = serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();
    let _ = std::fs::remove_file(&tmp);
    json
}

#[test]
fn diagnose_value_types_for_all_types_operator() {
    let ops = load();
    // The "all types" operator (has 'counter') — check value_type label vs actual bytes & class.
    let (_, ks) = find_with_state(&ops, "counter").expect("all-types operator");
    let key = &ks.keys[0].key;
    println!("\n=== value_type label vs reality (key '{}') ===", key);
    for d in &ks.states {
        let blen = ks.raw(key, &d.name).map(|b| b.len());
        let pojo_class = d
            .value_class
            .as_ref()
            .map(|c| c.rsplit('.').next().unwrap_or("").to_string());
        println!(
            "  {:<16} state={:<9} value_type={:<22} pojo={:<14?} bytes={:?}",
            d.name, d.state_type, d.value_type, pojo_class, blen
        );
    }
}

/// Verify nested POJO (ComplexRecord.address) and tuple states decode via the production
/// export path (the same reader and decoder the TUI uses).
#[test]
fn verify_tuples_and_nested_pojo() {
    let json = export("sp_export_audit");

    let mut complex = serde_json::Value::Null;
    let mut t2 = serde_json::Value::Null;
    let mut t3 = serde_json::Value::Null;
    for op in json["operators"].as_array().unwrap() {
        for k in op["keys"].as_array().unwrap() {
            let st = &k["states"];
            for (slot, name) in [
                (&mut complex, "complex-record"),
                (&mut t2, "tuple2-state"),
                (&mut t3, "tuple3-state"),
            ] {
                if slot.is_null() {
                    if let Some(v) = st.get(name) {
                        if !v.is_null() {
                            *slot = v.clone();
                        }
                    }
                }
            }
        }
    }

    println!(
        "\n--- NESTED POJO (complex-record) ---\n{}",
        serde_json::to_string_pretty(&complex).unwrap()
    );
    println!(
        "\n--- TUPLES ---\ntuple2-state = {}\ntuple3-state = {}",
        t2, t3
    );

    // Nested POJO: ComplexRecord must expose its nested NestedAddress.
    assert!(
        complex.is_object(),
        "complex-record should decode to an object"
    );
    let addr = complex
        .get("address")
        .expect("ComplexRecord.address (nested POJO) present");
    assert!(
        addr.get("street").is_some() && addr.get("city").is_some() && addr.get("zipCode").is_some(),
        "nested NestedAddress should expose street/city/zipCode, got {}",
        addr
    );
}

/// Dump the production-decoded value of every keyed state (first key per operator) so we can
/// see exactly which binary payloads decode, which come out null, and which are shown raw.
#[test]
fn dump_decoded_values() {
    let json = export("sp_export_full");

    println!("\n============ DECODED VALUES (first key per operator) ============");
    for op in json["operators"].as_array().unwrap() {
        let keys = op["keys"].as_array().unwrap();
        if keys.is_empty() {
            continue;
        }
        let k = &keys[0];
        println!("\nOP {}  key={}", op["name"], k["key"]);
        let states = k["states"].as_object().unwrap();
        let mut names: Vec<&String> = states.keys().collect();
        names.sort();
        for name in names {
            let v = &states[name];
            let mut s = serde_json::to_string(v).unwrap();
            if s.len() > 90 {
                s.truncate(90);
                s.push('…');
            }
            println!("    {:<22} = {}", name, s);
        }
    }
    println!("\n=================================================================");
}

/// Confirm `deserialize_pojo_to_json` decodes ComplexRecord fully, including date/time fields.
#[test]
fn verify_tui_decodes_complexrecord() {
    let ops = load();
    let (_, ks) = find_with_state(&ops, "complex-record").unwrap();
    let r = reader(dir());
    let pi = r
        .value_pojo(&ks.op_hex, "complex-record")
        .unwrap()
        .expect("pojo_info");
    let key = ks
        .keys
        .iter()
        .find(|k| ks.raw(&k.key, "complex-record").is_some())
        .expect("a key with a complex-record")
        .key
        .clone();
    let raw = ks.raw(&key, "complex-record").unwrap();
    let json = flink_explorer::parser::pojo::deserialize_pojo_to_json(&raw, &pi, None);
    println!("\nTUI complex-record decode:\n{}", json);
}

#[test]
fn dump_full_inventory() {
    let ops = load();
    println!("\n================ OPERATOR / STATE INVENTORY ================");
    for (op, keyed) in &ops {
        let kkeys = keyed.as_ref().map_or(0, |k| k.keys.len());
        let ktype = keyed.as_ref().map(|k| k.key_type.as_str()).unwrap_or("-");
        println!(
            "\nOP '{}'  [keyed keys={} key_type={}] [operator-states={}]",
            op.display_name,
            kkeys,
            ktype,
            op.states.len()
        );
        if let Some(ks) = keyed {
            for d in &ks.states {
                println!(
                    "    keyed: {:<22} type={:<10} value={:<22} entries={}",
                    d.name,
                    d.state_type,
                    d.value_type,
                    ks.entry_count(&d.name)
                );
            }
        }
        for s in &op.states {
            println!(
                "    opstate: {:<28} type={:<18} entries={}",
                s.name,
                s.state_type,
                s.elements.len()
            );
        }
    }
    println!("\n=========================================================");
}

/// The first two entries of a MAP state, as stored: namespace (`VoidNamespace` + map key) and
/// value (`NullableSerializer` byte + value).
#[test]
fn diagnose_map_entry_bytes() {
    let ops = load();
    let (_, ks) = find_with_state(&ops, "counter").unwrap();
    for state in ["simple-string-map", "events-by-type"] {
        if ks.state(state).is_none() {
            continue;
        }
        for k in &ks.keys {
            let entries = ks.entries(&k.key, state);
            if entries.is_empty() {
                continue;
            }
            println!("\n{} key='{}' count={}", state, k.key, entries.len());
            for e in entries.iter().take(2) {
                println!(
                    "  ns({})={:02x?}  val({})={:02x?}",
                    e.namespace.len(),
                    e.namespace,
                    e.raw.len(),
                    &e.raw[..e.raw.len().min(24)]
                );
            }
            break;
        }
    }
}

#[test]
fn diagnose_list_array_bytes() {
    let ops = load();
    let (_, ks) = find_with_state(&ops, "large-list").unwrap();
    let key = &ks.keys[0].key;
    for name in ["large-list", "log-entries", "event-history"] {
        if let Some(b) = ks.raw(key, name) {
            println!(
                "{} len={} first40={:02x?}",
                name,
                b.len(),
                &b[..b.len().min(40)]
            );
        }
    }
    // String[] is in the extra-types op
    let (_, ks2) = find_with_state(&ops, "string-array").unwrap();
    if let Some(b) = ks2.raw(&ks2.keys[0].key, "string-array") {
        println!(
            "string-array len={} all={:02x?}",
            b.len(),
            &b[..b.len().min(40)]
        );
    }
}

#[test]
fn diagnose_value_classes() {
    let ops = load();
    let r = reader(dir());
    println!("\n=== value classes (package + name) per POJO state ===");
    for (_, keyed) in &ops {
        let Some(ks) = keyed else { continue };
        for d in &ks.states {
            if let Some(pi) = r.value_pojo(&ks.op_hex, &d.name).unwrap() {
                println!(
                    "  {:<18} value_type={:<14} class={}",
                    d.name, d.value_type, pi.class_name
                );
                for f in &pi.fields {
                    if let Some(nested) = &f.nested_pojo {
                        println!(
                            "      nested field {:<10} class={}",
                            f.name, nested.class_name
                        );
                    }
                }
            }
        }
    }
}
