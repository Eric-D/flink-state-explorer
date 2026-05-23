//! Coverage audit: dumps every operator and every state the explorer reads from the
//! generated savepoint, so we can verify exhaustiveness across all stateful operator types.
use std::path::Path;

use flink_explorer::index::builder::build_index;
use flink_explorer::index::cache::IndexCache;
use flink_explorer::parser;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn load_index() -> IndexCache {
    let dir = Path::new(SAVEPOINT_DIR);
    let metadata_path = dir.join("_metadata");
    assert!(
        metadata_path.exists(),
        "Savepoint _metadata not found — run `cd test-flink-job && mvn verify`"
    );
    let savepoint = parser::parse_metadata(&metadata_path).expect("parse metadata");
    let meta = std::fs::metadata(&metadata_path).unwrap();
    build_index(&savepoint, meta.len(), 0u64, dir)
}

#[test]
fn diagnose_value_types_for_all_types_operator() {
    let index = load_index();
    // The "all types" operator (has 'counter') — check value_type label vs actual bytes & pojo_info.
    let op = index
        .operators
        .iter()
        .find(|o| {
            o.keyed_state
                .as_ref()
                .map_or(false, |k| k.descriptors.iter().any(|d| d.name == "counter"))
        })
        .expect("all-types operator");
    let ks = op.keyed_state.as_ref().unwrap();
    let part = &ks.partitions[0];
    println!("\n=== value_type label vs reality (key '{}') ===", part.key);
    for (idx, d) in ks.descriptors.iter().enumerate() {
        let blen = part
            .values
            .get(idx)
            .and_then(|v| v.as_ref())
            .map(|b| b.len());
        let pojo_class = d
            .pojo_info
            .as_ref()
            .map(|p| p.class_name.rsplit('.').next().unwrap_or("").to_string());
        println!(
            "  {:<16} state={:<9} value_type={:<22} pojo={:<14?} bytes={:?}",
            d.name, d.state_type, d.value_type, pojo_class, blen
        );
    }
}

/// Verify nested POJO (ComplexRecord.address) and tuple states decode via the production
/// export path (the same code the TUI/JSON export uses).
#[test]
fn verify_tuples_and_nested_pojo() {
    let index = load_index();
    let tmp = std::env::temp_dir().join("sp_export_audit.json");
    flink_explorer::export::export_json(&index, None, &tmp).expect("export_json");
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();

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
    let index = load_index();
    let tmp = std::env::temp_dir().join("sp_export_full.json");
    flink_explorer::export::export_json(&index, None, &tmp).expect("export_json");
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();

    println!("\n============ DECODED VALUES (first key per operator) ============");
    for op in json["operators"].as_array().unwrap() {
        let keys = op["keys"].as_array().unwrap();
        if keys.is_empty() {
            continue;
        }
        // pick the first key that has any non-null state
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

fn to_pojo_info(
    c: &flink_explorer::index::cache::CachedPojoInfo,
) -> flink_explorer::parser::pojo::PojoInfo {
    use flink_explorer::parser::pojo::{PojoField, PojoInfo};
    PojoInfo {
        class_name: c.class_name.clone(),
        short_name: c
            .class_name
            .rsplit('.')
            .next()
            .unwrap_or(&c.class_name)
            .to_string(),
        fields: c
            .fields
            .iter()
            .map(|f| PojoField {
                name: f.name.clone(),
                type_name: f.type_name.clone(),
                nested_pojo: f.nested.as_ref().map(|n| Box::new(to_pojo_info(n))),
            })
            .collect(),
    }
}

/// Confirm the *TUI* decoder (deserialize_pojo_to_json) decodes ComplexRecord fully,
/// including date/time fields the JSON-export decoder misses.
#[test]
fn verify_tui_decodes_complexrecord() {
    let index = load_index();
    let op = index
        .operators
        .iter()
        .find(|o| {
            o.keyed_state.as_ref().map_or(false, |k| {
                k.descriptors.iter().any(|d| d.name == "complex-record")
            })
        })
        .unwrap();
    let ks = op.keyed_state.as_ref().unwrap();
    let idx = ks
        .descriptors
        .iter()
        .position(|d| d.name == "complex-record")
        .unwrap();
    let pi = to_pojo_info(ks.descriptors[idx].pojo_info.as_ref().expect("pojo_info"));
    let raw = ks.partitions[0].values[idx].as_ref().expect("value bytes");
    let json = flink_explorer::parser::pojo::deserialize_pojo_to_json(raw, &pi, None);
    println!("\nTUI complex-record decode:\n{}", json);
}

#[test]
fn dump_full_inventory() {
    let index = load_index();
    println!("\n================ OPERATOR / STATE INVENTORY ================");
    for op in &index.operators {
        let kkeys = op.keyed_state.as_ref().map_or(0, |k| k.total_keys);
        let ktype = op
            .keyed_state
            .as_ref()
            .map(|k| k.key_type.as_str())
            .unwrap_or("-");
        println!(
            "\nOP '{}'  [keyed keys={} key_type={}] [operator-states={}]",
            op.display_name,
            kkeys,
            ktype,
            op.states.len()
        );
        if let Some(ks) = &op.keyed_state {
            for d in &ks.descriptors {
                println!(
                    "    keyed: {:<22} type={:<10} value={:<22} entries={}",
                    d.name, d.state_type, d.value_type, d.entry_count
                );
            }
        }
        for s in &op.states {
            println!(
                "    opstate: {:<28} type={:<18} entries={}",
                s.name,
                s.state_type,
                s.entries.len()
            );
        }
    }
    println!("\n=========================================================");
}

#[test]
fn diagnose_map_blob_bytes() {
    let index = load_index();
    for (state, opkey) in [
        ("simple-string-map", "counter"),
        ("events-by-type", "counter"),
    ] {
        let op = index
            .operators
            .iter()
            .find(|o| {
                o.keyed_state
                    .as_ref()
                    .map_or(false, |k| k.descriptors.iter().any(|d| d.name == opkey))
            })
            .unwrap();
        let ks = op.keyed_state.as_ref().unwrap();
        let idx = match ks.descriptors.iter().position(|d| d.name == state) {
            Some(i) => i,
            None => continue,
        };
        for part in &ks.partitions {
            if let Some(Some(blob)) = part.values.get(idx) {
                if blob.len() < 4 {
                    continue;
                }
                let count = i32::from_be_bytes(blob[0..4].try_into().unwrap());
                println!(
                    "\n{} key='{}' count={} blobLen={}",
                    state,
                    part.key,
                    count,
                    blob.len()
                );
                let mut p = 4usize;
                for _ in 0..count.min(2) {
                    let nl = i32::from_be_bytes(blob[p..p + 4].try_into().unwrap()) as usize;
                    p += 4;
                    let ns = &blob[p..p + nl];
                    p += nl;
                    let vl = i32::from_be_bytes(blob[p..p + 4].try_into().unwrap()) as usize;
                    p += 4;
                    let v = &blob[p..p + vl];
                    p += vl;
                    println!(
                        "  ns({})={:02x?}  val({})={:02x?}",
                        nl,
                        ns,
                        vl,
                        &v[..vl.min(24)]
                    );
                }
                break;
            }
        }
    }
}

#[test]
fn diagnose_list_array_bytes() {
    let index = load_index();
    let op = index
        .operators
        .iter()
        .find(|o| {
            o.keyed_state.as_ref().map_or(false, |k| {
                k.descriptors.iter().any(|d| d.name == "large-list")
            })
        })
        .unwrap();
    let ks = op.keyed_state.as_ref().unwrap();
    for name in ["large-list", "log-entries", "event-history"] {
        if let Some(idx) = ks.descriptors.iter().position(|d| d.name == name) {
            if let Some(Some(b)) = ks.partitions[0].values.get(idx) {
                println!(
                    "{} len={} first40={:02x?}",
                    name,
                    b.len(),
                    &b[..b.len().min(40)]
                );
            }
        }
    }
    // String[] is in the extra-types op
    let op2 = index
        .operators
        .iter()
        .find(|o| {
            o.keyed_state.as_ref().map_or(false, |k| {
                k.descriptors.iter().any(|d| d.name == "string-array")
            })
        })
        .unwrap();
    let ks2 = op2.keyed_state.as_ref().unwrap();
    if let Some(idx) = ks2
        .descriptors
        .iter()
        .position(|d| d.name == "string-array")
    {
        if let Some(Some(b)) = ks2.partitions[0].values.get(idx) {
            println!(
                "string-array len={} all={:02x?}",
                b.len(),
                &b[..b.len().min(40)]
            );
        }
    }
}

#[test]
fn diagnose_value_classes() {
    let index = load_index();
    println!("\n=== value classes (package + name) per POJO state ===");
    for op in &index.operators {
        if let Some(ks) = &op.keyed_state {
            for d in &ks.descriptors {
                if let Some(pi) = &d.pojo_info {
                    println!(
                        "  {:<18} value_type={:<14} class={}",
                        d.name, d.value_type, pi.class_name
                    );
                    for f in &pi.fields {
                        if let Some(nested) = &f.nested {
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
}
