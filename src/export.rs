use std::path::Path;

use serde_json::{json, Map, Value};

use crate::index::cache::{CachedPojoInfo, IndexCache};
use crate::parser::pojo::{PojoField, PojoInfo};
use crate::proto::ProtoContext;

/// Export the full savepoint index as JSON following the UI hierarchy.
pub fn export_json(
    index: &IndexCache,
    proto_ctx: Option<&ProtoContext>,
    output_path: &Path,
) -> Result<usize, String> {
    let root = build_json(index, proto_ctx);
    let json_str = serde_json::to_string_pretty(&root)
        .map_err(|e| format!("JSON serialization error: {}", e))?;
    let count = count_keys(&root);
    std::fs::write(output_path, &json_str).map_err(|e| format!("Write error: {}", e))?;
    Ok(count)
}

fn build_json(index: &IndexCache, proto_ctx: Option<&ProtoContext>) -> Value {
    // Include EVERY operator (not just keyed ones) with its full metadata, its operator states
    // (name + distribution mode + decoded element values), and its keyed state.
    let operators: Vec<Value> = index
        .operators
        .iter()
        .map(|op| {
            let info: Map<String, Value> = op
                .info_lines()
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();

            let operator_states: Vec<Value> = op
                .states
                .iter()
                .map(|s| {
                    let values: Vec<Value> = s
                        .entries
                        .iter()
                        .map(|e| Value::String(String::from_utf8_lossy(&e.key).into_owned()))
                        .collect();
                    json!({ "name": s.name, "mode": s.state_type, "values": values })
                })
                .collect();

            let (descriptors, keys) = match &op.keyed_state {
                Some(ks) => {
                    let descs: Vec<Value> = ks
                        .descriptors
                        .iter()
                        .map(|d| {
                            json!({
                                "name": d.name,
                                "state_type": d.state_type,
                                "value_type": d.value_type,
                                // Fully-qualified value class (package + name) when the value is a
                                // POJO/Enum — read from the serializer snapshot.
                                "value_class": d.pojo_info.as_ref().map(|p| p.class_name.clone()),
                            })
                        })
                        .collect();
                    let keys: Vec<Value> = ks
                        .partitions
                        .iter()
                        .map(|part| {
                            let mut states = Map::new();
                            for (i, desc) in ks.descriptors.iter().enumerate() {
                                // All binary decoding lives in parser::value (single source shared
                                // with the TUI). Export only serializes the resulting Value.
                                let raw = part.values.get(i).and_then(|v| v.as_ref());
                                let value = match raw {
                                    Some(bytes) if !bytes.is_empty() => {
                                        let pi = desc.pojo_info.as_ref().map(to_pojo_info);
                                        crate::parser::value::decode_state_value(
                                            bytes,
                                            &desc.value_type,
                                            pi.as_ref(),
                                            proto_ctx,
                                        )
                                    }
                                    _ => Value::Null,
                                };
                                states.insert(desc.name.clone(), value);
                            }
                            json!({ "key": part.key, "key_group": part.key_group, "states": states })
                        })
                        .collect();
                    (descs, keys)
                }
                None => (Vec::new(), Vec::new()),
            };

            json!({
                "name": op.display_name,
                "info": Value::Object(info),
                "parallelism": op.parallelism,
                "operator_states": operator_states,
                "state_descriptors": descriptors,
                "keys": keys
            })
        })
        .collect();

    json!({
        "checkpoint_id": index.checkpoint_id,
        "operators": operators
    })
}

/// Convert a cached POJO descriptor into the parser's `PojoInfo` for decoding.
fn to_pojo_info(c: &CachedPojoInfo) -> PojoInfo {
    PojoInfo {
        class_name: c.class_name.clone(),
        short_name: c.class_name.rsplit('.').next().unwrap_or("").to_string(),
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

fn count_keys(val: &Value) -> usize {
    match val {
        Value::Object(obj) => {
            if let Some(Value::Array(ops)) = obj.get("operators") {
                ops.iter()
                    .filter_map(|op| op.get("keys").and_then(|k| k.as_array()).map(|a| a.len()))
                    .sum()
            } else {
                0
            }
        }
        _ => 0,
    }
}
