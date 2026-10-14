use std::path::Path;

use serde_json::{json, Map, Value};

use crate::index::cache::{CachedPojoInfo, IndexCache};
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
                                        let pi = desc.pojo_info.as_ref().map(CachedPojoInfo::to_pojo_info);
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

            // The index keeps at most `MAX_STORED_PARTITIONS` keys per operator, so `keys` may
            // hold fewer than the savepoint has: say so rather than look complete.
            let (total_keys, stored_keys) = op
                .keyed_state
                .as_ref()
                .map_or((0, 0), |ks| (ks.total_keys, ks.stored_keys()));
            json!({
                "name": op.display_name,
                "info": Value::Object(info),
                // Every state file that could not be read (the `info` line shows only the first
                // few): non-empty means `keys` is partial.
                "load_errors": op.load_errors,
                "parallelism": op.parallelism,
                "operator_states": operator_states,
                "state_descriptors": descriptors,
                "total_keys": total_keys,
                "stored_keys": stored_keys,
                "truncated": stored_keys < total_keys,
                "keys": keys
            })
        })
        .collect();

    let truncated = index
        .operators
        .iter()
        .any(|op| op.keyed_state.as_ref().is_some_and(|ks| ks.is_truncated()));
    json!({
        "checkpoint_id": index.checkpoint_id,
        // True when any operator's `keys` is truncated (see its `truncated`); `--export-state`
        // streams every key of a state instead.
        "truncated": truncated,
        "operators": operators
    })
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
