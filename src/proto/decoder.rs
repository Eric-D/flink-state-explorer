use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage, Value};

use crate::error::ProtoError;

/// Decode raw bytes using a named protobuf message type from the pool.
/// Returns a compact JSON-like string.
pub fn decode_message(
    pool: &DescriptorPool,
    message_type: &str,
    bytes: &[u8],
) -> Result<String, ProtoError> {
    let descriptor = pool
        .get_message_by_name(message_type)
        .ok_or_else(|| ProtoError::UnknownType(message_type.to_string()))?;

    let message =
        DynamicMessage::decode(descriptor, bytes).map_err(|e| ProtoError::Decode(e.to_string()))?;

    Ok(format_message_json(&message))
}

/// Decode raw bytes and return pretty-printed JSON (one field per line).
pub fn decode_message_pretty(
    pool: &DescriptorPool,
    message_type: &str,
    bytes: &[u8],
    indent: usize,
) -> Result<Vec<(String, String)>, ProtoError> {
    let descriptor = pool
        .get_message_by_name(message_type)
        .ok_or_else(|| ProtoError::UnknownType(message_type.to_string()))?;

    let message =
        DynamicMessage::decode(descriptor, bytes).map_err(|e| ProtoError::Decode(e.to_string()))?;

    Ok(flatten_message(&message, "", indent))
}

/// Flatten a proto message into (name, value) pairs for display.
fn flatten_message(msg: &DynamicMessage, prefix: &str, max_depth: usize) -> Vec<(String, String)> {
    let descriptor = msg.descriptor();
    let mut fields = Vec::new();

    for field in descriptor.fields() {
        if msg.has_field(&field) {
            let name = if prefix.is_empty() {
                field.name().to_string()
            } else {
                format!("{}.{}", prefix, field.name())
            };
            let value = msg.get_field(&field);
            match &*value {
                Value::Message(m) if max_depth > 0 => {
                    fields.extend(flatten_message(m, &name, max_depth - 1));
                }
                _ => {
                    fields.push((name, format_value_json(&value, &field)));
                }
            }
        }
    }
    fields
}

fn format_message_json(msg: &DynamicMessage) -> String {
    let descriptor = msg.descriptor();
    let mut fields = Vec::new();

    for field in descriptor.fields() {
        if msg.has_field(&field) {
            let value = msg.get_field(&field);
            let formatted = format_value_json(&value, &field);
            fields.push(format!("{}: {}", field.name(), formatted));
        }
    }

    if fields.is_empty() {
        "{}".to_string()
    } else {
        format!("{{ {} }}", fields.join(", "))
    }
}

fn format_value_json(value: &Value, field: &prost_reflect::FieldDescriptor) -> String {
    match value {
        Value::Bool(b) => b.to_string(),
        Value::I32(n) => n.to_string(),
        Value::I64(n) => n.to_string(),
        Value::U32(n) => n.to_string(),
        Value::U64(n) => n.to_string(),
        Value::F32(f) => format!("{}", f),
        Value::F64(f) => format!("{}", f),
        Value::String(s) => format!("\"{}\"", s),
        Value::Bytes(b) => {
            if b.len() <= 64 {
                let hex: String = b
                    .iter()
                    .map(|x| format!("{:02x}", x))
                    .collect::<Vec<_>>()
                    .join("");
                format!("\"{}\"", hex)
            } else {
                format!("[{} bytes]", b.len())
            }
        }
        Value::EnumNumber(n) => {
            // Resolve enum name from descriptor
            if let Some(enum_desc) = field.kind().as_enum() {
                enum_desc
                    .get_value(*n)
                    .map(|v| v.name().to_string())
                    .unwrap_or_else(|| format!("{}", n))
            } else {
                format!("{}", n)
            }
        }
        Value::Message(m) => format_message_json(m),
        Value::List(list) => {
            let items: Vec<String> = list.iter().map(|v| format_value_json(v, field)).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Map(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        prost_reflect::MapKey::Bool(b) => b.to_string(),
                        prost_reflect::MapKey::I32(n) => n.to_string(),
                        prost_reflect::MapKey::I64(n) => n.to_string(),
                        prost_reflect::MapKey::U32(n) => n.to_string(),
                        prost_reflect::MapKey::U64(n) => n.to_string(),
                        prost_reflect::MapKey::String(s) => format!("\"{}\"", s),
                    };
                    format!("{}: {}", key, format_value_json(v, field))
                })
                .collect();
            format!("{{ {} }}", entries.join(", "))
        }
    }
}
