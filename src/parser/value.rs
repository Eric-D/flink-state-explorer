//! Unified value decoder — the single source of truth for turning a state value's raw bytes into
//! a structured [`serde_json::Value`]. Both the JSON export and the TUI render this `Value`; neither
//! contains its own binary-decoding logic. To support a new type, add it here once.
//!
//! Kryo-serialized fields are intentionally **not** decoded: they are marked `"[Kryo]"`. Because a
//! Kryo blob has unknown length, any field that can't be decoded stops decoding of the remaining
//! sibling fields (they become `null`).

use serde_json::{json, Map, Value};

use crate::parser::java_deser::JavaReader;
use crate::parser::pojo::{millis_to_iso, PojoInfo};
use crate::proto::ProtoContext;

/// Decode a complete state value (the bytes stored for one `(key, state)` slot).
///
/// * `pojo_info` is `Some` when the value serializer is a `PojoSerializer`.
/// * `value_type` is the descriptor's value type (e.g. `"Long"`, `"List<String>"`, `"Map<String,Pojo>"`).
pub fn decode_state_value(
    bytes: &[u8],
    value_type: &str,
    pojo_info: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Value {
    if bytes.is_empty() {
        return Value::Null;
    }
    // MAP values are stored as the index blob, not raw Flink bytes — decode them specially.
    if value_type.starts_with("Map<") {
        return decode_map_blob(bytes, value_type, pojo_info, proto);
    }
    let mut r = JavaReader::new(bytes);
    // TTL-wrapped value: TtlValue = writeLong(lastAccessTimestamp) + userValue.
    if let Some(inner) = value_type
        .strip_prefix("Ttl<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let ts = r.read_long().ok();
        let user_value = match pojo_info {
            Some(info) => decode_pojo(&mut r, info, proto),
            None => decode_field(&mut r, inner, None, proto).unwrap_or(Value::Null),
        };
        return json!({
            "_ttlLastAccess": ts.map(millis_to_iso),
            "value": user_value,
        });
    }
    // A keyed ListState value is delimiter-separated (ListDelimitedSerializer), unlike a List
    // *field* inside a POJO (which is length-prefixed and handled in decode_field).
    if let Some(inner) = value_type
        .strip_prefix("List<")
        .or_else(|| value_type.strip_prefix("Array<"))
    {
        let elem_type = inner.strip_suffix('>').unwrap_or(inner);
        let elem_pojo = if elem_type.contains("Pojo") {
            pojo_info
        } else {
            None
        };
        return decode_list(&mut r, elem_type, elem_pojo, proto);
    }
    // A bare POJO value (not List<Pojo>/Map<...,Pojo>, which carry pojo_info for their elements).
    if let Some(info) = pojo_info {
        if !value_type.contains('<') {
            return decode_pojo(&mut r, info, proto);
        }
    }
    decode_field(&mut r, value_type, None, proto).unwrap_or(Value::Null)
}

/// Decode a keyed ListState value: elements serialized back-to-back and separated by the
/// `ListDelimitedSerializer` delimiter byte (`,` = 0x2c). There is no length prefix.
fn decode_list(
    r: &mut JavaReader<&[u8]>,
    elem_type: &str,
    elem_pojo: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Value {
    // Flink `ListDelimitedSerializer.DELIMITER` — ',' separates list-state elements.
    const DELIM: u8 = b',';
    let mut items = Vec::new();
    loop {
        let v = match elem_pojo {
            Some(pi) => decode_pojo(r, pi, proto),
            None => match decode_field(r, elem_type, None, proto) {
                Some(v) => v,
                None => break,
            },
        };
        items.push(v);
        if items.len() > 1_000_000 {
            break;
        }
        // Between elements there is exactly one delimiter byte; EOF (or anything else) ends the list.
        match r.read_byte() {
            Ok(DELIM) => continue,
            _ => break,
        }
    }
    Value::Array(items)
}

/// Read a POJO value: the `PojoSerializer` flags byte, then each field. Public so the TUI's
/// string-based path (`parser::pojo`) can reuse this single decoder.
pub fn decode_pojo(
    r: &mut JavaReader<&[u8]>,
    info: &PojoInfo,
    proto: Option<&ProtoContext>,
) -> Value {
    let flags = match r.read_byte() {
        Ok(f) => f,
        Err(_) => return Value::Null,
    };
    if flags & 0x01 != 0 {
        return Value::Null;
    }
    // 0x00 is a NullableSerializer isNull=false byte; the real PojoSerializer flags follow.
    let flags = if flags == 0x00 {
        r.read_byte().unwrap_or(0x02)
    } else {
        flags
    };
    if flags & 0x01 != 0 {
        return Value::Null;
    }
    if flags & 0x08 != 0 {
        let _ = r.read_int(); // subclass tag
    } else if flags & 0x04 != 0 {
        let _ = r.read_utf(); // subclass name
    }
    decode_pojo_fields(r, info, proto)
}

/// Render a decoded [`Value`] as the compact display string used by the TUI.
pub fn value_to_display(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(value_to_display).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, value_to_display(v)))
                .collect();
            format!("{{ {} }}", parts.join(", "))
        }
    }
}

fn decode_pojo_fields(
    r: &mut JavaReader<&[u8]>,
    info: &PojoInfo,
    proto: Option<&ProtoContext>,
) -> Value {
    let mut obj = Map::new();
    let mut proto_class: Option<String> = None;
    let mut proto_bytes: Option<Vec<u8>> = None;
    let mut ok = true;

    for field in &info.fields {
        if !ok {
            // A previous field could not be decoded (e.g. Kryo) — its length is unknown, so the
            // reader position is no longer reliable; mark the rest as null.
            obj.insert(field.name.clone(), Value::Null);
            continue;
        }
        let is_null = match r.read_boolean() {
            Ok(b) => b,
            Err(_) => {
                ok = false;
                obj.insert(field.name.clone(), Value::Null);
                continue;
            }
        };
        if is_null {
            obj.insert(field.name.clone(), Value::Null);
            continue;
        }

        // Nested POJO: optional Nullable wrapper byte, then a flags byte, then recurse.
        if let Some(nested) = &field.nested_pojo {
            if field.type_name.starts_with("Nullable<") {
                match r.read_boolean() {
                    Ok(true) => {
                        obj.insert(field.name.clone(), Value::Null);
                        continue;
                    }
                    Ok(false) => {}
                    Err(_) => {
                        ok = false;
                        obj.insert(field.name.clone(), Value::Null);
                        continue;
                    }
                }
            }
            let flags = match r.read_byte() {
                Ok(f) => f,
                Err(_) => {
                    ok = false;
                    obj.insert(field.name.clone(), Value::Null);
                    continue;
                }
            };
            // 0x00 may be a stray NullableSerializer isNull=false byte; re-read as flags.
            let flags = if flags == 0x00 && !field.type_name.starts_with("Nullable<") {
                r.read_byte().unwrap_or(0x02)
            } else {
                flags
            };
            if flags & 0x01 != 0 {
                obj.insert(field.name.clone(), Value::Null);
                continue;
            }
            if flags & 0x08 != 0 {
                let _ = r.read_int();
            } else if flags & 0x04 != 0 {
                let _ = r.read_utf();
            }
            obj.insert(field.name.clone(), decode_pojo_fields(r, nested, proto));
            continue;
        }

        // Raw protobuf-bytes field (captured for decoding once we also have the class-name field).
        let is_proto_bytes = field.type_name == "byte[]"
            && proto.map_or(false, |c| {
                c.patterns.iter().any(|p| p.bytes_field == field.name)
            });
        if is_proto_bytes {
            match read_len_bytes(r) {
                Some(b) => {
                    obj.insert(field.name.clone(), json!(format!("[{} bytes]", b.len())));
                    proto_bytes = Some(b);
                }
                None => {
                    ok = false;
                    obj.insert(field.name.clone(), Value::Null);
                }
            }
            continue;
        }

        match decode_field(r, &field.type_name, field.nested_pojo.as_deref(), proto) {
            Some(v) => {
                if proto.map_or(false, |c| {
                    c.patterns.iter().any(|p| p.class_field == field.name)
                }) {
                    if let Value::String(s) = &v {
                        proto_class = Some(s.clone());
                    }
                }
                obj.insert(field.name.clone(), v);
            }
            None => {
                // Undecodable — mark Kryo explicitly, otherwise generic; stop (length unknown).
                let marker = if field.type_name.contains("Kryo") {
                    "[Kryo]".to_string()
                } else {
                    format!("[undecodable: {}]", field.type_name)
                };
                obj.insert(field.name.clone(), Value::String(marker));
                ok = false;
            }
        }
    }

    // Protobuf pattern: decode the captured bytes against the captured class name.
    if let (Some(ctx), Some(cn), Some(bytes)) = (proto, &proto_class, &proto_bytes) {
        let resolve = ctx
            .patterns
            .first()
            .map(|p| p.resolve.as_str())
            .unwrap_or("auto");
        if let Some(decoded) = ctx.decode(cn, bytes, resolve, None) {
            let bytes_field = ctx
                .patterns
                .first()
                .map(|p| p.bytes_field.as_str())
                .unwrap_or("protoBytes");
            obj.insert(bytes_field.to_string(), Value::String(decoded));
        }
    }

    Value::Object(obj)
}

/// Decode a single field/value. Returns `None` when the type is unknown or Kryo (length unknown).
pub fn decode_field(
    r: &mut JavaReader<&[u8]>,
    type_name: &str,
    nested: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Option<Value> {
    if let Some(info) = nested {
        // The PojoSerializer flags byte was already consumed by the caller for nested fields;
        // here (list elements) we read a fresh POJO value including its flags byte.
        return Some(decode_pojo(r, info, proto));
    }
    match type_name {
        "Int" => r.read_int().ok().map(|v| json!(v)),
        "Long" => r.read_long().ok().map(|v| json!(v)),
        "Boolean" => r.read_boolean().ok().map(|v| json!(v)),
        "Double" => r.read_long().ok().map(|v| json!(f64::from_bits(v as u64))),
        "Float" => r.read_int().ok().map(|v| json!(f32::from_bits(v as u32))),
        "Short" => r.read_short().ok().map(|v| json!(v)),
        "Byte" => r.read_byte().ok().map(|v| json!(v as i8)),
        "String" => r.read_string_value().ok().map(Value::String),
        "byte[]" => read_len_bytes(r).map(|b| bytes_to_value(&b)),
        // Flink LocalDateTimeSerializer: int(year) + byte(month) + byte(day) + byte(hour)
        // + byte(minute) + byte(second) + int(nano)  → 13 bytes.
        "LocalDateTime" => {
            let y = r.read_int().ok()?;
            let mo = r.read_byte().ok()?;
            let d = r.read_byte().ok()?;
            let h = r.read_byte().ok()?;
            let mi = r.read_byte().ok()?;
            let s = r.read_byte().ok()?;
            let n = r.read_int().ok()?;
            Some(Value::String(if n == 0 {
                format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, mo, d, h, mi, s)
            } else {
                format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}",
                    y,
                    mo,
                    d,
                    h,
                    mi,
                    s,
                    n / 1_000_000
                )
            }))
        }
        // Flink LocalDateSerializer: int(year) + byte(month) + byte(day)  → 6 bytes.
        "LocalDate" => {
            let y = r.read_int().ok()?;
            let mo = r.read_byte().ok()?;
            let d = r.read_byte().ok()?;
            Some(Value::String(format!("{:04}-{:02}-{:02}", y, mo, d)))
        }
        // Flink LocalTimeSerializer: byte(hour) + byte(minute) + byte(second) + int(nano)  → 7 bytes.
        "LocalTime" => {
            let h = r.read_byte().ok()?;
            let mi = r.read_byte().ok()?;
            let s = r.read_byte().ok()?;
            let n = r.read_int().ok()?;
            Some(Value::String(if n == 0 {
                format!("{:02}:{:02}:{:02}", h, mi, s)
            } else {
                format!("{:02}:{:02}:{:02}.{:03}", h, mi, s, n / 1_000_000)
            }))
        }
        "Instant" => {
            let epoch_sec = r.read_long().ok()?;
            let nano = r.read_int().ok()?;
            Some(Value::String(millis_to_iso(
                epoch_sec * 1000 + (nano / 1_000_000) as i64,
            )))
        }
        "Date" => r.read_long().ok().map(|m| Value::String(millis_to_iso(m))),
        "Timestamp" => {
            let millis = r.read_long().ok()?;
            let _nanos = r.read_int().ok()?;
            Some(Value::String(millis_to_iso(millis)))
        }
        // Bare "Enum" (e.g. a Map key serializer without captured constants): show the ordinal.
        "Enum" => r
            .read_int()
            .ok()
            .map(|o| Value::String(format!("ordinal({})", o))),
        t if t.starts_with("Enum<") => {
            let ordinal = r.read_int().ok()?;
            let name = t
                .strip_prefix("Enum<")
                .and_then(|s| s.strip_suffix('>'))
                .and_then(|s| s.split_once(':'))
                .map(|(_, cs)| cs)
                .and_then(|cs| cs.split(',').nth(ordinal as usize));
            Some(match name {
                Some(n) => Value::String(n.to_string()),
                None => json!(ordinal),
            })
        }
        "String[]" => {
            // StringArraySerializer: int(length) + StringValue per element (var-int length).
            let len = r.read_int().ok()?;
            if !(0..=100_000).contains(&len) {
                return None;
            }
            let mut items = Vec::new();
            for _ in 0..len {
                match r.read_string_value() {
                    Ok(s) => items.push(Value::String(s)),
                    Err(_) => break,
                }
            }
            Some(Value::Array(items))
        }
        "int[]" => read_prim_array(r, |r| r.read_int().ok().map(|v| json!(v))),
        "long[]" => read_prim_array(r, |r| r.read_long().ok().map(|v| json!(v))),
        "double[]" => read_prim_array(r, |r| {
            r.read_long().ok().map(|v| json!(f64::from_bits(v as u64)))
        }),
        "float[]" => read_prim_array(r, |r| {
            r.read_int().ok().map(|v| json!(f32::from_bits(v as u32)))
        }),
        "short[]" => read_prim_array(r, |r| r.read_short().ok().map(|v| json!(v))),
        "boolean[]" => read_prim_array(r, |r| r.read_boolean().ok().map(|v| json!(v))),
        t if t.starts_with("Tuple<") => {
            let inner = t.strip_prefix("Tuple<")?.strip_suffix('>')?;
            let mut items = Vec::new();
            for elem_type in split_top_level(inner) {
                match decode_field(r, elem_type.trim(), None, proto) {
                    Some(v) => items.push(v),
                    None => {
                        items.push(Value::Null);
                        break;
                    }
                }
            }
            Some(Value::Array(items))
        }
        t if t.starts_with("List<") || t.starts_with("Array<") => {
            let size = r.read_int().ok()?;
            if !(0..=1_000_000).contains(&size) {
                return None;
            }
            let elem_type = t
                .strip_prefix("List<")
                .or_else(|| t.strip_prefix("Array<"))
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or("?");
            if elem_type == "?" && nested.is_none() && size > 0 {
                return None;
            }
            let is_array = t.starts_with("Array<");
            let mut items = Vec::new();
            for _ in 0..size {
                if is_array && !r.read_boolean().unwrap_or(false) {
                    items.push(Value::Null);
                    continue;
                }
                match decode_field(r, elem_type, None, proto) {
                    Some(v) => items.push(v),
                    None => break,
                }
            }
            Some(Value::Array(items))
        }
        t if t.starts_with("Nullable<") => {
            if r.read_boolean().ok()? {
                return Some(Value::Null);
            }
            let inner = t
                .strip_prefix("Nullable<")
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or("?");
            decode_field(r, inner, nested, proto)
        }
        // Kryo and unknown serializers: cannot decode (and length is unknown → caller stops).
        _ => None,
    }
}

/// Decode a MAP value from the index blob: `int(count) + [int nsLen, ns, int valLen, val]*`.
fn decode_map_blob(
    bytes: &[u8],
    value_type: &str,
    pojo_info: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Value {
    let (key_type, val_type) = value_type
        .strip_prefix("Map<")
        .and_then(|s| s.strip_suffix('>'))
        .and_then(|s| {
            split_top_level(s)
                .into_iter()
                .collect::<Vec<_>>()
                .split_first()
                .map(|(k, rest)| (k.trim().to_string(), rest.join(",").trim().to_string()))
        })
        .unwrap_or_else(|| ("?".to_string(), "?".to_string()));

    let mut r = JavaReader::new(bytes);
    let count = match r.read_int() {
        Ok(c) if (0..=1_000_000).contains(&c) => c,
        _ => return Value::Null,
    };
    let mut obj = Map::new();
    for i in 0..count {
        let ns = match read_len_bytes(&mut r) {
            Some(b) => b,
            None => break,
        };
        let val = match read_len_bytes(&mut r) {
            Some(b) => b,
            None => break,
        };
        // Key bytes are `VoidNamespace(1 byte) + serialized map key` — skip the namespace byte.
        let key_payload: &[u8] = if ns.is_empty() { &ns } else { &ns[1..] };
        let key_str = {
            let mut kr = JavaReader::new(key_payload);
            match decode_field(&mut kr, &key_type, None, proto) {
                Some(Value::String(s)) => s,
                Some(v) => v.to_string().trim_matches('"').to_string(),
                None => format!("key{}", i),
            }
        };
        // Value bytes are `NullableSerializer(isNull byte) + serialized value`.
        let value = match val.first() {
            Some(1) => Value::Null,
            Some(_) => decode_state_value(&val[1..], &val_type, pojo_info, proto),
            None => Value::Null,
        };
        obj.insert(key_str, value);
    }
    Value::Object(obj)
}

// --- small helpers ----------------------------------------------------------------------------

fn read_len_bytes(r: &mut JavaReader<&[u8]>) -> Option<Vec<u8>> {
    let len = r.read_int().ok()?;
    if !(0..=10_000_000).contains(&len) {
        return None;
    }
    r.read_bytes(len as usize).ok()
}

fn read_prim_array<F>(r: &mut JavaReader<&[u8]>, mut read_one: F) -> Option<Value>
where
    F: FnMut(&mut JavaReader<&[u8]>) -> Option<Value>,
{
    let len = r.read_int().ok()?;
    if !(0..=1_000_000).contains(&len) {
        return None;
    }
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        match read_one(r) {
            Some(v) => items.push(v),
            None => return None,
        }
    }
    Some(Value::Array(items))
}

fn bytes_to_value(b: &[u8]) -> Value {
    if b.len() <= 200 {
        if let Ok(s) = std::str::from_utf8(b) {
            if s.chars().all(|c| !c.is_control() || c == '\n') {
                return Value::String(s.to_string());
            }
        }
    }
    Value::String(format!("[{} bytes]", b.len()))
}

/// Split a comma-separated generic argument list at the top level (ignoring nested `<...>`).
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}
