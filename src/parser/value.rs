//! Unified value decoder — the single source of truth for turning a state value's raw bytes into
//! a structured [`serde_json::Value`]. Both the JSON export and the TUI render this `Value`; neither
//! contains its own binary-decoding logic. To support a new type, add it here once.
//!
//! ## `null` and the markers
//!
//! `null` is only ever a value Flink stored as null: a null POJO field or POJO (`PojoSerializer`),
//! a `NullableSerializer` null, a null `StringValue` (length 0), a null `GenericArraySerializer`
//! element. A value this decoder could not read is never `null`; it is one of these strings, which
//! [`value_to_display`] prints without quotes so the TUI keeps them apart from string data:
//!
//! - `"[Kryo]"`: a Kryo-serialized value. Not decoded, by design.
//! - `"[undecodable: <type>]"`: a serializer this decoder does not handle (`<type>` is the type
//!   read from the serializer snapshot, e.g. `Map<String,Int>`, `subclass a.B of A`), or bytes
//!   that end before the value does.
//! - `"[not decoded: follows <name>]"`: a POJO field, or a tuple field `f<i>`, that comes after
//!   one that could not be fully decoded. Flink writes no length in front of a field, so once one
//!   value's length is unknown nothing after it can be located.
//! - `"[not decoded: <n> more elements]"`: the rest of a length-prefixed list or array whose
//!   decoding stopped at an element; `"[not decoded: the rest of the list]"` for a keyed
//!   `ListState`, whose elements are delimited, not counted.
//!
//! A value that could only be partly decoded keeps what was read, with the marker where decoding
//! stopped (see [`Decoded`]). A marker sits where a value of any type would, so a reader can
//! usually tell it from data by its type alone (a string where an `Int` was due); only a decoded
//! String whose whole content is a marker's text cannot be told apart from one.

use serde_json::{json, Map, Value};

use crate::parser::java_deser::JavaReader;
use crate::parser::pojo::{millis_to_iso, PojoInfo};
use crate::proto::ProtoContext;

/// The result of decoding one value.
///
/// `Ok` is fully decoded, and the reader sits right after the value. `Err` holds what could be
/// decoded, with a marker where decoding stopped: the value's length is then unknown, so the
/// reader position is unreliable and nothing after the value may be read.
pub type Decoded = Result<Value, Value>;

/// The value of a [`Decoded`], whether or not it was fully decoded.
pub fn value_of(decoded: Decoded) -> Value {
    decoded.unwrap_or_else(|partial| partial)
}

/// The marker for a Kryo-serialized value.
pub const KRYO_MARKER: &str = "[Kryo]";

/// The marker for a value of serializer type `type_name` that could not be decoded.
pub fn undecodable(type_name: &str) -> Value {
    if type_name.contains("Kryo") {
        Value::String(KRYO_MARKER.to_string())
    } else {
        Value::String(format!("[undecodable: {}]", type_name))
    }
}

/// The marker for a field that comes after `name`, which could not be fully decoded.
pub fn not_decoded_after(name: &str) -> Value {
    Value::String(format!("[not decoded: follows {}]", name))
}

/// Whether `s` is one of the markers of this module rather than decoded data.
pub fn is_marker(s: &str) -> bool {
    s == KRYO_MARKER
        || (s.ends_with(']')
            && (s.starts_with("[undecodable: ") || s.starts_with("[not decoded: ")))
}

// `PojoSerializer` flags (flink-core `api/java/typeutils/runtime/PojoSerializer.java:50-53`,
// release-1.20). `NO_SUBCLASS` (2) is the only one under which the fields follow.
const POJO_IS_NULL: u8 = 1;
const POJO_IS_SUBCLASS: u8 = 4;
const POJO_IS_TAGGED_SUBCLASS: u8 = 8;

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
        let Ok(ts) = r.read_long() else {
            return undecodable(value_type);
        };
        let user_value = match pojo_info {
            Some(info) => decode_pojo(&mut r, info, proto),
            None => decode_field(&mut r, inner, None, proto),
        };
        return json!({
            "_ttlLastAccess": millis_to_iso(ts),
            "value": value_of(user_value),
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
            return value_of(decode_pojo(&mut r, info, proto));
        }
    }
    value_of(decode_field(&mut r, value_type, None, proto))
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
        let element = match elem_pojo {
            Some(pi) => decode_pojo(r, pi, proto),
            None => decode_field(r, elem_type, None, proto),
        };
        match element {
            Ok(v) => items.push(v),
            Err(partial) => {
                // The next delimiter can't be found once an element's length is unknown: the
                // element's marker ends the list. Say so when bytes remain, or a list of three
                // Kryo elements would read as a list of one.
                items.push(partial);
                if r.remaining() > 0 {
                    items.push(Value::String(
                        "[not decoded: the rest of the list]".to_string(),
                    ));
                }
                break;
            }
        }
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

/// Read a POJO value: the `PojoSerializer` flags byte, then each field. Public so that
/// `parser::pojo`'s string helpers and `decode_key` reuse this single decoder.
pub fn decode_pojo(
    r: &mut JavaReader<&[u8]>,
    info: &PojoInfo,
    proto: Option<&ProtoContext>,
) -> Decoded {
    let Ok(flags) = r.read_byte() else {
        return Err(undecodable(&info.short_name));
    };
    // 0x00 is a NullableSerializer isNull=false byte; the real PojoSerializer flags follow
    // (PojoSerializer itself never writes 0).
    let flags = if flags == 0x00 {
        match r.read_byte() {
            Ok(f) => f,
            Err(_) => return Err(undecodable(&info.short_name)),
        }
    } else {
        flags
    };
    decode_pojo_body(r, flags, info, proto)
}

/// The rest of a POJO once its flags byte is read (`PojoSerializer.serialize`, flink-core
/// `api/java/typeutils/runtime/PojoSerializer.java:354-411`, release-1.20).
fn decode_pojo_body(
    r: &mut JavaReader<&[u8]>,
    flags: u8,
    info: &PojoInfo,
    proto: Option<&ProtoContext>,
) -> Decoded {
    if flags & POJO_IS_NULL != 0 {
        return Ok(Value::Null);
    }
    // A subclass instance is written after `writeUTF(className)` (or `writeByte(tag)` for a
    // registered one) by the subclass's own serializer — another POJO layout, or Kryo — which
    // the snapshot of this class does not describe (`PojoSerializer.java:382-386, 405-410`).
    if flags & POJO_IS_SUBCLASS != 0 {
        let class = r.read_utf().unwrap_or_else(|_| "?".to_string());
        return Err(undecodable(&format!(
            "subclass {} of {}",
            class, info.short_name
        )));
    }
    if flags & POJO_IS_TAGGED_SUBCLASS != 0 {
        let tag = r.read_byte().map_or("?".to_string(), |t| t.to_string());
        return Err(undecodable(&format!(
            "registered subclass #{} of {}",
            tag, info.short_name
        )));
    }
    decode_pojo_fields(r, info, proto)
}

/// Render a decoded [`Value`] as the compact display string used by the TUI. Markers (see the
/// module docs) are printed as they are, strings in quotes.
pub fn value_to_display(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) if is_marker(s) => s.clone(),
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

/// The fields of a `NO_SUBCLASS` POJO, in the snapshot's order — which is Flink's: `PojoTypeInfo`
/// sorts the fields by name (`api/java/typeutils/PojoTypeInfo.java:100-106`).
fn decode_pojo_fields(
    r: &mut JavaReader<&[u8]>,
    info: &PojoInfo,
    proto: Option<&ProtoContext>,
) -> Decoded {
    let mut obj = Map::new();
    let mut proto_class: Option<String> = None;
    let mut proto_bytes: Option<Vec<u8>> = None;
    // The first field that could not be fully decoded: nothing after it can be located.
    let mut lost: Option<&str> = None;

    for field in &info.fields {
        if let Some(after) = lost {
            obj.insert(field.name.clone(), not_decoded_after(after));
            continue;
        }
        let decoded = decode_pojo_field(r, field, proto, &mut proto_class, &mut proto_bytes);
        if decoded.is_err() {
            lost = Some(&field.name);
        }
        obj.insert(field.name.clone(), value_of(decoded));
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

    if lost.is_some() {
        Err(Value::Object(obj))
    } else {
        Ok(Value::Object(obj))
    }
}

/// One POJO field: `writeBoolean(isNull)`, then, when not null, the field serializer's bytes
/// (`PojoSerializer.java:390-399`). A byte[] field named by a protobuf pattern is captured into
/// `proto_bytes`, and the pattern's class-name field into `proto_class`.
fn decode_pojo_field(
    r: &mut JavaReader<&[u8]>,
    field: &crate::parser::pojo::PojoField,
    proto: Option<&ProtoContext>,
    proto_class: &mut Option<String>,
    proto_bytes: &mut Option<Vec<u8>>,
) -> Decoded {
    match r.read_boolean() {
        Ok(true) => return Ok(Value::Null),
        Ok(false) => {}
        Err(_) => return Err(undecodable(&field.type_name)),
    }

    // Raw protobuf-bytes field (captured for decoding once we also have the class-name field).
    let is_proto_bytes = field.type_name == "byte[]"
        && proto.is_some_and(|c| c.patterns.iter().any(|p| p.bytes_field == field.name));
    if is_proto_bytes {
        let bytes = read_len_bytes(r).ok_or_else(|| undecodable(&field.type_name))?;
        let shown = json!(format!("[{} bytes]", bytes.len()));
        *proto_bytes = Some(bytes);
        return Ok(shown);
    }

    let v = decode_field(r, &field.type_name, field.nested_pojo.as_deref(), proto)?;
    if proto.is_some_and(|c| c.patterns.iter().any(|p| p.class_field == field.name)) {
        if let Value::String(s) = &v {
            *proto_class = Some(s.clone());
        }
    }
    Ok(v)
}

/// Decode a single field/value of serializer type `type_name`. `nested` is the POJO layout when
/// the type is a POJO, or a list/array/nullable of one. See [`Decoded`] for what `Err` means.
pub fn decode_field(
    r: &mut JavaReader<&[u8]>,
    type_name: &str,
    nested: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Decoded {
    let is_container = ["List<", "Array<", "Nullable<", "Map<"]
        .iter()
        .any(|p| type_name.starts_with(p));
    if let (Some(info), false) = (nested, is_container) {
        // The type is the POJO itself (its type name is the class's short name).
        return decode_pojo(r, info, proto);
    }
    match type_name {
        "String[]" => decode_string_array(r),
        t if t.starts_with("Tuple<") => decode_tuple(r, t, proto),
        t if t.starts_with("List<") || t.starts_with("Array<") => {
            decode_list_field(r, t, nested, proto)
        }
        t if t.starts_with("Nullable<") => {
            // NullableSerializer: `writeBoolean(isNull)`, then the value when not null
            // (`api/java/typeutils/runtime/NullableSerializer.java:198-206`).
            let inner = t
                .strip_prefix("Nullable<")
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or("?");
            match r.read_boolean() {
                Ok(true) => Ok(Value::Null),
                Ok(false) => decode_field(r, inner, nested, proto),
                Err(_) => Err(undecodable(t)),
            }
        }
        t => decode_scalar(r, t).ok_or_else(|| undecodable(t)),
    }
}

/// A value with no inner value to stop at: read whole, or not at all. `None` when the type is
/// unknown (Kryo included) — its length is then unknown too — or the bytes end first.
fn decode_scalar(r: &mut JavaReader<&[u8]>, type_name: &str) -> Option<Value> {
    match type_name {
        "Int" => r.read_int().ok().map(|v| json!(v)),
        "Long" => r.read_long().ok().map(|v| json!(v)),
        "Boolean" => r.read_boolean().ok().map(|v| json!(v)),
        "Double" => r.read_long().ok().map(|v| json!(f64::from_bits(v as u64))),
        "Float" => r.read_int().ok().map(|v| json!(f32::from_bits(v as u32))),
        "Short" => r.read_short().ok().map(|v| json!(v)),
        "Byte" => r.read_byte().ok().map(|v| json!(v as i8)),
        // CharSerializer: `writeChar`, one UTF-16 code unit (`api/common/typeutils/base/
        // CharSerializer.java:66-68`).
        "Char" => r.read_char().ok().map(char_value),
        // BigIntSerializer / BigDecSerializer (flink-core typeutils/base, release-1.20): see
        // `read_big_integer`. A null value is written as length 0 and decodes to `null`.
        "BigInteger" => read_big_integer(r).map(|v| v.map_or(Value::Null, Value::String)),
        "BigDecimal" => read_big_decimal(r).map(|v| v.map_or(Value::Null, Value::String)),
        // StringSerializer is `StringValue.writeString` (`StringSerializer.java:67-68`), which,
        // unlike most serializers, writes a null String (as length 0): that one is a real null.
        "String" => r
            .read_nullable_string_value()
            .ok()
            .map(|s| s.map_or(Value::Null, Value::String)),
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
        // CharPrimitiveArraySerializer: `writeInt(length)` + `writeChar` per element
        // (`api/common/typeutils/base/array/CharPrimitiveArraySerializer.java:73-77`).
        "char[]" => read_prim_array(r, |r| r.read_char().ok().map(char_value)),
        // Kryo and unknown serializers: cannot decode (and length is unknown → caller stops).
        _ => None,
    }
}

/// `TupleSerializer`: the fields back to back, no null flags (`api/java/typeutils/runtime/
/// TupleSerializer.java:129-137`).
fn decode_tuple(r: &mut JavaReader<&[u8]>, t: &str, proto: Option<&ProtoContext>) -> Decoded {
    let Some(inner) = t.strip_prefix("Tuple<").and_then(|s| s.strip_suffix('>')) else {
        return Err(undecodable(t));
    };
    let mut items = Vec::new();
    let mut lost: Option<String> = None;
    for (i, elem_type) in split_top_level(inner).into_iter().enumerate() {
        if let Some(after) = &lost {
            items.push(not_decoded_after(after));
            continue;
        }
        match decode_field(r, elem_type.trim(), None, proto) {
            Ok(v) => items.push(v),
            Err(v) => {
                items.push(v);
                lost = Some(format!("f{}", i));
            }
        }
    }
    if lost.is_some() {
        Err(Value::Array(items))
    } else {
        Ok(Value::Array(items))
    }
}

/// A length-prefixed `List<T>` (`ListSerializer`: `writeInt(size)` + each element,
/// `api/common/typeutils/base/ListSerializer.java:118-128`) or `Array<T>`
/// (`GenericArraySerializer`: `writeInt(length)`, then per element `writeBoolean(notNull)` + the
/// element, `api/common/typeutils/base/GenericArraySerializer.java:116-127`).
fn decode_list_field(
    r: &mut JavaReader<&[u8]>,
    t: &str,
    nested: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Decoded {
    let is_array = t.starts_with("Array<");
    let elem_type = t
        .strip_prefix("List<")
        .or_else(|| t.strip_prefix("Array<"))
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or("?");
    let size = match r.read_int() {
        Ok(n) if (0..=1_000_000).contains(&n) => n as usize,
        _ => return Err(undecodable(t)),
    };
    let mut items = Vec::with_capacity(size.min(4096));
    for i in 0..size {
        let element = if is_array {
            match r.read_boolean() {
                Ok(false) => Ok(Value::Null),
                Ok(true) => decode_field(r, elem_type, nested, proto),
                Err(_) => Err(undecodable(elem_type)),
            }
        } else {
            decode_field(r, elem_type, nested, proto)
        };
        match element {
            Ok(v) => items.push(v),
            Err(v) => return Err(stopped_at(items, v, size - i - 1)),
        }
    }
    Ok(Value::Array(items))
}

/// `StringArraySerializer`: `writeInt(length)`, then each element as a `StringValue`, nulls
/// included (`api/common/typeutils/base/array/StringArraySerializer.java:74-78`).
fn decode_string_array(r: &mut JavaReader<&[u8]>) -> Decoded {
    let len = match r.read_int() {
        Ok(n) if (0..=100_000).contains(&n) => n as usize,
        _ => return Err(undecodable("String[]")),
    };
    let mut items = Vec::with_capacity(len.min(4096));
    for i in 0..len {
        match r.read_nullable_string_value() {
            Ok(s) => items.push(s.map_or(Value::Null, Value::String)),
            Err(_) => return Err(stopped_at(items, undecodable("String"), len - i - 1)),
        }
    }
    Ok(Value::Array(items))
}

/// The partial array of a length-prefixed list whose decoding stopped at `failed`, with `rest`
/// elements left unread.
fn stopped_at(mut items: Vec<Value>, failed: Value, rest: usize) -> Value {
    items.push(failed);
    if rest > 0 {
        items.push(Value::String(format!(
            "[not decoded: {} more elements]",
            rest
        )));
    }
    Value::Array(items)
}

/// Split a `Map<K,V>` serializer type string into its key and value types.
pub fn split_map_types(value_type: &str) -> (String, String) {
    value_type
        .strip_prefix("Map<")
        .and_then(|s| s.strip_suffix('>'))
        .and_then(|s| {
            split_top_level(s)
                .into_iter()
                .collect::<Vec<_>>()
                .split_first()
                .map(|(k, rest)| (k.trim().to_string(), rest.join(",").trim().to_string()))
        })
        .unwrap_or_else(|| ("?".to_string(), "?".to_string()))
}

/// Decode a single MAP-state entry as stored in a key-group.
///
/// `namespace` is `VoidNamespace(1 byte) + serialized map key` and `value` is
/// `NullableSerializer(isNull byte) + serialized value`, per the layout in
/// `docs/parsing-format.md`. Shared by [`decode_map_blob`] (which walks the blob the index
/// builder assembles per key) and by the streaming single-state export, which emits one row per
/// entry — so both read map bytes through the same code.
///
/// A map key that cannot be decoded is its marker followed by `#<fallback_key>`, which keeps the
/// keys of one map apart.
pub fn decode_map_entry(
    namespace: &[u8],
    value: &[u8],
    key_type: &str,
    val_type: &str,
    pojo_info: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
    fallback_key: usize,
) -> (String, Value) {
    // Key bytes are `VoidNamespace(1 byte) + serialized map key` — skip the namespace byte.
    let key_payload: &[u8] = if namespace.is_empty() {
        namespace
    } else {
        &namespace[1..]
    };
    let key_str = {
        let mut kr = JavaReader::new(key_payload);
        match decode_field(&mut kr, key_type, None, proto) {
            Ok(Value::String(s)) => s,
            // A null key (a heap MapState accepts one; StringSerializer writes it as length 0)
            // must not read as the string "null".
            Ok(Value::Null) => "[null]".to_string(),
            Ok(v) => v.to_string().trim_matches('"').to_string(),
            Err(marker) => format!("{} #{}", value_to_display(&marker), fallback_key),
        }
    };
    // Value bytes are `NullableSerializer(isNull byte) + serialized value`.
    let decoded = match value.first() {
        Some(1) => Value::Null,
        Some(_) => decode_state_value(&value[1..], val_type, pojo_info, proto),
        None => undecodable(val_type),
    };
    (key_str, decoded)
}

/// Decode a MAP value from the index blob: `int(count) + [int nsLen, ns, int valLen, val]*`.
fn decode_map_blob(
    bytes: &[u8],
    value_type: &str,
    pojo_info: Option<&PojoInfo>,
    proto: Option<&ProtoContext>,
) -> Value {
    let (key_type, val_type) = split_map_types(value_type);

    let mut r = JavaReader::new(bytes);
    let count = match r.read_int() {
        Ok(c) if (0..=1_000_000).contains(&c) => c,
        _ => return undecodable(value_type),
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
        let (key_str, value) = decode_map_entry(
            &ns, &val, &key_type, &val_type, pojo_info, proto, i as usize,
        );
        obj.insert(key_str, value);
    }
    Value::Object(obj)
}

// --- keys ---------------------------------------------------------------------------------------

/// Decode a keyed-state key: the key serializer's bytes, starting right after the key-group
/// prefix. Returns the key's display form and the number of bytes the key occupies, or `None`
/// when the key serializer is not one this decoder reads (the caller then renders the key with
/// [`opaque_key`]).
///
/// The index groups entries by this display form, so it must be **injective**: two distinct keys
/// must never render alike. Hence:
/// - scalars render as their value (`String` as-is, numbers and booleans in JSON form, `Char` as
///   the character, `BigInteger`/`BigDecimal` exactly);
/// - a `Tuple` renders as compact JSON (`["user-1",1]`), which escapes its strings — a naive
///   `(a,b)` join would merge `("a,b","c")` with `("a","b,c")`;
/// - a POJO key keeps its established `{ field: value, … }` rendering;
/// - a value that decodes to `null` (a `Double` NaN, a field that failed inside a tuple) renders
///   opaquely over exactly the bytes the key occupies;
/// - serializers whose rendering drops information (`Timestamp`, `Instant`, `LocalTime` and
///   `LocalDateTime` print milliseconds only; `byte[]` falls back to `[N bytes]`) are left to
///   [`opaque_key`].
pub fn decode_key(
    bytes: &[u8],
    key_type: &str,
    key_pojo: Option<&PojoInfo>,
) -> Option<(String, usize)> {
    let mut r = JavaReader::new(bytes);
    if key_type == "Pojo" {
        let info = key_pojo?;
        // A POJO key that did not fully decode (a Kryo field...) cannot be told apart from
        // another one that stops at the same field: render it opaquely instead.
        let v = decode_pojo(&mut r, info, None).ok()?;
        return Some((value_to_display(&v), r.position() as usize));
    }
    if !key_renders_injectively(key_type) {
        return None;
    }
    let v = decode_key_field(&mut r, key_type).ok()?;
    let used = r.position() as usize;
    let text = render_key(&v).unwrap_or_else(|| opaque_key(&bytes[..used]));
    Some((text, used))
}

/// One field of a key: a `String` rendered losslessly ([`key_string`]), a `Tuple` field by field,
/// anything else through `decode_field`, like a value.
fn decode_key_field(r: &mut JavaReader<&[u8]>, key_type: &str) -> Decoded {
    if key_type == "String" {
        return match r.read_nullable_string_units() {
            Ok(Some(units)) => Ok(Value::String(key_string(&units))),
            Ok(None) => Ok(Value::Null),
            Err(_) => Err(undecodable("String")),
        };
    }
    if let Some(inner) = key_type
        .strip_prefix("Tuple<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let mut items = Vec::new();
        for field in split_top_level(inner) {
            items.push(decode_key_field(r, field.trim())?);
        }
        return Ok(Value::Array(items));
    }
    decode_field(r, key_type, None, None)
}

/// A `String` key's UTF-16 units as text, **injectively**: a Java string may hold a lone
/// surrogate, which no Rust string can, and U+FFFD would merge distinct keys. A string with no
/// lone surrogate and no literal `\u` renders as itself (every real-world key); any other renders
/// escaped — each lone surrogate as `\uXXXX` (uppercase hex, as a `Char` key's lone surrogate
/// renders) and each literal backslash as `\\`. Escaped texts always contain `\u` and plain ones
/// never do, so the two forms cannot collide. [`parse_key_string`] inverts it.
pub fn key_string(units: &[u16]) -> String {
    let decoded: Vec<Result<char, u16>> = char::decode_utf16(units.iter().copied())
        .map(|c| c.map_err(|e| e.unpaired_surrogate()))
        .collect();
    let lone = decoded.iter().any(|c| c.is_err());
    let plain: String = decoded.iter().filter_map(|c| c.ok()).collect();
    if !lone && !plain.contains("\\u") {
        return plain;
    }
    let mut out = String::with_capacity(units.len() + 8);
    for c in decoded {
        match c {
            Ok('\\') => out.push_str("\\\\"),
            Ok(c) => out.push(c),
            Err(unit) => out.push_str(&format!("\\u{:04X}", unit)),
        }
    }
    out
}

/// The UTF-16 units of a `String` key rendered by [`key_string`]; `None` for a text containing
/// `\u` that is not a well-formed escaped rendering.
pub fn parse_key_string(text: &str) -> Option<Vec<u16>> {
    if !text.contains("\\u") {
        return Some(text.encode_utf16().collect());
    }
    let mut units = Vec::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            let mut buf = [0u16; 2];
            units.extend_from_slice(c.encode_utf16(&mut buf));
            continue;
        }
        match chars.next()? {
            '\\' => units.push(b'\\' as u16),
            'u' => {
                let hex: String = chars.by_ref().take(4).collect();
                if hex.len() != 4 || !hex.chars().all(|h| h.is_ascii_hexdigit()) {
                    return None;
                }
                units.push(u16::from_str_radix(&hex, 16).ok()?);
            }
            _ => return None,
        }
    }
    Some(units)
}

/// The display form of a key that could not be decoded: `0x` + the hex of its serialized bytes.
/// Distinct keys serialize to distinct bytes, so they stay distinct.
pub fn opaque_key(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// How many bytes one value of `type_name` occupies at the start of `bytes`, if it decodes.
pub fn decoded_len(bytes: &[u8], type_name: &str) -> Option<usize> {
    let mut r = JavaReader::new(bytes);
    decode_field(&mut r, type_name, None, None)
        .ok()
        .map(|_| r.position() as usize)
}

/// Key serializer types whose decoded form is rendered without losing information. (Enums and
/// arrays never key a stream: `KeyedStream.validateKeyTypeIsHashable` rejects them,
/// flink-streaming-java KeyedStream.java:225-237, release-1.20.)
fn key_renders_injectively(key_type: &str) -> bool {
    match key_type {
        "String" | "Long" | "Int" | "Short" | "Byte" | "Char" | "Boolean" | "Float" | "Double"
        | "BigInteger" | "BigDecimal" | "Date" | "LocalDate" => true,
        t => t
            .strip_prefix("Tuple<")
            .and_then(|s| s.strip_suffix('>'))
            .is_some_and(|inner| {
                split_top_level(inner)
                    .iter()
                    .all(|f| key_renders_injectively(f.trim()))
            }),
    }
}

/// A decoded key as text, or `None` if any part of it is `null` (not a real key value: Flink
/// rejects null keys and null tuple fields, so it means "could not be decoded").
fn render_key(v: &Value) -> Option<String> {
    fn has_null(v: &Value) -> bool {
        match v {
            Value::Null => true,
            Value::Array(items) => items.iter().any(has_null),
            Value::Object(map) => map.values().any(has_null),
            _ => false,
        }
    }
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        composite if has_null(composite) => None,
        composite => Some(composite.to_string()),
    }
}

// --- small helpers ----------------------------------------------------------------------------

/// A `char` as a one-character string; a lone UTF-16 surrogate (which no Rust `char` can hold)
/// as its Java escape, so it stays distinct from every real character.
fn char_value(unit: u16) -> Value {
    match char::from_u32(unit as u32) {
        Some(c) => Value::String(c.to_string()),
        None => Value::String(format!("\\u{:04X}", unit)),
    }
}

/// Largest `BigInteger` magnitude we render, in bytes (~19 700 decimal digits). Anything larger is
/// treated as corrupt rather than allocated and converted.
const MAX_BIG_INTEGER_BYTES: usize = 8192;

/// Read a `BigIntSerializer` value as a decimal string. `None` = could not be read; `Some(None)` =
/// a null value.
///
/// Layout (flink-core `BigIntSerializer.writeBigInteger`/`readBigInteger`, release-1.20,
/// BigIntSerializer.java:87-131): `writeInt(len)`, where `0` = null and `1`/`2`/`3` are the fast
/// paths for `BigInteger.ZERO`/`ONE`/`TEN`; otherwise `len - 4` bytes of
/// `BigInteger.toByteArray()` follow (big-endian two's complement).
fn read_big_integer(r: &mut JavaReader<&[u8]>) -> Option<Option<String>> {
    let len = r.read_int().ok()?;
    match len {
        0 => Some(None),
        1 => Some(Some("0".to_string())),
        2 => Some(Some("1".to_string())),
        3 => Some(Some("10".to_string())),
        // `new BigInteger(new byte[0])` throws, so a length of 4 is never written.
        n if n > 4 && (n as usize - 4) <= MAX_BIG_INTEGER_BYTES => {
            let bytes = r.read_bytes(n as usize - 4).ok()?;
            Some(Some(twos_complement_to_decimal(&bytes)))
        }
        _ => None,
    }
}

/// Read a `BigDecSerializer` value as Java's `BigDecimal.toString()`. `Some(None)` = null.
///
/// Layout (flink-core `BigDecSerializer.serialize`/`readBigDecimal`, release-1.20,
/// BigDecSerializer.java:65-131): the unscaled value as a `BigIntSerializer` BigInteger (null ⇒
/// null, and nothing follows), then `writeInt(scale)`.
fn read_big_decimal(r: &mut JavaReader<&[u8]>) -> Option<Option<String>> {
    let Some(unscaled) = read_big_integer(r)? else {
        return Some(None);
    };
    let scale = r.read_int().ok()?;
    Some(Some(big_decimal_to_string(&unscaled, scale)))
}

/// Decimal form of a big-endian two's complement integer (`BigInteger.toByteArray()`).
fn twos_complement_to_decimal(bytes: &[u8]) -> String {
    let negative = bytes.first().is_some_and(|b| b & 0x80 != 0);
    // Magnitude, big-endian base 256: the bytes themselves, or their two's complement negation.
    let mut mag: Vec<u8> = bytes.to_vec();
    if negative {
        for b in mag.iter_mut() {
            *b = !*b;
        }
        for b in mag.iter_mut().rev() {
            let (v, carry) = b.overflowing_add(1);
            *b = v;
            if !carry {
                break;
            }
        }
    }
    // Repeated division by 10^9 yields base-10^9 digits, least significant first.
    const BASE: u64 = 1_000_000_000;
    let mut start = mag.iter().position(|&b| b != 0).unwrap_or(mag.len());
    let mut chunks: Vec<u32> = Vec::new();
    while start < mag.len() {
        let mut rem: u64 = 0;
        for b in &mut mag[start..] {
            let cur = (rem << 8) | *b as u64;
            *b = (cur / BASE) as u8;
            rem = cur % BASE;
        }
        chunks.push(rem as u32);
        while start < mag.len() && mag[start] == 0 {
            start += 1;
        }
    }
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    match chunks.split_last() {
        None => out.push('0'),
        Some((most, rest)) => {
            out.push_str(&most.to_string());
            for c in rest.iter().rev() {
                out.push_str(&format!("{:09}", c));
            }
        }
    }
    out
}

/// Java's `BigDecimal.toString()` for `unscaled × 10^-scale` (`unscaled` in decimal, possibly
/// negative). Java documents this form as one-to-one with `(unscaled, scale)`, which keeps `1.0`
/// and `1.00` distinct — they are distinct keys, since `BigDecimal.equals` compares the scale.
///
/// The algorithm is the one `BigDecimal.toString()`'s Javadoc specifies: with
/// `adjusted = -scale + (digits - 1)`, plain notation when `scale >= 0 && adjusted >= -6`,
/// otherwise one digit, the remaining digits after a point, and `E±adjusted`.
fn big_decimal_to_string(unscaled: &str, scale: i32) -> String {
    let (negative, digits) = match unscaled.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, unscaled),
    };
    let ulen = digits.len() as i64;
    let scale = scale as i64;
    let adjusted = -scale + (ulen - 1);
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if scale >= 0 && adjusted >= -6 {
        if scale == 0 {
            out.push_str(digits);
        } else if scale < ulen {
            let point = (ulen - scale) as usize;
            out.push_str(&digits[..point]);
            out.push('.');
            out.push_str(&digits[point..]);
        } else {
            out.push_str("0.");
            for _ in 0..(scale - ulen) {
                out.push('0');
            }
            out.push_str(digits);
        }
    } else {
        out.push_str(&digits[..1]);
        if ulen > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        if adjusted != 0 {
            out.push('E');
            if adjusted > 0 {
                out.push('+');
            }
            out.push_str(&adjusted.to_string());
        }
    }
    out
}

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
pub(crate) fn split_top_level(s: &str) -> Vec<&str> {
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
