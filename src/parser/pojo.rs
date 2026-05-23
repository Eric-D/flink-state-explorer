//! Parser for Flink PojoSerializerSnapshot to extract POJO class name,
//! field names, and their types from the serializer snapshot data.
//! Supports recursive parsing of nested POJOs.

use crate::parser::java_deser::JavaReader;

// Magic numbers, named to match the Apache Flink source they come from (branch release-1.20).
/// Flink `CompositeTypeSerializerSnapshot.MAGIC_NUMBER` — precedes the outer snapshot version.
const COMPOSITE_TYPE_SERIALIZER_SNAPSHOT_MAGIC_NUMBER: i32 = 911108;
/// Flink `NestedSerializersSnapshotDelegate.MAGIC_NUMBER` — precedes the nested serializer count.
const NESTED_SERIALIZERS_SNAPSHOT_DELEGATE_MAGIC_NUMBER: i32 = 1333245;
/// Flink `LinkedOptionalMapSerializer.HEADER` — magic long at the start of an optional map.
const LINKED_OPTIONAL_MAP_HEADER: i64 = 0x4f2c_69a3_d70;

/// A POJO field with its name and type info.
#[derive(Debug, Clone)]
pub struct PojoField {
    pub name: String,
    pub type_name: String,
    /// If the field is itself a POJO, its parsed info.
    pub nested_pojo: Option<Box<PojoInfo>>,
}

/// Parsed POJO serializer info.
#[derive(Debug, Clone)]
pub struct PojoInfo {
    pub class_name: String,
    pub short_name: String,
    pub fields: Vec<PojoField>,
}

impl PojoInfo {
    /// Collect all leaf fields from this POJO and its nested POJOs,
    /// with a dotted path prefix.
    pub fn leaf_fields(&self, prefix: &str) -> Vec<(String, String)> {
        let mut result = Vec::new();
        for f in &self.fields {
            let path = if prefix.is_empty() {
                f.name.clone()
            } else {
                format!("{}.{}", prefix, f.name)
            };
            if let Some(nested) = &f.nested_pojo {
                result.extend(nested.leaf_fields(&path));
            } else {
                result.push((path, f.type_name.clone()));
            }
        }
        result
    }
}

/// Scan data for PojoSerializerSnapshot patterns associated with
/// VALUE_SERIALIZER entries and extract POJO info with recursive
/// field parsing.
pub fn extract_pojo_infos(data: &[u8]) -> Vec<PojoInfo> {
    let needle = b"PojoSerializerSnapshot";
    let mut results = Vec::new();
    let mut search_from = 0;

    while search_from < data.len().saturating_sub(needle.len()) {
        let pos = match find_needle(data, needle, search_from) {
            Some(p) => p,
            None => break,
        };
        search_from = pos + needle.len();

        let utf_start = match find_utf_start(data, pos, needle.len()) {
            Some(s) => s,
            None => continue,
        };

        let class_len = u16::from_be_bytes([data[utf_start], data[utf_start + 1]]) as usize;
        let after_class = utf_start + 2 + class_len;

        // Only parse top-level VALUE_SERIALIZER POJOs
        let is_value_ser = {
            let search_start = utf_start.saturating_sub(200);
            let region = &data[search_start..utf_start];
            region
                .windows(b"VALUE_SERIALIZER".len())
                .any(|w| w == b"VALUE_SERIALIZER")
        };
        if !is_value_ser {
            continue;
        }

        if let Some(info) = parse_pojo_snapshot(data, after_class) {
            if !results
                .iter()
                .any(|r: &PojoInfo| r.class_name == info.class_name)
            {
                results.push(info);
            }
        }
    }

    results
}

fn parse_pojo_snapshot(data: &[u8], offset: usize) -> Option<PojoInfo> {
    if offset + 8 > data.len() {
        return None;
    }
    let mut r = JavaReader::new(&data[offset..]);
    // The snapshotVersion is written by writeVersionedSnapshot, not writeSnapshot.
    // Consume it here before calling parse_pojo_from_reader.
    let _snapshot_ver = r.read_int().ok()?;
    parse_pojo_from_reader(&mut r)
}

/// Parse POJO info from a reader positioned after the PojoSerializerSnapshot
/// class name and TypeSerializer snapshot version.
///
/// PojoSerializerSnapshot format (Flink 1.20, snapshot v2+):
///   [int: pojoSnapshotVersion]
///   [writeUTF: pojoClassName]
///   [long: FIELD_MAP_MAGIC][int: numFields]
///   For each field:
///     [writeUTF: fieldName]
///     [boolean: keyPresent][if true: int(frameLen) + frameBytes]
///     [boolean: valuePresent][if true: int(frameLen) + frameBytes (serializer snapshot)]
///   [registered subclass map]
///   [non-registered subclass map]
/// Parse POJO info from writeSnapshot output.
/// Format: writeUTF(pojoClassName) + writeLong(FIELD_MAP_MAGIC) + writeInt(numFields) + fields...
/// Note: the snapshotVersion is NOT part of writeSnapshot — it's consumed by the caller.
pub fn parse_pojo_from_reader_pub(r: &mut JavaReader<&[u8]>) -> Option<PojoInfo> {
    parse_pojo_from_reader(r)
}

fn parse_pojo_from_reader(r: &mut JavaReader<&[u8]>) -> Option<PojoInfo> {
    let pojo_class = r.read_utf().ok()?;
    let short_name = pojo_class
        .rsplit('.')
        .next()
        .unwrap_or(&pojo_class)
        .to_string();

    // Field map: [long: MAGIC][int: numFields]
    let _field_map_magic = r.read_long().ok()?;
    let num_fields = r.read_int().ok()?;
    if !(0..=200).contains(&num_fields) {
        return None;
    }

    let mut fields = Vec::with_capacity(num_fields as usize);
    for _ in 0..num_fields {
        let field_name = match r.read_utf() {
            Ok(n) => n,
            Err(_) => break,
        };

        // Key: field descriptor (framed)
        let key_present = r.read_boolean().ok().unwrap_or(false);
        if key_present {
            let frame_len = r.read_int().ok().unwrap_or(0) as usize;
            if r.skip(frame_len).is_err() {
                break;
            }
        }

        // Value: serializer snapshot (framed)
        let val_present = r.read_boolean().ok().unwrap_or(false);
        if val_present {
            let frame_len = r.read_int().ok().unwrap_or(0) as usize;
            let frame_bytes = match r.read_bytes(frame_len) {
                Ok(b) => b,
                Err(_) => break,
            };
            // Parse the framed snapshot (writeVersionedSnapshot format:
            // writeUTF(className) + writeInt(version) + snapshotData — no proxyVersion)
            let mut fr = JavaReader::new(&frame_bytes[..]);
            match read_versioned_snapshot(&mut fr) {
                Some(ft) => fields.push(PojoField {
                    name: field_name,
                    type_name: ft.type_name,
                    nested_pojo: ft.nested_pojo,
                }),
                None => {
                    // Try to at least extract the class name for diagnostics
                    let mut fr2 = JavaReader::new(&frame_bytes[..]);
                    let class_hint = fr2
                        .read_utf()
                        .ok()
                        .map(|cn| {
                            let s = cn.rsplit('.').next().unwrap_or(&cn);
                            s.replace("Snapshot", "").replace('$', ".")
                        })
                        .unwrap_or_default();
                    let type_name = if class_hint.is_empty() {
                        "?".to_string()
                    } else {
                        class_hint
                    };
                    fields.push(PojoField {
                        name: field_name,
                        type_name,
                        nested_pojo: None,
                    });
                }
            }
        } else {
            fields.push(PojoField {
                name: field_name,
                type_name: "(removed)".to_string(),
                nested_pojo: None,
            });
        }
    }

    // Skip registered + non-registered subclass maps
    for _ in 0..2 {
        skip_optional_map(r);
    }

    Some(PojoInfo {
        class_name: pojo_class,
        short_name,
        fields,
    })
}

/// Read nested serializer snapshots from a CompositeTypeSerializerSnapshot.
///
/// Format after className+version:
///   [int: MAGIC=911108][int: outerVersion][outerData (type-specific)]
///   [int: MAGIC=911108][int: delegateVersion][int: numNested]
///   [writeVersionedSnapshot for each nested serializer]
/// Read nested serializers from a CompositeTypeSerializerSnapshot.
/// No outer data (for List, Map).
fn read_composite_nested_serializers(r: &mut JavaReader<&[u8]>) -> Vec<FieldTypeInfo> {
    read_composite_nested_serializers_with_outer(r, |_| {})
}

/// Read nested serializers from a CompositeTypeSerializerSnapshot,
/// with a callback to read type-specific outer snapshot data.
fn read_composite_nested_serializers_with_outer(
    r: &mut JavaReader<&[u8]>,
    read_outer: impl FnOnce(&mut JavaReader<&[u8]>),
) -> Vec<FieldTypeInfo> {
    let magic1 = r.read_int().unwrap_or(0);
    if magic1 != COMPOSITE_TYPE_SERIALIZER_SNAPSHOT_MAGIC_NUMBER {
        return Vec::new();
    }
    let _outer_ver = r.read_int().unwrap_or(0);
    read_outer(r);

    let magic2 = r.read_int().unwrap_or(0);
    if magic2 != NESTED_SERIALIZERS_SNAPSHOT_DELEGATE_MAGIC_NUMBER {
        return Vec::new();
    }
    let _delegate_ver = r.read_int().unwrap_or(0);
    let num_nested = r.read_int().unwrap_or(0);
    if num_nested <= 0 || num_nested > 10 {
        return Vec::new();
    }

    let mut results = Vec::new();
    for _ in 0..num_nested {
        match read_versioned_snapshot(r) {
            Some(ft) => results.push(ft),
            None => break,
        }
    }
    results
}

/// Skip a Flink `LinkedOptionalMapSerializer` map: `writeLong(HEADER) + writeInt(size) + entries`.
fn skip_optional_map(r: &mut JavaReader<&[u8]>) {
    if r.read_long().unwrap_or(0) != LINKED_OPTIONAL_MAP_HEADER {
        return;
    }
    let count = r.read_int().unwrap_or(0);
    for _ in 0..count.clamp(0, 50) {
        let _ = r.read_utf(); // map key (string)
        for _ in 0..2 {
            // key part, then value part — both framed
            let present = r.read_boolean().unwrap_or(false);
            if present {
                let len = r.read_int().unwrap_or(0) as usize;
                let _ = r.skip(len);
            }
        }
    }
}

struct FieldTypeInfo {
    type_name: String,
    nested_pojo: Option<Box<PojoInfo>>,
}

/// Read a versioned snapshot (writeVersionedSnapshot format):
/// writeUTF(className) + writeInt(version) + snapshotData.
/// No proxyVersion prefix.
/// Read a versioned snapshot (writeVersionedSnapshot format):
/// writeUTF(className) + writeInt(version) + snapshotData.
fn read_versioned_snapshot(r: &mut JavaReader<&[u8]>) -> Option<FieldTypeInfo> {
    let class_name = r.read_utf().ok()?;
    let _snap_ver = r.read_int().ok()?;
    let short = class_name
        .rsplit('.')
        .next()
        .unwrap_or(&class_name)
        .to_string();
    interpret_snapshot_class(r, &short)
}

/// Read a field type with proxyVersion prefix:
/// int(proxyVersion) + writeUTF(className) + int(snapshotVersion) + data.
fn read_field_type(r: &mut JavaReader<&[u8]>) -> Option<FieldTypeInfo> {
    let _proxy_ver = r.read_int().ok()?;
    let class_name = r.read_utf().ok()?;
    let _snap_ver = r.read_int().ok()?;
    let short = class_name
        .rsplit('.')
        .next()
        .unwrap_or(&class_name)
        .to_string();
    interpret_snapshot_class(r, &short)
}

fn interpret_snapshot_class(r: &mut JavaReader<&[u8]>, short: &str) -> Option<FieldTypeInfo> {
    // POJO: recursive parse
    if short.contains("PojoSerializer") {
        return match parse_pojo_from_reader(r) {
            Some(nested) => {
                let type_name = nested.short_name.clone();
                Some(FieldTypeInfo {
                    type_name,
                    nested_pojo: Some(Box::new(nested)),
                })
            }
            None => {
                // Nested POJO parsing failed — return the type name
                // but without field details. The reader is now at an
                // unknown position, so the caller should stop.
                Some(FieldTypeInfo {
                    type_name: "Pojo".to_string(),
                    nested_pojo: None,
                })
            }
        };
    }

    // Enum: extract class name + constant names
    if short.contains("EnumSerializer") {
        let enum_class = r.read_utf().ok()?;
        let enum_short = enum_class
            .rsplit('.')
            .next()
            .unwrap_or(&enum_class)
            .to_string();
        let n = r.read_int().unwrap_or(0);
        let mut constants = Vec::new();
        for _ in 0..n {
            match r.read_utf() {
                Ok(name) => constants.push(name),
                Err(_) => return None,
            }
        }
        // Store constants in type_name: "Enum<Name:CONST0,CONST1,...>"
        let type_name = if constants.is_empty() {
            format!("Enum<{}>", enum_short)
        } else {
            format!("Enum<{}:{}>", enum_short, constants.join(","))
        };
        return Some(FieldTypeInfo {
            type_name,
            nested_pojo: None,
        });
    }

    // CompositeTypeSerializerSnapshot-based types (List, Map, Array, etc.)
    if short.contains("ListSerializer") {
        let nested = read_composite_nested_serializers(r);
        let elem = nested.into_iter().next();
        return match elem {
            Some(ft) => Some(FieldTypeInfo {
                type_name: format!("List<{}>", ft.type_name),
                nested_pojo: ft.nested_pojo,
            }),
            None => Some(FieldTypeInfo {
                type_name: "List<?>".to_string(),
                nested_pojo: None,
            }),
        };
    }
    if short.contains("GenericArraySerializer") {
        let nested = read_composite_nested_serializers_with_outer(r, |or| {
            let _ = or.read_utf(); // componentClassName
        });
        let elem = nested.into_iter().next();
        return match elem {
            Some(ft) => Some(FieldTypeInfo {
                type_name: format!("Array<{}>", ft.type_name),
                nested_pojo: ft.nested_pojo,
            }),
            None => Some(FieldTypeInfo {
                type_name: "Array<?>".to_string(),
                nested_pojo: None,
            }),
        };
    }
    if short.contains("MapSerializer") {
        let nested = read_composite_nested_serializers(r);
        let mut it = nested.into_iter();
        let key = it.next();
        let val = it.next();
        let kt = key.as_ref().map(|f| f.type_name.as_str()).unwrap_or("?");
        let vt = val.as_ref().map(|f| f.type_name.as_str()).unwrap_or("?");
        let type_name = format!("Map<{},{}>", kt, vt);
        let nested_pojo = val.and_then(|f| f.nested_pojo);
        return Some(FieldTypeInfo {
            type_name,
            nested_pojo,
        });
    }

    // NullableSerializer wraps another serializer (CompositeTypeSerializerSnapshot)
    if short.contains("NullableSerializer") {
        let nested = read_composite_nested_serializers_with_outer(r, |or| {
            let _ = or.read_boolean(); // padding flag
        });
        let inner = nested.into_iter().next();
        return match inner {
            Some(ft) => Some(FieldTypeInfo {
                type_name: format!("Nullable<{}>", ft.type_name),
                nested_pojo: ft.nested_pojo,
            }),
            None => Some(FieldTypeInfo {
                type_name: "Nullable<?>".to_string(),
                nested_pojo: None,
            }),
        };
    }

    let type_name = snapshot_to_type(short);

    if !skip_snapshot_data(r, short) {
        return None;
    }

    Some(FieldTypeInfo {
        type_name,
        nested_pojo: None,
    })
}

fn snapshot_to_type(short: &str) -> String {
    if short.contains("StringSerializer") {
        "String".into()
    } else if short.contains("IntSerializer") {
        "Int".into()
    } else if short.contains("LongSerializer") {
        "Long".into()
    } else if short.contains("BooleanSerializer") {
        "Boolean".into()
    } else if short.contains("DoubleSerializer") {
        "Double".into()
    } else if short.contains("FloatSerializer") {
        "Float".into()
    } else if short.contains("ByteSerializer") {
        "Byte".into()
    } else if short.contains("ShortSerializer") {
        "Short".into()
    } else if short.contains("BytePrimitiveArraySerializer") {
        "byte[]".into()
    } else if short.contains("IntPrimitiveArraySerializer") {
        "int[]".into()
    } else if short.contains("LongPrimitiveArraySerializer") {
        "long[]".into()
    } else if short.contains("DoublePrimitiveArraySerializer") {
        "double[]".into()
    } else if short.contains("FloatPrimitiveArraySerializer") {
        "float[]".into()
    } else if short.contains("BooleanPrimitiveArraySerializer") {
        "boolean[]".into()
    } else if short.contains("ShortPrimitiveArraySerializer") {
        "short[]".into()
    } else if short.contains("CharPrimitiveArraySerializer") {
        "char[]".into()
    } else if short.contains("ListSerializer") {
        "List<?>".into()
    } else if short.contains("MapSerializer") {
        "Map<?,?>".into()
    } else if short.contains("NullableSerializer") {
        "Nullable<?>".into()
    } else if short.contains("LocalDateTimeSerializer") {
        "LocalDateTime".into()
    } else if short.contains("LocalDateSerializer") {
        "LocalDate".into()
    } else if short.contains("LocalTimeSerializer") {
        "LocalTime".into()
    } else if short.contains("InstantSerializer") {
        "Instant".into()
    } else if short.contains("DateSerializer") || short.contains("SqlDateSerializer") {
        "Date".into()
    } else if short.contains("TimestampSerializer") || short.contains("SqlTimestampSerializer") {
        "Timestamp".into()
    } else if short.contains("StringArraySerializer") {
        "String[]".into()
    } else if short.contains("VoidNamespace") || short.contains("VoidSerializer") {
        "Void".into()
    } else {
        short.replace("Snapshot", "").replace('$', ".")
    }
}

fn skip_snapshot_data(r: &mut JavaReader<&[u8]>, short: &str) -> bool {
    if short.contains("StringSerializer")
        || short.contains("IntSerializer")
        || short.contains("LongSerializer")
        || short.contains("BooleanSerializer")
        || short.contains("DoubleSerializer")
        || short.contains("FloatSerializer")
        || short.contains("ByteSerializer")
        || short.contains("ShortSerializer")
        || short.contains("CharSerializer")
        || short.contains("VoidNamespace")
        || short.contains("VoidSerializer")
        || short.contains("PrimitiveArraySerializer")
        || short.contains("StringArraySerializer")
        || short.contains("LocalDateTimeSerializer")
        || short.contains("LocalDateSerializer")
        || short.contains("LocalTimeSerializer")
        || short.contains("InstantSerializer")
        || short.contains("DateSerializer")
        || short.contains("TimestampSerializer")
    {
        return true;
    }
    if short.contains("ListSerializer") {
        return read_field_type(r).is_some();
    }
    if short.contains("MapSerializer") || short.contains("EitherSerializer") {
        return read_field_type(r).is_some() && read_field_type(r).is_some();
    }
    if short.contains("TimerSerializer") {
        return read_field_type(r).is_some() && read_field_type(r).is_some();
    }
    false
}

fn find_needle(data: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    let end = data.len();
    if needle.len() > end.saturating_sub(start) {
        return None;
    }
    (start..=end - needle.len()).find(|&i| data[i..i + needle.len()] == *needle)
}

fn find_utf_start(data: &[u8], needle_pos: usize, needle_len: usize) -> Option<usize> {
    for back in (needle_pos.saturating_sub(100)..needle_pos).rev() {
        if back + 2 > data.len() {
            continue;
        }
        let len = u16::from_be_bytes([data[back], data[back + 1]]) as usize;
        if back + 2 + len == needle_pos + needle_len {
            return Some(back);
        }
    }
    None
}

/// Deserialize a POJO value. If a `ProtoContext` is provided, ProtoSerializable
/// POJOs (`{className, protoBytes}`) will have their protoBytes decoded.
pub fn deserialize_pojo_to_json(
    raw: &[u8],
    info: &PojoInfo,
    proto_ctx: Option<&crate::proto::ProtoContext>,
) -> String {
    deserialize_pojo_to_json_with_pos(raw, info, proto_ctx).0
}

/// Deserialize a POJO and return (json_string, bytes_consumed).
pub fn deserialize_pojo_to_json_with_pos(
    raw: &[u8],
    info: &PojoInfo,
    proto_ctx: Option<&crate::proto::ProtoContext>,
) -> (String, usize) {
    let mut r = JavaReader::new(raw);
    let value = crate::parser::value::decode_pojo(&mut r, info, proto_ctx);
    (
        crate::parser::value::value_to_display(&value),
        r.position() as usize,
    )
}

/// Read a single field value from the reader. Public for use in tree display.
pub fn read_field_value_pub(
    r: &mut JavaReader<&[u8]>,
    type_name: &str,
    nested: Option<&PojoInfo>,
    proto_ctx: Option<&crate::proto::ProtoContext>,
) -> Option<String> {
    crate::parser::value::decode_field(r, type_name, nested, proto_ctx)
        .map(|v| crate::parser::value::value_to_display(&v))
}

/// Convert epoch milliseconds to ISO 8601 format (UTC).
pub fn millis_to_iso(millis: i64) -> String {
    const SECS_PER_DAY: i64 = 86400;
    const MILLIS_PER_SEC: i64 = 1000;

    if millis == 0 {
        return "1970-01-01T00:00:00Z".to_string();
    }

    let total_secs = millis.div_euclid(MILLIS_PER_SEC);
    let ms = millis.rem_euclid(MILLIS_PER_SEC);
    let mut days = total_secs.div_euclid(SECS_PER_DAY);
    let day_secs = total_secs.rem_euclid(SECS_PER_DAY);

    let hour = day_secs / 3600;
    let minute = (day_secs % 3600) / 60;
    let second = day_secs % 60;

    // Convert days since epoch to year-month-day
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    if ms == 0 {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            y, m, d, hour, minute, second
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            y, m, d, hour, minute, second, ms
        )
    }
}
