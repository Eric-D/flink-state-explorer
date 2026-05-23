//! Parser for Flink canonical keyed state data stored inline
//! in KeyGroupsStateHandle ByteStreamStateHandle delegates.
//!
//! Extracts state names and type information by scanning for the
//! KEYED_STATE_TYPE option pattern in the serialized header.

/// Flink `FullSnapshotUtil.END_OF_KEY_GROUP_MARK` — written (as a `short`) after the last state of
/// a key group in a full/canonical snapshot.
const END_OF_KEY_GROUP_MARK: u16 = 0xFFFF;

/// Upper bound on a key-group stateId; beyond this we assume misalignment and stop the block.
const MAX_PLAUSIBLE_STATE_ID: u16 = 1024;

/// Metadata for a single keyed state within an operator.
#[derive(Debug, Clone)]
pub struct KeyedStateMeta {
    pub name: String,
    /// VALUE, MAP, LIST, REDUCING, AGGREGATING, or PRIORITY_QUEUE
    pub state_type: String,
    /// Key TypeSerializer class (shortened), e.g. "String", "Long"
    pub key_type: String,
}

/// Extract state metadata from the inline keyed state data.
///
/// Scans for `KEYED_STATE_TYPE` patterns in the header and backtracks
/// to find the corresponding state name and backend type.
pub fn extract_state_metadata(data: &[u8]) -> Vec<KeyedStateMeta> {
    if data.len() < 10 {
        return Vec::new();
    }

    let key_type = extract_key_serializer_type(data);
    let needle = b"KEYED_STATE_TYPE";

    let mut results = Vec::new();

    // Find all occurrences of "KEYED_STATE_TYPE" in the data
    let limit = data.len().min(50000);
    let mut search_from = 0;

    while search_from < limit.saturating_sub(needle.len()) {
        let pos = match find_bytes(data, needle, search_from, limit) {
            Some(p) => p,
            None => break,
        };
        search_from = pos + needle.len();

        // Read the value (writeUTF right after the needle)
        // But "KEYED_STATE_TYPE" was itself a writeUTF, so we need to account
        // for the 2-byte length prefix BEFORE the needle.
        // The sequence is: [2-byte len=16]["KEYED_STATE_TYPE"][2-byte len][value]
        let val_start = pos + needle.len();
        if val_start + 2 > data.len() {
            continue;
        }
        let val_len = u16::from_be_bytes([data[val_start], data[val_start + 1]]) as usize;
        if val_start + 2 + val_len > data.len() {
            continue;
        }
        let state_type = match std::str::from_utf8(&data[val_start + 2..val_start + 2 + val_len]) {
            Ok(s) => s.to_string(),
            Err(_) => continue,
        };

        // Backtrack to find the state name.
        // The structure before "KEYED_STATE_TYPE" is:
        //   [writeUTF: name][int: backend_type][int: opts_size=1][writeUTF: "KEYED_STATE_TYPE"]
        // The writeUTF for "KEYED_STATE_TYPE" has a 2-byte len prefix at pos-2.
        // Before that: opts_size(4) + backend_type(4) + name(2+N)
        let kst_utf_start = pos - 2; // 2-byte length prefix of "KEYED_STATE_TYPE"
        if kst_utf_start < 8 {
            continue;
        }

        // Try to find the state name by scanning backward
        if let Some(meta) = find_state_name_before(data, kst_utf_start, &state_type, &key_type) {
            // Deduplicate
            if !results.iter().any(|r: &KeyedStateMeta| r.name == meta.name) {
                results.push(meta);
            }
        }
    }

    // Detect priority-queue timer states ("_timer_state/..."), which have no KEYED_STATE_TYPE
    // option. They follow the keyed states in the header (= ascending shared stateId order), so
    // appending them here keeps descriptors aligned with stateIds.
    let timer_needle = b"_timer_state/";
    let mut tf = 0;
    while tf < limit.saturating_sub(timer_needle.len()) {
        let pos = match find_bytes(data, timer_needle, tf, limit) {
            Some(p) => p,
            None => break,
        };
        tf = pos + timer_needle.len();
        if pos < 2 {
            continue;
        }
        let nlen = u16::from_be_bytes([data[pos - 2], data[pos - 1]]) as usize;
        if nlen < timer_needle.len() || pos + nlen > data.len() {
            continue;
        }
        if let Ok(name) = std::str::from_utf8(&data[pos..pos + nlen]) {
            if name.starts_with("_timer_state/") && !results.iter().any(|r| r.name == name) {
                results.push(KeyedStateMeta {
                    name: name.to_string(),
                    state_type: "TIMER".to_string(),
                    key_type: key_type.clone(),
                });
            }
        }
    }

    // Also find states that DON'T have KEYED_STATE_TYPE in options (opts_size=0)
    // These default to VALUE state type.
    find_states_without_options(data, &key_type, &results, &mut results.clone())
        .into_iter()
        .for_each(|m| {
            if !results.iter().any(|r| r.name == m.name) {
                results.push(m);
            }
        });

    results
}

/// Extract the value serializer type for each registered state,
/// aligned with the state metadata by position in the header.
///
/// The header format per state is:
///   `[name][backendType][options][valueSerializerSnapshot][namespaceSerializerSnapshot]`
///
/// Strategy: find each state name's position, find the value serializer
/// snapshot class name that appears BEFORE the VoidNamespaceSerializerSnapshot
/// following that state name.
pub fn extract_value_serializer_types(data: &[u8]) -> Vec<String> {
    let ns_marker = b"VoidNamespaceSerializerSnapshot";
    let snap_marker = b"SerializerSnapshot";
    let limit = data.len().min(50000);

    // Find all VoidNS positions
    let mut ns_positions = Vec::new();
    let mut s = 0;
    while s < limit.saturating_sub(ns_marker.len()) {
        if let Some(p) = find_bytes(data, ns_marker, s, limit) {
            ns_positions.push(p);
            s = p + ns_marker.len();
        } else {
            break;
        }
    }

    // Find state name positions (reuse the state metadata scan logic)
    let state_metas = extract_state_metadata(data);

    // For each state, find its name position, then the first VoidNS AFTER it
    let mut results = Vec::new();

    for meta in &state_metas {
        let name_bytes = meta.name.as_bytes();
        let name_pos = match find_bytes(data, name_bytes, 0, limit) {
            Some(p) => p,
            None => {
                results.push("?".to_string());
                continue;
            }
        };

        let void_ns_after = ns_positions.iter().find(|&&p| p > name_pos).copied();
        let search_end = void_ns_after.unwrap_or(limit);

        // Collect ALL non-VoidNamespace serializer types in order
        let mut all_types: Vec<String> = Vec::new();
        let mut ss = name_pos;
        while ss < search_end.saturating_sub(snap_marker.len()) {
            if let Some(p) = find_bytes(data, snap_marker, ss, search_end) {
                let class_end = p + snap_marker.len();
                if let Some(cn) = read_classname_ending_at(data, class_end) {
                    if !cn.contains("VoidNamespace") {
                        let mut t = snapshot_class_to_type(&cn);
                        // Enrich a bare "Enum" with its constants so ordinals resolve to names
                        // (e.g. for Map<Enum,...> keys).
                        if t == "Enum" {
                            if let Some(consts) = read_enum_constants_at(data, class_end) {
                                t = format!("Enum<{}>", consts);
                            }
                        }
                        all_types.push(t);
                    }
                }
                ss = class_end;
            } else {
                break;
            }
        }

        // The per-state header layout is:
        //   [name][backendType][options][VALUE serializer snapshot][namespace serializer snapshot]
        // so the value serializer is the FIRST snapshot after the name, not the last one before
        // the VoidNamespace marker. Taking the last would pick up a *nested* serializer (e.g. a
        // POJO's last field serializer, or a List's element serializer) and mislabel the state.
        let val_type = if all_types.len() >= 3 && all_types[0] == "Map" {
            // MAP: [MapSerializer, KeySerializer, ValueSerializer, ...] → Map<Key,Value>
            format!("Map<{},{}>", all_types[1], all_types[2])
        } else if all_types.len() >= 2 && all_types[0] == "List" {
            // LIST: [ListSerializer, ElementSerializer, ...] → List<Element>
            format!("List<{}>", all_types[1])
        } else if all_types.len() >= 2 && all_types[0] == "Tuple" {
            // TUPLE: [TupleSerializer, Field0, Field1, ...] → Tuple<F0,F1,...>
            format!("Tuple<{}>", all_types[1..].join(","))
        } else if all_types.len() >= 3 && all_types[0] == "Ttl" {
            // TTL: composite of [Long lastAccessTimestamp, userValue] → Ttl<userValue>
            format!("Ttl<{}>", all_types[2])
        } else {
            all_types
                .first()
                .cloned()
                .unwrap_or_else(|| "?".to_string())
        };
        results.push(val_type);
    }

    results
}

/// For each registered state (aligned with `extract_state_metadata` order), return the POJO
/// class name of its value serializer when the value is (or, for List/Map, contains) a
/// `PojoSerializer`.
///
/// Used to attach the correct `PojoInfo` to each descriptor *by class*. A positional match is
/// unreliable because `extract_pojo_infos` deduplicates POJOs by class, so several states sharing
/// a POJO type (e.g. `ValueState<SimpleEvent>` + `ListState<SimpleEvent>`) would otherwise exhaust
/// a positional iterator and leave later states without their POJO info.
pub fn extract_value_pojo_classes(data: &[u8]) -> Vec<Option<String>> {
    let ns_marker = b"VoidNamespaceSerializerSnapshot";
    let pojo_marker = b"PojoSerializerSnapshot";
    let limit = data.len().min(50000);

    let mut ns_positions = Vec::new();
    let mut s = 0;
    while s < limit.saturating_sub(ns_marker.len()) {
        if let Some(p) = find_bytes(data, ns_marker, s, limit) {
            ns_positions.push(p);
            s = p + ns_marker.len();
        } else {
            break;
        }
    }

    let state_metas = extract_state_metadata(data);
    let mut results = Vec::with_capacity(state_metas.len());
    for meta in &state_metas {
        let name_pos = match find_bytes(data, meta.name.as_bytes(), 0, limit) {
            Some(p) => p,
            None => {
                results.push(None);
                continue;
            }
        };
        let search_end = ns_positions
            .iter()
            .find(|&&p| p > name_pos)
            .copied()
            .unwrap_or(limit);
        // The value serializer is the first snapshot in the state block. If it is (or wraps) a
        // PojoSerializerSnapshot, the snapshot data begins with writeUTF(pojoClassName), right
        // after the snapshot class name and its 4-byte version int.
        let class = find_bytes(data, pojo_marker, name_pos, search_end)
            .and_then(|p| read_utf_at(data, p + pojo_marker.len() + 4));
        results.push(class);
    }
    results
}

/// Read a `writeUTF` at `pos`, returning the string and the position just after it.
fn read_utf_at_pos(data: &[u8], pos: usize) -> Option<(String, usize)> {
    if pos + 2 > data.len() {
        return None;
    }
    let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
    if len == 0 || len > 300 || pos + 2 + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[pos + 2..pos + 2 + len])
        .ok()?
        .to_string();
    Some((s, pos + 2 + len))
}

/// Read the constants of an `EnumSerializerSnapshot` whose class name ends at `pos`, returning
/// `"Short:C0,C1,..."`. The snapshot data is `[int version][writeUTF enumClass][int n][n×writeUTF]`;
/// we also try without the leading version int for robustness across framings.
fn read_enum_constants_at(data: &[u8], pos: usize) -> Option<String> {
    for skip in [4usize, 0] {
        let p0 = pos + skip;
        let (enum_class, mut p) = match read_utf_at_pos(data, p0) {
            Some(v) => v,
            None => continue,
        };
        if !enum_class.contains('.') {
            continue; // expect a fully-qualified class name
        }
        if p + 4 > data.len() {
            continue;
        }
        let n = i32::from_be_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
        p += 4;
        if !(1..=1000).contains(&n) {
            continue;
        }
        let mut consts = Vec::with_capacity(n as usize);
        let mut ok = true;
        for _ in 0..n {
            match read_utf_at_pos(data, p) {
                Some((c, np)) if is_enum_constant(&c) => {
                    consts.push(c);
                    p = np;
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && !consts.is_empty() {
            let short = enum_class.rsplit('.').next().unwrap_or(&enum_class);
            return Some(format!("{}:{}", short, consts.join(",")));
        }
    }
    None
}

fn is_enum_constant(s: &str) -> bool {
    !s.is_empty()
        && s.len() < 100
        && s.chars()
            .next()
            .map_or(false, |c| c.is_ascii_alphabetic() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// Read a `writeUTF` (2-byte big-endian length prefix + UTF-8 bytes) at `pos`.
fn read_utf_at(data: &[u8], pos: usize) -> Option<String> {
    if pos + 2 > data.len() {
        return None;
    }
    let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
    if len == 0 || len > 300 || pos + 2 + len > data.len() {
        return None;
    }
    std::str::from_utf8(&data[pos + 2..pos + 2 + len])
        .ok()
        .map(|s| s.to_string())
}

/// Try to read a writeUTF class name that ends at `end_pos` in the data.
fn read_classname_ending_at(data: &[u8], end_pos: usize) -> Option<String> {
    for try_len in 20..=150 {
        if end_pos < try_len + 2 {
            continue;
        }
        let len_pos = end_pos - try_len - 2;
        let stored_len = u16::from_be_bytes([data[len_pos], data[len_pos + 1]]) as usize;
        if stored_len == try_len {
            let cn_bytes = &data[len_pos + 2..end_pos];
            if let Ok(cn) = std::str::from_utf8(cn_bytes) {
                if cn.contains("Serializer") {
                    return Some(cn.to_string());
                }
            }
        }
    }
    None
}

/// Count the actual number of registered states by counting
/// VoidNamespaceSerializerSnapshot occurrences in the header.
/// This includes timer states that `extract_state_metadata` may miss.
pub fn count_states_in_header(data: &[u8]) -> usize {
    let marker = b"VoidNamespaceSerializerSnapshot";
    let limit = data.len().min(50000);
    let mut count = 0;
    let mut s = 0;
    while s < limit.saturating_sub(marker.len()) {
        if let Some(p) = find_bytes(data, marker, s, limit) {
            count += 1;
            s = p + marker.len();
        } else {
            break;
        }
    }
    count
}

/// Extract the key serializer's PojoInfo from the header, if the key is a POJO.
pub fn extract_key_pojo_info(data: &[u8]) -> Option<crate::parser::pojo::PojoInfo> {
    if data.len() < 12 {
        return None;
    }
    // Header: version(int=4) + compression(bool=1) + keySerializerSnapshot
    // Key serializer at pos 5: proxyVersion(int=2) + writeUTF(className) + snapshotVersion(int)
    let pos = 5;
    let proxy_ver = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    if proxy_ver != 2 {
        return None;
    }
    let cn_pos = pos + 4;
    if cn_pos + 2 > data.len() {
        return None;
    }
    let cn_len = u16::from_be_bytes([data[cn_pos], data[cn_pos + 1]]) as usize;
    if cn_pos + 2 + cn_len > data.len() {
        return None;
    }
    let cn = std::str::from_utf8(&data[cn_pos + 2..cn_pos + 2 + cn_len]).unwrap_or("?");
    if !cn.contains("PojoSerializer") {
        return None;
    }
    // After className: snapshotVersion(int) + PojoSerializerSnapshot data
    let snap_start = cn_pos + 2 + cn_len;
    if snap_start + 4 > data.len() {
        return None;
    }
    // snapshotVersion
    let after_ver = snap_start + 4;
    // Parse the POJO snapshot
    use crate::parser::java_deser::JavaReader;
    let mut r = JavaReader::new(&data[after_ver..]);
    crate::parser::pojo::parse_pojo_from_reader_pub(&mut r)
}

/// Byte size of a serialized value for known primitive types.
/// Returns None for variable-size types (String, Map, Pojo, etc.).
pub fn value_byte_size(value_type: &str) -> Option<usize> {
    match value_type {
        "Int" | "Enum" => Some(4),
        "Long" => Some(8),
        "Boolean" => Some(1),
        "Double" => Some(8),
        "Float" => Some(4),
        "Short" => Some(2),
        "Byte" => Some(1),
        "Char" => Some(2),
        _ => None,
    }
}

fn find_state_name_before(
    data: &[u8],
    kst_utf_start: usize,
    state_type: &str,
    key_type: &str,
) -> Option<KeyedStateMeta> {
    // Before kst_utf_start: opts_size (4 bytes, typically 1) + backend_type (4 bytes)
    // + state name writeUTF (2 + N bytes)
    // We don't know if there are other options before KEYED_STATE_TYPE,
    // so scan backward for valid [writeUTF name][int 0|1][int >= 1]
    let search_start = kst_utf_start.saturating_sub(300);

    for bp in (search_start..kst_utf_start.saturating_sub(8)).rev() {
        if bp + 2 > data.len() {
            continue;
        }
        let nlen = u16::from_be_bytes([data[bp], data[bp + 1]]) as usize;
        if nlen == 0 || nlen > 200 || bp + 2 + nlen + 8 > data.len() {
            continue;
        }

        let name = match std::str::from_utf8(&data[bp + 2..bp + 2 + nlen]) {
            Ok(s) => s,
            Err(_) => continue,
        };

        if !is_valid_state_name(name) {
            continue;
        }

        let after_name = bp + 2 + nlen;
        let bt = i32::from_be_bytes([
            data[after_name],
            data[after_name + 1],
            data[after_name + 2],
            data[after_name + 3],
        ]);
        let opts = i32::from_be_bytes([
            data[after_name + 4],
            data[after_name + 5],
            data[after_name + 6],
            data[after_name + 7],
        ]);

        if (bt == 0 || bt == 1) && opts >= 1 {
            return Some(KeyedStateMeta {
                name: name.to_string(),
                state_type: state_type.to_string(),
                key_type: key_type.to_string(),
            });
        }
    }

    None
}

/// Find states that have opts_size=0 (no KEYED_STATE_TYPE option).
/// These are VALUE states by default.
fn find_states_without_options(
    data: &[u8],
    key_type: &str,
    existing: &[KeyedStateMeta],
    _buf: &mut Vec<KeyedStateMeta>,
) -> Vec<KeyedStateMeta> {
    let mut found = Vec::new();
    let limit = data.len().min(50000);

    for pos in 5..limit.saturating_sub(10) {
        if pos + 2 > data.len() {
            break;
        }
        let nlen = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        if nlen == 0 || nlen > 200 || pos + 2 + nlen + 8 > data.len() {
            continue;
        }

        let name = match std::str::from_utf8(&data[pos + 2..pos + 2 + nlen]) {
            Ok(s) => s,
            Err(_) => continue,
        };

        if !is_valid_state_name(name) {
            continue;
        }

        // Already found via KEYED_STATE_TYPE scan
        if existing.iter().any(|r| r.name == name) {
            continue;
        }
        if found.iter().any(|r: &KeyedStateMeta| r.name == name) {
            continue;
        }

        let after = pos + 2 + nlen;
        let bt = i32::from_be_bytes([
            data[after],
            data[after + 1],
            data[after + 2],
            data[after + 3],
        ]);
        let opts = i32::from_be_bytes([
            data[after + 4],
            data[after + 5],
            data[after + 6],
            data[after + 7],
        ]);

        // opts_size = 0 means no KEYED_STATE_TYPE key, default to VALUE
        if (bt == 0 || bt == 1) && opts == 0 {
            // Verify next 4 bytes are ser_size (small int)
            let ser_pos = after + 8;
            if ser_pos + 4 <= data.len() {
                let ser_size = i32::from_be_bytes([
                    data[ser_pos],
                    data[ser_pos + 1],
                    data[ser_pos + 2],
                    data[ser_pos + 3],
                ]);
                if (0..=20).contains(&ser_size) {
                    let state_type = if bt == 1 { "PRIORITY_QUEUE" } else { "VALUE" };
                    found.push(KeyedStateMeta {
                        name: name.to_string(),
                        state_type: state_type.to_string(),
                        key_type: key_type.to_string(),
                    });
                }
            }
        }
    }

    found
}

/// A keyed entry extracted from a key group block.
#[derive(Debug, Clone)]
pub struct KeyedEntry {
    /// The state index (matches ordering in state metadata).
    pub state_id: u16,
    /// The deserialized key value (e.g. the keyBy string).
    pub key: String,
    /// The key group this entry belongs to.
    pub key_group: u16,
    /// Raw namespace bytes (contains map user key for MAP states, VoidNamespace byte for VALUE).
    pub namespace: Vec<u8>,
    /// Raw value bytes (the serialized state value).
    pub value: Vec<u8>,
}

/// Compute the number of key-group prefix bytes from maxParallelism.
///
/// Matches Flink's `KeyGroupPrefixBytes.computeRequiredBytesInKeyGroupPrefix`.
fn key_group_prefix_bytes(max_parallelism: i32) -> usize {
    // Flink CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix:
    //   maxParallelism > (Byte.MAX_VALUE + 1) ? 2 : 1
    // maxParallelism is capped at 32768, so the prefix is always 1 or 2 bytes (never 4).
    if max_parallelism > 128 {
        2
    } else {
        1
    }
}

/// Extract keyed entries from canonical key-group data blocks.
///
/// Reads the first key from each non-empty key-group for each state.
/// For states with known-size values (Int, Long, …), reads the value
/// and skips remaining entries to continue to the next state.
///
/// Entry format per entry:
///   `[keyGroupPrefix(1-4 bytes)][StringValue(key)][VoidNamespace(1 byte)][value]`
///
/// `state_types` maps state index → state type ("VALUE", "MAP", …). Value bytes are returned raw
/// here; typed decoding lives in `parser::value` (the single decode source of truth).
pub fn extract_keyed_entries(
    data: &[u8],
    offsets: &[i64],
    num_states: usize,
    key_type: &str,
    header_size: usize,
    max_parallelism: i32,
    state_types: &[String],
    key_pojo: Option<&crate::parser::pojo::PojoInfo>,
) -> Vec<KeyedEntry> {
    let kg_prefix_len = key_group_prefix_bytes(max_parallelism);

    // Check if offsets are valid: at least one offset points past the header
    let has_valid_offsets = offsets
        .iter()
        .any(|&o| o > 0 && (o as usize) >= header_size && (o as usize) < data.len());

    if has_valid_offsets {
        let mut entries = Vec::new();
        for (i, &offset) in offsets.iter().enumerate() {
            let start = offset as usize;
            if start < header_size || start >= data.len() {
                continue;
            }

            let end = offsets[(i + 1)..]
                .iter()
                .find(|&&o| (o as usize) > start)
                .map(|&o| o as usize)
                .unwrap_or(data.len())
                .min(data.len());

            if end <= start || end - start < 8 {
                continue;
            }

            parse_key_group_block(
                &data[start..end],
                i as u16,
                num_states,
                key_type,
                kg_prefix_len,
                key_pojo,
                state_types,
                &mut entries,
            );
        }
        entries
    } else {
        if header_size >= data.len() {
            return Vec::new();
        }
        Vec::new()
    }
}

fn parse_key_group_block(
    data: &[u8],
    kg_index: u16,
    num_states: usize,
    key_type: &str,
    kg_prefix_len: usize,
    key_pojo: Option<&crate::parser::pojo::PojoInfo>,
    state_types: &[String],
    entries: &mut Vec<KeyedEntry>,
) {
    // Canonical savepoint format (FullSnapshotAsyncWriter):
    // Per state: [short: stateId] then entries...
    // Per entry: [int: keyLen][keyBytes: kgPrefix+key+namespace][int: valueLen][valueBytes]
    // Entries continue until the next short(stateId) appears.
    //
    // We parse using position-based access to allow "peeking" without consuming.

    // Canonical savepoint format (FullSnapshotAsyncWriter):
    // States are written in stateId order but only for states with entries.
    // Per state: [short: stateId] then entries...
    // Per entry: [int: keyLen][keyBytes: kgPrefix+key+namespace][int: valueLen][valueBytes]

    let mut pos = 0usize;

    loop {
        // Read short(stateId). Stop on EOF or END_OF_KEY_GROUP_MARK.
        if pos + 2 > data.len() {
            break;
        }
        let state_id = u16::from_be_bytes([data[pos], data[pos + 1]]);
        // Terminate on the end marker. The stateId space is shared by keyed and priority-queue
        // (timer) states, so we don't bound it by `num_states` (which only counts keyed states);
        // a sane cap guards against reading garbage. keyLen plausibility ends each state's run.
        if state_id == END_OF_KEY_GROUP_MARK || state_id >= MAX_PLAUSIBLE_STATE_ID {
            break;
        }
        let _ = num_states;
        pos += 2;

        // Read ALL entries for this state.
        // Each entry: [int: keyLen][keyBytes][int: valueLen][valueBytes]
        loop {
            if pos + 4 > data.len() {
                return;
            }
            let kl = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            if kl <= 2 || kl > 10000 {
                break; // Next stateId or end marker
            }
            pos += 4;

            let key_start = pos;
            let key_end = pos + kl as usize;
            if key_end > data.len() {
                return;
            }

            // Timer (priority-queue) entries have a different key layout and no value; synthesize
            // the firing timestamp as the value so it can be displayed.
            let is_timer = state_types
                .get(state_id as usize)
                .map_or(false, |t| t == "TIMER");
            let (key, namespace, timer_value) = if is_timer {
                let (k, ts) = read_timer_key(&data[key_start..key_end], kg_prefix_len, key_type);
                (k, Vec::new(), Some(ts))
            } else {
                let (k, ns) = read_key_from_bytes(
                    &data[key_start..key_end],
                    kg_prefix_len,
                    key_type,
                    key_pojo,
                );
                (k, ns, None)
            };
            pos = key_end;

            // Read value: int(valueLen) + valueBytes
            if pos + 4 > data.len() {
                return;
            }
            let vl = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            if vl < 0 || pos + vl as usize > data.len() {
                return;
            }
            let value = timer_value.unwrap_or_else(|| data[pos..pos + vl as usize].to_vec());
            pos += vl as usize;

            entries.push(KeyedEntry {
                state_id,
                key,
                key_group: kg_index,
                namespace,
                value,
            });
        }
    }
}

/// Decode a priority-queue timer element key (Flink `TimerSerializer`):
/// `[kgPrefix][flipSignBit(ts): long][keyBy key][namespace]`. Returns the keyBy key and the firing
/// timestamp as 8 big-endian bytes (epoch millis) so it can be rendered like a Date.
fn read_timer_key(key_bytes: &[u8], kg_prefix_len: usize, key_type: &str) -> (String, Vec<u8>) {
    use crate::parser::java_deser::JavaReader;
    if kg_prefix_len + 8 > key_bytes.len() {
        return (String::new(), Vec::new());
    }
    let mut r = JavaReader::new(&key_bytes[kg_prefix_len..]);
    let flipped = r.read_long().unwrap_or(0);
    let ts = flipped ^ i64::MIN; // undo MathUtils.flipSignBit
    let key = match key_type {
        "String" => r.read_string_value().unwrap_or_default(),
        "Long" => r.read_long().map(|v| v.to_string()).unwrap_or_default(),
        "Int" => r.read_int().map(|v| v.to_string()).unwrap_or_default(),
        _ => String::new(),
    };
    (key, ts.to_be_bytes().to_vec())
}

fn read_key_from_bytes(
    key_bytes: &[u8],
    kg_prefix_len: usize,
    key_type: &str,
    key_pojo: Option<&crate::parser::pojo::PojoInfo>,
) -> (String, Vec<u8>) {
    use crate::parser::java_deser::JavaReader;

    if kg_prefix_len >= key_bytes.len() {
        return (format!("[{} bytes]", key_bytes.len()), Vec::new());
    }
    let payload_with_ns = &key_bytes[kg_prefix_len..];

    // Read key using a reader to know exactly how many bytes the key consumes
    let mut r = JavaReader::new(payload_with_ns);
    let key_str = match key_type {
        "String" => r.read_string_value().ok(),
        "Long" => r.read_long().ok().map(|v| v.to_string()),
        "Int" => r.read_int().ok().map(|v| v.to_string()),
        _ => None,
    };

    if let Some(s) = key_str {
        let key_end = r.position() as usize;
        let ns_bytes = payload_with_ns[key_end..].to_vec();
        return (s, ns_bytes);
    }

    // POJO key: deserialize and extract namespace from remaining bytes
    if let Some(pojo_info) = key_pojo {
        let (json, pos) = crate::parser::pojo::deserialize_pojo_to_json_with_pos(
            payload_with_ns,
            pojo_info,
            None,
        );
        let ns_bytes = if pos < payload_with_ns.len() {
            payload_with_ns[pos..].to_vec()
        } else {
            Vec::new()
        };
        return (json, ns_bytes);
    }

    // Fallback: extract printable ASCII
    let ascii: String = payload_with_ns
        .iter()
        .filter(|&&b| b >= 0x20 && b < 0x7F)
        .take(60)
        .map(|&b| b as char)
        .collect();
    let key = if ascii.len() >= 3 {
        ascii
    } else {
        format!("[{} bytes]", payload_with_ns.len())
    };
    (key, Vec::new())
}

fn find_bytes(data: &[u8], needle: &[u8], start: usize, end: usize) -> Option<usize> {
    let end = end.min(data.len());
    if needle.len() > end - start {
        return None;
    }
    (start..=end - needle.len()).find(|&i| data[i..i + needle.len()] == *needle)
}

fn extract_key_serializer_type(data: &[u8]) -> String {
    if data.len() < 12 {
        return "?".into();
    }
    // At position 5: serializer proxy version (int=2) + className (writeUTF)
    let pos = 5;
    let proxy_ver = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    if proxy_ver != 2 {
        return "?".into();
    }
    let cn_pos = pos + 4;
    if cn_pos + 2 > data.len() {
        return "?".into();
    }
    let cn_len = u16::from_be_bytes([data[cn_pos], data[cn_pos + 1]]) as usize;
    if cn_pos + 2 + cn_len > data.len() {
        return "?".into();
    }
    let cn = std::str::from_utf8(&data[cn_pos + 2..cn_pos + 2 + cn_len]).unwrap_or("?");
    snapshot_class_to_type(cn)
}

fn snapshot_class_to_type(class: &str) -> String {
    let short = class.rsplit('.').next().unwrap_or(class);
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
    } else if short.contains("PojoSerializer") {
        "Pojo".into()
    } else if short.contains("EnumSerializer") {
        "Enum".into()
    } else if short.contains("MapSerializer") {
        "Map".into()
    } else if short.contains("ListSerializer") {
        "List".into()
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
    } else if short.contains("ShortPrimitiveArraySerializer") {
        "short[]".into()
    } else if short.contains("BooleanPrimitiveArraySerializer") {
        "boolean[]".into()
    } else if short.contains("TupleSerializer") {
        "Tuple".into()
    } else if short.contains("TtlSerializer") {
        "Ttl".into()
    } else if short.contains("KryoSerializer") {
        "Kryo".into()
    } else if short.contains("NullableSerializer") {
        "Nullable".into()
    } else if short.contains("StringArraySerializer") {
        "String[]".into()
    } else if short.contains("ShortSerializer") {
        "Short".into()
    } else if short.contains("ByteSerializer") && !short.contains("Array") {
        "Byte".into()
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
    } else {
        short.replace("Snapshot", "").replace('$', ".")
    }
}

fn is_valid_state_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() < 200
        && s.chars().all(|c| {
            c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ' ' || c == '/'
        })
        && s.chars().any(|c| c.is_ascii_lowercase())
        && !(s.contains('.') && s.chars().any(|c| c.is_uppercase()))
        // Exclude only Flink internal serializer config keys
        && !matches!(
            s,
            "KEY_SERIALIZER"
                | "NAMESPACE_SERIALIZER"
                | "VALUE_SERIALIZER"
                | "ELEMENT_SERIALIZER"
        )
}
