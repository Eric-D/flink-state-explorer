//! Parser for Flink canonical keyed state data stored inline
//! in KeyGroupsStateHandle ByteStreamStateHandle delegates.
//!
//! Extracts state names and type information by scanning for the
//! KEYED_STATE_TYPE option pattern in the serialized header.
//!
//! The header extractors (`extract_state_metadata`, `extract_value_serializer_types`, …) expect
//! the **header only**: the bytes from offset 0 of the keyed stream up to the first key-group,
//! whose offset `KeyGroupsStateHandle.offsets` records. Handed key-group bytes, they would scan
//! them as if they were serializer snapshots.

/// Flink `FullSnapshotUtil.END_OF_KEY_GROUP_MARK` — written (as a `short`) after the last state of
/// a key group in a full/canonical snapshot.
const END_OF_KEY_GROUP_MARK: u16 = 0xFFFF;

/// Flink `FullSnapshotUtil.FIRST_BIT_IN_BYTE_MASK` (`FullSnapshotUtil.java:27`, release-1.20): set
/// on `key[0]` of the **last** entry of each state's run (`setMetaDataFollowsFlagInKey`), meaning
/// "a `short` stateId — or `END_OF_KEY_GROUP_MARK` — follows this entry". `key[0]` is the high
/// byte of the key-group prefix, which never uses that bit (maxParallelism ≤ 32768).
const FIRST_BIT_IN_BYTE_MASK: u8 = 0x80;

/// Metadata for a single keyed state within an operator.
#[derive(Debug, Clone)]
pub struct KeyedStateMeta {
    pub name: String,
    /// VALUE, MAP, LIST, REDUCING, AGGREGATING, or PRIORITY_QUEUE
    pub state_type: String,
    /// Key TypeSerializer class (shortened), e.g. "String", "Long"
    pub key_type: String,
}

/// Extract state metadata from a keyed stream's header (see the module doc: header bytes only).
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

    // Find all occurrences of "KEYED_STATE_TYPE" in the header. The whole header is scanned:
    // a fixed cap (it was 50 KB) silently dropped every state registered past it.
    let limit = data.len();
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
///
/// `data` is the header only; `state_metas` is `extract_state_metadata(data)`, computed once by
/// the caller and shared with the other extractors.
pub fn extract_value_serializer_types(data: &[u8], state_metas: &[KeyedStateMeta]) -> Vec<String> {
    let ns_marker = b"VoidNamespaceSerializerSnapshot";
    let snap_marker = b"SerializerSnapshot";
    let limit = data.len();

    // Find all VoidNS positions
    let ns_positions = find_all(data, ns_marker);
    let name_positions = state_name_positions(data, state_metas);

    // For each state, find its name position, then the first VoidNS AFTER it
    let mut results = Vec::new();

    for name_pos in name_positions {
        let name_pos = match name_pos {
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
///
/// `data` is the header only; `state_metas` is `extract_state_metadata(data)`.
pub fn extract_value_pojo_classes(
    data: &[u8],
    state_metas: &[KeyedStateMeta],
) -> Vec<Option<String>> {
    let ns_marker = b"VoidNamespaceSerializerSnapshot";
    let pojo_marker = b"PojoSerializerSnapshot";
    let limit = data.len();

    let ns_positions = find_all(data, ns_marker);

    let mut results = Vec::with_capacity(state_metas.len());
    for name_pos in state_name_positions(data, state_metas) {
        let name_pos = match name_pos {
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

/// Number of states the header registers — keyed **and** priority-queue (timer) states, which
/// share one stateId space in the key-groups — i.e. the bound on a key-group's stateIds.
///
/// `KeyedBackendSerializationProxy.write` (`KeyedBackendSerializationProxy.java:133`, release-1.20)
/// writes that count as a `short` right after the key serializer snapshot, and the first
/// `StateMetaInfoSnapshot` follows at once (`writeUTF(name)`, `writeInt(backendStateType)`,
/// `writeInt(options.size())`: `StateMetaInfoSnapshotReadersWriters.java:109-111`). A serializer
/// snapshot has no length prefix, so the `short` is found in front of the earliest state record.
///
/// The result is never below what the heuristics see — one `VoidNamespaceSerializerSnapshot` per
/// state with a void namespace (timers included), and the states `extract_state_metadata` found
/// (`state_metas`) — so a misread can only loosen the bound, never reject a valid stateId.
pub fn count_states_in_header(data: &[u8], state_metas: &[KeyedStateMeta]) -> usize {
    let void_namespaces = find_all(data, b"VoidNamespaceSerializerSnapshot").len();
    declared_state_count(data, state_metas)
        .unwrap_or(0)
        .max(void_namespaces)
        .max(state_metas.len())
        .max(1)
}

/// The `short` state count of `KeyedBackendSerializationProxy`, read in front of the earliest
/// `StateMetaInfoSnapshot` of a known state. `None` when no state record can be located.
fn declared_state_count(data: &[u8], state_metas: &[KeyedStateMeta]) -> Option<usize> {
    let first = meta_info_records(data, state_metas)
        .into_iter()
        .flatten()
        .min()?;
    // At the very least `[int version][bool compression][int proxy version]` precede the key
    // serializer snapshot, and the count sits between that snapshot and the first record.
    if first < 9 + 2 {
        return None;
    }
    Some(u16::from_be_bytes([data[first - 2], data[first - 1]]) as usize)
}

/// Where each state's `StateMetaInfoSnapshot` starts, in `state_metas` order: the first
/// `[u16 len][name][int backendStateType][int numOptions]` whose backend state type is an
/// ordinal of `StateMetaInfoSnapshot.BackendStateType` (KEY_VALUE 0 … PRIORITY_QUEUE 3), as
/// `StateMetaInfoSnapshotReadersWriters.java:109-111` writes it. `None` when not found.
///
/// One pass over the header, however many states it registers: with the header no longer cut at
/// 50 KB, searching each name from offset 0 would be quadratic on a wide operator.
fn meta_info_records(data: &[u8], state_metas: &[KeyedStateMeta]) -> Vec<Option<usize>> {
    // Reversed so that, should a name repeat, its first index wins.
    let by_name: std::collections::HashMap<&[u8], usize> = state_metas
        .iter()
        .enumerate()
        .rev()
        .map(|(i, m)| (m.name.as_bytes(), i))
        .collect();
    let max_len = state_metas.iter().map(|m| m.name.len()).max().unwrap_or(0);
    let mut out = vec![None; state_metas.len()];
    let mut missing = by_name.len();
    let mut p = 0;
    while missing > 0 && p + 2 <= data.len() {
        let len = u16::from_be_bytes([data[p], data[p + 1]]) as usize;
        let after = p + 2 + len;
        if (1..=max_len).contains(&len) && after + 8 <= data.len() {
            if let Some(&i) = by_name.get(&data[p + 2..after]) {
                let backend_type = i32::from_be_bytes([
                    data[after],
                    data[after + 1],
                    data[after + 2],
                    data[after + 3],
                ]);
                let num_options = i32::from_be_bytes([
                    data[after + 4],
                    data[after + 5],
                    data[after + 6],
                    data[after + 7],
                ]);
                if out[i].is_none()
                    && (0..=3).contains(&backend_type)
                    && (0..=64).contains(&num_options)
                {
                    out[i] = Some(p);
                    missing -= 1;
                }
            }
        }
        p += 1;
    }
    out
}

/// Where each state's name starts in the header, in `state_metas` order: inside its
/// `StateMetaInfoSnapshot` when that is found, else at the first occurrence of the name's bytes.
/// The per-state serializer snapshots follow it.
fn state_name_positions(data: &[u8], state_metas: &[KeyedStateMeta]) -> Vec<Option<usize>> {
    meta_info_records(data, state_metas)
        .into_iter()
        .zip(state_metas)
        .map(|(record, meta)| {
            record
                .map(|p| p + 2)
                .or_else(|| find_bytes(data, meta.name.as_bytes(), 0, data.len()))
        })
        .collect()
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
    let limit = data.len();

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

/// Current `KeyedBackendSerializationProxy.VERSION` in Flink 1.20.
pub const KEYED_BACKEND_VERSION: i32 = 6;

/// Header of a keyed-state stream, i.e. what `KeyedBackendSerializationProxy` writes at offset 0
/// of the delegate stream (before any key-group data).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyedStreamHeader {
    /// `KeyedBackendSerializationProxy` format version (6 for Flink 1.20).
    pub version: i32,
    /// When true, **each key-group is an independent Snappy-framed stream**; the header itself is
    /// always uncompressed.
    pub using_key_group_compression: bool,
}

/// Read the keyed-state stream header.
///
/// Layout verified against apache/flink `release-1.20`:
/// `VersionedIOReadableWritable.write` emits `writeInt(getVersion())`, then
/// `KeyedBackendSerializationProxy.write` emits `writeBoolean(usingKeyGroupCompression)` —
/// so the flag is a single byte at offset 4. It is only present from version 4 on
/// (`read()` guards with `if (readVersion >= 4)`); older streams are never compressed.
///
/// `FullSnapshotAsyncWriter.writeKVStateMetaData` writes this proxy straight to the checkpoint
/// output stream, so offset 0 of the delegate stream is the version int.
pub fn read_keyed_stream_header(data: &[u8]) -> Option<KeyedStreamHeader> {
    if data.len() < 5 {
        return None;
    }
    let version = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    // Guard against being handed something that isn't a keyed stream at all.
    if !(1..=KEYED_BACKEND_VERSION).contains(&version) {
        return None;
    }
    let using_key_group_compression = if version >= 4 { data[4] != 0 } else { false };
    Some(KeyedStreamHeader {
        version,
        using_key_group_compression,
    })
}

/// Return one key-group's raw bytes, decompressing them when the stream uses per-key-group
/// compression.
///
/// Flink decorates **each key-group separately**
/// (`FullSnapshotAsyncWriter.writeKVStateData` calls `decorateWithCompression` per key-group,
/// recording the offset on the *undecorated* stream first), so every key-group can be
/// decompressed independently — which is what makes seeking to a single key-group possible.
/// `SnappyStreamCompressionDecorator` uses `SnappyFramedOutputStream`, i.e. the Snappy *framed*
/// format that [`snap::read::FrameDecoder`] reads.
///
/// `buf` is reused across calls so a streaming reader keeps a bounded footprint.
pub fn key_group_block<'a>(
    raw: &'a [u8],
    compressed: bool,
    buf: &'a mut Vec<u8>,
) -> std::io::Result<&'a [u8]> {
    if !compressed {
        return Ok(raw);
    }
    use std::io::Read;
    buf.clear();
    snap::read::FrameDecoder::new(raw).read_to_end(buf)?;
    Ok(buf.as_slice())
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

/// Extract keyed entries from canonical key-group data blocks: every entry of every state of
/// every non-empty key-group (see [`parse_key_group_block`] for the layout).
///
/// Key bytes per entry: `[keyGroupPrefix(1-2 bytes)][serialized key][namespace]`.
///
/// `state_types` maps state index → state type ("VALUE", "MAP", …). Value bytes are returned raw
/// here; typed decoding lives in `parser::value` (the single decode source of truth).
///
/// `num_states` is [`count_states_in_header`]: it bounds the stateIds. Key-groups that cannot be
/// read to their end mark are listed in [`KeyedEntries::malformed`] — with the entries read before
/// the defect kept — instead of being cut short silently.
#[allow(clippy::too_many_arguments)]
pub fn extract_keyed_entries(
    data: &[u8],
    kg: &crate::parser::state_handle::KeyGroupsStateHandle,
    num_states: usize,
    key_type: &str,
    header_size: usize,
    max_parallelism: i32,
    state_types: &[String],
    key_pojo: Option<&crate::parser::pojo::PojoInfo>,
) -> KeyedEntries {
    let kg_prefix_len = key_group_prefix_bytes(max_parallelism);
    let offsets = &kg.offsets;
    // The header is never compressed, so the flag is readable from the same bytes; when it is set
    // each key-group is an independent Snappy-framed stream and must be inflated before parsing.
    // Without this, a savepoint written with snapshot compression decoded as garbage.
    let compressed = read_keyed_stream_header(data).is_some_and(|h| h.using_key_group_compression);
    let mut inflated = Vec::new();

    // Check if offsets are valid: at least one offset points past the header
    let has_valid_offsets = offsets
        .iter()
        .any(|&o| o > 0 && (o as usize) >= header_size && (o as usize) < data.len());

    if has_valid_offsets {
        let mut out = KeyedEntries::default();
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

            // No minimum size: Flink records an offset only for a key-group holding entries,
            // so a block too short to hold one is malformed, and the parser reports it.
            if end <= start {
                continue;
            }

            // `i` indexes this subtask's key-group range, so the absolute key-group is
            // `start_key_group + i` (Flink: `KeyGroupRangeOffsets` is built over
            // `computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtask)`).
            // Reporting `i` was only correct for the subtask whose range starts at 0.
            let absolute_kg = (kg.start_key_group + i as i32).max(0) as u16;

            let block = match key_group_block(&data[start..end], compressed, &mut inflated) {
                Ok(b) => b,
                // A key-group that will not inflate is not parsed as garbage, but it is reported.
                Err(_) => {
                    out.malformed.push(MalformedKeyGroup {
                        key_group: absolute_kg,
                        defect: BlockDefect::Decompression,
                    });
                    continue;
                }
            };

            let parsed = parse_key_group_block(
                block,
                absolute_kg,
                &KeyGroupParse {
                    num_states,
                    key_type,
                    kg_prefix_len,
                    key_pojo,
                    state_types,
                    only_state: None,
                },
                &mut |e| out.entries.push(e),
            );
            if let Err(defect) = parsed {
                out.malformed.push(MalformedKeyGroup {
                    key_group: absolute_kg,
                    defect,
                });
            }
        }
        out
    } else {
        KeyedEntries::default()
    }
}

/// What [`extract_keyed_entries`] read from one subtask's keyed stream.
#[derive(Debug, Clone, Default)]
pub struct KeyedEntries {
    pub entries: Vec<KeyedEntry>,
    /// Key-groups that could not be read to their end mark, in key-group order.
    pub malformed: Vec<MalformedKeyGroup>,
}

/// A key-group block that could not be read to its `END_OF_KEY_GROUP_MARK`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedKeyGroup {
    /// Absolute key-group.
    pub key_group: u16,
    pub defect: BlockDefect,
}

impl std::fmt::Display for MalformedKeyGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "key-group {}: {}", self.key_group, self.defect)
    }
}

/// Why a key-group block stopped before its `END_OF_KEY_GROUP_MARK`.
///
/// Every entry before the defect has been emitted. Nothing after it can be located — the next
/// length prefix is wherever the bad one says — so the rest of the key-group is lost, and callers
/// report it instead of passing a truncated read off as a smaller state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockDefect {
    /// The block ends inside the entry or stateId starting at `offset`, or before its end mark.
    Truncated { offset: usize },
    /// A key length that cannot even hold the key-group prefix, or a negative value length.
    BadLength { offset: usize, len: i32 },
    /// A stateId at or beyond the number of states the header registers.
    UnknownStateId {
        offset: usize,
        state_id: u16,
        num_states: usize,
    },
    /// A compressed key-group whose Snappy frame does not decode.
    Decompression,
}

impl std::fmt::Display for BlockDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockDefect::Truncated { offset } => write!(f, "truncated at byte {}", offset),
            BlockDefect::BadLength { offset, len } => {
                write!(f, "invalid length {} at byte {}", len, offset)
            }
            BlockDefect::UnknownStateId {
                offset,
                state_id,
                num_states,
            } => write!(
                f,
                "stateId {} at byte {}, but the header registers {} states",
                state_id, offset, num_states
            ),
            BlockDefect::Decompression => write!(f, "Snappy frame does not decode"),
        }
    }
}

/// What [`parse_key_group_block`] needs to know about the operator's header.
///
/// `only_state` restricts parsing to a single stateId: other states' entries are skipped using
/// their length prefixes without decoding their keys or copying their values, which is what makes
/// exporting one state cheap on a savepoint holding many.
pub struct KeyGroupParse<'a> {
    /// Number of states the header registers, keyed and priority-queue (timer) states together
    /// since they share the stateId space ([`count_states_in_header`]). A stateId at or beyond it
    /// is a [`BlockDefect::UnknownStateId`].
    pub num_states: usize,
    /// Key serializer type, e.g. `String`, `Long`, `Pojo`.
    pub key_type: &'a str,
    /// Key-group prefix width, 1 or 2 bytes depending on maxParallelism.
    pub kg_prefix_len: usize,
    /// Key POJO layout, when the key is a POJO.
    pub key_pojo: Option<&'a crate::parser::pojo::PojoInfo>,
    /// State type per stateId ("VALUE", "MAP", "TIMER", …).
    pub state_types: &'a [String],
    /// Restrict parsing to a single stateId.
    pub only_state: Option<u16>,
}

/// Parse one key-group block (already inflated), emitting every entry through `emit`.
///
/// Layout, verified against apache/flink `release-1.20`:
/// `FullSnapshotAsyncWriter.writeKVStateData` opens the block with `writeShort(kvStateId)`
/// (`FullSnapshotAsyncWriter.java:157`), then writes each entry as two
/// `BytePrimitiveArraySerializer` arrays — `[int len][kgPrefix + key + namespace]`, then
/// `[int len][value]` (`:233-237`). On `key[0]` of the **last** entry of each state's run it sets
/// the metadata-follows flag (`setMetaDataFollowsFlagInKey`, `:177` and `:217`), and only after
/// such an entry comes a `short`: the next stateId (`:200`, `:205`) or `END_OF_KEY_GROUP_MARK`
/// (`:186`, `:221`). `FullSnapshotRestoreOperation.KeyGroupEntriesIterator.next`
/// (`FullSnapshotRestoreOperation.java:298-311`) reads it back the same way: the next `short` is
/// read only `if (hasMetaDataFollowsFlag(key))`.
///
/// So a run ends on that flag and on nothing else. A key-length heuristic cannot replace it: a
/// key over the threshold ended the whole key-group (losing every later entry, of every state),
/// and a bounds check is fooled by a stateId `short` followed by the high bytes of the next key
/// length, which read together as a plausible length.
///
/// Returns `Err` when the block cannot be read to its end mark — see [`BlockDefect`]; the entries
/// read before the defect have been emitted. A run under a stateId the header does not declare
/// is skipped (not emitted), the rest of the block is still read, and the block is then reported
/// as [`BlockDefect::UnknownStateId`].
pub fn parse_key_group_block(
    data: &[u8],
    kg_index: u16,
    ctx: &KeyGroupParse<'_>,
    emit: &mut dyn FnMut(KeyedEntry),
) -> Result<(), BlockDefect> {
    let KeyGroupParse {
        num_states,
        key_type,
        kg_prefix_len,
        key_pojo,
        state_types,
        only_state,
    } = *ctx;
    // Canonical savepoint format (FullSnapshotAsyncWriter): each state with entries gets one run.
    // Runs are NOT in stateId order on the heap backend (HeapKeyValueStateIterator walks a
    // HashMap of states; real blocks show e.g. [2, 0, 1]).

    let mut pos = 0usize;
    let mut state_id = read_be_u16(data, &mut pos)?;
    // A stateId the header does not declare. The run is still delimited by the flag, so it is
    // skipped and the rest of the key-group is read; the defect is reported at the end. (The
    // declared count comes from header heuristics, which may undercount: aborting here would
    // drop good entries.)
    let mut unknown: Option<BlockDefect> = None;

    loop {
        if state_id == END_OF_KEY_GROUP_MARK {
            return unknown.map_or(Ok(()), Err);
        }
        // Keyed and priority-queue states share the stateId space; the header registers them all.
        let known = (state_id as usize) < num_states;
        if !known && unknown.is_none() {
            unknown = Some(BlockDefect::UnknownStateId {
                offset: pos - 2,
                state_id,
                num_states,
            });
        }

        // [int: keyLen][kgPrefix + key + namespace]. Flink always writes the key-group prefix
        // (`CompositeKeySerializationUtils.writeKeyGroup`), whose first byte carries the flag.
        let entry_at = pos;
        let kl = read_be_i32(data, &mut pos)?;
        if kl < kg_prefix_len.max(1) as i32 {
            return Err(BlockDefect::BadLength {
                offset: entry_at,
                len: kl,
            });
        }
        if kl as usize > data.len() - pos {
            return Err(BlockDefect::Truncated { offset: entry_at });
        }
        let key_start = pos;
        let key_end = pos + kl as usize;
        let last_of_run = data[key_start] & FIRST_BIT_IN_BYTE_MASK != 0;

        // Entries of a state we were not asked for are skipped without decoding the key or
        // copying the value: only the length prefixes are followed.
        let wanted = known && only_state.is_none_or(|s| s == state_id);

        // Timer (priority-queue) entries have a different key layout and no value; synthesize
        // the firing timestamp as the value so it can be displayed. Both key readers skip the
        // key-group prefix, so the flag on key[0] never reaches the decoded key.
        let is_timer = state_types
            .get(state_id as usize)
            .is_some_and(|t| t == "TIMER");
        let (key, namespace, timer_value) = if !wanted {
            (String::new(), Vec::new(), None)
        } else if is_timer {
            let (k, ts) = read_timer_key(&data[key_start..key_end], kg_prefix_len, key_type);
            (k, Vec::new(), Some(ts))
        } else {
            let (k, ns) =
                read_key_from_bytes(&data[key_start..key_end], kg_prefix_len, key_type, key_pojo);
            (k, ns, None)
        };
        pos = key_end;

        // [int: valueLen][value]
        let value_at = pos;
        let vl = read_be_i32(data, &mut pos)?;
        if vl < 0 {
            return Err(BlockDefect::BadLength {
                offset: value_at,
                len: vl,
            });
        }
        if vl as usize > data.len() - pos {
            return Err(BlockDefect::Truncated { offset: entry_at });
        }
        if wanted {
            let value = timer_value.unwrap_or_else(|| data[pos..pos + vl as usize].to_vec());
            emit(KeyedEntry {
                state_id,
                key,
                key_group: kg_index,
                namespace,
                value,
            });
        }
        pos += vl as usize;

        if last_of_run {
            state_id = read_be_u16(data, &mut pos)?;
        }
    }
}

/// Read a big-endian `short` at `*pos`, or report the block as truncated there.
fn read_be_u16(data: &[u8], pos: &mut usize) -> Result<u16, BlockDefect> {
    let b = data
        .get(*pos..*pos + 2)
        .ok_or(BlockDefect::Truncated { offset: *pos })?;
    *pos += 2;
    Ok(u16::from_be_bytes([b[0], b[1]]))
}

/// Read a big-endian `int` at `*pos`, or report the block as truncated there.
fn read_be_i32(data: &[u8], pos: &mut usize) -> Result<i32, BlockDefect> {
    let b = data
        .get(*pos..*pos + 4)
        .ok_or(BlockDefect::Truncated { offset: *pos })?;
    *pos += 4;
    Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
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

/// Every non-overlapping occurrence of `needle` in `data`.
fn find_all(data: &[u8], needle: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut s = 0;
    while s < data.len().saturating_sub(needle.len()) {
        match find_bytes(data, needle, s, data.len()) {
            Some(p) => {
                out.push(p);
                s = p + needle.len();
            }
            None => break,
        }
    }
    out
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
    } else if short.contains("CharPrimitiveArraySerializer") {
        "char[]".into()
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
    } else if short.contains("CharSerializer") {
        "Char".into()
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
