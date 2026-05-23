use std::path::Path;

use crate::parser::keyed_state::{
    count_states_in_header, extract_keyed_entries, extract_state_metadata,
    extract_value_serializer_types,
};
use crate::parser::metadata::{Operator, Savepoint, SubtaskState};
use crate::parser::pojo::extract_pojo_infos;
use crate::parser::state_handle::{
    FileStateHandle, KeyGroupsStateHandle, RelativeFileStateHandle, StateHandle, StreamStateHandle,
};

use super::cache::{
    CachedEntry, CachedKeyedState, CachedOperator, CachedPojoField, CachedPojoInfo, CachedState,
    IndexCache, KeyPartition, StateDescriptor, ValueRef,
};

pub fn build_index(
    savepoint: &Savepoint,
    metadata_size: u64,
    metadata_mtime: u64,
    savepoint_dir: &Path,
) -> IndexCache {
    build_index_with_progress(
        savepoint,
        metadata_size,
        metadata_mtime,
        savepoint_dir,
        |_, _, _| {},
    )
}

/// Build index with a progress callback called after each operator.
/// Callback args: (operator_index, operator_name, total_keys_so_far)
pub fn build_index_with_progress<F>(
    savepoint: &Savepoint,
    metadata_size: u64,
    metadata_mtime: u64,
    savepoint_dir: &Path,
    mut on_progress: F,
) -> IndexCache
where
    F: FnMut(usize, &str, usize),
{
    let mut operators = Vec::with_capacity(savepoint.operators.len());
    let mut total_keys = 0usize;

    for (i, op) in savepoint.operators.iter().enumerate() {
        let name = op.display_name();
        on_progress(i, &name, total_keys);
        let cached = build_cached_operator(op, savepoint_dir, |keys_so_far| {
            on_progress(i, &name, total_keys + keys_so_far);
        });
        if let Some(ks) = &cached.keyed_state {
            total_keys += ks.total_keys;
        }
        operators.push(cached);
    }

    IndexCache {
        metadata_size,
        metadata_mtime,
        checkpoint_id: savepoint.checkpoint_id,
        savepoint_version: savepoint.version,
        master_states: savepoint
            .master_states
            .iter()
            .map(|m| format!("{} (v{}, {} bytes)", m.name, m.version, m.data.len()))
            .collect(),
        operators,
    }
}

fn build_cached_operator<F>(op: &Operator, savepoint_dir: &Path, mut on_keys: F) -> CachedOperator
where
    F: FnMut(usize),
{
    let mut states = Vec::new();
    let mut keyed_state: Option<CachedKeyedState> = None;

    // A keyed operator's state is split across subtasks by key group; merge them all so the
    // explorer shows every key, not just the first subtask's.
    for sub in &op.subtask_states {
        collect_operator_states(sub, &mut states);
        if let Some(ks) = build_keyed_state(
            &sub.managed_keyed_state,
            savepoint_dir,
            op.max_parallelism,
            &mut on_keys,
        ) {
            match keyed_state {
                None => keyed_state = Some(ks),
                Some(ref mut acc) => merge_keyed_state(acc, ks),
            }
        }
    }

    // Compute keyed state data size
    let keyed_state_size = op
        .subtask_states
        .iter()
        .map(|sub| keyed_state_handle_size(&sub.managed_keyed_state))
        .sum();

    CachedOperator {
        id: op.id,
        display_name: op.display_name(),
        identifier_name: op.identifier_name.clone(),
        parallelism: op.parallelism,
        max_parallelism: op.max_parallelism,
        subtask_count: op.subtask_states.len(),
        keyed_state_size,
        fully_finished: op.fully_finished,
        has_coordinator_state: op.coordinator_state.is_some(),
        coordinator_info: op
            .coordinator_state
            .as_ref()
            .and_then(|cs| crate::parser::kafka::decode_enumerator_state(cs)),
        states,
        keyed_state,
    }
}

/// Merge one subtask's keyed state into the accumulator. Subtasks own disjoint key groups, so
/// partitions are simply appended; per-state entry counts are summed; descriptors are shared.
fn merge_keyed_state(acc: &mut CachedKeyedState, other: CachedKeyedState) {
    acc.total_keys += other.total_keys;
    for od in &other.descriptors {
        if let Some(d) = acc.descriptors.iter_mut().find(|d| d.name == od.name) {
            d.entry_count += od.entry_count;
        }
    }
    const MAX_STORED_PARTITIONS: usize = 100_000;
    if acc.partitions.len() < MAX_STORED_PARTITIONS {
        let room = MAX_STORED_PARTITIONS - acc.partitions.len();
        acc.partitions
            .extend(other.partitions.into_iter().take(room));
    }
}

/// Collect non-keyed operator states (managed-operator, raw-operator).
fn collect_operator_states(sub: &SubtaskState, states: &mut Vec<CachedState>) {
    if let Some(handle) = &sub.managed_operator_state {
        // Prefer one state per registered operator state (real name + distribution mode +
        // decoded elements); fall back to a generic dump for non-inline handles.
        if !push_operator_stream_states(handle, states) {
            let entries = extract_entries_from_handle(handle);
            if !entries.is_empty() {
                states.push(CachedState {
                    name: format!("managed-operator-subtask{}", sub.subtask_index),
                    state_type: "managed-operator".to_string(),
                    entries,
                });
            }
        }
    }
    if let Some(handle) = &sub.raw_operator_state {
        let entries = extract_entries_from_handle(handle);
        if !entries.is_empty() {
            states.push(CachedState {
                name: format!("raw-operator-subtask{}", sub.subtask_index),
                state_type: "raw-operator".to_string(),
                entries,
            });
        }
    }
}

/// Build one `CachedState` per registered operator state, labelled with its distribution mode
/// (`SPLIT_DISTRIBUTE` / `UNION` / `BROADCAST`) and with decoded element values. Returns false if
/// the handle isn't an inline `OperatorStream` (the caller then falls back to a generic dump).
fn push_operator_stream_states(handle: &StateHandle, states: &mut Vec<CachedState>) -> bool {
    let os = match handle {
        StateHandle::OperatorStream(os) => os,
        _ => return false,
    };
    let data = match &os.delegate_handle {
        StreamStateHandle::ByteStream(bs) => &bs.data,
        _ => return false,
    };
    if os.partition_offsets.is_empty() {
        return false;
    }
    // Every element runs from its offset to the next offset across ALL states (they share the
    // same byte stream, so per-partition "next offset" would over-read into the next state).
    let mut boundaries: Vec<usize> = os
        .partition_offsets
        .iter()
        .flat_map(|p| p.offsets.iter().map(|&o| o as usize))
        .collect();
    boundaries.push(data.len());
    boundaries.sort_unstable();
    boundaries.dedup();

    for part in &os.partition_offsets {
        let is_broadcast = part.mode == 2; // OperatorStateHandle.Mode.BROADCAST
        let mut entries = Vec::with_capacity(part.offsets.len());
        for &offset in &part.offsets {
            let start = (offset as usize).min(data.len());
            let end = boundaries
                .iter()
                .copied()
                .find(|&b| b > start)
                .unwrap_or(data.len())
                .min(data.len());
            let elem = &data[start..end];
            // Kafka connector states get connector-specific decoding; broadcast maps and other
            // operator-list elements are decoded best-effort.
            let label = match part.state_name.as_str() {
                "SourceReaderState" => crate::parser::kafka::decode_source_reader_split(elem),
                "writer_raw_states" => crate::parser::kafka::decode_writer_state(elem),
                "streaming_committer_raw_states" => {
                    crate::parser::kafka::decode_committer_state(elem)
                }
                _ if is_broadcast => decode_broadcast_map(elem),
                _ => None,
            }
            .unwrap_or_else(|| decode_operator_element(elem));
            entries.push(CachedEntry {
                key: label.into_bytes(),
                key_group: 0,
                value_ref: ValueRef::Inline {
                    data: elem.to_vec(),
                },
            });
        }
        states.push(CachedState {
            name: part.state_name.clone(),
            state_type: format!("operator/{}", operator_mode_label(part.mode)),
            entries,
        });
    }
    true
}

/// Flink `OperatorStateHandle.Mode` ordinal → name.
fn operator_mode_label(mode: i32) -> &'static str {
    match mode {
        0 => "SPLIT_DISTRIBUTE",
        1 => "UNION",
        2 => "BROADCAST",
        _ => "?",
    }
}

/// Best-effort decode of a broadcast-state element (`HeapBroadcastState`): `writeInt(size)` then
/// `key,value` pairs. Assumes String key/value (the common case); returns None to fall back to a
/// byte preview when the payload isn't String/String.
fn decode_broadcast_map(bytes: &[u8]) -> Option<String> {
    use crate::parser::java_deser::JavaReader;
    let mut r = JavaReader::new(bytes);
    let size = r.read_int().ok()?;
    if !(0..=100_000).contains(&size) {
        return None;
    }
    let mut pairs = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let k = r.read_string_value().ok()?;
        let v = r.read_string_value().ok()?;
        pairs.push(format!("{:?}: {:?}", k, v));
    }
    // The pairs should consume essentially all the bytes — otherwise it wasn't String/String.
    if (r.position() as usize) < bytes.len().saturating_sub(2) {
        return None;
    }
    Some(format!("{{ {} }}", pairs.join(", ")))
}

/// Best-effort decode of one operator-state element for display. Most operator `ListState`
/// elements are `StringValue`; anything else is shown as a short byte preview.
fn decode_operator_element(bytes: &[u8]) -> String {
    use crate::parser::java_deser::JavaReader;
    if bytes.is_empty() {
        return "(empty)".to_string();
    }
    let mut r = JavaReader::new(bytes);
    if let Ok(s) = r.read_string_value() {
        let consumed = r.position() as usize;
        if !s.is_empty()
            && consumed >= bytes.len().saturating_sub(1)
            && s.chars().all(|c| !c.is_control())
        {
            return format!("\"{}\"", s);
        }
    }
    let preview: String = bytes
        .iter()
        .take(16)
        .map(|b| format!("{:02x}", b))
        .collect();
    format!("[{} bytes] {}", bytes.len(), preview)
}

/// Build structured keyed state from a keyed state handle.
/// Returns None if the handle has no parseable keyed state data.
fn build_keyed_state<F>(
    handle: &StateHandle,
    savepoint_dir: &Path,
    max_parallelism: i32,
    on_keys: &mut F,
) -> Option<CachedKeyedState>
where
    F: FnMut(usize),
{
    let kg = match handle {
        StateHandle::KeyGroups(kg) => kg,
        StateHandle::Null => return None,
        _ => return None,
    };

    // Unwrap delegate: may be direct ByteStream/Relative, or nested KeyGroups
    let effective_delegate = match &kg.delegate_handle {
        StreamStateHandle::KeyGroups(nested) => &nested.delegate_handle,
        other => other,
    };
    let data: Vec<u8> = match effective_delegate {
        StreamStateHandle::ByteStream(bs) => bs.data.clone(),
        StreamStateHandle::Relative(rel) => {
            let file_path = savepoint_dir.join(&rel.relative_path);
            std::fs::read(&file_path).unwrap_or_default()
        }
        _ => return None,
    };

    if data.is_empty() {
        return None;
    }

    let state_metas = extract_state_metadata(&data);
    if state_metas.is_empty() {
        return None;
    }

    let pojo_infos = extract_pojo_infos(&data);
    let value_types = extract_value_serializer_types(&data);
    let value_pojo_classes = crate::parser::keyed_state::extract_value_pojo_classes(&data);
    let num_states = count_states_in_header(&data).max(state_metas.len()).max(1);
    let key_type = state_metas
        .first()
        .map(|m| m.key_type.as_str())
        .unwrap_or("String");
    let header_size = header_size_from_offsets(&kg.offsets, data.len());
    let state_type_strs: Vec<String> = state_metas.iter().map(|m| m.state_type.clone()).collect();

    // Extract POJO info for the key serializer (if key is a POJO)
    let key_pojo = if key_type == "Pojo" {
        crate::parser::keyed_state::extract_key_pojo_info(&data)
    } else {
        None
    };

    let keyed_entries = extract_keyed_entries(
        &data,
        &kg.offsets,
        num_states,
        key_type,
        header_size,
        max_parallelism,
        &state_type_strs,
        key_pojo.as_ref(),
    );

    // Build state descriptors (pure data, no formatting).
    // Associate each state with its value POJO *by class name*: `extract_pojo_infos`
    // deduplicates POJOs by class, so a positional match would under-count when several states
    // share a POJO type. `extract_value_pojo_classes` gives the value POJO class per state.
    let pojo_by_class: std::collections::HashMap<&str, &crate::parser::pojo::PojoInfo> = pojo_infos
        .iter()
        .map(|p| (p.class_name.as_str(), p))
        .collect();
    let descriptors: Vec<StateDescriptor> = state_metas
        .iter()
        .enumerate()
        .map(|(idx, meta)| {
            // Timer entries carry a synthesized firing timestamp (epoch millis) as their value,
            // and no POJO (a nearby PojoSerializerSnapshot would otherwise be matched spuriously).
            let is_timer = meta.state_type == "TIMER";
            let vtype = if is_timer {
                "Date".to_string()
            } else {
                value_types.get(idx).cloned().unwrap_or_default()
            };
            let matched_pojo = if is_timer {
                None
            } else {
                value_pojo_classes
                    .get(idx)
                    .and_then(|o| o.as_ref())
                    .and_then(|cls| pojo_by_class.get(cls.as_str()).copied())
            };
            let pojo_detail = matched_pojo
                .map(|p| {
                    let leaf = p.leaf_fields("");
                    let fields: Vec<String> = leaf
                        .iter()
                        .map(|(path, t)| format!("{}: {}", path, t))
                        .collect();
                    // Show the fully-qualified value class (package + name), then the fields.
                    format!(" → {} {{ {} }}", p.class_name, fields.join(", "))
                })
                .unwrap_or_default();
            let pojo_info = matched_pojo.map(|p| pojo_to_cached(p));
            // Count entries for this state_id
            let entry_count = keyed_entries
                .iter()
                .filter(|e| e.state_id as usize == idx)
                .count();
            StateDescriptor {
                name: meta.name.clone(),
                state_type: meta.state_type.clone(),
                value_type: vtype,
                pojo_detail,
                pojo_info,
                entry_count,
            }
        })
        .collect();

    // Group entries by key string → KeyPartition
    // Each unique key becomes a partition with its state values.
    // For MAP states, multiple entries share the same (key, state_id) — each is
    // one map pair. We accumulate them into a single blob.
    let map_state_ids: std::collections::HashSet<usize> = descriptors
        .iter()
        .enumerate()
        .filter(|(_, d)| d.state_type == "MAP")
        .map(|(i, _)| i)
        .collect();

    // (key_group, values_per_state, map_pairs_per_state[(namespace, value)])
    let mut partitions_map: std::collections::BTreeMap<
        String,
        (u16, Vec<Option<Vec<u8>>>, Vec<Vec<(Vec<u8>, Vec<u8>)>>),
    > = std::collections::BTreeMap::new();

    let num_descriptors = descriptors.len();
    for (ei, e) in keyed_entries.iter().enumerate() {
        let idx = e.state_id as usize;
        // Skip entries with state_id beyond known descriptors
        if idx >= num_descriptors {
            continue;
        }
        let entry = partitions_map.entry(e.key.clone()).or_insert_with(|| {
            let map_entries = vec![Vec::new(); num_descriptors];
            (e.key_group, vec![None; num_descriptors], map_entries)
        });
        if map_state_ids.contains(&idx) {
            // MAP state: accumulate each entry as (namespace=mapKey, value)
            entry.2[idx].push((e.namespace.clone(), e.value.clone()));
        } else if entry.1[idx].is_none() && !e.value.is_empty() {
            // VALUE/LIST state: store first value only
            entry.1[idx] = Some(e.value.clone());
        }
        if ei % 5000 == 0 && ei > 0 {
            on_keys(partitions_map.len());
        }
    }
    on_keys(partitions_map.len());

    // Build final partitions, merging accumulated MAP entries into blobs
    // Format: int(numPairs) + for each pair: int(nsLen) + nsBytes + int(valLen) + valBytes
    for (_, (_, values, map_entries)) in partitions_map.iter_mut() {
        for idx in &map_state_ids {
            if *idx < map_entries.len() && !map_entries[*idx].is_empty() {
                let pairs = &map_entries[*idx];
                let count = pairs.len() as i32;
                let mut blob = Vec::new();
                blob.extend_from_slice(&count.to_be_bytes());
                for (ns, val) in pairs {
                    blob.extend_from_slice(&(ns.len() as i32).to_be_bytes());
                    blob.extend_from_slice(ns);
                    blob.extend_from_slice(&(val.len() as i32).to_be_bytes());
                    blob.extend_from_slice(val);
                }
                values[*idx] = Some(blob);
            }
        }
    }

    let total_keys = partitions_map.len();
    // Limit stored partitions to avoid huge memory/cache for operators with millions of keys
    const MAX_STORED_PARTITIONS: usize = 100_000;
    let partitions: Vec<KeyPartition> = partitions_map
        .into_iter()
        .take(MAX_STORED_PARTITIONS)
        .map(|(key, (kg, values, _))| KeyPartition {
            key,
            key_group: kg,
            values,
        })
        .collect();

    Some(CachedKeyedState {
        descriptors,
        partitions,
        total_keys,
        key_type: key_type.to_string(),
    })
}

/// Derive header size from the key-group offsets array.
///
/// The smallest offset > 0 is where the first key-group data block starts,
/// which is exactly the byte after the header. This is far more reliable
/// than scanning for marker strings in the header.
///
/// Falls back to `data_len` (= no key-group data) if all offsets are 0.
fn pojo_to_cached(p: &crate::parser::pojo::PojoInfo) -> CachedPojoInfo {
    CachedPojoInfo {
        class_name: p.class_name.clone(),
        fields: p
            .fields
            .iter()
            .map(|f| CachedPojoField {
                name: f.name.clone(),
                type_name: f.type_name.clone(),
                nested: f.nested_pojo.as_ref().map(|n| Box::new(pojo_to_cached(n))),
            })
            .collect(),
    }
}

fn keyed_state_handle_size(handle: &StateHandle) -> u64 {
    match handle {
        StateHandle::KeyGroups(kg) => match &kg.delegate_handle {
            StreamStateHandle::ByteStream(bs) => bs.data.len() as u64,
            StreamStateHandle::Relative(rel) => rel.state_size as u64,
            StreamStateHandle::File(f) => f.state_size as u64,
            StreamStateHandle::KeyGroups(nested) => match &nested.delegate_handle {
                StreamStateHandle::Relative(rel) => rel.state_size as u64,
                StreamStateHandle::ByteStream(bs) => bs.data.len() as u64,
                _ => 0,
            },
            _ => 0,
        },
        _ => 0,
    }
}

fn header_size_from_offsets(offsets: &[i64], data_len: usize) -> usize {
    offsets
        .iter()
        .filter(|&&o| o > 0)
        .min()
        .map(|&o| (o as usize).min(data_len))
        .unwrap_or(data_len)
}

fn extract_entries_from_handle(handle: &StateHandle) -> Vec<CachedEntry> {
    match handle {
        StateHandle::Null => Vec::new(),
        StateHandle::KeyGroups(kg) => entries_from_key_groups(kg),
        StateHandle::IncrementalRemoteKeyed(inc) => {
            let mut entries = Vec::new();
            for item in &inc.shared_state {
                if let Some(vr) = value_ref_from_stream(&item.handle) {
                    entries.push(CachedEntry {
                        key: item.local_path.as_bytes().to_vec(),
                        key_group: 0,
                        value_ref: vr,
                    });
                }
            }
            for item in &inc.private_state {
                if let Some(vr) = value_ref_from_stream(&item.handle) {
                    entries.push(CachedEntry {
                        key: item.local_path.as_bytes().to_vec(),
                        key_group: 0,
                        value_ref: vr,
                    });
                }
            }
            entries
        }
        StateHandle::OperatorStream(os) => {
            let mut entries = Vec::new();
            for part in &os.partition_offsets {
                for (i, offset) in part.offsets.iter().enumerate() {
                    // Create entry for each partition offset
                    let vr = match &os.delegate_handle {
                        StreamStateHandle::ByteStream(bs) => {
                            // Extract slice from inline data at offset
                            let start = *offset as usize;
                            let end = part
                                .offsets
                                .get(i + 1)
                                .map(|&o| o as usize)
                                .unwrap_or(bs.data.len());
                            if start < bs.data.len() {
                                let slice = &bs.data[start..end.min(bs.data.len())];
                                Some(ValueRef::Inline {
                                    data: slice.to_vec(),
                                })
                            } else {
                                None
                            }
                        }
                        other => value_ref_from_stream(other).map(|vr| match vr {
                            ValueRef::File { path, .. } => ValueRef::File {
                                path,
                                offset: *offset as u64,
                            },
                            inline => inline,
                        }),
                    };
                    if let Some(vr) = vr {
                        entries.push(CachedEntry {
                            key: format!("{}[{}]", part.state_name, i).into_bytes(),
                            key_group: 0,
                            value_ref: vr,
                        });
                    }
                }
            }
            entries
        }
        StateHandle::Changelog(cl) => {
            let mut entries = Vec::new();
            for base in &cl.base_state {
                entries.extend(extract_entries_from_handle(base));
            }
            entries
        }
        StateHandle::ChangelogByteIncrement(_) | StateHandle::ChangelogFileIncrement(_) => {
            Vec::new()
        }
    }
}

fn entries_from_key_groups(kg: &KeyGroupsStateHandle) -> Vec<CachedEntry> {
    // Extract real state names and types from the inline data header
    let state_metas = match &kg.delegate_handle {
        StreamStateHandle::ByteStream(bs) => extract_state_metadata(&bs.data),
        _ => Vec::new(),
    };

    let mut entries = Vec::new();

    // Extract POJO infos for value type details
    let pojo_infos = match &kg.delegate_handle {
        StreamStateHandle::ByteStream(bs) => extract_pojo_infos(&bs.data),
        _ => Vec::new(),
    };

    // If we found state metadata, create one entry per state with type info
    if !state_metas.is_empty() {
        for (idx, meta) in state_metas.iter().enumerate() {
            // Build label with state type and POJO details if available
            let pojo_detail = pojo_infos.get(idx).map(|p| {
                let leaf = p.leaf_fields("");
                let fields: Vec<String> = leaf
                    .iter()
                    .map(|(path, t)| format!("{}: {}", path, t))
                    .collect();
                format!(
                    " → {} ({}) {{ {} }}",
                    p.short_name,
                    p.class_name,
                    fields.join(", ")
                )
            });
            let label = format!(
                "{} [{}] (key: {}){}",
                meta.name,
                meta.state_type,
                meta.key_type,
                pojo_detail.as_deref().unwrap_or("")
            );
            entries.push(CachedEntry {
                key: label.into_bytes(),
                key_group: 0,
                value_ref: match &kg.delegate_handle {
                    StreamStateHandle::ByteStream(bs) => ValueRef::Inline {
                        data: bs.data.clone(),
                    },
                    other => value_ref_from_stream(other)
                        .unwrap_or(ValueRef::Inline { data: Vec::new() }),
                },
            });
        }
    } else {
        // Fallback: one entry per key group
        for (i, offset) in kg.offsets.iter().enumerate() {
            let key_group = (kg.start_key_group + i as i32) as u16;
            let next_offset =
                kg.offsets
                    .get(i + 1)
                    .copied()
                    .unwrap_or_else(|| match &kg.delegate_handle {
                        StreamStateHandle::ByteStream(bs) => bs.data.len() as i64,
                        _ => *offset + 1,
                    });

            let vr = match &kg.delegate_handle {
                StreamStateHandle::ByteStream(bs) => {
                    let start = *offset as usize;
                    let end = (next_offset as usize).min(bs.data.len());
                    if start < bs.data.len() && start < end {
                        Some(ValueRef::Inline {
                            data: bs.data[start..end].to_vec(),
                        })
                    } else {
                        Some(ValueRef::Inline { data: Vec::new() })
                    }
                }
                other => value_ref_from_stream(other).map(|vr| match vr {
                    ValueRef::File { path, .. } => ValueRef::File {
                        path,
                        offset: *offset as u64,
                    },
                    inline => inline,
                }),
            };

            if let Some(vr) = vr {
                entries.push(CachedEntry {
                    key: format!("kg-{}", key_group).into_bytes(),
                    key_group,
                    value_ref: vr,
                });
            }
        }
    }

    entries
}

fn value_ref_from_stream(handle: &StreamStateHandle) -> Option<ValueRef> {
    match handle {
        StreamStateHandle::File(FileStateHandle { file_path, .. }) => Some(ValueRef::File {
            path: file_path.clone(),
            offset: 0,
        }),
        StreamStateHandle::Relative(RelativeFileStateHandle { relative_path, .. }) => {
            Some(ValueRef::File {
                path: relative_path.clone(),
                offset: 0,
            })
        }
        StreamStateHandle::Segment(seg) => Some(ValueRef::File {
            path: seg.file_path.clone(),
            offset: seg.start_pos as u64,
        }),
        StreamStateHandle::KeyGroups(kg) => value_ref_from_stream(&kg.delegate_handle),
        StreamStateHandle::ByteStream(bs) => Some(ValueRef::Inline {
            data: bs.data.clone(),
        }),
        StreamStateHandle::Null | StreamStateHandle::EmptySegment => None,
    }
}
