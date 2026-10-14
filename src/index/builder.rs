use std::borrow::Cow;
use std::path::Path;

use crate::parser::keyed_state::{
    count_states_in_header, extract_key_pojo_info, extract_keyed_entries, extract_state_metadata,
    extract_value_pojo_classes, extract_value_serializer_types,
};
use crate::parser::metadata::{Operator, Savepoint, SubtaskState};
use crate::parser::operator_state::OperatorStreamElements;
use crate::parser::pojo::extract_pojo_infos;
use crate::parser::state_handle::{
    FileStateHandle, KeyGroupsStateHandle, OperatorStreamStateHandle, RelativeFileStateHandle,
    StateHandle, StreamStateHandle,
};

use super::cache::{
    CachedEntry, CachedKeyedState, CachedOperator, CachedPojoField, CachedPojoInfo, CachedState,
    IndexCache, KeyPartition, StateDescriptor, ValueRef,
};

/// The index keeps at most this many keys per operator, so that an operator with millions of keys
/// does not hold every raw value in memory and in the cache file. `CachedKeyedState::total_keys`
/// still counts every key, the kept ones are spread across subtasks (`spread_quotas`), and the
/// TUI and the `E` export say when they show fewer keys than the savepoint holds.
/// `--export-state` streams every key whatever this cap is.
pub const MAX_STORED_PARTITIONS: usize = 100_000;

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

/// [`build_index`] with an explicit per-operator key cap instead of [`MAX_STORED_PARTITIONS`], so
/// that tests can exercise the cap without writing 100 000+ keys.
pub fn build_index_with_cap(
    savepoint: &Savepoint,
    metadata_size: u64,
    metadata_mtime: u64,
    savepoint_dir: &Path,
    max_stored_partitions: usize,
) -> IndexCache {
    build_index_capped(
        savepoint,
        metadata_size,
        metadata_mtime,
        savepoint_dir,
        max_stored_partitions,
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
    on_progress: F,
) -> IndexCache
where
    F: FnMut(usize, &str, usize),
{
    build_index_capped(
        savepoint,
        metadata_size,
        metadata_mtime,
        savepoint_dir,
        MAX_STORED_PARTITIONS,
        on_progress,
    )
}

fn build_index_capped<F>(
    savepoint: &Savepoint,
    metadata_size: u64,
    metadata_mtime: u64,
    savepoint_dir: &Path,
    max_stored_partitions: usize,
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
        let cached =
            build_cached_operator(op, savepoint_dir, max_stored_partitions, |keys_so_far| {
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
        cache_format: crate::index::cache::CACHE_FORMAT,
    }
}

fn build_cached_operator<F>(
    op: &Operator,
    savepoint_dir: &Path,
    max_stored_partitions: usize,
    mut on_keys: F,
) -> CachedOperator
where
    F: FnMut(usize),
{
    let mut states = Vec::new();
    let mut keyed_state: Option<CachedKeyedState> = None;
    let mut load_errors = Vec::new();
    // The keys kept from each subtask so far, in subtask order. Trimmed again after every
    // subtask, so that no more than `max_stored_partitions` are held from one to the next.
    let mut kept: Vec<Vec<KeyPartition>> = Vec::new();

    // A keyed operator's state is split across subtasks by key group; merge them all. Up to the
    // cap every key is kept; past it, `spread_quotas` gives each subtask an equal share.
    for sub in &op.subtask_states {
        collect_operator_states(sub, savepoint_dir, &mut states);
        match build_keyed_state(
            &sub.managed_keyed_state,
            savepoint_dir,
            op.max_parallelism,
            max_stored_partitions,
            &mut on_keys,
        ) {
            Ok(Some(mut ks)) => {
                kept.push(std::mem::take(&mut ks.partitions));
                let available: Vec<usize> = kept.iter().map(Vec::len).collect();
                for (partitions, quota) in kept
                    .iter_mut()
                    .zip(spread_quotas(&available, max_stored_partitions))
                {
                    partitions.truncate(quota);
                }
                match keyed_state {
                    None => keyed_state = Some(ks),
                    Some(ref mut acc) => merge_keyed_state(acc, ks),
                }
            }
            Ok(None) => {}
            // Recorded rather than skipped: the other subtasks still load, and the TUI says the
            // key set is partial instead of passing it off as complete.
            Err(e) => load_errors.push(e),
        }
    }
    if let Some(ks) = keyed_state.as_mut() {
        ks.partitions = kept.into_iter().flatten().collect();
        // One sorted list rather than one sorted run per subtask, so paging reads in order.
        ks.partitions.sort_unstable_by(|a, b| a.key.cmp(&b.key));
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
        load_errors,
    }
}

/// Merge one subtask's keyed state into the accumulator. Subtasks own disjoint key groups, so key
/// counts and per-state entry counts are summed; descriptors are shared. Partitions are not merged
/// here: `build_cached_operator` takes them from every subtask, with [`spread_quotas`].
fn merge_keyed_state(acc: &mut CachedKeyedState, other: CachedKeyedState) {
    acc.total_keys += other.total_keys;
    acc.malformed_key_groups += other.malformed_key_groups;
    if acc.first_malformed.is_none() {
        acc.first_malformed = other.first_malformed;
    }
    for od in &other.descriptors {
        if let Some(d) = acc.descriptors.iter_mut().find(|d| d.name == od.name) {
            d.entry_count += od.entry_count;
        }
    }
}

/// How many of their keys the subtasks may keep when `cap` keys are stored in all, given that
/// subtask `i` holds `available[i]`: an equal share each, where a subtask with fewer keys than its
/// share keeps them all and leaves the rest to the others (max-min fairness). Every subtask that
/// has keys therefore keeps some — at least one while `cap` is no smaller than the number of such
/// subtasks — instead of the first subtasks filling the cap. A quota never exceeds
/// `available[i]`, so applying this again after each new subtask only ever trims what is held.
fn spread_quotas(available: &[usize], cap: usize) -> Vec<usize> {
    if available.iter().sum::<usize>() <= cap {
        return available.to_vec();
    }
    let filled = |level: usize| available.iter().map(|&a| a.min(level)).sum::<usize>();
    // The highest level that fits: filled(level) <= cap < filled(level + 1).
    let (mut level, mut hi) = (0usize, available.iter().copied().max().unwrap_or(0));
    while level < hi {
        let mid = level + (hi - level).div_ceil(2);
        if filled(mid) <= cap {
            level = mid;
        } else {
            hi = mid - 1;
        }
    }
    let mut quotas: Vec<usize> = available.iter().map(|&a| a.min(level)).collect();
    // What is left is smaller than the number of subtasks above the level (that is what
    // `cap < filled(level + 1)` says): one more key each to the first of them.
    let mut left = cap - filled(level);
    for (quota, &a) in quotas.iter_mut().zip(available) {
        if left == 0 {
            break;
        }
        if a > level {
            *quota += 1;
            left -= 1;
        }
    }
    quotas
}

/// Collect non-keyed operator states (managed-operator, raw-operator).
fn collect_operator_states(
    sub: &SubtaskState,
    savepoint_dir: &Path,
    states: &mut Vec<CachedState>,
) {
    // Managed operator state is always an `OperatorStreamStateHandle` (or nothing).
    if let Some(StateHandle::OperatorStream(os)) = &sub.managed_operator_state {
        push_operator_stream_states(os, savepoint_dir, states);
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
/// (`SPLIT_DISTRIBUTE` / `UNION` / `BROADCAST`) and with one entry per element, holding its
/// decoded value. The stream is read wherever its delegate keeps it and inflated when written
/// with snapshot compression (`parser::operator_state`); if it cannot be read, every state with
/// elements gets a single `[error: …]` entry saying why.
fn push_operator_stream_states(
    os: &OperatorStreamStateHandle,
    savepoint_dir: &Path,
    states: &mut Vec<CachedState>,
) {
    // The stream is only read when some state has elements in it: a handle whose states are
    // all empty may point at nothing readable.
    let has_elements = os.partition_offsets.iter().any(|p| !p.offsets.is_empty());
    let stream = if has_elements {
        operator_stream_bytes(&os.delegate_handle, savepoint_dir)
    } else {
        Ok(Cow::Borrowed(&[][..]))
    };
    let elements = stream
        .as_deref()
        .map_err(String::clone)
        .and_then(|bytes| OperatorStreamElements::new(bytes, &os.partition_offsets));

    for part in &os.partition_offsets {
        let is_broadcast = part.mode == 2; // OperatorStateHandle.Mode.BROADCAST
        let entries = match &elements {
            _ if part.offsets.is_empty() => Vec::new(),
            Err(e) => vec![unread_entry(format!("[error: {}]", e))],
            Ok(elements) => part
                .offsets
                .iter()
                .map(|&offset| match elements.element(offset) {
                    Ok(elem) => CachedEntry {
                        key: label_operator_element(&part.state_name, is_broadcast, &elem)
                            .into_bytes(),
                        key_group: 0,
                        // Kept for the TUI's hex view (`x`): a partly decoded element is
                        // checked against its bytes. Inflated when the stream is compressed, and
                        // not bounded by the label (`[N bytes] <preview>` for any N).
                        value_ref: ValueRef::Inline {
                            data: elem.to_vec(),
                        },
                    },
                    Err(e) => unread_entry(format!("[error: {}]", e)),
                })
                .collect(),
        };
        states.push(CachedState {
            name: part.state_name.clone(),
            state_type: format!("operator/{}", operator_mode_label(part.mode)),
            entries,
        });
    }
}

/// An operator-state stream's bytes: borrowed when inline in `_metadata`, read otherwise.
fn operator_stream_bytes<'a>(
    delegate: &'a StreamStateHandle,
    savepoint_dir: &Path,
) -> Result<Cow<'a, [u8]>, String> {
    match delegate {
        StreamStateHandle::ByteStream(bs) => Ok(Cow::Borrowed(&bs.data)),
        other => crate::export_state::read_whole_stream(other, savepoint_dir).map(Cow::Owned),
    }
}

/// Kafka connector states get connector-specific decoding; broadcast maps and other
/// operator-list elements are decoded best-effort.
fn label_operator_element(state_name: &str, is_broadcast: bool, elem: &[u8]) -> String {
    match state_name {
        "SourceReaderState" => crate::parser::kafka::decode_source_reader_split(elem),
        "writer_raw_states" => crate::parser::kafka::decode_writer_state(elem),
        "streaming_committer_raw_states" => crate::parser::kafka::decode_committer_state(elem),
        _ if is_broadcast => decode_broadcast_map(elem),
        _ => None,
    }
    .unwrap_or_else(|| decode_operator_element(elem))
}

/// An operator-state entry whose element could not be read: the error label is all there is.
fn unread_entry(label: String) -> CachedEntry {
    CachedEntry {
        key: label.into_bytes(),
        key_group: 0,
        value_ref: ValueRef::Decoded,
    }
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
/// Returns `Ok(None)` if the handle has no parseable keyed state data, and `Err` — naming the
/// file — if its state file is missing, unreadable or shorter than the handle records.
fn build_keyed_state<F>(
    handle: &StateHandle,
    savepoint_dir: &Path,
    max_parallelism: i32,
    max_stored_partitions: usize,
    on_keys: &mut F,
) -> Result<Option<CachedKeyedState>, String>
where
    F: FnMut(usize),
{
    let kg = match handle {
        StateHandle::KeyGroups(kg) => kg,
        StateHandle::Null => return Ok(None),
        _ => return Ok(None),
    };

    // Unwrap delegate: may be direct ByteStream/Relative, or nested KeyGroups
    let effective_delegate = match &kg.delegate_handle {
        StreamStateHandle::KeyGroups(nested) => &nested.delegate_handle,
        other => other,
    };
    let data: Vec<u8> = match effective_delegate {
        StreamStateHandle::ByteStream(bs) => bs.data.clone(),
        StreamStateHandle::Relative(rel) => {
            crate::state_file::read(&savepoint_dir.join(&rel.relative_path), rel.state_size)?
        }
        StreamStateHandle::File(f) => {
            crate::state_file::read(&crate::state_file::local_path(&f.file_path)?, f.state_size)?
        }
        // File-merging segments are only read by the streaming export; say so rather than show
        // the operator as if it had no keys.
        StreamStateHandle::Segment(seg) => {
            return Err(format!(
                "{}: segment state handle (file merging) is not read by the index; use \
                 --export-state",
                seg.file_path
            ))
        }
        _ => return Ok(None),
    };

    if data.is_empty() {
        return Ok(None);
    }
    let what = match effective_delegate {
        StreamStateHandle::Relative(rel) => {
            savepoint_dir.join(&rel.relative_path).display().to_string()
        }
        StreamStateHandle::File(f) => f.file_path.clone(),
        _ => "inline keyed state".to_string(),
    };
    crate::state_file::check_offsets(&what, &kg.offsets, kg.start_key_group, data.len() as u64)?;

    // The header ends where the first key-group begins. Every header extractor gets that slice
    // and nothing more: handed the whole state file they scanned key-group bytes as if they were
    // serializer snapshots (and `extract_pojo_infos`, uncapped, scanned every byte of it).
    let header_size = header_size_from_offsets(&kg.offsets, data.len());
    let header = &data[..header_size];

    let state_metas = extract_state_metadata(header);
    if state_metas.is_empty() {
        return Ok(None);
    }

    let pojo_infos = extract_pojo_infos(header);
    let value_types = extract_value_serializer_types(header, &state_metas);
    let value_pojo_classes = extract_value_pojo_classes(header, &state_metas);
    let num_states = count_states_in_header(header, &state_metas);
    let key_type = state_metas
        .first()
        .map(|m| m.key_type.as_str())
        .unwrap_or("String");
    let state_type_strs: Vec<String> = state_metas.iter().map(|m| m.state_type.clone()).collect();

    // Extract POJO info for the key serializer (if key is a POJO)
    let key_pojo = if key_type == "Pojo" {
        extract_key_pojo_info(header)
    } else {
        None
    };

    let crate::parser::keyed_state::KeyedEntries {
        entries: keyed_entries,
        malformed,
    } = extract_keyed_entries(
        &data,
        kg,
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
    // Keep at most the cap from this subtask (its smallest keys); `build_cached_operator` then
    // shares the cap between subtasks. `total_keys` still counts every key.
    let partitions: Vec<KeyPartition> = partitions_map
        .into_iter()
        .take(max_stored_partitions)
        .map(|(key, (kg, values, _))| KeyPartition {
            key,
            key_group: kg,
            values,
        })
        .collect();

    Ok(Some(CachedKeyedState {
        descriptors,
        partitions,
        total_keys,
        key_type: key_type.to_string(),
        malformed_key_groups: malformed.len(),
        first_malformed: malformed.first().map(|m| m.to_string()),
    }))
}

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
            // Flink records -1 when it could not tell the size: count it as unknown (0).
            StreamStateHandle::Relative(rel) => rel.state_size.max(0) as u64,
            StreamStateHandle::File(f) => f.state_size.max(0) as u64,
            StreamStateHandle::KeyGroups(nested) => match &nested.delegate_handle {
                StreamStateHandle::Relative(rel) => rel.state_size.max(0) as u64,
                StreamStateHandle::ByteStream(bs) => bs.data.len() as u64,
                _ => 0,
            },
            _ => 0,
        },
        _ => 0,
    }
}

/// Derive header size from the key-group offsets array.
///
/// The smallest offset > 0 is where the first key-group data block starts,
/// which is exactly the byte after the header. This is far more reliable
/// than scanning for marker strings in the header.
///
/// Falls back to `data_len` (= no key-group data) if all offsets are 0.
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
    // Extract real state names and types from the inline data header (only the header: it ends
    // where the first key-group starts).
    let header = match &kg.delegate_handle {
        StreamStateHandle::ByteStream(bs) => {
            &bs.data[..header_size_from_offsets(&kg.offsets, bs.data.len())]
        }
        _ => &[][..],
    };
    let state_metas = extract_state_metadata(header);

    let mut entries = Vec::new();

    // Extract POJO infos for value type details
    let pojo_infos = extract_pojo_infos(header);

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

#[cfg(test)]
mod tests {
    use super::spread_quotas;

    #[test]
    fn quotas_keep_everything_under_the_cap() {
        assert_eq!(spread_quotas(&[3, 0, 5], 10), vec![3, 0, 5]);
        assert_eq!(spread_quotas(&[], 10), Vec::<usize>::new());
    }

    #[test]
    fn quotas_share_the_cap_between_subtasks() {
        // Equal shares; the remainder goes to the first subtasks above the level.
        assert_eq!(spread_quotas(&[100, 100, 100], 10), vec![4, 3, 3]);
        // A small subtask keeps all of its keys and leaves its unused share to the others.
        assert_eq!(spread_quotas(&[2, 100, 100], 10), vec![2, 4, 4]);
        // Subtask 0 no longer fills the cap on its own (#13).
        assert_eq!(spread_quotas(&[10, 10], 10), vec![5, 5]);
        // Empty subtasks get nothing; every other one keeps at least one key.
        assert_eq!(spread_quotas(&[0, 1000, 0, 1], 3), vec![0, 2, 0, 1]);
    }

    #[test]
    fn quotas_fill_the_cap_exactly_and_never_exceed_what_is_held() {
        let cases: [&[usize]; 5] = [
            &[7, 3, 9, 1, 0, 12],
            &[1, 1, 1, 1],
            &[500, 581],
            &[5, 1000, 3, 999, 0, 2],
            &[0, 0, 4],
        ];
        for available in cases {
            let total: usize = available.iter().sum();
            let holders = available.iter().filter(|&&a| a > 0).count();
            for cap in 0..=total + 2 {
                let quotas = spread_quotas(available, cap);
                assert_eq!(
                    quotas.iter().sum::<usize>(),
                    cap.min(total),
                    "{available:?} cap {cap}"
                );
                for (q, a) in quotas.iter().zip(available) {
                    assert!(q <= a, "{available:?} cap {cap}: {quotas:?}");
                    if *a > 0 && cap >= holders {
                        assert!(*q > 0, "{available:?} cap {cap}: {quotas:?}");
                    }
                }

                // As `build_cached_operator` uses it: one subtask at a time, each trimming what
                // the previous ones kept. The cap is still filled, and still from every subtask.
                let mut held: Vec<usize> = Vec::new();
                for &a in available {
                    held.push(a.min(cap));
                    held = spread_quotas(&held, cap);
                }
                assert_eq!(held.iter().sum::<usize>(), cap.min(total));
                for (h, a) in held.iter().zip(available) {
                    assert!(h <= a);
                    if *a > 0 && cap >= holders {
                        assert!(*h > 0, "{available:?} cap {cap}: incremental {held:?}");
                    }
                }
            }
        }
    }
}
