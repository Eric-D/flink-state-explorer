//! Synthetic Flink 1.20 canonical savepoints, written byte for byte in Flink's layout so that
//! file-backed, multi-subtask and compressed read paths can be tested without Docker.
//!
//! What is reproduced (each verified against Flink release-1.20 and against real savepoints of
//! `test-flink-job`; `tests/synth_savepoint_test.rs` compares the header byte for byte with one
//! Flink wrote, kept in `tests/data/`):
//! - `_metadata` v4 (`MetadataV4Serializer`): magic, version, checkpoint id, no master state,
//!   operators with their subtask states; keyed state as a `KeyGroupsSavepointStateHandle`
//!   (tag 7) whose delegate is either inline (`ByteStreamStateHandle`, tag 1) or a file next to
//!   `_metadata` (`RelativeFileStateHandle`, tag 6) — the layout of production savepoints; then
//!   the Java-serialized `CheckpointProperties` v4 appends (`MetadataV4Serializer.serialize`),
//!   copied verbatim from a real canonical savepoint (`tests/data/`);
//! - the keyed-state stream header (`KeyedBackendSerializationProxy` v6): compression flag, key
//!   serializer snapshot, one `StateMetaInfoSnapshot` per state, keyed states first and timer
//!   (priority-queue) states after, which fixes the shared stateId space;
//! - key-group blocks (`FullSnapshotAsyncWriter`): `short stateId`, entries
//!   `[int keyLen][kgPrefix + key + namespace][int valueLen][value]`, the metadata-follows flag
//!   (`0x80` on `key[0]`) on the last entry of each state's run, then `END_OF_KEY_GROUP_MARK`.
//!   Runs are **not** in stateId order by default: the heap backend walks a `HashMap` of states
//!   (`HeapSnapshotResources`, `HeapKeyValueStateIterator`), and real blocks come out as e.g.
//!   `[2, 0, 1]`, so readers must not assume ascending ids ([`RunOrder`]);
//!   with compression, each key-group is its own Snappy-framed stream (written with the `snap`
//!   crate, so only the local `savepoint-compressed` fixture proves compatibility with Flink's
//!   `SnappyFramedOutputStream`);
//! - Flink's key-group assignment (`MathUtils.murmurHash` + `KeyGroupRangeAssignment`), so each
//!   key lands in the key-group and subtask Flink would have chosen.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Scalar serializers this generator can write (keys, values, map keys/values, list elements).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Ser {
    String,
    Long,
    Int,
    Boolean,
    Double,
}

impl Ser {
    fn snapshot_class(self) -> &'static str {
        match self {
            Ser::String => {
                "org.apache.flink.api.common.typeutils.base.StringSerializer$StringSerializerSnapshot"
            }
            Ser::Long => {
                "org.apache.flink.api.common.typeutils.base.LongSerializer$LongSerializerSnapshot"
            }
            Ser::Int => {
                "org.apache.flink.api.common.typeutils.base.IntSerializer$IntSerializerSnapshot"
            }
            Ser::Boolean => {
                "org.apache.flink.api.common.typeutils.base.BooleanSerializer$BooleanSerializerSnapshot"
            }
            Ser::Double => {
                "org.apache.flink.api.common.typeutils.base.DoubleSerializer$DoubleSerializerSnapshot"
            }
        }
    }
}

/// A scalar value, serialized the way the matching Flink serializer writes it.
#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Str(String),
    Long(i64),
    Int(i32),
    Bool(bool),
    Double(f64),
}

impl Val {
    pub fn str(s: &str) -> Val {
        Val::Str(s.to_string())
    }

    /// The bytes Flink's serializer writes (`StringSerializer` = `StringValue.writeString`).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Val::Str(s) => write_string_value(&mut out, s),
            Val::Long(v) => out.extend_from_slice(&v.to_be_bytes()),
            Val::Int(v) => out.extend_from_slice(&v.to_be_bytes()),
            Val::Bool(v) => out.push(*v as u8),
            Val::Double(v) => out.extend_from_slice(&v.to_bits().to_be_bytes()),
        }
        out
    }

    /// `hashCode()` of the boxed Java value: what `KeyGroupRangeAssignment` hashes.
    pub fn java_hash(&self) -> i32 {
        match self {
            Val::Str(s) => java_string_hash(s),
            Val::Long(l) => (l ^ ((*l as u64) >> 32) as i64) as i32,
            Val::Int(i) => *i,
            Val::Bool(b) => {
                if *b {
                    1231
                } else {
                    1237
                }
            }
            Val::Double(d) => {
                let bits = d.to_bits() as i64;
                (bits ^ ((bits as u64) >> 32) as i64) as i32
            }
        }
    }

    /// How the explorer renders this value as a key or a JSON scalar.
    pub fn render(&self) -> String {
        match self {
            Val::Str(s) => s.clone(),
            Val::Long(v) => v.to_string(),
            Val::Int(v) => v.to_string(),
            Val::Bool(v) => v.to_string(),
            Val::Double(v) => v.to_string(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        match self {
            Val::Str(s) => serde_json::Value::String(s.clone()),
            Val::Long(v) => serde_json::json!(v),
            Val::Int(v) => serde_json::json!(v),
            Val::Bool(v) => serde_json::json!(v),
            Val::Double(v) => serde_json::json!(v),
        }
    }
}

/// The kind of a registered state.
#[derive(Clone, Debug)]
pub enum Kind {
    /// `ValueState<T>`.
    Value(Ser),
    /// `MapState<K, V>`.
    Map(Ser, Ser),
    /// `ListState<T>` (written with `ListDelimitedSerializer`).
    List(Ser),
    /// A timer priority queue (`_timer_state/...`), always after the keyed states.
    Timer,
}

#[derive(Clone, Debug)]
pub struct StateDef {
    pub name: String,
    pub kind: Kind,
}

impl StateDef {
    pub fn new(name: &str, kind: Kind) -> Self {
        StateDef {
            name: name.to_string(),
            kind,
        }
    }
}

/// One logical record of one key in one state. `state` is the index into the operator's
/// `states`, i.e. the stateId.
#[derive(Clone, Debug)]
pub enum Item {
    Value {
        state: usize,
        key: Val,
        value: Val,
    },
    MapEntry {
        state: usize,
        key: Val,
        map_key: Val,
        /// `None` = a null map value (only possible for String values on the heap backend).
        value: Option<Val>,
    },
    List {
        state: usize,
        key: Val,
        values: Vec<Val>,
    },
    Timer {
        state: usize,
        key: Val,
        timestamp: i64,
    },
}

impl Item {
    pub fn key(&self) -> &Val {
        match self {
            Item::Value { key, .. }
            | Item::MapEntry { key, .. }
            | Item::List { key, .. }
            | Item::Timer { key, .. } => key,
        }
    }

    pub fn state(&self) -> usize {
        match self {
            Item::Value { state, .. }
            | Item::MapEntry { state, .. }
            | Item::List { state, .. }
            | Item::Timer { state, .. } => *state,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyedOperator {
    /// The 16-byte OperatorID: `lower` long then `upper` long, as `_metadata` stores it.
    pub id: [u8; 16],
    pub parallelism: i32,
    pub max_parallelism: i32,
    pub key: Ser,
    pub states: Vec<StateDef>,
    pub items: Vec<Item>,
}

impl KeyedOperator {
    pub fn new(id_byte: u8, parallelism: i32, max_parallelism: i32, key: Ser) -> Self {
        KeyedOperator {
            id: [id_byte; 16],
            parallelism,
            max_parallelism,
            key,
            states: Vec::new(),
            items: Vec::new(),
        }
    }

    pub fn state(mut self, name: &str, kind: Kind) -> Self {
        self.states.push(StateDef::new(name, kind));
        self
    }

    pub fn id_hex(&self) -> String {
        self.id.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Where each subtask's keyed-state stream goes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Storage {
    /// Inside `_metadata` (`ByteStreamStateHandle`), like the Docker-generated fixtures.
    Inline,
    /// In its own file next to `_metadata` (`RelativeFileStateHandle`), like production.
    Files,
}

/// The order of the state runs inside a key-group block.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RunOrder {
    /// Last stateId first, then ascending (`[n-1, 0, 1, ...]`): the shape real heap savepoints
    /// show (`[2, 0, 1]` on `savepoint-big`). The default, so tests do not bake in an order.
    HashLike,
    /// Ascending stateIds.
    Ascending,
}

#[derive(Clone, Debug)]
pub struct Spec {
    pub checkpoint_id: i64,
    pub storage: Storage,
    pub compressed: bool,
    pub run_order: RunOrder,
    pub operators: Vec<KeyedOperator>,
}

impl Spec {
    pub fn new(storage: Storage) -> Self {
        Spec {
            checkpoint_id: 42,
            storage,
            compressed: false,
            run_order: RunOrder::HashLike,
            operators: Vec::new(),
        }
    }
}

/// What was written, for assertions.
#[derive(Debug, Default)]
pub struct Written {
    /// `(operator index, subtask index, path)` of every state file (Storage::Files only).
    pub state_files: Vec<(usize, i32, PathBuf)>,
}

/// A fresh, empty directory, unique to one call (cargo runs the tests of a binary in parallel)
/// and removed when dropped. Derefs to its path.
pub struct TempDir(PathBuf);

impl std::ops::Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn temp_dir(tag: &str) -> TempDir {
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "flink-explorer-synth-{}-{}-{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// The Java-serialized `CheckpointProperties` (a CANONICAL savepoint's) that
/// `MetadataV4Serializer.serialize` appends after the V3 body, copied from a real savepoint.
const CHECKPOINT_PROPERTIES: &[u8] = include_bytes!("../data/savepoint-checkpoint-properties.bin");

/// Write `spec` as a savepoint directory (`_metadata` plus, for `Storage::Files`, one state file
/// per subtask that holds keyed state).
pub fn write_savepoint(spec: &Spec, dir: &Path) -> Written {
    let mut written = Written::default();
    let mut meta = Vec::new();
    meta.extend_from_slice(&0x4960_672Du32.to_be_bytes()); // MetadataSerializers magic
    meta.extend_from_slice(&4i32.to_be_bytes()); // MetadataV4Serializer
    meta.extend_from_slice(&spec.checkpoint_id.to_be_bytes());
    meta.extend_from_slice(&0i32.to_be_bytes()); // no master state
    meta.extend_from_slice(&(spec.operators.len() as i32).to_be_bytes());

    for (oi, op) in spec.operators.iter().enumerate() {
        meta.extend_from_slice(&op.id);
        meta.extend_from_slice(&op.parallelism.to_be_bytes());
        meta.extend_from_slice(&op.max_parallelism.to_be_bytes());
        meta.push(0); // no coordinator state
        meta.extend_from_slice(&op.parallelism.to_be_bytes()); // one state per subtask
        for subtask in 0..op.parallelism {
            meta.extend_from_slice(&subtask.to_be_bytes());
            meta.extend_from_slice(&0i32.to_be_bytes()); // no managed operator state
            meta.extend_from_slice(&0i32.to_be_bytes()); // no raw operator state
            let (start, end) = range_for_subtask(op.max_parallelism, op.parallelism, subtask);
            let (stream, offsets) = keyed_stream(op, start, end, spec.compressed, spec.run_order);
            meta.push(7); // KeyGroupsSavepointStateHandle
            meta.extend_from_slice(&start.to_be_bytes());
            meta.extend_from_slice(&(end - start + 1).to_be_bytes());
            for off in &offsets {
                meta.extend_from_slice(&off.to_be_bytes());
            }
            match spec.storage {
                Storage::Inline => {
                    meta.push(1); // ByteStreamStateHandle
                    write_utf(&mut meta, &format!("inline-{}-{}", oi, subtask));
                    meta.extend_from_slice(&(stream.len() as i32).to_be_bytes());
                    meta.extend_from_slice(&stream);
                }
                Storage::Files => {
                    let name = format!(
                        "{:08x}-state-{}-{}",
                        0x5eed_0000u32 + oi as u32,
                        oi,
                        subtask
                    );
                    let path = dir.join(&name);
                    std::fs::write(&path, &stream).unwrap();
                    written.state_files.push((oi, subtask, path));
                    meta.push(6); // RelativeFileStateHandle
                    write_utf(&mut meta, &name);
                    meta.extend_from_slice(&(stream.len() as i64).to_be_bytes());
                }
            }
            meta.push(0); // no raw keyed state
            meta.extend_from_slice(&0i32.to_be_bytes()); // no input channel state
            meta.extend_from_slice(&0i32.to_be_bytes()); // no result subpartition state
        }
    }

    meta.extend_from_slice(CHECKPOINT_PROPERTIES);
    std::fs::write(dir.join("_metadata"), &meta).unwrap();
    written
}

/// One subtask's keyed-state stream for key-groups `start..=end`: the header, then each
/// non-empty key-group block. Returns the stream and one offset per key-group (0 = empty, as in
/// `FullSnapshotAsyncWriter`).
pub fn keyed_stream(
    op: &KeyedOperator,
    start: i32,
    end: i32,
    compressed: bool,
    order: RunOrder,
) -> (Vec<u8>, Vec<i64>) {
    let mut stream = header_bytes(op.key, &op.states, compressed);

    // key-group -> stateId -> [(key bytes, value bytes)], in insertion order per run.
    let mut groups: BTreeMap<i32, BTreeMap<usize, Vec<Entry>>> = BTreeMap::new();
    for item in &op.items {
        let kg = key_group_for_hash(item.key().java_hash(), op.max_parallelism);
        if kg < start || kg > end {
            continue;
        }
        let (k, v) = entry_bytes(op, kg, item);
        groups
            .entry(kg)
            .or_default()
            .entry(item.state())
            .or_default()
            .push((k, v));
    }

    let mut offsets = vec![0i64; (end - start + 1) as usize];
    for (kg, states) in groups {
        offsets[(kg - start) as usize] = stream.len() as i64;
        let mut block = Vec::new();
        let mut runs: Vec<(usize, Vec<Entry>)> = states.into_iter().collect();
        if order == RunOrder::HashLike {
            runs.rotate_right(1);
        }
        for (state_id, mut entries) in runs {
            block.extend_from_slice(&(state_id as u16).to_be_bytes());
            let last = entries.len() - 1;
            entries[last].0[0] |= 0x80; // FullSnapshotUtil.setMetaDataFollowsFlagInKey
            for (k, v) in entries {
                block.extend_from_slice(&(k.len() as i32).to_be_bytes());
                block.extend_from_slice(&k);
                block.extend_from_slice(&(v.len() as i32).to_be_bytes());
                block.extend_from_slice(&v);
            }
        }
        block.extend_from_slice(&0xFFFFu16.to_be_bytes()); // END_OF_KEY_GROUP_MARK
        if compressed {
            let mut enc = snap::write::FrameEncoder::new(&mut stream);
            enc.write_all(&block).unwrap();
            enc.flush().unwrap();
        } else {
            stream.extend_from_slice(&block);
        }
    }
    (stream, offsets)
}

/// One serialized `(key, value)` entry.
type Entry = (Vec<u8>, Vec<u8>);

/// The serialized `(key, value)` of one entry, as `FullSnapshotAsyncWriter` writes it.
fn entry_bytes(op: &KeyedOperator, kg: i32, item: &Item) -> Entry {
    let mut key = kg_prefix(kg, op.max_parallelism);
    match item {
        Item::Value { key: k, value, .. } => {
            key.extend(k.encode());
            key.push(0); // VoidNamespace
            (key, value.encode())
        }
        Item::MapEntry {
            key: k,
            map_key,
            value,
            ..
        } => {
            key.extend(k.encode());
            key.push(0); // VoidNamespace
            key.extend(map_key.encode());
            // HeapKeyValueStateIterator.java:360-361: writeBoolean(userValue == null), then
            // userValueSerializer.serialize(userValue) even when it is null.
            let v = match value {
                Some(v) => {
                    let mut b = vec![0u8];
                    b.extend(v.encode());
                    b
                }
                None => match op.states[item.state()].kind {
                    // StringValue.writeString(null) writes a single 0 (length 0 = null).
                    Kind::Map(_, Ser::String) => vec![1u8, 0u8],
                    // The boxed-primitive serializers throw on null: a heap savepoint cannot
                    // hold such an entry.
                    _ => panic!("a heap savepoint cannot hold a null non-String map value"),
                },
            };
            (key, v)
        }
        Item::List { key: k, values, .. } => {
            key.extend(k.encode());
            key.push(0); // VoidNamespace
                         // ListDelimitedSerializer: elements separated by ',' (0x2c), no length prefix.
            let mut v = Vec::new();
            for (i, e) in values.iter().enumerate() {
                if i > 0 {
                    v.push(b',');
                }
                v.extend(e.encode());
            }
            (key, v)
        }
        Item::Timer {
            key: k, timestamp, ..
        } => {
            // TimerSerializer: flipSignBit(timestamp), key, namespace. No value.
            key.extend_from_slice(&(timestamp ^ i64::MIN).to_be_bytes());
            key.extend(k.encode());
            key.push(0); // VoidNamespace
            (key, Vec::new())
        }
    }
}

/// `CompositeKeySerializationUtils.writeKeyGroup`: 1 byte up to maxParallelism 128, else 2.
fn kg_prefix(kg: i32, max_parallelism: i32) -> Vec<u8> {
    if max_parallelism > 128 {
        (kg as u16).to_be_bytes().to_vec()
    } else {
        vec![kg as u8]
    }
}

/// `KeyedBackendSerializationProxy.write` (version 6).
pub fn header_bytes(key: Ser, states: &[StateDef], compressed: bool) -> Vec<u8> {
    // HeapSnapshotResources lists keyed states first, then priority-queue (timer) states, which
    // is what fixes the shared stateId space.
    let first_timer = states.iter().position(|s| matches!(s.kind, Kind::Timer));
    if let Some(t) = first_timer {
        assert!(
            states[t..].iter().all(|s| matches!(s.kind, Kind::Timer)),
            "timer states must come after every keyed state"
        );
    }
    let mut h = Vec::new();
    h.extend_from_slice(&6i32.to_be_bytes());
    h.push(compressed as u8);
    proxied_snapshot(&mut h, Snap::Scalar(key));
    h.extend_from_slice(&(states.len() as u16).to_be_bytes());
    for s in states {
        write_utf(&mut h, &s.name);
        match &s.kind {
            Kind::Timer => {
                h.extend_from_slice(&3i32.to_be_bytes()); // BackendStateType.PRIORITY_QUEUE
                h.extend_from_slice(&0i32.to_be_bytes()); // no options
                h.extend_from_slice(&1i32.to_be_bytes()); // one serializer
                write_utf(&mut h, "VALUE_SERIALIZER");
                proxied_snapshot(&mut h, Snap::Timer(key));
            }
            kind => {
                let (type_name, value) = match kind {
                    Kind::Value(v) => ("VALUE", Snap::Scalar(*v)),
                    Kind::Map(k, v) => ("MAP", Snap::Map(*k, *v)),
                    Kind::List(e) => ("LIST", Snap::List(*e)),
                    Kind::Timer => unreachable!(),
                };
                h.extend_from_slice(&0i32.to_be_bytes()); // BackendStateType.KEY_VALUE
                h.extend_from_slice(&1i32.to_be_bytes()); // one option
                write_utf(&mut h, "KEYED_STATE_TYPE");
                write_utf(&mut h, type_name);
                h.extend_from_slice(&2i32.to_be_bytes()); // two serializers
                write_utf(&mut h, "VALUE_SERIALIZER");
                proxied_snapshot(&mut h, value);
                write_utf(&mut h, "NAMESPACE_SERIALIZER");
                proxied_snapshot(&mut h, Snap::VoidNamespace);
            }
        }
    }
    h
}

#[derive(Clone, Copy)]
enum Snap {
    Scalar(Ser),
    Map(Ser, Ser),
    List(Ser),
    Timer(Ser),
    VoidNamespace,
}

/// `TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot`: the
/// `TypeSerializerSnapshotSerializationProxy` version (2) then the versioned snapshot.
fn proxied_snapshot(out: &mut Vec<u8>, snap: Snap) {
    out.extend_from_slice(&2i32.to_be_bytes());
    versioned_snapshot(out, snap);
}

/// `TypeSerializerSnapshot.writeVersionedSnapshot`: class name, snapshot version, payload.
/// Composite snapshots (`CompositeTypeSerializerSnapshot`) write their magic number and outer
/// version, then `NestedSerializersSnapshotDelegate` writes its own magic, version and the nested
/// snapshots (versioned, without the proxy version).
fn versioned_snapshot(out: &mut Vec<u8>, snap: Snap) {
    let composite = |out: &mut Vec<u8>, class: &str, outer_version: i32, nested: &[Snap]| {
        write_utf(out, class);
        out.extend_from_slice(&3i32.to_be_bytes());
        out.extend_from_slice(&911_108i32.to_be_bytes()); // CompositeTypeSerializerSnapshot.MAGIC_NUMBER
        out.extend_from_slice(&outer_version.to_be_bytes());
        out.extend_from_slice(&1_333_245i32.to_be_bytes()); // NestedSerializersSnapshotDelegate.MAGIC_NUMBER
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&(nested.len() as i32).to_be_bytes());
        for n in nested {
            versioned_snapshot(out, *n);
        }
    };
    match snap {
        Snap::Scalar(s) => {
            write_utf(out, s.snapshot_class());
            out.extend_from_slice(&3i32.to_be_bytes());
        }
        Snap::VoidNamespace => {
            write_utf(
                out,
                "org.apache.flink.runtime.state.VoidNamespaceSerializer$VoidNamespaceSerializerSnapshot",
            );
            out.extend_from_slice(&3i32.to_be_bytes());
        }
        Snap::Map(k, v) => composite(
            out,
            "org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot",
            1,
            &[Snap::Scalar(k), Snap::Scalar(v)],
        ),
        Snap::List(e) => composite(
            out,
            "org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot",
            1,
            &[Snap::Scalar(e)],
        ),
        Snap::Timer(k) => composite(
            out,
            "org.apache.flink.streaming.api.operators.TimerSerializerSnapshot",
            2,
            &[Snap::Scalar(k), Snap::VoidNamespace],
        ),
    }
}

/// `DataOutput.writeUTF` (ASCII only here: every name this generator writes is ASCII).
fn write_utf(out: &mut Vec<u8>, s: &str) {
    assert!(s.is_ascii(), "write_utf here only handles ASCII: {:?}", s);
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// `StringValue.writeString`: var-length `charCount + 1`, then each UTF-16 unit var-length.
pub fn write_string_value(out: &mut Vec<u8>, s: &str) {
    let units: Vec<u16> = s.encode_utf16().collect();
    write_var(out, units.len() as u32 + 1);
    for u in units {
        write_var(out, u as u32);
    }
}

fn write_var(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// `org.apache.flink.util.MathUtils.bitMix` (flink-core, release-1.20).
fn bit_mix(mut i: i32) -> i32 {
    i ^= ((i as u32) >> 16) as i32;
    i = i.wrapping_mul(0x85eb_ca6bu32 as i32);
    i ^= ((i as u32) >> 13) as i32;
    i = i.wrapping_mul(0xc2b2_ae35u32 as i32);
    i ^= ((i as u32) >> 16) as i32;
    i
}

/// `org.apache.flink.util.MathUtils.murmurHash` (flink-core, release-1.20).
fn murmur_hash(mut code: i32) -> i32 {
    code = code.wrapping_mul(0xcc9e_2d51u32 as i32);
    code = code.rotate_left(15);
    code = code.wrapping_mul(0x1b87_3593);
    code = code.rotate_left(13);
    code = code.wrapping_mul(5).wrapping_add(0xe654_6b64u32 as i32);
    code ^= 4;
    code = bit_mix(code);
    if code >= 0 {
        code
    } else if code != i32::MIN {
        -code
    } else {
        0
    }
}

/// `KeyGroupRangeAssignment.computeKeyGroupForKeyHash`.
pub fn key_group_for_hash(key_hash: i32, max_parallelism: i32) -> i32 {
    murmur_hash(key_hash) % max_parallelism
}

/// `KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex`: inclusive `(start, end)`.
pub fn range_for_subtask(max_parallelism: i32, parallelism: i32, subtask: i32) -> (i32, i32) {
    let start = (subtask * max_parallelism + parallelism - 1) / parallelism;
    let end = ((subtask + 1) * max_parallelism - 1) / parallelism;
    (start, end)
}

/// `java.lang.String.hashCode` (over UTF-16 code units).
pub fn java_string_hash(s: &str) -> i32 {
    let mut h: i32 = 0;
    for c in s.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(c as i32);
    }
    h
}

/// The first timer timestamp of [`sample_operator`]: 2026-10-13T00:00:00Z.
pub const SAMPLE_EPOCH_MS: i64 = 1_791_849_600_000;

/// A ready-made operator covering VALUE (Long, String), MAP, LIST and a timer queue,
/// with `keys` String keys `key-0000000`.., two subtasks and maxParallelism 128.
pub fn sample_operator(keys: usize) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x34, 2, 128, Ser::String)
        .state("counter", Kind::Value(Ser::Long))
        .state("label", Kind::Value(Ser::String))
        .state("stats", Kind::Map(Ser::String, Ser::Long))
        .state("history", Kind::List(Ser::Long))
        .state("_timer_state/processing_user-timers", Kind::Timer);
    for i in 0..keys {
        let key = Val::Str(format!("key-{:07}", i));
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Long(i as i64 * 31 + 7),
        });
        op.items.push(Item::Value {
            state: 1,
            key: key.clone(),
            value: Val::Str(format!("label of {}", i)),
        });
        for (mk, mv) in [("clicks", i as i64 % 1000), ("views", i as i64 % 7919)] {
            op.items.push(Item::MapEntry {
                state: 2,
                key: key.clone(),
                map_key: Val::str(mk),
                value: Some(Val::Long(mv)),
            });
        }
        if i % 3 == 0 {
            op.items.push(Item::List {
                state: 3,
                key: key.clone(),
                values: (0..3).map(|j| Val::Long(i as i64 * 10 + j)).collect(),
            });
        }
        if i % 5 == 0 {
            op.items.push(Item::Timer {
                state: 4,
                key: key.clone(),
                // Non-zero millis: the explorer omits a `.000` fraction.
                timestamp: SAMPLE_EPOCH_MS + i as i64 * 1000 + 250,
            });
        }
    }
    op
}
