//! `reader::StateReader` on synthetic savepoints (`tests/common/synth.rs`): fixture-free, so it
//! runs in CI over inline, file-backed (relative and absolute), multi-subtask and compressed
//! layouts, with state runs in hash-like order.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::synth::{self, Item, KeyedOperator, Kind, RunOrder, Ser, Spec, Storage, TempDir, Val};
use flink_explorer::export_state::{export_state_rows, list_states, RowSink, StateRow};
use flink_explorer::parser;
use flink_explorer::parser::key_group::{
    java_string_hash, key_group_for_hash, subtask_for_key_group,
};
use flink_explorer::parser::state_handle::{StateHandle, StreamStateHandle};
use flink_explorer::reader::{Cursor, KeyRecord, Lookup, ReadStats, ReaderError, StateReader};
use std::sync::Arc;

const LAYOUTS: [(Storage, bool); 5] = [
    (Storage::Inline, false),
    (Storage::Inline, true),
    (Storage::Files, false),
    (Storage::Files, true),
    (Storage::AbsoluteFiles, false),
];

fn write(spec: &Spec, tag: &str) -> TempDir {
    let dir = synth::temp_dir(tag);
    synth::write_savepoint(spec, &dir);
    dir
}

fn spec_with(ops: &[KeyedOperator], storage: Storage, compressed: bool) -> Spec {
    let mut spec = Spec::new(storage);
    spec.compressed = compressed;
    spec.run_order = RunOrder::HashLike;
    spec.operators.extend(ops.iter().cloned());
    spec
}

fn reader(dir: &Path) -> StateReader<'static> {
    StateReader::open(dir).expect("synthetic savepoint opens")
}

/// The sample operator (String keys; VALUE, VALUE, MAP, LIST and a timer queue) on three
/// subtasks and maxParallelism 128.
fn three_subtasks(keys: usize) -> KeyedOperator {
    let mut op = synth::sample_operator(keys);
    op.parallelism = 3;
    op
}

/// Long keys, maxParallelism 4096 (a 2-byte key-group prefix), 4 subtasks registering their
/// states in different orders, some not at all.
fn long_keys(keys: i64) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x77, 4, 4096, Ser::Long)
        .state("total", Kind::Value(Ser::Long))
        .state("names", Kind::Map(Ser::Int, Ser::String))
        .state("_timer_state/event_user-timers", Kind::Timer);
    op.subtask_states = Some(vec![
        vec![
            "total".into(),
            "names".into(),
            "_timer_state/event_user-timers".into(),
        ],
        vec![
            "names".into(),
            "total".into(),
            "_timer_state/event_user-timers".into(),
        ],
        vec!["total".into()],
        vec!["names".into(), "total".into()],
    ]);
    for i in 0..keys {
        let key = Val::Long(i * 1_000_003 - 50_000);
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Long(i * 2),
        });
        if i % 2 == 0 {
            op.items.push(Item::MapEntry {
                state: 1,
                key: key.clone(),
                map_key: Val::Int(i as i32),
                value: Some(Val::Str(format!("n{}", i))),
            });
        }
        if i % 7 == 0 {
            op.items.push(Item::Timer {
                state: 2,
                key,
                timestamp: synth::SAMPLE_EPOCH_MS + i,
            });
        }
    }
    op
}

/// Rendered keys of an operator's items, distinct.
fn expected_keys(op: &KeyedOperator) -> BTreeSet<String> {
    op.items.iter().map(|i| i.key().render()).collect()
}

/// Every key of `op`, page by page, with `limit` keys per page.
fn page_all(r: &StateReader, op: &str, limit: usize) -> (Vec<KeyRecord>, usize) {
    let mut cursor = Cursor::START;
    let mut keys = Vec::new();
    let mut pages = 0;
    loop {
        let page = r.key_page(op, &cursor, limit).expect("page");
        assert!(page.malformed.is_empty(), "{:?}", page.malformed);
        assert!(page.keys.len() <= limit);
        pages += 1;
        keys.extend(page.keys);
        match page.next {
            Some(next) => {
                // Through its string form, as a URL would carry it.
                let s = next.to_string();
                cursor = s.parse().expect("cursor parses back");
                assert_eq!(cursor, next);
            }
            None => return (keys, pages),
        }
        assert!(pages < 100_000, "paging does not end");
    }
}

/// `(state, key, map_key, value, key_group, subtask, namespace_hex)`, sortable.
type Row = (
    String,
    String,
    Option<String>,
    String,
    u16,
    i32,
    Option<String>,
);

struct Collect(Vec<Row>);

impl RowSink for Collect {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        self.0.push((
            row.state.to_string(),
            row.key.to_string(),
            row.map_key.map(str::to_string),
            row.value.to_string(),
            row.key_group,
            row.subtask,
            row.namespace_hex.map(str::to_string),
        ));
        Ok(())
    }
    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{:02x}", b)).collect()
}

/// The paged records flattened into export rows.
fn rows_of(keys: &[KeyRecord]) -> Vec<Row> {
    let mut rows = Vec::new();
    for k in keys {
        for e in &k.entries {
            let ns = (e.map_key.is_none() && e.namespace.len() > 1).then(|| hex(&e.namespace));
            rows.push((
                e.state.to_string(),
                k.key.clone(),
                e.map_key.clone(),
                e.value.to_string(),
                k.key_group,
                k.subtask,
                ns,
            ));
        }
    }
    rows.sort();
    rows
}

/// Every state of `op_hex`, exported by `export_state_rows`.
fn export_rows(dir: &Path, op_hex: &str) -> Vec<Row> {
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let listing = list_states(&sp, dir);
    assert!(listing.errors.is_empty(), "{:?}", listing.errors);
    let mut sink = Collect(Vec::new());
    for (op, state, _) in &listing.states {
        if op == op_hex {
            export_state_rows(&sp, dir, Some(op), state, None, &mut sink).expect("export");
        }
    }
    sink.0.sort();
    sink.0
}

/// The stored size of each non-empty key-group of an operator, from its handles.
fn block_sizes(dir: &Path, op_hex: &str) -> BTreeMap<u16, u64> {
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let op = sp
        .operators
        .iter()
        .find(|o| flink_explorer::reader::hex_id(&o.id) == op_hex)
        .unwrap();
    let mut out = BTreeMap::new();
    for st in &op.subtask_states {
        let StateHandle::KeyGroups(kg) = &st.managed_keyed_state else {
            continue;
        };
        let len = match &kg.delegate_handle {
            StreamStateHandle::ByteStream(b) => b.data.len() as u64,
            StreamStateHandle::Relative(r) => r.state_size as u64,
            StreamStateHandle::File(f) => f.state_size as u64,
            other => panic!("unexpected handle {:?}", other),
        };
        for (i, &off) in kg.offsets.iter().enumerate() {
            if off <= 0 {
                continue;
            }
            let end = kg.offsets[i + 1..]
                .iter()
                .copied()
                .find(|&o| o > off)
                .map_or(len, |o| o as u64);
            out.insert((kg.start_key_group + i as i32) as u16, end - off as u64);
        }
    }
    out
}

#[test]
fn paging_visits_every_key_once_and_agrees_with_the_export() {
    let ops = [three_subtasks(300), long_keys(400)];
    for (storage, compressed) in LAYOUTS {
        let dir = write(&spec_with(&ops, storage, compressed), "reader-page");
        let r = reader(&dir);
        for op in &ops {
            let id = op.id_hex();
            let (keys, pages) = page_all(&r, &id, 7);
            let what = format!("{:?} compressed={} op {}", storage, compressed, id);
            assert!(pages > 10, "{}: {} pages", what, pages);

            // Every key exactly once.
            let rendered: Vec<&str> = keys.iter().map(|k| k.key.as_str()).collect();
            let distinct: BTreeSet<String> = rendered.iter().map(|s| s.to_string()).collect();
            assert_eq!(rendered.len(), distinct.len(), "{}: a key came twice", what);
            assert_eq!(distinct, expected_keys(op), "{}", what);

            // Key-group order, each key in the key-group and subtask Flink assigns it.
            for w in keys.windows(2) {
                assert!(w[0].key_group <= w[1].key_group, "{}", what);
            }
            for k in &keys {
                let item = op.items.iter().find(|i| i.key().render() == k.key).unwrap();
                let kg = synth::key_group_for_hash(item.key().java_hash(), op.max_parallelism);
                assert_eq!(k.key_group as i32, kg, "{}: {}", what, k.key);
                assert_eq!(
                    k.subtask,
                    subtask_for_key_group(op.max_parallelism, op.parallelism, kg)
                );
                assert_eq!(k.position.key_group, k.key_group);
            }

            // The same entries and values as the streaming export, state by state.
            assert_eq!(rows_of(&keys), export_rows(&dir, &id), "{}", what);

            // Page size does not change the sequence; a position resumes at its key.
            let (all, one) = page_all(&r, &id, usize::MAX);
            assert_eq!(one, 1);
            assert_eq!(all, keys, "{}", what);
            let mid = &keys[keys.len() / 2];
            let from = r.key_page(&id, &mid.position, 3).unwrap();
            assert_eq!(from.keys[0], *mid);
        }
    }
}

#[test]
fn get_finds_every_key_reading_one_block() {
    let ops = [three_subtasks(250), long_keys(300)];
    for (storage, compressed) in LAYOUTS {
        let dir = write(&spec_with(&ops, storage, compressed), "reader-get");
        // No cache: every lookup reads its block from storage.
        let r = reader(&dir).with_cache_bytes(0);
        for op in &ops {
            let id = op.id_hex();
            let sizes = block_sizes(&dir, &id);
            let (keys, _) = page_all(&r, &id, usize::MAX);
            for k in &keys {
                match r.get(&id, &k.key).unwrap() {
                    Lookup::Found { record, stats, .. } => {
                        assert_eq!(&record, k);
                        assert_eq!(stats.blocks, 1);
                        assert_eq!(stats.cache_hits, 0);
                        assert_eq!(stats.bytes_read, sizes[&k.key_group], "key {}", k.key);
                    }
                    other => panic!("{:?} {}: {:?}", storage, k.key, other),
                }
            }
        }
    }
}

#[test]
fn the_cache_serves_repeated_reads_of_one_block() {
    let op = three_subtasks(200);
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Files, true),
        "reader-cache",
    );
    let r = reader(&dir);
    let id = op.id_hex();
    let (keys, _) = page_all(&reader(&dir), &id, usize::MAX);
    let k = &keys[0];
    let Lookup::Found { stats: first, .. } = r.get(&id, &k.key).unwrap() else {
        panic!()
    };
    let Lookup::Found { stats: again, .. } = r.get(&id, &k.key).unwrap() else {
        panic!()
    };
    assert_eq!(again.cache_hits, 1);
    assert_eq!(again.bytes_read, 0);
    assert!(first.bytes_read > 0);
    assert_eq!(again.block_bytes, first.bytes_read);
    // A page inside that block is served from the cache too.
    let page = r.key_page(&id, &k.position, 1).unwrap();
    assert_eq!((page.stats.cache_hits, page.stats.bytes_read), (1, 0));
    // Disabled, it reads again.
    let r = reader(&dir).with_cache_bytes(0);
    for _ in 0..2 {
        let Lookup::Found { stats, .. } = r.get(&id, &k.key).unwrap() else {
            panic!()
        };
        assert_eq!(stats.bytes_read, first.bytes_read);
    }
}

/// String keys over 128 key-groups: with few keys most key-groups are empty.
#[test]
fn an_absent_key_costs_nothing_in_an_empty_key_group() {
    let op = three_subtasks(20);
    for (storage, compressed) in LAYOUTS {
        let dir = write(
            &spec_with(std::slice::from_ref(&op), storage, compressed),
            "reader-absent",
        );
        let r = reader(&dir).with_cache_bytes(0);
        let id = op.id_hex();
        let sizes = block_sizes(&dir, &id);
        let (mut empty, mut occupied) = (0, 0);
        for i in 0..200 {
            let text = format!("absent-{}", i);
            let kg = key_group_for_hash(java_string_hash(&text), 128);
            match r.get(&id, &text).unwrap() {
                Lookup::Absent {
                    key_group,
                    subtask,
                    stats,
                } => {
                    assert_eq!(key_group as i32, kg);
                    assert_eq!(subtask, subtask_for_key_group(128, 3, kg));
                    match sizes.get(&key_group) {
                        None => {
                            assert_eq!(stats, ReadStats::default(), "{}", text);
                            empty += 1;
                        }
                        Some(&size) => {
                            assert_eq!((stats.blocks, stats.bytes_read), (1, size));
                            occupied += 1;
                        }
                    }
                }
                other => panic!("{}: {:?}", text, other),
            }
        }
        assert!(
            empty > 0 && occupied > 0,
            "{} empty, {} occupied",
            empty,
            occupied
        );
    }
}

#[test]
fn a_tiny_budget_scan_resumes_without_duplicates_or_misses() {
    let ops = [three_subtasks(300), long_keys(300)];
    for (storage, compressed) in [(Storage::Files, true), (Storage::Inline, false)] {
        let dir = write(&spec_with(&ops, storage, compressed), "reader-scan");
        let r = reader(&dir);
        for op in &ops {
            let id = op.id_hex();
            let wanted = |k: &str| k.ends_with('7') || k.ends_with('3');
            let expected: Vec<String> = expected_keys(op)
                .into_iter()
                .filter(|k| wanted(k))
                .collect();
            let blocks = block_sizes(&dir, &id).len();

            let mut cursor = Cursor::START;
            let mut found = Vec::new();
            let mut calls = 0;
            loop {
                let page = r.scan(&id, &cursor, 1, &mut |k| wanted(k)).unwrap();
                calls += 1;
                assert_eq!(
                    page.stats.blocks, 1,
                    "a 1-byte budget reads one block per call"
                );
                found.extend(page.matches.into_iter().map(|m| m.key));
                match page.next {
                    Some(n) => cursor = n.to_string().parse().unwrap(),
                    None => break,
                }
            }
            assert_eq!(calls, blocks);
            let distinct: BTreeSet<String> = found.iter().cloned().collect();
            assert_eq!(distinct.len(), found.len(), "duplicates");
            assert_eq!(distinct, expected.into_iter().collect::<BTreeSet<_>>());

            // A cursor in the middle of a block: the scan starts at that very key.
            let (keys, _) = page_all(&r, &id, usize::MAX);
            let at = keys.iter().position(|k| k.position.ordinal > 0).unwrap();
            let rest = r
                .scan(&id, &keys[at].position, u64::MAX, &mut |_| true)
                .unwrap();
            assert_eq!(rest.next, None);
            let rest: Vec<&str> = rest.matches.iter().map(|m| m.key.as_str()).collect();
            let tail: Vec<&str> = keys[at..].iter().map(|k| k.key.as_str()).collect();
            assert_eq!(rest, tail);
        }
    }
}

#[test]
fn cursors_round_trip_through_their_string_form() {
    for c in [
        Cursor::START,
        Cursor {
            subtask: 3,
            key_group: 32767,
            ordinal: 4_000_000_000,
        },
    ] {
        assert_eq!(c.to_string().parse::<Cursor>().unwrap(), c);
    }
    assert_eq!(
        Cursor {
            subtask: 1,
            key_group: 64,
            ordinal: 17
        }
        .to_string(),
        "1.64.17"
    );
    for bad in ["", "1.2", "1.2.3.4", "a.b.c", "1.70000.0", "-1.2.x"] {
        assert!(bad.parse::<Cursor>().is_err(), "{:?}", bad);
    }
}

#[test]
fn a_cursor_of_another_subtask_is_refused() {
    let op = three_subtasks(30);
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Inline, false),
        "reader-badcursor",
    );
    let r = reader(&dir);
    let bad = Cursor {
        subtask: 2,
        key_group: 0,
        ordinal: 0,
    };
    let e = r.key_page(&op.id_hex(), &bad, 10).unwrap_err();
    assert!(e.to_string().contains("held by subtask 0"), "{}", e);
}

/// Each key type with a known `hashCode()` is found by its rendered text, on keys written with
/// the synthetic writer's own (independent) hash port.
#[test]
fn get_finds_keys_of_every_hashable_type() {
    let cases: Vec<(Ser, Vec<Val>)> = vec![
        (Ser::Int, (-50..50).map(|i| Val::Int(i * 7919)).collect()),
        (Ser::Short, (-50..50).map(|i| Val::Short(i * 31)).collect()),
        (Ser::Byte, (-50..50).map(|i| Val::Byte(i as i8)).collect()),
        (
            Ser::Char,
            vec![Val::Char(b'a' as u16), Val::Char(0xe9), Val::Char(0xD800)],
        ),
        (Ser::Boolean, vec![Val::Bool(true), Val::Bool(false)]),
        (
            Ser::Float,
            vec![
                Val::Float(0.1),
                Val::Float(-0.0),
                Val::Float(0.0),
                Val::Float(f32::NAN),
            ],
        ),
        (
            Ser::Double,
            vec![
                Val::Double(3.0),
                Val::Double(-0.0),
                Val::Double(1e300),
                Val::Double(f64::INFINITY),
                Val::Double(f64::NAN),
            ],
        ),
        (
            Ser::BigInteger,
            [0i128, 1, 10, -1, 255, -256, 1 << 70, -(1 << 100)]
                .into_iter()
                .map(Val::BigInt)
                .collect(),
        ),
        (
            Ser::Tuple(&[Ser::String, Ser::Long]),
            (0..40)
                .map(|i| Val::Tuple(vec![Val::Str(format!("a,\"{}", i)), Val::Long(i)]))
                .collect(),
        ),
    ];
    for (n, (ser, keys)) in cases.into_iter().enumerate() {
        let mut op =
            KeyedOperator::new(0x10 + n as u8, 2, 256, ser).state("v", Kind::Value(Ser::Long));
        for (i, k) in keys.iter().enumerate() {
            op.items.push(Item::Value {
                state: 0,
                key: k.clone(),
                value: Val::Long(i as i64),
            });
        }
        let dir = write(
            &spec_with(std::slice::from_ref(&op), Storage::Files, false),
            "reader-types",
        );
        let r = reader(&dir);
        for (i, k) in keys.iter().enumerate() {
            let text = k.render();
            match r.get(&op.id_hex(), &text).unwrap() {
                Lookup::Found { record, .. } => {
                    assert_eq!(record.key, text);
                    assert_eq!(record.entries.len(), 1);
                    assert_eq!(record.entries[0].value, serde_json::json!(i));
                }
                other => panic!("{:?} {:?}: {:?}", ser, text, other),
            }
        }
    }
}

#[test]
fn keys_without_a_known_hash_need_a_scan() {
    let mut op = KeyedOperator::new(
        0x51,
        2,
        128,
        Ser::Opaque("com.example.OpaqueKeySerializer$OpaqueKeySerializerSnapshot"),
    )
    .state("v", Kind::Value(Ser::Long));
    op.items.push(Item::Value {
        state: 0,
        key: Val::Opaque {
            bytes: vec![1, 2, 3],
            hash: 5,
        },
        value: Val::Long(1),
    });
    let mut longs = long_keys(10);
    longs.id = [0x52; 16];
    let dir = write(
        &spec_with(&[op.clone(), longs.clone()], Storage::Inline, false),
        "reader-scan-needed",
    );
    let r = reader(&dir);
    assert!(matches!(
        r.get(&op.id_hex(), "0x010203").unwrap(),
        Lookup::NeedsScan { .. }
    ));
    match r.get(&longs.id_hex(), "not-a-long").unwrap() {
        Lookup::NeedsScan { reason } => assert!(reason.contains("Long"), "{}", reason),
        other => panic!("{:?}", other),
    }
    // The opaque key is still found by a scan.
    let page = r
        .scan(&op.id_hex(), &Cursor::START, u64::MAX, &mut |k| {
            k == "0x010203"
        })
        .unwrap();
    assert_eq!(page.matches.len(), 1);
}

#[test]
fn an_unreadable_file_is_an_error_naming_it_never_an_absent_key() {
    let op = three_subtasks(300);
    let dir = synth::temp_dir("reader-missing");
    let written = synth::write_savepoint(
        &spec_with(std::slice::from_ref(&op), Storage::Files, false),
        &dir,
    );
    let (_, _, lost) = written
        .state_files
        .iter()
        .find(|(_, s, _)| *s == 1)
        .unwrap()
        .clone();
    std::fs::remove_file(&lost).unwrap();
    let r = reader(&dir);
    let id = op.id_hex();
    let lost_name = lost.file_name().unwrap().to_string_lossy().to_string();

    let states = r.states(&id).unwrap();
    assert_eq!(states.errors.len(), 1);
    assert!(states.errors[0].contains(&lost_name));
    assert_eq!(states.states.len(), 5);

    let (lost_kgs, kept_kgs) = (
        synth::range_for_subtask(128, 3, 1),
        synth::range_for_subtask(128, 3, 0),
    );
    let mut seen_lost = false;
    for i in 0..300 {
        let text = format!("key-{:07}", i);
        let kg = key_group_for_hash(java_string_hash(&text), 128);
        let got = r.get(&id, &text);
        if (lost_kgs.0..=lost_kgs.1).contains(&kg) {
            let e = got.unwrap_err();
            assert!(e.to_string().contains(&lost_name), "{}", e);
            assert!(
                matches!(&e, ReaderError::Unreadable { subtask: 1, .. }),
                "{:?}",
                e
            );
            seen_lost = true;
        } else if (kept_kgs.0..=kept_kgs.1).contains(&kg) {
            assert!(matches!(got.unwrap(), Lookup::Found { .. }));
        }
    }
    assert!(seen_lost);

    // Paging reports the broken subtask once, in the page that reaches it, and goes on past it
    // to subtask 2, keeping every key it read.
    let expected: BTreeSet<String> = expected_keys(&op)
        .into_iter()
        .filter(|k| {
            let kg = key_group_for_hash(java_string_hash(k), 128);
            !(lost_kgs.0..=lost_kgs.1).contains(&kg)
        })
        .collect();
    let mut cursor = Cursor::START;
    let mut keys = BTreeSet::new();
    let mut unreadable = Vec::new();
    for _ in 0..1000 {
        let page = r.key_page(&id, &cursor, 50).unwrap();
        assert!(page.keys.len() <= 50);
        keys.extend(page.keys.iter().map(|k| k.key.clone()));
        unreadable.extend(page.unreadable);
        match page.next {
            Some(n) => cursor = n,
            None => break,
        }
    }
    assert_eq!(keys, expected);
    assert_eq!(unreadable.len(), 1, "{:?}", unreadable);
    let u = &unreadable[0];
    assert_eq!(u.subtask, 1);
    assert_eq!(
        (u.first_key_group as i32, u.last_key_group as i32),
        lost_kgs
    );
    assert!(u.error.contains(&lost_name), "{}", u.error);

    // So does a scan, whatever its budget.
    for budget in [1, u64::MAX] {
        let mut cursor = Cursor::START;
        let mut found = BTreeSet::new();
        let mut unreadable = Vec::new();
        loop {
            let page = r.scan(&id, &cursor, budget, &mut |_| true).unwrap();
            found.extend(page.matches.iter().map(|k| k.key.clone()));
            unreadable.extend(page.unreadable);
            match page.next {
                Some(n) => cursor = n,
                None => break,
            }
        }
        assert_eq!(found, expected);
        assert_eq!(unreadable.len(), 1);
    }
}

/// A block that cannot be inflated is one unreadable key-group: the page reports it and moves on
/// to the next key-group, and `get` of a key it holds is an error, never `Absent`.
#[test]
fn paging_moves_past_a_block_that_does_not_inflate() {
    let op = three_subtasks(300);
    let dir = synth::temp_dir("reader-bad-snappy");
    let written = synth::write_savepoint(
        &spec_with(std::slice::from_ref(&op), Storage::Files, true),
        &dir,
    );
    let r0 = reader(&dir);
    let id = op.id_hex();
    let (all, _) = page_all(&r0, &id, usize::MAX);
    drop(r0);
    // Corrupt the first non-empty key-group of subtask 0: overwrite its Snappy stream identifier.
    let parsed = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let StateHandle::KeyGroups(kg) = &parsed.operators[0].subtask_states[0].managed_keyed_state
    else {
        panic!("key-groups handle")
    };
    let (rel, off) = kg
        .offsets
        .iter()
        .enumerate()
        .find(|(_, &o)| o > 0)
        .map(|(i, &o)| (i, o))
        .unwrap();
    let bad_kg = kg.start_key_group as u16 + rel as u16;
    let path = &written.state_files.iter().find(|f| f.1 == 0).unwrap().2;
    let mut bytes = std::fs::read(path).unwrap();
    for b in &mut bytes[off as usize..off as usize + 4] {
        *b = 0xEE;
    }
    std::fs::write(path, &bytes).unwrap();

    let r = reader(&dir);
    let expected: BTreeSet<String> = all
        .iter()
        .filter(|k| k.key_group != bad_kg)
        .map(|k| k.key.clone())
        .collect();
    let mut cursor = Cursor::START;
    let mut keys = BTreeSet::new();
    let mut unreadable = Vec::new();
    loop {
        let page = r.key_page(&id, &cursor, 7).unwrap();
        keys.extend(page.keys.iter().map(|k| k.key.clone()));
        unreadable.extend(page.unreadable);
        match page.next {
            Some(n) => cursor = n,
            None => break,
        }
    }
    assert_eq!(keys, expected);
    assert_eq!(unreadable.len(), 1, "{:?}", unreadable);
    assert_eq!(
        (unreadable[0].first_key_group, unreadable[0].last_key_group),
        (bad_kg, bad_kg)
    );
    assert!(unreadable[0].error.contains("snappy"), "{:?}", unreadable);

    let lost = all.iter().find(|k| k.key_group == bad_kg).unwrap();
    match r.get(&id, &lost.key) {
        Err(ReaderError::Unreadable {
            first_key_group, ..
        }) => assert_eq!(first_key_group, bad_kg),
        other => panic!("{:?}", other),
    }
}

/// A key found in a block whose end is malformed is `Found`, with the defect reported, and a key
/// that would come after the defect is an error rather than `Absent`.
#[test]
fn a_key_found_in_a_malformed_block_says_so() {
    let mut op = KeyedOperator::new(0x51, 1, 128, Ser::String)
        .state("v", Kind::Value(Ser::Long))
        .state("m", Kind::Map(Ser::Int, Ser::Long));
    // Many keys that share one key-group, so the block holds several of them.
    let target = key_group_for_hash(java_string_hash("k0"), 128);
    let names: Vec<String> = (0..20_000)
        .map(|i| format!("k{}", i))
        .filter(|k| key_group_for_hash(java_string_hash(k), 128) == target)
        .take(6)
        .collect();
    for (i, k) in names.iter().enumerate() {
        op.items.push(Item::Value {
            state: 0,
            key: Val::str(k),
            value: Val::Long(i as i64),
        });
        op.items.push(Item::MapEntry {
            state: 1,
            key: Val::str(k),
            map_key: Val::Int(i as i32),
            value: Some(Val::Long(1)),
        });
    }
    let mut spec = spec_with(std::slice::from_ref(&op), Storage::Files, false);
    spec.run_order = RunOrder::Ascending;
    let dir = synth::temp_dir("reader-malformed");
    let written = synth::write_savepoint(&spec, &dir);
    // Cut the stream inside the last run: the VALUE run (state 0) is whole, the MAP run is not.
    let path = &written.state_files[0].2;
    let bytes = std::fs::read(path).unwrap();
    let parsed = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let StateHandle::KeyGroups(kg) = &parsed.operators[0].subtask_states[0].managed_keyed_state
    else {
        panic!("key-groups handle")
    };
    let start = kg.offsets[target as usize] as usize;
    // Truncate the block's last 12 bytes (the end mark and part of the last map entry), but keep
    // the file as long as the handle says, by zero-padding: the block itself is malformed.
    let end = bytes.len();
    let mut cut = bytes.clone();
    for b in &mut cut[end - 12..] {
        *b = 0;
    }
    assert!(start < end - 12);
    std::fs::write(path, &cut).unwrap();

    let r = reader(&dir);
    let id = op.id_hex();
    let mut found_malformed = 0;
    for k in &names {
        match r.get(&id, k) {
            Ok(Lookup::Found {
                record, malformed, ..
            }) => {
                assert!(record.entries.iter().any(|e| &*e.state == "v"));
                if !malformed.is_empty() {
                    found_malformed += 1;
                }
            }
            Err(ReaderError::Malformed { key, .. }) => assert_eq!(&key, k),
            other => panic!("{}: {:?}", k, other),
        }
    }
    // Every key has its VALUE entry before the defect, so all are found, and all say so.
    assert_eq!(found_malformed, names.len());
}

#[test]
fn a_zero_limit_page_still_moves() {
    let op = three_subtasks(40);
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Inline, false),
        "reader-zero",
    );
    let r = reader(&dir);
    let id = op.id_hex();
    let mut cursor = Cursor::START;
    let mut keys = BTreeSet::new();
    for _ in 0..1000 {
        let page = r.key_page(&id, &cursor, 0).unwrap();
        assert_eq!(page.keys.len(), 1);
        assert!(keys.insert(page.keys[0].key.clone()));
        match page.next {
            Some(n) => cursor = n,
            None => break,
        }
    }
    assert_eq!(keys, expected_keys(&op));
}

/// A key with far more MAP entries than the entry limit: its record is bounded and says how many
/// there are, and `entries` pages through the rest, agreeing with an unbounded reader.
#[test]
fn a_key_with_many_map_entries_is_bounded_and_continued() {
    let mut op = KeyedOperator::new(0x52, 2, 128, Ser::String)
        .state("count", Kind::Value(Ser::Long))
        .state("big", Kind::Map(Ser::Int, Ser::Long));
    for i in 0..5_000 {
        op.items.push(Item::MapEntry {
            state: 1,
            key: Val::str("hot"),
            map_key: Val::Int(i),
            value: Some(Val::Long(i as i64 * 3)),
        });
    }
    for k in ["hot", "cold"] {
        op.items.push(Item::Value {
            state: 0,
            key: Val::str(k),
            value: Val::Long(7),
        });
    }
    for layout in [(Storage::Inline, false), (Storage::Files, true)] {
        let dir = write(
            &spec_with(std::slice::from_ref(&op), layout.0, layout.1),
            "reader-many",
        );
        let id = op.id_hex();
        let r = reader(&dir).with_entry_limit(100);
        let Lookup::Found { record, .. } = r.get(&id, "hot").unwrap() else {
            panic!("hot is there")
        };
        assert_eq!(record.entries.len(), 101, "100 map entries + the value");
        assert!(record.is_truncated());
        assert_eq!(record.truncated.len(), 1);
        assert_eq!(&*record.truncated[0].state, "big");
        assert_eq!(
            (record.truncated[0].total, record.truncated[0].returned),
            (5_000, 100)
        );
        // Paging hands out the same bounded record.
        let (paged, _) = page_all(&r, &id, 1);
        let hot = paged.iter().find(|k| k.key == "hot").unwrap();
        assert_eq!(hot, &record);
        assert!(!paged
            .iter()
            .find(|k| k.key == "cold")
            .unwrap()
            .is_truncated());

        // The continuation, page by page, equals the unbounded record's entries.
        let full = reader(&dir).with_entry_limit(usize::MAX);
        let Lookup::Found { record: all, .. } = full.get(&id, "hot").unwrap() else {
            panic!()
        };
        assert!(!all.is_truncated());
        let want: Vec<_> = all.entries.iter().filter(|e| &*e.state == "big").collect();
        assert_eq!(want.len(), 5_000);
        let mut got = Vec::new();
        let mut offset = Some(0);
        while let Some(o) = offset {
            let page = r.entries(&id, &record.position, "big", o, 777).unwrap();
            assert_eq!(page.total, 5_000);
            assert_eq!(page.key, "hot");
            assert!(page.entries.len() <= 777);
            got.extend(page.entries);
            offset = page.next;
        }
        assert_eq!(got.iter().collect::<Vec<_>>(), want);
        // The labels are shared, not copied per entry.
        assert!(Arc::ptr_eq(&got[0].state, &got[4_999].state));
        // Other states and positions.
        let v = r.entries(&id, &record.position, "count", 0, 10).unwrap();
        assert_eq!((v.total, v.entries.len(), v.next), (1, 1, None));
        assert!(matches!(
            r.entries(&id, &record.position, "nope", 0, 10),
            Err(ReaderError::NotFound { .. })
        ));
        let mut past = record.position;
        past.ordinal += 1_000;
        assert!(matches!(
            r.entries(&id, &past, "big", 0, 10),
            Err(ReaderError::NotFound { .. })
        ));

        // A scan's matches are capped too.
        let r = reader(&dir).with_scan_match_limit(1);
        let page = r
            .scan(&id, &Cursor::START, u64::MAX, &mut |_| true)
            .unwrap();
        assert_eq!(page.matches.len(), 1);
        let rest = r
            .scan(&id, &page.next.unwrap(), u64::MAX, &mut |_| true)
            .unwrap();
        assert_eq!(rest.matches.len(), 1);
        assert_ne!(rest.matches[0].key, page.matches[0].key);
    }
}

/// String keys holding lone surrogates (valid Java strings, impossible Rust ones) stay distinct
/// and round-trip through `get`, alone and inside a tuple; a literal `\u` too.
#[test]
fn lone_surrogate_string_keys_round_trip_through_get() {
    let raw_keys: Vec<Vec<u16>> = vec![
        vec![0x78, 0xD800],                  // "x" + lone high surrogate
        vec![0x78, 0xDC00],                  // "x" + lone low surrogate
        vec![0x78, 0xFFFD],                  // "x" + U+FFFD itself
        "x\\uD800".encode_utf16().collect(), // a literal backslash-u
        "x\\\\uD800".encode_utf16().collect(),
        vec![0xD83D, 0xDE00], // a real pair (an emoji)
        "plain".encode_utf16().collect(),
    ];
    let mut op = KeyedOperator::new(0x53, 2, 128, Ser::String).state("v", Kind::Value(Ser::Long));
    let mut tuple_op = KeyedOperator::new(0x54, 1, 128, Ser::Tuple(&[Ser::String, Ser::Long]))
        .state("v", Kind::Value(Ser::Long));
    for (i, units) in raw_keys.iter().enumerate() {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Utf16(units.clone()),
            value: Val::Long(i as i64),
        });
        tuple_op.items.push(Item::Value {
            state: 0,
            key: Val::Tuple(vec![Val::Utf16(units.clone()), Val::Long(1)]),
            value: Val::Long(i as i64),
        });
    }
    let dir = write(
        &spec_with(&[op.clone(), tuple_op.clone()], Storage::Inline, false),
        "reader-surrogates",
    );
    let r = reader(&dir);
    for o in [&op, &tuple_op] {
        let id = o.id_hex();
        let (keys, _) = page_all(&r, &id, usize::MAX);
        assert_eq!(keys.len(), raw_keys.len(), "distinct keys stay distinct");
        let rendered: BTreeSet<&str> = keys.iter().map(|k| k.key.as_str()).collect();
        assert_eq!(rendered.len(), raw_keys.len());
        for k in &keys {
            match r.get(&id, &k.key).unwrap() {
                Lookup::Found { record, .. } => assert_eq!(record.key, k.key),
                other => panic!("{:?}: {:?}", k.key, other),
            }
        }
    }
    let (keys, _) = page_all(&r, &op.id_hex(), usize::MAX);
    let rendered: BTreeSet<&str> = keys.iter().map(|k| k.key.as_str()).collect();
    for want in [
        "x\\uD800",
        "x\\uDC00",
        "x\u{FFFD}",
        "x\\\\uD800",
        "x\\\\\\\\uD800",
        "\u{1F600}",
        "plain",
    ] {
        assert!(rendered.contains(want), "{:?} not in {:?}", want, rendered);
    }
}

/// An incremental (or changelog) keyed handle is reported as unreadable, never as a subtask
/// without state.
#[test]
fn an_incremental_keyed_handle_is_reported() {
    use flink_explorer::parser::state_handle::IncrementalRemoteKeyedStateHandle;
    let op = three_subtasks(300);
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Inline, false),
        "reader-incremental",
    );
    let mut savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let st = &mut savepoint.operators[0].subtask_states[1];
    let StateHandle::KeyGroups(kg) = &st.managed_keyed_state else {
        panic!("key-groups handle")
    };
    let (start, num) = (kg.start_key_group, kg.num_key_groups);
    st.managed_keyed_state =
        StateHandle::IncrementalRemoteKeyed(IncrementalRemoteKeyedStateHandle {
            start_key_group: start,
            num_key_groups: num,
            checkpoint_id: 1,
            backend_id: "backend".to_string(),
            checkpointed_size: 0,
            meta_state_handle: StreamStateHandle::Null,
            shared_state: Vec::new(),
            private_state: Vec::new(),
        });
    let r = StateReader::from_savepoint(savepoint, &dir);
    let id = op.id_hex();
    assert_eq!(r.operators()[0].keyed_subtasks, 3);
    let states = r.states(&id).unwrap();
    assert_eq!(states.errors.len(), 1);
    assert!(
        states.errors[0].contains("unsupported keyed handle"),
        "{:?}",
        states.errors
    );
    let (lost_kgs, mut seen) = (synth::range_for_subtask(128, 3, 1), false);
    for i in 0..300 {
        let text = format!("key-{:07}", i);
        let kg = key_group_for_hash(java_string_hash(&text), 128);
        if (lost_kgs.0..=lost_kgs.1).contains(&kg) {
            match r.get(&id, &text) {
                Err(ReaderError::Unreadable {
                    subtask: 1, error, ..
                }) => {
                    assert!(
                        error.contains("IncrementalRemoteKeyedStateHandle"),
                        "{}",
                        error
                    );
                    seen = true;
                }
                other => panic!("{:?}", other),
            }
        }
    }
    assert!(seen);
    let mut cursor = Cursor::START;
    let mut unreadable = Vec::new();
    loop {
        let page = r.key_page(&id, &cursor, 64).unwrap();
        unreadable.extend(page.unreadable);
        match page.next {
            Some(n) => cursor = n,
            None => break,
        }
    }
    assert_eq!(unreadable.len(), 1);
    assert_eq!(
        (
            unreadable[0].first_key_group as i32,
            unreadable[0].last_key_group as i32
        ),
        lost_kgs
    );
}

/// A user subclass of a Flink tuple may override `hashCode()`: its keys need a scan.
#[test]
fn a_tuple_key_of_a_user_class_needs_a_scan() {
    let mut op = KeyedOperator::new(0x55, 1, 128, Ser::Tuple(&[Ser::String, Ser::Long]))
        .state("v", Kind::Value(Ser::Long));
    op.items.push(Item::Value {
        state: 0,
        key: Val::Tuple(vec![Val::str("a"), Val::Long(1)]),
        value: Val::Long(1),
    });
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Inline, false),
        "reader-tuple-class",
    );
    let text = r#"["a",1]"#;
    assert!(matches!(
        reader(&dir).get(&op.id_hex(), text).unwrap(),
        Lookup::Found { .. }
    ));
    // Same length, so every offset stays valid.
    let flink = b"org.apache.flink.api.java.tuple.Tuple2";
    let user = b"com.example.myjob.keys.CustomerTuple2";
    let user = &[&user[..], b"x"].concat();
    assert_eq!(flink.len(), user.len());
    let meta = dir.join("_metadata");
    let mut bytes = std::fs::read(&meta).unwrap();
    let at = bytes
        .windows(flink.len())
        .position(|w| w == flink)
        .expect("the tuple class is in the header");
    bytes[at..at + flink.len()].copy_from_slice(user);
    std::fs::write(&meta, &bytes).unwrap();
    match reader(&dir).get(&op.id_hex(), text).unwrap() {
        Lookup::NeedsScan { reason } => assert!(reason.contains("CustomerTuple2x"), "{}", reason),
        other => panic!("{:?}", other),
    }
}

/// Date and LocalDate keys are located by their `hashCode()`.
#[test]
fn date_keys_are_found_by_get() {
    let millis = [
        0i64,
        1,
        -1,
        1_700_000_000_123,
        -62_135_596_800_000,
        253_402_300_799_999,
    ];
    let mut op = KeyedOperator::new(0x56, 2, 128, Ser::Date).state("v", Kind::Value(Ser::Long));
    for (i, m) in millis.iter().enumerate() {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Date(*m),
            value: Val::Long(i as i64),
        });
    }
    let dates = [(2024, 2, 29), (1970, 1, 1), (-5, 12, 31), (12345, 6, 7)];
    let mut ld =
        KeyedOperator::new(0x57, 2, 128, Ser::LocalDate).state("v", Kind::Value(Ser::Long));
    for (i, (y, m, d)) in dates.iter().enumerate() {
        ld.items.push(Item::Value {
            state: 0,
            key: Val::LocalDate(*y, *m, *d),
            value: Val::Long(i as i64),
        });
    }
    let dir = write(
        &spec_with(&[op.clone(), ld.clone()], Storage::Files, true),
        "reader-dates",
    );
    let r = reader(&dir);
    for o in [&op, &ld] {
        let id = o.id_hex();
        let (keys, _) = page_all(&r, &id, usize::MAX);
        assert_eq!(keys.len(), o.items.len());
        for k in &keys {
            match r.get(&id, &k.key).unwrap() {
                Lookup::Found { record, stats, .. } => {
                    assert_eq!(record.key, k.key);
                    assert_eq!(stats.blocks, 1);
                }
                other => panic!("{:?}: {:?}", k.key, other),
            }
        }
    }
    // Epoch millis work too; a date that does not render back is refused, not normalised.
    assert!(matches!(
        r.get(&op.id_hex(), "1700000000123").unwrap(),
        Lookup::Found { .. }
    ));
    assert!(matches!(
        r.get(&op.id_hex(), "2020-02-30T00:00:00Z").unwrap(),
        Lookup::NeedsScan { .. }
    ));
    assert!(matches!(
        r.get(&ld.id_hex(), "2024-2-29").unwrap(),
        Lookup::NeedsScan { .. }
    ));
}

/// The reader holds at most its open-file limit, whatever the parallelism.
#[test]
fn open_files_are_bounded() {
    let mut op = synth::sample_operator(400);
    op.parallelism = 8;
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Files, false),
        "reader-fds",
    );
    let r = reader(&dir).with_open_files(3);
    let id = op.id_hex();
    let (keys, _) = page_all(&r, &id, 10);
    assert_eq!(keys.len(), expected_keys(&op).len());
    assert!(r.open_files() <= 3, "{}", r.open_files());
    assert!(r.open_files() > 0);
}

#[test]
fn operators_and_states_come_from_metadata_and_headers() {
    let ops = [three_subtasks(10), long_keys(10)];
    let dir = write(&spec_with(&ops, Storage::Files, false), "reader-meta");
    let r = reader(&dir);
    let infos = r.operators();
    assert_eq!(infos.len(), 2);
    assert_eq!(infos[0].id, ops[0].id_hex());
    assert_eq!((infos[0].parallelism, infos[0].max_parallelism), (3, 128));
    assert_eq!(infos[1].keyed_subtasks, 4);
    // A unique prefix names an operator.
    assert_eq!(r.operator_index(&ops[1].id_hex()[..6]).unwrap(), 1);
    assert!(r.operator_index("ff").is_err());

    let s = r.states(&ops[0].id_hex()).unwrap();
    assert_eq!(s.key_type, "String");
    let names: Vec<(&str, &str, &str)> = s
        .states
        .iter()
        .map(|i| {
            (
                i.name.as_str(),
                i.state_type.as_str(),
                i.value_type.as_str(),
            )
        })
        .collect();
    assert_eq!(
        names,
        [
            ("counter", "VALUE", "Long"),
            ("label", "VALUE", "String"),
            ("stats", "MAP", "Map<String,Long>"),
            ("history", "LIST", "List<Long>"),
            ("_timer_state/processing_user-timers", "TIMER", "Date"),
        ]
    );
    let s = r.states(&ops[1].id_hex()).unwrap();
    assert_eq!(s.key_type, "Long");
    let total = s.states.iter().find(|i| i.name == "total").unwrap();
    assert_eq!(total.subtasks, 4);
    let timers = s
        .states
        .iter()
        .find(|i| i.name.starts_with("_timer"))
        .unwrap();
    assert_eq!(timers.subtasks, 2);
}

/// One reader can serve concurrent requests (the web UI shares it between handlers).
#[test]
fn a_reader_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StateReader<'static>>();

    let op = three_subtasks(100);
    let dir = write(
        &spec_with(std::slice::from_ref(&op), Storage::Files, true),
        "reader-threads",
    );
    let r = reader(&dir);
    let id = op.id_hex();
    let (keys, _) = page_all(&r, &id, usize::MAX);
    std::thread::scope(|s| {
        for t in 0..4 {
            let (r, id, keys) = (&r, &id, &keys);
            s.spawn(move || {
                for k in keys.iter().skip(t).step_by(4) {
                    assert!(matches!(r.get(id, &k.key).unwrap(), Lookup::Found { .. }));
                }
            });
        }
    });
}
