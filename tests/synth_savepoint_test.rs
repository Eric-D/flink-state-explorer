//! Fixture-free tests on synthetic savepoints (see `tests/common/synth.rs`): they run in CI, and
//! they are the only CI coverage of file-backed (`RelativeFileStateHandle`), multi-subtask and
//! compressed savepoints.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use serde_json::{json, Value};

fn write(spec: &Spec, tag: &str) -> std::path::PathBuf {
    let dir = synth::temp_dir(tag);
    synth::write_savepoint(spec, &dir);
    dir
}

fn rows(dir: &Path, state: &str) -> Vec<Value> {
    let savepoint =
        parser::parse_metadata(&dir.join("_metadata")).expect("synthetic _metadata parses");
    let mut out = Vec::new();
    export_state_ndjson(&savepoint, dir, None, state, None, &mut out).expect("export");
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// The rows an export of `state` must produce, derived from the items alone.
fn expected_rows(op: &KeyedOperator, state: usize) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for item in op.items.iter().filter(|i| i.state() == state) {
        let kg = synth::key_group_for_hash(item.key().java_hash(), op.max_parallelism);
        let mut row = json!({ "key": item.key().render(), "key_group": kg });
        match item {
            Item::Value { value, .. } => row["value"] = value.json(),
            Item::MapEntry { map_key, value, .. } => {
                row["map_key"] = json!(map_key.render());
                row["value"] = value.as_ref().map_or(Value::Null, Val::json);
            }
            Item::List { values, .. } => {
                row["value"] = Value::Array(values.iter().map(Val::json).collect())
            }
            Item::Timer { .. } => continue,
        }
        out.insert(row.to_string());
    }
    out
}

fn projected(rows: &[Value]) -> BTreeSet<String> {
    rows.iter()
        .map(|r| {
            let mut p =
                json!({ "key": r["key"], "key_group": r["key_group"], "value": r["value"] });
            if let Some(mk) = r.get("map_key") {
                p["map_key"] = mk.clone();
            }
            p.to_string()
        })
        .collect()
}

#[test]
fn header_matches_a_real_flink_header() {
    // BigStateJob's operator, as Flink 1.20 wrote it (tests/fixtures/savepoint-big, opt-in:
    // `mvn verify -Dbig.savepoint=true -Dit.test=BigSavepointGeneratorIT`). Locally only.
    let dir = Path::new("tests/fixtures/savepoint-big");
    let Some(file) = std::fs::read_dir(dir).ok().and_then(|mut it| {
        it.find_map(|e| {
            let p = e.ok()?.path();
            (p.file_name()? != "_metadata" && !p.file_name()?.to_string_lossy().starts_with('.'))
                .then_some(p)
        })
    }) else {
        eprintln!("skipped: {} not generated", dir.display());
        return;
    };
    let states = [
        synth::StateDef::new("big-stats", Kind::Map(Ser::String, Ser::Long)),
        synth::StateDef::new("big-label", Kind::Value(Ser::String)),
        synth::StateDef::new("big-counter", Kind::Value(Ser::Long)),
        synth::StateDef::new("_timer_state/processing_user-timers", Kind::Timer),
        synth::StateDef::new("_timer_state/event_user-timers", Kind::Timer),
    ];
    let ours = synth::header_bytes(Ser::String, &states, false);
    let real = std::fs::read(&file).unwrap();
    assert_eq!(
        &real[..ours.len()],
        ours.as_slice(),
        "synthetic header differs from Flink's"
    );
    // ... and Flink's header ends exactly there: the first key-group starts right after it.
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let first_offset = savepoint
        .operators
        .iter()
        .flat_map(|op| &op.subtask_states)
        .find_map(|s| match &s.managed_keyed_state {
            parser::state_handle::StateHandle::KeyGroups(kg) => {
                kg.offsets.iter().copied().filter(|&o| o > 0).min()
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(first_offset as usize, ours.len());
}

#[test]
fn file_backed_savepoint_lists_and_exports_every_state() {
    let op = synth::sample_operator(400);
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = write(&spec, "files");

    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let listed: Vec<(String, String)> = list_states(&savepoint, &dir)
        .into_iter()
        .map(|(_, name, ty)| (name, ty))
        .collect();
    assert_eq!(
        listed,
        vec![
            ("counter".to_string(), "VALUE".to_string()),
            ("label".to_string(), "VALUE".to_string()),
            ("stats".to_string(), "MAP".to_string()),
            ("history".to_string(), "LIST".to_string()),
            (
                "_timer_state/processing_user-timers".to_string(),
                "TIMER".to_string()
            ),
        ]
    );

    for (i, name) in ["counter", "label", "stats", "history"].iter().enumerate() {
        let got = rows(&dir, name);
        assert_eq!(projected(&got), expected_rows(&op, i), "state {}", name);
        // Both subtasks contribute, each within its own key-group range.
        let subtasks: BTreeSet<i64> = got.iter().map(|r| r["subtask"].as_i64().unwrap()).collect();
        assert_eq!(subtasks, BTreeSet::from([0, 1]), "state {}", name);
        for r in &got {
            let (start, end) =
                synth::range_for_subtask(128, 2, r["subtask"].as_i64().unwrap() as i32);
            let kg = r["key_group"].as_i64().unwrap() as i32;
            assert!(
                (start..=end).contains(&kg),
                "{} outside {}..={}",
                kg,
                start,
                end
            );
        }
    }
}

#[test]
fn inline_file_backed_and_compressed_savepoints_read_alike() {
    let op = synth::sample_operator(200);
    let mut by_layout = Vec::new();
    for (storage, compressed) in [
        (Storage::Inline, false),
        (Storage::Files, false),
        (Storage::Files, true),
        (Storage::Inline, true),
    ] {
        let mut spec = Spec::new(storage);
        spec.compressed = compressed;
        spec.operators.push(op.clone());
        let dir = write(&spec, "layout");
        let all: Vec<BTreeSet<String>> = ["counter", "label", "stats", "history"]
            .iter()
            .map(|s| projected(&rows(&dir, s)))
            .collect();
        by_layout.push(((storage, compressed), all));
    }
    let (first_layout, first) = &by_layout[0];
    for (layout, all) in &by_layout[1..] {
        assert_eq!(
            all, first,
            "{:?} reads differently from {:?}",
            layout, first_layout
        );
    }
}

#[test]
fn index_path_sees_every_key_of_every_subtask() {
    let op = synth::sample_operator(300);
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = write(&spec, "index");

    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let index = build_index(&savepoint, 0, 0, &dir);
    let ks = index.operators[0]
        .keyed_state
        .as_ref()
        .expect("keyed state indexed");
    assert_eq!(ks.total_keys, 300);
    let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "counter",
            "label",
            "stats",
            "history",
            "_timer_state/processing_user-timers"
        ]
    );

    let counters: BTreeMap<String, Value> = ks
        .partitions
        .iter()
        .map(|p| {
            let raw = p.values[0].as_deref().expect("every key has a counter");
            let v = flink_explorer::parser::value::decode_state_value(raw, "Long", None, None);
            (p.key.clone(), v)
        })
        .collect();
    for i in 0..300 {
        assert_eq!(counters[&format!("key-{:07}", i)], json!(i as i64 * 31 + 7));
    }
}

#[test]
fn keys_of_other_types_and_two_byte_key_group_prefixes() {
    // maxParallelism > 128 switches the key-group prefix to 2 bytes.
    let mut op = KeyedOperator::new(0x51, 3, 4096, Ser::Long).state("v", Kind::Value(Ser::Int));
    for k in [-5i64, 0, 1, 42, 1 << 40, i64::MAX] {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Long(k),
            value: Val::Int((k % 1000) as i32),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = write(&spec, "longkeys");
    assert_eq!(projected(&rows(&dir, "v")), expected_rows(&op, 0));
}
