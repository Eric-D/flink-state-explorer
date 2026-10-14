//! Fixture-free tests on synthetic savepoints (see `tests/common/synth.rs`): they run in CI, and
//! they are the only CI coverage of file-backed (`RelativeFileStateHandle`), multi-subtask and
//! compressed savepoints.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;

use common::synth::{self, Item, KeyedOperator, Kind, RunOrder, Ser, Spec, Storage, TempDir, Val};
use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use flink_explorer::parser::state_handle::{StateHandle, StreamStateHandle};
use serde_json::{json, Value};

const LAYOUTS: [(Storage, bool); 4] = [
    (Storage::Inline, false),
    (Storage::Files, false),
    (Storage::Files, true),
    (Storage::Inline, true),
];

fn write(spec: &Spec, tag: &str) -> TempDir {
    let dir = synth::temp_dir(tag);
    synth::write_savepoint(spec, &dir);
    dir
}

fn spec_with(op: &KeyedOperator, storage: Storage, compressed: bool) -> Spec {
    let mut spec = Spec::new(storage);
    spec.compressed = compressed;
    spec.operators.push(op.clone());
    spec
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

/// Epoch milliseconds as the explorer renders a Date: `YYYY-MM-DDTHH:MM:SS.mmmZ`. Written out
/// here (civil-from-days) so the expectation does not come from the code under test.
fn iso_millis(ms: i64) -> String {
    let (days, rem) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y,
        m,
        d,
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
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
            Item::Timer { timestamp, .. } => row["value"] = json!(iso_millis(*timestamp)),
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

const SAMPLE_STATES: [&str; 5] = [
    "counter",
    "label",
    "stats",
    "history",
    "_timer_state/processing_user-timers",
];

/// `BigStateJob`'s operator (test-flink-job), whose keyed-stream header Flink 1.20 wrote into
/// `tests/data/big-state-job-keyed-header.bin`.
fn big_state_job_states() -> [synth::StateDef; 5] {
    [
        synth::StateDef::new("big-stats", Kind::Map(Ser::String, Ser::Long)),
        synth::StateDef::new("big-label", Kind::Value(Ser::String)),
        synth::StateDef::new("big-counter", Kind::Value(Ser::Long)),
        synth::StateDef::new("_timer_state/processing_user-timers", Kind::Timer),
        synth::StateDef::new("_timer_state/event_user-timers", Kind::Timer),
    ]
}

#[test]
fn header_matches_a_real_flink_header() {
    let golden: &[u8] = include_bytes!("data/big-state-job-keyed-header.bin");
    let ours = synth::header_bytes(Ser::String, &big_state_job_states(), false);
    assert_eq!(
        ours.as_slice(),
        golden,
        "synthetic header differs from Flink's"
    );
}

#[test]
fn golden_header_is_still_what_flink_writes() {
    // Locally only: re-derive the golden header from the opt-in fixture, through the handle
    // `_metadata` references (`mvn verify -Dbig.savepoint=true -Dit.test=BigSavepointGeneratorIT`).
    let dir = Path::new("tests/fixtures/savepoint-big");
    let Ok(savepoint) = parser::parse_metadata(&dir.join("_metadata")) else {
        eprintln!("skipped: {} not generated", dir.display());
        return;
    };
    let kg = savepoint
        .operators
        .iter()
        .flat_map(|op| &op.subtask_states)
        .find_map(|s| match &s.managed_keyed_state {
            StateHandle::KeyGroups(kg) => Some(kg.clone()),
            _ => None,
        })
        .expect("a keyed handle");
    let StreamStateHandle::Relative(rel) = &kg.delegate_handle else {
        panic!("savepoint-big should be file-backed");
    };
    let header_len = kg.offsets.iter().copied().filter(|&o| o > 0).min().unwrap() as usize;
    let mut real = vec![0u8; header_len];
    std::fs::File::open(dir.join(&rel.relative_path))
        .unwrap()
        .read_exact(&mut real)
        .unwrap();
    let golden: &[u8] = include_bytes!("data/big-state-job-keyed-header.bin");
    assert_eq!(
        real.as_slice(),
        golden,
        "regenerate tests/data/big-state-job-keyed-header.bin"
    );
}

#[test]
fn metadata_ends_with_flinks_checkpoint_properties() {
    let mut spec = Spec::new(Storage::Inline);
    spec.operators.push(synth::sample_operator(3));
    let dir = write(&spec, "trailer");
    let meta = std::fs::read(dir.join("_metadata")).unwrap();
    let trailer: &[u8] = include_bytes!("data/savepoint-checkpoint-properties.bin");
    assert!(meta.ends_with(trailer));
    assert_eq!(
        &trailer[..4],
        &[0xac, 0xed, 0x00, 0x05],
        "Java serialization stream"
    );
}

#[test]
fn file_backed_savepoint_lists_and_exports_every_state() {
    let op = synth::sample_operator(400);
    let dir = write(&spec_with(&op, Storage::Files, false), "files");

    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let listing = list_states(&savepoint, &dir);
    assert_eq!(listing.errors, Vec::<String>::new());
    let listed: Vec<(String, String)> = listing
        .states
        .into_iter()
        .map(|(_, name, ty)| (name, ty))
        .collect();
    let types = ["VALUE", "VALUE", "MAP", "LIST", "TIMER"];
    let want: Vec<(String, String)> = SAMPLE_STATES
        .iter()
        .zip(types)
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect();
    assert_eq!(listed, want);

    for (i, name) in SAMPLE_STATES.iter().enumerate() {
        let got = rows(&dir, name);
        let expected = expected_rows(&op, i);
        assert!(!expected.is_empty(), "state {} has no items", name);
        assert_eq!(projected(&got), expected, "state {}", name);
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
fn every_layout_exports_the_expected_rows() {
    let op = synth::sample_operator(200);
    for (storage, compressed) in LAYOUTS {
        let dir = write(&spec_with(&op, storage, compressed), "layout");
        for (i, name) in SAMPLE_STATES.iter().enumerate() {
            let expected = expected_rows(&op, i);
            assert!(!expected.is_empty());
            assert_eq!(
                projected(&rows(&dir, name)),
                expected,
                "{:?}, compressed {}: state {}",
                storage,
                compressed,
                name
            );
        }
    }
}

#[test]
fn state_runs_in_any_order_read_the_same() {
    // Flink's heap backend does not write a key-group's state runs in stateId order.
    let op = synth::sample_operator(150);
    for order in [RunOrder::HashLike, RunOrder::Ascending] {
        let mut spec = spec_with(&op, Storage::Files, false);
        spec.run_order = order;
        let dir = write(&spec, "order");
        for (i, name) in SAMPLE_STATES.iter().enumerate() {
            assert_eq!(
                projected(&rows(&dir, name)),
                expected_rows(&op, i),
                "{:?}: state {}",
                order,
                name
            );
        }
    }
}

#[test]
fn index_path_sees_every_key_of_every_subtask_in_every_layout() {
    let op = synth::sample_operator(300);
    for (storage, compressed) in LAYOUTS {
        let what = format!("{:?}, compressed {}", storage, compressed);
        let dir = write(&spec_with(&op, storage, compressed), "index");
        let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let index = build_index(&savepoint, 0, 0, &dir);
        let ks = index.operators[0]
            .keyed_state
            .as_ref()
            .unwrap_or_else(|| panic!("{}: keyed state indexed", what));
        assert_eq!(ks.total_keys, 300, "{}", what);
        let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, SAMPLE_STATES, "{}", what);

        let mut seen: BTreeMap<String, Value> = BTreeMap::new();
        for p in &ks.partitions {
            let want_kg = synth::key_group_for_hash(synth::java_string_hash(&p.key), 128);
            assert_eq!(
                p.key_group as i32, want_kg,
                "{}: key-group of {}",
                what, p.key
            );
            let raw = p.values[0].as_deref().expect("every key has a counter");
            let v = flink_explorer::parser::value::decode_state_value(raw, "Long", None, None);
            seen.insert(p.key.clone(), v);
        }
        for i in 0..300 {
            assert_eq!(
                seen[&format!("key-{:07}", i)],
                json!(i as i64 * 31 + 7),
                "{}",
                what
            );
        }
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
    let dir = write(&spec_with(&op, Storage::Files, false), "longkeys");
    let expected = expected_rows(&op, 0);
    assert_eq!(expected.len(), 6);
    assert_eq!(projected(&rows(&dir, "v")), expected);
}

#[test]
fn iso_millis_matches_known_dates() {
    assert_eq!(iso_millis(0), "1970-01-01T00:00:00.000Z");
    assert_eq!(
        iso_millis(synth::SAMPLE_EPOCH_MS),
        "2026-10-13T00:00:00.000Z"
    );
    assert_eq!(iso_millis(951_782_400_123), "2000-02-29T00:00:00.123Z");
}
