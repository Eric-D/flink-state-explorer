//! Fixture-free tests on synthetic savepoints (see `tests/common/synth.rs`): they run in CI, and
//! they are the only CI coverage of file-backed (`RelativeFileStateHandle`), multi-subtask and
//! compressed savepoints.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;

use common::synth::{self, Item, KeyedOperator, Kind, RunOrder, Ser, Spec, Storage, TempDir, Val};
use flink_explorer::export_state::{export_state_ndjson, list_states, ExportStats};
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

/// Export `state` as NDJSON and return its rows, with what the export reports.
fn rows_and_stats(dir: &Path, state: &str) -> (Vec<Value>, ExportStats) {
    let savepoint =
        parser::parse_metadata(&dir.join("_metadata")).expect("synthetic _metadata parses");
    let mut out = Vec::new();
    let stats = export_state_ndjson(&savepoint, dir, None, state, None, &mut out).expect("export");
    let rows = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    (rows, stats)
}

/// The rows of `state`, from a savepoint whose every key-group must read to its end mark.
fn rows(dir: &Path, state: &str) -> Vec<Value> {
    let (rows, stats) = rows_and_stats(dir, state);
    assert_eq!(
        stats.malformed_key_groups, 0,
        "state {}: {:?}",
        state, stats.first_malformed
    );
    rows
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
fn reader_path_sees_every_key_of_every_subtask_in_every_layout() {
    let op = synth::sample_operator(300);
    for (storage, compressed) in LAYOUTS {
        let what = format!("{:?}, compressed {}", storage, compressed);
        let dir = write(&spec_with(&op, storage, compressed), "reader");
        let ks = common::keyed::keyed(&dir, &op.id_hex())
            .unwrap_or_else(|| panic!("{}: keyed state read", what));
        assert_eq!(ks.keys.len(), 300, "{}", what);
        assert_eq!(ks.state_names(), SAMPLE_STATES, "{}", what);

        let mut seen: BTreeMap<String, Value> = BTreeMap::new();
        for p in &ks.keys {
            let want_kg = synth::key_group_for_hash(synth::java_string_hash(&p.key), 128);
            assert_eq!(
                p.key_group as i32, want_kg,
                "{}: key-group of {}",
                what, p.key
            );
            let v = ks
                .value(&p.key, SAMPLE_STATES[0])
                .expect("every key has a counter");
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

/// Raw entries per state, as the reader returns them (`Keyed::entry_count`).
fn item_counts(op: &KeyedOperator) -> Vec<usize> {
    (0..op.states.len())
        .map(|s| op.items.iter().filter(|it| it.state() == s).count())
        .collect()
}

#[test]
fn long_keys_and_long_map_user_keys_survive_on_both_paths() {
    // Issue #15: a key over 10,000 bytes ended its key-group's parse, dropping its own entries
    // and every later one of that key-group, for every state, on the index (now the reader) and on the export.
    // maxParallelism 32768: 2-byte key-group prefixes, all the way up to key-group 32767.
    let mut op = KeyedOperator::new(0x15, 2, 32768, Ser::String)
        .state("long-label", Kind::Value(Ser::String))
        .state("long-stats", Kind::Map(Ser::String, Ser::Long))
        .state("long-counter", Kind::Value(Ser::Long));
    let mut keys: Vec<String> = (0..150).map(|i| format!("key-{:07}", i)).collect();
    // VALUE entry keys of exactly 10,000, 10,001 and 12,000 bytes: a 2-byte key-group prefix,
    // the StringValue (2-byte length, then one byte per ASCII char) and the VoidNamespace byte.
    for (c, total) in [("a", 10_000usize), ("b", 10_001), ("c", 12_000)] {
        keys.push(c.repeat(total - 5));
    }
    // A key Flink assigns to the last key-group, 0x7FFF: its flagged key starts with 0xFF.
    let last_kg_key = (0..)
        .map(|i| format!("last-kg-{}", i))
        .find(|k| synth::key_group_for_hash(synth::java_string_hash(k), 32768) == 32767)
        .unwrap();
    keys.push(last_kg_key.clone());

    let long_user_key = "u".repeat(12_000);
    for (i, k) in keys.iter().enumerate() {
        op.items.push(Item::Value {
            state: 0,
            key: Val::str(k),
            value: Val::Str(format!("v{}", i)),
        });
        // Every 10th key (and the long ones) holds a 12,000-byte map key *mid-run*, between two
        // short ones, so its run and the next state's run go on after it.
        let user_keys = if i % 10 == 0 || k.len() > 1000 {
            vec!["first", long_user_key.as_str(), "last"]
        } else {
            vec!["only"]
        };
        for (j, uk) in user_keys.into_iter().enumerate() {
            op.items.push(Item::MapEntry {
                state: 1,
                key: Val::str(k),
                map_key: Val::str(uk),
                value: Some(Val::Long((i * 10 + j) as i64)),
            });
        }
        op.items.push(Item::Value {
            state: 2,
            key: Val::str(k),
            value: Val::Long(i as i64),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = write(&spec, "issue15");

    // Export path: every row of every state.
    for (i, name) in ["long-label", "long-stats", "long-counter"]
        .iter()
        .enumerate()
    {
        assert_eq!(
            projected(&rows(&dir, name)),
            expected_rows(&op, i),
            "state {}",
            name
        );
    }
    let last = rows(&dir, "long-counter")
        .into_iter()
        .find(|r| r["key"] == last_kg_key.as_str())
        .expect("the key-group 32767 row");
    assert_eq!(last["key_group"], 32767);

    // Reader path (the TUI's): every entry, every key, every value.
    let ks = common::keyed::keyed(&dir, &op.id_hex()).unwrap();
    assert!(ks.malformed.is_empty(), "{:?}", ks.malformed);
    assert_eq!(ks.keys.len(), keys.len());
    let counts: Vec<usize> = ks.states.iter().map(|d| ks.entry_count(&d.name)).collect();
    assert_eq!(counts, item_counts(&op));
    for (i, k) in keys.iter().enumerate() {
        assert!(ks.record(k).is_some(), "key of {} chars not read", k.len());
        assert_eq!(ks.value(k, "long-counter"), Some(json!(i as i64)));
    }
    assert_eq!(ks.record(&last_kg_key).unwrap().key_group, 32767);
}

#[test]
fn a_header_over_50_kb_lists_every_state_on_both_paths() {
    // Issue #17: the header extractors stopped at byte 50,000, so every state registered past it
    // vanished from --list-states, the index (TUI, `i` panel, export.json; now the reader) and
    // --export-state.
    let mut op = KeyedOperator::new(0x17, 2, 128, Ser::String);
    let kind = |i: usize| match i % 3 {
        0 => Kind::Value(Ser::Long),
        1 => Kind::Map(Ser::String, Ser::Long),
        _ => Kind::List(Ser::Long),
    };
    for i in 0..320 {
        op = op.state(&format!("wide-{:03}", i), kind(i));
    }
    op = op.state("_timer_state/processing_user-timers", Kind::Timer);
    let header_len = synth::header_bytes(op.key, &op.states, false).len();
    assert!(header_len > 50_000, "header of {} bytes", header_len);

    // Data in the first states and in the last ones, past 50 KB of header.
    for k in 0..40i64 {
        let key = Val::Str(format!("key-{:03}", k));
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Long(k),
        });
        op.items.push(Item::MapEntry {
            state: 316,
            key: key.clone(),
            map_key: Val::str("m"),
            value: Some(Val::Long(-k)),
        });
        op.items.push(Item::List {
            state: 317,
            key: key.clone(),
            values: vec![Val::Long(k), Val::Long(k + 1)],
        });
        op.items.push(Item::Value {
            state: 318,
            key: key.clone(),
            value: Val::Long(k * 1000),
        });
        op.items.push(Item::Timer {
            state: 320,
            key,
            timestamp: 1_700_000_000_000 + k,
        });
    }
    let expected_states: Vec<(String, String)> = op
        .states
        .iter()
        .map(|s| {
            let ty = match s.kind {
                Kind::Value(_) => "VALUE",
                Kind::Map(..) => "MAP",
                Kind::List(_) => "LIST",
                Kind::Timer => "TIMER",
            };
            (s.name.clone(), ty.to_string())
        })
        .collect();

    for (storage, compressed) in [(Storage::Files, false), (Storage::Inline, true)] {
        let mut spec = Spec::new(storage);
        spec.compressed = compressed;
        spec.operators.push(op.clone());
        let dir = write(&spec, "issue17");
        let layout = format!("{:?}, compressed {}", storage, compressed);

        let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let listed: Vec<(String, String)> = list_states(&savepoint, &dir)
            .states
            .into_iter()
            .map(|(_, name, ty)| (name, ty))
            .collect();
        assert_eq!(listed, expected_states, "{}", layout);

        let ks = common::keyed::keyed(&dir, &op.id_hex()).unwrap();
        let described: Vec<(String, String)> = ks
            .states
            .iter()
            .map(|d| (d.name.clone(), d.state_type.clone()))
            .collect();
        assert_eq!(described, expected_states, "{}", layout);
        let counts: Vec<usize> = ks.states.iter().map(|d| ks.entry_count(&d.name)).collect();
        assert_eq!(counts, item_counts(&op), "{}", layout);
        assert_eq!(ks.states[318].value_type, "Long", "{}", layout);
        assert_eq!(ks.keys.len(), 40, "{}", layout);

        for (id, name) in [(0, "wide-000"), (316, "wide-316"), (317, "wide-317")] {
            assert_eq!(
                projected(&rows(&dir, name)),
                expected_rows(&op, id),
                "{} {}",
                layout,
                name
            );
        }
        assert_eq!(
            projected(&rows(&dir, "wide-318")),
            expected_rows(&op, 318),
            "{}",
            layout
        );
    }
}

#[test]
fn the_reader_reads_its_header_from_the_header_only() {
    // Issue #17: on the index path (now the reader) the header extractors ran over the whole state file, so state
    // *values* that happen to look like a StateMetaInfoSnapshot were taken for states. This value
    // spells `[utf "phantom"][int KEY_VALUE][int 1 option][utf KEYED_STATE_TYPE][utf VALUE]`
    // (a StringValue writes chars below 0x80 as single bytes, U+0000 included).
    let mut fake = vec![0u8, 7];
    fake.extend_from_slice(b"phantom");
    fake.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1, 0, 16]);
    fake.extend_from_slice(b"KEYED_STATE_TYPE");
    fake.extend_from_slice(&[0, 5]);
    fake.extend_from_slice(b"VALUE");
    let fake: String = fake.into_iter().map(char::from).collect();

    let mut op =
        KeyedOperator::new(0x18, 2, 128, Ser::String).state("note", Kind::Value(Ser::String));
    for k in 0..20 {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Str(format!("key-{}", k)),
            value: Val::Str(fake.clone()),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = write(&spec, "header-only");

    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let listed: Vec<String> = list_states(&savepoint, &dir)
        .states
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    assert_eq!(listed, ["note"]);
    let ks = common::keyed::keyed(&dir, &op.id_hex()).unwrap();
    assert_eq!(
        ks.state_names(),
        ["note"],
        "the reader must see the states list_states sees"
    );
    assert_eq!(ks.entry_count("note"), 20);
    assert_eq!(projected(&rows(&dir, "note")), expected_rows(&op, 0));
}

#[test]
fn a_state_name_is_found_at_its_own_record() {
    // Each state's serializers are read after its name. The name used to be located by its first
    // byte match anywhere in the header, and "m" first matches inside the key serializer's class
    // name (`org.apache.flink.api.common…`), so its MAP value type was misread. Names are now
    // located at their `StateMetaInfoSnapshot` (the same single pass that finds the state count).
    let mut op = KeyedOperator::new(0x19, 1, 128, Ser::String)
        .state("m", Kind::Map(Ser::String, Ser::Long))
        .state("a", Kind::Value(Ser::Long));
    for k in 0..10 {
        let key = Val::Str(format!("key-{}", k));
        op.items.push(Item::MapEntry {
            state: 0,
            key: key.clone(),
            map_key: Val::str("clicks"),
            value: Some(Val::Long(k)),
        });
        op.items.push(Item::Value {
            state: 1,
            key,
            value: Val::Long(-k),
        });
    }
    let mut spec = Spec::new(Storage::Inline);
    spec.operators.push(op.clone());
    let dir = write(&spec, "short-names");

    let ks = common::keyed::keyed(&dir, &op.id_hex()).unwrap();
    let types: Vec<&str> = ks.states.iter().map(|d| d.value_type.as_str()).collect();
    assert_eq!(types, ["Map<String,Long>", "Long"]);
    assert_eq!(projected(&rows(&dir, "m")), expected_rows(&op, 0));
    assert_eq!(projected(&rows(&dir, "a")), expected_rows(&op, 1));
}

#[test]
fn a_malformed_key_group_is_reported_by_the_export_the_reader_and_the_cli() {
    // Issue #15: a key-group that cannot be read to its end mark used to end the parse silently,
    // passing a partial read off as the whole state.
    let op = synth::sample_operator(300);
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = synth::temp_dir("malformed");
    let written = synth::write_savepoint(&spec, &dir);

    // Corrupt one key-group of subtask 0 in place — the file keeps its length — by giving its
    // first entry a value length that runs past the block: `[short stateId][int keyLen][key]
    // [int valueLen]`.
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let StateHandle::KeyGroups(kg) = &savepoint.operators[0].subtask_states[0].managed_keyed_state
    else {
        panic!("keyed state expected");
    };
    let (i, offset) = kg
        .offsets
        .iter()
        .enumerate()
        .filter(|(_, &o)| o > 0)
        .nth(3)
        .expect("a fourth non-empty key-group");
    let bad_kg = (kg.start_key_group + i as i32) as i64;
    let (_, _, path) = written
        .state_files
        .iter()
        .find(|(o, s, _)| *o == 0 && *s == 0)
        .unwrap();
    let mut bytes = std::fs::read(path).unwrap();
    let at = *offset as usize;
    let key_len = i32::from_be_bytes(bytes[at + 2..at + 6].try_into().unwrap()) as usize;
    let value_len_at = at + 6 + key_len;
    bytes[value_len_at..value_len_at + 4].copy_from_slice(&0x7fff_0000i32.to_be_bytes());
    std::fs::write(path, &bytes).unwrap();

    // Export: the key-group is counted and named; only its rows are missing.
    let (got, stats) = rows_and_stats(&dir, "counter");
    assert_eq!(stats.malformed_key_groups, 1, "{:?}", stats);
    let first = stats.first_malformed.clone().unwrap();
    assert!(
        first.starts_with(&format!("subtask 0, key-group {}: truncated", bad_kg)),
        "{}",
        first
    );
    let expected: BTreeSet<String> = expected_rows(&op, 0)
        .into_iter()
        .filter(|r| serde_json::from_str::<Value>(r).unwrap()["key_group"] != bad_kg)
        .collect();
    assert!(expected.len() < 300, "the bad key-group held rows");
    assert_eq!(projected(&got), expected);

    // Reader (the TUI's path): the same key-group, counted across subtasks, and shown in the
    // `i` panel.
    let ks = common::keyed::keyed(&dir, &op.id_hex()).unwrap();
    assert_eq!(ks.malformed.len(), 1, "{:?}", ks.malformed);
    assert!(
        ks.malformed[0].starts_with(&format!("subtask 0, key-group {}: truncated", bad_kg)),
        "{}",
        ks.malformed[0]
    );
    assert_eq!(ks.keys.len(), expected.len());
    let summary = &flink_explorer::operators::summarize(&savepoint, &dir)[0];
    let info = flink_explorer::operators::KeyedInfo {
        key_type: ks.key_type.clone(),
        state_names: ks.states.iter().map(|s| s.name.clone()).collect(),
        keys: ks.keys.len().to_string(),
        errors: ks.errors.clone(),
        malformed: ks.malformed.clone(),
    };
    assert!(summary
        .info_lines(Some(&info))
        .iter()
        .any(|(k, v)| k == "Malformed key-groups" && v.starts_with("1 ")));

    // CLI: the rows still go out, with a warning on stderr.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_flink-explorer"))
        .arg(&*dir)
        .args(["--export-state", "counter", "-o", "-"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).lines().count(),
        got.len()
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Warning: 1 malformed key-group(s)") && stderr.contains("truncated"),
        "{}",
        stderr
    );
}

/// The explorer's value for each key of `state`, derived from the items alone (a MAP state as one
/// object per key, as the reader path shows it).
fn expected_by_key(op: &KeyedOperator, state: usize) -> BTreeMap<String, Value> {
    let mut out: BTreeMap<String, Value> = BTreeMap::new();
    for item in op.items.iter().filter(|i| i.state() == state) {
        let key = item.key().render();
        match item {
            Item::Value { value, .. } => {
                out.insert(key, value.json());
            }
            Item::MapEntry { map_key, value, .. } => {
                let entry = out.entry(key).or_insert_with(|| json!({}));
                entry[map_key.render()] = value.as_ref().map_or(Value::Null, Val::json);
            }
            Item::List { values, .. } => {
                out.insert(key, values.iter().map(Val::json).collect());
            }
            Item::Timer { .. } => {}
        }
    }
    out
}

/// `char`, `char[]` and strings outside the BMP, through both read paths (#25): the value types
/// come from the serializer snapshots in the header, and keys holding an emoji keep it.
#[test]
fn char_and_non_bmp_string_states_decode_on_both_paths() {
    let keys = ["plain", "café", "emoji-😀", "flag-🇫🇷", "math-𝔸"];
    let names = ["initial", "letters", "label", "grade", "typed"];
    let mut op = KeyedOperator::new(0x62, 2, 128, Ser::String)
        .state(names[0], Kind::Value(Ser::Char))
        .state(names[1], Kind::Value(Ser::CharArray))
        .state(names[2], Kind::Value(Ser::String))
        .state(names[3], Kind::Map(Ser::String, Ser::Char))
        .state(names[4], Kind::List(Ser::Char));
    // 'A', 'B', 'é', a lone high surrogate (a `char` may hold half of a pair), 'Z'.
    let initials = [0x41u16, 0x42, 0xE9, 0xD83D, 0x5A];
    for (key, initial) in keys.iter().zip(initials) {
        let units: Vec<u16> = key.encode_utf16().collect();
        let key = Val::str(key);
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Char(initial),
        });
        op.items.push(Item::Value {
            state: 1,
            key: key.clone(),
            value: Val::Chars(units.clone()),
        });
        op.items.push(Item::Value {
            state: 2,
            key: key.clone(),
            value: Val::Str(format!("{} ✓ 😀", key.render())),
        });
        op.items.push(Item::MapEntry {
            state: 3,
            key: key.clone(),
            map_key: Val::str("first"),
            value: Some(Val::Char(units[0])),
        });
        op.items.push(Item::List {
            state: 4,
            key,
            values: units.iter().map(|&u| Val::Char(u)).collect(),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = write(&spec, "chars");

    // Streaming export.
    for (i, name) in names.iter().enumerate() {
        assert_eq!(
            projected(&rows(&dir, name)),
            expected_rows(&op, i),
            "state {}",
            name
        );
    }
    // The same against literals, not the generator's own rendering.
    let value_of = |state: &str, key: &str| {
        rows(&dir, state)
            .into_iter()
            .find(|r| r["key"] == key)
            .unwrap_or_else(|| panic!("no row for key {:?} in {}", key, state))["value"]
            .clone()
    };
    assert_eq!(value_of("label", "emoji-😀"), json!("emoji-😀 ✓ 😀"));
    assert_eq!(value_of("label", "flag-🇫🇷"), json!("flag-🇫🇷 ✓ 😀"));
    assert_eq!(value_of("initial", "emoji-😀"), json!("é"));
    // A lone surrogate (half of a pair, which a Java `char` may hold) as its Java escape.
    assert_eq!(value_of("initial", "flag-🇫🇷"), json!("\\uD83D"));
    assert_eq!(value_of("letters", "café"), json!(["c", "a", "f", "é"]));
    assert_eq!(
        value_of("typed", "math-𝔸"),
        json!(["m", "a", "t", "h", "-", "\\uD835", "\\uDD38"])
    );

    // Reader (TUI) path: the value types read from the header, every key intact, same values.
    let ks = common::keyed::keyed(&dir, &op.id_hex()).expect("keyed state read");
    let types: Vec<&str> = ks.states.iter().map(|d| d.value_type.as_str()).collect();
    assert_eq!(
        types,
        ["Char", "char[]", "String", "Map<String,Char>", "List<Char>"]
    );
    let read_keys: BTreeSet<&str> = ks.keys.iter().map(|p| p.key.as_str()).collect();
    assert_eq!(read_keys, keys.into_iter().collect());
    for (i, desc) in ks.states.iter().enumerate() {
        let got: BTreeMap<String, Value> = ks
            .keys
            .iter()
            .map(|p| {
                let v = ks
                    .value(&p.key, &desc.name)
                    .expect("every key has every state");
                (p.key.clone(), v)
            })
            .collect();
        assert_eq!(got, expected_by_key(&op, i), "state {}", desc.name);
    }
}
