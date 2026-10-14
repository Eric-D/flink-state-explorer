//! Parquet output: the file is well-formed, the `value` column is typed per state, and the rows
//! are exactly the ones NDJSON emits.
//!
//! Run with `cargo test --features parquet`. Needs `tests/fixtures/savepoint-gitlab`.
#![cfg(feature = "parquet")]

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use flink_explorer::export_parquet::{ParquetSink, ValueColumn};
use flink_explorer::export_state::{export_state_ndjson, export_state_rows};
use flink_explorer::parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

fn savepoint() -> parser::Savepoint {
    let metadata_path = Path::new(SAVEPOINT_DIR).join("_metadata");
    assert!(
        metadata_path.exists(),
        "run `cd test-flink-job && mvn verify` first"
    );
    parser::parse_metadata(&metadata_path).expect("parse metadata")
}

fn tmp(name: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    std::env::temp_dir().join(format!(
        "parquet_{}_{}_{}.parquet",
        name,
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Export one state to Parquet and return the file path plus the chosen value column.
fn export(state: &str) -> (std::path::PathBuf, ValueColumn) {
    let sp = savepoint();
    let path = tmp(state);
    let mut sink = ParquetSink::create(&path).expect("create sink");
    export_state_rows(&sp, Path::new(SAVEPOINT_DIR), None, state, None, &mut sink)
        .unwrap_or_else(|e| panic!("export {}: {}", state, e));
    let kind = sink.value_column();
    (path, kind)
}

/// The same state as NDJSON, as `key -> value` (map states keyed by `key|map_key`).
fn ndjson_rows(state: &str) -> BTreeMap<String, Value> {
    let sp = savepoint();
    let mut buf: Vec<u8> = Vec::new();
    export_state_ndjson(&sp, Path::new(SAVEPOINT_DIR), None, state, None, &mut buf)
        .expect("ndjson export");
    String::from_utf8(buf)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let row: Value = serde_json::from_str(l).unwrap();
            let key = match row.get("map_key").and_then(|v| v.as_str()) {
                Some(mk) => format!("{}|{}", row["key"].as_str().unwrap(), mk),
                None => row["key"].as_str().unwrap().to_string(),
            };
            (key, row["value"].clone())
        })
        .collect()
}

/// Read the file back as `key -> value`, with `value` rendered the way NDJSON would.
fn parquet_rows(path: &Path) -> BTreeMap<String, Value> {
    let file = std::fs::File::open(path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet is well-formed")
        .build()
        .expect("record batch reader");

    let mut out = BTreeMap::new();
    for batch in reader {
        let batch = batch.expect("batch decodes");
        let keys = batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let map_keys = batch
            .column_by_name("map_key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch.column_by_name("value").unwrap();

        for i in 0..batch.num_rows() {
            let key = if map_keys.is_null(i) {
                keys.value(i).to_string()
            } else {
                format!("{}|{}", keys.value(i), map_keys.value(i))
            };
            let v = if values.is_null(i) {
                Value::Null
            } else {
                match values.data_type() {
                    DataType::Int64 => Value::from(
                        values
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i),
                    ),
                    DataType::Boolean => Value::from(
                        values
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(i),
                    ),
                    DataType::Utf8 => {
                        let s = values
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(i);
                        // A JSON column holds the serialized value; a String column holds it raw.
                        serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
                    }
                    other => panic!("unexpected value column type {:?}", other),
                }
            };
            out.insert(key, v);
        }
    }
    out
}

#[test]
fn scalar_state_gets_a_typed_value_column() {
    let (path, kind) = export("int-key-count");
    assert_eq!(kind, ValueColumn::Int64, "an Int state should not be JSON");

    let file = std::fs::File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let field = builder.schema().field_with_name("value").unwrap().clone();
    assert_eq!(field.data_type(), &DataType::Int64);
    // Every column but `value`, `map_key` and `namespace_hex` is required.
    assert!(!builder
        .schema()
        .field_with_name("key")
        .unwrap()
        .is_nullable());

    assert_eq!(parquet_rows(&path), ndjson_rows("int-key-count"));
    std::fs::remove_file(&path).ok();
}

#[test]
fn structured_state_falls_back_to_json_text() {
    let (path, kind) = export("all-fields-pojo");
    assert_eq!(kind, ValueColumn::Json, "a POJO state has no scalar column");

    let file = std::fs::File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert_eq!(
        builder
            .schema()
            .field_with_name("value")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    // The rows must survive the JSON round-trip through the column unchanged.
    assert_eq!(parquet_rows(&path), ndjson_rows("all-fields-pojo"));
    std::fs::remove_file(&path).ok();
}

#[test]
fn map_state_keeps_the_exploded_map_key() {
    let (path, _) = export("all-fields-map");
    let file = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let map_keys = batch
            .column_by_name("map_key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(!map_keys.is_null(i), "a MAP row must carry its map_key");
            rows += 1;
        }
    }
    assert!(rows > 0);
    assert_eq!(parquet_rows(&path), ndjson_rows("all-fields-map"));
    std::fs::remove_file(&path).ok();
}

#[test]
fn key_group_is_stored_and_absolute() {
    let (path, _) = export("int-key-count");
    let file = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut seen = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let kgs = batch
            .column_by_name("key_group")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            seen.push(kgs.value(i));
        }
    }
    assert!(!seen.is_empty());
    // maxParallelism is 128 in the fixture, and subtask 1's range is 64..=127: an absolute
    // key-group reaches above 63, a per-subtask index never would.
    assert!(seen.iter().all(|&k| (0..128).contains(&k)));
    assert!(
        seen.iter().any(|&k| k >= 64),
        "no key-group above 63 — these look like per-subtask indices: {:?}",
        seen
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn file_is_zstd_compressed() {
    let (path, _) = export("all-fields-pojo");
    let file = std::fs::File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let meta = builder.metadata().clone();
    assert!(meta.num_row_groups() >= 1);
    for rg in meta.row_groups() {
        for col in rg.columns() {
            // Only the codec is recorded in the footer — the level is a writer-side setting and
            // arrow-rs reports it back as the default, so matching on the level would be testing
            // something Parquet does not store.
            assert!(
                matches!(col.compression(), parquet::basic::Compression::ZSTD(_)),
                "column {} is {:?}, not zstd",
                col.column_path(),
                col.compression()
            );
        }
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn char_state_is_a_utf8_column_of_one_character_strings() {
    // A synthetic savepoint (no fixture needed): ValueState<Character>.
    use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
    let mut op =
        KeyedOperator::new(0x63, 1, 128, Ser::String).state("initial", Kind::Value(Ser::Char));
    for (key, c) in [("a", 0x41u16), ("e", 0xE9), ("half", 0xD83D)] {
        op.items.push(Item::Value {
            state: 0,
            key: Val::str(key),
            value: Val::Char(c),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op);
    let dir = synth::temp_dir("parquet-char");
    synth::write_savepoint(&spec, &dir);

    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let path = tmp("char");
    let mut sink = ParquetSink::create(&path).expect("create sink");
    export_state_rows(&sp, &dir, None, "initial", None, &mut sink).expect("export");
    assert_eq!(sink.value_column(), ValueColumn::Utf8);
    let rows = parquet_rows(&path);
    assert_eq!(
        rows,
        BTreeMap::from([
            ("a".to_string(), Value::from("A")),
            ("e".to_string(), Value::from("é")),
            ("half".to_string(), Value::from("\\uD83D")),
        ])
    );
    std::fs::remove_file(&path).ok();
}
