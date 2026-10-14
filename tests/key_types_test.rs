//! Keys of every serializer type stay distinct (issue #26), on synthetic savepoints
//! (`tests/common/synth.rs`) — fixture-free, so these run in CI.
//!
//! Only String, Long, Int and POJO keys used to be decoded. A Short, Double, Boolean, BigInteger
//! or Tuple key fell back to a printable-ASCII preview or `[N bytes]` that also swallowed the
//! namespace, so distinct keys merged into one in the index (which groups entries by the rendered
//! key) and a MapState's map key leaked into the key; `BigIntSerializer` was even read as `Int`.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use flink_explorer::export_state::export_state_ndjson;
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use flink_explorer::parser::value::decode_state_value;
use serde_json::{json, Value};

const VALUE: usize = 0;
const MAP: usize = 1;
const TIMER: usize = 2;
const MAP_KEYS: [&str; 2] = ["clicks", "views"];

/// An operator keyed by `key_ser`, with a `ValueState<Long>`, a `MapState<String, Long>` and a
/// timer queue, holding one value, two map entries and one timer per key.
fn operator(key_ser: Ser, keys: &[Val], max_parallelism: i32) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x26, 2, max_parallelism, key_ser)
        .state("visits", Kind::Value(Ser::Long))
        .state("stats", Kind::Map(Ser::String, Ser::Long))
        .state("_timer_state/processing_user-timers", Kind::Timer);
    for (i, key) in keys.iter().enumerate() {
        let i = i as i64;
        op.items.push(Item::Value {
            state: VALUE,
            key: key.clone(),
            value: Val::Long(1000 + i),
        });
        for (j, mk) in MAP_KEYS.iter().enumerate() {
            op.items.push(Item::MapEntry {
                state: MAP,
                key: key.clone(),
                map_key: Val::str(mk),
                value: Some(Val::Long(i * 10 + j as i64)),
            });
        }
        op.items.push(Item::Timer {
            state: TIMER,
            key: key.clone(),
            timestamp: 1_700_000_000_000 + i,
        });
    }
    op
}

fn write(op: &KeyedOperator, tag: &str) -> synth::TempDir {
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op.clone());
    let dir = synth::temp_dir(tag);
    synth::write_savepoint(&spec, &dir);
    dir
}

fn export(dir: &Path, state: &str) -> Vec<Value> {
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).expect("_metadata parses");
    let mut out = Vec::new();
    export_state_ndjson(&savepoint, dir, None, state, None, &mut out).expect("export");
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Flink's key-group for `key` (`KeyGroupRangeAssignment` over its Java `hashCode()`).
fn key_group(key: &Val, max_parallelism: i32) -> i64 {
    synth::key_group_for_hash(key.java_hash(), max_parallelism) as i64
}

/// Every distinct key stays distinct, with its own value, map entries and key-group, on the
/// streaming export and on the index alike; MAP keys go to `map_key`, never into the key.
fn assert_keys_stay_distinct(key_ser: Ser, keys: &[Val], expected_key_type: &str) {
    let rendered: BTreeSet<String> = keys.iter().map(Val::render).collect();
    assert_eq!(
        rendered.len(),
        keys.len(),
        "test keys must be distinct: {:?}",
        keys
    );

    for max_parallelism in [128, 4096] {
        let op = operator(key_ser, keys, max_parallelism);
        let dir = write(&op, "keytypes");
        let ctx = format!("{:?} keys, maxParallelism {}", key_ser, max_parallelism);

        // Streaming export, VALUE state: one row per key, with its value and Flink's key-group.
        let rows = export(&dir, "visits");
        let got: BTreeMap<String, (i64, Value)> = rows
            .iter()
            .map(|r| {
                (
                    r["key"].as_str().unwrap().to_string(),
                    (r["key_group"].as_i64().unwrap(), r["value"].clone()),
                )
            })
            .collect();
        assert_eq!(rows.len(), keys.len(), "{}: one VALUE row per key", ctx);
        let want: BTreeMap<String, (i64, Value)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.render(),
                    (key_group(k, max_parallelism), json!(1000 + i as i64)),
                )
            })
            .collect();
        assert_eq!(got, want, "{}: VALUE rows", ctx);
        assert!(
            rows.iter().all(|r| r.get("namespace_hex").is_none()),
            "{}: a VoidNamespace is not surfaced",
            ctx
        );

        // Streaming export, MAP state: exploded, the map key in `map_key`, never in the key.
        let rows = export(&dir, "stats");
        let got: BTreeSet<(String, String, i64, String)> = rows
            .iter()
            .map(|r| {
                (
                    r["key"].as_str().unwrap().to_string(),
                    r["map_key"].as_str().expect("map_key").to_string(),
                    r["key_group"].as_i64().unwrap(),
                    r["value"].to_string(),
                )
            })
            .collect();
        let want: BTreeSet<(String, String, i64, String)> = keys
            .iter()
            .enumerate()
            .flat_map(|(i, k)| {
                MAP_KEYS.iter().enumerate().map(move |(j, mk)| {
                    (
                        k.render(),
                        mk.to_string(),
                        key_group(k, max_parallelism),
                        json!(i as i64 * 10 + j as i64).to_string(),
                    )
                })
            })
            .collect();
        assert_eq!(rows.len(), keys.len() * MAP_KEYS.len(), "{}: MAP rows", ctx);
        assert_eq!(got, want, "{}: MAP rows", ctx);

        // Index path (the TUI's): one partition per key — timers included, which must render
        // their key exactly like the entries do or they would open partitions of their own.
        let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let index = build_index(&savepoint, 0, 0, &dir);
        let ks = index.operators[0]
            .keyed_state
            .as_ref()
            .expect("keyed state");
        assert_eq!(ks.key_type, expected_key_type, "{}: key type", ctx);
        assert_eq!(ks.total_keys, keys.len(), "{}: no key merged or split", ctx);
        let by_key: BTreeMap<&str, &flink_explorer::index::cache::KeyPartition> =
            ks.partitions.iter().map(|p| (p.key.as_str(), p)).collect();
        for (i, k) in keys.iter().enumerate() {
            let p = by_key
                .get(k.render().as_str())
                .unwrap_or_else(|| panic!("{}: no partition for {:?}", ctx, k.render()));
            assert_eq!(p.key_group as i64, key_group(k, max_parallelism), "{}", ctx);
            let value = decode_state_value(p.values[VALUE].as_deref().unwrap(), "Long", None, None);
            assert_eq!(value, json!(1000 + i as i64), "{}: value of {:?}", ctx, k);
            let map = decode_state_value(
                p.values[MAP].as_deref().unwrap(),
                "Map<String,Long>",
                None,
                None,
            );
            assert_eq!(
                map,
                json!({ "clicks": i as i64 * 10, "views": i as i64 * 10 + 1 }),
                "{}: map of {:?}",
                ctx,
                k
            );
            let ts = decode_state_value(p.values[TIMER].as_deref().unwrap(), "Long", None, None);
            assert_eq!(ts, json!(1_700_000_000_000 + i as i64), "{}: timer", ctx);
        }
    }
}

#[test]
fn short_keys_stay_distinct() {
    // Short keys 1 and 2 used to both render `[3 bytes]`.
    let keys: Vec<Val> = [1i16, 2, -1, 0, 300, i16::MIN, i16::MAX]
        .into_iter()
        .map(Val::Short)
        .collect();
    assert_keys_stay_distinct(Ser::Short, &keys, "Short");
}

#[test]
fn double_keys_stay_distinct() {
    // 0.0 and -0.0 are distinct keys: Double.equals compares doubleToLongBits. NaN and
    // ±Infinity have no JSON form: they render as the hex of their bytes, still distinct.
    let keys: Vec<Val> = [
        1.5f64,
        -2.25,
        0.0,
        -0.0,
        3.0,
        1e300,
        f64::MIN_POSITIVE,
        0.1,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]
    .into_iter()
    .map(Val::Double)
    .collect();
    assert_keys_stay_distinct(Ser::Double, &keys, "Double");
}

#[test]
fn boolean_keys_stay_distinct() {
    assert_keys_stay_distinct(
        Ser::Boolean,
        &[Val::Bool(true), Val::Bool(false)],
        "Boolean",
    );
}

#[test]
fn big_integer_keys_stay_distinct() {
    // BigIntSerializer used to be read as Int: the key 300 showed as `2`. 0, 1 and 10 take the
    // ZERO/ONE/TEN fast paths; the rest the length-prefixed two's complement form.
    let keys: Vec<Val> = [
        0i128,
        1,
        10,
        300,
        -1,
        -300,
        255,
        256,
        -129,
        1 << 70,
        -(1 << 70),
        i128::MAX,
        i128::MIN,
    ]
    .into_iter()
    .map(Val::BigInt)
    .collect();
    assert_keys_stay_distinct(Ser::BigInteger, &keys, "BigInteger");
}

#[test]
fn tuple2_keys_stay_distinct() {
    // ("user-1", 1) and ("user-1", 2) used to both render `user-1`. ("a,b", "c") and
    // ("a", "b,c") would merge under a naive comma join.
    let t = |s: &str, n: i64| Val::Tuple(vec![Val::str(s), Val::Long(n)]);
    let keys = vec![
        t("user-1", 1),
        t("user-1", 2),
        t("user-2", 1),
        t("a,b", 3),
        t("a", 3),
        t("quote\"d", -4),
        t("", 0),
    ];
    assert_keys_stay_distinct(
        Ser::Tuple(&[Ser::String, Ser::Long]),
        &keys,
        "Tuple<String,Long>",
    );
    let strings = |a: &str, b: &str| Val::Tuple(vec![Val::str(a), Val::str(b)]);
    assert_keys_stay_distinct(
        Ser::Tuple(&[Ser::String, Ser::String]),
        &[strings("a,b", "c"), strings("a", "b,c")],
        "Tuple<String,String>",
    );
}

#[test]
fn byte_float_and_char_keys_stay_distinct() {
    let bytes: Vec<Val> = [0i8, 1, -1, i8::MIN, i8::MAX]
        .into_iter()
        .map(Val::Byte)
        .collect();
    assert_keys_stay_distinct(Ser::Byte, &bytes, "Byte");
    let floats: Vec<Val> = [1.5f32, -0.0, 0.0, 0.1, f32::MAX, f32::NAN, f32::INFINITY]
        .into_iter()
        .map(Val::Float)
        .collect();
    assert_keys_stay_distinct(Ser::Float, &floats, "Float");
    let chars: Vec<Val> = ['a', 'b', ',', 'é', '€', '0']
        .into_iter()
        .map(|c| Val::Char(c as u16))
        .collect();
    assert_keys_stay_distinct(Ser::Char, &chars, "Char");
}

#[test]
fn undecodable_keys_render_opaquely_and_keep_their_namespace() {
    // A user serializer the explorer cannot read: each key renders as the hex of its own bytes —
    // the key-group prefix, the namespace and a MAP state's map key stay out of it. Keys that are
    // prefixes of one another, or end in a zero byte like the VoidNamespace, must not merge.
    let opaque = |bytes: &[u8], hash: i32| Val::Opaque {
        bytes: bytes.to_vec(),
        hash,
    };
    let keys = vec![
        opaque(&[0x01, 0x02], 11),
        opaque(&[0x01, 0x02, 0x00], 12),
        opaque(&[0x01, 0x02, 0x00, 0x00], 13),
        opaque(&[0x00], 14),
        opaque(&[0x07, b'c', b'l', b'i', b'c', b'k', b's'], 15),
        opaque(&[0xff; 40], 16),
    ];
    assert_keys_stay_distinct(
        Ser::Opaque("com.example.OpaqueKeySerializer$OpaqueKeySerializerSnapshot"),
        &keys,
        "OpaqueKeySerializer.OpaqueKeySerializer",
    );
}

#[test]
fn big_integer_and_char_values_decode() {
    // The same serializers as values, through the single decoder.
    let op = KeyedOperator::new(0x27, 1, 128, Ser::String)
        .state("big-total", Kind::Value(Ser::BigInteger))
        .state("first-char", Kind::Value(Ser::Char))
        .state("small-total", Kind::Value(Ser::Short));
    let mut op = op;
    for (i, v) in [0i128, 10, -300, 1 << 100].into_iter().enumerate() {
        let key = Val::Str(format!("k{}", i));
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::BigInt(v),
        });
        op.items.push(Item::Value {
            state: 1,
            key: key.clone(),
            value: Val::Char(['x', 'é', '€', ','][i] as u16),
        });
        op.items.push(Item::Value {
            state: 2,
            key,
            value: Val::Short(-(i as i16)),
        });
    }
    let dir = write(&op, "bigvalues");
    let values = |state: &str| -> BTreeMap<String, Value> {
        export(&dir, state)
            .into_iter()
            .map(|r| (r["key"].as_str().unwrap().to_string(), r["value"].clone()))
            .collect()
    };
    assert_eq!(
        values("big-total"),
        BTreeMap::from([
            ("k0".to_string(), json!("0")),
            ("k1".to_string(), json!("10")),
            ("k2".to_string(), json!("-300")),
            ("k3".to_string(), json!("1267650600228229401496703205376")),
        ])
    );
    assert_eq!(
        values("first-char"),
        BTreeMap::from([
            ("k0".to_string(), json!("x")),
            ("k1".to_string(), json!("é")),
            ("k2".to_string(), json!("€")),
            ("k3".to_string(), json!(",")),
        ])
    );
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let index = build_index(&savepoint, 0, 0, &dir);
    let types: Vec<&str> = index.operators[0]
        .keyed_state
        .as_ref()
        .unwrap()
        .descriptors
        .iter()
        .map(|d| d.value_type.as_str())
        .collect();
    // `BigIntSerializer` is matched exactly, not as a substring of `IntSerializer`.
    assert_eq!(types, ["BigInteger", "Char", "Short"]);
}

#[test]
fn big_integer_and_big_decimal_layouts() {
    // BigIntSerializer.writeBigInteger: int(0) = null, int(1|2|3) = ZERO/ONE/TEN, else
    // int(len + 4) + toByteArray(). BigDecSerializer: that BigInteger (unscaled) + int(scale).
    let big_int = |len: i32, bytes: &[u8]| {
        let mut b = len.to_be_bytes().to_vec();
        b.extend_from_slice(bytes);
        b
    };
    let cases: [(Vec<u8>, Value); 7] = [
        (big_int(0, &[]), Value::Null),
        (big_int(1, &[]), json!("0")),
        (big_int(2, &[]), json!("1")),
        (big_int(3, &[]), json!("10")),
        (big_int(5, &[0x00]), json!("0")), // a zero that is not BigInteger.ZERO
        (big_int(6, &[0x01, 0x2c]), json!("300")),
        (big_int(5, &[0x80]), json!("-128")),
    ];
    for (bytes, want) in cases {
        assert_eq!(
            decode_state_value(&bytes, "BigInteger", None, None),
            want,
            "{:02x?}",
            bytes
        );
    }

    // Java's BigDecimal.toString(), which is one-to-one with (unscaled, scale).
    let big_dec = |unscaled: i64, scale: i32| {
        let bytes = unscaled.to_be_bytes();
        let skip = (0..7)
            .take_while(|&i| {
                (bytes[i] == 0 && bytes[i + 1] & 0x80 == 0)
                    || (bytes[i] == 0xff && bytes[i + 1] & 0x80 != 0)
            })
            .count();
        let mut b = big_int(8 - skip as i32 + 4, &bytes[skip..]);
        b.extend_from_slice(&scale.to_be_bytes());
        b
    };
    for (unscaled, scale, want) in [
        (1i64, 0, "1"),
        (123, 2, "1.23"),
        (100, 2, "1.00"),
        (10, 1, "1.0"),
        (1, 2, "0.01"),
        (-5, 1, "-0.5"),
        (12, 7, "0.0000012"),
        (1, 7, "1E-7"),
        (1, -3, "1E+3"),
        (123, -2, "1.23E+4"),
        (0, 2, "0.00"),
        (0, -2, "0E+2"),
        (-123456789, 4, "-12345.6789"),
    ] {
        assert_eq!(
            decode_state_value(&big_dec(unscaled, scale), "BigDecimal", None, None),
            json!(want),
            "unscaled {} scale {}",
            unscaled,
            scale
        );
    }
    // A null BigDecimal is a null BigInteger and nothing else.
    assert_eq!(
        decode_state_value(&big_int(0, &[]), "BigDecimal", None, None),
        Value::Null
    );
}

#[test]
fn only_lossless_renderings_are_used_as_keys() {
    use flink_explorer::parser::value::{decode_key, opaque_key};
    let millis = 1_700_000_000_123i64.to_be_bytes();
    // DateSerializer: writeLong(millis), rendered to the millisecond, i.e. exactly.
    assert_eq!(
        decode_key(&millis, "Date", None),
        Some(("2023-11-14T22:13:20.123Z".to_string(), 8))
    );
    // LocalDateSerializer: int year + byte month + byte day.
    let mut local_date = 2024i32.to_be_bytes().to_vec();
    local_date.extend_from_slice(&[2, 29]);
    assert_eq!(
        decode_key(&local_date, "LocalDate", None),
        Some(("2024-02-29".to_string(), 6))
    );
    // Timestamp, Instant, LocalTime and LocalDateTime print milliseconds only, and byte[] may
    // print `[N bytes]`: rendering them would merge keys, so they are left to `opaque_key`.
    let mut timestamp = millis.to_vec();
    timestamp.extend_from_slice(&456_789i32.to_be_bytes());
    for t in [
        "Timestamp",
        "Instant",
        "LocalDateTime",
        "LocalTime",
        "byte[]",
        "Kryo",
        "Tuple<Long,Timestamp>",
        "Pojo", // without its layout
    ] {
        assert_eq!(decode_key(&timestamp, t, None), None, "{}", t);
    }
    assert_eq!(opaque_key(&[0x00, 0xab]), "0x00ab");
    // A Double NaN has no JSON form: rendered opaquely over exactly its 8 bytes.
    let mut nan = 0x7ff8_0000_0000_0000u64.to_be_bytes().to_vec();
    nan.push(0); // the VoidNamespace after the key
    assert_eq!(
        decode_key(&nan, "Double", None),
        Some(("0x7ff8000000000000".to_string(), 8))
    );
}

#[test]
fn java_hash_codes_match_the_jdk() {
    // Known answers from the JDK, so the key-groups above are the ones Flink assigns.
    assert_eq!(Val::Short(300).java_hash(), 300); // Short.hashCode = (int) value
    assert_eq!(Val::Byte(-1).java_hash(), -1);
    assert_eq!(Val::Char('a' as u16).java_hash(), 97);
    assert_eq!(Val::Bool(true).java_hash(), 1231);
    assert_eq!(Val::Bool(false).java_hash(), 1237);
    // Double.valueOf(1.5).hashCode(): bits 0x3FF8000000000000 → 0x3FF80000.
    assert_eq!(Val::Double(1.5).java_hash(), 0x3ff8_0000);
    assert_eq!(Val::Float(1.5).java_hash(), 0x3fc0_0000); // Float.floatToIntBits(1.5f)
                                                          // BigInteger: mag ints folded by 31, times the signum.
    assert_eq!(Val::BigInt(300).java_hash(), 300);
    assert_eq!(Val::BigInt(-300).java_hash(), -300);
    assert_eq!(Val::BigInt(1 << 70).java_hash(), 64 * 31 * 31); // mag = [64, 0, 0]
                                                                // Tuple2.of("ab", 1L).hashCode() = 31 * "ab".hashCode() + Long.hashCode(1).
    assert_eq!(
        Val::Tuple(vec![Val::str("ab"), Val::Long(1)]).java_hash(),
        31 * (31 * 97 + 98) + 1
    );
}

#[test]
fn one_letter_state_names_resolve_their_own_serializers() {
    // A state's serializers used to be looked up after the first occurrence of its name's bytes:
    // "m" occurs in `…common.typeutils…` and "v" in `…api.java.tuple…`, so a MAP state's map key
    // type was read off the wrong snapshot — and an undecodable key then swallowed its map key.
    let opaque = |bytes: &[u8], hash: i32| Val::Opaque {
        bytes: bytes.to_vec(),
        hash,
    };
    let keys = [opaque(&[0x0a, 0x00], 1), opaque(&[0x0a], 2)];
    let mut op = KeyedOperator::new(
        0x28,
        1,
        128,
        Ser::Opaque("com.example.OpaqueKeySerializer$OpaqueKeySerializerSnapshot"),
    )
    .state("v", Kind::Value(Ser::Tuple(&[Ser::String, Ser::Long])))
    .state("m", Kind::Map(Ser::String, Ser::Long));
    for (i, key) in keys.iter().enumerate() {
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Tuple(vec![Val::str("t"), Val::Long(i as i64)]),
        });
        op.items.push(Item::MapEntry {
            state: 1,
            key: key.clone(),
            map_key: Val::str("clicks"),
            value: Some(Val::Long(i as i64)),
        });
    }
    let dir = write(&op, "shortnames");
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let index = build_index(&savepoint, 0, 0, &dir);
    let ks = index.operators[0].keyed_state.as_ref().unwrap();
    let types: Vec<&str> = ks
        .descriptors
        .iter()
        .map(|d| d.value_type.as_str())
        .collect();
    assert_eq!(types, ["Tuple<String,Long>", "Map<String,Long>"]);
    assert_eq!(ks.total_keys, 2);

    let rows: BTreeSet<String> = export(&dir, "m")
        .iter()
        .map(|r| format!("{} {} {}", r["key"], r["map_key"], r["value"]))
        .collect();
    assert_eq!(
        rows,
        BTreeSet::from([
            r#""0x0a00" "clicks" 0"#.to_string(),
            r#""0x0a" "clicks" 1"#.to_string(),
        ])
    );
}
