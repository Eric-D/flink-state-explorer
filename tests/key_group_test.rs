//! `parser::key_group` against values computed by Java itself: each `hashCode()` and key-group
//! below was printed by Java 21 running `o.hashCode()` and Flink's `MathUtils.murmurHash(h) %
//! maxParallelism` (copied from flink-core release-1.20) on the boxed value. Fixture-free.

mod common;

use common::synth::{self, Val};
use flink_explorer::parser::key_group::{
    key_group_for_hash, murmur_hash, range_for_subtask, subtask_for_key_group, JavaKey,
};

/// `(key type, key text, Java hashCode, key-group at maxParallelism 128, at 32768)`.
const JAVA: &[(&str, &str, i32, i32, i32)] = &[
    ("String", "key-0000000", 860_643_522, 15, 21007),
    ("String", "hello", 99_162_322, 35, 31395),
    ("String", "\u{1F600}", 1_772_899, 54, 7350),
    ("Long", "1234567890123", 1_912_276_436, 92, 32092),
    ("Long", "-1", 0, 94, 31198),
    ("Int", "42", 42, 29, 28189),
    ("Int", "-7", -7, 33, 15777),
    ("Short", "-3", -3, 44, 2220),
    ("Byte", "-128", -128, 75, 4811),
    ("Char", "a", 97, 81, 19409),
    ("Boolean", "true", 1231, 77, 11725),
    ("Float", "0.1", 1_036_831_949, 125, 1277),
    ("Float", "NaN", 2_143_289_344, 83, 19795),
    ("Double", "-0.0", i32::MIN, 108, 10348),
    ("Double", "3.5", 1_074_528_256, 47, 19247),
    (
        "BigInteger",
        "-170141183460469231731687303715884105728",
        i32::MIN,
        108,
        10348,
    ),
    ("BigInteger", "4294967296", 31, 123, 2683),
    ("BigInteger", "-5", -5, 80, 12112),
    (
        "BigInteger",
        "123456789012345678901234567890",
        1_915_528_825,
        12,
        23564,
    ),
];

#[test]
fn hash_codes_and_key_groups_match_java() {
    for &(ty, text, hash, kg128, kg32768) in JAVA {
        let key = JavaKey::parse(ty, text).unwrap_or_else(|e| panic!("{} {}: {}", ty, text, e));
        assert_eq!(key.hash_code(), hash, "{} {:?}", ty, text);
        assert_eq!(key.key_group(128), kg128, "{} {:?}", ty, text);
        assert_eq!(key.key_group(32768), kg32768, "{} {:?}", ty, text);
    }
    // Tuple2.of("a,b", 7L): 31 * "a,b".hashCode() + Long.hashCode(7), rendered as JSON.
    let t = JavaKey::parse("Tuple<String,Long>", r#"["a,b",7]"#).unwrap();
    assert_eq!(t.hash_code(), 2_935_056);
    assert_eq!(t.key_group(128), 2);
}

#[test]
fn murmur_hash_matches_java() {
    assert_eq!(murmur_hash(0), 593_689_054);
    assert_eq!(murmur_hash(1), 68_075_478);
    assert_eq!(murmur_hash(-1), 1_982_413_648);
    assert_eq!(murmur_hash(i32::MIN), 1_718_298_732);
    // KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(128, 3, 100) = 100 * 3 / 128.
    assert_eq!(subtask_for_key_group(128, 3, 100), 2);
    assert_eq!(range_for_subtask(128, 3, 2), (86, 127));
}

/// The library agrees with the independent port the synthetic savepoints are written with, over
/// every key type that port knows.
#[test]
fn agrees_with_the_synthetic_writer() {
    let mut vals: Vec<(&str, Val)> = Vec::new();
    for i in -300i64..300 {
        vals.push(("Long", Val::Long(i * 7_919_993)));
        vals.push(("Int", Val::Int(i as i32 * 104_729)));
        vals.push(("Short", Val::Short(i as i16 * 97)));
        vals.push(("Byte", Val::Byte(i as i8)));
        vals.push(("Char", Val::Char((i * 211).rem_euclid(65536) as u16)));
        vals.push(("Float", Val::Float(i as f32 / 7.0)));
        vals.push(("Double", Val::Double(i as f64 * 1e-3 / 3.0)));
        vals.push((
            "BigInteger",
            Val::BigInt(i as i128 * 1_000_000_000_000_000_007),
        ));
        vals.push(("String", Val::Str(format!("k-{}-é-\u{1F600}", i))));
        vals.push((
            "Tuple<String,Long>",
            Val::Tuple(vec![Val::Str(format!("t{}", i)), Val::Long(i)]),
        ));
    }
    vals.push(("Boolean", Val::Bool(true)));
    vals.push(("Boolean", Val::Bool(false)));
    vals.push(("Double", Val::Double(f64::NAN)));
    vals.push(("Double", Val::Double(f64::INFINITY)));
    vals.push(("Float", Val::Float(f32::NEG_INFINITY)));
    vals.push(("Char", Val::Char(0xD800)));
    for (ty, v) in &vals {
        let text = v.render();
        let key = JavaKey::parse(ty, &text).unwrap_or_else(|e| panic!("{}: {}", text, e));
        assert_eq!(key.hash_code(), v.java_hash(), "{} {:?}", ty, text);
        assert_eq!(
            key_group_for_hash(key.hash_code(), 4096),
            synth::key_group_for_hash(v.java_hash(), 4096)
        );
        // Serialized like Flink does, and rendered back to the same text by the decoder.
        let mut bytes = Vec::new();
        key.serialize(&mut bytes);
        if !matches!(v, Val::BigInt(0 | 1 | 10)) {
            assert_eq!(bytes, v.encode(), "{} {:?}", ty, text);
        }
        assert_eq!(key.canonical_text(ty).as_deref(), Some(text.as_str()));
    }
}

#[test]
fn texts_that_are_not_keys_say_why() {
    for (ty, text) in [
        ("Long", "12x"),
        ("Int", "99999999999"),
        ("Boolean", "yes"),
        ("Char", "ab"),
        ("BigInteger", "1.5"),
        ("Tuple<String,Long>", r#"["a"]"#),
        ("Tuple<String,Long>", "a,1"),
    ] {
        assert!(JavaKey::parse(ty, text).is_err(), "{} {:?}", ty, text);
    }
    let e = JavaKey::parse("Pojo", "{}").unwrap_err();
    assert!(e.contains("POJO"), "{}", e);
    let e = JavaKey::parse("Kryo", "x").unwrap_err();
    assert!(e.contains("Kryo"), "{}", e);
}
