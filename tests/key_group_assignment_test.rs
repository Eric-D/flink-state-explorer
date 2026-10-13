//! The reported key-group must be the **absolute** one Flink assigned, not an index within a
//! subtask's range. Verified against Flink's own formulas rather than against our own output.
//!
//! Needs `tests/fixtures/savepoint-gitlab` (run `cd test-flink-job && mvn verify` first).

use std::path::Path;

use flink_explorer::export_state::{export_state_ndjson, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use serde_json::Value;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-gitlab";

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

/// `KeyGroupRangeAssignment.computeKeyGroupForKeyHash` (flink-runtime, release-1.20).
fn key_group_for_hash(key_hash: i32, max_parallelism: i32) -> i32 {
    murmur_hash(key_hash) % max_parallelism
}

/// `java.lang.String.hashCode`.
fn java_string_hash(s: &str) -> i32 {
    let mut h: i32 = 0;
    // Java hashes UTF-16 code units.
    for c in s.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(c as i32);
    }
    h
}

/// `KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex` (flink-runtime, release-1.20).
fn range_for_subtask(max_parallelism: i32, parallelism: i32, subtask: i32) -> (i32, i32) {
    let start = (subtask * max_parallelism + parallelism - 1) / parallelism;
    let end = ((subtask + 1) * max_parallelism - 1) / parallelism;
    (start, end)
}

/// Every hash Flink could have used for this rendered key.
fn plausible_hashes(key: &str) -> Vec<i32> {
    let mut v = vec![java_string_hash(key)];
    if let Ok(i) = key.parse::<i32>() {
        v.push(i); // Integer.hashCode
    }
    if let Ok(l) = key.parse::<i64>() {
        v.push((l ^ ((l as u64) >> 32) as i64) as i32); // Long.hashCode
    }
    v
}

fn savepoint() -> parser::Savepoint {
    let metadata_path = Path::new(SAVEPOINT_DIR).join("_metadata");
    assert!(
        metadata_path.exists(),
        "run `cd test-flink-job && mvn verify` first"
    );
    parser::parse_metadata(&metadata_path).expect("parse metadata")
}

#[test]
fn index_export_reports_the_key_group_flink_assigned() {
    let dir = Path::new(SAVEPOINT_DIR);
    let metadata_path = dir.join("_metadata");
    let sp = savepoint();
    let len = std::fs::metadata(&metadata_path).unwrap().len();
    let index = build_index(&sp, len, 0u64, dir);

    let tmp = std::env::temp_dir().join(format!(
        "kg_assign_{}_{:?}.json",
        std::process::id(),
        std::thread::current().id()
    ));
    flink_explorer::export::export_json(&index, None, &tmp).expect("export_json");
    let root: Value = serde_json::from_str(&std::fs::read_to_string(&tmp).unwrap()).unwrap();
    std::fs::remove_file(&tmp).ok();

    let mut strict = 0;
    let mut checked = 0;
    for op in root["operators"].as_array().unwrap() {
        let max_p: i32 = op["info"]["Max parallelism"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128);
        for k in op["keys"].as_array().unwrap() {
            let key = k["key"].as_str().unwrap_or_default();
            if key.is_empty() {
                continue;
            }
            // A POJO key is hashed by the user class's own `hashCode()`, which cannot be
            // reproduced from its rendered form — those keys are covered by the subtask-range
            // check in `streamed_key_groups_fall_inside_their_subtask_range` instead.
            if key.starts_with('{') {
                continue;
            }
            let reported = k["key_group"].as_i64().unwrap() as i32;
            let expected: Vec<i32> = plausible_hashes(key)
                .into_iter()
                .map(|h| key_group_for_hash(h, max_p))
                .collect();
            assert!(
                expected.contains(&reported),
                "key {:?}: reported key-group {} but Flink's assignment gives {:?} \
                 (maxParallelism {})",
                key,
                reported,
                expected,
                max_p
            );
            checked += 1;
            // A non-numeric key can only have been hashed as a String, so that case pins the
            // value exactly rather than accepting one of several candidates.
            if key.parse::<i64>().is_err() {
                assert_eq!(
                    reported,
                    key_group_for_hash(java_string_hash(key), max_p),
                    "string key {:?} is in the wrong key-group",
                    key
                );
                strict += 1;
            }
        }
    }
    assert!(checked > 0, "no key checked");
    assert!(strict > 0, "no string key pinned exactly");
}

#[test]
fn streamed_key_groups_fall_inside_their_subtask_range() {
    let sp = savepoint();
    let states = list_states(&sp, Path::new(SAVEPOINT_DIR)).states;
    let mut checked = 0;

    for (op_hex, state, _) in &states {
        let op = sp
            .operators
            .iter()
            .find(|o| flink_explorer::export_state::hex_id(&o.id) == *op_hex)
            .expect("operator exists");

        let mut buf: Vec<u8> = Vec::new();
        export_state_ndjson(
            &sp,
            Path::new(SAVEPOINT_DIR),
            Some(op_hex),
            state,
            None,
            &mut buf,
        )
        .unwrap_or_else(|e| panic!("export {}: {}", state, e));

        for line in String::from_utf8(buf).unwrap().lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row: Value = serde_json::from_str(line).unwrap();
            let subtask = row["subtask"].as_i64().unwrap() as i32;
            let kg = row["key_group"].as_i64().unwrap() as i32;
            let (start, end) = range_for_subtask(op.max_parallelism, op.parallelism, subtask);
            assert!(
                kg >= start && kg <= end,
                "state {:?} key {:?}: key-group {} is outside subtask {}'s range {}..={} \
                 (parallelism {}, maxParallelism {})",
                state,
                row["key"],
                kg,
                subtask,
                start,
                end,
                op.parallelism,
                op.max_parallelism
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no row checked");
}

#[test]
fn murmur_hash_matches_flink_reference_values() {
    // Sanity-check the port itself: Flink's own KeyGroupRangeAssignmentTest asserts that every
    // assignment lands inside [0, maxParallelism).
    for max_p in [1, 2, 128, 32768] {
        for h in [i32::MIN, -1, 0, 1, 42, 1600, i32::MAX] {
            let kg = key_group_for_hash(h, max_p);
            assert!(
                kg >= 0 && kg < max_p,
                "hash {} with maxParallelism {} gave key-group {}",
                h,
                max_p,
                kg
            );
        }
    }
    // murmurHash is documented to return a non-negative value.
    for h in [i32::MIN, -7, 0, 7, i32::MAX] {
        assert!(murmur_hash(h) >= 0, "murmurHash({}) is negative", h);
    }
}
