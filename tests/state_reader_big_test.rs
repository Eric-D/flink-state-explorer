//! `reader::StateReader` on `tests/fixtures/savepoint-big` (1M String keys, ~211 MB in two state
//! files), when it exists: generate it with
//! `mvn -B -Dbig.savepoint=true -Dit.test=BigSavepointGeneratorIT verify`. Skipped otherwise.
//!
//! Every key is reached by paging, its key-group is the one `get` computes from its text, a
//! sample is looked up for real, and the process's peak RSS stays bounded by a few key-groups.
//! Alone in its test binary so that the RSS it reports is its own. For timings, run it with
//! `cargo test --release --test state_reader_big_test -- --nocapture`.

use std::path::Path;
use std::time::Instant;

use flink_explorer::parser::key_group::JavaKey;
use flink_explorer::reader::{Cursor, Lookup, StateReader};

const DIR: &str = "tests/fixtures/savepoint-big";
const KEYS: usize = 1_000_000;

/// `VmHWM` (peak RSS) in MiB, from `/proc/self/status`.
fn peak_rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<f64>().ok())
        })
        .map_or(0.0, |kb| kb / 1024.0)
}

#[test]
fn every_key_of_the_big_savepoint_is_reachable_in_bounded_memory() {
    let dir = Path::new(DIR);
    if !dir.join("_metadata").exists() {
        eprintln!("{} absent: skipped", DIR);
        return;
    }
    let rss_before = peak_rss_mib();
    let t0 = Instant::now();
    let r = StateReader::open(dir).unwrap();
    let op = r
        .operators()
        .into_iter()
        .find(|o| o.keyed_subtasks > 0)
        .expect("a keyed operator");
    let states = r.states(&op.id).unwrap();
    assert!(states.errors.is_empty(), "{:?}", states.errors);
    assert_eq!(states.key_type, "String");

    // Page through every key. A bitset, not a set of strings, so the check does not weigh on the
    // RSS being measured.
    let mut seen = vec![0u64; KEYS.div_ceil(64)];
    let (mut count, mut pages, mut bytes_read, mut max_block) = (0usize, 0usize, 0u64, 0usize);
    let mut cursor = Cursor::START;
    loop {
        let page = r.key_page(&op.id, &cursor, 10_000).unwrap();
        assert!(page.malformed.is_empty(), "{:?}", page.malformed);
        pages += 1;
        bytes_read += page.stats.bytes_read;
        max_block = max_block.max(page.stats.max_block_bytes);
        for k in &page.keys {
            let i: usize = k.key["key-".len()..].parse().unwrap();
            assert!(seen[i / 64] & (1 << (i % 64)) == 0, "{} came twice", k.key);
            seen[i / 64] |= 1 << (i % 64);
            count += 1;
            // `get` would read exactly this key-group for this text.
            let key = JavaKey::parse("String", &k.key).unwrap();
            assert_eq!(key.key_group(op.max_parallelism), k.key_group as i32);
            assert_eq!(
                key.canonical_text("String").as_deref(),
                Some(k.key.as_str())
            );
            let counter = k.entries.iter().find(|e| e.state == "big-counter").unwrap();
            assert_eq!(counter.value, serde_json::json!(i as i64 * 31 + 7));
            let stats = k.entries.iter().filter(|e| e.state == "big-stats").count();
            assert_eq!(stats, if i.is_multiple_of(2) { 3 } else { 2 }, "{}", k.key);
        }
        match page.next {
            Some(n) => cursor = n,
            None => break,
        }
    }
    let paged = t0.elapsed();
    assert_eq!(count, KEYS);

    // Real lookups over a sample, cold (no cache) so each reads its block.
    let r = StateReader::open(dir).unwrap().with_cache_bytes(0);
    let t1 = Instant::now();
    let (mut gets, mut get_bytes) = (0usize, 0u64);
    for i in (0..KEYS).step_by(4999) {
        let text = format!("key-{:07}", i);
        match r.get(&op.id, &text).unwrap() {
            Lookup::Found { record, stats } => {
                assert_eq!(record.key, text);
                assert_eq!(stats.blocks, 1);
                get_bytes += stats.bytes_read;
                gets += 1;
            }
            other => panic!("{}: {:?}", text, other),
        }
    }
    let got = t1.elapsed();
    let absent = r.get(&op.id, "key-9999999").unwrap();
    assert!(matches!(absent, Lookup::Absent { .. }), "{:?}", absent);

    let peak = peak_rss_mib();
    eprintln!(
        "savepoint-big: {} keys in {} pages, {:.2} s, {:.1} MB read, largest key-group {:.2} MB; \
         {} gets in {:.2} s ({:.2} MB read, {:.2} MB/get); peak RSS {:.1} MB (before: {:.1} MB)",
        count,
        pages,
        paged.as_secs_f64(),
        bytes_read as f64 / 1e6,
        max_block as f64 / 1e6,
        gets,
        got.as_secs_f64(),
        get_bytes as f64 / 1e6,
        get_bytes as f64 / gets as f64 / 1e6,
        peak,
        rss_before
    );
    // A page of 10 000 decoded keys, the 8 MiB block cache and a block being parsed: a few
    // key-groups' worth, far below the 211 MB of state.
    assert!(peak < 96.0, "peak RSS {:.1} MB", peak);
}
