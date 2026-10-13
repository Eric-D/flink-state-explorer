//! Managed operator state (list, union-list and broadcast states) on synthetic savepoints (see
//! `tests/common/synth.rs`), fixture-free so it runs in CI: the stream behind an
//! `OperatorStreamStateHandle` decodes the same whether it is inline, in a file or a segment of
//! one, and compressed or not; an unreadable stream says so; element boundaries are found in
//! linear time.

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::synth::{self, KeyedOperator, OpState, Ser, Spec, Storage, Val};
use flink_explorer::index::builder::build_index;
use flink_explorer::index::cache::IndexCache;
use flink_explorer::parser;
use serde_json::Value;

/// `(state name, state type, entry labels)` of every operator state, in index order.
type OpStates = Vec<(String, String, Vec<String>)>;

fn index(dir: &Path) -> IndexCache {
    let savepoint =
        parser::parse_metadata(&dir.join("_metadata")).expect("synthetic _metadata parses");
    build_index(&savepoint, 0, 0, dir)
}

fn op_states(index: &IndexCache) -> OpStates {
    index
        .operators
        .iter()
        .flat_map(|op| &op.states)
        .map(|s| {
            let labels = s
                .entries
                .iter()
                .map(|e| String::from_utf8(e.key.clone()).unwrap())
                .collect();
            (s.name.clone(), s.state_type.clone(), labels)
        })
        .collect()
}

/// The same, as the full JSON export writes it (`operators[].operator_states`).
fn exported_op_states(index: &IndexCache, tag: &str) -> OpStates {
    let out = synth::temp_dir(tag).join("export.json");
    flink_explorer::export::export_json(index, None, &out).expect("export_json");
    let root: Value = serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    root["operators"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|op| op["operator_states"].as_array().unwrap())
        .map(|s| {
            let values = s["values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            (
                s["name"].as_str().unwrap().to_string(),
                s["mode"].as_str().unwrap().to_string(),
                values,
            )
        })
        .collect()
}

/// A non-keyed operator with a list, a union list and a broadcast state on two subtasks, an
/// empty list state on subtask 0 and, when `big`, one element over 64 KiB that compresses (it
/// spans several Snappy chunks, `0x00` ones).
fn operator(big: bool) -> KeyedOperator {
    let strs = |prefix: &str, n: usize| -> Vec<Val> {
        (0..n)
            .map(|i| Val::Str(format!("{}-{}", prefix, i)))
            .collect()
    };
    let mut buffer0 = strs("user", 5);
    if big {
        buffer0.push(Val::Str("abcdefgh".repeat(20_000)));
    }
    let config = |sub: i32| {
        vec![
            (Val::str("config-b"), Val::Str(format!("B{}", sub))),
            (Val::str("config-a"), Val::Str(format!("A{}", sub))),
        ]
    };
    KeyedOperator::new(0x0e, 2, 128, Ser::String)
        .op_state(0, OpState::list("operator-buffer", Ser::String, buffer0))
        .op_state(
            0,
            OpState::union_list("union-buffer", Ser::String, strs("u0", 3)),
        )
        .op_state(0, OpState::list("empty-buffer", Ser::String, Vec::new()))
        .op_state(
            0,
            OpState::broadcast("broadcast-config", Ser::String, Ser::String, config(0)),
        )
        .op_state(
            1,
            OpState::list("operator-buffer", Ser::String, strs("org", 4)),
        )
        .op_state(
            1,
            OpState::union_list("union-buffer", Ser::String, strs("u1", 3)),
        )
        .op_state(
            1,
            OpState::broadcast("broadcast-config", Ser::String, Ser::String, config(1)),
        )
}

/// What the explorer must show for `operator(big)`.
fn expected(big: bool) -> OpStates {
    let quoted = |prefix: &str, n: usize| -> Vec<String> {
        (0..n).map(|i| format!("\"{}-{}\"", prefix, i)).collect()
    };
    let mut buffer0 = quoted("user", 5);
    if big {
        buffer0.push(format!("\"{}\"", "abcdefgh".repeat(20_000)));
    }
    let config = |sub: i32| {
        vec![format!(
            "{{ \"config-b\": \"B{}\", \"config-a\": \"A{}\" }}",
            sub, sub
        )]
    };
    let s = |name: &str, mode: &str, labels: Vec<String>| {
        (name.to_string(), format!("operator/{}", mode), labels)
    };
    vec![
        s("operator-buffer", "SPLIT_DISTRIBUTE", buffer0),
        s("union-buffer", "UNION", quoted("u0", 3)),
        s("empty-buffer", "SPLIT_DISTRIBUTE", Vec::new()),
        s("broadcast-config", "BROADCAST", config(0)),
        s("operator-buffer", "SPLIT_DISTRIBUTE", quoted("org", 4)),
        s("union-buffer", "UNION", quoted("u1", 3)),
        s("broadcast-config", "BROADCAST", config(1)),
    ]
}

fn write(storage: Storage, compressed: bool, op: KeyedOperator, tag: &str) -> std::path::PathBuf {
    let mut spec = Spec::new(storage);
    spec.compressed = compressed;
    spec.operators.push(op);
    let dir = synth::temp_dir(tag);
    synth::write_savepoint(&spec, &dir);
    dir
}

#[test]
fn file_backed_streams_decode_like_their_inline_twin() {
    let inline = index(&write(Storage::Inline, false, operator(false), "op-inline"));
    assert_eq!(op_states(&inline), expected(false));

    // Every other delegate a stream can have: a file relative to the savepoint (what production
    // savepoints use above the inline threshold), an absolute file, a segment of a shared file.
    for storage in [Storage::Files, Storage::AbsoluteFiles, Storage::Segments] {
        let dir = write(storage, false, operator(false), "op-files");
        let idx = index(&dir);
        assert_eq!(op_states(&idx), op_states(&inline), "{:?}", storage);
        assert_eq!(
            exported_op_states(&idx, "op-files-export"),
            exported_op_states(&inline, "op-inline-export"),
            "{:?}: JSON export",
            storage
        );
    }
}

#[test]
fn compressed_streams_decode_like_uncompressed_ones() {
    let plain = op_states(&index(&write(
        Storage::Inline,
        false,
        operator(true),
        "op-plain",
    )));
    assert_eq!(plain, expected(true));
    for storage in [Storage::Inline, Storage::Files, Storage::Segments] {
        let dir = write(storage, true, operator(true), "op-compressed");
        assert_eq!(op_states(&index(&dir)), plain, "{:?}, compressed", storage);
    }
}

#[test]
fn compressed_layout_puts_the_identifier_before_the_first_offset() {
    // The synthetic stream itself: header, Snappy stream identifier, then one run of chunks
    // per element, each starting at its offset.
    let states = [OpState::list(
        "s",
        Ser::String,
        vec![Val::str("a"), Val::str("bb")],
    )];
    let (plain, plain_offsets) = synth::operator_stream(&states, false);
    let (packed, offsets) = synth::operator_stream(&states, true);
    let header = plain_offsets[0][0] as usize;
    assert_eq!(packed[4], 1, "compression flag");
    assert_eq!(packed[5..header], plain[5..header]);
    assert_eq!(packed[header..header + 10], synth::SNAPPY_STREAM_IDENTIFIER);
    assert_eq!(offsets[0][0] as usize, header + 10);
    assert_eq!(packed[offsets[0][1] as usize], 0x01, "uncompressed chunk");
}

#[test]
fn an_unreadable_stream_is_a_visible_error_not_a_placeholder() {
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(operator(false));
    let dir = synth::temp_dir("op-missing");
    let written = synth::write_savepoint(&spec, &dir);
    let (_, _, missing) = written
        .op_state_files
        .iter()
        .find(|(_, subtask, _)| *subtask == 1)
        .unwrap()
        .clone();
    std::fs::remove_file(&missing).unwrap();
    let (_, _, truncated) = written
        .op_state_files
        .iter()
        .find(|(_, subtask, _)| *subtask == 0)
        .unwrap()
        .clone();
    let bytes = std::fs::read(&truncated).unwrap();
    // Cut 40 bytes: the 28-byte broadcast map and the union list's last 12 bytes (its three
    // elements are 5 bytes each). What is before still decodes, the element cut short shows its
    // remaining bytes, and what starts past the end is an error.
    std::fs::write(&truncated, &bytes[..bytes.len() - 40]).unwrap();

    let got = op_states(&index(&dir));
    let names: Vec<&str> = got.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "operator-buffer",
            "union-buffer",
            "empty-buffer",
            "broadcast-config",
            "operator-buffer",
            "union-buffer",
            "broadcast-config"
        ],
        "every state keeps its name and mode"
    );
    // Subtask 1: one entry per state, naming the missing file and why.
    for (name, _, labels) in &got[4..] {
        assert_eq!(labels.len(), 1, "{}: {:?}", name, labels);
        assert!(
            labels[0].starts_with("[error: ")
                && labels[0].contains(&*missing.to_string_lossy())
                && labels[0].contains("No such file"),
            "{}: {}",
            name,
            labels[0]
        );
    }
    // Subtask 0: the list decodes; the union list's elements and the broadcast map that now
    // start past the end are errors.
    let past_the_end = |l: &String| l.starts_with("[error: offset ") && l.contains("outside the");
    assert_eq!(got[0].2, expected(false)[0].2);
    assert!(got[1].2[0].starts_with("[3 bytes]"), "{:?}", got[1].2);
    assert!(got[1].2[1..].iter().all(past_the_end), "{:?}", got[1].2);
    assert!(past_the_end(&got[3].2[0]), "{:?}", got[3].2);
    // Never a `name[i]` placeholder or a generic `managed-operator-subtaskN` dump.
    assert!(got
        .iter()
        .flat_map(|(_, _, l)| l)
        .all(|l| !l.contains("buffer[") && !l.contains("config[")));
    assert!(names.iter().all(|n| !n.starts_with("managed-operator")));
}

#[test]
fn element_boundaries_are_found_in_linear_time() {
    // The end of each element used to be a linear scan over all offsets: O(n²) in the elements
    // of a stream (in release, 250k took 17 s and 1M 250 s). With a binary search 1M elements
    // index in ~0.3 s in release, 200k in ~0.2 s in debug: bound both loosely, and check the
    // growth.
    let build = |n: usize, tag: &str| -> (std::path::PathBuf, usize) {
        let elements = (0..n).map(|i| Val::Str(format!("e{:07}", i))).collect();
        let op = KeyedOperator::new(0x0f, 1, 128, Ser::String)
            .op_state(0, OpState::list("big-list", Ser::String, elements));
        (write(Storage::Inline, false, op, tag), n)
    };
    let time = |(dir, n): &(std::path::PathBuf, usize)| -> Duration {
        let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let t = Instant::now();
        let idx = build_index(&savepoint, 0, 0, dir);
        let elapsed = t.elapsed();
        let s = &idx.operators[0].states[0];
        assert_eq!(s.entries.len(), *n);
        assert_eq!(
            s.entries[*n - 1].key,
            format!("\"e{:07}\"", n - 1).into_bytes()
        );
        elapsed
    };
    let n = if cfg!(debug_assertions) {
        200_000
    } else {
        1_000_000
    };
    let small = build(n / 4, "op-linear-small");
    let large = build(n, "op-linear-large");
    let (t_small, t_large) = (time(&small), time(&large));
    println!("{} elements: {:?}; {}: {:?}", n / 4, t_small, n, t_large);
    for (dir, _) in [&small, &large] {
        std::fs::remove_dir_all(dir).ok();
    }
    let bound = Duration::from_secs(if cfg!(debug_assertions) { 10 } else { 1 });
    assert!(t_large < bound, "{} elements took {:?}", n, t_large);
    // 4x the elements: ~4x the time when linear, ~16x when quadratic.
    assert!(
        t_large < t_small * 10 + Duration::from_millis(200),
        "{:?} for {} vs {:?} for {}: not linear",
        t_large,
        n,
        t_small,
        n / 4
    );
}

#[test]
fn operator_stream_matches_real_flink_streams() {
    // ClusterJob's operator states, as Flink 1.20 wrote them, uncompressed and compressed
    // (local fixtures from `mvn verify`; skipped in CI). The elements are taken from the real
    // index, the bytes are synth's: every byte of header, framing and offsets must match.
    for (dir, compressed) in [
        ("tests/fixtures/savepoint-gitlab", false),
        ("tests/fixtures/savepoint-compressed", true),
    ] {
        let dir = Path::new(dir);
        if !dir.join("_metadata").exists() {
            eprintln!("skipped: {} not generated", dir.display());
            continue;
        }
        let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let idx = build_index(&savepoint, 0, 0, dir);
        let mut checked = 0;
        for (op, cached) in savepoint.operators.iter().zip(&idx.operators) {
            let mut cached_states = cached.states.iter();
            for sub in &op.subtask_states {
                let Some(parser::state_handle::StateHandle::OperatorStream(os)) =
                    &sub.managed_operator_state
                else {
                    continue;
                };
                let parser::state_handle::StreamStateHandle::ByteStream(bs) = &os.delegate_handle
                else {
                    panic!("ClusterJob's operator state is inline");
                };
                // The index has one state per partition, in handle order; rebuild them.
                let mut states = Vec::new();
                for part in &os.partition_offsets {
                    let labels: Vec<String> = cached_states
                        .next()
                        .unwrap()
                        .entries
                        .iter()
                        .map(|e| String::from_utf8(e.key.clone()).unwrap())
                        .collect();
                    let unquote = |s: &str| Val::str(s.trim_matches('"'));
                    states.push(match part.mode {
                        0 => OpState::list(
                            &part.state_name,
                            Ser::String,
                            labels.iter().map(|l| unquote(l)).collect(),
                        ),
                        1 => OpState::union_list(
                            &part.state_name,
                            Ser::String,
                            labels.iter().map(|l| unquote(l)).collect(),
                        ),
                        _ => {
                            let pairs = labels[0].trim_start_matches("{ ").trim_end_matches(" }");
                            let entries = pairs
                                .split(", ")
                                .map(|kv| {
                                    let (k, v) = kv.split_once(": ").unwrap();
                                    (unquote(k), unquote(v))
                                })
                                .collect();
                            OpState::broadcast(&part.state_name, Ser::String, Ser::String, entries)
                        }
                    });
                }
                // Flink writes list states first, in its HashMap order: follow the stream's.
                let mut order: Vec<usize> = (0..states.len()).collect();
                order.sort_by_key(|&i| os.partition_offsets[i].offsets.first().copied());
                let states: Vec<OpState> = order.iter().map(|&i| states[i].clone()).collect();
                let (ours, offsets) = synth::operator_stream(&states, compressed);
                assert_eq!(
                    ours,
                    bs.data,
                    "{}: subtask {}",
                    dir.display(),
                    sub.subtask_index
                );
                for (k, &i) in order.iter().enumerate() {
                    assert_eq!(offsets[k], os.partition_offsets[i].offsets);
                }
                checked += 1;
            }
        }
        assert!(
            checked >= 4,
            "{}: {} streams compared",
            dir.display(),
            checked
        );
    }
}
