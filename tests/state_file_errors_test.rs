//! A missing, truncated or unreadable state file is never silent (issue #16): the index records
//! it per operator — and still loads the other subtasks — the TUI shows it, a partial index is
//! never cached, and the headless paths fail naming the file. Fixture-free: synthetic file-backed
//! savepoints (`tests/common/synth.rs`) with two subtasks, one of whose files is then damaged.

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use flink_explorer::export_state::{export_state_ndjson, find_state, list_states};
use flink_explorer::index::builder::build_index;
use flink_explorer::index::cache::{IndexCache, CACHE_FILE_NAME};
use flink_explorer::parser;
use flink_explorer::tui::app::load_index;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;
use ratatui::widgets::Block;
use ratatui::Terminal;

const KEYS: usize = 200;

/// A 2-subtask, file-backed savepoint; returns its directory (deleted when the guard drops, so
/// keep it alive for the whole test) and the state file of each subtask.
fn savepoint(tag: &str) -> (synth::TempDir, [PathBuf; 2]) {
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(synth::sample_operator(KEYS));
    let dir = synth::temp_dir(tag);
    let written = synth::write_savepoint(&spec, &dir);
    let file = |subtask: i32| {
        written
            .state_files
            .iter()
            .find(|(op, st, _)| *op == 0 && *st == subtask)
            .map(|(_, _, p)| p.clone())
            .expect("one state file per subtask")
    };
    (dir, [file(0), file(1)])
}

/// The keys Flink assigns to `subtask` (see `synth::range_for_subtask`).
fn keys_of_subtask(subtask: i32) -> BTreeSet<String> {
    let op = synth::sample_operator(KEYS);
    let (start, end) = synth::range_for_subtask(op.max_parallelism, op.parallelism, subtask);
    (0..KEYS)
        .map(|i| format!("key-{:07}", i))
        .filter(|k| {
            let kg = synth::key_group_for_hash(synth::java_string_hash(k), op.max_parallelism);
            (start..=end).contains(&kg)
        })
        .collect()
}

fn index(dir: &Path) -> IndexCache {
    let sp = parser::parse_metadata(&dir.join("_metadata")).expect("synthetic _metadata parses");
    build_index(&sp, 0, 0, dir)
}

fn truncate_to_half(path: &Path) -> (u64, u64) {
    let len = std::fs::metadata(path).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(len / 2).unwrap();
    (len / 2, len)
}

/// The rendered tree in its default view (`hide_empty` on), one screen line per node.
fn tree_screen(index: &IndexCache) -> String {
    let mut tv = TreeView::new(index);
    let mut term = Terminal::new(TestBackend::new(160, 20)).unwrap();
    term.draw(|f| tv.render(f, Block::default(), Rect::new(0, 0, 160, 20)))
        .unwrap();
    let buf = term.backend().buffer();
    let mut s = String::new();
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            s.push_str(buf[(x, y)].symbol());
        }
        s.push('\n');
    }
    s
}

/// Assert the index lost exactly `bad`'s subtask, said so naming `bad`, and kept the other one.
fn assert_partial_index(index: &IndexCache, bad: &Path, good_subtask: i32, expect: &str) {
    let op = &index.operators[0];
    assert_eq!(op.load_errors.len(), 1, "{:?}", op.load_errors);
    let err = &op.load_errors[0];
    assert!(err.contains(&bad.display().to_string()), "{}", err);
    assert!(err.contains(expect), "{}", err);
    assert!(index.has_load_errors());

    let ks = op
        .keyed_state
        .as_ref()
        .expect("the readable subtask still loads");
    let keys: BTreeSet<String> = ks.partitions.iter().map(|p| p.key.clone()).collect();
    let expected = keys_of_subtask(good_subtask);
    assert!(!expected.is_empty());
    assert_eq!(keys, expected);
    assert_eq!(ks.total_keys, expected.len());

    let info = op.info_lines();
    let (_, line) = info
        .iter()
        .find(|(k, _)| k == "Load errors")
        .expect("an info line for the load errors");
    assert!(line.contains(&bad.display().to_string()), "{}", line);
}

#[test]
fn index_records_a_missing_state_file_and_keeps_the_other_subtask() {
    for bad in [0, 1] {
        let (dir, files) = savepoint("missing");
        std::fs::remove_file(&files[bad]).unwrap();
        let idx = index(&dir);
        assert_partial_index(
            &idx,
            &files[bad],
            1 - bad as i32,
            &std::io::Error::from_raw_os_error(2).to_string(), // ENOENT, in the platform's words
        );
    }
}

#[test]
fn index_records_a_truncated_state_file() {
    let (dir, files) = savepoint("truncated");
    let (now, was) = truncate_to_half(&files[1]);
    let idx = index(&dir);
    assert_partial_index(
        &idx,
        &files[1],
        0,
        &format!("truncated: {} of {} bytes", now, was),
    );
}

#[cfg(unix)]
#[test]
fn index_and_export_report_an_unreadable_state_file() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, files) = savepoint("unreadable");
    std::fs::set_permissions(&files[1], std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&files[1]).is_ok() {
        // Root (e.g. some CI containers) reads through any mode bits.
        eprintln!("skipped: this user can read a mode-000 file");
        return;
    }
    let idx = index(&dir);
    assert_partial_index(&idx, &files[1], 0, "Permission denied");

    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let err = export_state_ndjson(&sp, &dir, None, "counter", None, &mut Vec::new()).unwrap_err();
    assert!(err.contains(&files[1].display().to_string()), "{}", err);
    let listing = list_states(&sp, &dir);
    assert_eq!(listing.errors.len(), 1, "{:?}", listing.errors);
    assert!(listing.errors[0].contains("Permission denied"));

    std::fs::set_permissions(&files[1], std::fs::Permissions::from_mode(0o644)).unwrap();
}

#[test]
fn an_operator_whose_files_are_all_missing_stays_in_the_tree() {
    let (dir, files) = savepoint("all-missing");
    for f in &files {
        std::fs::remove_file(f).unwrap();
    }
    let idx = index(&dir);
    let op = &idx.operators[0];
    assert!(op.keyed_state.is_none());
    assert_eq!(op.load_errors.len(), 2);

    // hide_empty is on by default: an operator with no keys would be hidden, and its error with
    // it, if load errors did not keep it visible.
    let screen = tree_screen(&idx);
    let line = screen
        .lines()
        .find(|l| l.contains(&op.display_name))
        .unwrap_or_else(|| panic!("operator hidden:\n{}", screen));
    assert!(line.contains("⚠ 2 state file(s) not read"), "{:?}", line);
}

#[test]
fn the_tree_label_flags_a_partial_operator() {
    let (dir, files) = savepoint("label");
    std::fs::remove_file(&files[1]).unwrap();
    let idx = index(&dir);
    let screen = tree_screen(&idx);
    let line = screen
        .lines()
        .find(|l| l.contains(&idx.operators[0].display_name))
        .expect("operator line");
    let keys = keys_of_subtask(0).len();
    assert!(line.contains(&format!("{} keys", keys)), "{:?}", line);
    assert!(line.contains("⚠ 1 state file(s) not read"), "{:?}", line);
}

#[test]
fn export_fails_naming_a_missing_state_file_before_writing_any_row() {
    for bad in [0, 1] {
        let (dir, files) = savepoint("export-missing");
        std::fs::remove_file(&files[bad]).unwrap();
        let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let mut out = Vec::new();
        let err = export_state_ndjson(&sp, &dir, None, "counter", None, &mut out).unwrap_err();
        // A bad first file is an I/O error, not "state not found".
        assert!(
            err.contains(&files[bad].display().to_string()),
            "subtask {}: {}",
            bad,
            err
        );
        assert!(!err.contains("not found in any"), "{}", err);
        assert!(out.is_empty(), "rows were written before the error");
    }
}

#[test]
fn export_fails_naming_a_truncated_state_file() {
    let (dir, files) = savepoint("export-truncated");
    let (now, was) = truncate_to_half(&files[1]);
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let mut out = Vec::new();
    let err = export_state_ndjson(&sp, &dir, None, "counter", None, &mut out).unwrap_err();
    assert!(err.contains(&files[1].display().to_string()), "{}", err);
    assert!(
        err.contains(&format!("truncated: {} of {} bytes", now, was)),
        "{}",
        err
    );
    assert!(out.is_empty());
}

/// The state is found in one operator, but another one has a file that cannot be read: that one
/// may hold the state too, so the export refuses to guess — and `--operator` lifts the doubt.
#[test]
fn an_unreadable_other_operator_makes_the_target_ambiguous_until_named() {
    let target = synth::sample_operator(KEYS);
    let mut other = KeyedOperator::new(0x51, 2, 128, Ser::Long).state("v", Kind::Value(Ser::Int));
    for k in 0..50i64 {
        other.items.push(Item::Value {
            state: 0,
            key: Val::Long(k),
            value: Val::Int(k as i32),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(target.clone());
    spec.operators.push(other);
    let dir = synth::temp_dir("other-op");
    let written = synth::write_savepoint(&spec, &dir);
    let (_, _, bad) = written
        .state_files
        .iter()
        .find(|(op, st, _)| *op == 1 && *st == 0)
        .unwrap();
    std::fs::remove_file(bad).unwrap();
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();

    let err = export_state_ndjson(&sp, &dir, None, "counter", None, &mut Vec::new()).unwrap_err();
    assert!(err.contains(&bad.display().to_string()), "{}", err);
    assert!(err.contains("--operator"), "{}", err);

    let mut out = Vec::new();
    let stats = export_state_ndjson(
        &sp,
        &dir,
        Some(&target.id_hex()[..8]),
        "counter",
        None,
        &mut out,
    )
    .expect("the named operator's files are all readable");
    assert_eq!(stats.rows, KEYS as u64);
}

#[test]
fn list_and_find_report_a_missing_state_file() {
    let (dir, files) = savepoint("list-missing");
    std::fs::remove_file(&files[0]).unwrap();
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();

    let listing = list_states(&sp, &dir);
    assert_eq!(listing.errors.len(), 1, "{:?}", listing.errors);
    assert!(listing.errors[0].contains(&files[0].display().to_string()));
    // The other subtask's header still lists the operator's states.
    let names: Vec<&str> = listing.states.iter().map(|(_, n, _)| n.as_str()).collect();
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

    let err = find_state(&sp, &dir, "counter").unwrap_err();
    assert!(err.contains(&files[0].display().to_string()), "{}", err);
}

#[test]
fn the_cli_exits_non_zero_naming_the_file() {
    let (dir, files) = savepoint("cli");
    std::fs::remove_file(&files[0]).unwrap();
    let bin = env!("CARGO_BIN_EXE_flink-explorer");

    let list = Command::new(bin)
        .arg(&*dir)
        .arg("--list-states")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&list.stderr);
    assert_ne!(list.status.code(), Some(0), "{}", stderr);
    assert!(
        stderr.contains(&files[0].display().to_string()),
        "{}",
        stderr
    );
    assert!(!stderr.contains("No keyed state found"), "{}", stderr);

    let export = Command::new(bin)
        .arg(&*dir)
        .args(["--export-state", "counter", "-o", "-"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&export.stderr);
    assert_ne!(export.status.code(), Some(0), "{}", stderr);
    assert!(
        stderr.contains(&files[0].display().to_string()),
        "{}",
        stderr
    );
    assert!(export.stdout.is_empty());
}

#[test]
fn no_cache_is_written_after_a_partial_build() {
    let (dir, files) = savepoint("cache-partial");
    std::fs::remove_file(&files[1]).unwrap();
    let idx = load_index(&dir, false, &mut |_| {}).unwrap();
    assert!(idx.has_load_errors());
    assert!(
        !dir.join(CACHE_FILE_NAME).exists(),
        "a partial index was cached"
    );

    // Once the file is back, the next load builds the full index (and caches it).
    synth::write_savepoint(
        &{
            let mut spec = Spec::new(Storage::Files);
            spec.operators.push(synth::sample_operator(KEYS));
            spec
        },
        &dir,
    );
    let idx = load_index(&dir, false, &mut |_| {}).unwrap();
    assert!(!idx.has_load_errors());
    assert_eq!(
        idx.operators[0].keyed_state.as_ref().unwrap().total_keys,
        KEYS
    );
    assert!(dir.join(CACHE_FILE_NAME).exists());
}

#[test]
fn no_cache_neither_reads_nor_writes_the_cache() {
    let (dir, files) = savepoint("cache-flag");
    load_index(&dir, true, &mut |_| {}).unwrap();
    assert!(
        !dir.join(CACHE_FILE_NAME).exists(),
        "--no-cache wrote a cache"
    );

    // Without the flag the complete index is cached ...
    load_index(&dir, false, &mut |_| {}).unwrap();
    let cached = std::fs::read(dir.join(CACHE_FILE_NAME)).expect("cache written");

    // ... and with it, that cache is not read: a file removed since shows up as an error, which
    // the cached index (built while the file was there) would not have.
    std::fs::remove_file(&files[0]).unwrap();
    let idx = load_index(&dir, true, &mut |_| {}).unwrap();
    assert!(idx.has_load_errors(), "--no-cache read the cache");
    assert_eq!(
        std::fs::read(dir.join(CACHE_FILE_NAME)).unwrap(),
        cached,
        "--no-cache rewrote the cache"
    );
}

#[test]
fn a_subtask_that_does_not_register_the_state_contributes_no_rows() {
    // Flink registers a state on first access, so subtasks can disagree: subtask 0 knows
    // [counter, label], subtask 1 knows [counter, other] — stateId 1 means two different states.
    let mut op = KeyedOperator::new(0x61, 2, 128, Ser::String)
        .state("counter", Kind::Value(Ser::Long))
        .state("label", Kind::Value(Ser::String))
        .state("other", Kind::Value(Ser::Long));
    op.subtask_states = Some(vec![
        vec!["counter".into(), "label".into()],
        vec!["counter".into(), "other".into()],
    ]);
    for i in 0..100 {
        let key = Val::Str(format!("key-{:07}", i));
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Long(i),
        });
        op.items.push(Item::Value {
            state: 1,
            key: key.clone(),
            value: Val::Str(format!("l{}", i)),
        });
        op.items.push(Item::Value {
            state: 2,
            key,
            value: Val::Long(-i),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op);
    let dir = synth::temp_dir("per-subtask-headers");
    synth::write_savepoint(&spec, &dir);
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();

    let mut out = Vec::new();
    export_state_ndjson(&sp, &dir, None, "label", None, &mut out).expect("export");
    let rows: Vec<serde_json::Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let in_subtask_0 = keys_of_subtask_n(0, 100);
    assert!(!rows.is_empty());
    for r in &rows {
        assert_eq!(r["subtask"], 0, "subtask 1 has no `label`: {}", r);
        assert!(
            r["value"].as_str().unwrap().starts_with('l'),
            "decoded another state: {}",
            r
        );
        assert!(in_subtask_0.contains(r["key"].as_str().unwrap()));
    }
    assert_eq!(rows.len(), in_subtask_0.len());
}

#[test]
fn an_absolute_file_handle_is_read_through_its_file_scheme() {
    let mut spec = Spec::new(Storage::AbsoluteFiles);
    spec.operators.push(synth::sample_operator(50));
    let dir = synth::temp_dir("absolute");
    synth::write_savepoint(&spec, &dir);
    let index = index(&dir);
    let op = &index.operators[0];
    assert!(op.load_errors.is_empty(), "{:?}", op.load_errors);
    assert_eq!(op.keyed_state.as_ref().unwrap().total_keys, 50);
    let sp = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let mut out = Vec::new();
    let stats = export_state_ndjson(&sp, &dir, None, "counter", None, &mut out).unwrap();
    assert_eq!(stats.rows, 50);
}

fn keys_of_subtask_n(subtask: i32, n: usize) -> BTreeSet<String> {
    let (start, end) = synth::range_for_subtask(128, 2, subtask);
    (0..n)
        .map(|i| format!("key-{:07}", i))
        .filter(|k| {
            (start..=end).contains(&synth::key_group_for_hash(synth::java_string_hash(k), 128))
        })
        .collect()
}

#[test]
fn a_cut_file_is_caught_by_its_key_group_offsets_even_without_a_state_size() {
    // Flink writes stateSize -1 when it could not tell; the offsets still tell.
    let (dir, files) = savepoint("offsets");
    let meta_path = dir.join("_metadata");
    let mut meta = std::fs::read(&meta_path).unwrap();
    // Each RelativeFileStateHandle is [tag 6][utf path][long stateSize]: set every size to -1.
    for f in &files {
        let name = f.file_name().unwrap().to_str().unwrap().as_bytes().to_vec();
        let mut needle = vec![6u8];
        needle.extend_from_slice(&(name.len() as u16).to_be_bytes());
        needle.extend_from_slice(&name);
        let at = meta
            .windows(needle.len())
            .position(|w| w == needle.as_slice())
            .unwrap()
            + needle.len();
        meta[at..at + 8].copy_from_slice(&(-1i64).to_be_bytes());
    }
    std::fs::write(&meta_path, &meta).unwrap();
    truncate_to_half(&files[1]);

    let idx = index(&dir);
    let errs = &idx.operators[0].load_errors;
    assert!(
        errs.iter()
            .any(|e| e.contains(files[1].to_str().unwrap()) && e.contains("truncated: key-group")),
        "{:?}",
        errs
    );
    let sp = parser::parse_metadata(&meta_path).unwrap();
    let e = export_state_ndjson(&sp, &dir, None, "counter", None, &mut Vec::new()).unwrap_err();
    assert!(e.contains("truncated: key-group"), "{}", e);
}
