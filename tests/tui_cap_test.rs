//! The TUI has no key cap any more (#13, #24). It used to keep at most 100,000 keys per operator
//! in an in-memory index; it now asks the `StateReader` for the page it shows, so every key of
//! every subtask is reachable. These fixture-free tests page a synthetic 2-subtask savepoint with
//! more keys than the old cap to its last key and back, check that both subtasks' keys are shown,
//! and that the `E` export streams every key.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use common::tui::{draw, labels, open_tree};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use flink_explorer::reader::{Cursor, StateReader};
use flink_explorer::tui::app::{App, AppOptions};
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::Terminal;
use serde_json::Value;

/// More than the old `MAX_STORED_PARTITIONS` (100,000).
const MANY: usize = 150_000;

fn operator(keys: usize, parallelism: i32) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x2a, parallelism, 128, Ser::String)
        .state("counter", Kind::Value(Ser::Long));
    for i in 0..keys {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Str(format!("key-{:06}", i)),
            value: Val::Long(i as i64 * 3 + 1),
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

fn subtask_of(key: &str, op: &KeyedOperator) -> i32 {
    let kg = synth::key_group_for_hash(synth::java_string_hash(key), op.max_parallelism);
    (0..op.parallelism)
        .find(|&s| {
            let (start, end) = synth::range_for_subtask(op.max_parallelism, op.parallelism, s);
            (start..=end).contains(&kg)
        })
        .unwrap()
}

fn row<'a>(rows: &'a [String], needle: &str) -> &'a str {
    rows.iter()
        .find(|r| r.contains(needle))
        .unwrap_or_else(|| panic!("no row contains {:?}:\n{}", needle, rows.join("\n")))
}

/// The keys of the drawn rows (`⯈ key (1 states)`).
fn shown_keys(rows: &[String]) -> Vec<String> {
    rows.iter()
        .filter_map(|r| r.trim_start().strip_prefix("⯈ "))
        .filter_map(|r| r.strip_suffix(" (1 states)"))
        .map(str::to_string)
        .collect()
}

/// The keys in the reader's order: key-groups ascending, on-disk order inside one.
fn reader_order(dir: &std::path::Path) -> Vec<String> {
    let reader = StateReader::open(dir).unwrap();
    let op = reader.operators()[0].id.clone();
    let mut out = Vec::new();
    let mut cursor = Some(Cursor::START);
    while let Some(c) = cursor {
        let page = reader.key_page(&op, &c, 50_000).unwrap();
        out.extend(page.keys.into_iter().map(|k| k.key));
        cursor = page.next;
    }
    out
}

/// 150,000 keys over two subtasks: PageDown reaches the very last key (through both subtasks),
/// the pager counts every one of them, and PageUp goes back to the first. Nothing caps it.
#[test]
fn paging_reaches_every_key_of_both_subtasks_past_the_old_cap() {
    let op = operator(MANY, 2);
    let dir = write(&op, "no-cap");
    let order = reader_order(&dir);
    assert_eq!(order.len(), MANY);
    let (first, last) = (order[0].clone(), order[MANY - 1].clone());
    assert_eq!(subtask_of(&first, &op), 0);
    assert_eq!(subtask_of(&last, &op), 1, "the last key is subtask 1's");

    let mut tree = open_tree(&dir);
    // A tall terminal: a page is ~500 rows, so a PageDown scrolls the keys by that much.
    let mut terminal = Terminal::new(TestBackend::new(100, 502)).unwrap();
    let rows = labels(&draw(&mut terminal, &mut tree));
    assert!(
        row(&rows, "2a2a").contains("(? keys, 1 states)"),
        "{:?}",
        rows
    );
    tree.jump_first();
    tree.expand();
    let rows = labels(&draw(&mut terminal, &mut tree));
    // Before the end is reached, the total is a lower bound.
    assert!(
        row(&rows, "↓ 1-30 / ").contains("/ ≥ "),
        "{}",
        row(&rows, "↓ 1-30")
    );
    assert_eq!(shown_keys(&rows)[0], first);

    let mut subtasks = BTreeSet::new();
    let mut presses = 0;
    loop {
        tree.page_down();
        presses += 1;
        let path = tree.selected_path().to_string();
        if let Some(key) = path.rsplit(" > ").next().filter(|k| k.starts_with("key-")) {
            subtasks.insert(subtask_of(key, &op));
        }
        if path.ends_with(&format!("> {}", last)) {
            break;
        }
        assert!(presses < 1_000, "stuck at {}", path);
        if presses % 50 == 0 {
            draw(&mut terminal, &mut tree);
        }
    }
    assert_eq!(
        subtasks,
        BTreeSet::from([0, 1]),
        "keys of both subtasks shown"
    );
    let rows = labels(&draw(&mut terminal, &mut tree));
    // At the end the count is exact: every key, no "not indexed" remainder.
    let up = format!("↑ {}-{} / {}", MANY - 29, MANY, MANY);
    row(&rows, &up);
    assert!(
        !rows.iter().any(|r| r.contains('↓')),
        "no page past the last key"
    );
    assert!(!rows.iter().any(|r| r.contains("not indexed")));
    assert!(row(&rows, "2a2a").contains(&format!("({} keys, 1 states)", MANY)));
    assert_eq!(shown_keys(&rows).last(), Some(&last));

    // And back to the first key.
    let mut presses = 0;
    while !tree.selected_path().ends_with(&format!("> {}", first)) {
        tree.page_up();
        presses += 1;
        assert!(presses < 1_000, "stuck at {}", tree.selected_path());
    }
    let rows = labels(&draw(&mut terminal, &mut tree));
    assert!(!rows.iter().any(|r| r.contains('↑')), "{}", rows.join("\n"));
}

/// `↓` held from the first key to the last of a few hundred keys, one row at a time, reaches
/// every key exactly in the reader's order, and `↑` comes back (the cap used to stop it, then
/// panic).
#[test]
fn moving_down_row_by_row_shows_every_key_in_reader_order() {
    let op = operator(400, 2);
    let dir = write(&op, "row-by-row");
    let order = reader_order(&dir);
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    let mut seen = Vec::new();
    for _ in 0..order.len() + 10 {
        tree.move_down();
        let key = tree
            .selected_path()
            .rsplit(" > ")
            .next()
            .unwrap()
            .to_string();
        if seen.last() != Some(&key) {
            seen.push(key);
        }
    }
    assert_eq!(seen, order, "every key, once, in the reader's order");
    for _ in 0..order.len() + 10 {
        tree.move_up();
    }
    assert!(
        !tree.selected_path().contains(" > "),
        "back on the operator"
    );
}

/// The keys of every subtask are shown, whatever their number: three and four subtasks.
#[test]
fn every_subtask_is_reachable() {
    for parallelism in [3, 4] {
        let op = operator(300, parallelism);
        let dir = write(&op, "every-subtask");
        let mut tree = open_tree(&dir);
        tree.jump_first();
        tree.expand();
        let mut terminal = Terminal::new(TestBackend::new(100, 40)).unwrap();
        let mut subtasks = BTreeSet::new();
        for _ in 0..40 {
            for k in shown_keys(&labels(&draw(&mut terminal, &mut tree))) {
                subtasks.insert(subtask_of(&k, &op));
            }
            tree.page_down();
        }
        let all: BTreeSet<i32> = (0..parallelism).collect();
        assert_eq!(subtasks, all, "parallelism {}", parallelism);
    }
}

/// `E` streams every key of every subtask into the JSON export — there is no `truncated` flag
/// any more, since nothing is left out.
#[test]
fn the_json_export_streams_every_key() {
    let op = operator(MANY / 10, 2);
    let dir = write(&op, "export-all");
    let reader = StateReader::open(&dir).unwrap();
    let summaries = flink_explorer::operators::summarize(reader.savepoint(), &dir);
    let mut out = Vec::new();
    let mut last_progress = 0;
    let summary = flink_explorer::export::write_json(
        &reader,
        &summaries,
        &mut out,
        &AtomicBool::new(false),
        &mut |n| last_progress = n,
    )
    .unwrap();
    assert_eq!(summary.keys, MANY / 10);
    assert_eq!(last_progress, MANY / 10);
    let v: Value = serde_json::from_slice(&out).expect("valid JSON");
    assert!(v.get("truncated").is_none());
    let ops = v["operators"].as_array().unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0]["total_keys"], MANY / 10);
    let keys = ops[0]["keys"].as_array().unwrap();
    assert_eq!(keys.len(), MANY / 10);
    let subtasks: BTreeSet<i32> = keys
        .iter()
        .map(|k| subtask_of(k["key"].as_str().unwrap(), &op))
        .collect();
    assert_eq!(subtasks, BTreeSet::from([0, 1]));
    let k = keys.iter().find(|k| k["key"] == "key-000007").unwrap();
    assert_eq!(k["states"]["counter"], 22);
    assert_eq!(ops[0]["info"]["Keys"], (MANY / 10).to_string());
    assert_eq!(ops[0]["state_descriptors"][0]["name"], "counter");

    // Cancelled, it stops instead of writing on.
    let err = flink_explorer::export::write_json(
        &reader,
        &summaries,
        &mut Vec::new(),
        &AtomicBool::new(true),
        &mut |_| {},
    )
    .unwrap_err();
    assert!(err.contains("cancelled"), "{}", err);
}

/// `E` in the app runs on a worker thread and writes `export.json` (through a side file).
#[test]
fn the_app_exports_in_the_background() {
    let op = operator(500, 2);
    let dir = write(&op, "export-app");
    let mut app = App::open(dir.to_path_buf(), AppOptions::default(), |_| {}).unwrap();
    app.handle_key(KeyEvent::new(KeyCode::Char('E'), KeyModifiers::SHIFT));
    app.wait_for_export();
    let status = app.status().unwrap_or_default().to_string();
    assert!(status.starts_with("Exported 500 keys to "), "{}", status);
    let v: Value =
        serde_json::from_slice(&std::fs::read(dir.join("export.json")).unwrap()).unwrap();
    assert_eq!(v["operators"][0]["keys"].as_array().unwrap().len(), 500);
    assert!(!dir.join("export.json.partial").exists());
}

/// A tree made of one operator's summary shows that operator only (what the parity tests use).
#[test]
fn a_tree_over_a_subset_of_operators() {
    let mut spec = Spec::new(Storage::Inline);
    spec.operators.push(operator(10, 1));
    let mut other = operator(5, 1);
    other.id = [0x3b; 16];
    spec.operators.push(other);
    let dir = synth::temp_dir("subset");
    synth::write_savepoint(&spec, &dir);
    let reader = std::sync::Arc::new(StateReader::open(&dir).unwrap());
    let mut summaries = flink_explorer::operators::summarize(reader.savepoint(), &dir);
    let second = summaries.remove(1);
    let mut tree = TreeView::new(reader, vec![second]);
    let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
    let rows = labels(&draw(&mut terminal, &mut tree));
    assert!(rows.iter().any(|r| r.contains("3b3b")), "{:?}", rows);
    assert!(!rows.iter().any(|r| r.contains("2a2a")), "{:?}", rows);
}
