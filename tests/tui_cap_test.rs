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

/// `E` streams every key of every subtask into the JSON export — `truncated` is always false,
/// since nothing is left out.
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
        &mut |p| last_progress = p.keys,
    )
    .unwrap();
    assert_eq!(summary.keys, MANY / 10);
    assert_eq!(last_progress, MANY / 10);
    let v: Value = serde_json::from_slice(&out).expect("valid JSON");
    // Nothing is left out; `truncated` stays, always false, for readers of older exports.
    assert_eq!(v["truncated"], false);
    let ops = v["operators"].as_array().unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0]["truncated"], false);
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

/// One key `hot` holding a MAP of `entries` entries (`m000000 → 0`, …) and a VALUE, next to a
/// few ordinary keys.
fn hot_map_operator(entries: usize) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x4c, 1, 128, Ser::String)
        .state("big", Kind::Map(Ser::String, Ser::Long))
        .state("counter", Kind::Value(Ser::Long));
    for j in 0..entries {
        op.items.push(Item::MapEntry {
            state: 0,
            key: Val::str("hot"),
            map_key: Val::Str(format!("m{:06}", j)),
            value: Some(Val::Long(j as i64)),
        });
    }
    for i in 0..3 {
        op.items.push(Item::Value {
            state: 1,
            key: Val::Str(format!("cold-{}", i)),
            value: Val::Long(i),
        });
    }
    op
}

/// Expanding a MAP of 200 000 entries reads one page of the entries it shows, not the map: the
/// window past the entries returned with the key is read alone (`StateReader::entries`), and
/// the `↓` pager row scrolls it a page at a time. (It used to read every entry on the UI thread:
/// 1.6M entries took a second and 670 MB.)
#[test]
fn expanding_a_huge_map_reads_one_page() {
    const ENTRIES: usize = 200_000;
    let dir = write(&hot_map_operator(ENTRIES), "hot-map");
    // A small entry limit: the shown window lies past what the key page returns.
    let reader = std::sync::Arc::new(StateReader::open(&dir).unwrap().with_entry_limit(10));
    let summaries = flink_explorer::operators::summarize(reader.savepoint(), &dir);
    let mut tree = TreeView::new(reader, summaries);
    let mut terminal = Terminal::new(TestBackend::new(120, 80)).unwrap();
    tree.jump_first();
    tree.expand();
    // Onto the hot key, open it, then its MAP row.
    let rows = labels(&draw(&mut terminal, &mut tree));
    let hot = rows.iter().position(|r| r.contains("⯈ hot (")).unwrap();
    tree.select_row(hot);
    tree.expand();
    tree.move_down();
    assert!(
        tree.selected_path().ends_with("hot > big"),
        "{}",
        tree.selected_path()
    );
    assert!(tree.expand());
    let rows = labels(&draw(&mut terminal, &mut tree));
    assert!(
        row(&rows, "big [").ends_with(&format!("{} entries", ENTRIES)),
        "{:?}",
        rows
    );
    assert_eq!(
        tree.entries_read(),
        50,
        "one page of 50 entries read, not the map"
    );
    let map_rows: Vec<&String> = rows.iter().filter(|r| r.contains(" : ")).collect();
    assert_eq!(map_rows.len(), 50);
    assert_eq!(map_rows[0].trim(), "m000000 : 0");
    assert_eq!(map_rows[49].trim(), "m000049 : 49");
    row(&rows, &format!("↓ 1-50 / {}", ENTRIES));

    // The bytes of an entry read with the page.
    let at = |rows: &[String], text: &str| rows.iter().position(|r| r.trim() == text).unwrap();
    tree.select_row(at(&rows, "m000020 : 20"));
    let raw = tree.selected_raw_bytes().unwrap();
    assert!(
        raw.value_title
            .contains(&format!("entry 21 of {}", ENTRIES)),
        "{:?}",
        raw
    );

    // Scrolling: one more page per window, never the map.
    tree.select_row(at(&rows, "m000049 : 49"));
    tree.move_down(); // onto the `↓` row: the window scrolls by one entry
    let rows = labels(&draw(&mut terminal, &mut tree));
    row(&rows, &format!("↑ 2-51 / {}", ENTRIES));
    assert!(rows.iter().any(|r| r.trim() == "m000050 : 50"));
    assert_eq!(tree.entries_read(), 100);
    for _ in 0..5 {
        tree.page_down();
    }
    assert!(
        tree.entries_read() <= 50 * 8,
        "{} entries read",
        tree.entries_read()
    );
}

/// The export streams a MAP of 200 000 entries entry by entry, a page at a time: the most
/// entries held at once is one page, every entry is written, in on-disk order, and a cancel is
/// seen inside the one key's map (there is a single page of keys here).
#[test]
fn the_export_streams_a_huge_map_page_by_page() {
    const ENTRIES: usize = 200_000;
    let dir = write(&hot_map_operator(ENTRIES), "hot-map-export");
    let reader = StateReader::open(&dir).unwrap();
    let summaries = flink_explorer::operators::summarize(reader.savepoint(), &dir);
    let mut out = Vec::new();
    let summary = flink_explorer::export::write_json(
        &reader,
        &summaries,
        &mut out,
        &AtomicBool::new(false),
        &mut |_| {},
    )
    .unwrap();
    assert!(
        summary.max_entries_held <= flink_explorer::export::EXPORT_ENTRY_PAGE,
        "{} entries held at once",
        summary.max_entries_held
    );
    assert!(summary.max_entries_held > 0);
    let v: Value = serde_json::from_slice(&out).expect("valid JSON");
    let keys = v["operators"][0]["keys"].as_array().unwrap();
    let hot = keys.iter().find(|k| k["key"] == "hot").unwrap();
    let big = hot["states"]["big"].as_object().unwrap();
    assert_eq!(big.len(), ENTRIES);
    assert_eq!(big["m123456"], 123_456);
    assert_eq!(hot["states"]["counter"], Value::Null);
    let cold = keys.iter().find(|k| k["key"] == "cold-2").unwrap();
    assert_eq!(cold["states"]["big"], Value::Null);
    assert_eq!(cold["states"]["counter"], 2);
    // On-disk order in the text (serde_json sorts what it parses).
    let text = String::from_utf8_lossy(&out).into_owned();
    let at = |k: &str| text.find(&format!("\"{}\":", k)).unwrap();
    assert!(at("m000000") < at("m000001") && at("m000001") < at("m199999"));

    /// Raises `cancel` once `after` bytes were written.
    struct CancelAfter<'a> {
        written: usize,
        after: usize,
        cancel: &'a AtomicBool,
    }
    impl std::io::Write for CancelAfter<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.written += buf.len();
            if self.written > self.after {
                self.cancel
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let cancel = AtomicBool::new(false);
    let mut w = CancelAfter {
        written: 0,
        after: at("m000500"),
        cancel: &cancel,
    };
    let err = flink_explorer::export::write_json(&reader, &summaries, &mut w, &cancel, &mut |_| {})
        .unwrap_err();
    assert!(err.contains("cancelled"), "{}", err);
    assert!(
        w.written < text.len() / 2,
        "stopped inside the map: {} of {} bytes",
        w.written,
        text.len()
    );
}

/// A MAP entry whose map key could not be decoded is put under `#<its index>`: two of them do
/// not collide (they all used to land on `""`, the last one kept).
#[test]
fn map_entries_without_a_map_key_do_not_collide() {
    use flink_explorer::reader::StateEntry;
    let entry = |map_key: Option<&str>, value: i64| StateEntry {
        state: "m".into(),
        state_type: "MAP".into(),
        value_type: "Map<String,Long>".into(),
        map_key: map_key.map(str::to_string),
        namespace: Vec::new(),
        raw: vec![1],
        value: Value::from(value),
    };
    let entries = [entry(None, 1), entry(Some("k"), 2), entry(None, 3)];
    let refs: Vec<&StateEntry> = entries.iter().collect();
    let v = flink_explorer::export::state_value("MAP", &refs).unwrap();
    assert_eq!(v, serde_json::json!({ "#0": 1, "k": 2, "#2": 3 }));
}

/// Keys that contain the pager's arrows are keys, not pager rows: moving onto them selects them
/// (rows are told apart by their kind, never by their label).
#[test]
fn a_key_containing_an_arrow_is_not_taken_for_the_pager() {
    let mut op =
        KeyedOperator::new(0x2a, 1, 128, Ser::String).state("counter", Kind::Value(Ser::Long));
    for (i, key) in ["key-0000", "b ↓ c", "↑ d"].iter().enumerate() {
        op.items.push(Item::Value {
            state: 0,
            key: Val::str(key),
            value: Val::Long(i as i64),
        });
    }
    let dir = write(&op, "arrow-keys");
    let order = reader_order(&dir);
    assert_eq!(order.len(), 3);
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    let last = |tree: &TreeView| {
        tree.selected_path()
            .rsplit(" > ")
            .next()
            .unwrap()
            .to_string()
    };
    let mut path = Vec::new();
    for _ in 0..3 {
        tree.move_down();
        path.push(last(&tree));
    }
    assert_eq!(path, order);
    tree.move_up();
    assert_eq!(last(&tree), order[1]);
}

/// `q` while the export runs does not abort it silently: it says so, and only a second `q` in a
/// row aborts it and quits (another key in between disarms it). Ctrl-C quits at once.
#[test]
fn q_during_an_export_asks_first() {
    let dir = write(&hot_map_operator(300_000), "export-q");
    let shift = |c| KeyEvent::new(KeyCode::Char(c), KeyModifiers::SHIFT);
    let key = |c| KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE);

    let mut app = App::open(dir.to_path_buf(), AppOptions::default(), |_| {}).unwrap();
    app.handle_key(shift('E'));
    assert!(
        app.exporting(),
        "a 300 000-entry export takes longer than this"
    );
    app.handle_key(key('q'));
    assert!(!app.should_quit(), "the first q only asks");
    assert!(app.exporting(), "and the export goes on");
    let status = app.status().unwrap().to_string();
    assert!(status.contains("q again to abort the export"), "{}", status);
    assert!(status.starts_with("Exporting…"), "{}", status);
    // Another key disarms it: the next q asks again.
    app.handle_key(key('j'));
    app.handle_key(key('q'));
    assert!(!app.should_quit());
    app.handle_key(key('q'));
    assert!(app.should_quit());
    assert!(!app.exporting(), "aborted and joined");
    assert!(!dir.join("export.json").exists());
    assert!(!dir.join("export.json.partial").exists());

    // Ctrl-C is a hard stop.
    let mut app = App::open(dir.to_path_buf(), AppOptions::default(), |_| {}).unwrap();
    app.handle_key(shift('E'));
    app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
    assert!(app.should_quit());
    assert!(!app.exporting());

    // Without an export, q quits at once; and the export, left alone, finishes.
    let mut app = App::open(dir.to_path_buf(), AppOptions::default(), |_| {}).unwrap();
    app.handle_key(key('q'));
    assert!(app.should_quit());
    let mut app = App::open(dir.to_path_buf(), AppOptions::default(), |_| {}).unwrap();
    app.handle_key(shift('E'));
    app.wait_for_export();
    let status = app.status().unwrap().to_string();
    assert!(status.starts_with("Exported 4 keys"), "{}", status);
    app.handle_key(key('q'));
    assert!(app.should_quit());
}
