//! The TUI's filter, count and cache on the `StateReader` (#24), fixture-free: a glob is
//! searched with `scan`, a step at a time; an exact key is looked up with `get` (no scan); the
//! background count and the search stop on Ctrl-C (and `Esc` stops a search); nothing is ever
//! written into the savepoint directory — the counts cache lives under the cache directory, and
//! is ignored once `_metadata` changes.

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use common::tui::{draw, labels, open_tree};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use flink_explorer::tui::app::{App, AppOptions};
use flink_explorer::tui::filter::AppliedFilter;
use flink_explorer::tui::tree::{KeyCount, TreeView};
use ratatui::backend::TestBackend;
use ratatui::Terminal;

fn operator(keys: usize, max_parallelism: i32, key_ser: Ser) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x2c, 2, max_parallelism, key_ser)
        .state("counter", Kind::Value(Ser::Long));
    for i in 0..keys {
        let key = match key_ser {
            Ser::Long => Val::Long(i as i64),
            _ => Val::Str(format!("user-{:05}", i)),
        };
        op.items.push(Item::Value {
            state: 0,
            key,
            value: Val::Long(i as i64),
        });
    }
    op
}

fn write(op: KeyedOperator, tag: &str) -> synth::TempDir {
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op);
    let dir = synth::temp_dir(tag);
    synth::write_savepoint(&spec, &dir);
    dir
}

/// Every file under `dir`, with its size and modification time.
fn listing(dir: &Path) -> BTreeSet<(PathBuf, u64, std::time::SystemTime)> {
    let mut out = BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let meta = e.metadata().unwrap();
            if meta.is_dir() {
                stack.push(e.path());
            } else {
                out.insert((e.path(), meta.len(), meta.modified().unwrap()));
            }
        }
    }
    out
}

/// The keys of the expanded operator, as shown (one page).
fn shown_keys(tree: &mut TreeView) -> Vec<String> {
    let mut terminal = Terminal::new(TestBackend::new(100, 60)).unwrap();
    labels(&draw(&mut terminal, tree))
        .iter()
        .filter_map(|r| r.trim_start().strip_prefix("⯈ "))
        .filter_map(|r| r.strip_suffix(" (1 states)"))
        .map(str::to_string)
        .collect()
}

/// Every key under the (expanded) operator, walked through with `↓` from the top (the window
/// scrolls past its pager rows), at most `max` moves.
fn walk_keys(tree: &mut TreeView, max: usize) -> BTreeSet<String> {
    tree.jump_first();
    let mut keys = BTreeSet::new();
    for _ in 0..max {
        tree.move_down();
        if let Some(k) = tree.selected_path().rsplit(" > ").next() {
            if k.starts_with("user-") {
                keys.insert(k.to_string());
            }
        }
    }
    keys
}

fn operator_row(tree: &mut TreeView) -> String {
    let mut terminal = Terminal::new(TestBackend::new(140, 60)).unwrap();
    labels(&draw(&mut terminal, tree))
        .into_iter()
        .find(|r| r.contains("2c2c"))
        .unwrap()
}

/// A glob is matched against every key of both subtasks by `scan`, one budgeted step at a time:
/// the matches grow step by step, the operator row says how far the search is, and once it is
/// done the count is exact.
#[test]
fn a_glob_filter_scans_the_keys_step_by_step() {
    let dir = write(operator(3000, 128, Ser::String), "glob");
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    // One block per report (a 1-byte step): it takes several reports, each showing progress.
    tree.set_search_step_bytes(1);
    tree.apply_filter(&Some(AppliedFilter::Pattern("USER-0*7".to_string())));
    assert!(tree.searching(), "a glob is searched, not looked up");
    let mut steps = 0;
    let mut progress_seen = false;
    while tree.search_step() {
        steps += 1;
        if operator_row(&mut tree).contains("searching ") {
            progress_seen = true;
        }
        assert!(steps < 1_000);
    }
    assert!(steps > 10, "{} steps", steps);
    assert!(progress_seen);
    let want: BTreeSet<String> = (0..3000)
        .map(|i| format!("user-{:05}", i))
        .filter(|k| k.starts_with("user-0") && k.ends_with('7'))
        .collect();
    let row = operator_row(&mut tree);
    assert!(
        row.contains(&format!("({}/? keys, 1 states)", want.len()))
            || row.contains(&format!("({}/≥ ", want.len())),
        "{}",
        row
    );
    // Every match, from both subtasks, walked through row by row.
    let all = walk_keys(&mut tree, 3000);
    assert_eq!(all, want);
}

/// An exact key of a hashable type is found with `get` — its key-group computed, one block read
/// — so there is nothing to search; the tree then shows that key alone, with its value.
#[test]
fn an_exact_key_is_looked_up_without_a_scan() {
    for (ser, key, value) in [
        (Ser::String, "user-01234", "1234"),
        (Ser::Long, "777", "777"),
    ] {
        let dir = write(operator(3000, 128, ser), "exact");
        let mut tree = open_tree(&dir);
        tree.jump_first();
        tree.expand();
        let before = tree.decode_calls();
        tree.apply_filter(&Some(AppliedFilter::Exact(key.as_bytes().to_vec())));
        assert!(!tree.searching(), "an exact key needs no scan");
        assert_eq!(
            tree.decode_calls(),
            before + 1,
            "one key read: the one found"
        );
        assert_eq!(shown_keys(&mut tree), [key]);
        assert!(operator_row(&mut tree).contains("(1/"));
        tree.move_down();
        tree.expand();
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let rows = labels(&draw(&mut terminal, &mut tree));
        assert!(
            rows.iter()
                .any(|r| r == &format!("    • counter: {}", value)),
            "{:?}",
            rows
        );

        // A key that is not there: found absent at once, no scan either.
        tree.apply_filter(&Some(AppliedFilter::Exact(b"999999".to_vec())));
        assert!(!tree.searching());
        assert!(shown_keys(&mut tree).is_empty());
    }

    // A pattern without wildcard shows the exact key at once, and the scan adds the other cases.
    let dir = write(operator(300, 128, Ser::String), "pattern-get");
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    tree.apply_filter(&Some(AppliedFilter::Pattern("user-00042".to_string())));
    assert!(tree.searching());
    assert_eq!(
        shown_keys(&mut tree),
        ["user-00042"],
        "shown before any scan"
    );
    tree.finish_search();
    assert_eq!(shown_keys(&mut tree), ["user-00042"], "and not twice");
}

/// A key-group range seeks its first key-group (on the subtask that holds it) and stops after
/// its last one.
#[test]
fn a_key_group_range_pages_only_its_key_groups() {
    let op = operator(2000, 128, Ser::String);
    let dir = write(op.clone(), "kg-range");
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    tree.apply_filter(&Some(AppliedFilter::KeyGroupRange { start: 60, end: 70 }));
    assert!(!tree.searching());
    let want: BTreeSet<String> = (0..2000)
        .map(|i| format!("user-{:05}", i))
        .filter(|k| (60..=70).contains(&synth::key_group_for_hash(synth::java_string_hash(k), 128)))
        .collect();
    let got = walk_keys(&mut tree, 3000);
    assert_eq!(
        got, want,
        "keys of key-groups 60-70 (both subtasks: 63/64 is the split)"
    );
}

fn ctrl_c() -> KeyEvent {
    KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
}

fn press(app: &mut App, code: KeyCode) {
    app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
}

/// Ctrl-C while the background count runs quits and stops the worker (joined, not left
/// running); `C` stops and restarts it.
#[test]
fn ctrl_c_stops_the_count() {
    let dir = write(operator(20_000, 4096, Ser::String), "ctrl-c-count");
    let options = AppOptions {
        cache_dir: None,
        count_keys: true,
        count_step_bytes: 1, // one key-group per step: thousands of steps
        search_step_bytes: 0,
    };
    let mut app = App::open(dir.to_path_buf(), options.clone(), |_| {}).unwrap();
    assert!(app.counting());
    app.handle_key(ctrl_c());
    assert!(app.should_quit());
    assert!(!app.counting(), "the count worker still runs after Ctrl-C");
    assert!(
        !matches!(app.tree().key_count(0), Some(KeyCount::Exact(20_000))),
        "it was stopped, not finished"
    );

    // `C` toggles it.
    let mut app = App::open(dir.to_path_buf(), options, |_| {}).unwrap();
    app.handle_key(KeyEvent::new(KeyCode::Char('C'), KeyModifiers::SHIFT));
    assert!(!app.counting());
    assert!(matches!(
        app.tree().key_count(0),
        Some(KeyCount::AtLeast(_) | KeyCount::Unknown)
    ));
    app.handle_key(KeyEvent::new(KeyCode::Char('C'), KeyModifiers::SHIFT));
    app.wait_for_count();
    assert_eq!(app.tree().key_count(0), Some(KeyCount::Exact(20_000)));
}

/// Ctrl-C during a search quits and ends it (its worker joined); `Esc` ends it and keeps what it
/// found. The worker reports one block at a time and cannot run more than a few reports ahead of
/// the event loop, so the search is still going on when the keys arrive.
#[test]
fn ctrl_c_and_esc_stop_a_search() {
    let dir = write(operator(5000, 4096, Ser::String), "ctrl-c-scan");
    let options = AppOptions {
        search_step_bytes: 1,
        ..Default::default()
    };
    let open = || App::open(dir.to_path_buf(), options.clone(), |_| {}).unwrap();
    let type_filter = |app: &mut App| {
        press(app, KeyCode::Char('/'));
        for c in "*9*".chars() {
            press(app, KeyCode::Char(c));
        }
        press(app, KeyCode::Enter);
    };

    let mut app = open();
    type_filter(&mut app);
    assert!(app.tree().searching());
    assert!(app.tick(), "a few reports do not cover 4096 key-groups");
    app.handle_key(ctrl_c());
    assert!(app.should_quit());
    assert!(!app.tree().searching());

    let mut app = open();
    type_filter(&mut app);
    app.tick();
    press(&mut app, KeyCode::Esc);
    assert!(!app.should_quit());
    assert!(!app.tree().searching(), "Esc stops the search");
    // The filter is still applied, its count a lower bound; a second Esc clears it.
    let mut terminal = Terminal::new(TestBackend::new(140, 20)).unwrap();
    terminal.draw(|f| app.draw(f)).unwrap();
    let screen = common::tui::screen(&terminal).join("\n");
    assert!(screen.contains("filter active"), "{}", screen);
    press(&mut app, KeyCode::Esc);
    terminal.draw(|f| app.draw(f)).unwrap();
    let screen = common::tui::screen(&terminal).join("\n");
    assert!(!screen.contains("filter active"), "{}", screen);
}

/// Opening, paging, filtering and counting write nothing into the savepoint directory; the
/// counts go to the cache directory, and a second run uses them instead of counting again.
#[test]
fn nothing_is_written_into_the_savepoint_directory() {
    let dir = write(operator(4000, 128, Ser::String), "no-write");
    let cache = synth::temp_dir("no-write-cache");
    let before = listing(&dir);
    let options = AppOptions {
        cache_dir: Some(cache.to_path_buf()),
        count_keys: true,
        count_step_bytes: 0,
        search_step_bytes: 0,
    };

    let mut app = App::open(dir.to_path_buf(), options.clone(), |_| {}).unwrap();
    app.wait_for_count();
    assert_eq!(app.tree().key_count(0), Some(KeyCount::Exact(4000)));
    let tree = app.tree();
    tree.jump_first();
    tree.expand();
    for _ in 0..20 {
        tree.page_down();
    }
    tree.apply_filter(&Some(AppliedFilter::Pattern("*12*".to_string())));
    tree.finish_search();
    drop(app);
    assert_eq!(listing(&dir), before, "the savepoint directory changed");

    // The counts file holds counts only, under the cache directory.
    let files: Vec<PathBuf> = listing(&cache).into_iter().map(|(p, _, _)| p).collect();
    assert_eq!(files.len(), 1, "{:?}", files);
    let text = std::fs::read_to_string(&files[0]).unwrap();
    assert!(text.lines().last().unwrap().ends_with(" 4000"), "{}", text);
    assert!(
        !text.contains("user-"),
        "a key in the counts cache:\n{}",
        text
    );

    // Next time the count comes from the cache: no pass runs.
    let mut app = App::open(dir.to_path_buf(), options.clone(), |_| {}).unwrap();
    assert!(!app.counting());
    assert_eq!(app.tree().key_count(0), Some(KeyCount::Exact(4000)));
    drop(app);

    // A changed `_metadata` makes it stale (decided from its header): counted again.
    let meta = dir.join("_metadata");
    let f = std::fs::OpenOptions::new().write(true).open(&meta).unwrap();
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .unwrap();
    drop(f);
    assert!(flink_explorer::tui::counts::load(&cache, &dir).is_none());
    let mut app = App::open(dir.to_path_buf(), options, |_| {}).unwrap();
    app.wait_for_count();
    assert_eq!(app.tree().key_count(0), Some(KeyCount::Exact(4000)));
    assert!(
        flink_explorer::tui::counts::load(&cache, &dir).is_some(),
        "counted again, and cached again"
    );
}

/// `--no-cache` disables the counts cache; without it, the cache lives under the user cache
/// directory from the environment (never next to the savepoint).
#[test]
fn the_cache_directory_comes_from_the_environment() {
    assert!(AppOptions::from_env(true).cache_dir.is_none());
    if cfg!(unix) {
        let base = synth::temp_dir("xdg");
        std::env::set_var("XDG_CACHE_HOME", &*base);
        assert_eq!(
            flink_explorer::tui::counts::default_cache_dir(),
            Some(base.join("flink-explorer"))
        );
        assert_eq!(
            AppOptions::from_env(false).cache_dir,
            Some(base.join("flink-explorer"))
        );
        std::env::remove_var("XDG_CACHE_HOME");
        if let Some(home) = std::env::var_os("HOME") {
            assert_eq!(
                flink_explorer::tui::counts::default_cache_dir(),
                Some(PathBuf::from(home).join(".cache").join("flink-explorer"))
            );
        }
    }
}

/// The search runs on a worker thread: `apply_filter` returns before it is done, `poll_search`
/// applies what was reported without waiting, and the worker, which may run only a few reports
/// ahead, is stopped and joined by `cancel_search` even while it waits on a full channel.
#[test]
fn the_search_runs_on_a_worker_thread() {
    let dir = write(operator(5000, 4096, Ser::String), "search-worker");
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    tree.set_search_step_bytes(1); // a report per block: thousands of them
    tree.apply_filter(&Some(AppliedFilter::Pattern("*9*".to_string())));
    assert!(tree.searching());
    // The worker fills its channel and waits; polling takes what is there and returns.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let t = std::time::Instant::now();
    assert!(
        tree.poll_search(),
        "a few reports do not cover 4096 key-groups"
    );
    assert!(t.elapsed() < std::time::Duration::from_secs(1));
    tree.cancel_search();
    assert!(!tree.searching());

    // Left to run, it finds every match.
    tree.apply_filter(&Some(AppliedFilter::Pattern("*9*".to_string())));
    while tree.searching() {
        tree.poll_search();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let want = (0..5000)
        .map(|i| format!("user-{:05}", i))
        .filter(|k| k.contains('9'))
        .count();
    assert!(
        operator_row(&mut tree).contains(&format!("({}/", want)),
        "{}",
        operator_row(&mut tree)
    );
}

/// A cancelled job is joined even when its worker is blocked sending on a full bounded channel
/// (receiving is what frees it), and what it sent meanwhile is still drained.
#[test]
fn a_job_blocked_on_a_full_channel_is_cancelled() {
    use flink_explorer::tui::jobs::Job;
    use std::sync::atomic::Ordering;
    let mut job: Job<usize> = Job::spawn_bounded(1, |tx, cancel| {
        for i in 0..1_000_000 {
            if cancel.load(Ordering::Relaxed) || tx.send(i).is_err() {
                return;
            }
        }
    });
    std::thread::sleep(std::time::Duration::from_millis(20));
    job.cancel();
    assert!(!job.is_running());
    let got = job.drain();
    assert!(!got.is_empty() && got.len() < 1_000_000, "{}", got.len());
    assert_eq!(got[0], 0);
}

/// The counts cache is written through a side file of its own — named after the process and a
/// counter, so two writers never share one — synced, then renamed; none is left behind.
#[test]
fn the_counts_cache_is_written_through_a_unique_side_file() {
    use flink_explorer::tui::counts;
    let dir = write(operator(10, 128, Ser::String), "counts-tmp");
    let cache = synth::temp_dir("counts-tmp-cache");
    let path = counts::cache_file(&cache, &dir);
    let (a, b) = (counts::temp_file(&path), counts::temp_file(&path));
    assert_ne!(a, b);
    let name = a.file_name().unwrap().to_string_lossy().into_owned();
    assert!(
        name.contains(&format!(".{}-", std::process::id())),
        "{}",
        name
    );
    assert!(name.ends_with(".partial"));
    for n in [10, 11] {
        let saved = counts::save(&cache, &dir, &[("2c2c".to_string(), n)]).unwrap();
        assert_eq!(saved, path);
        assert_eq!(counts::load(&cache, &dir).unwrap()["2c2c"], n);
    }
    let files: Vec<_> = std::fs::read_dir(&cache)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(files, [path.file_name().unwrap().to_owned()], "{:?}", files);
}
