//! The index keeps at most `MAX_STORED_PARTITIONS` keys per operator (#13). These fixture-free
//! tests check that the TUI pages only through the keys it holds and says how many it holds, that
//! the kept keys come from every subtask, and that the `E` export says when it is truncated.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use flink_explorer::index::builder::{build_index, build_index_with_cap};
use flink_explorer::index::cache::{
    CachedKeyedState, CachedOperator, IndexCache, KeyPartition, StateDescriptor,
};
use flink_explorer::parser;
use flink_explorer::tui::filter::AppliedFilter;
use flink_explorer::tui::text;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::widgets::{Block, Borders};
use ratatui::Terminal;
use serde_json::Value;

/// The screen as text, one string per row (the cells a wide character hides are skipped).
fn screen(terminal: &Terminal<TestBackend>) -> Vec<String> {
    let buf = terminal.backend().buffer();
    (0..buf.area.height)
        .map(|y| {
            let mut row = String::new();
            let mut hidden = 0;
            for x in 0..buf.area.width {
                if hidden > 0 {
                    hidden -= 1;
                    continue;
                }
                let symbol = buf[(x, y)].symbol();
                row.push_str(symbol);
                hidden = text::width(symbol).saturating_sub(1);
            }
            row.trim_end().to_string()
        })
        .collect()
}

fn draw(terminal: &mut Terminal<TestBackend>, tree: &mut TreeView) -> Vec<String> {
    terminal
        .draw(|f| tree.render(f, Block::default().borders(Borders::ALL), f.area()))
        .unwrap();
    screen(terminal)
}

/// An index whose one operator holds `stored` keys out of `total`: what the builder produces
/// past the cap, without writing 100 000+ keys.
fn capped_index(stored: usize, total: usize) -> IndexCache {
    let partitions = (0..stored)
        .map(|i| KeyPartition {
            key: format!("key-{:04}", i),
            key_group: (i % 128) as u16,
            values: vec![Some((i as i64).to_be_bytes().to_vec())],
        })
        .collect();
    let keyed = CachedKeyedState {
        descriptors: vec![StateDescriptor {
            name: "counter".to_string(),
            state_type: "VALUE".to_string(),
            value_type: "Long".to_string(),
            pojo_detail: String::new(),
            entry_count: total,
            pojo_info: None,
        }],
        partitions,
        total_keys: total,
        key_type: "String".to_string(),
    };
    index_of(keyed)
}

fn index_of(keyed: CachedKeyedState) -> IndexCache {
    IndexCache {
        metadata_size: 0,
        metadata_mtime: 0,
        checkpoint_id: 7,
        savepoint_version: 4,
        master_states: Vec::new(),
        operators: vec![CachedOperator {
            id: [0xab; 16],
            display_name: "keyed-op".to_string(),
            identifier_name: String::new(),
            parallelism: 2,
            max_parallelism: 128,
            subtask_count: 2,
            keyed_state_size: 0,
            fully_finished: false,
            has_coordinator_state: false,
            coordinator_info: None,
            states: Vec::new(),
            keyed_state: Some(keyed),
        }],
        cache_format: flink_explorer::index::cache::CACHE_FORMAT,
    }
}

fn row<'a>(rows: &'a [String], needle: &str) -> &'a str {
    rows.iter()
        .find(|r| r.contains(needle))
        .unwrap_or_else(|| panic!("no row contains {:?}:\n{}", needle, rows.join("\n")))
}

#[test]
fn paging_past_the_stored_keys_reaches_the_end_without_panicking() {
    let index = capped_index(100, 1000);
    let mut tree = TreeView::new(&index);
    let mut terminal = Terminal::new(TestBackend::new(110, 40)).unwrap();

    let rows = draw(&mut terminal, &mut tree);
    assert!(
        row(&rows, "keyed-op").contains("(100 of 1000 keys shown, 1 states)"),
        "{}",
        row(&rows, "keyed-op")
    );

    tree.expand(); // the operator
    let rows = draw(&mut terminal, &mut tree);
    assert!(row(&rows, "↓ 1-30 / 100").contains("(100 of 1000 keys shown)"));

    // Hold ↓ well past the 100 stored keys: this used to index `partitions[100]` and panic
    // (in `move_down` itself), since paging was bounded by the savepoint's 1000 keys.
    for i in 0..1100 {
        tree.move_down();
        if i % 25 == 0 {
            draw(&mut terminal, &mut tree);
        }
    }
    let rows = draw(&mut terminal, &mut tree);
    assert!(row(&rows, "↑ 71-100 / 100").contains("(100 of 1000 keys shown)"));
    row(&rows, "key-0099");
    assert!(!rows.iter().any(|r| r.contains("key-0100")));
    assert!(
        !rows.iter().any(|r| r.contains('↓')),
        "no page after the last stored key"
    );
    // The end of the stored keys says it is not the end of the operator's keys.
    assert!(row(&rows, "900 more keys not indexed").contains("--export-state"));

    // And back up to the first key.
    for i in 0..1100 {
        tree.move_up();
        if i % 25 == 0 {
            draw(&mut terminal, &mut tree);
        }
    }
    let rows = draw(&mut terminal, &mut tree);
    row(&rows, "key-0000");
    assert_eq!(tree.selected_path(), "keyed-op");
}

#[test]
fn a_filter_says_it_searched_only_the_stored_keys() {
    let index = capped_index(100, 1000);
    let mut tree = TreeView::new(&index);
    let mut terminal = Terminal::new(TestBackend::new(110, 40)).unwrap();
    tree.expand();

    tree.apply_filter(&Some(AppliedFilter::Pattern("key-00*".to_string())));
    let rows = draw(&mut terminal, &mut tree);
    assert!(
        row(&rows, "keyed-op").contains("(100 match in 100 of 1000 keys shown, 1 states)"),
        "{}",
        row(&rows, "keyed-op")
    );
    assert!(row(&rows, "↓ 1-30 / 100").contains("(filter searched 100 of 1000 keys)"));

    tree.apply_filter(&Some(AppliedFilter::Pattern("key-009*".to_string())));
    let rows = draw(&mut terminal, &mut tree);
    assert!(row(&rows, "keyed-op").contains("(10 match in 100 of 1000 keys shown, 1 states)"));
    for _ in 0..50 {
        tree.move_down();
        draw(&mut terminal, &mut tree);
    }
    let rows = draw(&mut terminal, &mut tree);
    row(&rows, "key-0099");
    row(
        &rows,
        "900 more keys not indexed (not shown, not searched by /)",
    );
}

#[test]
fn below_the_cap_the_counts_read_as_before() {
    let index = capped_index(40, 40);
    let mut tree = TreeView::new(&index);
    let mut terminal = Terminal::new(TestBackend::new(110, 40)).unwrap();
    let rows = draw(&mut terminal, &mut tree);
    assert!(row(&rows, "keyed-op").contains("(40 keys, 1 states)"));
    tree.expand();
    let rows = draw(&mut terminal, &mut tree);
    assert!(!row(&rows, "↓ 1-30 / 40").contains("keys shown"));
    assert!(!rows.iter().any(|r| r.contains("not indexed")));
    tree.apply_filter(&Some(AppliedFilter::Pattern("key-001*".to_string())));
    let rows = draw(&mut terminal, &mut tree);
    assert!(row(&rows, "keyed-op").contains("(10/40 keys, 1 states)"));
}

#[test]
fn a_key_containing_an_arrow_is_not_taken_for_the_pager() {
    let mut keyed = capped_index(3, 3).operators.remove(0).keyed_state.unwrap();
    keyed.partitions[1].key = "b ↓ c".to_string();
    keyed.partitions[2].key = "↑ d".to_string();
    let index = index_of(keyed);
    let mut tree = TreeView::new(&index);
    tree.expand();
    let mut path = Vec::new();
    for _ in 0..3 {
        tree.move_down();
        path.push(tree.selected_path().to_string());
    }
    assert_eq!(
        path,
        ["keyed-op > key-0000", "keyed-op > b ↓ c", "keyed-op > ↑ d"]
    );
    tree.move_up();
    assert_eq!(tree.selected_path(), "keyed-op > b ↓ c");
}

/// One VALUE state over `keys` String keys, `parallelism` subtasks, maxParallelism 128.
fn operator(keys: usize, parallelism: i32) -> KeyedOperator {
    let mut op = KeyedOperator::new(0x2a, parallelism, 128, Ser::String)
        .state("counter", Kind::Value(Ser::Long));
    for i in 0..keys {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Str(format!("key-{:05}", i)),
            value: Val::Long(i as i64 * 3 + 1),
        });
    }
    op
}

fn write(op: &KeyedOperator, tag: &str) -> std::path::PathBuf {
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

#[test]
fn stored_keys_come_from_every_subtask() {
    for (keys, parallelism, cap) in [(400, 2, 50), (300, 3, 31), (500, 4, 7)] {
        let op = operator(keys, parallelism);
        let dir = write(&op, "cap-spread");
        let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
        let index = build_index_with_cap(&savepoint, 0, 0, &dir, cap);
        let ks = index.operators[0].keyed_state.as_ref().unwrap();

        assert_eq!(ks.total_keys, keys, "every key is still counted");
        assert_eq!(ks.stored_keys(), cap);
        assert!(ks.is_truncated());

        // Every subtask holds keys here, so every subtask must be represented, in equal
        // shares (each holds far more than `cap / parallelism` keys).
        let mut per_subtask: BTreeMap<i32, Vec<&str>> = BTreeMap::new();
        for p in &ks.partitions {
            per_subtask
                .entry(subtask_of(&p.key, &op))
                .or_default()
                .push(&p.key);
        }
        assert_eq!(
            per_subtask.keys().copied().collect::<Vec<_>>(),
            (0..parallelism).collect::<Vec<_>>(),
            "{} keys over {} subtasks, cap {}: kept from {:?}",
            keys,
            parallelism,
            cap,
            per_subtask.keys()
        );
        let share = cap / parallelism as usize;
        for (s, kept) in &per_subtask {
            assert!(
                kept.len() == share || kept.len() == share + 1,
                "subtask {} kept {} of a share of {}",
                s,
                kept.len(),
                share
            );
            // Within a subtask, its smallest keys, as before.
            let all: BTreeSet<String> = (0..keys)
                .map(|i| format!("key-{:05}", i))
                .filter(|k| subtask_of(k, &op) == *s)
                .collect();
            let smallest: Vec<&str> = all.iter().take(kept.len()).map(String::as_str).collect();
            assert_eq!(kept, &smallest, "subtask {}", s);
        }

        // Keys and values still pair up.
        for p in &ks.partitions {
            let i: i64 = p.key["key-".len()..].parse().unwrap();
            let raw = p.values[0].as_deref().unwrap();
            assert_eq!(raw, (i * 3 + 1).to_be_bytes());
        }

        // And the TUI says how many of them it shows.
        let mut tree = TreeView::new(&index);
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let rows = draw(&mut terminal, &mut tree);
        assert!(row(&rows, "2a2a2a").contains(&format!("({} of {} keys shown", cap, keys)));
    }
}

#[test]
fn a_small_or_empty_subtask_leaves_its_share_to_the_others() {
    // Subtask 0 holds 100 keys, subtask 1 none, subtask 2 only 2: with a cap of 20, the last
    // subtask keeps both of its keys (filling from subtask 0 kept none of them) and subtask 0
    // the other 18.
    let probe = operator(0, 3);
    let mut picked: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    for i in 0.. {
        let key = format!("key-{:05}", i);
        let s = subtask_of(&key, &probe);
        let want = [100, 0, 2][s as usize];
        let got = picked.entry(s).or_default();
        if got.len() < want {
            got.push(key);
        }
        if picked.get(&0).map_or(0, Vec::len) == 100 && picked.get(&2).map_or(0, Vec::len) == 2 {
            break;
        }
    }
    let mut op =
        KeyedOperator::new(0x2a, 3, 128, Ser::String).state("counter", Kind::Value(Ser::Long));
    for key in picked.values().flatten() {
        op.items.push(Item::Value {
            state: 0,
            key: Val::Str(key.clone()),
            value: Val::Long(1),
        });
    }
    let dir = write(&op, "cap-uneven");
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let index = build_index_with_cap(&savepoint, 0, 0, &dir, 20);
    let ks = index.operators[0].keyed_state.as_ref().unwrap();
    assert_eq!((ks.total_keys, ks.stored_keys()), (102, 20));
    let mut per_subtask: BTreeMap<i32, usize> = BTreeMap::new();
    for p in &ks.partitions {
        *per_subtask.entry(subtask_of(&p.key, &op)).or_default() += 1;
    }
    assert_eq!(per_subtask, BTreeMap::from([(0, 18), (2, 2)]));
}

fn export(index: &IndexCache, tag: &str) -> Value {
    let path = std::env::temp_dir().join(format!(
        "flink-explorer-cap-{}-{}.json",
        tag,
        std::process::id()
    ));
    let count = flink_explorer::export::export_json(index, None, &path).unwrap();
    let root: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    std::fs::remove_file(&path).ok();
    let keys = root["operators"][0]["keys"].as_array().unwrap().len();
    assert_eq!(count, keys);
    root
}

#[test]
fn export_json_says_when_it_is_truncated() {
    let op = operator(400, 2);
    let dir = write(&op, "cap-export");
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();

    let capped = export(&build_index_with_cap(&savepoint, 0, 0, &dir, 50), "capped");
    let opj = &capped["operators"][0];
    assert_eq!(capped["truncated"], Value::Bool(true));
    assert_eq!(opj["truncated"], Value::Bool(true));
    assert_eq!(opj["stored_keys"], 50);
    assert_eq!(opj["total_keys"], 400);
    assert_eq!(opj["keys"].as_array().unwrap().len(), 50);
    assert_eq!(
        opj["info"]["Keys"],
        "400 (50 shown; --export-state reads them all)"
    );

    let full = export(&build_index(&savepoint, 0, 0, &dir), "full");
    let opj = &full["operators"][0];
    assert_eq!(full["truncated"], Value::Bool(false));
    assert_eq!(opj["truncated"], Value::Bool(false));
    assert_eq!(opj["stored_keys"], 400);
    assert_eq!(opj["total_keys"], 400);
    assert_eq!(opj["info"]["Keys"], "400");
}
