//! The TUI reaches every value's bytes and every operator-state element (#19), fixture-free:
//! `x` (or Enter on a leaf) opens the hex overlay on a keyed value or an operator-state element
//! with its real bytes; operator-state elements page past the first 500 like keyed partitions,
//! with their real index under a filter; and pager rows are told apart by what they are, so a
//! label holding `↓` or `↑` does not trap the cursor.

mod common;

use common::synth::{self, Item, KeyedOperator, Kind, OpState, Ser, Spec, Storage, Val};
use common::tui::{draw, labels};
use flink_explorer::index::builder::build_index;
use flink_explorer::index::cache::IndexCache;
use flink_explorer::parser;
use flink_explorer::tui::detail::DetailOverlay;
use flink_explorer::tui::filter::AppliedFilter;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::Terminal;

const ARROW_KEYS: &[&str] = &["alpha", "  ↓ 1-30 / 99", "↑ 2-31 / 99", "↓"];

fn element(i: usize) -> Val {
    Val::Str(format!("e-{:04}", i))
}

fn savepoint() -> (synth::TempDir, IndexCache) {
    let mut keyed = KeyedOperator::new(0x31, 1, 128, Ser::String)
        .state("count", Kind::Value(Ser::Long))
        .state("m", Kind::Map(Ser::String, Ser::Long))
        .op_state(
            0,
            OpState::list(
                "arrows",
                Ser::String,
                vec![Val::str("    ↓ 1-500 / 1200"), Val::str("↑"), Val::str("x")],
            ),
        );
    for (i, k) in ARROW_KEYS.iter().enumerate() {
        keyed.items.push(Item::Value {
            state: 0,
            key: Val::str(k),
            value: Val::Long(7 + i as i64),
        });
        keyed.items.push(Item::MapEntry {
            state: 1,
            key: Val::str(k),
            map_key: Val::str("a"),
            value: Some(Val::Long(1)),
        });
    }
    let big = KeyedOperator::new(0x32, 1, 128, Ser::String).op_state(
        0,
        OpState::list("big", Ser::String, (0..1200).map(element).collect()),
    );
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(keyed);
    spec.operators.push(big);
    let dir = synth::temp_dir("tui-raw-bytes");
    synth::write_savepoint(&spec, &dir);
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    let index = build_index(&savepoint, 0, 0, &dir);
    (dir, index)
}

/// Move down until the selected row's path satisfies `want`; panics after `limit` moves.
fn goto(tree: &mut TreeView, limit: usize, want: impl Fn(&str) -> bool) {
    for _ in 0..limit {
        if want(tree.selected_path()) {
            return;
        }
        tree.move_down();
    }
    panic!("no row found; stopped at {:?}", tree.selected_path());
}

fn rows(tree: &mut TreeView, width: u16, height: u16) -> Vec<String> {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    labels(&draw(&mut terminal, tree))
}

#[test]
fn the_overlay_opens_on_a_keyed_value_with_its_bytes() {
    let (_dir, index) = savepoint();
    let ks = index.operators[0].keyed_state.as_ref().unwrap();
    let part = ks.partitions.iter().find(|p| p.key == "alpha").unwrap();
    let mut tree = TreeView::new(&index);
    tree.jump_first();
    tree.expand(); // the keyed operator
    goto(&mut tree, 20, |p| p.ends_with("> alpha"));
    tree.expand();
    goto(&mut tree, 5, |p| p.ends_with("> alpha > count"));

    let raw = tree
        .selected_raw_bytes()
        .expect("bytes behind a keyed value");
    assert_eq!(raw.value, 7i64.to_be_bytes());
    assert_eq!(raw.value, part.values[0].clone().unwrap());
    assert_eq!(raw.key, b"alpha");
    // Enter on the leaf opens the same thing.
    assert_eq!(tree.expand(), Some(raw.clone()));

    let mut overlay = DetailOverlay::new();
    overlay.open_raw(raw);
    let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
    terminal.draw(|f| overlay.render(f, f.area())).unwrap();
    let screen = common::tui::screen(&terminal).join("\n");
    assert!(screen.contains("count [Long] (8 bytes)"), "{}", screen);
    assert!(
        screen.contains("00000000  00 00 00 00 00 00 00 07"),
        "{}",
        screen
    );

    // A MAP value: the per-key blob the index builds, and said to be one.
    tree.move_down();
    assert!(tree.selected_path().ends_with("> alpha > m"));
    let raw = tree.selected_raw_bytes().unwrap();
    assert_eq!(raw.value, part.values[1].clone().unwrap());
    assert!(
        raw.value_title.contains("per-key blob"),
        "{}",
        raw.value_title
    );
}

#[test]
fn an_operator_state_of_1200_elements_pages_to_the_last_one() {
    let (_dir, index) = savepoint();
    let mut tree = TreeView::new(&index);
    tree.jump_first();
    goto(&mut tree, 20, |p| !p.contains('>') && p.contains("3232"));
    tree.expand(); // the operator holding `big`
    goto(&mut tree, 5, |p| p.ends_with("[internal] big"));
    tree.expand();
    let shown = rows(&mut tree, 120, 600).join("\n");
    assert!(shown.contains("[internal] big [operator/SPLIT_DISTRIBUTE] (1200 entries)"));
    assert!(shown.contains("    ↓ 1-500 / 1200"), "{}", shown);

    goto(&mut tree, 2000, |p| p.ends_with("big[1199]"));
    let shown = rows(&mut tree, 120, 600);
    assert!(shown.iter().any(|r| r == "    • [1199] \"e-1199\""));
    assert!(shown.iter().any(|r| r == "    ↑ 701-1200 / 1200"));
    let raw = tree.selected_raw_bytes().expect("an element's bytes");
    assert_eq!(raw.value, element(1199).encode());
    assert_eq!(raw.key, b"\"e-1199\"");

    // Under a filter, an element keeps its real index, and its bytes.
    tree.apply_filter(&Some(AppliedFilter::Pattern("*119*".to_string())));
    let shown = rows(&mut tree, 120, 60);
    assert!(
        shown
            .iter()
            .any(|r| r.contains("big [operator/SPLIT_DISTRIBUTE] (12/1200 entries)")),
        "{}",
        shown.join("\n")
    );
    for i in [119, 1119, 1190, 1199] {
        let want = format!("    • [{}] \"e-{:04}\"", i, i);
        assert!(shown.contains(&want), "{} in\n{}", want, shown.join("\n"));
    }
    tree.jump_first();
    goto(&mut tree, 100, |p| p.ends_with("big[1195]"));
    assert_eq!(
        tree.selected_raw_bytes().unwrap().value,
        element(1195).encode()
    );
}

#[test]
fn a_label_with_an_arrow_does_not_trap_the_cursor() {
    let (_dir, mut index) = savepoint();
    index.operators.truncate(1); // the keys and elements with arrows, all on one page
    let mut tree = TreeView::new(&index);
    // Expand everything.
    common::tui::expand_all(&mut tree);
    let n = tree.row_count();
    let shown = rows(&mut tree, 120, n as u16 + 2);
    assert!(shown
        .iter()
        .any(|r| r.contains("⯈   ↓ 1-30 / 99 (2 states)")));
    assert!(shown
        .iter()
        .any(|r| r.contains("• [0] \"    ↓ 1-500 / 1200\"")));

    // Down from the top to the last row, one row per move, then back up.
    tree.jump_first();
    let mut paths = vec![tree.selected_path().to_string()];
    for _ in 1..n {
        tree.move_down();
        paths.push(tree.selected_path().to_string());
    }
    let shown_after = rows(&mut tree, 120, n as u16 + 2);
    assert_eq!(shown_after, shown, "moving down changed the tree");
    assert!(
        paths.last().unwrap().ends_with("arrows[2]"),
        "stopped at {:?}",
        paths.last()
    );
    for i in (0..n - 1).rev() {
        tree.move_up();
        assert_eq!(tree.selected_path(), paths[i]);
    }
}
