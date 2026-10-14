//! The TUI reaches every value's bytes and every operator-state element (#19), fixture-free:
//! `x` (or Enter on a leaf) opens the hex overlay on a keyed value (the entry's bytes as the
//! reader returns them; under a MAP, the entry the row shows) or an operator-state element with
//! its real bytes; operator-state elements page past the first 500 like keyed partitions,
//! with their real index under a filter; and pager rows are told apart by what they are, so a
//! label holding `↓` or `↑` does not trap the cursor.

mod common;

use common::synth::{self, Item, KeyedOperator, Kind, OpState, Ser, Spec, Storage, Val};
use common::tui::{draw, labels, open_tree};
use flink_explorer::tui::detail::DetailOverlay;
use flink_explorer::tui::filter::AppliedFilter;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::Terminal;

const ARROW_KEYS: &[&str] = &["alpha", "  ↓ 1-30 / 99", "↑ 2-31 / 99", "↓"];

fn element(i: usize) -> Val {
    Val::Str(format!("e-{:04}", i))
}

fn savepoint() -> synth::TempDir {
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
    dir
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
    let dir = savepoint();
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand(); // the keyed operator
    goto(&mut tree, 20, |p| p.ends_with("> alpha"));
    tree.expand();
    goto(&mut tree, 5, |p| p.ends_with("> alpha > count"));

    let raw = tree
        .selected_raw_bytes()
        .expect("bytes behind a keyed value");
    assert_eq!(raw.value, 7i64.to_be_bytes());
    assert_eq!(raw.key, b"alpha");
    // Enter on the leaf expands nothing: the app opens these bytes instead.
    assert!(!tree.expand());
    assert_eq!(tree.selected_raw_bytes(), Some(raw.clone()));

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

    // A MAP value: one entry of it, as stored — the value behind its `NullableSerializer` byte,
    // the map key in the namespace (after the `VoidNamespace` byte).
    tree.move_down();
    assert!(tree.selected_path().ends_with("> alpha > m"));
    let raw = tree.selected_raw_bytes().unwrap();
    let mut want = vec![0u8];
    want.extend_from_slice(&1i64.to_be_bytes());
    assert_eq!(raw.value, want);
    assert_eq!(raw.value_title, "m [Map<String,Long>]: entry 1 of 1");
    assert_eq!(raw.key_title, "m[a]: namespace + map key");
    assert_eq!(raw.key[0], 0, "VoidNamespace");
    assert_eq!(&raw.key[1..], Val::str("a").encode());
    // The same bytes from the map entry's own row.
    tree.expand();
    tree.move_down();
    assert!(
        tree.selected_path().ends_with("> alpha > m[a]"),
        "{}",
        tree.selected_path()
    );
    assert_eq!(tree.selected_raw_bytes(), Some(raw.clone()));
    // The overlay's title fits on an 80-column terminal.
    let mut overlay = DetailOverlay::new();
    overlay.open_raw(raw);
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal.draw(|f| overlay.render(f, f.area())).unwrap();
    let screen = common::tui::screen(&terminal).join("\n");
    assert!(
        screen.contains("m [Map<String,Long>]: entry 1 of 1"),
        "{}",
        screen
    );
}

/// `x` on an operator-state element gives the element's own bytes.
#[test]
fn operator_state_bytes_are_the_elements() {
    let dir = savepoint();
    let mut tree = open_tree(&dir);
    tree.jump_first();
    goto(&mut tree, 20, |p| !p.contains('>') && p.contains("3232"));
    tree.expand();
    goto(&mut tree, 5, |p| p.ends_with("[internal] big"));
    tree.expand();
    tree.move_down();
    assert!(tree.selected_path().ends_with("big[0]"));
    let raw = tree.selected_raw_bytes().unwrap();
    assert_eq!(raw.value, element(0).encode());
    assert_eq!(raw.value_title, "element big[0]");
}

/// Open the `big` operator state (1200 elements, a window of 500) and return the tree, its
/// cursor on the state's row.
fn open_big(dir: &std::path::Path) -> TreeView {
    let mut tree = open_tree(dir);
    tree.jump_first();
    goto(&mut tree, 20, |p| !p.contains('>') && p.contains("3232"));
    tree.expand();
    goto(&mut tree, 5, |p| p.ends_with("[internal] big"));
    tree.expand();
    tree
}

/// The index of the selected element of `big`, from its path (`…big[i]`).
fn element_at(tree: &TreeView) -> usize {
    let path = tree.selected_path();
    let i = path
        .rsplit_once("big[")
        .unwrap_or_else(|| panic!("not on an element: {}", path))
        .1;
    i.trim_end_matches(']').parse().unwrap()
}

#[test]
fn page_down_and_up_move_a_page_through_the_elements() {
    let dir = savepoint();
    let mut tree = open_big(&dir);
    // Rendered 22 rows high: 20 rows inside the borders, a page.
    rows(&mut tree, 120, 22);
    tree.page_down();
    assert_eq!(element_at(&tree), 19);
    // Past the window's 500 rows, PageDown scrolls it by the rest of the page.
    let mut presses = 1;
    while element_at(&tree) < 1199 {
        tree.page_down();
        presses += 1;
        assert!(presses < 100, "stuck at {}", tree.selected_path());
    }
    // 1199 elements, a page of 20 a press (one step lost to the `↑` row appearing), not one
    // per press as with `↓` (~700 presses past the first window).
    assert_eq!(presses, 61);
    tree.page_down(); // at the end: stays on the last element
    assert_eq!(element_at(&tree), 1199);
    let shown = rows(&mut tree, 120, 600);
    assert!(shown.iter().any(|r| r == "    ↑ 701-1200 / 1200"));

    rows(&mut tree, 120, 22); // a page is the height of the last render: 20 rows again
    tree.page_up();
    assert_eq!(element_at(&tree), 1179);
    let mut presses = 1;
    while element_at(&tree) > 0 {
        tree.page_up();
        presses += 1;
        assert!(presses < 100, "stuck at {}", tree.selected_path());
    }
    assert_eq!(presses, 60);
    let shown = rows(&mut tree, 120, 600);
    assert!(shown.iter().any(|r| r == "    ↓ 1-500 / 1200"));
    rows(&mut tree, 120, 22);
    // From the first element, fewer than a page of rows lie above: PageUp stops at the top.
    tree.page_up();
    let top = tree.selected_path().to_string();
    tree.jump_first();
    assert_eq!(tree.selected_path(), top);
}

#[test]
fn page_down_scrolls_the_keyed_partitions() {
    let mut keyed =
        KeyedOperator::new(0x41, 1, 128, Ser::String).state("v", Kind::Value(Ser::Long));
    for i in 0..99 {
        keyed.items.push(Item::Value {
            state: 0,
            key: Val::Str(format!("k{:02}", i)),
            value: Val::Long(i),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(keyed);
    let dir = synth::temp_dir("tui-page-keys");
    synth::write_savepoint(&spec, &dir);
    let mut tree = open_tree(&dir);
    tree.jump_first();
    tree.expand();
    // The last key in the reader's order (key-groups ascending, on-disk order inside one).
    let keyed = common::keyed::operators(&dir).remove(0).1.unwrap();
    assert_eq!(keyed.keys.len(), 99);
    let last_key = keyed.keys[98].key.clone();
    rows(&mut tree, 120, 12); // a page of 10 rows
    let mut presses = 0;
    while !tree.selected_path().ends_with(&format!("> {}", last_key)) {
        tree.page_down();
        presses += 1;
        assert!(presses < 30, "stuck at {}", tree.selected_path());
    }
    let shown = rows(&mut tree, 120, 200);
    assert!(
        shown.iter().any(|r| r.contains("↑ 70-99 / 99")),
        "{}",
        shown.join("\n")
    );
    // 99 keys, a page of 10 rows a press, not one per press.
    assert_eq!(presses, 10);
}

/// `G` lands on the last data row, not on a `↓` pager row (stepping onto one scrolls).
#[test]
fn jump_last_lands_on_data() {
    let dir = savepoint();
    let mut tree = open_big(&dir);
    let n = tree.row_count() as u16;
    let shown = rows(&mut tree, 120, n + 2);
    assert_eq!(shown.last().unwrap(), "    ↓ 1-500 / 1200");
    tree.jump_last();
    assert_eq!(element_at(&tree), 499);
    // And `G` then `↓` scrolls the window, as at any other last row of a window.
    tree.move_down();
    assert_eq!(element_at(&tree), 499);
    let shown = rows(&mut tree, 120, 600);
    assert!(
        shown.iter().any(|r| r == "    ↓ 2-501 / 1200"),
        "{}",
        shown.join("\n")
    );
}

#[test]
fn an_operator_state_of_1200_elements_pages_to_the_last_one() {
    let dir = savepoint();
    let mut tree = open_tree(&dir);
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
    tree.finish_search();
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
    let dir = savepoint();
    // The keys and elements with arrows, all on one page: the first operator alone.
    let reader = std::sync::Arc::new(flink_explorer::reader::StateReader::open(&dir).unwrap());
    let first = flink_explorer::operators::summarize(reader.savepoint(), &dir).remove(0);
    let mut tree = TreeView::new(reader, vec![first]);
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
