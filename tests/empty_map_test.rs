//! An empty MapState should render as a dimmed leaf labelled "empty" (not a dead expandable node).
//!
//! A key that holds no entry of a registered MAP state (it holds another state's value) is
//! written to a synthetic savepoint and read back through the `StateReader`, as the TUI does.

mod common;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;
use ratatui::widgets::Block;
use ratatui::Terminal;

fn render_expanded(dir: &std::path::Path) -> String {
    let mut tv = TreeView::open(dir).unwrap();
    tv.expand(); // operator
    tv.move_down();
    tv.expand(); // the key → its states appear
    let mut term = Terminal::new(TestBackend::new(120, 40)).unwrap();
    term.draw(|f| tv.render(f, Block::default(), Rect::new(0, 0, 120, 40)))
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

#[test]
fn empty_map_no_data_is_leaf_labelled_empty() {
    let mut op = KeyedOperator::new(0x21, 1, 128, Ser::String)
        .state("my-map", Kind::Map(Ser::String, Ser::String))
        .state("v", Kind::Value(Ser::Long));
    op.items.push(Item::Value {
        state: 1,
        key: Val::str("k1"),
        value: Val::Long(1),
    });
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op);
    let dir = synth::temp_dir("empty-map");
    synth::write_savepoint(&spec, &dir);

    let screen = render_expanded(&dir);
    let line = screen
        .lines()
        .find(|l| l.contains("my-map"))
        .expect("map line present");
    assert!(
        line.contains("empty"),
        "empty map should say 'empty': {line:?}"
    );
    assert!(
        line.contains('•') && !line.contains('⯈'),
        "empty map should be a leaf: {line:?}"
    );
}
