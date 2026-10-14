//! Non-ASCII text in the TUI (#14): long values, keys and operator names are shortened on
//! `char` boundaries (a byte slice panicked as soon as the cut fell inside `é`, CJK or an
//! emoji), and Ctrl-C quits. Fixture-free: the savepoint is synthetic.

mod common;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use flink_explorer::tui::app::App;
use flink_explorer::tui::loading::{LoadingScreen, LoadingStep};
use flink_explorer::tui::text;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::widgets::{Block, Borders};
use ratatui::Terminal;

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

/// Values whose byte length, char count and column width all differ: every byte-based cut
/// (`&s[..77]`, `&label[..width - 16]`) lands inside a character for some of them. All in the
/// BMP: `read_string_value` still drops UTF-16 surrogate pairs (#25), so a 4-byte emoji would not
/// reach the TUI through a `StringValue`; `☕` (3 bytes, 2 columns) stands in for it, and the
/// `tui::text` unit tests cover 4-byte characters directly.
fn long_values() -> Vec<(&'static str, String)> {
    vec![
        ("clé", "é".repeat(50)),
        ("鍵", "日本語のテキスト".repeat(10)),
        ("tasse-☕", "aé日☕".repeat(30)),
        ("mixed", format!("x{}", "Ωμέγα ".repeat(30))),
        ("short", "ça va".to_string()),
    ]
}

fn savepoint_with_long_values() -> synth::TempDir {
    let mut op = KeyedOperator::new(0x77, 2, 128, Ser::String)
        .state("label", Kind::Value(Ser::String))
        .state("count", Kind::Value(Ser::Long));
    for (i, (key, value)) in long_values().into_iter().enumerate() {
        let key = Val::str(key);
        op.items.push(Item::Value {
            state: 0,
            key: key.clone(),
            value: Val::Str(value),
        });
        op.items.push(Item::Value {
            state: 1,
            key,
            value: Val::Long(i as i64),
        });
    }
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(op);
    let dir = synth::temp_dir("non-ascii");
    synth::write_savepoint(&spec, &dir);
    dir
}

/// Expand the operator and every key, rendering at `width` columns after each step.
fn render_everything(tree: &mut TreeView, width: u16) -> Vec<String> {
    let mut terminal = Terminal::new(TestBackend::new(width, 40)).unwrap();
    let mut draw = |tree: &mut TreeView| {
        terminal
            .draw(|f| tree.render(f, Block::default().borders(Borders::ALL), f.area()))
            .unwrap();
        screen(&terminal)
    };
    draw(tree);
    tree.jump_first();
    tree.expand(); // the operator
    for _ in 0..long_values().len() {
        tree.move_down();
        tree.expand(); // a key: builds its value rows, where the cuts happen
        draw(tree);
        // Skip over the two value rows just opened.
        tree.move_down();
        tree.move_down();
    }
    draw(tree)
}

#[test]
fn long_non_ascii_values_render_at_80_and_120_columns() {
    let dir = savepoint_with_long_values();

    for width in [80u16, 120] {
        for debug in [false, true] {
            let mut tree = TreeView::open(&dir).unwrap();
            if debug {
                tree.toggle_debug(); // longer labels (hex suffix): more of them are cut
            }
            let rows = render_everything(&mut tree, width);
            let dump = rows.join("\n");
            for (key, value) in long_values() {
                // The key row is shown, at every width.
                rows.iter()
                    .find(|r| r.contains(&format!("⯈ {} (", key)))
                    .unwrap_or_else(|| panic!("key {:?} not shown at {}:\n{}", key, width, dump));
                // The value row starts with the value itself, whole or shortened (quoted, as
                // `parser::value::value_to_display` shows a string).
                let head: String = value.chars().take(5).collect();
                let value_row = rows
                    .iter()
                    .find(|r| {
                        r.contains(&format!(
                            "label{}: \"{}",
                            if debug { " [VALUE]" } else { "" },
                            head
                        ))
                    })
                    .unwrap_or_else(|| {
                        panic!("value of {:?} not shown at {}:\n{}", key, width, dump)
                    });
                if text::width(&value) > 80 {
                    assert!(
                        value_row.contains("..."),
                        "{:?} at {}: {}",
                        key,
                        width,
                        value_row
                    );
                } else if !debug {
                    assert!(
                        value_row.contains(&value),
                        "{:?} at {}: {}",
                        key,
                        width,
                        value_row
                    );
                }
            }
        }
    }
}

#[test]
fn the_loading_screen_shortens_a_non_ascii_operator_name() {
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    for name in [
        "日本語".repeat(20),
        format!("a{}", "é".repeat(60)),
        "🎉".repeat(40),
        "short ünïcödé".to_string(),
    ] {
        let mut loading = LoadingScreen::new(std::path::Path::new("/tmp/savepoint-é"));
        loading.advance(LoadingStep::ReadingHeaders {
            total_operators: 3,
            current_operator: 1,
            operator_name: name.clone(),
        });
        terminal.draw(|f| loading.draw(f)).unwrap();
        let rows = screen(&terminal);
        let head: String = name.chars().take(3).collect();
        let shown = rows
            .iter()
            .find(|r| r.contains(&head))
            .unwrap_or_else(|| panic!("{:?} not shown:\n{}", name, rows.join("\n")));
        if text::width(&name) > 55 {
            assert!(shown.contains("..."), "{}", shown);
        } else {
            assert!(shown.contains(&name), "{}", shown);
        }
    }
}

/// The app and its savepoint directory (removed when the guard drops: keep it alive).
fn app() -> (App, synth::TempDir) {
    let dir = savepoint_with_long_values();
    let app = App::new_with_progress(dir.to_path_buf(), true, |_| {})
        .expect("app over a synthetic savepoint");
    (app, dir)
}

fn ctrl_c() -> KeyEvent {
    KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
}

fn key(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)
}

#[test]
fn ctrl_c_quits_from_the_tree() {
    let (mut app, _dir) = app();
    app.handle_key(key('c'));
    assert!(!app.should_quit(), "a plain c is not a quit");
    app.handle_key(ctrl_c());
    assert!(app.should_quit());
}

#[test]
fn ctrl_c_quits_while_typing_a_filter() {
    let (mut app, _dir) = app();
    app.handle_key(key('/'));
    app.handle_key(key('q')); // typed into the filter, not a quit
    assert!(!app.should_quit());
    app.handle_key(ctrl_c());
    assert!(app.should_quit());
}

#[test]
fn ctrl_c_quits_from_an_overlay() {
    for overlay in ['S', 'I', 'P'] {
        let (mut app, _dir) = app();
        app.handle_key(KeyEvent::new(KeyCode::Char(overlay), KeyModifiers::SHIFT));
        app.handle_key(ctrl_c());
        assert!(app.should_quit(), "overlay {}", overlay);
    }
}

#[test]
fn input_queued_while_loading_is_dropped_and_ctrl_c_detected() {
    use crossterm::event::{Event, KeyEventKind, KeyEventState};
    use flink_explorer::tui::app::drain_is_interrupt;
    let key = |code, modifiers, kind| {
        Event::Key(KeyEvent {
            code,
            modifiers,
            kind,
            state: KeyEventState::NONE,
        })
    };
    let plain = || key(KeyCode::Char('j'), KeyModifiers::NONE, KeyEventKind::Press);
    assert!(!drain_is_interrupt(vec![plain(), plain()]));
    assert!(drain_is_interrupt(vec![
        plain(),
        Event::Key(ctrl_c()),
        plain()
    ]));
    // A release of Ctrl-C (kitty protocol) is not a press.
    assert!(!drain_is_interrupt(vec![key(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
        KeyEventKind::Release
    )]));
    // Every event is consumed, interrupt or not.
    let mut it = vec![plain(), plain(), plain()].into_iter();
    drain_is_interrupt(&mut it);
    assert_eq!(it.count(), 0);
}
