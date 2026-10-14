//! Render the TUI tree to a `TestBackend` and compare what it shows for every (key, state) with
//! what the streaming export (`--export-state`) decodes for it: both must come from
//! `parser::value`, so a label is `value_to_display` of the exported value (#18).

use std::collections::BTreeMap;
use std::path::Path;

use flink_explorer::export_state::{export_state_rows, hex_id, RowSink, StateRow};
use flink_explorer::index::cache::{CachedOperator, IndexCache, StateDescriptor};
use flink_explorer::parser::metadata::Savepoint;
use flink_explorer::parser::value::value_to_display;
use flink_explorer::tui::filter::AppliedFilter;
use flink_explorer::tui::text;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::widgets::{Block, Borders};
use ratatui::Terminal;
use serde_json::{Map, Value};

/// The screen as text, one string per row (the cells a wide character hides are skipped).
pub fn screen(terminal: &Terminal<TestBackend>) -> Vec<String> {
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

pub fn draw(terminal: &mut Terminal<TestBackend>, tree: &mut TreeView) -> Vec<String> {
    terminal
        .draw(|f| tree.render(f, Block::default().borders(Borders::ALL), f.area()))
        .unwrap();
    screen(terminal)
}

/// The labels of a drawn tree: the border and the highlight column taken off.
pub fn labels(rows: &[String]) -> Vec<String> {
    rows.iter()
        .filter_map(|r| r.strip_prefix('│'))
        .map(|r| {
            let r = r.strip_suffix('│').unwrap_or(r).trim_end();
            r.chars().skip(2).collect::<String>()
        })
        .collect()
}

/// Expand every row, top to bottom (no pager is crossed: the caller keeps the tree to one page).
pub fn expand_all(tree: &mut TreeView) {
    tree.jump_first();
    let mut i = 0;
    loop {
        tree.expand();
        i += 1;
        if i >= tree.row_count() {
            break;
        }
        tree.move_down();
    }
}

/// Every keyed value of one operator, as the streaming export decodes it: `(key, state) → value`.
/// A MAP state's rows (one per entry) are gathered back into an object, keyed by `map_key`.
#[derive(Default)]
pub struct Exported {
    pub values: BTreeMap<(String, String), Value>,
    /// Non-MAP (key, state) pairs exported more than once (a window state's several windows):
    /// the first row is kept, as the index keeps the first window.
    pub repeated: usize,
}

impl RowSink for Exported {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        let slot = (row.key.to_string(), row.state.to_string());
        match row.map_key {
            Some(mk) => {
                let obj = self
                    .values
                    .entry(slot)
                    .or_insert_with(|| Value::Object(Map::new()));
                obj.as_object_mut()
                    .unwrap()
                    .insert(mk.to_string(), row.value.clone());
            }
            None if self.values.contains_key(&slot) => self.repeated += 1,
            None => {
                self.values.insert(slot, row.value.clone());
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }
}

pub fn export_operator(
    savepoint: &Savepoint,
    dir: &Path,
    op: &CachedOperator,
) -> Result<Exported, String> {
    let mut out = Exported::default();
    let op_hex = hex_id(&op.id);
    for desc in &op.keyed_state.as_ref().unwrap().descriptors {
        export_state_rows(savepoint, dir, Some(&op_hex), &desc.name, None, &mut out)
            .map_err(|e| format!("{}: {}", desc.name, e))?;
    }
    Ok(out)
}

/// One state row of a key and the rows under it.
#[derive(Debug, PartialEq)]
pub struct EntryView {
    pub label: String,
    pub children: Vec<String>,
}

/// What the TUI must show for `value` (`None`: no exported row): the state row, and the rows of
/// a MAP / LIST. A structured VALUE's own rows are not part of the comparison (they are layout:
/// its label already holds the whole value).
pub fn expected_entry(desc: &StateDescriptor, value: Option<&Value>) -> EntryView {
    let collection = |prefix: &str, fallback: &str| {
        if desc.value_type.starts_with(prefix) {
            desc.value_type.clone()
        } else {
            fallback.to_string()
        }
    };
    let head = |expandable: bool| if expandable { "⯈" } else { "•" };
    match desc.state_type.as_str() {
        "MAP" => {
            let tl = collection("Map<", "MAP");
            match value.and_then(Value::as_object) {
                Some(m) if !m.is_empty() => {
                    let mut children: Vec<String> = m
                        .iter()
                        .take(50)
                        .map(|(k, v)| format!("        {} : {}", k, value_to_display(v)))
                        .collect();
                    if m.len() > 50 {
                        children.push(format!("        … +{} more", m.len() - 50));
                    }
                    EntryView {
                        label: format!(
                            "    {} {} [{}]{}: {} entries",
                            head(true),
                            desc.name,
                            tl,
                            desc.pojo_detail,
                            m.len()
                        ),
                        children,
                    }
                }
                _ => EntryView {
                    label: format!(
                        "    {} {} [{}]{}: empty",
                        head(false),
                        desc.name,
                        tl,
                        desc.pojo_detail
                    ),
                    children: Vec::new(),
                },
            }
        }
        "LIST" => {
            let tl = collection("List<", "LIST");
            match value.and_then(Value::as_array) {
                Some(a) if !a.is_empty() => {
                    let mut children: Vec<String> = a
                        .iter()
                        .take(100)
                        .enumerate()
                        .map(|(i, v)| format!("        [{}] {}", i, value_to_display(v)))
                        .collect();
                    if a.len() > 100 {
                        children.push(format!("        … +{} more", a.len() - 100));
                    }
                    EntryView {
                        label: format!(
                            "    {} {} [{}]{}: {} elements",
                            head(true),
                            desc.name,
                            tl,
                            desc.pojo_detail,
                            a.len()
                        ),
                        children,
                    }
                }
                _ => EntryView {
                    label: format!(
                        "    {} {} [{}]{}: empty",
                        head(false),
                        desc.name,
                        tl,
                        desc.pojo_detail
                    ),
                    children: Vec::new(),
                },
            }
        }
        _ => EntryView {
            // The bullet depends on the width (a wide structure expands): compared without it.
            label: format!(
                "{}: {}",
                desc.name,
                value.map_or("null".to_string(), value_to_display)
            ),
            children: Vec::new(),
        },
    }
}

/// Render the one key `key` of operator `op` (alone in its tree, every row expanded, auto-date
/// off) and return its state rows.
pub fn render_key(
    op: &CachedOperator,
    index: &IndexCache,
    key: &str,
    width: u16,
) -> Vec<EntryView> {
    let mut one = op.clone();
    one.states.clear(); // operator states are not keyed values
    let single = IndexCache {
        operators: vec![one],
        ..index.clone()
    };
    let mut tree = TreeView::new(&single);
    tree.toggle_auto_date(); // the export has no auto-date: compare the decoded values
    let mut terminal = Terminal::new(TestBackend::new(width, 4)).unwrap();
    draw(&mut terminal, &mut tree); // fixes the width before rows are built
    tree.apply_filter(&Some(AppliedFilter::Exact(key.as_bytes().to_vec())));
    expand_all(&mut tree);
    let height = tree.row_count() as u16 + 2;
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    let rows = labels(&draw(&mut terminal, &mut tree));

    let mut entries: Vec<EntryView> = Vec::new();
    for r in rows {
        if r.starts_with("        ") {
            if let Some(e) = entries.last_mut() {
                e.children.push(r);
            }
        } else if r.starts_with("    ") {
            entries.push(EntryView {
                label: r,
                children: Vec::new(),
            });
        }
    }
    entries
}

/// Compare every (key, state) of `op` with the export; returns one line per difference.
pub fn compare_operator(
    savepoint: &Savepoint,
    dir: &Path,
    index: &IndexCache,
    op: &CachedOperator,
) -> (usize, Vec<String>) {
    let exported = export_operator(savepoint, dir, op).expect("export");
    let ks = op.keyed_state.as_ref().unwrap();
    let mut pairs = 0;
    let mut diffs = Vec::new();
    for part in &ks.partitions {
        let expected: Vec<EntryView> = ks
            .descriptors
            .iter()
            .map(|d| expected_entry(d, exported.values.get(&(part.key.clone(), d.name.clone()))))
            .collect();
        let widest = expected
            .iter()
            .map(|e| text::width(&e.label))
            .max()
            .unwrap_or(0);
        let width = (widest + 40).clamp(200, 6000) as u16;
        let shown = render_key(op, index, &part.key, width);
        assert_eq!(
            shown.len(),
            expected.len(),
            "key {:?}: {} state rows shown, {} states",
            part.key,
            shown.len(),
            expected.len()
        );
        for ((desc, want), got) in ks.descriptors.iter().zip(&expected).zip(&shown) {
            pairs += 1;
            let same = match desc.state_type.as_str() {
                "MAP" | "LIST" => want == got,
                _ => {
                    got.label
                        .strip_prefix("    • ")
                        .or_else(|| got.label.strip_prefix("    ⯈ "))
                        == Some(want.label.as_str())
                }
            };
            if !same {
                diffs.push(format!(
                    "{} / {}:\n    tui:    {:?}\n    export: {:?}",
                    part.key, desc.name, got, want
                ));
            }
        }
    }
    (pairs, diffs)
}
