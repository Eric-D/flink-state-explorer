//! Render the TUI tree to a `TestBackend` and compare what it shows for every (key, state) with
//! what the streaming export (`--export-state`) decodes for it: both must come from
//! `parser::value`, so a label is `value_to_display` of the exported value (#18).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use flink_explorer::export_state::{export_state_rows, RowSink, StateRow};
use flink_explorer::operators::OperatorSummary;
use flink_explorer::parser::metadata::Savepoint;
use flink_explorer::parser::value::value_to_display;
use flink_explorer::reader::{StateInfo, StateReader};
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

/// Expand every row, top to bottom, selecting each in turn: a pager row (a MAP's `↓`) is
/// selected without scrolling its window.
pub fn expand_all(tree: &mut TreeView) {
    let mut i = 0;
    while i < tree.row_count() {
        tree.select_row(i);
        tree.expand();
        i += 1;
    }
}

/// A tree over the savepoint in `dir`, with a reader of its own (the default entry limit).
pub fn open_tree(dir: &Path) -> TreeView {
    TreeView::open(dir).expect("open savepoint")
}

/// Every keyed value of one operator, as the streaming export decodes it: `(key, state) → value`.
/// A MAP state's rows (one per entry) are gathered back into an object, keyed by `map_key`.
#[derive(Default)]
pub struct Exported {
    pub values: BTreeMap<(String, String), Value>,
    /// A MAP state's map keys, in the order they were exported (on-disk order, which the TUI
    /// shows).
    pub map_keys: BTreeMap<(String, String), Vec<String>>,
    /// Non-MAP (key, state) pairs exported more than once (a window state's several windows):
    /// the first row is kept, as the TUI shows the first window.
    pub repeated: usize,
}

impl RowSink for Exported {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        let slot = (row.key.to_string(), row.state.to_string());
        match row.map_key {
            Some(mk) => {
                self.map_keys
                    .entry(slot.clone())
                    .or_default()
                    .push(mk.to_string());
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

/// Every state of operator `op_hex`, exported.
pub fn export_operator(
    savepoint: &Savepoint,
    dir: &Path,
    op_hex: &str,
    states: &[StateInfo],
) -> Result<Exported, String> {
    let mut out = Exported::default();
    for s in states {
        export_state_rows(savepoint, dir, Some(op_hex), &s.name, None, &mut out)
            .map_err(|e| format!("{}: {}", s.name, e))?;
    }
    Ok(out)
}

/// ` → com.example.Pojo { a: Long, … }` for a POJO-valued state, as the TUI labels it.
pub fn pojo_detail(reader: &StateReader, op_hex: &str, state: &str) -> String {
    reader
        .value_pojo(op_hex, state)
        .unwrap()
        .map(|p| {
            let fields: Vec<String> = p
                .leaf_fields("")
                .iter()
                .map(|(path, t)| format!("{}: {}", path, t))
                .collect();
            format!(" → {} {{ {} }}", p.class_name, fields.join(", "))
        })
        .unwrap_or_default()
}

/// One state row of a key and the rows under it.
#[derive(Debug, PartialEq)]
pub struct EntryView {
    pub label: String,
    pub children: Vec<String>,
}

/// What the TUI must show for `value` (`None`: no exported row): the state row, and the rows of
/// a MAP (its first 50 entries in `map_keys` order — on-disk order — then a `↓` pager row) /
/// LIST. A structured VALUE's own rows are not part of the comparison (they are layout: its
/// label already holds the whole value).
pub fn expected_entry(
    desc: &StateInfo,
    pojo_detail: &str,
    value: Option<&Value>,
    map_keys: &[String],
) -> EntryView {
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
                    let mut children: Vec<String> = map_keys
                        .iter()
                        .take(50)
                        .map(|k| format!("        {} : {}", k, value_to_display(&m[k])))
                        .collect();
                    if m.len() > 50 {
                        children.push(format!("        ↓ 1-50 / {}", m.len()));
                    }
                    EntryView {
                        label: format!(
                            "    {} {} [{}]{}: {} entries",
                            head(true),
                            desc.name,
                            tl,
                            pojo_detail,
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
                        pojo_detail
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
                            pojo_detail,
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
                        pojo_detail
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
/// off) and return its state rows. The key is found through the filter: `get` for a hashable key
/// type, a scan otherwise.
pub fn render_key(
    reader: &Arc<StateReader<'static>>,
    op: &OperatorSummary,
    key: &str,
    width: u16,
) -> Vec<EntryView> {
    let mut one = op.clone();
    one.states.clear(); // operator states are not keyed values
    let mut tree = TreeView::new(reader.clone(), vec![one]);
    tree.toggle_auto_date(); // the export has no auto-date: compare the decoded values
    let mut terminal = Terminal::new(TestBackend::new(width, 4)).unwrap();
    draw(&mut terminal, &mut tree); // fixes the width before rows are built
    tree.apply_filter(&Some(AppliedFilter::Exact(key.as_bytes().to_vec())));
    tree.finish_search();
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

/// Compare every (key, state) of `op` with the export; returns the pairs compared and one line
/// per difference.
pub fn compare_operator(
    savepoint: &Savepoint,
    dir: &Path,
    reader: &Arc<StateReader<'static>>,
    op: &OperatorSummary,
) -> (usize, Vec<String>) {
    let hex = op.operator_id_hex();
    let keyed = super::keyed::keyed(dir, &hex).expect("keyed operator");
    let exported = export_operator(savepoint, dir, &hex, &keyed.states).expect("export");
    let details: Vec<String> = keyed
        .states
        .iter()
        .map(|s| pojo_detail(reader, &hex, &s.name))
        .collect();
    let mut pairs = 0;
    let mut diffs = Vec::new();
    for record in &keyed.keys {
        let expected: Vec<EntryView> = keyed
            .states
            .iter()
            .zip(&details)
            .map(|(d, detail)| {
                let slot = (record.key.clone(), d.name.clone());
                expected_entry(
                    d,
                    detail,
                    exported.values.get(&slot),
                    exported.map_keys.get(&slot).map_or(&[], Vec::as_slice),
                )
            })
            .collect();
        let widest = expected
            .iter()
            .map(|e| text::width(&e.label))
            .max()
            .unwrap_or(0);
        let width = (widest + 40).clamp(200, 6000) as u16;
        let shown = render_key(reader, op, &record.key, width);
        assert_eq!(
            shown.len(),
            expected.len(),
            "key {:?}: {} state rows shown, {} states",
            record.key,
            shown.len(),
            expected.len()
        );
        for ((desc, want), got) in keyed.states.iter().zip(&expected).zip(&shown) {
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
                    record.key, desc.name, got, want
                ));
            }
        }
    }
    (pairs, diffs)
}
