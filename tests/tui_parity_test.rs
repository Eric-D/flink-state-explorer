//! The TUI shows what `parser::value` decodes, and nothing else (#18): every (key, state) of a
//! synthetic savepoint, rendered through `TreeView` on a `TestBackend`, reads as
//! `value_to_display` of what the streaming export (`--export-state`) decodes for it. The types
//! are the ones the TUI's former private decoders got wrong: MAP values behind their
//! `NullableSerializer` byte, `Map<Enum,Boolean>`, keyed `ListState` (delimited, not
//! length-prefixed), `byte[]`, `int[]`, `Byte`, tuples and `Char`.

mod common;

use common::synth::{self, Item, KeyedOperator, Kind, Ser, Spec, Storage, Val};
use common::tui::{compare_operator, draw, expand_all, export_operator, labels};
use flink_explorer::index::builder::build_index;
use flink_explorer::parser;
use flink_explorer::parser::pojo::millis_to_iso;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::Terminal;

const COLORS: &[&str] = &["RED", "GREEN", "BLUE"];

fn operator() -> KeyedOperator {
    let mut op = KeyedOperator::new(0x5a, 2, 128, Ser::String)
        .state("m-str", Kind::Map(Ser::String, Ser::String))
        .state("m-long", Kind::Map(Ser::String, Ser::Long))
        .state(
            "m-enum",
            Kind::Map(Ser::Enum("com.example.Color", COLORS), Ser::Boolean),
        )
        .state("l-long", Kind::List(Ser::Long))
        .state("l-str", Kind::List(Ser::String))
        .state("bytes", Kind::Value(Ser::ByteArray))
        .state("ints", Kind::Value(Ser::IntArray))
        .state("byte", Kind::Value(Ser::Byte))
        .state("tuple", Kind::Value(Ser::Tuple(&[Ser::String, Ser::Long])))
        .state("char", Kind::Value(Ser::Char))
        .state("when", Kind::Value(Ser::Long));
    for i in 0..6i64 {
        let key = Val::Str(format!("user-{}", i));
        let item = |state: usize, value: Val| Item::Value {
            state,
            key: key.clone(),
            value,
        };
        // Key 5 holds no value at all for most states: "empty" / "null" rows.
        if i == 5 {
            op.items.push(item(7, Val::Byte(-1)));
            continue;
        }
        // Key 0 has more entries and elements than the TUI lists ("… +N more").
        let (n_map, n_list) = if i == 0 {
            (57, 130)
        } else {
            (2 + i as usize, 3)
        };
        for j in 0..n_map {
            op.items.push(Item::MapEntry {
                state: 0,
                key: key.clone(),
                map_key: Val::Str(format!("k{:02}", j)),
                // A null map value, and one that is a valid comma: the old decoders split there.
                value: match j {
                    1 => None,
                    2 => Some(Val::str("a,b")),
                    _ => Some(Val::Str(format!("value-{}", j))),
                },
            });
            op.items.push(Item::MapEntry {
                state: 1,
                key: key.clone(),
                map_key: Val::Str(format!("n{}", j)),
                value: Some(Val::Long(1_000 * j as i64 - 7)),
            });
        }
        for (o, c) in COLORS.iter().enumerate().take(1 + i as usize % 3) {
            op.items.push(Item::MapEntry {
                state: 2,
                key: key.clone(),
                map_key: Val::Enum(o as i32, c),
                value: Some(Val::Bool(o % 2 == 0)),
            });
        }
        op.items.push(Item::List {
            state: 3,
            key: key.clone(),
            // 0x2c (the ListDelimitedSerializer delimiter) inside the values themselves.
            values: (0..n_list)
                .map(|j| Val::Long(0x2c2c + j as i64 * 1000))
                .collect(),
        });
        op.items.push(Item::List {
            state: 4,
            key: key.clone(),
            values: vec![Val::str("x,y"), Val::str(""), Val::str("é☕")],
        });
        op.items
            .push(item(5, Val::Bytes(vec![0xff, 0x00, i as u8])));
        op.items
            .push(item(6, Val::Ints(vec![-1, 0, i as i32, 1 << 20])));
        op.items.push(item(7, Val::Byte(53 + i as i8)));
        op.items.push(item(
            8,
            Val::Tuple(vec![
                Val::Str(format!("org-{}", i)),
                Val::Long(1_791_835_342_586),
            ]),
        ));
        op.items.push(item(9, Val::Char(0x41 + i as u16)));
        op.items.push(item(10, Val::Long(1_700_000_000_000 + i)));
    }
    op
}

fn written() -> (synth::TempDir, parser::metadata::Savepoint) {
    let mut spec = Spec::new(Storage::Files);
    spec.operators.push(operator());
    let dir = synth::temp_dir("tui-parity");
    synth::write_savepoint(&spec, &dir);
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    (dir, savepoint)
}

#[test]
fn every_key_and_state_renders_as_the_export_decodes_it() {
    let (dir, savepoint) = written();
    let index = build_index(&savepoint, 0, 0, &dir);
    let op = &index.operators[0];
    let ks = op.keyed_state.as_ref().unwrap();
    assert_eq!(ks.descriptors.len(), 11);
    assert_eq!(ks.partitions.len(), 6);

    let (pairs, diffs) = compare_operator(&savepoint, &dir, &index, op);
    assert_eq!(pairs, 66);
    assert!(
        diffs.is_empty(),
        "{} of {} (key, state) pairs render differently:\n{}",
        diffs.len(),
        pairs,
        diffs.join("\n")
    );
}

/// The parity above holds against the export; this pins the export (and so the TUI) to what was
/// written, for the cases the old TUI decoders got wrong.
#[test]
fn the_exported_values_are_the_written_ones() {
    let (dir, savepoint) = written();
    let index = build_index(&savepoint, 0, 0, &dir);
    let exported = export_operator(&savepoint, &dir, &index.operators[0]).unwrap();
    let get = |key: &str, state: &str| {
        exported
            .values
            .get(&(key.to_string(), state.to_string()))
            .unwrap_or_else(|| panic!("{} / {} not exported", key, state))
            .clone()
    };
    let m = get("user-1", "m-str");
    assert_eq!(m["k00"], "value-0");
    assert!(m["k01"].is_null());
    assert_eq!(m["k02"], "a,b");
    assert_eq!(get("user-2", "m-long")["n3"], 2_993);
    let e = get("user-2", "m-enum");
    assert_eq!(e["RED"], true);
    assert_eq!(e["BLUE"], true);
    assert_eq!(e["GREEN"], false);
    assert_eq!(get("user-0", "l-long").as_array().unwrap().len(), 130);
    assert_eq!(get("user-1", "l-long")[2], 0x2c2c + 2000);
    assert_eq!(
        get("user-1", "l-str"),
        serde_json::json!(["x,y", "", "é☕"])
    );
    assert_eq!(get("user-3", "bytes"), "[3 bytes]");
    assert_eq!(
        get("user-3", "ints"),
        serde_json::json!([-1, 0, 3, 1 << 20])
    );
    assert_eq!(get("user-3", "byte"), 56);
    assert_eq!(get("user-5", "byte"), -1);
    assert_eq!(
        get("user-4", "tuple"),
        serde_json::json!(["org-4", 1_791_835_342_586i64])
    );
    assert_eq!(get("user-2", "char"), "C");
}

/// `auto_date` (on by default) is presentation: a `Long` in the epoch-millis range shows as a
/// date, everything else as `value_to_display`.
#[test]
fn auto_date_shows_a_long_state_as_a_date() {
    let (dir, savepoint) = written();
    let index = build_index(&savepoint, 0, 0, &dir);
    let mut tree = TreeView::new(&index);
    let mut terminal = Terminal::new(TestBackend::new(200, 4)).unwrap();
    draw(&mut terminal, &mut tree);
    tree.apply_filter(&Some(flink_explorer::tui::filter::AppliedFilter::Exact(
        b"user-1".to_vec(),
    )));
    expand_all(&mut tree);
    let mut terminal = Terminal::new(TestBackend::new(200, tree.row_count() as u16 + 2)).unwrap();
    let rows = labels(&draw(&mut terminal, &mut tree));
    let row = |needle: &str| {
        rows.iter()
            .find(|r| r.contains(needle))
            .unwrap_or_else(|| panic!("no {:?} in\n{}", needle, rows.join("\n")))
            .clone()
    };
    assert_eq!(
        row("• when: "),
        format!("    • when: {}", millis_to_iso(1_700_000_000_001))
    );
    // A tuple's Long is a field of a structure, not a Long state: left alone.
    assert_eq!(row("• tuple: "), "    • tuple: [\"org-1\", 1791835342586]");
    // A Map<String,Long> value is a Long: 993 is not in the date range, so stays a number.
    assert_eq!(row("n1 : "), "        n1 : 993");
}
