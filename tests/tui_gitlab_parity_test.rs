//! Needs `tests/fixtures/savepoint-gitlab` (`mvn verify` in `test-flink-job`): every (key, state)
//! of the project's own fixture renders in the TUI as `value_to_display` of what the streaming
//! export (`--export-state`) decodes for it (#18; 136 of 909 pairs differed while the TUI had
//! decoders of its own). The fixture-free counterpart, run in CI, is `tui_parity_test`.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::tui::compare_operator;
use flink_explorer::operators::summarize;
use flink_explorer::parser;
use flink_explorer::reader::StateReader;

#[test]
fn every_key_and_state_of_the_gitlab_fixture_renders_as_exported() {
    let dir = Path::new("tests/fixtures/savepoint-gitlab");
    assert!(
        dir.join("_metadata").exists(),
        "fixture missing: run `mvn verify` in test-flink-job"
    );
    let savepoint = parser::parse_metadata(&dir.join("_metadata")).unwrap();
    // The TUI writes nothing into the savepoint directory.
    let reader = Arc::new(StateReader::open(dir).unwrap());

    let (mut pairs, mut states, mut diffs) = (0, 0, Vec::new());
    for op in summarize(&savepoint, dir) {
        let Some(ks) = common::keyed::keyed(dir, &op.operator_id_hex()) else {
            continue;
        };
        if ks.keys.is_empty() {
            continue;
        }
        states += ks.states.len();
        let (n, d) = compare_operator(&savepoint, dir, &reader, &op);
        pairs += n;
        diffs.extend(d);
    }
    println!("{} (key, state) pairs over {} states", pairs, states);
    assert!(pairs >= 900, "only {} (key, state) pairs compared", pairs);
    assert!(
        diffs.is_empty(),
        "{} of {} (key, state) pairs render differently:\n{}",
        diffs.len(),
        pairs,
        diffs
            .iter()
            .take(40)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
