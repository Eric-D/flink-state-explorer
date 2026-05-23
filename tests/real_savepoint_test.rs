use std::path::Path;

use flink_explorer::index::builder::build_index;
use flink_explorer::parser;

const SAVEPOINT_DIR: &str = "tests/fixtures/savepoint-e6b0ef-0f4907c0f8f5";

#[test]
fn parse_real_savepoint_metadata() {
    let metadata_path = Path::new(SAVEPOINT_DIR).join("_metadata");
    if !metadata_path.exists() {
        eprintln!("Skipping: real savepoint fixture not found");
        return;
    }

    let savepoint = match parser::parse_metadata(&metadata_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Parse error: {} (debug: {:?})", e, e);
            panic!("failed to parse real savepoint _metadata: {}", e);
        }
    };

    assert!(
        savepoint.version >= 3 && savepoint.version <= 5,
        "expected metadata version 3-5, got {}",
        savepoint.version
    );
    assert!(
        !savepoint.operators.is_empty(),
        "expected at least one operator"
    );

    println!("Checkpoint ID: {}", savepoint.checkpoint_id);
    println!("Operators: {}", savepoint.operators.len());
    for (i, op) in savepoint.operators.iter().enumerate() {
        println!(
            "  [{}] id={} parallelism={} maxParallelism={} subtasks={}",
            i,
            op.display_name(),
            op.parallelism,
            op.max_parallelism,
            op.subtask_states.len()
        );
        for sub in &op.subtask_states {
            let has_keyed = !matches!(
                sub.managed_keyed_state,
                flink_explorer::parser::state_handle::StateHandle::Null
            ) || !matches!(
                sub.raw_keyed_state,
                flink_explorer::parser::state_handle::StateHandle::Null
            );
            let has_oper = sub.managed_operator_state.is_some() || sub.raw_operator_state.is_some();
            if has_keyed || has_oper {
                println!(
                    "    subtask {}: has_keyed={} has_operator={}",
                    sub.subtask_index, has_keyed, has_oper
                );
            }
        }
    }
    println!("Master states: {}", savepoint.master_states.len());
}

#[test]
fn build_index_from_real_savepoint() {
    let metadata_path = Path::new(SAVEPOINT_DIR).join("_metadata");
    if !metadata_path.exists() {
        eprintln!("Skipping: real savepoint fixture not found");
        return;
    }

    let meta = std::fs::metadata(&metadata_path).unwrap();
    let savepoint = parser::parse_metadata(&metadata_path).unwrap();
    let index = build_index(&savepoint, meta.len(), 0, Path::new(SAVEPOINT_DIR));

    assert!(!index.operators.is_empty());

    let total_entries: usize = index
        .operators
        .iter()
        .flat_map(|op| &op.states)
        .map(|s| s.entries.len())
        .sum();

    println!("Index built: {} operators", index.operators.len());
    for op in &index.operators {
        let keyed_keys = op.keyed_state.as_ref().map_or(0, |k| k.partitions.len());
        println!(
            "  {} — {} states, {} keys",
            op.display_name,
            op.states.len(),
            keyed_keys
        );
        for s in &op.states {
            if !s.entries.is_empty() {
                println!(
                    "    {} [{}]: {} entries",
                    s.name,
                    s.state_type,
                    s.entries.len()
                );
            }
        }
        if let Some(ks) = &op.keyed_state {
            for p in ks.partitions.iter().take(3) {
                println!("    key={:?} ({} values)", p.key, p.values.len());
            }
            if ks.partitions.len() > 3 {
                println!("    ... {} more keys", ks.partitions.len() - 3);
            }
        }
    }
    println!("Total entries indexed: {}", total_entries);
}
