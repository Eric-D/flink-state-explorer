mod common;

use std::path::Path;

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
fn read_operators_and_keys_from_real_savepoint() {
    let dir = Path::new(SAVEPOINT_DIR);
    if !dir.join("_metadata").exists() {
        eprintln!("Skipping: real savepoint fixture not found");
        return;
    }

    let ops = common::keyed::operators(dir);
    assert!(!ops.is_empty());

    let total_elements: usize = ops
        .iter()
        .flat_map(|(op, _)| &op.states)
        .map(|s| s.elements.len())
        .sum();

    println!("Read: {} operators", ops.len());
    for (op, keyed) in &ops {
        let keyed_keys = keyed.as_ref().map_or(0, |k| k.keys.len());
        println!(
            "  {} — {} states, {} keys",
            op.display_name,
            op.states.len(),
            keyed_keys
        );
        for s in &op.states {
            if !s.elements.is_empty() {
                println!(
                    "    {} [{}]: {} entries",
                    s.name,
                    s.state_type,
                    s.elements.len()
                );
            }
        }
        if let Some(ks) = keyed {
            for k in ks.keys.iter().take(3) {
                println!("    key={:?} ({} entries)", k.key, k.entries.len());
            }
            if ks.keys.len() > 3 {
                println!("    ... {} more keys", ks.keys.len() - 3);
            }
        }
    }
    println!("Total operator-state elements: {}", total_elements);
}
