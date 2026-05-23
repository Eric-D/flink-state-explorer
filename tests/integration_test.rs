use std::fs;

use flink_explorer::index::builder::build_index;
use flink_explorer::parser;

#[test]
fn integration_parse_and_index_synthetic_savepoint() {
    let dir = std::env::temp_dir().join("flink-explorer-integration-test");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let metadata_path = dir.join("_metadata");
    let metadata_bytes = build_synthetic_metadata();
    fs::write(&metadata_path, &metadata_bytes).unwrap();

    let savepoint = parser::parse_metadata(&metadata_path).unwrap();
    assert_eq!(savepoint.version, 3);
    assert_eq!(savepoint.checkpoint_id, 1001);
    assert_eq!(savepoint.operators.len(), 2);

    let op0 = &savepoint.operators[0];
    assert_eq!(op0.parallelism, 4);

    let op1 = &savepoint.operators[1];
    assert_eq!(op1.parallelism, 2);

    let meta = fs::metadata(&metadata_path).unwrap();
    let index = build_index(&savepoint, meta.len(), 0, &dir);
    assert_eq!(index.operators.len(), 2);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn integration_missing_metadata_file() {
    let dir = std::env::temp_dir().join("flink-explorer-no-metadata");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let result = parser::parse_metadata(&dir.join("_metadata"));
    assert!(result.is_err());

    let _ = fs::remove_dir_all(&dir);
}

/// Build synthetic _metadata v3 with 2 operators.
fn build_synthetic_metadata() -> Vec<u8> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&0x4960_672Du32.to_be_bytes()); // magic
    buf.extend_from_slice(&3u32.to_be_bytes()); // version 3
    buf.extend_from_slice(&1001i64.to_be_bytes()); // checkpoint ID

    // 0 master states
    buf.extend_from_slice(&0i32.to_be_bytes());

    // 2 operators
    buf.extend_from_slice(&2i32.to_be_bytes());

    // --- Operator 1: keyed state ---
    buf.extend_from_slice(&0xAAAAi64.to_be_bytes()); // lower ID
    buf.extend_from_slice(&0xBBBBi64.to_be_bytes()); // upper ID
    buf.extend_from_slice(&4i32.to_be_bytes()); // parallelism
    buf.extend_from_slice(&128i32.to_be_bytes()); // maxParallelism
    buf.push(0); // coordinator: null
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 subtask
    {
        buf.extend_from_slice(&0i32.to_be_bytes()); // subtask index
        buf.extend_from_slice(&0i32.to_be_bytes()); // managed op: absent
        buf.extend_from_slice(&0i32.to_be_bytes()); // raw op: absent
                                                    // managed keyed: KeyGroupsStateHandle (tag 3)
        buf.push(3);
        buf.extend_from_slice(&0i32.to_be_bytes()); // startKeyGroup
        buf.extend_from_slice(&3i32.to_be_bytes()); // numKeyGroups
        buf.extend_from_slice(&0i64.to_be_bytes());
        buf.extend_from_slice(&100i64.to_be_bytes());
        buf.extend_from_slice(&200i64.to_be_bytes());
        // delegate: FileStateHandle (tag 2): stateSize + filePath
        buf.push(2);
        buf.extend_from_slice(&4096i64.to_be_bytes());
        write_utf(&mut buf, "taskowned/keyed-state-file");
        // raw keyed: null
        buf.push(0);
        // input channel: 0, result subpartition: 0
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.extend_from_slice(&0i32.to_be_bytes());
    }

    // --- Operator 2: operator state ---
    buf.extend_from_slice(&0xCCCCi64.to_be_bytes()); // lower ID
    buf.extend_from_slice(&0xDDDDi64.to_be_bytes()); // upper ID
    buf.extend_from_slice(&2i32.to_be_bytes()); // parallelism
    buf.extend_from_slice(&64i32.to_be_bytes()); // maxParallelism
    buf.push(0); // coordinator: null
    buf.extend_from_slice(&1i32.to_be_bytes()); // 1 subtask
    {
        buf.extend_from_slice(&0i32.to_be_bytes()); // subtask index
                                                    // managed op: present (flag=1) + PartitionableOperatorStateHandle (tag 4)
        buf.extend_from_slice(&1i32.to_be_bytes());
        buf.push(4); // tag = PARTITIONABLE_OPERATOR_STATE_HANDLE
        buf.extend_from_slice(&1i32.to_be_bytes()); // 1 partition
        write_utf(&mut buf, "kafka-offsets");
        buf.push(1); // mode = UNION (writeByte, not writeInt)
        buf.extend_from_slice(&2i32.to_be_bytes()); // 2 offsets
        buf.extend_from_slice(&0i64.to_be_bytes());
        buf.extend_from_slice(&512i64.to_be_bytes());
        // delegate: FileStateHandle (tag 2)
        buf.push(2);
        buf.extend_from_slice(&1024i64.to_be_bytes());
        write_utf(&mut buf, "taskowned/op-state-file");
        // raw op: absent
        buf.extend_from_slice(&0i32.to_be_bytes());
        // managed keyed: null
        buf.push(0);
        // raw keyed: null
        buf.push(0);
        // input channel: 0, result subpartition: 0
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.extend_from_slice(&0i32.to_be_bytes());
    }

    buf
}

fn write_utf(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
    buf.extend_from_slice(b);
}
