use flink_explorer::parser::metadata;
use flink_explorer::parser::state_handle::StateHandle;

const MASTER_STATE_MAGIC: u32 = 0xC96B_1696;

/// Build a minimal valid _metadata byte buffer (v3 format):
/// - Magic 0x4960672D, version 3, checkpoint ID 1
/// - 0 master states
/// - 1 operator with 1 subtask containing 1 managed keyed state (KeyGroupsStateHandle tag 3)
fn build_minimal_metadata_v3() -> Vec<u8> {
    let mut buf = Vec::new();

    // Header
    buf.extend_from_slice(&0x4960_672Du32.to_be_bytes());
    buf.extend_from_slice(&3u32.to_be_bytes()); // version 3
    buf.extend_from_slice(&1i64.to_be_bytes()); // checkpoint ID

    // 0 master states
    buf.extend_from_slice(&0i32.to_be_bytes());

    // 1 operator
    buf.extend_from_slice(&1i32.to_be_bytes());
    {
        // Operator ID: lower=1, upper=2
        buf.extend_from_slice(&1i64.to_be_bytes());
        buf.extend_from_slice(&2i64.to_be_bytes());
        // parallelism=4, maxParallelism=128
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&128i32.to_be_bytes());
        // coordinator state: null (StreamStateHandle tag=0)
        buf.push(0);

        // 1 subtask
        buf.extend_from_slice(&1i32.to_be_bytes());
        {
            // subtask index
            buf.extend_from_slice(&0i32.to_be_bytes());
            // managed operator state: 0 (absent)
            buf.extend_from_slice(&0i32.to_be_bytes());
            // raw operator state: 0 (absent)
            buf.extend_from_slice(&0i32.to_be_bytes());

            // managed keyed state: KeyGroupsStateHandle (tag 3)
            buf.push(3); // tag
            buf.extend_from_slice(&0i32.to_be_bytes()); // startKeyGroup
            buf.extend_from_slice(&2i32.to_be_bytes()); // numKeyGroups
            buf.extend_from_slice(&0i64.to_be_bytes()); // offset 0
            buf.extend_from_slice(&100i64.to_be_bytes()); // offset 1
                                                          // delegate: FileStateHandle (tag 2): stateSize + filePath
            buf.push(2);
            buf.extend_from_slice(&4096i64.to_be_bytes());
            let path = "/data/keyed-state";
            buf.extend_from_slice(&(path.len() as u16).to_be_bytes());
            buf.extend_from_slice(path.as_bytes());

            // raw keyed state: null (tag 0)
            buf.push(0);

            // input channel state: 0 (v3)
            buf.extend_from_slice(&0i32.to_be_bytes());
            // result subpartition state: 0 (v3)
            buf.extend_from_slice(&0i32.to_be_bytes());
        }
    }

    buf
}

#[test]
fn parse_minimal_metadata_v3() {
    let data = build_minimal_metadata_v3();
    let savepoint = metadata::parse_metadata(&data[..]).unwrap();

    assert_eq!(savepoint.version, 3);
    assert_eq!(savepoint.checkpoint_id, 1);
    assert_eq!(savepoint.operators.len(), 1);
    assert_eq!(savepoint.master_states.len(), 0);

    let op = &savepoint.operators[0];
    assert_eq!(op.parallelism, 4);
    assert_eq!(op.max_parallelism, 128);
    assert!(op.coordinator_state.is_none());
    assert_eq!(op.subtask_states.len(), 1);

    let sub = &op.subtask_states[0];
    assert_eq!(sub.subtask_index, 0);
    assert!(matches!(&sub.managed_keyed_state, StateHandle::KeyGroups(h)
        if h.start_key_group == 0 && h.num_key_groups == 2));
}

#[test]
fn parse_invalid_magic() {
    let mut data = build_minimal_metadata_v3();
    data[0] = 0xFF;
    let result = metadata::parse_metadata(&data[..]);
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("invalid magic number"), "got: {}", err);
}

#[test]
fn parse_unsupported_version() {
    let mut data = build_minimal_metadata_v3();
    data[4..8].copy_from_slice(&99u32.to_be_bytes());
    let result = metadata::parse_metadata(&data[..]);
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("unsupported metadata version"), "got: {}", err);
}
