use flink_explorer::index::cache::{
    load_cache, save_cache, CachedEntry, CachedKeyedState, CachedOperator, CachedState, IndexCache,
    KeyPartition, StateDescriptor, ValueRef,
};

fn make_test_cache() -> IndexCache {
    IndexCache {
        metadata_size: 1024,
        metadata_mtime: 1711900000,
        checkpoint_id: 42,
        savepoint_version: 5,
        master_states: vec!["coordinator (v1, 64 bytes)".to_string()],
        operators: vec![CachedOperator {
            id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            display_name: "test-operator".to_string(),
            identifier_name: "RichFlatMapFunction".to_string(),
            parallelism: 4,
            max_parallelism: 128,
            subtask_count: 4,
            keyed_state_size: 8192,
            fully_finished: false,
            has_coordinator_state: false,
            coordinator_info: None,
            states: vec![CachedState {
                name: "managed-operator-subtask0".to_string(),
                state_type: "managed-operator".to_string(),
                entries: vec![CachedEntry {
                    key: b"offset-state".to_vec(),
                    key_group: 0,
                    value_ref: ValueRef::File {
                        path: "shared/sst-001".to_string(),
                        offset: 0,
                    },
                }],
            }],
            keyed_state: Some(CachedKeyedState {
                descriptors: vec![
                    StateDescriptor {
                        name: "my-value-state".to_string(),
                        state_type: "VALUE".to_string(),
                        value_type: "Int".to_string(),
                        pojo_detail: String::new(),
                        pojo_info: None,
                        entry_count: 0,
                    },
                    StateDescriptor {
                        name: "my-map-state".to_string(),
                        state_type: "MAP".to_string(),
                        value_type: "byte[]".to_string(),
                        pojo_detail: " → MyPojo { field: ? }".to_string(),
                        pojo_info: None,
                        entry_count: 0,
                    },
                ],
                partitions: vec![KeyPartition {
                    key: "key-1".to_string(),
                    key_group: 0,
                    values: vec![
                        Some(vec![0x00, 0x00, 0x00, 0x2A]), // Int = 42
                        None,                               // MAP: not extracted
                    ],
                }],
                total_keys: 1,
                key_type: "String".to_string(),
            }),
        }],
    }
}

#[test]
fn cache_round_trip() {
    let cache = make_test_cache();
    let dir = std::env::temp_dir().join("flink-explorer-test-cache");
    let _ = std::fs::create_dir_all(&dir);

    save_cache(&cache, &dir).unwrap();
    let loaded = load_cache(&dir, 1024, 1711900000).unwrap();
    assert_eq!(cache, loaded);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cache_stale_detection() {
    let cache = make_test_cache();
    let dir = std::env::temp_dir().join("flink-explorer-test-stale");
    let _ = std::fs::create_dir_all(&dir);

    save_cache(&cache, &dir).unwrap();
    let result = load_cache(&dir, 1024, 9999999);
    assert!(result.is_err());

    let result = load_cache(&dir, 2048, 1711900000);
    assert!(result.is_err());

    let _ = std::fs::remove_dir_all(&dir);
}
