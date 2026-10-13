use flink_explorer::error::IndexError;
use flink_explorer::index::cache::{
    load_cache, may_save_cache, save_cache, CachedEntry, CachedKeyedState, CachedOperator,
    CachedState, IndexCache, KeyPartition, StateDescriptor, ValueRef, CACHE_FILE_NAME,
    CACHE_FORMAT,
};

fn make_test_cache() -> IndexCache {
    IndexCache {
        metadata_size: 1024,
        metadata_mtime: 1711900000,
        checkpoint_id: 42,
        savepoint_version: 5,
        cache_format: CACHE_FORMAT,
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
            load_errors: vec![],
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

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "flink-explorer-test-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Staleness is decided from the fixed header, before the body — which can run to hundreds of
/// megabytes — is decoded: with a body that cannot decode, a stale cache is still `StaleCache`,
/// and only a fresh one reaches the decoder.
#[test]
fn a_stale_cache_is_rejected_without_decoding_its_body() {
    let cache = make_test_cache();
    let dir = temp_dir("header");
    save_cache(&cache, &dir).unwrap();
    let path = dir.join(CACHE_FILE_NAME);
    let mut bytes = std::fs::read(&path).unwrap();
    let body_len = postcard::to_allocvec(&cache).unwrap().len();
    let header_len = bytes.len() - body_len;
    assert!(header_len > 0, "the file starts with a header");
    for b in &mut bytes[header_len..] {
        *b = 0xff;
    }
    std::fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        load_cache(&dir, 1024, 1711900000),
        Err(IndexError::Decode(_))
    ));
    assert!(matches!(
        load_cache(&dir, 1024, 9999999),
        Err(IndexError::StaleCache)
    ));
    assert!(matches!(
        load_cache(&dir, 2048, 1711900000),
        Err(IndexError::StaleCache)
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A cache written before the header existed — a bare postcard body — or cut short is stale, even
/// when its body would decode and match.
#[test]
fn a_cache_without_the_header_is_stale() {
    let cache = make_test_cache();
    let dir = temp_dir("headerless");
    let path = dir.join(CACHE_FILE_NAME);

    std::fs::write(&path, postcard::to_allocvec(&cache).unwrap()).unwrap();
    assert!(matches!(
        load_cache(&dir, 1024, 1711900000),
        Err(IndexError::StaleCache)
    ));

    std::fs::write(&path, b"FX").unwrap();
    assert!(matches!(
        load_cache(&dir, 1024, 1711900000),
        Err(IndexError::StaleCache)
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn only_a_complete_index_may_be_cached() {
    let mut cache = make_test_cache();
    assert!(may_save_cache(&cache, false));
    assert!(!may_save_cache(&cache, true), "--no-cache must not write");
    cache.operators[0]
        .load_errors
        .push("/sp/state-0: No such file or directory (os error 2)".to_string());
    assert!(cache.has_load_errors());
    assert!(
        !may_save_cache(&cache, false),
        "a partial index was cacheable"
    );
}
