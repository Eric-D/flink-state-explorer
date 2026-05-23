use flink_explorer::parser::java_deser::JavaReader;
use flink_explorer::parser::state_handle::*;

fn file_state_handle_bytes(path: &str, size: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(2); // FileStreamStateHandle
    buf.extend_from_slice(&size.to_be_bytes());
    let path_bytes = path.as_bytes();
    buf.extend_from_slice(&(path_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(path_bytes);
    buf
}

fn null_stream_handle_bytes() -> Vec<u8> {
    vec![0]
}

fn utf_bytes(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
    buf.extend_from_slice(b);
    buf
}

#[test]
fn parse_key_groups_state_handle_tag3() {
    let mut data = Vec::new();
    data.push(3);
    data.extend_from_slice(&0i32.to_be_bytes()); // startKeyGroup
    data.extend_from_slice(&4i32.to_be_bytes()); // numKeyGroups
    for offset in [0i64, 100, 200, 300] {
        data.extend_from_slice(&offset.to_be_bytes());
    }
    data.extend(&file_state_handle_bytes("/data/keyed", 8192));

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_keyed_state_handle(&mut reader).unwrap();

    match handle {
        StateHandle::KeyGroups(h) => {
            assert_eq!(h.start_key_group, 0);
            assert_eq!(h.num_key_groups, 4);
            assert_eq!(h.offsets, vec![0, 100, 200, 300]);
        }
        other => panic!("expected KeyGroups, got {:?}", other),
    }
}

#[test]
fn parse_savepoint_key_groups_tag7() {
    let mut data = Vec::new();
    data.push(7); // SAVEPOINT_KEY_GROUPS_HANDLE
    data.extend_from_slice(&0i32.to_be_bytes());
    data.extend_from_slice(&2i32.to_be_bytes());
    data.extend_from_slice(&0i64.to_be_bytes());
    data.extend_from_slice(&50i64.to_be_bytes());
    data.extend(&null_stream_handle_bytes());

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_keyed_state_handle(&mut reader).unwrap();
    assert!(matches!(handle, StateHandle::KeyGroups(_)));
}

#[test]
fn parse_incremental_remote_keyed_tag5() {
    let mut data = Vec::new();
    data.push(5);
    data.extend_from_slice(&42i64.to_be_bytes()); // checkpointId
    data.extend(&utf_bytes("backend-1")); // backendId
    data.extend_from_slice(&0i32.to_be_bytes()); // startKeyGroup
    data.extend_from_slice(&128i32.to_be_bytes()); // numKeyGroups
                                                   // metaStateHandle
    data.extend(&null_stream_handle_bytes());
    // sharedStates: 1 entry
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(&utf_bytes("shared/sst1")); // localPath
    data.extend(&file_state_handle_bytes("/data/sst1", 1024));
    // privateStates: 0
    data.extend_from_slice(&0i32.to_be_bytes());

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_keyed_state_handle(&mut reader).unwrap();

    match handle {
        StateHandle::IncrementalRemoteKeyed(h) => {
            assert_eq!(h.checkpoint_id, 42);
            assert_eq!(h.backend_id, "backend-1");
            assert_eq!(h.shared_state.len(), 1);
            assert_eq!(h.private_state.len(), 0);
        }
        other => panic!("expected IncrementalRemoteKeyed, got {:?}", other),
    }
}

#[test]
fn parse_changelog_tag8() {
    let mut data = Vec::new();
    data.push(8);
    data.extend_from_slice(&0i32.to_be_bytes()); // startKeyGroup
    data.extend_from_slice(&64i32.to_be_bytes()); // numKeyGroups
    data.extend_from_slice(&1024i64.to_be_bytes()); // checkpointedSize
                                                    // 1 base state (KeyGroupsStateHandle)
    data.extend_from_slice(&1i32.to_be_bytes());
    {
        data.push(3);
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes());
        data.extend_from_slice(&50i64.to_be_bytes());
        data.extend(&null_stream_handle_bytes());
    }
    // 0 delta states
    data.extend_from_slice(&0i32.to_be_bytes());
    // materializationId
    data.extend_from_slice(&99i64.to_be_bytes());
    // stateHandleId
    data.extend(&utf_bytes("changelog-id"));

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_keyed_state_handle(&mut reader).unwrap();

    match handle {
        StateHandle::Changelog(h) => {
            assert_eq!(h.base_state.len(), 1);
            assert_eq!(h.materialization_id, 99);
        }
        other => panic!("expected Changelog, got {:?}", other),
    }
}

#[test]
fn parse_unknown_keyed_state_handle_tag() {
    let data = [99u8];
    let mut reader = JavaReader::new(&data[..]);
    assert!(read_keyed_state_handle(&mut reader).is_err());
}

#[test]
fn parse_unknown_operator_state_handle_tag() {
    let data = [77u8];
    let mut reader = JavaReader::new(&data[..]);
    assert!(read_operator_state_handle(&mut reader).is_err());
}

#[test]
fn parse_null_stream_handle() {
    let data = [0u8];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(
        read_stream_state_handle(&mut reader).unwrap(),
        StreamStateHandle::Null
    );
}

#[test]
fn parse_byte_stream_state_handle_tag1() {
    let mut data = Vec::new();
    data.push(1);
    data.extend(&utf_bytes("inline-data"));
    data.extend_from_slice(&3i32.to_be_bytes());
    data.extend_from_slice(&[0xAA, 0xBB, 0xCC]);

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_stream_state_handle(&mut reader).unwrap();

    match handle {
        StreamStateHandle::ByteStream(h) => {
            assert_eq!(h.handle_name, "inline-data");
            assert_eq!(h.data, vec![0xAA, 0xBB, 0xCC]);
        }
        other => panic!("expected ByteStream, got {:?}", other),
    }
}

#[test]
fn parse_file_state_handle_tag2() {
    let mut data = Vec::new();
    data.push(2);
    data.extend_from_slice(&4096i64.to_be_bytes());
    data.extend(&utf_bytes("/data/state-file"));

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_stream_state_handle(&mut reader).unwrap();

    match handle {
        StreamStateHandle::File(h) => {
            assert_eq!(h.file_path, "/data/state-file");
            assert_eq!(h.state_size, 4096);
        }
        other => panic!("expected File, got {:?}", other),
    }
}

#[test]
fn parse_relative_state_handle_tag6() {
    let mut data = Vec::new();
    data.push(6);
    data.extend(&utf_bytes("shared/sst-001"));
    data.extend_from_slice(&2048i64.to_be_bytes());

    let mut reader = JavaReader::new(&data[..]);
    let handle = read_stream_state_handle(&mut reader).unwrap();

    match handle {
        StreamStateHandle::Relative(h) => {
            assert_eq!(h.relative_path, "shared/sst-001");
            assert_eq!(h.state_size, 2048);
        }
        other => panic!("expected Relative, got {:?}", other),
    }
}
