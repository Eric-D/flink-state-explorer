use std::io::Read;

use crate::error::ParseError;
use crate::parser::java_deser::JavaReader;

// ── StreamStateHandle ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum StreamStateHandle {
    Null,
    ByteStream(ByteStreamStateHandle),
    File(FileStateHandle),
    Relative(RelativeFileStateHandle),
    Segment(SegmentFileStateHandle),
    EmptySegment,
    /// KeyGroupsStateHandle can appear as a StreamStateHandle (tag 3).
    KeyGroups(Box<KeyGroupsStateHandle>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ByteStreamStateHandle {
    pub handle_name: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileStateHandle {
    pub file_path: String,
    pub state_size: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelativeFileStateHandle {
    pub relative_path: String,
    pub state_size: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentFileStateHandle {
    pub start_pos: i64,
    pub state_size: i64,
    pub scope: i32,
    pub file_path: String,
    pub logical_file_id: String,
}

/// Tags for StreamStateHandle dispatch.
const SSH_NULL: u8 = 0;
const SSH_BYTE_STREAM: u8 = 1;
const SSH_FILE: u8 = 2;
const SSH_KEY_GROUPS: u8 = 3;
const SSH_RELATIVE: u8 = 6;
const SSH_SEGMENT: u8 = 15;
const SSH_EMPTY_SEGMENT: u8 = 16;

pub fn read_stream_state_handle<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<StreamStateHandle, ParseError> {
    let tag = reader.read_byte()?;
    match tag {
        SSH_NULL => Ok(StreamStateHandle::Null),
        SSH_BYTE_STREAM => {
            let handle_name = reader.read_utf()?;
            let data_len = reader.read_int()?;
            let data = if data_len > 0 {
                reader.read_bytes(data_len as usize)?
            } else {
                Vec::new()
            };
            Ok(StreamStateHandle::ByteStream(ByteStreamStateHandle {
                handle_name,
                data,
            }))
        }
        SSH_FILE => {
            let state_size = reader.read_long()?;
            let file_path = reader.read_utf()?;
            Ok(StreamStateHandle::File(FileStateHandle {
                file_path,
                state_size,
            }))
        }
        SSH_RELATIVE => {
            let relative_path = reader.read_utf()?;
            let state_size = reader.read_long()?;
            Ok(StreamStateHandle::Relative(RelativeFileStateHandle {
                relative_path,
                state_size,
            }))
        }
        SSH_KEY_GROUPS => {
            // KeyGroupsStateHandle appearing as StreamStateHandle (recursive)
            let kg = read_key_groups_inner(reader)?;
            Ok(StreamStateHandle::KeyGroups(Box::new(kg)))
        }
        SSH_SEGMENT => {
            let start_pos = reader.read_long()?;
            let state_size = reader.read_long()?;
            let scope = reader.read_int()?;
            let file_path = reader.read_utf()?;
            let logical_file_id = reader.read_utf()?;
            Ok(StreamStateHandle::Segment(SegmentFileStateHandle {
                start_pos,
                state_size,
                scope,
                file_path,
                logical_file_id,
            }))
        }
        SSH_EMPTY_SEGMENT => Ok(StreamStateHandle::EmptySegment),
        _ => Err(ParseError::UnknownStreamHandleTag {
            tag,
            position: reader.position() - 1,
        }),
    }
}

// ── KeyedStateHandle ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum StateHandle {
    Null,
    OperatorStream(OperatorStreamStateHandle),
    KeyGroups(KeyGroupsStateHandle),
    IncrementalRemoteKeyed(IncrementalRemoteKeyedStateHandle),
    Changelog(ChangelogHandle),
    ChangelogByteIncrement(ChangelogByteIncrementHandle),
    ChangelogFileIncrement(ChangelogFileIncrementHandle),
}

// Tag constants for keyed state handles
const KSH_NULL: u8 = 0;
const KSH_KEY_GROUPS: u8 = 3;
const KSH_INCREMENTAL: u8 = 5;
const KSH_SAVEPOINT_KEY_GROUPS: u8 = 7;
const KSH_CHANGELOG: u8 = 8;
const KSH_CHANGELOG_BYTE: u8 = 9;
const KSH_CHANGELOG_FILE: u8 = 10;
const KSH_INCREMENTAL_V2: u8 = 11;
const KSH_KEY_GROUPS_V2: u8 = 12;
const KSH_CHANGELOG_FILE_V2: u8 = 13;
const KSH_CHANGELOG_V2: u8 = 14;

#[derive(Debug, Clone, PartialEq)]
pub struct KeyGroupsStateHandle {
    pub start_key_group: i32,
    pub num_key_groups: i32,
    pub offsets: Vec<i64>,
    pub delegate_handle: StreamStateHandle,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HandleAndLocalPath {
    pub local_path: String,
    pub handle: StreamStateHandle,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IncrementalRemoteKeyedStateHandle {
    pub checkpoint_id: i64,
    pub backend_id: String,
    pub start_key_group: i32,
    pub num_key_groups: i32,
    pub checkpointed_size: i64,
    pub meta_state_handle: StreamStateHandle,
    pub shared_state: Vec<HandleAndLocalPath>,
    pub private_state: Vec<HandleAndLocalPath>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangelogHandle {
    pub start_key_group: i32,
    pub num_key_groups: i32,
    pub checkpointed_size: i64,
    pub base_state: Vec<StateHandle>,
    pub delta_state: Vec<StateHandle>,
    pub materialization_id: i64,
    pub checkpoint_id: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangelogByteIncrementHandle {
    pub start_key_group: i32,
    pub num_key_groups: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangelogFileIncrementHandle {
    pub start_key_group: i32,
    pub num_key_groups: i32,
    pub handles_and_offsets: Vec<(i64, StreamStateHandle)>,
    pub size: i64,
    pub checkpointed_size: i64,
}

pub fn read_keyed_state_handle<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<StateHandle, ParseError> {
    let tag = reader.read_byte()?;
    match tag {
        KSH_NULL => Ok(StateHandle::Null),
        KSH_KEY_GROUPS | KSH_KEY_GROUPS_V2 | KSH_SAVEPOINT_KEY_GROUPS => {
            let kg = read_key_groups_inner(reader)?;
            // tag 12 has an extra StateHandleID
            if tag == KSH_KEY_GROUPS_V2 {
                let _state_handle_id = reader.read_utf()?;
            }
            Ok(StateHandle::KeyGroups(kg))
        }
        KSH_INCREMENTAL | KSH_INCREMENTAL_V2 => {
            let is_v2 = tag == KSH_INCREMENTAL_V2;
            let checkpoint_id = reader.read_long()?;
            let backend_id = reader.read_utf()?;
            let start_key_group = reader.read_int()?;
            let num_key_groups = reader.read_int()?;
            let checkpointed_size = if is_v2 {
                reader.read_long()?
            } else {
                -1 // UNKNOWN
            };
            let meta_state_handle = read_stream_state_handle(reader)?;
            let shared_state = read_handle_and_local_path_list(reader)?;
            let private_state = read_handle_and_local_path_list(reader)?;
            if is_v2 {
                let _state_handle_id = reader.read_utf()?;
            }
            Ok(StateHandle::IncrementalRemoteKeyed(
                IncrementalRemoteKeyedStateHandle {
                    checkpoint_id,
                    backend_id,
                    start_key_group,
                    num_key_groups,
                    checkpointed_size,
                    meta_state_handle,
                    shared_state,
                    private_state,
                },
            ))
        }
        KSH_CHANGELOG | KSH_CHANGELOG_V2 => {
            let is_v2 = tag == KSH_CHANGELOG_V2;
            let start_key_group = reader.read_int()?;
            let num_key_groups = reader.read_int()?;
            let checkpointed_size = reader.read_long()?;
            let base_size = reader.read_int()?;
            let mut base_state = Vec::with_capacity(base_size as usize);
            for _ in 0..base_size {
                base_state.push(read_keyed_state_handle(reader)?);
            }
            let delta_size = reader.read_int()?;
            let mut delta_state = Vec::with_capacity(delta_size as usize);
            for _ in 0..delta_size {
                delta_state.push(read_keyed_state_handle(reader)?);
            }
            let materialization_id = reader.read_long()?;
            let checkpoint_id = if is_v2 {
                reader.read_long()?
            } else {
                materialization_id
            };
            let _state_handle_id = reader.read_utf()?;
            Ok(StateHandle::Changelog(ChangelogHandle {
                start_key_group,
                num_key_groups,
                checkpointed_size,
                base_state,
                delta_state,
                materialization_id,
                checkpoint_id,
            }))
        }
        KSH_CHANGELOG_BYTE => {
            let start_key_group = reader.read_int()?;
            let num_key_groups = reader.read_int()?;
            let _from = reader.read_long()?;
            let _to = reader.read_long()?;
            let size = reader.read_int()?;
            for _ in 0..size {
                let _key_group = reader.read_int()?;
                let bytes_size = reader.read_int()?;
                reader.skip(bytes_size as usize)?;
            }
            let _state_handle_id = reader.read_utf()?;
            Ok(StateHandle::ChangelogByteIncrement(
                ChangelogByteIncrementHandle {
                    start_key_group,
                    num_key_groups,
                },
            ))
        }
        KSH_CHANGELOG_FILE | KSH_CHANGELOG_FILE_V2 => {
            let is_v2 = tag == KSH_CHANGELOG_FILE_V2;
            let start_key_group = reader.read_int()?;
            let num_key_groups = reader.read_int()?;
            let num_handles = reader.read_int()?;
            let mut handles_and_offsets = Vec::with_capacity(num_handles as usize);
            for _ in 0..num_handles {
                let offset = reader.read_long()?;
                let handle = read_stream_state_handle(reader)?;
                handles_and_offsets.push((offset, handle));
            }
            let size = reader.read_long()?;
            let checkpointed_size = reader.read_long()?;
            let _state_handle_id = reader.read_utf()?;
            if is_v2 {
                let _storage_identifier = reader.read_utf()?;
            }
            Ok(StateHandle::ChangelogFileIncrement(
                ChangelogFileIncrementHandle {
                    start_key_group,
                    num_key_groups,
                    handles_and_offsets,
                    size,
                    checkpointed_size,
                },
            ))
        }
        _ => Err(ParseError::UnknownStateHandleTag {
            tag,
            position: reader.position() - 1,
        }),
    }
}

/// Shared logic for KeyGroupsStateHandle: startKeyGroup, numKeyGroups,
/// offsets, delegate stream handle.
fn read_key_groups_inner<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<KeyGroupsStateHandle, ParseError> {
    let start_key_group = reader.read_int()?;
    let num_key_groups = reader.read_int()?;
    let mut offsets = Vec::with_capacity(num_key_groups as usize);
    for _ in 0..num_key_groups {
        offsets.push(reader.read_long()?);
    }
    let delegate_handle = read_stream_state_handle(reader)?;
    Ok(KeyGroupsStateHandle {
        start_key_group,
        num_key_groups,
        offsets,
        delegate_handle,
    })
}

fn read_handle_and_local_path_list<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<Vec<HandleAndLocalPath>, ParseError> {
    let count = reader.read_int()?;
    let mut list = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let local_path = reader.read_utf()?;
        let handle = read_stream_state_handle(reader)?;
        list.push(HandleAndLocalPath { local_path, handle });
    }
    Ok(list)
}

// ── OperatorStateHandle ─────────────────────────────────────────────

const OSH_PARTITIONABLE: u8 = 4;
const OSH_SEGMENT_PARTITIONABLE: u8 = 17;

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionOffsetEntry {
    pub state_name: String,
    pub mode: i32,
    pub offsets: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperatorStreamStateHandle {
    pub partition_offsets: Vec<PartitionOffsetEntry>,
    pub delegate_handle: StreamStateHandle,
}

pub fn read_operator_state_handle<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<StateHandle, ParseError> {
    let tag = reader.read_byte()?;
    match tag {
        0 => Ok(StateHandle::Null),
        OSH_PARTITIONABLE | OSH_SEGMENT_PARTITIONABLE => read_operator_stream_state_handle(reader),
        _ => Err(ParseError::UnknownStateHandleTag {
            tag,
            position: reader.position() - 1,
        }),
    }
}

fn read_operator_stream_state_handle<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<StateHandle, ParseError> {
    let num_partitions = reader.read_int()?;
    let mut partition_offsets = Vec::with_capacity(num_partitions as usize);
    for _ in 0..num_partitions {
        let state_name = reader.read_utf()?;
        let mode = reader.read_byte()? as i32; // Distribution mode: writeByte (not writeInt)
        let num_offsets = reader.read_int()?;
        let mut offsets = Vec::with_capacity(num_offsets as usize);
        for _ in 0..num_offsets {
            offsets.push(reader.read_long()?);
        }
        partition_offsets.push(PartitionOffsetEntry {
            state_name,
            mode,
            offsets,
        });
    }
    let delegate_handle = read_stream_state_handle(reader)?;
    Ok(StateHandle::OperatorStream(OperatorStreamStateHandle {
        partition_offsets,
        delegate_handle,
    }))
}

// Re-export OperatorStream variant for metadata.rs
impl StateHandle {
    pub fn as_operator_stream(&self) -> Option<&OperatorStreamStateHandle> {
        match self {
            StateHandle::OperatorStream(h) => Some(h),
            _ => None,
        }
    }
}
