use std::io::Read;

use crate::error::ParseError;
use crate::parser::java_deser::JavaReader;
use crate::parser::state_handle::{
    read_keyed_state_handle, read_operator_state_handle, read_stream_state_handle, StateHandle,
    StreamStateHandle,
};

/// Flink `Checkpoints.HEADER_MAGIC_NUMBER` — magic at the start of every checkpoint/savepoint
/// `_metadata` file.
const HEADER_MAGIC_NUMBER: u32 = 0x4960_672D;
/// Flink `MetadataV2V3SerializerBase.MASTER_STATE_MAGIC_NUMBER` — written before each master state.
const MASTER_STATE_MAGIC_NUMBER: u32 = 0xC96B_1696;

/// Supported metadata versions: v3 (Flink 1.11+), v4 (Flink 1.20 default),
/// v5 (post-1.20, operator names)
const MIN_SUPPORTED_VERSION: u32 = 3;
const MAX_SUPPORTED_VERSION: u32 = 5;

#[derive(Debug, Clone)]
pub struct Savepoint {
    pub version: u32,
    pub checkpoint_id: i64,
    pub operators: Vec<Operator>,
    pub master_states: Vec<MasterState>,
}

#[derive(Debug, Clone)]
pub struct Operator {
    pub id: [u8; 16],
    /// Human-readable operator name (v5+, empty for older versions)
    pub name: String,
    /// Operator identifier name (v5+, empty for older versions)
    pub identifier_name: String,
    pub parallelism: i32,
    pub max_parallelism: i32,
    pub coordinator_state: Option<Vec<u8>>,
    pub subtask_states: Vec<SubtaskState>,
    pub fully_finished: bool,
}

impl Operator {
    /// Display name: use v5 operator name if available, else hex ID.
    pub fn display_name(&self) -> String {
        if !self.name.is_empty() {
            self.name.clone()
        } else if !self.identifier_name.is_empty() {
            self.identifier_name.clone()
        } else {
            let upper = u64::from_be_bytes(self.id[..8].try_into().unwrap_or([0; 8]));
            let lower = u64::from_be_bytes(self.id[8..].try_into().unwrap_or([0; 8]));
            format!("{:016x}{:016x}", upper, lower)
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubtaskState {
    pub subtask_index: i32,
    pub managed_operator_state: Option<StateHandle>,
    pub raw_operator_state: Option<StateHandle>,
    pub managed_keyed_state: StateHandle,
    pub raw_keyed_state: StateHandle,
    pub input_channel_state: Vec<InputChannelStateHandle>,
    pub result_subpartition_state: Vec<ResultSubpartitionStateHandle>,
}

#[derive(Debug, Clone)]
pub struct InputChannelStateHandle {
    pub subtask_index: i32,
    pub input_gate_idx: i32,
    pub input_channel_idx: i32,
    pub offsets: Vec<i64>,
    pub state_size: i64,
    pub delegate_handle: StreamStateHandle,
}

#[derive(Debug, Clone)]
pub struct ResultSubpartitionStateHandle {
    pub subtask_index: i32,
    pub partition_idx: i32,
    pub subpartition_idx: i32,
    pub offsets: Vec<i64>,
    pub state_size: i64,
    pub delegate_handle: StreamStateHandle,
}

#[derive(Debug, Clone)]
pub struct MasterState {
    pub version: i32,
    pub name: String,
    pub data: Vec<u8>,
}

pub fn parse_metadata<R: Read>(source: R) -> Result<Savepoint, ParseError> {
    let mut reader = JavaReader::new(source);

    // Header
    let magic = reader.read_int()? as u32;
    if magic != HEADER_MAGIC_NUMBER {
        return Err(ParseError::InvalidMagic(magic));
    }

    let version = reader.read_int()? as u32;
    if !(MIN_SUPPORTED_VERSION..=MAX_SUPPORTED_VERSION).contains(&version) {
        return Err(ParseError::UnsupportedVersion(version));
    }

    let checkpoint_id = reader.read_long()?;

    // Master states come FIRST (before operator states)
    let num_master = reader.read_int()?;
    let mut master_states = Vec::with_capacity(num_master as usize);
    for _ in 0..num_master {
        master_states.push(read_master_state(&mut reader)?);
    }

    // Operator states
    let num_operators = reader.read_int()?;
    let mut operators = Vec::with_capacity(num_operators as usize);
    for _i in 0..num_operators {
        match read_operator_state(&mut reader, version) {
            Ok(op) => operators.push(op),
            Err(ParseError::UnexpectedEof(_)) => {
                // EOF during operator parsing: stop gracefully.
                // This can happen with v4+ CheckpointProperties trailing bytes
                // or when the parser encounters a state handle variant it
                // doesn't fully support yet.
                break;
            }
            Err(e) => return Err(e),
        }
    }

    // v4+: CheckpointProperties appended via Java ObjectOutputStream — skip
    // (remaining bytes are Java-serialized object data, not useful for us)

    Ok(Savepoint {
        version,
        checkpoint_id,
        operators,
        master_states,
    })
}

fn read_operator_state<R: Read>(
    reader: &mut JavaReader<R>,
    version: u32,
) -> Result<Operator, ParseError> {
    // v5+ writes operator name and identifier name before the ID
    let (name, identifier_name) = if version >= 5 {
        (reader.read_utf()?, reader.read_utf()?)
    } else {
        (String::new(), String::new())
    };

    // Operator ID: lower long first, then upper long
    let lower = reader.read_long()?;
    let upper = reader.read_long()?;
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&lower.to_be_bytes());
    id[8..].copy_from_slice(&upper.to_be_bytes());

    let parallelism = reader.read_int()?;
    let max_parallelism = reader.read_int()?;

    // Coordinator state: serialized as StreamStateHandle (v3+)
    let coordinator_state = {
        let handle = read_stream_state_handle(reader)?;
        match handle {
            StreamStateHandle::ByteStream(bs) => Some(bs.data),
            StreamStateHandle::Null => None,
            _ => None,
        }
    };

    // numSubtaskStates: if -1, operator is "fully finished" (v3+)
    let num_subtasks_raw = reader.read_int()?;
    let fully_finished = num_subtasks_raw == -1;
    let num_subtasks = if fully_finished {
        0
    } else {
        num_subtasks_raw.unsigned_abs() as usize
    };

    let mut subtask_states = Vec::with_capacity(num_subtasks);
    for _ in 0..num_subtasks {
        subtask_states.push(read_subtask_state(reader, version)?);
    }

    Ok(Operator {
        id,
        name,
        identifier_name,
        parallelism,
        max_parallelism,
        coordinator_state,
        subtask_states,
        fully_finished,
    })
}

fn read_subtask_state<R: Read>(
    reader: &mut JavaReader<R>,
    version: u32,
) -> Result<SubtaskState, ParseError> {
    let subtask_index_raw = reader.read_int()?;
    // Negative index means finished subtask: -(index+1)
    let subtask_index = if subtask_index_raw < 0 {
        -(subtask_index_raw + 1)
    } else {
        subtask_index_raw
    };
    let is_finished = subtask_index_raw < 0;

    if is_finished {
        return Ok(SubtaskState {
            subtask_index,
            managed_operator_state: None,
            raw_operator_state: None,
            managed_keyed_state: StateHandle::Null,
            raw_keyed_state: StateHandle::Null,
            input_channel_state: Vec::new(),
            result_subpartition_state: Vec::new(),
        });
    }

    // Managed operator state: int flag (0/1), then handle if present
    let managed_operator_state = {
        let has = reader.read_int()?;
        if has == 1 {
            Some(read_operator_state_handle(reader)?)
        } else {
            None
        }
    };

    // Raw operator state: int flag (0/1), then handle if present
    let raw_operator_state = {
        let has = reader.read_int()?;
        if has == 1 {
            Some(read_operator_state_handle(reader)?)
        } else {
            None
        }
    };

    // Keyed state: type tag byte (0=null), directly serialized
    let managed_keyed_state = read_keyed_state_handle(reader)?;
    let raw_keyed_state = read_keyed_state_handle(reader)?;

    // Input channel + result subpartition state (v3+)
    let mut input_channel_state = Vec::new();
    let mut result_subpartition_state = Vec::new();

    if version >= 3 {
        let num_input = reader.read_int()?;
        for _ in 0..num_input {
            input_channel_state.push(read_input_channel_handle(reader)?);
        }

        let num_result = reader.read_int()?;
        for _ in 0..num_result {
            result_subpartition_state.push(read_result_subpartition_handle(reader)?);
        }
    }

    Ok(SubtaskState {
        subtask_index,
        managed_operator_state,
        raw_operator_state,
        managed_keyed_state,
        raw_keyed_state,
        input_channel_state,
        result_subpartition_state,
    })
}

fn read_input_channel_handle<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<InputChannelStateHandle, ParseError> {
    let subtask_index = reader.read_int()?;
    let input_gate_idx = reader.read_int()?;
    let input_channel_idx = reader.read_int()?;
    let num_offsets = reader.read_int()?;
    let mut offsets = Vec::with_capacity(num_offsets as usize);
    for _ in 0..num_offsets {
        offsets.push(reader.read_long()?);
    }
    let state_size = reader.read_long()?;
    let delegate_handle = read_stream_state_handle(reader)?;
    Ok(InputChannelStateHandle {
        subtask_index,
        input_gate_idx,
        input_channel_idx,
        offsets,
        state_size,
        delegate_handle,
    })
}

fn read_result_subpartition_handle<R: Read>(
    reader: &mut JavaReader<R>,
) -> Result<ResultSubpartitionStateHandle, ParseError> {
    let subtask_index = reader.read_int()?;
    let partition_idx = reader.read_int()?;
    let subpartition_idx = reader.read_int()?;
    let num_offsets = reader.read_int()?;
    let mut offsets = Vec::with_capacity(num_offsets as usize);
    for _ in 0..num_offsets {
        offsets.push(reader.read_long()?);
    }
    let state_size = reader.read_long()?;
    let delegate_handle = read_stream_state_handle(reader)?;
    Ok(ResultSubpartitionStateHandle {
        subtask_index,
        partition_idx,
        subpartition_idx,
        offsets,
        state_size,
        delegate_handle,
    })
}

fn read_master_state<R: Read>(reader: &mut JavaReader<R>) -> Result<MasterState, ParseError> {
    // Master state magic marker
    let magic = reader.read_int()? as u32;
    if magic != MASTER_STATE_MAGIC_NUMBER {
        return Err(ParseError::InvalidMagic(magic));
    }
    // Total remaining bytes for this master state
    let num_bytes = reader.read_int()?;
    // Inner structure: version + name + payload
    let version = reader.read_int()?;
    let name = reader.read_utf()?;
    let data_len = reader.read_int()?;
    let data = reader.read_bytes(data_len as usize)?;

    // Skip any remaining bytes (numBytes includes version+name+payloadLen+payload)
    let consumed = 4 + 2 + name.len() + 4 + data_len as usize;
    if (num_bytes as usize) > consumed {
        reader.skip(num_bytes as usize - consumed)?;
    }

    Ok(MasterState {
        version,
        name,
        data,
    })
}
