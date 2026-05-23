use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::IndexError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexCache {
    pub metadata_size: u64,
    pub metadata_mtime: u64,
    pub checkpoint_id: i64,
    pub savepoint_version: u32,
    pub master_states: Vec<String>,
    pub operators: Vec<CachedOperator>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedOperator {
    pub id: [u8; 16],
    pub display_name: String,
    /// Flink operator identifier (e.g. "KeyedBroadcastProcessFunction", "RichFlatMapFunction")
    pub identifier_name: String,
    pub parallelism: i32,
    pub max_parallelism: i32,
    pub subtask_count: usize,
    /// Total keyed state data size in bytes.
    pub keyed_state_size: u64,
    pub fully_finished: bool,
    pub has_coordinator_state: bool,
    /// Decoded operator-coordinator state when recognized (e.g. a Kafka source enumerator's
    /// assigned partitions); None if absent or unrecognized.
    pub coordinator_info: Option<String>,
    /// Non-keyed states (managed-operator, raw-operator).
    pub states: Vec<CachedState>,
    /// Structured keyed state data (key-first pivot).
    pub keyed_state: Option<CachedKeyedState>,
}

impl CachedOperator {
    /// The OperatorID as a 32-char hex string (Flink's `OperatorID` form).
    pub fn operator_id_hex(&self) -> String {
        self.id.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// All extractable operator metadata as (label, value) pairs, for display/export.
    ///
    /// Note: a savepoint's metadata stores only the **OperatorID** (a hash of the uid); the
    /// operator's human name/uid appear only in metadata v5+, and the Java class is never stored.
    /// Everything else here is derived from the operator's state.
    pub fn info_lines(&self) -> Vec<(String, String)> {
        let id_hex = self.operator_id_hex();
        let unknown = "(not in this savepoint's metadata)".to_string();
        let mut v = vec![
            ("OperatorID (uuid)".to_string(), id_hex.clone()),
            (
                "Name".to_string(),
                if self.display_name != id_hex && !self.display_name.is_empty() {
                    self.display_name.clone()
                } else {
                    unknown.clone()
                },
            ),
            (
                "uid / identifier".to_string(),
                if self.identifier_name.is_empty() {
                    unknown.clone()
                } else {
                    self.identifier_name.clone()
                },
            ),
            ("Parallelism".to_string(), self.parallelism.to_string()),
            (
                "Max parallelism".to_string(),
                self.max_parallelism.to_string(),
            ),
            ("Subtasks".to_string(), self.subtask_count.to_string()),
            (
                "Coordinator state".to_string(),
                if self.has_coordinator_state {
                    "yes".to_string()
                } else {
                    "no".to_string()
                },
            ),
            (
                "Fully finished".to_string(),
                self.fully_finished.to_string(),
            ),
            (
                "Keyed state size".to_string(),
                format!("{} bytes", self.keyed_state_size),
            ),
        ];
        if let Some(info) = &self.coordinator_info {
            v.push(("Source enumerator".to_string(), info.clone()));
        }
        if let Some(ks) = &self.keyed_state {
            v.push(("Key type".to_string(), ks.key_type.clone()));
            v.push(("Keys".to_string(), ks.total_keys.to_string()));
            let names: Vec<&str> = ks.descriptors.iter().map(|d| d.name.as_str()).collect();
            v.push((
                "Keyed states".to_string(),
                format!("{} [{}]", names.len(), names.join(", ")),
            ));
        }
        if !self.states.is_empty() {
            let os: Vec<String> = self
                .states
                .iter()
                .map(|s| format!("{} ({})", s.name, s.state_type))
                .collect();
            v.push(("Operator states".to_string(), os.join(", ")));
        }
        v
    }
}

/// Structured keyed state: state descriptors + per-key partitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedKeyedState {
    pub descriptors: Vec<StateDescriptor>,
    pub partitions: Vec<KeyPartition>,
    /// Total number of unique keys (may be > partitions.len() if truncated)
    pub total_keys: usize,
    /// Key serializer type (e.g. "String", "Pojo", "Long")
    pub key_type: String,
}

/// Metadata for a single registered keyed state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateDescriptor {
    pub name: String,
    pub state_type: String,
    pub value_type: String,
    pub pojo_detail: String,
    /// Number of raw entries found for this state (debug info).
    pub entry_count: usize,
    /// POJO field structure for value deserialization (None if not a POJO).
    pub pojo_info: Option<CachedPojoInfo>,
}

/// Serializable POJO field structure (mirrors parser::pojo::PojoInfo).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedPojoInfo {
    pub class_name: String,
    pub fields: Vec<CachedPojoField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedPojoField {
    pub name: String,
    pub type_name: String,
    pub nested: Option<Box<CachedPojoInfo>>,
}

/// One keyBy value with its raw state values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyPartition {
    pub key: String,
    pub key_group: u16,
    /// Raw value bytes per descriptor index.
    /// `None` = not extracted, `Some(vec![])` = null/empty value.
    pub values: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedState {
    pub name: String,
    pub state_type: String,
    pub entries: Vec<CachedEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedEntry {
    pub key: Vec<u8>,
    pub key_group: u16,
    pub value_ref: ValueRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValueRef {
    /// Value lives in an external file (SST or state file).
    File { path: String, offset: u64 },
    /// Value is inline in the metadata (canonical savepoint format).
    Inline { data: Vec<u8> },
}

const CACHE_FILE_NAME: &str = ".flink-explorer-cache";

/// Save the index cache to disk next to the savepoint directory.
pub fn save_cache(cache: &IndexCache, savepoint_dir: &Path) -> Result<(), IndexError> {
    let cache_path = savepoint_dir.join(CACHE_FILE_NAME);
    let encoded = postcard::to_allocvec(cache).map_err(|e| IndexError::Encode(e.to_string()))?;
    fs::write(&cache_path, &encoded).map_err(|e| IndexError::WriteCache {
        path: cache_path,
        source: e,
    })?;
    Ok(())
}

/// Load the index cache from disk. Returns `StaleCache` if metadata stats
/// don't match.
pub fn load_cache(
    savepoint_dir: &Path,
    current_metadata_size: u64,
    current_metadata_mtime: u64,
) -> Result<IndexCache, IndexError> {
    let cache_path = savepoint_dir.join(CACHE_FILE_NAME);
    let data = fs::read(&cache_path).map_err(|e| IndexError::ReadCache {
        path: cache_path.clone(),
        source: e,
    })?;
    let cache: IndexCache =
        postcard::from_bytes(&data).map_err(|e| IndexError::Decode(e.to_string()))?;

    if cache.metadata_size != current_metadata_size
        || cache.metadata_mtime != current_metadata_mtime
    {
        return Err(IndexError::StaleCache);
    }

    Ok(cache)
}
