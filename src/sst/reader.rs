use std::path::Path;

use crate::error::SstError;

/// A list of key-value byte pairs returned from SST scans.
pub type KvPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Wrapper around a read-only RocksDB instance for SST file access.
pub struct SstReader {
    db: rocksdb::DB,
}

impl SstReader {
    /// Open a RocksDB database directory in read-only mode.
    pub fn open(path: &Path) -> Result<Self, SstError> {
        let mut opts = rocksdb::Options::default();
        opts.set_error_if_exists(false);
        let db =
            rocksdb::DB::open_for_read_only(&opts, path, false).map_err(|e| SstError::Open {
                path: path.to_path_buf(),
                msg: e.to_string(),
            })?;
        Ok(Self { db })
    }

    /// Get a single value by exact key.
    pub fn get_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SstError> {
        self.db.get(key).map_err(|e| SstError::Read(e.to_string()))
    }

    /// Scan all keys matching a prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KvPairs, SstError> {
        let iter = self.db.prefix_iterator(prefix);
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| SstError::Read(e.to_string()))?;
            if !key.starts_with(prefix) {
                break;
            }
            results.push((key.to_vec(), value.to_vec()));
        }
        Ok(results)
    }

    /// Scan keys in a byte range [start, end).
    pub fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvPairs, SstError> {
        let mut iter = self.db.raw_iterator();
        iter.seek(start);
        let mut results = Vec::new();
        while iter.valid() {
            if let Some(key) = iter.key() {
                if key >= end {
                    break;
                }
                let value = iter.value().unwrap_or(&[]);
                results.push((key.to_vec(), value.to_vec()));
            }
            iter.next();
        }
        iter.status().map_err(|e| SstError::Read(e.to_string()))?;
        Ok(results)
    }
}
