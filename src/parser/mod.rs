pub mod java_deser;
pub mod kafka;
pub mod keyed_state;
pub mod metadata;
pub mod pojo;
pub mod state_handle;
pub mod value;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::ParseError;

pub use metadata::Savepoint;

/// Parse a Flink savepoint `_metadata` file at the given path.
pub fn parse_metadata(path: &Path) -> Result<Savepoint, ParseError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    metadata::parse_metadata(reader)
}
