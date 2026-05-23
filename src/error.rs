use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid magic number: expected 0x4960672D, got 0x{0:08X}")]
    InvalidMagic(u32),
    #[error("unsupported metadata version: {0}")]
    UnsupportedVersion(u32),
    #[error("unknown state handle tag: {tag} at position {position}")]
    UnknownStateHandleTag { tag: u8, position: u64 },
    #[error("unknown stream state handle tag: {tag} at position {position}")]
    UnknownStreamHandleTag { tag: u8, position: u64 },
    #[error("unexpected end of data at offset {0}")]
    UnexpectedEof(u64),
    #[error("invalid UTF-8 in metadata at offset {offset}: {source}")]
    InvalidUtf8 {
        offset: u64,
        source: std::string::FromUtf8Error,
    },
    #[error("I/O error reading metadata: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("failed to read index cache at {path}: {source}")]
    ReadCache {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to write index cache at {path}: {source}")]
    WriteCache {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("index cache is stale (metadata changed since last index)")]
    StaleCache,
    #[error("failed to decode index cache: {0}")]
    Decode(String),
    #[error("failed to encode index cache: {0}")]
    Encode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SstError {
    #[error("failed to open SST database at {path}: {msg}")]
    Open { path: PathBuf, msg: String },
    #[error("failed to read key from SST: {0}")]
    Read(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("failed to compile proto files: {0}")]
    Compile(String),
    #[error("unknown message type: {0}")]
    UnknownType(String),
    #[error("failed to decode protobuf message: {0}")]
    Decode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("profile not found: {0}")]
    NotFound(String),
    #[error("failed to read profile: {0}")]
    Read(#[source] std::io::Error),
    #[error("failed to write profile: {0}")]
    Write(#[source] std::io::Error),
    #[error("failed to parse profile TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to serialize profile TOML: {0}")]
    Serialize(#[from] toml::ser::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum UiError {
    #[error("terminal initialization failed: {0}")]
    Init(#[source] std::io::Error),
    #[error("terminal render failed: {0}")]
    Render(#[source] std::io::Error),
    #[error("event polling failed: {0}")]
    Event(#[source] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Sst(#[from] SstError),
    #[error(transparent)]
    Proto(#[from] ProtoError),
    #[error(transparent)]
    Profile(#[from] ProfileError),
    #[error(transparent)]
    Ui(#[from] UiError),
}
