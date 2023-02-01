//! Store Error Module.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, StoreError>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Glob(#[from] glob::GlobError),

    #[error(transparent)]
    Pattern(#[from] glob::PatternError),

    /// Custom error definitions.
    #[error("invalid bytes, cannot descrialize entry")]
    DeserializeError,

    #[error("crc check failed, data entry (key='{}', file_id={}, offset={}) was corrupted", String::from_utf8_lossy(.key), .file_id, .offset)]
    DataEntryCorrupted {
        file_id: u64,
        key: Vec<u8>,
        offset: u64,
    },

    #[error("key '{}' not found", String::from_utf8_lossy(.0))]
    KeyNotFound(Vec<u8>),

    #[error("key is too large")]
    KeyIsTooLarge,

    #[error("value is too large")]
    ValueIsTooLarge,

    #[error("file '{}' is not writeable", .0.display())]
    FileNotWriteable(std::path::PathBuf),

    #[error("db is already locked")]
    AlreadyLocked,

    #[error("{}", .0)]
    Custom(String),
}
