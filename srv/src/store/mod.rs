//! Store Module.

pub mod arc;
pub mod error;
pub mod keydir;
pub mod storage;

mod format;
mod lockfile;
mod logfile;
mod settings;

use keydir::HashmapKeydir;
use storage::DiskStorage;

#[derive(Debug, Copy, Clone)]
pub struct StoreOptions {
    pub(crate) max_log_file_size: u64,

    // sync data to storage after each writting operation.
    // we should balance data reliability and writting performance.
    pub(crate) sync: bool,

    pub(crate) max_key_size: u64,

    pub(crate) max_value_size: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_log_file_size: settings::DEFAULT_MAX_DATA_FILE_SIZE, // 100MB
            sync: false, // SyncStrategy::Interval(100),    // 100s
            max_key_size: settings::DEFAULT_MAX_KEY_SIZE,
            max_value_size: settings::DEFAULT_MAX_VALUE_SIZE,
        }
    }
}

pub type Store = DiskStorage<HashmapKeydir>;

pub use arc::{BitCask, OpenOptions};
