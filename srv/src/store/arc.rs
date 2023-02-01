//! Arc Store.

use std::sync::{Arc, RwLock};

use log::info;

use super::error::Result;
use super::storage::Storage;
use super::{Store, StoreOptions};

/// Build custom open options.
#[derive(Debug)]
pub struct OpenOptions(StoreOptions);

impl OpenOptions {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self(StoreOptions::default())
    }

    #[allow(dead_code)]
    pub fn max_log_file_size(mut self, value: u64) -> Self {
        self.0.max_log_file_size = value;
        self
    }

    #[allow(dead_code)]
    pub fn sync(mut self, value: bool) -> Self {
        self.0.sync = value;
        self
    }

    #[allow(dead_code)]
    pub fn max_value_size(mut self, value: u64) -> Self {
        self.0.max_value_size = value;
        self
    }

    #[allow(dead_code)]
    pub fn max_key_size(mut self, value: u64) -> Self {
        self.0.max_key_size = value;
        self
    }

    #[allow(dead_code)]
    pub fn open(&self, path: impl AsRef<std::path::Path>) -> Result<BitCask> {
        BitCask::open_with_options(path, self.0)
    }
}

/// Store handler for multiple threads.
#[derive(Debug)]
pub struct BitCask {
    inner: Arc<RwLock<Store>>,
}

impl BitCask {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::open_with_options(path, StoreOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<std::path::Path>,
        opts: StoreOptions,
    ) -> Result<Self> {
        let path = path.as_ref();

        let disk_storage = RwLock::new(Store::open_with_options(path, opts)?);
        Ok(Self {
            inner: Arc::new(disk_storage),
        })
    }
}

impl Clone for BitCask {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Storage for BitCask {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut store = self.inner.write().unwrap();
        store.get(key)
    }

    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let mut store = self.inner.write().unwrap();
        store.set(key, value)
    }

    fn close(&mut self) -> Result<()> {
        let mut store = self.inner.write().unwrap();
        store.close()
    }

    fn compact(&mut self) -> Result<()> {
        let mut store = self.inner.write().unwrap();
        store.compact()
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        let store = self.inner.read().unwrap();
        store.contains_key(key)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let mut store = self.inner.write().unwrap();
        store.delete(key)
    }

    fn is_empty(&self) -> bool {
        let store = self.inner.read().unwrap();
        store.is_empty()
    }

    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut store = self.inner.write().unwrap();
        store.for_each(f)
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let store = self.inner.read().unwrap();
        store.keys()
    }

    fn len(&self) -> u64 {
        let store = self.inner.read().unwrap();
        store.len()
    }

    fn sync(&mut self) -> Result<()> {
        let mut store = self.inner.write().unwrap();
        store.sync()
    }
}

impl Drop for BitCask {
    fn drop(&mut self) {
        info!("bitcask dropped...");
    }
}
