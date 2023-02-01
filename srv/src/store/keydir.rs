//! Keydir implementation.
//!
//! Keydir in an in-memory structure that maps all keys to their
//! corresponding locations on the disk.

use std::collections::HashMap;
// use std::hash::Hash;
// use std::sync::{Arc, RwLock};

use super::error::Result;
use super::format::DataEntry;

/// Keydir entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeydirEntry {
    /// file id the entry stored.
    pub file_id: u64,

    /// offset in the file.
    pub offset: u64,

    /// size of the entry in bytes.
    pub size: u64,

    /// timestamp of the record.
    pub timestamp: u32,
}

impl KeydirEntry {
    pub fn new(file_id: u64, offset: u64, size: u64, timestamp: u32) -> Self {
        Self {
            file_id,
            offset,
            size,
            timestamp,
        }
    }
}

impl From<&DataEntry> for KeydirEntry {
    fn from(v: &DataEntry) -> Self {
        KeydirEntry {
            file_id: v.file_id.unwrap(),
            offset: v.offset.unwrap(),
            size: v.size(),
            timestamp: v.timestamp(),
        }
    }
}

/// Keydir methods.
pub trait Keydir: Default {
    /// Returns a reference to corresponding entry.
    fn get(&self, key: &[u8]) -> Option<&KeydirEntry>;

    /// Puts a key and entry into the keydir.
    fn put(&mut self, key: Vec<u8>, entry: KeydirEntry) -> &KeydirEntry;

    /// Removes a key and entry from the keydir.
    fn remove(&mut self, key: &[u8]);

    /// List all keys in the keydir.
    fn keys(&self) -> Vec<Vec<u8>>;

    /// Iterate all keys in datastore and call function `f`
    /// for each entry.
    ///
    /// If function `f` returns an `Err`, it stops iteration
    /// and propagates the `Err` to the caller.
    ///
    /// You can continue iteration manually by return `Ok(true)`,
    /// or stop iteration by returning `Ok(false)`.
    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&Vec<u8>, &mut KeydirEntry) -> Result<bool>;

    /// length of the keys in the keydir
    fn len(&self) -> u64;

    /// Return `true` if datastore contains the given key.
    fn contains_key(&self, key: &[u8]) -> bool;
}

/// Keydir represented as a hashmap.
#[derive(Debug, Default)]
pub struct HashmapKeydir {
    /// mapping from a key to its keydir entry.
    mapping: HashMap<Vec<u8>, KeydirEntry>,
    // with rwlock
    // rwlock: Arc<RwLock<()>>,
}

impl Keydir for HashmapKeydir {
    fn get(&self, key: &[u8]) -> Option<&KeydirEntry> {
        // let _read_lock = self.rwlock.read().unwrap();
        self.mapping.get(key)
    }

    fn put(&mut self, key: Vec<u8>, entry: KeydirEntry) -> &KeydirEntry {
        // let _write_lock = self.rwlock.write().unwrap();
        self.mapping
            .entry(key)
            .and_modify(|e| {
                if e.timestamp <= entry.timestamp {
                    *e = entry.clone();
                }
            })
            .or_insert(entry)
    }

    fn remove(&mut self, key: &[u8]) {
        // let _write_lock = self.rwlock.write().unwrap();
        self.mapping.remove(key);
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        // let _read_lock = self.rwlock.read().unwrap();
        self.mapping.keys().cloned().collect()
    }

    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&Vec<u8>, &mut KeydirEntry) -> Result<bool>,
    {
        for (k, v) in self.mapping.iter_mut() {
            if f(k, v)? {
                break;
            }
        }

        Ok(())
    }

    fn len(&self) -> u64 {
        self.mapping.len() as u64
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.mapping.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_if_newer_inserts_when_nonexistent() {
        let mut k = HashmapKeydir::default();
        let entry = KeydirEntry::new(0, 42, 0, 0);
        let e = k.put(b"foo".to_vec(), entry.clone());
        assert!(e == &entry, "Expected {:?}, got {:?}", &entry, e);
    }
}
