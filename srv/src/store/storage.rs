//! Store Module.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use glob::glob;
use log::{debug, info, trace};

use super::error::{Result, StoreError};
use super::format::DataEntry;
use super::keydir::{Keydir, KeydirEntry};

use super::lockfile::Lockfile;
use super::logfile::{DataFile, HintFile};
use super::settings;
use super::StoreOptions;

/// Store implementation methods.
pub trait Storage {
    /// Set key and value to store.
    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()>;

    /// Get value by key from the store.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Delete key from the store.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// List all keys in the store.
    fn keys(&self) -> Result<Vec<Vec<u8>>>;

    /// Compact data files in the store.
    /// Clear stale entries from data files and reclaim disk space.
    fn compact(&mut self) -> Result<()>;

    /// Return total number of keys in datastore.
    fn len(&self) -> u64;

    /// Check datastore is empty or not.
    fn is_empty(&self) -> bool;

    /// Return `true` if datastore contains the given key.
    fn contains_key(&self, key: &[u8]) -> bool;

    /// Iterate all keys in datastore and call function `f`
    /// for each entry.
    ///
    /// If function `f` return an `Err`, it stops iteration
    /// and propagates the `Err` to the caller.
    ///
    /// You can continue iteration manually by returning `Ok(true)`,
    /// or stop iteration by returning `Ok(false)`.
    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>;

    /// Force flushing any pending writes to the datastore.
    fn sync(&mut self) -> Result<()>;

    /// Close a datastore, flush all pending writes to the datastore.
    fn close(&mut self) -> Result<()>;
}

/// Disk storage.
#[derive(Debug)]
pub struct DiskStorage<K>
where
    K: Keydir + Default,
{
    /// directory for database.
    path: PathBuf,

    /// lock for database directory.
    _lock: Lockfile,

    /// holds a bunch of data files.
    data_files: BTreeMap<u64, DataFile>,

    /// only active data files is writeable.
    active_data_file: Option<DataFile>,

    /// keydir maintains key value index for fast query.
    keydir: K,

    /// store options.
    opts: StoreOptions,
}

impl<K> DiskStorage<K>
where
    K: Keydir + Default,
{
    /// Initialize key value store with the given path.
    /// If the given path not found, a new one will be created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, StoreOptions::default())
    }

    /// Open datastore directory with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, opts: StoreOptions) -> Result<Self> {
        let path = path.as_ref();

        info!("open store path: {}", path.display());

        fs::create_dir_all(path)?;

        let lock = Lockfile::lock(path.join("LOCK")).or(Err(StoreError::AlreadyLocked))?;

        let mut store = Self {
            path: path.to_path_buf(),
            _lock: lock,
            data_files: BTreeMap::new(),
            active_data_file: None,
            keydir: K::default(),
            opts,
        };

        store.open_data_files()?;
        store.build_keydir()?;
        store.new_active_data_file(None)?;

        Ok(store)
    }

    /// Open data files (they are immutable).
    fn open_data_files(&mut self) -> Result<()> {
        let pattern = format!("{}/*{}", self.path.display(), settings::DATA_FILE_SUFFIX);
        trace!("read data files with pattern: {}", &pattern);
        for path in glob(&pattern)? {
            let df = DataFile::new(path?.as_path(), false)?;

            self.data_files.insert(df.file_id(), df);
        }
        trace!("got {} immutable data files", &self.data_files.len());

        Ok(())
    }

    fn build_keydir(&mut self) -> Result<()> {
        let mut file_ids: Vec<u64> = self.data_files.keys().cloned().collect();
        file_ids.sort();

        for file_id in file_ids {
            let hint_file_path = segment_hint_file_path(&self.path, file_id);
            if hint_file_path.exists() {
                self.build_keydir_from_hint_file(&hint_file_path)?;
            } else {
                self.build_keydir_from_data_file(file_id)?;
            }
        }

        info!("build keydir done, got {} keys.", self.keydir.len());

        Ok(())
    }

    fn build_keydir_from_hint_file(&mut self, path: &Path) -> Result<()> {
        trace!("build keydir from hint file {}", path.display());
        let mut hint_file = HintFile::new(path, false)?;
        let hind_file_id = hint_file.file_id();

        for entry in hint_file.iter() {
            let keydir_entry = KeydirEntry::new(hind_file_id, entry.offset(), entry.size(), 0);
            let _old = self.keydir.put(entry.key, keydir_entry);
            // todo!()
        }

        Ok(())
    }

    fn build_keydir_from_data_file(&mut self, file_id: u64) -> Result<()> {
        let df = self.data_files.get_mut(&file_id).unwrap();
        info!("build keydir from data file {}", df.path().display());

        for entry in df.iter() {
            if entry.value == settings::REMOVE_TOMESTONE {
                trace!("{} is a remove tomestone", &entry);

                self.keydir.remove(&entry.key);
            } else {
                let keydir_entry = KeydirEntry::from(&entry);
                let _old = self.keydir.put(entry.key, keydir_entry);
                // todo!()
            }
        }

        Ok(())
    }

    fn new_active_data_file(&mut self, file_id: Option<u64>) -> Result<()> {
        // default next file id should be `max_file_id` + 1
        let next_file_id: u64 =
            file_id.unwrap_or_else(|| self.data_files.keys().max().unwrap_or(&0) + 1);

        // build data file path.
        let p = segment_data_file_path(&self.path, next_file_id);
        debug!("new data file at: {}", &p.display());
        self.active_data_file = Some(DataFile::new(p.as_path(), true)?);

        // prepare a read-only data file with the same path.
        let df = DataFile::new(p.as_path(), false)?;
        self.data_files.insert(df.file_id(), df);

        Ok(())
    }

    fn next_file_id(&self) -> u64 {
        self.active_data_file
            .as_ref()
            .expect("active data file not found")
            .file_id()
            + 1
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<DataEntry> {
        let mut df = self
            .active_data_file
            .as_mut()
            .expect("active data file not found");

        // check file size, rotate to another one if nessessary.
        if df.size()? > self.opts.max_log_file_size {
            info!(
                "size of active data file `{}` exceeds maximum size of {} bytes, switch to another one",
                df.path().display(),
                self.opts.max_log_file_size
            );

            // sync data to disk.
            let _ = df.sync();

            // create a new active data file.
            self.new_active_data_file(None)?;

            // get new active data file for writting.
            df = self
                .active_data_file
                .as_mut()
                .expect("active data file not found");
        }

        let entry = df.write(key, value)?;
        if self.opts.sync {
            // make sure data entry is persisted in storage.
            df.sync()?;
        }

        Ok(entry)
    }
}

impl<K> Storage for DiskStorage<K>
where
    K: Keydir + Default,
{
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(key) {
            None => Ok(None),
            Some(keydir_entry) => {
                trace!(
                    "found key `{}` in keydir, got value {:?}",
                    String::from_utf8_lossy(key),
                    &keydir_entry,
                );

                let df = self
                    .data_files
                    .get_mut(&keydir_entry.file_id)
                    .unwrap_or_else(|| {
                        panic!("data file {} not found", &keydir_entry.file_id);
                    });

                match df.read(keydir_entry.offset)? {
                    None => Ok(None),
                    Some(e) => Ok(e.value.into()),
                }
            }
        }
    }

    fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let (key, value) = (key.as_ref(), value.as_ref());

        if key.len() as u64 > self.opts.max_key_size {
            return Err(StoreError::KeyIsTooLarge);
        }

        if value.len() as u64 > self.opts.max_value_size {
            return Err(StoreError::ValueIsTooLarge);
        }

        // save data to data file.
        let data_entry = self.write(key, value)?;

        // update keydir, the in-memory index.
        let keydir_entry = KeydirEntry::from(&data_entry);
        let _old = self.keydir.put(data_entry.key, keydir_entry);

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.keydir.contains_key(key) {
            trace!(
                "remove key `{}`, but it not found in datastore",
                String::from_utf8_lossy(key)
            );
        } else {
            trace!(
                "remove key `{}` from datastore",
                String::from_utf8_lossy(key)
            );

            // write tomestone, will be removed on compaction.
            let _entry = self.write(key, settings::REMOVE_TOMESTONE)?;

            // remove key from in-memory index.
            self.keydir.remove(key);
        }

        Ok(())
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        Ok(self.keydir.keys())
    }

    fn len(&self) -> u64 {
        self.keydir.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.keydir.contains_key(key)
    }

    fn for_each<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut wrapper = |_key: &Vec<u8>, keydir_entry: &mut KeydirEntry| -> Result<bool> {
            let df = self.data_files.get_mut(&keydir_entry.file_id).unwrap();
            let data_entry = df.read(keydir_entry.offset)?;
            match data_entry {
                None => Ok(false),
                Some(entry) => f(&entry.key, &entry.value),
            }
        };

        self.keydir.for_each(&mut wrapper)
    }

    fn sync(&mut self) -> Result<()> {
        if self.active_data_file.is_some() {
            self.active_data_file.as_mut().unwrap().sync()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.sync()?;
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let next_file_id = self.next_file_id();

        // switch to another active data file
        self.new_active_data_file(Some(next_file_id + 1))?;
        let mut compaction_data_file_id = next_file_id + 2;

        // create a new data file for compaction.
        let data_file_path = segment_data_file_path(&self.path, compaction_data_file_id);
        let mut compaction_df = DataFile::new(&data_file_path, true)?;

        // register read-only compaction data file.
        self.data_files.insert(
            compaction_df.file_id(),
            DataFile::new(&data_file_path, false)?,
        );

        // create a new hint file to store compaction file index.
        let hint_file_path = segment_hint_file_path(&self.path, compaction_data_file_id);
        let mut hint_file = HintFile::new(&hint_file_path, true)?;

        // copy all the data entries into compaction data file.
        let mut wrapper = |key: &Vec<u8>, keydir_entry: &mut KeydirEntry| -> Result<bool> {
            if compaction_df.size()? > self.opts.max_log_file_size {
                compaction_df.sync()?;
                hint_file.sync()?;

                compaction_data_file_id += 1;
                // switch to a new data file for compaction
                let data_file_path = segment_data_file_path(&self.path, compaction_data_file_id);
                compaction_df = DataFile::new(&data_file_path, true)?;

                self.data_files.insert(
                    compaction_df.file_id(),
                    DataFile::new(&data_file_path, false)?,
                );

                let hint_file_path = segment_hint_file_path(&self.path, compaction_data_file_id);
                hint_file = HintFile::new(&hint_file_path, true)?;
            }

            let df = self
                .data_files
                .get_mut(&keydir_entry.file_id)
                .expect("cannot find data file");

            let offset =
                compaction_df.copy_bytes_from(df, keydir_entry.offset, keydir_entry.size)?;

            keydir_entry.file_id = compaction_df.file_id();
            keydir_entry.offset = offset;

            hint_file.write(key, keydir_entry.offset, keydir_entry.size)?;

            Ok(false)
        };

        self.keydir.for_each(&mut wrapper)?;

        compaction_df.sync()?;
        hint_file.sync()?;

        // remove stale segments.
        for df in self.data_files.values() {
            if df.file_id() <= next_file_id {
                if df.path().exists() {
                    info!("remove stale log file {}", df.path().display());
                    fs::remove_file(df.path())?;
                }

                let hint_file_path = segment_hint_file_path(&self.path, df.file_id());
                if hint_file_path.exists() {
                    info!("remove stale log hint file {}", hint_file_path.display());
                    fs::remove_file(&hint_file_path)?;
                }
            }
        }

        self.data_files.retain(|&k, _| k > next_file_id);

        Ok(())
    }
}

impl<K> Drop for DiskStorage<K>
where
    K: Keydir + Default,
{
    fn drop(&mut self) {
        // ignore sync errors.
        trace!("sync all pending writes to disk.");
        let _r = self.sync();
    }
}

fn segment_data_file_path(dir: &Path, segment_id: u64) -> PathBuf {
    segment_file_path(dir, segment_id, settings::DATA_FILE_SUFFIX)
}

fn segment_hint_file_path(dir: &Path, segment_id: u64) -> PathBuf {
    segment_file_path(dir, segment_id, settings::HINT_FILE_SUFFIX)
}

fn segment_file_path(dir: &Path, segment_id: u64, suffix: &str) -> PathBuf {
    let mut p = dir.to_path_buf();
    p.push(format!("{:06}{}", segment_id, suffix));
    p
}

#[cfg(test)]
mod tests {
    use tempdir;

    use super::*;

    use super::super::keydir::HashmapKeydir;
    use super::super::OpenOptions;

    #[test]
    fn disk_storage_should_get_put() {
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();
        let mut db: DiskStorage<HashmapKeydir> = DiskStorage::open(dir.path()).unwrap();

        assert_eq!(db.len(), 0);

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, None);

        db.set(b"hello".to_vec(), b"world".to_vec()).unwrap();

        assert_eq!(db.len(), 1);
        assert_eq!(db.contains_key(b"hello"), true);

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, Some(b"world".to_vec()));

        db.set(b"hello".to_vec(), b"underworld".to_vec()).unwrap();

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, Some(b"underworld".to_vec()));

        db.delete(b"hello").unwrap();

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, None);
    }

    #[test]
    fn disk_storage_should_persist() {
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();

        {
            let mut db: DiskStorage<HashmapKeydir> = DiskStorage::open(dir.path()).unwrap();
            db.set(b"persistence".to_vec(), b"check".to_vec()).unwrap();
            db.set(b"removed".to_vec(), b"entry".to_vec()).unwrap();
            db.delete(b"removed").unwrap();
        }

        {
            let mut db: DiskStorage<HashmapKeydir> = DiskStorage::open(dir.path()).unwrap();
            let res = db.get(b"persistence").unwrap();
            assert_eq!(res, Some(b"check".to_vec()));

            let res = db.get(b"removed").unwrap();
            assert_eq!(res, None);
        }
    }

    #[test]
    fn disk_storage_should_retate_logs() {
        const VERSION: u8 = 10;
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();
        let open_opts = OpenOptions::new().max_log_file_size(50);

        {
            let mut db = open_opts.open(dir.path()).unwrap();

            for i in 0..=VERSION {
                db.set(b"version".to_vec(), vec![i]).unwrap();
            }
        }

        fn segment_data_file_path(dir: &Path, segment_id: u64) -> PathBuf {
            segment_file_path(dir, segment_id, settings::DATA_FILE_SUFFIX)
        }

        let logfile = segment_data_file_path(dir.path(), 1);
        assert_eq!(logfile.exists(), true);

        assert!(logfile.exists(), "log file has not been rotated");

        {
            let mut db = open_opts.open(dir.path()).unwrap();

            let res = db.get(b"version").unwrap();
            assert_eq!(res, Some(vec![VERSION]));
        }
    }

    #[test]
    fn test_lock_file() {
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();
        let _db: DiskStorage<HashmapKeydir> = DiskStorage::open(dir.path()).unwrap();

        let db2: Result<DiskStorage<HashmapKeydir>> = DiskStorage::open(dir.path());
        assert_eq!(db2.is_err(), true);
    }
}
