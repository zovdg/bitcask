//! Data File Module.

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use log::{error, trace};

use super::error::{Result, StoreError};
use super::format::{DataEntry, EntryIO, HintEntry};

use crate::utils::path::parse_file_id;

#[derive(Debug)]
pub struct LogFile {
    /// file path.
    pub path: PathBuf,

    /// file id.
    pub id: u64,

    /// Mark current data file can be writable or not.
    writeable: bool,

    /// File handle of data file for writing.
    writer: Option<File>,

    /// File handle of data file for reading.
    reader: File,
}

impl LogFile {
    pub fn new(path: impl AsRef<Path>, writeable: bool) -> Result<Self> {
        let path = path.as_ref();

        // Data name must starts with valid file id.
        let file_id = parse_file_id(path).expect("file id not found in file path");

        let writer = if writeable {
            let f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)?;
            Some(f)
        } else {
            None
        };

        let reader = fs::File::open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            id: file_id,
            writeable,
            writer,
            reader,
        })
    }

    /// Flush all pending writes to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        if let Some(file) = &mut self.writer {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Flush buf writer.
    fn flush(&mut self) -> Result<()> {
        if self.writeable {
            self.writer.as_mut().unwrap().flush()?;
        }
        Ok(())
    }

    /// file size.
    pub fn size(&self) -> Result<u64> {
        Ok(self.reader.metadata()?.len())
    }

    pub fn copy_bytes_from(&mut self, src: &mut LogFile, offset: u64, size: u64) -> Result<u64> {
        let w = self.writer.as_mut().expect("data file is not writeable");

        let r = &mut src.reader;
        r.seek(SeekFrom::Start(offset))?;

        let mut r = r.take(size);
        let w_offset = w.stream_position()?;

        let num_types = io::copy(&mut r, w)?;
        assert_eq!(num_types, size);

        Ok(w_offset)
    }
}

impl Drop for LogFile {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            error!(
                "failed to sync log file: {}, got error: {}",
                self.path.display(),
                e
            );
        }

        // auto clean up if file size is zero.
        if self.writeable && self.size().unwrap() == 0 {
            trace!("log file `{}` is empty, remove it.", self.path.display());

            fs::remove_file(self.path.as_path()).unwrap();
        }
    }
}

/// DataFile
#[derive(Debug)]
pub struct DataFile {
    inner: LogFile,
}

impl DataFile {
    pub fn new(path: impl AsRef<Path>, writeable: bool) -> Result<Self> {
        let inner = LogFile::new(path, writeable)?;

        Ok(Self { inner })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn file_id(&self) -> u64 {
        self.inner.id
    }

    pub fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    pub fn iter(&mut self) -> DataEntryIter {
        DataEntryIter {
            reader: &mut self.inner.reader,
            offset: 0,
            file_id: self.inner.id,
        }
    }

    /// Save key-value pair to segement file.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<DataEntry> {
        let path = self.inner.path.as_path();
        let w = self
            .inner
            .writer
            .as_mut()
            .ok_or_else(|| StoreError::FileNotWriteable(path.to_path_buf()))?;

        trace!(
            "append {} to segement file {}",
            String::from_utf8_lossy(key),
            self.inner.path.display()
        );

        let data_entry = DataEntry::new(key.to_vec(), value.to_vec());
        let offset = data_entry.write_to(w)?;

        trace!(
            "successfully append {} to data file {}",
            &data_entry,
            self.inner.path.display()
        );

        Ok(data_entry.offset(offset).file_id(self.inner.id))
    }

    /// Read key value in data file.
    pub fn read(&mut self, offset: u64) -> Result<Option<DataEntry>> {
        trace!(
            "read key value with offset {} in data file {}",
            offset,
            self.inner.path.display()
        );

        if self.inner.size()? < offset {
            return Ok(None);
        }

        match DataEntry::read_from(&mut self.inner.reader, offset)? {
            None => Ok(None),
            Some(entry) => {
                trace!(
                    "successfully read {} from data log file {}",
                    &entry,
                    self.inner.path.display()
                );

                Ok(Some(entry))
            }
        }
    }

    /// Flush all pending writes to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.inner.sync()
    }

    /// Copy `size` bytes from `src` data file.
    /// Return offset of the newly written entry.
    pub fn copy_bytes_from(&mut self, src: &mut DataFile, offset: u64, size: u64) -> Result<u64> {
        self.inner.copy_bytes_from(&mut src.inner, offset, size)
    }
}

pub struct DataEntryIter<'a> {
    reader: &'a mut File,
    offset: u64,
    file_id: u64,
}

impl<'a> Iterator for DataEntryIter<'a> {
    type Item = DataEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match DataEntry::read_from(self.reader, self.offset).unwrap() {
            None => None,
            Some(entry) => {
                let entry = entry.offset(self.offset).file_id(self.file_id);
                self.offset += entry.size();
                Some(entry)
            }
        }
    }
}

/// HintFile
#[derive(Debug)]
pub struct HintFile {
    inner: LogFile,
    /// Number of Written entries.
    entries_written: u64,
}

impl HintFile {
    pub fn new(path: impl AsRef<Path>, writeable: bool) -> Result<Self> {
        let inner = LogFile::new(path, writeable)?;

        Ok(Self {
            inner,
            entries_written: 0,
        })
    }

    // pub fn path(&self) -> &Path {
    //    &self.inner.path
    // }

    pub fn file_id(&self) -> u64 {
        self.inner.id
    }

    pub fn iter(&mut self) -> HintEntryIter {
        HintEntryIter {
            reader: &mut self.inner.reader,
            offset: 0,
        }
    }

    pub fn write(&mut self, key: impl AsRef<[u8]>, offset: u64, size: u64) -> Result<u64> {
        let entry = HintEntry::new(key.as_ref().to_vec(), offset, size);
        trace!("append {} to file {}", &entry, self.inner.path.display());

        let w = &mut self
            .inner
            .writer
            .as_mut()
            .expect("hint file is not writeable");

        let offset = entry.write_to(w)?;
        self.entries_written += 1;

        self.inner.flush()?;

        Ok(offset)
    }

    /// Sync all pending writes to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.inner.sync()
    }
}

pub struct HintEntryIter<'a> {
    reader: &'a mut File,
    offset: u64,
}

impl<'a> Iterator for HintEntryIter<'a> {
    type Item = HintEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match HintEntry::read_from(self.reader, self.offset).unwrap() {
            None => None,
            Some(entry) => {
                self.offset += entry.selfsize();
                Some(entry)
            }
        }
    }
}
