//! entries module.

use std::{
    fmt::Display,
    io::{Read, Seek, SeekFrom, Write},
};

use chrono::Utc;

use super::error::Result;

/// EntryIO trait.
pub trait EntryIO {
    type Entry;

    fn read_from<R>(r: &mut R, offset: u64) -> Result<Option<Self::Entry>>
    where
        R: Read + Seek;

    fn write_to<W>(&self, w: &mut W) -> Result<u64>
    where
        W: Write + Seek;
}

// use super::errors::Result;

pub const HEADER_SIZE: usize = 16;

/// Entry Header Structure.
///
/// # fields:
/// - crc: u32
/// - timestamp: u32
/// - key_sz: u32
/// - value_sz: u32
///
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DataHeader([u8; HEADER_SIZE]);

impl DataHeader {
    pub fn new(crc: u32, timestamp: u32, key_sz: u32, value_sz: u32) -> Self {
        let mut buf = [0u8; HEADER_SIZE];

        buf[0..4].copy_from_slice(&crc.to_be_bytes());
        buf[4..8].copy_from_slice(&timestamp.to_be_bytes());
        buf[8..12].copy_from_slice(&key_sz.to_be_bytes());
        buf[12..16].copy_from_slice(&value_sz.to_be_bytes());

        Self(buf)
    }

    pub fn crc(&self) -> u32 {
        u32::from_be_bytes(self.0[0..4].try_into().unwrap())
    }

    pub fn timestamp(&self) -> u32 {
        u32::from_be_bytes(self.0[4..8].try_into().unwrap())
    }

    pub fn key_sz(&self) -> u32 {
        u32::from_be_bytes(self.0[8..12].try_into().unwrap())
    }

    pub fn value_sz(&self) -> u32 {
        u32::from_be_bytes(self.0[12..16].try_into().unwrap())
    }
}

impl AsRef<[u8]> for DataHeader {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; HEADER_SIZE]> for DataHeader {
    fn from(value: [u8; HEADER_SIZE]) -> Self {
        Self(value)
    }
}

impl From<DataHeader> for [u8; HEADER_SIZE] {
    fn from(v: DataHeader) -> Self {
        v.0
    }
}

/// Disk Entry Structure.
#[derive(Debug, PartialEq, Eq)]
pub struct DataEntry {
    /// header of disk entry.
    header: DataHeader,

    /// key of disk entry.
    pub key: Vec<u8>,

    /// value of disk entry.
    pub value: Vec<u8>,

    /// offset of disk entry.
    pub offset: Option<u64>,

    /// file id of disk entry.
    pub file_id: Option<u64>,
}

impl DataEntry {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let timestamp: u32 = Utc::now().timestamp().try_into().unwrap();
        let crc = 0;
        let (key_sz, value_sz) = (key.len() as u32, value.len() as u32);
        let header = DataHeader::new(crc, timestamp, key_sz, value_sz);

        Self {
            header,
            key,
            value,
            offset: None,
            file_id: None,
        }
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn file_id(mut self, file_id: u64) -> Self {
        self.file_id = Some(file_id);
        self
    }

    pub fn size(&self) -> u64 {
        (HEADER_SIZE + self.key.len() + self.value.len()) as u64
    }

    // pub fn crc(&self) -> u32 {
    //     self.header.crc()
    // }

    pub fn timestamp(&self) -> u32 {
        self.header.timestamp()
    }

    // pub fn key_sz(&self) -> usize {
    //    self.header.key_sz() as usize
    // }

    // pub fn value_sz(&self) -> usize {
    //     self.header.value_sz() as usize
    // }
}

impl Display for DataEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DataEntry(file_id={:?}, key='{}', offset={:?}, size={})",
            self.file_id,
            String::from_utf8_lossy(self.key.as_ref()),
            self.offset,
            self.size(),
        )
    }
}

impl EntryIO for DataEntry {
    type Entry = Self;

    fn read_from<R>(r: &mut R, offset: u64) -> Result<Option<Self::Entry>>
    where
        R: Read + Seek,
    {
        r.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; HEADER_SIZE];
        if r.read(&mut buf)? == 0 {
            return Ok(None);
        }

        let header = DataHeader::from(buf);

        let mut key = vec![0u8; header.key_sz() as usize];
        r.read_exact(&mut key)?;

        let mut value = vec![0u8; header.value_sz() as usize];
        r.read_exact(&mut value)?;

        Ok(Some(Self {
            header,
            key,
            value,
            offset: None,
            file_id: None,
        }))
    }

    fn write_to<W>(&self, w: &mut W) -> Result<u64>
    where
        W: Write + Seek,
    {
        let offset = w.stream_position()?;

        w.write_all(self.header.as_ref())?;
        w.write_all(self.key.as_ref())?;
        w.write_all(self.value.as_ref())?;

        Ok(offset)
    }
}

/// Hint Entry Header Structure.
///
/// # fields:
/// - offset: u64
/// - key_sz: u32
/// - value_sz: u32
///
#[derive(Debug)]
pub struct HintHeader([u8; HEADER_SIZE]);

impl HintHeader {
    pub fn new(offset: u64, key_sz: u32, value_sz: u32) -> Self {
        let mut buf = [0u8; HEADER_SIZE];

        buf[0..8].copy_from_slice(&offset.to_be_bytes());
        buf[8..12].copy_from_slice(&key_sz.to_be_bytes());
        buf[12..16].copy_from_slice(&value_sz.to_be_bytes());

        Self(buf)
    }

    pub fn offset(&self) -> u64 {
        u64::from_be_bytes(self.0[0..8].try_into().unwrap())
    }

    pub fn key_sz(&self) -> usize {
        u32::from_be_bytes(self.0[8..12].try_into().unwrap()) as usize
    }

    pub fn value_sz(&self) -> usize {
        u32::from_be_bytes(self.0[12..16].try_into().unwrap()) as usize
    }

    pub fn size(&self) -> u64 {
        HEADER_SIZE as u64 + self.key_sz() as u64 + self.value_sz() as u64
    }
}

impl AsRef<[u8; HEADER_SIZE]> for HintHeader {
    fn as_ref(&self) -> &[u8; HEADER_SIZE] {
        &self.0
    }
}

impl From<[u8; HEADER_SIZE]> for HintHeader {
    fn from(buf: [u8; HEADER_SIZE]) -> Self {
        Self(buf)
    }
}

/// Entry in the hint file.
#[derive(Debug)]
pub struct HintEntry {
    /// header of hint entry.
    header: HintHeader,

    /// key of disk entry.
    pub key: Vec<u8>,
}

impl HintEntry {
    pub fn new(key: Vec<u8>, offset: u64, size: u64) -> Self {
        let key_sz = key.len() as u32;
        let value_sz = size as u32 - HEADER_SIZE as u32 - key_sz;
        let header = HintHeader::new(offset, key_sz, value_sz);
        Self { header, key }
    }

    pub fn offset(&self) -> u64 {
        self.header.offset()
    }

    pub fn size(&self) -> u64 {
        self.header.size()
    }

    pub fn selfsize(&self) -> u64 {
        HEADER_SIZE as u64 + self.key.len() as u64
    }

    // pub fn key_sz(&self) -> usize {
    //     self.header.key_sz() as usize
    // }
}

impl Display for HintEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HintEntry(key='{}', offset={}, size={})",
            String::from_utf8_lossy(self.key.as_ref()),
            self.offset(),
            self.size(),
        )
    }
}

impl EntryIO for HintEntry {
    type Entry = Self;

    fn read_from<R>(r: &mut R, offset: u64) -> Result<Option<Self::Entry>>
    where
        R: Read + Seek,
    {
        r.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; HEADER_SIZE];
        if r.read(&mut buf)? == 0 {
            return Ok(None);
        }

        let header = HintHeader::from(buf);

        let mut key = vec![0u8; header.key_sz() as usize];
        r.read_exact(&mut key)?;

        Ok(Some(Self::Entry { header, key }))
    }

    fn write_to<W>(&self, w: &mut W) -> Result<u64>
    where
        W: Write + Seek,
    {
        let offset = w.stream_position()?;

        w.write_all(self.header.as_ref())?;
        w.write_all(self.key.as_ref())?;

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use rand::Rng;

    fn header_test(header: DataHeader) {
        let data: [u8; HEADER_SIZE] = header.clone().into();
        let deserialized_header = DataHeader::from(data);

        assert_eq!(header, deserialized_header)
    }

    fn random_header() -> DataHeader {
        let mut rng = rand::thread_rng();

        DataHeader::new(rng.gen(), rng.gen(), rng.gen(), rng.gen())
    }

    #[test]
    fn it_should_serialize_header() {
        let tests = [
            DataHeader::new(10, 10, 10, 10),
            DataHeader::new(0, 0, 0, 0),
            DataHeader::new(10000, 10000, 10000, 10000),
        ];

        for test in tests {
            header_test(test)
        }
    }

    #[test]
    fn it_should_serialize_header_random() {
        for _ in 0..10 {
            header_test(random_header());
        }
    }

    #[test]
    fn it_should_create_disk_entry() {
        let entry = DataEntry::new(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(entry.header.key_sz(), 5);
        assert_eq!(entry.header.value_sz(), 5);
    }

    #[test]
    fn test_entry_io() {
        let entry = DataEntry::new(b"hello".to_vec(), b"world".to_vec());

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        let offset = entry.write_to(&mut cursor).unwrap();
        assert_eq!(offset, 0);

        let entry1 = DataEntry::read_from(&mut cursor, offset).unwrap();
        assert_eq!(entry1.is_some(), true);

        let e = entry1.unwrap();
        assert_eq!(e.key, b"hello".to_vec());
    }
}
