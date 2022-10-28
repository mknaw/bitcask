use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    str::from_utf8,
};

use log::{debug, info};

use crate::{log::LogEntry, Result};

pub struct LogFile {
    file: File,
}

impl LogFile {
    pub fn new(file: File) -> Self {
        Self { file }
    }

    pub fn read(&mut self, from: u64, len: usize) -> Result<String> {
        self.file.seek(SeekFrom::Start(from))?;
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }

    pub fn items(&mut self) -> ItemIterator {
        let reader = BufReader::new(&self.file);
        ItemIterator::new(reader)
    }
}

pub struct ItemIterator<'a> {
    reader: BufReader<&'a File>,
    position: usize,
}

impl<'a> ItemIterator<'a> {
    pub fn new(reader: BufReader<&'a File>) -> Self {
        Self {
            reader,
            position: 0,
        }
    }
}

impl<'a> Iterator for ItemIterator<'a> {
    type Item = Result<(LogEntry, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO not very pretty
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).ok()?;
        let crc = u32::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf).ok()?;
        let ts = u64::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        self.reader.read_exact(&mut buf).ok()?;
        let key_sz = usize::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        self.reader.read_exact(&mut buf).ok()?;
        let val_sz = usize::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut key = vec![0u8; key_sz];
        self.reader.read_exact(&mut key).ok()?;
        let key = std::str::from_utf8(&key).ok()?;

        let mut val = vec![0u8; val_sz];
        self.reader.read_exact(&mut val).ok()?;
        let val = std::str::from_utf8(&val).ok()?;

        // TODO another needless allocation, should do this nicer-ly
        let entry = LogEntry {
            key: key.to_string(),
            val: val.to_string(),
            ts,
        };

        self.position += 8 + 3 * 16 + key_sz + val_sz;

        if entry.crc() != crc {
            info!("CRC mismatch for entry: {:?}", entry);
            return None;
        }

        debug!(
            "crc: {:?}, ts: {:?}, key_sz: {:?}, val_sz: {:?}, key: {:?}, val: {:?}",
            crc, ts, key_sz, val_sz, key, val
        );

        Some(Ok((entry, (self.position - val_sz) as u64)))
    }
}
