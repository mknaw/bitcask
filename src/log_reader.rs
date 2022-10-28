use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use log::info;

use crate::keydir::Item;
use crate::log_writer::LogEntry;
use crate::Result;

pub struct LogReader {
    path: PathBuf,
}

impl LogReader {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn open(&self) -> Result<File> {
        let file = File::open(&self.path)?;
        Ok(file)
    }

    pub fn items(&self) -> ItemIterator {
        let file = self.open().unwrap();
        let reader = BufReader::new(file);
        ItemIterator::new(self.path.as_ref(), reader)
    }
}

pub struct ItemIterator<'a> {
    path: &'a Path,
    reader: BufReader<File>,
    position: usize,
}

impl<'a> ItemIterator<'a> {
    pub fn new(path: &'a Path, reader: BufReader<File>) -> Self {
        Self {
            path,
            reader,
            position: 0,
        }
    }
}

impl<'a> Iterator for ItemIterator<'a> {
    type Item = Result<(String, Item)>;

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

        info!(
            "crc: {:?}, ts: {:?}, key_sz: {:?}, val_sz: {:?}, key: {:?}, val: {:?}",
            crc, ts, key_sz, val_sz, key, val
        );

        Some(Ok((
            key.to_string(),
            Item {
                file_id: self.path.as_os_str().to_os_string(),
                val_sz,
                val_pos: (self.position - val_sz) as u64,
                ts,
            },
        )))
    }
}
