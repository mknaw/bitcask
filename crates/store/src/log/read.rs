use std::ffi::OsString;
use std::io::{BufReader, Read};

use log::{debug, info};

use crate::log::handle::Handle;
use crate::{log::LogEntry, Result};

pub struct Reader<'a> {
    reader: BufReader<&'a mut Handle>,
    position: usize,
}

impl<'a> Reader<'a> {
    pub fn new(handle: &'a mut Handle) -> Self {
        Self {
            reader: BufReader::new(handle),
            position: 0,
        }
    }
}

pub struct ReaderItem {
    pub file_id: OsString,
    pub entry: LogEntry,
    pub val_pos: u64,
}

impl ReaderItem {
    pub fn to_keydir_item(&self) -> crate::keydir::Item {
        crate::keydir::Item {
            file_id: self.file_id.to_os_string(),
            val_sz: self.entry.val_sz() as usize,
            val_pos: self.val_pos,
            ts: self.entry.ts,
        }
    }
}

impl<'a> Iterator for Reader<'a> {
    type Item = Result<ReaderItem>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO not very pretty
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).ok()?;
        let crc = u32::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut buf = [0u8; 32];
        self.reader.read_exact(&mut buf).ok()?;
        let ts = u128::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut buf = [0u8; 16];
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

        self.position += 8 + 32 + 2 * 16 + key_sz + val_sz;

        if entry.crc() != crc {
            info!("CRC mismatch for entry: {:?}", entry);
            return None;
        }

        debug!(
            "crc: {:?}, ts: {:?}, key_sz: {:?}, val_sz: {:?}, key: {:?}, val: {:?}",
            crc, ts, key_sz, val_sz, key, val
        );

        Some(Ok(ReaderItem {
            file_id: self.reader.get_ref().id.clone(),
            entry,
            val_pos: (self.position - val_sz) as u64,
        }))
    }
}
