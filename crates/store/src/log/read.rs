use std::io::{BufReader, Read};
use std::path::PathBuf;

use log::{debug, info};

use crate::log::files::FileHandle;
use crate::log::LogEntry;
use crate::Result;

pub struct LogReader<'a> {
    reader: BufReader<&'a mut FileHandle>,
    position: usize,
}

impl<'a> LogReader<'a> {
    pub fn new(handle: &'a mut FileHandle) -> Self {
        Self {
            reader: BufReader::new(handle),
            position: 0,
        }
    }
}

pub struct LogReaderItem {
    pub path: PathBuf,
    pub entry: LogEntry,
    pub val_pos: u64,
}

impl LogReaderItem {
    pub fn into_key_item_tuple(self) -> (String, crate::keydir::Item) {
        let val_sz = self.entry.val_sz() as usize;
        (
            self.entry.key,
            crate::keydir::Item {
                path: self.path.clone(),
                ts: self.entry.ts,
                val_pos: self.val_pos,
                val_sz,
            },
        )
    }
}

impl<'a> Iterator for LogReader<'a> {
    type Item = Result<LogReaderItem>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO not very pretty ... maybe `nom(_bufreader)?` would be better here
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
        } else {
            debug!("Reading from cask: {}: {}", key, val);
        }

        Some(Ok(LogReaderItem {
            path: self.reader.get_ref().path.clone(),
            entry,
            val_pos: (self.position - val_sz) as u64,
        }))
    }
}

pub struct HintReader<'a> {
    reader: BufReader<&'a mut FileHandle>,
}

impl<'a> HintReader<'a> {
    pub fn new(handle: &'a mut FileHandle) -> Self {
        Self {
            reader: BufReader::new(handle),
        }
    }
}

impl<'a> Iterator for HintReader<'a> {
    type Item = Result<(String, crate::keydir::Item)>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO not very pretty ... maybe `nom(_bufreader)?` would be better here
        let mut buf = [0u8; 32];
        self.reader.read_exact(&mut buf).ok()?;
        let ts = u128::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf).ok()?;
        let key_sz = usize::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf).ok()?;
        let val_sz = usize::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf).ok()?;
        let val_pos = usize::from_str_radix(std::str::from_utf8(&buf).ok()?, 16).ok()?;

        let mut key = vec![0u8; key_sz];
        self.reader.read_exact(&mut key).ok()?;
        let key = std::str::from_utf8(&key).ok()?;

        debug!("Reading from hint: {}", key);

        let mut path = self.reader.get_ref().path.clone();
        path.set_extension("cask");

        Some(Ok((
            key.to_string(),
            crate::keydir::Item {
                path,
                val_sz,
                val_pos: val_pos as u64,
                ts,
            },
        )))
    }
}
