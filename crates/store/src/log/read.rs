use std::io::{BufReader, Read};
use std::path::PathBuf;

use log::debug;

use crate::log::files::FileHandle;
use crate::log::LogEntry;
use crate::Result;

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

// TODO think we just should have a `LogFile` and a `HintFile`, both with their own iterators.
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
        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf).ok()?;
        let ts = u128::from_ne_bytes(buf);

        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).ok()?;
        let key_sz = u64::from_ne_bytes(buf) as usize;

        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).ok()?;
        let val_sz = u64::from_ne_bytes(buf) as usize;

        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).ok()?;
        let val_pos = u64::from_ne_bytes(buf) as usize;

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
