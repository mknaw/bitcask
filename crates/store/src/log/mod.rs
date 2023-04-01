use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use crc::{Crc, CRC_32_ISCSI};

use crate::Result;

pub mod files;
pub mod read;

// TODO investigate if this is the correct algorithm
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

// TODO probably should put this in some utils-oriented place...
fn from_utf8(input: &[u8]) -> &str {
    std::str::from_utf8(input).unwrap_or("UNREPRESENTABLE")
}

#[derive(Debug)]
pub struct LogEntry {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub ts: u128,
}

impl fmt::Display for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LogEntry: \"{}\" => \"{}\"",
            from_utf8(&self.key),
            from_utf8(&self.val)
        )
    }
}

impl LogEntry {
    pub fn from_set(key: &[u8], val: &[u8]) -> Result<Self> {
        let ts: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        Ok(Self {
            key: key.to_vec(),
            val: val.to_vec(),
            ts,
        })
    }

    pub fn key_sz(&self) -> u64 {
        self.key.len() as u64
    }

    pub fn val_sz(&self) -> u64 {
        self.val.len() as u64
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.extend(self.ts.to_ne_bytes());
        serialized.extend(self.key_sz().to_ne_bytes());
        serialized.extend(self.val_sz().to_ne_bytes());
        serialized.extend(self.key.clone());
        serialized.extend(self.val.clone());
        serialized
    }

    pub fn crc(&self) -> u32 {
        CRC.checksum(self.serialize().as_slice())
    }

    pub fn serialize_with_crc(&self) -> Vec<u8> {
        // TODO don't really need to call `serialize` 2x (the other time in `crc`)
        let mut serialized: Vec<u8> = self.crc().to_ne_bytes().into_iter().collect();
        serialized.extend(self.serialize());
        serialized
    }
}
