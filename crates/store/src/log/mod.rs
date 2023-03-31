use std::time::{SystemTime, UNIX_EPOCH};

use crc::{Crc, CRC_32_ISCSI};

use crate::Result;

pub mod files;
pub mod read;

// TODO investigate if this is the correct algorithm
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug)]
pub struct LogEntry {
    pub key: String,
    pub val: String,
    pub ts: u128,
}

impl LogEntry {
    pub fn from_set(key: &str, val: &str) -> Result<Self> {
        let ts: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        Ok(Self {
            key: key.to_string(),
            val: val.to_string(),
            ts,
        })
    }

    pub fn key_sz(&self) -> u64 {
        self.key.as_bytes().len() as u64
    }

    pub fn val_sz(&self) -> u64 {
        self.val.as_bytes().len() as u64
    }

    pub fn serialize(&self) -> Vec<u8> {
        // TODO this is still not so good, don't need to allocate 2x
        let mut serialized = Vec::new();
        serialized.extend(self.ts.to_ne_bytes());
        serialized.extend(self.key_sz().to_ne_bytes());
        serialized.extend(self.val_sz().to_ne_bytes());
        serialized.extend(self.key.as_bytes());
        serialized.extend(self.val.as_bytes());
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
