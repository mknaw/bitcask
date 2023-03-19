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

    pub fn serialize(&self) -> String {
        // TODO this is still not so good, don't need to allocate 2x
        format!(
            "{:032x}{:016x}{:016x}{}{}",
            self.ts,
            self.key_sz(),
            self.val_sz(),
            self.key,
            self.val,
        )
    }

    pub fn crc(&self) -> u32 {
        CRC.checksum(self.serialize().as_bytes())
    }

    pub fn serialize_with_crc(&self) -> String {
        let s = self.serialize();
        let crc = CRC.checksum(s.as_bytes());
        format!("{:08x}{}", crc, s)
    }
}
