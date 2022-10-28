use std::error::Error;
use std::fs::File;
use std::io::{Seek, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crc::{Crc, CRC_32_ISCSI};

use crate::command;
use crate::Result;

// TODO investigate if this is the correct algorithm
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug)]
pub struct LogEntry {
    pub key: String,
    pub val: String,
    pub ts: u64,
}

impl LogEntry {
    pub fn from_set_command(cmd: &command::Set) -> Result<Self> {
        let command::Set { key, val } = cmd;
        let ts: u64 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        Ok(Self {
            key: key.clone(),
            val: val.clone(),
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
            "{:016x}{:016x}{:016x}{}{}",
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

    pub fn serialize_hint(&self, position: u64) -> String {
        format!(
            "{},{},{},{},{}",
            self.ts,
            self.key_sz(),
            self.val_sz(),
            position,
            self.key,
        )
    }
}

pub trait LogWriterT {
    type Out: Write + Sync;

    fn write(&mut self, line: String) -> Result<()>;
    fn stream_position(&mut self) -> Result<u64>;
}

pub struct LogWriter<Out: Write + Seek> {
    out: Out,
}

impl<Out: Write + Seek> LogWriter<Out> {
    pub fn new(out: Out) -> Self {
        Self { out }
    }
}

// TODO this is basically just Write + Seek.
impl LogWriterT for LogWriter<File> {
    type Out = File;

    // TODO can we get generic implementation from a ... trait?
    fn write(&mut self, line: String) -> Result<()> {
        self.out.write_all(line.as_bytes())?;
        Ok(())
    }

    fn stream_position(&mut self) -> Result<u64> {
        self.out.stream_position().map_err(|err| {
            let dyn_err: Box<dyn Error> = Box::new(err);
            dyn_err
        })
    }
}

impl LogWriter<File> {
    // TODO probably should be something for the manager to check?
    // So we don't have to pass around config stuff (max file size).
    pub fn is_full(&self) -> Result<bool> {
        todo!();
    }
}
