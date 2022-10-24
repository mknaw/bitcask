use std::error::Error;
use std::fs::File;
use std::io::{Seek, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crc::{Crc, CRC_32_ISCSI};
use log::info;

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

    pub fn key_sz(&self) -> usize {
        self.key.as_bytes().len()
    }

    pub fn val_sz(&self) -> usize {
        self.val.as_bytes().len()
    }

    pub fn serialize(&self) -> String {
        // TODO do we even need to comma separate?
        let s = format!(
            "{},{},{},{},{}",
            self.ts,
            self.key_sz(),
            self.val_sz(),
            self.key,
            self.val,
        );
        format!("{},{}", CRC.checksum(s.as_bytes()), s)
    }

    pub fn deserialize(s: &str) -> Result<Self> {
        let mut parts = s.splitn(2, ',');
        // TODO probably shouldn't unwrap - might panic?
        let crc = parts.next().unwrap().parse::<u32>()?;
        let rest = parts.next().unwrap();
        if crc != CRC.checksum(rest.as_bytes()) {
            info!("{}", rest);
            // TODO should be a special CRC error
            return Err("CRC mismatch".into());
        }

        let mut parts = rest.split(',');
        let ts = parts.next().unwrap().parse::<u64>()?;
        let _ = parts.next().unwrap().parse::<usize>()?;
        let _ = parts.next().unwrap().parse::<usize>()?;
        let key = parts.next().unwrap().to_string();
        let val = parts.next().unwrap().to_string();
        Ok(Self { key, val, ts })
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
        self.out.write_all("\n".as_bytes())?;
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
