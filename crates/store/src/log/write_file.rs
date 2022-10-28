use std::error::Error;
use std::fs::File;
use std::io::{Seek, Write};

use crate::Result;

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
