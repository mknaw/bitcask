use crate::keydir::Item;
use crate::log::LogEntry;
use std::ffi::OsString;
use std::fmt;
use std::io::{Seek, Write};
use std::sync::Arc;

use log::debug;

use crate::log::handle::{ReadHandle, WriteHandle};
use crate::{Config, Result};

type NameGenerator = Arc<dyn Fn(i8) -> String + Send + Sync>;

#[derive(Debug)]
pub struct WriterError;

impl fmt::Display for WriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WriterError occurred")
    }
}

impl ::std::error::Error for WriterError {
    fn description(&self) -> &str {
        "write error"
    }
}

#[derive(Debug)]
pub struct WriteResult {
    pub file_id: OsString,
    pub position: u64,
    pub new_handle: Option<ReadHandle>,
}

pub struct Writer {
    // TODO still not sure we need the whole config here.
    // On the one hand nice to get max filesize + logdir when writing
    // But also want it to be abstract so logdir not always relevant.
    config: Arc<Config>,
    pub out: Option<WriteHandle>,
    file_counter: i8,
    make_name: NameGenerator,
}

impl Writer {
    pub fn new(config: Arc<Config>, make_name: NameGenerator) -> Self {
        Self {
            config,
            make_name,
            file_counter: 0,
            out: None,
        }
    }

    pub fn will_fit(&mut self, line: &[u8]) -> Result<bool> {
        Ok(line.len() as u64 + self.stream_position()? <= self.config.max_log_file_size)
    }

    pub fn reset(&mut self) {
        self.out = None;
    }

    fn open(&mut self) -> Result<WriteHandle> {
        // TODO should hit this with an `ok_or`?
        let fname = (self.make_name)(self.file_counter);
        let id: OsString = fname.clone().into();
        let path = self.config.log_dir.join(fname);
        debug!("Opening new write file {:?}", path);
        let write_handle = WriteHandle::new(id, path)?;
        self.file_counter += 1;
        Ok(write_handle)
    }

    pub fn write(&mut self, line: &[u8]) -> Result<WriteResult> {
        let need_new_out = self.out.is_none() || !self.will_fit(line)?;
        if need_new_out {
            self.out = Some(self.open()?);
        }
        {
            let write_handle = self.out.as_mut().unwrap();
            write_handle.write_all(line)?;
        }
        let position = self.stream_position()?;
        // TODO probably don't want manager to have a writable handle.
        let new_handle = if need_new_out {
            Some(ReadHandle::from_write_handle(self.out.as_ref().unwrap())?)
        } else {
            None
        };
        Ok(WriteResult {
            file_id: self.out.as_ref().unwrap().id.clone(),
            position,
            new_handle,
        })
    }

    pub fn set(&mut self, entry: &LogEntry) -> Result<(Item, Option<ReadHandle>)> {
        let line = entry.serialize_with_crc();
        let WriteResult {
            file_id,
            position,
            new_handle,
        } = self.write(line.as_bytes())?;
        let val_pos = position - entry.val_sz();
        let item = Item {
            file_id,
            val_sz: entry.val.len(),
            val_pos,
            ts: entry.ts,
        };
        Ok((item, new_handle))
    }

    fn stream_position(&mut self) -> Result<u64> {
        match &mut self.out {
            Some(write_handle) => write_handle.stream_position().map_err(|err| {
                let dyn_err: Box<dyn std::error::Error> = Box::new(err);
                dyn_err
            }),
            None => Err(Box::new(WriterError {})),
        }
    }

    pub fn current_file_id(&self) -> Option<&OsString> {
        self.out.as_ref().map(|out| &out.id)
    }
}
