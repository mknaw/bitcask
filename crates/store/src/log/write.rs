use std::ffi::OsString;
use std::fmt;
use std::io::{Seek, Write};
use std::sync::{Arc, RwLock};

use log::debug;

use crate::log::handle::{Handle, SharedHandle};
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
    pub position: u64,
    pub handle: Arc<RwLock<Handle>>,
}

pub struct Writer<'cfg> {
    // TODO still not sure we need the whole config here.
    // On the one hand nice to get max filesize + logdir when writing
    // But also want it to be abstract so logdir not always relevant.
    config: &'cfg Config<'cfg>,
    pub out: Option<SharedHandle>,
    file_counter: i8,
    make_name: NameGenerator,
}

impl<'cfg> Writer<'cfg> {
    pub fn new(config: &'cfg Config<'cfg>, make_name: NameGenerator) -> Self {
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

    fn open(&mut self) -> Result<SharedHandle> {
        // TODO should hit this with an `ok_or`?
        let fname = (self.make_name)(self.file_counter);
        let id: OsString = fname.clone().into();
        let path = self.config.log_dir.join(fname);
        debug!("Opening new write file {:?}", path);
        let handle = Handle::new(id, path, true)?;
        self.file_counter += 1;
        Ok(Arc::new(RwLock::new(handle)))
    }

    pub fn write(&mut self, line: &[u8]) -> Result<WriteResult> {
        let need_new_out = self.out.is_none() || !self.will_fit(line)?;
        if need_new_out {
            // TODO the `Handle::create` still doesn't seem ideal but whatever
            self.out = Some(self.open()?);
            self.write(line)
        } else {
            {
                let mut out = self.out.as_ref().unwrap().write().unwrap();
                out.write_all(line)?;
            }
            let position = self.stream_position()?;
            // TODO probably don't want manager to have a writable handle.
            Ok(WriteResult {
                position,
                handle: Arc::clone(self.out.as_ref().unwrap()),
            })
        }
    }

    fn stream_position(&mut self) -> Result<u64> {
        match &mut self.out {
            Some(out) => out.write().unwrap().stream_position().map_err(|err| {
                let dyn_err: Box<dyn std::error::Error> = Box::new(err);
                dyn_err
            }),
            None => Err(Box::new(WriterError {})),
        }
    }
}
