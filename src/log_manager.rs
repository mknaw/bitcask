use std::fs::File;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::log_writer::{LogWriterT, LogWriter};

pub trait LogManagerT {
    type Out: Write + Seek;
    fn write(&mut self, line: String) -> crate::Result<()>;
    fn make_new_writer(&mut self) -> crate::Result<LogWriter<Self::Out>>;
}

pub struct FileLogManager<'a> {
    config: &'a Config<'a>,
    pub current_path: Option<PathBuf>,
    writer: Option<LogWriter<File>>,
}

// TODO implement locking?
impl<'a> FileLogManager<'a> {
    pub fn new(config: &'a Config<'a>) -> Self {
        Self {
            config,
            // TODO this shouldn't really live here... probably
            current_path: None,
            writer: None,
        }
    }
    
    fn generate_fname(&self) -> crate::Result<String> {
        let ts: u64 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        // TODO append some random junk to avoid collisions?
        Ok(format!("{}.cask", ts))
    }

    fn need_new_writer(&mut self, line: &str) -> crate::Result<bool> {
        if self.writer.is_none() {
            return Ok(true);
        }
        Ok(line.len() as u64 + self.position()? > self.config.max_log_file_size)
    }

    pub fn position(&mut self) -> crate::Result<u64> {
        if let Some(ref mut writer) = self.writer {
            Ok(writer.stream_position()?)
        } else {
            // TODO should be an error
            Ok(0)
        }
    }
}

impl<'a> LogManagerT for FileLogManager<'a> {
    type Out = File;

    fn write(&mut self, line: String) -> crate::Result<()> {
        if self.need_new_writer(&line)? {
            self.writer = Some(self.make_new_writer()?);
        }
        let writer = self.writer.as_mut().unwrap();
        writer.write(line)?;
        Ok(())
    }

    fn make_new_writer(&mut self) -> crate::Result<LogWriter<Self::Out>> {
        let fname = self.generate_fname()?;
        let path = self.config.log_dir.join(fname);
        self.current_path = Some(path.clone());
        let file = File::options()
            .create_new(true)
            .append(true)
            .open(path)?;
        Ok(LogWriter::new(file))
    }
}
