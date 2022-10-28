use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str::from_utf8;
use std::time::{SystemTime, UNIX_EPOCH};

use log::info;

use crate::config::Config;
use crate::keydir::{Item, KeyDir};
use crate::log_reader::LogReader;
use crate::log_writer::{LogEntry, LogWriter, LogWriterT};

pub trait LogManagerT {
    type Out: Write + Seek;

    fn initialize_keydir(&self) -> KeyDir;
    fn get_file_id(&self) -> OsString;
    fn set(&mut self, entry: LogEntry) -> crate::Result<Item> {
        let line = entry.serialize_with_crc();
        self.write(line)?;
        Ok(Item {
            file_id: self.get_file_id(),
            val_sz: entry.val.len(),
            val_pos: self.position()? - entry.val_sz() as u64,
            ts: entry.ts,
        })
    }
    fn write(&mut self, line: String) -> crate::Result<()>;
    fn get(&self, item: &Item) -> crate::Result<String>;
    fn position(&mut self) -> crate::Result<u64>;
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

    fn make_new_writer(&mut self) -> crate::Result<LogWriter<File>> {
        let fname = self.generate_fname()?;
        let path = self.config.log_dir.join(fname);
        self.current_path = Some(path.clone());
        let file = File::options().create_new(true).append(true).open(path)?;
        Ok(LogWriter::new(file))
    }

    pub fn get_closed_files(&self) -> Vec<PathBuf> {
        let mut files = std::fs::read_dir(&self.config.log_dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();
        files.sort();
        files
    }

    fn generate_fname(&self) -> crate::Result<String> {
        let ts: u64 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        // TODO append some random junk to avoid collisions?
        Ok(format!("{}.cask", ts))
    }

    fn need_new_writer(&mut self, line: &str) -> crate::Result<bool> {
        if self.writer.is_none() {
            Ok(true)
        } else {
            Ok(line.len() as u64 + self.position()? > self.config.max_log_file_size)
        }
    }
}

impl<'cfg> LogManagerT for FileLogManager<'cfg> {
    type Out = File;

    fn write(&mut self, line: String) -> crate::Result<()> {
        if self.need_new_writer(&line)? {
            self.writer = Some(self.make_new_writer()?);
        }
        let writer = self.writer.as_mut().unwrap();
        // TODO should be able to write it from a reference?
        writer.write(line.to_string())?;
        Ok(())
    }

    fn get_file_id(&self) -> OsString {
        self.current_path
            .as_ref()
            .unwrap()
            .as_os_str()
            .to_os_string()
    }

    fn initialize_keydir(&self) -> KeyDir {
        let mut keydir = KeyDir::default();
        for file_id in self.get_closed_files() {
            info!("{:?}", file_id);
            let reader = LogReader::new(file_id);
            for item in reader.items() {
                // TODO shouldn't be unwrapping here!
                let (key, item) = item.unwrap();
                keydir.set(key, item);
            }
        }
        keydir
    }

    fn position(&mut self) -> crate::Result<u64> {
        if let Some(ref mut writer) = self.writer {
            Ok(writer.stream_position()?)
        } else {
            // TODO should be an error
            Ok(0)
        }
    }

    fn get(&self, item: &Item) -> crate::Result<String> {
        let path = self
            .config
            .log_dir
            .join(PathBuf::from(item.file_id.clone()));
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(item.val_pos))?;
        let mut buf = vec![0u8; item.val_sz];
        file.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }
}
