use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use log::error;

use crate::config::Config;
use crate::keydir::{Item, KeyDir};
use crate::log::read_file::LogFile;
use crate::log::write_file::{LogWriter, LogWriterT};
use crate::log::LogEntry;

pub trait LogManagerT {
    type Out: Write + Seek;

    fn initialize_keydir(&mut self) -> KeyDir;
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
    fn get(&mut self, item: &Item) -> crate::Result<String>;
    fn position(&mut self) -> crate::Result<u64>;
}

pub struct FileLogManager<'a> {
    config: &'a Config<'a>,
    pub current_path: Option<PathBuf>,
    writer: Option<LogWriter<File>>,
    read_files: BTreeMap<OsString, LogFile>,
}

// TODO implement locking
impl<'a> FileLogManager<'a> {
    pub fn new(config: &'a Config<'a>) -> crate::Result<Self> {
        let mut manager = Self {
            config,
            // TODO this shouldn't really live here... probably
            current_path: None,
            writer: None,
            read_files: Default::default(),
        };
        manager.initialize_read_files()?;
        Ok(manager)
    }

    fn initialize_read_files(&mut self) -> crate::Result<()> {
        for path in self.get_read_file_paths()? {
            match File::options().read(true).write(false).open(&path) {
                Ok(file) => {
                    self.read_files
                        .insert(path.into_os_string(), LogFile::new(file));
                }
                _ => log::error!("Error opening {:?}; skipped.", path),
            }
        }
        Ok(())
    }

    // TODO probably just have to return path refs here
    fn get_read_file_paths(&self) -> crate::Result<Vec<PathBuf>> {
        let mut paths = std::fs::read_dir(&self.config.log_dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        paths.sort();
        Ok(paths)
    }

    fn make_new_writer(&mut self) -> crate::Result<LogWriter<File>> {
        let fname = self.generate_fname()?;
        let path = self.config.log_dir.join(fname);
        self.current_path = Some(path.clone());
        let file = File::options().create_new(true).append(true).open(path)?;
        Ok(LogWriter::new(file))
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

    fn initialize_keydir(&mut self) -> KeyDir {
        let mut keydir = KeyDir::default();
        for (file_id, read_file) in self.read_files.iter_mut() {
            for item in read_file.items() {
                match item {
                    Ok((entry, val_pos)) => {
                        keydir.set(
                            entry.key.clone(),
                            Item {
                                file_id: file_id.to_os_string(),
                                val_sz: entry.val_sz() as usize,
                                val_pos,
                                ts: entry.ts,
                            },
                        );
                    }
                    _ => {
                        error!("Problem encountered parsing log file {:?}", file_id);
                    }
                }
            }
        }
        keydir
    }

    fn position(&mut self) -> crate::Result<u64> {
        if let Some(ref mut writer) = self.writer {
            writer.stream_position()
        } else {
            // TODO should be an error
            Ok(0)
        }
    }

    fn get(&mut self, item: &Item) -> crate::Result<String> {
        let file = self.read_files.get_mut(&item.file_id).unwrap();
        file.read(item.val_pos, item.val_sz)
    }
}
