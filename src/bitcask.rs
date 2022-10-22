use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::str::from_utf8;

use log::info;

use crate::command;
use crate::config::Config;
use crate::keydir::KeyDir;
use crate::lib::Result;
use crate::log_manager::{FileLogManager, LogManagerT};
use crate::log_writer::LogEntry;

pub struct BitCask<'a> {
    config: &'a Config<'a>,
    // TODO probably should just be `dyn LogWriterT`?
    log_manager: FileLogManager<'a>,
    keydir: KeyDir,
}

impl<'a> BitCask<'a> {
    pub fn new(config: &'a Config<'a>) -> Self {
        let log_manager = FileLogManager::new(config);
        let files = log_manager.get_closed_files();
        Self { 
            config,
            log_manager,
            keydir: KeyDir::scan(files),
        }
    }

    pub fn set(&mut self, cmd: command::Set) -> Result<()> {
        info!("{:?}", cmd);
        let entry = LogEntry::from_set_command(&cmd)?;
        self.log_manager.write(entry.serialize())?;
        let val_pos: u64 = self.log_manager.position()? - 1 - entry.val_sz() as u64;
        let path = self.log_manager.current_path.as_ref().unwrap();
        self.keydir.update(
            path.file_name().unwrap().into(),
            &entry,
            val_pos,
        );
        Ok(())
    }

    pub fn get(&self, cmd: command::Get) -> Result<String> {
        info!("{:?}", cmd);
        let command::Get(key) = cmd;
        if let Some(item) = self.keydir.get(&key) {
            let path = self.config.log_dir.join(PathBuf::from(item.file_id.clone()));
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(item.val_pos))?;
            let mut buf = vec![0u8; item.val_sz];
            file.read_exact(&mut buf)?;
            Ok(from_utf8(&buf[..])?.to_string())
        } else {
            // TODO should be a special miss error
            Err("Miss".into())
        }
    }
}
