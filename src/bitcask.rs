use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::str::from_utf8;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command;
use crate::config::Config;
use crate::keydir::KeyDir;
use crate::log_manager::{FileLogManager, LogManagerT};

pub struct BitCask<'a> {
    config: &'a Config<'a>,
    // TODOO probably should just be `dyn LogWriterT`?
    log_manager: FileLogManager<'a>,
    keydir: KeyDir,
}

impl<'a> BitCask<'a> {
    pub fn new(config: &'a Config<'a>) -> Self {
        Self { 
            config,
            log_manager: FileLogManager::new(config),
            keydir: KeyDir::new(),
        }
    }

    pub fn set(&mut self, cmd: command::Set) -> crate::Result<()> {
        // TODO probably better to have a single LogWriter with a buffer?
        let entry = Entry::from_set(&cmd)?;
        self.log_manager.write(entry.serialize())?;
        let val_pos: u64 = self.log_manager.position()? - entry.val_sz() as u64;
        let path = self.log_manager.current_path.as_ref().unwrap();
        self.keydir.update(
            path.file_name().unwrap().into(),
            &entry,
            val_pos,
        );
        Ok(())
    }

    // TODO should take a Command::Get ultimately.
    pub fn get(&self, key: String) -> crate::Result<String> {
        if let Some(item) = self.keydir.get(&key) {
            let path = self.config.log_dir.join(PathBuf::from(item.file_id.clone()));
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(item.val_pos))?;
            let mut buf = vec![0u8; item.val_sz];
            file.read_exact(&mut buf)?;
            Ok(from_utf8(&buf[..])?.to_string())
        } else {
            // TODO !!! should return Err
            Ok("bad string".to_string())
        }
    }
}

pub struct Entry {
    // TODO CRC
    pub key: String,
    pub val: String,
    pub ts: u64,
}

impl Entry {
    pub fn from_set(cmd: &command::Set) -> Result<Self, Box<dyn Error>> {
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
        format!(
            "{},{},{},{},{}",
            self.ts,
            self.key_sz(),
            self.val_sz(),
            self.key,
            self.val,
        )
    }
}
