use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::str::from_utf8;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command;
use crate::config::Config;
use crate::keydir::KeyDir;
use crate::logwriter::LogWriter;

pub struct BitCask<'a> {
    config: &'a Config<'a>,
    logfile: Option<PathBuf>,
    keydir: KeyDir,
}

impl<'a> BitCask<'a> {
    pub fn new(config: &'a Config<'a>) -> Self {
        Self { 
            config,
            // TODO implement locking?
            logfile: None,
            keydir: KeyDir::new(),
        }
    }

    // pub fn get_logfile(&mut self) -> Result<PathBuf, Box<dyn Error>> {
        // if self.logfile.is_none() {
            // self.make_new_logfile()?;
        // }
        // if let Some(path) = self.logfile {
            // Ok(path.clone())
        // } else {
            // panic!("Ba")
        // }
    // }

    // TODO make_new_logfile when current above size threshold
    fn make_new_logfile(&mut self) -> Result<(), Box<dyn Error>> {
        // TODO also have to make directory if it does not exist.
        let fname = self.generate_fname()?;
        let path = self.config.log_dir.join(fname);
        self.logfile = Some(path);
        Ok(())
    }

    fn generate_fname(&self) -> Result<String, Box<dyn Error>> {
        let ts: u64 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        // TODO append some random junk to avoid collisions?
        Ok(format!("{}.cask", ts))
    }

    pub fn set(&mut self, cmd: command::Set) -> Result<(), Box<dyn Error>> {
        self.make_new_logfile()?;
        if let Some(path) = &self.logfile {
            let file = File::options()
                .create_new(true)
                .append(true)
                .open(path)
                .unwrap();
            // TODO probably better to have a single LogWriter with a buffer?
            let mut writer = LogWriter::new(file);
            let entry = Entry::from_set(&cmd)?;
            writer.write(entry.serialize())?;
            let val_pos: u64 = writer.stream_position()? - entry.val_sz() as u64;
            self.keydir.update(
                path.file_name().unwrap().into(),
                &entry,
                val_pos,
            );
            Ok(())
        } else {
            // TODO !!! should return Err
            Ok(())
        }
    }

    // TODO should take a Command::Get ultimately.
    pub fn get(&self, key: String) -> Result<String, Box<dyn Error>> {
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
