use log::info;
use std::fmt;

use crate::command;
use crate::keydir::KeyDir;
use crate::log_manager::LogManagerT;
use crate::log_writer::LogEntry;
use crate::Result;

#[derive(Debug)]
struct KeyMiss(String);

impl std::error::Error for KeyMiss {
    fn description(&self) -> &str {
        "key miss"
    }
}

impl fmt::Display for KeyMiss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeyMiss: {}", self.0)
    }
}

pub struct BitCask<LM: LogManagerT> {
    log_manager: LM,
    keydir: KeyDir,
}

impl<LM: LogManagerT> BitCask<LM> {
    pub fn new(log_manager: LM) -> Self {
        let keydir = log_manager.initialize_keydir();
        Self {
            log_manager,
            keydir,
        }
    }

    pub fn set(&mut self, cmd: command::Set) -> Result<()> {
        info!("{:?}", cmd);
        let entry = LogEntry::from_set_command(&cmd)?;
        let key = entry.key.clone();
        let item = self.log_manager.set(entry)?;
        self.keydir.set(key, item);
        Ok(())
    }

    pub fn get(&self, cmd: command::Get) -> Result<String> {
        info!("{:?}", cmd);
        let command::Get(key) = cmd;
        if let Some(item) = self.keydir.get(&key) {
            let value = self.log_manager.get(item)?;
            if !crate::is_tombstone(&value) {
                return Ok(value);
            }
        }
        Err(KeyMiss(key).into())
    }

    pub fn delete(&mut self, cmd: command::Delete) -> Result<()> {
        info!("{:?}", cmd);
        let command::Delete(key) = cmd;
        self.set(command::Set {
            key,
            val: crate::TOMBSTONE.to_string(),
        })
    }
}
