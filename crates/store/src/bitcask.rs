use log::debug;
use std::fmt;

use crate::keydir::KeyDir;
use crate::log::manager::LogManagerT;
use crate::log::LogEntry;
use crate::Result;

// TODO should this one be a &str?
// TODO reexport under `store::errors::...`?
// TODO should probably be in the KeyDir file.
#[derive(Debug)]
pub struct KeyMiss(String);

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
    pub fn new(mut log_manager: LM) -> Self {
        let keydir = log_manager.initialize_keydir();
        Self {
            log_manager,
            keydir,
        }
    }

    pub fn set(&mut self, key: &str, val: &str) -> Result<()> {
        debug!("Set {} to {}", key, val);
        let entry = LogEntry::from_set(key, val)?;
        let key = entry.key.clone();
        let item = self.log_manager.set(&entry)?;
        self.keydir.set(key, item);
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        debug!("Get {}", key);
        if let Some(item) = self.keydir.get(key) {
            // TODO if we are having file problems, should we evict from the keydir?
            let value = self.log_manager.get(item)?;
            if !crate::is_tombstone(&value) {
                return Ok(value);
            }
        }
        Err(KeyMiss(key.to_string()).into())
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        debug!("Delete {}", key);
        self.set(key, crate::TOMBSTONE)
    }

    pub fn should_merge(&mut self) -> bool {
        // TODO
        true
    }

    pub fn merge(&mut self) -> Result<()> {
        self.log_manager.merge(&mut self.keydir)
    }
}
