use log::info;

use crate::command;
use crate::keydir::KeyDir;
use crate::log_manager::LogManagerT;
use crate::log_writer::LogEntry;
use crate::Result;

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
            Ok(self.log_manager.get(item)?)
        } else {
            // TODO should be a special miss error
            Err("Miss".into())
        }
    }
}
