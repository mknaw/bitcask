use std::path::PathBuf;
use std::{collections::HashMap, ffi::OsString};

use log::info;

use crate::log_reader::LogReader;
use crate::log_writer::LogEntry;

#[derive(PartialEq)]
pub struct Item {
    pub file_id: OsString,
    pub val_sz: usize,
    pub val_pos: u64,
    // TODO Should be the actual Rust timestamp type, just convert to whatever for serialization
    pub ts: u64,
}

pub struct KeyDir {
    data: HashMap<String, Item>,
}

impl KeyDir {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Item> {
        return self.data.get(key);
    }

    pub fn set(&mut self, key: String, item: Item) {
        self.data.insert(key, item);
    }

    pub fn scan(files: Vec<PathBuf>) -> Self {
        let mut keydir = Self::new();
        // TODO have to read the hint files, if they exist, before the original ones.
        for file_id in files {
            info!("{:?}", file_id);
            let reader = LogReader::new(file_id);
            for item in reader.items() {
                // TODO shouldn't be unwrapping here!
                let (key, item) = item.unwrap();
                keydir.data.insert(key, item);
            }
        }
        keydir
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{Item, KeyDir};

    use crate::bitcask::LogEntry;

    #[test]
    fn test_happy_update() {
        let mut state = KeyDir::new();
        // TODO randomize data?
        let key = "key".to_string();
        let file_id: OsString = "file".to_string().into();
        let entry = LogEntry {
            key: key.clone(),
            val: "val".to_string(),
            ts: 1,
        };
        let val_pos = 1;
        state.set(file_id.clone(), &entry, val_pos);
        if let Some(item) = state.get(&key) {
            assert!(
                item == &(Item {
                    file_id,
                    val_sz: entry.val_sz(),
                    val_pos,
                    ts: entry.ts,
                })
            );
        } else {
            assert!(false);
        }
    }
}
