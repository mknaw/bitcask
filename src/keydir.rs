use std::path::PathBuf;
use std::{collections::HashMap, ffi::OsString};

use log::info;

use crate::log_reader::LogReader;

#[derive(PartialEq)]
pub struct Item {
    pub file_id: OsString,
    pub val_sz: usize,
    pub val_pos: u64,
    // TODO Should be the actual Rust timestamp type, just convert to whatever for serialization
    pub ts: u64,
}

#[derive(Default)]
pub struct KeyDir {
    data: HashMap<String, Item>,
}

impl KeyDir {
    pub fn get(&self, key: &str) -> Option<&Item> {
        self.data.get(key)
    }

    pub fn set(&mut self, key: String, item: Item) {
        self.data.insert(key, item);
    }

    pub fn scan(files: Vec<PathBuf>) -> Self {
        let mut keydir = Self::default();
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
