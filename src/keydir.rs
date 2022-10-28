use std::{collections::HashMap, ffi::OsString};

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
}
