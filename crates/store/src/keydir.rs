use std::{collections::HashMap, ffi::OsString};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Item {
    pub file_id: OsString,
    pub val_sz: usize,
    pub val_pos: u64,
    pub ts: u128,
}

#[derive(Clone, Debug, Default)]
pub struct KeyDir {
    // TODO really shouldnt be `pub`
    pub data: HashMap<String, Item>,
}

impl KeyDir {
    pub fn get(&self, key: &str) -> Option<&Item> {
        self.data.get(key)
    }

    pub fn set(&mut self, key: String, item: Item) {
        self.data.insert(key, item);
    }
}
