use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Item {
    pub path: PathBuf,
    pub val_sz: usize,
    pub val_pos: u64,
    pub ts: u128,
}

impl Item {
    pub fn serialize_as_hint(&self, key: &str) -> String {
        let key_sz = key.len();
        format!(
            "{:032x}{:016x}{:016x}{:016x}{}",
            self.ts, key_sz, self.val_sz, self.val_pos, key,
        )
    }
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
