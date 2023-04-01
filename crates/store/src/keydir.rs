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
    // TODO key should be as bytes! len may be off.
    pub fn serialize_as_hint(&self, key: &str) -> Vec<u8> {
        let key_sz = key.len();
        let mut serialized = Vec::with_capacity(16 + 3 * 8 + key_sz);
        serialized.extend(self.ts.to_ne_bytes());
        serialized.extend((key_sz as u64).to_ne_bytes());
        serialized.extend(self.val_sz.to_ne_bytes());
        serialized.extend(self.val_pos.to_ne_bytes());
        serialized.extend(key.as_bytes());
        serialized
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
