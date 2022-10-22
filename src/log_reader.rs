use std::io::{BufRead, Lines, BufReader};
use std::fs::File;
use std::path::{PathBuf, Path};

use log::info;

use crate::bitcask::Entry;
use crate::keydir::Item;
use crate::lib::Result;

pub struct LogReader {
    path: PathBuf,
}

impl LogReader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path
        }
    }

    fn open(&self) -> Result<File> {
        let file = File::open(&self.path)?;
        Ok(file)
    }

    pub fn items(&self) -> ItemIterator {
        let file = self.open().unwrap();
        let reader = BufReader::new(file);
        let lines = reader.lines();
        ItemIterator::new(self.path.as_ref(), lines)
    }
}

pub struct ItemIterator<'a> {
    path: &'a Path,
    lines: Lines<BufReader<File>>,
    position: usize,
}

impl<'a> ItemIterator<'a> {
    pub fn new(path: &'a Path, lines: Lines<BufReader<File>>) -> Self {
        Self {
            path,
            lines,
            position: 0,
        }
    }
}

impl<'a> Iterator for ItemIterator<'a> {
    type Item = Result<(String, Item)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(line) = self.lines.next() {
            let line = line.ok()?;
            self.position += line.len();
            let entry = parse_entry(line).ok()?;
            let val_sz = entry.val.len();
            Some(Ok((entry.key, Item {
                file_id: self.path.as_os_str().to_os_string(),
                val_sz,
                val_pos: (self.position - val_sz) as u64,
                ts: entry.ts,
            })))
        } else {
            None
        }
    }
}

pub fn parse_entry(line: String) -> Result<Entry> {

    let parts: Vec<_> = line.split(",").collect();

    // TODO assert that parts.len() > 3?

    let ts = parts[0].parse::<u64>()?;
    let key_sz = parts[1].parse::<usize>()?;
    // TODO probably should have val_sz, and then don't need newlines?

    let key_idx = parts[0].len() + parts[1].len() + parts[2].len() + 3;
    let key = &line[key_idx..key_idx + key_sz];

    let val_idx = key_idx + key_sz + 1;
    let val = &line[val_idx..];
    info!("ts: {}, key: {}, val: {}", ts, key, val);
    Ok(Entry {
        ts,
        key: key.to_owned(),
        val: val.to_owned(),
    })
}
