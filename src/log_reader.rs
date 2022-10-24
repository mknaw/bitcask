use std::io::{BufRead, Lines, BufReader};
use std::fs::File;
use std::path::{PathBuf, Path};

use crate::keydir::Item;
use crate::Result;
use crate::log_writer::LogEntry;

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
            let entry = LogEntry::deserialize(&line).ok()?;
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
