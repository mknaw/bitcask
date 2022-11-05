use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{debug, error, info};

use crate::config::Config;
use crate::keydir::{Item, KeyDir};
use crate::log::handle::{Handle, SharedHandle};
use crate::log::read::Reader;
use crate::log::write::{WriteResult, Writer};
use crate::log::LogEntry;

// TODO did this for expediency, but really ought to have a couple different types of error
#[derive(Debug)]
pub struct ManagerError(String);

impl std::error::Error for ManagerError {
    fn description(&self) -> &str {
        "manager error"
    }
}

impl fmt::Display for ManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ManagerError: {}", self.0)
    }
}

pub struct TimeStampNameIterator();

impl Iterator for TimeStampNameIterator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| format!("{}.cask", d.as_micros()))
    }
}

pub struct MergeFileNameIterator {
    base: String,
    counter: i8,
}

impl MergeFileNameIterator {
    pub fn new(base: String) -> Self {
        Self { base, counter: 0 }
    }
}

impl Iterator for MergeFileNameIterator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let name = format!("{}.merge.{}", self.base, self.counter);
        self.counter += 1;
        Some(name)
    }
}

pub trait LogManagerT {
    type Out: Write + Seek;

    fn initialize_keydir(&mut self) -> KeyDir;
    fn set(&mut self, entry: &LogEntry) -> crate::Result<Item> {
        let (file_id, val_pos) = self.write(entry)?;
        Ok(Item {
            file_id,
            val_sz: entry.val.len(),
            val_pos,
            ts: entry.ts,
        })
    }
    fn write(&mut self, entry: &LogEntry) -> crate::Result<(OsString, u64)>;
    fn get(&mut self, item: &Item) -> crate::Result<String>;

    fn merge(&mut self, keydir: &mut KeyDir) -> crate::Result<()>;

    fn add_read_file(&mut self, path: &Path) -> crate::Result<()>;
}

pub struct FileLogManager<'cfg> {
    config: &'cfg Config<'cfg>,
    writer: Writer<'cfg>,
    handles: BTreeMap<OsString, SharedHandle>,
}

impl<'cfg> FileLogManager<'cfg> {
    pub fn new(config: &'cfg Config<'cfg>) -> crate::Result<Self> {
        let writer = Writer::new(config, Box::new(TimeStampNameIterator()));

        let mut manager = Self {
            config,
            writer,
            handles: Default::default(),
        };
        manager.initialize_read_files()?;
        Ok(manager)
    }

    fn initialize_read_files(&mut self) -> crate::Result<()> {
        for path in self.get_read_file_paths()? {
            match self.add_read_file(&path) {
                Ok(_) => info!("Added read file {:?}", path),
                Err(e) => error!("Error opening {:?}: {}", path, e),
            }
        }
        Ok(())
    }

    fn get_read_file_paths(&self) -> crate::Result<Vec<PathBuf>> {
        let mut paths = std::fs::read_dir(&self.config.log_dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        paths.sort();
        Ok(paths)
    }
}

impl<'cfg> LogManagerT for FileLogManager<'cfg> {
    type Out = File;

    fn write(&mut self, entry: &LogEntry) -> crate::Result<(OsString, u64)> {
        let line = entry.serialize_with_crc();
        let WriteResult { handle, position } = self.writer.write(line.as_bytes())?;
        let id = handle.borrow().id.clone();
        self.handles.insert(id.clone(), handle);
        // TODO should the writer itself do this subtraction?
        Ok((id, position - entry.val_sz()))
    }

    // TODO maybe the `BitCask` should take care of this.
    fn initialize_keydir(&mut self) -> KeyDir {
        let mut keydir = KeyDir::default();
        for (file_id, read_file) in self.handles.iter_mut() {
            let handle = &mut read_file.borrow_mut();
            let reader = Reader::new(handle);
            for item in reader {
                match item {
                    Ok((entry, val_pos)) => {
                        keydir.set(
                            entry.key.clone(),
                            Item {
                                file_id: file_id.to_os_string(),
                                val_sz: entry.val_sz() as usize,
                                val_pos,
                                ts: entry.ts,
                            },
                        );
                    }
                    _ => {
                        error!("Problem encountered parsing log file {:?}", file_id);
                    }
                }
            }
        }
        keydir
    }

    fn get(&mut self, item: &Item) -> crate::Result<String> {
        debug!("Getting item {:?}", item);
        let handle = self
            .handles
            .get_mut(&item.file_id)
            .ok_or(ManagerError(format!("File not found: {:?}", item.file_id)))?;
        handle.borrow_mut().read_item(item)
    }

    fn merge(&mut self, keydir: &mut KeyDir) -> crate::Result<()> {
        let last = self.handles.iter().next_back().unwrap().0;
        let mut merge_writer = Writer::new(
            self.config,
            // TODO super tedious conversions
            Box::new(MergeFileNameIterator::new(
                last.to_str().unwrap().to_string(),
            )),
        );

        let mut write_results = vec![];
        for handle in self
            .handles
            .values()
            .collect::<Vec<&SharedHandle>>()
            .clone()
        {
            let handle = &mut handle.borrow_mut();
            handle.rewind()?;
            // TODO should we just be able to iterate over items from the `handle`?
            let reader = Reader::new(handle);
            // TODO should at least log something about encountering parse errors along the way.
            for (entry, _val_pos) in reader.flatten() {
                info!("Merging {:?}", entry);
                if let Some(item) = keydir.get(&entry.key) {
                    if item.ts == entry.ts {
                        // TODO unfortunate copypasta here
                        let write_result =
                            merge_writer.write(entry.serialize_with_crc().as_bytes())?;
                        debug!("Wrote with result {:?}", write_result);
                        keydir.set(
                            entry.key.clone(),
                            Item {
                                file_id: write_result.handle.borrow().id.clone(),
                                val_sz: entry.val_sz() as usize,
                                val_pos: write_result.position - entry.val_sz() as u64,
                                ts: entry.ts,
                            },
                        );
                        write_results.push(write_result);
                    }
                }
            }
        }

        for handle in self.handles.values().map(|h| h.borrow()) {
            info!("Removing {:?}", handle.path);
            std::fs::remove_file(&handle.path)?;
        }

        // TODO hint file
        self.writer.reset();
        self.handles.clear();
        for write_result in write_results {
            // TODO some copypasta here
            let handle = write_result.handle;
            let id = handle.borrow().id.clone();
            self.handles.insert(id.clone(), handle);
        }

        Ok(())
    }

    fn add_read_file(&mut self, path: &Path) -> crate::Result<()> {
        let id = path.file_name().unwrap();
        self.handles.insert(
            path.file_name().unwrap().to_os_string(),
            Handle::new_shared(id.to_os_string(), path.to_path_buf(), false)?,
        );
        Ok(())
    }
}
