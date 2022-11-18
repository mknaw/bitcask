use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{debug, error, info};

use crate::config::Config;
use crate::keydir::{Item, KeyDir};
use crate::log::handle::{Handle, SharedHandle};
use crate::log::read::{Reader, ReaderItem};
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

pub struct FileLogManager<'cfg> {
    config: &'cfg Config<'cfg>,
    writer: Writer<'cfg>,
    handles: BTreeMap<OsString, SharedHandle>,
}

impl<'cfg> FileLogManager<'cfg> {
    pub fn new(config: &'cfg Config<'cfg>) -> crate::Result<Self> {
        let writer = Writer::new(
            config,
            Arc::new(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|d| format!("{}.cask", d.as_micros()))
                    .unwrap()
            }),
        );

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
        let mut paths = std::fs::read_dir(self.config.log_dir)?
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
    }

    fn add_read_file(&mut self, path: &Path) -> crate::Result<()> {
        let id = path.file_name().unwrap();
        self.handles.insert(
            path.file_name().unwrap().to_os_string(),
            Handle::new_shared(id.to_os_string(), path.to_path_buf(), false)?,
        );
        Ok(())
    }

    pub fn read_all_items(&self) -> impl Iterator<Item = ReaderItem> + '_ {
        self.handles.values().flat_map(|handle| {
            let handle = &mut handle.write().unwrap();
            let reader = Reader::new(handle);
            // TODO probably should log errors instead of just flattening!
            reader.flatten().collect::<Vec<_>>()
        })
    }

    pub fn get(&mut self, item: &Item) -> crate::Result<String> {
        debug!("Getting item {:?}", item);
        let handle = self
            .handles
            .get_mut(&item.file_id)
            .ok_or_else(|| ManagerError(format!("File not found: {:?}", item.file_id)))?;
        handle.write().unwrap().read_item(item)
    }

    pub fn set(&mut self, entry: &LogEntry) -> crate::Result<Item> {
        let (file_id, val_pos) = self.write(entry)?;
        Ok(Item {
            file_id,
            val_sz: entry.val.len(),
            val_pos,
            ts: entry.ts,
        })
    }

    fn write(&mut self, entry: &LogEntry) -> crate::Result<(OsString, u64)> {
        let line = entry.serialize_with_crc();
        let WriteResult { handle, position } = self.writer.write(line.as_bytes())?;
        let id = handle.read().unwrap().id.clone();
        self.handles.insert(id.clone(), handle);
        // TODO should the writer itself do this subtraction?
        Ok((id, position - entry.val_sz()))
    }

    pub fn merge(&mut self, keydir: &mut KeyDir) -> crate::Result<()> {
        // TODO this is all sorts of fucked up!
        let last = self
            .handles
            .iter()
            .next_back()
            .unwrap()
            .0
            .to_str()
            .unwrap()
            .to_string();
        let mut merge_writer = Writer::new(
            self.config,
            Arc::new(move |k| format!("{}.merge.{}", last, k)),
        );

        let mut write_results = vec![];
        for handle in self
            .handles
            .values()
            .collect::<Vec<&SharedHandle>>()
            .clone()
        {
            let handle = &mut handle.write().unwrap();
            handle.rewind()?;
            // TODO should we just be able to iterate over items from the `handle`?
            let reader = Reader::new(handle);
            // TODO should at least log something about encountering parse errors along the way.
            for ReaderItem { entry, .. } in reader.flatten() {
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
                                file_id: write_result.handle.read().unwrap().id.clone(),
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

        for handle in self.handles.values().map(|h| h.read().unwrap()) {
            info!("Removing {:?}", handle.path);
            std::fs::remove_file(&handle.path)?;
        }

        // TODO hint file
        self.writer.reset();
        self.handles.clear();
        for write_result in write_results {
            // TODO some copypasta here
            let handle = write_result.handle;
            let id = handle.read().unwrap().id.clone();
            self.handles.insert(id.clone(), handle);
        }

        Ok(())
    }
}
