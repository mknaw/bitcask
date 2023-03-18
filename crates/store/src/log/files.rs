use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::debug;

use crate::config::StoreConfig;
use crate::keydir::Item;
use crate::log::read::{Reader, ReaderItem};
use crate::log::LogEntry;
use crate::Result;

#[derive(Debug)]
pub struct FileHandle {
    writable: bool,
    pub path: PathBuf,
    inner: File,
}

impl FileHandle {
    pub fn new(path: PathBuf, writable: bool) -> Result<Self> {
        let exists = path.exists();
        if writable && exists {
            return Err(format!("Can't write to existing file: {:?}", path).into());
        }
        let inner = File::options()
            .create_new(!exists)
            .read(true)
            .append(writable)
            .open(&path)?;
        Ok(Self {
            writable,
            path,
            inner,
        })
    }

    pub fn rewind(&mut self) -> Result<()> {
        self.inner.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn read_item(&mut self, item: &Item) -> Result<String> {
        debug!("Reading {:?} from {:?}", item, self.inner);
        self.inner.seek(SeekFrom::Start(item.val_pos))?;
        let mut buf = vec![0u8; item.val_sz];
        self.inner.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }

    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            writable: self.writable,
            path: self.path.clone(),
            inner: self.inner.try_clone()?,
        })
    }

    pub fn close_for_write(handle: Self) -> Result<Self> {
        // TODO this is pretty clunky, make it prettier
        Self::new(handle.path, false)
    }
}

impl Write for FileHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // TODO `Err` when not `writable`
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Read for FileHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for FileHandle {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[derive(Debug, Default)]
pub struct FileManager {
    config: Arc<StoreConfig>,
    // TODO would prefer to keep just a ref to the handle itself rather than have to look it up,
    // but haven't been able to solve the borrow-checker complexities involved.
    pub current: Option<PathBuf>,
    // TODO only temporarily `pub`!
    pub inner: BTreeMap<PathBuf, FileHandle>,
}

impl FileManager {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        Self {
            config,
            current: None,
            inner: BTreeMap::default(),
        }
    }

    pub fn initialize_from_log_dir(&mut self) -> Result<()> {
        for entry in std::fs::read_dir(&self.config.log_dir)?.flatten() {
            let path = entry.path();
            let handle = FileHandle::new(path, false).unwrap();
            self.insert(handle);
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &FileHandle> {
        self.inner.values()
    }

    pub fn iter_closed(&self) -> impl Iterator<Item = &FileHandle> {
        self.iter().filter(|f| !f.writable)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut FileHandle> {
        self.inner.values_mut()
    }

    pub fn read_all_items(&mut self) -> impl Iterator<Item = ReaderItem> + '_ {
        self.iter_mut().flat_map(|handle| {
            let reader = Reader::new(handle);
            // TODO probably should log errors instead of just flattening!
            reader.flatten().collect::<Vec<_>>()
        })
    }

    pub fn try_clone(&self) -> crate::Result<Self> {
        let inner = self
            .inner
            .iter()
            .map(|(id, handle)| (id.clone(), handle.try_clone().unwrap()))
            .collect();
        // TODO not sure if should be setting `current: None`
        Ok(Self {
            config: self.config.clone(),
            current: None,
            inner,
        })
    }

    pub fn get_current_mut(&mut self) -> Result<&mut FileHandle> {
        // TODO might not have it open! handle properly with an `Error` type.
        let current = self.current.as_ref().unwrap();
        // TODO hate copypasta here but not sure how to call `self.get_mut` without running afoul
        // of borrow checker...
        self.inner
            .get_mut(current)
            .ok_or_else(|| format!("No log file found for id: {:?}", current).into())
    }

    pub fn get_mut(&mut self, id: &PathBuf) -> crate::Result<&mut FileHandle> {
        self.inner
            .get_mut(id)
            .ok_or_else(|| format!("No log file found for id: {:?}", id).into())
    }

    pub fn insert(&mut self, handle: FileHandle) {
        self.inner.insert(handle.path.clone(), handle);
    }

    pub fn remove(&mut self, path: &PathBuf) -> Option<FileHandle> {
        self.inner.remove(path)
    }

    fn new_file_name(&self) -> OsString {
        // Maybe you'd want to call the merge files something different, but OK for now.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| format!("{}.cask", d.as_micros()))
            .unwrap()
            .into()
    }

    fn rotate(&mut self) -> Result<()> {
        // Close current for write
        if let Some(current) = self.current.take() {
            let old = self.inner.remove(&current);
            if let Some(old) = old {
                self.insert(FileHandle::close_for_write(old)?);
            }
        }

        // TODO should hit this with an `ok_or`?
        let file_name = self.new_file_name();
        let path = self.config.log_dir.join(file_name);
        debug!("Opening new write file {:?}", path);
        let write_handle = FileHandle::new(path.clone(), true)?;
        self.insert(write_handle);
        self.current = Some(path);
        Ok(())
    }

    pub fn will_fit(&mut self, line: &[u8]) -> Result<bool> {
        let current = self.get_current_mut()?;
        Ok(line.len() as u64 + current.stream_position()? <= self.config.max_log_file_size)
    }

    pub fn write(&mut self, line: &[u8]) -> Result<(PathBuf, u64)> {
        if self.current.is_none() || !self.will_fit(line)? {
            self.rotate()?;
        };
        let current = self.get_current_mut()?;
        current.write_all(line)?;
        Ok((current.path.clone(), current.stream_position()?))
    }

    pub fn set(&mut self, entry: &LogEntry) -> Result<Item> {
        let line = entry.serialize_with_crc();
        let (path, position) = self.write(line.as_bytes())?;
        let val_pos = position - entry.val_sz();
        Ok(Item {
            path,
            val_sz: entry.val.len(),
            val_pos,
            ts: entry.ts,
        })
    }
}
