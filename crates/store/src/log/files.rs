use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::debug;
use memmap2::{Mmap, MmapOptions};

use crate::config::StoreConfig;
use crate::keydir::Item;
use crate::log::read::LogReaderItem;
use crate::log::LogEntry;
use crate::Result;

#[derive(Debug)]
pub struct FileHandle {
    writable: bool,
    pub path: PathBuf,
    inner: File,
    mmap: Option<Mmap>,
    pub offset: u64,
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
            mmap: None,
            offset: 0,
        })
    }

    /// Return the length of the associated `File`.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.inner.metadata().unwrap().len()
    }

    /// Memory-maps the associated `File`.
    pub fn memory_map(&mut self, max_log_file_size: u64) {
        let len = if self.writable {
            // TODO this is particularly dicey with writes whose values exceed the
            // `max_log_file_size`. Need safeguards around that.
            max_log_file_size
        } else {
            self.len()
        };
        let mmap = unsafe {
            MmapOptions::new()
                .len(len as usize)
                .map(&self.inner)
                .expect("failed to map the file")
        };
        self.mmap = Some(mmap);
    }

    pub fn is_at_end(&self) -> bool {
        self.offset >= self.len()
    }

    pub fn read_entry(&mut self, start: usize) -> Result<LogEntry> {
        self.seek(SeekFrom::Start(start as u64))?;
        let mut metadata = [0u8; 4 + 16 + 2 * 8];
        self.read_exact(&mut metadata)?;
        let crc = u32::from_ne_bytes(metadata[0..4].try_into().unwrap());
        let ts = u128::from_ne_bytes(metadata[4..20].try_into().unwrap());
        let key_sz = u64::from_ne_bytes(metadata[20..28].try_into().unwrap());
        let val_sz = u64::from_ne_bytes(metadata[28..36].try_into().unwrap());
        let mut key = vec![0u8; key_sz as usize];
        self.read_exact(&mut key)?;
        let mut val = vec![0u8; val_sz as usize];
        self.read_exact(&mut val)?;
        let entry = LogEntry { key, val, ts };
        if entry.crc() != crc {
            // TODO should `Err` here!
            debug!("TODO mismatched CRC!");
        };
        Ok(entry)
    }

    pub fn read_item(&mut self, item: &Item) -> Result<Vec<u8>> {
        let entry = self.read_entry(item.val_pos as usize)?;
        Ok(entry.val)
    }

    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            writable: self.writable,
            path: self.path.clone(),
            inner: self.inner.try_clone()?,
            mmap: None,
            offset: 0,
        })
    }

    pub fn close_for_write(handle: Self) -> Result<Self> {
        // TODO this is pretty clunky, make it prettier
        Self::new(handle.path, false)
    }

    pub fn get_hint_file(&self, writable: bool) -> Option<Self> {
        // TODO if self extension is hint, return None?
        let mut hint_path = self.path.clone();
        hint_path.set_extension("hint");
        if !(hint_path.exists() || writable) {
            return None;
        }

        Self::new(hint_path, writable).ok()
    }
}

impl Read for FileHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_written = match self.mmap.as_ref() {
            Some(mmap) => {
                let start = self.offset as usize;
                let end = std::cmp::min(start + buf.len(), mmap.len());
                // TODO not sure we always want to copy to the start of buf... maybe
                buf[..(end - start)].copy_from_slice(&mmap[start..end]);
                end - start
            }
            None => self.inner.read(buf)?,
        };
        self.offset += bytes_written as u64;
        Ok(bytes_written)
    }
}

impl Write for FileHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // TODO `Err` when not `writable`
        let bytes_written = self.inner.write(buf)?;
        self.offset += bytes_written as u64;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for FileHandle {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.offset = match self.mmap.as_ref() {
            Some(mmap) => {
                match pos {
                    SeekFrom::Start(k) => k,
                    SeekFrom::Current(k) => self.offset + k as u64,
                    // TODO mmap len or file len? Or just err out since we shouldn't be using?
                    SeekFrom::End(k) => mmap.len() as u64 + k as u64,
                }
            }
            None => self.inner.seek(pos)?,
        };
        Ok(self.offset)
    }
}

// TODO really should just be an `Iterator` for `FileHandle`.
impl Iterator for FileHandle {
    type Item = Result<LogReaderItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_at_end() {
            return None;
        }
        let val_pos = self.offset;
        let entry = self.read_entry(val_pos as usize).unwrap();
        debug!("Read: {}", entry);

        Some(Ok(LogReaderItem {
            path: self.path.clone(),
            entry,
            val_pos,
        }))
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
        for entry in std::fs::read_dir(&self.config.log_dir)?
            .flatten()
            .filter(|dir_entry| dir_entry.path().extension() == Some(OsStr::new("cask")))
        {
            let path = entry.path();
            let mut handle = FileHandle::new(path, false).unwrap();
            handle.memory_map(self.config.max_log_file_size);
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
                let mut read_handle = FileHandle::close_for_write(old)?;
                read_handle.memory_map(self.config.max_log_file_size);
                self.insert(read_handle);
            }
        }

        // TODO should hit this with an `ok_or`?
        let file_name = self.new_file_name();
        let path = self.config.log_dir.join(file_name);
        debug!("Opening new write file {:?}", path);
        let mut write_handle = FileHandle::new(path.clone(), true)?;
        write_handle.memory_map(self.config.max_log_file_size);
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
        let serialized = entry.serialize_with_crc();
        let line = serialized.as_slice();
        let (path, position) = self.write(line)?;
        let val_pos = position - (line.len() as u64);
        Ok(Item {
            path,
            val_sz: entry.val.len(),
            val_pos,
            ts: entry.ts,
        })
    }

    pub fn read_item(&mut self, item: &Item) -> Result<Vec<u8>> {
        let handle = self.get_mut(&item.path)?;
        handle.read_item(item)
    }

    fn get_hint_file_for_current(&self) -> Option<File> {
        self.current.as_ref().map(|path| {
            let mut hint_path = path.clone();
            hint_path.set_extension("hint");
            File::options()
                .create_new(!hint_path.exists())
                .append(true)
                .open(hint_path)
                .unwrap() // TODO
        })
    }

    pub fn write_hint(&self, hint: &[u8]) -> Result<()> {
        let mut hint_file = self.get_hint_file_for_current().unwrap();
        hint_file.write_all(hint)?;
        Ok(())
    }
}
