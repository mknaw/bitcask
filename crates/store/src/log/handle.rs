use crate::config::StoreConfig;
use crate::log::read::{Reader, ReaderItem};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::{
    ffi::OsString,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    str::from_utf8,
};

use log::debug;

use crate::keydir::Item;
use crate::Result;

#[derive(Debug)]
pub struct ReadHandle {
    // TODO timestamp, for sorting
    pub id: OsString,
    pub path: PathBuf,
    out: File,
}

impl ReadHandle {
    pub fn new(id: OsString, path: PathBuf) -> Result<Self> {
        if !path.exists() {
            return Err(format!("File does not exist: {:?}", path).into());
        }
        let out = File::options().read(true).open(&path)?;
        Ok(Self { id, path, out })
    }

    pub fn from_write_handle(write_handle: &WriteHandle) -> Result<Self> {
        Self::new(write_handle.id.clone(), write_handle.path.clone())
    }

    pub fn rewind(&mut self) -> Result<()> {
        self.out.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn read_item(&mut self, item: &Item) -> Result<String> {
        debug!("Reading {:?} from {:?}", item, self.out);
        self.out.seek(SeekFrom::Start(item.val_pos))?;
        let mut buf = vec![0u8; item.val_sz];
        self.out.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }

    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            id: self.id.clone(),
            path: self.path.clone(),
            out: self.out.try_clone()?,
        })
    }
}

impl Read for ReadHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.out.read(buf)
    }
}

impl Seek for ReadHandle {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.out.seek(pos)
    }
}

#[derive(Debug, Default)]
pub struct HandleMap {
    // TODO only temporarily `pub`!
    pub inner: BTreeMap<OsString, ReadHandle>,
}

impl HandleMap {
    pub fn new(config: &Arc<StoreConfig>) -> crate::Result<Self> {
        let mut paths = std::fs::read_dir(&config.log_dir)?
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        let inner = paths
            .drain(..)
            .map(|path| {
                let id = path.file_name().unwrap().to_os_string();
                let handle = ReadHandle::new(id.clone(), path).unwrap();
                (id, handle)
            })
            .collect();
        Ok(Self { inner })
    }

    pub fn read_all_items(&mut self) -> impl Iterator<Item = ReaderItem> + '_ {
        self.inner.values_mut().flat_map(|handle| {
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
        Ok(Self { inner })
    }

    pub fn get(&mut self, id: &OsString) -> crate::Result<&mut ReadHandle> {
        self.inner
            .get_mut(id)
            .ok_or_else(|| format!("No log file found for id: {:?}", id).into())
    }

    pub fn insert(&mut self, handle: ReadHandle) {
        self.inner.insert(handle.id.clone(), handle);
    }

    pub fn remove(&mut self, id: &OsString) -> Option<ReadHandle> {
        self.inner.remove(id)
    }
}

#[derive(Debug)]
pub struct WriteHandle {
    // TODO timestamp, for sorting
    pub id: OsString,
    pub path: PathBuf,
    out: File,
}

impl WriteHandle {
    pub fn new(id: OsString, path: PathBuf) -> Result<Self> {
        if path.exists() {
            return Err(format!("File already exists: {:?}", path).into());
        }
        let out = File::options()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self { id, path, out })
    }

    // TODO ???
    pub fn rewind(&mut self) -> Result<()> {
        self.out.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn read_item(&mut self, item: &Item) -> Result<String> {
        debug!("Reading {:?} from {:?}", item, self.out);
        self.out.seek(SeekFrom::Start(item.val_pos))?;
        let mut buf = vec![0u8; item.val_sz];
        self.out.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }
}

impl Write for WriteHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.out.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

impl Seek for WriteHandle {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.out.seek(pos)
    }
}
