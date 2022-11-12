use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::rc::Rc;
use std::{
    ffi::OsString,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    str::from_utf8,
};

use log::debug;

use crate::keydir::Item;
use crate::Result;

pub struct Handle {
    // TODO timestamp, for sorting
    pub id: OsString,
    pub path: PathBuf,
    out: File,
}

pub type SharedHandle = Rc<RefCell<Handle>>;

// TODO would like something that "drops" / converts a write
// handle to a read-only one when the writer is done with it.
impl Handle {
    pub fn new(id: OsString, path: PathBuf, write: bool) -> Result<Self> {
        let out = if write {
            if path.exists() {
                return Err(format!("File already exists: {:?}", path).into());
            }
            File::options()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&path)?
        } else {
            if !path.exists() {
                return Err(format!("File does not exist: {:?}", path).into());
            }
            File::options().read(true).open(&path)?
        };
        Ok(Self { id, path, out })
    }

    pub fn new_shared(id: OsString, path: PathBuf, write: bool) -> Result<SharedHandle> {
        let handle = Self::new(id, path, write)?;
        Ok(Rc::new(RefCell::new(handle)))
    }

    pub fn rewind(&mut self) -> Result<()> {
        self.out.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn read_item(&mut self, item: &Item) -> Result<String> {
        debug!("Reading? {:?}", self.out);
        self.out.seek(SeekFrom::Start(item.val_pos))?;
        let mut buf = vec![0u8; item.val_sz];
        self.out.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }
}

impl Debug for Handle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Handle {{ id: {:?}, out: {:?} }}", self.id, self.out)
    }
}

impl Read for Handle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.out.read(buf)
    }
}

impl Seek for Handle {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.out.seek(pos)
    }
}

impl Write for Handle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.out.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}
