use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use log::info;

use crate::config::StoreConfig;
use crate::keydir::KeyDir;
use crate::log::handle::HandleMap;
use crate::log::write::Writer;
use crate::log::LogEntry;
use crate::merge::merge;

// TODO should this one be a &str?
// TODO reexport under `store::errors::...`?
// TODO should probably be in the KeyDir file.
#[derive(Debug)]
pub struct KeyMiss;

impl fmt::Display for KeyMiss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key miss")
    }
}

impl std::error::Error for KeyMiss {}

#[derive(Debug)]
pub struct MergeUnderway;

impl fmt::Display for MergeUnderway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Merge already underway!")
    }
}

impl std::error::Error for MergeUnderway {}

pub type SharedKeyDir = Arc<RwLock<KeyDir>>;

type SharedHandleMap = Arc<Mutex<HandleMap>>;

type SharedWriter = Arc<Mutex<Writer>>;

pub struct BitCask {
    pub config: Arc<StoreConfig>,
    keydir: SharedKeyDir,
    handles: SharedHandleMap,
    writer: SharedWriter,
    merge_mutex: Arc<Mutex<()>>, // TODO maybe use a single-permit `Semaphore`
}

impl BitCask {
    pub fn new(config: Arc<StoreConfig>) -> crate::Result<Self> {
        // Create the log directory if it doesn't exist.
        if !config.log_dir.exists() {
            std::fs::create_dir_all(&config.log_dir)?;
        }

        let mut handles = HandleMap::new(&config)?;
        let keydir = Self::initialize_keydir(&mut handles);
        let writer = Writer::new(
            config.clone(),
            Arc::new(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|d| format!("{}.cask", d.as_micros()))
                    .unwrap()
            }),
        );

        Ok(Self {
            config,
            keydir: Arc::new(RwLock::new(keydir)),
            handles: Arc::new(Mutex::new(handles)),
            writer: Arc::new(Mutex::new(writer)),
            merge_mutex: Arc::new(Mutex::new(())),
        })
    }

    pub fn initialize_keydir(handles: &mut HandleMap) -> KeyDir {
        handles
            .read_all_items()
            .fold(KeyDir::default(), |mut keydir, reader_item| {
                keydir.set(reader_item.entry.key.clone(), reader_item.to_keydir_item());
                keydir
            })
    }

    pub fn set(&self, key: &str, val: &str) -> crate::Result<()> {
        let entry = LogEntry::from_set(key, val)?;
        let key = entry.key.clone();
        // TODO still have to add to `handles` somewhere!
        let (item, new_handle) = self.writer.lock().unwrap().set(&entry)?;
        self.keydir.write().unwrap().set(key, item);
        if let Some(new_handle) = new_handle {
            self.handles.lock().unwrap().insert(new_handle);
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> crate::Result<String> {
        if let Some(item) = self.keydir.read().unwrap().get(key) {
            // TODO if we are having file problems, should we evict from the keydir?
            let mut handles = self.handles.lock().unwrap();
            let handle = handles.get(&item.file_id)?;
            let value = handle.read_item(item)?;
            if !crate::is_tombstone(&value) {
                return Ok(value);
            }
        }
        Err(KeyMiss.into())
    }

    pub fn delete(&self, key: &str) -> crate::Result<()> {
        self.set(key, crate::TOMBSTONE)
    }

    pub fn merge(&self) -> crate::Result<()> {
        // Take a mutex to hold throughout this scope.
        let _merge_mutex = self.merge_mutex.try_lock().map_err(|_| MergeUnderway)?;
        let handles = {
            let mut handles = self.handles.lock().unwrap().try_clone().unwrap();
            let writer = self.writer.lock().unwrap();
            if let Some(current_write_file_id) = writer.current_file_id() {
                handles.remove(current_write_file_id);
            };
            handles
        };
        let (new_keydir, new_handles) =
            merge(self.keydir.clone(), handles, self.config.clone()).unwrap();
        self.finalize_merge(new_keydir, new_handles);

        Ok(())
    }

    pub fn finalize_merge(&self, new_keydir: KeyDir, new_handles: HandleMap) {
        let mut keydir = self.keydir.write().unwrap();
        for (key, item) in new_keydir.data {
            keydir.set(key, item);
        }

        // TODO have to remove this unseemly bit.
        let writer = self.writer.lock().unwrap();
        let mut handles = self.handles.lock().unwrap();
        let current_write_file_id = writer.current_file_id();
        for handle in handles
            .inner
            .values()
            .filter(|h| Some(&h.id) != current_write_file_id)
        {
            info!("Removing {:?}", handle.path);
            std::fs::remove_file(&handle.path).unwrap();
        }
        // TODO this really isnt a sufficient condition!
        // We could have added files since the merge job was queued, so we need to get
        // from the merge job a list of files that were merged and only delete those.
        handles
            .inner
            .retain(|id, _| Some(id) == current_write_file_id);
        for (_, handle) in new_handles.inner {
            handles.insert(handle);
        }
        // earlier unwrap error for example would screw it up.
        // TODO hint file
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::{tempdir, TempDir};

    use super::{BitCask, StoreConfig};
    use crate::random_string;

    fn default_bitcask() -> (BitCask, TempDir) {
        let dir = tempdir().unwrap();
        let cfg = StoreConfig {
            log_dir: dir.path().to_path_buf(),
            max_log_file_size: 1000,
        };
        // Return to ensure tempdir does not go out of scope.
        (BitCask::new(Arc::new(cfg)).unwrap(), dir)
    }

    /// Basic happy path test.
    #[test]
    fn test_happy_path() {
        let (bitcask, _tempdir) = default_bitcask();

        let key1 = "foo";
        let val1 = "bar";
        bitcask.set(key1, val1).unwrap();

        let key2 = "baz";
        let val2 = "quux\n\nand\n\nother\n\nstuff\n\ntoo";
        bitcask.set(key2, val2).unwrap();

        assert_eq!(bitcask.get(key1).unwrap(), val1);
        assert_eq!(bitcask.get(key2).unwrap(), val2);
    }

    /// Test merge functionality.
    #[test]
    fn test_merge() {
        let (bitcask, tempdir) = default_bitcask();
        let config = bitcask.config.clone();
        let key1 = "foo";
        let mut val = String::new();
        for _ in 0..50 {
            val = random_string(25);
            bitcask.set(key1, &val).unwrap();
        }
        assert!(std::fs::read_dir(tempdir.path()).unwrap().count() > 2);

        bitcask.merge().unwrap();
        assert_eq!(bitcask.get(key1).unwrap(), val);
        assert!(std::fs::read_dir(config.log_dir.clone()).unwrap().count() <= 2);
    }

    /// Wrapper around a test fn that sets up a bitcask instance good for testing.
    fn run_test(cfg: Option<Arc<StoreConfig>>, test: impl FnOnce(&mut BitCask)) {
        let dir = tempdir().unwrap();
        // TODO this whole thing is a bit clunky, oughta be a smoother way
        let default_cfg = StoreConfig {
            log_dir: dir.path().to_path_buf(),
            max_log_file_size: 1000,
        };
        let cfg = cfg.unwrap_or_else(|| Arc::new(default_cfg));
        let mut bitcask = BitCask::new(cfg).unwrap();
        test(&mut bitcask);
    }

    /// Tests whether preexisting log files read correctly on `bitcask` initialization.
    #[test]
    fn test_read_existing_on_init() {
        let dir = tempdir().unwrap();
        let cfg = StoreConfig {
            log_dir: dir.path().to_path_buf(),
            max_log_file_size: 1000,
        };
        let cfg = Arc::new(cfg);
        let key = "foo";
        let val = "bar";
        run_test(Some(cfg.clone()), |bitcask| {
            bitcask.set(key, val).unwrap();
        });

        // Open new `bitcask` in same directory.
        run_test(Some(cfg), |bitcask| {
            assert_eq!(bitcask.get(key).unwrap(), val);
        });
    }
}
