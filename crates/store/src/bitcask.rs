use std::fmt;
use std::str::from_utf8;
use std::sync::{Arc, Mutex, RwLock};

use log::info;

use crate::config::StoreConfig;
use crate::keydir::KeyDir;
use crate::log::files::FileManager;
use crate::log::read::HintReader;
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

pub struct BitCask {
    pub config: Arc<StoreConfig>,
    keydir: SharedKeyDir,
    file_manager: Arc<Mutex<FileManager>>,
    merge_mutex: Arc<Mutex<()>>,
}

impl BitCask {
    pub fn new(config: Arc<StoreConfig>) -> crate::Result<Self> {
        info!("Initializing BitCask in {:?}", config.log_dir);
        // Create the log directory if it doesn't exist.
        if !config.log_dir.exists() {
            info!("Directory not found! Creating...");
            std::fs::create_dir_all(&config.log_dir)?;
        }

        let mut file_manager = FileManager::new(config.clone());
        file_manager.initialize_from_log_dir()?;
        let keydir = Self::initialize_keydir(&mut file_manager);

        Ok(Self {
            config,
            keydir: Arc::new(RwLock::new(keydir)),
            file_manager: Arc::new(Mutex::new(file_manager)),
            merge_mutex: Arc::new(Mutex::new(())),
        })
    }

    /// Construct `KeyDir` reflecting existing data in log- and hintfiles in directory.
    pub fn initialize_keydir(file_manager: &mut FileManager) -> KeyDir {
        file_manager
            .iter_mut()
            .flat_map(|handle| {
                match handle.get_hint_file(false).as_mut() {
                    Some(hint_file) => {
                        let hint_reader = HintReader::new(hint_file);
                        hint_reader.flatten().collect::<Vec<_>>()
                    }
                    None => {
                        // TODO probably should log errors instead of just flattening!
                        handle
                            .flatten()
                            .map(|ri| ri.into_key_item_tuple())
                            .collect::<Vec<_>>()
                    }
                }
            })
            .fold(KeyDir::default(), |mut keydir, (key, item)| {
                keydir.set(key, item);
                keydir
            })
    }

    pub fn set(&self, key: &str, val: &str) -> crate::Result<()> {
        let entry = LogEntry::from_set(key, val)?;
        let key = entry.key.clone();
        let mut file_manager = self.file_manager.lock().unwrap();
        let item = file_manager.set(&entry)?;
        self.keydir.write().unwrap().set(key, item);
        Ok(())
    }

    pub fn get(&self, key: &str) -> crate::Result<String> {
        if let Some(item) = self.keydir.read().unwrap().get(key) {
            // TODO if we are having file problems, should we evict from the keydir?
            let mut file_manager = self.file_manager.lock().unwrap();
            let val = file_manager.read_item(item)?;
            if !crate::is_tombstone(val.as_bytes()) {
                return Ok(val);
            }
        }
        Err(KeyMiss.into())
    }

    pub fn delete(&self, key: &str) -> crate::Result<()> {
        self.set(key, from_utf8(crate::TOMBSTONE)?)
    }

    pub fn merge(&self) -> crate::Result<()> {
        // Take mutex to hold throughout this function's scope.
        let _merge_mutex = self.merge_mutex.try_lock().map_err(|_| MergeUnderway)?;
        let files_to_merge: Vec<_> = {
            let file_manager = self.file_manager.lock().unwrap();
            file_manager.iter_closed().map(|f| f.path.clone()).collect()
        };
        let (merge_keydir, merge_file_manager) =
            merge(self.keydir.clone(), &files_to_merge, self.config.clone()).unwrap();

        let mut keydir = self.keydir.write().unwrap();
        for (key, item) in merge_keydir.data {
            keydir.set(key, item);
        }

        let mut file_manager = self.file_manager.lock().unwrap();
        for path in files_to_merge {
            if let Some(handle) = file_manager.remove(&path) {
                std::fs::remove_file(&handle.path).unwrap();
                let mut hint_path = handle.path.clone();
                hint_path.set_extension("hint");
                if hint_path.exists() {
                    std::fs::remove_file(&hint_path).unwrap();
                }
            }
        }

        for (_, handle) in merge_file_manager.inner {
            file_manager.insert(handle);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
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

    /// Test merge functionality.
    #[test]
    fn test_merge() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().to_path_buf();
        let cfg = Arc::new(StoreConfig {
            log_dir: log_dir.clone(),
            max_log_file_size: 1000,
        });

        let vals: Vec<_> = (0..50).map(|_| random_string(25)).collect();

        run_test(Some(cfg.clone()), |bitcask| {
            for val in vals.clone() {
                bitcask.set("foo", &val).unwrap();
            }
            assert!(std::fs::read_dir(&log_dir).unwrap().count() > 2);
        });
        // Open new `bitcask`, since current write file won't be `merge`d.
        run_test(Some(cfg.clone()), |bitcask| {
            bitcask.merge().unwrap();

            assert_eq!(&bitcask.get("foo").unwrap(), vals.last().unwrap());
            let (cask_files, hint_files): (Vec<_>, Vec<_>) = std::fs::read_dir(&log_dir)
                .unwrap()
                .flatten()
                .partition(|f| f.path().extension() == Some(OsStr::new("cask")));
            assert!(cask_files.len() <= 2);
            assert!(hint_files.len() >= cask_files.len() - 1);
        });

        // Check hint files read on startup.
        // TODO doesn't actually _explicitly_ check that it read the hint files...
        run_test(Some(cfg.clone()), |bitcask| {
            assert_eq!(&bitcask.get("foo").unwrap(), vals.last().unwrap());
        });
    }
}
