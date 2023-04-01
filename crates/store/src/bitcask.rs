use std::fmt;
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

    pub fn set(&self, key: &[u8], val: &[u8]) -> crate::Result<()> {
        let entry = LogEntry::from_set(key, val)?;
        let key = entry.key.clone();
        let mut file_manager = self.file_manager.lock().unwrap();
        let item = file_manager.set(&entry)?;
        self.keydir.write().unwrap().set(key, item);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> crate::Result<Vec<u8>> {
        if let Some(item) = self.keydir.read().unwrap().get(key) {
            // TODO if we are having file problems, should we evict from the keydir?
            let mut file_manager = self.file_manager.lock().unwrap();
            let val = file_manager.read_item(item)?;
            if !crate::is_tombstone(&val) {
                return Ok(val);
            }
        }
        Err(KeyMiss.into())
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        self.set(key, crate::TOMBSTONE)
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
