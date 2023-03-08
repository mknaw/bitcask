use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{debug, info};
use tokio::sync::{mpsc, oneshot};

use crate::config::StoreConfig;
use crate::keydir::KeyDir;
use crate::log::handle::HandleMap;
use crate::log::write::Writer;
use crate::log::LogEntry;
use crate::merge::{MergeJob, MergeResult};

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

#[derive(Debug)]
pub enum Command {
    Set((String, String)),
    Get(String),
    Delete(String),
    Merge,
}

pub type BitCaskTx = mpsc::Sender<(Command, oneshot::Sender<Option<String>>)>;

type SharedKeyDir = Arc<RwLock<KeyDir>>;

type SharedHandleMap = Arc<Mutex<HandleMap>>;

type SharedWriter = Arc<Mutex<Writer>>;

pub struct BitCask {
    pub config: Arc<StoreConfig>,
    keydir: SharedKeyDir,
    handles: SharedHandleMap,
    writer: SharedWriter,
    is_merging: Arc<Mutex<bool>>, // TODO maybe use a single-permit `Semaphore`
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
            is_merging: Arc::new(Mutex::new(false)),
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

    pub fn listen(bitcask: Self) -> BitCaskTx {
        let (tx, mut rx) = mpsc::channel::<(Command, oneshot::Sender<Option<String>>)>(32);
        let bitcask = Arc::new(Mutex::new(bitcask));

        let tx = tx.clone();
        tokio::spawn(async move {
            while let Some((cmd, resp_tx)) = rx.recv().await {
                debug!("received command: {:?}", cmd);
                match cmd {
                    Command::Set((key, val)) => {
                        debug!("Set {} to {}", key, val);
                        let bitcask = bitcask.clone();
                        tokio::spawn(async move {
                            bitcask.lock().unwrap().set(&key, &val).unwrap();
                            resp_tx.send(None).unwrap();
                        });
                    }
                    Command::Get(key) => {
                        let bitcask = bitcask.clone();
                        tokio::spawn(async move {
                            let val = bitcask.lock().unwrap().get(&key).unwrap();
                            resp_tx.send(Some(val)).unwrap();
                        });
                    }
                    Command::Delete(key) => {
                        bitcask.lock().unwrap().delete(&key).unwrap();
                        resp_tx.send(None).unwrap();
                    }
                    Command::Merge => {
                        match {
                            let bitcask = bitcask.lock().unwrap();
                            let merge_job = bitcask.get_merge_job();
                            *bitcask.is_merging.lock().unwrap() = true;
                            merge_job
                        } {
                            Ok(mut merge_job) => {
                                // Do the actual work in a spawned task; don't want to keep the mutex!
                                let bitcask = bitcask.clone();
                                tokio::spawn(async move {
                                    let result = merge_job.merge().unwrap();
                                    resp_tx.send(Some("merge complete".to_string())).unwrap();
                                    bitcask.lock().unwrap().finalize_merge(result);
                                });
                            }
                            Err(e) => resp_tx.send(Some(e.to_string())).unwrap(),
                        };
                    }
                };
            }
        });

        tx
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
        debug!("Delete {}", key);
        self.set(key, crate::TOMBSTONE)
    }

    fn get_merge_job(&self) -> Result<MergeJob, MergeUnderway> {
        if *self.is_merging.lock().unwrap() {
            return Err(MergeUnderway);
        }
        // TODO would like to not have to `clone`, if possible
        let keydir = self.keydir.read().unwrap().clone();
        let mut handles = self.handles.lock().unwrap().try_clone().unwrap();
        let writer = self.writer.lock().unwrap();
        if let Some(current_write_file_id) = writer.current_file_id() {
            handles.remove(current_write_file_id);
        }
        Ok(MergeJob {
            keydir,
            // TODO should there be a `handles.get_closed_only`?
            handles,
            config: self.config.clone(),
        })
    }

    fn finalize_merge(&mut self, merge_result: MergeResult) {
        info!("finalizing merge");
        let mut keydir = self.keydir.write().unwrap();
        for (key, item) in merge_result.keydir.data {
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
        for (_, handle) in merge_result.handle_map.inner {
            handles.insert(handle);
        }
        let mut is_merging = self.is_merging.lock().unwrap();
        // TODO can't wait til here to set `is_merging = false`
        // earlier unwrap error for example would screw it up.
        *is_merging = false;
        // TODO hint file
    }
}
