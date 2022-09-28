use crate::log::LogEntry;
use crate::merge::MergeJob;
use crate::Config;
use log::{debug, info};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

use crate::keydir::KeyDir;
use crate::log::handle::HandleMap;
use crate::log::write::Writer;
use crate::merge::MergeResult;

// TODO should this one be a &str?
// TODO reexport under `store::errors::...`?
// TODO should probably be in the KeyDir file.
#[derive(Debug)]
pub struct KeyMiss(String);

impl std::error::Error for KeyMiss {
    fn description(&self) -> &str {
        "key miss"
    }
}

impl fmt::Display for KeyMiss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeyMiss: {}", self.0)
    }
}

#[derive(Debug)]
pub enum Command {
    Set((String, String)),
    Get(String),
    Delete(String),
    Merge,
}

#[derive(Debug)]
pub enum Message {
    Command((Command, oneshot::Sender<Option<String>>)),
    MergeEnd(Box<MergeResult>),
}

pub type BitCaskTx = mpsc::Sender<Message>;

type SharedKeyDir = Arc<RwLock<KeyDir>>;

type SharedHandleMap = Arc<Mutex<HandleMap>>;

type SharedWriter = Arc<Mutex<Writer>>;

pub struct BitCask {
    config: Arc<Config>,
    keydir: SharedKeyDir,
    handles: SharedHandleMap,
    writer: SharedWriter,
    is_merging: Arc<Mutex<bool>>, // TODO is a semaphore overkill here?
}

impl BitCask {
    pub fn new(config: Arc<Config>) -> crate::Result<Self> {
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
        let (tx, mut rx) = mpsc::channel(32);
        let ret_tx = tx.clone();
        let keydir = bitcask.keydir.clone();
        let handles = bitcask.handles.clone();
        let bitcask = Arc::new(Mutex::new(bitcask));

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                debug!("received message: {:?}", msg);
                match msg {
                    Message::Command((cmd, resp_tx)) => match cmd {
                        Command::Set((key, val)) => {
                            let bitcask = bitcask.clone();
                            tokio::spawn(async move {
                                bitcask.lock().unwrap().set(&key, &val).unwrap();
                                resp_tx.send(None).unwrap();
                            });
                        }
                        Command::Get(key) => {
                            let keydir = keydir.clone();
                            let handles = handles.clone();
                            tokio::spawn(async move {
                                // TODO maybe there's a better way than making this distinction.
                                // Maybe put the whole BitCask in an RwLock?
                                let val = Self::get_concurrent(keydir, &key, handles).unwrap();
                                debug!("Get {}", val);
                                resp_tx.send(Some(val)).unwrap();
                            });
                        }
                        Command::Delete(key) => {
                            bitcask.lock().unwrap().delete(&key).unwrap();
                            resp_tx.send(None).unwrap();
                        }
                        Command::Merge => {
                            let bitcask = bitcask.lock().unwrap();
                            let mut is_merging = bitcask.is_merging.lock().unwrap();
                            if *is_merging {
                                resp_tx
                                    .send(Some("already have merge task underway!".to_string()))
                                    .unwrap();
                            } else {
                                let keydir = keydir.read().unwrap().clone();
                                let mut handles = handles.lock().unwrap().try_clone().unwrap();
                                let writer = bitcask.writer.lock().unwrap();
                                if let Some(current_write_file_id) = writer.current_file_id() {
                                    handles.remove(current_write_file_id);
                                }
                                // TODO wonder if there should be something like a `get_closed_only`
                                let mut merge_job = MergeJob { keydir, handles };
                                let config = bitcask.config.clone();
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    // TODO send failure as an actual `Result<...>` so MergeEnd
                                    // branch can "clean up" (like set `is_merging` to false).
                                    let result = merge_job.merge(config).unwrap();
                                    tx.send(Message::MergeEnd(Box::new(result))).await.unwrap();
                                });
                                resp_tx.send(None).unwrap();
                                *is_merging = true;
                            }
                        }
                    },
                    Message::MergeEnd(merge_result) => {
                        info!("finalizing merge");
                        // self.merging = false;
                        let mut keydir = keydir.write().unwrap();
                        for (key, item) in merge_result.keydir.data {
                            keydir.set(key, item);
                        }

                        // TODO have to remove this unseemly bit.
                        let bitcask = bitcask.lock().unwrap();
                        let writer = bitcask.writer.lock().unwrap();
                        let mut handles = handles.lock().unwrap();
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
                        let mut is_merging = bitcask.is_merging.lock().unwrap();
                        // TODO can't wait til here to set `is_merging = false`
                        // earlier unwrap error for example would screw it up.
                        *is_merging = false;
                        // TODO hint file
                    }
                };
            }
        });

        ret_tx
    }

    pub fn set(&self, key: &str, val: &str) -> crate::Result<()> {
        debug!("Set {} to {}", key, val);
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

    pub fn delete(&self, key: &str) -> crate::Result<()> {
        debug!("Delete {}", key);
        self.set(key, crate::TOMBSTONE)
    }

    fn get_concurrent(
        keydir: SharedKeyDir,
        key: &str,
        handles: SharedHandleMap,
    ) -> crate::Result<String> {
        if let Some(item) = keydir.read().unwrap().get(key) {
            // TODO if we are having file problems, should we evict from the keydir?
            let mut handles = handles.lock().unwrap();
            let handle = handles.get(&item.file_id)?;
            let value = handle.read_item(item)?;
            if !crate::is_tombstone(&value) {
                return Ok(value);
            }
        }
        Err(KeyMiss(key.to_string()).into())
    }

    pub fn get(&self, key: &str) -> crate::Result<String> {
        Self::get_concurrent(self.keydir.clone(), key, self.handles.clone())
    }
}
