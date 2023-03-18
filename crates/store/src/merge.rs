use std::path::PathBuf;
use std::sync::Arc;

use log::info;

use crate::bitcask::SharedKeyDir;
use crate::config::StoreConfig;
use crate::keydir::{Item, KeyDir};
use crate::log::files::{FileHandle, FileManager};
use crate::log::read::{Reader, ReaderItem};

pub struct MergeResult {
    pub keydir: KeyDir,
    pub file_manager: FileManager,
}

/// Actually perform the brunt of the merge.
/// Iterate over candidates for merge and retain the values which match those
/// of the keydir in merge files.
pub fn merge(
    keydir: SharedKeyDir,
    files_to_merge: &Vec<PathBuf>,
    config: Arc<StoreConfig>,
) -> crate::Result<(KeyDir, FileManager)> {
    let mut new_keydir = KeyDir::default();
    let mut file_manager: FileManager = FileManager::new(config);
    let keydir = keydir.read().unwrap();
    for path in files_to_merge {
        let mut handle = FileHandle::new(path.clone(), false)?;
        handle.rewind()?;
        let reader = Reader::new(&mut handle);
        // TODO should at least log something about encountering parse errors along the way.
        for ReaderItem { entry, .. } in reader.flatten() {
            info!("Merging {:?}", entry);
            if let Some(item) = keydir.get(&entry.key) {
                if item.ts == entry.ts {
                    let (path, position) =
                        file_manager.write(entry.serialize_with_crc().as_bytes())?;
                    new_keydir.set(
                        entry.key.clone(),
                        Item {
                            path,
                            val_sz: entry.val_sz() as usize,
                            val_pos: position - entry.val_sz() as u64,
                            ts: entry.ts,
                        },
                    );
                }
            }
        }
    }

    Ok((new_keydir, file_manager))
}
