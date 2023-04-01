use std::path::PathBuf;
use std::sync::Arc;

use log::info;

use crate::bitcask::SharedKeyDir;
use crate::config::StoreConfig;
use crate::keydir::{Item, KeyDir};
use crate::log::files::{FileHandle, FileManager};
use crate::log::read::LogReaderItem;

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
    let mut val_pos = 0;
    for path in files_to_merge {
        let handle = FileHandle::new(path.clone(), false)?;
        // TODO should at least log something about encountering parse errors along the way.
        for LogReaderItem { entry, .. } in handle.flatten() {
            if let Some(item) = keydir.get(&entry.key) {
                if item.ts == entry.ts {
                    info!("Merging {:?}", entry);
                    let (path, next_val_pos) =
                        file_manager.write(entry.serialize_with_crc().as_slice())?;
                    let item = Item {
                        path,
                        val_sz: entry.val_sz() as usize,
                        val_pos,
                        ts: entry.ts,
                    };
                    val_pos = next_val_pos;
                    file_manager.write_hint(item.serialize_as_hint(&entry.key).as_slice())?;
                    // TODO these writes should definitely be from a `BufWriter`...
                    new_keydir.set(entry.key.clone(), item);
                }
            }
        }
    }

    Ok((new_keydir, file_manager))
}
