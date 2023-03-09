use std::sync::Arc;

use log::info;

use crate::bitcask::SharedKeyDir;
use crate::config::StoreConfig;
use crate::keydir::{Item, KeyDir};
use crate::log::handle::HandleMap;
use crate::log::read::{Reader, ReaderItem};
use crate::log::write::{WriteResult, Writer};

#[derive(Debug)]
pub struct MergeResult {
    pub keydir: KeyDir,
    pub handle_map: HandleMap,
}

pub fn merge(
    keydir: SharedKeyDir,
    mut handles: HandleMap,
    config: Arc<StoreConfig>,
) -> crate::Result<(KeyDir, HandleMap)> {
    // TODO move this to `HandleMap`.
    // TODO blows up when "merge" before any sets.. should fix
    let last = handles
        .inner
        .iter()
        .next_back()
        .unwrap()
        .0
        .to_str()
        .unwrap()
        .to_string();
    let mut merge_writer = Writer::new(
        config.clone(),
        Arc::new(move |k| format!("{}.merge.{}", last, k)),
    );

    let mut new_keydir = KeyDir::default();
    let mut handle_map: HandleMap = Default::default();
    let keydir = keydir.read().unwrap();
    // TODO dont use inner
    for handle in handles.inner.values_mut() {
        // let handle = &mut handle.write().unwrap();
        handle.rewind()?;
        // TODO should we just be able to iterate over items from the `handle`?
        let reader = Reader::new(handle);
        // TODO should at least log something about encountering parse errors along the way.
        for ReaderItem { entry, .. } in reader.flatten() {
            info!("Merging {:?}", entry);
            if let Some(item) = keydir.get(&entry.key) {
                if item.ts == entry.ts {
                    // TODO unfortunate copypasta here
                    let WriteResult {
                        file_id,
                        position,
                        new_handle,
                    } = merge_writer.write(entry.serialize_with_crc().as_bytes())?;
                    // debug!("Wrote with result {:?}", write_result);
                    new_keydir.set(
                        entry.key.clone(),
                        Item {
                            file_id,
                            val_sz: entry.val_sz() as usize,
                            val_pos: position - entry.val_sz() as u64,
                            ts: entry.ts,
                        },
                    );
                    if let Some(new_handle) = new_handle {
                        handle_map.insert(new_handle);
                    }
                }
            }
        }
    }

    Ok((new_keydir, handle_map))
}
