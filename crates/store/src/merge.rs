use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Seek, Write};

use crate::config::Config;
use crate::log::LogEntry;
use crate::Result;

// TODO have to iterate over all files in ts-order, then compare against keydir
// and only record non-expired entries. Check again the special handling for no
// keydirs and tombstones.
pub async fn merge<'a>(config: &'a Config<'a>) -> Result<()> {
    // TODO have to exclude open (mutable) files from this exercise.
    let mut paths: Vec<_> = fs::read_dir(config.log_dir)?
        .map(|res| res.unwrap())
        .collect();

    let data = HashMap::<String, LogEntry>::new();
    paths.sort_by_key(|dir| dir.path());
    for _ in &paths {
        todo!();
    }

    // TODO has to be a nicer API to just get the file name sans extension
    let last = paths.last().unwrap().path().to_str().unwrap().to_owned();
    let last_ts = last.split('.').next().unwrap();
    // TODO what happens if this merge file exists already?
    let mut merge_file = File::options()
        .create_new(true)
        .append(true)
        .open(format!("{}.merge", last_ts))?;
    let mut hint_file = File::options()
        .create_new(true)
        .append(true)
        .open(format!("{}.hint", last_ts))?;

    for entry in data.values() {
        merge_file.write_all(entry.serialize_with_crc().as_bytes())?;
        let pos = merge_file.stream_position()? - entry.val_sz() as u64;
        merge_file.write_all(b"\n")?; // TODO do we really need this?
        hint_file.write_all(entry.serialize_hint(pos).as_bytes())?;
    }

    for path in &paths {
        fs::remove_file(path.path())?;
    }

    Ok(())
}
