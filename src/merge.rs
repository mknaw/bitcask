use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, Write};

use log::info;

use crate::config::Config;
use crate::Result;
use crate::log_writer::LogEntry;

pub async fn merge<'a>(config: &'a Config<'a>) -> Result<()> {
    // TODO have to exclude open (mutable) files from this exercise.
    let mut paths: Vec<_> = fs::read_dir(config.log_dir)?
        .map(|res| res.unwrap())
        .collect();

    let mut data = HashMap::new();
    paths.sort_by_key(|dir| dir.path());
    for path in &paths {
        let file = File::open(path.path())?;
        for line in BufReader::new(file).lines() {
            let entry = LogEntry::deserialize(&line?);
            match entry {
                Ok(entry) => {
                    data.insert(entry.key.clone(), entry);
                }
                Err(e) => {
                    info!("{}", e);
                }
            }
        }
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
        merge_file.write_all(entry.serialize().as_bytes())?;
        let pos = merge_file.stream_position()? - entry.val_sz() as u64;
        merge_file.write_all(b"\n")?; // TODO do we really need this?
        hint_file.write_all(entry.serialize_hint(pos).as_bytes())?;
    }

    for path in &paths {
        fs::remove_file(path.path())?;
    }

    Ok(())
}
