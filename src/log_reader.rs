use log::info;

use crate::bitcask::Entry;
use crate::lib::Result;

pub fn parse_line(line: String) -> Result<Entry> {

    let parts: Vec<_> = line.split(",").collect();

    // TODO assert that parts.len() > 3?

    let ts = parts[0].parse::<u64>()?;
    let key_sz = parts[1].parse::<usize>()?;
    // TODO probably should have val_sz, and then don't need newlines?

    let key_idx = parts[0].len() + parts[1].len() + parts[2].len() + 3;
    let key = &line[key_idx..key_idx + key_sz];

    let val_idx = key_idx + key_sz + 1;
    let val = &line[val_idx..];
    info!("ts: {}, key: {}, val: {}", ts, key, val);
    Ok(Entry {
        ts,
        key: key.to_owned(),
        val: val.to_owned(),
    })
}
