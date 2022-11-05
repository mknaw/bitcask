use std::path::Path;

pub struct Config<'a> {
    pub log_dir: &'a Path,
    pub max_log_file_size: u64,
}

impl<'a> Default for Config<'a> {
    fn default() -> Self {
        Self {
            log_dir: Path::new("/tmp/bitcask/"),
            max_log_file_size: 25_000_000,
        }
    }
}
