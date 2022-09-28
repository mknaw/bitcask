use std::path::PathBuf;

pub struct Config {
    pub log_dir: PathBuf,
    pub max_log_file_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_dir: "/tmp/bitcask/".into(),
            max_log_file_size: 25_000_000,
        }
    }
}
