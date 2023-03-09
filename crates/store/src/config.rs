use std::path::PathBuf;

use ::config::{Config, ConfigError};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct StoreConfig {
    pub log_dir: PathBuf,
    pub max_log_file_size: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            log_dir: "/tmp/bitcask/".into(),
            max_log_file_size: 2_000_000_000,
        }
    }
}

/// Coalesce env vars with defaults to get a `StoreConfig`.
pub fn get_store_config() -> Result<StoreConfig, ConfigError> {
    let config = Config::builder()
        .add_source(config::Environment::with_prefix("BITCASK").try_parsing(true))
        .set_default("log_dir", "/tmp/bitcask/")?
        .set_default("max_log_file_size", 25_000_000)?
        .build()?;
    // TODO would be good to validate that the provided values make sense.
    config.try_deserialize()
}
