pub use self::log::manager::FileLogManager;
pub use bitcask::BitCask;
pub use config::Config;

pub mod bitcask;
pub mod config;
pub mod keydir;
pub mod log;
pub mod merge;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) const TOMBSTONE: &str = "☗";

pub(crate) fn is_tombstone(s: &str) -> bool {
    s == TOMBSTONE
}
