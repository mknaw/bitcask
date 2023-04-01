pub use bitcask::BitCask;
pub use merge::MergeResult;

pub use crate::config::{get_store_config, StoreConfig};

pub mod bitcask;
pub mod config;
pub mod keydir;
pub mod log;
pub mod merge;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

// TODO I guess I could pick a smaller tombstone (1 byte)
pub(crate) const TOMBSTONE: &[u8; 3] = b"\xE2\x98\x97";

pub(crate) fn is_tombstone(bytes: &[u8]) -> bool {
    bytes == TOMBSTONE
}
