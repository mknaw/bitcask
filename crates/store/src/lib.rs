pub use bitcask::BitCask;
pub use merge::MergeResult;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

pub use crate::config::get_store_config;

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

// TODO no great reason why it shouldn't be `pub` in `(crate)` only
// other than not wanting to reimplement it in test suite.
pub fn random_string(n: usize) -> String {
    // TODO would be nice to have a seeded singleton
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}
