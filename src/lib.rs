pub mod bitcask;
pub mod command;
pub mod config;
pub mod keydir;
pub mod log_manager;
pub mod log_reader;
pub mod log_writer;
pub mod merge;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;
