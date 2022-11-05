use simple_logger::SimpleLogger;
use tempfile::tempdir;

use store::bitcask::BitCask;
use store::random_string;
use store::{Config, FileLogManager};

/// Wrapper around a test fn that sets up a bitcask instance good for testing.
fn run_test(test: impl FnOnce(&mut BitCask<FileLogManager>)) {
    SimpleLogger::new().init().ok();
    let dir = tempdir().unwrap();
    let cfg = Config {
        log_dir: dir.path(),
        max_log_file_size: 1000,
    };
    let manager = FileLogManager::new(&cfg).unwrap();
    let mut bitcask = BitCask::new(manager);
    test(&mut bitcask);
}

/// Basic happy path test.
#[test]
fn test_happy_path() {
    run_test(|bitcask| {
        let key1 = "foo";
        let val1 = "bar";
        bitcask.set(key1, val1).unwrap();

        let key2 = "baz";
        let val2 = "quux\n\nand\n\nother\n\nstuff\n\ntoo";
        bitcask.set(key2, val2).unwrap();

        assert_eq!(bitcask.get(key1).unwrap(), val1);
        assert_eq!(bitcask.get(key1).unwrap(), val1);

        bitcask.delete(key1).unwrap();
        let error = bitcask.get(key1).err().unwrap();
        assert!(error.is::<store::bitcask::KeyMiss>());
    });
}

/// Test merge functionality.
#[test]
fn test_merge() {
    run_test(|bitcask| {
        let key1 = "foo";
        let mut val = String::new();
        for _ in 0..50 {
            val = random_string(25);
            bitcask.set(key1, &val).unwrap();
        }
        bitcask.merge().unwrap();
        assert_eq!(bitcask.get(key1).unwrap(), val);
    });
}

// TODO test initialization - read existing log files
