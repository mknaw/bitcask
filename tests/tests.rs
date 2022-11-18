use simple_logger::SimpleLogger;
use tempfile::tempdir;

use store::bitcask::BitCask;
use store::random_string;
use store::{Config, FileLogManager};

/// Wrapper around a test fn that sets up a bitcask instance good for testing.
fn run_test(cfg: Option<&Config>, test: impl FnOnce(&mut BitCask)) {
    SimpleLogger::new().init().ok();
    let dir = tempdir().unwrap();
    // TODO this whole thing is a bit clunky, oughta be a smoother way
    let default_cfg = Config {
        log_dir: dir.path(),
        max_log_file_size: 1000,
    };
    let cfg = cfg.unwrap_or(&default_cfg);
    let manager = FileLogManager::new(cfg).unwrap();
    let mut bitcask = BitCask::new(manager);
    test(&mut bitcask);
}

/// Basic happy path test.
#[test]
fn test_happy_path() {
    run_test(None, |bitcask| {
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
    run_test(None, |bitcask| {
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

/// Tests whether preexisting log files read correctly on `bitcask` initialization.
#[test]
fn test_read_existing_on_init() {
    let dir = tempdir().unwrap();
    let cfg = Config {
        log_dir: dir.path(),
        max_log_file_size: 1000,
    };
    let key = "foo";
    let val = "bar";
    run_test(Some(&cfg), |bitcask| {
        bitcask.set(key, val).unwrap();
    });

    // Open new `bitcask` in same directory.
    run_test(Some(&cfg), |bitcask| {
        assert_eq!(bitcask.get(key).unwrap(), val);
    });
}
