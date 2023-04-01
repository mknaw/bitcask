use std::ffi::OsStr;
use std::sync::Arc;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use store::{BitCask, StoreConfig};
use tempfile::{tempdir, TempDir};

pub fn random_bytes(n: usize) -> Vec<u8> {
    // TODO would be nice to have a seeded singleton
    thread_rng().sample_iter(&Alphanumeric).take(n).collect()
}

fn default_bitcask() -> (BitCask, TempDir) {
    let dir = tempdir().unwrap();
    let cfg = StoreConfig {
        log_dir: dir.path().to_path_buf(),
        max_log_file_size: 1000,
    };
    // Return to ensure tempdir does not go out of scope.
    (BitCask::new(Arc::new(cfg)).unwrap(), dir)
}

/// Basic happy path test.
#[test]
fn test_happy_path() {
    let (bitcask, _tempdir) = default_bitcask();

    let key1 = b"foo";
    let val1 = b"bar";
    bitcask.set(key1, val1).unwrap();

    let key2 = b"baz";
    let val2 = b"quux\n\nand\n\nother\n\nstuff\n\ntoo";
    bitcask.set(key2, val2).unwrap();

    assert_eq!(bitcask.get(key1).unwrap(), val1);
    assert_eq!(bitcask.get(key2).unwrap(), val2);
}

/// Should support arbitrary bytes.
#[test]
fn test_non_ascii() {
    let (bitcask, _tempdir) = default_bitcask();

    let key = "sprawdź".as_bytes();
    let val = "że działa tak, jak powinno";
    bitcask.set(key, val.as_bytes()).unwrap();

    let got = bitcask.get(key).unwrap();
    assert_eq!(got, val.as_bytes());
    assert_eq!(std::str::from_utf8(&got).unwrap(), val);
}

/// Wrapper around a test fn that sets up a bitcask instance good for testing.
fn run_test(cfg: Option<Arc<StoreConfig>>, test: impl FnOnce(&mut BitCask)) {
    let dir = tempdir().unwrap();
    // TODO this whole thing is a bit clunky, oughta be a smoother way
    let default_cfg = StoreConfig {
        log_dir: dir.path().to_path_buf(),
        max_log_file_size: 1000,
    };
    let cfg = cfg.unwrap_or_else(|| Arc::new(default_cfg));
    let mut bitcask = BitCask::new(cfg).unwrap();
    test(&mut bitcask);
}

/// Tests whether preexisting log files read correctly on `bitcask` initialization.
#[test]
fn test_read_existing_on_init() {
    let dir = tempdir().unwrap();
    let cfg = StoreConfig {
        log_dir: dir.path().to_path_buf(),
        max_log_file_size: 1000,
    };
    let cfg = Arc::new(cfg);
    let key = b"foo";
    let val = b"bar";
    run_test(Some(cfg.clone()), |bitcask| {
        bitcask.set(key, val).unwrap();
    });

    // Open new `bitcask` in same directory.
    run_test(Some(cfg), |bitcask| {
        assert_eq!(bitcask.get(key).unwrap(), val);
    });
}

/// Test merge functionality.
#[test]
fn test_merge() {
    let dir = tempdir().unwrap();
    let log_dir = dir.path().to_path_buf();
    let cfg = Arc::new(StoreConfig {
        log_dir: log_dir.clone(),
        max_log_file_size: 1000,
    });

    let vals: Vec<_> = (0..50).map(|_| random_bytes(25)).collect();

    run_test(Some(cfg.clone()), |bitcask| {
        for val in vals.clone() {
            bitcask.set(b"foo", val.as_slice()).unwrap();
        }
        assert!(std::fs::read_dir(&log_dir).unwrap().count() > 2);
    });
    // Open new `bitcask`, since current write file won't be `merge`d.
    run_test(Some(cfg.clone()), |bitcask| {
        bitcask.merge().unwrap();

        assert_eq!(
            &bitcask.get(b"foo").unwrap(),
            vals.last().unwrap().as_slice()
        );
        let (cask_files, hint_files): (Vec<_>, Vec<_>) = std::fs::read_dir(&log_dir)
            .unwrap()
            .flatten()
            .partition(|f| f.path().extension() == Some(OsStr::new("cask")));
        assert!(cask_files.len() <= 2);
        assert!(hint_files.len() >= cask_files.len() - 1);
    });

    // Check hint files read on startup.
    // TODO doesn't actually _explicitly_ check that it read the hint files...
    run_test(Some(cfg.clone()), |bitcask| {
        assert_eq!(
            &bitcask.get(b"foo").unwrap(),
            vals.last().unwrap().as_slice()
        );
    });
}
