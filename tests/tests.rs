use simple_logger::SimpleLogger;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::sync::oneshot;

use store::bitcask::{BitCask, Command};
use store::config::StoreConfig;
use store::random_string;

/// Wrapper around a test fn that sets up a bitcask instance good for testing.
fn run_test(cfg: Option<Arc<StoreConfig>>, test: impl FnOnce(&mut BitCask)) {
    SimpleLogger::new().init().ok();
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
#[tokio::test]
async fn test_happy_path() {
    let (bitcask, _tempdir) = default_bitcask();
    let tx = BitCask::listen(bitcask);

    let key1 = "foo".to_string();
    let val1 = "bar".to_string();
    let (server_tx, rx) = oneshot::channel();
    tx.send((Command::Set((key1.clone(), val1.clone())), server_tx))
        .await
        .unwrap();
    rx.await.unwrap();

    let key2 = "baz".to_string();
    let val2 = "quux\n\nand\n\nother\n\nstuff\n\ntoo".to_string();
    let (server_tx, rx) = oneshot::channel();
    tx.send((Command::Set((key2.clone(), val2.clone())), server_tx))
        .await
        .unwrap();
    rx.await.unwrap();

    let (server_tx, rx1) = oneshot::channel();
    tx.send((Command::Get(key1.clone()), server_tx))
        .await
        .unwrap();
    let (server_tx, rx2) = oneshot::channel();
    tx.send((Command::Get(key2.clone()), server_tx))
        .await
        .unwrap();

    assert_eq!(rx1.await.unwrap(), Some(val1));
    assert_eq!(rx2.await.unwrap().unwrap(), val2);
}

/// Test merge functionality.
#[tokio::test]
async fn test_merge() {
    let (bitcask, tempdir) = default_bitcask();
    let config = bitcask.config.clone();
    let tx = BitCask::listen(bitcask);
    let key1 = "foo".to_string();
    let mut val = String::new();
    for _ in 0..50 {
        val = random_string(25);
        let (server_tx, rx) = oneshot::channel();
        tx.send((Command::Set((key1.clone(), val.clone())), server_tx))
            .await
            .unwrap();
        rx.await.unwrap();
    }
    assert!(std::fs::read_dir(tempdir.path()).unwrap().count() > 2);

    let (server_tx, rx) = oneshot::channel();
    tx.send((Command::Merge, server_tx)).await.unwrap();
    rx.await.unwrap();
    let (server_tx, rx) = oneshot::channel();
    tx.send((Command::Get(key1), server_tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), Some(val));
    assert!(std::fs::read_dir(config.log_dir.clone()).unwrap().count() <= 2);
}

/// Ensure that a merge doesn't block a get.
/// TODO would have to do some mocking to ensure merge takes non 0 time?
// #[tokio::test]
// async fn test_merge_nonblocking() {
//     let (bitcask, _) = default_bitcask();
//     let tx = BitCask::listen(bitcask);
//     let (server_tx1, rx_merge) = oneshot::channel();
//     tx.send((Command::Merge, server_tx1)).await.unwrap();
//     let (server_tx2, rx_get) = oneshot::channel();
//     tx.send((Command::Get("foo".to_string()), server_tx2))
//         .await
//         .unwrap();
//
//     let res = tokio::select! {
//         _ = rx_get => Ok(()),
//         _ = rx_merge => Err(()),
//     };
//     assert!(res.is_ok());
// }

/// Tests whether preexisting log files read correctly on `bitcask` initialization.
#[test]
fn test_read_existing_on_init() {
    let dir = tempdir().unwrap();
    let cfg = StoreConfig {
        log_dir: dir.path().to_path_buf(),
        max_log_file_size: 1000,
    };
    let cfg = Arc::new(cfg);
    let key = "foo";
    let val = "bar";
    run_test(Some(cfg.clone()), |bitcask| {
        bitcask.set(key, val).unwrap();
    });

    // Open new `bitcask` in same directory.
    run_test(Some(cfg), |bitcask| {
        assert_eq!(bitcask.get(key).unwrap(), val);
    });
}
