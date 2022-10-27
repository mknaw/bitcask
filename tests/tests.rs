use std::ffi::OsString;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::str::from_utf8;

use bitcask::bitcask::BitCask;
use bitcask::command::{Get, Set};
use bitcask::keydir::{Item, KeyDir};
use bitcask::log_manager::LogManagerT;
use bitcask::Result;

struct TestLogManager {
    log: Cursor<Vec<u8>>,
}

impl Default for TestLogManager {
    fn default() -> Self {
        Self {
            log: Cursor::new(Vec::new()),
        }
    }
}

impl LogManagerT for TestLogManager {
    type Out = Cursor<Vec<u8>>;

    fn get_file_id(&self) -> OsString {
        OsString::from("test")
    }

    fn write(&mut self, line: String) -> Result<()> {
        self.log.write(line.as_bytes())?;
        Ok(())
    }

    fn initialize_keydir(&self) -> KeyDir {
        KeyDir::default()
    }

    fn get(&self, item: &Item) -> crate::Result<String> {
        // TODO the clone here is hardly ideal!
        let mut log = self.log.clone();
        log.seek(SeekFrom::Start(item.val_pos))?;
        let mut buf = vec![0u8; item.val_sz];
        log.read_exact(&mut buf)?;
        Ok(from_utf8(&buf[..])?.to_string())
    }

    fn position(&mut self) -> Result<u64> {
        Ok(self.log.stream_position()?)
    }
}

fn init_test_bitcask() -> BitCask<TestLogManager> {
    BitCask::new(TestLogManager::default())
}

#[test]
fn test_happy_bitcask() {
    let mut bitcask = init_test_bitcask();
    bitcask
        .set(Set {
            key: "foo".to_string(),
            val: "bar".to_string(),
        })
        .unwrap();
    bitcask
        .set(Set {
            key: "baz".to_string(),
            val: "quux".to_string(),
        })
        .unwrap();
    assert_eq!(bitcask.get(Get("foo".to_string())).unwrap(), "bar");
    assert_eq!(bitcask.get(Get("baz".to_string())).unwrap(), "quux");
}
