use std::ffi::OsString;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::str::from_utf8;

use store::bitcask::BitCask;
use store::keydir::{Item, KeyDir};
use store::log::manager::LogManagerT;
use store::Result;

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

    fn initialize_keydir(&mut self) -> KeyDir {
        KeyDir::default()
    }

    fn get(&mut self, item: &Item) -> crate::Result<String> {
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
    let key1 = "foo";
    let val1 = "bar";
    let key2 = "baz";
    let val2 = "quux\n\nand\n\nother\n\nstuff\n\ntoo";
    bitcask.set(key1, val1).unwrap();
    bitcask.set(key2, val2).unwrap();
    assert_eq!(bitcask.get(key1).unwrap(), val1);
    assert_eq!(bitcask.get(key1).unwrap(), val1);
    bitcask.delete(key1).unwrap();
    let error = bitcask.get(key1).err().unwrap();
    assert!(error.is::<store::bitcask::KeyMiss>());
}
