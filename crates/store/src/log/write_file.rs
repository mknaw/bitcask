use std::error::Error;
use std::fs::File;
use std::io::{Seek, Write};

use crate::Result;

pub trait WriteTarget {
    type Out: Write + Seek + Sync;

    fn get_inner(&mut self) -> &mut Self::Out;

    fn will_fit(&mut self, line: &str, max_log_file_size: u64) -> Result<bool> {
        Ok(line.len() as u64 + self.stream_position()? <= max_log_file_size)
    }

    fn write(&mut self, line: String) -> Result<u64> {
        let out = self.get_inner();
        out.write_all(line.as_bytes())?;
        Ok(self.stream_position()?)
    }

    fn stream_position(&mut self) -> Result<u64> {
        let out = self.get_inner();
        out.stream_position().map_err(|err| {
            let dyn_err: Box<dyn Error> = Box::new(err);
            dyn_err
        })
    }
}

pub struct WriteFile<Out: Write + Seek> {
    out: Out,
}

impl<Out: Write + Seek + Sync> WriteFile<Out> {
    pub fn new(out: Out) -> Self {
        Self { out }
    }
}

impl WriteTarget for WriteFile<File> {
    type Out = File;

    fn get_inner(&mut self) -> &mut Self::Out {
        &mut self.out
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::Result;
    use super::WriteTarget;

    #[derive(Default)]
    struct TestWriteTarget {
        out: Cursor<Vec<u8>>,
    }

    impl WriteTarget for TestWriteTarget {
        type Out = Cursor<Vec<u8>>;

        fn get_inner(&mut self) -> &mut Self::Out {
            &mut self.out
        }
    }

    #[test]
    fn test_will_fit() -> Result<()> {
        let mut write_target = TestWriteTarget::default();
        write_target.write("foo".to_string())?;
        assert!(write_target.will_fit("bar", 10)?);
        assert!(!write_target.will_fit("bar", 5)?);
        Ok(())
    }
}
