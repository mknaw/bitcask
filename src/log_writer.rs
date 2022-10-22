use std::error::Error;
use std::fs::File;
use std::io::{Write, Seek};

pub trait LogWriterT {
    type Out: Write + Sync;

    fn write(&mut self, line: String) -> crate::Result<()>;
    fn stream_position(&mut self) -> crate::Result<u64>;
}

pub struct LogWriter<Out: Write + Seek> {
    out: Out,
}

impl<Out: Write + Seek> LogWriter<Out> {
    pub fn new(out: Out) -> Self {
        Self { out }
    }
}

// TODO this is basically just Write + Seek.
impl LogWriterT for LogWriter<File> {
    type Out = File;

    // TODO can we get generic implementation from a ... trait?
    fn write(&mut self, line: String) -> crate::Result<()> {
        self.out.write(line.as_bytes())?;
        self.out.write("\n".as_bytes())?;
        Ok(())
    }

    fn stream_position(&mut self) -> crate::Result<u64> {
        self.out.stream_position().map_err(|err| {
            let dyn_err: Box<dyn Error> = Box::new(err);
            dyn_err
        })
    }
}

impl LogWriter<File> {
    // TODO probably should be something for the manager to check?
    // So we don't have to pass around config stuff (max file size).
    pub fn is_full(&self) -> crate::Result<bool> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::str;

    use super::LogWriter;

    #[test]
    fn test_happy_set() {
        let mut buf = Vec::new();
        let cur = Cursor::new(&mut buf);
        let mut writer = LogWriter::new(cur);
        // TODO have to implement test log_writer?
        if writer.write("foo".to_string()).is_ok() {
            let out = writer.out.into_inner();
            let line = str::from_utf8(&out).unwrap();
            assert!(line.ends_with("foo"));
        } else {
            assert!(false);
        }
    }
}
