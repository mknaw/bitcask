use std::error::Error;
use std::io::{Write, Seek};

pub struct LogWriter<Out: Write + Seek> {
    out: Out,
}

impl<Out: Write + Seek> LogWriter<Out> {
    pub fn new(out: Out) -> Self {
        Self { out }
    }

    pub fn write(&mut self, line: String) -> Result<(), Box<dyn Error>> {
        self.out.write(line.as_bytes())?;
        // TODO should return a position in the file for value?
        Ok(())
    }

    pub fn stream_position(&mut self) -> Result<u64, Box<dyn Error>> {
        self.out.stream_position().map_err(|err| {
            let dyn_err: Box<dyn Error> = Box::new(err);
            dyn_err
        })
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
        if writer.write("foo".to_string()).is_ok() {
            let out = writer.out.into_inner();
            let line = str::from_utf8(&out).unwrap();
            assert!(line.ends_with("foo"));
        } else {
            assert!(false);
        }
    }
}
