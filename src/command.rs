use std::fmt;
use std::io::{Cursor, BufRead};
use std::vec::IntoIter;

#[derive(Debug)]
pub struct ParseError;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ParseError occurred")
    }
}

impl ::std::error::Error for ParseError {
    fn description(&self) -> &str {
        "no error"
    }
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Get,
    Set,
    Simple(String),
}

pub enum Command {
    Get(Get),
    Set(Set),
}

#[derive(Debug, PartialEq)]
pub struct Get(pub String);


#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub val: String,
}


fn make_get(tokens: &mut IntoIter<Token>) -> crate::Result<Command> {
    // Extract a `Command::Get` from `tokens`.
    if let Some(Token::Simple(s)) = tokens.next() {
        if tokens.next().is_none() {
            return Ok(Command::Get(Get(s)));
        }
    }
    Err(Box::new(ParseError {}))
}

fn make_set(tokens: &mut IntoIter<Token>) -> crate::Result<Command> {
    if let Some(Token::Simple(key)) = tokens.next() {
        if let Some(Token::Simple(val)) = tokens.next() {
            if tokens.next().is_none() {
                return Ok(Command::Set(Set { key, val }));
            }
        }
    }

    Err(Box::new(ParseError {}))
}

pub fn parse(cur: &mut Cursor<&[u8]>) -> crate::Result<Command> {
    let mut tokens = parse_tokens(cur).into_iter();

    match tokens.next() {
        Some(Token::Get) => make_get(&mut tokens),
        Some(Token::Set) => make_set(&mut tokens),
        _ => Err(Box::new(ParseError {})),
    }
}

fn parse_token(bytes: Vec<u8>) -> crate::Result<Token> {
    if bytes == b"get".to_vec() {
        Ok(Token::Get)
    } else if bytes == b"set".to_vec() {
        Ok(Token::Set)
    } else {
        let simple = String::from_utf8(bytes)?.trim().to_string();
        Ok(Token::Simple(simple))
    }
}

pub fn parse_tokens(cur: &mut Cursor<&[u8]>) -> Vec<Token> {
    cur.position();
    let cur_iter = cur.split(b' ');
    let mut tokens = vec![];
    for res in cur_iter {
        if let Ok(bytes) = res {
            if let Ok(token) = parse_token(bytes) {
                tokens.push(token);
            }
        }
    }

    tokens
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::{Command, Get, Set, parse};

    #[test]
    fn test_parse_error() {
        let buf: &[u8] = b"foo bar";
        let mut cur = Cursor::new(buf);
        assert!(parse(&mut cur).is_err(), "should not have parsed successfully");
    }

    #[test]
    fn test_simple_get_parse() {
        let buf: &[u8] = b"get foo";
        let mut cur = Cursor::new(buf);
        match parse(&mut cur) {
            Ok(Command::Get(Get(c))) => assert!(c == "foo".to_string()),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_get_parse_error() {
        let buf: &[u8] = b"get foo bar";
        let mut cur = Cursor::new(buf);
        assert!(parse(&mut cur).is_err(), "should not have parsed successfully");
    }

    #[test]
    fn test_simple_set_parse() {
        let buf: &[u8] = b"set foo bar";
        let mut cur = Cursor::new(buf);
        match parse(&mut cur) {
            Ok(Command::Set(Set { key, val })) => {
                assert!(key == "foo".to_string());
                assert!(val == "bar".to_string());
            },
            _ => assert!(false),
        }
    }

    #[test]
    fn test_set_parse_error() {
        let buf: &[u8] = b"set foo bar baz";
        let mut cur = Cursor::new(buf);
        assert!(parse(&mut cur).is_err(), "should not have parsed successfully");
    }
}
