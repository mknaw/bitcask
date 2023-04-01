use std::fmt;

use nom::branch::alt;
use nom::bytes::streaming::{tag, take};
use nom::character::complete::{line_ending, multispace0, u64 as nom_u64};
use nom::combinator::all_consuming;
use nom::sequence::preceded;
use nom::IResult;

#[derive(Debug)]
pub struct ParseError;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parse error occurred")
    }
}

impl ::std::error::Error for ParseError {}

#[derive(Debug)]
pub enum Command {
    Set((Vec<u8>, Vec<u8>)),
    Get(Vec<u8>),
    Delete(Vec<u8>),
    Merge,
}

fn from_utf8(input: &[u8]) -> &str {
    std::str::from_utf8(input).unwrap_or("UNREPRESENTABLE")
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Set((key, value)) => {
                write!(f, "Set \"{}\" => \"{}\"", from_utf8(key), from_utf8(value))
            }
            Command::Get(key) => write!(f, "Get \"{}\"", from_utf8(key)),
            Command::Delete(key) => write!(f, "Delete \"{}\"", from_utf8(key)),
            Command::Merge => write!(f, "Merge"),
        }
    }
}

/// Parse a `Command::Get` from `input`.
fn parse_get(i: &str) -> IResult<&str, Command> {
    let (i, _) = preceded(tag("get"), line_ending)(i)?;
    let (i, len) = nom_u64(i)?;
    let (i, key) = preceded(line_ending, take(len))(i)?;
    Ok((i, Command::Get(key.as_bytes().to_vec())))
}

/// Parse a `Command::Set` from `i`.
fn parse_set(i: &str) -> IResult<&str, Command> {
    let (i, _) = preceded(tag("set"), line_ending)(i)?;
    let (i, len) = nom_u64(i)?;
    let (i, key) = preceded(line_ending, take(len))(i)?;
    let (i, len) = preceded(line_ending, nom_u64)(i)?;
    let (i, val) = preceded(line_ending, take(len))(i)?;
    let (i, _) = all_consuming(multispace0)(i)?;
    Ok((
        i,
        Command::Set((key.as_bytes().to_vec(), val.as_bytes().to_vec())),
    ))
}

/// Parse a `Command::Delete` from `i`.
fn parse_delete(i: &str) -> IResult<&str, Command> {
    // TODO should be tag, then any amount of whitespace
    let (i, _) = preceded(tag("delete"), line_ending)(i)?;
    let (i, len) = nom_u64(i)?;
    let (i, key) = preceded(line_ending, take(len))(i)?;
    let (i, _) = all_consuming(multispace0)(i)?;
    Ok((i, Command::Delete(key.as_bytes().to_vec())))
}

/// Parse a `Command::Merge` from `i`.
fn parse_merge(i: &str) -> IResult<&str, Command> {
    // TODO should be tag, then any amount of whitespace
    let (i, _) = tag("merge")(i)?;
    let (i, _) = all_consuming(multispace0)(i)?;
    Ok((i, Command::Merge))
}

fn _parse(i: &str) -> IResult<&str, Command> {
    let (i, parsed) = alt((parse_get, parse_set, parse_delete, parse_merge))(i)?;
    let (i, _) = all_consuming(multispace0)(i)?;
    Ok((i, parsed))
}

pub fn parse(input: &str) -> Result<Command, ParseError> {
    let (_, command) = _parse(input).map_err(|_| ParseError)?;
    Ok(command)
}

#[cfg(test)]
mod tests {
    use super::{parse, Command};

    #[test]
    fn test_parse_get() {
        match parse("get\r\n3\r\nfoo") {
            Ok(Command::Get(c)) => assert!(c == b"foo"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_set() {
        match parse("set\r\n3\r\nfoo\r\n7\r\nbar baz") {
            Ok(Command::Set((key, val))) => {
                assert!(key == b"foo");
                assert!(val == b"bar baz");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_delete() {
        match parse("delete\r\n3\r\nfoo") {
            Ok(Command::Delete(c)) => assert!(c == b"foo"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_merge() {
        match parse("merge\n") {
            Ok(Command::Merge) => (),
            _ => panic!(),
        }
    }
}
