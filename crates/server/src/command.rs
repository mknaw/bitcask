use std::fmt;

use nom::branch::alt;
use nom::bytes::streaming::{tag, take};
use nom::character::complete::u64 as nom_u64;
use nom::combinator::all_consuming;
use nom::sequence::preceded;
use nom::IResult;

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

#[derive(Debug)]
pub enum Command {
    Set((String, String)),
    Get(String),
    Delete(String),
    Merge,
}

/// Parse a `Command::Get` from `input`.
fn parse_get(input: &str) -> IResult<&str, Command> {
    let (input, len) = preceded(tag("get\r\n"), nom_u64)(input)?;
    let (input, key) = all_consuming(preceded(tag("\r\n"), take(len)))(input)?;
    Ok((input, Command::Get(key.to_string())))
}

/// Parse a `Command::Set` from `input`.
fn parse_set(input: &str) -> IResult<&str, Command> {
    let (input, len) = preceded(tag("set\r\n"), nom_u64)(input)?;
    let (input, key) = preceded(tag("\r\n"), take(len))(input)?;
    let (input, len) = preceded(tag("\r\n"), nom_u64)(input)?;
    let (input, val) = all_consuming(preceded(tag("\r\n"), take(len)))(input)?;
    Ok((input, Command::Set((key.to_string(), val.to_string()))))
}

/// Parse a `Command::Delete` from `input`.
fn parse_delete(input: &str) -> IResult<&str, Command> {
    // TODO should be tag, then any amount of whitespace
    let (input, len) = preceded(tag("delete\r\n"), nom_u64)(input)?;
    let (input, key) = all_consuming(preceded(tag("\r\n"), take(len)))(input)?;
    Ok((input, Command::Delete(key.to_string())))
}

/// Parse a `Command::Merge` from `input`.
fn parse_merge(input: &str) -> IResult<&str, Command> {
    // TODO should be tag, then any amount of whitespace
    let (input, _) = all_consuming(tag("merge\n"))(input)?;
    Ok((input, Command::Merge))
}

pub fn parse(input: &str) -> crate::Result<Command> {
    let (_, parsed) = alt((parse_get, parse_set, parse_delete, parse_merge))(input).unwrap();
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{parse, Command};

    #[test]
    fn test_parse_get() {
        match parse("get\r\n3\r\nfoo") {
            Ok(Command::Get(c)) => assert!(c == "foo"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_set() {
        match parse("set\r\n3\r\nfoo\r\n7\r\nbar baz") {
            Ok(Command::Set((key, val))) => {
                assert!(key == "foo");
                assert!(val == "bar baz");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_delete() {
        match parse("delete\r\n3\r\nfoo") {
            Ok(Command::Delete(c)) => assert!(c == "foo"),
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
