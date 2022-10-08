use std::io::Cursor;
// use std::net::TcpListener;

// use bytes::BytesMut;
// use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, BufWriter};

use crate::config::Config;
use crate::bitcask::BitCask;
use crate::lib::Result;

mod bitcask;
mod command;
mod config;
mod keydir;
mod lib;
mod log_manager;
mod log_writer;

// pub type Error = Box<dyn std::error::Error + Send + Sync>;
// type Result<T> = std::result::Result<T, Error>;

fn main() -> Result<()> {
    let config = Config::new();
    let mut bitcask = BitCask::new(&config);
    // let listener = TcpListener::bind(config.socket_addr());
    let buf: &[u8] = b"set foo bar";
    let mut cur = Cursor::new(buf);
    let command = command::parse(&mut cur)?;
    match command {
        command::Command::Set(set) => bitcask.set(set)?,
        _ => {
            println!("wrong command!");
        }
    };
    let val = bitcask.get("foo".to_string())?;
    println!("{}", val);
    Ok(())
}


// #[tokio::main]
// async fn main() -> Result<()> {
    // // TODO get port from dotenv
    // let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // loop {
        // // The second item contains the IP and port of the new connection.
        // let (socket, _) = listener.accept().await.unwrap();
        // process(socket).await?;
    // }

    // // file.write_all(b"Hello, world!")?;
// }

// async fn process(socket: TcpStream) -> Result<()> {

    // let mut stream = BufWriter::new(socket);
    // let mut buf = BytesMut::with_capacity(4 * 1024);
    // stream.read_buf(&mut buf).await?;
    // let mut cur = Cursor::new(&buf[..]);
    // // TODO our standard error won't cut it in async world
    // nice_parse(&mut cur);
    // Ok(())
// }
