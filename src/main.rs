use std::io::Cursor;

use bytes::BytesMut;
use log::info;
use simple_logger::SimpleLogger;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::config::Config;
use crate::bitcask::BitCask;
use crate::lib::Result;

mod bitcask;
mod command;
mod config;
mod keydir;
mod lib;
mod log_manager;
mod log_reader;
mod log_writer;
mod merge;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().init().unwrap();
    // TODO get port from dotenv
    let config = Config::new();
    let socket_addr = config.socket_addr();
    info!("listening on {}", socket_addr);
    let listener = TcpListener::bind(socket_addr).await.unwrap();
    let mut bitcask = BitCask::new(&config);

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        process(&mut bitcask, socket).await?;
    }
}

async fn process(bitcask: &mut BitCask<'_>, socket: TcpStream) -> Result<()> {

    let mut stream = BufWriter::new(socket);
    let mut buf = BytesMut::with_capacity(4 * 1024);
    stream.read_buf(&mut buf).await?;
    let mut cur = Cursor::new(&buf[..]);
    match command::parse(&mut cur) {
        Ok(command::Command::Set(set)) => bitcask.set(set)?,
        Ok(command::Command::Get(get)) => {
            info!("{}", bitcask.get(get)?);
        },
        Err(e) => {
            info!("{}", e);
        },
    };
    Ok(())
}
