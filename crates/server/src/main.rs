use std::io::Cursor;

use bytes::BytesMut;
use log::info;
use simple_logger::SimpleLogger;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use store::{BitCask, Config as StoreConfig, FileLogManager, Result};

mod command;
mod config;

use command::{Command, Delete, Get, Set};
use config::Config as ServerConfig;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().init().unwrap();
    // TODO settings from env
    let server_config = ServerConfig::default();
    let socket_addr = server_config.socket_addr();
    info!("listening on {}", socket_addr);
    let listener = TcpListener::bind(socket_addr).await.unwrap();
    let store_config = StoreConfig::default();
    let log_manager = FileLogManager::new(&store_config).unwrap();
    let mut bitcask = BitCask::new(log_manager);

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        process(&mut bitcask, socket).await?;
    }
}

async fn process<'cfg>(
    bitcask: &mut BitCask<FileLogManager<'cfg>>,
    socket: TcpStream,
) -> Result<()> {
    let mut stream = BufWriter::new(socket);
    let mut buf = BytesMut::with_capacity(4 * 1024);
    stream.read_buf(&mut buf).await?;
    let mut cur = Cursor::new(&buf[..]);
    match command::parse(&mut cur) {
        Ok(Command::Set(Set { key, val })) => bitcask.set(&key, &val)?,
        Ok(Command::Get(Get(get))) => match bitcask.get(&get) {
            Ok(val) => info!("{}", val),
            Err(e) => info!("{:?}", e),
        },
        Ok(Command::Delete(Delete(key))) => bitcask.delete(&key)?,
        Err(e) => {
            info!("{}", e);
        }
    };
    Ok(())
}
