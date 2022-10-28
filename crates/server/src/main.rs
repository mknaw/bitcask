use std::io::Cursor;

use bytes::BytesMut;
use log::info;
use simple_logger::SimpleLogger;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use store::{BitCask, Config, FileLogManager, Result};

mod command;

use command::{Command, Delete, Get, Set};

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().init().unwrap();
    // TODO get port from dotenv
    let config = Config::default();
    let socket_addr = config.socket_addr();
    info!("listening on {}", socket_addr);
    let listener = TcpListener::bind(socket_addr).await.unwrap();
    // TODO probably should be an initializer that just take config and returns a
    // `BitCask<Whatever>`; this crate shouldn't have to worry about internals.
    let log_manager = FileLogManager::new(&config).unwrap();
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
