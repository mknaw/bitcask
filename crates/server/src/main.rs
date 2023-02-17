use std::io::Cursor;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use bytes::BytesMut;
use log::{debug, info};
use simple_logger::SimpleLogger;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use store::{get_store_config, BitCask, Command, Message, Result};

mod command;
mod config;

use crate::config::get_server_config;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().init().unwrap();
    let server_config = get_server_config()?;
    let socket_addr = server_config.socket_addr();
    info!("listening on {}", socket_addr);
    let listener = TcpListener::bind(socket_addr).await.unwrap();

    let store_config = Arc::new(get_store_config()?);
    let bitcask = BitCask::new(store_config).unwrap();
    let bitcask_tx = BitCask::listen(bitcask);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let (server_tx, server_rx) = oneshot::channel();
        let mut stream = BufWriter::new(socket);
        if let Ok(command) = parse_command(&mut stream).await {
            bitcask_tx
                .send(Message::Command((command, server_tx)))
                .await
                .unwrap();
            tokio::spawn(async move {
                let res = server_rx.await.unwrap();
                if let Some(res) = res {
                    debug!("sending response: {}", res);
                    stream.write_all(res.as_bytes()).await.unwrap();
                    stream.flush().await.unwrap();
                }
            });
        }
    }
}

async fn parse_command<'cfg>(stream: &mut BufWriter<TcpStream>) -> Result<Command> {
    let mut buf = BytesMut::with_capacity(4 * 1024);
    stream.read_buf(&mut buf).await?;
    let mut cur = Cursor::new(&buf[..]);
    command::parse(&mut cur)
}
