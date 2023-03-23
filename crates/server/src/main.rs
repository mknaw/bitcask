use std::sync::Arc;

use bytes::BytesMut;
use log::{debug, info};
use simple_logger::SimpleLogger;
use store::{get_store_config, BitCask, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::command::Command;
use crate::config::get_server_config;

mod command;
mod config;

pub type BitCaskTx = mpsc::Sender<(Command, oneshot::Sender<Option<String>>)>;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().init().unwrap();
    let server_config = get_server_config()?;
    let socket_addr = server_config.socket_addr();
    info!("listening on {}", socket_addr);
    let listener = TcpListener::bind(socket_addr).await.unwrap();

    let store_config = Arc::new(get_store_config()?);
    let bitcask = BitCask::new(store_config).unwrap();
    let bitcask_tx = bitcask_loop(bitcask);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let (server_tx, server_rx) = oneshot::channel();
        let mut stream = BufWriter::new(socket);
        if let Ok(command) = parse_command(&mut stream).await {
            bitcask_tx.send((command, server_tx)).await.unwrap();
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

/// Put data sent from connection through command parser.
async fn parse_command<'cfg>(stream: &mut BufWriter<TcpStream>) -> Result<Command> {
    let mut buf = BytesMut::with_capacity(4 * 1024);
    stream.read_buf(&mut buf).await?;
    command::parse(std::str::from_utf8(&buf).unwrap())
}

fn bitcask_loop(bitcask: BitCask) -> BitCaskTx {
    let (tx, mut rx) = mpsc::channel::<(Command, oneshot::Sender<Option<String>>)>(32);
    let bitcask = Arc::new(bitcask);

    let tx = tx.clone();
    tokio::spawn(async move {
        while let Some((cmd, resp_tx)) = rx.recv().await {
            debug!("received command: {:?}", cmd);
            match cmd {
                Command::Set((key, val)) => {
                    let bitcask = bitcask.clone();
                    tokio::spawn(async move {
                        bitcask.set(&key, &val).unwrap();
                        resp_tx.send(None).unwrap();
                    });
                }
                Command::Get(key) => {
                    let bitcask = bitcask.clone();
                    tokio::spawn(async move {
                        let val = bitcask.get(&key).unwrap();
                        resp_tx.send(Some(val)).unwrap();
                    });
                }
                Command::Delete(key) => {
                    bitcask.delete(&key).unwrap();
                    resp_tx.send(None).unwrap();
                }
                Command::Merge => {
                    let bitcask = bitcask.clone();
                    tokio::spawn(async move {
                        bitcask.merge().unwrap();
                        resp_tx.send(Some("all done!".to_string())).unwrap();
                    });
                }
            };
        }
    });

    tx
}
