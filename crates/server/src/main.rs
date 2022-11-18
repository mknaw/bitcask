use std::io::Cursor;
use std::sync::Mutex;

use bytes::BytesMut;
use log::info;
use simple_logger::SimpleLogger;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

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

    let (tx, rx) = mpsc::channel(32);

    bitcask(rx);

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        let (resp_tx, resp_rx) = oneshot::channel();
        if let Ok(command) = parse_command(socket).await {
            tx.send((command, resp_tx)).await.unwrap();
            let res = resp_rx.await;
            info!("response: {:?}", res);
        }
    }
}

fn bitcask(mut rx: mpsc::Receiver<(Command, oneshot::Sender<Option<String>>)>) {
    tokio::spawn(async move {
        // TODO should create `log_dir` if doesn't yet exist.
        let store_config = StoreConfig::default();
        let log_manager = FileLogManager::new(&store_config).unwrap();
        // TODO might have to move the receiver onto the `bitcask` itself.
        let bitcask = Mutex::new(BitCask::new(log_manager));
        while let Some((cmd, resp_tx)) = rx.recv().await {
            {
                let mut bitcask = bitcask.lock().unwrap();
                match cmd {
                    Command::Set(Set { key, val }) => {
                        bitcask.set(&key, &val).unwrap();
                        resp_tx.send(None).unwrap();
                        if bitcask.should_merge() {
                            // TODO should be able to do the log file writing of it in the background
                            // without interfering with the main loop;
                            // then only have to wait when files deleted and keydir swapped out.
                            // bitcask.merge().unwrap();
                            info!("merging...");
                            std::thread::sleep(std::time::Duration::from_secs(10));
                            info!("All finished");
                        }
                    }
                    Command::Get(Get(get)) => match bitcask.get(&get) {
                        Ok(val) => resp_tx.send(Some(val)).unwrap(),
                        // TODO how to send errors back?
                        Err(e) => info!("{:?}", e),
                    },
                    Command::Delete(Delete(key)) => {
                        bitcask.delete(&key).unwrap();
                        resp_tx.send(None).unwrap();
                    }
                };
            }
        }
    });
}

async fn parse_command<'cfg>(socket: TcpStream) -> Result<command::Command> {
    let mut stream = BufWriter::new(socket);
    let mut buf = BytesMut::with_capacity(4 * 1024);
    stream.read_buf(&mut buf).await?;
    let mut cur = Cursor::new(&buf[..]);
    command::parse(&mut cur)
}
