use std::io::{self, Read, Write};
use std::net::TcpStream;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long = "HOST")]
    host: Option<String>,
    #[arg(short = 'p', long = "PORT")]
    port: Option<u16>,
}

#[derive(Subcommand)]
enum Commands {
    Set { key: String, val: String },
    Get { key: String },
    Merge,
}

fn main() {
    let cli = Cli::parse();
    // TODO should also take these from the same env vars as we have in `store`
    let host = cli.host.unwrap_or("127.0.0.1".to_string());
    let port = cli.port.unwrap_or(6969);
    let address = format!("{}:{}", host, port);

    match &cli.command {
        Commands::Set { key, val } => {
            let message = format!(
                "set\r\n{}\r\n{}\r\n{}\r\n{}",
                key.len(),
                key,
                val.len(),
                val
            );
            send_message(address, &message);
        }
        Commands::Get { key } => {
            let message = format!("get\r\n{}\r\n{}", key.len(), key);
            let response = send_message(address, &message);
            println!("{}", response);
        }
        Commands::Merge => {
            let response = send_message(address, "merge");
            println!("{}", response);
        }
    }
}

fn send_message(address: String, message: &str) -> String {
    let mut stream = TcpStream::connect(address).unwrap();

    stream.write_all(message.as_bytes()).unwrap();

    let mut buffer = String::new();
    let mut reader = io::BufReader::new(stream);
    reader.read_to_string(&mut buffer).unwrap();

    buffer
}
