use std::net::{IpAddr, SocketAddr};

use ::config::{Config, ConfigError};
use serde::Deserialize;

/// Configuration specific to the bitcask server.
#[derive(Deserialize)]
pub struct ServerConfig {
    pub host: IpAddr,
    pub port: u16,
}

impl ServerConfig {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

/// Coalesce env vars with defaults to get a `ServerConfig`.
pub fn get_server_config() -> Result<ServerConfig, ConfigError> {
    let config = Config::builder()
        .add_source(config::Environment::with_prefix("BITCASK").try_parsing(true))
        .set_default("host", "127.0.0.1")?
        .set_default("port", "6969")?
        .build()?;
    // TODO would be good to validate that the provided values make sense.
    config.try_deserialize()
}
