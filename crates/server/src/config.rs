use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub struct Config {
    pub host: IpAddr,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 6969,
        }
    }
}

// TODO read config vals from env
impl Config {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}
