use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, path::Path};

pub struct Config<'a> {
    pub log_dir: &'a Path,
    host: IpAddr,
    port: u16,
}

// TODO read config vals from dot env
impl<'a> Config<'a> {
    pub fn new() -> Self {
        Self { 
            log_dir: Path::new("/tmp/bitcask/"),
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 6969,
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}
