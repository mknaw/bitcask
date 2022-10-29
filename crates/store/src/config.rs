use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
};

pub struct Config<'a> {
    pub log_dir: &'a Path,
    host: IpAddr,
    port: u16,
    pub max_log_file_size: u64,
}

impl<'a> Default for Config<'a> {
    fn default() -> Self {
        Self {
            log_dir: Path::new("/tmp/bitcask/"),
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 6969,
            max_log_file_size: 128,
        }
    }
}

// TODO read config vals from env
impl<'a> Config<'a> {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}
