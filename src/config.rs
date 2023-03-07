use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub port: u16,
    pub max_connection_limit: Option<usize>,
    pub rocksdb_path: Option<String>,
}
