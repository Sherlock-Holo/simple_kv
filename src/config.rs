use std::collections::HashSet;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{ColorChoice, Parser};
use http::Uri;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PeerConfig {
    pub node_id: u64,
    pub node_raft_url: String,
    pub node_kv_url: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub local_kv_listen_addr: SocketAddr,
    pub local_raft_listen_addr: SocketAddr,
    pub node_id: u64,
    pub data: PathBuf,

    pub peers: Vec<PeerConfig>,
}

impl Config {
    fn valid(&self) {
        if self.peers.is_empty() {
            panic!("not support single node");
        }

        assert_ne!((self.peers.len() + 1) % 2, 0, "nodes number must be odd");

        let mut node_ids = HashSet::with_capacity(self.peers.len() + 1);
        for peer_node_id in self.peers.iter().map(|peer| peer.node_id) {
            if !node_ids.insert(peer_node_id) {
                panic!("peer node id {} is duplicated", peer_node_id);
            }
        }

        if !node_ids.insert(self.node_id) {
            panic!("local node id {} is duplicated", self.node_id);
        }

        self.peers.iter().for_each(|peer| {
            Uri::from_str(&peer.node_raft_url).unwrap_or_else(|err| {
                panic!("node raft url {} is invalid: {}", peer.node_raft_url, err)
            });

            Uri::from_str(&peer.node_kv_url).unwrap_or_else(|err| {
                panic!("node kv url {} is invalid: {}", peer.node_raft_url, err)
            });
        });
    }
}

#[derive(Debug, Parser)]
#[clap(color = ColorChoice::Always)]
struct CommandArg {
    #[clap(short, long)]
    config: PathBuf,
}

pub fn get_config() -> anyhow::Result<Config> {
    let command_arg = CommandArg::parse();

    let file = File::open(&command_arg.config)?;

    let config: Config = serde_yaml::from_reader(file)?;

    config.valid();

    Ok(config)
}
