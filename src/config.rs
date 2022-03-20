use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::{ColorChoice, Parser};
use http::uri::Scheme;
use http::Uri;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub local_kv_listen_addr: SocketAddr,
    pub local_raft_listen_addr: SocketAddr,
    pub node_uri: String,
    pub node_id: u64,
    pub data: PathBuf,
    pub registry_uri: String,
    #[serde(default)]
    pub debug_log: bool,
}

impl Config {
    fn valid(&self) -> anyhow::Result<()> {
        if self.node_id == 0 {
            return Err(anyhow::anyhow!("local node id can't be 0"));
        }

        let node_uri = self
            .node_uri
            .parse::<Uri>()
            .with_context(|| format!("node uri {} is invalid", self.node_uri))?;

        let node_uri_scheme = node_uri
            .scheme()
            .with_context(|| "scheme should be http or https")?;

        if *node_uri_scheme != Scheme::HTTP && *node_uri_scheme != Scheme::HTTPS {
            return Err(anyhow::anyhow!("scheme should be http or https"));
        }

        let registry_uri = self
            .registry_uri
            .parse::<Uri>()
            .with_context(|| format!("registry uri {} is invalid", self.registry_uri))?;

        let scheme = registry_uri
            .scheme()
            .with_context(|| "scheme should be http or https")?;

        if *scheme != Scheme::HTTP && *scheme != Scheme::HTTPS {
            return Err(anyhow::anyhow!("scheme should be http or https"));
        }

        Ok(())
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

    config.valid()?;

    Ok(config)
}
