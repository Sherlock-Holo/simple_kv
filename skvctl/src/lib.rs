use std::collections::HashMap;
use std::env;
use std::env::VarError;
use std::ffi::OsString;

use clap::Parser;
use clap::Subcommand;
use http::Uri;

use crate::registry::Node;

mod kv;
mod registry;

fn get_registry_from_env() -> Option<String> {
    env::var("SKV_REGISTRY").ok()
}

fn get_nodes_from_env() -> HashMap<u64, Node> {
    let raw_nodes = match env::var("SKV_NODES") {
        Err(VarError::NotPresent) => panic!("env SKV_NODES not set"),
        Err(_) => panic!("env SKV_NODES value invalid"),
        Ok(raw_nodes) => raw_nodes,
    };

    raw_nodes
        .split(';')
        .map(|raw_node| {
            let raw_node = raw_node.split(',').collect::<Vec<_>>();
            if raw_node.len() != 2 {
                panic!("SKV_NODES format should be {{node_id,node_url;node_id,node_url;}}");
            }

            let node_id = raw_node[0].parse::<u64>().expect("node_id format invalid");
            let kv_uri = raw_node[1].parse::<Uri>().expect("node_url format invalid");

            (node_id, Node { kv_uri })
        })
        .collect()
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let nodes = match get_registry_from_env() {
        None => get_nodes_from_env(),
        Some(registry_uri) => registry::get_nodes(&registry_uri).await?,
    };

    if nodes.is_empty() {
        panic!("no skv nodes set");
    }

    for &node_id in nodes.keys() {
        match kv::handle_operation(&args.operation, &nodes, node_id).await {
            Err(err) => {
                eprintln!("failed: {:?}", err);
            }

            Ok(_) => return Ok(()),
        }
    }

    Err(anyhow::anyhow!("all nodes failed"))
}

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    operation: Operation,
}

#[derive(Subcommand)]
enum Operation {
    Insert { key: OsString, value: OsString },
    Get { key: OsString },
    Delete { key: OsString },
}
