use std::collections::HashMap;
use std::env;
use std::env::VarError;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;

use anyhow::Context;
use async_recursion::async_recursion;
use bytes::Bytes;
use clap::Parser;
use clap::Subcommand;
use http::Uri;
use tonic::transport::Channel;

use crate::pb::kv_client::KvClient;
use crate::pb::*;

mod pb {
    tonic::include_proto!("simple_kv");
}

#[derive(Debug)]
struct Node {
    node_url: Uri,
}

fn get_nodes_from_env() -> HashMap<u64, Node> {
    let raw_nodes = match env::var("SKV_NODES") {
        Err(VarError::NotPresent) => panic!("env SKV_NODES not set"),
        Err(_) => panic!("env SKV_NODES value invalid"),
        Ok(raw_nodes) => raw_nodes,
    };

    raw_nodes
        .split(',')
        .map(|raw_node| {
            let raw_node = raw_node.split(':').collect::<Vec<_>>();
            if raw_node.len() != 2 {
                panic!("SKV_NODES format should be {{node_id:node_url,node_id:node_url}}");
            }

            let node_id = raw_node[0].parse::<u64>().expect("node_id format invalid");
            let node_url = raw_node[1].parse::<Uri>().expect("node_url format invalid");

            (node_id, Node { node_url })
        })
        .collect()
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let nodes = get_nodes_from_env();
    if nodes.is_empty() {
        panic!("no skv nodes set");
    }

    for &node_id in nodes.keys() {
        if handle_operation(&args.operation, &nodes, node_id)
            .await
            .is_ok()
        {
            return Ok(());
        }
    }

    Err(anyhow::anyhow!("all nodes failed"))
}

#[async_recursion]
async fn handle_operation(
    op: &Operation,
    nodes: &HashMap<u64, Node>,
    node_id: u64,
) -> anyhow::Result<()> {
    let node = &nodes[&node_id];

    let mut client = KvClient::connect(node.node_url.clone())
        .await
        .context(format!("connect node {:?} failed", node))?;

    match op {
        Operation::Insert { key, value } => {
            match insert(&mut client, key.clone(), value.clone()).await? {
                KVResult::NotLeader(leader) => handle_operation(op, nodes, leader).await,

                KVResult::Result(_) => Ok(()),
            }
        }
        Operation::Get { key } => match get(&mut client, key.clone()).await? {
            KVResult::NotLeader(leader) => handle_operation(op, nodes, leader).await,
            KVResult::Result(value) => match value {
                None => {
                    println!("not found");

                    Ok(())
                }
                Some(value) => {
                    println!("{}", value);

                    Ok(())
                }
            },
        },
        Operation::Delete { key } => match delete(&mut client, key.clone()).await? {
            KVResult::NotLeader(leader) => handle_operation(op, nodes, leader).await,
            KVResult::Result(_) => Ok(()),
        },
    }
}

async fn insert(
    client: &mut KvClient<Channel>,
    key: OsString,
    value: OsString,
) -> anyhow::Result<KVResult<()>> {
    let resp = client
        .insert(KvInsertRequest {
            key: Bytes::from(key.into_vec()),
            value: Bytes::from(value.into_vec()),
        })
        .await?
        .into_inner();

    match resp.result.context("result is none")? {
        kv_insert_response::Result::Error(err) => Err(anyhow::anyhow!("{}", err.detail)),
        kv_insert_response::Result::NotLeader(not_leader) => {
            Ok(KVResult::NotLeader(not_leader.leader_id))
        }
        kv_insert_response::Result::Success(_) => Ok(KVResult::Result(())),
    }
}

async fn get(
    client: &mut KvClient<Channel>,
    key: OsString,
) -> anyhow::Result<KVResult<Option<String>>> {
    let resp = client
        .get(KvGetRequest {
            key: Bytes::from(key.into_vec()),
        })
        .await?
        .into_inner();

    match resp.result.context("result is none")? {
        kv_get_response::Result::Error(err) => Err(anyhow::anyhow!("{}", err.detail)),
        kv_get_response::Result::NotLeader(not_leader) => {
            Ok(KVResult::NotLeader(not_leader.leader_id))
        }
        kv_get_response::Result::Value(value) => Ok(KVResult::Result(
            value
                .value
                .map(|value| String::from_utf8_lossy(&value).to_string()),
        )),
    }
}

async fn delete(client: &mut KvClient<Channel>, key: OsString) -> anyhow::Result<KVResult<()>> {
    let resp = client
        .delete(KvDeleteRequest {
            key: Bytes::from(key.into_vec()),
        })
        .await?
        .into_inner();

    match resp.result.context("result is none")? {
        kv_delete_response::Result::Error(err) => Err(anyhow::anyhow!("{}", err.detail)),
        kv_delete_response::Result::NotLeader(not_leader) => {
            Ok(KVResult::NotLeader(not_leader.leader_id))
        }
        kv_delete_response::Result::Success(_) => Ok(KVResult::Result(())),
    }
}

enum KVResult<T> {
    Result(T),
    NotLeader(u64),
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
