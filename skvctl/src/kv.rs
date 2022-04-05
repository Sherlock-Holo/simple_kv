use std::collections::HashMap;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::process;

use anyhow::Context;
use async_recursion::async_recursion;
use bytes::Bytes;
use pb::kv_client::KvClient;
use pb::*;
use tokio::io;
use tokio::io::AsyncWriteExt;
use tonic::transport::Channel;

use crate::{registry::Node, Operation};

mod pb {
    tonic::include_proto!("simple_kv");
}

#[async_recursion]
pub(crate) async fn handle_operation(
    op: &Operation,
    nodes: &HashMap<u64, Node>,
    node_id: u64,
) -> anyhow::Result<()> {
    let node = &nodes[&node_id];

    let mut client = KvClient::connect(node.kv_uri.clone())
        .await
        .with_context(|| format!("connect node {:?} failed", node))?;

    match op {
        Operation::Insert { key, value } => {
            match insert(&mut client, key.clone(), value.clone()).await? {
                KVResult::NotLeader(mut leader) => {
                    if leader == 0 {
                        leader = node_id;
                    }
                    handle_operation(op, nodes, leader).await
                }

                KVResult::Result(_) => Ok(()),
            }
        }
        Operation::Get { key } => match get(&mut client, key.clone()).await? {
            KVResult::NotLeader(mut leader) => {
                if leader == 0 {
                    leader = node_id;
                }
                handle_operation(op, nodes, leader).await
            }
            KVResult::Result(value) => match value {
                None => process::exit(1),
                Some(value) => {
                    io::stdout().write_all(&value).await?;

                    Ok(())
                }
            },
        },
        Operation::Delete { key } => match delete(&mut client, key.clone()).await? {
            KVResult::NotLeader(mut leader) => {
                if leader == 0 {
                    leader = node_id;
                }
                handle_operation(op, nodes, leader).await
            }
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
) -> anyhow::Result<KVResult<Option<Bytes>>> {
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
        kv_get_response::Result::Value(value) => Ok(KVResult::Result(value.value)),
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
