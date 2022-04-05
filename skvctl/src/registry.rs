use std::collections::HashMap;

use http::Uri;
use pb::*;
use tonic::transport::Channel;

use crate::registry::pb::register_client::RegisterClient;

mod pb {
    tonic::include_proto!("skv_register");
}

#[derive(Debug)]
pub struct Node {
    pub kv_uri: Uri,
}

pub async fn get_nodes(registry: &str) -> anyhow::Result<HashMap<u64, Node>> {
    let registry = registry.parse()?;
    let endpoint = Channel::builder(registry);

    let mut register_client = RegisterClient::connect(endpoint).await?;

    let resp = register_client
        .list_nodes(ListNodesRequest { self_node_id: 0 })
        .await?
        .into_inner();

    resp.node_list
        .into_iter()
        .map(|node_info| {
            let node = Node {
                kv_uri: node_info.kv_uri.parse()?,
            };

            Ok((node_info.node_id, node))
        })
        .collect::<Result<HashMap<_, _>, _>>()
}
