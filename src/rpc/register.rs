use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use flume::r#async::SendSink;
use flume::Sender;
use futures_util::{stream, SinkExt, StreamExt, TryStreamExt};
use http::Uri;
use raft::eraftpb;
use tap::TapFallible;
use tokio::time;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument};

use crate::rpc::pb::register::register_client::RegisterClient;
use crate::rpc::pb::register::*;

#[derive(Debug, Eq, PartialEq)]
struct PeerNode {
    uri: Uri,
}

pub enum NodeChangeEvent {
    Add {
        node_id: u64,
        mailbox_sender: Option<Sender<eraftpb::Message>>,
    },

    Remove {
        node_id: u64,
    },
}

pub enum RpcRaftNodeEvent {
    Add {
        node_id: u64,
        uri: Uri,
        /// let raft module to provide a mailbox sender
        mailbox_sender_provider: SendSink<'static, Sender<eraftpb::Message>>,
    },

    Remove {
        node_id: u64,
    },
}

pub struct Register {
    registry_client: RegisterClient<Channel>,

    node_id: u64,
    node_uri: Uri,
    period: Duration,

    /// current nodes doesn't include self
    current_nodes: HashMap<u64, PeerNode>,
    rpc_raft_node_change_event_sender: SendSink<'static, RpcRaftNodeEvent>,
    node_list_change_event_sender: SendSink<'static, NodeChangeEvent>,
}

impl Register {
    pub fn new(
        channel: Channel,
        node_id: u64,
        node_uri: Uri,
        period: Duration,
        rpc_raft_node_change_event_sender: SendSink<'static, RpcRaftNodeEvent>,
        node_list_change_event_sender: SendSink<'static, NodeChangeEvent>,
    ) -> Self {
        Self {
            registry_client: RegisterClient::new(channel),
            node_id,
            node_uri,
            period,
            current_nodes: Default::default(),
            rpc_raft_node_change_event_sender,
            node_list_change_event_sender,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            self.register_self().await?;

            debug!("register self done");

            self.update_node_list().await?;

            debug!("update node list done");

            time::sleep(self.period / 2).await;
        }
    }

    #[instrument(skip(self), err)]
    async fn register_self(&mut self) -> anyhow::Result<()> {
        let request = RegisterNodeRequest {
            node_id: self.node_id,
            period: self.period.as_millis() as _,
            node_uri: self.node_uri.to_string(),
        };

        match time::timeout(
            Duration::from_secs(1),
            self.registry_client.register_node(request),
        )
        .await
        {
            Err(_) => {
                error!("register self timeout");

                Err(anyhow::anyhow!("register self timeout"))
            }

            Ok(Err(err)) => {
                error!(?err, uri = %self.node_uri, "register self failed");

                Err(err).context("register self failed")
            }

            Ok(Ok(_)) => {
                info!(uri = %self.node_uri, "register self done");

                Ok(())
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn update_node_list(&mut self) -> anyhow::Result<()> {
        let resp = match time::timeout(
            Duration::from_secs(1),
            self.registry_client.list_nodes(ListNodesRequest {
                self_node_id: self.node_id,
            }),
        )
        .await
        {
            Err(_) => {
                error!("list nodes timeout");

                return Err(anyhow::anyhow!("list nodes timeout"));
            }

            Ok(Err(err)) => {
                error!(?err, "list nodes failed");

                return Err(err).context("list nodes failed");
            }

            Ok(Ok(resp)) => resp.into_inner(),
        };

        debug!(latest_nodes = ?resp.node_list, "list nodes done");

        let latest_nodes =
            resp.node_list
                .into_iter()
                .map(|node| {
                    let uri = node.node_uri.parse::<Uri>().tap_err(
                        |err| error!(?err, uri = %node.node_uri, "parse node uri failed"),
                    )?;

                    Ok((node.node_id, PeerNode { uri }))
                })
                .collect::<anyhow::Result<HashMap<_, _>>>()?;

        debug!(?latest_nodes, "collect latest nodes done");

        let (removed, add) = diff_node_list(&self.current_nodes, &latest_nodes);

        info!(?removed, ?add, "get removed and add node id done");

        self.remove_nodes(removed).await?;

        info!("remove nodes done");

        self.add_nodes(add, latest_nodes).await?;

        info!("add nodes done");

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn remove_nodes(&mut self, removed: Vec<u64>) -> anyhow::Result<()> {
        for node_id in removed.iter() {
            self.current_nodes.remove(node_id);
        }

        {
            let mut removed = stream::iter(
                removed
                    .iter()
                    .map(|node_id| RpcRaftNodeEvent::Remove { node_id: *node_id })
                    .map(Ok),
            );

            self.rpc_raft_node_change_event_sender
                .send_all(&mut removed)
                .await
                .tap_err(|err| error!(?err, "send removed node id to rpc raft failed"))?;

            info!("send removed node id to rpc raft done");
        }

        let mut removed = stream::iter(
            removed
                .into_iter()
                .map(|node_id| NodeChangeEvent::Remove { node_id })
                .map(Ok),
        );

        self.node_list_change_event_sender
            .send_all(&mut removed)
            .await
            .tap_err(|err| error!(?err, "send removed node id to node failed"))?;

        info!("send removed node id to node done");

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn add_nodes(
        &mut self,
        add: Vec<u64>,
        latest_nodes: HashMap<u64, PeerNode>,
    ) -> anyhow::Result<()> {
        // can update the current nodes directly
        self.current_nodes = latest_nodes;

        let mut mailbox_sender_collectors = Vec::with_capacity(add.len());
        {
            let mut add = stream::iter(
                add.iter()
                    .map(|node_id| {
                        let uri = self.current_nodes[node_id].uri.clone();

                        let (mailbox_sender_provider, mailbox_sender_collector) = flume::bounded(1);

                        mailbox_sender_collectors.push(mailbox_sender_collector);

                        RpcRaftNodeEvent::Add {
                            node_id: *node_id,
                            uri,
                            mailbox_sender_provider: mailbox_sender_provider.into_sink(),
                        }
                    })
                    .map(Ok),
            );

            self.rpc_raft_node_change_event_sender
                .send_all(&mut add)
                .await
                .tap_err(|err| error!(?err, "send add node id and uri to rpc raft failed"))?;

            info!("send add node id and uri to rpc raft done");
        }

        let mailbox_senders = stream::iter(mailbox_sender_collectors)
            .then(|collector| async move {
                collector
                    .into_stream()
                    .next()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("no mailbox sender found"))
            })
            .try_collect::<Vec<_>>()
            .await
            .tap_err(|err| error!(?err, "collect mailbox sender failed"))?;

        info!("collect mailbox senders done");

        let mut add = stream::iter(
            add.into_iter()
                .zip(mailbox_senders)
                .map(|(node_id, mailbox_sender)| NodeChangeEvent::Add {
                    node_id,
                    mailbox_sender: Some(mailbox_sender),
                })
                .map(Ok),
        );

        self.node_list_change_event_sender
            .send_all(&mut add)
            .await
            .tap_err(|err| error!(?err, "send add node id and uri to node failed"))?;

        Ok(())
    }
}

/// diff old and new peer node map, return (removed, add) node ids
fn diff_node_list(
    old: &HashMap<u64, PeerNode>,
    new: &HashMap<u64, PeerNode>,
) -> (Vec<u64>, Vec<u64>) {
    let mut removed = vec![];
    let mut add = vec![];

    for (node_id, peer_node) in old.iter() {
        match new.get(node_id) {
            None => {
                removed.push(*node_id);
            }

            Some(new_peer_node) => {
                if *peer_node != *new_peer_node {
                    removed.push(*node_id);
                }
            }
        }
    }

    for (node_id, peer_node) in new.iter() {
        match old.get(node_id) {
            None => {
                add.push(*node_id);
            }

            Some(old_peer_node) => {
                if *old_peer_node != *peer_node {
                    add.push(*node_id);
                }
            }
        }
    }

    (removed, add)
}
