use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bytes::BytesMut;
use dashmap::DashMap;
use flume::r#async::{RecvStream, SendSink};
use flume::{Receiver, Sender};
use futures_util::future::Either;
use futures_util::{future, stream, SinkExt, Stream, StreamExt};
use prost07::Message;
use raft::eraftpb;
use rand::prelude::*;
use tap::TapFallible;
use tokio::task::JoinHandle;
use tokio::time;
use tonic::transport::{Channel, Server};
use tonic::{IntoRequest, Request, Response, Status};
use tracing::{debug, error, info, info_span, instrument, warn, Instrument};

use super::connect::Connector;
use super::pb::simple_kv::raft_client::RaftClient;
use super::pb::simple_kv::raft_server::{Raft, RaftServer};
use super::pb::simple_kv::*;
use super::register::RpcRaftNodeEvent;

struct PeerNode {
    id: u64,
    grpc_client: RaftClient<Channel>,
    mailbox: RecvStream<'static, eraftpb::Message>,
}

impl PeerNode {
    fn send_message_background(&self, message: eraftpb::Message) -> JoinHandle<anyhow::Result<()>> {
        let peer_id = self.id;
        let mut grpc_client = self.grpc_client.clone();

        tokio::spawn(
            async move {
                let mut buf = BytesMut::with_capacity(message.encoded_len());

                message
                    .encode(&mut buf)
                    .tap_err(|err| error!(%err, "encode raft message failed"))?;

                let message = buf.freeze();

                debug!("encode raft message done");

                let timeout = Duration::from_secs(1);

                let mut request = RaftMessageRequest { message }.into_request();
                request.set_timeout(timeout);

                match time::timeout(timeout, grpc_client.send_message(request)).await {
                    Err(_) => {
                        error!(peer_id, "send raft message to peer timeout");

                        return Err(anyhow::anyhow!("send raft message to peer timeout"));
                    }

                    Ok(Err(err)) => {
                        error!(peer_id, %err, "send raft message to peer node failed");

                        Err(err).context("send raft message to peer node failed")
                    }

                    Ok(Ok(_)) => {
                        debug!("send raft message to peer node done");

                        Ok(())
                    }
                }
            }
            .instrument(info_span!("send_message")),
        )
    }
}

struct LocalNode {
    raft_message_sender: SendSink<'static, eraftpb::Message>,
}

#[async_trait]
impl Raft for LocalNode {
    #[instrument(skip(self, request), err)]
    async fn send_message(
        &self,
        request: Request<RaftMessageRequest>,
    ) -> Result<Response<RaftMessageResponse>, Status> {
        let request = request.into_inner();

        let message = eraftpb::Message::decode(request.message.as_ref()).map_err(|err| {
            error!(%err, "decode raft message request failed");

            Status::failed_precondition("decode raft message request failed")
        })?;

        debug!("decode raft message done");

        self.raft_message_sender
            .clone()
            .send(message)
            .await
            .map_err(|err| {
                error!(%err, "send other node raft message to local node failed");

                Status::failed_precondition("send other node raft message to local node failed")
            })?;

        debug!("send other node raft message to local node done");

        Ok(Response::new(RaftMessageResponse {}))
    }
}

pub struct Rpc {
    grpc_listen_addr: SocketAddr,
    local_node: Option<LocalNode>,
    peer_node_mailboxes: PeerNodeMailboxes,
    rpc_raft_node_change_event_receiver: Receiver<RpcRaftNodeEvent>,
}

impl Rpc {
    pub fn new(
        listen_addr: SocketAddr,
        raft_message_sender: Sender<eraftpb::Message>,
        rpc_raft_node_change_event_receiver: Receiver<RpcRaftNodeEvent>,
    ) -> Self {
        let local_node = LocalNode {
            raft_message_sender: raft_message_sender.into_sink(),
        };

        Self {
            grpc_listen_addr: listen_addr,
            local_node: Some(local_node),
            peer_node_mailboxes: Default::default(),
            rpc_raft_node_change_event_receiver,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let local_node = self.local_node.take().unwrap();
        let listen_addr = self.grpc_listen_addr;

        let local_node_task = tokio::spawn(run_local_node(listen_addr, local_node));
        let peer_nodes_task = tokio::spawn(async move { self.run_peer_nodes().await });

        match future::select(local_node_task, peer_nodes_task).await {
            Either::Left((result, _)) => result.unwrap(),
            Either::Right((result, _)) => result.unwrap(),
        }
    }

    async fn run_peer_nodes(&mut self) -> anyhow::Result<()> {
        let peer_node_mailboxes = self.peer_node_mailboxes.clone().map(Either::Left);

        let mut either_stream = stream::select(
            peer_node_mailboxes,
            self.rpc_raft_node_change_event_receiver
                .clone()
                .into_stream()
                .map(Either::Right),
        );

        while let Some(either) = either_stream.next().await {
            match either {
                Either::Left((peer_node_id, message)) => {
                    self.peer_node_mailboxes
                        .send_message_to_peer_node(peer_node_id, message);

                    debug!(peer_node_id, "send message to peer node done");
                }

                Either::Right(change_event) => {
                    self.handle_node_change_event(change_event).await?;

                    info!("handle node change event done");
                }
            }
        }

        Err(anyhow::anyhow!("run_peer_nodes stop unexpected"))
    }

    #[instrument(skip(self), err)]
    async fn handle_node_change_event(&mut self, event: RpcRaftNodeEvent) -> anyhow::Result<()> {
        match event {
            RpcRaftNodeEvent::Add {
                node_id,
                uri,
                mut mailbox_sender_provider,
            } => {
                let channel = Channel::builder(uri.clone())
                    .connect_with_connector_lazy(Connector::default())
                    .tap_err(|err| error!(?err, node_id, %uri, "connect to new node failed"))?;

                let (mailbox_sender, mailbox) = flume::unbounded();

                self.peer_node_mailboxes.insert_peer_node(
                    node_id,
                    PeerNode {
                        id: node_id,
                        grpc_client: RaftClient::new(channel),
                        mailbox: mailbox.into_stream(),
                    },
                );

                mailbox_sender_provider
                    .send(mailbox_sender)
                    .await
                    .tap_err(|err| error!(?err, "provide mailbox sender failed"))?;

                info!(node_id, %uri, "add new peer node done");
            }

            RpcRaftNodeEvent::Remove { node_id } => {
                self.peer_node_mailboxes.remove_peer_node(node_id);

                info!(node_id, "remove peer node done");
            }
        }

        Ok(())
    }
}

async fn run_local_node(listen_addr: SocketAddr, local_node: LocalNode) -> anyhow::Result<()> {
    Server::builder()
        .add_service(RaftServer::new(local_node))
        .serve(listen_addr)
        .await
        .tap_err(|err| error!(%err, "local raft node grpc server stopped unexpected"))?;

    Err(anyhow::anyhow!(
        "local raft node grpc server stopped unexpected"
    ))
}

#[derive(Clone, Default)]
struct PeerNodeMailboxes {
    peer_nodes: Arc<DashMap<u64, PeerNode>>,
}

impl Stream for PeerNodeMailboxes {
    type Item = (u64, eraftpb::Message);

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.peer_nodes.is_empty() {
            return Poll::Pending;
        }

        let mut node_ids = self
            .peer_nodes
            .iter()
            .map(|item| *item.key())
            .collect::<Vec<_>>();

        // rand slice to make sure no peer_node is starved
        node_ids.shuffle(&mut rand::thread_rng());

        for node_id in node_ids {
            let mut peer_node = self.peer_nodes.get_mut(&node_id).unwrap();
            match peer_node.mailbox.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some((node_id, item))),

                _ => continue,
            }
        }

        Poll::Pending
    }
}

impl PeerNodeMailboxes {
    fn insert_peer_node(&self, node_id: u64, peer_node: PeerNode) {
        self.peer_nodes.insert(node_id, peer_node);
    }

    fn remove_peer_node(&self, node_id: u64) {
        self.peer_nodes.remove(&node_id);
    }

    fn send_message_to_peer_node(&self, node_id: u64, message: eraftpb::Message) {
        match self.peer_nodes.get(&node_id) {
            None => {
                warn!(node_id, "raft message target node not exist");
            }

            Some(peer_node) => {
                peer_node.send_message_background(message);

                debug!(node_id, "send message to peer node in background");
            }
        }
    }
}
