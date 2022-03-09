use std::collections::HashMap;
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::BytesMut;
use flume::r#async::{RecvStream, SendSink};
use flume::{Receiver, Sender};
use futures_util::future::Either;
use futures_util::{future, stream, SinkExt, StreamExt};
use prost07::Message;
use raft::eraftpb;
use tap::TapFallible;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};
use tracing::{error, info, info_span, instrument, warn, Instrument};

use super::pb::*;
use crate::rpc::pb::raft_client::RaftClient;
use crate::rpc::pb::raft_server::{Raft, RaftServer};

#[derive(Clone, Debug)]
pub struct PeerNodeConfig {
    pub id: u64,
    pub channel: Channel,
    pub mailbox: Receiver<eraftpb::Message>,
}

struct PeerNode {
    grpc_client: RaftClient<Channel>,
    mailbox: RecvStream<'static, eraftpb::Message>,
}

impl PeerNode {
    fn send_message_background(&self, message: eraftpb::Message) -> JoinHandle<anyhow::Result<()>> {
        let mut grpc_client = self.grpc_client.clone();

        tokio::spawn(
            async move {
                let mut buf = BytesMut::with_capacity(message.encoded_len());

                message
                    .encode(&mut buf)
                    .tap_err(|err| error!(%err, "encode raft message failed"))?;

                let message = buf.freeze();

                info!("encode raft message done");

                grpc_client
                    .send_message(RaftMessageRequest { message })
                    .await
                    .tap_err(|err| error!(%err, "send raft message to peer node failed"))?;

                info!("send raft message to peer node done");

                Ok::<_, anyhow::Error>(())
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

        info!("decode raft message done");

        self.raft_message_sender
            .clone()
            .send(message)
            .await
            .map_err(|err| {
                error!(%err, "send other node raft message to local node failed");

                Status::failed_precondition("send other node raft message to local node failed")
            })?;

        info!("send other node raft message to local node done");

        Ok(Response::new(RaftMessageResponse {}))
    }
}

pub struct Rpc {
    grpc_listen_addr: SocketAddr,
    local_node: Option<LocalNode>,
    peer_nodes: HashMap<u64, PeerNode>,
}

impl Rpc {
    pub fn new(
        listen_addr: SocketAddr,
        raft_message_sender: Sender<eraftpb::Message>,
        peer_node_configs: Vec<PeerNodeConfig>,
    ) -> Self {
        let local_node = LocalNode {
            raft_message_sender: raft_message_sender.into_sink(),
        };

        let peer_nodes = peer_node_configs
            .into_iter()
            .map(|config| {
                (
                    config.id,
                    PeerNode {
                        grpc_client: RaftClient::new(config.channel),
                        mailbox: config.mailbox.into_stream(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        Self {
            grpc_listen_addr: listen_addr,
            local_node: Some(local_node),
            peer_nodes,
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
        let peer_node_mailboxes = self
            .peer_nodes
            .iter()
            .map(|(peer_node_id, peer_node)| (*peer_node_id, peer_node.mailbox.clone()))
            .map(|(peer_node_id, peer_node_mailbox)| {
                peer_node_mailbox.map(move |message| (peer_node_id, message))
            })
            .collect::<Vec<_>>();

        let mut peer_node_mailboxes = stream::select_all(peer_node_mailboxes);

        while let Some((peer_node_id, message)) = peer_node_mailboxes.next().await {
            match self.peer_nodes.get(&peer_node_id) {
                None => {
                    warn!(peer_node_id, "raft message target node not exist");

                    continue;
                }

                Some(peer_node) => {
                    peer_node.send_message_background(message);

                    info!(peer_node_id, "send message to peer node in background");
                }
            }
        }

        error!("get raft message from peer node mailboxes stopped unexpected");

        Err(anyhow::anyhow!(
            "get raft message from peer node mailboxes stopped unexpected"
        ))
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
