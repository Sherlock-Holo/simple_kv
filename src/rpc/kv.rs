use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use flume::r#async::SendSink;
use flume::Sender;
use futures_util::{SinkExt, StreamExt};
use tap::TapFallible;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument, warn};

use super::pb::simple_kv::*;
use crate::reply::{GetRequestReply, ProposalRequestReply, RequestError};
use crate::rpc::pb::simple_kv::kv_server::{Kv, KvServer};
use crate::storage::key_value::{KeyValueOperation, Operation};

pub struct Rpc {
    proposal_request_sender: SendSink<'static, ProposalRequestReply>,
    get_request_sender: SendSink<'static, GetRequestReply>,
}

impl Rpc {
    pub fn new(
        proposal_request_sender: Sender<ProposalRequestReply>,
        get_request_sender: Sender<GetRequestReply>,
    ) -> Self {
        Self {
            proposal_request_sender: proposal_request_sender.into_sink(),
            get_request_sender: get_request_sender.into_sink(),
        }
    }

    pub async fn run(self, listen_addr: SocketAddr) -> anyhow::Result<()> {
        Server::builder()
            .add_service(KvServer::new(self))
            .serve(listen_addr)
            .await
            .tap_err(|err| error!(%err, "local kv node grpc server stopped unexpected"))?;

        Err(anyhow::anyhow!(
            "local kv node grpc server stopped unexpected"
        ))
    }
}

#[async_trait]
impl Kv for Rpc {
    #[instrument(skip(self, request), err)]
    async fn insert(
        &self,
        request: Request<KvInsertRequest>,
    ) -> Result<Response<KvInsertResponse>, Status> {
        let request = request.into_inner();

        let (reply, result_receiver) = ProposalRequestReply::new(KeyValueOperation::new(
            Operation::InsertOrUpdate,
            request.key,
            request.value,
        ));

        info!("create insert proposal request reply done");

        self.proposal_request_sender
            .clone()
            .send(reply)
            .await
            .map_err(|err| {
                error!(%err, "send insert proposal request failed");

                Status::failed_precondition("send insert proposal request failed")
            })?;

        info!("send insert proposal request reply done");

        match result_receiver.into_stream().next().await.transpose() {
            Err(RequestError::NotLeader(leader_id)) => {
                warn!(leader_id, "current node is not leader");

                let resp = KvInsertResponse {
                    result: Some(kv_insert_response::Result::NotLeader(KvNotLeader {
                        leader_id,
                    })),
                };

                Ok(Response::new(resp))
            }

            Err(RequestError::Other(err)) => {
                error!(%err, "insert proposal request failed");

                Err(Status::failed_precondition(
                    "insert proposal request failed",
                ))
            }

            Ok(result) => {
                if result.is_none() {
                    error!("result receiver is closed");

                    Err(Status::unavailable("result is unavailable"))
                } else {
                    info!("insert proposal request done");

                    let resp = KvInsertResponse {
                        result: Some(kv_insert_response::Result::Success(())),
                    };

                    Ok(Response::new(resp))
                }
            }
        }
    }

    #[instrument(skip(self, request), err)]
    async fn get(&self, request: Request<KvGetRequest>) -> Result<Response<KvGetResponse>, Status> {
        let request = request.into_inner();

        let (reply, result_receiver) = GetRequestReply::new(request.key);

        info!("create get request reply done");

        self.get_request_sender
            .clone()
            .send(reply)
            .await
            .map_err(|err| {
                error!(%err, "send get request failed");

                Status::failed_precondition("send get request failed")
            })?;

        info!("send get request done");

        match result_receiver.into_stream().next().await.transpose() {
            Err(RequestError::NotLeader(leader_id)) => {
                warn!(leader_id, "current node is not leader");

                let resp = KvGetResponse {
                    result: Some(kv_get_response::Result::NotLeader(KvNotLeader {
                        leader_id,
                    })),
                };

                Ok(Response::new(resp))
            }

            Err(RequestError::Other(err)) => {
                error!(%err, "get request failed");

                Err(Status::failed_precondition("get request failed"))
            }

            Ok(None) => {
                error!("result receiver is closed");

                Err(Status::unavailable("result is unavailable"))
            }

            Ok(Some(value)) => match value {
                None => {
                    warn!("key-value not exist");

                    let resp = KvGetResponse {
                        result: Some(kv_get_response::Result::Value(KvGetResponseValue {
                            value: None,
                        })),
                    };

                    Ok(Response::new(resp))
                }

                Some(value) => {
                    info!("get request done");

                    let resp = KvGetResponse {
                        result: Some(kv_get_response::Result::Value(KvGetResponseValue {
                            value: Some(value),
                        })),
                    };

                    Ok(Response::new(resp))
                }
            },
        }
    }

    #[instrument(skip(self, request), err)]
    async fn delete(
        &self,
        request: Request<KvDeleteRequest>,
    ) -> Result<Response<KvDeleteResponse>, Status> {
        let request = request.into_inner();

        let (reply, result_receiver) = ProposalRequestReply::new(KeyValueOperation::new(
            Operation::Delete,
            request.key,
            Bytes::new(),
        ));

        info!("create delete proposal request reply done");

        self.proposal_request_sender
            .clone()
            .send(reply)
            .await
            .map_err(|err| {
                error!(%err, "send delete proposal request failed");

                Status::failed_precondition("send delete proposal request failed")
            })?;

        info!("send insert proposal request reply done");

        match result_receiver.into_stream().next().await.transpose() {
            Err(RequestError::NotLeader(leader_id)) => {
                warn!(leader_id, "current node is not leader");

                let resp = KvDeleteResponse {
                    result: Some(kv_delete_response::Result::NotLeader(KvNotLeader {
                        leader_id,
                    })),
                };

                Ok(Response::new(resp))
            }

            Err(RequestError::Other(err)) => {
                error!(%err, "delete proposal request failed");

                Err(Status::failed_precondition(
                    "delete proposal request failed",
                ))
            }

            Ok(result) => {
                if result.is_none() {
                    error!("result receiver is closed");

                    Err(Status::unavailable("result is unavailable"))
                } else {
                    info!("delete proposal request done");

                    let resp = KvDeleteResponse {
                        result: Some(kv_delete_response::Result::Success(())),
                    };

                    Ok(Response::new(resp))
                }
            }
        }
    }
}
