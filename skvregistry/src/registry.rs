use async_trait::async_trait;
use dashmap::DashMap;
use http::Uri;
use tonic::{Request, Response, Status};
use tracing::{debug, error, instrument};

use crate::pb::register_server::Register;
use crate::pb::*;

#[derive(Debug, Clone)]
struct Node {
    raft_uri: Uri,
    kv_uri: Uri,
}

#[derive(Default)]
pub struct Registry {
    nodes: DashMap<u64, Node>,
}

#[async_trait]
impl Register for Registry {
    #[instrument(skip(self), err)]
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();

        let raft_uri = req.node_uri.parse::<Uri>().map_err(|err| {
            error!(%err, "node uri invalid");

            Status::invalid_argument(format!("node uri {} invalid", req.node_uri))
        })?;

        let kv_uri = req.kv_uri.parse::<Uri>().map_err(|err| {
            error!(?err, "kv uri invalid");

            Status::invalid_argument(format!("kv uri {} invalid", req.kv_uri))
        })?;

        debug!(node_id = req.node_id, %raft_uri, "parse node uri done");

        self.nodes.insert(req.node_id, Node { raft_uri, kv_uri });

        Ok(Response::new(RegisterNodeResponse {}))
    }

    #[instrument(skip(self), err)]
    async fn list_nodes(
        &self,
        request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let req = request.into_inner();

        let nodes = self
            .nodes
            .iter()
            // ignore self node
            .filter(|node| *node.key() != req.self_node_id)
            .map(|node| NodeInfo {
                node_id: *node.key(),
                node_uri: node.raft_uri.to_string(),
                kv_uri: node.kv_uri.to_string(),
            })
            .collect::<Vec<_>>();

        debug!(?nodes, "list nodes done");

        Ok(Response::new(ListNodesResponse { node_list: nodes }))
    }

    #[instrument(skip(self), err)]
    async fn leave_node(
        &self,
        request: Request<LeaveNodeRequest>,
    ) -> Result<Response<LeaveNodeResponse>, Status> {
        let req = request.into_inner();

        if self.nodes.remove(&req.node_id).is_some() {
            debug!(node_id = req.node_id, "leave node done");

            Ok(Response::new(LeaveNodeResponse {}))
        } else {
            error!(node_id = req.node_id, "leave node failed, node not found");

            Err(Status::not_found(format!("node {} not found", req.node_id)))
        }
    }
}

#[cfg(test)]
mod tests {
    use tonic::IntoRequest;

    use super::*;

    #[tokio::test]
    async fn test_register_node() {
        let registry = Registry::default();

        registry
            .register_node(
                RegisterNodeRequest {
                    node_id: 1,
                    node_uri: "http://127.0.0.1".to_string(),
                    kv_uri: "http://127.0.0.1".to_string(),
                }
                .into_request(),
            )
            .await
            .unwrap();

        let node = registry.nodes.get(&1).unwrap();
        assert_eq!(*node.key(), 1);
        assert_eq!(node.raft_uri, Uri::from_static("http://127.0.0.1"));
        assert_eq!(node.kv_uri, Uri::from_static("http://127.0.0.1"));
    }

    #[tokio::test]
    async fn test_list_nodes() {
        let registry = Registry::default();

        registry
            .register_node(
                RegisterNodeRequest {
                    node_id: 1,
                    node_uri: "http://127.0.0.1".to_string(),
                    kv_uri: "http://127.0.0.1".to_string(),
                }
                .into_request(),
            )
            .await
            .unwrap();

        let resp = registry
            .list_nodes(ListNodesRequest { self_node_id: 100 }.into_request())
            .await
            .unwrap();
        let node = &resp.into_inner().node_list[0];

        assert_eq!(node.node_id, 1);
        assert_eq!(node.node_uri, "http://127.0.0.1/");
        assert_eq!(node.kv_uri, "http://127.0.0.1/");
    }

    #[tokio::test]
    async fn test_list_nodes_ignore_self() {
        let registry = Registry::default();

        registry
            .register_node(
                RegisterNodeRequest {
                    node_id: 1,
                    node_uri: "http://127.0.0.1".to_string(),
                    kv_uri: "http://127.0.0.1".to_string(),
                }
                .into_request(),
            )
            .await
            .unwrap();

        let resp = registry
            .list_nodes(ListNodesRequest { self_node_id: 1 }.into_request())
            .await
            .unwrap();

        assert!(resp.into_inner().node_list.is_empty());
    }
}
