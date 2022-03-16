use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use http::Uri;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument};

use crate::pb::register_server::Register;
use crate::pb::*;

#[derive(Debug, Clone)]
struct Node {
    uri: Uri,
    deadline: Instant,
}

pub struct Registry {
    max_period: Duration,
    nodes: DashMap<u64, Node>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            max_period: Duration::from_secs(30),
            nodes: Default::default(),
        }
    }
}

#[async_trait]
impl Register for Registry {
    #[instrument(skip(self), err)]
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();

        let uri = req.node_uri.parse::<Uri>().map_err(|err| {
            error!(%err, "node url invalid");

            Status::invalid_argument(format!("node url {} invalid", req.node_uri))
        })?;

        debug!(node_id = req.node_id, %uri, "parse node uri done");

        let period = Duration::from_millis(req.period).min(self.max_period);

        debug!(node_id = req.node_id, ?period, "node period");

        let deadline = Instant::now() + period;

        debug!(node_id = req.node_id, ?deadline, "node deadline");

        self.nodes.insert(req.node_id, Node { uri, deadline });

        Ok(Response::new(RegisterNodeResponse {
            period: period.as_millis() as _,
        }))
    }

    #[instrument(skip(self), err)]
    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let mut out_of_date_ids = vec![];

        let now = Instant::now();

        let nodes = self
            .nodes
            .iter()
            .filter(|node| {
                if node.deadline >= now {
                    return true;
                }

                out_of_date_ids.push(*node.key());

                false
            })
            .map(|node| NodeInfo {
                node_id: *node.key(),
                node_uri: node.uri.to_string(),
            })
            .collect::<Vec<_>>();

        if !out_of_date_ids.is_empty() {
            info!(
                ?out_of_date_ids,
                "some nodes are out of date, need to be cleared"
            );

            for node_id in out_of_date_ids {
                // the out of date node may update when removing, so use remove_if not remove
                self.nodes.remove_if(&node_id, |_, node| {
                    node.deadline.elapsed() >= self.max_period
                });

                debug!(node_id, "out of date node is cleaed");
            }
        }

        debug!(?nodes, "list nodes done");

        Ok(Response::new(ListNodesResponse { node_list: nodes }))
    }
}

#[cfg(test)]
mod tests {
    use tokio::time;
    use tonic::IntoRequest;

    use super::*;

    #[tokio::test]
    async fn test_register_node() {
        let registry = Registry::default();

        let start = Instant::now();

        registry
            .register_node(
                RegisterNodeRequest {
                    node_id: 1,
                    period: 1000,
                    node_uri: "http://127.0.0.1".to_string(),
                }
                .into_request(),
            )
            .await
            .unwrap();

        let node = registry.nodes.get(&1).unwrap();
        assert_eq!(*node.key(), 1);
        assert_eq!(node.uri, Uri::from_static("http://127.0.0.1"));

        assert!(node.deadline - Duration::from_millis(1000) >= start);
    }

    #[tokio::test]
    async fn test_list_nodes() {
        let registry = Registry::default();

        registry
            .register_node(
                RegisterNodeRequest {
                    node_id: 1,
                    period: 1000,
                    node_uri: "http://127.0.0.1".to_string(),
                }
                .into_request(),
            )
            .await
            .unwrap();

        let resp = registry
            .list_nodes(ListNodesRequest {}.into_request())
            .await
            .unwrap();
        let node = &resp.into_inner().node_list[0];

        assert_eq!(node.node_id, 1);
        assert_eq!(node.node_uri, "http://127.0.0.1/".to_string());
    }

    #[tokio::test]
    async fn test_list_nodes_timeout() {
        let registry = Registry::default();

        registry
            .register_node(
                RegisterNodeRequest {
                    node_id: 1,
                    period: 10,
                    node_uri: "http://127.0.0.1".to_string(),
                }
                .into_request(),
            )
            .await
            .unwrap();

        time::sleep(Duration::from_millis(100)).await;

        let resp = registry
            .list_nodes(ListNodesRequest {}.into_request())
            .await
            .unwrap();

        assert!(resp.into_inner().node_list.is_empty());
    }
}
