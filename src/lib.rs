use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::Path;

use futures_util::{stream, try_join, StreamExt, TryStreamExt};
use http::Uri;
use raft::Config;
use rpc::kv::Rpc as KvRpc;
use rpc::raft::Rpc as RaftRpc;
use storage::key_value::rocksdb::RocksdbBackend as KvBackend;
use storage::log::rocksdb::RocksdbBackend as LogBackend;
use tap::TapFallible;
use tokio::{fs, task};
use tokio_stream::wrappers::ReadDirStream;
use tonic::transport::Channel;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

use crate::node::NodeBuilder;
use crate::rpc::raft::PeerNodeConfig;
use crate::storage::log::ConfigState;
use crate::tokio_ext::TokioResultTaskExt;

mod config;
// mod example;
// mod lib_example;
mod node;
mod reply;
mod rpc;
mod storage;
mod tokio_ext;

fn init_log() {
    let layer = Layer::new().pretty();

    let layered = Registry::default().with(layer).with(LevelFilter::INFO);

    tracing::subscriber::set_global_default(layered).unwrap();
}

pub async fn run() -> anyhow::Result<()> {
    init_log();

    let config = config::get_config()?;

    info!(?config, "get config done");

    let (proposal_request_sender, proposal_request_queue) = flume::unbounded();
    let (raft_message_sender, mailbox) = flume::unbounded();
    let (get_request_sender, get_request_queue) = flume::unbounded();

    let peers = config
        .peers
        .into_iter()
        .map(|peer| (peer.node_id, peer))
        .collect::<HashMap<_, _>>();

    let peers = stream::iter(peers)
        .then(|(node_id, peer_cfg)| async move {
            let uri = peer_cfg.node_raft_url.parse::<Uri>().unwrap();

            info!(node_id, %uri, "create node uri");

            let channel = Channel::builder(uri.clone()).connect().await?;

            info!(node_id, %uri, "connect channel done");

            Ok::<_, anyhow::Error>((node_id, channel))
        })
        .map_ok(|(node_id, channel)| {
            let (mailbox_sender, mailbox) = flume::unbounded();

            let peer_node_config = PeerNodeConfig {
                id: node_id,
                channel,
                mailbox,
            };

            (node_id, (mailbox_sender, peer_node_config))
        })
        .try_collect::<HashMap<_, _>>()
        .await
        .tap_err(|err| error!(%err, "collect peer info failed"))?;

    info!(?peers, "collect peer info done");

    let peer_node_configs = peers
        .values()
        .map(|(_, cfg)| cfg)
        .cloned()
        .collect::<Vec<_>>();

    let other_node_mailbox_senders = peers
        .into_iter()
        .map(|(node_id, (other_node_mailbox_sender, _))| (node_id, other_node_mailbox_sender))
        .collect::<HashMap<_, _>>();

    let mut node_ids = other_node_mailbox_senders
        .keys()
        .copied()
        .collect::<Vec<_>>();
    node_ids.push(config.node_id);

    let config_state = ConfigState {
        voters: node_ids,
        ..Default::default()
    };

    let (kv_backend, log_backend) = init_backend(&config.data, config_state).await?;

    info!("create kv backend and log backend done");

    let raft_rpc = RaftRpc::new(
        config.local_raft_listen_addr,
        raft_message_sender,
        peer_node_configs,
    );

    let kv_rpc = KvRpc::new(proposal_request_sender, get_request_sender);

    info!("create raft rpc and kv rpc done");

    let mut builder = NodeBuilder::default();
    builder
        .config(Config::new(config.node_id))
        .storage(log_backend)
        .kv(kv_backend)
        .mailbox(mailbox)
        .other_node_mailboxes(other_node_mailbox_senders)
        .proposal_request_queue(proposal_request_queue)
        .get_request_queue(get_request_queue);

    let mut node = builder.build()?;

    info!("create raft node done");

    let raft_rpc_task = tokio::spawn(async move { raft_rpc.run().await }).flatten_result();

    let kv_rpc_task =
        tokio::spawn(async move { kv_rpc.run(config.local_kv_listen_addr).await }).flatten_result();

    let node_task = task::spawn_blocking(move || node.run()).flatten_result();

    if let Err(err) = try_join!(raft_rpc_task, kv_rpc_task, node_task) {
        error!(%err, "tasks stop with error");

        return Err(err);
    }

    error!("tasks stop unexpected");

    Err(anyhow::anyhow!("tasks stop unexpected"))
}

async fn init_backend(
    path: &Path,
    config_state: ConfigState,
) -> anyhow::Result<(KvBackend, LogBackend<KvBackend>)> {
    match fs::read_dir(path).await {
        Err(err) if err.kind() == ErrorKind::NotFound => create_backend(path, config_state).await,

        Err(err) => {
            error!(%err, ?path, "read data path dir failed");

            Err(err.into())
        }

        Ok(dir) => {
            let mut dir = ReadDirStream::new(dir);
            if dir
                .try_next()
                .await
                .tap_err(|err| error!(%err, ?path, "get data path dir first entry failed"))?
                .is_none()
            {
                create_backend(path, config_state).await
            } else {
                let kv_path = path.join("kv");
                let kv_backend = KvBackend::from_exist(&kv_path)?;

                info!(?kv_path, "start kv backend done");

                let log_path = path.join("log");
                let log_backend =
                    LogBackend::from_exist(&log_path, config_state.clone(), kv_backend.clone())?;

                info!(?log_path, ?config_state, "start log backend done");

                Ok((kv_backend, log_backend))
            }
        }
    }
}

async fn create_backend(
    path: &Path,
    config_state: ConfigState,
) -> anyhow::Result<(KvBackend, LogBackend<KvBackend>)> {
    let kv_path = path.join("kv");
    let kv_backend = KvBackend::create(&kv_path)?;

    info!(?kv_path, "create kv backend done");

    let log_path = path.join("log");
    let log_backend = LogBackend::create(&log_path, config_state.clone(), kv_backend.clone())?;

    info!(?log_path, ?config_state, "create log backend done");

    Ok((kv_backend, log_backend))
}
