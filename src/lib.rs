use std::io::{self, ErrorKind};
use std::path::Path;
use std::time::Duration;

use futures_util::{try_join, TryStreamExt};
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
use crate::rpc::connect::Connector;
use crate::rpc::register::Register;
use crate::tokio_ext::TokioResultTaskExt;

mod config;
mod node;
mod reply;
mod rpc;
mod storage;
mod stream_ext;
mod tokio_ext;

fn init_log(debug_log: bool) {
    let layer = Layer::new().pretty().with_writer(io::stderr);

    let level = if debug_log {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };

    let layered = Registry::default().with(layer).with(level);

    tracing::subscriber::set_global_default(layered).unwrap();
}

pub async fn run() -> anyhow::Result<()> {
    let config = config::get_config()?;

    init_log(config.debug_log);

    info!(?config, "get config done");

    let (proposal_request_sender, proposal_request_queue) = flume::unbounded();
    let (raft_message_sender, mailbox) = flume::unbounded();
    let (get_request_sender, get_request_queue) = flume::unbounded();
    let (rpc_raft_node_change_event_sender, rpc_raft_node_change_event_receiver) =
        flume::unbounded();
    let (node_change_event_sender, node_change_event_receiver) = flume::unbounded();

    let (kv_backend, log_backend) = init_backend(&config.data).await?;

    info!("create kv backend and log backend done");

    let raft_rpc = RaftRpc::new(
        config.local_raft_listen_addr,
        raft_message_sender,
        rpc_raft_node_change_event_receiver,
    );

    let kv_rpc = KvRpc::new(proposal_request_sender, get_request_sender);

    info!("create raft rpc and kv rpc done");

    let registry_channel = Channel::builder(config.registry_uri.parse().unwrap())
        .connect_with_connector(Connector::default())
        .await
        .tap_err(|err| error!(?err, "connect to registry failed"))?;

    let mut register = Register::new(
        registry_channel,
        config.node_id,
        config.node_uri.parse().unwrap(),
        Duration::from_secs(30),
        rpc_raft_node_change_event_sender.into_sink(),
        node_change_event_sender.into_sink(),
    );

    let mut builder = NodeBuilder::default();
    builder
        .config(Config::new(config.node_id))
        .storage(log_backend)
        .kv(kv_backend)
        .mailbox(mailbox)
        .proposal_request_queue(proposal_request_queue)
        .get_request_queue(get_request_queue)
        .node_change_event_receiver(node_change_event_receiver);

    let mut node = builder.build()?;

    info!("create raft node done");

    let raft_rpc_task = tokio::spawn(async move { raft_rpc.run().await }).flatten_result();

    let kv_rpc_task =
        tokio::spawn(async move { kv_rpc.run(config.local_kv_listen_addr).await }).flatten_result();

    let register_task = tokio::spawn(async move { register.run().await }).flatten_result();

    let node_task = task::spawn_blocking(move || node.run()).flatten_result();

    if let Err(err) = try_join!(raft_rpc_task, kv_rpc_task, node_task, register_task) {
        error!(%err, "tasks stop with error");

        return Err(err);
    }

    error!("tasks stop unexpected");

    Err(anyhow::anyhow!("tasks stop unexpected"))
}

async fn init_backend(path: &Path) -> anyhow::Result<(KvBackend, LogBackend<KvBackend>)> {
    match fs::read_dir(path).await {
        Err(err) if err.kind() == ErrorKind::NotFound => create_backend(path).await,

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
                create_backend(path).await
            } else {
                let kv_path = path.join("kv");
                let kv_backend = KvBackend::from_exist(&kv_path)?;

                info!(?kv_path, "start kv backend done");

                let log_path = path.join("log");
                let log_backend = LogBackend::from_exist(&log_path, kv_backend.clone())?;

                info!(?log_path, "start log backend done");

                Ok((kv_backend, log_backend))
            }
        }
    }
}

async fn create_backend(path: &Path) -> anyhow::Result<(KvBackend, LogBackend<KvBackend>)> {
    let kv_path = path.join("kv");
    let kv_backend = KvBackend::create(&kv_path)?;

    info!(?kv_path, "create kv backend done");

    let log_path = path.join("log");
    let log_backend = LogBackend::create(&log_path, kv_backend.clone())?;

    info!(?log_path, "create log backend done");

    Ok((kv_backend, log_backend))
}
