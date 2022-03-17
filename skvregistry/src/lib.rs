use std::io;
use std::net::SocketAddr;

use clap::{ColorChoice, Parser};
use pb::register_server::RegisterServer;
use tap::TapFallible;
use tonic::transport::Server;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;

use crate::registry::Registry;

mod registry;

mod pb {
    tonic::include_proto!("skv_register");
}

fn init_log(debug_log: bool) {
    let layer = Layer::new().pretty().with_writer(io::stderr);

    let level = if debug_log {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };

    let layered = tracing_subscriber::Registry::default()
        .with(layer)
        .with(level);

    tracing::subscriber::set_global_default(layered).unwrap();
}

#[derive(Debug, Parser)]
#[clap(color = ColorChoice::Always)]
struct Args {
    #[clap(short, long)]
    /// debug log level
    debug: bool,

    #[clap(short, long)]
    listen_addr: SocketAddr,
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    init_log(args.debug);

    info!(listen_addr = %args.listen_addr, "start listen registry");

    Server::builder()
        .add_service(RegisterServer::new(Registry::default()))
        .serve(args.listen_addr)
        .await
        .tap_err(|err| error!(?err, "registry failed"))?;

    Err(anyhow::anyhow!("registry stop unexpected"))
}
