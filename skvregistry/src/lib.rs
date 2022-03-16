use std::io;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;

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
