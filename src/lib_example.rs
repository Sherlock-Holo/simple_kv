use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use raft::Config;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

use crate::example::node::{Node, ProposalQueue};
use crate::example::proposal_request::ProposalRequest;

fn init_log() {
    let layer = Layer::new().pretty();

    let layered = Registry::default().with(layer).with(LevelFilter::INFO);

    tracing::subscriber::set_global_default(layered).unwrap();
}

pub fn run() {
    init_log();

    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    let (tx3, rx3) = mpsc::channel();

    let proposal_queue = Arc::new(Mutex::new(ProposalQueue::new()));

    let cfg1 = Config::new(1);

    let node1 = Node::new(
        &cfg1,
        rx1,
        HashMap::from([(2, tx2.clone()), (3, tx3.clone())]),
        proposal_queue.clone(),
    )
    .unwrap();
    let core1 = node1.get_inner_storage();

    let cfg2 = Config::new(2);

    let node2 = Node::new(
        &cfg2,
        rx2,
        HashMap::from([(1, tx1.clone()), (3, tx3)]),
        proposal_queue.clone(),
    )
    .unwrap();
    let core2 = node2.get_inner_storage();

    let cfg3 = Config::new(3);

    let node3 = Node::new(
        &cfg3,
        rx3,
        HashMap::from([(2, tx2), (1, tx1)]),
        proposal_queue.clone(),
    )
    .unwrap();
    let core3 = node3.get_inner_storage();

    let nodes = vec![node1, node2, node3];

    let mut threads = vec![];

    for mut node in nodes {
        let handle = thread::spawn(move || node.run());

        threads.push(handle);
    }

    let proposal_request = ProposalRequest::CreateOrUpdate(10, "123".to_string());
    let (result_tx, result_rx) = mpsc::channel();

    proposal_queue
        .lock()
        .unwrap()
        .push_back((Some(proposal_request), result_tx));

    result_rx.recv().unwrap().unwrap();

    info!("proposal request done");

    thread::sleep(Duration::from_secs(1));

    let core1 = core1.read().unwrap();
    core1.map.iter().for_each(|(key, value)| {
        info!("core1 key {}, value {}", key, value);
    });

    let core2 = core2.read().unwrap();
    core2.map.iter().for_each(|(key, value)| {
        info!("core2 key {}, value {}", key, value);
    });

    let core3 = core3.read().unwrap();
    core3.map.iter().for_each(|(key, value)| {
        info!("core3 key {}, value {}", key, value);
    });
}
