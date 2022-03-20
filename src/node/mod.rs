use std::collections::{HashMap, VecDeque};
#[cfg(test)]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::Context;
use bincode::{DefaultOptions, Options};
use flume::select::SelectError;
use flume::{Receiver, Sender};
use message::ReceiveMessage;
use prost07::Message as _;
use protobuf::error::WireError;
use protobuf::ProtobufError;
use raft::{eraftpb, Config, RawNode, StateRole, Storage};
use rayon::prelude::*;
use tap::TapFallible;
use tracing::{debug, error, info, instrument, warn};

use crate::node::message::ContinueSelector;
use crate::reply::{GetRequestReply, ProposalRequestReply, RequestError};
use crate::rpc::register::NodeChangeEvent;
use crate::storage::key_value::{KeyValueBackend, KeyValueOperation};
use crate::storage::log::{ConfigState, Entry, EntryType, LogBackend, Snapshot};

mod message;

pub struct NodeBuilder<KV, ST> {
    config: Option<Config>,
    tick_interval: Duration,
    storage: Option<ST>,
    kv: Option<KV>,
    mailbox: Option<Receiver<eraftpb::Message>>,
    proposal_request_queue: Option<Receiver<ProposalRequestReply>>,
    get_request_queue: Option<Receiver<GetRequestReply>>,
    node_change_event_receiver: Option<Receiver<NodeChangeEvent>>,

    #[cfg(test)]
    stop_signal: Option<Arc<AtomicBool>>,
}

impl<KV, ST> Default for NodeBuilder<KV, ST> {
    fn default() -> Self {
        Self {
            config: None,
            tick_interval: Duration::from_millis(100),
            storage: None,
            kv: None,
            mailbox: None,
            proposal_request_queue: None,
            get_request_queue: None,
            node_change_event_receiver: None,

            #[cfg(test)]
            stop_signal: None,
        }
    }
}

impl<KV, ST> NodeBuilder<KV, ST> {
    pub fn config(&mut self, config: Config) -> &mut Self {
        self.config.replace(config);

        self
    }

    pub fn storage(&mut self, storage: ST) -> &mut Self {
        self.storage.replace(storage);

        self
    }

    pub fn kv(&mut self, kv: KV) -> &mut Self {
        self.kv.replace(kv);

        self
    }

    pub fn mailbox(&mut self, mailbox: Receiver<eraftpb::Message>) -> &mut Self {
        self.mailbox.replace(mailbox);

        self
    }

    pub fn proposal_request_queue(
        &mut self,
        proposal_request_queue: Receiver<ProposalRequestReply>,
    ) -> &mut Self {
        self.proposal_request_queue.replace(proposal_request_queue);

        self
    }

    pub fn get_request_queue(&mut self, get_request_queue: Receiver<GetRequestReply>) -> &mut Self {
        self.get_request_queue.replace(get_request_queue);

        self
    }

    pub fn node_change_event_receiver(
        &mut self,
        node_change_event_receiver: Receiver<NodeChangeEvent>,
    ) -> &mut Self {
        self.node_change_event_receiver
            .replace(node_change_event_receiver);

        self
    }

    #[cfg(test)]
    fn stop_signal(&mut self, stop_signal: Arc<AtomicBool>) -> &mut Self {
        self.stop_signal.replace(stop_signal);

        self
    }
}

impl<KV, ST> NodeBuilder<KV, ST>
where
    KV: KeyValueBackend,
    ST: Storage + LogBackend,
{
    pub fn build(mut self) -> anyhow::Result<Node<KV, ST>> {
        use anyhow::anyhow;

        let config = self
            .config
            .take()
            .with_context(|| anyhow!("config is not set"))?;

        let tick_interval = self.tick_interval;

        let storage = self
            .storage
            .take()
            .with_context(|| anyhow!("storage is not set"))?;
        let kv = self.kv.take().with_context(|| anyhow!("kv is not set"))?;

        let mailbox = self
            .mailbox
            .take()
            .with_context(|| anyhow!("mailbox is not set"))?;

        let proposal_request_queue = self
            .proposal_request_queue
            .take()
            .with_context(|| anyhow!("proposal_request_queue is not set"))?;

        let get_request_queue = self
            .get_request_queue
            .take()
            .with_context(|| anyhow!("get_request_queue is not set"))?;

        let node_change_event_receiver = self
            .node_change_event_receiver
            .take()
            .with_context(|| anyhow!("node_change_event_receiver is not set"))?;

        let raw_node = RawNode::with_default_logger(&config, storage)
            .tap_err(|err| error!(%err, "init raw node failed"))?;

        info!("init raw node done");

        Ok(Node {
            mailbox,
            other_node_mailboxes: Default::default(),
            raw_node,
            tick_interval,
            key_value_backend: kv,
            proposal_request_queue,
            proposal_request_reply_queue: Default::default(),
            get_request_queue,
            node_change_event_receiver,

            #[cfg(test)]
            stop_signal: self.stop_signal.take().unwrap(),
        })
    }
}

pub struct Node<KV, ST>
where
    KV: KeyValueBackend,
    ST: Storage + LogBackend,
{
    mailbox: Receiver<eraftpb::Message>,
    other_node_mailboxes: HashMap<u64, Sender<eraftpb::Message>>,

    raw_node: RawNode<ST>,
    tick_interval: Duration,

    key_value_backend: KV,

    /// when leader receives a proposal request, it will send message to other nodes, and when most
    /// nodes append this entry which represent the proposal request, this entry will be committed
    ///
    /// when leader commits a proposal request entry, it only represent one proposal request, so we
    /// can get the result sender from the proposal request reply queue to reply the client
    proposal_request_queue: Receiver<ProposalRequestReply>,
    proposal_request_reply_queue: VecDeque<ProposalRequestReply>,

    get_request_queue: Receiver<GetRequestReply>,

    node_change_event_receiver: Receiver<NodeChangeEvent>,

    #[cfg(test)]
    /// use for unit test to stop the node
    stop_signal: Arc<AtomicBool>,
}

impl<KV, ST> Node<KV, ST>
where
    ST: Storage + LogBackend + Send + Sync,
    ST::Error: Send + Sync + 'static,
    KV: KeyValueBackend + Send + Sync,
{
    pub fn run(&mut self) -> anyhow::Result<()> {
        self.wait_other_nodes_online()?;

        info!("wait other nodes online done");

        info!("start node infinite running loop");

        let mut instant = Instant::now();

        loop {
            self.run_a_cycle(&mut instant)?;

            #[cfg(test)]
            {
                if self.stop_signal.load(Ordering::Acquire) {
                    return Ok(());
                }
            }
        }
    }

    fn wait_other_nodes_online(&mut self) -> anyhow::Result<()> {
        while self.other_node_mailboxes.is_empty() {
            let first_event = match self.node_change_event_receiver.recv() {
                Err(_) => {
                    error!("node change event receiver closed");

                    return Err(anyhow::anyhow!("node change event receiver closed"));
                }

                Ok(event) => event,
            };

            let mut events = self
                .node_change_event_receiver
                .try_iter()
                .collect::<VecDeque<_>>();
            events.push_front(first_event);
            let events = events.into_iter().collect();

            info!("receive node change event to init node done");

            self.handle_node_change_events(events)?;

            info!("add nodes done");
        }

        let mut nodes = self
            .other_node_mailboxes
            .keys()
            .copied()
            .collect::<Vec<_>>();
        nodes.push(self.raw_node.raft.id);

        info!(?nodes, "get all node ids");

        let config_state = ConfigState {
            voters: nodes,
            ..Default::default()
        };

        self.raw_node.mut_store().apply_config_state(config_state)?;

        info!("apply config state done");

        Ok(())
    }

    #[instrument(skip(self), err)]
    fn run_a_cycle(&mut self, instant: &mut Instant) -> anyhow::Result<()> {
        let node_id = self.raw_node.raft.id;

        if self.mailbox.is_disconnected() {
            error!(node_id, "mailbox is disconnected");

            return Err(anyhow::anyhow!("mailbox is disconnected"));
        }

        if self.get_request_queue.is_disconnected() {
            error!(node_id, "get request queue is disconnected");

            return Err(anyhow::anyhow!("get request queue is disconnected"));
        }

        if self.proposal_request_queue.is_disconnected() {
            error!(node_id, "proposal request queue is disconnected");

            return Err(anyhow::anyhow!("proposal request queue is disconnected"));
        }

        match self.receive_all_messages() {
            Err(_) => {
                let elapsed = instant.elapsed();
                if elapsed >= self.tick_interval {
                    debug!(
                        node_id,
                        ?elapsed,
                        tick_interval = ?self.tick_interval,
                        "elapsed >= tick_interval, tick the raw node"
                    );

                    self.raw_node.tick();

                    *instant = Instant::now();
                }

                if self.is_leader() {
                    debug!(node_id, "node is leader");
                }
            }
            Ok(all_messages) => {
                let (raft_msgs, get_request_msgs, proposal_request_msg, node_change_events) =
                    ReceiveMessage::split(all_messages);

                for msg in raft_msgs {
                    let from = msg.from;

                    info!(node_id, from, "get other node message done");

                    self.raw_node.step(msg).tap_err(|err| {
                        error!(node_id, %err, from, "step other node message failed");
                    })?;

                    info!(node_id, from, "step other node message done");
                }

                let elapsed = instant.elapsed();
                if elapsed >= self.tick_interval {
                    debug!(
                        node_id,
                        ?elapsed,
                        tick_interval = ?self.tick_interval,
                        "elapsed >= tick_interval, tick the raw node"
                    );

                    self.raw_node.tick();

                    *instant = Instant::now();
                }

                self.handle_node_change_events(node_change_events)?;

                debug!("handle node change events done");

                let leader_id = self.raw_node.raft.leader_id;

                if self.is_leader() {
                    debug!(node_id, "node is leader");
                }

                self.handle_get_requests(node_id, leader_id, get_request_msgs)?;

                info!("handle get requests done");

                self.handle_proposal_requests(node_id, leader_id, proposal_request_msg)?;

                info!("handle proposal requests done");
            }
        }

        if !self.raw_node.has_ready() {
            debug!(node_id, "node has no ready event");

            return Ok(());
        }

        debug!(node_id, "node has ready event");

        self.handle_ready()?;

        debug!(node_id, "handle ready event done");

        Ok(())
    }

    #[instrument(skip(self))]
    fn receive_all_messages(&mut self) -> Result<Vec<ReceiveMessage>, SelectError> {
        ContinueSelector::new(
            &self.mailbox,
            &self.get_request_queue,
            &self.proposal_request_queue,
            &self.node_change_event_receiver,
        )
        .wait_timeout(self.tick_interval)
    }

    #[instrument(skip(self), err)]
    fn handle_ready(&mut self) -> anyhow::Result<()> {
        let mut ready = self.raw_node.ready();

        self.send_message_to_other_nodes(ready.take_messages())?;

        info!("send message to other nodes done");

        let snapshot = ready.snapshot();
        if snapshot.metadata.is_some() && !snapshot.data.is_empty() {
            debug!("ready snapshot is not empty snapshot, need to apply");

            let snapshot = Snapshot::try_from(snapshot)
                .tap_err(|err| error!(%err, "convert rpc snapshot to log snapshot failed"))?;

            info!("convert rpc snapshot to log snapshot done");

            self.raw_node.mut_store().apply_snapshot(snapshot)?;

            info!("apply snapshot done");
        }

        for commit_entry in ready.take_committed_entries() {
            self.apply_commit_entry(commit_entry)?;

            info!("apply commit entry done");
        }

        info!("apply all commit entries done");

        let entries = ready
            .take_entries()
            .into_iter()
            .map(Entry::try_from)
            .collect::<Result<Vec<_>, _>>()
            .tap_err(|err| error!(%err, "convert rpc entries to log entries failed"))?;

        info!("convert rcp entries to log entries done");

        self.raw_node.mut_store().append_entries(entries)?;

        info!("apply entries done");

        if let Some(hard_state) = ready.hs() {
            self.raw_node
                .mut_store()
                .apply_hard_state(hard_state.clone().into())?;

            info!(?hard_state, "apply hard state done");
        }

        self.send_message_to_other_nodes(ready.take_persisted_messages())?;

        info!("send persisted messages to other nodes done");

        let mut light_ready = self.raw_node.advance(ready);

        info!("advance ready done");

        if let Some(commit_index) = light_ready.commit_index() {
            self.raw_node.mut_store().apply_commit_index(commit_index)?;

            info!(commit_index, "apply light ready commit index done");
        }

        self.send_message_to_other_nodes(light_ready.take_messages())?;

        info!("send light ready message to other nodes done");

        for commit_entry in light_ready.take_committed_entries() {
            self.apply_commit_entry(commit_entry)?;

            info!("apply light ready commit entry done");
        }

        info!("apply all light ready commit entries done");

        self.raw_node.advance_apply();

        info!("raw node advance apply done");

        Ok(())
    }

    #[instrument(skip(self, key_value_operation), err)]
    fn propose_key_value_operation(
        &mut self,
        key_value_operation: KeyValueOperation,
    ) -> anyhow::Result<()> {
        let key_value_operation = encoding()
            .serialize(&key_value_operation)
            .tap_err(|err| error!(%err, "serialize key value operation failed"))?;

        info!("serialize key value operation done");

        self.raw_node
            .propose(vec![], key_value_operation)
            .tap_err(|err| error!(%err, "raw node propose key value operation failed"))?;

        Ok(())
    }

    fn handle_node_change_events(&mut self, events: Vec<NodeChangeEvent>) -> anyhow::Result<()> {
        let mut config_change = eraftpb::ConfChangeV2 {
            transition: eraftpb::ConfChangeTransition::Auto as _,
            ..Default::default()
        };

        for event in events {
            info!(?event, "handle node change event");

            match event {
                NodeChangeEvent::Add {
                    node_id,
                    mut mailbox_sender,
                } => {
                    self.other_node_mailboxes.insert(
                        node_id,
                        mailbox_sender.take().expect("mailbox sender is none"),
                    );

                    info!(node_id, "add node to other node mailboxes done");

                    config_change.changes.push(eraftpb::ConfChangeSingle {
                        change_type: eraftpb::ConfChangeType::AddNode as _,
                        node_id,
                    });
                }

                NodeChangeEvent::Remove { node_id } => {
                    self.other_node_mailboxes.remove(&node_id);

                    info!(node_id, "remove node from other node mailboxes done");

                    config_change.changes.push(eraftpb::ConfChangeSingle {
                        change_type: eraftpb::ConfChangeType::RemoveNode as _,
                        node_id,
                    });
                }
            }
        }

        // only leader can propose a conf change
        if self.is_leader() {
            self.raw_node
                .propose_conf_change(vec![], config_change)
                .tap_err(|err| error!(?err, "propose config change failed"))?;
        }

        Ok(())
    }

    #[instrument(skip(self, reply), err)]
    fn handle_proposal_requests(
        &mut self,
        node_id: u64,
        mut leader_id: u64,
        reply: Option<ProposalRequestReply>,
    ) -> anyhow::Result<()> {
        if self.is_leader() {
            if let Some(mut reply) = reply {
                debug!(node_id, "node is raft leader, handle proposal request");

                let key_value_operation = reply.handle();

                self.propose_key_value_operation(key_value_operation)?;

                info!(node_id, "propose key value operation done");

                self.proposal_request_reply_queue.push_back(reply);
            }

            return Ok(());
        }

        // when leader_id = 0, means no leader is elected
        if leader_id == 0 {
            // set leader_id to node_id to make sure client retry
            leader_id = node_id;
        }

        // follower node can't handle proposal request
        if let Some(reply) = reply {
            warn!(
                node_id,
                leader_id, "node is not raff leader, reject proposal request"
            );

            reply.reply_err(RequestError::NotLeader(leader_id));
        }

        for reply in self.proposal_request_queue.try_iter() {
            warn!(
                node_id,
                leader_id, "node is not raff leader, reject proposal request"
            );

            reply.reply_err(RequestError::NotLeader(leader_id));
        }

        // all handling proposal request need to be dropped
        self.proposal_request_reply_queue
            .drain(..)
            .for_each(|reply| {
                warn!(
                    node_id,
                    leader_id, "node is not raff leader, reject proposal request"
                );

                reply.reply_err(RequestError::NotLeader(leader_id));
            });

        Ok(())
    }

    #[instrument(skip(self, messages), err)]
    fn send_message_to_other_nodes(
        &mut self,
        messages: Vec<eraftpb::Message>,
    ) -> anyhow::Result<()> {
        for msg in messages {
            let to = msg.to;

            debug!(to, "message start send to node");

            match self.other_node_mailboxes.get(&to) {
                None => {
                    error!(to, "node not exist");

                    return Err(raft::Error::NotExists { id: to, set: "" }.into());
                }

                Some(mailbox) => {
                    mailbox
                        .send(msg)
                        .tap_err(|err| error!(%err, to, "send message to node failed"))?;
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, commit_entry), err)]
    fn apply_commit_entry(&mut self, commit_entry: eraftpb::Entry) -> anyhow::Result<()> {
        if commit_entry.data.is_empty() {
            debug!("commit entry data is empty, skip it");

            return Ok(());
        }

        let commit_entry = Entry::try_from(commit_entry)
            .tap_err(|err| error!(%err, "convert rpc entry type to log entry failed"))?;

        info!("convert rpc entry type to log entry done");

        let conf_state =
            match commit_entry.entry_type {
                EntryType::EntryNormal => {
                    let key_value_operation: KeyValueOperation = encoding()
                        .deserialize(&commit_entry.data)
                        .tap_err(|err| error!(%err, "deserialize entry data failed"))?;

                    self.key_value_backend
                        .apply_key_value_operation(vec![key_value_operation])?;

                    info!("apply key value operation done");

                    // when leader apply the commit entry, means the proposal request is accepted
                    // and committed, now can reply the client
                    if self.is_leader() {
                        if let Some(reply) = self.proposal_request_reply_queue.pop_front() {
                            debug!("a proposal request can reply now");

                            reply.reply();

                            info!("reply proposal request done");
                        }
                    }

                    return Ok(());
                }

                EntryType::EntryConfChange => {
                    let conf_change = eraftpb::ConfChange::decode(commit_entry.data.as_ref())
                        .tap_err(|err| error!(%err, "decode rpc config change failed"))
                        .map_err(|_| {
                            raft::Error::CodecError(ProtobufError::WireError(WireError::Other))
                        })?;

                    info!(?conf_change, "decode rpc config change done");

                    let conf_state = self.raw_node.apply_conf_change(&conf_change).tap_err(
                        |err| error!(%err, ?conf_change, "apply rpc config change failed"),
                    )?;

                    info!(?conf_change, ?conf_state, "apply rpc config change done");

                    conf_state
                }

                EntryType::EntryConfChangeV2 => {
                    let conf_change = eraftpb::ConfChangeV2::decode(commit_entry.data.as_ref())
                        .tap_err(|err| error!(%err, "decode rpc config change failed"))
                        .map_err(|_| {
                            raft::Error::CodecError(ProtobufError::WireError(WireError::Other))
                        })?;

                    info!(?conf_change, "decode rpc config change done");

                    let conf_state = self.raw_node.apply_conf_change(&conf_change).tap_err(
                        |err| error!(%err, ?conf_change, "apply rpc config change failed"),
                    )?;

                    info!(?conf_change, ?conf_state, "apply rpc config change done");

                    conf_state
                }
            };

        let config_state: ConfigState = conf_state.into();

        info!(
            ?config_state,
            "convert rpc config state to log config state done"
        );

        self.raw_node.mut_store().apply_config_state(config_state)?;

        info!("apply config state done");

        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.raw_node.raft.state == StateRole::Leader
    }

    #[instrument(skip(self, reply))]
    fn handle_get(&self, reply: GetRequestReply) {
        match self.key_value_backend.get(reply.key()) {
            Err(err) => {
                reply.reply_err(RequestError::Other(anyhow::Error::from(err)));
            }
            Ok(value) => {
                debug!("get value done");

                reply.reply(value);
            }
        }
    }

    #[instrument(skip(self, get_request_messages), err)]
    fn handle_get_requests(
        &mut self,
        node_id: u64,
        mut leader_id: u64,
        get_request_messages: Vec<GetRequestReply>,
    ) -> anyhow::Result<()> {
        if self.is_leader() {
            get_request_messages.into_par_iter().for_each(|reply| {
                self.handle_get(reply);
            });

            info!(node_id, "handle get request done");
        } else {
            // // set leader_id to node_id to make sure client retry
            if leader_id == 0 {
                leader_id = node_id;
            }

            // follower node data may not be latest, can't handle the get request
            get_request_messages.into_par_iter().for_each(|reply| {
                warn!(
                    node_id,
                    leader_id, "reject get request because node is not leader"
                );
                reply.reply_err(RequestError::NotLeader(leader_id))
            });
        }

        Ok(())
    }
}

fn encoding() -> impl Options + Copy {
    DefaultOptions::new()
        .with_big_endian()
        .with_varint_encoding()
}

#[cfg(test)]
mod tests {
    use std::{env, mem, thread};

    use tempfile::TempDir;

    use super::*;
    use crate::storage::key_value::rocksdb::RocksdbBackend as KvBackend;
    use crate::storage::key_value::Operation;
    use crate::storage::log::rocksdb::RocksdbBackend as LogBackend;
    use crate::storage::log::LogBackend as _;

    #[test]
    fn full_test() {
        // crate::init_log(true);

        let tmp_dir1 = TempDir::new_in(env::temp_dir()).unwrap();
        let log_path1 = tmp_dir1.path().join("log1");
        let kv_path1 = tmp_dir1.path().join("kv1");

        let tmp_dir2 = TempDir::new_in(env::temp_dir()).unwrap();
        let log_path2 = tmp_dir2.path().join("log2");
        let kv_path2 = tmp_dir2.path().join("kv2");

        let tmp_dir3 = TempDir::new_in(env::temp_dir()).unwrap();
        let log_path3 = tmp_dir3.path().join("log3");
        let kv_path3 = tmp_dir3.path().join("kv3");

        let kv_backend1 = KvBackend::create(&kv_path1).unwrap();

        let log_backend1 = LogBackend::create(&log_path1, kv_backend1.clone()).unwrap();

        let kv_backend2 = KvBackend::create(&kv_path2).unwrap();

        let log_backend2 = LogBackend::create(&log_path2, kv_backend2.clone()).unwrap();

        let kv_backend3 = KvBackend::create(&kv_path3).unwrap();

        let log_backend3 = LogBackend::create(&log_path3, kv_backend3.clone()).unwrap();

        let mut backends = HashMap::from([
            (1, (log_backend1, kv_backend1)),
            (2, (log_backend2, kv_backend2)),
            (3, (log_backend3, kv_backend3)),
        ]);

        // to skip the wait other nodes online
        for (log_backend, _) in backends.values_mut() {
            log_backend
                .apply_config_state(ConfigState {
                    voters: vec![1, 2, 3],
                    ..Default::default()
                })
                .unwrap();
        }

        let mut mailbox_senders = HashMap::new();
        let mut mailboxes = HashMap::new();
        let mut proposal_request_senders = HashMap::new();
        let mut proposal_request_queues = HashMap::new();
        let mut get_request_senders = HashMap::new();
        let mut get_request_queues = HashMap::new();
        let mut node_change_event_receivers = HashMap::new();

        for node_id in 1..=3 {
            let (mailbox_sender, mailbox) = flume::unbounded();

            mailbox_senders.insert(node_id, mailbox_sender);
            mailboxes.insert(node_id, mailbox);

            let (proposal_request_sender, proposal_request_queue) = flume::unbounded();
            proposal_request_senders.insert(node_id, proposal_request_sender);
            proposal_request_queues.insert(node_id, proposal_request_queue);

            let (get_request_sender, get_request_queue) = flume::unbounded();
            get_request_senders.insert(node_id, get_request_sender);
            get_request_queues.insert(node_id, get_request_queue);

            let (node_change_event_sender, node_change_event_receiver) = flume::unbounded();
            node_change_event_receivers.insert(node_id, node_change_event_receiver);

            // ignore receiver recv failed, here we don't need to send node change event
            mem::forget(node_change_event_sender);
        }

        let stop_signal = Arc::new(AtomicBool::new(false));

        let mut nodes = vec![];
        for node_id in 1..=3 {
            let mut builder = NodeBuilder::default();

            let (log_backend, kv_backend) = backends.remove(&node_id).unwrap();
            let mailbox = mailboxes.remove(&node_id).unwrap();

            let other_node_mailbox_senders = if node_id == 1 {
                let mailbox2 = mailbox_senders.get(&2).unwrap().clone();
                let mailbox3 = mailbox_senders.get(&3).unwrap().clone();

                [(2, mailbox2), (3, mailbox3)]
            } else if node_id == 2 {
                let mailbox1 = mailbox_senders.get(&1).unwrap().clone();
                let mailbox3 = mailbox_senders.get(&3).unwrap().clone();

                [(1, mailbox1), (3, mailbox3)]
            } else {
                let mailbox1 = mailbox_senders.get(&1).unwrap().clone();
                let mailbox2 = mailbox_senders.get(&2).unwrap().clone();

                [(1, mailbox1), (2, mailbox2)]
            }
            .into();

            let proposal_request_queue = proposal_request_queues.remove(&node_id).unwrap();
            let get_request_queue = get_request_queues.remove(&node_id).unwrap();
            let node_change_event_receiver = node_change_event_receivers.remove(&node_id).unwrap();

            builder
                .config(Config::new(node_id))
                .storage(log_backend)
                .kv(kv_backend)
                .mailbox(mailbox)
                .proposal_request_queue(proposal_request_queue)
                .get_request_queue(get_request_queue)
                .node_change_event_receiver(node_change_event_receiver)
                .stop_signal(stop_signal.clone());

            let mut node = builder.build().unwrap();

            // todo for test
            node.other_node_mailboxes = other_node_mailbox_senders;

            nodes.push(node);
        }

        let join_handles = nodes
            .into_iter()
            .map(|mut node| {
                thread::spawn(move || {
                    node.run()
                        .tap_err(|err| error!(%err, "node stop with error"))
                })
            })
            .collect::<Vec<_>>();

        let mut leader_id = 1;

        loop {
            dbg!(leader_id);

            let sender = proposal_request_senders.get_mut(&leader_id).unwrap();

            let (reply, result_receiver) = ProposalRequestReply::new(KeyValueOperation::new(
                Operation::InsertOrUpdate,
                b"test1".as_slice(),
                b"test1".as_slice(),
            ));

            sender.send(reply).unwrap();

            match result_receiver.recv().unwrap() {
                Err(RequestError::NotLeader(new_leader)) => {
                    dbg!(new_leader);

                    leader_id = new_leader;

                    thread::sleep(Duration::from_millis(100));

                    continue;
                }

                Ok(_) => {
                    eprintln!("proposal request done");

                    break;
                }

                Err(err) => panic!("{}", err),
            }
        }

        loop {
            dbg!(leader_id);

            let sender = get_request_senders.get_mut(&leader_id).unwrap();

            let (reply, result_receiver) = GetRequestReply::new(b"test1".as_slice());

            sender.send(reply).unwrap();

            match result_receiver.recv().unwrap() {
                Err(RequestError::NotLeader(new_leader)) => {
                    dbg!(new_leader);

                    leader_id = new_leader;

                    thread::sleep(Duration::from_millis(100));

                    continue;
                }

                Ok(value) => {
                    println!("{}", String::from_utf8_lossy(&value.unwrap()));

                    break;
                }

                Err(err) => panic!("{}", err),
            }
        }

        stop_signal.store(true, Ordering::Release);

        for join_handle in join_handles {
            let _ = join_handle.join();
        }
    }
}
