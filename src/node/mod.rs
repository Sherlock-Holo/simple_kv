use std::collections::HashMap;
use std::iter::Peekable;
use std::time::{Duration, Instant};

use anyhow::Context;
use bincode::{DefaultOptions, Options};
use flume::{Receiver, Sender, TryIter};
use prost07::Message as _;
use protobuf::error::WireError;
use protobuf::ProtobufError;
use raft::{eraftpb, Config, RawNode, StateRole, Storage};
use tap::TapFallible;
use tracing::{error, info, instrument, warn};

use crate::reply::{GetRequestReply, ProposalRequestReply};
use crate::storage::key_value::{KeyValueBackend, KeyValueOperation};
use crate::storage::log::{ConfigState, Entry, EntryType, LogBackend, Snapshot};

pub struct NodeBuilder<KV, ST> {
    config: Option<Config>,
    tick_interval: Duration,
    storage: Option<ST>,
    kv: Option<KV>,
    mailbox: Option<Receiver<eraftpb::Message>>,
    other_node_mailboxes: Option<HashMap<u64, Sender<eraftpb::Message>>>,
    proposal_request_queue: Option<Receiver<ProposalRequestReply>>,
    get_request_queue: Option<Receiver<GetRequestReply>>,
}

impl<KV, ST> Default for NodeBuilder<KV, ST> {
    fn default() -> Self {
        Self {
            config: None,
            tick_interval: Duration::from_millis(100),
            storage: None,
            kv: None,
            mailbox: None,
            other_node_mailboxes: None,
            proposal_request_queue: None,
            get_request_queue: None,
        }
    }
}

impl<KV, ST> NodeBuilder<KV, ST> {
    pub fn config(&mut self, config: Config) -> &mut Self {
        self.config.replace(config);

        self
    }

    pub fn tick_interval(&mut self, tick_interval: Duration) -> &mut Self {
        self.tick_interval = tick_interval;

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

    pub fn other_node_mailboxes(
        &mut self,
        other_node_mailboxes: HashMap<u64, Sender<eraftpb::Message>>,
    ) -> &mut Self {
        self.other_node_mailboxes.replace(other_node_mailboxes);

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

        let other_node_mailboxes = self
            .other_node_mailboxes
            .take()
            .with_context(|| anyhow!("other_node_mailboxes is not set"))?;

        let proposal_request_queue = self
            .proposal_request_queue
            .take()
            .with_context(|| anyhow!("proposal_request_queue is not set"))?;

        let get_request_queue = self
            .get_request_queue
            .take()
            .with_context(|| anyhow!("get_request_queue is not set"))?;

        let raw_node = RawNode::with_default_logger(&config, storage)
            .tap_err(|err| error!(%err, "init raw node failed"))?;

        info!("init raw node done");

        Ok(Node {
            mailbox,
            other_node_mailboxes,
            raw_node,
            tick_interval,
            key_value_backend: kv,
            proposal_request_queue,
            get_request_queue,
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
    /// can get the result sender from the proposal request queue to reply the client
    proposal_request_queue: Receiver<ProposalRequestReply>,

    get_request_queue: Receiver<GetRequestReply>,
}

impl<KV, ST> Node<KV, ST>
where
    ST: Storage + LogBackend,
    ST::Error: Send + Sync + 'static,
    KV: KeyValueBackend,
{
    pub fn run(&mut self) -> anyhow::Result<()> {
        info!("start node infinite running loop");

        let mut instant = Instant::now();

        let proposal_request_queue = self.proposal_request_queue.clone();
        let mut proposal_request_queue = proposal_request_queue.try_iter().peekable();

        loop {
            self.run_a_cycle(&mut instant, &mut proposal_request_queue)?;
        }
    }

    #[instrument(skip(self, proposal_request_queue), err)]
    fn run_a_cycle(
        &mut self,
        instant: &mut Instant,
        proposal_request_queue: &mut Peekable<TryIter<ProposalRequestReply>>,
    ) -> anyhow::Result<()> {
        if self.get_request_queue.is_disconnected() {
            error!("get request queue is disconnected");

            return Err(anyhow::anyhow!("get request queue is disconnected"));
        }

        if self.is_leader() {
            for reply in self.get_request_queue.clone().try_iter() {
                self.handle_get(reply);
            }

            info!("handle get request done");
        } else {
            self.get_request_queue
                .try_iter()
                .into_iter()
                .for_each(|reply| {
                    warn!("reject get request because node is not leader");
                    reply.reply(Err(anyhow::anyhow!("node is not raft leader")))
                });
        }

        if self.mailbox.is_disconnected() {
            error!("mailbox is disconnected");

            return Err(anyhow::anyhow!("mailbox is disconnected"));
        }

        for msg in self.mailbox.clone().try_iter() {
            let from = msg.from;

            info!(from, "get other node message done");

            self.raw_node.step(msg).tap_err(|err| {
                error!(%err, from, "step other node message failed");
            })?;

            info!(from, "step other node message done");
        }

        let elapsed = instant.elapsed();
        if elapsed >= self.tick_interval {
            info!(?elapsed, tick_interval = ?self.tick_interval, "elapsed >= tick_interval, tick the raw node");

            self.raw_node.tick();

            *instant = Instant::now();
        }

        if self.proposal_request_queue.is_disconnected() {
            error!("proposal request queue is disconnected");

            return Err(anyhow::anyhow!("proposal request queue is disconnected"));
        }

        if self.raw_node.raft.state != StateRole::Leader {
            for reply in proposal_request_queue.by_ref() {
                warn!("node is not raff leader, reject proposal request");

                reply.reply(Err(anyhow::anyhow!("node is not leader")));
            }
        } else if let Some(reply) = proposal_request_queue.peek_mut() {
            if !reply.handling() {
                info!("node is raft leader, handle proposal request");

                let key_value_operation = reply.handle();

                self.propose_key_value_operation(key_value_operation)?;

                info!("propose key value operation done");
            }
        }

        if !self.raw_node.has_ready() {
            info!("node has no ready event");

            return Ok(());
        }

        info!("node has ready event");

        self.handle_ready()?;

        info!("handle ready event done");

        Ok(())
    }

    fn handle_ready(&mut self) -> anyhow::Result<()> {
        let mut ready = self.raw_node.ready();

        self.send_message_to_other_nodes(ready.take_messages())?;

        info!("send message to other nodes done");

        let snapshot = ready.snapshot();
        if snapshot.metadata.is_some() && !snapshot.data.is_empty() {
            info!("ready snapshot is not empty snapshot, need to apply");

            let snapshot = Snapshot::try_from(snapshot)
                .tap_err(|err| error!(%err, "convert rpc snapshot to log snapshot failed"))?;

            info!("convert rpc snapshot to log snapshot done");

            self.raw_node.mut_store().apply_snapshot(snapshot)?;

            info!("apply snapshot done");
        }

        let entries = ready
            .take_entries()
            .into_iter()
            .map(Entry::try_from)
            .collect::<Result<Vec<_>, _>>()
            .tap_err(|err| error!(%err, "convert rpc entries to log entries failed"))?;

        info!("convert rcp entries to log entries done");

        self.raw_node.mut_store().append_entries(entries)?;

        info!("apply entries done");

        for commit_entry in ready.take_committed_entries() {
            self.apply_commit_entry(commit_entry)?;

            info!("apply commit entry done");
        }

        info!("apply all commit entries done");

        if let Some(hard_state) = ready.hs() {
            self.raw_node
                .mut_store()
                .apply_hard_state(hard_state.clone().into())?;

            info!("apply hard state done");
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

    fn send_message_to_other_nodes(
        &mut self,
        messages: Vec<eraftpb::Message>,
    ) -> anyhow::Result<()> {
        for msg in messages {
            let to = msg.to;

            info!(to, "message start send to node");

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

    fn apply_commit_entry(&mut self, commit_entry: eraftpb::Entry) -> anyhow::Result<()> {
        if commit_entry.data.is_empty() {
            info!("commit entry data is empty, skip it");

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
                        let mut proposal_request_queue =
                            self.proposal_request_queue.try_iter().peekable();

                        if let Some(reply) = proposal_request_queue.peek_mut() {
                            if reply.handling() {
                                info!("a proposal request can reply now");

                                let reply = proposal_request_queue.next().unwrap();

                                reply.reply(Ok(()));

                                info!("reply proposal request done");
                            }
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

    fn handle_get(&mut self, reply: GetRequestReply) {
        let result = match self.key_value_backend.get(reply.key()) {
            Err(err) => Err(anyhow::Error::from(err)),
            Ok(value) => {
                info!("get value done");

                Ok(value)
            }
        };

        reply.reply(result);
    }
}

fn encoding() -> impl Options + Copy {
    DefaultOptions::new()
        .with_big_endian()
        .with_varint_encoding()
}

#[cfg(test)]
mod tests {
    use std::{env, thread};

    use tempfile::TempDir;

    use super::*;
    use crate::storage::key_value::rocksdb::RocksdbBackend as KvBackend;
    use crate::storage::log::rocksdb::RocksdbBackend as LogBackend;

    #[test]
    fn test_new() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let log_path = tmp_dir.path().join("log");
        let kv_path = tmp_dir.path().join("kv");

        let kv_backend = KvBackend::create(&kv_path).unwrap();

        let log_backend = LogBackend::create(
            &log_path,
            ConfigState {
                voters: vec![1, 2, 3],
                ..Default::default()
            },
            kv_backend.clone(),
        )
        .unwrap();

        let mut builder = NodeBuilder::default();

        let (_mailbox_sender, mailbox) = flume::unbounded();

        let (node2_mailbox_sender, _node2_mailbox) = flume::unbounded();
        let (node3_mailbox_sender, _node3_mailbox) = flume::unbounded();

        let other_node_mailboxes =
            HashMap::from([(2, node2_mailbox_sender), (3, node3_mailbox_sender)]);

        let (_proposal_request_sender, proposal_request_queue) = flume::unbounded();
        let (_get_request_sender, get_request_queue) = flume::unbounded();

        builder
            .config(Config::new(1))
            .storage(log_backend)
            .kv(kv_backend)
            .mailbox(mailbox)
            .other_node_mailboxes(other_node_mailboxes)
            .proposal_request_queue(proposal_request_queue)
            .get_request_queue(get_request_queue);

        let _node = builder.build().unwrap();
    }

    #[test]
    fn test_send_vote_request() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let log_path = tmp_dir.path().join("log");
        let kv_path = tmp_dir.path().join("kv");

        let kv_backend = KvBackend::create(&kv_path).unwrap();

        let log_backend = LogBackend::create(
            &log_path,
            ConfigState {
                voters: vec![1, 2, 3],
                ..Default::default()
            },
            kv_backend.clone(),
        )
        .unwrap();

        let mut builder = NodeBuilder::default();

        let (_mailbox_sender, mailbox) = flume::unbounded();

        let (node2_mailbox_sender, node2_mailbox) = flume::unbounded();
        let (node3_mailbox_sender, node3_mailbox) = flume::unbounded();

        let other_node_mailboxes =
            HashMap::from([(2, node2_mailbox_sender), (3, node3_mailbox_sender)]);

        let (_proposal_request_sender, proposal_request_queue) = flume::unbounded();
        let (_get_request_sender, get_request_queue) = flume::unbounded();

        builder
            .config(Config::new(1))
            .storage(log_backend)
            .kv(kv_backend)
            .mailbox(mailbox)
            .other_node_mailboxes(other_node_mailboxes)
            .proposal_request_queue(proposal_request_queue)
            .get_request_queue(get_request_queue);

        let mut node = builder.build().unwrap();

        thread::spawn(move || node.run());

        // wait the node send vote request
        thread::sleep(Duration::from_secs(5));

        let node2_msg = node2_mailbox.try_recv().unwrap();
        let node3_msg = node3_mailbox.try_recv().unwrap();

        dbg!(node2_msg);
        dbg!(node3_msg);
    }
}
