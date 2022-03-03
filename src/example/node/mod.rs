use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{error, io};

use prost::Message as _;
use protobuf::error::WireError;
use protobuf::ProtobufError;
use raft::prelude::*;
use raft::{Error, StateRole, StorageError};
use tracing::info;

use super::proposal_request::ProposalRequest;
use super::storage::{MemStorage, MemStorageCore};

pub type ProposalQueue = VecDeque<(
    Option<ProposalRequest>,
    Sender<Result<(), Box<dyn error::Error + Send>>>,
)>;

pub struct Node {
    raw_node: RawNode<MemStorage>,
    my_mailbox: Receiver<Message>,
    other_mailboxes: HashMap<u64, Sender<Message>>,
    proposal_queue: Arc<Mutex<ProposalQueue>>,
}

impl Node {
    pub fn new(
        cfg: &Config,
        my_mailbox: Receiver<Message>,
        other_mailboxes: HashMap<u64, Sender<Message>>,
        proposal_queue: Arc<Mutex<ProposalQueue>>,
    ) -> raft::Result<Self> {
        let mut node_ids = other_mailboxes.keys().copied().collect::<Vec<_>>();
        node_ids.push(cfg.id);

        info!(?node_ids, "get all node ids");

        let mem_storage = MemStorage::new_with_node_ids(&node_ids);

        info!(?mem_storage, "create mem storage done");

        let raw_node = RawNode::with_default_logger(cfg, mem_storage)?;

        info!("create raw node done");

        Ok(Self {
            raw_node,
            my_mailbox,
            other_mailboxes,
            proposal_queue,
        })
    }

    pub fn get_inner_storage(&self) -> Arc<RwLock<MemStorageCore>> {
        self.raw_node.store().get_core()
    }

    pub fn run(&mut self) -> raft::Result<()> {
        let node_id = self.raw_node.raft.id;

        const TICK_INTERVAL: Duration = Duration::from_millis(100);

        let mut instant = Instant::now();

        loop {
            loop {
                match self.my_mailbox.try_recv() {
                    Ok(msg) => {
                        info!(node_id, ?msg, "get msg from mailbox");

                        self.raw_node.step(msg)?;
                    }

                    Err(TryRecvError::Disconnected) => {
                        return Err(Error::Io(io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "mailbox is died",
                        )))
                    }

                    _ => break,
                }
            }

            if instant.elapsed() >= TICK_INTERVAL {
                self.raw_node.tick();
                instant = Instant::now();
            }

            if self.raw_node.raft.state == StateRole::Leader {
                let proposal_request = self
                    .proposal_queue
                    .lock()
                    .unwrap()
                    .front_mut()
                    .and_then(|(req, _)| req.take());

                if let Some(proposal_request) = proposal_request {
                    info!(node_id, ?proposal_request, "leader handle proposal request");

                    self.proposal(proposal_request)?;
                }
            }

            if !self.raw_node.has_ready() {
                continue;
            }

            let mut ready = self.raw_node.ready();

            self.send_messages(ready.take_messages());

            let snapshot = ready.snapshot();
            if !snapshot.data.is_empty() && snapshot.metadata.is_some() {
                let mem_storage = self.raw_node.store();
                mem_storage.apply_snapshot(snapshot)?;
            }

            for commit_entry in ready.take_committed_entries() {
                self.handle_commit_entry(commit_entry)?;
            }

            self.raw_node.store().append_entries(ready.take_entries())?;

            if let Some(hard_state) = ready.hs() {
                self.raw_node.store().apply_hard_state(hard_state.clone());
            }

            self.send_messages(ready.take_persisted_messages());

            let mut light_ready = self.raw_node.advance(ready);

            if let Some(commit_index) = light_ready.commit_index() {
                self.raw_node.store().apply_commit_index(commit_index);
            }

            self.send_messages(light_ready.take_messages());

            for commit_entry in light_ready.take_committed_entries() {
                self.handle_commit_entry(commit_entry)?;
            }

            self.raw_node.advance_apply();
        }
    }

    fn send_messages(&mut self, messages: Vec<Message>) {
        for message in messages {
            if let Some(mailbox) = self.other_mailboxes.get(&message.to) {
                let _ = mailbox.send(message);
            }
        }
    }

    fn handle_commit_entry(&mut self, commit_entry: Entry) -> raft::Result<()> {
        let node_id = self.raw_node.raft.id;

        if commit_entry.data.is_empty() {
            return Ok(());
        }

        let entry_type = if let Ok(entry_type) = entry_type_from_i32(commit_entry.entry_type) {
            entry_type
        } else {
            todo!("handle incorrect commit entry")
        };

        info!(node_id, ?entry_type, "get entry type done");

        match entry_type {
            EntryType::EntryNormal => {
                let command = String::from_utf8(commit_entry.data).map_err(|err| {
                    Error::Store(StorageError::Other(
                        format!("convert entry data to command failed: {}", err).into(),
                    ))
                })?;

                info!(node_id, %command, "get command done");

                let commit_proposal_request = ProposalRequest::try_from(command.as_str())?;

                self.raw_node
                    .store()
                    .apply_proposal_request(commit_proposal_request);

                if self.raw_node.raft.state == StateRole::Leader {
                    if let Some((_, result_sender)) =
                        self.proposal_queue.lock().unwrap().pop_front()
                    {
                        let _ = result_sender.send(Ok(()));

                        info!(node_id, "leader respond the proposal request");
                    }
                }
            }

            EntryType::EntryConfChange => {
                let conf_change = ConfChange::decode(commit_entry.data.as_slice())
                    .map_err(|_| Error::CodecError(ProtobufError::WireError(WireError::Other)))?;

                let conf_state = self.raw_node.apply_conf_change(&conf_change)?;

                self.raw_node.store().apply_conf_state(conf_state);
            }

            EntryType::EntryConfChangeV2 => {
                let conf_change = ConfChangeV2::decode(commit_entry.data.as_slice())
                    .map_err(|_| Error::CodecError(ProtobufError::WireError(WireError::Other)))?;

                let conf_state = self.raw_node.apply_conf_change(&conf_change)?;

                self.raw_node.store().apply_conf_state(conf_state);
            }
        }

        Ok(())
    }

    fn proposal(&mut self, proposal_request: ProposalRequest) -> raft::Result<()> {
        self.raw_node.propose(vec![], proposal_request.into())
    }
}

fn entry_type_from_i32(t: i32) -> raft::Result<EntryType> {
    if t == EntryType::EntryNormal as i32 {
        Ok(EntryType::EntryNormal)
    } else if t == EntryType::EntryConfChange as i32 {
        Ok(EntryType::EntryConfChange)
    } else if t == EntryType::EntryConfChangeV2 as i32 {
        Ok(EntryType::EntryConfChangeV2)
    } else {
        Err(Error::CodecError(ProtobufError::WireError(
            WireError::InvalidEnumValue(t),
        )))
    }
}
