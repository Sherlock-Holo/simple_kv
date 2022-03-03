use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use prost::Message;
use raft::{
    eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata},
    Error, RaftState, Storage, StorageError,
};
use tracing::info;

use super::proposal_request::ProposalRequest;

#[derive(Debug, Default)]
pub struct MemStorageCore {
    state: RaftState,
    entries: Vec<Entry>,
    snapshot_metadata: SnapshotMetadata,
    pub map: HashMap<u64, String>,
}

impl MemStorageCore {
    pub fn initial_state(&self) -> RaftState {
        self.state.clone()
    }

    fn first_index(&self) -> u64 {
        self.entries
            .first()
            .map(|entry| entry.index)
            .unwrap_or_else(|| self.snapshot_metadata.index + 1)
    }

    fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or_else(|| self.snapshot_metadata.index + 1)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if self.snapshot_metadata.index == idx {
            return Ok(self.snapshot_metadata.term);
        }

        let first_index = self.first_index();

        if idx < first_index {
            return Err(Error::Store(StorageError::Compacted));
        }

        if idx > self.last_index() {
            return Err(Error::Store(StorageError::Unavailable));
        }

        Ok(self.entries[(idx - first_index) as usize].term)
    }

    fn create_snapshot_data(&self) -> Vec<u8> {
        todo!()
    }

    fn apply_snapshot_data(&mut self, _snapshot: &Snapshot) {
        todo!()
    }
}

#[derive(Debug)]
pub struct MemStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    pub fn new_with_node_ids(ids: &[u64]) -> Self {
        Self {
            core: Arc::new(RwLock::new(MemStorageCore {
                state: RaftState::new(
                    HardState::default(),
                    ConfState {
                        voters: ids.to_vec(),
                        ..Default::default()
                    },
                ),
                entries: vec![Entry {
                    index: 1,
                    ..Default::default()
                }],
                ..Default::default()
            })),
        }
    }

    pub fn get_core(&self) -> Arc<RwLock<MemStorageCore>> {
        self.core.clone()
    }

    pub fn apply_snapshot(&self, snapshot: &Snapshot) -> raft::Result<()> {
        let snapshot_metadata = if let Some(metadata) = snapshot.metadata.as_ref() {
            metadata
        } else {
            return Err(Error::RequestSnapshotDropped);
        };

        let mut core = self.core.write().unwrap();

        if core.first_index() > snapshot_metadata.index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        core.state.hard_state.commit = snapshot_metadata.index;
        core.state.hard_state.term = core.state.hard_state.term.max(snapshot_metadata.term);

        core.apply_snapshot_data(snapshot);

        if core.last_index() <= snapshot_metadata.index {
            core.entries.clear();

            return Ok(());
        }

        let pos = core.entries.iter().enumerate().find_map(|(pos, entry)| {
            if entry.index == snapshot_metadata.index {
                Some(pos)
            } else {
                None
            }
        }).expect("persist log doesn't contain snapshot last index when entries[0].index <= snapshot_metadata.index <= entries[last].index");

        core.entries.drain(..=pos).count();

        Ok(())
    }

    pub fn apply_conf_state(&self, conf_change: ConfState) {
        self.core.write().unwrap().state.conf_state = conf_change;
    }

    pub fn apply_hard_state(&self, hard_state: HardState) {
        self.core.write().unwrap().state.hard_state = hard_state;
    }

    pub fn apply_commit_index(&self, commit_index: u64) {
        self.core.write().unwrap().state.hard_state.commit = commit_index;
    }

    pub fn apply_proposal_request(&self, proposal_request: ProposalRequest) {
        info!(?proposal_request, "apply proposal request");

        let mut core = self.core.write().unwrap();

        match proposal_request {
            ProposalRequest::CreateOrUpdate(key, value) => {
                core.map.insert(key, value);
            }

            ProposalRequest::Delete(key) => {
                core.map.remove(&key);
            }
        }
    }

    pub fn append_entries(&self, entries: Vec<Entry>) -> raft::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut core = self.core.write().unwrap();
        let first_index = core.first_index();
        let last_index = core.last_index();
        let first_entry_index = entries[0].index;

        if first_index > first_entry_index {
            return Err(Error::Store(StorageError::Other(
                format!(
                    "entries contains compacted logs, first index {}, entries first index {}",
                    first_index, first_entry_index
                )
                .into(),
            )));
        }

        if last_index + 1 < first_entry_index {
            return Err(Error::Store(StorageError::Other(
                format!(
                    "logs should be continuous, last index {}, entries first index {}",
                    last_index, first_entry_index
                )
                .into(),
            )));
        }

        let diff_pos = (first_entry_index - first_index) as usize;

        core.entries.drain(diff_pos..).count();
        core.entries.extend(entries);

        Ok(())
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.core.read().unwrap().initial_state())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        let core = self.core.read().unwrap();

        let last_index = core.last_index();
        assert!(high <= last_index + 1);

        let max_size = max_size.into().unwrap_or(u64::MAX);

        let first_index = self.first_index()?;

        if low < first_index {
            return Err(Error::Store(StorageError::Compacted));
        }

        if core.entries.is_empty() {
            return Ok(vec![]);
        }

        let slice_left_index = (low - first_index) as usize;
        let slice_right_index = (high - first_index) as usize;

        let mut total_size = 0;

        let entries = core.entries[slice_left_index..slice_right_index]
            .iter()
            .take_while(|entry| {
                if total_size == 0 {
                    total_size += entry.encoded_len() as u64;

                    true
                } else {
                    total_size += entry.encoded_len() as u64;

                    total_size <= max_size
                }
            })
            .cloned()
            .collect::<Vec<_>>();

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.core.read().unwrap().term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.core.read().unwrap().first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.core.read().unwrap().last_index())
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        let mut core = self.core.write().unwrap();

        let commit_index = core.state.hard_state.commit;
        if request_index > commit_index {
            return Err(Error::Store(StorageError::Unavailable));
        }

        let commit_term = core.term(commit_index)?;

        let snapshot_data = core.create_snapshot_data();

        let metadata = SnapshotMetadata {
            conf_state: Some(core.state.conf_state.clone()),
            index: commit_index,
            term: commit_term,
        };

        let snapshot = Snapshot {
            metadata: Some(metadata),
            data: snapshot_data,
        };

        // 没有新 commit 数据产生
        if commit_index == core.snapshot_metadata.index {
            return Ok(snapshot);
        }

        let need_to_remove_entry_count = (commit_index - core.entries[0].index + 1) as usize;

        core.entries.drain(..need_to_remove_entry_count).count();

        Ok(snapshot)
    }
}
