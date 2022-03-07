use std::error::Error;
use std::fmt::Formatter;

use bytes::Bytes;
use raft::eraftpb;
use serde::de::{Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use super::key_value::KeyValueDatabaseBackend;

mod rocks_db;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntryType {
    EntryNormal,
    EntryConfChange,
    EntryConfChangeV2,
}

impl From<EntryType> for i32 {
    fn from(et: EntryType) -> Self {
        match et {
            EntryType::EntryNormal => 0,
            EntryType::EntryConfChange => 1,
            EntryType::EntryConfChangeV2 => 2,
        }
    }
}

impl From<eraftpb::EntryType> for EntryType {
    fn from(t: eraftpb::EntryType) -> Self {
        match t {
            eraftpb::EntryType::EntryNormal => EntryType::EntryNormal,
            eraftpb::EntryType::EntryConfChange => EntryType::EntryConfChange,
            eraftpb::EntryType::EntryConfChangeV2 => EntryType::EntryConfChangeV2,
        }
    }
}

impl From<EntryType> for eraftpb::EntryType {
    fn from(et: EntryType) -> Self {
        match et {
            EntryType::EntryNormal => eraftpb::EntryType::EntryNormal,
            EntryType::EntryConfChange => eraftpb::EntryType::EntryConfChange,
            EntryType::EntryConfChangeV2 => eraftpb::EntryType::EntryConfChangeV2,
        }
    }
}

impl Serialize for EntryType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i32(i32::from(*self))
    }
}

struct EntryTypeVisitor;

impl<'de> Visitor<'de> for EntryTypeVisitor {
    type Value = EntryType;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("valid value is 0, 1, or 2")
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match EntryType::try_from(v) {
            Ok(et) => Ok(et),
            Err(err) => Err(E::invalid_value(Unexpected::Signed(err.0 as _), &self)),
        }
    }
}

impl<'de> Deserialize<'de> for EntryType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_i32(EntryTypeVisitor)
    }
}

#[derive(Debug, Error)]
#[error("invalid entry type number {0}")]
pub struct EntryTypeError(pub i32);

impl TryFrom<i32> for EntryType {
    type Error = EntryTypeError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EntryType::EntryNormal),
            1 => Ok(EntryType::EntryConfChange),
            2 => Ok(EntryType::EntryConfChangeV2),
            other => Err(EntryTypeError(other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,
    pub data: Bytes,
    pub context: Bytes,
}

impl TryFrom<eraftpb::Entry> for Entry {
    type Error = EntryTypeError;

    fn try_from(entry: eraftpb::Entry) -> Result<Self, Self::Error> {
        let entry_type = EntryType::try_from(entry.entry_type)?;

        Ok(Self {
            entry_type,
            term: entry.term,
            index: entry.index,
            data: entry.data.into(),
            context: entry.context.into(),
        })
    }
}

impl From<Entry> for eraftpb::Entry {
    fn from(e: Entry) -> Self {
        Self {
            entry_type: e.entry_type.into(),
            term: e.term,
            index: e.index,
            data: e.data.to_vec(),
            context: e.context.to_vec(),
            sync_log: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigState {
    pub voters: Vec<u64>,

    pub learners: Vec<u64>,

    /// The voters in the outgoing config. If not empty the node is in joint consensus.
    pub voters_outgoing: Vec<u64>,

    /// The nodes that will become learners when the outgoing config is removed.
    /// These nodes are necessarily currently in nodes_joint (or they would have
    /// been added to the incoming config right away).
    pub learners_next: Vec<u64>,
    /// If set, the config is joint and Raft will automatically transition into
    /// the final config (i.e. remove the outgoing config) when this is safe.
    pub auto_leave: bool,
}

impl From<eraftpb::ConfState> for ConfigState {
    fn from(conf_state: eraftpb::ConfState) -> Self {
        Self {
            voters: conf_state.voters,
            learners: conf_state.learners,
            voters_outgoing: conf_state.voters_outgoing,
            learners_next: conf_state.learners_next,
            auto_leave: conf_state.auto_leave,
        }
    }
}

impl From<ConfigState> for eraftpb::ConfState {
    fn from(c: ConfigState) -> Self {
        Self {
            voters: c.voters,
            learners: c.learners,
            voters_outgoing: c.voters_outgoing,
            learners_next: c.learners_next,
            auto_leave: c.auto_leave,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

impl From<eraftpb::HardState> for HardState {
    fn from(hard_state: eraftpb::HardState) -> Self {
        Self {
            term: hard_state.term,
            vote: hard_state.vote,
            commit: hard_state.commit,
        }
    }
}

impl From<HardState> for eraftpb::HardState {
    fn from(h: HardState) -> Self {
        Self {
            term: h.term,
            vote: h.vote,
            commit: h.commit,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// The current `ConfigState`.
    pub config_state: Option<ConfigState>,

    /// The applied index.
    pub index: u64,

    /// The term of the applied index.
    pub term: u64,
}

impl From<eraftpb::SnapshotMetadata> for SnapshotMetadata {
    fn from(metadata: eraftpb::SnapshotMetadata) -> Self {
        Self {
            config_state: metadata.conf_state.map(Into::into),
            index: metadata.index,
            term: metadata.term,
        }
    }
}

impl From<SnapshotMetadata> for eraftpb::SnapshotMetadata {
    fn from(m: SnapshotMetadata) -> Self {
        Self {
            conf_state: m.config_state.map(Into::into),
            index: m.index,
            term: m.term,
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub data: Bytes,
    pub metadata: SnapshotMetadata,
}

#[derive(Debug, Error)]
#[error("snapshot metadata is none")]
pub struct SnapshotError(());

impl TryFrom<eraftpb::Snapshot> for Snapshot {
    type Error = SnapshotError;

    fn try_from(snapshot: eraftpb::Snapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            data: snapshot.data.into(),
            metadata: snapshot.metadata.ok_or(SnapshotError(()))?.into(),
        })
    }
}

impl From<Snapshot> for eraftpb::Snapshot {
    fn from(s: Snapshot) -> Self {
        Self {
            data: s.data.to_vec(),
            metadata: Some(s.metadata.into()),
        }
    }
}

pub trait LogBackend {
    type Error: Error + Into<raft::Error>;

    fn append_entries(&mut self, entries: Vec<Entry>) -> Result<(), Self::Error>;

    fn apply_config_state(&mut self, config_state: ConfigState) -> Result<(), Self::Error>;

    fn apply_hard_state(&mut self, hard_state: HardState) -> Result<(), Self::Error>;

    fn apply_commit_index(&mut self, commit_index: u64) -> Result<(), Self::Error>;

    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Self::Error>;

    /// `initial_hard_state` is called when Raft is initialized. This interface will return a
    /// `HardState`.
    fn initial_hard_state(&self) -> Result<HardState, Self::Error>;

    /// `initial_config_state` is called when Raft is initialized. This interface will return a
    /// `ConfigState`.
    fn initial_config_state(&self) -> Result<ConfigState, Self::Error>;

    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// # Panics
    ///
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: Option<u64>,
    ) -> Result<Vec<Entry>, Self::Error>;

    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    fn term(&self, idx: u64) -> Result<u64, Self::Error>;

    /// Returns the index of the first log entry that is possible available via entries, which will
    /// always equal to `truncated index` plus 1.
    ///
    /// New created (but not initialized) `Storage` can be considered as truncated at 0 so that 1
    /// will be returned in this case.
    fn first_index(&self) -> Result<u64, Self::Error>;

    /// The index of the last entry replicated in the `Storage`.
    fn last_index(&self) -> Result<u64, Self::Error>;

    /// Returns the most recent snapshot.
    ///
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    /// A snapshot's index must not less than the `request_index`.
    fn snapshot<KV: KeyValueDatabaseBackend>(
        &self,
        request_index: u64,
        backend: &KV,
    ) -> Result<Snapshot, Self::Error>;
}
