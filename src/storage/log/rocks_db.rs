use std::io;
use std::io::ErrorKind;

use bincode::config::{BigEndian, VarintEncoding, WithOtherEndian, WithOtherIntEncoding};
use bincode::{DefaultOptions, Options};
use bytes::buf::Writer;
use bytes::{BufMut, BytesMut};
use raft::{Error, StorageError};
use rocksdb::{ColumnFamily, Direction, IteratorMode, WriteBatch, DB};
use tracing::{error, info};

use crate::storage::log::{ConfigState, Entry, HardState, LogBackend, Snapshot, SnapshotMetadata};

type DataEncoding =
    WithOtherIntEncoding<WithOtherEndian<DefaultOptions, BigEndian>, VarintEncoding>;

const HARD_STATE_PATH: &str = "kv_core/state/hard_state";
const CONFIG_STATE_PATH: &str = "kv_core/state/config_state";
const ENTRIES_COLUMN_FAMILY: &str = "kv_core_entries";
const ENTRIES_PATH_PREFIX: &str = "kv_core/entries/";
const SNAPSHOT_METADATA_PATH: &str = "kv_core/snapshot/metadata";
const SNAPSHOT_DATA_PATH: &str = "kv_core/snapshot/data";
const LAST_INDEX_PATH: &str = "kv_core/last_index";

/// The state path is `kv_core/state/{hard_state, config_state}`
///
/// The last entry index path is `kv_core/last_index`
///
/// The snapshot path is `kv_core/snapshot/`. The snapshot metadata path is
/// `kv_core/snapshot/metadata`, the snapshot data path is `kv_core/snapshot/data`
///
/// The entries path is `kv_core/entries/`, and the entry name is the entry index, such as:
/// `kv_core/entries/1`, `kv_core/entries/2`. The entries are in the **entries** column family.
///
/// The entry index is continuous, but may not start from 1, because the entries can be compressed
/// to a snapshot
pub struct RocksdbBackend {
    db: DB,
    data_encoding: DataEncoding,
}

impl RocksdbBackend {
    fn apply_hard_state_with_batch(
        &mut self,
        hard_state: HardState,
        batch: Option<&mut WriteBatch>,
        encode_buf: &mut Writer<BytesMut>,
    ) -> Result<(), Error> {
        self.data_encoding
            .serialize_into(encode_buf, &hard_state)
            .map_err(|err| {
                error!(%err, ?hard_state, "serialize hard state failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!(?hard_state, "serialize hard state done");

        let hard_state = encode_buf.get_mut().split();

        match batch {
            None => self.db.put(HARD_STATE_PATH, hard_state).map_err(|err| {
                error!(%err, ?hard_state, "insert hard state to rocksdb failed");

                Error::Io(io::Error::new(ErrorKind::Other, err))
            }),

            Some(batch) => batch.put(HARD_STATE_PATH, hard_state).map_err(|err| {
                error!(%err, ?hard_state, "insert hard state to rocksdb failed");

                Error::Io(io::Error::new(ErrorKind::Other, err))
            }),
        }
    }

    fn get_snapshot_metadata(&self) -> Result<SnapshotMetadata, Error> {
        let metadata = self.db.get_pinned(SNAPSHOT_METADATA_PATH).map_err(|err| {
            error!(%err, "get snapshot metadata failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        match metadata {
            None => {
                error!("snapshot metadata is not exist");

                return Err(Error::ConfigInvalid(
                    "snapshot metadata is not exist".to_string(),
                ));
            }

            Some(metadata) => self.data_encoding.deserialize(&metadata).map_err(|err| {
                error!(%err, "deserialize snapshot metadata failed");

                Error::Store(StorageError::Other(err))
            }),
        }
    }

    fn get_column_family_handle(&self, column_family: &str) -> Result<&ColumnFamily, Error> {
        let column_family_handle = self.db.cf_handle(column_family).ok_or_else(|| {
            error!(column_family, "column family not found");

            Error::ConfigInvalid(format!("{} column family not found", column_family))
        })?;

        Ok(column_family_handle)
    }
}

impl LogBackend for RocksdbBackend {
    type Error = Error;

    fn append_entries(&mut self, entries: Vec<Entry>) -> Result<(), Self::Error> {
        if entries.is_empty() {
            info!("entries is empty");

            return Ok(());
        }

        let first_index = self.first_index()?;
        let mut last_index = self.last_index()?;
        let first_entry_index = entries[0].index;

        if first_entry_index < first_index {
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

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        let mut write_batch = WriteBatch::default();

        info!("get entries column family handle done");

        for delete_index in first_entry_index..=last_index {
            let delete_entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, delete_index);

            info!(delete_index, %delete_entry_path, "delete entry path");

            if !self
                .db
                .key_may_exist_cf(column_family_handle, &delete_entry_path)
            {
                info!(delete_index, %delete_entry_path, "entry definitely doesn't exist");

                break;
            }

            write_batch.delete_cf(column_family_handle, &delete_entry_path);

            info!(delete_index, %delete_entry_path, "delete entry in write batch done");
        }

        info!(
            first_entry_index,
            last_index, "delete all will be covered entries in write batch done"
        );

        let mut buf = BytesMut::with_capacity(entries[0].data.len()).writer();

        last_index = last_index.max(entries.last().unwrap().index);

        for entry in entries {
            let entry_index = entry.index;

            self.data_encoding
                .serialize_into(&mut buf, &entry)
                .map_err(|err| {
                    error!(%err, entry_index, "serialize entry failee");

                    Error::Store(StorageError::Other(err))
                })?;

            info!(entry_index, "serialize entry done");

            let entry = buf.get_mut().split();

            let entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, entry_index);

            info!(entry_index, %entry_path, "insert entry path");

            write_batch.put_cf(column_family_handle, entry_path, entry);

            info!(entry_index, %entry_path, "insert entry in write batch done");
        }

        self.data_encoding
            .serialize_into(&mut buf, &last_index)
            .map_err(|err| {
                error!(%err, last_index, "serialize last index failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!(last_index, "serialize last index done");

        let last_index = buf.into_inner();

        write_batch.put(LAST_INDEX_PATH, last_index);

        self.db.write(write_batch).map_err(|err| {
            error!(%err, "delete will be covered entries failed, append entries failed or update last index failed");

            error!(%err, "the whole append entries operation failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        info!(
            "delete will be covered entries done, append entries done and update last index done"
        );

        Ok(())
    }

    fn apply_config_state(&mut self, config_state: ConfigState) -> Result<(), Self::Error> {
        todo!()
    }

    fn apply_hard_state(&mut self, hard_state: HardState) -> Result<(), Self::Error> {
        todo!()
    }

    fn apply_commit_index(&mut self, commit_index: u64) -> Result<(), Self::Error> {
        todo!()
    }

    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Self::Error> {
        let index = snapshot.metadata.index;
        let snapshot_term = snapshot.metadata.term;

        let first_index = self.first_index()?;

        if first_index > index {
            error!(
                snapshot_index = index,
                first_index, "first index > snapshot index"
            );

            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let mut hard_state = self.initial_hard_state()?;

        hard_state.commit = index;
        hard_state.term = hard_state.term.max(snapshot_term);
    }

    fn initial_hard_state(&self) -> Result<HardState, Self::Error> {
        let hard_state = self.db.get(HARD_STATE_PATH).map_err(|err| {
            error!(%err, "get hard state failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        let hard_state = match hard_state {
            None => {
                error!("an initial rocks db has no hard state");

                return Err(Error::ConfigInvalid(
                    "an initial rocks db has no hard state".to_string(),
                ));
            }

            Some(hard_state) => {
                let hard_state: HardState =
                    self.data_encoding.deserialize(&hard_state).map_err(|err| {
                        error!(%err, "deserialize hard state data failed");

                        Error::ConfigInvalid(format!("deserialize hard state data failed: {}", err))
                    })?;

                info!(?hard_state, "deserialize hard state done");

                hard_state
            }
        };

        Ok(hard_state)
    }

    fn initial_config_state(&self) -> Result<ConfigState, Self::Error> {
        let config_state = self.db.get(CONFIG_STATE_PATH).map_err(|err| {
            error!(%err, "get config state failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        let config_state = match config_state {
            None => {
                error!("an initial rocks db has no config state");

                return Err(Error::ConfigInvalid(
                    "an initial rocks db has no config state".to_string(),
                ));
            }

            Some(hard_state) => {
                let config_state: ConfigState =
                    self.data_encoding.deserialize(&hard_state).map_err(|err| {
                        error!(%err, "deserialize config state data failed");

                        Error::ConfigInvalid(format!(
                            "deserialize config state data failed: {}",
                            err
                        ))
                    })?;

                info!(?config_state, "deserialize config state done");

                config_state
            }
        };

        Ok(config_state)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: Option<u64>,
    ) -> Result<Vec<Entry>, Self::Error> {
        let last_index = self.last_index()?;

        info!(last_index, "get last index done");

        assert!(high <= last_index + 1);

        let max_size = max_size.unwrap_or(u64::MAX);

        let first_index = self.first_index()?;

        info!(first_index, "get first index done");

        if low < first_index {
            error!(low, first_index, "low index < first index");

            return Err(Error::Store(StorageError::Compacted));
        }

        let mut total_size = 0;
        let mut entries = Vec::with_capacity((high - low) as _);

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        info!("get entries column family handle done");

        for index in low..high {
            let entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, index);

            info!(index, %entry_path, "get entry path done");

            let entry = self
                .db
                .get_pinned_cf(column_family_handle, &entry_path)
                .map_err(|err| {
                    error!(%err, "get entry failed");

                    Error::Store(StorageError::Other(err.into()))
                })?;

            let entry = match entry {
                None => {
                    error!(index, %entry_path, "entry not exists");

                    return Err(Error::Store(StorageError::Unavailable));
                }

                Some(entry) => entry,
            };

            if total_size == 0 {
                total_size += entry.len();
            } else {
                total_size += entry.len();

                if total_size > max_size as usize {
                    info!(
                        total_size = total_size - entry.len(),
                        max_size, "reach max size limit"
                    );

                    break;
                }
            }

            let entry: Entry = self.data_encoding.deserialize(&entry).map_err(|err| {
                error!(%err, index, %entry_path, "deserialize entry failed");

                Error::Store(StorageError::Other(err))
            })?;

            info!(index, %entry_path, "get entry done");

            entries.push(entry);
        }

        info!(low, high, max_size, total_size, "get entries done");

        Ok(entries)
    }

    fn term(&self, idx: u64) -> Result<u64, Self::Error> {
        let snapshot_metadata = self.get_snapshot_metadata()?;

        info!(?snapshot_metadata, "get snapshot metadata done");

        if snapshot_metadata.index == idx {
            info!(idx, "the entry index == snapshot index");

            return Ok(snapshot_metadata.term);
        }

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        info!(
            entries_clomn_family = ENTRIES_COLUMN_FAMILY,
            "get entries column family handle done"
        );

        let entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, idx);

        info!(idx, %entry_path, "get entry path done");

        let entry = self
            .db
            .get_pinned_cf(column_family_handle, &entry_path)
            .map_err(|err| {
                error!(%err, %entry_path, "get entry failed");

                Error::Store(StorageError::Other(err.into()))
            })?;

        let entry = match entry {
            None => {
                error!(idx, "entry is not exist");

                return Err(Error::Store(StorageError::Other(
                    format!("entry {} is not exist", idx).into(),
                )));
            }

            Some(entry) => entry,
        };

        let entry: Entry = self.data_encoding.deserialize(&entry).map_err(|err| {
            error!(%err, "deserialize entry failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        info!(idx, "get entry done");

        Ok(entry.term)
    }

    fn first_index(&self) -> Result<u64, Self::Error> {
        let snapshot_metadata = self.get_snapshot_metadata()?;

        info!(?snapshot_metadata, "get snapshot metadata done");

        // the first entry index is always snapshot index+1, because the log is continuous
        Ok(snapshot_metadata.index + 1)
    }

    fn last_index(&self) -> Result<u64, Self::Error> {
        let last_index = self.db.get_pinned(LAST_INDEX_PATH).map_err(|err| {
            error!(%err, "get last index path failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        match last_index {
            None => {
                error!("the last index not exist");

                Err(Error::ConfigInvalid("the last index not exist".to_string()))
            }

            Some(last_index) => {
                let last_index = String::from_utf8_lossy(&last_index)
                    .parse::<u64>()
                    .map_err(|err| {
                        error!(%err, "deserialize last index failed");

                        Error::Store(StorageError::Other(err.into()))
                    })?;

                Ok(last_index)
            }
        }
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot, Self::Error> {
        todo!()
    }
}
