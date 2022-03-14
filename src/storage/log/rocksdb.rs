use std::io;
use std::io::ErrorKind;
use std::path::Path;

use bincode::config::{BigEndian, VarintEncoding, WithOtherEndian, WithOtherIntEncoding};
use bincode::{DefaultOptions, Options};
use bytes::{BufMut, BytesMut};
use raft::{eraftpb, Error, RaftState, Storage, StorageError};
use rocksdb::{ColumnFamily, WriteBatch, DB};
use tap::TapFallible;
use tracing::{debug, error, info, instrument};

use crate::storage::key_value::{KeyValueBackend, KeyValuePair};
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
pub struct RocksdbBackend<KV> {
    db: DB,
    kv_backend: KV,
    data_encoding: DataEncoding,
}

impl<KV> RocksdbBackend<KV>
where
    KV: KeyValueBackend,
    KV::Error: Into<Error>,
{
    pub fn from_exist(
        db_path: &Path,
        config_state: ConfigState,
        kv_backend: KV,
    ) -> anyhow::Result<Self> {
        let db = DB::open_cf(
            &rocksdb::Options::default(),
            db_path,
            [ENTRIES_COLUMN_FAMILY],
        )
        .tap_err(|err| {
            error!(%err, ?db_path, "open rocksdb failed");
        })?;

        info!("open rocksdb done");

        let data_encoding = DefaultOptions::new()
            .with_big_endian()
            .with_varint_encoding();

        let mut backend = Self {
            db,
            kv_backend,
            data_encoding,
        };

        backend
            .apply_config_state(config_state.clone())
            .tap_err(|err| {
                error!(%err, ?config_state, "apply current config state failed");
            })?;

        info!(?config_state, "apply current config state done");

        Ok(backend)
    }

    pub fn create(path: &Path, config_state: ConfigState, kv_backend: KV) -> anyhow::Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let data_encoding = DefaultOptions::new()
            .with_big_endian()
            .with_varint_encoding();

        let db = DB::open_cf(&options, path, [ENTRIES_COLUMN_FAMILY]).tap_err(|err| {
            error!(%err, ?path, "create rocksdb failed");
        })?;

        let hard_state = HardState::default();

        let hard_state = data_encoding.serialize(&hard_state).tap_err(|err| {
            error!(%err, ?hard_state, "serialize hard state failed");
        })?;

        db.put(HARD_STATE_PATH, hard_state)
            .tap_err(|err| error!(%err, "insert hard state failed"))?;

        let config_state = data_encoding
            .serialize(&config_state)
            .tap_err(|err| error!(%err, ?config_state, "serialize config state failed"))?;

        db.put(CONFIG_STATE_PATH, config_state)
            .tap_err(|err| error!(%err, "insert config state failed"))?;

        let snapshot_metadata = SnapshotMetadata::default();
        let snapshot_metadata = data_encoding.serialize(&snapshot_metadata).tap_err(
            |err| error!(%err, ?snapshot_metadata, "serialize snapshot metadata failed"),
        )?;

        db.put(SNAPSHOT_METADATA_PATH, snapshot_metadata)
            .tap_err(|err| error!(%err, "insert snapshot metadata failed"))?;

        let snapshot_data = Vec::<KeyValuePair>::new();
        let snapshot_data = data_encoding
            .serialize(&snapshot_data)
            .tap_err(|err| error!(%err, "serialize emtpy snapshot data failed"))?;

        db.put(SNAPSHOT_DATA_PATH, snapshot_data)
            .tap_err(|err| error!(%err, "insert empty snapshot data failed"))?;

        let last_index = data_encoding
            .serialize(&0u64)
            .tap_err(|err| error!(%err, "serialize 0 last index failed"))?;

        db.put(LAST_INDEX_PATH, last_index)
            .tap_err(|err| error!(%err, "insert 0 last index failed"))?;

        info!("init rocksdb done");

        Ok(RocksdbBackend {
            db,
            kv_backend,
            data_encoding,
        })
    }

    #[instrument(skip(self, batch, encode_buf), err)]
    fn apply_hard_state_with_batch(
        &mut self,
        hard_state: &HardState,
        batch: Option<&mut WriteBatch>,
        encode_buf: &mut BytesMut,
    ) -> Result<(), Error> {
        let mut encode_buf = encode_buf.writer();

        self.data_encoding
            .serialize_into(&mut encode_buf, hard_state)
            .map_err(|err| {
                error!(%err, ?hard_state, "serialize hard state failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!(?hard_state, "serialize hard state done");

        let hard_state_data = encode_buf.get_mut().split();

        match batch {
            None => self
                .db
                .put(HARD_STATE_PATH, hard_state_data)
                .map_err(|err| {
                    error!(%err, ?hard_state, "insert hard state failed");

                    Error::Io(io::Error::new(ErrorKind::Other, err))
                }),

            Some(batch) => {
                batch.put(HARD_STATE_PATH, hard_state_data);

                info!(?hard_state, "insert hard state to write batch done");

                Ok(())
            }
        }
    }

    #[instrument(skip(self, batch, encode_buf), err)]
    fn apply_config_state_with_batch(
        &mut self,
        config_state: &ConfigState,
        batch: Option<&mut WriteBatch>,
        encode_buf: &mut BytesMut,
    ) -> Result<(), Error> {
        let mut encode_buf = encode_buf.writer();

        self.data_encoding
            .serialize_into(&mut encode_buf, config_state)
            .map_err(|err| {
                error!(%err, ?config_state, "serialize config state failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!(?config_state, "serialize config state done");

        let config_state_data = encode_buf.get_mut().split();

        match batch {
            None => self
                .db
                .put(CONFIG_STATE_PATH, config_state_data)
                .map_err(|err| {
                    error!(%err, ?config_state, "insert config state failed");

                    Error::Io(io::Error::new(ErrorKind::Other, err))
                }),

            Some(batch) => {
                batch.put(CONFIG_STATE_PATH, config_state_data);

                info!(?config_state, "insert config state to write batch done");

                Ok(())
            }
        }
    }

    #[instrument(skip(self), err)]
    fn get_snapshot_metadata(&self) -> Result<SnapshotMetadata, Error> {
        let metadata = self.db.get_pinned(SNAPSHOT_METADATA_PATH).map_err(|err| {
            error!(%err, "get snapshot metadata failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        match metadata {
            None => {
                error!("snapshot metadata is not exist");

                Err(Error::ConfigInvalid(
                    "snapshot metadata is not exist".to_string(),
                ))
            }

            Some(metadata) => self.data_encoding.deserialize(&metadata).map_err(|err| {
                error!(%err, "deserialize snapshot metadata failed");

                Error::Store(StorageError::Other(err))
            }),
        }
    }

    #[instrument(skip(self), err)]
    fn get_column_family_handle(&self, column_family: &str) -> Result<&ColumnFamily, Error> {
        let column_family_handle = self.db.cf_handle(column_family).ok_or_else(|| {
            error!(column_family, "column family not found");

            Error::ConfigInvalid(format!("{} column family not found", column_family))
        })?;

        Ok(column_family_handle)
    }

    #[instrument(skip(self), err)]
    fn create_snapshot(&self, request_index: u64) -> Result<Snapshot, Error> {
        let hard_state = self.hard_state()?;

        info!(?hard_state, "get hard state done");

        if request_index > hard_state.commit {
            error!(
                request_index,
                commit_index = hard_state.commit,
                "request index > commit index"
            );

            return Err(Error::Store(StorageError::Other(
                format!(
                    "request index {} > commit index {}",
                    request_index, hard_state.commit
                )
                .into(),
            )));
        }

        let first_index = self.first_index()?;

        info!(first_index, "get first index done");

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        info!("get entry column family handle done");

        let mut write_batch = WriteBatch::default();

        for delete_index in first_index..=hard_state.commit {
            let delete_entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, delete_index);

            debug!(delete_index, %delete_entry_path, "delete entry path");

            write_batch.delete_cf(column_family_handle, &delete_entry_path);

            debug!(delete_index, %delete_entry_path, "delete entry to write batch done");
        }

        let config_state = self.config_state()?;

        info!(?config_state, "get current config state done");

        let mut buf = BytesMut::with_capacity(4096).writer();

        let new_snapshot_metadata = SnapshotMetadata {
            config_state: Some(config_state),
            index: hard_state.commit,
            term: hard_state.term,
        };

        self.data_encoding
            .serialize_into(&mut buf, &new_snapshot_metadata)
            .map_err(|err| {
                error!(%err, "serialize new snapshot metadata failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!(
            ?new_snapshot_metadata,
            "serialize new snapshot metadata done"
        );

        let new_snapshot_metadata_data = buf.get_mut().split();

        write_batch.put(SNAPSHOT_METADATA_PATH, new_snapshot_metadata_data);

        debug!("insert new snapshot metadata to write batch done");

        let key_value_pairs = self.kv_backend.all().map_err(|err| {
            error!(%err, "get all key value pairs failed");

            Error::Store(StorageError::Other(err.into().into()))
        })?;

        info!("get all key value pairs done");

        self.data_encoding
            .serialize_into(&mut buf, &key_value_pairs)
            .map_err(|err| {
                error!(%err, "serialize all key value pairs failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!("serialize all key value pairs to snapshot data done");

        let new_snapshot_data = buf.into_inner().freeze();

        write_batch.put(SNAPSHOT_DATA_PATH, new_snapshot_data.clone());

        debug!("insert new snapshot data to write batch done");

        self.db.write(write_batch).map_err(|err| {
            error!(%err, "the whole update snapshot operation failed");

            Error::Io(io::Error::new(ErrorKind::Other, err))
        })?;

        info!("the whole update snapshot operation done");

        Ok(Snapshot {
            data: new_snapshot_data,
            metadata: new_snapshot_metadata,
        })
    }

    #[instrument(skip(self), err)]
    fn hard_state(&self) -> Result<HardState, Error> {
        let hard_state = self.db.get_pinned(HARD_STATE_PATH).map_err(|err| {
            error!(%err, "get hard state failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        let hard_state = match hard_state {
            None => {
                error!("rocksdb has no hard state");

                return Err(Error::ConfigInvalid(
                    "rocksdb has no hard state".to_string(),
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

    #[instrument(skip(self), err)]
    fn config_state(&self) -> Result<ConfigState, Error> {
        let config_state = self.db.get_pinned(CONFIG_STATE_PATH).map_err(|err| {
            error!(%err, "get config state failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        let config_state = match config_state {
            None => {
                error!("rocksdb has no config state");

                return Err(Error::ConfigInvalid(
                    "rocksdb has no config state".to_string(),
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
}

impl<KV> LogBackend for RocksdbBackend<KV>
where
    KV: KeyValueBackend,
    KV::Error: Into<Error>,
{
    type Error = Error;

    #[instrument(skip(self, entries), err)]
    fn append_entries(&mut self, entries: Vec<Entry>) -> Result<(), Self::Error> {
        if entries.is_empty() {
            debug!("entries is empty");

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

        info!("get entries column family handle done");

        let mut write_batch = WriteBatch::default();

        for delete_index in first_entry_index..=last_index {
            let delete_entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, delete_index);

            debug!(delete_index, %delete_entry_path, "delete entry path");

            if !self
                .db
                .key_may_exist_cf(column_family_handle, &delete_entry_path)
            {
                info!(delete_index, %delete_entry_path, "entry definitely doesn't exist");

                break;
            }

            write_batch.delete_cf(column_family_handle, &delete_entry_path);

            debug!(delete_index, %delete_entry_path, "delete entry in write batch done");
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

            debug!(entry_index, %entry_path, "insert entry path");

            write_batch.put_cf(column_family_handle, &entry_path, entry);

            debug!(entry_index, %entry_path, "insert entry in write batch done");
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
            error!(%err, "the whole append entries operation failed");

            Error::Store(StorageError::Other(err.into()))
        })?;

        info!(
            "delete will be covered entries done, append entries done and update last index done"
        );

        Ok(())
    }

    #[instrument(skip(self), err)]
    fn apply_config_state(&mut self, config_state: ConfigState) -> Result<(), Self::Error> {
        self.apply_config_state_with_batch(&config_state, None, &mut BytesMut::new())
    }

    #[instrument(skip(self), err)]
    fn apply_hard_state(&mut self, hard_state: HardState) -> Result<(), Self::Error> {
        self.apply_hard_state_with_batch(&hard_state, None, &mut BytesMut::new())
    }

    #[instrument(skip(self), err)]
    fn apply_commit_index(&mut self, commit_index: u64) -> Result<(), Self::Error> {
        let last_index = self.last_index()?;

        info!(last_index, "get last index done");

        if last_index < commit_index {
            error!(last_index, commit_index, "last index < commit index");

            return Err(Error::Store(StorageError::Unavailable));
        }

        let mut hard_state = self.hard_state()?;

        info!(?hard_state, "get hard state done");

        hard_state.commit = commit_index;

        self.apply_hard_state(hard_state.clone())?;

        info!(new_hard_state = ?hard_state, "apply new hard state done");

        Ok(())
    }

    #[instrument(skip(self, snapshot), err)]
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Self::Error> {
        let snapshot_index = snapshot.metadata.index;
        let snapshot_term = snapshot.metadata.term;

        let first_index = self.first_index()?;

        info!(first_index, "get first index done");

        let last_index = self.last_index()?;

        info!(last_index, "get last index done");

        if first_index > snapshot_index {
            error!(snapshot_index, first_index, "first index > snapshot index");

            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let mut hard_state = self.hard_state()?;

        hard_state.commit = snapshot_index;
        hard_state.term = hard_state.term.max(snapshot_term);

        let Snapshot {
            data: snapshot_data,
            metadata: snapshot_metadata,
        } = snapshot;

        let key_value_pairs: Vec<KeyValuePair> = self
            .data_encoding
            .deserialize(&snapshot_data)
            .map_err(|err| {
                error!(%err, "deserialize snapshot data failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!("deserialize snapshot data done");

        let mut write_batch = WriteBatch::default();
        let mut buf = BytesMut::with_capacity(snapshot_data.len());

        self.apply_hard_state_with_batch(&hard_state, Some(&mut write_batch), &mut buf)?;

        debug!(new_hard_state = ?hard_state, "insert new hard state to write batch done");

        if let Some(config_state) = &snapshot_metadata.config_state {
            self.apply_config_state_with_batch(config_state, Some(&mut write_batch), &mut buf)?;

            debug!(new_config_state = ?config_state, "insert new config state to write batch done");
        }

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        debug!("get entries column family handle done");

        for delete_index in first_index..=snapshot_index {
            let delete_entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, delete_index);

            info!(delete_index, %delete_entry_path, "delete entry path");

            write_batch.delete_cf(column_family_handle, &delete_entry_path);

            info!(delete_index, %delete_entry_path, "delete entry to write batch done");
        }

        self.data_encoding
            .serialize_into((&mut buf).writer(), &snapshot_metadata)
            .map_err(|err| {
                error!(%err, ?snapshot_metadata, "serialize snapshot metadata failed");

                Error::Store(StorageError::Other(err))
            })?;

        info!(?snapshot_metadata, "serialize snapshot metadata done");

        let snapshot_metadata_data = buf.split();

        write_batch.put(SNAPSHOT_METADATA_PATH, snapshot_metadata_data);

        debug!(
            ?snapshot_metadata,
            "insert snapshot metadata to write batch done"
        );

        write_batch.put(SNAPSHOT_DATA_PATH, snapshot_data);

        debug!("insert snapshot data to write batch done");

        self.db.write(write_batch).map_err(|err| {
            error!(%err, "the whole apply snapshot operation failed");

            Error::Io(io::Error::new(ErrorKind::Other, err))
        })?;

        info!("insert new hard state done, delete compacted entries done, insert snapshot metadata done and insert snapshot data done");

        self.kv_backend
            .apply_key_value_pairs(key_value_pairs.into_iter().collect())
            .map_err(Into::into)?;

        info!("apply key value pairs to key value backend done");

        Ok(())
    }
}

impl<KV> Storage for RocksdbBackend<KV>
where
    KV: KeyValueBackend,
    KV::Error: Into<Error>,
{
    #[instrument(skip(self), err)]
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.hard_state()?;

        info!(?hard_state, "get initial hard state done");

        let config_state = self.config_state()?;

        info!(?config_state, "get initial config state done");

        Ok(RaftState {
            hard_state: hard_state.into(),
            conf_state: config_state.into(),
        })
    }

    #[instrument(skip(self, max_size), err)]
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<eraftpb::Entry>> {
        let last_index = self.last_index()?;

        info!(last_index, "get last index done");

        assert!(high <= last_index + 1);

        let max_size = max_size.into().unwrap_or(u64::MAX);

        let first_index = self.first_index()?;

        info!(first_index, "get first index done");

        if low < first_index {
            error!(low, first_index, "low index < first index");

            return Err(Error::Store(StorageError::Compacted));
        }

        let mut total_size = 0;
        let mut entries = Vec::with_capacity((high - low) as _);

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        debug!("get entries column family handle done");

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

            entries.push(entry.into());
        }

        info!(low, high, max_size, total_size, "get entries done");

        Ok(entries)
    }

    #[instrument(skip(self), err)]
    fn term(&self, idx: u64) -> raft::Result<u64> {
        let snapshot_metadata = self.get_snapshot_metadata()?;

        info!(?snapshot_metadata, "get snapshot metadata done");

        if snapshot_metadata.index == idx {
            debug!(idx, "the entry index == snapshot index");

            return Ok(snapshot_metadata.term);
        }

        let column_family_handle = self.get_column_family_handle(ENTRIES_COLUMN_FAMILY)?;

        debug!(
            entries_clomn_family = ENTRIES_COLUMN_FAMILY,
            "get entries column family handle done"
        );

        let entry_path = format!("{}{}", ENTRIES_PATH_PREFIX, idx);

        debug!(idx, %entry_path, "get entry path done");

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

    #[instrument(skip(self), err)]
    fn first_index(&self) -> raft::Result<u64> {
        let snapshot_metadata = self.get_snapshot_metadata()?;

        info!(?snapshot_metadata, "get snapshot metadata done");

        // the first entry index is always snapshot index+1, because the log is continuous
        Ok(snapshot_metadata.index + 1)
    }

    #[instrument(skip(self), err)]
    fn last_index(&self) -> raft::Result<u64> {
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
                let last_index = self.data_encoding.deserialize(&last_index).map_err(|err| {
                    error!(%err, "deserialize last index failed");

                    Error::Store(StorageError::Other(err.into()))
                })?;

                Ok(last_index)
            }
        }
    }

    #[instrument(skip(self), err)]
    fn snapshot(&self, request_index: u64) -> raft::Result<eraftpb::Snapshot> {
        let snapshot_metadata = self.get_snapshot_metadata()?;

        info!("get snapshot metadata done");

        if snapshot_metadata.index == request_index {
            debug!(
                request_index,
                snapshot_index = snapshot_metadata.index,
                "request index == snapshot index"
            );

            let snapshot_data = self.db.get(SNAPSHOT_DATA_PATH).map_err(|err| {
                error!(%err, "get snapshot data failed");

                Error::Store(StorageError::Other(err.into()))
            })?;

            return match snapshot_data {
                None => {
                    error!("snapshot data not exists");

                    Err(Error::Store(StorageError::Other(
                        "snapshot data not exist".into(),
                    )))
                }

                Some(snapshot_data) => Ok(eraftpb::Snapshot {
                    data: snapshot_data,
                    metadata: Some(snapshot_metadata.into()),
                }),
            };
        }

        self.create_snapshot(request_index).map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};
    use std::env;
    use std::rc::Rc;

    use bytes::Bytes;
    use once_cell::sync::OnceCell;
    use tempfile::TempDir;

    use super::*;
    use crate::storage::key_value::{KeyValueOperation, KeyValuePair, Operation};
    use crate::storage::log::EntryType;

    fn init_log() {
        static INIT_LOG: OnceCell<()> = OnceCell::new();

        INIT_LOG.get_or_init(|| {
            crate::init_log();
        });
    }

    fn encoding() -> impl Options + Copy {
        DefaultOptions::new()
            .with_big_endian()
            .with_varint_encoding()
    }

    fn create_backend(path: &Path) -> RocksdbBackend<TestKVBackend> {
        create_backend_with_config_state(path, Default::default())
    }

    fn create_backend_with_config_state(
        path: &Path,
        config_state: ConfigState,
    ) -> RocksdbBackend<TestKVBackend> {
        RocksdbBackend::create(path, config_state, TestKVBackend::default()).unwrap()
    }

    fn create_backend_with_kv_backend_and_config_state<KV>(
        path: &Path,
        config_state: ConfigState,
        kv: KV,
    ) -> RocksdbBackend<KV>
    where
        KV: KeyValueBackend,
        KV::Error: Into<Error>,
    {
        RocksdbBackend::create(path, config_state, kv).unwrap()
    }

    #[test]
    fn test_create() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        create_backend(tmp_dir.path());
    }

    #[test]
    fn test_from_exist() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        create_backend(tmp_dir.path());

        RocksdbBackend::from_exist(
            tmp_dir.path(),
            ConfigState::default(),
            TestKVBackend::default(),
        )
        .unwrap();
    }

    #[test]
    fn test_first_index() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let backend = create_backend(tmp_dir.path());

        assert_eq!(backend.first_index().unwrap(), 1);
    }

    #[test]
    fn test_last_index() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let backend = create_backend(tmp_dir.path());

        assert_eq!(backend.last_index().unwrap(), 0);
    }

    #[test]
    fn test_hard_state() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let backend = create_backend(tmp_dir.path());

        let hard_state = backend.hard_state().unwrap();

        assert_eq!(hard_state, HardState::default())
    }

    #[test]
    fn test_config_state() {
        init_log();

        let config_state = ConfigState {
            voters: vec![1, 2, 3],
            learners: vec![],
            voters_outgoing: vec![],
            learners_next: vec![],
            auto_leave: false,
        };

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let backend = create_backend_with_config_state(tmp_dir.path(), config_state.clone());

        assert_eq!(backend.config_state().unwrap(), config_state)
    }

    #[test]
    fn test_append_entries() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();
    }

    #[test]
    fn test_term() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        assert_eq!(backend.term(1).unwrap(), 1);
        assert_eq!(backend.term(2).unwrap(), 1);
        assert!(backend.term(3).is_err());
    }

    #[test]
    fn test_first_index_after_append_entries() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        assert_eq!(backend.first_index().unwrap(), 1);
    }

    #[test]
    fn test_last_index_after_append_entries() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        assert_eq!(backend.last_index().unwrap(), 2);
    }

    #[test]
    fn test_apply_hard_state() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        let hard_state = HardState {
            term: 1,
            vote: 0,
            commit: 1,
        };

        backend.apply_hard_state(hard_state.clone()).unwrap();

        assert_eq!(backend.hard_state().unwrap(), hard_state);
    }

    #[test]
    fn test_apply_config_state() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        let mut config_state = backend.config_state().unwrap();
        config_state.voters.push(1);

        backend.apply_config_state(config_state.clone()).unwrap();

        assert_eq!(backend.config_state().unwrap(), config_state);
    }

    #[test]
    fn test_apply_commit_index() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        let mut hard_state = backend.hard_state().unwrap();

        backend.apply_commit_index(2).unwrap();

        hard_state.commit = 2;

        assert_eq!(backend.hard_state().unwrap(), hard_state);
    }

    #[test]
    fn test_entries() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        let entries = backend.entries(1, 3, None).unwrap();
        let entries = entries
            .into_iter()
            .map(|entry| entry.try_into())
            .collect::<Result<Vec<Entry>, _>>()
            .unwrap();

        assert_eq!(
            entries,
            vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: Default::default(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: Default::default(),
                    context: Default::default(),
                },
            ]
        );

        let entries = backend.entries(1, 1, None).unwrap();
        assert!(entries.is_empty());

        let entries = backend.entries(1, 2, None).unwrap();
        let entries = entries
            .into_iter()
            .map(|entry| entry.try_into())
            .collect::<Result<Vec<Entry>, _>>()
            .unwrap();
        assert_eq!(
            entries,
            vec![Entry {
                entry_type: EntryType::EntryNormal,
                term: 1,
                index: 1,
                data: Default::default(),
                context: Default::default(),
            }]
        );

        let entries = backend.entries(2, 3, None).unwrap();
        let entries = entries
            .into_iter()
            .map(|entry| entry.try_into())
            .collect::<Result<Vec<Entry>, _>>()
            .unwrap();
        assert_eq!(
            entries,
            vec![Entry {
                entry_type: EntryType::EntryNormal,
                term: 1,
                index: 2,
                data: Default::default(),
                context: Default::default(),
            }]
        )
    }

    #[derive(Default, Clone)]
    struct TestKVBackend {
        map: Rc<RefCell<HashMap<Bytes, Bytes>>>,
    }

    impl KeyValueBackend for TestKVBackend {
        type Error = raft::Error;

        fn apply_key_value_operation(
            &mut self,
            operations: Vec<KeyValueOperation>,
        ) -> Result<(), Self::Error> {
            for operation in operations {
                match operation.operation {
                    Operation::InsertOrUpdate => {
                        self.map.borrow_mut().insert(operation.key, operation.value);
                    }
                    Operation::Delete => {
                        self.map.borrow_mut().remove(&operation.key);
                    }
                }
            }

            Ok(())
        }

        fn get<S: AsRef<[u8]>>(&self, key: S) -> Result<Option<Bytes>, Self::Error> {
            Ok(self.map.borrow_mut().get(key.as_ref()).cloned())
        }

        fn apply_key_value_pairs(
            &mut self,
            pairs: HashSet<KeyValuePair>,
        ) -> Result<(), Self::Error> {
            self.map.borrow_mut().clear();

            for pair in pairs {
                self.map.borrow_mut().insert(pair.key, pair.value);
            }

            Ok(())
        }

        fn all(&self) -> Result<Vec<KeyValuePair>, Self::Error> {
            Ok(self
                .map
                .borrow()
                .iter()
                .map(|(key, value)| KeyValuePair::new(key.clone(), value.clone()))
                .collect())
        }
    }

    #[test]
    fn test_snapshot() {
        init_log();

        let mut kv_backend = TestKVBackend::default();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend_with_kv_backend_and_config_state(
            tmp_dir.path(),
            ConfigState::default(),
            kv_backend.clone(),
        );

        let encoding = encoding();

        let kv_op1 = KeyValueOperation::new(
            Operation::InsertOrUpdate,
            b"test1".as_slice(),
            b"test1".as_slice(),
        );

        let kv_op2 = KeyValueOperation::new(
            Operation::InsertOrUpdate,
            b"test2".as_slice(),
            b"test2".as_slice(),
        );

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: encoding.serialize(&kv_op1).unwrap().into(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: encoding.serialize(&kv_op2).unwrap().into(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        let mut hard_state = backend.hard_state().unwrap();
        hard_state.commit = 2;
        hard_state.term = 1;

        backend.apply_hard_state(hard_state).unwrap();

        kv_backend
            .apply_key_value_operation(vec![kv_op1, kv_op2])
            .unwrap();

        let snapshot = backend.snapshot(2).unwrap();

        assert_eq!(
            SnapshotMetadata::from(snapshot.metadata.unwrap()),
            SnapshotMetadata {
                config_state: Some(ConfigState::default()),
                index: 2,
                term: 1,
            }
        );

        let key_value_pairs: Vec<KeyValuePair> = encoding.deserialize(&snapshot.data).unwrap();

        // we use key: Bytes to implement PartialEq
        #[allow(clippy::mutable_key_type)]
        let key_value_pairs = key_value_pairs.into_iter().collect::<HashSet<_>>();

        assert_eq!(
            key_value_pairs,
            HashSet::from([
                KeyValuePair::new(b"test1".as_slice(), b"test1".as_slice()),
                KeyValuePair::new(b"test2".as_slice(), b"test2".as_slice()),
            ])
        )
    }

    #[test]
    fn test_apply_snapshot() {
        init_log();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut backend = create_backend(tmp_dir.path());

        let encoding = encoding();

        let kv_op1 = KeyValueOperation::new(
            Operation::InsertOrUpdate,
            b"test1".as_slice(),
            b"test1".as_slice(),
        );

        let kv_op2 = KeyValueOperation::new(
            Operation::InsertOrUpdate,
            b"test2".as_slice(),
            b"test2".as_slice(),
        );

        backend
            .append_entries(vec![
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 1,
                    data: encoding.serialize(&kv_op1).unwrap().into(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 2,
                    data: encoding.serialize(&kv_op2).unwrap().into(),
                    context: Default::default(),
                },
                Entry {
                    entry_type: EntryType::EntryNormal,
                    term: 1,
                    index: 3,
                    data: encoding.serialize(&kv_op2).unwrap().into(),
                    context: Default::default(),
                },
            ])
            .unwrap();

        let pairs = vec![
            KeyValuePair::new(b"test11".as_slice(), b"test11".as_slice()),
            KeyValuePair::new(b"test22".as_slice(), b"test22".as_slice()),
        ];

        let snapshot_data = encoding.serialize(&pairs).unwrap();

        let snapshot = Snapshot {
            data: snapshot_data.into(),
            metadata: SnapshotMetadata {
                config_state: Some(ConfigState {
                    voters: vec![1],
                    ..Default::default()
                }),
                index: 2,
                term: 2,
            },
        };

        backend.apply_snapshot(snapshot).unwrap();

        assert_eq!(backend.first_index().unwrap(), 3);
        assert_eq!(backend.last_index().unwrap(), 3);

        let entries = backend.entries(3, 4, None).unwrap();
        let entries = entries
            .into_iter()
            .map(|entry| entry.try_into())
            .collect::<Result<Vec<Entry>, _>>()
            .unwrap();

        assert_eq!(
            entries,
            vec![Entry {
                entry_type: EntryType::EntryNormal,
                term: 1,
                index: 3,
                data: encoding.serialize(&kv_op2).unwrap().into(),
                context: Default::default(),
            }]
        );

        assert_eq!(backend.term(3).unwrap(), 1);

        assert_eq!(
            backend.hard_state().unwrap(),
            HardState {
                term: 2,
                vote: 0,
                commit: 2,
            }
        );

        assert_eq!(
            backend.config_state().unwrap(),
            ConfigState {
                voters: vec![1],
                ..Default::default()
            }
        );
    }
}
