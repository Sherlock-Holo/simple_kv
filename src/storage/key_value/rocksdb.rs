use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use raft::StorageError;
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use tap::TapFallible;
use thiserror::Error;
use tracing::{debug, error, info, instrument};

use crate::storage::key_value::{KeyValueBackend, KeyValueOperation, KeyValuePair, Operation};

#[derive(Debug, Error)]
#[error("{0}")]
pub struct Error(rocksdb::Error);

impl From<Error> for raft::Error {
    fn from(err: Error) -> Self {
        raft::Error::Store(StorageError::Other(err.into()))
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        Self(err)
    }
}

#[derive(Clone)]
pub struct RocksdbBackend {
    db: Arc<DB>,
}

impl RocksdbBackend {
    pub fn from_exist(path: &Path) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(false);

        let db = DB::open(&opts, path).tap_err(|err| error!(%err, ?path, "open rocksdb failed"))?;

        info!(?path, "open exist rocksdb done");

        Ok(RocksdbBackend { db: Arc::new(db) })
    }

    pub fn create(path: &Path) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db =
            DB::open(&opts, path).tap_err(|err| error!(%err, ?path, "create rocksdb failed"))?;

        info!(?path, "create rocksdb done");

        Ok(RocksdbBackend { db: Arc::new(db) })
    }
}

impl KeyValueBackend for RocksdbBackend {
    type Error = Error;

    #[instrument(skip(self, operations), err)]
    fn apply_key_value_operation(
        &mut self,
        operations: Vec<KeyValueOperation>,
    ) -> Result<(), Self::Error> {
        let mut write_batch = WriteBatch::default();

        for op in operations {
            match op.operation {
                Operation::InsertOrUpdate => {
                    write_batch.put(op.key, op.value);

                    debug!("insert or update key-value to write batch done");
                }

                Operation::Delete => {
                    write_batch.delete(op.key);

                    debug!("delete key-value to write batch done");
                }
            }
        }

        info!("all operation are handled in write batch");

        self.db
            .write(write_batch)
            .tap_err(|err| error!(%err, "apply key value operation failed"))?;

        Ok(())
    }

    #[instrument(skip(self, key), err)]
    fn get<S: AsRef<[u8]>>(&self, key: S) -> Result<Option<Bytes>, Self::Error> {
        let result = self
            .db
            .get(key)
            .tap_err(|err| error!(%err, "get value by key failed"))?;

        Ok(result.map(Into::into))
    }

    #[instrument(skip(self, pairs), err)]
    fn apply_key_value_pairs(&mut self, pairs: HashSet<KeyValuePair>) -> Result<(), Self::Error> {
        let mut write_batch = WriteBatch::default();

        for key in self
            .db
            .iterator(IteratorMode::Start)
            .map(|(key, _)| Bytes::from(key))
        {
            write_batch.delete(key);
        }

        debug!("delete all exists key-value to write batch done");

        for pair in pairs {
            write_batch.put(pair.key, pair.value);

            debug!("insert key-value to write batch done");
        }

        info!("insert all key-value to write batch done");

        self.db
            .write(write_batch)
            .tap_err(|err| error!(%err, "apply key value pairs failed"))?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    fn all(&self) -> Result<Vec<KeyValuePair>, Self::Error> {
        let pairs = self
            .db
            .iterator(IteratorMode::Start)
            .map(|(key, value)| KeyValuePair::new(key, value))
            .collect();

        Ok(pairs)
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_create() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        RocksdbBackend::create(tmp_dir.path()).unwrap();
    }

    #[test]
    fn test_from_exist() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let backend = RocksdbBackend::create(tmp_dir.path()).unwrap();

        backend.db.put(b"test", b"test").unwrap();

        drop(backend);

        let backend = RocksdbBackend::from_exist(tmp_dir.path()).unwrap();

        assert_eq!(backend.db.get(b"test").unwrap().unwrap(), b"test");
    }

    #[test]
    fn test_apply_operation() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let mut backend = RocksdbBackend::create(tmp_dir.path()).unwrap();

        backend
            .apply_key_value_operation(vec![
                KeyValueOperation::new(
                    Operation::InsertOrUpdate,
                    b"test1".as_slice(),
                    b"test1".as_slice(),
                ),
                KeyValueOperation::new(
                    Operation::InsertOrUpdate,
                    b"test2".as_slice(),
                    b"test2".as_slice(),
                ),
            ])
            .unwrap();

        assert_eq!(backend.db.get(b"test1").unwrap().unwrap(), b"test1");
        assert_eq!(backend.db.get(b"test2").unwrap().unwrap(), b"test2");

        backend
            .apply_key_value_operation(vec![KeyValueOperation::new(
                Operation::Delete,
                b"test2".as_slice(),
                [].as_slice(),
            )])
            .unwrap();

        assert_eq!(backend.db.get(b"test1").unwrap().unwrap(), b"test1");
        assert!(backend.db.get(b"test2").unwrap().is_none());
    }

    #[test]
    fn test_get() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let mut backend = RocksdbBackend::create(tmp_dir.path()).unwrap();

        backend
            .apply_key_value_operation(vec![KeyValueOperation::new(
                Operation::InsertOrUpdate,
                b"test1".as_slice(),
                b"test1".as_slice(),
            )])
            .unwrap();

        assert_eq!(
            backend.get(b"test1".as_slice()).unwrap().unwrap(),
            b"test1".as_slice()
        );

        backend
            .apply_key_value_operation(vec![KeyValueOperation::new(
                Operation::Delete,
                b"test1".as_slice(),
                [].as_slice(),
            )])
            .unwrap();

        assert!(backend.get(b"test1").unwrap().is_none());
    }

    #[test]
    fn test_apply_key_value_pairs() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let mut backend = RocksdbBackend::create(tmp_dir.path()).unwrap();

        backend
            .apply_key_value_operation(vec![
                KeyValueOperation::new(
                    Operation::InsertOrUpdate,
                    b"test1".as_slice(),
                    b"test1".as_slice(),
                ),
                KeyValueOperation::new(
                    Operation::InsertOrUpdate,
                    b"test2".as_slice(),
                    b"test2".as_slice(),
                ),
            ])
            .unwrap();

        backend
            .apply_key_value_pairs(HashSet::from([
                KeyValuePair::new(b"test11".as_slice(), b"test11".as_slice()),
                KeyValuePair::new(b"test22".as_slice(), b"test22".as_slice()),
            ]))
            .unwrap();

        assert!(backend.get(b"test1").unwrap().is_none());
        assert!(backend.get(b"test2").unwrap().is_none());

        assert_eq!(
            backend.get(b"test11").unwrap().unwrap(),
            b"test11".as_slice()
        );
        assert_eq!(
            backend.get(b"test22").unwrap().unwrap(),
            b"test22".as_slice()
        );
    }

    #[test]
    fn test_all() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let mut backend = RocksdbBackend::create(tmp_dir.path()).unwrap();

        backend
            .apply_key_value_operation(vec![
                KeyValueOperation::new(
                    Operation::InsertOrUpdate,
                    b"test1".as_slice(),
                    b"test1".as_slice(),
                ),
                KeyValueOperation::new(
                    Operation::InsertOrUpdate,
                    b"test2".as_slice(),
                    b"test2".as_slice(),
                ),
            ])
            .unwrap();

        assert_eq!(
            backend.all().unwrap(),
            vec![
                KeyValuePair::new(b"test1".as_slice(), b"test1".as_slice()),
                KeyValuePair::new(b"test2".as_slice(), b"test2".as_slice()),
            ]
        );
    }
}
