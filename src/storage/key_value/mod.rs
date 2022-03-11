use std::collections::HashSet;
use std::error::Error;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use serde::de::{Unexpected, Visitor};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

pub mod rocksdb;

#[derive(Debug, Error)]
#[error("invalid operation {0}")]
pub struct OperationError(pub u8);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Operation {
    InsertOrUpdate,
    Delete,
}

impl From<Operation> for u8 {
    fn from(op: Operation) -> Self {
        match op {
            Operation::InsertOrUpdate => 1,
            Operation::Delete => 2,
        }
    }
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let op = (*self).into();

        serializer.serialize_u8(op)
    }
}

struct OperationVisitor;

impl<'de> Visitor<'de> for OperationVisitor {
    type Value = Operation;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("an operation should be 1 or 2")
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match Operation::try_from(v) {
            Ok(op) => Ok(op),
            Err(err) => Err(E::invalid_value(Unexpected::Unsigned(err.0 as _), &self)),
        }
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u8(OperationVisitor)
    }
}

impl TryFrom<u8> for Operation {
    type Error = OperationError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Operation::InsertOrUpdate),
            2 => Ok(Operation::Delete),

            invalid => Err(OperationError(invalid)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueOperation {
    pub operation: Operation,
    pub key: Bytes,
    pub value: Bytes,
}

impl KeyValueOperation {
    pub fn new(operation: Operation, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            operation,
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: Bytes,
    pub value: Bytes,
}

impl KeyValuePair {
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl PartialEq for KeyValuePair {
    fn eq(&self, other: &Self) -> bool {
        // only compare the key because in a key-value database, definitely not exists two
        // key-value which key equal but value not equal
        self.key == other.key
    }
}

impl Eq for KeyValuePair {}

impl Hash for KeyValuePair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

pub trait KeyValueBackend {
    type Error: Error + Send + Sync + 'static;

    /// apply the key value operation to the database backend
    fn apply_key_value_operation(
        &mut self,
        operations: Vec<KeyValueOperation>,
    ) -> Result<(), Self::Error>;

    /// get the value by key
    fn get<S: AsRef<[u8]>>(&mut self, key: S) -> Result<Option<Bytes>, Self::Error>;

    // we use the KeyValuePair.key(String) to implement the PartialEq and Hash
    #[allow(clippy::mutable_key_type)]
    /// apply the key value pairs to the backend, before it, make sure clear the original all key
    /// value pairs
    fn apply_key_value_pairs(&mut self, pairs: HashSet<KeyValuePair>) -> Result<(), Self::Error>;

    /// get all key value pairs
    fn all(&self) -> Result<Vec<KeyValuePair>, Self::Error>;
}
