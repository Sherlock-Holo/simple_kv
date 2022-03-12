use bytes::Bytes;
use flume::{Receiver, Sender};
use thiserror::Error;

use crate::storage::key_value::KeyValueOperation;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("this node is not leader, leader node id is {0}")]
    NotLeader(u64),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub struct ProposalRequestReply {
    key_value_operation: Option<KeyValueOperation>,
    result_sender: Sender<Result<(), RequestError>>,
}

impl ProposalRequestReply {
    pub fn new(
        key_value_operation: KeyValueOperation,
    ) -> (Self, Receiver<Result<(), RequestError>>) {
        let (result_sender, result_receiver) = flume::bounded(1);

        (
            Self {
                key_value_operation: Some(key_value_operation),
                result_sender,
            },
            result_receiver,
        )
    }

    pub fn handle(&mut self) -> KeyValueOperation {
        self.key_value_operation
            .take()
            .expect("key value operation is handled")
    }

    pub fn reply(self) {
        assert!(
            self.key_value_operation.is_none(),
            "key value operation is not handled"
        );

        let _ = self.result_sender.send(Ok(()));
    }

    pub fn reply_err(self, err: RequestError) {
        let _ = self.result_sender.send(Err(err));
    }
}

pub struct GetRequestReply {
    key: Bytes,
    result_sender: Sender<Result<Option<Bytes>, RequestError>>,
}

impl GetRequestReply {
    pub fn new(key: impl Into<Bytes>) -> (Self, Receiver<Result<Option<Bytes>, RequestError>>) {
        let (result_sender, result_receiver) = flume::bounded(1);

        (
            Self {
                key: key.into(),
                result_sender,
            },
            result_receiver,
        )
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn reply(self, value: Option<Bytes>) {
        let _ = self.result_sender.send(Ok(value));
    }

    pub fn reply_err(self, err: RequestError) {
        let _ = self.result_sender.send(Err(err));
    }
}
