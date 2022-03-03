use protobuf::error::WireError;
use protobuf::ProtobufError;
use raft::Error;

#[derive(Debug)]
pub enum ProposalRequest {
    CreateOrUpdate(u64, String),
    Delete(u64),
}

impl TryFrom<&str> for ProposalRequest {
    type Error = raft::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Some(key_and_value) = value.strip_prefix("CREATE_OR_UPDATE ") {
            if let Some(key_and_value) = key_and_value.strip_prefix('[') {
                let key = if let Some(pos) = key_and_value.find(']') {
                    &key_and_value[..pos]
                } else {
                    return Err(Error::CodecError(ProtobufError::WireError(
                        WireError::Other,
                    )));
                };

                let value = if let Some(pos) = key_and_value.find("] ") {
                    &key_and_value[pos + "] ".len()..]
                } else {
                    return Err(Error::CodecError(ProtobufError::WireError(
                        WireError::Other,
                    )));
                };

                let key: u64 = key
                    .parse()
                    .map_err(|_| Error::CodecError(ProtobufError::WireError(WireError::Other)))?;

                return Ok(ProposalRequest::CreateOrUpdate(key, value.to_string()));
            }

            return Err(Error::CodecError(ProtobufError::WireError(
                WireError::Other,
            )));
        }

        if let Some(key) = value.strip_prefix("DELETE ") {
            if let Some(key) = key.strip_prefix('[') {
                let key = if let Some(pos) = key.find(']') {
                    &key[..pos]
                } else {
                    return Err(Error::CodecError(ProtobufError::WireError(
                        WireError::Other,
                    )));
                };

                let key: u64 = key
                    .parse()
                    .map_err(|_| Error::CodecError(ProtobufError::WireError(WireError::Other)))?;

                return Ok(ProposalRequest::Delete(key));
            }
        }

        Err(Error::CodecError(ProtobufError::WireError(
            WireError::Other,
        )))
    }
}

impl From<ProposalRequest> for Vec<u8> {
    fn from(proposal_request: ProposalRequest) -> Self {
        match proposal_request {
            ProposalRequest::Delete(key) => format!("DELETE [{}]", key).into_bytes(),
            ProposalRequest::CreateOrUpdate(key, value) => {
                format!("CREATE_OR_UPDATE [{}] {}", key, value).into_bytes()
            }
        }
    }
}
