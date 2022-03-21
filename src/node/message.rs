use std::time::{Duration, Instant};

use flume::select::SelectError;
use flume::{Receiver, RecvError, Selector};
use raft::eraftpb;

use crate::reply::{GetRequestReply, ProposalRequestReply};
use crate::rpc::register::NodeChangeEvent;

pub enum ReceiveMessage {
    Raft(Result<eraftpb::Message, RecvError>),
    GetRequest(Result<GetRequestReply, RecvError>),
    ProposalRequest(Result<ProposalRequestReply, RecvError>),
    NodeChangeEvent(Result<NodeChangeEvent, RecvError>),
}

impl ReceiveMessage {
    pub fn split(
        all_messages: Vec<Self>,
    ) -> (
        Vec<eraftpb::Message>,
        Vec<GetRequestReply>,
        Option<ProposalRequestReply>,
        Vec<NodeChangeEvent>,
    ) {
        let mut raft_messages = vec![];
        let mut get_request_reply_messages = vec![];
        let mut proposal_request_reply = None;
        let mut node_change_events = vec![];

        for message in all_messages {
            match message {
                ReceiveMessage::Raft(Ok(msg)) => raft_messages.push(msg),
                ReceiveMessage::GetRequest(Ok(msg)) => get_request_reply_messages.push(msg),
                ReceiveMessage::ProposalRequest(Ok(msg)) => {
                    proposal_request_reply.replace(msg);
                }
                ReceiveMessage::NodeChangeEvent(Ok(msg)) => node_change_events.push(msg),

                _ => {}
            }
        }

        (
            raft_messages,
            get_request_reply_messages,
            proposal_request_reply,
            node_change_events,
        )
    }
}

pub struct ContinueSelector<'a> {
    raft_message_receiver: &'a Receiver<eraftpb::Message>,
    get_request_receiver: &'a Receiver<GetRequestReply>,
    proposal_request_receiver: &'a Receiver<ProposalRequestReply>,
    node_change_event_receiver: Option<&'a Receiver<NodeChangeEvent>>,
}

impl<'a> ContinueSelector<'a> {
    pub fn new(
        raft_message_receiver: &'a Receiver<eraftpb::Message>,
        get_request_receiver: &'a Receiver<GetRequestReply>,
        proposal_request_receiver: &'a Receiver<ProposalRequestReply>,
        node_change_event_receiver: Option<&'a Receiver<NodeChangeEvent>>,
    ) -> Self {
        Self {
            raft_message_receiver,
            get_request_receiver,
            proposal_request_receiver,
            node_change_event_receiver,
        }
    }

    pub fn wait_deadline(self, deadline: Instant) -> Result<Vec<ReceiveMessage>, SelectError> {
        let mut messages = vec![];

        let mut has_proposal_request = false;

        loop {
            let mut selector = Selector::new()
                .recv(self.raft_message_receiver, ReceiveMessage::Raft)
                .recv(self.get_request_receiver, ReceiveMessage::GetRequest);

            if let Some(node_change_event_receiver) = self.node_change_event_receiver {
                selector =
                    selector.recv(node_change_event_receiver, ReceiveMessage::NodeChangeEvent);
            }

            if !has_proposal_request {
                selector = selector.recv(
                    self.proposal_request_receiver,
                    ReceiveMessage::ProposalRequest,
                );
            }

            match selector.wait_deadline(deadline) {
                Err(err) if messages.is_empty() => return Err(err),
                Err(_) => return Ok(messages),
                Ok(message) => {
                    has_proposal_request = matches!(&message, ReceiveMessage::ProposalRequest(_));

                    messages.push(message);
                }
            }
        }
    }

    pub fn wait_timeout(self, timeout: Duration) -> Result<Vec<ReceiveMessage>, SelectError> {
        let deadline = Instant::now() + timeout;

        self.wait_deadline(deadline)
    }
}
