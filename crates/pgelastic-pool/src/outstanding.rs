//! The per-server outstanding-request queue.
//!
//! Every frontend message that draws a response is recorded here before it goes
//! to the backend, and every backend response is matched against the head of the
//! queue. That is what makes pipelining safe: the pool can inject its own
//! `Parse` and `Close` messages into a client's stream and still know, for each
//! byte coming back, whether it belongs to the client or to the pool.
//!
//! The queue emptying is also the per-batch boundary used for `query_time` and
//! `xact_time`, and it is one of the conditions of the release gate — see
//! [`crate::gate`].

use std::collections::VecDeque;

use pgelastic_wire::{BackendMessage, FrontendMessage};
use thiserror::Error;

/// What to do with the response a queued request will draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Disposition {
    /// The client asked for it; relay the response.
    Forward,
    /// The pool injected the request; swallow the response.
    Skip,
    /// The pool answers on the backend's behalf and never sends the request.
    Fake,
}

impl Disposition {
    pub fn forwards(self) -> bool {
        matches!(self, Self::Forward)
    }
}

/// How a request is being relayed, as told to the queue when it is recorded.
///
/// The input counterpart of [`Disposition`]: a faked request carries the
/// response the pool will answer with, which a plain tag cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Relay {
    Forward,
    Skip,
    Fake(Box<BackendMessage>),
}

impl Relay {
    pub fn fake(response: BackendMessage) -> Self {
        Self::Fake(Box::new(response))
    }

    pub fn disposition(&self) -> Disposition {
        match self {
            Self::Forward => Disposition::Forward,
            Self::Skip => Disposition::Skip,
            Self::Fake(_) => Disposition::Fake,
        }
    }
}

/// The frontend messages that draw a response.
///
/// `Flush` and `Terminate` are absent because they draw none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestKind {
    Parse,
    Bind,
    Describe,
    Execute,
    Close,
    Sync,
    Query,
    FunctionCall,
}

impl RequestKind {
    /// Whether `ReadyForQuery` is the message that retires this request.
    ///
    /// These are also the points an `ErrorResponse` stops discarding at: the
    /// backend itself resynchronises at exactly the same places.
    pub fn terminates_batch(self) -> bool {
        matches!(self, Self::Sync | Self::Query | Self::FunctionCall)
    }

    /// The request kind of a frame too large to decode, from its tag byte alone.
    ///
    /// A frame over the relay's inline limit is streamed through without ever becoming a
    /// `FrontendMessage`, so `from_frontend` never sees it - and the request went unrecorded
    /// while the backend's reply for it arrived anyway. `d` (`CopyData`) must map to `None`
    /// here: COPY payload is data inside a request already recorded, and recording each chunk
    /// would desynchronise the ledger in the other direction.
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            b'P' => Some(Self::Parse),
            b'B' => Some(Self::Bind),
            b'D' => Some(Self::Describe),
            b'E' => Some(Self::Execute),
            b'C' => Some(Self::Close),
            b'S' => Some(Self::Sync),
            b'Q' => Some(Self::Query),
            b'F' => Some(Self::FunctionCall),
            _ => None,
        }
    }

    pub fn from_frontend(message: &FrontendMessage) -> Option<Self> {
        match message {
            FrontendMessage::Parse(_) => Some(Self::Parse),
            FrontendMessage::Bind(_) => Some(Self::Bind),
            FrontendMessage::Describe(_) => Some(Self::Describe),
            FrontendMessage::Execute(_) => Some(Self::Execute),
            FrontendMessage::Close(_) => Some(Self::Close),
            FrontendMessage::Sync => Some(Self::Sync),
            FrontendMessage::Query(_) => Some(Self::Query),
            FrontendMessage::FunctionCall(_) => Some(Self::FunctionCall),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OutstandingError {
    #[error("backend sent {message} with no outstanding request")]
    Underflow { message: &'static str },
    #[error("backend sent {message} but the outstanding request is {expected:?}")]
    Mismatch {
        expected: RequestKind,
        message: &'static str,
    },
}

#[derive(Debug, Clone)]
struct Entry {
    kind: RequestKind,
    disposition: Disposition,
    fake: Option<BackendMessage>,
}

/// The result of matching one backend message against the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaction {
    /// What to do with this message.
    pub disposition: Disposition,
    /// The request this message retired, if any.
    pub popped: Option<RequestKind>,
    /// The queue drained to empty, closing a query/transaction timing batch.
    pub batch_ended: bool,
}

/// The queue of requests sent to a backend and not yet answered.
#[derive(Debug, Default)]
pub struct OutstandingQueue {
    entries: VecDeque<Entry>,
    query_failed: bool,
}

impl OutstandingQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether the current batch has seen an `ErrorResponse`.
    ///
    /// Cleared by the `ReadyForQuery` that ends the batch.
    pub fn query_failed(&self) -> bool {
        self.query_failed
    }

    pub fn kinds(&self) -> impl Iterator<Item = RequestKind> {
        self.entries.iter().map(|entry| entry.kind)
    }

    /// Records a request sent on the client's behalf.
    pub fn forward(&mut self, kind: RequestKind) {
        self.push(kind, Disposition::Forward, None);
    }

    /// Records a request the pool injected; its response is swallowed.
    pub fn skip(&mut self, kind: RequestKind) {
        self.push(kind, Disposition::Skip, None);
    }

    /// Records a request the pool answers itself and never sends to the backend.
    ///
    /// The entry still occupies its place in the queue so that the synthesised
    /// response reaches the client in the order the client asked for it, which
    /// is the only thing that makes it safe under pipelining.
    pub fn fake(&mut self, kind: RequestKind, response: BackendMessage) {
        self.push(kind, Disposition::Fake, Some(response));
    }

    /// Records a request however it is being relayed.
    pub fn record(&mut self, kind: RequestKind, relay: Relay) {
        match relay {
            Relay::Forward => self.forward(kind),
            Relay::Skip => self.skip(kind),
            Relay::Fake(response) => self.fake(kind, *response),
        }
    }

    fn push(&mut self, kind: RequestKind, disposition: Disposition, fake: Option<BackendMessage>) {
        self.entries.push_back(Entry {
            kind,
            disposition,
            fake,
        });
    }

    /// Removes and returns the synthesised responses that have reached the head.
    ///
    /// Call after every push and every [`apply`](Self::apply); a fake entry is
    /// answerable exactly when every request queued ahead of it has been.
    pub fn take_ready_fakes(&mut self) -> Vec<BackendMessage> {
        let mut ready = Vec::new();
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.disposition == Disposition::Fake)
        {
            let mut entry = self.entries.pop_front().expect("front was just observed");
            if let Some(response) = entry.fake.take() {
                ready.push(response);
            }
        }
        ready
    }

    /// Matches a backend message against the queue.
    pub fn apply(&mut self, message: &BackendMessage) -> Result<Reaction, OutstandingError> {
        if is_asynchronous(message) {
            return Ok(Reaction {
                disposition: Disposition::Forward,
                popped: None,
                batch_ended: false,
            });
        }

        match message {
            BackendMessage::ParseComplete => {
                self.pop_expecting(RequestKind::Parse, "ParseComplete")
            }
            BackendMessage::BindComplete => self.pop_expecting(RequestKind::Bind, "BindComplete"),
            BackendMessage::CloseComplete => {
                self.pop_expecting(RequestKind::Close, "CloseComplete")
            }
            BackendMessage::RowDescription(_) | BackendMessage::NoData => {
                Ok(self.pop_if_head(RequestKind::Describe))
            }
            BackendMessage::CommandComplete(_)
            | BackendMessage::EmptyQueryResponse
            | BackendMessage::PortalSuspended => Ok(self.pop_if_head(RequestKind::Execute)),
            BackendMessage::ReadyForQuery(_) => self.pop_batch_terminator(),
            BackendMessage::ErrorResponse(_) => Ok(self.fail_until_terminator()),
            _ => Ok(self.passthrough()),
        }
    }

    /// A message that retires nothing inherits the disposition of the request
    /// whose response it is part of — the head of the queue.
    fn passthrough(&self) -> Reaction {
        Reaction {
            disposition: self.head_disposition(),
            popped: None,
            batch_ended: false,
        }
    }

    fn head_disposition(&self) -> Disposition {
        self.entries
            .front()
            .map_or(Disposition::Forward, |entry| entry.disposition)
    }

    fn pop_expecting(
        &mut self,
        expected: RequestKind,
        message: &'static str,
    ) -> Result<Reaction, OutstandingError> {
        match self.entries.front() {
            None => Err(OutstandingError::Underflow { message }),
            Some(entry) if entry.kind != expected => Err(OutstandingError::Mismatch {
                expected: entry.kind,
                message,
            }),
            Some(_) => Ok(self.pop_head(expected)),
        }
    }

    /// Pops the head only if it is the expected kind.
    ///
    /// `RowDescription` and `CommandComplete` arrive in both the simple and the
    /// extended protocol. In the simple protocol they belong to a `Query` that
    /// only `ReadyForQuery` retires, so popping unconditionally would desync the
    /// queue on the very first `SELECT`.
    fn pop_if_head(&mut self, expected: RequestKind) -> Reaction {
        if self
            .entries
            .front()
            .is_some_and(|entry| entry.kind == expected)
        {
            self.pop_head(expected)
        } else {
            self.passthrough()
        }
    }

    fn pop_head(&mut self, kind: RequestKind) -> Reaction {
        let entry = self.entries.pop_front().expect("head was just observed");
        Reaction {
            disposition: entry.disposition,
            popped: Some(kind),
            batch_ended: self.entries.is_empty(),
        }
    }

    fn pop_batch_terminator(&mut self) -> Result<Reaction, OutstandingError> {
        self.query_failed = false;
        match self.entries.front() {
            None => Err(OutstandingError::Underflow {
                message: "ReadyForQuery",
            }),
            Some(entry) if !entry.kind.terminates_batch() => Err(OutstandingError::Mismatch {
                expected: entry.kind,
                message: "ReadyForQuery",
            }),
            Some(entry) => {
                let kind = entry.kind;
                Ok(self.pop_head(kind))
            }
        }
    }

    /// Discards everything the backend will not answer after an error.
    ///
    /// The backend skips messages until the next synchronisation point, so the
    /// queue must skip exactly the same ones, leaving the terminator in place
    /// for the `ReadyForQuery` that follows.
    fn fail_until_terminator(&mut self) -> Reaction {
        self.query_failed = true;
        let disposition = self.head_disposition();
        while self
            .entries
            .front()
            .is_some_and(|entry| !entry.kind.terminates_batch())
        {
            self.entries.pop_front();
        }
        Reaction {
            disposition,
            popped: None,
            batch_ended: false,
        }
    }
}

/// Messages the backend may send at any moment, unrelated to any request.
///
/// `NotificationResponse` in particular must not disturb queue state or batch
/// timing: a `LISTEN`ing session receives them between batches, and counting
/// them corrupts both the state machine and the statistics.
fn is_asynchronous(message: &BackendMessage) -> bool {
    matches!(
        message,
        BackendMessage::NotificationResponse(_)
            | BackendMessage::NoticeResponse(_)
            | BackendMessage::ParameterStatus(_)
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pgelastic_wire::{
        Fields, NotificationResponse, ParameterStatus, RowDescription, TransactionStatus,
    };

    use super::*;

    fn command_complete(tag: &str) -> BackendMessage {
        BackendMessage::CommandComplete(Bytes::copy_from_slice(tag.as_bytes()))
    }

    fn ready(status: TransactionStatus) -> BackendMessage {
        BackendMessage::ReadyForQuery(status)
    }

    fn error() -> BackendMessage {
        BackendMessage::ErrorResponse(Fields::default())
    }

    // A frame over the relay's inline limit never becomes a `FrontendMessage`, so the request
    // it carries has to be recovered from its tag byte. `d` must stay `None`: COPY payload is
    // data inside a request already on the queue, and recording every chunk would
    // desynchronise the ledger in the other direction.
    #[test]
    fn an_oversized_frame_is_recorded_from_its_tag() {
        assert_eq!(RequestKind::from_tag(b'Q'), Some(RequestKind::Query));
        assert_eq!(RequestKind::from_tag(b'P'), Some(RequestKind::Parse));
        assert_eq!(RequestKind::from_tag(b'B'), Some(RequestKind::Bind));
        assert_eq!(RequestKind::from_tag(b'E'), Some(RequestKind::Execute));
        assert_eq!(RequestKind::from_tag(b'S'), Some(RequestKind::Sync));
        for tag in [b'd', b'c', b'f', b'H', b'X'] {
            assert_eq!(RequestKind::from_tag(tag), None, "tag {}", char::from(tag));
        }
    }

    // What the proxy did with an oversized `Query` before it recorded one: the statement
    // executed and committed, and its `ReadyForQuery` arrived against an empty queue.
    #[test]
    fn a_reply_to_an_unrecorded_request_underflows() {
        let mut queue = OutstandingQueue::new();

        let reaction = queue.apply(&ready(TransactionStatus::Idle));

        assert!(matches!(
            reaction,
            Err(OutstandingError::Underflow { message: "ReadyForQuery" })
        ));
    }

    #[test]
    fn a_simple_query_is_retired_only_by_ready_for_query() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Query);

        let row_description = queue
            .apply(&BackendMessage::RowDescription(RowDescription {
                fields: Vec::new(),
            }))
            .unwrap();
        assert_eq!(row_description.popped, None);
        assert_eq!(queue.len(), 1);

        let complete = queue.apply(&command_complete("SELECT 1")).unwrap();
        assert_eq!(complete.popped, None);
        assert_eq!(queue.len(), 1);

        let ready = queue.apply(&ready(TransactionStatus::Idle)).unwrap();
        assert_eq!(ready.popped, Some(RequestKind::Query));
        assert!(ready.batch_ended);
        assert!(queue.is_empty());
    }

    #[test]
    fn an_extended_batch_retires_one_request_per_response() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Parse);
        queue.forward(RequestKind::Bind);
        queue.forward(RequestKind::Describe);
        queue.forward(RequestKind::Execute);
        queue.forward(RequestKind::Sync);

        assert_eq!(
            queue.apply(&BackendMessage::ParseComplete).unwrap().popped,
            Some(RequestKind::Parse)
        );
        assert_eq!(
            queue.apply(&BackendMessage::BindComplete).unwrap().popped,
            Some(RequestKind::Bind)
        );
        assert_eq!(
            queue
                .apply(&BackendMessage::RowDescription(RowDescription {
                    fields: Vec::new()
                }))
                .unwrap()
                .popped,
            Some(RequestKind::Describe)
        );
        assert_eq!(
            queue.apply(&command_complete("SELECT 1")).unwrap().popped,
            Some(RequestKind::Execute)
        );
        let end = queue.apply(&ready(TransactionStatus::Idle)).unwrap();
        assert_eq!(end.popped, Some(RequestKind::Sync));
        assert!(end.batch_ended);
        assert!(queue.is_empty());
    }

    #[test]
    fn describe_without_result_columns_is_retired_by_no_data() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Describe);
        queue.forward(RequestKind::Sync);
        assert_eq!(
            queue.apply(&BackendMessage::NoData).unwrap().popped,
            Some(RequestKind::Describe)
        );
    }

    #[test]
    fn parameter_description_does_not_retire_the_describe() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Describe);
        assert_eq!(
            queue
                .apply(&BackendMessage::ParameterDescription(vec![23]))
                .unwrap()
                .popped,
            None
        );
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn a_suspended_portal_retires_its_execute() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Execute);
        queue.forward(RequestKind::Sync);
        assert_eq!(
            queue
                .apply(&BackendMessage::PortalSuspended)
                .unwrap()
                .popped,
            Some(RequestKind::Execute)
        );
    }

    #[test]
    fn an_empty_query_retires_its_execute() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Execute);
        queue.forward(RequestKind::Sync);
        assert_eq!(
            queue
                .apply(&BackendMessage::EmptyQueryResponse)
                .unwrap()
                .popped,
            Some(RequestKind::Execute)
        );
    }

    #[test]
    fn function_call_is_retired_by_ready_for_query() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::FunctionCall);
        assert_eq!(
            queue
                .apply(&BackendMessage::FunctionCallResponse(None))
                .unwrap()
                .popped,
            None
        );
        assert_eq!(
            queue.apply(&ready(TransactionStatus::Idle)).unwrap().popped,
            Some(RequestKind::FunctionCall)
        );
    }

    #[test]
    fn an_error_discards_the_rest_of_the_batch_but_keeps_the_sync() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Parse);
        queue.forward(RequestKind::Bind);
        queue.forward(RequestKind::Execute);
        queue.forward(RequestKind::Sync);

        let reaction = queue.apply(&error()).unwrap();
        assert_eq!(reaction.popped, None);
        assert!(queue.query_failed());
        assert_eq!(queue.kinds().collect::<Vec<_>>(), vec![RequestKind::Sync]);

        let end = queue.apply(&ready(TransactionStatus::Failed)).unwrap();
        assert_eq!(end.popped, Some(RequestKind::Sync));
        assert!(!queue.query_failed());
        assert!(queue.is_empty());
    }

    #[test]
    fn an_error_in_the_simple_protocol_keeps_the_query() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Query);
        queue.apply(&error()).unwrap();
        assert_eq!(queue.kinds().collect::<Vec<_>>(), vec![RequestKind::Query]);
        assert_eq!(
            queue.apply(&ready(TransactionStatus::Idle)).unwrap().popped,
            Some(RequestKind::Query)
        );
    }

    #[test]
    fn an_error_outside_any_batch_is_forwarded() {
        let mut queue = OutstandingQueue::new();
        let reaction = queue.apply(&error()).unwrap();
        assert_eq!(reaction.disposition, Disposition::Forward);
        assert!(queue.query_failed());
    }

    #[test]
    fn an_injected_request_swallows_its_response() {
        let mut queue = OutstandingQueue::new();
        queue.skip(RequestKind::Parse);
        queue.forward(RequestKind::Bind);
        assert_eq!(
            queue
                .apply(&BackendMessage::ParseComplete)
                .unwrap()
                .disposition,
            Disposition::Skip
        );
        assert_eq!(
            queue
                .apply(&BackendMessage::BindComplete)
                .unwrap()
                .disposition,
            Disposition::Forward
        );
    }

    #[test]
    fn rows_of_an_injected_query_are_swallowed_too() {
        let mut queue = OutstandingQueue::new();
        queue.skip(RequestKind::Query);
        assert_eq!(
            queue
                .apply(&BackendMessage::DataRow(pgelastic_wire::DataRow::new(
                    Bytes::new()
                )))
                .unwrap()
                .disposition,
            Disposition::Skip
        );
    }

    #[test]
    fn a_fake_response_is_released_only_once_it_reaches_the_head() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Parse);
        queue.fake(RequestKind::Close, BackendMessage::CloseComplete);
        queue.forward(RequestKind::Sync);

        assert!(queue.take_ready_fakes().is_empty());
        queue.apply(&BackendMessage::ParseComplete).unwrap();
        assert_eq!(
            queue.take_ready_fakes(),
            vec![BackendMessage::CloseComplete]
        );
        assert_eq!(queue.kinds().collect::<Vec<_>>(), vec![RequestKind::Sync]);
    }

    #[test]
    fn a_notification_disturbs_neither_the_queue_nor_the_batch() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Query);
        let reaction = queue
            .apply(&BackendMessage::NotificationResponse(
                NotificationResponse {
                    process_id: 1,
                    channel: Bytes::from_static(b"chan"),
                    payload: Bytes::new(),
                },
            ))
            .unwrap();
        assert_eq!(reaction.popped, None);
        assert!(!reaction.batch_ended);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn a_parameter_status_is_always_forwarded_even_mid_batch() {
        let mut queue = OutstandingQueue::new();
        queue.skip(RequestKind::Query);
        let reaction = queue
            .apply(&BackendMessage::ParameterStatus(ParameterStatus {
                name: Bytes::from_static(b"TimeZone"),
                value: Bytes::from_static(b"UTC"),
            }))
            .unwrap();
        assert_eq!(reaction.disposition, Disposition::Forward);
    }

    #[test]
    fn an_unmatched_response_is_an_error_not_a_panic() {
        let mut queue = OutstandingQueue::new();
        assert_eq!(
            queue.apply(&BackendMessage::ParseComplete).unwrap_err(),
            OutstandingError::Underflow {
                message: "ParseComplete"
            }
        );

        queue.forward(RequestKind::Bind);
        assert_eq!(
            queue.apply(&BackendMessage::ParseComplete).unwrap_err(),
            OutstandingError::Mismatch {
                expected: RequestKind::Bind,
                message: "ParseComplete"
            }
        );
    }

    #[test]
    fn ready_for_query_against_a_non_terminator_is_an_error() {
        let mut queue = OutstandingQueue::new();
        queue.forward(RequestKind::Bind);
        assert_eq!(
            queue.apply(&ready(TransactionStatus::Idle)).unwrap_err(),
            OutstandingError::Mismatch {
                expected: RequestKind::Bind,
                message: "ReadyForQuery"
            }
        );
    }

    #[test]
    fn request_kinds_are_derived_from_frontend_messages() {
        assert_eq!(
            RequestKind::from_frontend(&FrontendMessage::Sync),
            Some(RequestKind::Sync)
        );
        assert_eq!(RequestKind::from_frontend(&FrontendMessage::Flush), None);
        assert_eq!(
            RequestKind::from_frontend(&FrontendMessage::Terminate),
            None
        );
        assert_eq!(RequestKind::from_frontend(&FrontendMessage::CopyDone), None);
    }
}
