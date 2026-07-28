//! The client-side state machine.
//!
//! The four live states are the ones `PgBouncer` gauges as `cl_active`,
//! `cl_waiting`, `cl_active_cancel_req` and `cl_waiting_cancel_req`, and they
//! are kept name-for-name so that a `SHOW`-compatible exporter needs no
//! translation table.
//!
//! `Active` does not mean "running a query" — it means "not queued for a
//! server". A client sitting idle between transactions is active; only a client
//! blocked in the wait queue is waiting.

use std::fmt;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientState {
    /// Connected and not queued for a server.
    Active,
    /// Blocked in the wait queue.
    Waiting,
    /// A `CancelRequest` connection whose cancel is being delivered.
    ActiveCancelReq,
    /// A `CancelRequest` connection waiting for the capacity to deliver it.
    WaitingCancelReq,
    /// Terminal.
    Closed,
}

impl ClientState {
    pub fn is_waiting(self) -> bool {
        matches!(self, Self::Waiting | Self::WaitingCancelReq)
    }

    pub fn is_cancel_request(self) -> bool {
        matches!(self, Self::ActiveCancelReq | Self::WaitingCancelReq)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Waiting => "waiting",
            Self::ActiveCancelReq => "active_cancel_req",
            Self::WaitingCancelReq => "waiting_cancel_req",
            Self::Closed => "closed",
        }
    }
}

impl fmt::Display for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientEvent {
    /// Joined the wait queue for a server.
    Enqueued,
    /// The queue handed it a server.
    ServerAssigned,
    /// The wait ended without a server: timeout, pool pause or shutdown.
    WaitAbandoned,
    /// The server was released back to the pool.
    ServerReleased,
    /// The cancel connection got the capacity it needed.
    CancelDispatched,
    /// The client disconnected or was killed.
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("client cannot {event:?} while {state}")]
pub struct IllegalClientTransition {
    pub state: ClientState,
    pub event: ClientEvent,
}

/// A client's position in the pool.
#[derive(Debug, Clone)]
pub struct ClientMachine {
    state: ClientState,
}

impl ClientMachine {
    /// A freshly authenticated client, holding no server and queued for none.
    pub fn connected() -> Self {
        Self {
            state: ClientState::Active,
        }
    }

    /// A `CancelRequest` connection, which is born waiting: it has nothing to
    /// send until the cancel subsystem grants it a fresh backend connection.
    pub fn cancel_request() -> Self {
        Self {
            state: ClientState::WaitingCancelReq,
        }
    }

    pub fn state(&self) -> ClientState {
        self.state
    }

    pub fn apply(&mut self, event: ClientEvent) -> Result<ClientState, IllegalClientTransition> {
        let next = Self::next(self.state, event).ok_or(IllegalClientTransition {
            state: self.state,
            event,
        })?;
        self.state = next;
        Ok(next)
    }

    fn next(state: ClientState, event: ClientEvent) -> Option<ClientState> {
        use ClientEvent as E;
        use ClientState as S;

        match (state, event) {
            (S::Closed, _) => None,

            (_, E::Disconnected) | (S::WaitingCancelReq, E::WaitAbandoned) => Some(S::Closed),
            (S::Active, E::Enqueued) => Some(S::Waiting),
            (S::Waiting, E::ServerAssigned | E::WaitAbandoned) | (S::Active, E::ServerReleased) => {
                Some(S::Active)
            }
            (S::WaitingCancelReq, E::CancelDispatched) => Some(S::ActiveCancelReq),

            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_client_is_active_not_waiting() {
        assert_eq!(ClientMachine::connected().state(), ClientState::Active);
    }

    #[test]
    fn queueing_and_being_served_round_trips_through_waiting() {
        let mut client = ClientMachine::connected();
        assert_eq!(
            client.apply(ClientEvent::Enqueued).unwrap(),
            ClientState::Waiting
        );
        assert_eq!(
            client.apply(ClientEvent::ServerAssigned).unwrap(),
            ClientState::Active
        );
    }

    #[test]
    fn a_timed_out_waiter_stays_connected() {
        let mut client = ClientMachine::connected();
        client.apply(ClientEvent::Enqueued).unwrap();
        assert_eq!(
            client.apply(ClientEvent::WaitAbandoned).unwrap(),
            ClientState::Active
        );
    }

    #[test]
    fn a_cancel_request_starts_waiting_and_ends_closed() {
        let mut cancel = ClientMachine::cancel_request();
        assert_eq!(cancel.state(), ClientState::WaitingCancelReq);
        assert!(cancel.state().is_cancel_request());
        assert_eq!(
            cancel.apply(ClientEvent::CancelDispatched).unwrap(),
            ClientState::ActiveCancelReq
        );
        assert_eq!(
            cancel.apply(ClientEvent::Disconnected).unwrap(),
            ClientState::Closed
        );
    }

    #[test]
    fn a_cancel_request_that_never_gets_capacity_is_closed_not_reactivated() {
        let mut cancel = ClientMachine::cancel_request();
        assert_eq!(
            cancel.apply(ClientEvent::WaitAbandoned).unwrap(),
            ClientState::Closed
        );
    }

    #[test]
    fn a_client_cannot_be_assigned_a_server_it_never_queued_for() {
        let mut client = ClientMachine::connected();
        assert_eq!(
            client.apply(ClientEvent::ServerAssigned).unwrap_err(),
            IllegalClientTransition {
                state: ClientState::Active,
                event: ClientEvent::ServerAssigned,
            }
        );
        assert_eq!(client.state(), ClientState::Active);
    }

    #[test]
    fn a_waiting_client_cannot_queue_twice() {
        let mut client = ClientMachine::connected();
        client.apply(ClientEvent::Enqueued).unwrap();
        assert!(client.apply(ClientEvent::Enqueued).is_err());
    }

    #[test]
    fn a_closed_client_accepts_nothing_further() {
        let mut client = ClientMachine::connected();
        client.apply(ClientEvent::Disconnected).unwrap();
        for event in [
            ClientEvent::Enqueued,
            ClientEvent::ServerAssigned,
            ClientEvent::WaitAbandoned,
            ClientEvent::ServerReleased,
            ClientEvent::CancelDispatched,
            ClientEvent::Disconnected,
        ] {
            assert!(client.apply(event).is_err(), "{event:?}");
        }
        assert_eq!(client.state(), ClientState::Closed);
    }

    #[test]
    fn a_normal_client_never_enters_a_cancel_state() {
        let mut client = ClientMachine::connected();
        assert!(client.apply(ClientEvent::CancelDispatched).is_err());
    }

    #[test]
    fn every_illegal_transition_leaves_the_state_untouched() {
        for state in [
            ClientState::Active,
            ClientState::Waiting,
            ClientState::ActiveCancelReq,
            ClientState::WaitingCancelReq,
        ] {
            for event in [
                ClientEvent::Enqueued,
                ClientEvent::ServerAssigned,
                ClientEvent::WaitAbandoned,
                ClientEvent::ServerReleased,
                ClientEvent::CancelDispatched,
            ] {
                let mut client = ClientMachine { state };
                if client.apply(event).is_err() {
                    assert_eq!(client.state(), state);
                }
            }
        }
    }
}
