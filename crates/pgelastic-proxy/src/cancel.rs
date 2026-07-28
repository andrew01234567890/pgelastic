//! Query cancellation.
//!
//! Two rules shape everything here.
//!
//! **The backend's real cancel key is never given to a client.** It is a
//! bearer token for `pg_cancel_backend`, and a client that holds another
//! tenant's key can cancel that tenant's queries at will. The proxy mints its
//! own key, hands that out, and keeps the real one.
//!
//! **A `CancelRequest` gets a fresh, unauthenticated connection.** The protocol
//! requires it — the request carries no startup packet and the server closes
//! the socket immediately — so it can never travel on a pooled link.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use pgelastic_wire::{BackendKeyData, CancelKey, CancelRequest};
use tokio::io::AsyncWriteExt;

use crate::error::Result;
use crate::scram::crypto::random_bytes;

/// Bytes of key material in a minted cancel key.
///
/// Four, because protocol 3.0 fixes `BackendKeyData` at twelve bytes on the
/// wire and libpq rejects anything else. Protocol 3.2 allows up to 256, which
/// is what a hop TTL will need; until a 3.2 client is actually negotiated the
/// routing identity has to live in the process-id field instead.
const KEY_LEN: usize = 4;

/// A minted cancel token: the pair a client will quote back.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CancelToken {
    pub process_id: i32,
    pub key: Bytes,
}

impl CancelToken {
    /// Mints a token carrying this replica's routing id.
    ///
    /// The sign bit of the process id is cleared because `pg_basebackup` and
    /// several drivers reject a negative PID outright.
    pub fn mint(routing_id: u16) -> Result<Self> {
        let random: [u8; 2] = random_bytes()?;
        let low = u32::from(u16::from_be_bytes(random));
        let high = u32::from(routing_id & 0x7fff) << 16;
        Ok(Self {
            process_id: (high | low).cast_signed(),
            key: Bytes::copy_from_slice(&random_bytes::<KEY_LEN>()?),
        })
    }

    pub fn routing_id(&self) -> u16 {
        u16::try_from((self.process_id.cast_unsigned() >> 16) & 0x7fff).unwrap_or(0)
    }

    pub fn key_data(&self) -> Result<BackendKeyData> {
        Ok(BackendKeyData {
            process_id: self.process_id,
            key: CancelKey::new(self.key.clone())?,
        })
    }
}

impl From<&CancelRequest> for CancelToken {
    fn from(request: &CancelRequest) -> Self {
        Self {
            process_id: request.process_id,
            key: Bytes::copy_from_slice(request.key.as_bytes()),
        }
    }
}

/// Everything needed to open a fresh cancel connection to the right backend.
#[derive(Debug, Clone)]
pub struct CancelTarget {
    pub address: String,
    pub key_data: Option<BackendKeyData>,
}

/// Where a client's cancel should currently be sent.
///
/// In transaction pooling the answer changes with every checkout: the query the
/// client wants cancelled is running on whichever backend it holds *now*, and a
/// cancel delivered to the backend it held a moment ago would cancel a different
/// tenant's statement. So the route is a shared cell the session rewrites at
/// every checkout and clears at every release, and the cancel path reads it at
/// send time rather than at registration time.
#[derive(Debug, Clone, Default)]
pub struct CancelRoute {
    target: Arc<Mutex<Option<CancelTarget>>>,
    in_flight: Arc<AtomicUsize>,
}

impl CancelRoute {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, target: Option<CancelTarget>) {
        *self
            .target
            .lock()
            .expect("a cancel route is never poisoned") = target;
    }

    /// The current target, read at the moment of sending.
    pub fn resolve(&self) -> Option<CancelTarget> {
        self.target
            .lock()
            .expect("a cancel route is never poisoned")
            .clone()
    }

    /// Cancels aimed at this client that have been picked up but not yet
    /// delivered.
    ///
    /// A release taken inside that window hands the backend to the next client
    /// and the cancel then lands on *that* client's statement, which is a
    /// cross-tenant cancel. It is a condition of the release gate for exactly
    /// that reason.
    pub fn cancels_in_flight(&self) -> usize {
        self.in_flight.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Marks a cancel as under way, clearing the mark when the guard drops.
    pub fn dispatching(&self) -> CancelInFlight {
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        CancelInFlight {
            in_flight: Arc::clone(&self.in_flight),
        }
    }
}

/// Holds a cancel open for as long as it is being delivered.
#[derive(Debug)]
pub struct CancelInFlight {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for CancelInFlight {
    fn drop(&mut self) {
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
pub struct CancelRegistry {
    entries: Mutex<HashMap<CancelToken, CancelRoute>>,
}

impl CancelRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers a session, returning a guard that deregisters on drop.
    ///
    /// A guard rather than a matching `remove` call because the session task
    /// can exit down many paths, and a leaked entry means a later token
    /// collision resolves to a connection that no longer exists.
    pub fn register(
        self: &Arc<Self>,
        token: CancelToken,
        route: CancelRoute,
    ) -> CancelRegistration {
        self.entries
            .lock()
            .expect("cancel registry is never poisoned")
            .insert(token.clone(), route);
        CancelRegistration {
            registry: Arc::clone(self),
            token,
        }
    }

    /// The route registered for a token, if any. Resolving it to an address is
    /// deliberately a separate step, taken at send time.
    pub fn lookup(&self, token: &CancelToken) -> Option<CancelRoute> {
        self.entries
            .lock()
            .expect("cancel registry is never poisoned")
            .get(token)
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("cancel registry is never poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn remove(&self, token: &CancelToken) {
        self.entries
            .lock()
            .expect("cancel registry is never poisoned")
            .remove(token);
    }
}

#[derive(Debug)]
pub struct CancelRegistration {
    registry: Arc<CancelRegistry>,
    token: CancelToken,
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        self.registry.remove(&self.token);
    }
}

/// Opens a fresh connection to the backend and delivers the real cancel key.
///
/// The connection is fresh and unauthenticated because the protocol requires it:
/// a `CancelRequest` carries no startup packet and the server closes the socket
/// as soon as it has read one, so it can never travel on a pooled link.
pub async fn deliver(
    target: &CancelTarget,
    tls: Option<&crate::tls::BackendTls>,
    connect_timeout: std::time::Duration,
) -> Result<()> {
    let Some(key_data) = target.key_data.clone() else {
        return Ok(());
    };
    let mut stream = crate::backend::connect_socket(&target.address, tls, connect_timeout).await?;

    let mut wire = BytesMut::new();
    CancelRequest {
        process_id: key_data.process_id,
        key: key_data.key,
    }
    .encode(&mut wire);
    stream.write_all(&wire).await?;
    stream.flush().await?;
    // The server answers a CancelRequest by closing the socket, never with a
    // message, so there is nothing to read back.
    let _ = stream.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> CancelTarget {
        target_for(4242)
    }

    fn target_for(process_id: i32) -> CancelTarget {
        CancelTarget {
            address: "127.0.0.1:5432".to_owned(),
            key_data: Some(BackendKeyData {
                process_id,
                key: CancelKey::new(Bytes::from_static(b"real")).unwrap(),
            }),
        }
    }

    fn route(target: CancelTarget) -> CancelRoute {
        let route = CancelRoute::new();
        route.set(Some(target));
        route
    }

    #[test]
    fn a_minted_token_never_has_a_negative_process_id() {
        for _ in 0..1000 {
            assert!(CancelToken::mint(0x7fff).unwrap().process_id >= 0);
        }
    }

    #[test]
    fn a_minted_token_carries_the_routing_id_back() {
        for routing_id in [0u16, 1, 0x1234, 0x7ffe] {
            let token = CancelToken::mint(routing_id).unwrap();
            assert_eq!(token.routing_id(), routing_id);
        }
    }

    #[test]
    fn a_minted_key_fits_protocol_3_0_backend_key_data() {
        let token = CancelToken::mint(7).unwrap();
        assert_eq!(token.key_data().unwrap().key.len(), 4);
    }

    #[test]
    fn minted_tokens_do_not_repeat() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..5000 {
            assert!(seen.insert(CancelToken::mint(1).unwrap()));
        }
    }

    #[test]
    fn a_registration_is_removed_when_its_guard_drops() {
        let registry = CancelRegistry::new();
        let token = CancelToken::mint(0).unwrap();
        {
            let _guard = registry.register(token.clone(), route(target()));
            assert_eq!(
                registry
                    .lookup(&token)
                    .unwrap()
                    .resolve()
                    .unwrap()
                    .key_data
                    .unwrap()
                    .process_id,
                4242
            );
        }
        assert!(registry.lookup(&token).is_none());
        assert!(registry.is_empty());
    }

    /// The property transaction pooling depends on: the same token must resolve
    /// to whichever backend is running the client's query *now*, not to the one
    /// it was running when the token was registered.
    #[test]
    fn a_route_resolves_to_the_backend_the_client_holds_at_send_time() {
        let registry = CancelRegistry::new();
        let token = CancelToken::mint(0).unwrap();
        let route = CancelRoute::new();
        let _guard = registry.register(token.clone(), route.clone());

        route.set(Some(target_for(1)));
        assert_eq!(
            registry
                .lookup(&token)
                .unwrap()
                .resolve()
                .unwrap()
                .key_data
                .unwrap()
                .process_id,
            1
        );

        route.set(Some(target_for(2)));
        assert_eq!(
            registry
                .lookup(&token)
                .unwrap()
                .resolve()
                .unwrap()
                .key_data
                .unwrap()
                .process_id,
            2
        );

        // Between transactions the client holds nothing, so there is nothing to
        // cancel and nobody else's query is cancelled in its place.
        route.set(None);
        assert!(registry.lookup(&token).unwrap().resolve().is_none());
    }

    #[test]
    fn an_unknown_token_resolves_to_nothing() {
        let registry = CancelRegistry::new();
        let _guard = registry.register(CancelToken::mint(0).unwrap(), route(target()));
        let other = CancelToken {
            process_id: 1,
            key: Bytes::from_static(b"nope"),
        };
        assert!(registry.lookup(&other).is_none());
    }

    #[test]
    fn a_cancel_request_maps_onto_the_token_it_quotes() {
        let token = CancelToken::mint(3).unwrap();
        let request = CancelRequest {
            process_id: token.process_id,
            key: CancelKey::new(token.key.clone()).unwrap(),
        };
        assert_eq!(CancelToken::from(&request), token);
    }
}
