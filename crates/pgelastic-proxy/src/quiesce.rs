//! Quiesce: holding a tenant's clients still, without dropping them.
//!
//! A live tenant migration's cutover is the one moment the whole design turns
//! on. The sequence is *quiesce → drain in-flight → wait for the subscription
//! to catch up → set every sequence → verify → flip the routing table → resume
//! the queued clients against the target*, and only a proxy can perform it,
//! because only a proxy owns both ends of every socket involved.
//!
//! The differentiator is what happens to the clients during the pause. Azure's
//! elastic-pool move drops the connections and relies on client retry, so a
//! move is a visible error to every application on that database. pgelastic
//! **queues the transactions and keeps the sockets**: a client that issues a
//! statement during the cutover sees latency, not a failure, and its next
//! transaction runs against the target without it having reconnected.
//!
//! Three properties make that safe:
//!
//! - **The gate admits transactions, not messages.** A client that already
//!   holds a backend is untouched — its transaction runs to completion and its
//!   link goes back to the pool, which is what makes the drain finite. Only the
//!   *next* transaction waits.
//! - **Release is first-in-first-out.** [`Baton`] is handed to the next waiter
//!   when the one in front of it has taken its backend, so the order clients
//!   were admitted in is the order they were queued in. Nothing is woken as a
//!   broadcast, because a broadcast reorders a tenant's traffic at exactly the
//!   moment it is being told the cutover was transparent.
//! - **The quiesce is leased.** An operator that dies mid-cutover must not be
//!   able to leave a tenant frozen: the lease expires, and expiry performs
//!   precisely an [`unquiesce`](TenantGate::unquiesce) — the gate opens and, if
//!   nothing has yet run on the target, the route goes back to the source.
//!
//! # What quiesce does not cover
//!
//! **Session pooling.** A session-mode client owns its backend for its whole
//! life, so there is no boundary at which to hold it and no point at which the
//! tenant is drained. Such a session is counted as permanently in flight, which
//! makes [`DrainStatus::drained`] report `false` rather than letting a cutover
//! proceed over a link that is still writing to the source. A tenant that is to
//! be migrated online has to be in transaction pooling.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::route::InstanceId;

/// Why a control-plane call was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuiesceError {
    /// Another holder's lease is live. Never silently taken over: two
    /// operators believing they own a cutover is how a tenant ends up split
    /// between two instances.
    LeaseHeld {
        holder: String,
        expires_in: Duration,
    },
    /// The call needs a quiesce that is not in effect.
    NotQuiesced,
    /// The requested lease is longer than the configured ceiling.
    TtlTooLong { requested: Duration, max: Duration },
    /// `setRoute` names an instance this proxy does not front.
    NoSuchInstance { instance: String },
    /// `setRoute` while the tenant is running. Moving a tenant whose clients
    /// are still being admitted would split its traffic across two instances
    /// with no ordering between the halves.
    NotHeld,
}

impl std::fmt::Display for QuiesceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeaseHeld { holder, expires_in } => write!(
                f,
                "the quiesce lease is held by {holder:?} for another {}ms",
                expires_in.as_millis()
            ),
            Self::NotQuiesced => f.write_str("this tenant is not quiesced"),
            Self::TtlTooLong { requested, max } => write!(
                f,
                "a lease of {}ms exceeds the configured ceiling of {}ms",
                requested.as_millis(),
                max.as_millis()
            ),
            Self::NoSuchInstance { instance } => {
                write!(f, "{instance:?} is not an instance this proxy fronts")
            }
            Self::NotHeld => f.write_str(
                "this tenant's traffic is not held, so its route cannot be moved without \
                 splitting it across two instances",
            ),
        }
    }
}

/// The lease that entitles one holder to keep a tenant quiesced.
#[derive(Debug, Clone)]
pub struct Lease {
    pub holder: String,
    pub ttl: Duration,
    expires: Instant,
    /// Where the tenant was routed when the lease was taken.
    ///
    /// Kept until a [`resume`](TenantGate::resume) commits the flip, because up
    /// to that instant nothing has run on the target and going back is free.
    /// After it, going back would discard transactions clients have already
    /// been told committed.
    source: Option<InstanceId>,
}

impl Lease {
    pub fn expires_in(&self) -> Duration {
        self.expires.saturating_duration_since(Instant::now())
    }

    pub fn expired(&self) -> bool {
        Instant::now() >= self.expires
    }

    /// Whether a rollback to the source is still available.
    pub fn reversible(&self) -> bool {
        self.source.is_some()
    }
}

/// What dropping an expired lease implied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expired {
    pub holder: String,
    /// Where the tenant belongs now: `Some` when the cutover never committed
    /// and the route has to go back, `None` when a resume already made the
    /// target authoritative.
    pub rollback: Option<InstanceId>,
    /// How long the tenant's clients were held.
    pub held: Option<Duration>,
}

/// What `drainStatus` answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainStatus {
    pub tenant: String,
    pub instance: InstanceId,
    pub quiesced: bool,
    pub in_flight: u64,
    pub queued: u64,
    /// Quiesced with nothing still holding a backend: the instant the cutover
    /// is allowed to proceed.
    pub drained: bool,
    pub holder: Option<String>,
    pub lease_expires_in: Option<Duration>,
}

/// What a gate holds: one tenant, or every tenant on one instance.
///
/// The two are the same machinery under two leases, and that is the whole
/// design. A tenant lease is owned by whoever is moving *that tenant between
/// instances*; an instance lease is owned by whoever is changing *which member
/// of that instance is the primary*. They are different exclusions over
/// different things, so a live migration and a planned switchover compose
/// instead of colliding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Tenant,
    Instance,
}

impl Scope {
    const fn label(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Instance => "instance",
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// One tenant's admission gate.
#[derive(Debug)]
pub struct TenantGate {
    scope: Scope,
    tenant: String,
    /// `true` while transactions are admitted.
    open: watch::Sender<bool>,
    queue: Mutex<Queue>,
    lease: Mutex<Option<Lease>>,
    in_flight: AtomicU64,
    admitted_while_queued: AtomicU64,
    /// When the gate closed, or `None` while it is admitting.
    ///
    /// Set by the quiesce that actually closes it and by no other. A renewal
    /// must not restart the clock, or a cutover that renews its lease every
    /// five seconds would report a five-second pause however long it really
    /// held the tenant.
    closed_at: Mutex<Option<Instant>>,
}

#[derive(Debug, Default)]
struct Queue {
    next_ticket: u64,
    /// Ticket order is arrival order, which is why this is ordered rather than
    /// hashed.
    waiting: BTreeMap<u64, oneshot::Sender<()>>,
    /// Set between a waiter being woken and it releasing its baton.
    running: bool,
}

impl TenantGate {
    fn new(scope: Scope, tenant: &str) -> Arc<Self> {
        Arc::new(Self {
            scope,
            tenant: tenant.to_owned(),
            open: watch::Sender::new(true),
            queue: Mutex::new(Queue::default()),
            lease: Mutex::new(None),
            in_flight: AtomicU64::new(0),
            admitted_while_queued: AtomicU64::new(0),
            closed_at: Mutex::new(None),
        })
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn scope(&self) -> Scope {
        self.scope
    }

    pub fn is_open(&self) -> bool {
        *self.open.borrow()
    }

    /// Whether somebody is deliberately keeping these clients still.
    ///
    /// The fence's input, and the reason it is a *live* lease rather than
    /// merely a closed gate: a lease that has expired but has not been swept
    /// yet is nobody's deliberate hold, and treating it as one would soften the
    /// fence on the strength of a stale record. Reap-then-ask is not available
    /// here — the fence fires from the data path, and the sweep is the control
    /// plane's.
    pub fn held(&self) -> bool {
        if *self.open.borrow() {
            return false;
        }
        self.lease
            .lock()
            .expect("a tenant gate is never poisoned")
            .as_ref()
            .is_some_and(|live| !live.expired())
    }

    pub fn queued(&self) -> u64 {
        let depth = self
            .queue
            .lock()
            .expect("a tenant gate is never poisoned")
            .waiting
            .len();
        u64::try_from(depth).unwrap_or(u64::MAX)
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Transactions that were queued by a quiesce and later released.
    pub fn resumed(&self) -> u64 {
        self.admitted_while_queued.load(Ordering::Relaxed)
    }

    /// Marks a backend as held by this tenant for as long as the guard lives.
    pub fn hold(self: &Arc<Self>) -> InFlight {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlight {
            gate: Arc::clone(self),
        }
    }

    /// Waits until this tenant is admitting transactions.
    ///
    /// Returns a [`Baton`]; the next queued client is released when it drops,
    /// so the caller holds it across whatever it does to bind itself to a
    /// backend. On the open path — every transaction outside a cutover — this
    /// allocates nothing and returns immediately.
    pub async fn admit(self: &Arc<Self>) -> Baton {
        let waiting = {
            let mut queue = self.queue.lock().expect("a tenant gate is never poisoned");
            if *self.open.borrow() && queue.waiting.is_empty() && !queue.running {
                return Baton { gate: None };
            }
            let ticket = queue.next_ticket;
            queue.next_ticket += 1;
            let (tx, rx) = oneshot::channel();
            queue.waiting.insert(ticket, tx);
            Queued {
                gate: Arc::clone(self),
                ticket,
                rx,
            }
        };
        debug!(tenant = %self.tenant, "a transaction is queued behind a quiesce");
        waiting.wait().await;
        self.admitted_while_queued.fetch_add(1, Ordering::Relaxed);
        Baton {
            gate: Some(Arc::clone(self)),
        }
    }

    /// Wakes the client at the head of the queue, if the gate is open.
    fn pass(&self) {
        let mut queue = self.queue.lock().expect("a tenant gate is never poisoned");
        queue.running = false;
        if !*self.open.borrow() {
            return;
        }
        while let Some(ticket) = queue.waiting.keys().next().copied() {
            let sender = queue.waiting.remove(&ticket).expect("just observed");
            if sender.send(()).is_ok() {
                queue.running = true;
                return;
            }
            // That client is gone. The next one is entitled to the slot rather
            // than to another wait.
        }
    }

    fn withdraw(&self, ticket: u64) {
        let mut queue = self.queue.lock().expect("a tenant gate is never poisoned");
        queue.waiting.remove(&ticket);
    }

    // ---- the control-plane operations -----------------------------------

    /// Closes the gate under a lease.
    ///
    /// Repeating it with the same holder renews rather than fails, so an
    /// operator that keeps calling while it drains does not have to distinguish
    /// "take" from "renew".
    pub fn quiesce(
        &self,
        holder: &str,
        ttl: Duration,
        max_ttl: Duration,
        source: InstanceId,
    ) -> Result<Lease, QuiesceError> {
        if ttl > max_ttl {
            return Err(QuiesceError::TtlTooLong {
                requested: ttl,
                max: max_ttl,
            });
        }
        let mut lease = self.lease.lock().expect("a tenant gate is never poisoned");
        if let Some(live) = lease.as_ref()
            && live.holder != holder
            && !live.expired()
        {
            return Err(QuiesceError::LeaseHeld {
                holder: live.holder.clone(),
                expires_in: live.expires_in(),
            });
        }
        // A renewal must not re-arm a rollback the holder has already given up
        // by resuming.
        let source = match lease.as_ref() {
            Some(live) if live.holder == holder && !live.expired() => live.source.clone(),
            _ => Some(source),
        };
        let taken = Lease {
            holder: holder.to_owned(),
            ttl,
            expires: Instant::now() + ttl,
            source,
        };
        *lease = Some(taken.clone());
        drop(lease);
        if self.open.send_replace(false) {
            *self
                .closed_at
                .lock()
                .expect("a tenant gate is never poisoned") = Some(Instant::now());
        }
        info!(
            scope = %self.scope,
            tenant = %self.tenant,
            holder,
            ttl_ms = ttl.as_millis(),
            "this gate is quiesced; new transactions are queued and their sockets held"
        );
        Ok(taken)
    }

    /// Opens the gate and commits whatever route is current.
    ///
    /// The point of no return: from here a rollback would discard transactions
    /// the target has already answered, so the lease stops being reversible
    /// even though the holder keeps it.
    pub fn resume(&self, holder: &str) -> Result<u64, QuiesceError> {
        {
            let mut lease = self.lease.lock().expect("a tenant gate is never poisoned");
            let live = lease.as_mut().ok_or(QuiesceError::NotQuiesced)?;
            check_holder(live, holder)?;
            live.source = None;
        }
        Ok(self.open_gate())
    }

    /// Releases the lease, opening the gate and — if no [`resume`](Self::resume)
    /// has committed the flip — putting the route back where it was.
    ///
    /// Returns the instance the tenant should be routed to now.
    pub fn unquiesce(&self, holder: &str) -> Result<Option<InstanceId>, QuiesceError> {
        let rollback = {
            let mut lease = self.lease.lock().expect("a tenant gate is never poisoned");
            let live = lease.as_ref().ok_or(QuiesceError::NotQuiesced)?;
            check_holder(live, holder)?;
            lease.take().and_then(|live| live.source)
        };
        self.open_gate();
        Ok(rollback)
    }

    /// Drops an expired lease, reporting the rollback it implies.
    ///
    /// Expiry is defined to perform exactly an [`unquiesce`](Self::unquiesce),
    /// which is the whole reason a killed operator cannot leave a tenant
    /// frozen: there is one recovery path and it is the same one the happy path
    /// uses.
    fn reap(&self) -> Option<Expired> {
        let live = {
            let mut lease = self.lease.lock().expect("a tenant gate is never poisoned");
            match lease.as_ref() {
                Some(live) if live.expired() => lease.take()?,
                _ => return None,
            }
        };
        warn!(
            scope = %self.scope,
            tenant = %self.tenant,
            holder = %live.holder,
            reverting = live.source.is_some(),
            "the quiesce lease expired without being renewed; the gate is unquiesced"
        );
        let held = self.held_for();
        let released = self.open_gate();
        debug!(tenant = %self.tenant, released, "queued transactions released by lease expiry");
        Some(Expired {
            holder: live.holder,
            rollback: live.source,
            held,
        })
    }

    fn open_gate(&self) -> u64 {
        let queued = self.queued();
        self.open.send_replace(true);
        self.closed_at
            .lock()
            .expect("a tenant gate is never poisoned")
            .take();
        self.pass();
        queued
    }

    /// How long this tenant has been held, for a caller about to release it.
    ///
    /// The client-visible pause a cutover commits to, measured at the only
    /// place that knows both ends of it. Read before the release rather than
    /// after, because opening the gate is what clears the clock.
    pub fn held_for(&self) -> Option<Duration> {
        self.closed_at
            .lock()
            .expect("a tenant gate is never poisoned")
            .map(|closed| closed.elapsed())
    }

    pub fn lease(&self) -> Option<Lease> {
        self.lease
            .lock()
            .expect("a tenant gate is never poisoned")
            .clone()
    }

    /// Checks that `holder` owns a live lease, for operations that need one.
    pub fn assert_held(&self, holder: &str) -> Result<(), QuiesceError> {
        let lease = self.lease.lock().expect("a tenant gate is never poisoned");
        let live = lease.as_ref().ok_or(QuiesceError::NotQuiesced)?;
        check_holder(live, holder)?;
        if *self.open.borrow() {
            return Err(QuiesceError::NotHeld);
        }
        Ok(())
    }
}

fn check_holder(live: &Lease, holder: &str) -> Result<(), QuiesceError> {
    if live.holder == holder {
        return Ok(());
    }
    Err(QuiesceError::LeaseHeld {
        holder: live.holder.clone(),
        expires_in: live.expires_in(),
    })
}

/// A client parked in the queue, deregistered if it goes away first.
struct Queued {
    gate: Arc<TenantGate>,
    ticket: u64,
    rx: oneshot::Receiver<()>,
}

impl Queued {
    async fn wait(mut self) {
        // A dropped sender means the gate is gone, which can only happen at
        // shutdown; proceeding is right, because the alternative is hanging.
        let _ = (&mut self.rx).await;
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        self.gate.withdraw(self.ticket);
    }
}

/// The right to proceed, and the obligation to release the next client.
///
/// Held across the caller's checkout so the queue drains in the order it
/// filled: the client behind is woken once the client in front has its backend,
/// never before.
#[derive(Debug)]
pub struct Baton {
    gate: Option<Arc<TenantGate>>,
}

impl Drop for Baton {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.pass();
        }
    }
}

/// A backend held by this tenant, counted for `drainStatus`.
#[derive(Debug)]
pub struct InFlight {
    gate: Arc<TenantGate>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Every gate, tenant-scoped and instance-scoped, created on first use.
///
/// The two maps are kept apart rather than sharing one keyspace because a
/// tenant and an instance may legitimately have the same name, and because
/// their expiries mean different things: an expired tenant lease implies a route
/// rollback and an expired instance lease implies nothing but "stop holding".
#[derive(Debug, Default)]
pub struct QuiesceRegistry {
    gates: Mutex<HashMap<String, Arc<TenantGate>>>,
    instances: Mutex<HashMap<String, Arc<TenantGate>>>,
}

impl QuiesceRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn gate(&self, tenant: &str) -> Arc<TenantGate> {
        Self::entry(&self.gates, Scope::Tenant, tenant)
    }

    /// The gate that holds every tenant on one instance.
    ///
    /// Composes with the tenant gates rather than replacing them: a checkout
    /// passes both, so a live migration of one tenant and a planned switchover
    /// of the instance it is leaving can be owned by two different holders at
    /// once without either taking the other's lease.
    pub fn instance(&self, instance: &str) -> Arc<TenantGate> {
        Self::entry(&self.instances, Scope::Instance, instance)
    }

    fn entry(
        map: &Mutex<HashMap<String, Arc<TenantGate>>>,
        scope: Scope,
        name: &str,
    ) -> Arc<TenantGate> {
        let mut gates = map.lock().expect("the quiesce registry is never poisoned");
        Arc::clone(
            gates
                .entry(name.to_owned())
                .or_insert_with(|| TenantGate::new(scope, name)),
        )
    }

    /// The gate for a tenant that has one, without creating it.
    pub fn existing(&self, tenant: &str) -> Option<Arc<TenantGate>> {
        self.gates
            .lock()
            .expect("the quiesce registry is never poisoned")
            .get(tenant)
            .map(Arc::clone)
    }

    /// Every tenant this proxy has ever admitted a client for.
    ///
    /// The input to resolving an instance's tenants: the routing table names
    /// only the tenants somebody configured a route for, and a tenant on the
    /// default instance has no entry there at all.
    pub fn tenants(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .gates
            .lock()
            .expect("the quiesce registry is never poisoned")
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    pub fn quiesced(&self) -> Vec<Arc<TenantGate>> {
        self.gates
            .lock()
            .expect("the quiesce registry is never poisoned")
            .values()
            .filter(|gate| !gate.is_open())
            .map(Arc::clone)
            .collect()
    }

    /// Drops every expired tenant lease, returning the rollbacks they imply.
    pub fn reap_expired(&self) -> Vec<(String, Expired)> {
        Self::reap(&self.gates)
    }

    /// Drops every expired instance lease.
    ///
    /// Separate from [`reap_expired`](Self::reap_expired) because the caller
    /// must not act on the rollback: a switchover never moved the tenants
    /// anywhere, so putting them "back" would mean writing the instance's own
    /// name into the routing table under an instance's name.
    pub fn reap_expired_instances(&self) -> Vec<(String, Expired)> {
        Self::reap(&self.instances)
    }

    fn reap(map: &Mutex<HashMap<String, Arc<TenantGate>>>) -> Vec<(String, Expired)> {
        let gates: Vec<Arc<TenantGate>> = map
            .lock()
            .expect("the quiesce registry is never poisoned")
            .values()
            .map(Arc::clone)
            .collect();
        gates
            .into_iter()
            .filter_map(|gate| gate.reap().map(|expiry| (gate.tenant().to_owned(), expiry)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_TTL: Duration = Duration::from_secs(60);

    fn gate() -> Arc<TenantGate> {
        TenantGate::new(Scope::Tenant, "alpha")
    }

    fn source() -> InstanceId {
        InstanceId::new("inst-a")
    }

    #[tokio::test]
    async fn an_open_gate_admits_without_queueing() {
        let gate = gate();
        let baton = gate.admit().await;
        assert_eq!(gate.queued(), 0);
        drop(baton);
    }

    #[tokio::test]
    async fn a_quiesced_gate_holds_a_transaction_rather_than_refusing_it() {
        let gate = gate();
        gate.quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        let waiting = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.admit().await }
        });
        wait_for(|| gate.queued() == 1).await;
        assert!(!waiting.is_finished());

        gate.resume("migration").unwrap();
        let baton = waiting.await.unwrap();
        drop(baton);
        assert_eq!(gate.queued(), 0);
        assert_eq!(gate.resumed(), 1);
    }

    #[tokio::test]
    async fn queued_transactions_are_released_in_the_order_they_arrived() {
        let gate = gate();
        gate.quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();

        let order = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for index in 0..8u32 {
            let waiter = Arc::clone(&gate);
            let order = Arc::clone(&order);
            handles.push(tokio::spawn(async move {
                let baton = waiter.admit().await;
                order.lock().unwrap().push(index);
                drop(baton);
            }));
            wait_for(|| gate.queued() == u64::from(index) + 1).await;
        }

        gate.resume("migration").unwrap();
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), (0..8).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_client_that_leaves_the_queue_does_not_strand_the_ones_behind_it() {
        let gate = gate();
        gate.quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();

        let leaving = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move {
                tokio::time::timeout(Duration::from_millis(50), gate.admit())
                    .await
                    .ok()
            }
        });
        wait_for(|| gate.queued() == 1).await;
        let staying = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.admit().await }
        });
        wait_for(|| gate.queued() == 2).await;
        assert!(leaving.await.unwrap().is_none());

        gate.resume("migration").unwrap();
        let baton = tokio::time::timeout(Duration::from_secs(2), staying)
            .await
            .expect("the client behind a departed one must still be released")
            .unwrap();
        drop(baton);
    }

    #[test]
    fn a_second_holder_cannot_take_a_live_lease() {
        let gate = gate();
        gate.quiesce("migration-a", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        let error = gate
            .quiesce("migration-b", Duration::from_secs(5), MAX_TTL, source())
            .unwrap_err();
        assert!(matches!(error, QuiesceError::LeaseHeld { .. }));
    }

    #[test]
    fn the_same_holder_renews_rather_than_conflicting_with_itself() {
        let gate = gate();
        let first = gate
            .quiesce("migration", Duration::from_millis(100), MAX_TTL, source())
            .unwrap();
        let second = gate
            .quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        assert!(second.expires_in() > first.ttl);
    }

    #[test]
    fn a_lease_longer_than_the_ceiling_is_refused() {
        let gate = gate();
        let error = gate
            .quiesce("migration", Duration::from_secs(600), MAX_TTL, source())
            .unwrap_err();
        assert!(matches!(error, QuiesceError::TtlTooLong { .. }));
    }

    #[test]
    fn an_expired_lease_unquiesces_back_to_the_source() {
        let gate = gate();
        gate.quiesce("migration", Duration::from_millis(1), MAX_TTL, source())
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(gate.reap().unwrap().rollback, Some(source()));
        assert!(gate.is_open());
        assert!(gate.lease().is_none());
    }

    #[test]
    fn an_expired_lease_after_a_resume_does_not_move_the_tenant_back() {
        let gate = gate();
        gate.quiesce("migration", Duration::from_millis(1), MAX_TTL, source())
            .unwrap();
        gate.resume("migration").unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(gate.reap().unwrap().rollback, None);
    }

    #[test]
    fn a_renewal_does_not_re_arm_a_rollback_the_holder_gave_up() {
        let gate = gate();
        gate.quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        gate.resume("migration").unwrap();
        gate.quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        assert!(!gate.lease().unwrap().reversible());
    }

    #[test]
    fn unquiesce_without_a_quiesce_is_refused_rather_than_ignored() {
        assert_eq!(
            gate().unquiesce("migration"),
            Err(QuiesceError::NotQuiesced)
        );
        assert_eq!(gate().resume("migration"), Err(QuiesceError::NotQuiesced));
    }

    #[test]
    fn a_route_cannot_be_moved_while_the_tenant_is_still_being_admitted() {
        let gate = gate();
        assert_eq!(
            gate.assert_held("migration"),
            Err(QuiesceError::NotQuiesced)
        );
        gate.quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        assert_eq!(gate.assert_held("migration"), Ok(()));
        gate.resume("migration").unwrap();
        assert_eq!(gate.assert_held("migration"), Err(QuiesceError::NotHeld));
    }

    #[test]
    fn in_flight_holders_are_counted_until_they_drop() {
        let gate = gate();
        let held = gate.hold();
        assert_eq!(gate.in_flight(), 1);
        drop(held);
        assert_eq!(gate.in_flight(), 0);
    }

    #[test]
    fn the_registry_reaps_every_expired_lease_at_once() {
        let registry = QuiesceRegistry::new();
        for tenant in ["alpha", "beta"] {
            registry
                .gate(tenant)
                .quiesce("migration", Duration::from_millis(1), MAX_TTL, source())
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
        let reaped = registry.reap_expired();
        assert_eq!(reaped.len(), 2);
        assert!(registry.gate("alpha").is_open());
        assert!(registry.quiesced().is_empty());
    }

    #[test]
    fn a_gate_is_held_only_while_a_live_lease_keeps_it_closed() {
        let gate = gate();
        assert!(!gate.held());
        gate.quiesce("switchover", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        assert!(gate.held());
        gate.resume("switchover").unwrap();
        assert!(!gate.held(), "an open gate holds nobody");
    }

    /// The fence reads this from the data path and the sweep runs on the
    /// control plane's timer, so the two are never ordered. A lease that has
    /// run out must stop counting as a deliberate hold the instant it runs out.
    #[test]
    fn an_expired_lease_stops_holding_before_anything_reaps_it() {
        let gate = gate();
        gate.quiesce("switchover", Duration::from_millis(1), MAX_TTL, source())
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert!(!gate.is_open(), "the sweep has not run yet");
        assert!(!gate.held());
    }

    #[test]
    fn a_tenant_gate_and_an_instance_gate_of_the_same_name_are_two_gates() {
        let registry = QuiesceRegistry::new();
        registry
            .instance("alpha")
            .quiesce("switchover", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        assert!(registry.instance("alpha").held());
        assert!(registry.gate("alpha").is_open());
    }

    /// The composition the instance-scoped lease exists for: two holders, two
    /// leases, neither taking the other's.
    #[test]
    fn a_migration_and_a_switchover_hold_the_same_traffic_without_conflicting() {
        let registry = QuiesceRegistry::new();
        registry
            .gate("alpha")
            .quiesce("migration", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        registry
            .instance("inst-a")
            .quiesce("switchover", Duration::from_secs(5), MAX_TTL, source())
            .unwrap();
        assert!(registry.gate("alpha").held());
        assert!(registry.instance("inst-a").held());
    }

    #[test]
    fn instance_leases_are_reaped_apart_from_tenant_ones() {
        let registry = QuiesceRegistry::new();
        registry
            .gate("alpha")
            .quiesce("migration", Duration::from_millis(1), MAX_TTL, source())
            .unwrap();
        registry
            .instance("inst-a")
            .quiesce("switchover", Duration::from_millis(1), MAX_TTL, source())
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(registry.reap_expired().len(), 1);
        assert_eq!(registry.reap_expired_instances().len(), 1);
    }

    #[test]
    fn the_registry_lists_every_tenant_it_has_a_gate_for() {
        let registry = QuiesceRegistry::new();
        registry.gate("beta");
        registry.gate("alpha");
        registry.instance("inst-a");
        assert_eq!(registry.tenants(), vec!["alpha", "beta"]);
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !condition() {
            assert!(Instant::now() < deadline, "the condition never held");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
