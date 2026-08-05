//! The pool manager: pool keys, checkout, check-in, and the admission ladder.
//!
//! Three separate accounts meet here, and keeping them distinct is what makes
//! the pool's ceiling explainable.
//!
//! - [`Allocator`] owns *capacity*. Every checkout goes through
//!   [`Allocator::try_lease`] and nothing else decides whether a client may hold
//!   a backend. A slot is fungible.
//! - [`Pool`] owns *physical links*, keyed by [`PoolKey`]. A link is not
//!   fungible: two clients whose keys differ may never touch the same socket,
//!   so a capacity slot whose parked link belongs to another key is honoured by
//!   closing that link and opening a new one, never by reusing it.
//! - [`BudgetLedger`] owns the *split* between reusable and pinned. A pinned
//!   link still occupies a backend connection but has left the elastic pool, and
//!   without this account the effective ceiling silently drops with no way to
//!   say why.
//!
//! Release is decided by [`ServerLink::can_check_in`] and by nothing else. There
//! is no second predicate in this file, and no caller is allowed to write one.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use pgelastic_capacity::{
    Admission, Allocator, ClientId, ConnectRejection, DenialReason, Disposition, Grant, Lease,
    RequestKind, TenantId as CapacityTenant, TenantSpec, TicketId,
};
use pgelastic_pool::{
    ConnectDecision, ConnectGate, ConnectPermit, GlobalStatementCache, LoginFailure, PinReason,
    PoolKey, Priority, ServerLink, ServerStatements, WaitQueue, Waiter, jittered_lifetime,
};
use pgelastic_wire::{BackendKeyData, BackendMessage, StartupMessage};
use tokio::io::AsyncWrite;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::config::PoolConfig;
use crate::epoch::{Epoch, EpochSource, FenceRuntime, InDoubtKey};
use crate::error::{ProxyError, Result};
use crate::metrics::{ConnectGateOutcome, Metrics};
use crate::relay::FrameRelay;
use crate::route::InstanceId;
use crate::scram::KdfPool;
use crate::stream::BackendStream;
use crate::tls::BackendTls;
use crate::vars::VariableCache;

/// Spread applied to `serverLifetime` so a pool opened in one second does not
/// recycle every link in one second an hour later.
const LIFETIME_JITTER_PERCENT: u32 = 10;

/// A refusal on its way to a client, already in wire terms.
///
/// The taxonomy is API surface, not diagnostics: the message leads with the
/// `PGE` code so a client that cannot read SQLSTATE still gets a stable token
/// to match on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    pub sqlstate: &'static str,
    pub message: String,
}

impl Denial {
    fn from_reason(reason: &DenialReason) -> Self {
        Self {
            sqlstate: reason.sqlstate(),
            message: format!("{}: {reason}", reason.code()),
        }
    }

    fn backend(message: impl Into<String>) -> Self {
        Self {
            sqlstate: crate::error::sqlstate::CONNECTION_FAILURE,
            message: message.into(),
        }
    }

    fn login(failure: &LoginFailure) -> Self {
        Self {
            sqlstate: crate::error::sqlstate::intern(&failure.sqlstate),
            message: failure.message.clone(),
        }
    }

    /// A checkout refused because the backend behind it is on a superseded
    /// primary epoch. Nothing was forwarded, so this is a definite failure and
    /// safe to retry — which is exactly what distinguishes it from
    /// [`ProxyError::OutcomeUnknown`](crate::error::ProxyError::OutcomeUnknown).
    fn superseded(message: impl std::fmt::Display) -> Self {
        Self {
            sqlstate: crate::error::sqlstate::READ_ONLY_SQL_TRANSACTION,
            message: format!("{}: {message}", crate::error::fence_code::SUPERSEDED_EPOCH),
        }
    }
}

impl std::fmt::Display for Denial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.sqlstate)
    }
}

/// One physical backend link and everything the pool knows about it.
#[derive(Debug)]
pub struct BackendConn {
    pub stream: BackendStream,
    /// The single buffer on this socket. The handshake's [`MessageBuffer`]
    /// residue is folded in here at hand-over, because two buffers over one
    /// stream lose whichever bytes land in the one nobody is reading.
    pub relay: FrameRelay,
    pub link: ServerLink,
    pub statements: ServerStatements,
    pub vars: VariableCache,
    /// The backend's real cancel key. Never handed to a client.
    pub key_data: Option<BackendKeyData>,
    /// Where this link is dialled, kept behind an `Arc` because it is written once when the
    /// link opens and copied at every checkout to route a cancel. As a `String` that copy was
    /// a heap allocation per transaction.
    pub address: Arc<str>,
    /// The primary epoch this link was opened — or last verified — under.
    ///
    /// The fence compares this against the highest epoch the proxy has ever
    /// seen, and anything below it is severed before it can carry another
    /// client's write to a postmaster that is about to be rewound.
    pub epoch: Epoch,
    /// The backend's own PID, one of the four fields the in-doubt log is keyed
    /// by. Read from the verification probe, which is the only place it is
    /// available on a link the proxy did not just open.
    pub backend_pid: Option<i32>,
    /// The last WAL LSN this link reported, and `None` if it never reported
    /// one. Not the LSN of any particular commit — it bounds the region a
    /// reconciliation has to inspect, which is what the in-doubt log needs it
    /// for.
    pub lsn: Option<String>,
}

impl BackendConn {
    /// Best-effort `Terminate`, so the server logs a logout rather than an
    /// unexpected EOF.
    pub async fn close(mut self) {
        crate::session::terminate_backend(&mut self.stream).await;
    }

    /// Severs the link with an RST, without waiting for anything.
    ///
    /// The fence's primitive. A `Terminate` is a request the backend may honour
    /// after it has finished the `COMMIT` it is in the middle of, and that
    /// commit is exactly what must not happen.
    pub fn sever(self) {
        self.stream.sever();
    }

    /// The in-doubt key for a transaction on this link.
    pub fn in_doubt_key(&self, tenant: &str, epoch: Epoch) -> InDoubtKey {
        InDoubtKey::new(tenant, epoch, self.backend_pid, self.lsn.clone())
    }
}

/// A backend held by a client, with the lease that entitles it to hold it.
#[derive(Debug)]
pub struct Checkout {
    pub server: pgelastic_capacity::ServerId,
    pub conn: BackendConn,
    lease: Lease,
}

/// The per-`PoolKey` view: which links exist under this identity.
#[derive(Debug)]
struct Pool {
    idle: BTreeSet<pgelastic_capacity::ServerId>,
    active: BTreeSet<pgelastic_capacity::ServerId>,
    /// The `ParameterStatus` set a new client of this pool is greeted with.
    ///
    /// Cached from the first link opened under the key so that the twentieth
    /// client does not have to hold a backend just to learn the server's
    /// `TimeZone`.
    greeting: Option<Arc<Vec<BackendMessage>>>,
    /// One backend connect at a time, and the cached failure the rest are
    /// refused with.
    gate: ConnectGate,
    /// Bumped whenever this pool's connect slot settles.
    ///
    /// Subscribed to under the manager's lock and in the same critical section
    /// that read `AlreadyInFlight`, which is what makes the wake-up impossible
    /// to miss: a settle that has not happened by then still has a receiver to
    /// reach.
    settled: watch::Sender<u64>,
}

impl Pool {
    fn new(login_retry: Duration) -> Self {
        Self {
            idle: BTreeSet::new(),
            active: BTreeSet::new(),
            greeting: None,
            gate: ConnectGate::new(login_retry),
            settled: watch::Sender::new(0),
        }
    }
}

/// A link parked between clients, under the key it may be reused for.
#[derive(Debug)]
struct Parked {
    key: PoolKey,
    conn: BackendConn,
    /// When this link was last given back, which is what `serverIdleTimeout`
    /// measures. Stamped at every park rather than at open: a link handed round
    /// all day has been idle for none of it.
    since: std::time::Instant,
}

#[derive(Debug)]
struct Inner {
    allocator: Allocator,
    pools: HashMap<PoolKey, Pool>,
    parked: HashMap<pgelastic_capacity::ServerId, Parked>,
    waits: HashMap<CapacityTenant, Arc<WaitQueue<Grant>>>,
    ledger: pgelastic_pool::BudgetLedger,
    statements: GlobalStatementCache,
    tenants: BTreeSet<CapacityTenant>,
    login_retry: Duration,
}

/// Everything a checkout needs to open a link it could not find parked.
pub struct Connector<'a> {
    pub backend: &'a crate::config::BackendConfig,
    pub tls: Option<&'a BackendTls>,
    pub kdf: &'a KdfPool,
    pub startup: &'a StartupMessage,
}

impl std::fmt::Debug for Connector<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("address", &self.backend.address)
            .finish_non_exhaustive()
    }
}

/// The map of pools, the capacity allocator and the pinning ledger.
#[derive(Debug)]
pub struct PoolManager {
    /// The instance this manager's capacity belongs to.
    ///
    /// One manager per instance is what bounds a write stall: the backends a
    /// stalled primary parks in `IPC.SyncRep` are drawn from this account and
    /// no other, so a tenant on a different instance cannot be starved by them.
    instance: InstanceId,
    inner: Mutex<Inner>,
    config: PoolConfig,
    fence: FenceRuntime,
    metrics: Arc<Metrics>,
    next_link_id: AtomicU64,
    /// The statement deadline in seconds, zero for none.
    ///
    /// An atomic rather than a field of `config`, because this is read once per
    /// statement on every link in the estate and `config` is only replaced by
    /// building a new process. It is deliberately not behind `inner`: taking
    /// the pool's mutex per statement would put a contended lock on the hot
    /// path of every query the proxy carries.
    query_deadline_seconds: AtomicU64,
    /// The idle-in-transaction bound in seconds, zero for none. An atomic for
    /// the same reason as the deadline above it.
    client_idle_in_transaction_seconds: AtomicU64,
    /// The pinned share ceiling as a percentage, zero for none.
    max_pinned_percent: AtomicU64,
    /// How long a link may sit parked, in seconds, zero for no reaping.
    server_idle_timeout_seconds: AtomicU64,
    /// The parameters a link's own variable cache follows.
    tracked: Arc<crate::vars::Tracked>,
    /// How long one link may stay pinned, in seconds, zero for no bound.
    max_pin_duration_seconds: AtomicU64,
    /// When the budget gauges were last refreshed, in milliseconds since
    /// `budget_epoch`. See [`publish_budget`](Self::publish_budget).
    budget_published_ms: AtomicU64,
    budget_epoch: std::time::Instant,
}

/// How stale a budget gauge is allowed to be on the checkout path.
///
/// Metrics are scraped on the order of seconds, so bounding staleness here costs an observer
/// nothing it can see. What it buys is large: refreshing the gauges reads the ledger, which
/// needs this manager's own lock, and the transaction state machine asks for a refresh at
/// fifteen separate points. On the checkout path that turned observability into a contended
/// acquisition per state transition, on a mutex both runtime workers share.
///
/// It applies to that path and to nothing else. Anything that moves the *ceiling* rather than
/// the occupancy — a pin, an unpin, a configuration reload, a route change — publishes at once
/// through [`PoolManager::publish_budget_now`]. A ceiling that dropped is the number an
/// operator reads to find out why, and it has to be true the moment the thing that moved it
/// happened rather than a few milliseconds afterwards.
const BUDGET_PUBLISH_INTERVAL_MS: u64 = 5;

impl PoolManager {
    pub fn new(
        instance: InstanceId,
        config: PoolConfig,
        tracked: Arc<crate::vars::Tracked>,
        fence: FenceRuntime,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        let pool_spec = pgelastic_capacity::PoolSpec {
            backend_connections: config.backend_connections,
            headroom_percent: config.headroom_percent,
            max_client_connections: config.max_client_connections,
            max_oversubscription: None,
            mode: config.mode.into(),
            fd_budget: None,
        };
        let admission = pgelastic_capacity::AdmissionSpec {
            strategy: pgelastic_capacity::AdmissionStrategy::WeightedDeficit,
            queue_depth_per_tenant: config.queue_depth_per_tenant,
            max_wait: config.query_wait_timeout(),
        };
        let query_deadline_seconds = config.query_deadline_seconds;
        let client_idle_in_transaction_seconds = config.client_idle_in_transaction_seconds;
        let max_pinned_percent = u64::from(config.max_pinned_percent);
        let server_idle_timeout_seconds = config.server_idle_timeout_seconds;
        let max_pin_duration_seconds = config.max_pin_duration_seconds;
        let mut allocator = Allocator::new(pool_spec, admission)
            .map_err(|e| ProxyError::config(format!("pool capacity: {e}")))?;

        let mut tenants = BTreeSet::new();
        for tenant in &config.tenants {
            let id = CapacityTenant::new(tenant.name.as_str());
            allocator
                .add_tenant(
                    id.clone(),
                    TenantSpec {
                        guaranteed: tenant.guaranteed,
                        burstable: tenant.burstable,
                        weight: tenant.weight,
                        priority: tenant.priority,
                        max_client_connections: tenant.max_client_connections,
                        storage_bytes: u64::MAX,
                    },
                )
                .map_err(|e| ProxyError::config(format!("tenant {}: {e}", tenant.name)))?;
            tenants.insert(id);
        }

        let ledger = pgelastic_pool::BudgetLedger::new(config.backend_connections);
        let login_retry = config.server_login_retry();
        Ok(Arc::new(Self {
            instance,
            inner: Mutex::new(Inner {
                allocator,
                pools: HashMap::new(),
                parked: HashMap::new(),
                waits: HashMap::new(),
                ledger,
                statements: GlobalStatementCache::with_capacity(config.max_global_statements),
                tenants,
                login_retry,
            }),
            config,
            fence,
            metrics,
            next_link_id: AtomicU64::new(1),
            query_deadline_seconds: AtomicU64::new(query_deadline_seconds),
            client_idle_in_transaction_seconds: AtomicU64::new(client_idle_in_transaction_seconds),
            max_pinned_percent: AtomicU64::new(max_pinned_percent),
            server_idle_timeout_seconds: AtomicU64::new(server_idle_timeout_seconds),
            max_pin_duration_seconds: AtomicU64::new(max_pin_duration_seconds),
            tracked,
            budget_published_ms: AtomicU64::new(0),
            budget_epoch: std::time::Instant::now(),
        }))
    }

    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    pub fn fence(&self) -> &FenceRuntime {
        &self.fence
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("the pool manager is never poisoned")
    }

    /// Binds a tenant seen for the first time.
    ///
    /// An unconfigured tenant is `BestEffort` — no guarantee — and has **no
    /// ceiling of its own**, so the pool's ceiling is the only one it can meet.
    /// The distinction is not cosmetic: a tenant that has hit its own ceiling is
    /// told `PGE1928 raise burstable`, and telling that to a tenant whose
    /// `burstable` was never set would name a knob that does not exist. A tenant
    /// nobody has given a claim to waits for the pool instead.
    ///
    /// A tenant whose guarantee could not be honoured is rejected rather than
    /// silently degraded, which is why this can fail.
    pub fn ensure_tenant(&self, name: &str) -> Result<CapacityTenant, Denial> {
        let id = CapacityTenant::new(name);
        let mut inner = self.lock();
        if inner.tenants.contains(&id) {
            return Ok(id);
        }
        let spec = TenantSpec {
            guaranteed: 0,
            burstable: u32::MAX,
            weight: 100,
            priority: 1_000,
            max_client_connections: u32::MAX,
            storage_bytes: u64::MAX,
        };
        inner.allocator.add_tenant(id.clone(), spec).map_err(|e| {
            Denial::backend(format!(
                "tenant {name} cannot be admitted to this pool: {e}"
            ))
        })?;
        inner.tenants.insert(id.clone());
        Ok(id)
    }

    /// Adopts the per-tenant claims the operator published.
    ///
    /// Applied under the allocator's own lock, which is held by a checkout for
    /// the moment it takes to decide and never across a statement — so a claim
    /// changes between two transactions of a session and never inside one. A
    /// tenant whose ceiling rises has its queued clients re-examined
    /// immediately, which is what makes raising `burstable` take effect for the
    /// clients already waiting on it rather than only for the next arrivals.
    ///
    /// Returns the number of claims accepted; the rest were refused and logged.
    /// Adopts the pool limits a running replica may change without being rebuilt.
    ///
    /// Returns whether anything moved, so a reload can say what it did. The
    /// value is read at the next checkout rather than mid-statement, which is
    /// what keeps a limit change from altering a transaction already underway.
    pub fn apply_limits(&self, config: &PoolConfig) -> bool {
        let deadline = config.query_deadline_seconds;
        let idle = config.client_idle_in_transaction_seconds;
        // Both swaps run: short-circuiting on the first would leave the second
        // limit unapplied whenever they change in the same document.
        let deadline_moved = self
            .query_deadline_seconds
            .swap(deadline, Ordering::Relaxed)
            != deadline;
        let idle_moved = self
            .client_idle_in_transaction_seconds
            .swap(idle, Ordering::Relaxed)
            != idle;
        let pinned = u64::from(config.max_pinned_percent);
        let pinned_moved = self.max_pinned_percent.swap(pinned, Ordering::Relaxed) != pinned;
        let reap = config.server_idle_timeout_seconds;
        let reap_moved = self
            .server_idle_timeout_seconds
            .swap(reap, Ordering::Relaxed)
            != reap;
        let pin_for = config.max_pin_duration_seconds;
        let pin_for_moved = self
            .max_pin_duration_seconds
            .swap(pin_for, Ordering::Relaxed)
            != pin_for;
        deadline_moved || idle_moved || pinned_moved || pin_for_moved || reap_moved
    }

    /// The statement deadline in force now, or `None` when the pool sets none.
    #[must_use]
    pub fn query_deadline(&self) -> Option<Duration> {
        let seconds = self.query_deadline_seconds.load(Ordering::Relaxed);
        (seconds > 0).then(|| Duration::from_secs(seconds))
    }

    /// The idle-in-transaction bound in force now, or `None` when there is none.
    #[must_use]
    pub fn client_idle_in_transaction(&self) -> Option<Duration> {
        let seconds = self
            .client_idle_in_transaction_seconds
            .load(Ordering::Relaxed);
        (seconds > 0).then(|| Duration::from_secs(seconds))
    }

    pub fn apply_tenants(&self, tenants: &[crate::config::TenantConfig]) -> usize {
        let mut inner = self.lock();
        let mut changed = 0;
        for tenant in tenants {
            let id = CapacityTenant::new(tenant.name.as_str());
            let spec = TenantSpec {
                guaranteed: tenant.guaranteed,
                burstable: tenant.burstable,
                weight: tenant.weight,
                priority: tenant.priority,
                max_client_connections: tenant.max_client_connections,
                storage_bytes: u64::MAX,
            };
            let outcome = if inner.tenants.contains(&id) {
                inner.allocator.set_tenant_spec(&id, spec)
            } else {
                inner
                    .allocator
                    .add_tenant(id.clone(), spec)
                    .map(|()| Vec::new())
            };
            match outcome {
                Ok(grants) => {
                    inner.tenants.insert(id);
                    changed += 1;
                    inner.dispatch(grants);
                }
                Err(error) => {
                    warn!(tenant = tenant.name, %error, "a published tenant claim was refused");
                }
            }
        }
        changed += Self::forget_departed(&mut inner, tenants);
        changed
    }

    /// Gives back the budget a tenant that has left the document was holding.
    ///
    /// `apply_tenants` only ever added and updated, so a deleted tenant kept its entry - and
    /// with it its `guaranteed`, which `free()` subtracts from the pool whether anybody is
    /// using it or not. Every surviving tenant was refused capacity on behalf of one that no
    /// longer existed, on every replica, until the process happened to restart.
    ///
    /// Absence from the document is not proof of deletion, which is why this only takes
    /// tenants holding nothing. `proxyTenants` also omits a tenant whose backend credential it
    /// could not read this pass, and one whose database name a sibling holds; dropping those
    /// would kill live sessions over a Secret that was briefly unreadable. A tenant with
    /// backends open keeps its entry and is reconsidered next time, by which point the reaper
    /// has closed its idle links.
    fn forget_departed(inner: &mut Inner, published: &[crate::config::TenantConfig]) -> usize {
        let known: std::collections::HashSet<CapacityTenant> = published
            .iter()
            .map(|tenant| CapacityTenant::new(tenant.name.as_str()))
            .collect();
        let departed: Vec<CapacityTenant> = inner
            .tenants
            .iter()
            .filter(|id| !known.contains(*id))
            .filter(|id| {
                inner
                    .allocator
                    .budget()
                    .tenant(id)
                    .is_none_or(|entry| entry.live() == 0)
            })
            .cloned()
            .collect();

        let mut forgotten = 0;
        for id in departed {
            match inner.allocator.remove_tenant(&id) {
                Ok(grants) => {
                    inner.tenants.remove(&id);
                    forgotten += 1;
                    inner.dispatch(grants);
                }
                Err(error) => {
                    warn!(tenant = %id, %error, "a departed tenant could not be forgotten");
                }
            }
        }
        forgotten
    }

    pub fn connect_client(&self, tenant: &CapacityTenant) -> std::result::Result<ClientId, Denial> {
        self.lock()
            .allocator
            .connect_client(tenant)
            .map_err(|rejection| match rejection {
                ConnectRejection::Denied(reason) => Denial::from_reason(&reason),
                ConnectRejection::UnknownTenant(tenant) => {
                    Denial::backend(format!("no tenant {tenant} is bound to this pool"))
                }
            })
    }

    pub fn disconnect_client(&self, client: ClientId) {
        let mut inner = self.lock();
        let grants = inner.allocator.disconnect_client(client);
        inner.dispatch(grants);
    }

    /// Draws step 0 of the ladder — the dedicated cancel credit pool — for one
    /// `CancelRequest` aimed at `client`.
    ///
    /// A cancel takes a fresh unauthenticated socket, so it is a backend
    /// connection like any other and has to be admitted like one. It cannot go
    /// through the normal rungs: a cancel that queues behind the query it is
    /// cancelling never completes, and a tenant sitting at its burst ceiling is
    /// precisely the tenant whose clients most need to cancel. So the credit
    /// pool is bounded at `min(8, burstable)` and sits outside `total` — a
    /// cancel storm can neither consume tenant capacity nor be starved by it.
    pub fn lease_cancel(
        self: &Arc<Self>,
        client: ClientId,
    ) -> std::result::Result<CancelCredit, Denial> {
        let mut inner = self.lock();
        match inner.allocator.try_lease(client, RequestKind::Cancel) {
            Admission::Granted(lease) => Ok(CancelCredit {
                manager: Arc::clone(self),
                lease: Some(lease),
            }),
            Admission::Denied(reason) => {
                self.metrics.admission_denied(reason.code());
                Err(Denial::from_reason(&reason))
            }
            Admission::Stale => Err(Denial::backend(
                "the client a cancel names is no longer connected",
            )),
            // Step 0 grants or denies and has no queue to put a ticket in.
            Admission::Queued { .. } => Err(Denial::backend("a cancel request was queued")),
        }
    }

    /// Returns a cancel's credit. The connection it paid for is already gone —
    /// the server closes a cancel socket as soon as it has read the request.
    fn release_cancel(&self, lease: Lease) {
        let mut inner = self.lock();
        let outcome = inner.allocator.release(lease);
        inner.dispatch(outcome.grants);
    }

    /// The cached greeting for a pool, if any link has ever been opened under
    /// this key.
    pub fn greeting(&self, key: &PoolKey) -> Option<Arc<Vec<BackendMessage>>> {
        self.lock().pools.get(key)?.greeting.clone()
    }

    pub fn intern_statement(
        &self,
        key: pgelastic_pool::StatementKey,
    ) -> Arc<pgelastic_pool::PreparedStatement> {
        self.lock().statements.intern(key)
    }

    /// Reports the elastic/pinned split for the metrics exposition.
    pub fn ledger_snapshot(&self) -> LedgerSnapshot {
        let inner = self.lock();
        LedgerSnapshot {
            limit: inner.ledger.limit(),
            elastic: inner.ledger.elastic(),
            elastic_limit: inner.ledger.elastic_limit(),
            pinned: PinReason::ALL.map(|reason| (reason, inner.ledger.pinned_for(reason))),
            statements: inner.statements.len(),
            statements_evicted: inner.statements.evicted(),
        }
    }

    /// Moves a link out of the elastic pool and into the pinned account.
    ///
    /// The connection is still a real backend connection and still counts
    /// against the pool's total; what it has left is the *reusable* pool, and
    /// [`BudgetLedger::elastic_limit`](pgelastic_pool::BudgetLedger::elastic_limit)
    /// is the ceiling that drops as a result. Without this split the drop has no
    /// attributable cause.
    /// Refuses the pin when the pinned account is already at its share of the
    /// budget, reporting whether the link may be pinned at all.
    ///
    /// The caller must close a link it was refused a pin for. There is no third
    /// option: the state that provoked the pin is state no reset removes, so
    /// handing the link on would give one tenant's `LISTEN`, cursor or advisory
    /// lock to whoever gets it next.
    pub fn record_pin(&self, reason: PinReason) -> PinOutcome {
        let outcome = {
            let mut inner = self.lock();
            let ceiling = pin_ceiling(inner.ledger.limit(), self.max_pinned_percent());
            if let Some(ceiling) = ceiling
                && inner.ledger.pinned() >= ceiling
            {
                PinOutcome::Refused {
                    pinned: inner.ledger.pinned(),
                    ceiling,
                }
            } else if let Err(error) = inner.ledger.pin(reason) {
                warn!(%error, %reason, "pinning a link the ledger does not know about");
                PinOutcome::Pinned
            } else {
                PinOutcome::Pinned
            }
        };
        self.publish_budget_now();
        outcome
    }

    /// How long a link may stay pinned, or `None` when the pool sets no bound.
    #[must_use]
    pub fn max_pin_duration(&self) -> Option<Duration> {
        let seconds = self.max_pin_duration_seconds.load(Ordering::Relaxed);
        (seconds > 0).then(|| Duration::from_secs(seconds))
    }

    /// The pinned share ceiling in force now, or `None` when the pool sets none.
    #[must_use]
    pub fn max_pinned_percent(&self) -> Option<u32> {
        let percent = self.max_pinned_percent.load(Ordering::Relaxed);
        (percent > 0).then(|| u32::try_from(percent).unwrap_or(u32::MAX))
    }

    /// Returns a pinned link to the elastic pool, once the client that dirtied
    /// it has gone and the scrub that removes the state has run.
    pub fn release_pin(&self, reason: PinReason) {
        {
            let mut inner = self.lock();
            if let Err(error) = inner.ledger.unpin(reason) {
                warn!(%error, %reason, "unpinning a link the ledger does not know about");
            }
        }
        self.publish_budget_now();
    }

    fn record_unpin(inner: &mut Inner, pin: Option<PinReason>) {
        match pin {
            Some(reason) => {
                if inner.ledger.close_pinned(reason).is_err() {
                    let _ = inner.ledger.close();
                }
            }
            None => {
                let _ = inner.ledger.close();
            }
        }
    }

    /// The whole checkout path: admission, then a link.
    ///
    /// `client` is written to only while the request is queued, and only with a
    /// `NoticeResponse` naming the limit it is waiting on.
    pub async fn acquire<W: AsyncWrite + Unpin>(
        &self,
        request: &AcquireRequest<'_>,
        connector: &Connector<'_>,
        client: &mut W,
    ) -> std::result::Result<Checkout, Denial> {
        let admission = {
            let mut inner = self.lock();
            let admission = inner
                .allocator
                .try_lease(request.client, RequestKind::Normal);
            match admission {
                Admission::Queued {
                    ticket, blocked_by, ..
                } => {
                    let queue = inner.wait_queue(request.tenant);
                    let waiter = queue
                        .enqueue(Priority::Normal, Instant::now())
                        .map_err(|_| Denial::backend("the pool is shutting down"))?;
                    Waiting::Queued(waiter, ticket, blocked_by)
                }
                Admission::Granted(lease) => Waiting::Granted(lease),
                Admission::Denied(reason) => Waiting::Denied(reason),
                Admission::Stale => Waiting::Denied(DenialReason::PoolCapacity {
                    live: 0,
                    total: self.config.backend_connections,
                }),
            }
        };

        let lease = match admission {
            Waiting::Granted(lease) => lease,
            Waiting::Denied(reason) => {
                self.metrics.admission_denied(reason.code());
                return Err(Denial::from_reason(&reason));
            }
            Waiting::Queued(waiter, ticket, blocked_by) => {
                self.await_grant(request, waiter, ticket, &blocked_by, client)
                    .await?
                    .lease
            }
        };

        self.attach(request, connector, lease).await
    }

    /// Waits for the queue to reach this client, emitting the notice threshold
    /// on the way and refusing with `PGE1024` at the deadline.
    ///
    /// This is the span that matters most in the proxy, and until now it did not exist. The
    /// admission-queue wait is the whole point of the capacity model - it is where
    /// oversubscription is either working or hurting - and from outside it is
    /// indistinguishable from a slow query: the client simply waits. The counters say how
    /// many waited and the histogram says how long they waited in aggregate, but neither can
    /// answer "why was *this* statement slow", which is the question somebody actually has.
    ///
    /// `outcome` and `waited_ms` are recorded on the span rather than logged as events, so the
    /// wait is one record with a duration rather than two lines to correlate. The subscriber
    /// emits span close records, which is what makes those fields observable at all; the shape
    /// is the one an OTLP layer will export when the proxy grows one, which it has not yet.
    #[tracing::instrument(
        name = "proxy.admission_wait",
        skip_all,
        fields(
            tenant = %request.tenant,
            blocked_by = %blocked_by.code(),
            outcome = tracing::field::Empty,
            waited_ms = tracing::field::Empty,
        )
    )]
    async fn await_grant<W: AsyncWrite + Unpin>(
        &self,
        request: &AcquireRequest<'_>,
        waiter: Waiter<Grant>,
        ticket: TicketId,
        blocked_by: &DenialReason,
        client: &mut W,
    ) -> std::result::Result<Grant, Denial> {
        self.metrics.admission_queued();
        let started = Instant::now();
        let mut ticket = TicketGuard {
            manager: self,
            tenant: request.tenant.clone(),
            ticket,
            settled: false,
        };
        tokio::pin!(waiter);

        let notice = tokio::time::sleep(self.config.notify_after());
        tokio::pin!(notice);
        let deadline = tokio::time::sleep(self.config.query_wait_timeout());
        tokio::pin!(deadline);
        let mut notified = false;

        loop {
            tokio::select! {
                grant = &mut waiter => {
                    ticket.settled = true;
                    self.metrics.admission_dequeued();
                    Self::record_wait("granted", started.elapsed());
                    return grant.map_err(|_| Denial::backend("the pool is shutting down"));
                }
                () = &mut notice, if !notified => {
                    notified = true;
                    let code = blocked_by.code();
                    let text = format!(
                        "{code}: waiting for a backend connection ({blocked_by}); \
                         waited {:?} of {:?}",
                        started.elapsed(),
                        self.config.query_wait_timeout(),
                    );
                    let _ = crate::wire_io::write_backend(
                        client,
                        &[crate::wire_io::notice(blocked_by.sqlstate(), &text)],
                    )
                    .await;
                }
                () = &mut deadline => {
                    let reason = DenialReason::AdmissionTimeout {
                        tenant: request.tenant.clone(),
                        waited: started.elapsed(),
                    };
                    self.metrics.admission_denied(reason.code());
                    Self::record_wait("timed_out", started.elapsed());
                    return Err(Denial::from_reason(&reason));
                }
            }
        }
    }

    /// Records how the admission wait ended, on the span the caller is inside.
    ///
    /// A span whose `outcome` is unset is a wait that never returned at all: the client hung
    /// up and the future was dropped. That is a real state and worth being able to see, which
    /// is why this does not try to be a `Drop` guard that always fires.
    fn record_wait(outcome: &'static str, waited: std::time::Duration) {
        let span = tracing::Span::current();
        span.record("outcome", outcome);
        span.record(
            "waited_ms",
            u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
        );
    }

    /// Severs every parked link opened under a superseded epoch.
    ///
    /// **Sever, do not deregister.** The link is closed with an RST rather than
    /// a `Terminate`, because a graceful close leaves the backend free to
    /// finish whatever it was doing first, and a demoted primary finishing one
    /// more commit is the entire failure this fence exists to prevent.
    ///
    /// Returns how many were severed. This is the entry point for the epoch
    /// watcher and the background sweeper; the checkout path does the same
    /// scan through [`take_superseded`](Self::take_superseded), inside the lock
    /// it already holds to claim. Between them the ordering the design requires
    /// — *before any further checkout* — holds by construction rather than by a
    /// timer being fast enough, and the sweeper only makes it prompt for a pool
    /// nobody is checking out of.
    pub fn sever_superseded(&self) -> usize {
        let current = self.fence.current();
        let severed = {
            let mut inner = self.lock();
            Self::take_superseded(&mut inner, current)
        };
        let count = severed.len();
        self.sever_all(severed, current);
        count
    }

    /// Removes every parked link below `current` from a caller-held lock.
    ///
    /// Split out so the checkout path can do this inside the lock it already
    /// takes to claim. Two acquisitions — one to sever, one to claim — is not
    /// merely slower: `check_in` parks a link without consulting the epoch, so
    /// the gap between them is a window in which a link superseded while it was
    /// checked out is parked just after the scan walked past it, and the claim
    /// then takes it.
    ///
    /// With `verify_at_checkout` set that is caught one round trip later by
    /// [`verify_epoch`](Self::verify_epoch) and costs only a wasted connection.
    /// **Without it there is no second check**, and the client is handed a link
    /// to a demoted primary — which is the whole failure the fence exists to
    /// prevent. Doing both under one lock closes the window by construction
    /// rather than leaving it to a backstop that is switched off by
    /// configuration.
    fn take_superseded(inner: &mut Inner, current: Epoch) -> Vec<BackendConn> {
        let stale: Vec<pgelastic_capacity::ServerId> = inner
            .parked
            .iter()
            .filter(|(_, parked)| parked.conn.epoch < current)
            .map(|(server, _)| *server)
            .collect();

        let mut severed = Vec::with_capacity(stale.len());
        for server in stale {
            if let Some(conn) = inner.unpark(server) {
                let grants = inner.allocator.backend_died(server);
                inner.dispatch(grants);
                severed.push(conn);
            }
        }
        severed
    }

    /// Closes links that have sat parked longer than the pool allows.
    ///
    /// A parked link is a backend `PostgreSQL` is holding open for nobody - a
    /// process, its `work_mem`, and one of the instance's `max_connections`. The
    /// pool opens them on demand and gave none of them back, so an estate's
    /// connection count ratcheted to its busiest minute and stayed there.
    ///
    /// **A tenant is never reaped below its guarantee.** The guarantee is the one
    /// promise this allocator makes that nothing else may take back: `acquire`
    /// admits a tenant under its floor without queueing, and closing a link that
    /// puts it there would turn the next arrival from an immediate grant into a
    /// connect. Idle is not the same as unpromised.
    ///
    /// Shaped like [`sever_superseded`](Self::sever_superseded), and for the same
    /// reason: the removal happens under the lock so no checkout can claim a link
    /// that is about to be closed, and the sockets are closed after it is
    /// released.
    pub fn reap_idle(&self) {
        // Before the idle-timeout guard on purpose. Reaping links is configurable and off by
        // default; the map sweep is a bound on what a client can make this process allocate,
        // and a bound that a configuration knob can switch off is not one.
        let Some(idle_for) = self.server_idle_timeout() else {
            Self::forget_empty_pools(&mut self.lock());
            return;
        };
        let reaped = {
            let mut inner = self.lock();
            let reaped = Self::take_idle(&mut inner, idle_for);
            Self::forget_empty_pools(&mut inner);
            reaped
        };
        if reaped.is_empty() {
            return;
        }
        self.metrics.backends_reaped(reaped.len());
        for conn in reaped {
            self.metrics.backend_closed();
            // Spawned, like every other close on this path: a Terminate is
            // best-effort and the reaper must not wait on a backend that has
            // stopped answering to get to the next one.
            tokio::spawn(conn.close());
        }
        self.publish_budget_now();
    }

    /// Drops the per-key state of a pool that is holding nothing.
    ///
    /// `pools` is keyed on the whole `PoolKey`, and part of that key is the startup
    /// fingerprint - which the client chooses. A client that varies a tracked startup
    /// parameter on every connection therefore mints a new entry every time, and nothing ever
    /// removed one: the map grew for as long as the process lived, on input an unauthenticated
    /// peer supplies, until the proxy died and took every tenant's sessions with it.
    ///
    /// Only entries holding no link at all are dropped, so nothing in use is disturbed. What
    /// is lost is the cached greeting, which the next client of that key pays one backend
    /// round trip to rebuild - the same price the first one paid.
    fn forget_empty_pools(inner: &mut Inner) {
        inner
            .pools
            .retain(|_, pool| !pool.idle.is_empty() || !pool.active.is_empty());
    }

    /// Removes every link parked longer than `idle_for` that its tenant does not
    /// need to honour a guarantee, from a caller-held lock.
    fn take_idle(inner: &mut Inner, idle_for: Duration) -> Vec<BackendConn> {
        let now = std::time::Instant::now();
        let stale: Vec<pgelastic_capacity::ServerId> = inner
            .parked
            .iter()
            .filter(|(_, parked)| now.duration_since(parked.since) >= idle_for)
            .map(|(server, _)| *server)
            .collect();

        // The tenant's parked links, counted once and decremented as they go.
        //
        // The *physical* links, deliberately, and not the allocator's `live`
        // count. A capacity slot outlives its link on every `discard` - an
        // abandoned session, a failed reset, an expired `serverLifetime` - and it
        // also counts links the allocator has already marked `close_needed` for
        // somebody else's guarantee, which `revocation_candidates` nets out for
        // exactly this reason. Reaping against `live` closes a tenant's last warm
        // link while reporting its floor covered, and an adversarial review
        // reproduced that both ways.
        let mut parked_per_tenant: HashMap<CapacityTenant, u32> = HashMap::new();
        for parked in inner.parked.values() {
            *parked_per_tenant
                .entry(CapacityTenant::new(parked.key.tenant().as_str()))
                .or_default() += 1;
        }

        let mut reaped = Vec::new();
        for server in stale {
            let Some(tenant) = inner
                .parked
                .get(&server)
                .map(|parked| CapacityTenant::new(parked.key.tenant().as_str()))
            else {
                continue;
            };
            let guaranteed = inner
                .allocator
                .budget()
                .tenant(&tenant)
                .map_or(0, pgelastic_capacity::TenantEntry::guaranteed);
            // The floor is a count of warm links kept, so a tenant at it keeps
            // what it has however long any one of them has been sitting.
            if parked_per_tenant.get(&tenant).copied().unwrap_or(0) <= guaranteed {
                continue;
            }
            if let Some(conn) = inner.unpark(server) {
                let grants = inner.allocator.backend_died(server);
                inner.dispatch(grants);
                reaped.push(conn);
                *parked_per_tenant.entry(tenant).or_default() -= 1;
            }
        }
        reaped
    }

    /// How long a link may sit parked, or `None` when the pool reaps nothing.
    #[must_use]
    pub fn server_idle_timeout(&self) -> Option<Duration> {
        let seconds = self.server_idle_timeout_seconds.load(Ordering::Relaxed);
        (seconds > 0).then(|| Duration::from_secs(seconds))
    }

    /// Closes what [`take_superseded`](Self::take_superseded) removed, with the
    /// lock released — the sockets are already unreachable to any later
    /// checkout, so the ordering the fence needs is satisfied before this runs.
    fn sever_all(&self, severed: Vec<BackendConn>, current: Epoch) {
        for conn in severed {
            debug!(
                epoch = %conn.epoch,
                %current,
                "severing a parked backend socket opened under a superseded primary epoch"
            );
            self.metrics.backend_closed();
            self.metrics
                .backend_severed(crate::epoch::FenceAction::Close);
            conn.sever();
        }
    }

    /// Turns a capacity slot into a physical link under `request.key`.
    async fn attach(
        &self,
        request: &AcquireRequest<'_>,
        connector: &Connector<'_>,
        lease: Lease,
    ) -> std::result::Result<Checkout, Denial> {
        let server = lease.server;
        let current = self.fence.current();
        // Severed and claimed under one lock. A link parked across a promotion
        // is precisely the one that would carry this client to a demoted
        // primary, so the sever has to be ordered before the claim — and taking
        // the lock twice to do it leaves a window between them in which exactly
        // such a link can be parked.
        let (superseded, parked, stale) = {
            let mut inner = self.lock();
            let superseded = Self::take_superseded(&mut inner, current);
            let (parked, stale) = inner.claim(server, request.key);
            (superseded, parked, stale)
        };
        self.sever_all(superseded, current);
        if let Some(stale) = stale {
            self.metrics.backend_closed();
            tokio::spawn(stale.close());
        }

        if let Some(mut conn) = parked {
            if conn
                .link
                .apply(pgelastic_pool::ServerEvent::Assigned)
                .is_ok()
            {
                conn.link.check_lifetime(Instant::now());
                match self.verify_epoch(&mut conn).await {
                    Ok(()) => {
                        self.metrics.checkout(true);
                        return Ok(Checkout {
                            server,
                            conn,
                            lease,
                        });
                    }
                    Err(denial) => {
                        // The parked link is gone, but the capacity slot is
                        // fungible: fall through and open a fresh one, which
                        // will reach whatever the Service now points at.
                        debug!(%denial, "a parked link failed its epoch check and is severed");
                        {
                            let mut inner = self.lock();
                            let _ = inner.ledger.close();
                            if let Some(pool) = inner.pools.get_mut(request.key) {
                                pool.active.remove(&server);
                            }
                        }
                        self.metrics.backend_closed();
                        self.metrics
                            .backend_severed(crate::epoch::FenceAction::Close);
                        conn.sever();
                        return self.open_verified(request, connector, server, lease).await;
                    }
                }
            }
            // The link is in a state a client can no longer be handed, so it is
            // dropped and replaced rather than repaired.
            {
                let mut inner = self.lock();
                let _ = inner.ledger.close();
                if let Some(pool) = inner.pools.get_mut(request.key) {
                    pool.active.remove(&server);
                }
            }
            self.metrics.backend_closed();
            tokio::spawn(conn.close());
        }

        self.open_verified(request, connector, server, lease).await
    }

    /// Opens a fresh link and refuses to hand it over unless it proves which
    /// epoch it is serving.
    ///
    /// A newly opened link that is already superseded means the address the
    /// proxy dialled is a demoted primary. There is nothing to retry against,
    /// so the client is refused: a stalled tenant is recoverable and a write to
    /// a postmaster that is about to be rewound is not.
    async fn open_verified(
        &self,
        request: &AcquireRequest<'_>,
        connector: &Connector<'_>,
        server: pgelastic_capacity::ServerId,
        lease: Lease,
    ) -> std::result::Result<Checkout, Denial> {
        let opened = match self.gated_open(request, connector, server).await {
            Ok(mut conn) => match self.verify_epoch(&mut conn).await {
                Ok(()) => Ok(conn),
                Err(denial) => {
                    self.metrics.backend_closed();
                    self.metrics
                        .backend_severed(crate::epoch::FenceAction::Close);
                    let mut inner = self.lock();
                    let _ = inner.ledger.close();
                    if let Some(pool) = inner.pools.get_mut(request.key) {
                        pool.active.remove(&server);
                    }
                    drop(inner);
                    conn.sever();
                    Err(denial)
                }
            },
            Err(denial) => Err(denial),
        };

        match opened {
            Ok(conn) => {
                self.metrics.checkout(false);
                Ok(Checkout {
                    server,
                    conn,
                    lease,
                })
            }
            Err(denial) => {
                let mut inner = self.lock();
                let grants = inner.allocator.backend_died(server);
                inner.dispatch(grants);
                Err(denial)
            }
        }
    }

    /// The pull/verify path, run on every link before a client touches it.
    ///
    /// Mandatory rather than an optimisation: a proxy cut off from the API
    /// server and unreachable by the promoting agent still learns, from the
    /// connection itself, which postmaster is on the other end of it. That is
    /// why this runs on parked links as well as fresh ones, and why it is the
    /// last thing between a checkout and a client.
    async fn verify_epoch(&self, conn: &mut BackendConn) -> std::result::Result<(), Denial> {
        if self.fence.verify_at_checkout {
            let probe = crate::epoch::verify::probe(&mut conn.stream, &mut conn.relay)
                .await
                .map_err(|error| {
                    Denial::backend(format!(
                        "verifying a backend's primary epoch failed: {error}"
                    ))
                })?;
            if probe.backend_pid.is_some() {
                conn.backend_pid = probe.backend_pid;
            }
            if probe.lsn.is_some() {
                conn.lsn = probe.lsn;
            }
            match probe.epoch {
                Some(observed) => {
                    let observation = self.fence.fence.observe(EpochSource::Verify, observed);
                    self.metrics
                        .epoch_observed(EpochSource::Verify, observation.into());
                    self.metrics.primary_epoch(self.fence.current());
                    conn.epoch = observed;
                }
                None if self.fence.require_epoch => {
                    return Err(Denial::superseded(
                        "this backend carries no pgelastic.primary_epoch, so the epoch it is \
                         serving cannot be established",
                    ));
                }
                // The backend publishes no epoch at all. That is not evidence
                // that it is superseded, so the link is tagged with what the
                // proxy knows and a later push or watch still fences it.
                None => conn.epoch = self.fence.current(),
            }
        } else {
            conn.epoch = self.fence.current();
        }

        let current = self.fence.current();
        if conn.epoch < current {
            return Err(Denial::superseded(format!(
                "this backend is serving primary epoch {} and the cluster has reached {current}",
                conn.epoch
            )));
        }
        Ok(())
    }

    /// Opens a link through the pool's connect gate.
    ///
    /// One connect per pool key is in flight at a time. Everything else that
    /// wants a link either waits for that attempt to settle or, if it has
    /// already failed inside `serverLoginRetry`, is refused with the remembered
    /// error without dialling. That is the whole thundering-herd defense: two
    /// hundred clients reconnecting at a backend that has just come back open
    /// one socket between them, not two hundred.
    async fn gated_open(
        &self,
        request: &AcquireRequest<'_>,
        connector: &Connector<'_>,
        server: pgelastic_capacity::ServerId,
    ) -> std::result::Result<BackendConn, Denial> {
        loop {
            let gated = {
                let mut inner = self.lock();
                let pool = inner.pool(request.key);
                match pool.gate.try_start(Instant::now()) {
                    ConnectDecision::Start(permit) => Gated::Open(ConnectSlot {
                        manager: self,
                        key: request.key.clone(),
                        permit: Some(permit),
                    }),
                    ConnectDecision::AlreadyInFlight => Gated::Wait(pool.settled.subscribe()),
                    ConnectDecision::BackingOff(failure) => Gated::Refused(failure),
                }
            };

            match gated {
                Gated::Refused(failure) => {
                    self.metrics.connect_gated(ConnectGateOutcome::FastFailed);
                    return Err(Denial::login(&failure));
                }
                Gated::Wait(mut settled) => {
                    self.metrics.connect_gated(ConnectGateOutcome::Deferred);
                    // A closed channel means the pool was forgotten, which can
                    // only leave the gate free; looping re-reads it either way.
                    let _ = settled.changed().await;
                }
                Gated::Open(slot) => {
                    self.metrics.connect_gated(ConnectGateOutcome::Attempted);
                    return match self.open_transiently(request, connector, server).await {
                        Ok(conn) => {
                            slot.succeeded();
                            Ok(conn)
                        }
                        Err(error) => {
                            let failure = slot.failed(
                                LoginFailure::new(
                                    error.sqlstate(),
                                    format!("opening a backend connection failed: {error}"),
                                ),
                                Instant::now(),
                            );
                            Err(Denial::login(&failure))
                        }
                    };
                }
            }
        }
    }

    /// Opens a backend, retrying only while the failure is the endpoint still moving.
    ///
    /// A planned switchover repoints `<instance>-rw` at the promoted member, and kube-proxy
    /// converges its conntrack rules some time after the API server says the `EndpointSlice`
    /// changed. A connect that lands in that window is refused. Without this the first
    /// refusal is recorded as a login failure, which arms the gate's `serverLoginRetry`
    /// backoff and fast-fails every queued client for the next fifteen seconds — so a
    /// sub-second endpoint gap became a fifteen-second outage for a tenant whose clients had
    /// just been held specifically so they would not notice.
    ///
    /// The retry stays inside the connect slot deliberately: other clients keep waiting on
    /// the gate rather than stampeding the backend, which is the property the gate exists for.
    /// Only transport failures are retried — an authentication failure or a missing database
    /// is answered immediately, because those do not become true by waiting.
    async fn open_transiently(
        &self,
        request: &AcquireRequest<'_>,
        connector: &Connector<'_>,
        server: pgelastic_capacity::ServerId,
    ) -> Result<BackendConn> {
        const ATTEMPTS: u32 = 5;
        let mut backoff = Duration::from_millis(50);
        for attempt in 1..=ATTEMPTS {
            match self.open(request, connector, server).await {
                Ok(conn) => return Ok(conn),
                Err(error) if attempt < ATTEMPTS && transport_failure(&error) => {
                    self.metrics.connect_gated(ConnectGateOutcome::Retried);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the loop returns on the final attempt")
    }

    async fn open(
        &self,
        request: &AcquireRequest<'_>,
        connector: &Connector<'_>,
        server: pgelastic_capacity::ServerId,
    ) -> Result<BackendConn> {
        {
            let mut inner = self.lock();
            inner
                .ledger
                .open()
                .map_err(|e| ProxyError::backend(format!("backend budget: {e}")))?;
        }

        let session = match crate::backend::connect(
            connector.backend,
            connector.tls,
            connector.kdf,
            connector.startup,
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                let mut inner = self.lock();
                let _ = inner.ledger.close();
                return Err(error);
            }
        };
        self.metrics
            .backend_auth(crate::metrics::AuthOutcome::Success);
        self.metrics.backend_opened();

        let id = pgelastic_pool::ServerId::new(self.next_link_id.fetch_add(1, Ordering::Relaxed));
        let mut link = ServerLink::new(id, request.key.clone());
        // Recorded while the link is still in `login`, where the queue is
        // deliberately not fed, so the release gate has a transaction status to
        // read without a request ever having been made.
        link.observe_backend(&BackendMessage::ReadyForQuery(
            pgelastic_wire::TransactionStatus::Idle,
        ))
        .map_err(|e| ProxyError::backend(e.to_string()))?;
        link.apply(pgelastic_pool::ServerEvent::LoginSucceeded)
            .map_err(|e| ProxyError::backend(e.to_string()))?;
        link.apply(pgelastic_pool::ServerEvent::Assigned)
            .map_err(|e| ProxyError::backend(e.to_string()))?;
        link.set_deadline(
            Instant::now()
                + jittered_lifetime(
                    self.config.server_lifetime(),
                    LIFETIME_JITTER_PERCENT,
                    server.0,
                ),
        );

        let mut vars = VariableCache::with_tracked(Arc::clone(&self.tracked));
        // The zero-round-trip half of the verify path: if the epoch GUC is
        // `GUC_REPORT` the postmaster volunteers it in the start-up parameter
        // set, and the probe below has nothing left to learn.
        let mut reported_epoch = None;
        for message in &session.parameters {
            if let BackendMessage::ParameterStatus(status) = message {
                vars.observe(&status.name, &status.value);
                if let Some((epoch, observation)) =
                    crate::epoch::verify::observe_parameter_status(&self.fence.fence, status)
                {
                    self.metrics
                        .epoch_observed(EpochSource::Verify, observation.into());
                    reported_epoch = Some(epoch);
                }
            }
        }
        self.metrics.primary_epoch(self.fence.current());

        let mut relay = FrameRelay::new(
            crate::relay::DEFAULT_INLINE_FRAME_BYTES,
            crate::relay::DEFAULT_MAX_FRAME_BYTES,
        );
        relay.extend_from_slice(session.buf.as_slice());

        let mut inner = self.lock();
        let pool = inner.pool(request.key);
        pool.active.insert(server);
        if pool.greeting.is_none() {
            pool.greeting = Some(Arc::new(session.parameters.clone()));
        }
        drop(inner);

        Ok(BackendConn {
            stream: session.stream,
            relay,
            link,
            statements: ServerStatements::new(self.config.max_server_statements),
            vars,
            backend_pid: session.key_data.as_ref().map(|data| data.process_id),
            key_data: session.key_data,
            address: Arc::from(connector.backend.address.as_str()),
            epoch: reported_epoch.unwrap_or_else(|| self.fence.current()),
            lsn: None,
        })
    }

    /// Returns a link to the pool. The caller has already run the reset ladder
    /// and has already had `Ok(())` from [`ServerLink::can_check_in`].
    pub fn check_in(&self, key: &PoolKey, checkout: Checkout) {
        let Checkout {
            server,
            mut conn,
            lease,
        } = checkout;
        // Asserted before the state moves, because the gate's first condition
        // is that the link is in a state a release can be reached from, and
        // `idle` — where the link is about to land — is not one of them.
        debug_assert_eq!(
            conn.link.can_check_in(),
            Ok(()),
            "a link may only be checked in through the release gate"
        );
        let _ = match conn.link.state() {
            pgelastic_pool::ServerState::Active => {
                conn.link.apply(pgelastic_pool::ServerEvent::Released)
            }
            pgelastic_pool::ServerState::Tested => {
                conn.link.apply(pgelastic_pool::ServerEvent::ResetSucceeded)
            }
            _ => Ok(conn.link.state()),
        };

        let mut inner = self.lock();
        inner.park(server, key.clone(), conn);
        let outcome = inner.allocator.release(lease);
        let closing = match outcome.disposition {
            Disposition::Idle(_) => None,
            // `close_needed` set by a revocation, a cancel connection, or a
            // lease that no longer refers to a live checkout: the slot is gone
            // and the link goes with it.
            Disposition::Closed(_) | Disposition::Stale | Disposition::Pinned(_) => {
                inner.unpark(server)
            }
        };
        inner.dispatch(outcome.grants);
        drop(inner);

        self.metrics.check_in();
        if let Some(conn) = closing {
            self.metrics.backend_closed();
            tokio::spawn(conn.close());
        }
    }

    /// Drops a link without returning it: it failed, expired, or carries state
    /// no reset can remove.
    ///
    /// The capacity slot is released all the same. A slot is fungible, so the
    /// next client to be granted it simply opens a new link — which is the whole
    /// reason the physical account and the capacity account are separate.
    pub fn discard(&self, checkout: Checkout, reason: crate::metrics::BackendCloseReason) {
        let Checkout {
            server,
            conn,
            lease,
        } = checkout;
        let pin = conn.link.pin();
        let key = conn.link.key().clone();

        let mut inner = self.lock();
        if let Some(pool) = inner.pools.get_mut(&key) {
            pool.active.remove(&server);
            pool.idle.remove(&server);
        }
        Self::record_unpin(&mut inner, pin);
        let outcome = inner.allocator.release(lease);
        inner.dispatch(outcome.grants);
        drop(inner);

        self.metrics.backend_closed();
        self.metrics.backend_close(reason);
        tokio::spawn(conn.close());
    }

    /// Drops a checked-out link with an RST rather than a `Terminate`.
    ///
    /// [`discard`](Self::discard) asks the backend to go away and lets it
    /// finish first. The fence cannot afford that: the statement it would be
    /// finishing is the write `pg_rewind` is about to discard.
    pub fn sever(&self, checkout: Checkout, action: crate::epoch::FenceAction) {
        let Checkout {
            server,
            conn,
            lease,
        } = checkout;
        let pin = conn.link.pin();
        let key = conn.link.key().clone();

        let mut inner = self.lock();
        if let Some(pool) = inner.pools.get_mut(&key) {
            pool.active.remove(&server);
            pool.idle.remove(&server);
        }
        Self::record_unpin(&mut inner, pin);
        let outcome = inner.allocator.release(lease);
        inner.dispatch(outcome.grants);
        drop(inner);

        self.metrics.backend_closed();
        self.metrics.backend_severed(action);
        conn.sever();
    }

    /// Refreshes the exported budget gauges from the ledger.
    pub fn publish_budget(&self) {
        // Refuse to take the lock if the gauges were refreshed recently enough. The
        // compare_exchange is what stops two workers both deciding it is their turn: the loser
        // returns rather than queueing behind the winner, because a gauge does not need to be
        // written twice in the same millisecond.
        let elapsed = self.budget_epoch.elapsed();
        let now = elapsed
            .as_secs()
            .saturating_mul(1000)
            .saturating_add(u64::from(elapsed.subsec_millis()));
        let last = self.budget_published_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < BUDGET_PUBLISH_INTERVAL_MS {
            return;
        }
        if self
            .budget_published_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.publish_budget_now();
    }

    /// Refreshes the budget gauges without waiting for the interval.
    ///
    /// For the paths where a stale gauge would be misread as a fact rather than as lag: a
    /// configuration reload, or a route change that moves capacity between instances. Those
    /// happen on the order of seconds, so the lock they take is not on anybody's hot path.
    pub fn publish_budget_now(&self) {
        let snapshot = self.ledger_snapshot();
        self.metrics.budget(
            &self.instance,
            snapshot.limit,
            snapshot.elastic,
            snapshot.elastic_limit,
        );
        self.metrics
            .statement_cache(snapshot.statements, snapshot.statements_evicted);
    }
}

/// Builds the identity a link opened for this client may ever be reused under.
///
/// Every axis along which two sessions can differ observably is a field, so
/// "could this link leak state across a boundary?" reduces to "are the keys
/// equal?". `RESET ALL` restores GUCs to their *session-start* values, so the
/// startup parameters are part of identity rather than of state and no reset
/// ladder can undo them.
pub async fn pool_key(
    config: &crate::config::Config,
    policy: &pgelastic_pool::FingerprintPolicy,
    backend: &crate::config::BackendConfig,
    startup: &StartupMessage,
    tenant: &str,
    credential_generation: u64,
) -> Result<PoolKey> {
    use pgelastic_pool::{
        BackendTarget, CredentialGeneration, DatabaseName, PoolKeySpec, ReplicationKind, RoleName,
        StartupFingerprint, TenantId, TlsPosture,
    };

    let text =
        |value: Option<&Bytes>| value.map(|value| String::from_utf8_lossy(value).into_owned());
    let replication = text(startup.get(b"replication"))
        .as_deref()
        .map_or(ReplicationKind::None, |value| {
            ReplicationKind::from_startup_value(value).unwrap_or(ReplicationKind::None)
        });

    let fingerprint = StartupFingerprint::build(
        startup.parameters.iter().filter_map(|(name, value)| {
            let name = String::from_utf8_lossy(name).into_owned();
            (!name.starts_with("_pq_."))
                .then(|| (name, String::from_utf8_lossy(value).into_owned()))
        }),
        policy,
    )
    .map_err(|e| ProxyError::client(e.to_string()))?;

    let address = crate::config::resolve(&backend.address).await?;
    let database = backend
        .database
        .clone()
        .or_else(|| text(startup.get(b"database")))
        .unwrap_or_else(|| tenant.to_owned());

    Ok(PoolKey::new(PoolKeySpec {
        tenant: TenantId::new(tenant),
        target: BackendTarget::new(address.ip().to_string(), address.port()),
        database: DatabaseName::new(database),
        role: RoleName::new(&backend.user),
        fingerprint,
        tls: match backend.tls.mode {
            crate::config::BackendTlsMode::Disable => TlsPosture::Plaintext,
            _ => TlsPosture::Tls,
        },
        replication,
        configured_mode: config.pool.mode.into(),
        // Fed from the tenant's own entry, so re-issuing a credential makes every link opened
        // under the old one unreachable from any new binding: they drain out through
        // serverLifetime rather than being handed to somebody holding a secret PostgreSQL no
        // longer accepts.
        //
        // Passed in rather than looked up here. The generation lives in the half of the
        // document a running process adopts, and this function is handed the half it was
        // started with - so reading it from `config` would key every link on the generation
        // this process booted with and quietly undo the eviction above.
        credentials: CredentialGeneration::new(credential_generation),
    }))
}

/// One cancel's place in the tenant's cancel credit pool, returned on drop.
#[derive(Debug)]
pub struct CancelCredit {
    manager: Arc<PoolManager>,
    lease: Option<Lease>,
}

impl Drop for CancelCredit {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.manager.release_cancel(lease);
        }
    }
}

/// What the connect gate said, once the manager's lock has been given back.
enum Gated<'a> {
    Open(ConnectSlot<'a>),
    Wait(watch::Receiver<u64>),
    Refused(Arc<LoginFailure>),
}

/// The pool's single connect slot, held for as long as an attempt is running.
///
/// Whatever ends the attempt — success, failure, or the whole future being
/// dropped — wakes the clients queued behind it, and the wake-up is published
/// under the manager's lock so it cannot land between a waiter reading
/// `AlreadyInFlight` and that waiter subscribing.
struct ConnectSlot<'a> {
    manager: &'a PoolManager,
    key: PoolKey,
    permit: Option<ConnectPermit>,
}

impl ConnectSlot<'_> {
    fn succeeded(mut self) {
        if let Some(permit) = self.permit.take() {
            permit.succeeded();
        }
    }

    fn failed(mut self, failure: LoginFailure, now: Instant) -> Arc<LoginFailure> {
        match self.permit.take() {
            Some(permit) => permit.failed(failure, now),
            None => Arc::new(failure),
        }
    }
}

impl Drop for ConnectSlot<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        let mut inner = self.manager.lock();
        inner.pool(&self.key).settled.send_modify(|seen| *seen += 1);
    }
}

/// What the ladder produced, before any awaiting happens.
enum Waiting {
    Granted(Lease),
    Queued(Waiter<Grant>, TicketId, DenialReason),
    Denied(DenialReason),
}

/// Removes an admission ticket however the wait ends.
///
/// The `Waiter`'s own `Drop` deregisters it from the wait queue; this
/// deregisters the matching ticket from the allocator. Both queues are strict
/// FIFO within a tenant and both lose the same entry, so their heads stay in
/// lockstep — which is what lets a grant be delivered to the head of the wait
/// queue without carrying a client identity through the handoff.
struct TicketGuard<'a> {
    manager: &'a PoolManager,
    tenant: CapacityTenant,
    ticket: TicketId,
    settled: bool,
}

impl Drop for TicketGuard<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut inner = self.manager.lock();
        inner.allocator.cancel_ticket(&self.tenant, self.ticket);
    }
}

/// The identity a checkout is made under.
#[derive(Debug)]
pub struct AcquireRequest<'a> {
    pub key: &'a PoolKey,
    pub tenant: &'a CapacityTenant,
    pub client: ClientId,
}

/// The elastic/pinned split, for the metrics exposition.
#[derive(Debug, Clone)]
pub struct LedgerSnapshot {
    pub limit: u32,
    pub elastic: u32,
    pub elastic_limit: u32,
    pub pinned: [(PinReason, u32); PinReason::ALL.len()],
    /// Distinct statement texts interned, and how many the bound has discarded.
    ///
    /// Published together because either alone misleads: a full table is the normal steady
    /// state, and only the eviction count rising with it says the capacity is too small for
    /// the query variety actually arriving.
    pub statements: usize,
    pub statements_evicted: u64,
}

impl Inner {
    fn pool(&mut self, key: &PoolKey) -> &mut Pool {
        let login_retry = self.login_retry;
        self.pools
            .entry(key.clone())
            .or_insert_with(|| Pool::new(login_retry))
    }

    fn wait_queue(&mut self, tenant: &CapacityTenant) -> Arc<WaitQueue<Grant>> {
        Arc::clone(
            self.waits
                .entry(tenant.clone())
                .or_insert_with(|| Arc::new(WaitQueue::new())),
        )
    }

    /// Takes the link parked in `server`'s slot if it may be reused under `key`.
    ///
    /// A slot is fungible and a link is not. A parked link opened under a
    /// different key is returned as the second element for the caller to close:
    /// reusing it would hand one identity's session to another, which is the one
    /// thing the pool key exists to prevent.
    fn claim(
        &mut self,
        server: pgelastic_capacity::ServerId,
        key: &PoolKey,
    ) -> (Option<BackendConn>, Option<BackendConn>) {
        let Some(parked) = self.parked.remove(&server) else {
            return (None, None);
        };
        if let Some(pool) = self.pools.get_mut(&parked.key) {
            pool.idle.remove(&server);
        }
        if &parked.key == key {
            self.pool(&parked.key).active.insert(server);
            return (Some(parked.conn), None);
        }
        let _ = self.ledger.close();
        debug!(%server, "a capacity slot changed pool key; its link is closed rather than reused");
        (None, Some(parked.conn))
    }

    fn park(&mut self, server: pgelastic_capacity::ServerId, key: PoolKey, conn: BackendConn) {
        let pool = self.pool(&key);
        pool.active.remove(&server);
        pool.idle.insert(server);
        self.parked.insert(
            server,
            Parked {
                key,
                conn,
                since: std::time::Instant::now(),
            },
        );
    }

    fn unpark(&mut self, server: pgelastic_capacity::ServerId) -> Option<BackendConn> {
        let parked = self.parked.remove(&server)?;
        if let Some(pool) = self.pools.get_mut(&parked.key) {
            pool.idle.remove(&server);
            pool.active.remove(&server);
        }
        PoolManager::record_unpin(self, parked.conn.link.pin());
        Some(parked.conn)
    }

    /// Hands each grant to the head of its tenant's wait queue.
    ///
    /// Strictly in the order the allocator produced them: within a tenant both
    /// queues are FIFO and both lose the same entries, so the *n*th grant
    /// belongs to the *n*th waiter. Handing them out in any other order would
    /// give one client's lease to another, and the lease is what a later
    /// disconnect uses to find the connection to retire.
    ///
    /// A grant nobody is waiting for is released again rather than dropped;
    /// releasing pops another ticket, so the allocator's queue strictly shrinks
    /// and the loop terminates.
    fn dispatch(&mut self, grants: Vec<Grant>) {
        let mut pending = std::collections::VecDeque::from(grants);
        while let Some(grant) = pending.pop_front() {
            let queue = self.wait_queue(&grant.tenant);
            if let Err(orphan) = queue.hand_off(grant) {
                let outcome = self.allocator.release(orphan.lease);
                pending.extend(outcome.grants);
            }
        }
    }
}

/// Whether a failed backend open is the transport still settling rather than a refusal.
///
/// Retrying an authentication failure or a missing database only delays the answer the
/// client is owed; retrying a refused or reset connect is what carries a tenant across the
/// moment its instance's Service is repointed.
fn transport_failure(error: &ProxyError) -> bool {
    let ProxyError::Io(io) = error else {
        return false;
    };
    // Refused, unreachable and timed out all mean nothing answered at that address, which
    // is what a Service being repointed looks like. A reset is deliberately excluded: it
    // means something did accept and then killed the connection, which is a different
    // condition and one far likelier to persist — retrying it just multiplies the load on a
    // backend that is already failing.
    matches!(
        io.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::HostUnreachable
            | std::io::ErrorKind::NetworkUnreachable
    )
}

/// How often the reaper looks. Coarse on purpose: `serverIdleTimeout` is measured
/// in minutes, and a link that lives ten seconds past its welcome has cost
/// nothing a tighter loop would have saved.
const REAP_INTERVAL: Duration = Duration::from_secs(10);

/// Closes each instance's over-idle parked links, for as long as the proxy runs.
///
/// A loop of its own rather than a step of the reload loop, which returns early
/// down several paths - a document that has not changed, one that does not parse -
/// and would take the reaper with it.
pub async fn reap_loop(fleet: Arc<crate::route::Fleet>, mut shutdown: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            () = tokio::time::sleep(REAP_INTERVAL) => {}
        }
        for instance in fleet.instances() {
            instance.pools.reap_idle();
        }
    }
}

/// What a pin request was allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinOutcome {
    Pinned,
    /// The pinned account is already at its share of the budget. The link must be
    /// closed rather than pinned or shared.
    Refused {
        pinned: u32,
        ceiling: u32,
    },
}

/// How many links of a budget may be pinned at once.
///
/// Rounded up, so a small pool with a small percentage is allowed one pinned link
/// rather than none: a ceiling of zero derived from a non-zero percentage would
/// refuse every `LISTEN` in the pool and read as a bug rather than a limit. A pool
/// that wants none says so by pinning being off.
fn pin_ceiling(limit: u32, percent: Option<u32>) -> Option<u32> {
    let percent = percent?;
    let ceiling = u64::from(limit) * u64::from(percent.min(100));
    Some(
        u32::try_from(ceiling.div_ceil(100))
            .unwrap_or(u32::MAX)
            .max(1),
    )
}

#[cfg(test)]
mod tests {

    /// A ceiling of zero derived from a non-zero percentage would refuse every
    /// LISTEN in a small pool and read as a bug rather than a limit.
    #[test]
    fn a_small_pool_with_a_small_percentage_still_gets_one_pinned_link() {
        assert_eq!(super::pin_ceiling(4, Some(20)), Some(1));
        assert_eq!(super::pin_ceiling(1, Some(1)), Some(1));
    }

    #[test]
    fn the_ceiling_rounds_up_and_never_exceeds_the_budget() {
        assert_eq!(super::pin_ceiling(100, Some(20)), Some(20));
        assert_eq!(super::pin_ceiling(10, Some(25)), Some(3));
        assert_eq!(super::pin_ceiling(10, Some(100)), Some(10));
        // A percentage above 100 is clamped rather than allowed to exceed the
        // budget it is a share of.
        assert_eq!(super::pin_ceiling(10, Some(250)), Some(10));
    }

    #[test]
    fn no_percentage_is_no_ceiling() {
        assert_eq!(super::pin_ceiling(100, None), None);
    }
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_refused_connect_is_the_service_still_moving_and_is_retried() {
        for kind in [
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::HostUnreachable,
        ] {
            let error = ProxyError::Io(std::io::Error::new(kind, "endpoint converging"));
            assert!(transport_failure(&error), "{kind:?} must be retried");
        }
    }

    #[test]
    fn an_answer_the_client_is_owed_is_never_retried() {
        assert!(
            !transport_failure(&ProxyError::AuthenticationFailed),
            "waiting does not make a wrong password right"
        );
        assert!(
            !transport_failure(&ProxyError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "no",
            ))),
            "a refusal on the merits is not the transport settling"
        );
        assert!(
            !transport_failure(&ProxyError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "accepted then killed",
            ))),
            "something accepted and reset us; that is not an endpoint still arriving"
        );
    }

    use crate::config::{PoolModeConfig, TenantConfig};

    fn manager(config: PoolConfig) -> Arc<PoolManager> {
        PoolManager::new(
            InstanceId::new("default"),
            config,
            std::sync::Arc::default(),
            FenceRuntime::in_memory(),
            Metrics::new(),
        )
        .unwrap()
    }

    fn transaction_config(backend_connections: u32) -> PoolConfig {
        PoolConfig {
            mode: PoolModeConfig::Transaction,
            backend_connections,
            ..PoolConfig::default()
        }
    }

    #[test]
    fn an_unconfigured_tenant_is_bound_on_first_sight_with_no_guarantee() {
        let manager = manager(transaction_config(4));
        let tenant = manager.ensure_tenant("acme").unwrap();
        assert_eq!(tenant.as_str(), "acme");
        // Idempotent: a second client of the same tenant must not re-add it.
        assert_eq!(manager.ensure_tenant("acme").unwrap(), tenant);
    }

    // apply_tenants only ever added and updated. A tenant deleted from the pool kept its
    // entry, and with it its guarantee, which free() subtracts whether anybody is using it or
    // not - so every surviving tenant was refused capacity on behalf of one that no longer
    // existed, on every replica, until the process happened to restart.
    #[test]
    fn a_tenant_that_leaves_the_document_gives_its_guarantee_back() {
        let claim = |name: &str, guaranteed: u32| TenantConfig {
            name: name.to_owned(),
            guaranteed,
            burstable: 4,
            weight: 100,
            priority: 1_000,
            max_client_connections: 10,
            ..TenantConfig::default()
        };
        let config = PoolConfig {
            mode: PoolModeConfig::Transaction,
            backend_connections: 10,
            headroom_percent: 0,
            tenants: vec![claim("stays", 2), claim("goes", 4)],
            ..PoolConfig::default()
        };
        let manager = PoolManager::new(
            InstanceId::new("default"),
            config,
            std::sync::Arc::default(),
            FenceRuntime::in_memory(),
            Metrics::new(),
        )
        .unwrap();
        let before = manager.lock().allocator.budget().free();

        manager.apply_tenants(&[claim("stays", 2)]);

        let after = manager.lock().allocator.budget().free();
        assert_eq!(
            after,
            before + 4,
            "the departed tenant's guarantee is still reserved against every survivor"
        );
        assert!(
            manager
                .lock()
                .allocator
                .budget()
                .tenant(&CapacityTenant::new("goes"))
                .is_none(),
            "the departed tenant still has an entry"
        );
    }

    // `pools` is keyed on the whole PoolKey, and part of that key is the startup fingerprint,
    // which the client chooses. A client varying a tracked parameter on every connection mints
    // a new entry each time and nothing removed one, so the map grew for as long as the
    // process lived - on input an unauthenticated peer supplies.
    #[test]
    fn a_pool_holding_nothing_is_not_kept_for_ever() {
        let manager = PoolManager::new(
            InstanceId::new("default"),
            PoolConfig {
                mode: PoolModeConfig::Transaction,
                backend_connections: 10,
                ..PoolConfig::default()
            },
            std::sync::Arc::default(),
            FenceRuntime::in_memory(),
            Metrics::new(),
        )
        .unwrap();
        {
            let mut inner = manager.lock();
            for index in 0..64 {
                let key = PoolKey::new(pgelastic_pool::PoolKeySpec {
                    tenant: pgelastic_pool::TenantId::new("acme"),
                    target: pgelastic_pool::BackendTarget::new("127.0.0.1", 5432),
                    database: pgelastic_pool::DatabaseName::new("acme"),
                    role: pgelastic_pool::RoleName::new("ops"),
                    fingerprint: pgelastic_pool::StartupFingerprint::build(
                        [("application_name", format!("client-{index}"))],
                        &pgelastic_pool::FingerprintPolicy::default(),
                    )
                    .unwrap(),
                    tls: pgelastic_pool::TlsPosture::Plaintext,
                    replication: pgelastic_pool::ReplicationKind::None,
                    configured_mode: pgelastic_pool::PoolMode::Transaction,
                    credentials: pgelastic_pool::CredentialGeneration::new(0),
                });
                inner.pools.insert(key, Pool::new(Duration::from_secs(1)));
            }
            assert_eq!(inner.pools.len(), 64);
        }

        manager.reap_idle();

        assert_eq!(
            manager.lock().pools.len(),
            0,
            "empty pools survive, so a client that varies its startup packet grows this map \
             without bound"
        );
    }

    #[test]
    fn a_guarantee_that_cannot_be_honoured_is_refused_at_construction() {
        let config = PoolConfig {
            mode: PoolModeConfig::Transaction,
            backend_connections: 4,
            headroom_percent: 50,
            tenants: vec![TenantConfig {
                name: "greedy".to_owned(),
                guaranteed: 4,
                burstable: 4,
                weight: 100,
                priority: 1_000,
                max_client_connections: 10,
                ..TenantConfig::default()
            }],
            ..PoolConfig::default()
        };
        assert!(
            PoolManager::new(
                InstanceId::new("default"),
                config,
                std::sync::Arc::default(),
                FenceRuntime::in_memory(),
                Metrics::new(),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn the_ladder_denies_a_tenant_at_its_own_ceiling_with_pge1928() {
        let config = PoolConfig {
            mode: PoolModeConfig::Transaction,
            backend_connections: 8,
            tenants: vec![TenantConfig {
                name: "small".to_owned(),
                guaranteed: 0,
                burstable: 1,
                weight: 100,
                priority: 1_000,
                max_client_connections: 10,
                ..TenantConfig::default()
            }],
            ..PoolConfig::default()
        };
        let manager = manager(config);
        let tenant = manager.ensure_tenant("small").unwrap();

        let first = manager.connect_client(&tenant).unwrap();
        let second = manager.connect_client(&tenant).unwrap();

        let mut inner = manager.lock();
        assert!(matches!(
            inner.allocator.try_lease(first, RequestKind::Normal),
            Admission::Granted(_)
        ));
        let Admission::Denied(reason) = inner.allocator.try_lease(second, RequestKind::Normal)
        else {
            panic!("the second checkout must exceed the tenant ceiling");
        };
        assert_eq!(Denial::from_reason(&reason).sqlstate, "53300");
        assert!(Denial::from_reason(&reason).message.starts_with("PGE1928"));
    }

    #[tokio::test]
    async fn a_full_pool_queues_rather_than_denying_and_reports_pge1936_as_the_cause() {
        let manager = manager(transaction_config(1));
        let tenant = manager.ensure_tenant("acme").unwrap();
        let holder = manager.connect_client(&tenant).unwrap();
        let waiter = manager.connect_client(&tenant).unwrap();

        let mut inner = manager.lock();
        assert!(matches!(
            inner.allocator.try_lease(holder, RequestKind::Normal),
            Admission::Granted(_)
        ));
        let Admission::Queued { blocked_by, .. } =
            inner.allocator.try_lease(waiter, RequestKind::Normal)
        else {
            panic!("a full pool must queue");
        };
        let denial = Denial::from_reason(&blocked_by);
        assert_eq!(denial.sqlstate, "53400");
        assert!(denial.message.starts_with("PGE1936"));
    }

    #[test]
    fn an_admission_timeout_is_pge1024() {
        let denial = Denial::from_reason(&DenialReason::AdmissionTimeout {
            tenant: CapacityTenant::new("acme"),
            waited: Duration::from_secs(120),
        });
        assert_eq!(denial.sqlstate, "53400");
        assert!(denial.message.starts_with("PGE1024"));
    }

    fn capped_pool(backend_connections: u32, burstable: u32) -> PoolConfig {
        PoolConfig {
            mode: PoolModeConfig::Transaction,
            backend_connections,
            tenants: vec![TenantConfig {
                name: "acme".to_owned(),
                guaranteed: 0,
                burstable,
                weight: 100,
                priority: 1_000,
                max_client_connections: 10,
                ..TenantConfig::default()
            }],
            ..PoolConfig::default()
        }
    }

    #[test]
    fn a_cancel_is_admitted_from_step_zero_while_the_tenant_is_at_its_ceiling() {
        let manager = manager(capped_pool(4, 1));
        let tenant = manager.ensure_tenant("acme").unwrap();
        let holder = manager.connect_client(&tenant).unwrap();
        let other = manager.connect_client(&tenant).unwrap();

        {
            let mut inner = manager.lock();
            assert!(matches!(
                inner.allocator.try_lease(holder, RequestKind::Normal),
                Admission::Granted(_)
            ));
            assert!(matches!(
                inner.allocator.try_lease(other, RequestKind::Normal),
                Admission::Denied(DenialReason::TenantCap { .. })
            ));
        }

        assert!(
            manager.lease_cancel(holder).is_ok(),
            "a cancel must bypass the rungs that just refused a normal checkout"
        );
    }

    #[test]
    fn a_cancel_storm_is_bounded_by_the_credit_pool_and_leaves_normal_capacity_alone() {
        let manager = manager(capped_pool(2, 4));
        let tenant = manager.ensure_tenant("acme").unwrap();
        let canceller = manager.connect_client(&tenant).unwrap();
        let worker = manager.connect_client(&tenant).unwrap();

        let credits = (0..4)
            .map(|_| {
                manager
                    .lease_cancel(canceller)
                    .expect("min(8, burstable) cancels fit in the credit pool")
            })
            .collect::<Vec<_>>();
        let refused = manager
            .lease_cancel(canceller)
            .expect_err("the storm must stop at the credit pool's edge");
        assert_eq!(refused.sqlstate, "53400");
        assert!(refused.message.starts_with("PGE1929"));

        {
            let mut inner = manager.lock();
            assert_eq!(inner.allocator.cancel_in_flight(&tenant), 4);
            for _ in 0..2 {
                assert!(
                    matches!(
                        inner.allocator.try_lease(worker, RequestKind::Normal),
                        Admission::Granted(_)
                    ),
                    "a cancel storm must not have taken any of the pool's own capacity"
                );
            }
        }
        assert_eq!(manager.ledger_snapshot().elastic, 0);

        drop(credits);
        assert_eq!(manager.lock().allocator.cancel_in_flight(&tenant), 0);
        assert!(
            manager.lease_cancel(canceller).is_ok(),
            "credit must come back when a cancel finishes"
        );
    }

    #[test]
    fn a_pinned_link_lowers_the_elastic_ceiling_and_is_counted_under_its_reason() {
        let manager = manager(transaction_config(4));
        {
            let mut inner = manager.lock();
            inner.ledger.open().unwrap();
            inner.ledger.open().unwrap();
        }
        manager.record_pin(PinReason::Listen);

        let snapshot = manager.ledger_snapshot();
        assert_eq!(snapshot.limit, 4);
        assert_eq!(snapshot.elastic, 1);
        assert_eq!(snapshot.elastic_limit, 3);
        assert_eq!(
            snapshot
                .pinned
                .iter()
                .find(|(reason, _)| *reason == PinReason::Listen)
                .unwrap()
                .1,
            1
        );
    }
}
