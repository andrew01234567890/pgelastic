//! The allocator: the admission ladder, revocation, the two-level scheduler,
//! and the client-connection currency.
//!
//! Every method takes `&mut self`. There is no interior mutability, no atomic,
//! no lock and no I/O anywhere in this file.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::budget::{CapacityBudget, TenantEntry};
use crate::config::{AdmissionSpec, AdmissionStrategy, CANCEL_CREDIT_CAP, PoolSpec, TenantSpec};
use crate::error::{ConfigError, ConnectRejection, DenialReason};
use crate::time::{Clock, SystemClock, Timestamp};
use crate::types::{ClientId, RequestKind, ServerId, TenantId, TicketId};

/// Which credit a lease was drawn from.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CreditKind {
    /// Step 0: the dedicated cancel pool, outside `total`.
    Cancel,
    /// Step 1: the tenant's own reserved credit.
    Reserved,
    /// Step 3: shared burst credit.
    Burst,
}

/// A granted unit of work-in-progress. Consumed by [`Allocator::release`], so
/// a lease cannot be returned twice.
#[derive(PartialEq, Eq, Debug)]
pub struct Lease {
    pub server: ServerId,
    pub tenant: TenantId,
    pub client: ClientId,
    pub credit: CreditKind,
    epoch: u64,
}

/// What the ladder decided.
#[derive(PartialEq, Eq, Debug)]
pub enum Admission {
    Granted(Lease),
    Queued {
        ticket: TicketId,
        /// The limit that blocked the immediate grant.
        blocked_by: DenialReason,
        /// The connection revocation this request triggered, if any.
        revoked: Option<Revocation>,
    },
    Denied(DenialReason),
    /// The client disconnected before admission completed. There is nobody left
    /// to deliver an error to, so this carries no code.
    Stale,
}

/// What step 4 did to make room.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Revocation {
    /// The victim had an idle server; it was closed immediately.
    ClosedIdle { tenant: TenantId, server: ServerId },
    /// The victim had none idle; its LRU active server closes at next release.
    MarkedActive { tenant: TenantId, server: ServerId },
}

impl Revocation {
    pub fn tenant(&self) -> &TenantId {
        match self {
            Self::ClosedIdle { tenant, .. } | Self::MarkedActive { tenant, .. } => tenant,
        }
    }

    pub fn server(&self) -> ServerId {
        match self {
            Self::ClosedIdle { server, .. } | Self::MarkedActive { server, .. } => *server,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ServerState {
    Idle,
    Active,
}

/// A backend connection, from the budget's point of view.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ServerConn {
    pub id: ServerId,
    pub tenant: TenantId,
    pub credit: CreditKind,
    pub state: ServerState,
    pub close_needed: bool,
    pub client: Option<ClientId>,
    seq: u64,
    epoch: u64,
}

/// A client waiting in a tenant's FIFO queue.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Ticket {
    pub id: TicketId,
    pub client: ClientId,
    pub enqueued_at: Timestamp,
    pub blocked_by: DenialReason,
    seq: u64,
}

/// A queued client that has just been given a lease.
#[derive(PartialEq, Eq, Debug)]
pub struct Grant {
    pub ticket: TicketId,
    pub client: ClientId,
    pub tenant: TenantId,
    pub lease: Lease,
}

/// A queued client whose admission deadline passed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Expired {
    pub ticket: TicketId,
    pub client: ClientId,
    pub tenant: TenantId,
    /// Always [`DenialReason::AdmissionTimeout`].
    pub reason: DenialReason,
    /// The limit that put the client in the queue in the first place.
    pub blocked_by: DenialReason,
}

/// What happened to a released server.
#[derive(PartialEq, Eq, Debug)]
pub enum Disposition {
    /// Returned to the tenant's idle set.
    Idle(ServerId),
    /// Closed: it was marked `close_needed`, or it was a cancel connection.
    Closed(ServerId),
    /// Quorum is lost, so the transaction has not committed and the backend is
    /// still pinned. The lease comes back for the caller to retry.
    Pinned(Lease),
    /// The lease no longer refers to a live checkout.
    Stale,
}

#[derive(PartialEq, Eq, Debug)]
pub struct ReleaseOutcome {
    pub disposition: Disposition,
    pub grants: Vec<Grant>,
}

/// Server lifecycle counters. `created == active + idle + closed` always.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Accounting {
    pub created: u64,
    pub active: u64,
    pub idle: u64,
    pub closed: u64,
}

impl Accounting {
    pub const fn conserved(&self) -> bool {
        self.created == self.active + self.idle + self.closed
    }
}

#[derive(Debug, Default)]
struct TenantRuntime {
    /// `(seq, id)` ordered oldest first: `first` is LRU, `last` is MRU.
    idle: BTreeSet<(u64, ServerId)>,
    active: BTreeSet<(u64, ServerId)>,
    queue: VecDeque<Ticket>,
    clients: u32,
    cancel_live: u32,
    /// Servers already flagged `close_needed`. Revocation subtracts this from
    /// the tenant's surplus, so repeated revocations can never mark more
    /// connections than the tenant holds above its own guarantee.
    marked: u32,
    storage_used: u64,
    migrating: bool,
}

enum Classification {
    Reserved,
    Burst,
    RevokeAndQueue,
    Queue,
}

/// The pure capacity state machine for one pool.
#[derive(Debug)]
pub struct Allocator<C: Clock = SystemClock> {
    clock: C,
    budget: CapacityBudget,
    admission: AdmissionSpec,
    tenants: BTreeMap<TenantId, TenantRuntime>,
    servers: BTreeMap<ServerId, ServerConn>,
    clients: BTreeMap<ClientId, TenantId>,
    seq: u64,
    next_server: u64,
    next_client: u64,
    next_ticket: u64,
    created: u64,
    closed: u64,
    clients_live: u32,
    queued_total: u32,
    quorum_lost: bool,
}

impl Allocator<SystemClock> {
    pub fn new(pool: PoolSpec, admission: AdmissionSpec) -> Result<Self, ConfigError> {
        Self::with_clock(pool, admission, SystemClock::new())
    }
}

impl<C: Clock> Allocator<C> {
    pub fn with_clock(
        pool: PoolSpec,
        admission: AdmissionSpec,
        clock: C,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            clock,
            budget: CapacityBudget::new(pool)?,
            admission,
            tenants: BTreeMap::new(),
            servers: BTreeMap::new(),
            clients: BTreeMap::new(),
            seq: 0,
            next_server: 0,
            next_client: 0,
            next_ticket: 0,
            created: 0,
            closed: 0,
            clients_live: 0,
            queued_total: 0,
            quorum_lost: false,
        })
    }

    pub const fn budget(&self) -> &CapacityBudget {
        &self.budget
    }

    pub const fn clock(&self) -> &C {
        &self.clock
    }

    pub const fn admission(&self) -> &AdmissionSpec {
        &self.admission
    }

    pub fn accounting(&self) -> Accounting {
        let mut accounting = Accounting {
            created: self.created,
            closed: self.closed,
            ..Accounting::default()
        };
        for conn in self.servers.values() {
            match conn.state {
                ServerState::Active => accounting.active += 1,
                ServerState::Idle => accounting.idle += 1,
            }
        }
        accounting
    }

    pub fn server(&self, id: ServerId) -> Option<&ServerConn> {
        self.servers.get(&id)
    }

    pub fn servers(&self) -> impl Iterator<Item = &ServerConn> {
        self.servers.values()
    }

    pub fn client_count(&self) -> u32 {
        self.clients_live
    }

    pub fn tenant_client_count(&self, tenant: &TenantId) -> u32 {
        self.tenants.get(tenant).map_or(0, |rt| rt.clients)
    }

    pub fn cancel_in_flight(&self, tenant: &TenantId) -> u32 {
        self.tenants.get(tenant).map_or(0, |rt| rt.cancel_live)
    }

    pub fn queued(&self, tenant: &TenantId) -> usize {
        self.tenants.get(tenant).map_or(0, |rt| rt.queue.len())
    }

    pub const fn queued_total(&self) -> u32 {
        self.queued_total
    }

    pub const fn quorum_lost(&self) -> bool {
        self.quorum_lost
    }

    /// The per-tenant cancel credit pool: `min(8, burstable_i)`.
    ///
    /// Evaluated at the moment of the request. Cancels are ephemeral, so a tier
    /// change that lowers `burstable` withholds new cancel credit rather than
    /// rejecting the change or tearing down cancels already in flight.
    pub fn cancel_credit(&self, tenant: &TenantId) -> u32 {
        self.budget
            .tenant(tenant)
            .map_or(0, |entry| CANCEL_CREDIT_CAP.min(entry.burstable()))
    }

    pub fn close_needed_active(&self) -> Vec<ServerId> {
        self.servers
            .values()
            .filter(|conn| conn.close_needed && conn.state == ServerState::Active)
            .map(|conn| conn.id)
            .collect()
    }

    // ---- configuration -------------------------------------------------

    pub fn add_tenant(&mut self, id: TenantId, spec: TenantSpec) -> Result<(), ConfigError> {
        self.budget.insert_tenant(id.clone(), spec)?;
        self.tenants.insert(id, TenantRuntime::default());
        Ok(())
    }

    pub fn set_tenant_spec(
        &mut self,
        id: &TenantId,
        spec: TenantSpec,
    ) -> Result<Vec<Grant>, ConfigError> {
        self.budget.update_tenant(id, spec)?;
        Ok(self.drain_queue())
    }

    pub fn remove_tenant(&mut self, id: &TenantId) -> Result<Vec<Grant>, ConfigError> {
        if !self.budget.contains(id) {
            return Err(ConfigError::UnknownTenant(id.clone()));
        }
        let owned: Vec<ServerId> = self
            .servers
            .values()
            .filter(|conn| &conn.tenant == id)
            .map(|conn| conn.id)
            .collect();
        for server in owned {
            self.close_server(server);
        }
        self.clients.retain(|_, tenant| tenant != id);
        if let Some(rt) = self.tenants.remove(id) {
            self.clients_live -= rt.clients;
            self.queued_total -= u32::try_from(rt.queue.len()).unwrap_or(u32::MAX);
        }
        self.budget.remove_tenant(id)?;
        Ok(self.drain_queue())
    }

    pub fn set_pool_spec(&mut self, spec: PoolSpec) -> Result<Vec<Grant>, ConfigError> {
        self.budget.set_pool_spec(spec)?;
        Ok(self.drain_queue())
    }

    /// Prepare a scale-down: close idle servers and mark the rest, until at
    /// most `target` backend connections remain live. Returns how many are
    /// still above `target` and therefore still draining.
    pub fn drain_to(&mut self, target: u32) -> u32 {
        while self.budget.live_total() > target {
            let Some(server) = self
                .servers
                .values()
                .filter(|conn| conn.state == ServerState::Idle)
                .min_by_key(|conn| conn.seq)
                .map(|conn| conn.id)
            else {
                break;
            };
            self.close_server(server);
        }

        let excess = self.budget.live_total().saturating_sub(target);
        let mut unmarked: Vec<(u64, ServerId)> = self
            .servers
            .values()
            .filter(|conn| {
                conn.credit != CreditKind::Cancel
                    && conn.state == ServerState::Active
                    && !conn.close_needed
            })
            .map(|conn| (conn.seq, conn.id))
            .collect();
        unmarked.sort_unstable();
        for (_, server) in unmarked.into_iter().take(excess as usize) {
            self.mark_close_needed(server);
        }
        excess
    }

    // ---- client connections: the second currency -----------------------

    /// Admit a client socket. Bounded by file descriptors and the per-tenant
    /// client limit, both of which are independent of `max_connections`.
    pub fn connect_client(&mut self, tenant: &TenantId) -> Result<ClientId, ConnectRejection> {
        let Some(entry) = self.budget.tenant(tenant) else {
            return Err(ConnectRejection::UnknownTenant(tenant.clone()));
        };
        let tenant_max = entry
            .spec()
            .effective_max_client_connections(self.budget.spec().mode);
        let pool_max = self.budget.spec().effective_max_client_connections();

        if self.clients_live >= pool_max {
            return Err(DenialReason::PoolClientCap {
                live: self.clients_live,
                max: pool_max,
            }
            .into());
        }
        let rt = self.tenants.get_mut(tenant).expect("tenant is bound");
        if rt.clients >= tenant_max {
            return Err(DenialReason::TenantClientCap {
                tenant: tenant.clone(),
                live: rt.clients,
                max: tenant_max,
            }
            .into());
        }

        rt.clients += 1;
        self.clients_live += 1;
        let id = ClientId(self.next_client);
        self.next_client += 1;
        self.clients.insert(id, tenant.clone());
        Ok(id)
    }

    pub fn disconnect_client(&mut self, client: ClientId) -> Vec<Grant> {
        let Some(tenant) = self.clients.remove(&client) else {
            return Vec::new();
        };
        if let Some(rt) = self.tenants.get_mut(&tenant) {
            rt.clients -= 1;
            let before = rt.queue.len();
            rt.queue.retain(|ticket| ticket.client != client);
            self.queued_total -= u32::try_from(before - rt.queue.len()).unwrap_or(0);
        }
        self.clients_live -= 1;

        let held: Vec<ServerId> = self
            .servers
            .values()
            .filter(|conn| conn.client == Some(client) && conn.state == ServerState::Active)
            .map(|conn| conn.id)
            .collect();
        for server in held {
            self.retire_active(server);
        }
        self.drain_queue()
    }

    // ---- the admission ladder ------------------------------------------

    /// The unified admission ladder. One function, one place, one log line
    /// naming the blocking limit.
    ///
    /// ```text
    /// 0. CANCEL BURST: a queued CancelRequest draws from a dedicated credit
    ///                  pool, min(8, burstable_i), bypassing 1..5 entirely.
    /// 1. GUARANTEED:   live_i < guaranteed_i   -> reserved credit, never queues
    /// 2. BURST CEIL:   live_i >= burstable_i   -> Denied(TenantCap)
    /// 3. BURST AVAIL:  free() > 0              -> burst credit
    /// 4. REVOCATION:   free() == 0 and guaranteed -> revoke the largest surplus
    /// 5. else queue, ordered by the cross-tenant scheduler
    /// ```
    pub fn try_lease(&mut self, client: ClientId, kind: RequestKind) -> Admission {
        let Some(tenant) = self.clients.get(&client).cloned() else {
            return Admission::Stale;
        };

        if kind == RequestKind::Cancel {
            return self.cancel_burst(&tenant, client);
        }

        if self.tenants[&tenant].migrating {
            return deny(DenialReason::MigrationCutover {
                tenant: tenant.clone(),
            });
        }

        // An idle server the tenant already holds is already counted in
        // `live_i`; handing it back out moves no capacity, so the ladder does
        // not run for it.
        if let Some(lease) = self.reuse_idle(&tenant, client) {
            return Admission::Granted(lease);
        }

        match self.classify(&tenant) {
            Err(reason) => deny(reason),
            Ok(Classification::Reserved) => {
                Admission::Granted(self.open_server(&tenant, CreditKind::Reserved, client))
            }
            Ok(Classification::Burst) => {
                Admission::Granted(self.open_server(&tenant, CreditKind::Burst, client))
            }
            Ok(Classification::RevokeAndQueue) => match self.revoke() {
                // Closing an idle server freed a physical slot outright, so the
                // guarantee is honoured now. Only the marked-active case has to
                // wait for the victim's next release.
                Some(Revocation::ClosedIdle {
                    tenant: victim,
                    server,
                }) => {
                    tracing::debug!(%victim, %server, "revoked an idle server to honour a guarantee");
                    Admission::Granted(self.open_server(&tenant, CreditKind::Reserved, client))
                }
                revoked => {
                    let mut admission = self.enqueue(&tenant, client);
                    if let Admission::Queued { revoked: slot, .. } = &mut admission {
                        *slot = revoked;
                    }
                    admission
                }
            },
            Ok(Classification::Queue) => self.enqueue(&tenant, client),
        }
    }

    /// Step 0. The cancel credit pool sits outside `total`, drawn from the
    /// `superuser_reserved_connections` that `total` already excludes: a cancel
    /// that queues behind the query it is cancelling never completes.
    fn cancel_burst(&mut self, tenant: &TenantId, client: ClientId) -> Admission {
        let limit = self.cancel_credit(tenant);
        let in_flight = self.tenants[tenant].cancel_live;
        if in_flight >= limit {
            return deny(DenialReason::CancelCredit {
                tenant: tenant.clone(),
                in_flight,
                limit,
            });
        }
        Admission::Granted(self.open_server(tenant, CreditKind::Cancel, client))
    }

    fn classify(&self, tenant: &TenantId) -> Result<Classification, DenialReason> {
        let entry = self.budget.tenant(tenant).expect("tenant is bound");

        // Step 1. Reserved credit is the tenant's own, but it still needs a
        // physical slot. The two diverge only after a config change raised a
        // guarantee while other tenants were already bursting — which is
        // exactly the case step 4 exists to repair.
        if entry.live() < entry.guaranteed() {
            return Ok(if self.budget.live_total() < self.budget.total() {
                Classification::Reserved
            } else {
                Classification::RevokeAndQueue
            });
        }

        // Step 2.
        if entry.live() >= entry.burstable() {
            return Err(DenialReason::TenantCap {
                tenant: tenant.clone(),
                live: entry.live(),
                burstable: entry.burstable(),
            });
        }

        // Step 3.
        if self.budget.free() > 0 {
            return Ok(Classification::Burst);
        }

        // Step 5.
        Ok(Classification::Queue)
    }

    /// Step 4. Take capacity from the tenant with the largest
    /// `live_j − guaranteed_j` surplus, so revocation can never push any tenant
    /// below its own guarantee.
    fn revoke(&mut self) -> Option<Revocation> {
        for tenant in self.revocation_candidates() {
            let rt = &self.tenants[&tenant];
            if let Some(&(_, server)) = rt
                .idle
                .iter()
                .find(|(_, id)| self.servers[id].credit != CreditKind::Cancel)
            {
                self.close_server(server);
                return Some(Revocation::ClosedIdle { tenant, server });
            }
            let marked = rt.active.iter().find(|(_, id)| {
                let conn = &self.servers[id];
                conn.credit != CreditKind::Cancel && !conn.close_needed
            });
            if let Some(&(_, server)) = marked {
                self.mark_close_needed(server);
                return Some(Revocation::MarkedActive { tenant, server });
            }
        }
        None
    }

    /// Tenants with surplus left to take, worst offender first. Ties break
    /// towards the lowest priority, then the lowest weight.
    ///
    /// Surplus is counted net of connections already marked for close: a tenant
    /// that has been revoked from twice has already given up two, and taking a
    /// third would push it below its own guarantee.
    fn revocation_candidates(&self) -> Vec<TenantId> {
        let mut candidates: Vec<(&TenantId, &TenantEntry, u32)> = self
            .budget
            .iter()
            .filter_map(|(id, entry)| {
                let marked = self.tenants.get(id).map_or(0, |rt| rt.marked);
                let takeable = entry.surplus().checked_sub(marked).filter(|net| *net > 0)?;
                Some((id, entry, takeable))
            })
            .collect();
        candidates.sort_by(
            |(left_id, left, left_takeable), (right_id, right, right_takeable)| {
                right_takeable
                    .cmp(left_takeable)
                    .then_with(|| left.priority().cmp(&right.priority()))
                    .then_with(|| left.weight().cmp(&right.weight()))
                    .then_with(|| left_id.cmp(right_id))
            },
        );
        candidates
            .into_iter()
            .map(|(id, _, _)| id.clone())
            .collect()
    }

    fn mark_close_needed(&mut self, server: ServerId) -> bool {
        let Some(conn) = self.servers.get_mut(&server) else {
            return false;
        };
        if conn.close_needed {
            return false;
        }
        conn.close_needed = true;
        let tenant = conn.tenant.clone();
        self.tenants
            .get_mut(&tenant)
            .expect("tenant is bound")
            .marked += 1;
        true
    }

    fn enqueue(&mut self, tenant: &TenantId, client: ClientId) -> Admission {
        let limit = self.admission.queue_depth_per_tenant;
        let queued = u32::try_from(self.tenants[tenant].queue.len()).unwrap_or(u32::MAX);
        if queued >= limit {
            return deny(DenialReason::QueueFull {
                tenant: tenant.clone(),
                queued,
                limit,
            });
        }

        let blocked_by = DenialReason::PoolCapacity {
            live: self.budget.live_total(),
            total: self.budget.total(),
        };
        let seq = self.next_seq();
        let id = TicketId(self.next_ticket);
        self.next_ticket += 1;
        let enqueued_at = self.clock.now();

        self.tenants
            .get_mut(tenant)
            .expect("tenant is bound")
            .queue
            .push_back(Ticket {
                id,
                client,
                enqueued_at,
                blocked_by: blocked_by.clone(),
                seq,
            });
        self.queued_total += 1;

        tracing::debug!(
            tenant = %tenant,
            ticket = %id,
            blocked_by = %blocked_by,
            code = %blocked_by.code(),
            "admission queued"
        );
        Admission::Queued {
            ticket: id,
            blocked_by,
            revoked: None,
        }
    }

    // ---- release and the scheduler -------------------------------------

    pub fn release(&mut self, lease: Lease) -> ReleaseOutcome {
        let Some(conn) = self.servers.get(&lease.server) else {
            return ReleaseOutcome {
                disposition: Disposition::Stale,
                grants: Vec::new(),
            };
        };
        if conn.epoch != lease.epoch || conn.state != ServerState::Active {
            return ReleaseOutcome {
                disposition: Disposition::Stale,
                grants: Vec::new(),
            };
        }
        // A blocked COMMIT has not returned, so the backend is still pinned.
        // This is what bounds the blast radius of quorum loss to the pool that
        // lost quorum: the pins consume that pool's budget and no other.
        if self.quorum_lost && conn.credit != CreditKind::Cancel {
            return ReleaseOutcome {
                disposition: Disposition::Pinned(lease),
                grants: Vec::new(),
            };
        }

        let disposition = self.retire_active(lease.server);
        let grants = self.drain_queue();
        ReleaseOutcome {
            disposition,
            grants,
        }
    }

    pub fn backend_died(&mut self, server: ServerId) -> Vec<Grant> {
        self.close_server(server);
        self.drain_queue()
    }

    pub fn expire_queued(&mut self) -> Vec<Expired> {
        let now = self.clock.now();
        let max_wait = self.admission.max_wait;
        let mut expired = Vec::new();
        for (tenant, rt) in &mut self.tenants {
            while let Some(front) = rt.queue.front() {
                if now.saturating_since(front.enqueued_at) < max_wait {
                    break;
                }
                let ticket = rt.queue.pop_front().expect("front exists");
                expired.push(Expired {
                    ticket: ticket.id,
                    client: ticket.client,
                    tenant: tenant.clone(),
                    reason: DenialReason::AdmissionTimeout {
                        tenant: tenant.clone(),
                        waited: now.saturating_since(ticket.enqueued_at),
                    },
                    blocked_by: ticket.blocked_by,
                });
            }
        }
        self.queued_total -= u32::try_from(expired.len()).unwrap_or(0);
        expired
    }

    pub fn cancel_ticket(&mut self, tenant: &TenantId, ticket: TicketId) -> bool {
        let Some(rt) = self.tenants.get_mut(tenant) else {
            return false;
        };
        let before = rt.queue.len();
        rt.queue.retain(|queued| queued.id != ticket);
        let removed = before - rt.queue.len();
        self.queued_total -= u32::try_from(removed).unwrap_or(0);
        removed > 0
    }

    /// Quorum loss pins every in-flight backend: a blocked COMMIT has not
    /// returned, so its connection cannot go back to the pool.
    pub fn set_quorum_lost(&mut self, lost: bool) {
        self.quorum_lost = lost;
    }

    pub fn begin_migration(&mut self, tenant: &TenantId) -> Result<(), ConfigError> {
        let Some(rt) = self.tenants.get_mut(tenant) else {
            return Err(ConfigError::UnknownTenant(tenant.clone()));
        };
        rt.migrating = true;
        let owned: Vec<ServerId> = self
            .servers
            .values()
            .filter(|conn| &conn.tenant == tenant)
            .map(|conn| conn.id)
            .collect();
        for server in owned {
            self.mark_close_needed(server);
        }
        Ok(())
    }

    pub fn finish_migration(&mut self, tenant: &TenantId) -> Result<Vec<Grant>, ConfigError> {
        let Some(rt) = self.tenants.get_mut(tenant) else {
            return Err(ConfigError::UnknownTenant(tenant.clone()));
        };
        rt.migrating = false;
        Ok(self.drain_queue())
    }

    // ---- storage: a third, per-tenant currency -------------------------

    pub fn set_storage_used(&mut self, tenant: &TenantId, bytes: u64) -> Result<(), ConfigError> {
        let Some(rt) = self.tenants.get_mut(tenant) else {
            return Err(ConfigError::UnknownTenant(tenant.clone()));
        };
        rt.storage_used = bytes;
        Ok(())
    }

    /// The write path's gate. Exhausting it fails writes only; `SELECT` and
    /// `DELETE` are deliberately unaffected, so a tenant can dig itself out.
    pub fn check_storage(&self, tenant: &TenantId, additional: u64) -> Result<(), DenialReason> {
        let Some(entry) = self.budget.tenant(tenant) else {
            return Ok(());
        };
        let used = self.tenants[tenant].storage_used.saturating_add(additional);
        let quota = entry.spec().storage_bytes;
        if used > quota {
            return Err(DenialReason::StorageQuota {
                tenant: tenant.clone(),
                used,
                quota,
            });
        }
        Ok(())
    }

    // ---- two-level fairness --------------------------------------------

    /// Tenants with a queued client, in the order the scheduler will serve
    /// them. Within a tenant the queue is strict FIFO, which is what keeps
    /// `maxwait` meaningful.
    pub fn scheduler_order(&self) -> Vec<TenantId> {
        let mut waiting: Vec<(&TenantId, &TenantEntry, u64)> = self
            .tenants
            .iter()
            .filter_map(|(id, rt)| {
                let head = rt.queue.front()?;
                Some((id, self.budget.tenant(id)?, head.seq))
            })
            .collect();

        // The cross-tenant scheduler runs only when free() == 0. While there is
        // free capacity there is nothing to arbitrate, so arrival order wins.
        let arbitrate = self.budget.free() == 0
            && self.admission.strategy == AdmissionStrategy::WeightedDeficit;

        if arbitrate {
            waiting.sort_by(|left, right| weighted_deficit_cmp(*left, *right));
        } else {
            waiting.sort_by_key(|&(_, _, seq)| seq);
        }
        waiting.into_iter().map(|(id, _, _)| id.clone()).collect()
    }

    fn drain_queue(&mut self) -> Vec<Grant> {
        let mut grants = Vec::new();
        'progress: loop {
            if self.queued_total == 0 {
                break;
            }
            for tenant in self.scheduler_order() {
                if self.tenants[&tenant].migrating {
                    continue;
                }
                let client = self.tenants[&tenant]
                    .queue
                    .front()
                    .expect("scheduler_order only yields tenants with a queued client")
                    .client;
                let lease = if let Some(lease) = self.reuse_idle(&tenant, client) {
                    lease
                } else {
                    match self.classify(&tenant) {
                        Ok(Classification::Reserved) => {
                            self.open_server(&tenant, CreditKind::Reserved, client)
                        }
                        Ok(Classification::Burst) => {
                            self.open_server(&tenant, CreditKind::Burst, client)
                        }
                        _ => continue,
                    }
                };
                let ticket = self
                    .tenants
                    .get_mut(&tenant)
                    .expect("tenant is bound")
                    .queue
                    .pop_front()
                    .expect("the head was peeked above");
                self.queued_total -= 1;
                grants.push(Grant {
                    ticket: ticket.id,
                    client: ticket.client,
                    tenant,
                    lease,
                });
                continue 'progress;
            }
            break;
        }
        grants
    }

    // ---- server bookkeeping --------------------------------------------

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn reuse_idle(&mut self, tenant: &TenantId, client: ClientId) -> Option<Lease> {
        let seq = self.next_seq();
        let rt = self.tenants.get_mut(tenant)?;
        // LIFO: the most recently used idle server. The LRU end is what
        // revocation and idle timeouts consume.
        let key = *rt.idle.iter().next_back()?;
        rt.idle.remove(&key);
        rt.active.insert((seq, key.1));

        let conn = self.servers.get_mut(&key.1).expect("server is indexed");
        conn.state = ServerState::Active;
        conn.client = Some(client);
        conn.seq = seq;
        conn.epoch = seq;
        Some(Lease {
            server: conn.id,
            tenant: tenant.clone(),
            client,
            credit: conn.credit,
            epoch: seq,
        })
    }

    fn open_server(&mut self, tenant: &TenantId, credit: CreditKind, client: ClientId) -> Lease {
        let seq = self.next_seq();
        let id = ServerId(self.next_server);
        self.next_server += 1;
        self.created += 1;

        self.servers.insert(
            id,
            ServerConn {
                id,
                tenant: tenant.clone(),
                credit,
                state: ServerState::Active,
                close_needed: false,
                client: Some(client),
                seq,
                epoch: seq,
            },
        );
        let rt = self.tenants.get_mut(tenant).expect("tenant is bound");
        rt.active.insert((seq, id));
        if credit == CreditKind::Cancel {
            rt.cancel_live += 1;
        } else {
            self.budget.inc_live(tenant);
        }

        Lease {
            server: id,
            tenant: tenant.clone(),
            client,
            credit,
            epoch: seq,
        }
    }

    fn retire_active(&mut self, server: ServerId) -> Disposition {
        let Some(conn) = self.servers.get(&server) else {
            return Disposition::Stale;
        };
        if conn.close_needed || conn.credit == CreditKind::Cancel {
            self.close_server(server);
            return Disposition::Closed(server);
        }

        let seq = self.next_seq();
        let conn = self.servers.get_mut(&server).expect("server is indexed");
        let old = conn.seq;
        let tenant = conn.tenant.clone();
        conn.state = ServerState::Idle;
        conn.client = None;
        conn.seq = seq;
        conn.epoch = seq;

        let rt = self.tenants.get_mut(&tenant).expect("tenant is bound");
        rt.active.remove(&(old, server));
        rt.idle.insert((seq, server));
        Disposition::Idle(server)
    }

    fn close_server(&mut self, server: ServerId) -> Option<ServerConn> {
        let conn = self.servers.remove(&server)?;
        if let Some(rt) = self.tenants.get_mut(&conn.tenant) {
            match conn.state {
                ServerState::Idle => rt.idle.remove(&(conn.seq, server)),
                ServerState::Active => rt.active.remove(&(conn.seq, server)),
            };
            if conn.credit == CreditKind::Cancel {
                rt.cancel_live -= 1;
            }
            if conn.close_needed {
                rt.marked -= 1;
            }
        }
        if conn.credit != CreditKind::Cancel {
            self.budget.dec_live(&conn.tenant);
        }
        self.closed += 1;
        Some(conn)
    }
}

fn deny(reason: DenialReason) -> Admission {
    tracing::debug!(
        reason = %reason,
        code = %reason.code(),
        sqlstate = reason.sqlstate(),
        "admission denied"
    );
    Admission::Denied(reason)
}

fn weighted_deficit_cmp(
    left: (&TenantId, &TenantEntry, u64),
    right: (&TenantId, &TenantEntry, u64),
) -> Ordering {
    let (left_id, left_entry, left_seq) = left;
    let (right_id, right_entry, right_seq) = right;
    right_entry
        .deficit()
        .cmp(&left_entry.deficit())
        .then_with(|| weighted_fraction_cmp(left_entry, right_entry))
        .then_with(|| right_entry.priority().cmp(&left_entry.priority()))
        .then_with(|| left_seq.cmp(&right_seq))
        .then_with(|| left_id.cmp(right_id))
}

/// `(live − guaranteed) / (burstable − guaranteed)` ascending, weighted by the
/// workload class weight: a heavier class is charged less for the same burst
/// fraction and therefore takes a larger share of the contended surplus.
fn weighted_fraction_cmp(left: &TenantEntry, right: &TenantEntry) -> Ordering {
    let numerator = |entry: &TenantEntry| u128::from(entry.surplus());
    let denominator = |entry: &TenantEntry| {
        u128::from(entry.burstable() - entry.guaranteed()) * u128::from(entry.weight())
    };
    let (left_n, left_d) = (numerator(left), denominator(left));
    let (right_n, right_d) = (numerator(right), denominator(right));
    match (left_d == 0, right_d == 0) {
        // No burst range at all: nothing to arbitrate, so it sorts last.
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => (left_n * right_d).cmp(&(right_n * left_d)),
    }
}
