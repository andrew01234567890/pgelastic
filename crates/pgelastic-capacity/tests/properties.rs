//! The capacity model as a pure state machine, under `proptest`.
//!
//! Random sequences of {tenant arrives, departs, checkout, release, backend
//! dies, config change, quorum loss, migration} are replayed against the
//! allocator and every safety invariant is re-checked after every single
//! operation. Liveness is a bounded-step obligation and gets its own harness.

use std::time::Duration;

use pgelastic_capacity::{
    Admission, AdmissionSpec, Allocator, CANCEL_CREDIT_CAP, ClientId, ConfigError, Disposition,
    Lease, ManualClock, PoolSpec, RequestKind, ServerId, TenantId, TenantSpec, check_reservations,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

const NAMES: [&str; 4] = ["alpha", "bravo", "charlie", "delta"];

fn pool(total: u32, headroom: u8) -> PoolSpec {
    PoolSpec {
        backend_connections: total,
        headroom_percent: headroom,
        max_oversubscription: None,
        ..PoolSpec::default()
    }
}

fn spec(guaranteed: u32, burstable: u32) -> TenantSpec {
    TenantSpec {
        guaranteed,
        burstable,
        ..TenantSpec::default()
    }
}

#[derive(Debug, Clone)]
enum Op {
    TenantArrives {
        idx: usize,
        guaranteed: u32,
        burstable: u32,
        weight: u32,
        priority: u32,
    },
    TenantDeparts {
        idx: usize,
    },
    ClientConnects {
        idx: usize,
    },
    Checkout {
        pick: usize,
        cancel: bool,
    },
    Release {
        pick: usize,
    },
    ClientDisconnects {
        pick: usize,
    },
    BackendDies {
        pick: usize,
    },
    ConfigChange {
        idx: usize,
        guaranteed: u32,
        burstable: u32,
    },
    QuorumLoss(bool),
    Migration {
        idx: usize,
        begin: bool,
    },
    Expire,
    AdvanceClock(u64),
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0usize..4, 0u32..6, 1u32..12, 1u32..=400u32, 0u32..=5_000).prop_map(
            |(idx, guaranteed, burstable, weight, priority)| Op::TenantArrives {
                idx,
                guaranteed,
                burstable: burstable.max(guaranteed),
                weight,
                priority,
            }
        ),
        1 => (0usize..4).prop_map(|idx| Op::TenantDeparts { idx }),
        5 => (0usize..4).prop_map(|idx| Op::ClientConnects { idx }),
        10 => (any::<u8>(), proptest::bool::weighted(0.15))
            .prop_map(|(pick, cancel)| Op::Checkout { pick: pick as usize, cancel }),
        8 => any::<u8>().prop_map(|pick| Op::Release { pick: pick as usize }),
        2 => any::<u8>().prop_map(|pick| Op::ClientDisconnects { pick: pick as usize }),
        2 => any::<u8>().prop_map(|pick| Op::BackendDies { pick: pick as usize }),
        3 => (0usize..4, 0u32..6, 1u32..12).prop_map(|(idx, guaranteed, burstable)| {
            Op::ConfigChange { idx, guaranteed, burstable: burstable.max(guaranteed) }
        }),
        1 => any::<bool>().prop_map(Op::QuorumLoss),
        1 => (0usize..4, any::<bool>()).prop_map(|(idx, begin)| Op::Migration { idx, begin }),
        1 => Just(Op::Expire),
        1 => (0u64..60_000).prop_map(Op::AdvanceClock),
    ]
}

struct Model {
    allocator: Allocator<ManualClock>,
    leases: Vec<Lease>,
    clients: Vec<ClientId>,
}

impl Model {
    fn new(total: u32, headroom: u8) -> Self {
        Self {
            allocator: Allocator::with_clock(
                pool(total, headroom),
                AdmissionSpec::default(),
                ManualClock::new(),
            )
            .unwrap(),
            leases: Vec::new(),
            clients: Vec::new(),
        }
    }

    fn absorb(&mut self, grants: Vec<pgelastic_capacity::Grant>) {
        self.leases
            .extend(grants.into_iter().map(|grant| grant.lease));
    }

    fn apply(&mut self, op: &Op) {
        match *op {
            Op::TenantArrives { .. }
            | Op::TenantDeparts { .. }
            | Op::ConfigChange { .. }
            | Op::QuorumLoss(_)
            | Op::Migration { .. } => self.apply_control_plane(op),
            _ => self.apply_traffic(op),
        }
    }

    fn apply_control_plane(&mut self, op: &Op) {
        match *op {
            Op::TenantArrives {
                idx,
                guaranteed,
                burstable,
                weight,
                priority,
            } => {
                let spec = TenantSpec {
                    guaranteed,
                    burstable,
                    weight,
                    priority,
                    ..TenantSpec::default()
                };
                let _ = self.allocator.add_tenant(NAMES[idx].into(), spec);
            }
            Op::TenantDeparts { idx } => {
                if let Ok(grants) = self.allocator.remove_tenant(&NAMES[idx].into()) {
                    self.absorb(grants);
                }
            }
            Op::ConfigChange {
                idx,
                guaranteed,
                burstable,
            } => {
                let id = TenantId::from(NAMES[idx]);
                let Some(entry) = self.allocator.budget().tenant(&id) else {
                    return;
                };
                let spec = TenantSpec {
                    guaranteed,
                    burstable,
                    ..entry.spec()
                };
                if let Ok(grants) = self.allocator.set_tenant_spec(&id, spec) {
                    self.absorb(grants);
                }
            }
            Op::QuorumLoss(lost) => self.allocator.set_quorum_lost(lost),
            Op::Migration { idx, begin } => {
                let id = TenantId::from(NAMES[idx]);
                if begin {
                    let _ = self.allocator.begin_migration(&id);
                } else if let Ok(grants) = self.allocator.finish_migration(&id) {
                    self.absorb(grants);
                }
            }
            _ => unreachable!("dispatched by apply"),
        }
    }

    fn apply_traffic(&mut self, op: &Op) {
        match *op {
            Op::ClientConnects { idx } => {
                if let Ok(client) = self.allocator.connect_client(&NAMES[idx].into()) {
                    self.clients.push(client);
                }
            }
            Op::Checkout { pick, cancel } => {
                if self.clients.is_empty() {
                    return;
                }
                let client = self.clients[pick % self.clients.len()];
                let kind = if cancel {
                    RequestKind::Cancel
                } else {
                    RequestKind::Normal
                };
                if let Admission::Granted(lease) = self.allocator.try_lease(client, kind) {
                    self.leases.push(lease);
                }
            }
            Op::Release { pick } => {
                if self.leases.is_empty() {
                    return;
                }
                let lease = self.leases.swap_remove(pick % self.leases.len());
                let outcome = self.allocator.release(lease);
                if let Disposition::Pinned(lease) = outcome.disposition {
                    self.leases.push(lease);
                }
                self.absorb(outcome.grants);
            }
            Op::ClientDisconnects { pick } => {
                if self.clients.is_empty() {
                    return;
                }
                let client = self.clients.swap_remove(pick % self.clients.len());
                let grants = self.allocator.disconnect_client(client);
                self.absorb(grants);
            }
            Op::BackendDies { pick } => {
                let servers: Vec<ServerId> = self.allocator.servers().map(|conn| conn.id).collect();
                if servers.is_empty() {
                    return;
                }
                let grants = self.allocator.backend_died(servers[pick % servers.len()]);
                self.absorb(grants);
            }
            Op::Expire => {
                self.allocator.expire_queued();
            }
            Op::AdvanceClock(millis) => {
                self.allocator
                    .clock()
                    .advance(Duration::from_millis(millis));
            }
            _ => unreachable!("dispatched by apply"),
        }
    }

    fn check(&self) -> Result<(), TestCaseError> {
        let budget = self.allocator.budget();

        prop_assert!(
            budget.live_total() <= budget.total(),
            "active backends {} exceed total {}",
            budget.live_total(),
            budget.total()
        );

        let mut sum_max: u64 = 0;
        let mut reserved: u64 = 0;
        let mut live_total: u64 = 0;
        for (id, entry) in budget.iter() {
            prop_assert!(
                entry.live() <= entry.burstable(),
                "tenant {id} holds {} above its burst ceiling {}",
                entry.live(),
                entry.burstable()
            );
            // The cancel credit is min(8, burstable) at the moment of the
            // request; a later tier change can lower `burstable` under cancels
            // that are already in flight, so only the constant bound holds
            // unconditionally.
            prop_assert!(
                self.allocator.cancel_in_flight(id) <= CANCEL_CREDIT_CAP,
                "tenant {id} exceeded its cancel credit"
            );
            prop_assert!(
                self.allocator.tenant_client_count(id)
                    <= entry
                        .spec()
                        .effective_max_client_connections(budget.spec().mode),
                "tenant {id} exceeded its client connection limit"
            );
            sum_max += u64::from(entry.guaranteed().max(entry.live()));
            reserved += u64::from(entry.guaranteed());
            live_total += u64::from(entry.live());
        }

        prop_assert_eq!(live_total, u64::from(budget.live_total()));
        prop_assert!(
            reserved <= u64::from(budget.allocatable()),
            "guarantees {reserved} over-commit allocatable {}",
            budget.allocatable()
        );
        prop_assert_eq!(
            u64::from(budget.free()),
            u64::from(budget.total()).saturating_sub(sum_max),
            "free() must equal total - sum of max(guaranteed, live)"
        );

        let accounting = self.allocator.accounting();
        prop_assert!(
            accounting.conserved(),
            "created {} != active {} + idle {} + closed {}",
            accounting.created,
            accounting.active,
            accounting.idle,
            accounting.closed
        );
        prop_assert!(
            self.allocator.client_count() <= budget.spec().effective_max_client_connections()
        );
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: ProptestConfig::default().cases.max(512),
        ..ProptestConfig::default()
    })]

    #[test]
    fn safety_holds_over_random_operation_sequences(
        total in 4u32..24,
        headroom in 0u8..=25,
        ops in prop::collection::vec(any_op(), 1..150),
    ) {
        let mut model = Model::new(total, headroom);
        model.check()?;
        for op in &ops {
            model.apply(op);
            model.check()?;
        }
    }

    #[test]
    fn an_over_committed_configuration_is_rejected_at_admission(
        total in 1u32..=200,
        headroom in 0u8..=50,
        guarantees in prop::collection::vec(0u32..=100, 1..6),
    ) {
        let pool_spec = pool(total, headroom);
        let allocatable = pool_spec.allocatable();
        let mut allocator =
            Allocator::with_clock(pool_spec, AdmissionSpec::default(), ManualClock::new()).unwrap();

        let mut accepted: u64 = 0;
        for (index, guaranteed) in guarantees.into_iter().enumerate() {
            let id = TenantId::from(format!("t{index}"));
            let result = allocator.add_tenant(id, spec(guaranteed, guaranteed.max(1) * 4));
            let projected = accepted + u64::from(guaranteed);

            if projected <= u64::from(allocatable) {
                prop_assert!(result.is_ok(), "{result:?} for a config within headroom");
                accepted = projected;
            } else {
                prop_assert!(
                    matches!(result, Err(ConfigError::OverCommitted { .. })),
                    "expected an over-commitment rejection"
                );
            }
        }

        let budget = allocator.budget();
        let ledger = check_reservations(
            budget.spec(),
            budget.iter().map(|(id, entry)| (id, entry.spec())),
        ).unwrap();
        prop_assert_eq!(ledger, budget.reservations());
        prop_assert_eq!(u64::from(ledger.reserved), accepted);
    }

    /// Liveness, as a bounded-step obligation: a tenant whose demand reaches its
    /// guarantee is granted that guarantee within `guarantee + 2` rounds of
    /// (one request, one release), no matter how thoroughly the pool was
    /// occupied by bursting tenants beforehand.
    #[test]
    fn a_guarantee_is_honoured_within_a_bounded_number_of_steps(
        total in 4u32..=24,
        guarantee_fraction in 1u32..=8,
        hog_count in 1usize..=3,
    ) {
        let guarantee = (total / 2).min(guarantee_fraction).max(1);
        let mut allocator =
            Allocator::with_clock(pool(total, 0), AdmissionSpec::default(), ManualClock::new())
                .unwrap();

        let vip = TenantId::from("vip");
        allocator.add_tenant(vip.clone(), spec(0, total)).unwrap();
        let hogs: Vec<TenantId> = (0..hog_count)
            .map(|index| {
                let id = TenantId::from(format!("hog{index}"));
                allocator.add_tenant(id.clone(), spec(0, total)).unwrap();
                id
            })
            .collect();

        // Fill every last slot with burst traffic.
        let mut hog_leases: Vec<Lease> = Vec::new();
        'fill: loop {
            for hog in &hogs {
                if allocator.budget().live_total() == total {
                    break 'fill;
                }
                let client = allocator.connect_client(hog).unwrap();
                if let Admission::Granted(lease) = allocator.try_lease(client, RequestKind::Normal)
                {
                    hog_leases.push(lease);
                }
            }
        }
        prop_assert_eq!(allocator.budget().live_total(), total);
        prop_assert_eq!(allocator.budget().free(), 0);

        // The tier change that turns the vip into a guaranteed tenant.
        allocator
            .set_tenant_spec(&vip, spec(guarantee, total))
            .unwrap();

        let bound = guarantee + 2;
        let mut rounds = 0;
        let mut vip_leases: Vec<Lease> = Vec::new();
        while allocator.budget().tenant(&vip).unwrap().live() < guarantee && rounds < bound {
            rounds += 1;

            let client = allocator.connect_client(&vip).unwrap();
            if let Admission::Granted(lease) = allocator.try_lease(client, RequestKind::Normal) {
                vip_leases.push(lease);
            }

            let marked = allocator.close_needed_active();
            if let Some(position) = hog_leases
                .iter()
                .position(|lease| marked.contains(&lease.server))
            {
                let outcome = allocator.release(hog_leases.swap_remove(position));
                for grant in outcome.grants {
                    if grant.tenant == vip {
                        vip_leases.push(grant.lease);
                    } else {
                        hog_leases.push(grant.lease);
                    }
                }
            }
        }

        prop_assert_eq!(
            allocator.budget().tenant(&vip).unwrap().live(),
            guarantee,
            "the guarantee was not honoured within {} rounds",
            bound
        );
        prop_assert!(allocator.budget().live_total() <= total);
    }

    /// Monotonicity: raising one tenant's guarantee never pushes another below
    /// its own, or the configuration is rejected as over-committed.
    #[test]
    fn raising_one_guarantee_never_breaks_another(
        total in 6u32..=24,
        shape in prop::collection::vec((0u32..4, 1u32..10), 3..=3),
        demand in prop::collection::vec(1usize..8, 3..=3),
        raise in 1u32..=6,
    ) {
        let mut allocator =
            Allocator::with_clock(pool(total, 0), AdmissionSpec::default(), ManualClock::new())
                .unwrap();

        let mut tenants: Vec<TenantId> = Vec::new();
        for (index, (guaranteed, burstable)) in shape.iter().copied().enumerate() {
            let id = TenantId::from(format!("t{index}"));
            if allocator
                .add_tenant(id.clone(), spec(guaranteed, burstable.max(guaranteed)))
                .is_ok()
            {
                tenants.push(id);
            }
        }
        prop_assume!(!tenants.is_empty());

        let mut leases: Vec<Lease> = Vec::new();
        for (id, wanted) in tenants.iter().zip(demand) {
            for _ in 0..wanted {
                let client = allocator.connect_client(id).unwrap();
                if let Admission::Granted(lease) = allocator.try_lease(client, RequestKind::Normal)
                {
                    leases.push(lease);
                }
            }
        }

        let baseline: Vec<(TenantId, u32, u32)> = allocator
            .budget()
            .iter()
            .map(|(id, entry)| (id.clone(), entry.guaranteed(), entry.live()))
            .collect();

        let target = tenants[0].clone();
        let raised = {
            let entry = allocator.budget().tenant(&target).unwrap();
            TenantSpec {
                guaranteed: entry.guaranteed() + raise,
                burstable: entry.burstable().max(entry.guaranteed() + raise),
                ..entry.spec()
            }
        };

        let Ok(grants) = allocator.set_tenant_spec(&target, raised) else {
            // Rejected as over-committed: nothing moved, so nothing broke.
            for (id, guaranteed, live) in baseline {
                let entry = allocator.budget().tenant(&id).unwrap();
                prop_assert_eq!(entry.guaranteed(), guaranteed);
                prop_assert_eq!(entry.live(), live);
            }
            return Ok(());
        };
        leases.extend(grants.into_iter().map(|grant| grant.lease));

        let assert_nobody_dropped =
            |allocator: &Allocator<ManualClock>| -> Result<(), TestCaseError> {
                for (id, guaranteed, live) in &baseline {
                    if id == &target || *live < *guaranteed {
                        continue;
                    }
                    let entry = allocator.budget().tenant(id).unwrap();
                    prop_assert!(
                        entry.live() >= *guaranteed,
                        "tenant {id} fell to {} below its guarantee {guaranteed}",
                        entry.live()
                    );
                }
                Ok(())
            };
        assert_nobody_dropped(&allocator)?;

        for _ in 0..(raise + 2) {
            let client = allocator.connect_client(&target).unwrap();
            if let Admission::Granted(lease) = allocator.try_lease(client, RequestKind::Normal) {
                leases.push(lease);
            }
            assert_nobody_dropped(&allocator)?;

            let marked = allocator.close_needed_active();
            if let Some(position) = leases
                .iter()
                .position(|lease| marked.contains(&lease.server))
            {
                let outcome = allocator.release(leases.swap_remove(position));
                leases.extend(outcome.grants.into_iter().map(|grant| grant.lease));
            }
            assert_nobody_dropped(&allocator)?;
        }
    }
}
