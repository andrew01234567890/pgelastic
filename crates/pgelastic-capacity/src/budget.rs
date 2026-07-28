//! The per-pool capacity budget: the arithmetic, and nothing else.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::config::{PoolSpec, QosClass, Ratio, Reservations, TenantSpec, check_reservations};
use crate::error::{ConfigError, ShrinkScope};
use crate::types::TenantId;

/// One row of the budget's per-tenant table.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TenantEntry {
    spec: TenantSpec,
    live: u32,
}

impl TenantEntry {
    pub const fn spec(&self) -> TenantSpec {
        self.spec
    }

    pub const fn guaranteed(&self) -> u32 {
        self.spec.guaranteed
    }

    pub const fn burstable(&self) -> u32 {
        self.spec.burstable
    }

    pub const fn weight(&self) -> u32 {
        self.spec.weight
    }

    pub const fn priority(&self) -> u32 {
        self.spec.priority
    }

    /// Backend connections currently attributed to this tenant, idle included.
    /// An idle server still occupies a slot, which is why revocation can free
    /// capacity by closing one.
    pub const fn live(&self) -> u32 {
        self.live
    }

    pub const fn qos_class(&self) -> QosClass {
        self.spec.qos_class()
    }

    /// How far the tenant is above its guarantee — what revocation may take.
    pub const fn surplus(&self) -> u32 {
        self.live.saturating_sub(self.spec.guaranteed)
    }

    /// How far the tenant is below its guarantee — the primary fairness key.
    pub const fn deficit(&self) -> u32 {
        self.spec.guaranteed.saturating_sub(self.live)
    }

    /// `(live − guaranteed) / (burstable − guaranteed)`, the secondary fairness
    /// key. A tenant with no burst range at all has no meaningful fraction.
    pub const fn burst_fraction(&self) -> Option<Ratio> {
        let span = self.spec.burstable - self.spec.guaranteed;
        if span == 0 {
            None
        } else {
            Some(Ratio::new(self.surplus() as u64, span as u64))
        }
    }
}

/// Total, headroom, and the per-tenant table.
#[derive(Clone, Debug)]
pub struct CapacityBudget {
    spec: PoolSpec,
    tenants: BTreeMap<TenantId, TenantEntry>,
    reserved: u32,
    committed_burst: u64,
    live_total: u32,
    /// `Σ max(0, live_i − guaranteed_i)`, maintained incrementally so `free()`
    /// costs no iteration on the admission hot path.
    surplus_total: u32,
}

impl CapacityBudget {
    pub fn new(spec: PoolSpec) -> Result<Self, ConfigError> {
        spec.validate()?;
        Ok(Self {
            spec,
            tenants: BTreeMap::new(),
            reserved: 0,
            committed_burst: 0,
            live_total: 0,
            surplus_total: 0,
        })
    }

    pub const fn spec(&self) -> &PoolSpec {
        &self.spec
    }

    pub const fn total(&self) -> u32 {
        self.spec.backend_connections
    }

    pub const fn headroom_percent(&self) -> u8 {
        self.spec.headroom_percent
    }

    pub const fn allocatable(&self) -> u32 {
        self.spec.allocatable()
    }

    pub const fn reserved(&self) -> u32 {
        self.reserved
    }

    pub const fn committed_burst(&self) -> u64 {
        self.committed_burst
    }

    pub const fn live_total(&self) -> u32 {
        self.live_total
    }

    /// `Σ burstable / allocatable`. Above 1.0 is the product, not a bug.
    pub const fn oversubscription(&self) -> Ratio {
        Ratio::new(self.committed_burst, self.allocatable() as u64)
    }

    /// `free() = total − Σ_i max(guaranteed_i, live_i)`.
    ///
    /// The `max()` is load-bearing. An idle tenant's unused guarantee is
    /// deliberately **not** lendable: if it were, every guarantee request would
    /// arrive at a pool with zero free capacity and have to revoke somebody
    /// else's connection first, which reduces a guarantee to a promise plus
    /// eviction latency. Tenants who want their idle capacity lent out set
    /// `guaranteed: 0` — that is exactly what `qosClass: BestEffort` means.
    pub const fn free(&self) -> u32 {
        self.total()
            .saturating_sub(self.reserved)
            .saturating_sub(self.surplus_total)
    }

    /// Headroom bounds *reservations*, not bursts: `free()` is measured against
    /// `total` so a burst may use the headroom, but `Σ guaranteed` may not.
    /// The headroom is what makes a guarantee grantable without revocation.
    pub fn reservations(&self) -> Reservations {
        Reservations {
            total: self.total(),
            allocatable: self.allocatable(),
            reserved: self.reserved,
            available: self.allocatable().saturating_sub(self.reserved),
            committed_burst: self.committed_burst,
            oversubscription: self.oversubscription(),
        }
    }

    pub fn tenant(&self, id: &TenantId) -> Option<&TenantEntry> {
        self.tenants.get(id)
    }

    pub fn contains(&self, id: &TenantId) -> bool {
        self.tenants.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.tenants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TenantId, &TenantEntry)> {
        self.tenants.iter()
    }

    pub fn insert_tenant(&mut self, id: TenantId, spec: TenantSpec) -> Result<(), ConfigError> {
        if self.tenants.contains_key(&id) {
            return Err(ConfigError::DuplicateTenant(id));
        }
        self.check_with(Some((&id, spec)), None)?;
        self.tenants.insert(id, TenantEntry { spec, live: 0 });
        self.reserved += spec.guaranteed;
        self.committed_burst += u64::from(spec.burstable);
        Ok(())
    }

    pub fn update_tenant(&mut self, id: &TenantId, spec: TenantSpec) -> Result<(), ConfigError> {
        let Some(entry) = self.tenants.get(id) else {
            return Err(ConfigError::UnknownTenant(id.clone()));
        };
        if spec.burstable < entry.live {
            return Err(ConfigError::ShrinkBelowLive {
                scope: ShrinkScope::TenantBurstable,
                target: spec.burstable,
                live: entry.live,
            });
        }
        self.check_with(None, Some((id, spec)))?;

        let entry = self.tenants.get_mut(id).expect("checked above");
        self.reserved = self.reserved - entry.spec.guaranteed + spec.guaranteed;
        self.committed_burst =
            self.committed_burst - u64::from(entry.spec.burstable) + u64::from(spec.burstable);
        self.surplus_total -= entry.surplus();
        entry.spec = spec;
        self.surplus_total += entry.surplus();
        Ok(())
    }

    /// Removing a tenant with live connections would corrupt the ledger; the
    /// allocator closes its servers first.
    pub fn remove_tenant(&mut self, id: &TenantId) -> Result<TenantEntry, ConfigError> {
        let Entry::Occupied(occupied) = self.tenants.entry(id.clone()) else {
            return Err(ConfigError::UnknownTenant(id.clone()));
        };
        let entry = occupied.remove();
        self.reserved -= entry.spec.guaranteed;
        self.committed_burst -= u64::from(entry.spec.burstable);
        self.surplus_total -= entry.surplus();
        self.live_total -= entry.live;
        Ok(entry)
    }

    pub fn set_pool_spec(&mut self, spec: PoolSpec) -> Result<(), ConfigError> {
        spec.validate()?;
        if spec.backend_connections < self.live_total {
            return Err(ConfigError::ShrinkBelowLive {
                scope: ShrinkScope::PoolCapacity,
                target: spec.backend_connections,
                live: self.live_total,
            });
        }
        check_reservations(&spec, self.tenants.iter().map(|(id, e)| (id, e.spec)))?;
        self.spec = spec;
        Ok(())
    }

    pub(crate) fn inc_live(&mut self, id: &TenantId) {
        let entry = self.tenants.get_mut(id).expect("tenant is bound");
        let before = entry.surplus();
        entry.live += 1;
        self.surplus_total += entry.surplus() - before;
        self.live_total += 1;
    }

    pub(crate) fn dec_live(&mut self, id: &TenantId) {
        let entry = self.tenants.get_mut(id).expect("tenant is bound");
        let before = entry.surplus();
        entry.live -= 1;
        self.surplus_total -= before - entry.surplus();
        self.live_total -= 1;
    }

    fn check_with(
        &self,
        added: Option<(&TenantId, TenantSpec)>,
        replaced: Option<(&TenantId, TenantSpec)>,
    ) -> Result<Reservations, ConfigError> {
        let existing = self.tenants.iter().map(move |(id, entry)| match replaced {
            Some((target, spec)) if target == id => (id, spec),
            _ => (id, entry.spec),
        });
        check_reservations(&self.spec, existing.chain(added))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PoolSpec;

    fn budget(total: u32, headroom: u8) -> CapacityBudget {
        CapacityBudget::new(PoolSpec {
            backend_connections: total,
            headroom_percent: headroom,
            max_oversubscription: None,
            ..PoolSpec::default()
        })
        .unwrap()
    }

    fn spec(guaranteed: u32, burstable: u32) -> TenantSpec {
        TenantSpec {
            guaranteed,
            burstable,
            ..TenantSpec::default()
        }
    }

    #[test]
    fn an_idle_tenants_guarantee_is_not_lendable() {
        let mut budget = budget(100, 0);
        budget
            .insert_tenant("guaranteed".into(), spec(10, 10))
            .unwrap();
        budget
            .insert_tenant("burster".into(), spec(0, 100))
            .unwrap();

        assert_eq!(budget.free(), 90);

        let burster = TenantId::from("burster");
        for _ in 0..90 {
            budget.inc_live(&burster);
        }
        assert_eq!(budget.free(), 0);
        assert_eq!(budget.live_total(), 90);
    }

    #[test]
    fn free_capacity_tracks_the_max_of_guarantee_and_live() {
        let mut budget = budget(50, 0);
        budget.insert_tenant("a".into(), spec(10, 40)).unwrap();
        let a = TenantId::from("a");

        for expected in [40, 40, 40] {
            assert_eq!(budget.free(), expected);
            budget.inc_live(&a);
        }
        for _ in 3..10 {
            budget.inc_live(&a);
        }
        assert_eq!(budget.free(), 40);

        budget.inc_live(&a);
        assert_eq!(budget.free(), 39);
    }

    #[test]
    fn shrinking_the_pool_below_the_live_count_is_rejected() {
        let mut budget = budget(10, 0);
        budget.insert_tenant("a".into(), spec(0, 10)).unwrap();
        let a = TenantId::from("a");
        for _ in 0..6 {
            budget.inc_live(&a);
        }

        let err = budget
            .set_pool_spec(PoolSpec {
                backend_connections: 4,
                headroom_percent: 0,
                max_oversubscription: None,
                ..PoolSpec::default()
            })
            .unwrap_err();
        assert_eq!(
            err,
            ConfigError::ShrinkBelowLive {
                scope: ShrinkScope::PoolCapacity,
                target: 4,
                live: 6
            }
        );
    }

    #[test]
    fn raising_a_guarantee_past_allocatable_is_rejected() {
        let mut budget = budget(100, 25);
        budget.insert_tenant("a".into(), spec(70, 70)).unwrap();
        budget.insert_tenant("b".into(), spec(5, 20)).unwrap();

        let err = budget.update_tenant(&"b".into(), spec(10, 20)).unwrap_err();
        assert_eq!(
            err,
            ConfigError::OverCommitted {
                reserved: 80,
                allocatable: 75
            }
        );
        assert_eq!(budget.reserved(), 75);
    }
}
