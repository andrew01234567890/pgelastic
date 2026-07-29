//! The fleet: the instances this proxy fronts, and which tenant is on which.
//!
//! An instance is a **capacity boundary**, not just an address. Its
//! `backendConnections` is derived from its own `max_connections`, so each one
//! gets its own [`PoolManager`] with its own allocator, its own ledger and its
//! own connect gates. That is what makes the write-stall blast radius
//! containable at all: a stalled instance's backends park in `IPC.SyncRep`
//! against *its* budget, and a tenant on another instance cannot be starved by
//! them because there is no shared account to starve it out of.
//!
//! Each instance also carries its own [`EpochFence`](crate::epoch::EpochFence).
//! Two instances are two primaries with two independent promotion histories,
//! and one shared epoch counter would fence links to the instance that never
//! failed over.
//!
//! The routing table is the thing a migration cutover flips. It is read at
//! every checkout rather than captured when a client connects, so a client that
//! is queued behind a quiesce resumes against whichever instance the table names
//! when it is released — which is the entire point of `setRoute`.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::config::{BackendConfig, Config, DEFAULT_INSTANCE, PoolConfig};
use crate::epoch::FenceRuntime;
use crate::error::Result;
use crate::metrics::Metrics;
use crate::pool::PoolManager;
use crate::stall::StallMonitor;
use crate::tls::BackendTls;
use tracing::warn;

/// The name of one `PgInstance`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstanceId(Arc<str>);

impl InstanceId {
    pub fn new(name: impl AsRef<str>) -> Self {
        Self(Arc::from(name.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One instance and everything that is scoped to it.
pub struct Instance {
    pub id: InstanceId,
    pub backend: BackendConfig,
    pub tls: Option<BackendTls>,
    pub pools: Arc<PoolManager>,
    pub fence: FenceRuntime,
    pub stall: Arc<StallMonitor>,
}

impl fmt::Debug for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Instance")
            .field("id", &self.id)
            .field("backend", &self.backend.address)
            .finish_non_exhaustive()
    }
}

/// Every instance, plus the tenant routing table.
#[derive(Debug)]
pub struct Fleet {
    instances: HashMap<InstanceId, Arc<Instance>>,
    order: Vec<InstanceId>,
    default: InstanceId,
    routes: Mutex<HashMap<String, InstanceId>>,
    metrics: Arc<Metrics>,
}

impl Fleet {
    /// Builds the fleet a configuration describes.
    ///
    /// A configuration with no `[[instances]]` describes exactly one instance,
    /// named [`DEFAULT_INSTANCE`], at `backend.address` — so the single-instance
    /// deployment keeps its meaning and never pays for a fleet it does not
    /// have.
    pub fn build(config: &Config, metrics: &Arc<Metrics>) -> Result<Arc<Self>> {
        let declared: Vec<(InstanceId, BackendConfig, Option<u32>)> = if config.instances.is_empty()
        {
            vec![(
                InstanceId::new(DEFAULT_INSTANCE),
                config.backend.clone(),
                None,
            )]
        } else {
            config
                .instances
                .iter()
                .map(|instance| {
                    (
                        InstanceId::new(&instance.name),
                        instance.backend(&config.backend),
                        instance.backend_connections,
                    )
                })
                .collect()
        };

        let mut instances = HashMap::new();
        let mut order = Vec::new();
        for (id, backend, backend_connections) in declared {
            let host = backend
                .address
                .rsplit_once(':')
                .map_or(backend.address.as_str(), |(host, _)| host);
            let tls = crate::tls::backend_connector(&backend.tls, host)?;
            let pool_config = PoolConfig {
                backend_connections: backend_connections.unwrap_or(config.pool.backend_connections),
                ..config.pool.clone()
            };
            let fence = FenceRuntime::from(&config.fence);
            let pools =
                PoolManager::new(id.clone(), pool_config, fence.clone(), Arc::clone(metrics))?;
            pools.publish_budget();
            let stall = StallMonitor::new(
                id.clone(),
                config.stall.confirmations,
                config.stall.enabled && config.stall.fail_fast,
            );
            order.push(id.clone());
            instances.insert(
                id.clone(),
                Arc::new(Instance {
                    id,
                    backend,
                    tls,
                    pools,
                    fence,
                    stall,
                }),
            );
        }

        let default = config
            .routing
            .default_instance
            .as_ref()
            .map_or_else(|| order[0].clone(), InstanceId::new);
        let routes = config
            .routing
            .tenants
            .iter()
            .map(|(tenant, instance)| (tenant.clone(), InstanceId::new(instance)))
            .collect();

        Ok(Arc::new(Self {
            instances,
            order,
            default,
            routes: Mutex::new(routes),
            metrics: Arc::clone(metrics),
        }))
    }

    pub fn instances(&self) -> impl Iterator<Item = &Arc<Instance>> {
        self.order.iter().map(|id| {
            self.instances
                .get(id)
                .expect("the order names every instance")
        })
    }

    pub fn get(&self, id: &InstanceId) -> Option<&Arc<Instance>> {
        self.instances.get(id)
    }

    pub fn default_instance(&self) -> &Arc<Instance> {
        self.instances
            .get(&self.default)
            .expect("the default instance is validated at start-up")
    }

    /// Which instance a tenant's next transaction goes to.
    pub fn route(&self, tenant: &str) -> Arc<Instance> {
        let id = self
            .routes
            .lock()
            .expect("the routing table is never poisoned")
            .get(tenant)
            .cloned()
            .unwrap_or_else(|| self.default.clone());
        self.instances
            .get(&id)
            .map_or_else(|| Arc::clone(self.default_instance()), Arc::clone)
    }

    pub fn route_id(&self, tenant: &str) -> InstanceId {
        self.route(tenant).id.clone()
    }

    /// Which of `known` this proxy would route to `instance`, resolved in one
    /// pass under one acquisition of the routing lock.
    ///
    /// Two things make this not a filter over the routes map. The map holds
    /// only the tenants somebody published a route for, while [`route`](Self::route)
    /// falls back to the default instance — so a tenant with no entry is on the
    /// default and belongs in the answer when `instance` is it. And the lock
    /// this reads is the one every checkout takes, so asking per tenant would
    /// put a linear number of acquisitions of the data path's own mutex on the
    /// control plane; asking once does not.
    ///
    /// `known` is the caller's tenant universe — in practice every tenant the
    /// proxy has admitted a client for. A tenant nobody has connected as has no
    /// clients to hold, so its absence costs nothing.
    pub fn tenants_on(&self, instance: &InstanceId, known: &[String]) -> Vec<String> {
        if !self.instances.contains_key(instance) {
            return Vec::new();
        }
        let default_is_target = &self.default == instance;
        let routes = self
            .routes
            .lock()
            .expect("the routing table is never poisoned");
        let mut on = Vec::new();
        for tenant in known {
            let here = match routes.get(tenant) {
                // A route naming an instance this proxy does not front is
                // dropped by `route`, so it resolves to the default too.
                Some(id) if self.instances.contains_key(id) => id == instance,
                _ => default_is_target,
            };
            if here {
                on.push(tenant.clone());
            }
        }
        on
    }

    /// Points a tenant at another instance.
    ///
    /// Takes effect at the next checkout, which for a quiesced tenant is the
    /// moment its queued clients are released — so the flip and the resume are
    /// two calls rather than one racy one.
    pub fn set_route(&self, tenant: &str, instance: &InstanceId) -> Option<InstanceId> {
        if !self.instances.contains_key(instance) {
            return None;
        }
        let previous = self
            .routes
            .lock()
            .expect("the routing table is never poisoned")
            .insert(tenant.to_owned(), instance.clone());
        self.metrics.tenant_rerouted();
        Some(previous.unwrap_or_else(|| self.default.clone()))
    }

    /// Replaces the whole routing table with the one the operator published.
    ///
    /// Wholesale rather than entry by entry, because a tenant that has left the
    /// pool has to lose its route as well as gain none: merging would leave a
    /// deleted tenant's clients pointed at an instance that no longer holds
    /// their data. Entries naming an instance this proxy does not front are
    /// dropped and counted rather than applied — a fleet mid-rollout can see a
    /// route to an instance its own process was not built with.
    ///
    /// Returns the number of routes that changed. Nothing here touches a session
    /// in flight: the table is read at every checkout, so a client between two
    /// transactions moves and a client inside one does not.
    pub fn apply_routes(&self, routes: &std::collections::BTreeMap<String, String>) -> usize {
        let mut wanted = HashMap::with_capacity(routes.len());
        for (tenant, instance) in routes {
            let id = InstanceId::new(instance);
            if self.instances.contains_key(&id) {
                wanted.insert(tenant.clone(), id);
            } else {
                warn!(
                    tenant,
                    instance, "a published route names an instance this proxy does not front"
                );
            }
        }

        let mut current = self
            .routes
            .lock()
            .expect("the routing table is never poisoned");
        if *current == wanted {
            return 0;
        }
        let changed = wanted
            .iter()
            .filter(|(tenant, id)| current.get(*tenant) != Some(id))
            .count()
            + current
                .keys()
                .filter(|tenant| !wanted.contains_key(*tenant))
                .count();
        *current = wanted;
        drop(current);
        for _ in 0..changed {
            self.metrics.tenant_rerouted();
        }
        changed
    }

    /// Refreshes every instance's budget gauge.
    pub fn publish_budget(&self) {
        for instance in self.instances() {
            instance.pools.publish_budget();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr as _;

    const MINIMAL: &str = r#"
        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:5432"
        user = "postgres"
    "#;

    fn fleet(source: &str) -> Arc<Fleet> {
        let config = Config::from_str(source).expect("the configuration parses");
        Fleet::build(&config, &Metrics::new()).expect("the fleet builds")
    }

    #[test]
    fn a_configuration_with_no_instances_describes_exactly_one() {
        let fleet = fleet(MINIMAL);
        assert_eq!(fleet.instances().count(), 1);
        assert_eq!(fleet.default_instance().id.as_str(), DEFAULT_INSTANCE);
        assert_eq!(fleet.route("anybody").id.as_str(), DEFAULT_INSTANCE);
    }

    const TWO: &str = r#"
        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:5432"
        user = "postgres"

        [[instances]]
        name = "inst-a"
        address = "127.0.0.1:5001"

        [[instances]]
        name = "inst-b"
        address = "127.0.0.1:5002"
        backendConnections = 4

        [routing]
        defaultInstance = "inst-a"
        tenants = { beta = "inst-b" }
    "#;

    #[test]
    fn each_instance_carries_its_own_address_and_budget() {
        let fleet = fleet(TWO);
        let a = fleet.get(&InstanceId::new("inst-a")).unwrap();
        let b = fleet.get(&InstanceId::new("inst-b")).unwrap();
        assert_eq!(a.backend.address, "127.0.0.1:5001");
        assert_eq!(b.backend.address, "127.0.0.1:5002");
        assert_eq!(a.pools.config().backend_connections, 20);
        assert_eq!(b.pools.config().backend_connections, 4);
    }

    #[test]
    fn two_instances_never_share_a_capacity_account() {
        let fleet = fleet(TWO);
        let a = fleet.get(&InstanceId::new("inst-a")).unwrap();
        let b = fleet.get(&InstanceId::new("inst-b")).unwrap();
        assert!(!Arc::ptr_eq(&a.pools, &b.pools));
        assert!(!Arc::ptr_eq(&a.fence.fence, &b.fence.fence));
        assert!(!Arc::ptr_eq(&a.stall, &b.stall));
    }

    #[test]
    fn the_routing_table_starts_where_the_configuration_put_it() {
        let fleet = fleet(TWO);
        assert_eq!(fleet.route("alpha").id.as_str(), "inst-a");
        assert_eq!(fleet.route("beta").id.as_str(), "inst-b");
    }

    #[test]
    fn set_route_moves_a_tenant_and_reports_where_it_was() {
        let fleet = fleet(TWO);
        let previous = fleet
            .set_route("alpha", &InstanceId::new("inst-b"))
            .expect("inst-b exists");
        assert_eq!(previous.as_str(), "inst-a");
        assert_eq!(fleet.route("alpha").id.as_str(), "inst-b");
    }

    #[test]
    fn set_route_to_an_instance_this_proxy_does_not_front_changes_nothing() {
        let fleet = fleet(TWO);
        assert!(
            fleet
                .set_route("alpha", &InstanceId::new("elsewhere"))
                .is_none()
        );
        assert_eq!(fleet.route("alpha").id.as_str(), "inst-a");
    }

    #[test]
    fn a_tenant_routed_nowhere_lands_on_the_default() {
        let fleet = fleet(TWO);
        assert_eq!(fleet.route("never-seen").id.as_str(), "inst-a");
    }

    fn known() -> Vec<String> {
        ["alpha", "beta", "gamma"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn an_instances_tenants_include_the_ones_the_routing_table_never_mentions() {
        let fleet = fleet(TWO);
        assert_eq!(
            fleet.tenants_on(&InstanceId::new("inst-a"), &known()),
            vec!["alpha".to_owned(), "gamma".to_owned()],
            "a tenant with no route is on the default instance"
        );
        assert_eq!(
            fleet.tenants_on(&InstanceId::new("inst-b"), &known()),
            vec!["beta".to_owned()]
        );
    }

    #[test]
    fn a_flip_moves_a_tenant_between_the_two_answers() {
        let fleet = fleet(TWO);
        fleet.set_route("gamma", &InstanceId::new("inst-b"));
        assert_eq!(
            fleet.tenants_on(&InstanceId::new("inst-a"), &known()),
            vec!["alpha".to_owned()]
        );
        assert_eq!(
            fleet.tenants_on(&InstanceId::new("inst-b"), &known()),
            vec!["beta".to_owned(), "gamma".to_owned()]
        );
    }

    #[test]
    fn an_instance_this_proxy_does_not_front_holds_no_tenants() {
        let fleet = fleet(TWO);
        assert!(
            fleet
                .tenants_on(&InstanceId::new("elsewhere"), &known())
                .is_empty()
        );
    }
}
