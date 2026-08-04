/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package placement chooses which PgInstance a tenant's database lives on.
//
// Two algorithms, for two different questions. Packing a whole pool at once is
// Best-Fit-Decreasing: sort the tenants by how much they demand and give each the fullest
// instance that can still hold it, which is the classic bin-packing heuristic and the one
// that leaves whole instances empty for a scale-in to reclaim. Admitting a single new tenant
// is power-of-two-choices: sample two candidate instances and take the better. Running
// best-fit for a single admission would send every concurrently admitted tenant to the same
// instance, because they all read the same ledger before any of them has written to it.
//
// Everything is packed on the trailing-window percentile the pool's policy names, never on
// the mean. A bursty tenant's mean is a number no instance is ever asked to serve.
package placement

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// ErrOverCommitted reports that the guarantees already sold exceed what the pool's instances
// can hold. No packing exists, and producing one anyway would mean quietly breaking a floor
// somebody was promised.
var ErrOverCommitted = errors.New("pool is over-committed: the sum of guaranteed connections exceeds the instances' allocatable capacity")

// Refusal reasons. Each names the constraint that blocked the placement, because "no
// instance available" is the answer that makes an operator open a support ticket.
const (
	ReasonNoCapacity   = "NoCapacity"
	ReasonAntiAffinity = "AntiAffinityConflict"
	// ReasonVersionUnsupported is a destination whose PostgreSQL major the migration cannot
	// reach from where the tenant currently is.
	ReasonVersionUnsupported  = "VersionUnsupported"
	ReasonPinnedUnavailable   = "PinnedInstanceUnavailable"
	ReasonNoInstances         = "NoSchedulableInstance"
	ReasonInstanceUnavailable = "InstanceUnavailable"
	ReasonOverCommitted       = "PoolOverCommitted"
)

// Demand is one tenant's claim on an instance, in every dimension that is packed on.
type Demand struct {
	// GuaranteedConnections is the reservation the tenant was admitted with. It is a hard
	// constraint: an instance that cannot hold it cannot host the tenant, whatever the
	// tenant's observed usage says.
	GuaranteedConnections int32
	// ObservedConnections is the trailing-window statistic the pool packs on — the 95th
	// percentile by default. It is what makes packing reflect what tenants do rather than
	// what they declared.
	ObservedConnections int32
	// StorageBytes is the tenant database's allocated size.
	StorageBytes int64
	// Relations is the tenant's relation count, which bounds catalogue size, autovacuum
	// work and the duration of a logical-replication copy.
	Relations int32
}

// Connections is the connection demand an instance has to satisfy: the reservation, or the
// observed percentile when the tenant routinely exceeds its own floor.
func (d Demand) Connections() int32 {
	return max(d.GuaranteedConnections, d.ObservedConnections)
}

// Capacity is what an instance can hold.
type Capacity struct {
	Connections  int32
	StorageBytes int64
	Relations    int32
}

func (c Capacity) add(other Capacity) Capacity {
	return Capacity{
		Connections:  c.Connections + other.Connections,
		StorageBytes: c.StorageBytes + other.StorageBytes,
		Relations:    c.Relations + other.Relations,
	}
}

func (d Demand) asCapacity() Capacity {
	return Capacity{Connections: d.Connections(), StorageBytes: d.StorageBytes, Relations: d.Relations}
}

// Tenant is one placement candidate.
type Tenant struct {
	// Name is the PgTenant's name.
	Name string
	// Demand is what it needs.
	Demand Demand
	// AntiAffinity maps each label key the tenant declared to that key's value on the
	// tenant. Two tenants sharing a key and a value must not share an instance: correlated
	// workloads defeat the oversubscription bet the whole capacity model rests on.
	AntiAffinity map[string]string
	// PinnedInstance is set when the tenant names an instance outright. A pin is honoured
	// or refused, never silently overridden.
	PinnedInstance string
	// BoundInstance is where the tenant currently lives, if anywhere. A repack prefers to
	// leave it there when the alternatives are equally good, because every move is a live
	// migration.
	BoundInstance string
	// GuaranteedOnly marks a tenant that has never been metered, so its observed demand is
	// an absence rather than a zero.
	GuaranteedOnly bool
	// BoundMajor is the PostgreSQL major the tenant currently sits on. Zero means it sits
	// nowhere yet, which is what makes a first admission unconstrained by any of this.
	BoundMajor int
}

// Instance is one placement target.
type Instance struct {
	// Name is the PgInstance's name.
	Name string
	// Capacity is what the instance can hold in total.
	Capacity Capacity
	// Reserved is capacity already consumed by things not being packed in this call —
	// tenants pinned elsewhere in the same pass, or overhead the instance itself carries.
	Reserved Capacity
	// Schedulable is false for a cordoned or draining instance. It may still host the
	// tenants already on it; it takes no new ones.
	Schedulable bool
	// Major is the PostgreSQL major this instance runs. Zero means unknown, which is treated
	// as "do not refuse on this axis" rather than as a version.
	Major int
	// Ready is false while the instance cannot serve tenant traffic. Its headroom must not
	// be counted as available: an instance re-cloning a replica has capacity on paper only.
	Ready bool
	// Occupied records, per anti-affinity label key, the values already hosted here by
	// tenants that are not part of this packing. Without it an admission would place a
	// tenant next to the very sibling its anti-affinity key exists to keep it away from.
	Occupied map[string]map[string]struct{}
	// Tenants already hosted here and not part of this packing. The skew bound is a count,
	// so an admission that could not see the counts would never narrow anything and the
	// bound would silently do nothing.
	Tenants int32
}

// Policy is the pool's placement configuration, resolved.
type Policy struct {
	// PackOn names the trailing-window statistic the caller filled Demand.ObservedConnections
	// from. It is carried for the explanation, not for the arithmetic.
	PackOn pgelasticv1alpha1.Percentile
	// MaxSkewTenants bounds how much fuller, in tenant count, the chosen instance may be
	// than the emptiest one before placement starts preferring the emptiest. Best-fit alone
	// produces a pool where one instance holds everything and a restart of it is an outage
	// for every tenant.
	MaxSkewTenants int32
}

// Assignment is one tenant placed on one instance.
type Assignment struct {
	Tenant   string
	Instance string
	// Moved reports that the tenant was already bound somewhere else, so honouring this
	// assignment costs a live migration.
	Moved bool
	// From is the instance the tenant is leaving, when Moved.
	From string
}

// Refusal is one tenant that could not be placed, and why.
type Refusal struct {
	Tenant  string
	Reason  string
	Message string
}

// Result is one packing.
type Result struct {
	Assignments []Assignment
	Refusals    []Refusal
	// PerInstance is the post-packing load of each instance, keyed by name, which is what
	// the caller publishes as the per-instance ledger.
	PerInstance map[string]Capacity
	// TenantsPerInstance is the post-packing tenant count of each instance.
	TenantsPerInstance map[string]int32
}

// AssignmentFor returns the instance a tenant was placed on.
func (r Result) AssignmentFor(tenant string) (Assignment, bool) {
	for _, assignment := range r.Assignments {
		if assignment.Tenant == tenant {
			return assignment, true
		}
	}
	return Assignment{}, false
}

// RefusalFor returns the refusal recorded for a tenant.
func (r Result) RefusalFor(tenant string) (Refusal, bool) {
	for _, refusal := range r.Refusals {
		if refusal.Tenant == tenant {
			return refusal, true
		}
	}
	return Refusal{}, false
}

// Moves is every assignment that relocates an already-bound tenant.
func (r Result) Moves() []Assignment {
	moves := make([]Assignment, 0, len(r.Assignments))
	for _, assignment := range r.Assignments {
		if assignment.Moved {
			moves = append(moves, assignment)
		}
	}
	return moves
}

// bin is an instance's mutable state during a packing.
type bin struct {
	instance Instance
	used     Capacity
	tenants  int32
	// labels holds, per anti-affinity key, the values already present on the instance.
	labels map[string]map[string]struct{}
}

func newBin(instance Instance) *bin {
	created := &bin{
		instance: instance,
		used:     instance.Reserved,
		tenants:  instance.Tenants,
		labels:   map[string]map[string]struct{}{},
	}
	for key, values := range instance.Occupied {
		copied := make(map[string]struct{}, len(values))
		for value := range values {
			copied[value] = struct{}{}
		}
		created.labels[key] = copied
	}
	return created
}

// free is the headroom left in each dimension. A dimension whose capacity is zero is
// unconstrained rather than full: an instance whose relation ceiling has never been measured
// must not refuse every tenant.
func (b *bin) free() Capacity {
	return Capacity{
		Connections:  b.instance.Capacity.Connections - b.used.Connections,
		StorageBytes: b.instance.Capacity.StorageBytes - b.used.StorageBytes,
		Relations:    b.instance.Capacity.Relations - b.used.Relations,
	}
}

// fits reports whether the tenant can go here, and when it cannot, both the reason code the
// refusal will carry and a message naming the numbers.
func (b *bin) fits(tenant Tenant) (bool, string, string) {
	if !b.instance.Ready {
		return false, ReasonInstanceUnavailable, "is not ready to serve tenant traffic"
	}
	if !b.instance.Schedulable {
		return false, ReasonInstanceUnavailable, "is cordoned"
	}
	// A move has to be one the migration can actually perform. Both of this tree's dumps run
	// in the *target's* container, so pg_dump cannot read a server newer than itself: a move
	// to an older major is refused at preflight, permanently and by construction. A packer
	// blind to the major proposes exactly those moves, and each one becomes a migration that
	// is refused for ever - so this refuses them where they are proposed rather than where
	// they are carried out.
	//
	// One major forward is allowed and is the whole point: the recommended upgrade route is
	// an instance on the new major with tenants migrated onto it one at a time. Two is
	// refused because one at a time is the path that gets tested, and a tenant with no
	// binding carries no floor at all.
	if tenant.BoundMajor > 0 && b.instance.Major > 0 {
		if b.instance.Major < tenant.BoundMajor {
			return false, ReasonVersionUnsupported, fmt.Sprintf(
				"runs PostgreSQL %d and the tenant is on %d; a dump runs in the target and "+
					"cannot read a newer server", b.instance.Major, tenant.BoundMajor)
		}
		if b.instance.Major > tenant.BoundMajor+1 {
			return false, ReasonVersionUnsupported, fmt.Sprintf(
				"runs PostgreSQL %d and the tenant is on %d; majors are crossed one at a time",
				b.instance.Major, tenant.BoundMajor)
		}
	}
	for key, value := range tenant.AntiAffinity {
		if values, ok := b.labels[key]; ok {
			if _, clash := values[value]; clash {
				return false, ReasonAntiAffinity,
					fmt.Sprintf("already hosts a tenant with %s=%s", key, value)
			}
		}
	}
	free := b.free()
	demand := tenant.Demand
	if b.instance.Capacity.Connections > 0 && demand.Connections() > free.Connections {
		return false, ReasonNoCapacity,
			fmt.Sprintf("has %d connections free for a tenant needing %d", max(free.Connections, 0), demand.Connections())
	}
	if b.instance.Capacity.StorageBytes > 0 && demand.StorageBytes > free.StorageBytes {
		return false, ReasonNoCapacity,
			fmt.Sprintf("has %d storage bytes free for a tenant needing %d", max(free.StorageBytes, 0), demand.StorageBytes)
	}
	if b.instance.Capacity.Relations > 0 && demand.Relations > free.Relations {
		return false, ReasonNoCapacity,
			fmt.Sprintf("has room for %d more relations, tenant has %d", max(free.Relations, 0), demand.Relations)
	}
	return true, "", ""
}

func (b *bin) place(tenant Tenant) {
	b.used = b.used.add(tenant.Demand.asCapacity())
	b.tenants++
	for key, value := range tenant.AntiAffinity {
		values, ok := b.labels[key]
		if !ok {
			values = map[string]struct{}{}
			b.labels[key] = values
		}
		values[value] = struct{}{}
	}
}

// residual scores how much room is left after a hypothetical placement, normalised across
// dimensions so that connections and storage are comparable. Lower is a tighter fit, which
// is what best-fit wants.
func (b *bin) residual(tenant Tenant) float64 {
	free := b.free()
	capacity := b.instance.Capacity
	worst := float64(0)
	for _, dimension := range []struct{ free, demand, total float64 }{
		{float64(free.Connections), float64(tenant.Demand.Connections()), float64(capacity.Connections)},
		{float64(free.StorageBytes), float64(tenant.Demand.StorageBytes), float64(capacity.StorageBytes)},
		{float64(free.Relations), float64(tenant.Demand.Relations), float64(capacity.Relations)},
	} {
		if dimension.total <= 0 {
			continue
		}
		worst = max(worst, (dimension.free-dimension.demand)/dimension.total)
	}
	return worst
}

// size is a tenant's dominant demand as a fraction of the largest instance, which is the
// ordering key for the "decreasing" half of Best-Fit-Decreasing.
func size(tenant Tenant, largest Capacity) float64 {
	worst := float64(0)
	for _, dimension := range []struct{ demand, total float64 }{
		{float64(tenant.Demand.Connections()), float64(largest.Connections)},
		{float64(tenant.Demand.StorageBytes), float64(largest.StorageBytes)},
		{float64(tenant.Demand.Relations), float64(largest.Relations)},
	} {
		if dimension.total <= 0 {
			continue
		}
		worst = max(worst, dimension.demand/dimension.total)
	}
	return worst
}

// Pack places every tenant, Best-Fit-Decreasing.
//
// It refuses the whole pool before placing anything when the guarantees already sold exceed
// the instances' capacity, because in that state any packing it produced would be a lie
// about at least one tenant's floor.
func Pack(tenants []Tenant, instances []Instance, policy Policy) (Result, error) {
	bins, byName := binsOf(instances)
	result := Result{PerInstance: map[string]Capacity{}, TenantsPerInstance: map[string]int32{}}

	if err := checkCommitment(tenants, instances); err != nil {
		for _, tenant := range tenants {
			result.Refusals = append(result.Refusals, Refusal{
				Tenant: tenant.Name, Reason: ReasonOverCommitted, Message: err.Error(),
			})
		}
		publish(&result, bins)
		return result, err
	}

	ordered := orderedByDecreasingSize(tenants, instances)

	// Pins are honoured before anything else competes for the capacity they need.
	remaining := make([]Tenant, 0, len(ordered))
	for _, tenant := range ordered {
		if tenant.PinnedInstance == "" {
			remaining = append(remaining, tenant)
			continue
		}
		target, ok := byName[tenant.PinnedInstance]
		if !ok {
			result.Refusals = append(result.Refusals, Refusal{
				Tenant: tenant.Name, Reason: ReasonPinnedUnavailable,
				Message: fmt.Sprintf("PgInstance %q is not a member of this pool", tenant.PinnedInstance),
			})
			continue
		}
		if fits, _, why := target.fits(tenant); !fits {
			result.Refusals = append(result.Refusals, Refusal{
				Tenant: tenant.Name, Reason: ReasonPinnedUnavailable,
				Message: fmt.Sprintf("pinned to PgInstance %q, which %s", tenant.PinnedInstance, why),
			})
			continue
		}
		target.place(tenant)
		result.Assignments = append(result.Assignments, assignmentOf(tenant, target.instance.Name))
	}

	for _, tenant := range remaining {
		chosen := bestFit(tenant, bins, policy)
		if chosen == nil {
			result.Refusals = append(result.Refusals, refuse(tenant, bins))
			continue
		}
		chosen.place(tenant)
		result.Assignments = append(result.Assignments, assignmentOf(tenant, chosen.instance.Name))
	}

	slices.SortFunc(result.Assignments, func(a, b Assignment) int { return strings.Compare(a.Tenant, b.Tenant) })
	slices.SortFunc(result.Refusals, func(a, b Refusal) int { return strings.Compare(a.Tenant, b.Tenant) })
	publish(&result, bins)
	return result, nil
}

// Admit chooses an instance for one new tenant, power-of-two-choices.
//
// Two feasible instances are sampled and the better one wins. Compared with taking the
// single best instance, this costs almost nothing in packing quality and removes the
// herding that a stateless best-fit produces when several tenants are admitted between two
// reconciles: they all read the same ledger, and best-fit sends every one of them to the
// same instance, which then has to shed most of them again.
func Admit(tenant Tenant, instances []Instance, policy Policy, rng *rand.Rand) (Assignment, *Refusal) {
	bins, byName := binsOf(instances)

	if tenant.PinnedInstance != "" {
		target, ok := byName[tenant.PinnedInstance]
		if !ok {
			return Assignment{}, &Refusal{
				Tenant: tenant.Name, Reason: ReasonPinnedUnavailable,
				Message: fmt.Sprintf("PgInstance %q is not a member of this pool", tenant.PinnedInstance),
			}
		}
		if fits, _, why := target.fits(tenant); !fits {
			return Assignment{}, &Refusal{
				Tenant: tenant.Name, Reason: ReasonPinnedUnavailable,
				Message: fmt.Sprintf("pinned to PgInstance %q, which %s", tenant.PinnedInstance, why),
			}
		}
		return assignmentOf(tenant, target.instance.Name), nil
	}

	candidates := feasible(tenant, bins, policy)
	switch len(candidates) {
	case 0:
		refusal := refuse(tenant, bins)
		return Assignment{}, &refusal
	case 1:
		return assignmentOf(tenant, candidates[0].instance.Name), nil
	}

	first := candidates[pick(rng, len(candidates))]
	second := candidates[pick(rng, len(candidates))]
	for second == first {
		second = candidates[pick(rng, len(candidates))]
	}
	return assignmentOf(tenant, better(tenant, first, second).instance.Name), nil
}

func pick(rng *rand.Rand, n int) int {
	if rng == nil {
		return rand.IntN(n)
	}
	return rng.IntN(n)
}

// better is the head-to-head comparison both algorithms score with: tightness of fit, then
// a stable tiebreak that keeps an already-bound tenant where it is.
func better(tenant Tenant, a, b *bin) *bin {
	switch {
	case a.residual(tenant) < b.residual(tenant):
		return a
	case b.residual(tenant) < a.residual(tenant):
		return b
	case tenant.BoundInstance == a.instance.Name:
		return a
	case tenant.BoundInstance == b.instance.Name:
		return b
	case a.instance.Name <= b.instance.Name:
		return a
	default:
		return b
	}
}

// feasible is every instance that can take the tenant, narrowed to those that would not
// break the pool's tenant-count skew bound.
//
// Narrowing before choosing rather than as a tiebreak is what makes the bound hold. A
// pairwise preference cannot: two instances that are both already too full compare only with
// each other, and one of them still wins.
func feasible(tenant Tenant, bins []*bin, policy Policy) []*bin {
	candidates := make([]*bin, 0, len(bins))
	for _, candidate := range bins {
		if fits, _, _ := candidate.fits(tenant); fits {
			candidates = append(candidates, candidate)
		}
	}
	if policy.MaxSkewTenants <= 0 || len(candidates) < 2 {
		return candidates
	}

	emptiest := int32(0)
	for i, candidate := range bins {
		if i == 0 || candidate.tenants < emptiest {
			emptiest = candidate.tenants
		}
	}
	level := make([]*bin, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.tenants+1-emptiest <= policy.MaxSkewTenants {
			level = append(level, candidate)
		}
	}
	if len(level) == 0 {
		return candidates
	}
	return level
}

func bestFit(tenant Tenant, bins []*bin, policy Policy) *bin {
	var chosen *bin
	for _, candidate := range feasible(tenant, bins, policy) {
		if chosen == nil {
			chosen = candidate
			continue
		}
		chosen = better(tenant, chosen, candidate)
	}
	return chosen
}

func binsOf(instances []Instance) ([]*bin, map[string]*bin) {
	bins := make([]*bin, 0, len(instances))
	byName := make(map[string]*bin, len(instances))
	for _, instance := range instances {
		created := newBin(instance)
		bins = append(bins, created)
		byName[instance.Name] = created
	}
	slices.SortFunc(bins, func(a, b *bin) int { return strings.Compare(a.instance.Name, b.instance.Name) })
	return bins, byName
}

// SeedBinsWithBoundTenants records tenants that are staying put, so a packing of the rest
// sees both the capacity and the anti-affinity values they already occupy. The input is left
// alone; the returned instances are a copy.
func SeedBinsWithBoundTenants(instances []Instance, bound []Tenant) []Instance {
	seeded := slices.Clone(instances)
	byName := make(map[string]*Instance, len(seeded))
	for i := range seeded {
		seeded[i].Occupied = cloneOccupied(seeded[i].Occupied)
		byName[seeded[i].Name] = &seeded[i]
	}
	for _, tenant := range bound {
		instance, ok := byName[tenant.BoundInstance]
		if !ok {
			continue
		}
		instance.Reserved = instance.Reserved.add(tenant.Demand.asCapacity())
		instance.Tenants++
		for key, value := range tenant.AntiAffinity {
			if instance.Occupied == nil {
				instance.Occupied = map[string]map[string]struct{}{}
			}
			if instance.Occupied[key] == nil {
				instance.Occupied[key] = map[string]struct{}{}
			}
			instance.Occupied[key][value] = struct{}{}
		}
	}
	return seeded
}

func cloneOccupied(occupied map[string]map[string]struct{}) map[string]map[string]struct{} {
	if occupied == nil {
		return nil
	}
	cloned := make(map[string]map[string]struct{}, len(occupied))
	for key, values := range occupied {
		copied := make(map[string]struct{}, len(values))
		for value := range values {
			copied[value] = struct{}{}
		}
		cloned[key] = copied
	}
	return cloned
}

func checkCommitment(tenants []Tenant, instances []Instance) error {
	guaranteed := int64(0)
	for _, tenant := range tenants {
		guaranteed += int64(tenant.Demand.GuaranteedConnections)
	}
	allocatable := int64(0)
	for _, instance := range instances {
		if instance.Ready {
			allocatable += int64(instance.Capacity.Connections - instance.Reserved.Connections)
		}
	}
	if guaranteed > allocatable {
		return fmt.Errorf("%w: %d guaranteed against %d allocatable", ErrOverCommitted, guaranteed, allocatable)
	}
	return nil
}

func orderedByDecreasingSize(tenants []Tenant, instances []Instance) []Tenant {
	largest := Capacity{}
	for _, instance := range instances {
		largest.Connections = max(largest.Connections, instance.Capacity.Connections)
		largest.StorageBytes = max(largest.StorageBytes, instance.Capacity.StorageBytes)
		largest.Relations = max(largest.Relations, instance.Capacity.Relations)
	}
	ordered := slices.Clone(tenants)
	slices.SortStableFunc(ordered, func(a, b Tenant) int {
		sizeA, sizeB := size(a, largest), size(b, largest)
		switch {
		case sizeA > sizeB:
			return -1
		case sizeB > sizeA:
			return 1
		default:
			return strings.Compare(a.Name, b.Name)
		}
	})
	return ordered
}

func assignmentOf(tenant Tenant, instance string) Assignment {
	return Assignment{
		Tenant:   tenant.Name,
		Instance: instance,
		Moved:    tenant.BoundInstance != "" && tenant.BoundInstance != instance,
		From:     tenant.BoundInstance,
	}
}

// refuse names the constraint that blocked every instance, preferring the one that blocked
// the most: a tenant refused by anti-affinity everywhere gets a different answer from one
// refused for capacity everywhere, and those have opposite remediations.
func refuse(tenant Tenant, bins []*bin) Refusal {
	if len(bins) == 0 {
		return Refusal{Tenant: tenant.Name, Reason: ReasonNoInstances,
			Message: "the pool has no instance that can accept tenants"}
	}
	counts := map[string]int{}
	messages := map[string]string{}
	for _, candidate := range bins {
		fits, reason, why := candidate.fits(tenant)
		if fits {
			continue
		}
		counts[reason]++
		messages[reason] = fmt.Sprintf("PgInstance %s %s", candidate.instance.Name, why)
	}
	reason, best := ReasonNoCapacity, -1
	for candidate, count := range counts {
		if count > best || (count == best && candidate < reason) {
			reason, best = candidate, count
		}
	}
	return Refusal{Tenant: tenant.Name, Reason: reason, Message: messages[reason]}
}

func publish(result *Result, bins []*bin) {
	for _, candidate := range bins {
		result.PerInstance[candidate.instance.Name] = candidate.used
		result.TenantsPerInstance[candidate.instance.Name] = candidate.tenants
	}
}
