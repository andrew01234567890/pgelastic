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

package placement

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// acmeTenantName is the tenant most of these fixtures are about.
const acmeTenantName = "acme"

// The instances every fixture names.
const (
	instanceA = "pg-a"
	instanceB = "pg-b"
)

func instanceOf(name string, connections int32) Instance {
	return Instance{
		Name:        name,
		Capacity:    Capacity{Connections: connections},
		Schedulable: true,
		Ready:       true,
	}
}

func tenantOf(name string, guaranteed, observed int32) Tenant {
	return Tenant{
		Name:   name,
		Demand: Demand{GuaranteedConnections: guaranteed, ObservedConnections: observed},
	}
}

func defaultPolicy() Policy {
	return Policy{PackOn: pgelasticv1alpha1.PercentileP95}
}

func instanceFor(t *testing.T, result Result, tenant string) string {
	t.Helper()
	assignment, ok := result.AssignmentFor(tenant)
	if !ok {
		refusal, _ := result.RefusalFor(tenant)
		t.Fatalf("tenant %q was not placed: %s %s", tenant, refusal.Reason, refusal.Message)
	}
	return assignment.Instance
}

// The load-bearing claim of the whole packing design: a bursty tenant is placed against the
// 95th percentile of its trailing window, and the same tenant packed on its mean lands
// somewhere that could not serve it.
func TestPacksOnThePercentileAndNotOnTheMean(t *testing.T) {
	instances := []Instance{
		{Name: "pg-roomy", Capacity: Capacity{Connections: 200}, Reserved: Capacity{Connections: 80}, Schedulable: true, Ready: true},
		{Name: "pg-tight", Capacity: Capacity{Connections: 200}, Reserved: Capacity{Connections: 180}, Schedulable: true, Ready: true},
	}

	const meanConnections, percentileConnections = 11, 107

	onPercentile, err := Pack([]Tenant{tenantOf("bursty", 0, percentileConnections)}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing on the percentile: %v", err)
	}
	onMean, err := Pack([]Tenant{tenantOf("bursty", 0, meanConnections)}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing on the mean: %v", err)
	}

	if got := instanceFor(t, onMean, "bursty"); got != "pg-tight" {
		t.Fatalf("packed on the mean the tenant lands on %q; the fixture is meant to make the mean choose the "+
			"instance with 20 connections free", got)
	}
	if got := instanceFor(t, onPercentile, "bursty"); got != "pg-roomy" {
		t.Errorf("packed on p95 the tenant lands on %q, want pg-roomy: 107 connections do not fit in the 20 "+
			"that the mean would have been happy with", got)
	}
}

func TestAnOverCommittedPoolIsRefusedBeforeAnythingIsPlaced(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 100), instanceOf(instanceB, 100)}
	tenants := []Tenant{
		tenantOf("a", 90, 90),
		tenantOf("b", 90, 90),
		tenantOf("c", 90, 90),
	}

	result, err := Pack(tenants, instances, defaultPolicy())
	if !errors.Is(err, ErrOverCommitted) {
		t.Fatalf("err = %v, want ErrOverCommitted: 270 guaranteed connections were sold against 200", err)
	}
	if len(result.Assignments) != 0 {
		t.Errorf("%d tenants were placed anyway; a packing under over-commitment breaks somebody's floor silently",
			len(result.Assignments))
	}
	if len(result.Refusals) != len(tenants) {
		t.Errorf("%d refusals for %d tenants, want one each", len(result.Refusals), len(tenants))
	}
	for _, refusal := range result.Refusals {
		if refusal.Reason != ReasonOverCommitted {
			t.Errorf("tenant %q refused with %q, want %q", refusal.Tenant, refusal.Reason, ReasonOverCommitted)
		}
	}
}

func TestOversubscribedBurstIsNotOverCommitment(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 100)}
	tenants := []Tenant{
		{Name: "a", Demand: Demand{GuaranteedConnections: 4, ObservedConnections: 20}},
		{Name: "b", Demand: Demand{GuaranteedConnections: 4, ObservedConnections: 20}},
		{Name: "c", Demand: Demand{GuaranteedConnections: 4, ObservedConnections: 20}},
	}

	result, err := Pack(tenants, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("err = %v: the guarantees fit, and selling more burst than allocatable is the product", err)
	}
	if len(result.Assignments) != 3 {
		t.Errorf("placed %d of 3 tenants", len(result.Assignments))
	}
}

func TestCorrelatedTenantsAreKeptApart(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 400), instanceOf(instanceB, 400), instanceOf("pg-c", 400)}
	shard := func(name, value string) Tenant {
		tenant := tenantOf(name, 2, 10)
		tenant.AntiAffinity = map[string]string{"saas.example.com/customer-shard": value}
		return tenant
	}
	tenants := []Tenant{
		shard("acme-eu", "acme"), shard("acme-us", "acme"), shard("acme-ap", "acme"),
		shard("globex-eu", "globex"), shard("globex-us", "globex"),
	}

	result, err := Pack(tenants, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}

	byInstance := map[string]map[string]int{}
	for _, assignment := range result.Assignments {
		shardValue := ""
		for _, tenant := range tenants {
			if tenant.Name == assignment.Tenant {
				shardValue = tenant.AntiAffinity["saas.example.com/customer-shard"]
			}
		}
		if byInstance[assignment.Instance] == nil {
			byInstance[assignment.Instance] = map[string]int{}
		}
		byInstance[assignment.Instance][shardValue]++
	}
	for instance, shards := range byInstance {
		for value, count := range shards {
			if count > 1 {
				t.Errorf("instance %q hosts %d tenants of shard %q; correlated tenants must not share an instance",
					instance, count, value)
			}
		}
	}
}

func TestAntiAffinityRefusalNamesTheConflict(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 400)}
	occupant := tenantOf("acme-eu", 2, 10)
	occupant.AntiAffinity = map[string]string{"shard": "acme"}
	newcomer := tenantOf("acme-us", 2, 10)
	newcomer.AntiAffinity = map[string]string{"shard": "acme"}

	result, err := Pack([]Tenant{occupant, newcomer}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	refusal, ok := result.RefusalFor("acme-us")
	if !ok {
		t.Fatal("the second tenant of the same shard was placed on the only instance")
	}
	if refusal.Reason != ReasonAntiAffinity {
		t.Errorf("refusal reason = %q, want %q (message %q)", refusal.Reason, ReasonAntiAffinity, refusal.Message)
	}
}

func TestAPinIsHonouredOrRefusedButNeverOverridden(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 400), instanceOf(instanceB, 400)}

	pinned := tenantOf("pinned", 4, 20)
	pinned.PinnedInstance = instanceB
	result, err := Pack([]Tenant{pinned}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	if got := instanceFor(t, result, "pinned"); got != instanceB {
		t.Errorf("pinned tenant landed on %q, want pg-b", got)
	}

	elsewhere := tenantOf("elsewhere", 4, 20)
	elsewhere.PinnedInstance = "pg-z"
	result, err = Pack([]Tenant{elsewhere}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	refusal, ok := result.RefusalFor("elsewhere")
	if !ok {
		t.Fatal("a tenant pinned to an instance outside the pool was placed somewhere else")
	}
	if refusal.Reason != ReasonPinnedUnavailable {
		t.Errorf("refusal reason = %q, want %q", refusal.Reason, ReasonPinnedUnavailable)
	}
}

func TestAnInstanceThatCannotServeIsNotCapacity(t *testing.T) {
	notReady := instanceOf("pg-recloning", 400)
	notReady.Ready = false
	cordoned := instanceOf("pg-draining", 400)
	cordoned.Schedulable = false

	result, err := Pack([]Tenant{tenantOf("acme", 4, 20)}, []Instance{notReady, cordoned}, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	if _, ok := result.AssignmentFor("acme"); ok {
		t.Fatal("a tenant was placed on an instance that cannot serve it")
	}
	refusal, _ := result.RefusalFor("acme")
	if refusal.Reason != ReasonInstanceUnavailable {
		t.Errorf("refusal reason = %q, want %q (message %q)", refusal.Reason, ReasonInstanceUnavailable, refusal.Message)
	}
}

func TestBestFitFillsAnInstanceRatherThanSpreadingThin(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 100), instanceOf(instanceB, 100), instanceOf("pg-c", 100)}
	tenants := []Tenant{
		tenantOf("big", 0, 60),
		tenantOf("medium", 0, 30),
		tenantOf("small-1", 0, 5),
		tenantOf("small-2", 0, 5),
	}

	result, err := Pack(tenants, instances, Policy{PackOn: pgelasticv1alpha1.PercentileP95})
	if err != nil {
		t.Fatalf("packing: %v", err)
	}

	empty := 0
	for _, used := range result.PerInstance {
		if used.Connections == 0 {
			empty++
		}
	}
	if empty < 2 {
		t.Errorf("packing left %d of 3 instances empty; best-fit exists so that a scale-in has whole instances "+
			"to reclaim (per-instance load %v)", empty, result.PerInstance)
	}
}

func TestSkewBoundStopsOneInstanceHoldingEverything(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 1000), instanceOf(instanceB, 1000)}
	tenants := make([]Tenant, 0, 20)
	for i := range 20 {
		tenants = append(tenants, tenantOf(fmt.Sprintf("t-%02d", i), 1, 1))
	}

	result, err := Pack(tenants, instances, Policy{PackOn: pgelasticv1alpha1.PercentileP95, MaxSkewTenants: 4})
	if err != nil {
		t.Fatalf("packing: %v", err)
	}

	lowest, highest := int32(1<<30), int32(0)
	for _, count := range result.TenantsPerInstance {
		lowest = min(lowest, count)
		highest = max(highest, count)
	}
	if highest-lowest > 4 {
		t.Errorf("tenant counts %v skew by %d, want no more than the configured 4",
			result.TenantsPerInstance, highest-lowest)
	}
}

// The skew bound has to hold for the incremental admission path too, and that is the harder
// case: each admission reads a ledger no previous admission has written to.
func TestSkewBoundHoldsAcrossPowerOfTwoChoicesAdmissions(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 1000), instanceOf(instanceB, 1000), instanceOf("pg-c", 1000)}
	policy := Policy{PackOn: pgelasticv1alpha1.PercentileP95, MaxSkewTenants: 2}
	rng := rand.New(rand.NewPCG(3, 5))

	counts := map[string]int32{}
	for i := range 30 {
		bins, _ := binsOf(instances)
		for _, candidate := range bins {
			candidate.tenants = counts[candidate.instance.Name]
		}
		tenant := tenantOf(fmt.Sprintf("t-%02d", i), 1, 1)
		chosen := feasible(tenant, bins, policy)
		if len(chosen) == 0 {
			t.Fatalf("tenant %d had no feasible instance", i)
		}
		counts[chosen[pick(rng, len(chosen))].instance.Name]++
	}

	lowest, highest := int32(1<<30), int32(0)
	for _, count := range counts {
		lowest = min(lowest, count)
		highest = max(highest, count)
	}
	if highest-lowest > policy.MaxSkewTenants {
		t.Errorf("tenant counts %v skew by %d, want no more than %d",
			counts, highest-lowest, policy.MaxSkewTenants)
	}
}

func TestPowerOfTwoChoicesDoesNotHerdConcurrentAdmissions(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 400), instanceOf(instanceB, 400), instanceOf("pg-c", 400)}
	policy := defaultPolicy()

	// Every admission reads the same ledger, which is what happens when several tenants are
	// created between two reconciles: nothing any of them does is visible to the others.
	chosen := map[string]int{}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 30 {
		assignment, refusal := Admit(tenantOf(fmt.Sprintf("t-%02d", i), 2, 10), instances, policy, rng)
		if refusal != nil {
			t.Fatalf("tenant %d refused: %s", i, refusal.Message)
		}
		chosen[assignment.Instance]++
	}

	if len(chosen) < 2 {
		t.Errorf("30 admissions against an unchanged ledger all landed on one instance (%v); "+
			"that is the herding power-of-two-choices exists to break", chosen)
	}
	for instance, count := range chosen {
		if count == 30 {
			t.Errorf("instance %q took every admission", instance)
		}
	}
}

func TestAdmitPrefersTheInstanceThatCanActuallyHoldTheTenant(t *testing.T) {
	instances := []Instance{
		{Name: "pg-full", Capacity: Capacity{Connections: 100}, Reserved: Capacity{Connections: 98}, Schedulable: true, Ready: true},
		{Name: "pg-free", Capacity: Capacity{Connections: 100}, Schedulable: true, Ready: true},
	}
	rng := rand.New(rand.NewPCG(7, 11))

	for range 20 {
		assignment, refusal := Admit(tenantOf("acme", 10, 40), instances, defaultPolicy(), rng)
		if refusal != nil {
			t.Fatalf("refused: %s", refusal.Message)
		}
		if assignment.Instance != "pg-free" {
			t.Fatalf("admitted onto %q, which has 2 connections free for a tenant that needs 40",
				assignment.Instance)
		}
	}
}

func TestAdmitRefusesWhenNothingFits(t *testing.T) {
	instances := []Instance{
		{Name: instanceA, Capacity: Capacity{Connections: 100}, Reserved: Capacity{Connections: 100}, Schedulable: true, Ready: true},
	}
	_, refusal := Admit(tenantOf("acme", 10, 40), instances, defaultPolicy(), rand.New(rand.NewPCG(1, 1)))
	if refusal == nil {
		t.Fatal("a tenant was admitted onto a full instance")
	}
	if refusal.Reason != ReasonNoCapacity {
		t.Errorf("refusal reason = %q, want %q", refusal.Reason, ReasonNoCapacity)
	}
}

func TestAdmitRefusesWhenThePoolHasNoInstances(t *testing.T) {
	_, refusal := Admit(tenantOf("acme", 1, 1), nil, defaultPolicy(), rand.New(rand.NewPCG(1, 1)))
	if refusal == nil {
		t.Fatal("a tenant was admitted into a pool with no instances")
	}
	if refusal.Reason != ReasonNoInstances {
		t.Errorf("refusal reason = %q, want %q", refusal.Reason, ReasonNoInstances)
	}
}

func TestAlreadyBoundTenantsStayPutWhenTheAlternativesAreEqual(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 400), instanceOf(instanceB, 400)}
	tenant := tenantOf("acme", 4, 20)
	tenant.BoundInstance = instanceB

	result, err := Pack([]Tenant{tenant}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	assignment, _ := result.AssignmentFor("acme")
	if assignment.Instance != instanceB {
		t.Errorf("tenant moved to %q for no gain; every move is a live migration", assignment.Instance)
	}
	if assignment.Moved {
		t.Error("assignment reports a move that did not happen")
	}
}

func TestMovesReportWhereTheTenantIsLeaving(t *testing.T) {
	instances := []Instance{
		{Name: instanceA, Capacity: Capacity{Connections: 100}, Reserved: Capacity{Connections: 95}, Schedulable: true, Ready: true},
		instanceOf(instanceB, 100),
	}
	tenant := tenantOf("acme", 0, 40)
	tenant.BoundInstance = instanceA

	result, err := Pack([]Tenant{tenant}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	moves := result.Moves()
	if len(moves) != 1 {
		t.Fatalf("%d moves, want 1", len(moves))
	}
	if moves[0].From != instanceA || moves[0].Instance != instanceB {
		t.Errorf("move is %s -> %s, want pg-a -> pg-b", moves[0].From, moves[0].Instance)
	}
}

func TestStorageAndRelationsAreHardDimensionsToo(t *testing.T) {
	instances := []Instance{{
		Name:        instanceA,
		Capacity:    Capacity{Connections: 400, StorageBytes: 100 << 30, Relations: 5000},
		Schedulable: true,
		Ready:       true,
	}}

	tooBig := Tenant{Name: "hoarder", Demand: Demand{ObservedConnections: 1, StorageBytes: 200 << 30}}
	result, err := Pack([]Tenant{tooBig}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	if _, ok := result.RefusalFor("hoarder"); !ok {
		t.Error("a tenant needing 200GiB was placed on an instance with 100GiB")
	}

	tooMany := Tenant{Name: "catalogue", Demand: Demand{ObservedConnections: 1, Relations: 9000}}
	result, err = Pack([]Tenant{tooMany}, instances, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	if _, ok := result.RefusalFor("catalogue"); !ok {
		t.Error("a tenant with 9000 relations was placed on an instance rated for 5000")
	}
}

func TestSeedBinsWithBoundTenantsChargesTheCapacityTheyAlreadyHold(t *testing.T) {
	instances := []Instance{instanceOf(instanceA, 100)}
	bound := []Tenant{{Name: "sitting", Demand: Demand{ObservedConnections: 90}, BoundInstance: instanceA}}

	seeded := SeedBinsWithBoundTenants(instances, bound)
	result, err := Pack([]Tenant{tenantOf("newcomer", 0, 40)}, seeded, defaultPolicy())
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	if _, ok := result.RefusalFor("newcomer"); !ok {
		t.Error("a newcomer needing 40 connections was placed alongside a tenant already holding 90 of 100")
	}
	if instances[0].Reserved.Connections != 0 {
		t.Error("SeedBinsWithBoundTenants mutated its input")
	}
}

func TestQuantileForMapsEveryPercentileTheAPIAccepts(t *testing.T) {
	for percentile, want := range map[pgelasticv1alpha1.Percentile]float64{
		pgelasticv1alpha1.PercentileP50:  0.50,
		pgelasticv1alpha1.PercentileP75:  0.75,
		pgelasticv1alpha1.PercentileP90:  0.90,
		pgelasticv1alpha1.PercentileP95:  0.95,
		pgelasticv1alpha1.PercentileP99:  0.99,
		pgelasticv1alpha1.Percentile(""): 0.95,
	} {
		if got := QuantileFor(percentile); got != want {
			t.Errorf("QuantileFor(%q) = %v, want %v", percentile, got, want)
		}
	}
}

// Widening the version enum makes a mixed-major pool constructible, and the packer was blind
// to it. Both of this tree's dumps run in the *target's* container, so pg_dump cannot read a
// server newer than itself: a move onto an older major is refused at preflight, permanently
// and by construction. A packer that proposes those moves manufactures migrations that can
// never succeed - and each one then sits at the gate.
func TestAMoveIsNotProposedOntoAMajorTheMigrationCannotReach(t *testing.T) {
	tenant := Tenant{
		Name:          acmeTenantName,
		Demand:        Demand{GuaranteedConnections: 10},
		BoundInstance: "pg-19",
		BoundMajor:    19,
	}
	// pg-18 is the tightest fit and pg-21 the next, so a packer judging only on capacity
	// picks one of the two destinations the migration can never reach. That is deliberate:
	// with the version rule removed this test has to fail, and a fixture where best-fit
	// happens to choose a legal instance anyway proves nothing.
	instances := []Instance{
		{Name: "pg-18", Capacity: Capacity{Connections: 10}, Schedulable: true, Ready: true, Major: 18},
		{Name: "pg-21", Capacity: Capacity{Connections: 12}, Schedulable: true, Ready: true, Major: 21},
		{Name: "pg-19", Capacity: Capacity{Connections: 400}, Schedulable: true, Ready: true, Major: 19},
		{Name: "pg-20", Capacity: Capacity{Connections: 500}, Schedulable: true, Ready: true, Major: 20},
	}

	result, err := Pack([]Tenant{tenant}, instances, Policy{})
	if err != nil {
		t.Fatalf("packing: %v", err)
	}
	assignment, ok := result.AssignmentFor(acmeTenantName)
	if !ok {
		t.Fatal("a tenant with three reachable destinations was refused outright")
	}
	switch assignment.Instance {
	case "pg-18":
		t.Error("packed a tenant from 19 onto 18; the dump runs in the target and cannot read a newer server")
	case "pg-21":
		t.Error("packed a tenant two majors forward; majors are crossed one at a time")
	}

	// A tenant bound nowhere carries no floor, so a first admission is unaffected.
	fresh, err := Pack([]Tenant{{Name: "new", Demand: Demand{GuaranteedConnections: 10}}}, instances, Policy{})
	if err != nil {
		t.Fatalf("packing a new tenant: %v", err)
	}
	if _, ok := fresh.AssignmentFor("new"); !ok {
		t.Error("a tenant with no binding was refused on a version axis it has no position on")
	}
}

// The skew reference used to be the emptiest of ALL bins rather than of the ones a tenant may
// actually land on. A member that is not ready holds no tenants and cannot take any, so it sat
// at zero and dragged the reference there - which turns a relative bound into an absolute cap
// of MaxSkewTenants per instance. Every real candidate then fails it, the fallback hands back
// the whole list, and the bound that exists to spread tenants applies to nothing.
func TestTheSkewReferenceIgnoresInstancesNothingMayLandOn(t *testing.T) {
	unready := instanceOf("pg-draining", 1000)
	unready.Ready = false
	instances := []Instance{
		instanceOf(instanceA, 1000), instanceOf(instanceB, 1000), instanceOf("pg-c", 1000),
		unready,
	}

	policy := Policy{PackOn: pgelasticv1alpha1.PercentileP95, MaxSkewTenants: 1}
	bins, _ := binsOf(instances)
	for _, candidate := range bins {
		switch candidate.instance.Name {
		case instanceA:
			candidate.tenants = 5
		case instanceB:
			candidate.tenants = 5
		case "pg-c":
			// Far enough ahead that the bound must exclude it, which is the whole
			// discrimination: with a vacuous bound the fallback hands back all three.
			candidate.tenants = 9
		default:
			candidate.tenants = 0
		}
	}

	chosen := feasible(tenantOf("newcomer", 1, 1), bins, policy)

	for _, candidate := range chosen {
		if candidate.instance.Name == "pg-draining" {
			t.Fatal("a tenant was offered an instance that is not ready")
		}
	}
	if len(chosen) != 2 {
		t.Fatalf("the skew bound admitted %d of 2 level candidates; an unplaceable member at "+
			"zero tenants made the bound vacuous", len(chosen))
	}
}
