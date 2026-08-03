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

package pgconf

import (
	"fmt"
	"maps"
	"slices"
)

const (
	// SuperuserReservedConnections is the slice of max_connections kept for superusers.
	SuperuserReservedConnections int32 = 3
	// ReservedConnections is the slice kept for roles holding pg_use_reserved_connections,
	// which is how the control plane keeps a way in while tenants are saturating the
	// instance.
	ReservedConnections int32 = 5

	// BaseAgentConnections covers the in-pod instance manager, the metrics exporter and
	// the physical-backup shim. It is the floor of the 10..16 agent-overhead band.
	BaseAgentConnections int32 = 10
	// MaxAgentOverhead is the ceiling of that band. Per-tenant logical dumps push the
	// overhead up towards it, and the cap is what stops a large maxConcurrentDumps
	// silently eating capacity that has already been sold to tenants.
	MaxAgentOverhead int32 = 16
)

// Capacity is the max_connections split. Allocatable is the only part tenants may draw
// on, and it is the number the pool's reservation ledger sums.
type Capacity struct {
	// Allocatable is A: what tenants may draw on.
	Allocatable int32
	// SuperuserReserved is superuser_reserved_connections.
	SuperuserReserved int32
	// Reserved is reserved_connections.
	Reserved int32
	// AgentOverhead is what the in-pod agents hold outside every tenant's budget.
	AgentOverhead int32
	// MaxConnections is the sum, and the value PostgreSQL boots with.
	MaxConnections int32
	// ReplicationSlots is max_replication_slots. It is published for the pool's ledger
	// but is deliberately not an addend: walsenders are governed by max_wal_senders, not
	// by max_connections.
	ReplicationSlots int32
	// WALSenders is max_wal_senders.
	WALSenders int32
}

// AgentOverhead derives the agent reserve from the concurrent logical-dump budget. Every
// open dump holds a connection and pins the instance-wide xmin horizon, so it is charged
// to a reserve held outside every tenant's guarantee rather than to allocatable capacity.
func AgentOverhead(concurrentDumps int32) int32 {
	if concurrentDumps < 0 {
		concurrentDumps = 0
	}
	return min(BaseAgentConnections+concurrentDumps, MaxAgentOverhead)
}

// DeriveCapacity computes the max_connections split for an instance rated at allocatable
// tenant connections.
func DeriveCapacity(allocatable, concurrentDumps, replicas, migrationSlots int32) Capacity {
	allocatable = max(allocatable, 0)
	standbys := max(replicas-1, 0)
	migrationSlots = max(migrationSlots, 0)

	capacity := Capacity{
		Allocatable:       allocatable,
		SuperuserReserved: SuperuserReservedConnections,
		Reserved:          ReservedConnections,
		AgentOverhead:     AgentOverhead(concurrentDumps),
		WALSenders:        standbys + migrationSlots,
		ReplicationSlots:  standbys + migrationSlots,
	}
	capacity.MaxConnections = capacity.Allocatable +
		capacity.SuperuserReserved +
		capacity.Reserved +
		capacity.AgentOverhead
	return capacity
}

// SizingClass is one rated instance shape. The rating is expressed in tenant-usable
// connections rather than in max_connections, because that is the number the capacity
// model, admission and chargeback all quote.
type SizingClass struct {
	// Name is the class name carried in PgInstance.spec.class.
	Name string
	// AllocatableConnections is A for this class.
	AllocatableConnections int32
	// RatedCPUMillis and RatedMemoryBytes are the shape the connection rating assumes.
	//
	// They are not a request: the Pod's resources come from spec.resources, and these are
	// what the parameter derivation falls back to when it has nothing else to read. Without
	// them a gp-32 rated at 1200 connections and a dev-1 rated at 50 get byte-identical
	// PostgreSQL memory settings, which is what happens today because nothing in the tree
	// sets spec.resources at all.
	RatedCPUMillis   int64
	RatedMemoryBytes int64
}

const (
	gibibyte = int64(1) << 30
	// memoryPerVCPU is the ratio the gp- ratings are built on. It is a ratification of the
	// naming convention rather than a derivation from it: gp-N tracks N vCPU and the
	// connection counts follow ~25/vCPU up to gp-16. gp-32 breaks that pattern - 1200 rather
	// than 1600 - which reads as a deliberate density ceiling nobody wrote down, so its
	// rating follows the name and not the connection count.
	memoryPerVCPU = 4 * gibibyte
)

var sizingClasses = map[string]SizingClass{
	// dev-1 exists for kind and for CI: small enough that three postmasters fit on one
	// node, large enough that the derivation is still exercised end to end.
	"dev-1": {Name: "dev-1", AllocatableConnections: 50,
		RatedCPUMillis: 1000, RatedMemoryBytes: 2 * gibibyte},
	"gp-2": {Name: "gp-2", AllocatableConnections: 100,
		RatedCPUMillis: 2000, RatedMemoryBytes: 2 * memoryPerVCPU},
	"gp-4": {Name: "gp-4", AllocatableConnections: 200,
		RatedCPUMillis: 4000, RatedMemoryBytes: 4 * memoryPerVCPU},
	"gp-8": {Name: "gp-8", AllocatableConnections: 400,
		RatedCPUMillis: 8000, RatedMemoryBytes: 8 * memoryPerVCPU},
	"gp-16": {Name: "gp-16", AllocatableConnections: 800,
		RatedCPUMillis: 16000, RatedMemoryBytes: 16 * memoryPerVCPU},
	"gp-32": {Name: "gp-32", AllocatableConnections: 1200,
		RatedCPUMillis: 32000, RatedMemoryBytes: 32 * memoryPerVCPU},
}

// LookupSizingClass resolves a class name. An unknown class is an error rather than a
// fallback: silently provisioning a different size than was asked for would leave the
// pool's budget promising connections the instance cannot serve.
func LookupSizingClass(name string) (SizingClass, error) {
	class, ok := sizingClasses[name]
	if !ok {
		return SizingClass{}, fmt.Errorf(
			"unknown instance class %q; known classes are %v", name, SizingClassNames())
	}
	return class, nil
}

// SizingClassNames lists every known class in sorted order.
func SizingClassNames() []string {
	return slices.Sorted(maps.Keys(sizingClasses))
}
