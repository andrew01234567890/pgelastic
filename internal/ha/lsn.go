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

// Package ha holds the failover decision logic: candidate selection, the quorum gate, the
// four named veto conditions, the lease invariants and the two-phase sentinel state
// machine.
//
// Nothing in this package talks to Kubernetes or to PostgreSQL. Every function is a pure
// function of an observation, which is what makes the safety-critical decisions testable
// without a cluster: a promotion that cannot be reproduced in a unit test is a promotion
// nobody can reason about at three in the morning.
package ha

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a write-ahead log position, held as the single 64-bit number PostgreSQL's
// pg_lsn already is.
//
// It is parsed rather than compared as text because the textual form is not
// lexicographically ordered: "10/0" is ahead of "9/FFFFFFFF" and sorts before it. Comparing
// LSNs as strings is the kind of mistake that silently promotes the more behind of two
// candidates.
type LSN uint64

// ParseLSN reads PostgreSQL's "XXXXXXXX/XXXXXXXX" form.
//
// An empty string is position zero rather than an error: a member that has not reported a
// position yet is behind every member that has, which is exactly the ordering an unset
// value should produce.
func ParseLSN(value string) (LSN, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	high, low, found := strings.Cut(value, "/")
	if !found {
		return 0, fmt.Errorf("%q is not a log sequence number", value)
	}
	upper, err := strconv.ParseUint(high, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q has an unreadable segment: %w", value, err)
	}
	lower, err := strconv.ParseUint(low, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q has an unreadable offset: %w", value, err)
	}
	return LSN(upper<<32 | lower), nil
}

// MustParseLSN is ParseLSN with an unreadable value treated as position zero.
//
// Candidate selection uses it because an LSN the operator cannot read is not a reason to
// abandon the decision: it is a reason to rank that member last, behind every member whose
// position is known. Refusing to sort at all would deny a failover that the quorum gate has
// already proven safe.
func MustParseLSN(value string) LSN {
	parsed, err := ParseLSN(value)
	if err != nil {
		return 0
	}
	return parsed
}

// String renders the LSN back into PostgreSQL's form.
func (l LSN) String() string {
	return fmt.Sprintf("%X/%08X", uint64(l)>>32, uint64(l)&0xFFFFFFFF)
}
