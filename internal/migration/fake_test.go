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

package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// fakeSQL answers by matching the longest configured fragment against the statement, and
// records everything it was asked in order, so a test can assert on the order the cleanup
// ladder ran in as well as on what it returned.
type fakeSQL struct {
	mutex     sync.Mutex
	answers   map[string][]Row
	failures  map[string]error
	statement []string
	endpoints []Endpoint
}

func newFakeSQL() *fakeSQL {
	return &fakeSQL{answers: map[string][]Row{}, failures: map[string]error{}}
}

func (f *fakeSQL) answer(fragment string, rows ...Row) *fakeSQL {
	f.answers[fragment] = rows
	return f
}

func (f *fakeSQL) scalarAnswer(fragment, value string) *fakeSQL {
	return f.answer(fragment, Row{value})
}

func (f *fakeSQL) fail(fragment string, err error) *fakeSQL {
	f.failures[fragment] = err
	return f
}

func (f *fakeSQL) Exec(_ context.Context, at Endpoint, statement string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.statement = append(f.statement, statement)
	f.endpoints = append(f.endpoints, at)
	if err := f.matchFailure(statement); err != nil {
		return err
	}
	return nil
}

func (f *fakeSQL) Query(_ context.Context, at Endpoint, statement string) ([]Row, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.statement = append(f.statement, statement)
	f.endpoints = append(f.endpoints, at)
	if err := f.matchFailure(statement); err != nil {
		return nil, err
	}
	best, found := "", false
	for fragment := range f.answers {
		if strings.Contains(statement, fragment) && len(fragment) > len(best) {
			best, found = fragment, true
		}
	}
	if !found {
		return nil, fmt.Errorf("fakeSQL has no answer for %q", statement)
	}
	return f.answers[best], nil
}

func (f *fakeSQL) matchFailure(statement string) error {
	for fragment, err := range f.failures {
		if strings.Contains(statement, fragment) {
			return err
		}
	}
	return nil
}

// ran reports the index of the first statement containing a fragment, or -1.
func (f *fakeSQL) ran(fragment string) int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for index, statement := range f.statement {
		if strings.Contains(statement, fragment) {
			return index
		}
	}
	return -1
}

func (f *fakeSQL) sawAll(fragments ...string) error {
	for _, fragment := range fragments {
		if f.ran(fragment) < 0 {
			return fmt.Errorf("no statement contained %q", fragment)
		}
	}
	return nil
}

// fakeShell records the argv it was handed and can be made to fail.
type fakeShell struct {
	commands [][]string
	err      error
}

func (s *fakeShell) Run(_ context.Context, _ Endpoint, argv []string) ([]byte, error) {
	s.commands = append(s.commands, argv)
	return nil, s.err
}

func (s *fakeShell) joined() string {
	parts := make([]string, 0, len(s.commands))
	for _, command := range s.commands {
		parts = append(parts, strings.Join(command, " "))
	}
	return strings.Join(parts, "\n")
}

// fakeRouter is the proxy stand-in. It records the routing table so a test can assert the
// tenant ended up on the instance it started on.
type fakeRouter struct {
	routed    string
	quiesced  bool
	preWarmed string
	released  bool
	resumed   bool
	gate      DrainStatus
	err       error
}

func (r *fakeRouter) Quiesce(_ context.Context, _ TenantRef, _ string) error {
	r.quiesced = true
	return r.err
}

func (r *fakeRouter) PreWarm(_ context.Context, _ TenantRef, instance string) error {
	r.preWarmed = instance
	return r.err
}

func (r *fakeRouter) Route(_ context.Context, _ TenantRef, instance string) error {
	if r.err != nil {
		return r.err
	}
	r.routed = instance
	return nil
}

func (r *fakeRouter) Release(_ context.Context, _ TenantRef) error {
	r.released = true
	r.quiesced = false
	return r.err
}

func (r *fakeRouter) Resume(_ context.Context, _ TenantRef) error {
	r.resumed = true
	r.released = true
	r.quiesced = false
	return r.err
}

func (r *fakeRouter) RoutedTo(_ context.Context, _ TenantRef) (string, error) {
	return r.routed, r.err
}

func (r *fakeRouter) DrainStatus(_ context.Context, _ TenantRef) (DrainStatus, error) {
	return r.gate, r.err
}

var errFake = errors.New("the source went away")

// Names shared by the fakes and the specs. They are constants so a rename cannot leave one
// spec asserting about an instance no other spec is talking about.
const (
	namespaceName  = "shop"
	sourceInstance = "pg-a"
	targetInstance = "pg-b"
	sourceStandby  = "pg-a-2"
	secondStandby  = "pg-a-3"
	tenantDatabase = "acme"
	userSchema     = "public"
	ordersRelation = "orders"
	ordersSequence = "orders_id_seq"
	firstSequence  = "a_seq"
	offendingTable = "public.events"
	liveSlotName   = "pgelastic_mig_live_00000000"
)
