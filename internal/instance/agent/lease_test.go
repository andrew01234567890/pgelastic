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

package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/andrew01234567890/pgelastic/internal/ha"
)

const (
	leaseNamespace = "pgelastic-lease"
	leaseName      = "pg"
	holderOne      = "pg-1"
	holderTwo      = "pg-2"
)

// briefLease keeps the real durations' relationships while making the waits testable: the
// take-over rule is about elapsed time on the observer's clock, not about fifteen seconds
// in particular.
func briefLease() ha.LeaseConfig {
	return ha.LeaseConfig{
		LeaseDuration:         150 * time.Millisecond,
		RenewDeadline:         100 * time.Millisecond,
		RetryPeriod:           20 * time.Millisecond,
		ReleasedLeaseDuration: 10 * time.Millisecond,
	}
}

// managerFor builds the manager under test. It is always holderOne: every spec here is
// about what holderOne may do to a lease somebody else is, or is not, still renewing.
func managerFor(built client.Client) *LeaseManager {
	return &LeaseManager{
		Client:    built,
		Namespace: leaseNamespace,
		Name:      leaseName,
		Holder:    holderOne,
		Config:    briefLease(),
	}
}

func fakeClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).Build()
}

func existingLease(holder string, renewedAgo time.Duration, validity time.Duration) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: leaseNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr.To(holder),
			LeaseDurationSeconds: ptr.To(int32(validity.Seconds())),
			RenewTime:            ptr.To(metav1.NewMicroTime(time.Now().Add(-renewedAgo))),
			LeaseTransitions:     ptr.To(int32(2)),
		},
	}
}

func TestAcquiringAnAbsentLeaseCreatesIt(t *testing.T) {
	manager := managerFor(fakeClient())

	lease, err := manager.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if *lease.Spec.HolderIdentity != holderOne {
		t.Fatalf("holderIdentity was %q", *lease.Spec.HolderIdentity)
	}
	if LeaderTransitions(lease) != 0 {
		t.Fatalf("the first holder is not a transition, got %d", LeaderTransitions(lease))
	}
}

func TestAcquiringGivesUpWhileAnotherMemberKeepsRenewing(t *testing.T) {
	built := fakeClient(existingLease(holderTwo, 0, time.Hour))
	manager := managerFor(built)

	ctx, cancel := context.WithTimeout(context.Background(), manager.Config.AcquireTimeout())
	defer cancel()
	if _, err := manager.Acquire(ctx); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected the acquisition to give up, got %v", err)
	}

	held := &coordinationv1.Lease{}
	if err := built.Get(context.Background(), client.ObjectKey{
		Namespace: leaseNamespace, Name: leaseName}, held); err != nil {
		t.Fatal(err)
	}
	if *held.Spec.HolderIdentity != holderTwo {
		t.Fatalf("a lease with a live holder was taken anyway: %q", *held.Spec.HolderIdentity)
	}
}

func TestAcquiringSucceedsOnceTheHolderStopsRenewing(t *testing.T) {
	// The holder's RenewTime never changes for the whole of this test, which is the only
	// evidence the take-over rule accepts: the elapsed time is measured on the acquirer's
	// own clock, and the holder's timestamp is compared for equality alone.
	manager := managerFor(fakeClient(existingLease(holderTwo, 0, briefLease().LeaseDuration)))

	ctx, cancel := context.WithTimeout(context.Background(), manager.Config.AcquireTimeout())
	defer cancel()
	lease, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if *lease.Spec.HolderIdentity != holderOne {
		t.Fatalf("holderIdentity was %q", *lease.Spec.HolderIdentity)
	}
	if LeaderTransitions(lease) != 3 {
		t.Fatalf("a take-over must advance the transition counter, got %d", LeaderTransitions(lease))
	}
}

func TestTheEpochFollowsTheTransitionCounter(t *testing.T) {
	manager := managerFor(fakeClient(existingLease(holderTwo, 0, briefLease().LeaseDuration)))

	ctx, cancel := context.WithTimeout(context.Background(), manager.Config.AcquireTimeout())
	defer cancel()
	lease, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if epoch := ha.Epoch(ha.InitialPrimaryEpoch, LeaderTransitions(lease)); epoch != 4 {
		t.Fatalf("epoch was %d, want 4", epoch)
	}
}

func TestRenewingALeaseHeldElsewhereIsTerminal(t *testing.T) {
	manager := managerFor(fakeClient(existingLease(holderTwo, 0, time.Hour)))

	outcome := manager.Renew(context.Background())

	if outcome != ha.RenewLost {
		t.Fatalf("outcome was %s", outcome)
	}
	if !outcome.Terminal() {
		t.Fatal("a lease demonstrably held by somebody else must stop the postmaster")
	}
}

func TestRenewingOurOwnLeaseSucceeds(t *testing.T) {
	manager := managerFor(fakeClient(existingLease(holderOne, time.Second, time.Hour)))

	if outcome := manager.Renew(context.Background()); outcome != ha.RenewOK {
		t.Fatalf("outcome was %s", outcome)
	}
}

// unreachableClient is an API server that does not answer, which is a different thing from
// an API server that answers and names somebody else.
type unreachableClient struct {
	client.Client
}

func (unreachableClient) Get(
	context.Context, client.ObjectKey, client.Object, ...client.GetOption,
) error {
	return errors.New("the API server did not answer")
}

func (unreachableClient) Scheme() *runtime.Scheme { return scheme.Scheme }

func TestAnUnreachableAPIServerDoesNotFenceTheNode(t *testing.T) {
	manager := managerFor(unreachableClient{})

	outcome := manager.Renew(context.Background())

	if outcome != ha.RenewUnverified {
		t.Fatalf("outcome was %s", outcome)
	}
	if outcome.Terminal() {
		t.Fatal("failing to verify the lease is not losing it; the isolation probe fences a lone node")
	}
}

func TestReleasingStampsTheShortValidity(t *testing.T) {
	built := fakeClient(existingLease(holderOne, 0, time.Hour))
	manager := managerFor(built)

	if err := manager.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	released := &coordinationv1.Lease{}
	if err := built.Get(context.Background(), client.ObjectKey{
		Namespace: leaseNamespace, Name: leaseName}, released); err != nil {
		t.Fatal(err)
	}
	if *released.Spec.HolderIdentity != "" {
		t.Fatalf("holderIdentity was %q", *released.Spec.HolderIdentity)
	}
	if *released.Spec.LeaseDurationSeconds != int32(manager.Config.ReleasedLeaseDuration.Seconds()) {
		t.Fatalf("a released lease must not cost a successor a full leaseDuration, got %ds",
			*released.Spec.LeaseDurationSeconds)
	}
}

func TestReleasingALeaseWeDoNotHoldChangesNothing(t *testing.T) {
	built := fakeClient(existingLease(holderTwo, 0, time.Hour))
	manager := managerFor(built)

	if err := manager.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	held := &coordinationv1.Lease{}
	if err := built.Get(context.Background(), client.ObjectKey{
		Namespace: leaseNamespace, Name: leaseName}, held); err != nil {
		t.Fatal(err)
	}
	if *held.Spec.HolderIdentity != holderTwo {
		t.Fatalf("holderIdentity was %q", *held.Spec.HolderIdentity)
	}
}
