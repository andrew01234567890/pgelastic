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
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/andrew01234567890/pgelastic/internal/ha"
)

// LeaseManager holds the promotion Lease for one member.
//
// The lease is held by the in-pod agent rather than by the operator, and that is the whole
// point: a dead operator then cannot cause an unnecessary failover, because nothing outside
// the pod can take the lease away from a primary that is still renewing it. The cost is
// that a dead operator means no failover happens either, which is fail-stop rather than
// fail-dangerous and is the right trade.
type LeaseManager struct {
	// Client is the agent's own API server connection.
	Client client.Client
	// Namespace and Name address the Lease, which is named after the PgInstance.
	Namespace string
	Name      string
	// Holder is this pod's name, written as holderIdentity.
	Holder string
	// Config carries the four durations.
	Config ha.LeaseConfig
}

// ErrLeaseHeld is returned when an acquisition attempt ran out of time with somebody else
// still renewing. The caller requeues after one retryPeriod rather than blocking, so a
// lease that is legitimately held does not pin a worker for a whole leaseDuration.
var ErrLeaseHeld = errors.New("the promotion lease is held by another member")

func (m *LeaseManager) key() types.NamespacedName {
	return types.NamespacedName{Namespace: m.Namespace, Name: m.Name}
}

// Snapshot reads the Lease and pairs it with the reader's own clock.
func (m *LeaseManager) Snapshot(ctx context.Context) (ha.LeaseSnapshot, error) {
	lease := &coordinationv1.Lease{}
	if err := m.Client.Get(ctx, m.key(), lease); err != nil {
		if apierrors.IsNotFound(err) {
			return ha.LeaseSnapshot{ObservedAt: time.Now()}, nil
		}
		return ha.LeaseSnapshot{}, err
	}
	return snapshotOf(lease), nil
}

func snapshotOf(lease *coordinationv1.Lease) ha.LeaseSnapshot {
	snapshot := ha.LeaseSnapshot{ObservedAt: time.Now()}
	if lease.Spec.HolderIdentity != nil {
		snapshot.Holder = *lease.Spec.HolderIdentity
	}
	if lease.Spec.RenewTime != nil {
		snapshot.RenewTime = lease.Spec.RenewTime.Time
	}
	if lease.Spec.LeaseDurationSeconds != nil {
		snapshot.LeaseDuration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return snapshot
}

// LeaderTransitions is the Lease's own transition counter, which is what the fence epoch is
// derived from.
func LeaderTransitions(lease *coordinationv1.Lease) int32 {
	if lease.Spec.LeaseTransitions == nil {
		return 0
	}
	return *lease.Spec.LeaseTransitions
}

// Acquire takes the Lease, waiting out a holder that has stopped renewing.
//
// The context must already carry the per-attempt deadline (LeaseConfig.AcquireTimeout).
// Running out of it is ErrLeaseHeld rather than a failure: somebody else is the primary and
// the right response is to requeue, not to force anything.
func (m *LeaseManager) Acquire(ctx context.Context) (*coordinationv1.Lease, error) {
	first, err := m.Snapshot(ctx)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(m.Config.RetryPeriod)
	defer ticker.Stop()
	for {
		lease, verdict, err := m.attempt(ctx, first)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			return lease, nil
		}
		if verdict.Reason == ha.TakeOverHolderAlive {
			// A holder that is demonstrably alive resets the observation window: the
			// elapsed time that matters is time since the *last* renewal we saw, not time
			// since we started watching.
			if first, err = m.Snapshot(ctx); err != nil {
				return nil, err
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %s", ErrLeaseHeld, verdict.Reason)
		case <-ticker.C:
		}
	}
}

// attempt makes one pass: read, judge, and write if the judgement allows it. A nil lease
// with a nil error means "not yet".
func (m *LeaseManager) attempt(
	ctx context.Context,
	first ha.LeaseSnapshot,
) (*coordinationv1.Lease, ha.TakeOverVerdict, error) {
	lease := &coordinationv1.Lease{}
	err := m.Client.Get(ctx, m.key(), lease)
	if apierrors.IsNotFound(err) {
		created, createErr := m.create(ctx)
		if createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return nil, ha.TakeOverVerdict{}, createErr
		}
		if createErr == nil {
			return created, ha.TakeOverVerdict{Allowed: true, Reason: ha.TakeOverUnheld}, nil
		}
		return nil, ha.TakeOverVerdict{Reason: ha.TakeOverHolderAlive}, nil
	}
	if err != nil {
		return nil, ha.TakeOverVerdict{}, err
	}

	verdict := m.Config.MayTakeOver(first, snapshotOf(lease), m.Holder)
	if !verdict.Allowed {
		return nil, verdict, nil
	}

	previous := ""
	if lease.Spec.HolderIdentity != nil {
		previous = *lease.Spec.HolderIdentity
	}
	transitions := LeaderTransitions(lease)
	if previous != m.Holder {
		transitions++
		lease.Spec.AcquireTime = ptr.To(metav1.NowMicro())
	}
	lease.Spec.HolderIdentity = ptr.To(m.Holder)
	lease.Spec.LeaseTransitions = ptr.To(transitions)
	lease.Spec.LeaseDurationSeconds = ptr.To(int32(m.Config.LeaseDuration.Seconds()))
	lease.Spec.RenewTime = ptr.To(metav1.NowMicro())

	// An optimistic-concurrency conflict means somebody else wrote the lease between the
	// read and the write, which is exactly the race the resourceVersion exists to lose
	// safely. Losing it is "not yet", never "force it".
	if err := m.Client.Update(ctx, lease); err != nil {
		if apierrors.IsConflict(err) {
			return nil, ha.TakeOverVerdict{Reason: ha.TakeOverHolderAlive}, nil
		}
		return nil, ha.TakeOverVerdict{}, err
	}
	return lease, verdict, nil
}

func (m *LeaseManager) create(ctx context.Context) (*coordinationv1.Lease, error) {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr.To(m.Holder),
			LeaseDurationSeconds: ptr.To(int32(m.Config.LeaseDuration.Seconds())),
			AcquireTime:          ptr.To(metav1.NowMicro()),
			RenewTime:            ptr.To(metav1.NowMicro()),
			LeaseTransitions:     ptr.To(int32(0)),
		},
	}
	return lease, m.Client.Create(ctx, lease)
}

// Renew extends this member's hold.
//
// The three outcomes are deliberately distinct. Lost means the API server answered and
// named somebody else, which is terminal for a primary. Unverified means the API server did
// not answer at all, which is not terminal: an operator having a bad day and a node alone
// in the dark look identical from here, and fencing on that evidence turns control-plane
// maintenance into simultaneous self-immolation across the fleet. The isolation probe,
// which asks the peers rather than the API server, is what fences a node that is genuinely
// alone.
func (m *LeaseManager) Renew(ctx context.Context) ha.RenewOutcome {
	lease := &coordinationv1.Lease{}
	if err := m.Client.Get(ctx, m.key(), lease); err != nil {
		if apierrors.IsNotFound(err) {
			if _, createErr := m.create(ctx); createErr == nil {
				return ha.RenewOK
			}
		}
		return ha.RenewUnverified
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != m.Holder {
		return ha.RenewLost
	}
	lease.Spec.RenewTime = ptr.To(metav1.NowMicro())
	lease.Spec.LeaseDurationSeconds = ptr.To(int32(m.Config.LeaseDuration.Seconds()))
	// Every failure is unverified, conflict included: a renew that did not commit leaves the
	// holder unable to say whether it still holds the lease, and that is the same answer
	// whatever the API server's reason was.
	if err := m.Client.Update(ctx, lease); err != nil {
		return ha.RenewUnverified
	}
	return ha.RenewOK
}

// Release hands the lease back cooperatively, stamping the short validity so a planned
// switchover does not make the successor wait out a full leaseDuration.
func (m *LeaseManager) Release(ctx context.Context) error {
	lease := &coordinationv1.Lease{}
	if err := m.Client.Get(ctx, m.key(), lease); err != nil {
		return client.IgnoreNotFound(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != m.Holder {
		return nil
	}
	lease.Spec.HolderIdentity = ptr.To("")
	lease.Spec.LeaseDurationSeconds = ptr.To(int32(m.Config.ReleasedLeaseDuration.Seconds()))
	lease.Spec.RenewTime = ptr.To(metav1.NowMicro())
	return m.Client.Update(ctx, lease)
}
