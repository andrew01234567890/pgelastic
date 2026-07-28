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

package ha

import "time"

// LeaseConfig parameterises the promotion Lease. The values are CNPG's, validated at
// admission.
//
// They are also the proxy's fencing deadline. A candidate cannot take over a held lease
// until LeaseDuration has elapsed without a renewal, and the proxy severs old-epoch sockets
// within one RetryPeriod, so the design point is: fence at T+RetryPeriod, promotion
// impossible before T+LeaseDuration. Shortening LeaseDuration therefore shortens the fence
// deadline in lockstep, which is why the two are never configured apart.
type LeaseConfig struct {
	// LeaseDuration is how long a lease stays valid without a renewal.
	LeaseDuration time.Duration
	// RenewDeadline is how long the holder keeps trying to renew before treating the loss
	// as terminal.
	RenewDeadline time.Duration
	// RetryPeriod is the interval between renewal and acquisition attempts.
	RetryPeriod time.Duration
	// ReleasedLeaseDuration is the short validity stamped on a lease released
	// cooperatively, so a planned switchover does not wait out a full LeaseDuration.
	ReleasedLeaseDuration time.Duration
}

// DefaultLeaseConfig is the validated set from the design.
func DefaultLeaseConfig() LeaseConfig {
	return LeaseConfig{
		LeaseDuration:         15 * time.Second,
		RenewDeadline:         10 * time.Second,
		RetryPeriod:           2 * time.Second,
		ReleasedLeaseDuration: time.Second,
	}
}

// AcquireTimeout bounds one acquisition attempt. On DeadlineExceeded the caller requeues
// after RetryPeriod rather than retrying inline, so a lease that is genuinely held does not
// pin a reconcile worker for the whole of its duration.
func (c LeaseConfig) AcquireTimeout() time.Duration {
	return c.LeaseDuration + 3*c.RetryPeriod
}

// Valid reports whether the configuration satisfies the two invariants admission enforces:
// a lease must outlive the deadline for renewing it, and there must be room for enough
// retries inside one renewal window for a single lost round trip not to lose the lease.
func (c LeaseConfig) Valid() bool {
	return c.LeaseDuration > c.RenewDeadline && 5*c.RenewDeadline > 6*c.RetryPeriod
}

// LeaseSnapshot is one observation of the Lease, paired with the observer's own clock
// reading.
//
// RenewTime is carried as an opaque value that is only ever compared for equality. The
// previous holder's clock is never trusted for ordering: a holder whose clock runs fast can
// stamp a renewal arbitrarily far in the future, and an implementation that compares
// timestamps would then refuse to ever take the lease from it.
type LeaseSnapshot struct {
	// Holder is holderIdentity, which is the Pod name of the agent holding the lease.
	Holder string
	// RenewTime is the holder's own stamp. Equality only.
	RenewTime time.Time
	// LeaseDuration is the validity the holder stamped on the record, which is shortened to
	// ReleasedLeaseDuration on a cooperative release.
	LeaseDuration time.Duration
	// ObservedAt is the observer's clock when this snapshot was read. Every elapsed-time
	// judgement is made from this field and never from RenewTime.
	ObservedAt time.Time
}

// TakeOverVerdict is the answer to "may this member acquire the lease".
type TakeOverVerdict struct {
	// Allowed is whether acquisition may proceed.
	Allowed bool
	// Reason is why.
	Reason string
}

// Reasons a take-over is allowed or refused.
const (
	// TakeOverUnheld is a lease with no holder.
	TakeOverUnheld = "LeaseUnheld"
	// TakeOverAlreadyHeld is a lease this member already holds.
	TakeOverAlreadyHeld = "AlreadyHeld"
	// TakeOverExpired is a lease whose holder has not renewed for a full validity period,
	// measured entirely on the observer's own clock.
	TakeOverExpired = "HolderStoppedRenewing"
	// TakeOverHolderAlive is a lease whose RenewTime changed between the two observations,
	// which proves the holder is still there.
	TakeOverHolderAlive = "HolderIsRenewing"
	// TakeOverTooSoon is a lease whose holder has not renewed yet but whose validity has
	// not elapsed on the observer's clock either.
	TakeOverTooSoon = "ValidityHasNotElapsed"
)

// MayTakeOver decides whether self may seize a lease held by somebody else, from two
// observations separated by wall time on the observer's own clock.
//
// The two-observation form is what makes the decision safe without trusting the holder's
// clock: a RenewTime that differs between the observations proves the holder is alive, and
// a RenewTime that is unchanged across at least one full validity period proves it has
// stopped. Neither judgement reads the timestamp as a point in time.
func (c LeaseConfig) MayTakeOver(first, latest LeaseSnapshot, self string) TakeOverVerdict {
	switch {
	case latest.Holder == "":
		return TakeOverVerdict{Allowed: true, Reason: TakeOverUnheld}
	case latest.Holder == self:
		return TakeOverVerdict{Allowed: true, Reason: TakeOverAlreadyHeld}
	case latest.Holder != first.Holder || !latest.RenewTime.Equal(first.RenewTime):
		return TakeOverVerdict{Reason: TakeOverHolderAlive}
	}

	validity := latest.LeaseDuration
	if validity <= 0 {
		validity = c.LeaseDuration
	}
	if latest.ObservedAt.Sub(first.ObservedAt) < validity {
		return TakeOverVerdict{Reason: TakeOverTooSoon}
	}
	return TakeOverVerdict{Allowed: true, Reason: TakeOverExpired}
}

// RenewOutcome is what one renewal attempt established.
type RenewOutcome string

const (
	// RenewOK is a renewal that landed.
	RenewOK RenewOutcome = "Renewed"
	// RenewLost is a lease this member demonstrably no longer holds, because the API server
	// answered and named somebody else. It is terminal for a primary: the postmaster stops.
	RenewLost RenewOutcome = "Lost"
	// RenewUnverified is a renewal that could not be attempted or confirmed because the API
	// server did not answer.
	//
	// It is deliberately NOT terminal. An operator having a bad day and a node alone in the
	// dark look identical from here, and treating them alike is what turns routine
	// control-plane maintenance into simultaneous self-immolation across the fleet. The
	// isolation probe, which asks the peers rather than the API server, fences a node that
	// is genuinely alone.
	RenewUnverified RenewOutcome = "Unverified"
)

// Terminal reports whether an outcome requires the primary to stop its postmaster.
func (o RenewOutcome) Terminal() bool { return o == RenewLost }

// InitialPrimaryEpoch is the epoch a freshly bootstrapped instance publishes.
//
// It starts at one rather than zero so that "no epoch has ever been published" and "the
// first primary" stay distinguishable. The proxy's in-memory epoch never decreases, so a
// zero would be indistinguishable from an unset field on the fencing path.
const InitialPrimaryEpoch int64 = 1

// Epoch derives the fence token from the Lease's transition counter.
//
// Deriving it rather than incrementing a status field is what makes it monotonic across
// operator restarts and impossible to reuse: the API server owns the counter, and two
// members cannot both observe the same transition as theirs. The base offset keeps the
// first primary at a non-zero epoch so that "no epoch has ever been published" stays
// distinguishable from "the first epoch", which matters because the proxy's in-memory epoch
// never decreases.
func Epoch(base int64, leaderTransitions int32) int64 {
	if leaderTransitions < 0 {
		return base
	}
	return base + int64(leaderTransitions)
}
