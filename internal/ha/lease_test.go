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

import (
	"testing"
	"time"
)

var observedAt = time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

func TestDefaultLeaseParameters(t *testing.T) {
	config := DefaultLeaseConfig()

	if config.LeaseDuration != 15*time.Second || config.RenewDeadline != 10*time.Second ||
		config.RetryPeriod != 2*time.Second || config.ReleasedLeaseDuration != time.Second {
		t.Fatalf("the validated parameter set changed: %+v", config)
	}
	if config.AcquireTimeout() != 21*time.Second {
		t.Fatalf("acquire timeout is leaseDuration + 3*retryPeriod, got %s", config.AcquireTimeout())
	}
	if !config.Valid() {
		t.Fatal("the default set must satisfy the admission invariants")
	}
}

func TestInvalidLeaseParameters(t *testing.T) {
	tooShort := DefaultLeaseConfig()
	tooShort.LeaseDuration = tooShort.RenewDeadline
	if tooShort.Valid() {
		t.Fatal("a lease must outlive the deadline for renewing it")
	}

	tooFewRetries := DefaultLeaseConfig()
	tooFewRetries.RetryPeriod = 9 * time.Second
	if tooFewRetries.Valid() {
		t.Fatal("5*renewDeadline must exceed 6*retryPeriod")
	}
}

func snapshot(holder string, renew time.Time, at time.Time) LeaseSnapshot {
	return LeaseSnapshot{
		Holder:        holder,
		RenewTime:     renew,
		LeaseDuration: 15 * time.Second,
		ObservedAt:    at,
	}
}

func TestTakeOverIsRefusedWhileTheHolderKeepsRenewing(t *testing.T) {
	config := DefaultLeaseConfig()
	first := snapshot(memberOne, observedAt, observedAt)
	latest := snapshot(memberOne, observedAt.Add(time.Second), observedAt.Add(time.Minute))

	verdict := config.MayTakeOver(first, latest, memberTwo)

	if verdict.Allowed {
		t.Fatal("a changed RenewTime proves the holder is alive")
	}
	if verdict.Reason != TakeOverHolderAlive {
		t.Fatalf("reason was %q", verdict.Reason)
	}
}

func TestTakeOverIsRefusedBeforeTheValidityElapses(t *testing.T) {
	config := DefaultLeaseConfig()
	first := snapshot(memberOne, observedAt, observedAt)
	latest := snapshot(memberOne, observedAt, observedAt.Add(14*time.Second))

	verdict := config.MayTakeOver(first, latest, memberTwo)

	if verdict.Allowed {
		t.Fatal("14s is inside a 15s lease")
	}
	if verdict.Reason != TakeOverTooSoon {
		t.Fatalf("reason was %q", verdict.Reason)
	}
}

func TestTakeOverIsAllowedOnceTheHolderStopsRenewing(t *testing.T) {
	config := DefaultLeaseConfig()
	first := snapshot(memberOne, observedAt, observedAt)
	latest := snapshot(memberOne, observedAt, observedAt.Add(15*time.Second))

	verdict := config.MayTakeOver(first, latest, memberTwo)

	if !verdict.Allowed || verdict.Reason != TakeOverExpired {
		t.Fatalf("verdict was %+v", verdict)
	}
}

func TestTheHoldersClockIsNeverTrustedForOrdering(t *testing.T) {
	// A holder whose clock runs an hour fast stamps RenewTime an hour into the future. An
	// implementation that compared timestamps would conclude the lease is valid until then
	// and never take it, so the elapsed-time judgement is made entirely on the observer's
	// own clock and RenewTime is only ever compared for equality.
	config := DefaultLeaseConfig()
	skewed := observedAt.Add(time.Hour)
	first := snapshot(memberOne, skewed, observedAt)
	latest := snapshot(memberOne, skewed, observedAt.Add(20*time.Second))

	verdict := config.MayTakeOver(first, latest, memberTwo)

	if !verdict.Allowed {
		t.Fatalf("a future-stamped RenewTime must not block a take-over: %+v", verdict)
	}
}

func TestACooperativelyReleasedLeaseIsTakenAfterItsShortValidity(t *testing.T) {
	config := DefaultLeaseConfig()
	released := snapshot(memberOne, observedAt, observedAt)
	released.LeaseDuration = config.ReleasedLeaseDuration
	latest := released
	latest.ObservedAt = observedAt.Add(config.ReleasedLeaseDuration)

	verdict := config.MayTakeOver(released, latest, memberTwo)

	if !verdict.Allowed {
		t.Fatalf("a released lease must not cost a full leaseDuration: %+v", verdict)
	}
}

func TestAnUnheldLeaseIsAcquiredImmediately(t *testing.T) {
	config := DefaultLeaseConfig()
	empty := snapshot("", time.Time{}, observedAt)

	verdict := config.MayTakeOver(empty, empty, memberTwo)

	if !verdict.Allowed || verdict.Reason != TakeOverUnheld {
		t.Fatalf("verdict was %+v", verdict)
	}
}

func TestOnlyALostLeaseIsTerminal(t *testing.T) {
	if !RenewLost.Terminal() {
		t.Fatal("a lease demonstrably held by somebody else must stop the postmaster")
	}
	if RenewUnverified.Terminal() {
		t.Fatal("failing to verify the lease must not fence the node; the isolation probe does that")
	}
	if RenewOK.Terminal() {
		t.Fatal("a renewed lease is not terminal")
	}
}

func TestEpochFollowsLeaderTransitions(t *testing.T) {
	if got := Epoch(1, 0); got != 1 {
		t.Fatalf("the first primary publishes the base epoch, got %d", got)
	}
	if got := Epoch(1, 3); got != 4 {
		t.Fatalf("epoch was %d, want 4", got)
	}
	if got := Epoch(1, -1); got != 1 {
		t.Fatalf("an unset transition counter must not produce an epoch below the base, got %d", got)
	}
}
