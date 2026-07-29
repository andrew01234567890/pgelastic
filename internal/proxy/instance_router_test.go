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

package proxy

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const switchoverHolder = "shop/roll-pg-a"

func poolKey() client.ObjectKey {
	return client.ObjectKey{Namespace: routerNamespace, Name: routerPool}
}

func instanceReportJSON(quiesced, drained bool, queued, inFlight int, tenants ...string) string {
	quoted := make([]string, 0, len(tenants))
	for _, tenant := range tenants {
		quoted = append(quoted, tomlString(tenant))
	}
	return fmt.Sprintf(
		`{"instance":%q,"quiesced":%t,"tenants":[%s],"inFlight":%d,"queued":%d,`+
			`"drained":%t,"holder":%q,"leaseExpiresInMs":9000}`,
		routerSource, quiesced, strings.Join(quoted, ","), inFlight, queued, drained,
		switchoverHolder)
}

func withInstanceReports(h *harness, report string) {
	for _, pod := range h.fleet.pods {
		h.fleet.report[pod+"/instance"] = report
	}
}

func TestAnInstanceHoldReachesEveryReplica(t *testing.T) {
	h := newHarness(t, routerSource)
	withInstanceReports(h, instanceReportJSON(true, true, 0, 0, routerDatabase))

	if err := h.router.QuiesceInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("holding the instance: %v", err)
	}
	for _, pod := range h.fleet.pods {
		if !h.journal.has("/quiesceInstance " + pod) {
			t.Fatalf("%s was never held; its share of every tenant would serve straight "+
				"through the role change: %v", pod, h.journal.all())
		}
	}
	if err := h.router.ResumeInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("resuming the instance: %v", err)
	}
}

// An instance held on two replicas out of three queues two thirds of every tenant's traffic
// behind a role change that is not going to happen, with the lease TTL as the only way out.
func TestAPartialInstanceHoldIsUnwoundRatherThanLeftForTheLease(t *testing.T) {
	h := newHarness(t, routerSource)
	h.fleet.refuse = "proxy-2"

	err := h.router.QuiesceInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder)
	if err == nil {
		t.Fatal("a partial hold was reported as success")
	}
	for _, pod := range []string{firstReplica, secondReplica} {
		if !h.journal.has("/unquiesceInstance " + pod) {
			t.Fatalf("%s kept its hold after the take failed: %v", pod, h.journal.all())
		}
	}
}

func TestTheInstanceDrainIsSummedAndItsTenantsAreUnioned(t *testing.T) {
	h := newHarness(t, routerSource)
	h.fleet.report[firstReplica+"/instance"] = instanceReportJSON(true, false, 2, 1, "acme_db", "beta_db")
	h.fleet.report[secondReplica+"/instance"] = instanceReportJSON(true, true, 1, 0, "acme_db")
	h.fleet.report["proxy-2/instance"] = instanceReportJSON(true, true, 0, 0, "gamma_db")

	drain, err := h.router.InstanceDrainStatus(context.Background(), poolKey(), routerSource)
	if err != nil {
		t.Fatalf("reading the instance drain: %v", err)
	}
	if drain.Queued != 3 || drain.InFlight != 1 {
		t.Fatalf("queued/inFlight = %d/%d, want the sums 3/1", drain.Queued, drain.InFlight)
	}
	if drain.Drained {
		t.Fatal("one replica still holding a backend is enough to make a role change unsafe")
	}
	slices.Sort(drain.Tenants)
	if !slices.Equal(drain.Tenants, []string{"acme_db", "beta_db", "gamma_db"}) {
		t.Fatalf("tenants = %v, want the union across the replicas", drain.Tenants)
	}
}

func TestAnInstanceDrainIsOnlyDrainedWhenEveryReplicaAgrees(t *testing.T) {
	h := newHarness(t, routerSource)
	withInstanceReports(h, instanceReportJSON(true, true, 0, 0, routerDatabase))
	h.fleet.report[secondReplica+"/instance"] = instanceReportJSON(false, false, 0, 0, routerDatabase)

	drain, err := h.router.InstanceDrainStatus(context.Background(), poolKey(), routerSource)
	if err != nil {
		t.Fatalf("reading the instance drain: %v", err)
	}
	if drain.Quiesced || drain.Drained {
		t.Fatalf("one replica still admitting reported quiesced=%t drained=%t",
			drain.Quiesced, drain.Drained)
	}
}

// Resume commits, then the lease goes back. Leaving it to expire refuses the next
// switchover of the same instance with a conflict for up to the whole ceiling.
func TestResumingAnInstanceOpensTheGateBeforeGivingTheLeaseBack(t *testing.T) {
	h := newHarness(t, routerSource)
	withInstanceReports(h, instanceReportJSON(true, true, 0, 0, routerDatabase))
	if err := h.router.QuiesceInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("holding the instance: %v", err)
	}
	if err := h.router.ResumeInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("resuming the instance: %v", err)
	}
	resume := h.journal.indexOf("/resumeInstance " + firstReplica)
	release := h.journal.indexOf("/unquiesceInstance " + firstReplica)
	if resume < 0 || release < 0 || release < resume {
		t.Fatalf("the release did not follow the resume: %v", h.journal.all())
	}
}

// An abort never resumes: the queued clients are released by the unquiesce, but nothing
// commits a role change that did not happen.
func TestReleasingAnInstanceNeverResumesFirst(t *testing.T) {
	h := newHarness(t, routerSource)
	withInstanceReports(h, instanceReportJSON(true, true, 0, 0, routerDatabase))
	if err := h.router.QuiesceInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("holding the instance: %v", err)
	}
	if err := h.router.ReleaseInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("releasing the instance: %v", err)
	}
	if h.journal.has("/resumeInstance " + firstReplica) {
		t.Fatalf("an abort committed the hold: %v", h.journal.all())
	}
	if !h.journal.has("/unquiesceInstance " + firstReplica) {
		t.Fatalf("an abort left the clients queued: %v", h.journal.all())
	}
}

// A hold that is never released would otherwise be released by nothing, so the pause has to
// be recorded where an operator can read it.
func TestAnInstanceHoldRecordsHowLongItsClientsWereQueued(t *testing.T) {
	h := newHarness(t, routerSource)
	withInstanceReports(h, instanceReportJSON(true, true, 0, 0, routerDatabase))
	if err := h.router.QuiesceInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("holding the instance: %v", err)
	}
	h.clock = h.clock.Add(1300 * time.Millisecond)
	if err := h.router.ResumeInstance(
		context.Background(), poolKey(), routerSource, switchoverHolder); err != nil {
		t.Fatalf("resuming the instance: %v", err)
	}
	held, ok := h.router.InstancePause(poolKey(), routerSource)
	if !ok || held <= 0 {
		t.Fatalf("the pause was %v/%t, want the time the clients spent queued", held, ok)
	}
	if _, again := h.router.InstancePause(poolKey(), routerSource); again {
		t.Fatal("the pause was reported twice, so a caller would double count it")
	}
}

// Two pools may front instances of the same name, and one release must not stop the other's
// renewal loop.
func TestInstanceHoldsAreKeyedByPoolAsWellAsInstance(t *testing.T) {
	other := client.ObjectKey{Namespace: routerNamespace, Name: "other-pool"}
	if instanceHoldKey(poolKey(), routerSource) == instanceHoldKey(other, routerSource) {
		t.Fatal("two pools' holds on the same instance name share a key")
	}
}
