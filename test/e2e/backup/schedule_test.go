//go:build e2e

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

package backup

import (
	"regexp"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// scheduledBackupName is what the controller derives from the slot a backup fills:
// <instance>-<YYYYMMDD>t<HHMM>, in UTC.
var scheduledBackupName = regexp.MustCompile(`^` + archiveInstance + `-\d{8}t\d{4}$`)

// The schedule is the only route by which a backup happens without somebody asking for one,
// and it is therefore the only route that matters for a product claiming nightly backups.
//
// Every other spec in this suite creates its PgBackup by hand, which is exactly why the
// schedule could mint on none of the day's 1440 minutes without a single one of them going
// red. This runs last so that the minute-by-minute schedule it sets cannot contend with the
// backups the earlier specs take.
func scheduledBackupSpecs() {
	Describe("a backup nobody asked for", Ordered, func() {
		It("mints one when the schedule says so", func() {
			// The instance has carried the default 0 2 * * * schedule since it was created,
			// and the grace window is an hour, so a run that happens to straddle 02:00 UTC
			// already has a slot-named backup sitting there. Without a baseline this spec
			// passes on that one and proves nothing about the schedule it sets.
			before := scheduledBackupNames(Default)

			By("asking for a backup every minute")
			Eventually(func(g Gomega) {
				instance := readInstance(g)
				instance.Spec.Backup.Schedule = ptr.To("* * * * *")
				g.Expect(k8sClient.Update(suiteCtx, instance)).To(Succeed())
			}).Should(Succeed())

			// Named after the slot rather than generated, so this also proves the name is the
			// idempotency key: a controller reconciling twice inside one minute fills one slot.
			By("waiting for the controller to mint a backup of its own accord")
			Eventually(func(g Gomega) {
				var minted []string
				for _, name := range scheduledBackupNames(g) {
					if !slices.Contains(before, name) {
						minted = append(minted, name)
					}
				}
				g.Expect(minted).NotTo(BeEmpty(),
					"the schedule minted nothing, so a nightly backup would never be taken")
			}, "3m", "5s").Should(Succeed())
		})

		AfterAll(func() {
			Eventually(func(g Gomega) {
				instance := readInstance(g)
				instance.Spec.Backup.Schedule = nil
				g.Expect(k8sClient.Update(suiteCtx, instance)).To(Succeed())
			}).Should(Succeed())
		})
	})
}

// scheduledBackupNames is every backup in the namespace whose name was derived from a
// schedule slot rather than written by hand.
func scheduledBackupNames(g Gomega) []string {
	GinkgoHelper()
	backups := &pgelasticv1alpha1.PgBackupList{}
	g.Expect(k8sClient.List(suiteCtx, backups, client.InNamespace(archiveNamespace))).To(Succeed())

	var names []string
	for i := range backups.Items {
		if scheduledBackupName.MatchString(backups.Items[i].Name) {
			names = append(names, backups.Items[i].Name)
		}
	}
	return names
}
