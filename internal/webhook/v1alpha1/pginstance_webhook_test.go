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

package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

const maxConnections = "max_connections"

var _ = Describe("the parameters a PgInstance may carry", func() {
	var validator PgInstanceCustomValidator

	instanceWith := func(parameters map[string]pgelasticv1alpha1.GUCValue) *pgelasticv1alpha1.PgInstance {
		return &pgelasticv1alpha1.PgInstance{
			Spec: pgelasticv1alpha1.PgInstanceSpec{Parameters: parameters},
		}
	}

	BeforeEach(func() { validator = PgInstanceCustomValidator{} })

	// The refusal spec.parameters has always documented and nothing has ever performed.
	// max_connections is the sharpest case: it is the unit the pool's rating, the reservation
	// ledger, oversubscription and chargeback are all denominated in, and a user value for it
	// was accepted by the API server, written to the object, and then dropped in silence.
	It("refuses a parameter the operator computes", func() {
		_, err := validator.ValidateCreate(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{maxConnections: "5000"}))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(maxConnections))
		Expect(err.Error()).To(ContainSubstring("Fixed"))
	})

	It("refuses a parameter blocked for safety, on update as well as create", func() {
		instance := instanceWith(map[string]pgelasticv1alpha1.GUCValue{"fsync": "off"})

		_, created := validator.ValidateCreate(ctx, instance)
		_, updated := validator.ValidateUpdate(ctx, instanceWith(nil), instance)

		Expect(created).To(HaveOccurred())
		Expect(updated).To(HaveOccurred())
	})

	// The whole point of the Tuned level is that the computed value is a default. A webhook
	// asking IsOwned rather than IsPinned would refuse this, and the level would be a no-op
	// that looked like it worked.
	It("admits a parameter the operator only has an opinion about", func() {
		var tuned string
		for _, name := range pgconf.OwnedNames() {
			if pgconf.Classify(name).Ownership == pgconf.OwnershipTuned {
				tuned = name
				break
			}
		}
		if tuned == "" {
			Skip("nothing is Tuned yet; this becomes live with the tuned table")
		}

		_, err := validator.ValidateCreate(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{tuned: "1"}))

		Expect(err).NotTo(HaveOccurred())
	})

	It("admits a parameter that is nobody's business but the tenant's", func() {
		_, err := validator.ValidateCreate(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{"random_page_cost": "1.1"}))

		Expect(err).NotTo(HaveOccurred())
	})

	// A manifest spelling out the whole effective configuration is a GitOps repository doing
	// the right thing rather than an error - as long as what it spells out is the value the
	// operator was going to emit anyway.
	It("admits a blocked parameter set to exactly the operator's own value", func() {
		var name, value string
		for _, candidate := range pgconf.PinnedNames() {
			owned := pgconf.Classify(candidate)
			if owned.Ownership == pgconf.OwnershipBlocked && !owned.Omit && owned.Value != "" {
				name, value = candidate, owned.Value
				break
			}
		}
		Expect(name).NotTo(BeEmpty(), "no blocked parameter carries a constant to agree with")

		_, agreeing := validator.ValidateCreate(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{name: pgelasticv1alpha1.GUCValue(value)}))
		_, disagreeing := validator.ValidateCreate(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{name: pgelasticv1alpha1.GUCValue(value + "x")}))

		Expect(agreeing).NotTo(HaveOccurred())
		Expect(disagreeing).To(HaveOccurred())
	})

	// Well-formedness is a separate axis from ownership. Folding them together is how a value
	// carrying a newline gets past a check that only looked at who owns the name.
	It("refuses a value that would not render as one line, whoever owns it", func() {
		_, err := validator.ValidateCreate(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{"random_page_cost": "1.1\nfsync = off"}))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("exactly one postgresql.conf line"))
	})

	It("reports every offending parameter at once, at the path that is wrong", func() {
		_, err := validator.ValidateCreate(ctx, instanceWith(map[string]pgelasticv1alpha1.GUCValue{
			maxConnections: "5000",
			"fsync":        "off",
		}))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.parameters[" + maxConnections + "]"))
		Expect(err.Error()).To(ContainSubstring("spec.parameters[fsync]"))
	})

	It("has nothing to say about a deletion", func() {
		_, err := validator.ValidateDelete(ctx, instanceWith(
			map[string]pgelasticv1alpha1.GUCValue{maxConnections: "5000"}))

		Expect(err).NotTo(HaveOccurred())
	})
})
