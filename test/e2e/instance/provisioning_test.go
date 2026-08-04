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

package instance

import (
	"fmt"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/agent"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

const (
	// e2eNamespace is created and destroyed by this suite.
	e2eNamespace = "pgelastic-e2e-instance"
	// instanceName is short so the generated member and slot names stay well inside every
	// identifier limit they end up in.
	instanceName = "pg-e2e"
	// sizingClass is the development tier: three postmasters fit on one kind node while
	// still exercising the whole max_connections derivation.
	sizingClass = "dev-1"
	// e2eReplicas is the only topology the quorum gate is designed around.
	e2eReplicas = 3
)

// psql runs a query on one member over its local Unix socket, as the bootstrap superuser.
// It is deliberately the socket rather than a TCP connection: the superuser has no
// password at all, and peer authentication over a socket in an emptyDir is the only way
// to reach it, which is exactly the property the design intends.
func psql(member, query string) (string, error) {
	command := kubectlCommand("exec", "-n", e2eNamespace, member, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-d", "postgres", "-tAqc", query)
	output, err := command.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func mustQuery(member, query string) string {
	GinkgoHelper()
	output, err := psql(member, query)
	Expect(err).NotTo(HaveOccurred(), "psql on %s failed: %s", member, output)
	return output
}

func memberNames() []string {
	names := make([]string, 0, e2eReplicas)
	for serial := int32(1); serial <= e2eReplicas; serial++ {
		names = append(names, provision.MemberName(instanceName, serial))
	}
	return names
}

func fetchInstance() *pgelasticv1alpha1.PgInstance {
	GinkgoHelper()
	fetched := &pgelasticv1alpha1.PgInstance{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: instanceName}, fetched)).To(Succeed())
	return fetched
}

var _ = Describe("Provisioning a three-node PostgreSQL 18 instance", Ordered, func() {
	BeforeAll(func() {
		probeNamespace.Store(e2eNamespace)
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, namespace))).To(Succeed())
		claimNamespace(e2eNamespace)

		instance := &pgelasticv1alpha1.PgInstance{
			ObjectMeta: metav1.ObjectMeta{Name: instanceName, Namespace: e2eNamespace},
			Spec: pgelasticv1alpha1.PgInstanceSpec{
				PoolRef: corev1.LocalObjectReference{Name: claimPoolName},
				Class:   sizingClass,
				Storage: pgelasticv1alpha1.InstanceStorage{
					Size: resource.MustParse("1Gi"),
					WALVolume: pgelasticv1alpha1.WALVolume{
						Size: resource.MustParse("1Gi"),
					},
				},
			},
		}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, instance))).To(Succeed())

		DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, instance)
			_ = k8sClient.Delete(suiteCtx, namespace)
		})
	})

	It("brings every member up and reports the instance Ready", func() {
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace),
				client.MatchingLabels{provision.LabelInstanceName: instanceName})).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(e2eReplicas))
			for i := range pods.Items {
				g.Expect(pods.Items[i].Status.Phase).To(Equal(corev1.PodRunning),
					"%s is %s", pods.Items[i].Name, pods.Items[i].Status.Phase)
			}
			g.Expect(fetchInstance().Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
		}).Should(Succeed())
	})

	It("elects exactly one primary and two standbys", func() {
		Eventually(func(g Gomega) {
			status := fetchInstance().Status
			g.Expect(status.Instances).To(HaveLen(e2eReplicas))

			roles := map[pgelasticv1alpha1.InstanceRole]int{}
			for _, member := range status.Instances {
				roles[member.Role]++
			}
			g.Expect(roles[pgelasticv1alpha1.InstanceRolePrimary]).To(Equal(1),
				"two members reporting primary is split brain, not a tiebreak")
			g.Expect(roles[pgelasticv1alpha1.InstanceRoleReplica]).To(Equal(2))
			g.Expect(status.CurrentPrimary).NotTo(BeEmpty())
		}).Should(Succeed())

		// The same claim, verified against PostgreSQL rather than against the CR: exactly
		// one member is out of recovery.
		outOfRecovery := 0
		for _, member := range memberNames() {
			if mustQuery(member, "SELECT pg_is_in_recovery()") == "f" {
				outOfRecovery++
			}
		}
		Expect(outOfRecovery).To(Equal(1))
	})

	It("streams from both standbys under an ANY 1 quorum", func() {
		primary := fetchInstance().Status.CurrentPrimary
		Eventually(func(g Gomega) {
			rows := mustQuery(primary,
				"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")
			g.Expect(rows).To(Equal("2"), "both standbys must be streaming")

			// With ANY 1 over two standbys, PostgreSQL reports both as quorum members.
			// Anything else means the clause the postmaster loaded is not the clause the
			// operator believes it loaded.
			syncStates := mustQuery(primary,
				"SELECT string_agg(DISTINCT sync_state, ',' ORDER BY sync_state) "+
					"FROM pg_stat_replication WHERE state = 'streaming'")
			g.Expect(syncStates).To(Equal("quorum"))
		}).Should(Succeed())

		loaded := mustQuery(primary, "SHOW synchronous_standby_names")
		numSync, members := agent.ParseSyncStandbyNames(loaded)
		Expect(numSync).To(Equal(int32(1)), "loaded clause was %q", loaded)
		Expect(members).To(HaveLen(2), "loaded clause was %q", loaded)
	})

	It("publishes the quorum evidence PostgreSQL actually loaded", func() {
		// The two halves are settled by different writers a reconcile apart: the primary
		// publishes the clause it read back out of its own postmaster, and the operator
		// derives inSyncSet from that record afterwards. Asserting the second outside the
		// wait for the first passes only while the operator happens to be quick.
		Eventually(func(g Gomega) {
			instance := fetchInstance()
			evidence := instance.Status.QuorumEvidence
			g.Expect(evidence).NotTo(BeNil())
			g.Expect(evidence.NumSync).To(Equal(int32(1)))
			g.Expect(evidence.VotingMembers).To(HaveLen(2))
			g.Expect(evidence.SynchronousStandbyNames).To(HavePrefix("ANY 1"))

			var replicas int
			for _, member := range instance.Status.Instances {
				if member.Role != pgelasticv1alpha1.InstanceRoleReplica {
					continue
				}
				replicas++
				g.Expect(member.InSyncSet).To(BeTrue(),
					"%s is streaming and must count towards the quorum", member.Name)
			}
			g.Expect(replicas).To(Equal(e2eReplicas-1),
				"every standby has to be reported, or the check above proves nothing")
		}).Should(Succeed())
	})

	It("records the collation contract initdb was pinned to", func() {
		Eventually(func(g Gomega) {
			g.Expect(fetchInstance().Status.CollationContract).NotTo(BeNil())
		}).Should(Succeed())

		contract := fetchInstance().Status.CollationContract
		Expect(contract.Encoding).To(Equal("UTF8"))
		Expect(contract.LocaleProvider).To(Equal("b"),
			"the builtin provider is what removes ICU and glibc drift between instances")
		Expect(contract.Locale).To(Equal("C.UTF-8"))
		Expect(contract.WALSegmentSize).To(Equal(int64(16 << 20)))
		Expect(contract.DataChecksums).To(BeTrue())
		Expect(contract.SystemIdentifier).NotTo(BeEmpty())

		// Every member must share the tuple, or the pool cannot move a tenant between
		// them without producing indexes silently inconsistent with their heap ordering.
		for _, member := range memberNames() {
			Expect(mustQuery(member,
				"SELECT datlocprovider FROM pg_database WHERE datname = 'postgres'")).To(Equal("b"))
			Expect(mustQuery(member,
				"SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'")).
				To(Equal(strconv.Itoa(16 << 20)))
			Expect(mustQuery(member, "SHOW data_checksums")).To(Equal("on"))
		}
	})

	It("runs with the max_connections the capacity model derives", func() {
		class, err := pgconf.LookupSizingClass(sizingClass)
		Expect(err).NotTo(HaveOccurred())
		want := pgconf.DeriveCapacity(class.AllocatableConnections, 4, e2eReplicas, 4)

		capacity := fetchInstance().Status.Capacity
		Expect(capacity).NotTo(BeNil())
		Expect(capacity.MaxConnections).To(Equal(want.MaxConnections))
		Expect(capacity.Allocatable).To(Equal(class.AllocatableConnections))
		Expect(capacity.ReservedForAdmin).To(
			Equal(pgconf.SuperuserReservedConnections + pgconf.ReservedConnections))
		Expect(capacity.MaxConnections).To(Equal(
			capacity.Allocatable + capacity.ReservedForAdmin + want.AgentOverhead))

		for _, member := range memberNames() {
			Expect(mustQuery(member, "SHOW max_connections")).
				To(Equal(strconv.Itoa(int(want.MaxConnections))),
					"%s is running with a different budget from the one the pool was sold", member)
			Expect(mustQuery(member, "SHOW superuser_reserved_connections")).To(Equal("3"))
			Expect(mustQuery(member, "SHOW reserved_connections")).To(Equal("5"))
		}
	})

	It("applies the operator-owned parameters section 4.6 requires", func() {
		for _, member := range memberNames() {
			Expect(mustQuery(member, "SHOW wal_level")).To(Equal("logical"))
			Expect(mustQuery(member, "SHOW track_commit_timestamp")).To(Equal("on"))
			Expect(mustQuery(member, "SHOW allow_alter_system")).To(Equal("off"))
			Expect(mustQuery(member, "SHOW restart_after_crash")).To(Equal("off"))
			Expect(mustQuery(member, "SHOW io_method")).To(Equal("worker"))
			Expect(mustQuery(member, "SHOW archive_mode")).To(Equal("on"))
			Expect(mustQuery(member, "SHOW logging_collector")).To(Equal("on"))
			Expect(mustQuery(member, "SHOW wal_log_hints")).To(Equal("off"),
				"PG18 data checksums make wal_log_hints redundant, and it costs WAL volume")
		}
	})

	It("binds the configuration hash and the fence epoch into the postmaster", func() {
		for _, member := range memberNames() {
			hash := mustQuery(member, "SELECT current_setting('"+pgconf.GUCConfigSHA256+"')")
			Expect(hash).To(HaveLen(64), "the config hash must be readable with a plain SHOW")

			epoch := mustQuery(member, "SELECT current_setting('"+pgconf.GUCPrimaryEpoch+"')")
			Expect(epoch).To(Equal(strconv.FormatInt(fetchInstance().Status.PrimaryEpoch, 10)),
				"%s carries a fence token that disagrees with the published one", member)
		}
		Expect(fetchInstance().Status.PrimaryEpoch).To(BeNumerically(">=", 1))
	})

	It("replicates a committed write to both standbys", func() {
		primary := fetchInstance().Status.CurrentPrimary
		mustQuery(primary, "CREATE TABLE IF NOT EXISTS e2e_replication (id int primary key)")
		mustQuery(primary, "INSERT INTO e2e_replication VALUES (1), (2), (3)")

		committed := mustQuery(primary, "SELECT count(*) FROM e2e_replication")
		Expect(committed).To(Equal("3"))

		for _, member := range memberNames() {
			if member == primary {
				continue
			}
			Eventually(func(g Gomega) {
				output, err := psql(member, "SELECT count(*) FROM e2e_replication")
				g.Expect(err).NotTo(HaveOccurred(), output)
				g.Expect(output).To(Equal("3"))
			}, "2m", "2s").Should(Succeed(), "%s never saw the committed rows", member)

			Expect(mustQuery(member, "SELECT pg_is_in_recovery()")).To(Equal("t"))
		}
	})

	It("gives every member its own persistent replication slot", func() {
		primary := fetchInstance().Status.CurrentPrimary
		for _, member := range memberNames() {
			if member == primary {
				continue
			}
			slot := provision.ReplicationSlotName(member)
			Expect(mustQuery(primary, fmt.Sprintf(
				"SELECT slot_type || ':' || active::text FROM pg_replication_slots WHERE slot_name = '%s'",
				slot))).To(Equal("physical:true"), "slot %s", slot)
		}
	})

	It("leaves no authentication failures behind it", func() {
		// The generated pg_hba.conf has to admit every connection the instance itself
		// makes. Slot synchronisation in particular opens an ordinary connection to the
		// database named in primary_conninfo rather than a replication one, and getting
		// that wrong costs nothing visible: replication still streams, while every standby
		// loops on an authentication FATAL and the synchronised slots never advance.
		for _, member := range memberNames() {
			logs := kubectlCommand("logs", "-n", e2eNamespace, member, "-c", "postgres")
			output, err := logs.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(output))
			Expect(string(output)).NotTo(ContainSubstring("pg_hba.conf rejects connection"),
				"%s is rejecting a connection the instance itself makes", member)
			Expect(string(output)).NotTo(ContainSubstring("password authentication failed"),
				"%s is refusing a credential the operator issued", member)
		}
	})

	It("keeps the two volumes of every member in one labelled group", func() {
		claims := &corev1.PersistentVolumeClaimList{}
		Expect(k8sClient.List(suiteCtx, claims, client.InNamespace(e2eNamespace),
			client.MatchingLabels{provision.LabelInstanceName: instanceName})).To(Succeed())
		Expect(claims.Items).To(HaveLen(2 * e2eReplicas))

		groups := provision.GroupsOf(claims.Items)
		Expect(groups).To(HaveLen(e2eReplicas))
		for _, group := range groups {
			Expect(group.Bound()).To(BeTrue(), "serial %d", group.Serial)
			Expect(group.Data.Annotations[provision.AnnotationPVCStatus]).
				To(Equal(provision.PVCStatusReady))
			Expect(group.WAL.Annotations[provision.AnnotationPVCStatus]).
				To(Equal(provision.PVCStatusReady))
		}

		// pg_wal really is on the WAL volume rather than inside PGDATA, which is the whole
		// reason the second volume is mandatory.
		for _, member := range memberNames() {
			command := kubectlCommand("exec", "-n", e2eNamespace, member, "-c", "postgres",
				"--", "readlink", provision.DataDir+"/pg_wal")
			output, err := command.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(output))
			Expect(strings.TrimSpace(string(output))).To(Equal(provision.WALDir))
		}
	})
})

// The authentication the metrics scrape depends on, proven where it actually happens.
//
// The scrape must not be the bootstrap superuser - CloudNativePG's exporter was, for the
// project's whole life, until a low-privilege user chained to superuser through it. So the
// agent reads pg_stat_database as pgelastic_ops, which holds pg_monitor and nothing else.
//
// Reaching that role at all takes one ordered pg_hba record. The agent runs as OS user
// postgres and the generated file says `local all all peer`, so over the socket the agent can
// only ever *be* postgres unless a scram record for the ops role sits above that catch-all.
// A unit test on the rendered file cannot prove the ordering works, because ordering is
// something the postmaster does; the container tests cannot prove it either, because they
// connect over TCP. This is the only place the production shape exists: the real file, the
// real postmaster, the real socket, and the real OS user.
var _ = It("admits the scrape identity on the socket, and no more than it", func() {
	member := provision.MemberName(instanceName, 1)

	secret := &corev1.Secret{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: e2eNamespace,
		Name:      provision.CredentialsSecretName(instanceName),
	}, secret)).To(Succeed())
	password := string(secret.Data[provision.SecretKeyOpsPassword])
	Expect(password).NotTo(BeEmpty(), "the instance published no ops password")

	asOps := func(query string) (string, error) {
		command := kubectlCommand("exec", "-n", e2eNamespace, member, "-c", "postgres", "--",
			"env", "PGPASSWORD="+password,
			"psql", "-h", provision.SocketDir, "-U", provision.OpsRole,
			"-d", "postgres", "-tAqc", query)
		output, err := command.CombinedOutput()
		return strings.TrimSpace(string(output)), err
	}

	// The record itself. Without it the postmaster falls through to `local all all peer`,
	// the OS user is postgres, the role is not, and this is a FATAL rather than a row.
	answer, err := asOps("SELECT current_user")
	Expect(err).NotTo(HaveOccurred(), "pgelastic_ops could not reach the socket: %s", answer)
	Expect(answer).To(Equal(provision.OpsRole))

	// And what the scrape is for: the counters are readable through pg_monitor, without any
	// privilege on the databases being counted.
	counters, err := asOps(
		"SELECT count(*) FROM pg_catalog.pg_stat_database WHERE datname IS NOT NULL")
	Expect(err).NotTo(HaveOccurred(), "the scrape identity cannot read pg_stat_database: %s", counters)
	Expect(counters).NotTo(Equal("0"))

	// Least privilege is the whole point of using this role rather than the superuser, so it
	// is asserted rather than assumed: a scrape identity that turned out to be a superuser
	// would pass every test above and none of the reasoning behind them.
	attributes, err := asOps(
		"SELECT rolsuper::int::text || rolcreatedb::int::text || rolcreaterole::int::text " +
			"|| rolbypassrls::int::text FROM pg_catalog.pg_roles WHERE rolname = current_user")
	Expect(err).NotTo(HaveOccurred(), "%s", attributes)
	Expect(attributes).To(Equal("0000"),
		"the scrape identity carries a privileged attribute: superuser/createdb/createrole/bypassrls")
})
