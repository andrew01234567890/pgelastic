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

package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
	"github.com/andrew01234567890/pgelastic/test/e2e/certcheck"
)

const (
	e2eNamespace = "pgelastic-e2e-proxy"
	// The instances are short-named so every generated slot and member name stays well
	// inside the identifier limits it ends up in.
	instanceA = "px-a"
	instanceB = "px-b"
	poolName  = "px-pool"
	className = "px-class"
	classTier = "px-standard"
	// sizingClass is the development tier: six postmasters have to fit on one node, and it
	// publishes 50 allocatable connections per instance.
	sizingClass = "dev-1"

	// The two tenants. Each name is both the PgTenant and its database, which is what the
	// proxy's DatabaseName discriminator reads off the startup packet.
	tenantAlpha = "alpha"
	tenantBeta  = "beta"

	// poolingClients is how many clients the pooling spec holds open at once.
	poolingClients = 12
	// tenantBurstable is at least the client count on purpose. The admission ladder refuses
	// a tenant at its own ceiling outright rather than queueing it — that is the PGE1928
	// rung, and it is correct — so a lower ceiling here would prove that the ladder works
	// and nothing at all about pooling. What the spec is for is the other claim: that a
	// client between statements holds no backend, so twelve of them need fewer than twelve.
	tenantBurstable = poolingClients

	// proxyReplicas is the fleet size the pool starts with.
	proxyReplicas = 2
	// scaledReplicas is what it is scaled to.
	scaledReplicas = 3

	instanceReadyTimeout = 15 * time.Minute
	fleetReadyTimeout    = 5 * time.Minute
)

var (
	primaryA string
	primaryB string
	endpoint *forwarder
)

var _ = Describe("the pool's inline proxy fleet", Ordered, func() {
	BeforeAll(func() {
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace},
		})).To(Succeed(),
			"namespace %s already exists; a previous run left it behind and this suite "+
				"refuses to reuse it rather than assert against somebody else's objects",
			e2eNamespace)

		elasticClass := &pgelasticv1alpha1.PgElasticClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: suiteControllerName},
		}
		Expect(k8sClient.Create(suiteCtx, elasticClass)).To(Succeed())

		workloadClass := &pgelasticv1alpha1.PgWorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: classTier},
			Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
				Priority: 1000,
				Capacity: pgelasticv1alpha1.WorkloadCapacity{
					Guaranteed: ptr.To(int32(1)),
					Burstable:  tenantBurstable,
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, workloadClass)).To(Succeed())

		pool := &pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: e2eNamespace},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				ClassRef: pgelasticv1alpha1.ClassReference{
					APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
					Kind:     "PgElasticClass",
					Name:     className,
				},
				Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 100},
				Instances: pgelasticv1alpha1.PoolInstances{
					Replicas: ptr.To(int32(2)),
					Template: instanceTemplate(),
				},
				Admission: &pgelasticv1alpha1.PoolAdmission{
					DefaultWorkloadClassName: classTier,
				},
				Pooling: &pgelasticv1alpha1.PoolingConfig{
					Mode: pgelasticv1alpha1.PoolModeTransaction,
				},
				Proxy: &pgelasticv1alpha1.ProxySpec{
					Replicas: ptr.To(int32(proxyReplicas)),
					Workers:  ptr.To(int32(2)),
					Routing: &pgelasticv1alpha1.ProxyRouting{
						// The pool's Service is one endpoint in front of every tenant, and
						// the database name is the only discriminator every PostgreSQL
						// client already sends.
						TenantDiscriminators: []pgelasticv1alpha1.TenantDiscriminator{
							pgelasticv1alpha1.DiscriminatorDatabaseName,
						},
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
				},
			},
		}
		// The members go first. A pool provisions the members it declares, so creating it
		// ahead of them opens a window in which it makes its own - and this suite wants the
		// two it writes by hand, not four.
		for _, name := range []string{instanceA, instanceB} {
			Expect(k8sClient.Create(suiteCtx, makeInstance(name))).To(Succeed())
		}

		Expect(k8sClient.Create(suiteCtx, pool)).To(Succeed())

		// The tenants go first, and the namespace only once they are gone. A PgTenant's
		// finalizer is released by the tenant controller, the controller reaches the tenant's
		// database through the instance hosting it, and the admission webhook refuses any
		// write to a tenant whose pool has been deleted. Deleting the namespace wholesale
		// takes all three away at once and leaves the namespace Terminating for ever.
		DeferCleanup(func() {
			for _, name := range []string{tenantAlpha, tenantBeta} {
				Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &pgelasticv1alpha1.PgTenant{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
				}))).To(Succeed())
			}
			for _, name := range []string{tenantAlpha, tenantBeta} {
				Eventually(func() bool {
					err := k8sClient.Get(suiteCtx, client.ObjectKey{
						Namespace: e2eNamespace, Name: name,
					}, &pgelasticv1alpha1.PgTenant{})
					return err != nil
				}).WithTimeout(3*time.Minute).WithPolling(2*time.Second).Should(BeTrue(),
					"%s never released its finalizer, so the namespace cannot be deleted", name)
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace},
			}))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, elasticClass))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, workloadClass))).To(Succeed())
		})
	})

	AfterAll(func() {
		if endpoint != nil {
			endpoint.close()
		}
	})

	It("brings up the two instances the tenants are placed across", func() {
		primaryA = awaitInstanceReady(instanceA)
		primaryB = awaitInstanceReady(instanceB)
		Expect(psql(primaryA, "postgres", "SELECT 1")).To(Equal("1"))
		Expect(psql(primaryB, "postgres", "SELECT 1")).To(Equal("1"))
	})

	It("places one tenant on each instance and creates its database there", func() {
		Expect(k8sClient.Create(suiteCtx, makeTenant(tenantAlpha, instanceA))).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, makeTenant(tenantBeta, instanceB))).To(Succeed())

		for _, name := range []string{tenantAlpha, tenantBeta} {
			Eventually(func(g Gomega) {
				tenant := &pgelasticv1alpha1.PgTenant{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: e2eNamespace, Name: name,
				}, tenant)).To(Succeed())
				ready := conditionOf(tenant.Status.Conditions, pgelasticv1alpha1.ConditionReady)
				g.Expect(ready.Status).To(Equal(metav1.ConditionTrue),
					"%s is %s: %s / %s", name, ready.Status, ready.Reason, ready.Message)
			}).WithTimeout(5 * time.Minute).Should(Succeed())
		}

		// PostgreSQL's own answer, not the CR's: each database exists on exactly the
		// instance its tenant was placed on, and nowhere else.
		Expect(psql(primaryA, "postgres", countDatabase(tenantAlpha))).To(Equal("1"))
		Expect(psql(primaryB, "postgres", countDatabase(tenantBeta))).To(Equal("1"))
		Expect(psql(primaryA, "postgres", countDatabase(tenantBeta))).To(Equal("0"))
		Expect(psql(primaryB, "postgres", countDatabase(tenantAlpha))).To(Equal("0"))

		seedMarker(primaryA, tenantAlpha)
		seedMarker(primaryB, tenantBeta)
	})

	It("creates the proxy Deployment and Service from spec.proxy and they become Ready", func() {
		awaitFleet(proxyReplicas)

		service := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: e2eNamespace, Name: proxyobjects.ServiceName(poolName),
		}, service)).To(Succeed())
		Expect(service.Spec.Ports).To(HaveLen(1))
		Expect(service.Spec.Ports[0].Port).To(Equal(int32(5432)))
	})

	It("reports the fleet, and the configuration it has converged on, in status.proxy", func() {
		Eventually(func(g Gomega) {
			fetched := fetchPool()
			g.Expect(fetched.Status.Proxy).NotTo(BeNil())
			g.Expect(fetched.Status.Proxy.Replicas).To(Equal(int32(proxyReplicas)))
			g.Expect(fetched.Status.Proxy.Ready).To(Equal(int32(proxyReplicas)))
			g.Expect(fetched.Status.Proxy.ConfigVersion).NotTo(BeEmpty(),
				"no configVersion means at least one ready replica is still serving "+
					"a different configuration")
			g.Expect(fetched.Status.Selector).
				To(Equal(proxyobjects.SelectorString(poolName)))
		}).WithTimeout(3 * time.Minute).Should(Succeed())

		// Forwarded only now. A port-forward follows one Pod, and until the fleet has
		// converged the operator is still rolling replicas as each instance publishes its
		// address and then its allocatable capacity: a forward opened earlier is attached to
		// a Pod that is about to be replaced.
		endpoint = forward(proxyobjects.ServiceName(poolName), 5432)
	})

	It("carries a client from the pool Service to a real query on its tenant database", func() {
		connection := connect(tenantAlpha)
		defer func() { _ = connection.Close(suiteCtx) }()

		var database string
		Expect(connection.QueryRow(suiteCtx, "SELECT current_database()").Scan(&database)).
			To(Succeed())
		Expect(database).To(Equal(tenantAlpha))

		var answer int
		Expect(connection.QueryRow(suiteCtx, "SELECT 6 * 7").Scan(&answer)).To(Succeed())
		Expect(answer).To(Equal(42))
	})

	// The client authenticates at the proxy as the control plane's role, and the statement
	// still arrives on the far side as the tenant's own. That substitution is what lets
	// pg_stat_activity, log_line_prefix and any audit extension attribute a statement to
	// whoever ran it, and until this spec existed nothing exercised it: the suite had been
	// granting its fixtures to the role the client dials with, so it went on passing for
	// three days after the proxy stopped using that role and then failed on the first
	// privileged read with no test naming the cause.
	It("puts the tenant's own role on the backend, not the role the client dialled with", func() {
		connection := connect(tenantAlpha)
		defer func() { _ = connection.Close(suiteCtx) }()

		var sessionUser, currentUser string
		Expect(connection.QueryRow(suiteCtx, "SELECT session_user, current_user").
			Scan(&sessionUser, &currentUser)).To(Succeed())

		Expect(sessionUser).To(Equal(backendRole(tenantAlpha)),
			"the backend is open as %s; a session that is still the control plane's role "+
				"attributes every tenant statement to pgelastic", sessionUser)
		Expect(currentUser).To(Equal(sessionUser),
			"SET ROLE moves current_user and leaves session_user behind, which is the "+
				"impersonation this deliberately does not use")
		Expect(sessionUser).NotTo(Equal(provision.OpsRole))
	})

	// The routing claim and the isolation claim are one spec because they are one property:
	// a connection reaches the instance holding its own tenant's data, and there is nothing
	// on that instance belonging to the other tenant for it to reach by mistake.
	It("routes two tenants on two instances through the same endpoint, each to its own", func() {
		alpha := connect(tenantAlpha)
		defer func() { _ = alpha.Close(suiteCtx) }()
		beta := connect(tenantBeta)
		defer func() { _ = beta.Close(suiteCtx) }()

		Expect(markerOn(alpha)).To(Equal(marker(tenantAlpha)))
		Expect(markerOn(beta)).To(Equal(marker(tenantBeta)))

		// Asked of PostgreSQL rather than of the proxy: each instance is serving the one
		// database it holds, and the other tenant's database is not present on it at all.
		Expect(psql(primaryA, "postgres", backendsFor(tenantAlpha))).NotTo(Equal("0"),
			"no backend for %s ever appeared on %s, so the client did not reach it",
			tenantAlpha, instanceA)
		Expect(psql(primaryB, "postgres", backendsFor(tenantBeta))).NotTo(Equal("0"),
			"no backend for %s ever appeared on %s", tenantBeta, instanceB)
		Expect(psql(primaryA, "postgres", backendsFor(tenantBeta))).To(Equal("0"),
			"%s served a backend for %s, which it does not hold", instanceA, tenantBeta)
		Expect(psql(primaryB, "postgres", backendsFor(tenantAlpha))).To(Equal("0"))

		// The other side of the isolation claim: neither connection can be pointed at the
		// other's data, because the other's database does not exist where it landed.
		var visible int
		Expect(alpha.QueryRow(suiteCtx,
			fmt.Sprintf("SELECT count(*) FROM pg_database WHERE datname = '%s'", tenantBeta)).
			Scan(&visible)).To(Succeed())
		Expect(visible).To(Equal(0),
			"the %s connection can see %s's database, so the two tenants are not on "+
				"separate instances", tenantAlpha, tenantBeta)
	})

	It("multiplexes many clients over fewer backends than clients", func() {
		ctx, cancel := context.WithCancel(suiteCtx)
		defer cancel()

		var running sync.WaitGroup
		var connected atomic.Int64
		var statements atomic.Int64
		failure := make(chan error, poolingClients)
		for range poolingClients {
			running.Add(1)
			go func() {
				defer GinkgoRecover()
				defer running.Done()
				// Staggered, and with a jittered pause between statements. Twelve clients
				// released together stay in lockstep and really are twelve concurrent
				// transactions, which needs twelve backends and is not a pooling failure —
				// it is twelve clients genuinely working at once. What transaction pooling
				// claims is the other case, and the common one: clients that spend most of
				// their time thinking hold no backend while they think.
				think := func() time.Duration {
					return time.Duration(200+rand.IntN(200)) * time.Millisecond
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(rand.IntN(800)) * time.Millisecond):
				}

				connection, err := pgx.Connect(ctx, endpoint.dsn(provision.OpsRole, tenantAlpha))
				if err != nil {
					failure <- err
					return
				}
				defer func() { _ = connection.Close(context.Background()) }()
				connected.Add(1)
				for ctx.Err() == nil {
					var ignored int
					if err := connection.QueryRow(ctx, "SELECT 1").Scan(&ignored); err != nil {
						if ctx.Err() == nil {
							failure <- err
						}
						return
					}
					statements.Add(1)
					select {
					case <-ctx.Done():
					case <-time.After(think()):
					}
				}
			}()
		}

		Eventually(func() int64 { return connected.Load() }).
			WithTimeout(time.Minute).WithPolling(200*time.Millisecond).
			Should(BeNumerically("==", poolingClients),
				"only %d of %d clients reached the pool", connected.Load(), poolingClients)

		// Sampled while every client is live and working, so what is counted is concurrent
		// backends rather than whatever is left once they have gone.
		var peak int
		for range 12 {
			backends, err := psql(primaryA, "postgres", backendsFor(tenantAlpha))
			Expect(err).NotTo(HaveOccurred())
			count, err := strconv.Atoi(backends)
			Expect(err).NotTo(HaveOccurred())
			peak = max(peak, count)
			time.Sleep(250 * time.Millisecond)
		}

		cancel()
		running.Wait()
		close(failure)

		var refused []string
		for err := range failure {
			refused = append(refused, err.Error())
		}
		Expect(refused).To(BeEmpty(),
			"%d of %d clients failed; multiplexing is supposed to admit them all",
			len(refused), poolingClients)
		Expect(statements.Load()).To(BeNumerically(">=", int64(poolingClients)),
			"the clients were connected but barely ran anything, so the backend count "+
				"below is not evidence of anything")
		AddReportEntry("peak backends", fmt.Sprintf("%d for %d clients", peak, poolingClients))

		Expect(peak).To(BeNumerically(">", 0),
			"no backend was ever open, so the clients were not reaching PostgreSQL")
		// Half is a ceiling with a great deal of room in it: at a fifth of a second of
		// thinking per sub-millisecond statement the expected concurrency is well under one,
		// so reaching six would take a slowdown of two orders of magnitude. Asserting merely
		// "fewer than twelve" would pass at eleven and prove nothing.
		Expect(peak).To(BeNumerically("<=", poolingClients/2),
			"%d clients held %d backends at once: the pool is not multiplexing, it is "+
				"passing through", poolingClients, peak)
	})

	// The control listener is the one endpoint that can hold a tenant's clients still. An
	// unauthenticated caller reaching it could stall any tenant at will, which is why the
	// listener was deliberately never rendered until it could prove who was calling.
	//
	// Three callers, one endpoint. The refusals are asserted as 401 rather than as a dropped
	// connection on purpose: a cutover that cannot authenticate has to be diagnosable.
	It("serves the cutover API only to the certificate the operator was issued", func() {
		certcheck.AwaitControlClientSecret(suiteCtx, k8sClient, e2eNamespace, poolName)

		operator := &corev1.Secret{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: e2eNamespace,
			Name:      proxyobjects.ControlClientSecretName(poolName),
		}, operator)).To(Succeed())

		pods := readyProxyPods(proxyobjects.ServiceName(poolName))
		Expect(pods).NotTo(BeEmpty())
		control := forwardPod(pods[0], proxyobjects.DefaultControlPort)
		defer control.close()

		roots := x509.NewCertPool()
		Expect(roots.AppendCertsFromPEM(operator.Data["ca.crt"])).To(BeTrue(),
			"the operator's Secret carries no issuing CA, so it cannot verify the listener")
		identity, err := tls.X509KeyPair(operator.Data["tls.crt"], operator.Data["tls.key"])
		Expect(err).NotTo(HaveOccurred())

		serverName := proxyobjects.ControlServerName(poolName, e2eNamespace)
		url := "https://" + control.address + "/instances"

		By("refusing a caller that presents no certificate at all")
		status, body := controlGet(url, roots, serverName, nil)
		Expect(status).To(Equal(http.StatusUnauthorized),
			"an unauthenticated caller reached the cutover API; body: %s", body)
		Expect(body).To(ContainSubstring("client certificate"))

		By("refusing a certificate issued by an authority the listener does not trust")
		status, body = controlGet(url, roots, serverName,
			ptr.To(selfSignedClient(proxyobjects.ControlClientName(poolName, e2eNamespace))))
		Expect(status).To(Equal(http.StatusUnauthorized),
			"a certificate from another authority was accepted; body: %s", body)

		By("serving the certificate cert-manager issued for this pool")
		status, body = controlGet(url, roots, serverName, &identity)
		Expect(status).To(Equal(http.StatusOK),
			"the operator's own certificate was refused; body: %s", body)
		Expect(body).To(ContainSubstring(instanceA),
			"the instances report does not name the fleet's members: %s", body)
	})

	// The failure mode no single-process pooler can see. Each replica reads one
	// configuration document carrying the undivided budget, so N replicas can hold N times
	// it against PostgreSQL — and the count that matters is the one on the instance, summed
	// over every replica, which is a question only PostgreSQL can answer.
	//
	// Load is driven through one forward per replica rather than through the Service:
	// kube-proxy pins a connection in conntrack for its life, so clients arriving at one
	// endpoint would prove nothing about the fleet.
	It("keeps fleet-wide backend connections inside the pool budget across every replica", func() {
		pods := readyProxyPods(proxyobjects.ServiceName(poolName))
		Expect(pods).To(HaveLen(proxyReplicas),
			"the fleet-wide claim needs every replica, and only %d are ready", len(pods))

		forwards := make([]*forwarder, 0, len(pods))
		for _, pod := range pods {
			forwards = append(forwards, forwardPod(pod, 5432))
		}
		defer func() {
			for _, endpoint := range forwards {
				endpoint.close()
			}
		}()

		ctx, cancel := context.WithCancel(suiteCtx)
		defer cancel()

		var running sync.WaitGroup
		var statements atomic.Int64
		// Exactly the tenant's ceiling per replica. One more would be refused outright rather
		// than queued — that is the admission ladder's tenant-cap rung and it is correct — so
		// this is the largest load the fleet is supposed to carry, which is what a claim about
		// the bound has to be measured under.
		const perReplica = tenantBurstable
		for _, target := range forwards {
			for range perReplica {
				running.Add(1)
				go func() {
					defer GinkgoRecover()
					defer running.Done()
					connection, err := pgx.Connect(ctx, target.dsn(provision.OpsRole, tenantAlpha))
					if err != nil {
						return
					}
					defer func() { _ = connection.Close(context.Background()) }()
					for ctx.Err() == nil {
						var ignored int
						if err := connection.QueryRow(ctx, "SELECT 1 FROM pg_sleep(0.02)").
							Scan(&ignored); err != nil {
							return
						}
						statements.Add(1)
					}
				}()
			}
		}

		var peak int
		Eventually(func() int64 { return statements.Load() }).
			WithTimeout(2*time.Minute).WithPolling(500*time.Millisecond).
			Should(BeNumerically(">", int64(perReplica)),
				"the load never started, so the count below is not evidence of anything")
		var replicasSeen int
		for range 20 {
			backends, err := psql(primaryA, "postgres", backendsFor(tenantAlpha))
			Expect(err).NotTo(HaveOccurred())
			count, err := strconv.Atoi(backends)
			Expect(err).NotTo(HaveOccurred())
			peak = max(peak, count)

			sources, err := psql(primaryA, "postgres", backendSourcesFor(tenantAlpha))
			Expect(err).NotTo(HaveOccurred())
			distinct, err := strconv.Atoi(sources)
			Expect(err).NotTo(HaveOccurred())
			replicasSeen = max(replicasSeen, distinct)

			time.Sleep(250 * time.Millisecond)
		}

		cancel()
		running.Wait()

		fetched := fetchPool()
		budget := fetched.Spec.Capacity.BackendConnections
		AddReportEntry("fleet-wide peak backends", fmt.Sprintf(
			"%d on %s across %d replicas, against a budget of %d",
			peak, instanceA, len(pods), budget))

		Expect(peak).To(BeNumerically(">", 0),
			"no backend was ever open, so the fleet was not reaching PostgreSQL")
		// Otherwise the budget could be respected merely because only one replica was ever
		// serving, which is the opposite of the thing under test. PostgreSQL answers it: each
		// replica is a different Pod and so a different client address.
		Expect(replicasSeen).To(Equal(len(pods)),
			"backends arrived from %d of %d replicas, so this is not a fleet-wide measurement",
			replicasSeen, len(pods))
		Expect(peak).To(BeNumerically("<=", int(budget)),
			"%d replicas opened %d backend connections on %s against a pool budget of %d; "+
				"every replica reads the undivided budget, so the fleet multiplied it",
			len(pods), peak, instanceA, budget)
		// The admission gate's own arithmetic, observed rather than asserted about itself: a
		// tenant cannot exceed its ceiling on any one replica, so the fleet cannot exceed
		// that ceiling times the replica count.
		Expect(peak).To(BeNumerically("<=", len(pods)*tenantBurstable),
			"%d backends is past replicas x burstable (%d x %d), which is the bound the "+
				"cross-replica gate computes admission against",
			peak, len(pods), tenantBurstable)

		Eventually(func(g Gomega) {
			status := fetchPool().Status.Proxy
			g.Expect(status).NotTo(BeNil())
			g.Expect(status.LeasedConnections).To(BeNumerically(">", 0),
				"leasedConnections still reads zero, which says nothing is leased while "+
					"%d replicas each hold the whole budget", len(pods))
		}).WithTimeout(2 * time.Minute).Should(Succeed())
	})

	It("scales the fleet when spec.proxy.replicas changes", func() {
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fetched := &pgelasticv1alpha1.PgElasticPool{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: e2eNamespace, Name: poolName,
			}, fetched); err != nil {
				return err
			}
			fetched.Spec.Proxy.Replicas = ptr.To(int32(scaledReplicas))
			return k8sClient.Update(suiteCtx, fetched)
		})).To(Succeed())

		awaitFleet(scaledReplicas)

		Eventually(func(g Gomega) {
			fetched := fetchPool()
			g.Expect(fetched.Status.Proxy).NotTo(BeNil())
			g.Expect(fetched.Status.Proxy.Ready).To(Equal(int32(scaledReplicas)))
		}).WithTimeout(3 * time.Minute).Should(Succeed())
	})

	// The whole point of splitting the document into a structural half and a dynamic one:
	// a tenant's claim changing must reach every replica without any of them restarting,
	// because a restart drops every client on the replica.
	It("propagates a configuration change without dropping an established connection", func() {
		held := connect(tenantAlpha)
		defer func() { _ = held.Close(suiteCtx) }()
		var before int
		Expect(held.QueryRow(suiteCtx, "SELECT 1").Scan(&before)).To(Succeed())

		podsBefore := proxyPodUIDs()
		Expect(podsBefore).To(HaveLen(scaledReplicas))
		versionBefore := publishedVersion()

		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			tenant := &pgelasticv1alpha1.PgTenant{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: e2eNamespace, Name: tenantAlpha,
			}, tenant); err != nil {
				return err
			}
			tenant.Spec.Capacity = &pgelasticv1alpha1.PgTenantCapacity{
				Guaranteed: ptr.To(int32(1)),
				Burstable:  ptr.To(int32(tenantBurstable + 2)),
			}
			return k8sClient.Update(suiteCtx, tenant)
		})).To(Succeed())

		// Two instants: when the operator's document changed, and when every replica had
		// adopted it. The gap between them is the propagation latency, and it is measured
		// rather than asserted at a number the implementation happens to hit today.
		var published string
		Eventually(func() string {
			published = publishedVersion()
			return published
		}).WithTimeout(2 * time.Minute).WithPolling(50 * time.Millisecond).
			ShouldNot(Equal(versionBefore))
		wrote := time.Now()

		Eventually(func(g Gomega) {
			applied := appliedVersions()
			g.Expect(applied).To(HaveLen(scaledReplicas))
			for pod, version := range applied {
				g.Expect(version).To(Equal(published), "%s is still serving %q", pod, version)
			}
		}).WithTimeout(2 * time.Minute).WithPolling(50 * time.Millisecond).Should(Succeed())
		latency := time.Since(wrote)

		AddReportEntry("config propagation latency", latency.String())
		GinkgoWriter.Printf("configuration %s reached every replica in %s\n", published, latency)

		Expect(proxyPodUIDs()).To(Equal(podsBefore),
			"a dynamic configuration change restarted the fleet, which drops every client "+
				"on every replica")

		var after int
		Expect(held.QueryRow(suiteCtx, "SELECT 42").Scan(&after)).To(Succeed(),
			"the connection held across the change was dropped by it")
		Expect(after).To(Equal(42))
		Expect(held.IsClosed()).To(BeFalse())

		Eventually(func(g Gomega) {
			fetched := fetchPool()
			g.Expect(fetched.Status.Proxy).NotTo(BeNil())
			g.Expect(fetched.Status.Proxy.ConfigVersion).To(Equal(published))
		}).WithTimeout(2 * time.Minute).Should(Succeed())
	})
	// The pod-template hatch patches the container the rendered configuration is mounted
	// into, and that document carries every tenant's password and backend SCRAM keys. The
	// webhook refuses this at admission; this asserts the half that still holds when the
	// webhook is not installed, which is one line of kustomize away.
	It("keeps the operator's image and command on the proxy container", func() {
		operatorImage := func() (string, []string) {
			GinkgoHelper()
			deployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: e2eNamespace, Name: proxyobjects.DeploymentName(poolName),
			}, deployment)).To(Succeed())
			for _, container := range deployment.Spec.Template.Spec.Containers {
				if container.Name == proxyobjects.ContainerName {
					return container.Image, container.Command
				}
			}
			Fail("the fleet Deployment carries no container named " + proxyobjects.ContainerName)
			return "", nil
		}
		before, _ := operatorImage()
		Expect(before).NotTo(BeEmpty())

		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fetched := &pgelasticv1alpha1.PgElasticPool{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: e2eNamespace, Name: poolName,
			}, fetched); err != nil {
				return err
			}
			fetched.Spec.Proxy.Template = &pgelasticv1alpha1.ProxyPodTemplate{
				Spec: &corev1.PodSpec{Containers: []corev1.Container{{
					Name:    proxyobjects.ContainerName,
					Image:   "busybox:latest",
					Command: []string{"sleep", "3600"},
				}}},
			}
			return k8sClient.Update(suiteCtx, fetched)
		})).To(Succeed())

		var patched map[string]types.UID
		Consistently(func(g Gomega) {
			patched = proxyPodUIDs()
			image, command := operatorImage()
			g.Expect(image).To(Equal(before),
				"the template replaced the proxy image, so an arbitrary process now runs "+
					"with every tenant's credentials mounted")
			g.Expect(command).To(BeEmpty(),
				"the template replaced the proxy command: %v", command)
		}).WithTimeout(30 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fetched := &pgelasticv1alpha1.PgElasticPool{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: e2eNamespace, Name: poolName,
			}, fetched); err != nil {
				return err
			}
			fetched.Spec.Proxy.Template = nil
			return k8sClient.Update(suiteCtx, fetched)
		})).To(Succeed())
		// The template is structural, so both the patch and its removal roll the fleet.
		// awaitFleet alone would race the Deployment controller: until it has observed the
		// new generation, ObservedGeneration still equals the old Generation and the fleet
		// reads as settled when it has not started. So wait for the pod set to turn over
		// first. This spec runs last in the container for the same reason.
		Eventually(proxyPodUIDs).WithTimeout(fleetReadyTimeout).
			WithPolling(2 * time.Second).ShouldNot(Equal(patched))
		awaitFleet(scaledReplicas)
	})

})

func instanceTemplate() pgelasticv1alpha1.PgInstanceTemplate {
	return pgelasticv1alpha1.PgInstanceTemplate{
		Class: sizingClass,
		Storage: pgelasticv1alpha1.InstanceStorage{
			Size:      resource.MustParse("1Gi"),
			WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
		},
	}
}

func makeInstance(name string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: poolName},
			Class:   sizingClass,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      resource.MustParse("1Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
			},
		},
	}
}

// makeTenant pins the tenant to one instance. The placement scheduler would spread these two
// across the pool anyway, but this suite is about routing rather than about placement, and a
// spec that asserts "alpha is on px-a" has to be the one that decided it.
func makeTenant(name, instance string) *pgelasticv1alpha1.PgTenant {
	return &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: poolName},
			DatabaseName: name,
			Placement: &pgelasticv1alpha1.PgTenantPlacement{
				InstanceRef: &corev1.LocalObjectReference{Name: instance},
			},
		},
	}
}

// awaitFleet waits for the Deployment to have finished rolling, not merely to have enough
// ready replicas. The difference matters: while the operator is still replacing replicas
// because an instance has just published its address, a Deployment briefly reports the old
// generation's replicas as ready, and anything that attaches to one of those Pods is about
// to lose it.
func awaitFleet(replicas int32) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		deployment := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: e2eNamespace, Name: proxyobjects.DeploymentName(poolName),
		}, deployment)).To(Succeed())
		g.Expect(deployment.Status.ObservedGeneration).To(Equal(deployment.Generation))
		g.Expect(deployment.Status.Replicas).To(Equal(replicas))
		g.Expect(deployment.Status.UpdatedReplicas).To(Equal(replicas))
		g.Expect(deployment.Status.ReadyReplicas).To(Equal(replicas),
			"the fleet has %d/%d replicas ready", deployment.Status.ReadyReplicas, replicas)
	}).WithTimeout(fleetReadyTimeout).Should(Succeed())
}

func awaitInstanceReady(name string) string {
	GinkgoHelper()
	var primary string
	Eventually(func(g Gomega) {
		instance := &pgelasticv1alpha1.PgInstance{}
		g.Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, instance)).To(Succeed())
		g.Expect(instance.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
		g.Expect(instance.Status.CurrentPrimary).NotTo(BeEmpty())
		primary = instance.Status.CurrentPrimary
	}).WithTimeout(instanceReadyTimeout).Should(Succeed(), "%s never became ready", name)
	return primary
}

// seedMarker puts one row in each tenant's database that names the database it is in, and
// grants the role the proxy will actually arrive as enough to read it.
//
// That is the tenant's own backend role, not the role the client dials with: the proxy
// assumes the tenant's identity on the backend leg. The grant is explicit and goes to that
// role alone, so a proxy that regressed to dialling on the control plane's identity would
// fail this read rather than pass it - which is the failure the suite missed the first time.
func seedMarker(member, database string) {
	GinkgoHelper()
	role := backendRole(database)
	_, err := psql(member, database, fmt.Sprintf(
		`CREATE TABLE marker (tag text NOT NULL);
		 INSERT INTO marker VALUES ('%s');
		 GRANT USAGE ON SCHEMA public TO %s;
		 GRANT SELECT ON marker TO %s`,
		marker(database), role, role))
	Expect(err).NotTo(HaveOccurred())
}

// backendRole is the role the proxy opens a backend as for one tenant. The suite's tenants
// are named after their databases, which is what makes this derivable here.
func backendRole(database string) string {
	return migration.BackendRoleName(e2eNamespace, database)
}

func marker(database string) string { return database + "-marker" }

func markerOn(connection *pgx.Conn) string {
	GinkgoHelper()
	var tag string
	Expect(connection.QueryRow(suiteCtx, "SELECT tag FROM marker").Scan(&tag)).To(Succeed())
	return tag
}

// connect dials the pool's Service, which is the only route these specs ever take to
// PostgreSQL. The role dialled is the control plane's, because the tenant roles pgelastic
// creates have no password a client could hold - that is a property of the tenant path rather
// than of the proxy, and is not what this suite is testing. What the proxy does with it is:
// the backend leg arrives as the tenant's own role, which the spec above asserts.
func connect(database string) *pgx.Conn {
	GinkgoHelper()
	var connection *pgx.Conn
	Eventually(func() error {
		opened, err := pgx.Connect(suiteCtx, endpoint.dsn(provision.OpsRole, database))
		if err != nil {
			return err
		}
		connection = opened
		return nil
	}).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed(),
		"connecting to %s through the pool Service; the forward said:\n%s",
		database, endpoint.log())
	return connection
}

func fetchPool() *pgelasticv1alpha1.PgElasticPool {
	GinkgoHelper()
	fetched := &pgelasticv1alpha1.PgElasticPool{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: poolName}, fetched)).To(Succeed())
	return fetched
}

// publishedVersion is the configVersion the operator has written into the Secret the fleet
// reads, which is the instant a change reaches the data plane's doorstep.
func publishedVersion() string {
	GinkgoHelper()
	secret := &corev1.Secret{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: e2eNamespace, Name: proxyobjects.ConfigSecretName(poolName),
	}, secret)).To(Succeed())
	document := string(secret.Data[proxyobjects.ConfigKey])
	const prefix = "configVersion = \""
	start := len(prefix)
	Expect(document).To(HavePrefix(prefix))
	end := start
	for end < len(document) && document[end] != '"' {
		end++
	}
	return document[start:end]
}

// appliedVersions is what each replica says it is serving, read off the annotation the
// replica writes onto its own Pod. It is the only ground truth for whether a change reached
// the data plane; the operator's status is derived from it.
func appliedVersions() map[string]string {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace),
		client.MatchingLabels(proxyobjects.Selector(poolName)))).To(Succeed())
	applied := map[string]string{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || !podReady(pod) {
			continue
		}
		applied[pod.Name] = pod.Annotations[proxyobjects.AnnotationAppliedVersion]
	}
	return applied
}

func proxyPodUIDs() map[string]types.UID {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace),
		client.MatchingLabels(proxyobjects.Selector(poolName)))).To(Succeed())
	uids := map[string]types.UID{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || !podReady(pod) {
			continue
		}
		uids[pod.Name] = pod.UID
	}
	return uids
}

func conditionOf(conditions []metav1.Condition, name string) metav1.Condition {
	for _, condition := range conditions {
		if condition.Type == name {
			return condition
		}
	}
	return metav1.Condition{Type: name, Status: metav1.ConditionUnknown, Reason: "NotFound"}
}

func countDatabase(name string) string {
	return fmt.Sprintf(`SELECT count(*) FROM pg_database WHERE datname = '%s'`, name)
}

// backendSourcesFor counts how many distinct proxy replicas currently hold a backend on one
// database. Each replica is its own Pod and so its own client address, which is how a
// fleet-wide measurement is told apart from one replica's.
func backendSourcesFor(database string) string {
	return fmt.Sprintf(
		`SELECT count(DISTINCT client_addr) FROM pg_stat_activity `+
			`WHERE datname = '%s' AND usename = '%s'`,
		database, backendRole(database))
}

// backendsFor counts the backends the proxy currently holds on one database. It is the
// question the whole pooling claim reduces to, and only PostgreSQL can answer it.
//
// It counts by the tenant's own role because that is who the backend is open as. Counting by
// the control plane's role instead answers zero for every tenant, which reads as "the proxy
// opened nothing" and would pass the isolation assertions for the wrong reason.
func backendsFor(database string) string {
	return fmt.Sprintf(
		`SELECT count(*) FROM pg_stat_activity WHERE datname = '%s' AND usename = '%s'`,
		database, backendRole(database))
}

// controlGet issues one request at the control listener and answers with the status and the
// body, which is where the refusal names its cause.
//
// The server name is set explicitly because the request goes through a port-forward on
// 127.0.0.1 while the listener's certificate carries the fleet's Service name. Verifying it
// is the point: a listener that presented anything at all would be one the operator could be
// impersonated to.
func controlGet(
	url string,
	roots *x509.CertPool,
	serverName string,
	identity *tls.Certificate,
) (int, string) {
	GinkgoHelper()
	config := &tls.Config{
		RootCAs:    roots,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}
	if identity != nil {
		config.Certificates = []tls.Certificate{*identity}
	}
	caller := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: config},
	}
	defer caller.CloseIdleConnections()

	var status int
	var body string
	Eventually(func() error {
		request, err := http.NewRequestWithContext(suiteCtx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		response, err := caller.Do(request)
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		payload, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		status, body = response.StatusCode, string(payload)
		return nil
	}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(Succeed(),
		"the control listener at %s never answered", url)
	return status, body
}

// selfSignedClient is a client certificate carrying the right name and the wrong issuer,
// which is the misconfiguration the name check alone would miss.
func selfSignedClient(name string) tls.Certificate {
	GinkgoHelper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	Expect(err).NotTo(HaveOccurred())

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
