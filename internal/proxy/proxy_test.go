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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	testImage    = "proxy:test"
	testInstance = "pg-a"
	testOpsRole  = "ops"
)

func testPool() *pgelasticv1alpha1.PgElasticPool {
	return &pgelasticv1alpha1.PgElasticPool{
		ObjectMeta: metav1.ObjectMeta{Name: "saas", Namespace: "prod", Generation: 7},
		Spec: pgelasticv1alpha1.PgElasticPoolSpec{
			Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 100},
			Proxy:    &pgelasticv1alpha1.ProxySpec{},
		},
	}
}

func testConfig() Config {
	return Config{
		Pool: testPool(),
		Instances: []Instance{
			{Name: "pg-b", Address: "pg-b-rw.prod.svc:5432", User: testOpsRole, Password: "b"},
			{Name: testInstance, Address: "pg-a-rw.prod.svc:5432", User: testOpsRole, Password: "a"},
		},
		Tenants: []Tenant{
			{Name: "orders", Instance: "pg-b", Guaranteed: 2, Burstable: 8, Weight: 100},
			{Name: "billing", Instance: testInstance, Guaranteed: 1, Burstable: 4, Weight: 100},
		},
	}
}

func TestRenderIsByteIdenticalAcrossPasses(t *testing.T) {
	config := testConfig()
	first := config.Render()

	shuffled := testConfig()
	shuffled.Instances[0], shuffled.Instances[1] = shuffled.Instances[1], shuffled.Instances[0]
	shuffled.Tenants[0], shuffled.Tenants[1] = shuffled.Tenants[1], shuffled.Tenants[0]
	second := shuffled.Render()

	if first.TOML != second.TOML {
		t.Fatalf("the same pool rendered two different documents:\n%s\n---\n%s",
			first.TOML, second.TOML)
	}
	if first.StructuralHash != second.StructuralHash {
		t.Fatalf("the same pool produced two structural hashes: %q and %q",
			first.StructuralHash, second.StructuralHash)
	}
}

func TestAddingATenantRewritesTheDocumentWithoutRollingTheFleet(t *testing.T) {
	before := testConfig().Render()

	config := testConfig()
	config.Tenants = append(config.Tenants,
		Tenant{Name: "shipping", Instance: testInstance, Burstable: 4, Weight: 100})
	after := config.Render()

	if before.TOML == after.TOML {
		t.Fatal("a new tenant did not reach the document the fleet reads")
	}
	if before.StructuralHash != after.StructuralHash {
		t.Fatalf("a new tenant changed the pod template hash from %q to %q, "+
			"which restarts every replica and drops every client on it",
			before.StructuralHash, after.StructuralHash)
	}
}

func TestMovingATenantToAnotherInstanceDoesNotRollTheFleet(t *testing.T) {
	before := testConfig().Render()

	config := testConfig()
	config.Tenants[0].Instance = testInstance
	after := config.Render()

	if before.StructuralHash != after.StructuralHash {
		t.Fatal("a routing change restarted the fleet it was supposed to be adopted by")
	}
	if !strings.Contains(after.TOML, `"orders" = "pg-a"`) {
		t.Fatalf("the routing table does not carry the move:\n%s", after.TOML)
	}
}

func TestAddingAnInstanceRollsTheFleet(t *testing.T) {
	before := testConfig().Render()

	config := testConfig()
	config.Instances = append(config.Instances,
		Instance{Name: "pg-c", Address: "pg-c-rw.prod.svc:5432", User: testOpsRole, Password: "c"})
	after := config.Render()

	if before.StructuralHash == after.StructuralHash {
		t.Fatal("a new instance left the pod template unchanged, so no replica would ever " +
			"build a pool for it")
	}
}

func TestEveryInstanceCarriesItsOwnCredentials(t *testing.T) {
	document := testConfig().Render().TOML
	if !strings.Contains(document, `password = "a"`) ||
		!strings.Contains(document, `password = "b"`) {
		t.Fatalf("an instance lost the credentials its own bootstrap issued:\n%s", document)
	}
}

func TestTheVersionChangesWithTheDocumentAndNamesTheGeneration(t *testing.T) {
	before := testConfig().Render()
	config := testConfig()
	config.Tenants[0].Burstable = 16
	after := config.Render()

	if !strings.HasPrefix(before.Version, "7-") {
		t.Fatalf("the version does not name the generation it was rendered from: %q",
			before.Version)
	}
	if before.Version == after.Version {
		t.Fatal("two different documents carry the same version, so a replica cannot tell " +
			"whether it has picked the change up")
	}
	if !strings.Contains(after.TOML, "configVersion = "+`"`+after.Version+`"`) {
		t.Fatal("the document does not carry the version it is identified by")
	}
}

func TestAPasswordWithAQuoteInItStillParsesAsTOML(t *testing.T) {
	config := testConfig()
	config.Instances[0].Password = `he said "hi"\and then`
	document := config.Render().TOML
	if !strings.Contains(document, `password = "he said \"hi\"\\and then"`) {
		t.Fatalf("a password was written in a form no TOML parser accepts:\n%s", document)
	}
}

func TestTheDiscriminatorsRenderedAreTheOnesThePoolAskedFor(t *testing.T) {
	config := testConfig()
	config.Pool.Spec.Proxy.Routing = &pgelasticv1alpha1.ProxyRouting{
		TenantDiscriminators: []pgelasticv1alpha1.TenantDiscriminator{
			pgelasticv1alpha1.DiscriminatorDatabaseName,
		},
	}
	document := config.Render().TOML
	if !strings.Contains(document, `tenantDiscriminators = ["DatabaseName"]`) {
		t.Fatalf("the pool's discriminator list did not reach the fleet:\n%s", document)
	}
}

func TestAPoolThatNamesNoDiscriminatorsGetsTheApiDefault(t *testing.T) {
	document := testConfig().Render().TOML
	if !strings.Contains(document,
		`tenantDiscriminators = ["SNI", "StartupOptions", "DatabaseName"]`) {
		t.Fatalf("the default discriminator list is not the API's:\n%s", document)
	}
}

func TestAnUnplacedTenantHoldsItsClaimButNoRoute(t *testing.T) {
	config := testConfig()
	config.Tenants[0].Instance = ""
	document := config.Render().TOML
	if strings.Contains(document, `"orders" = `) {
		t.Fatalf("a tenant with nowhere to go was given a route anyway:\n%s", document)
	}
	if !strings.Contains(document, `name = "orders"`) {
		t.Fatalf("an unplaced tenant lost the capacity it has already been promised:\n%s",
			document)
	}
}

func TestTheServiceCarriesOnlyTheClientPort(t *testing.T) {
	builder := Builder{Pool: testPool(), Image: testImage}
	service := builder.Service()
	if len(service.Spec.Ports) != 1 {
		t.Fatalf("the client Service publishes %d ports; the metrics endpoint is not a "+
			"tenant-visible fact", len(service.Spec.Ports))
	}
	if service.Spec.Ports[0].Port != 5432 {
		t.Fatalf("the pool Service does not answer on 5432: %d", service.Spec.Ports[0].Port)
	}
}

func TestReadinessIsAdminStateAndNeverABareTcpProbe(t *testing.T) {
	builder := Builder{Pool: testPool(), Image: testImage}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}
	probe := deployment.Spec.Template.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.HTTPGet == nil {
		t.Fatal("the readiness probe is not an admin-state check")
	}
	if probe.TCPSocket != nil {
		t.Fatal("a bare TCP probe marks a replica ready while every client on it would be " +
			"refused")
	}
	if probe.HTTPGet.Path != "/readyz" {
		t.Fatalf("the readiness probe asks for %q", probe.HTTPGet.Path)
	}
	if deployment.Spec.Template.Spec.Containers[0].LivenessProbe != nil {
		t.Fatal("a liveness probe is on by default; a restart drops every client on the " +
			"replica")
	}
}

func TestARolloutNeverSurges(t *testing.T) {
	builder := Builder{Pool: testPool(), Image: testImage}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}
	surge := deployment.Spec.Strategy.RollingUpdate.MaxSurge
	if surge == nil || surge.IntValue() != 0 {
		t.Fatalf("maxSurge is %v; a surge runs two fleets at once and doubles the pool's "+
			"backend usage", surge)
	}
}

func TestTheFleetHoldsTerminationOpenLongEnoughToDrain(t *testing.T) {
	pool := testPool()
	pool.Spec.Proxy.TerminationGracePeriodSeconds = ptr.To(int64(200))
	pool.Spec.Proxy.Drain = &pgelasticv1alpha1.ProxyDrain{
		PreStopDelay: &metav1.Duration{Duration: 30_000_000_000},
	}
	builder := Builder{Pool: pool, Image: testImage}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}
	spec := deployment.Spec.Template.Spec
	if spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 200 {
		t.Fatalf("the grace period is %v", spec.TerminationGracePeriodSeconds)
	}
	sleep := spec.Containers[0].Lifecycle.PreStop.Sleep
	if sleep == nil || sleep.Seconds != 30 {
		t.Fatalf("the pre-stop delay is %v; without it clients keep arriving at a replica "+
			"that has already begun to drain", sleep)
	}
}

func TestTheTemplateAddsToTheProxyContainerRatherThanReplacingIt(t *testing.T) {
	pool := testPool()
	pool.Spec.Proxy.Template = &pgelasticv1alpha1.ProxyPodTemplate{
		Metadata: &pgelasticv1alpha1.ProxyPodTemplateMetadata{
			Labels:      map[string]string{"team": "platform", LabelComponent: "hijacked"},
			Annotations: map[string]string{"note": "kept"},
		},
		Spec: &corev1.PodSpec{
			NodeSelector: map[string]string{"disktype": "nvme"},
			Containers: []corev1.Container{{
				Name: ContainerName,
				Env:  []corev1.EnvVar{{Name: "RUST_LOG", Value: "debug"}},
			}},
		},
	}
	builder := Builder{
		Pool:     pool,
		Image:    testImage,
		Document: Config{Pool: pool}.Render(),
	}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}

	template := deployment.Spec.Template
	if template.Labels["team"] != "platform" {
		t.Fatal("the template's own labels were dropped")
	}
	if template.Labels[LabelComponent] != ComponentProxy {
		t.Fatal("a template label overwrote the selector, so the Deployment would not " +
			"select its own pods")
	}
	if template.Annotations["note"] != "kept" {
		t.Fatal("the template's annotations were dropped")
	}
	if template.Annotations[AnnotationConfigHash] == "" {
		t.Fatal("the configuration hash was lost to the template")
	}

	spec := template.Spec
	if spec.NodeSelector["disktype"] != "nvme" {
		t.Fatal("the template's node selector was dropped")
	}
	if len(spec.Containers) != 1 {
		t.Fatalf("the template produced %d containers", len(spec.Containers))
	}
	container := spec.Containers[0]
	if container.Image != testImage {
		t.Fatalf("the merge replaced the proxy container rather than adding to it: image %q",
			container.Image)
	}
	if container.ReadinessProbe == nil {
		t.Fatal("the merge dropped the readiness probe")
	}
	var found bool
	for _, env := range container.Env {
		if env.Name == "RUST_LOG" && env.Value == "debug" {
			found = true
		}
	}
	if !found {
		t.Fatal("the template's env var never reached the proxy container")
	}
}

func TestATemplateThatOverridesNothingLeavesTheContainerIntact(t *testing.T) {
	pool := testPool()
	pool.Spec.Proxy.Template = &pgelasticv1alpha1.ProxyPodTemplate{Spec: &corev1.PodSpec{}}
	builder := Builder{Pool: pool, Image: testImage}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatal("an empty template deleted the proxy container: PodSpec.Containers has no " +
			"omitempty, so it marshals to an explicit null a strategic merge reads as a " +
			"deletion")
	}
}

func TestTheBudgetDefaultsToOneUnavailableWhenThePoolNamesNeitherField(t *testing.T) {
	builder := Builder{Pool: testPool(), Image: testImage}
	budget := builder.PodDisruptionBudget()
	if budget.Spec.MaxUnavailable == nil || budget.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("maxUnavailable is %v", budget.Spec.MaxUnavailable)
	}
	if budget.Spec.MinAvailable != nil {
		t.Fatal("both fields were set, which the API refuses as mutually exclusive")
	}
}

func TestAPoolThatNamesMinAvailableGetsOnlyThat(t *testing.T) {
	pool := testPool()
	pool.Spec.Proxy.PodDisruptionBudget = &pgelasticv1alpha1.ProxyPodDisruptionBudget{
		MinAvailable: ptr.To(intstr.FromInt32(2)),
	}
	budget := Builder{Pool: pool, Image: testImage}.PodDisruptionBudget()
	if budget.Spec.MaxUnavailable != nil {
		t.Fatal("maxUnavailable was injected alongside minAvailable, which the API rejects")
	}
	if budget.Spec.MinAvailable.IntValue() != 2 {
		t.Fatalf("minAvailable is %v", budget.Spec.MinAvailable)
	}
}

func TestTheWorkerCountIsDeclaredRatherThanDerivedFromTheHost(t *testing.T) {
	pool := testPool()
	pool.Spec.Proxy.Workers = ptr.To(int32(4))
	builder := Builder{Pool: pool, Image: testImage}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}
	var workers string
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "TOKIO_WORKER_THREADS" {
			workers = env.Value
		}
	}
	if workers != "4" {
		t.Fatalf("the runtime worker count is %q; a pod that spawns one worker per host "+
			"core under a CPU limit spends its quota on CFS throttling", workers)
	}
}

func TestTheFleetCanReadItsOwnConfigurationAndNoOtherSecret(t *testing.T) {
	role := Builder{Pool: testPool(), Image: testImage}.Role()
	var secretRule *int
	for i := range role.Rules {
		for _, resource := range role.Rules[i].Resources {
			if resource == "secrets" {
				secretRule = &i
			}
		}
	}
	if secretRule == nil {
		t.Fatal("the fleet cannot read the configuration it is told to poll")
	}
	rule := role.Rules[*secretRule]
	if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != ConfigSecretName("saas") {
		t.Fatalf("the Secret grant is not restricted to the fleet's own configuration: %v",
			rule.ResourceNames)
	}
	for _, verb := range rule.Verbs {
		if verb == "list" || verb == "watch" {
			t.Fatal("a list or watch cannot be restricted by resourceName, so this grant " +
				"reads every Secret in the namespace")
		}
	}
}

func TestTheConfigurationVolumeIsMountedReadOnly(t *testing.T) {
	builder := Builder{Pool: testPool(), Image: testImage}
	deployment, err := builder.Deployment()
	if err != nil {
		t.Fatal(err)
	}
	mounts := deployment.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 1 || mounts[0].MountPath != ConfigDir || !mounts[0].ReadOnly {
		t.Fatalf("the configuration is not mounted read-only at %s: %v", ConfigDir, mounts)
	}
}
