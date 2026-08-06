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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	roundTripNamespace    = "pgelastic-roundtrip"
	defaultControllerName = "pgelastic.io/elastic-pool-controller"
	elasticClassKind      = "PgElasticClass"
	elasticClassName      = "saas-shared-gp"
	poolName              = "saas-pool"
	primaryInstanceName   = "pg-a"
	targetInstanceName    = "pg-b"
	tenantName            = "acme-prod"
	tenantDatabaseName    = "acme_prod"
	instanceClassName     = "gp-8"
	storageClassName      = "fast-nvme"
	postgresVersion       = "18"
	backupSchedule        = "0 2 * * *"
	backupRetention       = "30d"
	objectStoreRegion     = "eu-west-1"
	extraFloatDigitsParam = "extra_float_digits"
	optionsParam          = "options"
	standardClassName     = "tenant-standard"
	premiumClassName      = "tenant-premium"
	quarantineClassName   = "tenant-quarantine"
	observationWindow     = 168 * time.Hour
)

var strictFieldValidation = client.FieldValidation(metav1.FieldValidationStrict)

func ptrTo[T any](v T) *T { return &v }

func quantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func duration(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

// divergences reports every path at which roundTripped fails to reproduce submitted.
// A missing key is the signature of CRD schema pruning; a changed scalar is the
// signature of a lossy defaulter or a normalising conversion.
func divergences(path string, submitted, roundTripped any) []string {
	switch want := submitted.(type) {
	case map[string]any:
		got, ok := roundTripped.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s: object replaced by %T", path, roundTripped)}
		}
		out := make([]string, 0, len(want))
		for key, value := range want {
			gotValue, present := got[key]
			if !present {
				out = append(out, fmt.Sprintf("%s.%s: pruned", path, key))
				continue
			}
			out = append(out, divergences(path+"."+key, value, gotValue)...)
		}
		slices.Sort(out)
		return out
	case []any:
		got, ok := roundTripped.([]any)
		if !ok {
			return []string{fmt.Sprintf("%s: list replaced by %T", path, roundTripped)}
		}
		if len(got) != len(want) {
			return []string{fmt.Sprintf("%s: length %d became %d", path, len(want), len(got))}
		}
		out := make([]string, 0, len(want))
		for i := range want {
			out = append(out, divergences(fmt.Sprintf("%s[%d]", path, i), want[i], got[i])...)
		}
		return out
	default:
		if !reflect.DeepEqual(submitted, roundTripped) {
			return []string{fmt.Sprintf("%s: %#v became %#v", path, submitted, roundTripped)}
		}
		return nil
	}
}

func asJSONValue(v any) any {
	GinkgoHelper()
	encoded, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())
	var decoded any
	Expect(json.Unmarshal(encoded, &decoded)).To(Succeed())
	return decoded
}

func expectNoPruning(submittedSpec, roundTrippedSpec any) {
	GinkgoHelper()
	Expect(divergences("spec", asJSONValue(submittedSpec), asJSONValue(roundTrippedSpec))).To(BeEmpty())
}

var _ = Describe("v1alpha1 CRD round-trip", Ordered, func() {
	ctx := context.Background()

	namespacedName := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: roundTripNamespace}
	}

	objectMeta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: roundTripNamespace}
	}

	poolRef := corev1.LocalObjectReference{Name: poolName}

	BeforeAll(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: roundTripNamespace}}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, namespace))).To(Succeed())
	})

	Describe("a full-fidelity object graph", Ordered, func() {
		It("preserves every field of the PgElasticClass", func() {
			class := &pgelasticv1alpha1.PgElasticClass{
				ObjectMeta: metav1.ObjectMeta{Name: elasticClassName},
				Spec: pgelasticv1alpha1.PgElasticClassSpec{
					ControllerName: defaultControllerName,
					Tier:           pgelasticv1alpha1.TierGeneralPurpose,
					TenancyModel:   pgelasticv1alpha1.TenancySharedInstance,
					Description:    ptrTo("shared general-purpose pools for the SaaS estate"),
					CapacityModel: &pgelasticv1alpha1.ElasticClassCapacityModel{
						Unit:        pgelasticv1alpha1.CapacityBackendConnections,
						Enforcement: pgelasticv1alpha1.EnforcementProxyLeaseWithDatabaseBackstop,
						MinFloor:    ptrTo(int32(2)),
						ScoringDimensions: []pgelasticv1alpha1.CapacityScoringDimension{
							{Name: pgelasticv1alpha1.PlacementDimensionBackendConnections, Weight: ptrTo(int32(1000))},
							{Name: pgelasticv1alpha1.PlacementDimensionStorageBytes, Weight: ptrTo(int32(400))},
							{Name: pgelasticv1alpha1.PlacementDimensionRelationCount, Weight: ptrTo(int32(100))},
						},
					},
					Density: &pgelasticv1alpha1.ElasticClassDensity{
						MaxTenantsPerInstance:            ptrTo(int32(250)),
						MaxTenantsPerPool:                ptrTo(int32(1000)),
						MaxInstancesPerPool:              ptrTo(int32(16)),
						MaxBackendConnectionsPerInstance: ptrTo(int32(500)),
						MaxRelationsPerInstance:          ptrTo(int32(200000)),
						MaxStoragePerTenant:              quantity("500Gi"),
					},
					Governance: &pgelasticv1alpha1.ElasticClassGovernance{
						Tier1Proxy: &pgelasticv1alpha1.Tier1ProxyGovernance{
							Enforced: []pgelasticv1alpha1.Tier1Control{
								"MaxBackendConnections", "GuaranteedFloor", "AdmissionQueue",
								"QueryDeadline", "MaxResultBytes", "TransactionRateLimit",
								"ByteRateLimit", "MaxClientConnections", "CancelBurstCredit",
							},
						},
						Tier2Postgres: &pgelasticv1alpha1.Tier2PostgresGovernance{
							Advisory: []pgelasticv1alpha1.Tier2Control{
								"StatementTimeout", "IdleInTransactionSessionTimeout",
								"IdleSessionTimeout", "LockTimeout", "TempFileLimit",
							},
							ReapplyOnCheckout: ptrTo(true),
						},
						Tier3OS: &pgelasticv1alpha1.Tier3OSGovernance{
							Enforced:        []pgelasticv1alpha1.Tier3Control{"CPUWeight", "CPUMax"},
							NeverPerBackend: []pgelasticv1alpha1.Tier3Control{"MemoryMax", "MemoryHigh", "IOMax", "IOWeight"},
						},
						Storage: &pgelasticv1alpha1.StorageGovernance{
							DiskQuota: &pgelasticv1alpha1.DiskQuotaGovernance{
								Enabled:       ptrTo(true),
								Naptime:       duration(2 * time.Second),
								WarnAtPercent: ptrTo(int32(90)),
								HardAction:    "ReadOnly",
							},
						},
					},
					Defaults: &pgelasticv1alpha1.ElasticClassDefaults{
						HeadroomPercent:          ptrTo(int32(25)),
						MigrationHeadroomPercent: ptrTo(int32(10)),
						Admission: &pgelasticv1alpha1.ElasticClassAdmissionDefaults{
							Strategy:                  "WeightedDeficit",
							QueueDepthPerTenant:       ptrTo(int32(64)),
							MaxWait:                   duration(30 * time.Second),
							NotifyAfter:               duration(5 * time.Second),
							ReservationMode:           "Strict",
							DefaultWorkloadClassName:  ptrTo(standardClassName),
							AllowedWorkloadClassNames: []string{standardClassName, premiumClassName, quarantineClassName},
							RequireQuarantine:         ptrTo(true),
							QuarantineWindow:          duration(observationWindow),
						},
						Pooling: &pgelasticv1alpha1.ElasticClassPoolingDefaults{
							Mode:                     pgelasticv1alpha1.PoolModeTransaction,
							PreparedStatements:       "Extended",
							PreparedStatementsLimit:  ptrTo(int32(1000)),
							ServerIdleTimeout:        duration(600 * time.Second),
							ServerLifetime:           duration(3600 * time.Second),
							ServerLifetimeJitter:     duration(300 * time.Second),
							IdleSelection:            "Lifo",
							ResetMode:                pgelasticv1alpha1.ResetDirtyTracked,
							TrackExtraParameters:     []string{"IntervalStyle", "search_path"},
							IgnoreStartupParameters:  []string{extraFloatDigitsParam, optionsParam},
							StartupParameterPolicy:   pgelasticv1alpha1.StartupParameterPoolKey,
							PinningPolicy:            pgelasticv1alpha1.PinningPin,
							MaxPinnedFractionPercent: ptrTo(int32(20)),
							MaxPinDuration:           duration(time.Hour),
						},
						Placement: &pgelasticv1alpha1.ElasticClassPlacementDefaults{
							Strategy:          "BestFitDecreasing",
							PackOnPercentile:  "P95",
							ObservationWindow: duration(observationWindow),
							MaxSkewTenants:    ptrTo(int32(15)),
						},
						Rebalancing: &pgelasticv1alpha1.ElasticClassRebalancingDefaults{
							Enabled:                              ptrTo(true),
							Mode:                                 "ColdTenantsOnly",
							EvaluationInterval:                   duration(15 * time.Minute),
							MinImbalancePercent:                  ptrTo(int32(20)),
							MaxConcurrentMigrations:              ptrTo(int32(1)),
							HotTenantUtilizationThresholdPercent: ptrTo(int32(15)),
							ForbidMoveWhenSourceUtilizationAbovePercent: ptrTo(int32(65)),
							BlackoutWindows: []pgelasticv1alpha1.TimeWindow{{
								Schedule: "0 8 * * 1-5",
								Duration: metav1.Duration{Duration: 10 * time.Hour},
								TimeZone: "Europe/London",
							}},
						},
						Migration: &pgelasticv1alpha1.ElasticClassMigrationDefaults{
							Strategy:        "Online",
							AllowAutomatic:  ptrTo(true),
							RequireApproval: ptrTo(false),
							MaxPause:        duration(time.Second),
							RollbackWindow:  duration(time.Hour),
							OfflineWindow: &pgelasticv1alpha1.TimeWindow{
								Schedule: "0 1 * * *",
								Duration: metav1.Duration{Duration: 4 * time.Hour},
								TimeZone: "UTC",
							},
						},
					},
					Runtime: &pgelasticv1alpha1.ElasticClassRuntime{
						ProxyImage:      ptrTo("pgelastic/proxy:v1alpha1"),
						ImagePullPolicy: ptrTo(corev1.PullIfNotPresent),
						Workers:         ptrTo(int32(2)),
						MemoryBuffers: &pgelasticv1alpha1.ProxyMemoryBuffers{
							PacketBufferSize:         quantity("4Ki"),
							MaxPacketSize:            quantity("1Gi"),
							PerConnectionBufferLimit: quantity("1Mi"),
							ListenBacklog:            ptrTo(int32(128)),
						},
						Protocol: &pgelasticv1alpha1.ProxyProtocolPosture{
							CancelKeySizeBytes:     ptrTo(int32(32)),
							DirectTLS:              ptrTo(true),
							GSSEncryption:          ptrTo(false),
							ReplicationConnections: ptrTo(true),
							ChannelBinding:         "Prefer",
							StartupPacketMaxBytes:  ptrTo(int32(10000)),
							NegotiateGrease:        ptrTo(true),
							EchoUnknownPqOptions:   ptrTo(true),
						},
					},
					ErrorCodes: &pgelasticv1alpha1.ElasticClassErrorCodes{
						TenantCapReached: &pgelasticv1alpha1.ErrorCodeMapping{
							Code: "PGE1928", SQLState: "53300", Retryable: ptrTo(false),
							MessageTemplate: ptrTo("tenant burstable ceiling reached"),
						},
						PoolCapacityExhausted: &pgelasticv1alpha1.ErrorCodeMapping{
							Code: "PGE1936", SQLState: "53400", Retryable: ptrTo(true),
							RetryAfter: duration(5 * time.Second),
						},
						PoolBusy: &pgelasticv1alpha1.ErrorCodeMapping{
							Code: "PGE1929", SQLState: "53400", Retryable: ptrTo(true),
							RetryAfter: duration(time.Second),
						},
						StorageQuotaExceeded: &pgelasticv1alpha1.ErrorCodeMapping{
							Code: "PGE0544", SQLState: "53100", Retryable: ptrTo(false),
						},
						AdmissionQueueTimeout: &pgelasticv1alpha1.ErrorCodeMapping{
							Code: "PGE1024", SQLState: "53400", Retryable: ptrTo(true),
							RetryAfter: duration(5 * time.Second),
						},
						MigrationCutover: &pgelasticv1alpha1.ErrorCodeMapping{
							Code: "PGE1613", SQLState: "57P01", Retryable: ptrTo(true),
							RetryAfter: duration(time.Second),
						},
					},
					AdmittedNamespaces: &pgelasticv1alpha1.NamespaceAdmission{
						From: pgelasticv1alpha1.NamespaceFromSelector,
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"pgelastic.io/estate": "saas"},
						},
					},
					ReclaimPolicy: pgelasticv1alpha1.ReclaimRetain,
				},
			}

			submitted := class.DeepCopy()
			Expect(k8sClient.Create(ctx, class, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgElasticClass{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: elasticClassName}, fetched)).To(Succeed())

			expectNoPruning(submitted.Spec, fetched.Spec)
			Expect(fetched.Spec).To(Equal(class.Spec))
		})

		It("preserves every field of the three PgWorkloadClasses", func() {
			classes := []*pgelasticv1alpha1.PgWorkloadClass{
				{
					ObjectMeta: metav1.ObjectMeta{Name: standardClassName},
					Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
						Priority:         1000,
						PreemptionPolicy: ptrTo(pgelasticv1alpha1.PreemptionNever),
						Description:      ptrTo("the landing zone for characterised production tenants"),
						Global:           ptrTo(true),
						Capacity: pgelasticv1alpha1.WorkloadCapacity{
							Guaranteed:           ptrTo(int32(0)),
							Burstable:            8,
							Weight:               ptrTo(int32(100)),
							Storage:              quantity("20Gi"),
							MaxClientConnections: ptrTo(int32(200)),
						},
						Limits: &pgelasticv1alpha1.TenantLimits{
							StatementTimeout:                duration(30 * time.Second),
							IdleInTransactionSessionTimeout: duration(60 * time.Second),
							IdleSessionTimeout:              duration(10 * time.Minute),
							LockTimeout:                     duration(5 * time.Second),
							TempFileLimit:                   quantity("2Gi"),
							MaxResultBytes:                  quantity("256Mi"),
							RateLimit: &pgelasticv1alpha1.RateLimit{
								TransactionsPerSecond: ptrTo(int32(200)),
								BytesPerSecond:        quantity("20Mi"),
							},
						},
						SLO: &pgelasticv1alpha1.WorkloadSLO{
							CheckoutWaitP99:             duration(100 * time.Millisecond),
							AdmissionErrorBudgetPercent: ptrTo("1.0"),
						},
						OnBudgetExhaustion: ptrTo(pgelasticv1alpha1.BudgetExhaustionThrottle),
						Admission: &pgelasticv1alpha1.WorkloadAdmission{
							Quarantine: &pgelasticv1alpha1.QuarantinePolicy{
								Required: ptrTo(true),
								Duration: duration(observationWindow),
							},
						},
						Migration: &pgelasticv1alpha1.WorkloadMigrationPolicy{
							AllowAutomatic:  ptrTo(true),
							RequireApproval: ptrTo(false),
						},
						AutoPause: &pgelasticv1alpha1.AutoPausePolicy{Delay: duration(60 * time.Minute)},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: premiumClassName},
					Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
						Priority:         5000,
						PreemptionPolicy: ptrTo(pgelasticv1alpha1.PreemptionLowerPriority),
						Global:           ptrTo(false),
						Capacity: pgelasticv1alpha1.WorkloadCapacity{
							Guaranteed:           ptrTo(int32(4)),
							Burstable:            40,
							Weight:               ptrTo(int32(400)),
							Storage:              quantity("100Gi"),
							MaxClientConnections: ptrTo(int32(500)),
						},
						Limits: &pgelasticv1alpha1.TenantLimits{
							StatementTimeout:                duration(60 * time.Second),
							IdleInTransactionSessionTimeout: duration(120 * time.Second),
							LockTimeout:                     duration(10 * time.Second),
							TempFileLimit:                   quantity("8Gi"),
							MaxResultBytes:                  quantity("1Gi"),
							RateLimit: &pgelasticv1alpha1.RateLimit{
								TransactionsPerSecond: ptrTo(int32(2000)),
								BytesPerSecond:        quantity("100Mi"),
							},
						},
						SLO: &pgelasticv1alpha1.WorkloadSLO{
							CheckoutWaitP99:             duration(25 * time.Millisecond),
							AdmissionErrorBudgetPercent: ptrTo("0.1"),
						},
						OnBudgetExhaustion: ptrTo(pgelasticv1alpha1.BudgetExhaustionThrottle),
						Migration: &pgelasticv1alpha1.WorkloadMigrationPolicy{
							AllowAutomatic:  ptrTo(false),
							RequireApproval: ptrTo(true),
						},
						AutoPause: &pgelasticv1alpha1.AutoPausePolicy{Delay: duration(-time.Second)},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: quarantineClassName},
					Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
						Priority:    100,
						Description: ptrTo("landing zone for tenants whose workload is not yet characterised"),
						Capacity: pgelasticv1alpha1.WorkloadCapacity{
							Guaranteed:           ptrTo(int32(0)),
							Burstable:            2,
							Weight:               ptrTo(int32(50)),
							Storage:              quantity("10Gi"),
							MaxClientConnections: ptrTo(int32(50)),
						},
						OnBudgetExhaustion: ptrTo(pgelasticv1alpha1.BudgetExhaustionReject),
						Admission: &pgelasticv1alpha1.WorkloadAdmission{
							Quarantine: &pgelasticv1alpha1.QuarantinePolicy{
								Required: ptrTo(true),
								Duration: duration(observationWindow),
							},
						},
					},
				},
			}

			for _, workloadClass := range classes {
				submitted := workloadClass.DeepCopy()
				Expect(k8sClient.Create(ctx, workloadClass, strictFieldValidation)).To(Succeed())

				fetched := &pgelasticv1alpha1.PgWorkloadClass{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: workloadClass.Name}, fetched)).To(Succeed())

				expectNoPruning(submitted.Spec, fetched.Spec)
				Expect(fetched.Spec).To(Equal(workloadClass.Spec))
			}
		})

		It("preserves every field of a PgElasticPool sized for 200 tenants over 3 instances", func() {
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      poolName,
					Namespace: roundTripNamespace,
					Labels:    map[string]string{"pgelastic.io/tenant-tier": "saas"},
				},
				Spec: pgelasticv1alpha1.PgElasticPoolSpec{
					ClassRef: pgelasticv1alpha1.ClassReference{
						APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
						Kind:     elasticClassKind,
						Name:     elasticClassName,
					},
					Capacity: pgelasticv1alpha1.PoolCapacity{
						BackendConnections:       900,
						HeadroomPercent:          ptrTo(int32(25)),
						MaxClientConnections:     ptrTo(int32(12000)),
						Storage:                  quantity("1500Gi"),
						MaxOversubscriptionRatio: "12",
					},
					Instances: pgelasticv1alpha1.PoolInstances{
						Replicas: ptrTo(int32(3)),
						Template: pgelasticv1alpha1.PgInstanceTemplate{
							Class:           instanceClassName,
							PostgresVersion: ptrTo(postgresVersion),
							HighAvailability: &pgelasticv1alpha1.InstanceHighAvailability{
								Replicas:          ptrTo(int32(3)),
								SynchronousCommit: ptrTo(pgelasticv1alpha1.SynchronousCommitOn),
								Quorum:            ptrTo("ANY 1"),
								DataDurability:    ptrTo(pgelasticv1alpha1.DataDurabilityRequired),
								FailoverQuorum:    ptrTo(true),
								SwitchoverTimeout: duration(60 * time.Second),
								FailoverDelay:     duration(10 * time.Second),
								PrimaryLease: &pgelasticv1alpha1.PrimaryLeaseSpec{
									LeaseDuration:         duration(15 * time.Second),
									RenewDeadline:         duration(10 * time.Second),
									RetryPeriod:           duration(2 * time.Second),
									ReleasedLeaseDuration: duration(time.Second),
								},
							},
							Storage: pgelasticv1alpha1.InstanceStorage{
								Size:      resource.MustParse("500Gi"),
								ClassName: ptrTo(storageClassName),
								WALVolume: pgelasticv1alpha1.WALVolume{
									Size:      resource.MustParse("100Gi"),
									ClassName: ptrTo(storageClassName),
								},
							},
							Resources: &corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("16Gi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("8"),
									corev1.ResourceMemory: resource.MustParse("16Gi"),
								},
							},
							Parameters: map[string]pgelasticv1alpha1.GUCValue{
								tenantParameter:        tenantParameterValue,
								"maintenance_work_mem": "512MB",
							},
							Backup: &pgelasticv1alpha1.InstanceBackup{
								ObjectStore: pgelasticv1alpha1.ObjectStore{
									Path:                 "s3://pgelastic-backups/saas-pool",
									CredentialsSecretRef: corev1.LocalObjectReference{Name: "pgelastic-backup-s3"},
									Region:               objectStoreRegion,
								},
								Retention:     &pgelasticv1alpha1.RetentionPolicy{Full: backupRetention, WAL: backupRetention},
								Schedule:      ptrTo(backupSchedule),
								BackupStandby: ptrTo(true),
							},
							PerTenantLogicalBackup: &pgelasticv1alpha1.PerTenantLogicalBackup{
								Enabled:            ptrTo(true),
								MaxConcurrentDumps: ptrTo(int32(4)),
							},
						},
					},
					Admission: &pgelasticv1alpha1.PoolAdmission{
						Strategy:               pgelasticv1alpha1.AdmissionWeightedDeficit,
						QueueDepthPerTenant:    ptrTo(int32(64)),
						MaxWaitSeconds:         ptrTo(int32(30)),
						QueryWaitNotifySeconds: ptrTo(int32(5)),
						ReservationMode:        pgelasticv1alpha1.ReservationStrict,
						AdmittedNamespaces: &pgelasticv1alpha1.NamespaceAdmission{
							From: pgelasticv1alpha1.NamespaceFromSame,
						},
						DefaultWorkloadClassName:  standardClassName,
						AllowedWorkloadClassNames: []string{standardClassName, premiumClassName, quarantineClassName},
						BreakGlassRole:            "pgelastic_admin",
						Quarantine: &pgelasticv1alpha1.PoolQuarantine{
							Enabled:                    ptrTo(true),
							ObservationWindow:          duration(observationWindow),
							PromotionPercentile:        pgelasticv1alpha1.PercentileP95,
							PromotionWorkloadClassName: standardClassName,
							AutoPromote:                ptrTo(true),
						},
					},
					Pooling: &pgelasticv1alpha1.PoolingConfig{
						Mode:                     pgelasticv1alpha1.PoolModeTransaction,
						PreparedStatements:       pgelasticv1alpha1.PreparedStatementsExtended,
						PreparedStatementsLimit:  ptrTo(int32(1000)),
						ServerIdleTimeout:        duration(600 * time.Second),
						ServerLifetime:           duration(3600 * time.Second),
						ServerLifetimeJitter:     duration(300 * time.Second),
						IdleSelection:            pgelasticv1alpha1.IdleSelectionLIFO,
						ResetMode:                pgelasticv1alpha1.ResetDirtyTracked,
						TrackExtraParameters:     []string{"IntervalStyle", "search_path"},
						IgnoreStartupParameters:  []string{extraFloatDigitsParam, optionsParam},
						StartupParameterPolicy:   pgelasticv1alpha1.StartupParameterPoolKey,
						PinOnSessionState:        ptrTo(true),
						MaxPinnedFractionPercent: ptrTo(int32(20)),
						MaxPinDuration:           duration(time.Hour),
					},
					Timeouts: &pgelasticv1alpha1.PoolTimeouts{
						Connect:                 duration(5 * time.Second),
						Checkout:                duration(30 * time.Second),
						Query:                   duration(120 * time.Second),
						ClientLogin:             duration(10 * time.Second),
						ClientIdleInTransaction: duration(60 * time.Second),
						Rollback:                duration(5 * time.Second),
						CancelWait:              duration(10 * time.Second),
						Shutdown:                duration(60 * time.Second),
						ShutdownTermination:     duration(60 * time.Second),
					},
					Placement: &pgelasticv1alpha1.PoolPlacement{
						Strategy:          pgelasticv1alpha1.PlacementBestFitDecreasing,
						PackOnPercentile:  pgelasticv1alpha1.PercentileP95,
						ObservationWindow: duration(observationWindow),
						MaxSkewTenants:    ptrTo(int32(15)),
					},
					Rebalancing: &pgelasticv1alpha1.PoolRebalancing{
						Enabled:                              ptrTo(true),
						Mode:                                 pgelasticv1alpha1.RebalanceColdTenantsOnly,
						EvaluationInterval:                   duration(15 * time.Minute),
						MinImbalancePercent:                  ptrTo(int32(20)),
						MaxConcurrentMigrations:              ptrTo(int32(1)),
						HotTenantUtilizationThresholdPercent: ptrTo(int32(15)),
						ForbidMoveWhenSourceUtilizationAbovePercent: ptrTo(int32(65)),
						BlackoutWindows: []pgelasticv1alpha1.TimeWindow{{
							Schedule: "0 8 * * 1-5",
							Duration: metav1.Duration{Duration: 10 * time.Hour},
							TimeZone: "Europe/London",
						}},
					},
					Migration: &pgelasticv1alpha1.PoolMigration{
						DefaultStrategy:                pgelasticv1alpha1.MigrationOnline,
						AllowOnlineDuringBusinessHours: ptrTo(true),
						OfflineWindows: []pgelasticv1alpha1.TimeWindow{{
							Schedule: "0 1 * * *",
							Duration: metav1.Duration{Duration: 4 * time.Hour},
							TimeZone: "UTC",
						}},
						MaxPause:        duration(time.Second),
						RollbackWindow:  duration(time.Hour),
						Verification:    pgelasticv1alpha1.MigrationVerifyRowCounts,
						RequireApproval: ptrTo(false),
					},
					Autoscaling: &pgelasticv1alpha1.PoolAutoscaling{
						Mode:                     pgelasticv1alpha1.AutoscalingRecommend,
						MinInstances:             ptrTo(int32(3)),
						MaxInstances:             ptrTo(int32(8)),
						TargetUtilizationPercent: ptrTo(int32(70)),
						StabilizationWindow: &pgelasticv1alpha1.PoolStabilizationWindow{
							ScaleUp:   duration(3 * time.Minute),
							ScaleDown: duration(30 * time.Minute),
						},
						AutoActions: []pgelasticv1alpha1.AutoAction{pgelasticv1alpha1.AutoActionStorageExpand},
					},
					Auth: &pgelasticv1alpha1.PoolAuth{
						Mode:            pgelasticv1alpha1.AuthScramPassthrough,
						ScramIterations: ptrTo(int32(4096)),
						Rotation: &pgelasticv1alpha1.PoolAuthRotation{
							Schedule:      "0 3 1 * *",
							OverlapWindow: duration(24 * time.Hour),
						},
					},
					Proxy: &pgelasticv1alpha1.ProxySpec{
						Replicas: ptrTo(int32(3)),
						Resources: &corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
						Workers: ptrTo(int32(2)),
						TLS: &pgelasticv1alpha1.ProxyTLS{
							TLSConfig: pgelasticv1alpha1.TLSConfig{
								Mode:                 pgelasticv1alpha1.TLSVerifyFull,
								CertificateSecretRef: &corev1.LocalObjectReference{Name: "saas-pool-wildcard-tls"},
								Protocols:            []string{"tlsv1.2", "tlsv1.3"},
							},
							WildcardSNI: "*.db.example.com",
							BackendTLS: &pgelasticv1alpha1.TLSConfig{
								Mode:        pgelasticv1alpha1.TLSVerifyFull,
								CASecretRef: &corev1.LocalObjectReference{Name: "pgelastic-ca"},
								Protocols:   []string{"tlsv1.3"},
							},
						},
						Routing: &pgelasticv1alpha1.ProxyRouting{
							TenantDiscriminators: []pgelasticv1alpha1.TenantDiscriminator{
								pgelasticv1alpha1.DiscriminatorSNI,
								pgelasticv1alpha1.DiscriminatorStartupOptions,
								pgelasticv1alpha1.DiscriminatorDatabaseName,
							},
							DiscriminatorPrecedence: pgelasticv1alpha1.DiscriminatorPrecedenceStrict,
							StartupOptionKey:        "pgelastic.tenant",
							ReservedSNISubdomains:   []string{"admin", "metrics", "health"},
							CancelKeyRouting:        pgelasticv1alpha1.CancelKeyEmbeddedPodIdentity,
						},
						Drain: &pgelasticv1alpha1.ProxyDrain{
							Mode:           pgelasticv1alpha1.ProxyDrainWaitForClients,
							PreStopDelay:   duration(20 * time.Second),
							MaxSurge:       ptrTo(intstr.FromInt32(0)),
							MaxUnavailable: ptrTo(intstr.FromInt32(1)),
						},
						Readiness: &pgelasticv1alpha1.ProxyReadiness{
							Mode:                pgelasticv1alpha1.ProxyReadinessAdminState,
							EnableLivenessProbe: ptrTo(false),
						},
						Service: &pgelasticv1alpha1.ProxyService{
							Type: corev1.ServiceTypeClusterIP,
							Port: ptrTo(int32(5432)),
						},
						PodDisruptionBudget: &pgelasticv1alpha1.ProxyPodDisruptionBudget{
							MaxUnavailable: ptrTo(intstr.FromInt32(1)),
						},
						TerminationGracePeriodSeconds: ptrTo(int64(150)),
						Template: &pgelasticv1alpha1.ProxyPodTemplate{
							Metadata: &pgelasticv1alpha1.ProxyPodTemplateMetadata{
								Labels:      map[string]string{"example.com/cost-centre": "platform"},
								Annotations: map[string]string{"example.com/runbook": "proxy-fleet"},
							},
							Spec: &corev1.PodSpec{
								NodeSelector: map[string]string{"topology.kubernetes.io/region": "eu-west-1"},
								Tolerations: []corev1.Toleration{{
									Key:      "pgelastic.io/proxy",
									Operator: corev1.TolerationOpExists,
									Effect:   corev1.TaintEffectNoSchedule,
								}},
								Containers: []corev1.Container{{
									Name: "proxy",
									Env:  []corev1.EnvVar{{Name: "RUST_LOG", Value: "info"}},
								}},
							},
						},
						Metrics: &pgelasticv1alpha1.ProxyMetrics{Port: ptrTo(int32(9127))},
					},
					Metering: &pgelasticv1alpha1.PoolMetering{
						Enabled:         ptrTo(true),
						SampleInterval:  duration(60 * time.Second),
						RetentionWindow: duration(observationWindow),
						PerTenantSeries: ptrTo(true),
					},
					Observability: &pgelasticv1alpha1.PoolObservability{
						LogLevel:               "Info",
						LogFormat:              "Json",
						PerTenantMetrics:       ptrTo(true),
						PgBouncerCompatShow:    ptrTo(true),
						RewriteApplicationName: ptrTo(true),
					},
					Paused: ptrTo(false),
				},
			}

			submitted := pool.DeepCopy()
			Expect(k8sClient.Create(ctx, pool, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgElasticPool{}
			Expect(k8sClient.Get(ctx, namespacedName(poolName), fetched)).To(Succeed())

			expectNoPruning(submitted.Spec, fetched.Spec)
			Expect(fetched.Spec).To(Equal(pool.Spec))
		})

		It("preserves every field of a PgInstance", func() {
			instance := &pgelasticv1alpha1.PgInstance{
				ObjectMeta: objectMeta(primaryInstanceName),
				Spec: pgelasticv1alpha1.PgInstanceSpec{
					PoolRef:         poolRef,
					Class:           instanceClassName,
					PostgresVersion: ptrTo(postgresVersion),
					HighAvailability: &pgelasticv1alpha1.InstanceHighAvailability{
						Replicas:          ptrTo(int32(3)),
						SynchronousCommit: ptrTo(pgelasticv1alpha1.SynchronousCommitOn),
						Quorum:            ptrTo("ANY 1"),
						DataDurability:    ptrTo(pgelasticv1alpha1.DataDurabilityRequired),
						FailoverQuorum:    ptrTo(true),
						SwitchoverTimeout: duration(60 * time.Second),
						FailoverDelay:     duration(10 * time.Second),
						PrimaryLease: &pgelasticv1alpha1.PrimaryLeaseSpec{
							LeaseDuration:         duration(15 * time.Second),
							RenewDeadline:         duration(10 * time.Second),
							RetryPeriod:           duration(2 * time.Second),
							ReleasedLeaseDuration: duration(time.Second),
						},
					},
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      resource.MustParse("500Gi"),
						ClassName: ptrTo(storageClassName),
						WALVolume: pgelasticv1alpha1.WALVolume{
							Size:      resource.MustParse("100Gi"),
							ClassName: ptrTo(storageClassName),
						},
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("8"),
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
					},
					Parameters: map[string]pgelasticv1alpha1.GUCValue{
						"work_mem":             "8MB",
						"maintenance_work_mem": "512MB",
					},
					Backup: &pgelasticv1alpha1.InstanceBackup{
						ObjectStore: pgelasticv1alpha1.ObjectStore{
							Path:                 "s3://pgelastic-backups/saas-pool/pg-a",
							CredentialsSecretRef: corev1.LocalObjectReference{Name: "pgelastic-backup-s3"},
							EndpointURL:          "https://object.example.com",
							Region:               objectStoreRegion,
						},
						Retention:     &pgelasticv1alpha1.RetentionPolicy{Full: backupRetention, WAL: backupRetention},
						Schedule:      ptrTo(backupSchedule),
						BackupStandby: ptrTo(true),
					},
					PerTenantLogicalBackup: &pgelasticv1alpha1.PerTenantLogicalBackup{
						Enabled:            ptrTo(true),
						MaxConcurrentDumps: ptrTo(int32(4)),
					},
					Admission: &pgelasticv1alpha1.InstanceAdmission{
						Schedulable: ptrTo(true),
						Cordoned:    ptrTo(false),
					},
					Drain: &pgelasticv1alpha1.InstanceDrain{
						Mode: ptrTo(pgelasticv1alpha1.InstanceDrainNever),
					},
				},
			}

			submitted := instance.DeepCopy()
			Expect(k8sClient.Create(ctx, instance, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName(primaryInstanceName), fetched)).To(Succeed())

			expectNoPruning(submitted.Spec, fetched.Spec)
			Expect(fetched.Spec).To(Equal(instance.Spec))
		})

		It("preserves every field of a PgTenant", func() {
			tenant := &pgelasticv1alpha1.PgTenant{
				ObjectMeta: objectMeta(tenantName),
				Spec: pgelasticv1alpha1.PgTenantSpec{
					PoolRef:           poolRef,
					DatabaseName:      tenantDatabaseName,
					WorkloadClassName: ptrTo(premiumClassName),
					Capacity: &pgelasticv1alpha1.PgTenantCapacity{
						Guaranteed: ptrTo(int32(6)),
						Burstable:  ptrTo(int32(60)),
						Storage:    quantity("250Gi"),
					},
					Owner: ptrTo(tenantDatabaseName),
					Auth: &pgelasticv1alpha1.PgTenantAuth{
						Mode:                 ptrTo(pgelasticv1alpha1.AuthScramPassthrough),
						CredentialsSecretRef: &corev1.LocalObjectReference{Name: "acme-prod-db"},
					},
					Placement: &pgelasticv1alpha1.PgTenantPlacement{
						InstanceRef:           &corev1.LocalObjectReference{Name: primaryInstanceName},
						AntiAffinityLabelKeys: []string{"saas.example.com/customer-shard"},
					},
					Extensions:    []string{"pg_stat_statements", "pgcrypto"},
					ReclaimPolicy: ptrTo(pgelasticv1alpha1.ReclaimRetain),
				},
			}

			submitted := tenant.DeepCopy()
			Expect(k8sClient.Create(ctx, tenant, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgTenant{}
			Expect(k8sClient.Get(ctx, namespacedName(tenantName), fetched)).To(Succeed())

			expectNoPruning(submitted.Spec, fetched.Spec)
			Expect(fetched.Spec).To(Equal(tenant.Spec))
		})

		It("preserves every field of a PgTenantMigration", func() {
			migration := &pgelasticv1alpha1.PgTenantMigration{
				ObjectMeta: objectMeta("acme-prod-to-pg-b"),
				Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
					TenantRef:         corev1.LocalObjectReference{Name: tenantName},
					TargetInstanceRef: corev1.LocalObjectReference{Name: targetInstanceName},
					Strategy:          pgelasticv1alpha1.TenantMigrationOnline,
					Preflight: &pgelasticv1alpha1.TenantMigrationPreflight{
						RequireReplicaIdentity:                      ptrTo(true),
						ForbidLargeObjects:                          ptrTo(true),
						ForbidPreparedTransactions:                  ptrTo(true),
						MaxSourceLagBytes:                           ptrTo(int64(8388608)),
						RequireColdTenant:                           ptrTo(true),
						ForbidMoveWhenSourceUtilizationAbovePercent: ptrTo(int32(65)),
					},
					SequenceHandling: &pgelasticv1alpha1.TenantMigrationSequenceHandling{
						Mode:      pgelasticv1alpha1.SequenceHandlingSetvalWithGap,
						SafetyGap: ptrTo(int64(1000)),
					},
					DrainTimeout:   duration(30 * time.Second),
					CutoverTimeout: duration(60 * time.Second),
					RollbackWindow: duration(time.Hour),
					ApprovedBy:     ptrTo("platform-oncall"),
				},
			}

			submitted := migration.DeepCopy()
			Expect(k8sClient.Create(ctx, migration, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgTenantMigration{}
			Expect(k8sClient.Get(ctx, namespacedName("acme-prod-to-pg-b"), fetched)).To(Succeed())

			expectNoPruning(submitted.Spec, fetched.Spec)
			Expect(fetched.Spec).To(Equal(migration.Spec))
		})
	})

	Describe("schema defaulting", func() {
		It("defaults pool headroom to 25 percent and pooling mode to Transaction", func() {
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: objectMeta("defaulted-pool"),
				Spec: pgelasticv1alpha1.PgElasticPoolSpec{
					ClassRef: pgelasticv1alpha1.ClassReference{Kind: elasticClassKind, Name: elasticClassName},
					Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 900},
					Instances: pgelasticv1alpha1.PoolInstances{
						Template: pgelasticv1alpha1.PgInstanceTemplate{
							Class: instanceClassName,
							Storage: pgelasticv1alpha1.InstanceStorage{
								Size:      resource.MustParse("500Gi"),
								WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("100Gi")},
							},
						},
					},
					Pooling: &pgelasticv1alpha1.PoolingConfig{},
				},
			}
			Expect(k8sClient.Create(ctx, pool, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgElasticPool{}
			Expect(k8sClient.Get(ctx, namespacedName("defaulted-pool"), fetched)).To(Succeed())

			Expect(fetched.Spec.Capacity.HeadroomPercent).To(HaveValue(Equal(int32(25))))
			Expect(fetched.Spec.Capacity.MaxOversubscriptionRatio).To(Equal("12"))
			Expect(fetched.Spec.ClassRef.APIGroup).To(Equal(pgelasticv1alpha1.SchemeGroupVersion.Group))
			Expect(fetched.Spec.Instances.Replicas).To(HaveValue(Equal(int32(3))))
			Expect(fetched.Spec.Instances.Template.PostgresVersion).To(HaveValue(Equal(postgresVersion)))
			Expect(fetched.Spec.Pooling.Mode).To(Equal(pgelasticv1alpha1.PoolModeTransaction))
			Expect(fetched.Spec.Pooling.ResetMode).To(Equal(pgelasticv1alpha1.ResetDirtyTracked))
			Expect(fetched.Spec.Pooling.IgnoreStartupParameters).To(Equal([]string{extraFloatDigitsParam, optionsParam}))
			Expect(fetched.Spec.Paused).To(HaveValue(BeFalse()))
		})

		It("defaults instance postgresVersion to 18 and the primary lease to 15s/10s/2s/1s", func() {
			instance := &pgelasticv1alpha1.PgInstance{
				ObjectMeta: objectMeta("defaulted-instance"),
				Spec: pgelasticv1alpha1.PgInstanceSpec{
					PoolRef: poolRef,
					Class:   instanceClassName,
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      resource.MustParse("500Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("100Gi")},
					},
				},
			}
			Expect(k8sClient.Create(ctx, instance, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName("defaulted-instance"), fetched)).To(Succeed())

			Expect(fetched.Spec.PostgresVersion).To(HaveValue(Equal(postgresVersion)))

			highAvailability := fetched.Spec.HighAvailability
			Expect(highAvailability).NotTo(BeNil())
			Expect(highAvailability.Replicas).To(HaveValue(Equal(int32(3))))
			Expect(highAvailability.Quorum).To(HaveValue(Equal("ANY 1")))
			Expect(highAvailability.SynchronousCommit).To(HaveValue(Equal(pgelasticv1alpha1.SynchronousCommitOn)))
			Expect(highAvailability.FailoverDelay).To(HaveValue(Equal(metav1.Duration{Duration: 10 * time.Second})))

			lease := highAvailability.PrimaryLease
			Expect(lease).NotTo(BeNil())
			Expect(lease.LeaseDuration).To(HaveValue(Equal(metav1.Duration{Duration: 15 * time.Second})))
			Expect(lease.RenewDeadline).To(HaveValue(Equal(metav1.Duration{Duration: 10 * time.Second})))
			Expect(lease.RetryPeriod).To(HaveValue(Equal(metav1.Duration{Duration: 2 * time.Second})))
			Expect(lease.ReleasedLeaseDuration).To(HaveValue(Equal(metav1.Duration{Duration: time.Second})))

			Expect(fetched.Spec.Admission.Schedulable).To(HaveValue(BeTrue()))
			Expect(fetched.Spec.Admission.Cordoned).To(HaveValue(BeFalse()))
			Expect(fetched.Spec.Drain.Mode).To(HaveValue(Equal(pgelasticv1alpha1.InstanceDrainNever)))
		})

		It("defaults the tenant reclaim policy to Retain", func() {
			tenant := &pgelasticv1alpha1.PgTenant{
				ObjectMeta: objectMeta("defaulted-tenant"),
				Spec: pgelasticv1alpha1.PgTenantSpec{
					PoolRef:      poolRef,
					DatabaseName: "defaulted_tenant",
				},
			}
			Expect(k8sClient.Create(ctx, tenant, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgTenant{}
			Expect(k8sClient.Get(ctx, namespacedName("defaulted-tenant"), fetched)).To(Succeed())

			Expect(fetched.Spec.ReclaimPolicy).To(HaveValue(Equal(pgelasticv1alpha1.ReclaimRetain)))
		})
	})

	Describe("schema validation", func() {
		It("rejects a PgWorkloadClass whose guaranteed exceeds its burstable", func() {
			workloadClass := &pgelasticv1alpha1.PgWorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "inverted-capacity"},
				Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
					Priority: 1000,
					Capacity: pgelasticv1alpha1.WorkloadCapacity{
						Guaranteed: ptrTo(int32(40)),
						Burstable:  8,
					},
				},
			}

			err := k8sClient.Create(ctx, workloadClass, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("capacity.guaranteed must not exceed capacity.burstable"))
		})

		It("rejects a PgElasticPool whose headroomPercent exceeds 50", func() {
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: objectMeta("excessive-headroom"),
				Spec: pgelasticv1alpha1.PgElasticPoolSpec{
					ClassRef: pgelasticv1alpha1.ClassReference{Kind: elasticClassKind, Name: elasticClassName},
					Capacity: pgelasticv1alpha1.PoolCapacity{
						BackendConnections: 900,
						HeadroomPercent:    ptrTo(int32(80)),
					},
					Instances: pgelasticv1alpha1.PoolInstances{
						Template: pgelasticv1alpha1.PgInstanceTemplate{
							Class: instanceClassName,
							Storage: pgelasticv1alpha1.InstanceStorage{
								Size:      resource.MustParse("500Gi"),
								WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("100Gi")},
							},
						},
					},
				},
			}

			err := k8sClient.Create(ctx, pool, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.capacity.headroomPercent"))
		})

		It("rejects a classRef pointing at the wrong kind", func() {
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: objectMeta("wrong-class-kind"),
				Spec: pgelasticv1alpha1.PgElasticPoolSpec{
					ClassRef: pgelasticv1alpha1.ClassReference{
						Kind: "PgWorkloadClass",
						Name: standardClassName,
					},
					Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 900},
					Instances: pgelasticv1alpha1.PoolInstances{
						Template: pgelasticv1alpha1.PgInstanceTemplate{
							Class: instanceClassName,
							Storage: pgelasticv1alpha1.InstanceStorage{
								Size:      resource.MustParse("500Gi"),
								WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("100Gi")},
							},
						},
					},
				},
			}

			err := k8sClient.Create(ctx, pool, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("classRef must reference a pgelastic.io PgElasticClass"))
		})

		It("refuses to change a pool's classRef", func() {
			fetched := &pgelasticv1alpha1.PgElasticPool{}
			Expect(k8sClient.Get(ctx, namespacedName(poolName), fetched)).To(Succeed())

			fetched.Spec.ClassRef.Name = "some-other-class"
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("classRef is immutable"))
		})

		It("refuses to change a tenant's databaseName", func() {
			fetched := &pgelasticv1alpha1.PgTenant{}
			Expect(k8sClient.Get(ctx, namespacedName(tenantName), fetched)).To(Succeed())

			fetched.Spec.DatabaseName = "acme_prod_renamed"
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("databaseName is immutable"))
		})

		It("refuses to change an instance's class", func() {
			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName(primaryInstanceName), fetched)).To(Succeed())

			fetched.Spec.Class = "gp-16"
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("class is immutable"))
		})

		// Both edits corrupt something outside the object, in opposite ways, and the
		// field-level immutability rule catches neither: CEL does not evaluate a rule on an
		// optional field that is absent, which is true of the old object when the marker is
		// added and of the new one when it is removed.
		It("refuses to remove an instance's restore marker", func() {
			restored := &pgelasticv1alpha1.PgInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pg-recovered-roundtrip", Namespace: roundTripNamespace,
				},
				Spec: pgelasticv1alpha1.PgInstanceSpec{
					PoolRef: corev1.LocalObjectReference{Name: poolName},
					Class:   "dev-1",
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      resource.MustParse("1Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
					},
					Restore: &pgelasticv1alpha1.InstanceRestore{
						SourceInstanceName: "pg-source",
						Stanza:             restoreTestStanza,
						BackupID:           restoreTestBackupID,
					},
				},
			}
			Expect(k8sClient.Create(ctx, restored, strictFieldValidation)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, restored))).To(Succeed())
			})

			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName("pg-recovered-roundtrip"), fetched)).To(Succeed())
			fetched.Spec.Restore = nil
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("whether restore is set cannot change"))
		})

		// The other direction, and the more dangerous one: the marker makes the agent return
		// success from archive_command without archiving, so an ordinary instance given one
		// recycles WAL that never reached the repository and loses its recovery window with
		// nothing logged as an error.
		It("refuses to add a restore marker to an instance that never had one", func() {
			ordinary := &pgelasticv1alpha1.PgInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pg-ordinary-roundtrip", Namespace: roundTripNamespace,
				},
				Spec: pgelasticv1alpha1.PgInstanceSpec{
					PoolRef: corev1.LocalObjectReference{Name: poolName},
					Class:   "dev-1",
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      resource.MustParse("1Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
					},
				},
			}
			Expect(k8sClient.Create(ctx, ordinary, strictFieldValidation)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ordinary))).To(Succeed())
			})

			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName("pg-ordinary-roundtrip"), fetched)).To(Succeed())
			fetched.Spec.Restore = &pgelasticv1alpha1.InstanceRestore{
				SourceInstanceName: "pg-source",
				Stanza:             restoreTestStanza,
				BackupID:           restoreTestBackupID,
			}
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("whether restore is set cannot change"))
		})

		It("refuses to shrink an instance's data volume", func() {
			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName(primaryInstanceName), fetched)).To(Succeed())

			fetched.Spec.Storage.Size = resource.MustParse("400Gi")
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("storage size cannot be decreased"))
		})

		It("refuses to shrink an instance's WAL volume", func() {
			fetched := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(ctx, namespacedName(primaryInstanceName), fetched)).To(Succeed())

			fetched.Spec.Storage.WALVolume.Size = resource.MustParse("50Gi")
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("walVolume size cannot be decreased"))
		})

		It("refuses to change a migration's spec", func() {
			fetched := &pgelasticv1alpha1.PgTenantMigration{}
			Expect(k8sClient.Get(ctx, namespacedName("acme-prod-to-pg-b"), fetched)).To(Succeed())

			fetched.Spec.TargetInstanceRef.Name = "pg-c"
			err := k8sClient.Update(ctx, fetched, strictFieldValidation)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec is immutable"))
		})

		// Regression guard: ProxyPodTemplate declares metadata explicitly rather than
		// embedding metav1.ObjectMeta, which controller-gen renders as a bare
		// `type: object` with no properties. Reverting to the embedded type puts labels
		// and annotations outside the structural schema, where strict validation rejects
		// them and default validation drops them silently.
		It("round-trips pod template metadata", func() {
			labels := map[string]string{"example.com/scrape": "true"}
			annotations := map[string]string{"example.com/runbook": "proxy-fleet"}

			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: objectMeta("templated-pool"),
				Spec: pgelasticv1alpha1.PgElasticPoolSpec{
					ClassRef: pgelasticv1alpha1.ClassReference{Kind: elasticClassKind, Name: elasticClassName},
					Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 900},
					Instances: pgelasticv1alpha1.PoolInstances{
						Template: pgelasticv1alpha1.PgInstanceTemplate{
							Class: instanceClassName,
							Storage: pgelasticv1alpha1.InstanceStorage{
								Size:      resource.MustParse("500Gi"),
								WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("100Gi")},
							},
						},
					},
					Proxy: &pgelasticv1alpha1.ProxySpec{
						Template: &pgelasticv1alpha1.ProxyPodTemplate{
							Metadata: &pgelasticv1alpha1.ProxyPodTemplateMetadata{
								Labels:      labels,
								Annotations: annotations,
							},
							Spec: &corev1.PodSpec{
								Containers: []corev1.Container{{Name: "proxy"}},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, pool, strictFieldValidation)).To(Succeed())

			fetched := &pgelasticv1alpha1.PgElasticPool{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), fetched)).To(Succeed())
			Expect(fetched.Spec.Proxy.Template.Metadata.Labels).To(Equal(labels))
			Expect(fetched.Spec.Proxy.Template.Metadata.Annotations).To(Equal(annotations))
		})
	})
})
