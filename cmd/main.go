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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"maps"
	"os"
	"slices"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/controller"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
	webhookv1alpha1 "github.com/andrew01234567890/pgelastic/internal/webhook/v1alpha1"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// envControllerName names this operator's identity from the environment, so a Deployment
// can give two operators disjoint claims without changing their command line.
const envControllerName = "PGELASTIC_CONTROLLER_NAME"

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(pgelasticv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var controllerName string
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&controllerName, "controller-name",
		envOrDefault(envControllerName, controller.DefaultControllerName),
		"The controllerName a PgElasticClass must name for this operator to claim it. That "+
			"claim is inherited by every pool bound to the class and by every instance, tenant "+
			"and migration under those pools; anything governed by another controller is "+
			"ignored entirely. Overrides "+envControllerName+".")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "72e24d87.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	if err := index.Setup(context.Background(), mgr.GetFieldIndexer()); err != nil {
		setupLog.Error(err, "Failed to register field indexes")
		os.Exit(1)
	}

	if err := (&controller.PgElasticClassReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgelasticclass")
		os.Exit(1)
	}
	if err := (&controller.PgWorkloadClassReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgworkloadclass")
		os.Exit(1)
	}
	// One metering collector serves both the pool planner and the tenant placer, so the
	// percentile a tenant is placed on and the percentile the plan packs on are the same
	// number rather than two independently sampled ones.
	meteringMetrics, err := metering.NewMetrics(metrics.Registry)
	if err != nil {
		setupLog.Error(err, "Failed to register metering metrics")
		os.Exit(1)
	}
	meteringCollector := metering.NewCollector(metering.Options{}, meteringMetrics)

	if err := (&controller.PgElasticPoolReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Metering:       meteringCollector,
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgelasticpool")
		os.Exit(1)
	}
	// The instance controller and the migration controller reach the same fleet through the
	// same control API, and are given one router between them: both take holds on it, the
	// renewal loops live inside it, and two routers would each keep half the holds alive.
	fleetRouter := &proxy.ProxyRouter{
		Binding:   migration.BindingRouter{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()},
		Reader:    mgr.GetAPIReader(),
		Endpoints: proxy.PodEndpoints{Reader: mgr.GetAPIReader()},
		Caller:    &proxy.MutualTLSCaller{Reader: mgr.GetAPIReader()},
	}
	if err := (&controller.PgInstanceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		// Without this a rolling restart hands the primary role over with nobody holding the
		// instance's clients, which is the difference between a latency spike and a dropped
		// connection for every tenant on it.
		Quiescer:       fleetRouter,
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pginstance")
		os.Exit(1)
	}
	// Every statement pgelastic issues runs as the bootstrap superuser over a member's Unix
	// socket, because that superuser has no password and no TCP route by design. The exec
	// transport is therefore not a fallback for a missing driver; it is the only way in.
	// Tenant provisioning and tenant migration share it so that a provisioned database and
	// a migrated one are reached, and created, the same way.
	migrationExec, err := migration.NewKubeExec(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "Failed to build the exec transport")
		os.Exit(1)
	}
	migrationSQL := migration.PodSQL{
		Runner:  migrationExec,
		Members: migration.PrimaryResolver{Client: mgr.GetAPIReader()},
	}
	if err := (&controller.PgTenantReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Metering:       meteringCollector,
		SQL:            migrationSQL,
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgtenant")
		os.Exit(1)
	}
	// The routing table of record, and the fleet that makes a flip immediate, composed
	// rather than chosen between. status.binding is what a replica that restarts or one
	// added later resolves the tenant through, so it is written on every path; the control
	// API is asked only of a pool that has a fleet, and a pool without one migrates through
	// the binding alone.
	//
	// Every read goes through the API reader. The safety property that an abort restores the
	// source rests on knowing where the tenant is routed now, and quiescing the replicas an
	// informer cache remembers is quiescing the ones that have already been replaced.
	if err := (&controller.PgTenantMigrationReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		SQL:            migrationSQL,
		Shell:          migrationSQL,
		Router:         fleetRouter,
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgtenantmigration")
		os.Exit(1)
	}
	// The backup controller reports on backups and never takes one: a backup runs inside the
	// member's Pod, claimed by that member's own agent, which is what keeps this operator's
	// ServiceAccount free of pods/exec.
	if err := (&controller.PgBackupReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgbackup")
		os.Exit(1)
	}
	if err := (&controller.PgRestoreReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		SQL:            migrationSQL,
		Shell:          migrationSQL,
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "pgrestore")
		os.Exit(1)
	}
	if err := mgr.Add(&controller.MigrationSweeper{
		Client:         mgr.GetClient(),
		SQL:            migrationSQL,
		ControllerName: controllerName,
	}); err != nil {
		setupLog.Error(err, "Failed to add the migration orphan sweeper")
		os.Exit(1)
	}
	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		webhooks := map[string]func(ctrl.Manager) error{
			"PgElasticPool":   webhookv1alpha1.SetupPgElasticPoolWebhookWithManager,
			"PgTenant":        webhookv1alpha1.SetupPgTenantWebhookWithManager,
			"PgTenantUser":    webhookv1alpha1.SetupPgTenantUserWebhookWithManager,
			"PgWorkloadClass": webhookv1alpha1.SetupPgWorkloadClassWebhookWithManager,
		}
		for _, kind := range slices.Sorted(maps.Keys(webhooks)) {
			if err := webhooks[kind](mgr); err != nil {
				setupLog.Error(err, "Failed to create webhook", "webhook", kind)
				os.Exit(1)
			}
		}
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
