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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The object store this suite archives into.
//
// It is stood up per namespace rather than shared, because the fault specs deliberately
// break credentials and a shared store would make one spec's induced failure another
// spec's flake.
const (
	objectStoreName   = "objectstore"
	objectStorePort   = 9000
	objectStoreBucket = "backups"
	// objectStoreData is where the store keeps its objects; a top-level directory of it is
	// a bucket.
	objectStoreData = "/data"
	// objectStoreDataVolume is the Pod volume backing it.
	objectStoreDataVolume = "data"
	// The key pair the store is configured with and pgelastic authenticates as. MinIO
	// requires at least three and eight characters respectively.
	objectStoreAccessKey = "pgelastic"
	objectStoreSecretKey = "pgelastic-secret"
)

// objectStoreImage is overridable so a cluster with no egress can point at a mirror.
var objectStoreImage = envOr("PGELASTIC_OBJECTSTORE_IMG", "quay.io/minio/minio:latest")

// objectStoreEndpoint is what pgBackRest is pointed at.
//
// https, not http, and not negotiable: pgBackRest's S3 driver speaks TLS only. A suite that
// stood up a plaintext store would be testing a configuration that cannot exist in
// production, and would have to skip the CA-bundle path entirely - which is exactly the
// path an S3-compatible store inside a cluster always takes.
func objectStoreEndpoint(namespace string) string {
	return fmt.Sprintf("https://%s.%s.svc:%d", objectStoreName, namespace, objectStorePort)
}

// deployObjectStore stands up an S3-compatible store with a certificate this suite signed,
// and returns the CA that signed it.
func deployObjectStore(namespace string) []byte {
	GinkgoHelper()

	caPEM, certPEM, keyPEM := issueServingCertificate(namespace)

	Expect(k8sClient.Create(suiteCtx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: objectStoreName + "-tls", Namespace: namespace},
		Data: map[string][]byte{
			// The filenames MinIO looks for inside its certificate directory.
			"public.crt":  certPEM,
			"private.key": keyPEM,
		},
	})).To(Succeed())

	Expect(k8sClient.Create(suiteCtx, objectStoreDeployment(namespace))).To(Succeed())
	Expect(k8sClient.Create(suiteCtx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: objectStoreName, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": objectStoreName},
			Ports: []corev1.ServicePort{{
				Port:       objectStorePort,
				TargetPort: intstr.FromInt32(objectStorePort),
			}},
		},
	})).To(Succeed())

	By("waiting for the object store to accept connections")
	Eventually(func(g Gomega) {
		deployment := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: namespace, Name: objectStoreName,
		}, deployment)).To(Succeed())
		g.Expect(deployment.Status.ReadyReplicas).To(BeNumerically(">=", 1))
	}, "5m", "3s").Should(Succeed())

	return caPEM
}

func objectStoreDeployment(namespace string) *appsv1.Deployment {
	labels := map[string]string{"app": objectStoreName}
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: objectStoreName, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// A top-level directory of the data volume is a bucket, so creating the
					// bucket needs no S3 client at all. pgBackRest does not create buckets:
					// only barman-cloud's check command ever did, and relying on which
					// command happens to run first is how a repository ends up half made.
					InitContainers: []corev1.Container{{
						Name:         "create-bucket",
						Image:        "busybox:1.37",
						Command:      []string{"mkdir", "-p", objectStoreData + "/" + objectStoreBucket},
						VolumeMounts: []corev1.VolumeMount{{Name: objectStoreDataVolume, MountPath: objectStoreData}},
					}},
					Containers: []corev1.Container{{
						Name:  objectStoreName,
						Image: objectStoreImage,
						Args: []string{
							"server", objectStoreData,
							"--certs-dir", "/certs",
							"--address", fmt.Sprintf(":%d", objectStorePort),
						},
						Env: []corev1.EnvVar{
							{Name: "MINIO_ROOT_USER", Value: objectStoreAccessKey},
							{Name: "MINIO_ROOT_PASSWORD", Value: objectStoreSecretKey},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: objectStorePort}},
						VolumeMounts: []corev1.VolumeMount{
							{Name: objectStoreDataVolume, MountPath: objectStoreData},
							{Name: "certs", MountPath: "/certs", ReadOnly: true},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
								Port: intstr.FromInt32(objectStorePort),
							}},
							PeriodSeconds: 2,
						},
					}},
					Volumes: []corev1.Volume{
						{Name: objectStoreDataVolume, VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						}},
						{Name: "certs", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: objectStoreName + "-tls",
							},
						}},
					},
				},
			},
		},
	}
}

// issueServingCertificate mints a CA and a leaf for the store's in-cluster name.
//
// Self-signed on purpose rather than through cert-manager: the path being exercised is the
// one where the store presents a certificate the PostgreSQL image does not already trust,
// which is what every S3-compatible store inside a cluster does, and the CA has to travel
// to pgBackRest through the credentials Secret for it to work at all.
func issueServingCertificate(namespace string) (caPEM, certPEM, keyPEM []byte) {
	GinkgoHelper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pgelastic e2e object store CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	Expect(err).NotTo(HaveOccurred())
	caCert, err := x509.ParseCertificate(caDER)
	Expect(err).NotTo(HaveOccurred())

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())
	host := fmt.Sprintf("%s.%s.svc", objectStoreName, namespace)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		// Both the bare and fully qualified service names, because which one a resolver
		// returns is a property of the cluster's search domains rather than of this suite.
		DNSNames: []string{
			host,
			host + ".cluster.local",
			objectStoreName,
			objectStoreName + "." + namespace,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	Expect(err).NotTo(HaveOccurred())
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	Expect(err).NotTo(HaveOccurred())

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})
}
