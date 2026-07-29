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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodEndpoints resolves a fleet's control API through the Service's own selector.
//
// Through the selector rather than by listing Pods with a label set of this package's own
// choosing: the selector is what actually decides which replicas serve the pool, so an
// operator that got it wrong fails here instead of quietly quiescing Pods nothing routes to.
//
// The Pod IP rather than the Service, and deliberately so. The Service load-balances, and
// a control call that reached a different replica each time would quiesce one shard of the
// tenant's clients and report the drain of another.
type PodEndpoints struct {
	// Reader should be uncached. A quiesce aimed at the replicas an informer remembers is
	// one that misses the replica that replaced them.
	Reader client.Reader
}

var _ ControlEndpoints = PodEndpoints{}

// Endpoints names every ready replica of the pool's fleet.
func (p PodEndpoints) Endpoints(
	ctx context.Context,
	pool client.ObjectKey,
) ([]ControlEndpoint, error) {
	service := &corev1.Service{}
	key := client.ObjectKey{Namespace: pool.Namespace, Name: ServiceName(pool.Name)}
	if err := p.Reader.Get(ctx, key, service); err != nil {
		return nil, fmt.Errorf("proxy Service %s: %w", key.Name, err)
	}
	if len(service.Spec.Selector) == 0 {
		return nil, fmt.Errorf("the proxy Service %s selects nothing", key.Name)
	}

	pods := &corev1.PodList{}
	if err := p.Reader.List(ctx, pods,
		client.InNamespace(pool.Namespace),
		client.MatchingLabels(service.Spec.Selector)); err != nil {
		return nil, fmt.Errorf("proxy replicas of %s: %w", pool.Name, err)
	}

	endpoints := make([]ControlEndpoint, 0, len(pods.Items))
	for i := range pods.Items {
		replica := &pods.Items[i]
		if replica.DeletionTimestamp != nil || replica.Status.PodIP == "" || !ready(replica) {
			continue
		}
		endpoints = append(endpoints, ControlEndpoint{
			Pod:     replica.Name,
			BaseURL: fmt.Sprintf("https://%s:%d", hostLiteral(replica.Status.PodIP), DefaultControlPort),
		})
	}
	return endpoints, nil
}

// hostLiteral brackets an IPv6 address so it can carry a port.
func hostLiteral(address string) string {
	if strings.Contains(address, ":") {
		return "[" + address + "]"
	}
	return address
}

func ready(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// MutualTLSCaller presents the operator's per-pool client certificate.
//
// Per pool, because the control CA is per pool: one identity would not authenticate against
// another pool's fleet, and that is the property that keeps a compromised issuer from
// authorising a cutover on somebody else's tenants. The server name is pinned to the name
// the listener's certificate carries rather than taken from the URL, because the URL names
// a Pod IP and a listener that presented anything at all would be one the operator could be
// impersonated to.
type MutualTLSCaller struct {
	// Reader should be uncached: a rotated certificate has to be picked up on the call that
	// needs it, not one informer resync later.
	Reader client.Reader
	// Timeout bounds one control call. Every one of them is inside the client-visible pause.
	Timeout time.Duration

	mu       sync.Mutex
	identity map[string]*cachedIdentity
}

var _ Caller = (*MutualTLSCaller)(nil)

type cachedIdentity struct {
	version string
	client  *http.Client
}

// DefaultCallTimeout bounds one control call. It is short because the caller is inside the
// pause and a replica that cannot answer in this long has to be reported rather than waited
// for; the fan-out reports it against the replica's own name.
const DefaultCallTimeout = 5 * time.Second

// Do issues one control-API request under the pool's own identity.
func (c *MutualTLSCaller) Do(
	ctx context.Context,
	pool client.ObjectKey,
	method, endpoint string,
	body any,
) (Answer, error) {
	caller, err := c.clientFor(ctx, pool)
	if err != nil {
		return Answer{}, err
	}

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return Answer{}, err
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, payload)
	if err != nil {
		return Answer{}, err
	}
	if body != nil {
		request.Header.Set("content-type", "application/json")
	}

	response, err := caller.Do(request)
	if err != nil {
		return Answer{}, err
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(io.LimitReader(response.Body, maxAnswerBytes))
	if err != nil {
		return Answer{}, err
	}
	return Answer{Status: response.StatusCode, Body: answer}, nil
}

// maxAnswerBytes bounds what a replica can make the operator hold. Every response is a
// handful of short fields.
const maxAnswerBytes = 1 << 16

func (c *MutualTLSCaller) clientFor(ctx context.Context, pool client.ObjectKey) (*http.Client, error) {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: pool.Namespace, Name: ControlClientSecretName(pool.Name)}
	if err := c.Reader.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("the operator's control certificate %s: %w", key.Name, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.identity[key.String()]; ok && cached.version == secret.ResourceVersion {
		return cached.client, nil
	}

	identity, err := tls.X509KeyPair(secret.Data["tls.crt"], secret.Data["tls.key"])
	if err != nil {
		return nil, fmt.Errorf("the operator's control certificate %s: %w", key.Name, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data["ca.crt"]) {
		return nil, fmt.Errorf("%s carries no issuing CA, so the listener cannot be verified", key.Name)
	}

	caller := &http.Client{
		Timeout: c.timeout(),
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{identity},
			RootCAs:      roots,
			ServerName:   ControlServerName(pool.Name, pool.Namespace),
			MinVersion:   tls.VersionTLS12,
		}},
	}
	if c.identity == nil {
		c.identity = map[string]*cachedIdentity{}
	}
	c.identity[key.String()] = &cachedIdentity{version: secret.ResourceVersion, client: caller}
	return caller, nil
}

func (c *MutualTLSCaller) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return DefaultCallTimeout
}
