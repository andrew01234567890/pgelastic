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

package migration

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// PostgresContainer is the container every statement and every tool runs in.
const PostgresContainer = "postgres"

// fieldSeparator is what psql is told to delimit columns with. It is the ASCII unit
// separator rather than a pipe or a comma because a catalog name, a rendered row or a
// GUC value may legitimately contain either of those, and a separator that can occur inside
// a value silently splits one column into two.
const fieldSeparator = "\x1f"

// PodExec runs one command inside a container of one Pod.
type PodExec interface {
	Exec(ctx context.Context, namespace, pod, container string, argv []string, stdin string) ([]byte, error)
}

// KubeExec runs commands through the API server's exec subresource. It works identically
// from inside the cluster and from a developer's machine, which matters because a Pod CIDR
// is not routable from the latter.
type KubeExec struct {
	Config    *restclient.Config
	Clientset kubernetes.Interface
}

// NewKubeExec builds an executor from a rest config.
func NewKubeExec(config *restclient.Config) (KubeExec, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return KubeExec{}, err
	}
	return KubeExec{Config: config, Clientset: clientset}, nil
}

// Exec runs argv and returns stdout, folding stderr into the error so a failure says what
// PostgreSQL complained about rather than only that the exit code was non-zero.
func (k KubeExec) Exec(
	ctx context.Context, namespace, pod, container string, argv []string, stdin string,
) ([]byte, error) {
	request := k.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   argv,
			Stdin:     stdin != "",
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(k.Config, "POST", request.URL())
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	streams := remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}
	if stdin != "" {
		streams.Stdin = strings.NewReader(stdin)
	}
	if err := executor.StreamWithContext(ctx, streams); err != nil {
		return stdout.Bytes(), fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// MemberResolver turns an endpoint into the Pod a statement should run in. An endpoint with
// no member names the instance's current primary, which is the only correct place for
// anything that writes.
type MemberResolver interface {
	Resolve(ctx context.Context, at Endpoint) (string, error)
}

// PrimaryResolver reads the current primary off the PgInstance the operator publishes. It
// reads uncached wherever it can: a statement sent to the member that used to be the primary
// fails in a way that looks like PostgreSQL being wrong rather than the cache being stale.
type PrimaryResolver struct {
	Client client.Reader
}

// Resolve returns the endpoint's member, or the instance's current primary.
func (r PrimaryResolver) Resolve(ctx context.Context, at Endpoint) (string, error) {
	if at.Member != "" {
		return at.Member, nil
	}
	instance := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: at.Namespace, Name: at.Instance}
	if err := r.Client.Get(ctx, key, instance); err != nil {
		return "", err
	}
	if instance.Status.CurrentPrimary == "" {
		return "", fmt.Errorf("PgInstance %s/%s has no current primary", at.Namespace, at.Instance)
	}
	return instance.Status.CurrentPrimary, nil
}

// PodSQL satisfies both SQL and Shell by running psql and the PostgreSQL tools inside the
// member's own container, over the Unix socket the bootstrap superuser is reachable on.
//
// There is no TCP alternative. The superuser has no password by design and is admitted only
// by peer authentication over a socket in an emptyDir, and a migration needs superuser to
// create a subscription, fence a database and drop one.
type PodSQL struct {
	Runner  PodExec
	Members MemberResolver
}

var (
	_ SQL   = PodSQL{}
	_ Shell = PodSQL{}
)

// Exec runs a statement and discards its rows.
func (p PodSQL) Exec(ctx context.Context, at Endpoint, statement string) error {
	_, err := p.psql(ctx, at, statement)
	return err
}

// Query runs a script and returns its rows. A statement that produces no rows - every SET
// in the verifier's setup string, every DO block in the cleanup ladder - contributes
// nothing, so a script of settings followed by one query yields exactly that query's rows.
func (p PodSQL) Query(ctx context.Context, at Endpoint, statement string) ([]Row, error) {
	output, err := p.psql(ctx, at, statement)
	if err != nil {
		return nil, err
	}
	return parseRows(output), nil
}

// Run executes one of the PostgreSQL command-line tools in the member's container.
func (p PodSQL) Run(ctx context.Context, at Endpoint, argv []string) ([]byte, error) {
	member, err := p.Members.Resolve(ctx, at)
	if err != nil {
		return nil, err
	}
	return p.Runner.Exec(ctx, at.Namespace, member, PostgresContainer, argv, "")
}

// psqlStdinMarker is the file psql reads the script from. The script arrives on stdin rather
// than in argv so that a statement containing a newline, a quote or a dollar-quoted block
// reaches the server exactly as written.
const psqlStdinMarker = "-"

func (p PodSQL) psql(ctx context.Context, at Endpoint, statement string) ([]byte, error) {
	member, err := p.Members.Resolve(ctx, at)
	if err != nil {
		return nil, err
	}
	database := at.Database
	if database == "" {
		database = "postgres"
	}
	argv := []string{
		"psql", "--host=" + provision.SocketDir, "--username=postgres", "--dbname=" + database,
		"--no-psqlrc", "--quiet", "--tuples-only", "--no-align",
		"--field-separator=" + fieldSeparator,
		"--set=ON_ERROR_STOP=1", "--file=" + psqlStdinMarker,
	}
	output, err := p.Runner.Exec(ctx, at.Namespace, member, PostgresContainer, argv, statement+"\n")
	if err != nil {
		return output, fmt.Errorf("psql on %s/%s (%s): %w", at.Namespace, member, database, err)
	}
	return output, nil
}

// parseRows splits psql's unaligned, tuples-only output into rows and columns.
//
// Exactly one trailing newline is stripped, not every one. In -tA output a query that
// returned nothing prints no bytes at all, while one row whose only column is the empty
// string prints a single newline - and collapsing the second into the first makes an empty
// answer indistinguishable from an absent one. That is not hypothetical: the schema
// fingerprint coalesces to the empty string on purpose, so a tenant database with no
// sequences produced "no rows" and failed its own verification mid-cutover.
func parseRows(output []byte) []Row {
	text := strings.TrimSuffix(string(output), "\n")
	if len(output) == 0 {
		return nil
	}
	lines := strings.Split(text, "\n")
	rows := make([]Row, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, strings.Split(line, fieldSeparator))
	}
	return rows
}
