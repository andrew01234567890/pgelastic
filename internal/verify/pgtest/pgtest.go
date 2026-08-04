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

// Package pgtest starts a throwaway PostgreSQL for the durability oracle's own tests.
package pgtest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// MajorEnv names the PostgreSQL major these containers run, and is the same variable the
// Rust harness and the e2e suites read. The oracle's verdict covers exactly the major it ran
// on, so which one that is has to be a decision rather than a literal buried here.
const MajorEnv = "PGELASTIC_PG_MAJOR"

// defaultMajor is the major the merge gate runs.
const defaultMajor = "18"

// Image is the PostgreSQL the oracle calibrates against.
func Image() string {
	if major := os.Getenv(MajorEnv); major != "" {
		return "postgres:" + major
	}
	return "postgres:" + defaultMajor
}

const (
	database = "verify"
	username = "verify"
	password = "verify"
)

// SkipEnvVar lets an environment without a container runtime opt out explicitly.
const SkipEnvVar = "PGELASTIC_SKIP_CONTAINER_TESTS"

// Start brings up PostgreSQL and returns a DSN for it. The container is terminated when
// the test finishes.
func Start(t *testing.T) string {
	t.Helper()
	if os.Getenv(SkipEnvVar) != "" {
		t.Skipf("%s is set", SkipEnvVar)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := postgres.Run(ctx, Image(),
		postgres.WithDatabase(database),
		postgres.WithUsername(username),
		postgres.WithPassword(password),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		if isRuntimeUnavailable(err) {
			t.Skipf("no container runtime available: %v", err)
		}
		t.Fatalf("starting %s: %v", Image(), err)
	}
	testcontainers.CleanupContainer(t, container)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func isRuntimeUnavailable(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{"docker daemon", "rootless docker", "cannot connect", "permission denied"} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}
