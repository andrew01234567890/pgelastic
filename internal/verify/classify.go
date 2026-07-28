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

package verify

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
)

// Outcome is the verdict Classify reaches about a single INSERT.
type Outcome int

const (
	// OutcomeCommitted means the server acknowledged the commit. Only a nil error proves this.
	OutcomeCommitted Outcome = iota
	// OutcomeIndeterminate means the value may or may not be durable. This is the default
	// for anything not proven, and it is not a failure.
	OutcomeIndeterminate
	// OutcomeFailed means the server definitely rejected the statement, so no commit can
	// exist. Reserved for a short allowlist of deterministic rejections.
	OutcomeFailed
)

func (o Outcome) String() string {
	switch o {
	case OutcomeCommitted:
		return "committed"
	case OutcomeIndeterminate:
		return "indeterminate"
	case OutcomeFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Classification is the outcome of one INSERT together with why it was reached.
type Classification struct {
	Outcome Outcome
	// Code is the SQLSTATE when the server named one, and empty for transport-level errors.
	Code   string
	Reason string
}

// definiteRejections are the SQLSTATEs for which the server has provably not committed:
// the statement was rejected before, or rolled back as part of, its own transaction, and
// the error is not one a mid-commit crash can produce. Everything else — including
// 40003 statement_completion_unknown, every class 08 transport failure and 25006
// read_only_sql_transaction — stays indeterminate.
var definiteRejections = map[string]bool{
	"40001": true, // serialization_failure
	"40P01": true, // deadlock_detected
	"53300": true, // too_many_connections
	"3D000": true, // invalid_catalog_name
}

// definiteRejectionClasses are whole SQLSTATE classes of deterministic rejection:
// data exceptions, integrity constraint violations, invalid authorization and
// syntax/access-rule violations.
var definiteRejectionClasses = []string{"22", "23", "28", "42"}

// Classify maps the error from a single INSERT onto the ledger state it deserves.
//
// The bias is deliberate and total: only a nil error yields OutcomeCommitted, and
// anything not on the definite-rejection allowlist yields OutcomeIndeterminate. An
// outcome wrongly called committed loses the durability assertion its teeth; an outcome
// wrongly called failed hides a real lost commit. Indeterminate costs nothing but a line
// in the informational RECOVERED set.
func Classify(err error) Classification {
	if err == nil {
		return Classification{Outcome: OutcomeCommitted, Reason: "server acknowledged the commit"}
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return classifyServerError(pgErr)
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return Classification{Outcome: OutcomeIndeterminate, Reason: "context deadline exceeded"}
	case errors.Is(err, context.Canceled):
		return Classification{Outcome: OutcomeIndeterminate, Reason: "context canceled"}
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return Classification{Outcome: OutcomeIndeterminate, Reason: "connection reset by peer"}
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		return Classification{Outcome: OutcomeIndeterminate, Reason: "connection closed mid-statement"}
	case isTimeout(err):
		return Classification{Outcome: OutcomeIndeterminate, Reason: "network timeout"}
	case strings.Contains(err.Error(), "connection reset by peer"):
		return Classification{Outcome: OutcomeIndeterminate, Reason: "connection reset by peer"}
	default:
		return Classification{Outcome: OutcomeIndeterminate, Reason: "unclassified error: " + err.Error()}
	}
}

func classifyServerError(pgErr *pgconn.PgError) Classification {
	switch pgErr.Code {
	case "57P01":
		return Classification{Outcome: OutcomeIndeterminate, Code: pgErr.Code, Reason: "admin shutdown"}
	case "57014":
		return Classification{Outcome: OutcomeIndeterminate, Code: pgErr.Code, Reason: "query canceled"}
	case "08006":
		return Classification{Outcome: OutcomeIndeterminate, Code: pgErr.Code, Reason: "connection failure"}
	case "08003":
		return Classification{Outcome: OutcomeIndeterminate, Code: pgErr.Code, Reason: "connection does not exist"}
	}
	if definiteRejections[pgErr.Code] || isDefiniteRejectionClass(pgErr.Code) {
		return Classification{Outcome: OutcomeFailed, Code: pgErr.Code, Reason: "server rejected the statement"}
	}
	return Classification{Outcome: OutcomeIndeterminate, Code: pgErr.Code, Reason: "ambiguous server error"}
}

func isDefiniteRejectionClass(code string) bool {
	if len(code) < 2 {
		return false
	}
	for _, class := range definiteRejectionClasses {
		if strings.HasPrefix(code, class) {
			return true
		}
	}
	return false
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
