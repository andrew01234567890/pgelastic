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

package verify_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgelastic/internal/verify"
)

func pgErr(code, message string) error {
	return &pgconn.PgError{Code: code, Message: message, Severity: "FATAL"}
}

func TestClassify(t *testing.T) {
	connReset := &net.OpError{
		Op:  "write",
		Net: "tcp",
		Err: syscall.ECONNRESET,
	}

	tests := []struct {
		name string
		err  error
		want verify.Outcome
		code string
	}{
		{
			name: "a nil error is the only proof of a commit",
			err:  nil,
			want: verify.OutcomeCommitted,
		},
		{
			name: "57P01 admin shutdown may arrive after the commit record was flushed",
			err:  pgErr("57P01", "terminating connection due to administrator command"),
			want: verify.OutcomeIndeterminate,
			code: "57P01",
		},
		{
			name: "57014 query canceled cannot prove the commit did not happen",
			err:  pgErr("57014", "canceling statement due to user request"),
			want: verify.OutcomeIndeterminate,
			code: "57014",
		},
		{
			name: "08006 connection failure",
			err:  pgErr("08006", "connection failure"),
			want: verify.OutcomeIndeterminate,
			code: "08006",
		},
		{
			name: "08003 connection does not exist",
			err:  pgErr("08003", "connection does not exist"),
			want: verify.OutcomeIndeterminate,
			code: "08003",
		},
		{
			name: "40003 statement completion unknown is ambiguous by definition",
			err:  pgErr("40003", "statement completion unknown"),
			want: verify.OutcomeIndeterminate,
			code: "40003",
		},
		{
			name: "25006 read-only transaction is treated as ambiguous, not failed",
			err:  pgErr("25006", "cannot execute INSERT in a read-only transaction"),
			want: verify.OutcomeIndeterminate,
			code: "25006",
		},
		{
			name: "a context deadline says nothing about the server",
			err:  fmt.Errorf("exec: %w", context.DeadlineExceeded),
			want: verify.OutcomeIndeterminate,
		},
		{
			name: "a canceled context says nothing about the server",
			err:  fmt.Errorf("exec: %w", context.Canceled),
			want: verify.OutcomeIndeterminate,
		},
		{
			name: "a bare connection reset",
			err:  connReset,
			want: verify.OutcomeIndeterminate,
		},
		{
			name: "a wrapped connection reset",
			err:  fmt.Errorf("write tcp: %w", syscall.ECONNRESET),
			want: verify.OutcomeIndeterminate,
		},
		{
			name: "an unexpected EOF mid-statement",
			err:  io.ErrUnexpectedEOF,
			want: verify.OutcomeIndeterminate,
		},
		{
			name: "an unrecognised error is never assumed committed or failed",
			err:  errors.New("something nobody anticipated"),
			want: verify.OutcomeIndeterminate,
		},
		{
			name: "23505 unique violation is a deterministic rejection",
			err:  pgErr("23505", "duplicate key value violates unique constraint"),
			want: verify.OutcomeFailed,
			code: "23505",
		},
		{
			name: "42601 syntax error is a deterministic rejection",
			err:  pgErr("42601", "syntax error"),
			want: verify.OutcomeFailed,
			code: "42601",
		},
		{
			name: "40001 serialization failure definitely rolled back",
			err:  pgErr("40001", "could not serialize access"),
			want: verify.OutcomeFailed,
			code: "40001",
		},
		{
			name: "53300 too many connections never reached a commit",
			err:  pgErr("53300", "sorry, too many clients already"),
			want: verify.OutcomeFailed,
			code: "53300",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := verify.Classify(tc.err)
			if got.Outcome != tc.want {
				t.Fatalf("Outcome = %s, want %s (reason %q)", got.Outcome, tc.want, got.Reason)
			}
			if got.Code != tc.code {
				t.Fatalf("Code = %q, want %q", got.Code, tc.code)
			}
			if got.Reason == "" {
				t.Fatal("Reason is empty; every classification must explain itself")
			}
		})
	}
}

func TestClassifyNeverInventsACommit(t *testing.T) {
	errs := []error{
		pgErr("57P01", "admin shutdown"),
		pgErr("XX000", "internal error"),
		context.DeadlineExceeded,
		io.EOF,
		net.ErrClosed,
		errors.New("unknown"),
	}
	for _, err := range errs {
		if got := verify.Classify(err); got.Outcome == verify.OutcomeCommitted {
			t.Fatalf("Classify(%v) = committed", err)
		}
	}
}
