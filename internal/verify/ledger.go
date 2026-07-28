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

// Package verify implements the pgelastic durability oracle: the Patroni-Jepsen
// `patroni-set` checker. A workload inserts monotonically increasing integers into
// `set(value bigint primary key)` while recording every attempt in a durable
// append-only ledger; after the chaos window heals the surviving primary is read back
// and the observed set is checked against the ledger.
//
// The two assertions are `COMMITTED ⊆ R` — no committed transaction was lost, the only
// assertion that fails a release — and `R ⊆ ATTEMPTED` — no value the workload never
// tried to write appeared, which catches split-brain writes and phantom acknowledgements.
package verify

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// State is one of the three facts the ledger can record about a value.
type State string

const (
	// StateAttempted is written, and made durable, before the INSERT is issued.
	StateAttempted State = "ATTEMPTED"
	// StateCommitted is written only on a definite, acknowledged success.
	StateCommitted State = "COMMITTED"
	// StateIndeterminate is written for every ambiguous outcome. It is not a failure.
	StateIndeterminate State = "INDETERMINATE"
)

// ErrCorruptLedger reports damage that is not a torn trailing write and so cannot be
// attributed to the verifier being killed mid-append.
var ErrCorruptLedger = errors.New("corrupt ledger")

// Record is one ledger line.
type Record struct {
	State State
	Value int64
}

const checksumWidth = 8

func encodeRecord(rec Record) []byte {
	payload := string(rec.State) + " " + strconv.FormatInt(rec.Value, 10)
	sum := crc32.ChecksumIEEE([]byte(payload))
	return fmt.Appendf(nil, "%s %0*x\n", payload, checksumWidth, sum)
}

func decodeRecord(line []byte) (Record, error) {
	fields := strings.Fields(string(line))
	if len(fields) != 3 {
		return Record{}, fmt.Errorf("expected 3 fields, got %d", len(fields))
	}
	state := State(fields[0])
	switch state {
	case StateAttempted, StateCommitted, StateIndeterminate:
	default:
		return Record{}, fmt.Errorf("unknown state %q", fields[0])
	}
	value, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return Record{}, fmt.Errorf("bad value: %w", err)
	}
	want, err := strconv.ParseUint(fields[2], 16, 32)
	if err != nil {
		return Record{}, fmt.Errorf("bad checksum: %w", err)
	}
	rec := Record{State: state, Value: value}
	if got := crc32.ChecksumIEEE([]byte(fields[0] + " " + fields[1])); got != uint32(want) {
		return Record{}, fmt.Errorf("checksum mismatch: have %08x, want %08x", got, want)
	}
	return rec, nil
}

// Replay parses a ledger, returning every intact record and the byte length of the
// intact prefix. A trailing record that is short or fails its checksum is a torn append
// from the verifier being killed mid-write: it is dropped and excluded from the returned
// length. Damage anywhere before the last record is not attributable to a crash and
// yields ErrCorruptLedger, because silently discarding it could discard a COMMITTED.
func Replay(r io.Reader) ([]Record, int64, error) {
	br := bufio.NewReader(r)
	var (
		recs   []Record
		intact int64
	)
	for {
		line, err := br.ReadBytes('\n')
		terminated := len(line) > 0 && line[len(line)-1] == '\n'
		if terminated {
			rec, decErr := decodeRecord(line[:len(line)-1])
			if decErr != nil {
				if _, peekErr := br.Peek(1); errors.Is(peekErr, io.EOF) {
					return recs, intact, nil
				}
				return nil, 0, fmt.Errorf("%w at offset %d: %w", ErrCorruptLedger, intact, decErr)
			}
			recs = append(recs, rec)
			intact += int64(len(line))
			continue
		}
		if err == nil || errors.Is(err, io.EOF) {
			return recs, intact, nil
		}
		return nil, 0, err
	}
}

// ReadFile replays the ledger at path. A missing file is an empty ledger.
func ReadFile(path string) ([]Record, error) {
	f, err := os.Open(path) // #nosec G304 -- the ledger path is operator-supplied by design
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	recs, _, err := Replay(f)
	return recs, err
}

type syncWriter interface {
	io.Writer
	Sync() error
	Close() error
}

// Ledger is the append-only, fsync-per-record write-ahead log of the workload's
// intentions and outcomes. Every method makes its record durable before returning; a
// record the verifier has not yet made durable is a record the check cannot rely on.
type Ledger struct {
	mu sync.Mutex
	w  syncWriter
}

// Open replays the ledger at path, truncates a torn trailing record, and returns a
// ledger positioned to append along with the records already durable. Reopening an
// existing ledger is how a second run checks the work of a run that was killed.
func Open(path string) (*Ledger, []Record, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600) // #nosec G304 -- operator-supplied path
	if err != nil {
		return nil, nil, err
	}
	recs, intact, err := Replay(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if err := f.Truncate(intact); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(intact, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	// Without an fsync on the containing directory a freshly created ledger can lose its
	// own directory entry in a crash, taking every COMMITTED record with it.
	if err := syncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return &Ledger{w: f}, recs, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- derived from the operator-supplied ledger path
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func newLedger(w syncWriter) *Ledger {
	return &Ledger{w: w}
}

// Attempt durably records the intent to insert v. It must return before the INSERT is
// issued: a value inserted without a durable ATTEMPTED would violate `R ⊆ ATTEMPTED`.
func (l *Ledger) Attempt(v int64) error { return l.append(StateAttempted, v) }

// Commit records a definite, acknowledged success. Anything short of proof belongs in
// Indeterminate instead.
func (l *Ledger) Commit(v int64) error { return l.append(StateCommitted, v) }

// Indeterminate records an ambiguous outcome — a timeout, a reset, an admin shutdown, a
// commit whose acknowledgement never arrived. It is not a failure and never fails a run.
func (l *Ledger) Indeterminate(v int64) error { return l.append(StateIndeterminate, v) }

func (l *Ledger) append(state State, v int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.w.Write(encodeRecord(Record{State: state, Value: v})); err != nil {
		return err
	}
	return l.w.Sync()
}

// Close releases the underlying file.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Close()
}
