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
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

const opSync = "sync"

type recordingWriter struct {
	mu       sync.Mutex
	ops      []string
	buf      bytes.Buffer
	syncErr  error
	writeErr error
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = append(w.ops, "write("+strings.TrimSuffix(string(p), "\n")+")")
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.buf.Write(p)
}

func (w *recordingWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = append(w.ops, opSync)
	return w.syncErr
}

func (w *recordingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = append(w.ops, "close")
	return nil
}

func (w *recordingWriter) sequence() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.ops)
}

func TestEveryRecordIsFsyncedBeforeItsWriteReturns(t *testing.T) {
	w := &recordingWriter{}
	ledger := newLedger(w)

	if err := ledger.Attempt(7); err != nil {
		t.Fatalf("Attempt: %v", err)
	}
	if got := w.sequence(); !slices.Equal(got, []string{"write(ATTEMPTED 7 90dcf4f9)", opSync}) {
		t.Fatalf("after Attempt, ops = %v", got)
	}

	if err := ledger.Commit(7); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	want := []string{"write(ATTEMPTED 7 90dcf4f9)", opSync, "write(COMMITTED 7 aba6bebd)", opSync}
	if got := w.sequence(); !slices.Equal(got, want) {
		t.Fatalf("ops = %v, want %v", got, want)
	}
}

func TestAttemptFailsWhenTheRecordCannotBeMadeDurable(t *testing.T) {
	syncFailure := errors.New("fsync failed")
	ledger := newLedger(&recordingWriter{syncErr: syncFailure})

	if err := ledger.Attempt(1); !errors.Is(err, syncFailure) {
		t.Fatalf("Attempt error = %v, want %v", err, syncFailure)
	}
}

func TestARecordThatCannotBeWrittenIsNotFsynced(t *testing.T) {
	writeFailure := errors.New("disk full")
	w := &recordingWriter{writeErr: writeFailure}
	ledger := newLedger(w)

	if err := ledger.Commit(1); !errors.Is(err, writeFailure) {
		t.Fatalf("Commit error = %v, want %v", err, writeFailure)
	}
	if got := w.sequence(); slices.Contains(got, opSync) {
		t.Fatalf("ops = %v, want no sync after a failed write", got)
	}
}

func TestReplayRoundTripsEveryState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, prior, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(prior) != 0 {
		t.Fatalf("a fresh ledger replayed %d records", len(prior))
	}
	writeAll(t, ledger, []Record{
		{StateAttempted, 1}, {StateCommitted, 1},
		{StateAttempted, 2}, {StateIndeterminate, 2},
		{StateAttempted, 3},
	})
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := []Record{
		{StateAttempted, 1}, {StateCommitted, 1},
		{StateAttempted, 2}, {StateIndeterminate, 2},
		{StateAttempted, 3},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
}

func TestReplayDropsATornTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeAll(t, ledger, []Record{{StateAttempted, 1}, {StateCommitted, 1}})
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	appendRaw(t, path, "ATTEMPTED 2 f4f")

	records, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if want := []Record{{StateAttempted, 1}, {StateCommitted, 1}}; !slices.Equal(records, want) {
		t.Fatalf("records = %v, want %v", records, want)
	}
}

// A crash mid-append leaves a prefix with no newline. It can never leave a whole
// terminated line, so a terminated line whose checksum is wrong is damage rather than a
// torn tail - and dropping it would take a COMMITTED out of the set the oracle requires,
// which turns a lost acknowledged write into a passing verdict.
func TestReplayRefusesATerminatedTrailingRecordThatFailsItsChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeAll(t, ledger, []Record{{StateAttempted, 1}})
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	appendRaw(t, path, "COMMITTED 1 00000000\n")

	if _, err := ReadFile(path); !errors.Is(err, ErrCorruptLedger) {
		t.Fatalf("ReadFile error = %v, want ErrCorruptLedger", err)
	}
}

// The same record with its newline lost is the one shape a crash can produce, and is the
// only shape that may be dropped.
func TestReplayDropsTheSameRecordWhenItsNewlineIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeAll(t, ledger, []Record{{StateAttempted, 1}})
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	appendRaw(t, path, "COMMITTED 1 00000000")

	records, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if want := []Record{{StateAttempted, 1}}; !slices.Equal(records, want) {
		t.Fatalf("records = %v, want %v", records, want)
	}
}

func TestReplayRefusesDamageThatIsNotATornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeAll(t, ledger, []Record{{StateCommitted, 1}, {StateCommitted, 2}, {StateCommitted, 3}})
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.SplitAfter(string(raw), "\n")
	lines[0] = strings.Replace(lines[0], "COMMITTED 1", "COMMITTED 9", 1)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "")), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := ReadFile(path); !errors.Is(err, ErrCorruptLedger) {
		t.Fatalf("error = %v, want ErrCorruptLedger", err)
	}
}

func TestOpenTruncatesATornTailSoAppendsStayValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeAll(t, ledger, []Record{{StateAttempted, 1}, {StateCommitted, 1}})
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	appendRaw(t, path, "ATTEMPTED 2 f4f")

	reopened, prior, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(prior) != 2 {
		t.Fatalf("replayed %d records, want 2", len(prior))
	}
	writeAll(t, reopened, []Record{{StateAttempted, 2}, {StateCommitted, 2}})
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	records, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := []Record{{StateAttempted, 1}, {StateCommitted, 1}, {StateAttempted, 2}, {StateCommitted, 2}}
	if !slices.Equal(records, want) {
		t.Fatalf("records = %v, want %v", records, want)
	}
}

func TestConcurrentAppendsProduceAWellFormedLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.log")
	ledger, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const writers, perWriter = 8, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				v := int64(w*perWriter + i)
				if err := ledger.Attempt(v); err != nil {
					t.Errorf("Attempt(%d): %v", v, err)
					return
				}
				if err := ledger.Commit(v); err != nil {
					t.Errorf("Commit(%d): %v", v, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	records, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(records) != writers*perWriter*2 {
		t.Fatalf("replayed %d records, want %d", len(records), writers*perWriter*2)
	}
	summary := Summarize(records)
	if len(summary.Committed) != writers*perWriter {
		t.Fatalf("committed %d values, want %d", len(summary.Committed), writers*perWriter)
	}
	if len(summary.Orphans) != 0 {
		t.Fatalf("orphans = %v, want none", summary.Orphans)
	}
}

func TestNextValueResumesAboveThePriorLedger(t *testing.T) {
	if got := NextValue(nil); got != 1 {
		t.Fatalf("NextValue(nil) = %d, want 1", got)
	}
	records := []Record{{StateAttempted, 4}, {StateCommitted, 4}, {StateAttempted, 41}}
	if got := NextValue(records); got != 42 {
		t.Fatalf("NextValue = %d, want 42", got)
	}
}

func writeAll(t *testing.T, ledger *Ledger, records []Record) {
	t.Helper()
	for _, rec := range records {
		var err error
		switch rec.State {
		case StateAttempted:
			err = ledger.Attempt(rec.Value)
		case StateCommitted:
			err = ledger.Commit(rec.Value)
		case StateIndeterminate:
			err = ledger.Indeterminate(rec.Value)
		}
		if err != nil {
			t.Fatalf("writing %s %d: %v", rec.State, rec.Value, err)
		}
	}
}

func appendRaw(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := f.WriteString(text); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
