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

package pgtool

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleHistory = "1\t0/3000060\tno recovery target specified\n" +
	"\n" +
	"2\t0/70000A0\tno recovery target specified\n"

func TestParseTimelineHistory(t *testing.T) {
	history, err := ParseTimelineHistory(3, sampleHistory)
	if err != nil {
		t.Fatalf("ParseTimelineHistory = %v", err)
	}
	if history.Timeline != 3 {
		t.Errorf("timeline = %d, want 3", history.Timeline)
	}
	if len(history.Forks) != 2 {
		t.Fatalf("forks = %v, want two", history.Forks)
	}
	if history.Forks[1].Parent != 2 || history.Forks[1].SwitchLSN != "0/70000A0" {
		t.Errorf("newest fork = %+v", history.Forks[1])
	}
}

func TestSwitchFromReportsWhetherTheTimelineIsAnAncestorAtAll(t *testing.T) {
	history, err := ParseTimelineHistory(3, sampleHistory)
	if err != nil {
		t.Fatalf("ParseTimelineHistory = %v", err)
	}
	if lsn, found := history.SwitchFrom(2); !found || lsn != "0/70000A0" {
		t.Errorf("SwitchFrom(2) = %q, %v", lsn, found)
	}
	if _, found := history.SwitchFrom(7); found {
		t.Error("a timeline the history never descended from must not report a switch point")
	}
}

func TestParseTimelineHistoryRejectsALineWithNoPosition(t *testing.T) {
	if _, err := ParseTimelineHistory(2, "1\n"); err == nil {
		t.Error("a history line with no switch position must be an error, not a zero position")
	}
}

func TestLatestTimelineHistoryTakesTheHighestNumberedFile(t *testing.T) {
	walDir := t.TempDir()
	write := func(name, contents string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(walDir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("00000002.history", "1\t0/3000060\tno recovery target specified\n")
	write("00000010.history", "1\t0/3000060\tx\n2\t0/70000A0\tx\n")
	write("000000010000000000000009", "not a history file")

	history, found, err := LatestTimelineHistory(walDir)
	if err != nil || !found {
		t.Fatalf("LatestTimelineHistory = %v, %v", found, err)
	}
	if history.Timeline != 16 {
		t.Errorf("timeline = %d, want 16 from the hexadecimal stem", history.Timeline)
	}
}

func TestLatestTimelineHistoryIsAbsentRatherThanAnErrorWhenThereIsNone(t *testing.T) {
	if _, found, err := LatestTimelineHistory(t.TempDir()); found || err != nil {
		t.Errorf("LatestTimelineHistory = %v, %v", found, err)
	}
	if _, found, err := LatestTimelineHistory(filepath.Join(t.TempDir(), "gone")); found || err != nil {
		t.Errorf("a missing WAL directory = %v, %v", found, err)
	}
}
