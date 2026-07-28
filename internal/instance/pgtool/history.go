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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// historySuffix is the extension PostgreSQL gives a timeline history file. The stem is the
// timeline the file describes, in eight hexadecimal digits.
const historySuffix = ".history"

// TimelineFork is one line of a timeline history file: the timeline that was left, and the
// position it was left at.
type TimelineFork struct {
	// Parent is the timeline the history forked away from.
	Parent int32
	// SwitchLSN is the last position that timeline and its successor have in common. WAL a
	// member wrote or replayed beyond it on Parent exists on no other copy.
	SwitchLSN string
}

// TimelineHistory is one .history file: which timeline it describes and every fork in its
// ancestry, oldest first.
type TimelineHistory struct {
	// Timeline is the timeline the file is named after.
	Timeline int32
	// Forks is the ancestry, in file order.
	Forks []TimelineFork
}

// SwitchFrom is the position at which this history left the given timeline, and whether it
// ever was on it at all.
//
// A history that does not mention a timeline never descended from it. That is not a gap in
// the record: it means the two histories share no ancestor this file can name, which is a
// stronger statement than being behind.
func (h TimelineHistory) SwitchFrom(timeline int32) (string, bool) {
	for _, fork := range h.Forks {
		if fork.Parent == timeline {
			return fork.SwitchLSN, true
		}
	}
	return "", false
}

// ParseTimelineHistory reads a .history file.
//
// The format is three tab-separated columns - parent timeline, switch position, reason -
// with blank lines and # comments allowed anywhere. Only the first two columns are
// meaningful here; the reason is free text PostgreSQL writes for humans.
func ParseTimelineHistory(timeline int32, contents string) (TimelineHistory, error) {
	history := TimelineHistory{Timeline: timeline}
	for line := range strings.SplitSeq(contents, "\n") {
		if comment := strings.Index(line, "#"); comment >= 0 {
			line = line[:comment]
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 {
			return TimelineHistory{}, fmt.Errorf("timeline history line %q has no switch position", line)
		}
		parent, err := strconv.ParseInt(fields[0], 10, 32)
		if err != nil {
			return TimelineHistory{}, fmt.Errorf("timeline history line %q: %w", line, err)
		}
		history.Forks = append(history.Forks, TimelineFork{
			Parent:    int32(parent),
			SwitchLSN: fields[1],
		})
	}
	return history, nil
}

// LatestTimelineHistory reads the highest-numbered .history file in a WAL directory.
//
// A standby fetches the primary's history files before it asks to stream, and keeps them
// whether or not the streaming attempt then succeeds - which is what makes this readable
// evidence of the primary's history rather than a guess about it. Returning false means
// this member has never been told about a timeline switch, which is not the same as there
// not having been one.
func LatestTimelineHistory(walDir string) (TimelineHistory, bool, error) {
	entries, err := os.ReadDir(walDir)
	if os.IsNotExist(err) {
		return TimelineHistory{}, false, nil
	}
	if err != nil {
		return TimelineHistory{}, false, err
	}

	latest := int32(0)
	name := ""
	for _, entry := range entries {
		timeline, ok := timelineOfHistoryFile(entry.Name())
		if !ok || timeline <= latest {
			continue
		}
		latest, name = timeline, entry.Name()
	}
	if name == "" {
		return TimelineHistory{}, false, nil
	}

	contents, err := os.ReadFile(filepath.Join(walDir, name))
	if err != nil {
		return TimelineHistory{}, false, err
	}
	history, err := ParseTimelineHistory(latest, string(contents))
	if err != nil {
		return TimelineHistory{}, false, err
	}
	return history, true, nil
}

func timelineOfHistoryFile(name string) (int32, bool) {
	stem, found := strings.CutSuffix(name, historySuffix)
	if !found || len(stem) != 8 {
		return 0, false
	}
	timeline, err := strconv.ParseUint(stem, 16, 32)
	if err != nil {
		return 0, false
	}
	return int32(timeline), true
}
